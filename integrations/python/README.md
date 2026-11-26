# speko-gateway

Python client and voice-framework integrations for
[Speko Gateway](https://github.com/SpekoAI/gateway), the open-source sidecar
that connects a voice agent to speech providers. The package has LiveKit
Agents and Pipecat classes for speech-to-text (STT), text-to-speech (TTS) and
LLM, a plain asyncio client for the Gateway socket, and clients for the hosted
Speko Router.

## Install

The package needs Python 3.10 to 3.14. It is shipped inside the Gateway image
at `/opt/speko/python`, and it installs from a checkout of the repository:

```bash
pip install "./integrations/python[livekit]"   # or [pipecat]
```

| Extra | Brings | Notes |
| --- | --- | --- |
| `livekit` | `livekit-agents>=1.4.5,<2` | |
| `pipecat` | `pipecat-ai>=1.7,<2` | Python 3.11 or newer |

The voice classes need a running Gateway. See the
[Gateway README](https://github.com/SpekoAI/gateway#quick-start) for how to
start one and how to add it to an agent image.

## What is in the package

| Import | Talks to | Use |
| --- | --- | --- |
| `speko_gateway.livekit`: `STT`, `TTS` | Local Gateway socket | LiveKit Agents voice legs |
| `speko_gateway.livekit`: `LLM` | Speko Router | LiveKit Agents LLM |
| `speko_gateway.pipecat`: `SpekoSTTService`, `SpekoTTSService`, `SpekoLLMService` | Gateway socket, Router | Pipecat services |
| `speko_gateway`: `GatewayClient` | Local Gateway socket | Sessions without a framework |
| `speko_gateway.probe`: `ConversationProbe` | Local Gateway socket | Per-turn latency markers |
| `speko_gateway`: `RelaySpeechClient`, `RelayLLMClient`, `RelayTTSClient`, `RelayEvaluationClient` | Speko Router | See [Speko Router clients](#speko-router-clients) |

## Configuration

| Variable | Read by | Meaning |
| --- | --- | --- |
| `SPEKO_LOCAL_AUTH_TOKEN` (or `_FILE`) | `GatewayClient.from_env()`, and so `STT()`, `TTS()`, `ConversationProbe` | Bearer token of the local Gateway. Required |
| `SPEKO_SOCKET_PATH` | the same | Gateway socket. Default `/run/speko/runtime.sock` |
| `SPEKO_API_KEY` (or `_FILE`) | `LLM()`, `SpekoLLMService`, `RelayEvaluationClient.from_env()` | Speko API key. Also decides `credential_source="auto"` |
| `SPEKO_ROUTER_URL` | the same | Router origin. Default `https://router.speko.dev`. `SPEKO_RELAY_URL` is read as a fallback |

`credential_source` on the voice classes chooses whose provider key is used:

| Value | Meaning |
| --- | --- |
| `"auto"` (default) | Managed when `SPEKO_API_KEY` is set, otherwise BYOK |
| `"byok"` | The provider key configured in the Gateway |
| `"managed"` | Speko's provider credential, billed through Speko |

## LiveKit Agents

```python
from livekit.agents import AgentSession
from livekit.plugins import openai

from speko_gateway.livekit import STT, TTS

session = AgentSession(
    stt=STT(
        language="en",             # default
        provider="auto",           # default. Set it when several BYOK STT keys exist
        model="auto",              # default: the provider's catalog default
        credential_source="auto",  # default
        sample_rate=16_000,        # default
    ),
    llm=openai.LLM(model="gpt-4.1-mini"),  # any LiveKit LLM plugin
    tts=TTS(
        provider="auto",           # default
        model="auto",              # default
        voice="",                  # default: the configured or catalog voice
        language="en",             # default
        sample_rate=24_000,        # default
        credential_source="auto",  # default
    ),
)
```

### STT options

```python
stt = STT(
    provider="deepgram",             # pin provider and model when an option is required
    model="nova-3",                  # Deepgram's default, Flux, has no diarization
    diarization=True,                # speaker labels
    keywords=["Speko", "Casey"],     # vocabulary biasing
    provider_options={               # the vendor's own settings
        "deepgram": {"numerals": True, "endpointing": 1200},
        "elevenlabs": {"vad_silence_threshold_secs": 0.7},
    },
)
```

- `diarization`, `keywords` and `noise_reduction` fail closed. A session
  routed to a provider that cannot honor one is refused at create with
  `stt_option_unsupported` and the option named. Deepgram, for example, has
  no `noise_reduction`.
- With `provider="auto"`, routing may pick a provider that refuses the
  option. Pin the provider when an option is a requirement.
- A setting under `provider_options` that the provider's model does not
  allow is refused by name. Settings for a provider the session does not
  reach are ignored.
- Speaker labels arrive on finals as LiveKit `speaker_id`.

ElevenLabs has two realtime Scribe models. `scribe_v2_realtime` is the
default. `model="scribe_v3_live"` adds `diarization=True` and the provider
options `turn_prediction` and `multilang_mode`, which other Scribe models
refuse. With diarization, a final waits up to 0.8 s after its commit for the
speaker labels and is delivered without them when they do not arrive.

### TTS options and standby sessions

`TTS(provider_options=...)` passes a vendor's synthesis settings, for example
`{"cartesia": {"normalization": "en-IN"}}` or
`{"elevenlabs": {"stability": 0.4, "similarity_boost": 0.8, "style": 0.2}}`.
`update_options(language=..., voice=..., provider_options=...)` changes them
for the next reply. A setting the routed model cannot honor is refused before
the session opens.

With the local Gateway, `TTS` keeps one Gateway session open in standby so a
reply's first text does not wait for a session to open.

| Parameter | Default | Meaning |
| --- | --- | --- |
| `standby_sessions` | `1` with `GatewayClient`, `0` with `RelaySpeechClient` | Sessions kept open ahead of the next reply. `0` opens each session at the reply's first text. With `RelaySpeechClient`, `0` keeps one stream for all replies (below); more than `0` opens a stream per reply |
| `max_input_characters` | `10000` | Character limit of one reply, about ten minutes of speech. The Gateway refuses text past it and the reply fails |

- LiveKit calls `prewarm()` when the agent starts, which opens the standby.
  Each reply opens the next one.
- A standby that no reply takes within 3 minutes closes. `update_options`
  replaces a standby that has the old settings.
- LiveKit does not close the `TTS` when the session ends. Call `aclose()` to
  close the standby sooner.
- Each open session counts toward your organization's concurrent-session
  limit and the provider's connection limits: one between replies, two
  during a reply.
- With managed credentials, Speko reserves credit for each open session's
  `max_input_characters`, so a standby holds a second reservation. Lower
  `max_input_characters` to reserve less.

### One TTS stream per call on the native relay

With `RelaySpeechClient` and no `standby_sessions`, `TTS` opens one admitted
relay stream at the first reply's text and sends each later reply as more
utterances on it (`input.commit` per utterance). The relay renews the
stream's lease itself.

- The stream is reused while the options are unchanged. A change through
  `update_options` closes it and the next reply opens a new one.
- A stream that the relay or the provider ended, or that failed while idle,
  is dropped and the next reply opens a new one. A reply that meets a failure
  fails; its text is never sent again.
- A stream idle for 15 seconds closes, before the relay's 30-second
  inactivity timeout would end it as a failed request. The next reply
  reopens it, so a reply that comes within 15 seconds of the previous one
  skips the open and a later one opens a new stream as before.
- Before a reply would pass `max_input_characters` on one stream, the stream
  closes between utterances and the reply continues on a new one.
- When the caller interrupts a reply, only that utterance is cancelled
  (`input.cancel`) and the stream stays open. The relay does not acknowledge
  a cancel, so audio of the cancelled utterance is dropped until the next
  `utterance.started`. A cancel before the utterance's first audio is sent
  and then the stream is closed with `session.close` (so its usage is
  reported), because that utterance's start could not be told from the next
  one's. The next reply opens a new stream.
- A closing stream waits up to 2 seconds in all for the relay's terminal
  frame, so a slow close ends before an agent worker's own 5-second
  teardown bound.
- Replies share the stream one at a time. A reply waits for the previous one
  to let go.
- `aclose()` ends the stream with `session.close`. Until then the stream is
  open and holds the organization's TTS slot and its reservation. The
  terminal receipt for the call's speech arrives when the stream closes.

### LiveKit versions

- The classes support `livekit-agents` 1.4.5 and later.
- Before 1.6.5 there is no keyterm capability flag. `STT(keywords=...)` still
  sends the keywords to the provider.
- From 1.8.9, `STT(provider="soniox")` and `STT(provider="omi")` let LiveKit
  flush the stream when the user stops speaking. The Gateway sends the
  provider's `finalize`, so the final transcript arrives sooner than the
  provider's own endpointer would send it. Other providers and
  `provider="auto"` wait for the provider's endpointer.
- This package depends on `httpx` because `livekit-agents` before 1.6.10
  imports it without declaring it.

## Speko LLM

With a Speko API key the LLM can come from Speko too. `LLM()` talks HTTPS to
the Speko Router, not to the local socket, so the conversation history
travels through the Router.

```python
from speko_gateway.livekit import LLM, STT, TTS

session = AgentSession(
    stt=STT(),
    llm=LLM(
        provider="auto",          # default. Set provider and model together to pin
        model="auto",             # default. GET router.speko.dev/v1/models lists models
        objective="balanced",     # default, or "quality", "latency", "cost"
        max_output_tokens=8_192,  # default
        reasoning_effort=None,    # default, or "none" ... "xhigh"
        frequency_penalty=None,   # default, or -2 to 2
        presence_penalty=None,    # default, or -2 to 2
        repetition_penalty=None,  # default, or greater than 0 and at most 2
    ),
    tts=TTS(),
)
```

Function tools are supported. Image and audio content is skipped: only text
is forwarded.

| `reasoning_effort` | Meaning |
| --- | --- |
| `None` | No field is sent. The provider default applies |
| `"none"` | Reasoning off |
| `"minimal"` | The least reasoning the routed model allows |
| `"low"`, `"medium"`, `"high"`, `"xhigh"` | More reasoning |

- Any other value raises `ValueError` when the LLM is built.
- While reasoning is on (any value but `"none"`), the Router refuses
  `temperature` and `top_p`.
- `"none"` is accepted on models that cannot reason, except on older
  Routers, which refuse it there (for example on `gpt-4.1`).
- Four models always reason and refuse `"none"`. Their floor is `"minimal"`:
  Inception `mercury-voice`, `gpt-oss-120b` on Cerebras and on Together, and
  Together `zai-org/GLM-5.3-Flash`.
- Every other value needs a model that can reason. A pinned model that
  cannot is refused with `capability_unsupported`, and auto routing
  considers only models that can.

The three penalties guard against a model that repeats one phrase until
`max_output_tokens`.

| Parameter | Range | Neutral |
| --- | --- | --- |
| `frequency_penalty` | -2 to 2 | 0 |
| `presence_penalty` | -2 to 2 | 0 |
| `repetition_penalty` | greater than 0, at most 2 | 1 |

- A value out of range raises `ValueError`. `None` sends no field.
- The Router applies a non-neutral penalty only on Together and Baseten.
  Auto routing then keeps the request on those models, and a model pinned
  elsewhere is refused with `capability_unsupported`.
- Older Routers ignore all three.

## Pipecat

```python
from speko_gateway.pipecat import SpekoSTTService, SpekoTTSService

stt = SpekoSTTService(provider="auto", model="auto", language="en")
tts = SpekoTTSService(provider="auto", model="auto", language="en")

pipeline = Pipeline(
    [
        transport.input(),
        stt,
        user_aggregator,
        llm,
        tts,
        transport.output(),
        assistant_aggregator,
    ]
)
```

Keep a Pipecat VAD in the pipeline: `VADUserStoppedSpeakingFrame` commits the
current STT utterance. The image setup, `SpekoLLMService` and all options are
in the
[Pipecat guide](https://github.com/SpekoAI/gateway/blob/main/docs/PIPECAT.md).

## Direct client

`GatewayClient` opens Gateway sessions without a framework. This example
writes synthesized speech to a file as 24 kHz mono 16-bit PCM:

```python
import asyncio

from speko_gateway import GatewayClient
from speko_gateway.client import SessionConfig


async def main() -> None:
    client = GatewayClient.from_env()
    session = await client.open(
        SessionConfig(
            kind="tts",
            execution={
                "provider_route": "provider_direct",
                "credential_source": "byok",
                "relay_policy": "forbidden",
            },
            request={
                "provider": "deepgram",
                "language": "en",
                "max_input_characters": 1000,
            },
            media={"encoding": "pcm_s16le", "sample_rate_hz": 24000, "channels": 1},
        )
    )
    await session.append_text("Hello from Speko Gateway.")
    await session.commit_text()
    with open("hello.pcm", "wb") as out:
        async for event in session.events():
            if event.audio:
                out.write(event.audio)
            elif event.type in ("audio.done", "error"):
                break
    await session.aclose()
    await client.aclose()


asyncio.run(main())
```

A session also has `send_audio`, `commit_audio`, `cancel` and `finish`. The
request and event shapes are in the
[protocol document](https://github.com/SpekoAI/gateway/blob/main/docs/PROTOCOL.md).
`client.wait_until_ready()` waits for a Gateway that is still starting.

## Conversation profiler

`ConversationProbe` ties the STT, LLM, TTS and playback of each turn into one
latency trace. It sends timing markers only, never content, and never raises
into the agent. `SPEKO_TELEMETRY_DISABLED=true` on the Gateway suppresses it.

```python
from speko_gateway.probe import ConversationProbe

session = AgentSession(stt=STT(), llm=LLM(), tts=TTS())
probe = ConversationProbe(session)
probe.start()                  # before session.start()

await session.start(...)
...
await probe.aclose()           # during shutdown
```

The marker list is in the
[trust document](https://github.com/SpekoAI/gateway/blob/main/docs/TRUST.md#conversation-turn-markers).

## Speko Router clients

These clients talk to the Speko Router, either at its HTTPS origin or through
a Gateway running in native relay mode (`speko-gateway native`). Speko's own
managed agents use them. Each client carries its own Speko credential.

### Speech: `RelaySpeechClient`

`RelaySpeechClient` streams STT and TTS for one session. It takes the
session's credential, its session ID and the Router origin explicitly. It
never reads `SPEKO_API_KEY` and never shares prefetched plans.

```python
from speko_gateway import RelaySpeechClient
from speko_gateway.livekit import STT, TTS

speech = RelaySpeechClient(
    api_key=model_key,            # starts with sk_speko_
    session_id=spec.session_id,
    base_url=relay_origin,
)
stt = STT(speech, provider=stt_provider, model=stt_model,
          language=stt_language, credential_source="managed", turn_vad=local_vad)
tts = TTS(speech, provider=tts_provider, model=tts_model, voice="",
          language=tts_language, credential_source="managed")
stt.bind_session(session)         # before AgentSession.start()
```

For the native sidecar, pass `socket_path` and `local_auth_token` instead of
`base_url`. `RelayLLMClient` takes the same parameters:

```python
from speko_gateway import RelayLLMClient, RelaySpeechClient

connection = dict(
    api_key=model_key,
    session_id=spec.session_id,
    socket_path="/run/speko/runtime.sock",
    local_auth_token=socket_token,
)
speech = RelaySpeechClient(**connection)
llm_client = RelayLLMClient(**connection)
```

What to supply:

- A concrete `provider` and `model`. `"auto"` is refused.
- `credential_source="managed"`.
- `voice=""` for TTS when Speko has configured the session's voice.
- A session ID of 1 to 200 characters: a letter or digit, then letters,
  digits or `._:-`. It is sent unchanged.
- `turn_vad` for LiveKit STT: the worker's local LiveKit VAD, such as
  Silero. Without it the constructor raises `ValueError`. A VAD that reports
  probabilities must expose its `activation_threshold`.
- `stt.bind_session(session)` before the session starts.
  `bind_agent(agent)` finds the session when the first stream starts. Code
  that reads the stream itself uses `stream(manual_turns=True)`.
  `recognize()` on a complete buffer needs no binding.

How LiveKit STT behaves:

- The LiveKit stream stays open across turns. Each VAD turn is its own
  Router request.
- Final transcript segments and `END_OF_SPEECH` are emitted only after the
  request's final usage arrives.
- `speaker_id` values are valid within one request only. The vendor's label
  is in `provider_speaker_id`. The same vendor label in a later request is
  not the same person.
- The last part of each provider final carries the whole text as
  `canonical_text`.
- `stt.MultiSpeakerAdapter` and a single-model `stt.FallbackAdapter` may
  wrap the STT. A wrapper that rewrites or drops transcripts is refused
  before the request is admitted. Falling back to another model is the
  platform's decision, not the wrapper's.
- Custom audio processing that replaces a frame must keep its userdata and
  duration.
- A speech onset that stays unresolved for one second without new VAD output
  fails the stream.

When the `AgentSession` has its own VAD, wrap it so that it keeps processing
buffered audio:

```python
from livekit.agents import AgentSession, inference
from speko_gateway.livekit import DrainingVAD

session = AgentSession(
    stt=stt,
    llm=llm,
    tts=tts,
    vad=DrainingVAD(inference.VAD(model="silero"), flush_mode="reset"),
    turn_handling={
        "turn_detection": inference.TurnDetector(version="v1-mini"),
        "interruption": {"mode": "vad", "enabled": True},
    },
)
stt.bind_session(session)
```

`DrainingVAD` supports the Silero and local-inference VADs. Use
`flush_mode="reset"` for the session's own VAD and `flush_mode="segment"`
for a batch STT `StreamAdapter`, with a separate instance for each.

How LiveKit TTS sends text:

- Text goes out sentence by sentence. A sentence ends at `.` followed by
  whitespace, at `!` or `?`, at a CJK full stop, or at a line break. An
  explicit `flush()` and the end of input send whatever is left.
- A pause alone never completes a word or a number: `Ple` can still become
  `Please`, and `0.` can become `0.05`. Text that stops inside a word waits.
- A period after an abbreviation (`Dr.`, `St.`, `e.g.`) does not end a
  sentence. When whitespace follows and nothing more arrives for 300 ms,
  that text is sent, except a title on its own (`Dr. `, `Capt. `): it waits
  for the next word, a `flush()` or the end of input.
- One utterance holds at most 240 Unicode code points (not bytes, not
  grapheme clusters). Longer text is cut at whitespace. A single unit longer
  than that without whitespace is refused and not retried.
- Every character is sent as given.
- LiveKit's default Markdown transform can hold back a trailing delimiter.
  `AgentSession(..., tts_text_transforms=[])` passes plain text through at
  once.
- Each utterance waits for the previous one's `utterance.done`.

With Pipecat, `SpekoSTTService` and `SpekoTTSService` accept the same client.
Pipecat's own VAD commits turns, and the automatic provider fallback does not
apply.

At the end of the call, close both models and then `await speech.aclose()`.

### Realtime voice models

Speech-to-speech models run on LiveKit's own OpenAI, xAI or GPT-Live plugin.
`realtime_options` points the plugin at the same `RelaySpeechClient`:

```python
from livekit.plugins import openai  # or xai
from speko_gateway.livekit import realtime_options

llm = openai.realtime.RealtimeModel(model="gpt-realtime-2.1", **realtime_options(speech))
# or: openai.realtime.GPTLiveModel(**realtime_options(speech))
```

- This needs LiveKit Agents 1.8 or later with the matching plugin.
- Each connection is a separately admitted and metered Router session.
  Failed connections are not retried.
- The OpenAI realtime plugin replaces its connection every 20 minutes by
  default and replays the conversation into a new session. The xAI and
  GPT-Live plugins do not. Pass `max_session_duration` to change it.
- GPT-Live needs the native sidecar socket. Its plugin always dials
  `/live/sessions`, which only the sidecar maps to the Router's `/v1/live`.
  Its default `responses` delegation runs the backend model on the Router,
  which reserves and meters it with the session.
- Gemini Live is not covered. Google's SDK opens its own socket and cannot
  use the sidecar's Unix socket.

### One-shot speech: `RelayTTSClient`

`RelayTTSClient` sends one `POST /v1/tts/speech` and yields the audio bytes
as they arrive.

```python
from speko_gateway import RelayTTSClient

speech = RelayTTSClient(api_key=key, base_url=router_origin, session_id=session_id)
try:
    async for chunk in speech.synthesize(
        {
            "routing": {"mode": "explicit", "provider": provider, "model": model},
            "input": text,
            "language": language,
            "audio": {"encoding": "pcm_s16le", "sample_rate_hz": 24000, "channels": 1},
        },
        idempotency_key=original_request_id,
    ):
        await consume_audio(chunk)
finally:
    await speech.aclose()
```

| Limit | Value |
| --- | --- |
| Request body | 1 MiB |
| Audio | 64 MiB. Lower it with `max_audio_bytes` |
| `io_timeout` | 30 s of waiting for headers and audio. Time your code spends on a chunk does not count |
| `idempotency_key` | Required. Up to 256 UTF-8 bytes |

Pass `work_nonce` and `input_blinder` together as keyword arguments when the
request belongs to registered work. Do not put them in the request
dictionary.

### Evaluations: `RelayEvaluationClient`

```python
from speko_gateway import RelayEvaluationClient

client = RelayEvaluationClient.from_env(session_id="conversation-42")
result = await client.evaluate(
    state={"transcript": "Can I speak to someone?"},
    questions={
        "handoff": {
            "type": "noul",
            "instructions": "Is a human explicitly requested?",
        }
    },
    idempotency_key="conversation-42-turn-7",
)
await client.aclose()
```

`python examples/jev_voice_router.py --fake` runs a framework-independent
example against a local fake server. It reads final transcripts from standard
input, asks three questions in one batch, applies a 500 ms deadline, cancels
superseded turns and calls only allow-listed read-only handlers. Its
confidence thresholds are examples: tune them on your own traffic.

### Registered model requests

`RelayLLMClient.complete_registered_response` and
`stream_registered_response` send a model request that the Speko platform
has prepared and registered for one piece of work. Use the first for a
`stream:false` body and the second for a `stream:true` body.

| Argument | Meaning |
| --- | --- |
| `rendering` | The rendering name the platform gave |
| `request_body` | The exact request string, at most 262144 UTF-8 bytes. It is sent untouched |
| `work_nonce`, `input_blinder` | 64 lowercase hex characters each, from the platform |
| `idempotency_key` | The work's original key |
| `attempt_id` | Lowercase UUID. Required for `agent-graph/openai-responses/v2` and `agent-graph/anthropic-messages/v2`, refused for other renderings |

- Each call makes one `POST /v1/llm/responses` with
  `Speko-Client-Session-ID`. Invalid input is refused before any HTTP
  request.
- Responses and the cumulative stream are limited to 1 MiB. Duplicate JSON
  keys, non-finite numbers, invalid Unicode and non-object payloads are
  refused.
- The stream yields its success event only after a clean end of the HTTP
  response. An event after the terminal event, an unknown event, or bare CR
  line endings fail the stream.

## Rules

- `RelaySpeechClient`, `RelayTTSClient` and the registered-request methods
  never retry, follow redirects or fall back to another provider or
  transport. The same holds for `RelayLLMClient` on the native socket. A
  retry hint, a timeout or a transport error does not prove that the
  provider was not called.
- With `RelaySpeechClient`, the LiveKit classes turn off LiveKit's automatic
  retries for STT and TTS. After a failed native STT or Pipecat native TTS
  instance, build a new instance only once the platform has decided how to
  recover.
- A retry of registered work must reuse the original work, input and key.
  For the `v2` renderings, do not reuse an attempt ID after cancellation, a
  restart or an uncertain send, and do not use it as the idempotency key.
- Keep credentials, `input_blinder`, `attempt_id` and registered request
  bodies in memory only. Do not log or store them.
- A response is not a settled charge, and received audio is not proof that
  anyone heard it.
- Only a final usage frame produces `usage.reported`. A lost final frame
  fails the stream.

## Development

```bash
cd integrations/python
uv run --locked --extra dev ruff check .
uv run --locked --extra dev pytest
```
