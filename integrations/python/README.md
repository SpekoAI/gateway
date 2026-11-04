# Speko Gateway Python integration

This package connects Python voice-agent frameworks to the authenticated local
Speko Gateway socket, or to the Speko Router's native STT, TTS, LLM and typed
evaluation surfaces. Voice classes use an explicitly supplied transport client
or the local Gateway socket. Relay clients carry their own Speko credential.

## Native relay speech

`RelaySpeechClient` sends STT/TTS streams through the router's native execution
grants. Give each platform session its own registered model credential; the
client takes the key, platform session UUID and configured relay origin
explicitly and never reads `SPEKO_API_KEY` or shares warm plans. The relay and
cell still perform admission, enforce the compiled route and settle usage.

```python
from speko_gateway import RelaySpeechClient
from speko_gateway.livekit import STT, TTS

speech = RelaySpeechClient(
    api_key=model_key,
    session_id=spec.session_id,
    base_url=relay_origin,
)
stt = STT(speech, provider=stt_provider, model=stt_model,
          language=stt_language, credential_source="managed", turn_vad=local_vad)
tts = TTS(speech, provider=tts_provider, model=tts_model, voice="",
          language=tts_language, credential_source="managed")
# Before AgentSession.start(agent=agent), bind the owned session.
stt.bind_session(session)  # bind_agent(agent) also locates its initial session
```

The provider/model/language fields come from the compiled spec. A managed
worker leaves the original TTS voice blank so the cell can sign its compiled
voice override. `SpekoSTTService` and `SpekoTTSService` in `speko_gateway.pipecat`
accept the same client and explicit managed configuration. Native admission's
`session.ready` frame checks readiness; these clients use no local health probe
and Pipecat's optional automatic provider fallback does not apply to them.
Close both models and then `await speech.aclose()` at the end of the call.

Relay-backed LiveKit STT requires `turn_vad`, the worker's local LiveKit VAD.
It must acknowledge contiguous unchanged audio in `INFERENCE_DONE.frames`
before its start/end events, as Silero does. The inference and audio sample
rates may differ. Only acknowledged audio reaches the relay; queued input
cannot overtake an earlier speech boundary. Missing VAD is refused before
admission, and stalled processing has a bounded input backlog.

The LiveKit stream stays open across turns. Each VAD turn uses a separate
native request, streaming audio and interim text while speech continues.
Turn ownership follows transcript-preserving `stt.MultiSpeakerAdapter` and
single-model `stt.FallbackAdapter` wrappers, including nested wrappers. An
ambiguous native failure fences their later stream and recovery calls too.
Whole-final speaker labels reach LiveKit's `speaker_id`; attributed segments
become individual timed finals, with explicit segment labels taking precedence.
Unlabeled segments inherit a whole-final label when present. Otherwise they
remain unattributed. The MultiSpeaker wrapper can use these labels and timings
for primary-speaker detection. Attributed segments must cover the final's full
text in order; invalid attribution refuses the result and fences automatic retries.
Speaker RMS lookup uses accepted audio positions, including after custom-node
filtering; emitted transcript timestamps keep their original input positions.
Multi-model fallback requires a control-plane recovery decision; wrappers
that rewrite or suppress transcripts are refused before native admission.
At speech end the request commits and closes; final transcript segments and
`END_OF_SPEECH` are emitted only after terminal usage arrives. A final segment
alone cannot complete a turn. `bind_session(session)` observes that session's
actual synchronous turn acceptance, including `skip_reply=True`, and follows
`update_agent()` handoffs and live `Agent.update_options(stt=...)` swaps. The
current activity has one turn owner; the old callback is restored before a new
model binds. The binding observes the Agent's input before its custom
`stt_node` consumes or buffers frames, keeping sample positions in the SDK
recognition pipeline's timeline. Filtered frames advance that timeline without
being sent to the provider. Buffered audio retains its original position, and
fresh input retains its configured endpointing delay even when a wrapper omits
its child's offset. Each input occurrence receives its own frame copy with the
original PCM and userdata; the producer's frames remain untouched. Custom DSP
that replaces a frame must preserve its userdata and duration. Repacketization
that loses the source position is refused. Public Agent lifecycle hooks remain
untouched; the owned processing-node and option-update hooks are restored on close.
Handoffs release inactive Agent hooks so the Agent can join another session;
the original session keeps its retained pipeline. A captured node stays inert
after its last native owner closes, including SDK restarts for local STT.
Duration-preserving DSP may deep-copy userdata without changing its source witness.
`bind_agent(agent)` locates the same session when its first stream starts.
The owned SDK activity interface is checked before admission and before each
turn's final transcript, including SDKs that retain a stream across handoff.
It is restored on close or handoff; the binding never changes the global SDK. A later speech
request waits for the framework's actual complete-turn commit. Binding is
required for native AgentSession streaming. Explicit event consumers can use
`stream(manual_turns=True)`; completed-buffer `recognize()` needs no binding
and joins all final segments. EOS timing excludes the detector's trailing
silence. Overlapping VAD prefixes exclude samples already assigned to
an earlier request, and request timestamps map to the persistent stream's
timeline. All PCM, including VAD prefixes and inference batches, is chunked to
fit the native frame bound. Pipecat
uses its pipeline's VAD and explicit commits.

Native speech disables LiveKit automatic retries for streaming STT, recognition,
streaming TTS and chunked synthesis, including when retry-enabled connection
options were supplied. Relay retry hints and timeouts cannot prove no provider
I/O occurred; a new attempt needs an explicit control-plane decision.
A failed native STT instance, including cancellation during admission before
`session.ready`, also refuses new streams and new turn admissions,
including LiveKit's outer AgentSession recovery loop. The worker must obtain
its recovery decision before constructing a replacement instance.

The client preserves binary PCM, stream commits, TTS cancellation and native
error codes. Live usage updates remain progress events; only a terminal frame
with final usage produces `usage.reported`. A lost terminal frame fails without
an automatic retry. These transport tests use owned protocol peers; wiring
worker claims through the actual cell and settling a full conversation remains
an integration gate. Short VAD pauses and a separately configured Session VAD
still need logical-turn coordination before managed worker activation.

For LiveKit Agents:

```python
from livekit.plugins import openai

from speko_gateway.livekit import STT, TTS

session = AgentSession(
    stt=STT(
        language="en",         # default
        provider="auto",       # default; set for BYOK when several STT keys exist
        model="auto",          # default: the provider's catalog default
        credential_source="auto", # or "byok" / "managed"
        sample_rate=16_000,    # default
    ),
    llm=openai.LLM(model="gpt-4.1-mini"),  # any LiveKit LLM plugin
    tts=TTS(
        provider="auto",       # default: the configured BYOK vendor, or the managed plan's pick
        model="auto",          # default: the provider's catalog default
        voice="",              # default: request, configured fallback, or catalog default
        language="en",         # default
        sample_rate=24_000,    # default
        credential_source="auto", # or "byok" / "managed"
    ),
)
```

`TTS(provider_options=...)` passes a vendor's own synthesis settings, for
example `{"cartesia": {"normalization": "en-IN"}}` to read numbers and dates
the Indian-English way in a Hindi session, or `{"elevenlabs": {"stability":
0.4, "similarity_boost": 0.8, "style": 0.2}}`. `update_options(provider_options=...)`
changes them for the next reply. The Gateway refuses a setting the routed
model cannot honor before the session opens.

`STT()` and `TTS()` read `SPEKO_SOCKET_PATH` and `SPEKO_LOCAL_AUTH_TOKEN`.
`credential_source="auto"` preserves the original behavior: managed when
`SPEKO_API_KEY` is present and local BYOK otherwise. Set `"byok"` explicitly
to use local provider credentials while retaining a Speko API key for LLM or
other managed traffic.

The LiveKit classes support `livekit-agents` 1.4.5 and later. Releases before
1.6.5 have no keyterm capability flag, so `STT(keywords=...)` still sends the
keywords to the provider but does not advertise keyterm support to LiveKit.
From 1.8.9, `STT(provider="soniox")` lets LiveKit flush the stream when the
user stops speaking (VAD end of speech, a turn detector's end-of-turn verdict,
or `commit_user_turn`). The Gateway turns the flush into Soniox's `finalize`,
so the final transcript arrives a few hundred milliseconds later instead of
waiting for Soniox's own endpointer (up to 2 s by default). Other providers
and `provider="auto"` keep waiting for the provider's endpointer.
Releases before 1.6.10 import `httpx` without declaring it, and openai 3 no
longer brings it in; this package depends on `httpx`, so any install of it,
with or without the `livekit` extra, works without an `openai` pin.

With a Speko API key the LLM can come from Speko too, served by the hosted
relay:

```python
from speko_gateway.livekit import LLM, STT, TTS

session = AgentSession(
    stt=STT(),
    llm=LLM(
        provider="auto",          # default: relay picks; set with model= to pin a route
        model="auto",             # default: GET router.speko.dev/v1/models lists the options
        objective="balanced",     # default: or "quality", "latency", "cost"
        max_output_tokens=8_192,  # default
        reasoning_effort=None,    # default: provider default; or "none" ... "xhigh"
    ),
    tts=TTS(),
)
```

`reasoning_effort` is the Router's reasoning control. Use `"none"` to turn
reasoning off. `"minimal"` asks for the least reasoning the routed model allows
(the Router maps it onto each provider's own scale), and `"low"`, `"medium"`,
`"high"` and `"xhigh"` ask for more. Unset, no field is sent and the provider
default applies. Any other value raises `ValueError` when the LLM is
constructed. While reasoning is enabled (any value but `"none"`) the Router
refuses `temperature` and `top_p`.

`"none"` is accepted on models that cannot reason, where it changes nothing,
except on older Routers, which refuse it there (e.g. on `gpt-4.1`). Four models
always reason and refuse `"none"`; `"minimal"` is their floor: Inception
`mercury-voice`, `gpt-oss-120b` on Cerebras and Together, and Together
`zai-org/GLM-5.3-Flash`. Every other value, `"minimal"` included, needs a model
that can reason: a pinned model that cannot is refused
(`capability_unsupported`), and auto routing considers only models that can.

`LLM()` requires `SPEKO_API_KEY` (plus optional `SPEKO_ROUTER_URL`; the old
`SPEKO_RELAY_URL` remains a fallback) and speaks HTTPS directly to
`router.speko.dev`, not the local socket — so unlike the
provider-direct voice legs, the conversation history travels through the
Speko Router. Function tools are supported; image and audio content is
silently skipped — only text is forwarded.

Jev evaluations use the separate pooled async client and require an explicit
idempotency key:

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

Run `python examples/jev_voice_router.py --fake` for the framework-independent
final-transcript example. It batches Choice, Noul, and Score, applies a 500 ms
application deadline, cancels superseded turns, and dispatches only allowlisted
read-only handlers. Its confidence thresholds are example defaults that must be
evaluated on your own traffic.

For Pipecat 1.7+:

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

Use a Pipecat VAD in the pipeline so `VADUserStoppedSpeakingFrame` commits the
current Gateway STT utterance. The complete container and Pipecat Cloud setup
is in the [Pipecat integration guide](../../docs/PIPECAT.md).
