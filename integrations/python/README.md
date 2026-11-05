# Speko Gateway Python integration

This package connects Python voice-agent frameworks to the authenticated local
Speko Gateway socket, and optionally to the hosted Speko Router for LLM and
typed evaluation. The
voice classes read no Speko API key or provider credentials; only the relay
hosted clients authenticate with `SPEKO_API_KEY`.

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

`TTS` keeps a Gateway session open in standby, so a reply's first text goes
out without waiting for a session to open. Opening a session also connects to
the provider, and some providers hold the first audio until that setup
finishes. Managed ElevenLabs, for example, checks its token only after the
WebSocket upgrade. LiveKit calls `prewarm()` when the agent starts, which
opens the standby, and each reply opens the next standby when it takes the
current one. A reply opens its own session at its first text, as before, when
the standby has other settings, has ended, or failed to open. `update_options`
replaces a standby that has the old settings, so an agent that changes
language every turn opens one unused session per change. A standby that no
reply takes within 3 minutes closes, and the next reply opens its own session
and a new standby. LiveKit does not close the `TTS` when the session ends, so
call `aclose()` to close the standby sooner. Between replies, each `TTS` holds
one open Gateway session and provider connection, and during a reply it holds
two. Both count toward your organization's concurrent-session limit and the
provider's connection limits. With managed credentials, Speko reserves credit
for each open session's `max_input_characters`, so the standby holds a second
reservation. Lower `max_input_characters` to reserve less. Set
`standby_sessions` to keep more standbys, or to `0` to open each session at
its reply's first text.

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

### Prepared admissions

Each Router LLM request waits for an admission round trip (credit check and a
signed plan) before the model is dialed: about 270 ms from Singapore. With
`prepare_admissions=True`, the LLM makes that round trip when the caller starts
speaking, and the reply's request skips it:

```python
llm = LLM(prepare_admissions=True)
session = AgentSession(stt=STT(), llm=llm, tts=TTS())
llm.attach(session)  # prepare on each user speech start
await session.start(agent=MyAgent(), room=ctx.room)
```

On each user speech start, the LLM prepares from the current agent's chat
context and the tools of the last request (`POST /v1/llm/admissions`). The
prepare call's `Idempotency-Key` is the one the real request will use: the
Router binds the ticket to it. The next request presents the ticket with that
same key, and the ticket is then spent. Every request without a ticket sends a
fresh key. That includes a retry of a request that presented one, which goes
the ordinary way. A tool-call
follow-up and any request with no live ticket are admitted the ordinary way.
There is at most one ticket per LLM. A false start keeps a ticket with more
than a minute left. An older ticket is released, and the new prepare waits for
that release. Without an `AgentSession`, call `llm.prepare(chat_ctx)` yourself
when the caller starts speaking.

An unused ticket holds one organization concurrency slot, so the LLM never lets
a ticket compete with a real request for one:

- A request waits up to 3 s for a prepare still in flight. Past that it goes
  the ordinary way, and the prepare releases its ticket when it lands.
- A ticket the Router did not use is released as soon as the response headers
  arrive.
- A ticket too close to expiry is released, with a wait of up to 1 s, before
  the ordinary request goes out.
- The session closing, the job shutting down, or `aclose()` releases the
  unused ticket, including one whose prepare is still in flight. `aclose()`
  waits up to 3 s and never cancels a prepare. A prepare has no timeout of
  its own beyond the transport's request timeout, so a late ticket is never
  lost. An LLM that owns its transport keeps it open until that ticket is
  released or the transport's timeout fires.

`llm.prepared_admission_outcome` (and `prepared_admission_outcome` on each
stream `llm.chat()` returns) says what the Router did with the last request's
ticket. `"used"` means the request skipped the round trip. `"invalid"`,
`"expired"`, `"stale"`, `"route"`, `"budget"` or `"redialed"` mean it was
admitted the ordinary way. `""` means no outcome arrived: no response, or a
request handed off to another region, which does not carry the ticket on.
`None` means no ticket was presented. Any outcome but `"used"` releases the
ticket. A ticket never fails a request, and a failed prepare or
release never raises into the agent.

An unused ticket holds a credit reservation and one organization concurrency
slot until it is used, released, or expires (5 minutes). The option is off by
default and needs a Router that serves `/v1/llm/admissions`. On an older Router,
the first prepare gets a 404, the LLM stops preparing, and requests go the
ordinary way.

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
