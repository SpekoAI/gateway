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

`STT()` and `TTS()` read `SPEKO_SOCKET_PATH` and `SPEKO_LOCAL_AUTH_TOKEN`.
`credential_source="auto"` preserves the original behavior: managed when
`SPEKO_API_KEY` is present and local BYOK otherwise. Set `"byok"` explicitly
to use local provider credentials while retaining a Speko API key for LLM or
other managed traffic.

The LiveKit classes support `livekit-agents` 1.4.5 and later. Releases before
1.6.5 have no keyterm capability flag, so `STT(keywords=...)` still sends the
keywords to the provider but does not advertise keyterm support to LiveKit.
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
