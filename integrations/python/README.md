# Speko Gateway Python integration

This package connects Python voice-agent frameworks to the authenticated local
Speko Gateway socket, and optionally to the hosted Speko relay for LLM. The
voice classes read no Speko API key or provider credentials; only the relay
`LLM` class authenticates with `SPEKO_API_KEY`.

For LiveKit Agents:

```python
from livekit.plugins import openai

from speko_gateway.livekit import STT, TTS

session = AgentSession(
    stt=STT(
        language="en",         # default
        model="nova-3",        # default
        sample_rate=16_000,    # default
    ),
    llm=openai.LLM(model="gpt-4.1-mini"),  # any LiveKit LLM plugin
    tts=TTS(
        provider="auto",       # default: the configured BYOK vendor, or the managed plan's pick
        model="auto",          # default: the provider's catalog default
        voice="",              # default: plan- or catalog-supplied; BYOK ElevenLabs/Cartesia need one
        language="en",         # default
        sample_rate=24_000,    # default
    ),
)
```

`STT()` and `TTS()` read `SPEKO_SOCKET_PATH` and `SPEKO_LOCAL_AUTH_TOKEN`.
Routing mode is derived from `SPEKO_API_KEY`: managed when present, local BYOK
when absent.

With a Speko API key the LLM can come from Speko too, served by the hosted
relay:

```python
from speko_gateway.livekit import LLM, STT, TTS

session = AgentSession(
    stt=STT(),
    llm=LLM(),  # hosted Speko relay picks a routable model
    tts=TTS(),
)
```

`LLM()` requires `SPEKO_API_KEY` (plus optional `SPEKO_RELAY_URL`) and speaks
HTTPS directly to `relay.speko.dev`, not the local socket — so unlike the
provider-direct voice legs, the conversation history travels through the
Speko relay. Routing defaults to auto/balanced; `LLM(provider=..., model=...)`
pins an explicit route and `objective=` accepts `quality`, `latency`, or
`cost`. Function tools are supported; image and audio content is silently
skipped — only text is forwarded.
