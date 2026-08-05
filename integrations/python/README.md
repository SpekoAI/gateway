# Speko Gateway Python integration

This package connects Python voice-agent frameworks to the authenticated local
Speko Gateway socket. It does not read Speko API keys or provider credentials.

For LiveKit Agents:

```python
from speko_gateway.livekit import STT

session = AgentSession(stt=STT())
```

`STT()` reads `SPEKO_SOCKET_PATH` and `SPEKO_LOCAL_AUTH_TOKEN`. Routing mode is
derived from `SPEKO_API_KEY`: managed when present, local BYOK when absent.
