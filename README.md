# Speko Gateway

Speko Gateway is the open customer-side runtime for real-time voice AI. It
gives agents one local streaming protocol across voice providers, keeps BYOK
credentials inside your process, and can optionally use Speko for managed
routing, observability, and consolidated billing.

> Early preview: the protocol is versioned, but breaking changes may occur
> before the first stable release.

## Add Gateway to a LiveKit agent

The public image contains both the Gateway binary and its Python integration.
Apply this diff to the official
[LiveKit Python agent starter](https://github.com/livekit-examples/agent-starter-python)
Dockerfile:

```diff
 ARG PYTHON_VERSION=3.14
+FROM spekoai/gateway:latest AS speko-gateway
 FROM ghcr.io/astral-sh/uv:python${PYTHON_VERSION}-bookworm-slim AS base

 FROM base AS build
 WORKDIR /app
 COPY pyproject.toml uv.lock ./
 RUN mkdir -p src
 RUN uv sync --locked
+COPY --from=speko-gateway /opt/speko/python /opt/speko/python
+RUN uv pip install --python /app/.venv/bin/python /opt/speko/python
 RUN uv run --module livekit.agents download-files
 COPY . .

 FROM base
 ARG UID=10001
 RUN adduser \
     --disabled-password \
     --gecos "" \
     --home "/app" \
     --shell "/sbin/nologin" \
     --uid "${UID}" \
     appuser
 COPY --from=build --chown=appuser:appuser /app /app
+COPY --from=speko-gateway /usr/local/bin/speko-gateway /usr/local/bin/speko-gateway
+RUN install -d -o appuser -g appuser -m 0700 /run/speko
 WORKDIR /app
 USER appuser
-CMD ["uv", "run", "src/agent.py", "start"]
+CMD ["sh", "-c", "/usr/local/bin/speko-gateway & exec uv run src/agent.py start"]
```

Then select Speko for the voice legs where the agent creates its
`AgentSession`:

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

With a Speko API key the LLM can come from Speko too, served by the hosted
relay (`relay.speko.dev`):

```python
from speko_gateway.livekit import LLM, STT, TTS

session = AgentSession(
    stt=STT(),
    llm=LLM(
        provider="auto",          # default: relay picks; set with model= to pin a route
        model="auto",             # default: GET relay.speko.dev/v1/models lists the options
        objective="balanced",     # default: or "quality", "latency", "cost"
        max_output_tokens=8_192,  # default
    ),
    tts=TTS(),
)
```

`LLM()` requires `SPEKO_API_KEY` and speaks HTTPS directly to the relay —
not the local socket — under the public [`relayapi`](relayapi/doc.go)
contract. That crosses a different trust boundary: unlike the
provider-direct voice legs, the conversation history travels through the
Speko relay.

Optionally, attach the conversation profiler probe to correlate STT, LLM,
TTS, and playback into per-turn latency traces. It emits content-free timing
markers only (see [TRUST.md](docs/TRUST.md#conversation-turn-markers)), never
raises into the agent, and is suppressed by `SPEKO_TELEMETRY_DISABLED=true`:

```python
from speko_gateway.probe import ConversationProbe

session = AgentSession(stt=STT(), llm=LLM(), tts=TTS())
probe = ConversationProbe(session)
probe.start()                  # before session.start()

await session.start(...)
...
await probe.aclose()           # during shutdown
```

Set a local token plus one credential choice:

```bash
# Speko-managed routing, observability, and consolidated billing
lk agent update-secrets \
  --secrets "SPEKO_LOCAL_AUTH_TOKEN=$(openssl rand -hex 32)" \
  --secrets "SPEKO_API_KEY=your-speko-api-key"

# Or BYOK: omit SPEKO_API_KEY and use your provider key
lk agent update-secrets \
  --secrets "SPEKO_LOCAL_AUTH_TOKEN=$(openssl rand -hex 32)" \
  --secrets "SPEKO_DEEPGRAM_BYOK_API_KEY=your-deepgram-key"
```

Both choices use the same local socket and LiveKit integration. With BYOK,
provider credentials stay in the Gateway process and anonymous telemetry
remains on unless explicitly disabled. The local image serves
provider-direct routes; the hosted Speko relay (`relay.speko.dev`) is a
separate, generally available Speko-operated surface whose public wire
contract lives in this repository's `relayapi` package (OpenAPI + AsyncAPI).
Any Speko organization can route relay requests to any model in the
published catalog.

Because the LiveKit agent and Gateway share one container in this setup, the
agent process can inspect the container environment. Use separate containers
when process-level credential isolation is required.

## Anonymous telemetry and opt-out

Speko Gateway sends anonymous, content-free usage telemetry by default,
including when only BYOK is configured. Without `SPEKO_API_KEY`, telemetry is
unauthenticated and is not associated with a Speko account. Disable it with:

```bash
SPEKO_TELEMETRY_DISABLED=true
```

Telemetry contains session lifecycle markers, timings, error classifications,
and provider request correlation IDs. It never contains audio, transcripts,
prompts, generated text, tool names/arguments/results, or credentials.

When Speko supplies the provider credential, minimum usage and terminal
records are still required for consolidated billing even if optional telemetry
is disabled. The exact payloads and trust boundaries are documented in
[Trust and data flow](docs/TRUST.md).

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `SPEKO_LOCAL_AUTH_TOKEN` | required | Local API bearer token |
| `SPEKO_DEEPGRAM_BYOK_API_KEY` | unset | Local Deepgram key |
| `SPEKO_ELEVENLABS_BYOK_API_KEY` | unset | Local ElevenLabs key |
| `SPEKO_CARTESIA_BYOK_API_KEY` | unset | Local Cartesia key |
| `SPEKO_API_KEY` | unset | Enables Speko-managed routing and billing |
| `SPEKO_TELEMETRY_DISABLED` | `false` | Opts out of telemetry |
| `SPEKO_SOCKET_PATH` | `/run/speko/runtime.sock` | Absolute Unix socket path |
| `SPEKO_LOCAL_MAX_SESSION_DURATION` | `24h` | Local BYOK session ceiling |
| `SPEKO_CONTROL_PLANE_URL` | `https://gateway.speko.dev` | Speko control plane |
| `SPEKO_JWKS_URL` | `<control-plane>/.well-known/jwks.json` | Signing keys |
| `SPEKO_PLAN_ISSUER` | control-plane URL | Required plan issuer |
| `SPEKO_PLAN_AUDIENCE` | `speko-runtime` | Required plan audience |
| `SPEKO_RUNTIME_INSTANCE_ID` | hostname | Non-secret process identity |
| `SPEKO_WORKLOAD_TYPE` | `agent` when an ID is set | Dashboard workload category |
| `SPEKO_WORKLOAD_ID` | unset | Stable Agent or custom workload ID |
| `SPEKO_MAX_SESSIONS` | `100` | Per-process session capacity |
| `SPEKO_INSTANCE_HEARTBEAT_INTERVAL` | `20s` | Hosted dashboard worker heartbeat interval |
| `SPEKO_WARM_PLAN_TARGET` | `4` | Prefetched session plans kept per route; `0` disables prefetching |
| `SPEKO_WARM_ROUTES` | unset | Routes to warm at startup, `kind:provider[:model[:language]]`, comma-separated |
| `SPEKO_WARM_TTS_MAX_CHARACTERS` | `100000` | Character allowance requested for warmed TTS routes |

## Zero-overhead session setup

With Speko-managed routing, the Gateway keeps a small pool of signed session
plans warm in the background. Creating a session takes one out of memory and
dials the provider immediately, so using the Gateway costs the provider
handshake and nothing else — no control-plane round trip on the path a caller
waits on.

The pool learns route shapes from traffic, so it warms itself after the first
session of each shape. `SPEKO_WARM_ROUTES` covers the one case that cannot
learn: the first session after a deploy or a scale-up, which is the one a real
person is waiting on.

```bash
SPEKO_WARM_ROUTES=stt:deepgram:nova-3:en,tts:elevenlabs::en
```

A miss — a cold process, an unseen route shape, an unreachable control plane —
falls through to a synchronous plan request. Nothing fails that would not have
failed before; it is only slower. `GET /metrics` reports
`speko_gateway_warm_plan_hits_total` and `speko_gateway_warm_plan_misses_total`;
a miss rate that does not fall toward zero after warm-up means prefetching is
not working.

Prefetching does not apply to BYOK, where plans are signed inside this process
and already cost nothing.

Every secret also supports an exclusive `*_FILE` form for Docker and
Kubernetes secrets—for example,
`SPEKO_API_KEY_FILE=/run/secrets/speko_api_key`.

## Included provider adapters

| Capability | Provider | Adapter | Default model |
| --- | --- | --- | --- |
| STT | Deepgram | `deepgram.stt.v1` | `nova-3` |
| TTS | Deepgram | `deepgram.tts.v1` | `aura-2-thalia-en` |
| TTS | ElevenLabs | `elevenlabs.tts.v1` | `eleven_flash_v2_5` |
| TTS | Cartesia | `cartesia.tts.v1` | `sonic-3` |
| STT | Cartesia | `cartesia.stt.v1` | `ink-2` |

Provider endpoints are checked against exact official host allowlists before
credentials are attached. Production connections require TLS and port 443.

## What is open

This repository contains the complete customer-side gateway: local HTTP and
WebSocket service, protocol and schema, plan verification, BYOK injection,
provider adapters, bounded telemetry exporter, tests, and container build.

Speko's hosted control plane, credential broker, billing systems, databases,
and infrastructure are separate and are not included.

## Build

Go 1.26 or newer is required.

```bash
make check
make build
docker build -t spekoai/gateway:dev .
```

See [SECURITY.md](SECURITY.md) to report vulnerabilities and
[CONTRIBUTING.md](CONTRIBUTING.md) to contribute. Speko Gateway is released
under the [MIT License](LICENSE) and follows the
[Code of Conduct](CODE_OF_CONDUCT.md).
