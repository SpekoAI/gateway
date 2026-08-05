# Speko Gateway

Speko Gateway is the open customer-side runtime for real-time voice AI. It
gives agents one local streaming protocol across voice providers, keeps BYOK
credentials inside your process, and can optionally use Speko for managed
routing, observability, and consolidated billing.

> Early preview: the protocol is versioned, but breaking changes may occur
> before the first stable release.

## Add Gateway to a LiveKit agent

LiveKit Cloud builds your agent from its Dockerfile. To colocate Speko Gateway
with the official [Python agent starter](https://github.com/livekit-examples/agent-starter-python),
make these changes to the starter Dockerfile:

```diff
 # syntax=docker/dockerfile:1
+ARG PYTHON_VERSION=3.14
+FROM spekoai/gateway:latest AS speko-gateway

 FROM ghcr.io/astral-sh/uv:python${PYTHON_VERSION}-bookworm-slim AS base
 ...
 FROM base
 ...
 COPY --from=build --chown=appuser:appuser /app /app
+COPY --from=speko-gateway /usr/local/bin/speko-gateway /usr/local/bin/speko-gateway
+RUN install -d -o appuser -g appuser -m 0700 /run/speko
 ...
 USER appuser
-CMD ["uv", "run", "src/agent.py", "start"]
+CMD ["sh", "-c", "/usr/local/bin/speko-gateway & exec uv run src/agent.py start"]
```

The two processes share `/run/speko/runtime.sock` inside the container. Point
the LiveKit-side Speko adapter at that socket; it authenticates with
`SPEKO_LOCAL_AUTH_TOKEN`. The provider connection remains inside Gateway.

For Speko-managed routing and consolidated billing, add the two secrets to the
LiveKit deployment:

```bash
lk agent update-secrets \
  --secrets "SPEKO_LOCAL_AUTH_TOKEN=$(openssl rand -hex 32)" \
  --secrets "SPEKO_API_KEY=your-speko-api-key"
```

For BYOK, omit `SPEKO_API_KEY` and provide a provider key such as
`SPEKO_DEEPGRAM_BYOK_API_KEY` instead. Gateway adds that key in memory only
after verifying its locally signed routing plan.

This single-container layout is convenient for LiveKit Cloud, but both
processes share the container environment. Run Gateway in a separate container
when the agent process must not be able to inspect provider credentials.

The current image supports provider-direct routes; it does not yet implement
Speko relay. See [the protocol guide](docs/PROTOCOL.md) for the local API and
WebSocket contract.

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
| `SPEKO_CONTROL_PLANE_URL` | `https://gateway.speko.ai` | Speko control plane |
| `SPEKO_JWKS_URL` | `<control-plane>/.well-known/jwks.json` | Signing keys |
| `SPEKO_PLAN_ISSUER` | control-plane URL | Required plan issuer |
| `SPEKO_PLAN_AUDIENCE` | `speko-runtime` | Required plan audience |
| `SPEKO_RUNTIME_INSTANCE_ID` | hostname | Non-secret process identity |

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
