# Speko Gateway

Speko Gateway is an open-source sidecar for real-time voice AI. A voice agent
talks to it over a local Unix socket with one streaming protocol, and the
Gateway connects to the speech-to-text (STT), text-to-speech (TTS) or
speech-to-speech provider. It runs with your own provider keys (BYOK), with a
Speko API key for managed routing and consolidated billing, or with both.

This is an early preview. The protocol is versioned, but breaking changes may
occur before the first stable release.

## How it works

- The Gateway listens on an owner-only Unix socket and opens no TCP port.
  Every call needs the local bearer token `SPEKO_LOCAL_AUTH_TOKEN`.
- The agent creates a session, then streams audio or text over a WebSocket.
  On a `provider_direct` route the Gateway dials the provider itself, and
  audio and text do not pass through Speko. Routes served by the Speko Router
  do pass through it: managed GPT-Live, [native relay mode](#native-relay-mode)
  and the Python LLM clients.
- With BYOK, the Gateway plans the session in process. The provider key is
  sent only to that provider.
- With `SPEKO_API_KEY`, the Gateway asks the Speko control plane for a signed,
  short-lived session plan that names the provider and carries a short-lived
  provider credential. The Gateway verifies the plan before it accepts media.
- Content-free usage telemetry is on by default. See
  [Telemetry](#telemetry).

The wire format is in [docs/PROTOCOL.md](docs/PROTOCOL.md). What the Gateway
can see and where it sends data is in [docs/TRUST.md](docs/TRUST.md).

## Quick start

Start the Gateway with one provider key. This example uses Deepgram:

```bash
export SPEKO_LOCAL_AUTH_TOKEN="$(openssl rand -hex 32)"
export SPEKO_DEEPGRAM_BYOK_API_KEY=your-deepgram-key
mkdir -m 700 run
docker run -d --rm --name speko-gateway \
  --user "$(id -u):$(id -g)" -v "$PWD/run:/run/speko" \
  -e SPEKO_LOCAL_AUTH_TOKEN -e SPEKO_DEEPGRAM_BYOK_API_KEY \
  spekoai/gateway:0.1
```

Check that it is ready and see what it can open:

```bash
curl --unix-socket run/runtime.sock http://localhost/readyz
curl --unix-socket run/runtime.sock "http://localhost/v1/models?provider=deepgram"
```

In the same shell, install the Python client from a checkout of this
repository and synthesize a sentence:

```bash
pip install ./integrations/python
export SPEKO_SOCKET_PATH="$PWD/run/runtime.sock"
python hello.py
docker stop speko-gateway
```

```python
# hello.py: writes 24 kHz mono 16-bit PCM to hello.pcm
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

To run a long-lived Gateway beside an agent container, use
[`deploy/docker-compose.yml`](deploy/docker-compose.yml). It puts the socket
in the `speko-runtime` volume, which the agent container mounts at
`/run/speko` and reads as UID 65532:

```bash
docker compose --env-file .env -f deploy/docker-compose.yml up -d
```

[`.env.example`](.env.example) lists the variables it reads.

## Use it in a voice agent

The Python package `speko-gateway` in
[`integrations/python`](integrations/python/README.md) has LiveKit Agents and
Pipecat classes. The Gateway image carries the binary and the package, so an
agent image can copy both.

For the
[LiveKit Python agent starter](https://github.com/livekit-examples/agent-starter-python),
add these lines to its Dockerfile:

```dockerfile
FROM spekoai/gateway:0.1 AS speko-gateway

# Build stage, after `uv sync`:
COPY --from=speko-gateway /opt/speko/python /opt/speko/python
RUN uv pip install --python /app/.venv/bin/python /opt/speko/python

# Final stage, before `USER appuser`:
COPY --from=speko-gateway /usr/local/bin/speko-gateway /usr/local/bin/speko-gateway
RUN install -d -o appuser -g appuser -m 0700 /run/speko

# Replace the final CMD:
CMD ["sh", "-c", "/usr/local/bin/speko-gateway & exec uv run src/agent.py start"]
```

Then use Speko for the voice legs where the agent creates its session:

```python
from livekit.plugins import openai

from speko_gateway.livekit import STT, TTS

session = AgentSession(
    stt=STT(),                             # language="en", provider="auto"
    llm=openai.LLM(model="gpt-4.1-mini"),  # any LiveKit LLM plugin
    tts=TTS(),
)
```

Set `SPEKO_LOCAL_AUTH_TOKEN` and either `SPEKO_API_KEY` or one BYOK key as
secrets of the agent. The agent and the Gateway share one container here, so
the agent process can read the Gateway's environment. Use separate containers
when the provider keys must be hidden from the agent.

For Pipecat, follow [docs/PIPECAT.md](docs/PIPECAT.md). Options, the Speko
LLM class, realtime models and the conversation profiler are in the
[Python package README](integrations/python/README.md).

## Configuration

The Gateway reads environment variables only. Every secret also has an
exclusive `_FILE` form, for example
`SPEKO_API_KEY_FILE=/run/secrets/speko_api_key`. A provider credential file
is read again for every new session, so a short-lived token can be rotated
without a restart.

| Variable | Default | Meaning |
| --- | --- | --- |
| `SPEKO_LOCAL_AUTH_TOKEN` | required | Bearer token for the local socket |
| `SPEKO_API_KEY` | unset | Speko API key. Turns on managed routing and billing. Requests that ask for BYOK stay local |
| `SPEKO_<PROVIDER>_BYOK_API_KEY` | unset | Provider key. Without `SPEKO_API_KEY` at least one is required |
| `SPEKO_<PROVIDER>_BYOK_TTS_VOICE` | catalog default | Fallback voice for BYOK TTS. A voice in the request wins |
| `SPEKO_SOCKET_PATH` | `/run/speko/runtime.sock` | Absolute Unix socket path |
| `SPEKO_MAX_SESSIONS` | `100` | Concurrent sessions in this process |
| `SPEKO_LOCAL_MAX_SESSION_DURATION` | `24h` | Longest BYOK session |
| `SPEKO_TELEMETRY_DISABLED` | `false` | `true` turns optional telemetry off |
| `SPEKO_GATEWAY_REGION` | unset | Cloud region the Gateway runs in, such as `ap-northeast-1`. Managed routing uses it to pick a nearby provider deployment |
| `SPEKO_RUNTIME_INSTANCE_ID` | hostname | Non-secret name of this process in reports to Speko |
| `SPEKO_WORKLOAD_ID` | unset | Stable ID of the agent or service that uses this Gateway |
| `SPEKO_WORKLOAD_TYPE` | `agent` when an ID is set | Workload category in the Speko dashboard |
| `SPEKO_INSTANCE_HEARTBEAT_INTERVAL` | `20s` | How often a managed Gateway reports its session counts |
| `SPEKO_WARM_PLAN_TARGET` | `4` | Session plans kept ready per route. `0` turns prefetching off |
| `SPEKO_WARM_ROUTES` | unset | Routes to prefetch at startup |
| `SPEKO_WARM_TTS_MAX_CHARACTERS` | `100000` | Character allowance of prefetched TTS plans |
| `SPEKO_CONTROL_PLANE_URL` | `https://gateway.speko.dev` | Speko control plane |
| `SPEKO_JWKS_URL` | `<control plane>/.well-known/jwks.json` | Keys that sign session plans |
| `SPEKO_PLAN_ISSUER` | control-plane URL | Required plan issuer |
| `SPEKO_PLAN_AUDIENCE` | `speko-runtime` | Required plan audience |
| `SPEKO_LIVE_RELAY_HOSTS` | unset | Extra Speko Router hostnames allowed for managed GPT-Live, comma-separated. `router.speko.dev` is always allowed |

### Providers

| Kind | `provider` names |
| --- | --- |
| STT | `alibaba`, `assemblyai`, `cartesia`, `deepgram`, `elevenlabs`, `gemini`, `gladia`, `gradium`, `hamsa`, `hopper`, `inworld`, `meta`, `modulate`, `munsit`, `nari`, `omi`, `openai`, `palabra`, `smallest`, `soniox`, `speechmatics`, `xai` |
| TTS | `alibaba`, `cartesia`, `deepgram`, `elevenlabs`, `fish`, `gemini`, `gradium`, `hamsa`, `hopper`, `hume`, `inworld`, `maya`, `minimax`, `munsit`, `nari`, `openai`, `palabra`, `paxa`, `rime`, `smallest`, `soniox`, `speechify`, `xai` |
| Realtime | `google`, `openai`, `xai` |

- The BYOK variable is the name in upper case:
  `SPEKO_DEEPGRAM_BYOK_API_KEY`. The one exception is
  `SPEKO_GOOGLE_BYOK_ACCESS_TOKEN`.
- `google` is Gemini Live and takes a bearer access token. Use the `_FILE`
  form so a token refresher can rotate it. `gemini` is the separate AI Studio
  speech API and takes an API key.
- `inworld` takes the Base64 `key:secret` portal credential. `fish` takes a
  team API key.
- `GET /v1/models` lists every model. Default models and endpoints are in
  `gateway/catalog.go`.
- The Gateway checks each provider endpoint against an allow-list of the
  provider's official hosts before it attaches a credential. Provider
  connections use TLS on port 443.

### Prefetched session plans

With `SPEKO_API_KEY`, the Gateway keeps a few signed session plans per route
in memory, so opening a session needs no control-plane round trip. It learns
routes from traffic. `SPEKO_WARM_ROUTES` covers the first session after a
start, as `kind:provider[:model[:language]]` entries:

```bash
SPEKO_WARM_ROUTES=stt:deepgram:nova-3:en,tts:elevenlabs::en
```

When no plan is ready, the Gateway asks the control plane during session
creation. `GET /metrics` reports `speko_gateway_warm_plan_hits_total` and
`speko_gateway_warm_plan_misses_total`. BYOK sessions are never prefetched.

## Native relay mode

`speko-gateway native` is a second mode, used by agents that Speko runs. It
forwards STT, TTS, LLM and realtime requests from the local socket to one
Speko Router origin. It holds no provider key and no process-wide Speko key:
each request carries its own session credential. The local API is in
[docs/PROTOCOL.md](docs/PROTOCOL.md#native-relay-mode), and a container
template is in
[`deploy/docker-compose.native.yml`](deploy/docker-compose.native.yml).

| Variable | Default | Meaning |
| --- | --- | --- |
| `SPEKO_LOCAL_AUTH_TOKEN` | required | Bearer token for the local socket |
| `SPEKO_RELAY_URL` | required | HTTPS origin of the Router |
| `SPEKO_SOCKET_PATH` | `/run/speko/runtime.sock` | Absolute Unix socket path |
| `SPEKO_MAX_SESSIONS` | `100` | Concurrent requests |
| `SPEKO_NATIVE_MAX_REQUEST_BYTES` | `134217728` | Largest HTTP request body |
| `SPEKO_NATIVE_SETUP_TIMEOUT` | `30s` | Limit for connecting to the Router and for a stalled upload |
| `SPEKO_NATIVE_WRITE_TIMEOUT` | `15s` | Limit for one write to the local client |

- The socket directory must belong to the process user with mode 0700.
- The Gateway keeps a `runtime.sock.lock` file beside the socket. Leave it in
  place: after a crash, the next process uses it to take over the old socket.
  A socket that the lock file does not record is refused and needs manual
  cleanup.

## Telemetry

The Gateway sends anonymous, content-free usage telemetry by default, also
when only BYOK keys are set. Turn it off with:

```bash
SPEKO_TELEMETRY_DISABLED=true
```

Telemetry holds session lifecycle events, timings, error classes and provider
request IDs. It never holds audio, transcripts, prompts, generated text, tool
names, arguments or results, or credentials. When Speko supplies the provider
credential, the usage and end-of-session records needed for billing are still
sent with telemetry off. [docs/TRUST.md](docs/TRUST.md) lists every payload.

## Versions

Pin a released version in production. Each release has notes on the
[releases page](https://github.com/SpekoAI/gateway/releases).

| Image tag | What it is | Use it for |
| --- | --- | --- |
| `spekoai/gateway:0.1.0` | One exact release. It never changes | Production, when you choose every upgrade |
| `spekoai/gateway:0.1` | The newest `0.1.x` release. Gets fixes only | Production, when you want fixes automatically |
| `latest`, `edge`, `sha-<commit>` | Every merge to `main` | Testing unreleased changes |

`speko-gateway version` prints the version of a binary.

## Repository layout

| Path | Contents |
| --- | --- |
| `cmd/speko-gateway` | The command: configuration, startup, shutdown |
| `gateway` | Local HTTP and WebSocket server, BYOK planner, provider catalog, native relay mode |
| `runtime` | Session engine, plan verification, telemetry exporter |
| `protocol` | Wire types and the session-plan JSON schema |
| `providers` | One adapter package per provider |
| `controlplane` | Client for the Speko control plane, and its API notes |
| `relayapi` | Public wire contract of the hosted Speko Router (OpenAPI and AsyncAPI) |
| `integrations/python` | Python package for LiveKit Agents and Pipecat |
| `deploy` | Docker Compose templates |

Speko's hosted control plane, Router, billing and infrastructure are separate
services and are not in this repository.

## Build and test

Go 1.26 or newer. The Python checks need [uv](https://docs.astral.sh/uv/).

```bash
make build         # bin/speko-gateway
make test          # go test ./...
make check         # go vet, tests, race tests, Python lint and tests
docker build -t spekoai/gateway:dev .
```

## More documentation

| Document | Subject |
| --- | --- |
| [docs/PROTOCOL.md](docs/PROTOCOL.md) | Local HTTP and WebSocket protocol, signed plans |
| [docs/TRUST.md](docs/TRUST.md) | Trust boundaries, telemetry payloads, credential handling |
| [docs/PIPECAT.md](docs/PIPECAT.md) | Pipecat setup |
| [integrations/python/README.md](integrations/python/README.md) | Python package |
| [controlplane/renewals.md](controlplane/renewals.md) | Funded Google session renewal |
| [controlplane/instances-v2.md](controlplane/instances-v2.md), [instances-v3.md](controlplane/instances-v3.md) | Process reports to the control plane |
| [docs/CARTESIA_BYTES_TTS.md](docs/CARTESIA_BYTES_TTS.md) | Cartesia whole-input TTS adapter for Go embedders |
| [docs/DEEPGRAM-CAPACITY-EVIDENCE.md](docs/DEEPGRAM-CAPACITY-EVIDENCE.md) | Detecting Deepgram rate-limit refusals in Go |
| [docs/RELEASING.md](docs/RELEASING.md) | Cutting a release |

Report vulnerabilities as described in [SECURITY.md](SECURITY.md). To
contribute, read [CONTRIBUTING.md](CONTRIBUTING.md) and the
[Code of Conduct](CODE_OF_CONDUCT.md). Speko Gateway is released under the
[MIT License](LICENSE).
