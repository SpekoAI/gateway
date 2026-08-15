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
        provider="auto",       # default; set when several BYOK STT keys exist
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

STT accepts transcription options in the same canonical vocabulary as the
hosted Speko API, plus a per-vendor passthrough for each provider's own
settings:

```python
stt = STT(
    provider="deepgram",             # pin provider AND model when an option is a requirement:
    model="nova-3",                  # deepgram's default is Flux, which has no diarization
    diarization=True,                # speaker labels (deepgram nova, assemblyai, soniox)
    keywords=["Speko", "Casey"],     # vocabulary biasing, every provider's own spelling
    noise_reduction=None,            # gladia audio enhancer, assemblyai voice focus
    provider_options={               # vendor-native settings, allow-listed per provider AND model
        "deepgram": {"numerals": True, "endpointing": 1200},
        "elevenlabs": {"vad_silence_threshold_secs": 0.7},
    },
)
```

Options fail closed: a session routed to a provider that cannot honor a
canonical ask is refused at create (`stt_option_unsupported`) with the option
named, never opened with the feature silently missing. With `provider="auto"`,
routing may pick a provider that must refuse the ask — pin the provider when
an option is a requirement. Settings under `provider_options` never narrow
routing; a provider the session does not reach simply ignores its entry, and a
setting outside a provider's allow-list is refused by name. Speaker labels
arrive on the raw vendor frames each event carries in `extensions`.

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

# BYOK. SPEKO_API_KEY may remain set for LLM/managed traffic when the voice
# classes use credential_source="byok".
lk agent update-secrets \
  --secrets "SPEKO_LOCAL_AUTH_TOKEN=$(openssl rand -hex 32)" \
  --secrets "SPEKO_DEEPGRAM_BYOK_API_KEY=your-deepgram-key"
```

Both choices can coexist through the same local socket and LiveKit integration.
With BYOK,
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
| `SPEKO_API_KEY` | unset | Enables managed routing, relay, and billing; explicit BYOK remains local |
| `SPEKO_GOOGLE_STT_ENDPOINT` | unset | Project-scoped Google Speech V2 `:recognize` URL required for Google STT |
| `SPEKO_<PROVIDER>_BYOK_TTS_VOICE` | catalog default | Operator-selected fallback voice; request voice wins |
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

Every catalog provider has a BYOK credential variable:

| Provider | Credential variable | Notes |
| --- | --- | --- |
| Alibaba | `SPEKO_ALIBABA_BYOK_API_KEY` | DashScope API key |
| AssemblyAI | `SPEKO_ASSEMBLYAI_BYOK_API_KEY` | API key |
| Cartesia | `SPEKO_CARTESIA_BYOK_API_KEY` | API key |
| Deepgram | `SPEKO_DEEPGRAM_BYOK_API_KEY` | API key |
| ElevenLabs | `SPEKO_ELEVENLABS_BYOK_API_KEY` | API key |
| Fish Audio | `SPEKO_FISH_BYOK_API_KEY` | Team API key; `/v1/tts/live` does not accept Fish agent session tokens |
| Gladia | `SPEKO_GLADIA_BYOK_API_KEY` | API key used for live-session initialization |
| Google | `SPEKO_GOOGLE_BYOK_ACCESS_TOKEN` | OAuth access token; prefer `_FILE` for rotation |
| Gradium | `SPEKO_GRADIUM_BYOK_API_KEY` | API key |
| Hume | `SPEKO_HUME_BYOK_API_KEY` | API key |
| Inworld | `SPEKO_INWORLD_BYOK_API_KEY` | Base64 portal credential (`key:secret`) |
| MiniMax | `SPEKO_MINIMAX_BYOK_API_KEY` | API key |
| OpenAI | `SPEKO_OPENAI_BYOK_API_KEY` | API key |
| Rime | `SPEKO_RIME_BYOK_API_KEY` | API key |
| Smallest | `SPEKO_SMALLEST_BYOK_API_KEY` | API key |
| Soniox | `SPEKO_SONIOX_BYOK_API_KEY` | API key |
| xAI | `SPEKO_XAI_BYOK_API_KEY` | API key |

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
Single-value provider credential files are reread for every new session, so an
external refresher can replace a short-lived Google OAuth token without
restarting the Gateway.

## Included provider adapters

| Capability | Provider | Adapter | Default model |
| --- | --- | --- | --- |
| STT | Deepgram | `deepgram.stt.v1` | `flux-general-en` |
| TTS | Deepgram | `deepgram.tts.v1` | `flux-haley-en` |
| STT | ElevenLabs | `elevenlabs.stt.v1` | `scribe_v2_realtime` |
| TTS | ElevenLabs | `elevenlabs.tts.v1` | `eleven_flash_v2_5` |
| STT | Cartesia | `cartesia.stt.v1` | `ink-2` |
| TTS | Cartesia | `cartesia.tts.v1` | `sonic-3` |
| STT | AssemblyAI | `assemblyai.stt.v1` | `universal-3-5-pro` |
| STT | Modulate | `modulate.stt.v1` | `velma-2-stt-streaming-english-v2` |
| STT | Gladia | `gladia.stt.v1` | `solaria-1` |
| TTS | MiniMax | `minimax.tts.v1` | `speech-2.8-hd` |
| STT | xAI | `xai.stt.v1` | `stt` |
| TTS | xAI | `xai.tts.v1` | `tts` |
| STT | Google | `google.stt.v1` | `chirp_3` |
| TTS | Google | `google.tts.v1` | `chirp-3-hd` |
| STT | Alibaba | `alibaba.stt.v1` | `qwen3-asr-flash-realtime` |
| TTS | Alibaba | `alibaba.tts.v1` | `qwen3-tts-flash-realtime` |
| STT | Gradium | `gradium.stt.v1` | `default` |
| TTS | Gradium | `gradium.tts.v1` | `default` |
| TTS | Rime | `rime.tts.v1` | `coda` |
| TTS | Hume | `hume.tts.v1` | `octave-2` |
| STT | Inworld | `inworld.stt.v1` | `inworld-stt-1` |
| TTS | Inworld | `inworld.tts.v1` | `inworld-tts-2` |
| STT | OpenAI | `openai.stt.v1` | `gpt-live-transcribe` |
| TTS | OpenAI | `openai.tts.v1` | `gpt-4o-mini-tts` |
| STT | Soniox | `soniox.stt.v1` | `stt-rt-v5` |
| TTS | Fish Audio | `fish.tts.v1` | `s2.1-pro` |
| TTS | Soniox | `soniox.tts.v1` | `tts-rt-v2` |
| STT | Smallest | `smallest.stt.v1` | `pulse` |
| TTS | Smallest | `smallest.tts.v1` | `lightning_v3.1` |

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
