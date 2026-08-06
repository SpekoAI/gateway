# Local protocol

Speko Gateway exposes a small HTTP setup API and a canonical streaming
WebSocket over a Unix socket. All routes except `GET /healthz`, `GET /readyz`
and `GET /v1/models` require `Authorization: Bearer <SPEKO_LOCAL_AUTH_TOKEN>`.

## Routes

| Method and path | Purpose |
| --- | --- |
| `GET /healthz` | Process liveness |
| `GET /readyz` | Readiness; returns 503 while draining |
| `GET /metrics` | Prometheus text metrics; authenticated |
| `GET /v1/models` | Providers and models this build implements; unauthenticated |
| `POST /v1/sessions` | Create and open a provider session |
| `GET /v1/sessions/{id}/stream` | Attach the one canonical WebSocket consumer |
| `DELETE /v1/sessions/{id}` | Abort and remove a session |

### `GET /v1/models`

Every (provider, modality) this build implements, with the adapter behind it, the
default model used when a caller names none, and whether **this process** has the
adapter loaded. Unauthenticated on purpose: it names no customer, carries no
credential, and an integrator has to be able to read it before the first session
exists.

Filter with `?kind=stt|tts` and `?provider=<name>`.

```json
{
  "protocol": "speko.voice.v0",
  "protocol_revision": 3,
  "runtime": { "name": "go-gateway", "version": "0.1.0", "placement": "sidecar" },
  "models": [
    {
      "id": "elevenlabs:scribe_v2_realtime",
      "provider": "elevenlabs",
      "kind": "stt",
      "adapter": "elevenlabs.stt.v1",
      "transport": "websocket",
      "installed": true
    }
  ]
}
```

`installed: false` means the row is part of the catalog but this deployment did
not load its adapter, so a session for it cannot open here. The row is reported
rather than hidden so the published list has the same shape everywhere.

Both the catalog and standalone route construction read one table
(`gateway/catalog.go`). A provider is published exactly when it is routable —
they were separate lists before, which is how a published id and an openable
route drift apart.

`POST /v1/sessions` requires an `Idempotency-Key`. Repeating the same key and
body reuses the active session. Reusing the key with a different body returns
a conflict.

The setup body is provider-neutral:

```json
{
  "kind": "tts",
  "integration": {
    "name": "custom",
    "version": "1.0.0"
  },
  "execution": {
    "provider_route": "provider_direct",
    "credential_source": "managed",
    "relay_policy": "forbidden"
  },
  "request": {
    "provider": "auto",
    "model": "auto",
    "voice": "voice_123",
    "language": "en",
    "max_input_characters": 4000
  },
  "media": {
    "encoding": "pcm_s16le",
    "sample_rate_hz": 16000,
    "channels": 1
  }
}
```

Standalone mode requires `credential_source=byok`,
`provider_route=provider_direct`, and `relay_policy=forbidden`. If more than one
configured BYOK provider supports the requested kind, `request.provider` must
be explicit.

The response exposes the selected route but never its endpoint query or
credential:

```json
{
  "session_id": "session_...",
  "attempt_id": "attempt_...",
  "execution": {
    "placement": "sidecar",
    "provider_route": "provider_direct",
    "credential_source": "managed"
  },
  "route": {
    "provider": "deepgram",
    "model": "nova-3",
    "adapter": "deepgram.stt.v1",
    "transport": "websocket"
  },
  "stream_url": "/v1/sessions/session_.../stream"
}
```

### Voice resolution

`request.voice` is an override and always wins. It is also the only way to name a
voice when you have named the provider yourself.

When it is absent, the runtime uses `route.voice` from the signed plan. That
field exists for `provider: "auto"`: a caller that delegates the vendor choice
cannot know which id space to send a voice from — a Cartesia voice id means
nothing to ElevenLabs — so the party that picks the vendor picks a voice for it
too. Every voice-taking adapter rejects an empty voice id, so without this a
delegated TTS route could not open at all.

`route.voice` is optional and omitted when empty, so plans that carry no voice
are unchanged on the wire.

## WebSocket

Connect to `stream_url` through the same Unix socket with subprotocol
`speko.voice.v0.r3` and the local authorization header. Only one connection can
claim a session stream.

Client to gateway:

- binary messages: raw audio matching the setup media format;
- `{"type":"audio.commit"}`;
- `{"type":"text.append","data":{"text":"Hello"}}`;
- `{"type":"text.commit"}`;
- `{"type":"response.cancel"}`; and
- `{"type":"session.close"}`.

Gateway to client:

- binary messages: synthesized audio frames; and
- JSON messages: canonical events from `protocol.Event`.

Representative JSON event types are `session.ready`, `speech.started`,
`transcript.delta`, `transcript.final`, `audio.started`, `audio.done`,
`usage.observed`, `warning`, `error`, and `session.closed`. Provider-specific
metadata, when preserved for local consumers, lives under namespaced
`extensions` and is never copied into telemetry.

The normative signed-plan structure is
[`protocol/schema/session-plan.v0.schema.json`](../protocol/schema/session-plan.v0.schema.json).
