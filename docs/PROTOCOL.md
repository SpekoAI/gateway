# Local protocol

Speko Gateway serves a small HTTP setup API and one streaming WebSocket per
session over a Unix socket. This is the contract for clients of that socket.
It also describes the signed plans the Gateway accepts from the Speko control
plane.

The protocol name is `speko.voice.v0` and the local revision is `3`
(`protocol.VoiceV0`, `protocol.CurrentRevision`).

## Routes

| Method and path | Auth | Purpose |
| --- | --- | --- |
| `GET /healthz` | none | Process liveness |
| `GET /readyz` | none | Readiness. Returns 503 while draining |
| `GET /v1/models` | none | Providers and models this build implements |
| `GET /metrics` | bearer | Prometheus text metrics |
| `POST /v1/sessions` | bearer | Create a session and open the provider connection |
| `GET /v1/sessions/{id}/stream` | bearer | Attach the one WebSocket consumer of a session |
| `DELETE /v1/sessions/{id}` | bearer | Abort and remove a session. Returns 204 |
| `POST /v1/turn-events` | bearer | Post conversation timing markers |

`bearer` means `Authorization: Bearer <SPEKO_LOCAL_AUTH_TOKEN>`.

## Errors

Errors are JSON: `{"error": {"code": "...", "message": "..."}}`. When a
provider refused the session, the error also has `"source": "provider"`,
`retryable`, and `provider` with the provider's `code`, `status` and
`retryable`. The provider's response body is never returned.

| Status | Code | When |
| --- | --- | --- |
| 401 | `local_auth_required` | Missing or wrong local bearer token |
| 400 | `idempotency_key_required` | `POST /v1/sessions` without `Idempotency-Key` |
| 400 | `invalid_request` | Malformed body or unknown field |
| 400 | `invalid_stt_options`, `invalid_tts_options`, `invalid_realtime_options` | Options are malformed or on the wrong session kind |
| 409 | `idempotency_key_conflict` | The key was used with a different body |
| 409 | `duplicate_session` | The planner returned a session that is already active |
| 422 | `stt_option_unsupported`, `tts_option_unsupported` | The selected provider or model cannot honor an option. The message names it |
| 429 | `local_concurrency_exhausted` | `SPEKO_MAX_SESSIONS` reached |
| 502 | `session_open_failed` | The provider connection could not be opened |
| 502 | `control_plane_unavailable` | No session plan was issued. A BYOK request that the local planner refuses also gets this code |
| 504 | `control_plane_timeout` | Plan request timed out |
| control plane's status | control plane's code, or `control_plane_rejected` | The control plane refused the plan, for example `no_eligible_route` |
| 503 | `gateway_draining` | The Gateway is shutting down |
| 400 | `unsupported_protocol` | WebSocket request without the subprotocol |
| 404 | `session_not_found` | Unknown session ID |
| 409 | `stream_already_attached` | A second WebSocket for the same session |
| 400 | `invalid_turn_event` | A turn marker failed validation. The whole batch is refused |

A refusal from the control plane also sets the `X-Control-Plane-Request-ID`
response header when the control plane sent a request ID.

## `GET /v1/models`

Lists every provider, kind and model this build implements. Filter with
`?kind=stt|tts|realtime` and `?provider=<name>`.

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

- `installed: false` means the model is in the catalog but this process did
  not load its adapter, so a session for it cannot open here.
- Realtime rows also have `protocol`, the native protocol they speak.
- `requires_config` is present when a row needs deployment configuration
  before it can be used.
- The list and the BYOK planner read the same table, `gateway/catalog.go`. A
  provider is listed exactly when it is routable.

## `POST /v1/sessions`

Requires an `Idempotency-Key` header. A new session returns 201. Repeating
the same key and body while the session is active returns 200 with the same
response. The same key with a different body returns 409.

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

| Field | Meaning |
| --- | --- |
| `kind` | `stt`, `tts` or `realtime` |
| `integration` | Optional. `name` and `version` are required together. Also `framework_version`, `transport` |
| `execution.provider_route` | `auto`, `provider_direct` or `speko_relay` |
| `execution.credential_source` | `byok` or `managed` |
| `execution.relay_policy` | `required`, `allowed` or `forbidden` |
| `request.provider`, `request.model` | A catalog name, or `auto` |
| `request.voice` | TTS and realtime voice. See [Voice resolution](#voice-resolution) |
| `request.language` | Language tag |
| `request.max_input_characters` | Required and positive for `tts`. Not allowed on other kinds |
| `request.max_session_seconds` | Requested hard ceiling. Policy may shorten it, never extend it |
| `request.client_session_id` | Session ID chosen by the caller. At most 128 characters, no whitespace or slashes |
| `request.objective`, `request.region_hint`, `request.allow`, `request.deny` | Routing hints for managed plans. These are sent to Speko, so they must not hold user content |
| `request.stt` | STT options: `diarization`, `language_identification`, `keywords`, `noise_reduction`, `word_timestamps`, `translation.target_language`, `provider_options` |
| `request.tts` | TTS options: `provider_options` |
| `request.s2s` | Realtime options. See [Realtime sessions](#realtime-sessions-openai-realtime-and-gpt-live) |
| `media` | Audio format: `encoding`, `sample_rate_hz`, `channels` |

Unknown fields are refused. `request.stt`, `request.tts` and `request.s2s`
stay in the Gateway and are never sent to the control plane.

Rules for options:

- `provider_options` maps a provider name to that vendor's own settings.
  Values are scalars. A setting outside the provider's allow-list for the
  routed model is refused by name. Settings for a provider the session does
  not reach are ignored.
- The other STT options fail closed: a session routed to a provider that
  cannot honor one is refused with `stt_option_unsupported`.
- `word_timestamps` is a batch option. No streaming session honors it.

BYOK planning requires `credential_source=byok`,
`provider_route=provider_direct` (or `auto`) and `relay_policy=forbidden`. If
more than one configured BYOK provider supports the kind, `request.provider`
must be explicit.

The response shows the selected route but never its endpoint query or
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

`route.region` and `control_plane_request_id` are present when known. A
session whose WebSocket is not attached within 30 seconds is aborted.

### Voice resolution

`request.voice` always wins. It is the only way to name a voice when the
caller names the provider.

When it is absent, the runtime uses `route.voice` from the signed plan. With
`provider: "auto"` the caller cannot know which provider's voice IDs apply,
so the party that picks the provider picks the voice. `route.voice` is
optional and omitted when empty.

For Paxa, Munsit and Hamsa, a request with a language and no voice leaves the
choice to the adapter, which picks its default voice for that language.

## WebSocket

Connect to `stream_url` on the same Unix socket with subprotocol
`speko.voice.v0.r3` and the local authorization header. Only one connection
can claim a session's stream.

Client to Gateway:

- binary messages: raw audio in the setup media format;
- `{"type":"audio.commit"}`;
- `{"type":"text.append","data":{"text":"Hello"}}`;
- `{"type":"text.commit"}`;
- `{"type":"response.cancel"}`;
- `{"type":"provider.control","data":{"type":"<native type>","payload":{...}}}`
  on realtime sessions only; and
- `{"type":"session.close"}`.

Gateway to client:

- binary messages: synthesized audio frames; and
- JSON messages: events (`protocol.Event`) with `type`, `event_id`,
  `session_id`, `attempt_id`, `sequence`, `created_at_ms`, and optional
  `data` and `extensions`.

| Group | Event types |
| --- | --- |
| Session | `session.ready`, `session.recovering`, `session.recovered`, `session.closed`, `route.selected` |
| Speech in | `speech.started`, `speech.ended`, `transcript.delta`, `transcript.final` |
| Response | `response.started`, `text.delta`, `text.done`, `tool.call`, `response.done`, `response.cancelled` |
| Audio out | `audio.started`, `audio.done`, `alignment` |
| Usage | `usage.observed`, `usage.reported`, `usage.finalized` |
| Other | `warning`, `error`, `provider.event` |

Provider-specific metadata lives under namespaced `extensions`. It is for
local consumers and is never copied into telemetry.

### Transcript completion

STT transcript data may include `speech_final`. `true` means the utterance's
transcript is complete, including the answer to an explicit `audio.commit`.
Turn consumers can stop waiting for more text.

- A `transcript.final` without the flag can be a stable chunk inside an
  unfinished utterance (for example Deepgram Nova's `is_final: true,
  speech_final: false`).
- Do not infer completion from `is_final` alone.
- Do not use `speech.ended` instead. It can precede the last transcript.

### Finalized audio: covered_ms

On Deepgram Listen (not Flux), Speechmatics and Gladia, `transcript.final`
data carries `covered_ms`: all audio before that position is reflected in
finals already sent, this one included.

- It is measured from the first audio byte sent on the provider connection.
- It never goes backwards within a stream.
- `transcript.delta` never carries it.
- Deepgram and Gladia send it on mono streams only.
- Every other adapter leaves it out. Absent means unknown, not 0.

### Speakers: ElevenLabs Scribe v3 Live

On a diarized Scribe v3 Live session, `transcript.final` data has:

- `words`, each with the vendor's label as `speaker`;
- a top-level `speaker` when one speaker said the whole segment.

Rules:

- A word the vendor left unattributed has no `speaker`, and its final then
  has no top-level `speaker`.
- A word sent without timing has no `start_ms` or `end_ms`. A final whose
  first or last word has none has no `audio_start_ms` or `audio_end_ms`.
- A segment's final waits at most 0.8 s after its commit for the labels, also
  while the session closes. If the labels have not arrived by then, the
  stream ended first, or the vendor sent none, the committed text is
  delivered without speakers. Labels that arrive later are ignored.
- Events that follow a segment's commit wait for that segment's final. A
  later `transcript.delta` never precedes an earlier segment's
  `transcript.final`, and no event waits longer than 0.8 s.
- A `warning` event with `provider_type: "speakers_merged"`, `from_speaker`
  and `to_speaker` means the provider decided two labels are one person. No
  event rewrites a delivered transcript. A client that stores speakers
  applies the merge itself.
- `transcript.delta` data carries the vendor's `turn_probability`, the
  estimated chance that the speaker has finished.

### Speakers: Soniox

On a diarized Soniox session, `transcript.final` data carries `words` in the
Scribe v3 Live shape: each word with `speaker`, `start_ms` and `end_ms`, plus
a top-level `speaker` when one speaker said every word.

- Soniox's word pieces are joined into words. A word starts wherever the
  speaker changes.
- A word Soniox left unlabelled has no `speaker`, and its final then has no
  top-level `speaker`.
- A label longer than 64 bytes is dropped and its piece counts as unlabelled.

Soniox labels every token, so each `transcript.delta` attributes its whole
hypothesis and finals are not held back for labels. A delta carries
`speaker_runs` instead of `words`:

| Field | Meaning |
| --- | --- |
| `speaker` | The vendor's label for this run |
| `start_ms`, `end_ms` | Run timing |
| `text_start`, `text_end` | Offsets into the delta's `text` in Unicode code points, end exclusive (Python `text[text_start:text_end]`) |

- A run starts wherever the label changes, also inside a word.
- The run's text is not repeated.
- A delta with one run also carries a top-level `speaker`.
- A delta's labels are provisional. A later delta or the final may change
  them.

Timing on both events:

- A word's or run's `start_ms` is its first piece's start and `end_ms` its
  last piece's end. Either is absent when that piece has no time.
- A delta's `audio_start_ms` and `audio_end_ms` follow the same rule.
- A final carries the pair only when both are known. An inner piece's time
  never stands in for a missing outer one.
- This holds on diarized Soniox sessions only. A session without diarization
  reports the span from the first start and the last end that any piece
  carried.

The Router's public `transcript.delta` turns runs into `segments` with each
run's text and speaker, and `start_ms` and `end_ms` only when the run has
both.

Soniox notes that endpoint detection, which this adapter always enables,
lowers diarization accuracy.

### Interim finals: Soniox

A labelled segment that runs on without an endpoint is cut, so a long talk
does not resend a growing hypothesis on every delta.

- Once the finalized part spans more than 20 s of audio or 2,000 characters,
  its prefix goes out once as a `transcript.final` with
  `speech_final: false` and `interim: true`. More of the same segment
  follows.
- An interim final never answers a commit. A pending `audio.commit` is
  answered by the segment's last final, after the provider has finalized all
  its audio.
- A segment that sent an interim final always closes with a final that is
  not interim, with empty `text` when nothing was left after the cut.
- Later deltas carry only the rest, with run offsets into their own text.
  Every word and character reaches exactly one final, in order.
- The Router's public `transcript.final` carries the same `interim: true`
  and never a `commit_id` with it.
- A client that asks Soniox for diarization must read `interim` before it
  takes a final for the end of a turn.

Where the prefix ends:

- After the last finalized piece that ends a sentence in the last quarter of
  the finalized text.
- Otherwise at the last word boundary between finalized pieces, so never
  inside a word.
- Text with neither, such as Chinese or Japanese without spaces, is cut at
  the last boundary between finalized pieces within the first 2,000
  characters.
- A boundary inside a user-perceived character is skipped for the previous
  character boundary: Unicode extended grapheme clusters (UAX #29, Unicode
  15.0 tables) plus the Indic conjunct rule of Unicode 15.1. Thai กำ,
  Devanagari क्ष, emoji joined by U+200D, a letter with its combining marks,
  and a character with its variation selector each stay in one final.
- The prefix never ends after the last finalized piece, which the next piece
  may continue.

Which sessions are cut:

- A session with speaker labels or language labels is cut.
- A session without either is never cut.
- A session that asked for Soniox's own translation is never cut, because the
  translation trails the words and is not aligned to them. Its deltas and
  final grow with the segment and repeat every speaker run on each delta.

### Spoken-language labels: Soniox

Set `request.stt.language_identification: true` for Soniox `stt-rt-v5` or
`stt-rt-v6`. The Router spelling is
`session.configure.options.language_identification`.

- An unsupported route is refused. The Gateway uses
  `stt_option_unsupported`, the Router `capability_unsupported`. Both name
  the option.
- The option does not apply to batch.
- `language` on the request still acts as a recognition hint.

Gateway `transcript.delta` and `transcript.final` data add `language_runs`,
with the same code-point offsets and outer-piece timings as `speaker_runs`.

- Runs split on every change in the vendor's token language or requested
  speaker label, also within a word.
- The BCP-47 `language` field is omitted for pieces without an identified
  language. Adjacent unknown pieces can share a run.
- Only source tokens contribute. The recognized `text` and native
  `translation` stay separate.
- The final's top-level majority language is independent of the runs.
- Omitted or `false` adds no fields and leaves the event bytes unchanged.
- There is no wait for language labels.

On the Router, the runs map to `segments[].language`. Deltas can contain
untimed runs and later revise their labels. Final segments still require both
timings. The final's full text remains available even when a run has no
ordered span. Use only measured, finalized runs to identify audio intervals.

## Realtime sessions: OpenAI Realtime and GPT-Live

`kind: "realtime"` opens a speech-to-speech session. The body carries
`request.s2s` with `output_media` (required) and `instructions`. For GPT-Live
it also carries `live` with seeded `history` and the `delegation` mode:

```json
{
  "kind": "realtime",
  "request": {
    "provider": "openai",
    "model": "gpt-live-1",
    "voice": "marin",
    "s2s": {
      "instructions": "Be concise. Delegate lookups to the backend.",
      "output_media": {"encoding": "pcm_s16le", "sample_rate_hz": 24000, "channels": 1},
      "live": {
        "history": [{"role": "user", "text": "My order is A0042."}],
        "delegation": {
          "type": "responses",
          "responses": {"model": "gpt-5.6-luna", "instructions": "Backend rules.", "tools": [{"type": "web_search"}], "max_output_tokens": 1024}
        }
      }
    }
  },
  "media": {"encoding": "pcm_s16le", "sample_rate_hz": 24000, "channels": 1}
}
```

The model selects the protocol:

| Model | Adapter | Vendor endpoint | Notes |
| --- | --- | --- | --- |
| `gpt-realtime-*` | `openai.realtime.v1` | `wss://api.openai.com/v1/realtime` | 24 kHz mono PCM16, server VAD, `audio.commit` marks the turn |
| `grok-voice-*` | `xai.realtime.v1` | `wss://api.x.ai/v1/realtime` | Keeps xAI's top-level `voice` and `turn_detection` session fields. `grok-voice-3.0` reasons at `reasoning.effort: "high"` unless the session sets `"none"` |
| `gpt-live-1` | `openai.live.v1` | `wss://api.openai.com/v1/live/sessions` | Mono PCM16 at 16 or 24 kHz, the same in both directions. Voice `marin` by default |

- `provider: "openai"` with `model: "auto"` stays on Realtime. GPT-Live is
  selected only by name.
- Omitting `delegation` selects client delegation. `responses` delegation
  requires an explicit backend `model`.
- Voice instructions and backend instructions are never merged.

Events:

- Every non-audio vendor event is also surfaced verbatim as `provider.event`
  with `data` `{"type": "<vendor type>", "payload": {...}}`. Native
  identifiers (`event_id`, `delegation_id`, `response_id`, `call_id`) and
  nested Responses events (`response.event`) survive untouched.
- GPT-Live transcript deltas keep their `start_ms` and `end_ms` session
  timeline in `transcript.delta` and `text.delta` data. The intervals may
  overlap and are not turn boundaries.
- The adapter never creates `speech.started`, `speech.ended` or
  `response.done` for a Live session.
- Backend usage is observed once per backend response ID.

`provider.control` forwards one native command (`protocol.ProviderControl`).
The payload is at most 64 KiB and its own `type` equals the envelope `type`.

| Protocol | Allowed control types |
| --- | --- |
| GPT-Live | `session.update`, `session.instructions.append`, `session.thinking.append`, `session.commentary.append`, `session.input_audio.mute`, `session.input_audio.unmute`, `response.item.create`, `response.create` |
| Realtime (OpenAI, xAI) | `session.update`, `input_audio_buffer.commit`, `input_audio_buffer.clear`, `conversation.item.create`, `conversation.item.retrieve`, `conversation.item.truncate`, `conversation.item.delete`, `response.create`, `response.cancel`, `output_audio_buffer.clear` |

Refused: a control from the other protocol, a `session.update` that changes
the model or audio format, and `audio.commit` or `response.cancel` on a Live
session. GPT-Live streams audio continuously and has no turn commit or
response cancel.

Closing a Live session sends `session.close` and waits up to 15 seconds for
the vendor's `session.closed`. Its cumulative `usage.seconds` is the final
voice usage. Snapshots replace one another and are never summed. Without
that event the session ends with a `finalization_incomplete` error: a
transport disconnect is never reported as confirmed completion.

Credentials:

- BYOK sessions dial the vendor with the customer's key.
- Managed Realtime sessions use ephemeral client secrets.
- Managed GPT-Live sessions never carry a vendor credential. The control
  plane routes them on a `speko_relay` plan whose endpoint is the Speko
  Router's `/v1/live`, which speaks the same protocol.

## `POST /v1/turn-events`

Accepts timing markers from the conversation profiler
(`speko_gateway.probe`). The body is `{"events": [...]}` with 1 to 64 events
and at most 256 KiB. Each event has:

| Field | Rule |
| --- | --- |
| `type` | A marker type listed in [TRUST.md](TRUST.md#conversation-turn-markers) |
| `conversation_id` | `conv_` plus 32 lowercase hex characters |
| `turn_id` | `turn_` plus 6 digits. Empty on `conversation.*` markers |
| `seq`, `created_at_ms` | Positive integers |
| `data` | Object with `mono_ms` and only the fields that marker type allows |

An unknown type or field refuses the whole batch with `invalid_turn_event`.
Success is `202` with `{"accepted": n, "dropped": n}`.

## Signed session plans

A session plan is the signed decision that tells the runtime which provider
to dial and with which credential. The normative structure is
[`protocol/schema/session-plan.v0.schema.json`](../protocol/schema/session-plan.v0.schema.json),
and the Go types are in `protocol/session_plan.go`.

`plan.signature` is a compact JWS over an envelope:

| Claim | Type | Rule |
| --- | --- | --- |
| `iss` | string | Equals the configured issuer |
| `aud` | array of strings | Contains the configured audience (`speko-runtime` by default) |
| `iat`, `exp` | RFC3339 strings | `exp` equals the plan's `expires_at` |
| `jti` | string | Used once |
| `plan` | object | The plan without `signature` |

There are two JWS families, told apart by the protected-header `typ`:

| `typ` | Signed `plan` |
| --- | --- |
| `speko.session-plan+jws` | The exact plan |
| `speko.session-plan+credential-digest+jws` | The plan with `route.credential.value` replaced by `sha256:` plus the unpadded base64url SHA-256 of the credential |

- `alg` is `EdDSA` or `RS256`. Keys come from the configured JWKS URL.
- The verifier checks the signature, that the envelope binds the supplied
  plan byte for byte, issuer, audience, time bounds (30 s clock skew), and
  then consumes the `jti`. A reused plan is refused.
- Timestamps in plans and envelopes are RFC3339 strings. Counts and money
  (`authorized_units`, `maximum_amount_micros`, `slots`) are JSON integers
  and must keep their exact int64 value.
- `requirements.protocol_revision` must equal `3`.
- A runtime ignores plan fields it does not know. A new field is not
  enforced by older Gateways.
- BYOK plans are issued and verified inside the process with a per-process
  HMAC key and carry no credential.

## Protocol revision 5: relay plans

The local protocol stays at revision 3. Relay plans (`protocol.RelayPlan`,
`protocol/relay_plan.go`) are a separate plan family: the signed dispatch
authorization the Speko control plane issues for the hosted Speko Router's
connectors.

- Relay plans declare revision `5` (`RelayRevision`) or `6`
  (`RelayBillingRevision`, which requires operation metering). Session-plan
  validators accept only `3`, so a runtime that predates the relay rejects a
  relay plan outright.
- Relay plans are compact JWS with protected-header
  `typ: "speko.relay-plan+jws"` (`RelayPlanJWSType`) and audience
  `"speko-relay"` (`RelayPlanAudience`). Session plans keep their own `typ`
  and audience, so neither signature can authorize the other's consumer.
- Keys are not shared. Relay plans are signed by a dedicated control-plane
  key published at `/.well-known/relay-jwks.json`, never the session-plan
  key or kid.
- `credential_source` says whether the provider credential is Speko-managed
  or supplied by the organization.
- A relay plan carries the dispatch route (provider, model, and the exact
  endpoint as an assertion to verify, never an input to dial), signed budget
  ceilings per `RelayBudgetGroup`, a single-use `jti`, the `relay_access`
  bearer the edge presents on the connector handshake, and a session-scoped
  control token for the edge's follow-up ledger calls.
- Relay-route adapters accept credential kind `relay_access` in addition to
  `bearer`. BYOK and provider-direct behavior is unchanged.
- Speech-to-speech relay plans (`kind: "s2s"`) also assert the native
  `protocol` (`protocol.SpeechProtocol`): `openai.realtime.v1`,
  `openai.live.v1`, `google.live.v1`, `xai.realtime.v1`,
  `openai.realtime.translation.v1` or `alibaba.livetranslate.v1`. The
  connector checks it against its embedded catalog before it reads a
  credential and dispatches the matching adapter.
- Live plans may carry the `backend_input`, `backend_output` and
  `backend_tools` budget groups beside the renewable `s2s_duration` lease.
  They reserve a delegated Responses backend's tokens and billable tool
  calls separately from voice time.

The Router's customer-facing wire contract (HTTP, SSE and WebSocket shapes
for `router.speko.dev`) is the separate [`relayapi`](../relayapi/doc.go)
package with its OpenAPI and AsyncAPI documents. It is not part of the local
socket protocol.

## Native relay mode

`speko-gateway native` serves a different local API. It forwards Router
requests from the socket to one configured Router origin, and bodies follow
`relayapi`.

Every request except `/healthz` and `/readyz` needs
`Speko-Local-Authorization: Bearer <SPEKO_LOCAL_AUTH_TOKEN>`. The sidecar
consumes that header and never sends it upstream. A model request also needs:

| Header | Rule |
| --- | --- |
| `Authorization` | `Bearer sk_speko_...`, the session's own credential |
| `Speko-Client-Session-ID` | 1 to 200 characters: a letter or digit, then letters, digits or `._:-` |
| `Idempotency-Key` | At most 512 visible ASCII characters |

| Method and path | Kind |
| --- | --- |
| `GET /v1/stt/stream`, `GET /v1/tts/stream` | WebSocket speech streams |
| `POST /v1/llm/responses`, `POST /v1/stt/transcriptions`, `POST /v1/tts/speech` | HTTP |
| `GET /v1/realtime`, `/v1/realtime/translations`, `/v1/realtime/translations/qwen` | WebSocket. Exactly one `?model=` query parameter |
| `GET /v1/live`, `/v1/live/sessions`, `/v1/bidi` | WebSocket. No query parameters. `/v1/live/sessions` is sent to the Router's `/v1/live` |
| `GET /v1/models`, `GET /metrics` | Answered locally |

WebSocket frames are limited to 2 MiB. Redirects from the Router are refused.
