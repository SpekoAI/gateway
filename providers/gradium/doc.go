// Package gradium implements the provider-direct Gradium streaming adapters
// for speech-to-text and text-to-speech.
//
// Both modalities are WebSocket surfaces that share one JSON framing, one
// handshake header, and one lifecycle, so both adapters set
// protocol.TransportWebSocket and neither falls back to a batch endpoint.
// Gradium does publish REST equivalents (POST /api/post/speech/asr streaming
// NDJSON, POST /api/post/speech/tts); they are deliberately not implemented
// here because the sockets are the low-latency surface this runtime is built
// around.
//
//	stt: gradium.stt.v1, wss://api.gradium.ai/api/speech/asr
//	tts: gradium.tts.v1, wss://api.gradium.ai/api/speech/tts
//
// Neither adapter hardcodes a model. Gradium's documented model alias for both
// modalities is the literal string "default" (with "gradium-tts-beta" offered
// as an opt-in newer TTS model), but the alias still has to arrive in the
// signed plan: an adapter that substituted its own default would be choosing a
// route the control plane did not authorize. "auto" is rejected for the same
// reason every sibling adapter rejects it.
//
// # One credential, one code path
//
// Every other dual-modality adapter in this repository branches on
// Plan.Execution.CredentialSource, because a customer-owned key and a managed
// short-lived token ride different transports. This package deliberately does
// not branch.
//
// Gradium authenticates both sockets with a single long-lived account API key
// in the `x-api-key` handshake header, and the same key works for STT and TTS
// — there is no per-service scoping and no per-modality key. So a managed
// route and a BYOK route would produce a byte-identical handshake apart from
// the secret itself, and branching on CredentialSource would encode a
// distinction the vendor does not make. Both sources therefore take the same
// path, and the runtime is expected to reject a managed Gradium route upstream
// if account-key delegation is not acceptable policy.
//
// # A short-lived token surface exists and is NOT implemented here
//
// This contradicts the premise this package was written under, so it is
// recorded rather than buried. Gradium's /guides/browser-websockets documents
// a mint endpoint:
//
//	GET https://api.gradium.ai/api/api-keys/token
//	x-api-key: <long-lived key>
//	-> {"token": "...", "expires_at": "2026-05-23T12:00:00Z"}
//
// The minted token is single-use ("consumed when it is verified"), server-set
// TTL, and is presented as a `?token=` handshake query parameter — NOT as
// `x-api-key`. So it is not a drop-in for the header path: wiring it up is a
// real credential split (mint in the control plane, query parameter here,
// one token per connection including reconnects), not a header swap. It was
// out of scope for this package and is left unimplemented on purpose. A future
// managed route that wants it must add the CredentialSource branch that this
// package currently and deliberately omits.
//
// # Provenance
//
// Every wire fact this package encodes was read from Gradium's raw Markdown
// doc sources on 2026-08-07 (each docs page serves clean Markdown when `.md`
// is appended to its URL), not from a summarizer:
//
//   - /api-reference/endpoint/stt-websocket.md and
//     /api-reference/endpoint/tts-websocket.md — endpoints, per-message field
//     tables for both directions, error frame shape.
//   - /guides/websocket-lifecycle.md — setup/ready/input/flush/end ordering,
//     the `x-api-key` header, the shared setup fields, the 80 ms frame table,
//     and the 1002/1008/1011 code list.
//   - /guides/errors.md — the WebSocket error contract and the statement that
//     authentication failures "always produce code 1008".
//   - /guides/limits.md — the exact accepted `output_format` / `input_format`
//     vocabulary and the 300-second session ceiling.
//   - /guides/transcription-settings.md — the language enumeration
//     (en, fr, de, es, pt) both adapters validate against.
//   - /guides/websocket-stream-options.md — that waiting for `ready` before
//     sending input is optional and off by default.
//   - /guides/browser-websockets.md — the token mint endpoint described above.
//
// # Deliberate omissions
//
// `client_req_id` and `close_ws_on_eos` are never sent. Both exist only to
// multiplex several logical requests over one socket; this runtime owns one
// provider stream per session, so the documented default (one request, server
// closes after end_of_stream) is exactly what is wanted, and sending the
// fields would add wire surface with no behavior change.
//
// `x-api-source: speko-platform` — sent by the platform's TypeScript client —
// is not sent here. It appears in no published Gradium documentation, and this
// package only writes headers the vendor documents.
//
// Regional hosts. The platform's TypeScript client connects to
// `us.api.gradium.ai`, and Gradium advertises EU and US capacity, but no
// published doc page names a regional hostname. The endpoint allowlist here
// pins only the documented `api.gradium.ai`; a deployment that wants a
// regional host must add it explicitly through Config.AllowedEndpointHosts.
//
// # Known gaps
//
// STT `step` frames are dropped. They carry the semantic-VAD horizon
// predictions and arrive every 80 ms (`step_duration_s: 0.08`), so forwarding
// them as canonical events would emit roughly 12.5 events per second into a
// 32-slot bounded buffer and starve real transcripts. The runtime owns turn
// detection, and endpointing information the runtime does need is already
// carried by `end_text`. A caller that genuinely wants the VAD horizon needs a
// validated extension, not a firehose.
//
// Compressed media is refused. protocol.MediaFormat admits "opus", and Gradium
// accepts an `opus` format on both sockets, but Gradium's is specifically
// "Ogg-wrapped Opus" — a container stream, not the bare frames this runtime
// passes through. Rather than emit a plausible-looking byte stream that no
// consumer can decode, both adapters require "pcm_s16le" and say so.
//
// There is no reservation passthrough. Deepgram takes an arbitrary `extra`
// query parameter that this repository uses to stamp a managed reservation ID
// onto the provider's own records; Gradium documents no equivalent free-form
// field on either socket. Managed metering must correlate on the `request_id`
// that arrives in `ready`, which both adapters surface as a usage.observed
// event before any media event.
//
// No adapter-side session timer. Gradium caps a single session at 300 seconds
// on both transports. Neither adapter enforces that locally, matching the
// stance every sibling takes toward provider-side idle and lifetime limits:
// the socket simply ends, and the runtime's lease machinery is the layer that
// owns session duration.
package gradium
