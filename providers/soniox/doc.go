// Package soniox implements the provider-direct Soniox streaming adapters for
// speech-to-text and text-to-speech.
//
// Both modalities are WebSocket surfaces, so both adapters set
// protocol.TransportWebSocket and neither falls back to a batch endpoint.
// Soniox does publish an async STT REST API and a one-shot TTS REST endpoint;
// they are deliberately not implemented here because the realtime sockets are
// the lower-latency surface the runtime is built around.
//
//	stt: soniox.stt.v1, model stt-rt-v5, wss://stt-rt.soniox.com/transcribe-websocket
//	tts: soniox.tts.v1, model tts-rt-v1,  wss://tts-rt.soniox.com/tts-websocket
//
// # Authentication is a single mechanism, not two
//
// Every other adapter in this repository chooses a header by
// Plan.Execution.CredentialSource, because BYOK keys and managed short-lived
// tokens ride different transports. Soniox does not work that way and this
// package deliberately does not pretend otherwise.
//
// Neither socket authenticates during the HTTP handshake. Both read the
// credential out of an `api_key` field inside the first JSON message on the
// open socket, and Soniox's own reference for both endpoints writes that field
// as `"<SONIOX_API_KEY|SONIOX_TEMPORARY_API_KEY>"` — the long-lived key and the
// short-lived key are interchangeable in the same field. So a managed route and
// a BYOK route produce a byte-identical start message apart from the secret
// itself, and branching on CredentialSource would encode a distinction the
// vendor does not make.
//
// Short-lived credentials are minted by the control plane, not here:
//
//	POST https://api.soniox.com/v1/auth/temporary-api-key
//	Authorization: Bearer <long-lived key>
//	{"usage_type": "...", "expires_in_seconds": N}
//
// `usage_type` is required and scopes the key to exactly one service:
// `transcribe_websocket` for STT, `tts_rt` for TTS (which covers both the TTS
// WebSocket and the TTS REST endpoint). A key issued for one service is
// rejected by the other with HTTP 401, so a session that needs both stages
// needs two keys. TTL is caller-chosen through the required
// `expires_in_seconds`; it bounds only how long the key may *open* new streams
// and never terminates a stream already running. Optional `single_use` caps a
// key at one stream and optional `max_session_duration_seconds` caps how long
// one stream may run — when that cap elapses the server sends
// `error_type: "temp_api_key_session_expired"` with HTTP 403, which this
// package classifies as authentication_failed because only a fresh key clears
// it.
//
// # Provenance
//
// Every wire fact below was read from Soniox's raw MDX sources on 2026-08-07
// (each docs page serves clean Markdown when `.mdx` is appended to its URL),
// not from a summarizer:
//
//   - /api-reference/stt/websocket-api and /api-reference/tts/websocket-api —
//     endpoints, start-message fields, response shapes, error catalogs.
//   - /guides/temporary-api-keys — mint path, usage_type scoping, TTL semantics.
//   - /api-reference/errors — the error_type taxonomy this package branches on.
//     Soniox states error_type is stable and error_message is not, so the
//     classifiers here switch on error_type and use the numeric code only as a
//     fallback for a type we have not seen.
//   - /stt/rt/endpoint-detection and /stt/rt/manual-finalization — the <end>
//     and <fin> marker tokens.
//   - /tts/rt/termination — the three-step text_end / audio_end / terminated
//     handshake.
//   - /stt/concepts/supported-languages and /tts/concepts/supported-languages —
//     Soniox spells Norwegian `no` and Tagalog `tl`; it does not accept the
//     platform's `nb`, `nn`, or `fil`, and rejects them with HTTP 400
//     "Invalid language hint." Both adapters alias accordingly.
//
// # Known gaps
//
// Soniox closes an STT socket that receives neither audio nor a
// `{"type":"keepalive"}` control for more than 20 seconds, and closes a TTS
// socket idle for more than 20-30 seconds (or generating no audio at all for
// three minutes). Note the two keepalive messages have different shapes: STT
// takes `{"type":"keepalive"}`, TTS takes `{"keep_alive":true}`. Neither
// adapter runs a keepalive ticker, because in this runtime writes are
// serialized by the session and driven by the caller — the same stance the
// Deepgram adapter takes toward its own idle timeout. A caller that gates audio
// behind local VAD must therefore keep the socket fed itself, or the socket is
// closed underneath it.
package soniox
