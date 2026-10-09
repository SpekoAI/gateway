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
//	tts: soniox.tts.v1, model tts-rt-v2,  wss://tts-rt.soniox.com/tts-websocket
//
// # Authentication is a single mechanism, not two
//
// Every other adapter in this repository chooses a header by
// Plan.Execution.CredentialSource, because BYOK keys and managed short-lived
// tokens ride different transports. Soniox does not work that way and this
// package deliberately does not pretend otherwise.
//
// Both sockets authenticate the HTTP handshake with `Authorization: Bearer
// <key>`, and the long-lived key and a temporary key are interchangeable
// there. Soniox deprecated the older `api_key` field of the first JSON message
// and refuses a connection that sends its key only there from 2027-01-15
// (soniox.com/docs/guides/migrate-websocket-authentication). So a managed
// route and a BYOK route open a byte-identical connection apart from the
// secret itself, and branching on CredentialSource would encode a distinction
// the vendor does not make. The key sent with the connection authenticates
// every stream the socket runs, which matters for TTS: a single-use temporary
// key allows only one stream on the connection, so the control plane mints
// TTS keys reusable for the key's lifetime.
//
// A relay plan (Execution.ProviderRoute == RouteSpekoRelay) changes nothing
// about the channel either: the relay connector's permanent key rides the same
// Authorization header. The only relay accommodation is in the credential-kind check —
// protocol.SessionPlan validation requires relay plans to label their
// credential relay_access, while the relay connector that synthesizes plans
// and drives these adapters directly labels the same permanent key bearer, so
// both spellings are accepted on the relay route and only there (see
// acceptableCredentialKind).
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
// # Reservation attribution
//
// Every request this package sends on Speko's own Soniox credential carries
// client_reference_id = "speko_reservation:<reservation id>" — realtime STT
// and realtime TTS in the start message, async batch in the transcription
// creation body. Soniox echoes the value verbatim in GET /v1/usage-logs, and
// that echo is the only thing provider-authoritative settlement can attribute
// the vendor's invoiced cost_usd by; an unstamped request belongs to no
// reservation and no organization and can only be charged from relay telemetry
// at a flat catalog rate.
//
// The stamp follows the plan's billing authority, not its credential
// placement: relay plans and managed provider-direct plans are tagged, BYOK
// provider-direct plans are not, because those bill the customer's own Soniox
// project. See reservationReference.
//
// Two vendor rules bound it. client_reference_id is capped at 256 characters
// and a longer value fails the whole request with HTTP 400, so an over-long
// stamp is dropped rather than sent. And it is "Ignored if the request
// authenticates with a temporary API key" — the managed provider-direct case —
// where the control plane binds the identifier to the key at mint time
// instead; that binding writes the BARE reservation id, so both spellings
// reach the usage log and settlement accepts either.
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
// # Long TTS utterances
//
// Soniox bounds one TTS stream at 5,000 bytes of accumulated text and, more
// tightly, at two minutes of generated audio: past that the output is
// truncated and the stream ends with max_audio_duration_reached (HTTP 413).
// Both caps are fixed. The adapter never puts more than ttsStreamTextBudget
// (1,200 bytes, with CJK characters counted at 4) on one stream, about 80
// seconds of speech, and an utterance longer than that is not refused: it is
// spread over several streams run one after another on the same socket. When
// text would overflow the active stream, the adapter sends what fits, cut at
// the latest sentence end (then clause mark, then whitespace, and only as a
// last resort at a rune boundary), ends that stream with text_end, and queues
// the rest. The next stream starts with the queued text when the previous one
// reports terminated, so audio arrives in order and one stream is active at a
// time. Past ttsStreamSoftBudget a sentence end rolls the stream over early,
// so text streamed a token at a time is cut between sentences rather than
// inside one.
//
// The runtime still sees one utterance: one audio.started, frames in order,
// alignment spans shifted onto the utterance's timeline (Soniox times each
// stream from its own first sample; the raw frame under the extension key is
// left unshifted), and one audio.done after the last stream terminates.
// CommitText's text_end goes to the utterance's last stream; Cancel cancels
// the active stream and drops the queued text; Close waits for every queued
// stream. Each stream is its own Soniox request, so each carries
// client_reference_id and writes its own usage-log entry, and each counts
// toward Soniox's 100-requests-a-minute limit. If a stream still reaches the
// audio cap (text that speaks far slower than the budget assumes), the 413 is
// classified as a non-retryable invalid_request.
//
// # Known gaps
//
// Soniox closes an STT socket that receives neither audio nor a
// `{"type":"keepalive"}` control for too long, and closes a TTS connection
// idle for more than 40 seconds; a TTS stream that receives no text for a few
// seconds ends with request_timeout, which applies equally to a queued stream
// of a long utterance whose caller stops sending text without committing.
// Note the two keepalive messages have different shapes: STT takes
// `{"type":"keepalive"}`, TTS takes `{"keep_alive":true}`. Neither adapter
// runs a keepalive ticker, because in this runtime writes are serialized by
// the session and driven by the caller — the same stance the Deepgram adapter
// takes toward its own idle timeout. A caller that gates audio behind local
// VAD must therefore keep the socket fed itself, or the socket is
// closed underneath it.
package soniox
