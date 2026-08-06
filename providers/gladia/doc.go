// Package gladia implements the provider-direct Gladia streaming STT adapter.
//
// Gladia's live API is deliberately two-step, which is why this adapter looks
// different from the single-dial adapters next to it:
//
//  1. POST https://api.gladia.io/v2/live with the audio format and transcription
//     options, authenticated by the account key in an `x-gladia-key` header.
//     The response carries `{"id", "created_at", "url"}` where `url` is a
//     session-scoped WebSocket URL that already embeds a temporary token
//     (observed form: wss://api.gladia.io/v2/live?token=<uuid>).
//  2. Dial that returned URL. No account key is ever sent on the socket.
//
// The split maps cleanly onto CredentialSource. A BYOK plan carries the
// customer's account key as a bearer credential and this adapter performs the
// init POST itself. A managed plan carries the already-minted session URL as a
// protocol.CredentialSessionURL credential, so the adapter skips the POST
// entirely and the long-lived Speko-owned key never reaches the runtime.
//
// Four behaviours are worth knowing before reading the code:
//
// Partial transcripts are opt-in. Gladia defaults
// `messages_config.receive_partial_transcripts` to false, so a session opened
// with vendor defaults emits nothing until an utterance closes. This adapter
// always requests partials because the runtime's transcript.delta contract
// depends on them. Partial and final share one `transcript` message type and
// are told apart by `data.is_final`.
//
// Language tags must be narrowed. Gladia's TranscriptionLanguageCodeEnum is
// 207 bare ISO codes with no regional variant anywhere in it, so a portable
// "en-US" is a 422 at init and has to reach the wire as "en".
//
// There is no non-terminal flush. `stop_recording` is Gladia's only documented
// client command besides audio, and the server answers it by flushing pending
// audio into a last transcript and then closing with 1000. CommitAudio and
// Close therefore converge on one guarded send rather than mapping to two
// different frames the way the Deepgram and Cartesia protocols allow.
//
// Mid-stream vendor errors arrive as a close, not a frame. The AsyncAPI defines
// no standalone error message; an `error` object may only ride on the
// acknowledgement, add-on, and post-processing messages, and never on
// transcript or the speech events. Per-chunk acknowledgements are the vendor's
// only mid-stream error channel, and they are declined here because one
// acknowledgement per 20ms frame is a lot of traffic for a byte range the
// runtime cannot act on. The practical consequence is that a failure during
// streaming surfaces as a WebSocket close, whose vendor-specific idle codes
// (4408, 4504) this adapter names explicitly.
package gladia
