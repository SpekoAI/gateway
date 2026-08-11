// Package inworld implements the provider-direct Inworld TTS adapter.
//
// It targets Inworld's HTTP server-streaming resource,
// POST /tts/v1/voice:stream, which returns audio chunks as they are generated
// rather than one buffered file. The non-streaming twin, POST /tts/v1/voice,
// is deliberately not used: waiting for a whole utterance costs latency that
// realtime voice cannot spend.
//
// Inworld also publishes a bidirectional WebSocket resource,
// wss://api.inworld.ai/tts/v1/voice:streamBidirectional, which adds multi-context
// reuse over one connection and authenticates through an `authorization` query
// parameter instead of a header. That would be a second adapter with a
// websocket route; this one is the HTTP path.
//
// # Credentials and the relay arm
//
// Both adapters in this package authenticate through the Authorization header,
// with the prefix keyed to where the credential came from (see
// sttAuthorization and authorizationHeader):
//
//   - BYOK: the customer's permanent portal key — already the Base64 of
//     "<key>:<secret>" — as `Basic <key>`.
//   - Managed provider-direct: the short-lived JWT the control plane mints at
//     POST /auth/v1/tokens/token:generate, as `Bearer <jwt>`.
//   - Relay (RouteSpekoRelay): the relay connector's permanent portal key,
//     which is the same kind of value as a BYOK key and therefore takes the
//     Basic channel. On the relay route the adapters accept the relay_access
//     credential kind beside bearer, because protocol.SessionPlan validation
//     labels a relay plan's credential relay_access while the relay connector,
//     which synthesizes its plans and never runs Validate, labels the same key
//     bearer.
//
// This is the first HTTP-transport adapter in the repository. Every other
// provider here speaks WebSocket, so the shape is worth stating once: the
// runtime engine only requires a runtimepkg.ProviderStream, and nothing in that
// interface implies a persistent socket. A request/response provider satisfies
// it by buffering AppendText and performing the exchange in CommitText, then
// streaming the response body into the same canonical audio events a socket
// adapter emits.
//
// Consequences of HTTP that a reader of the WebSocket adapters should expect:
//
//   - Open performs no network I/O. Credential and endpoint problems that a
//     WebSocket adapter reports from a failed handshake surface here at the
//     first CommitText instead.
//   - One utterance is one HTTP request. Utterances are sequential: CommitText
//     refuses to start a second request while one is in flight.
//   - Cancel and Abort work by cancelling the request context, which aborts the
//     in-flight response rather than sending a provider control message.
//   - The response body is decoded incrementally. It is never read into memory
//     in full, so time-to-first-audio matches the provider's own chunking.
package inworld
