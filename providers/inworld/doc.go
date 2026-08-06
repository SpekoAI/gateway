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
