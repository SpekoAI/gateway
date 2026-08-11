// Package minimax implements the MiniMax T2A v2 streaming TTS adapter.
//
// MiniMax exposes T2A v2 over both HTTP (SSE) and WebSocket. This adapter uses
// the WebSocket API, which keeps one warm session across utterances and puts
// the stable generation settings in a single opening handshake, so an utterance
// costs one small text frame instead of a fresh TLS and HTTP round trip.
//
// # Credentials
//
// MiniMax documents no ephemeral or minted token: every source — BYOK,
// managed provider-direct, and the relay (RouteSpekoRelay) — sends its key as
// `Authorization: Bearer` on the handshake, and there is no query-parameter
// auth on this endpoint. The routes differ only in which credential kinds
// they accept: bearer everywhere, plus relay_access on the relay arm, because
// the relay connector synthesizes plans that bypass
// protocol.SessionPlan.Validate and label the connector's permanent key
// bearer, while validated relay plans must label it relay_access.
package minimax
