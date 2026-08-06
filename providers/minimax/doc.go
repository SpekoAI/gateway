// Package minimax implements the provider-direct MiniMax T2A v2 streaming TTS
// adapter for customer-owned credentials.
//
// MiniMax exposes T2A v2 over both HTTP (SSE) and WebSocket. This adapter uses
// the WebSocket API, which keeps one warm session across utterances and puts
// the stable generation settings in a single opening handshake, so an utterance
// costs one small text frame instead of a fresh TLS and HTTP round trip.
package minimax
