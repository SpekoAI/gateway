// Package inworld implements provider-direct Inworld STT and TTS adapters.
//
// TTS targets Inworld's persistent bidirectional WebSocket resource,
// wss://api.inworld.ai/tts/v1/voice:streamBidirectional. One authenticated
// connection carries sequential gateway utterances through a reusable Inworld
// context. This shape is required for managed routing because an Inworld
// one-time token authorizes exactly one HTTP request or WebSocket connection.
//
// # Credentials and the relay arm
//
// Both adapters in this package authenticate through the Authorization header,
// with the prefix keyed to where the credential came from (see
// sttAuthorization and authorizationHeader):
//
//   - BYOK: the customer's permanent portal key — already the Base64 of
//     "<key>:<secret>" — as `Basic <key>`.
//   - Managed provider-direct: a reservation-bound, single-use token minted at
//     POST /auth/v1/tokens, as `Bearer <token>`.
//   - Relay (RouteSpekoRelay): the relay connector's permanent portal key,
//     which is the same kind of value as a BYOK key and therefore takes the
//     Basic channel. On the relay route the adapters accept the relay_access
//     credential kind beside bearer, because protocol.SessionPlan validation
//     labels a relay plan's credential relay_access while the relay connector,
//     which synthesizes its plans and never runs Validate, labels the same key
//     bearer.
//
// Credentials are sent only in the Authorization handshake header. Inworld
// also supports a browser-oriented query/subprotocol channel, but a server-side
// gateway does not need it and must not place secrets in loggable URLs.
package inworld
