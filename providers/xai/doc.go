// Package xai implements the xAI STT and TTS provider adapters.
//
// STT is xAI's streaming transcription socket (wss://api.x.ai/v1/stt). TTS
// serves both documented surfaces behind one adapter id, selected by the
// transport in the signed route: the bidirectional streaming socket
// wss://api.x.ai/v1/tts, and the unary POST https://api.x.ai/v1/tts whose
// chunked response body is sliced into audio frames as bytes arrive. The
// surfaces deliberately NOT implemented — the batch multipart POST /v1/stt
// upload and the separate Speech-to-Speech realtime product — are documented
// on each adapter where the refusal happens.
//
// Credentials. Every surface takes a single access token and sends it as
// Authorization: Bearer — xAI documents exactly one server-side channel, and
// its ephemeral client secret is used "in the same fashion as an API key", so
// a managed plan differs from a BYOK plan only in who owns the string. Relay
// plans (RouteSpekoRelay) carry the relay connector's permanent xAI key in
// the identical header, never in a URL; the only relay-specific behaviour is
// credential-kind acceptance, because protocol.SessionPlan validation labels
// a relay credential relay_access while the relay connector — which
// synthesizes plans and drives these adapters directly — labels the same
// permanent key bearer. Both spellings are accepted on the relay route, only
// bearer elsewhere (see acceptableCredentialKind). The
// xai-client-secret.<token> WebSocket subprotocol exists solely because
// browsers cannot set headers and must not be used from a server-side
// gateway.
//
// Every wire detail was verified against xAI's official documentation at
// https://docs.x.ai/developers/model-capabilities/audio on 2026-08-07. Where
// the documentation is silent the code says so explicitly rather than
// guessing.
package xai
