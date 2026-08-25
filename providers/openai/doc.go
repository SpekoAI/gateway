// Package openai implements the OpenAI STT and TTS provider adapters.
//
// Two surfaces, two transports, chosen because they are the only OpenAI
// endpoints that stream in the direction a live session needs:
//
//   - STT: the Realtime transcription session over WebSocket
//     (wss://api.openai.com/v1/realtime). Audio goes up as
//     input_audio_buffer.append while the caller is still speaking, and
//     transcript deltas come back before the turn ends. The alternative,
//     POST /v1/audio/transcriptions, streams its RESPONSE as SSE but takes its
//     REQUEST as multipart/form-data carrying a complete file, so it cannot
//     start before the caller's audio ends. It is therefore batch and is not
//     implemented here.
//   - TTS: POST /v1/audio/speech over HTTP. OpenAI publishes no text-in /
//     audio-out WebSocket; the Realtime API only synthesizes speech as part of
//     a conversational response. /v1/audio/speech is nonetheless a real
//     streaming surface — the OpenAPI document declares Transfer-Encoding
//     chunked on the 200 — so the adapter emits an audio frame per body read
//     rather than buffering the utterance.
//
// Credentials. Both adapters take a single bearer credential and send it as
// Authorization: Bearer, for managed and BYOK alike, because OpenAI documents
// exactly one mechanism per surface. The short-lived ek_ client secret minted
// by POST /v1/realtime/client_secrets (TTL 10-7200 s, default 600 s) is scoped
// to Realtime sessions: its request body's session field is oneOf a realtime or
// a transcription session configuration, and nothing in OpenAI's documentation
// extends it to /v1/audio/speech or /v1/audio/transcriptions. The
// openai-insecure-api-key.<token> WebSocket subprotocol is the browser
// workaround for clients that cannot set headers, not a separate
// ephemeral-credential channel. Branching on CredentialSource would invent a
// split the vendor does not publish.
//
// Router. Plans routed through router.speko.dev (RouteSpekoRelay) carry the
// relay connector's permanent OpenAI key. There is no separate channel for it:
// the key travels in the identical Authorization: Bearer header, never in a
// URL. The only relay-specific behaviour is credential-kind acceptance —
// protocol.SessionPlan validation requires relay plans to label their
// credential relay_access, while the relay connector, which synthesizes plans
// and drives these adapters directly (no Engine, no SessionPlan.Validate),
// labels the same permanent key bearer — so both adapters accept either
// spelling on the relay route, and only bearer elsewhere.
package openai
