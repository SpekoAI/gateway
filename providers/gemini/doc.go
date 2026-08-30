// Package geminitranscribe is the relay's speech-to-text integration for
// Google's Gemini 3.5 Transcribe family, which is a different product from the
// Cloud Speech-to-Text V2 recognizer behind provider "google". The two are
// deliberately separate provider keys:
//
//   - "google" is Cloud Speech V2 (Chirp), a project-scoped regional endpoint
//     authenticated with an OAuth bearer minted from the relay's
//     service-account ADC document.
//   - "gemini" is the AI Studio generative surface, a global endpoint
//     authenticated with a raw API key in x-goog-api-key.
//
// Splitting them keeps one adapter per (provider, kind) — the shape the relay
// connector dispatches on — instead of teaching a single google STT adapter to
// speak two unrelated APIs with two unrelated credential types. The measured
// board already spells the split this way, keying Gemini speech rows under
// their own provider name.
//
// # Why this package exists at all
//
// Cloud Speech V2 has no streaming STT the relay can reach: its
// Speech.StreamingRecognize is bidirectional-streaming gRPC with no HTTP/JSON
// binding (see gateway providers/google/stt.go, which documents the discovery
// probe). That is why the relay has had no live Google transcription route.
// Gemini's Live API is a plain WebSocket and closes that gap.
//
// # Wire facts this package is built on
//
// Verified against the live v1beta discovery document (revision 20260829) and
// by probing the live service on 2026-08-30:
//
//   - BidiGenerateContentSetup publishes inputAudioTranscription as an
//     AudioTranscriptionConfig, whose fields are diarization (bool),
//     wordTimestamp (bool), languageCodes ([]string, BCP-47), customVocabulary
//     ([]string) and mode (VERBATIM and siblings). languageHints,
//     languageAuto and adaptationPhrases are marked deprecated in the same
//     document and are therefore not sent.
//   - The live socket is the same BidiGenerateContent path the s2s adapter
//     dials. Streaming input is raw 16 kHz mono little-endian PCM carried as
//     base64 on realtimeInput.audio with mimeType "audio/pcm;rate=16000".
//   - The batch surface is POST /v1beta/interactions. It is absent from every
//     published discovery document (v1, v1alpha, v1beta, v1beta2, all at
//     revision 20260829), so its existence was established by probe rather
//     than assumed: POST /v1beta/interactions answers 403 "Method doesn't
//     allow unregistered callers" — the path routed, the credential did not —
//     while POST /v1beta/nonexistentsurface answers 404. That is the same
//     403-vs-404 discrimination the google STT adapter used to prove
//     :streamingRecognize does NOT exist over REST, read in the opposite
//     direction.
//
// # What is not pinned here
//
// The Interactions API response field names are documented but not published
// in a machine-readable schema, so the batch decoder accepts both the
// snake_case spelling the REST documentation shows and the camelCase spelling
// Google's JSON surfaces normally emit, and treats word-level annotations as
// optional. A response carrying only transcript text yields a transcription
// with no segments, which BatchTranscription explicitly allows. Neither model
// id is routable until its live canary passes, which is the gate that catches
// a wrong id before a customer pays admission latency for it.
package gemini
