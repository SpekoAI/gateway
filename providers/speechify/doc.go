// Package speechify implements SpeechifyAI Build's chunked-HTTP text-to-speech
// stream for the Simba model family.
//
// Speechify deprecated its client-side JWT minting endpoint and documents API
// keys plus a server-side proxy for Build TTS. This adapter therefore supports
// customer-owned BYOK and relay-held credentials, but it must not be selected
// for managed provider-direct sessions where the credential reaches a customer
// sidecar.
//
// The adapter admits simba-3.5, simba-3.2, simba-3.0, simba-multilingual and
// simba-english under Speechify-Version 2026-07-07; simba-3.5 answers under it
// with 24 kHz PCM (measured 2026-10-20). The session language goes to
// Speechify verbatim: the API resolves nb/no and fil/tl itself and accepted
// et-EE, outside its documented list, with correct audio, so the adapter keeps
// no language table. A refusal's error.code (model_retired,
// pronunciations_unsupported, content_policy_violation) is appended to the
// provider error message, and the raw body rides the speechify.ai/tts/v1
// extension.
package speechify
