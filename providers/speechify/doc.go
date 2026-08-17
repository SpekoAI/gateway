// Package speechify implements SpeechifyAI Build's chunked-HTTP text-to-speech
// stream for the Simba model family.
//
// Speechify deprecated its client-side JWT minting endpoint and documents API
// keys plus a server-side proxy for Build TTS. This adapter therefore supports
// customer-owned BYOK and relay-held credentials, but it must not be selected
// for managed provider-direct sessions where the credential reaches a customer
// sidecar.
package speechify
