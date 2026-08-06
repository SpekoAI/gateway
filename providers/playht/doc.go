// Package playht implements the provider-direct PlayHT streaming TTS adapter.
//
// PlayHT is a two-step provider: an account credential buys a short-lived,
// per-session WebSocket URL from an HTTPS auth endpoint, and synthesis happens
// on that returned URL. The adapter performs step one itself for BYOK plans and
// accepts an already-minted URL as a session_url credential for managed plans,
// so a permanent account key never has to live in customer infrastructure.
package playht
