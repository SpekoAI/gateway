// Package maya implements Maya Research's realtime WebSocket v2 text-to-speech
// protocol for the Maya 2 Native model family.
//
// Maya currently authenticates with a permanent API key. It does not publish
// a short-lived or session-scoped credential exchange, and its own browser
// guidance says to proxy browser audio rather than ship the key to JavaScript.
// The adapter therefore supports customer-owned BYOK and Speko Relay plans,
// but rejects managed provider-direct plans where a credential would reach a
// customer sidecar.
package maya
