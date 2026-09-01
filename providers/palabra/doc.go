// Package palabra implements Palabra's provider-direct realtime speech-to-text
// and text-to-speech WebSocket APIs.
//
// Both dedicated sockets authenticate in the standard Authorization bearer
// header. The value may be a customer-owned account key or an interim publisher
// JWT from a manually managed Palabra session. Publisher JWT support remains a
// live-canary-gated managed route because the credential is reusable across
// modalities rather than scoped to one dedicated ASR or TTS connection.
package palabra
