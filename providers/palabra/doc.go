// Package palabra implements Palabra's provider-direct realtime speech-to-text
// and text-to-speech WebSocket APIs.
//
// Both dedicated sockets authenticate with an account API key in the standard
// Authorization bearer header. Palabra also issues short-lived publisher JWTs
// for manually created speech-to-speech/WebRTC sessions, but those credentials
// are bound to the returned translation-session URL and are not a documented
// grant for the dedicated ASR or TTS endpoints implemented here. Consequently
// these adapters are safe for customer-owned BYOK credentials and relay-held
// keys; managed provider-direct routing must remain disabled unless Palabra
// exposes a scoped grant for these exact resources.
package palabra
