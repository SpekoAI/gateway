// Package palabra implements Palabra's provider-direct realtime speech-to-text
// and text-to-speech WebSocket APIs.
//
// Both dedicated sockets document account-key authentication in the standard
// Authorization bearer header. The adapter transports the bearer selected by
// the signed plan but does not decide whether Palabra grants that credential
// access to a product. Publisher-JWT compatibility with these dedicated sockets
// therefore remains a staging live-canary gate in managed routing policy.
package palabra
