// Package palabra implements Palabra's provider-direct realtime speech-to-text
// and text-to-speech WebSocket APIs.
//
// The TTS socket authenticates with the standard Authorization bearer header.
// The ASR socket sends the key in its documented token query parameter: the
// 2026-09-11 staging canary saw Palabra reject the account key in both bearer
// and raw Authorization headers with HTTP 401 while the token parameter
// succeeded. STT dial errors redact that token. The adapter transports the
// credential selected by the signed plan but does not decide whether Palabra
// grants that credential access to a product. Publisher-JWT compatibility with
// these dedicated sockets therefore remains a staging live-canary gate in
// managed routing policy.
package palabra
