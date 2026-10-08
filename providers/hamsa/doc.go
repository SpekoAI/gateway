// Package hamsa implements the Hamsa (api.tryhamsa.com) speech-to-text and
// text-to-speech adapters. Hamsa is an Arabic-first vendor — dialect-aware
// across Egyptian, Gulf, Levantine, North African, Iraqi, Yemeni, and MSA,
// with mixed Arabic-English speech handled inside the `ar` language — and its
// realtime WebSocket transcribes one WHOLE UTTERANCE per message rather than
// streaming frames. The STT adapter therefore buffers each turn locally and
// performs one socket round trip per commit; there are no interim transcripts
// by construction, and turn latency behaves like batch, not like a partials
// stream.
//
// TTS is POST /v1/realtime/tts-stream: one request per utterance, a chunked
// body of bare PCM at 16 or 8 kHz, a required speaker, and a dialect the
// adapter derives from the session language.
//
// Both directions bill 3 credits per minute of audio, rounded up to a whole
// second per request, so each adapter reports duration_seconds at that grain.
package hamsa
