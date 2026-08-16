// Package hamsa implements the Hamsa (api.tryhamsa.com) speech-to-text
// adapter. Hamsa is an Arabic-first vendor — dialect-aware across Egyptian,
// Gulf, Levantine, North African, Iraqi, Yemeni, and MSA, with mixed
// Arabic-English speech handled inside the `ar` language — and its realtime
// WebSocket transcribes one WHOLE UTTERANCE per message rather than streaming
// frames. The adapter therefore buffers each turn locally and performs one
// socket round trip per commit; there are no interim transcripts by
// construction, and turn latency behaves like batch, not like a partials
// stream.
package hamsa
