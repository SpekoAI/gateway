// Package speechmatics implements Speechmatics' realtime v2 streaming STT
// adapter.
//
// The adapter dials the global realtime endpoint by default and also permits
// Speechmatics' EU and US residency hosts. It always authenticates with an
// Authorization: Bearer header. That channel accepts both a customer's
// long-lived BYOK key and the short-lived realtime JWT minted by the Speko
// control plane; the browser-only ?jwt= form is intentionally not used because
// URLs are more likely than headers to reach access logs.
//
// A session is configured with StartRecognition and does not accept audio until
// RecognitionStarted arrives. Audio is sent as binary AddAudio messages.
// CommitAudio maps to ForceEndOfUtterance, a non-terminal flush suitable for a
// multi-turn call. Close sends EndOfStream with the total number of audio chunks
// and leaves the reader alive until EndOfTranscript, so the final transcripts
// emitted between those two messages are not discarded.
package speechmatics
