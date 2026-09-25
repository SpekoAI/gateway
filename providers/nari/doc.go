// Package nari is the relay's integration for Nari Labs, which serves the
// Qwen3 speech models on its own inference: Qwen3-TTS (qwen3-tts,
// qwen3-tts-fast) over an HTTP synthesis endpoint and Qwen3-ASR (qwen3-asr,
// qwen3-asr-fast) over a realtime transcription socket. Both ride one API key
// sent as an Authorization bearer header.
//
// # Wire facts this package is built on
//
// Taken from docs.narilabs.com (generate-speech, streaming-audio,
// transcribe-audio, errors, rate-limits; read 2026-09-24) and confirmed
// against the live API the same day:
//
//   - Synthesis is POST https://api.narilabs.com/v1/audio/speech with
//     {model, input, voice, stream, response_format}. With stream true and
//     response_format "pcm" the body is raw 24 kHz mono s16le samples under
//     Content-Type audio/pcm. `input` is 1–2,048 code points after trimming.
//     `voice` is a case-sensitive catalog id (claire, ben, …) and fixes the
//     language; a `language` that disagrees with the voice is rejected, so it
//     is never sent.
//   - A failure after the 200 status line truncates the body without a JSON
//     error, so a read that ends in anything but EOF is a failed synthesis.
//   - Transcription is wss://api.narilabs.com/v1/realtime?intent=transcription.
//     The first frame is session.configure with a FLAT session object
//     {model, language?, turn_detection}; audio sent before session.configured
//     is not transcribed. Audio is base64 16 kHz mono s16le in
//     input_audio_buffer.append; the vendor recommends 100 ms (3,200 byte)
//     appends. input_audio_buffer.commit ends an utterance.
//   - Every commit is answered by input_audio_buffer.committed{item_id} or,
//     when nothing was buffered, input_audio_buffer.commit_empty. An
//     utterance reaching 36 s of audio is committed by the service itself
//     (commit_reason "max_duration") and the socket stays open, so one
//     caller commit can yield several items. Each item then streams
//     transcript.partial (each replaces the item's previous hypothesis) and
//     ends with one transcript.completed carrying the final text.
//   - Errors are {"type":"error","error":{code,message,requestId}} with
//     UPPER_SNAKE codes, sent before the close frame when the service can.
//     The HTTP endpoint answers {"error":{code,message,requestId}}.
//   - A language outside the 30 the models support fails session setup as
//     UPSTREAM_UNAVAILABLE rather than a validation error, so the tag is
//     checked here and an unknown one is left unsent (auto-detect).
//   - x-request-id on every HTTP response and on the socket handshake is the
//     id Nari's request logs and support are keyed on.
//   - Concurrency is per organization per region, shared by every key and by
//     both models of a kind: 20 in-flight TTS requests and 20 open STT
//     sockets on Paid Tier 1. Excess is a 429 CONCURRENCY_LIMIT_EXCEEDED,
//     which is retryable once a slot frees, so it is classified as a rate
//     limit and the relay fails over.
//
// Usage is metered by the relay (input characters and streamed audio
// seconds), which is what Nari bills: trimmed input characters and accepted
// audio including silence.
//
// Neither route is routable until its live canary passes.
package nari
