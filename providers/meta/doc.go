// Package meta is the relay's integration for Meta's Muse Voice Transcribe
// speech-to-text service (model muse-voice-transcribe-1.0): a realtime
// WebSocket for live transcription and a multipart HTTP endpoint for
// prerecorded WAV files. Both ride one API key sent as a bearer token.
//
// # Wire facts this package is built on
//
// Taken from the developer documentation at dev.meta.ai/docs/speech-to-text
// (read 2026-09-02; the page publishes no schema document, so the field names
// below are the documented JSON examples rather than a generated type):
//
//   - The realtime socket is wss://api.meta.ai/v1/asr/realtime. The FIRST
//     frame is a JSON text frame carrying the credential
//     (authorization.accessToken = "Bearer <key>"), audioEncoding, model,
//     mode, partialMode and the optional languageBias and keywords lists, and
//     must arrive within ten seconds of the socket opening. The service
//     acknowledges it with {"sessionId": "..."}.
//   - Audio is mono 16-bit little-endian PCM at 24 kHz (preferred) or 16 kHz,
//     declared as PCM_24KHZ or PCM_16KHZ and carried as raw BINARY frames
//     paced at real time. The caller ends input with the text frame
//     {"type":"endStream"}.
//   - Server events are JSON text frames discriminated by `type`:
//     speechStart, transcript (with `final`), speechEnd, speechComplete (the
//     turn's cleaned text), speaker (DIARIZATION only), audioProgress and
//     error. In ENDPOINTING and DIARIZATION modes the model detects turn
//     boundaries and speechComplete is the turn's final; `final: true` on a
//     transcript event is documented for PUSH_TO_TALK only.
//   - partialMode CUMULATIVE (the default) makes each partial the whole text
//     of the current turn so far, which is the shape Deepgram's interim
//     results already present through transcript.delta; DELTA would make
//     each partial an increment and cannot express a revision.
//   - Timestamps are turn-level only. There are no word timings.
//   - The prerecorded endpoint is POST https://api.meta.ai/v1/asr/transcribe,
//     multipart/form-data with a `request` part (application/json: mode,
//     model, audioEncoding "WAV") and an `audio` part holding a RIFF/WAVE
//     file. The JSON answer is {sessionId, transcript, audioDurationMs,
//     turns: [{turnId, startMs, endMs, transcript, speaker}]}. Limits are a
//     32 MB request body and ten minutes of audio; 400 is an unsupported or
//     over-long file, 413 an oversized body, 429 a rate or concurrency limit.
//   - Tenancy limits: eight concurrent streams, one thousand streams per
//     hour, sixty minutes per session. List price is $0.18 per hour of
//     processed audio.
//   - Twenty-five languages with code-switching. languageBias takes language
//     NAMES ("English"), not BCP-47 tags, so a caller's tag is translated
//     through a fixed table and an unknown tag is dropped rather than sent.
//
// # Mode selection
//
// The relay's STT contract streams continuous audio and expects a final per
// detected turn, so the socket is opened in ENDPOINTING mode, or DIARIZATION
// when the caller asks for speaker labels. PUSH_TO_TALK is single-turn and
// user-delimited, which would hold every final until the caller ends the
// stream; it is not offered.
//
// Neither route is routable until its live canary passes, which is the gate
// that catches a wrong id or a changed handshake before a customer pays
// admission latency for it.
package meta
