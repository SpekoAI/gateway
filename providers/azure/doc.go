// Package azure is the relay's integration for Microsoft's MAI-Transcribe
// speech-to-text models (MAI-Transcribe-2 by default), served through the
// Azure Speech fast-transcription REST endpoint in "enhanced mode". It is a
// prerecorded (batch) adapter only.
//
// # Wire facts this package is built on
//
// Taken from learn.microsoft.com/azure/ai-services/speech-service/mai-transcribe
// and the fast-transcription / LLM Speech REST guides (read 2026-09-03):
//
//   - One synchronous call: POST https://{host}/speechtotext/transcriptions:transcribe
//     with the query api-version=2025-10-15. {host} is either the regional
//     Speech host {region}.api.cognitive.microsoft.com or a resource's own
//     {resource}.cognitiveservices.azure.com. MAI-Transcribe is served in
//     eastus, northeurope, southeastasia and westus; the catalog row names
//     the eastus regional host.
//   - Authentication is the Speech resource key in the
//     Ocp-Apim-Subscription-Key header (an Entra bearer is the documented
//     alternative; the relay holds a key).
//   - The body is multipart/form-data: an `audio` file part (WAV, MP3 or
//     FLAC; the relay always sends RIFF/WAVE) and a `definition` text part
//     holding JSON. enhancedMode.enabled=true plus enhancedMode.model
//     selects a MAI model; without it the request runs Azure's classic fast
//     transcription. Optional: locales (ONE bare ISO-639-1 code for MAI, the
//     model auto-detects and code-switches otherwise), diarization.enabled,
//     phraseList.phrases (keyword biasing), modelOptions.timestamps
//     ("word" | "segment" | "none", default none) and
//     modelOptions.transcribeStyle ("verbatim" default | "clean").
//   - The answer is {durationMilliseconds, combinedPhrases: [{channel,
//     text}], phrases: [{channel, speaker, offsetMilliseconds,
//     durationMilliseconds, text, locale, confidence, words: [...]}]}.
//     confidence is always 0 in enhanced mode, speaker is present only when
//     diarization was asked for, and word timings only for timestamps=word.
//   - Limits: audio under 300 MB and five hours; 400 is an unsupported or
//     over-long file, 413 an oversized body, 429 a quota or concurrency
//     limit. Launch list price is $0.10 per hour of audio.
//   - Sixty languages. locales takes bare codes ("en", "yue"), so a caller's
//     BCP-47 tag is reduced to its primary subtag and dropped when the model
//     does not list it.
//
// # Why batch only
//
// MAI-Transcribe-2 has no realtime endpoint of its own. Voice Live can run
// `mai-transcribe` as its input transcriber, but that resolves to
// MAI-Transcribe-1.5, requires a chat model on the session and does not
// diarize, so it cannot stand behind the relay's streaming STT contract.
// The catalog row is therefore BatchOnly and streaming selection skips it.
//
// The route is not routable until its live canary passes, which is the gate
// that catches a wrong model id or a changed definition schema before a
// customer pays admission latency for it.
package azure
