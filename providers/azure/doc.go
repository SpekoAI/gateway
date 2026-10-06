// Package azure is the relay's integration for Microsoft's in-house MAI
// speech models on Azure. It carries three adapters:
//
//   - BatchAdapter: MAI-Transcribe-2 prerecorded speech-to-text through the
//     Azure Speech fast-transcription REST endpoint in "enhanced mode"
//     (batch.go; wire facts below).
//   - RealtimeAdapter: MAI-Transcribe-2-Streaming realtime speech-to-text
//     through the Microsoft Foundry Realtime API (realtime.go).
//   - TTSAdapter: MAI-Voice-2.1 and MAI-Voice-2.1-Flash text-to-speech
//     through the Speech service's SSML REST action (tts.go).
//
// The batch and TTS adapters authenticate with an Azure Speech resource key;
// the realtime adapter with the key of a Foundry resource that DEPLOYS the
// streaming model, which is a different credential (see realtime.go).
//
// # MAI-Transcribe-2-Streaming wire facts
//
// Taken from learn.microsoft.com/azure/ai-services/speech-service/
// mai-transcribe-2-streaming-realtime (read 2026-10-02; public preview):
//
//   - wss://{resource}.services.ai.azure.com/mai/v1/realtime?intent=transcription,
//     with the resource key in an `api-key` handshake header (a Microsoft
//     Entra bearer is the documented alternative). The protocol is
//     OpenAI-Realtime-like JSON.
//   - session.update{session:{type:"transcription", audio:{input:{format:
//     {type:"audio/pcm", rate:16000|24000}, transcription:{model:<deployment
//     name>, language:<code> or omitted}, turn_detection:null,
//     noise_reduction:null}}}} before any audio; settings lock at the first
//     append.
//   - input_audio_buffer.append carries base64 PCM16 mono. There is no
//     server-side speech detection: input_audio_buffer.commit asks for a
//     final ("completed") of everything since the previous commit.
//   - Server events: `delta` is newly FINALIZED text (concatenated verbatim),
//     `intermediate` (MAI-specific) is the provisional suffix after the last
//     delta, replacing the previous one; `completed` carries the commit
//     window's transcript. No word timings, confidence or language ids.
//   - Sessions last at most one hour. Sixty languages (the batch table), with
//     multilingual auto-detection when language is OMITTED — live, the
//     service refuses an explicit null, and its language enum is not the
//     batch table (tl, no; no as/bn/gu/ml/or/pa/te/yue). Served globally from
//     swedencentral, centralus, southindia/southeastasia (eastus2 coming).
//     Introductory price $0.54 per audio hour through 2026-12-31.
//
// # MAI-Voice-2.1 wire facts
//
// Taken from learn.microsoft.com/azure/ai-services/speech-service/mai-voices
// (read 2026-10-02; public preview): the same SSML action as every Azure
// prebuilt voice, POST https://{region}.tts.speech.microsoft.com/
// cognitiveservices/v1 with Ocp-Apim-Subscription-Key and
// X-Microsoft-OutputFormat. The MODEL is the voice-name suffix
// (`en-US-Harper:MAI-Voice-2.1`, `…:MAI-Voice-2.1-Flash`); every persona
// serves both models. 2.1 is the long-form, highest-fidelity tier ($22 per 1M
// characters), Flash the low-latency agent tier ($15 per 1M).
//
// # MAI-Transcribe-2 (batch) wire facts
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
// # Why MAI-Transcribe-2 is batch only
//
// MAI-Transcribe-2 has no realtime endpoint of its own. Voice Live can run
// `mai-transcribe` as its input transcriber, but that needs a chat model on
// the session, so it cannot stand behind the relay's streaming STT
// contract. Realtime MAI transcription is the separate
// MAI-Transcribe-2-Streaming model above, which the batch adapter refuses.
//
// The route is not routable until its live canary passes, which is the gate
// that catches a wrong model id or a changed definition schema before a
// customer pays admission latency for it.
package azure
