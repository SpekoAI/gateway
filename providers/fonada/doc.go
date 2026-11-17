// Package fonada implements the FonadaLabs (api.fonada.ai) Kapture v3
// speech-to-text adapter. FonadaLabs documents one prerecorded endpoint,
// POST /v3/asr/transcribe, and no streaming endpoint, so the package has a
// BatchTranscriber only.
//
// The request is multipart: the audio as `file` and a required `language_id`
// from eleven codes (en, bn, gu, hi, kn, ml, mr, or, pa, ta, te). FonadaLabs
// refuses any other code with 400 unsupported_language before it charges, and
// the adapter refuses it before upload. The response carries the whole
// transcript, chunk-level timings and the processed duration. It has no word
// timings, no speaker labels and no confidence.
//
// FonadaLabs bills ₹6 per audio hour, per second of audio, so the adapter
// reports the response's audio_duration_s as duration_seconds.
//
// Docs: https://fonadalabs.ai/docs/speech-to-text/kapture-v3 (read 2026-11-17).
package fonada
