package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Realtime speech translation (openai.realtime.translation.v1).
//
// A translation session is configured by ONE native command, session.update,
// whose session object holds nothing but audio settings: the target language
// under audio.output.language, and optionally the source-transcript model and
// noise reduction under audio.input. There is no voice, no instructions, no
// tools and no turn detection — the vendor translates a continuous stream —
// so the hop accepts exactly that shape and refuses every other field rather
// than forwarding a conversational session body the translation socket would
// reject mid-session (or, worse, silently ignore).
//
// Vendor reference, read 2026-09-28:
// https://developers.openai.com/api/docs/guides/realtime-translation

// TranslationOutputLanguages are the target languages gpt-realtime-translate
// speaks, in the vendor's published order. Source speech is auto-detected, so
// only the output side is named.
var TranslationOutputLanguages = []string{"es", "pt", "fr", "ja", "ru", "zh", "de", "ko", "hi", "id", "vi", "it", "en"}

// TranslationTranscriptionModels are the source-transcript models the
// translation session accepts under audio.input.transcription.model. Only the
// vendor's documented one is admitted: a transcription model may be priced
// on its own, and a caller must not be able to attach an unpriced one to a
// session billed as translation minutes.
var TranslationTranscriptionModels = []string{"gpt-realtime-whisper"}

// TranslationNoiseReductionTypes are the audio.input.noise_reduction.type
// values the vendor documents for its realtime audio input.
var TranslationNoiseReductionTypes = []string{"near_field", "far_field"}

// ValidTranslationOutputLanguage reports whether language is a supported
// target language.
func ValidTranslationOutputLanguage(language string) bool {
	return containsString(TranslationOutputLanguages, language)
}

// TranslationSessionUpdate is the decoded, validated translation
// session.update. The hop forwards the caller's own bytes once they validate
// (native identifiers such as event_id survive); this value is what the hop
// learned from them.
type TranslationSessionUpdate struct {
	EventID string
	// Language is the target language, "" when this update leaves it as is.
	Language string
	// TranscriptionModel is the source-transcript model, "" when unchanged.
	// TranscriptionDisabled reports an explicit null.
	TranscriptionModel    string
	TranscriptionDisabled bool
	// NoiseReduction is the noise reduction type, "" when unchanged.
	// NoiseReductionDisabled reports an explicit null.
	NoiseReduction         string
	NoiseReductionDisabled bool
}

// ValidateTranslationSessionUpdate strictly decodes a translation
// session.update. Unknown fields are refused at every level.
func ValidateTranslationSessionUpdate(payload []byte) (TranslationSessionUpdate, error) {
	var result TranslationSessionUpdate
	var envelope struct {
		Type    string          `json:"type"`
		EventID string          `json:"event_id"`
		Session json.RawMessage `json:"session"`
	}
	if err := decodeStrict(payload, &envelope); err != nil {
		return result, err
	}
	if envelope.Type != "session.update" {
		return result, fmt.Errorf("type: must be session.update")
	}
	if len(envelope.EventID) > 256 {
		return result, fmt.Errorf("event_id: at most 256 bytes")
	}
	result.EventID = envelope.EventID
	if !isJSONObject(envelope.Session) {
		return result, fmt.Errorf("session: must be an object")
	}
	var session struct {
		Audio json.RawMessage `json:"audio"`
	}
	if err := decodeStrict(envelope.Session, &session); err != nil {
		return result, fmt.Errorf("session: %w (a translation session carries only audio settings)", err)
	}
	if len(session.Audio) == 0 {
		return result, fmt.Errorf("session.audio: required")
	}
	if !isJSONObject(session.Audio) {
		return result, fmt.Errorf("session.audio: must be an object")
	}
	var audio struct {
		Input  json.RawMessage `json:"input"`
		Output json.RawMessage `json:"output"`
	}
	if err := decodeStrict(session.Audio, &audio); err != nil {
		return result, fmt.Errorf("session.audio: %w", err)
	}
	if len(audio.Input) == 0 && len(audio.Output) == 0 {
		return result, fmt.Errorf("session.audio: names neither input nor output settings")
	}
	if len(audio.Output) > 0 {
		if !isJSONObject(audio.Output) {
			return result, fmt.Errorf("session.audio.output: must be an object")
		}
		var output struct {
			Language *string `json:"language"`
		}
		if err := decodeStrict(audio.Output, &output); err != nil {
			return result, fmt.Errorf("session.audio.output: %w (the output voice and format are fixed)", err)
		}
		if output.Language == nil {
			return result, fmt.Errorf("session.audio.output.language: required when output is present")
		}
		if !ValidTranslationOutputLanguage(*output.Language) {
			return result, fmt.Errorf("session.audio.output.language: got %q, want one of %s", *output.Language, strings.Join(TranslationOutputLanguages, ", "))
		}
		result.Language = *output.Language
	}
	if len(audio.Input) > 0 {
		if !isJSONObject(audio.Input) {
			return result, fmt.Errorf("session.audio.input: must be an object")
		}
		var input struct {
			Transcription  json.RawMessage `json:"transcription"`
			NoiseReduction json.RawMessage `json:"noise_reduction"`
		}
		if err := decodeStrict(audio.Input, &input); err != nil {
			return result, fmt.Errorf("session.audio.input: %w (the input format is fixed)", err)
		}
		if len(input.Transcription) > 0 {
			if isJSONNull(input.Transcription) {
				result.TranscriptionDisabled = true
			} else {
				var transcription struct {
					Model string `json:"model"`
				}
				if err := decodeStrict(input.Transcription, &transcription); err != nil {
					return result, fmt.Errorf("session.audio.input.transcription: %w", err)
				}
				if !containsString(TranslationTranscriptionModels, transcription.Model) {
					return result, fmt.Errorf("session.audio.input.transcription.model: got %q, want one of %s", transcription.Model, strings.Join(TranslationTranscriptionModels, ", "))
				}
				result.TranscriptionModel = transcription.Model
			}
		}
		if len(input.NoiseReduction) > 0 {
			if isJSONNull(input.NoiseReduction) {
				result.NoiseReductionDisabled = true
			} else {
				var reduction struct {
					Type string `json:"type"`
				}
				if err := decodeStrict(input.NoiseReduction, &reduction); err != nil {
					return result, fmt.Errorf("session.audio.input.noise_reduction: %w", err)
				}
				if !containsString(TranslationNoiseReductionTypes, reduction.Type) {
					return result, fmt.Errorf("session.audio.input.noise_reduction.type: got %q, want one of %s", reduction.Type, strings.Join(TranslationNoiseReductionTypes, ", "))
				}
				result.NoiseReduction = reduction.Type
			}
		}
	}
	return result, nil
}

// decodeStrict decodes one JSON object refusing unknown fields and trailing
// data.
func decodeStrict(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.More() {
		return errors.New("trailing data after the JSON object")
	}
	return nil
}

func isJSONNull(raw json.RawMessage) bool { return strings.TrimSpace(string(raw)) == "null" }
