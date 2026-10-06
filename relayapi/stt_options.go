package relayapi

import (
	"fmt"
	"math"
	"strings"
	"unicode"
	"unicode/utf8"
)

// STTOptions carries caller transcription options for both STT surfaces.
type STTOptions struct {
	Diarization    *bool    `json:"diarization,omitempty"`
	Keywords       []string `json:"keywords,omitempty"`
	NoiseReduction *bool    `json:"noise_reduction,omitempty"`
	// WordTimestamps asks for per-word start/end timings on the batch result
	// (TranscriptionResponse.Words). A pointer like Diarization: nil is
	// silence, false is a statement.
	WordTimestamps *bool `json:"word_timestamps,omitempty"`
	// Translation asks for the spoken audio translated into another
	// language, returned BESIDE the transcript: `text` stays the original
	// words so a caller that ignores translation reads exactly what it read
	// before, and the translated words ride a separate `translation` field.
	// Fails closed like every canonical ask: a route that cannot translate
	// is refused with capability_unsupported naming "translation".
	Translation     *STTTranslation           `json:"translation,omitempty"`
	ProviderOptions map[string]map[string]any `json:"provider_options,omitempty"`
}

// STTTranslation names the language the transcript is translated into. One
// way only: the source language is whatever was spoken (or the request's
// language hint), so there is nothing else for a caller to say.
type STTTranslation struct {
	// TargetLanguage is a BCP-47 tag ("es", "pt-BR", "zh-Hans"). Adapters
	// pass the primary subtag in the vendor's own spelling.
	TargetLanguage string `json:"target_language"`
}

// MaxSTTLanguageTagLength bounds a translation target. 35 characters is the
// longest tag RFC 5646 section 4.4.1 asks implementations to accept, far
// beyond any tag a vendor lists.
const MaxSTTLanguageTagLength = 35

// Bounds on caller input, matching the local gateway's protocol.SttOptions.
const (
	MaxSTTKeywords          = 100
	MaxSTTKeywordLength     = 64
	MaxSTTOptionProviders   = 8
	MaxSTTOptionKeys        = 16
	MaxSTTOptionStringValue = 256
)

// reservedSTTOptionKeys are settings the relay itself owns.
var reservedSTTOptionKeys = map[string]struct{}{
	"model": {}, "model_id": {}, "speech_model": {},
	"language": {}, "language_code": {}, "language_codes": {}, "language_hints": {},
	"detect_language": {}, "language_detection": {}, "enable_language_identification": {},
	"encoding": {}, "audio_format": {}, "sample_rate": {}, "bit_depth": {}, "channels": {},
	"api_key": {}, "token": {}, "authorization": {},
	"diarize": {}, "diarization": {}, "enable_speaker_diarization": {},
	"keywords": {}, "keyterm": {}, "keyterms": {}, "keyterms_prompt": {}, "custom_vocabulary": {},
	"format_turns": {}, "interim_results": {}, "include_partial_turns": {},
	"include_timestamps": {}, "commit_strategy": {}, "intent": {},
	"word_timestamps": {}, "timestamp_granularities": {}, "timestamps": {},
	// translation rides the canonical Translation field: forwarded as a
	// provider setting it would skip the capability gate and change what
	// the transcript's text means.
	"translation": {}, "translation_config": {}, "target_language": {},
}

// IsZero reports whether the caller asked for nothing.
func (o *STTOptions) IsZero() bool {
	return o == nil ||
		(o.Diarization == nil && len(o.Keywords) == 0 && o.NoiseReduction == nil && o.WordTimestamps == nil && o.Translation == nil && len(o.ProviderOptions) == 0)
}

// TranslationTarget returns the trimmed target language, or "" when the
// caller asked for no translation.
func (o *STTOptions) TranslationTarget() string {
	if o == nil || o.Translation == nil {
		return ""
	}
	return strings.TrimSpace(o.Translation.TargetLanguage)
}

// WantsWordTimestamps reports whether the caller asked for per-word timings.
func (o *STTOptions) WantsWordTimestamps() bool {
	return o != nil && o.WordTimestamps != nil && *o.WordTimestamps
}

// Diarize reports whether the caller asked for speaker labels.
func (o *STTOptions) Diarize() bool {
	return o != nil && o.Diarization != nil && *o.Diarization
}

// ReduceNoise reports whether the caller asked for audio enhancement.
func (o *STTOptions) ReduceNoise() bool {
	return o != nil && o.NoiseReduction != nil && *o.NoiseReduction
}

// GetKeywords returns the caller's vocabulary-biasing terms, trimmed.
func (o *STTOptions) GetKeywords() []string {
	if o == nil || len(o.Keywords) == 0 {
		return nil
	}
	keywords := make([]string, 0, len(o.Keywords))
	for _, keyword := range o.Keywords {
		if trimmed := strings.TrimSpace(keyword); trimmed != "" {
			keywords = append(keywords, trimmed)
		}
	}
	return keywords
}

// Validate checks shape and bounds. Names must arrive lower case: the
// idempotency content hash covers the request bytes as sent, so one intent
// must have one spelling.
func (o *STTOptions) Validate() error {
	if o == nil {
		return nil
	}
	if len(o.Keywords) > MaxSTTKeywords {
		return fmt.Errorf("keywords: at most %d are accepted", MaxSTTKeywords)
	}
	for i, keyword := range o.Keywords {
		trimmed := strings.TrimSpace(keyword)
		if trimmed == "" || utf8.RuneCountInString(trimmed) > MaxSTTKeywordLength || hasControlRune(trimmed) {
			return fmt.Errorf("keywords[%d]: must be 1-%d characters with no control characters", i, MaxSTTKeywordLength)
		}
	}
	if o.Translation != nil && !ValidSTTLanguageTag(o.TranslationTarget()) {
		return fmt.Errorf("translation.target_language: must be a BCP-47 language tag of at most %d characters, such as \"es\" or \"pt-BR\"", MaxSTTLanguageTagLength)
	}
	if len(o.ProviderOptions) == 0 {
		return nil
	}
	if len(o.ProviderOptions) > MaxSTTOptionProviders {
		return fmt.Errorf("provider_options: may name at most %d providers", MaxSTTOptionProviders)
	}
	for provider, settings := range o.ProviderOptions {
		if err := validateSTTOptionProvider(provider, settings); err != nil {
			return err
		}
	}
	return nil
}

func validateSTTOptionProvider(provider string, settings map[string]any) error {
	if strings.TrimSpace(provider) == "" {
		return fmt.Errorf("provider_options: names an empty provider")
	}
	if provider != strings.ToLower(provider) {
		return fmt.Errorf("provider_options.%s: provider id must be lower case", provider)
	}
	if len(settings) > MaxSTTOptionKeys {
		return fmt.Errorf("provider_options.%s: may carry at most %d settings", provider, MaxSTTOptionKeys)
	}
	for key, value := range settings {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("provider_options.%s: names an empty setting", provider)
		}
		if key != strings.ToLower(key) {
			return fmt.Errorf("provider_options.%s.%s: setting name must be lower case", provider, key)
		}
		if _, reserved := reservedSTTOptionKeys[key]; reserved {
			return fmt.Errorf("provider_options.%s.%s: is owned by the relay and cannot be forwarded", provider, key)
		}
		if err := validateSTTOptionValue(provider, key, value); err != nil {
			return err
		}
	}
	return nil
}

// validateSTTOptionValue accepts finite scalars only.
func validateSTTOptionValue(provider, key string, value any) error {
	switch typed := value.(type) {
	case bool:
		return nil
	case float64:
		// NaN and ±Inf have no JSON representation, so they would fail at
		// marshal time instead of being refused here.
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			return fmt.Errorf("provider_options.%s.%s: must be a finite number", provider, key)
		}
		return nil
	case int, int64:
		return nil
	case string:
		if utf8.RuneCountInString(typed) > MaxSTTOptionStringValue || hasControlRune(typed) {
			return fmt.Errorf("provider_options.%s.%s: must be a string of at most %d characters with no control characters", provider, key, MaxSTTOptionStringValue)
		}
		return nil
	default:
		return fmt.Errorf("provider_options.%s.%s: must be a boolean, a number, or a string", provider, key)
	}
}

func hasControlRune(text string) bool {
	return strings.ContainsFunc(text, unicode.IsControl)
}

// ValidSTTLanguageTag accepts the BCP-47 shape a translation target needs: a
// 2-3 letter primary language subtag, then hyphen-separated alphanumeric
// subtags of 1-8 characters, 35 characters in all. It is a shape check, not
// a registry lookup: whether a vendor translates INTO the language is the
// vendor's answer, and a tag outside its table is refused by the vendor with
// an error rather than served wrong. "auto" and other non-languages fail the
// primary-subtag rule, because a translation target cannot be detected.
func ValidSTTLanguageTag(tag string) bool {
	if tag == "" || len(tag) > MaxSTTLanguageTagLength {
		return false
	}
	for i, subtag := range strings.Split(tag, "-") {
		if i == 0 {
			if len(subtag) < 2 || len(subtag) > 3 || !isASCIIAlnum(subtag, false) {
				return false
			}
			continue
		}
		if len(subtag) < 1 || len(subtag) > 8 || !isASCIIAlnum(subtag, true) {
			return false
		}
	}
	return true
}

func isASCIIAlnum(text string, digits bool) bool {
	for _, r := range text {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z':
		case digits && r >= '0' && r <= '9':
		default:
			return false
		}
	}
	return true
}
