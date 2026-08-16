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
	Diarization     *bool                     `json:"diarization,omitempty"`
	Keywords        []string                  `json:"keywords,omitempty"`
	NoiseReduction  *bool                     `json:"noise_reduction,omitempty"`
	ProviderOptions map[string]map[string]any `json:"provider_options,omitempty"`
}

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
}

// IsZero reports whether the caller asked for nothing.
func (o *STTOptions) IsZero() bool {
	return o == nil ||
		(o.Diarization == nil && len(o.Keywords) == 0 && o.NoiseReduction == nil && len(o.ProviderOptions) == 0)
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
