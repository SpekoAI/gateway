package protocol

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

// SttOptions carries what a caller asked a transcription session to do beyond
// returning text. The canonical fields use the same vocabulary as the Speko
// platform API's provider params, so a caller moving between the hosted API
// and this gateway does not learn a second spelling.
//
// Canonical asks are provider-neutral and fail closed: a session routed to a
// provider that cannot honor one is refused with the option named, never
// answered 200 with the feature silently missing. A diarization request served
// by a provider that cannot diarize is indistinguishable from a one-speaker
// recording, so nobody would ever report it.
//
// ProviderOptions are the vendor's OWN settings, namespaced by provider name.
// They never fail a route: settings for a provider the session does not reach
// are ignored by design, so a caller can tune two providers and let routing
// decide which one reads its own.
type SttOptions struct {
	// Diarization labels each transcript segment with the speaker who said it.
	// A pointer, not a bool: nil means the caller said nothing and the vendor
	// default stands, which is not the same as an explicit false.
	Diarization *bool `json:"diarization,omitempty"`
	// Keywords biases recognition toward domain terms (names, products,
	// jargon). Adapters translate to the vendor's own spelling: Deepgram
	// keyterm/keywords, AssemblyAI keyterms_prompt, ElevenLabs keyterms,
	// Soniox context terms, Gladia custom vocabulary, OpenAI prompt text.
	Keywords []string `json:"keywords,omitempty"`
	// NoiseReduction asks the provider to clean the audio before transcribing.
	// Canonical rather than a provider option because the one evidenced wire
	// shape (Gladia pre_processing.audio_enhancer) is a nested object, and
	// provider option values are deliberately scalar-only.
	NoiseReduction *bool `json:"noise_reduction,omitempty"`
	// ProviderOptions maps a provider name to that vendor's own settings,
	// e.g. {"deepgram": {"numerals": true}, "elevenlabs":
	// {"vad_silence_threshold_secs": 0.7}}. Scalars only; each adapter
	// forwards only the keys it allow-lists and refuses the rest by name.
	ProviderOptions map[string]map[string]any `json:"provider_options,omitempty"`
}

// Bounds on caller input. They exist because keywords and provider options are
// written into vendor URLs and config frames, and unbounded caller input would
// let a caller decide how large those frames are.
const (
	maxSttKeywords          = 100
	maxSttKeywordLength     = 64
	maxSttOptionProviders   = 8
	maxSttOptionKeys        = 16
	maxSttOptionStringValue = 256
)

// reservedSttOptionKeys are settings the gateway itself owns. Every one of
// these would produce a 200 that is wrong rather than an error: a second
// model bills one model and runs another, a wrong encoding makes the vendor
// misread the PCM, and a diarize smuggled past the canonical field skips the
// fail-closed support check that field exists to provide.
var reservedSttOptionKeys = map[string]struct{}{
	"model": {}, "model_id": {}, "speech_model": {},
	"language": {}, "language_code": {}, "language_codes": {}, "language_hints": {},
	// detect_language routes around the gateway-owned language pin: a vendor
	// told to auto-detect ignores the language this gateway resolved.
	"detect_language": {}, "language_detection": {}, "enable_language_identification": {},
	"encoding": {}, "audio_format": {}, "sample_rate": {}, "bit_depth": {}, "channels": {},
	"api_key": {}, "token": {}, "authorization": {},
	"diarize": {}, "diarization": {}, "enable_speaker_diarization": {},
	"keywords": {}, "keyterm": {}, "keyterms": {}, "keyterms_prompt": {}, "custom_vocabulary": {},
	"format_turns": {}, "interim_results": {}, "include_partial_turns": {},
	"include_timestamps": {}, "commit_strategy": {}, "intent": {},
}

// IsZero reports whether the caller asked for nothing, which is every request
// that predates this type.
func (o *SttOptions) IsZero() bool {
	return o == nil ||
		(o.Diarization == nil && len(o.Keywords) == 0 && o.NoiseReduction == nil && len(o.ProviderOptions) == 0)
}

// Diarize reports whether the caller asked for speaker labels.
func (o *SttOptions) Diarize() bool {
	return o != nil && o.Diarization != nil && *o.Diarization
}

// ReduceNoise reports whether the caller asked for audio enhancement.
func (o *SttOptions) ReduceNoise() bool {
	return o != nil && o.NoiseReduction != nil && *o.NoiseReduction
}

// GetKeywords returns the caller's vocabulary-biasing terms, trimmed. Nil-safe
// so adapters can ask unconditionally.
func (o *SttOptions) GetKeywords() []string {
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

// Provider returns the caller's settings for one provider, already normalized
// to lower-case names by Normalize. Nil when nothing was sent, so an adapter
// can range over it unconditionally.
func (o *SttOptions) Provider(name string) map[string]any {
	if o == nil {
		return nil
	}
	return o.ProviderOptions[strings.ToLower(name)]
}

// ProviderKeys returns the caller's setting names for one provider in a stable
// order, so a URL built from them is byte-identical run to run.
func (o *SttOptions) ProviderKeys(name string) []string {
	settings := o.Provider(name)
	keys := make([]string, 0, len(settings))
	for key := range settings {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// Normalize folds provider and setting names to lower case and validates
// shape and bounds. Folding happens here, once, because validation folds case
// and map lookup cannot: a document written {"Deepgram":{"Numerals":true}}
// must not validate and then match nothing at the adapter — accepted but
// silently never forwarded is the failure mode this type exists to prevent.
//
// Normalize checks SHAPE only. Whether a named provider exists and whether it
// accepts a named setting are routing-time questions answered against the
// deployment's support table, which this package does not have.
func (o *SttOptions) Normalize() error {
	if o == nil {
		return nil
	}
	if len(o.Keywords) > maxSttKeywords {
		return fmt.Errorf("stt options: at most %d keywords are accepted", maxSttKeywords)
	}
	for _, keyword := range o.Keywords {
		trimmed := strings.TrimSpace(keyword)
		if trimmed == "" || len(trimmed) > maxSttKeywordLength || hasControlRune(trimmed) {
			return fmt.Errorf("stt options: each keyword must be 1-%d characters with no control characters", maxSttKeywordLength)
		}
	}
	if len(o.ProviderOptions) == 0 {
		return nil
	}
	if len(o.ProviderOptions) > maxSttOptionProviders {
		return fmt.Errorf("stt options: provider_options may name at most %d providers", maxSttOptionProviders)
	}
	normalized := make(map[string]map[string]any, len(o.ProviderOptions))
	for provider, settings := range o.ProviderOptions {
		name := strings.ToLower(strings.TrimSpace(provider))
		if name == "" {
			return fmt.Errorf("stt options: provider_options names an empty provider")
		}
		if len(settings) > maxSttOptionKeys {
			return fmt.Errorf("stt options: provider %q may carry at most %d settings", name, maxSttOptionKeys)
		}
		kept := make(map[string]any, len(settings))
		for key, value := range settings {
			folded := strings.ToLower(strings.TrimSpace(key))
			if folded == "" {
				return fmt.Errorf("stt options: provider %q names an empty setting", name)
			}
			if _, reserved := reservedSttOptionKeys[folded]; reserved {
				return fmt.Errorf("stt options: provider_options.%s.%s is owned by the gateway and cannot be forwarded", name, folded)
			}
			if err := validateSttOptionValue(name, folded, value); err != nil {
				return err
			}
			kept[folded] = value
		}
		normalized[name] = kept
	}
	o.ProviderOptions = normalized
	return nil
}

// validateSttOptionValue accepts scalars only. Every evidenced vendor setting
// is a boolean, a number, or a short string, and a scalar is the one shape
// that can be written into a query parameter AND a JSON config frame without
// the gateway deciding what nesting means for a vendor whose field is flat.
// Strings must carry no control characters: several adapters write values
// into URLs and frames verbatim, where a CR/LF could mint fields validation
// never saw.
func validateSttOptionValue(provider, key string, value any) error {
	switch typed := value.(type) {
	case bool:
		return nil
	case float64, int, int64:
		return nil
	case string:
		if len(typed) > maxSttOptionStringValue || hasControlRune(typed) {
			return fmt.Errorf("stt options: provider_options.%s.%s must be a string of at most %d characters with no control characters", provider, key, maxSttOptionStringValue)
		}
		return nil
	default:
		return fmt.Errorf("stt options: provider_options.%s.%s must be a boolean, a number, or a string", provider, key)
	}
}

func hasControlRune(text string) bool {
	return strings.ContainsFunc(text, unicode.IsControl)
}

// SttOptionString writes a scalar provider option the way a caller writing
// the query parameter by hand would: strings unquoted, numbers and booleans
// in their JSON spelling. Shared by the adapters that spell settings into
// URLs so two vendors cannot disagree about what a value looks like.
func SttOptionString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case bool:
		return strconv.FormatBool(typed)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	default:
		return fmt.Sprintf("%v", typed)
	}
}
