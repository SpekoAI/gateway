package protocol

import (
	"fmt"
	"strings"
)

// TtsOptions carries per-provider synthesis settings: the vendor's OWN knobs,
// namespaced by provider name, e.g. {"cartesia": {"normalization": "en-IN"},
// "elevenlabs": {"stability": 0.4}}.
//
// Like SttOptions.ProviderOptions, settings for a provider the session does
// not reach are ignored by design, and settings for the routed provider fail
// closed: each adapter owns an allow-list (its package's ValidateTTSOptions)
// and a key outside it is refused by name before a provider is dialed, never
// forwarded or dropped.
type TtsOptions struct {
	ProviderOptions map[string]map[string]any `json:"provider_options,omitempty"`
}

const (
	maxTtsOptionProviders = 8
	maxTtsOptionKeys      = 16
)

// reservedTtsOptionKeys are settings the gateway itself owns: the model, the
// voice, the language and the output format are routing and media decisions,
// and credentials are never caller input. Twin of relayapi's list.
var reservedTtsOptionKeys = map[string]struct{}{
	"model": {}, "model_id": {},
	"voice": {}, "voice_id": {}, "voices": {},
	"language": {}, "language_code": {},
	"output_format": {}, "encoding": {}, "sample_rate": {}, "container": {},
	"transcript": {}, "text": {}, "context_id": {},
	"add_timestamps": {}, "sync_alignment": {},
	"api_key": {}, "xi_api_key": {}, "token": {}, "authorization": {},
}

// IsZero reports whether the caller asked for nothing.
func (o *TtsOptions) IsZero() bool {
	return o == nil || len(o.ProviderOptions) == 0
}

// Provider returns the caller's settings for one provider. Nil when nothing
// was sent, so an adapter can range over it unconditionally.
func (o *TtsOptions) Provider(name string) map[string]any {
	if o == nil {
		return nil
	}
	return o.ProviderOptions[strings.ToLower(name)]
}

// Normalize folds provider and setting names to lower case and validates
// shape and bounds, exactly as SttOptions.Normalize does and for the same
// reason: a setting that validates under one casing and then matches nothing
// at the adapter is accepted-but-ignored.
func (o *TtsOptions) Normalize() error {
	if o == nil || len(o.ProviderOptions) == 0 {
		return nil
	}
	if len(o.ProviderOptions) > maxTtsOptionProviders {
		return fmt.Errorf("tts options: provider_options may name at most %d providers", maxTtsOptionProviders)
	}
	normalized := make(map[string]map[string]any, len(o.ProviderOptions))
	for provider, settings := range o.ProviderOptions {
		name := strings.ToLower(strings.TrimSpace(provider))
		if name == "" {
			return fmt.Errorf("tts options: provider_options names an empty provider")
		}
		if _, collision := normalized[name]; collision {
			return fmt.Errorf("tts options: provider_options names %q more than once across casings", name)
		}
		if len(settings) > maxTtsOptionKeys {
			return fmt.Errorf("tts options: provider %q may carry at most %d settings", name, maxTtsOptionKeys)
		}
		kept := make(map[string]any, len(settings))
		for key, value := range settings {
			folded := strings.ToLower(strings.TrimSpace(key))
			if folded == "" {
				return fmt.Errorf("tts options: provider %q names an empty setting", name)
			}
			if _, collision := kept[folded]; collision {
				return fmt.Errorf("tts options: provider %q names setting %q more than once across casings", name, folded)
			}
			if _, reserved := reservedTtsOptionKeys[folded]; reserved {
				return fmt.Errorf("tts options: provider_options.%s.%s is owned by the gateway and cannot be forwarded", name, folded)
			}
			if err := validateOptionScalar("tts options", name, folded, value); err != nil {
				return err
			}
			kept[folded] = value
		}
		normalized[name] = kept
	}
	o.ProviderOptions = normalized
	return nil
}

// TtsOptionError is a provider setting the routed provider's adapter refuses:
// a key it does not forward on this model, or a value outside the range the
// vendor accepts. Out-of-range values matter as much as unknown keys here:
// ElevenLabs answers an out-of-range voice setting on its multi-context socket
// with no audio and no error frame, so the only useful refusal is ours.
type TtsOptionError struct {
	Provider string
	Key      string
	Detail   string
}

func (e *TtsOptionError) Error() string {
	return fmt.Sprintf("provider_options.%s.%s: %s", e.Provider, e.Key, e.Detail)
}

// Option is the dotted name the refusal is about, for error envelopes that
// name the ask.
func (e *TtsOptionError) Option() string {
	return "provider_options." + e.Provider + "." + e.Key
}

// TtsOptionNumber reads a numeric provider setting in any spelling JSON
// decoding or a Go caller can produce.
func TtsOptionNumber(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	default:
		return 0, false
	}
}

// ValidLanguageTag is a BCP-47 shape check: a 2-3 letter primary subtag, then
// 1-8 character alphanumeric subtags, 35 characters in all. Shape only, not a
// registry lookup.
func ValidLanguageTag(tag string) bool {
	return validSttLanguageTag(tag)
}
