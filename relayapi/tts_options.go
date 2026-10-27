package relayapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// TTSOptions carries caller synthesis options for both TTS surfaces: the
// POST /v1/tts/speech body and the streaming session.configure frame.
//
// There are no canonical asks yet, only ProviderOptions: the vendor's OWN
// settings, namespaced by provider name, e.g. {"cartesia": {"normalization":
// "en-IN"}, "elevenlabs": {"stability": 0.4}}. Settings addressed to a
// provider the request does not reach are ignored by design, so a caller can
// tune two vendors and let routing decide which one reads its own. Settings
// addressed to the routed provider fail closed: a key that provider's route
// does not forward is refused by name rather than accepted and dropped.
type TTSOptions struct {
	ProviderOptions map[string]map[string]any `json:"provider_options,omitempty"`
}

// TTSOptionsDecodeError is an options object the strict decoder refused,
// typically a misspelled member such as "provider_option". The edges match it
// to answer invalid_request naming the problem rather than "not valid JSON".
type TTSOptionsDecodeError struct {
	Err error
}

func (e *TTSOptionsDecodeError) Error() string { return "options: " + e.Err.Error() }

func (e *TTSOptionsDecodeError) Unwrap() error { return e.Err }

// UnmarshalJSON keeps the options object closed. encoding/json would drop a
// misspelled member, leaving empty options that validate and synthesize with
// vendor defaults: a 200 that silently did not do what was asked.
func (o *TTSOptions) UnmarshalJSON(data []byte) error {
	type wire TTSOptions
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var decoded wire
	if err := decoder.Decode(&decoded); err != nil {
		return &TTSOptionsDecodeError{Err: err}
	}
	*o = TTSOptions(decoded)
	return nil
}

// Bounds on caller input, matching the local gateway's protocol.TtsOptions.
const (
	MaxTTSOptionProviders = 8
	MaxTTSOptionKeys      = 16
)

// reservedTTSOptionKeys are settings the relay itself owns. Each would let a
// vendor setting override what routing, billing, or the audio contract
// already decided: a second model bills one model and runs another, and a
// different output format makes the caller misread the PCM.
var reservedTTSOptionKeys = map[string]struct{}{
	"model": {}, "model_id": {},
	"voice": {}, "voice_id": {}, "voices": {},
	"language": {}, "language_code": {},
	"output_format": {}, "encoding": {}, "sample_rate": {}, "container": {},
	"transcript": {}, "text": {}, "context_id": {},
	"add_timestamps": {}, "sync_alignment": {},
	"api_key": {}, "xi_api_key": {}, "token": {}, "authorization": {},
}

// IsZero reports whether the caller asked for nothing.
func (o *TTSOptions) IsZero() bool {
	return o == nil || len(o.ProviderOptions) == 0
}

// Provider returns the caller's settings for one provider. Nil when nothing
// was sent, so callers can range over it unconditionally.
func (o *TTSOptions) Provider(name string) map[string]any {
	if o == nil {
		return nil
	}
	return o.ProviderOptions[name]
}

// Validate checks shape and bounds. Names must arrive lower case and
// trimmed: the idempotency content hash covers the request bytes as sent, so
// one intent must have one spelling, and the connector's normalization (which
// folds and trims) must be a no-op on anything admitted here. Whether a
// provider accepts a named setting is a routing-time question the Router
// answers per route.
func (o *TTSOptions) Validate() error {
	if o == nil || len(o.ProviderOptions) == 0 {
		return nil
	}
	if len(o.ProviderOptions) > MaxTTSOptionProviders {
		return fmt.Errorf("provider_options: may name at most %d providers", MaxTTSOptionProviders)
	}
	for provider, settings := range o.ProviderOptions {
		if strings.TrimSpace(provider) == "" {
			return fmt.Errorf("provider_options: names an empty provider")
		}
		if provider != strings.ToLower(strings.TrimSpace(provider)) {
			return fmt.Errorf("provider_options.%s: provider id must be lower case with no surrounding spaces", provider)
		}
		if len(settings) > MaxTTSOptionKeys {
			return fmt.Errorf("provider_options.%s: may carry at most %d settings", provider, MaxTTSOptionKeys)
		}
		for key, value := range settings {
			if strings.TrimSpace(key) == "" {
				return fmt.Errorf("provider_options.%s: names an empty setting", provider)
			}
			if key != strings.ToLower(strings.TrimSpace(key)) {
				return fmt.Errorf("provider_options.%s.%s: setting name must be lower case with no surrounding spaces", provider, key)
			}
			if _, reserved := reservedTTSOptionKeys[key]; reserved {
				return fmt.Errorf("provider_options.%s.%s: is owned by the relay and cannot be forwarded", provider, key)
			}
			if err := validateSTTOptionValue(provider, key, value); err != nil {
				return err
			}
		}
	}
	return nil
}
