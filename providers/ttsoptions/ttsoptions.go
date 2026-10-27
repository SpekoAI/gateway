// Package ttsoptions answers whether a TTS route can honor the caller's
// provider_options, in one place for both edges that ask: the local gateway's
// session create and the hosted relay's routing. The per-vendor rules live in
// each provider package's ValidateTTSOptions, which that provider's adapter
// also calls, so what is admitted and what reaches the wire cannot drift.
package ttsoptions

import (
	"sort"

	"github.com/SpekoAI/gateway/protocol"
	"github.com/SpekoAI/gateway/providers/cartesia"
	"github.com/SpekoAI/gateway/providers/elevenlabs"
)

var validators = map[string]func(model string, settings map[string]any) error{
	"cartesia":   cartesia.ValidateTTSOptions,
	"elevenlabs": elevenlabs.ValidateTTSOptions,
}

// Accepts reports whether any route of this provider takes settings. A
// provider_options entry naming a provider outside this set is a misspelling
// or a vendor with no tunable synthesis settings; either way nothing would
// ever read it.
func Accepts(provider string) bool {
	_, ok := validators[provider]
	return ok
}

// Providers lists the providers that take settings, sorted, for error text.
func Providers() []string {
	names := make([]string, 0, len(validators))
	for name := range validators {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Validate fails closed on the routed provider: every setting addressed to it
// must be one its route forwards. Settings for OTHER providers are ignored by
// design, so a caller can tune two vendors and let routing pick. options must
// already be normalized (lower-case names).
func Validate(provider, model string, options *protocol.TtsOptions) error {
	settings := options.Provider(provider)
	if len(settings) == 0 {
		return nil
	}
	validate, ok := validators[provider]
	if !ok {
		return &protocol.TtsOptionError{Provider: provider, Key: firstKey(settings), Detail: "this provider accepts no synthesis settings"}
	}
	return validate(model, settings)
}

func firstKey(settings map[string]any) string {
	keys := make([]string, 0, len(settings))
	for key := range settings {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys[0]
}
