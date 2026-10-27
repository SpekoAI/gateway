package protocol

import (
	"strings"
	"testing"
)

func TestTtsOptionsNormalizeFoldsNamesAndRefusesBadShapes(t *testing.T) {
	t.Parallel()
	options := &TtsOptions{ProviderOptions: map[string]map[string]any{" ElevenLabs ": {"Stability": 0.4}}}
	if err := options.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if got := options.Provider("elevenlabs")["stability"]; got != 0.4 {
		t.Fatalf("folded options = %v, want elevenlabs.stability 0.4", options.ProviderOptions)
	}
	for _, test := range []struct {
		name    string
		options TtsOptions
		want    string
	}{
		{"casing collision", TtsOptions{ProviderOptions: map[string]map[string]any{"cartesia": {"normalization": "en"}, "Cartesia": {"normalization": "hi"}}}, "more than once"},
		{"reserved", TtsOptions{ProviderOptions: map[string]map[string]any{"cartesia": {"Language": "en"}}}, "owned by the gateway"},
		{"nested", TtsOptions{ProviderOptions: map[string]map[string]any{"elevenlabs": {"voice_settings": map[string]any{}}}}, "must be a boolean, a number, or a string"},
		{"control character", TtsOptions{ProviderOptions: map[string]map[string]any{"cartesia": {"normalization": "en\n"}}}, "no control characters"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := test.options.Normalize()
			if err == nil || !strings.Contains(err.Error(), test.want) || !strings.HasPrefix(err.Error(), "tts options: ") {
				t.Fatalf("Normalize = %v, want a tts options error mentioning %q", err, test.want)
			}
		})
	}
}
