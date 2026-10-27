package ttsoptions

import (
	"errors"
	"testing"

	"github.com/SpekoAI/gateway/protocol"
)

func TestValidateChecksOnlyTheRoutedProvider(t *testing.T) {
	t.Parallel()
	options := &protocol.TtsOptions{ProviderOptions: map[string]map[string]any{
		"cartesia":   {"normalization": "en-IN"},
		"elevenlabs": {"stability": 0.4},
	}}
	for _, route := range []struct{ provider, model string }{
		{"cartesia", "sonic-3.6"}, {"elevenlabs", "eleven_v4_turbo"}, {"deepgram", "aura-2"},
	} {
		if err := Validate(route.provider, route.model, options); err != nil {
			t.Fatalf("Validate(%s/%s) = %v, want nil", route.provider, route.model, err)
		}
	}
	if err := Validate("cartesia", "sonic-3", options); err == nil {
		t.Fatal("normalization on sonic-3 must be refused")
	}
	foreign := &protocol.TtsOptions{ProviderOptions: map[string]map[string]any{"deepgram": {"mip_opt_out": true}}}
	var refusal *protocol.TtsOptionError
	if err := Validate("deepgram", "aura-2", foreign); !errors.As(err, &refusal) || refusal.Option() != "provider_options.deepgram.mip_opt_out" {
		t.Fatalf("settings for a provider that takes none = %v, want a refusal naming the setting", err)
	}
	if Accepts("deepgram") || !Accepts("cartesia") || !Accepts("elevenlabs") {
		t.Fatal("Accepts disagrees with the validator table")
	}
}
