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
	stream := &protocol.TtsOptions{ProviderOptions: map[string]map[string]any{
		"cartesia": {"max_buffer_delay_ms": 300.0, "speed": 1.2},
	}}
	for _, model := range []string{"sonic-3.6", "sonic-3.5", "sonic-3"} {
		if err := Validate("cartesia", model, stream); err != nil {
			t.Fatalf("Validate(cartesia/%s, stream options) = %v, want nil", model, err)
		}
	}
	var speedRefusal *protocol.TtsOptionError
	tooFast := &protocol.TtsOptions{ProviderOptions: map[string]map[string]any{"cartesia": {"speed": 2.0}}}
	if err := Validate("cartesia", "sonic-3.6", tooFast); !errors.As(err, &speedRefusal) || speedRefusal.Option() != "provider_options.cartesia.speed" {
		t.Fatalf("out-of-range speed = %v, want a refusal naming it", err)
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
