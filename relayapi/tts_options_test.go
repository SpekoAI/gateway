package relayapi_test

import (
	"strings"
	"testing"

	"github.com/SpekoAI/gateway/relayapi"
)

func TestTTSOptionsValidation(t *testing.T) {
	t.Parallel()
	var absent *relayapi.TTSOptions
	if !absent.IsZero() || absent.Validate() != nil || absent.Provider("cartesia") != nil {
		t.Fatal("absent options must be zero and valid")
	}
	valid := relayapi.TTSOptions{ProviderOptions: map[string]map[string]any{
		"cartesia":   {"normalization": "en-IN"},
		"elevenlabs": {"stability": 0.4, "use_speaker_boost": true},
	}}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid options refused: %v", err)
	}
	for _, test := range []struct {
		name    string
		options relayapi.TTSOptions
		want    string
	}{
		{"upper-case provider", relayapi.TTSOptions{ProviderOptions: map[string]map[string]any{"Cartesia": {"normalization": "en"}}}, "provider_options.Cartesia"},
		{"upper-case setting", relayapi.TTSOptions{ProviderOptions: map[string]map[string]any{"cartesia": {"Normalization": "en"}}}, "provider_options.cartesia.Normalization"},
		{"reserved setting", relayapi.TTSOptions{ProviderOptions: map[string]map[string]any{"elevenlabs": {"model_id": "eleven_v3"}}}, "owned by the relay"},
		{"nested value", relayapi.TTSOptions{ProviderOptions: map[string]map[string]any{"elevenlabs": {"voice_settings": map[string]any{"stability": 0.4}}}}, "provider_options.elevenlabs.voice_settings"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := test.options.Validate()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate = %v, want an error mentioning %q", err, test.want)
			}
		})
	}
}

func TestTTSRequestsValidateTheirOptions(t *testing.T) {
	t.Parallel()
	bad := &relayapi.TTSOptions{ProviderOptions: map[string]map[string]any{"cartesia": {"model": "sonic-3"}}}
	audio := relayapi.AudioConfig{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1}
	speech := relayapi.SpeechRequest{Routing: relayapi.Routing{}.NormalizeDefault(), Input: "hi", Audio: audio, Options: bad}
	if err := speech.Validate(); err == nil || !strings.HasPrefix(err.Error(), "options: ") {
		t.Fatalf("SpeechRequest.Validate = %v, want an options error", err)
	}
	configure := relayapi.TTSSessionConfigure{Type: relayapi.TTSControlSessionConfigure, Routing: relayapi.Routing{}.NormalizeDefault(), Audio: audio, Options: bad}
	if err := configure.Validate(); err == nil || !strings.HasPrefix(err.Error(), "options: ") {
		t.Fatalf("TTSSessionConfigure.Validate = %v, want an options error", err)
	}
}
