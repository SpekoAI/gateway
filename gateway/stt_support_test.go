package gateway

import (
	"errors"
	"testing"

	"github.com/SpekoAI/gateway/protocol"
)

func boolPointer(value bool) *bool { return &value }

// Every STT provider in the catalog must either have a support row or be
// refused on any canonical ask. This is the default-deny guard: the next
// adapter someone adds cannot silently accept options and drop them.
func TestEverySttCatalogProviderFailsClosedOrHasASupportRow(t *testing.T) {
	t.Parallel()
	ask := &protocol.SttOptions{Diarization: boolPointer(true)}
	for _, entry := range providerCatalog {
		if entry.Kind != protocol.SessionKindSTT {
			continue
		}
		if _, known := sttOptionSupport[entry.Provider]; known {
			continue
		}
		err := validateSttRouteSupport(entry.Provider, entry.DefaultModel, ask)
		var supportError *SttSupportError
		if !errors.As(err, &supportError) {
			t.Fatalf("provider %q has no support row and must refuse any canonical ask, got %v", entry.Provider, err)
		}
	}
}

func TestSttRouteSupportFailsClosedByOptionAndProvider(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		provider string
		model    string
		options  *protocol.SttOptions
		refused  bool
		option   string
	}{
		{name: "nothing asked passes everywhere", provider: "cartesia", model: "ink-2", options: nil, refused: false},
		{name: "explicit false is not an ask", provider: "cartesia", model: "ink-2",
			options: &protocol.SttOptions{Diarization: boolPointer(false), NoiseReduction: boolPointer(false)}, refused: false},
		{name: "deepgram nova diarizes", provider: "deepgram", model: "nova-3",
			options: &protocol.SttOptions{Diarization: boolPointer(true)}, refused: false},
		{name: "deepgram flux has no diarization", provider: "deepgram", model: "flux-general-en",
			options: &protocol.SttOptions{Diarization: boolPointer(true)}, refused: true, option: "diarization"},
		{name: "assemblyai socket carries no speakers", provider: "assemblyai", model: "universal-3-5-pro",
			options: &protocol.SttOptions{Diarization: boolPointer(true)}, refused: true, option: "diarization"},
		{name: "soniox diarizes", provider: "soniox", model: "stt-rt-v3",
			options: &protocol.SttOptions{Diarization: boolPointer(true)}, refused: false},
		{name: "gladia cleans audio", provider: "gladia", model: "solaria-1",
			options: &protocol.SttOptions{NoiseReduction: boolPointer(true)}, refused: false},
		{name: "deepgram has no noise reduction", provider: "deepgram", model: "nova-3",
			options: &protocol.SttOptions{NoiseReduction: boolPointer(true)}, refused: true, option: "noise_reduction"},
		{name: "cartesia takes no options at all", provider: "cartesia", model: "ink-2",
			options: &protocol.SttOptions{Keywords: []string{"Speko"}}, refused: true, option: "keywords"},
		{name: "openai folds keywords into the prompt", provider: "openai", model: "gpt-4o-transcribe",
			options: &protocol.SttOptions{Keywords: []string{"Speko"}}, refused: false},
		{name: "an unlisted setting is refused by name", provider: "deepgram", model: "nova-3",
			options: &protocol.SttOptions{ProviderOptions: map[string]map[string]any{"deepgram": {"keyterm_boost": "high"}}},
			refused: true, option: "provider_options.deepgram.keyterm_boost"},
		{name: "settings for a provider the route did not pick are ignored", provider: "deepgram", model: "nova-3",
			options: &protocol.SttOptions{ProviderOptions: map[string]map[string]any{"elevenlabs": {"vad_silence_threshold_secs": 0.7}}},
			refused: false},
		{name: "deepgram forwards its allow-listed settings", provider: "deepgram", model: "nova-3",
			options: &protocol.SttOptions{ProviderOptions: map[string]map[string]any{"deepgram": {"numerals": true, "eot_threshold": 0.8}}},
			refused: false},
	}
	for _, testCase := range cases {
		err := validateSttRouteSupport(testCase.provider, testCase.model, testCase.options)
		if !testCase.refused {
			if err != nil {
				t.Fatalf("%s: unexpected refusal %v", testCase.name, err)
			}
			continue
		}
		var supportError *SttSupportError
		if !errors.As(err, &supportError) {
			t.Fatalf("%s: expected an SttSupportError, got %v", testCase.name, err)
		}
		if supportError.Option != testCase.option {
			t.Fatalf("%s: refusal must name %q, named %q", testCase.name, testCase.option, supportError.Option)
		}
		if supportError.Provider == "" {
			t.Fatalf("%s: refusal must name the provider", testCase.name)
		}
	}
}

// A provider_options block naming a provider this build has never heard of is
// refused at create time, not silently carried: a misspelling would otherwise
// validate and then match nothing at any adapter.
func TestUnknownProviderInOptionsIsRefusedAtCreate(t *testing.T) {
	t.Parallel()
	options := &protocol.SttOptions{ProviderOptions: map[string]map[string]any{"whisperfleet": {"punctuate": true}}}
	if err := validateSttOptionProviders(options); err == nil {
		t.Fatal("an unknown provider name must be refused")
	}
	known := &protocol.SttOptions{ProviderOptions: map[string]map[string]any{"deepgram": {"punctuate": true}}}
	if err := validateSttOptionProviders(known); err != nil {
		t.Fatalf("a known provider validates: %v", err)
	}
	if err := validateSttOptionProviders(nil); err != nil {
		t.Fatalf("nil asks for nothing: %v", err)
	}
}

// Options never reach the control plane: the plan request must stay
// byte-identical whether or not the caller asked for anything, so a strict
// upstream parser and the warm-plan cache both see one request shape.
func TestPlanRequestStripsSttOptions(t *testing.T) {
	t.Parallel()
	body := CreateSessionRequest{
		Kind: protocol.SessionKindSTT,
		Request: protocol.RequestOptions{
			Provider: "auto", Language: "en", Model: "auto",
			STT: &protocol.SttOptions{Diarization: boolPointer(true)},
		},
	}
	request := planRequestFor(body, protocol.RuntimeDescriptor{}, nil)
	if request.Request.STT != nil {
		t.Fatal("stt options must not ride the control-plane request")
	}
	if body.Request.STT == nil {
		t.Fatal("stripping must not mutate the caller's body")
	}
}
