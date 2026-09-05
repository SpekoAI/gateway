package relayapi_test

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/SpekoAI/gateway/protocol"
	"github.com/SpekoAI/gateway/relayapi"
)

func boolPointer(value bool) *bool { return &value }

func TestSTTOptionsValidation(t *testing.T) {
	t.Parallel()

	var absent *relayapi.STTOptions
	if err := absent.Validate(); err != nil {
		t.Fatalf("absent options must validate: %v", err)
	}
	if !absent.IsZero() {
		t.Fatal("absent options must report zero")
	}
	// A word_timestamps ask alone is an ask: it must survive IsZero, or the
	// edge would forward nothing and the caller would get untimed text.
	wordsOnly := relayapi.STTOptions{WordTimestamps: boolPointer(true)}
	if wordsOnly.IsZero() || !wordsOnly.WantsWordTimestamps() {
		t.Fatal("a word_timestamps ask must not report zero")
	}
	if (&relayapi.STTOptions{WordTimestamps: boolPointer(false)}).WantsWordTimestamps() {
		t.Fatal("an explicit false is not an ask")
	}

	full := relayapi.STTOptions{
		Diarization:     boolPointer(true),
		Keywords:        []string{"Speko", "Tashkent"},
		NoiseReduction:  boolPointer(false),
		ProviderOptions: map[string]map[string]any{"deepgram": {"numerals": true, "utterance_end_ms": 1000.0, "extra": "value"}},
	}
	if err := full.Validate(); err != nil {
		t.Fatalf("fully specified options must validate: %v", err)
	}
	if full.IsZero() {
		t.Fatal("fully specified options must not report zero")
	}

	cases := []struct {
		name    string
		options relayapi.STTOptions
		want    string
	}{
		{"blank keyword", relayapi.STTOptions{Keywords: []string{"ok", "  "}}, "keywords[1]"},
		{"keyword with a control character", relayapi.STTOptions{Keywords: []string{"one\nline"}}, "keywords[0]"},
		{"keyword too long", relayapi.STTOptions{Keywords: []string{strings.Repeat("a", relayapi.MaxSTTKeywordLength+1)}}, "keywords[0]"},
		{"too many keywords", relayapi.STTOptions{Keywords: make([]string, relayapi.MaxSTTKeywords+1)}, "at most 100"},
		{"blank provider", relayapi.STTOptions{ProviderOptions: map[string]map[string]any{" ": {"a": true}}}, "empty provider"},
		{"upper-case provider", relayapi.STTOptions{ProviderOptions: map[string]map[string]any{"Deepgram": {"numerals": true}}}, "must be lower case"},
		{"upper-case setting", relayapi.STTOptions{ProviderOptions: map[string]map[string]any{"deepgram": {"Numerals": true}}}, "must be lower case"},
		{"blank setting", relayapi.STTOptions{ProviderOptions: map[string]map[string]any{"deepgram": {" ": true}}}, "empty setting"},
		{"reserved setting", relayapi.STTOptions{ProviderOptions: map[string]map[string]any{"deepgram": {"model": "nova-3"}}}, "owned by the relay"},
		{"reserved canonical setting", relayapi.STTOptions{ProviderOptions: map[string]map[string]any{"deepgram": {"diarize": true}}}, "owned by the relay"},
		{"non-scalar setting", relayapi.STTOptions{ProviderOptions: map[string]map[string]any{"deepgram": {"nested": map[string]any{"a": 1}}}}, "boolean, a number, or a string"},
		{"setting string too long", relayapi.STTOptions{ProviderOptions: map[string]map[string]any{"deepgram": {"tag": strings.Repeat("x", relayapi.MaxSTTOptionStringValue+1)}}}, "at most 256 characters"},
		{"setting string with a control character", relayapi.STTOptions{ProviderOptions: map[string]map[string]any{"deepgram": {"tag": "a\rb"}}}, "control characters"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertInvalid(t, tc.options.Validate(), tc.want)
		})
	}
}

func TestSTTOptionsTooManyProviders(t *testing.T) {
	t.Parallel()

	options := relayapi.STTOptions{ProviderOptions: map[string]map[string]any{}}
	for _, name := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"} {
		options.ProviderOptions[name] = map[string]any{"setting": true}
	}
	assertInvalid(t, options.Validate(), "at most 8 providers")
}

func TestSTTOptionsTooManySettings(t *testing.T) {
	t.Parallel()

	settings := map[string]any{}
	for i := 0; i <= relayapi.MaxSTTOptionKeys; i++ {
		settings[string(rune('a'+i))] = true
	}
	options := relayapi.STTOptions{ProviderOptions: map[string]map[string]any{"deepgram": settings}}
	assertInvalid(t, options.Validate(), "at most 16 settings")
}

func TestSTTOptionsDecodedNumbersValidate(t *testing.T) {
	t.Parallel()

	var options relayapi.STTOptions
	if err := json.Unmarshal([]byte(`{"provider_options":{"deepgram":{"utterance_end_ms":1000}}}`), &options); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if err := options.Validate(); err != nil {
		t.Fatalf("decoded number must validate: %v", err)
	}
}

func TestSTTOptionsRejectNonFiniteNumbers(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		value float64
	}{
		{"NaN", math.NaN()},
		{"positive infinity", math.Inf(1)},
		{"negative infinity", math.Inf(-1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			options := relayapi.STTOptions{ProviderOptions: map[string]map[string]any{"deepgram": {"threshold": tc.value}}}
			assertInvalid(t, options.Validate(), "must be a finite number")
			if _, err := json.Marshal(options); err == nil {
				t.Fatal("json.Marshal accepted a non-finite number; the refusal above is guarding nothing")
			}
		})
	}

	finite := relayapi.STTOptions{ProviderOptions: map[string]map[string]any{"deepgram": {
		"utterance_end_ms": 1000.0,
		"count":            7,
		"wide":             int64(1 << 40),
		"negative":         -0.5,
	}}}
	if err := finite.Validate(); err != nil {
		t.Fatalf("finite numbers must validate: %v", err)
	}
}

func TestSTTOptionsAbsentStayOffTheWire(t *testing.T) {
	t.Parallel()

	encoded, err := json.Marshal(relayapi.TranscriptionRequest{
		Routing:  relayapi.Routing{}.NormalizeDefault(),
		Language: "en",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(encoded), "options") {
		t.Fatalf("absent options must not appear on the wire: %s", encoded)
	}
}

func TestSTTOptionsExplicitFalseSurvives(t *testing.T) {
	t.Parallel()

	encoded, err := json.Marshal(relayapi.STTOptions{Diarization: boolPointer(false)})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(encoded) != `{"diarization":false}` {
		t.Fatalf("explicit false must survive, got %s", encoded)
	}

	var decoded relayapi.STTOptions
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Diarization == nil || *decoded.Diarization {
		t.Fatal("explicit false must decode to a non-nil false")
	}
	if decoded.Diarize() {
		t.Fatal("an explicit false is not an ask")
	}
}

func TestSTTOptionsRideBothSTTSurfaces(t *testing.T) {
	t.Parallel()

	options := &relayapi.STTOptions{ProviderOptions: map[string]map[string]any{"deepgram": {"model": "nova-3"}}}

	request := relayapi.TranscriptionRequest{Routing: relayapi.Routing{}.NormalizeDefault(), Options: options}
	assertInvalid(t, request.Validate(), "options: provider_options.deepgram.model")

	configure := relayapi.STTSessionConfigure{
		Type:    relayapi.STTControlSessionConfigure,
		Routing: relayapi.Routing{}.NormalizeDefault(),
		Audio:   relayapi.AudioConfig{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
		Options: options,
	}
	assertInvalid(t, configure.Validate(), "options: provider_options.deepgram.model")
}

func TestModelCapabilitiesGateSTTOptions(t *testing.T) {
	t.Parallel()

	capable := relayapi.ModelCapabilities{Diarization: true, Keywords: true, NoiseReduction: true, WordTimestamps: true}
	if ask, ok := capable.SupportsSTTOptions(&relayapi.STTOptions{
		Diarization:    boolPointer(true),
		Keywords:       []string{"Speko"},
		NoiseReduction: boolPointer(true),
		WordTimestamps: boolPointer(true),
	}); !ok {
		t.Fatalf("a fully capable model must serve every ask, refused %q", ask)
	}

	var none relayapi.ModelCapabilities
	if ask, ok := none.SupportsSTTOptions(nil); !ok {
		t.Fatalf("an empty ask must be servable, refused %q", ask)
	}

	if ask, ok := none.SupportsSTTOptions(&relayapi.STTOptions{Diarization: boolPointer(false)}); !ok {
		t.Fatalf("an explicit false must not require the capability, refused %q", ask)
	}

	if ask, ok := none.SupportsSTTOptions(&relayapi.STTOptions{
		ProviderOptions: map[string]map[string]any{"deepgram": {"numerals": true}},
	}); !ok {
		t.Fatalf("provider settings must not be capability-gated, refused %q", ask)
	}

	cases := []struct {
		name         string
		capabilities relayapi.ModelCapabilities
		options      relayapi.STTOptions
		want         string
	}{
		{"diarization", relayapi.ModelCapabilities{Keywords: true, NoiseReduction: true}, relayapi.STTOptions{Diarization: boolPointer(true)}, "diarization"},
		{"keywords", relayapi.ModelCapabilities{Diarization: true, NoiseReduction: true}, relayapi.STTOptions{Keywords: []string{"Speko"}}, "keywords"},
		{"noise reduction", relayapi.ModelCapabilities{Diarization: true, Keywords: true}, relayapi.STTOptions{NoiseReduction: boolPointer(true)}, "noise_reduction"},
		{"word timestamps", relayapi.ModelCapabilities{Diarization: true, Keywords: true, NoiseReduction: true}, relayapi.STTOptions{WordTimestamps: boolPointer(true)}, "word_timestamps"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ask, ok := tc.capabilities.SupportsSTTOptions(&tc.options)
			if ok {
				t.Fatalf("a model without %s must refuse the ask", tc.want)
			}
			if ask != tc.want {
				t.Fatalf("refusal must name the ask: got %q, want %q", ask, tc.want)
			}
		})
	}
}

// TestSTTOptionsMatchGatewayProtocol holds the relayapi mirror and
// protocol.SttOptions in step. The protocol import is test-only.
func TestSTTOptionsMatchGatewayProtocol(t *testing.T) {
	t.Parallel()

	t.Run("field set", func(t *testing.T) {
		t.Parallel()
		relayFields := jsonFieldNames(t, reflect.TypeOf(relayapi.STTOptions{}))
		protocolFields := jsonFieldNames(t, reflect.TypeOf(protocol.SttOptions{}))
		for name := range relayFields {
			if !protocolFields[name] {
				t.Fatalf("relayapi carries option %q that the gateway protocol does not", name)
			}
		}
		for name := range protocolFields {
			if !relayFields[name] {
				t.Fatalf("gateway protocol carries option %q that the relay contract does not", name)
			}
		}
	})

	t.Run("bounds", func(t *testing.T) {
		t.Parallel()
		// The protocol bounds are unexported, so they are compared through
		// behavior.
		for _, tc := range []struct {
			name    string
			atLimit relayapi.STTOptions
			over    relayapi.STTOptions
		}{
			{
				"keyword length",
				relayapi.STTOptions{Keywords: []string{strings.Repeat("a", relayapi.MaxSTTKeywordLength)}},
				relayapi.STTOptions{Keywords: []string{strings.Repeat("a", relayapi.MaxSTTKeywordLength+1)}},
			},
			{
				"keyword count",
				relayapi.STTOptions{Keywords: repeatKeyword(relayapi.MaxSTTKeywords)},
				relayapi.STTOptions{Keywords: repeatKeyword(relayapi.MaxSTTKeywords + 1)},
			},
			{
				"setting string length",
				relayapi.STTOptions{ProviderOptions: map[string]map[string]any{"deepgram": {"tag": strings.Repeat("x", relayapi.MaxSTTOptionStringValue)}}},
				relayapi.STTOptions{ProviderOptions: map[string]map[string]any{"deepgram": {"tag": strings.Repeat("x", relayapi.MaxSTTOptionStringValue+1)}}},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				if err := tc.atLimit.Validate(); err != nil {
					t.Fatalf("relayapi rejects a value at its own limit: %v", err)
				}
				if err := asProtocolOptions(tc.atLimit).Normalize(); err != nil {
					t.Fatalf("protocol rejects a value relayapi accepts: %v", err)
				}
				if err := tc.over.Validate(); err == nil {
					t.Fatal("relayapi accepts a value over its limit")
				}
				if err := asProtocolOptions(tc.over).Normalize(); err == nil {
					t.Fatal("protocol accepts a value relayapi rejects")
				}
			})
		}
	})

	t.Run("reserved keys", func(t *testing.T) {
		t.Parallel()
		for _, key := range []string{
			"model", "model_id", "speech_model",
			"language", "language_code", "language_codes", "language_hints",
			"detect_language", "language_detection", "enable_language_identification",
			"encoding", "audio_format", "sample_rate", "bit_depth", "channels",
			"api_key", "token", "authorization",
			"diarize", "diarization", "enable_speaker_diarization",
			"keywords", "keyterm", "keyterms", "keyterms_prompt", "custom_vocabulary",
			"format_turns", "interim_results", "include_partial_turns",
			"include_timestamps", "commit_strategy", "intent",
		} {
			options := relayapi.STTOptions{ProviderOptions: map[string]map[string]any{"deepgram": {key: "value"}}}
			if err := options.Validate(); err == nil {
				t.Fatalf("relayapi forwards %q, which the gateway reserves", key)
			}
			if err := asProtocolOptions(options).Normalize(); err == nil {
				t.Fatalf("gateway forwards %q, which relayapi reserves", key)
			}
		}

		passthrough := relayapi.STTOptions{ProviderOptions: map[string]map[string]any{"deepgram": {"numerals": true}}}
		if err := passthrough.Validate(); err != nil {
			t.Fatalf("relayapi refuses an ordinary vendor setting: %v", err)
		}
		if err := asProtocolOptions(passthrough).Normalize(); err != nil {
			t.Fatalf("gateway refuses an ordinary vendor setting: %v", err)
		}
	})
}

func repeatKeyword(count int) []string {
	keywords := make([]string, count)
	for i := range keywords {
		keywords[i] = "term"
	}
	return keywords
}

// asProtocolOptions mirrors the conversion the relay performs.
func asProtocolOptions(options relayapi.STTOptions) *protocol.SttOptions {
	converted := &protocol.SttOptions{
		Diarization:    options.Diarization,
		Keywords:       options.Keywords,
		NoiseReduction: options.NoiseReduction,
	}
	if len(options.ProviderOptions) > 0 {
		converted.ProviderOptions = make(map[string]map[string]any, len(options.ProviderOptions))
		for provider, settings := range options.ProviderOptions {
			copied := make(map[string]any, len(settings))
			for key, value := range settings {
				copied[key] = value
			}
			converted.ProviderOptions[provider] = copied
		}
	}
	return converted
}
