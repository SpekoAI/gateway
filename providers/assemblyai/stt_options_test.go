package assemblyai

import (
	"encoding/json"
	"net/url"
	"testing"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
)

// keyterms_prompt is a JSON-encoded string array on the v3 connect URL — the
// same spelling the platform's AssemblyAI adapter uses on this socket. A
// caller's allow-listed turn-detection settings ride the same query.
func TestStreamEndpointCarriesKeytermsAndProviderOptions(t *testing.T) {
	t.Parallel()
	policy, err := upstream.NewWebSocketPolicy(officialAPIHost, nil, false)
	if err != nil {
		t.Fatalf("endpoint policy: %v", err)
	}
	media := protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1}

	plain, err := streamEndpoint(policy, "wss://streaming.assemblyai.com/v3/ws", "universal-3-5-pro", protocol.RequestOptions{Language: "en"}, media)
	if err != nil {
		t.Fatalf("plain endpoint: %v", err)
	}
	plainQuery := mustQuery(t, plain)
	if plainQuery.Get("keyterms_prompt") != "" {
		t.Fatalf("a request that asked for nothing must not mention keyterms: %s", plain)
	}

	diarize := true
	focus := true
	options := protocol.RequestOptions{Language: "en", STT: &protocol.SttOptions{
		Diarization:    &diarize,
		NoiseReduction: &focus,
		Keywords:       []string{"Speko", "São Paulo"},
		ProviderOptions: map[string]map[string]any{
			"assemblyai": {"end_of_turn_confidence_threshold": 0.55},
			// Another provider's settings must not leak onto this URL.
			"deepgram": {"numerals": true},
		},
	}}
	endpoint, err := streamEndpoint(policy, "wss://streaming.assemblyai.com/v3/ws", "universal-3-5-pro", options, media)
	if err != nil {
		t.Fatalf("endpoint: %v", err)
	}
	query := mustQuery(t, endpoint)
	var keyterms []string
	if err := json.Unmarshal([]byte(query.Get("keyterms_prompt")), &keyterms); err != nil {
		t.Fatalf("keyterms_prompt is a JSON array: %v (%s)", err, query.Get("keyterms_prompt"))
	}
	if len(keyterms) != 2 || keyterms[1] != "São Paulo" {
		t.Fatalf("keyterms must survive encoding intact: %v", keyterms)
	}
	if query.Get("end_of_turn_confidence_threshold") != "0.55" {
		t.Fatalf("allow-listed setting must be forwarded: %s", endpoint)
	}
	// Streaming diarization and Voice Focus, in the v3 parameter table's own
	// spellings. near-field is the conversational default; a provider option
	// naming far-field would overwrite it.
	if query.Get("speaker_labels") != "true" {
		t.Fatalf("diarization must become speaker_labels=true: %s", endpoint)
	}
	if query.Get("voice_focus") != "near-field" {
		t.Fatalf("noise_reduction must become voice_focus=near-field: %s", endpoint)
	}
	if query.Get("numerals") != "" {
		t.Fatalf("another provider's setting must not leak: %s", endpoint)
	}
	// The gateway-owned pins survive untouched.
	if query.Get("format_turns") != "true" || query.Get("speech_model") != "universal-3-5-pro" {
		t.Fatalf("gateway-owned parameters must stay pinned: %s", endpoint)
	}
}

// Universal-3.6 Pro serves languages 3.5 Pro does not, over the same socket
// and the same parameter. The vendor accepts either code on either model, so
// the refusal is ours to make per model.
func TestStreamingLanguagesAreScopedPerModel(t *testing.T) {
	t.Parallel()
	policy, err := upstream.NewWebSocketPolicy(officialAPIHost, nil, false)
	if err != nil {
		t.Fatalf("endpoint policy: %v", err)
	}
	media := protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1}
	const socket = "wss://streaming.assemblyai.com/v3/ws"

	for _, language := range []string{"ru", "ko-KR", "yue", "fa", "nn", "zu", "ca"} {
		endpoint, err := streamEndpoint(policy, socket, "universal-3-6-pro", protocol.RequestOptions{Language: language}, media)
		if err != nil {
			t.Fatalf("universal-3-6-pro must accept %q: %v", language, err)
		}
		// AssemblyAI lists the same 32 languages for the 3.7 agent model.
		if _, err := streamEndpoint(policy, socket, RealtimeAgent37Model, protocol.RequestOptions{Language: language}, media); err != nil {
			t.Fatalf("%s must accept %q: %v", RealtimeAgent37Model, language, err)
		}
		var codes []string
		if err := json.Unmarshal([]byte(mustQuery(t, endpoint).Get("language_codes")), &codes); err != nil || len(codes) != 1 {
			t.Fatalf("language_codes must be a one-element JSON array: %v %s", err, endpoint)
		}
		if _, err := streamEndpoint(policy, socket, "universal-3-5-pro", protocol.RequestOptions{Language: language}, media); err == nil {
			t.Fatalf("universal-3-5-pro must refuse %q: 3.6 Pro alone serves it", language)
		}
	}
	// The shared vocabulary still serves every model, 3.6 Pro included.
	for _, model := range []string{"universal-3-5-pro", "universal-3-6-pro", RealtimeAgent37Model} {
		if _, err := streamEndpoint(policy, socket, model, protocol.RequestOptions{Language: "es-419"}, media); err != nil {
			t.Fatalf("%s must accept es-419 as es: %v", model, err)
		}
	}
	if _, err := streamEndpoint(policy, socket, "universal-3-6-pro", protocol.RequestOptions{Language: "uz"}, media); err == nil {
		t.Fatal("universal-3-6-pro must refuse a language outside its 32")
	}
	if _, err := streamEndpoint(policy, socket, RealtimeAgent37Model, protocol.RequestOptions{Language: "uz"}, media); err == nil {
		t.Fatalf("%s must refuse a language outside its 32", RealtimeAgent37Model)
	}
}

// The 3.7 agent model rides the same socket: its id is the speech_model, and a
// caller's mode preset reaches the connect URL beside the gateway pins.
func TestStreamEndpointCarriesRealtimeAgentModelAndMode(t *testing.T) {
	t.Parallel()
	policy, err := upstream.NewWebSocketPolicy(officialAPIHost, nil, false)
	if err != nil {
		t.Fatalf("endpoint policy: %v", err)
	}
	media := protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1}
	options := protocol.RequestOptions{STT: &protocol.SttOptions{
		ProviderOptions: map[string]map[string]any{"assemblyai": {"mode": "min_latency"}},
	}}
	endpoint, err := streamEndpoint(policy, "wss://streaming.assemblyai.com/v3/ws", RealtimeAgent37Model, options, media)
	if err != nil {
		t.Fatalf("stream endpoint: %v", err)
	}
	query := mustQuery(t, endpoint)
	if query.Get("speech_model") != RealtimeAgent37Model || query.Get("mode") != "min_latency" || query.Get("format_turns") != "true" {
		t.Fatalf("connect URL = %s", endpoint)
	}
	// The adapter holds the same rule as the gateway server, so an in-process
	// caller (the relay) cannot dial a preset the model refuses mid-call.
	for model, value := range map[string]any{RealtimeAgent37Model: "manual", "universal-3-6-pro": "fastest"} {
		bad := protocol.RequestOptions{STT: &protocol.SttOptions{ProviderOptions: map[string]map[string]any{"assemblyai": {"mode": value}}}}
		if _, err := streamEndpoint(policy, "wss://streaming.assemblyai.com/v3/ws", model, bad, media); err == nil {
			t.Fatalf("%s mode=%v must be refused before the dial", model, value)
		}
	}
}

func mustQuery(t *testing.T, endpoint string) url.Values {
	t.Helper()
	parsed, err := url.Parse(endpoint)
	if err != nil {
		t.Fatalf("parse %q: %v", endpoint, err)
	}
	return parsed.Query()
}
