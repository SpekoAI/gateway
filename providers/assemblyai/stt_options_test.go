package assemblyai

import (
	"context"
	"encoding/json"
	"net/url"
	"testing"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
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

// 3.7 with no caller mode is served the way the benchmark measured it:
// min_latency. A caller's own mode or turn timing wins, and other models keep
// the vendor default.
func TestStreamEndpointDefaultsRealtimeAgentToMinLatency(t *testing.T) {
	t.Parallel()
	policy, err := upstream.NewWebSocketPolicy(officialAPIHost, nil, false)
	if err != nil {
		t.Fatalf("endpoint policy: %v", err)
	}
	media := protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1}
	withSettings := func(settings map[string]any) protocol.RequestOptions {
		return protocol.RequestOptions{STT: &protocol.SttOptions{ProviderOptions: map[string]map[string]any{"assemblyai": settings}}}
	}
	cases := []struct {
		name    string
		model   string
		options protocol.RequestOptions
		mode    string
	}{
		{"3.7 without options", RealtimeAgent37Model, protocol.RequestOptions{Language: "en"}, "min_latency"},
		{"3.7 with unrelated options", RealtimeAgent37Model, withSettings(map[string]any{"voice_focus": "far-field"}), "min_latency"},
		{"3.7 with the caller's mode", RealtimeAgent37Model, withSettings(map[string]any{"mode": "max_accuracy"}), "max_accuracy"},
		{"3.7 with the caller's turn timing", RealtimeAgent37Model, withSettings(map[string]any{"max_turn_silence": 1200}), ""},
		{"3.7 with the caller's silence and unrelated options", RealtimeAgent37Model, withSettings(map[string]any{"min_turn_silence": 200, "voice_focus": "near-field"}), ""},
		{"3.6 Pro without options", universal36Model, protocol.RequestOptions{}, ""},
	}
	for _, tc := range cases {
		endpoint, err := streamEndpoint(policy, "wss://streaming.assemblyai.com/v3/ws", tc.model, tc.options, media)
		if err != nil {
			t.Fatalf("%s: stream endpoint: %v", tc.name, err)
		}
		query := mustQuery(t, endpoint)
		if got := query.Get("mode"); got != tc.mode || len(query["mode"]) > 1 {
			t.Fatalf("%s: mode = %v, want %q", tc.name, query["mode"], tc.mode)
		}
	}
}

// end_of_turn_confidence_threshold does not exist on Universal-3.6 Pro, and
// the 3.7 models follow the 3.6 Pro turn parameters. The adapter leaves it off
// the URL there and the session still opens. A caller that sets it keeps
// choosing its own turn timing, so 3.7 gets no preset, as before. Universal-3.5
// Pro and Universal-Streaming still get it.
func TestStreamEndpointScopesEndOfTurnConfidenceByModel(t *testing.T) {
	t.Parallel()
	policy, err := upstream.NewWebSocketPolicy(officialAPIHost, nil, false)
	if err != nil {
		t.Fatalf("endpoint policy: %v", err)
	}
	media := protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1}
	options := protocol.RequestOptions{STT: &protocol.SttOptions{ProviderOptions: map[string]map[string]any{
		"assemblyai": {EndOfTurnConfidenceOption: 0.6},
	}}}
	for _, model := range []string{RealtimeAgent37Model, universal36Model, "assemblyai-stt-3-7-realtime"} {
		endpoint, err := streamEndpoint(policy, "wss://streaming.assemblyai.com/v3/ws", model, options, media)
		if err != nil {
			t.Fatalf("%s: the threshold must not refuse the session: %v", model, err)
		}
		if query := mustQuery(t, endpoint); query.Has(EndOfTurnConfidenceOption) {
			t.Fatalf("%s: the threshold must be left off the URL: %s", model, endpoint)
		}
	}
	agent, err := streamEndpoint(policy, "wss://streaming.assemblyai.com/v3/ws", RealtimeAgent37Model, options, media)
	if err != nil {
		t.Fatalf("3.7 agent: %v", err)
	}
	if mustQuery(t, agent).Get("mode") != "" {
		t.Fatalf("a caller that sets the threshold keeps choosing its own turn timing (no preset): %s", agent)
	}
	for _, model := range []string{"universal-3-5-pro", "universal-streaming-english", "universal-streaming-multilingual"} {
		endpoint, err := streamEndpoint(policy, "wss://streaming.assemblyai.com/v3/ws", model, options, media)
		if err != nil {
			t.Fatalf("%s: stream endpoint: %v", model, err)
		}
		query := mustQuery(t, endpoint)
		if query.Get(EndOfTurnConfidenceOption) != "0.6" || query.Get("mode") != "" {
			t.Fatalf("%s: want the threshold forwarded and no preset: %s", model, endpoint)
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

// A threshold the model does not read is reported once, as a warning event,
// when AssemblyAI confirms the session. Models that read it report nothing.
func TestIgnoredEndOfTurnConfidenceIsReportedOnBegin(t *testing.T) {
	t.Parallel()
	options := protocol.RequestOptions{STT: &protocol.SttOptions{ProviderOptions: map[string]map[string]any{
		"assemblyai": {EndOfTurnConfidenceOption: 0.6},
	}}}
	for _, model := range []string{"universal-3-5-pro", "universal-streaming-english", "universal-streaming-multilingual"} {
		if got := ignoredOptions(model, options); len(got) != 0 {
			t.Fatalf("%s reads the threshold; ignoredOptions = %v", model, got)
		}
	}
	if got := ignoredOptions(RealtimeAgent37Model, protocol.RequestOptions{}); len(got) != 0 {
		t.Fatalf("no options must report nothing, got %v", got)
	}
	for _, model := range []string{RealtimeAgent37Model, universal36Model, "assemblyai-stt-3-7-realtime"} {
		ignored := ignoredOptions(model, options)
		if len(ignored) != 1 || ignored[0] != EndOfTurnConfidenceOption {
			t.Fatalf("%s: ignoredOptions = %v", model, ignored)
		}
		s := &stream{ctx: context.Background(), events: make(chan runtimepkg.ProviderEvent, 4), billingModel: model, ignoredOptions: ignored}
		if _, err := s.handleMessage([]byte(`{"type":"Begin","id":"session-1","expires_at":1780000000}`)); err != nil {
			t.Fatalf("%s: begin: %v", model, err)
		}
		if usage := <-s.events; usage.Type != protocol.EventUsageObserved {
			t.Fatalf("%s: first event = %q, want usage", model, usage.Type)
		}
		warning := <-s.events
		var data map[string]any
		if err := json.Unmarshal(warning.Data, &data); err != nil || warning.Type != protocol.EventWarning ||
			data["ignored_option"] != "provider_options.assemblyai."+EndOfTurnConfidenceOption || data["model"] != model {
			t.Fatalf("%s: warning = %q %s (err %v)", model, warning.Type, warning.Data, err)
		}
		if len(s.events) != 0 {
			t.Fatalf("%s: want exactly one warning", model)
		}
	}
}
