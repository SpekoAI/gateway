package elevenlabs

import (
	"net/url"
	"slices"
	"testing"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
)

// Scribe realtime takes repeatable `keyterms`, and the caller's allow-listed
// vad_silence_threshold_secs replaces the vendor default on the same URL.
func TestRealtimeEndpointCarriesKeytermsAndVadThreshold(t *testing.T) {
	t.Parallel()
	policy, err := upstream.NewWebSocketPolicy(officialAPIHost, nil, false)
	if err != nil {
		t.Fatalf("endpoint policy: %v", err)
	}
	media := protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1}
	raw := "wss://api.elevenlabs.io/v1/speech-to-text/realtime"

	plain, err := realtimeEndpoint(policy, raw, "scribe_v2_realtime", protocol.RequestOptions{Language: "en"}, media)
	if err != nil {
		t.Fatalf("plain endpoint: %v", err)
	}
	if sttQuery(t, plain).Get("keyterms") != "" {
		t.Fatalf("a request that asked for nothing must not mention keyterms: %s", plain)
	}

	options := protocol.RequestOptions{Language: "en", STT: &protocol.SttOptions{
		Keywords: []string{"Speko", "São Paulo"},
		ProviderOptions: map[string]map[string]any{
			"elevenlabs": {"vad_silence_threshold_secs": 0.7},
			"deepgram":   {"numerals": true},
		},
	}}
	endpoint, err := realtimeEndpoint(policy, raw, "scribe_v2_realtime", options, media)
	if err != nil {
		t.Fatalf("endpoint: %v", err)
	}
	query := sttQuery(t, endpoint)
	if got := query["keyterms"]; len(got) != 2 || got[1] != "São Paulo" {
		t.Fatalf("repeatable keyterms must survive encoding: %v", got)
	}
	if query.Get("vad_silence_threshold_secs") != "0.7" {
		t.Fatalf("the caller's threshold must be forwarded: %s", endpoint)
	}
	if query.Get("numerals") != "" {
		t.Fatalf("another provider's setting must not leak: %s", endpoint)
	}
	// Gateway-owned pins stay: word timestamps and the VAD commit strategy.
	if query.Get("include_timestamps") != "true" || query.Get("commit_strategy") != "vad" {
		t.Fatalf("gateway-owned parameters must stay pinned: %s", endpoint)
	}
}

func sttQuery(t *testing.T, endpoint string) url.Values {
	t.Helper()
	parsed, err := url.Parse(endpoint)
	if err != nil {
		t.Fatalf("parse %q: %v", endpoint, err)
	}
	return parsed.Query()
}

// Scribe lists Norwegian only as `nor`; a platform `nb` is refused with
// invalid_request and close 1008. Other languages pass as the primary subtag,
// and the TTS-only `no` alias must not leak onto STT.
func TestRealtimeEndpointAliasesNorwegianToScribeCode(t *testing.T) {
	t.Parallel()
	policy, err := upstream.NewWebSocketPolicy(officialAPIHost, nil, false)
	if err != nil {
		t.Fatalf("endpoint policy: %v", err)
	}
	media := protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1}
	raw := "wss://api.elevenlabs.io/v1/speech-to-text/realtime"
	for language, want := range map[string]string{"nb": "nor", "nb-NO": "nor", "nn": "nor", "NN_no": "nor", "es-MX": "es", "fil": "fil", "uz": "uz"} {
		endpoint, err := realtimeEndpoint(policy, raw, "scribe_v2_realtime", protocol.RequestOptions{Language: language}, media)
		if err != nil {
			t.Fatalf("%s endpoint: %v", language, err)
		}
		if got := sttQuery(t, endpoint).Get("language_code"); got != want {
			t.Errorf("language %q sent language_code=%q, want %q", language, got, want)
		}
	}
}

// scribe_v3_live shares the v2 Realtime socket and parameters. The model id
// reaches the socket verbatim, and nothing else in the URL changes: the
// gateway still pins the VAD commit strategy (v3 Live defaults to manual) and
// word timestamps, and the caller's settings ride the same way.
func TestRealtimeEndpointSendsScribeV3LiveVerbatimWithTheV2Contract(t *testing.T) {
	t.Parallel()
	policy, err := upstream.NewWebSocketPolicy(officialAPIHost, nil, false)
	if err != nil {
		t.Fatalf("endpoint policy: %v", err)
	}
	media := protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1}
	raw := "wss://api.elevenlabs.io/v1/speech-to-text/realtime"
	options := protocol.RequestOptions{Language: "es-MX", STT: &protocol.SttOptions{
		Keywords:        []string{"Speko"},
		ProviderOptions: map[string]map[string]any{"elevenlabs": {"vad_silence_threshold_secs": 0.7}},
	}}
	queries := make(map[string]url.Values, 2)
	for _, model := range []string{"scribe_v2_realtime", "scribe_v3_live"} {
		endpoint, err := realtimeEndpoint(policy, raw, model, options, media)
		if err != nil {
			t.Fatalf("%s endpoint: %v", model, err)
		}
		query := sttQuery(t, endpoint)
		if got := query.Get("model_id"); got != model {
			t.Fatalf("%s model_id = %q, want it verbatim", model, got)
		}
		queries[model] = query
	}
	v2, v3 := queries["scribe_v2_realtime"], queries["scribe_v3_live"]
	if len(v2) != len(v3) {
		t.Fatalf("v2 query %v and v3 query %v carry different parameters", v2, v3)
	}
	for key, values := range v2 {
		if key == "model_id" {
			continue
		}
		if got := v3[key]; !slices.Equal(got, values) {
			t.Fatalf("v3 %s = %v, want the v2 value %v", key, got, values)
		}
	}
	if v3.Get("commit_strategy") != "vad" || v3.Get("include_timestamps") != "true" || v3.Get("language_code") != "es" || v3.Get("audio_format") != "pcm_16000" {
		t.Fatalf("v3 query = %v, want the gateway-owned pins", v3)
	}
}
