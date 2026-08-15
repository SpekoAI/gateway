package elevenlabs

import (
	"net/url"
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
