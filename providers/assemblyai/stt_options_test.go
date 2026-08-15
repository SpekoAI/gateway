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

	options := protocol.RequestOptions{Language: "en", STT: &protocol.SttOptions{
		Keywords: []string{"Speko", "São Paulo"},
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
	if query.Get("numerals") != "" {
		t.Fatalf("another provider's setting must not leak: %s", endpoint)
	}
	// The gateway-owned pins survive untouched.
	if query.Get("format_turns") != "true" || query.Get("speech_model") != "universal-3-5-pro" {
		t.Fatalf("gateway-owned parameters must stay pinned: %s", endpoint)
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
