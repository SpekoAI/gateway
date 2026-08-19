package deepgram

import (
	"net/url"
	"strings"
	"testing"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

func sttTestPolicy(t *testing.T) upstream.WebSocketPolicy {
	t.Helper()
	policy, err := upstream.NewWebSocketPolicy(officialAPIHost, nil, false)
	if err != nil {
		t.Fatalf("endpoint policy: %v", err)
	}
	return policy
}

func optionsWith(stt *protocol.SttOptions) protocol.RequestOptions {
	return protocol.RequestOptions{Language: "en", STT: stt}
}

func boolPointer(value bool) *bool { return &value }

func queryOf(t *testing.T, endpoint string) url.Values {
	t.Helper()
	parsed, err := url.Parse(endpoint)
	if err != nil {
		t.Fatalf("parse %q: %v", endpoint, err)
	}
	return parsed.Query()
}

// diarize rides the v1 URL when asked; an absent ask leaves the URL exactly as
// it was before options existed.
func TestListenEndpointCarriesDiarization(t *testing.T) {
	t.Parallel()
	policy := sttTestPolicy(t)
	plain, err := listenEndpoint(policy, "wss://api.deepgram.com/v1/listen", "nova-3", optionsWith(nil), *media(), runtimepkg.AudioDeliveryLive, "")
	if err != nil {
		t.Fatalf("plain endpoint: %v", err)
	}
	if strings.Contains(plain, "diarize") || strings.Contains(plain, "keyterm") {
		t.Fatalf("a request that asked for nothing must not mention options: %s", plain)
	}
	diarized, err := listenEndpoint(policy, "wss://api.deepgram.com/v1/listen", "nova-3",
		optionsWith(&protocol.SttOptions{Diarization: boolPointer(true)}), *media(), runtimepkg.AudioDeliveryLive, "")
	if err != nil {
		t.Fatalf("diarized endpoint: %v", err)
	}
	if queryOf(t, diarized).Get("diarize") != "true" {
		t.Fatalf("diarize=true must ride the v1 URL: %s", diarized)
	}
}

// Vocabulary biasing is spelled per model family: keyterm on nova-3 and Flux,
// keywords on everything earlier. Multi-word and non-ASCII terms must survive
// URL encoding — a keyword that arrives mangled biases nothing.
func TestListenEndpointSpellsKeywordsByModelFamily(t *testing.T) {
	t.Parallel()
	policy := sttTestPolicy(t)
	terms := &protocol.SttOptions{Keywords: []string{"Speko", "São Paulo"}}

	nova3, err := listenEndpoint(policy, "wss://api.deepgram.com/v1/listen", "nova-3", optionsWith(terms), *media(), runtimepkg.AudioDeliveryLive, "")
	if err != nil {
		t.Fatalf("nova-3: %v", err)
	}
	if got := queryOf(t, nova3)["keyterm"]; len(got) != 2 || got[1] != "São Paulo" {
		t.Fatalf("nova-3 takes repeatable keyterm: %v", got)
	}
	if len(queryOf(t, nova3)["keywords"]) != 0 {
		t.Fatal("nova-3 must not also receive keywords")
	}

	nova2, err := listenEndpoint(policy, "wss://api.deepgram.com/v1/listen", "nova-2", optionsWith(terms), *media(), runtimepkg.AudioDeliveryLive, "")
	if err != nil {
		t.Fatalf("nova-2: %v", err)
	}
	if got := queryOf(t, nova2)["keywords"]; len(got) != 2 || got[0] != "Speko" {
		t.Fatalf("nova-2 takes repeatable keywords: %v", got)
	}

	flux, err := listenEndpoint(policy, "wss://api.deepgram.com/v2/listen", "flux-general-en", optionsWith(terms), *media(), runtimepkg.AudioDeliveryLive, "")
	if err != nil {
		t.Fatalf("flux: %v", err)
	}
	if got := queryOf(t, flux)["keyterm"]; len(got) != 2 {
		t.Fatalf("flux takes repeatable keyterm: %v", got)
	}
}

// The caller's own allow-listed Deepgram settings reach the query in their
// caller spelling, and integral numbers do not grow a decimal point.
func TestListenEndpointForwardsProviderOptions(t *testing.T) {
	t.Parallel()
	policy := sttTestPolicy(t)
	options := &protocol.SttOptions{ProviderOptions: map[string]map[string]any{
		"deepgram": {"numerals": true, "endpointing": float64(1200)},
		// Another provider's settings must not leak onto Deepgram's URL.
		"elevenlabs": {"vad_silence_threshold_secs": 0.7},
	}}
	endpoint, err := listenEndpoint(policy, "wss://api.deepgram.com/v1/listen", "nova-2", optionsWith(options), *media(), runtimepkg.AudioDeliveryLive, "")
	if err != nil {
		t.Fatalf("endpoint: %v", err)
	}
	query := queryOf(t, endpoint)
	if query.Get("numerals") != "true" {
		t.Fatalf("numerals must be forwarded: %s", endpoint)
	}
	if query.Get("endpointing") != "1200" {
		t.Fatalf("endpointing must keep its integral spelling: %s", endpoint)
	}
	if query.Get("vad_silence_threshold_secs") != "" {
		t.Fatalf("another provider's setting must not leak: %s", endpoint)
	}
}
