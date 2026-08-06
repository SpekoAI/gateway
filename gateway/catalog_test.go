package gateway_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/SpekoAI/gateway/gateway"
	"github.com/SpekoAI/gateway/protocol"
)

type catalogResponse struct {
	Protocol         string `json:"protocol"`
	ProtocolRevision int    `json:"protocol_revision"`
	Runtime          struct {
		Name      string `json:"name"`
		Placement string `json:"placement"`
	} `json:"runtime"`
	Models []struct {
		ID        string `json:"id"`
		Provider  string `json:"provider"`
		Kind      string `json:"kind"`
		Adapter   string `json:"adapter"`
		Transport string `json:"transport"`
		Installed bool   `json:"installed"`
	} `json:"models"`
}

func fetchCatalog(t *testing.T, path string) catalogResponse {
	t.Helper()
	gatewayServer, _ := newServer(t)
	httpServer := httptest.NewServer(gatewayServer.Handler())
	t.Cleanup(httpServer.Close)
	// Deliberately no local auth header: an integrator has to be able to read the
	// catalog before the first session, and it names no customer and carries no
	// credential.
	response, err := http.Get(httpServer.URL + path)
	if err != nil {
		t.Fatalf("get %s: %v", path, err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("get %s = %d, want 200 without auth", path, response.StatusCode)
	}
	var decoded catalogResponse
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode catalog: %v", err)
	}
	return decoded
}

func TestModelsPublishesEveryCatalogEntry(t *testing.T) {
	t.Parallel()
	catalog := fetchCatalog(t, "/v1/models")
	if catalog.Protocol != string(protocol.VoiceV0) || catalog.ProtocolRevision != protocol.CurrentRevision {
		t.Fatalf("catalog protocol = %s r%d, want %s r%d", catalog.Protocol, catalog.ProtocolRevision, protocol.VoiceV0, protocol.CurrentRevision)
	}
	if len(catalog.Models) != len(gateway.Catalog()) {
		t.Fatalf("published %d models, want the %d catalog entries", len(catalog.Models), len(gateway.Catalog()))
	}
	// ElevenLabs STT is the row this endpoint exists to make discoverable: it is the
	// board's strongest transcriber across the most languages and had no adapter at
	// all until now.
	var found bool
	for _, model := range catalog.Models {
		if model.Provider == "elevenlabs" && model.Kind == "stt" {
			found = true
			if model.ID != "elevenlabs:scribe_v2_realtime" {
				t.Fatalf("elevenlabs stt id = %q, want the realtime model, not the batch one", model.ID)
			}
			if model.Adapter != "elevenlabs.stt.v1" || model.Transport != string(protocol.TransportWebSocket) {
				t.Fatalf("elevenlabs stt row = %+v", model)
			}
		}
	}
	if !found {
		t.Fatal("catalog does not publish elevenlabs stt")
	}
	// Stable order, so a page rendering this does not reshuffle between restarts.
	for index := 1; index < len(catalog.Models); index++ {
		previous, current := catalog.Models[index-1], catalog.Models[index]
		if previous.Provider > current.Provider {
			t.Fatalf("catalog is unordered at %d: %q before %q", index, previous.Provider, current.Provider)
		}
	}
}

// A row can be in the catalog and absent from a given process. Hiding that would
// let a caller wire an id this instance cannot open; reporting it keeps the list
// the same shape everywhere while staying honest about the deployment.
func TestModelsReportsWhatThisProcessActuallyInstalled(t *testing.T) {
	t.Parallel()
	catalog := fetchCatalog(t, "/v1/models")
	// The harness installs only a mock adapter, so no real row may claim installed.
	for _, model := range catalog.Models {
		if model.Installed {
			t.Fatalf("%s claims installed, but the test runtime carries only the mock adapter", model.ID)
		}
	}
	if catalog.Runtime.Name == "" || catalog.Runtime.Placement == "" {
		t.Fatalf("catalog runtime = %+v, want the descriptor identified", catalog.Runtime)
	}
}

func TestModelsFiltersByKindAndProvider(t *testing.T) {
	t.Parallel()
	tts := fetchCatalog(t, "/v1/models?kind=tts")
	if len(tts.Models) == 0 {
		t.Fatal("kind=tts returned nothing")
	}
	for _, model := range tts.Models {
		if model.Kind != "tts" {
			t.Fatalf("kind=tts returned %s", model.Kind)
		}
	}
	elevenlabs := fetchCatalog(t, "/v1/models?provider=elevenlabs")
	if len(elevenlabs.Models) != 2 {
		t.Fatalf("provider=elevenlabs returned %d rows, want stt and tts", len(elevenlabs.Models))
	}
	if unknown := fetchCatalog(t, "/v1/models?provider=nope"); len(unknown.Models) != 0 {
		t.Fatalf("an unknown provider returned %d rows, want none", len(unknown.Models))
	}
}

// The property that makes the catalog trustworthy: every published entry is
// routable, and every routable provider is published. They were separate lists
// before, which is how a published id and an openable route drift apart.
func TestEveryPublishedEntryIsRoutable(t *testing.T) {
	t.Parallel()
	entries := gateway.Catalog()
	if len(entries) == 0 {
		t.Fatal("catalog is empty")
	}
	for _, entry := range entries {
		if entry.DefaultModel == "" || entry.Endpoint == "" || entry.Adapter == "" {
			t.Fatalf("catalog entry is incomplete: %+v", entry)
		}
		if entry.Kind != protocol.SessionKindSTT && entry.Kind != protocol.SessionKindTTS {
			t.Fatalf("catalog entry %+v claims a modality the gateway has no adapter shape for", entry)
		}
	}
}

// Several TTS vendors refuse to open without a voice — Rime, Gradium and Google
// among them. In standalone BYOK mode there is no benchmark board to pick from,
// so the catalog carries the fallback. Without it the adapter is registered,
// published, and fails at open with a vendor error the operator cannot act on.
func TestCatalogCarriesADefaultVoiceWhereTheVendorDemandsOne(t *testing.T) {
	t.Parallel()
	voices := map[string]string{}
	for _, entry := range gateway.Catalog() {
		if entry.Kind == protocol.SessionKindTTS {
			voices[entry.Provider] = entry.DefaultVoice
		}
	}
	for _, provider := range []string{"rime", "hume"} {
		if voices[provider] == "" {
			t.Fatalf("%s TTS has no default voice, so a standalone plan cannot open", provider)
		}
	}
	// Google is deliberately blank: its voice names embed the language
	// (hi-IN-Chirp3-HD-Aoede), so no single value is correct for every request.
	// A wrong-language default would be worse than none — it would synthesize
	// Hindi text in an English voice rather than failing.
	if voices["google"] != "" {
		t.Fatalf("google TTS carries default voice %q, but its voices are language-specific", voices["google"])
	}
}

// Greptile caught both of these on review, and both were real: a published route
// that cannot possibly dial is worse than an absent one, because an integrator
// wires the id and gets a vendor error instead of ours.
func TestNoPublishedRouteIsSilentlyUndialable(t *testing.T) {
	t.Parallel()
	for _, entry := range gateway.Catalog() {
		// A placeholder in an endpoint MUST be declared, so the planner can refuse
		// with a reason instead of sending the literal upstream.
		if strings.Contains(entry.Endpoint, "PROJECT_ID") && entry.RequiresDeploymentConfig == "" {
			t.Fatalf("%s/%s publishes a placeholder endpoint with no RequiresDeploymentConfig", entry.Provider, entry.Kind)
		}
		// And the converse: a row that declares config is required must actually
		// carry a placeholder, or the declaration is stale and blocks a good route.
		if entry.RequiresDeploymentConfig != "" && !strings.Contains(entry.Endpoint, "PROJECT_ID") {
			t.Fatalf("%s/%s demands deployment config but its endpoint looks complete", entry.Provider, entry.Kind)
		}
	}
}

// Gradium, Rime and Google all refuse to synthesize without a voice. Gradium was
// missing its default even though the catalog comment already said it needed one
// — the kind of gap that only shows up when someone actually opens a session.
func TestEveryVoiceDemandingTTSHasADefaultOrAStatedReason(t *testing.T) {
	t.Parallel()
	for _, entry := range gateway.Catalog() {
		if entry.Kind != protocol.SessionKindTTS {
			continue
		}
		switch entry.Provider {
		case "gradium", "rime", "hume":
			if entry.DefaultVoice == "" {
				t.Fatalf("%s TTS requires a voice but the catalog carries no default", entry.Provider)
			}
		case "google":
			// Deliberately blank: Google's voice names embed the language, so any
			// single default would speak the wrong one rather than fail honestly.
			if entry.DefaultVoice != "" {
				t.Fatalf("google TTS carries default voice %q despite language-specific naming", entry.DefaultVoice)
			}
		}
	}
}
