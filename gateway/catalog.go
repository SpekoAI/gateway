package gateway

import (
	"net/http"
	"sort"
	"strings"

	"github.com/SpekoAI/gateway/protocol"
)

// CatalogEntry is one thing this build can actually do: a provider, a modality,
// the adapter that implements it, and the default model and endpoint used when a
// caller names neither.
//
// This table is the single source for BOTH standalone route construction and the
// published catalog. Keeping them separate is what lets a catalog claim a provider
// the binary cannot open — the failure mode is a customer reading the list, wiring
// the id, and getting a route error. Adding a provider here makes it routable and
// published in one edit, or neither.
type CatalogEntry struct {
	Provider     string               `json:"provider"`
	Kind         protocol.SessionKind `json:"kind"`
	Adapter      string               `json:"adapter"`
	DefaultModel string               `json:"default_model"`
	// DefaultVoice is used only in standalone BYOK mode. Several TTS vendors
	// REFUSE to open without a voice — Rime, Gradium and Google among them — and
	// the local planner has no benchmark board to choose from, so without this a
	// perfectly good adapter fails at open with a vendor error the operator cannot
	// act on. Empty means the vendor supplies its own default.
	//
	// Google is deliberately empty: its voice names embed the language
	// (hi-IN-Chirp3-HD-Aoede), so no single value is correct and the caller must
	// pass one. The control plane fills this from the measured board instead.
	DefaultVoice string             `json:"default_voice,omitempty"`
	Transport    protocol.Transport `json:"transport"`
	Endpoint     string             `json:"endpoint"`
	// ModelRoutes selects a different vendor endpoint for model families that
	// share one provider/kind adapter but are served on a versioned path. It is
	// local-planner metadata, not another published provider row. The first
	// matching prefix wins; unmatched models retain Endpoint.
	ModelRoutes []CatalogModelRoute `json:"-"`
	// RequiresDeploymentConfig, when non-empty, says this row cannot be dialled as
	// written and names what an operator must supply. Google STT is the case: its
	// path embeds the caller's own GCP project
	// (/v2/projects/{project}/locations/{location}/recognizers/_:recognize), so no
	// static endpoint is correct for anybody.
	//
	// The row stays published because the catalog answers "what does this build
	// implement", and hiding a supported vendor would be its own lie. But the
	// planner refuses it until configured rather than dialling a literal
	// PROJECT_ID and handing back a vendor 404 an operator cannot act on.
	RequiresDeploymentConfig string `json:"requires_deployment_config,omitempty"`
}

type CatalogModelRoute struct {
	ModelPrefix string
	Endpoint    string
}

var providerCatalog = []CatalogEntry{
	{Provider: "deepgram", Kind: protocol.SessionKindSTT, Adapter: "deepgram.stt.v1", DefaultModel: "flux-general-en", Transport: protocol.TransportWebSocket, Endpoint: "wss://api.deepgram.com/v2/listen", ModelRoutes: []CatalogModelRoute{{ModelPrefix: "flux-", Endpoint: "wss://api.deepgram.com/v2/listen"}, {ModelPrefix: "", Endpoint: "wss://api.deepgram.com/v1/listen"}}},
	{Provider: "deepgram", Kind: protocol.SessionKindTTS, Adapter: "deepgram.tts.v1", DefaultModel: "flux-haley-en", Transport: protocol.TransportWebSocket, Endpoint: "wss://api.deepgram.com/v2/speak", ModelRoutes: []CatalogModelRoute{{ModelPrefix: "flux-", Endpoint: "wss://api.deepgram.com/v2/speak"}, {ModelPrefix: "", Endpoint: "wss://api.deepgram.com/v1/speak"}}},
	// The realtime Scribe family, NOT the batch `scribe_v2`: only this id streams
	// over /v1/speech-to-text/realtime.
	{Provider: "elevenlabs", Kind: protocol.SessionKindSTT, Adapter: "elevenlabs.stt.v1", DefaultModel: "scribe_v2_realtime", Transport: protocol.TransportWebSocket, Endpoint: "wss://api.elevenlabs.io/v1/speech-to-text/realtime"},
	{Provider: "elevenlabs", Kind: protocol.SessionKindTTS, Adapter: "elevenlabs.tts.v1", DefaultModel: "eleven_flash_v2_5", Transport: protocol.TransportWebSocket, Endpoint: "wss://api.elevenlabs.io/v1/text-to-speech"},
	{Provider: "cartesia", Kind: protocol.SessionKindSTT, Adapter: "cartesia.stt.v1", DefaultModel: "ink-2", Transport: protocol.TransportWebSocket, Endpoint: "wss://api.cartesia.ai/stt/websocket"},
	{Provider: "cartesia", Kind: protocol.SessionKindTTS, Adapter: "cartesia.tts.v1", DefaultModel: "sonic-3", Transport: protocol.TransportWebSocket, Endpoint: "wss://api.cartesia.ai/tts/websocket"},
	{Provider: "assemblyai", Kind: protocol.SessionKindSTT, Adapter: "assemblyai.stt.v1", DefaultModel: "universal-3-5-pro", Transport: protocol.TransportWebSocket, Endpoint: "wss://streaming.assemblyai.com/v3/ws"},
	// Modulate selects its two streaming transcription models by endpoint path,
	// not a model query field. English Fast is the conversational default;
	// multilingual remains explicitly selectable through the same adapter.
	{Provider: "modulate", Kind: protocol.SessionKindSTT, Adapter: "modulate.stt.v1", DefaultModel: "velma-2-stt-streaming-english-v2", Transport: protocol.TransportWebSocket, Endpoint: "wss://platform.modulate.ai/api/velma-2-stt-streaming-english-v2", ModelRoutes: []CatalogModelRoute{{ModelPrefix: "velma-2-stt-streaming-english-v2", Endpoint: "wss://platform.modulate.ai/api/velma-2-stt-streaming-english-v2"}, {ModelPrefix: "velma-2-stt-streaming", Endpoint: "wss://platform.modulate.ai/api/velma-2-stt-streaming"}}},
	// Gladia discovers its real socket at runtime: an init call returns
	// a URL with the session token already embedded. The endpoint here is the
	// nominal one a plan carries — the adapter derives the init call from it (BYOK)
	// or ignores it in favour of the credential (managed).
	{Provider: "gladia", Kind: protocol.SessionKindSTT, Adapter: "gladia.stt.v1", DefaultModel: "solaria-1", Transport: protocol.TransportWebSocket, Endpoint: "wss://api.gladia.io/v2/live"},
	{Provider: "minimax", Kind: protocol.SessionKindTTS, Adapter: "minimax.tts.v1", DefaultModel: "speech-2.8-hd", Transport: protocol.TransportWebSocket, Endpoint: "wss://api.minimax.io/ws/v1/t2a_v2"},
	{Provider: "xai", Kind: protocol.SessionKindTTS, Adapter: "xai.tts.v1", DefaultModel: "tts", Transport: protocol.TransportWebSocket, Endpoint: "wss://api.x.ai/v1/tts"},
	// The first two HTTP-transport rows. The engine is transport-agnostic — it only
	// needs a ProviderStream — so these stream by decoding the response body
	// incrementally rather than by holding a socket open.
	{Provider: "google", Kind: protocol.SessionKindTTS, Adapter: "google.tts.v1", DefaultModel: "chirp-3-hd", Transport: protocol.TransportHTTP, Endpoint: "https://texttospeech.googleapis.com/v1/text:synthesize"},
	// Alibaba's realtime socket is one path serving both modalities; the model id
	// selects which. International host — the mainland twin is a separate account.
	{Provider: "alibaba", Kind: protocol.SessionKindSTT, Adapter: "alibaba.stt.v1", DefaultModel: "qwen3-asr-flash-realtime", Transport: protocol.TransportWebSocket, Endpoint: "wss://dashscope-intl.aliyuncs.com/api-ws/v1/realtime"},
	{Provider: "alibaba", Kind: protocol.SessionKindTTS, Adapter: "alibaba.tts.v1", DefaultModel: "qwen3-tts-flash-realtime", Transport: protocol.TransportWebSocket, Endpoint: "wss://dashscope-intl.aliyuncs.com/api-ws/v1/realtime"},
	// Google STT is the one PROJECT-SCOPED endpoint in the catalog: the real path
	// is /v2/projects/{project}/locations/{location}/recognizers/_:recognize, so a
	// plan MUST rewrite the project and location. The row below is a template, not
	// a dialable URL — `eu` because Chirp 3 wins hi/ta/te only from that region.
	{Provider: "google", Kind: protocol.SessionKindSTT, Adapter: "google.stt.v1", DefaultModel: "chirp_3", Transport: protocol.TransportHTTP, Endpoint: "https://speech.googleapis.com/v2/projects/PROJECT_ID/locations/eu/recognizers/_:recognize",
		RequiresDeploymentConfig: "set SPEKO_GOOGLE_STT_ENDPOINT to a project-scoped recognize URL"},
	{Provider: "gradium", Kind: protocol.SessionKindSTT, Adapter: "gradium.stt.v1", DefaultModel: "default", Transport: protocol.TransportWebSocket, Endpoint: "wss://api.gradium.ai/api/speech/asr"},
	{Provider: "gradium", Kind: protocol.SessionKindTTS, Adapter: "gradium.tts.v1", DefaultModel: "default", DefaultVoice: "YTpq7expH9539ERJ", Transport: protocol.TransportWebSocket, Endpoint: "wss://api.gradium.ai/api/speech/tts"},
	{Provider: "rime", Kind: protocol.SessionKindTTS, Adapter: "rime.tts.v1", DefaultModel: "coda", DefaultVoice: "astra", Transport: protocol.TransportWebSocket, Endpoint: "wss://users-ws.rime.ai/ws3"},
	{Provider: "hume", Kind: protocol.SessionKindTTS, Adapter: "hume.tts.v1", DefaultModel: "octave-2", DefaultVoice: "Colton Rivers", Transport: protocol.TransportHTTP, Endpoint: "https://api.hume.ai/v0/tts/stream/json"},
	{Provider: "fish", Kind: protocol.SessionKindTTS, Adapter: "fish.tts.v1", DefaultModel: "s2.1-pro", DefaultVoice: "802e3bc2b27e49c2995d23ef70e6ac89", Transport: protocol.TransportWebSocket, Endpoint: "wss://api.fish.audio/v1/tts/live"},
	{Provider: "inworld", Kind: protocol.SessionKindSTT, Adapter: "inworld.stt.v1", DefaultModel: "inworld-stt-1", Transport: protocol.TransportWebSocket, Endpoint: "wss://api.inworld.ai/stt/v1/transcribe:streamBidirectional"},
	{Provider: "xai", Kind: protocol.SessionKindSTT, Adapter: "xai.stt.v1", DefaultModel: "stt", Transport: protocol.TransportWebSocket, Endpoint: "wss://api.x.ai/v1/stt"},
	{Provider: "openai", Kind: protocol.SessionKindSTT, Adapter: "openai.stt.v1", DefaultModel: "gpt-live-transcribe", Transport: protocol.TransportWebSocket, Endpoint: "wss://api.openai.com/v1/realtime"},
	{Provider: "openai", Kind: protocol.SessionKindTTS, Adapter: "openai.tts.v1", DefaultModel: "gpt-4o-mini-tts", Transport: protocol.TransportHTTP, Endpoint: "https://api.openai.com/v1/audio/speech"},
	{Provider: "soniox", Kind: protocol.SessionKindSTT, Adapter: "soniox.stt.v1", DefaultModel: "stt-rt-v5", Transport: protocol.TransportWebSocket, Endpoint: "wss://stt-rt.soniox.com/transcribe-websocket"},
	{Provider: "soniox", Kind: protocol.SessionKindTTS, Adapter: "soniox.tts.v1", DefaultModel: "tts-rt-v2", Transport: protocol.TransportWebSocket, Endpoint: "wss://tts-rt.soniox.com/tts-websocket"},
	{Provider: "smallest", Kind: protocol.SessionKindSTT, Adapter: "smallest.stt.v1", DefaultModel: "pulse", Transport: protocol.TransportWebSocket, Endpoint: "wss://api.smallest.ai/waves/v1/stt/live"},
	{Provider: "smallest", Kind: protocol.SessionKindTTS, Adapter: "smallest.tts.v1", DefaultModel: "lightning_v3.1", Transport: protocol.TransportWebSocket, Endpoint: "wss://api.smallest.ai/waves/v1/tts/live"},
	{Provider: "inworld", Kind: protocol.SessionKindTTS, Adapter: "inworld.tts.v1", DefaultModel: "inworld-tts-2", Transport: protocol.TransportHTTP, Endpoint: "https://api.inworld.ai/tts/v1/voice:stream"},
}

// Catalog returns every (provider, modality) this build implements, ordered so the
// published list is stable across restarts.
func Catalog() []CatalogEntry {
	entries := append([]CatalogEntry(nil), providerCatalog...)
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Provider != entries[j].Provider {
			return entries[i].Provider < entries[j].Provider
		}
		return entries[i].Kind < entries[j].Kind
	})
	return entries
}

// catalogHasProvider reports whether any modality of this provider is implemented.
func catalogHasProvider(provider string) bool {
	for _, entry := range providerCatalog {
		if entry.Provider == provider {
			return true
		}
	}
	return false
}

func catalogEntryFor(kind protocol.SessionKind, provider string) (CatalogEntry, bool) {
	for _, entry := range providerCatalog {
		if entry.Provider == provider && entry.Kind == kind {
			return entry, true
		}
	}
	return CatalogEntry{}, false
}

func catalogEntryForAdapter(adapter string) (CatalogEntry, bool) {
	for _, entry := range providerCatalog {
		if entry.Adapter == adapter {
			return entry, true
		}
	}
	return CatalogEntry{}, false
}

// catalogModel is one published model row.
type catalogModel struct {
	ID        string               `json:"id"`
	Provider  string               `json:"provider"`
	Kind      protocol.SessionKind `json:"kind"`
	Adapter   string               `json:"adapter"`
	Transport protocol.Transport   `json:"transport"`
	// Installed reports whether THIS process has the adapter loaded. A model can be
	// in the catalog and absent from a given deployment, and a caller needs to tell
	// those apart before wiring an id: only an installed row can open a session
	// here. Reporting the row rather than hiding it keeps the published list the
	// same shape everywhere while staying honest about this instance.
	Installed bool `json:"installed"`
	// RequiresConfig repeats the catalog's reason so a caller reading this list
	// learns the row needs deployment config BEFORE wiring the id, rather than
	// from a failed session.
	RequiresConfig string `json:"requires_config,omitempty"`
}

// models publishes the catalog. Unauthenticated on purpose: it names no customer,
// carries no credential, and an integrator has to be able to read it before the
// first session exists. It is exactly the data a `/gateway/models` page renders.
func (s *Server) models(writer http.ResponseWriter, request *http.Request) {
	installed := make(map[string]struct{}, len(s.runtime.Adapters))
	for _, adapter := range s.runtime.Adapters {
		installed[adapter] = struct{}{}
	}
	kindFilter := strings.ToLower(strings.TrimSpace(request.URL.Query().Get("kind")))
	providerFilter := strings.ToLower(strings.TrimSpace(request.URL.Query().Get("provider")))

	rows := make([]catalogModel, 0, len(providerCatalog))
	for _, entry := range Catalog() {
		if kindFilter != "" && string(entry.Kind) != kindFilter {
			continue
		}
		if providerFilter != "" && entry.Provider != providerFilter {
			continue
		}
		_, present := installed[entry.Adapter]
		rows = append(rows, catalogModel{
			ID:             entry.Provider + ":" + entry.DefaultModel,
			Provider:       entry.Provider,
			Kind:           entry.Kind,
			Adapter:        entry.Adapter,
			Transport:      entry.Transport,
			Installed:      present,
			RequiresConfig: entry.RequiresDeploymentConfig,
		})
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"protocol":          protocol.VoiceV0,
		"protocol_revision": protocol.CurrentRevision,
		"runtime": map[string]string{
			"name":      s.runtime.Name,
			"version":   s.runtime.Version,
			"placement": string(s.runtime.Placement),
		},
		"models": rows,
	})
}
