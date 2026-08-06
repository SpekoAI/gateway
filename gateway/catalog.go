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
	Transport    protocol.Transport   `json:"transport"`
	Endpoint     string               `json:"endpoint"`
}

var providerCatalog = []CatalogEntry{
	{Provider: "deepgram", Kind: protocol.SessionKindSTT, Adapter: "deepgram.stt.v1", DefaultModel: "nova-3", Transport: protocol.TransportWebSocket, Endpoint: "wss://api.deepgram.com/v1/listen"},
	{Provider: "deepgram", Kind: protocol.SessionKindTTS, Adapter: "deepgram.tts.v1", DefaultModel: "aura-2-thalia-en", Transport: protocol.TransportWebSocket, Endpoint: "wss://api.deepgram.com/v1/speak"},
	// The realtime Scribe family, NOT the batch `scribe_v2`: only this id streams
	// over /v1/speech-to-text/realtime.
	{Provider: "elevenlabs", Kind: protocol.SessionKindSTT, Adapter: "elevenlabs.stt.v1", DefaultModel: "scribe_v2_realtime", Transport: protocol.TransportWebSocket, Endpoint: "wss://api.elevenlabs.io/v1/speech-to-text/realtime"},
	{Provider: "elevenlabs", Kind: protocol.SessionKindTTS, Adapter: "elevenlabs.tts.v1", DefaultModel: "eleven_flash_v2_5", Transport: protocol.TransportWebSocket, Endpoint: "wss://api.elevenlabs.io/v1/text-to-speech"},
	{Provider: "cartesia", Kind: protocol.SessionKindSTT, Adapter: "cartesia.stt.v1", DefaultModel: "ink-2", Transport: protocol.TransportWebSocket, Endpoint: "wss://api.cartesia.ai/stt/websocket"},
	{Provider: "cartesia", Kind: protocol.SessionKindTTS, Adapter: "cartesia.tts.v1", DefaultModel: "sonic-3", Transport: protocol.TransportWebSocket, Endpoint: "wss://api.cartesia.ai/tts/websocket"},
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

func catalogEntryFor(kind protocol.SessionKind, provider string) (CatalogEntry, bool) {
	for _, entry := range providerCatalog {
		if entry.Provider == provider && entry.Kind == kind {
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
			ID:        entry.Provider + ":" + entry.DefaultModel,
			Provider:  entry.Provider,
			Kind:      entry.Kind,
			Adapter:   entry.Adapter,
			Transport: entry.Transport,
			Installed: present,
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
