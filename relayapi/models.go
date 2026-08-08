package relayapi

import (
	"fmt"
	"strings"
)

// Kind selects the provider-neutral operation served by the relay. The set
// is deliberately smaller than the local gateway's session kinds: the relay
// serves stt, tts, and llm, and has no realtime kind.
type Kind string

const (
	KindSTT Kind = "stt"
	KindTTS Kind = "tts"
	KindLLM Kind = "llm"
)

// ModelCapabilities advertises what a model supports. Capability gating
// happens before admission: a request using tools or structured output on a
// model that does not advertise them is rejected with
// capability_unsupported, never silently stripped.
type ModelCapabilities struct {
	Tools            bool `json:"tools"`
	StructuredOutput bool `json:"structured_output"`
	CachedInput      bool `json:"cached_input"`
	Reasoning        bool `json:"reasoning"`
}

// Model is one entitled, currently routable catalog entry. Regions lists the
// Speko relay regions (AWS region ids) where the model is routable right
// now — relay locations, never provider-processing residency.
type Model struct {
	ID           string            `json:"id"`
	Provider     string            `json:"provider"`
	Kind         Kind              `json:"kind"`
	Capabilities ModelCapabilities `json:"capabilities"`
	Regions      []string          `json:"regions"`
}

// Validate checks that a catalog entry is concrete and routable somewhere.
func (m Model) Validate() error {
	if strings.TrimSpace(m.ID) == "" || strings.TrimSpace(m.Provider) == "" {
		return fmt.Errorf("id and provider are required")
	}
	if !validKind(m.Kind) {
		return fmt.Errorf("kind: unsupported value %q", m.Kind)
	}
	if len(m.Regions) == 0 {
		return fmt.Errorf("regions: at least one routable region is required")
	}
	for i, region := range m.Regions {
		if strings.TrimSpace(region) == "" {
			return fmt.Errorf("regions[%d]: region id must not be blank", i)
		}
	}
	return nil
}

// ModelsResponse is the GET /v1/models body: the caller's entitled and
// currently routable slice of the catalog. CatalogDigest identifies the
// release-pinned catalog that produced the listing so support can correlate
// a caller's view with an exact release.
type ModelsResponse struct {
	Models        []Model `json:"models"`
	CatalogDigest string  `json:"catalog_digest"`
}

// Validate checks every entry and the catalog digest format.
func (m ModelsResponse) Validate() error {
	for i, model := range m.Models {
		if err := model.Validate(); err != nil {
			return fmt.Errorf("models[%d]: %w", i, err)
		}
	}
	if !validSHA256Digest(m.CatalogDigest) {
		return fmt.Errorf("catalog_digest: must be sha256:<64 lowercase hex>")
	}
	return nil
}

func validKind(v Kind) bool {
	return v == KindSTT || v == KindTTS || v == KindLLM
}
