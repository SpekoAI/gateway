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
// happens before admission: a request using a capability a model does not
// advertise is rejected with capability_unsupported, never silently stripped.
type ModelCapabilities struct {
	Tools            bool `json:"tools"`
	StructuredOutput bool `json:"structured_output"`
	CachedInput      bool `json:"cached_input"`
	Reasoning        bool `json:"reasoning"`
	Diarization      bool `json:"diarization"`
	Keywords         bool `json:"keywords"`
	NoiseReduction   bool `json:"noise_reduction"`
}

// SampleRateRange is an inclusive set of sample rates accepted by one audio
// format. It is used instead of enumerating every integer rate for providers
// that accept a bounded continuous range.
type SampleRateRange struct {
	Min int `json:"min"`
	Max int `json:"max"`
}

// AudioFormat is one exact media constraint advertised for a speech model.
// Exactly one of SampleRatesHz and SampleRateRangeHz must be present.
type AudioFormat struct {
	Encoding          string           `json:"encoding"`
	SampleRatesHz     []int            `json:"sample_rates_hz,omitempty"`
	SampleRateRangeHz *SampleRateRange `json:"sample_rate_range_hz,omitempty"`
	Channels          []int            `json:"channels"`
}

// Validate checks that the format is concrete and unambiguous.
func (a AudioFormat) Validate() error {
	if a.Encoding != "pcm_s16le" && a.Encoding != "opus" {
		return fmt.Errorf("encoding: unsupported value %q", a.Encoding)
	}
	if (len(a.SampleRatesHz) == 0) == (a.SampleRateRangeHz == nil) {
		return fmt.Errorf("exactly one of sample_rates_hz and sample_rate_range_hz is required")
	}
	for i, rate := range a.SampleRatesHz {
		if rate < 8_000 || rate > 192_000 || (i > 0 && rate <= a.SampleRatesHz[i-1]) {
			return fmt.Errorf("sample_rates_hz: values must be unique, ascending, and between 8000 and 192000")
		}
	}
	if r := a.SampleRateRangeHz; r != nil && (r.Min < 8_000 || r.Max > 192_000 || r.Min > r.Max) {
		return fmt.Errorf("sample_rate_range_hz: invalid inclusive range")
	}
	if len(a.Channels) == 0 {
		return fmt.Errorf("channels: at least one value is required")
	}
	for i, channels := range a.Channels {
		if channels < 1 || channels > 8 || (i > 0 && channels <= a.Channels[i-1]) {
			return fmt.Errorf("channels: values must be unique, ascending, and between 1 and 8")
		}
	}
	return nil
}

// Supports reports whether the exact request media satisfies this format.
func (a AudioFormat) Supports(encoding string, sampleRateHz, channels int) bool {
	if a.Encoding != encoding || !containsInt(a.Channels, channels) {
		return false
	}
	if a.SampleRateRangeHz != nil {
		return sampleRateHz >= a.SampleRateRangeHz.Min && sampleRateHz <= a.SampleRateRangeHz.Max
	}
	return containsInt(a.SampleRatesHz, sampleRateHz)
}

func containsInt(values []int, want int) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// SupportsSTTOptions reports whether this model advertises every canonical
// ask the options carry, and names the first one it does not. Provider
// settings are the routed vendor's own and are not checked here.
func (c ModelCapabilities) SupportsSTTOptions(options *STTOptions) (string, bool) {
	switch {
	case options.Diarize() && !c.Diarization:
		return "diarization", false
	case len(options.GetKeywords()) > 0 && !c.Keywords:
		return "keywords", false
	case options.ReduceNoise() && !c.NoiseReduction:
		return "noise_reduction", false
	default:
		return "", true
	}
}

// Model is one currently routable catalog entry. Regions lists the
// Speko relay regions (AWS region ids) where the model is routable right
// now — relay locations, never provider-processing residency.
type Model struct {
	ID           string            `json:"id"`
	Provider     string            `json:"provider"`
	Kind         Kind              `json:"kind"`
	Capabilities ModelCapabilities `json:"capabilities"`
	Regions      []string          `json:"regions"`
	AudioFormats []AudioFormat     `json:"audio_formats,omitempty"`
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
	if m.Kind == KindLLM && len(m.AudioFormats) != 0 {
		return fmt.Errorf("audio_formats: must be omitted for llm models")
	}
	if (m.Kind == KindSTT || m.Kind == KindTTS) && len(m.AudioFormats) == 0 {
		return fmt.Errorf("audio_formats: at least one format is required for speech models")
	}
	for i, format := range m.AudioFormats {
		if err := format.Validate(); err != nil {
			return fmt.Errorf("audio_formats[%d]: %w", i, err)
		}
	}
	return nil
}

// ModelsResponse is the GET /v1/models body: the currently routable slice
// of the catalog. CatalogDigest identifies the release-pinned catalog that
// produced the listing so support can correlate a caller's view with an
// exact release.
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
