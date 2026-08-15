package openai

import (
	"testing"

	"github.com/SpekoAI/gateway/protocol"
)

// The caller's prompt and keywords fold into the one transcription.prompt
// field this API reads: prose first, then the comma-joined terms on their own
// line — the identical translation the platform's OpenAI adapter performs.
func TestSttPromptMergesPromptAndKeywords(t *testing.T) {
	t.Parallel()
	both := &protocol.SttOptions{
		Keywords: []string{"Speko", "Casey"},
		ProviderOptions: map[string]map[string]any{
			"openai": {"prompt": "Expect a hiring call about voice AI."},
		},
	}
	if got := sttPromptFor(both); got != "Expect a hiring call about voice AI.\nSpeko, Casey" {
		t.Fatalf("prompt = %q", got)
	}
	promptOnly := &protocol.SttOptions{ProviderOptions: map[string]map[string]any{
		"openai": {"prompt": "Names matter."},
	}}
	if got := sttPromptFor(promptOnly); got != "Names matter." {
		t.Fatalf("prompt = %q", got)
	}
	keywordsOnly := &protocol.SttOptions{Keywords: []string{"Speko"}}
	if got := sttPromptFor(keywordsOnly); got != "Speko" {
		t.Fatalf("prompt = %q", got)
	}
	// Validation admits any scalar, so a numeric prompt that validated must
	// not vanish at a string assertion.
	numeric := &protocol.SttOptions{ProviderOptions: map[string]map[string]any{
		"openai": {"prompt": float64(42)},
	}}
	if got := sttPromptFor(numeric); got != "42" {
		t.Fatalf("a scalar prompt must survive: %q", got)
	}
	// No ask, no prompt — and another provider's prompt is not OpenAI's.
	if sttPromptFor(nil) != "" {
		t.Fatal("nil options carry no prompt")
	}
	other := &protocol.SttOptions{ProviderOptions: map[string]map[string]any{
		"deepgram": {"numerals": true},
	}}
	if sttPromptFor(other) != "" {
		t.Fatal("another provider's settings must not become a prompt")
	}
}

// The portable noise_reduction ask becomes `session.audio.input.noise_reduction`,
// an object with a placement type rather than a boolean. near_field is the
// conversational default; a caller wanting the far-field model says so in
// provider options. Verified against the live realtime session, which echoes
// the value back on session.updated.
func TestSttNoiseReductionUsesTheVendorObjectShape(t *testing.T) {
	t.Parallel()
	reduce := true
	if got := sttNoiseReductionFor(nil); got != nil {
		t.Fatalf("no ask, no noise_reduction: %+v", got)
	}
	off := &protocol.SttOptions{}
	if got := sttNoiseReductionFor(off); got != nil {
		t.Fatalf("an unset ask must stay absent: %+v", got)
	}
	near := sttNoiseReductionFor(&protocol.SttOptions{NoiseReduction: &reduce})
	if near == nil || near.Type != "near_field" {
		t.Fatalf("default placement = %+v, want near_field", near)
	}
	far := sttNoiseReductionFor(&protocol.SttOptions{
		NoiseReduction:  &reduce,
		ProviderOptions: map[string]map[string]any{"openai": {"noise_reduction": "far_field"}},
	})
	if far == nil || far.Type != "far_field" {
		t.Fatalf("far-field placement = %+v", far)
	}
	// An unrecognized placement falls back to the default rather than sending
	// the vendor a value its enum does not contain.
	bogus := sttNoiseReductionFor(&protocol.SttOptions{
		NoiseReduction:  &reduce,
		ProviderOptions: map[string]map[string]any{"openai": {"noise_reduction": "outer_space"}},
	})
	if bogus == nil || bogus.Type != "near_field" {
		t.Fatalf("unknown placement = %+v, want the near_field default", bogus)
	}
}
