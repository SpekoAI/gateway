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
