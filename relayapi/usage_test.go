package relayapi_test

import (
	"encoding/json"
	"testing"

	"github.com/SpekoAI/gateway/relayapi"
)

func TestUsageSplitLinesSumToTotals(t *testing.T) {
	t.Parallel()

	// Split lines are mutually exclusive, so the totals are plain sums.
	split := relayapi.Usage{
		InputTokens:       400,
		CachedInputTokens: 100,
		OutputTokens:      50,
		ReasoningTokens:   25,
	}
	if got := split.TotalInputTokens(); got != 500 {
		t.Fatalf("TotalInputTokens = %d, want 500", got)
	}
	if got := split.TotalOutputTokens(); got != 75 {
		t.Fatalf("TotalOutputTokens = %d, want 75", got)
	}

	// A provider reporting no split reports all-uncached / all-visible:
	// the totals must come out identical to the split report above.
	unsplit := relayapi.Usage{InputTokens: 500, OutputTokens: 75}
	if got := unsplit.TotalInputTokens(); got != split.TotalInputTokens() {
		t.Fatalf("unsplit TotalInputTokens = %d, want %d", got, split.TotalInputTokens())
	}
	if got := unsplit.TotalOutputTokens(); got != split.TotalOutputTokens() {
		t.Fatalf("unsplit TotalOutputTokens = %d, want %d", got, split.TotalOutputTokens())
	}
}

func TestUsageValidateRejectsNegativeLines(t *testing.T) {
	t.Parallel()

	if err := (relayapi.Usage{}).Validate(); err != nil {
		t.Fatalf("zero usage must validate: %v", err)
	}

	cases := []struct {
		name  string
		usage relayapi.Usage
	}{
		{"duration_ms", relayapi.Usage{DurationMS: -1}},
		{"characters", relayapi.Usage{Characters: -1}},
		{"input_tokens", relayapi.Usage{InputTokens: -1}},
		{"cached_input_tokens", relayapi.Usage{CachedInputTokens: -1}},
		{"cache_write_5m_tokens", relayapi.Usage{CacheWrite5mTokens: -1}},
		{"cache_write_1h_tokens", relayapi.Usage{CacheWrite1hTokens: -1}},
		{"output_tokens", relayapi.Usage{OutputTokens: -1}},
		{"reasoning_tokens", relayapi.Usage{ReasoningTokens: -1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertInvalid(t, tc.usage.Validate(), tc.name+": must not be negative")
		})
	}
}

func TestCacheWriteUsageWireFixture(t *testing.T) {
	const fixture = `{"input_tokens":300,"cached_input_tokens":100,"cache_write_5m_tokens":11,"cache_write_1h_tokens":19,"output_tokens":34}`
	var usage relayapi.Usage
	if err := json.Unmarshal([]byte(fixture), &usage); err != nil {
		t.Fatal(err)
	}
	if err := usage.Validate(); err != nil {
		t.Fatal(err)
	}
	if usage.TotalInputTokens() != 430 || usage.TotalOutputTokens() != 34 {
		t.Fatalf("usage: %+v", usage)
	}
	encoded, err := json.Marshal(usage)
	if err != nil {
		t.Fatal(err)
	}
	var roundtrip relayapi.Usage
	if err := json.Unmarshal(encoded, &roundtrip); err != nil || roundtrip != usage {
		t.Fatalf("roundtrip %s: %v", encoded, err)
	}
	var legacy relayapi.Usage
	if err := json.Unmarshal([]byte(`{"input_tokens":430}`), &legacy); err != nil || legacy.TotalInputTokens() != 430 {
		t.Fatalf("legacy: %+v, %v", legacy, err)
	}
}
