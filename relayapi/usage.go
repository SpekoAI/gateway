package relayapi

import "fmt"

// Usage reports normalized, provider-neutral quantities for one request or
// stream. Only the lines relevant to the request's kind are set; zero lines
// are omitted on the wire.
//
// Split lines are mutually exclusive by contract: a token counted in
// cached_input_tokens is not repeated in input_tokens, and a reasoning token
// is not repeated in output_tokens. The splits therefore always sum to the
// totals — see TotalInputTokens and TotalOutputTokens. Providers that report
// no split report all-uncached / all-visible: everything lands in
// input_tokens and output_tokens with the split lines at zero.
type Usage struct {
	DurationMS        int64 `json:"duration_ms,omitempty"`
	Characters        int64 `json:"characters,omitempty"`
	InputTokens       int64 `json:"input_tokens,omitempty"`
	CachedInputTokens int64 `json:"cached_input_tokens,omitempty"`
	OutputTokens      int64 `json:"output_tokens,omitempty"`
	ReasoningTokens   int64 `json:"reasoning_tokens,omitempty"`
}

// TotalInputTokens is the full prompt size: uncached plus cached input.
// It is a sum precisely because the split lines are mutually exclusive.
func (u Usage) TotalInputTokens() int64 {
	return u.InputTokens + u.CachedInputTokens
}

// TotalOutputTokens is the full generation size: visible output plus
// reasoning. It is a sum precisely because the split lines are mutually
// exclusive.
func (u Usage) TotalOutputTokens() int64 {
	return u.OutputTokens + u.ReasoningTokens
}

// Validate rejects negative quantities. A zero Usage is valid: usage may
// legitimately be empty before any output was produced.
func (u Usage) Validate() error {
	for _, line := range []struct {
		field string
		value int64
	}{
		{"duration_ms", u.DurationMS},
		{"characters", u.Characters},
		{"input_tokens", u.InputTokens},
		{"cached_input_tokens", u.CachedInputTokens},
		{"output_tokens", u.OutputTokens},
		{"reasoning_tokens", u.ReasoningTokens},
	} {
		if line.value < 0 {
			return fmt.Errorf("%s: must not be negative", line.field)
		}
	}
	return nil
}
