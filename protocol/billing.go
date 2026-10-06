package protocol

import (
	"errors"
	"math/big"
	"slices"
	"strings"
)

// BillingRevision versions trusted server-side metering, independently of the
// public relay API. These quantities are never accepted from a customer.
const BillingRevision = 1

// BillingObservation is the cumulative quantity snapshot of one upstream
// operation. Distinct responses/utterances/chunks require distinct OperationIDs.
// An incomplete observation can omit Model until the provider identifies it.
// A Complete observation is immutable; retrying it is harmless. Quantities are
// thousandths of the named billable unit, including explicitly measured zeros.
// Metadata contains classifications only, never request or response content.
type BillingObservation struct {
	ProviderRequestID  string           `json:"provider_request_id,omitempty"`
	ProviderResponseID string           `json:"provider_response_id,omitempty"`
	OperationID        string           `json:"operation_id"`
	Model              string           `json:"model"`
	Mode               string           `json:"mode"`
	Quantities         map[string]int64 `json:"quantities"`
	// QuantityDenominators overrides the default 1000 for fractional duration or credits.
	QuantityDenominators map[string]int64 `json:"quantity_denominators,omitempty"`
	Complete             bool             `json:"complete"`
	ContextTokens        int64            `json:"context_tokens,omitempty"`
	Channels             int64            `json:"channels,omitempty"`
	Language             string           `json:"language,omitempty"`
	Features             []string         `json:"features,omitempty"`
}

// BillingReport is the terminal, normalized evidence for an attempt. Complete
// means every dispatched operation is represented, not just that one finished.
type BillingReport struct {
	Revision   int                  `json:"revision"`
	Complete   bool                 `json:"complete"`
	Operations []BillingObservation `json:"operations"`
}

func ValidBillingUnit(unit string) bool {
	switch unit {
	case "input_tokens", "cached_input_tokens", "cache_write_tokens",
		"cache_write_5m_tokens", "cache_write_1h_tokens", "output_tokens", "reasoning_tokens",
		"input_audio_tokens", "cached_input_audio_tokens", "output_audio_tokens",
		"input_image_tokens", "cached_input_image_tokens", "input_video_tokens",
		"duration_seconds", "characters", "utf8_bytes", "credits", "text_inputs",
		"web_search_calls", "web_search_content_tokens":
		return true
	}
	return false
}

func (o BillingObservation) Validate() error {
	if strings.TrimSpace(o.OperationID) == "" || len(o.OperationID) > 512 || (o.Complete && strings.TrimSpace(o.Model) == "") || (o.Model != "" && strings.TrimSpace(o.Model) == "") || len(o.Model) > 256 {
		return errors.New("billing: operation and model identities required")
	}
	switch o.Mode {
	case "streaming", "batch", "sync", "backend":
	default:
		return errors.New("billing: unknown execution mode")
	}
	if len(o.ProviderRequestID) > 512 || len(o.ProviderResponseID) > 512 || o.ContextTokens < 0 || o.Channels < 0 || o.Channels > 64 || len(o.Language) > 64 || len(o.Features) > 32 || len(o.Quantities) > 32 {
		return errors.New("billing: invalid operation metadata")
	}
	for u, d := range o.QuantityDenominators {
		if _, ok := o.Quantities[u]; !ok || (u != "duration_seconds" && u != "credits") || d < 1 || d > 1_000_000_000 {
			return errors.New("billing: invalid quantity denominator")
		}
	}
	for u, q := range o.Quantities {
		if !ValidBillingUnit(u) || q < 0 || (u != "duration_seconds" && u != "credits" && q%1000 != 0) {
			return errors.New("billing: invalid quantity")
		}
	}
	if o.Complete && len(o.Quantities) == 0 {
		return errors.New("billing: complete operation requires measured quantities")
	}
	seen := map[string]bool{}
	for _, f := range o.Features {
		if f == "" || len(f) > 64 || seen[f] {
			return errors.New("billing: invalid feature")
		}
		seen[f] = true
	}
	return nil
}

func (r BillingReport) Validate() error {
	if r.Revision != BillingRevision || len(r.Operations) > 10000 || (r.Complete && len(r.Operations) == 0) {
		return errors.New("billing: invalid report")
	}
	seen := map[string]bool{}
	for _, o := range r.Operations {
		if err := o.Validate(); err != nil {
			return err
		}
		if seen[o.OperationID] || (r.Complete && !o.Complete) {
			return errors.New("billing: duplicate or incomplete operation")
		}
		seen[o.OperationID] = true
	}
	return nil
}

func (o BillingObservation) Clone() BillingObservation {
	q := o
	q.Quantities = make(map[string]int64, len(o.Quantities))
	for u, n := range o.Quantities {
		q.Quantities[u] = n
	}
	if o.QuantityDenominators != nil {
		q.QuantityDenominators = make(map[string]int64, len(o.QuantityDenominators))
		for u, d := range o.QuantityDenominators {
			q.QuantityDenominators[u] = d
		}
	}
	q.Features = slices.Clone(o.Features)
	return q
}

// MergeBillingObservation accepts monotonic cumulative snapshots and exact
// duplicates. It never sums two snapshots of the same operation.
func MergeBillingObservation(previous, next BillingObservation) (BillingObservation, error) {
	if err := next.Validate(); err != nil {
		return BillingObservation{}, err
	}
	if previous.OperationID == "" {
		return next.Clone(), nil
	}
	if previous.OperationID != next.OperationID || (previous.Model != "" && previous.Model != next.Model) || previous.Mode != next.Mode || previous.Channels != next.Channels || previous.Language != next.Language || !slices.Equal(previous.Features, next.Features) {
		return BillingObservation{}, errors.New("billing: operation identity changed")
	}
	if (previous.Complete && (previous.ProviderRequestID != next.ProviderRequestID || previous.ProviderResponseID != next.ProviderResponseID)) || (previous.ProviderRequestID != "" && previous.ProviderRequestID != next.ProviderRequestID) || (previous.ProviderResponseID != "" && previous.ProviderResponseID != next.ProviderResponseID) {
		return BillingObservation{}, errors.New("billing: provider identity changed")
	}
	if next.ContextTokens < previous.ContextTokens || (previous.Complete && (!next.Complete || next.ContextTokens != previous.ContextTokens || len(next.Quantities) != len(previous.Quantities))) {
		return BillingObservation{}, errors.New("billing: completed evidence changed")
	}
	for u, n := range previous.Quantities {
		v, ok := next.Quantities[u]
		comparison := new(big.Rat).SetFrac(big.NewInt(v), big.NewInt(next.QuantityDenominator(u))).Cmp(new(big.Rat).SetFrac(big.NewInt(n), big.NewInt(previous.QuantityDenominator(u))))
		if !ok || comparison < 0 || (previous.Complete && comparison != 0) {
			return BillingObservation{}, errors.New("billing: quantities regressed or changed after completion")
		}
	}
	return next.Clone(), nil
}

func (o BillingObservation) QuantityDenominator(unit string) int64 {
	if d := o.QuantityDenominators[unit]; d > 0 {
		return d
	}
	return 1000
}
