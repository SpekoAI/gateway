package gemini

import (
	"context"
	"encoding/json"
	"github.com/SpekoAI/gateway/metering"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

// This adapter sends only text and requests only AUDIO. The response totals
// therefore describe text input and audio output. No tokenization estimate is
// used. Repeated usageMetadata contains cumulative request totals.
func (s *ttsStream) observeTTSBilling(ctx context.Context, raw []byte) {
	var envelope struct {
		Usage json.RawMessage `json:"usageMetadata"`
		ID    string          `json:"responseId"`
	}
	if json.Unmarshal(raw, &envelope) != nil || len(envelope.Usage) == 0 {
		return
	}
	input, hasInput := metering.Quantity(envelope.Usage, 1000, "promptTokenCount")
	output, hasOutput := metering.Quantity(envelope.Usage, 1000, "candidatesTokenCount")
	s.stateMu.Lock()
	if s.billing == nil {
		s.stateMu.Unlock()
		return
	}
	next := s.billing.Clone()
	if envelope.ID != "" {
		next.ProviderResponseID = envelope.ID
	}
	if hasInput {
		next.Quantities["input_tokens"] = input
	}
	if hasOutput {
		next.Quantities["output_audio_tokens"] = output
	}
	merged, err := protocol.MergeBillingObservation(*s.billing, next)
	if err == nil {
		s.billing = &merged
	}
	s.billingInvalid = s.billingInvalid || err != nil
	s.billingValid = !s.billingInvalid && hasInput && hasOutput
	// The TTS model does not enable thinking or caching. A newly billed dimension
	// must not quietly disappear if a provider starts returning it.
	for _, field := range []string{"thoughtsTokenCount", "cachedContentTokenCount", "toolUsePromptTokenCount"} {
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(envelope.Usage, &fields)
		if _, present := fields[field]; present {
			n, ok := metering.Quantity(envelope.Usage, 1000, field)
			if !ok || n != 0 {
				s.billingValid = false
				s.billingInvalid = true
			}
		}
	}
	s.stateMu.Unlock()
	s.emit(ctx, runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Billing: s.billingSnapshot(false)})
}

func (s *ttsStream) billingSnapshot(complete bool) *protocol.BillingObservation {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.billing == nil {
		return nil
	}
	o := s.billing.Clone()
	o.Complete = complete && s.billingValid
	return &o
}
