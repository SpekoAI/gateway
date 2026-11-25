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
	if !unbilledZero(envelope.Usage, "thoughtsTokenCount", "cachedContentTokenCount", "toolUsePromptTokenCount") {
		s.billingValid = false
		s.billingInvalid = true
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

// batchBilling reads the Interactions usage block into the two dimensions
// Google invoices for gemini-3.5-transcribe: input AUDIO tokens ($2 per M) and
// output TEXT tokens ($12 per M). Measured live 2026-11-25 on a 7.9 s clip:
//
//	{"total_input_tokens":199,"input_tokens_by_modality":[{"modality":"text",
//	 "tokens":1},{"modality":"audio","tokens":198}],"total_output_tokens":29,
//	 "total_thought_tokens":0,"total_cached_tokens":0,...}
//
// The Cloud Billing export for the same project carries exactly two SKUs for
// the model, "audio input token count" and "text output token count", and the
// fleet's output share there (19.7M output against 135.9M audio input tokens
// over 14 days, 0.145) matches the probe's 29/198. The one text input token is
// the service's own instruction and has no SKU, so it is not reported: a
// reported quantity the frozen schedule cannot price would leave the whole
// attempt unresolved.
//
// The observation is complete only when both billed counts are present.
// Thought, cached and tool-use tokens are not billed dimensions of this model
// today; a non-zero value marks the observation incomplete rather than letting
// a newly billed dimension vanish.
func batchBilling(model, interactionID string, usage json.RawMessage) *protocol.BillingObservation {
	o := &protocol.BillingObservation{OperationID: "request", Model: model, Mode: "batch", ProviderRequestID: interactionID, ProviderResponseID: interactionID, Quantities: map[string]int64{}}
	var fields map[string]json.RawMessage
	if json.Unmarshal(usage, &fields) != nil {
		return o
	}
	var byModality []struct {
		Modality string          `json:"modality"`
		Tokens   json.RawMessage `json:"tokens"`
	}
	if raw := fields["input_tokens_by_modality"]; raw == nil || json.Unmarshal(raw, &byModality) != nil {
		return o
	}
	audio, hasAudio := int64(0), false
	for _, entry := range byModality {
		if entry.Modality != "audio" {
			continue
		}
		n, ok := metering.Quantity(entry.Tokens, 1000)
		if !ok || hasAudio {
			return o
		}
		audio, hasAudio = n, true
	}
	output, hasOutput := metering.Quantity(fields["total_output_tokens"], 1000)
	if !hasAudio || !hasOutput || !unbilledZero(usage, "total_thought_tokens", "total_cached_tokens", "total_tool_use_tokens") {
		return o
	}
	o.Quantities["input_audio_tokens"] = audio
	o.Quantities["output_tokens"] = output
	o.Complete = true
	return o
}

// unbilledZero reports whether every named usage field the schedule does not
// price is absent or exactly zero. A present non-zero or unreadable value is a
// newly billed dimension, and the caller must not report the observation as
// complete without it.
func unbilledZero(usage json.RawMessage, fields ...string) bool {
	var object map[string]json.RawMessage
	if json.Unmarshal(usage, &object) != nil {
		return false
	}
	for _, field := range fields {
		raw, present := object[field]
		if !present {
			continue
		}
		if n, ok := metering.Quantity(raw, 1000); !ok || n != 0 {
			return false
		}
	}
	return true
}
