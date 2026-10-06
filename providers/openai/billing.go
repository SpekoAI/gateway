package openai

import (
	"encoding/json"

	"github.com/SpekoAI/gateway/metering"
	"github.com/SpekoAI/gateway/protocol"
)

// OpenAI states a transcription's billing basis on the wire. The `usage` union
// is identical on /v1/audio/transcriptions and on the Realtime
// `conversation.item.input_audio_transcription.completed` event, and the vendor
// documents its two arms as "usage statistics for models billed by token usage"
// (`type: "tokens"`) and "usage statistics for models billed by audio input
// duration" (`type: "duration"`).
//
// sttDurationBilledModels is that same split, taken from the models the pricing
// page quotes per audio MINUTE rather than per token: gpt-transcribe $0.0045,
// gpt-live-transcribe and gpt-realtime-whisper $0.017, whisper-1 $0.006. Every
// other transcription model this package serves publishes per-token rates.
// https://developers.openai.com/api/docs/pricing, checked 2026-09-25.
var sttDurationBilledModels = map[string]struct{}{
	"gpt-transcribe":       {},
	"gpt-live-transcribe":  {},
	"gpt-realtime-whisper": {},
	"whisper-1":            {},
}

// sttBillingBasis names the usage arm this model's published price is quoted in.
func sttBillingBasis(model string) string {
	if _, ok := sttDurationBilledModels[model]; ok {
		return "duration"
	}
	return "tokens"
}

// transcriptionBilling normalizes one provider `usage` object. It never
// substitutes one basis for the other: a response that reports tokens for a
// per-minute model — or a duration for a per-token one — leaves the observation
// INCOMPLETE, so the attempt becomes an unresolved obligation rather than being
// priced on a quantity the frozen schedule does not carry. Audio the caller
// sent is never a fallback for either count.
func transcriptionBilling(operationID, model, mode string, usage json.RawMessage) *protocol.BillingObservation {
	incomplete := &protocol.BillingObservation{OperationID: operationID, Model: model, Mode: mode, Quantities: map[string]int64{}}
	if len(usage) == 0 {
		return incomplete
	}
	var envelope struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(usage, &envelope) != nil || envelope.Type != sttBillingBasis(model) {
		return incomplete
	}
	switch envelope.Type {
	case "duration":
		// `seconds` is fractional; metering.Duration keeps sub-millisecond
		// values exact by switching denominators rather than rounding.
		return metering.Duration(operationID, model, mode, usage, 1000, "seconds")
	case "tokens":
		input, hasInput := metering.Quantity(usage, 1000, "input_tokens")
		output, hasOutput := metering.Quantity(usage, 1000, "output_tokens")
		if !hasInput || !hasOutput {
			return incomplete
		}
		// input_token_details splits the same input_tokens total into text and
		// audio; the published transcription price has ONE input rate, so the
		// total is the billable dimension and the split stays diagnostic.
		incomplete.Quantities["input_tokens"] = input
		incomplete.Quantities["output_tokens"] = output
		incomplete.Complete = true
	}
	return incomplete
}

// ttsBilling normalizes the `usage` object on a /v1/audio/speech
// `speech.audio.done` event. gpt-4o-mini-tts bills $0.60 per M input TEXT
// tokens and $12 per M output AUDIO tokens, so both counts are billable and the
// accepted character count cannot stand in for either.
// https://developers.openai.com/api/docs/pricing, checked 2026-09-25.
func ttsBilling(operationID, model string, usage json.RawMessage) *protocol.BillingObservation {
	o := &protocol.BillingObservation{OperationID: operationID, Model: model, Mode: "streaming", Quantities: map[string]int64{}}
	if len(usage) == 0 {
		return o
	}
	input, hasInput := metering.Quantity(usage, 1000, "input_tokens")
	output, hasOutput := metering.Quantity(usage, 1000, "output_tokens")
	if !hasInput || !hasOutput {
		return o
	}
	o.Quantities["input_tokens"] = input
	o.Quantities["output_audio_tokens"] = output
	o.Complete = true
	return o
}
