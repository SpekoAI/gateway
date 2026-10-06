package openairealtime

import (
	"encoding/json"

	"github.com/SpekoAI/gateway/protocol"
)

// OpenAI Realtime does not sell connected time. It sells tokens, and it
// prices the modalities an order of magnitude apart — gpt-realtime lists $32/M
// for input audio against $4/M for input text, and $64/M for output audio
// against $16/M for output text. A meter that reported only seconds could
// never reach that invoice, which is why every Realtime route has been
// settling at its reserve ceiling and staying out of the billing export.
//
// The eight dimensions below are the whole card: each modality's uncached
// input, its cache reads, and (for audio and text) its generation. They are
// always reported together, explicit zeros included, because the frozen
// billing variant prices an exact dimension set — an observation that dropped
// the zeros would fail to match its own rate card rather than bill short.
const (
	unitInputAudioTokens       = "input_audio_tokens"
	unitCachedInputAudioTokens = "cached_input_audio_tokens"
	unitOutputAudioTokens      = "output_audio_tokens"
	unitInputTextTokens        = "input_tokens"
	unitCachedInputTextTokens  = "cached_input_tokens"
	unitOutputTextTokens       = "output_tokens"
	unitInputImageTokens       = "input_image_tokens"
	unitCachedInputImageTokens = "cached_input_image_tokens"
)

// sessionOperationID anchors a session that produced no response at all. Its
// quantities are the same eight dimensions at zero, complete from the moment
// setup is confirmed, and they never change. Without it, "the caller hung up
// before the model answered" and "this adapter never metered anything" reach
// settlement as the same empty report — the first is a zero charge, the second
// is missing evidence, and they must not be confused.
const sessionOperationID = "session"

// responseUsage is the usage document on a terminal response.done. The
// modality details are the SPLIT of the totals beside them, not additions to
// them: input_token_details.audio_tokens counts every audio token in the
// prompt, cached or not, and cached_tokens_details.audio_tokens says how many
// of those were cache reads. Pricing needs the disjoint pieces, so the
// uncached share is derived by subtraction and every relationship the vendor
// asserts is checked before a single token is billed.
type responseUsage struct {
	TotalTokens       int64 `json:"total_tokens"`
	InputTokens       int64 `json:"input_tokens"`
	OutputTokens      int64 `json:"output_tokens"`
	InputTokenDetails *struct {
		CachedTokens        int64 `json:"cached_tokens"`
		TextTokens          int64 `json:"text_tokens"`
		AudioTokens         int64 `json:"audio_tokens"`
		ImageTokens         int64 `json:"image_tokens"`
		CachedTokensDetails *struct {
			TextTokens  int64 `json:"text_tokens"`
			AudioTokens int64 `json:"audio_tokens"`
			ImageTokens int64 `json:"image_tokens"`
		} `json:"cached_tokens_details"`
	} `json:"input_token_details"`
	OutputTokenDetails *struct {
		TextTokens  int64 `json:"text_tokens"`
		AudioTokens int64 `json:"audio_tokens"`
	} `json:"output_token_details"`
}

// zeroBillingQuantities is the dimension set at rest.
func zeroBillingQuantities() map[string]int64 {
	return map[string]int64{
		unitInputAudioTokens: 0, unitCachedInputAudioTokens: 0, unitOutputAudioTokens: 0,
		unitInputTextTokens: 0, unitCachedInputTextTokens: 0, unitOutputTextTokens: 0,
		unitInputImageTokens: 0, unitCachedInputImageTokens: 0,
	}
}

// sessionBilling is the zero anchor described at sessionOperationID. It is
// emitted once, when the provider confirms setup.
func (s *realtimeStream) sessionBilling() *protocol.BillingObservation {
	if !s.profile.tokenMetered {
		return nil
	}
	return &protocol.BillingObservation{
		OperationID: sessionOperationID, Model: s.model, Mode: "streaming",
		Quantities: zeroBillingQuantities(), Complete: true,
	}
}

// responseBilling turns one terminal response.done into the observation the
// relay ledger prices. A response whose usage document is absent or does not
// reconcile is reported INCOMPLETE rather than dropped: an unexplained
// response has to leave the attempt unsettled, never settle as free speech.
func (s *realtimeStream) responseBilling(event serverEvent) *protocol.BillingObservation {
	if !s.profile.tokenMetered || event.Response == nil || event.Response.ID == "" {
		return nil
	}
	observation := &protocol.BillingObservation{
		OperationID: "response/" + event.Response.ID, ProviderResponseID: event.Response.ID,
		Model: s.model, Mode: "streaming", Quantities: zeroBillingQuantities(),
	}
	quantities, ok := billingQuantities(event.Response.Usage)
	if !ok {
		observation.Quantities = nil
		return observation
	}
	observation.Quantities = quantities
	observation.Complete = true
	return observation
}

// billingQuantities derives the eight disjoint dimensions from one usage
// document, in thousandths of a token as the billing contract requires. It
// returns false — rather than a best-effort count — whenever the vendor's own
// numbers disagree with each other, because a prompt whose modality split does
// not add up to its own input total is not evidence of anything.
func billingQuantities(raw json.RawMessage) (map[string]int64, bool) {
	if len(raw) == 0 {
		return nil, false
	}
	var usage responseUsage
	if json.Unmarshal(raw, &usage) != nil {
		return nil, false
	}
	if usage.InputTokens < 0 || usage.OutputTokens < 0 || usage.TotalTokens < 0 {
		return nil, false
	}
	// A totals-only document is the shape that must never be guessed at: the
	// two modalities differ 8x on input and 4x on output, so splitting an
	// unattributed total any way at all would invent the bill.
	if usage.InputTokens > 0 && usage.InputTokenDetails == nil {
		return nil, false
	}
	if usage.OutputTokens > 0 && usage.OutputTokenDetails == nil {
		return nil, false
	}
	if usage.TotalTokens > 0 && usage.InputTokens+usage.OutputTokens != usage.TotalTokens {
		return nil, false
	}

	var text, audio, image, cachedTotal, cachedText, cachedAudio, cachedImage int64
	if details := usage.InputTokenDetails; details != nil {
		text, audio, image, cachedTotal = details.TextTokens, details.AudioTokens, details.ImageTokens, details.CachedTokens
		if cached := details.CachedTokensDetails; cached != nil {
			cachedText, cachedAudio, cachedImage = cached.TextTokens, cached.AudioTokens, cached.ImageTokens
		} else if cachedTotal > 0 {
			// Cache reads are a tenth to a fifth of the uncached rate on every
			// modality; without the split they cannot be attributed.
			return nil, false
		}
	}
	var outputText, outputAudio int64
	if details := usage.OutputTokenDetails; details != nil {
		outputText, outputAudio = details.TextTokens, details.AudioTokens
	}
	for _, value := range []int64{text, audio, image, cachedTotal, cachedText, cachedAudio, cachedImage, outputText, outputAudio} {
		if value < 0 {
			return nil, false
		}
	}
	if text+audio+image != usage.InputTokens || outputText+outputAudio != usage.OutputTokens {
		return nil, false
	}
	if cachedText+cachedAudio+cachedImage != cachedTotal {
		return nil, false
	}
	if cachedText > text || cachedAudio > audio || cachedImage > image {
		return nil, false
	}
	return map[string]int64{
		unitInputAudioTokens:       (audio - cachedAudio) * 1000,
		unitCachedInputAudioTokens: cachedAudio * 1000,
		unitOutputAudioTokens:      outputAudio * 1000,
		unitInputTextTokens:        (text - cachedText) * 1000,
		unitCachedInputTextTokens:  cachedText * 1000,
		unitOutputTextTokens:       outputText * 1000,
		unitInputImageTokens:       (image - cachedImage) * 1000,
		unitCachedInputImageTokens: cachedImage * 1000,
	}, true
}
