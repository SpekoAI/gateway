package protocol

import "math"

// TimingGranularity is the unit a TimingSpan measures. It is always what the
// provider itself measured, never a conversion: a character-level engine
// reports TimingGranularityCharacter, and grouping those characters into
// words is left to whoever knows where the words are in the source text.
type TimingGranularity string

const (
	TimingGranularityWord      TimingGranularity = "word"
	TimingGranularityCharacter TimingGranularity = "character"
)

// TimingSpan is one time-aligned span of synthesized speech, measured in
// integer milliseconds from the start of the utterance. EndMS is an end
// time, never a duration, because providers disagree about which they send
// and a normalized event must not make the consumer guess.
//
// Spans are individually well-formed but are NOT mutually ordered: engines
// legitimately return adjacent spans that overlap, where one sound is still
// being articulated as the next begins, and zero-width spans occur for
// characters that carry no measurable duration of their own.
type TimingSpan struct {
	Text    string `json:"text"`
	StartMS int64  `json:"start_ms"`
	EndMS   int64  `json:"end_ms"`
}

// SpeechTimings is the normalized part of an EventAlignment payload for a
// TTS adapter. Adapters embed these two keys alongside whatever native
// timing arrays they already carry, so the raw provider reading stays
// available for debugging while consumers read one shape across providers.
//
// The zero value is the "measured nothing" case and is what an adapter emits
// for a frame it cannot or must not normalize — an empty alignment, or a
// reading taken over normalized rather than caller-supplied text. Consumers
// treat an empty Spans as "stay silent" rather than as an error.
//
// This shape is NOT emitted by the STT adapters that also use
// EventAlignment; it is specific to synthesis.
type SpeechTimings struct {
	Granularity TimingGranularity `json:"granularity,omitempty"`
	Spans       []TimingSpan      `json:"spans,omitempty"`
}

// TimingMSFromSeconds converts a provider's fractional-second timestamp to
// integer milliseconds, rounding rather than truncating: truncation biases
// every span low, and the bias accumulates visibly across a long render.
func TimingMSFromSeconds(seconds float64) int64 {
	if math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return 0
	}
	return int64(math.Round(seconds * 1000))
}

// TimingSpansFromSeconds builds spans from parallel arrays of text, start
// seconds, and END seconds — the shape Cartesia and Soniox send.
//
// Disagreeing array lengths return nil rather than a partial reading: the
// arrays are one measurement split three ways, so a length mismatch means
// the frame was not understood, and half-decoding it would emit spans whose
// text and times belong to different positions.
func TimingSpansFromSeconds(texts []string, starts, ends []float64, granularity TimingGranularity) SpeechTimings {
	if len(texts) == 0 || len(texts) != len(starts) || len(texts) != len(ends) {
		return SpeechTimings{}
	}
	spans := make([]TimingSpan, 0, len(texts))
	for i, text := range texts {
		spans = append(spans, TimingSpan{
			Text:    text,
			StartMS: TimingMSFromSeconds(starts[i]),
			EndMS:   TimingMSFromSeconds(ends[i]),
		})
	}
	return SpeechTimings{Granularity: granularity, Spans: spans}
}

// TimingSpansFromMillisecondDurations builds spans from parallel arrays of
// text, start milliseconds, and DURATION milliseconds — the shape
// ElevenLabs sends on its streaming transport. The end time is the sum, so
// the caller never has to know which of the two the provider meant.
//
// Disagreeing array lengths return nil, for the reason given on
// TimingSpansFromSeconds.
func TimingSpansFromMillisecondDurations(texts []string, starts, durations []int, granularity TimingGranularity) SpeechTimings {
	if len(texts) == 0 || len(texts) != len(starts) || len(texts) != len(durations) {
		return SpeechTimings{}
	}
	spans := make([]TimingSpan, 0, len(texts))
	for i, text := range texts {
		spans = append(spans, TimingSpan{
			Text:    text,
			StartMS: int64(starts[i]),
			EndMS:   int64(starts[i]) + int64(durations[i]),
		})
	}
	return SpeechTimings{Granularity: granularity, Spans: spans}
}
