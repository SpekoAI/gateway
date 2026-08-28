package relayapi

import (
	"fmt"
	"strings"
)

// AudioConfig describes raw audio on a relay stream: the output format of a
// TTS request, or the binary input frames of an STT stream. The bounds match
// the local gateway's portable media contract so a caller can move between
// provider-direct and relay routes without re-encoding.
type AudioConfig struct {
	Encoding     string `json:"encoding"`
	SampleRateHz int    `json:"sample_rate_hz"`
	Channels     int    `json:"channels"`
}

// Validate checks that the format is within the portable protocol bounds.
func (a AudioConfig) Validate() error {
	if a.Encoding != "pcm_s16le" && a.Encoding != "opus" {
		return fmt.Errorf("encoding: unsupported value %q", a.Encoding)
	}
	if a.SampleRateHz < 8_000 || a.SampleRateHz > 192_000 {
		return fmt.Errorf("sample_rate_hz: must be between 8000 and 192000")
	}
	if a.Channels < 1 || a.Channels > 8 {
		return fmt.Errorf("channels: must be between 1 and 8")
	}
	return nil
}

// SpeechRequest is the POST /v1/tts/speech body. The response is a raw audio
// stream; the route and billed character count come back in headers
// (Speko-Provider, Speko-Model, Speko-Region, Speko-Usage-Characters)
// because a byte stream has no place for a JSON envelope. Language is an
// optional hint, like the STT shapes: auto routing ranks candidates on that
// language's board and the relay picks a voice curated for it; omitted means
// English, which was the only behavior before the field existed.
type SpeechRequest struct {
	Routing  Routing     `json:"routing"`
	Input    string      `json:"input"`
	Voice    string      `json:"voice,omitempty"`
	Language string      `json:"language,omitempty"`
	Audio    AudioConfig `json:"audio"`
}

// Validate checks routing, input, and the requested output format. Voice is
// optional: in auto mode the party that picks the provider picks a default
// voice for it. Callers normalize Routing with NormalizeDefault first.
func (r SpeechRequest) Validate() error {
	if err := r.Routing.Validate(); err != nil {
		return fmt.Errorf("routing: %w", err)
	}
	if r.Input == "" {
		return fmt.Errorf("input: required")
	}
	if err := r.Audio.Validate(); err != nil {
		return fmt.Errorf("audio: %w", err)
	}
	return nil
}

// TTSControlType tags every client-to-server JSON text frame on
// GET /v1/tts/stream. The set is closed.
type TTSControlType string

const (
	// TTSControlSessionConfigure must be the first text frame on the
	// socket. Its exact frame bytes are the WebSocket idempotency content
	// hash (see idempotency.go); later frames are intentionally outside
	// the hash.
	TTSControlSessionConfigure TTSControlType = "session.configure"
	TTSControlInputAppend      TTSControlType = "input.append"
	TTSControlInputCommit      TTSControlType = "input.commit"
	TTSControlInputCancel      TTSControlType = "input.cancel"
	TTSControlSessionClose     TTSControlType = "session.close"
)

// TTSEventType tags every server-to-client JSON text frame on
// GET /v1/tts/stream. Synthesized audio travels in binary frames between
// utterance.started and utterance.done, and utterance.timings — when the
// caller's model measures speech timings and the client asked for them —
// travels alongside that audio, before the utterance.done that closes it.
// Exactly one terminal frame — session.closed or error — ends every session.
// The set is closed.
type TTSEventType string

const (
	TTSEventSessionReady     TTSEventType = "session.ready"
	TTSEventUtteranceStarted TTSEventType = "utterance.started"
	TTSEventUtteranceTimings TTSEventType = "utterance.timings"
	TTSEventUtteranceDone    TTSEventType = "utterance.done"
	TTSEventUsageUpdated     TTSEventType = "usage.updated"
	TTSEventSessionClosed    TTSEventType = "session.closed"
	TTSEventError            TTSEventType = "error"
)

// TTSSessionConfigure opens a streaming synthesis session. Language is an
// optional hint, like the STT shapes: auto routing ranks candidates on that
// language's board and the relay picks a voice curated for it; omitted means
// English, which was the only behavior before the field existed.
type TTSSessionConfigure struct {
	Type     TTSControlType `json:"type"`
	Routing  Routing        `json:"routing"`
	Voice    string         `json:"voice,omitempty"`
	Language string         `json:"language,omitempty"`
	Audio    AudioConfig    `json:"audio"`
}

// Validate checks the frame tag, routing, and requested output format. The
// routing must already be normalized; the idempotency hash covers the raw
// frame bytes as sent, before any normalization.
func (c TTSSessionConfigure) Validate() error {
	if c.Type != TTSControlSessionConfigure {
		return fmt.Errorf("type: got %q, want %q", c.Type, TTSControlSessionConfigure)
	}
	if err := c.Routing.Validate(); err != nil {
		return fmt.Errorf("routing: %w", err)
	}
	if err := c.Audio.Validate(); err != nil {
		return fmt.Errorf("audio: %w", err)
	}
	return nil
}

// TTSInputAppend adds text to the utterance being assembled. Characters are
// counted against the reserved budget when appended, so an over-budget
// append fails before any provider work happens.
type TTSInputAppend struct {
	Type TTSControlType `json:"type"`
	Text string         `json:"text"`
}

// Validate checks the frame tag and that the append carries text.
func (c TTSInputAppend) Validate() error {
	if c.Type != TTSControlInputAppend {
		return fmt.Errorf("type: got %q, want %q", c.Type, TTSControlInputAppend)
	}
	if c.Text == "" {
		return fmt.Errorf("text: required")
	}
	return nil
}

// TTSInputCommit finalizes the appended text as one utterance and starts
// synthesis.
type TTSInputCommit struct {
	Type TTSControlType `json:"type"`
}

// Validate checks the frame tag.
func (c TTSInputCommit) Validate() error {
	if c.Type != TTSControlInputCommit {
		return fmt.Errorf("type: got %q, want %q", c.Type, TTSControlInputCommit)
	}
	return nil
}

// TTSInputCancel abandons uncommitted appended text and any in-flight
// synthesis for the current utterance.
type TTSInputCancel struct {
	Type TTSControlType `json:"type"`
}

// Validate checks the frame tag.
func (c TTSInputCancel) Validate() error {
	if c.Type != TTSControlInputCancel {
		return fmt.Errorf("type: got %q, want %q", c.Type, TTSControlInputCancel)
	}
	return nil
}

// TTSSessionClose asks the server to finish outstanding synthesis and end
// the session with a terminal session.closed frame.
type TTSSessionClose struct {
	Type TTSControlType `json:"type"`
}

// Validate checks the frame tag.
func (c TTSSessionClose) Validate() error {
	if c.Type != TTSControlSessionClose {
		return fmt.Errorf("type: got %q, want %q", c.Type, TTSControlSessionClose)
	}
	return nil
}

// TTSSessionReady confirms admission and reports the concrete route. It is
// always the first event frame; the client must not append text before it.
type TTSSessionReady struct {
	Type      TTSEventType `json:"type"`
	RequestID string       `json:"request_id"`
	Route     Route        `json:"route"`
}

// Validate checks the frame tag, request id, and route.
func (e TTSSessionReady) Validate() error {
	if e.Type != TTSEventSessionReady {
		return fmt.Errorf("type: got %q, want %q", e.Type, TTSEventSessionReady)
	}
	if strings.TrimSpace(e.RequestID) == "" {
		return fmt.Errorf("request_id: required")
	}
	if err := e.Route.Validate(); err != nil {
		return fmt.Errorf("route: %w", err)
	}
	return nil
}

// TTSUtteranceStarted announces the binary audio frames for one committed
// utterance. Sequence is the 1-based utterance index within the session so
// clients can correlate audio with commits.
type TTSUtteranceStarted struct {
	Type     TTSEventType `json:"type"`
	Sequence int          `json:"sequence"`
}

// Validate checks the frame tag and sequence.
func (e TTSUtteranceStarted) Validate() error {
	if e.Type != TTSEventUtteranceStarted {
		return fmt.Errorf("type: got %q, want %q", e.Type, TTSEventUtteranceStarted)
	}
	if e.Sequence < 1 {
		return fmt.Errorf("sequence: must be at least 1")
	}
	return nil
}

// TTSUtteranceDone marks the end of one utterance's binary audio frames.
type TTSUtteranceDone struct {
	Type     TTSEventType `json:"type"`
	Sequence int          `json:"sequence"`
}

// Validate checks the frame tag and sequence.
func (e TTSUtteranceDone) Validate() error {
	if e.Type != TTSEventUtteranceDone {
		return fmt.Errorf("type: got %q, want %q", e.Type, TTSEventUtteranceDone)
	}
	if e.Sequence < 1 {
		return fmt.Errorf("sequence: must be at least 1")
	}
	return nil
}

// TimingGranularity says what unit a TimingSpan measures.
//
// Synthesis always reports word. A character-level engine still measures
// characters, and the adapters still report exactly what it gave them, but
// the relay groups that reading into words before it reaches a caller — so
// the fold is written once, here, instead of once per client per provider.
//
// character stays in the set for a reading passed through ungrouped. The set
// is closed.
type TimingGranularity string

const (
	TimingGranularityWord      TimingGranularity = "word"
	TimingGranularityCharacter TimingGranularity = "character"
)

// TimingSpan is one time-aligned span of synthesized speech, measured from
// the start of the utterance. Field names and validation mirror
// TranscriptSegment: end_ms is an end time, never a duration.
//
// Spans describe the caller's OWN text — a provider that also reports
// timings over its normalized reading ("five dollars" for "$5") has that
// reading dropped rather than carried, because a client walking its own
// string against normalized spans desynchronizes silently.
type TimingSpan struct {
	Text    string `json:"text"`
	StartMS int64  `json:"start_ms"`
	EndMS   int64  `json:"end_ms"`
}

// Validate checks that the span is a non-negative, ordered range.
func (s TimingSpan) Validate() error {
	if s.StartMS < 0 || s.EndMS < s.StartMS {
		return fmt.Errorf("start_ms and end_ms must form a non-negative ordered range")
	}
	return nil
}

// TTSUtteranceTimings carries time-aligned spans for the utterance sharing
// its sequence number. It is emitted only when the route's model measures
// timings; a model that does not advertise word_timings or character_timings
// never sends it.
//
// One utterance may produce SEVERAL of these events, all carrying the same
// sequence: a long render's spans are flushed as they accumulate rather than
// held for one frame that could exceed the stream's frame ceiling. The
// client concatenates them in arrival order. Every event for an utterance
// arrives before the utterance.done that closes it.
type TTSUtteranceTimings struct {
	Type        TTSEventType      `json:"type"`
	Sequence    int               `json:"sequence"`
	Granularity TimingGranularity `json:"granularity"`
	Spans       []TimingSpan      `json:"spans"`
}

// Validate checks the frame tag, sequence, granularity, and every span.
//
// Spans are checked INDIVIDUALLY and never against each other: engines
// legitimately return adjacent spans that overlap, where one sound is still
// being articulated as the next begins, and a monotonicity rule would reject
// those valid frames.
func (e TTSUtteranceTimings) Validate() error {
	if e.Type != TTSEventUtteranceTimings {
		return fmt.Errorf("type: got %q, want %q", e.Type, TTSEventUtteranceTimings)
	}
	if e.Sequence < 1 {
		return fmt.Errorf("sequence: must be at least 1")
	}
	if e.Granularity != TimingGranularityWord && e.Granularity != TimingGranularityCharacter {
		return fmt.Errorf("granularity: got %q, want %q or %q", e.Granularity, TimingGranularityWord, TimingGranularityCharacter)
	}
	if len(e.Spans) == 0 {
		return fmt.Errorf("spans: required")
	}
	for i, span := range e.Spans {
		if err := span.Validate(); err != nil {
			return fmt.Errorf("spans[%d]: %w", i, err)
		}
	}
	return nil
}

// TTSUsageUpdated reports cumulative usage while the stream is live so
// callers can watch spend before the terminal frame.
type TTSUsageUpdated struct {
	Type  TTSEventType `json:"type"`
	Usage Usage        `json:"usage"`
}

// Validate checks the frame tag and usage.
func (e TTSUsageUpdated) Validate() error {
	if e.Type != TTSEventUsageUpdated {
		return fmt.Errorf("type: got %q, want %q", e.Type, TTSEventUsageUpdated)
	}
	if err := e.Usage.Validate(); err != nil {
		return fmt.Errorf("usage: %w", err)
	}
	return nil
}

// TTSSessionClosed is the clean terminal frame carrying final usage.
type TTSSessionClosed struct {
	Type  TTSEventType `json:"type"`
	Usage Usage        `json:"usage"`
}

// Validate checks the frame tag and usage.
func (e TTSSessionClosed) Validate() error {
	if e.Type != TTSEventSessionClosed {
		return fmt.Errorf("type: got %q, want %q", e.Type, TTSEventSessionClosed)
	}
	if err := e.Usage.Validate(); err != nil {
		return fmt.Errorf("usage: %w", err)
	}
	return nil
}
