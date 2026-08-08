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
// because a byte stream has no place for a JSON envelope.
type SpeechRequest struct {
	Routing Routing     `json:"routing"`
	Input   string      `json:"input"`
	Voice   string      `json:"voice,omitempty"`
	Audio   AudioConfig `json:"audio"`
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
// utterance.started and utterance.done. Exactly one terminal frame —
// session.closed or error — ends every session. The set is closed.
type TTSEventType string

const (
	TTSEventSessionReady     TTSEventType = "session.ready"
	TTSEventUtteranceStarted TTSEventType = "utterance.started"
	TTSEventUtteranceDone    TTSEventType = "utterance.done"
	TTSEventUsageUpdated     TTSEventType = "usage.updated"
	TTSEventSessionClosed    TTSEventType = "session.closed"
	TTSEventError            TTSEventType = "error"
)

// TTSSessionConfigure opens a streaming synthesis session.
type TTSSessionConfigure struct {
	Type    TTSControlType `json:"type"`
	Routing Routing        `json:"routing"`
	Voice   string         `json:"voice,omitempty"`
	Audio   AudioConfig    `json:"audio"`
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
