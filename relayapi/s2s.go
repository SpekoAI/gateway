package relayapi

import (
	"fmt"
	"strings"
)

// S2SControlType is retained for decoding legacy internal connector frames.
// The Relay exposes no public speech-to-speech media route.
type S2SControlType string

const (
	// S2SControlSessionConfigure must be the first text frame on the socket.
	// Its exact frame bytes are the WebSocket idempotency content hash (see
	// idempotency.go); later frames are intentionally outside the hash.
	S2SControlSessionConfigure S2SControlType = "session.configure"
	// S2SControlInputCommit tells the model the caller finished speaking. It
	// is a hint: every routed model also detects turn ends on its own.
	S2SControlInputCommit S2SControlType = "input.commit"
	// S2SControlResponseCancel interrupts the answer the model is speaking
	// (barge-in). Audio already delivered stays delivered and billed.
	S2SControlResponseCancel S2SControlType = "response.cancel"
	S2SControlSessionClose   S2SControlType = "session.close"
)

// S2SEventType is retained for decoding legacy internal connector frames.
type S2SEventType string

const (
	S2SEventSessionReady            S2SEventType = "session.ready"
	S2SEventInputSpeechStarted      S2SEventType = "input.speech_started"
	S2SEventInputSpeechEnded        S2SEventType = "input.speech_ended"
	S2SEventInputTranscript         S2SEventType = "input.transcript"
	S2SEventResponseStarted         S2SEventType = "response.started"
	S2SEventResponseTranscriptDelta S2SEventType = "response.transcript.delta"
	S2SEventResponseDone            S2SEventType = "response.done"
	S2SEventResponseCancelled       S2SEventType = "response.cancelled"
	S2SEventUsageUpdated            S2SEventType = "usage.updated"
	S2SEventSessionClosed           S2SEventType = "session.closed"
	S2SEventError                   S2SEventType = "error"
)

// MaxS2SInstructionsBytes bounds the system prompt a session may carry. It
// rides the configure frame, and therefore the idempotency hash, so it is
// bounded like any other request body field rather than streamed.
const MaxS2SInstructionsBytes = 32 << 10

// S2SAudioConfig declares both directions of a speech-to-speech session:
// the raw binary frames the client sends and the raw binary frames it wants
// back. Streamed audio has no container, so both formats are declared up
// front, and they may differ — most models listen and speak at different
// rates.
type S2SAudioConfig struct {
	Input  AudioConfig `json:"input"`
	Output AudioConfig `json:"output"`
}

// Validate checks both directions.
func (a S2SAudioConfig) Validate() error {
	if err := a.Input.Validate(); err != nil {
		return fmt.Errorf("input: %w", err)
	}
	if err := a.Output.Validate(); err != nil {
		return fmt.Errorf("output: %w", err)
	}
	return nil
}

// S2SSessionConfigure opens a speech-to-speech session. Voice, language,
// instructions and temperature are session intent: they ride this frame and
// the idempotency hash, and later frames cannot change them.
type S2SSessionConfigure struct {
	Type    S2SControlType `json:"type"`
	Routing Routing        `json:"routing"`
	Audio   S2SAudioConfig `json:"audio"`
	// Language is an optional hint: auto routing ranks candidates on that
	// language's board and the relay picks a voice curated for it.
	Language string `json:"language,omitempty"`
	// Voice is optional: in auto mode the party that picks the provider picks
	// a default voice for it.
	Voice string `json:"voice,omitempty"`
	// Instructions is the system prompt the model speaks under.
	Instructions string `json:"instructions,omitempty"`
	// Temperature is the sampling temperature, 0 to 2 inclusive; omitted
	// leaves the vendor default.
	Temperature *float64 `json:"temperature,omitempty"`
}

// Validate checks the frame tag, routing, both audio formats, the
// instructions bound and the temperature range. The routing must already be
// normalized.
func (c S2SSessionConfigure) Validate() error {
	if c.Type != S2SControlSessionConfigure {
		return fmt.Errorf("type: got %q, want %q", c.Type, S2SControlSessionConfigure)
	}
	if err := c.Routing.Validate(); err != nil {
		return fmt.Errorf("routing: %w", err)
	}
	if err := c.Audio.Validate(); err != nil {
		return fmt.Errorf("audio: %w", err)
	}
	if len([]byte(c.Instructions)) > MaxS2SInstructionsBytes {
		return fmt.Errorf("instructions: must be at most %d bytes", MaxS2SInstructionsBytes)
	}
	if c.Temperature != nil && (*c.Temperature < 0 || *c.Temperature > 2) {
		return fmt.Errorf("temperature: must be between 0 and 2")
	}
	return nil
}

// S2SInputCommit tells the model the caller finished speaking.
type S2SInputCommit struct {
	Type S2SControlType `json:"type"`
}

// Validate checks the frame tag.
func (c S2SInputCommit) Validate() error {
	if c.Type != S2SControlInputCommit {
		return fmt.Errorf("type: got %q, want %q", c.Type, S2SControlInputCommit)
	}
	return nil
}

// S2SResponseCancel interrupts the answer being spoken.
type S2SResponseCancel struct {
	Type S2SControlType `json:"type"`
}

// Validate checks the frame tag.
func (c S2SResponseCancel) Validate() error {
	if c.Type != S2SControlResponseCancel {
		return fmt.Errorf("type: got %q, want %q", c.Type, S2SControlResponseCancel)
	}
	return nil
}

// S2SSessionClose asks the server to end the session with a terminal
// session.closed frame.
type S2SSessionClose struct {
	Type S2SControlType `json:"type"`
}

// Validate checks the frame tag.
func (c S2SSessionClose) Validate() error {
	if c.Type != S2SControlSessionClose {
		return fmt.Errorf("type: got %q, want %q", c.Type, S2SControlSessionClose)
	}
	return nil
}

// S2SSessionReady confirms admission and reports the concrete route. It is
// always the first event frame; the client must not send audio before it.
type S2SSessionReady struct {
	Type      S2SEventType `json:"type"`
	RequestID string       `json:"request_id"`
	Route     Route        `json:"route"`
}

// Validate checks the frame tag, request id, and route.
func (e S2SSessionReady) Validate() error {
	if e.Type != S2SEventSessionReady {
		return fmt.Errorf("type: got %q, want %q", e.Type, S2SEventSessionReady)
	}
	if strings.TrimSpace(e.RequestID) == "" {
		return fmt.Errorf("request_id: required")
	}
	if err := e.Route.Validate(); err != nil {
		return fmt.Errorf("route: %w", err)
	}
	return nil
}

// S2SInputSpeechStarted reports that the model detected the caller speaking.
type S2SInputSpeechStarted struct {
	Type S2SEventType `json:"type"`
}

// Validate checks the frame tag.
func (e S2SInputSpeechStarted) Validate() error {
	if e.Type != S2SEventInputSpeechStarted {
		return fmt.Errorf("type: got %q, want %q", e.Type, S2SEventInputSpeechStarted)
	}
	return nil
}

// S2SInputSpeechEnded reports that the model detected the caller's turn end.
type S2SInputSpeechEnded struct {
	Type S2SEventType `json:"type"`
}

// Validate checks the frame tag.
func (e S2SInputSpeechEnded) Validate() error {
	if e.Type != S2SEventInputSpeechEnded {
		return fmt.Errorf("type: got %q, want %q", e.Type, S2SEventInputSpeechEnded)
	}
	return nil
}

// S2SInputTranscript is the model's transcription of what the caller said.
// Final marks a completed span; a non-final transcript is an interim
// hypothesis superseded by later frames.
type S2SInputTranscript struct {
	Type  S2SEventType `json:"type"`
	Text  string       `json:"text"`
	Final bool         `json:"final"`
}

// Validate checks the frame tag. Text may be empty on a final frame: a
// finalized span of silence is legitimate.
func (e S2SInputTranscript) Validate() error {
	if e.Type != S2SEventInputTranscript {
		return fmt.Errorf("type: got %q, want %q", e.Type, S2SEventInputTranscript)
	}
	if !e.Final && e.Text == "" {
		return fmt.Errorf("text: required on an interim transcript")
	}
	return nil
}

// S2SResponseStarted announces the binary audio frames of one spoken answer.
// Sequence is the 1-based answer index within the session.
type S2SResponseStarted struct {
	Type     S2SEventType `json:"type"`
	Sequence int          `json:"sequence"`
}

// Validate checks the frame tag and sequence.
func (e S2SResponseStarted) Validate() error {
	if e.Type != S2SEventResponseStarted {
		return fmt.Errorf("type: got %q, want %q", e.Type, S2SEventResponseStarted)
	}
	if e.Sequence < 1 {
		return fmt.Errorf("sequence: must be at least 1")
	}
	return nil
}

// S2SResponseTranscriptDelta is a piece of the text of the answer being
// spoken, for captions and logs.
type S2SResponseTranscriptDelta struct {
	Type     S2SEventType `json:"type"`
	Sequence int          `json:"sequence"`
	Text     string       `json:"text"`
}

// Validate checks the frame tag, sequence and that the delta says something.
func (e S2SResponseTranscriptDelta) Validate() error {
	if e.Type != S2SEventResponseTranscriptDelta {
		return fmt.Errorf("type: got %q, want %q", e.Type, S2SEventResponseTranscriptDelta)
	}
	if e.Sequence < 1 {
		return fmt.Errorf("sequence: must be at least 1")
	}
	if e.Text == "" {
		return fmt.Errorf("text: required")
	}
	return nil
}

// S2SResponseDone marks the end of one answer's binary audio frames.
type S2SResponseDone struct {
	Type     S2SEventType `json:"type"`
	Sequence int          `json:"sequence"`
}

// Validate checks the frame tag and sequence.
func (e S2SResponseDone) Validate() error {
	if e.Type != S2SEventResponseDone {
		return fmt.Errorf("type: got %q, want %q", e.Type, S2SEventResponseDone)
	}
	if e.Sequence < 1 {
		return fmt.Errorf("sequence: must be at least 1")
	}
	return nil
}

// S2SResponseCancelled reports that an answer stopped before it finished —
// because the caller interrupted it (barge-in or response.cancel). No
// response.done follows for that sequence.
type S2SResponseCancelled struct {
	Type     S2SEventType `json:"type"`
	Sequence int          `json:"sequence"`
}

// Validate checks the frame tag and sequence.
func (e S2SResponseCancelled) Validate() error {
	if e.Type != S2SEventResponseCancelled {
		return fmt.Errorf("type: got %q, want %q", e.Type, S2SEventResponseCancelled)
	}
	if e.Sequence < 1 {
		return fmt.Errorf("sequence: must be at least 1")
	}
	return nil
}

// S2SUsageUpdated reports cumulative usage while the session is live. For
// s2s, duration_ms is connected wall-clock time so far.
type S2SUsageUpdated struct {
	Type  S2SEventType `json:"type"`
	Usage Usage        `json:"usage"`
}

// Validate checks the frame tag and usage.
func (e S2SUsageUpdated) Validate() error {
	if e.Type != S2SEventUsageUpdated {
		return fmt.Errorf("type: got %q, want %q", e.Type, S2SEventUsageUpdated)
	}
	if err := e.Usage.Validate(); err != nil {
		return fmt.Errorf("usage: %w", err)
	}
	return nil
}

// S2SSessionClosed is the clean terminal frame carrying final usage.
type S2SSessionClosed struct {
	Type  S2SEventType `json:"type"`
	Usage Usage        `json:"usage"`
}

// Validate checks the frame tag and usage.
func (e S2SSessionClosed) Validate() error {
	if e.Type != S2SEventSessionClosed {
		return fmt.Errorf("type: got %q, want %q", e.Type, S2SEventSessionClosed)
	}
	if err := e.Usage.Validate(); err != nil {
		return fmt.Errorf("usage: %w", err)
	}
	return nil
}
