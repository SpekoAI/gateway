package relayapi

import (
	"fmt"
	"strings"
)

// Multipart part names for POST /v1/stt/transcriptions. The audio always
// travels as an uploaded part, never as a URL: the relay fetches nothing on
// a caller's behalf. The parts are hashed for idempotency in order — see
// idempotency.go.
const (
	TranscriptionRequestPart = "request"
	TranscriptionAudioPart   = "audio"
)

// TranscriptionRequest is the JSON metadata part of a batch transcription.
// The audio container itself (WAV/PCM headers at launch) declares the media
// format, so the metadata carries only routing and options.
type TranscriptionRequest struct {
	Routing  Routing     `json:"routing"`
	Language string      `json:"language,omitempty"`
	Options  *STTOptions `json:"options,omitempty"`
}

// Validate checks the metadata part. Callers normalize Routing with
// NormalizeDefault first — an omitted routing object defaults to
// {mode: auto, objective: balanced}.
func (r TranscriptionRequest) Validate() error {
	if err := r.Routing.Validate(); err != nil {
		return fmt.Errorf("routing: %w", err)
	}
	if err := r.Options.Validate(); err != nil {
		return fmt.Errorf("options: %w", err)
	}
	return nil
}

// TranscriptSegment is one time-aligned span of the transcript.
type TranscriptSegment struct {
	Text    string `json:"text"`
	StartMS int64  `json:"start_ms"`
	EndMS   int64  `json:"end_ms"`
	// Speaker is the vendor's own label, carried verbatim.
	Speaker string `json:"speaker,omitempty"`
}

// Validate checks that the span is a non-negative, ordered range.
func (s TranscriptSegment) Validate() error {
	if s.StartMS < 0 || s.EndMS < s.StartMS {
		return fmt.Errorf("start_ms and end_ms must form a non-negative ordered range")
	}
	return nil
}

// TranscriptWord is one word of the transcript with its own timing. Present
// only when the request asked for word_timestamps and the routed model
// advertises it; the vendor's own speaker label rides along when the request
// also asked for diarization.
type TranscriptWord struct {
	Text    string `json:"text"`
	StartMS int64  `json:"start_ms"`
	EndMS   int64  `json:"end_ms"`
	Speaker string `json:"speaker,omitempty"`
}

// Validate checks that the word is a non-negative, ordered range with text.
func (w TranscriptWord) Validate() error {
	if strings.TrimSpace(w.Text) == "" {
		return fmt.Errorf("text is required")
	}
	if w.StartMS < 0 || w.EndMS < w.StartMS {
		return fmt.Errorf("start_ms and end_ms must form a non-negative ordered range")
	}
	return nil
}

// TranscriptionResponse is the batch transcription result. Usage carries the
// billed duration; there is deliberately no STT usage header.
type TranscriptionResponse struct {
	Text     string              `json:"text"`
	Segments []TranscriptSegment `json:"segments,omitempty"`
	// Words are per-word timings, present only when word_timestamps was asked
	// for. Omitted otherwise, so every existing response is byte-identical.
	Words []TranscriptWord `json:"words,omitempty"`
	Route Route            `json:"route"`
	Usage Usage            `json:"usage"`
}

// Validate checks segments, words, route, and usage. Text may be empty:
// silent audio legitimately transcribes to nothing.
func (r TranscriptionResponse) Validate() error {
	for i, segment := range r.Segments {
		if err := segment.Validate(); err != nil {
			return fmt.Errorf("segments[%d]: %w", i, err)
		}
	}
	for i, word := range r.Words {
		if err := word.Validate(); err != nil {
			return fmt.Errorf("words[%d]: %w", i, err)
		}
	}
	if err := r.Route.Validate(); err != nil {
		return fmt.Errorf("route: %w", err)
	}
	if err := r.Usage.Validate(); err != nil {
		return fmt.Errorf("usage: %w", err)
	}
	return nil
}

// STTControlType tags every client-to-server JSON text frame on
// GET /v1/stt/stream. Audio input travels in binary frames and is
// deliberately untyped here. The set is closed.
type STTControlType string

const (
	// STTControlSessionConfigure must be the first text frame on the
	// socket. Its exact frame bytes are the WebSocket idempotency content
	// hash (see idempotency.go); later frames are intentionally outside
	// the hash.
	STTControlSessionConfigure STTControlType = "session.configure"
	STTControlInputCommit      STTControlType = "input.commit"
	STTControlSessionClose     STTControlType = "session.close"
)

// STTEventType tags every server-to-client JSON text frame on
// GET /v1/stt/stream. Exactly one terminal frame — session.closed or
// error — ends every session. The set is closed.
type STTEventType string

const (
	STTEventSessionReady    STTEventType = "session.ready"
	STTEventTranscriptDelta STTEventType = "transcript.delta"
	STTEventTranscriptFinal STTEventType = "transcript.final"
	STTEventUsageUpdated    STTEventType = "usage.updated"
	STTEventSessionClosed   STTEventType = "session.closed"
	STTEventError           STTEventType = "error"
)

// STTSessionConfigure opens a streaming transcription. Audio describes the
// raw binary frames the client will send: streamed audio has no container,
// so the format must be declared up front.
type STTSessionConfigure struct {
	Type     STTControlType `json:"type"`
	Routing  Routing        `json:"routing"`
	Audio    AudioConfig    `json:"audio"`
	Language string         `json:"language,omitempty"`
	Options  *STTOptions    `json:"options,omitempty"`
}

// Validate checks the frame tag, routing, declared audio format, and option
// asks. The routing must already be normalized.
func (c STTSessionConfigure) Validate() error {
	if c.Type != STTControlSessionConfigure {
		return fmt.Errorf("type: got %q, want %q", c.Type, STTControlSessionConfigure)
	}
	if err := c.Routing.Validate(); err != nil {
		return fmt.Errorf("routing: %w", err)
	}
	if err := c.Audio.Validate(); err != nil {
		return fmt.Errorf("audio: %w", err)
	}
	if err := c.Options.Validate(); err != nil {
		return fmt.Errorf("options: %w", err)
	}
	return nil
}

// STTInputCommit marks the end of the audio the client wants finalized.
type STTInputCommit struct {
	Type STTControlType `json:"type"`
}

// Validate checks the frame tag.
func (c STTInputCommit) Validate() error {
	if c.Type != STTControlInputCommit {
		return fmt.Errorf("type: got %q, want %q", c.Type, STTControlInputCommit)
	}
	return nil
}

// STTSessionClose asks the server to finish outstanding transcripts and end
// the session with a terminal session.closed frame.
type STTSessionClose struct {
	Type STTControlType `json:"type"`
}

// Validate checks the frame tag.
func (c STTSessionClose) Validate() error {
	if c.Type != STTControlSessionClose {
		return fmt.Errorf("type: got %q, want %q", c.Type, STTControlSessionClose)
	}
	return nil
}

// STTSessionReady confirms admission and reports the concrete route. It is
// always the first event frame; the client must not send audio before it.
type STTSessionReady struct {
	Type      STTEventType `json:"type"`
	RequestID string       `json:"request_id"`
	Route     Route        `json:"route"`
}

// Validate checks the frame tag, request id, and route.
func (e STTSessionReady) Validate() error {
	if e.Type != STTEventSessionReady {
		return fmt.Errorf("type: got %q, want %q", e.Type, STTEventSessionReady)
	}
	if strings.TrimSpace(e.RequestID) == "" {
		return fmt.Errorf("request_id: required")
	}
	if err := e.Route.Validate(); err != nil {
		return fmt.Errorf("route: %w", err)
	}
	return nil
}

// STTTranscriptDelta is an interim hypothesis; later deltas and the next
// transcript.final supersede it.
type STTTranscriptDelta struct {
	Type STTEventType `json:"type"`
	Text string       `json:"text"`
}

// Validate checks the frame tag and that the delta says something — empty
// interim hypotheses are dropped at normalization, never forwarded.
func (e STTTranscriptDelta) Validate() error {
	if e.Type != STTEventTranscriptDelta {
		return fmt.Errorf("type: got %q, want %q", e.Type, STTEventTranscriptDelta)
	}
	if e.Text == "" {
		return fmt.Errorf("text: required")
	}
	return nil
}

// STTTranscriptFinal is a finalized span of transcript with optional
// time-aligned segments.
type STTTranscriptFinal struct {
	Type     STTEventType        `json:"type"`
	Text     string              `json:"text"`
	Segments []TranscriptSegment `json:"segments,omitempty"`
	// Speaker labels the whole finalized turn; per-span attribution rides
	// Segments[].Speaker.
	Speaker string `json:"speaker,omitempty"`
}

// Validate checks the frame tag and segments. Text may be empty: a
// finalized span of silence is legitimate.
func (e STTTranscriptFinal) Validate() error {
	if e.Type != STTEventTranscriptFinal {
		return fmt.Errorf("type: got %q, want %q", e.Type, STTEventTranscriptFinal)
	}
	for i, segment := range e.Segments {
		if err := segment.Validate(); err != nil {
			return fmt.Errorf("segments[%d]: %w", i, err)
		}
	}
	return nil
}

// STTUsageUpdated reports cumulative usage while the stream is live so
// callers can watch spend before the terminal frame.
type STTUsageUpdated struct {
	Type  STTEventType `json:"type"`
	Usage Usage        `json:"usage"`
}

// Validate checks the frame tag and usage.
func (e STTUsageUpdated) Validate() error {
	if e.Type != STTEventUsageUpdated {
		return fmt.Errorf("type: got %q, want %q", e.Type, STTEventUsageUpdated)
	}
	if err := e.Usage.Validate(); err != nil {
		return fmt.Errorf("usage: %w", err)
	}
	return nil
}

// STTSessionClosed is the clean terminal frame carrying final usage.
type STTSessionClosed struct {
	Type  STTEventType `json:"type"`
	Usage Usage        `json:"usage"`
}

// Validate checks the frame tag and usage.
func (e STTSessionClosed) Validate() error {
	if e.Type != STTEventSessionClosed {
		return fmt.Errorf("type: got %q, want %q", e.Type, STTEventSessionClosed)
	}
	if err := e.Usage.Validate(); err != nil {
		return fmt.Errorf("usage: %w", err)
	}
	return nil
}
