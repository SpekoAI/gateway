package relayapi

import (
	"encoding/json"
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
	// Translation is the transcript translated into
	// options.translation.target_language, present only when translation
	// was asked for. Text stays the ORIGINAL spoken words, so a caller that
	// never asks reads a byte-identical response. Translated words carry no
	// timings (the vendor generates them after the words they translate),
	// so there are no translated segments.
	Translation string `json:"translation,omitempty"`
	Route       Route  `json:"route"`
	Usage       Usage  `json:"usage"`
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
// Every commit is answered by exactly one transcript.final, which may be
// empty when nothing was left to finalize.
type STTInputCommit struct {
	Type STTControlType `json:"type"`
	// CommitID optionally names the commit; the transcript.final that
	// answers it carries the same id, so a client can tell its answer from
	// a final the provider sent on its own. See ValidCommitID for the form.
	CommitID string `json:"commit_id,omitempty"`
	// commitIDSent records a commit_id member on the wire, so Validate can
	// refuse "commit_id":"" instead of reading it as no id.
	commitIDSent bool
}

// UnmarshalJSON notes whether the frame carried commit_id. A null
// commit_id counts as absent, as a null member does everywhere in relayapi.
func (c *STTInputCommit) UnmarshalJSON(data []byte) error {
	type wire STTInputCommit
	var decoded struct {
		wire
		CommitID *string `json:"commit_id"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*c = STTInputCommit(decoded.wire)
	if decoded.CommitID != nil {
		c.CommitID, c.commitIDSent = *decoded.CommitID, true
	}
	return nil
}

// Validate checks the frame tag and, when present, the commit id.
func (c STTInputCommit) Validate() error {
	if c.Type != STTControlInputCommit {
		return fmt.Errorf("type: got %q, want %q", c.Type, STTControlInputCommit)
	}
	if (c.CommitID != "" || c.commitIDSent) && !ValidCommitID(c.CommitID) {
		return fmt.Errorf("commit_id: want 1-64 characters of A-Z a-z 0-9 . _ : -")
	}
	return nil
}

// MaxCommitIDLength bounds STTInputCommit.CommitID.
const MaxCommitIDLength = 64

// ValidCommitID reports whether id is a well-formed commit id: 1 to
// MaxCommitIDLength characters, each an ASCII letter or digit or one of
// . _ : -
func ValidCommitID(id string) bool {
	if id == "" || len(id) > MaxCommitIDLength {
		return false
	}
	for i := 0; i < len(id); i++ {
		switch c := id[i]; {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '.', c == '_', c == ':', c == '-':
		default:
			return false
		}
	}
	return true
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
	// CommitIDs is true when the server echoes each input.commit's commit_id
	// on the transcript.final that answers it, so a client can choose
	// correlated commits up front instead of guessing. False and absent both
	// mean no echo.
	CommitIDs bool `json:"commit_ids,omitempty"`
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
	// Translation is the interim translation of the current span, present
	// only on a session that asked for one. It trails Text: the vendor
	// translates words after it recognises them, so a delta may carry
	// original words whose translation has not arrived yet, or only a
	// translation of words already recognised.
	Translation string `json:"translation,omitempty"`
}

// Validate checks the frame tag and that the delta says something — empty
// interim hypotheses are dropped at normalization, never forwarded. On a
// translating session a delta that advances only the translation is
// something.
func (e STTTranscriptDelta) Validate() error {
	if e.Type != STTEventTranscriptDelta {
		return fmt.Errorf("type: got %q, want %q", e.Type, STTEventTranscriptDelta)
	}
	if e.Text == "" && e.Translation == "" {
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
	// Translation is the finalized translation delivered with this span,
	// present only on a session that asked for one. Text stays the original
	// words. The vendor's translation trails its transcript, so the last
	// words of a span can be translated in the NEXT final's Translation; a
	// caller rendering captions concatenates finals rather than pairing them
	// one to one.
	Translation string `json:"translation,omitempty"`
	// CommitID is the id of the input.commit this final answers, present
	// only when that commit carried one. A final the provider sent with no
	// commit pending carries none.
	CommitID string `json:"commit_id,omitempty"`
}

// Validate checks the frame tag, segments and, when present, the commit id.
// Text may be empty: a finalized span of silence is legitimate.
func (e STTTranscriptFinal) Validate() error {
	if e.Type != STTEventTranscriptFinal {
		return fmt.Errorf("type: got %q, want %q", e.Type, STTEventTranscriptFinal)
	}
	if e.CommitID != "" && !ValidCommitID(e.CommitID) {
		return fmt.Errorf("commit_id: want 1-64 characters of A-Z a-z 0-9 . _ : -")
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
