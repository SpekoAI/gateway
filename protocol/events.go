package protocol

import "encoding/json"

// EventType is a provider-neutral streaming event name.
type EventType string

const (
	EventSessionReady      EventType = "session.ready"
	EventSessionRecovering EventType = "session.recovering"
	EventSessionRecovered  EventType = "session.recovered"
	EventSessionClosed     EventType = "session.closed"

	EventSpeechStarted    EventType = "speech.started"
	EventSpeechEnded      EventType = "speech.ended"
	EventTranscriptDelta  EventType = "transcript.delta"
	EventTranscriptFinal  EventType = "transcript.final"
	EventResponseStarted  EventType = "response.started"
	EventTextDelta        EventType = "text.delta"
	EventTextDone         EventType = "text.done"
	EventToolCall         EventType = "tool.call"
	EventResponseDone     EventType = "response.done"
	EventResponseCanceled EventType = "response.cancelled"
	EventAudioStarted     EventType = "audio.started"
	// EventAudioFrame is the in-memory marker for a binary TTS frame. WebSocket
	// transports carry the associated Event.Audio bytes in a binary frame.
	EventAudioFrame    EventType = "audio.frame"
	EventAudioDone     EventType = "audio.done"
	EventAlignment     EventType = "alignment"
	EventRouteSelected EventType = "route.selected"
	EventUsageObserved EventType = "usage.observed"
	// EventUsageReported is authenticated, content-free metering telemetry from
	// the Gateway. It reports only a normalized quantity, never media or text.
	EventUsageReported EventType = "usage.reported"
	EventWarning       EventType = "warning"
	EventError         EventType = "error"
)

// Event is the canonical in-memory representation of a streaming server
// event. Audio is deliberately not JSON encoded: transports send it in binary
// frames and the embedded API preserves the supplied buffer without copying.
// Once delivered, Audio is owned by the consumer and must not be retained by
// the runtime.
type Event struct {
	Type        EventType                  `json:"type"`
	EventID     string                     `json:"event_id"`
	SessionID   string                     `json:"session_id"`
	AttemptID   string                     `json:"attempt_id"`
	Sequence    uint64                     `json:"sequence"`
	CreatedAtMS int64                      `json:"created_at_ms"`
	Data        json.RawMessage            `json:"data,omitempty"`
	Extensions  map[string]json.RawMessage `json:"extensions,omitempty"`
	Audio       []byte                     `json:"-"`
}
