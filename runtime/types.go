package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/SpekoAI/gateway/protocol"
)

var (
	// ErrBackpressure means a bounded session queue is full. The caller retains
	// ownership of the rejected AudioInput and may retry or release it.
	ErrBackpressure = errors.New("runtime: backpressure")
	// ErrFrameTooLarge means a frame cannot ever fit in the configured bounded
	// audio queue. The caller retains ownership of the rejected input.
	ErrFrameTooLarge = errors.New("runtime: audio frame exceeds queue limit")
	// ErrSessionClosed means an input was submitted after the session started
	// closing. The caller retains ownership of any rejected audio input.
	ErrSessionClosed = errors.New("runtime: session is closed")
	// ErrSessionAborted means the runtime force-terminated the provider stream
	// because its local consumer or transport could not continue safely.
	ErrSessionAborted = errors.New("runtime: session aborted")
	// ErrSessionLeaseExpired means the session reached its fixed deadline.
	ErrSessionLeaseExpired = errors.New("runtime: session lease expired")
	// ErrUsageLimitExceeded means accepting the input would exceed the fixed
	// provider-usage allowance in the signed reservation. Lease renewal cannot
	// increase this allowance.
	ErrUsageLimitExceeded = errors.New("runtime: authorized usage exceeded")
	// ErrUnsupportedOperation means the selected kind cannot accept that input.
	ErrUnsupportedOperation = errors.New("runtime: unsupported session operation")
	// ErrOutputBackpressure means the consumer did not drain canonical events
	// within the configured bound. The session emits a terminal error event.
	ErrOutputBackpressure = errors.New("runtime: event consumer is too slow")
	// ErrPlanUnverified prevents accidental use of a plan without a configured
	// signature verifier.
	ErrPlanUnverified = errors.New("runtime: session plan was not verified")
)

// Limits bounds all session-owned buffering. Values are per active session.
type Limits struct {
	MaxInputMessages int
	MaxInputBytes    int
	MaxOutputEvents  int
}

// DefaultLimits are intentionally small enough that a misbehaving local
// caller cannot consume unbounded memory, while allowing normal audio frames.
func DefaultLimits() Limits {
	return Limits{
		MaxInputMessages: 64,
		MaxInputBytes:    1 << 20,
		MaxOutputEvents:  64,
	}
}

func (l Limits) validate() error {
	if l.MaxInputMessages < 1 {
		return errors.New("max input messages must be positive")
	}
	if l.MaxInputBytes < 1 {
		return errors.New("max input bytes must be positive")
	}
	if l.MaxOutputEvents < 1 {
		return errors.New("max output events must be positive")
	}
	return nil
}

// OpenRequest supplies the operation-specific information omitted from a
// signed SessionPlan because the plan is also used by local and remote APIs.
type OpenRequest struct {
	Kind    protocol.SessionKind
	Plan    protocol.SessionPlan
	Options protocol.RequestOptions
	Media   *protocol.MediaFormat
}

// LocalCredential is a customer-owned provider credential installed in the
// local runtime. It is deliberately separate from SessionPlan:
// BYOK secrets are never sent to, signed by, or logged by the control plane.
type LocalCredential struct {
	Kind protocol.CredentialKind
	// Exactly one of Value and ValueFile is configured. ValueFile is reread for
	// every session so an external OAuth refresher can rotate short-lived tokens
	// without restarting the Gateway.
	Value     string
	ValueFile string
}

// Adapter is the one provider-specific implementation point in the embedded
// engine. Framework integrations must use Session rather than this interface.
type Adapter interface {
	ID() string
	Open(context.Context, AdapterRequest) (ProviderStream, error)
}

// AdapterRequest contains the concrete, already-validated execution choice.
type AdapterRequest struct {
	Kind    protocol.SessionKind
	Plan    protocol.SessionPlan
	Options protocol.RequestOptions
	Media   *protocol.MediaFormat
}

// ProviderStream is serialized by the runtime: WriteAudio, control methods,
// and Close never run concurrently for one session. Events may arrive
// concurrently from the provider implementation.
type ProviderStream interface {
	Events() <-chan ProviderEvent
	WriteAudio(context.Context, []byte) error
	CommitAudio(context.Context) error
	AppendText(context.Context, string) error
	CommitText(context.Context) error
	Cancel(context.Context) error
	Close(context.Context) error
}

// AbortingProviderStream is an optional fast-close capability used only after
// a terminal runtime failure. Normal sessions always use ProviderStream.Close
// so an upstream can flush final results first.
type AbortingProviderStream interface {
	Abort(context.Context) error
}

// ProviderEvent is a provider adapter's normalized output. The adapter grants
// ownership of Data, Extensions, and Audio to the runtime when sending the
// event; it must not mutate any of them after delivery. Err terminates the
// attempt and is never emitted as a normal event.
type ProviderEvent struct {
	Type       protocol.EventType
	Data       json.RawMessage
	Extensions map[string]json.RawMessage
	Audio      []byte
	Err        error
}

// ProviderError preserves the stable protocol classification when an adapter
// cannot continue an upstream stream. It must not contain credentials or raw
// request URLs.
type ProviderError struct {
	Code           string
	Message        string
	Retryable      bool
	ProviderStatus int
	Cause          error
	// Extensions may retain a provider's raw error payload for the local
	// canonical error event. Telemetry never copies this field.
	Extensions map[string]json.RawMessage
}

func (e *ProviderError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		return e.Message
	}
	if e.Cause != nil {
		return e.Cause.Error()
	}
	return "provider error"
}

func (e *ProviderError) Unwrap() error { return e.Cause }

// PlanVerifier verifies a SessionPlan signature against its trusted issuer.
// Managed routing uses control-plane keys; local routing uses a process-local
// signer. An engine refuses to open sessions without a verifier.
type PlanVerifier interface {
	Verify(context.Context, protocol.SessionPlan) error
}

// PlanVerifierFunc adapts a function into a PlanVerifier.
type PlanVerifierFunc func(context.Context, protocol.SessionPlan) error

func (f PlanVerifierFunc) Verify(ctx context.Context, plan protocol.SessionPlan) error {
	return f(ctx, plan)
}

// TelemetryEvent captures local experience metadata only. It intentionally
// contains no media, transcripts, or provider credentials.
//
// EventID is deterministic per attempt and payload so an at-least-once
// exporter can be deduplicated by the receiver. Destination is empty for
// anonymous local routing or carries the plan-scoped contract for an
// authenticated attempt; Destination.Token is a credential and must never be
// logged or persisted by a sink. Data carries a redacted, provider-neutral
// payload for events that have one (for example usage.observed's provider
// request ID).
type TelemetryEvent struct {
	Name        string
	SessionID   string
	AttemptID   string
	At          time.Time
	EventID     string
	Destination protocol.Telemetry
	Data        json.RawMessage
	// Required marks the minimal terminal/usage events needed to meter a
	// managed route. Optional telemetry controls never suppress these events.
	Required bool
}

// TelemetrySink must return without waiting for network or disk I/O. Returning
// false reports a dropped event; it never affects session delivery.
type TelemetrySink interface {
	TryRecord(TelemetryEvent) bool
}

// NopTelemetry discards all telemetry and is safe for tests and local use.
type NopTelemetry struct{}

func (NopTelemetry) TryRecord(TelemetryEvent) bool { return true }

// Stats is a snapshot of bounded-buffer and terminal-session state.
type Stats struct {
	InputMessages    int
	InputBytes       int
	TelemetryDropped uint64
	EventSequence    uint64
}
