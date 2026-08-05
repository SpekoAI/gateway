package mock

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

var ErrEventQueueFull = errors.New("mock provider event queue is full")

// Factory constructs a stream for each opened mock session.
type Factory func(runtimepkg.AdapterRequest) *Stream

// Adapter is a deterministic runtime.Adapter suitable for tests. Calls and
// Audio slices are recorded without copying to make ownership tests possible.
type Adapter struct {
	AdapterID string
	Factory   Factory
	OpenError error

	mu      sync.RWMutex
	streams []*Stream
}

// NewAdapter returns a configurable mock adapter.
func NewAdapter(id string, factory Factory) *Adapter {
	return &Adapter{AdapterID: id, Factory: factory}
}

func (a *Adapter) ID() string { return a.AdapterID }

func (a *Adapter) Open(_ context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if a.OpenError != nil {
		return nil, a.OpenError
	}
	if a.Factory == nil {
		return nil, errors.New("mock adapter factory is required")
	}
	stream := a.Factory(request)
	if stream == nil {
		return nil, errors.New("mock adapter factory returned nil")
	}
	a.mu.Lock()
	a.streams = append(a.streams, stream)
	a.mu.Unlock()
	return stream, nil
}

// LastStream returns the most recently opened stream, or nil before Open.
func (a *Adapter) LastStream() *Stream {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if len(a.streams) == 0 {
		return nil
	}
	return a.streams[len(a.streams)-1]
}

// Stream is an inspectable in-memory ProviderStream.
type Stream struct {
	events chan runtimepkg.ProviderEvent

	mu              sync.RWMutex
	closed          bool
	calls           []Call
	WriteAudioHook  func(context.Context, []byte) error
	CommitAudioHook func(context.Context) error
	AppendTextHook  func(context.Context, string) error
	CommitTextHook  func(context.Context) error
	CancelHook      func(context.Context) error
	CloseHook       func(context.Context) error
}

// Call records one operation received by the mock stream. Audio is the exact
// caller-owned slice passed through the runtime; it is intentionally not a
// copy and must be treated as immutable by tests.
type Call struct {
	Kind  string
	Text  string
	Audio []byte
}

// NewStream makes a mock event channel with the requested bounded capacity.
func NewStream(eventCapacity int) *Stream {
	if eventCapacity < 1 {
		eventCapacity = 16
	}
	return &Stream{events: make(chan runtimepkg.ProviderEvent, eventCapacity)}
}

func (s *Stream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

func (s *Stream) WriteAudio(ctx context.Context, audio []byte) error {
	s.record(Call{Kind: "audio", Audio: audio})
	if s.WriteAudioHook != nil {
		return s.WriteAudioHook(ctx, audio)
	}
	return nil
}

func (s *Stream) CommitAudio(ctx context.Context) error {
	s.record(Call{Kind: "audio.commit"})
	if s.CommitAudioHook != nil {
		return s.CommitAudioHook(ctx)
	}
	return nil
}

func (s *Stream) AppendText(ctx context.Context, text string) error {
	s.record(Call{Kind: "text.append", Text: text})
	if s.AppendTextHook != nil {
		return s.AppendTextHook(ctx, text)
	}
	return nil
}

func (s *Stream) CommitText(ctx context.Context) error {
	s.record(Call{Kind: "text.commit"})
	if s.CommitTextHook != nil {
		return s.CommitTextHook(ctx)
	}
	return nil
}

func (s *Stream) Cancel(ctx context.Context) error {
	s.record(Call{Kind: "response.cancel"})
	if s.CancelHook != nil {
		return s.CancelHook(ctx)
	}
	return nil
}

func (s *Stream) Close(ctx context.Context) error {
	s.record(Call{Kind: "session.close"})
	if s.CloseHook != nil {
		if err := s.CloseHook(ctx); err != nil {
			return err
		}
	}
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.events)
	}
	s.mu.Unlock()
	return nil
}

// Emit sends a normalized provider event without blocking. It returns a
// bounded-queue error instead of allowing a test fixture to grow indefinitely.
func (s *Stream) Emit(event runtimepkg.ProviderEvent) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return errors.New("mock provider stream is closed")
	}
	select {
	case s.events <- event:
		return nil
	default:
		return ErrEventQueueFull
	}
}

// Calls returns a snapshot of calls in provider order.
func (s *Stream) Calls() []Call {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]Call(nil), s.calls...)
}

func (s *Stream) record(call Call) {
	s.mu.Lock()
	s.calls = append(s.calls, call)
	s.mu.Unlock()
}

// NewSTTAdapter creates an adapter that emits deterministic transcript events
// for each input frame and a final transcript on CommitAudio.
func NewSTTAdapter(id string) *Adapter {
	return NewAdapter(id, func(_ runtimepkg.AdapterRequest) *Stream {
		stream := NewStream(32)
		var frames int
		stream.WriteAudioHook = func(_ context.Context, _ []byte) error {
			frames++
			if frames == 1 {
				if err := stream.Emit(runtimepkg.ProviderEvent{Type: protocol.EventSpeechStarted}); err != nil {
					return err
				}
			}
			return stream.Emit(runtimepkg.ProviderEvent{
				Type: protocol.EventTranscriptDelta,
				Data: json.RawMessage(`{"text":"mock partial"}`),
			})
		}
		stream.CommitAudioHook = func(_ context.Context) error {
			if err := stream.Emit(runtimepkg.ProviderEvent{Type: protocol.EventSpeechEnded}); err != nil {
				return err
			}
			data, err := json.Marshal(map[string]any{"text": "mock final", "frames": frames})
			if err != nil {
				return err
			}
			return stream.Emit(runtimepkg.ProviderEvent{Type: protocol.EventTranscriptFinal, Data: data})
		}
		stream.CancelHook = func(_ context.Context) error {
			return stream.Emit(runtimepkg.ProviderEvent{Type: protocol.EventResponseCanceled})
		}
		return stream
	})
}

// NewTTSAdapter creates an adapter that emits one deterministic synthetic
// audio frame per committed text utterance. The bytes are test fixtures, not
// playable audio.
func NewTTSAdapter(id string) *Adapter {
	return NewAdapter(id, func(_ runtimepkg.AdapterRequest) *Stream {
		stream := NewStream(32)
		var parts []string
		stream.AppendTextHook = func(_ context.Context, text string) error {
			parts = append(parts, text)
			return nil
		}
		stream.CommitTextHook = func(_ context.Context) error {
			if err := stream.Emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted}); err != nil {
				return err
			}
			audio := []byte("mock-audio:" + strings.Join(parts, ""))
			if err := stream.Emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Audio: audio}); err != nil {
				return err
			}
			parts = nil
			return stream.Emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone})
		}
		stream.CancelHook = func(_ context.Context) error {
			return stream.Emit(runtimepkg.ProviderEvent{Type: protocol.EventResponseCanceled})
		}
		return stream
	})
}
