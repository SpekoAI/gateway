package geminirealtime

import (
	"context"
	"sync"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

// resumableStream retains one bounded event source and the original setup
// options. A renewal replaces only the credential and private provider handle.
// It never replays audio or resets conversation/output accounting.
type resumableStream struct {
	adapter           *Adapter
	request           runtimepkg.AdapterRequest
	mu                sync.Mutex
	current, detached *liveStream
	bridgeDone        chan struct{}
	closed            bool
	eventsMu          sync.Mutex
	events            chan runtimepkg.ProviderEvent
	eventsClosed      bool
	terminal          error
}

func newResumableStream(a *Adapter, request runtimepkg.AdapterRequest, stream *liveStream) *resumableStream {
	s := &resumableStream{adapter: a, request: request, current: stream, events: make(chan runtimepkg.ProviderEvent, a.eventBuffer), bridgeDone: make(chan struct{})}
	go s.bridge(stream, s.bridgeDone)
	return s
}

func (s *resumableStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }
func (s *resumableStream) TerminalError() error {
	s.eventsMu.Lock()
	defer s.eventsMu.Unlock()
	return s.terminal
}
func (s *resumableStream) end(err error) {
	s.eventsMu.Lock()
	defer s.eventsMu.Unlock()
	if s.eventsClosed {
		return
	}
	s.terminal, s.eventsClosed = err, true
	close(s.events)
}
func (s *resumableStream) bridge(stream *liveStream, done chan struct{}) {
	defer close(done)
	for event := range stream.Events() {
		s.mu.Lock()
		active := s.current == stream && !s.closed
		s.mu.Unlock()
		if !active && event.Err != nil {
			continue
		}
		s.eventsMu.Lock()
		overflow := false
		if !s.eventsClosed {
			select {
			case s.events <- event:
			default:
				overflow = true
			}
		}
		s.eventsMu.Unlock()
		if overflow {
			s.end(runtimepkg.ErrOutputBackpressure)
			_ = stream.abort()
			for range stream.Events() {
			}
			return
		}
	}
	s.mu.Lock()
	active := s.current == stream && !s.closed
	s.mu.Unlock()
	if active {
		s.end(stream.TerminalError())
	}
}
func (s *resumableStream) handle() string {
	stream := s.current
	if stream == nil {
		stream = s.detached
	}
	if stream == nil {
		return ""
	}
	stream.resumptionMu.Lock()
	defer stream.resumptionMu.Unlock()
	return stream.resumptionHandle
}
func (s *resumableStream) CanResume() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Google's current managed token pins sessionResumption to {}. Removing
	// that mask permits resumptions outside uses:1, without an accepted bound.
	return s.request.Plan.Execution.CredentialSource == protocol.CredentialsBYOK && !s.closed && s.handle() != ""
}
func (s *resumableStream) Detach() {
	s.mu.Lock()
	stream := s.current
	if stream != nil {
		s.detached, s.current = stream, nil
	}
	s.mu.Unlock()
	if stream != nil {
		_ = stream.abort()
	}
}
func (s *resumableStream) Resume(ctx context.Context, credential protocol.DelegatedCredential) error {
	if s.request.Plan.Execution.CredentialSource != protocol.CredentialsBYOK {
		return &runtimepkg.ProviderError{Code: "unsupported_operation", Message: "Managed Google resumption has no accepted provider-enforced bound"}
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return runtimepkg.ErrSessionClosed
	}
	handle := s.handle()
	previous := s.current
	if previous == nil {
		previous = s.detached
	}
	done := s.bridgeDone
	s.mu.Unlock()
	if handle == "" {
		return runtimepkg.ErrResumptionUnavailable
	}
	s.Detach()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	request := s.request
	request.Plan.Route.Credential = &credential
	stream, err := s.adapter.openSocket(ctx, request, handle, previous.responding)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed || ctx.Err() != nil {
		s.mu.Unlock()
		_ = stream.abort()
		return runtimepkg.ErrSessionClosed
	}
	s.current, s.detached, s.bridgeDone = stream, nil, make(chan struct{})
	go s.bridge(stream, s.bridgeDone)
	s.mu.Unlock()
	return nil
}
func (s *resumableStream) socket() (*liveStream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.current == nil {
		return nil, runtimepkg.ErrSessionClosed
	}
	return s.current, nil
}
func (s *resumableStream) WriteAudio(ctx context.Context, audio []byte) error {
	stream, err := s.socket()
	if err != nil {
		return err
	}
	return stream.WriteAudio(ctx, audio)
}
func (s *resumableStream) CommitAudio(ctx context.Context) error {
	stream, err := s.socket()
	if err != nil {
		return err
	}
	return stream.CommitAudio(ctx)
}
func (s *resumableStream) AppendText(context.Context, string) error {
	return runtimepkg.ErrUnsupportedOperation
}
func (s *resumableStream) CommitText(context.Context) error {
	return runtimepkg.ErrUnsupportedOperation
}
func (s *resumableStream) Cancel(ctx context.Context) error {
	stream, err := s.socket()
	if err != nil {
		return err
	}
	return stream.Cancel(ctx)
}
func (s *resumableStream) Close(ctx context.Context) error { return s.Abort(ctx) }
func (s *resumableStream) Abort(context.Context) error {
	s.mu.Lock()
	s.closed = true
	stream := s.current
	done := s.bridgeDone
	s.current = nil
	s.mu.Unlock()
	if stream != nil {
		_ = stream.abort()
	}
	<-done
	s.end(nil)
	return nil
}
