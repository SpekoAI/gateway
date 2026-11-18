package geminirealtime

import (
	"io"
	"sync"
	"time"
)

// physicalSocket observes the actual upgraded transport, independently of the
// SDK's close-error selection. Its Engine-derived cap remains armed even when
// setup has finished but an outer adapter has not returned the stream yet.
type physicalSocket struct {
	io.ReadWriteCloser
	closeOnce sync.Once
	mu        sync.Mutex
	closeErr  error
	timer     *time.Timer
	expired   bool
	onExpiry  func()
}

func newPhysicalSocket(body io.ReadWriteCloser, deadline time.Time) *physicalSocket {
	s := &physicalSocket{ReadWriteCloser: body}
	if !deadline.IsZero() {
		s.mu.Lock()
		s.timer = time.AfterFunc(max(time.Until(deadline), 0), s.expire)
		s.mu.Unlock()
	}
	return s
}

func (s *physicalSocket) expire() {
	s.mu.Lock()
	s.expired = true
	callback := s.onExpiry
	s.mu.Unlock()
	if callback != nil {
		callback()
	} else {
		_ = s.Close() // Dial may still be validating the upgrade response.
	}
}

func (s *physicalSocket) bindExpiry(callback func()) {
	s.mu.Lock()
	s.onExpiry = callback
	expired := s.expired
	s.mu.Unlock()
	if expired {
		callback()
	}
}

func (s *physicalSocket) Close() error {
	s.closeOnce.Do(func() {
		err := s.ReadWriteCloser.Close()
		s.mu.Lock()
		s.closeErr = err
		timer := s.timer
		s.mu.Unlock()
		if err == nil && timer != nil {
			timer.Stop()
		}
	})
	return s.closeError()
}

func (s *physicalSocket) closeError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeErr
}
