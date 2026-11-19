package geminirealtime

import (
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

// physicalSocket observes the actual upgraded transport, independently of the
// SDK's close-error selection. Its Engine-derived cap remains armed even when
// setup has finished but an outer adapter has not returned the stream yet.
type physicalSocket struct {
	io.ReadWriteCloser
	mu                      sync.Mutex
	closeErr                error
	timer                   *time.Timer
	deadline                time.Time
	expired, closed, joined bool
	onExpiry                func()
	normal, interrupt       *physicalCloseAttempt
	active                  int
	retryInterrupt          bool
	stopped                 chan struct{}
}

type physicalCloseAttempt struct {
	done     chan struct{}
	err      error
	complete bool // protected by physicalSocket.mu
}

func newPhysicalSocket(body io.ReadWriteCloser, deadline time.Time) *physicalSocket {
	s := &physicalSocket{ReadWriteCloser: body, stopped: make(chan struct{})}
	s.tighten(deadline)
	return s
}

func (s *physicalSocket) tighten(deadline time.Time) {
	if deadline.IsZero() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || (!s.deadline.IsZero() && !deadline.Before(s.deadline)) {
		return
	}
	s.deadline = deadline
	if s.timer != nil {
		s.timer.Stop()
	}
	// An already-running old callback still addresses only this socket. A
	// shortened deadline cannot make that callback premature or renew authority.
	s.timer = time.AfterFunc(max(time.Until(deadline), 0), s.expire)
}

func (s *physicalSocket) expire() {
	s.mu.Lock()
	s.expired = true
	callback := s.onExpiry
	s.mu.Unlock()
	if callback != nil {
		callback()
	} else {
		s.Interrupt() // Dial may still be validating the upgrade response.
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
	attempt := s.startClose(false)
	if attempt == nil {
		return s.closeError()
	}
	<-attempt.done // serialized SDK teardown may join; interruption never does
	return attempt.err
}

// Interrupt never waits for a transport Close, including one already in
// progress. At most two physical attempts run concurrently. An unused or
// completed failed normal slot stays available when an interrupt is held.
// Repeated interruptions retain one retry if the current attempt later fails.
func (s *physicalSocket) Interrupt() { s.startClose(true) }

func (s *physicalSocket) startClose(interrupt bool) *physicalCloseAttempt {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	slot := &s.normal
	if interrupt {
		slot = &s.interrupt
		if *slot != nil && !(*slot).complete && (s.normal == nil || s.normal.complete) {
			// The SDK may not have called normal Close yet (invalid upgrade
			// diagnostics), or that attempt failed without closure evidence.
			// Expiry can use either free slot while the first interrupt is held.
			slot = &s.normal
		}
	}
	if *slot != nil && !(*slot).complete {
		attempt := *slot
		if interrupt {
			s.retryInterrupt = true
		}
		s.mu.Unlock()
		return attempt
	}
	attempt := &physicalCloseAttempt{done: make(chan struct{})}
	*slot = attempt
	s.active++
	s.mu.Unlock()
	go func() {
		err := s.ReadWriteCloser.Close()
		closed := err == nil || errors.Is(err, net.ErrClosed)
		s.mu.Lock()
		attempt.err, attempt.complete = err, true
		s.active--
		if s.closeErr == nil && err != nil {
			s.closeErr = err // diagnostic evidence survives a later successful retry
		}
		if closed {
			s.closed = true // actual transport success/already-closed evidence
			if s.timer != nil {
				s.timer.Stop()
			}
		}
		// A queued interrupt belongs to the socket, not the occupied slot.
		// Either failed attempt can free capacity while the other return is
		// held, including normal Close completing after physical expiry.
		retry := s.retryInterrupt && !s.closed
		if retry {
			s.retryInterrupt = false
		}
		if s.closed && s.active == 0 && !s.joined {
			s.joined = true
			close(s.stopped)
		}
		close(attempt.done)
		s.mu.Unlock()
		if retry {
			s.Interrupt()
		}
	}()
	return attempt
}

func (s *physicalSocket) waitStopped() { <-s.stopped }

func (s *physicalSocket) closeError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeErr
}
