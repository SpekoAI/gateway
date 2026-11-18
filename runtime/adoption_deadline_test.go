package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	"github.com/SpekoAI/gateway/providers/geminirealtime"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

func TestGoogleConcurrentPeerClosePreservesNormalStatuses(t *testing.T) {
	for _, status := range []websocket.StatusCode{websocket.StatusNormalClosure, websocket.StatusGoingAway, websocket.StatusPolicyViolation} {
		t.Run(fmt.Sprint(int(status)), func(t *testing.T) {
			t.Parallel()
			p, _, client := newContinuityPeer(t, 20*time.Second, 0, true)
			peerClose := make(chan struct{})
			var peerOnce sync.Once
			releasePeer := func() { peerOnce.Do(func() { close(peerClose) }) }
			defer releasePeer()
			p.holdClose, p.closeWithStatus = peerClose, status
			closeSent, physicalClosed := make(chan struct{}), make(chan struct{})
			httpClient := *p.server.Client()
			transport := httpClient.Transport
			httpClient.Transport = observerTransport(func(r *http.Request) (*http.Response, error) {
				response, err := transport.RoundTrip(r)
				if err == nil && response.StatusCode == 101 {
					response.Body = &socketObserver{ReadWriteCloser: response.Body.(io.ReadWriteCloser), closeSent: closeSent, physicalClosed: physicalClosed}
				}
				return response, err
			})
			adapter, err := geminirealtime.New(geminirealtime.Config{HTTPClient: &httpClient, AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
			if err != nil {
				t.Fatal(err)
			}
			returned, release := make(chan error, 1), make(chan struct{})
			var once sync.Once
			releaseReturn := func() { once.Do(func() { close(release) }) }
			defer releaseReturn()
			engine, err := runtimepkg.New(runtimepkg.Config{Adapters: []runtimepkg.Adapter{joinedCloseAdapter{Adapter: adapter, reached: returned, release: release}}, Verifier: p.verifier, Telemetry: &p.telemetry})
			if err != nil {
				t.Fatal(err)
			}
			var capacity atomic.Int32
			released := make(chan struct{}, 1)
			request := continuityRequest(p, client)
			request.TrackVerifiedCapacity = func(*protocol.CapacityAttribution, protocol.CredentialSource) (func(), error) {
				capacity.Add(1)
				return func() { capacity.Add(-1); released <- struct{}{} }, nil
			}
			session, err := engine.Open(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(session.Abort)
			_ = awaitContinuity(t, session.Events())
			session.Close()
			_ = awaitContinuity(t, closeSent)
			releasePeer() // send the actual peer status while local Close is pending
			closeErr := awaitContinuity(t, returned)
			_ = awaitContinuity(t, session.Done())
			_ = awaitContinuity(t, physicalClosed)
			if capacity.Load() != 1 {
				t.Fatal("close result hold released serialized teardown capacity")
			}
			normal := status != websocket.StatusPolicyViolation
			if normal && (closeErr != nil || session.Err() != nil) {
				t.Fatalf("normal peer status %d became failure: Close=%v session=%v", status, closeErr, session.Err())
			}
			if !normal && (closeErr == nil || session.Err() == nil) {
				t.Fatal("protocol failure became a normal close")
			}
			releaseReturn()
			_ = awaitContinuity(t, released)
			wantEvent, wantReceipt := protocol.EventError, "session.failed"
			if normal {
				wantEvent, wantReceipt = protocol.EventSessionClosed, "session.closed"
			}
			assertOwnedTerminalOutcome(t, p, session, wantEvent, wantReceipt)
		})
	}
}

func TestFundedSetupContextDoesNotShortenExecutionLease(t *testing.T) {
	p, engine, client := newContinuityPeer(t, time.Second, 0, true)
	setup, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	session, err := engine.Open(setup, continuityRequest(p, client))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(session.Abort)
	_ = awaitContinuity(t, session.Events())
	<-setup.Done()
	if err := session.SubmitAudio(runtimepkg.AudioInput{Data: []byte{1, 0}}); err != nil {
		t.Fatalf("setup timeout shortened funded execution: %v", err)
	}
	select {
	case <-p.inputs:
	case <-session.Done():
		t.Fatalf("setup timeout became execution authority: %v", session.Err())
	case <-time.After(300 * time.Millisecond):
		t.Fatal("live funded socket did not accept input after setup context ended")
	}
	_ = awaitContinuity(t, session.Done())
	_ = awaitContinuity(t, p.closed)
	if !errors.Is(session.Err(), runtimepkg.ErrSessionLeaseExpired) {
		t.Fatalf("original funded expiry changed: %v", session.Err())
	}
	assertOwnedTerminalOutcome(t, p, session, protocol.EventError, "session.failed")
}

type adoptionDeadlineAdapter struct {
	runtimepkg.Adapter
	peer                 *continuityPeer
	mode                 string
	resumeExpiry         chan time.Time
	resumeReturned       chan struct{}
	joinStarted, joinEnd chan struct{}
}

func (a *adoptionDeadlineAdapter) Open(ctx context.Context, r runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	stream, err := a.Adapter.Open(ctx, r)
	if err != nil {
		return stream, err
	}
	return &adoptionDeadlineStream{ResumableProviderStream: stream.(runtimepkg.ResumableProviderStream), owner: a}, nil
}

type adoptionDeadlineStream struct {
	runtimepkg.ResumableProviderStream
	owner *adoptionDeadlineAdapter
}

func (s *adoptionDeadlineStream) TerminalError() error {
	return s.ResumableProviderStream.(runtimepkg.TerminalErrorProviderStream).TerminalError()
}
func (s *adoptionDeadlineStream) Interrupt() {
	s.ResumableProviderStream.(runtimepkg.InterruptingProviderStream).Interrupt()
}
func (s *adoptionDeadlineStream) Abort(ctx context.Context) error {
	err := s.ResumableProviderStream.(runtimepkg.AbortingProviderStream).Abort(ctx)
	close(s.owner.joinStarted)
	<-s.owner.joinEnd
	return err
}
func (s *adoptionDeadlineStream) Resume(ctx context.Context, credential protocol.DelegatedCredential) error {
	// Establish the real owned WebSocket and setup acknowledgment FIRST. Only
	// the adapter's return is delayed; physical work exists during that delay.
	if err := s.ResumableProviderStream.Resume(ctx, credential); err != nil {
		return err
	}
	s.owner.resumeExpiry <- credential.ExpiresAt
	returnAt := credential.ExpiresAt.Add(-600 * time.Millisecond)
	if s.owner.mode == "late_return" {
		returnAt = credential.ExpiresAt.Add(600 * time.Millisecond)
	}
	timer := time.NewTimer(max(time.Until(returnAt), 0))
	defer timer.Stop()
	<-timer.C
	if s.owner.mode == "blocked_clock" {
		s.owner.peer.wallOffset.Store(int64(-time.Minute))
	}
	close(s.owner.resumeReturned)
	return nil
}

func TestOwnedResumedSocketDeadlineSurvivesAdoption(t *testing.T) {
	for _, mode := range []string{"blocked_clock", "late_return", "adopted"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			p, _, client := newContinuityPeer(t, time.Second, 0)
			if mode == "blocked_clock" {
				p.clockRelease, p.clockStarted = make(chan struct{}), make(chan struct{}, 1)
				defer close(p.clockRelease)
			}
			physicalClosed := make(chan struct{})
			httpClient := *p.server.Client()
			transport := httpClient.Transport
			httpClient.Transport = observerTransport(func(r *http.Request) (*http.Response, error) {
				response, err := transport.RoundTrip(r)
				if err == nil && response.StatusCode == 101 && r.URL.Query().Get("access_token") == "renewed-token" {
					response.Body = &socketObserver{ReadWriteCloser: response.Body.(io.ReadWriteCloser), closeSent: make(chan struct{}), physicalClosed: physicalClosed}
				}
				return response, err
			})
			google, err := geminirealtime.New(geminirealtime.Config{HTTPClient: &httpClient, AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
			if err != nil {
				t.Fatal(err)
			}
			adapter := &adoptionDeadlineAdapter{Adapter: ownedContinuationAdapter{google}, peer: p, mode: mode, resumeExpiry: make(chan time.Time, 1), resumeReturned: make(chan struct{}), joinStarted: make(chan struct{}), joinEnd: make(chan struct{})}
			defer close(adapter.joinEnd)
			engine, err := runtimepkg.New(runtimepkg.Config{Adapters: []runtimepkg.Adapter{adapter}, Verifier: p.verifier, Now: func() time.Time { return time.Now().Add(time.Duration(p.wallOffset.Load())) }, Telemetry: &p.telemetry})
			if err != nil {
				t.Fatal(err)
			}
			var capacity atomic.Int32
			released := make(chan struct{}, 1)
			request := continuityRequest(p, client)
			request.TrackVerifiedCapacity = func(*protocol.CapacityAttribution, protocol.CredentialSource) (func(), error) {
				capacity.Add(1)
				return func() { capacity.Add(-1); released <- struct{}{} }, nil
			}
			session, err := engine.Open(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(session.Abort)
			_ = awaitContinuity(t, session.Events())
			expires := awaitContinuity(t, adapter.resumeExpiry)
			if !expires.Equal(p.plan.Reservation.Billing.RenewableUntil) {
				t.Fatal("resumed socket changed original absolute horizon")
			}
			if mode == "blocked_clock" {
				_ = awaitContinuity(t, p.clockStarted)
			}
			if mode == "adopted" {
				_ = awaitContinuity(t, adapter.resumeReturned)
				if err := session.SubmitAudio(runtimepkg.AudioInput{Data: []byte{1, 0}}); err != nil {
					t.Fatal(err)
				}
				_ = awaitContinuity(t, p.inputs) // successful canonical adoption
			}
			deadline := time.NewTimer(max(time.Until(expires.Add(300*time.Millisecond)), 0))
			defer deadline.Stop()
			select {
			case <-physicalClosed:
			case <-deadline.C:
				t.Fatal("resumed socket remained physically open 300ms beyond original expiry")
			}
			_ = awaitContinuity(t, session.Done()) // terminal Events stay unread
			if !errors.Is(session.Err(), runtimepkg.ErrSessionLeaseExpired) {
				t.Fatalf("expired resumed work became a clean/unknown outcome: %v", session.Err())
			}
			_ = awaitContinuity(t, adapter.resumeReturned)
			_ = awaitContinuity(t, adapter.joinStarted)
			if capacity.Load() != 1 {
				t.Fatal("unknown resumed teardown released capacity before actual join")
			}
			// Release only after proving the real serialized abort has finished
			// physical teardown; then preserve the earlier terminal outcome.
			adapter.joinEnd <- struct{}{}
			_ = awaitContinuity(t, released)
			if capacity.Load() != 0 {
				t.Fatal("joined resumed work retained physical capacity")
			}
			assertOwnedTerminalOutcome(t, p, session, protocol.EventError, "session.failed")
			p.mu.Lock()
			defer p.mu.Unlock()
			if p.mints != 1 || p.sockets != 2 {
				t.Fatalf("adoption forked mint/socket: %d/%d", p.mints, p.sockets)
			}
		})
	}
}

func assertOwnedTerminalOutcome(t *testing.T, p *continuityPeer, session *runtimepkg.Session, wantEvent protocol.EventType, wantReceipt string) {
	t.Helper()
	events := collectEvents(t, session)
	if len(events) != 1 || events[0].Type != wantEvent {
		t.Fatalf("terminal events = %+v, want %s", events, wantEvent)
	}
	count := 0
	for _, receipt := range p.telemetry.snapshot() {
		if receipt.Name != "session.failed" && receipt.Name != "session.closed" {
			continue
		}
		count++
		if receipt.Name != wantReceipt || receipt.AttemptID != p.plan.AttemptID || receipt.SessionID != p.plan.SessionID || receipt.Destination.Token != p.plan.Telemetry.Token {
			t.Fatalf("terminal receipt lost original authority/outcome: %+v", receipt)
		}
	}
	if count != 1 {
		t.Fatalf("terminal receipt count %d", count)
	}
}
