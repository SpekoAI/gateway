package runtime_test

import (
	"context"
	"errors"
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

type heldPhysicalClose struct {
	io.ReadWriteCloser
	entered, release chan struct{}
	once             sync.Once
}

func (s *heldPhysicalClose) Close() error {
	err := s.ReadWriteCloser.Close() // the actual TLS socket really closes
	s.once.Do(func() { close(s.entered) })
	<-s.release // hold the transport's return, not the outer adapter's return
	return err
}

func TestGoogleTerminalEvidenceDoesNotWaitForHeldTransportClose(t *testing.T) {
	for _, mode := range []string{"abort", "lease"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			p, _, client := newContinuityPeer(t, time.Second, 0, true)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			releaseClose := func() { once.Do(func() { close(release) }) }
			defer releaseClose()
			httpClient := *p.server.Client()
			transport := httpClient.Transport
			httpClient.Transport = observerTransport(func(r *http.Request) (*http.Response, error) {
				response, err := transport.RoundTrip(r)
				if err == nil && response.StatusCode == 101 {
					response.Body = &heldPhysicalClose{ReadWriteCloser: response.Body.(io.ReadWriteCloser), entered: entered, release: release}
				}
				return response, err
			})
			google, err := geminirealtime.New(geminirealtime.Config{HTTPClient: &httpClient, AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
			if err != nil {
				t.Fatal(err)
			}
			engine, err := runtimepkg.New(runtimepkg.Config{Adapters: []runtimepkg.Adapter{google}, Verifier: p.verifier, Telemetry: &p.telemetry})
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
			_ = awaitContinuity(t, entered)
			want := runtimepkg.ErrSessionLeaseExpired
			deadline := p.plan.Reservation.LeaseExpiresAt.Add(300 * time.Millisecond)
			if mode == "abort" {
				want = runtimepkg.ErrSessionAborted
				deadline = time.Now().Add(300 * time.Millisecond)
				returned := make(chan struct{})
				go func() { session.Abort(); close(returned) }()
				select {
				case <-returned:
				case <-time.After(300 * time.Millisecond):
					t.Fatal("Abort waited for the held physical Close return")
				}
			}
			timer := time.NewTimer(max(time.Until(deadline), 0))
			defer timer.Stop()
			select {
			case <-session.Done():
			case <-timer.C:
				t.Fatal("terminal evidence waited for the held physical Close return")
			}
			if !errors.Is(session.Err(), want) || capacity.Load() != 1 {
				t.Fatalf("outcome/capacity changed before join: %v/%d", session.Err(), capacity.Load())
			}
			assertOwnedTerminalOutcome(t, p, session, protocol.EventError, "session.failed")
			releaseClose()
			_ = awaitContinuity(t, released)
			if capacity.Load() != 0 || !errors.Is(session.Err(), want) {
				t.Fatal("joined transport changed outcome or retained capacity")
			}
		})
	}
}

type failedThenHeldClose struct {
	io.ReadWriteCloser
	attempts              atomic.Int32
	disconnected, release chan struct{}
	once                  sync.Once
}

func (s *failedThenHeldClose) Close() error {
	if s.attempts.Add(1) == 1 {
		return errors.New("owned first close failed without disconnecting")
	}
	err := s.ReadWriteCloser.Close()
	s.once.Do(func() { close(s.disconnected) })
	<-s.release
	return err
}

func TestGoogleFailedPhysicalCloseRetriesButRetainsUnjoinedCapacity(t *testing.T) {
	p, _, client := newContinuityPeer(t, 2*time.Second, 0, true)
	body := &failedThenHeldClose{disconnected: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	releaseClose := func() { releaseOnce.Do(func() { close(body.release) }) }
	defer releaseClose()
	httpClient := *p.server.Client()
	transport := httpClient.Transport
	httpClient.Transport = observerTransport(func(r *http.Request) (*http.Response, error) {
		response, err := transport.RoundTrip(r)
		if err == nil && response.StatusCode == 101 {
			body.ReadWriteCloser = response.Body.(io.ReadWriteCloser)
			response.Body = body
		}
		return response, err
	})
	google, err := geminirealtime.New(geminirealtime.Config{HTTPClient: &httpClient, AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := runtimepkg.New(runtimepkg.Config{Adapters: []runtimepkg.Adapter{google}, Verifier: p.verifier, Telemetry: &p.telemetry})
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
	conn := awaitContinuity(t, p.connected)
	if err := conn.Write(t.Context(), websocket.MessageText, []byte(`{"error":{"code":500}}`)); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(max(time.Until(p.plan.Reservation.LeaseExpiresAt.Add(300*time.Millisecond)), 0))
	defer deadline.Stop()
	select {
	case <-body.disconnected:
	case <-deadline.C:
		t.Fatal("first failed close latched later real interruption")
	}
	select {
	case <-session.Done():
	case <-time.After(300 * time.Millisecond):
		t.Fatal("retry return blocked terminal metadata")
	}
	if session.Err() == nil || body.attempts.Load() < 2 || capacity.Load() != 1 {
		t.Fatalf("retry lost failure/join evidence: %v/%d/%d", session.Err(), body.attempts.Load(), capacity.Load())
	}
	select {
	case <-released:
		t.Fatal("disconnection without joined Close returns released capacity")
	case <-time.After(50 * time.Millisecond):
	}
	assertOwnedTerminalOutcome(t, p, session, protocol.EventError, "session.failed")
	releaseClose()
	_ = awaitContinuity(t, released)
	if capacity.Load() != 0 {
		t.Fatal("joined retried close retained capacity")
	}
}

func nonrenewableGooglePlan(p *continuityPeer) {
	p.plan.Reservation.Billing.RenewalURL = ""
	p.plan.Reservation.Billing.RenewableUntil = time.Time{}
	p.envelope.Plan = p.plan.Unsigned()
	p.plan = resignPlan(p.t, p.private, "continuity", p.envelope, p.plan)
}

func TestNonrenewableFundedGoogleRefusesWithoutSignedClockAuthority(t *testing.T) {
	for _, skew := range []time.Duration{-10 * time.Second, 0, 10 * time.Second} {
		t.Run(skew.String(), func(t *testing.T) {
			t.Parallel()
			p, engine, client := newContinuityPeer(t, 3*time.Second, skew, true)
			p.plan.Route.Credential.ExpiresAt = p.plan.Reservation.LeaseExpiresAt.Add(-2 * time.Second)
			nonrenewableGooglePlan(p)
			session, err := engine.Open(t.Context(), continuityRequest(p, client))
			if session != nil {
				session.Abort()
			}
			if !errors.Is(err, runtimepkg.ErrFundedGoogleClockAuthority) || session != nil {
				t.Fatalf("unsupported funding did not receive precise authenticated refusal: %v", err)
			}
			assertAdmissionFailure(t, p, false)
			p.mu.Lock()
			defer p.mu.Unlock()
			if p.calls != 0 || p.mints != 0 || p.sockets != 0 || p.clockCalls.Load() != 0 {
				t.Fatal("unsupported funded shape performed clock/funding/provider I/O")
			}
		})
	}
}

func TestNonrenewableFundedGoogleRequiresClockAuthorityBeforeIO(t *testing.T) {
	for _, mode := range []string{"missing_client", "missing_clocked_verifier"} {
		t.Run(mode, func(t *testing.T) {
			p, engine, client := newContinuityPeer(t, 3*time.Second, 0, true)
			nonrenewableGooglePlan(p)
			request := continuityRequest(p, client)
			if mode == "missing_client" {
				request.Entitlements = nil
			} else {
				var err error
				engine, err = runtimepkg.New(runtimepkg.Config{Adapters: []runtimepkg.Adapter{p.adapter}, Verifier: runtimepkg.PlanVerifierFunc(func(context.Context, protocol.SessionPlan) error { return nil })})
				if err != nil {
					t.Fatal(err)
				}
			}
			session, err := engine.Open(t.Context(), request)
			if session != nil {
				session.Abort()
			}
			if err == nil {
				t.Fatal("nonrenewable funding silently used ordinary admission")
			}
			p.mu.Lock()
			defer p.mu.Unlock()
			if p.sockets != 0 || p.mints != 0 {
				t.Fatal("missing trusted authority reached provider I/O")
			}
		})
	}
}

func TestNonrenewableFundedGoogleRefusalPreservesOriginalAllowance(t *testing.T) {
	p, engine, client := newContinuityPeer(t, 3*time.Second, 0, true)
	p.plan.Reservation.Usage.AuthorizedUnits = 1
	nonrenewableGooglePlan(p)
	session, err := engine.Open(t.Context(), continuityRequest(p, client))
	if session != nil {
		session.Abort()
	}
	if !errors.Is(err, runtimepkg.ErrFundedGoogleClockAuthority) || p.plan.Reservation.Usage.AuthorizedUnits != 1 {
		t.Fatalf("refusal changed original allowance/authority: %v", err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sockets != 0 || p.mints != 0 {
		t.Fatal("refused root spent provider allowance")
	}
}
