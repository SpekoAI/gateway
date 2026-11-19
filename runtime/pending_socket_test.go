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
)

func TestConfiguredGoogleAdapterRefusesUnclockedFunding(t *testing.T) {
	for _, skew := range []time.Duration{-10 * time.Second, 0, 10 * time.Second} {
		t.Run(skew.String(), func(t *testing.T) {
			t.Parallel()
			p, _, client := newContinuityPeer(t, 3*time.Second, skew, true)
			p.plan.Route.Adapter = "google.custom"
			p.plan.Route.Credential.ExpiresAt = p.plan.Reservation.LeaseExpiresAt.Add(-2 * time.Second)
			nonrenewableGooglePlan(p)
			google, err := geminirealtime.New(geminirealtime.Config{AdapterID: p.plan.Route.Adapter, HTTPClient: p.server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
			if err != nil {
				t.Fatal(err)
			}
			engine, err := runtimepkg.New(runtimepkg.Config{Adapters: []runtimepkg.Adapter{google}, Verifier: p.verifier, Now: func() time.Time { return time.Now().Add(skew) }, Telemetry: &p.telemetry})
			if err != nil {
				t.Fatal(err)
			}
			session, err := engine.Open(t.Context(), continuityRequest(p, client))
			if session != nil {
				session.Abort()
			}
			if session != nil || !errors.Is(err, runtimepkg.ErrFundedGoogleClockAuthority) {
				t.Fatalf("configured Google missed authenticated pre-I/O refusal: %v", err)
			}
			assertAdmissionFailure(t, p, false)
			p.mu.Lock()
			defer p.mu.Unlock()
			if p.clockCalls.Load() != 0 || p.sockets != 0 || p.mints != 0 {
				t.Fatal("refused configured Google performed funded/provider I/O")
			}
		})
	}
}

// An arbitrary adapter cannot gain funded Google acceptance by choosing an ID
// that looks like a configured alias. Explicit supported adapters retain the
// existing SQL clock, original credential cap and original receipt identity.
type unmarkedGoogleAlias struct{ runtimepkg.Adapter }

func TestConfiguredGoogleAdmissionRequiresInstalledGoogleCapability(t *testing.T) {
	for _, supported := range []bool{false, true} {
		name := "arbitrary_adapter_refused"
		if supported {
			name = "configured_google_capped"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p, _, client := newContinuityPeer(t, 3*time.Second, -10*time.Second, true)
			p.plan.Route.Adapter = "google.custom"
			p.plan.Route.Credential.ExpiresAt = p.plan.Reservation.LeaseExpiresAt.Add(-2 * time.Second)
			p.envelope.Plan = p.plan.Unsigned()
			p.plan = resignPlan(t, p.private, "continuity", p.envelope, p.plan)
			closed := make(chan struct{})
			hc := *p.server.Client()
			transport := hc.Transport
			hc.Transport = observerTransport(func(r *http.Request) (*http.Response, error) {
				response, err := transport.RoundTrip(r)
				if err == nil && response.StatusCode == 101 {
					response.Body = &socketObserver{ReadWriteCloser: response.Body.(io.ReadWriteCloser), closeSent: make(chan struct{}), physicalClosed: closed}
				}
				return response, err
			})
			google, err := geminirealtime.New(geminirealtime.Config{AdapterID: p.plan.Route.Adapter, HTTPClient: &hc, AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
			if err != nil {
				t.Fatal(err)
			}
			var adapter runtimepkg.Adapter = google
			if !supported {
				adapter = unmarkedGoogleAlias{google}
			}
			engine, err := runtimepkg.New(runtimepkg.Config{Adapters: []runtimepkg.Adapter{adapter}, Verifier: p.verifier, Now: func() time.Time { return time.Now().Add(-10 * time.Second) }, Telemetry: &p.telemetry})
			if err != nil {
				t.Fatal(err)
			}
			session, err := engine.Open(t.Context(), continuityRequest(p, client))
			if !supported {
				if session != nil {
					session.Abort()
				}
				if !errors.Is(err, runtimepkg.ErrFundedGoogleAdapter) {
					t.Fatalf("arbitrary alias received funded acceptance: %v", err)
				}
				assertAdmissionFailure(t, p, false)
				if p.clockCalls.Load() != 0 {
					t.Fatal("unrecognized adapter performed clock I/O")
				}
				p.mu.Lock()
				defer p.mu.Unlock()
				if p.sockets != 0 || p.mints != 0 {
					t.Fatal("unrecognized adapter reached funded/provider I/O")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(session.Abort)
			_ = awaitContinuity(t, session.Events())
			if p.clockCalls.Load() != 1 {
				t.Fatal("supported alias did not use SQL admission")
			}
			if err := session.SubmitAudio(runtimepkg.AudioInput{Data: []byte{1, 0}}); err != nil {
				t.Fatal(err)
			}
			_ = awaitContinuity(t, p.inputs)
			timer := time.NewTimer(max(time.Until(p.plan.Route.Credential.ExpiresAt.Add(200*time.Millisecond)), 0))
			defer timer.Stop()
			select {
			case <-closed:
			case <-timer.C:
				t.Fatal("configured Google passed its original credential cap")
			}
			_ = awaitContinuity(t, session.Done())
			if !errors.Is(session.Err(), runtimepkg.ErrSessionLeaseExpired) {
				t.Fatalf("configured cap outcome changed: %v", session.Err())
			}
			assertOwnedTerminalOutcome(t, p, session, protocol.EventError, "session.failed")
		})
	}
}

func TestGoogleCanceledSetupPublishesFailureBeforePhysicalJoins(t *testing.T) {
	p, _, client := newContinuityPeer(t, 3*time.Second, 0, true)
	p.holdSetup = true
	body := &failedThenHeldClose{disconnected: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(body.release) }) }
	defer release()
	hc := *p.server.Client()
	transport := hc.Transport
	hc.Transport = observerTransport(func(r *http.Request) (*http.Response, error) {
		response, err := transport.RoundTrip(r)
		if err == nil && response.StatusCode == 101 {
			body.ReadWriteCloser = response.Body.(io.ReadWriteCloser)
			response.Body = body
		}
		return response, err
	})
	google, err := geminirealtime.New(geminirealtime.Config{HTTPClient: &hc, AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := runtimepkg.New(runtimepkg.Config{Adapters: []runtimepkg.Adapter{google}, Verifier: p.verifier, Telemetry: &p.telemetry})
	if err != nil {
		t.Fatal(err)
	}
	var capacity atomic.Int32
	released := make(chan struct{}, 1)
	r := continuityRequest(p, client)
	r.TrackVerifiedCapacity = func(*protocol.CapacityAttribution, protocol.CredentialSource) (func(), error) {
		capacity.Add(1)
		return func() { capacity.Add(-1); released <- struct{}{} }, nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		s, err := engine.Open(ctx, r)
		if s != nil {
			s.Abort()
		}
		result <- err
	}()
	_ = awaitContinuity(t, p.setups)
	cancel()
	_ = awaitContinuity(t, body.disconnected)
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("setup cancellation lost original error: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Open/failure evidence waited for the held successful transport Close return")
	}
	assertAdmissionFailure(t, p, true)
	if body.attempts.Load() < 2 || capacity.Load() != 1 {
		t.Fatal("setup failure lost physical retry/join ownership")
	}
	select {
	case <-released:
		t.Fatal("setup failure released capacity before all attempts joined")
	case <-time.After(50 * time.Millisecond):
	}
	release()
	_ = awaitContinuity(t, released)
	if capacity.Load() != 0 {
		t.Fatal("joined setup failure retained capacity")
	}
}

// ReviewWaitNormal is used only by the external scheduling overlay. It makes
// the interrupt-fails-then-normal-close-blocks ordering deterministic without
// changing any transport result, deadline, or production policy.
type pendingSetupClose struct {
	io.ReadWriteCloser
	calls                                atomic.Int32
	first, second, release, disconnected chan struct{}
	once                                 sync.Once
	failBefore                           time.Time
}

func (c *pendingSetupClose) ReviewWaitNormal()    { <-c.first }
func (c *pendingSetupClose) ReviewWaitInterrupt() { <-c.first }
func (c *pendingSetupClose) Close() error {
	call := c.calls.Add(1)
	if !c.failBefore.IsZero() {
		if call == 1 {
			close(c.first)
		}
		if call == 2 {
			close(c.second)
		}
		if time.Now().Before(c.failBefore) {
			return errors.New("pending replacement transport is still live after failed close")
		}
		err := c.ReadWriteCloser.Close()
		c.once.Do(func() { close(c.disconnected) })
		<-c.release // even actual cap-driven closure must join its return
		return err
	}
	switch call {
	case 1:
		close(c.first)
		return errors.New("pending replacement first close failed without disconnecting")
	case 2:
		close(c.second)
		<-c.release
	}
	err := c.ReadWriteCloser.Close()
	c.once.Do(func() { close(c.disconnected) })
	return err
}

func TestAdoptedReplacementSurvivesOriginalSocketExpiry(t *testing.T) {
	p, engine, client := newContinuityPeer(t, time.Second, 0)
	session := openContinuity(t, p, engine, client)
	_ = awaitContinuity(t, p.connected)
	_ = awaitContinuity(t, p.connected) // actual replacement setup completed
	if token := awaitContinuity(t, p.closed); token != "root-token" {
		t.Fatal("replacement detached instead of its predecessor")
	}
	timer := time.NewTimer(max(time.Until(p.plan.Route.Credential.ExpiresAt.Add(100*time.Millisecond)), 0))
	defer timer.Stop()
	<-timer.C
	if err := session.SubmitAudio(runtimepkg.AudioInput{Data: []byte{1, 0}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.inputs: // canonical adoption, after the old socket's original cap
	case <-session.Done():
		t.Fatalf("old physical callback killed its adopted replacement: %v", session.Err())
	case <-time.After(300 * time.Millisecond):
		t.Fatal("adopted replacement lost I/O at predecessor expiry")
	}
	session.Abort()
	_ = awaitContinuity(t, session.Done())
	assertOwnedTerminalOutcome(t, p, session, protocol.EventError, "session.failed")
}

func TestPendingReplacementOwnsRenewedPhysicalCap(t *testing.T) {
	for _, order := range []string{"timer_before_join", "join_before_timer", "cap_retry"} {
		t.Run(order, func(t *testing.T) {
			t.Parallel()
			p, _, client := newContinuityPeer(t, time.Second, 0)
			p.renewalRelease = make(chan struct{})
			body := &pendingSetupClose{first: make(chan struct{}), second: make(chan struct{}), release: make(chan struct{}), disconnected: make(chan struct{})}
			if order == "cap_retry" {
				body.failBefore = p.plan.Reservation.Billing.RenewableUntil.Add(-100 * time.Millisecond)
			}
			var once sync.Once
			release := func() { once.Do(func() { close(body.release) }) }
			defer release()
			hc := *p.server.Client()
			transport := hc.Transport
			hc.Transport = observerTransport(func(r *http.Request) (*http.Response, error) {
				response, err := transport.RoundTrip(r)
				if err == nil && response.StatusCode == 101 && r.URL.Query().Get("access_token") == "renewed-token" {
					body.ReadWriteCloser = response.Body.(io.ReadWriteCloser)
					response.Body = body
				}
				return response, err
			})
			google, err := geminirealtime.New(geminirealtime.Config{HTTPClient: &hc, AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
			if err != nil {
				t.Fatal(err)
			}
			engine, err := runtimepkg.New(runtimepkg.Config{Adapters: []runtimepkg.Adapter{ownedContinuationAdapter{google}}, Verifier: p.verifier, Telemetry: &p.telemetry})
			if err != nil {
				t.Fatal(err)
			}
			var capacity atomic.Int32
			released := make(chan struct{}, 1)
			r := continuityRequest(p, client)
			r.TrackVerifiedCapacity = func(*protocol.CapacityAttribution, protocol.CredentialSource) (func(), error) {
				capacity.Add(1)
				return func() { capacity.Add(-1); released <- struct{}{} }, nil
			}
			session, err := engine.Open(t.Context(), r)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(session.Abort)
			_ = awaitContinuity(t, session.Events())
			_ = awaitContinuity(t, p.connected)
			_ = awaitContinuity(t, p.renewalStarted)
			p.holdSetup = true
			close(p.renewalRelease)
			_ = awaitContinuity(t, p.connected)
			session.Abort() // pending socket is not installed; terminal events stay unread
			_ = awaitContinuity(t, session.Done())
			_ = awaitContinuity(t, body.first)
			_ = awaitContinuity(t, body.second)
			if capacity.Load() != 1 {
				t.Fatal("pending replacement released capacity without teardown proof")
			}
			if order == "join_before_timer" {
				release()
			}
			timer := time.NewTimer(max(time.Until(p.plan.Reservation.Billing.RenewableUntil.Add(300*time.Millisecond)), 0))
			defer timer.Stop()
			select {
			case <-body.disconnected:
			case <-timer.C:
				t.Fatalf("unadopted replacement remained physically open beyond its original renewed cap; attempts=%d", body.calls.Load())
			}
			if order != "join_before_timer" {
				if capacity.Load() != 1 {
					t.Fatal("expiry/disconnection released capacity before the held return joined")
				}
				select {
				case <-released:
					t.Fatal("unjoined pending transport released capacity")
				case <-time.After(50 * time.Millisecond):
				}
				release()
			}
			_ = awaitContinuity(t, released)
			if capacity.Load() != 0 || !errors.Is(session.Err(), runtimepkg.ErrSessionAborted) {
				t.Fatalf("joined pending socket changed terminal/capacity: %v/%d", session.Err(), capacity.Load())
			}
			assertOwnedTerminalOutcome(t, p, session, protocol.EventError, "session.failed")
		})
	}
}
