package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/controlplane"
	"github.com/SpekoAI/gateway/protocol"
	"github.com/SpekoAI/gateway/providers/geminirealtime"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

type pendingClockResumeAdapter struct {
	ownedContinuationAdapter
	resumed                  chan struct{}
	release                  <-chan struct{}
	tightened, tightenReturn chan struct{}
}

func (a pendingClockResumeAdapter) Open(ctx context.Context, r runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	s, err := a.ownedContinuationAdapter.Open(ctx, r)
	if err != nil {
		return s, err
	}
	return &pendingClockResumeStream{ResumableProviderStream: s.(runtimepkg.ResumableProviderStream), resumed: a.resumed, release: a.release, tightened: a.tightened, tightenReturn: a.tightenReturn}, nil
}

type pendingClockResumeStream struct {
	runtimepkg.ResumableProviderStream
	resumed                  chan struct{}
	release                  <-chan struct{}
	tightened, tightenReturn chan struct{}
	tightenOnce              sync.Once
}

func (s *pendingClockResumeStream) Resume(ctx context.Context, credential protocol.DelegatedCredential) error {
	if err := s.ResumableProviderStream.Resume(ctx, credential); err != nil {
		return err
	}
	if s.resumed != nil {
		close(s.resumed)
		<-s.release
	}
	return nil
}
func (s *pendingClockResumeStream) TightenPhysicalDeadline(deadline time.Time) {
	s.ResumableProviderStream.TightenPhysicalDeadline(deadline)
	if s.tightened != nil {
		s.tightenOnce.Do(func() { close(s.tightened) })
		<-s.tightenReturn
	}
}
func (s *pendingClockResumeStream) Interrupt() {
	s.ResumableProviderStream.(runtimepkg.InterruptingProviderStream).Interrupt()
}
func (s *pendingClockResumeStream) Abort(ctx context.Context) error {
	return s.ResumableProviderStream.(runtimepkg.AbortingProviderStream).Abort(ctx)
}
func (s *pendingClockResumeStream) TerminalError() error {
	return s.ResumableProviderStream.(runtimepkg.TerminalErrorProviderStream).TerminalError()
}

func TestPendingReplacementSQLRefreshRetainsPhysicalOwnership(t *testing.T) {
	for _, heldReturn := range []bool{false, true} {
		for _, forward := range []bool{false, true} {
			name := "backward_control"
			if forward {
				name = "forward_expired"
			}
			if heldReturn {
				name = "held_return_" + name
			} else {
				name = "pending_setup_" + name
			}
			t.Run(name, func(t *testing.T) { pendingReplacementSQLRefresh(t, forward, heldReturn) })
		}
	}
}

type pendingClockHolds struct{ shorter, tightenAndClose bool }

func TestPendingSQLPhysicalCapSurvivesHeldTightenAndClose(t *testing.T) {
	for _, heldReturn := range []bool{false, true} {
		for _, shorter := range []bool{false, true} {
			name := "pending_setup"
			if heldReturn {
				name = "held_resume_return"
			}
			if shorter {
				name += "/shorter"
			} else {
				name += "/expired"
			}
			t.Run(name, func(t *testing.T) {
				pendingReplacementSQLRefresh(t, true, heldReturn, pendingClockHolds{shorter: shorter, tightenAndClose: true})
			})
		}
	}
}

func pendingReplacementSQLRefresh(t *testing.T, forward, heldReturn bool, options ...pendingClockHolds) {
	var holds pendingClockHolds
	if len(options) != 0 {
		holds = options[0]
	}
	p, _, client := newContinuityPeer(t, 12*time.Second, 0)
	p.plan.Reservation.Billing.RenewableUntil = p.plan.Reservation.LeaseExpiresAt.Add(60 * time.Second)
	p.envelope.Plan = p.plan.Unsigned()
	p.plan = resignPlan(t, p.private, "continuity", p.envelope, p.plan)
	renewedExpiry := p.plan.Reservation.LeaseExpiresAt.Add(4 * time.Second)
	p.clockSamples = make(chan time.Time, 8)
	p.renewalRelease = make(chan struct{})
	p.renewalHandler = func(w http.ResponseWriter, r *http.Request) {
		p.renewalStarted <- struct{}{}
		select {
		case <-p.renewalRelease:
		case <-r.Context().Done():
			return
		}
		_ = json.NewEncoder(w).Encode(controlplane.EntitlementRenewal{EntitlementID: "owned-slice", SessionID: p.plan.SessionID, AttemptID: p.plan.AttemptID, ReservationID: p.plan.Reservation.ID, Sequence: 1, PreviousExpiresAt: p.plan.Reservation.LeaseExpiresAt, LeaseExpiresAt: renewedExpiry, AuthorizedUnits: 4, MaximumAmountMicros: 100, Currency: "USD", Credential: protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "renewed-token", ExpiresAt: renewedExpiry}})
	}
	closed := make(chan struct{})
	closeReturn, tightenReached, tightenReturn := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var closeOnce, tightenOnce sync.Once
	releaseClose := func() { closeOnce.Do(func() { close(closeReturn) }) }
	releaseTighten := func() { tightenOnce.Do(func() { close(tightenReturn) }) }
	defer releaseClose()
	defer releaseTighten()
	hc := *p.server.Client()
	transport := hc.Transport
	hc.Transport = observerTransport(func(r *http.Request) (*http.Response, error) {
		response, err := transport.RoundTrip(r)
		if err == nil && response.StatusCode == 101 && r.URL.Query().Get("access_token") == "renewed-token" {
			if holds.tightenAndClose {
				response.Body = &heldPhysicalClose{ReadWriteCloser: response.Body.(io.ReadWriteCloser), entered: closed, release: closeReturn}
			} else {
				response.Body = &socketObserver{ReadWriteCloser: response.Body.(io.ReadWriteCloser), closeSent: make(chan struct{}), physicalClosed: closed}
			}
		}
		return response, err
	})
	google, err := geminirealtime.New(geminirealtime.Config{HTTPClient: &hc, AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatal(err)
	}
	var adapter runtimepkg.Adapter = ownedContinuationAdapter{google}
	resumed, release := make(chan struct{}), make(chan struct{})
	if heldReturn || holds.tightenAndClose {
		a := pendingClockResumeAdapter{ownedContinuationAdapter: ownedContinuationAdapter{google}}
		if heldReturn {
			a.resumed, a.release = resumed, release
		}
		if holds.tightenAndClose {
			a.tightened, a.tightenReturn = tightenReached, tightenReturn
		}
		adapter = a
	}
	engine, err := runtimepkg.New(runtimepkg.Config{Adapters: []runtimepkg.Adapter{adapter}, Verifier: p.verifier, Now: func() time.Time { return time.Now().Add(time.Duration(p.wallOffset.Load())) }, Telemetry: &p.telemetry})
	if err != nil {
		t.Fatal(err)
	}
	var active atomic.Int32
	capacityReleased := make(chan struct{})
	r := continuityRequest(p, client)
	r.TrackVerifiedCapacity = func(*protocol.CapacityAttribution, protocol.CredentialSource) (func(), error) {
		active.Add(1)
		return func() { active.Add(-1); close(capacityReleased) }, nil
	}
	session, err := engine.Open(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	var resumeOnce sync.Once
	releaseResume := func() { resumeOnce.Do(func() { close(release) }) }
	defer releaseResume()
	t.Cleanup(func() {
		releaseTighten()
		releaseClose()
		releaseResume()
		session.Abort()
		_ = awaitContinuity(t, capacityReleased)
		if active.Load() != 0 {
			t.Fatal("pending replacement cleanup did not join original capacity")
		}
	})
	_ = awaitContinuity(t, session.Events())
	_ = awaitContinuity(t, p.connected)
	_ = awaitContinuity(t, p.clockSamples)
	_ = awaitContinuity(t, p.renewalStarted)
	if !heldReturn {
		p.holdSetup = true
	}
	close(p.renewalRelease)
	_ = awaitContinuity(t, p.connected)
	if heldReturn {
		_ = awaitContinuity(t, resumed)
	}
	if holds.shorter {
		p.sqlOffset.Store(int64(13 * time.Second))
	} else if forward {
		p.sqlOffset.Store(int64(18 * time.Second))
	} else {
		p.sqlOffset.Store(int64(-30 * time.Second))
	}
	p.wallOffset.Store(int64(time.Minute))
	fresh := awaitContinuity(t, p.clockSamples)
	if forward && !holds.shorter && (fresh.Before(renewedExpiry) || !fresh.Before(p.plan.Reservation.Billing.RenewableUntil)) {
		t.Fatal("sample did not pass renewed credential while remaining before root horizon")
	}
	if holds.shorter && !fresh.Before(renewedExpiry) {
		t.Fatal("shorter control already crossed its original renewed horizon")
	}
	if holds.tightenAndClose {
		_ = awaitContinuity(t, tightenReached)
	}
	window := 700 * time.Millisecond
	if holds.shorter {
		window = max(renewedExpiry.Sub(fresh), 0) + 300*time.Millisecond
	}
	wasClosed := false
	select {
	case <-closed:
		wasClosed = true
	case <-time.After(window):
	}
	capacity := active.Load()
	if holds.tightenAndClose {
		if !wasClosed {
			t.Fatal("physical pending cap did not run while the coordinator tightening return was held")
		}
		if capacity != 1 {
			t.Fatal("physical close return was still held but capacity escaped")
		}
	}
	releaseTighten()
	if holds.tightenAndClose {
		select {
		case <-session.Done():
		case <-time.After(300 * time.Millisecond):
			t.Fatal("tightened pending expiry did not publish terminal metadata independently of physical joins")
		}
	} else {
		session.Abort()
		_ = awaitContinuity(t, session.Done())
	}
	if holds.tightenAndClose {
		if !errors.Is(session.Err(), runtimepkg.ErrSessionLeaseExpired) {
			t.Fatalf("tightened physical expiry lost original terminal cause: %v", session.Err())
		}
		assertOwnedTerminalOutcome(t, p, session, protocol.EventError, "session.failed")
		if active.Load() != 1 {
			t.Fatal("terminal receipt released unjoined physical capacity")
		}
		releaseResume()
		releaseClose()
		_ = awaitContinuity(t, capacityReleased)
		if active.Load() != 0 {
			t.Fatal("joined original replacement retained capacity")
		}
	}
	var failedAt time.Time
	for _, event := range p.telemetry.snapshot() {
		if event.Name == "session.failed" {
			failedAt = event.At
		}
	}
	if failedAt.Before(fresh) || failedAt.After(fresh.Add(4*time.Second)) {
		t.Fatalf("trusted sample was not installed before observation: sample=%v receipt=%v", fresh, failedAt)
	}
	if !forward && wasClosed {
		t.Fatal("backward SQL sample prematurely closed pending replacement")
	}
	if forward && !wasClosed {
		t.Fatalf("pending replacement remained connected after installed SQL passed its signed horizon; capacity=%d", capacity)
	}
}
