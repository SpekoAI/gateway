package runtime_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
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
	resumed chan struct{}
	release <-chan struct{}
}

func (a pendingClockResumeAdapter) Open(ctx context.Context, r runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	s, err := a.ownedContinuationAdapter.Open(ctx, r)
	if err != nil {
		return s, err
	}
	return &pendingClockResumeStream{ResumableProviderStream: s.(runtimepkg.ResumableProviderStream), resumed: a.resumed, release: a.release}, nil
}

type pendingClockResumeStream struct {
	runtimepkg.ResumableProviderStream
	resumed chan struct{}
	release <-chan struct{}
}

func (s *pendingClockResumeStream) Resume(ctx context.Context, credential protocol.DelegatedCredential) error {
	if err := s.ResumableProviderStream.Resume(ctx, credential); err != nil {
		return err
	}
	close(s.resumed)
	<-s.release
	return nil
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

func pendingReplacementSQLRefresh(t *testing.T, forward, heldReturn bool) {
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
	hc := *p.server.Client()
	transport := hc.Transport
	hc.Transport = observerTransport(func(r *http.Request) (*http.Response, error) {
		response, err := transport.RoundTrip(r)
		if err == nil && response.StatusCode == 101 && r.URL.Query().Get("access_token") == "renewed-token" {
			response.Body = &socketObserver{ReadWriteCloser: response.Body.(io.ReadWriteCloser), closeSent: make(chan struct{}), physicalClosed: closed}
		}
		return response, err
	})
	google, err := geminirealtime.New(geminirealtime.Config{HTTPClient: &hc, AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatal(err)
	}
	var adapter runtimepkg.Adapter = ownedContinuationAdapter{google}
	resumed, release := make(chan struct{}), make(chan struct{})
	if heldReturn {
		adapter = pendingClockResumeAdapter{ownedContinuationAdapter: ownedContinuationAdapter{google}, resumed: resumed, release: release}
	}
	engine, err := runtimepkg.New(runtimepkg.Config{Adapters: []runtimepkg.Adapter{adapter}, Verifier: p.verifier, Now: func() time.Time { return time.Now().Add(time.Duration(p.wallOffset.Load())) }, Telemetry: &p.telemetry})
	if err != nil {
		t.Fatal(err)
	}
	var active atomic.Int32
	r := continuityRequest(p, client)
	r.TrackVerifiedCapacity = func(*protocol.CapacityAttribution, protocol.CredentialSource) (func(), error) {
		active.Add(1)
		return func() { active.Add(-1) }, nil
	}
	session, err := engine.Open(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(session.Abort)
	defer close(release)
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
	if forward {
		p.sqlOffset.Store(int64(18 * time.Second))
	} else {
		p.sqlOffset.Store(int64(-30 * time.Second))
	}
	p.wallOffset.Store(int64(time.Minute))
	fresh := awaitContinuity(t, p.clockSamples)
	if forward && (fresh.Before(renewedExpiry) || !fresh.Before(p.plan.Reservation.Billing.RenewableUntil)) {
		t.Fatal("sample did not pass renewed credential while remaining before root horizon")
	}
	wasClosed := false
	select {
	case <-closed:
		wasClosed = true
	case <-time.After(700 * time.Millisecond):
	}
	capacity := active.Load()
	session.Abort()
	_ = awaitContinuity(t, session.Done())
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
