package runtime_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/gateway"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

type setupOnlyPlanner struct{ gateway.PlanClient }

func assertAdmissionFailure(t *testing.T, p *continuityPeer, trusted bool) {
	t.Helper()
	events := p.telemetry.snapshot()
	if len(events) != 1 || events[0].Name != "session.failed" || !events[0].Required || events[0].SessionID != p.plan.SessionID || events[0].AttemptID != p.plan.AttemptID || events[0].Destination != p.plan.Telemetry {
		t.Fatalf("admission evidence %+v", events)
	}
	if trusted && events[0].At.After(time.Now().Add(time.Second)) {
		t.Fatal("admission evidence ignored the trusted sample")
	}
	assertNoFinalUsage(t, events)
}

func TestFundedAdmissionRequiresClockAuthorityForEmbeddedAndCustomPlanner(t *testing.T) {
	for _, mode := range []string{"embedded", "custom_gateway_planner"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			p, e, c := newContinuityPeer(t, 3*time.Second, -10*time.Second, true)
			if mode == "embedded" {
				request := continuityRequest(p, c)
				request.Entitlements = nil
				if _, err := e.Open(context.Background(), request); err == nil || !strings.Contains(err.Error(), "SQL-clock authority") {
					t.Fatalf("missing clock result %v", err)
				}
			} else {
				server, err := gateway.New(gateway.Config{Engine: e, Plans: setupOnlyPlanner{c}, Runtime: protocol.RuntimeDescriptor{Name: "test", Version: "test", InstanceID: "test", Placement: protocol.PlacementSidecar, ProviderRoutes: []protocol.ProviderRoute{protocol.RouteProviderDirect}, Adapters: []string{p.plan.Route.Adapter}}, LocalAuthToken: "local"})
				if err != nil {
					t.Fatal(err)
				}
				peer := httptest.NewServer(server.Handler())
				defer peer.Close()
				ask := continuityRequest(p, c)
				body, _ := json.Marshal(gateway.CreateSessionRequest{Kind: ask.Kind, Execution: protocol.ExecutionRequest{ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsManaged, RelayPolicy: protocol.RelayForbidden}, Request: ask.Options, Media: ask.Media})
				request, _ := http.NewRequest(http.MethodPost, peer.URL+"/v1/sessions", bytes.NewReader(body))
				request.Header.Set("Authorization", "Bearer local")
				request.Header.Set("Content-Type", "application/json")
				request.Header.Set("Idempotency-Key", "admission")
				response, err := peer.Client().Do(request)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				if response.StatusCode != 502 {
					t.Fatalf("custom planner admitted unclocked root: %d", response.StatusCode)
				}
			}
			assertAdmissionFailure(t, p, false)
			if p.clockCalls.Load() != 0 {
				t.Fatal("missing authority performed clock I/O")
			}
			p.mu.Lock()
			defer p.mu.Unlock()
			if p.sockets != 0 {
				t.Fatal("unclocked root opened a provider")
			}
		})
	}
}

func TestFundedAdmissionShortPlansUseSQLTimeAcrossBothJWSFamilies(t *testing.T) {
	for _, skew := range []time.Duration{-10 * time.Second, 10 * time.Second} {
		for _, family := range []string{runtimepkg.SessionPlanJWSType, runtimepkg.SessionPlanCredentialDigestJWSType} {
			t.Run(skew.String()+family, func(t *testing.T) {
				t.Parallel()
				p, e, c := newContinuityPeer(t, 3*time.Second, skew, true)
				if family == runtimepkg.SessionPlanCredentialDigestJWSType {
					bound, err := p.plan.CredentialDigestBoundUnsigned()
					if err != nil {
						t.Fatal(err)
					}
					p.envelope.Plan = bound
					p.plan = resignPlanWithType(t, p.private, "continuity", family, p.envelope, p.plan)
				}
				// This is the same composite used by the CLI in mixed BYOK/managed mode.
				mixed := gateway.CredentialSourceVerifier{BYOK: runtimepkg.PlanVerifierFunc(func(context.Context, protocol.SessionPlan) error {
					t.Fatal("managed root reached BYOK verifier")
					return nil
				}), Managed: p.verifier}
				e, err := runtimepkg.New(runtimepkg.Config{Adapters: []runtimepkg.Adapter{p.adapter}, Verifier: mixed, Now: func() time.Time { return time.Now().Add(time.Duration(p.wallOffset.Load())) }, Telemetry: &p.telemetry})
				if err != nil {
					t.Fatal(err)
				}
				session := openContinuity(t, p, e, c)
				_ = awaitContinuity(t, p.setups)
				_ = awaitContinuity(t, p.connected)
				clocks := p.clockCalls.Load()
				if _, err := e.Open(context.Background(), continuityRequest(p, c)); !errors.Is(err, runtimepkg.ErrPlanReplay) {
					t.Fatalf("clock jump erased replay guard: %v", err)
				}
				if p.clockCalls.Load() != clocks {
					t.Fatal("replay performed a second clock request")
				}
				session.Abort()
				_ = awaitContinuity(t, p.closed)
				p.wallOffset.Store(int64(time.Hour))
				// A fresh Engine sharing the stock verifier must reject the same nonce too.
				other, _ := runtimepkg.New(runtimepkg.Config{Adapters: []runtimepkg.Adapter{p.adapter}, Verifier: p.verifier})
				if _, err := other.Open(context.Background(), continuityRequest(p, c)); !errors.Is(err, runtimepkg.ErrPlanReplay) {
					t.Fatalf("new Engine lost nonce protection: %v", err)
				}
				session.Abort()
			})
		}
	}
}

func TestFundedAdmissionAuthenticatesBeforeClockAndRecordsInitialClockFailure(t *testing.T) {
	for _, mode := range []string{"tampered", "wrong_audience", "initial_clock_failure"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			p, e, c := newContinuityPeer(t, 3*time.Second, 10*time.Second, true)
			switch mode {
			case "tampered":
				p.plan.Route.Model = "tampered"
			case "wrong_audience":
				p.envelope.Audience = []string{"another-runtime"}
				p.plan = resignPlan(t, p.private, "continuity", p.envelope, p.plan)
			case "initial_clock_failure":
				p.initialClockRefusal = true
			}
			if _, err := e.Open(context.Background(), continuityRequest(p, c)); err == nil {
				t.Fatal("invalid admission succeeded")
			}
			if mode == "initial_clock_failure" {
				assertAdmissionFailure(t, p, false)
				if p.clockCalls.Load() != 1 {
					t.Fatal("clock fixture not reached")
				}
				if _, err := e.Open(context.Background(), continuityRequest(p, c)); !errors.Is(err, runtimepkg.ErrPlanReplay) {
					t.Fatalf("clock failure did not retain nonce: %v", err)
				}
				assertAdmissionFailure(t, p, false)
			} else if p.clockCalls.Load() != 0 || len(p.telemetry.snapshot()) != 0 {
				t.Fatal("unverified plan crossed clock/evidence boundary")
			}
			p.mu.Lock()
			defer p.mu.Unlock()
			if p.sockets != 0 {
				t.Fatal("failed admission opened provider")
			}
		})
	}
}

type admissionAdapter struct {
	runtimepkg.Adapter
	waitExpiry               bool
	gateStarted, gateRelease chan struct{}
}

func (a admissionAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	stream, err := a.Adapter.Open(ctx, request)
	if stream == nil {
		return nil, err
	}
	if a.waitExpiry && err == nil {
		<-ctx.Done()
		_ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background())
	}
	return &admissionStream{ProviderStream: stream, started: a.gateStarted, release: a.gateRelease}, err
}

type admissionStream struct {
	runtimepkg.ProviderStream
	started, release chan struct{}
}

func (s *admissionStream) Abort(ctx context.Context) error {
	var err error
	if stream, ok := s.ProviderStream.(runtimepkg.AbortingProviderStream); ok {
		err = stream.Abort(ctx)
	} else {
		err = s.ProviderStream.Close(ctx)
	}
	s.started <- struct{}{}
	<-s.release
	return err
}

func TestFundedAdmissionBoundsProviderSetupAndRetainsPhysicalCapacity(t *testing.T) {
	for _, mode := range []string{"handshake", "bad_handshake", "setup_ack", "expired_return", "unsupported_stream"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			lease := time.Second
			if mode == "unsupported_stream" {
				lease = 3 * time.Second
			}
			p, _, c := newContinuityPeer(t, lease, 10*time.Second, true)
			p.holdHandshake = mode == "handshake"
			p.badHandshake = mode == "bad_handshake"
			handshake := p.holdHandshake || p.badHandshake
			p.holdSetup = mode == "setup_ack"
			started, release := make(chan struct{}, 1), make(chan struct{})
			var releaseOnce sync.Once
			releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(releaseGate)
			var adapter runtimepkg.Adapter = p.adapter
			if !handshake {
				adapter = admissionAdapter{Adapter: p.adapter, waitExpiry: mode == "expired_return", gateStarted: started, gateRelease: release}
			}
			e, err := runtimepkg.New(runtimepkg.Config{Adapters: []runtimepkg.Adapter{adapter}, Verifier: p.verifier, Now: func() time.Time { return time.Now().Add(time.Duration(p.wallOffset.Load())) }, Telemetry: &p.telemetry})
			if err != nil {
				t.Fatal(err)
			}
			var active atomic.Int32
			released := make(chan struct{}, 1)
			request := continuityRequest(p, c)
			request.TrackVerifiedCapacity = func(*protocol.CapacityAttribution, protocol.CredentialSource) (func(), error) {
				active.Add(1)
				return func() { active.Add(-1); released <- struct{}{} }, nil
			}
			began := time.Now()
			outcome := make(chan error, 1)
			go func() { _, err := e.Open(context.Background(), request); outcome <- err }()
			if handshake {
				_ = awaitContinuity(t, p.handshakeStarted)
			} else {
				_ = awaitContinuity(t, p.setups)
				_ = awaitContinuity(t, p.connected)
			}
			err = awaitContinuity(t, outcome)
			if handshake && time.Since(began) > 2*time.Second {
				t.Fatal("funded handshake exceeded its deadline")
			}
			if err == nil {
				t.Fatal("invalid funded setup succeeded")
			}
			if mode != "unsupported_stream" && !errors.Is(err, runtimepkg.ErrSessionLeaseExpired) {
				t.Fatalf("deadline classification: %v", err)
			}
			if handshake {
				_ = awaitContinuity(t, p.handshakeClosed)
			} else {
				_ = awaitContinuity(t, started)
				if active.Load() != 1 {
					t.Fatal("capacity released before teardown returned")
				}
				_ = awaitContinuity(t, p.closed)
				releaseGate()
			}
			_ = awaitContinuity(t, released)
			if active.Load() != 0 {
				t.Fatal("teardown did not release capacity")
			}
			assertAdmissionFailure(t, p, true)
		})
	}
}

func TestFundedGoogleGracefulClosePreservesNormalHandshakeAndBufferedUsage(t *testing.T) {
	p, e, c := newContinuityPeer(t, 3*time.Second, 10*time.Second, true)
	p.server.Client().Timeout = 5 * time.Second
	session := openContinuity(t, p, e, c)
	_ = awaitContinuity(t, p.setups)
	conn := awaitContinuity(t, p.connected)
	if err := conn.Write(context.Background(), websocket.MessageText, []byte(`{"usageMetadata":{}}`)); err != nil {
		t.Fatal(err)
	}
	if event := awaitContinuity(t, session.Events()); event.Type != protocol.EventUsageObserved {
		t.Fatalf("usage %s", event.Type)
	}
	if err := session.SubmitAudio(runtimepkg.AudioInput{Data: []byte{1, 0}}); err != nil {
		t.Fatal(err)
	}
	if err := session.CommitAudio(); err != nil {
		t.Fatal(err)
	}
	session.Close()
	_ = awaitContinuity(t, p.inputs)
	_ = awaitContinuity(t, p.inputs)
	events := collectEvents(t, session)
	_ = awaitContinuity(t, p.closed)
	if status := awaitContinuity(t, p.closeStatuses); status != websocket.StatusNormalClosure {
		t.Fatalf("graceful close observed %v", status)
	}
	if session.Err() != nil || len(events) != 1 || events[0].Type != protocol.EventSessionClosed {
		t.Fatalf("close lost terminal event: %v %+v", session.Err(), events)
	}
	count := 0
	for _, event := range p.telemetry.snapshot() {
		if event.Name == "usage.observed" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("usage occurrences %d", count)
	}
}

func TestFundedGoogleDeadlineInterruptsAnUnacknowledgedGracefulClose(t *testing.T) {
	p, e, c := newContinuityPeer(t, time.Second, 10*time.Second, true)
	release := make(chan struct{})
	p.holdClose = release
	// Release the owned peer even if an assertion fails while it withholds the
	// close acknowledgment. It does not detach itself at the lease deadline.
	defer close(release)
	session := openContinuity(t, p, e, c)
	_ = awaitContinuity(t, p.setups)
	_ = awaitContinuity(t, p.connected)
	closedAt := time.Now()
	session.Close()
	_ = awaitContinuity(t, session.Done())
	if !errors.Is(session.Err(), runtimepkg.ErrSessionLeaseExpired) {
		t.Fatalf("close suspended hard lease: %v", session.Err())
	}
	if time.Since(closedAt) > 2*time.Second {
		t.Fatal("lease waited for the five-second WebSocket close timeout")
	}
}

func TestFundedAdmissionRejectsSQLExpiryEvenWhenLocalClockIsSlow(t *testing.T) {
	p, e, c := newContinuityPeer(t, 3*time.Second, -10*time.Second, true)
	expired := time.Now().Add(-time.Second)
	p.plan.ExpiresAt = expired
	p.plan.Reservation.LeaseExpiresAt = expired
	p.plan.Route.Credential.ExpiresAt = expired
	p.envelope.ExpiresAt = expired
	p.envelope.Plan = p.plan.Unsigned()
	p.plan = resignPlan(t, p.private, "continuity", p.envelope, p.plan)
	if _, err := e.Open(context.Background(), continuityRequest(p, c)); err == nil {
		t.Fatal("SQL-expired credential admitted using local skew")
	}
	assertAdmissionFailure(t, p, true)
	if p.clockCalls.Load() != 1 {
		t.Fatal("expiry did not use SQL time")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sockets != 0 {
		t.Fatal("expired root opened provider")
	}
}
