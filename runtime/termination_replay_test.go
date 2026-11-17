package runtime_test

import (
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/controlplane"
	"github.com/SpekoAI/gateway/gateway"
	"github.com/SpekoAI/gateway/protocol"
	"github.com/SpekoAI/gateway/providers/geminirealtime"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

type socketObserver struct {
	io.ReadWriteCloser
	closeSent, physicalClosed chan struct{}
	sentOnce, closedOnce      sync.Once
	wireMu                    sync.Mutex
	wire                      []byte
}

func (s *socketObserver) Write(data []byte) (int, error) {
	n, err := s.ReadWriteCloser.Write(data)
	s.wireMu.Lock()
	defer s.wireMu.Unlock()
	s.wire = append(s.wire, data[:n]...)
	for len(s.wire) >= 2 {
		header, length := 2, uint64(s.wire[1]&0x7f)
		if length == 126 {
			if len(s.wire) < 4 {
				break
			}
			length = uint64(binary.BigEndian.Uint16(s.wire[2:4]))
			header = 4
		}
		if length == 127 {
			if len(s.wire) < 10 {
				break
			}
			length = binary.BigEndian.Uint64(s.wire[2:10])
			header = 10
		}
		if s.wire[1]&0x80 != 0 {
			header += 4
		}
		if uint64(len(s.wire)) < uint64(header)+length {
			break
		}
		if s.wire[0]&0xf == 8 {
			s.sentOnce.Do(func() { close(s.closeSent) })
		}
		s.wire = s.wire[header+int(length):]
	}
	return n, err
}
func (s *socketObserver) Close() error {
	err := s.ReadWriteCloser.Close()
	s.closedOnce.Do(func() { close(s.physicalClosed) })
	return err
}

type observerTransport func(*http.Request) (*http.Response, error)

func (t observerTransport) RoundTrip(r *http.Request) (*http.Response, error) { return t(r) }

type joinedCloseStream struct {
	runtimepkg.ResumableProviderStream
	reached, release chan struct{}
}

func (s *joinedCloseStream) Interrupt() {
	s.ResumableProviderStream.(runtimepkg.InterruptingProviderStream).Interrupt()
}
func (s *joinedCloseStream) Close(ctx context.Context) error {
	err := s.ResumableProviderStream.Close(ctx)
	s.reached <- struct{}{}
	<-s.release
	return err
}

type joinedCloseAdapter struct {
	runtimepkg.Adapter
	reached, release chan struct{}
}

func (a joinedCloseAdapter) Open(ctx context.Context, r runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	stream, err := a.Adapter.Open(ctx, r)
	if err != nil {
		return stream, err
	}
	return &joinedCloseStream{ResumableProviderStream: stream.(runtimepkg.ResumableProviderStream), reached: a.reached, release: a.release}, nil
}

func TestFundedAbortInterruptsBlockedGracefulCloseBeforeJoining(t *testing.T) {
	for _, nearLease := range []bool{false, true} {
		t.Run(map[bool]string{false: "before_lease", true: "at_lease"}[nearLease], func(t *testing.T) {
			t.Parallel()
			p, _, c := newContinuityPeer(t, 2*time.Second, 10*time.Second, true)
			peerRead := make(chan struct{})
			p.holdClose = peerRead
			defer close(peerRead)
			closeSent, physicalClosed := make(chan struct{}), make(chan struct{})
			client := *p.server.Client()
			transport := client.Transport
			client.Transport = observerTransport(func(r *http.Request) (*http.Response, error) {
				response, err := transport.RoundTrip(r)
				if err == nil && response.StatusCode == 101 {
					response.Body = &socketObserver{ReadWriteCloser: response.Body.(io.ReadWriteCloser), closeSent: closeSent, physicalClosed: physicalClosed}
				}
				return response, err
			})
			adapter, err := geminirealtime.New(geminirealtime.Config{HTTPClient: &client, AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
			if err != nil {
				t.Fatal(err)
			}
			joined, joinRelease := make(chan struct{}, 1), make(chan struct{})
			var releaseOnce sync.Once
			releaseJoin := func() { releaseOnce.Do(func() { close(joinRelease) }) }
			defer releaseJoin()
			engine, err := runtimepkg.New(runtimepkg.Config{Adapters: []runtimepkg.Adapter{joinedCloseAdapter{Adapter: adapter, reached: joined, release: joinRelease}}, Verifier: p.verifier, Now: func() time.Time { return time.Now().Add(10 * time.Second) }, Telemetry: &p.telemetry})
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
			session, err := engine.Open(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(session.Abort)
			_ = awaitContinuity(t, session.Events())
			_ = awaitContinuity(t, p.setups)
			_ = awaitContinuity(t, p.connected)
			session.Close()
			_ = awaitContinuity(t, closeSent) // Actual close frame sent while peer refuses its ACK.
			if nearLease {
				timer := time.NewTimer(max(time.Until(p.plan.Reservation.LeaseExpiresAt.Add(-10*time.Millisecond)), 0))
				<-timer.C
			}
			abortReturned := make(chan struct{})
			go func() { session.Abort(); close(abortReturned) }()
			select {
			case <-physicalClosed:
			case <-time.After(300 * time.Millisecond):
				t.Fatal("abort waited for the provider mutex/close ACK before stopping the socket")
			}
			_ = awaitContinuity(t, abortReturned)
			_ = awaitContinuity(t, session.Done())
			_ = awaitContinuity(t, joined)
			if active.Load() != 1 {
				t.Fatal("physical capacity released before serialized teardown joined")
			}
			if !errors.Is(session.Err(), runtimepkg.ErrSessionAborted) && !errors.Is(session.Err(), runtimepkg.ErrSessionLeaseExpired) {
				t.Fatalf("terminal abort became clean close: %v", session.Err())
			}
			if time.Now().After(p.plan.Reservation.LeaseExpiresAt.Add(300 * time.Millisecond)) {
				t.Fatal("physical termination exceeded the original funded lease")
			}
			// The public consumer deliberately leaves terminal Events unread. Neither
			// physical interruption nor teardown release depends on draining it.
			releaseJoin()
			_ = awaitContinuity(t, released)
			if active.Load() != 0 {
				t.Fatal("joined teardown failed to release capacity")
			}
			failures := 0
			for _, event := range p.telemetry.snapshot() {
				if event.Name == "session.closed" {
					t.Fatal("abort fabricated clean terminal evidence")
				}
				if event.Name == "session.failed" {
					failures++
					if event.AttemptID != p.plan.AttemptID || event.Destination.Token != p.plan.Telemetry.Token {
						t.Fatal("abort changed receipt authority")
					}
				}
			}
			if failures != 1 {
				t.Fatalf("terminal failure occurrences %d", failures)
			}
		})
	}
}

func TestLocalBYOKSpentPlansSurviveRollbackAndBoundedEviction(t *testing.T) {
	for _, mode := range []string{"local_planner", "signature_only", "default_verifier"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			p, _, _ := newContinuityPeer(t, 3*time.Second, 0, true)
			initial := time.Now().UTC()
			var clock atomic.Int64
			clock.Store(initial.UnixNano())
			wall := func() time.Time { return time.Unix(0, clock.Load()).UTC() }
			planner, err := gateway.NewLocalPlanner(gateway.LocalPlannerConfig{Providers: []string{"google"}, Now: wall, MaxSessionDuration: 2 * time.Minute, RouteOverrides: map[string]gateway.LocalRouteOverride{p.plan.Route.Adapter: {Endpoint: p.plan.Route.Endpoint}}})
			if err != nil {
				t.Fatal(err)
			}
			var verifier runtimepkg.PlanVerifier = planner
			var stock *runtimepkg.JWKSPlanVerifier
			if mode == "signature_only" {
				verifier = runtimepkg.PlanVerifierFunc(planner.Verify)
			}
			if mode == "default_verifier" {
				jwks := newJWKSFixture(t, "rollback", p.private.Public().(ed25519.PublicKey))
				stock, err = runtimepkg.NewPlanVerifier(runtimepkg.PlanVerifierConfig{JWKSURL: jwks.URL, Issuer: p.envelope.Issuer, Audience: "speko-runtime", Now: wall, ClockSkew: time.Nanosecond, AllowInsecureJWKS: true, MaxReplayEntries: 1})
				if err != nil {
					t.Fatal(err)
				}
				verifier = stock
			}
			ask := continuityRequest(p, nil)
			planRequest := protocol.SessionPlanRequest{Kind: ask.Kind, Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision, Runtime: protocol.RuntimeDescriptor{Name: "owned-byok", Version: "test", InstanceID: "rollback", Placement: protocol.PlacementEmbedded, ProviderRoutes: []protocol.ProviderRoute{protocol.RouteProviderDirect}, Adapters: []string{p.plan.Route.Adapter}}, Execution: protocol.ExecutionRequest{ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK, RelayPolicy: protocol.RelayForbidden}, Request: ask.Options, Media: ask.Media}
			planRequest.Request.Provider = "google"
			planRequest.Request.MaxSessionSeconds = 120
			issue := func() protocol.SessionPlan {
				plan, _, err := planner.CreateSessionPlan(context.Background(), planRequest, controlplane.CreateOptions{IdempotencyKey: "owned"})
				if err != nil {
					t.Fatal(err)
				}
				if stock != nil {
					envelope := protocol.SessionPlanEnvelope{Issuer: p.envelope.Issuer, Audience: []string{"speko-runtime"}, ID: plan.PlanID, IssuedAt: wall().Add(-time.Second), ExpiresAt: plan.ExpiresAt, Plan: plan.Unsigned()}
					plan = resignPlan(t, p.private, "rollback", envelope, plan)
				}
				return plan
			}
			engine, err := runtimepkg.New(runtimepkg.Config{Adapters: []runtimepkg.Adapter{p.adapter}, Verifier: verifier, Now: wall, MaxReplayEntries: 1, LocalCredentials: map[string]runtimepkg.LocalCredential{"google": {Kind: protocol.CredentialBearer, Value: "customer-owned-key"}}})
			if err != nil {
				t.Fatal(err)
			}
			open := func(plan protocol.SessionPlan) error {
				request := ask
				request.Plan = plan
				session, err := engine.Open(context.Background(), request)
				if err != nil {
					return err
				}
				_ = awaitContinuity(t, session.Events())
				_ = awaitContinuity(t, p.setups)
				_ = awaitContinuity(t, p.connected)
				session.Close()
				_ = awaitContinuity(t, session.Done())
				_ = awaitContinuity(t, p.closed)
				return session.Err()
			}
			original := issue()
			// The real LocalPlanner issues a one-minute redemption TTL. Open near its
			// end so the old monotonic eviction interval is short without editing or
			// resigning its HMAC plan. Its two-minute session lease remains live.
			admittedAt := original.ExpiresAt.Add(-200 * time.Millisecond)
			clock.Store(admittedAt.UnixNano())
			if err := open(original); err != nil {
				t.Fatal(err)
			}
			clock.Store(admittedAt.Add(-10 * time.Second).UnixNano())
			timer := time.NewTimer(300 * time.Millisecond)
			<-timer.C
			if err := original.Validate(wall()); err != nil {
				t.Fatalf("rollback fixture no longer locally valid: %v", err)
			}
			if err := open(original); !errors.Is(err, runtimepkg.ErrPlanReplay) {
				t.Fatalf("spent local token reopened after its monotonic interval: %v", err)
			}
			for range 2 {
				if err := open(issue()); !errors.Is(err, runtimepkg.ErrReplayCacheFull) {
					t.Fatalf("retention bound evicted a live key: %v", err)
				}
			}
			if err := open(original); !errors.Is(err, runtimepkg.ErrPlanReplay) {
				t.Fatalf("capacity refusal removed original fence: %v", err)
			}
			// Advance past expiry to make bounded room, then roll backwards again.
			// The admission watermark must stop an already-evicted token reviving.
			clock.Store(original.ExpiresAt.Add(time.Second).UnixNano())
			if err := open(issue()); err != nil {
				t.Fatalf("expired guards did not free bounded capacity: %v", err)
			}
			clock.Store(admittedAt.Add(-10 * time.Second).UnixNano())
			if err := original.Validate(wall()); err != nil {
				t.Fatal(err)
			}
			if err := open(original); err == nil {
				t.Fatal("backward clock revived an evicted spent token")
			}
			if stock != nil {
				if err := stock.Verify(context.Background(), original); err == nil {
					t.Fatal("default verifier revived an evicted nonce")
				}
				another, err := runtimepkg.New(runtimepkg.Config{Adapters: []runtimepkg.Adapter{p.adapter}, Verifier: stock, Now: wall, LocalCredentials: map[string]runtimepkg.LocalCredential{"google": {Kind: protocol.CredentialBearer, Value: "customer-owned-key"}}})
				if err != nil {
					t.Fatal(err)
				}
				request := ask
				request.Plan = original
				if _, err := another.Open(context.Background(), request); err == nil {
					t.Fatal("fresh Engine bypassed the shared verifier watermark")
				}
			}
			p.mu.Lock()
			defer p.mu.Unlock()
			if p.sockets != 2 {
				t.Fatalf("replay reached a real provider socket: %d", p.sockets)
			}
		})
	}
}
