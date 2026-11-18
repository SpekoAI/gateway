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
	"github.com/coder/websocket"
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
	reached chan error
	release chan struct{}
}

func (s *joinedCloseStream) TerminalError() error {
	return s.ResumableProviderStream.(runtimepkg.TerminalErrorProviderStream).TerminalError()
}
func (s *joinedCloseStream) Abort(ctx context.Context) error {
	return s.ResumableProviderStream.(runtimepkg.AbortingProviderStream).Abort(ctx)
}
func (s *joinedCloseStream) Interrupt() {
	s.ResumableProviderStream.(runtimepkg.InterruptingProviderStream).Interrupt()
}
func (s *joinedCloseStream) Close(ctx context.Context) error {
	err := s.ResumableProviderStream.Close(ctx)
	s.reached <- err
	<-s.release
	return err
}

type joinedCloseAdapter struct {
	runtimepkg.Adapter
	reached chan error
	release chan struct{}
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
			joined, joinRelease := make(chan error, 1), make(chan struct{})
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

func TestFundedGoogleCloseOutcomePrecedesHeldReturn(t *testing.T) {
	for _, order := range []string{"failed_close_first", "abort_first", "lease_first", "normal_close"} {
		t.Run(order, func(t *testing.T) {
			t.Parallel()
			lease := 30 * time.Second // allow the SDK's real five-second ACK timeout
			if order == "lease_first" {
				lease = time.Second
			}
			p, _, client := newContinuityPeer(t, lease, 10*time.Second, true)
			peerRead := make(chan struct{})
			if order != "normal_close" {
				p.holdClose = peerRead
			}
			defer close(peerRead)
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
			var releaseOnce sync.Once
			releaseReturn := func() { releaseOnce.Do(func() { close(release) }) }
			defer releaseReturn()
			engine, err := runtimepkg.New(runtimepkg.Config{Adapters: []runtimepkg.Adapter{joinedCloseAdapter{Adapter: adapter, reached: returned, release: release}}, Verifier: p.verifier, Telemetry: &p.telemetry})
			if err != nil {
				t.Fatal(err)
			}
			var active atomic.Int32
			released := make(chan struct{}, 1)
			request := continuityRequest(p, client)
			request.TrackVerifiedCapacity = func(*protocol.CapacityAttribution, protocol.CredentialSource) (func(), error) {
				active.Add(1)
				return func() { active.Add(-1); released <- struct{}{} }, nil
			}
			session, err := engine.Open(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(session.Abort)
			_ = awaitContinuity(t, session.Events()) // leave all terminal events unread
			_ = awaitContinuity(t, p.setups)
			_ = awaitContinuity(t, p.connected)
			session.Close()
			_ = awaitContinuity(t, closeSent)
			if order == "abort_first" {
				started := time.Now()
				session.Abort()
				_ = awaitContinuity(t, physicalClosed)
				if time.Since(started) > 300*time.Millisecond {
					t.Fatal("abort waited for close result or provider mutex")
				}
			}
			// This is the actual adapter/SDK result, held AFTER it returns. The
			// runtime close caller cannot yet observe either its error or success.
			closeErr := awaitContinuity(t, returned)
			_ = awaitContinuity(t, physicalClosed)
			_ = awaitContinuity(t, session.Done())
			if active.Load() != 1 {
				t.Fatal("capacity released while Close's serialized join is held")
			}
			switch order {
			case "failed_close_first":
				if closeErr == nil {
					t.Fatal("withheld ACK did not fail the real provider close")
				}
				var failure *runtimepkg.ProviderError
				if !errors.As(session.Err(), &failure) || failure.Code != "provider_unavailable" {
					t.Fatalf("failed close became clean before Close return: %v", session.Err())
				}
			case "abort_first":
				if !errors.Is(session.Err(), runtimepkg.ErrSessionAborted) {
					t.Fatalf("close result changed earlier abort: %v", session.Err())
				}
			case "lease_first":
				if !errors.Is(session.Err(), runtimepkg.ErrSessionLeaseExpired) || time.Now().After(p.plan.Reservation.LeaseExpiresAt.Add(300*time.Millisecond)) {
					t.Fatalf("close result delayed/changed earlier lease expiry: %v", session.Err())
				}
			case "normal_close":
				if closeErr != nil || session.Err() != nil {
					t.Fatalf("normal close became failure: close=%v session=%v", closeErr, session.Err())
				}
				if status := awaitContinuity(t, p.closeStatuses); status != websocket.StatusNormalClosure {
					t.Fatalf("normal close lost its handshake: %v", status)
				}
			}
			original := session.Err()
			session.Abort() // a later abort must not replace the established result
			if session.Err() != original {
				t.Fatal("later abort replaced the terminal outcome")
			}
			releaseReturn()
			_ = awaitContinuity(t, released)
			if active.Load() != 0 || session.Err() != original {
				t.Fatal("teardown join changed outcome or retained capacity")
			}
			events := collectEvents(t, session)
			wantEvent, wantReceipt := protocol.EventError, "session.failed"
			if order == "normal_close" {
				wantEvent, wantReceipt = protocol.EventSessionClosed, "session.closed"
			}
			if len(events) != 1 || events[0].Type != wantEvent {
				t.Fatalf("terminal event = %+v, want %s", events, wantEvent)
			}
			terminal := 0
			for _, event := range p.telemetry.snapshot() {
				if event.Name != "session.failed" && event.Name != "session.closed" {
					continue
				}
				terminal++
				if event.Name != wantReceipt || event.AttemptID != p.plan.AttemptID || event.Destination.Token != p.plan.Telemetry.Token {
					t.Fatalf("close published wrong terminal evidence: %+v", event)
				}
			}
			if terminal != 1 {
				t.Fatalf("terminal evidence count = %d", terminal)
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
