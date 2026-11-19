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

type heldRootAdapter struct {
	runtimepkg.Adapter
	opened, openReturn, joined, joinReturn chan struct{}
}

func (a *heldRootAdapter) GoogleRealtimeAdapter() bool { return installedGoogleCapability(a.Adapter) }

func (a *heldRootAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	stream, err := a.Adapter.Open(ctx, request)
	if err != nil {
		return stream, err
	}
	close(a.opened) // actual Google setupComplete has been consumed
	if a.openReturn != nil {
		<-a.openReturn
	}
	return &heldRootStream{ResumableProviderStream: stream.(runtimepkg.ResumableProviderStream), owner: a}, nil
}

type heldRootStream struct {
	runtimepkg.ResumableProviderStream
	owner *heldRootAdapter
}

func (s *heldRootStream) TerminalError() error {
	return s.ResumableProviderStream.(runtimepkg.TerminalErrorProviderStream).TerminalError()
}
func (s *heldRootStream) Interrupt() {
	s.ResumableProviderStream.(runtimepkg.InterruptingProviderStream).Interrupt()
}
func (s *heldRootStream) Abort(ctx context.Context) error {
	err := s.ResumableProviderStream.(runtimepkg.AbortingProviderStream).Abort(ctx)
	close(s.owner.joined)
	<-s.owner.joinReturn
	return err
}

func TestFundedRootPhysicalDeadlineUsesOriginalCredentialAndSurvivesHeldOpen(t *testing.T) {
	for _, mode := range []string{"short_credential", "held_open", "short_credential_held_open"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			lease := time.Second
			if mode != "held_open" {
				lease = 3 * time.Second
			}
			p, _, client := newContinuityPeer(t, lease, 0, true)
			expires := p.plan.Reservation.LeaseExpiresAt
			if mode != "held_open" {
				expires = expires.Add(-2 * time.Second)
				p.plan.Route.Credential.ExpiresAt = expires
				p.envelope.Plan = p.plan.Unsigned()
				p.plan = resignPlan(t, p.private, "continuity", p.envelope, p.plan)
				if err := p.plan.Validate(time.Now()); err != nil {
					t.Fatalf("ordinary signed short credential must remain admissible: %v", err)
				}
			}
			physicalClosed := make(chan struct{})
			httpClient := *p.server.Client()
			transport := httpClient.Transport
			httpClient.Transport = observerTransport(func(r *http.Request) (*http.Response, error) {
				response, err := transport.RoundTrip(r)
				if err == nil && response.StatusCode == 101 {
					response.Body = &socketObserver{ReadWriteCloser: response.Body.(io.ReadWriteCloser), closeSent: make(chan struct{}), physicalClosed: physicalClosed}
				}
				return response, err
			})
			google, err := geminirealtime.New(geminirealtime.Config{HTTPClient: &httpClient, AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
			if err != nil {
				t.Fatal(err)
			}
			adapter := &heldRootAdapter{Adapter: google, opened: make(chan struct{}), joined: make(chan struct{}), joinReturn: make(chan struct{})}
			defer close(adapter.joinReturn)
			if mode != "short_credential" {
				adapter.openReturn = make(chan struct{})
			}
			var once sync.Once
			releaseOpen := func() {
				if adapter.openReturn != nil {
					once.Do(func() { close(adapter.openReturn) })
				}
			}
			defer releaseOpen()
			engine, err := runtimepkg.New(runtimepkg.Config{Adapters: []runtimepkg.Adapter{adapter}, Verifier: p.verifier, Telemetry: &p.telemetry})
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
			var session *runtimepkg.Session
			result := make(chan error, 1)
			if adapter.openReturn == nil {
				session, err = engine.Open(t.Context(), request)
				if err != nil {
					t.Fatalf("valid short credential was rejected: %v", err)
				}
				t.Cleanup(session.Abort)
				_ = awaitContinuity(t, session.Events())
				if err := session.SubmitAudio(runtimepkg.AudioInput{Data: []byte{1, 0}}); err != nil {
					t.Fatal(err)
				}
				_ = awaitContinuity(t, p.inputs)
			} else {
				go func() {
					s, err := engine.Open(t.Context(), request)
					if s != nil {
						s.Abort()
					}
					result <- err
				}()
			}
			_ = awaitContinuity(t, adapter.opened)
			deadline := time.NewTimer(max(time.Until(expires.Add(400*time.Millisecond)), 0))
			defer deadline.Stop()
			select {
			case <-physicalClosed:
			case <-deadline.C:
				t.Fatal("original root socket physically open 400ms after signed credential/lease expiry")
			}
			if capacity.Load() != 1 {
				t.Fatal("physical expiry released capacity before the real teardown joined")
			}
			if session == nil {
				select {
				case err := <-result:
					t.Fatalf("Open returned before the actual adapter return was released: %v", err)
				default:
				}
				releaseOpen()
				if err := awaitContinuity(t, result); !errors.Is(err, runtimepkg.ErrSessionLeaseExpired) {
					t.Fatalf("late Open changed original deadline outcome: %v", err)
				}
			} else {
				_ = awaitContinuity(t, session.Done())
				if !errors.Is(session.Err(), runtimepkg.ErrSessionLeaseExpired) {
					t.Fatalf("short credential expiry became success: %v", session.Err())
				}
			}
			_ = awaitContinuity(t, adapter.joined)
			if capacity.Load() != 1 {
				t.Fatal("capacity escaped the held serialized abort result")
			}
			adapter.joinReturn <- struct{}{}
			_ = awaitContinuity(t, released)
			if session != nil {
				assertOwnedTerminalOutcome(t, p, session, protocol.EventError, "session.failed")
			} else {
				assertAdmissionFailure(t, p, true)
			}
		})
	}
}

type failingPhysicalClose struct {
	io.ReadWriteCloser
	failure error
}

func (s failingPhysicalClose) Close() error {
	// The owned connection really closes; its transport also reports a genuine
	// close failure which coder/websocket discards when the peer status is1001.
	return errors.Join(s.ReadWriteCloser.Close(), s.failure)
}

func TestGoogleNormalPeerStatusPreservesPhysicalCloseFailure(t *testing.T) {
	for _, name := range []string{"clean_1001", "failed_transport_1001", "failed_transport_1000", "peer_only_clean_1001", "peer_only_failed_transport_1001"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p, _, client := newContinuityPeer(t, 20*time.Second, 0, true)
			peerClose := make(chan struct{})
			p.holdClose = peerClose
			p.closeWithStatus = websocket.StatusGoingAway
			if name == "failed_transport_1000" {
				p.closeWithStatus = websocket.StatusNormalClosure
			}
			var peerOnce sync.Once
			releasePeer := func() { peerOnce.Do(func() { close(peerClose) }) }
			defer releasePeer()
			closeFailure := errors.New("owned transport close failure")
			peerOnly := name == "peer_only_clean_1001" || name == "peer_only_failed_transport_1001"
			failed := name != "clean_1001" && name != "peer_only_clean_1001"
			closeSent, physicalClosed := make(chan struct{}), make(chan struct{})
			httpClient := *p.server.Client()
			transport := httpClient.Transport
			httpClient.Transport = observerTransport(func(r *http.Request) (*http.Response, error) {
				response, err := transport.RoundTrip(r)
				if err == nil && response.StatusCode == 101 {
					body := response.Body.(io.ReadWriteCloser)
					if failed {
						body = failingPhysicalClose{ReadWriteCloser: body, failure: closeFailure}
					}
					response.Body = &socketObserver{ReadWriteCloser: body, closeSent: closeSent, physicalClosed: physicalClosed}
				}
				return response, err
			})
			google, err := geminirealtime.New(geminirealtime.Config{HTTPClient: &httpClient, AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
			if err != nil {
				t.Fatal(err)
			}
			returned, release := make(chan error, 1), make(chan struct{})
			var releaseOnce sync.Once
			releaseReturn := func() { releaseOnce.Do(func() { close(release) }) }
			defer releaseReturn()
			var selected runtimepkg.Adapter = joinedCloseAdapter{Adapter: google, reached: returned, release: release}
			if peerOnly {
				selected = &heldRootAdapter{Adapter: google, opened: make(chan struct{}), joined: make(chan struct{}), joinReturn: release}
			}
			engine, err := runtimepkg.New(runtimepkg.Config{Adapters: []runtimepkg.Adapter{selected}, Verifier: p.verifier, Telemetry: &p.telemetry})
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
			var closeErr error
			if peerOnly {
				releasePeer()
				_ = awaitContinuity(t, closeSent) // actual peer-initiated close ACK
			} else {
				session.Close()
				_ = awaitContinuity(t, closeSent)
				releasePeer()
				closeErr = awaitContinuity(t, returned)
			}
			_ = awaitContinuity(t, physicalClosed)
			_ = awaitContinuity(t, session.Done()) // Close's returned result is still held
			if capacity.Load() != 1 {
				t.Fatal("close outcome released capacity before the held serialized return")
			}
			if failed && ((!peerOnly && !errors.Is(closeErr, closeFailure)) || !errors.Is(session.Err(), closeFailure)) {
				t.Fatalf("normal status discarded actual transport close failure: Close=%v session=%v", closeErr, session.Err())
			}
			if !failed && (closeErr != nil || session.Err() != nil) {
				t.Fatalf("clean1001 became failure: %v/%v", closeErr, session.Err())
			}
			wantEvent, wantReceipt := protocol.EventSessionClosed, "session.closed"
			if failed {
				wantEvent, wantReceipt = protocol.EventError, "session.failed"
			}
			assertOwnedTerminalOutcome(t, p, session, wantEvent, wantReceipt)
			releaseReturn()
			_ = awaitContinuity(t, released)
			if capacity.Load() != 0 {
				t.Fatal("joined close retained capacity")
			}
		})
	}
}
