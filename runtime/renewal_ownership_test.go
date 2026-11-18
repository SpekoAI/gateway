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

type capDroppingAdapter struct{ runtimepkg.Adapter }

type declinedGoogleCapability struct{ runtimepkg.Adapter }

func (declinedGoogleCapability) GoogleRealtimeAdapter() bool { return false }

func (a capDroppingAdapter) Open(ctx context.Context, r runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	r.PhysicalDeadline = time.Time{}
	return a.Adapter.Open(ctx, r)
}

func TestDefaultGoogleRequiresInstalledCapabilityBeforeIO(t *testing.T) {
	p, _, client := newContinuityPeer(t, time.Second, 0, true)
	adapter := &heldRootAdapter{Adapter: capDroppingAdapter{p.adapter}, opened: make(chan struct{}), openReturn: make(chan struct{}), joined: make(chan struct{}), joinReturn: make(chan struct{})}
	defer close(adapter.joinReturn)
	defer close(adapter.openReturn)
	engine, err := runtimepkg.New(runtimepkg.Config{Adapters: []runtimepkg.Adapter{adapter}, Verifier: p.verifier, Telemetry: &p.telemetry})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		s, err := engine.Open(t.Context(), continuityRequest(p, client))
		if s != nil {
			s.Abort()
		}
		result <- err
	}()
	select {
	case err := <-result:
		if !errors.Is(err, runtimepkg.ErrFundedGoogleAdapter) {
			t.Fatalf("default ID did not receive precise capability refusal: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("unmarked default-ID adapter reached held Open instead of pre-I/O refusal")
	}
	assertAdmissionFailure(t, p, false)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.clockCalls.Load() != 0 || p.sockets != 0 || p.mints != 0 {
		t.Fatal("unmarked default ID performed clock/provider I/O")
	}
}

func TestDefaultGoogleRefusesAbsentOrDeclinedCapability(t *testing.T) {
	for _, absent := range []bool{false, true} {
		name := "explicit_false"
		if absent {
			name = "not_installed"
		}
		t.Run(name, func(t *testing.T) {
			p, _, client := newContinuityPeer(t, 3*time.Second, 0, true)
			adapters := []runtimepkg.Adapter{declinedGoogleCapability{p.adapter}}
			if absent {
				adapters = nil
			}
			engine, err := runtimepkg.New(runtimepkg.Config{Adapters: adapters, Verifier: p.verifier, Telemetry: &p.telemetry})
			if err != nil {
				t.Fatal(err)
			}
			session, err := engine.Open(t.Context(), continuityRequest(p, client))
			if session != nil {
				session.Abort()
			}
			if !errors.Is(err, runtimepkg.ErrFundedGoogleAdapter) {
				t.Fatalf("default ID bypassed installed capability: %v", err)
			}
			assertAdmissionFailure(t, p, false)
			p.mu.Lock()
			defer p.mu.Unlock()
			if p.clockCalls.Load() != 0 || p.sockets != 0 || p.mints != 0 {
				t.Fatal("absent/declined default implementation performed I/O")
			}
		})
	}
}

// This peer serves real revision-3 HTTP renewals. Each original intent has a
// canonical response, so a timed-out delivery replays the same second slice.
func installTwoSliceHTTP(t *testing.T, p *continuityPeer, release <-chan struct{}, second chan struct{}) {
	t.Helper()
	var started sync.Once
	issued := make(map[int64]bool)
	p.renewalHandler = func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		previous, err := time.Parse(time.RFC3339Nano, body["previous_expires_at"])
		if err != nil || len(body) != 1 || r.Header.Get("Idempotency-Key") != "renew:"+p.plan.AttemptID+":"+previous.UTC().Format(time.RFC3339Nano) {
			t.Error("changed renewal intent/body")
			w.WriteHeader(400)
			return
		}
		var sequence int64
		switch {
		case previous.Equal(p.plan.Reservation.LeaseExpiresAt):
			sequence = 1
		case previous.Equal(p.plan.Reservation.LeaseExpiresAt.Add(3 * time.Second)):
			sequence = 2
		default:
			w.WriteHeader(401)
			return // no third positive slice or fork
		}
		p.mu.Lock()
		fresh := !issued[sequence]
		issued[sequence] = true
		p.calls++
		if fresh {
			p.mints++
		}
		p.mu.Unlock()
		if sequence == 2 {
			started.Do(func() { close(second) })
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		expires := previous.Add(3 * time.Second)
		value := "slice-one-token"
		if sequence == 2 {
			value = "slice-two-token"
		}
		response := controlplane.EntitlementRenewal{EntitlementID: value, SessionID: p.plan.SessionID, AttemptID: p.plan.AttemptID, ReservationID: p.plan.Reservation.ID, Sequence: sequence, PreviousExpiresAt: previous, LeaseExpiresAt: expires, AuthorizedUnits: 3, MaximumAmountMicros: 100, Currency: "USD", Credential: protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: value, ExpiresAt: expires}}
		if fresh {
			w.WriteHeader(201)
		} else {
			w.WriteHeader(200)
		}
		_ = json.NewEncoder(w).Encode(response)
	}
}

// Delay only the coordinator's Detach for the adopted first replacement. Its
// independent physical timer remains real and unchanged. This forces the valid
// scheduling order in which socket expiry wins before coordinator detachment.
type heldExpiryDetachAdapter struct {
	ownedContinuationAdapter
	release <-chan struct{}
}

func (a heldExpiryDetachAdapter) Open(ctx context.Context, r runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	stream, err := a.ownedContinuationAdapter.Open(ctx, r)
	if err != nil {
		return stream, err
	}
	return &heldExpiryDetachStream{ResumableProviderStream: stream.(runtimepkg.ResumableProviderStream), release: a.release}, nil
}

type heldExpiryDetachStream struct {
	runtimepkg.ResumableProviderStream
	release <-chan struct{}
	resumes atomic.Int32
}

func (s *heldExpiryDetachStream) Detach() {
	if s.resumes.Load() == 1 {
		<-s.release
	}
	s.ResumableProviderStream.Detach()
}
func (s *heldExpiryDetachStream) Resume(ctx context.Context, c protocol.DelegatedCredential) error {
	err := s.ResumableProviderStream.Resume(ctx, c)
	if err == nil {
		s.resumes.Add(1)
	}
	return err
}
func (s *heldExpiryDetachStream) TerminalError() error {
	return s.ResumableProviderStream.(runtimepkg.TerminalErrorProviderStream).TerminalError()
}
func (s *heldExpiryDetachStream) Interrupt() {
	s.ResumableProviderStream.(runtimepkg.InterruptingProviderStream).Interrupt()
}
func (s *heldExpiryDetachStream) Abort(ctx context.Context) error {
	return s.ResumableProviderStream.(runtimepkg.AbortingProviderStream).Abort(ctx)
}

func TestAdoptedReplacementExpiryPreservesSecondPendingHTTPRenewal(t *testing.T) {
	for _, late := range []bool{false, true} {
		name := "early_response"
		if late {
			name = "late_response"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p, _, client := newContinuityPeer(t, time.Second, 0)
			p.plan.Reservation.Billing.RenewableUntil = p.plan.Reservation.LeaseExpiresAt.Add(8 * time.Second)
			p.envelope.Plan = p.plan.Unsigned()
			p.plan = resignPlan(t, p.private, "continuity", p.envelope, p.plan)
			release, second := make(chan struct{}), make(chan struct{})
			var once sync.Once
			deliver := func() { once.Do(func() { close(release) }) }
			defer deliver()
			installTwoSliceHTTP(t, p, release, second)
			detachRelease := make(chan struct{})
			var detachOnce sync.Once
			releaseDetach := func() { detachOnce.Do(func() { close(detachRelease) }) }
			defer releaseDetach()
			if !late {
				releaseDetach()
			}
			firstClosed, lastClosed, joinReturn := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var joinOnce sync.Once
			join := func() { joinOnce.Do(func() { close(joinReturn) }) }
			defer join()
			hc := *p.server.Client()
			transport := hc.Transport
			hc.Transport = observerTransport(func(r *http.Request) (*http.Response, error) {
				response, err := transport.RoundTrip(r)
				if err == nil && response.StatusCode == 101 {
					switch r.URL.Query().Get("access_token") {
					case "slice-one-token":
						response.Body = &socketObserver{ReadWriteCloser: response.Body.(io.ReadWriteCloser), closeSent: make(chan struct{}), physicalClosed: firstClosed}
					case "slice-two-token":
						response.Body = &heldPhysicalClose{ReadWriteCloser: response.Body.(io.ReadWriteCloser), entered: lastClosed, release: joinReturn}
					}
				}
				return response, err
			})
			google, err := geminirealtime.New(geminirealtime.Config{HTTPClient: &hc, AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
			if err != nil {
				t.Fatal(err)
			}
			engine, err := runtimepkg.New(runtimepkg.Config{Adapters: []runtimepkg.Adapter{heldExpiryDetachAdapter{ownedContinuationAdapter: ownedContinuationAdapter{google}, release: detachRelease}}, Verifier: p.verifier, Telemetry: &p.telemetry})
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
			_ = awaitContinuity(t, p.connected)
			_ = awaitContinuity(t, second)
			predecessorCap := p.plan.Reservation.LeaseExpiresAt.Add(3 * time.Second)
			if late {
				timer := time.NewTimer(max(time.Until(predecessorCap.Add(150*time.Millisecond)), 0))
				defer timer.Stop()
				select {
				case <-session.Done():
					t.Fatalf("pending valid HTTP replay lost stable event source at predecessor expiry: %v", session.Err())
				case <-timer.C:
				}
				select {
				case <-firstClosed:
				case <-time.After(150 * time.Millisecond):
					t.Fatal("predecessor socket missed its original cap+300ms")
				}
				if capacity.Load() != 1 {
					t.Fatal("pending response released physical capacity")
				}
			}
			releaseDetach()
			deliver()
			select {
			case <-p.connected:
			case <-session.Done():
				t.Fatalf("second valid slice lost recovery: %v", session.Err())
			case <-time.After(3 * time.Second):
				t.Fatal("second valid slice did not open")
			}
			// In the early control, the prior cap occurs AFTER newer adoption.
			if !late {
				timer := time.NewTimer(max(time.Until(predecessorCap.Add(150*time.Millisecond)), 0))
				defer timer.Stop()
				<-timer.C
			}
			if err := session.SubmitAudio(runtimepkg.AudioInput{Data: []byte{1, 0}}); err != nil {
				t.Fatal(err)
			}
			select {
			case <-p.inputs:
			case <-session.Done():
				t.Fatalf("predecessor callback interrupted newer socket: %v", session.Err())
			case <-time.After(time.Second):
				t.Fatal("adopted second slice lost actual input")
			}
			p.mu.Lock()
			mints, sockets := p.mints, p.sockets
			p.mu.Unlock()
			if mints != 2 || sockets != 3 {
				t.Fatalf("second response forked financial/socket work: %d/%d", mints, sockets)
			}
			session.Abort()
			_ = awaitContinuity(t, session.Done())
			_ = awaitContinuity(t, lastClosed)
			if !errors.Is(session.Err(), runtimepkg.ErrSessionAborted) || capacity.Load() != 1 {
				t.Fatal("unread terminal evidence released unjoined socket")
			}
			assertOwnedTerminalOutcome(t, p, session, protocol.EventError, "session.failed")
			join()
			_ = awaitContinuity(t, released)
			if capacity.Load() != 0 {
				t.Fatal("joined second-slice socket retained capacity")
			}
		})
	}
}

type pendingUpgradeClose struct {
	io.ReadWriteCloser
	readStarted, firstClose, disconnected, releaseFirst, releaseSecond chan struct{}
	readOnce, disconnectedOnce                                         sync.Once
	calls, active, peak                                                atomic.Int32
}

func (b *pendingUpgradeClose) Read(p []byte) (int, error) {
	b.readOnce.Do(func() { close(b.readStarted) })
	return b.ReadWriteCloser.Read(p)
}
func (b *pendingUpgradeClose) Close() error {
	active := b.active.Add(1)
	defer b.active.Add(-1)
	for peak := b.peak.Load(); active > peak; peak = b.peak.Load() {
		if b.peak.CompareAndSwap(peak, active) {
			break
		}
	}
	call := b.calls.Add(1)
	if call == 1 {
		close(b.firstClose)
		<-b.releaseFirst
	}
	err := b.ReadWriteCloser.Close()
	b.disconnectedOnce.Do(func() { close(b.disconnected) })
	if call == 2 {
		<-b.releaseSecond
	}
	return err
}

func TestPendingInvalidUpgradeUsesUnusedCloseSlotAndJoinsBoth(t *testing.T) {
	p, _, client := newContinuityPeer(t, 600*time.Millisecond, 0)
	p.renewalRelease = make(chan struct{})
	body := &pendingUpgradeClose{readStarted: make(chan struct{}), firstClose: make(chan struct{}), disconnected: make(chan struct{}), releaseFirst: make(chan struct{}), releaseSecond: make(chan struct{})}
	var one, two sync.Once
	releaseOne := func() { one.Do(func() { close(body.releaseFirst) }) }
	releaseTwo := func() { two.Do(func() { close(body.releaseSecond) }) }
	defer releaseOne()
	defer releaseTwo()
	hc := *p.server.Client()
	transport := hc.Transport
	hc.Transport = observerTransport(func(r *http.Request) (*http.Response, error) {
		response, err := transport.RoundTrip(r)
		if err == nil && response.StatusCode == 101 && r.URL.Query().Get("access_token") == "renewed-token" {
			body.ReadWriteCloser = response.Body.(io.ReadWriteCloser)
			response.Body = body
			response.Header.Set("Sec-WebSocket-Accept", "invalid")
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
	timer := time.NewTimer(max(time.Until(p.plan.Reservation.Billing.RenewableUntil.Add(-2*time.Second)), 0))
	defer timer.Stop()
	<-timer.C
	close(p.renewalRelease)
	_ = awaitContinuity(t, body.readStarted)
	session.Abort()
	_ = awaitContinuity(t, session.Done())
	_ = awaitContinuity(t, body.firstClose)
	deadline := time.NewTimer(max(time.Until(p.plan.Reservation.Billing.RenewableUntil.Add(300*time.Millisecond)), 0))
	defer deadline.Stop()
	select {
	case <-body.disconnected:
	case <-deadline.C:
		t.Fatalf("pending socket missed original cap with unused normal Close slot: attempts=%d", body.calls.Load())
	}
	if body.calls.Load() != 2 || body.peak.Load() != 2 || capacity.Load() != 1 || !errors.Is(session.Err(), runtimepkg.ErrSessionAborted) {
		t.Fatalf("physical attempts/outcome/join ownership changed: %d/%d/%d/%v", body.calls.Load(), body.peak.Load(), capacity.Load(), session.Err())
	}
	assertOwnedTerminalOutcome(t, p, session, protocol.EventError, "session.failed")
	releaseOne()
	select {
	case <-released:
		t.Fatal("joining only the first Close released physical capacity")
	case <-time.After(50 * time.Millisecond):
	}
	releaseTwo()
	_ = awaitContinuity(t, released)
	if capacity.Load() != 0 || body.peak.Load() > 2 {
		t.Fatal("physical join failed or Close concurrency exceeded two")
	}
}
