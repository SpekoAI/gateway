package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/controlplane"
	"github.com/SpekoAI/gateway/protocol"
	"github.com/SpekoAI/gateway/providers/geminirealtime"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

type registrationBlockedWriteAdapter struct {
	ownedContinuationAdapter
	entered, release, resumed, resumeRelease chan struct{}
}

func (a registrationBlockedWriteAdapter) Open(ctx context.Context, r runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	s, err := a.ownedContinuationAdapter.Open(ctx, r)
	if err != nil {
		return s, err
	}
	return &registrationBlockedWriteStream{ResumableProviderStream: s.(runtimepkg.ResumableProviderStream), entered: a.entered, release: a.release, resumed: a.resumed, resumeRelease: a.resumeRelease}, nil
}

type registrationBlockedWriteStream struct {
	runtimepkg.ResumableProviderStream
	entered, release, resumed, resumeRelease chan struct{}
}

func (s *registrationBlockedWriteStream) WriteAudio(ctx context.Context, b []byte) error {
	close(s.entered)
	<-s.release
	return nil
}
func (s *registrationBlockedWriteStream) Resume(ctx context.Context, cred protocol.DelegatedCredential) error {
	err := s.ResumableProviderStream.Resume(ctx, cred)
	if err != nil {
		return err
	}
	close(s.resumed)
	<-s.resumeRelease
	return nil
}
func (s *registrationBlockedWriteStream) Interrupt() {
	s.ResumableProviderStream.(runtimepkg.InterruptingProviderStream).Interrupt()
}
func (s *registrationBlockedWriteStream) Abort(ctx context.Context) error {
	return s.ResumableProviderStream.(runtimepkg.AbortingProviderStream).Abort(ctx)
}
func (s *registrationBlockedWriteStream) TerminalError() error {
	return s.ResumableProviderStream.(runtimepkg.TerminalErrorProviderStream).TerminalError()
}

func TestRegistrationRechecksLatestSQLAfterProviderLock(t *testing.T) {
	for _, mode := range []string{"expired", "shorter", "backward"} {
		t.Run(mode, func(t *testing.T) { registrationAfterProviderLock(t, mode) })
	}
}
func registrationAfterProviderLock(t *testing.T, mode string) {
	p, _, client := newContinuityPeer(t, 12*time.Second, 0)
	p.plan.Reservation.Billing.RenewableUntil = p.plan.Reservation.LeaseExpiresAt.Add(60 * time.Second)
	p.envelope.Plan = p.plan.Unsigned()
	p.plan = resignPlan(t, p.private, "continuity", p.envelope, p.plan)
	renewed := p.plan.Reservation.LeaseExpiresAt.Add(4 * time.Second)
	p.clockSamples = make(chan time.Time, 8)
	p.renewalRelease = make(chan struct{})
	p.renewalHandler = func(w http.ResponseWriter, r *http.Request) {
		p.renewalStarted <- struct{}{}
		select {
		case <-p.renewalRelease:
		case <-r.Context().Done():
			return
		}
		_ = json.NewEncoder(w).Encode(controlplane.EntitlementRenewal{EntitlementID: "owned-slice", SessionID: p.plan.SessionID, AttemptID: p.plan.AttemptID, ReservationID: p.plan.Reservation.ID, Sequence: 1, PreviousExpiresAt: p.plan.Reservation.LeaseExpiresAt, LeaseExpiresAt: renewed, AuthorizedUnits: 4, MaximumAmountMicros: 100, Currency: "USD", Credential: protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "renewed-token", ExpiresAt: renewed}})
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
	a := registrationBlockedWriteAdapter{ownedContinuationAdapter: ownedContinuationAdapter{google}, entered: make(chan struct{}), release: make(chan struct{}), resumed: make(chan struct{}), resumeRelease: make(chan struct{})}
	var once, resumeOnce sync.Once
	release := func() { once.Do(func() { close(a.release) }) }
	releaseResume := func() { resumeOnce.Do(func() { close(a.resumeRelease) }) }
	defer release()
	defer releaseResume()
	e, err := runtimepkg.New(runtimepkg.Config{Adapters: []runtimepkg.Adapter{a}, Verifier: p.verifier, Now: func() time.Time { return time.Now().Add(time.Duration(p.wallOffset.Load())) }, Telemetry: &p.telemetry})
	if err != nil {
		t.Fatal(err)
	}
	s, err := e.Open(t.Context(), continuityRequest(p, client))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Abort()
	_ = awaitContinuity(t, s.Events())
	_ = awaitContinuity(t, p.connected)
	_ = awaitContinuity(t, p.clockSamples)
	_ = awaitContinuity(t, p.renewalStarted)
	if err := s.SubmitAudio(runtimepkg.AudioInput{Data: []byte{1, 0}}); err != nil {
		t.Fatal(err)
	}
	_ = awaitContinuity(t, a.entered)
	close(p.renewalRelease)
	<-time.After(400 * time.Millisecond) // renewal computes its deadline and waits for providerMu
	offset := 18 * time.Second
	if mode == "shorter" {
		offset = 13 * time.Second
	}
	if mode == "backward" {
		offset = -30 * time.Second
	}
	p.sqlOffset.Store(int64(offset))
	p.wallOffset.Store(int64(time.Minute))
	fresh := awaitContinuity(t, p.clockSamples)
	if mode == "expired" && (!fresh.After(renewed) || !fresh.Before(p.plan.Reservation.Billing.RenewableUntil)) {
		t.Fatalf("invalid sample %v", fresh)
	}
	if mode == "shorter" && !fresh.Before(renewed) {
		t.Fatalf("shorter sample already expired: %v", fresh)
	}
	if mode != "backward" {
		_ = awaitContinuity(t, p.closed)
	} // the original socket physically closed under the installed sample
	release()
	if mode == "expired" {
		select {
		case <-p.connected:
			t.Fatal("expired replacement entered provider setup after the latest SQL sample was installed before registration")
		case <-s.Done():
			if !errors.Is(s.Err(), runtimepkg.ErrSessionLeaseExpired) {
				t.Fatalf("original lease cause lost: %v", s.Err())
			}
		case <-time.After(700 * time.Millisecond):
			t.Fatal("expired replacement did not terminate within the original observation window")
		}
		return
	}
	_ = awaitContinuity(t, p.connected)
	_ = awaitContinuity(t, a.resumed)
	window := 700 * time.Millisecond
	if mode == "shorter" {
		window = max(renewed.Sub(fresh), 0) + 300*time.Millisecond
	}
	select {
	case <-closed:
		if mode == "backward" {
			t.Fatal("backward sample shortened the original replacement cap")
		}
	case <-time.After(window):
		if mode == "shorter" {
			t.Fatal("latest SQL sample before registration did not shorten the exact physical cap while Resume return was held")
		}
	}
}
