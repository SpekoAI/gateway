package runtime_test

import (
	"context"
	"crypto/ed25519"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	"github.com/SpekoAI/gateway/providers/geminirealtime"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

// Hold only the return AFTER the real transport received the tightened cap.
// The coordinator cannot yet re-arm its timer; physical expiry must still run.
type heldTightenAdapter struct {
	*geminirealtime.Adapter
	reached, release chan struct{}
}

func (a heldTightenAdapter) Open(ctx context.Context, r runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	stream, err := a.Adapter.Open(ctx, r)
	if err != nil {
		return stream, err
	}
	return &heldTightenStream{ResumableProviderStream: stream.(runtimepkg.ResumableProviderStream), reached: a.reached, release: a.release}, nil
}

type heldTightenStream struct {
	runtimepkg.ResumableProviderStream
	reached, release chan struct{}
	once             sync.Once
}

func (s *heldTightenStream) TightenPhysicalDeadline(deadline time.Time) {
	s.ResumableProviderStream.TightenPhysicalDeadline(deadline)
	s.once.Do(func() { close(s.reached) })
	<-s.release
}
func (s *heldTightenStream) Interrupt() {
	s.ResumableProviderStream.(runtimepkg.InterruptingProviderStream).Interrupt()
}
func (s *heldTightenStream) Abort(ctx context.Context) error {
	return s.ResumableProviderStream.(runtimepkg.AbortingProviderStream).Abort(ctx)
}
func (s *heldTightenStream) TerminalError() error {
	return s.ResumableProviderStream.(runtimepkg.TerminalErrorProviderStream).TerminalError()
}

func TestSQLRefreshRetainsCredentialHorizonAndTightensPhysicalAuthority(t *testing.T) {
	for _, mode := range []string{"expired", "shorter", "backward_control", "unexpired_control"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			p, _, client := newContinuityPeer(t, 12*time.Second, 0, true)
			p.clockSamples = make(chan time.Time, 16)
			p.plan.Route.Credential.ExpiresAt = time.Now().Add(3 * time.Second)
			originalCredentialExpiry := p.plan.Route.Credential.ExpiresAt
			p.envelope.Plan = p.plan.Unsigned()
			p.plan = resignPlan(t, p.private, "continuity", p.envelope, p.plan)
			closed, closeReturn := make(chan struct{}), make(chan struct{})
			var closeOnce sync.Once
			releaseClose := func() { closeOnce.Do(func() { close(closeReturn) }) }
			defer releaseClose()
			hc := *p.server.Client()
			transport := hc.Transport
			hc.Transport = observerTransport(func(r *http.Request) (*http.Response, error) {
				response, err := transport.RoundTrip(r)
				if err == nil && response.StatusCode == 101 {
					response.Body = &heldPhysicalClose{ReadWriteCloser: response.Body.(io.ReadWriteCloser), entered: closed, release: closeReturn}
				}
				return response, err
			})
			google, err := geminirealtime.New(geminirealtime.Config{HTTPClient: &hc, AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
			if err != nil {
				t.Fatal(err)
			}
			tightenReached, tightenReturn := make(chan struct{}), make(chan struct{})
			var tightenOnce sync.Once
			releaseTighten := func() { tightenOnce.Do(func() { close(tightenReturn) }) }
			defer releaseTighten()
			var adapter runtimepkg.Adapter = google
			if mode == "shorter" {
				adapter = heldTightenAdapter{Adapter: google, reached: tightenReached, release: tightenReturn}
			}
			engine, err := runtimepkg.New(runtimepkg.Config{Adapters: []runtimepkg.Adapter{adapter}, Verifier: p.verifier, Now: func() time.Time { return time.Now().Add(time.Duration(p.wallOffset.Load())) }, Telemetry: &p.telemetry})
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
			_ = awaitContinuity(t, p.clockSamples)
			offset := time.Duration(0)
			switch mode {
			case "expired":
				offset = 5 * time.Second
			case "shorter":
				offset = time.Second
			case "backward_control":
				offset = -time.Second
			}
			p.sqlOffset.Store(int64(offset))
			p.wallOffset.Store(int64(time.Minute))
			fresh := awaitContinuity(t, p.clockSamples) // actual revision3 plan-token HTTP sample
			deadline := originalCredentialExpiry
			if refreshed := time.Now().Add(max(originalCredentialExpiry.Sub(fresh), 0)); refreshed.Before(deadline) {
				deadline = refreshed
			}
			if mode == "shorter" {
				_ = awaitContinuity(t, tightenReached)
			}
			if mode != "expired" && mode != "shorter" {
				if err := session.SubmitAudio(runtimepkg.AudioInput{Data: []byte{1, 0}}); err != nil {
					t.Fatal(err)
				}
				_ = awaitContinuity(t, p.inputs)
			}
			timer := time.NewTimer(max(time.Until(deadline.Add(300*time.Millisecond)), 0))
			defer timer.Stop()
			select {
			case <-closed:
			case <-timer.C:
				t.Fatal("trusted SQL refresh failed to tighten actual root socket credential cap")
			}
			releaseTighten()
			select {
			case <-session.Done():
			case <-time.After(300 * time.Millisecond):
				t.Fatal("physical expiry did not publish terminal evidence")
			}
			if !errors.Is(session.Err(), runtimepkg.ErrSessionLeaseExpired) || capacity.Load() != 1 {
				t.Fatalf("expiry lost terminal/join ownership: %v/%d", session.Err(), capacity.Load())
			}
			if mode == "expired" {
				select {
				case <-p.inputs:
					t.Fatal("expired root performed provider input")
				default:
				}
			}
			if !p.plan.Route.Credential.ExpiresAt.Equal(originalCredentialExpiry) {
				t.Fatal("refresh rewrote original signed credential")
			}
			assertOwnedTerminalOutcome(t, p, session, protocol.EventError, "session.failed")
			for _, receipt := range p.telemetry.snapshot() {
				if receipt.Name == "session.failed" && receipt.At.After(time.Now().Add(10*time.Second)) {
					t.Fatal("receipt used stepped host wall clock instead of SQL sample")
				}
			}
			releaseClose()
			_ = awaitContinuity(t, released)
			if capacity.Load() != 0 {
				t.Fatal("joined refreshed socket retained capacity")
			}
		})
	}
}

// Each verification constructs a fresh STOCK verifier, isolating the Engine's
// spent-attempt guard from any nonce cache previously consumed by this test.
type freshStockPerOpen struct{ config runtimepkg.PlanVerifierConfig }

func (v freshStockPerOpen) Verify(ctx context.Context, p protocol.SessionPlan) error {
	verifier, err := runtimepkg.NewPlanVerifier(v.config)
	if err != nil {
		return err
	}
	return verifier.Verify(ctx, p)
}
func (v freshStockPerOpen) VerifyWithClock(ctx context.Context, p protocol.SessionPlan, c func(context.Context) (time.Time, error)) error {
	verifier, err := runtimepkg.NewPlanVerifier(v.config)
	if err != nil {
		return err
	}
	return verifier.VerifyWithClock(ctx, p, c)
}

func TestFundedReplayFencesSurviveSQLRollbackIndependently(t *testing.T) {
	for _, mode := range []string{"stock_fresh_engine", "engine_fresh_stock_verifier", "shared_cache_fresh_verifier"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			p, _, client := newContinuityPeer(t, 900*time.Millisecond, 0, true)
			p.envelope.IssuedAt = time.Now().Add(-time.Millisecond)
			p.envelope.Plan = p.plan.Unsigned()
			p.plan = resignPlan(t, p.private, "continuity", p.envelope, p.plan)
			jwks := newJWKSFixture(t, "continuity", p.private.Public().(ed25519.PublicKey))
			config := runtimepkg.PlanVerifierConfig{JWKSURL: jwks.URL, Issuer: p.envelope.Issuer, Audience: "speko-runtime", ClockSkew: time.Nanosecond, AllowInsecureJWKS: true, MaxReplayEntries: 1}
			if mode == "shared_cache_fresh_verifier" {
				config.ReplayCache = runtimepkg.NewMemoryReplayCache(time.Now)
			}
			newVerifier := func() runtimepkg.PlanVerifier {
				v, err := runtimepkg.NewPlanVerifier(config)
				if err != nil {
					t.Fatal(err)
				}
				return v
			}
			verifier := newVerifier()
			if mode == "engine_fresh_stock_verifier" {
				verifier = freshStockPerOpen{config}
			}
			newEngine := func(v runtimepkg.PlanVerifier) *runtimepkg.Engine {
				e, err := runtimepkg.New(runtimepkg.Config{Adapters: []runtimepkg.Adapter{p.adapter}, Verifier: v, Telemetry: &p.telemetry, MaxReplayEntries: 1})
				if err != nil {
					t.Fatal(err)
				}
				return e
			}
			engine := newEngine(verifier)
			var capacity atomic.Int32
			released := make(chan struct{}, 2)
			r := continuityRequest(p, client)
			r.TrackVerifiedCapacity = func(*protocol.CapacityAttribution, protocol.CredentialSource) (func(), error) {
				capacity.Add(1)
				return func() { capacity.Add(-1); released <- struct{}{} }, nil
			}
			session, err := engine.Open(t.Context(), r)
			if err != nil {
				t.Fatal(err)
			}
			_ = awaitContinuity(t, session.Events())
			session.Abort()
			_ = awaitContinuity(t, session.Done())
			_ = awaitContinuity(t, released)
			assertOwnedTerminalOutcome(t, p, session, protocol.EventError, "session.failed")
			timer := time.NewTimer(max(time.Until(p.plan.ExpiresAt.Add(100*time.Millisecond)), 0))
			defer timer.Stop()
			<-timer.C
			p.sqlOffset.Store(int64(-800 * time.Millisecond))
			clocks := p.clockCalls.Load()
			switch mode {
			case "stock_fresh_engine":
				engine = newEngine(verifier)
			case "shared_cache_fresh_verifier":
				engine = newEngine(newVerifier())
			}
			second, err := engine.Open(t.Context(), r)
			if second != nil {
				second.Abort()
				_ = awaitContinuity(t, released)
			}
			if second != nil || !errors.Is(err, runtimepkg.ErrPlanReplay) {
				t.Fatalf("spent funded authority revived across SQL rollback: session=%v err=%v", second != nil, err)
			}
			wantClocks := clocks
			if mode == "engine_fresh_stock_verifier" {
				wantClocks++
			}
			if p.clockCalls.Load() != wantClocks {
				t.Fatalf("replay fence ran at wrong boundary: clock calls%d want%d", p.clockCalls.Load(), wantClocks)
			}
			p.mu.Lock()
			sockets, mints := p.sockets, p.mints
			p.mu.Unlock()
			if sockets != 1 || mints != 0 || capacity.Load() != 0 {
				t.Fatal("replay acquired provider work or physical capacity")
			}
			failed := 0
			for _, event := range p.telemetry.snapshot() {
				if event.Name == "session.failed" {
					failed++
				}
			}
			if failed != 1 {
				t.Fatal("replay duplicated terminal failure evidence")
			}
			// Reclaimed space remains usable for a genuinely distinct signed root,
			// not a retry or re-signing of the spent attempt. SQL stays rolled back.
			fresh := p.plan
			fresh.PlanID += "-fresh"
			fresh.AttemptID += "-fresh"
			fresh.Reservation.ID += "-fresh"
			fresh.ExpiresAt = fresh.ExpiresAt.Add(2 * time.Second)
			fresh.Reservation.LeaseExpiresAt = fresh.ExpiresAt
			billing := *fresh.Reservation.Billing
			billing.RenewableUntil = fresh.ExpiresAt.Add(4 * time.Second)
			fresh.Reservation.Billing = &billing
			fresh.Route.Credential = &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "fresh-owned-root", ExpiresAt: fresh.ExpiresAt}
			fresh.Telemetry.Token = "gwt1." + strings.Repeat("b", 43)
			envelope := p.envelope
			envelope.ID += "-fresh"
			envelope.IssuedAt = time.Now().Add(-800*time.Millisecond - time.Millisecond)
			envelope.ExpiresAt = fresh.ExpiresAt
			envelope.Plan = fresh.Unsigned()
			p.plan = resignPlan(t, p.private, "continuity", envelope, fresh)
			r.Plan = p.plan
			freshSession, err := engine.Open(t.Context(), r)
			if err != nil {
				t.Fatalf("retirement did not reclaim bounded space for distinct signed authority: %v", err)
			}
			freshSession.Abort()
			_ = awaitContinuity(t, released)
			if capacity.Load() != 0 {
				t.Fatal("fresh signed control lost physical join ownership")
			}
		})
	}
}

type ttlOnlyNonceCache struct{ runtimepkg.ReplayCache }

func (ttlOnlyNonceCache) TryUseFor(context.Context, string, time.Duration) (bool, error) {
	return true, nil
}

func TestFundedVerifierRefusesTTLOnlyCustomReplayBeforeClock(t *testing.T) {
	p, _, _ := newContinuityPeer(t, time.Second, 0, true)
	jwks := newJWKSFixture(t, "continuity", p.private.Public().(ed25519.PublicKey))
	verifier, err := runtimepkg.NewPlanVerifier(runtimepkg.PlanVerifierConfig{JWKSURL: jwks.URL, Issuer: p.envelope.Issuer, Audience: "speko-runtime", AllowInsecureJWKS: true, ReplayCache: ttlOnlyNonceCache{runtimepkg.NewMemoryReplayCache(time.Now)}})
	if err != nil {
		t.Fatal(err)
	}
	err = verifier.VerifyWithClock(t.Context(), p.plan, func(context.Context) (time.Time, error) {
		t.Fatal("TTL-only cache reached SQL clock I/O")
		return time.Time{}, nil
	})
	if err == nil || !strings.Contains(err.Error(), "rollback-safe replay cache") {
		t.Fatalf("TTL-only custom cache was not refused precisely: %v", err)
	}
	if p.clockCalls.Load() != 0 {
		t.Fatal("unsupported nonce cache caused clock request")
	}
}
