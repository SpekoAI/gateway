package runtime_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

// Each verifier uses the public constructor, real signature verification,
// unique JTIs and an owned HTTP JWKS. No cache timestamps are rewritten.
func jwksLifetimeVerifier(t *testing.T, kind, endpoint string, wall func() time.Time, now time.Time) func(ed25519.PrivateKey, string, string) error {
	t.Helper()
	config := runtimepkg.PlanVerifierConfig{
		JWKSURL: endpoint, Issuer: "https://control.speko.test", Audience: "speko-runtime",
		AllowInsecureJWKS: true, Now: wall, MaxCacheTTL: time.Second,
	}
	if kind == "relay" {
		config.Audience = protocol.RelayPlanAudience
		verifier, err := runtimepkg.NewRelayPlanVerifier(config)
		if err != nil {
			t.Fatal(err)
		}
		return func(key ed25519.PrivateKey, kid, jti string) error {
			return verifier.Verify(t.Context(), signedRelayPlan(t, key, kid, now, jti, config.Issuer, config.Audience, now.Add(time.Minute)))
		}
	}
	verifier, err := runtimepkg.NewPlanVerifier(config)
	if err != nil {
		t.Fatal(err)
	}
	return func(key ed25519.PrivateKey, kid, jti string) error {
		plan := signedPlan(t, key, kid, now, jti, config.Audience, now.Add(time.Minute))
		if kind == "clocked_session" {
			return verifier.VerifyWithClock(t.Context(), plan, func(context.Context) (time.Time, error) { return now, nil })
		}
		return verifier.Verify(t.Context(), plan)
	}
}

func TestJWKSKeyRemovalExpiresAcrossWallRollback(t *testing.T) {
	for _, kind := range []string{"session", "clocked_session", "relay"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			oldPublic, oldPrivate, err := ed25519.GenerateKey(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			newPublic, newPrivate, err := ed25519.GenerateKey(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			var mode, requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.Header().Set("Cache-Control", "max-age=1")
				switch mode.Load() {
				case 1:
					if r.Header.Get("If-None-Match") != `"new"` {
						t.Error("revalidation did not send the cached ETag")
					}
					// Revalidation may renew authority, but cannot escape the
					// configured elapsed maximum even with a longer max-age.
					w.Header().Set("Cache-Control", "max-age=3600")
					w.WriteHeader(http.StatusNotModified)
					return
				case 3:
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				kid, public := "old", oldPublic
				if mode.Load() == 2 {
					kid, public = "new", newPublic
				}
				w.Header().Set("ETag", `"`+kid+`"`)
				_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]string{
					"kty": "OKP", "kid": kid, "alg": "EdDSA", "crv": "Ed25519", "x": base64.RawURLEncoding.EncodeToString(public),
				}}})
			}))
			defer server.Close()
			now := time.Date(2026, time.November, 1, 12, 0, 0, 0, time.UTC)
			var offset atomic.Int64
			verify := jwksLifetimeVerifier(t, kind, server.URL, func() time.Time { return now.Add(time.Duration(offset.Load())) }, now)
			if err := verify(oldPrivate, "old", "initial"); err != nil {
				t.Fatal(err)
			}
			offset.Store(int64(-time.Hour))
			if err := verify(oldPrivate, "old", "cached"); err != nil {
				t.Fatal(err)
			}
			if requests.Load() != 1 {
				t.Fatal("fresh cache hit must not fetch or reset its lifetime")
			}
			mode.Store(2) // the authority removes the original signing key
			time.Sleep(1100 * time.Millisecond)
			if err := verify(oldPrivate, "old", "removed"); !errors.Is(err, runtimepkg.ErrPlanSignature) || requests.Load() != 2 {
				t.Fatalf("removed key accepted past max-age=1 after rollback: requests=%d, err=%v", requests.Load(), err)
			}
			if err := verify(newPrivate, "new", "replacement"); err != nil || requests.Load() != 2 {
				t.Fatalf("replacement key must use refreshed set: requests=%d, err=%v", requests.Load(), err)
			}
			mode.Store(1)
			offset.Store(int64(-2 * time.Hour))
			time.Sleep(1100 * time.Millisecond)
			if err := verify(newPrivate, "new", "revalidated"); err != nil || requests.Load() != 3 {
				t.Fatalf("elapsed TTL must trigger 304 revalidation: requests=%d, err=%v", requests.Load(), err)
			}
			mode.Store(3)
			offset.Store(int64(-3 * time.Hour))
			time.Sleep(1100 * time.Millisecond)
			for _, jti := range []string{"failed-refresh", "retry-failed-refresh"} {
				if err := verify(newPrivate, "new", jti); err == nil {
					t.Fatal("failed refresh extended expired key authority")
				}
			}
			if requests.Load() != 5 {
				t.Fatalf("failed refresh must not reset TTL: requests=%d", requests.Load())
			}
			mode.Store(2)
			if err := verify(newPrivate, "new", "recovered"); err != nil || requests.Load() != 6 {
				t.Fatalf("retry must fetch current authority: requests=%d, err=%v", requests.Load(), err)
			}
		})
	}
}

func TestJWKSRefreshCannotRestartExpiredRetrievalLifetime(t *testing.T) {
	for _, kind := range []string{"session", "clocked_session", "relay"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			public, private, err := ed25519.GenerateKey(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Cache-Control", "max-age=1")
				w.WriteHeader(http.StatusOK)
				w.(http.Flusher).Flush()
				select {
				case <-time.After(1100 * time.Millisecond):
				case <-r.Context().Done():
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]string{
					"kty": "OKP", "kid": "slow", "alg": "EdDSA", "crv": "Ed25519", "x": base64.RawURLEncoding.EncodeToString(public),
				}}})
			}))
			defer server.Close()
			now := time.Date(2026, time.November, 1, 12, 0, 0, 0, time.UTC)
			verify := jwksLifetimeVerifier(t, kind, server.URL, func() time.Time { return now }, now)
			if err := verify(private, "slow", "expired-during-refresh"); !errors.Is(err, runtimepkg.ErrPlanSignature) {
				t.Fatalf("slow response must not restart key lifetime at completion: %v", err)
			}
		})
	}
}
