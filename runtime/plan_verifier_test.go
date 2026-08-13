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

func TestPlanVerifierRejectsTamperedWrongAudienceExpiredAndReplayedPlans(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 1, 12, 0, 0, 0, time.UTC)
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	server := newJWKSFixture(t, "key-1", public)
	verifier := newVerifier(t, server.URL, now)

	t.Run("tampered route", func(t *testing.T) {
		plan := signedPlan(t, private, "key-1", now, "jti-tampered", "speko-runtime", now.Add(time.Minute))
		plan.Route.Model = "untrusted-model"
		if err := verifier.Verify(context.Background(), plan); !errors.Is(err, runtimepkg.ErrPlanSignature) {
			t.Fatalf("Verify tampered plan = %v, want ErrPlanSignature", err)
		}
	})

	t.Run("wrong audience", func(t *testing.T) {
		plan := signedPlan(t, private, "key-1", now, "jti-audience", "different-runtime", now.Add(time.Minute))
		if err := verifier.Verify(context.Background(), plan); !errors.Is(err, runtimepkg.ErrPlanSignature) {
			t.Fatalf("Verify wrong audience = %v, want ErrPlanSignature", err)
		}
	})

	t.Run("expired envelope", func(t *testing.T) {
		plan := signedPlan(t, private, "key-1", now, "jti-expired", "speko-runtime", now.Add(-time.Minute))
		if err := verifier.Verify(context.Background(), plan); !errors.Is(err, runtimepkg.ErrPlanSignature) {
			t.Fatalf("Verify expired envelope = %v, want ErrPlanSignature", err)
		}
	})

	t.Run("replay", func(t *testing.T) {
		plan := signedPlan(t, private, "key-1", now, "jti-once", "speko-runtime", now.Add(time.Minute))
		if err := verifier.Verify(context.Background(), plan); err != nil {
			t.Fatalf("first Verify: %v", err)
		}
		if err := verifier.Verify(context.Background(), plan); !errors.Is(err, runtimepkg.ErrPlanReplay) {
			t.Fatalf("second Verify = %v, want ErrPlanReplay", err)
		}
	})
}

func TestPlanVerifierCachesJWKSAndRefreshesForSigningKeyRotation(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 1, 12, 0, 0, 0, time.UTC)
	publicOne, privateOne, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate first key: %v", err)
	}
	publicTwo, privateTwo, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate second key: %v", err)
	}
	var keys atomic.Value
	keys.Store(jwksTestKey{ID: "key-1", Public: publicOne})
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		key := keys.Load().(jwksTestKey)
		writer.Header().Set("Cache-Control", "max-age=300")
		_ = json.NewEncoder(writer).Encode(map[string]any{"keys": []any{map[string]string{
			"kty": "OKP", "kid": key.ID, "alg": "EdDSA", "use": "sig", "crv": "Ed25519",
			"x": base64.RawURLEncoding.EncodeToString(key.Public),
		}}})
	}))
	t.Cleanup(server.Close)
	verifier := newVerifier(t, server.URL, now)

	if err := verifier.Verify(context.Background(), signedPlan(t, privateOne, "key-1", now, "jti-key-1", "speko-runtime", now.Add(time.Minute))); err != nil {
		t.Fatalf("verify original key: %v", err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("jwks requests after original key = %d, want 1", got)
	}
	keys.Store(jwksTestKey{ID: "key-2", Public: publicTwo})
	if err := verifier.Verify(context.Background(), signedPlan(t, privateTwo, "key-2", now, "jti-key-2", "speko-runtime", now.Add(time.Minute))); err != nil {
		t.Fatalf("verify rotated key: %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("jwks requests after rotation = %d, want 2", got)
	}
	if err := verifier.Verify(context.Background(), signedPlan(t, privateTwo, "key-2", now, "jti-key-3", "speko-runtime", now.Add(time.Minute))); err != nil {
		t.Fatalf("verify cached rotated key: %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("jwks cache was bypassed: %d requests, want 2", got)
	}
}

type jwksTestKey struct {
	ID     string
	Public ed25519.PublicKey
}

func newJWKSFixture(t *testing.T, id string, public ed25519.PublicKey) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Cache-Control", "max-age=300")
		_ = json.NewEncoder(writer).Encode(map[string]any{"keys": []any{map[string]string{
			"kty": "OKP", "kid": id, "alg": "EdDSA", "use": "sig", "crv": "Ed25519",
			"x": base64.RawURLEncoding.EncodeToString(public),
		}}})
	}))
	t.Cleanup(server.Close)
	return server
}

func newVerifier(t *testing.T, jwksURL string, now time.Time) *runtimepkg.JWKSPlanVerifier {
	t.Helper()
	verifier, err := runtimepkg.NewPlanVerifier(runtimepkg.PlanVerifierConfig{
		JWKSURL:           jwksURL,
		Issuer:            "https://control.speko.test",
		Audience:          "speko-runtime",
		AllowInsecureJWKS: true,
		CacheTTL:          time.Hour,
		ClockSkew:         time.Second,
		Now:               func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}
	return verifier
}

func signedPlan(t *testing.T, private ed25519.PrivateKey, keyID string, now time.Time, jti, audience string, expiresAt time.Time) protocol.SessionPlan {
	t.Helper()
	plan := testBYOKPlan(now, expiresAt)
	envelope := protocol.SessionPlanEnvelope{
		Issuer: "https://control.speko.test", Audience: []string{audience}, IssuedAt: now.Add(-time.Second), ExpiresAt: expiresAt, ID: jti, Plan: plan.Unsigned(),
	}
	return resignPlan(t, private, keyID, envelope, plan)
}

func resignPlan(t *testing.T, private ed25519.PrivateKey, keyID string, envelope protocol.SessionPlanEnvelope, plan protocol.SessionPlan) protocol.SessionPlan {
	t.Helper()
	payload, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	header, err := json.Marshal(map[string]string{"alg": "EdDSA", "kid": keyID, "typ": runtimepkg.SessionPlanJWSType})
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	encodedHeader := base64.RawURLEncoding.EncodeToString(header)
	encodedPayload := base64.RawURLEncoding.EncodeToString(payload)
	signingInput := encodedHeader + "." + encodedPayload
	plan.Signature = signingInput + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(private, []byte(signingInput)))
	return plan
}

func testBYOKPlan(now, expiresAt time.Time) protocol.SessionPlan {
	return protocol.SessionPlan{
		PlanID: "plan-test", SessionID: "session-test", AttemptID: "attempt-test", ExpiresAt: expiresAt,
		Execution:    protocol.Execution{Placement: protocol.PlacementEmbedded, ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK},
		Route:        protocol.PlanRoute{Provider: "mock", Model: "model", Adapter: "mock.stt.v1", Transport: protocol.TransportWebSocket, Endpoint: "wss://provider.speko.test/stream"},
		Reservation:  protocol.Reservation{ID: "reservation-test", LeaseDurationSeconds: 60, LeaseExpiresAt: expiresAt.Add(time.Minute), Concurrency: protocol.ConcurrencyReservation{LeaseID: "lease-test", Slots: 1}, Usage: protocol.UsageReservation{Unit: protocol.UsageUnitDurationSeconds, AuthorizedUnits: 60}},
		Telemetry:    protocol.Telemetry{Endpoint: "https://control.speko.test/v1/runtime-events", Token: "telemetry-token", FlushIntervalMS: 5_000},
		Requirements: protocol.Requirements{Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision, RuntimeVersion: "test"},
	}
}
