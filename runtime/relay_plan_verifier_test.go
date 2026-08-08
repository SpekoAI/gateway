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
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

func TestRelayPlanVerifierAcceptsValidPlansAndRejectsTamperedClaims(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 1, 12, 0, 0, 0, time.UTC)
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	server := newJWKSFixture(t, "relay-key-1", public)

	t.Run("valid plan", func(t *testing.T) {
		verifier := newRelayVerifier(t, server.URL, now, nil)
		plan := signedRelayPlan(t, private, "relay-key-1", now, "relay-use_valid", "https://control.speko.test", protocol.RelayPlanAudience, now.Add(time.Minute))
		if err := verifier.Verify(context.Background(), plan); err != nil {
			t.Fatalf("Verify valid relay plan: %v", err)
		}
	})

	t.Run("tampered plan breaks the byte binding", func(t *testing.T) {
		verifier := newRelayVerifier(t, server.URL, now, nil)
		plan := signedRelayPlan(t, private, "relay-key-1", now, "relay-use_tampered", "https://control.speko.test", protocol.RelayPlanAudience, now.Add(time.Minute))
		plan.Model = "untrusted-model"
		if err := verifier.Verify(context.Background(), plan); !errors.Is(err, runtimepkg.ErrPlanSignature) {
			t.Fatalf("Verify tampered relay plan = %v, want ErrPlanSignature", err)
		}
	})

	t.Run("wrong audience", func(t *testing.T) {
		verifier := newRelayVerifier(t, server.URL, now, nil)
		plan := signedRelayPlan(t, private, "relay-key-1", now, "relay-use_audience", "https://control.speko.test", "speko-runtime", now.Add(time.Minute))
		if err := verifier.Verify(context.Background(), plan); !errors.Is(err, runtimepkg.ErrPlanSignature) {
			t.Fatalf("Verify wrong audience = %v, want ErrPlanSignature", err)
		}
	})

	t.Run("wrong issuer", func(t *testing.T) {
		verifier := newRelayVerifier(t, server.URL, now, nil)
		plan := signedRelayPlan(t, private, "relay-key-1", now, "relay-use_issuer", "https://impostor.speko.test", protocol.RelayPlanAudience, now.Add(time.Minute))
		if err := verifier.Verify(context.Background(), plan); !errors.Is(err, runtimepkg.ErrPlanSignature) {
			t.Fatalf("Verify wrong issuer = %v, want ErrPlanSignature", err)
		}
	})

	t.Run("unknown kid", func(t *testing.T) {
		verifier := newRelayVerifier(t, server.URL, now, nil)
		plan := signedRelayPlan(t, private, "relay-key-unknown", now, "relay-use_kid", "https://control.speko.test", protocol.RelayPlanAudience, now.Add(time.Minute))
		if err := verifier.Verify(context.Background(), plan); !errors.Is(err, runtimepkg.ErrPlanSignature) {
			t.Fatalf("Verify unknown kid = %v, want ErrPlanSignature", err)
		}
	})

	t.Run("expired plan", func(t *testing.T) {
		verifier := newRelayVerifier(t, server.URL, now, nil)
		plan := signedRelayPlan(t, private, "relay-key-1", now, "relay-use_expired", "https://control.speko.test", protocol.RelayPlanAudience, now.Add(-time.Minute))
		if err := verifier.Verify(context.Background(), plan); !errors.Is(err, runtimepkg.ErrPlanSignature) {
			t.Fatalf("Verify expired relay plan = %v, want ErrPlanSignature", err)
		}
	})
}

// A signature minted for one control-plane artifact must never authorize the
// other, even under a shared signing key: the protected header's typ is the
// guard, and it must hold in both directions.
func TestRelayAndSessionVerifiersRejectEachOthersJWSTypes(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 1, 12, 0, 0, 0, time.UTC)
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	server := newJWKSFixture(t, "shared-key", public)

	t.Run("session-plan typ rejected by the relay verifier", func(t *testing.T) {
		verifier := newRelayVerifier(t, server.URL, now, nil)
		plan := testRelayPlan(now.Add(time.Minute))
		envelope := relayEnvelope(now, "relay-use_session-typ", "https://control.speko.test", protocol.RelayPlanAudience, plan)
		plan.Signature = signCompactJWS(t, private, "shared-key", runtimepkg.SessionPlanJWSType, envelope)
		if err := verifier.Verify(context.Background(), plan); !errors.Is(err, runtimepkg.ErrPlanSignature) {
			t.Fatalf("Verify session-plan typ = %v, want ErrPlanSignature", err)
		}
	})

	t.Run("relay typ rejected by the session verifier", func(t *testing.T) {
		verifier := newVerifier(t, server.URL, now)
		plan := testBYOKPlan(now, now.Add(time.Minute))
		envelope := protocol.SessionPlanEnvelope{
			Issuer: "https://control.speko.test", Audience: []string{"speko-runtime"}, IssuedAt: now.Add(-time.Second), ExpiresAt: plan.ExpiresAt, ID: "jti-relay-typ", Plan: plan.Unsigned(),
		}
		plan.Signature = signCompactJWS(t, private, "shared-key", protocol.RelayPlanJWSType, envelope)
		if err := verifier.Verify(context.Background(), plan); !errors.Is(err, runtimepkg.ErrPlanSignature) {
			t.Fatalf("Verify relay typ = %v, want ErrPlanSignature", err)
		}
	})
}

func TestRelayPlanVerifierReplaySemanticsFollowTheConfiguredCache(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 1, 12, 0, 0, 0, time.UTC)
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	server := newJWKSFixture(t, "relay-key-1", public)

	t.Run("memory cache consumes the jti once", func(t *testing.T) {
		verifier := newRelayVerifier(t, server.URL, now, runtimepkg.NewMemoryReplayCache(func() time.Time { return now }))
		plan := signedRelayPlan(t, private, "relay-key-1", now, "relay-use_once", "https://control.speko.test", protocol.RelayPlanAudience, now.Add(time.Minute))
		if err := verifier.Verify(context.Background(), plan); err != nil {
			t.Fatalf("first Verify: %v", err)
		}
		if err := verifier.Verify(context.Background(), plan); !errors.Is(err, runtimepkg.ErrPlanReplay) {
			t.Fatalf("second Verify = %v, want ErrPlanReplay", err)
		}
	})

	// The connector contract: Verify with a pass-through cache never burns
	// the jti, so a plan can be re-verified after a routing self-check
	// rejected it elsewhere. Explicit jti consumption is the caller's job.
	t.Run("pass-through cache leaves the jti unconsumed", func(t *testing.T) {
		verifier := newRelayVerifier(t, server.URL, now, runtimepkg.PassthroughReplayCache{})
		plan := signedRelayPlan(t, private, "relay-key-1", now, "relay-use_passthrough", "https://control.speko.test", protocol.RelayPlanAudience, now.Add(time.Minute))
		if err := verifier.Verify(context.Background(), plan); err != nil {
			t.Fatalf("first Verify: %v", err)
		}
		if err := verifier.Verify(context.Background(), plan); err != nil {
			t.Fatalf("repeat Verify with pass-through cache: %v", err)
		}
	})
}

// Algorithm confusion has two verifier guards and no third: the protected
// header's alg must sit inside the parse-time allowlist, and a published JWK
// that names an alg must agree with the header before any signature math
// runs. verifyJWS itself fails closed on algorithms outside its switch, so a
// future allowlist addition that forgets to extend the switch rejects every
// JWS claiming the new name instead of accepting them all. Pin all of it
// through the relay path.
func TestRelayPlanVerifierRejectsAlgorithmConfusion(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 1, 12, 0, 0, 0, time.UTC)
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	server := newJWKSFixture(t, "relay-key-1", public)

	// Every artifact below carries a REAL Ed25519 signature over its signing
	// input; only the claimed algorithm lies. Acceptance of any of them would
	// mean a header claim, not a key, decided what counts as a signature.
	for _, alg := range []string{"ES256", "none"} {
		t.Run("header claims alg "+alg, func(t *testing.T) {
			verifier := newRelayVerifier(t, server.URL, now, nil)
			plan := testRelayPlan(now.Add(time.Minute))
			envelope := relayEnvelope(now, "relay-use_alg-"+alg, "https://control.speko.test", protocol.RelayPlanAudience, plan)
			plan.Signature = signCompactJWSWithAlg(t, private, "relay-key-1", protocol.RelayPlanJWSType, alg, envelope)
			if err := verifier.Verify(context.Background(), plan); !errors.Is(err, runtimepkg.ErrPlanSignature) {
				t.Fatalf("Verify alg %s = %v, want ErrPlanSignature", alg, err)
			}
		})
	}

	// An EdDSA header over an Ed25519 signature is internally consistent,
	// but the JWKS published the kid as an RS256 key: the mismatch branch
	// must reject before the (otherwise valid) Ed25519 verification can run.
	t.Run("jwks key alg disagrees with the header", func(t *testing.T) {
		mismatchServer := newJWKSFixtureWithAlg(t, "relay-key-rs", "RS256", public)
		verifier := newRelayVerifier(t, mismatchServer.URL, now, nil)
		plan := testRelayPlan(now.Add(time.Minute))
		envelope := relayEnvelope(now, "relay-use_alg-mismatch", "https://control.speko.test", protocol.RelayPlanAudience, plan)
		plan.Signature = signCompactJWSWithAlg(t, private, "relay-key-rs", protocol.RelayPlanJWSType, "EdDSA", envelope)
		if err := verifier.Verify(context.Background(), plan); !errors.Is(err, runtimepkg.ErrPlanSignature) {
			t.Fatalf("Verify with mismatched jwks alg = %v, want ErrPlanSignature", err)
		}
	})
}

// The relay signing key lives in a dedicated JWKS, separate from the
// session-plan set. Each verifier must resolve kids only against its own
// document so a key published for one artifact class cannot vouch for the
// other, no matter which typ the JWS claims.
func TestRelayAndSessionVerifiersResolveKidsFromTheirOwnJWKS(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 1, 12, 0, 0, 0, time.UTC)
	sessionPublic, sessionPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate session key: %v", err)
	}
	relayPublic, relayPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate relay key: %v", err)
	}
	sessionServer := newJWKSFixture(t, "session-key", sessionPublic)
	relayServer := newJWKSFixture(t, "relay-key", relayPublic)
	sessionVerifier := newVerifier(t, sessionServer.URL, now)
	relayVerifier := newRelayVerifier(t, relayServer.URL, now, nil)

	if err := relayVerifier.Verify(context.Background(), signedRelayPlan(t, relayPrivate, "relay-key", now, "relay-use_own-key", "https://control.speko.test", protocol.RelayPlanAudience, now.Add(time.Minute))); err != nil {
		t.Fatalf("relay verifier must accept its own key: %v", err)
	}
	if err := relayVerifier.Verify(context.Background(), signedRelayPlan(t, sessionPrivate, "session-key", now, "relay-use_foreign-key", "https://control.speko.test", protocol.RelayPlanAudience, now.Add(time.Minute))); !errors.Is(err, runtimepkg.ErrPlanSignature) {
		t.Fatalf("relay verifier accepted a session-set kid: %v, want ErrPlanSignature", err)
	}
	if err := sessionVerifier.Verify(context.Background(), signedPlan(t, sessionPrivate, "session-key", now, "jti-own-key", "speko-runtime", now.Add(time.Minute))); err != nil {
		t.Fatalf("session verifier must accept its own key: %v", err)
	}
	if err := sessionVerifier.Verify(context.Background(), signedPlan(t, relayPrivate, "relay-key", now, "jti-foreign-key", "speko-runtime", now.Add(time.Minute))); !errors.Is(err, runtimepkg.ErrPlanSignature) {
		t.Fatalf("session verifier accepted a relay-set kid: %v, want ErrPlanSignature", err)
	}
}

func newRelayVerifier(t *testing.T, jwksURL string, now time.Time, cache runtimepkg.ReplayCache) *runtimepkg.RelayPlanVerifier {
	t.Helper()
	verifier, err := runtimepkg.NewRelayPlanVerifier(runtimepkg.PlanVerifierConfig{
		JWKSURL:           jwksURL,
		Issuer:            "https://control.speko.test",
		Audience:          protocol.RelayPlanAudience,
		AllowInsecureJWKS: true,
		CacheTTL:          time.Hour,
		ClockSkew:         time.Second,
		Now:               func() time.Time { return now },
		ReplayCache:       cache,
	})
	if err != nil {
		t.Fatalf("new relay verifier: %v", err)
	}
	return verifier
}

func signedRelayPlan(t *testing.T, private ed25519.PrivateKey, keyID string, now time.Time, jti, issuer, audience string, expiresAt time.Time) protocol.RelayPlan {
	t.Helper()
	plan := testRelayPlan(expiresAt)
	envelope := relayEnvelope(now, jti, issuer, audience, plan)
	plan.Signature = signCompactJWS(t, private, keyID, protocol.RelayPlanJWSType, envelope)
	return plan
}

func relayEnvelope(now time.Time, jti, issuer, audience string, plan protocol.RelayPlan) protocol.RelayPlanEnvelope {
	return protocol.RelayPlanEnvelope{
		Issuer: issuer, Audience: []string{audience}, IssuedAt: now.Add(-time.Second), ExpiresAt: plan.ExpiresAt, ID: jti, Plan: plan.Unsigned(),
	}
}

// signCompactJWS signs an arbitrary envelope with a caller-chosen typ so
// tests can mint deliberately mismatched artifacts as well as valid ones.
func signCompactJWS(t *testing.T, private ed25519.PrivateKey, keyID, typ string, payload any) string {
	t.Helper()
	return signCompactJWSWithAlg(t, private, keyID, typ, "EdDSA", payload)
}

// signCompactJWSWithAlg additionally lets the protected header claim an
// arbitrary algorithm while the actual signature stays Ed25519 — exactly the
// artifact an algorithm-confusion attempt presents to a verifier.
func signCompactJWSWithAlg(t *testing.T, private ed25519.PrivateKey, keyID, typ, alg string, payload any) string {
	t.Helper()
	encodedPayload, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	header, err := json.Marshal(map[string]string{"alg": alg, "kid": keyID, "typ": typ})
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	signingInput := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(encodedPayload)
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(private, []byte(signingInput)))
}

// newJWKSFixtureWithAlg publishes one Ed25519 key whose JWK claims a
// caller-chosen alg, so tests can pin the JWKS-alg-versus-header mismatch
// branch without inventing a second key type.
func newJWKSFixtureWithAlg(t *testing.T, id, alg string, public ed25519.PublicKey) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Cache-Control", "max-age=300")
		_ = json.NewEncoder(writer).Encode(map[string]any{"keys": []any{map[string]string{
			"kty": "OKP", "kid": id, "alg": alg, "use": "sig", "crv": "Ed25519",
			"x": base64.RawURLEncoding.EncodeToString(public),
		}}})
	}))
	t.Cleanup(server.Close)
	return server
}

func testRelayPlan(expiresAt time.Time) protocol.RelayPlan {
	return protocol.RelayPlan{
		PlanID: "rplan-test", RequestID: "rreq-test", SessionID: "rsess-test", AttemptID: "att-1", ReservationID: "res-relay-test",
		OrganizationID: "org-test", PrincipalID: "cred-test",
		Kind:   protocol.SessionKindLLM,
		Region: "us-west-2", Provider: "openai", Model: "gpt-4.1-mini",
		Endpoint: "https://api.openai.com/v1/responses", Transport: protocol.TransportHTTP,
		CatalogDigest:   "sha256:4d0c8e2a9f713b5e6d1c0a8b3f2e9d4c5b6a7f8e9d0c1b2a3f4e5d6c7b8a9f00",
		RateCardVersion: "2026-08-01",
		Budgets: []protocol.RelayBudget{
			{Group: protocol.RelayBudgetGroupLLMInput, CeilingUnits: 4096},
			{Group: protocol.RelayBudgetGroupLLMOutput, CeilingUnits: 1024},
		},
		RelayAccess:  protocol.RelayAccessCredential{Value: "sra_test-bearer", ExpiresAt: expiresAt},
		ControlToken: protocol.RelayControlToken{Value: "rct_test-token", ExpiresAt: expiresAt},
		ExpiresAt:    expiresAt,
		Requirements: protocol.RelayRequirements{Protocol: protocol.VoiceV0, ProtocolRevision: protocol.RelayRevision},
	}
}
