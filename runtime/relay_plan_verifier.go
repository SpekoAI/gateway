package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/SpekoAI/gateway/protocol"
)

// RelayPlanVerifier verifies compact JWS relay-plan envelopes using cached
// control-plane public keys. It shares the JWKS cache, key-rotation, and
// clock-skew machinery of JWKSPlanVerifier unchanged; only the envelope type,
// the expected JWS typ, and the byte-exact plan binding differ. A relay plan
// signed for the session-plan typ (or vice versa) is rejected before any key
// lookup semantics matter, so the two verifiers never accept each other's
// artifacts even when pointed at the same JWKS.
//
// A relay connector is the sole JTI consumer for a relay plan, and it must
// only consume the JTI once it knows the plan was routed to the right place.
// Connectors therefore construct this verifier with PassthroughReplayCache,
// run their region/provider/model/endpoint self-checks after Verify returns,
// and only then consume the envelope ID against their shared replay cache. A
// valid plan misrouted to the wrong connector fails the self-check with its
// JTI intact, so redelivery to the correct connector still succeeds exactly
// once.
type RelayPlanVerifier struct {
	// keys reuses JWKSPlanVerifier verbatim for configuration defaults and
	// the JWKS fetch/cache/rotation path, keeping the revision-3 code
	// byte-identical instead of forking it.
	keys *JWKSPlanVerifier
}

// NewRelayPlanVerifier creates the production relay-plan verifier used by
// relay edges and connectors. The config carries the relay issuer, the
// dedicated relay JWKS URL, and the relay audience (protocol.RelayPlanAudience
// for production deployments).
func NewRelayPlanVerifier(config PlanVerifierConfig) (*RelayPlanVerifier, error) {
	keys, err := NewPlanVerifier(config)
	if err != nil {
		return nil, err
	}
	return &RelayPlanVerifier{keys: keys}, nil
}

// Verify validates the outer relay plan, verifies its JWS against the relay
// typ, binds the envelope to that exact plan, validates
// issuer/audience/times, and offers its jti to the configured ReplayCache.
// With PassthroughReplayCache the final step always accepts and the caller
// owns replay protection; see the type comment for the connector contract.
func (v *RelayPlanVerifier) Verify(ctx context.Context, plan protocol.RelayPlan) error {
	now := v.keys.config.Now().UTC()
	if err := plan.Validate(now); err != nil {
		return fmt.Errorf("%w: plan validation: %v", ErrPlanSignature, err)
	}
	header, payload, signingInput, err := parseCompactJWSWithType(plan.Signature, protocol.RelayPlanJWSType)
	if err != nil {
		return err
	}
	key, err := v.keys.keyFor(ctx, header.KeyID)
	if err != nil {
		return err
	}
	if key.Algorithm != "" && key.Algorithm != header.Algorithm {
		return fmt.Errorf("%w: jwks key algorithm does not match jws header", ErrPlanSignature)
	}
	if err := verifyJWS(key.PublicKey, header.Algorithm, signingInput, header.Signature); err != nil {
		return err
	}
	var envelope protocol.RelayPlanEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return fmt.Errorf("%w: envelope payload is not valid JSON", ErrPlanSignature)
	}
	if err := envelope.Validate(now, v.keys.config.Issuer, v.keys.config.Audience, v.keys.config.ClockSkew); err != nil {
		return fmt.Errorf("%w: envelope validation: %v", ErrPlanSignature, err)
	}
	if err := relayPlanMatchesEnvelope(plan, envelope.Plan); err != nil {
		return err
	}
	accepted, err := v.keys.config.ReplayCache.TryUse(ctx, envelope.ID, envelope.ExpiresAt.Add(v.keys.config.ClockSkew))
	if err != nil {
		return fmt.Errorf("runtime: record relay plan replay guard: %w", err)
	}
	if !accepted {
		return ErrPlanReplay
	}
	return nil
}

func relayPlanMatchesEnvelope(plan, enclosed protocol.RelayPlan) error {
	want, err := json.Marshal(plan.Unsigned())
	if err != nil {
		return fmt.Errorf("%w: encode plan", ErrPlanSignature)
	}
	got, err := json.Marshal(enclosed)
	if err != nil {
		return fmt.Errorf("%w: encode envelope plan", ErrPlanSignature)
	}
	if string(want) != string(got) {
		return fmt.Errorf("%w: envelope does not bind the supplied plan", ErrPlanSignature)
	}
	return nil
}

// PassthroughReplayCache accepts every envelope ID without recording it. It
// exists for callers that own replay protection themselves: a relay connector
// verifies the plan's signature first, runs its routing self-checks, and only
// then consumes the JTI against its shared Redis cache. Wiring a real cache
// into Verify would burn the JTI before the self-checks run, turning a
// misrouted-but-valid plan into a permanently unusable one. Never use this
// cache anywhere the Verify call itself is the last line of replay defense.
type PassthroughReplayCache struct{}

// TryUse accepts any well-formed ID and remembers nothing.
func (PassthroughReplayCache) TryUse(_ context.Context, id string, expiresAt time.Time) (bool, error) {
	if strings.TrimSpace(id) == "" || expiresAt.IsZero() {
		return false, errors.New("runtime: replay cache requires an id and expiry")
	}
	return true, nil
}
