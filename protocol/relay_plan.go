package protocol

import (
	"fmt"
	"strings"
	"time"
)

const (
	// RelayRevision is the protocol revision claimed by relay plans. It is
	// deliberately a separate constant from CurrentRevision: rev-3 session
	// plans and rev-4 relay plans coexist, each validator exact-matches its
	// own revision, and a runtime that predates the relay rejects a relay
	// plan outright instead of half-understanding it.
	// RelayLegacyRevision is the original managed-only Relay plan revision.
	// New connectors keep accepting it as an *implicit managed* plan while the
	// fleet rolls forward; callers must never attach credential_source to it.
	RelayLegacyRevision = 4
	// RelayRevision adds the signed credential source required for Relay BYOK.
	// A connector that predates this revision rejects it rather than treating a
	// zero-charge BYOK reservation as managed traffic.
	RelayRevision = 5
	// RelayPlanJWSType is the protected-header typ required on a relay-plan
	// compact JWS. Even under a shared signing key, the typ check stops a
	// session-plan signature from authorizing a relay dispatch and a
	// relay-plan signature from opening a provider session.
	RelayPlanJWSType = "speko.relay-plan+jws"
	// RelayPlanAudience is the aud claim relay verifiers require. Relay
	// connectors are the sole intended consumers of relay plans; the distinct
	// audience keeps a plan minted for the runtime fleet from verifying at a
	// connector even before the typ check is reached.
	RelayPlanAudience = "speko-relay"
)

// RelayUsageUnit identifies a provider quantity metered by the relay ledger.
// It is a separate enum from the rev-3 UsageUnit on purpose: rev-3 settlement
// exact-matches the two session-plan units, and widening that enum would
// change revision-3 acceptance behavior. Units appear on reservation and
// usage lines; the signed plan itself carries budgets keyed by
// RelayBudgetGroup.
type RelayUsageUnit string

const (
	RelayUsageUnitDurationSeconds   RelayUsageUnit = "duration_seconds"
	RelayUsageUnitCharacters        RelayUsageUnit = "characters"
	RelayUsageUnitInputTokens       RelayUsageUnit = "input_tokens"
	RelayUsageUnitCachedInputTokens RelayUsageUnit = "cached_input_tokens"
	RelayUsageUnitOutputTokens      RelayUsageUnit = "output_tokens"
	RelayUsageUnitReasoningTokens   RelayUsageUnit = "reasoning_tokens"
)

// RelayBudgetGroup names an authorized spend bucket in a relay plan. Groups
// are deliberately coarser than units: settlement can split one group across
// several usage-line units (llm_input covers fresh and cached input tokens),
// but authorization is granted and capped per group.
type RelayBudgetGroup string

const (
	RelayBudgetGroupSTTDuration   RelayBudgetGroup = "stt_duration"
	RelayBudgetGroupTTSCharacters RelayBudgetGroup = "tts_characters"
	RelayBudgetGroupLLMInput      RelayBudgetGroup = "llm_input"
	RelayBudgetGroupLLMOutput     RelayBudgetGroup = "llm_output"
)

// RelayBudget is one signed spend ceiling. The ceiling is the connector's
// defense-in-depth guard and the ledger's settlement cap. Streaming sessions
// may extend a group later through lease slices or top-ups; extensions never
// edit the signed plan, so the plan always records what admission authorized.
type RelayBudget struct {
	Group        RelayBudgetGroup `json:"group"`
	CeilingUnits int64            `json:"ceiling_units"`
}

// RelayPlan is the signed relay execution decision. It is issued by the
// control plane at admission, carried by the edge, and consumed exactly once
// by the provider connector named in it. Endpoint is an assertion the
// connector checks against its embedded catalog, never a dial input, so a
// signed plan cannot redirect a connector to an attacker-chosen host.
// Signature verification is intentionally separate from structural
// validation.
type RelayPlan struct {
	PlanID        string `json:"plan_id"`
	RequestID     string `json:"request_id"`
	SessionID     string `json:"session_id"`
	AttemptID     string `json:"attempt_id"`
	ReservationID string `json:"reservation_id"`
	// OrganizationID and PrincipalID identify the paying customer.
	// PrincipalID is the credential identifier, never the key itself: relay
	// plans are content-free and safe to persist in the ledger.
	OrganizationID string `json:"organization_id"`
	PrincipalID    string `json:"principal_id"`
	// Kind is limited to stt, tts, and llm; the relay has no realtime kind.
	Kind SessionKind `json:"kind"`
	// Region is the Speko relay region (an AWS region identifier) whose
	// connector fleet must execute this plan, not a provider residency claim.
	Region    string    `json:"region"`
	Provider  string    `json:"provider"`
	Model     string    `json:"model"`
	Endpoint  string    `json:"endpoint"`
	Transport Transport `json:"transport"`
	// CatalogDigest pins the release catalog the endpoint and model were
	// selected from, in "sha256:<64 hex>" form. A connector running a
	// different catalog release rejects the plan instead of guessing.
	CatalogDigest string `json:"catalog_digest"`
	// RateCardVersion records the rate card frozen into the attempt's price
	// lines at admission so settlement disputes are reconstructible.
	RateCardVersion string `json:"rate_card_version"`
	// CredentialSource names who owns the provider credential. It is signed
	// with the plan and is required for revision 5. Revision 4 deliberately
	// omits it and therefore always means managed credentials.
	CredentialSource CredentialSource `json:"credential_source,omitempty"`
	// Budgets carries at least one group with a positive ceiling; groups are
	// unique and must be legal for Kind.
	Budgets []RelayBudget `json:"budgets"`
	// Lease is present for streaming sessions, whose accounting horizon is
	// extended in slices rather than reserved up front.
	Lease *RelayLease `json:"lease,omitempty"`
	// RelayAccess is the random one-use bearer the edge presents on the
	// connector handshake. It is bound into the signature so it cannot be
	// swapped onto another plan.
	RelayAccess RelayAccessCredential `json:"relay_access"`
	// ControlToken authorizes the edge's session-scoped follow-up calls to
	// the control plane (status, usage reports, renewals, top-ups, fallback).
	ControlToken RelayControlToken `json:"control_token"`
	ExpiresAt    time.Time         `json:"expires_at"`
	Requirements RelayRequirements `json:"requirements"`
	// Signature is a compact JWS over a RelayPlanEnvelope containing this
	// plan with Signature omitted. It is deliberately opaque to callers: only
	// a relay plan verifier may interpret it.
	Signature string `json:"signature,omitempty"`
}

// RelayAccessCredential is the one-use bearer that admits the edge to the
// connector named in the plan. Callers must never log Value, which is omitted
// by its String method.
type RelayAccessCredential struct {
	Value     string    `json:"value"`
	ExpiresAt time.Time `json:"expires_at"`
}

func (c RelayAccessCredential) String() string {
	return fmt.Sprintf("relay_access(redacted, expires_at=%s)", c.ExpiresAt.UTC().Format(time.RFC3339))
}

// RelayControlToken authenticates content-free follow-up control-plane calls
// for one relay session. The control plane stores only its hash. Callers must
// never log Value, which is omitted by its String method.
type RelayControlToken struct {
	Value     string    `json:"value"`
	ExpiresAt time.Time `json:"expires_at"`
}

func (t RelayControlToken) String() string {
	return fmt.Sprintf("relay_control_token(redacted, expires_at=%s)", t.ExpiresAt.UTC().Format(time.RFC3339))
}

// RelayLease describes the first renewable accounting slice of a streaming
// relay session. Renewal extends accounting only, exactly like the rev-3
// session lease: the provider stream and the relay access bearer are never
// reissued by a renewal. TopUpURL is present only when the session's budget
// groups can be extended mid-stream (TTS character top-ups).
type RelayLease struct {
	DurationSeconds int       `json:"duration_seconds"`
	ExpiresAt       time.Time `json:"expires_at"`
	RenewalURL      string    `json:"renewal_url"`
	TopUpURL        string    `json:"top_up_url,omitempty"`
}

// RelayRequirements binds a relay plan to its exact protocol contract. It
// deliberately exact-matches RelayRevision rather than CurrentRevision so the
// rev-3 Requirements validator stays untouched and neither artifact can
// masquerade as the other.
type RelayRequirements struct {
	Protocol         string `json:"protocol"`
	ProtocolRevision int    `json:"protocol_revision"`
}

// RelayPlanEnvelope is the payload of the compact JWS carried in
// RelayPlan.Signature. The control plane signs the full unsigned plan so a
// caller cannot transplant a valid signature onto a modified route, budget,
// or bearer.
//
// The JSON field names intentionally follow registered JWT claim names even
// though this is a JWS envelope rather than a bearer identity token.
type RelayPlanEnvelope struct {
	Issuer    string    `json:"iss"`
	Audience  []string  `json:"aud"`
	IssuedAt  time.Time `json:"iat"`
	ExpiresAt time.Time `json:"exp"`
	ID        string    `json:"jti"`
	Plan      RelayPlan `json:"plan"`
}

// Unsigned returns the exact plan representation that is signed inside a
// RelayPlanEnvelope. It is useful to control-plane code and test fixtures,
// but it deliberately does not attempt to create a signature.
func (p RelayPlan) Unsigned() RelayPlan {
	p.Signature = ""
	return p
}

// Validate verifies structural and time-bound invariants. It does not verify
// the signature; callers must do that with a trusted control-plane key first.
func (p RelayPlan) Validate(now time.Time) error {
	if strings.TrimSpace(p.PlanID) == "" || strings.TrimSpace(p.RequestID) == "" || strings.TrimSpace(p.SessionID) == "" || strings.TrimSpace(p.AttemptID) == "" || strings.TrimSpace(p.ReservationID) == "" {
		return fmt.Errorf("plan_id, request_id, session_id, attempt_id, and reservation_id are required")
	}
	if strings.TrimSpace(p.OrganizationID) == "" || strings.TrimSpace(p.PrincipalID) == "" {
		return fmt.Errorf("organization_id and principal_id are required")
	}
	if !validRelayKind(p.Kind) {
		return fmt.Errorf("kind: unsupported value %q", p.Kind)
	}
	if strings.TrimSpace(p.Region) == "" {
		return fmt.Errorf("region: required")
	}
	if strings.TrimSpace(p.Provider) == "" || strings.TrimSpace(p.Model) == "" {
		return fmt.Errorf("provider and model are required")
	}
	if !validTransport(p.Transport) {
		return fmt.Errorf("transport: unsupported value %q", p.Transport)
	}
	if err := validateEndpoint(p.Endpoint, p.Transport); err != nil {
		return fmt.Errorf("endpoint: %w", err)
	}
	if err := validateCatalogDigest(p.CatalogDigest); err != nil {
		return fmt.Errorf("catalog_digest: %w", err)
	}
	if strings.TrimSpace(p.RateCardVersion) == "" {
		return fmt.Errorf("rate_card_version: required")
	}
	if p.ExpiresAt.IsZero() || !p.ExpiresAt.After(now) {
		return fmt.Errorf("expires_at: plan is expired")
	}
	if err := validateRelayBudgets(p.Kind, p.Budgets); err != nil {
		return fmt.Errorf("budgets: %w", err)
	}
	if p.Lease != nil {
		if err := p.Lease.validate(now); err != nil {
			return fmt.Errorf("lease: %w", err)
		}
	}
	if err := validateRelayCredential(p.RelayAccess.Value, p.RelayAccess.ExpiresAt, now, p.ExpiresAt); err != nil {
		return fmt.Errorf("relay_access: %w", err)
	}
	if err := validateRelayCredential(p.ControlToken.Value, p.ControlToken.ExpiresAt, now, p.ExpiresAt); err != nil {
		return fmt.Errorf("control_token: %w", err)
	}
	if err := p.Requirements.validate(); err != nil {
		return fmt.Errorf("requirements: %w", err)
	}
	if err := p.validateCredentialSource(); err != nil {
		return fmt.Errorf("credential_source: %w", err)
	}
	if strings.TrimSpace(p.Signature) == "" {
		return fmt.Errorf("signature: required")
	}
	return nil
}

// Validate checks envelope claims that are independent of the signature. A
// verifier must first verify the JWS and then call this method before using
// any endpoint or bearer in the enclosed plan.
func (e RelayPlanEnvelope) Validate(now time.Time, issuer, audience string, clockSkew time.Duration) error {
	if strings.TrimSpace(e.Issuer) == "" || strings.TrimSpace(e.ID) == "" {
		return fmt.Errorf("iss and jti are required")
	}
	if e.Issuer != issuer {
		return fmt.Errorf("iss: got %q, want %q", e.Issuer, issuer)
	}
	if !containsString(e.Audience, audience) {
		return fmt.Errorf("aud: missing %q", audience)
	}
	if e.IssuedAt.IsZero() || e.ExpiresAt.IsZero() {
		return fmt.Errorf("iat and exp are required")
	}
	if e.IssuedAt.After(now.Add(clockSkew)) {
		return fmt.Errorf("iat: issued in the future")
	}
	if !e.ExpiresAt.After(now.Add(-clockSkew)) {
		return fmt.Errorf("exp: envelope is expired")
	}
	if !e.Plan.ExpiresAt.Equal(e.ExpiresAt) {
		return fmt.Errorf("exp: must match plan expires_at")
	}
	if strings.TrimSpace(e.Plan.Signature) != "" {
		return fmt.Errorf("plan.signature: must be omitted from the signed envelope")
	}
	return nil
}

func (l RelayLease) validate(now time.Time) error {
	if l.DurationSeconds <= 0 {
		return fmt.Errorf("duration_seconds: must be positive")
	}
	if l.ExpiresAt.IsZero() || !l.ExpiresAt.After(now) {
		return fmt.Errorf("lease is expired")
	}
	if err := validateSecureURL(l.RenewalURL, "https"); err != nil {
		return fmt.Errorf("renewal_url: %w", err)
	}
	if l.TopUpURL != "" {
		if err := validateSecureURL(l.TopUpURL, "https"); err != nil {
			return fmt.Errorf("top_up_url: %w", err)
		}
	}
	return nil
}

func (r RelayRequirements) validate() error {
	if r.Protocol != VoiceV0 {
		return fmt.Errorf("protocol: got %q, want %q", r.Protocol, VoiceV0)
	}
	if r.ProtocolRevision != RelayLegacyRevision && r.ProtocolRevision != RelayRevision {
		return fmt.Errorf("protocol_revision: got %d, want %d or %d", r.ProtocolRevision, RelayLegacyRevision, RelayRevision)
	}
	return nil
}

// validateCredentialSource keeps the rollout boundary explicit. A rev-4 plan
// has no source claim and is managed by definition; accepting a populated
// source on rev-4 would let an old connector silently ignore a BYOK claim.
func (p RelayPlan) validateCredentialSource() error {
	switch p.Requirements.ProtocolRevision {
	case RelayLegacyRevision:
		if p.CredentialSource != "" {
			return fmt.Errorf("must be omitted for protocol revision %d", RelayLegacyRevision)
		}
	case RelayRevision:
		if p.CredentialSource != CredentialsManaged && p.CredentialSource != CredentialsBYOK {
			return fmt.Errorf("must be %q or %q for protocol revision %d", CredentialsManaged, CredentialsBYOK, RelayRevision)
		}
	}
	return nil
}

// EffectiveCredentialSource returns the source an executing connector must
// use. It is the only compatibility shim: revision 4 is managed, and every
// revision 5 plan has already been structurally required to state a source.
func (p RelayPlan) EffectiveCredentialSource() CredentialSource {
	if p.Requirements.ProtocolRevision == RelayLegacyRevision {
		return CredentialsManaged
	}
	return p.CredentialSource
}

// validateRelayCredential is shared by the relay access bearer and the
// control token: both are opaque values whose lifetime must sit inside the
// plan's, so an expired plan leaves no live credential behind.
func validateRelayCredential(value string, expiresAt, now, planExpiresAt time.Time) error {
	if strings.TrimSpace(value) == "" || expiresAt.IsZero() {
		return fmt.Errorf("value and expires_at are required")
	}
	if !expiresAt.After(now) {
		return fmt.Errorf("credential is expired")
	}
	if expiresAt.After(planExpiresAt) {
		return fmt.Errorf("expiry must not outlive the plan")
	}
	return nil
}

func validateRelayBudgets(kind SessionKind, budgets []RelayBudget) error {
	if len(budgets) == 0 {
		return fmt.Errorf("at least one budget group is required")
	}
	seen := make(map[RelayBudgetGroup]bool, len(budgets))
	for _, budget := range budgets {
		if !validRelayBudgetGroup(budget.Group) {
			return fmt.Errorf("group: unsupported value %q", budget.Group)
		}
		if seen[budget.Group] {
			return fmt.Errorf("group %q appears more than once", budget.Group)
		}
		seen[budget.Group] = true
		if budget.CeilingUnits <= 0 {
			return fmt.Errorf("ceiling_units: must be positive for group %q", budget.Group)
		}
		if !relayBudgetGroupLegalForKind(kind, budget.Group) {
			return fmt.Errorf("group %q is not valid for %s plans", budget.Group, kind)
		}
	}
	if kind == SessionKindLLM && (!seen[RelayBudgetGroupLLMInput] || !seen[RelayBudgetGroupLLMOutput]) {
		return fmt.Errorf("llm plans require both llm_input and llm_output groups")
	}
	return nil
}

func validateCatalogDigest(digest string) error {
	const prefix = "sha256:"
	if !strings.HasPrefix(digest, prefix) || len(digest) != len(prefix)+64 {
		return fmt.Errorf("must be sha256:<64 hex characters>")
	}
	for _, r := range digest[len(prefix):] {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return fmt.Errorf("must be sha256:<64 hex characters>")
		}
	}
	return nil
}

func validRelayKind(v SessionKind) bool {
	return v == SessionKindSTT || v == SessionKindTTS || v == SessionKindLLM
}

func validRelayBudgetGroup(v RelayBudgetGroup) bool {
	return v == RelayBudgetGroupSTTDuration || v == RelayBudgetGroupTTSCharacters || v == RelayBudgetGroupLLMInput || v == RelayBudgetGroupLLMOutput
}

func relayBudgetGroupLegalForKind(kind SessionKind, group RelayBudgetGroup) bool {
	switch kind {
	case SessionKindSTT:
		return group == RelayBudgetGroupSTTDuration
	case SessionKindTTS:
		return group == RelayBudgetGroupTTSCharacters
	case SessionKindLLM:
		return group == RelayBudgetGroupLLMInput || group == RelayBudgetGroupLLMOutput
	}
	return false
}
