package protocol

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

const (
	// VoiceV0 is the current wire-protocol identifier.
	VoiceV0 = "speko.voice.v0"
	// CurrentRevision is intentionally exact during initial development. A
	// runtime must reject a plan built for any other revision before media is
	// accepted.
	CurrentRevision = 3
)

// SessionKind selects the provider-neutral operation requested by a session.
type SessionKind string

const (
	SessionKindSTT      SessionKind = "stt"
	SessionKindTTS      SessionKind = "tts"
	SessionKindLLM      SessionKind = "llm"
	SessionKindRealtime SessionKind = "realtime"
)

// Placement identifies where the runtime executes.
type Placement string

const (
	PlacementEmbedded Placement = "embedded"
	PlacementSidecar  Placement = "sidecar"
	PlacementClient   Placement = "client"
)

// ProviderRoute identifies the remote hop selected for a session.
type ProviderRoute string

const (
	RouteAuto           ProviderRoute = "auto"
	RouteProviderDirect ProviderRoute = "provider_direct"
	RouteSpekoRelay     ProviderRoute = "speko_relay"
)

// CredentialSource identifies who owns the upstream credential.
type CredentialSource string

const (
	CredentialsManaged CredentialSource = "managed"
	CredentialsBYOK    CredentialSource = "byok"
)

// RelayPolicy constrains whether the control plane may select the relay route.
type RelayPolicy string

const (
	RelayRequired  RelayPolicy = "required"
	RelayAllowed   RelayPolicy = "allowed"
	RelayForbidden RelayPolicy = "forbidden"
)

// Transport describes the upstream protocol selected in a session plan.
type Transport string

const (
	TransportWebSocket Transport = "websocket"
	TransportHTTP      Transport = "http"
	TransportGRPC      Transport = "grpc"
	TransportWebRTC    Transport = "webrtc"
)

// CredentialKind identifies a bounded credential that can safely be placed in
// a customer-controlled runtime. It deliberately has no server-key variant.
type CredentialKind string

const (
	CredentialBearer      CredentialKind = "bearer"
	CredentialSignedURL   CredentialKind = "signed_url"
	CredentialSessionURL  CredentialKind = "session_url"
	CredentialRelayAccess CredentialKind = "relay_access"
)

// SessionPlanRequest is the control-plane request made before a new local or
// embedded session opens. ProtocolRevision mirrors the protocol header so
// JSON fixtures can validate the full request contract in one place.
type SessionPlanRequest struct {
	Kind             SessionKind       `json:"kind"`
	Protocol         string            `json:"protocol"`
	ProtocolRevision int               `json:"protocol_revision"`
	Runtime          RuntimeDescriptor `json:"runtime"`
	Workload         *Workload         `json:"workload,omitempty"`
	Integration      *Integration      `json:"integration,omitempty"`
	Execution        ExecutionRequest  `json:"execution"`
	Request          RequestOptions    `json:"request"`
	Media            *MediaFormat      `json:"media,omitempty"`
}

// Workload identifies the customer-owned logical workload that opened a
// session. It is deliberately framework-neutral: a managed Speko agent is one
// workload type, while a customer can use another stable type/id pair for a
// custom service. The identifier is operational metadata only and carries no
// prompt, transcript, or user content.
type Workload struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

// RuntimeDescriptor describes the implementation that will execute the plan.
type RuntimeDescriptor struct {
	Name           string          `json:"name"`
	Version        string          `json:"version"`
	InstanceID     string          `json:"instance_id"`
	Placement      Placement       `json:"placement"`
	ProviderRoutes []ProviderRoute `json:"provider_routes"`
	Adapters       []string        `json:"adapters"`
}

// Integration identifies an optional framework integration without coupling
// the plan to framework-specific provider logic.
type Integration struct {
	Name             string `json:"name"`
	Version          string `json:"version"`
	FrameworkVersion string `json:"framework_version,omitempty"`
	Transport        string `json:"transport,omitempty"`
}

// ExecutionRequest expresses route constraints. RouteAuto is valid only here;
// returned SessionPlans always contain a concrete route.
type ExecutionRequest struct {
	ProviderRoute    ProviderRoute    `json:"provider_route"`
	CredentialSource CredentialSource `json:"credential_source"`
	RelayPolicy      RelayPolicy      `json:"relay_policy"`
}

// RequestOptions contains portable, provider-neutral session options.
type RequestOptions struct {
	Provider           string   `json:"provider,omitempty"`
	MaxInputCharacters int64    `json:"max_input_characters,omitempty"`
	Language           string   `json:"language,omitempty"`
	Objective          string   `json:"objective,omitempty"`
	Model              string   `json:"model,omitempty"`
	Voice              string   `json:"voice,omitempty"`
	RegionHint         string   `json:"region_hint,omitempty"`
	Allow              []string `json:"allow,omitempty"`
	Deny               []string `json:"deny,omitempty"`
}

// UsageUnit identifies the provider quantity whose spend was authorized by a
// reservation. Time leases and usage authorization are deliberately separate:
// renewing a lease never changes a character allowance.
type UsageUnit string

const (
	UsageUnitDurationSeconds UsageUnit = "duration_seconds"
	UsageUnitCharacters      UsageUnit = "characters"
)

// UsageReservation is the fixed provider-usage allowance committed before a
// credential is minted. Character top-ups require a separate authorization
// flow and are not implied by SessionLease renewal.
type UsageReservation struct {
	Unit            UsageUnit `json:"unit"`
	AuthorizedUnits int64     `json:"authorized_units"`
}

// MediaFormat describes unencoded audio supplied to or received from a
// streaming session.
type MediaFormat struct {
	Encoding     string `json:"encoding"`
	SampleRateHz int    `json:"sample_rate_hz"`
	Channels     int    `json:"channels"`
}

// Validate checks that a portable audio format is within the current protocol
// bounds. Provider adapters may apply stricter documented constraints.
func (m MediaFormat) Validate() error {
	return m.validate()
}

// SessionPlan is the signed execution decision consumed by the runtime.
// Signature verification is intentionally separate from structural validation.
type SessionPlan struct {
	PlanID       string       `json:"plan_id"`
	SessionID    string       `json:"session_id"`
	AttemptID    string       `json:"attempt_id"`
	Execution    Execution    `json:"execution"`
	ExpiresAt    time.Time    `json:"expires_at"`
	Route        PlanRoute    `json:"route"`
	Reservation  Reservation  `json:"reservation"`
	Telemetry    Telemetry    `json:"telemetry"`
	Fallback     *Fallback    `json:"fallback,omitempty"`
	Requirements Requirements `json:"requirements"`
	// Signature is a compact JWS over a SessionPlanEnvelope containing this
	// plan with Signature omitted. It is deliberately opaque to callers: only
	// a runtime PlanVerifier may interpret it.
	Signature string `json:"signature,omitempty"`
}

// Execution records the concrete route selected by the control plane.
type Execution struct {
	Placement        Placement        `json:"placement"`
	ProviderRoute    ProviderRoute    `json:"provider_route"`
	CredentialSource CredentialSource `json:"credential_source"`
}

// PlanRoute contains no permanent Speko-owned provider credential.
type PlanRoute struct {
	Provider   string               `json:"provider"`
	Model      string               `json:"model"`
	Region     string               `json:"region,omitempty"`
	Adapter    string               `json:"adapter"`
	Transport  Transport            `json:"transport"`
	Endpoint   string               `json:"endpoint"`
	Credential *DelegatedCredential `json:"credential,omitempty"`
}

// DelegatedCredential must be scoped and short lived. Callers must never log
// Value, which is omitted by its String method.
type DelegatedCredential struct {
	Kind      CredentialKind `json:"kind"`
	Value     string         `json:"value"`
	ExpiresAt time.Time      `json:"expires_at"`
}

func (c DelegatedCredential) String() string {
	return fmt.Sprintf("%s(redacted, expires_at=%s)", c.Kind, c.ExpiresAt.UTC().Format(time.RFC3339))
}

// Reservation describes the first renewable capacity lease for a session.
// LeaseDurationSeconds is the duration of one lease slice, not a maximum stream
// lifetime. LeaseExpiresAt is the current absolute deadline and RenewalURL is
// the plan-scoped control-plane endpoint used to extend it without reopening
// the provider stream.
type Reservation struct {
	ID                   string                 `json:"id"`
	LeaseDurationSeconds int                    `json:"lease_duration_seconds"`
	LeaseExpiresAt       time.Time              `json:"lease_expires_at"`
	RenewalURL           string                 `json:"renewal_url"`
	Concurrency          ConcurrencyReservation `json:"concurrency"`
	Usage                UsageReservation       `json:"usage"`
}

// SessionLeaseRenewalRequest uses optimistic concurrency at the current
// deadline. Repeating an identical request is idempotent and returns the same
// extension; a stale caller cannot manufacture overlapping capacity.
type SessionLeaseRenewalRequest struct {
	ReservationID  string    `json:"reservation_id"`
	AttemptID      string    `json:"attempt_id"`
	LeaseExpiresAt time.Time `json:"lease_expires_at"`
}

// SessionLease is a control-plane-authorized extension of an existing
// reservation. It changes only accounting/concurrency lifetime; the runtime
// keeps the already-open provider stream and delegated credential untouched.
type SessionLease struct {
	ReservationID      string    `json:"reservation_id"`
	SessionID          string    `json:"session_id"`
	AttemptID          string    `json:"attempt_id"`
	ConcurrencyLeaseID string    `json:"concurrency_lease_id"`
	Sequence           int       `json:"sequence"`
	ExpiresAt          time.Time `json:"expires_at"`
	RenewAfter         time.Time `json:"renew_after"`
}

// Validate checks that a renewal is bound to the active session and advances
// its current lease. HTTP authentication and TLS protect the control-plane
// response; the original signed plan protects the renewal endpoint and IDs.
func (l SessionLease) Validate(now time.Time, plan SessionPlan) error {
	if l.ReservationID != plan.Reservation.ID || l.SessionID != plan.SessionID || l.AttemptID != plan.AttemptID || l.ConcurrencyLeaseID != plan.Reservation.Concurrency.LeaseID {
		return fmt.Errorf("session lease: binding does not match session plan")
	}
	if l.Sequence < 1 || l.ExpiresAt.IsZero() || !l.ExpiresAt.After(now) || !l.ExpiresAt.After(plan.Reservation.LeaseExpiresAt) {
		return fmt.Errorf("session lease: expiry must advance the active lease")
	}
	if l.RenewAfter.IsZero() || !l.RenewAfter.Before(l.ExpiresAt) {
		return fmt.Errorf("session lease: renew_after must precede expiry")
	}
	return nil
}

// ConcurrencyReservation identifies the capacity lease consumed by this
// session. It is control-plane metadata, not a client-controlled limit.
// Settlement uses the lease ID alongside the reservation ID so a retry cannot
// release or charge a different concurrent session.
type ConcurrencyReservation struct {
	LeaseID string `json:"lease_id"`
	Slots   int    `json:"slots"`
}

// Telemetry identifies an asynchronous, session-scoped authenticated
// destination. It is empty for locally issued plans, whose anonymous default
// destination belongs to the exporter rather than the signed routing plan.
type Telemetry struct {
	Endpoint        string `json:"endpoint,omitempty"`
	Token           string `json:"token,omitempty"`
	FlushIntervalMS int    `json:"flush_interval_ms,omitempty"`
}

// Fallback provides the control-plane endpoint used only at an explicit
// recovery boundary.
type Fallback struct {
	ExchangeURL string `json:"exchange_url"`
}

// Requirements binds a plan to an exact implementation contract.
type Requirements struct {
	Protocol         string `json:"protocol"`
	ProtocolRevision int    `json:"protocol_revision"`
	RuntimeVersion   string `json:"runtime_version"`
}

// SessionPlanEnvelope is the payload of the compact JWS carried in
// SessionPlan.Signature. The control plane signs the full unsigned plan so a
// caller cannot transplant a valid signature onto a modified route,
// reservation, or delegated credential.
//
// The JSON field names intentionally follow registered JWT claim names even
// though this is a JWS envelope rather than a bearer identity token.
type SessionPlanEnvelope struct {
	Issuer    string      `json:"iss"`
	Audience  []string    `json:"aud"`
	IssuedAt  time.Time   `json:"iat"`
	ExpiresAt time.Time   `json:"exp"`
	ID        string      `json:"jti"`
	Plan      SessionPlan `json:"plan"`
}

// Unsigned returns the exact plan representation that is signed inside a
// SessionPlanEnvelope. It is useful to control-plane clients and test
// fixtures, but it deliberately does not attempt to create a signature.
func (p SessionPlan) Unsigned() SessionPlan {
	p.Signature = ""
	return p
}

// Validate rejects a request before any media is accepted or a plan is sought.
func (r SessionPlanRequest) Validate() error {
	if !validKind(r.Kind) {
		return fmt.Errorf("kind: unsupported value %q", r.Kind)
	}
	if r.Protocol != VoiceV0 {
		return fmt.Errorf("protocol: got %q, want %q", r.Protocol, VoiceV0)
	}
	if r.ProtocolRevision != CurrentRevision {
		return fmt.Errorf("protocol_revision: got %d, want %d", r.ProtocolRevision, CurrentRevision)
	}
	if err := r.Runtime.validate(); err != nil {
		return fmt.Errorf("runtime: %w", err)
	}
	if r.Workload != nil {
		if err := r.Workload.validate(); err != nil {
			return fmt.Errorf("workload: %w", err)
		}
	}
	if r.Integration != nil && (strings.TrimSpace(r.Integration.Name) == "" || strings.TrimSpace(r.Integration.Version) == "") {
		return fmt.Errorf("integration: name and version are required together")
	}
	if err := r.Execution.validateRequest(r.Runtime.ProviderRoutes); err != nil {
		return fmt.Errorf("execution: %w", err)
	}
	provider := strings.TrimSpace(r.Request.Provider)
	if provider == "" {
		provider = "auto"
	}
	if strings.ContainsAny(provider, " \t\r\n") {
		return fmt.Errorf("request.provider: must be a provider id or auto")
	}
	if r.Kind == SessionKindTTS {
		if r.Request.MaxInputCharacters <= 0 {
			return fmt.Errorf("request.max_input_characters: positive value is required for tts")
		}
	} else if r.Request.MaxInputCharacters != 0 {
		return fmt.Errorf("request.max_input_characters: valid only for tts")
	}
	if needsMedia(r.Kind) {
		if r.Media == nil {
			return fmt.Errorf("media: required for %s", r.Kind)
		}
		if err := r.Media.validate(); err != nil {
			return fmt.Errorf("media: %w", err)
		}
	}
	return nil
}

func (w Workload) validate() error {
	if strings.TrimSpace(w.Type) == "" || strings.TrimSpace(w.ID) == "" {
		return fmt.Errorf("type and id are required")
	}
	if len(w.Type) > 64 || len(w.ID) > 256 {
		return fmt.Errorf("type or id exceeds its size limit")
	}
	return nil
}

// Validate verifies structural and time-bound invariants. It does not verify
// the signature; callers must do that with a trusted control-plane key first.
func (p SessionPlan) Validate(now time.Time) error {
	if strings.TrimSpace(p.PlanID) == "" || strings.TrimSpace(p.SessionID) == "" || strings.TrimSpace(p.AttemptID) == "" {
		return fmt.Errorf("plan_id, session_id, and attempt_id are required")
	}
	if p.ExpiresAt.IsZero() || !p.ExpiresAt.After(now) {
		return fmt.Errorf("expires_at: plan is expired")
	}
	if err := p.Requirements.validate(); err != nil {
		return fmt.Errorf("requirements: %w", err)
	}
	if err := p.Execution.validatePlan(); err != nil {
		return fmt.Errorf("execution: %w", err)
	}
	if err := p.Route.validate(); err != nil {
		return fmt.Errorf("route: %w", err)
	}
	if err := p.validateCredential(now); err != nil {
		return err
	}
	if strings.TrimSpace(p.Reservation.ID) == "" || p.Reservation.LeaseDurationSeconds <= 0 {
		return fmt.Errorf("reservation: id and positive lease duration are required")
	}
	if err := p.Reservation.Usage.validate(); err != nil {
		return fmt.Errorf("reservation.usage: %w", err)
	}
	if p.Reservation.LeaseExpiresAt.IsZero() || !p.Reservation.LeaseExpiresAt.After(now) {
		return fmt.Errorf("reservation: lease is expired")
	}
	if p.Reservation.RenewalURL != "" {
		if err := validateSecureURL(p.Reservation.RenewalURL, "https"); err != nil {
			return fmt.Errorf("reservation.renewal_url: %w", err)
		}
	}
	if strings.TrimSpace(p.Reservation.Concurrency.LeaseID) == "" || p.Reservation.Concurrency.Slots <= 0 {
		return fmt.Errorf("reservation.concurrency: lease_id and positive slots are required")
	}
	if err := p.Telemetry.validate(); err != nil {
		return fmt.Errorf("telemetry: %w", err)
	}
	if p.Execution.CredentialSource == CredentialsManaged && p.Telemetry == (Telemetry{}) {
		return fmt.Errorf("telemetry: managed plans require an authenticated destination")
	}
	if p.Fallback != nil {
		if err := validateSecureURL(p.Fallback.ExchangeURL, "https"); err != nil {
			return fmt.Errorf("fallback.exchange_url: %w", err)
		}
	}
	if strings.TrimSpace(p.Signature) == "" {
		return fmt.Errorf("signature: required")
	}
	return nil
}

func (p SessionPlan) validateCredential(now time.Time) error {
	credential := p.Route.Credential
	if p.Execution.CredentialSource == CredentialsBYOK {
		if credential != nil {
			return fmt.Errorf("route.credential: byok plans must not carry a provider credential")
		}
		return nil
	}
	if credential == nil {
		return fmt.Errorf("route.credential: managed plans require a delegated credential")
	}
	if err := credential.validate(); err != nil {
		return fmt.Errorf("route.credential: %w", err)
	}
	if !credential.ExpiresAt.After(now) {
		return fmt.Errorf("route.credential: credential is expired")
	}
	if credential.ExpiresAt.After(p.ExpiresAt) {
		return fmt.Errorf("route.credential: expiry must not outlive the plan")
	}
	if p.Execution.ProviderRoute == RouteSpekoRelay && credential.Kind != CredentialRelayAccess {
		return fmt.Errorf("route.credential: relay plans require relay_access credentials")
	}
	if p.Execution.ProviderRoute == RouteProviderDirect && credential.Kind == CredentialRelayAccess {
		return fmt.Errorf("route.credential: provider-direct plans cannot use relay_access credentials")
	}
	return nil
}

func (r RuntimeDescriptor) validate() error {
	if strings.TrimSpace(r.Name) == "" || strings.TrimSpace(r.Version) == "" || strings.TrimSpace(r.InstanceID) == "" {
		return fmt.Errorf("name, version, and instance_id are required")
	}
	if !validPlacement(r.Placement) {
		return fmt.Errorf("placement: unsupported value %q", r.Placement)
	}
	if len(r.ProviderRoutes) == 0 {
		return fmt.Errorf("provider_routes: at least one concrete route is required")
	}
	for _, route := range r.ProviderRoutes {
		if route != RouteProviderDirect && route != RouteSpekoRelay {
			return fmt.Errorf("provider_routes: unsupported value %q", route)
		}
	}
	return nil
}

func (e ExecutionRequest) validateRequest(runtimeRoutes []ProviderRoute) error {
	if e.ProviderRoute != RouteAuto && e.ProviderRoute != RouteProviderDirect && e.ProviderRoute != RouteSpekoRelay {
		return fmt.Errorf("provider_route: unsupported value %q", e.ProviderRoute)
	}
	if !validCredentialSource(e.CredentialSource) {
		return fmt.Errorf("credential_source: unsupported value %q", e.CredentialSource)
	}
	if !validRelayPolicy(e.RelayPolicy) {
		return fmt.Errorf("relay_policy: unsupported value %q", e.RelayPolicy)
	}
	if e.CredentialSource == CredentialsBYOK && e.ProviderRoute == RouteSpekoRelay {
		return fmt.Errorf("byok cannot use speko_relay")
	}
	if e.CredentialSource == CredentialsBYOK && e.RelayPolicy == RelayRequired {
		return fmt.Errorf("byok cannot require speko_relay")
	}
	if e.RelayPolicy == RelayRequired && e.ProviderRoute == RouteProviderDirect {
		return fmt.Errorf("provider_direct conflicts with required relay policy")
	}
	if e.RelayPolicy == RelayForbidden && e.ProviderRoute == RouteSpekoRelay {
		return fmt.Errorf("speko_relay conflicts with forbidden relay policy")
	}
	if e.ProviderRoute != RouteAuto && !containsRoute(runtimeRoutes, e.ProviderRoute) {
		return fmt.Errorf("provider_route %q is not supported by the runtime", e.ProviderRoute)
	}
	return nil
}

func (e Execution) validatePlan() error {
	if !validPlacement(e.Placement) {
		return fmt.Errorf("placement: unsupported value %q", e.Placement)
	}
	if e.ProviderRoute != RouteProviderDirect && e.ProviderRoute != RouteSpekoRelay {
		return fmt.Errorf("provider_route: plans require a concrete route, got %q", e.ProviderRoute)
	}
	if !validCredentialSource(e.CredentialSource) {
		return fmt.Errorf("credential_source: unsupported value %q", e.CredentialSource)
	}
	if e.ProviderRoute == RouteSpekoRelay && e.CredentialSource != CredentialsManaged {
		return fmt.Errorf("speko_relay requires managed credentials")
	}
	return nil
}

func (m MediaFormat) validate() error {
	if m.Encoding != "pcm_s16le" && m.Encoding != "opus" {
		return fmt.Errorf("encoding: unsupported value %q", m.Encoding)
	}
	if m.SampleRateHz < 8_000 || m.SampleRateHz > 192_000 {
		return fmt.Errorf("sample_rate_hz: must be between 8000 and 192000")
	}
	if m.Channels < 1 || m.Channels > 8 {
		return fmt.Errorf("channels: must be between 1 and 8")
	}
	return nil
}

func (r PlanRoute) validate() error {
	if strings.TrimSpace(r.Provider) == "" || strings.TrimSpace(r.Model) == "" || strings.TrimSpace(r.Adapter) == "" {
		return fmt.Errorf("provider, model, and adapter are required")
	}
	if !validTransport(r.Transport) {
		return fmt.Errorf("transport: unsupported value %q", r.Transport)
	}
	if err := validateEndpoint(r.Endpoint, r.Transport); err != nil {
		return fmt.Errorf("endpoint: %w", err)
	}
	return nil
}

func (c DelegatedCredential) validate() error {
	if !validCredentialKind(c.Kind) {
		return fmt.Errorf("kind: unsupported value %q", c.Kind)
	}
	if strings.TrimSpace(c.Value) == "" || c.ExpiresAt.IsZero() {
		return fmt.Errorf("value and expires_at are required")
	}
	return nil
}

func (u UsageReservation) validate() error {
	if u.Unit != UsageUnitDurationSeconds && u.Unit != UsageUnitCharacters {
		return fmt.Errorf("unit: unsupported value %q", u.Unit)
	}
	if u.AuthorizedUnits <= 0 {
		return fmt.Errorf("authorized_units: must be positive")
	}
	return nil
}

func (t Telemetry) validate() error {
	if t == (Telemetry{}) {
		return nil
	}
	if err := validateSecureURL(t.Endpoint, "https"); err != nil {
		return err
	}
	if strings.TrimSpace(t.Token) == "" {
		return fmt.Errorf("token: required")
	}
	if t.FlushIntervalMS < 1_000 || t.FlushIntervalMS > 60_000 {
		return fmt.Errorf("flush_interval_ms: must be between 1000 and 60000")
	}
	return nil
}

func (r Requirements) validate() error {
	if r.Protocol != VoiceV0 {
		return fmt.Errorf("protocol: got %q, want %q", r.Protocol, VoiceV0)
	}
	if r.ProtocolRevision != CurrentRevision {
		return fmt.Errorf("protocol_revision: got %d, want %d", r.ProtocolRevision, CurrentRevision)
	}
	if strings.TrimSpace(r.RuntimeVersion) == "" {
		return fmt.Errorf("runtime_version: required")
	}
	return nil
}

// Validate checks envelope claims that are independent of the signature. A
// verifier must first verify the JWS and then call this method before using
// any route or credential in the enclosed plan.
func (e SessionPlanEnvelope) Validate(now time.Time, issuer, audience string, clockSkew time.Duration) error {
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

func validateEndpoint(raw string, transport Transport) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return fmt.Errorf("must be an absolute URL")
	}
	switch transport {
	case TransportWebSocket:
		if u.Scheme != "wss" && u.Scheme != "ws" {
			return fmt.Errorf("websocket transport requires ws or wss endpoint")
		}
	case TransportHTTP:
		if u.Scheme != "https" && u.Scheme != "http" {
			return fmt.Errorf("http transport requires http or https endpoint")
		}
	case TransportGRPC:
		if u.Scheme != "https" && u.Scheme != "grpcs" {
			return fmt.Errorf("grpc transport requires https or grpcs endpoint")
		}
	case TransportWebRTC:
		if u.Scheme != "https" && u.Scheme != "wss" {
			return fmt.Errorf("webrtc transport requires https or wss endpoint")
		}
	}
	return nil
}

func validateSecureURL(raw, scheme string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.Scheme != scheme {
		return fmt.Errorf("must be an absolute %s URL", scheme)
	}
	return nil
}

func validKind(v SessionKind) bool {
	return v == SessionKindSTT || v == SessionKindTTS || v == SessionKindLLM || v == SessionKindRealtime
}

func needsMedia(v SessionKind) bool {
	return v == SessionKindSTT || v == SessionKindTTS || v == SessionKindRealtime
}

func validPlacement(v Placement) bool {
	return v == PlacementEmbedded || v == PlacementSidecar || v == PlacementClient
}

func validCredentialSource(v CredentialSource) bool {
	return v == CredentialsManaged || v == CredentialsBYOK
}

func validRelayPolicy(v RelayPolicy) bool {
	return v == RelayRequired || v == RelayAllowed || v == RelayForbidden
}

func validTransport(v Transport) bool {
	return v == TransportWebSocket || v == TransportHTTP || v == TransportGRPC || v == TransportWebRTC
}

func validCredentialKind(v CredentialKind) bool {
	return v == CredentialBearer || v == CredentialSignedURL || v == CredentialSessionURL || v == CredentialRelayAccess
}

func containsRoute(routes []ProviderRoute, want ProviderRoute) bool {
	for _, route := range routes {
		if route == want {
			return true
		}
	}
	return false
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
