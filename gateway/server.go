// Package gateway exposes the canonical Speko Voice Protocol to local
// cross-language clients. It is intentionally a thin transport wrapper around
// runtime.Engine: provider routing and credentials remain in the signed plan.
package gateway

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SpekoAI/gateway/controlplane"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

const (
	// WebSocketSubprotocol is the only streaming wire format supported by this
	// development revision.
	WebSocketSubprotocol      = "speko.voice.v0.r3"
	defaultMaxSessions        = 100
	defaultAttachTimeout      = 30 * time.Second
	defaultStreamWriteTimeout = 15 * time.Second
	defaultSetupTimeout       = 30 * time.Second
	maxRequestBytes           = 64 << 10
)

// PlanClient is the setup-only planning contract consumed by a gateway. It is
// implemented by the Speko client for managed routing and LocalPlanner for
// local routing, and is deliberately absent from runtime.Engine's hot path.
type PlanClient interface {
	CreateSessionPlan(context.Context, protocol.SessionPlanRequest, controlplane.CreateOptions) (protocol.SessionPlan, string, error)
	RenewSessionLease(context.Context, protocol.SessionPlan) (protocol.SessionLease, string, error)
	// ExchangeFallbackPlan performs the signed one-per-attempt fallback
	// exchange when provider opening fails before any output was produced.
	ExchangeFallbackPlan(context.Context, protocol.SessionPlan, controlplane.FallbackRequest, string) (protocol.SessionPlan, string, error)
}

// Config builds a local gateway service. LocalAuthToken is mandatory even for
// a Unix socket: a mounted socket can otherwise be reached by another process
// in the same host or container namespace.
type Config struct {
	Engine         *runtimepkg.Engine
	Plans          PlanClient
	Runtime        protocol.RuntimeDescriptor
	Workload       *protocol.Workload
	LocalAuthToken string
	MaxSessions    int
	// AttachTimeout bounds the time a newly-created provider session may wait
	// for its local WebSocket client. A zero value uses a conservative default.
	AttachTimeout time.Duration
	// StreamWriteTimeout bounds one canonical event write to a local client.
	// A zero value uses a conservative default.
	StreamWriteTimeout time.Duration
	// SetupTimeout bounds control-plane plan issuance and provider opening.
	// A zero value uses a conservative default.
	SetupTimeout time.Duration
	// Now exists for deterministic lease scheduling tests. Production uses the
	// process wall clock.
	Now func() time.Time
}

// Server serves REST setup, canonical WebSocket streaming, readiness/drain,
// and Prometheus metrics. It holds no permanent provider credential.
type Server struct {
	engine             *runtimepkg.Engine
	plans              PlanClient
	runtime            protocol.RuntimeDescriptor
	workload           *protocol.Workload
	localAuthHash      [sha256.Size]byte
	maxSessions        int
	attachTimeout      time.Duration
	streamWriteTimeout time.Duration
	setupTimeout       time.Duration
	now                func() time.Time

	mu              sync.Mutex
	sessions        map[string]*localSession
	idempotency     map[string]idempotencyRecord
	inflight        map[string]*inflightCreate
	pendingSessions int
	draining        bool
	drained         chan struct{}
	drainedClosed   bool
	sessionsActive  atomic.Int64
	sessionsTotal   atomic.Uint64
	authFailures    atomic.Uint64
	leaseRenewals   atomic.Uint64
	leaseFailures   atomic.Uint64
}

// Stats is the bounded, content-free process state safe to report to the
// hosted customer control plane. It intentionally excludes local socket
// paths, host resources, request bodies, and session identifiers.
type Stats struct {
	ActiveSessions  int64
	PendingSessions int
	SessionCapacity int
	SessionsTotal   uint64
	Draining        bool
}

// localSession keeps local lifecycle state that must be updated atomically
// with the session registry. Provider events remain owned by runtime.Session.
type localSession struct {
	session         *runtimepkg.Session
	idempotencyKey  string
	attached        bool
	attachDeadline  time.Time
	attachmentTimer *time.Timer
	leaseCancel     context.CancelFunc
}

// inflightCreate represents the single setup operation permitted for an
// idempotency key while the control plane and provider are being contacted.
// Its result is published before done is closed, so concurrent local retries
// can reuse the leader's outcome without opening another provider session.
type inflightCreate struct {
	done        chan struct{}
	fingerprint [sha256.Size]byte
	outcome     createOutcome
}

// idempotencyRecord is retained only while its provider session is active.
// The fingerprint prevents a key from being reused for a different request.
type idempotencyRecord struct {
	response    createResponse
	fingerprint [sha256.Size]byte
}

type createOutcome struct {
	response  createResponse
	status    int
	code      string
	message   string
	requestID string
}

// CreateSessionRequest contains caller-selectable, provider-neutral data.
// Runtime identity is configured by the gateway and cannot be supplied by the
// local caller.
type CreateSessionRequest struct {
	Kind        protocol.SessionKind      `json:"kind"`
	Integration *protocol.Integration     `json:"integration,omitempty"`
	Execution   protocol.ExecutionRequest `json:"execution"`
	Request     protocol.RequestOptions   `json:"request"`
	Media       *protocol.MediaFormat     `json:"media,omitempty"`
}

type createResponse struct {
	SessionID string             `json:"session_id"`
	AttemptID string             `json:"attempt_id"`
	Execution protocol.Execution `json:"execution"`
	Route     selectedRoute      `json:"route"`
	StreamURL string             `json:"stream_url"`
	RequestID string             `json:"control_plane_request_id,omitempty"`
}

// selectedRoute is safe to return to a local caller. It intentionally omits
// endpoint query data and any delegated provider credential.
type selectedRoute struct {
	Provider  string             `json:"provider"`
	Model     string             `json:"model"`
	Region    string             `json:"region,omitempty"`
	Adapter   string             `json:"adapter"`
	Transport protocol.Transport `json:"transport"`
}

type clientCommand struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data,omitempty"`
}

// New validates static gateway configuration.
func New(config Config) (*Server, error) {
	if config.Engine == nil || config.Plans == nil {
		return nil, errors.New("gateway: engine and plan client are required")
	}
	if strings.TrimSpace(config.LocalAuthToken) == "" {
		return nil, errors.New("gateway: local auth token is required")
	}
	if config.Runtime.Placement != protocol.PlacementSidecar || strings.TrimSpace(config.Runtime.Name) == "" || strings.TrimSpace(config.Runtime.Version) == "" || strings.TrimSpace(config.Runtime.InstanceID) == "" || len(config.Runtime.ProviderRoutes) != 1 || config.Runtime.ProviderRoutes[0] != protocol.RouteProviderDirect {
		return nil, errors.New("gateway: runtime must be a complete provider-direct descriptor")
	}
	if config.Workload != nil {
		if err := config.Workload.Validate(); err != nil {
			return nil, fmt.Errorf("gateway: invalid workload: %w", err)
		}
	}
	if config.MaxSessions == 0 {
		config.MaxSessions = defaultMaxSessions
	}
	if config.MaxSessions < 1 {
		return nil, errors.New("gateway: max sessions must be positive")
	}
	if config.AttachTimeout == 0 {
		config.AttachTimeout = defaultAttachTimeout
	}
	if config.AttachTimeout < 0 {
		return nil, errors.New("gateway: attach timeout must not be negative")
	}
	if config.StreamWriteTimeout == 0 {
		config.StreamWriteTimeout = defaultStreamWriteTimeout
	}
	if config.StreamWriteTimeout < 0 {
		return nil, errors.New("gateway: stream write timeout must not be negative")
	}
	if config.SetupTimeout == 0 {
		config.SetupTimeout = defaultSetupTimeout
	}
	if config.SetupTimeout < 0 {
		return nil, errors.New("gateway: setup timeout must not be negative")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &Server{
		engine:             config.Engine,
		plans:              config.Plans,
		runtime:            config.Runtime,
		workload:           config.Workload,
		localAuthHash:      sha256.Sum256([]byte(config.LocalAuthToken)),
		maxSessions:        config.MaxSessions,
		attachTimeout:      config.AttachTimeout,
		streamWriteTimeout: config.StreamWriteTimeout,
		setupTimeout:       config.SetupTimeout,
		now:                config.Now,
		sessions:           make(map[string]*localSession),
		idempotency:        make(map[string]idempotencyRecord),
		inflight:           make(map[string]*inflightCreate),
		drained:            make(chan struct{}),
	}, nil
}

// Handler returns the local HTTP handler. Bind it only to a Unix socket or
// loopback listener; Handler itself intentionally does not make a public
// network listener safe.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /readyz", s.ready)
	mux.HandleFunc("GET /metrics", s.metrics)
	mux.HandleFunc("POST /v1/sessions", s.createSession)
	mux.HandleFunc("GET /v1/sessions/{session_id}/stream", s.streamSession)
	mux.HandleFunc("DELETE /v1/sessions/{session_id}", s.deleteSession)
	return mux
}

// Stats returns a point-in-time snapshot for instance heartbeats and local
// operational tooling.
func (s *Server) Stats() Stats {
	s.mu.Lock()
	pending := s.pendingSessions
	draining := s.draining
	s.mu.Unlock()
	return Stats{
		ActiveSessions:  s.sessionsActive.Load(),
		PendingSessions: pending,
		SessionCapacity: s.maxSessions,
		SessionsTotal:   s.sessionsTotal.Load(),
		Draining:        draining,
	}
}

// BeginDrain atomically stops new session creation without waiting for active
// sessions. Callers can report the draining state before they wait or exit.
func (s *Server) BeginDrain() {
	s.mu.Lock()
	if !s.draining {
		s.draining = true
		s.closeDrainedLocked()
	}
	s.mu.Unlock()
}

// Drain stops new session creation, allows existing sessions to finish, and
// returns once all are gone or ctx expires. Existing WebSockets remain active.
func (s *Server) Drain(ctx context.Context) error {
	s.BeginDrain()
	s.mu.Lock()
	drained := s.drained
	s.mu.Unlock()
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) health(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) ready(writer http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	draining := s.draining
	s.mu.Unlock()
	if draining {
		writeJSON(writer, http.StatusServiceUnavailable, map[string]string{"status": "draining"})
		return
	}
	writeJSON(writer, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) metrics(writer http.ResponseWriter, request *http.Request) {
	if !s.authorize(writer, request) {
		return
	}
	s.mu.Lock()
	draining := s.draining
	var inputMessages, inputBytes, telemetryDropped uint64
	for _, local := range s.sessions {
		stats := local.session.Stats()
		inputMessages += uint64(stats.InputMessages)
		inputBytes += uint64(stats.InputBytes)
		telemetryDropped += stats.TelemetryDropped
	}
	s.mu.Unlock()
	writer.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = fmt.Fprintf(writer, "# HELP speko_gateway_sessions_active Active local sessions.\n# TYPE speko_gateway_sessions_active gauge\nspeko_gateway_sessions_active %d\n", s.sessionsActive.Load())
	_, _ = fmt.Fprintf(writer, "# HELP speko_gateway_sessions_total Sessions opened by this process.\n# TYPE speko_gateway_sessions_total counter\nspeko_gateway_sessions_total %d\n", s.sessionsTotal.Load())
	_, _ = fmt.Fprintf(writer, "# HELP speko_gateway_draining Whether new sessions are refused.\n# TYPE speko_gateway_draining gauge\nspeko_gateway_draining %d\n", boolMetric(draining))
	_, _ = fmt.Fprintf(writer, "# HELP speko_gateway_auth_failures_total Rejected local authentication attempts.\n# TYPE speko_gateway_auth_failures_total counter\nspeko_gateway_auth_failures_total %d\n", s.authFailures.Load())
	_, _ = fmt.Fprintf(writer, "# HELP speko_gateway_lease_renewals_total Successful session lease renewals.\n# TYPE speko_gateway_lease_renewals_total counter\nspeko_gateway_lease_renewals_total %d\n", s.leaseRenewals.Load())
	_, _ = fmt.Fprintf(writer, "# HELP speko_gateway_lease_renewal_failures_total Failed lease renewal requests.\n# TYPE speko_gateway_lease_renewal_failures_total counter\nspeko_gateway_lease_renewal_failures_total %d\n", s.leaseFailures.Load())
	_, _ = fmt.Fprintf(writer, "# HELP speko_gateway_input_queue_messages Total queued session input messages.\n# TYPE speko_gateway_input_queue_messages gauge\nspeko_gateway_input_queue_messages %d\n", inputMessages)
	_, _ = fmt.Fprintf(writer, "# HELP speko_gateway_input_queue_bytes Total queued session input bytes.\n# TYPE speko_gateway_input_queue_bytes gauge\nspeko_gateway_input_queue_bytes %d\n", inputBytes)
	_, _ = fmt.Fprintf(writer, "# HELP speko_gateway_telemetry_dropped_total Telemetry events dropped under pressure.\n# TYPE speko_gateway_telemetry_dropped_total counter\nspeko_gateway_telemetry_dropped_total %d\n", telemetryDropped)
}

func (s *Server) createSession(writer http.ResponseWriter, request *http.Request) {
	if !s.authorize(writer, request) {
		return
	}
	idempotencyKey := request.Header.Get("Idempotency-Key")
	if strings.TrimSpace(idempotencyKey) == "" {
		writeError(writer, http.StatusBadRequest, "idempotency_key_required", "Idempotency-Key is required")
		return
	}
	var body CreateSessionRequest
	decoder := json.NewDecoder(io.LimitReader(request.Body, maxRequestBytes+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", "invalid session request")
		return
	}
	fingerprint, err := fingerprintCreateRequest(body)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "request_fingerprint_failed", "session request could not be fingerprinted")
		return
	}
	s.mu.Lock()
	if s.draining {
		s.mu.Unlock()
		writeError(writer, http.StatusServiceUnavailable, "gateway_draining", "gateway is draining")
		return
	}
	if record, exists := s.idempotency[idempotencyKey]; exists {
		if record.fingerprint != fingerprint {
			s.mu.Unlock()
			writeIdempotencyConflict(writer)
			return
		}
		s.mu.Unlock()
		writeJSON(writer, http.StatusOK, record.response)
		return
	}
	if inflight, exists := s.inflight[idempotencyKey]; exists {
		if inflight.fingerprint != fingerprint {
			s.mu.Unlock()
			writeIdempotencyConflict(writer)
			return
		}
		s.mu.Unlock()
		s.waitForCreate(writer, request, inflight)
		return
	}
	if len(s.sessions)+s.pendingSessions >= s.maxSessions {
		s.mu.Unlock()
		writeError(writer, http.StatusTooManyRequests, "local_concurrency_exhausted", "gateway session limit reached")
		return
	}
	inflight := &inflightCreate{done: make(chan struct{}), fingerprint: fingerprint}
	s.inflight[idempotencyKey] = inflight
	s.pendingSessions++
	s.mu.Unlock()
	defer s.finishPendingSession()
	// Creation is shared by all callers with this idempotency key. It must not
	// inherit cancellation from whichever request happened to become leader.
	setupCtx, cancel := context.WithTimeout(context.Background(), s.setupTimeout)
	defer cancel()
	planRequest := protocol.SessionPlanRequest{
		Kind: body.Kind, Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision,
		Runtime: s.runtime, Workload: s.workload, Integration: body.Integration, Execution: body.Execution, Request: body.Request, Media: body.Media,
	}
	plan, requestID, err := s.plans.CreateSessionPlan(setupCtx, planRequest, controlplane.CreateOptions{IdempotencyKey: idempotencyKey})
	if err != nil {
		outcome := planFailure(err, requestID)
		s.finishCreate(idempotencyKey, inflight, outcome)
		writeCreateOutcome(writer, outcome, false)
		return
	}
	session, plan, requestID, err := s.openWithFallback(setupCtx, body, plan, requestID, idempotencyKey)
	if err != nil {
		outcome := createOutcome{status: http.StatusBadGateway, code: "session_open_failed", message: "provider session could not be opened"}
		s.finishCreate(idempotencyKey, inflight, outcome)
		writeCreateOutcome(writer, outcome, false)
		return
	}
	response := createResponse{
		SessionID: plan.SessionID, AttemptID: plan.AttemptID, Execution: plan.Execution,
		Route:     selectedRoute{Provider: plan.Route.Provider, Model: plan.Route.Model, Region: plan.Route.Region, Adapter: plan.Route.Adapter, Transport: plan.Route.Transport},
		StreamURL: "/v1/sessions/" + url.PathEscape(plan.SessionID) + "/stream", RequestID: requestID,
	}
	s.mu.Lock()
	if s.draining {
		s.mu.Unlock()
		session.Close()
		outcome := createOutcome{status: http.StatusServiceUnavailable, code: "gateway_draining", message: "gateway is draining"}
		s.finishCreate(idempotencyKey, inflight, outcome)
		writeCreateOutcome(writer, outcome, false)
		return
	}
	if existing, exists := s.sessions[plan.SessionID]; exists {
		s.mu.Unlock()
		session.Close()
		_ = existing
		outcome := createOutcome{status: http.StatusConflict, code: "duplicate_session", message: "control plane returned an active session"}
		s.finishCreate(idempotencyKey, inflight, outcome)
		writeCreateOutcome(writer, outcome, false)
		return
	}
	leaseCtx, leaseCancel := context.WithCancel(context.Background())
	local := &localSession{
		session:        session,
		idempotencyKey: idempotencyKey,
		attachDeadline: s.now().Add(s.attachTimeout),
		leaseCancel:    leaseCancel,
	}
	s.sessions[plan.SessionID] = local
	s.idempotency[idempotencyKey] = idempotencyRecord{response: response, fingerprint: fingerprint}
	s.sessionsActive.Add(1)
	s.sessionsTotal.Add(1)
	local.attachmentTimer = time.AfterFunc(s.attachTimeout, func() {
		s.expireUnattached(plan.SessionID, session)
	})
	s.mu.Unlock()
	go s.removeWhenDone(plan.SessionID, session)
	if plan.Reservation.RenewalURL != "" {
		go s.renewSessionLease(leaseCtx, session, plan)
	}
	outcome := createOutcome{response: response, status: http.StatusCreated}
	s.finishCreate(idempotencyKey, inflight, outcome)
	writeCreateOutcome(writer, outcome, false)
}

func (s *Server) renewSessionLease(ctx context.Context, session *runtimepkg.Session, plan protocol.SessionPlan) {
	current := plan
	retryDelay := 500 * time.Millisecond
	renewAt := current.Reservation.LeaseExpiresAt.Add(-time.Duration(current.Reservation.LeaseDurationSeconds) * time.Second / 3)
	for {
		if renewAt.Before(s.now()) {
			renewAt = s.now()
		}
		timer := time.NewTimer(renewAt.Sub(s.now()))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-session.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		remainingBeforeCall := current.Reservation.LeaseExpiresAt.Sub(s.now())
		callTimeout := s.setupTimeout
		if halfRemaining := remainingBeforeCall / 2; halfRemaining > 0 && callTimeout > halfRemaining {
			callTimeout = halfRemaining
		}
		if callTimeout > 5*time.Second {
			callTimeout = 5 * time.Second
		}
		if callTimeout <= 0 {
			return
		}
		callCtx, cancel := context.WithTimeout(ctx, callTimeout)
		lease, _, err := s.plans.RenewSessionLease(callCtx, current)
		cancel()
		if err == nil {
			if err := session.RenewLease(lease); err != nil {
				s.leaseFailures.Add(1)
				session.RejectLeaseRenewal()
				return
			}
			current.Reservation.LeaseExpiresAt = lease.ExpiresAt
			renewAt = lease.RenewAfter
			s.leaseRenewals.Add(1)
			retryDelay = 500 * time.Millisecond
			continue
		}
		s.leaseFailures.Add(1)
		if leaseRenewalExplicitlyDenied(err) {
			session.RejectLeaseRenewal()
			return
		}
		// A transport or 5xx failure does not disturb the provider stream. Retry
		// within the current lease; the runtime's deadline remains the final
		// fail-closed guard if this process or the control plane stays unavailable.
		remaining := current.Reservation.LeaseExpiresAt.Sub(s.now())
		if remaining <= 0 {
			return
		}
		wait := retryDelay
		if wait > remaining {
			wait = remaining
		}
		select {
		case <-ctx.Done():
			return
		case <-session.Done():
			return
		case <-time.After(wait):
		}
		if retryDelay < 5*time.Second {
			retryDelay *= 2
		}
		// Retry immediately instead of waiting for the normal renewal point.
		renewAt = s.now()
	}
}

func leaseRenewalExplicitlyDenied(err error) bool {
	var httpError *controlplane.HTTPError
	if !errors.As(err, &httpError) {
		return false
	}
	return httpError.Status >= 400 && httpError.Status < 500 && httpError.Status != http.StatusRequestTimeout
}

// openWithFallback opens the provider session and performs at most one
// control-plane fallback exchange when a provider-classified opening failure
// occurs before any output. Non-provider failures (plan validation, signature
// verification, adapter wiring) never consume the single exchange, and the
// whole recovery stays inside the caller's bounded setup context. A failed
// exchange reports the original open failure; a failed reopen is terminal.
func (s *Server) openWithFallback(ctx context.Context, body CreateSessionRequest, plan protocol.SessionPlan, requestID, idempotencyKey string) (*runtimepkg.Session, protocol.SessionPlan, string, error) {
	session, err := s.engine.Open(ctx, runtimepkg.OpenRequest{Kind: body.Kind, Plan: plan, Options: body.Request, Media: body.Media})
	if err == nil {
		return session, plan, requestID, nil
	}
	var providerError *runtimepkg.ProviderError
	if plan.Fallback == nil || !errors.As(err, &providerError) {
		return nil, plan, requestID, err
	}
	// The exchange needs its own idempotency key: the create key already
	// identifies the plan-issuance request in a different ledger.
	fallbackPlan, fallbackRequestID, exchangeErr := s.plans.ExchangeFallbackPlan(ctx, plan, controlplane.FallbackRequest{
		AttemptID:      plan.AttemptID,
		Reason:         "provider_open_failed",
		ProviderCode:   providerError.Code,
		ProviderStatus: providerError.ProviderStatus,
	}, idempotencyKey+":fallback:"+plan.AttemptID)
	if exchangeErr != nil {
		return nil, plan, requestID, err
	}
	if fallbackRequestID == "" {
		fallbackRequestID = requestID
	}
	session, retryErr := s.engine.Open(ctx, runtimepkg.OpenRequest{Kind: body.Kind, Plan: fallbackPlan, Options: body.Request, Media: body.Media})
	if retryErr != nil {
		return nil, fallbackPlan, fallbackRequestID, retryErr
	}
	return session, fallbackPlan, fallbackRequestID, nil
}

func (s *Server) streamSession(writer http.ResponseWriter, request *http.Request) {
	if !s.authorize(writer, request) {
		return
	}
	if !requestedSubprotocol(request, WebSocketSubprotocol) {
		writeError(writer, http.StatusBadRequest, "unsupported_protocol", "canonical WebSocket subprotocol is required")
		return
	}
	session, found, claimed := s.claimSession(request.PathValue("session_id"))
	if !found {
		writeError(writer, http.StatusNotFound, "session_not_found", "session does not exist")
		return
	}
	if !claimed {
		writeError(writer, http.StatusConflict, "stream_already_attached", "session stream is already attached")
		return
	}
	connection, err := websocket.Accept(writer, request, &websocket.AcceptOptions{Subprotocols: []string{WebSocketSubprotocol}})
	if err != nil {
		s.releaseSessionAttachment(request.PathValue("session_id"), session)
		return
	}
	defer connection.Close(websocket.StatusNormalClosure, "")
	writeDone := make(chan error, 1)
	go func() { writeDone <- s.writeSessionEvents(request.Context(), connection, session) }()
	readDone := make(chan error, 1)
	go func() { readDone <- readSessionInput(request.Context(), connection, session) }()
	var readErr, writeErr error
	select {
	case readErr = <-readDone:
		if readErr != nil {
			session.Abort()
		} else {
			session.Close()
		}
		writeErr = <-writeDone
	case writeErr = <-writeDone:
		if writeErr != nil {
			session.Abort()
		} else {
			session.Close()
		}
		_ = connection.Close(websocket.StatusNormalClosure, "session ended")
		readErr = <-readDone
	}
	if readErr != nil || writeErr != nil {
		return
	}
}

func (s *Server) deleteSession(writer http.ResponseWriter, request *http.Request) {
	if !s.authorize(writer, request) {
		return
	}
	session, ok := s.detachSession(request.PathValue("session_id"), nil)
	if !ok {
		writeError(writer, http.StatusNotFound, "session_not_found", "session does not exist")
		return
	}
	session.Abort()
	writer.WriteHeader(http.StatusNoContent)
}

func (s *Server) removeWhenDone(sessionID string, session *runtimepkg.Session) {
	<-session.Done()
	s.mu.Lock()
	_, _ = s.detachSessionLocked(sessionID, session)
	s.mu.Unlock()
}

// detachSession removes a session from every replayable local registry before
// caller-controlled teardown begins. A retry after DELETE therefore cannot
// receive a stream URL for a session that is closing in the provider.
func (s *Server) detachSession(sessionID string, expected *runtimepkg.Session) (*runtimepkg.Session, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.detachSessionLocked(sessionID, expected)
}

func (s *Server) detachSessionLocked(sessionID string, expected *runtimepkg.Session) (*runtimepkg.Session, bool) {
	local, exists := s.sessions[sessionID]
	if !exists || (expected != nil && local.session != expected) {
		return nil, false
	}
	if local.attachmentTimer != nil {
		local.attachmentTimer.Stop()
	}
	if local.leaseCancel != nil {
		local.leaseCancel()
	}
	delete(s.sessions, sessionID)
	if record, exists := s.idempotency[local.idempotencyKey]; exists && record.response.SessionID == sessionID {
		delete(s.idempotency, local.idempotencyKey)
	}
	s.sessionsActive.Add(-1)
	s.closeDrainedLocked()
	return local.session, true
}

func (s *Server) finishPendingSession() {
	s.mu.Lock()
	s.pendingSessions--
	s.closeDrainedLocked()
	s.mu.Unlock()
}

func (s *Server) closeDrainedLocked() {
	if s.draining && !s.drainedClosed && len(s.sessions)+s.pendingSessions == 0 {
		s.drainedClosed = true
		close(s.drained)
	}
}

func (s *Server) getSession(id string) (*runtimepkg.Session, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	local, ok := s.sessions[id]
	if !ok {
		return nil, false
	}
	return local.session, true
}

// claimSession makes a canonical WebSocket stream single-consumer. The claim
// is made before the upgrade so two racing requests cannot split Event reads.
func (s *Server) claimSession(id string) (*runtimepkg.Session, bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	local, found := s.sessions[id]
	if !found {
		return nil, false, false
	}
	if local.attached {
		return nil, true, false
	}
	local.attached = true
	if local.attachmentTimer != nil {
		local.attachmentTimer.Stop()
	}
	return local.session, true, true
}

// releaseSessionAttachment restores the creation-time attachment deadline if
// the HTTP upgrade fails before a WebSocket has been established.
func (s *Server) releaseSessionAttachment(id string, session *runtimepkg.Session) {
	var expired *runtimepkg.Session
	s.mu.Lock()
	if local, exists := s.sessions[id]; exists && local.session == session && local.attached {
		local.attached = false
		if remaining := local.attachDeadline.Sub(s.now()); remaining > 0 {
			local.attachmentTimer = time.AfterFunc(remaining, func() {
				s.expireUnattached(id, session)
			})
		} else {
			expired = session
		}
	}
	s.mu.Unlock()
	if expired != nil {
		expired.Close()
	}
}

// expireUnattached reclaims capacity from a caller that created a session but
// never established its required canonical stream.
func (s *Server) expireUnattached(id string, session *runtimepkg.Session) {
	s.mu.Lock()
	local, exists := s.sessions[id]
	var expired *runtimepkg.Session
	if exists && local.session == session && !local.attached {
		expired, _ = s.detachSessionLocked(id, session)
	}
	s.mu.Unlock()
	if expired != nil {
		expired.Abort()
	}
}

// waitForCreate waits only for the leader associated with this idempotency
// key. A waiting request may be canceled without affecting the leader.
func (s *Server) waitForCreate(writer http.ResponseWriter, request *http.Request, inflight *inflightCreate) {
	select {
	case <-inflight.done:
		writeCreateOutcome(writer, inflight.outcome, true)
	case <-request.Context().Done():
	}
}

func fingerprintCreateRequest(request CreateSessionRequest) ([sha256.Size]byte, error) {
	canonical, err := json.Marshal(request)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(canonical), nil
}

func writeIdempotencyConflict(writer http.ResponseWriter) {
	writeError(writer, http.StatusConflict, "idempotency_key_conflict", "Idempotency-Key is already associated with a different session request")
}

func (s *Server) finishCreate(idempotencyKey string, inflight *inflightCreate, outcome createOutcome) {
	s.mu.Lock()
	if current, exists := s.inflight[idempotencyKey]; exists && current == inflight {
		current.outcome = outcome
		delete(s.inflight, idempotencyKey)
		close(current.done)
	}
	s.mu.Unlock()
}

func planFailure(err error, requestID string) createOutcome {
	status := http.StatusBadGateway
	code := "control_plane_unavailable"
	if errors.Is(err, context.DeadlineExceeded) {
		status = http.StatusGatewayTimeout
		code = "control_plane_timeout"
	}
	var controlError *controlplane.HTTPError
	if errors.As(err, &controlError) {
		status = controlError.Status
		code = "control_plane_rejected"
	}
	return createOutcome{
		status: status, code: code, message: "control plane did not issue a session plan", requestID: requestID,
	}
}

func writeCreateOutcome(writer http.ResponseWriter, outcome createOutcome, replay bool) {
	if outcome.code != "" {
		if outcome.requestID != "" {
			writer.Header().Set("X-Control-Plane-Request-ID", outcome.requestID)
		}
		writeError(writer, outcome.status, outcome.code, outcome.message)
		return
	}
	status := outcome.status
	if replay {
		status = http.StatusOK
	}
	writeJSON(writer, status, outcome.response)
}

func (s *Server) authorize(writer http.ResponseWriter, request *http.Request) bool {
	provided := request.Header.Get("Authorization")
	if !strings.HasPrefix(provided, "Bearer ") {
		s.authFailures.Add(1)
		writer.Header().Set("WWW-Authenticate", "Bearer")
		writeError(writer, http.StatusUnauthorized, "local_auth_required", "local gateway authentication is required")
		return false
	}
	providedHash := sha256.Sum256([]byte(strings.TrimPrefix(provided, "Bearer ")))
	if subtle.ConstantTimeCompare(providedHash[:], s.localAuthHash[:]) != 1 {
		s.authFailures.Add(1)
		writer.Header().Set("WWW-Authenticate", "Bearer")
		writeError(writer, http.StatusUnauthorized, "local_auth_required", "local gateway authentication is required")
		return false
	}
	return true
}

func requestedSubprotocol(request *http.Request, wanted string) bool {
	for _, candidate := range strings.Split(request.Header.Get("Sec-WebSocket-Protocol"), ",") {
		if strings.TrimSpace(candidate) == wanted {
			return true
		}
	}
	return false
}

func readSessionInput(ctx context.Context, connection *websocket.Conn, session *runtimepkg.Session) error {
	for {
		messageType, payload, err := connection.Read(ctx)
		if err != nil {
			return err
		}
		if messageType == websocket.MessageBinary {
			if err := session.SubmitAudio(runtimepkg.AudioInput{Data: payload}); err != nil {
				return err
			}
			continue
		}
		if messageType != websocket.MessageText {
			return errors.New("gateway: unsupported websocket message")
		}
		var command clientCommand
		if err := json.Unmarshal(payload, &command); err != nil {
			return errors.New("gateway: invalid websocket command")
		}
		switch command.Type {
		case "audio.commit":
			err = session.CommitAudio()
		case "text.append":
			var data struct {
				Text string `json:"text"`
			}
			if json.Unmarshal(command.Data, &data) != nil {
				return errors.New("gateway: text.append requires data.text")
			}
			err = session.AppendText(data.Text)
		case "text.commit":
			err = session.CommitText()
		case "response.cancel":
			err = session.Cancel()
		case "session.close":
			session.Close()
			return nil
		default:
			return errors.New("gateway: unsupported websocket command")
		}
		if err != nil {
			return err
		}
	}
}

func (s *Server) writeSessionEvents(ctx context.Context, connection *websocket.Conn, session *runtimepkg.Session) error {
	for event := range session.Events() {
		if event.Type == protocol.EventAudioFrame {
			if err := s.writeStreamMessage(ctx, connection, websocket.MessageBinary, event.Audio); err != nil {
				return err
			}
			continue
		}
		payload, err := json.Marshal(event)
		if err != nil {
			return err
		}
		if err := s.writeStreamMessage(ctx, connection, websocket.MessageText, payload); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) writeStreamMessage(ctx context.Context, connection *websocket.Conn, messageType websocket.MessageType, payload []byte) error {
	writeCtx, cancel := context.WithTimeout(ctx, s.streamWriteTimeout)
	defer cancel()
	return connection.Write(writeCtx, messageType, payload)
}

func writeError(writer http.ResponseWriter, status int, code, message string) {
	writeJSON(writer, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func boolMetric(value bool) int {
	if value {
		return 1
	}
	return 0
}

// ListenUnix creates an owner-only Unix socket listener. It will not remove an
// existing filesystem entry, avoiding an accidental unlink of an unexpected
// path during service startup.
func ListenUnix(socketPath string) (net.Listener, error) {
	if strings.TrimSpace(socketPath) == "" || !path.IsAbs(socketPath) {
		return nil, errors.New("gateway: unix socket path must be absolute")
	}
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("gateway: listen unix socket: %w", err)
	}
	if err := setSocketMode(socketPath); err != nil {
		_ = listener.Close()
		return nil, err
	}
	return listener, nil
}

func setSocketMode(socketPath string) error {
	// Kept in one small function so non-Unix builds can replace this behavior
	// without changing the HTTP server contract.
	return setSocketPermissions(socketPath)
}

// Serve serves listener connections. Calling Drain controls readiness and
// active sessions; callers own listener shutdown after draining completes.
func (s *Server) Serve(listener net.Listener) error {
	server := &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	return server.Serve(listener)
}
