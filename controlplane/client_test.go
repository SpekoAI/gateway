package controlplane_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/controlplane"
	"github.com/SpekoAI/gateway/protocol"
)

func TestCreateSessionPlanSendsAPIKeyIdempotencyAndRevision(t *testing.T) {
	t.Parallel()
	plan := validPlan()
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/v1/session-plans" {
			t.Fatalf("request = %s %s", request.Method, request.URL.Path)
		}
		if got := request.Header.Get("Authorization"); got != "Bearer sk_speko_test" {
			t.Fatalf("Authorization = %q", got)
		}
		if got := request.Header.Get("Idempotency-Key"); got != "create-1" {
			t.Fatalf("Idempotency-Key = %q", got)
		}
		if got := request.Header.Get("Speko-Protocol-Revision"); got != "3" {
			t.Fatalf("Speko-Protocol-Revision = %q", got)
		}
		var received protocol.SessionPlanRequest
		if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if err := received.Validate(); err != nil {
			t.Fatalf("request must be valid: %v", err)
		}
		writer.Header().Set("X-Request-ID", "cp_create_123")
		_ = json.NewEncoder(writer).Encode(plan)
	}))
	t.Cleanup(server.Close)
	client := newClient(t, server, "sk_speko_test")

	got, requestID, err := client.CreateSessionPlan(context.Background(), validRequest(), controlplane.CreateOptions{IdempotencyKey: "create-1"})
	if err != nil {
		t.Fatalf("CreateSessionPlan: %v", err)
	}
	if got.PlanID != plan.PlanID || requestID != "cp_create_123" {
		t.Fatalf("response = %+v, request_id = %q", got, requestID)
	}
}

func TestFallbackPlanExchange(t *testing.T) {
	t.Parallel()
	plan := validPlan()
	var serverURL string
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if got := request.Header.Get("Authorization"); got != "Bearer sk_speko_test" {
			t.Fatalf("Authorization = %q", got)
		}
		writer.Header().Set("X-Request-ID", "cp_fallback_456")
		switch request.URL.Path {
		case "/v1/session-plans":
			plan.Fallback = &protocol.Fallback{ExchangeURL: serverURL + "/v1/sessions/session-test/fallback-plans"}
			_ = json.NewEncoder(writer).Encode(map[string]any{"plan": plan})
		case "/v1/sessions/session-test/fallback-plans":
			var body controlplane.FallbackRequest
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Fatalf("decode fallback request: %v", err)
			}
			if body.AttemptID != plan.AttemptID || body.Reason != "provider_connection_failed" || body.ProviderStatus != 503 {
				t.Fatalf("fallback request = %+v", body)
			}
			if got := request.Header.Get("Idempotency-Key"); got != "fallback-1" {
				t.Fatalf("fallback idempotency key = %q", got)
			}
			plan.AttemptID = "attempt-fallback"
			plan.Signature = "signed-fallback-plan"
			_ = json.NewEncoder(writer).Encode(plan)
		default:
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(server.Close)
	serverURL = server.URL
	client := newClient(t, server, "sk_speko_test")

	current, _, err := client.CreateSessionPlan(context.Background(), validRequest(), controlplane.CreateOptions{IdempotencyKey: "create-client-1"})
	if err != nil {
		t.Fatalf("create session plan: %v", err)
	}
	got, requestID, err := client.ExchangeFallbackPlan(context.Background(), current, controlplane.FallbackRequest{AttemptID: current.AttemptID, Reason: "provider_connection_failed", ProviderStatus: 503}, "fallback-1")
	if err != nil {
		t.Fatalf("ExchangeFallbackPlan: %v", err)
	}
	if got.AttemptID != "attempt-fallback" || requestID != "cp_fallback_456" {
		t.Fatalf("fallback response = %+v, request_id=%q", got, requestID)
	}
}

func TestFallbackPlanCannotForwardAPIKeyToAnotherOrigin(t *testing.T) {
	t.Parallel()
	var attackerRequests atomic.Int32
	attacker := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		attackerRequests.Add(1)
	}))
	t.Cleanup(attacker.Close)
	trusted := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(trusted.Close)
	client := newClient(t, trusted, "permanent-speko-key")
	plan := validPlan()
	plan.Fallback = &protocol.Fallback{ExchangeURL: attacker.URL + "/fallback"}

	_, _, err := client.ExchangeFallbackPlan(context.Background(), plan, controlplane.FallbackRequest{
		AttemptID: plan.AttemptID, Reason: "provider_connection_failed",
	}, "fallback-cross-origin")
	if err == nil || !strings.Contains(err.Error(), "configured control-plane origin") {
		t.Fatalf("cross-origin fallback error = %v", err)
	}
	if attackerRequests.Load() != 0 {
		t.Fatal("cross-origin fallback received the Speko API key")
	}
}

func TestCreateSessionPlanReturnsRequestIDOnControlPlaneError(t *testing.T) {
	t.Parallel()
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Request-ID", "cp_rejected")
		writer.WriteHeader(http.StatusTooManyRequests)
		_, _ = writer.Write([]byte(`{"error":{"message":"quota exhausted"}}`))
	}))
	t.Cleanup(server.Close)
	client := newClient(t, server, "sk_speko_test")
	_, requestID, err := client.CreateSessionPlan(context.Background(), validRequest(), controlplane.CreateOptions{IdempotencyKey: "create-rejected"})
	if requestID != "cp_rejected" {
		t.Fatalf("request ID = %q", requestID)
	}
	if controlError, ok := err.(*controlplane.HTTPError); !ok || controlError.Status != http.StatusTooManyRequests || controlError.RequestID != requestID {
		t.Fatalf("error = %#v", err)
	}
}

func TestRuntimeInstanceHeartbeatUsesAPIKeyAndInstancePath(t *testing.T) {
	t.Parallel()
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPut || request.URL.Path != "/v1/runtime-instances/gateway-pod-1" {
			t.Fatalf("request = %s %s", request.Method, request.URL.Path)
		}
		if got := request.Header.Get("Authorization"); got != "Bearer sk_speko_test" {
			t.Fatalf("Authorization = %q", got)
		}
		var heartbeat controlplane.InstanceHeartbeat
		if err := json.NewDecoder(request.Body).Decode(&heartbeat); err != nil {
			t.Fatalf("decode heartbeat: %v", err)
		}
		if heartbeat.RuntimeName != "go-gateway" || heartbeat.ActiveSessions != 2 || heartbeat.SessionCapacity != 10 {
			t.Fatalf("heartbeat = %+v", heartbeat)
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	client := newClient(t, server, "sk_speko_test")

	err := client.ReportInstance(context.Background(), "gateway-pod-1", controlplane.InstanceHeartbeat{
		RuntimeName: "go-gateway", RuntimeVersion: "test", StartedAt: time.Now().UTC(),
		ActiveSessions: 2, SessionCapacity: 10,
	})
	if err != nil {
		t.Fatalf("ReportInstance: %v", err)
	}
}

func newClient(t *testing.T, server *httptest.Server, apiKey string) *controlplane.Client {
	t.Helper()
	client, err := controlplane.New(controlplane.Config{BaseURL: server.URL, APIKey: apiKey, HTTPClient: server.Client()})
	if err != nil {
		t.Fatalf("new control-plane client: %v", err)
	}
	return client
}

func validRequest() protocol.SessionPlanRequest {
	return protocol.SessionPlanRequest{
		Kind: protocol.SessionKindSTT, Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision,
		Runtime:   protocol.RuntimeDescriptor{Name: "go", Version: "test", InstanceID: "runtime-test", Placement: protocol.PlacementEmbedded, ProviderRoutes: []protocol.ProviderRoute{protocol.RouteProviderDirect}, Adapters: []string{"mock.stt.v1"}},
		Execution: protocol.ExecutionRequest{ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK, RelayPolicy: protocol.RelayForbidden},
		Request:   protocol.RequestOptions{Model: "model"},
		Media:     &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
	}
}

func validPlan() protocol.SessionPlan {
	now := time.Date(2026, time.August, 1, 12, 0, 0, 0, time.UTC)
	return protocol.SessionPlan{
		PlanID: "plan-test", SessionID: "session-test", AttemptID: "attempt-test", ExpiresAt: now.Add(time.Minute), Signature: "signed-plan",
		Execution:    protocol.Execution{Placement: protocol.PlacementEmbedded, ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK},
		Route:        protocol.PlanRoute{Provider: "mock", Model: "model", Adapter: "mock.stt.v1", Transport: protocol.TransportWebSocket, Endpoint: "wss://provider.speko.test/stream"},
		Reservation:  protocol.Reservation{ID: "reservation-test", LeaseDurationSeconds: 60, LeaseExpiresAt: now.Add(time.Minute), Concurrency: protocol.ConcurrencyReservation{LeaseID: "lease-test", Slots: 1}, Usage: protocol.UsageReservation{Unit: protocol.UsageUnitDurationSeconds, AuthorizedUnits: 60}},
		Telemetry:    protocol.Telemetry{Endpoint: "https://control.speko.test/v1/runtime-events", Token: "telemetry", FlushIntervalMS: 5_000},
		Requirements: protocol.Requirements{Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision, RuntimeVersion: "test"},
	}
}

func TestRefreshCredentialMintsForTheSameAttempt(t *testing.T) {
	t.Parallel()
	plan := validPlan()
	plan.Execution.CredentialSource = protocol.CredentialsManaged
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if request.Method != http.MethodPost || request.URL.Path != "/v1/sessions/session-test/attempts/attempt-test/credentials" {
			t.Errorf("request = %s %s", request.Method, request.URL.Path)
		}
		if got := request.Header.Get("Authorization"); got != "Bearer sk_speko_test" {
			t.Errorf("Authorization = %q", got)
		}
		var body controlplane.CredentialRefreshRequest
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil || body.Reason != "provider_reconnect" {
			t.Errorf("refresh body = %+v, %v", body, err)
		}
		writer.Header().Set("X-Request-ID", "cp_refresh_1")
		_ = json.NewEncoder(writer).Encode(protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "fresh-token", ExpiresAt: time.Now().Add(time.Minute)})
	}))
	t.Cleanup(server.Close)
	client := newClient(t, server, "sk_speko_test")

	credential, requestID, err := client.RefreshCredential(context.Background(), plan, controlplane.CredentialRefreshRequest{Reason: "provider_reconnect"})
	if err != nil || credential.Value != "fresh-token" || requestID != "cp_refresh_1" {
		t.Fatalf("refresh = %+v, %q, %v", credential, requestID, err)
	}
	// Only managed provider-direct attempts have a control-plane credential to
	// refresh: a BYOK key is local, a relay key belongs to the relay.
	for _, execution := range []protocol.Execution{
		{Placement: protocol.PlacementEmbedded, ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK},
		{Placement: protocol.PlacementEmbedded, ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged},
	} {
		other := plan
		other.Execution = execution
		if _, _, err := client.RefreshCredential(context.Background(), other, controlplane.CredentialRefreshRequest{Reason: "provider_reconnect"}); err == nil {
			t.Fatalf("refresh allowed for %+v", execution)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("control-plane calls = %d, want 1", calls.Load())
	}
}

// The control plane records an issued plan a moment after returning it, so a
// Cancel in the first instants of a session can ask for a credential before
// the plan row exists (Pullfrog review of runtime#341, 2026-10-28). That
// answer is retried; a real refusal is not.
func TestRefreshCredentialRetriesAPlanNotYetRecorded(t *testing.T) {
	t.Parallel()
	plan := validPlan()
	plan.Execution.CredentialSource = protocol.CredentialsManaged
	refuse := func(writer http.ResponseWriter, status int, code string) {
		writer.WriteHeader(status)
		_, _ = fmt.Fprintf(writer, `{"error":{"code":%q}}`, code)
	}
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch calls.Add(1) {
		case 1, 2:
			refuse(writer, http.StatusConflict, "plan_not_recorded")
		case 3:
			refuse(writer, http.StatusServiceUnavailable, "service_unavailable")
		default:
			_ = json.NewEncoder(writer).Encode(protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "fresh-token", ExpiresAt: time.Now().Add(time.Minute)})
		}
	}))
	t.Cleanup(server.Close)
	client := newClient(t, server, "sk_speko_test")
	credential, _, err := client.RefreshCredential(context.Background(), plan, controlplane.CredentialRefreshRequest{Reason: "provider_reconnect"})
	if err != nil || credential.Value != "fresh-token" || calls.Load() != 4 {
		t.Fatalf("refresh = %+v, %v after %d calls", credential, err, calls.Load())
	}

	for _, refusal := range []struct {
		status int
		code   string
	}{{http.StatusUnprocessableEntity, "no_eligible_route"}, {http.StatusTooManyRequests, "credential_reissue_limit"}, {http.StatusConflict, "idempotency_conflict"}} {
		var refused atomic.Int32
		final := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			refused.Add(1)
			refuse(writer, refusal.status, refusal.code)
		}))
		_, _, err := newClient(t, final, "sk_speko_test").RefreshCredential(context.Background(), plan, controlplane.CredentialRefreshRequest{Reason: "provider_reconnect"})
		final.Close()
		var failure *controlplane.HTTPError
		if !errors.As(err, &failure) || failure.Code != refusal.code || refused.Load() != 1 {
			t.Fatalf("%s: err = %v after %d calls, want one call", refusal.code, err, refused.Load())
		}
	}
}

func TestRefreshCredentialStopsRetryingWhenTheCallerGivesUp(t *testing.T) {
	t.Parallel()
	plan := validPlan()
	plan.Execution.CredentialSource = protocol.CredentialsManaged
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusConflict)
		_, _ = writer.Write([]byte(`{"error":{"code":"plan_not_recorded"}}`))
	}))
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, _, err := newClient(t, server, "sk_speko_test").RefreshCredential(ctx, plan, controlplane.CredentialRefreshRequest{Reason: "provider_reconnect"})
	var failure *controlplane.HTTPError
	if !errors.As(err, &failure) || failure.Code != "plan_not_recorded" {
		t.Fatalf("err = %v, want the last refusal", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("refresh kept retrying %v past its caller's deadline", elapsed)
	}
}

func TestRefreshCredentialBoundsAStalledRetry(t *testing.T) {
	t.Parallel()
	plan := validPlan()
	plan.Execution.CredentialSource = protocol.CredentialsManaged
	var calls atomic.Int32
	release := make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if calls.Add(1) == 1 {
			writer.WriteHeader(http.StatusConflict)
			_, _ = writer.Write([]byte(`{"error":{"code":"plan_not_recorded"}}`))
			return
		}
		select {
		case <-request.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) }) // runs first: Close waits on handlers
	started := time.Now()
	_, _, err := newClient(t, server, "sk_speko_test").RefreshCredential(context.Background(), plan, controlplane.CredentialRefreshRequest{Reason: "provider_reconnect"})
	if err == nil {
		t.Fatal("a refresh whose retry never answered succeeded")
	}
	if elapsed := time.Since(started); elapsed > 7*time.Second {
		t.Fatalf("a stalled retry held the refresh for %v, past its 5s budget", elapsed)
	}
}
