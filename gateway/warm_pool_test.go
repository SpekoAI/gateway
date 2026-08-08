package gateway_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/gateway"
	"github.com/SpekoAI/gateway/protocol"
	"github.com/SpekoAI/gateway/providers/mock"
)

func managedWarmRequest() protocol.SessionPlanRequest {
	return protocol.SessionPlanRequest{
		Kind: protocol.SessionKindSTT, Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision,
		Runtime: protocol.RuntimeDescriptor{
			Name: "go-gateway", Version: "test", InstanceID: "gateway-test", Placement: protocol.PlacementSidecar,
			ProviderRoutes: []protocol.ProviderRoute{protocol.RouteProviderDirect}, Adapters: []string{"mock.stt.v1"},
		},
		Execution: protocol.ExecutionRequest{
			ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsManaged, RelayPolicy: protocol.RelayForbidden,
		},
		Request: protocol.RequestOptions{Model: "mock-model", Language: "en"},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
	}
}

func newWarmPool(t *testing.T, plans gateway.PlanClient, target int) *gateway.PlanPool {
	t.Helper()
	pool, err := gateway.NewPlanPool(gateway.PlanPoolConfig{
		Plans: plans, Target: target,
		Runtime: managedWarmRequest().Runtime,
		Now:     func() time.Time { return gatewayNow },
	})
	if err != nil {
		t.Fatalf("new plan pool: %v", err)
	}
	return pool
}

// TestPlanPoolServesWithoutContactingTheControlPlane is the claim under test:
// with plans warm, creating a session costs no control-plane round trip.
func TestPlanPoolServesWithoutContactingTheControlPlane(t *testing.T) {
	t.Parallel()
	plans := &fakePlanClient{plan: gatewayPlan()}
	pool := newWarmPool(t, plans, 3)

	if err := pool.Warm(context.Background(), managedWarmRequest()); err != nil {
		t.Fatalf("warm: %v", err)
	}
	if plans.batchCallCount() != 1 {
		t.Fatalf("warm made %d batch calls, want 1", plans.batchCallCount())
	}

	for index := 0; index < 3; index++ {
		if _, warm := pool.Take(managedWarmRequest()); !warm {
			t.Fatalf("take %d missed a warm plan", index)
		}
	}
	// Three sessions, zero synchronous plan calls. That is the whole point.
	if plans.createCallCount() != 0 {
		t.Fatalf("warm takes made %d synchronous plan calls, want 0", plans.createCallCount())
	}
	// A drained pool misses rather than failing; the caller falls back.
	if _, warm := pool.Take(managedWarmRequest()); warm {
		t.Fatal("drained pool returned a plan")
	}
	metrics := pool.Metrics()
	if metrics.Hits != 3 || metrics.Misses != 1 || metrics.Depth != 0 {
		t.Fatalf("pool metrics = %+v", metrics)
	}
}

// TestPlanPoolHandsOutEachPlanExactlyOnce guards the invariant that makes
// prefetching safe at all: a plan's jti is single-use at the runtime verifier,
// so two sessions must never receive the same one.
func TestPlanPoolHandsOutEachPlanExactlyOnce(t *testing.T) {
	t.Parallel()
	plans := &fakePlanClient{plan: gatewayPlan()}
	pool := newWarmPool(t, plans, 4)
	if err := pool.Warm(context.Background(), managedWarmRequest()); err != nil {
		t.Fatalf("warm: %v", err)
	}
	seen := map[string]struct{}{}
	for {
		plan, warm := pool.Take(managedWarmRequest())
		if !warm {
			break
		}
		if _, duplicate := seen[plan.PlanID]; duplicate {
			t.Fatalf("plan %q was handed out twice", plan.PlanID)
		}
		seen[plan.PlanID] = struct{}{}
	}
	if len(seen) != 4 {
		t.Fatalf("pool yielded %d distinct plans, want 4", len(seen))
	}
}

// TestPlanPoolSeparatesRoutesThatResolveDifferently pins the pool key. Two
// requests that the control plane would route to different providers must never
// share a plan, and the allow/deny lists are the easiest way to get that wrong
// because they change the route without changing the named provider.
func TestPlanPoolSeparatesRoutesThatResolveDifferently(t *testing.T) {
	t.Parallel()
	plans := &fakePlanClient{plan: gatewayPlan()}
	pool := newWarmPool(t, plans, 1)
	if err := pool.Warm(context.Background(), managedWarmRequest()); err != nil {
		t.Fatalf("warm: %v", err)
	}

	for name, mutate := range map[string]func(*protocol.SessionPlanRequest){
		"different model":    func(r *protocol.SessionPlanRequest) { r.Request.Model = "other-model" },
		"different language": func(r *protocol.SessionPlanRequest) { r.Request.Language = "de" },
		"different voice":    func(r *protocol.SessionPlanRequest) { r.Request.Voice = "some-voice" },
		"added deny list":    func(r *protocol.SessionPlanRequest) { r.Request.Deny = []string{"cartesia"} },
		"added objective":    func(r *protocol.SessionPlanRequest) { r.Request.Objective = "quality" },
		"different kind":     func(r *protocol.SessionPlanRequest) { r.Kind = protocol.SessionKindTTS },
	} {
		request := managedWarmRequest()
		mutate(&request)
		if _, warm := pool.Take(request); warm {
			t.Fatalf("%s reused a plan warmed for a different route", name)
		}
	}

	// Only case-and-order differences may share. The control plane trims and
	// lowercases provider names, so a pool that split on them would waste plans.
	equivalent := managedWarmRequest()
	equivalent.Request.Model = "  Mock-Model  "
	equivalent.Request.Language = "EN"
	if _, warm := pool.Take(equivalent); !warm {
		t.Fatal("a request differing only in case and whitespace missed the pool")
	}
}

// TestPlanPoolIgnoresRoutesItMustNotServe keeps prefetching to the one shape it
// is meant for. BYOK plans are signed locally and already free, and relay
// sessions are not provider-direct.
func TestPlanPoolIgnoresRoutesItMustNotServe(t *testing.T) {
	t.Parallel()
	plans := &fakePlanClient{plan: gatewayPlan()}
	pool := newWarmPool(t, plans, 2)

	byok := managedWarmRequest()
	byok.Execution.CredentialSource = protocol.CredentialsBYOK
	if err := pool.Warm(context.Background(), byok); err == nil {
		t.Fatal("BYOK route was accepted for prefetching")
	}
	if _, warm := pool.Take(byok); warm {
		t.Fatal("BYOK request was served from the pool")
	}

	relayed := managedWarmRequest()
	relayed.Execution.RelayPolicy = protocol.RelayRequired
	relayed.Execution.ProviderRoute = protocol.RouteSpekoRelay
	if err := pool.Warm(context.Background(), relayed); err == nil {
		t.Fatal("relay route was accepted for prefetching")
	}
	if plans.batchCallCount() != 0 {
		t.Fatalf("ineligible routes triggered %d batch calls", plans.batchCallCount())
	}
}

// TestPlanPoolRefusesPlansTooCloseToExpiry checks the safety margin. A plan
// only has to be live at the provider handshake, but handing over one with a
// second left trades a predictable round trip for an unpredictable failure.
func TestPlanPoolRefusesPlansTooCloseToExpiry(t *testing.T) {
	t.Parallel()
	expiring := gatewayPlan()
	expiring.ExpiresAt = gatewayNow.Add(10 * time.Second)
	plans := &fakePlanClient{plan: expiring}
	pool, err := gateway.NewPlanPool(gateway.PlanPoolConfig{
		Plans: plans, Target: 2, MinRemaining: 60 * time.Second,
		Runtime: managedWarmRequest().Runtime,
		Now:     func() time.Time { return gatewayNow },
	})
	if err != nil {
		t.Fatalf("new plan pool: %v", err)
	}
	if err := pool.Warm(context.Background(), managedWarmRequest()); err != nil {
		t.Fatalf("warm: %v", err)
	}
	if _, warm := pool.Take(managedWarmRequest()); warm {
		t.Fatal("a plan inside the expiry margin was handed out")
	}
	if metrics := pool.Metrics(); metrics.Expired != 2 {
		t.Fatalf("pool metrics = %+v, want both plans retired", metrics)
	}
}

// TestPlanPoolSurvivesAnUnreachableControlPlane keeps the pool a pure cache. A
// refill failure must not become a create failure.
func TestPlanPoolSurvivesAnUnreachableControlPlane(t *testing.T) {
	t.Parallel()
	plans := &fakePlanClient{plan: gatewayPlan(), batchErr: errors.New("control plane unavailable")}
	pool := newWarmPool(t, plans, 2)
	if err := pool.Warm(context.Background(), managedWarmRequest()); err != nil {
		t.Fatalf("warm must register the route even when refill fails: %v", err)
	}
	if _, warm := pool.Take(managedWarmRequest()); warm {
		t.Fatal("a failed refill produced a plan")
	}
	if metrics := pool.Metrics(); metrics.Failures == 0 || metrics.Routes != 1 {
		t.Fatalf("pool metrics = %+v", metrics)
	}
}

// TestGatewayCreatesSessionFromWarmPlanWithoutControlPlaneCall is the
// end-to-end version: a real create over HTTP that never reaches the planner.
func TestGatewayCreatesSessionFromWarmPlanWithoutControlPlaneCall(t *testing.T) {
	t.Parallel()
	config, plans := newServerConfigWithAdapter(t, mock.NewSTTAdapter("mock.stt.v1"), 0, 0, 0, 0)
	// The warm plan is managed, so the create body has to be too.
	managedPlan := gatewayPlan()
	managedPlan.Execution.CredentialSource = protocol.CredentialsManaged
	managedPlan.Route.Credential = &protocol.DelegatedCredential{
		Kind: protocol.CredentialBearer, Value: "delegated", ExpiresAt: managedPlan.ExpiresAt,
	}
	plans.plan = managedPlan
	pool := newWarmPool(t, plans, 2)
	if err := pool.Warm(context.Background(), managedWarmRequest()); err != nil {
		t.Fatalf("warm: %v", err)
	}
	config.WarmPlans = pool
	server, err := gateway.New(config)
	if err != nil {
		t.Fatalf("new gateway: %v", err)
	}
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)

	body := map[string]any{
		"kind":      "stt",
		"execution": map[string]string{"provider_route": "provider_direct", "credential_source": "managed", "relay_policy": "forbidden"},
		"request":   map[string]string{"model": "mock-model", "language": "en"},
		"media":     map[string]any{"encoding": "pcm_s16le", "sample_rate_hz": 16000, "channels": 1},
	}
	response := postJSON(t, httpServer.URL+"/v1/sessions", body, "local-token", "warm-create-1")
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		payload, _ := io.ReadAll(response.Body)
		t.Fatalf("create status = %d: %s", response.StatusCode, payload)
	}
	var created struct {
		SessionID string `json:"session_id"`
	}
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if !strings.HasPrefix(created.SessionID, "session-gateway_warm_") {
		t.Fatalf("session %q did not come from the warm pool", created.SessionID)
	}
	if plans.createCallCount() != 0 {
		t.Fatalf("a warm create made %d synchronous plan calls, want 0", plans.createCallCount())
	}

	metrics := postGet(t, httpServer.URL+"/metrics", "local-token")
	defer metrics.Body.Close()
	payload, _ := io.ReadAll(metrics.Body)
	if !strings.Contains(string(payload), "speko_gateway_warm_plan_hits_total 1") {
		t.Fatalf("metrics did not report the warm hit:\n%s", payload)
	}
	cleanup := deleteSession(t, httpServer.URL+"/v1/sessions/"+created.SessionID, "local-token")
	cleanup.Body.Close()
}
