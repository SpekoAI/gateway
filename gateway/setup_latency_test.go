package gateway_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/controlplane"
	"github.com/SpekoAI/gateway/gateway"
	"github.com/SpekoAI/gateway/protocol"
	"github.com/SpekoAI/gateway/providers/mock"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

// The injected delays are large relative to scheduling noise on a shared CI
// runner, so the arms separate unambiguously rather than by a few hundred
// microseconds that a loaded machine could erase.
const (
	benchControlPlaneRTT = 200 * time.Millisecond
	benchProviderDial    = 40 * time.Millisecond
)

// slowAdapter wraps an adapter with a fixed dial cost, standing in for the
// provider handshake that no amount of gateway work can remove.
type slowAdapter struct {
	inner runtimepkg.Adapter
	dial  time.Duration
}

func (a slowAdapter) ID() string { return a.inner.ID() }

func (a slowAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	select {
	case <-time.After(a.dial):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return a.inner.Open(ctx, request)
}

// slowPlanClient adds a fixed cost to every control-plane call, standing in for
// the cross-region round trip a managed create used to pay.
type slowPlanClient struct {
	fakePlanClient
	rtt time.Duration
}

func (c *slowPlanClient) CreateSessionPlan(ctx context.Context, request protocol.SessionPlanRequest, options controlplane.CreateOptions) (protocol.SessionPlan, string, error) {
	select {
	case <-time.After(c.rtt):
	case <-ctx.Done():
		return protocol.SessionPlan{}, "", ctx.Err()
	}
	return c.fakePlanClient.CreateSessionPlan(ctx, request, options)
}

func (c *slowPlanClient) CreateSessionPlanBatch(ctx context.Context, request protocol.SessionPlanRequest, count int, options controlplane.CreateOptions) ([]protocol.SessionPlan, string, error) {
	select {
	case <-time.After(c.rtt):
	case <-ctx.Done():
		return nil, "", ctx.Err()
	}
	return c.fakePlanClient.CreateSessionPlanBatch(ctx, request, count, options)
}

// TestSetupLatencyWarmPathDoesNotPayTheControlPlane is the acceptance test for
// the zero-overhead claim, in the only form that can run without provider
// credentials or a deployed control plane.
//
// It does NOT measure real provider or network latency — the provider dial and
// the control-plane round trip are both simulated. What it measures is the one
// thing that can regress in this repository: whether creating a session waits
// on the control plane at all. A warm create must cost about what dialing the
// provider costs, and must not move when the control plane gets slower.
//
// The staging benchmark against real providers is still the number to quote to
// a customer. This is the gate that stops the property being lost between now
// and then.
func TestSetupLatencyWarmPathDoesNotPayTheControlPlane(t *testing.T) {
	t.Parallel()

	rawProvider := measure(t, func() {
		adapter := slowAdapter{inner: mock.NewSTTAdapter("mock.stt.v1"), dial: benchProviderDial}
		stream, err := adapter.Open(context.Background(), runtimepkg.AdapterRequest{
			Kind: protocol.SessionKindSTT, Plan: gatewayPlan(),
			Media: &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
		})
		if err != nil {
			t.Fatalf("raw provider dial: %v", err)
		}
		_ = stream.Close(context.Background())
	})

	coldServer, coldClient := newLatencyServer(t, false)
	cold := measure(t, func() { createManagedSession(t, coldServer, "cold") })

	warmServer, warmClient := newLatencyServer(t, true)
	warm := measure(t, func() { createManagedSession(t, warmServer, "warm") })

	t.Logf("raw_provider    %6.1fms", rawProvider.Seconds()*1000)
	t.Logf("sidecar_cold    %6.1fms  (control-plane RTT %s)", cold.Seconds()*1000, benchControlPlaneRTT)
	t.Logf("sidecar_warm    %6.1fms", warm.Seconds()*1000)

	// Sanity: the cold arm must actually pay the round trip, or the test is
	// measuring nothing and the comparison below is meaningless.
	if cold < benchControlPlaneRTT {
		t.Fatalf("sidecar_cold = %s, want at least the injected control-plane RTT %s", cold, benchControlPlaneRTT)
	}
	if coldClient.createCallCount() == 0 {
		t.Fatal("sidecar_cold made no synchronous plan call; the arms are not distinguishable")
	}

	// The claim: a warm create does not pay the round trip. Half the injected
	// RTT is a deliberately loose bound — the real result is far below it, and
	// anything near it means the property is gone, not that the runner was busy.
	if warm >= benchControlPlaneRTT/2 {
		t.Fatalf("sidecar_warm = %s, want well under half the control-plane RTT %s", warm, benchControlPlaneRTT)
	}
	// And it does so by not calling at all, not by being coincidentally fast.
	if got := warmClient.createCallCount(); got != 0 {
		t.Fatalf("sidecar_warm made %d synchronous plan calls, want 0", got)
	}
	// Whatever remains is the provider dial. Generous headroom so this reports
	// a regression rather than CI weather.
	if ceiling := rawProvider + benchControlPlaneRTT/4; warm > ceiling {
		t.Fatalf("sidecar_warm = %s exceeds raw_provider %s by more than the allowed margin", warm, rawProvider)
	}
}

// measure runs one setup and returns its wall time. A single sample is enough
// because the injected delays dominate: the thresholds are separated by tens of
// milliseconds, not by the microseconds that would need averaging.
func measure(t *testing.T, setup func()) time.Duration {
	t.Helper()
	started := time.Now()
	setup()
	return time.Since(started)
}

func newLatencyServer(t *testing.T, warm bool) (*httptest.Server, *slowPlanClient) {
	t.Helper()
	config, _ := newServerConfigWithAdapter(t,
		slowAdapter{inner: mock.NewSTTAdapter("mock.stt.v1"), dial: benchProviderDial}, 0, 0, 0, 0)

	managedPlan := gatewayPlan()
	managedPlan.Execution.CredentialSource = protocol.CredentialsManaged
	managedPlan.Route.Credential = &protocol.DelegatedCredential{
		Kind: protocol.CredentialBearer, Value: "delegated", ExpiresAt: managedPlan.ExpiresAt,
	}
	slow := &slowPlanClient{fakePlanClient: fakePlanClient{plan: managedPlan}, rtt: benchControlPlaneRTT}
	config.Plans = slow

	if warm {
		pool := newWarmPool(t, slow, 4)
		// Warming happens off the measured path, which is the entire point: the
		// round trip is paid in the background, before anyone is waiting.
		if err := pool.Warm(context.Background(), managedWarmRequest()); err != nil {
			t.Fatalf("warm: %v", err)
		}
		config.WarmPlans = pool
	}

	server, err := gateway.New(config)
	if err != nil {
		t.Fatalf("new gateway: %v", err)
	}
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)
	return httpServer, slow
}

func createManagedSession(t *testing.T, server *httptest.Server, key string) {
	t.Helper()
	body := map[string]any{
		"kind":      "stt",
		"execution": map[string]string{"provider_route": "provider_direct", "credential_source": "managed", "relay_policy": "forbidden"},
		"request":   map[string]string{"model": "mock-model", "language": "en"},
		"media":     map[string]any{"encoding": "pcm_s16le", "sample_rate_hz": 16000, "channels": 1},
	}
	response := postJSON(t, server.URL+"/v1/sessions", body, "local-token", fmt.Sprintf("latency-%s", key))
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
	cleanup := deleteSession(t, server.URL+"/v1/sessions/"+created.SessionID, "local-token")
	cleanup.Body.Close()
}
