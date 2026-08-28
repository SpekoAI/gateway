package gateway_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/controlplane"
	"github.com/SpekoAI/gateway/gateway"
	"github.com/SpekoAI/gateway/protocol"
)

func TestLocalPlannerIssuesVerifiableCredentialFreeBYOKPlan(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	planner, err := gateway.NewLocalPlanner(gateway.LocalPlannerConfig{
		Providers: []string{"deepgram"}, Now: func() time.Time { return now }, MaxSessionDuration: time.Hour,
	})
	if err != nil {
		t.Fatalf("new local planner: %v", err)
	}
	plan, requestID, err := planner.CreateSessionPlan(context.Background(), localPlanRequest(), controlplane.CreateOptions{IdempotencyKey: "local-1"})
	if err != nil {
		t.Fatalf("create local plan: %v", err)
	}
	if requestID != "" || plan.Execution.CredentialSource != protocol.CredentialsBYOK || plan.Execution.ProviderRoute != protocol.RouteProviderDirect || plan.Route.Provider != "deepgram" || plan.Route.Credential != nil {
		t.Fatalf("local plan = %+v, request_id=%q", plan, requestID)
	}
	if plan.Telemetry != (protocol.Telemetry{}) {
		t.Fatalf("local plan unexpectedly carries a remote destination: telemetry=%+v", plan.Telemetry)
	}
	if err := plan.Validate(now); err != nil {
		t.Fatalf("local plan validation: %v", err)
	}
	if err := planner.Verify(context.Background(), plan); err != nil {
		t.Fatalf("verify local plan: %v", err)
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal local plan: %v", err)
	}
	if !strings.Contains(string(encoded), `"telemetry":{}`) {
		t.Fatalf("local telemetry JSON = %s", encoded)
	}

	plan.Route.Model = "tampered"
	if err := planner.Verify(context.Background(), plan); !errors.Is(err, gateway.ErrLocalPlanSignature) {
		t.Fatalf("tampered plan error = %v", err)
	}
}

func TestLocalPlannerHonorsRequestedSessionCeiling(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 28, 12, 0, 0, 0, time.UTC)
	planner, err := gateway.NewLocalPlanner(gateway.LocalPlannerConfig{
		Providers: []string{"deepgram"}, Now: func() time.Time { return now }, MaxSessionDuration: time.Hour,
	})
	if err != nil {
		t.Fatalf("new local planner: %v", err)
	}
	request := localPlanRequest()
	request.Request.MaxSessionSeconds = 90
	plan, _, err := planner.CreateSessionPlan(context.Background(), request, controlplane.CreateOptions{})
	if err != nil {
		t.Fatalf("create local plan: %v", err)
	}
	if plan.Reservation.LeaseDurationSeconds != 90 || plan.Reservation.Usage.AuthorizedUnits != 90 {
		t.Fatalf("reservation = %+v, want 90-second caller ceiling", plan.Reservation)
	}
	if want := now.Add(90 * time.Second); !plan.Reservation.LeaseExpiresAt.Equal(want) {
		t.Fatalf("lease expiry = %s, want %s", plan.Reservation.LeaseExpiresAt, want)
	}
}

func TestLocalPlannerRequiresBYOKAndUnambiguousProvider(t *testing.T) {
	t.Parallel()
	planner, err := gateway.NewLocalPlanner(gateway.LocalPlannerConfig{Providers: []string{"deepgram", "elevenlabs"}})
	if err != nil {
		t.Fatalf("new local planner: %v", err)
	}
	request := localPlanRequest()
	request.Execution.CredentialSource = protocol.CredentialsManaged
	if _, _, err := planner.CreateSessionPlan(context.Background(), request, controlplane.CreateOptions{}); !errors.Is(err, gateway.ErrLocalModeRequiresBYOK) {
		t.Fatalf("managed local request error = %v", err)
	}
	request = localPlanRequest()
	request.Request.Provider = "auto"
	request.Kind = protocol.SessionKindTTS
	request.Request.MaxInputCharacters = 100
	request.Media = &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1}
	if _, _, err := planner.CreateSessionPlan(context.Background(), request, controlplane.CreateOptions{}); err == nil {
		t.Fatal("ambiguous local provider was selected implicitly")
	}
}

func TestLocalPlannerRoutesCartesiaSTT(t *testing.T) {
	t.Parallel()
	planner, err := gateway.NewLocalPlanner(gateway.LocalPlannerConfig{Providers: []string{"cartesia"}})
	if err != nil {
		t.Fatalf("new local planner: %v", err)
	}
	request := localPlanRequest()
	request.Runtime.Adapters = []string{"cartesia.stt.v1"}
	request.Request.Provider = "cartesia"
	request.Request.Model = "auto"
	plan, _, err := planner.CreateSessionPlan(context.Background(), request, controlplane.CreateOptions{})
	if err != nil {
		t.Fatalf("create Cartesia STT plan: %v", err)
	}
	if plan.Route.Provider != "cartesia" || plan.Route.Adapter != "cartesia.stt.v1" || plan.Route.Model != "ink-2" || plan.Route.Endpoint != "wss://api.cartesia.ai/stt/websocket" {
		t.Fatalf("Cartesia STT route = %+v", plan.Route)
	}
}

func TestLocalPlannerRoutesEveryCatalogEntryWithBYOK(t *testing.T) {
	t.Parallel()
	for _, entry := range gateway.Catalog() {
		entry := entry
		t.Run(string(entry.Kind)+"/"+entry.Provider, func(t *testing.T) {
			t.Parallel()
			overrides := map[string]gateway.LocalRouteOverride{}
			if entry.RequiresDeploymentConfig != "" {
				overrides[entry.Adapter] = gateway.LocalRouteOverride{
					Endpoint: "https://speech.googleapis.com/v2/projects/test-project/locations/eu/recognizers/_:recognize",
				}
			}
			planner, err := gateway.NewLocalPlanner(gateway.LocalPlannerConfig{
				Providers: []string{entry.Provider}, RouteOverrides: overrides,
			})
			if err != nil {
				t.Fatalf("new local planner: %v", err)
			}
			request := localPlanRequest()
			request.Kind = entry.Kind
			request.Runtime.Adapters = []string{entry.Adapter}
			request.Request.Provider = entry.Provider
			request.Request.Model = "auto"
			if entry.Kind == protocol.SessionKindTTS {
				request.Request.MaxInputCharacters = 1_000
				request.Request.Voice = "test-voice"
			}
			if entry.Kind == protocol.SessionKindRealtime {
				request.Request.Voice = "test-voice"
				request.Request.S2S = &protocol.S2SOptions{OutputMedia: &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1}}
			}
			plan, _, err := planner.CreateSessionPlan(context.Background(), request, controlplane.CreateOptions{})
			if err != nil {
				t.Fatalf("create local plan: %v", err)
			}
			if plan.Route.Provider != entry.Provider || plan.Route.Adapter != entry.Adapter || plan.Route.Model != entry.DefaultModel || plan.Route.Transport != entry.Transport {
				t.Fatalf("route = %+v, catalog = %+v", plan.Route, entry)
			}
			if entry.RequiresDeploymentConfig == "" && plan.Route.Endpoint != entry.Endpoint {
				t.Fatalf("route endpoint = %q, want %q", plan.Route.Endpoint, entry.Endpoint)
			}
			if strings.Contains(plan.Route.Endpoint, "PROJECT_ID") {
				t.Fatalf("route kept placeholder endpoint %q", plan.Route.Endpoint)
			}
		})
	}
}

func TestLocalPlannerAppliesOperatorVoiceOverride(t *testing.T) {
	t.Parallel()
	planner, err := gateway.NewLocalPlanner(gateway.LocalPlannerConfig{
		Providers: []string{"cartesia"},
		RouteOverrides: map[string]gateway.LocalRouteOverride{
			"cartesia.tts.v1": {DefaultVoice: "operator-voice"},
		},
	})
	if err != nil {
		t.Fatalf("new local planner: %v", err)
	}
	request := localPlanRequest()
	request.Kind = protocol.SessionKindTTS
	request.Runtime.Adapters = []string{"cartesia.tts.v1"}
	request.Request = protocol.RequestOptions{Provider: "cartesia", Model: "auto", MaxInputCharacters: 1_000}
	plan, _, err := planner.CreateSessionPlan(context.Background(), request, controlplane.CreateOptions{})
	if err != nil {
		t.Fatalf("create local plan: %v", err)
	}
	if plan.Route.Voice != "operator-voice" {
		t.Fatalf("route voice = %q", plan.Route.Voice)
	}
}

func localPlanRequest() protocol.SessionPlanRequest {
	return protocol.SessionPlanRequest{
		Kind: protocol.SessionKindSTT, Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision,
		Runtime:   protocol.RuntimeDescriptor{Name: "go-gateway", Version: "test", InstanceID: "local-test", Placement: protocol.PlacementSidecar, ProviderRoutes: []protocol.ProviderRoute{protocol.RouteProviderDirect}, Adapters: []string{"deepgram.stt.v1"}},
		Execution: protocol.ExecutionRequest{ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK, RelayPolicy: protocol.RelayForbidden},
		Request:   protocol.RequestOptions{Provider: "deepgram", Model: "nova-3"},
		Media:     &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
	}
}
