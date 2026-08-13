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

func localPlanRequest() protocol.SessionPlanRequest {
	return protocol.SessionPlanRequest{
		Kind: protocol.SessionKindSTT, Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision,
		Runtime:   protocol.RuntimeDescriptor{Name: "go-gateway", Version: "test", InstanceID: "local-test", Placement: protocol.PlacementSidecar, ProviderRoutes: []protocol.ProviderRoute{protocol.RouteProviderDirect}, Adapters: []string{"deepgram.stt.v1"}},
		Execution: protocol.ExecutionRequest{ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK, RelayPolicy: protocol.RelayForbidden},
		Request:   protocol.RequestOptions{Provider: "deepgram", Model: "nova-3"},
		Media:     &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
	}
}
