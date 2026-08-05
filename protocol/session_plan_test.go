package protocol_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
)

func TestSessionPlanFixturesAreValid(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 1, 11, 59, 0, 0, time.UTC)
	for _, fixture := range []string{"session-plan-provider-direct-managed.json", "session-plan-elevenlabs-managed.json", "session-plan-local-byok.json"} {
		var plan protocol.SessionPlan
		decodeFixture(t, fixture, &plan)
		if err := plan.Validate(now); err != nil {
			t.Fatalf("%s must validate: %v", fixture, err)
		}
		if plan.Route.Credential != nil {
			if got := plan.Route.Credential.String(); strings.Contains(got, plan.Route.Credential.Value) {
				t.Fatalf("%s credential String leaked its value: %q", fixture, got)
			}
		}
	}
}

func TestSessionPlanRequestFixturesAreValid(t *testing.T) {
	t.Parallel()

	for _, fixture := range []string{"session-plan-request-stt.json", "session-plan-request-tts.json"} {
		var request protocol.SessionPlanRequest
		decodeFixture(t, fixture, &request)
		if err := request.Validate(); err != nil {
			t.Fatalf("%s must validate: %v", fixture, err)
		}
	}
}

func TestSessionPlanRejectsCredentialPolicyViolations(t *testing.T) {
	t.Parallel()

	var plan protocol.SessionPlan
	decodeFixture(t, "session-plan-provider-direct-managed.json", &plan)
	now := time.Date(2026, time.August, 1, 11, 59, 0, 0, time.UTC)

	plan.Route.Credential = nil
	assertInvalid(t, plan.Validate(now), "managed plans require")

	decodeFixture(t, "session-plan-provider-direct-managed.json", &plan)
	plan.Execution.CredentialSource = protocol.CredentialsBYOK
	assertInvalid(t, plan.Validate(now), "byok plans must not")

	decodeFixture(t, "session-plan-provider-direct-managed.json", &plan)
	plan.Route.Credential.ExpiresAt = plan.ExpiresAt.Add(time.Second)
	assertInvalid(t, plan.Validate(now), "must not outlive")

	decodeFixture(t, "session-plan-provider-direct-managed.json", &plan)
	plan.Route.Credential.ExpiresAt = now
	assertInvalid(t, plan.Validate(now), "credential is expired")

	decodeFixture(t, "session-plan-provider-direct-managed.json", &plan)
	plan.Telemetry = protocol.Telemetry{}
	assertInvalid(t, plan.Validate(now), "managed plans require")
}

func TestSessionPlanRejectsRevisionAndExpirationMismatches(t *testing.T) {
	t.Parallel()

	var plan protocol.SessionPlan
	decodeFixture(t, "session-plan-provider-direct-managed.json", &plan)
	now := time.Date(2026, time.August, 1, 11, 59, 0, 0, time.UTC)

	plan.Requirements.ProtocolRevision++
	assertInvalid(t, plan.Validate(now), "protocol_revision")

	decodeFixture(t, "session-plan-provider-direct-managed.json", &plan)
	assertInvalid(t, plan.Validate(plan.ExpiresAt), "plan is expired")
}

func TestSessionPlanRequestRejectsConflictingRoutePolicy(t *testing.T) {
	t.Parallel()

	request := validRequest(t)
	request.Execution.ProviderRoute = protocol.RouteSpekoRelay
	request.Execution.RelayPolicy = protocol.RelayForbidden
	assertInvalid(t, request.Validate(), "conflicts with forbidden")

	request = validRequest(t)
	request.Runtime.ProviderRoutes = []protocol.ProviderRoute{protocol.RouteAuto}
	assertInvalid(t, request.Validate(), "unsupported value")

	request = validRequest(t)
	request.Execution.CredentialSource = protocol.CredentialsBYOK
	request.Execution.RelayPolicy = protocol.RelayRequired
	assertInvalid(t, request.Validate(), "byok cannot require")

	request = validRequest(t)
	request.Runtime.ProviderRoutes = []protocol.ProviderRoute{protocol.RouteSpekoRelay}
	request.Execution.ProviderRoute = protocol.RouteProviderDirect
	assertInvalid(t, request.Validate(), "not supported by the runtime")
}

func validRequest(t *testing.T) protocol.SessionPlanRequest {
	t.Helper()
	var request protocol.SessionPlanRequest
	decodeFixture(t, "session-plan-request-stt.json", &request)
	return request
}

func decodeFixture(t *testing.T, name string, target any) {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	if err := json.Unmarshal(contents, target); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
}

func assertInvalid(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("expected validation error containing %q, got %v", want, err)
	}
}
