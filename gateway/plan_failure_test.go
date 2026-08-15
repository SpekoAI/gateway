package gateway

import (
	"net/http"
	"strings"
	"testing"

	"github.com/SpekoAI/gateway/controlplane"
)

// A refused pin must be told apart from an outage. The control plane answers
// a provider/model it cannot serve with 422 no_eligible_route; collapsing
// that into "control plane did not issue a session plan" made a working pin
// read as a broken gateway.
func TestPlanFailureSurfacesTheControlPlaneRefusalCode(t *testing.T) {
	t.Parallel()
	refused := planFailure(&controlplane.HTTPError{
		Status: http.StatusUnprocessableEntity, Code: "no_eligible_route", RequestID: "req_1",
	}, "req_1")
	if refused.status != http.StatusUnprocessableEntity || refused.code != "no_eligible_route" {
		t.Fatalf("refusal = %d %q", refused.status, refused.code)
	}
	if !strings.Contains(refused.message, "no_eligible_route") || !strings.Contains(refused.message, "BYOK") {
		t.Fatalf("the refusal must tell the caller what to do: %q", refused.message)
	}

	// Other coded refusals pass their code through without the route hint.
	credit := planFailure(&controlplane.HTTPError{Status: http.StatusPaymentRequired, Code: "credit_exhausted"}, "")
	if credit.code != "credit_exhausted" || strings.Contains(credit.message, "BYOK") {
		t.Fatalf("credit refusal = %q %q", credit.code, credit.message)
	}

	// A body with no code keeps today's generic shape.
	uncoded := planFailure(&controlplane.HTTPError{Status: http.StatusBadGateway}, "")
	if uncoded.code != "control_plane_rejected" || uncoded.message != "control plane did not issue a session plan" {
		t.Fatalf("uncoded = %q %q", uncoded.code, uncoded.message)
	}
}
