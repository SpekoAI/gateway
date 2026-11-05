package relayapi_test

import (
	"testing"
	"time"

	"github.com/SpekoAI/gateway/relayapi"
)

func TestLLMAdmissionRejectsEachRuleViolation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		mutate func(a *relayapi.LLMAdmission)
		want   string
	}{
		{"wrong object", func(a *relayapi.LLMAdmission) { a.Object = "llm.response" }, "object: must be"},
		{"missing ticket", func(a *relayapi.LLMAdmission) { a.Admission = " " }, "admission: required"},
		{"missing expiry", func(a *relayapi.LLMAdmission) { a.ExpiresAt = time.Time{} }, "expires_at: required"},
	}
	for _, tc := range cases {
		var admission relayapi.LLMAdmission
		decodeFixture(t, "llm-admission.json", &admission)
		tc.mutate(&admission)
		assertInvalid(t, admission.Validate(), tc.want)
	}
}

func TestLLMAdmissionReleaseRejectsEachRuleViolation(t *testing.T) {
	t.Parallel()

	assertInvalid(t, relayapi.LLMAdmissionReleaseRequest{Admission: "\t"}.Validate(), "admission: required")
	assertInvalid(t, relayapi.LLMAdmissionRelease{Object: relayapi.LLMAdmissionObject, Released: true}.Validate(), "object: must be")
	if err := (relayapi.LLMAdmissionRelease{Object: relayapi.LLMAdmissionReleaseObject}).Validate(); err != nil {
		t.Fatalf("released=false is a valid answer: %v", err)
	}
}

func TestPreparedAdmissionOutcomesAreClosedAndDistinct(t *testing.T) {
	t.Parallel()

	outcomes := relayapi.PreparedAdmissionOutcomes()
	if len(outcomes) != 7 || outcomes[0] != relayapi.PreparedAdmissionUsed {
		t.Fatalf("PreparedAdmissionOutcomes() = %v, want seven outcomes led by used", outcomes)
	}
	seen := make(map[relayapi.PreparedAdmissionOutcome]bool, len(outcomes))
	for _, outcome := range outcomes {
		if outcome == "" || seen[outcome] {
			t.Fatalf("PreparedAdmissionOutcomes() repeats or blanks %q", outcome)
		}
		seen[outcome] = true
	}
}
