package protocol_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
)

var relayPlanFixtures = []string{"relay-plan-stt.json", "relay-plan-tts.json", "relay-plan-llm.json", "relay-plan-s2s.json"}

func TestRelayPlanFixturesAreValid(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 1, 11, 59, 0, 0, time.UTC)
	for _, fixture := range relayPlanFixtures {
		var plan protocol.RelayPlan
		decodeFixture(t, fixture, &plan)
		if err := plan.Validate(now); err != nil {
			t.Fatalf("%s must validate: %v", fixture, err)
		}
	}
}

// The relay verifier binds a plan to its envelope by re-marshaling both sides
// through the Go struct and byte-comparing. That binding only works if a plan
// that survives one decode/encode cycle keeps producing the same bytes, so
// this test pins marshal stability for every fixture shape (with and without
// a lease or top-up URL).
func TestRelayPlanMarshalIsByteStable(t *testing.T) {
	t.Parallel()

	for _, fixture := range relayPlanFixtures {
		var plan protocol.RelayPlan
		decodeFixture(t, fixture, &plan)
		first, err := json.Marshal(plan)
		if err != nil {
			t.Fatalf("%s: marshal decoded plan: %v", fixture, err)
		}
		var reparsed protocol.RelayPlan
		if err := json.Unmarshal(first, &reparsed); err != nil {
			t.Fatalf("%s: reparse marshaled plan: %v", fixture, err)
		}
		second, err := json.Marshal(reparsed)
		if err != nil {
			t.Fatalf("%s: marshal reparsed plan: %v", fixture, err)
		}
		if !bytes.Equal(first, second) {
			t.Fatalf("%s: marshal is not byte-stable:\n%s\n%s", fixture, first, second)
		}
	}
}

func TestRelayPlanRejectsInvalidMutations(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 1, 11, 59, 0, 0, time.UTC)
	cases := []struct {
		name    string
		fixture string
		mutate  func(*protocol.RelayPlan)
		want    string
	}{
		{"missing request id", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.RequestID = "" }, "reservation_id are required"},
		{"missing principal", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.PrincipalID = "" }, "organization_id and principal_id are required"},
		{"realtime kind", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.Kind = protocol.SessionKindRealtime }, "kind: unsupported value"},
		{"missing region", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.Region = "" }, "region: required"},
		{"missing model", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.Model = "" }, "provider and model are required"},
		{"unsupported transport", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.Transport = "carrier-pigeon" }, "transport: unsupported value"},
		{"endpoint scheme conflicts with transport", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.Endpoint = "https://api.deepgram.com/v1/listen" }, "endpoint: websocket transport requires"},
		{"catalog digest without prefix", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.CatalogDigest = strings.Repeat("ab", 32) }, "catalog_digest"},
		{"catalog digest with non-hex characters", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.CatalogDigest = "sha256:" + strings.Repeat("zz", 32) }, "catalog_digest"},
		{"missing rate card version", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.RateCardVersion = "" }, "rate_card_version: required"},
		{"expired plan", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.ExpiresAt = now }, "plan is expired"},
		{"no budgets", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.Budgets = nil }, "at least one budget group"},
		{"unknown budget group", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.Budgets[0].Group = "gpu_seconds" }, "group: unsupported value"},
		{"duplicate budget groups", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.Budgets = append(p.Budgets, p.Budgets[0]) }, "appears more than once"},
		{"non-positive ceiling", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.Budgets[0].CeilingUnits = 0 }, "ceiling_units: must be positive"},
		{"tts group on an stt plan", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.Budgets[0].Group = protocol.RelayBudgetGroupTTSCharacters }, "not valid for stt plans"},
		{"llm group on a tts plan", "relay-plan-tts.json", func(p *protocol.RelayPlan) { p.Budgets[0].Group = protocol.RelayBudgetGroupLLMInput }, "not valid for tts plans"},
		{"llm plan missing the output group", "relay-plan-llm.json", func(p *protocol.RelayPlan) { p.Budgets = p.Budgets[:1] }, "require both llm_input and llm_output"},
		{"stt group on an s2s plan", "relay-plan-s2s.json", func(p *protocol.RelayPlan) { p.Budgets[0].Group = protocol.RelayBudgetGroupSTTDuration }, "not valid for s2s plans"},
		{"s2s group on an stt plan", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.Budgets[0].Group = protocol.RelayBudgetGroupS2SDuration }, "not valid for stt plans"},
		{"lease with non-positive duration", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.Lease.DurationSeconds = 0 }, "lease: duration_seconds: must be positive"},
		{"lease already expired", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.Lease.ExpiresAt = now }, "lease: lease is expired"},
		{"lease renewal over plain http", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.Lease.RenewalURL = "http://gateway.speko.dev/renew" }, "lease: renewal_url"},
		{"top-up over plain http", "relay-plan-tts.json", func(p *protocol.RelayPlan) { p.Lease.TopUpURL = "http://gateway.speko.dev/top-up" }, "lease: top_up_url"},
		{"missing relay access", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.RelayAccess = protocol.RelayAccessCredential{} }, "relay_access: value and expires_at are required"},
		{"expired relay access", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.RelayAccess.ExpiresAt = now }, "relay_access: credential is expired"},
		{"relay access outlives the plan", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.RelayAccess.ExpiresAt = p.ExpiresAt.Add(time.Second) }, "relay_access: expiry must not outlive the plan"},
		{"missing control token", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.ControlToken = protocol.RelayControlToken{} }, "control_token: value and expires_at are required"},
		{"control token outlives the plan", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.ControlToken.ExpiresAt = p.ExpiresAt.Add(time.Second) }, "control_token: expiry must not outlive the plan"},
		{"wrong protocol", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.Requirements.Protocol = "speko.voice.v1" }, "requirements: protocol: got"},
		{"prior relay revision", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.Requirements.ProtocolRevision = 4 }, "protocol_revision: got 4, want 5"},
		{"session-plan revision", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.Requirements.ProtocolRevision = protocol.CurrentRevision }, "protocol_revision: got 3, want 5"},
		{"missing credential source", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.CredentialSource = "" }, "credential_source: must be \"managed\" or \"byok\""},
		{"unsupported credential source", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.CredentialSource = "delegated" }, "credential_source: must be \"managed\" or \"byok\""},
		{"missing signature", "relay-plan-stt.json", func(p *protocol.RelayPlan) { p.Signature = "" }, "signature: required"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			var plan protocol.RelayPlan
			decodeFixture(t, testCase.fixture, &plan)
			testCase.mutate(&plan)
			assertInvalid(t, plan.Validate(now), testCase.want)
		})
	}
}

func TestRelayPlanAcceptsBYOKCredentialSource(t *testing.T) {
	t.Parallel()

	var plan protocol.RelayPlan
	decodeFixture(t, "relay-plan-stt.json", &plan)
	plan.CredentialSource = protocol.CredentialsBYOK
	if err := plan.Validate(time.Date(2026, time.August, 1, 11, 59, 0, 0, time.UTC)); err != nil {
		t.Fatalf("BYOK relay plan must validate: %v", err)
	}
}

func TestRelayPlanEnvelopeValidatesClaims(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 1, 11, 59, 0, 0, time.UTC)
	base := func(t *testing.T) protocol.RelayPlanEnvelope {
		t.Helper()
		var plan protocol.RelayPlan
		decodeFixture(t, "relay-plan-stt.json", &plan)
		return protocol.RelayPlanEnvelope{
			Issuer:    "https://gateway.speko.dev",
			Audience:  []string{protocol.RelayPlanAudience},
			IssuedAt:  now.Add(-time.Second),
			ExpiresAt: plan.ExpiresAt,
			ID:        "relay-use_123",
			Plan:      plan.Unsigned(),
		}
	}
	if err := base(t).Validate(now, "https://gateway.speko.dev", protocol.RelayPlanAudience, time.Second); err != nil {
		t.Fatalf("valid envelope must validate: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*protocol.RelayPlanEnvelope)
		want   string
	}{
		{"missing jti", func(e *protocol.RelayPlanEnvelope) { e.ID = "" }, "iss and jti are required"},
		{"wrong issuer", func(e *protocol.RelayPlanEnvelope) { e.Issuer = "https://impostor.example" }, "iss: got"},
		{"missing relay audience", func(e *protocol.RelayPlanEnvelope) { e.Audience = []string{"speko-runtime"} }, "aud: missing"},
		{"missing iat", func(e *protocol.RelayPlanEnvelope) { e.IssuedAt = time.Time{} }, "iat and exp are required"},
		{"issued in the future", func(e *protocol.RelayPlanEnvelope) { e.IssuedAt = now.Add(time.Minute) }, "issued in the future"},
		{"expired envelope", func(e *protocol.RelayPlanEnvelope) {
			e.ExpiresAt = now.Add(-time.Minute)
			e.Plan.ExpiresAt = e.ExpiresAt
		}, "envelope is expired"},
		{"exp diverges from plan expiry", func(e *protocol.RelayPlanEnvelope) { e.ExpiresAt = e.ExpiresAt.Add(time.Second) }, "must match plan expires_at"},
		{"inner signature present", func(e *protocol.RelayPlanEnvelope) { e.Plan.Signature = "leftover" }, "must be omitted from the signed envelope"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			envelope := base(t)
			testCase.mutate(&envelope)
			assertInvalid(t, envelope.Validate(now, "https://gateway.speko.dev", protocol.RelayPlanAudience, time.Second), testCase.want)
		})
	}
}

// Both relay credentials are logging secrets: the relay access bearer admits
// a caller to a provider connector and the control token authorizes billing
// mutations. Their String methods must therefore never surface Value.
func TestRelayCredentialStringsRedactValues(t *testing.T) {
	t.Parallel()

	expiry := time.Date(2026, time.August, 1, 12, 0, 30, 0, time.UTC)
	access := protocol.RelayAccessCredential{Value: "sra_super-secret-bearer", ExpiresAt: expiry}
	if got := access.String(); strings.Contains(got, access.Value) || !strings.Contains(got, "redacted") {
		t.Fatalf("relay access String leaked its value: %q", got)
	}
	token := protocol.RelayControlToken{Value: "rct_super-secret-token", ExpiresAt: expiry}
	if got := token.String(); strings.Contains(got, token.Value) || !strings.Contains(got, "redacted") {
		t.Fatalf("control token String leaked its value: %q", got)
	}
}
