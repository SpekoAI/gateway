package relayapi_test

import (
	"testing"

	"github.com/SpekoAI/gateway/relayapi"
)

func TestRoutingNormalizeDefault(t *testing.T) {
	t.Parallel()

	// A wholly omitted routing object means {mode: auto, objective: balanced}.
	normalized := relayapi.Routing{}.NormalizeDefault()
	if normalized.Mode != relayapi.RoutingModeAuto || normalized.Objective != relayapi.ObjectiveBalanced {
		t.Fatalf("zero routing normalized to %+v, want auto/balanced", normalized)
	}
	if err := normalized.Validate(); err != nil {
		t.Fatalf("normalized default must validate: %v", err)
	}

	// An auto routing without an objective defaults to balanced.
	normalized = relayapi.Routing{Mode: relayapi.RoutingModeAuto}.NormalizeDefault()
	if normalized.Objective != relayapi.ObjectiveBalanced {
		t.Fatalf("auto routing objective = %q, want balanced", normalized.Objective)
	}

	// A stated objective is never overridden.
	normalized = relayapi.Routing{Mode: relayapi.RoutingModeAuto, Objective: relayapi.ObjectiveCost}.NormalizeDefault()
	if normalized.Objective != relayapi.ObjectiveCost {
		t.Fatalf("stated objective was overridden to %q", normalized.Objective)
	}

	// Explicit routing passes through untouched.
	explicit := relayapi.Routing{Mode: relayapi.RoutingModeExplicit, Provider: "openai", Model: "gpt-5.2"}
	normalized = explicit.NormalizeDefault()
	if normalized.Mode != explicit.Mode || normalized.Provider != explicit.Provider || normalized.Model != explicit.Model || normalized.Objective != "" {
		t.Fatalf("explicit routing was rewritten to %+v", normalized)
	}

	// The default exists for omission only: a routing that names a field
	// without a mode stays broken and fails validation.
	partial := relayapi.Routing{Objective: relayapi.ObjectiveLatency}.NormalizeDefault()
	if partial.Mode != "" {
		t.Fatalf("partial routing was given mode %q", partial.Mode)
	}
	assertInvalid(t, partial.Validate(), "mode: unsupported value")
}

// JSON [] is present, not omitted: a routing carrying only an empty filter
// names the auto arm without a mode, so it is partially specified routing —
// the omission default must leave it broken for Validate to reject rather
// than quietly adopting it.
func TestRoutingNormalizeDefaultTreatsEmptyFilterAsPresent(t *testing.T) {
	t.Parallel()

	partial := relayapi.Routing{AllowProviders: []string{}}.NormalizeDefault()
	if partial.Mode != "" {
		t.Fatalf("routing with an empty filter was given mode %q", partial.Mode)
	}
	assertInvalid(t, partial.Validate(), "mode: unsupported value")
}

func TestRoutingValidateRejectsMixedAndUnknownFields(t *testing.T) {
	t.Parallel()

	auto := func() relayapi.Routing {
		return relayapi.Routing{Mode: relayapi.RoutingModeAuto, Objective: relayapi.ObjectiveBalanced}
	}
	explicit := func() relayapi.Routing {
		return relayapi.Routing{Mode: relayapi.RoutingModeExplicit, Provider: "anthropic", Model: "claude-sonnet-4-5"}
	}

	cases := []struct {
		name    string
		routing relayapi.Routing
		want    string
	}{
		{"empty mode", relayapi.Routing{}, "mode: unsupported value"},
		{"unknown mode", relayapi.Routing{Mode: "hybrid"}, "mode: unsupported value"},
		{"auto without objective", relayapi.Routing{Mode: relayapi.RoutingModeAuto}, "objective: unsupported value"},
		{"auto with unknown objective", relayapi.Routing{Mode: relayapi.RoutingModeAuto, Objective: "cheapest"}, "objective: unsupported value"},
		{"auto with provider", func() relayapi.Routing { r := auto(); r.Provider = "openai"; return r }(), "valid only in explicit mode"},
		{"auto with model", func() relayapi.Routing { r := auto(); r.Model = "gpt-5.2"; return r }(), "valid only in explicit mode"},
		{"auto with blank allow entry", func() relayapi.Routing { r := auto(); r.AllowProviders = []string{" "}; return r }(), "allow_providers[0]"},
		{"auto with blank deny entry", func() relayapi.Routing { r := auto(); r.DenyProviders = []string{""}; return r }(), "deny_providers[0]"},
		{"explicit without provider", func() relayapi.Routing { r := explicit(); r.Provider = ""; return r }(), "explicit mode requires provider and model"},
		{"explicit without model", func() relayapi.Routing { r := explicit(); r.Model = ""; return r }(), "explicit mode requires provider and model"},
		{"explicit with objective", func() relayapi.Routing { r := explicit(); r.Objective = relayapi.ObjectiveQuality; return r }(), "valid only in auto mode"},
		{"explicit with allow filter", func() relayapi.Routing { r := explicit(); r.AllowProviders = []string{"openai"}; return r }(), "valid only in auto mode"},
		{"explicit with deny filter", func() relayapi.Routing { r := explicit(); r.DenyProviders = []string{"openai"}; return r }(), "valid only in auto mode"},
		// json.Unmarshal keeps [] distinct from an omitted array, so an empty
		// filter is a present cross-arm field — rejecting it here is what
		// stops the marshal side's omitempty from silently rewriting the
		// request instead of failing it.
		{"explicit with empty allow filter", func() relayapi.Routing { r := explicit(); r.AllowProviders = []string{}; return r }(), "valid only in auto mode"},
		{"explicit with empty deny filter", func() relayapi.Routing { r := explicit(); r.DenyProviders = []string{}; return r }(), "valid only in auto mode"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertInvalid(t, tc.routing.Validate(), tc.want)
		})
	}
}

func TestRouteRequiresAllFields(t *testing.T) {
	t.Parallel()

	route := relayapi.Route{Provider: "openai", Model: "gpt-5.2", Region: "us-west-2", AttemptID: "att_1"}
	if err := route.Validate(); err != nil {
		t.Fatalf("complete route must validate: %v", err)
	}
	for _, mutate := range []func(*relayapi.Route){
		func(r *relayapi.Route) { r.Provider = "" },
		func(r *relayapi.Route) { r.Model = "" },
		func(r *relayapi.Route) { r.Region = " " },
		func(r *relayapi.Route) { r.AttemptID = "" },
	} {
		mutated := route
		mutate(&mutated)
		assertInvalid(t, mutated.Validate(), "required")
	}
}
