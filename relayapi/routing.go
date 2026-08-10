package relayapi

import (
	"fmt"
	"strings"
)

// RoutingMode discriminates the routing tagged union. The set is closed: an
// empty or unknown mode is rejected so a typo can never silently fall back to
// automatic provider selection.
type RoutingMode string

const (
	RoutingModeAuto     RoutingMode = "auto"
	RoutingModeExplicit RoutingMode = "explicit"
)

// RoutingObjective ranks auto-mode candidates. Unknown objectives are
// rejected rather than treated as balanced so callers learn about typos
// before any money is reserved.
type RoutingObjective string

const (
	ObjectiveBalanced RoutingObjective = "balanced"
	ObjectiveQuality  RoutingObjective = "quality"
	ObjectiveLatency  RoutingObjective = "latency"
	ObjectiveCost     RoutingObjective = "cost"
)

// Routing selects how the relay picks a provider for one request. It is a
// tagged union over Mode: auto carries the objective and optional
// allow/deny provider filters, explicit carries exactly a provider and
// model. Fields from the other arm are rejected in both directions — with
// one documented asymmetry in what "present" means. JSON distinguishes a
// present-but-empty array from an omitted one, so an explicit routing
// carrying allow_providers or deny_providers as [] is rejected like any
// other cross-arm field. The string fields cannot make that distinction
// without pointer types, so "provider": "" in auto mode is indistinguishable
// from omission and tolerated as such.
//
// A wholly omitted routing object means {mode: auto, objective: balanced}.
// Handlers apply that default with NormalizeDefault before Validate;
// Validate deliberately rejects the unnormalized zero forms so no code path
// can skip the defaulting decision silently.
type Routing struct {
	Mode           RoutingMode      `json:"mode,omitempty"`
	Objective      RoutingObjective `json:"objective,omitempty"`
	AllowProviders []string         `json:"allow_providers,omitempty"`
	DenyProviders  []string         `json:"deny_providers,omitempty"`
	Provider       string           `json:"provider,omitempty"`
	Model          string           `json:"model,omitempty"`
}

// NormalizeDefault returns the routing with the contract defaults applied: a
// zero routing (the caller omitted the object entirely) becomes
// {mode: auto, objective: balanced}, and an auto routing without an
// objective defaults to balanced. A routing that names any other field
// without a mode is returned unchanged so Validate rejects it — the default
// exists for omission, never for partially specified routing. An empty
// filter array counts as naming the field: {"allow_providers": []} is a
// partially specified routing, not an omitted one.
//
// Explicit mode additionally accepts the combined "provider/model" spelling
// in the model field: {"model": "openai/gpt-5.2"} with no provider splits at
// the FIRST slash into provider "openai" and model "gpt-5.2", and a model
// redundantly prefixed with the provider field's own value ("provider":
// "openai", "model": "openai/gpt-5.2") has the prefix stripped. Splitting at
// the first slash is what keeps slash-bearing upstream ids expressible:
// "together/meta-llama/Llama-X" names provider "together" and model
// "meta-llama/Llama-X". A model whose own id contains a slash therefore
// cannot be sent without naming its provider one way or the other — the
// leading segment is always claimed as the provider — which is the existing
// contract anyway: explicit mode has never accepted a bare model.
func (r Routing) NormalizeDefault() Routing {
	if r.Mode == "" && r.Objective == "" && r.AllowProviders == nil && r.DenyProviders == nil && r.Provider == "" && r.Model == "" {
		return Routing{Mode: RoutingModeAuto, Objective: ObjectiveBalanced}
	}
	if r.Mode == RoutingModeAuto && r.Objective == "" {
		r.Objective = ObjectiveBalanced
	}
	if r.Mode == RoutingModeExplicit {
		if prefix, rest, found := strings.Cut(r.Model, "/"); found && strings.TrimSpace(prefix) != "" && strings.TrimSpace(rest) != "" {
			if r.Provider == "" {
				r.Provider, r.Model = prefix, rest
			} else if prefix == r.Provider {
				r.Model = rest
			}
		}
	}
	return r
}

// Validate checks the closed tagged-union contract. Callers normalize with
// NormalizeDefault first; a zero routing fails here by design.
func (r Routing) Validate() error {
	switch r.Mode {
	case RoutingModeAuto:
		if !validObjective(r.Objective) {
			return fmt.Errorf("objective: unsupported value %q", r.Objective)
		}
		if r.Provider != "" || r.Model != "" {
			return fmt.Errorf("provider and model are valid only in explicit mode")
		}
		for i, provider := range r.AllowProviders {
			if strings.TrimSpace(provider) == "" {
				return fmt.Errorf("allow_providers[%d]: provider id must not be blank", i)
			}
		}
		for i, provider := range r.DenyProviders {
			if strings.TrimSpace(provider) == "" {
				return fmt.Errorf("deny_providers[%d]: provider id must not be blank", i)
			}
		}
	case RoutingModeExplicit:
		if strings.TrimSpace(r.Provider) == "" || strings.TrimSpace(r.Model) == "" {
			return fmt.Errorf("explicit mode requires provider and model")
		}
		// A non-nil empty slice is a present field: json.Unmarshal keeps []
		// distinct from an omitted array, so {"allow_providers": []} on an
		// explicit routing is a cross-arm field like any other, not noise
		// for omitempty to silently rewrite away on the next marshal.
		if r.Objective != "" || r.AllowProviders != nil || r.DenyProviders != nil {
			return fmt.Errorf("objective and provider filters are valid only in auto mode")
		}
	default:
		return fmt.Errorf("mode: unsupported value %q", r.Mode)
	}
	return nil
}

// Route is the response-side counterpart of Routing: the concrete decision
// that served a request. Region is the Speko relay location, not a
// provider-processing residency guarantee, and AttemptID identifies the
// (possibly post-fallback) attempt that produced the output.
type Route struct {
	Provider  string `json:"provider"`
	Model     string `json:"model"`
	Region    string `json:"region"`
	AttemptID string `json:"attempt_id"`
}

// Validate checks that a served route is fully concrete.
func (r Route) Validate() error {
	if strings.TrimSpace(r.Provider) == "" || strings.TrimSpace(r.Model) == "" || strings.TrimSpace(r.Region) == "" || strings.TrimSpace(r.AttemptID) == "" {
		return fmt.Errorf("provider, model, region, and attempt_id are required")
	}
	return nil
}

func validObjective(v RoutingObjective) bool {
	return v == ObjectiveBalanced || v == ObjectiveQuality || v == ObjectiveLatency || v == ObjectiveCost
}
