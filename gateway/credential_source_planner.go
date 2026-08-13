package gateway

import (
	"context"
	"errors"

	"github.com/SpekoAI/gateway/controlplane"
	"github.com/SpekoAI/gateway/protocol"
)

// CredentialSourcePlanner lets one Gateway serve local BYOK sessions and
// hosted managed sessions at the same time. BYOK requests never leave the
// process; all other requests retain the hosted control-plane behavior.
type CredentialSourcePlanner struct {
	byok    PlanClient
	managed PlanClient
}

func NewCredentialSourcePlanner(byok, managed PlanClient) (*CredentialSourcePlanner, error) {
	if byok == nil || managed == nil {
		return nil, errors.New("gateway: BYOK and managed planners are required")
	}
	return &CredentialSourcePlanner{byok: byok, managed: managed}, nil
}

func (p *CredentialSourcePlanner) planner(source protocol.CredentialSource) (PlanClient, error) {
	switch source {
	case protocol.CredentialsBYOK:
		return p.byok, nil
	case protocol.CredentialsManaged:
		return p.managed, nil
	default:
		return nil, errors.New("gateway: unsupported credential source")
	}
}

func (p *CredentialSourcePlanner) CreateSessionPlan(ctx context.Context, request protocol.SessionPlanRequest, options controlplane.CreateOptions) (protocol.SessionPlan, string, error) {
	planner, err := p.planner(request.Execution.CredentialSource)
	if err != nil {
		return protocol.SessionPlan{}, "", err
	}
	return planner.CreateSessionPlan(ctx, request, options)
}

func (p *CredentialSourcePlanner) CreateSessionPlanBatch(ctx context.Context, request protocol.SessionPlanRequest, count int, options controlplane.CreateOptions) ([]protocol.SessionPlan, string, error) {
	planner, err := p.planner(request.Execution.CredentialSource)
	if err != nil {
		return nil, "", err
	}
	return planner.CreateSessionPlanBatch(ctx, request, count, options)
}

func (p *CredentialSourcePlanner) ExchangeFallbackPlan(ctx context.Context, plan protocol.SessionPlan, request controlplane.FallbackRequest, idempotencyKey string) (protocol.SessionPlan, string, error) {
	planner, err := p.planner(plan.Execution.CredentialSource)
	if err != nil {
		return protocol.SessionPlan{}, "", err
	}
	return planner.ExchangeFallbackPlan(ctx, plan, request, idempotencyKey)
}
