package gateway_test

import (
	"context"
	"testing"

	"github.com/SpekoAI/gateway/controlplane"
	"github.com/SpekoAI/gateway/gateway"
	"github.com/SpekoAI/gateway/protocol"
)

type sourcePlannerStub struct {
	name  string
	calls int
}

func (p *sourcePlannerStub) CreateSessionPlan(context.Context, protocol.SessionPlanRequest, controlplane.CreateOptions) (protocol.SessionPlan, string, error) {
	p.calls++
	return protocol.SessionPlan{PlanID: p.name}, p.name, nil
}

func (p *sourcePlannerStub) CreateSessionPlanBatch(context.Context, protocol.SessionPlanRequest, int, controlplane.CreateOptions) ([]protocol.SessionPlan, string, error) {
	p.calls++
	return []protocol.SessionPlan{{PlanID: p.name}}, p.name, nil
}

func (p *sourcePlannerStub) ExchangeFallbackPlan(context.Context, protocol.SessionPlan, controlplane.FallbackRequest, string) (protocol.SessionPlan, string, error) {
	p.calls++
	return protocol.SessionPlan{PlanID: p.name}, p.name, nil
}

func TestCredentialSourcePlannerKeepsBYOKLocal(t *testing.T) {
	t.Parallel()
	byok := &sourcePlannerStub{name: "byok"}
	managed := &sourcePlannerStub{name: "managed"}
	planner, err := gateway.NewCredentialSourcePlanner(byok, managed)
	if err != nil {
		t.Fatalf("new credential source planner: %v", err)
	}

	request := protocol.SessionPlanRequest{Execution: protocol.ExecutionRequest{CredentialSource: protocol.CredentialsBYOK}}
	plan, _, err := planner.CreateSessionPlan(context.Background(), request, controlplane.CreateOptions{})
	if err != nil || plan.PlanID != "byok" || byok.calls != 1 || managed.calls != 0 {
		t.Fatalf("BYOK plan = %+v, err=%v, calls=%d/%d", plan, err, byok.calls, managed.calls)
	}

	request.Execution.CredentialSource = protocol.CredentialsManaged
	plan, _, err = planner.CreateSessionPlan(context.Background(), request, controlplane.CreateOptions{})
	if err != nil || plan.PlanID != "managed" || byok.calls != 1 || managed.calls != 1 {
		t.Fatalf("managed plan = %+v, err=%v, calls=%d/%d", plan, err, byok.calls, managed.calls)
	}
}
