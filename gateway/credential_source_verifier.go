package gateway

import (
	"context"
	"errors"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

// CredentialSourceVerifier retains the hosted verifier's trusted-time
// capability when a process also serves locally signed BYOK plans.
type CredentialSourceVerifier struct {
	BYOK    runtimepkg.PlanVerifier
	Managed runtimepkg.ClockedPlanVerifier
}

func (v CredentialSourceVerifier) Verify(ctx context.Context, plan protocol.SessionPlan) error {
	if plan.Execution.CredentialSource == protocol.CredentialsBYOK {
		if v.BYOK == nil {
			return runtimepkg.ErrPlanUnverified
		}
		return v.BYOK.Verify(ctx, plan)
	}
	if v.Managed == nil {
		return runtimepkg.ErrPlanUnverified
	}
	return v.Managed.Verify(ctx, plan)
}
func (v CredentialSourceVerifier) VerifyWithClock(ctx context.Context, plan protocol.SessionPlan, clock func(context.Context) (time.Time, error)) error {
	if plan.Execution.CredentialSource != protocol.CredentialsManaged {
		return errors.New("gateway: only managed plans use the hosted clock verifier")
	}
	if v.Managed == nil {
		return runtimepkg.ErrPlanUnverified
	}
	return v.Managed.VerifyWithClock(ctx, plan, clock)
}
