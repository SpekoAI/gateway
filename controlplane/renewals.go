package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/SpekoAI/gateway/protocol"
)

// EntitlementRenewalRequest identifies the last funded horizon. Preserve it
// and the idempotency key when retrying a request whose acknowledgment was lost.
type EntitlementRenewalRequest struct {
	PreviousExpiresAt time.Time `json:"previous_expires_at"`
}

// EntitlementRenewal is one newly funded slice, using the original price.
// MaximumAmountMicros describes this slice, not a cumulative charge. Only the
// cell computes money. Obtaining it alone does not extend a running session:
// its credential must be adopted before advancing the runtime's lease.
type EntitlementRenewal struct {
	EntitlementID       string                       `json:"entitlement_id"`
	SessionID           string                       `json:"session_id"`
	AttemptID           string                       `json:"attempt_id"`
	ReservationID       string                       `json:"reservation_id"`
	Sequence            int64                        `json:"sequence"`
	PreviousExpiresAt   time.Time                    `json:"previous_expires_at"`
	LeaseExpiresAt      time.Time                    `json:"lease_expires_at"`
	AuthorizedUnits     int64                        `json:"authorized_units"`
	MaximumAmountMicros int64                        `json:"maximum_amount_micros"`
	Currency            string                       `json:"currency"`
	Credential          protocol.DelegatedCredential `json:"credential"`
}

var gatewayPlanToken = regexp.MustCompile(`^gwt1\.[A-Za-z0-9_-]{43}$`)

// RenewEntitlement funds and mints the next Google realtime slice. The caller
// must first verify the signed plan. Its plan token authenticates this call;
// the setup API key is never substituted. A retry must keep both the request
// and key unchanged. Admission timing is decided by the cell's clock.
func (c *Client) RenewEntitlement(ctx context.Context, plan protocol.SessionPlan, request EntitlementRenewalRequest, idempotencyKey string) (EntitlementRenewal, string, error) {
	billing := plan.Reservation.Billing
	if plan.Execution.CredentialSource != protocol.CredentialsManaged ||
		plan.Execution.ProviderRoute != protocol.RouteProviderDirect ||
		plan.Route.Provider != "google" || plan.Route.Adapter != "google.live.v1" ||
		plan.Reservation.Usage.Unit != protocol.UsageUnitDurationSeconds ||
		billing == nil || billing.Mode != "direct_entitlement" || billing.Currency != "USD" ||
		!billing.RenewableUntil.After(plan.Reservation.LeaseExpiresAt) {
		return EntitlementRenewal{}, "", errors.New("controlplane: plan does not permit a Google entitlement renewal")
	}
	if strings.TrimSpace(plan.SessionID) == "" || strings.TrimSpace(plan.AttemptID) == "" ||
		strings.TrimSpace(plan.Reservation.ID) == "" || plan.Signature == "" ||
		!gatewayPlanToken.MatchString(plan.Telemetry.Token) ||
		strings.TrimSpace(idempotencyKey) == "" || len(idempotencyKey) > 1024 || strings.ContainsRune(idempotencyKey, 0) ||
		request.PreviousExpiresAt.Before(plan.Reservation.LeaseExpiresAt) || !request.PreviousExpiresAt.Before(billing.RenewableUntil) {
		return EntitlementRenewal{}, "", errors.New("controlplane: invalid entitlement renewal binding")
	}
	endpoint, err := url.Parse(billing.RenewalURL)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil ||
		endpoint.RawQuery != "" || endpoint.Fragment != "" || !sameOrigin(endpoint, c.baseURL) {
		return EntitlementRenewal{}, "", errors.New("controlplane: renewal URL must use the configured control-plane origin")
	}
	// A redirect is not part of the signed authority, even when an HTTP client
	// would otherwise forward Authorization to another port on the same host.
	client := *c.httpClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	body, requestID, err := c.postAuthenticated(ctx, endpoint, request, idempotencyKey, plan.Telemetry.Token, &client)
	if err != nil {
		return EntitlementRenewal{}, requestID, err
	}
	var renewal EntitlementRenewal
	if json.Unmarshal(body, &renewal) != nil || !validEntitlementRenewal(plan, request, renewal) {
		return EntitlementRenewal{}, requestID, errors.New("controlplane: invalid entitlement renewal response")
	}
	return renewal, requestID, nil
}

func validEntitlementRenewal(plan protocol.SessionPlan, request EntitlementRenewalRequest, renewal EntitlementRenewal) bool {
	duration := renewal.LeaseExpiresAt.Sub(request.PreviousExpiresAt)
	return strings.TrimSpace(renewal.EntitlementID) != "" && renewal.SessionID == plan.SessionID && renewal.AttemptID == plan.AttemptID &&
		renewal.ReservationID == plan.Reservation.ID && renewal.Sequence > 0 &&
		renewal.PreviousExpiresAt.Equal(request.PreviousExpiresAt) &&
		duration > 0 && duration <= 300*time.Second && duration%time.Second == 0 &&
		!renewal.LeaseExpiresAt.After(plan.Reservation.Billing.RenewableUntil) &&
		renewal.AuthorizedUnits == int64(duration/time.Second) && renewal.MaximumAmountMicros > 0 && renewal.Currency == "USD" &&
		renewal.Credential.Kind == protocol.CredentialBearer && strings.TrimSpace(renewal.Credential.Value) != "" &&
		renewal.Credential.ExpiresAt.Equal(renewal.LeaseExpiresAt)
}
