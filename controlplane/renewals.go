package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"io"
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

// Validate checks a continuation against the original verified plan. It does
// not verify a plan or grant authority to open a different attempt.
func (r EntitlementRenewal) Validate(plan protocol.SessionPlan, previous time.Time) error {
	if plan.Reservation.Billing == nil || !validEntitlementRenewal(plan, EntitlementRenewalRequest{PreviousExpiresAt: previous}, r) {
		return errors.New("controlplane: invalid entitlement renewal response")
	}
	return nil
}

// EntitlementClock samples the cell's SQL clock without reserving or minting.
// Callers account for the full round trip before using this sample.
func (c *Client) EntitlementClock(ctx context.Context, plan protocol.SessionPlan) (time.Time, error) {
	billing := plan.Reservation.Billing
	if billing == nil || !gatewayPlanToken.MatchString(plan.Telemetry.Token) || plan.Signature == "" {
		return time.Time{}, errors.New("controlplane: missing entitlement clock authority")
	}
	endpoint, err := url.Parse(billing.RenewalURL)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil ||
		endpoint.RawQuery != "" || endpoint.Fragment != "" || !sameOrigin(endpoint, c.baseURL) ||
		endpoint.EscapedPath() != "/v1/sessions/"+escapeIdentitySegment(plan.SessionID)+"/entitlements/renew" {
		return time.Time{}, errors.New("controlplane: invalid entitlement clock URL")
	}
	endpoint.Path = strings.TrimSuffix(endpoint.Path, "/renew") + "/clock"
	if endpoint.RawPath != "" {
		endpoint.RawPath = strings.TrimSuffix(endpoint.RawPath, "/renew") + "/clock"
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return time.Time{}, errors.New("controlplane: invalid clock request")
	}
	request.Header.Set("Authorization", "Bearer "+plan.Telemetry.Token)
	request.Header.Set("Speko-Protocol-Revision", "3")
	request.Header.Set("Cache-Control", "no-store")
	client := *c.httpClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Do(request)
	if err != nil {
		return time.Time{}, errors.New("controlplane: entitlement clock unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return time.Time{}, &HTTPError{Status: response.StatusCode}
	}
	var reply struct {
		ServerTime time.Time `json:"server_time"`
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 4097))
	if err != nil || len(data) > 4096 || json.Unmarshal(data, &reply) != nil || reply.ServerTime.IsZero() {
		return time.Time{}, errors.New("controlplane: invalid entitlement clock response")
	}
	return reply.ServerTime, nil
}
