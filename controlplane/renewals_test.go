package controlplane_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/controlplane"
	"github.com/SpekoAI/gateway/protocol"
)

func renewalPlan(origin string) protocol.SessionPlan {
	plan := validPlan()
	plan.Execution.CredentialSource = protocol.CredentialsManaged
	plan.Route.Provider, plan.Route.Adapter = "google", "google.live.v1"
	plan.Telemetry.Token = "gwt1." + strings.Repeat("a", 43)
	plan.Reservation.Billing = &protocol.BillingAuthorization{
		Mode: "direct_entitlement", Currency: "USD", State: "estimated",
		RenewalURL:     origin + "/v1/sessions/session-test/entitlements/renew",
		RenewableUntil: plan.Reservation.LeaseExpiresAt.Add(300 * time.Second),
	}
	return plan
}

func renewedSlice(plan protocol.SessionPlan) controlplane.EntitlementRenewal {
	end := plan.Reservation.LeaseExpiresAt.Add(300 * time.Second)
	return controlplane.EntitlementRenewal{
		SessionID: plan.SessionID, AttemptID: plan.AttemptID, ReservationID: plan.Reservation.ID,
		Sequence: 1, PreviousExpiresAt: plan.Reservation.LeaseExpiresAt, LeaseExpiresAt: end,
		AuthorizedUnits: 300, ReservedAmountMicros: 9007199254740993, Currency: "USD",
		Credential: protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "owned-renewal-token", ExpiresAt: end},
	}
}

func TestRenewEntitlementRetriesWithTheSamePlanTokenBodyAndNumericInt64(t *testing.T) {
	var plan protocol.SessionPlan
	var first string
	calls := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodPost || r.URL.Path != "/v1/sessions/session-test/entitlements/renew" ||
			r.Header.Get("Authorization") != "Bearer "+plan.Telemetry.Token ||
			r.Header.Get("Speko-Protocol-Revision") != "3" || r.Header.Get("Idempotency-Key") != "renew-once" {
			t.Error("incorrect renewal authentication or route")
		}
		body, _ := io.ReadAll(r.Body)
		if calls == 1 {
			first = string(body)
		} else if first != string(body) {
			t.Error("retry changed the request body")
		}
		if calls == 1 {
			w.WriteHeader(503) // The cell may have committed before its ACK was lost.
			return
		}
		w.Header().Set("X-Request-ID", "cp_renew")
		_ = json.NewEncoder(w).Encode(renewedSlice(plan))
	}))
	t.Cleanup(server.Close)
	plan = renewalPlan(server.URL)
	client := newClient(t, server, "sk_speko_setup-only")
	request := controlplane.EntitlementRenewalRequest{PreviousExpiresAt: plan.Reservation.LeaseExpiresAt}
	_, _, err := client.RenewEntitlement(context.Background(), plan, request, "renew-once")
	var failure *controlplane.HTTPError
	if !errors.As(err, &failure) || failure.Status != 503 {
		t.Fatalf("first result: %v", err)
	}
	got, requestID, err := client.RenewEntitlement(context.Background(), plan, request, "renew-once")
	if err != nil || got.ReservedAmountMicros != 9007199254740993 || requestID != "cp_renew" || calls != 2 {
		t.Fatalf("retry: amount=%d request=%s calls=%d error=%v", got.ReservedAmountMicros, requestID, calls, err)
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal([]byte(first), &fields) != nil || len(fields) != 1 || fields["previous_expires_at"] == nil {
		t.Fatal("renewal sent caller-authored financial authority")
	}
}

func TestRenewEntitlementRefusesInvalidResponseBindingsAndBounds(t *testing.T) {
	for _, change := range []struct {
		name string
		edit func(*controlplane.EntitlementRenewal)
	}{
		{"session", func(r *controlplane.EntitlementRenewal) { r.SessionID = "foreign" }},
		{"attempt", func(r *controlplane.EntitlementRenewal) { r.AttemptID = "foreign" }},
		{"reservation", func(r *controlplane.EntitlementRenewal) { r.ReservationID = "foreign" }},
		{"previous", func(r *controlplane.EntitlementRenewal) { r.PreviousExpiresAt = r.PreviousExpiresAt.Add(time.Second) }},
		{"horizon", func(r *controlplane.EntitlementRenewal) { r.LeaseExpiresAt = r.LeaseExpiresAt.Add(time.Second) }},
		{"units", func(r *controlplane.EntitlementRenewal) { r.AuthorizedUnits++ }},
		{"credential", func(r *controlplane.EntitlementRenewal) {
			r.Credential.ExpiresAt = r.Credential.ExpiresAt.Add(time.Second)
		}},
		{"currency", func(r *controlplane.EntitlementRenewal) { r.Currency = "EUR" }},
		{"negative", func(r *controlplane.EntitlementRenewal) { r.ReservedAmountMicros = -1 }},
	} {
		t.Run(change.name, func(t *testing.T) {
			var plan protocol.SessionPlan
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				response := renewedSlice(plan)
				change.edit(&response)
				_ = json.NewEncoder(w).Encode(response)
			}))
			t.Cleanup(server.Close)
			plan = renewalPlan(server.URL)
			_, _, err := newClient(t, server, "sk_speko_test").RenewEntitlement(context.Background(), plan, controlplane.EntitlementRenewalRequest{PreviousExpiresAt: plan.Reservation.LeaseExpiresAt}, "renew")
			if err == nil || strings.Contains(err.Error(), "owned-renewal-token") {
				t.Fatalf("invalid response accepted or exposed credentials: %v", err)
			}
		})
	}
}

func TestRenewEntitlementRefusesForeignOriginsAndUnsupportedAuthorityBeforeIO(t *testing.T) {
	calls := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++ }))
	t.Cleanup(server.Close)
	client := newClient(t, server, "sk_speko_test")
	for _, edit := range []func(*protocol.SessionPlan){
		func(p *protocol.SessionPlan) { p.Reservation.Billing.RenewalURL = "https://foreign.test/renew" },
		func(p *protocol.SessionPlan) { p.Reservation.Billing.RenewalURL += "?token=forbidden" },
		func(p *protocol.SessionPlan) { p.Telemetry.Token = "sk_speko_test" },
		func(p *protocol.SessionPlan) { p.Execution.CredentialSource = protocol.CredentialsBYOK },
		func(p *protocol.SessionPlan) { p.Route.Provider = "elevenlabs" },
		func(p *protocol.SessionPlan) { p.Reservation.Billing = nil },
	} {
		plan := renewalPlan(server.URL)
		edit(&plan)
		_, _, err := client.RenewEntitlement(context.Background(), plan, controlplane.EntitlementRenewalRequest{PreviousExpiresAt: plan.Reservation.LeaseExpiresAt}, "renew")
		if err == nil {
			t.Fatal("unsupported authority accepted")
		}
	}
	if calls != 0 {
		t.Fatal("refusal performed provider-facing I/O")
	}
}

func TestRenewEntitlementDoesNotForwardItsPlanTokenThroughRedirects(t *testing.T) {
	calls := 0
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++ }))
	t.Cleanup(target.Close)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 307) }))
	t.Cleanup(server.Close)
	plan := renewalPlan(server.URL)
	_, _, err := newClient(t, server, "sk_speko_test").RenewEntitlement(context.Background(), plan, controlplane.EntitlementRenewalRequest{PreviousExpiresAt: plan.Reservation.LeaseExpiresAt}, "renew")
	if err == nil || calls != 0 {
		t.Fatalf("redirect followed: calls=%d error=%v", calls, err)
	}
}
