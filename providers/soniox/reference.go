package soniox

import (
	"strings"

	"github.com/SpekoAI/gateway/protocol"
)

const (
	// reservationReferencePrefix namespaces the reservation inside Soniox's
	// client_reference_id, mirroring the Deepgram adapter's
	// `extra=speko_reservation:<id>` convention. All three Soniox surfaces —
	// realtime STT, realtime TTS, async batch — write this one spelling so a
	// usage-log entry can be attributed without knowing which surface
	// produced it.
	reservationReferencePrefix = "speko_reservation:"

	// maxClientReferenceCharacters is the vendor's documented ceiling for
	// client_reference_id. Soniox rejects a longer value with HTTP 400
	// "`client_reference_id` is N characters, which exceeds the maximum
	// allowed length of 256." — it fails the whole request rather than
	// dropping the tag, so an over-long stamp is omitted instead of sent.
	maxClientReferenceCharacters = 256
)

// reservationReference is the client_reference_id a Soniox request carries so
// the provider's own usage log can be matched back to the Speko reservation
// the request was spent under. Soniox echoes the value verbatim in
// GET /v1/usage-logs, and provider-authoritative settlement attributes the
// vendor's invoiced cost_usd by it; an unstamped request is attributable to no
// reservation and no organization, and can only be charged from relay
// telemetry at a flat catalog rate.
//
// The stamp follows the plan's BILLING AUTHORITY, not its credential
// placement, exactly as the Deepgram adapter's `extra` tag does. A relay plan
// settles against a Speko reservation just like a managed provider-direct
// plan, so both are tagged; a BYOK provider-direct plan bills the customer's
// own Soniox project and must never carry a Speko reservation id. Keying on
// CredentialSource alone drops every relay plan, and the relay is the route
// that reaches Soniox on a long-lived key — the only route the vendor records
// the tag for at all, because client_reference_id is documented as "Ignored if
// the request authenticates with a temporary API key". On managed
// provider-direct routes the control plane binds the identifier to the
// temporary key at mint time instead; the request-level stamp stays correct
// there and costs nothing.
func reservationReference(plan protocol.SessionPlan) string {
	if plan.Execution.ProviderRoute != protocol.RouteSpekoRelay && plan.Execution.CredentialSource != protocol.CredentialsManaged {
		return ""
	}
	return reservationReferenceID(plan.Reservation.ID)
}

// reservationReferenceID also bounds batch tags, whose plans are relay-only.
func reservationReferenceID(id string) string {
	reservationID := strings.TrimSpace(id)
	if reservationID == "" {
		return ""
	}
	reference := reservationReferencePrefix + reservationID
	if len(reference) > maxClientReferenceCharacters {
		return ""
	}
	return reference
}
