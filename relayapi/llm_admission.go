package relayapi

import (
	"fmt"
	"strings"
	"time"
)

// Prepared LLM admissions move the per-request admission round trip off a
// voice turn's critical path. A voice client knows a request is coming as
// soon as the caller starts speaking, before it knows the request's content.
// POST /v1/llm/admissions takes an ordinary LLMRequest holding the
// conversation so far, admits it with headroom for the turn still being
// spoken, under the Idempotency-Key the real request will send, and returns
// the admission sealed into an opaque ticket (LLMAdmission). The real
// POST /v1/llm/responses presents the ticket in HeaderPreparedAdmission
// under that same key and is dispatched on it without a second admission,
// and HeaderPreparedAdmissionOutcome reports what became of the ticket. The
// Router never runs a second execution under a ticket's key. A ticket of the
// caller's under the request's key that does not fit, or whose attempt fails
// before the provider connector accepts it, is answered from what retiring it
// established: 409 idempotency_conflict with HeaderReplacementIdempotencyKey
// (retired, never ran: send the request again without the ticket under that
// key, which every copy of a duplicated request receives alike; retiring is
// idempotent, so a resend after a lost response gets the same answer), 409
// request_already_started (used and may have run, or expired unconfirmed: do
// not resend), or another failure (unconfirmed: retry unchanged, ticket and
// key included). Only that header licenses a resend under another key; an
// idempotency_conflict without it comes from an ordinary admission and keeps
// its ordinary meaning, and a failure after the connector accepted the
// request is an ordinary failure of a request that may have run. A ticket
// that is not the caller's, or was prepared under another key, is ignored
// and the request admitted the ordinary way. A ticket the client will not
// present is released with POST /v1/llm/admissions/release.

// Object tags of the two prepared-admission response bodies.
const (
	LLMAdmissionObject        = "llm.admission"
	LLMAdmissionReleaseObject = "llm.admission.release"
)

// LLMAdmission is the 201 body of POST /v1/llm/admissions. Admission is the
// ticket: opaque, issued with the spa1. prefix, and meaningful only to the
// Router region that issued it. ExpiresAt is when the admitted plan expires;
// a ticket is redeemed only while at least five seconds of that remain.
type LLMAdmission struct {
	Object    string    `json:"object"`
	Admission string    `json:"admission"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Validate checks the object tag, that a ticket is present, and that the
// expiry is set.
func (a LLMAdmission) Validate() error {
	if a.Object != LLMAdmissionObject {
		return fmt.Errorf("object: must be %q", LLMAdmissionObject)
	}
	if strings.TrimSpace(a.Admission) == "" {
		return fmt.Errorf("admission: required")
	}
	if a.ExpiresAt.IsZero() {
		return fmt.Errorf("expires_at: required")
	}
	return nil
}

// LLMAdmissionReleaseRequest is the POST /v1/llm/admissions/release body:
// the ticket exactly as POST /v1/llm/admissions returned it.
type LLMAdmissionReleaseRequest struct {
	Admission string `json:"admission"`
}

// Validate checks that the body names a ticket.
func (r LLMAdmissionReleaseRequest) Validate() error {
	if strings.TrimSpace(r.Admission) == "" {
		return fmt.Errorf("admission: required")
	}
	return nil
}

// LLMAdmissionRelease is the 200 body of POST /v1/llm/admissions/release.
// Released reports whether the ticket's plan is retired, by this call or an
// earlier release or retirement (so a release can be sent again), after
// which the served-nothing report that frees its slot is delivered or still
// being retried. false means a request used the ticket, it expired, or the
// Router could not retire it now, and its reservation then ends the way any
// unused admission's does.
type LLMAdmissionRelease struct {
	Object   string `json:"object"`
	Released bool   `json:"released"`
}

// Validate checks the object tag.
func (r LLMAdmissionRelease) Validate() error {
	if r.Object != LLMAdmissionReleaseObject {
		return fmt.Errorf("object: must be %q", LLMAdmissionReleaseObject)
	}
	return nil
}

// PreparedAdmissionOutcome is the closed value set of
// HeaderPreparedAdmissionOutcome. Every value but PreparedAdmissionUsed says
// why the ticket was not used. PreparedAdmissionInvalid arrives on an
// ordinary admission under the request's own key; the others arrive on a
// refusal; only HeaderReplacementIdempotencyKey, never the outcome, says
// whether the client may resend under another Idempotency-Key.
type PreparedAdmissionOutcome string

const (
	// PreparedAdmissionUsed: the ticket passed every check and its plan was
	// presented for dispatch, with no admission round trip.
	PreparedAdmissionUsed PreparedAdmissionOutcome = "used"
	// PreparedAdmissionInvalid: the ticket did not decrypt, was issued to
	// another API key, client session, or region, or was prepared under
	// another Idempotency-Key.
	PreparedAdmissionInvalid PreparedAdmissionOutcome = "invalid"
	// PreparedAdmissionExpired: the ticket's plan had less than five seconds
	// left.
	PreparedAdmissionExpired PreparedAdmissionOutcome = "expired"
	// PreparedAdmissionStale: the ticket was issued under another catalog
	// release.
	PreparedAdmissionStale PreparedAdmissionOutcome = "stale"
	// PreparedAdmissionRoute: the routing mode changed, or the prepared
	// provider and model are not a candidate for this request.
	PreparedAdmissionRoute PreparedAdmissionOutcome = "route"
	// PreparedAdmissionBudget: the request needs more input or output tokens
	// than the ticket reserved.
	PreparedAdmissionBudget PreparedAdmissionOutcome = "budget"
)

// PreparedAdmissionOutcomes returns the closed outcome set in a stable
// order, the order the OpenAPI enum lists it in.
func PreparedAdmissionOutcomes() []PreparedAdmissionOutcome {
	return []PreparedAdmissionOutcome{
		PreparedAdmissionUsed,
		PreparedAdmissionInvalid,
		PreparedAdmissionExpired,
		PreparedAdmissionStale,
		PreparedAdmissionRoute,
		PreparedAdmissionBudget,
	}
}
