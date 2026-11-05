package relayapi

// Response headers returned by every relay endpoint. They identify the
// request and the concrete route decision without carrying any content.
const (
	// HeaderRequestID is the relay request id, also embedded in error
	// envelopes and Speko-minted LLM response ids.
	HeaderRequestID = "Speko-Request-ID"
	// HeaderAttemptID identifies the (possibly post-fallback) attempt that
	// produced the response.
	HeaderAttemptID = "Speko-Attempt-ID"
	// HeaderProvider and HeaderModel name the concrete provider decision.
	HeaderProvider = "Speko-Provider"
	HeaderModel    = "Speko-Model"
	// HeaderRegion names the Speko Router location that served the request.
	// It is a proximity fact about Speko infrastructure, not a guarantee
	// about where the provider processed the content.
	HeaderRegion = "Speko-Region"
)

// HeaderIdempotencyKey is required on every POST and on every WebSocket
// upgrade request, with one exception: POST /v1/llm/admissions/release
// takes it optionally, because releasing a ticket replays nothing. See
// idempotency.go for the content-hash and replay semantics attached to the
// key.
const HeaderIdempotencyKey = "Idempotency-Key"

// HeaderClientSessionID is the optional request header that ties a request
// to the caller's Speko session; the Router records it on the admission. A
// prepared LLM admission is bound to the value its prepare call sent, so the
// real request and any release must send the same value (or omit it alike).
const HeaderClientSessionID = "Speko-Client-Session-ID"

// Prepared LLM admission headers. HeaderPreparedAdmission carries a ticket
// from POST /v1/llm/admissions on the POST /v1/llm/responses request it was
// prepared for. HeaderPreparedAdmissionOutcome is the response header that
// says, on a request that carried a ticket, whether the ticket was used and
// if not why; its values are the closed PreparedAdmissionOutcome set.
const (
	HeaderPreparedAdmission        = "Speko-Prepared-Admission"
	HeaderPreparedAdmissionOutcome = "Speko-Prepared-Admission-Outcome"
)

// Agent-readable lifecycle and throttling headers. RateLimit-Policy follows
// the current IETF HTTPAPI structured-field draft; Retry-After, Deprecation,
// and Sunset use their standard HTTP definitions.
const (
	HeaderRateLimitPolicy = "RateLimit-Policy"
	HeaderRetryAfter      = "Retry-After"
	HeaderDeprecation     = "Deprecation"
	HeaderSunset          = "Sunset"
)

// HeaderUsageCharacters is the TTS-only usage header on POST /v1/tts/speech
// responses: the billed character count for the synthesized input. It exists
// because the one-shot TTS response body is a raw audio stream with no place
// for a usage object. There is deliberately no STT equivalent — STT usage
// lives in the JSON response body and in WebSocket usage events.
const HeaderUsageCharacters = "Speko-Usage-Characters"
