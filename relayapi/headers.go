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
	// HeaderRegion names the Speko relay location that served the request.
	// It is a proximity fact about Speko infrastructure, not a guarantee
	// about where the provider processed the content.
	HeaderRegion = "Speko-Region"
)

// HeaderIdempotencyKey is required on every POST and on every WebSocket
// upgrade request. See idempotency.go for the content-hash and replay
// semantics attached to the key.
const HeaderIdempotencyKey = "Idempotency-Key"

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
