package relayapi

import (
	"fmt"
	"strings"
)

// ErrorCode is a stable, machine-readable failure class. The set is closed:
// the relay normalizes every failure — including provider failures — to one
// of these codes and never forwards a raw provider response body.
//
// When an error envelope travels as an HTTP response, each code maps to one
// canonical status — retry logic commonly keys off the status before parsing
// the body, so the mapping is contract, not implementation detail:
//
//	400  capability_unsupported, invalid_request
//	401  authentication_failed
//	402  insufficient_credit
//	409  idempotency_conflict, request_in_progress, request_already_started
//	413  payload_too_large
//	415  unsupported_media
//	429  rate_limited, concurrency_exhausted
//	500  relay_error
//	502  provider_error
//	503  provider_unavailable
//	504  request_timeout
//
// budget_exhausted and lease_expired terminate streams that are already
// established — an SSE error event or a WebSocket error frame after the 200
// or the upgrade — so no HTTP status carries them.
type ErrorCode string

const (
	ErrorCodeAuthenticationFailed  ErrorCode = "authentication_failed"
	ErrorCodeInsufficientCredit    ErrorCode = "insufficient_credit"
	ErrorCodeCapabilityUnsupported ErrorCode = "capability_unsupported"
	ErrorCodeInvalidRequest        ErrorCode = "invalid_request"
	ErrorCodeRateLimited           ErrorCode = "rate_limited"
	ErrorCodeConcurrencyExhausted  ErrorCode = "concurrency_exhausted"
	ErrorCodeProviderError         ErrorCode = "provider_error"
	ErrorCodeProviderUnavailable   ErrorCode = "provider_unavailable"
	ErrorCodeRelayError            ErrorCode = "relay_error"
	// ErrorCodeIdempotencyConflict reports an Idempotency-Key reused with a
	// different content hash. The other two idempotency codes report a reuse
	// with the SAME hash: request_in_progress while a prior admission is
	// still live (retryable), request_already_started once output may have
	// been produced — stateless mode cannot replay output, so the caller
	// gets the original request id instead of a rerun.
	ErrorCodeIdempotencyConflict   ErrorCode = "idempotency_conflict"
	ErrorCodeRequestInProgress     ErrorCode = "request_in_progress"
	ErrorCodeRequestAlreadyStarted ErrorCode = "request_already_started"
	ErrorCodeBudgetExhausted       ErrorCode = "budget_exhausted"
	ErrorCodeLeaseExpired          ErrorCode = "lease_expired"
	ErrorCodePayloadTooLarge       ErrorCode = "payload_too_large"
	ErrorCodeUnsupportedMedia      ErrorCode = "unsupported_media"
	ErrorCodeRequestTimeout        ErrorCode = "request_timeout"
)

// ErrorCodes returns the closed code set in a stable order. It exists so
// spec drift checks and exhaustive tests never re-list the codes by hand.
func ErrorCodes() []ErrorCode {
	return []ErrorCode{
		ErrorCodeAuthenticationFailed,
		ErrorCodeInsufficientCredit,
		ErrorCodeCapabilityUnsupported,
		ErrorCodeInvalidRequest,
		ErrorCodeRateLimited,
		ErrorCodeConcurrencyExhausted,
		ErrorCodeProviderError,
		ErrorCodeProviderUnavailable,
		ErrorCodeRelayError,
		ErrorCodeIdempotencyConflict,
		ErrorCodeRequestInProgress,
		ErrorCodeRequestAlreadyStarted,
		ErrorCodeBudgetExhausted,
		ErrorCodeLeaseExpired,
		ErrorCodePayloadTooLarge,
		ErrorCodeUnsupportedMedia,
		ErrorCodeRequestTimeout,
	}
}

// MaxErrorHintBytes bounds the caller-facing remediation text. Hints are
// intentionally short, single-line, relay-authored strings; provider payloads
// must never be copied into them.
const MaxErrorHintBytes = 512

// DefaultErrorHint returns the safe remediation used when a more specific
// call-site hint is unavailable. Keeping this exhaustive beside ErrorCodes
// guarantees older connectors can omit hint while a newer edge still emits a
// useful public error.
func DefaultErrorHint(code ErrorCode) string {
	switch code {
	case ErrorCodeAuthenticationFailed:
		return "Check that the Bearer token is an active Speko API key and try again."
	case ErrorCodeInsufficientCredit:
		return "Add credit or reduce the requested work before retrying."
	case ErrorCodeCapabilityUnsupported:
		return "Remove the unsupported option or select a model that advertises the capability in GET /v1/models."
	case ErrorCodeInvalidRequest:
		return "Correct the request fields described by the message and try again."
	case ErrorCodeRateLimited:
		return "Retry with exponential backoff and reduce the request rate."
	case ErrorCodeConcurrencyExhausted:
		return "Wait for an active request to finish, then retry."
	case ErrorCodeProviderError:
		return "Retry the request; if it continues, choose another provider or use auto routing."
	case ErrorCodeProviderUnavailable:
		return "Retry after a brief delay or use auto routing so the relay can choose another provider."
	case ErrorCodeRelayError:
		return "Retry once; if it continues, contact support with the request_id."
	case ErrorCodeIdempotencyConflict:
		return "Use a new Idempotency-Key when the request content changes."
	case ErrorCodeRequestInProgress:
		return "Wait for the original request to finish before retrying with the same Idempotency-Key."
	case ErrorCodeRequestAlreadyStarted:
		return "Do not replay this request; use the returned request_id to correlate the original attempt."
	case ErrorCodeBudgetExhausted:
		return "Start a new request with a larger budget or reduce the requested output."
	case ErrorCodeLeaseExpired:
		return "Start a new streaming session; the previous session lease cannot be renewed."
	case ErrorCodePayloadTooLarge:
		return "Reduce the request payload to the documented size limit and try again."
	case ErrorCodeUnsupportedMedia:
		return "Convert the audio to a format advertised by GET /v1/models and try again."
	case ErrorCodeRequestTimeout:
		return "The provider made no progress for 30 seconds; retry or use auto routing."
	default:
		return "Retry once; if it continues, contact support with the request_id."
	}
}

// ErrorBody is the normalized failure detail. Message is human-readable and
// intentionally carries no provider payload; RequestID is present whenever a
// relay request id had been minted before the failure.
type ErrorBody struct {
	Code      ErrorCode `json:"code"`
	Message   string    `json:"message"`
	Hint      string    `json:"hint"`
	Retryable bool      `json:"retryable"`
	RequestID string    `json:"request_id,omitempty"`
}

// Validate checks that a body uses the closed code set and says something.
func (b ErrorBody) Validate() error {
	if !validErrorCode(b.Code) {
		return fmt.Errorf("code: unsupported value %q", b.Code)
	}
	if strings.TrimSpace(b.Message) == "" {
		return fmt.Errorf("message: required")
	}
	if strings.TrimSpace(b.Hint) == "" {
		return fmt.Errorf("hint: required")
	}
	if b.Hint != strings.TrimSpace(b.Hint) || strings.ContainsAny(b.Hint, "\r\n") {
		return fmt.Errorf("hint: must be a trimmed single line")
	}
	if len([]byte(b.Hint)) > MaxErrorHintBytes {
		return fmt.Errorf("hint: must be at most %d bytes", MaxErrorHintBytes)
	}
	return nil
}

// ErrorEnvelope is the body of every non-2xx HTTP response and the data
// payload of the SSE error event, so both transports share one normalized
// error shape.
type ErrorEnvelope struct {
	Error ErrorBody `json:"error"`
}

// Validate checks the enclosed body.
func (e ErrorEnvelope) Validate() error {
	if err := e.Error.Validate(); err != nil {
		return fmt.Errorf("error: %w", err)
	}
	return nil
}

// ErrorEventType tags the terminal error frame on STT and TTS WebSocket
// sessions. The name matches the SSE error event deliberately: one error
// vocabulary across every streaming transport.
const ErrorEventType = "error"

// ErrorEvent is the terminal error frame on a WebSocket session. Exactly one
// terminal frame — session.closed or error — ends every session.
type ErrorEvent struct {
	Type  string    `json:"type"`
	Error ErrorBody `json:"error"`
}

// Validate checks the frame tag and the enclosed body.
func (e ErrorEvent) Validate() error {
	if e.Type != ErrorEventType {
		return fmt.Errorf("type: got %q, want %q", e.Type, ErrorEventType)
	}
	if err := e.Error.Validate(); err != nil {
		return fmt.Errorf("error: %w", err)
	}
	return nil
}

func validErrorCode(v ErrorCode) bool {
	for _, code := range ErrorCodes() {
		if v == code {
			return true
		}
	}
	return false
}
