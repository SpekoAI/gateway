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
//	403  not_entitled, preview_access_required
//	409  idempotency_conflict, request_in_progress, request_already_started
//	413  payload_too_large
//	415  unsupported_media
//	429  rate_limited, concurrency_exhausted
//	500  relay_error
//	502  provider_error
//	503  provider_unavailable
//
// budget_exhausted and lease_expired terminate streams that are already
// established — an SSE error event or a WebSocket error frame after the 200
// or the upgrade — so no HTTP status carries them.
type ErrorCode string

const (
	ErrorCodeAuthenticationFailed  ErrorCode = "authentication_failed"
	ErrorCodeNotEntitled           ErrorCode = "not_entitled"
	ErrorCodePreviewAccessRequired ErrorCode = "preview_access_required"
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
)

// ErrorCodes returns the closed code set in a stable order. It exists so
// spec drift checks and exhaustive tests never re-list the codes by hand.
func ErrorCodes() []ErrorCode {
	return []ErrorCode{
		ErrorCodeAuthenticationFailed,
		ErrorCodeNotEntitled,
		ErrorCodePreviewAccessRequired,
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
	}
}

// ErrorBody is the normalized failure detail. Message is human-readable and
// intentionally carries no provider payload; RequestID is present whenever a
// relay request id had been minted before the failure.
type ErrorBody struct {
	Code      ErrorCode `json:"code"`
	Message   string    `json:"message"`
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
