package paxa

import (
	"encoding/json"
	"net/http"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// ProviderName is the only provider this package serves.
	ProviderName = "paxa"

	officialHost = "api.paxalabs.com"

	// extensionID namespaces raw vendor payloads attached to canonical events.
	extensionID = "api.paxalabs.com/v1"

	// requestIDHeader keys Paxa's request logs and support reports.
	requestIDHeader = "X-Request-Id"
)

// acceptableCredentialKind accepts relay_access on the relay route, where the
// plan labels the connector's permanent key that way; the key travels in the
// same bearer header either way.
func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

// problem is the RFC 9457 body every error carries. Paxa sends no prose:
// `title` is the stable code.
type problem struct {
	Title  string `json:"title"`
	Status int    `json:"status"`
}

// classify maps a documented Paxa error code onto the protocol classification
// and whether the same request may succeed later. An unknown code falls back
// to the HTTP status.
func classify(code string, status int) (string, bool) {
	switch code {
	case "unauthorized":
		return "authentication_failed", false
	case "insufficient_credits", "key_limit":
		return "provider_quota_exceeded", false
	case "rate_limited", "concurrency_limited":
		return "provider_rate_limited", true
	case "text_too_long":
		return "input_too_large", false
	case "validation", "unknown_model", "unknown_voice", "tag_invalid", "unspeakable_text", "content_blocked", "idempotency_mismatch", "not_found":
		return "invalid_request", false
	case "internal", "provider_error", "provider_unavailable", "idempotency_in_flight", "idempotency_refunded":
		// Each is documented as retry-later; a retry here is a new request,
		// and this adapter never sends an Idempotency-Key.
		return "provider_unavailable", true
	}
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return "authentication_failed", false
	case status == http.StatusPaymentRequired:
		return "provider_quota_exceeded", false
	case status == http.StatusRequestEntityTooLarge:
		return "input_too_large", false
	case status == http.StatusTooManyRequests:
		return "provider_rate_limited", true
	case status >= 400 && status < 500:
		return "invalid_request", false
	default:
		return "provider_unavailable", true
	}
}

func providerError(message string, detail problem, status int, raw []byte) *runtimepkg.ProviderError {
	canonical, retryable := classify(detail.Title, status)
	providerErr := &runtimepkg.ProviderError{Code: canonical, Message: message, Retryable: retryable, ProviderStatus: status}
	if detail.Title != "" {
		providerErr.Message += " (" + detail.Title + ")"
	}
	if json.Valid(raw) {
		providerErr.Extensions = extension(raw)
	}
	return providerErr
}

func extension(raw []byte) map[string]json.RawMessage {
	return map[string]json.RawMessage{extensionID: append(json.RawMessage(nil), raw...)}
}

func marshalData(value any) json.RawMessage {
	payload, _ := json.Marshal(value)
	return payload
}
