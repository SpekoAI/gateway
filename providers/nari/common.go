package nari

import (
	"encoding/json"
	"net/http"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// ProviderName is the only provider this package serves.
	ProviderName = "nari"

	officialHost = "api.narilabs.com"

	// extensionID namespaces raw vendor payloads attached to canonical events.
	extensionID = "api.narilabs.com/v1"

	// requestIDHeader keys Nari's request logs. It is set on synthesis
	// responses and on the transcription socket's handshake response.
	requestIDHeader = "X-Request-Id"
)

// acceptableCredentialKind accepts relay_access on the relay route, where the
// plan labels the connector's permanent key that way; the key travels in the
// same bearer header either way.
func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

// errorDetail is the `error` object both surfaces send.
type errorDetail struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"requestId"`
}

// classify maps a documented Nari error code onto the protocol classification
// and whether the same request may succeed later. An unknown code falls back
// to the HTTP status, and a status of zero (a socket event) to unavailable.
func classify(code string, status int) (string, bool) {
	switch code {
	case "INVALID_API_KEY":
		return "authentication_failed", false
	case "INSUFFICIENT_CREDITS":
		return "provider_quota_exceeded", false
	case "CONCURRENCY_LIMIT_EXCEEDED", "UPSTREAM_RATE_LIMITED":
		return "provider_rate_limited", true
	case "REQUEST_TOO_LARGE", "MESSAGE_TOO_LARGE":
		return "input_too_large", false
	case "INVALID_REQUEST", "INVALID_SPEECH_REQUEST", "INVALID_VOICE", "INVALID_VOICE_LANGUAGE",
		"UNSUPPORTED_RESPONSE_FORMAT", "MODEL_NOT_FOUND", "WEBSOCKET_REQUIRED", "SESSION_CONFIGURATION_LOCKED":
		return "invalid_request", false
	case "":
	default:
		// Every other documented code (INTERNAL_ERROR, UPSTREAM_UNAVAILABLE,
		// SERVER_AT_CAPACITY, SERVER_DRAINING, the *_UNAVAILABLE family, the VAD
		// failures, SESSION_IDLE_TIMEOUT, SESSION_SETUP_TIMEOUT) is documented as
		// retry-with-backoff.
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

func providerError(message string, detail *errorDetail, status int, raw []byte) *runtimepkg.ProviderError {
	code := ""
	if detail != nil {
		code = detail.Code
	}
	canonical, retryable := classify(code, status)
	providerErr := &runtimepkg.ProviderError{Code: canonical, Message: message, Retryable: retryable, ProviderStatus: status}
	if detail != nil && detail.Code != "" {
		providerErr.Message += " (" + detail.Code + ")"
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
