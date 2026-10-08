package munsit

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// ProviderName is the only provider this package serves.
	ProviderName = "munsit"

	officialHost = "api.munsit.com"

	// extensionID namespaces raw vendor payloads attached to canonical events.
	extensionID = "api.munsit.com/api/v1"

	// apiKeyHeader carries the key on every surface, the socket handshake
	// included. Munsit reads no Authorization header.
	apiKeyHeader = "X-Api-Key"
)

// acceptableCredentialKind accepts relay_access on the relay route, where the
// plan labels the connector's permanent key that way; the key travels in the
// same header either way.
func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

// restError is the JSON body of a REST failure. The live API sends
// errorMessage; the docs spell it message and add statusCode.
type restError struct {
	StatusCode   int    `json:"statusCode"`
	ErrorCode    int    `json:"errorCode"`
	ErrorMessage string `json:"errorMessage"`
	Message      string `json:"message"`
}

// classify maps a documented REST errorCode onto the protocol classification
// and whether the same request may succeed later. An unknown code falls back
// to the HTTP status.
func classify(code, status int) (string, bool) {
	switch code {
	case 40101:
		return "authentication_failed", false
	case 40201:
		return "provider_quota_exceeded", false
	case 42901:
		return "provider_rate_limited", true
	case 40001, 40401:
		return "invalid_request", false
	case 50001, 50202:
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
	case status >= 300 && status < 500:
		return "invalid_request", false
	default:
		return "provider_unavailable", true
	}
}

func providerError(message string, detail restError, status int, raw []byte) *runtimepkg.ProviderError {
	canonical, retryable := classify(detail.ErrorCode, status)
	providerErr := &runtimepkg.ProviderError{Code: canonical, Message: message, Retryable: retryable, ProviderStatus: status}
	if detail.ErrorCode != 0 {
		providerErr.Message += fmt.Sprintf(" (%d)", detail.ErrorCode)
	}
	if json.Valid(raw) {
		providerErr.Extensions = extension(raw)
	}
	return providerErr
}

// baseLanguage lowercases a BCP-47 tag and strips everything after the
// primary subtag: ar-SA and ar_EG both become ar.
func baseLanguage(language string) string {
	base := strings.ToLower(strings.TrimSpace(language))
	if index := strings.IndexAny(base, "-_"); index > 0 {
		base = base[:index]
	}
	return base
}

func extension(raw []byte) map[string]json.RawMessage {
	return map[string]json.RawMessage{extensionID: append(json.RawMessage(nil), raw...)}
}

func marshalData(value any) json.RawMessage {
	payload, _ := json.Marshal(value)
	return payload
}
