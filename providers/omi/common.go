package omi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// ProviderName is the only provider this package serves.
	ProviderName = "omi"
	// Model is the only model routed. Omi also serves omi-general-1, which
	// the partner agreement leaves out.
	Model = "omi-medical-1"

	// officialHost is Omi's US (Ohio) host. It is the only host either
	// adapter accepts.
	officialHost = "api.us.omi.health"

	// extensionID namespaces raw vendor payloads attached to canonical events.
	extensionID = "api.us.omi.health/v1"

	// realtimeDefaultLanguage is sent when the caller names no language: the
	// socket's auto drops every utterance.
	realtimeDefaultLanguage = "en"
	// batchAutoLanguage asks an upload to detect its language.
	batchAutoLanguage = "auto"
)

// languages are the base codes Omi transcribes on both surfaces. Only en is
// medically validated; the others are Omi's "testing only" languages.
var languages = map[string]struct{}{
	"en": {}, "es": {}, "pt": {}, "fr": {}, "de": {}, "nl": {}, "ar": {}, "hi": {},
}

// acceptableCredentialKind accepts relay_access on the relay route, where the
// plan labels the connector's permanent key that way.
func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

// baseLanguage lowercases a BCP-47 tag and strips everything after the
// primary subtag: es-MX and pt_BR become es and pt.
func baseLanguage(language string) string {
	base := strings.ToLower(strings.TrimSpace(language))
	if index := strings.IndexAny(base, "-_"); index > 0 {
		base = base[:index]
	}
	return base
}

// realtimeLanguage maps the caller's tag onto the code the socket gets. Omi
// accepts regional tags too, but the base code is what it reports back. No
// language means en; auto is refused, because the socket accepts it and then
// drops every utterance.
func realtimeLanguage(language string) (string, error) {
	base := baseLanguage(language)
	if base == "" {
		return realtimeDefaultLanguage, nil
	}
	if base == batchAutoLanguage {
		return "", &runtimepkg.ProviderError{
			Code:    "unsupported_language",
			Message: "Omi realtime transcription cannot detect the language",
			Hint:    "Name one of en, es, pt, fr, de, nl, ar or hi, or omit the language for English.",
		}
	}
	return supportedLanguage(base, language)
}

// batchLanguage maps the caller's tag onto the code an upload gets, or auto
// when the caller names none.
func batchLanguage(language string) (string, error) {
	base := baseLanguage(language)
	if base == "" || base == batchAutoLanguage {
		return batchAutoLanguage, nil
	}
	return supportedLanguage(base, language)
}

func supportedLanguage(base, original string) (string, error) {
	if _, ok := languages[base]; ok {
		return base, nil
	}
	return "", &runtimepkg.ProviderError{
		Code:    "unsupported_language",
		Message: "Omi omi-medical-1 transcribes en, es, pt, fr, de, nl, ar and hi, got " + strings.TrimSpace(original),
		Hint:    "Route this language to another provider.",
	}
}

// errorDetail is the `error` object of a REST failure, and the shape of a
// socket error frame: {code, message}.
type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// parseError reads the {"error":{…}} envelope, or nil when the body is not one.
func parseError(body []byte) *errorDetail {
	var envelope struct {
		Error *errorDetail `json:"error"`
	}
	if json.Unmarshal(body, &envelope) != nil {
		return nil
	}
	return envelope.Error
}

// classify maps an Omi error code, REST (lower snake) or socket (UPPER
// SNAKE), onto the protocol classification and whether the same request may
// succeed later. An unknown code falls back to the HTTP status. The vendor
// message is read only to tell a 422 for an unsupported language from any
// other invalid request, because Omi gives both the code invalid_request.
func classify(detail *errorDetail, status int) (string, bool) {
	code := ""
	if detail != nil {
		code = strings.ToLower(strings.TrimSpace(detail.Code))
	}
	switch code {
	case "unauthorized", "invalid_api_key", "authentication_failed":
		return "authentication_failed", false
	case "billing_blocked", "insufficient_quota", "quota_exceeded":
		return "provider_quota_exceeded", false
	case "queue_full", "tenant_upload_concurrency_exceeded", "too_many_streams", "speaker_capacity_full", "rate_limited", "rate_limit_exceeded":
		return "provider_rate_limited", true
	case "capacity_unavailable", "service_unavailable":
		return "provider_unavailable", true
	case "audio_too_long", "payload_too_large", "file_too_large":
		return "input_too_large", false
	case "unsupported_audio_format", "unsupported_media_type":
		return "unsupported_media", false
	case "unsupported_language":
		return "unsupported_language", false
	case "invalid_request":
		if status == http.StatusUnprocessableEntity && strings.Contains(strings.ToLower(detail.Message), "unsupported language") {
			return "unsupported_language", false
		}
	}
	switch {
	case status == http.StatusUnauthorized:
		return "authentication_failed", false
	case status == http.StatusPaymentRequired:
		return "provider_quota_exceeded", false
	case status == http.StatusRequestEntityTooLarge:
		return "input_too_large", false
	case status == http.StatusUnsupportedMediaType:
		return "unsupported_media", false
	case status == http.StatusTooManyRequests:
		return "provider_rate_limited", true
	case status == http.StatusRequestTimeout || status == http.StatusGatewayTimeout:
		return "request_timeout", true
	case status >= 300 && status < 400:
		return "provider_error", false
	case status >= 400 && status < 500:
		// 403 is a key without access to this surface or model, which a
		// retry or a fresh key of the same project does not fix.
		return "invalid_request", false
	case status == 0 && code != "":
		// An unknown socket error code: the start frame was refused.
		return "invalid_request", false
	default:
		return "provider_unavailable", true
	}
}

// providerError classifies a failed exchange. The vendor's message stays in
// the extension; only its code reaches the caller-facing text.
func providerError(message string, detail *errorDetail, status int, raw []byte) *runtimepkg.ProviderError {
	canonical, retryable := classify(detail, status)
	providerErr := &runtimepkg.ProviderError{Code: canonical, Message: message, Retryable: retryable, ProviderStatus: status}
	if detail != nil {
		if code := strings.TrimSpace(detail.Code); code != "" && len(code) <= 64 {
			providerErr.Message += " (" + code + ")"
		}
	}
	if status >= 300 && status < 400 {
		providerErr.Message += ": redirects are refused"
	}
	if raw = bytes.TrimSpace(raw); len(raw) > 0 && json.Valid(raw) {
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

// noRedirects returns a copy of client that answers a redirect with the 3xx
// itself. The copy shares the transport.
func noRedirects(client *http.Client) *http.Client {
	if client == nil {
		client = http.DefaultClient
	}
	copied := *client
	copied.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &copied
}
