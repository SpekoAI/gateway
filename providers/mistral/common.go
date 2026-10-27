package mistral

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// ProviderName is the only provider this package serves.
	ProviderName = "mistral"

	officialHost = "api.mistral.ai"

	// extensionID namespaces raw vendor payloads attached to canonical events.
	extensionID = "api.mistral.ai/v1"
)

// languages are the 58 ISO-639-1 codes both transcription models list under
// audio_transcription_capabilities.languages (GET /v1/models/<id>,
// 2026-10-21). The socket refuses anything else with an error frame and the
// upload with a 400, so a tag outside the set is refused locally.
var languages = map[string]struct{}{
	"ar": {}, "bg": {}, "bn": {}, "ca": {}, "cs": {}, "da": {}, "de": {}, "el": {}, "en": {}, "es": {},
	"et": {}, "eu": {}, "fa": {}, "fi": {}, "fr": {}, "ga": {}, "gu": {}, "hi": {}, "hr": {}, "hu": {},
	"id": {}, "is": {}, "it": {}, "ja": {}, "km": {}, "kn": {}, "ko": {}, "lb": {}, "lt": {}, "lv": {},
	"mk": {}, "ml": {}, "mr": {}, "ms": {}, "mt": {}, "my": {}, "nl": {}, "no": {}, "or": {}, "pa": {},
	"pl": {}, "ps": {}, "pt": {}, "ro": {}, "ru": {}, "sk": {}, "sl": {}, "sr": {}, "sv": {}, "ta": {},
	"te": {}, "th": {}, "tl": {}, "tr": {}, "uk": {}, "ur": {}, "vi": {}, "zh": {},
}

// languageAliases folds the spellings callers send onto the listed code.
var languageAliases = map[string]string{
	"fil": "tl",
	"nb":  "no",
	"nn":  "no",
	"in":  "id", // Indonesian, pre-1989 code
	"cmn": "zh",
}

// acceptableCredentialKind accepts relay_access on the relay route, where the
// plan labels the connector's permanent key that way; the key travels in the
// same bearer header either way.
func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

// errorDetail is the REST error body: {"object":"error", message, type,
// param, code, raw_status_code}. message is a string or, on a 422, the
// validation detail object; code is a string on REST ("1902") and a number
// in a socket error frame (3051). Every field is kept raw so that no
// spelling of one can make the whole body unreadable. A refused key answers
// {"detail":"Invalid API Key"} instead, which leaves every field empty and
// classifies from the status.
type errorDetail struct {
	Message       json.RawMessage `json:"message"`
	Type          json.RawMessage `json:"type"`
	Code          json.RawMessage `json:"code"`
	RawStatusCode json.RawMessage `json:"raw_status_code"`
}

// parseError reads an error body, or nil when the body is not a JSON object.
func parseError(body []byte) *errorDetail {
	var detail errorDetail
	if json.Unmarshal(body, &detail) != nil {
		return nil
	}
	return &detail
}

// scalar reads a JSON string or number as text, or "" for anything else.
func scalar(raw json.RawMessage) string {
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return strings.TrimSpace(text)
	}
	var number json.Number
	if json.Unmarshal(raw, &number) == nil {
		return number.String()
	}
	return ""
}

func (d *errorDetail) code() string {
	if d == nil {
		return ""
	}
	return scalar(d.Code)
}

// kind is the error's type, lowercased.
func (d *errorDetail) kind() string {
	if d == nil {
		return ""
	}
	return strings.ToLower(scalar(d.Type))
}

// status is raw_status_code, a number or a numeric string, or 0.
func (d *errorDetail) status() int {
	if d == nil {
		return 0
	}
	status, err := strconv.Atoi(scalar(d.RawStatusCode))
	if err != nil {
		return 0
	}
	return status
}

// text lowercases the error's message, string or object, for the keyword
// checks below. It never reaches a caller-facing message.
func (d *errorDetail) text() string {
	if d == nil {
		return ""
	}
	var message string
	if json.Unmarshal(d.Message, &message) == nil {
		return strings.ToLower(message)
	}
	return strings.ToLower(string(d.Message))
}

// classify maps a failed exchange onto the protocol classification and
// whether the same request may succeed later. Mistral documents no error
// code table: 1902 (invalid_voice) and 3051 (validation) are the codes seen
// live. The message's quota and moderation wording leads, then the vendor
// type, then the status. A body that arrives without an HTTP status of its
// own (a socket or event-stream error) uses the raw_status_code it carries,
// when it carries one.
func classify(detail *errorDetail, status int) (string, bool) {
	if status == 0 {
		status = detail.status()
	}
	errorType, message := detail.kind(), detail.text()
	switch {
	case errorType == "invalid_voice" || detail.code() == "1902":
		return "invalid_request", false
	case strings.Contains(message, "quota") || strings.Contains(message, "credit") || strings.Contains(message, "billing") || strings.Contains(message, "payment"):
		return "provider_quota_exceeded", false
	case strings.Contains(message, "moderation"):
		return "invalid_request", false
	case strings.HasPrefix(errorType, "invalid_request"):
		return "invalid_request", false
	case strings.Contains(errorType, "authentication") || strings.Contains(errorType, "unauthorized") || strings.Contains(errorType, "permission"):
		return "authentication_failed", false
	case strings.Contains(errorType, "rate_limit"):
		return "provider_rate_limited", true
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
	case status == http.StatusRequestTimeout || status == http.StatusGatewayTimeout:
		return "request_timeout", true
	case status >= 300 && status < 500:
		return "invalid_request", false
	case status == 0 && detail.code() == "3051":
		return "invalid_request", false
	case status == 0 && (strings.Contains(message, "api key") || strings.Contains(message, "unauthorized")):
		return "authentication_failed", false
	case status == 0 && strings.Contains(message, "rate limit"):
		return "provider_rate_limited", true
	default:
		return "provider_unavailable", true
	}
}

// providerError classifies a failed exchange. The vendor's message stays in
// the extension; only its type or code reaches the caller-facing text.
func providerError(message string, detail *errorDetail, status int, raw []byte) *runtimepkg.ProviderError {
	canonical, retryable := classify(detail, status)
	providerErr := &runtimepkg.ProviderError{Code: canonical, Message: message, Retryable: retryable, ProviderStatus: status}
	if label := detail.kind(); label != "" {
		providerErr.Message += " (" + label + ")"
	} else if code := detail.code(); code != "" {
		providerErr.Message += " (" + code + ")"
	}
	if json.Valid(raw) {
		providerErr.Extensions = extension(raw)
	}
	return providerErr
}

// sttLanguage reduces a BCP-47 tag to the primary subtag both models take,
// or "" to omit the field and let the model detect the language. A tag
// outside the listed set is refused: sent, it fails the session.
func sttLanguage(language string) (string, error) {
	primary := strings.ToLower(strings.TrimSpace(language))
	if primary == "" || primary == "auto" {
		return "", nil
	}
	if index := strings.IndexAny(primary, "-_"); index > 0 {
		primary = primary[:index]
	}
	if folded, ok := languageAliases[primary]; ok {
		primary = folded
	}
	if _, ok := languages[primary]; !ok {
		return "", &runtimepkg.ProviderError{
			Code:    "unsupported_language",
			Message: "Mistral Voxtral does not transcribe " + strings.TrimSpace(language),
			Hint:    "Route this language to another provider, or omit language to let Voxtral detect it.",
		}
	}
	return primary, nil
}

func extension(raw []byte) map[string]json.RawMessage {
	return map[string]json.RawMessage{extensionID: append(json.RawMessage(nil), raw...)}
}

func marshalData(value any) json.RawMessage {
	payload, _ := json.Marshal(value)
	return payload
}
