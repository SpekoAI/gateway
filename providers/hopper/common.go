package hopper

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// ProviderName is the only provider this package serves.
	ProviderName = "hopper"

	officialHost = "api.withhopper.com"

	// extensionID namespaces raw vendor payloads attached to canonical events.
	extensionID = "api.withhopper.com/v1"
)

// sampleRates are the rates Hopper documents for both transcription surfaces.
var sampleRates = map[int]struct{}{8_000: {}, 16_000: {}, 22_050: {}, 24_000: {}, 44_100: {}, 48_000: {}}

// acceptableCredentialKind accepts relay_access on the relay route, where the
// plan labels the connector's permanent key that way; the key travels in the
// same bearer header either way.
func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

// errorDetail is the `error` object of every REST failure and of a refused
// socket handshake: {message, type, code, param}.
type errorDetail struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code"`
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

// classify maps a documented Hopper error code onto the protocol
// classification and whether the same request may succeed later. An unknown
// code falls back to the HTTP status.
func classify(code string, status int) (string, bool) {
	switch code {
	case "invalid_api_key", "api_key_disabled":
		return "authentication_failed", false
	case "insufficient_quota":
		return "provider_quota_exceeded", false
	case "rate_limit_exceeded":
		return "provider_rate_limited", true
	case "body_too_large", "transcript_too_long":
		return "input_too_large", false
	case "unsupported_audio_format", "invalid_sample_rate":
		return "unsupported_media", false
	case "invalid_json", "invalid_content_type", "missing_file", "missing_model", "invalid_turn_detection",
		"invalid_transcript", "invalid_output_format", "unsupported_parameter", "model_not_found",
		"voice_not_found", "voice_read_only", "method_not_allowed":
		return "invalid_request", false
	case "upstream_timeout":
		return "request_timeout", true
	case "upstream_unavailable", "upstream_error", "model_offline", "verify_unavailable":
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

// providerError classifies a failed exchange. The vendor's message stays in
// the extension; only its code reaches the caller-facing text.
func providerError(message string, detail *errorDetail, status int, raw []byte) *runtimepkg.ProviderError {
	code := ""
	if detail != nil {
		code = strings.TrimSpace(detail.Code)
	}
	canonical, retryable := classify(code, status)
	providerErr := &runtimepkg.ProviderError{Code: canonical, Message: message, Retryable: retryable, ProviderStatus: status}
	if code != "" {
		providerErr.Message += " (" + code + ")"
	}
	if json.Valid(raw) {
		providerErr.Extensions = extension(raw)
	}
	return providerErr
}

// baseLanguage lowercases a BCP-47 tag and strips everything after the
// primary subtag: en-US and en_GB both become en.
func baseLanguage(language string) string {
	base := strings.ToLower(strings.TrimSpace(language))
	if index := strings.IndexAny(base, "-_"); index > 0 {
		base = base[:index]
	}
	return base
}

// sttLanguage refuses anything but English. nemotron-asr transcribes English
// only and takes no language parameter, so the tag is checked, never sent.
func sttLanguage(language string) (string, error) {
	switch base := baseLanguage(language); base {
	case "", "en":
		return "en", nil
	}
	return "", &runtimepkg.ProviderError{
		Code:    "unsupported_language",
		Message: "Hopper nemotron-asr transcribes English only, got " + strings.TrimSpace(language),
		Hint:    "Route non-English audio to another provider, or send language en.",
	}
}

// word is one timed word as both transcription surfaces send it. conf is
// reserved and always 0, so it is not read. A word without both timings is
// dropped rather than placed at zero.
type word struct {
	Text  string   `json:"text"`
	Start *float64 `json:"start"`
	End   *float64 `json:"end"`
}

// timed returns the word's trimmed text and timings, or false when it has no
// text or lacks a timing. Word text may start with a space.
func (w word) timed() (string, float64, float64, bool) {
	text := strings.TrimSpace(w.Text)
	if text == "" || w.Start == nil || w.End == nil {
		return "", 0, 0, false
	}
	return text, *w.Start, *w.End, true
}

func extension(raw []byte) map[string]json.RawMessage {
	return map[string]json.RawMessage{extensionID: append(json.RawMessage(nil), raw...)}
}

func marshalData(value any) json.RawMessage {
	payload, _ := json.Marshal(value)
	return payload
}
