package runtime

import (
	"encoding/json"
	"errors"
	"maps"
	"slices"

	"github.com/coder/websocket"
)

// maxProviderCodeBytes bounds one provider error code.
const maxProviderCodeBytes = 64

// SanitizeProviderCode admits s as a provider's machine-readable error code:
// 1 to 64 bytes of ASCII letters, digits, '_', '.', ':' and '-'. Anything else
// returns "": a message (it has spaces), a URL, JSON, control characters, or
// non-ASCII text. The check is on shape only. A credential can be letters and
// digits too, so a consumer that persists the code (a log line, a metric)
// admits it against its own list of known codes as well.
func SanitizeProviderCode(s string) string {
	if s == "" || len(s) > maxProviderCodeBytes {
		return ""
	}
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '.', c == ':', c == '-':
		default:
			return ""
		}
	}
	return s
}

// SafeProviderCode is the provider's own machine-readable error code
// (voice_not_found, insufficient_quota, RESOURCE_EXHAUSTED, ...), so an
// operator can see why a provider failed beyond the closed Code. It is, in
// order: the adapter's ProviderCode; the code the raw error payload in
// Extensions names (see providerCodeFromPayload); or the reason of a
// WebSocket close frame in Cause. Every answer passes SanitizeProviderCode,
// so it is one token, never a message or a payload. "" means the provider
// named no code.
func (e *ProviderError) SafeProviderCode() string {
	if e == nil {
		return ""
	}
	if code := SanitizeProviderCode(e.ProviderCode); code != "" {
		return code
	}
	for _, key := range slices.Sorted(maps.Keys(e.Extensions)) {
		if code := providerCodeFromPayload(e.Extensions[key]); code != "" {
			return code
		}
	}
	// A close reason is usually a sentence, which the sanitizer refuses; some
	// providers close with a bare code instead.
	var closeErr websocket.CloseError
	if errors.As(e.Cause, &closeErr) {
		return SanitizeProviderCode(closeErr.Reason)
	}
	return ""
}

// providerCodeFromPayload reads the error envelopes providers use, most
// specific first, and returns the first string field that sanitizes:
//
//  1. an "error" object's code, status, or type: OpenAI and the
//     OpenAI-compatible hosts ({"error":{"code":"insufficient_quota",
//     "type":"insufficient_quota"}}, code null on some errors), Anthropic's
//     error.type, and Google's {"error":{"code":429,"status":
//     "RESOURCE_EXHAUSTED"}}, whose numeric code is skipped for its status;
//  2. a "detail" object's status, code, or type: ElevenLabs. Its docs call
//     status legacy, but live responses still send it and it is the more
//     specific of the two ({"code":"bad_request","status":
//     "voice_limit_reached"});
//  3. a top-level error_code, err_code (Deepgram), code (DashScope), or an
//     "error" string (ElevenLabs' WebSocket frames: {"error":
//     "quota_exceeded","code":1008}, where code is the close status).
//
// Numbers are never read: they are HTTP or close statuses, which
// ProviderStatus already carries. A top-level "type" is not read either: in
// most streaming protocols it is the frame's event name ("error", "done",
// "audio"), and an adapter whose vendor names its code there sets
// ProviderCode instead.
func providerCodeFromPayload(raw json.RawMessage) string {
	var payload map[string]json.RawMessage
	if json.Unmarshal(raw, &payload) != nil {
		return ""
	}
	if code := envelopeCode(payload["error"], "code", "status", "type"); code != "" {
		return code
	}
	if code := envelopeCode(payload["detail"], "status", "code", "type"); code != "" {
		return code
	}
	return firstCode(payload, "error_code", "err_code", "code", "error")
}

func envelopeCode(raw json.RawMessage, fields ...string) string {
	var envelope map[string]json.RawMessage
	if json.Unmarshal(raw, &envelope) != nil {
		return ""
	}
	return firstCode(envelope, fields...)
}

func firstCode(object map[string]json.RawMessage, fields ...string) string {
	for _, field := range fields {
		var value string
		if json.Unmarshal(object[field], &value) != nil {
			continue
		}
		if code := SanitizeProviderCode(value); code != "" {
			return code
		}
	}
	return ""
}
