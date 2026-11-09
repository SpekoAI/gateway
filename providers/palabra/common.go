package palabra

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/SpekoAI/gateway/protocol"
	"github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

const extensionID = "palabra.ai/v1"

type endpointPolicy struct {
	hosts         map[string]struct{}
	allowInsecure bool
}

func newEndpointPolicy(officialHosts, additionalHosts []string, allowInsecure bool) (endpointPolicy, error) {
	hosts := make(map[string]struct{}, len(officialHosts)+len(additionalHosts))
	for _, host := range append(append([]string(nil), officialHosts...), additionalHosts...) {
		host = strings.ToLower(strings.TrimSpace(host))
		if host == "" || strings.ContainsAny(host, "/:@?#") {
			return endpointPolicy{}, errors.New("palabra: allowed endpoint host is invalid")
		}
		hosts[host] = struct{}{}
	}
	return endpointPolicy{hosts: hosts, allowInsecure: allowInsecure}, nil
}

func (p endpointPolicy) parse(raw, path string) (*url.URL, error) {
	endpoint, err := url.Parse(raw)
	if err != nil || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.Fragment != "" || endpoint.RawQuery != "" {
		return nil, errors.New("endpoint must be a clean absolute WebSocket URL")
	}
	if endpoint.Scheme != "wss" && !(p.allowInsecure && endpoint.Scheme == "ws") {
		return nil, errors.New("endpoint must use wss")
	}
	if !p.allowInsecure && endpoint.Port() != "" && endpoint.Port() != "443" {
		return nil, errors.New("endpoint uses a non-standard port")
	}
	if _, ok := p.hosts[strings.ToLower(endpoint.Hostname())]; !ok {
		return nil, errors.New("endpoint host is not allowed")
	}
	if endpoint.Path != path {
		return nil, fmt.Errorf("endpoint path must be %s, got %q", path, endpoint.Path)
	}
	return endpoint, nil
}

func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

func authHeaders(value string) http.Header {
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer "+value)
	return headers
}

// retryRemedy ends the caller-safe Hint on transient failures. Hints are fixed
// sentences plus at most a numeric status or close code, never Palabra's text.
const retryRemedy = "retry after a brief delay or use auto routing."

// streamFailure is the provider_unavailable error for a stream that broke
// before its output was complete. The hint distinguishes Palabra closing the
// socket (with its numeric close code) from the connection dropping.
func streamFailure(message, unfinished string, err error) error {
	hint := "The connection to Palabra dropped before " + unfinished + "; " + retryRemedy
	if code := websocket.CloseStatus(err); code != -1 {
		hint = fmt.Sprintf("Palabra closed the stream with WebSocket code %d before %s; %s", int(code), unfinished, retryRemedy)
	}
	return &runtime.ProviderError{Code: "provider_unavailable", Message: message, Hint: hint, Retryable: true, Cause: err}
}

func dialError(provider string, response *http.Response, err error) error {
	status := 0
	if response != nil {
		status = response.StatusCode
	}
	code := "provider_unavailable"
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		code = "provider_authentication_failed"
	} else if status == http.StatusBadRequest || status == http.StatusConflict || status == http.StatusUnprocessableEntity {
		code = "provider_rejected_request"
	} else if status == http.StatusTooManyRequests {
		code = "provider_rate_limited"
	}
	return &runtime.ProviderError{
		Code: code, Message: provider + " streaming connection could not be established",
		Retryable:      status == 0 || status == http.StatusTooManyRequests || status >= 500,
		ProviderStatus: status, Cause: err,
	}
}

// frameError is the data object of an inbound {"message_type":"error"} frame.
// Streaming codes are uppercase strings with a desc (docs.palabra.ai realtime
// TTS), while session errors carry a numeric code with a reason, such as
// {"code":202103,"reason":"session_revoked"} (docs.palabra.ai/docs/auth/session).
type frameError struct {
	Code        frameErrorCode `json:"code"`
	Description string         `json:"desc"`
	Reason      string         `json:"reason"`
}

// frameErrorCode accepts both spellings and keeps a number as its decimal text.
type frameErrorCode string

func (c *frameErrorCode) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err == nil {
		*c = frameErrorCode(text)
		return nil
	}
	var number json.Number
	if err := json.Unmarshal(data, &number); err != nil {
		return fmt.Errorf("palabra error code must be a string or number: %w", err)
	}
	*c = frameErrorCode(number.String())
	return nil
}

func (e frameError) providerError(raw []byte) error {
	description := e.Description
	if strings.TrimSpace(description) == "" {
		description = e.Reason
	}
	return providerFrameError(string(e.Code), description, raw)
}

func providerFrameError(code, description string, raw []byte) error {
	stable := "provider_rejected_request"
	retryable := false
	switch code {
	case "UNAUTHORIZED",
		// Session tokens that can no longer authorize this stream:
		// connect_window_expired, session_revoked, intent_mismatch,
		// connection_type_mismatch, account_blocked.
		"202101", "202103", "202104", "202105", "400020":
		stable = "provider_authentication_failed"
	case "RATE_LIMIT_EXCEEDED",
		"202106": // session_limit_reached
		stable, retryable = "provider_rate_limited", true
	case "SERVICE_UNAVAILABLE",
		// session_verification_unavailable, billing_unavailable: retry advised.
		"202107", "200030":
		stable, retryable = "provider_unavailable", true
	case "SERVER_ERROR", "UNKNOWN_ERROR":
		stable = "provider_unavailable"
	}
	if strings.TrimSpace(description) == "" {
		description = "Palabra reported a streaming error"
	}
	return &runtime.ProviderError{
		Code: stable, Message: description, Retryable: retryable,
		Extensions: extension(raw),
	}
}

func extension(raw []byte) map[string]json.RawMessage {
	return map[string]json.RawMessage{extensionID: append(json.RawMessage(nil), raw...)}
}

func marshalData(value any) json.RawMessage {
	payload, _ := json.Marshal(value)
	return payload
}

func isNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}
