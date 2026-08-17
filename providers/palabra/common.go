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

func providerFrameError(code, description string, raw []byte) error {
	retryable := code == "SERVICE_UNAVAILABLE" || code == "RATE_LIMIT_EXCEEDED"
	stable := "provider_rejected_request"
	switch code {
	case "UNAUTHORIZED":
		stable = "provider_authentication_failed"
	case "RATE_LIMIT_EXCEEDED":
		stable = "provider_rate_limited"
	case "SERVICE_UNAVAILABLE", "SERVER_ERROR", "UNKNOWN_ERROR":
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
