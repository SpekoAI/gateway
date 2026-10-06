// Package upstream validates provider endpoints before any customer-owned
// credential is attached to a request.
package upstream

import (
	"errors"
	"net/url"
	"strings"
)

// WebSocketPolicy is an immutable allowlist for one provider adapter.
type WebSocketPolicy struct {
	hosts         map[string]struct{}
	suffixes      []string
	allowInsecure bool
}

// NewWebSocketPolicy builds a policy for an official provider hostname.
// Additional hosts are intended for dedicated provider deployments. A host
// beginning with "*." is a one-label suffix pattern, exactly as in
// NewHTTPPolicy, for providers that serve every customer resource on its own
// subdomain of a documented domain ("*.services.ai.azure.com"). Insecure
// WebSockets are available only as an explicit test/development override.
func NewWebSocketPolicy(officialHost string, additionalHosts []string, allowInsecure bool) (WebSocketPolicy, error) {
	policy := WebSocketPolicy{hosts: make(map[string]struct{}, 1+len(additionalHosts)), allowInsecure: allowInsecure}
	for _, host := range append([]string{officialHost}, additionalHosts...) {
		host = strings.ToLower(strings.TrimSpace(host))
		if suffix, ok := strings.CutPrefix(host, "*."); ok {
			if suffix == "" || strings.ContainsAny(suffix, "/:@?#*") {
				return WebSocketPolicy{}, errors.New("upstream: allowed endpoint host pattern is invalid")
			}
			policy.suffixes = append(policy.suffixes, "."+suffix)
			continue
		}
		if host == "" || strings.ContainsAny(host, "/:@?#*") {
			return WebSocketPolicy{}, errors.New("upstream: allowed endpoint host is invalid")
		}
		policy.hosts[host] = struct{}{}
	}
	return policy, nil
}

// Parse validates the scheme, hostname, port, userinfo, and preexisting query
// before an adapter adds authentication or provider parameters.
func (p WebSocketPolicy) Parse(raw string) (*url.URL, error) {
	endpoint, err := url.Parse(raw)
	if err != nil || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.Fragment != "" || endpoint.RawQuery != "" {
		return nil, errors.New("upstream: endpoint must be a clean absolute WebSocket URL")
	}
	if endpoint.Scheme != "wss" && !(p.allowInsecure && endpoint.Scheme == "ws") {
		return nil, errors.New("upstream: endpoint must use wss")
	}
	if !p.allowInsecure && endpoint.Port() != "" && endpoint.Port() != "443" {
		return nil, errors.New("upstream: endpoint uses a non-standard port")
	}
	if !p.allows(strings.ToLower(endpoint.Hostname())) {
		return nil, errors.New("upstream: endpoint host is not allowed")
	}
	return endpoint, nil
}

func (p WebSocketPolicy) allows(host string) bool {
	if _, ok := p.hosts[host]; ok {
		return true
	}
	for _, suffix := range p.suffixes {
		label, ok := strings.CutSuffix(host, suffix)
		if ok && label != "" && !strings.Contains(label, ".") {
			return true
		}
	}
	return false
}
