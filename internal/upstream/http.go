package upstream

import (
	"errors"
	"net/url"
	"strings"
)

// HTTPPolicy is the HTTPS counterpart of WebSocketPolicy: an immutable host
// allowlist that a batch adapter checks before attaching a credential to a
// request. Unlike the WebSocket policy it accepts a path, because REST
// endpoints are path-addressed; it still refuses a preexisting query, user
// info, and fragment so an adapter always owns the parameters it sends.
type HTTPPolicy struct {
	hosts         map[string]struct{}
	suffixes      []string
	allowInsecure bool
}

// NewHTTPPolicy builds a policy for an official provider hostname plus any
// additional exact hosts. A host beginning with "*." is a suffix pattern
// ("*.asr.api.speechmatics.com") for providers whose regional endpoints share
// a documented domain; the wildcard covers exactly one label.
func NewHTTPPolicy(officialHost string, additionalHosts []string, allowInsecure bool) (HTTPPolicy, error) {
	policy := HTTPPolicy{hosts: make(map[string]struct{}, 1+len(additionalHosts)), allowInsecure: allowInsecure}
	for _, host := range append([]string{officialHost}, additionalHosts...) {
		host = strings.ToLower(strings.TrimSpace(host))
		if suffix, ok := strings.CutPrefix(host, "*."); ok {
			if suffix == "" || strings.ContainsAny(suffix, "/:@?#*") {
				return HTTPPolicy{}, errors.New("upstream: allowed endpoint host pattern is invalid")
			}
			policy.suffixes = append(policy.suffixes, "."+suffix)
			continue
		}
		if host == "" || strings.ContainsAny(host, "/:@?#*") {
			return HTTPPolicy{}, errors.New("upstream: allowed endpoint host is invalid")
		}
		policy.hosts[host] = struct{}{}
	}
	return policy, nil
}

// Parse validates the scheme, host, port, user info, query and fragment of a
// REST endpoint before an adapter adds authentication or parameters.
func (p HTTPPolicy) Parse(raw string) (*url.URL, error) {
	endpoint, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.Fragment != "" || endpoint.RawQuery != "" {
		return nil, errors.New("upstream: endpoint must be a clean absolute HTTPS URL")
	}
	if endpoint.Scheme != "https" && !(p.allowInsecure && endpoint.Scheme == "http") {
		return nil, errors.New("upstream: endpoint must use https")
	}
	if !p.allowInsecure && endpoint.Port() != "" && endpoint.Port() != "443" {
		return nil, errors.New("upstream: endpoint uses a non-standard port")
	}
	if !p.allows(strings.ToLower(endpoint.Hostname())) {
		return nil, errors.New("upstream: endpoint host is not allowed")
	}
	return endpoint, nil
}

func (p HTTPPolicy) allows(host string) bool {
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
