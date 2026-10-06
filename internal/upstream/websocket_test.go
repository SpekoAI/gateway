package upstream_test

import (
	"testing"

	"github.com/SpekoAI/gateway/internal/upstream"
)

func TestWebSocketPolicyPinsCleanTLSProviderURLs(t *testing.T) {
	t.Parallel()
	policy, err := upstream.NewWebSocketPolicy("api.provider.test", nil, false)
	if err != nil {
		t.Fatalf("new policy: %v", err)
	}
	if _, err := policy.Parse("wss://api.provider.test/v1/stream"); err != nil {
		t.Fatalf("official endpoint rejected: %v", err)
	}
	for _, raw := range []string{
		"wss://lookalike.test/v1/stream",
		"ws://api.provider.test/v1/stream",
		"wss://api.provider.test:8443/v1/stream",
		"wss://user@api.provider.test/v1/stream",
		"wss://api.provider.test/v1/stream?redirect=lookalike.test",
	} {
		if _, err := policy.Parse(raw); err == nil {
			t.Fatalf("unsafe endpoint accepted: %s", raw)
		}
	}
}

func TestWebSocketPolicyAllowsExplicitDevelopmentEndpoint(t *testing.T) {
	t.Parallel()
	policy, err := upstream.NewWebSocketPolicy("api.provider.test", []string{"127.0.0.1"}, true)
	if err != nil {
		t.Fatalf("new policy: %v", err)
	}
	if _, err := policy.Parse("ws://127.0.0.1:43123/v1/stream"); err != nil {
		t.Fatalf("explicit development endpoint rejected: %v", err)
	}
}

func TestWebSocketPolicySuffixPatternCoversExactlyOneLabel(t *testing.T) {
	t.Parallel()
	policy, err := upstream.NewWebSocketPolicy("*.services.provider.test", nil, false)
	if err != nil {
		t.Fatalf("new policy: %v", err)
	}
	for _, raw := range []string{
		"wss://my-resource.services.provider.test/v1/realtime",
		"wss://MY-RESOURCE.services.provider.test/v1/realtime",
	} {
		if _, err := policy.Parse(raw); err != nil {
			t.Fatalf("%s: %v", raw, err)
		}
	}
	for _, raw := range []string{
		"wss://services.provider.test/v1/realtime",
		"wss://a.b.services.provider.test/v1/realtime",
		"wss://my-resource.services.provider.test.lookalike.test/v1/realtime",
		"wss://my-resourceservices.provider.test/v1/realtime",
	} {
		if _, err := policy.Parse(raw); err == nil {
			t.Fatalf("%s: accepted, want refusal", raw)
		}
	}
}

func TestWebSocketPolicyRefusesMalformedHosts(t *testing.T) {
	t.Parallel()
	for _, host := range []string{"", "api.provider.test/path", "*.", "*.a*b.test", "a*b.test"} {
		if _, err := upstream.NewWebSocketPolicy(host, nil, false); err == nil {
			t.Fatalf("%q: accepted, want refusal", host)
		}
	}
}
