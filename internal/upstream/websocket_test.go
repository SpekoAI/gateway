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
