package openailive

import (
	"context"
	"encoding/json"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"testing"
)

func TestNativeLiveBillingPresenceAndPendingBackend(t *testing.T) {
	for _, tc := range []struct {
		raw      string
		complete bool
		quantity int64
	}{
		{`{"type":"session.closed","usage":{"seconds":0}}`, true, 0},
		{`{"type":"session.closed","usage":{"seconds":1.234}}`, true, 1234},
		{`{"type":"session.closed","usage":{}}`, false, 0},
		{`{"type":"session.usage.updated","usage":{"seconds":1.234}}`, false, 1234},
		{`{"type":"response.event","event":{"type":"response.completed","response":{"id":"r1","model":"gpt-5.6-luna","usage":{"input_tokens":1,"output_tokens":1}}}}`, false, 0},
	} {
		s := &liveStream{ctx: context.Background(), billingModel: "gpt-live-1", events: make(chan runtimepkg.ProviderEvent, 8), closedEvent: make(chan struct{}), usage: Usage{Backend: map[string]BackendUsage{}}}
		var event serverEvent
		if err := json.Unmarshal([]byte(tc.raw), &event); err != nil {
			t.Fatal(err)
		}
		s.handle(event, []byte(tc.raw))
		forwarded := <-s.events
		if forwarded.Billing == nil || forwarded.Billing.Complete != tc.complete || forwarded.Billing.Quantities["duration_seconds"] != tc.quantity {
			t.Fatalf("%s: %+v", tc.raw, forwarded.Billing)
		}
		var envelope struct {
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(forwarded.Data, &envelope); err != nil || string(envelope.Payload) != tc.raw {
			t.Fatalf("native event changed: %s %v", forwarded.Data, err)
		}
	}
}
