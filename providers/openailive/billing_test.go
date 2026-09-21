package openailive

import (
	"context"
	"encoding/json"
	"github.com/SpekoAI/gateway/protocol"
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

func TestBackendModelCanArriveAfterResponseIdentity(t *testing.T) {
	s := &liveStream{ctx: context.Background(), billingModel: "gpt-live-1", events: make(chan runtimepkg.ProviderEvent, 16), usage: Usage{Backend: map[string]BackendUsage{}}}
	var previous protocol.BillingObservation
	for _, raw := range []string{
		`{"type":"response.event","event":{"type":"response.created","response":{"id":"r1"}}}`,
		`{"type":"response.event","event":{"type":"response.completed","response":{"id":"r1","model":"gpt-5.6-luna"}}}`,
		`{"type":"response.event","event":{"type":"response.output_item.done","response_id":"r1"}}`,
	} {
		var event serverEvent
		if err := json.Unmarshal([]byte(raw), &event); err != nil {
			t.Fatal(err)
		}
		s.handle(event, []byte(raw))
		observed := (<-s.events).Billing
		if observed == nil {
			t.Fatal("lost pending operation")
		}
		merged, err := protocol.MergeBillingObservation(previous, *observed)
		if err != nil {
			t.Fatal(err)
		}
		previous = merged
	}
	if previous.Model != "gpt-5.6-luna" || previous.Complete {
		t.Fatalf("%+v", previous)
	}
}
