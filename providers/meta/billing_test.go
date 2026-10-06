package meta

import (
	"context"
	"encoding/json"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
	"testing"
)

func TestProcessedAudioBilling(t *testing.T) {
	for _, tc := range []struct {
		name, duration string
		want           int64
		complete       bool
	}{
		{"fractional", "1999", 1999, true}, {"silence", "0", 0, true},
		{"missing", "", 0, false}, {"null", "null", 0, false}, {"negative", "-1", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeRealtime(t)
			final := `{"type":"audioProgress"`
			if tc.duration != "" {
				final += `,"audioProcessedMs":` + tc.duration
			}
			final += `}`
			fake.closingReplies = []string{final, final}
			stream, ctx := openRealtime(t, fake, realtimeRequest(fake.endpoint()))
			if err := stream.Close(ctx); err != nil {
				t.Fatal(err)
			}
			var last *protocol.BillingObservation
			for event := range stream.Events() {
				if event.Err != nil {
					t.Fatal(event.Err)
				}
				if event.Billing != nil {
					last = event.Billing
				}
			}
			if last == nil || last.Complete != tc.complete || last.Quantities["duration_seconds"] != tc.want || last.ProviderRequestID != "sess-1" {
				t.Fatalf("%+v", last)
			}
		})
	}
}

func TestInterruptedAudioKeepsPartialBilling(t *testing.T) {
	s := &sttStream{ctx: context.Background(), events: make(chan runtimepkg.ProviderEvent, 8), setupDone: make(chan error, 1), billing: protocol.BillingObservation{OperationID: "session", Model: DefaultModel, Mode: "streaming", Quantities: map[string]int64{}}}
	raw := []byte(`{"type":"audioProgress","audioProcessedMs":30400}`)
	var message serverMessage
	_ = json.Unmarshal(raw, &message)
	s.handle(message, raw)
	s.finish(websocket.CloseError{Code: websocket.StatusInternalError})
	first := <-s.events
	if first.Billing == nil || first.Billing.Complete || first.Billing.Quantities["duration_seconds"] != 30400 {
		t.Fatalf("%+v", first)
	}
	final := <-s.events
	if final.Billing == nil || final.Billing.Complete || final.Billing.Quantities["duration_seconds"] != 30400 {
		t.Fatalf("%+v", final)
	}
}
