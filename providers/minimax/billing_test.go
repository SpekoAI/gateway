package minimax

import (
	"context"
	"testing"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

func TestBillingUsesReportedCharactersAndExplicitZero(t *testing.T) {
	for _, tc := range []struct {
		body     string
		complete bool
		want     int64
	}{
		{`{"event":"task_continued","is_final":true,"extra_info":{"usage_characters":7}}`, true, 7000},
		{`{"event":"task_continued","is_final":true,"extra_info":{"usage_characters":0}}`, true, 0},
		{`{"event":"task_continued","is_final":true,"extra_info":{"usage_characters":null}}`, false, 0},
		{`{"event":"task_continued","is_final":true}`, false, 0},
		{`{"event":"task_continued","is_final":true,"extra_info":{"usage_characters":-1}}`, false, 0},
	} {
		s := &stream{ctx: context.Background(), events: make(chan runtimepkg.ProviderEvent, 10), model: "speech-2.8-hd", billingSequence: 1, taskActive: true}
		if err := s.handleMessage([]byte(tc.body)); err != nil {
			t.Fatal(err)
		}
		if err := s.handleMessage([]byte(`{"event":"task_finished"}`)); err != nil {
			t.Fatal(err)
		}
		var observed *protocol.BillingObservation
		for len(s.events) > 0 {
			event := <-s.events
			if event.Billing != nil {
				observed = event.Billing
			}
		}
		if observed == nil || observed.Complete != tc.complete || observed.Quantities["characters"] != tc.want {
			t.Fatalf("%s: %+v", tc.body, observed)
		}
	}
}
