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
		{`{"event":"task_continued","trace_id":"chunk-1","is_final":true,"extra_info":{"usage_characters":7}}`, true, 7000},
		{`{"event":"task_continued","trace_id":"chunk-1","is_final":true,"extra_info":{"usage_characters":0}}`, true, 0},
		{`{"event":"task_continued","trace_id":"chunk-1","is_final":true,"extra_info":{"usage_characters":null}}`, false, 0},
		{`{"event":"task_continued","trace_id":"chunk-1","is_final":true}`, false, 0},
		{`{"event":"task_continued","trace_id":"chunk-1","is_final":true,"extra_info":{"usage_characters":-1}}`, false, 0},
	} {
		s := &stream{ctx: context.Background(), events: make(chan runtimepkg.ProviderEvent, 10), model: "speech-2.8-hd", billingSequence: 1, billingSubmitted: 1, taskActive: true}
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

func TestBillingMultipleChunksDeduplicatesAndRetainsPartialEvidence(t *testing.T) {
	s := &stream{ctx: context.Background(), events: make(chan runtimepkg.ProviderEvent, 32), model: "speech-2.8-hd", billingSequence: 1, billingSubmitted: 2, taskActive: true}
	for _, body := range []string{
		`{"event":"task_continued","trace_id":"one","is_final":true,"extra_info":{"usage_characters":7}}`,
		`{"event":"task_continued","trace_id":"one","is_final":true,"extra_info":{"usage_characters":7}}`,
	} {
		if err := s.handleMessage([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	if got := s.billingObservation(true); got.Complete || got.Quantities["characters"] != 7000 {
		t.Fatalf("missing second chunk: %+v", got)
	}
	if err := s.handleMessage([]byte(`{"event":"task_continued","trace_id":"two","is_final":true,"extra_info":{"usage_characters":3}}`)); err != nil {
		t.Fatal(err)
	}
	if got := s.billingObservation(false); got.Complete || got.Quantities["characters"] != 10000 {
		t.Fatalf("partial task: %+v", got)
	}
	if got := s.billingObservation(true); !got.Complete || got.Quantities["characters"] != 10000 {
		t.Fatalf("finished task: %+v", got)
	}
	if err := s.handleMessage([]byte(`{"event":"task_failed","trace_id":"two","extra_info":{"usage_characters":3}}`)); err == nil {
		t.Fatal("expected provider failure")
	}
	var partial *protocol.BillingObservation
	for len(s.events) > 0 {
		if e := <-s.events; e.Billing != nil {
			partial = e.Billing
		}
	}
	if partial == nil || partial.Complete || partial.Quantities["characters"] != 10000 {
		t.Fatalf("lost failure evidence: %+v", partial)
	}
}

func TestBillingConflictingOrUnidentifiedChunksStayUnresolved(t *testing.T) {
	for _, body := range []string{
		`{"event":"task_continued","is_final":true,"extra_info":{"usage_characters":7}}`,
		`{"event":"task_continued","trace_id":"one","is_final":true,"extra_info":{"usage_characters":9}}`,
	} {
		s := &stream{ctx: context.Background(), events: make(chan runtimepkg.ProviderEvent, 16), model: "speech-2.8-hd", billingSequence: 1, billingSubmitted: 1}
		if err := s.handleMessage([]byte(`{"event":"task_continued","trace_id":"one","is_final":true,"extra_info":{"usage_characters":7}}`)); err != nil {
			t.Fatal(err)
		}
		if err := s.handleMessage([]byte(body)); err != nil {
			t.Fatal(err)
		}
		if got := s.billingObservation(true); got.Complete || got.Quantities["characters"] != 7000 {
			t.Fatalf("conflicting evidence: %+v", got)
		}
	}
}
