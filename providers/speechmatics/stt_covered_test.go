package speechmatics

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

// covered_ms on a Speechmatics stream is the end_time of the latest
// AddTranscript: partials never carry it and it never goes backwards.
func TestAddTranscriptCarriesCoveredMS(t *testing.T) {
	t.Parallel()
	s := &stream{ctx: context.Background(), events: make(chan runtimepkg.ProviderEvent, 16)}
	for _, message := range []map[string]any{
		transcriptFrame("AddPartialTranscript", "hello wor", 0, 0.8, 0.1),
		transcriptFrame("AddTranscript", "hello world", 0, 1.04, 0.9),
		transcriptFrame("AddPartialTranscript", "how", 1.5, 1.7, 0.4),
		transcriptFrame("AddTranscript", "how are you", 1.5, 2.3, 0.9),
		// An empty final carries no event and leaves covered_ms where it was.
		transcriptFrame("AddTranscript", "", 2.3, 4.0, 0),
		// A final the vendor reports ending earlier must not pull covered_ms back.
		transcriptFrame("AddTranscript", "you", 2.0, 2.2, 0.9),
		transcriptFrame("AddTranscript", "bye", 4.0, 4.6, 0.9),
	} {
		payload, err := json.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.handleMessage(payload); err != nil {
			t.Fatal(err)
		}
	}
	close(s.events)
	var finals []int64
	for event := range s.events {
		var data struct {
			CoveredMS *int64 `json:"covered_ms"`
		}
		if err := json.Unmarshal(event.Data, &data); err != nil {
			t.Fatal(err)
		}
		switch event.Type {
		case protocol.EventTranscriptFinal:
			if data.CoveredMS == nil {
				t.Fatalf("final without covered_ms: %s", event.Data)
			}
			finals = append(finals, *data.CoveredMS)
		default:
			if data.CoveredMS != nil {
				t.Fatalf("%s carries covered_ms: %s", event.Type, event.Data)
			}
		}
	}
	want := []int64{1040, 2300, 2300, 4600}
	if len(finals) != len(want) {
		t.Fatalf("finals covered_ms = %v, want %v", finals, want)
	}
	for i := range want {
		if finals[i] != want[i] {
			t.Fatalf("finals covered_ms = %v, want %v", finals, want)
		}
	}
}
