package speechmatics

import (
	"context"
	"encoding/json"
	"testing"

	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

// end_time is an approximate word end, not a proven boundary, so no
// Speechmatics event carries covered_ms.
func TestNoCoveredMS(t *testing.T) {
	t.Parallel()
	s := &stream{ctx: context.Background(), events: make(chan runtimepkg.ProviderEvent, 4)}
	for _, message := range []map[string]any{
		transcriptFrame("AddPartialTranscript", "hello", 0, 0.8, 0.1),
		transcriptFrame("AddTranscript", "hello world", 0, 1.04, 0.9),
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
	for event := range s.events {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(event.Data, &fields); err != nil {
			t.Fatal(err)
		}
		if _, ok := fields["covered_ms"]; ok {
			t.Fatalf("%s carries covered_ms: %s", event.Type, event.Data)
		}
	}
}
