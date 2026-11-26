package gladia

import (
	"context"
	"encoding/json"
	"testing"

	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

// An utterance end is no proven bound (a forced cut can split a word), so no
// Gladia event carries covered_ms.
func TestNoCoveredMS(t *testing.T) {
	t.Parallel()
	s := &stream{ctx: context.Background(), events: make(chan runtimepkg.ProviderEvent, 8), seenSessionIDs: map[string]struct{}{}}
	for _, message := range []map[string]any{
		frame("transcript", map[string]any{"id": "u1", "is_final": false, "utterance": map[string]any{"text": " hi", "start": 0.2, "end": 0.6, "channel": 0}}),
		frame("transcript", map[string]any{"id": "u1", "is_final": true, "utterance": map[string]any{"text": " hi there", "start": 0.2, "end": 1.1, "channel": 0}}),
		frame("transcript", map[string]any{"id": "u2", "is_final": true, "utterance": map[string]any{"text": " and then", "start": 1.1, "end": 6.1, "channel": 0, "forced": "max_duration"}}),
	} {
		payload, err := json.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.handleMessage(payload); err != nil {
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
