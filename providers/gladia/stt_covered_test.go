package gladia

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

// covered_ms on a Gladia stream is the end of the latest final utterance:
// partials never carry it and it never goes backwards.
func TestFinalUtterancesCarryCoveredMS(t *testing.T) {
	t.Parallel()
	s := &stream{ctx: context.Background(), events: make(chan runtimepkg.ProviderEvent, 16), seenSessionIDs: map[string]struct{}{}, mono: true}
	for _, message := range []map[string]any{
		frame("transcript", map[string]any{"id": "u1", "is_final": false, "utterance": map[string]any{"text": " hi", "start": 0.2, "end": 0.6, "channel": 0}}),
		frame("transcript", map[string]any{"id": "u1", "is_final": true, "utterance": map[string]any{"text": " hi there", "start": 0.2, "end": 1.1, "channel": 0}}),
		frame("transcript", map[string]any{"id": "u2", "is_final": false, "utterance": map[string]any{"text": " how", "start": 1.8, "end": 2.1, "channel": 0}}),
		frame("transcript", map[string]any{"id": "u2", "is_final": true, "utterance": map[string]any{"text": " how are you", "start": 1.8, "end": 2.75, "channel": 0}}),
		// A final the vendor reports ending earlier must not pull covered_ms back.
		frame("transcript", map[string]any{"id": "u3", "is_final": true, "utterance": map[string]any{"text": " you", "start": 2.0, "end": 2.5, "channel": 0}}),
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
	var finals []int64
	for event := range s.events {
		covered, ok := coveredMS(t, event.Data)
		switch event.Type {
		case protocol.EventTranscriptFinal:
			if !ok {
				t.Fatalf("final without covered_ms: %s", event.Data)
			}
			finals = append(finals, covered)
		default:
			if ok {
				t.Fatalf("%s carries covered_ms: %s", event.Type, event.Data)
			}
		}
	}
	want := []int64{1100, 2750, 2750}
	if len(finals) != len(want) || finals[0] != want[0] || finals[1] != want[1] || finals[2] != want[2] {
		t.Fatalf("finals covered_ms = %v, want %v", finals, want)
	}
}

// Gladia finalizes each channel on its own, so a multichannel final never
// carries covered_ms.
func TestCoveredMSOnlyOnMono(t *testing.T) {
	t.Parallel()
	s := &stream{ctx: context.Background(), events: make(chan runtimepkg.ProviderEvent, 4), seenSessionIDs: map[string]struct{}{}}
	payload, err := json.Marshal(frame("transcript", map[string]any{"id": "u1", "is_final": true, "utterance": map[string]any{"text": " hi", "start": 0, "end": 1, "channel": 1}}))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.handleMessage(payload); err != nil {
		t.Fatal(err)
	}
	close(s.events)
	for event := range s.events {
		if _, ok := coveredMS(t, event.Data); ok {
			t.Fatalf("multichannel %s carries covered_ms: %s", event.Type, event.Data)
		}
	}
}

func coveredMS(t *testing.T, data json.RawMessage) (int64, bool) {
	t.Helper()
	var fields struct {
		CoveredMS *int64 `json:"covered_ms"`
	}
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	if fields.CoveredMS == nil {
		return 0, false
	}
	return *fields.CoveredMS, true
}
