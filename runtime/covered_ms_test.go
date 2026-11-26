package runtime_test

import (
	"encoding/json"
	"testing"

	"github.com/SpekoAI/gateway/protocol"
	"github.com/SpekoAI/gateway/providers/mock"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

// covered_ms is evidence for the relay's opt-in stream continuation. A
// session on this engine has no way to opt in, so its clients never see it.
func TestSessionDropsCoveredMSFromFinals(t *testing.T) {
	t.Parallel()

	adapter := mock.NewAdapter("mock.covered.stt", func(_ runtimepkg.AdapterRequest) *mock.Stream {
		stream := mock.NewStream(8)
		for _, event := range []runtimepkg.ProviderEvent{
			{Type: protocol.EventTranscriptFinal, Data: json.RawMessage(`{"text":"hello","audio_end_ms":900,"covered_ms":1000}`)},
			{Type: protocol.EventTranscriptFinal, Data: json.RawMessage(`{"text":"again","audio_end_ms":1900}`)},
		} {
			if err := stream.Emit(event); err != nil {
				t.Fatalf("emit provider event: %v", err)
			}
		}
		return stream
	})
	engine := newEngine(t, adapter, runtimepkg.DefaultLimits(), &collectingTelemetry{})
	session := openSession(t, engine, protocol.SessionKindSTT, adapter.ID())
	session.Close()
	var finals []string
	for _, event := range collectEvents(t, session) {
		if event.Type == protocol.EventTranscriptFinal {
			finals = append(finals, string(event.Data))
		}
	}
	want := []string{`{"audio_end_ms":900,"text":"hello"}`, `{"text":"again","audio_end_ms":1900}`}
	if len(finals) != len(want) || finals[0] != want[0] || finals[1] != want[1] {
		t.Fatalf("finals = %v, want %v", finals, want)
	}
}
