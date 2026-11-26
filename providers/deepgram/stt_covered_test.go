package deepgram

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

// covered_ms on a Listen stream is the end of the chain of is_final windows
// from 0: interim results never carry it, a chunk final (is_final without
// speech_final) moves it like an endpoint does, an empty final extends the
// chain, a window after a hole does not, it rounds down, and it never goes
// backwards.
func TestListenFinalsCarryCoveredMS(t *testing.T) {
	t.Parallel()
	s := &stream{ctx: context.Background(), events: make(chan runtimepkg.ProviderEvent, 16), mono: true}
	for _, raw := range []string{
		`{"type":"Results","start":0,"duration":1.2,"is_final":false,"channel":{"alternatives":[{"transcript":"hello"}]}}`,
		`{"type":"Results","start":0,"duration":2.5,"is_final":true,"speech_final":false,"channel":{"alternatives":[{"transcript":"hello there"}]}}`,
		`{"type":"Results","start":2.5,"duration":0.8,"is_final":false,"channel":{"alternatives":[{"transcript":"how"}]}}`,
		`{"type":"Results","start":2.5,"duration":1.25,"is_final":true,"speech_final":true,"channel":{"alternatives":[{"transcript":"how are you"}]}}`,
		// A window the vendor reports ending earlier must not pull covered_ms back.
		`{"type":"Results","start":3.0,"duration":0.5,"is_final":true,"speech_final":false,"channel":{"alternatives":[{"transcript":"you"}]}}`,
		// An empty final is silence: no event, and covered_ms waits for the next final.
		`{"type":"Results","start":3.75,"duration":3.0,"is_final":true,"channel":{"alternatives":[{"transcript":""}]}}`,
		`{"type":"Results","start":6.75,"duration":1.0,"is_final":true,"channel":{"alternatives":[{"transcript":"bye"}]}}`,
		// A window after a hole proves nothing about the hole.
		`{"type":"Results","start":9.0,"duration":1.0,"is_final":true,"channel":{"alternatives":[{"transcript":"later"}]}}`,
		// 40 ms past the chain's end is a hole, not float error.
		`{"type":"Results","start":7.79,"duration":0.5006,"is_final":true,"channel":{"alternatives":[{"transcript":"on"}]}}`,
		// Float error at the joint (under 1 ms) still joins; the end is rounded down.
		`{"type":"Results","start":7.7504,"duration":0.5006,"is_final":true,"channel":{"alternatives":[{"transcript":"so"}]}}`,
	} {
		var message inboundMessage
		if err := json.Unmarshal([]byte(raw), &message); err != nil {
			t.Fatal(err)
		}
		if err := s.emitResults(message, json.RawMessage(raw)); err != nil {
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
		case protocol.EventTranscriptDelta, protocol.EventSpeechEnded:
			if data.CoveredMS != nil {
				t.Fatalf("%s carries covered_ms %d", event.Type, *data.CoveredMS)
			}
		case protocol.EventTranscriptFinal:
			if data.CoveredMS == nil {
				t.Fatalf("final without covered_ms: %s", event.Data)
			}
			finals = append(finals, *data.CoveredMS)
		}
	}
	want := []int64{2500, 3750, 3750, 7750, 7750, 7750, 8251}
	if len(finals) != len(want) {
		t.Fatalf("finals covered_ms = %v, want %v", finals, want)
	}
	for i := range want {
		if finals[i] != want[i] {
			t.Fatalf("finals covered_ms = %v, want %v", finals, want)
		}
	}
}

// Deepgram finalizes each channel on its own, so a multichannel stream and
// Flux (whose turns are not Listen windows) never carry covered_ms.
func TestCoveredMSOnlyOnMonoListen(t *testing.T) {
	t.Parallel()
	raw := `{"type":"Results","start":0,"duration":1,"is_final":true,"speech_final":true,"channel":{"alternatives":[{"transcript":"hi"}]}}`
	var message inboundMessage
	if err := json.Unmarshal([]byte(raw), &message); err != nil {
		t.Fatal(err)
	}
	s := &stream{ctx: context.Background(), events: make(chan runtimepkg.ProviderEvent, 4), mono: false}
	if err := s.emitResults(message, json.RawMessage(raw)); err != nil {
		t.Fatal(err)
	}
	if event := <-s.events; hasCoveredMS(t, event.Data) {
		t.Fatalf("multichannel final carries covered_ms: %s", event.Data)
	}

	flux := &stream{ctx: context.Background(), events: make(chan runtimepkg.ProviderEvent, 4), flux: true, mono: true, extension: fluxExtensionID}
	turn := `{"type":"TurnInfo","event":"EndOfTurn","transcript":"hi","audio_window_start":0,"audio_window_end":1}`
	if err := flux.handleMessage([]byte(turn)); err != nil {
		t.Fatal(err)
	}
	close(flux.events)
	for event := range flux.events {
		if hasCoveredMS(t, event.Data) {
			t.Fatalf("flux %s carries covered_ms: %s", event.Type, event.Data)
		}
	}
}

func hasCoveredMS(t *testing.T, data json.RawMessage) bool {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	_, ok := fields["covered_ms"]
	return ok
}
