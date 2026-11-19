package cartesia

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	"github.com/coder/websocket"
)

// A normal multi-utterance session must keep sending Cartesia exactly the
// frames it sent before the adapter learned to survive context expiry and
// idle socket loss. Context ids are random, so they are numbered by first
// appearance before comparison.
func TestAdapterNormalMultiUtteranceSessionSendsUnchangedFrames(t *testing.T) {
	t.Parallel()

	received := make(chan []string, 1)
	server := newTTSServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		var frames []string
		defer func() { received <- frames }()
		// With max_buffer_delay_ms 0 Cartesia answers every transcript frame
		// as it arrives: audio tagged with the frame's flush_id (counted per
		// context from 0), words, then flush_done; the final frame gets only
		// flush_done, then done (measured live).
		frameIndex := map[string]int{}
		for {
			_, payload, err := conn.Read(ctx)
			if err != nil {
				return
			}
			frames = append(frames, string(payload))
			var frame generation
			if err := json.Unmarshal(payload, &frame); err != nil {
				t.Errorf("decode frame: %v", err)
				return
			}
			flushID := frameIndex[frame.ContextID]
			frameIndex[frame.ContextID]++
			var answer []map[string]any
			if frame.Continue {
				answer = append(answer, flushChunk(frame.ContextID, flushID, []byte{1, 2}), wordsFrame(frame.ContextID, strings.Fields(frame.Transcript)...))
			}
			answer = append(answer, flushDoneFrame(frame.ContextID))
			if !frame.Continue {
				answer = append(answer, doneFrame(frame.ContextID))
			}
			if !writeFrames(ctx, t, conn, answer...) {
				return
			}
		}
	})
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	request := adapterRequest(server.URL)
	request.Plan.Route.Model = "sonic-3.5"
	request.Options.TTS = &protocol.TtsOptions{ProviderOptions: map[string]map[string]any{"cartesia": {"max_buffer_delay_ms": 0.0}}}
	providerStream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, utterance := range [][]string{{"Hello, ", "world."}, {"Again."}} {
		want := "audio.started" + strings.Repeat(",audio.frame,alignment,warning", len(utterance)) + ",warning,audio.done"
		for _, text := range utterance {
			if err := providerStream.AppendText(context.Background(), text); err != nil {
				t.Fatalf("append %q: %v", text, err)
			}
		}
		if err := providerStream.CommitText(context.Background()); err != nil {
			t.Fatalf("commit: %v", err)
		}
		events := collectProviderEvents(t, providerStream.Events(), 3+3*len(utterance))
		if got := strings.Join(eventTypes(events), ","); got != want {
			t.Fatalf("utterance events = %s", got)
		}
	}
	if err := providerStream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	const frame = `{"model_id":"sonic-3.5","transcript":%q,"voice":{"mode":"id","id":"voice_123"},"language":"en","max_buffer_delay_ms":0,"context_id":"%s","output_format":{"container":"raw","encoding":"pcm_s16le","sample_rate":16000},"add_timestamps":true,"continue":%t}`
	want := []string{
		fmt.Sprintf(frame, "Hello, ", "ctx-1", true),
		fmt.Sprintf(frame, "world.", "ctx-1", true),
		fmt.Sprintf(frame, "", "ctx-1", false),
		fmt.Sprintf(frame, "Again.", "ctx-2", true),
		fmt.Sprintf(frame, "", "ctx-2", false),
	}
	select {
	case frames := <-received:
		got := numberContextIDs(t, frames)
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Fatalf("vendor frames changed:\n got: %s\nwant: %s", strings.Join(got, "\n      "), strings.Join(want, "\n      "))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not finish")
	}
}

func numberContextIDs(t *testing.T, frames []string) []string {
	t.Helper()
	numbers := map[string]string{}
	out := make([]string, len(frames))
	for index, frame := range frames {
		var decoded generation
		if err := json.Unmarshal([]byte(frame), &decoded); err != nil {
			t.Fatalf("decode %s: %v", frame, err)
		}
		if _, ok := numbers[decoded.ContextID]; !ok {
			numbers[decoded.ContextID] = fmt.Sprintf("ctx-%d", len(numbers)+1)
		}
		out[index] = strings.Replace(frame, decoded.ContextID, numbers[decoded.ContextID], 1)
	}
	return out
}
