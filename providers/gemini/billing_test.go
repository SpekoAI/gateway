package gemini

import (
	"context"
	"github.com/SpekoAI/gateway/protocol"
	"io"
	"net/http"
	"testing"
)

func TestTTSBillsProviderTokensAndKeepsCumulativeReports(t *testing.T) {
	for _, streaming := range []bool{true, false} {
		stream, cleanup := newTTSFixture(t, func(w http.ResponseWriter, r *http.Request) {
			body := `{"responseId":"response-1","candidates":[{"content":{"parts":[{"inlineData":{"data":"AQI="}}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":7,"candidatesTokenCount":12}}`
			if streaming {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: "+body+"\n\n")
				_, _ = io.WriteString(w, `data: {"responseId":"response-1","usageMetadata":{"promptTokenCount":7,"candidatesTokenCount":12}}`+"\n\n")
			} else {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, body)
			}
		})
		input := "hello"
		if !streaming {
			for len(input) <= ttsStreamMaxChars {
				input += " hello"
			}
		}
		if err := stream.AppendText(context.Background(), input); err != nil {
			t.Fatal(err)
		}
		if err := stream.CommitText(context.Background()); err != nil {
			t.Fatal(err)
		}
		for {
			e := ttsEventWithin(t, stream.Events())
			if e.Type != protocol.EventAudioDone {
				continue
			}
			got := e.Billing
			if got == nil || !got.Complete || got.Quantities["input_tokens"] != 7000 || got.Quantities["output_audio_tokens"] != 12000 || got.ProviderResponseID != "response-1" {
				t.Fatalf("%+v", got)
			}
			break
		}
		cleanup()
	}
}
