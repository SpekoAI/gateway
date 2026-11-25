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

// The usage block is the one measured live on 2026-11-25 (7.9 s clip). Input
// audio and output text are the two dimensions Google invoices; anything else
// that is not provably zero leaves the observation incomplete.
func TestBatchBillingReportsAudioInputAndTextOutputTokens(t *testing.T) {
	const measured = `{"total_tokens":228,"total_input_tokens":199,"input_tokens_by_modality":[{"modality":"text","tokens":1},{"modality":"audio","tokens":198}],"total_cached_tokens":0,"total_output_tokens":29,"total_tool_use_tokens":0,"total_thought_tokens":0,"initial_input_tokens":199}`
	o := batchBilling(BatchModel, "interaction-1", []byte(measured))
	if !o.Complete || o.Model != BatchModel || o.Mode != "batch" || o.ProviderRequestID != "interaction-1" {
		t.Fatalf("observation = %+v", o)
	}
	if len(o.Quantities) != 2 || o.Quantities["input_audio_tokens"] != 198_000 || o.Quantities["output_tokens"] != 29_000 {
		t.Fatalf("quantities = %v, want 198 audio input and 29 output tokens in millis", o.Quantities)
	}

	silence := `{"total_input_tokens":751,"input_tokens_by_modality":[{"modality":"audio","tokens":750}],"total_output_tokens":0}`
	if o := batchBilling(BatchModel, "i", []byte(silence)); !o.Complete || o.Quantities["output_tokens"] != 0 || o.Quantities["input_audio_tokens"] != 750_000 {
		t.Fatalf("silence observation = %+v", o)
	}

	for name, usage := range map[string]string{
		"no usage":           ``,
		"no audio modality":  `{"input_tokens_by_modality":[{"modality":"text","tokens":1}],"total_output_tokens":3}`,
		"no output count":    `{"input_tokens_by_modality":[{"modality":"audio","tokens":10}]}`,
		"null output":        `{"input_tokens_by_modality":[{"modality":"audio","tokens":10}],"total_output_tokens":null}`,
		"two audio entries":  `{"input_tokens_by_modality":[{"modality":"audio","tokens":10},{"modality":"audio","tokens":5}],"total_output_tokens":3}`,
		"thinking billed":    `{"input_tokens_by_modality":[{"modality":"audio","tokens":10}],"total_output_tokens":3,"total_thought_tokens":7}`,
		"cache read":         `{"input_tokens_by_modality":[{"modality":"audio","tokens":10}],"total_output_tokens":3,"total_cached_tokens":4}`,
		"malformed thinking": `{"input_tokens_by_modality":[{"modality":"audio","tokens":10}],"total_output_tokens":3,"total_thought_tokens":"x"}`,
	} {
		if o := batchBilling(BatchModel, "i", []byte(usage)); o.Complete {
			t.Fatalf("%s: observation complete = %+v", name, o)
		}
	}
}
