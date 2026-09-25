package openai

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

// TestRealtimeTranscriptionBillsThePerItemUsage: the Realtime socket reports
// usage per COMPLETED ITEM, not per session, and the two arms of its usage
// union are the vendor's own statement of the model's billing basis. The
// duration-priced models must produce duration_seconds and the token-priced
// ones must produce the two token counts; neither may be derived from the
// audio the caller sent.
func TestRealtimeTranscriptionBillsThePerItemUsage(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		model      string
		usage      string
		quantities map[string]int64
		complete   bool
	}{
		{
			name:       "per-minute model reports duration",
			model:      "gpt-live-transcribe",
			usage:      `,"usage":{"type":"duration","seconds":12.5}`,
			quantities: map[string]int64{"duration_seconds": 12_500},
			complete:   true,
		},
		{
			name:       "per-token model reports tokens",
			model:      "gpt-4o-transcribe",
			usage:      `,"usage":{"type":"tokens","input_tokens":14,"output_tokens":45,"total_tokens":59,"input_token_details":{"text_tokens":0,"audio_tokens":14}}`,
			quantities: map[string]int64{"input_tokens": 14_000, "output_tokens": 45_000},
			complete:   true,
		},
		{
			// The charge is withheld rather than converted. A per-minute model
			// that started reporting tokens would need a token rate nobody has
			// published, so an unresolved obligation is the only honest result.
			name:       "per-minute model reporting tokens stays incomplete",
			model:      "gpt-transcribe",
			usage:      `,"usage":{"type":"tokens","input_tokens":14,"output_tokens":45,"total_tokens":59}`,
			quantities: map[string]int64{},
		},
		{
			name:       "per-token model reporting a duration stays incomplete",
			model:      "gpt-4o-transcribe",
			usage:      `,"usage":{"type":"duration","seconds":12.5}`,
			quantities: map[string]int64{},
		},
		{
			name:       "no usage at all stays incomplete",
			model:      "gpt-live-transcribe",
			usage:      "",
			quantities: map[string]int64{},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			stream, cancel := newHandlerOnlySTTStream()
			defer cancel()
			stream.model = testCase.model
			stream.sessionID = "sess_openai_1"

			frame := `{"type":"conversation.item.input_audio_transcription.completed","event_id":"e1","item_id":"item_003","transcript":"Hello"` + testCase.usage + `}`
			if err := stream.handleMessage([]byte(frame)); err != nil {
				t.Fatalf("handle completed item: %v", err)
			}
			event := <-stream.events
			if event.Type != protocol.EventTranscriptFinal {
				t.Fatalf("event type = %q", event.Type)
			}
			observation := event.Billing
			if observation == nil {
				t.Fatal("a completed transcription item carries no billing observation")
			}
			if observation.OperationID != "item/item_003" {
				t.Errorf("operation id = %q, want item/item_003 — one operation per billed item", observation.OperationID)
			}
			if observation.Model != testCase.model || observation.Mode != "streaming" {
				t.Errorf("identity = %q/%q", observation.Model, observation.Mode)
			}
			if observation.ProviderRequestID != "sess_openai_1" {
				t.Errorf("provider request id = %q", observation.ProviderRequestID)
			}
			if observation.Complete != testCase.complete {
				t.Errorf("complete = %v, want %v", observation.Complete, testCase.complete)
			}
			if !reflect.DeepEqual(observation.Quantities, testCase.quantities) {
				t.Errorf("quantities = %v, want %v", observation.Quantities, testCase.quantities)
			}
			if testCase.complete {
				if err := observation.Validate(); err != nil {
					t.Errorf("observation does not validate: %v", err)
				}
			}
		})
	}
}

// TestRealtimeTranscriptionGivesEveryItemItsOwnOperation: settlement sums
// operations and refuses to sum two snapshots of the SAME operation, so two
// utterances on one socket must not share an id.
func TestRealtimeTranscriptionGivesEveryItemItsOwnOperation(t *testing.T) {
	t.Parallel()

	stream, cancel := newHandlerOnlySTTStream()
	defer cancel()
	stream.model = "gpt-live-transcribe"

	for _, item := range []string{"item_a", "item_b"} {
		frame := `{"type":"conversation.item.input_audio_transcription.completed","item_id":"` + item + `","transcript":"x","usage":{"type":"duration","seconds":1}}`
		if err := stream.handleMessage([]byte(frame)); err != nil {
			t.Fatalf("handle %s: %v", item, err)
		}
	}
	if first, second := billedOperationID(t, <-stream.events), billedOperationID(t, <-stream.events); first == second {
		t.Fatalf("both items billed as %q", first)
	}

	// An item with no id still has to be distinguishable.
	stream.billedItems = 0
	for range 2 {
		if err := stream.handleMessage([]byte(`{"type":"conversation.item.input_audio_transcription.completed","transcript":"x","usage":{"type":"duration","seconds":1}}`)); err != nil {
			t.Fatalf("handle anonymous item: %v", err)
		}
	}
	if third, fourth := billedOperationID(t, <-stream.events), billedOperationID(t, <-stream.events); third == fourth {
		t.Fatalf("both anonymous items billed as %q", third)
	}
}

func billedOperationID(t *testing.T, event runtimepkg.ProviderEvent) string {
	t.Helper()
	if event.Billing == nil {
		t.Fatalf("%q event carries no billing observation", event.Type)
	}
	return event.Billing.OperationID
}

// TestBatchTranscriptionBillsTheVendorUsage covers the file endpoint's half of
// the same union, including the executed-model split: gpt-realtime-whisper's
// prerecorded twin runs whisper-1 at a DIFFERENT published rate, so the
// observation has to name whisper-1.
func TestBatchTranscriptionBillsTheVendorUsage(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		model      string
		body       string
		quantities map[string]int64
		complete   bool
	}{
		{
			name:       "whisper-1 duration usage",
			model:      "whisper-1",
			body:       `{"text":"Hello","duration":1823.17,"usage":{"type":"duration","seconds":1823}}`,
			quantities: map[string]int64{"duration_seconds": 1_823_000},
			complete:   true,
		},
		{
			// verbose_json states the processed duration at the top level. It is
			// provider evidence for the same quantity, so it is the fallback
			// when the response carries no usage object at all.
			name:       "whisper-1 with no usage object falls back to the reported duration",
			model:      "whisper-1",
			body:       `{"text":"Hello","duration":1823.17}`,
			quantities: map[string]int64{"duration_seconds": 1_823_170},
			complete:   true,
		},
		{
			name:       "gpt-4o-transcribe token usage",
			model:      "gpt-4o-transcribe",
			body:       `{"text":"Hello","usage":{"type":"tokens","input_tokens":14,"output_tokens":45,"total_tokens":59}}`,
			quantities: map[string]int64{"input_tokens": 14_000, "output_tokens": 45_000},
			complete:   true,
		},
		{
			name:       "gpt-transcribe duration usage",
			model:      "gpt-transcribe",
			body:       `{"text":"Hello","usage":{"type":"duration","seconds":42}}`,
			quantities: map[string]int64{"duration_seconds": 42_000},
			complete:   true,
		},
		{
			// A per-token model with no usage has no fallback: `duration` is not
			// the quantity its price is quoted against.
			name:       "gpt-4o-transcribe with no usage stays incomplete",
			model:      "gpt-4o-transcribe",
			body:       `{"text":"Hello","duration":1823.17}`,
			quantities: map[string]int64{},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("x-request-id", "req_1")
				_, _ = w.Write([]byte(testCase.body))
			}))
			defer server.Close()

			adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
			if err != nil {
				t.Fatalf("adapter: %v", err)
			}
			result, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
				Plan:       batchPlan(server.URL+"/v1/audio/transcriptions", testCase.model),
				Audio:      strings.NewReader("RIFF....WAVE"),
				AudioBytes: 12,
			})
			if err != nil {
				t.Fatalf("transcribe: %v", err)
			}
			if result.Billing == nil || len(result.Billing.Operations) != 1 {
				t.Fatalf("billing report = %+v", result.Billing)
			}
			observation := result.Billing.Operations[0]
			if observation.Model != testCase.model || observation.Mode != "batch" {
				t.Errorf("identity = %q/%q, want %q/batch", observation.Model, observation.Mode, testCase.model)
			}
			if observation.Complete != testCase.complete || result.Billing.Complete != testCase.complete {
				t.Errorf("complete = %v/%v, want %v", observation.Complete, result.Billing.Complete, testCase.complete)
			}
			if !reflect.DeepEqual(observation.Quantities, testCase.quantities) {
				t.Errorf("quantities = %v, want %v", observation.Quantities, testCase.quantities)
			}
			if testCase.complete && observation.ProviderRequestID != "req_1" {
				t.Errorf("provider request id = %q", observation.ProviderRequestID)
			}
			if err := result.Billing.Validate(); err != nil {
				t.Errorf("report does not validate: %v", err)
			}
		})
	}
}
