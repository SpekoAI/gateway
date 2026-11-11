package elevenlabs

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

func elevenlabsOptions(settings map[string]any) *protocol.SttOptions {
	return &protocol.SttOptions{ProviderOptions: map[string]map[string]any{"elevenlabs": settings}}
}

func TestScribeV3BatchFormCarriesItsOptions(t *testing.T) {
	t.Parallel()
	forms := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		forms <- orderedForm(t, r)
		_, _ = w.Write([]byte(`{"language_code":"en","text":"Call 42.","transcription_id":"tx_3","audio_duration_secs":2.5,"words":[]}`))
	}))
	defer server.Close()
	adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	plan := batchPlan(server.URL + "/v1/speech-to-text")
	plan.Route.Model = ModelScribeV3
	result, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
		Plan: plan,
		Options: protocol.RequestOptions{Language: "en", STT: elevenlabsOptions(map[string]any{
			"numbers_format": "digits", "secondary_language_codes": "es, FR,nb-NO", "no_verbatim": true,
		})},
		Audio:      strings.NewReader("RIFF....WAVE"),
		AudioBytes: 12,
	})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	// One secondary_language_codes field per language: the vendor rejects a
	// JSON array sent as a single string.
	const want = "model_id=scribe_v3|diarize=false|timestamps_granularity=word|tag_audio_events=false|language_code=en|no_verbatim=true|numbers_format=digits|secondary_language_codes=es|secondary_language_codes=fr|secondary_language_codes=nor|file=RIFF....WAVE"
	if got := <-forms; got != want {
		t.Fatalf("scribe_v3 form\n got %s\nwant %s", got, want)
	}
	if len(result.Billing.Operations) != 1 || result.Billing.Operations[0].Model != ModelScribeV3 || result.Billing.Operations[0].Mode != "batch" {
		t.Fatalf("billing = %+v, want the pinned scribe_v3 in batch mode", result.Billing)
	}
}

// The vendor answers a scribe_v3 parameter on another model with
// invalid_parameters, after the upload. Each one is refused before it.
func TestScribeBatchRefusesUnsupportedOptionsBeforeUploading(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		model    string
		settings map[string]any
	}{
		{name: "numbers_format on v2", model: BatchModel, settings: map[string]any{"numbers_format": "digits"}},
		{name: "secondary_language_codes on v2", model: BatchModel, settings: map[string]any{"secondary_language_codes": "es"}},
		{name: "numbers_format on an unlisted model", model: "scribe_v2_medical", settings: map[string]any{"numbers_format": "words"}},
		{name: "a live option on v3", model: ModelScribeV3, settings: map[string]any{"multilang_mode": true}},
		{name: "an unknown numbers_format", model: ModelScribeV3, settings: map[string]any{"numbers_format": "roman"}},
		{name: "a language name instead of a code", model: ModelScribeV3, settings: map[string]any{"secondary_language_codes": "es,spanish"}},
		{name: "an empty language list", model: ModelScribeV3, settings: map[string]any{"secondary_language_codes": ""}},
		{name: "native diarize on v2", model: BatchModel, settings: map[string]any{"diarize": true}},
		{name: "a second model id", model: BatchModel, settings: map[string]any{"model_id": ModelScribeV3}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var uploads atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { uploads.Add(1) }))
			defer server.Close()
			adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
			if err != nil {
				t.Fatalf("adapter: %v", err)
			}
			plan := batchPlan(server.URL + "/v1/speech-to-text")
			plan.Route.Model = test.model
			_, err = adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
				Plan: plan, Options: protocol.RequestOptions{STT: elevenlabsOptions(test.settings)},
				Audio: strings.NewReader("RIFF....WAVE"), AudioBytes: 12,
			})
			var providerErr *runtimepkg.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Code != "invalid_request" || providerErr.Retryable {
				t.Fatalf("transcribe = %v, want a non-retryable invalid_request", err)
			}
			if uploads.Load() != 0 {
				t.Fatal("the adapter uploaded with an option the model does not support")
			}
		})
	}
}
