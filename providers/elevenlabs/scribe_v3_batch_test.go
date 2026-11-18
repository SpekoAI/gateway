package elevenlabs

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
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
	const want = "model_id=scribe_v3|diarize=false|timestamps_granularity=word|tag_audio_events=false|language_code=en|no_verbatim=true|numbers_format=digits|secondary_language_codes=spa|secondary_language_codes=fra|secondary_language_codes=nor|file=RIFF....WAVE"
	if got := <-forms; got != want {
		t.Fatalf("scribe_v3 form\n got %s\nwant %s", got, want)
	}
	if len(result.Billing.Operations) != 1 || result.Billing.Operations[0].Model != ModelScribeV3 || result.Billing.Operations[0].Mode != "batch" {
		t.Fatalf("billing = %+v, want the pinned scribe_v3 in batch mode", result.Billing)
	}
}

// Scribe skips a secondary language it does not recognize without an error,
// so a code outside its documented languages is refused by name before the
// upload, and an accepted one reaches the vendor as the documented ISO 639-3
// code.
func TestScribeV3SecondaryLanguageCodes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		codes string
		// want is the secondary_language_codes fields the vendor receives.
		want []string
		// refused is the code a refusal names.
		refused string
	}{
		{name: "two ISO 639-1 codes", codes: "es,fr", want: []string{"spa", "fra"}},
		{name: "an ISO 639-3 code", codes: "nor", want: []string{"nor"}},
		{name: "regional tags and a Norwegian alias", codes: " ES-mx ,nb-NO", want: []string{"spa", "nor"}},
		{name: "an unknown two-letter code", codes: "zz", refused: "zz"},
		{name: "an unknown code beside a known one", codes: "es,zz", refused: "zz"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fields := make(chan []string, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := r.ParseMultipartForm(1 << 20); err != nil {
					t.Errorf("multipart: %v", err)
				}
				fields <- r.MultipartForm.Value["secondary_language_codes"]
				_, _ = w.Write([]byte(`{"language_code":"en","text":"Hola.","audio_duration_secs":1,"words":[]}`))
			}))
			defer server.Close()
			adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
			if err != nil {
				t.Fatalf("adapter: %v", err)
			}
			plan := batchPlan(server.URL + "/v1/speech-to-text")
			plan.Route.Model = ModelScribeV3
			_, err = adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
				Plan: plan, Options: protocol.RequestOptions{STT: elevenlabsOptions(map[string]any{"secondary_language_codes": test.codes})},
				Audio: strings.NewReader("RIFF....WAVE"), AudioBytes: 12,
			})
			if test.refused != "" {
				var providerErr *runtimepkg.ProviderError
				if !errors.As(err, &providerErr) || providerErr.Code != "invalid_request" || !strings.Contains(providerErr.Message, `"`+test.refused+`"`) {
					t.Fatalf("transcribe = %v, want an invalid_request naming %q", err, test.refused)
				}
				if len(fields) != 0 {
					t.Fatal("the adapter uploaded a secondary language Scribe would skip")
				}
				return
			}
			if err != nil {
				t.Fatalf("transcribe: %v", err)
			}
			if got := <-fields; !reflect.DeepEqual(got, test.want) {
				t.Fatalf("secondary_language_codes = %q, want %q", got, test.want)
			}
		})
	}
}

// An accepted ISO 639-1 code reaches the vendor as a language Scribe
// documents, never as a code it would skip.
func TestScribeISO6391CodesNameDocumentedLanguages(t *testing.T) {
	t.Parallel()
	for short, long := range scribeISO6391 {
		if len(short) != 2 || !slices.Contains(scribeLanguages, long) {
			t.Errorf("%q maps to %q, which is not on Scribe's documented 639-3 list", short, long)
		}
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
