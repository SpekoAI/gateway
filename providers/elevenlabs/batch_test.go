package elevenlabs

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/SpekoAI/gateway/internal/batchhttp"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

func batchPlan(endpoint string) protocol.SessionPlan {
	return protocol.SessionPlan{
		Execution: protocol.Execution{ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged},
		Route: protocol.PlanRoute{
			Provider: "elevenlabs", Model: BatchModel, Endpoint: endpoint, Transport: protocol.TransportHTTP,
			Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "xi-key"},
		},
	}
}

func TestBatchTranscribeUsesScribeFileContract(t *testing.T) {
	t.Parallel()
	diarize := true
	type observed struct {
		header http.Header
		form   map[string][]string
		file   string
	}
	seen := make(chan observed, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Errorf("multipart: %v", err)
		}
		file, header, err := r.FormFile("file")
		var content []byte
		if err == nil {
			content, _ = io.ReadAll(file)
			if header.Filename != "audio.wav" || header.Header.Get("Content-Type") != "audio/wav" {
				t.Errorf("file header = %v", header.Header)
			}
		}
		seen <- observed{header: r.Header.Clone(), form: r.MultipartForm.Value, file: string(content)}
		w.Header().Set("request-id", "rid-1")
		_, _ = w.Write([]byte(`{"language_code":"en","language_probability":0.99,"text":"Hello there. Hi.","transcription_id":"tx_1","audio_duration_secs":1823.17,"words":[{"text":"Hello","type":"word","start":0.1,"end":0.4,"speaker_id":"speaker_0"},{"text":" ","type":"spacing","start":0.4,"end":0.45},{"text":"there.","type":"word","start":0.45,"end":0.8,"speaker_id":"speaker_0"},{"text":"(laughs)","type":"audio_event","start":0.8,"end":0.9},{"text":"Hi.","type":"word","start":1.0,"end":1.2,"speaker_id":"speaker_1"}]}`))
	}))
	defer server.Close()

	adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	result, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
		Plan:       batchPlan(server.URL + "/v1/speech-to-text"),
		Options:    protocol.RequestOptions{Language: "en-GB", STT: &protocol.SttOptions{Diarization: &diarize, Keywords: []string{"Speko", "Router"}, ProviderOptions: map[string]map[string]any{"elevenlabs": {"num_speakers": 2}}}},
		Audio:      strings.NewReader("RIFF....WAVE"),
		AudioBytes: 12,
	})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	got := <-seen
	if got.header.Get("xi-api-key") != "xi-key" {
		t.Fatalf("xi-api-key = %q", got.header.Get("xi-api-key"))
	}
	if got.file != "RIFF....WAVE" {
		t.Fatalf("file = %q", got.file)
	}
	for key, want := range map[string]string{"model_id": BatchModel, "diarize": "true", "timestamps_granularity": "word", "tag_audio_events": "false", "language_code": "en", "num_speakers": "2"} {
		if values := got.form[key]; len(values) != 1 || values[0] != want {
			t.Fatalf("form %s = %v, want %q", key, values, want)
		}
	}
	if strings.Join(got.form["keyterms"], ",") != "Speko,Router" {
		t.Fatalf("keyterms = %v", got.form["keyterms"])
	}
	if result.Text != "Hello there. Hi." || result.Language != "en" || result.DurationMS != 1823170 || result.ProviderRequestID != "tx_1" {
		t.Fatalf("result = %+v", result)
	}
	if len(result.Segments) != 2 || result.Segments[0] != (runtimepkg.BatchSegment{Text: "Hello there.", StartMS: 100, EndMS: 800, Speaker: "speaker_0"}) || result.Segments[1].Speaker != "speaker_1" {
		t.Fatalf("segments = %+v", result.Segments)
	}
	// Batch Scribe bills apart from realtime, so the observation names the
	// batch model and mode, and Scribe's own processed duration.
	want := protocol.BillingReport{Revision: protocol.BillingRevision, Complete: true, Operations: []protocol.BillingObservation{{
		OperationID: "request", Model: BatchModel, Mode: "batch", ProviderRequestID: "tx_1",
		Quantities: map[string]int64{"duration_seconds": 1_823_170}, Complete: true,
	}}}
	if result.Billing == nil || !reflect.DeepEqual(*result.Billing, want) {
		t.Fatalf("billing = %+v, want %+v", result.Billing, want)
	}
	if err := result.Billing.Validate(); err != nil {
		t.Fatalf("billing invalid: %v", err)
	}
}

// Without audio_duration_secs there is no vendor quantity, and the uploaded
// bytes are not substituted for it.
func TestBatchBillingStaysIncompleteWithoutTheVendorDuration(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("request-id", "rid-2")
		_, _ = w.Write([]byte(`{"language_code":"en","text":"Hi.","words":[]}`))
	}))
	defer server.Close()
	adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	result, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(server.URL + "/v1/speech-to-text"), Audio: strings.NewReader("RIFF....WAVE"), AudioBytes: 12})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	want := protocol.BillingReport{Revision: protocol.BillingRevision, Operations: []protocol.BillingObservation{{
		OperationID: "request", Model: BatchModel, Mode: "batch", ProviderRequestID: "rid-2", Quantities: map[string]int64{},
	}}}
	if result.Billing == nil || !reflect.DeepEqual(*result.Billing, want) {
		t.Fatalf("billing = %+v, want %+v", result.Billing, want)
	}
}

func TestBatchTranscribeRefusals(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"detail":{"status":"invalid_model_id"}}`))
	}))
	defer server.Close()
	adapter, _ := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	base := runtimepkg.BatchTranscribeRequest{Plan: batchPlan(server.URL + "/v1/speech-to-text"), Audio: strings.NewReader("x"), AudioBytes: 1}

	_, err := adapter.Transcribe(context.Background(), base)
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeInvalidRequest || providerErr.Retryable {
		t.Fatalf("422: %v", err)
	}
	realtime := base
	realtime.Plan.Route.Model = "scribe_v2_realtime"
	if _, err := adapter.Transcribe(context.Background(), realtime); err == nil {
		t.Fatal("realtime model accepted")
	}
	huge := base
	huge.AudioBytes = BatchMaxAudioBytes + 1
	if _, err := adapter.Transcribe(context.Background(), huge); !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeInputTooLarge {
		t.Fatalf("oversized: %v", err)
	}
}

func TestBatchTranscribeAliasesNorwegianToScribeCode(t *testing.T) {
	t.Parallel()
	seen := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Errorf("multipart: %v", err)
		}
		seen <- r.FormValue("language_code")
		_, _ = w.Write([]byte(`{"language_code":"nor","text":"Hei.","transcription_id":"tx_1","audio_duration_secs":1}`))
	}))
	defer server.Close()

	adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	if _, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
		Plan:       batchPlan(server.URL + "/v1/speech-to-text"),
		Options:    protocol.RequestOptions{Language: "nb-NO"},
		Audio:      strings.NewReader("RIFF....WAVE"),
		AudioBytes: 12,
	}); err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	if got := <-seen; got != "nor" {
		t.Fatalf("language_code = %q, want nor", got)
	}
}
