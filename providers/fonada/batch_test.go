package fonada

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/SpekoAI/gateway/internal/batchhttp"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

func batchPlan(endpoint string) protocol.SessionPlan {
	return protocol.SessionPlan{
		Execution: protocol.Execution{ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged},
		Route: protocol.PlanRoute{
			Provider: ProviderName, Model: BatchModel, Endpoint: endpoint, Transport: protocol.TransportHTTP,
			Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "fonada-key"},
		},
	}
}

func batchServer(t *testing.T, status int, body string, inspect func(*http.Request)) (*BatchAdapter, string, *atomic.Int32) {
	t.Helper()
	calls := &atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if inspect != nil {
			inspect(r)
		} else {
			_, _ = io.Copy(io.Discard, r.Body)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	return adapter, server.URL + batchPath, calls
}

func request(endpoint string, options protocol.RequestOptions) runtimepkg.BatchTranscribeRequest {
	return runtimepkg.BatchTranscribeRequest{
		Plan: batchPlan(endpoint), Options: options,
		Media: protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16000, Channels: 1},
		Audio: strings.NewReader("RIFF....WAVE"), AudioBytes: 12,
	}
}

// documentedResponse is the example response on the Kapture v3 docs page.
const documentedResponse = `{"job_id":"3d02014b-6ddf-45e3-94bc-8e6f69da6834",` +
	`"text":"Sir नहीं ऐसा कुछ होता तो मैं बताता है ऐसा कुछ नहीं है sir.","language":"hi",` +
	`"audio_duration_s":3.12,"num_chunks":1,"chunk_durations_s":[3.12],` +
	`"chunk_texts":["Sir नहीं ऐसा कुछ होता तो मैं बताता है ऐसा कुछ नहीं है sir."],` +
	`"transcript_chunks":[{"idx":0,"text":"Sir नहीं ऐसा कुछ होता तो मैं बताता है ऐसा कुछ नहीं है sir.","duration_s":3.12,"start_s":0.0,"end_s":3.12}],` +
	`"bucket":null,"storage_prefix":null,"processing_time_ms":1731.9,"credits_used":9}`

func TestBatchTranscribeUsesKaptureContract(t *testing.T) {
	t.Parallel()
	adapter, endpoint, _ := batchServer(t, http.StatusOK, documentedResponse, func(r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != batchPath {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer fonada-key" {
			t.Errorf("authorization = %q", got)
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Errorf("multipart: %v", err)
			return
		}
		if _, header, err := r.FormFile("file"); err != nil || header.Filename != "audio.wav" {
			t.Errorf("file part: %v %v", header, err)
		}
		if values := r.MultipartForm.Value["language_id"]; len(values) != 1 || values[0] != "hi" {
			t.Errorf("language_id = %v, want hi", values)
		}
		if len(r.MultipartForm.Value) != 1 {
			t.Errorf("form fields = %v, want language_id only", r.MultipartForm.Value)
		}
	})
	result, err := adapter.Transcribe(context.Background(), request(endpoint, protocol.RequestOptions{Language: "hi-IN"}))
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	want := "Sir नहीं ऐसा कुछ होता तो मैं बताता है ऐसा कुछ नहीं है sir."
	if result.Text != want {
		t.Fatalf("text = %q", result.Text)
	}
	if len(result.Segments) != 1 || result.Segments[0].Text != want || result.Segments[0].StartMS != 0 || result.Segments[0].EndMS != 3120 {
		t.Fatalf("segments = %+v", result.Segments)
	}
	if result.Language != "hi" || result.DurationMS != 3120 || result.ProviderRequestID != "3d02014b-6ddf-45e3-94bc-8e6f69da6834" {
		t.Fatalf("result = %+v", result)
	}
	if result.Words != nil {
		t.Fatalf("words = %+v, want none", result.Words)
	}
	if len(result.Extensions) == 0 {
		t.Fatal("raw response extension is missing")
	}
}

func TestBatchBillingReportsProcessedDuration(t *testing.T) {
	t.Parallel()
	adapter, endpoint, _ := batchServer(t, http.StatusOK, documentedResponse, nil)
	result, err := adapter.Transcribe(context.Background(), request(endpoint, protocol.RequestOptions{Language: "hi"}))
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	if result.Billing == nil || !result.Billing.Complete || len(result.Billing.Operations) != 1 {
		t.Fatalf("billing = %+v", result.Billing)
	}
	operation := result.Billing.Operations[0]
	if operation.Model != BatchModel || operation.Mode != "batch" || operation.ProviderRequestID != "3d02014b-6ddf-45e3-94bc-8e6f69da6834" {
		t.Fatalf("operation = %+v", operation)
	}
	if got := operation.Quantities["duration_seconds"]; got != 3120 {
		t.Fatalf("duration_seconds = %d, want 3120 (milliseconds at scale 1000)", got)
	}
}

func TestBatchSilenceIsAnEmptyTranscript(t *testing.T) {
	t.Parallel()
	adapter, endpoint, _ := batchServer(t, http.StatusOK, `{"job_id":"j","text":"","language":"en","audio_duration_s":2.0,"transcript_chunks":[{"idx":0,"text":"","start_s":0,"end_s":2}]}`, nil)
	result, err := adapter.Transcribe(context.Background(), request(endpoint, protocol.RequestOptions{Language: "en"}))
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	if result.Text != "" || len(result.Segments) != 0 || !result.Billing.Complete {
		t.Fatalf("result = %+v", result)
	}
}

func TestBatchTextFallsBackToChunks(t *testing.T) {
	t.Parallel()
	adapter, endpoint, _ := batchServer(t, http.StatusOK, `{"job_id":"j","text":"","language":"hi-IN","audio_duration_s":2.0,"transcript_chunks":[{"idx":0,"text":" नमस्ते ","start_s":0,"end_s":1},{"idx":1,"text":"दुनिया","start_s":1,"end_s":2}]}`, nil)
	result, err := adapter.Transcribe(context.Background(), request(endpoint, protocol.RequestOptions{Language: "hi"}))
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	if result.Text != "नमस्ते दुनिया" || len(result.Segments) != 2 || result.Language != "hi" {
		t.Fatalf("result = %+v", result)
	}
}

func TestBatchAcceptsStereo(t *testing.T) {
	t.Parallel()
	adapter, endpoint, calls := batchServer(t, http.StatusOK, documentedResponse, nil)
	req := request(endpoint, protocol.RequestOptions{Language: "hi"})
	req.Media.Channels = 2
	if _, err := adapter.Transcribe(context.Background(), req); err != nil || calls.Load() != 1 {
		t.Fatalf("stereo: err = %v, calls = %d", err, calls.Load())
	}
}

func TestBatchRefusesBeforeUpload(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		mutate func(*runtimepkg.BatchTranscribeRequest)
		code   string
	}{
		"no language":           {func(r *runtimepkg.BatchTranscribeRequest) { r.Options.Language = "" }, "unsupported_language"},
		"auto language":         {func(r *runtimepkg.BatchTranscribeRequest) { r.Options.Language = "auto" }, "unsupported_language"},
		"undocumented language": {func(r *runtimepkg.BatchTranscribeRequest) { r.Options.Language = "th-TH" }, "unsupported_language"},
		"too large":             {func(r *runtimepkg.BatchTranscribeRequest) { r.AudioBytes = BatchMaxAudioBytes + 1 }, batchhttp.CodeInputTooLarge},
		"float samples":         {func(r *runtimepkg.BatchTranscribeRequest) { r.Media.Encoding = "pcm_f32le" }, batchhttp.CodeUnsupportedMedia},
		"low sample rate":       {func(r *runtimepkg.BatchTranscribeRequest) { r.Media.SampleRateHz = 6000 }, batchhttp.CodeUnsupportedMedia},
		"three channels":        {func(r *runtimepkg.BatchTranscribeRequest) { r.Media.Channels = 3 }, batchhttp.CodeUnsupportedMedia},
		"odia":                  {func(r *runtimepkg.BatchTranscribeRequest) { r.Options.Language = "or" }, "unsupported_language"},
		"diarization": {func(r *runtimepkg.BatchTranscribeRequest) {
			diarize := true
			r.Options.STT = &protocol.SttOptions{Diarization: &diarize}
		}, batchhttp.CodeInvalidRequest},
		"word timestamps": {func(r *runtimepkg.BatchTranscribeRequest) {
			words := true
			r.Options.STT = &protocol.SttOptions{WordTimestamps: &words}
		}, batchhttp.CodeInvalidRequest},
		"keywords": {func(r *runtimepkg.BatchTranscribeRequest) {
			r.Options.STT = &protocol.SttOptions{Keywords: []string{"Speko"}}
		}, batchhttp.CodeInvalidRequest},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			adapter, endpoint, calls := batchServer(t, http.StatusOK, documentedResponse, nil)
			req := request(endpoint, protocol.RequestOptions{Language: "hi"})
			tc.mutate(&req)
			_, err := adapter.Transcribe(context.Background(), req)
			var providerErr *runtimepkg.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Code != tc.code {
				t.Fatalf("err = %v, want code %s", err, tc.code)
			}
			if calls.Load() != 0 {
				t.Fatalf("the upload was sent %d times, want none", calls.Load())
			}
		})
	}
}

func TestBatchMapsStatus(t *testing.T) {
	t.Parallel()
	cases := map[int]string{
		http.StatusBadRequest:            batchhttp.CodeInvalidRequest,
		http.StatusUnauthorized:          batchhttp.CodeAuthenticationFailed,
		http.StatusTooManyRequests:       batchhttp.CodeRateLimited,
		http.StatusServiceUnavailable:    batchhttp.CodeUnavailable,
		http.StatusRequestEntityTooLarge: batchhttp.CodeInputTooLarge,
	}
	for status, code := range cases {
		adapter, endpoint, _ := batchServer(t, status, `{"detail":"unsupported_language"}`, nil)
		_, err := adapter.Transcribe(context.Background(), request(endpoint, protocol.RequestOptions{Language: "hi"}))
		var providerErr *runtimepkg.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != code {
			t.Fatalf("status %d: err = %v, want %s", status, err, code)
		}
	}
}

func TestBatchRefusesMalformedSuccess(t *testing.T) {
	t.Parallel()
	for name, body := range map[string]string{
		"no duration":     `{"text":"hello","transcript_chunks":[{"text":"hello","start_s":0,"end_s":1}]}`,
		"text no chunks":  `{"text":"hello","audio_duration_s":1.0}`,
		"chunk no timing": `{"text":"hello","audio_duration_s":1.0,"transcript_chunks":[{"text":"hello"}]}`,
		"negative start":  `{"text":"hello","audio_duration_s":1.0,"transcript_chunks":[{"text":"hello","start_s":-1,"end_s":1}]}`,
		"no transcript":   `{"audio_duration_s":2.0}`,
		"not json":        `<html>`,
	} {
		adapter, endpoint, _ := batchServer(t, http.StatusOK, body, nil)
		_, err := adapter.Transcribe(context.Background(), request(endpoint, protocol.RequestOptions{Language: "en"}))
		var providerErr *runtimepkg.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeProviderError {
			t.Fatalf("%s: err = %v, want provider_error", name, err)
		}
	}
}

func TestBatchRefusesWrongRoute(t *testing.T) {
	t.Parallel()
	adapter, endpoint, calls := batchServer(t, http.StatusOK, documentedResponse, nil)
	wrongModel := request(endpoint, protocol.RequestOptions{Language: "hi"})
	wrongModel.Plan.Route.Model = "kapture-v2"
	wrongPath := request(strings.TrimSuffix(endpoint, batchPath)+"/v2/asr/transcribe", protocol.RequestOptions{Language: "hi"})
	wrongProvider := request(endpoint, protocol.RequestOptions{Language: "hi"})
	wrongProvider.Plan.Route.Provider = "fish"
	for _, req := range []runtimepkg.BatchTranscribeRequest{wrongModel, wrongPath, wrongProvider} {
		if _, err := adapter.Transcribe(context.Background(), req); err == nil {
			t.Fatalf("route %+v was accepted", req.Plan.Route)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("calls = %d, want 0", calls.Load())
	}
}

func TestNewBatchPinsOfficialHost(t *testing.T) {
	t.Parallel()
	adapter, err := NewBatch(BatchConfig{})
	if err != nil {
		t.Fatal(err)
	}
	req := request("https://api.example.com"+batchPath, protocol.RequestOptions{Language: "hi"})
	if _, err := adapter.Transcribe(context.Background(), req); err == nil {
		t.Fatal("a non-FonadaLabs host was accepted")
	}
}
