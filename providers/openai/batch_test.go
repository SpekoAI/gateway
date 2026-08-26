package openai

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/SpekoAI/gateway/internal/batchhttp"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

func batchPlan(endpoint, model string) protocol.SessionPlan {
	return protocol.SessionPlan{
		Execution: protocol.Execution{ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged},
		Route: protocol.PlanRoute{
			Provider: "openai", Model: model, Endpoint: endpoint, Transport: protocol.TransportHTTP,
			Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "sk-key"},
		},
	}
}

func TestBatchTranscribeWhisperVerboseJSON(t *testing.T) {
	t.Parallel()
	forms := make(chan map[string][]string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer sk-key" {
			t.Errorf("authorization = %q", r.Header.Get("Authorization"))
		}
		_ = r.ParseMultipartForm(1 << 20)
		if file, header, err := r.FormFile("file"); err != nil || header.Filename != "audio.wav" {
			t.Errorf("file: %v %v", header, err)
		} else if content, _ := io.ReadAll(file); string(content) != "RIFF....WAVE" {
			t.Errorf("file content = %q", content)
		}
		forms <- r.MultipartForm.Value
		w.Header().Set("x-request-id", "req_1")
		_, _ = w.Write([]byte(`{"task":"transcribe","language":"english","duration":1823.17,"text":"Hello there. Hi.","segments":[{"id":0,"start":0.1,"end":0.8,"text":" Hello there."},{"id":1,"start":1.0,"end":1.2,"text":" Hi."}],"usage":{"type":"duration","seconds":1823}}`))
	}))
	defer server.Close()

	adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	result, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
		Plan:       batchPlan(server.URL+"/v1/audio/transcriptions", "whisper-1"),
		Options:    protocol.RequestOptions{Language: "en-US", STT: &protocol.SttOptions{Keywords: []string{"Speko", "Router"}, ProviderOptions: map[string]map[string]any{"openai": {"prompt": "Sales call.", "temperature": 0}}}},
		Audio:      strings.NewReader("RIFF....WAVE"),
		AudioBytes: 12,
	})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	form := <-forms
	for key, want := range map[string]string{"model": "whisper-1", "response_format": "verbose_json", "timestamp_granularities[]": "segment", "language": "en", "prompt": "Sales call.\nSpeko, Router", "temperature": "0"} {
		if values := form[key]; len(values) != 1 || values[0] != want {
			t.Fatalf("form %s = %v, want %q", key, values, want)
		}
	}
	if result.Text != "Hello there. Hi." || result.DurationMS != 1823170 || result.ProviderRequestID != "req_1" || result.Language != "english" {
		t.Fatalf("result = %+v", result)
	}
	if len(result.Segments) != 2 || result.Segments[0] != (runtimepkg.BatchSegment{Text: "Hello there.", StartMS: 100, EndMS: 800}) {
		t.Fatalf("segments = %+v", result.Segments)
	}
}

func TestBatchTranscribeGPTTranscribeUsesUsageDuration(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseMultipartForm(1 << 20)
		if r.FormValue("response_format") != "json" || r.FormValue("timestamp_granularities[]") != "" {
			t.Errorf("form = %v", r.MultipartForm.Value)
		}
		_, _ = w.Write([]byte(`{"text":"Hello there.","usage":{"type":"duration","seconds":42}}`))
	}))
	defer server.Close()
	adapter, _ := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	result, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(server.URL+"/v1/audio/transcriptions", "gpt-transcribe"), Audio: strings.NewReader("x"), AudioBytes: 1})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	if result.Text != "Hello there." || result.DurationMS != 42000 || len(result.Segments) != 0 {
		t.Fatalf("result = %+v", result)
	}
}

func TestBatchTranscribeRefusals(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
	}))
	defer server.Close()
	adapter, _ := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	diarize := true
	var providerErr *runtimepkg.ProviderError

	_, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(server.URL+"/v1/audio/transcriptions", "gpt-transcribe"), Audio: strings.NewReader("x"), AudioBytes: BatchMaxAudioBytes + 1})
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeInputTooLarge {
		t.Fatalf("local size cap: %v", err)
	}
	_, err = adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(server.URL+"/v1/audio/transcriptions", "gpt-transcribe"), Audio: strings.NewReader("x"), AudioBytes: 1})
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeInputTooLarge || providerErr.ProviderStatus != 413 {
		t.Fatalf("413: %v", err)
	}
	if _, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(server.URL+"/v1/audio/transcriptions", "gpt-live-transcribe"), Audio: strings.NewReader("x"), AudioBytes: 1}); err == nil {
		t.Fatal("realtime-only model accepted")
	}
	_, err = adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(server.URL+"/v1/audio/transcriptions", "gpt-transcribe"), Options: protocol.RequestOptions{STT: &protocol.SttOptions{Diarization: &diarize}}, Audio: strings.NewReader("x"), AudioBytes: 1})
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeInvalidRequest {
		t.Fatalf("diarize on non-diarize model: %v", err)
	}
}
