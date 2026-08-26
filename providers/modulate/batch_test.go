package modulate

import (
	"context"
	"errors"
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
			Provider: "modulate", Model: model, Endpoint: endpoint, Transport: protocol.TransportHTTP,
			Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "md-key"},
		},
	}
}

func TestBatchTranscribeMultilingual(t *testing.T) {
	t.Parallel()
	diarize := true
	forms := make(chan map[string][]string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-API-Key") != "md-key" || r.URL.Path != "/api/velma-2-stt-batch" {
			t.Errorf("%s %v", r.URL.Path, r.Header)
		}
		_ = r.ParseMultipartForm(1 << 20)
		if _, header, err := r.FormFile("upload_file"); err != nil || header.Filename != "audio.wav" {
			t.Errorf("upload_file: %v %v", header, err)
		}
		forms <- r.MultipartForm.Value
		_, _ = w.Write([]byte(`{"text":"Hello there. Hi.","duration_ms":1823170,"utterances":[{"utterance_uuid":"u1","text":"Hello there.","start_ms":100,"duration_ms":700,"speaker":1,"language":"en"},{"utterance_uuid":"u2","text":"Hi.","start_ms":1000,"duration_ms":200,"speaker":2,"language":"en"}]}`))
	}))
	defer server.Close()
	adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	result, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
		Plan: batchPlan(server.URL+"/api/velma-2-stt-batch", BatchModelMultilingual), Options: protocol.RequestOptions{Language: "es-MX", STT: &protocol.SttOptions{Diarization: &diarize}},
		Audio: strings.NewReader("RIFF....WAVE"), AudioBytes: 12,
	})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	form := <-forms
	if form["language"][0] != "es" || form["diarize"][0] != "true" {
		t.Fatalf("form = %v", form)
	}
	if result.Text != "Hello there. Hi." || result.DurationMS != 1823170 || result.Language != "en" {
		t.Fatalf("result = %+v", result)
	}
	if len(result.Segments) != 2 || result.Segments[0] != (runtimepkg.BatchSegment{Text: "Hello there.", StartMS: 100, EndMS: 800, Speaker: "1"}) {
		t.Fatalf("segments = %+v", result.Segments)
	}
}

func TestBatchTranscribeRefusals(t *testing.T) {
	t.Parallel()
	adapter, _ := NewBatch(BatchConfig{})
	var providerErr *runtimepkg.ProviderError
	if _, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(BatchEndpointMultilingual, BatchModelEnglishFast), Audio: strings.NewReader("x"), AudioBytes: 1}); err == nil {
		t.Fatal("model/path mismatch accepted")
	}
	if _, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(BatchEndpointMultilingual, "velma-2-stt-streaming"), Audio: strings.NewReader("x"), AudioBytes: 1}); err == nil {
		t.Fatal("streaming model accepted")
	}
	_, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(BatchEndpointEnglishFast, BatchModelEnglishFast), Options: protocol.RequestOptions{Language: "de"}, Audio: strings.NewReader("x"), AudioBytes: 1})
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeInvalidRequest {
		t.Fatalf("english fast with german: %v", err)
	}
	_, err = adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(BatchEndpointMultilingual, BatchModelMultilingual), Audio: strings.NewReader("x"), AudioBytes: BatchMaxAudioBytes + 1})
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeInputTooLarge {
		t.Fatalf("oversized: %v", err)
	}
}
