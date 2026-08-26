package cartesia

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
			Provider: "cartesia", Model: model, Endpoint: endpoint, Transport: protocol.TransportHTTP,
			Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "ct-key"},
		},
	}
}

func TestBatchTranscribeUsesBatchSTTContract(t *testing.T) {
	t.Parallel()
	forms := make(chan map[string][]string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-API-Key") != "ct-key" || r.Header.Get("Cartesia-Version") != defaultSTTVersion || r.Header.Get("Authorization") != "" {
			t.Errorf("headers = %v", r.Header)
		}
		_ = r.ParseMultipartForm(1 << 20)
		if _, header, err := r.FormFile("file"); err != nil || header.Filename != "audio.wav" {
			t.Errorf("file: %v %v", header, err)
		}
		forms <- r.MultipartForm.Value
		_, _ = w.Write([]byte(`{"type":"transcript","request_id":"req-c","text":"Hello there. Hi.","language":"en","duration":1823.17,"words":[{"word":"Hello","start":0.1,"end":0.4},{"word":"there.","start":0.45,"end":0.8},{"word":"Hi.","start":2.0,"end":2.2}]}`))
	}))
	defer server.Close()
	adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	result, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
		Plan: batchPlan(server.URL+"/stt", BatchModel), Options: protocol.RequestOptions{Language: "en-US"},
		Audio: strings.NewReader("RIFF....WAVE"), AudioBytes: 12,
	})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	form := <-forms
	for key, want := range map[string]string{"model": BatchModel, "timestamp_granularities[]": "word", "language": "en"} {
		if values := form[key]; len(values) != 1 || values[0] != want {
			t.Fatalf("form %s = %v", key, values)
		}
	}
	if result.Text != "Hello there. Hi." || result.DurationMS != 1823170 || result.ProviderRequestID != "req-c" || result.Language != "en" {
		t.Fatalf("result = %+v", result)
	}
	if len(result.Segments) != 2 || result.Segments[0].Text != "Hello there." || result.Segments[1].StartMS != 2000 {
		t.Fatalf("segments = %+v", result.Segments)
	}
}

func TestBatchTranscribeRefusals(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusBadGateway) }))
	defer server.Close()
	adapter, _ := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	diarize := true
	var providerErr *runtimepkg.ProviderError
	if _, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(server.URL+"/stt", "ink-2"), Audio: strings.NewReader("x"), AudioBytes: 1}); err == nil {
		t.Fatal("ink-2 accepted on the batch endpoint")
	}
	_, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(server.URL+"/stt", BatchModel), Options: protocol.RequestOptions{STT: &protocol.SttOptions{Diarization: &diarize}}, Audio: strings.NewReader("x"), AudioBytes: 1})
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeInvalidRequest {
		t.Fatalf("diarize: %v", err)
	}
	_, err = adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(server.URL+"/stt", BatchModel), Audio: strings.NewReader("x"), AudioBytes: 1})
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeUnavailable || !providerErr.Retryable {
		t.Fatalf("502: %v", err)
	}
}
