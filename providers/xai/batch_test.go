package xai

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

func batchPlan(endpoint string) protocol.SessionPlan {
	return protocol.SessionPlan{
		Execution: protocol.Execution{ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged},
		Route: protocol.PlanRoute{
			Provider: "xai", Model: "grok-stt", Endpoint: endpoint, Transport: protocol.TransportHTTP,
			Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "xai-key"},
		},
	}
}

func TestBatchTranscribeUsesRESTContract(t *testing.T) {
	t.Parallel()
	diarize := true
	forms := make(chan map[string][]string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer xai-key" {
			t.Errorf("authorization = %q", r.Header.Get("Authorization"))
		}
		_ = r.ParseMultipartForm(1 << 20)
		if _, header, err := r.FormFile("file"); err != nil || header.Filename != "audio.wav" {
			t.Errorf("file: %v %v", header, err)
		}
		forms <- r.MultipartForm.Value
		w.Header().Set("x-request-id", "xreq")
		_, _ = w.Write([]byte(`{"text":"Hello there. Hi.","language":"en","duration":1823.17,"words":[{"text":"Hello","start":0.1,"end":0.4,"speaker":0},{"text":"there.","start":0.45,"end":0.8,"speaker":0},{"text":"Hi.","start":1.0,"end":1.2,"speaker":1}]}`))
	}))
	defer server.Close()
	adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	result, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
		Plan: batchPlan(server.URL + "/v1/stt"), Options: protocol.RequestOptions{Language: "en", STT: &protocol.SttOptions{Diarization: &diarize, Keywords: []string{"Speko"}}},
		Audio: strings.NewReader("RIFF....WAVE"), AudioBytes: 12,
	})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	form := <-forms
	for key, want := range map[string]string{"language": "en", "diarize": "true", "keyterm": "Speko"} {
		if values := form[key]; len(values) != 1 || values[0] != want {
			t.Fatalf("form %s = %v", key, values)
		}
	}
	if form["model"] != nil {
		t.Fatal("xai takes no model parameter")
	}
	if result.Text != "Hello there. Hi." || result.DurationMS != 1823170 || result.ProviderRequestID != "xreq" {
		t.Fatalf("result = %+v", result)
	}
	if len(result.Segments) != 2 || result.Segments[0].Speaker != "0" || result.Segments[1].Speaker != "1" {
		t.Fatalf("segments = %+v", result.Segments)
	}
}

func TestBatchTranscribeSizeCap(t *testing.T) {
	t.Parallel()
	adapter, _ := NewBatch(BatchConfig{})
	_, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(BatchEndpoint), Audio: strings.NewReader("x"), AudioBytes: BatchMaxAudioBytes + 1})
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeInputTooLarge {
		t.Fatalf("oversized: %v", err)
	}
}
