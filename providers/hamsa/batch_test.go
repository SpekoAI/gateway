package hamsa

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/SpekoAI/gateway/internal/batchhttp"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

func batchPlan(endpoint string) protocol.SessionPlan {
	return protocol.SessionPlan{
		Execution: protocol.Execution{ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged},
		Route: protocol.PlanRoute{
			Provider: "hamsa", Model: BatchModelGeneral, Endpoint: endpoint, Transport: protocol.TransportHTTP,
			Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "hm-key"},
		},
	}
}

func TestBatchTranscribeSubmitsURLAndPolls(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var calls []string
	var submission map[string]any
	polls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Header.Get("Authorization") != "Token hm-key" {
			t.Errorf("authorization = %q", r.Header.Get("Authorization"))
		}
		calls = append(calls, r.Method+" "+r.URL.RequestURI())
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs/transcribe":
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &submission)
			_, _ = w.Write([]byte(`{"success":true,"message":"Job created","data":{"jobId":"job-7"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/jobs":
			polls++
			if polls == 1 {
				_, _ = w.Write([]byte(`{"success":true,"data":{"status":"PENDING"}}`))
				return
			}
			_, _ = w.Write([]byte(`{"success":true,"data":{"status":"COMPLETED","usageTime":"1823.17","jobResponse":{"text":"Hello there. Hi.","language":"ar","segments":[{"text":"Hello there.","start":0.1,"end":0.8,"speaker":"SPEAKER_00"},{"text":"Hi.","start":"1.0","end":"1.2","speaker":1}]}}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), PollInterval: 1, AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	result, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
		Plan: batchPlan(server.URL + "/v1/jobs/transcribe"), Options: protocol.RequestOptions{Language: "ar"},
		Audio: strings.NewReader("RIFF"), AudioBytes: 4, SourceURL: "https://bucket.example.com/jobs/1/audio.wav?X-Amz-Signature=abc",
	})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(calls, ",") != "POST /v1/jobs/transcribe,GET /v1/jobs?jobId=job-7,GET /v1/jobs?jobId=job-7" {
		t.Fatalf("calls = %v", calls)
	}
	if submission["mediaUrl"] != "https://bucket.example.com/jobs/1/audio.wav?X-Amz-Signature=abc" || submission["model"] != BatchModelGeneral || submission["processingType"] != "async" || submission["language"] != "ar" {
		t.Fatalf("submission = %v", submission)
	}
	if result.Text != "Hello there. Hi." || result.DurationMS != 1823170 || result.ProviderRequestID != "job-7" || result.Language != "ar" {
		t.Fatalf("result = %+v", result)
	}
	if len(result.Segments) != 2 || result.Segments[0] != (runtimepkg.BatchSegment{Text: "Hello there.", StartMS: 100, EndMS: 800, Speaker: "SPEAKER_00"}) || result.Segments[1] != (runtimepkg.BatchSegment{Text: "Hi.", StartMS: 1000, EndMS: 1200, Speaker: "1"}) {
		t.Fatalf("segments = %+v", result.Segments)
	}
}

func TestBatchTranscribeRefusals(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			_, _ = w.Write([]byte(`{"data":{"jobId":"job-8"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"data":{"status":"FAILED","error":"media unreachable"}}`))
	}))
	defer server.Close()
	adapter, _ := NewBatch(BatchConfig{HTTPClient: server.Client(), PollInterval: 1, AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	var providerErr *runtimepkg.ProviderError

	_, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(server.URL + "/v1/jobs/transcribe"), Audio: strings.NewReader("x"), AudioBytes: 1})
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeInvalidRequest {
		t.Fatalf("missing source url: %v", err)
	}
	realtime := batchPlan(server.URL + "/v1/jobs/transcribe")
	realtime.Route.Model = "s3"
	if _, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: realtime, Audio: strings.NewReader("x"), AudioBytes: 1, SourceURL: "https://x/y"}); err == nil {
		t.Fatal("realtime model accepted")
	}
	_, err = adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(server.URL + "/v1/jobs/transcribe"), Audio: strings.NewReader("x"), AudioBytes: 1, SourceURL: "https://x/y"})
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeProviderError || string(providerErr.Extensions[batchExtensionID]) != `"media unreachable"` {
		t.Fatalf("failed job: %v", err)
	}
}
