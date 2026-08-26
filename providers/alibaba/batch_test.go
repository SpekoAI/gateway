package alibaba

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
			Provider: "alibaba", Model: BatchModel, Endpoint: endpoint, Transport: protocol.TransportHTTP,
			Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "ds-key"},
		},
	}
}

func TestBatchTranscribeSubmitsPollsAndFetchesResult(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var calls []string
	var submission map[string]any
	polls := 0
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, r.Method+" "+r.URL.Path)
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/services/audio/asr/transcription":
			if r.Header.Get("Authorization") != "Bearer ds-key" || r.Header.Get("X-DashScope-Async") != "enable" {
				t.Errorf("headers = %v", r.Header)
			}
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &submission)
			_, _ = w.Write([]byte(`{"request_id":"rq","output":{"task_id":"task-1","task_status":"PENDING"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/tasks/task-1":
			polls++
			if polls == 1 {
				_, _ = w.Write([]byte(`{"output":{"task_id":"task-1","task_status":"RUNNING"}}`))
				return
			}
			_, _ = w.Write([]byte(`{"output":{"task_id":"task-1","task_status":"SUCCEEDED","result":{"transcription_url":"` + strings.Replace(server.URL, "http://", "https://", 1) + `/result.json"}},"usage":{"seconds":1824}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/result.json":
			if r.Header.Get("Authorization") != "" {
				t.Error("credential leaked to the result fetch")
			}
			_, _ = w.Write([]byte(`{"file_url":"x","properties":{},"transcripts":[{"channel_id":0,"text":"Hello there. Hi.","sentences":[{"begin_time":100,"end_time":800,"text":"Hello there.","language":"en"},{"begin_time":1000,"end_time":1200,"text":"Hi."}]}]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	// The result URL is https on a loopback address; route it back to the
	// plain test server through the client transport.
	client := server.Client()
	client.Transport = rewriteScheme{base: http.DefaultTransport}
	adapter, err := NewBatch(BatchConfig{HTTPClient: client, PollInterval: 1, AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	result, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
		Plan: batchPlan(server.URL + "/api/v1/services/audio/asr/transcription"), Options: protocol.RequestOptions{Language: "en-US"},
		Audio: strings.NewReader("RIFF"), AudioBytes: 4, SourceURL: "https://bucket.example.com/jobs/1/audio.wav?sig=1",
	})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(calls, ",") != "POST /api/v1/services/audio/asr/transcription,GET /api/v1/tasks/task-1,GET /api/v1/tasks/task-1,GET /result.json" {
		t.Fatalf("calls = %v", calls)
	}
	want := map[string]any{"model": BatchModel, "input": map[string]any{"file_url": "https://bucket.example.com/jobs/1/audio.wav?sig=1"}, "parameters": map[string]any{"language_hints": []any{"en"}}}
	got, _ := json.Marshal(submission)
	wantJSON, _ := json.Marshal(want)
	if string(got) != string(wantJSON) {
		t.Fatalf("submission = %s\nwant %s", got, wantJSON)
	}
	if result.Text != "Hello there. Hi." || result.DurationMS != 1824000 || result.ProviderRequestID != "task-1" || result.Language != "en" {
		t.Fatalf("result = %+v", result)
	}
	if len(result.Segments) != 2 || result.Segments[0] != (runtimepkg.BatchSegment{Text: "Hello there.", StartMS: 100, EndMS: 800}) {
		t.Fatalf("segments = %+v", result.Segments)
	}
}

func TestBatchTranscribeRefusals(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			_, _ = w.Write([]byte(`{"output":{"task_id":"task-2","task_status":"PENDING"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"output":{"task_id":"task-2","task_status":"FAILED","code":"InvalidFile","message":"file too long"}}`))
	}))
	defer server.Close()
	adapter, _ := NewBatch(BatchConfig{HTTPClient: server.Client(), PollInterval: 1, AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	var providerErr *runtimepkg.ProviderError
	diarize := true

	_, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(server.URL + "/api/v1/services/audio/asr/transcription"), Audio: strings.NewReader("x"), AudioBytes: 1})
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeInvalidRequest {
		t.Fatalf("missing source url: %v", err)
	}
	_, err = adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(server.URL + "/api/v1/services/audio/asr/transcription"), Options: protocol.RequestOptions{STT: &protocol.SttOptions{Diarization: &diarize}}, Audio: strings.NewReader("x"), AudioBytes: 1, SourceURL: "https://x/y"})
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeInvalidRequest {
		t.Fatalf("diarize: %v", err)
	}
	realtime := batchPlan(server.URL + "/api/v1/services/audio/asr/transcription")
	realtime.Route.Model = "qwen3-asr-flash-realtime"
	if _, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: realtime, Audio: strings.NewReader("x"), AudioBytes: 1, SourceURL: "https://x/y"}); err == nil {
		t.Fatal("realtime model accepted")
	}
	_, err = adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(server.URL + "/api/v1/services/audio/asr/transcription"), Audio: strings.NewReader("x"), AudioBytes: 1, SourceURL: "https://x/y"})
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeProviderError || string(providerErr.Extensions[batchExtensionID]) != `"file too long"` {
		t.Fatalf("failed task: %v", err)
	}
}

// rewriteScheme lets the test hand the adapter an https result URL that the
// plain httptest server answers.
type rewriteScheme struct{ base http.RoundTripper }

func (r rewriteScheme) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.URL.Scheme == "https" {
		clone := request.Clone(request.Context())
		clone.URL.Scheme = "http"
		return r.base.RoundTrip(clone)
	}
	return r.base.RoundTrip(request)
}
