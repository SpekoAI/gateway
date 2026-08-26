package speechmatics

import (
	"context"
	"encoding/json"
	"errors"
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
			Provider: "speechmatics", Model: "enhanced", Endpoint: endpoint, Transport: protocol.TransportHTTP,
			Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "sm-key"},
		},
	}
}

func TestBatchTranscribeRunsTheJobSequence(t *testing.T) {
	t.Parallel()
	diarize := true
	var mu sync.Mutex
	var calls []string
	var config map[string]any
	polls := 0
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer sm-key" {
			t.Errorf("authorization = %q", r.Header.Get("Authorization"))
		}
		calls = append(calls, r.Method+" "+r.URL.Path)
		switch r.Method + " " + r.URL.Path {
		case "POST /v2/jobs":
			if err := r.ParseMultipartForm(1 << 20); err != nil {
				t.Errorf("multipart: %v", err)
			}
			_ = json.Unmarshal([]byte(r.FormValue("config")), &config)
			if _, header, err := r.FormFile("data_file"); err != nil || header.Filename != "audio.wav" {
				t.Errorf("data_file: %v %v", header, err)
			}
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id":"job1"}`))
		case "GET /v2/jobs/job1":
			polls++
			if polls == 1 {
				_, _ = w.Write([]byte(`{"job":{"id":"job1","status":"running"}}`))
				return
			}
			_, _ = w.Write([]byte(`{"job":{"id":"job1","status":"done","duration":1823.17}}`))
		case "GET /v2/jobs/job1/transcript":
			if r.URL.Query().Get("format") != "json-v2" {
				t.Errorf("format = %q", r.URL.Query().Get("format"))
			}
			_, _ = w.Write([]byte(`{"job":{"duration":1823.17},"metadata":{"transcription_config":{"language":"en"}},"results":[{"type":"word","start_time":0.1,"end_time":0.4,"speaker":"S1","alternatives":[{"content":"Hello","language":"en"}]},{"type":"word","start_time":0.45,"end_time":0.8,"speaker":"S1","alternatives":[{"content":"there"}]},{"type":"punctuation","start_time":0.8,"end_time":0.8,"speaker":"S1","attaches_to":"previous","alternatives":[{"content":"."}]},{"type":"word","start_time":1.0,"end_time":1.2,"speaker":"S2","alternatives":[{"content":"Hi"}]}]}`))
		case "DELETE /v2/jobs/job1":
			_, _ = w.Write([]byte(`{"job":{"status":"deleted"}}`))
			close(done)
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
		Plan:       batchPlan(server.URL + "/v2/jobs"),
		Options:    protocol.RequestOptions{Language: "en-GB", STT: &protocol.SttOptions{Diarization: &diarize, Keywords: []string{"Speko"}, ProviderOptions: map[string]map[string]any{"speechmatics": {"domain": "finance"}}}},
		Audio:      strings.NewReader("RIFF....WAVE"),
		AudioBytes: 12,
	})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	<-done
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(calls, ",") != "POST /v2/jobs,GET /v2/jobs/job1,GET /v2/jobs/job1,GET /v2/jobs/job1/transcript,DELETE /v2/jobs/job1" {
		t.Fatalf("calls = %v", calls)
	}
	want := map[string]any{"type": "transcription", "transcription_config": map[string]any{"model": "enhanced", "language": "en", "output_locale": "en-GB", "diarization": "speaker", "additional_vocab": []any{map[string]any{"content": "Speko"}}, "domain": "finance"}}
	got, _ := json.Marshal(config)
	wantJSON, _ := json.Marshal(want)
	if string(got) != string(wantJSON) {
		t.Fatalf("config = %s\nwant %s", got, wantJSON)
	}
	if result.Text != "Hello there. Hi" || result.Language != "en" || result.DurationMS != 1823170 || result.ProviderRequestID != "job1" {
		t.Fatalf("result = %+v", result)
	}
	if len(result.Segments) != 2 || result.Segments[0] != (runtimepkg.BatchSegment{Text: "Hello there.", StartMS: 100, EndMS: 800, Speaker: "S1"}) || result.Segments[1].Speaker != "S2" {
		t.Fatalf("segments = %+v", result.Segments)
	}
}

func TestBatchTranscribeAutoLanguageAndRejectedJob(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "POST /v2/jobs":
			_ = r.ParseMultipartForm(1 << 20)
			if !strings.Contains(r.FormValue("config"), `"language":"auto"`) {
				t.Errorf("config = %s", r.FormValue("config"))
			}
			_, _ = w.Write([]byte(`{"id":"job2"}`))
		case "GET /v2/jobs/job2":
			_, _ = w.Write([]byte(`{"job":{"status":"rejected","errors":[{"message":"File format not supported"}]}}`))
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()
	adapter, _ := NewBatch(BatchConfig{HTTPClient: server.Client(), PollInterval: 1, AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	_, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(server.URL + "/v2/jobs"), Audio: strings.NewReader("x"), AudioBytes: 1})
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeProviderError || string(providerErr.Extensions[batchExtensionID]) != `"File format not supported"` {
		t.Fatalf("rejected: %v", err)
	}
}

func TestBatchPolicyAcceptsRegionalHosts(t *testing.T) {
	t.Parallel()
	adapter, err := NewBatch(BatchConfig{})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	for _, endpoint := range []string{BatchEndpoint, "https://us1.asr.api.speechmatics.com/v2/jobs", "https://au1.asr.api.speechmatics.com/v2/jobs"} {
		if _, err := adapter.endpointPolicy.Parse(endpoint); err != nil {
			t.Fatalf("%s: %v", endpoint, err)
		}
	}
	if _, err := adapter.endpointPolicy.Parse("https://asr.api.speechmatics.com/v2/jobs"); err == nil {
		t.Fatal("bare domain accepted")
	}
}
