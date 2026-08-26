package assemblyai

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
			Provider: "assemblyai", Model: "universal-3-5-pro", Endpoint: endpoint, Transport: protocol.TransportHTTP,
			Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialRelayAccess, Value: "aai-key"},
		},
	}
}

func TestBatchTranscribeUploadsSubmitsAndPolls(t *testing.T) {
	t.Parallel()
	diarize := true
	var mu sync.Mutex
	var calls []string
	var uploadBody []byte
	var submitBody map[string]any
	polls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Header.Get("Authorization") != "aai-key" {
			t.Errorf("authorization = %q", r.Header.Get("Authorization"))
		}
		calls = append(calls, r.Method+" "+r.URL.Path)
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/upload":
			uploadBody, _ = io.ReadAll(r.Body)
			if r.Header.Get("Content-Type") != "application/octet-stream" {
				t.Errorf("upload content-type = %q", r.Header.Get("Content-Type"))
			}
			_, _ = w.Write([]byte(`{"upload_url":"https://cdn.assemblyai.com/upload/abc"}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v2/transcript":
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &submitBody)
			_, _ = w.Write([]byte(`{"id":"tr_1","status":"queued"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v2/transcript/tr_1":
			polls++
			if polls < 3 {
				_, _ = w.Write([]byte(`{"id":"tr_1","status":"processing","text":null,"audio_duration":null}`))
				return
			}
			_, _ = w.Write([]byte(`{"id":"tr_1","status":"completed","text":"Hello there. Hi.","language_code":"en","audio_duration":1823,"words":[{"text":"Hello","start":100,"end":400,"speaker":"A"},{"text":"there.","start":450,"end":800,"speaker":"A"},{"text":"Hi.","start":1000,"end":1200,"speaker":"B"}],"utterances":[{"text":"Hello there.","start":100,"end":800,"speaker":"A"},{"text":"Hi.","start":1000,"end":1200,"speaker":"B"}]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), PollInterval: 1, AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	audio := "RIFF....WAVE"
	result, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
		Plan:       batchPlan(server.URL + "/v2/transcript"),
		Options:    protocol.RequestOptions{Language: "en-US", STT: &protocol.SttOptions{Diarization: &diarize, Keywords: []string{"Speko"}, ProviderOptions: map[string]map[string]any{"assemblyai": {"disfluencies": true}}}},
		Audio:      strings.NewReader(audio),
		AudioBytes: int64(len(audio)),
	})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(calls, ",") != "POST /v2/upload,POST /v2/transcript,GET /v2/transcript/tr_1,GET /v2/transcript/tr_1,GET /v2/transcript/tr_1" {
		t.Fatalf("calls = %v", calls)
	}
	if string(uploadBody) != audio {
		t.Fatal("upload body was not the WAV container")
	}
	want := map[string]any{"audio_url": "https://cdn.assemblyai.com/upload/abc", "speech_models": []any{"universal-3-5-pro"}, "punctuate": true, "format_text": true, "language_code": "en_us", "speaker_labels": true, "keyterms_prompt": []any{"Speko"}, "disfluencies": true}
	if got, _ := json.Marshal(submitBody); string(got) != mustJSON(t, want) {
		t.Fatalf("submit body = %s\nwant %s", got, mustJSON(t, want))
	}
	if result.Text != "Hello there. Hi." || result.Language != "en" || result.DurationMS != 1823000 || result.ProviderRequestID != "tr_1" {
		t.Fatalf("result = %+v", result)
	}
	if len(result.Segments) != 2 || result.Segments[0] != (runtimepkg.BatchSegment{Text: "Hello there.", StartMS: 100, EndMS: 800, Speaker: "A"}) {
		t.Fatalf("segments = %+v", result.Segments)
	}
}

func TestBatchTranscribeReportsProviderFailureAndLanguageDetection(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/upload":
			_, _ = w.Write([]byte(`{"upload_url":"https://cdn.assemblyai.com/upload/x"}`))
		case "/v2/transcript":
			body, _ := io.ReadAll(r.Body)
			if !strings.Contains(string(body), `"language_detection":true`) || strings.Contains(string(body), "language_code") {
				t.Errorf("submit body = %s", body)
			}
			_, _ = w.Write([]byte(`{"id":"tr_2","status":"queued"}`))
		default:
			_, _ = w.Write([]byte(`{"id":"tr_2","status":"error","error":"Audio file is too short"}`))
		}
	}))
	defer server.Close()
	adapter, _ := NewBatch(BatchConfig{HTTPClient: server.Client(), PollInterval: 1, AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	_, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(server.URL + "/v2/transcript"), Audio: strings.NewReader("x"), AudioBytes: 1})
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeProviderError || providerErr.Retryable {
		t.Fatalf("failed job: %v", err)
	}
	if string(providerErr.Extensions[batchExtensionID]) != `"Audio file is too short"` {
		t.Fatalf("extension = %s", providerErr.Extensions[batchExtensionID])
	}
}

func TestBatchTranscribeMapsUploadStatus(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"Authentication error"}`))
	}))
	defer server.Close()
	adapter, _ := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	_, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(server.URL + "/v2/transcript"), Audio: strings.NewReader("x"), AudioBytes: 1})
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeAuthenticationFailed {
		t.Fatalf("401: %v", err)
	}
	if _, err := NewBatch(BatchConfig{}); err != nil {
		t.Fatalf("default config: %v", err)
	}
	for tag, want := range map[string]string{"en": "en", "en-US": "en_us", "pt_BR": "pt_br", "de-DE": "de", "zh-TW": "zh_tw"} {
		if got := baseBatchLanguage(tag); got != want {
			t.Fatalf("%s -> %s, want %s", tag, got, want)
		}
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	encoded, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}
