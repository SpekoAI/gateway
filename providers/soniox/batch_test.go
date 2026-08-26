package soniox

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
			Provider: "soniox", Model: BatchModel, Endpoint: endpoint, Transport: protocol.TransportHTTP,
			Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "sx-key"},
		},
		Reservation: protocol.Reservation{ID: "rsv_9"},
	}
}

func TestBatchTranscribeRunsTheAsyncSequence(t *testing.T) {
	t.Parallel()
	diarize := true
	var mu sync.Mutex
	var calls []string
	var creation map[string]any
	polls := 0
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer sx-key" {
			t.Errorf("authorization = %q", r.Header.Get("Authorization"))
		}
		calls = append(calls, r.Method+" "+r.URL.Path)
		switch r.Method + " " + r.URL.Path {
		case "POST /v1/files":
			if !strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
				t.Errorf("upload content-type = %q", r.Header.Get("Content-Type"))
			}
			if err := r.ParseMultipartForm(1 << 20); err != nil {
				t.Errorf("multipart: %v", err)
			}
			file, header, err := r.FormFile("file")
			if err != nil || header.Filename != "audio.wav" {
				t.Errorf("file part: %v %v", header, err)
			} else {
				content, _ := io.ReadAll(file)
				if string(content) != "RIFF....WAVE" {
					t.Errorf("file content = %q", content)
				}
			}
			_, _ = w.Write([]byte(`{"id":"file_1"}`))
		case "POST /v1/transcriptions":
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &creation)
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id":"tx_1","status":"queued"}`))
		case "GET /v1/transcriptions/tx_1":
			polls++
			if polls == 1 {
				_, _ = w.Write([]byte(`{"id":"tx_1","status":"processing"}`))
				return
			}
			_, _ = w.Write([]byte(`{"id":"tx_1","status":"completed","audio_duration_ms":1823170}`))
		case "GET /v1/transcriptions/tx_1/transcript":
			_, _ = w.Write([]byte(`{"id":"tx_1","text":"Hello there. Hi.","tokens":[{"text":"Hel","start_ms":100,"end_ms":250,"speaker":"1","language":"en"},{"text":"lo","start_ms":250,"end_ms":400,"speaker":"1"},{"text":" there.","start_ms":450,"end_ms":800,"speaker":"1"},{"text":" Hi.","start_ms":1000,"end_ms":1200,"speaker":"2"}]}`))
		case "DELETE /v1/transcriptions/tx_1", "DELETE /v1/files/file_1":
			w.WriteHeader(http.StatusNoContent)
			if len(calls) == 7 {
				close(done)
			}
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
		Plan:       batchPlan(server.URL + "/v1/transcriptions"),
		Options:    protocol.RequestOptions{Language: "en-US", STT: &protocol.SttOptions{Diarization: &diarize, Keywords: []string{"Speko"}}},
		Audio:      strings.NewReader(audio),
		AudioBytes: int64(len(audio)),
	})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	<-done
	mu.Lock()
	defer mu.Unlock()
	want := "POST /v1/files,POST /v1/transcriptions,GET /v1/transcriptions/tx_1,GET /v1/transcriptions/tx_1,GET /v1/transcriptions/tx_1/transcript,DELETE /v1/transcriptions/tx_1,DELETE /v1/files/file_1"
	if strings.Join(calls[:7], ",") != want {
		t.Fatalf("calls = %v", calls)
	}
	wantCreation := map[string]any{"file_id": "file_1", "model": BatchModel, "enable_speaker_diarization": true, "language_hints": []any{"en"}, "context": map[string]any{"terms": []any{"Speko"}}, "client_reference_id": "speko_reservation:rsv_9"}
	got, _ := json.Marshal(creation)
	wantJSON, _ := json.Marshal(wantCreation)
	if string(got) != string(wantJSON) {
		t.Fatalf("creation = %s\nwant %s", got, wantJSON)
	}
	if result.Text != "Hello there. Hi." || result.DurationMS != 1823170 || result.ProviderRequestID != "tx_1" || result.Language != "en" {
		t.Fatalf("result = %+v", result)
	}
	if len(result.Segments) != 2 || result.Segments[0] != (runtimepkg.BatchSegment{Text: "Hello there.", StartMS: 100, EndMS: 800, Speaker: "1"}) || result.Segments[1] != (runtimepkg.BatchSegment{Text: "Hi.", StartMS: 1000, EndMS: 1200, Speaker: "2"}) {
		t.Fatalf("segments = %+v", result.Segments)
	}
}

func TestBatchTranscribeRefusesRealtimeModelAndMapsJobError(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "POST /v1/files":
			_, _ = w.Write([]byte(`{"id":"file_2"}`))
		case "POST /v1/transcriptions":
			body, _ := io.ReadAll(r.Body)
			if !strings.Contains(string(body), `"enable_language_identification":true`) {
				t.Errorf("creation = %s", body)
			}
			_, _ = w.Write([]byte(`{"id":"tx_2","status":"queued"}`))
		case "GET /v1/transcriptions/tx_2":
			_, _ = w.Write([]byte(`{"id":"tx_2","status":"error","error_message":"unsupported audio"}`))
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer server.Close()
	adapter, _ := NewBatch(BatchConfig{HTTPClient: server.Client(), PollInterval: 1, AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})

	realtime := batchPlan(server.URL + "/v1/transcriptions")
	realtime.Route.Model = "stt-rt-v5"
	if _, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: realtime, Audio: strings.NewReader("x"), AudioBytes: 1}); err == nil {
		t.Fatal("realtime model accepted")
	}

	_, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(server.URL + "/v1/transcriptions"), Audio: strings.NewReader("x"), AudioBytes: 1})
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeProviderError || providerErr.Retryable {
		t.Fatalf("job error: %v", err)
	}
}
