package smallest

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

func batchPlan(endpoint string) protocol.SessionPlan {
	return protocol.SessionPlan{
		Execution: protocol.Execution{ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged},
		Route: protocol.PlanRoute{
			Provider: "smallest", Model: "pulse", Endpoint: endpoint, Transport: protocol.TransportHTTP,
			Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "sm-key"},
		},
	}
}

func TestBatchTranscribePostsRawBody(t *testing.T) {
	t.Parallel()
	observed := make(chan *http.Request, 1)
	bodies := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		observed <- r.Clone(context.Background())
		bodies <- body
		_, _ = w.Write([]byte(`{"request_id":"rq1","status":"success","text":"Hello there. Hi.","language":"en","words":[{"word":"Hello","start":0.1,"end":0.4},{"word":"there.","start":0.45,"end":0.8},{"word":"Hi.","start":2.0,"end":2.2}],"metadata":{"duration":1823.17,"processing_time_ms":900}}`))
	}))
	defer server.Close()
	adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	result, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
		Plan: batchPlan(server.URL + "/waves/v1/stt/"), Options: protocol.RequestOptions{Language: "en"},
		Media: protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16000, Channels: 1},
		Audio: strings.NewReader("RIFF....WAVE"), AudioBytes: 12,
	})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	request := <-observed
	if request.Header.Get("Authorization") != "Bearer sm-key" || request.Header.Get("Content-Type") != "application/octet-stream" {
		t.Fatalf("headers = %v", request.Header)
	}
	if string(<-bodies) != "RIFF....WAVE" || request.ContentLength != 12 {
		t.Fatal("body was not the raw WAV")
	}
	query := request.URL.Query()
	if query.Get("model") != "pulse" || query.Get("word_timestamps") != "true" || query.Get("language") != "en" {
		t.Fatalf("query = %v", query)
	}
	if result.Text != "Hello there. Hi." || result.DurationMS != 1823170 || result.ProviderRequestID != "rq1" || len(result.Segments) != 2 {
		t.Fatalf("result = %+v", result)
	}
}

// TestBatchTranscribeDefaultsRequiredLanguageAndReadsNumericSpeakers: the
// pre-recorded endpoint 400s without `language`, so an unpinned request sends
// English; speaker labels arrive as integers.
func TestBatchTranscribeDefaultsRequiredLanguageAndReadsNumericSpeakers(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("language") != "en" {
			t.Errorf("language = %q, want the en default", r.URL.Query().Get("language"))
		}
		_, _ = w.Write([]byte(`{"request_id":"rq2","status":"success","transcription":"Hello there. Hi.","words":[{"word":"Hello","start":0.1,"end":0.4,"speaker":0},{"word":"there.","start":0.45,"end":0.8,"speaker":0},{"word":"Hi.","start":1.0,"end":1.2,"speaker":1}],"metadata":{"duration":1.5}}`))
	}))
	defer server.Close()
	adapter, _ := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	result, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
		Plan: batchPlan(server.URL + "/waves/v1/stt/"), Media: protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16000, Channels: 1},
		Audio: strings.NewReader("RIFF"), AudioBytes: 4,
	})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	if result.Text != "Hello there. Hi." || len(result.Segments) != 2 || result.Segments[0].Speaker != "0" || result.Segments[1].Speaker != "1" || result.DurationMS != 1500 {
		t.Fatalf("result = %+v", result)
	}
}

func TestBatchTranscribeRefusals(t *testing.T) {
	t.Parallel()
	adapter, _ := NewBatch(BatchConfig{})
	var providerErr *runtimepkg.ProviderError
	_, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(BatchEndpoint), Media: protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 8000, Channels: 2}, Audio: strings.NewReader("x"), AudioBytes: 1})
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeUnsupportedMedia {
		t.Fatalf("stereo: %v", err)
	}
	_, err = adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(BatchEndpoint), Media: protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 8000, Channels: 1}, Audio: strings.NewReader("x"), AudioBytes: BatchMaxAudioBytes + 1})
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeInputTooLarge {
		t.Fatalf("oversized: %v", err)
	}
}
