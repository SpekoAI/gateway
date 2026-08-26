package deepgram

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
			Provider: "deepgram", Model: model, Endpoint: endpoint, Transport: protocol.TransportHTTP,
			Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "dg-key"},
		},
		Reservation: protocol.Reservation{ID: "rsv_1"},
	}
}

func newBatchTestAdapter(t *testing.T, server *httptest.Server) *BatchAdapter {
	t.Helper()
	adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	return adapter
}

func TestBatchTranscribeUsesPreRecordedContract(t *testing.T) {
	t.Parallel()
	diarize := true
	observed := make(chan *http.Request, 1)
	bodies := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		observed <- r.Clone(context.Background())
		bodies <- body
		_, _ = w.Write([]byte(`{"metadata":{"request_id":"req-1","duration":1823.17},"results":{"channels":[{"detected_language":"en","alternatives":[{"transcript":"Hello there. Hi.","words":[{"word":"hello","punctuated_word":"Hello","start":0.1,"end":0.4,"speaker":0},{"word":"there","punctuated_word":"there.","start":0.45,"end":0.8,"speaker":0},{"word":"hi","punctuated_word":"Hi.","start":1.0,"end":1.2,"speaker":1}]}]}],"utterances":[{"start":0.1,"end":0.8,"speaker":0,"transcript":"Hello there."},{"start":1.0,"end":1.2,"speaker":1,"transcript":"Hi."}]}}`))
	}))
	defer server.Close()

	adapter := newBatchTestAdapter(t, server)
	audio := "RIFF....WAVEfmt data...."
	result, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
		Plan:       batchPlan(server.URL+"/v1/listen", "nova-3"),
		Options:    protocol.RequestOptions{Language: "en", STT: &protocol.SttOptions{Diarization: &diarize, Keywords: []string{"Speko"}, ProviderOptions: map[string]map[string]any{"deepgram": {"numerals": true}}}},
		Media:      protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 8000, Channels: 2},
		Audio:      strings.NewReader(audio),
		AudioBytes: int64(len(audio)),
	})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	request := <-observed
	if request.Method != http.MethodPost || request.URL.Path != "/v1/listen" {
		t.Fatalf("%s %s", request.Method, request.URL.Path)
	}
	if got := request.Header.Get("Authorization"); got != "Token dg-key" {
		t.Fatalf("authorization = %q", got)
	}
	if got := request.Header.Get("Content-Type"); got != "audio/wav" {
		t.Fatalf("content-type = %q", got)
	}
	if request.ContentLength != int64(len(audio)) || string(<-bodies) != audio {
		t.Fatal("body was not the raw WAV container")
	}
	query := request.URL.Query()
	for key, want := range map[string]string{"model": "nova-3", "language": "en", "diarize": "true", "utterances": "true", "punctuate": "true", "smart_format": "true", "keyterm": "Speko", "numerals": "true", "extra": "speko_reservation:rsv_1"} {
		if got := query.Get(key); got != want {
			t.Fatalf("query %s = %q, want %q", key, got, want)
		}
	}
	if query.Has("detect_language") || query.Has("keywords") {
		t.Fatalf("unexpected query: %v", query)
	}

	if result.Text != "Hello there. Hi." || result.Language != "en" || result.DurationMS != 1823170 || result.ProviderRequestID != "req-1" {
		t.Fatalf("result = %+v", result)
	}
	if len(result.Segments) != 2 || result.Segments[0] != (runtimepkg.BatchSegment{Text: "Hello there.", StartMS: 100, EndMS: 800, Speaker: "0"}) || result.Segments[1].Speaker != "1" {
		t.Fatalf("segments = %+v", result.Segments)
	}
	if result.LastTimedMS() != 1200 {
		t.Fatalf("last timed = %d", result.LastTimedMS())
	}
}

func TestBatchTranscribeGroupsWordsWithoutUtterancesAndDetectsLanguage(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("detect_language") != "true" || r.URL.Query().Get("keywords") != "Speko" {
			t.Errorf("query = %v", r.URL.Query())
		}
		_, _ = w.Write([]byte(`{"metadata":{"request_id":"req-2","duration":3},"results":{"channels":[{"alternatives":[{"transcript":"one two three","words":[{"word":"one","start":0,"end":0.3},{"word":"two","start":0.4,"end":0.6},{"word":"three","start":2.5,"end":2.9}]}]}]}}`))
	}))
	defer server.Close()
	adapter := newBatchTestAdapter(t, server)
	result, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
		Plan: batchPlan(server.URL+"/v1/listen", "nova-2"), Options: protocol.RequestOptions{STT: &protocol.SttOptions{Keywords: []string{"Speko"}}},
		Audio: strings.NewReader("x"), AudioBytes: 1,
	})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	if len(result.Segments) != 2 || result.Segments[0].Text != "one two" || result.Segments[1].Text != "three" || result.Segments[1].StartMS != 2500 {
		t.Fatalf("segments = %+v", result.Segments)
	}
}

func TestBatchTranscribeRefusals(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"err_code":"TOO_MANY_REQUESTS"}`))
	}))
	defer server.Close()
	adapter := newBatchTestAdapter(t, server)
	base := runtimepkg.BatchTranscribeRequest{Plan: batchPlan(server.URL+"/v1/listen", "nova-3"), Audio: strings.NewReader("x"), AudioBytes: 1}

	_, err := adapter.Transcribe(context.Background(), base)
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeRateLimited || !providerErr.Retryable || providerErr.ProviderStatus != 429 {
		t.Fatalf("429: %v", err)
	}

	flux := base
	flux.Plan.Route.Model = fluxEnglishModel
	if _, err := adapter.Transcribe(context.Background(), flux); err == nil {
		t.Fatal("flux accepted on the pre-recorded adapter")
	}

	huge := base
	huge.AudioBytes = BatchMaxAudioBytes + 1
	_, err = adapter.Transcribe(context.Background(), huge)
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeInputTooLarge {
		t.Fatalf("oversized: %v", err)
	}

	foreign := base
	foreign.Plan.Route.Endpoint = "https://evil.example.com/v1/listen"
	if _, err := adapter.Transcribe(context.Background(), foreign); err == nil {
		t.Fatal("foreign host accepted")
	}
}
