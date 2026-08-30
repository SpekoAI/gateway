package gemini

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

type capturedBatch struct {
	apiKey      string
	contentType string
	body        map[string]any
}

func newFakeInteractions(t *testing.T, status int, response string) (*httptest.Server, *capturedBatch) {
	t.Helper()
	captured := &capturedBatch{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		captured.apiKey = request.Header.Get(APIKeyHeader)
		captured.contentType = request.Header.Get("Content-Type")
		_ = json.NewDecoder(request.Body).Decode(&captured.body)
		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("x-request-id", "req-42")
		writer.WriteHeader(status)
		_, _ = writer.Write([]byte(response))
	}))
	t.Cleanup(server.Close)
	return server, captured
}

func batchRequest(endpoint string, audio []byte) runtimepkg.BatchTranscribeRequest {
	return runtimepkg.BatchTranscribeRequest{
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged},
			Route: protocol.PlanRoute{Provider: ProviderName, Model: "gemini-3.5-transcribe", Adapter: BatchAdapterID, Transport: protocol.TransportHTTP, Endpoint: endpoint,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "test-api-key", ExpiresAt: time.Now().Add(time.Minute)}},
		},
		Media:      protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
		Audio:      bytes.NewReader(audio),
		AudioBytes: int64(len(audio)),
	}
}

func newBatchAdapter(t *testing.T, server *httptest.Server) *BatchAdapter {
	t.Helper()
	host := strings.TrimPrefix(server.URL, "http://")
	if index := strings.IndexByte(host, ':'); index > 0 {
		host = host[:index]
	}
	adapter, err := NewBatch(BatchConfig{AllowedEndpointHosts: []string{host}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return adapter
}

func TestBatchTranscribesInlineAudio(t *testing.T) {
	t.Parallel()
	server, captured := newFakeInteractions(t, http.StatusOK, `{"output_text":"  the whole transcript  "}`)
	adapter := newBatchAdapter(t, server)

	result, err := adapter.Transcribe(context.Background(), batchRequest(server.URL, []byte("RIFFwav-bytes")))
	if err != nil {
		t.Fatalf("Transcribe: %v", err)
	}
	if result.Text != "the whole transcript" {
		t.Fatalf("Text = %q", result.Text)
	}
	if result.ProviderRequestID != "req-42" {
		t.Fatalf("ProviderRequestID = %q", result.ProviderRequestID)
	}
	if len(result.Segments) != 0 {
		t.Fatalf("Segments = %v, want none until the annotation schema is pinned", result.Segments)
	}
	if captured.apiKey != "test-api-key" {
		t.Fatalf("api key header = %q — the AI Studio surface takes a key, not a bearer", captured.apiKey)
	}
	if captured.contentType != "application/json" {
		t.Fatalf("content type = %q", captured.contentType)
	}
	if captured.body["model"] != "gemini-3.5-transcribe" {
		t.Fatalf("model = %v", captured.body["model"])
	}
	input, _ := captured.body["input"].([]any)
	if len(input) != 1 {
		t.Fatalf("input = %v", captured.body["input"])
	}
	item, _ := input[0].(map[string]any)
	if item["type"] != "audio" || item["mime_type"] != "audio/wav" {
		t.Fatalf("input item = %v", item)
	}
	decoded, err := base64.StdEncoding.DecodeString(item["data"].(string))
	if err != nil || string(decoded) != "RIFFwav-bytes" {
		t.Fatalf("inline data = %v (%v)", item["data"], err)
	}
	// The request carries model and audio and nothing else: every other field
	// spelling is unpinned, and Google rejects unknown fields outright.
	if len(captured.body) != 2 {
		t.Fatalf("body keys = %v, want only model and input", captured.body)
	}
}

// Google's JSON surfaces normally emit camelCase; the documentation shows
// snake_case. Both are accepted because no schema settles which arrives.
func TestBatchAcceptsCamelCaseTranscript(t *testing.T) {
	t.Parallel()
	server, _ := newFakeInteractions(t, http.StatusOK, `{"outputText":"camel transcript"}`)
	result, err := newBatchAdapter(t, server).Transcribe(context.Background(), batchRequest(server.URL, []byte("wav")))
	if err != nil {
		t.Fatalf("Transcribe: %v", err)
	}
	if result.Text != "camel transcript" {
		t.Fatalf("Text = %q", result.Text)
	}
}

// Diarization is refused rather than silently dropped: a caller who asked for
// speaker labels and got unlabelled text has a wrong 200, not an error.
func TestBatchRefusesDiarization(t *testing.T) {
	t.Parallel()
	server, _ := newFakeInteractions(t, http.StatusOK, `{"output_text":"x"}`)
	request := batchRequest(server.URL, []byte("wav"))
	diarize := true
	request.Options.STT = &protocol.SttOptions{Diarization: &diarize}
	_, err := newBatchAdapter(t, server).Transcribe(context.Background(), request)
	if err == nil || !strings.Contains(err.Error(), "diarization") {
		t.Fatalf("Transcribe error = %v, want a diarization refusal", err)
	}
}

func TestBatchRefusesLiveModelAndForeignProvider(t *testing.T) {
	t.Parallel()
	server, _ := newFakeInteractions(t, http.StatusOK, `{"output_text":"x"}`)
	adapter := newBatchAdapter(t, server)

	request := batchRequest(server.URL, []byte("wav"))
	request.Plan.Route.Model = "gemini-3.5-transcribe-live"
	if _, err := adapter.Transcribe(context.Background(), request); err == nil {
		t.Fatal("accepted the live-only model on the interactions endpoint")
	}
	request = batchRequest(server.URL, []byte("wav"))
	request.Plan.Route.Provider = "google"
	if _, err := adapter.Transcribe(context.Background(), request); err == nil {
		t.Fatal("accepted provider google — that is Cloud Speech, not Gemini")
	}
}

func TestBatchRefusesOversizedInlineRequest(t *testing.T) {
	t.Parallel()
	server, _ := newFakeInteractions(t, http.StatusOK, `{"output_text":"x"}`)
	request := batchRequest(server.URL, []byte("wav"))
	// Declared size crosses the ceiling once base64 expansion is applied, so
	// the refusal must happen before the file is read.
	request.AudioBytes = batchRequestCeilingBytes
	_, err := newBatchAdapter(t, server).Transcribe(context.Background(), request)
	if err == nil || !strings.Contains(err.Error(), "inline request limit") {
		t.Fatalf("Transcribe error = %v, want an input_too_large refusal", err)
	}
}

func TestBatchClassifiesUpstreamFailure(t *testing.T) {
	t.Parallel()
	server, _ := newFakeInteractions(t, http.StatusTooManyRequests, `{"error":{"code":429,"message":"quota"}}`)
	_, err := newBatchAdapter(t, server).Transcribe(context.Background(), batchRequest(server.URL, []byte("wav")))
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "provider_rate_limited" {
		t.Fatalf("Transcribe error = %v, want provider_rate_limited", err)
	}
}

// An empty transcript is a failure, not an empty success: it would settle as a
// billed request that produced nothing the caller can use.
func TestBatchRefusesEmptyTranscript(t *testing.T) {
	t.Parallel()
	server, _ := newFakeInteractions(t, http.StatusOK, `{"output_text":"   "}`)
	if _, err := newBatchAdapter(t, server).Transcribe(context.Background(), batchRequest(server.URL, []byte("wav"))); err == nil {
		t.Fatal("accepted a response with no transcript")
	}
}

func TestEncodedLenSaturates(t *testing.T) {
	t.Parallel()
	if got := encodedLen(3); got != 4 {
		t.Fatalf("encodedLen(3) = %d, want 4", got)
	}
	if got := encodedLen(-1); got != 1<<62 {
		t.Fatalf("encodedLen(-1) = %d, want the saturating ceiling", got)
	}
}
