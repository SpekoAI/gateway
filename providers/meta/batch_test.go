package meta

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/internal/batchhttp"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

type capturedBatch struct {
	authorization      string
	accept             string
	requestContentType string
	request            map[string]any
	audioContentType   string
	audioFilename      string
	audio              []byte
	parts              []string
}

func newFakeTranscribe(t *testing.T, status int, response string) (*httptest.Server, *capturedBatch) {
	t.Helper()
	captured := &capturedBatch{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		captured.authorization = request.Header.Get("Authorization")
		captured.accept = request.Header.Get("Accept")
		mediaType, params, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
		if err != nil || mediaType != "multipart/form-data" {
			t.Errorf("Content-Type = %q", request.Header.Get("Content-Type"))
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		reader := multipart.NewReader(request.Body, params["boundary"])
		for {
			part, err := reader.NextPart()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				// A client that aborts its upload mid-body ends here; the
				// oversized-upload test relies on that not being a test error.
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			body, _ := io.ReadAll(part)
			captured.parts = append(captured.parts, part.FormName())
			switch part.FormName() {
			case requestPartName:
				captured.requestContentType = part.Header.Get("Content-Type")
				_ = json.Unmarshal(body, &captured.request)
			case audioPartName:
				captured.audioContentType = part.Header.Get("Content-Type")
				captured.audioFilename = part.FileName()
				captured.audio = body
			}
		}
		writer.Header().Set("Content-Type", "application/json")
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
			Route: protocol.PlanRoute{Provider: ProviderName, Model: BatchModel, Adapter: BatchAdapterID, Transport: protocol.TransportHTTP, Endpoint: endpoint,
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

func TestBatchTranscribesTheWavAsTwoParts(t *testing.T) {
	t.Parallel()
	server, captured := newFakeTranscribe(t, http.StatusOK, `{"sessionId":"9f1c","transcript":"How is the weather? It is raining.","audioDurationMs":8240,
		"turns":[{"turnId":1,"startMs":1520,"endMs":4640,"transcript":"How is the weather?","speaker":"A"},{"turnId":2,"startMs":5900,"endMs":8240,"transcript":"It is raining.","speaker":"B"},{"turnId":3,"startMs":8240,"endMs":8240,"transcript":"  "}]}`)
	adapter := newBatchAdapter(t, server)

	result, err := adapter.Transcribe(context.Background(), batchRequest(server.URL, []byte("RIFFwav-bytes")))
	if err != nil {
		t.Fatalf("Transcribe: %v", err)
	}
	if result.Text != "How is the weather? It is raining." {
		t.Fatalf("Text = %q", result.Text)
	}
	if result.DurationMS != 8240 || result.ProviderRequestID != "9f1c" {
		t.Fatalf("DurationMS = %d, ProviderRequestID = %q", result.DurationMS, result.ProviderRequestID)
	}
	if len(result.Segments) != 2 {
		t.Fatalf("Segments = %+v, want the two non-empty turns", result.Segments)
	}
	if segment := result.Segments[1]; segment.Text != "It is raining." || segment.StartMS != 5900 || segment.EndMS != 8240 || segment.Speaker != "B" {
		t.Fatalf("second segment = %+v", segment)
	}
	if _, present := result.Extensions[batchExtensionID]; !present {
		t.Fatal("raw response is not attached under the extension id")
	}

	if captured.authorization != "Bearer test-api-key" {
		t.Fatalf("Authorization = %q", captured.authorization)
	}
	if captured.accept != "application/json" {
		t.Fatalf("Accept = %q", captured.accept)
	}
	if strings.Join(captured.parts, ",") != requestPartName+","+audioPartName {
		t.Fatalf("parts = %v, want request then audio", captured.parts)
	}
	if captured.requestContentType != "application/json" {
		t.Fatalf("request part Content-Type = %q, want the documented application/json", captured.requestContentType)
	}
	if captured.request["model"] != BatchModel || captured.request["mode"] != modeEndpointing || captured.request["audioEncoding"] != encodingWAV {
		t.Fatalf("request part = %v", captured.request)
	}
	for _, absent := range []string{"languageBias", "keywords"} {
		if _, present := captured.request[absent]; present {
			t.Fatalf("request part = %v, want %s absent when the caller asked for nothing", captured.request, absent)
		}
	}
	if captured.audioContentType != "audio/wav" || captured.audioFilename != "audio.wav" || string(captured.audio) != "RIFFwav-bytes" {
		t.Fatalf("audio part = %q %q %q", captured.audioContentType, captured.audioFilename, captured.audio)
	}
}

func TestBatchDiarizationBiasingAndKeywordsRideTheRequestPart(t *testing.T) {
	t.Parallel()
	server, captured := newFakeTranscribe(t, http.StatusOK, `{"sessionId":"s","transcript":"olá","audioDurationMs":900,"turns":[]}`)
	adapter := newBatchAdapter(t, server)
	request := batchRequest(server.URL, []byte("RIFF"))
	request.Options.Language = "pt-BR"
	diarize := true
	request.Options.STT = &protocol.SttOptions{Diarization: &diarize, Keywords: []string{" Speko ", ""}}

	result, err := adapter.Transcribe(context.Background(), request)
	if err != nil {
		t.Fatalf("Transcribe: %v", err)
	}
	if result.Text != "olá" || len(result.Segments) != 0 {
		t.Fatalf("result = %+v", result)
	}
	if captured.request["mode"] != modeDiarization {
		t.Fatalf("mode = %v", captured.request["mode"])
	}
	if bias, _ := captured.request["languageBias"].([]any); len(bias) != 1 || bias[0] != "Portuguese" {
		t.Fatalf("languageBias = %v", captured.request["languageBias"])
	}
	if keywords, _ := captured.request["keywords"].([]any); len(keywords) != 1 || keywords[0] != "Speko" {
		t.Fatalf("keywords = %v", captured.request["keywords"])
	}
}

func TestBatchJoinsTurnsWhenTheTranscriptIsEmpty(t *testing.T) {
	t.Parallel()
	server, _ := newFakeTranscribe(t, http.StatusOK, `{"sessionId":"s","transcript":"","audioDurationMs":900,"turns":[{"turnId":1,"startMs":0,"endMs":400,"transcript":"one"},{"turnId":2,"startMs":500,"endMs":900,"transcript":"two"}]}`)
	adapter := newBatchAdapter(t, server)
	result, err := adapter.Transcribe(context.Background(), batchRequest(server.URL, []byte("RIFF")))
	if err != nil {
		t.Fatalf("Transcribe: %v", err)
	}
	if result.Text != "one two" {
		t.Fatalf("Text = %q, want the turns joined", result.Text)
	}
}

func TestBatchRefusesAnEmptyTranscript(t *testing.T) {
	t.Parallel()
	server, _ := newFakeTranscribe(t, http.StatusOK, `{"sessionId":"s","transcript":"  ","audioDurationMs":900,"turns":[]}`)
	adapter := newBatchAdapter(t, server)
	_, err := adapter.Transcribe(context.Background(), batchRequest(server.URL, []byte("RIFF")))
	var failure *runtimepkg.ProviderError
	if !errors.As(err, &failure) || failure.Code != batchhttp.CodeProviderError {
		t.Fatalf("err = %v, want a provider error for the empty transcript", err)
	}
}

func TestBatchRefusesForeignProviderAndModel(t *testing.T) {
	t.Parallel()
	server, _ := newFakeTranscribe(t, http.StatusOK, `{}`)
	adapter := newBatchAdapter(t, server)
	foreign := batchRequest(server.URL, []byte("RIFF"))
	foreign.Plan.Route.Provider = "gemini"
	if _, err := adapter.Transcribe(context.Background(), foreign); err == nil || !strings.Contains(err.Error(), "cannot serve provider") {
		t.Fatalf("foreign provider: err = %v", err)
	}
	other := batchRequest(server.URL, []byte("RIFF"))
	other.Plan.Route.Model = "muse-voice-transcribe-2.0"
	if _, err := adapter.Transcribe(context.Background(), other); err == nil || !strings.Contains(err.Error(), "cannot serve model") {
		t.Fatalf("other model: err = %v", err)
	}
}

func TestBatchRefusesOversizedUploads(t *testing.T) {
	t.Parallel()
	server, captured := newFakeTranscribe(t, http.StatusOK, `{}`)
	adapter := newBatchAdapter(t, server)

	declared := batchRequest(server.URL, []byte("RIFF"))
	declared.AudioBytes = BatchMaxAudioBytes + 1
	_, err := adapter.Transcribe(context.Background(), declared)
	var failure *runtimepkg.ProviderError
	if !errors.As(err, &failure) || failure.Code != batchhttp.CodeInputTooLarge {
		t.Fatalf("declared oversize: err = %v", err)
	}
	if captured.authorization != "" {
		t.Fatal("an oversized declaration still reached the service")
	}

	// A file longer than its declaration must fail rather than be truncated.
	oversized := bytes.Repeat([]byte("x"), int(BatchMaxAudioBytes)+1)
	lying := batchRequest(server.URL, oversized)
	lying.AudioBytes = 4
	if _, err := adapter.Transcribe(context.Background(), lying); err == nil {
		t.Fatal("a stream longer than the ceiling was accepted")
	}
}

func TestBatchClassifiesUpstreamFailure(t *testing.T) {
	t.Parallel()
	server, _ := newFakeTranscribe(t, http.StatusTooManyRequests, `{"message":"concurrency limit reached"}`)
	adapter := newBatchAdapter(t, server)
	_, err := adapter.Transcribe(context.Background(), batchRequest(server.URL, []byte("RIFF")))
	var failure *runtimepkg.ProviderError
	if !errors.As(err, &failure) || failure.Code != batchhttp.CodeRateLimited || !failure.Retryable || failure.ProviderStatus != http.StatusTooManyRequests {
		t.Fatalf("err = %v, want a retryable rate-limit error", err)
	}
	if _, present := failure.Extensions[batchExtensionID]; !present {
		t.Fatal("error body is not attached under the extension id")
	}
}

func TestBatchLimitsFollowTheDocumentedCeilings(t *testing.T) {
	t.Parallel()
	if BatchMaxAudioBytes >= 32<<20 || BatchMaxAudioBytes < 31<<20 {
		t.Fatalf("BatchMaxAudioBytes = %d, want just under the 32 MB body ceiling", BatchMaxAudioBytes)
	}
	if BatchMaxDurationSeconds != 600 {
		t.Fatalf("BatchMaxDurationSeconds = %d, want the ten-minute cap", BatchMaxDurationSeconds)
	}
	if BatchModel != DefaultModel {
		t.Fatalf("BatchModel = %q, want the single published id", BatchModel)
	}
	if BatchEndpoint != "https://api.meta.ai/v1/asr/transcribe" {
		t.Fatalf("BatchEndpoint = %q", BatchEndpoint)
	}
}
