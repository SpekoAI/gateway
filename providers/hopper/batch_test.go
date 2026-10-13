package hopper

import (
	"bytes"
	"context"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/internal/batchhttp"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

// documentedBatchResponse follows the shape the live endpoint returned on
// 2026-10-13, leading spaces on words included, with a word that has no
// timings, which is dropped.
const documentedBatchResponse = `{"text":" thanks for calling","words":[{"text":" thanks","start":0.8,"end":1.12,"conf":0},{"text":" for","start":1.12,"end":1.4,"conf":0},{"text":" uh"},{"text":" calling","start":1.4,"end":1.9,"conf":0}],"duration_seconds":6.08}`

type capturedUpload struct {
	authorization string
	path          string
	fields        map[string]string
	fileName      string
	fileType      string
	file          []byte
	parts         []string
}

func newFakeTranscribe(t *testing.T, status int, response string) (*httptest.Server, *capturedUpload) {
	t.Helper()
	return newFakeTranscribeWithCost(t, status, response, "")
}

// newFakeTranscribeWithCost also sets x-hopper-request-cost-usd when cost is
// not empty.
func newFakeTranscribeWithCost(t *testing.T, status int, response, cost string) (*httptest.Server, *capturedUpload) {
	t.Helper()
	captured := &capturedUpload{fields: map[string]string{}}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		captured.authorization = request.Header.Get("Authorization")
		captured.path = request.URL.Path
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
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			body, _ := io.ReadAll(part)
			captured.parts = append(captured.parts, part.FormName())
			if part.FormName() == "file" {
				captured.fileName, captured.fileType, captured.file = part.FileName(), part.Header.Get("Content-Type"), body
				continue
			}
			captured.fields[part.FormName()] = string(body)
		}
		writer.Header().Set("Content-Type", "application/json")
		if cost != "" {
			writer.Header().Set("x-hopper-request-cost-usd", cost)
		}
		writer.WriteHeader(status)
		_, _ = writer.Write([]byte(response))
	}))
	t.Cleanup(server.Close)
	return server, captured
}

func batchRequest(endpoint, language string, audio []byte) runtimepkg.BatchTranscribeRequest {
	return runtimepkg.BatchTranscribeRequest{
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK},
			Route: protocol.PlanRoute{Provider: ProviderName, Model: DefaultSTTModel, Adapter: BatchAdapterID, Transport: protocol.TransportHTTP, Endpoint: endpoint,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "sk_hopper_test", ExpiresAt: time.Now().Add(time.Minute)}},
		},
		Options:    protocol.RequestOptions{Language: language},
		Media:      protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
		Audio:      bytes.NewReader(audio),
		AudioBytes: int64(len(audio)),
	}
}

func newBatchAdapter(t *testing.T, server *httptest.Server) *BatchAdapter {
	t.Helper()
	parsed, _ := url.Parse(server.URL)
	adapter, err := NewBatch(BatchConfig{AllowedEndpointHosts: []string{parsed.Hostname()}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return adapter
}

func TestBatchUploadsTheWavWithModelAndRate(t *testing.T) {
	t.Parallel()
	server, captured := newFakeTranscribeWithCost(t, http.StatusOK, documentedBatchResponse, "0.000084")
	audio := append([]byte("RIFF....WAVEfmt "), make([]byte, 64)...)
	request := batchRequest(server.URL+batchPath, "en-US", audio)
	request.Media.SampleRateHz = 24_000
	request.Options.STT = &protocol.SttOptions{WordTimestamps: boolPointer(true)}
	result, err := newBatchAdapter(t, server).Transcribe(context.Background(), request)
	if err != nil {
		t.Fatalf("Transcribe: %v", err)
	}
	if captured.authorization != "Bearer sk_hopper_test" || captured.path != batchPath {
		t.Fatalf("request = auth %q path %q", captured.authorization, captured.path)
	}
	if strings.Join(captured.parts, ",") != "model,sample_rate,file" || captured.fields["model"] != DefaultSTTModel || captured.fields["sample_rate"] != "24000" {
		t.Fatalf("parts = %v fields = %v", captured.parts, captured.fields)
	}
	if captured.fileName != "audio.wav" || captured.fileType != "audio/wav" || !bytes.Equal(captured.file, audio) {
		t.Fatalf("file part = %q %q %d bytes", captured.fileName, captured.fileType, len(captured.file))
	}
	if result.Text != "thanks for calling" || result.DurationMS != 6_080 || result.Language != "en" {
		t.Fatalf("result = %+v", result)
	}
	if len(result.Words) != 3 || result.Words[0].Text != "thanks" || result.Words[1].Text != "for" || result.Words[2].Text != "calling" || result.Words[0].StartMS != 800 || result.Words[2].EndMS != 1_900 {
		t.Fatalf("words = %+v, want them trimmed", result.Words)
	}
	if len(result.Segments) != 1 || result.Segments[0].Text != "thanks for calling" {
		t.Fatalf("segments = %+v", result.Segments)
	}
	if err := result.Billing.Validate(); err != nil || !result.Billing.Complete {
		t.Fatalf("billing = %+v %v", result.Billing, err)
	}
	operation := result.Billing.Operations[0]
	if operation.Quantities["duration_seconds"] != 6_080 || operation.Mode != "batch" || operation.Model != DefaultSTTModel || operation.Language != "en" {
		t.Fatalf("billing operation = %+v", operation)
	}
	// $0.000084, the header's figure, in nano-dollars.
	if operation.Quantities["provider_cost_usd"] != 84_000 || operation.QuantityDenominator("provider_cost_usd") != 1_000_000_000 {
		t.Fatalf("provider cost = %d/%d", operation.Quantities["provider_cost_usd"], operation.QuantityDenominator("provider_cost_usd"))
	}
}

func TestBatchWithoutACostHeaderKeepsTheDuration(t *testing.T) {
	t.Parallel()
	for _, cost := range []string{"", "n/a", "-1"} {
		server, _ := newFakeTranscribeWithCost(t, http.StatusOK, documentedBatchResponse, cost)
		result, err := newBatchAdapter(t, server).Transcribe(context.Background(), batchRequest(server.URL+batchPath, "", []byte("RIFF")))
		if err != nil {
			t.Fatal(err)
		}
		operation := result.Billing.Operations[0]
		if _, present := operation.Quantities["provider_cost_usd"]; present || !operation.Complete || operation.Quantities["duration_seconds"] != 6_080 {
			t.Fatalf("cost %q: operation = %+v", cost, operation)
		}
		if err := result.Billing.Validate(); err != nil {
			t.Fatal(err)
		}
	}
}

// A duration finer than a millisecond is read in nanoseconds with its own
// denominator. The cost header must add its denominator beside that one, or
// the duration falls back to milliseconds and bills a million times over.
func TestBatchCostHeaderKeepsASubMillisecondDurationDenominator(t *testing.T) {
	t.Parallel()
	response := strings.Replace(documentedBatchResponse, `"duration_seconds":6.08`, `"duration_seconds":6.0800001`, 1)
	server, _ := newFakeTranscribeWithCost(t, http.StatusOK, response, "0.000084")
	result, err := newBatchAdapter(t, server).Transcribe(context.Background(), batchRequest(server.URL+batchPath, "", []byte("RIFF")))
	if err != nil {
		t.Fatal(err)
	}
	if err := result.Billing.Validate(); err != nil || !result.Billing.Complete {
		t.Fatalf("billing = %+v %v", result.Billing, err)
	}
	operation := result.Billing.Operations[0]
	if operation.Quantities["duration_seconds"] != 6_080_000_100 || operation.QuantityDenominator("duration_seconds") != 1_000_000_000 {
		t.Fatalf("duration = %d/%d, want 6080000100/1e9", operation.Quantities["duration_seconds"], operation.QuantityDenominator("duration_seconds"))
	}
	if operation.Quantities["provider_cost_usd"] != 84_000 || operation.QuantityDenominator("provider_cost_usd") != 1_000_000_000 {
		t.Fatalf("provider cost = %d/%d", operation.Quantities["provider_cost_usd"], operation.QuantityDenominator("provider_cost_usd"))
	}
}

// firstTurnTimedResponse is the live shape from 2026-10-13: a two-turn clip
// whose words cover only the first turn.
const firstTurnTimedResponse = `{"text":"thanks for calling speco support i can help you","words":[{"text":" thanks","start":0.96,"end":1.28,"conf":0},{"text":"for","start":1.28,"end":1.44,"conf":0},{"text":"calling","start":1.44,"end":1.76,"conf":0},{"text":"speco","start":2.24,"end":2.88,"conf":0},{"text":"support","start":2.88,"end":3.2,"conf":0}],"duration_seconds":6.08}`

func TestBatchWithholdsWordsThatDoNotCoverTheText(t *testing.T) {
	t.Parallel()
	server, _ := newFakeTranscribe(t, http.StatusOK, firstTurnTimedResponse)
	request := batchRequest(server.URL+batchPath, "", []byte("RIFF"))
	request.Options.STT = &protocol.SttOptions{WordTimestamps: boolPointer(true)}
	result, err := newBatchAdapter(t, server).Transcribe(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Text != "thanks for calling speco support i can help you" || result.Words != nil || result.Segments != nil {
		t.Fatalf("result = text %q words %+v segments %+v, want the text alone", result.Text, result.Words, result.Segments)
	}
}

func TestBatchReturnsNoWordsUnlessAsked(t *testing.T) {
	t.Parallel()
	server, captured := newFakeTranscribe(t, http.StatusOK, documentedBatchResponse)
	result, err := newBatchAdapter(t, server).Transcribe(context.Background(), batchRequest(server.URL+batchPath, "", []byte("RIFF")))
	if err != nil {
		t.Fatal(err)
	}
	if result.Words != nil {
		t.Fatalf("words returned without being asked for: %+v", result.Words)
	}
	if _, sent := captured.fields["language"]; sent || captured.fields["sample_rate"] != "16000" {
		t.Fatalf("fields = %v", captured.fields)
	}
}

func TestBatchRefusesWhatTheEndpointCannotServe(t *testing.T) {
	t.Parallel()
	adapter, err := NewBatch(BatchConfig{})
	if err != nil {
		t.Fatal(err)
	}
	unknownModel := batchRequest(BatchEndpoint, "", nil)
	unknownModel.Plan.Route.Model = "whisper"
	foreign := batchRequest(BatchEndpoint, "", nil)
	foreign.Plan.Route.Provider = "nari"
	rate := batchRequest(BatchEndpoint, "", nil)
	rate.Media.SampleRateHz = 32_000
	cases := map[string]runtimepkg.BatchTranscribeRequest{
		"spanish":          batchRequest(BatchEndpoint, "es", nil),
		"unknown model":    unknownModel,
		"foreign provider": foreign,
		"rate":             rate,
		"wrong path":       batchRequest("https://api.withhopper.com/v1/audio/speech", "", nil),
		"foreign host":     batchRequest("https://example.com/v1/audio/transcriptions", "", nil),
	}
	for name, request := range cases {
		if _, err := adapter.Transcribe(context.Background(), request); err == nil {
			t.Fatalf("%s: Transcribe accepted", name)
		}
	}
	var providerErr *runtimepkg.ProviderError
	if _, err := adapter.Transcribe(context.Background(), rate); !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeUnsupportedMedia {
		t.Fatalf("32 kHz = %v, want unsupported_media", err)
	}
}

func TestBatchRefusesAnUploadOver25MB(t *testing.T) {
	t.Parallel()
	adapter, err := NewBatch(BatchConfig{})
	if err != nil {
		t.Fatal(err)
	}
	request := batchRequest(BatchEndpoint, "", nil)
	request.AudioBytes = BatchMaxAudioBytes + 1
	_, err = adapter.Transcribe(context.Background(), request)
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeInputTooLarge {
		t.Fatalf("oversized upload = %v, want input_too_large", err)
	}
	if BatchMaxAudioBytes > 25_000_000 {
		t.Fatalf("BatchMaxAudioBytes = %d, must stay under 25 MB", BatchMaxAudioBytes)
	}
}

func TestBatchFailuresAreClassified(t *testing.T) {
	t.Parallel()
	cases := []struct {
		status    int
		body      string
		code      string
		retryable bool
	}{
		{http.StatusUnauthorized, `{"error":{"message":"Incorrect API key","type":"invalid_request_error","code":"invalid_api_key","param":null}}`, "authentication_failed", false},
		{http.StatusPaymentRequired, `{"error":{"message":"Key balance is zero","type":"insufficient_quota","code":"insufficient_quota","param":null}}`, "provider_quota_exceeded", false},
		{http.StatusBadRequest, `{"error":{"message":"mp3 is not supported","type":"invalid_request_error","code":"unsupported_audio_format","param":"file"}}`, "unsupported_media", false},
		{http.StatusBadRequest, `{"error":{"message":"no file","type":"invalid_request_error","code":"missing_file","param":"file"}}`, "invalid_request", false},
		{http.StatusRequestEntityTooLarge, `{"error":{"message":"too large","type":"invalid_request_error","code":"body_too_large","param":null}}`, "input_too_large", false},
		{http.StatusTooManyRequests, `{"error":{"message":"slow down","type":"rate_limit_error","code":"rate_limit_exceeded","param":null}}`, "provider_rate_limited", true},
		{http.StatusServiceUnavailable, `{"error":{"message":"offline","type":"server_error","code":"model_offline","param":null}}`, "provider_unavailable", true},
	}
	for _, tc := range cases {
		server, _ := newFakeTranscribe(t, tc.status, tc.body)
		_, err := newBatchAdapter(t, server).Transcribe(context.Background(), batchRequest(server.URL+batchPath, "", []byte("RIFF")))
		var providerErr *runtimepkg.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != tc.code || providerErr.Retryable != tc.retryable || providerErr.ProviderStatus != tc.status || strings.Contains(providerErr.Message, "Incorrect") {
			t.Fatalf("%d: %#v, want %s retryable=%v", tc.status, err, tc.code, tc.retryable)
		}
	}
	server, _ := newFakeTranscribe(t, http.StatusOK, `{"duration_seconds":1}`)
	if _, err := newBatchAdapter(t, server).Transcribe(context.Background(), batchRequest(server.URL+batchPath, "", []byte("RIFF"))); err == nil {
		t.Fatal("a response with no text was accepted")
	}
}

func TestBatchWithoutDurationLeavesBillingIncomplete(t *testing.T) {
	t.Parallel()
	response := strings.Replace(documentedBatchResponse, `,"duration_seconds":6.08`, "", 1)
	server, _ := newFakeTranscribe(t, http.StatusOK, response)
	result, err := newBatchAdapter(t, server).Transcribe(context.Background(), batchRequest(server.URL+batchPath, "", []byte("RIFF")))
	if err != nil {
		t.Fatal(err)
	}
	if result.Billing.Complete || result.Billing.Operations[0].Complete {
		t.Fatalf("billing = %+v, want incomplete", result.Billing)
	}
}

func boolPointer(value bool) *bool { return &value }
