package munsit

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

// liveBatchResponse is the munsit-en-ar answer recorded on 2026-10-07,
// trimmed of the attributes block, leading space included.
const liveBatchResponse = `{"statusCode":200,"data":{"transcriptionId":"a532fc3d-ce74-4b3c-be56-2d3dcd91824e","transcription":" مرحبا, كيف حالك اليوم يا صديقي؟","summary":"","duration":2.56,"timestamps":[{"word":"مرحبا","start":0.099,"end":0.495},{"word":"كيف","start":0.842,"end":1.09},{"word":"حالك","start":1.173,"end":1.453},{"word":"اليوم","start":1.486,"end":1.718},{"word":"يا","start":1.734,"end":1.817},{"word":"صديقي","start":1.866,"end":2.263}],"stats":{"fileName":"audio.wav","fileSize":"0.08 MB","mimeType":"audio/x-wav","creditsConsumed":4}},"message":"Success"}`

type capturedUpload struct {
	apiKey        string
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
	captured := &capturedUpload{fields: map[string]string{}}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		captured.apiKey = request.Header.Get("X-Api-Key")
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
		writer.WriteHeader(status)
		_, _ = writer.Write([]byte(response))
	}))
	t.Cleanup(server.Close)
	return server, captured
}

func batchRequest(endpoint, model, language string, audio []byte) runtimepkg.BatchTranscribeRequest {
	return runtimepkg.BatchTranscribeRequest{
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK},
			Route: protocol.PlanRoute{Provider: ProviderName, Model: model, Adapter: BatchAdapterID, Transport: protocol.TransportHTTP, Endpoint: endpoint,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "munsit-key", ExpiresAt: time.Now().Add(time.Minute)}},
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

func TestBatchUploadsTheWavAndTrimsTheTranscript(t *testing.T) {
	t.Parallel()
	server, captured := newFakeTranscribe(t, http.StatusOK, liveBatchResponse)
	audio := append([]byte("RIFF....WAVEfmt "), make([]byte, 64)...)
	request := batchRequest(server.URL+batchPath, CodeSwitchSTTModel, "ar-AE", audio)
	request.Options.STT = &protocol.SttOptions{WordTimestamps: boolPointer(true)}
	result, err := newBatchAdapter(t, server).Transcribe(context.Background(), request)
	if err != nil {
		t.Fatalf("Transcribe: %v", err)
	}
	if captured.apiKey != "munsit-key" || captured.authorization != "" || captured.path != batchPath {
		t.Fatalf("request = key %q auth %q path %q", captured.apiKey, captured.authorization, captured.path)
	}
	if strings.Join(captured.parts, ",") != "model,language,file" || captured.fields["model"] != CodeSwitchSTTModel || captured.fields["language"] != "ar" {
		t.Fatalf("parts = %v fields = %v", captured.parts, captured.fields)
	}
	if captured.fileName != "audio.wav" || captured.fileType != "audio/wav" || !bytes.Equal(captured.file, audio) {
		t.Fatalf("file part = %q %q %d bytes", captured.fileName, captured.fileType, len(captured.file))
	}
	if result.Text != "مرحبا, كيف حالك اليوم يا صديقي؟" {
		t.Fatalf("text = %q, want it trimmed", result.Text)
	}
	if result.ProviderRequestID != "a532fc3d-ce74-4b3c-be56-2d3dcd91824e" || result.DurationMS != 2_560 || result.Language != "ar" {
		t.Fatalf("result = %+v", result)
	}
	if len(result.Words) != 6 || result.Words[0].Text != "مرحبا" || result.Words[0].StartMS != 99 || result.Words[5].EndMS != 2_263 {
		t.Fatalf("words = %+v", result.Words)
	}
	if len(result.Segments) != 1 || result.LastTimedMS() != 2_263 {
		t.Fatalf("segments = %+v", result.Segments)
	}
	if err := result.Billing.Validate(); err != nil || !result.Billing.Complete {
		t.Fatalf("billing = %+v %v", result.Billing, err)
	}
	operation := result.Billing.Operations[0]
	if operation.Quantities["credits"] != 4_000 || operation.Mode != "batch" || operation.Model != CodeSwitchSTTModel || operation.ProviderRequestID != result.ProviderRequestID {
		t.Fatalf("billing operation = %+v", operation)
	}
}

func TestBatchSendsNoLanguageForMunsitOrAutoDetection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ model, language string }{{DefaultSTTModel, "ar-SA"}, {DefaultSTTModel, ""}, {CodeSwitchSTTModel, ""}, {CodeSwitchSTTModel, "auto"}} {
		server, captured := newFakeTranscribe(t, http.StatusOK, liveBatchResponse)
		result, err := newBatchAdapter(t, server).Transcribe(context.Background(), batchRequest(server.URL+batchPath, tc.model, tc.language, []byte("RIFF")))
		if err != nil {
			t.Fatalf("%s %q: %v", tc.model, tc.language, err)
		}
		if _, sent := captured.fields["language"]; sent || captured.fields["model"] != tc.model {
			t.Fatalf("%s %q: fields = %v", tc.model, tc.language, captured.fields)
		}
		if result.Words != nil {
			t.Fatalf("words returned without being asked for: %+v", result.Words)
		}
	}
}

func TestBatchRefusesWhatTheEndpointCannotServe(t *testing.T) {
	t.Parallel()
	adapter, err := NewBatch(BatchConfig{})
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]runtimepkg.BatchTranscribeRequest{
		"english on munsit": batchRequest(BatchEndpoint, DefaultSTTModel, "en", nil),
		"unknown model":     batchRequest(BatchEndpoint, "whisper", "", nil),
		"wrong path":        batchRequest("https://api.munsit.com/api/v1/listen", DefaultSTTModel, "", nil),
		"foreign host":      batchRequest("https://example.com/api/v1/audio/transcribe", DefaultSTTModel, "", nil),
	}
	foreign := batchRequest(BatchEndpoint, DefaultSTTModel, "", nil)
	foreign.Plan.Route.Provider = "hamsa"
	cases["foreign provider"] = foreign
	for name, request := range cases {
		if _, err := adapter.Transcribe(context.Background(), request); err == nil {
			t.Fatalf("%s: Transcribe accepted", name)
		}
	}
}

func TestBatchRefusesAnHourOfAudio(t *testing.T) {
	t.Parallel()
	adapter, err := NewBatch(BatchConfig{})
	if err != nil {
		t.Fatal(err)
	}
	request := batchRequest(BatchEndpoint, DefaultSTTModel, "", nil)
	request.AudioBytes = 60*60*16_000*2 + 44
	_, err = adapter.Transcribe(context.Background(), request)
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeInputTooLarge {
		t.Fatalf("hour upload = %v, want input_too_large", err)
	}
	if BatchMaxDurationSeconds >= 60*60 {
		t.Fatalf("BatchMaxDurationSeconds = %d, must stay under 60 minutes", BatchMaxDurationSeconds)
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
		{http.StatusUnauthorized, `{"errorCode":40101,"errorMessage":"Invalid API key"}`, batchhttp.CodeAuthenticationFailed, false},
		{http.StatusBadRequest, `{"errorCode":40001,"errorMessage":"model must be one of: munsit, munsit-en-ar"}`, batchhttp.CodeInvalidRequest, false},
		{http.StatusTooManyRequests, `{"errorCode":42901,"errorMessage":"Too many concurrent requests"}`, batchhttp.CodeRateLimited, true},
		{http.StatusBadGateway, `{"errorCode":50202,"errorMessage":"Upstream error"}`, batchhttp.CodeUnavailable, true},
	}
	for _, tc := range cases {
		server, _ := newFakeTranscribe(t, tc.status, tc.body)
		_, err := newBatchAdapter(t, server).Transcribe(context.Background(), batchRequest(server.URL+batchPath, DefaultSTTModel, "", []byte("RIFF")))
		var providerErr *runtimepkg.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != tc.code || providerErr.Retryable != tc.retryable || strings.Contains(providerErr.Message, "Invalid") {
			t.Fatalf("%d: %#v, want %s retryable=%v", tc.status, err, tc.code, tc.retryable)
		}
	}
	server, _ := newFakeTranscribe(t, http.StatusOK, `{"statusCode":200,"data":null,"message":"Success"}`)
	if _, err := newBatchAdapter(t, server).Transcribe(context.Background(), batchRequest(server.URL+batchPath, DefaultSTTModel, "", []byte("RIFF"))); err == nil {
		t.Fatal("a response with no transcription was accepted")
	}
}

func TestBatchWithoutCreditsLeavesBillingIncomplete(t *testing.T) {
	t.Parallel()
	response := strings.Replace(liveBatchResponse, `,"creditsConsumed":4`, "", 1)
	server, _ := newFakeTranscribe(t, http.StatusOK, response)
	result, err := newBatchAdapter(t, server).Transcribe(context.Background(), batchRequest(server.URL+batchPath, DefaultSTTModel, "", []byte("RIFF")))
	if err != nil {
		t.Fatal(err)
	}
	if result.Billing.Complete || result.Billing.Operations[0].Complete {
		t.Fatalf("billing = %+v, want incomplete", result.Billing)
	}
}

func boolPointer(value bool) *bool { return &value }
