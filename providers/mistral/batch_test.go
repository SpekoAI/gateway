package mistral

import (
	"bytes"
	"context"
	"encoding/binary"
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

// liveBatchResponse is what voxtral-mini-transcribe-3-0 returned for the
// 3.5 s TTS clip on 2026-10-21 with diarize=true and
// timestamp_granularities=segment. prompt_audio_seconds is floored.
const liveBatchResponse = `{"model":"voxtral-mini-transcribe-3-0","text":"Hello, this is Speko testing the new Voxtral voice. How does it sound today?","language":null,"segments":[{"type":"transcription_segment","text":"Hello, this is Speko testing the new Voxtral voice.","start":0.0,"end":2.5,"speaker_id":"speaker_1"},{"type":"transcription_segment","text":" How does it sound today?","start":2.6,"end":3.5,"speaker_id":"speaker_1"}],"usage":{"prompt_audio_seconds":3,"prompt_tokens":14,"total_tokens":428,"completion_tokens":414}}`

type capturedUpload struct {
	authorization string
	path          string
	parts         []string
	fields        map[string][]string
	fileName      string
	fileType      string
	file          []byte
}

func newFakeTranscribe(t *testing.T, status int, response string) (*httptest.Server, *capturedUpload) {
	t.Helper()
	captured := &capturedUpload{fields: map[string][]string{}}
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
			captured.fields[part.FormName()] = append(captured.fields[part.FormName()], string(body))
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("Mistral-Correlation-Id", "01a15e2b-efbc-7f4f-82b5-be55c1781a0a")
		writer.WriteHeader(status)
		_, _ = writer.Write([]byte(response))
	}))
	t.Cleanup(server.Close)
	return server, captured
}

// wav builds a canonical 44-byte-header container around pcm.
func wav(rate int, pcm []byte) []byte {
	var header bytes.Buffer
	header.WriteString("RIFF")
	_ = binary.Write(&header, binary.LittleEndian, uint32(36+len(pcm)))
	header.WriteString("WAVEfmt ")
	for _, field := range []any{uint32(16), uint16(1), uint16(1), uint32(rate), uint32(rate * 2), uint16(2), uint16(16)} {
		_ = binary.Write(&header, binary.LittleEndian, field)
	}
	header.WriteString("data")
	_ = binary.Write(&header, binary.LittleEndian, uint32(len(pcm)))
	return append(header.Bytes(), pcm...)
}

func batchRequest(endpoint, model string, audio []byte) runtimepkg.BatchTranscribeRequest {
	return runtimepkg.BatchTranscribeRequest{
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK},
			Route: protocol.PlanRoute{Provider: ProviderName, Model: model, Adapter: BatchAdapterID, Transport: protocol.TransportHTTP, Endpoint: endpoint,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "mistral_test_key", ExpiresAt: time.Now().Add(time.Minute)}},
		},
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

func TestBatchUploadsTheWavWithEveryAskedField(t *testing.T) {
	t.Parallel()
	server, captured := newFakeTranscribe(t, http.StatusOK, liveBatchResponse)
	diarize := true
	audio := wav(16_000, make([]byte, 56_000))
	request := batchRequest(server.URL+batchPath, BatchSmallModel, audio)
	request.Options = protocol.RequestOptions{Language: "en-US", STT: &protocol.SttOptions{Diarization: &diarize, Keywords: []string{"Speko", "Voxtral"}}}
	result, err := newBatchAdapter(t, server).Transcribe(context.Background(), request)
	if err != nil {
		t.Fatalf("Transcribe: %v", err)
	}
	if captured.authorization != "Bearer mistral_test_key" || captured.path != batchPath {
		t.Fatalf("request = auth %q path %q", captured.authorization, captured.path)
	}
	if got := strings.Join(captured.parts, ","); got != "model,timestamp_granularities,language,diarize,context_bias,context_bias,file" {
		t.Fatalf("parts = %s", got)
	}
	if captured.fields["model"][0] != BatchSmallModel || captured.fields["timestamp_granularities"][0] != "segment" || captured.fields["language"][0] != "en" || captured.fields["diarize"][0] != "true" || strings.Join(captured.fields["context_bias"], ",") != "Speko,Voxtral" {
		t.Fatalf("fields = %v", captured.fields)
	}
	if captured.fileName != "audio.wav" || captured.fileType != "audio/wav" || !bytes.Equal(captured.file, audio) {
		t.Fatalf("file part = %q %q %d bytes", captured.fileName, captured.fileType, len(captured.file))
	}
	if result.Text != "Hello, this is Speko testing the new Voxtral voice. How does it sound today?" || result.Language != "en" || result.ProviderRequestID != "01a15e2b-efbc-7f4f-82b5-be55c1781a0a" {
		t.Fatalf("result = %+v", result)
	}
	// The floored prompt_audio_seconds is not the provider duration.
	if result.DurationMS != 0 || result.Words != nil {
		t.Fatalf("result invents a duration or words: %+v", result)
	}
	want := []runtimepkg.BatchSegment{
		{Text: "Hello, this is Speko testing the new Voxtral voice.", StartMS: 0, EndMS: 2_500, Speaker: "speaker_1"},
		{Text: "How does it sound today?", StartMS: 2_600, EndMS: 3_500, Speaker: "speaker_1"},
	}
	if len(result.Segments) != 2 || result.Segments[0] != want[0] || result.Segments[1] != want[1] {
		t.Fatalf("segments = %+v", result.Segments)
	}
	if err := result.Billing.Validate(); err != nil || !result.Billing.Complete {
		t.Fatalf("billing = %+v %v", result.Billing, err)
	}
	// 56,000 bytes of 16 kHz PCM16 is 1.75 s, not the 3 s floor nor the
	// container's length.
	operation := result.Billing.Operations[0]
	if operation.Quantities["duration_seconds"] != 56_000 || operation.QuantityDenominator("duration_seconds") != 32_000 || operation.Mode != "batch" || operation.Model != BatchSmallModel || operation.ProviderRequestID != "01a15e2b-efbc-7f4f-82b5-be55c1781a0a" {
		t.Fatalf("billing operation = %+v", operation)
	}
}

// Without a language the field is omitted so the model detects one; without
// a diarization ask the speaker labels are dropped.
func TestBatchSendsOnlyWhatWasAskedFor(t *testing.T) {
	t.Parallel()
	server, captured := newFakeTranscribe(t, http.StatusOK, liveBatchResponse)
	result, err := newBatchAdapter(t, server).Transcribe(context.Background(), batchRequest(server.URL+batchPath, BatchMiniModel, wav(16_000, make([]byte, 320))))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(captured.parts, ","); got != "model,timestamp_granularities,file" {
		t.Fatalf("parts = %s", got)
	}
	if result.Segments[0].Speaker != "" || result.Language != "" {
		t.Fatalf("result = %+v", result)
	}
}

// The data chunk is found behind ancillary chunks, and a streaming header's
// oversized length is clamped to what the container holds.
func TestWAVDataBytes(t *testing.T) {
	t.Parallel()
	canonical := wav(16_000, make([]byte, 1_000))
	withList := append(append(append([]byte(nil), canonical[:36]...), []byte("LIST\x05\x00\x00\x00abcde\x00")...), canonical[36:]...)
	streaming := append([]byte(nil), canonical...)
	binary.LittleEndian.PutUint32(streaming[40:44], 0xFFFFFFFF)
	for name, audio := range map[string][]byte{"canonical": canonical, "list chunk": withList, "streaming": streaming} {
		got, err := wavDataBytes(bytes.NewReader(audio), int64(len(audio)))
		if err != nil || got != 1_000 {
			t.Errorf("%s: wavDataBytes = %d %v, want 1000", name, got, err)
		}
	}
	for name, audio := range map[string][]byte{"not wav": []byte("ID3\x04 an mp3"), "no data": canonical[:36]} {
		_, err := wavDataBytes(bytes.NewReader(audio), int64(len(audio)))
		var providerErr *runtimepkg.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeUnsupportedMedia {
			t.Errorf("%s: wavDataBytes = %v, want unsupported_media", name, err)
		}
	}
}

func TestBatchRefusesWhatTheEndpointCannotServe(t *testing.T) {
	t.Parallel()
	adapter, err := NewBatch(BatchConfig{})
	if err != nil {
		t.Fatal(err)
	}
	audio := wav(16_000, make([]byte, 320))
	realtime := batchRequest(BatchEndpoint, DefaultSTTModel, audio)
	foreign := batchRequest(BatchEndpoint, BatchMiniModel, audio)
	foreign.Plan.Route.Provider = "openai"
	swahili := batchRequest(BatchEndpoint, BatchMiniModel, audio)
	swahili.Options.Language = "sw"
	words := batchRequest(BatchEndpoint, BatchMiniModel, audio)
	asked := true
	words.Options.STT = &protocol.SttOptions{WordTimestamps: &asked}
	oversized := batchRequest(BatchEndpoint, BatchMiniModel, audio)
	oversized.AudioBytes = BatchMaxAudioBytes + 1
	cases := map[string]struct {
		request runtimepkg.BatchTranscribeRequest
		code    string
	}{
		"realtime model":   {request: realtime},
		"foreign provider": {request: foreign},
		"wrong path":       {request: batchRequest("https://api.mistral.ai/v1/audio/speech", BatchMiniModel, audio)},
		"foreign host":     {request: batchRequest("https://example.com/v1/audio/transcriptions", BatchMiniModel, audio)},
		"swahili":          {request: swahili, code: "unsupported_language"},
		"word timestamps":  {request: words, code: batchhttp.CodeInvalidRequest},
		"over 500 MB":      {request: oversized, code: batchhttp.CodeInputTooLarge},
	}
	for name, tc := range cases {
		_, err := adapter.Transcribe(context.Background(), tc.request)
		if err == nil {
			t.Fatalf("%s: Transcribe accepted", name)
		}
		var providerErr *runtimepkg.ProviderError
		if tc.code != "" && (!errors.As(err, &providerErr) || providerErr.Code != tc.code) {
			t.Fatalf("%s: Transcribe = %v, want %s", name, err, tc.code)
		}
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
		// The first two are the live answers of 2026-10-21.
		{http.StatusUnauthorized, `{"detail":"Invalid API Key"}`, "authentication_failed", false},
		{http.StatusBadRequest, `{"object":"error","message":"Word-level segments are not enabled for this model","type":"invalid_request_invalid_args","param":null,"code":"3051","raw_status_code":400}`, "invalid_request", false},
		{http.StatusTooManyRequests, `{"object":"error","message":"Requests rate limit exceeded","type":"rate_limited","param":null,"code":"1300"}`, "provider_rate_limited", true},
		{http.StatusInternalServerError, `{"object":"error","message":"Internal error","type":"internal_server_error"}`, "provider_unavailable", true},
	}
	for _, tc := range cases {
		server, _ := newFakeTranscribe(t, tc.status, tc.body)
		_, err := newBatchAdapter(t, server).Transcribe(context.Background(), batchRequest(server.URL+batchPath, BatchMiniModel, wav(16_000, make([]byte, 320))))
		var providerErr *runtimepkg.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != tc.code || providerErr.Retryable != tc.retryable || providerErr.ProviderStatus != tc.status || strings.Contains(providerErr.Message, "Invalid") {
			t.Fatalf("%d: error = %#v, want %s", tc.status, err, tc.code)
		}
	}
}

func TestBatchRefusesAResponseWithoutText(t *testing.T) {
	t.Parallel()
	server, _ := newFakeTranscribe(t, http.StatusOK, `{"model":"voxtral-mini-transcribe-3-0","segments":[]}`)
	_, err := newBatchAdapter(t, server).Transcribe(context.Background(), batchRequest(server.URL+batchPath, BatchMiniModel, wav(16_000, make([]byte, 320))))
	if err == nil {
		t.Fatal("a response without text was accepted")
	}
}
