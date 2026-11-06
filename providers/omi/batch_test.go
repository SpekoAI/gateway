package omi

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/internal/batchhttp"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

// verboseResponse follows the verbose_json the US host returned on
// 2026-11-06, without diarize: no model field, a build id, segments without
// speakers.
const verboseResponse = `{"text":" Patient reports chest pain. ","language":"es","segments":[{"id":0,"text":" Patient reports","start":0.32,"end":1.4,"confidence":0.93},{"id":1,"text":"chest pain.","start":1.5,"end":2.6,"confidence":0.91}],"duration":7.38,"model_build":"omi-medical-1@9e0248f339dc","request_id":"req_abc123","metadata":{},"diagnostics":{},"language_source":"requested"}`

// diarizedResponse is the diarize=true shape: words and speakers.
const diarizedResponse = `{"text":"Hello doctor. Hi there.","language":"en","segments":[{"id":0,"text":"Hello doctor.","start":0.1,"end":0.9,"speaker":"SPEAKER_0"},{"id":1,"text":"Hi there.","start":1.2,"end":1.8,"speaker":"SPEAKER_1"}],"words":[{"word":"Hello","start":0.1,"end":0.4,"speaker":"SPEAKER_0"},{"word":"doctor.","start":0.45,"end":0.9,"speaker":"SPEAKER_0"},{"word":"Hi","start":1.2,"end":1.4,"speaker":"SPEAKER_1"},{"word":"there.","start":1.45,"end":1.8,"speaker":"SPEAKER_1"}],"speakers":["SPEAKER_0","SPEAKER_1"],"duration":2.0,"model":"omi-medical-1","model_build":"omi-medical-1@9e0248f339dc","request_id":"req_diarized"}`

type capturedUpload struct {
	authorization string
	idempotency   string
	path          string
	fields        map[string]string
	fileName      string
	fileType      string
	file          []byte
}

// fakeOmiAPI stands in for https://api.us.omi.health: the upload answers
// with uploadStatus/uploadBody, and /v1/jobs/{id} answers the scripted polls
// in order, the last one repeating.
type fakeOmiAPI struct {
	t            *testing.T
	server       *httptest.Server
	uploadStatus int
	uploadBody   string
	uploadHeader http.Header
	polls        []fakeAnswer
	results      map[string]string

	mu          sync.Mutex
	upload      capturedUpload
	uploads     int
	pollQueries []url.Values
	pollAuth    []string
	resultAuth  []string
}

type fakeAnswer struct {
	status int
	body   string
}

func newFakeOmiAPI(t *testing.T, status int, body string) *fakeOmiAPI {
	t.Helper()
	fake := &fakeOmiAPI{t: t, uploadStatus: status, uploadBody: body, results: map[string]string{}}
	mux := http.NewServeMux()
	mux.HandleFunc(batchPath, fake.handleUpload)
	mux.HandleFunc("/v1/jobs/", fake.handlePoll)
	mux.HandleFunc("/v1/results/", func(writer http.ResponseWriter, request *http.Request) {
		fake.mu.Lock()
		fake.resultAuth = append(fake.resultAuth, request.Header.Get("Authorization"))
		fake.mu.Unlock()
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(fake.results[request.URL.Path]))
	})
	fake.server = httptest.NewServer(mux)
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeOmiAPI) handleUpload(writer http.ResponseWriter, request *http.Request) {
	captured := capturedUpload{fields: map[string]string{}}
	captured.authorization = request.Header.Get("Authorization")
	captured.idempotency = request.Header.Get("Idempotency-Key")
	captured.path = request.URL.Path
	mediaType, params, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "multipart/form-data" {
		f.t.Errorf("Content-Type = %q", request.Header.Get("Content-Type"))
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
		if part.FormName() == "file" {
			captured.fileName, captured.fileType, captured.file = part.FileName(), part.Header.Get("Content-Type"), body
			continue
		}
		captured.fields[part.FormName()] = string(body)
	}
	f.mu.Lock()
	f.upload = captured
	f.uploads++
	f.mu.Unlock()
	for name, values := range f.uploadHeader {
		writer.Header()[name] = values
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("X-Request-Id", "req_header")
	writer.WriteHeader(f.uploadStatus)
	_, _ = writer.Write([]byte(f.uploadBody))
}

func (f *fakeOmiAPI) handlePoll(writer http.ResponseWriter, request *http.Request) {
	f.mu.Lock()
	f.pollQueries = append(f.pollQueries, request.URL.Query())
	f.pollAuth = append(f.pollAuth, request.Header.Get("Authorization"))
	index := min(len(f.pollQueries)-1, len(f.polls)-1)
	f.mu.Unlock()
	if index < 0 {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	answer := f.polls[index]
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(answer.status)
	_, _ = writer.Write([]byte(answer.body))
}

func (f *fakeOmiAPI) endpoint() string { return f.server.URL + batchPath }

func (f *fakeOmiAPI) host() string {
	parsed, _ := url.Parse(f.server.URL)
	return parsed.Hostname()
}

func (f *fakeOmiAPI) captured() capturedUpload {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.upload
}

// wav is a canonical 16 kHz mono PCM16 container around pcmBytes of silence.
// declared overrides the sizes the header states, as a piped writer does.
func wav(pcmBytes int, declared uint32) []byte {
	header := make([]byte, 44)
	copy(header[0:4], "RIFF")
	riff := declared
	if declared < 0xFFFFFFFF-36 {
		riff = declared + 36
	}
	binary.LittleEndian.PutUint32(header[4:8], riff)
	copy(header[8:12], "WAVE")
	copy(header[12:16], "fmt ")
	binary.LittleEndian.PutUint32(header[16:20], 16)
	binary.LittleEndian.PutUint16(header[20:22], 1)
	binary.LittleEndian.PutUint16(header[22:24], 1)
	binary.LittleEndian.PutUint32(header[24:28], 16_000)
	binary.LittleEndian.PutUint32(header[28:32], 32_000)
	binary.LittleEndian.PutUint16(header[32:34], 2)
	binary.LittleEndian.PutUint16(header[34:36], 16)
	copy(header[36:40], "data")
	binary.LittleEndian.PutUint32(header[40:44], declared)
	return append(header, make([]byte, pcmBytes)...)
}

func uploadRequest(endpoint, language string, audio []byte) runtimepkg.BatchTranscribeRequest {
	return runtimepkg.BatchTranscribeRequest{
		Plan: protocol.SessionPlan{
			PlanID: "plan_1", SessionID: "sess_1", AttemptID: "attempt_1",
			Execution: protocol.Execution{ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK},
			Route: protocol.PlanRoute{Provider: ProviderName, Model: Model, Adapter: BatchAdapterID, Transport: protocol.TransportHTTP, Endpoint: endpoint,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: testKey, ExpiresAt: time.Now().Add(time.Minute)}},
		},
		Options:    protocol.RequestOptions{Language: language},
		Media:      protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
		Audio:      bytes.NewReader(audio),
		AudioBytes: int64(len(audio)),
	}
}

func newTestBatch(t *testing.T, config BatchConfig, hosts ...string) *BatchAdapter {
	t.Helper()
	config.AllowedEndpointHosts = append(config.AllowedEndpointHosts, hosts...)
	config.AllowInsecureEndpoint = true
	if config.PollInterval == 0 {
		config.PollInterval = 5 * time.Millisecond
	}
	if config.MaxBackoff == 0 {
		config.MaxBackoff = 20 * time.Millisecond
	}
	adapter, err := NewBatch(config)
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return adapter
}

func transcribe(t *testing.T, fake *fakeOmiAPI, config BatchConfig, request runtimepkg.BatchTranscribeRequest) (*runtimepkg.BatchTranscription, error) {
	t.Helper()
	return newTestBatch(t, config, fake.host()).Transcribe(context.Background(), request)
}

func checkBilling(t *testing.T, result *runtimepkg.BatchTranscription, thousandths int64, requestID string) {
	t.Helper()
	if result.Billing == nil || result.Billing.Validate() != nil || len(result.Billing.Operations) != 1 {
		t.Fatalf("billing = %+v", result.Billing)
	}
	observation := result.Billing.Operations[0]
	if !result.Billing.Complete || !observation.Complete || observation.Quantities["duration_seconds"] != thousandths ||
		observation.Mode != "batch" || observation.Model != Model || observation.Language != "" || observation.ProviderRequestID != requestID {
		t.Fatalf("billing observation = %+v", observation)
	}
}

func TestBatchUploadsVerboseJSONAndBillsTheVendorDuration(t *testing.T) {
	t.Parallel()
	fake := newFakeOmiAPI(t, http.StatusOK, verboseResponse)
	audio := wav(32_000, 32_000)
	result, err := transcribe(t, fake, BatchConfig{}, uploadRequest(fake.endpoint(), "es-MX", audio))
	if err != nil {
		t.Fatal(err)
	}
	upload := fake.captured()
	if upload.authorization != "Bearer "+testKey || upload.path != batchPath {
		t.Fatalf("auth = %q path = %q", upload.authorization, upload.path)
	}
	want := map[string]string{"model": Model, "language": "es", "response_format": "verbose_json"}
	if fmt.Sprint(upload.fields) != fmt.Sprint(want) {
		t.Fatalf("fields = %v, want %v", upload.fields, want)
	}
	if upload.fileName != "audio.wav" || upload.fileType != "audio/wav" || !bytes.Equal(upload.file, audio) {
		t.Fatalf("file = %s %s %d bytes", upload.fileName, upload.fileType, len(upload.file))
	}
	if result.Text != "Patient reports chest pain." || result.Language != "es" || result.DurationMS != 7380 || result.ProviderRequestID != "req_abc123" {
		t.Fatalf("result = %+v", result)
	}
	if len(result.Segments) != 2 || result.Segments[0].Text != "Patient reports" || result.Segments[0].StartMS != 320 || result.Segments[1].EndMS != 2600 || result.Segments[0].Speaker != "" {
		t.Fatalf("segments = %+v", result.Segments)
	}
	if result.Words != nil {
		t.Fatalf("words without an ask = %+v", result.Words)
	}
	checkBilling(t, result, 7380, "req_abc123")
}

func TestBatchWithoutALanguageAsksForDetection(t *testing.T) {
	t.Parallel()
	fake := newFakeOmiAPI(t, http.StatusOK, strings.Replace(verboseResponse, `"request_id":"req_abc123",`, "", 1))
	result, err := transcribe(t, fake, BatchConfig{}, uploadRequest(fake.endpoint(), "", wav(3200, 3200)))
	if err != nil {
		t.Fatal(err)
	}
	if language := fake.captured().fields["language"]; language != "auto" {
		t.Fatalf("language = %q, want auto", language)
	}
	// The detected language is reported; the header request id stands in
	// for a body without one.
	if result.Language != "es" || result.ProviderRequestID != "req_header" {
		t.Fatalf("result = %+v", result)
	}
	checkBilling(t, result, 7380, "req_header")
}

func TestBatchSpeakerLabelsAndWordTimingsBothSendDiarize(t *testing.T) {
	t.Parallel()
	on := true
	speakers := protocol.SttOptions{Diarization: &on}
	words := protocol.SttOptions{WordTimestamps: &on}
	for name, options := range map[string]*protocol.SttOptions{"diarization": &speakers, "word timestamps": &words} {
		fake := newFakeOmiAPI(t, http.StatusOK, diarizedResponse)
		request := uploadRequest(fake.endpoint(), "en", wav(3200, 3200))
		request.Options.STT = options
		result, err := transcribe(t, fake, BatchConfig{}, request)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if fake.captured().fields["diarize"] != "true" {
			t.Fatalf("%s: fields = %v", name, fake.captured().fields)
		}
		switch name {
		case "diarization":
			if result.Segments[1].Speaker != "SPEAKER_1" || result.Words != nil {
				t.Fatalf("%s: segments = %+v words = %+v", name, result.Segments, result.Words)
			}
		case "word timestamps":
			if len(result.Words) != 4 || result.Words[3].Text != "there." || result.Words[3].StartMS != 1450 || result.Words[0].Speaker != "" {
				t.Fatalf("%s: words = %+v", name, result.Words)
			}
		}
		checkBilling(t, result, 2000, "req_diarized")
	}
}

func TestBatchWordsMissingForALanguageAreNotAnError(t *testing.T) {
	t.Parallel()
	// Portuguese answered diarize=true with speaker-labelled segments and no
	// words (word_timing_available false, language_aligner_unavailable).
	fake := newFakeOmiAPI(t, http.StatusOK, `{"text":"Olá doutor.","language":"pt","segments":[{"id":0,"text":"Olá doutor.","start":0.1,"end":0.9,"speaker":0}],"duration":1.0,"model_build":"omi-medical-1@9e0248f339dc","request_id":"req_pt","diagnostics":{"word_timing_available":false,"reason":"language_aligner_unavailable"}}`)
	on := true
	request := uploadRequest(fake.endpoint(), "pt-BR", wav(3200, 3200))
	request.Options.STT = &protocol.SttOptions{WordTimestamps: &on, Diarization: &on}
	result, err := transcribe(t, fake, BatchConfig{}, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Text != "Olá doutor." || result.Words != nil || result.Segments[0].Speaker != "0" {
		t.Fatalf("result = %+v", result)
	}
}

func TestBatchDigitalSilenceIsAnEmptyBilledTranscript(t *testing.T) {
	t.Parallel()
	fake := newFakeOmiAPI(t, http.StatusOK, `{"text":"","language":"en","segments":[],"duration":2.0,"model_build":"omi-medical-1@9e0248f339dc","request_id":"req_silence","diagnostics":{"empty_reason":"digital_silence"}}`)
	result, err := transcribe(t, fake, BatchConfig{}, uploadRequest(fake.endpoint(), "en", wav(64_000, 64_000)))
	if err != nil {
		t.Fatal(err)
	}
	if result.Text != "" || len(result.Segments) != 0 {
		t.Fatalf("result = %+v", result)
	}
	checkBilling(t, result, 2000, "req_silence")
}

func TestBatchWithoutDurationLeavesBillingIncomplete(t *testing.T) {
	t.Parallel()
	fake := newFakeOmiAPI(t, http.StatusOK, `{"text":"hello","model_build":"omi-medical-1@9e0248f339dc","request_id":"req_x"}`)
	result, err := transcribe(t, fake, BatchConfig{}, uploadRequest(fake.endpoint(), "en", wav(64_000, 64_000)))
	if err != nil {
		t.Fatal(err)
	}
	if result.Billing.Complete || result.Billing.Operations[0].Complete || len(result.Billing.Operations[0].Quantities) != 0 {
		t.Fatalf("billing = %+v, want incomplete without a byte fallback", result.Billing)
	}
}

func TestBatchRefusesAnotherServedModel(t *testing.T) {
	t.Parallel()
	for _, body := range []string{
		`{"text":"x","duration":1.0,"model_build":"omi-general-1@abc"}`,
		`{"text":"x","duration":1.0,"model":"omi-general-1"}`,
	} {
		fake := newFakeOmiAPI(t, http.StatusOK, body)
		_, err := transcribe(t, fake, BatchConfig{}, uploadRequest(fake.endpoint(), "en", wav(3200, 3200)))
		var providerErr *runtimepkg.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeUnavailable || !strings.Contains(providerErr.Message, "omi-general-1") {
			t.Errorf("%s: error = %v", body, err)
		}
	}
}

func TestBatchSendsAnExactWAVHeader(t *testing.T) {
	t.Parallel()
	fake := newFakeOmiAPI(t, http.StatusOK, verboseResponse)
	// A piped writer declares the maximal size, which Omi reads as more
	// than two hours.
	audio := wav(32_000, 0xFFFFFFFF)
	if _, err := transcribe(t, fake, BatchConfig{}, uploadRequest(fake.endpoint(), "en", audio)); err != nil {
		t.Fatal(err)
	}
	file := fake.captured().file
	if len(file) != len(audio) || binary.LittleEndian.Uint32(file[4:8]) != uint32(len(audio)-8) || binary.LittleEndian.Uint32(file[40:44]) != 32_000 || !bytes.Equal(file[44:], audio[44:]) {
		t.Fatalf("header sizes = %d/%d, length %d", binary.LittleEndian.Uint32(file[4:8]), binary.LittleEndian.Uint32(file[40:44]), len(file))
	}
}

func TestBatchIdempotencyKeyFollowsTheAttempt(t *testing.T) {
	t.Parallel()
	keys := map[string]string{}
	for _, attempt := range []string{"attempt_1", "attempt_1", "attempt_2"} {
		fake := newFakeOmiAPI(t, http.StatusOK, verboseResponse)
		request := uploadRequest(fake.endpoint(), "en", wav(3200, 3200))
		request.Plan.AttemptID = attempt
		if _, err := transcribe(t, fake, BatchConfig{}, request); err != nil {
			t.Fatal(err)
		}
		key := fake.captured().idempotency
		if !strings.HasPrefix(key, "speko-") || len(key) != len("speko-")+32 || strings.Contains(key, "plan_1") {
			t.Fatalf("Idempotency-Key = %q", key)
		}
		if previous, seen := keys[attempt]; seen && previous != key {
			t.Fatalf("attempt %s keys differ: %s, %s", attempt, previous, key)
		}
		keys[attempt] = key
	}
	if keys["attempt_1"] == keys["attempt_2"] {
		t.Fatal("a failover attempt reused the key")
	}
	if first, second := idempotencyKey(protocol.SessionPlan{}), idempotencyKey(protocol.SessionPlan{}); first == second {
		t.Fatal("plans without ids share a key")
	}
}

func TestBatchRefusesWhatItCannotServeWithoutUploading(t *testing.T) {
	t.Parallel()
	on := true
	cases := map[string]struct {
		mutate func(*runtimepkg.BatchTranscribeRequest)
		code   string
	}{
		"keywords": {func(r *runtimepkg.BatchTranscribeRequest) {
			r.Options.STT = &protocol.SttOptions{Keywords: []string{"metformin"}}
		}, "invalid_request"},
		"noise": {func(r *runtimepkg.BatchTranscribeRequest) { r.Options.STT = &protocol.SttOptions{NoiseReduction: &on} }, "invalid_request"},
		"realtime knob": {func(r *runtimepkg.BatchTranscribeRequest) {
			r.Options.STT = &protocol.SttOptions{ProviderOptions: map[string]map[string]any{ProviderName: {"silence_ms": float64(500)}}}
		}, "invalid_request"},
		"japanese":       {func(r *runtimepkg.BatchTranscribeRequest) { r.Options.Language = "ja" }, "unsupported_language"},
		"stereo":         {func(r *runtimepkg.BatchTranscribeRequest) { r.Media.Channels = 2 }, "unsupported_media"},
		"over the limit": {func(r *runtimepkg.BatchTranscribeRequest) { r.AudioBytes = BatchMaxAudioBytes + 1 }, "input_too_large"},
	}
	for name, test := range cases {
		fake := newFakeOmiAPI(t, http.StatusOK, verboseResponse)
		request := uploadRequest(fake.endpoint(), "en", wav(3200, 3200))
		test.mutate(&request)
		_, err := transcribe(t, fake, BatchConfig{}, request)
		var providerErr *runtimepkg.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != test.code {
			t.Errorf("%s: error = %v, want %s", name, err, test.code)
		}
		if fake.uploads != 0 {
			t.Errorf("%s uploaded", name)
		}
	}
	fake := newFakeOmiAPI(t, http.StatusOK, verboseResponse)
	if _, err := newTestBatch(t, BatchConfig{}).Transcribe(context.Background(), uploadRequest("https://api.omi.health"+batchPath, "en", wav(3200, 3200))); err == nil {
		t.Fatal("the region-picking host was accepted")
	}
	if fake.uploads != 0 {
		t.Fatal("uploaded to an unlisted host")
	}
}

func TestBatchErrorsAreClassifiedByCode(t *testing.T) {
	t.Parallel()
	cases := []struct {
		status    int
		body      string
		code      string
		retryable bool
	}{
		{401, `{"error":{"code":"unauthorized","message":"vendor words"}}`, "authentication_failed", false},
		{402, `{"error":{"code":"billing_blocked","message":"vendor words"}}`, "provider_quota_exceeded", false},
		{403, `{"error":{"code":"forbidden","message":"vendor words"}}`, "invalid_request", false},
		{429, `{"error":{"code":"queue_full","message":"vendor words"}}`, "provider_rate_limited", true},
		{429, `{"error":{"code":"tenant_upload_concurrency_exceeded","message":"vendor words"}}`, "provider_rate_limited", true},
		{503, `{"error":{"code":"capacity_unavailable","message":"vendor words"}}`, "provider_unavailable", true},
		{503, `{"error":{"code":"service_unavailable","message":"vendor words"}}`, "provider_unavailable", true},
		{413, `{"error":{"code":"payload_too_large","message":"vendor words"}}`, "input_too_large", false},
		{422, `{"error":{"code":"audio_too_long","message":"vendor words"}}`, "input_too_large", false},
		{415, `{"error":{"code":"unsupported_media_type","message":"vendor words"}}`, "unsupported_media", false},
		{422, `{"error":{"code":"invalid_request","message":"Unsupported language 'ja'. vendor words"}}`, "unsupported_language", false},
		{422, `{"error":{"code":"invalid_request","message":"vendor words"}}`, "invalid_request", false},
		{500, `not json`, "provider_unavailable", true},
	}
	for _, test := range cases {
		fake := newFakeOmiAPI(t, test.status, test.body)
		_, err := transcribe(t, fake, BatchConfig{}, uploadRequest(fake.endpoint(), "en", wav(3200, 3200)))
		var providerErr *runtimepkg.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != test.code || providerErr.Retryable != test.retryable || providerErr.ProviderStatus != test.status {
			t.Errorf("%d %s: error = %#v, want %s", test.status, test.body, err, test.code)
			continue
		}
		if strings.Contains(providerErr.Message, "vendor words") || strings.Contains(providerErr.Message, "Unsupported") {
			t.Errorf("%d: vendor message leaked into %q", test.status, providerErr.Message)
		}
	}
}

func TestBatchRefusesARedirect(t *testing.T) {
	t.Parallel()
	var followed atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { followed.Add(1) }))
	t.Cleanup(target.Close)
	redirector := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		http.Redirect(writer, request, target.URL+batchPath, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(redirector.Close)
	adapter := newTestBatch(t, BatchConfig{}, "127.0.0.1")
	_, err := adapter.Transcribe(context.Background(), uploadRequest(redirector.URL+batchPath, "en", wav(3200, 3200)))
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeProviderError || !strings.Contains(providerErr.Message, "redirect") {
		t.Fatalf("error = %#v, want a refused redirect", err)
	}
	if followed.Load() != 0 {
		t.Fatal("the redirect was followed")
	}
}

func TestBatchJobIsLongPolledToItsInlineResult(t *testing.T) {
	t.Parallel()
	fake := newFakeOmiAPI(t, http.StatusAccepted, `{"id":"job_1","status":"queued","poll_url":"/v1/jobs/job_1"}`)
	fake.polls = []fakeAnswer{
		{200, `{"id":"job_1","status":"running"}`},
		{503, `{"error":{"code":"service_unavailable"}}`},
		{429, `{"error":{"code":"queue_full"}}`},
		{200, `{"id":"job_1","status":"succeeded","result":{"inline":true,"content":` + verboseResponse + `}}`},
	}
	result, err := transcribe(t, fake, BatchConfig{}, uploadRequest(fake.endpoint(), "en", wav(3200, 3200)))
	if err != nil {
		t.Fatal(err)
	}
	if result.Text != "Patient reports chest pain." || result.ProviderRequestID != "req_abc123" {
		t.Fatalf("result = %+v", result)
	}
	checkBilling(t, result, 7380, "req_abc123")
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.pollQueries) != 4 {
		t.Fatalf("polls = %d, want 4", len(fake.pollQueries))
	}
	for index, query := range fake.pollQueries {
		if query.Get("wait") != "20" || query.Get("include_result") != "true" || fake.pollAuth[index] != "Bearer "+testKey {
			t.Fatalf("poll %d query = %v auth = %q", index, query, fake.pollAuth[index])
		}
	}
}

func TestBatchJobResultCanBeAJSONString(t *testing.T) {
	t.Parallel()
	fake := newFakeOmiAPI(t, http.StatusAccepted, `{"id":"job_2","status":"queued"}`)
	fake.uploadHeader = http.Header{"Location": {"/v1/jobs/job_2"}}
	quoted := strings.ReplaceAll(strings.ReplaceAll(verboseResponse, `\`, `\\`), `"`, `\"`)
	fake.polls = []fakeAnswer{{200, `{"id":"job_2","status":"succeeded","result":{"content":"` + quoted + `"}}`}}
	result, err := transcribe(t, fake, BatchConfig{}, uploadRequest(fake.endpoint(), "en", wav(3200, 3200)))
	if err != nil {
		t.Fatal(err)
	}
	checkBilling(t, result, 7380, "req_abc123")
}

func TestBatchBearerGoesToOmiButNeverToASignedDownloadURL(t *testing.T) {
	t.Parallel()
	var storageAuth atomic.Value
	storageAuth.Store("unset")
	storage := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		storageAuth.Store(request.Header.Get("Authorization"))
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(verboseResponse))
	}))
	t.Cleanup(storage.Close)

	fake := newFakeOmiAPI(t, http.StatusAccepted, `{"id":"job_3","status":"queued","poll_url":"/v1/jobs/job_3"}`)
	fake.polls = []fakeAnswer{{200, `{"id":"job_3","status":"succeeded","result":{"download_url":"` + storage.URL + `/bucket/job_3.json?X-Amz-Signature=deadbeef"}}`}}
	result, err := transcribe(t, fake, BatchConfig{}, uploadRequest(fake.endpoint(), "en", wav(3200, 3200)))
	if err != nil {
		t.Fatal(err)
	}
	if got := storageAuth.Load(); got != "" {
		t.Fatalf("signed download got Authorization %q", got)
	}
	checkBilling(t, result, 7380, "req_abc123")

	// A result stored on the API's own origin needs the bearer.
	fake = newFakeOmiAPI(t, http.StatusAccepted, `{"id":"job_4","status":"queued","poll_url":"/v1/jobs/job_4"}`)
	fake.results["/v1/results/job_4"] = verboseResponse
	fake.polls = []fakeAnswer{{200, `{"id":"job_4","status":"succeeded","result":{"download_url":"` + fake.server.URL + `/v1/results/job_4"}}`}}
	if _, err := transcribe(t, fake, BatchConfig{}, uploadRequest(fake.endpoint(), "en", wav(3200, 3200))); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.resultAuth) != 1 || fake.resultAuth[0] != "Bearer "+testKey {
		t.Fatalf("API-hosted result auth = %v", fake.resultAuth)
	}
}

func TestBatchFailedDownloadNeverQuotesTheSignedURL(t *testing.T) {
	t.Parallel()
	storage := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusForbidden)
		_, _ = writer.Write([]byte(`<Error>signature for ` + request.URL.String() + `</Error>`))
	}))
	t.Cleanup(storage.Close)
	fake := newFakeOmiAPI(t, http.StatusAccepted, `{"id":"job_5","status":"queued","poll_url":"/v1/jobs/job_5"}`)
	fake.polls = []fakeAnswer{{200, `{"id":"job_5","status":"succeeded","result":{"download_url":"` + storage.URL + `/b/job_5.json?X-Amz-Signature=deadbeef"}}`}}
	_, err := transcribe(t, fake, BatchConfig{}, uploadRequest(fake.endpoint(), "en", wav(3200, 3200)))
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) {
		t.Fatalf("error = %v", err)
	}
	if rendered := fmt.Sprintf("%v %+v %s", err, providerErr, marshalData(providerErr.Extensions)); strings.Contains(rendered, "deadbeef") {
		t.Fatalf("the signed URL leaked: %s", rendered)
	}

	// A transport failure on the download drops the URL from its cause.
	storage.Close()
	_, err = transcribe(t, fake, BatchConfig{}, uploadRequest(fake.endpoint(), "en", wav(3200, 3200)))
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeUnavailable {
		t.Fatalf("error = %#v", err)
	}
	if rendered := fmt.Sprintf("%v %v", err, providerErr.Cause); strings.Contains(rendered, "deadbeef") {
		t.Fatalf("the signed URL leaked: %s", rendered)
	}
}

func TestBatchRefusesAPollURLOffTheAPIOrigin(t *testing.T) {
	t.Parallel()
	var called atomic.Int32
	elsewhere := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called.Add(1) }))
	t.Cleanup(elsewhere.Close)
	fake := newFakeOmiAPI(t, http.StatusAccepted, `{"id":"job_6","status":"queued","poll_url":"`+elsewhere.URL+`/v1/jobs/job_6"}`)
	_, err := transcribe(t, fake, BatchConfig{}, uploadRequest(fake.endpoint(), "en", wav(3200, 3200)))
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeProviderError {
		t.Fatalf("error = %#v", err)
	}
	if called.Load() != 0 {
		t.Fatal("the off-origin poll URL was called")
	}
}

func TestBatchJobDeadlineIsARequestTimeout(t *testing.T) {
	t.Parallel()
	fake := newFakeOmiAPI(t, http.StatusAccepted, `{"id":"job_7","status":"queued","poll_url":"/v1/jobs/job_7"}`)
	fake.polls = []fakeAnswer{{200, `{"id":"job_7","status":"running"}`}}
	started := time.Now()
	_, err := transcribe(t, fake, BatchConfig{PollDeadline: 150 * time.Millisecond}, uploadRequest(fake.endpoint(), "en", wav(3200, 3200)))
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeRequestTimeout || !strings.Contains(providerErr.Message, "did not finish") {
		t.Fatalf("error = %#v", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("deadline took %s", elapsed)
	}
}

func TestBatchFailedJobsAreClassified(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		`{"id":"j","status":"failed","error":{"code":"queue_full","message":"vendor words"}}`:      batchhttp.CodeRateLimited,
		`{"id":"j","status":"failed","error":{"code":"audio_too_long","message":"vendor words"}}`:  batchhttp.CodeInputTooLarge,
		`{"id":"j","status":"failed","error":{"code":"decoder_crashed","message":"vendor words"}}`: batchhttp.CodeProviderError,
		`{"id":"j","status":"failed"}`:                                                    batchhttp.CodeProviderError,
		`{"id":"j","status":"expired"}`:                                                   batchhttp.CodeProviderError,
		`{"id":"j","status":"succeeded","result":{"expired":true}}`:                       batchhttp.CodeProviderError,
		`{"id":"j","status":"succeeded","result":{}}`:                                     batchhttp.CodeProviderError,
		`{"id":"j","status":"mystery"}`:                                                   batchhttp.CodeProviderError,
		`{"id":"j","status":"succeeded","result":{"content":{"duration":1.0}}}`:           batchhttp.CodeProviderError,
		`{"id":"j","status":"succeeded","result":{"download_url":"ftp://example.com/x"}}`: batchhttp.CodeProviderError,
	}
	for body, code := range cases {
		fake := newFakeOmiAPI(t, http.StatusAccepted, `{"id":"j","status":"queued","poll_url":"/v1/jobs/j"}`)
		fake.polls = []fakeAnswer{{200, body}}
		_, err := transcribe(t, fake, BatchConfig{}, uploadRequest(fake.endpoint(), "en", wav(3200, 3200)))
		var providerErr *runtimepkg.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != code || strings.Contains(providerErr.Message, "vendor words") {
			t.Errorf("%s: error = %#v, want %s", body, err, code)
		}
	}
}

func TestBatchConstantsDescribeTheUSRoute(t *testing.T) {
	t.Parallel()
	parsed, err := url.Parse(BatchEndpoint)
	if err != nil || parsed.Host != officialHost || parsed.Path != batchPath {
		t.Fatalf("BatchEndpoint = %s", BatchEndpoint)
	}
	if BatchModel != Model || BatchMaxAudioBytes > 100_000_000 || BatchMaxDurationSeconds != 7200 {
		t.Fatalf("limits = %d bytes, %d s", BatchMaxAudioBytes, BatchMaxDurationSeconds)
	}
	// The byte cap binds before the duration cap at every rate the relay
	// carries.
	if (BatchMaxAudioBytes-44)/(8_000*2) > BatchMaxDurationSeconds {
		t.Fatal("the duration cap binds before the byte cap at 8 kHz")
	}
}
