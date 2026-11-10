package elevenlabs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/SpekoAI/gateway/internal/batchhttp"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

func batchPlan(endpoint string) protocol.SessionPlan {
	return protocol.SessionPlan{
		Execution: protocol.Execution{ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged},
		Route: protocol.PlanRoute{
			Provider: "elevenlabs", Model: BatchModel, Endpoint: endpoint, Transport: protocol.TransportHTTP,
			Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "xi-key"},
		},
	}
}

func TestBatchTranscribeUsesScribeFileContract(t *testing.T) {
	t.Parallel()
	diarize := true
	type observed struct {
		header http.Header
		form   map[string][]string
		file   string
	}
	seen := make(chan observed, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Errorf("multipart: %v", err)
		}
		file, header, err := r.FormFile("file")
		var content []byte
		if err == nil {
			content, _ = io.ReadAll(file)
			if header.Filename != "audio.wav" || header.Header.Get("Content-Type") != "audio/wav" {
				t.Errorf("file header = %v", header.Header)
			}
		}
		seen <- observed{header: r.Header.Clone(), form: r.MultipartForm.Value, file: string(content)}
		w.Header().Set("request-id", "rid-1")
		_, _ = w.Write([]byte(`{"language_code":"en","language_probability":0.99,"text":"Hello there. Hi.","transcription_id":"tx_1","audio_duration_secs":1823.17,"words":[{"text":"Hello","type":"word","start":0.1,"end":0.4,"speaker_id":"speaker_0"},{"text":" ","type":"spacing","start":0.4,"end":0.45},{"text":"there.","type":"word","start":0.45,"end":0.8,"speaker_id":"speaker_0"},{"text":"(laughs)","type":"audio_event","start":0.8,"end":0.9},{"text":"Hi.","type":"word","start":1.0,"end":1.2,"speaker_id":"speaker_1"}]}`))
	}))
	defer server.Close()

	adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	result, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
		Plan:       batchPlan(server.URL + "/v1/speech-to-text"),
		Options:    protocol.RequestOptions{Language: "en-GB", STT: &protocol.SttOptions{Diarization: &diarize, Keywords: []string{"Speko", "Router"}, ProviderOptions: map[string]map[string]any{"elevenlabs": {"num_speakers": 2}}}},
		Audio:      strings.NewReader("RIFF....WAVE"),
		AudioBytes: 12,
	})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	got := <-seen
	if got.header.Get("xi-api-key") != "xi-key" {
		t.Fatalf("xi-api-key = %q", got.header.Get("xi-api-key"))
	}
	if got.file != "RIFF....WAVE" {
		t.Fatalf("file = %q", got.file)
	}
	for key, want := range map[string]string{"model_id": BatchModel, "diarize": "true", "timestamps_granularity": "word", "tag_audio_events": "false", "language_code": "en", "num_speakers": "2"} {
		if values := got.form[key]; len(values) != 1 || values[0] != want {
			t.Fatalf("form %s = %v, want %q", key, values, want)
		}
	}
	if strings.Join(got.form["keyterms"], ",") != "Speko,Router" {
		t.Fatalf("keyterms = %v", got.form["keyterms"])
	}
	if result.Text != "Hello there. Hi." || result.Language != "en" || result.DurationMS != 1823170 || result.ProviderRequestID != "tx_1" {
		t.Fatalf("result = %+v", result)
	}
	if len(result.Segments) != 2 || result.Segments[0] != (runtimepkg.BatchSegment{Text: "Hello there.", StartMS: 100, EndMS: 800, Speaker: "speaker_0"}) || result.Segments[1].Speaker != "speaker_1" {
		t.Fatalf("segments = %+v", result.Segments)
	}
	// Batch Scribe bills apart from realtime, so the observation names the
	// batch model and mode, and Scribe's own processed duration.
	want := protocol.BillingReport{Revision: protocol.BillingRevision, Complete: true, Operations: []protocol.BillingObservation{{
		OperationID: "request", Model: BatchModel, Mode: "batch", ProviderRequestID: "tx_1",
		Quantities: map[string]int64{"duration_seconds": 1_823_170}, Complete: true,
	}}}
	if result.Billing == nil || !reflect.DeepEqual(*result.Billing, want) {
		t.Fatalf("billing = %+v, want %+v", result.Billing, want)
	}
	if err := result.Billing.Validate(); err != nil {
		t.Fatalf("billing invalid: %v", err)
	}
}

// Without audio_duration_secs there is no vendor quantity, and the uploaded
// bytes are not substituted for it.
func TestBatchBillingStaysIncompleteWithoutTheVendorDuration(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("request-id", "rid-2")
		_, _ = w.Write([]byte(`{"language_code":"en","text":"Hi.","words":[]}`))
	}))
	defer server.Close()
	adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	result, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(server.URL + "/v1/speech-to-text"), Audio: strings.NewReader("RIFF....WAVE"), AudioBytes: 12})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	want := protocol.BillingReport{Revision: protocol.BillingRevision, Operations: []protocol.BillingObservation{{
		OperationID: "request", Model: BatchModel, Mode: "batch", ProviderRequestID: "rid-2", Quantities: map[string]int64{},
	}}}
	if result.Billing == nil || !reflect.DeepEqual(*result.Billing, want) {
		t.Fatalf("billing = %+v, want %+v", result.Billing, want)
	}
}

func TestBatchTranscribeRefusals(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"detail":{"status":"invalid_model_id"}}`))
	}))
	defer server.Close()
	adapter, _ := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	base := runtimepkg.BatchTranscribeRequest{Plan: batchPlan(server.URL + "/v1/speech-to-text"), Audio: strings.NewReader("x"), AudioBytes: 1}

	_, err := adapter.Transcribe(context.Background(), base)
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeInvalidRequest || providerErr.Retryable {
		t.Fatalf("422: %v", err)
	}
	realtime := base
	realtime.Plan.Route.Model = "scribe_v2_realtime"
	if _, err := adapter.Transcribe(context.Background(), realtime); err == nil {
		t.Fatal("realtime model accepted")
	}
	huge := base
	huge.AudioBytes = BatchMaxAudioBytes + 1
	if _, err := adapter.Transcribe(context.Background(), huge); !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeInputTooLarge {
		t.Fatalf("oversized: %v", err)
	}
}

func TestBatchTranscribeAliasesNorwegianToScribeCode(t *testing.T) {
	t.Parallel()
	seen := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Errorf("multipart: %v", err)
		}
		seen <- r.FormValue("language_code")
		_, _ = w.Write([]byte(`{"language_code":"nor","text":"Hei.","transcription_id":"tx_1","audio_duration_secs":1}`))
	}))
	defer server.Close()

	adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	if _, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
		Plan:       batchPlan(server.URL + "/v1/speech-to-text"),
		Options:    protocol.RequestOptions{Language: "nb-NO"},
		Audio:      strings.NewReader("RIFF....WAVE"),
		AudioBytes: 12,
	}); err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	if got := <-seen; got != "nor" {
		t.Fatalf("language_code = %q, want nor", got)
	}
}

// testWAV builds a 16-bit PCM WAV container. A LIST chunk precedes data so the
// chunk walk is exercised, and dataSize overrides the declared data length.
func testWAV(pcmBytes int, dataSize uint32) []byte {
	var buf bytes.Buffer
	put32 := func(v uint32) { _ = binary.Write(&buf, binary.LittleEndian, v) }
	put16 := func(v uint16) { _ = binary.Write(&buf, binary.LittleEndian, v) }
	buf.WriteString("RIFF")
	put32(uint32(4 + 8 + 16 + 8 + 4 + 8 + pcmBytes))
	buf.WriteString("WAVE")
	buf.WriteString("fmt ")
	put32(16)
	put16(1)
	put16(1)
	put32(16_000)
	put32(32_000)
	put16(2)
	put16(16)
	buf.WriteString("LIST")
	put32(4)
	buf.WriteString("INFO")
	buf.WriteString("data")
	put32(dataSize)
	buf.Write(make([]byte, pcmBytes))
	return buf.Bytes()
}

// Scribe documents audio_duration_secs as optional. Without it, a finished
// (200) batch observation takes the length of the uploaded PCM: wall-clock
// for an ordinary upload, channel-seconds with use_multi_channel, which Scribe
// bills per channel. A 202 acceptance or an encoding it cannot measure stays
// incomplete, and webhook mode is refused before anything is sent.
func TestBatchTranscribeBillsTheUploadWhenScribeOmitsTheDuration(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name            string
		audio           []byte
		encoding        string
		channels        int
		providerOptions map[string]any
		status          int
		wantErr         bool
		complete        bool
		want, wantDen   int64
	}{
		{name: "declared data chunk", audio: testWAV(48_000, 48_000), encoding: "pcm_s16le", channels: 1, status: 200, complete: true, want: 48_000, wantDen: 32_000},
		{name: "open-ended data size", audio: testWAV(48_000, 0xFFFFFFFF), encoding: "pcm_s16le", channels: 1, status: 200, complete: true, want: 48_000, wantDen: 32_000},
		{name: "stereo wall clock", audio: testWAV(64_000, 64_000), encoding: "pcm_s16le", channels: 2, status: 200, complete: true, want: 64_000, wantDen: 64_000},
		{name: "stereo multichannel bills each channel", audio: testWAV(64_000, 64_000), encoding: "pcm_s16le", channels: 2, providerOptions: map[string]any{"use_multi_channel": true}, status: 200, complete: true, want: 64_000, wantDen: 32_000},
		{name: "not PCM", audio: testWAV(48_000, 48_000), encoding: "opus", channels: 1, status: 200},
		{name: "202 acceptance", audio: testWAV(48_000, 48_000), encoding: "pcm_s16le", channels: 1, status: 202},
		{name: "webhook refused", audio: testWAV(48_000, 48_000), encoding: "pcm_s16le", channels: 1, providerOptions: map[string]any{"webhook": true}, status: 200, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				_ = r.ParseMultipartForm(1 << 20)
				w.Header().Set("request-id", "rid-nodur")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(`{"language_code":"en","language_probability":0.99,"text":"Hi.","transcription_id":"tx_nodur","words":[]}`))
			}))
			defer server.Close()
			adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
			if err != nil {
				t.Fatalf("adapter: %v", err)
			}
			options := protocol.RequestOptions{}
			if tc.providerOptions != nil {
				options.STT = &protocol.SttOptions{ProviderOptions: map[string]map[string]any{"elevenlabs": tc.providerOptions}}
			}
			result, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
				Plan:       batchPlan(server.URL + "/v1/speech-to-text"),
				Options:    options,
				Media:      protocol.MediaFormat{Encoding: tc.encoding, SampleRateHz: 16_000, Channels: tc.channels},
				Audio:      bytes.NewReader(tc.audio),
				AudioBytes: int64(len(tc.audio)),
			})
			if tc.wantErr {
				if err == nil || requests != 0 {
					t.Fatalf("err = %v after %d requests, want a refusal before any request", err, requests)
				}
				return
			}
			if err != nil {
				t.Fatalf("transcribe: %v", err)
			}
			got := result.Billing.Operations[0]
			if got.Complete != tc.complete || result.Billing.Complete != tc.complete {
				t.Fatalf("complete = %v (report %v), want %v", got.Complete, result.Billing.Complete, tc.complete)
			}
			if !tc.complete {
				return
			}
			if got.Quantities["duration_seconds"] != tc.want || got.QuantityDenominators["duration_seconds"] != tc.wantDen || got.Model != BatchModel || got.Mode != "batch" {
				t.Fatalf("observation = %+v, want %d/%d scribe_v2 batch", got, tc.want, tc.wantDen)
			}
		})
	}
}

// Each realtime Scribe generation has its own file twin. A v3 Live session
// that needs a file transcription runs scribe_v3, v2 Realtime keeps
// scribe_v2, and an id that is not a realtime Scribe model has no twin, so the
// lookup never guesses a generation for an unknown id.
func TestBatchModelForPairsByGeneration(t *testing.T) {
	t.Parallel()
	if BatchModel != "scribe_v2" || BatchModelV3 != "scribe_v3" {
		t.Fatalf("BatchModel = %q, BatchModelV3 = %q; want scribe_v2 and scribe_v3", BatchModel, BatchModelV3)
	}
	for _, tc := range []struct {
		realtime string
		want     string
		ok       bool
	}{
		{realtime: "scribe_v2_realtime", want: "scribe_v2", ok: true},
		{realtime: "scribe_v3_live", want: "scribe_v3", ok: true},
		{realtime: " scribe_v3_live ", want: "scribe_v3", ok: true},
		{realtime: "scribe_v2"},
		{realtime: "scribe_v3"},
		{realtime: "scribe_v1"},
		{realtime: "auto"},
		{realtime: ""},
	} {
		got, ok := BatchModelFor(tc.realtime)
		if got != tc.want || ok != tc.ok {
			t.Errorf("BatchModelFor(%q) = %q, %v; want %q, %v", tc.realtime, got, ok, tc.want, tc.ok)
		}
	}
}

// The pairing table and the batch guard must agree: every realtime id that
// has a file twin is refused by the guard, and every file twin passes it. A
// future realtime id with a new spelling fails here until the guard learns it.
func TestRealtimeBatchModelsAgreeWithTheBatchGuard(t *testing.T) {
	t.Parallel()
	if len(realtimeBatchModels) == 0 {
		t.Fatal("realtimeBatchModels is empty")
	}
	for realtime, batch := range realtimeBatchModels {
		if !isRealtimeModel(realtime) {
			t.Errorf("isRealtimeModel(%q) = false; the batch guard would send a realtime id to the file endpoint", realtime)
		}
		if isRealtimeModel(batch) {
			t.Errorf("isRealtimeModel(%q) = true; the batch guard would refuse the file twin of %q", batch, realtime)
		}
	}
}

// Regression: the guard looked only for "realtime", so scribe_v3_live (the
// v3 realtime id) would have been POSTed to the file endpoint. Every realtime
// spelling is refused before a request leaves.
func TestBatchTranscribeRefusesEveryRealtimeScribeModelBeforeSending(t *testing.T) {
	t.Parallel()
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte(`{"language_code":"en","text":"","transcription_id":"tx_1","audio_duration_secs":1}`))
	}))
	defer server.Close()
	adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	// The last three are not shipped ids. They pin both spellings of the rule
	// apart from the shipped ids, so a later realtime generation is refused
	// without another guard change.
	for _, model := range []string{"scribe_v2_realtime", "scribe_v3_live", " scribe_v3_live ", "", "  ", "scribe_v4_realtime", "scribe_v4_live", "scribe_v3_live_preview"} {
		request := runtimepkg.BatchTranscribeRequest{Plan: batchPlan(server.URL + "/v1/speech-to-text"), Audio: strings.NewReader("x"), AudioBytes: 1}
		request.Plan.Route.Model = model
		if _, err := adapter.Transcribe(context.Background(), request); err == nil || !strings.Contains(err.Error(), "requires a file Scribe model") {
			t.Errorf("model %q: err = %v, want a file-model refusal", model, err)
		}
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("file endpoint received %d requests for realtime models, want 0", n)
	}
}

// Both file generations go to Scribe verbatim, and each names its own billing
// observation. Adding v3 must not move what a v2 upload sends or bills.
func TestBatchTranscribeSendsEachGenerationsFileModelVerbatim(t *testing.T) {
	t.Parallel()
	for _, model := range []string{BatchModel, BatchModelV3} {
		t.Run(model, func(t *testing.T) {
			t.Parallel()
			seen := make(chan string, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := r.ParseMultipartForm(1 << 20); err != nil {
					t.Errorf("multipart: %v", err)
				}
				seen <- r.FormValue("model_id")
				_, _ = w.Write([]byte(`{"language_code":"en","text":"Hi.","transcription_id":"tx_1","audio_duration_secs":2}`))
			}))
			defer server.Close()
			adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
			if err != nil {
				t.Fatalf("adapter: %v", err)
			}
			request := runtimepkg.BatchTranscribeRequest{Plan: batchPlan(server.URL + "/v1/speech-to-text"), Audio: strings.NewReader("RIFF....WAVE"), AudioBytes: 12}
			request.Plan.Route.Model = model
			result, err := adapter.Transcribe(context.Background(), request)
			if err != nil {
				t.Fatalf("transcribe: %v", err)
			}
			if got := <-seen; got != model {
				t.Fatalf("model_id = %q, want %q", got, model)
			}
			want := protocol.BillingReport{Revision: protocol.BillingRevision, Complete: true, Operations: []protocol.BillingObservation{{
				OperationID: "request", Model: model, Mode: "batch", ProviderRequestID: "tx_1",
				Quantities: map[string]int64{"duration_seconds": 2_000}, Complete: true,
			}}}
			if result.Billing == nil || !reflect.DeepEqual(*result.Billing, want) {
				t.Fatalf("billing = %+v, want %+v", result.Billing, want)
			}
		})
	}
}
