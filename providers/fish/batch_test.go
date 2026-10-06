package fish

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/SpekoAI/gateway/internal/batchhttp"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

func batchPlan(endpoint, model string) protocol.SessionPlan {
	return protocol.SessionPlan{
		Execution: protocol.Execution{ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged},
		Route: protocol.PlanRoute{
			Provider: "fish", Model: model, Endpoint: endpoint, Transport: protocol.TransportHTTP,
			Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "fish-key"},
		},
	}
}

func batchServer(t *testing.T, body string, inspect func(*http.Request)) (*BatchAdapter, string) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if inspect != nil {
			inspect(r)
		} else {
			_, _ = io.Copy(io.Discard, r.Body)
		}
		w.Header().Set("x-fish-trace-id", "trace-1")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	return adapter, server.URL + "/v1/asr"
}

func transcribe(t *testing.T, adapter *BatchAdapter, endpoint, model string, options protocol.RequestOptions) (*runtimepkg.BatchTranscription, error) {
	t.Helper()
	return adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
		Plan: batchPlan(endpoint, model), Options: options, Audio: strings.NewReader("RIFF....WAVE"), AudioBytes: 12,
	})
}

// The response is the shape the live API returned on 2026-09-28 for a
// two-speaker clip, with an emotion cue added to the second turn.
const proResponse = `{"duration":7.988375,"language":"English","language_code":"en","segments":[` +
	`{"end":0.4,"start":0.0,"text":"Hi"},{"end":0.96,"start":0.64,"text":"thanks"},{"end":1.12,"start":0.96,"text":"for"},{"end":1.52,"start":1.12,"text":"calling"},{"end":2.08,"start":1.52,"text":"Speko"},` +
	`{"end":4.48,"start":4.32,"text":"I'd"},{"end":4.88,"start":4.64,"text":"like"},{"end":5.04,"start":4.96,"text":"to"},{"end":5.36,"start":5.04,"text":"check"}],` +
	`"text":"<|speaker:0|> Hi, thanks for calling Speko. <|speaker:1|> [laughter] I'd like to check."}`

func TestBatchTranscribeUsesASRContract(t *testing.T) {
	t.Parallel()
	adapter, endpoint := batchServer(t, proResponse, func(r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer fish-key" {
			t.Errorf("authorization = %q", got)
		}
		if got := r.Header.Get("model"); got != BatchModel {
			t.Errorf("model header = %q", got)
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Errorf("multipart: %v", err)
			return
		}
		if _, header, err := r.FormFile("audio"); err != nil || header.Filename != "audio.wav" {
			t.Errorf("audio part: %v %v", header, err)
		}
		for key, want := range map[string]string{"ignore_timestamps": "false", "language": "en"} {
			if values := r.MultipartForm.Value[key]; len(values) != 1 || values[0] != want {
				t.Errorf("form %s = %v, want %s", key, values, want)
			}
		}
		if _, ok := r.MultipartForm.Value["model"]; ok {
			t.Errorf("model must travel in the header, not the form")
		}
	})
	diarize := true
	result, err := transcribe(t, adapter, endpoint, BatchModel, protocol.RequestOptions{Language: "en-US", STT: &protocol.SttOptions{Diarization: &diarize}})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	if want := "Hi, thanks for calling Speko. [laughter] I'd like to check."; result.Text != want {
		t.Fatalf("text = %q, want %q (markers stripped, cue kept)", result.Text, want)
	}
	if len(result.Segments) != 2 {
		t.Fatalf("segments = %+v", result.Segments)
	}
	first, second := result.Segments[0], result.Segments[1]
	if first.Speaker != "0" || first.Text != "Hi, thanks for calling Speko." || first.StartMS != 0 || first.EndMS != 2080 {
		t.Fatalf("first segment = %+v", first)
	}
	if second.Speaker != "1" || second.Text != "[laughter] I'd like to check." || second.StartMS != 4320 || second.EndMS != 5360 {
		t.Fatalf("second segment = %+v", second)
	}
	if result.Language != "en" || result.DurationMS != 7988 || result.ProviderRequestID != "trace-1" {
		t.Fatalf("result = %+v", result)
	}
	if result.Words != nil {
		t.Fatalf("words = %+v, want none when word_timestamps was not asked", result.Words)
	}
}

func TestBatchBillingReportsFishProcessedDuration(t *testing.T) {
	t.Parallel()
	adapter, endpoint := batchServer(t, proResponse, nil)
	result, err := transcribe(t, adapter, endpoint, BatchModel, protocol.RequestOptions{})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	if result.Billing == nil || !result.Billing.Complete || len(result.Billing.Operations) != 1 {
		t.Fatalf("billing = %+v", result.Billing)
	}
	op := result.Billing.Operations[0]
	if err := op.Validate(); err != nil {
		t.Fatalf("observation: %v", err)
	}
	if op.Model != BatchModel || op.Mode != "batch" || op.ProviderRequestID != "trace-1" {
		t.Fatalf("observation = %+v", op)
	}
	// Fractional milliseconds fall through to the nano-scaled quantity.
	if op.Quantities["duration_seconds"] != 7_988_375_000 || op.QuantityDenominators["duration_seconds"] != 1_000_000_000 {
		t.Fatalf("duration = %d/%d, want 7.988375 s exactly", op.Quantities["duration_seconds"], op.QuantityDenominators["duration_seconds"])
	}
}

func TestBatchSpeakersOnlyWhenDiarizationAsked(t *testing.T) {
	t.Parallel()
	adapter, endpoint := batchServer(t, proResponse, nil)
	words := true
	result, err := transcribe(t, adapter, endpoint, BatchModel, protocol.RequestOptions{STT: &protocol.SttOptions{WordTimestamps: &words}})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	for _, segment := range result.Segments {
		if segment.Speaker != "" {
			t.Fatalf("segment speaker %q without a diarization ask", segment.Speaker)
		}
	}
	if len(result.Words) != 9 || result.Words[0].Speaker != "" {
		t.Fatalf("words = %+v", result.Words)
	}

	diarize := true
	result, err = transcribe(t, adapter, endpoint, BatchModel, protocol.RequestOptions{STT: &protocol.SttOptions{WordTimestamps: &words, Diarization: &diarize}})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	if result.Words[0].Speaker != "0" || result.Words[8].Speaker != "1" || result.Words[8].Text != "check" {
		t.Fatalf("words = %+v", result.Words)
	}
}

// Chinese comes back one character per segment. The segment text is the
// turn's own span, so it keeps punctuation and gains no spaces.
func TestBatchAlignsCharacterSegmentsWithoutSpaces(t *testing.T) {
	t.Parallel()
	body := `{"duration":6.4,"language_code":"zh","segments":[` +
		`{"start":0,"end":0.2,"text":"你"},{"start":0.2,"end":0.5,"text":"好"},` +
		`{"start":1.0,"end":1.2,"text":"很"},{"start":1.2,"end":1.4,"text":"开"},{"start":1.4,"end":1.6,"text":"心"},` +
		`{"start":3.0,"end":3.2,"text":"2026"},{"start":3.2,"end":3.4,"text":"年"}],` +
		`"text":"<|speaker:0|>你好。<|speaker:1|>[高兴]很开心。 <|speaker:0|>2026年。"}`
	adapter, endpoint := batchServer(t, body, nil)
	diarize := true
	result, err := transcribe(t, adapter, endpoint, BatchModel, protocol.RequestOptions{STT: &protocol.SttOptions{Diarization: &diarize}})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	want := []runtimepkg.BatchSegment{
		{Text: "你好。", StartMS: 0, EndMS: 500, Speaker: "0"},
		{Text: "[高兴]很开心。", StartMS: 1000, EndMS: 1600, Speaker: "1"},
		{Text: "2026年。", StartMS: 3000, EndMS: 3400, Speaker: "0"},
	}
	if len(result.Segments) != len(want) {
		t.Fatalf("segments = %+v", result.Segments)
	}
	for i := range want {
		if result.Segments[i] != want[i] {
			t.Fatalf("segment %d = %+v, want %+v", i, result.Segments[i], want[i])
		}
	}
	if result.Text != "你好。 [高兴]很开心。 2026年。" {
		t.Fatalf("text = %q", result.Text)
	}
}

func TestBatchSplitsATurnOnLongPauses(t *testing.T) {
	t.Parallel()
	body := `{"duration":5,"segments":[{"start":0,"end":0.4,"text":"One"},{"start":0.5,"end":0.9,"text":"two"},{"start":3.0,"end":3.4,"text":"three"}],"text":"One, two. Three."}`
	adapter, endpoint := batchServer(t, body, nil)
	result, err := transcribe(t, adapter, endpoint, BatchModelStandard, protocol.RequestOptions{})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	if len(result.Segments) != 2 || result.Segments[0].Text != "One, two." || result.Segments[1].Text != "Three." {
		t.Fatalf("segments = %+v", result.Segments)
	}
}

// A word the text does not contain means the two disagree; speakers are then
// withheld rather than guessed.
func TestBatchMisalignmentFallsBackWithoutSpeakers(t *testing.T) {
	t.Parallel()
	body := `{"duration":2,"segments":[{"start":0,"end":0.4,"text":"Hello"},{"start":0.5,"end":0.9,"text":"world"}],"text":"<|speaker:0|> Hello <|speaker:1|> there"}`
	adapter, endpoint := batchServer(t, body, nil)
	diarize := true
	result, err := transcribe(t, adapter, endpoint, BatchModel, protocol.RequestOptions{STT: &protocol.SttOptions{Diarization: &diarize}})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	if len(result.Segments) != 1 || result.Segments[0].Speaker != "" || result.Segments[0].Text != "Hello world" {
		t.Fatalf("segments = %+v", result.Segments)
	}
}

// Fish returns 200 with text but no segments past ~290 s, and Pro cuts text
// at 5040 characters. Neither can be verified as complete.
func TestBatchRefusesUnverifiableTranscripts(t *testing.T) {
	t.Parallel()
	cases := map[string]struct{ model, body string }{
		"no segments": {BatchModelStandard, `{"duration":301,"segments":[],"text":"Hello there."}`},
		"pro cap":     {BatchModel, `{"duration":200,"segments":[{"start":0,"end":0.4,"text":"a"}],"text":"` + strings.Repeat("a", proTextCapRunes) + `"}`},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			adapter, endpoint := batchServer(t, c.body, nil)
			_, err := transcribe(t, adapter, endpoint, c.model, protocol.RequestOptions{})
			var providerErr *runtimepkg.ProviderError
			if !errors.As(err, &providerErr) || !providerErr.Retryable {
				t.Fatalf("err = %v, want a retryable provider error", err)
			}
		})
	}
}

func TestBatchSilenceIsAnEmptySuccess(t *testing.T) {
	t.Parallel()
	adapter, endpoint := batchServer(t, `{"duration":3.2,"segments":[],"text":""}`, nil)
	result, err := transcribe(t, adapter, endpoint, BatchModel, protocol.RequestOptions{})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	if result.Text != "" || len(result.Segments) != 0 || result.DurationMS != 3200 {
		t.Fatalf("result = %+v", result)
	}
}

// Fish answers an unknown `model` header by running transcribe-1, so the
// adapter must refuse before sending anything.
func TestBatchRejectsUnknownModelBeforeSending(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	adapter, endpoint := batchServer(t, proResponse, func(r *http.Request) { calls.Add(1) })
	if _, err := transcribe(t, adapter, endpoint, "transcribe-2", protocol.RequestOptions{}); err == nil {
		t.Fatalf("unknown model accepted")
	}
	if calls.Load() != 0 {
		t.Fatalf("request sent for an unknown model")
	}
}

func TestBatchMapsStatusAndSizeErrors(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"status":401,"message":"Invalid Token"}`))
	}))
	defer server.Close()
	adapter, _ := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	_, err := transcribe(t, adapter, server.URL+"/v1/asr", BatchModel, protocol.RequestOptions{})
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeAuthenticationFailed {
		t.Fatalf("401: %v", err)
	}

	_, err = adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(BatchEndpoint, BatchModel), Audio: strings.NewReader("x"), AudioBytes: BatchMaxAudioBytes + 1})
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeInputTooLarge {
		t.Fatalf("oversized: %v", err)
	}
}

// A turn with no timed words (a lone cue, or trailing text Fish did not
// align) still reaches the segments, at the point the previous speech ended,
// with its speaker.
func TestBatchKeepsUntimedTurnsAsPointSegments(t *testing.T) {
	t.Parallel()
	body := `{"duration":4,"segments":[{"start":0,"end":0.4,"text":"Hi"},{"start":0.5,"end":0.9,"text":"there"}],` +
		`"text":"<|speaker:0|> Hi there. <|speaker:1|> [laughter] <|speaker:0|> Okay"}`
	adapter, endpoint := batchServer(t, body, nil)
	diarize := true
	result, err := transcribe(t, adapter, endpoint, BatchModel, protocol.RequestOptions{STT: &protocol.SttOptions{Diarization: &diarize}})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	want := []runtimepkg.BatchSegment{
		{Text: "Hi there.", StartMS: 0, EndMS: 900, Speaker: "0"},
		{Text: "[laughter]", StartMS: 900, EndMS: 900, Speaker: "1"},
		{Text: "Okay", StartMS: 900, EndMS: 900, Speaker: "0"},
	}
	if len(result.Segments) != len(want) {
		t.Fatalf("segments = %+v", result.Segments)
	}
	for i := range want {
		if result.Segments[i] != want[i] {
			t.Fatalf("segment %d = %+v, want %+v", i, result.Segments[i], want[i])
		}
	}
}

func TestBatchRefusesAnEmptySuccessBody(t *testing.T) {
	t.Parallel()
	adapter, endpoint := batchServer(t, `{}`, nil)
	_, err := transcribe(t, adapter, endpoint, BatchModelStandard, protocol.RequestOptions{})
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeProviderError {
		t.Fatalf("empty body: %v", err)
	}
}

// Fish takes only a language hint. Every other ask is refused before upload
// rather than silently dropped.
func TestBatchRefusesAsksFishCannotHonour(t *testing.T) {
	t.Parallel()
	on := true
	cases := map[string]struct {
		model   string
		options protocol.RequestOptions
	}{
		"diarization on transcribe-1": {BatchModelStandard, protocol.RequestOptions{STT: &protocol.SttOptions{Diarization: &on}}},
		"keywords":                    {BatchModel, protocol.RequestOptions{STT: &protocol.SttOptions{Keywords: []string{"Speko"}}}},
		"noise reduction":             {BatchModel, protocol.RequestOptions{STT: &protocol.SttOptions{NoiseReduction: &on}}},
		"translation":                 {BatchModel, protocol.RequestOptions{STT: &protocol.SttOptions{Translation: &protocol.SttTranslation{TargetLanguage: "es"}}}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			adapter, endpoint := batchServer(t, proResponse, func(*http.Request) { calls.Add(1) })
			_, err := transcribe(t, adapter, endpoint, c.model, c.options)
			var providerErr *runtimepkg.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeInvalidRequest {
				t.Fatalf("err = %v, want invalid_request", err)
			}
			if calls.Load() != 0 {
				t.Fatal("request sent for an ask Fish cannot honour")
			}
		})
	}
}

func TestBatchRefusesAnotherPathOnTheFishHost(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	adapter, endpoint := batchServer(t, proResponse, func(*http.Request) { calls.Add(1) })
	if _, err := transcribe(t, adapter, strings.TrimSuffix(endpoint, "/v1/asr")+"/v1/tts", BatchModel, protocol.RequestOptions{}); err == nil {
		t.Fatal("a route to /v1/tts was accepted")
	}
	if calls.Load() != 0 {
		t.Fatal("request sent to the wrong path")
	}
}

func TestBatchRefusesAudioOverTheDurationLimit(t *testing.T) {
	t.Parallel()
	adapter, endpoint := batchServer(t, proResponse, nil)
	media := protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1}
	bytes := int64(wavHeaderAllowance + (BatchMaxDurationSeconds+1)*32_000)
	_, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
		Plan: batchPlan(endpoint, BatchModel), Media: media, Audio: strings.NewReader("RIFF"), AudioBytes: bytes,
	})
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeInputTooLarge {
		t.Fatalf("over-long recording: %v", err)
	}
}

// A recording exactly at the limit with a longer-than-canonical header (a
// LIST chunk, say) is still accepted.
func TestBatchAcceptsAChunkAtTheLimitWithALongHeader(t *testing.T) {
	t.Parallel()
	adapter, endpoint := batchServer(t, proResponse, nil)
	media := protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1}
	bytes := int64(4096 + BatchMaxDurationSeconds*32_000)
	if _, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
		Plan: batchPlan(endpoint, BatchModel), Media: media, Audio: strings.NewReader("RIFF"), AudioBytes: bytes,
	}); err != nil {
		t.Fatalf("a %d s chunk with a 4 KiB header was refused: %v", BatchMaxDurationSeconds, err)
	}
}
