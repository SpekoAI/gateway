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
	server, captured := newFakeInteractions(t, http.StatusOK, `{"id":"interactions/abc","steps":[{"type":"user_input","content":[{"type":"audio"}]},{"type":"model_output","content":[{"type":"text","text":"  the whole transcript  "}]}]}`)
	adapter := newBatchAdapter(t, server)

	result, err := adapter.Transcribe(context.Background(), batchRequest(server.URL, []byte("RIFFwav-bytes")))
	if err != nil {
		t.Fatalf("Transcribe: %v", err)
	}
	if result.Text != "the whole transcript" {
		t.Fatalf("Text = %q", result.Text)
	}
	if result.ProviderRequestID != "interactions/abc" {
		t.Fatalf("ProviderRequestID = %q", result.ProviderRequestID)
	}
	if len(result.Segments) != 0 {
		t.Fatalf("Segments = %v, want none when no word annotations were requested", result.Segments)
	}
	// No transcription asks means no config at all, so the vendor default
	// (smart mode, which cleans disfluencies) stands.
	if _, present := captured.body["generation_config"]; present {
		t.Fatalf("generation_config = %v, want absent when the caller asked for nothing", captured.body["generation_config"])
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
	if len(captured.body) != 2 {
		t.Fatalf("body keys = %v, want only model and input", captured.body)
	}
}

// Google's own SDK recomputes output_text from steps rather than trusting it,
// so the decoder prefers steps — but a response that carries only the flat
// field still yields a transcript.
func TestBatchFallsBackToOutputText(t *testing.T) {
	t.Parallel()
	server, _ := newFakeInteractions(t, http.StatusOK, `{"id":"i1","output_text":"flat transcript"}`)
	result, err := newBatchAdapter(t, server).Transcribe(context.Background(), batchRequest(server.URL, []byte("wav")))
	if err != nil {
		t.Fatalf("Transcribe: %v", err)
	}
	if result.Text != "flat transcript" {
		t.Fatalf("Text = %q", result.Text)
	}
}

// Steps win over the flat field when both are present, matching how the SDK
// derives the value.
func TestBatchPrefersStepsOverOutputText(t *testing.T) {
	t.Parallel()
	server, _ := newFakeInteractions(t, http.StatusOK, `{"id":"i1","output_text":"stale","steps":[{"type":"model_output","content":[{"type":"text","text":"fresh"}]}]}`)
	result, err := newBatchAdapter(t, server).Transcribe(context.Background(), batchRequest(server.URL, []byte("wav")))
	if err != nil {
		t.Fatalf("Transcribe: %v", err)
	}
	if result.Text != "fresh" {
		t.Fatalf("Text = %q, want the steps value", result.Text)
	}
}

// Diarization switches the request into verbatim mode — the only mode that
// carries speaker labels and word timings — and the word_info annotations that
// come back become timed, speaker-attributed segments.
func TestBatchDiarizationRequestsVerbatimModeAndBuildsSegments(t *testing.T) {
	t.Parallel()
	const response = `{"id":"i1","steps":[{"type":"model_output","content":[{"type":"text","text":"hello there friend","annotations":[
		{"type":"word_info","text":"hello","start_offset":"0s","end_offset":"0.400s","speaker":"spk_1"},
		{"type":"word_info","text":"there","start_offset":"0.400s","end_offset":"0.900s","speaker":"spk_1"},
		{"type":"url_citation","text":"ignored"},
		{"type":"word_info","text":"friend","start_offset":"1.100s","end_offset":"1.600s","speaker":"spk_2"}
	]}]}]}`
	server, captured := newFakeInteractions(t, http.StatusOK, response)
	request := batchRequest(server.URL, []byte("wav"))
	diarize := true
	request.Options.Language = "en-US"
	request.Options.STT = &protocol.SttOptions{Diarization: &diarize, Keywords: []string{"Speko", "   "}}

	result, err := newBatchAdapter(t, server).Transcribe(context.Background(), request)
	if err != nil {
		t.Fatalf("Transcribe: %v", err)
	}
	generation, _ := captured.body["generation_config"].(map[string]any)
	config, _ := generation["transcription_config"].(map[string]any)
	if languages, _ := config["language_codes"].([]any); len(languages) != 1 || languages[0] != "en-US" {
		t.Fatalf("language_codes = %v", config["language_codes"])
	}
	if vocabulary, _ := config["custom_vocabulary"].([]any); len(vocabulary) != 1 || vocabulary[0] != "Speko" {
		t.Fatalf("custom_vocabulary = %v, want blank keywords dropped", config["custom_vocabulary"])
	}
	mode, _ := config["mode"].(map[string]any)
	if mode["type"] != "verbatim" || mode["diarization_mode"] != "speaker" {
		t.Fatalf("mode = %v, want the verbatim mode object", config["mode"])
	}
	if granularities, _ := mode["timestamp_granularities"].([]any); len(granularities) != 1 || granularities[0] != "word" {
		t.Fatalf("timestamp_granularities = %v", mode["timestamp_granularities"])
	}
	// Deprecated root-level spellings must not be sent alongside the mode object.
	for _, deprecated := range []string{"diarization_mode", "timestamp_granularities", "adaptation_phrases"} {
		if _, present := config[deprecated]; present {
			t.Fatalf("deprecated root field %q was sent", deprecated)
		}
	}

	// A speaker change splits the segment; the non-word annotation is skipped.
	if len(result.Segments) != 2 {
		t.Fatalf("Segments = %+v, want two", result.Segments)
	}
	if result.Segments[0].Text != "hello there" || result.Segments[0].Speaker != "spk_1" || result.Segments[0].EndMS != 900 {
		t.Fatalf("first segment = %+v", result.Segments[0])
	}
	if result.Segments[1].Text != "friend" || result.Segments[1].Speaker != "spk_2" || result.Segments[1].StartMS != 1100 {
		t.Fatalf("second segment = %+v", result.Segments[1])
	}
}

// An offset the service omits or spells unexpectedly degrades the segment's
// timing rather than failing the transcription.
func TestOffsetMSToleratesMissingAndMalformedDurations(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		offset string
		want   int64
	}{{"", 0}, {"0s", 0}, {"1.5s", 1500}, {"12s", 12000}, {"  2.25s ", 2250}, {"garbage", 0}, {"1.5", 1500}} {
		if got := offsetMS(test.offset); got != test.want {
			t.Errorf("offsetMS(%q) = %d, want %d", test.offset, got, test.want)
		}
	}
}

func TestBatchRefusesLiveModelAndForeignProvider(t *testing.T) {
	t.Parallel()
	server, _ := newFakeInteractions(t, http.StatusOK, `{"id":"i1","output_text":"x"}`)
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
	server, _ := newFakeInteractions(t, http.StatusOK, `{"id":"i1","output_text":"x"}`)
	request := batchRequest(server.URL, []byte("wav"))
	// Declared size crosses the ceiling once base64 expansion is applied, so
	// the refusal must happen before the file is read.
	request.AudioBytes = BatchMaxAudioBytes + 1
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
	server, _ := newFakeInteractions(t, http.StatusOK, `{"id":"i1","steps":[{"type":"model_output","content":[{"type":"text","text":"   "}]}]}`)
	if _, err := newBatchAdapter(t, server).Transcribe(context.Background(), batchRequest(server.URL, []byte("wav"))); err == nil {
		t.Fatal("accepted a response with no transcript")
	}
}

// The declared size gates the read, but the bytes actually delivered are
// checked too: a reader that outruns its declaration must not slip through.
func TestBatchRefusesAudioLongerThanItsDeclaration(t *testing.T) {
	t.Parallel()
	server, _ := newFakeInteractions(t, http.StatusOK, `{"id":"i1","output_text":"x"}`)
	request := batchRequest(server.URL, bytes.Repeat([]byte("a"), int(BatchMaxAudioBytes)+1))
	request.AudioBytes = 16
	_, err := newBatchAdapter(t, server).Transcribe(context.Background(), request)
	if err == nil || !strings.Contains(err.Error(), "inline request limit") {
		t.Fatalf("Transcribe error = %v, want an input_too_large refusal", err)
	}
}

// The byte ceiling is what the catalog restates and the connector pins.
func TestBatchLimitsFollowTheInlineRequestCeiling(t *testing.T) {
	t.Parallel()
	if want := batchRequestCeilingBytes/4*3 - (16 << 10); BatchMaxAudioBytes != want {
		t.Fatalf("BatchMaxAudioBytes = %d, want %d", BatchMaxAudioBytes, want)
	}
	// Eight minutes of 16 kHz mono s16le must fit inside the byte cap, or the
	// duration bound would be the lie rather than the slack.
	if seconds := BatchMaxAudioBytes / (16_000 * 2); seconds < BatchMaxDurationSeconds {
		t.Fatalf("byte cap holds only %d s of 16 kHz mono, less than the %d s duration bound", seconds, BatchMaxDurationSeconds)
	}
}
