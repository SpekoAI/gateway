package azure

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
	subscriptionKey  string
	authorization    string
	accept           string
	query            string
	definition       map[string]any
	audioContentType string
	audioFilename    string
	audio            []byte
	parts            []string
}

func newFakeTranscribe(t *testing.T, status int, response string) (*httptest.Server, *capturedBatch) {
	t.Helper()
	captured := &capturedBatch{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		captured.subscriptionKey = request.Header.Get(subscriptionHeader)
		captured.authorization = request.Header.Get("Authorization")
		captured.accept = request.Header.Get("Accept")
		captured.query = request.URL.RawQuery
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
			case definitionPartName:
				_ = json.Unmarshal(body, &captured.definition)
			case audioPartName:
				captured.audioContentType = part.Header.Get("Content-Type")
				captured.audioFilename = part.FileName()
				captured.audio = body
			}
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("apim-request-id", "req-7f3a")
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
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "test-speech-key", ExpiresAt: time.Now().Add(time.Minute)}},
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

const okResponse = `{"durationMilliseconds":8240,"combinedPhrases":[{"channel":0,"text":"How is the weather? It is raining."}],
	"phrases":[{"channel":0,"offsetMilliseconds":1520,"durationMilliseconds":3120,"text":"How is the weather?","locale":"en","confidence":0,
	"words":[{"text":"How","offsetMilliseconds":1520,"durationMilliseconds":200}]},
	{"channel":0,"offsetMilliseconds":5900,"durationMilliseconds":2340,"text":"It is raining.","locale":"en","confidence":0},
	{"channel":0,"offsetMilliseconds":8240,"durationMilliseconds":0,"text":"  ","locale":"en","confidence":0}]}`

func TestBatchTranscribesTheWavInEnhancedMode(t *testing.T) {
	t.Parallel()
	server, captured := newFakeTranscribe(t, http.StatusOK, okResponse)
	adapter := newBatchAdapter(t, server)

	result, err := adapter.Transcribe(context.Background(), batchRequest(server.URL+"/speechtotext/transcriptions:transcribe", []byte("RIFFwav-bytes")))
	if err != nil {
		t.Fatalf("Transcribe: %v", err)
	}
	if result.Text != "How is the weather? It is raining." {
		t.Fatalf("Text = %q", result.Text)
	}
	if result.DurationMS != 8240 || result.ProviderRequestID != "req-7f3a" || result.Language != "en" {
		t.Fatalf("DurationMS = %d, ProviderRequestID = %q, Language = %q", result.DurationMS, result.ProviderRequestID, result.Language)
	}
	if len(result.Segments) != 2 {
		t.Fatalf("Segments = %+v, want the two non-empty phrases", result.Segments)
	}
	if result.Segments[0] != (runtimepkg.BatchSegment{Text: "How is the weather?", StartMS: 1520, EndMS: 4640}) {
		t.Fatalf("Segments[0] = %+v", result.Segments[0])
	}
	if result.Segments[1].Speaker != "" {
		t.Fatalf("Speaker = %q without diarization", result.Segments[1].Speaker)
	}
	if _, ok := result.Extensions[batchExtensionID]; !ok {
		t.Fatalf("Extensions = %v, want the raw response under %q", result.Extensions, batchExtensionID)
	}

	if captured.subscriptionKey != "test-speech-key" || captured.authorization != "" {
		t.Fatalf("%s = %q, Authorization = %q", subscriptionHeader, captured.subscriptionKey, captured.authorization)
	}
	if captured.accept != "application/json" {
		t.Fatalf("Accept = %q", captured.accept)
	}
	if captured.query != "api-version="+APIVersion {
		t.Fatalf("query = %q", captured.query)
	}
	if strings.Join(captured.parts, ",") != definitionPartName+","+audioPartName {
		t.Fatalf("parts = %v, want definition then audio", captured.parts)
	}
	if captured.audioContentType != "audio/wav" || captured.audioFilename != "audio.wav" || string(captured.audio) != "RIFFwav-bytes" {
		t.Fatalf("audio part = %q %q %q", captured.audioContentType, captured.audioFilename, captured.audio)
	}
	enhanced, _ := captured.definition["enhancedMode"].(map[string]any)
	if enhanced["enabled"] != true || enhanced["model"] != "MAI-Transcribe-2" {
		t.Fatalf("enhancedMode = %v", captured.definition["enhancedMode"])
	}
	if modelOptions, _ := enhanced["modelOptions"].(map[string]any); modelOptions["timestamps"] != "word" {
		t.Fatalf("modelOptions = %v", enhanced["modelOptions"])
	}
	for _, absent := range []string{"locales", "diarization", "phraseList"} {
		if _, ok := captured.definition[absent]; ok {
			t.Fatalf("definition carries %s without an ask: %v", absent, captured.definition)
		}
	}
}

func TestBatchDiarizationLocaleAndKeywordsRideTheDefinition(t *testing.T) {
	t.Parallel()
	server, captured := newFakeTranscribe(t, http.StatusOK, `{"durationMilliseconds":4000,"combinedPhrases":[{"channel":0,"text":"Hola. Adiós."}],
		"phrases":[{"channel":0,"speaker":1,"offsetMilliseconds":0,"durationMilliseconds":1000,"text":"Hola.","locale":"es"},
		{"channel":0,"speaker":2,"offsetMilliseconds":2000,"durationMilliseconds":1000,"text":"Adiós.","locale":"es"}]}`)
	adapter := newBatchAdapter(t, server)
	request := batchRequest(server.URL, []byte("RIFF"))
	yes := true
	request.Options = protocol.RequestOptions{Language: "es-MX", STT: &protocol.SttOptions{Diarization: &yes, Keywords: []string{" Speko ", "", "Muse"}}}

	result, err := adapter.Transcribe(context.Background(), request)
	if err != nil {
		t.Fatalf("Transcribe: %v", err)
	}
	if locales, _ := captured.definition["locales"].([]any); len(locales) != 1 || locales[0] != "es" {
		t.Fatalf("locales = %v, want the bare primary subtag", captured.definition["locales"])
	}
	if diarization, _ := captured.definition["diarization"].(map[string]any); diarization["enabled"] != true {
		t.Fatalf("diarization = %v", captured.definition["diarization"])
	}
	phraseList, _ := captured.definition["phraseList"].(map[string]any)
	if phrases, _ := phraseList["phrases"].([]any); len(phrases) != 2 || phrases[0] != "Speko" || phrases[1] != "Muse" {
		t.Fatalf("phraseList = %v", captured.definition["phraseList"])
	}
	if len(result.Segments) != 2 || result.Segments[0].Speaker != "1" || result.Segments[1].Speaker != "2" {
		t.Fatalf("Segments = %+v, want Azure's speaker labels as text", result.Segments)
	}
	if result.Language != "es" {
		t.Fatalf("Language = %q", result.Language)
	}
}

func TestBatchDropsAnUnlistedLocaleAndACodeSwitchedLanguage(t *testing.T) {
	t.Parallel()
	server, captured := newFakeTranscribe(t, http.StatusOK, `{"durationMilliseconds":4000,"combinedPhrases":[{"channel":0,"text":"Hello. Bonjour."}],
		"phrases":[{"channel":0,"offsetMilliseconds":0,"durationMilliseconds":1000,"text":"Hello.","locale":"en"},
		{"channel":0,"offsetMilliseconds":2000,"durationMilliseconds":1000,"text":"Bonjour.","locale":"fr"}]}`)
	adapter := newBatchAdapter(t, server)
	request := batchRequest(server.URL, []byte("RIFF"))
	request.Options = protocol.RequestOptions{Language: "haw"}

	result, err := adapter.Transcribe(context.Background(), request)
	if err != nil {
		t.Fatalf("Transcribe: %v", err)
	}
	if _, ok := captured.definition["locales"]; ok {
		t.Fatalf("locales = %v, want the unlisted language left unsent", captured.definition["locales"])
	}
	if result.Language != "" {
		t.Fatalf("Language = %q, want none for a code-switched answer", result.Language)
	}
}

func TestBatchJoinsPhrasesWhenTheCombinedTranscriptIsEmpty(t *testing.T) {
	t.Parallel()
	server, _ := newFakeTranscribe(t, http.StatusOK, `{"durationMilliseconds":3000,"combinedPhrases":[],"phrases":[{"offsetMilliseconds":0,"durationMilliseconds":1000,"text":"One."},{"offsetMilliseconds":1000,"durationMilliseconds":1000,"text":"Two."}]}`)
	adapter := newBatchAdapter(t, server)

	result, err := adapter.Transcribe(context.Background(), batchRequest(server.URL, []byte("RIFF")))
	if err != nil {
		t.Fatalf("Transcribe: %v", err)
	}
	if result.Text != "One. Two." {
		t.Fatalf("Text = %q", result.Text)
	}
}

func TestBatchRefusesAnEmptyTranscript(t *testing.T) {
	t.Parallel()
	server, _ := newFakeTranscribe(t, http.StatusOK, `{"durationMilliseconds":3000,"combinedPhrases":[{"channel":0,"text":""}],"phrases":[]}`)
	adapter := newBatchAdapter(t, server)

	_, err := adapter.Transcribe(context.Background(), batchRequest(server.URL, []byte("RIFF")))
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeProviderError {
		t.Fatalf("err = %v, want a provider_error for the empty transcript", err)
	}
}

func TestBatchRefusesForeignProviderAndNonMAIModel(t *testing.T) {
	t.Parallel()
	server, _ := newFakeTranscribe(t, http.StatusOK, okResponse)
	adapter := newBatchAdapter(t, server)

	request := batchRequest(server.URL, []byte("RIFF"))
	request.Plan.Route.Provider = "meta"
	if _, err := adapter.Transcribe(context.Background(), request); err == nil || !strings.Contains(err.Error(), `provider "meta"`) {
		t.Fatalf("foreign provider err = %v", err)
	}
	request = batchRequest(server.URL, []byte("RIFF"))
	request.Plan.Route.Model = "fast-transcription"
	if _, err := adapter.Transcribe(context.Background(), request); err == nil || !strings.Contains(err.Error(), `model "fast-transcription"`) {
		t.Fatalf("classic model err = %v", err)
	}
	// The 1.5 generation rides the same enhanced-mode contract.
	request = batchRequest(server.URL, []byte("RIFF"))
	request.Plan.Route.Model = "MAI-Transcribe-1.5"
	if _, err := adapter.Transcribe(context.Background(), request); err != nil {
		t.Fatalf("MAI-Transcribe-1.5 err = %v", err)
	}
}

func TestBatchRefusesOversizedUploads(t *testing.T) {
	t.Parallel()
	server, captured := newFakeTranscribe(t, http.StatusOK, okResponse)
	adapter := newBatchAdapter(t, server)

	request := batchRequest(server.URL, []byte("RIFF"))
	request.AudioBytes = BatchMaxAudioBytes + 1
	_, err := adapter.Transcribe(context.Background(), request)
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeInputTooLarge {
		t.Fatalf("err = %v, want input_too_large before any upload", err)
	}
	if captured.parts != nil {
		t.Fatalf("the oversized declaration reached the server: %v", captured.parts)
	}

	// A stream that outruns its declaration is cut at the ceiling rather
	// than uploaded whole.
	reader := &boundedReader{reader: bytes.NewReader(bytes.Repeat([]byte{1}, 10)), remaining: 4}
	if _, err := io.ReadAll(reader); !errors.Is(err, errUploadTooLarge) {
		t.Fatalf("bounded read err = %v", err)
	}
	reader = &boundedReader{reader: bytes.NewReader(bytes.Repeat([]byte{1}, 4)), remaining: 4}
	if out, err := io.ReadAll(reader); err != nil || len(out) != 4 {
		t.Fatalf("exact-size read = %d bytes, err %v", len(out), err)
	}
}

func TestBatchClassifiesUpstreamFailure(t *testing.T) {
	t.Parallel()
	server, _ := newFakeTranscribe(t, http.StatusTooManyRequests, `{"error":{"code":"429","message":"quota"}}`)
	adapter := newBatchAdapter(t, server)

	_, err := adapter.Transcribe(context.Background(), batchRequest(server.URL, []byte("RIFF")))
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != batchhttp.CodeRateLimited || providerErr.ProviderStatus != http.StatusTooManyRequests {
		t.Fatalf("err = %v, want provider_rate_limited with status 429", err)
	}
}

func TestBatchEndpointPolicyAcceptsBothDocumentedHostFamilies(t *testing.T) {
	t.Parallel()
	adapter, err := NewBatch(BatchConfig{})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	for _, endpoint := range []string{BatchEndpoint, "https://my-speech.cognitiveservices.azure.com/speechtotext/transcriptions:transcribe"} {
		if _, err := adapter.endpointPolicy.Parse(endpoint); err != nil {
			t.Fatalf("Parse(%q) = %v", endpoint, err)
		}
	}
	for _, endpoint := range []string{"https://evil.example.com/speechtotext/transcriptions:transcribe", BatchEndpoint + "?api-version=" + APIVersion} {
		if _, err := adapter.endpointPolicy.Parse(endpoint); err == nil {
			t.Fatalf("Parse(%q) accepted", endpoint)
		}
	}
}

func TestBatchLimitsFollowTheDocumentedCeilings(t *testing.T) {
	t.Parallel()
	if BatchMaxAudioBytes != (300<<20)-(16<<10) {
		t.Fatalf("BatchMaxAudioBytes = %d", BatchMaxAudioBytes)
	}
	if BatchMaxDurationSeconds != 18_000 {
		t.Fatalf("BatchMaxDurationSeconds = %d", BatchMaxDurationSeconds)
	}
	// At 16 kHz mono the byte cap binds before the five-hour cap.
	if seconds := BatchMaxAudioBytes / (16_000 * 2); seconds >= BatchMaxDurationSeconds {
		t.Fatalf("byte cap admits %d seconds at 16 kHz; the duration cap should not be the binding one", seconds)
	}
}

func TestLocaleReducesTagsToListedCodes(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"en": "en", "en-US": "en", "pt_BR": "pt", "FIL": "fil", "tl": "fil", "iw": "he", "cmn": "zh",
		"yue": "yue", "zh-HK": "zh", "no": "nb", "haw": "", "": "  ", " ": "",
	}
	for input, want := range cases {
		if want == "  " {
			want = ""
		}
		if got := locale(input); got != want {
			t.Errorf("locale(%q) = %q, want %q", input, got, want)
		}
	}
}
