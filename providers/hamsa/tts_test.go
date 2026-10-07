package hamsa

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

func ttsRequest(endpoint, language string, sampleRateHz int) runtimepkg.AdapterRequest {
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindTTS,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged},
			Route: protocol.PlanRoute{Provider: "hamsa", Model: "default", Adapter: TTSAdapterID, Transport: protocol.TransportHTTP, Endpoint: endpoint,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialRelayAccess, Value: "hamsa-key", ExpiresAt: time.Now().Add(time.Minute)}},
		},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: sampleRateHz, Channels: 1},
		Options: protocol.RequestOptions{Language: language},
	}
}

// openTTS uses a 3-byte read size, so every chunk boundary after the first
// splits a sample, the way Hamsa's chunked body does.
func openTTS(t *testing.T, server *httptest.Server, request runtimepkg.AdapterRequest) runtimepkg.ProviderStream {
	t.Helper()
	parsed, _ := url.Parse(server.URL)
	adapter, err := NewTTS(TTSConfig{AllowedEndpointHosts: []string{parsed.Hostname()}, AllowInsecureEndpoint: true, AudioChunkBytes: 3})
	if err != nil {
		t.Fatal(err)
	}
	request.Plan.Route.Endpoint = server.URL + "/v1/realtime/tts-stream"
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) })
	return stream
}

func nextTTSEvent(t *testing.T, stream runtimepkg.ProviderStream) runtimepkg.ProviderEvent {
	t.Helper()
	select {
	case event := <-stream.Events():
		return event
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for an event")
		return runtimepkg.ProviderEvent{}
	}
}

// drainTTS collects audio until audio.done, failing on an error event or on a
// frame that is not sample-aligned.
func drainTTS(t *testing.T, stream runtimepkg.ProviderStream) ([]byte, *protocol.BillingObservation) {
	t.Helper()
	var audio []byte
	for {
		event := nextTTSEvent(t, stream)
		if event.Err != nil {
			t.Fatalf("event error: %v", event.Err)
		}
		if len(event.Audio)%2 != 0 {
			t.Fatalf("frame of %d bytes splits a PCM16 sample", len(event.Audio))
		}
		audio = append(audio, event.Audio...)
		if event.Type == protocol.EventAudioDone {
			return audio, event.Billing
		}
	}
}

func TestCommitTextSendsTheDocumentedRequestAndAlignsSamples(t *testing.T) {
	t.Parallel()
	// Eleven bytes: five samples and a dangling half sample.
	pcm := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
	bodies := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/v1/realtime/tts-stream" || request.Header.Get("Authorization") != "Token hamsa-key" {
			t.Errorf("request = %s %s %v", request.Method, request.URL.Path, request.Header)
		}
		raw, _ := io.ReadAll(request.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		bodies <- body
		writer.Header().Set("Content-Type", "audio/wav")
		writer.Header().Set("X-Request-Id", "req-tts")
		_, _ = writer.Write(pcm)
	}))
	t.Cleanup(server.Close)
	stream := openTTS(t, server, ttsRequest("", "ar-EG", 16_000))
	if err := stream.AppendText(context.Background(), "مرحبا "); err != nil {
		t.Fatal(err)
	}
	if err := stream.AppendText(context.Background(), "بكم"); err != nil {
		t.Fatal(err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}
	body := <-bodies
	want := map[string]any{"text": "مرحبا بكم", "speaker": "Salem", "dialect": "egy", "mulaw": false, "sampleRate": "16k"}
	for key, value := range want {
		if body[key] != value {
			t.Errorf("body[%s] = %v, want %v (body %v)", key, body[key], value, body)
		}
	}
	audio, billing := drainTTS(t, stream)
	if string(audio) != string(pcm[:10]) {
		t.Fatalf("audio = %v, want the first five samples %v", audio, pcm[:10])
	}
	if billing == nil || billing.Validate() != nil || !billing.Complete || billing.ProviderRequestID != "req-tts" || billing.Model != "default" || billing.Mode != "streaming" {
		t.Fatalf("billing = %+v", billing)
	}
	// 10 bytes of 16 kHz PCM16 is 0.3 ms; Hamsa charges a whole second.
	if billing.Quantities["duration_seconds"] != 1000 {
		t.Fatalf("duration_seconds = %d, want 1000", billing.Quantities["duration_seconds"])
	}
}

func TestSynthesisBillsDeliveredAudioInWholeSecondsAt8kHz(t *testing.T) {
	t.Parallel()
	bodies := make(chan map[string]any, 1)
	// 2.5 s at 8 kHz PCM16, which Hamsa charges as 3 s.
	pcm := make([]byte, 40_000)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		raw, _ := io.ReadAll(request.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		bodies <- body
		writer.Header().Set("Content-Type", "audio/wav")
		_, _ = writer.Write(pcm)
	}))
	t.Cleanup(server.Close)
	stream := openTTS(t, server, ttsRequest("", "", 8_000))
	if err := stream.AppendText(context.Background(), "نص"); err != nil {
		t.Fatal(err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}
	if body := <-bodies; body["sampleRate"] != "8k" || body["dialect"] != "msa" {
		t.Fatalf("body = %v, want sampleRate 8k and the MSA dialect for an unset language", body)
	}
	audio, billing := drainTTS(t, stream)
	if len(audio) != len(pcm) || billing.Quantities["duration_seconds"] != 3000 {
		t.Fatalf("audio %d bytes billed %d ms, want %d bytes billed 3000 ms", len(audio), billing.Quantities["duration_seconds"], len(pcm))
	}
}

func TestCommitTextStripsALeadingWAVHeader(t *testing.T) {
	t.Parallel()
	header := make([]byte, 44)
	copy(header[0:], "RIFF")
	binary.LittleEndian.PutUint32(header[4:], 0xFFFFFFFF)
	copy(header[8:], "WAVE")
	copy(header[12:], "fmt ")
	binary.LittleEndian.PutUint32(header[16:], 16)
	binary.LittleEndian.PutUint16(header[20:], 1)
	binary.LittleEndian.PutUint16(header[22:], 1)
	binary.LittleEndian.PutUint32(header[24:], 16_000)
	copy(header[36:], "data")
	binary.LittleEndian.PutUint32(header[40:], 0xFFFFFFFF)
	pcm := []byte{9, 8, 7, 6}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "audio/wav")
		_, _ = writer.Write(append(header, pcm...))
	}))
	t.Cleanup(server.Close)
	stream := openTTS(t, server, ttsRequest("", "ar", 16_000))
	if err := stream.AppendText(context.Background(), "نص"); err != nil {
		t.Fatal(err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}
	if audio, _ := drainTTS(t, stream); string(audio) != string(pcm) {
		t.Fatalf("audio = %v, want %v with the header removed", audio, pcm)
	}
}

func TestTTSDialectFollowsTheVoicesCatalogTags(t *testing.T) {
	t.Parallel()
	for language, want := range map[string]string{
		"": "msa", "ar": "msa", "AR-sa": "ksa", "ar_EG": "egy", "ar-PS": "pls", "ar-AE": "uae", "ar-OM": "oma", "en": "en", "en-GB": "en",
	} {
		if got, err := ttsDialect(language); err != nil || got != want {
			t.Errorf("ttsDialect(%q) = %q, %v; want %q", language, got, err, want)
		}
	}
	for _, language := range []string{"fr", "ar-MA", "he"} {
		if _, err := ttsDialect(language); err == nil {
			t.Errorf("ttsDialect(%q) must refuse a language Hamsa has no dialect for", language)
		}
	}
}

func TestRequestedVoiceWinsOverThePlannedOne(t *testing.T) {
	t.Parallel()
	if got := ttsVoice("Mariam", "Salem"); got != "Mariam" {
		t.Fatalf("ttsVoice = %q, want the requested voice", got)
	}
	if got := ttsVoice("", "6b52beba-b560-45d4-827b-49be73d50db7"); got != "6b52beba-b560-45d4-827b-49be73d50db7" {
		t.Fatalf("ttsVoice = %q, want the planned cloned-voice UUID", got)
	}
	if got := ttsVoice(" ", ""); got != DefaultVoice {
		t.Fatalf("ttsVoice = %q, want the default voice", got)
	}
}

func TestOpenRefusesMediaHamsaCannotEmit(t *testing.T) {
	t.Parallel()
	adapter, err := NewTTS(TTSConfig{})
	if err != nil {
		t.Fatal(err)
	}
	for name, request := range map[string]runtimepkg.AdapterRequest{
		"24 kHz": ttsRequest("https://api.tryhamsa.com/v1/realtime/tts-stream", "ar", 24_000),
		"unknown model": func() runtimepkg.AdapterRequest {
			r := ttsRequest("https://api.tryhamsa.com/v1/realtime/tts-stream", "ar", 16_000)
			r.Plan.Route.Model = "s3"
			return r
		}(),
		"wrong endpoint": ttsRequest("https://api.tryhamsa.com/v1/realtime/tts", "ar", 16_000),
		"foreign host":   ttsRequest("https://example.com/v1/realtime/tts-stream", "ar", 16_000),
	} {
		if _, err := adapter.Open(context.Background(), request); err == nil {
			t.Errorf("%s: Open must fail", name)
		}
	}
	if _, err := adapter.Open(context.Background(), ttsRequest("https://api.tryhamsa.com/v1/realtime/tts-stream", "ar", 16_000)); err != nil {
		t.Fatalf("the catalog endpoint must open: %v", err)
	}
}

func TestSynthesisRejectionsKeepTheirClassification(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		status    int
		body      string
		code      string
		retryable bool
	}{
		{http.StatusUnauthorized, `{"code":401,"message":"Unauthorized"}`, "authentication_failed", false},
		{http.StatusTooManyRequests, `{"code":10429,"message":"You have reached your text-to-speech usage limit!"}`, "provider_quota_exceeded", false},
		{http.StatusTooManyRequests, `{"code":429,"message":"Rate limit exceeded for this API key"}`, "provider_rate_limited", true},
		{http.StatusBadRequest, `{"code":400,"message":"speaker is required"}`, "invalid_request", false},
		{http.StatusInternalServerError, `{"code":500,"message":"boom"}`, "provider_unavailable", true},
	} {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(test.status)
			_, _ = writer.Write([]byte(test.body))
		}))
		stream := openTTS(t, server, ttsRequest("", "ar", 16_000))
		if err := stream.AppendText(context.Background(), "نص"); err != nil {
			t.Fatal(err)
		}
		err := stream.CommitText(context.Background())
		providerErr, ok := err.(*runtimepkg.ProviderError)
		if !ok || providerErr.Code != test.code || providerErr.Retryable != test.retryable || providerErr.ProviderStatus != test.status {
			t.Errorf("status %d %s: error = %#v, want %s retryable=%v", test.status, test.body, err, test.code, test.retryable)
		}
		server.Close()
	}
}

func TestTruncatedStreamIsAFailureWithPartialBilling(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "audio/wav")
		writer.Header().Set("Content-Length", "100")
		// Past the 12 bytes held while ruling out a RIFF header.
		_, _ = writer.Write(make([]byte, 16))
		writer.(http.Flusher).Flush()
	}))
	t.Cleanup(server.Close)
	stream := openTTS(t, server, ttsRequest("", "ar", 16_000))
	if err := stream.AppendText(context.Background(), "نص"); err != nil {
		t.Fatal(err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}
	for {
		event := nextTTSEvent(t, stream)
		if event.Type == protocol.EventAudioDone {
			t.Fatal("a torn body must not finish as audio.done")
		}
		if event.Err != nil {
			if event.Billing == nil || event.Billing.Complete || event.Billing.Quantities["duration_seconds"] != 1000 {
				t.Fatalf("billing = %+v, want an incomplete snapshot of the audio delivered", event.Billing)
			}
			return
		}
	}
}

func TestInputLimitCountsCharacters(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(server.Close)
	stream := openTTS(t, server, ttsRequest("", "ar", 16_000))
	// 2000 two-byte Arabic letters are 4000 bytes but exactly the limit.
	if err := stream.AppendText(context.Background(), strings.Repeat("ب", 2_000)); err != nil {
		t.Fatalf("2000 characters must fit: %v", err)
	}
	err := stream.AppendText(context.Background(), "ب")
	if providerErr, ok := err.(*runtimepkg.ProviderError); !ok || providerErr.Code != "input_too_large" {
		t.Fatalf("error = %v, want input_too_large", err)
	}
}

func TestCommitTextDoesNotFollowRedirects(t *testing.T) {
	t.Parallel()
	followed := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/elsewhere" {
			followed <- struct{}{}
			return
		}
		http.Redirect(writer, request, "/elsewhere", http.StatusTemporaryRedirect)
	}))
	t.Cleanup(server.Close)
	stream := openTTS(t, server, ttsRequest("", "ar", 16_000))
	if err := stream.AppendText(context.Background(), "نص"); err != nil {
		t.Fatal(err)
	}
	if err := stream.CommitText(context.Background()); err == nil {
		t.Fatal("a redirect must fail the synthesis")
	}
	select {
	case <-followed:
		t.Fatal("the redirect was followed with the API key attached")
	default:
	}
}

func TestErrorBodyThatNeverEndsIsBoundedByTheHeaderTimeout(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusInternalServerError)
		_, _ = writer.Write([]byte(`{"code":500,`))
		writer.(http.Flusher).Flush()
		<-release
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })
	parsed, _ := url.Parse(server.URL)
	adapter, err := NewTTS(TTSConfig{AllowedEndpointHosts: []string{parsed.Hostname()}, AllowInsecureEndpoint: true, HeaderTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	request := ttsRequest(server.URL+"/v1/realtime/tts-stream", "ar", 16_000)
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) })
	if err := stream.AppendText(context.Background(), "نص"); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- stream.CommitText(context.Background()) }()
	select {
	case err := <-result:
		if providerErr, ok := err.(*runtimepkg.ProviderError); !ok || providerErr.ProviderStatus != http.StatusInternalServerError {
			t.Fatalf("error = %v, want the 500 classification", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CommitText hung on an error body that never ended")
	}
}

func TestNextUtteranceCanStartOnAudioDone(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "audio/wav")
		_, _ = writer.Write(make([]byte, 64))
	}))
	t.Cleanup(server.Close)
	stream := openTTS(t, server, ttsRequest("", "ar", 16_000))
	for turn := range 20 {
		if err := stream.AppendText(context.Background(), "نص"); err != nil {
			t.Fatalf("turn %d: AppendText right after audio.done: %v", turn, err)
		}
		if err := stream.CommitText(context.Background()); err != nil {
			t.Fatalf("turn %d: %v", turn, err)
		}
		drainTTS(t, stream)
	}
}

func TestCancelledSynthesisKeepsTheDeliveredDuration(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "audio/wav")
		// 1.25 s of 16 kHz PCM16, then a stall the caller cancels.
		_, _ = writer.Write(make([]byte, 40_000))
		writer.(http.Flusher).Flush()
		select {
		case <-release:
		case <-request.Context().Done():
		}
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })
	parsed, _ := url.Parse(server.URL)
	adapter, err := NewTTS(TTSConfig{AllowedEndpointHosts: []string{parsed.Hostname()}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := adapter.Open(context.Background(), ttsRequest(server.URL+"/v1/realtime/tts-stream", "ar", 16_000))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) })
	if err := stream.AppendText(context.Background(), "نص"); err != nil {
		t.Fatal(err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}
	var last *protocol.BillingObservation
	var audio int
	for audio < 40_000 {
		event := nextTTSEvent(t, stream)
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		audio += len(event.Audio)
		if event.Billing != nil {
			last = event.Billing
		}
	}
	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatal(err)
	}
	if last == nil || last.Complete || last.Quantities["duration_seconds"] != 2000 {
		t.Fatalf("billing = %+v, want an incomplete 2 s snapshot of the audio already delivered", last)
	}
}

func TestWAVHeaderWithoutADataChunkIsAFailureNotAudio(t *testing.T) {
	t.Parallel()
	header := make([]byte, 36)
	copy(header[0:], "RIFF")
	copy(header[8:], "WAVE")
	copy(header[12:], "fmt ")
	binary.LittleEndian.PutUint32(header[16:], 16)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "audio/wav")
		_, _ = writer.Write(header)
	}))
	t.Cleanup(server.Close)
	stream := openTTS(t, server, ttsRequest("", "ar", 16_000))
	if err := stream.AppendText(context.Background(), "نص"); err != nil {
		t.Fatal(err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}
	for {
		event := nextTTSEvent(t, stream)
		if len(event.Audio) > 0 || event.Type == protocol.EventAudioDone {
			t.Fatalf("a header with no data chunk became %s with %d audio bytes", event.Type, len(event.Audio))
		}
		if event.Err != nil {
			return
		}
	}
}
