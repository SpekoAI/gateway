package munsit

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	testTTSEndpoint = "https://api.munsit.com/api/v1/text-to-speech/faseeh-v1-preview"
	ttsPath         = speechPathPrefix + DefaultTTSModel
)

func rawContentType(rate int) string {
	return "audio/raw;codec=pcm16;rate=" + strconv.Itoa(rate) + ";channels=1"
}

func ttsRequest(endpoint, language string, rate int) runtimepkg.AdapterRequest {
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindTTS,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged},
			Route: protocol.PlanRoute{Provider: ProviderName, Model: DefaultTTSModel, Adapter: TTSAdapterID, Transport: protocol.TransportHTTP, Endpoint: endpoint,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialRelayAccess, Value: "munsit-key", ExpiresAt: time.Now().Add(time.Minute)}},
		},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: rate, Channels: 1},
		Options: protocol.RequestOptions{Language: language},
	}
}

func openTTS(t *testing.T, server *httptest.Server, request runtimepkg.AdapterRequest, config TTSConfig) runtimepkg.ProviderStream {
	t.Helper()
	parsed, _ := url.Parse(server.URL)
	config.AllowedEndpointHosts = []string{parsed.Hostname()}
	config.AllowInsecureEndpoint = true
	if config.AudioChunkBytes == 0 {
		// An odd read size splits samples across reads.
		config.AudioChunkBytes = 3
	}
	adapter, err := NewTTS(config)
	if err != nil {
		t.Fatal(err)
	}
	request.Plan.Route.Endpoint = server.URL + ttsPath
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) })
	return stream
}

func nextEvent(t *testing.T, stream runtimepkg.ProviderStream) runtimepkg.ProviderEvent {
	t.Helper()
	select {
	case event := <-stream.Events():
		return event
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for an event")
		return runtimepkg.ProviderEvent{}
	}
}

// drainAudio collects audio until audio.done, failing on an error event, and
// returns it with the audio.done event.
func drainAudio(t *testing.T, stream runtimepkg.ProviderStream) ([]byte, runtimepkg.ProviderEvent) {
	t.Helper()
	var audio []byte
	for {
		event := nextEvent(t, stream)
		if event.Err != nil {
			t.Fatalf("event error: %v", event.Err)
		}
		if len(event.Audio)%2 == 1 {
			t.Fatalf("frame of %d bytes splits a sample", len(event.Audio))
		}
		audio = append(audio, event.Audio...)
		if event.Type == protocol.EventAudioDone {
			return audio, event
		}
	}
}

func TestCommitTextSendsTheDocumentedRequestAndStreamsWholeSamples(t *testing.T) {
	t.Parallel()
	pcm := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	bodies := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != ttsPath || request.Header.Get("X-Api-Key") != "munsit-key" || request.Header.Get("Authorization") != "" {
			t.Errorf("request = %s %s %v", request.Method, request.URL.Path, request.Header)
		}
		raw, _ := io.ReadAll(request.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		bodies <- body
		writer.Header().Set("Content-Type", rawContentType(22_050))
		_, _ = writer.Write(pcm)
	}))
	t.Cleanup(server.Close)
	stream := openTTS(t, server, ttsRequest("", "ar-SA", 22_050), TTSConfig{})
	if err := stream.AppendText(context.Background(), "مرحبا "); err != nil {
		t.Fatal(err)
	}
	if err := stream.AppendText(context.Background(), "بكم"); err != nil {
		t.Fatal(err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("CommitText: %v", err)
	}
	body := <-bodies
	want := map[string]any{"voice_id": DefaultVoice, "text": "مرحبا بكم", "stability": 0.5, "streaming": true, "sample_rate": float64(22_050)}
	if len(body) != len(want) {
		t.Fatalf("body = %#v, want %#v", body, want)
	}
	for key, value := range want {
		if body[key] != value {
			t.Fatalf("body[%s] = %#v, want %#v", key, body[key], value)
		}
	}
	started := nextEvent(t, stream)
	if started.Type != protocol.EventAudioStarted || started.Billing == nil || started.Billing.Complete {
		t.Fatalf("first event = %s %+v, want audio.started with pending billing", started.Type, started.Billing)
	}
	audio, done := drainAudio(t, stream)
	if string(audio) != string(pcm) {
		t.Fatalf("audio = %v, want %v", audio, pcm)
	}
	// 9 characters at 2 credits each, in thousandths.
	if done.Billing == nil || !done.Billing.Complete || done.Billing.Quantities["credits"] != 18_000 || done.Billing.Model != DefaultTTSModel {
		t.Fatalf("audio.done billing = %+v", done.Billing)
	}
	if err := done.Billing.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestEverySupportedRateIsRequestedAndChecked(t *testing.T) {
	t.Parallel()
	for rate := range ttsSampleRates {
		t.Run(strconv.Itoa(rate), func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				var body speechRequest
				_ = json.NewDecoder(request.Body).Decode(&body)
				writer.Header().Set("Content-Type", rawContentType(body.SampleRate))
				_, _ = writer.Write([]byte{1, 0})
			}))
			t.Cleanup(server.Close)
			stream := openTTS(t, server, ttsRequest("", "", rate), TTSConfig{})
			_ = stream.AppendText(context.Background(), "ok")
			if err := stream.CommitText(context.Background()); err != nil {
				t.Fatal(err)
			}
			if audio, _ := drainAudio(t, stream); len(audio) != 2 {
				t.Fatalf("audio = %v", audio)
			}
		})
	}
}

// A rate other than the session's would play at the wrong pitch with no
// error, so the content type is checked before any audio is emitted.
func TestUnexpectedFormatIsARejectionNotAudio(t *testing.T) {
	t.Parallel()
	for name, contentType := range map[string]string{
		"rate":     rawContentType(16_000),
		"channels": "audio/raw;codec=pcm16;rate=24000;channels=2",
		"codec":    "audio/raw;codec=mulaw;rate=24000;channels=1",
		"no rate":  "audio/raw",
		"wav":      "audio/wav",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", contentType)
				_, _ = writer.Write([]byte{1, 2, 3, 4})
			}))
			t.Cleanup(server.Close)
			stream := openTTS(t, server, ttsRequest("", "ar", 24_000), TTSConfig{})
			_ = stream.AppendText(context.Background(), "مرحبا")
			err := stream.CommitText(context.Background())
			var providerErr *runtimepkg.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Code != "provider_unavailable" || !providerErr.Retryable {
				t.Fatalf("CommitText = %v, want a retryable rejection", err)
			}
		})
	}
}

func TestLanguagePicksTheDefaultVoice(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, language, requested, planned, want string
	}{
		{name: "english session takes the english voice", language: "en-US", want: DefaultEnglishVoice},
		{name: "english session replaces the catalog fill", language: "en", planned: DefaultVoice, want: DefaultEnglishVoice},
		{name: "explicit arabic voice on an english session is honored", language: "en", requested: DefaultVoice, planned: DefaultVoice, want: DefaultVoice},
		{name: "english session keeps a planned choice", language: "en", planned: "PCtWbxjoNTpVQ6gIPaVZ2Hqm", want: "PCtWbxjoNTpVQ6gIPaVZ2Hqm"},
		{name: "arabic session", language: "ar", want: DefaultVoice},
		{name: "no language keeps the plan", language: "", planned: "PCtWbxjoNTpVQ6gIPaVZ2Hqm", want: "PCtWbxjoNTpVQ6gIPaVZ2Hqm"},
		{name: "no language and no plan", want: DefaultVoice},
	}
	for _, tc := range cases {
		if got := ttsVoice(tc.requested, tc.planned, baseLanguage(tc.language)); got != tc.want {
			t.Errorf("%s: voice = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestOpenRefusesWhatTheEndpointCannotServe(t *testing.T) {
	t.Parallel()
	adapter, err := NewTTS(TTSConfig{})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := adapter.Open(context.Background(), ttsRequest(testTTSEndpoint, "en", 24_000))
	if err != nil {
		t.Fatal(err)
	}
	_ = stream.Close(context.Background())
	model := ttsRequest(testTTSEndpoint, "en", 24_000)
	model.Plan.Route.Model = "faseeh-v2"
	stereo := ttsRequest(testTTSEndpoint, "en", 24_000)
	stereo.Media.Channels = 2
	for name, request := range map[string]runtimepkg.AdapterRequest{
		"model":    model,
		"rate":     ttsRequest(testTTSEndpoint, "en", 32_000),
		"stereo":   stereo,
		"language": ttsRequest(testTTSEndpoint, "fr", 24_000),
		"path":     ttsRequest("https://api.munsit.com/api/v1/text-to-speech/other-model", "en", 24_000),
		"host":     ttsRequest("https://api.example.com/api/v1/text-to-speech/faseeh-v1-preview", "en", 24_000),
	} {
		if _, err := adapter.Open(context.Background(), request); err == nil {
			t.Errorf("%s: Open succeeded, want refusal", name)
		}
	}
	_, err = adapter.Open(context.Background(), ttsRequest(testTTSEndpoint, "fr-FR", 24_000))
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "unsupported_language" {
		t.Fatalf("fr error = %v, want unsupported_language", err)
	}
}

func TestSynthesisRejectionsKeepTheirClassification(t *testing.T) {
	t.Parallel()
	cases := []struct {
		status    int
		body      string
		want      string
		retryable bool
	}{
		{status: http.StatusUnauthorized, body: `{"errorCode":40101,"errorMessage":"Invalid API key"}`, want: "authentication_failed"},
		{status: http.StatusBadRequest, body: `{"errorCode":40001,"errorMessage":"Voice embeddings not found"}`, want: "invalid_request"},
		{status: http.StatusPaymentRequired, body: `{"statusCode":402,"message":"Insufficient balance","errorCode":40201}`, want: "provider_quota_exceeded"},
		{status: http.StatusTooManyRequests, body: `{"errorCode":42901,"errorMessage":"Concurrency limit"}`, want: "provider_rate_limited", retryable: true},
		{status: http.StatusBadGateway, body: `{"errorCode":50202,"errorMessage":"Upstream error"}`, want: "provider_unavailable", retryable: true},
		{status: http.StatusInternalServerError, body: `{"errorCode":50001,"errorMessage":"Internal error"}`, want: "provider_unavailable", retryable: true},
		{status: http.StatusBadRequest, body: `{"message":"Either 'speakers' array or both 'text' and 'voice_id' are required"}`, want: "invalid_request"},
		{status: http.StatusServiceUnavailable, body: `not json`, want: "provider_unavailable", retryable: true},
	}
	for _, tc := range cases {
		t.Run(strconv.Itoa(tc.status)+"/"+tc.want, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", "application/json")
				writer.WriteHeader(tc.status)
				_, _ = writer.Write([]byte(tc.body))
			}))
			t.Cleanup(server.Close)
			stream := openTTS(t, server, ttsRequest("", "ar", 24_000), TTSConfig{})
			_ = stream.AppendText(context.Background(), "مرحبا")
			err := stream.CommitText(context.Background())
			var providerErr *runtimepkg.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Code != tc.want || providerErr.Retryable != tc.retryable || providerErr.ProviderStatus != tc.status {
				t.Fatalf("CommitText error = %#v, want %s retryable=%v", err, tc.want, tc.retryable)
			}
			if strings.Contains(providerErr.Message, "Invalid API key") {
				t.Fatalf("message quotes the vendor text: %q", providerErr.Message)
			}
		})
	}
}

func TestSuccessWithoutAudioIsARetryableFailure(t *testing.T) {
	t.Parallel()
	for name, body := range map[string][]byte{"empty": nil, "one byte": {7}} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", rawContentType(24_000))
				_, _ = writer.Write(body)
			}))
			t.Cleanup(server.Close)
			stream := openTTS(t, server, ttsRequest("", "ar", 24_000), TTSConfig{})
			_ = stream.AppendText(context.Background(), "ok")
			if err := stream.CommitText(context.Background()); err != nil {
				t.Fatal(err)
			}
			event := nextEvent(t, stream)
			var providerErr *runtimepkg.ProviderError
			if !errors.As(event.Err, &providerErr) || providerErr.Code != "provider_unavailable" || !providerErr.Retryable || event.Billing == nil || event.Billing.Complete {
				t.Fatalf("event = %+v, want a retryable failure with incomplete billing", event)
			}
		})
	}
}

// A failure after the first byte cuts the chunked body. The audio already
// delivered must not be followed by audio.done.
func TestTruncatedStreamIsAFailureNotAShortUtterance(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", rawContentType(24_000))
		writer.Header().Set("Content-Length", "1000")
		_, _ = writer.Write([]byte{1, 2, 3, 4})
		writer.(http.Flusher).Flush()
		conn, _, err := writer.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}))
	t.Cleanup(server.Close)
	stream := openTTS(t, server, ttsRequest("", "ar", 24_000), TTSConfig{})
	_ = stream.AppendText(context.Background(), "مرحبا")
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}
	for {
		event := nextEvent(t, stream)
		if event.Type == protocol.EventAudioDone {
			t.Fatal("a torn stream reported audio.done")
		}
		if event.Err != nil {
			var providerErr *runtimepkg.ProviderError
			if !errors.As(event.Err, &providerErr) || providerErr.Code != "provider_unavailable" || !providerErr.Retryable {
				t.Fatalf("error = %v", event.Err)
			}
			return
		}
	}
}

func TestInputLimitCountsCharacters(t *testing.T) {
	t.Parallel()
	adapter, err := NewTTS(TTSConfig{})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := adapter.Open(context.Background(), ttsRequest(testTTSEndpoint, "ar", 24_000))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()
	if err := stream.AppendText(context.Background(), strings.Repeat("م", maxInputRunes-1)); err != nil {
		t.Fatalf("text under the ceiling must fit: %v", err)
	}
	var providerErr *runtimepkg.ProviderError
	if err := stream.AppendText(context.Background(), "مر"); !errors.As(err, &providerErr) || providerErr.Code != "input_too_large" {
		t.Fatalf("AppendText error = %v, want input_too_large", err)
	}
	if err := stream.AppendText(context.Background(), "م"); err != nil {
		t.Fatalf("exactly the ceiling must fit: %v", err)
	}
}

// Munsit bills the text as sent, whitespace included.
func TestSynthesisReportsCreditsPerUtterance(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", rawContentType(24_000))
		_, _ = w.Write([]byte{1, 0, 2, 0})
	}))
	defer server.Close()
	stream := openTTS(t, server, ttsRequest("", "en", 24_000), TTSConfig{})
	var previous string
	for _, tc := range []struct {
		text    string
		credits int64
	}{{"ok", 4_000}, {"   ok   ", 16_000}, {"مرحبا بكم في منصتنا", 38_000}} {
		if err := stream.AppendText(context.Background(), tc.text); err != nil {
			t.Fatal(err)
		}
		if err := stream.CommitText(context.Background()); err != nil {
			t.Fatal(err)
		}
		_, done := drainAudio(t, stream)
		if done.Billing == nil || !done.Billing.Complete || done.Billing.OperationID == previous || done.Billing.Quantities["credits"] != tc.credits {
			t.Fatalf("%q billing = %+v, want credits %d", tc.text, done.Billing, tc.credits)
		}
		previous = done.Billing.OperationID
		// audio.done can reach the consumer just before the reader releases its
		// in-flight marker; Close is intentionally avoided between utterances.
		s := stream.(*ttsStream)
		s.stateMu.Lock()
		requestDone := s.requestDone
		s.stateMu.Unlock()
		if requestDone != nil {
			<-requestDone
		}
	}
}

func TestIdleCancelKeepsTheStreamOpen(t *testing.T) {
	t.Parallel()
	pcm := []byte{1, 2, 3, 4}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", rawContentType(24_000))
		_, _ = w.Write(pcm)
	}))
	t.Cleanup(server.Close)
	stream := openTTS(t, server, ttsRequest("", "ar", 24_000), TTSConfig{})
	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatalf("idle cancellation closed a live session: %v", err)
	}
	if err := stream.AppendText(context.Background(), "next turn"); err != nil {
		t.Fatalf("session unusable after idle cancel: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("next turn commit: %v", err)
	}
	if audio, _ := drainAudio(t, stream); string(audio) != string(pcm) {
		t.Fatalf("next turn audio=%v, want %v", audio, pcm)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := stream.Cancel(context.Background()); !errors.Is(err, runtimepkg.ErrSessionClosed) {
		t.Fatalf("closed session cancel=%v", err)
	}
}

// A vendor that accepts the connection but never answers must not hold
// CommitText, and with it the runtime's provider lock, indefinitely.
func TestCommitTextBoundsTheWaitForResponseHeaders(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		select {
		case <-release:
		case <-request.Context().Done():
		}
	}))
	defer server.Close()
	defer close(release)
	stream := openTTS(t, server, ttsRequest("", "ar", 24_000), TTSConfig{HeaderTimeout: 50 * time.Millisecond})
	_ = stream.AppendText(context.Background(), "مرحبا")
	started := time.Now()
	err := stream.CommitText(context.Background())
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "request_timeout" || !providerErr.Retryable {
		t.Fatalf("CommitText error = %v, want retryable request_timeout", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("CommitText took %s, want it bounded by the header timeout", elapsed)
	}
}

// A 3xx must not replay the API key to a URL the endpoint policy never
// checked; it surfaces as a rejection instead.
func TestCommitTextDoesNotFollowRedirects(t *testing.T) {
	t.Parallel()
	followed := make(chan struct{}, 1)
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { followed <- struct{}{} }))
	t.Cleanup(target.Close)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL+ttsPath, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(server.Close)
	stream := openTTS(t, server, ttsRequest("", "ar", 24_000), TTSConfig{})
	_ = stream.AppendText(context.Background(), "مرحبا")
	err := stream.CommitText(context.Background())
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.ProviderStatus != http.StatusTemporaryRedirect {
		t.Fatalf("CommitText error = %v, want the redirect surfaced as a rejection", err)
	}
	select {
	case <-followed:
		t.Fatal("the redirect was followed")
	default:
	}
}
