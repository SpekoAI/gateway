package paxa

import (
	"context"
	"encoding/binary"
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

const testEndpoint = "https://api.paxalabs.com/v1/tts"

func ttsRequest(endpoint, language string) runtimepkg.AdapterRequest {
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindTTS,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged},
			Route: protocol.PlanRoute{Provider: ProviderName, Model: DefaultTTSModel, Adapter: TTSAdapterID, Transport: protocol.TransportHTTP, Endpoint: endpoint,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialRelayAccess, Value: "paxa-key", ExpiresAt: time.Now().Add(time.Minute)}},
		},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1},
		Options: protocol.RequestOptions{Language: language},
	}
}

// wavHeader is the 44-byte streaming header Paxa sends: RIFF and data sizes
// are the 0xFFFFFFFF placeholder.
func wavHeader(rate uint32, channels uint16) []byte {
	header := make([]byte, 44)
	copy(header[0:], "RIFF")
	binary.LittleEndian.PutUint32(header[4:], 0xFFFFFFFF)
	copy(header[8:], "WAVE")
	copy(header[12:], "fmt ")
	binary.LittleEndian.PutUint32(header[16:], 16)
	binary.LittleEndian.PutUint16(header[20:], 1)
	binary.LittleEndian.PutUint16(header[22:], channels)
	binary.LittleEndian.PutUint32(header[24:], rate)
	binary.LittleEndian.PutUint32(header[28:], rate*uint32(channels)*2)
	binary.LittleEndian.PutUint16(header[32:], channels*2)
	binary.LittleEndian.PutUint16(header[34:], 16)
	copy(header[36:], "data")
	binary.LittleEndian.PutUint32(header[40:], 0xFFFFFFFF)
	return header
}

func openTTS(t *testing.T, server *httptest.Server, language string) runtimepkg.ProviderStream {
	t.Helper()
	parsed, _ := url.Parse(server.URL)
	// A 5-byte read size splits the 44-byte header across reads.
	adapter, err := NewTTS(TTSConfig{AllowedEndpointHosts: []string{parsed.Hostname()}, AllowInsecureEndpoint: true, AudioChunkBytes: 5})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := adapter.Open(context.Background(), ttsRequest(server.URL+speechPath, language))
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

// drain collects audio until audio.done, failing on an error event.
func drain(t *testing.T, stream runtimepkg.ProviderStream) []byte {
	t.Helper()
	var audio []byte
	for {
		event := nextEvent(t, stream)
		if event.Err != nil {
			t.Fatalf("event error: %v", event.Err)
		}
		audio = append(audio, event.Audio...)
		if event.Type == protocol.EventAudioDone {
			return audio
		}
	}
}

func TestCommitTextSendsTheDocumentedRequestAndStripsTheWAVHeader(t *testing.T) {
	t.Parallel()
	pcm := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	bodies := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != speechPath || request.Header.Get("Authorization") != "Bearer paxa-key" {
			t.Errorf("request = %s %s %v", request.Method, request.URL.Path, request.Header)
		}
		raw, _ := io.ReadAll(request.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		bodies <- body
		writer.Header().Set("Content-Type", "audio/wav")
		writer.Header().Set(requestIDHeader, "req-tts")
		_, _ = writer.Write(append(wavHeader(24_000, 1), pcm...))
	}))
	t.Cleanup(server.Close)
	stream := openTTS(t, server, "th-TH")
	if err := stream.AppendText(context.Background(), "สวัสดีค่ะ "); err != nil {
		t.Fatal(err)
	}
	if err := stream.AppendText(context.Background(), "ยินดีต้อนรับ"); err != nil {
		t.Fatal(err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("CommitText: %v", err)
	}
	body := <-bodies
	want := map[string]any{"text": "สวัสดีค่ะ ยินดีต้อนรับ", "voice": DefaultVoice, "model": DefaultTTSModel, "format": "wav", "stream": true, "language": "th"}
	if len(body) != len(want) {
		t.Fatalf("body = %#v, want %#v", body, want)
	}
	for key, value := range want {
		if body[key] != value {
			t.Fatalf("body[%s] = %#v, want %#v", key, body[key], value)
		}
	}
	usage := nextEvent(t, stream)
	if usage.Type != protocol.EventUsageObserved || !strings.Contains(string(usage.Data), `"req-tts"`) {
		t.Fatalf("first event = %s %s", usage.Type, usage.Data)
	}
	if started := nextEvent(t, stream); started.Type != protocol.EventAudioStarted {
		t.Fatalf("second event = %s, want audio.started", started.Type)
	}
	if audio := drain(t, stream); string(audio) != string(pcm) {
		t.Fatalf("audio = %v, want only the PCM after the header", audio)
	}
}

func TestLanguagePicksTheVoiceAndTheReadingLanguage(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, language, requested, planned string
		wantVoice, wantLanguage            string
	}{
		{name: "english session takes the english voice", language: "en-US", wantVoice: DefaultEnglishVoice, wantLanguage: "en"},
		{name: "english session replaces the catalog fill", language: "en", planned: DefaultVoice, wantVoice: DefaultEnglishVoice, wantLanguage: "en"},
		{name: "english session keeps a planned choice", language: "en", planned: "toast", wantVoice: "toast", wantLanguage: "en"},
		{name: "caller voice wins", language: "en", requested: "nomyen", wantVoice: "nomyen", wantLanguage: "en"},
		{name: "thai session", language: "th", wantVoice: DefaultVoice, wantLanguage: "th"},
		{name: "no language is auto", language: "", planned: "tako", wantVoice: "tako", wantLanguage: "auto"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			adapter, err := NewTTS(TTSConfig{})
			if err != nil {
				t.Fatal(err)
			}
			request := ttsRequest(testEndpoint, tc.language)
			request.Options.Voice = tc.requested
			request.Plan.Route.Voice = tc.planned
			stream, err := adapter.Open(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()
			got := stream.(*ttsStream)
			if got.voice != tc.wantVoice || got.language != tc.wantLanguage {
				t.Fatalf("voice, language = %q, %q; want %q, %q", got.voice, got.language, tc.wantVoice, tc.wantLanguage)
			}
		})
	}
}

func TestOpenRefusesLanguagesPaxaIsNotRoutedFor(t *testing.T) {
	t.Parallel()
	adapter, err := NewTTS(TTSConfig{})
	if err != nil {
		t.Fatal(err)
	}
	for _, language := range []string{"zh", "zh-CN", "ja", "auto"} {
		_, err := adapter.Open(context.Background(), ttsRequest(testEndpoint, language))
		var providerErr *runtimepkg.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != "unsupported_language" {
			t.Errorf("%s: Open error = %v, want unsupported_language", language, err)
		}
	}
}

// A rate other than the pinned 24 kHz would play at the wrong pitch with no
// error, so the fmt chunk is checked before any audio is emitted.
func TestUnexpectedWAVFormatIsAFailureNotAudio(t *testing.T) {
	t.Parallel()
	for name, header := range map[string][]byte{
		"rate":     wavHeader(22_050, 1),
		"channels": wavHeader(24_000, 2),
		"not wav":  []byte("ID3\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", "audio/wav")
				_, _ = writer.Write(append(header, 1, 2, 3, 4))
			}))
			t.Cleanup(server.Close)
			stream := openTTS(t, server, "en")
			_ = stream.AppendText(context.Background(), "hello")
			if err := stream.CommitText(context.Background()); err != nil {
				t.Fatal(err)
			}
			event := nextEvent(t, stream)
			var providerErr *runtimepkg.ProviderError
			if !errors.As(event.Err, &providerErr) || providerErr.Code != "provider_unavailable" || !providerErr.Retryable {
				t.Fatalf("event = %s %v, want a retryable failure before any audio", event.Type, event.Err)
			}
		})
	}
}

func TestSynthesisRejectionsKeepTheirClassification(t *testing.T) {
	t.Parallel()
	cases := []struct {
		status    int
		code      string
		want      string
		retryable bool
	}{
		{status: http.StatusUnauthorized, code: "unauthorized", want: "authentication_failed"},
		{status: http.StatusPaymentRequired, code: "insufficient_credits", want: "provider_quota_exceeded"},
		{status: http.StatusForbidden, code: "key_limit", want: "provider_quota_exceeded"},
		{status: http.StatusBadRequest, code: "unknown_voice", want: "invalid_request"},
		{status: http.StatusBadRequest, code: "text_too_long", want: "input_too_large"},
		{status: http.StatusUnprocessableEntity, code: "unspeakable_text", want: "invalid_request"},
		{status: http.StatusTooManyRequests, code: "rate_limited", want: "provider_rate_limited", retryable: true},
		{status: http.StatusTooManyRequests, code: "concurrency_limited", want: "provider_rate_limited", retryable: true},
		{status: http.StatusBadGateway, code: "provider_error", want: "provider_unavailable", retryable: true},
		{status: http.StatusServiceUnavailable, code: "provider_unavailable", want: "provider_unavailable", retryable: true},
		{status: http.StatusBadGateway, code: "", want: "provider_unavailable", retryable: true},
	}
	for _, tc := range cases {
		t.Run(tc.code+"/"+http.StatusText(tc.status), func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", "application/problem+json")
				writer.WriteHeader(tc.status)
				_, _ = writer.Write([]byte(`{"title":"` + tc.code + `","status":` + strconv.Itoa(tc.status) + `}`))
			}))
			t.Cleanup(server.Close)
			stream := openTTS(t, server, "en")
			_ = stream.AppendText(context.Background(), "hello")
			err := stream.CommitText(context.Background())
			var providerErr *runtimepkg.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Code != tc.want || providerErr.Retryable != tc.retryable || providerErr.ProviderStatus != tc.status {
				t.Fatalf("CommitText error = %#v, want %s retryable=%v", err, tc.want, tc.retryable)
			}
		})
	}
}

func TestSuccessWithoutAudioIsARetryableFailure(t *testing.T) {
	t.Parallel()
	for name, body := range map[string][]byte{"empty": nil, "header only": wavHeader(24_000, 1)} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", "audio/wav")
				_, _ = writer.Write(body)
			}))
			t.Cleanup(server.Close)
			stream := openTTS(t, server, "en")
			_ = stream.AppendText(context.Background(), "hello")
			if err := stream.CommitText(context.Background()); err != nil {
				t.Fatal(err)
			}
			event := nextEvent(t, stream)
			var providerErr *runtimepkg.ProviderError
			if !errors.As(event.Err, &providerErr) || providerErr.Code != "provider_unavailable" || !providerErr.Retryable {
				t.Fatalf("event = %+v, want a retryable failure", event)
			}
		})
	}
}

// A failure after the first byte cuts the body with no trailer. The audio
// already delivered must not be followed by audio.done.
func TestTruncatedStreamIsAFailureNotAShortUtterance(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "audio/wav")
		writer.Header().Set("Content-Length", "1000")
		_, _ = writer.Write(append(wavHeader(24_000, 1), 1, 2, 3, 4))
		writer.(http.Flusher).Flush()
		conn, _, err := writer.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}))
	t.Cleanup(server.Close)
	stream := openTTS(t, server, "en")
	_ = stream.AppendText(context.Background(), "hello")
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

// Paxa counts UTF-16 code units, so a supplementary character counts twice.
func TestInputLimitCountsUTF16Units(t *testing.T) {
	t.Parallel()
	adapter, err := NewTTS(TTSConfig{})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := adapter.Open(context.Background(), ttsRequest(testEndpoint, "th"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()
	if err := stream.AppendText(context.Background(), strings.Repeat("ก", maxInputUnits-2)); err != nil {
		t.Fatalf("text under the ceiling must fit: %v", err)
	}
	var providerErr *runtimepkg.ProviderError
	if err := stream.AppendText(context.Background(), "😀x"); !errors.As(err, &providerErr) || providerErr.Code != "input_too_large" {
		t.Fatalf("AppendText error = %v, want input_too_large for 5001 units", err)
	}
	if err := stream.AppendText(context.Background(), "😀"); err != nil {
		t.Fatalf("exactly 5000 units must fit: %v", err)
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
	parsed, _ := url.Parse(server.URL)
	adapter, err := NewTTS(TTSConfig{AllowedEndpointHosts: []string{parsed.Hostname()}, AllowInsecureEndpoint: true, HeaderTimeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := adapter.Open(context.Background(), ttsRequest(server.URL+speechPath, "en"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()
	if err := stream.AppendText(context.Background(), "Hello."); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	err = stream.CommitText(context.Background())
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "request_timeout" || !providerErr.Retryable {
		t.Fatalf("CommitText error = %v, want retryable request_timeout", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("CommitText took %s, want it bounded by the header timeout", elapsed)
	}
}

func TestOpenAcceptsTheFlashModelAndNativeEndpointOnly(t *testing.T) {
	t.Parallel()
	adapter, err := NewTTS(TTSConfig{})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := adapter.Open(context.Background(), ttsRequest(testEndpoint, "en"))
	if err != nil {
		t.Fatal(err)
	}
	_ = stream.Close(context.Background())
	model := ttsRequest(testEndpoint, "en")
	model.Plan.Route.Model = "paxa-tts-pro-v1"
	media := ttsRequest(testEndpoint, "en")
	media.Media.SampleRateHz = 16_000
	for name, request := range map[string]runtimepkg.AdapterRequest{
		"model": model,
		"media": media,
		"path":  ttsRequest("https://api.paxalabs.com/v1/audio/speech", "en"),
		"host":  ttsRequest("https://api.example.com/v1/tts", "en"),
	} {
		if _, err := adapter.Open(context.Background(), request); err == nil {
			t.Errorf("%s: Open succeeded, want refusal", name)
		}
	}
}
