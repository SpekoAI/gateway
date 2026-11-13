package azure

import (
	"context"
	"errors"
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

type capturedSynthesis struct {
	path, key, contentType, outputFormat, userAgent string
	body                                            string
}

// newFakeSynthesis answers one SSML POST with the given status and writes
// the body in the given chunks, flushing between them, so the client sees
// chunk boundaries that are not sample boundaries.
func newFakeSynthesis(t *testing.T, status int, contentType string, chunks ...[]byte) (*httptest.Server, chan capturedSynthesis) {
	t.Helper()
	captured := make(chan capturedSynthesis, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		captured <- capturedSynthesis{
			path: request.URL.Path, key: request.Header.Get(subscriptionHeader), contentType: request.Header.Get("Content-Type"),
			outputFormat: request.Header.Get("X-Microsoft-OutputFormat"), userAgent: request.Header.Get("User-Agent"), body: string(body),
		}
		if contentType != "" {
			writer.Header().Set("Content-Type", contentType)
		}
		writer.Header().Set("X-RequestId", "tts-req-1")
		writer.WriteHeader(status)
		for _, chunk := range chunks {
			_, _ = writer.Write(chunk)
			if flusher, ok := writer.(http.Flusher); ok {
				flusher.Flush()
			}
		}
	}))
	t.Cleanup(server.Close)
	return server, captured
}

func ttsAdapterFor(t *testing.T, server *httptest.Server) *TTSAdapter {
	t.Helper()
	endpoint, _ := url.Parse(server.URL)
	adapter, err := NewTTS(TTSConfig{AllowedEndpointHosts: []string{endpoint.Hostname()}, AllowInsecureEndpoint: true, GracefulCloseIdleTimeout: time.Second})
	if err != nil {
		t.Fatalf("NewTTS: %v", err)
	}
	return adapter
}

func ttsRequest(endpoint, model, voice string) runtimepkg.AdapterRequest {
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindTTS,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged},
			Route: protocol.PlanRoute{Provider: ProviderName, Model: model, Adapter: TTSAdapterID, Transport: protocol.TransportHTTP, Endpoint: endpoint + ttsPath,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialRelayAccess, Value: "test-speech-key", ExpiresAt: time.Now().Add(time.Minute)}},
		},
		Options: protocol.RequestOptions{Voice: voice},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1},
	}
}

func drainTTS(t *testing.T, events <-chan runtimepkg.ProviderEvent) ([]runtimepkg.ProviderEvent, error) {
	t.Helper()
	var collected []runtimepkg.ProviderEvent
	timeout := time.After(3 * time.Second)
	for {
		select {
		case event, ok := <-events:
			if !ok {
				return collected, nil
			}
			if event.Err != nil {
				return collected, event.Err
			}
			collected = append(collected, event)
			if event.Type == protocol.EventAudioDone {
				return collected, nil
			}
		case <-timeout:
			t.Fatalf("timed out after %d events", len(collected))
		}
	}
}

func TestTTSPostsSSMLAndStreamsSampleAlignedPCM(t *testing.T) {
	t.Parallel()
	// audio/basic is what Azure actually labels raw PCM with (measured live).
	server, captured := newFakeSynthesis(t, http.StatusOK, "audio/basic", []byte{1, 2, 3}, []byte{4, 5}, []byte{6, 7, 8})
	stream, err := ttsAdapterFor(t, server).Open(context.Background(), ttsRequest(server.URL, TTSModelMAIVoice21Flash, "en-GB-Emily"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := stream.AppendText(context.Background(), `Fish & chips <now>, "please"`); err != nil {
		t.Fatalf("AppendText: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("CommitText: %v", err)
	}
	request := <-captured
	if request.path != ttsPath || request.key != "test-speech-key" || request.contentType != "application/ssml+xml" ||
		request.outputFormat != "raw-24khz-16bit-mono-pcm" || request.userAgent == "" {
		t.Fatalf("request = %+v", request)
	}
	want := `<speak version="1.0" xmlns="http://www.w3.org/2001/10/synthesis" xml:lang="en-GB"><voice name="en-GB-Emily:MAI-Voice-2.1-Flash">` +
		`Fish &amp; chips &lt;now&gt;, &#34;please&#34;</voice></speak>`
	if request.body != want {
		t.Fatalf("ssml =\n%s\nwant\n%s", request.body, want)
	}
	events, err := drainTTS(t, stream.Events())
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	var audio []byte
	var types []protocol.EventType
	for _, event := range events {
		types = append(types, event.Type)
		if event.Type == protocol.EventAudioFrame {
			if len(event.Audio)%2 != 0 {
				t.Fatalf("frame of %d bytes splits a sample", len(event.Audio))
			}
			audio = append(audio, event.Audio...)
		}
	}
	if string(audio) != string([]byte{1, 2, 3, 4, 5, 6, 7, 8}) {
		t.Fatalf("audio = %v", audio)
	}
	if types[0] != protocol.EventUsageObserved || types[1] != protocol.EventAudioStarted || types[len(types)-1] != protocol.EventAudioDone {
		t.Fatalf("event order = %v", types)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestSSMLVoiceNameMakesTheRoutedModelAuthoritative(t *testing.T) {
	t.Parallel()
	for _, test := range []struct{ voice, model, want string }{
		{"", TTSModelMAIVoice21, "en-US-Harper:MAI-Voice-2.1"},
		{"en-IN-Priya", TTSModelMAIVoice21, "en-IN-Priya:MAI-Voice-2.1"},
		{"en-US-Harper:MAI-Voice-2", TTSModelMAIVoice21Flash, "en-US-Harper:MAI-Voice-2.1-Flash"},
		{"fr-FR-Soleil:mai-voice-2.1", TTSModelMAIVoice21Flash, "fr-FR-Soleil:MAI-Voice-2.1-Flash"},
		{"fil-PH-Grant", TTSModelMAIVoice21, "fil-PH-Grant:MAI-Voice-2.1"},
	} {
		got, err := SSMLVoiceName(test.voice, test.model)
		if err != nil || got != test.want {
			t.Fatalf("SSMLVoiceName(%q, %q) = %q, %v; want %q", test.voice, test.model, got, err, test.want)
		}
	}
	for _, voice := range []string{"en-US-Ava:DragonHDLatestNeural", "Harper", `en-US-Harper"/><x`, "MAI-Voice-2.1"} {
		_, err := SSMLVoiceName(voice, TTSModelMAIVoice21)
		var providerErr *runtimepkg.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != "invalid_request" {
			t.Fatalf("SSMLVoiceName(%q) err = %v, want invalid_request", voice, err)
		}
	}
}

func TestTTSRefusesMismatchedPlans(t *testing.T) {
	t.Parallel()
	server, _ := newFakeSynthesis(t, http.StatusOK, "audio/x-wav", []byte{0, 0})
	adapter := ttsAdapterFor(t, server)
	for name, mutate := range map[string]func(*runtimepkg.AdapterRequest){
		"model":     func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "MAI-Voice-2" },
		"provider":  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Provider = "openai" },
		"transport": func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Transport = protocol.TransportWebSocket },
		"rate":      func(r *runtimepkg.AdapterRequest) { r.Media.SampleRateHz = 32_000 },
		"stereo":    func(r *runtimepkg.AdapterRequest) { r.Media.Channels = 2 },
		"path":      func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = server.URL + "/tts/cognitiveservices/v1" },
		"host": func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Endpoint = "https://eastus.tts.speech.microsoft.com.evil.test" + ttsPath
		},
		"kind":  func(r *runtimepkg.AdapterRequest) { r.Kind = protocol.SessionKindSTT },
		"voice": func(r *runtimepkg.AdapterRequest) { r.Options.Voice = "en-US-Ava:DragonHDLatestNeural" },
	} {
		request := ttsRequest(server.URL, TTSModelMAIVoice21, "")
		mutate(&request)
		if _, err := adapter.Open(context.Background(), request); err == nil {
			t.Fatalf("%s: Open accepted a mismatched plan", name)
		}
	}
}

func TestTTSOutputFormatFollowsTheRequestedRate(t *testing.T) {
	t.Parallel()
	server, captured := newFakeSynthesis(t, http.StatusOK, "audio/x-wav", []byte{0, 0})
	request := ttsRequest(server.URL, TTSModelMAIVoice21, "")
	request.Media.SampleRateHz = 8_000
	stream, err := ttsAdapterFor(t, server).Open(context.Background(), request)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = stream.AppendText(context.Background(), "hi")
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("CommitText: %v", err)
	}
	if got := (<-captured).outputFormat; got != "raw-8khz-16bit-mono-pcm" {
		t.Fatalf("output format = %q", got)
	}
	_ = stream.Close(context.Background())
}

func TestTTSClassifiesUpstreamRefusals(t *testing.T) {
	t.Parallel()
	for status, want := range map[int]struct {
		code      string
		retryable bool
	}{
		http.StatusBadRequest:          {"invalid_request", false},
		http.StatusUnauthorized:        {"authentication_failed", false},
		http.StatusTooManyRequests:     {"provider_rate_limited", true},
		http.StatusInternalServerError: {"provider_unavailable", true},
	} {
		server, _ := newFakeSynthesis(t, status, "", nil)
		stream, err := ttsAdapterFor(t, server).Open(context.Background(), ttsRequest(server.URL, TTSModelMAIVoice21, ""))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		_ = stream.AppendText(context.Background(), "hello")
		err = stream.CommitText(context.Background())
		var providerErr *runtimepkg.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != want.code || providerErr.Retryable != want.retryable || providerErr.ProviderStatus != status {
			t.Fatalf("status %d: err = %#v", status, err)
		}
		_ = stream.Close(context.Background())
	}
}

func TestTTSRefusesAnErrorPageBehindA200(t *testing.T) {
	t.Parallel()
	server, _ := newFakeSynthesis(t, http.StatusOK, "application/json", []byte(`{"error":"nope"}`))
	stream, err := ttsAdapterFor(t, server).Open(context.Background(), ttsRequest(server.URL, TTSModelMAIVoice21, ""))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = stream.AppendText(context.Background(), "hello")
	if err := stream.CommitText(context.Background()); err == nil || !strings.Contains(err.Error(), "content type") {
		t.Fatalf("CommitText err = %v", err)
	}
	_ = stream.Close(context.Background())
}

func TestTTSReportsASuccessWithoutAudio(t *testing.T) {
	t.Parallel()
	server, _ := newFakeSynthesis(t, http.StatusOK, "audio/x-wav")
	stream, err := ttsAdapterFor(t, server).Open(context.Background(), ttsRequest(server.URL, TTSModelMAIVoice21, ""))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = stream.AppendText(context.Background(), "hello")
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("CommitText: %v", err)
	}
	_, err = drainTTS(t, stream.Events())
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || !providerErr.Retryable || !strings.Contains(providerErr.Message, "without returning audio") {
		t.Fatalf("err = %v", err)
	}
	_ = stream.Close(context.Background())
}

func TestTTSRefusesAnOversizedUtteranceBeforeDialing(t *testing.T) {
	t.Parallel()
	server, captured := newFakeSynthesis(t, http.StatusOK, "audio/x-wav", []byte{0, 0})
	stream, err := ttsAdapterFor(t, server).Open(context.Background(), ttsRequest(server.URL, TTSModelMAIVoice21, ""))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = stream.AppendText(context.Background(), strings.Repeat("&", ttsMaxSSMLBytes/5))
	err = stream.CommitText(context.Background())
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "input_too_large" {
		t.Fatalf("err = %v", err)
	}
	select {
	case <-captured:
		t.Fatal("an oversized utterance reached the vendor")
	default:
	}
	_ = stream.Close(context.Background())
}

// The caller's cancellation must stop a synthesis whose response headers have
// not arrived, rather than leave the HTTP call running until shutdown.
func TestTTSCommitTextHonoursTheCallersCancellation(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		select {
		case <-request.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() { close(release); server.Close() })
	stream, err := ttsAdapterFor(t, server).Open(context.Background(), ttsRequest(server.URL, TTSModelMAIVoice21Flash, ""))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = stream.AppendText(context.Background(), "hello")
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	started := time.Now()
	err = stream.CommitText(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CommitText err = %v, want the caller's deadline", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("CommitText ran %s past its caller's deadline", elapsed)
	}
	_ = stream.Close(context.Background())
}

// A body cut mid-sample is damaged audio, not a completed utterance.
func TestTTSReportsAResponseCutMidSample(t *testing.T) {
	t.Parallel()
	server, _ := newFakeSynthesis(t, http.StatusOK, "audio/basic", []byte{1, 2, 3})
	stream, err := ttsAdapterFor(t, server).Open(context.Background(), ttsRequest(server.URL, TTSModelMAIVoice21, ""))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = stream.AppendText(context.Background(), "hello")
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("CommitText: %v", err)
	}
	events, err := drainTTS(t, stream.Events())
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || !strings.Contains(providerErr.Message, "mid-sample") {
		t.Fatalf("err = %v, want a mid-sample provider error", err)
	}
	for _, event := range events {
		if event.Type == protocol.EventAudioDone {
			t.Fatal("a truncated body was reported as audio.done")
		}
	}
	_ = stream.Close(context.Background())
}

func TestIdleCancelKeepsTheStreamOpen(t *testing.T) {
	t.Parallel()
	server, _ := newFakeSynthesis(t, http.StatusOK, "audio/basic", []byte{1, 2})
	adapter := ttsAdapterFor(t, server)
	stream, err := adapter.Open(context.Background(), ttsRequest(server.URL, TTSModelMAIVoice21, ""))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()
	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatalf("idle cancellation closed a live session: %v", err)
	}
	if err := stream.AppendText(context.Background(), "next turn"); err != nil {
		t.Fatalf("session unusable after idle cancel: %v", err)
	}
	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatalf("buffered text cancellation: %v", err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := stream.Cancel(context.Background()); !errors.Is(err, runtimepkg.ErrSessionClosed) {
		t.Fatalf("closed session cancel=%v", err)
	}
}
