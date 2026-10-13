package hopper

import (
	"bytes"
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

const testTTSEndpoint = "https://api.withhopper.com/v1/audio/speech"

func ttsRequest(endpoint string, rate int) runtimepkg.AdapterRequest {
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindTTS,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged},
			Route: protocol.PlanRoute{Provider: ProviderName, Model: DefaultTTSModel, Adapter: TTSAdapterID, Transport: protocol.TransportHTTP, Endpoint: endpoint,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialRelayAccess, Value: "sk_hopper_test", ExpiresAt: time.Now().Add(time.Minute)}},
		},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: rate, Channels: 1},
		Options: protocol.RequestOptions{Voice: "voice_ava"},
	}
}

func pcmServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
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
	request.Plan.Route.Endpoint = server.URL + speechPath
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
	server := pcmServer(t, func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != speechPath || request.Header.Get("Authorization") != "Bearer sk_hopper_test" {
			t.Errorf("request = %s %s %v", request.Method, request.URL.Path, request.Header)
		}
		raw, _ := io.ReadAll(request.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		bodies <- body
		writer.Header().Set("Content-Type", "audio/pcm")
		_, _ = writer.Write(pcm)
	})
	stream := openTTS(t, server, ttsRequest("", 24_000), TTSConfig{})
	if err := stream.AppendText(context.Background(), "Thanks for "); err != nil {
		t.Fatal(err)
	}
	if err := stream.AppendText(context.Background(), "calling."); err != nil {
		t.Fatal(err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("CommitText: %v", err)
	}
	body := <-bodies
	// No speed, language or any other generation parameter: qwen3-tts
	// answers each with 400 unsupported_parameter.
	want := map[string]any{"model": DefaultTTSModel, "voice": "voice_ava", "input": "Thanks for calling.", "response_format": "pcm", "stream": true}
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
	// 12 bytes of 24 kHz PCM16 is 12/48000 of a second.
	if done.Billing == nil || !done.Billing.Complete || done.Billing.Quantities["duration_seconds"] != 12 || done.Billing.QuantityDenominator("duration_seconds") != 48_000 || done.Billing.Model != DefaultTTSModel {
		t.Fatalf("audio.done billing = %+v", done.Billing)
	}
	if err := done.Billing.Validate(); err != nil {
		t.Fatal(err)
	}
	if merged, err := protocol.MergeBillingObservation(*started.Billing, *done.Billing); err != nil || !merged.Complete {
		t.Fatalf("started and done observations do not merge: %+v %v", merged, err)
	}
}

func TestVoicePrefersCallerThenPlanThenDefault(t *testing.T) {
	t.Parallel()
	cases := []struct{ requested, planned, want string }{
		{requested: " voice_ava ", planned: "voice_ben", want: "voice_ava"},
		{planned: "voice_ben", want: "voice_ben"},
		{want: "elena"},
	}
	for _, tc := range cases {
		if got := ttsVoice(tc.requested, tc.planned); got != tc.want {
			t.Errorf("ttsVoice(%q, %q) = %q, want %q", tc.requested, tc.planned, got, tc.want)
		}
	}
}

func TestOpenRefusesWhatTheEndpointCannotServe(t *testing.T) {
	t.Parallel()
	adapter, err := NewTTS(TTSConfig{})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := adapter.Open(context.Background(), ttsRequest(testTTSEndpoint, 24_000))
	if err != nil {
		t.Fatal(err)
	}
	_ = stream.Close(context.Background())
	model := ttsRequest(testTTSEndpoint, 24_000)
	model.Plan.Route.Model = "qwen3-tts-fast"
	stereo := ttsRequest(testTTSEndpoint, 24_000)
	stereo.Media.Channels = 2
	offRelay := ttsRequest(testTTSEndpoint, 24_000)
	offRelay.Plan.Execution.ProviderRoute = protocol.RouteProviderDirect
	for name, request := range map[string]runtimepkg.AdapterRequest{
		"model":               model,
		"rate":                ttsRequest(testTTSEndpoint, 16_000),
		"stereo":              stereo,
		"relay key off relay": offRelay,
		"path":                ttsRequest("https://api.withhopper.com/v1/audio/transcriptions", 24_000),
		"host":                ttsRequest("https://api.example.com/v1/audio/speech", 24_000),
	} {
		if _, err := adapter.Open(context.Background(), request); err == nil {
			t.Errorf("%s: Open succeeded, want refusal", name)
		}
	}
	_, err = adapter.Open(context.Background(), ttsRequest(testTTSEndpoint, 16_000))
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "unsupported_media" || providerErr.Hint == "" {
		t.Fatalf("16 kHz error = %v, want unsupported_media with a hint", err)
	}
}

func TestSynthesisRejectionsKeepTheirClassification(t *testing.T) {
	t.Parallel()
	envelope := func(code string) string {
		return `{"error":{"message":"vendor detail","type":"invalid_request_error","code":"` + code + `","param":null}}`
	}
	cases := []struct {
		status    int
		body      string
		want      string
		retryable bool
	}{
		{status: http.StatusBadRequest, body: envelope("unsupported_parameter"), want: "invalid_request"},
		{status: http.StatusBadRequest, body: envelope("transcript_too_long"), want: "input_too_large"},
		{status: http.StatusBadRequest, body: envelope("invalid_transcript"), want: "invalid_request"},
		{status: http.StatusNotFound, body: envelope("voice_not_found"), want: "invalid_request"},
		{status: http.StatusUnauthorized, body: envelope("invalid_api_key"), want: "authentication_failed"},
		{status: http.StatusPaymentRequired, body: envelope("insufficient_quota"), want: "provider_quota_exceeded"},
		{status: http.StatusTooManyRequests, body: envelope("rate_limit_exceeded"), want: "provider_rate_limited", retryable: true},
		{status: http.StatusBadGateway, body: envelope("upstream_unavailable"), want: "provider_unavailable", retryable: true},
		{status: http.StatusGatewayTimeout, body: envelope("upstream_timeout"), want: "request_timeout", retryable: true},
		{status: http.StatusServiceUnavailable, body: `not json`, want: "provider_unavailable", retryable: true},
	}
	for _, tc := range cases {
		t.Run(strconv.Itoa(tc.status)+"/"+tc.want, func(t *testing.T) {
			t.Parallel()
			server := pcmServer(t, func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", "application/json")
				writer.WriteHeader(tc.status)
				_, _ = writer.Write([]byte(tc.body))
			})
			stream := openTTS(t, server, ttsRequest("", 24_000), TTSConfig{})
			_ = stream.AppendText(context.Background(), "hello")
			err := stream.CommitText(context.Background())
			var providerErr *runtimepkg.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Code != tc.want || providerErr.Retryable != tc.retryable || providerErr.ProviderStatus != tc.status {
				t.Fatalf("CommitText error = %#v, want %s retryable=%v", err, tc.want, tc.retryable)
			}
			if strings.Contains(providerErr.Message, "vendor detail") {
				t.Fatalf("message quotes the vendor text: %q", providerErr.Message)
			}
		})
	}
}

func TestUnexpectedContentTypeIsARejectionNotAudio(t *testing.T) {
	t.Parallel()
	server := pcmServer(t, func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "audio/wav")
		_, _ = writer.Write([]byte("RIFF....WAVE"))
	})
	stream := openTTS(t, server, ttsRequest("", 24_000), TTSConfig{})
	_ = stream.AppendText(context.Background(), "hello")
	err := stream.CommitText(context.Background())
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "provider_unavailable" || !providerErr.Retryable {
		t.Fatalf("CommitText = %v, want a retryable rejection", err)
	}
}

func TestSuccessWithoutAudioIsARetryableFailure(t *testing.T) {
	t.Parallel()
	for name, body := range map[string][]byte{"empty": nil, "one byte": {7}} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			server := pcmServer(t, func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", "audio/pcm")
				_, _ = writer.Write(body)
			})
			stream := openTTS(t, server, ttsRequest("", 24_000), TTSConfig{})
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
	server := pcmServer(t, func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "audio/pcm")
		writer.Header().Set("Content-Length", "1000")
		_, _ = writer.Write([]byte{1, 2, 3, 4})
		writer.(http.Flusher).Flush()
		conn, _, err := writer.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	})
	stream := openTTS(t, server, ttsRequest("", 24_000), TTSConfig{})
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
			if event.Billing == nil || event.Billing.Complete || event.Billing.Quantities["duration_seconds"] != 4 {
				t.Fatalf("billing = %+v, want the delivered audio, incomplete", event.Billing)
			}
			return
		}
	}
}

// A clean EOF on an odd byte leaves half a sample behind. The whole sample
// before it is delivered, but the synthesis fails rather than completing.
func TestTrailingOddByteIsAFailureNotAShortUtterance(t *testing.T) {
	t.Parallel()
	server := pcmServer(t, func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "audio/pcm")
		_, _ = writer.Write([]byte{1, 2, 3})
	})
	stream := openTTS(t, server, ttsRequest("", 24_000), TTSConfig{AudioChunkBytes: 2})
	_ = stream.AppendText(context.Background(), "hello")
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}
	var audio []byte
	for {
		event := nextEvent(t, stream)
		if event.Type == protocol.EventAudioDone {
			t.Fatal("a body ending mid-sample reported audio.done")
		}
		if event.Billing != nil && event.Billing.Complete {
			t.Fatalf("billing = %+v, want incomplete", event.Billing)
		}
		audio = append(audio, event.Audio...)
		if event.Err != nil {
			var providerErr *runtimepkg.ProviderError
			if !errors.As(event.Err, &providerErr) || providerErr.Code != "provider_unavailable" || !providerErr.Retryable {
				t.Fatalf("error = %v", event.Err)
			}
			if event.Billing == nil || event.Billing.Quantities["duration_seconds"] != 2 {
				t.Fatalf("billing = %+v, want the one whole sample, incomplete", event.Billing)
			}
			if !bytes.Equal(audio, []byte{1, 2}) {
				t.Fatalf("audio = %v, want only the whole sample", audio)
			}
			return
		}
	}
}

// Hopper refuses input past 1000 characters, so the buffer is refused here
// with a hint rather than sent to fail.
func TestInputLimitIs1000Characters(t *testing.T) {
	t.Parallel()
	adapter, err := NewTTS(TTSConfig{})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := adapter.Open(context.Background(), ttsRequest(testTTSEndpoint, 24_000))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()
	if err := stream.AppendText(context.Background(), strings.Repeat("é", maxInputRunes-1)); err != nil {
		t.Fatalf("text under the ceiling must fit: %v", err)
	}
	var providerErr *runtimepkg.ProviderError
	if err := stream.AppendText(context.Background(), "ab"); !errors.As(err, &providerErr) || providerErr.Code != "input_too_large" || providerErr.Hint == "" {
		t.Fatalf("AppendText error = %v, want input_too_large with a hint", err)
	}
	if err := stream.AppendText(context.Background(), "a"); err != nil {
		t.Fatalf("exactly the ceiling must fit: %v", err)
	}
	if maxInputRunes != 1_000 {
		t.Fatalf("maxInputRunes = %d, want Hopper's 1000", maxInputRunes)
	}
}

// Hopper bills synthesis on generated audio, so each utterance reports its own
// PCM duration under its own operation.
func TestSynthesisBillsGeneratedAudioPerUtterance(t *testing.T) {
	t.Parallel()
	server := pcmServer(t, func(w http.ResponseWriter, r *http.Request) {
		var body speechRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "audio/pcm")
		_, _ = w.Write(make([]byte, 2*len(body.Input)))
	})
	stream := openTTS(t, server, ttsRequest("", 24_000), TTSConfig{AudioChunkBytes: 1024})
	var previous string
	for _, text := range []string{"ok", "hello there"} {
		if err := stream.AppendText(context.Background(), text); err != nil {
			t.Fatal(err)
		}
		if err := stream.CommitText(context.Background()); err != nil {
			t.Fatal(err)
		}
		_, done := drainAudio(t, stream)
		if done.Billing == nil || !done.Billing.Complete || done.Billing.OperationID == previous || done.Billing.Quantities["duration_seconds"] != int64(2*len(text)) {
			t.Fatalf("%q billing = %+v", text, done.Billing)
		}
		previous = done.Billing.OperationID
	}
}

// A caller that starts the next utterance as soon as it sees audio.done must
// find the previous one finished. A one-slot buffer holds the reader inside
// its audio.done send, which makes the ordering observable without a race.
func TestNextUtteranceMayStartOnAudioDone(t *testing.T) {
	t.Parallel()
	server := pcmServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "audio/pcm")
		_, _ = w.Write([]byte{1, 0, 2, 0})
	})
	stream := openTTS(t, server, ttsRequest("", 24_000), TTSConfig{EventBuffer: 1, AudioChunkBytes: 1024})
	_ = stream.AppendText(context.Background(), "hello")
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}
	if started := nextEvent(t, stream); started.Type != protocol.EventAudioStarted {
		t.Fatalf("first event = %s", started.Type)
	}
	// The frame now fills the buffer and the reader waits to send audio.done.
	time.Sleep(50 * time.Millisecond)
	if err := stream.AppendText(context.Background(), "next"); err != nil {
		t.Fatalf("slot still held while audio.done is being published: %v", err)
	}
	if frame := nextEvent(t, stream); frame.Type != protocol.EventAudioFrame {
		t.Fatalf("second event = %s", frame.Type)
	}
	if done := nextEvent(t, stream); done.Type != protocol.EventAudioDone {
		t.Fatalf("third event = %s", done.Type)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("next utterance right after audio.done: %v", err)
	}
	if audio, _ := drainAudio(t, stream); len(audio) != 4 {
		t.Fatalf("next utterance audio = %v", audio)
	}
}

// Releasing the slot before audio.done must not let a graceful Close cut
// that event off: Close waits for the reader, not the slot.
func TestGracefulCloseStillDeliversAudioDone(t *testing.T) {
	t.Parallel()
	server := pcmServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "audio/pcm")
		for range 5 {
			_, _ = w.Write([]byte{1, 0})
			w.(http.Flusher).Flush()
			time.Sleep(10 * time.Millisecond)
		}
	})
	stream := openTTS(t, server, ttsRequest("", 24_000), TTSConfig{EventBuffer: 1})
	_ = stream.AppendText(context.Background(), "hello")
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- stream.Close(context.Background()) }()
	var audio []byte
	sawDone := false
	for event := range stream.Events() {
		if event.Err != nil {
			t.Fatalf("event error: %v", event.Err)
		}
		audio = append(audio, event.Audio...)
		sawDone = sawDone || event.Type == protocol.EventAudioDone
	}
	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !sawDone || len(audio) != 10 {
		t.Fatalf("audio.done=%v audio=%d bytes, want the whole utterance and audio.done", sawDone, len(audio))
	}
}

// Error headers that arrive promptly followed by a body that never ends must
// not hold CommitText, and with it the runtime's provider lock.
func TestCommitTextBoundsTheErrorBody(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"code":`))
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	defer close(release)
	stream := openTTS(t, server, ttsRequest("", 24_000), TTSConfig{HeaderTimeout: 50 * time.Millisecond})
	_ = stream.AppendText(context.Background(), "hello")
	result := make(chan error, 1)
	go func() { result <- stream.CommitText(context.Background()) }()
	var err error
	select {
	case err = <-result:
	case <-time.After(2 * time.Second):
		t.Fatal("CommitText is still reading a stalled error body")
	}
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "authentication_failed" || providerErr.ProviderStatus != http.StatusUnauthorized {
		t.Fatalf("CommitText error = %#v, want the status classification", err)
	}
	if err := stream.AppendText(context.Background(), "next"); err != nil {
		t.Fatalf("session unusable after a bounded error body: %v", err)
	}
}

func TestIdleCancelKeepsTheStreamOpen(t *testing.T) {
	t.Parallel()
	pcm := []byte{1, 2, 3, 4}
	server := pcmServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "audio/pcm")
		_, _ = w.Write(pcm)
	})
	stream := openTTS(t, server, ttsRequest("", 24_000), TTSConfig{})
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
	stream := openTTS(t, server, ttsRequest("", 24_000), TTSConfig{HeaderTimeout: 50 * time.Millisecond})
	_ = stream.AppendText(context.Background(), "hello")
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
	target := pcmServer(t, func(http.ResponseWriter, *http.Request) { followed <- struct{}{} })
	server := pcmServer(t, func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL+speechPath, http.StatusTemporaryRedirect)
	})
	stream := openTTS(t, server, ttsRequest("", 24_000), TTSConfig{})
	_ = stream.AppendText(context.Background(), "hello")
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
