package mistral

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	testTTSEndpoint = "https://api.mistral.ai/v1/audio/speech"
	// liveDoneEvent is the stream's last event as it arrived on 2026-10-21.
	liveDoneEvent = "event: speech.audio.done\ndata: {\"type\":\"speech.audio.done\",\"usage\":{\"prompt_audio_seconds\":null,\"prompt_tokens\":14,\"total_tokens\":7694,\"completion_tokens\":7680,\"request_count\":null}}\n\n"
)

func ttsRequest(endpoint string, rate int) runtimepkg.AdapterRequest {
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindTTS,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged},
			Route: protocol.PlanRoute{Provider: ProviderName, Model: DefaultTTSModel, Adapter: TTSAdapterID, Transport: protocol.TransportHTTP, Endpoint: endpoint,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialRelayAccess, Value: "mistral_test_key", ExpiresAt: time.Now().Add(time.Minute)}},
		},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: rate, Channels: 1},
		Options: protocol.RequestOptions{Voice: "fr_marie_neutral"},
	}
}

// floats encodes samples as raw float32 little-endian, the pcm format.
func floats(samples ...float32) []byte {
	raw := make([]byte, 4*len(samples))
	for index, sample := range samples {
		binary.LittleEndian.PutUint32(raw[4*index:], math.Float32bits(sample))
	}
	return raw
}

func deltaEvent(raw []byte) string {
	return fmt.Sprintf("event: speech.audio.delta\ndata: {\"type\":\"speech.audio.delta\",\"audio_data\":%q}\n\n", base64.StdEncoding.EncodeToString(raw))
}

func sseServer(t *testing.T, events ...string) *httptest.Server {
	t.Helper()
	return ttsServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		for _, event := range events {
			_, _ = io.WriteString(w, event)
			w.(http.Flusher).Flush()
		}
	})
}

func ttsServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
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

// drainAudio collects audio until audio.done, failing on an error event.
func drainAudio(t *testing.T, stream runtimepkg.ProviderStream) ([]byte, []runtimepkg.ProviderEvent) {
	t.Helper()
	var audio []byte
	var events []runtimepkg.ProviderEvent
	for {
		event := nextEvent(t, stream)
		if event.Err != nil {
			t.Fatalf("event error: %v", event.Err)
		}
		if len(event.Audio)%2 == 1 {
			t.Fatalf("frame of %d bytes splits a sample", len(event.Audio))
		}
		events = append(events, event)
		audio = append(audio, event.Audio...)
		if event.Type == protocol.EventAudioDone {
			return audio, events
		}
	}
}

// failure reads events until the first error, which must be a provider error.
func failure(t *testing.T, stream runtimepkg.ProviderStream) *runtimepkg.ProviderError {
	t.Helper()
	for {
		event := nextEvent(t, stream)
		if event.Type == protocol.EventAudioDone {
			t.Fatal("audio.done where a failure was expected")
		}
		if event.Err != nil {
			var providerErr *runtimepkg.ProviderError
			if !errors.As(event.Err, &providerErr) {
				t.Fatalf("error = %v, want a provider error", event.Err)
			}
			return providerErr
		}
	}
}

func commit(t *testing.T, stream runtimepkg.ProviderStream, text string) error {
	t.Helper()
	if err := stream.AppendText(context.Background(), text); err != nil {
		t.Fatal(err)
	}
	return stream.CommitText(context.Background())
}

func TestCommitTextSendsTheDocumentedRequestAndConvertsFloats(t *testing.T) {
	t.Parallel()
	bodies := make(chan map[string]any, 1)
	headers := make(chan http.Header, 1)
	samples := floats(0, 0.5, -0.5, 1, -1, 2, -3)
	server := ttsServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != speechPath {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		bodies <- body
		headers <- r.Header.Clone()
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		// The first delta ends mid-sample: its last two bytes open the next
		// delta's first float.
		_, _ = io.WriteString(w, deltaEvent(samples[:10]))
		w.(http.Flusher).Flush()
		_, _ = io.WriteString(w, deltaEvent(samples[10:]))
		_, _ = io.WriteString(w, liveDoneEvent)
	})
	stream := openTTS(t, server, ttsRequest("", 24_000), TTSConfig{})
	if err := stream.AppendText(context.Background(), "Bonjour et "); err != nil {
		t.Fatal(err)
	}
	if err := commit(t, stream, "bienvenue."); err != nil {
		t.Fatalf("CommitText: %v", err)
	}
	header := <-headers
	if header.Get("Authorization") != "Bearer mistral_test_key" || header.Get("Accept") != "text/event-stream" {
		t.Fatalf("headers = %v", header)
	}
	body := <-bodies
	// No speed or language: the endpoint answers 422 to speed and the voice
	// fixes the language.
	want := map[string]any{"model": DefaultTTSModel, "voice_id": "fr_marie_neutral", "input": "Bonjour et bienvenue.", "response_format": "pcm", "stream": true}
	if len(body) != len(want) {
		t.Fatalf("body = %#v, want %#v", body, want)
	}
	for key, value := range want {
		if body[key] != value {
			t.Fatalf("body[%s] = %#v, want %#v", key, body[key], value)
		}
	}
	audio, events := drainAudio(t, stream)
	if got := eventTypes(events); got != "audio.started,audio.frame,audio.frame,audio.done" {
		t.Fatalf("events = %s", got)
	}
	var pcm []int16
	for index := 0; index < len(audio); index += 2 {
		pcm = append(pcm, int16(binary.LittleEndian.Uint16(audio[index:])))
	}
	// Out-of-range floats clamp to full scale.
	if fmt.Sprint(pcm) != fmt.Sprint([]int16{0, 16384, -16384, 32767, -32767, 32767, -32767}) {
		t.Fatalf("pcm = %v", pcm)
	}
	if len(events[1].Audio) != 4 {
		t.Fatalf("first frame = %d bytes, want the two whole samples of the first delta", len(events[1].Audio))
	}
	for _, event := range events {
		if event.Billing != nil {
			t.Fatalf("%s carries billing; the relay meters characters itself", event.Type)
		}
	}
	if data := dataOf(t, events[len(events)-1]); data["sample_rate_hz"] != float64(24_000) {
		t.Fatalf("audio.done data = %v", data)
	}
}

func TestVoicePrefersCallerThenPlanThenDefault(t *testing.T) {
	t.Parallel()
	cases := []struct{ requested, planned, want string }{
		{requested: " voice_3150 ", planned: "gb_oliver_neutral", want: "voice_3150"},
		{planned: "01a14675-dc30-778a-8d01-b5dead5a522a", want: "01a14675-dc30-778a-8d01-b5dead5a522a"},
		{want: "en_paul_neutral"},
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
	if _, err := adapter.Open(context.Background(), ttsRequest(testTTSEndpoint, 24_000)); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*runtimepkg.AdapterRequest){
		"16 kHz": func(r *runtimepkg.AdapterRequest) { r.Media.SampleRateHz = 16_000 },
		"stereo": func(r *runtimepkg.AdapterRequest) { r.Media.Channels = 2 },
		"model":  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "voxtral-mini-tts-2603" },
		"wrong path": func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Endpoint = "https://api.mistral.ai/v1/audio/transcriptions"
		},
		"foreign host": func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = "https://example.com/v1/audio/speech" },
		"stt kind":     func(r *runtimepkg.AdapterRequest) { r.Kind = protocol.SessionKindSTT },
		"relay key off relay": func(r *runtimepkg.AdapterRequest) {
			r.Plan.Execution.ProviderRoute = protocol.RouteProviderDirect
		},
	}
	for name, mutate := range cases {
		request := ttsRequest(testTTSEndpoint, 24_000)
		mutate(&request)
		if _, err := adapter.Open(context.Background(), request); err == nil {
			t.Fatalf("%s: Open accepted", name)
		}
	}
}

func TestSynthesisRejectionsKeepTheirClassification(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		status    int
		body      string
		code      string
		retryable bool
	}{
		// The first three are the live answers of 2026-10-21.
		{name: "unknown voice", status: http.StatusNotFound, body: `{"object":"error","message":"Voice 'nope_x' not found.","type":"invalid_voice","param":null,"code":"1902","raw_status_code":404}`, code: "invalid_request"},
		{name: "refused key", status: http.StatusUnauthorized, body: `{"detail":"Invalid API Key"}` + "\n", code: "authentication_failed"},
		{name: "missing voice", status: http.StatusBadRequest, body: `{"object":"error","message":"Either ref_audio or voice must be provided.","type":"invalid_request_error","param":null,"code":null}`, code: "invalid_request"},
		{name: "rate limit", status: http.StatusTooManyRequests, body: `{"object":"error","message":"Requests rate limit exceeded","type":"rate_limited"}`, code: "provider_rate_limited", retryable: true},
		{name: "outage", status: http.StatusBadGateway, body: `<html>bad gateway</html>`, code: "provider_unavailable", retryable: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := ttsServer(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			})
			stream := openTTS(t, server, ttsRequest("", 24_000), TTSConfig{})
			err := commit(t, stream, "hello")
			var providerErr *runtimepkg.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Code != tc.code || providerErr.Retryable != tc.retryable || providerErr.ProviderStatus != tc.status {
				t.Fatalf("CommitText error = %#v, want %s retryable=%v", err, tc.code, tc.retryable)
			}
			if strings.Contains(providerErr.Message, "not found") || strings.Contains(providerErr.Message, "Invalid") {
				t.Fatalf("message quotes the vendor text: %q", providerErr.Message)
			}
			if tc.name == "unknown voice" && (!strings.Contains(providerErr.Message, "fr_marie_neutral") || providerErr.Hint == "") {
				t.Fatalf("unknown voice error does not name the voice: %#v", providerErr)
			}
			if err := stream.AppendText(context.Background(), "next"); err != nil {
				t.Fatalf("session unusable after a rejection: %v", err)
			}
		})
	}
}

func TestUnexpectedContentTypeIsARejectionNotAudio(t *testing.T) {
	t.Parallel()
	server := ttsServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"audio_data":"AAAA"}`)
	})
	stream := openTTS(t, server, ttsRequest("", 24_000), TTSConfig{})
	var providerErr *runtimepkg.ProviderError
	if err := commit(t, stream, "hello"); !errors.As(err, &providerErr) || providerErr.Code != "provider_unavailable" {
		t.Fatalf("CommitText error = %v", err)
	}
}

func TestStreamFailuresAreNotShortUtterances(t *testing.T) {
	t.Parallel()
	cases := map[string][]string{
		"done without audio":    {liveDoneEvent},
		"body ends before done": {deltaEvent(floats(0.1, 0.2))},
		"ends mid-sample":       {deltaEvent(floats(0.1, 0.2)[:6]), liveDoneEvent},
		"malformed event":       {"data: {not json\n\n"},
		"undecodable audio":     {"data: {\"type\":\"speech.audio.delta\",\"audio_data\":\"***\"}\n\n"},
		"error event":           {deltaEvent(floats(0.1)), "data: {\"type\":\"error\",\"message\":\"generation failed\"}\n\n"},
	}
	for name, events := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			stream := openTTS(t, sseServer(t, events...), ttsRequest("", 24_000), TTSConfig{})
			if err := commit(t, stream, "hello"); err != nil {
				t.Fatal(err)
			}
			if failure := failure(t, stream); failure.Code != "provider_unavailable" || !failure.Retryable {
				t.Fatalf("failure = %#v", failure)
			}
		})
	}
}

// A final event without its trailing blank line still ends the utterance.
func TestDoneWithoutTrailingBlankLineCompletes(t *testing.T) {
	t.Parallel()
	stream := openTTS(t, sseServer(t, deltaEvent(floats(0.25)), strings.TrimSuffix(liveDoneEvent, "\n\n")), ttsRequest("", 24_000), TTSConfig{})
	if err := commit(t, stream, "hello"); err != nil {
		t.Fatal(err)
	}
	if audio, _ := drainAudio(t, stream); len(audio) != 2 {
		t.Fatalf("audio = %v", audio)
	}
}

func TestResponseLimitIsEnforced(t *testing.T) {
	t.Parallel()
	stream := openTTS(t, sseServer(t, deltaEvent(floats(make([]float32, 4096)...)), liveDoneEvent), ttsRequest("", 24_000), TTSConfig{MaxResponseBytes: 1024})
	if err := commit(t, stream, "hello"); err != nil {
		t.Fatal(err)
	}
	if failure := failure(t, stream); !strings.Contains(failure.Message, "limit") {
		t.Fatalf("failure = %#v", failure)
	}
}

func TestNextUtteranceMayStartOnAudioDone(t *testing.T) {
	t.Parallel()
	stream := openTTS(t, sseServer(t, deltaEvent(floats(0.1, 0.2)), liveDoneEvent), ttsRequest("", 24_000), TTSConfig{EventBuffer: 1})
	if err := commit(t, stream, "hello"); err != nil {
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
	server := ttsServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for range 5 {
			_, _ = io.WriteString(w, deltaEvent(floats(0.5)))
			w.(http.Flusher).Flush()
			time.Sleep(10 * time.Millisecond)
		}
		_, _ = io.WriteString(w, liveDoneEvent)
	})
	stream := openTTS(t, server, ttsRequest("", 24_000), TTSConfig{EventBuffer: 1})
	if err := commit(t, stream, "hello"); err != nil {
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
		_, _ = w.Write([]byte(`{"detail":`))
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	defer close(release)
	stream := openTTS(t, server, ttsRequest("", 24_000), TTSConfig{HeaderTimeout: 50 * time.Millisecond})
	result := make(chan error, 1)
	_ = stream.AppendText(context.Background(), "hello")
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
}

func TestIdleCancelKeepsTheStreamOpen(t *testing.T) {
	t.Parallel()
	stream := openTTS(t, sseServer(t, deltaEvent(floats(0.1, 0.2)), liveDoneEvent), ttsRequest("", 24_000), TTSConfig{})
	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatalf("idle cancellation closed a live session: %v", err)
	}
	if err := commit(t, stream, "next turn"); err != nil {
		t.Fatalf("next turn commit: %v", err)
	}
	if audio, _ := drainAudio(t, stream); len(audio) != 4 {
		t.Fatalf("next turn audio = %v", audio)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := stream.Cancel(context.Background()); !errors.Is(err, runtimepkg.ErrSessionClosed) {
		t.Fatalf("closed session cancel = %v", err)
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
	started := time.Now()
	err := commit(t, stream, "hello")
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
	target := ttsServer(t, func(http.ResponseWriter, *http.Request) { followed <- struct{}{} })
	server := ttsServer(t, func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL+speechPath, http.StatusTemporaryRedirect)
	})
	stream := openTTS(t, server, ttsRequest("", 24_000), TTSConfig{})
	err := commit(t, stream, "hello")
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
