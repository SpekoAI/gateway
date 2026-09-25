package nari

import (
	"context"
	"encoding/json"
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

func ttsRequest(endpoint, model string) runtimepkg.AdapterRequest {
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindTTS,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged},
			Route: protocol.PlanRoute{Provider: ProviderName, Model: model, Adapter: TTSAdapterID, Transport: protocol.TransportHTTP, Endpoint: endpoint,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialRelayAccess, Value: "nari-key", ExpiresAt: time.Now().Add(time.Minute)}},
		},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1},
		Options: protocol.RequestOptions{Language: "es"},
	}
}

func openTTS(t *testing.T, server *httptest.Server, model string) runtimepkg.ProviderStream {
	t.Helper()
	parsed, _ := url.Parse(server.URL)
	adapter, err := NewTTS(TTSConfig{AllowedEndpointHosts: []string{parsed.Hostname()}, AllowInsecureEndpoint: true, AudioChunkBytes: 4})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := adapter.Open(context.Background(), ttsRequest(server.URL+speechPath, model))
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

func TestCommitTextSendsTheDocumentedRequestAndStreamsPCM(t *testing.T) {
	t.Parallel()
	bodies := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != speechPath || request.Header.Get("Authorization") != "Bearer nari-key" {
			t.Errorf("request = %s %s %v", request.Method, request.URL.Path, request.Header)
		}
		raw, _ := io.ReadAll(request.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		bodies <- body
		writer.Header().Set("Content-Type", "audio/pcm")
		writer.Header().Set(requestIDHeader, "req-tts")
		_, _ = writer.Write([]byte{1, 2, 3, 4, 5, 6})
	}))
	t.Cleanup(server.Close)
	stream := openTTS(t, server, "qwen3-tts")
	if err := stream.AppendText(context.Background(), "Hi, this is "); err != nil {
		t.Fatal(err)
	}
	if err := stream.AppendText(context.Background(), "Claire."); err != nil {
		t.Fatal(err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("CommitText: %v", err)
	}
	body := <-bodies
	want := map[string]any{"model": "qwen3-tts", "input": "Hi, this is Claire.", "voice": DefaultVoice, "stream": true, "response_format": "pcm"}
	if len(body) != len(want) {
		t.Fatalf("body = %#v; language must never be sent", body)
	}
	for key, value := range want {
		if body[key] != value {
			t.Fatalf("body[%s] = %#v, want %#v", key, body[key], value)
		}
	}
	var audio []byte
	for _, wantType := range []protocol.EventType{protocol.EventUsageObserved, protocol.EventAudioStarted, protocol.EventAudioFrame, protocol.EventAudioFrame, protocol.EventAudioDone} {
		event := nextEvent(t, stream)
		if event.Err != nil || event.Type != wantType {
			t.Fatalf("event = %s %v, want %s", event.Type, event.Err, wantType)
		}
		if wantType == protocol.EventUsageObserved && !strings.Contains(string(event.Data), `"req-tts"`) {
			t.Fatalf("usage data = %s", event.Data)
		}
		audio = append(audio, event.Audio...)
	}
	if string(audio) != string([]byte{1, 2, 3, 4, 5, 6}) {
		t.Fatalf("audio = %v", audio)
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
		{status: http.StatusUnauthorized, code: "INVALID_API_KEY", want: "authentication_failed"},
		{status: http.StatusPaymentRequired, code: "INSUFFICIENT_CREDITS", want: "provider_quota_exceeded"},
		{status: http.StatusBadRequest, code: "INVALID_VOICE", want: "invalid_request"},
		{status: http.StatusTooManyRequests, code: "CONCURRENCY_LIMIT_EXCEEDED", want: "provider_rate_limited", retryable: true},
		{status: http.StatusTooManyRequests, code: "UPSTREAM_RATE_LIMITED", want: "provider_rate_limited", retryable: true},
		{status: http.StatusServiceUnavailable, code: "SERVER_AT_CAPACITY", want: "provider_unavailable", retryable: true},
		{status: http.StatusBadGateway, code: "", want: "provider_unavailable", retryable: true},
	}
	for _, tc := range cases {
		t.Run(tc.code+"/"+http.StatusText(tc.status), func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", "application/json")
				writer.WriteHeader(tc.status)
				_, _ = writer.Write([]byte(`{"error":{"code":"` + tc.code + `","message":"nope","requestId":"req-err"}}`))
			}))
			t.Cleanup(server.Close)
			stream := openTTS(t, server, DefaultTTSModel)
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
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "audio/pcm")
	}))
	t.Cleanup(server.Close)
	stream := openTTS(t, server, DefaultTTSModel)
	_ = stream.AppendText(context.Background(), "hello")
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}
	event := nextEvent(t, stream)
	var providerErr *runtimepkg.ProviderError
	if !errors.As(event.Err, &providerErr) || providerErr.Code != "provider_unavailable" || !providerErr.Retryable {
		t.Fatalf("event = %+v, want a retryable failure", event)
	}
}

// A failure after the status line cuts the body with no JSON error. The
// audio already delivered must not be followed by audio.done.
func TestTruncatedStreamIsAFailureNotAShortUtterance(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "audio/pcm")
		writer.Header().Set("Content-Length", "1000")
		_, _ = writer.Write([]byte{1, 2, 3, 4})
		writer.(http.Flusher).Flush()
		conn, _, err := writer.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}))
	t.Cleanup(server.Close)
	stream := openTTS(t, server, DefaultTTSModel)
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

func TestInputLimitCountsTrimmedCodePoints(t *testing.T) {
	t.Parallel()
	adapter, err := NewTTS(TTSConfig{})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := adapter.Open(context.Background(), ttsRequest("https://api.narilabs.com/v1/audio/speech", DefaultTTSModel))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()
	if err := stream.AppendText(context.Background(), "  "+strings.Repeat("é", maxInputCodePoints)+"  "); err != nil {
		t.Fatalf("2048 code points inside whitespace must fit: %v", err)
	}
	err = stream.AppendText(context.Background(), "x")
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "input_too_large" {
		t.Fatalf("AppendText error = %v, want input_too_large", err)
	}
}

func TestOpenAcceptsBothServingClassesOnly(t *testing.T) {
	t.Parallel()
	adapter, err := NewTTS(TTSConfig{})
	if err != nil {
		t.Fatal(err)
	}
	for _, model := range []string{"qwen3-tts", "qwen3-tts-fast"} {
		stream, err := adapter.Open(context.Background(), ttsRequest("https://api.narilabs.com/v1/audio/speech", model))
		if err != nil {
			t.Errorf("%s: %v", model, err)
			continue
		}
		_ = stream.Close(context.Background())
	}
	for name, request := range map[string]runtimepkg.AdapterRequest{
		"model": ttsRequest("https://api.narilabs.com/v1/audio/speech", "qwen3-tts-flash"),
		"path":  ttsRequest("https://api.narilabs.com/v1/audio/stream", DefaultTTSModel),
		"host":  ttsRequest("https://api.example.com/v1/audio/speech", DefaultTTSModel),
	} {
		if _, err := adapter.Open(context.Background(), request); err == nil {
			t.Errorf("%s: Open succeeded, want refusal", name)
		}
	}
}
