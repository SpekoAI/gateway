package modulate

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

func TestEnglishFastWireContractAndEvents(t *testing.T) {
	t.Parallel()
	requestSeen := make(chan *http.Request, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := websocket.Accept(writer, request, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		defer func() { _ = conn.CloseNow() }()
		requestSeen <- request.Clone(request.Context())
		ctx := context.Background()
		messageType, audio, err := conn.Read(ctx)
		if err != nil || messageType != websocket.MessageBinary || string(audio) != "pcm" {
			t.Errorf("audio frame = type %d payload %q err %v", messageType, audio, err)
			return
		}
		writeJSON(t, conn, map[string]any{"type": "partial_utterance", "partial_utterance": map[string]any{"text": "Hello", "is_final": false}})
		writeJSON(t, conn, map[string]any{"type": "utterance", "utterance": map[string]any{"text": "Hello there.", "is_final": true, "start_ms": 100, "duration_ms": 900}})
		messageType, end, err := conn.Read(ctx)
		if err != nil || messageType != websocket.MessageText || len(end) != 0 {
			t.Errorf("end frame = type %d payload %q err %v", messageType, end, err)
			return
		}
		writeJSON(t, conn, map[string]any{"type": "done", "duration_ms": 1000})
	}))
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), testRequest(server.URL, EnglishFastModel, protocol.RouteProviderDirect, protocol.CredentialsBYOK))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := stream.WriteAudio(context.Background(), []byte("pcm")); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	if err := stream.CommitAudio(context.Background()); err != nil {
		t.Fatalf("commit audio: %v", err)
	}
	events := receiveEvents(t, stream.Events(), 4)
	want := []protocol.EventType{protocol.EventSpeechStarted, protocol.EventTranscriptDelta, protocol.EventTranscriptFinal, protocol.EventSpeechEnded}
	for index := range want {
		if events[index].Type != want[index] {
			t.Fatalf("event %d = %s, want %s", index, events[index].Type, want[index])
		}
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := stream.Close(closeCtx); err != nil {
		t.Fatalf("close: %v", err)
	}
	usage := receiveEvents(t, stream.Events(), 1)[0]
	if usage.Type != protocol.EventUsageObserved || string(usage.Data) != `{"duration_ms":1000}` {
		t.Fatalf("usage = %s %s", usage.Type, usage.Data)
	}

	handshake := <-requestSeen
	query := handshake.URL.Query()
	for key, wantValue := range map[string]string{
		"api_key": "modulate-key", "audio_format": "s16le", "sample_rate": "16000", "num_channels": "1", "endpointing": "true",
	} {
		if got := query.Get(key); got != wantValue {
			t.Errorf("query %s = %q, want %q", key, got, wantValue)
		}
	}
	if handshake.Header.Get("Authorization") != "" {
		t.Fatal("Modulate credential must use the documented query channel")
	}
}

func TestMultilingualQueryAndRichTranscript(t *testing.T) {
	t.Parallel()
	requestSeen := make(chan *http.Request, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := websocket.Accept(writer, request, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		defer func() { _ = conn.CloseNow() }()
		requestSeen <- request.Clone(request.Context())
		writeJSON(t, conn, map[string]any{"type": "utterance", "utterance": map[string]any{
			"utterance_uuid": "utt-1", "text": "Hola.", "start_ms": 20, "duration_ms": 480,
			"speaker": 2, "language": "es", "emotion": "Happy", "accent": nil, "deepfake_score": 0.01,
		}})
		messageType, _, err := conn.Read(context.Background())
		if err != nil || messageType != websocket.MessageText {
			t.Errorf("end frame: type %d err %v", messageType, err)
			return
		}
		writeJSON(t, conn, map[string]any{"type": "done", "duration_ms": 500})
	}))
	defer server.Close()
	adapter, _ := New(testConfig(server.URL))
	request := testRequest(server.URL, MultilingualModel, protocol.RouteSpekoRelay, protocol.CredentialsManaged)
	request.Options.Language = "es-MX"
	request.Plan.Route.Credential.Kind = protocol.CredentialRelayAccess
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open relay: %v", err)
	}
	events := receiveEvents(t, stream.Events(), 3)
	if events[1].Type != protocol.EventTranscriptFinal || !strings.Contains(string(events[1].Data), `"provider_request_id":"utt-1"`) || !strings.Contains(string(events[1].Data), `"speaker":2`) {
		t.Fatalf("final = %s %s", events[1].Type, events[1].Data)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := stream.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	query := (<-requestSeen).URL.Query()
	if query.Get("language") != "es-MX" || query.Get("partial_results") != "true" || query.Get("speaker_diarization") != "true" || query.Has("endpointing") {
		t.Fatalf("multilingual query = %v", query)
	}
}

func TestRejectsManagedProviderDirectAndWrongLanguage(t *testing.T) {
	t.Parallel()
	adapter, err := New(Config{})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	managed := testRequest("wss://platform.modulate.ai"+englishFastPath, EnglishFastModel, protocol.RouteProviderDirect, protocol.CredentialsManaged)
	if _, err := adapter.Open(context.Background(), managed); err == nil || !strings.Contains(err.Error(), "does not support delegated") {
		t.Fatalf("managed direct open = %v", err)
	}
	byok := testRequest("wss://platform.modulate.ai"+englishFastPath, EnglishFastModel, protocol.RouteProviderDirect, protocol.CredentialsBYOK)
	byok.Options.Language = "fr"
	if _, err := adapter.Open(context.Background(), byok); err == nil || !strings.Contains(err.Error(), "English Fast") {
		t.Fatalf("English Fast French open = %v", err)
	}
}

func TestProviderErrorClassification(t *testing.T) {
	t.Parallel()
	tests := []struct {
		message string
		code    string
		retry   bool
	}{
		{"Invalid API key.", "authentication_failed", false},
		{"Insufficient credits.", "provider_quota_exceeded", false},
		{"Concurrent request limit reached.", "provider_rate_limited", true},
		{"The service is temporarily unavailable.", "provider_unavailable", true},
		{"Audio data does not match the declared audio_format.", "invalid_request", false},
	}
	for _, testCase := range tests {
		err := providerMessageError(testCase.message)
		if err.Code != testCase.code || err.Retryable != testCase.retry {
			t.Errorf("%q => %s retry=%t, want %s retry=%t", testCase.message, err.Code, err.Retryable, testCase.code, testCase.retry)
		}
	}
}

func TestCloseBoundsNonResponsiveProvider(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.CloseNow() }()
		_, _, _ = conn.Read(context.Background())
		_, _, _ = conn.Read(context.Background())
	}))
	defer server.Close()
	config := testConfig(server.URL)
	config.ShutdownTimeout = 50 * time.Millisecond
	adapter, err := New(config)
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	provider, err := adapter.Open(context.Background(), testRequest(server.URL, EnglishFastModel, protocol.RouteProviderDirect, protocol.CredentialsBYOK))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := provider.Close(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close = %v, want bounded deadline", err)
	}
}

func testConfig(serverURL string) Config {
	parsed, _ := url.Parse(serverURL)
	return Config{AllowedEndpointHosts: []string{parsed.Hostname()}, AllowInsecureEndpoint: true}
}

func testRequest(serverURL, model string, route protocol.ProviderRoute, source protocol.CredentialSource) runtimepkg.AdapterRequest {
	parsed, _ := url.Parse(serverURL)
	if parsed.Scheme == "http" {
		parsed.Scheme = "ws"
	} else if parsed.Scheme == "https" {
		parsed.Scheme = "wss"
	}
	path := englishFastPath
	if model == MultilingualModel {
		path = multilingualPath
	}
	parsed.Path = path
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindSTT,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{ProviderRoute: route, CredentialSource: source},
			Route:     protocol.PlanRoute{Provider: "modulate", Model: model, Adapter: AdapterID, Transport: protocol.TransportWebSocket, Endpoint: parsed.String(), Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "modulate-key"}},
		},
		Media: &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
	}
}

func writeJSON(t *testing.T, conn *websocket.Conn, value any) {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := conn.Write(context.Background(), websocket.MessageText, payload); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func receiveEvents(t *testing.T, events <-chan runtimepkg.ProviderEvent, count int) []runtimepkg.ProviderEvent {
	t.Helper()
	result := make([]runtimepkg.ProviderEvent, 0, count)
	for len(result) < count {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatalf("events closed after %d of %d", len(result), count)
			}
			if event.Err != nil {
				t.Fatalf("provider error: %v", event.Err)
			}
			result = append(result, event)
		case <-time.After(time.Second):
			t.Fatalf("timed out after %d of %d events", len(result), count)
		}
	}
	return result
}
