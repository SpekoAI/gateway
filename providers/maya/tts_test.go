package maya

import (
	"context"
	"encoding/base64"
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

func TestRealtimeTurnStreamsAudioAndClosesWithContextID(t *testing.T) {
	frames := make(chan map[string]any, 3)
	server := mayaServer(t, func(conn *websocket.Conn, request *http.Request) {
		if got := request.Header.Get("Authorization"); got != "Bearer maya-key" {
			t.Errorf("authorization = %q", got)
		}
		start := readFrame(t, conn)
		frames <- start
		writeFrame(t, conn, map[string]any{"type": "metadata", "sample_rate": 24000, "channels": 1, "encoding": "pcm_s16le", "session_id": "maya-session-1"})
		first := readFrame(t, conn)
		second := readFrame(t, conn)
		closer := readFrame(t, conn)
		frames <- first
		frames <- second
		contextID, _ := first["context_id"].(string)
		if contextID == "" || second["context_id"] != contextID || closer["context_id"] != contextID {
			t.Errorf("context ids = %#v %#v %#v", first["context_id"], second["context_id"], closer["context_id"])
		}
		if closer["continue"] != false {
			t.Errorf("closer = %#v", closer)
		}
		writeFrame(t, conn, map[string]any{"type": "audio", "context_id": contextID, "audio": base64.StdEncoding.EncodeToString([]byte{0, 1, 2, 3})})
		writeFrame(t, conn, map[string]any{"type": "end", "context_id": contextID})
		waitForClose(conn)
	})
	defer server.Close()

	adapter := testAdapter(t, server)
	stream, err := adapter.Open(context.Background(), mayaRequest(wsURL(server), DefaultModel))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := stream.AppendText(context.Background(), "नमस्ते।"); err != nil {
		t.Fatal(err)
	}
	if err := stream.AppendText(context.Background(), "धन्यवाद।"); err != nil {
		t.Fatal(err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}

	start := <-frames
	if start["type"] != "start" || start["v2"] != true || start["voice"] != "Ananya" || start["language"] != "hi" || start["model"] != DefaultModel {
		t.Fatalf("start = %#v", start)
	}
	first := <-frames
	second := <-frames
	if first["text"] != "नमस्ते।" || first["continue"] != true || second["text"] != "धन्यवाद।" || second["continue"] != true {
		t.Fatalf("text frames = %#v %#v", first, second)
	}

	want := []protocol.EventType{protocol.EventUsageObserved, protocol.EventAudioStarted, protocol.EventAudioFrame, protocol.EventAudioDone}
	for _, eventType := range want {
		event := eventWithin(t, stream.Events())
		if event.Type != eventType {
			t.Fatalf("event = %q, want %q", event.Type, eventType)
		}
		if eventType == protocol.EventAudioFrame && string(event.Audio) != string([]byte{0, 1, 2, 3}) {
			t.Fatalf("audio = %v", event.Audio)
		}
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestCancelEndsTurnWithoutWaitingForAudio(t *testing.T) {
	server := mayaServer(t, func(conn *websocket.Conn, _ *http.Request) {
		_ = readFrame(t, conn)
		writeFrame(t, conn, map[string]any{"type": "metadata", "sample_rate": 24000, "channels": 1, "encoding": "pcm_s16le", "session_id": "maya-session-cancel"})
		text := readFrame(t, conn)
		cancel := readFrame(t, conn)
		if cancel["type"] != "cancel" || cancel["context_id"] != text["context_id"] {
			t.Errorf("cancel = %#v, text = %#v", cancel, text)
		}
		writeFrame(t, conn, map[string]any{"type": "cancelled", "context_id": text["context_id"]})
		waitForClose(conn)
	})
	defer server.Close()

	stream, err := testAdapter(t, server).Open(context.Background(), mayaRequest(wsURL(server), DefaultModel))
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.AppendText(context.Background(), "stop me"); err != nil {
		t.Fatal(err)
	}
	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatal(err)
	}
	if event := eventWithin(t, stream.Events()); event.Type != protocol.EventUsageObserved {
		t.Fatalf("first event = %q", event.Type)
	}
	event := eventWithin(t, stream.Events())
	if event.Type != protocol.EventAudioDone || !strings.Contains(string(event.Data), `"cancelled":true`) {
		t.Fatalf("cancel event = %+v", event)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestManagedProviderDirectIsRejectedButRelayAccessIsAccepted(t *testing.T) {
	adapter, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	request := mayaRequest("wss://tts.mayaresearch.ai/v1/tts/stream", DefaultModel)
	request.Plan.Execution.CredentialSource = protocol.CredentialsManaged
	if _, err := adapter.Open(context.Background(), request); err == nil || !strings.Contains(err.Error(), "short-lived session credentials") {
		t.Fatalf("managed provider-direct error = %v", err)
	}

	server := mayaServer(t, func(conn *websocket.Conn, _ *http.Request) {
		_ = readFrame(t, conn)
		writeFrame(t, conn, map[string]any{"type": "metadata", "sample_rate": 24000, "channels": 1, "encoding": "pcm_s16le", "session_id": "maya-relay"})
		waitForClose(conn)
	})
	defer server.Close()
	request = mayaRequest(wsURL(server), DefaultModel)
	request.Plan.Execution.ProviderRoute = protocol.RouteSpekoRelay
	request.Plan.Execution.CredentialSource = protocol.CredentialsManaged
	request.Plan.Route.Credential.Kind = protocol.CredentialRelayAccess
	stream, err := testAdapter(t, server).Open(context.Background(), request)
	if err != nil {
		t.Fatalf("relay open: %v", err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestStreamingModelsAndFixedOutputAreValidatedBeforeDial(t *testing.T) {
	adapter, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, model := range []string{"Maya 2 Native", "Maya 2 Native Emotional"} {
		request := mayaRequest("wss://tts.mayaresearch.ai/v1/tts/stream", model)
		request.Plan.Execution.CredentialSource = protocol.CredentialsManaged
		if _, err := adapter.Open(context.Background(), request); err == nil || !strings.Contains(err.Error(), "short-lived") {
			t.Errorf("model %q did not pass model validation first: %v", model, err)
		}
	}
	request := mayaRequest("wss://tts.mayaresearch.ai/v1/tts/stream", "Maya 2 Global")
	if _, err := adapter.Open(context.Background(), request); err == nil || !strings.Contains(err.Error(), "does not support model") {
		t.Fatalf("HTTP-only model error = %v", err)
	}
	request = mayaRequest("wss://tts.mayaresearch.ai/v1/tts/stream", DefaultModel)
	request.Media.SampleRateHz = 16_000
	if _, err := adapter.Open(context.Background(), request); err == nil || !strings.Contains(err.Error(), "24000") {
		t.Fatalf("sample rate error = %v", err)
	}
}

func TestCloseBoundsUndrainedEventBackpressure(t *testing.T) {
	server := mayaServer(t, func(conn *websocket.Conn, _ *http.Request) {
		_ = readFrame(t, conn)
		writeFrame(t, conn, map[string]any{"type": "metadata", "sample_rate": 24000, "channels": 1, "encoding": "pcm_s16le", "session_id": "maya-stall"})
		text := readFrame(t, conn)
		_ = readFrame(t, conn)
		writeFrame(t, conn, map[string]any{"type": "audio", "context_id": text["context_id"], "audio": base64.StdEncoding.EncodeToString([]byte{1, 2})})
		writeFrame(t, conn, map[string]any{"type": "end", "context_id": text["context_id"]})
	})
	defer server.Close()
	parsed, _ := url.Parse(server.URL)
	adapter, err := New(Config{EventBuffer: 1, GracefulCloseIdleTimeout: 25 * time.Millisecond, AllowedEndpointHosts: []string{parsed.Hostname()}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := adapter.Open(context.Background(), mayaRequest(wsURL(server), DefaultModel))
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.AppendText(context.Background(), "hello"); err != nil {
		t.Fatal(err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- stream.Close(context.Background()) }()
	select {
	case err := <-closed:
		var providerErr *runtimepkg.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != "provider_unavailable" || !providerErr.Retryable {
			t.Fatalf("close error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("close blocked on undrained events")
	}
}

func mayaRequest(endpoint, model string) runtimepkg.AdapterRequest {
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindTTS,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK},
			Route:     protocol.PlanRoute{Provider: "maya", Model: model, Voice: DefaultVoice, Adapter: AdapterID, Transport: protocol.TransportWebSocket, Endpoint: endpoint, Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "maya-key"}},
		},
		Options: protocol.RequestOptions{Language: "hi"},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: outputSampleHz, Channels: 1},
	}
}

func testAdapter(t *testing.T, server *httptest.Server) *Adapter {
	t.Helper()
	parsed, _ := url.Parse(server.URL)
	adapter, err := New(Config{AllowedEndpointHosts: []string{parsed.Hostname()}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func mayaServer(t *testing.T, handler func(*websocket.Conn, *http.Request)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := websocket.Accept(writer, request, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		defer conn.CloseNow()
		handler(conn, request)
	}))
}

func wsURL(server *httptest.Server) string {
	return "ws" + strings.TrimPrefix(server.URL, "http") + streamPath
}

func readFrame(t *testing.T, conn *websocket.Conn) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	messageType, payload, err := conn.Read(ctx)
	if err != nil {
		t.Errorf("read websocket: %v", err)
		return nil
	}
	if messageType != websocket.MessageText {
		t.Errorf("message type = %v", messageType)
	}
	var value map[string]any
	if err := json.Unmarshal(payload, &value); err != nil {
		t.Errorf("decode websocket frame: %v", err)
	}
	return value
}

func writeFrame(t *testing.T, conn *websocket.Conn, value any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	payload, _ := json.Marshal(value)
	if err := conn.Write(ctx, websocket.MessageText, payload); err != nil {
		t.Errorf("write websocket: %v", err)
	}
}

func waitForClose(conn *websocket.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _, _ = conn.Read(ctx)
}

func eventWithin(t *testing.T, events <-chan runtimepkg.ProviderEvent) runtimepkg.ProviderEvent {
	t.Helper()
	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("events closed")
		}
		if event.Err != nil {
			t.Fatalf("provider event error: %v", event.Err)
		}
		return event
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
		return runtimepkg.ProviderEvent{}
	}
}
