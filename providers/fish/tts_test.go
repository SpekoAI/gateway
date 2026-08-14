package fish

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
	"github.com/vmihailenco/msgpack/v5"
)

func TestAdapterUsesDocumentedMessagePackWireContract(t *testing.T) {
	t.Parallel()
	requests := make(chan *http.Request, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := websocket.Accept(writer, request, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		defer func() { _ = conn.CloseNow() }()
		requests <- request.Clone(request.Context())
		ctx := context.Background()
		start := readClientEvent(t, ctx, conn)
		if start.Event != "start" || start.Request == nil || start.Request.Text != "" || start.Request.Format != "pcm" || start.Request.SampleRate != 24_000 || start.Request.ReferenceID != "voice-123" {
			t.Errorf("start = %+v", start)
			return
		}
		text := readClientEvent(t, ctx, conn)
		if text.Event != "text" || text.Text != "Hello, fish." {
			t.Errorf("text = %+v", text)
			return
		}
		stop := readClientEvent(t, ctx, conn)
		if stop.Event != "stop" {
			t.Errorf("stop = %+v", stop)
			return
		}
		writeServerEvent(t, ctx, conn, serverEvent{Event: "audio", Audio: []byte{1, 2, 3}})
		writeServerEvent(t, ctx, conn, serverEvent{Event: "finish", Reason: "stop"})
	}))
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), adapterRequest(server.URL, protocol.CredentialsBYOK))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := stream.AppendText(context.Background(), "Hello, fish."); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	events := receiveEvents(t, stream.Events(), 3)
	if events[0].Type != protocol.EventAudioStarted || events[1].Type != protocol.EventAudioFrame || events[2].Type != protocol.EventAudioDone {
		t.Fatalf("events = %s, %s, %s", events[0].Type, events[1].Type, events[2].Type)
	}
	if string(events[1].Audio) != string([]byte{1, 2, 3}) || events[2].Extensions[extensionID] == nil {
		t.Fatalf("audio event = %+v", events[1])
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	request := <-requests
	if got := request.Header.Get("Authorization"); got != "Bearer customer-fish-key" {
		t.Fatalf("authorization = %q", got)
	}
	if got := request.Header.Get("model"); got != "s2.1-pro" {
		t.Fatalf("model header = %q", got)
	}
}

func TestAdapterReconnectsForTheNextUtterance(t *testing.T) {
	t.Parallel()
	dials := make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := websocket.Accept(writer, request, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		defer func() { _ = conn.CloseNow() }()
		dials <- struct{}{}
		ctx := context.Background()
		_ = readClientEvent(t, ctx, conn)
		_ = readClientEvent(t, ctx, conn)
		_ = readClientEvent(t, ctx, conn)
		writeServerEvent(t, ctx, conn, serverEvent{Event: "finish", Reason: "stop"})
	}))
	defer server.Close()
	adapter, _ := New(testConfig(server.URL))
	stream, err := adapter.Open(context.Background(), adapterRequest(server.URL, protocol.CredentialsBYOK))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = stream.Close(context.Background()) }()
	for _, text := range []string{"first", "second"} {
		if err := stream.AppendText(context.Background(), text); err != nil {
			t.Fatalf("append %q: %v", text, err)
		}
		if err := stream.CommitText(context.Background()); err != nil {
			t.Fatalf("commit %q: %v", text, err)
		}
		if event := receiveEvents(t, stream.Events(), 1)[0]; event.Type != protocol.EventAudioDone {
			t.Fatalf("done event = %s", event.Type)
		}
	}
	if len(dials) != 2 {
		t.Fatalf("dials = %d, want one Fish connection per utterance", len(dials))
	}
}

func TestGenerationCompletesOnlyAfterAudioDoneIsQueued(t *testing.T) {
	t.Parallel()
	audioSent := make(chan struct{})
	sendFinish := make(chan struct{})
	clientClosed := make(chan struct{})
	secondText := make(chan struct{})
	var dials atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := websocket.Accept(writer, request, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		defer func() { _ = conn.CloseNow() }()
		ctx := context.Background()
		_ = readClientEvent(t, ctx, conn)
		_ = readClientEvent(t, ctx, conn)
		if dials.Add(1) > 1 {
			close(secondText)
			_, _, _ = conn.Read(ctx)
			return
		}
		_ = readClientEvent(t, ctx, conn)
		writeServerEvent(t, ctx, conn, serverEvent{Event: "audio", Audio: []byte{1, 2, 3}})
		close(audioSent)
		<-sendFinish
		writeServerEvent(t, ctx, conn, serverEvent{Event: "finish", Reason: "stop"})
		_, _, _ = conn.Read(ctx)
		close(clientClosed)
	}))
	defer server.Close()

	config := testConfig(server.URL)
	config.EventBuffer = 2
	adapter, err := New(config)
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	provider, err := adapter.Open(context.Background(), adapterRequest(server.URL, protocol.CredentialsBYOK))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	implementation := provider.(*stream)
	implementation.stateMu.Lock()
	generation := implementation.active
	implementation.stateMu.Unlock()
	if err := provider.AppendText(context.Background(), "ordered"); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := provider.CommitText(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	select {
	case <-audioSent:
	case <-time.After(time.Second):
		t.Fatal("server did not send audio")
	}
	deadline := time.Now().Add(time.Second)
	for len(implementation.events) != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := len(implementation.events); got != 2 {
		t.Fatalf("buffered events = %d, want audio.started and audio.frame", got)
	}
	close(sendFinish)
	select {
	case <-clientClosed:
	case <-time.After(time.Second):
		t.Fatal("adapter did not process finish")
	}
	select {
	case <-generation.done:
		t.Fatal("generation completed before audio.done could be queued")
	default:
	}
	nextAppend := make(chan error, 1)
	go func() { nextAppend <- provider.AppendText(context.Background(), "next") }()
	select {
	case err := <-nextAppend:
		t.Fatalf("queued append returned before audio.done: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	events := receiveEvents(t, provider.Events(), 3)
	for index, want := range []protocol.EventType{protocol.EventAudioStarted, protocol.EventAudioFrame, protocol.EventAudioDone} {
		if events[index].Type != want {
			t.Fatalf("event %d = %s, want %s", index, events[index].Type, want)
		}
	}
	select {
	case <-generation.done:
	case <-time.After(time.Second):
		t.Fatal("generation did not complete after audio.done was queued")
	}
	select {
	case err := <-nextAppend:
		if err != nil {
			t.Fatalf("queued append: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued append did not start the next generation")
	}
	select {
	case <-secondText:
	case <-time.After(time.Second):
		t.Fatal("provider did not receive queued text")
	}
	_ = provider.Cancel(context.Background())
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
		_, _, _ = conn.Read(context.Background())
	}))
	defer server.Close()
	config := testConfig(server.URL)
	config.ShutdownTimeout = 50 * time.Millisecond
	adapter, err := New(config)
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	provider, err := adapter.Open(context.Background(), adapterRequest(server.URL, protocol.CredentialsBYOK))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := provider.Close(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close = %v, want bounded deadline", err)
	}
}

func TestAdapterRejectsManagedProviderDirectCredentials(t *testing.T) {
	t.Parallel()
	adapter, err := New(Config{})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	request := adapterRequest("wss://api.fish.audio/v1/tts/live", protocol.CredentialsManaged)
	if _, err := adapter.Open(context.Background(), request); err == nil || !strings.Contains(err.Error(), "does not support delegated") {
		t.Fatalf("open managed provider-direct = %v", err)
	}
}

func testConfig(serverURL string) Config {
	parsed, _ := url.Parse(serverURL)
	return Config{AllowedEndpointHosts: []string{parsed.Hostname()}, AllowInsecureEndpoint: true}
}

func adapterRequest(serverURL string, credentials protocol.CredentialSource) runtimepkg.AdapterRequest {
	parsed, _ := url.Parse(serverURL)
	parsed.Scheme = "ws"
	parsed.Path = endpointPath
	now := time.Now().UTC()
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindTTS,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{ProviderRoute: protocol.RouteProviderDirect, CredentialSource: credentials},
			Route: protocol.PlanRoute{
				Provider: "fish", Model: "s2.1-pro", Adapter: TTSAdapterID,
				Transport: protocol.TransportWebSocket, Endpoint: parsed.String(),
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "customer-fish-key", ExpiresAt: now.Add(time.Hour)},
			},
		},
		Options: protocol.RequestOptions{Voice: "voice-123"},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1},
	}
}

func readClientEvent(t *testing.T, ctx context.Context, conn *websocket.Conn) clientEvent {
	t.Helper()
	messageType, payload, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if messageType != websocket.MessageBinary {
		t.Fatalf("message type = %v", messageType)
	}
	var event clientEvent
	if err := msgpack.Unmarshal(payload, &event); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return event
}

func writeServerEvent(t *testing.T, ctx context.Context, conn *websocket.Conn, event serverEvent) {
	t.Helper()
	payload, err := msgpack.Marshal(event)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := conn.Write(ctx, websocket.MessageBinary, payload); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func receiveEvents(t *testing.T, events <-chan runtimepkg.ProviderEvent, count int) []runtimepkg.ProviderEvent {
	t.Helper()
	result := make([]runtimepkg.ProviderEvent, 0, count)
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for len(result) < count {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatalf("events closed after %d/%d", len(result), count)
			}
			if event.Err != nil {
				t.Fatalf("provider error: %v", event.Err)
			}
			result = append(result, event)
		case <-timer.C:
			t.Fatalf("timed out after %d/%d events", len(result), count)
		}
	}
	return result
}
