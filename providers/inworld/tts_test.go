package inworld

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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
)

func TestTTSCommitUsesDocumentedWebSocketProtocol(t *testing.T) {
	t.Parallel()

	observed := make(chan map[string]any, 2)
	harness := newTTSHarness(t, func(ctx context.Context, conn *websocket.Conn, _ *http.Request) {
		create := readTTSJSON(t, ctx, conn)
		observed <- create
		contextID := create["context_id"].(string)
		send := readTTSJSON(t, ctx, conn)
		observed <- send
		writeTTSJSON(t, ctx, conn, map[string]any{"result": map[string]any{
			"contextId":  contextID,
			"audioChunk": map[string]any{"audioContent": base64.StdEncoding.EncodeToString([]byte{1, 2})},
		}})
		writeTTSJSON(t, ctx, conn, map[string]any{"result": map[string]any{"contextId": contextID, "flushCompleted": map[string]any{}}})
	})
	defer harness.Close()

	stream := openTTSStream(t, harness, nil)
	defer func() { _ = stream.Abort(context.Background()) }()
	if err := stream.AppendText(context.Background(), "Hello, "); err != nil {
		t.Fatalf("append text: %v", err)
	}
	if err := stream.AppendText(context.Background(), "world!"); err != nil {
		t.Fatalf("append second fragment: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit text: %v", err)
	}

	create := <-observed
	contextID, _ := create["context_id"].(string)
	if contextID == "" {
		t.Fatal("create.context_id is empty")
	}
	wantCreate := map[string]any{
		"context_id": contextID,
		"create": map[string]any{
			"voice_id": "Dennis", "model_id": "inworld-tts-2", "language": "en-US",
			"audio_config": map[string]any{"audio_encoding": "PCM", "sample_rate_hertz": float64(24_000)},
		},
	}
	if fmt.Sprint(create) != fmt.Sprint(wantCreate) {
		t.Fatalf("create = %v, want %v", create, wantCreate)
	}
	send := <-observed
	wantSend := map[string]any{
		"context_id": contextID,
		"send_text":  map[string]any{"text": "Hello, world!", "flush_context": map[string]any{}},
	}
	if fmt.Sprint(send) != fmt.Sprint(wantSend) {
		t.Fatalf("send = %v, want %v", send, wantSend)
	}
	if auth := <-harness.authorization; auth != "Basic customer-inworld-key" {
		t.Fatalf("Authorization = %q", auth)
	}
	if query := <-harness.rawQuery; query != "" {
		t.Fatalf("credential leaked into query %q", query)
	}
	if got := eventTypes(collectTTSEvents(t, stream.Events(), 3)); got != "audio.started,audio.frame,audio.done" {
		t.Fatalf("events = %s", got)
	}
}

func TestTTSManagedTokenUsesBearerHeader(t *testing.T) {
	t.Parallel()
	harness := newTTSHarness(t, func(context.Context, *websocket.Conn, *http.Request) {})
	defer harness.Close()
	stream := openTTSStream(t, harness, func(request *runtimepkg.AdapterRequest) {
		request.Plan.Execution.CredentialSource = protocol.CredentialsManaged
		request.Plan.Route.Credential.Value = "one-time-token"
	})
	defer func() { _ = stream.Abort(context.Background()) }()
	if auth := <-harness.authorization; auth != "Bearer one-time-token" {
		t.Fatalf("Authorization = %q, want managed Bearer", auth)
	}
}

func TestTTSEmitsPCMAlignmentUsageAndDone(t *testing.T) {
	t.Parallel()
	first, second := []byte{1, 2, 3, 4}, []byte{5, 6}
	harness := newTTSHarness(t, func(ctx context.Context, conn *websocket.Conn, _ *http.Request) {
		create := readTTSJSON(t, ctx, conn)
		contextID := create["context_id"].(string)
		_ = readTTSJSON(t, ctx, conn)
		writeTTSJSON(t, ctx, conn, map[string]any{"result": map[string]any{
			"contextId": contextID,
			"audioChunk": map[string]any{
				"audioContent":  base64.StdEncoding.EncodeToString(first),
				"timestampInfo": map[string]any{"wordAlignment": []any{}},
			},
		}})
		writeTTSJSON(t, ctx, conn, map[string]any{"result": map[string]any{
			"contextId":  contextID,
			"audioChunk": map[string]any{"audioContent": base64.RawStdEncoding.EncodeToString(second)},
			"usage":      map[string]any{"processedCharactersCount": 13},
		}})
		writeTTSJSON(t, ctx, conn, map[string]any{"result": map[string]any{"contextId": contextID, "flushCompleted": map[string]any{}}})
	})
	defer harness.Close()

	stream := openTTSStream(t, harness, nil)
	defer func() { _ = stream.Abort(context.Background()) }()
	synthesizeTTS(t, stream, "Hello, world!")
	events := collectTTSEvents(t, stream.Events(), 6)
	if got := eventTypes(events); got != "alignment,audio.started,audio.frame,audio.frame,usage.observed,audio.done" {
		t.Fatalf("events = %s", got)
	}
	if string(events[2].Audio) != string(first) || string(events[3].Audio) != string(second) {
		t.Fatalf("audio frames = %v, %v", events[2].Audio, events[3].Audio)
	}
	if events[2].Extensions[extensionID] == nil {
		t.Fatal("audio frame lost raw Inworld extension")
	}
	var usage struct {
		Usage struct {
			Characters int `json:"processedCharactersCount"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(events[4].Data, &usage); err != nil || usage.Usage.Characters != 13 {
		t.Fatalf("usage = %s, err=%v", events[4].Data, err)
	}
}

func TestTTSSequentialUtterancesReuseOneContextAndConnection(t *testing.T) {
	t.Parallel()
	texts := make(chan string, 2)
	contexts := make(chan string, 2)
	harness := newTTSHarness(t, func(ctx context.Context, conn *websocket.Conn, _ *http.Request) {
		create := readTTSJSON(t, ctx, conn)
		contextID := create["context_id"].(string)
		for range 2 {
			send := readTTSJSON(t, ctx, conn)
			sendText := send["send_text"].(map[string]any)
			texts <- sendText["text"].(string)
			contexts <- send["context_id"].(string)
			writeTTSJSON(t, ctx, conn, map[string]any{"result": map[string]any{
				"contextId":  contextID,
				"audioChunk": map[string]any{"audioContent": base64.StdEncoding.EncodeToString([]byte{1})},
			}})
			writeTTSJSON(t, ctx, conn, map[string]any{"result": map[string]any{"contextId": contextID, "flushCompleted": map[string]any{}}})
		}
	})
	defer harness.Close()

	stream := openTTSStream(t, harness, nil)
	defer func() { _ = stream.Abort(context.Background()) }()
	for _, text := range []string{"first", "second"} {
		synthesizeTTS(t, stream, text)
		if got := eventTypes(collectTTSEvents(t, stream.Events(), 3)); got != "audio.started,audio.frame,audio.done" {
			t.Fatalf("%s events = %s", text, got)
		}
		if got := <-texts; got != text {
			t.Fatalf("text = %q, want %q", got, text)
		}
	}
	firstContext, secondContext := <-contexts, <-contexts
	if firstContext == "" || firstContext != secondContext {
		t.Fatalf("contexts = %q, %q; want one persistent context", firstContext, secondContext)
	}
	if connections := harness.connections.Load(); connections != 1 {
		t.Fatalf("connections = %d, want 1", connections)
	}
}

func TestTTSCancelClosesContextAndNextUtteranceGetsFreshContext(t *testing.T) {
	t.Parallel()
	contexts := make(chan string, 2)
	harness := newTTSHarness(t, func(ctx context.Context, conn *websocket.Conn, _ *http.Request) {
		firstCreate := readTTSJSON(t, ctx, conn)
		firstID := firstCreate["context_id"].(string)
		contexts <- firstID
		_ = readTTSJSON(t, ctx, conn)
		closeFrame := readTTSJSON(t, ctx, conn)
		if _, ok := closeFrame["close_context"]; !ok {
			t.Errorf("cancel frame = %v", closeFrame)
		}
		writeTTSJSON(t, ctx, conn, map[string]any{"result": map[string]any{"contextId": firstID, "contextClosed": map[string]any{}}})

		secondCreate := readTTSJSON(t, ctx, conn)
		secondID := secondCreate["context_id"].(string)
		contexts <- secondID
		_ = readTTSJSON(t, ctx, conn)
		writeTTSJSON(t, ctx, conn, map[string]any{"result": map[string]any{
			"contextId": secondID, "audioChunk": map[string]any{"audioContent": base64.StdEncoding.EncodeToString([]byte{1})},
		}})
		writeTTSJSON(t, ctx, conn, map[string]any{"result": map[string]any{"contextId": secondID, "flushCompleted": map[string]any{}}})
	})
	defer harness.Close()

	stream := openTTSStream(t, harness, nil)
	defer func() { _ = stream.Abort(context.Background()) }()
	synthesizeTTS(t, stream, "cancel me")
	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	synthesizeTTS(t, stream, "next")
	_ = collectTTSEvents(t, stream.Events(), 3)
	first, second := <-contexts, <-contexts
	if first == second {
		t.Fatalf("canceled context %q was reused", first)
	}
}

func TestTTSCloseWaitsForFlushThenClosesContext(t *testing.T) {
	t.Parallel()
	allowFlush := make(chan struct{})
	sawClose := make(chan struct{})
	harness := newTTSHarness(t, func(ctx context.Context, conn *websocket.Conn, _ *http.Request) {
		create := readTTSJSON(t, ctx, conn)
		contextID := create["context_id"].(string)
		_ = readTTSJSON(t, ctx, conn)
		<-allowFlush
		writeTTSJSON(t, ctx, conn, map[string]any{"result": map[string]any{
			"contextId": contextID, "audioChunk": map[string]any{"audioContent": base64.StdEncoding.EncodeToString([]byte{1})},
		}})
		writeTTSJSON(t, ctx, conn, map[string]any{"result": map[string]any{"contextId": contextID, "flushCompleted": map[string]any{}}})
		closeFrame := readTTSJSON(t, ctx, conn)
		if _, ok := closeFrame["close_context"]; ok {
			close(sawClose)
		}
		writeTTSJSON(t, ctx, conn, map[string]any{"result": map[string]any{"contextId": contextID, "contextClosed": map[string]any{}}})
	})
	defer harness.Close()

	stream := openTTSStream(t, harness, nil)
	synthesizeTTS(t, stream, "finish me")
	closed := make(chan error, 1)
	go func() { closed <- stream.Close(context.Background()) }()
	select {
	case <-sawClose:
		t.Fatal("close_context arrived before synthesis flushed")
	case <-time.After(30 * time.Millisecond):
	}
	close(allowFlush)
	if got := eventTypes(collectTTSEvents(t, stream.Events(), 3)); got != "audio.started,audio.frame,audio.done" {
		t.Fatalf("events = %s", got)
	}
	select {
	case <-sawClose:
	case <-time.After(2 * time.Second):
		t.Fatal("close_context not sent")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("close did not finish")
	}
	if _, ok := <-stream.Events(); ok {
		t.Fatal("events channel remains open")
	}
}

func TestTTSInBandErrorsAreClassified(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		code      int
		want      string
		retryable bool
	}{{16, "authentication_failed", false}, {8, "provider_rate_limited", true}, {14, "provider_unavailable", true}, {3, "invalid_request", false}} {
		t.Run(fmt.Sprint(testCase.code), func(t *testing.T) {
			t.Parallel()
			harness := newTTSHarness(t, func(ctx context.Context, conn *websocket.Conn, _ *http.Request) {
				_ = readTTSJSON(t, ctx, conn)
				_ = readTTSJSON(t, ctx, conn)
				writeTTSJSON(t, ctx, conn, map[string]any{"error": map[string]any{"code": testCase.code, "message": "nope"}})
			})
			defer harness.Close()
			stream := openTTSStream(t, harness, nil)
			defer func() { _ = stream.Abort(context.Background()) }()
			synthesizeTTS(t, stream, "hello")
			event := nextTTSEvent(t, stream.Events())
			var providerErr *runtimepkg.ProviderError
			if !errors.As(event.Err, &providerErr) || providerErr.Code != testCase.want || providerErr.Retryable != testCase.retryable {
				t.Fatalf("error = %#v, want %s retryable=%t", event.Err, testCase.want, testCase.retryable)
			}
			if providerErr.Extensions[extensionID] == nil {
				t.Fatal("provider error lost raw payload")
			}
		})
	}
}

func TestTTSHandshakeErrorsAreClassified(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		status int
		code   string
	}{{http.StatusUnauthorized, "authentication_failed"}, {http.StatusTooManyRequests, "provider_rate_limited"}, {http.StatusServiceUnavailable, "provider_unavailable"}} {
		t.Run(fmt.Sprint(testCase.status), func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(testCase.status) }))
			defer server.Close()
			endpoint := strings.Replace(server.URL, "http://", "ws://", 1) + streamPath
			adapter, err := New(Config{AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
			if err != nil {
				t.Fatal(err)
			}
			_, err = adapter.Open(context.Background(), ttsAdapterRequest(endpoint))
			var providerErr *runtimepkg.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Code != testCase.code || providerErr.ProviderStatus != testCase.status {
				t.Fatalf("error = %#v, want %s status=%d", err, testCase.code, testCase.status)
			}
		})
	}
}

func TestTTSOpenValidationAndLocalLimits(t *testing.T) {
	t.Parallel()
	base := ttsAdapterRequest("wss://api.inworld.ai" + streamPath)
	for _, testCase := range []struct {
		name   string
		mutate func(*runtimepkg.AdapterRequest)
		want   string
	}{
		{"stt", func(r *runtimepkg.AdapterRequest) { r.Kind = protocol.SessionKindSTT }, "tts sessions"},
		{"provider", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Provider = "cartesia" }, "cannot open provider"},
		{"transport", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Transport = protocol.TransportHTTP }, "websocket transport"},
		{"credential", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential = nil }, "bearer credential"},
		{"model", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "auto" }, "concrete model"},
		{"retired model", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "inworld-tts-1" }, "discontinued"},
		{"voice", func(r *runtimepkg.AdapterRequest) { r.Options.Voice = " " }, "voice id"},
		{"media", func(r *runtimepkg.AdapterRequest) { r.Media.Encoding = "opus" }, "mono pcm_s16le"},
		{"rate", func(r *runtimepkg.AdapterRequest) { r.Media.SampleRateHz = 11_025 }, "sample rate"},
		{"path", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = "wss://api.inworld.ai/tts/v1/voice:stream" }, "endpoint path"},
		{"host", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = "wss://evil.example" + streamPath }, "host is not allowed"},
		{"query", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint += "?authorization=leak" }, "clean absolute WebSocket URL"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			r := base
			credential := *base.Plan.Route.Credential
			r.Plan.Route.Credential = &credential
			media := *base.Media
			r.Media = &media
			testCase.mutate(&r)
			adapter, _ := New(Config{})
			_, err := adapter.Open(context.Background(), r)
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("error = %v, want %q", err, testCase.want)
			}
			if strings.Contains(err.Error(), "customer-inworld-key") {
				t.Fatalf("credential leaked: %v", err)
			}
		})
	}

	harness := newTTSHarness(t, func(context.Context, *websocket.Conn, *http.Request) {})
	defer harness.Close()
	stream := openTTSStream(t, harness, nil)
	defer func() { _ = stream.Abort(context.Background()) }()
	if err := stream.WriteAudio(context.Background(), []byte{1}); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Fatalf("WriteAudio = %v", err)
	}
	if err := stream.CommitAudio(context.Background()); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Fatalf("CommitAudio = %v", err)
	}
	if err := stream.CommitText(context.Background()); err == nil || !strings.Contains(err.Error(), "no buffered text") {
		t.Fatalf("empty commit = %v", err)
	}
	if err := stream.AppendText(context.Background(), strings.Repeat("é", maxInputCharacters)); err != nil {
		t.Fatalf("2000 runes: %v", err)
	}
	var providerErr *runtimepkg.ProviderError
	if err := stream.AppendText(context.Background(), "!"); !errors.As(err, &providerErr) || providerErr.Code != "input_too_large" {
		t.Fatalf("oversize append = %v", err)
	}
}

func TestTTSFlushWithoutAudioIsFailure(t *testing.T) {
	t.Parallel()
	harness := newTTSHarness(t, func(ctx context.Context, conn *websocket.Conn, _ *http.Request) {
		create := readTTSJSON(t, ctx, conn)
		_ = readTTSJSON(t, ctx, conn)
		writeTTSJSON(t, ctx, conn, map[string]any{"result": map[string]any{"contextId": create["context_id"], "flushCompleted": map[string]any{}}})
	})
	defer harness.Close()
	stream := openTTSStream(t, harness, nil)
	defer func() { _ = stream.Abort(context.Background()) }()
	synthesizeTTS(t, stream, "silent")
	var providerErr *runtimepkg.ProviderError
	if event := nextTTSEvent(t, stream.Events()); !errors.As(event.Err, &providerErr) || providerErr.Code != "provider_unavailable" {
		t.Fatalf("event = %#v", event)
	}
}

type ttsTestStream interface {
	runtimepkg.ProviderStream
	runtimepkg.AbortingProviderStream
}

type ttsHarness struct {
	server        *httptest.Server
	endpoint      string
	authorization chan string
	rawQuery      chan string
	connections   atomic.Int32
}

func newTTSHarness(t *testing.T, respond func(context.Context, *websocket.Conn, *http.Request)) *ttsHarness {
	t.Helper()
	h := &ttsHarness{authorization: make(chan string, 1), rawQuery: make(chan string, 1)}
	h.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != streamPath {
			http.NotFound(w, r)
			return
		}
		h.connections.Add(1)
		h.authorization <- r.Header.Get("Authorization")
		h.rawQuery <- r.URL.RawQuery
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		defer conn.CloseNow()
		respond(r.Context(), conn, r)
	}))
	parsed, _ := url.Parse(h.server.URL)
	parsed.Scheme = "ws"
	parsed.Path = streamPath
	h.endpoint = parsed.String()
	return h
}

func (h *ttsHarness) Close() { h.server.Close() }

func openTTSStream(t *testing.T, harness *ttsHarness, mutate func(*runtimepkg.AdapterRequest)) ttsTestStream {
	t.Helper()
	parsed, _ := url.Parse(harness.endpoint)
	adapter, err := New(Config{AllowedEndpointHosts: []string{parsed.Hostname()}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	request := ttsAdapterRequest(harness.endpoint)
	if mutate != nil {
		mutate(&request)
	}
	opened, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	stream, ok := opened.(ttsTestStream)
	if !ok {
		t.Fatalf("stream %T does not implement abort", opened)
	}
	return stream
}

func ttsAdapterRequest(endpoint string) runtimepkg.AdapterRequest {
	now := time.Date(2026, time.September, 9, 0, 0, 0, 0, time.UTC)
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindTTS,
		Plan: protocol.SessionPlan{
			PlanID: "plan_inworld", SessionID: "sess_inworld", AttemptID: "att_1", ExpiresAt: now.Add(time.Hour),
			Execution: protocol.Execution{Placement: protocol.PlacementEmbedded, ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK},
			Route: protocol.PlanRoute{
				Provider: "inworld", Model: DefaultModel, Adapter: AdapterID,
				Transport: protocol.TransportWebSocket, Endpoint: endpoint,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "customer-inworld-key", ExpiresAt: now.Add(30 * time.Minute)},
			},
		},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1},
		Options: protocol.RequestOptions{Voice: "Dennis", Language: "en-US"},
	}
}

func synthesizeTTS(t *testing.T, stream runtimepkg.ProviderStream, text string) {
	t.Helper()
	if err := stream.AppendText(context.Background(), text); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func readTTSJSON(t *testing.T, ctx context.Context, conn *websocket.Conn) map[string]any {
	t.Helper()
	_, payload, err := conn.Read(ctx)
	if err != nil {
		t.Errorf("read websocket: %v", err)
		return nil
	}
	var value map[string]any
	if err := json.Unmarshal(payload, &value); err != nil {
		t.Errorf("decode websocket: %v", err)
	}
	return value
}

func writeTTSJSON(t *testing.T, ctx context.Context, conn *websocket.Conn, value any) {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Errorf("marshal websocket: %v", err)
		return
	}
	if err := conn.Write(ctx, websocket.MessageText, payload); err != nil {
		t.Errorf("write websocket: %v", err)
	}
}

func collectTTSEvents(t *testing.T, events <-chan runtimepkg.ProviderEvent, count int) []runtimepkg.ProviderEvent {
	t.Helper()
	got := make([]runtimepkg.ProviderEvent, 0, count)
	for len(got) < count {
		got = append(got, nextTTSEvent(t, events))
	}
	return got
}

func nextTTSEvent(t *testing.T, events <-chan runtimepkg.ProviderEvent) runtimepkg.ProviderEvent {
	t.Helper()
	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("events closed early")
		}
		return event
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
		return runtimepkg.ProviderEvent{}
	}
}

func eventTypes(events []runtimepkg.ProviderEvent) string {
	values := make([]string, len(events))
	for i, event := range events {
		if event.Err != nil {
			values[i] = "error"
		} else {
			values[i] = string(event.Type)
		}
	}
	return strings.Join(values, ",")
}
