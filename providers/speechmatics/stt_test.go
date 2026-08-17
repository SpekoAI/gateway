package speechmatics

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

func TestRealtimeStreamsAndFlushesBeforeShutdown(t *testing.T) {
	h := newHarness(t)
	defer h.Close()
	adapter, err := New(Config{AllowedEndpointHosts: []string{h.host}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	diarize := true
	request := testRequest(h.endpoint)
	request.Options.Language = "en-US"
	request.Options.STT = &protocol.SttOptions{Diarization: &diarize, Keywords: []string{"Speko", "Tashkent"}}
	providerStream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = providerStream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()

	if got := h.authorization(t); got != "Bearer temporary-jwt" {
		t.Fatalf("authorization = %q", got)
	}
	config := h.start(t)["transcription_config"].(map[string]any)
	if config["language"] != "en" || config["output_locale"] != "en-US" || config["model"] != "standard" {
		t.Fatalf("language/model config = %v", config)
	}
	if config["enable_partials"] != true || config["max_delay"] != 0.7 || config["max_delay_mode"] != "fixed" {
		t.Fatalf("low-latency config = %v", config)
	}
	if config["diarization"] != "speaker" {
		t.Fatalf("diarization config = %v", config)
	}
	vocab := config["additional_vocab"].([]any)
	if len(vocab) != 2 || vocab[1] != "Tashkent" {
		t.Fatalf("additional vocab = %v", vocab)
	}

	usage := nextEvent(t, providerStream.Events())
	if usage.Type != protocol.EventUsageObserved || dataString(t, usage.Data, "provider_request_id") != "recognition-1" {
		t.Fatalf("first event = %#v", usage)
	}
	if err := providerStream.WriteAudio(context.Background(), []byte{1, 2, 3, 4}); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	messageType, payload, err := h.conn(t).Read(context.Background())
	if err != nil || messageType != websocket.MessageBinary || string(payload) != string([]byte{1, 2, 3, 4}) {
		t.Fatalf("audio frame type=%v payload=%v err=%v", messageType, payload, err)
	}

	h.send(t, transcriptFrame("AddPartialTranscript", "hello wor", 0, 0.8, 0.1))
	delta := nextEvent(t, providerStream.Events())
	if delta.Type != protocol.EventTranscriptDelta || dataString(t, delta.Data, "text") != "hello wor" {
		t.Fatalf("delta = %#v", delta)
	}
	h.send(t, transcriptFrame("AddTranscript", "hello world", 0, 1, 0.9))
	final := nextEvent(t, providerStream.Events())
	if final.Type != protocol.EventTranscriptFinal || dataString(t, final.Data, "text") != "hello world" {
		t.Fatalf("final = %#v", final)
	}

	if err := providerStream.CommitAudio(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if got := h.readJSON(t)["message"]; got != "ForceEndOfUtterance" {
		t.Fatalf("commit message = %v", got)
	}
	if err := providerStream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	end := h.readJSON(t)
	if end["message"] != "EndOfStream" || end["last_seq_no"] != float64(1) {
		t.Fatalf("end frame = %v", end)
	}
	if err := providerStream.WriteAudio(context.Background(), []byte{5}); !errors.Is(err, runtimepkg.ErrSessionClosed) {
		t.Fatalf("post-close write = %v", err)
	}

	// Speechmatics sends pending finals after EndOfStream. Close must leave the
	// reader alive until EndOfTranscript or this tail is silently lost.
	h.send(t, transcriptFrame("AddTranscript", "tail", 1, 1.2, 0.8))
	if tail := nextEvent(t, providerStream.Events()); dataString(t, tail.Data, "text") != "tail" {
		t.Fatalf("tail = %#v", tail)
	}
	h.send(t, map[string]any{"message": "EndOfTranscript"})
	select {
	case _, open := <-providerStream.Events():
		if open {
			t.Fatal("events remained open after EndOfTranscript")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("events did not close after EndOfTranscript")
	}
}

func TestModelsEndpointsAndPlanValidation(t *testing.T) {
	t.Parallel()
	adapter, err := New(Config{})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	for _, endpoint := range []string{"wss://global.rt.speechmatics.com/v2/", "wss://eu.rt.speechmatics.com/v2/", "wss://us.rt.speechmatics.com/v2/"} {
		if _, err := adapter.endpointPolicy.Parse(endpoint); err != nil {
			t.Errorf("endpoint %q rejected: %v", endpoint, err)
		}
	}
	if _, ok := supportedModels["standard"]; !ok {
		t.Fatal("standard missing")
	}
	if _, ok := supportedModels["enhanced"]; !ok {
		t.Fatal("enhanced missing")
	}
	if _, ok := supportedModels["melia-1"]; ok {
		t.Fatal("batch-only melia-1 must not be advertised as realtime")
	}
	if _, ok := supportedEncodings["pcm_s16le"]; !ok || len(supportedEncodings) != 1 {
		t.Fatalf("gateway media contract must expose only pcm_s16le, got %v", supportedEncodings)
	}
	for name, mutate := range map[string]func(*runtimepkg.AdapterRequest){
		"wrong kind":       func(r *runtimepkg.AdapterRequest) { r.Kind = protocol.SessionKindTTS },
		"wrong provider":   func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Provider = "deepgram" },
		"wrong transport":  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Transport = protocol.TransportHTTP },
		"missing media":    func(r *runtimepkg.AdapterRequest) { r.Media = nil },
		"stereo":           func(r *runtimepkg.AdapterRequest) { r.Media.Channels = 2 },
		"unknown encoding": func(r *runtimepkg.AdapterRequest) { r.Media.Encoding = "opus" },
		"unknown model":    func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "melia-1" },
		"auto model":       func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "auto" },
		"empty credential": func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential.Value = "" },
		"wrong credential": func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential.Kind = protocol.CredentialSessionURL },
		"query endpoint": func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Endpoint = "wss://global.rt.speechmatics.com/v2/?jwt=secret"
		},
		"wrong host": func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = "wss://example.com/v2/" },
	} {
		request := testRequest("wss://global.rt.speechmatics.com/v2/")
		mutate(&request)
		if _, err := adapter.Open(context.Background(), request); err == nil {
			t.Errorf("%s: accepted unusable plan", name)
		}
	}
}

func TestErrorClassification(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		vendor    string
		code      string
		retryable bool
	}{
		{"not_authorised", "authentication_failed", false},
		{"quota_exceeded", "provider_quota_exceeded", true},
		{"invalid_config", "invalid_request", false},
		{"job_error", "provider_unavailable", true},
	} {
		code, retryable := classifyError(test.vendor)
		if code != test.code || retryable != test.retryable {
			t.Errorf("%s => (%s,%v), want (%s,%v)", test.vendor, code, retryable, test.code, test.retryable)
		}
	}
}

func testRequest(endpoint string) runtimepkg.AdapterRequest {
	expiresAt := time.Now().Add(time.Minute)
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindSTT,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{Placement: protocol.PlacementEmbedded, ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsManaged},
			Route: protocol.PlanRoute{
				Provider: "speechmatics", Adapter: AdapterID, Model: DefaultModel,
				Transport: protocol.TransportWebSocket, Endpoint: endpoint,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "temporary-jwt", ExpiresAt: expiresAt},
			},
		},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
		Options: protocol.RequestOptions{Language: "en"},
	}
}

func transcriptFrame(kind, text string, start, end, confidence float64) map[string]any {
	return map[string]any{
		"message":  kind,
		"metadata": map[string]any{"start_time": start, "end_time": end, "transcript": text},
		"results": []map[string]any{{"type": "word", "start_time": start, "end_time": end,
			"alternatives": []map[string]any{{"content": text, "confidence": confidence, "language": "en", "speaker": "S1"}}}},
	}
}

type harness struct {
	server   *httptest.Server
	endpoint string
	host     string
	connCh   chan *websocket.Conn
	authCh   chan string
	startCh  chan map[string]any
	socket   *websocket.Conn
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{connCh: make(chan *websocket.Conn, 1), authCh: make(chan string, 1), startCh: make(chan map[string]any, 1)}
	h.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := websocket.Accept(writer, request, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		h.authCh <- request.Header.Get("Authorization")
		messageType, payload, err := conn.Read(context.Background())
		if err != nil || messageType != websocket.MessageText {
			t.Errorf("read start: type=%v err=%v", messageType, err)
			_ = conn.CloseNow()
			return
		}
		var start map[string]any
		if err := json.Unmarshal(payload, &start); err != nil {
			t.Errorf("decode start: %v", err)
			_ = conn.CloseNow()
			return
		}
		h.startCh <- start
		if err := writeJSON(context.Background(), conn, map[string]any{"message": "RecognitionStarted", "id": "recognition-1"}); err != nil {
			t.Errorf("write started: %v", err)
			_ = conn.CloseNow()
			return
		}
		h.connCh <- conn
	}))
	parsed, err := url.Parse(h.server.URL)
	if err != nil {
		t.Fatalf("parse server URL: %v", err)
	}
	h.host = parsed.Hostname()
	h.endpoint = "ws" + strings.TrimPrefix(h.server.URL, "http") + endpointPath
	return h
}

func (h *harness) Close() {
	if h.socket != nil {
		_ = h.socket.CloseNow()
	}
	h.server.Close()
}

func (h *harness) conn(t *testing.T) *websocket.Conn {
	t.Helper()
	if h.socket == nil {
		select {
		case h.socket = <-h.connCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for provider socket")
		}
	}
	return h.socket
}

func (h *harness) authorization(t *testing.T) string {
	t.Helper()
	select {
	case value := <-h.authCh:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for authorization")
		return ""
	}
}

func (h *harness) start(t *testing.T) map[string]any {
	t.Helper()
	select {
	case value := <-h.startCh:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for StartRecognition")
		return nil
	}
}

func (h *harness) send(t *testing.T, value any) {
	t.Helper()
	if err := writeJSON(context.Background(), h.conn(t), value); err != nil {
		t.Fatalf("provider send: %v", err)
	}
}

func (h *harness) readJSON(t *testing.T) map[string]any {
	t.Helper()
	messageType, payload, err := h.conn(t).Read(context.Background())
	if err != nil || messageType != websocket.MessageText {
		t.Fatalf("read JSON: type=%v err=%v", messageType, err)
	}
	var value map[string]any
	if err := json.Unmarshal(payload, &value); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
	return value
}

func nextEvent(t *testing.T, events <-chan runtimepkg.ProviderEvent) runtimepkg.ProviderEvent {
	t.Helper()
	select {
	case event, open := <-events:
		if !open {
			t.Fatal("events closed early")
		}
		if event.Err != nil {
			t.Fatalf("provider error: %v", event.Err)
		}
		return event
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
		return runtimepkg.ProviderEvent{}
	}
}

func dataString(t *testing.T, raw json.RawMessage, key string) string {
	t.Helper()
	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		t.Fatalf("decode event data: %v", err)
	}
	value, _ := data[key].(string)
	return value
}
