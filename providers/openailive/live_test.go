package openailive

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

// fakeLive is an in-process GPT-Live upstream. It records what the adapter
// sent and scripts vendor behaviour per test through hooks.
type fakeLive struct {
	t      *testing.T
	server *httptest.Server
	path   string

	mu       sync.Mutex
	headers  http.Header
	rawQuery string
	start    map[string]any
	appends  []string
	controls []map[string]any
	closes   int

	// hooks
	onStart    func(conn *websocket.Conn, ctx context.Context) bool // return false to stop handling
	onControl  func(conn *websocket.Conn, ctx context.Context, control map[string]any)
	onClose    func(conn *websocket.Conn, ctx context.Context)
	skipClosed bool
}

func newFakeLive(t *testing.T, path string) *fakeLive {
	fake := &fakeLive{t: t, path: path}
	mux := http.NewServeMux()
	mux.HandleFunc(path, fake.handle)
	fake.server = httptest.NewServer(mux)
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeLive) endpoint() string {
	return "ws" + strings.TrimPrefix(f.server.URL, "http") + f.path
}

func (f *fakeLive) host() string {
	parsed, _ := url.Parse(f.server.URL)
	return parsed.Hostname()
}

func send(t *testing.T, conn *websocket.Conn, ctx context.Context, message string) bool {
	t.Helper()
	return conn.Write(ctx, websocket.MessageText, []byte(message)) == nil
}

func (f *fakeLive) handle(writer http.ResponseWriter, request *http.Request) {
	f.mu.Lock()
	f.headers = request.Header.Clone()
	f.rawQuery = request.URL.RawQuery
	f.mu.Unlock()
	conn, err := websocket.Accept(writer, request, nil)
	if err != nil {
		f.t.Errorf("accept: %v", err)
		return
	}
	defer conn.CloseNow()
	ctx := request.Context()
	_, payload, err := conn.Read(ctx)
	if err != nil {
		return
	}
	var start struct {
		Type    string         `json:"type"`
		Session map[string]any `json:"session"`
	}
	if json.Unmarshal(payload, &start) != nil || start.Type != "session.start" {
		f.t.Errorf("first frame was not session.start: %s", payload)
		return
	}
	f.mu.Lock()
	f.start = start.Session
	f.mu.Unlock()
	if f.onStart != nil {
		if !f.onStart(conn, ctx) {
			return
		}
	} else if !send(f.t, conn, ctx, `{"type":"session.started","event_id":"evt_started","session":{"id":"sess_1","model":"gpt-live-1"}}`) {
		return
	}
	for {
		_, frame, err := conn.Read(ctx)
		if err != nil {
			return
		}
		var event map[string]any
		if json.Unmarshal(frame, &event) != nil {
			continue
		}
		switch event["type"] {
		case "session.input_audio.append":
			f.mu.Lock()
			f.appends = append(f.appends, event["audio"].(string))
			f.mu.Unlock()
		case "session.close":
			f.mu.Lock()
			f.closes++
			f.mu.Unlock()
			if f.onClose != nil {
				f.onClose(conn, ctx)
			}
			if !f.skipClosed {
				send(f.t, conn, ctx, `{"type":"session.closed","event_id":"evt_closed","reason":"close_requested","usage":{"seconds":12.5},"session":{"id":"sess_1"}}`)
			}
		default:
			f.mu.Lock()
			f.controls = append(f.controls, event)
			f.mu.Unlock()
			if f.onControl != nil {
				f.onControl(conn, ctx, event)
			}
		}
	}
}

func liveRequest(endpoint string, rate int) runtimepkg.AdapterRequest {
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindRealtime,
		Plan: protocol.SessionPlan{
			AttemptID: "att_live_1",
			Execution: protocol.Execution{ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK},
			Route: protocol.PlanRoute{
				Provider: "openai", Model: "gpt-live-1", Transport: protocol.TransportWebSocket, Endpoint: endpoint,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "sk-customer-key", ExpiresAt: time.Now().Add(time.Minute)},
			},
		},
		Media: &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: rate, Channels: 1},
		Options: protocol.RequestOptions{
			S2S: &protocol.S2SOptions{
				Instructions: "Be concise.",
				OutputMedia:  &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: rate, Channels: 1},
			},
		},
	}
}

func newAdapter(t *testing.T, fake *fakeLive, native bool) *Adapter {
	t.Helper()
	adapter, err := New(Config{AllowedEndpointHosts: []string{fake.host()}, AllowInsecureEndpoint: true, SetupTimeout: 2 * time.Second, CloseDrainTimeout: time.Second, NativeEvents: native})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return adapter
}

func nextEvent(t *testing.T, ctx context.Context, stream runtimepkg.ProviderStream) runtimepkg.ProviderEvent {
	t.Helper()
	select {
	case event, ok := <-stream.Events():
		if !ok {
			t.Fatal("events closed")
		}
		return event
	case <-ctx.Done():
		t.Fatal("timed out waiting for event")
	}
	return runtimepkg.ProviderEvent{}
}

func nextOfType(t *testing.T, ctx context.Context, stream runtimepkg.ProviderStream, want protocol.EventType) runtimepkg.ProviderEvent {
	t.Helper()
	for {
		event := nextEvent(t, ctx, stream)
		if event.Err != nil {
			t.Fatalf("stream error while waiting for %s: %v", want, event.Err)
		}
		if event.Type == want {
			return event
		}
	}
}

func decodeProviderEvent(t *testing.T, event runtimepkg.ProviderEvent) (protocol.ProviderEvent, map[string]any) {
	t.Helper()
	var envelope protocol.ProviderEvent
	if err := json.Unmarshal(event.Data, &envelope); err != nil {
		t.Fatalf("decode provider event: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	return envelope, payload
}

func TestSessionStartShapeAndConnection(t *testing.T) {
	t.Parallel()
	fake := newFakeLive(t, sessionsPath)
	adapter := newAdapter(t, fake, false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request := liveRequest(fake.endpoint(), 16_000)
	parallel := false
	request.Options.Voice = "quartz"
	request.Options.S2S.Live = &protocol.LiveOptions{
		History: []protocol.LiveHistoryItem{{Role: "user", Text: "My order is A0042."}, {Role: "assistant", Text: "Noted."}},
		Delegation: &protocol.LiveDelegation{Type: protocol.LiveDelegationResponses, Responses: &protocol.LiveResponsesConfig{
			Model: "gpt-5.6-luna", Instructions: "Backend rules.", MaxOutputTokens: 512, ParallelToolCalls: &parallel,
			Tools: []protocol.LiveTool{protocol.NewLiveTool(json.RawMessage(`{"type":"web_search"}`)), protocol.NewLiveTool(json.RawMessage(`{"type":"function","name":"lookup","parameters":{"type":"object"}}`))},
		}},
	}
	stream, err := adapter.Open(ctx, request)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer stream.Close(context.Background())

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.rawQuery != "" {
		t.Fatalf("live socket must take no query parameters, got %q", fake.rawQuery)
	}
	if got := fake.headers.Get("Authorization"); got != "Bearer sk-customer-key" {
		t.Fatalf("authorization = %q", got)
	}
	if fake.headers.Get("Idempotency-Key") != "" {
		t.Fatal("provider-direct dial must not carry a Router idempotency key")
	}
	if fake.start["model"] != "gpt-live-1" || fake.start["instructions"] != "Be concise." {
		t.Fatalf("session = %#v", fake.start)
	}
	audio := fake.start["audio"].(map[string]any)
	format := audio["format"].(map[string]any)
	if format["type"] != "audio/pcm" || format["rate"].(float64) != 16_000 {
		t.Fatalf("audio format = %#v", format)
	}
	if audio["output"].(map[string]any)["voice"] != "quartz" {
		t.Fatalf("voice = %#v", audio["output"])
	}
	input := fake.start["input"].([]any)
	if len(input) != 2 {
		t.Fatalf("input = %#v", input)
	}
	first := input[0].(map[string]any)
	if first["role"] != "user" || first["content"].([]any)[0].(map[string]any)["type"] != "input_text" {
		t.Fatalf("history[0] = %#v", first)
	}
	second := input[1].(map[string]any)
	if second["content"].([]any)[0].(map[string]any)["type"] != "output_text" {
		t.Fatalf("history[1] = %#v", second)
	}
	delegation := fake.start["delegation"].(map[string]any)
	if delegation["type"] != "responses" {
		t.Fatalf("delegation = %#v", delegation)
	}
	responses := delegation["responses"].(map[string]any)
	if responses["model"] != "gpt-5.6-luna" || responses["instructions"] != "Backend rules." || responses["max_output_tokens"].(float64) != 512 || responses["parallel_tool_calls"] != false {
		t.Fatalf("responses = %#v", responses)
	}
	if tools := responses["tools"].([]any); len(tools) != 2 || tools[0].(map[string]any)["type"] != "web_search" || tools[1].(map[string]any)["name"] != "lookup" {
		t.Fatalf("tools = %#v", responses["tools"])
	}
	// Voice instructions never leak into the backend prompt or vice versa.
	if responses["instructions"] == fake.start["instructions"] {
		t.Fatal("voice and backend instructions were merged")
	}
}

func TestClientDelegationIsTheDefaultAndMarinTheDefaultVoice(t *testing.T) {
	t.Parallel()
	fake := newFakeLive(t, sessionsPath)
	adapter := newAdapter(t, fake, false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := adapter.Open(ctx, liveRequest(fake.endpoint(), 24_000))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer stream.Close(context.Background())
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.start["delegation"].(map[string]any)["type"] != "client" {
		t.Fatalf("delegation = %#v", fake.start["delegation"])
	}
	if fake.start["audio"].(map[string]any)["output"].(map[string]any)["voice"] != DefaultVoice {
		t.Fatalf("voice = %#v", fake.start["audio"])
	}
	if _, present := fake.start["input"]; present {
		t.Fatal("empty history must be omitted")
	}
}

func TestContinuousAudioCarriesOddByteAndNeverCommits(t *testing.T) {
	t.Parallel()
	fake := newFakeLive(t, sessionsPath)
	adapter := newAdapter(t, fake, false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := adapter.Open(ctx, liveRequest(fake.endpoint(), 24_000))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer stream.Close(context.Background())
	if err := stream.WriteAudio(ctx, []byte{1, 2, 3}); err != nil {
		t.Fatalf("WriteAudio: %v", err)
	}
	if err := stream.WriteAudio(ctx, []byte{4, 5, 6, 7}); err != nil {
		t.Fatalf("WriteAudio: %v", err)
	}
	if err := stream.CommitAudio(ctx); err == nil {
		t.Fatal("CommitAudio must be refused as a Realtime-only control")
	} else if !strings.Contains(err.Error(), "Realtime") {
		t.Fatalf("CommitAudio error = %v", err)
	}
	if err := stream.Cancel(ctx); err == nil {
		t.Fatal("Cancel must be refused as a Realtime-only control")
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		fake.mu.Lock()
		appends := append([]string(nil), fake.appends...)
		fake.mu.Unlock()
		if len(appends) == 2 {
			first, _ := base64.StdEncoding.DecodeString(appends[0])
			second, _ := base64.StdEncoding.DecodeString(appends[1])
			if string(first) != string([]byte{1, 2}) || string(second) != string([]byte{3, 4, 5, 6}) {
				t.Fatalf("appends = %v %v", first, second)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("appends = %v", appends)
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, control := range fake.controls {
		if control["type"] == "input_audio_buffer.commit" || control["type"] == "response.create" {
			t.Fatalf("live session sent a realtime turn control: %v", control)
		}
	}
}

func TestTranscriptsPreserveOverlappingTimestampsAndPassEventsThrough(t *testing.T) {
	t.Parallel()
	fake := newFakeLive(t, sessionsPath)
	fake.onStart = func(conn *websocket.Conn, ctx context.Context) bool {
		for _, message := range []string{
			`{"type":"session.started","event_id":"evt_started","session":{"id":"sess_1"}}`,
			`{"type":"session.input_transcript.delta","event_id":"evt_t1","delta":"What is","start_ms":1000,"end_ms":1200}`,
			`{"type":"session.output_transcript.delta","event_id":"evt_t2","delta":"Sure,","start_ms":1100,"end_ms":1300}`,
			`{"type":"session.output_audio.delta","event_id":"evt_a1","delta":"` + base64.StdEncoding.EncodeToString([]byte("provider-audio")) + `"}`,
			`{"type":"session.delegation.created","event_id":"evt_d1","offset_ms":1000,"delegation":{"id":"item_9tA2","type":"delegation","target":"client"}}`,
			`{"type":"session.usage.updated","event_id":"evt_u1","usage":{"seconds":3},"context_window":{"usage_ratio":0.1}}`,
		} {
			if !send(t, conn, ctx, message) {
				return false
			}
		}
		return true
	}
	adapter := newAdapter(t, fake, false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := adapter.Open(ctx, liveRequest(fake.endpoint(), 24_000))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer stream.Close(context.Background())

	var got []runtimepkg.ProviderEvent
	for len(got) < 9 {
		event := nextEvent(t, ctx, stream)
		if event.Err != nil {
			t.Fatalf("stream error: %v", event.Err)
		}
		got = append(got, event)
	}
	types := make([]protocol.EventType, 0, len(got))
	for _, event := range got {
		types = append(types, event.Type)
	}
	want := []protocol.EventType{
		protocol.EventProviderEvent, // session.started
		protocol.EventProviderEvent, protocol.EventTranscriptDelta,
		protocol.EventProviderEvent, protocol.EventTextDelta,
		protocol.EventAudioFrame,
		protocol.EventProviderEvent, // delegation
		protocol.EventProviderEvent, protocol.EventUsageObserved,
	}
	for i := range want {
		if types[i] != want[i] {
			t.Fatalf("event types = %v, want %v", types, want)
		}
	}
	var transcript struct {
		Text    string `json:"text"`
		StartMS int64  `json:"start_ms"`
		EndMS   int64  `json:"end_ms"`
	}
	_ = json.Unmarshal(got[2].Data, &transcript)
	if transcript.Text != "What is" || transcript.StartMS != 1000 || transcript.EndMS != 1200 {
		t.Fatalf("input transcript = %+v", transcript)
	}
	_ = json.Unmarshal(got[4].Data, &transcript)
	if transcript.Text != "Sure," || transcript.StartMS != 1100 || transcript.EndMS != 1300 {
		t.Fatalf("output transcript overlapping the input was altered: %+v", transcript)
	}
	if string(got[5].Audio) != "provider-audio" {
		t.Fatalf("audio = %q", got[5].Audio)
	}
	envelope, payload := decodeProviderEvent(t, got[6])
	if envelope.Type != "session.delegation.created" || payload["delegation"].(map[string]any)["id"] != "item_9tA2" || payload["event_id"] != "evt_d1" {
		t.Fatalf("delegation envelope = %+v %v", envelope, payload)
	}
	if usage := stream.(UsageStream).Usage(); usage.VoiceSeconds != 3 || usage.Final {
		t.Fatalf("usage snapshot = %+v", usage)
	}
	for _, event := range got {
		if event.Type == protocol.EventSpeechStarted || event.Type == protocol.EventSpeechEnded || event.Type == protocol.EventResponseDone || event.Type == protocol.EventResponseStarted {
			t.Fatalf("adapter manufactured a turn boundary: %s", event.Type)
		}
	}
}

func TestNativeModeForwardsAudioDeltasVerbatim(t *testing.T) {
	t.Parallel()
	fake := newFakeLive(t, sessionsPath)
	fake.onStart = func(conn *websocket.Conn, ctx context.Context) bool {
		return send(t, conn, ctx, `{"type":"session.started","event_id":"evt_started","session":{"id":"sess_1"}}`) &&
			send(t, conn, ctx, `{"type":"session.output_audio.delta","event_id":"evt_a1","delta":"AAEC"}`)
	}
	adapter := newAdapter(t, fake, true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := adapter.Open(ctx, liveRequest(fake.endpoint(), 24_000))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer stream.Close(context.Background())
	nextOfType(t, ctx, stream, protocol.EventProviderEvent) // started
	event := nextEvent(t, ctx, stream)
	if event.Type != protocol.EventProviderEvent {
		t.Fatalf("native mode emitted %s, want provider.event", event.Type)
	}
	envelope, payload := decodeProviderEvent(t, event)
	if envelope.Type != "session.output_audio.delta" || payload["delta"] != "AAEC" || payload["event_id"] != "evt_a1" {
		t.Fatalf("audio envelope = %+v %v", envelope, payload)
	}
}

func TestProviderControlsForwardAllowedAndRefuseRealtimeOnly(t *testing.T) {
	t.Parallel()
	fake := newFakeLive(t, sessionsPath)
	fake.onControl = func(conn *websocket.Conn, ctx context.Context, control map[string]any) {
		if control["type"] == "session.commentary.append" {
			send(t, conn, ctx, `{"type":"session.commentary.appended","event_id":"evt_ack","client_event_id":"`+control["event_id"].(string)+`"}`)
		}
	}
	adapter := newAdapter(t, fake, false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := adapter.Open(ctx, liveRequest(fake.endpoint(), 24_000))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer stream.Close(context.Background())
	nextOfType(t, ctx, stream, protocol.EventProviderEvent)
	controls := stream.(runtimepkg.ProviderControlStream)

	result := json.RawMessage(`{"type":"session.commentary.append","event_id":"result_123","delegation_id":"item_9tA2","content":"The order shipped today."}`)
	if err := controls.SendProviderControl(ctx, protocol.ProviderControl{Type: "session.commentary.append", Payload: result}); err != nil {
		t.Fatalf("commentary append: %v", err)
	}
	ack := nextOfType(t, ctx, stream, protocol.EventProviderEvent)
	envelope, payload := decodeProviderEvent(t, ack)
	if envelope.Type != "session.commentary.appended" || payload["client_event_id"] != "result_123" {
		t.Fatalf("ack = %+v %v", envelope, payload)
	}

	for _, tc := range []struct {
		name    string
		control protocol.ProviderControl
	}{
		{"realtime commit", protocol.ProviderControl{Type: "input_audio_buffer.commit", Payload: json.RawMessage(`{"type":"input_audio_buffer.commit"}`)}},
		{"realtime cancel", protocol.ProviderControl{Type: "response.cancel", Payload: json.RawMessage(`{"type":"response.cancel"}`)}},
		{"session start replay", protocol.ProviderControl{Type: "session.start", Payload: json.RawMessage(`{"type":"session.start","session":{}}`)}},
		{"mismatched tag", protocol.ProviderControl{Type: "response.create", Payload: json.RawMessage(`{"type":"response.cancel"}`)}},
		{"model change", protocol.ProviderControl{Type: "session.update", Payload: json.RawMessage(`{"type":"session.update","session":{"model":"gpt-realtime-2.1"}}`)}},
		{"audio change", protocol.ProviderControl{Type: "session.update", Payload: json.RawMessage(`{"type":"session.update","session":{"audio":{"format":{"type":"audio/pcmu","rate":8000}}}}`)}},
		{"delegation type change", protocol.ProviderControl{Type: "session.update", Payload: json.RawMessage(`{"type":"session.update","session":{"delegation":{"type":"client"}}}`)}},
	} {
		if err := controls.SendProviderControl(ctx, tc.control); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
			t.Fatalf("%s: err = %v, want unsupported operation", tc.name, err)
		}
	}
	update := json.RawMessage(`{"type":"session.update","event_id":"upd_1","session":{"delegation":{"responses":{"max_output_tokens":256}}}}`)
	if err := controls.SendProviderControl(ctx, protocol.ProviderControl{Type: "session.update", Payload: update}); err != nil {
		t.Fatalf("backend settings update: %v", err)
	}
	tool := json.RawMessage(`{"type":"response.item.create","event_id":"tool_1","item":{"type":"function_call_output","call_id":"call_1","output":"{}"}}`)
	if err := controls.SendProviderControl(ctx, protocol.ProviderControl{Type: "response.item.create", Payload: tool}); err != nil {
		t.Fatalf("tool result: %v", err)
	}
	if err := controls.SendProviderControl(ctx, protocol.ProviderControl{Type: "response.create", Payload: json.RawMessage(`{"type":"response.create","event_id":"continue_1"}`)}); err != nil {
		t.Fatalf("continuation: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		fake.mu.Lock()
		count := len(fake.controls)
		fake.mu.Unlock()
		if count == 4 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("forwarded controls = %d, want 4", count)
		}
		time.Sleep(10 * time.Millisecond)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.controls[2]["item"].(map[string]any)["call_id"] != "call_1" {
		t.Fatalf("tool result was altered: %v", fake.controls[2])
	}
}

func TestBackendUsageIsDedupedByResponseIDAndToolCallsCounted(t *testing.T) {
	t.Parallel()
	completed := `{"type":"response.event","event_id":"evt_r1","delegation_id":"item_1","event":{"type":"response.completed","response":{"id":"resp_1","status":"completed","output":[{"type":"web_search_call"},{"type":"message"}],"usage":{"input_tokens":100,"output_tokens":40,"input_tokens_details":{"cached_tokens":20},"output_tokens_details":{"reasoning_tokens":8}}}}}`
	fake := newFakeLive(t, sessionsPath)
	fake.onStart = func(conn *websocket.Conn, ctx context.Context) bool {
		return send(t, conn, ctx, `{"type":"session.started","event_id":"evt_started","session":{"id":"sess_1"}}`) &&
			send(t, conn, ctx, `{"type":"response.event","event_id":"evt_r0","delegation_id":"item_1","event":{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call_1","name":"lookup","arguments":"{}"},"response":{"id":"resp_1"}}}`) &&
			send(t, conn, ctx, completed) && send(t, conn, ctx, completed)
	}
	adapter := newAdapter(t, fake, false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := adapter.Open(ctx, liveRequest(fake.endpoint(), 24_000))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer stream.Close(context.Background())
	observed := 0
	envelopes := 0
	for envelopes < 4 {
		event := nextEvent(t, ctx, stream)
		if event.Err != nil {
			t.Fatalf("stream error: %v", event.Err)
		}
		switch event.Type {
		case protocol.EventProviderEvent:
			envelopes++
			envelope, payload := decodeProviderEvent(t, event)
			if envelope.Type == "response.event" {
				nested := payload["event"].(map[string]any)
				if payload["delegation_id"] != "item_1" || nested["type"] == nil {
					t.Fatalf("nested response event lost its identifiers: %v", payload)
				}
			}
		case protocol.EventUsageObserved:
			observed++
			var data map[string]string
			_ = json.Unmarshal(event.Data, &data)
			if data["provider_request_id"] != "resp_1" {
				t.Fatalf("usage data = %v", data)
			}
		}
	}
	if observed != 1 {
		t.Fatalf("usage observed %d times for one response id, want 1", observed)
	}
	usage := stream.(UsageStream).Usage()
	backend := usage.Backend["resp_1"]
	if backend.InputTokens != 100 || backend.CachedInputTokens != 20 || backend.OutputTokens != 40 || backend.ReasoningTokens != 8 || backend.ToolCalls != 1 {
		t.Fatalf("backend usage = %+v", backend)
	}
}

func TestGracefulCloseWaitsForSessionClosedAndKeepsFinalUsage(t *testing.T) {
	t.Parallel()
	fake := newFakeLive(t, sessionsPath)
	fake.onStart = func(conn *websocket.Conn, ctx context.Context) bool {
		return send(t, conn, ctx, `{"type":"session.started","event_id":"evt_started","session":{"id":"sess_1"}}`) &&
			send(t, conn, ctx, `{"type":"session.usage.updated","event_id":"evt_u1","usage":{"seconds":40}}`) &&
			send(t, conn, ctx, `{"type":"session.usage.updated","event_id":"evt_u2","usage":{"seconds":9}}`)
	}
	adapter := newAdapter(t, fake, false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := adapter.Open(ctx, liveRequest(fake.endpoint(), 24_000))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	nextOfType(t, ctx, stream, protocol.EventUsageObserved)
	nextOfType(t, ctx, stream, protocol.EventUsageObserved)
	if usage := stream.(UsageStream).Usage(); usage.VoiceSeconds != 40 {
		t.Fatalf("a reordered smaller snapshot lowered the account: %+v", usage)
	}
	done := make(chan error, 1)
	go func() { done <- stream.Close(ctx) }()
	closed := nextOfType(t, ctx, stream, protocol.EventProviderEvent)
	envelope, payload := decodeProviderEvent(t, closed)
	if envelope.Type != "session.closed" || payload["reason"] != "close_requested" {
		t.Fatalf("closed envelope = %+v %v", envelope, payload)
	}
	nextOfType(t, ctx, stream, protocol.EventUsageObserved)
	if err := <-done; err != nil {
		t.Fatalf("Close: %v", err)
	}
	for range stream.Events() {
	}
	if err := stream.(runtimepkg.TerminalErrorProviderStream).TerminalError(); err != nil {
		t.Fatalf("clean close recorded a terminal error: %v", err)
	}
	usage := stream.(UsageStream).Usage()
	if !usage.Final || usage.VoiceSeconds != 12.5 || usage.CloseReason != "close_requested" {
		t.Fatalf("final usage = %+v", usage)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.closes != 1 {
		t.Fatalf("session.close sent %d times", fake.closes)
	}
}

func TestCloseTimeoutReportsIncompleteFinalization(t *testing.T) {
	t.Parallel()
	fake := newFakeLive(t, sessionsPath)
	fake.skipClosed = true
	adapter := newAdapter(t, fake, false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := adapter.Open(ctx, liveRequest(fake.endpoint(), 24_000))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	started := time.Now()
	_ = stream.Close(ctx)
	if elapsed := time.Since(started); elapsed < 900*time.Millisecond {
		t.Fatalf("close returned after %s without draining", elapsed)
	}
	for range stream.Events() {
	}
	var providerErr *runtimepkg.ProviderError
	if err := stream.(runtimepkg.TerminalErrorProviderStream).TerminalError(); !errors.As(err, &providerErr) || providerErr.Code != "finalization_incomplete" {
		t.Fatalf("terminal error = %v, want finalization_incomplete", err)
	}
	if usage := stream.(UsageStream).Usage(); usage.Final {
		t.Fatal("usage was marked final without session.closed")
	}
}

func TestDisconnectWithoutSessionClosedIsNotConfirmedCompletion(t *testing.T) {
	t.Parallel()
	fake := newFakeLive(t, sessionsPath)
	fake.onStart = func(conn *websocket.Conn, ctx context.Context) bool {
		send(t, conn, ctx, `{"type":"session.started","event_id":"evt_started","session":{"id":"sess_1"}}`)
		_ = conn.Close(websocket.StatusNormalClosure, "bye")
		return false
	}
	adapter := newAdapter(t, fake, false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := adapter.Open(ctx, liveRequest(fake.endpoint(), 24_000))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	var terminal error
	for event := range stream.Events() {
		if event.Err != nil {
			terminal = event.Err
		}
	}
	var providerErr *runtimepkg.ProviderError
	if !errors.As(terminal, &providerErr) || providerErr.Code != "provider_unavailable" {
		t.Fatalf("transport disconnect surfaced as %v, want provider_unavailable", terminal)
	}
}

func TestStartupFailuresAreClassified(t *testing.T) {
	t.Parallel()
	fake := newFakeLive(t, sessionsPath)
	fake.onStart = func(conn *websocket.Conn, ctx context.Context) bool {
		send(t, conn, ctx, `{"type":"error","event_id":"evt_err","error":{"type":"invalid_request_error","code":"invalid_model","message":"unknown model","client_event_id":"speko_session_start"}}`)
		return true
	}
	adapter := newAdapter(t, fake, false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := adapter.Open(ctx, liveRequest(fake.endpoint(), 24_000))
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "provider_rejected_request" {
		t.Fatalf("startup error = %v, want provider_rejected_request", err)
	}

	silent := newFakeLive(t, sessionsPath)
	silent.onStart = func(conn *websocket.Conn, ctx context.Context) bool { <-ctx.Done(); return false }
	quick, err := New(Config{AllowedEndpointHosts: []string{silent.host()}, AllowInsecureEndpoint: true, SetupTimeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, err = quick.Open(ctx, liveRequest(silent.endpoint(), 24_000))
	if !errors.As(err, &providerErr) || providerErr.Code != "provider_unavailable" || !providerErr.Retryable {
		t.Fatalf("setup timeout = %v, want retryable provider_unavailable", err)
	}
}

func TestRejectsIncompatibleMediaAndEndpoints(t *testing.T) {
	t.Parallel()
	adapter, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	request := liveRequest("wss://api.openai.com/v1/live/sessions", 8_000)
	if _, err := adapter.Open(ctx, request); err == nil || !strings.Contains(err.Error(), "16 or 24 kHz") {
		t.Fatalf("8 kHz accepted: %v", err)
	}
	request = liveRequest("wss://api.openai.com/v1/live/sessions", 24_000)
	request.Options.S2S.OutputMedia.SampleRateHz = 16_000
	if _, err := adapter.Open(ctx, request); err == nil || !strings.Contains(err.Error(), "both directions") {
		t.Fatalf("mismatched rates accepted: %v", err)
	}
	request = liveRequest("wss://api.openai.com/v1/live/sessions?model=gpt-live-1", 24_000)
	if _, err := adapter.Open(ctx, request); err == nil || !strings.Contains(err.Error(), "query") {
		t.Fatalf("query endpoint accepted: %v", err)
	}
	request = liveRequest("wss://api.openai.com/v1/realtime", 24_000)
	if _, err := adapter.Open(ctx, request); err == nil || !strings.Contains(err.Error(), sessionsPath) {
		t.Fatalf("realtime path accepted: %v", err)
	}
	request = liveRequest("wss://router.speko.dev/v1/live", 24_000)
	if _, err := adapter.Open(ctx, request); err == nil || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("unconfigured router host accepted: %v", err)
	}
	request = liveRequest("wss://api.openai.com/v1/live/sessions", 24_000)
	request.Plan.Route.Credential.Kind = protocol.CredentialRelayAccess
	if _, err := adapter.Open(ctx, request); err == nil || !strings.Contains(err.Error(), "bearer") {
		t.Fatalf("relay_access accepted at the vendor: %v", err)
	}
	request = liveRequest("wss://api.openai.com/v1/live/sessions", 24_000)
	request.Plan.Route.Model = "gpt-realtime-2.1"
	request.Kind = protocol.SessionKindTTS
	if _, err := adapter.Open(ctx, request); err == nil {
		t.Fatal("tts kind accepted")
	}
}

func TestManagedSessionsDialTheRouterOnASpekoRelayRoute(t *testing.T) {
	t.Parallel()
	router := newFakeLive(t, relayPath)
	adapter, err := New(Config{RelayEndpointHosts: []string{router.host()}, AllowInsecureEndpoint: true, SetupTimeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request := liveRequest(router.endpoint(), 24_000)
	request.Plan.Execution = protocol.Execution{ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged}
	request.Plan.Route.Credential = &protocol.DelegatedCredential{Kind: protocol.CredentialRelayAccess, Value: "sra_relay_access", ExpiresAt: time.Now().Add(time.Minute)}
	stream, err := adapter.Open(ctx, request)
	if err != nil {
		t.Fatalf("Open via router: %v", err)
	}
	defer stream.Close(context.Background())
	router.mu.Lock()
	defer router.mu.Unlock()
	if got := router.headers.Get("Authorization"); got != "Bearer sra_relay_access" {
		t.Fatalf("authorization = %q", got)
	}
	if got := router.headers.Get("Idempotency-Key"); got != "att_live_1" {
		t.Fatalf("idempotency key = %q", got)
	}
	if router.start["model"] != "gpt-live-1" {
		t.Fatalf("router received session = %#v", router.start)
	}

	// The same Router endpoint is refused on a provider-direct plan: a
	// customer key must never be sent to the Router as a vendor credential.
	direct := liveRequest(router.endpoint(), 24_000)
	if _, err := adapter.Open(ctx, direct); err == nil || !strings.Contains(err.Error(), "speko_relay") {
		t.Fatalf("router accepted on provider-direct: %v", err)
	}
}
