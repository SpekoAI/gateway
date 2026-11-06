package omi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

const testKey = "omi_sk_test_9f3c2a"

// liveReady follows the ready the live socket sent on 2026-11-06.
const liveReady = `{"type":"ready","session_id":"%s","model":{"requested":"omi-medical-1","served":"%s"},"max_session_s":1800.0,"language":"en","client_controls":["finalize","flush_request","update_configuration","keep_alive","stop"],"billing":{"usd_per_audio_hour":0.45},"tiers":{"draft":{"source":"qwen_bounded_preview"}}}`

// recordFrame follows the canonical_record the live socket sent after stop.
// Its transcripts are a repaired text that must never be forwarded.
const defaultRecord = `{"type":"canonical_record","confirmed_transcript":"repaired confirmed words","final_transcript":"repaired final words","record":{"status":"complete","unresolved_intervals":[]},"session_stats":{"audio_ms":2000,"utterances":1,"ended_reason":"client_stop"}}`

// fakeStream stands in for wss://api.us.omi.health/v1/stream: it reads the
// start frame, sends ready, and replays scripted frames on the first audio,
// on finalize and on stop.
type fakeStream struct {
	t *testing.T
	// open replaces ready when not nil.
	open []string
	// served is the model ready reports; the requested one when empty.
	served    string
	openDelay time.Duration
	// closeAfterOpen closes the socket with this status after open.
	closeAfterOpen websocket.StatusCode
	closeReason    string
	onAudio        []string
	// closeAfterAudio closes with this status after onAudio.
	closeAfterAudio websocket.StatusCode
	// onFinalize is written after each finalize; %s is the finalize id.
	onFinalize []string
	// onStop precedes record after stop. ignoreStop sends nothing;
	// closeOnStop closes normally without a record.
	onStop      []string
	record      string
	ignoreStop  bool
	closeOnStop bool
	server      *httptest.Server
	connections atomic.Int32

	mu       sync.Mutex
	header   http.Header
	start    map[string]any
	startRaw string
	sequence []string
	frames   [][]byte
	controls []string
}

func newFakeStream(t *testing.T) *fakeStream {
	fake := &fakeStream{t: t, record: defaultRecord}
	mux := http.NewServeMux()
	mux.HandleFunc(streamPath, fake.handle)
	fake.server = httptest.NewServer(mux)
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeStream) endpoint() string {
	return "ws" + strings.TrimPrefix(f.server.URL, "http") + streamPath
}

func (f *fakeStream) host() string {
	return strings.Split(strings.TrimPrefix(f.server.URL, "http://"), ":")[0]
}

func (f *fakeStream) log(entry string) {
	f.mu.Lock()
	f.sequence = append(f.sequence, entry)
	f.mu.Unlock()
}

func (f *fakeStream) handle(writer http.ResponseWriter, request *http.Request) {
	f.connections.Add(1)
	f.mu.Lock()
	f.header = request.Header.Clone()
	f.mu.Unlock()
	conn, err := websocket.Accept(writer, request, nil)
	if err != nil {
		return
	}
	defer conn.CloseNow()
	ctx := request.Context()
	messageType, payload, err := conn.Read(ctx)
	if err != nil {
		return
	}
	if messageType != websocket.MessageText {
		f.t.Errorf("first client frame is binary, want the start frame")
		return
	}
	var start map[string]any
	if err := json.Unmarshal(payload, &start); err != nil {
		f.t.Errorf("start frame %s: %v", payload, err)
		return
	}
	f.mu.Lock()
	f.start, f.startRaw = start, string(payload)
	f.mu.Unlock()
	f.log("start")
	time.Sleep(f.openDelay)
	open := f.open
	if open == nil {
		served := f.served
		if served == "" {
			served, _ = start["model"].(string)
		}
		open = []string{fmt.Sprintf(liveReady, start["session_id"], served)}
	}
	f.write(ctx, conn, open)
	f.log("ready")
	if f.closeAfterOpen != 0 {
		_ = conn.Close(f.closeAfterOpen, f.closeReason)
		return
	}
	heard := false
	for {
		messageType, payload, err := conn.Read(ctx)
		if err != nil {
			return
		}
		if messageType == websocket.MessageBinary {
			f.mu.Lock()
			f.frames = append(f.frames, payload)
			f.mu.Unlock()
			f.log("audio")
			if !heard {
				heard = true
				f.write(ctx, conn, f.onAudio)
				if f.closeAfterAudio != 0 {
					_ = conn.Close(f.closeAfterAudio, f.closeReason)
					return
				}
			}
			continue
		}
		f.mu.Lock()
		f.controls = append(f.controls, string(payload))
		f.mu.Unlock()
		var control struct {
			Type string `json:"type"`
			ID   string `json:"id"`
		}
		_ = json.Unmarshal(payload, &control)
		f.log(control.Type)
		switch control.Type {
		case "finalize":
			for _, frame := range f.onFinalize {
				f.write(ctx, conn, []string{strings.ReplaceAll(frame, "%s", control.ID)})
			}
		case "keep_alive":
			f.write(ctx, conn, []string{`{"type":"keep_alive_ack"}`})
		case "stop":
			if f.ignoreStop {
				continue
			}
			f.write(ctx, conn, f.onStop)
			if f.closeOnStop {
				_ = conn.Close(websocket.StatusNormalClosure, "")
				return
			}
			f.write(ctx, conn, []string{`{"type":"confirmed_complete"}`, `{"type":"finalizing"}`, `{"type":"canonical_revision"}`, f.record})
			// The client closes once it has read the record.
		default:
			f.t.Errorf("unexpected client text frame: %s", payload)
		}
	}
}

func (f *fakeStream) write(ctx context.Context, conn *websocket.Conn, messages []string) {
	for _, message := range messages {
		if conn.Write(ctx, websocket.MessageText, []byte(message)) != nil {
			return
		}
	}
}

func (f *fakeStream) snapshot() (http.Header, map[string]any, string, []string, [][]byte, []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.header, f.start, f.startRaw, append([]string(nil), f.sequence...), append([][]byte(nil), f.frames...), append([]string(nil), f.controls...)
}

func sttRequest(endpoint string) runtimepkg.AdapterRequest {
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindSTT,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged},
			Route: protocol.PlanRoute{Provider: ProviderName, Model: Model, Adapter: STTAdapterID, Transport: protocol.TransportWebSocket, Endpoint: endpoint,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialRelayAccess, Value: testKey, ExpiresAt: time.Now().Add(time.Minute)}},
		},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
		Options: protocol.RequestOptions{Language: "en-US"},
	}
}

func newTestSTT(t *testing.T, fake *fakeStream, config STTConfig) *STTAdapter {
	t.Helper()
	config.AllowedEndpointHosts = append(config.AllowedEndpointHosts, fake.host())
	config.AllowInsecureEndpoint = true
	if config.SetupTimeout == 0 {
		config.SetupTimeout = 2 * time.Second
	}
	if config.CloseDrainTimeout == 0 {
		config.CloseDrainTimeout = 2 * time.Second
	}
	adapter, err := NewSTT(config)
	if err != nil {
		t.Fatalf("NewSTT: %v", err)
	}
	return adapter
}

func openSTT(t *testing.T, fake *fakeStream, config STTConfig, request runtimepkg.AdapterRequest) runtimepkg.ProviderStream {
	t.Helper()
	stream, err := newTestSTT(t, fake, config).Open(context.Background(), request)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) })
	return stream
}

// drainSTT collects every event until the stream closes Events.
func drainSTT(t *testing.T, stream runtimepkg.ProviderStream) []runtimepkg.ProviderEvent {
	t.Helper()
	var events []runtimepkg.ProviderEvent
	timeout := time.After(5 * time.Second)
	for {
		select {
		case event, ok := <-stream.Events():
			if !ok {
				return events
			}
			events = append(events, event)
		case <-timeout:
			t.Fatalf("events did not close; got %s", eventTypes(events))
		}
	}
}

func nextSTTEvent(t *testing.T, stream runtimepkg.ProviderStream) runtimepkg.ProviderEvent {
	t.Helper()
	select {
	case event := <-stream.Events():
		return event
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for an event")
		return runtimepkg.ProviderEvent{}
	}
}

// skipOpening reads past session.ready and the opening usage.
func skipOpening(t *testing.T, stream runtimepkg.ProviderStream) {
	t.Helper()
	if event := nextSTTEvent(t, stream); event.Type != protocol.EventSessionReady {
		t.Fatalf("first event = %s, want session.ready", event.Type)
	}
	if event := nextSTTEvent(t, stream); event.Type != protocol.EventUsageObserved {
		t.Fatalf("second event = %s, want the opening usage", event.Type)
	}
}

func dataOf(t *testing.T, event runtimepkg.ProviderEvent) map[string]any {
	t.Helper()
	var data map[string]any
	if err := json.Unmarshal(event.Data, &data); err != nil {
		t.Fatalf("event %s data %s: %v", event.Type, event.Data, err)
	}
	return data
}

func eventTypes(events []runtimepkg.ProviderEvent) string {
	var types []string
	for _, event := range events {
		if event.Err != nil {
			types = append(types, "error")
			continue
		}
		types = append(types, string(event.Type))
	}
	return strings.Join(types, ",")
}

func terminalFailure(t *testing.T, events []runtimepkg.ProviderEvent) *runtimepkg.ProviderError {
	t.Helper()
	var failures []error
	for _, event := range events {
		if event.Err != nil {
			failures = append(failures, event.Err)
		}
	}
	var providerErr *runtimepkg.ProviderError
	if len(failures) != 1 || !errors.As(failures[0], &providerErr) {
		t.Fatalf("failures = %v, want exactly one provider error", failures)
	}
	return providerErr
}

// lastUsage returns the last usage observation and validates it.
func lastUsage(t *testing.T, events []runtimepkg.ProviderEvent) runtimepkg.ProviderEvent {
	t.Helper()
	for index := len(events) - 1; index >= 0; index-- {
		if events[index].Type == protocol.EventUsageObserved && events[index].Billing != nil {
			if err := events[index].Billing.Validate(); err != nil {
				t.Fatalf("billing invalid: %v (%+v)", err, events[index].Billing)
			}
			return events[index]
		}
	}
	t.Fatalf("no usage in %s", eventTypes(events))
	return runtimepkg.ProviderEvent{}
}

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestOpenSendsTheStartFrameAndWaitsForReady(t *testing.T) {
	t.Parallel()
	fake := newFakeStream(t)
	fake.openDelay = 150 * time.Millisecond
	started := time.Now()
	stream := openSTT(t, fake, STTConfig{}, sttRequest(fake.endpoint()))
	if elapsed := time.Since(started); elapsed < 150*time.Millisecond {
		t.Fatalf("Open returned after %s, before ready", elapsed)
	}
	header, start, _, _, _, _ := fake.snapshot()
	if header.Get("Authorization") != "" || header.Get("Api-Key") != "" {
		t.Fatalf("the key rode the handshake: %v", header)
	}
	sessionID, _ := start["session_id"].(string)
	if !uuidPattern.MatchString(sessionID) {
		t.Fatalf("session_id = %q, want a v4 UUID", sessionID)
	}
	want := map[string]any{
		"type": "start", "api_key": testKey, "session_id": sessionID, "model": Model,
		"streaming_mode": "realtime", "event_schema": "omi-agent-v1", "audio_format": "pcm_s16le",
		"sample_rate_hz": float64(16000), "channels": float64(1), "language": "en",
		"diarization": map[string]any{"enabled": false},
	}
	if got, wanted := marshalData(start), marshalData(want); string(got) != string(wanted) {
		t.Fatalf("start frame = %s, want %s", got, wanted)
	}
	ready := nextSTTEvent(t, stream)
	data := dataOf(t, ready)
	if ready.Type != protocol.EventSessionReady || data["provider_model"] != Model || data["provider_request_id"] != sessionID || data["max_session_s"] != float64(1800) {
		t.Fatalf("first event = %s %s, want session.ready", ready.Type, ready.Data)
	}
	usage := nextSTTEvent(t, stream)
	if usage.Type != protocol.EventUsageObserved || usage.Billing == nil || usage.Billing.Complete || usage.Billing.Model != Model ||
		usage.Billing.Mode != "streaming" || usage.Billing.Language != "" || usage.Billing.ProviderRequestID != sessionID {
		t.Fatalf("opening usage = %s %+v", usage.Type, usage.Billing)
	}
	if err := usage.Billing.Validate(); err != nil {
		t.Fatalf("opening billing invalid: %v", err)
	}
}

func TestDefaultSetupTimeoutIsTwelveSeconds(t *testing.T) {
	t.Parallel()
	adapter, err := NewSTT(STTConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if adapter.setupTimeout != 12*time.Second || adapter.ID() != STTAdapterID {
		t.Fatalf("setup timeout = %s, id = %s", adapter.setupTimeout, adapter.ID())
	}
}

func TestSilenceMSRidesTheStartFrame(t *testing.T) {
	t.Parallel()
	fake := newFakeStream(t)
	request := sttRequest(fake.endpoint())
	request.Options.STT = &protocol.SttOptions{ProviderOptions: map[string]map[string]any{ProviderName: {"silence_ms": float64(600)}}}
	openSTT(t, fake, STTConfig{}, request)
	_, start, _, _, _, _ := fake.snapshot()
	if endpointing, _ := start["endpointing"].(map[string]any); endpointing["silence_ms"] != float64(600) {
		t.Fatalf("endpointing = %v, want silence_ms 600", start["endpointing"])
	}
}

func TestSilenceMSOutsideItsBoundsIsRefusedBeforeDialing(t *testing.T) {
	t.Parallel()
	for _, value := range []any{float64(150), float64(1200), 600.5, "600", true} {
		fake := newFakeStream(t)
		request := sttRequest(fake.endpoint())
		request.Options.STT = &protocol.SttOptions{ProviderOptions: map[string]map[string]any{ProviderName: {"silence_ms": value}}}
		_, err := newTestSTT(t, fake, STTConfig{}).Open(context.Background(), request)
		var providerErr *runtimepkg.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != "invalid_request" {
			t.Errorf("silence_ms %v: Open error = %v, want invalid_request", value, err)
		}
		if fake.connections.Load() != 0 {
			t.Errorf("silence_ms %v dialed the socket", value)
		}
	}
}

func TestNoAudioReachesTheSocketBeforeReady(t *testing.T) {
	t.Parallel()
	fake := newFakeStream(t)
	fake.openDelay = 100 * time.Millisecond
	stream := openSTT(t, fake, STTConfig{}, sttRequest(fake.endpoint()))
	if err := stream.WriteAudio(context.Background(), make([]byte, 3200)); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	drainSTT(t, stream)
	_, _, _, sequence, _, _ := fake.snapshot()
	if len(sequence) < 3 || sequence[0] != "start" || sequence[1] != "ready" || sequence[2] != "audio" {
		t.Fatalf("socket sequence = %v, want start, ready, then audio", sequence)
	}
}

func TestOpenFailsWhenOmiServesAnotherModel(t *testing.T) {
	t.Parallel()
	fake := newFakeStream(t)
	fake.served = "omi-general-1"
	_, err := newTestSTT(t, fake, STTConfig{}).Open(context.Background(), sttRequest(fake.endpoint()))
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "provider_unavailable" || providerErr.Retryable ||
		!strings.Contains(providerErr.Message, "omi-general-1") || !strings.Contains(providerErr.Message, Model) {
		t.Fatalf("Open error = %#v, want a served-model mismatch", err)
	}
}

func TestOpenTimesOutWithoutReady(t *testing.T) {
	t.Parallel()
	fake := newFakeStream(t)
	fake.open = []string{}
	_, err := newTestSTT(t, fake, STTConfig{SetupTimeout: 100 * time.Millisecond}).Open(context.Background(), sttRequest(fake.endpoint()))
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "provider_unavailable" || !providerErr.Retryable {
		t.Fatalf("Open error = %#v, want a retryable missing ready", err)
	}
}

func TestPartialsReplaceEachOtherAndTheFinalEndsTheTurn(t *testing.T) {
	t.Parallel()
	fake := newFakeStream(t)
	fake.onAudio = []string{
		`{"type":"turn_start","turn_id":"t1"}`,
		`{"type":"partial","tier":"draft","text":"chest","mutable":true,"provisional":true,"turn_id":"t1","start_s":0.4,"end_s":0.8,"words":[]}`,
		`{"type":"partial","tier":"draft","text":"chest pain","mutable":true,"provisional":true,"turn_id":"t1","start_s":0.4,"end_s":1.1,"words":[]}`,
		`{"type":"partial","tier":"draft","text":"chest pain","mutable":true,"provisional":true,"turn_id":"t1","start_s":0.4,"end_s":1.2,"words":[]}`,
		`{"type":"turn_end_candidate","turn_id":"t1"}`,
		`{"type":"turn_final","tier":"confirmed","text":"Chest pain since Monday.","turn_id":"t1","start_s":0.4,"end_s":1.9,"turn_complete":true,"latency_ms":51,"speaker_span_id":"s0"}`,
		`{"type":"partial","tier":"draft","text":"chest pain since","turn_id":"t1","start_s":0.4,"end_s":1.9,"words":[]}`,
		`{"type":"turn_final","tier":"confirmed","text":"Chest pain since Monday.","turn_id":"t1","start_s":0.4,"end_s":1.9,"turn_complete":true}`,
		`{"type":"notice","code":"UTTERANCE_DROPPED"}`,
		`{"type":"partial","tier":"draft","text":"no fever","turn_id":7,"start_s":2.5,"end_s":3.0,"words":[]}`,
		`{"type":"turn_final","tier":"confirmed","text":"No fever.","turn_id":7,"start_s":2.5,"end_s":3.2,"turn_complete":true}`,
	}
	stream := openSTT(t, fake, STTConfig{}, sttRequest(fake.endpoint()))
	skipOpening(t, stream)
	if err := stream.WriteAudio(context.Background(), make([]byte, 64_000)); err != nil {
		t.Fatal(err)
	}
	var events []runtimepkg.ProviderEvent
	ended := 0
	for ended < 2 {
		event := nextSTTEvent(t, stream)
		events = append(events, event)
		if event.Type == protocol.EventSpeechEnded {
			ended++
		}
	}
	want := "speech.started,transcript.delta,transcript.delta,transcript.final,speech.ended,speech.started,transcript.delta,transcript.final,speech.ended"
	if got := eventTypes(events); got != want {
		t.Fatalf("events = %s, want %s", got, want)
	}
	texts := []string{"", "chest", "chest pain", "Chest pain since Monday.", "", "", "no fever", "No fever."}
	for index, text := range texts {
		if text == "" {
			continue
		}
		if data := dataOf(t, events[index]); data["text"] != text {
			t.Errorf("event %d text = %v, want %q", index, data["text"], text)
		}
	}
	final := dataOf(t, events[3])
	if final["is_final"] != true || final["turn_id"] != float64(0) || final["provider_turn_id"] != "t1" || final["audio_start_ms"] != float64(400) || final["audio_end_ms"] != float64(1900) || final["language"] != "en" {
		t.Fatalf("first final = %v", final)
	}
	if second := dataOf(t, events[7]); second["turn_id"] != float64(1) || second["provider_turn_id"] != "7" {
		t.Fatalf("second final = %v", second)
	}
	if ended := dataOf(t, events[4]); ended["reason"] != "end_of_turn" || ended["audio_end_ms"] != float64(1900) {
		t.Fatalf("speech.ended = %v", ended)
	}
}

func TestAFinalWithoutPartialsSupersedesAnAbandonedTurn(t *testing.T) {
	t.Parallel()
	fake := newFakeStream(t)
	fake.onAudio = []string{
		`{"type":"partial","text":"dropped words","turn_id":"t1","start_s":0.1,"end_s":0.5,"words":[]}`,
		`{"type":"notice","code":"UTTERANCE_DROPPED"}`,
		`{"type":"turn_final","text":"Kept words.","turn_id":"t2","start_s":1.0,"end_s":1.6,"turn_complete":true}`,
		`{"type":"turn_final","text":"","turn_id":"t3","turn_complete":true}`,
	}
	stream := openSTT(t, fake, STTConfig{}, sttRequest(fake.endpoint()))
	skipOpening(t, stream)
	if err := stream.WriteAudio(context.Background(), make([]byte, 3200)); err != nil {
		t.Fatal(err)
	}
	var events []runtimepkg.ProviderEvent
	finals := 0
	for finals < 2 {
		event := nextSTTEvent(t, stream)
		events = append(events, event)
		if event.Type == protocol.EventTranscriptFinal {
			finals++
		}
	}
	want := "speech.started,transcript.delta,speech.ended,speech.started,transcript.final,speech.ended,transcript.final"
	if got := eventTypes(events); got != want {
		t.Fatalf("events = %s, want %s", got, want)
	}
	if superseded := dataOf(t, events[2]); superseded["reason"] != "superseded" || superseded["provider_turn_id"] != "t1" {
		t.Fatalf("abandoned turn ended with %v", superseded)
	}
	if empty := dataOf(t, events[6]); empty["text"] != "" || empty["is_final"] != true {
		t.Fatalf("empty final = %v", empty)
	}
}

func TestCommitAudioSendsFinalizeWithIncrementingIDsAndKeepsTheSession(t *testing.T) {
	t.Parallel()
	fake := newFakeStream(t)
	fake.onFinalize = []string{
		`{"type":"turn_final","tier":"confirmed","text":"Flushed %s.","turn_id":"f%s","start_s":0.0,"end_s":0.2,"turn_complete":true,"latency_ms":50}`,
		`{"type":"finalize_ack","id":"%s","turn_id":"f%s","flushed":true}`,
	}
	stream := openSTT(t, fake, STTConfig{}, sttRequest(fake.endpoint()))
	skipOpening(t, stream)
	ctx := context.Background()
	for round := 1; round <= 2; round++ {
		if err := stream.WriteAudio(ctx, make([]byte, 3200)); err != nil {
			t.Fatal(err)
		}
		if err := stream.CommitAudio(ctx); err != nil {
			t.Fatal(err)
		}
		var final runtimepkg.ProviderEvent
		for final.Type != protocol.EventTranscriptFinal {
			final = nextSTTEvent(t, stream)
		}
		if data := dataOf(t, final); data["text"] != fmt.Sprintf("Flushed %d.", round) {
			t.Fatalf("round %d final = %v", round, data)
		}
	}
	_, _, _, _, _, controls := fake.snapshot()
	if want := []string{`{"id":"1","type":"finalize"}`, `{"id":"2","type":"finalize"}`}; strings.Join(controls, " ") != strings.Join(want, " ") {
		t.Fatalf("controls = %v, want %v", controls, want)
	}
}

func TestCloseSendsStopAndBillsTheRecordsAudioMS(t *testing.T) {
	t.Parallel()
	fake := newFakeStream(t)
	// Omi's audio_ms is the metered quantity even where it differs from the
	// bytes the relay counted.
	fake.record = strings.Replace(defaultRecord, `"audio_ms":2000`, `"audio_ms":1990`, 1)
	stream := openSTT(t, fake, STTConfig{}, sttRequest(fake.endpoint()))
	if err := stream.WriteAudio(context.Background(), make([]byte, 64_000)); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	events := drainSTT(t, stream)
	for _, event := range events {
		if event.Err != nil {
			t.Fatalf("clean close reported %v", event.Err)
		}
		if event.Type == protocol.EventWarning {
			t.Fatalf("clean close warned: %s", event.Data)
		}
		for _, raw := range append([][]byte{event.Data}, extensionsOf(event)...) {
			if strings.Contains(string(raw), "repaired") {
				t.Fatalf("canonical_record text leaked into %s: %s", event.Type, raw)
			}
		}
	}
	usage := lastUsage(t, events)
	billing := usage.Billing
	_, start, _, _, _, controls := fake.snapshot()
	if !billing.Complete || billing.Quantities["duration_seconds"] != 1990 || billing.QuantityDenominator("duration_seconds") != 1000 ||
		billing.Mode != "streaming" || billing.Model != Model || billing.Language != "" || billing.ProviderRequestID != start["session_id"] {
		t.Fatalf("closing billing = %+v", billing)
	}
	if data := dataOf(t, usage); data["audio_ms"] != float64(1990) || data["audio_bytes"] != float64(64_000) {
		t.Fatalf("closing usage data = %v", data)
	}
	if len(controls) != 1 || controls[0] != `{"type":"stop"}` {
		t.Fatalf("controls = %v, want one stop", controls)
	}
	if err := stream.WriteAudio(context.Background(), make([]byte, 2)); !errors.Is(err, runtimepkg.ErrSessionClosed) {
		t.Fatalf("WriteAudio after Close = %v", err)
	}
}

func extensionsOf(event runtimepkg.ProviderEvent) [][]byte {
	var raws [][]byte
	for _, raw := range event.Extensions {
		raws = append(raws, raw)
	}
	return raws
}

func TestARecordThatIsNotCompleteIsAWarningNotAFailure(t *testing.T) {
	t.Parallel()
	for _, status := range []string{"incomplete", "empty"} {
		fake := newFakeStream(t)
		fake.record = `{"type":"canonical_record","confirmed_transcript":"","final_transcript":"","record":{"status":"` + status + `","empty_reason":"no_speech","unresolved_intervals":[[0,1]]},"session_stats":{"audio_ms":1000,"utterances":0}}`
		stream := openSTT(t, fake, STTConfig{}, sttRequest(fake.endpoint()))
		if err := stream.WriteAudio(context.Background(), make([]byte, 32_000)); err != nil {
			t.Fatal(err)
		}
		if err := stream.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		events := drainSTT(t, stream)
		var warned bool
		for _, event := range events {
			if event.Err != nil {
				t.Fatalf("%s record failed the session: %v", status, event.Err)
			}
			if event.Type == protocol.EventWarning {
				data := dataOf(t, event)
				warned = data["record_status"] == status && data["empty_reason"] == "no_speech"
			}
		}
		if billing := lastUsage(t, events).Billing; !warned || !billing.Complete || billing.Quantities["duration_seconds"] != 1000 {
			t.Fatalf("%s record: warned=%v billing=%+v", status, warned, billing)
		}
	}
}

func TestARecordWithoutAudioMSLeavesTheBytesAsAnIncompleteEstimate(t *testing.T) {
	t.Parallel()
	fake := newFakeStream(t)
	fake.record = `{"type":"canonical_record","record":{"status":"complete"},"session_stats":{"utterances":1}}`
	stream := openSTT(t, fake, STTConfig{}, sttRequest(fake.endpoint()))
	if err := stream.WriteAudio(context.Background(), make([]byte, 48_000)); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	events := drainSTT(t, stream)
	usage := lastUsage(t, events)
	if billing := usage.Billing; billing.Complete || billing.Quantities["duration_seconds"] != 48_000 || billing.QuantityDenominator("duration_seconds") != 32_000 {
		t.Fatalf("billing = %+v, want 1.5 s incomplete", billing)
	}
	if dataOf(t, usage)["billing_evidence"] != "pcm_bytes_sent" {
		t.Fatalf("usage data = %s", usage.Data)
	}
}

func TestCloseDrainTimeoutBillsTheBytesSentAsAnIncompleteEstimate(t *testing.T) {
	t.Parallel()
	fake := newFakeStream(t)
	fake.ignoreStop = true
	stream := openSTT(t, fake, STTConfig{CloseDrainTimeout: 150 * time.Millisecond}, sttRequest(fake.endpoint()))
	if err := stream.WriteAudio(context.Background(), make([]byte, 16_001)); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	events := drainSTT(t, stream)
	if failure := terminalFailure(t, events); failure.Code != "request_timeout" || !strings.Contains(failure.Message, "canonical_record") {
		t.Fatalf("failure = %+v", failure)
	}
	// The odd byte is never sent, so it is never counted.
	if billing := lastUsage(t, events).Billing; billing.Complete || billing.Quantities["duration_seconds"] != 16_000 || billing.QuantityDenominator("duration_seconds") != 32_000 {
		t.Fatalf("billing = %+v, want 0.5 s incomplete", billing)
	}
}

func TestNormalCloseAfterStopWithoutARecordIsAFailure(t *testing.T) {
	t.Parallel()
	fake := newFakeStream(t)
	fake.closeOnStop = true
	stream := openSTT(t, fake, STTConfig{}, sttRequest(fake.endpoint()))
	if err := stream.WriteAudio(context.Background(), make([]byte, 3200)); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	events := drainSTT(t, stream)
	if failure := terminalFailure(t, events); failure.Code != "provider_unavailable" || !strings.Contains(failure.Message, "without a canonical_record") {
		t.Fatalf("failure = %+v", failure)
	}
	if billing := lastUsage(t, events).Billing; billing.Complete || billing.Quantities["duration_seconds"] != 3200 {
		t.Fatalf("billing = %+v", billing)
	}
}

func TestMidSessionDisconnectBillsTheBytesSentAndIsRetryable(t *testing.T) {
	t.Parallel()
	fake := newFakeStream(t)
	fake.closeAfterAudio = websocket.StatusInternalError
	stream := openSTT(t, fake, STTConfig{}, sttRequest(fake.endpoint()))
	if err := stream.WriteAudio(context.Background(), make([]byte, 3200)); err != nil {
		t.Fatal(err)
	}
	events := drainSTT(t, stream)
	failure := terminalFailure(t, events)
	if failure.Code != "provider_unavailable" || !failure.Retryable {
		t.Fatalf("failure = %+v", failure)
	}
	if billing := lastUsage(t, events).Billing; billing.Complete || billing.Quantities["duration_seconds"] != 3200 {
		t.Fatalf("billing = %+v", billing)
	}
}

func TestAServerEndedSessionAtItsCapIsInputTooLarge(t *testing.T) {
	t.Parallel()
	fake := newFakeStream(t)
	fake.onAudio = []string{`{"type":"canonical_record","record":{"status":"complete"},"session_stats":{"audio_ms":1800000,"ended_reason":"max_session_s"}}`}
	fake.closeAfterAudio = websocket.StatusNormalClosure
	stream := openSTT(t, fake, STTConfig{}, sttRequest(fake.endpoint()))
	if err := stream.WriteAudio(context.Background(), make([]byte, 3200)); err != nil {
		t.Fatal(err)
	}
	events := drainSTT(t, stream)
	if failure := terminalFailure(t, events); failure.Code != "input_too_large" || !strings.Contains(failure.Message, "cap") {
		t.Fatalf("failure = %+v", failure)
	}
	if billing := lastUsage(t, events).Billing; !billing.Complete || billing.Quantities["duration_seconds"] != 1_800_000 {
		t.Fatalf("billing = %+v", billing)
	}
}

func TestErrorFramesInPlaceOfReadyFailOpenByCode(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		code      string
		retryable bool
	}{
		"UNAUTHORIZED":             {"authentication_failed", false},
		"UNSUPPORTED_LANGUAGE":     {"unsupported_language", false},
		"UNSUPPORTED_AUDIO_FORMAT": {"unsupported_media", false},
		"TOO_MANY_STREAMS":         {"provider_rate_limited", true},
		"SPEAKER_CAPACITY_FULL":    {"provider_rate_limited", true},
		"SOMETHING_NEW":            {"invalid_request", false},
	}
	for vendorCode, want := range cases {
		fake := newFakeStream(t)
		fake.open = []string{`{"type":"error","code":"` + vendorCode + `","message":"vendor detail that stays out of Message","supported_languages":["auto","en"]}`}
		fake.closeAfterOpen = websocket.StatusPolicyViolation
		_, err := newTestSTT(t, fake, STTConfig{}).Open(context.Background(), sttRequest(fake.endpoint()))
		var providerErr *runtimepkg.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != want.code || providerErr.Retryable != want.retryable {
			t.Errorf("%s: Open error = %#v, want %s", vendorCode, err, want.code)
			continue
		}
		if strings.Contains(providerErr.Message, "vendor detail") || !strings.Contains(providerErr.Message, vendorCode) {
			t.Errorf("%s: message = %q", vendorCode, providerErr.Message)
		}
	}
}

func TestA1013CloseBeforeReadyIsRateLimited(t *testing.T) {
	t.Parallel()
	for _, reason := range []string{"TOO_MANY_STREAMS", "SPEAKER_CAPACITY_FULL", ""} {
		fake := newFakeStream(t)
		fake.open = []string{}
		fake.closeAfterOpen = websocket.StatusTryAgainLater
		fake.closeReason = reason
		_, err := newTestSTT(t, fake, STTConfig{}).Open(context.Background(), sttRequest(fake.endpoint()))
		var providerErr *runtimepkg.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != "provider_rate_limited" || !providerErr.Retryable {
			t.Errorf("1013 %q: Open error = %#v", reason, err)
		}
	}
}

func TestOpenRefusesWhatTheSocketCannotServe(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		mutate func(*runtimepkg.AdapterRequest)
		code   string
	}{
		"8 kHz":         {func(r *runtimepkg.AdapterRequest) { r.Media.SampleRateHz = 8_000 }, "unsupported_media"},
		"48 kHz":        {func(r *runtimepkg.AdapterRequest) { r.Media.SampleRateHz = 48_000 }, "unsupported_media"},
		"stereo":        {func(r *runtimepkg.AdapterRequest) { r.Media.Channels = 2 }, "unsupported_media"},
		"japanese":      {func(r *runtimepkg.AdapterRequest) { r.Options.Language = "ja-JP" }, "unsupported_language"},
		"explicit auto": {func(r *runtimepkg.AdapterRequest) { r.Options.Language = "auto" }, "unsupported_language"},
		"diarization": {func(r *runtimepkg.AdapterRequest) {
			on := true
			r.Options.STT = &protocol.SttOptions{Diarization: &on}
		}, "invalid_request"},
		"keywords": {func(r *runtimepkg.AdapterRequest) {
			r.Options.STT = &protocol.SttOptions{Keywords: []string{"metformin"}}
		}, "invalid_request"},
		"word timestamps": {func(r *runtimepkg.AdapterRequest) {
			on := true
			r.Options.STT = &protocol.SttOptions{WordTimestamps: &on}
		}, "invalid_request"},
	}
	for name, test := range cases {
		fake := newFakeStream(t)
		request := sttRequest(fake.endpoint())
		media := *request.Media
		request.Media = &media
		test.mutate(&request)
		_, err := newTestSTT(t, fake, STTConfig{}).Open(context.Background(), request)
		var providerErr *runtimepkg.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != test.code {
			t.Errorf("%s: Open error = %v, want %s", name, err, test.code)
		}
		if fake.connections.Load() != 0 {
			t.Errorf("%s dialed the socket", name)
		}
	}
	fake := newFakeStream(t)
	request := sttRequest(fake.endpoint())
	request.Plan.Route.Model = "omi-general-1"
	if _, err := newTestSTT(t, fake, STTConfig{}).Open(context.Background(), request); err == nil || fake.connections.Load() != 0 {
		t.Fatalf("omi-general-1 opened: %v", err)
	}
	request = sttRequest("wss://api.omi.health/v1/stream")
	if _, err := newTestSTT(t, fake, STTConfig{}).Open(context.Background(), request); err == nil {
		t.Fatal("the region-picking host was accepted")
	}
}

func TestTheSocketGetsTheBaseLanguageAndEnglishByDefault(t *testing.T) {
	t.Parallel()
	for tag, want := range map[string]string{"": "en", "es-MX": "es", "pt_BR": "pt", "AR-eg": "ar", "hi": "hi", "nl-BE": "nl", "fr": "fr", "de-AT": "de"} {
		fake := newFakeStream(t)
		request := sttRequest(fake.endpoint())
		request.Options.Language = tag
		openSTT(t, fake, STTConfig{}, request)
		if _, start, _, _, _, _ := fake.snapshot(); start["language"] != want {
			t.Errorf("language %q sent %v, want %s", tag, start["language"], want)
		}
	}
}

func TestTheKeyNeverLeavesTheStartFrame(t *testing.T) {
	t.Parallel()
	fake := newFakeStream(t)
	fake.open = []string{`{"type":"error","code":"UNAUTHORIZED","message":"key ` + testKey + ` is not valid"}`}
	fake.closeAfterOpen = websocket.StatusPolicyViolation
	_, err := newTestSTT(t, fake, STTConfig{}).Open(context.Background(), sttRequest(fake.endpoint()))
	if err == nil {
		t.Fatal("Open succeeded")
	}
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) {
		t.Fatalf("error = %#v", err)
	}
	rendered := fmt.Sprintf("%v %+v %#v %s", err, providerErr, providerErr, marshalData(providerErr.Extensions))
	if strings.Contains(rendered, testKey) {
		t.Fatalf("the key leaked into the error: %s", rendered)
	}
	if _, _, raw, _, _, _ := fake.snapshot(); !strings.Contains(raw, testKey) {
		t.Fatalf("the start frame did not carry the key: %s", raw)
	}

	// A live session whose frames echo the key.
	fake = newFakeStream(t)
	fake.onAudio = []string{
		`{"type":"surprise","echo":"` + testKey + `"}`,
		`{"type":"partial","text":"` + testKey + `","turn_id":"t1"}`,
	}
	fake.closeAfterAudio = websocket.StatusInternalError
	stream := openSTT(t, fake, STTConfig{}, sttRequest(fake.endpoint()))
	if err := stream.WriteAudio(context.Background(), make([]byte, 3200)); err != nil {
		t.Fatal(err)
	}
	for _, event := range drainSTT(t, stream) {
		rendered := fmt.Sprintf("%s %v %+v %s", event.Data, event.Err, event.Billing, marshalData(event.Extensions))
		if strings.Contains(rendered, testKey) {
			t.Fatalf("the key leaked into %s: %s", event.Type, rendered)
		}
	}
	frame := startFrame{APIKey: testKey}
	if rendered := fmt.Sprintf("%v %+v %#v %s", frame, frame, frame, frame); strings.Contains(rendered, testKey) {
		t.Fatalf("formatting the start frame printed the key: %s", rendered)
	}
}

func TestTheDialRefusesARedirect(t *testing.T) {
	t.Parallel()
	target := newFakeStream(t)
	redirector := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.server.URL+streamPath, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(redirector.Close)
	adapter := newTestSTT(t, target, STTConfig{})
	_, err := adapter.Open(context.Background(), sttRequest("ws"+strings.TrimPrefix(redirector.URL, "http")+streamPath))
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "provider_error" || providerErr.ProviderStatus != http.StatusTemporaryRedirect {
		t.Fatalf("Open error = %#v, want a refused redirect", err)
	}
	if target.connections.Load() != 0 {
		t.Fatal("the redirect was followed")
	}
}

func TestKeepAliveGoesOutWhileTheCallerIsSilent(t *testing.T) {
	t.Parallel()
	fake := newFakeStream(t)
	stream := openSTT(t, fake, STTConfig{KeepAliveInterval: 40 * time.Millisecond}, sttRequest(fake.endpoint()))
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, _, _, _, _, controls := fake.snapshot()
		if len(controls) > 0 {
			if controls[0] != `{"type":"keep_alive"}` {
				t.Fatalf("controls = %v", controls)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no keep_alive was sent")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// keep_alive_ack is quiet.
	if err := stream.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, event := range drainSTT(t, stream) {
		if event.Type == protocol.EventWarning || event.Err != nil {
			t.Fatalf("unexpected %s %v %s", event.Type, event.Err, event.Data)
		}
	}
}

func TestWriteAudioSendsWholeSampleFramesOfAtMost100ms(t *testing.T) {
	t.Parallel()
	fake := newFakeStream(t)
	stream := openSTT(t, fake, STTConfig{}, sttRequest(fake.endpoint()))
	ctx := context.Background()
	if err := stream.WriteAudio(ctx, make([]byte, 7_001)); err != nil {
		t.Fatal(err)
	}
	if err := stream.WriteAudio(ctx, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(ctx); err != nil {
		t.Fatal(err)
	}
	drainSTT(t, stream)
	_, _, _, _, frames, _ := fake.snapshot()
	var sizes []int
	for _, frame := range frames {
		sizes = append(sizes, len(frame))
	}
	if fmt.Sprint(sizes) != "[3200 3200 600 2]" {
		t.Fatalf("frame sizes = %v", sizes)
	}
}

func TestCancelIsANoOpAndAbortClosesTheSocket(t *testing.T) {
	t.Parallel()
	fake := newFakeStream(t)
	stream := openSTT(t, fake, STTConfig{}, sttRequest(fake.endpoint()))
	ctx := context.Background()
	if err := stream.Cancel(ctx); err != nil {
		t.Fatal(err)
	}
	if err := stream.WriteAudio(ctx, make([]byte, 320)); err != nil {
		t.Fatalf("WriteAudio after Cancel = %v", err)
	}
	if err := stream.(runtimepkg.AbortingProviderStream).Abort(ctx); err != nil {
		t.Fatal(err)
	}
	drainSTT(t, stream)
	if err := stream.WriteAudio(ctx, make([]byte, 320)); !errors.Is(err, runtimepkg.ErrSessionClosed) {
		t.Fatalf("WriteAudio after Abort = %v", err)
	}
	if err := stream.CommitAudio(ctx); !errors.Is(err, runtimepkg.ErrSessionClosed) {
		t.Fatalf("CommitAudio after Abort = %v", err)
	}
}

func TestUnknownFramesWarnAndQuietFramesDoNot(t *testing.T) {
	t.Parallel()
	fake := newFakeStream(t)
	var quiet []string
	for frameType := range quietFrames {
		quiet = append(quiet, `{"type":"`+frameType+`"}`)
	}
	fake.onAudio = append(quiet, `{"type":"brand_new"}`)
	stream := openSTT(t, fake, STTConfig{}, sttRequest(fake.endpoint()))
	skipOpening(t, stream)
	if err := stream.WriteAudio(context.Background(), make([]byte, 3200)); err != nil {
		t.Fatal(err)
	}
	event := nextSTTEvent(t, stream)
	if data := dataOf(t, event); event.Type != protocol.EventWarning || data["provider_type"] != "brand_new" {
		t.Fatalf("first event = %s %s, want the warning for brand_new only", event.Type, event.Data)
	}
}
