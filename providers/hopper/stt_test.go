package hopper

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

// readyFrame is the opening frame as the live socket sent it on 2026-10-13.
const readyFrame = `{"type": "Ready", "stream_id": 4781}`

// fakeListen stands in for wss://api.withhopper.com/stt/websocket: it records
// the handshake, sends Ready, and replays scripted events on the first audio
// frame and on END.
type fakeListen struct {
	t *testing.T
	// open is written right after the handshake; nil means Ready.
	open []string
	// openDelay holds the open frames back.
	openDelay time.Duration
	// closeAfterOpen, when set, closes the socket with this status after the
	// open frames.
	closeAfterOpen websocket.StatusCode
	// onAudio is written once the first binary frame arrives.
	onAudio []string
	// closeAfterAudio closes the socket with this status and reason after
	// onAudio.
	closeAfterAudio websocket.StatusCode
	closeReason     string
	// onEnd is written after the client's END, followed by the END reply
	// unless ignoreEnd is set.
	onEnd     []string
	ignoreEnd bool
	server    *httptest.Server

	mu       sync.Mutex
	header   http.Header
	query    url.Values
	frames   [][]byte
	controls []string
}

func newFakeListen(t *testing.T) *fakeListen {
	fake := &fakeListen{t: t}
	mux := http.NewServeMux()
	mux.HandleFunc(listenPath, fake.handle)
	fake.server = httptest.NewServer(mux)
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeListen) endpoint() string {
	return "ws" + strings.TrimPrefix(f.server.URL, "http") + listenPath
}

func (f *fakeListen) host() string {
	parsed, _ := url.Parse(f.server.URL)
	return parsed.Hostname()
}

func (f *fakeListen) handle(writer http.ResponseWriter, request *http.Request) {
	f.mu.Lock()
	f.header, f.query = request.Header.Clone(), request.URL.Query()
	f.mu.Unlock()
	conn, err := websocket.Accept(writer, request, nil)
	if err != nil {
		return
	}
	defer conn.CloseNow()
	ctx := request.Context()
	open := f.open
	if open == nil {
		open = []string{readyFrame}
	}
	time.Sleep(f.openDelay)
	f.write(ctx, conn, open)
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
		if string(payload) != "END" {
			f.t.Errorf("unexpected client text frame: %s", payload)
			continue
		}
		if f.ignoreEnd {
			continue
		}
		f.write(ctx, conn, append(append([]string(nil), f.onEnd...), "END"))
		// The client closes once it has read the END reply.
	}
}

func (f *fakeListen) write(ctx context.Context, conn *websocket.Conn, messages []string) {
	for _, message := range messages {
		if conn.Write(ctx, websocket.MessageText, []byte(message)) != nil {
			return
		}
	}
}

func (f *fakeListen) snapshot() (http.Header, url.Values, [][]byte, []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.header, f.query, append([][]byte(nil), f.frames...), append([]string(nil), f.controls...)
}

func sttRequest(endpoint string) runtimepkg.AdapterRequest {
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindSTT,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged},
			Route: protocol.PlanRoute{Provider: ProviderName, Model: DefaultSTTModel, Adapter: STTAdapterID, Transport: protocol.TransportWebSocket, Endpoint: endpoint,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialRelayAccess, Value: "sk_hopper_test", ExpiresAt: time.Now().Add(time.Minute)}},
		},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
		Options: protocol.RequestOptions{Language: "en-US"},
	}
}

func newTestSTT(t *testing.T, fake *fakeListen, config STTConfig) *STTAdapter {
	t.Helper()
	config.AllowedEndpointHosts = []string{fake.host()}
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

func openSTT(t *testing.T, fake *fakeListen, config STTConfig, request runtimepkg.AdapterRequest) runtimepkg.ProviderStream {
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
			t.Fatalf("events did not close; got %d", len(events))
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

func TestStreamIdentifierReadsNumbersAndStrings(t *testing.T) {
	t.Parallel()
	for raw, want := range map[string]string{`4781`: "4781", `"s-1"`: "s-1", `null`: "", ``: "", `{}`: ""} {
		if got := streamIdentifier(json.RawMessage(raw)); got != want {
			t.Errorf("streamIdentifier(%s) = %q, want %q", raw, got, want)
		}
	}
}

func TestOpenSendsTheQueryAndBearerAndWaitsForReady(t *testing.T) {
	t.Parallel()
	fake := newFakeListen(t)
	fake.openDelay = 150 * time.Millisecond
	request := sttRequest(fake.endpoint())
	request.Options.STT = &protocol.SttOptions{ProviderOptions: map[string]map[string]any{
		ProviderName: {"turn_detection": "Patient", "api_key": "never forwarded"},
	}}
	started := time.Now()
	stream := openSTT(t, fake, STTConfig{}, request)
	if elapsed := time.Since(started); elapsed < 150*time.Millisecond {
		t.Fatalf("Open returned after %s, before Ready", elapsed)
	}
	header, query, _, _ := fake.snapshot()
	if header.Get("Authorization") != "Bearer sk_hopper_test" {
		t.Fatalf("handshake headers = %v", header)
	}
	want := url.Values{"model": {"nemotron-asr"}, "sample_rate": {"16000"}, "turn_detection": {"patient"}}
	if query.Encode() != want.Encode() {
		t.Fatalf("query = %s, want %s", query.Encode(), want.Encode())
	}
	ready := nextSTTEvent(t, stream)
	if data := dataOf(t, ready); ready.Type != protocol.EventSessionReady || data["provider_model"] != DefaultSTTModel || data["provider_request_id"] != "4781" {
		t.Fatalf("first event = %s %s, want session.ready", ready.Type, ready.Data)
	}
	usage := nextSTTEvent(t, stream)
	if usage.Type != protocol.EventUsageObserved || usage.Billing == nil || usage.Billing.Complete || usage.Billing.Model != DefaultSTTModel || usage.Billing.Language != "en" || usage.Billing.ProviderRequestID != "4781" {
		t.Fatalf("opening usage = %s %+v", usage.Type, usage.Billing)
	}
	if err := usage.Billing.Validate(); err != nil {
		t.Fatalf("opening billing invalid: %v", err)
	}
}

func TestTurnDetectionDefaultsToBalanced(t *testing.T) {
	t.Parallel()
	fake := newFakeListen(t)
	openSTT(t, fake, STTConfig{}, sttRequest(fake.endpoint()))
	if _, query, _, _ := fake.snapshot(); query.Get("turn_detection") != "balanced" {
		t.Fatalf("turn_detection = %q, want balanced", query.Get("turn_detection"))
	}
}

func TestOpenTimesOutWithoutReady(t *testing.T) {
	t.Parallel()
	fake := newFakeListen(t)
	fake.open = []string{}
	_, err := newTestSTT(t, fake, STTConfig{SetupTimeout: 100 * time.Millisecond}).Open(context.Background(), sttRequest(fake.endpoint()))
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "provider_unavailable" || !providerErr.Retryable {
		t.Fatalf("Open error = %#v, want a retryable missing Ready", err)
	}
}

func TestTurnMapsPartialsToDeltasAndTheFinalEndsTheTurn(t *testing.T) {
	t.Parallel()
	fake := newFakeListen(t)
	// Text leads with a space on partials and finals alike, and events carry
	// timestamp, audio_timestamp, stream_id and frames_processed besides.
	fake.onAudio = []string{
		`{"text":" thanks","is_final":false,"audio_start":0.8,"audio_end":1.12,"timestamp":1760342400.1,"audio_timestamp":1.12,"stream_id":4781,"frames_processed":14}`,
		`{"text":"","is_final":false,"audio_start":0.8,"audio_end":1.2,"stream_id":4781}`,
		`{"text":" thanks for","is_final":false,"audio_start":0.8,"audio_end":1.4,"stream_id":4781}`,
		`{"text":" thanks for calling ","is_final":true,"audio_start":0.8,"audio_end":1.9,"stream_id":4781,"words":[{"text":" thanks","start":0.8,"end":1.12,"conf":0},{"text":" for","start":1.12,"end":1.4,"conf":0},{"text":" ","start":1.4,"end":1.4,"conf":0},{"text":" untimed"},{"text":" calling","start":1.4,"end":1.9,"conf":0}]}`,
	}
	stream := openSTT(t, fake, STTConfig{}, sttRequest(fake.endpoint()))
	if err := stream.WriteAudio(context.Background(), make([]byte, 64_000)); err != nil {
		t.Fatal(err)
	}
	// Wait for the turn before closing so the order below is the socket's.
	var events []runtimepkg.ProviderEvent
	for len(events) == 0 || events[len(events)-1].Type != protocol.EventSpeechEnded {
		events = append(events, nextSTTEvent(t, stream))
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	events = append(events, drainSTT(t, stream)...)
	for _, event := range events {
		if event.Err != nil {
			t.Fatalf("unexpected error event: %v", event.Err)
		}
	}
	want := "session.ready,usage.observed,speech.started,transcript.delta,transcript.delta,transcript.final,speech.ended,usage.observed"
	if got := eventTypes(events); got != want {
		t.Fatalf("events = %s, want %s", got, want)
	}
	if data := dataOf(t, events[2]); data["audio_start_ms"] != float64(800) || data["turn_id"] != float64(0) {
		t.Fatalf("speech.started = %v", data)
	}
	if data := dataOf(t, events[3]); data["text"] != "thanks" || data["is_final"] != false || data["audio_end_ms"] != float64(1120) {
		t.Fatalf("first delta = %v", data)
	}
	if data := dataOf(t, events[4]); data["text"] != "thanks for" || data["words"] != nil {
		t.Fatalf("cumulative delta = %v", data)
	}
	final := dataOf(t, events[5])
	if final["text"] != "thanks for calling" || final["is_final"] != true || final["speech_final"] != true || final["language"] != "en" || final["audio_end_ms"] != float64(1900) {
		t.Fatalf("final = %v", final)
	}
	if _, reported := final["confidence"]; reported {
		t.Fatalf("final reports the reserved conf: %v", final)
	}
	words, _ := json.Marshal(final["words"])
	if string(words) != `[{"end_ms":1120,"start_ms":800,"text":"thanks"},{"end_ms":1400,"start_ms":1120,"text":"for"},{"end_ms":1900,"start_ms":1400,"text":"calling"}]` {
		t.Fatalf("words = %s", words)
	}
	if data := dataOf(t, events[6]); data["audio_end_ms"] != float64(1900) || data["reason"] != "end_of_turn" {
		t.Fatalf("speech.ended = %v", data)
	}
	billing := events[7].Billing
	// 64,000 bytes of 16 kHz PCM16 is two seconds.
	if billing == nil || !billing.Complete || billing.OperationID != "stream" || billing.ProviderRequestID != "4781" || billing.Quantities["duration_seconds"] != 64_000 || billing.QuantityDenominator("duration_seconds") != 32_000 {
		t.Fatalf("closing billing = %+v", billing)
	}
	if merged, err := protocol.MergeBillingObservation(*events[1].Billing, *billing); err != nil || !merged.Complete {
		t.Fatalf("opening and closing observations do not merge: %+v %v", merged, err)
	}
	if err := stream.(runtimepkg.TerminalErrorProviderStream).TerminalError(); err != nil {
		t.Fatalf("clean close left a terminal error: %v", err)
	}
}

// A final flushed by END arrives without words, audio_start or audio_end, as
// the live socket sent it on 2026-10-13. The relay reports no timings rather
// than zero ones.
func TestCloseSendsENDAndFlushesTheOpenTurn(t *testing.T) {
	t.Parallel()
	fake := newFakeListen(t)
	fake.onAudio = []string{`{"text":" i can help","is_final":false,"audio_start":0.1,"audio_end":0.4}`}
	fake.onEnd = []string{`{"text": " i can help you reset your password", "timestamp": 1760342401.5, "audio_timestamp": 0.0, "is_final": true}`}
	stream := openSTT(t, fake, STTConfig{}, sttRequest(fake.endpoint()))
	if err := stream.WriteAudio(context.Background(), make([]byte, 3_200)); err != nil {
		t.Fatal(err)
	}
	if err := stream.CommitAudio(context.Background()); err != nil {
		t.Fatalf("CommitAudio: %v", err)
	}
	for event := nextSTTEvent(t, stream); event.Type != protocol.EventTranscriptDelta; event = nextSTTEvent(t, stream) {
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	events := drainSTT(t, stream)
	if got := eventTypes(events); got != "transcript.final,speech.ended,usage.observed" {
		t.Fatalf("events after Close = %s", got)
	}
	final := dataOf(t, events[0])
	if final["text"] != "i can help you reset your password" || final["is_final"] != true {
		t.Fatalf("flushed final = %s", events[0].Data)
	}
	for _, key := range []string{"words", "audio_start_ms", "audio_end_ms"} {
		if _, present := final[key]; present {
			t.Fatalf("flushed final invents %s: %s", key, events[0].Data)
		}
	}
	if _, present := dataOf(t, events[1])["audio_end_ms"]; present {
		t.Fatalf("speech.ended invents an end time: %s", events[1].Data)
	}
	if billing := events[2].Billing; billing == nil || !billing.Complete || billing.Quantities["duration_seconds"] != 3_200 {
		t.Fatalf("closing billing = %+v", billing)
	}
	// CommitAudio sent nothing; END is the only control frame.
	if _, _, _, controls := fake.snapshot(); strings.Join(controls, ",") != "END" {
		t.Fatalf("controls = %v, want only END", controls)
	}
	if err := stream.WriteAudio(context.Background(), []byte{0, 0}); !errors.Is(err, runtimepkg.ErrSessionClosed) {
		t.Fatalf("write after close = %v", err)
	}
}

// Hopper segments one utterance into a final per turn; each final ends its
// own turn.
func TestEachFinalEndsItsOwnTurn(t *testing.T) {
	t.Parallel()
	fake := newFakeListen(t)
	fake.onAudio = []string{
		`{"text":" thanks for calling speco support","is_final":true,"audio_start":0.2,"audio_end":2.1,"words":[{"text":" thanks","start":0.2,"end":0.5,"conf":0}]}`,
		`{"text":" i can help","is_final":false,"audio_start":2.6,"audio_end":3.2}`,
		`{"text":" i can help you reset your password","is_final":true,"audio_start":2.6,"audio_end":4.4,"words":[{"text":" i","start":2.6,"end":2.7,"conf":0}]}`,
	}
	stream := openSTT(t, fake, STTConfig{}, sttRequest(fake.endpoint()))
	_ = nextSTTEvent(t, stream)
	_ = nextSTTEvent(t, stream)
	if err := stream.WriteAudio(context.Background(), make([]byte, 640)); err != nil {
		t.Fatal(err)
	}
	var events []runtimepkg.ProviderEvent
	for len(events) < 7 {
		events = append(events, nextSTTEvent(t, stream))
	}
	want := "speech.started,transcript.final,speech.ended,speech.started,transcript.delta,transcript.final,speech.ended"
	if got := eventTypes(events); got != want {
		t.Fatalf("events = %s, want %s", got, want)
	}
	first, second := dataOf(t, events[1]), dataOf(t, events[5])
	if first["text"] != "thanks for calling speco support" || first["turn_id"] != float64(0) || second["text"] != "i can help you reset your password" || second["turn_id"] != float64(1) {
		t.Fatalf("finals = %v / %v", first, second)
	}
	if data := dataOf(t, events[3]); data["audio_start_ms"] != float64(2600) || data["turn_id"] != float64(1) {
		t.Fatalf("second speech.started = %v", data)
	}
}

func TestEmptyFinalIsStillAFinal(t *testing.T) {
	t.Parallel()
	fake := newFakeListen(t)
	fake.onAudio = []string{`{"text":"","is_final":true,"audio_start":0,"audio_end":0.5,"words":[]}`}
	stream := openSTT(t, fake, STTConfig{}, sttRequest(fake.endpoint()))
	_ = nextSTTEvent(t, stream)
	_ = nextSTTEvent(t, stream)
	if err := stream.WriteAudio(context.Background(), make([]byte, 640)); err != nil {
		t.Fatal(err)
	}
	final := nextSTTEvent(t, stream)
	if final.Type != protocol.EventTranscriptFinal || dataOf(t, final)["text"] != "" {
		t.Fatalf("event = %s %s, want an empty final", final.Type, final.Data)
	}
}

func TestWriteAudioSendsWholeSampleFramesOfAtMost100ms(t *testing.T) {
	t.Parallel()
	fake := newFakeListen(t)
	request := sttRequest(fake.endpoint())
	request.Media.SampleRateHz = 8_000
	stream := openSTT(t, fake, STTConfig{}, request)
	// 8 kHz: 100 ms is 1,600 bytes. 3,201 bytes is two full frames and one
	// byte that must wait for the next write.
	if err := stream.WriteAudio(context.Background(), make([]byte, 3_201)); err != nil {
		t.Fatal(err)
	}
	if err := stream.WriteAudio(context.Background(), make([]byte, 3)); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	events := drainSTT(t, stream)
	_, query, frames, _ := fake.snapshot()
	if query.Get("sample_rate") != "8000" {
		t.Fatalf("sample_rate = %q", query.Get("sample_rate"))
	}
	var sizes []int
	for _, frame := range frames {
		sizes = append(sizes, len(frame))
	}
	if len(sizes) != 3 || sizes[0] != 1_600 || sizes[1] != 1_600 || sizes[2] != 4 {
		t.Fatalf("frame sizes = %v, want [1600 1600 4]", sizes)
	}
	billing := events[len(events)-1].Billing
	if billing == nil || billing.Quantities["duration_seconds"] != 3_204 || billing.QuantityDenominator("duration_seconds") != 16_000 {
		t.Fatalf("billing = %+v, want the bytes sent at 8 kHz", billing)
	}
}

func TestMidSessionCloseIsClassified(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		status    websocket.StatusCode
		reason    string
		code      string
		retryable bool
	}{
		{name: "balance exhausted", status: websocket.StatusPolicyViolation, reason: "insufficient balance", code: "provider_quota_exceeded"},
		{name: "bare 1008", status: websocket.StatusPolicyViolation, code: "provider_quota_exceeded"},
		{name: "key revoked", status: websocket.StatusPolicyViolation, reason: "invalid api key", code: "authentication_failed"},
		{name: "internal", status: websocket.StatusInternalError, code: "provider_unavailable", retryable: true},
		{name: "bad audio", status: websocket.StatusUnsupportedData, code: "invalid_request"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fake := newFakeListen(t)
			fake.closeAfterAudio, fake.closeReason = tc.status, tc.reason
			stream := openSTT(t, fake, STTConfig{}, sttRequest(fake.endpoint()))
			if err := stream.WriteAudio(context.Background(), make([]byte, 640)); err != nil {
				t.Fatal(err)
			}
			events := drainSTT(t, stream)
			for _, event := range events {
				if event.Billing != nil && event.Billing.Complete {
					t.Fatal("billing completed without the END reply")
				}
			}
			failure := terminalFailure(t, events)
			if failure.Code != tc.code || failure.Retryable != tc.retryable {
				t.Fatalf("failure = %#v, want %s retryable=%v", failure, tc.code, tc.retryable)
			}
			if stream.(runtimepkg.TerminalErrorProviderStream).TerminalError() == nil {
				t.Fatal("terminal error not preserved")
			}
		})
	}
}

func TestSetupClosesFailOpenWithTheirClassification(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		status    websocket.StatusCode
		code      string
		retryable bool
	}{
		{name: "empty balance", status: websocket.StatusPolicyViolation, code: "provider_quota_exceeded"},
		{name: "internal", status: websocket.StatusInternalError, code: "provider_unavailable", retryable: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fake := newFakeListen(t)
			fake.open = []string{}
			fake.closeAfterOpen = tc.status
			_, err := newTestSTT(t, fake, STTConfig{}).Open(context.Background(), sttRequest(fake.endpoint()))
			var providerErr *runtimepkg.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Code != tc.code || providerErr.Retryable != tc.retryable {
				t.Fatalf("Open error = %#v, want %s retryable=%v", err, tc.code, tc.retryable)
			}
		})
	}
}

func TestHandshakeRejectionIsClassifiedFromTheEnvelope(t *testing.T) {
	t.Parallel()
	cases := []struct {
		status int
		body   string
		code   string
	}{
		{http.StatusUnauthorized, `{"error":{"message":"Incorrect API key provided","type":"invalid_request_error","code":"invalid_api_key","param":null}}`, "authentication_failed"},
		{http.StatusPaymentRequired, `{"error":{"message":"Key balance is zero","type":"insufficient_quota","code":"insufficient_quota","param":null}}`, "provider_quota_exceeded"},
		{http.StatusBadRequest, `{"error":{"message":"bad rate","type":"invalid_request_error","code":"invalid_sample_rate","param":"sample_rate"}}`, "unsupported_media"},
		// The live socket refuses a bad key at the upgrade; the status alone
		// classifies it whatever the body.
		{http.StatusUnauthorized, `Unauthorized`, "authentication_failed"},
	}
	for _, tc := range cases {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(tc.status)
			_, _ = w.Write([]byte(tc.body))
		}))
		t.Cleanup(server.Close)
		parsed, _ := url.Parse(server.URL)
		adapter, err := NewSTT(STTConfig{AllowedEndpointHosts: []string{parsed.Hostname()}, AllowInsecureEndpoint: true})
		if err != nil {
			t.Fatal(err)
		}
		_, err = adapter.Open(context.Background(), sttRequest("ws"+strings.TrimPrefix(server.URL, "http")+listenPath))
		var providerErr *runtimepkg.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != tc.code || providerErr.Retryable || providerErr.ProviderStatus != tc.status {
			t.Fatalf("%d: Open error = %#v, want %s", tc.status, err, tc.code)
		}
		if strings.Contains(providerErr.Message, "Incorrect") {
			t.Fatalf("message quotes the vendor text: %q", providerErr.Message)
		}
	}
}

func TestCloseDrainTimeoutReportsTheMissingEND(t *testing.T) {
	t.Parallel()
	fake := newFakeListen(t)
	fake.ignoreEnd = true
	stream := openSTT(t, fake, STTConfig{CloseDrainTimeout: 100 * time.Millisecond}, sttRequest(fake.endpoint()))
	if err := stream.WriteAudio(context.Background(), make([]byte, 640)); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	events := drainSTT(t, stream)
	for _, event := range events {
		if event.Billing != nil && event.Billing.Complete {
			t.Fatal("billing completed without the END reply")
		}
	}
	if failure := terminalFailure(t, events); failure.Code != "request_timeout" {
		t.Fatalf("failure = %#v, want request_timeout", failure)
	}
}

func TestOpenRefusesWhatTheSocketCannotServe(t *testing.T) {
	t.Parallel()
	adapter, err := NewSTT(STTConfig{})
	if err != nil {
		t.Fatal(err)
	}
	base := sttRequest("wss://api.withhopper.com/stt/websocket")
	cases := map[string]func(*runtimepkg.AdapterRequest){
		"11025 Hz": func(r *runtimepkg.AdapterRequest) {
			r.Media = &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 11_025, Channels: 1}
		},
		"stereo": func(r *runtimepkg.AdapterRequest) {
			r.Media = &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 2}
		},
		"french":        func(r *runtimepkg.AdapterRequest) { r.Options.Language = "fr-FR" },
		"unknown model": func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "parakeet" },
		"wrong path":    func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = "wss://api.withhopper.com/v1/realtime" },
		"foreign host":  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = "wss://example.com/stt/websocket" },
		"relay key off relay": func(r *runtimepkg.AdapterRequest) {
			r.Plan.Execution.ProviderRoute = protocol.RouteProviderDirect
		},
		"turn detection": func(r *runtimepkg.AdapterRequest) {
			r.Options.STT = &protocol.SttOptions{ProviderOptions: map[string]map[string]any{ProviderName: {"turn_detection": "eager"}}}
		},
	}
	for name, mutate := range cases {
		request := base
		media := *base.Media
		request.Media = &media
		mutate(&request)
		if _, err := adapter.Open(context.Background(), request); err == nil {
			t.Fatalf("%s: Open accepted", name)
		}
	}
	coded := map[string]func(*runtimepkg.AdapterRequest){
		"unsupported_media": cases["11025 Hz"],
		"unsupported_language": func(r *runtimepkg.AdapterRequest) {
			r.Options.Language = "ar"
		},
		"invalid_request": cases["turn detection"],
	}
	for code, mutate := range coded {
		request := base
		media := *base.Media
		request.Media = &media
		mutate(&request)
		_, err := adapter.Open(context.Background(), request)
		var providerErr *runtimepkg.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != code || providerErr.Hint == "" {
			t.Fatalf("Open error = %#v, want %s with a hint", err, code)
		}
	}
}

// The reader is the only sender on Events, so Open neither sends on a closed
// channel when the socket closes right after Ready nor blocks on a one-slot
// buffer that nobody drains until Open returns.
func TestOpenNeverSendsOnEvents(t *testing.T) {
	t.Parallel()
	cases := map[string]websocket.StatusCode{"ready then close": websocket.StatusInternalError, "ready and stay open": 0}
	for name, closeStatus := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fake := newFakeListen(t)
			fake.closeAfterOpen = closeStatus
			adapter := newTestSTT(t, fake, STTConfig{EventBuffer: 1})
			type result struct {
				stream runtimepkg.ProviderStream
				err    error
			}
			opened := make(chan result, 1)
			go func() {
				stream, err := adapter.Open(context.Background(), sttRequest(fake.endpoint()))
				opened <- result{stream, err}
			}()
			var got result
			select {
			case got = <-opened:
			case <-time.After(3 * time.Second):
				t.Fatal("Open blocked on the event buffer")
			}
			if got.err != nil {
				t.Fatalf("Open: %v", got.err)
			}
			t.Cleanup(func() { _ = got.stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) })
			if ready := nextSTTEvent(t, got.stream); ready.Type != protocol.EventSessionReady {
				t.Fatalf("first event = %s, want session.ready", ready.Type)
			}
			if usage := nextSTTEvent(t, got.stream); usage.Type != protocol.EventUsageObserved || usage.Billing == nil {
				t.Fatalf("second event = %s, want the opening usage", usage.Type)
			}
			if closeStatus == 0 {
				return
			}
			if failure := terminalFailure(t, drainSTT(t, got.stream)); failure.Code != "provider_unavailable" || !failure.Retryable {
				t.Fatalf("failure = %#v, want a retryable close", failure)
			}
		})
	}
}
