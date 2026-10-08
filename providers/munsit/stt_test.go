package munsit

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

const (
	openMetadata    = `{"type":"Metadata","session_id":"sess-1","correlation_id":null,"model":"munsit-v2","protocol_version":1,"channels":1,"sample_rate":16000}`
	closingMetadata = `{"type":"Metadata","session_id":"sess-1","audio_seconds_billed":3.56,"turn_count":1}`
)

// fakeListen stands in for wss://api.munsit.com/api/v1/listen: it records
// the handshake, sends the opening events, and replays scripted events on the
// first audio frame and on CloseStream.
type fakeListen struct {
	t *testing.T
	// open is written right after the handshake; nil means openMetadata.
	open []string
	// closeAfterOpen, when set, closes the socket with this status after the
	// open events, as the service does after a refusal.
	closeAfterOpen websocket.StatusCode
	// onAudio is written once the first binary frame arrives.
	onAudio []string
	// closeAfterAudio closes the socket with this status after onAudio.
	closeAfterAudio websocket.StatusCode
	// onCloseStream is written after CloseStream, followed by a 1000 close
	// unless ignoreCloseStream is set.
	onCloseStream     []string
	ignoreCloseStream bool
	server            *httptest.Server

	mu         sync.Mutex
	header     http.Header
	query      url.Values
	frames     [][]byte
	keepAlives int
	controls   []string
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
		open = []string{openMetadata}
	}
	f.write(ctx, conn, open)
	if f.closeAfterOpen != 0 {
		_ = conn.Close(f.closeAfterOpen, "engine closed")
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
					_ = conn.Close(f.closeAfterAudio, "engine closed")
					return
				}
			}
			continue
		}
		var control struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal(payload, &control)
		f.mu.Lock()
		f.controls = append(f.controls, control.Type)
		if control.Type == "KeepAlive" {
			f.keepAlives++
		}
		f.mu.Unlock()
		switch control.Type {
		case "KeepAlive":
		case "CloseStream":
			if f.ignoreCloseStream {
				continue
			}
			f.write(ctx, conn, f.onCloseStream)
			_ = conn.Close(websocket.StatusNormalClosure, "engine closed")
			return
		default:
			f.t.Errorf("unexpected client control: %s", payload)
		}
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
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialRelayAccess, Value: "munsit-key", ExpiresAt: time.Now().Add(time.Minute)}},
		},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
		Options: protocol.RequestOptions{Language: "ar-SA"},
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

func TestOpenSendsTheListenQueryAndWaitsForMetadata(t *testing.T) {
	t.Parallel()
	fake := newFakeListen(t)
	request := sttRequest(fake.endpoint())
	request.Options.STT = &protocol.SttOptions{ProviderOptions: map[string]map[string]any{
		ProviderName: {"endpointing": float64(500), "smart_turn": false, "hotwords": "never forwarded"},
	}}
	stream := openSTT(t, fake, STTConfig{}, request)
	header, query, _, _ := fake.snapshot()
	if header.Get("X-Api-Key") != "munsit-key" || header.Get("Authorization") != "" {
		t.Fatalf("handshake headers = %v", header)
	}
	want := url.Values{
		"encoding": {"linear16"}, "sample_rate": {"16000"}, "model": {"munsit"}, "language": {"ar"},
		"interim_results": {"true"}, "endpointing": {"500"}, "smart_turn": {"false"},
	}
	if query.Encode() != want.Encode() {
		t.Fatalf("query = %s, want %s", query.Encode(), want.Encode())
	}
	ready := nextSTTEvent(t, stream)
	if ready.Type != protocol.EventSessionReady {
		t.Fatalf("first event = %s, want session.ready", ready.Type)
	}
	if data := dataOf(t, ready); data["session_id"] != "sess-1" || data["provider_model"] != "munsit-v2" {
		t.Fatalf("session.ready data = %v", data)
	}
	usage := nextSTTEvent(t, stream)
	if usage.Type != protocol.EventUsageObserved || usage.Billing == nil || usage.Billing.Complete || usage.Billing.ProviderRequestID != "sess-1" || usage.Billing.Model != DefaultSTTModel {
		t.Fatalf("opening usage = %s %+v", usage.Type, usage.Billing)
	}
	if err := usage.Billing.Validate(); err != nil {
		t.Fatalf("opening billing invalid: %v", err)
	}
}

func TestTurnMapsCumulativePartialsFinalAndUtteranceEnd(t *testing.T) {
	t.Parallel()
	fake := newFakeListen(t)
	fake.onAudio = []string{
		`{"type":"SpeechStarted","channel":0,"ts":0.25}`,
		`{"type":"Results","channel":0,"turn_id":0,"transcript":"مرحبا","words":[{"word":"مرحبا","start":0.28,"end":0.68,"confidence":1}],"is_final":false,"speech_final":false,"language":"ar","confidence":null}`,
		`{"type":"Results","channel":0,"turn_id":0,"transcript":"مرحبا كيف","words":[{"word":"مرحبا","start":0.04,"end":0.4},{"word":"كيف","start":0.4,"end":0.8}],"is_final":false,"speech_final":false,"language":"ar","confidence":null}`,
		`{"type":"Results","channel":0,"turn_id":0,"transcript":"","words":[],"is_final":false,"speech_final":false}`,
		`{"type":"Results","channel":0,"turn_id":0,"transcript":" مرحبا كيف حالك ","words":[{"word":"مرحبا","start":0.04,"end":0.4},{"word":"حالك","start":0.96,"end":1.28}],"is_final":true,"speech_final":true,"language":"ar","confidence":0.995}`,
		`{"type":"UtteranceEnd","channel":0,"turn_id":0,"last_word_end":1.28}`,
		`{"type":"Gender","channel":0,"turn_id":0,"label":"male","score":0.999}`,
		`{"type":"Sentiment","channel":0,"turn_id":0,"label":"neutral","score":0.636}`,
	}
	fake.onCloseStream = []string{closingMetadata}
	stream := openSTT(t, fake, STTConfig{}, sttRequest(fake.endpoint()))
	if err := stream.WriteAudio(context.Background(), make([]byte, 640)); err != nil {
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
	var types []string
	for _, event := range events {
		if event.Err != nil {
			t.Fatalf("unexpected error event: %v", event.Err)
		}
		types = append(types, string(event.Type))
	}
	wantTypes := []string{"session.ready", "usage.observed", "speech.started", "transcript.delta", "transcript.delta", "transcript.final", "speech.ended", "usage.observed"}
	if strings.Join(types, ",") != strings.Join(wantTypes, ",") {
		t.Fatalf("events = %v, want %v", types, wantTypes)
	}
	if data := dataOf(t, events[3]); data["text"] != "مرحبا" || data["is_final"] != false {
		t.Fatalf("first delta = %v", data)
	}
	if data := dataOf(t, events[4]); data["text"] != "مرحبا كيف" || data["audio_end_ms"] != float64(800) {
		t.Fatalf("cumulative delta = %v", data)
	}
	final := dataOf(t, events[5])
	if final["text"] != "مرحبا كيف حالك" || final["is_final"] != true || final["speech_final"] != true || final["confidence"] != 0.995 || final["provider_request_id"] != "sess-1" {
		t.Fatalf("final = %v", final)
	}
	if data := dataOf(t, events[6]); data["audio_end_ms"] != float64(1280) {
		t.Fatalf("speech.ended = %v", data)
	}
	billing := events[7].Billing
	if billing == nil || !billing.Complete || billing.Quantities["duration_seconds"] != 3560 || billing.OperationID != "stream" || billing.ProviderRequestID != "sess-1" || billing.Language != "ar" {
		t.Fatalf("closing billing = %+v", billing)
	}
	if merged, err := protocol.MergeBillingObservation(*events[1].Billing, *billing); err != nil || !merged.Complete {
		t.Fatalf("opening and closing observations do not merge: %+v %v", merged, err)
	}
	if err := stream.(runtimepkg.TerminalErrorProviderStream).TerminalError(); err != nil {
		t.Fatalf("clean close left a terminal error: %v", err)
	}
	if _, _, _, controls := fake.snapshot(); strings.Join(controls, ",") != "CloseStream" {
		t.Fatalf("controls = %v, want only CloseStream", controls)
	}
}

func TestEmptyFinalIsStillAFinal(t *testing.T) {
	t.Parallel()
	fake := newFakeListen(t)
	fake.onAudio = []string{`{"type":"Results","channel":0,"turn_id":2,"transcript":"","words":[],"is_final":true,"speech_final":true}`}
	stream := openSTT(t, fake, STTConfig{}, sttRequest(fake.endpoint()))
	_ = nextSTTEvent(t, stream)
	_ = nextSTTEvent(t, stream)
	if err := stream.WriteAudio(context.Background(), make([]byte, 640)); err != nil {
		t.Fatal(err)
	}
	final := nextSTTEvent(t, stream)
	if final.Type != protocol.EventTranscriptFinal || dataOf(t, final)["text"] != "" || dataOf(t, final)["turn_id"] != float64(2) {
		t.Fatalf("event = %s %s, want an empty final", final.Type, final.Data)
	}
}

func TestWriteAudioSendsWholeSampleFramesOfAtMost100ms(t *testing.T) {
	t.Parallel()
	fake := newFakeListen(t)
	fake.onCloseStream = []string{closingMetadata}
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
	if err := stream.CommitAudio(context.Background()); err != nil {
		t.Fatalf("CommitAudio: %v", err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	drainSTT(t, stream)
	_, query, frames, controls := fake.snapshot()
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
	// CommitAudio sends nothing: the socket has no Finalize.
	if strings.Join(controls, ",") != "CloseStream" {
		t.Fatalf("controls = %v, want only CloseStream", controls)
	}
	if err := stream.WriteAudio(context.Background(), []byte{0, 0}); !errors.Is(err, runtimepkg.ErrSessionClosed) {
		t.Fatalf("write after close = %v", err)
	}
}

func TestKeepAliveFillsInputSilence(t *testing.T) {
	t.Parallel()
	fake := newFakeListen(t)
	stream := openSTT(t, fake, STTConfig{KeepAliveInterval: 20 * time.Millisecond}, sttRequest(fake.endpoint()))
	deadline := time.Now().Add(2 * time.Second)
	for {
		fake.mu.Lock()
		count := fake.keepAlives
		fake.mu.Unlock()
		if count >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("keepalives = %d, want at least 2", count)
		}
		time.Sleep(5 * time.Millisecond)
	}
	_ = stream
}

func TestBufferedAudioIsHeldWithinTheLead(t *testing.T) {
	t.Parallel()
	fake := newFakeListen(t)
	stream := openSTT(t, fake, STTConfig{MaxAudioLead: 50 * time.Millisecond}, sttRequest(fake.endpoint()))
	started := time.Now()
	// 400 ms of 16 kHz audio may run at most 50 ms ahead of the wall clock.
	if err := stream.WriteAudio(context.Background(), make([]byte, 12_800)); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 200*time.Millisecond {
		t.Fatalf("400 ms of audio went out in %s; pacing did not hold it", elapsed)
	}
}

func TestSetupErrorsFailOpenWithTheirClassification(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		open      []string
		status    websocket.StatusCode
		code      string
		retryable bool
	}{
		{name: "bad key", open: []string{`{"type":"Error","code":1008,"message":"authentication required","recoverable":false}`}, status: websocket.StatusPolicyViolation, code: "authentication_failed"},
		{name: "low wallet", open: []string{`{"type":"Error","code":1008,"message":"insufficient wallet balance","recoverable":false}`}, status: websocket.StatusPolicyViolation, code: "provider_quota_exceeded"},
		{name: "session limit", open: []string{`{"type":"Error","code":1008,"message":"concurrent session limit reached","recoverable":false}`}, status: websocket.StatusPolicyViolation, code: "provider_rate_limited", retryable: true},
		{name: "bad parameter", open: []string{`{"type":"Error","code":4002,"message":"sample_rate must be one of (8000, 16000)","recoverable":false}`}, status: 4002, code: "invalid_request"},
		{name: "close without metadata", open: []string{}, status: websocket.StatusInternalError, code: "provider_unavailable", retryable: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fake := newFakeListen(t)
			fake.open = tc.open
			fake.closeAfterOpen = tc.status
			_, err := newTestSTT(t, fake, STTConfig{}).Open(context.Background(), sttRequest(fake.endpoint()))
			var providerErr *runtimepkg.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Code != tc.code || providerErr.Retryable != tc.retryable {
				t.Fatalf("Open error = %#v, want %s retryable=%v", err, tc.code, tc.retryable)
			}
		})
	}
}

func TestHandshakeRejectionIsClassified(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"errorCode":40101,"errorMessage":"Invalid API key"}`))
	}))
	t.Cleanup(server.Close)
	parsed, _ := url.Parse(server.URL)
	adapter, err := NewSTT(STTConfig{AllowedEndpointHosts: []string{parsed.Hostname()}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Open(context.Background(), sttRequest("ws"+strings.TrimPrefix(server.URL, "http")+listenPath))
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "authentication_failed" || providerErr.Retryable || providerErr.ProviderStatus != http.StatusUnauthorized {
		t.Fatalf("Open error = %#v", err)
	}
}

func TestMidSessionErrorIsReportedOnceAndRecoverableIsAWarning(t *testing.T) {
	t.Parallel()
	fake := newFakeListen(t)
	fake.onAudio = []string{
		`{"type":"Error","code":4002,"message":"unknown control type 'Finalize'","recoverable":true}`,
		`{"type":"Error","code":1011,"message":"internal error","recoverable":false}`,
	}
	fake.closeAfterAudio = websocket.StatusInternalError
	stream := openSTT(t, fake, STTConfig{}, sttRequest(fake.endpoint()))
	if err := stream.WriteAudio(context.Background(), make([]byte, 640)); err != nil {
		t.Fatal(err)
	}
	events := drainSTT(t, stream)
	var failures []error
	warnings := 0
	for _, event := range events {
		if event.Err != nil {
			failures = append(failures, event.Err)
		}
		if event.Type == protocol.EventWarning {
			warnings++
		}
	}
	if warnings != 1 {
		t.Fatalf("warnings = %d, want the recoverable error as one warning", warnings)
	}
	var providerErr *runtimepkg.ProviderError
	if len(failures) != 1 || !errors.As(failures[0], &providerErr) || providerErr.Code != "provider_unavailable" || !providerErr.Retryable {
		t.Fatalf("failures = %v, want one retryable provider_unavailable", failures)
	}
	if terminal := stream.(runtimepkg.TerminalErrorProviderStream).TerminalError(); terminal == nil {
		t.Fatal("terminal error not preserved")
	}
}

func TestUnexplainedCloseIsAFailure(t *testing.T) {
	t.Parallel()
	fake := newFakeListen(t)
	fake.closeAfterAudio = 4008
	stream := openSTT(t, fake, STTConfig{}, sttRequest(fake.endpoint()))
	if err := stream.WriteAudio(context.Background(), make([]byte, 640)); err != nil {
		t.Fatal(err)
	}
	var failure error
	for _, event := range drainSTT(t, stream) {
		if event.Err != nil {
			failure = event.Err
		}
	}
	var providerErr *runtimepkg.ProviderError
	if !errors.As(failure, &providerErr) || providerErr.Code != "invalid_request" || !strings.Contains(providerErr.Message, "4008") {
		t.Fatalf("failure = %#v", failure)
	}
}

func TestCloseDrainTimeoutReportsTheMissingClose(t *testing.T) {
	t.Parallel()
	fake := newFakeListen(t)
	fake.ignoreCloseStream = true
	stream := openSTT(t, fake, STTConfig{CloseDrainTimeout: 100 * time.Millisecond}, sttRequest(fake.endpoint()))
	if err := stream.WriteAudio(context.Background(), make([]byte, 640)); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	var failure error
	for _, event := range drainSTT(t, stream) {
		if event.Billing != nil && event.Billing.Complete {
			t.Fatal("billing completed without the closing Metadata")
		}
		if event.Err != nil {
			failure = event.Err
		}
	}
	var providerErr *runtimepkg.ProviderError
	if !errors.As(failure, &providerErr) || providerErr.Code != "request_timeout" {
		t.Fatalf("failure = %#v, want request_timeout", failure)
	}
}

func TestSTTLanguageFollowsTheModel(t *testing.T) {
	t.Parallel()
	cases := []struct {
		model, language, want string
		refused               bool
	}{
		{model: DefaultSTTModel, language: "", want: "ar"},
		{model: DefaultSTTModel, language: "ar-SA", want: "ar"},
		{model: DefaultSTTModel, language: "AR_eg", want: "ar"},
		{model: DefaultSTTModel, language: "en", refused: true},
		{model: CodeSwitchSTTModel, language: "", want: "auto"},
		{model: CodeSwitchSTTModel, language: "auto", want: "auto"},
		{model: CodeSwitchSTTModel, language: "en-US", want: "en"},
		{model: CodeSwitchSTTModel, language: "ar-AE", want: "ar"},
		{model: CodeSwitchSTTModel, language: "fr", refused: true},
	}
	for _, tc := range cases {
		got, err := sttLanguage(tc.model, tc.language)
		if tc.refused {
			var providerErr *runtimepkg.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Code != "unsupported_language" {
				t.Fatalf("%s %q: err = %v, want unsupported_language", tc.model, tc.language, err)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Fatalf("%s %q = %q %v, want %q", tc.model, tc.language, got, err, tc.want)
		}
	}
	if _, err := sttLanguage("munsit-v2", "ar"); err == nil {
		t.Fatal("unknown model accepted")
	}
}

func TestOpenRefusesWhatTheSocketCannotServe(t *testing.T) {
	t.Parallel()
	adapter, err := NewSTT(STTConfig{})
	if err != nil {
		t.Fatal(err)
	}
	base := sttRequest("wss://api.munsit.com/api/v1/listen")
	cases := map[string]func(*runtimepkg.AdapterRequest){
		"24 kHz": func(r *runtimepkg.AdapterRequest) {
			r.Media = &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1}
		},
		"stereo": func(r *runtimepkg.AdapterRequest) {
			r.Media = &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 2}
		},
		"english munsit": func(r *runtimepkg.AdapterRequest) { r.Options.Language = "en" },
		"unknown model":  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "munsit-v2" },
		"wrong path":     func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = "wss://api.munsit.com/api/v1/stream" },
		"foreign host":   func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = "wss://example.com/api/v1/listen" },
		"relay key off relay": func(r *runtimepkg.AdapterRequest) {
			r.Plan.Execution.ProviderRoute = protocol.RouteProviderDirect
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
	request := base
	request.Media = &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1}
	_, err = adapter.Open(context.Background(), request)
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "unsupported_media" || providerErr.Hint == "" {
		t.Fatalf("24 kHz error = %#v, want unsupported_media with a hint", err)
	}
}

// The reader is the only sender on Events. Open used to send the opening
// events itself after starting it, which panicked on a closed channel when
// the socket closed right after Metadata, or blocked forever on a one-slot
// buffer that nobody drains until Open returns.
func TestOpenNeverSendsOnEvents(t *testing.T) {
	t.Parallel()
	cases := map[string]websocket.StatusCode{"metadata then close": websocket.StatusInternalError, "metadata and stay open": 0}
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
			var failure error
			for _, event := range drainSTT(t, got.stream) {
				if event.Err != nil {
					failure = event.Err
				}
			}
			var providerErr *runtimepkg.ProviderError
			if !errors.As(failure, &providerErr) || providerErr.Code != "provider_unavailable" || !providerErr.Retryable {
				t.Fatalf("failure = %#v, want a retryable close", failure)
			}
		})
	}
}
