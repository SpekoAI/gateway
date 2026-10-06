package nari

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

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

// fakeRealtime stands in for wss://api.narilabs.com/v1/realtime: it captures
// the configure frame, acknowledges it, and replays scripted events.
type fakeRealtime struct {
	t *testing.T
	// ack answers session.configure; empty means session.configured.
	ack string
	// onAppend is written after the first append arrives.
	onAppend []string
	// onCommit is written after each commit, with the gap between events.
	onCommit []string
	gap      time.Duration
	// closeAfterAppend, when set, closes the socket with this status once the
	// onAppend events are written, as the service does after an error event.
	closeAfterAppend websocket.StatusCode
	server           *httptest.Server

	mu        sync.Mutex
	header    http.Header
	query     url.Values
	configure map[string]any
	appends   [][]byte
	commits   int
}

func newFakeRealtime(t *testing.T) *fakeRealtime {
	fake := &fakeRealtime{t: t}
	mux := http.NewServeMux()
	mux.HandleFunc(realtimePath, fake.handle)
	fake.server = httptest.NewServer(mux)
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeRealtime) endpoint() string {
	return "ws" + strings.TrimPrefix(f.server.URL, "http") + realtimePath
}

func (f *fakeRealtime) host() string {
	parsed, _ := url.Parse(f.server.URL)
	return parsed.Hostname()
}

func (f *fakeRealtime) handle(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set(requestIDHeader, "req-1")
	conn, err := websocket.Accept(writer, request, nil)
	if err != nil {
		return
	}
	defer conn.CloseNow()
	ctx := request.Context()
	_, payload, err := conn.Read(ctx)
	if err != nil {
		return
	}
	var configure map[string]any
	if json.Unmarshal(payload, &configure) != nil || configure["type"] != "session.configure" {
		f.t.Errorf("first frame was not session.configure: %s", payload)
		return
	}
	f.mu.Lock()
	f.header, f.query, f.configure = request.Header.Clone(), request.URL.Query(), configure
	f.mu.Unlock()
	ack := f.ack
	if ack == "" {
		ack = `{"type":"session.configured","session":{"id":"sess-1","model":"qwen3-asr-fast"}}`
	}
	if conn.Write(ctx, websocket.MessageText, []byte(ack)) != nil {
		return
	}
	appended := false
	for {
		_, payload, err := conn.Read(ctx)
		if err != nil {
			return
		}
		var event struct {
			Type  string `json:"type"`
			Audio string `json:"audio"`
		}
		_ = json.Unmarshal(payload, &event)
		switch event.Type {
		case "input_audio_buffer.append":
			audio, _ := base64.StdEncoding.DecodeString(event.Audio)
			f.mu.Lock()
			f.appends = append(f.appends, audio)
			f.mu.Unlock()
			if !appended {
				appended = true
				f.write(ctx, conn, f.onAppend)
				if f.closeAfterAppend != 0 {
					_ = conn.Close(f.closeAfterAppend, "INSUFFICIENT_CREDITS")
					return
				}
			}
		case "input_audio_buffer.commit":
			f.mu.Lock()
			f.commits++
			f.mu.Unlock()
			f.write(ctx, conn, f.onCommit)
		default:
			f.t.Errorf("unexpected client frame: %s", payload)
		}
	}
}

func (f *fakeRealtime) write(ctx context.Context, conn *websocket.Conn, messages []string) {
	for _, message := range messages {
		if f.gap > 0 {
			time.Sleep(f.gap)
		}
		if conn.Write(ctx, websocket.MessageText, []byte(message)) != nil {
			return
		}
	}
}

func sttRequest(endpoint string) runtimepkg.AdapterRequest {
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindSTT,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged},
			Route: protocol.PlanRoute{Provider: ProviderName, Model: DefaultSTTModel, Adapter: STTAdapterID, Transport: protocol.TransportWebSocket, Endpoint: endpoint,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialRelayAccess, Value: "nari-key", ExpiresAt: time.Now().Add(time.Minute)}},
		},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
		Options: protocol.RequestOptions{Language: "en-US"},
	}
}

func newTestSTT(t *testing.T, fake *fakeRealtime) *STTAdapter {
	t.Helper()
	adapter, err := NewSTT(STTConfig{AllowedEndpointHosts: []string{fake.host()}, AllowInsecureEndpoint: true, SetupTimeout: 2 * time.Second, CloseDrainTimeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("NewSTT: %v", err)
	}
	return adapter
}

func openSTT(t *testing.T, fake *fakeRealtime) runtimepkg.ProviderStream {
	t.Helper()
	stream, err := newTestSTT(t, fake).Open(context.Background(), sttRequest(fake.endpoint()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) })
	return stream
}

// drain collects every non-empty event until the stream closes Events.
func drain(t *testing.T, stream runtimepkg.ProviderStream) []runtimepkg.ProviderEvent {
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
			t.Fatalf("Events did not close; got %d events", len(events))
		}
	}
}

func finals(t *testing.T, events []runtimepkg.ProviderEvent) []string {
	t.Helper()
	var texts []string
	for _, event := range events {
		if event.Err != nil {
			t.Fatalf("unexpected error event: %v", event.Err)
		}
		if event.Type == protocol.EventTranscriptFinal {
			var data map[string]any
			_ = json.Unmarshal(event.Data, &data)
			texts = append(texts, data["item_id"].(string)+":"+data["text"].(string))
		}
	}
	return texts
}

func TestOpenSendsFlatConfigureAndWaitsForConfigured(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	stream := openSTT(t, fake)

	fake.mu.Lock()
	header, query, configure := fake.header, fake.query, fake.configure
	fake.mu.Unlock()
	if header.Get("Authorization") != "Bearer nari-key" {
		t.Fatalf("authorization = %q", header.Get("Authorization"))
	}
	if query.Get("intent") != "transcription" {
		t.Fatalf("query = %v", query)
	}
	session, _ := configure["session"].(map[string]any)
	if session["model"] != DefaultSTTModel || session["language"] != "en" {
		t.Fatalf("session = %#v", session)
	}
	if value, present := session["turn_detection"]; !present || value != nil {
		t.Fatalf("turn_detection must be an explicit null, got %#v (present=%v)", value, present)
	}
	for _, want := range []protocol.EventType{protocol.EventSessionReady, protocol.EventUsageObserved} {
		event := <-stream.Events()
		var data map[string]any
		_ = json.Unmarshal(event.Data, &data)
		if event.Type != want || data["provider_request_id"] != "req-1" || data["session_id"] != "sess-1" {
			t.Fatalf("event = %s %s, want %s carrying the handshake request id", event.Type, event.Data, want)
		}
	}
}

func TestPartialsReplaceAndCompletedIsFinal(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.onAppend = []string{
		`{"type":"transcript.partial","item_id":"a","transcript":"how is","revision":1}`,
		`{"type":"transcript.partial","item_id":"a","transcript":"how is the weather","revision":2}`,
	}
	fake.onCommit = []string{
		`{"type":"input_audio_buffer.committed","item_id":"a","commit_reason":"manual"}`,
		`{"type":"transcript.completed","item_id":"a","transcript":"How is the weather?","language":"en","usage":{"input_audio_seconds":1.5},"commit_reason":"manual"}`,
	}
	stream := openSTT(t, fake)
	if err := stream.WriteAudio(context.Background(), make([]byte, 6_400)); err != nil {
		t.Fatal(err)
	}
	if err := stream.CommitAudio(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	events := drain(t, stream)
	var deltas []string
	for _, event := range events {
		if event.Type == protocol.EventTranscriptDelta {
			var data map[string]any
			_ = json.Unmarshal(event.Data, &data)
			deltas = append(deltas, data["text"].(string))
		}
	}
	if strings.Join(deltas, "|") != "how is|how is the weather" {
		t.Fatalf("deltas = %q", deltas)
	}
	if got := finals(t, events); len(got) != 1 || got[0] != "a:How is the weather?" {
		t.Fatalf("finals = %q", got)
	}
	fake.mu.Lock()
	commits := fake.commits
	fake.mu.Unlock()
	if commits != 1 {
		t.Fatalf("Close after an explicit commit must not commit again, got %d commits", commits)
	}
}

// A recording past 36 s is split by the service: the first item is committed
// with reason max_duration and finalizes on its own, and the caller's commit
// closes a second item. Close must wait for both finals, in whichever order
// they land, before tearing the socket down.
func TestCloseDrainsEveryItemOfASplitRecording(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.gap = 50 * time.Millisecond
	fake.onAppend = []string{
		`{"type":"transcript.partial","item_id":"first","transcript":"the first","revision":1}`,
		`{"type":"input_audio_buffer.committed","item_id":"first","commit_reason":"max_duration"}`,
	}
	fake.onCommit = []string{
		`{"type":"input_audio_buffer.committed","item_id":"second","previous_item_id":"first","commit_reason":"manual"}`,
		`{"type":"transcript.completed","item_id":"second","transcript":"second half","commit_reason":"manual"}`,
		`{"type":"transcript.completed","item_id":"first","transcript":"first half","commit_reason":"max_duration"}`,
	}
	stream := openSTT(t, fake)
	if err := stream.WriteAudio(context.Background(), make([]byte, 3_200)); err != nil {
		t.Fatal(err)
	}
	// Give the scripted partial time to open the first item before Close.
	time.Sleep(200 * time.Millisecond)
	if err := stream.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := finals(t, drain(t, stream))
	if strings.Join(got, "|") != "second:second half|first:first half" {
		t.Fatalf("finals = %q, want both items", got)
	}
}

// A final that never arrives is a transcript the caller lost; the drain
// timeout must end the session with an error, not a clean close.
func TestCloseDrainTimeoutReportsTheMissingFinal(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.onCommit = []string{`{"type":"input_audio_buffer.committed","item_id":"owed","commit_reason":"manual"}`}
	adapter, err := NewSTT(STTConfig{AllowedEndpointHosts: []string{fake.host()}, AllowInsecureEndpoint: true, SetupTimeout: 2 * time.Second, CloseDrainTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := adapter.Open(context.Background(), sttRequest(fake.endpoint()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) })
	if err := stream.WriteAudio(context.Background(), make([]byte, 3_200)); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	var providerErr *runtimepkg.ProviderError
	for _, event := range drain(t, stream) {
		if errors.As(event.Err, &providerErr) {
			break
		}
	}
	if providerErr == nil || providerErr.Code != "request_timeout" || providerErr.Retryable || !strings.Contains(providerErr.Message, "1 item") {
		t.Fatalf("terminal error = %v, want non-retryable request_timeout naming the open item", providerErr)
	}
}

func TestCommitEmptyReleasesClose(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.onCommit = []string{`{"type":"input_audio_buffer.commit_empty","item_id":null}`}
	stream := openSTT(t, fake)
	if err := stream.CommitAudio(context.Background()); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := stream.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	events := drain(t, stream)
	if got := finals(t, events); len(got) != 0 {
		t.Fatalf("finals = %q", got)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Close waited %s for a commit that has no final", elapsed)
	}
}

func TestWriteAudioSendsWholeSampleAppendsOfTheRecommendedSize(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	stream := openSTT(t, fake)
	if err := stream.WriteAudio(context.Background(), make([]byte, 7_001)); err != nil {
		t.Fatal(err)
	}
	if err := stream.WriteAudio(context.Background(), make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		fake.mu.Lock()
		sizes := make([]int, 0, len(fake.appends))
		for _, audio := range fake.appends {
			sizes = append(sizes, len(audio))
		}
		fake.mu.Unlock()
		if len(sizes) == 4 {
			if sizes[0] != 3_200 || sizes[1] != 3_200 || sizes[2] != 600 || sizes[3] != 2 {
				t.Fatalf("append sizes = %v", sizes)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("append sizes = %v", sizes)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSetupErrorsFailOpenWithTheirClassification(t *testing.T) {
	t.Parallel()
	cases := []struct {
		code      string
		want      string
		retryable bool
	}{
		{code: "INVALID_API_KEY", want: "authentication_failed"},
		{code: "INSUFFICIENT_CREDITS", want: "provider_quota_exceeded"},
		{code: "MODEL_NOT_FOUND", want: "invalid_request"},
		{code: "CONCURRENCY_LIMIT_EXCEEDED", want: "provider_rate_limited", retryable: true},
		{code: "SERVER_AT_CAPACITY", want: "provider_unavailable", retryable: true},
	}
	for _, tc := range cases {
		t.Run(tc.code, func(t *testing.T) {
			t.Parallel()
			fake := newFakeRealtime(t)
			fake.ack = `{"type":"error","error":{"code":"` + tc.code + `","message":"nope","requestId":"req-x"}}`
			_, err := newTestSTT(t, fake).Open(context.Background(), sttRequest(fake.endpoint()))
			var providerErr *runtimepkg.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Code != tc.want || providerErr.Retryable != tc.retryable {
				t.Fatalf("Open error = %#v, want %s retryable=%v", err, tc.want, tc.retryable)
			}
		})
	}
}

func TestHandshakeRateLimitIsRetryable(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(server.Close)
	parsed, _ := url.Parse(server.URL)
	adapter, err := NewSTT(STTConfig{AllowedEndpointHosts: []string{parsed.Hostname()}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Open(context.Background(), sttRequest("ws"+strings.TrimPrefix(server.URL, "http")+realtimePath))
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "provider_rate_limited" || !providerErr.Retryable || providerErr.ProviderStatus != http.StatusTooManyRequests {
		t.Fatalf("Open error = %#v, want a retryable rate limit", err)
	}
}

func TestMidSessionErrorIsReportedOnce(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.onAppend = []string{`{"type":"error","error":{"code":"INSUFFICIENT_CREDITS","message":"top up"}}`}
	fake.closeAfterAppend = websocket.StatusPolicyViolation
	stream := openSTT(t, fake)
	if err := stream.WriteAudio(context.Background(), make([]byte, 3_200)); err != nil {
		t.Fatal(err)
	}
	var failures []*runtimepkg.ProviderError
	for _, event := range drain(t, stream) {
		var providerErr *runtimepkg.ProviderError
		if errors.As(event.Err, &providerErr) {
			failures = append(failures, providerErr)
		}
	}
	if len(failures) != 1 || failures[0].Code != "provider_quota_exceeded" || failures[0].Retryable {
		t.Fatalf("failures = %#v, want one non-retryable quota error", failures)
	}
}

func TestLanguageCodeFoldsToTheSupportedSet(t *testing.T) {
	t.Parallel()
	for tag, want := range map[string]string{
		"en-US": "en", "EN": "en", "tl": "fil", "fil-PH": "fil", "cmn": "zh", "zh-TW": "zh", "yue": "yue",
		"pt_BR": "pt", "in": "id", "auto": "", "": "", "sw": "",
	} {
		if got := languageCode(tag); got != want {
			t.Errorf("languageCode(%q) = %q, want %q", tag, got, want)
		}
	}
}

func TestOpenRefusesWhatTheSocketCannotServe(t *testing.T) {
	t.Parallel()
	adapter, err := NewSTT(STTConfig{})
	if err != nil {
		t.Fatal(err)
	}
	wrongRate := sttRequest("wss://api.narilabs.com/v1/realtime")
	wrongRate.Media = &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1}
	wrongModel := sttRequest("wss://api.narilabs.com/v1/realtime")
	wrongModel.Plan.Route.Model = "qwen3-asr-flash"
	wrongHost := sttRequest("wss://api.example.com/v1/realtime")
	directRelayAccess := sttRequest("wss://api.narilabs.com/v1/realtime")
	directRelayAccess.Plan.Execution.ProviderRoute = protocol.RouteProviderDirect
	for name, request := range map[string]runtimepkg.AdapterRequest{"rate": wrongRate, "model": wrongModel, "host": wrongHost, "credential": directRelayAccess} {
		if _, err := adapter.Open(context.Background(), request); err == nil {
			t.Errorf("%s: Open succeeded, want refusal", name)
		}
	}
}

// When the service closes without an error event, the close reason carries
// the code and the close status is the fallback.
func TestCloseReasonCarriesTheErrorCode(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.closeAfterAppend = websocket.StatusPolicyViolation
	stream := openSTT(t, fake)
	if err := stream.WriteAudio(context.Background(), make([]byte, 3_200)); err != nil {
		t.Fatal(err)
	}
	var failures []*runtimepkg.ProviderError
	for _, event := range drain(t, stream) {
		var providerErr *runtimepkg.ProviderError
		if errors.As(event.Err, &providerErr) {
			failures = append(failures, providerErr)
		}
	}
	if len(failures) != 1 || failures[0].Code != "provider_quota_exceeded" || failures[0].Retryable {
		t.Fatalf("failures = %#v, want the close reason's classification", failures)
	}
}
