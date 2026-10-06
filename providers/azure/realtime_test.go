package azure

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

// fakeRealtime is a scripted MAI Realtime server. It records the handshake
// and every client frame, acknowledges session.update the way the service
// does (session.created first), and answers each commit with `answer`.
type fakeRealtime struct {
	handshakes chan *http.Request
	frames     chan map[string]any
	// onUpdate replaces the session.updated acknowledgement when set.
	onUpdate func(context.Context, *websocket.Conn)
	// onCommit is called for each input_audio_buffer.commit.
	onCommit func(context.Context, *websocket.Conn, int)
}

func newFakeRealtime(t *testing.T, fake *fakeRealtime) *httptest.Server {
	t.Helper()
	fake.handshakes = make(chan *http.Request, 4)
	fake.frames = make(chan map[string]any, 64)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != realtimePath {
			http.NotFound(writer, request)
			return
		}
		fake.handshakes <- request.Clone(context.Background())
		conn, err := websocket.Accept(writer, request, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		ctx := context.Background()
		defer conn.CloseNow()
		writeFrame(t, ctx, conn, `{"type":"session.created","session":{"id":"sess_mai_1"}}`)
		commits := 0
		for {
			_, payload, err := conn.Read(ctx)
			if err != nil {
				return
			}
			var frame map[string]any
			_ = json.Unmarshal(payload, &frame)
			fake.frames <- frame
			switch frame["type"] {
			case "session.update":
				if fake.onUpdate != nil {
					fake.onUpdate(ctx, conn)
					continue
				}
				writeFrame(t, ctx, conn, `{"type":"session.updated","session":{"id":"sess_mai_1"}}`)
			case "input_audio_buffer.commit":
				commits++
				if fake.onCommit != nil {
					fake.onCommit(ctx, conn, commits)
				}
			}
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func writeFrame(t *testing.T, ctx context.Context, conn *websocket.Conn, payload string) {
	t.Helper()
	if !json.Valid([]byte(payload)) {
		t.Fatalf("fixture is not JSON: %s", payload)
	}
	_ = conn.Write(ctx, websocket.MessageText, []byte(payload))
}

func realtimeRequest(serverURL string) runtimepkg.AdapterRequest {
	endpoint, _ := url.Parse(serverURL)
	endpoint.Scheme = "ws"
	endpoint.Path = realtimePath
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindSTT,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged},
			Route: protocol.PlanRoute{Provider: ProviderName, Model: RealtimeModel, Adapter: RealtimeAdapterID, Transport: protocol.TransportWebSocket, Endpoint: endpoint.String(),
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialRelayAccess, Value: "test-foundry-key", ExpiresAt: time.Now().Add(time.Minute)}},
		},
		Options: protocol.RequestOptions{Language: "pt-BR"},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
	}
}

func realtimeAdapterFor(t *testing.T, serverURL string) *RealtimeAdapter {
	t.Helper()
	endpoint, _ := url.Parse(serverURL)
	adapter, err := NewRealtime(RealtimeConfig{AllowedEndpointHosts: []string{endpoint.Hostname()}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("NewRealtime: %v", err)
	}
	return adapter
}

func nextFrame(t *testing.T, frames <-chan map[string]any, wantType string) map[string]any {
	t.Helper()
	timeout := time.After(3 * time.Second)
	for {
		select {
		case frame := <-frames:
			if frame["type"] == wantType {
				return frame
			}
		case <-timeout:
			t.Fatalf("no %s frame", wantType)
			return nil
		}
	}
}

func collectRealtime(t *testing.T, events <-chan runtimepkg.ProviderEvent, until protocol.EventType) []runtimepkg.ProviderEvent {
	t.Helper()
	var collected []runtimepkg.ProviderEvent
	timeout := time.After(3 * time.Second)
	for {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatalf("events closed after %d events", len(collected))
			}
			if event.Err != nil {
				t.Fatalf("event error: %v", event.Err)
			}
			collected = append(collected, event)
			if event.Type == until {
				return collected
			}
		case <-timeout:
			t.Fatalf("timed out after %d events", len(collected))
		}
	}
}

func eventText(t *testing.T, event runtimepkg.ProviderEvent) string {
	t.Helper()
	var data struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(event.Data, &data); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return data.Text
}

func waitClosed(t *testing.T, events <-chan runtimepkg.ProviderEvent) {
	t.Helper()
	timeout := time.After(3 * time.Second)
	for {
		select {
		case event, ok := <-events:
			if !ok {
				return
			}
			if event.Err != nil {
				t.Fatalf("event error while closing: %v", event.Err)
			}
		case <-timeout:
			t.Fatal("events never closed")
		}
	}
}

func TestRealtimeHandshakeConfiguresATranscriptionSession(t *testing.T) {
	t.Parallel()
	fake := &fakeRealtime{}
	server := newFakeRealtime(t, fake)
	stream, err := realtimeAdapterFor(t, server.URL).Open(context.Background(), realtimeRequest(server.URL))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()
	handshake := <-fake.handshakes
	if handshake.Header.Get("api-key") != "test-foundry-key" || handshake.Header.Get("Authorization") != "" {
		t.Fatalf("auth headers = %v", handshake.Header)
	}
	if handshake.URL.Query().Get("intent") != "transcription" {
		t.Fatalf("query = %q", handshake.URL.RawQuery)
	}
	update := nextFrame(t, fake.frames, "session.update")
	encoded, _ := json.Marshal(update["session"])
	want := `{"audio":{"input":{"format":{"rate":16000,"type":"audio/pcm"},"noise_reduction":null,"transcription":{"language":"pt","model":"MAI-Transcribe-2-Streaming"},"turn_detection":null}},"type":"transcription"}`
	if string(encoded) != want {
		t.Fatalf("session =\n%s\nwant\n%s", encoded, want)
	}
	events := collectRealtime(t, stream.Events(), protocol.EventUsageObserved)
	if !strings.Contains(string(events[0].Data), "sess_mai_1") {
		t.Fatalf("usage = %s", events[0].Data)
	}
}

// The session validates the hint against its own enum (read off the live
// refusal, 2026-10-02), which is not the batch table: Filipino is `tl`,
// Norwegian `no`, and Bengali, Cantonese and the other unlisted codes fail
// the whole session, so they are dropped for auto-detection instead.
func TestRealtimeLanguageHintFollowsTheLiveEnum(t *testing.T) {
	t.Parallel()
	for _, language := range []string{"", "auto", "xx-YY", "bn", "yue", "te-IN", "pa"} {
		if got := realtimeLanguage(language); got != nil {
			t.Fatalf("realtimeLanguage(%q) = %q, want it omitted", language, *got)
		}
	}
	for language, want := range map[string]string{
		"en-US": "en", "pt_BR": "pt", "fil": "tl", "fil-PH": "tl", "tl": "tl",
		"nb": "no", "nn-NO": "no", "he": "he", "iw": "iw", "cmn": "zh", "sr": "sr",
	} {
		if got := realtimeLanguage(language); got == nil || *got != want {
			t.Fatalf("realtimeLanguage(%q) = %v, want %q", language, got, want)
		}
	}
}

// Live, the service refuses `"language": null` (contrary to the docs) with
// the same 400 as an unknown code; auto-detection is the field's absence.
func TestRealtimeSessionUpdateOmitsAnUnsetLanguage(t *testing.T) {
	t.Parallel()
	fake := &fakeRealtime{}
	server := newFakeRealtime(t, fake)
	request := realtimeRequest(server.URL)
	request.Options.Language = ""
	stream, err := realtimeAdapterFor(t, server.URL).Open(context.Background(), request)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()
	update := nextFrame(t, fake.frames, "session.update")
	encoded, _ := json.Marshal(update)
	if strings.Contains(string(encoded), `"language"`) {
		t.Fatalf("session.update carries a language field without a hint: %s", encoded)
	}
}

func TestRealtimeOpenFailsWhenTheSessionIsRefused(t *testing.T) {
	t.Parallel()
	fake := &fakeRealtime{onUpdate: func(ctx context.Context, conn *websocket.Conn) {
		_ = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"error","error":{"type":"invalid_request_error","code":"DeploymentNotFound","message":"The deployment does not exist"}}`))
	}}
	server := newFakeRealtime(t, fake)
	_, err := realtimeAdapterFor(t, server.URL).Open(context.Background(), realtimeRequest(server.URL))
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "invalid_request" || providerErr.Retryable || !strings.Contains(providerErr.Message, "deployment does not exist") {
		t.Fatalf("err = %#v", err)
	}
}

func TestRealtimeMapsDeltasAndIntermediatesToCumulativePartialsThenFinal(t *testing.T) {
	t.Parallel()
	fake := &fakeRealtime{onCommit: func(ctx context.Context, conn *websocket.Conn, commit int) {
		if commit == 1 {
			for _, frame := range []string{
				`{"type":"conversation.item.input_audio_transcription.delta","item_id":"item_1","delta":"Hello"}`,
				`{"type":"conversation.item.input_audio_transcription.intermediate","item_id":"item_1","intermediate":" world"}`,
				`{"type":"conversation.item.input_audio_transcription.intermediate","item_id":"item_1","intermediate":" there"}`,
				`{"type":"conversation.item.input_audio_transcription.delta","item_id":"item_1","delta":" there!"}`,
				`{"type":"input_audio_buffer.committed"}`,
				`{"type":"conversation.item.input_audio_transcription.completed","item_id":"item_1","transcript":"Hello there!"}`,
			} {
				_ = conn.Write(ctx, websocket.MessageText, []byte(frame))
			}
			return
		}
		_ = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"conversation.item.input_audio_transcription.intermediate","item_id":"item_2","intermediate":"Next"}`))
		_ = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"conversation.item.input_audio_transcription.completed","item_id":"item_2","transcript":"Next one."}`))
	}}
	server := newFakeRealtime(t, fake)
	stream, err := realtimeAdapterFor(t, server.URL).Open(context.Background(), realtimeRequest(server.URL))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := stream.WriteAudio(context.Background(), make([]byte, 3*realtimePCMFrameBytes/2)); err != nil {
		t.Fatalf("WriteAudio: %v", err)
	}
	if frame := nextFrame(t, fake.frames, "input_audio_buffer.append"); frame["audio"] == "" {
		t.Fatal("append carries no audio")
	}
	nextFrame(t, fake.frames, "input_audio_buffer.append")
	if err := stream.CommitAudio(context.Background()); err != nil {
		t.Fatalf("CommitAudio: %v", err)
	}
	events := collectRealtime(t, stream.Events(), protocol.EventTranscriptFinal)
	var got []string
	for _, event := range events {
		if event.Type == protocol.EventTranscriptDelta || event.Type == protocol.EventTranscriptFinal {
			got = append(got, string(event.Type)+":"+eventText(t, event))
		}
	}
	want := []string{"transcript.delta:Hello", "transcript.delta:Hello world", "transcript.delta:Hello there", "transcript.delta:Hello there!", "transcript.final:Hello there!"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("events = %v\nwant %v", got, want)
	}
	// The second turn starts from an empty partial.
	_ = stream.WriteAudio(context.Background(), make([]byte, 320))
	_ = stream.CommitAudio(context.Background())
	events = collectRealtime(t, stream.Events(), protocol.EventTranscriptFinal)
	if first := events[0]; first.Type != protocol.EventTranscriptDelta || eventText(t, first) != "Next" {
		t.Fatalf("second turn opened with %s %q", first.Type, eventText(t, first))
	}
	_ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background())
}

func TestRealtimeCloseCommitsTrailingAudioAndWaitsForItsFinal(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	fake := &fakeRealtime{onCommit: func(ctx context.Context, conn *websocket.Conn, commit int) {
		// The runtime's commit is answered late, after Close has committed the
		// trailing audio: a stale `completed` must not release Close early.
		if commit == 2 {
			<-release
			_ = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"conversation.item.input_audio_transcription.completed","transcript":"first"}`))
			_ = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"conversation.item.input_audio_transcription.completed","transcript":"trailing"}`))
		}
	}}
	server := newFakeRealtime(t, fake)
	stream, err := realtimeAdapterFor(t, server.URL).Open(context.Background(), realtimeRequest(server.URL))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = stream.WriteAudio(context.Background(), make([]byte, 320))
	_ = stream.CommitAudio(context.Background())
	_ = stream.WriteAudio(context.Background(), make([]byte, 320))
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	nextFrame(t, fake.frames, "input_audio_buffer.commit")
	nextFrame(t, fake.frames, "input_audio_buffer.commit")
	close(release)
	var finals []string
	timeout := time.After(3 * time.Second)
	for done := false; !done; {
		select {
		case event, ok := <-stream.Events():
			if !ok {
				done = true
				break
			}
			if event.Err != nil {
				t.Fatalf("event error: %v", event.Err)
			}
			if event.Type == protocol.EventTranscriptFinal {
				finals = append(finals, eventText(t, event))
			}
		case <-timeout:
			t.Fatal("Close never released the stream")
		}
	}
	if strings.Join(finals, "|") != "first|trailing" {
		t.Fatalf("finals = %v", finals)
	}
	_ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background())
}

func TestRealtimeCloseWithNothingBufferedReleasesImmediately(t *testing.T) {
	t.Parallel()
	server := newFakeRealtime(t, &fakeRealtime{})
	stream, err := realtimeAdapterFor(t, server.URL).Open(context.Background(), realtimeRequest(server.URL))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	waitClosed(t, stream.Events())
	_ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background())
}

func TestRealtimeEmptyCommitIsASkippedTurnNotAFailure(t *testing.T) {
	t.Parallel()
	fake := &fakeRealtime{onCommit: func(ctx context.Context, conn *websocket.Conn, commit int) {
		if commit == 1 {
			_ = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"error","error":{"type":"invalid_request_error","code":"input_audio_buffer_commit_empty","message":"buffer too small"}}`))
			return
		}
		_ = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"conversation.item.input_audio_transcription.completed","transcript":"still alive"}`))
	}}
	server := newFakeRealtime(t, fake)
	stream, err := realtimeAdapterFor(t, server.URL).Open(context.Background(), realtimeRequest(server.URL))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = stream.CommitAudio(context.Background())
	collectRealtime(t, stream.Events(), protocol.EventWarning)
	_ = stream.WriteAudio(context.Background(), make([]byte, 320))
	_ = stream.CommitAudio(context.Background())
	events := collectRealtime(t, stream.Events(), protocol.EventTranscriptFinal)
	if text := eventText(t, events[len(events)-1]); text != "still alive" {
		t.Fatalf("final = %q", text)
	}
	_ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background())
}

func TestRealtimeTranscriptionFailureIsTerminal(t *testing.T) {
	t.Parallel()
	stream := &realtimeStream{ctx: context.Background(), events: make(chan runtimepkg.ProviderEvent, 4)}
	err := stream.handleMessage([]byte(`{"type":"conversation.item.input_audio_transcription.failed","error":{"type":"server_error","message":"decoder crashed"}}`))
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || !providerErr.Retryable || !strings.Contains(providerErr.Message, "decoder crashed") {
		t.Fatalf("err = %#v", err)
	}
	if err := stream.handleMessage([]byte(`{"type":"error","error":{"code":"invalid_api_key","message":"bad key"}}`)); !errors.As(err, &providerErr) || providerErr.Code != "authentication_failed" || providerErr.Retryable {
		t.Fatalf("auth err = %#v", err)
	}
}

func TestRealtimeRefusesMismatchedPlans(t *testing.T) {
	t.Parallel()
	server := newFakeRealtime(t, &fakeRealtime{})
	adapter := realtimeAdapterFor(t, server.URL)
	for name, mutate := range map[string]func(*runtimepkg.AdapterRequest){
		"model":     func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = BatchModel },
		"provider":  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Provider = "openai" },
		"transport": func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Transport = protocol.TransportHTTP },
		"rate":      func(r *runtimepkg.AdapterRequest) { r.Media.SampleRateHz = 8_000 },
		"stereo":    func(r *runtimepkg.AdapterRequest) { r.Media.Channels = 2 },
		"kind":      func(r *runtimepkg.AdapterRequest) { r.Kind = protocol.SessionKindTTS },
		"path": func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Endpoint = strings.Replace(r.Plan.Route.Endpoint, realtimePath, "/openai/realtime", 1)
		},
		"credential": func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential = nil },
	} {
		request := realtimeRequest(server.URL)
		mutate(&request)
		if _, err := adapter.Open(context.Background(), request); err == nil {
			t.Fatalf("%s: Open accepted a mismatched plan", name)
		}
	}
}

func TestRealtimeEndpointPolicyAdmitsOnlyFoundryResourceHosts(t *testing.T) {
	t.Parallel()
	adapter, err := NewRealtime(RealtimeConfig{})
	if err != nil {
		t.Fatalf("NewRealtime: %v", err)
	}
	if _, err := adapter.endpointPolicy.Parse("wss://speko-mai.services.ai.azure.com" + realtimePath); err != nil {
		t.Fatalf("foundry host refused: %v", err)
	}
	for _, raw := range []string{
		"wss://services.ai.azure.com" + realtimePath,
		"wss://speko-mai.services.ai.azure.com.evil.test" + realtimePath,
		"wss://eastus.api.cognitive.microsoft.com" + realtimePath,
		"ws://speko-mai.services.ai.azure.com" + realtimePath,
	} {
		if _, err := adapter.endpointPolicy.Parse(raw); err == nil {
			t.Fatalf("%s accepted", raw)
		}
	}
}

// The frame sequence the live service sent on 2026-10-02 (centralus,
// MAI-Transcribe-2-Streaming 2026-08-06). Partials stay cumulative and never
// shrink to the finalized text alone while the delta matches the suffix, a
// respelled fragment ("Speaco" -> "Speako.") takes the delta's spelling, and
// the final is the vendor's transcript.
func TestRealtimeReplaysTheLiveTrace(t *testing.T) {
	t.Parallel()
	stream := &realtimeStream{ctx: context.Background(), cancel: func() {}, events: make(chan runtimepkg.ProviderEvent, 64)}
	frames := []string{
		`{"type":"conversation.item.input_audio_transcription.intermediate","intermediate":"Hello."}`,
		`{"type":"conversation.item.input_audio_transcription.intermediate","intermediate":"Hello from Speaco. The quick"}`,
		`{"type":"conversation.item.input_audio_transcription.delta","delta":"Hello from"}`,
		`{"type":"conversation.item.input_audio_transcription.intermediate","intermediate":" Speaco. The quick brown"}`,
		`{"type":"conversation.item.input_audio_transcription.delta","delta":" Spe"}`,
		`{"type":"conversation.item.input_audio_transcription.intermediate","intermediate":"aco. The quick brown fox"}`,
		`{"type":"conversation.item.input_audio_transcription.delta","delta":"ako."}`,
		`{"type":"conversation.item.input_audio_transcription.intermediate","intermediate":" The quick brown fox jumped"}`,
		`{"type":"input_audio_buffer.committed"}`,
		`{"type":"conversation.item.added"}`,
		`{"type":"conversation.item.done"}`,
		`{"type":"conversation.item.input_audio_transcription.delta","delta":" The quick brown fox jumps over the lazy dog twice."}`,
		`{"type":"conversation.item.input_audio_transcription.completed","transcript":"Hello from Speako. The quick brown fox jumps over the lazy dog twice."}`,
	}
	stream.outstandingCommits.Store(1)
	for _, frame := range frames {
		if err := stream.handleMessage([]byte(frame)); err != nil {
			t.Fatalf("handleMessage(%s): %v", frame, err)
		}
	}
	close(stream.events)
	var got []string
	for event := range stream.events {
		if event.Type == protocol.EventWarning {
			t.Fatalf("documented frame produced a warning: %s", event.Data)
		}
		got = append(got, string(event.Type)+":"+eventText(t, event))
	}
	want := []string{
		"transcript.delta:Hello.",
		"transcript.delta:Hello from Speaco. The quick",
		"transcript.delta:Hello from Speaco. The quick",
		"transcript.delta:Hello from Speaco. The quick brown",
		"transcript.delta:Hello from Speaco. The quick brown",
		"transcript.delta:Hello from Speaco. The quick brown fox",
		"transcript.delta:Hello from Speako.",
		"transcript.delta:Hello from Speako. The quick brown fox jumped",
		"transcript.delta:Hello from Speako. The quick brown fox jumps over the lazy dog twice.",
		"transcript.final:Hello from Speako. The quick brown fox jumps over the lazy dog twice.",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("events =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// The live refusal of a commit over less than 100 ms of audio.
func TestRealtimeRecognizesTheLiveEmptyCommitRefusal(t *testing.T) {
	t.Parallel()
	detail := &realtimeErrorDetail{Type: "invalid_request_error", Code: "input_audio_buffer_commit_empty", Message: "Error committing input audio buffer: buffer too small. Expected at least 100ms of audio, but buffer only has 0.00ms of audio."}
	if !realtimeIsEmptyCommit(detail) {
		t.Fatal("the live empty-commit refusal is not recognized")
	}
}

func openRealtimeWithCloseTimeout(t *testing.T, serverURL string, closeTimeout time.Duration) runtimepkg.ProviderStream {
	t.Helper()
	endpoint, _ := url.Parse(serverURL)
	adapter, err := NewRealtime(RealtimeConfig{AllowedEndpointHosts: []string{endpoint.Hostname()}, AllowInsecureEndpoint: true, CloseTimeout: closeTimeout})
	if err != nil {
		t.Fatalf("NewRealtime: %v", err)
	}
	stream, err := adapter.Open(context.Background(), realtimeRequest(serverURL))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return stream
}

func terminalError(t *testing.T, events <-chan runtimepkg.ProviderEvent) error {
	t.Helper()
	timeout := time.After(3 * time.Second)
	for {
		select {
		case event, ok := <-events:
			if !ok {
				return nil
			}
			if event.Err != nil {
				return event.Err
			}
		case <-timeout:
			t.Fatal("stream neither failed nor closed")
			return nil
		}
	}
}

// Azure owes a final for the commit Close sent and never sends it. Close
// must not park the caller until the session lease runs out: the stream
// fails with an error naming the missing final once CloseTimeout passes.
func TestRealtimeCloseBoundsTheWaitForAMissingFinal(t *testing.T) {
	t.Parallel()
	server := newFakeRealtime(t, &fakeRealtime{}) // never answers a commit
	stream := openRealtimeWithCloseTimeout(t, server.URL, 200*time.Millisecond)
	_ = stream.WriteAudio(context.Background(), make([]byte, 3200))
	started := time.Now()
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	err := terminalError(t, stream.Events())
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || !strings.Contains(providerErr.Message, "no final") {
		t.Fatalf("err = %v, want a missing-final provider error", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("Close waited %s for a final it was never sent", elapsed)
	}
	_ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background())
}

// A socket that ends after Close while a final is still owed lost the
// caller's last words; ending Events cleanly would report that as success.
func TestRealtimeDisconnectWhileAFinalIsOwedIsAnError(t *testing.T) {
	t.Parallel()
	fake := &fakeRealtime{onCommit: func(_ context.Context, conn *websocket.Conn, _ int) {
		_ = conn.Close(websocket.StatusNormalClosure, "bye")
	}}
	server := newFakeRealtime(t, fake)
	stream := openRealtimeWithCloseTimeout(t, server.URL, 5*time.Second)
	_ = stream.WriteAudio(context.Background(), make([]byte, 3200))
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	err := terminalError(t, stream.Events())
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || !strings.Contains(providerErr.Message, "before the final") {
		t.Fatalf("err = %v, want a lost-final provider error", err)
	}
	_ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background())
}
