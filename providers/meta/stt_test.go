package meta

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

// fakeRealtime is a stand-in for wss://api.meta.ai/v1/asr/realtime: it
// captures the handshake, acknowledges it, and replays scripted events.
type fakeRealtime struct {
	t *testing.T
	// replies are written after the first binary audio frame arrives.
	replies []string
	// closingReplies are written after the client's endStream, before the
	// server closes the socket normally.
	closingReplies []string
	// ackWith overrides the acknowledgement frame when non-empty.
	ackWith string
	server  *httptest.Server

	mu        sync.Mutex
	handshake map[string]any
	audio     [][]byte
	endStream bool
}

func newFakeRealtime(t *testing.T, replies ...string) *fakeRealtime {
	fake := &fakeRealtime{t: t, replies: replies}
	mux := http.NewServeMux()
	mux.HandleFunc(socketPath, fake.handle)
	fake.server = httptest.NewServer(mux)
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeRealtime) endpoint() string {
	return "ws" + strings.TrimPrefix(f.server.URL, "http") + socketPath
}

func (f *fakeRealtime) host() string {
	parsed, _ := url.Parse(f.server.URL)
	return parsed.Hostname()
}

func (f *fakeRealtime) handle(writer http.ResponseWriter, request *http.Request) {
	conn, err := websocket.Accept(writer, request, nil)
	if err != nil {
		return
	}
	defer conn.CloseNow()
	ctx := request.Context()
	kind, payload, err := conn.Read(ctx)
	if err != nil {
		return
	}
	if kind != websocket.MessageText {
		f.t.Errorf("handshake must be a text frame, got %v", kind)
		return
	}
	var handshake map[string]any
	if json.Unmarshal(payload, &handshake) != nil || handshake["authorization"] == nil {
		f.t.Errorf("first frame was not the handshake: %s", payload)
		return
	}
	f.mu.Lock()
	f.handshake = handshake
	f.mu.Unlock()
	ack := f.ackWith
	if ack == "" {
		ack = `{"sessionId":"sess-1"}`
	}
	if conn.Write(ctx, websocket.MessageText, []byte(ack)) != nil {
		return
	}
	repliesSent := false
	for {
		kind, payload, err := conn.Read(ctx)
		if err != nil {
			return
		}
		if kind == websocket.MessageBinary {
			f.mu.Lock()
			f.audio = append(f.audio, append([]byte(nil), payload...))
			f.mu.Unlock()
			if !repliesSent {
				repliesSent = true
				for _, message := range f.replies {
					if conn.Write(ctx, websocket.MessageText, []byte(message)) != nil {
						return
					}
				}
			}
			continue
		}
		var control struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(payload, &control) != nil || control.Type != clientEndStream {
			f.t.Errorf("unexpected text frame from client: %s", payload)
			continue
		}
		f.mu.Lock()
		f.endStream = true
		f.mu.Unlock()
		for _, message := range f.closingReplies {
			if conn.Write(ctx, websocket.MessageText, []byte(message)) != nil {
				return
			}
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
		return
	}
}

func realtimeRequest(endpoint string) runtimepkg.AdapterRequest {
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindSTT,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged},
			Route: protocol.PlanRoute{Provider: ProviderName, Model: DefaultModel, Adapter: STTAdapterID, Transport: protocol.TransportWebSocket, Endpoint: endpoint,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "test-api-key", ExpiresAt: time.Now().Add(time.Minute)}},
		},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1},
		Options: protocol.RequestOptions{},
	}
}

func openRealtime(t *testing.T, fake *fakeRealtime, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, context.Context) {
	t.Helper()
	adapter, err := NewSTT(STTConfig{AllowedEndpointHosts: []string{fake.host()}, AllowInsecureEndpoint: true, SetupTimeout: 5 * time.Second, CloseDrainTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("NewSTT: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	stream, err := adapter.Open(ctx, request)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { stream.Close(context.Background()) })
	return stream, ctx
}

func expectEvents(t *testing.T, stream runtimepkg.ProviderStream, ctx context.Context, want []protocol.EventType) []runtimepkg.ProviderEvent {
	t.Helper()
	got := make([]runtimepkg.ProviderEvent, 0, len(want))
	for index, wantType := range want {
		select {
		case event := <-stream.Events():
			if event.Err != nil {
				t.Fatalf("event %d failed: %v, want %q", index, event.Err, wantType)
			}
			if event.Type != wantType {
				t.Fatalf("event %d = %q, want %q", index, event.Type, wantType)
			}
			got = append(got, event)
		case <-ctx.Done():
			t.Fatalf("timed out waiting for event %d (%s)", index, wantType)
		}
	}
	return got
}

func eventData(t *testing.T, event runtimepkg.ProviderEvent) map[string]any {
	t.Helper()
	var data map[string]any
	if err := json.Unmarshal(event.Data, &data); err != nil {
		t.Fatalf("event data: %v", err)
	}
	return data
}

// Cumulative partials are published as deltas and speechComplete as the
// turn's final, with the VAD boundaries around them.
func TestTranscribeStreamsPartialsThenTurnFinal(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t,
		`{"type":"speechStart","turnId":1,"audioProcessedMs":1200}`,
		`{"type":"transcript","transcript":"how is","final":false,"audioProcessedMs":2000}`,
		`{"type":"transcript","transcript":"how is the weather","final":false,"audioProcessedMs":2400}`,
		`{"type":"speechEnd","turnId":1,"audioProcessedMs":3600}`,
		`{"type":"speechComplete","turnId":1,"transcript":"How is the weather?","audioProcessedMs":3600}`,
	)
	request := realtimeRequest(fake.endpoint())
	request.Options.Language = "en-US"
	request.Options.STT = &protocol.SttOptions{Keywords: []string{"Speko", "  ", "relay"}}
	stream, ctx := openRealtime(t, fake, request)

	if err := stream.WriteAudio(ctx, []byte("caller-pcm")); err != nil {
		t.Fatalf("WriteAudio: %v", err)
	}
	if err := stream.CommitAudio(ctx); err != nil {
		t.Fatalf("CommitAudio: %v", err)
	}
	events := expectEvents(t, stream, ctx, []protocol.EventType{
		protocol.EventSessionReady, protocol.EventUsageObserved,
		protocol.EventSpeechStarted,
		protocol.EventTranscriptDelta, protocol.EventTranscriptDelta,
		protocol.EventSpeechEnded,
		protocol.EventTranscriptFinal,
	})
	if data := eventData(t, events[0]); data["provider_request_id"] != "sess-1" {
		t.Fatalf("session.ready = %v, want the acknowledged sessionId", data)
	}
	if data := eventData(t, events[2]); data["turn_id"] != float64(1) || data["audio_end_ms"] != float64(1200) {
		t.Fatalf("speech.started = %v", data)
	}
	if data := eventData(t, events[3]); data["text"] != "how is" || data["is_final"] != false {
		t.Fatalf("first delta = %v", data)
	}
	if data := eventData(t, events[4]); data["text"] != "how is the weather" {
		t.Fatalf("second delta = %v, want the cumulative partial as sent", data)
	}
	if data := eventData(t, events[5]); data["reason"] != "end_of_turn" || data["turn_id"] != float64(1) {
		t.Fatalf("speech.ended = %v", data)
	}
	final := eventData(t, events[6])
	if final["text"] != "How is the weather?" || final["is_final"] != true || final["turn_id"] != float64(1) || final["provider_request_id"] != "sess-1" {
		t.Fatalf("final = %v, want the cleaned speechComplete text", final)
	}
	if _, present := final["speaker"]; present {
		t.Fatalf("final = %v, want no speaker outside diarization", final)
	}
	if _, present := events[6].Extensions[extensionID]; !present {
		t.Fatal("final carries no raw vendor frame under the extension id")
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	authorization, _ := fake.handshake["authorization"].(map[string]any)
	if authorization["accessToken"] != "Bearer test-api-key" {
		t.Fatalf("authorization = %v, want the bearer inside the handshake", fake.handshake["authorization"])
	}
	if fake.handshake["audioEncoding"] != encoding24k {
		t.Fatalf("audioEncoding = %v for 24 kHz media", fake.handshake["audioEncoding"])
	}
	if fake.handshake["model"] != DefaultModel {
		t.Fatalf("model = %v", fake.handshake["model"])
	}
	if fake.handshake["mode"] != modeEndpointing {
		t.Fatalf("mode = %v, want the service to detect turns", fake.handshake["mode"])
	}
	if fake.handshake["partialMode"] != partialCumulative {
		t.Fatalf("partialMode = %v", fake.handshake["partialMode"])
	}
	if fake.handshake["emitAudioProgress"] != false {
		t.Fatalf("emitAudioProgress = %v", fake.handshake["emitAudioProgress"])
	}
	if bias, _ := fake.handshake["languageBias"].([]any); len(bias) != 1 || bias[0] != "English" {
		t.Fatalf("languageBias = %v, want the BCP-47 tag translated to the documented name", fake.handshake["languageBias"])
	}
	if keywords, _ := fake.handshake["keywords"].([]any); len(keywords) != 2 || keywords[0] != "Speko" || keywords[1] != "relay" {
		t.Fatalf("keywords = %v, want blank keywords dropped", fake.handshake["keywords"])
	}
	if len(fake.audio) != 1 || string(fake.audio[0]) != "caller-pcm" {
		t.Fatalf("audio frames = %q, want the PCM forwarded as one binary frame", fake.audio)
	}
	if fake.endStream {
		t.Fatal("endStream was sent before Close")
	}
}

// A diarization ask selects the DIARIZATION mode and the speaker label rides
// the turn's transcript events.
func TestTranscribeDiarizationCarriesTheSpeakerLabel(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t,
		`{"type":"speechStart","turnId":1,"audioProcessedMs":100}`,
		`{"type":"speaker","label":"A","audioProcessedMs":480}`,
		`{"type":"transcript","transcript":"hello","final":false,"audioProcessedMs":600}`,
		`{"type":"speechComplete","turnId":1,"transcript":"Hello.","audioProcessedMs":900}`,
		`{"type":"speechStart","turnId":2,"audioProcessedMs":1500}`,
		`{"type":"speechComplete","turnId":2,"transcript":"Hi there.","audioProcessedMs":2200}`,
	)
	request := realtimeRequest(fake.endpoint())
	request.Media.SampleRateHz = 16_000
	diarize := true
	request.Options.STT = &protocol.SttOptions{Diarization: &diarize}
	stream, ctx := openRealtime(t, fake, request)
	if err := stream.WriteAudio(ctx, []byte("pcm")); err != nil {
		t.Fatalf("WriteAudio: %v", err)
	}
	events := expectEvents(t, stream, ctx, []protocol.EventType{
		protocol.EventSessionReady, protocol.EventUsageObserved,
		protocol.EventSpeechStarted, protocol.EventTranscriptDelta, protocol.EventTranscriptFinal,
		protocol.EventSpeechStarted, protocol.EventTranscriptFinal,
	})
	if data := eventData(t, events[3]); data["speaker"] != "A" {
		t.Fatalf("delta = %v, want speaker A", data)
	}
	if data := eventData(t, events[4]); data["speaker"] != "A" || data["text"] != "Hello." {
		t.Fatalf("final = %v", data)
	}
	if data := eventData(t, events[6]); data["turn_id"] != float64(2) {
		t.Fatalf("second final = %v", data)
	}
	if _, present := eventData(t, events[6])["speaker"]; present {
		t.Fatalf("second final = %v, want the label cleared between turns", eventData(t, events[6]))
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.handshake["mode"] != modeDiarization {
		t.Fatalf("mode = %v", fake.handshake["mode"])
	}
	if fake.handshake["audioEncoding"] != encoding16k {
		t.Fatalf("audioEncoding = %v for 16 kHz media", fake.handshake["audioEncoding"])
	}
	if _, present := fake.handshake["languageBias"]; present {
		t.Fatalf("languageBias = %v, want absent without a language ask", fake.handshake["languageBias"])
	}
}

// A diarized turn that ends without text still releases its speaker label, so
// the next turn is not attributed to the previous speaker.
func TestTranscribeSilentDiarizedTurnReleasesTheSpeaker(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t,
		`{"type":"speechStart","turnId":1,"audioProcessedMs":100}`,
		`{"type":"speaker","label":"A","audioProcessedMs":200}`,
		`{"type":"speechComplete","turnId":1,"transcript":"","audioProcessedMs":400}`,
		`{"type":"speechStart","turnId":2,"audioProcessedMs":900}`,
		`{"type":"transcript","transcript":"hi","final":false,"audioProcessedMs":1100}`,
		`{"type":"speechComplete","turnId":2,"transcript":"Hi.","audioProcessedMs":1300}`,
	)
	request := realtimeRequest(fake.endpoint())
	diarize := true
	request.Options.STT = &protocol.SttOptions{Diarization: &diarize}
	stream, ctx := openRealtime(t, fake, request)
	if err := stream.WriteAudio(ctx, []byte("pcm")); err != nil {
		t.Fatalf("WriteAudio: %v", err)
	}
	events := expectEvents(t, stream, ctx, []protocol.EventType{
		protocol.EventSessionReady, protocol.EventUsageObserved,
		protocol.EventSpeechStarted, protocol.EventSpeechStarted,
		protocol.EventTranscriptDelta, protocol.EventTranscriptFinal,
	})
	for _, event := range events[4:] {
		if data := eventData(t, event); data["speaker"] != nil {
			t.Fatalf("event %s = %v, want no speaker carried over from the silent turn", event.Type, data)
		}
	}
}

// A turn whose speechComplete arrives empty falls back to its last partial;
// a turn that carried no text at all publishes no final.
func TestTranscribeFallsBackToTheLastPartialAndSuppressesEmptyTurns(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t,
		`{"type":"speechStart","turnId":1,"audioProcessedMs":100}`,
		`{"type":"speechEnd","turnId":1,"audioProcessedMs":300}`,
		`{"type":"speechComplete","turnId":1,"transcript":"","audioProcessedMs":300}`,
		`{"type":"speechStart","turnId":2,"audioProcessedMs":500}`,
		`{"type":"transcript","transcript":"okay then","final":false,"audioProcessedMs":800}`,
		`{"type":"speechComplete","turnId":2,"transcript":"  ","audioProcessedMs":900}`,
		`{"type":"transcript","transcript":"pushed","final":true,"audioProcessedMs":1200}`,
	)
	stream, ctx := openRealtime(t, fake, realtimeRequest(fake.endpoint()))
	if err := stream.WriteAudio(ctx, []byte("pcm")); err != nil {
		t.Fatalf("WriteAudio: %v", err)
	}
	events := expectEvents(t, stream, ctx, []protocol.EventType{
		protocol.EventSessionReady, protocol.EventUsageObserved,
		protocol.EventSpeechStarted, protocol.EventSpeechEnded,
		protocol.EventSpeechStarted, protocol.EventTranscriptDelta, protocol.EventTranscriptFinal,
		protocol.EventTranscriptFinal,
	})
	if data := eventData(t, events[6]); data["text"] != "okay then" {
		t.Fatalf("final = %v, want the last cumulative partial", data)
	}
	if data := eventData(t, events[7]); data["text"] != "pushed" || data["is_final"] != true {
		t.Fatalf("final-flagged transcript = %v, want honored as a final", data)
	}
}

// Close sends endStream and keeps the socket open for the trailing turn, then
// the event channel closes cleanly once the service hangs up.
func TestTranscribeDrainsTheTrailingFinalAfterClose(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.closingReplies = []string{`{"type":"speechComplete","turnId":1,"transcript":"Last words.","audioProcessedMs":4000}`}
	stream, ctx := openRealtime(t, fake, realtimeRequest(fake.endpoint()))
	if err := stream.WriteAudio(ctx, []byte("pcm")); err != nil {
		t.Fatalf("WriteAudio: %v", err)
	}
	expectEvents(t, stream, ctx, []protocol.EventType{protocol.EventSessionReady, protocol.EventUsageObserved})
	if err := stream.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := stream.WriteAudio(ctx, []byte("late")); !errors.Is(err, runtimepkg.ErrSessionClosed) {
		t.Fatalf("WriteAudio after Close = %v, want ErrSessionClosed", err)
	}
	events := expectEvents(t, stream, ctx, []protocol.EventType{protocol.EventTranscriptFinal})
	if data := eventData(t, events[0]); data["text"] != "Last words." {
		t.Fatalf("final = %v", data)
	}
	select {
	case event, open := <-stream.Events():
		if open {
			t.Fatalf("unexpected event after the service closed: %+v", event)
		}
	case <-ctx.Done():
		t.Fatal("events channel did not close after the service hung up")
	}
	if terminal, ok := stream.(runtimepkg.TerminalErrorProviderStream); ok && terminal.TerminalError() != nil {
		t.Fatalf("TerminalError = %v, want none after a normal close", terminal.TerminalError())
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if !fake.endStream {
		t.Fatal("Close did not send endStream")
	}
}

// An error frame is terminal and is preserved past the event queue.
func TestTranscribeErrorFrameIsTerminal(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t, `{"type":"error","message":"audio too quiet","sessionId":"sess-1"}`)
	stream, ctx := openRealtime(t, fake, realtimeRequest(fake.endpoint()))
	if err := stream.WriteAudio(ctx, []byte("pcm")); err != nil {
		t.Fatalf("WriteAudio: %v", err)
	}
	expectEvents(t, stream, ctx, []protocol.EventType{protocol.EventSessionReady, protocol.EventUsageObserved})
	select {
	case event := <-stream.Events():
		var failure *runtimepkg.ProviderError
		if !errors.As(event.Err, &failure) || failure.Code != "provider_rejected_request" || failure.Hint != "audio too quiet" {
			t.Fatalf("event = %+v, want a rejected-request error carrying the message", event)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for the error")
	}
	if stream.(runtimepkg.TerminalErrorProviderStream).TerminalError() == nil {
		t.Fatal("TerminalError = nil after an error frame")
	}
}

// An error instead of the acknowledgement fails Open rather than the timeout.
func TestTranscribeOpenFailsOnARefusedHandshake(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.ackWith = `{"type":"error","message":"invalid api key"}`
	adapter, err := NewSTT(STTConfig{AllowedEndpointHosts: []string{fake.host()}, AllowInsecureEndpoint: true, SetupTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("NewSTT: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := time.Now()
	_, err = adapter.Open(ctx, realtimeRequest(fake.endpoint()))
	var failure *runtimepkg.ProviderError
	if !errors.As(err, &failure) || failure.Code != "provider_rejected_request" {
		t.Fatalf("Open = %v, want the handshake refusal", err)
	}
	if time.Since(started) > 3*time.Second {
		t.Fatal("Open waited for the setup timeout instead of the error frame")
	}
}

func TestTranscribeRefusesForeignEndpointsAndMedia(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	adapter, err := NewSTT(STTConfig{AllowedEndpointHosts: []string{fake.host()}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("NewSTT: %v", err)
	}
	ctx := context.Background()

	foreignPath := realtimeRequest(strings.Replace(fake.endpoint(), socketPath, "/v1/asr/other", 1))
	if _, err := adapter.Open(ctx, foreignPath); err == nil || !strings.Contains(err.Error(), "endpoint path") {
		t.Fatalf("foreign path: err = %v", err)
	}
	foreignHost := realtimeRequest("wss://api.meta.ai.evil.example/v1/asr/realtime")
	if _, err := adapter.Open(ctx, foreignHost); err == nil {
		t.Fatal("foreign host was accepted")
	}
	wrongRate := realtimeRequest(fake.endpoint())
	wrongRate.Media.SampleRateHz = 8_000
	if _, err := adapter.Open(ctx, wrongRate); err == nil || !strings.Contains(err.Error(), "16000 or 24000") {
		t.Fatalf("8 kHz: err = %v", err)
	}
	stereo := realtimeRequest(fake.endpoint())
	stereo.Media.Channels = 2
	if _, err := adapter.Open(ctx, stereo); err == nil {
		t.Fatal("stereo was accepted")
	}
	foreignProvider := realtimeRequest(fake.endpoint())
	foreignProvider.Plan.Route.Provider = "gemini"
	if _, err := adapter.Open(ctx, foreignProvider); err == nil {
		t.Fatal("foreign provider was accepted")
	}
	noCredential := realtimeRequest(fake.endpoint())
	noCredential.Plan.Route.Credential = nil
	if _, err := adapter.Open(ctx, noCredential); err == nil {
		t.Fatal("missing credential was accepted")
	}
	if _, err := adapter.Open(ctx, runtimepkg.AdapterRequest{Kind: protocol.SessionKindTTS, Plan: realtimeRequest(fake.endpoint()).Plan}); err == nil {
		t.Fatal("tts kind was accepted")
	}
}

func TestLanguageBiasTranslatesTagsAndPassesNames(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"en": "English", "en-US": "English", "pt_BR": "Portuguese", "zh-CN": "Mandarin Chinese",
		"cmn": "Mandarin Chinese", "fil": "Tagalog", "he": "Hebrew", "iw": "Hebrew",
		"english": "English", " Vietnamese ": "Vietnamese",
		"ru": "", "": "", "xx-YY": "",
	}
	for input, want := range cases {
		if got := languageBias(input); got != want {
			t.Errorf("languageBias(%q) = %q, want %q", input, got, want)
		}
	}
}
