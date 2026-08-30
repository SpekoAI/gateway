package gemini

import (
	"context"
	"encoding/json"
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

type fakeTranscribe struct {
	t *testing.T
	// replies are written after the client signals audioStreamEnd.
	replies []string
	server  *httptest.Server

	mu     sync.Mutex
	apiKey string
	setup  map[string]any
	audio  map[string]string
}

func newFakeTranscribe(t *testing.T, replies ...string) *fakeTranscribe {
	fake := &fakeTranscribe{t: t, replies: replies}
	mux := http.NewServeMux()
	mux.HandleFunc(socketPath, fake.handle)
	fake.server = httptest.NewServer(mux)
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeTranscribe) endpoint() string {
	return "ws" + strings.TrimPrefix(f.server.URL, "http") + socketPath
}

func (f *fakeTranscribe) host() string {
	parsed, _ := url.Parse(f.server.URL)
	return parsed.Hostname()
}

func (f *fakeTranscribe) handle(writer http.ResponseWriter, request *http.Request) {
	f.mu.Lock()
	f.apiKey = request.Header.Get(APIKeyHeader)
	f.mu.Unlock()
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
	var setup struct {
		Setup map[string]any `json:"setup"`
	}
	if json.Unmarshal(payload, &setup) != nil || setup.Setup == nil {
		f.t.Errorf("first frame was not setup: %s", payload)
		return
	}
	f.mu.Lock()
	f.setup = setup.Setup
	f.mu.Unlock()
	// Answered as a BINARY frame on purpose: the live service does this, and
	// an adapter that only accepts text frames waits out its setup timeout.
	if conn.Write(ctx, websocket.MessageBinary, []byte(`{"setupComplete":{}}`)) != nil {
		return
	}
	for {
		_, payload, err = conn.Read(ctx)
		if err != nil {
			return
		}
		var input struct {
			RealtimeInput struct {
				Audio          map[string]string `json:"audio"`
				AudioStreamEnd bool              `json:"audioStreamEnd"`
			} `json:"realtimeInput"`
		}
		if json.Unmarshal(payload, &input) != nil {
			continue
		}
		if input.RealtimeInput.Audio != nil {
			f.mu.Lock()
			f.audio = input.RealtimeInput.Audio
			f.mu.Unlock()
		}
		if input.RealtimeInput.AudioStreamEnd {
			for _, message := range f.replies {
				if conn.Write(ctx, websocket.MessageBinary, []byte(message)) != nil {
					return
				}
			}
		}
	}
}

func transcribeRequest(endpoint string) runtimepkg.AdapterRequest {
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindSTT,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged},
			Route: protocol.PlanRoute{Provider: ProviderName, Model: "gemini-3.5-transcribe-live", Adapter: STTAdapterID, Transport: protocol.TransportWebSocket, Endpoint: endpoint,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "test-api-key", ExpiresAt: time.Now().Add(time.Minute)}},
		},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
		Options: protocol.RequestOptions{},
	}
}

func openTranscribe(t *testing.T, fake *fakeTranscribe, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, context.Context) {
	t.Helper()
	adapter, err := NewSTT(STTConfig{AllowedEndpointHosts: []string{fake.host()}, AllowInsecureEndpoint: true, SetupTimeout: 5 * time.Second})
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

func eventText(t *testing.T, event runtimepkg.ProviderEvent) string {
	t.Helper()
	var data struct {
		Text    string `json:"text"`
		IsFinal bool   `json:"is_final"`
	}
	if err := json.Unmarshal(event.Data, &data); err != nil {
		t.Fatalf("event data: %v", err)
	}
	return data.Text
}

// The transcript arrives as fragments and is published as deltas, then joined
// into one final when the service marks the turn complete.
func TestTranscribeStreamsDeltasThenFinal(t *testing.T) {
	t.Parallel()
	fake := newFakeTranscribe(t,
		`{"serverContent":{"inputTranscription":{"text":"hello "}}}`,
		`{"serverContent":{"inputTranscription":{"text":"world"}}}`,
		`{"serverContent":{"turnComplete":true},"usageMetadata":{"promptTokenCount":2}}`,
	)
	request := transcribeRequest(fake.endpoint())
	request.Options.Language = "en-US"
	diarize := true
	request.Options.STT = &protocol.SttOptions{Diarization: &diarize, Keywords: []string{"Speko", "  ", "relay"}}
	stream, ctx := openTranscribe(t, fake, request)

	if err := stream.WriteAudio(ctx, []byte("caller-pcm")); err != nil {
		t.Fatalf("WriteAudio: %v", err)
	}
	if err := stream.CommitAudio(ctx); err != nil {
		t.Fatalf("CommitAudio: %v", err)
	}
	events := expectEvents(t, stream, ctx, []protocol.EventType{
		protocol.EventSessionReady,
		protocol.EventTranscriptDelta, protocol.EventTranscriptDelta,
		protocol.EventTranscriptFinal, protocol.EventUsageObserved,
	})
	if text := eventText(t, events[1]); text != "hello " {
		t.Fatalf("first delta = %q", text)
	}
	if text := eventText(t, events[3]); text != "hello world" {
		t.Fatalf("final = %q, want the joined turn", text)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.apiKey != "test-api-key" {
		t.Fatalf("api key header = %q", fake.apiKey)
	}
	if fake.setup["model"] != "models/gemini-3.5-transcribe-live" {
		t.Fatalf("setup model = %v", fake.setup["model"])
	}
	generation, _ := fake.setup["generationConfig"].(map[string]any)
	modalities, _ := generation["responseModalities"].([]any)
	if len(modalities) != 1 || modalities[0] != "TEXT" {
		t.Fatalf("responseModalities = %v, want [TEXT] so the model does not speak", generation["responseModalities"])
	}
	if _, speaks := fake.setup["outputAudioTranscription"]; speaks {
		t.Fatal("outputAudioTranscription must be absent: there is no model audio to transcribe")
	}
	transcription, _ := fake.setup["inputAudioTranscription"].(map[string]any)
	if languages, _ := transcription["languageCodes"].([]any); len(languages) != 1 || languages[0] != "en-US" {
		t.Fatalf("languageCodes = %v", transcription["languageCodes"])
	}
	if transcription["diarization"] != true {
		t.Fatalf("diarization = %v", transcription["diarization"])
	}
	vocabulary, _ := transcription["customVocabulary"].([]any)
	if len(vocabulary) != 2 || vocabulary[0] != "Speko" || vocabulary[1] != "relay" {
		t.Fatalf("customVocabulary = %v, want blank keywords dropped", transcription["customVocabulary"])
	}
	// Deprecated spellings must not be mirrored alongside the current ones.
	for _, deprecated := range []string{"languageHints", "languageAuto", "adaptationPhrases"} {
		if _, present := transcription[deprecated]; present {
			t.Fatalf("deprecated field %q was sent", deprecated)
		}
	}
	if fake.audio["mimeType"] != "audio/pcm;rate=16000" {
		t.Fatalf("audio = %v", fake.audio)
	}
}

// A transcription model that answers with model-turn text instead of the
// dedicated channel still produces transcripts.
func TestTranscribeFallsBackToModelTurnText(t *testing.T) {
	t.Parallel()
	fake := newFakeTranscribe(t,
		`{"serverContent":{"modelTurn":{"parts":[{"text":"spoken words"}]}}}`,
		`{"serverContent":{"generationComplete":true}}`,
	)
	stream, ctx := openTranscribe(t, fake, transcribeRequest(fake.endpoint()))
	if err := stream.CommitAudio(ctx); err != nil {
		t.Fatalf("CommitAudio: %v", err)
	}
	events := expectEvents(t, stream, ctx, []protocol.EventType{
		protocol.EventSessionReady, protocol.EventTranscriptDelta, protocol.EventTranscriptFinal,
	})
	if text := eventText(t, events[2]); text != "spoken words" {
		t.Fatalf("final = %q", text)
	}
}

// Once the service has used inputTranscription, model-turn text is the SAME
// transcript arriving again. Publishing both would double every fragment.
func TestTranscribeDoesNotDoublePublishBothChannels(t *testing.T) {
	t.Parallel()
	fake := newFakeTranscribe(t,
		`{"serverContent":{"inputTranscription":{"text":"once"}}}`,
		`{"serverContent":{"modelTurn":{"parts":[{"text":"once"}]}}}`,
		`{"serverContent":{"turnComplete":true}}`,
	)
	stream, ctx := openTranscribe(t, fake, transcribeRequest(fake.endpoint()))
	if err := stream.CommitAudio(ctx); err != nil {
		t.Fatalf("CommitAudio: %v", err)
	}
	events := expectEvents(t, stream, ctx, []protocol.EventType{
		protocol.EventSessionReady, protocol.EventTranscriptDelta, protocol.EventTranscriptFinal,
	})
	if text := eventText(t, events[2]); text != "once" {
		t.Fatalf("final = %q, want the fragment published exactly once", text)
	}
}

// A turn that carried no text publishes no final: an empty final is
// indistinguishable from silence to a caller.
func TestTranscribeSuppressesEmptyTurn(t *testing.T) {
	t.Parallel()
	fake := newFakeTranscribe(t,
		`{"serverContent":{"turnComplete":true}}`,
		`{"serverContent":{"inputTranscription":{"text":"after"}}}`,
		`{"serverContent":{"turnComplete":true}}`,
	)
	stream, ctx := openTranscribe(t, fake, transcribeRequest(fake.endpoint()))
	if err := stream.CommitAudio(ctx); err != nil {
		t.Fatalf("CommitAudio: %v", err)
	}
	expectEvents(t, stream, ctx, []protocol.EventType{
		protocol.EventSessionReady, protocol.EventTranscriptDelta, protocol.EventTranscriptFinal,
	})
}

func TestTranscribeRefusesForeignEndpointsAndMedia(t *testing.T) {
	t.Parallel()
	adapter, err := NewSTT(STTConfig{})
	if err != nil {
		t.Fatalf("NewSTT: %v", err)
	}
	request := transcribeRequest("wss://router.speko.dev/v1/stt/stream")
	if _, err := adapter.Open(context.Background(), request); err == nil {
		t.Fatal("accepted a non-provider host")
	}
	// The right host but the wrong RPC is still refused: only this package
	// knows which path on that host it may dial.
	request.Plan.Route.Endpoint = "wss://" + officialHost + "/ws/google.ai.generativelanguage.v1beta.GenerativeService.SomethingElse"
	if _, err := adapter.Open(context.Background(), request); err == nil || !strings.Contains(err.Error(), "path") {
		t.Fatalf("Open error = %v, want a path rejection", err)
	}
	request.Plan.Route.Endpoint = "wss://" + officialHost + socketPath
	request.Media.SampleRateHz = 24_000
	if _, err := adapter.Open(context.Background(), request); err == nil || !strings.Contains(err.Error(), "16000") {
		t.Fatalf("Open error = %v, want a 16 kHz rejection", err)
	}
	request.Media.SampleRateHz = 16_000
	request.Plan.Route.Provider = "google"
	if _, err := adapter.Open(context.Background(), request); err == nil || !strings.Contains(err.Error(), "google") {
		t.Fatalf("Open error = %v, want provider google refused — that is Cloud Speech, not Gemini", err)
	}
	request.Plan.Route.Provider = ProviderName
	request.Kind = protocol.SessionKindS2S
	if _, err := adapter.Open(context.Background(), request); err == nil {
		t.Fatal("accepted a non-stt session kind")
	}
}

// A session that establishes inputTranscription and then carries a turn on the
// model channel alone must still publish that turn. Suppressing the fallback
// for the whole session drops such a turn silently — no delta, no final — and
// silence is indistinguishable from "the caller said nothing".
func TestTranscribeDoesNotDropAModelChannelTurnAfterAnInputChannelTurn(t *testing.T) {
	t.Parallel()
	fake := newFakeTranscribe(t,
		`{"serverContent":{"inputTranscription":{"text":"first turn"}}}`,
		`{"serverContent":{"turnComplete":true}}`,
		`{"serverContent":{"modelTurn":{"parts":[{"text":"second turn"}]}}}`,
		`{"serverContent":{"turnComplete":true}}`,
	)
	stream, ctx := openTranscribe(t, fake, transcribeRequest(fake.endpoint()))
	if err := stream.CommitAudio(ctx); err != nil {
		t.Fatalf("CommitAudio: %v", err)
	}
	events := expectEvents(t, stream, ctx, []protocol.EventType{
		protocol.EventSessionReady,
		protocol.EventTranscriptDelta, protocol.EventTranscriptFinal,
		protocol.EventTranscriptFinal,
	})
	if text := eventText(t, events[2]); text != "first turn" {
		t.Fatalf("first final = %q", text)
	}
	if text := eventText(t, events[3]); text != "second turn" {
		t.Fatalf("second final = %q, want the model-channel turn published", text)
	}
}

// The retained model text must not become a second publication of a turn the
// dedicated channel already covered — the duplicate guard has to survive the
// per-turn fallback, on later turns as much as the first.
func TestTranscribeStillDeduplicatesOnALaterTurn(t *testing.T) {
	t.Parallel()
	fake := newFakeTranscribe(t,
		`{"serverContent":{"inputTranscription":{"text":"one"}}}`,
		`{"serverContent":{"turnComplete":true}}`,
		`{"serverContent":{"inputTranscription":{"text":"two"}}}`,
		`{"serverContent":{"modelTurn":{"parts":[{"text":"two"}]}}}`,
		`{"serverContent":{"turnComplete":true}}`,
		`{"serverContent":{"inputTranscription":{"text":"three"}}}`,
		`{"serverContent":{"turnComplete":true}}`,
	)
	stream, ctx := openTranscribe(t, fake, transcribeRequest(fake.endpoint()))
	if err := stream.CommitAudio(ctx); err != nil {
		t.Fatalf("CommitAudio: %v", err)
	}
	events := expectEvents(t, stream, ctx, []protocol.EventType{
		protocol.EventSessionReady,
		protocol.EventTranscriptDelta, protocol.EventTranscriptFinal,
		protocol.EventTranscriptDelta, protocol.EventTranscriptFinal,
		protocol.EventTranscriptDelta, protocol.EventTranscriptFinal,
	})
	if text := eventText(t, events[4]); text != "two" {
		t.Fatalf("second final = %q, want the echoed model text dropped", text)
	}
	// The third turn proves the retained text did not survive the flush and
	// leak into a later turn's final.
	if text := eventText(t, events[6]); text != "three" {
		t.Fatalf("third final = %q, want no carry-over from the retained buffer", text)
	}
}
