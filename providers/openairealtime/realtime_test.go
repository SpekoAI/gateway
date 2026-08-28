package openairealtime

import (
	"context"
	"encoding/base64"
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

type fakeRealtime struct {
	t      *testing.T
	server *httptest.Server

	mu      sync.Mutex
	headers http.Header
	query   url.Values
	session map[string]any
	appends []string
	commits int
	creates int
	cancels int
}

func newFakeRealtime(t *testing.T) *fakeRealtime {
	fake := &fakeRealtime{t: t}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/realtime", fake.handle)
	fake.server = httptest.NewServer(mux)
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeRealtime) endpoint() string {
	return "ws" + strings.TrimPrefix(f.server.URL, "http") + "/v1/realtime"
}

func (f *fakeRealtime) host() string {
	parsed, _ := url.Parse(f.server.URL)
	return parsed.Hostname()
}

func (f *fakeRealtime) handle(writer http.ResponseWriter, request *http.Request) {
	f.mu.Lock()
	f.headers = request.Header.Clone()
	f.query = request.URL.Query()
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
	var update struct {
		Type    string         `json:"type"`
		Session map[string]any `json:"session"`
	}
	if json.Unmarshal(payload, &update) != nil || update.Type != "session.update" {
		f.t.Errorf("first frame was not session.update: %s", payload)
		return
	}
	f.mu.Lock()
	f.session = update.Session
	f.mu.Unlock()
	if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"session.updated"}`)); err != nil {
		return
	}
	for {
		_, frame, err := conn.Read(ctx)
		if err != nil {
			return
		}
		var event struct {
			Type  string `json:"type"`
			Audio string `json:"audio"`
		}
		if json.Unmarshal(frame, &event) != nil {
			continue
		}
		switch event.Type {
		case "input_audio_buffer.append":
			f.mu.Lock()
			f.appends = append(f.appends, event.Audio)
			f.mu.Unlock()
		case "input_audio_buffer.commit":
			f.mu.Lock()
			f.commits++
			f.mu.Unlock()
			// server_vad owns response creation once the committed turn reaches
			// the provider. The client must not also send response.create.
			for _, message := range []string{
				`{"type":"input_audio_buffer.speech_started"}`,
				`{"type":"conversation.item.input_audio_transcription.completed","transcript":"hello"}`,
				`{"type":"response.created","response":{"id":"resp_1"}}`,
				`{"type":"response.output_audio.delta","delta":"` + base64.StdEncoding.EncodeToString([]byte("provider-audio")) + `"}`,
				`{"type":"response.done","response":{"id":"resp_1","status":"completed","usage":{"input_tokens":1,"output_tokens":2}}}`,
			} {
				if err := conn.Write(ctx, websocket.MessageText, []byte(message)); err != nil {
					return
				}
			}
		case "response.create":
			f.mu.Lock()
			f.creates++
			f.mu.Unlock()
		case "response.cancel":
			f.mu.Lock()
			f.cancels++
			f.mu.Unlock()
		}
	}
}

func realtimeRequest(provider, model, endpoint string) runtimepkg.AdapterRequest {
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindRealtime,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsManaged},
			Route: protocol.PlanRoute{
				Provider: provider, Model: model, Transport: protocol.TransportWebSocket, Endpoint: endpoint,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "ek-short-lived", ExpiresAt: time.Now().Add(time.Minute)},
			},
		},
		Media: &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1},
		Options: protocol.RequestOptions{
			Voice: "marin",
			S2S: &protocol.S2SOptions{
				Instructions: "Answer briefly.",
				OutputMedia:  &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1},
			},
		},
	}
}

func TestXAISessionUpdateUsesDocumentedTranscriptionAndNoTemperature(t *testing.T) {
	t.Parallel()
	temperature := 0.7
	update := buildSessionUpdate(profiles["xai"], "grok-voice-latest", 24_000, 24_000, protocol.RequestOptions{
		Voice: "eve",
		S2S: &protocol.S2SOptions{
			Instructions: "Answer briefly.", Temperature: &temperature,
			OutputMedia: &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1},
		},
	})
	session, ok := update["session"].(map[string]any)
	if !ok {
		t.Fatalf("session update = %#v", update)
	}
	if _, sent := session["temperature"]; sent {
		t.Fatal("xAI realtime session update sent undocumented temperature")
	}
	audio := session["audio"].(map[string]any)
	input := audio["input"].(map[string]any)
	transcription := input["transcription"].(map[string]any)
	if got := transcription["model"]; got != "grok-transcribe" {
		t.Fatalf("xAI transcription model = %v", got)
	}
}

func TestProviderDirectRoundTrip(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	adapter, err := New(Config{Provider: "openai", AllowedEndpointHosts: []string{fake.host()}, AllowInsecureEndpoint: true, SetupTimeout: time.Second})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := adapter.Open(ctx, realtimeRequest("openai", "gpt-realtime", fake.endpoint()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer stream.Close(context.Background())

	fake.mu.Lock()
	if got := fake.headers.Get("Authorization"); got != "Bearer ek-short-lived" {
		t.Fatalf("authorization = %q", got)
	}
	if got := fake.query.Get("model"); got != "gpt-realtime" {
		t.Fatalf("model = %q", got)
	}
	if fake.session["instructions"] != "Answer briefly." {
		t.Fatalf("session = %#v", fake.session)
	}
	fake.mu.Unlock()

	if err := stream.WriteAudio(ctx, []byte("caller-audio")); err != nil {
		t.Fatalf("WriteAudio: %v", err)
	}
	if err := stream.CommitAudio(ctx); err != nil {
		t.Fatalf("CommitAudio: %v", err)
	}

	want := []protocol.EventType{
		protocol.EventSpeechStarted, protocol.EventTranscriptFinal, protocol.EventResponseStarted,
		protocol.EventAudioFrame, protocol.EventUsageObserved, protocol.EventResponseDone,
	}
	for index, wantType := range want {
		select {
		case event := <-stream.Events():
			if event.Type != wantType {
				t.Fatalf("event %d = %q, want %q", index, event.Type, wantType)
			}
			if event.Type == protocol.EventAudioFrame && string(event.Audio) != "provider-audio" {
				t.Fatalf("audio = %q", event.Audio)
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for event %d", index)
		}
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.appends) != 1 || fake.commits != 1 || fake.creates != 0 {
		t.Fatalf("wire counts appends=%d commits=%d creates=%d", len(fake.appends), fake.commits, fake.creates)
	}
}

func TestRefusesHostedRelayAndForeignEndpoint(t *testing.T) {
	t.Parallel()
	adapter, err := New(Config{Provider: "openai"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	request := realtimeRequest("openai", "gpt-realtime", "wss://api.openai.com/v1/realtime")
	request.Plan.Execution.ProviderRoute = protocol.RouteSpekoRelay
	if _, err := adapter.Open(context.Background(), request); err == nil {
		t.Fatal("adapter accepted a hosted relay route")
	}
	request.Plan.Execution.ProviderRoute = protocol.RouteProviderDirect
	request.Plan.Route.Endpoint = "wss://router.speko.dev/v1/s2s/stream"
	if _, err := adapter.Open(context.Background(), request); err == nil {
		t.Fatal("adapter accepted the Speko router as a provider endpoint")
	}
}

func TestRequiresProviderRealtimePCMRate(t *testing.T) {
	t.Parallel()
	adapter, err := New(Config{Provider: "openai"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	request := realtimeRequest("openai", "gpt-realtime", "wss://api.openai.com/v1/realtime")
	request.Media.SampleRateHz = 16_000
	if _, err := adapter.Open(context.Background(), request); err == nil || !strings.Contains(err.Error(), "24 kHz") {
		t.Fatalf("Open error = %v, want 24 kHz rejection", err)
	}
}
