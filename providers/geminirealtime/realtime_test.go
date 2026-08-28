package geminirealtime

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

type fakeGemini struct {
	t      *testing.T
	server *httptest.Server
	mu     sync.Mutex
	query  url.Values
	setup  map[string]any
	audio  map[string]string
}

func newFakeGemini(t *testing.T) *fakeGemini {
	fake := &fakeGemini{t: t}
	mux := http.NewServeMux()
	mux.HandleFunc(socketPath, fake.handle)
	fake.server = httptest.NewServer(mux)
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeGemini) endpoint() string {
	return "ws" + strings.TrimPrefix(f.server.URL, "http") + socketPath
}
func (f *fakeGemini) host() string { parsed, _ := url.Parse(f.server.URL); return parsed.Hostname() }
func (f *fakeGemini) handle(writer http.ResponseWriter, request *http.Request) {
	f.mu.Lock()
	f.query = request.URL.Query()
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
			audio := base64.StdEncoding.EncodeToString([]byte("gemini-audio"))
			for _, message := range []string{
				`{"serverContent":{"inputTranscription":{"text":"hello"}}}`,
				`{"serverContent":{"modelTurn":{"parts":[{"inlineData":{"mimeType":"audio/pcm;rate=24000","data":"` + audio + `"}}]}}}`,
				`{"serverContent":{"turnComplete":true},"usageMetadata":{"promptTokenCount":2,"responseTokenCount":3}}`,
			} {
				if conn.Write(ctx, websocket.MessageBinary, []byte(message)) != nil {
					return
				}
			}
		}
	}
}

func geminiRequest(endpoint string) runtimepkg.AdapterRequest {
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindRealtime,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsManaged},
			Route: protocol.PlanRoute{Provider: "google", Model: "gemini-3.1-flash-live-preview", Adapter: AdapterID, Transport: protocol.TransportWebSocket, Endpoint: endpoint,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "auth_tokens/one-use", ExpiresAt: time.Now().Add(time.Minute)}},
		},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
		Options: protocol.RequestOptions{Voice: "Puck", S2S: &protocol.S2SOptions{Instructions: "Be brief.", OutputMedia: &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1}}},
	}
}

func TestGeminiProviderDirectRoundTrip(t *testing.T) {
	t.Parallel()
	fake := newFakeGemini(t)
	adapter, err := New(Config{AllowedEndpointHosts: []string{fake.host()}, AllowInsecureEndpoint: true, SetupTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := adapter.Open(ctx, geminiRequest(fake.endpoint()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer stream.Close(context.Background())
	fake.mu.Lock()
	if fake.query.Get("access_token") != "auth_tokens/one-use" || fake.setup["model"] != "models/gemini-3.1-flash-live-preview" {
		t.Fatalf("query=%v setup=%v", fake.query, fake.setup)
	}
	fake.mu.Unlock()
	if err := stream.WriteAudio(ctx, []byte("caller-pcm")); err != nil {
		t.Fatalf("WriteAudio: %v", err)
	}
	if err := stream.CommitAudio(ctx); err != nil {
		t.Fatalf("CommitAudio: %v", err)
	}
	want := []protocol.EventType{protocol.EventTranscriptFinal, protocol.EventResponseStarted, protocol.EventAudioFrame, protocol.EventResponseDone, protocol.EventUsageObserved}
	for index, wantType := range want {
		select {
		case event := <-stream.Events():
			if event.Type != wantType {
				t.Fatalf("event %d = %q, want %q", index, event.Type, wantType)
			}
			if event.Type == protocol.EventAudioFrame && string(event.Audio) != "gemini-audio" {
				t.Fatalf("audio = %q", event.Audio)
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for event %d", index)
		}
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.audio["mimeType"] != "audio/pcm;rate=16000" {
		t.Fatalf("audio = %v", fake.audio)
	}
}

func TestGeminiRefusesRouterAndWrongSampleRate(t *testing.T) {
	t.Parallel()
	adapter, _ := New(Config{})
	request := geminiRequest("wss://router.speko.dev/v1/s2s/stream")
	if _, err := adapter.Open(context.Background(), request); err == nil {
		t.Fatal("accepted the Speko router")
	}
	request.Plan.Route.Endpoint = "wss://generativelanguage.googleapis.com" + socketPath
	request.Media.SampleRateHz = 24_000
	if _, err := adapter.Open(context.Background(), request); err == nil || !strings.Contains(err.Error(), "16000") {
		t.Fatalf("Open error = %v, want 16000 Hz rejection", err)
	}
}
