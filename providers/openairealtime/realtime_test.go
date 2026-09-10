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
	var native []string
	for index, wantType := range want {
		for {
			select {
			case event := <-stream.Events():
				if event.Type == protocol.EventProviderEvent {
					var envelope protocol.ProviderEvent
					if err := json.Unmarshal(event.Data, &envelope); err != nil {
						t.Fatalf("decode provider event: %v", err)
					}
					native = append(native, envelope.Type)
					continue
				}
				if event.Type != wantType {
					t.Fatalf("event %d = %q, want %q", index, event.Type, wantType)
				}
				if event.Type == protocol.EventAudioFrame && string(event.Audio) != "provider-audio" {
					t.Fatalf("audio = %q", event.Audio)
				}
			case <-ctx.Done():
				t.Fatalf("timed out waiting for event %d", index)
			}
			break
		}
	}
	// Every non-audio vendor event rides beside its canonical translation;
	// audio deltas stay canonical frames unless NativeEvents is set.
	wantNative := []string{"session.updated", "input_audio_buffer.speech_started", "conversation.item.input_audio_transcription.completed", "response.created", "response.done"}
	if strings.Join(native, ",") != strings.Join(wantNative, ",") {
		t.Fatalf("native envelopes = %v, want %v", native, wantNative)
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

func TestRefusesGPTLiveOptions(t *testing.T) {
	t.Parallel()
	adapter, err := New(Config{Provider: "openai"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	request := realtimeRequest("openai", "gpt-realtime-2.1", "wss://api.openai.com/v1/realtime")
	request.Options.S2S.Live = &protocol.LiveOptions{Delegation: &protocol.LiveDelegation{Type: protocol.LiveDelegationClient}}
	if _, err := adapter.Open(context.Background(), request); err == nil || !strings.Contains(err.Error(), "GPT-Live") {
		t.Fatalf("Open error = %v, want a GPT-Live option rejection", err)
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

// Both requested Realtime versions must reach the vendor exactly as named:
// the query parameter, the session.update model, and the session identity
// are all the caller's model, never a default or a sibling.
func TestRealtimeModelsAreForwardedExactly(t *testing.T) {
	t.Parallel()
	for _, model := range []string{"gpt-realtime-2", "gpt-realtime-2.1"} {
		model := model
		t.Run(model, func(t *testing.T) {
			t.Parallel()
			fake := newFakeRealtime(t)
			adapter, err := New(Config{Provider: "openai", AllowedEndpointHosts: []string{fake.host()}, AllowInsecureEndpoint: true, SetupTimeout: time.Second})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream, err := adapter.Open(ctx, realtimeRequest("openai", model, fake.endpoint()))
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer stream.Close(context.Background())
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if got := fake.query.Get("model"); got != model {
				t.Fatalf("query model = %q, want %q", got, model)
			}
			if fake.session["model"] != model || fake.session["type"] != "realtime" {
				t.Fatalf("session = %#v", fake.session)
			}
			audio := fake.session["audio"].(map[string]any)
			if audio["output"].(map[string]any)["voice"] != "marin" {
				t.Fatalf("voice = %#v", audio["output"])
			}
		})
	}
}

func TestProviderControlsForwardRealtimeCommandsAndRefuseLiveOnes(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	adapter, err := New(Config{Provider: "openai", AllowedEndpointHosts: []string{fake.host()}, AllowInsecureEndpoint: true, SetupTimeout: time.Second})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := adapter.Open(ctx, realtimeRequest("openai", "gpt-realtime-2.1", fake.endpoint()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer stream.Close(context.Background())
	controls := stream.(runtimepkg.ProviderControlStream)
	if err := controls.SendProviderControl(ctx, protocol.ProviderControl{Type: "response.create", Payload: json.RawMessage(`{"type":"response.create","event_id":"c1"}`)}); err != nil {
		t.Fatalf("response.create: %v", err)
	}
	if err := controls.SendProviderControl(ctx, protocol.ProviderControl{Type: "response.cancel", Payload: json.RawMessage(`{"type":"response.cancel"}`)}); err != nil {
		t.Fatalf("response.cancel: %v", err)
	}
	if err := controls.SendProviderControl(ctx, protocol.ProviderControl{Type: "session.update", Payload: json.RawMessage(`{"type":"session.update","session":{"type":"realtime","instructions":"Be extra nice today!"}}`)}); err != nil {
		t.Fatalf("session.update: %v", err)
	}
	for _, tc := range []struct {
		name    string
		control protocol.ProviderControl
	}{
		{"live append", protocol.ProviderControl{Type: "session.commentary.append", Payload: json.RawMessage(`{"type":"session.commentary.append","delegation_id":null,"content":"x"}`)}},
		{"live audio append as control", protocol.ProviderControl{Type: "session.input_audio.append", Payload: json.RawMessage(`{"type":"session.input_audio.append","audio":"AAAA"}`)}},
		{"model change", protocol.ProviderControl{Type: "session.update", Payload: json.RawMessage(`{"type":"session.update","session":{"model":"gpt-realtime-2"}}`)}},
		{"format change", protocol.ProviderControl{Type: "session.update", Payload: json.RawMessage(`{"type":"session.update","session":{"audio":{"output":{"format":{"type":"audio/pcmu"}}}}}`)}},
		{"oversized", protocol.ProviderControl{Type: "response.create", Payload: json.RawMessage(`{"type":"response.create","pad":"` + strings.Repeat("x", protocol.MaxProviderControlBytes) + `"}`)}},
	} {
		if err := controls.SendProviderControl(ctx, tc.control); err == nil {
			t.Fatalf("%s: control accepted", tc.name)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		fake.mu.Lock()
		creates, cancels := fake.creates, fake.cancels
		fake.mu.Unlock()
		if creates == 1 && cancels == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("forwarded creates=%d cancels=%d", creates, cancels)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestNativeEventsForwardAudioDeltasVerbatim(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	adapter, err := New(Config{Provider: "openai", AllowedEndpointHosts: []string{fake.host()}, AllowInsecureEndpoint: true, SetupTimeout: time.Second, NativeEvents: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := adapter.Open(ctx, realtimeRequest("openai", "gpt-realtime-2", fake.endpoint()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer stream.Close(context.Background())
	if err := stream.CommitAudio(ctx); err != nil {
		t.Fatalf("CommitAudio: %v", err)
	}
	sawAudioEnvelope := false
	for !sawAudioEnvelope {
		select {
		case event := <-stream.Events():
			if event.Type == protocol.EventAudioFrame {
				t.Fatal("native mode emitted a canonical audio frame")
			}
			if event.Type == protocol.EventProviderEvent {
				var envelope protocol.ProviderEvent
				_ = json.Unmarshal(event.Data, &envelope)
				if envelope.Type == "response.output_audio.delta" {
					var payload map[string]any
					_ = json.Unmarshal(envelope.Payload, &payload)
					if decoded, _ := base64.StdEncoding.DecodeString(payload["delta"].(string)); string(decoded) != "provider-audio" {
						t.Fatalf("audio delta = %v", payload)
					}
					sawAudioEnvelope = true
				}
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for the native audio envelope")
		}
	}
}
