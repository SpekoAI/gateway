package palabra

import (
	"context"
	"encoding/base64"
	"encoding/json"
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

func TestSTTDialsWithBearerAndNormalizesTranscript(t *testing.T) {
	observed := make(chan *http.Request, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed <- r.Clone(r.Context())
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		defer func() { _ = conn.CloseNow() }()
		messageType, audio, err := conn.Read(r.Context())
		if err != nil || messageType != websocket.MessageBinary || string(audio) != "pcm" {
			t.Errorf("audio frame = %v %q err=%v", messageType, audio, err)
			return
		}
		payload := `{"message_type":"transcription","transcription_id":"segment-1","language":"en","is_eos":true,"segment":{"text":"hello","start_time":0.25,"end_time":1.5}}`
		if err := conn.Write(r.Context(), websocket.MessageText, []byte(payload)); err != nil {
			t.Errorf("write transcript: %v", err)
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	}))
	defer server.Close()

	parsed, _ := url.Parse(server.URL)
	adapter, err := NewSTT(STTConfig{AllowedEndpointHosts: []string{parsed.Hostname()}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatal(err)
	}
	request := sttRequest(wsURL(server.URL) + sttPath)
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = stream.Close(context.Background()) }()
	if err := stream.WriteAudio(context.Background(), []byte("pcm")); err != nil {
		t.Fatalf("write audio: %v", err)
	}

	gotRequest := <-observed
	if gotRequest.Header.Get("Authorization") != "Bearer publisher.jwt" {
		t.Errorf("Authorization = %q", gotRequest.Header.Get("Authorization"))
	}
	if gotRequest.URL.Query().Get("format") != "pcm_s16le" || gotRequest.URL.Query().Get("sample_rate") != "16000" || gotRequest.URL.Query().Get("language") != "en" {
		t.Errorf("query = %v", gotRequest.URL.Query())
	}
	if gotRequest.URL.Query().Get("token") != "" {
		t.Fatal("credential leaked into query string")
	}

	event := eventWithin(t, stream.Events())
	if event.Type != protocol.EventTranscriptFinal {
		t.Fatalf("event type = %q", event.Type)
	}
	var data map[string]any
	if err := json.Unmarshal(event.Data, &data); err != nil {
		t.Fatal(err)
	}
	if data["text"] != "hello" || data["audio_start_ms"] != float64(250) || data["audio_end_ms"] != float64(1500) {
		t.Fatalf("transcript data = %#v", data)
	}
}

func TestTTSInitStreamingAndAudioEvents(t *testing.T) {
	frames := make(chan map[string]any, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer publisher.jwt" || r.URL.RawQuery != "" {
			t.Errorf("auth=%q query=%q", r.Header.Get("Authorization"), r.URL.RawQuery)
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		defer func() { _ = conn.CloseNow() }()
		for range 3 {
			_, payload, err := conn.Read(r.Context())
			if err != nil {
				t.Errorf("read frame: %v", err)
				return
			}
			var decoded map[string]any
			if err := json.Unmarshal(payload, &decoded); err != nil {
				t.Errorf("decode frame: %v", err)
				return
			}
			frames <- decoded
		}
		audio := base64.StdEncoding.EncodeToString([]byte{0, 1, 2, 3})
		_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"message_type":"audio_chunk","data":{"audio":"`+audio+`","generation_id":"gen-1","last_chunk":false}}`))
		_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"message_type":"audio_chunk","data":{"audio":"","generation_id":"gen-1","last_chunk":true}}`))
	}))
	defer server.Close()

	parsed, _ := url.Parse(server.URL)
	adapter, err := NewTTS(TTSConfig{AllowedEndpointHosts: []string{parsed.Hostname()}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatal(err)
	}
	request := ttsRequest(wsURL(server.URL) + ttsPath)
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = stream.Close(context.Background()) }()
	if err := stream.AppendText(context.Background(), "hello "); err != nil {
		t.Fatal(err)
	}
	if err := stream.AppendText(context.Background(), "world"); err != nil {
		t.Fatal(err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}

	init := <-frames
	if init["type"] != "init" || init["model"] != "auto" || init["language"] != "en" {
		t.Fatalf("init = %#v", init)
	}
	first, last := <-frames, <-frames
	if first["text"] != "hello " || first["is_eos"] != false || last["text"] != "world" || last["is_eos"] != true {
		t.Fatalf("text frames = %#v %#v", first, last)
	}
	started, audio, done := eventWithin(t, stream.Events()), eventWithin(t, stream.Events()), eventWithin(t, stream.Events())
	if started.Type != protocol.EventAudioStarted || audio.Type != protocol.EventAudioFrame || string(audio.Audio) != string([]byte{0, 1, 2, 3}) || done.Type != protocol.EventAudioDone {
		t.Fatalf("events = %q %q(%v) %q", started.Type, audio.Type, audio.Audio, done.Type)
	}
}

func TestTTSRejectsUnpublishedModel(t *testing.T) {
	adapter, err := NewTTS(TTSConfig{})
	if err != nil {
		t.Fatal(err)
	}
	request := ttsRequest("wss://stream.palabra.ai" + ttsPath)
	request.Plan.Route.Model = "undocumented-model"
	if _, err := adapter.Open(context.Background(), request); err == nil {
		t.Fatal("unpublished Palabra TTS model was accepted")
	}
}

func sttRequest(endpoint string) runtimepkg.AdapterRequest {
	return runtimepkg.AdapterRequest{
		Kind:    protocol.SessionKindSTT,
		Plan:    protocol.SessionPlan{Execution: protocol.Execution{ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsManaged}, Route: protocol.PlanRoute{Provider: "palabra", Model: "default", Adapter: STTAdapterID, Transport: protocol.TransportWebSocket, Endpoint: endpoint, Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "publisher.jwt"}}},
		Options: protocol.RequestOptions{Language: "en-US"},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
	}
}

func ttsRequest(endpoint string) runtimepkg.AdapterRequest {
	return runtimepkg.AdapterRequest{
		Kind:    protocol.SessionKindTTS,
		Plan:    protocol.SessionPlan{Execution: protocol.Execution{ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsManaged}, Route: protocol.PlanRoute{Provider: "palabra", Model: "auto", Voice: "default_low", Adapter: TTSAdapterID, Transport: protocol.TransportWebSocket, Endpoint: endpoint, Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "publisher.jwt"}}},
		Options: protocol.RequestOptions{Language: "en"},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1},
	}
}

func wsURL(raw string) string { return "ws" + strings.TrimPrefix(raw, "http") }

func eventWithin(t *testing.T, events <-chan runtimepkg.ProviderEvent) runtimepkg.ProviderEvent {
	t.Helper()
	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("events closed")
		}
		if event.Err != nil {
			t.Fatalf("provider event error: %v", event.Err)
		}
		return event
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for provider event")
		return runtimepkg.ProviderEvent{}
	}
}
