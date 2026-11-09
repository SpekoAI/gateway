package palabra

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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

func TestOpaqueBearerCredentialUsesAuthorizationHeader(t *testing.T) {
	headers := authHeaders("publisher.jwt")
	if got := headers.Get("Authorization"); got != "Bearer publisher.jwt" {
		t.Fatalf("Authorization = %q", got)
	}
}

func TestSTTDialsWithTokenAndFinalizesTranscript(t *testing.T) {
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
		messageType, command, err := conn.Read(r.Context())
		if err != nil || messageType != websocket.MessageText || string(command) != `{"message_type":"finalize"}` {
			t.Errorf("finalize frame = %v %q err=%v", messageType, command, err)
			return
		}
		payload := `{"message_type":"transcription","transcription_id":"segment-1","language":"en","is_eos":true,"segment":{"text":"hello<fin>","start_time":0.25,"end_time":1.5}}`
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

	if err := stream.CommitAudio(context.Background()); err != nil {
		t.Fatal(err)
	}
	gotRequest := <-observed
	if gotRequest.Header.Get("Authorization") != "" {
		t.Errorf("Authorization = %q", gotRequest.Header.Get("Authorization"))
	}
	if gotRequest.URL.Query().Get("format") != "pcm_s16le" || gotRequest.URL.Query().Get("sample_rate") != "16000" || gotRequest.URL.Query().Get("language") != "en" {
		t.Errorf("query = %v", gotRequest.URL.Query())
	}
	if gotRequest.URL.Query().Get("token") != "palabra-key" {
		t.Fatal("missing ASR token")
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
		if r.Header.Get("Authorization") != "Bearer palabra-key" || r.URL.RawQuery != "" {
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
		Plan:    protocol.SessionPlan{Execution: protocol.Execution{ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK}, Route: protocol.PlanRoute{Provider: "palabra", Model: "default", Adapter: STTAdapterID, Transport: protocol.TransportWebSocket, Endpoint: endpoint, Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "palabra-key"}}},
		Options: protocol.RequestOptions{Language: "en-US"},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
	}
}

func ttsRequest(endpoint string) runtimepkg.AdapterRequest {
	return runtimepkg.AdapterRequest{
		Kind:    protocol.SessionKindTTS,
		Plan:    protocol.SessionPlan{Execution: protocol.Execution{ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK}, Route: protocol.PlanRoute{Provider: "palabra", Model: "auto", Voice: "default_low", Adapter: TTSAdapterID, Transport: protocol.TransportWebSocket, Endpoint: endpoint, Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "palabra-key"}}},
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

func TestSTTFinalizeMarkerIsNotTranscriptContent(t *testing.T) {
	for _, tc := range []struct{ wire, want string }{
		{"hello<fin>", "hello"}, {"<fin>", ""},
	} {
		t.Run(tc.wire, func(t *testing.T) {
			s := &sttStream{ctx: context.Background(), events: make(chan runtimepkg.ProviderEvent, 1)}
			payload, _ := json.Marshal(map[string]any{"message_type": "transcription", "is_eos": false, "segment": map[string]any{"text": tc.wire}})
			if err := s.handleMessage(payload); err != nil {
				t.Fatal(err)
			}
			event := <-s.events
			var data struct {
				Text  string `json:"text"`
				Final bool   `json:"is_final"`
			}
			_ = json.Unmarshal(event.Data, &data)
			if event.Type != protocol.EventTranscriptFinal || data.Text != tc.want || !data.Final {
				t.Fatalf("event %s data %+v", event.Type, data)
			}
		})
	}
}

func TestSTTDialFailureRedactsToken(t *testing.T) {
	key := "secret+token/with=characters"
	adapter, err := NewSTT(STTConfig{HTTPClient: &http.Client{Transport: sttFailingTransport{}}})
	if err != nil {
		t.Fatal(err)
	}
	req := sttRequest("wss://stream.palabra.ai" + sttPath)
	req.Plan.Route.Credential.Value = key
	_, err = adapter.Open(context.Background(), req)
	if err == nil {
		t.Fatal("expected dial error")
	}
	for cause := err; cause != nil; cause = errors.Unwrap(cause) {
		if strings.Contains(cause.Error(), key) || strings.Contains(cause.Error(), url.QueryEscape(key)) {
			t.Fatal("dial error retained credential")
		}
	}
}

type sttFailingTransport struct{}

func (sttFailingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("failed request %s", r.URL)
}

// Batch STT calls Close before consuming Events. The provider must still get
// time to recognize the uploaded audio and acknowledge every finalize command.
func TestSTTCloseDrainsFinalization(t *testing.T) {
	for _, tc := range []struct {
		name, text string
		commits    int
	}{
		{"speech", "hello", 1}, {"silence", "", 1}, {"implicit commit", "hello", 0}, {"two pending commits", "hello", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			allowFinal := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				typ, _, err := conn.Read(ctx)
				if err != nil || typ != websocket.MessageBinary {
					t.Errorf("audio: %v %v", typ, err)
					return
				}
				for range max(tc.commits, 1) {
					typ, b, err := conn.Read(ctx)
					if err != nil || typ != websocket.MessageText || string(b) != `{"message_type":"finalize"}` {
						t.Errorf("finalize: %v %s %v", typ, b, err)
						return
					}
				}
				select {
				case <-allowFinal:
				case <-ctx.Done():
					return
				}
				// More than EventBuffer events: Close must not wait for the caller to drain.
				for i := range max(tc.commits, 1) {
					partial, _ := json.Marshal(map[string]any{"message_type": "transcription", "segment": map[string]any{"text": tc.text}})
					if err = conn.Write(ctx, websocket.MessageText, partial); err != nil {
						t.Errorf("partial %d: %v", i, err)
						return
					}
					final, _ := json.Marshal(map[string]any{"message_type": "transcription", "is_eos": tc.text != "", "segment": map[string]any{"text": tc.text + "<fin>"}})
					if err = conn.Write(ctx, websocket.MessageText, final); err != nil {
						t.Errorf("final %d: %v", i, err)
						return
					}
				}
				_, _, _ = conn.Read(ctx) // Keep the socket open until the client finishes.
			}))
			defer server.Close()
			parsed, _ := url.Parse(server.URL)
			adapter, _ := NewSTT(STTConfig{AllowedEndpointHosts: []string{parsed.Hostname()}, AllowInsecureEndpoint: true, EventBuffer: 1})
			provider, err := adapter.Open(ctx, sttRequest(wsURL(server.URL)+sttPath))
			if err != nil {
				t.Fatal(err)
			}
			defer provider.(*sttStream).Abort(ctx)
			if err = provider.WriteAudio(ctx, []byte("pcm")); err != nil {
				t.Fatal(err)
			}
			for range tc.commits {
				if err = provider.CommitAudio(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if err = provider.Close(ctx); err != nil {
				t.Fatal(err)
			}
			close(allowFinal)
			finals := 0
			for {
				select {
				case e, ok := <-provider.Events():
					if !ok {
						if finals != max(tc.commits, 1) {
							t.Fatalf("finals=%d", finals)
						}
						if err := provider.(*sttStream).TerminalError(); err != nil {
							t.Fatal(err)
						}
						return
					}
					if e.Err != nil {
						t.Fatal(e.Err)
					}
					if e.Type == protocol.EventTranscriptFinal {
						var d struct {
							Text string `json:"text"`
						}
						json.Unmarshal(e.Data, &d)
						if d.Text != tc.text {
							t.Fatalf("text=%q", d.Text)
						}
						finals++
					}
				case <-ctx.Done():
					t.Fatal("events did not finish")
				}
			}
		})
	}
}

func TestSTTCloseReportsMissingAcknowledgement(t *testing.T) {
	for _, earlyClose := range []bool{false, true} {
		t.Run(fmt.Sprint(earlyClose), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				_, _, _ = conn.Read(ctx) // finalize sent by Close
				if earlyClose {
					_ = conn.Close(websocket.StatusNormalClosure, "")
					return
				}
				_, _, _ = conn.Read(ctx)
			}))
			defer server.Close()
			parsed, _ := url.Parse(server.URL)
			adapter, _ := NewSTT(STTConfig{AllowedEndpointHosts: []string{parsed.Hostname()}, AllowInsecureEndpoint: true, GracefulCloseTimeout: 30 * time.Millisecond})
			provider, err := adapter.Open(ctx, sttRequest(wsURL(server.URL)+sttPath))
			if err != nil {
				t.Fatal(err)
			}
			defer provider.(*sttStream).Abort(ctx)
			if err = provider.Close(ctx); err != nil {
				t.Fatal(err)
			}
			for {
				select {
				case _, ok := <-provider.Events():
					if !ok {
						var pe *runtimepkg.ProviderError
						if !errors.As(provider.(*sttStream).TerminalError(), &pe) {
							t.Fatal("missing finalize was reported as success")
						}
						want := "request_timeout"
						if earlyClose {
							want = "provider_unavailable"
						}
						if pe.Code != want {
							t.Fatalf("code=%s want=%s", pe.Code, want)
						}
						wantHint := "did not finish the transcript within 30ms"
						if earlyClose {
							wantHint = "closed the stream with WebSocket code 1000"
						}
						if !strings.Contains(pe.Hint, wantHint) {
							t.Fatalf("hint=%q want substring %q", pe.Hint, wantHint)
						}
						return
					}
				case <-ctx.Done():
					t.Fatal("shutdown stalled")
				}
			}
		})
	}
}
