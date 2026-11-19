package geminirealtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
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

func TestGracefulCloseClassifiesReaderWakeupAfterCloseCompletion(t *testing.T) {
	for _, abnormal := range []bool{false, true} {
		t.Run(fmt.Sprint("abnormal_status=", abnormal), func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			stream := &liveStream{ctx: ctx, cancel: cancel, gracefulDone: make(chan struct{}), events: make(chan runtimepkg.ProviderEvent, 1), setupDone: make(chan error, 1)}
			stream.gracefulStarted.Store(true)
			stream.closed.Store(true)
			err := error(fmt.Errorf("reader woke after local close: %w", net.ErrClosed))
			if abnormal {
				err = websocket.CloseError{Code: websocket.StatusPolicyViolation}
			}
			done := make(chan struct{})
			go func() { stream.finish(err); close(done) }()
			if abnormal {
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("provider refusal waited for graceful close")
				}
				var failure *runtimepkg.ProviderError
				if !errors.As(stream.TerminalError(), &failure) || websocket.CloseStatus(failure.Cause) != websocket.StatusPolicyViolation {
					t.Fatal("graceful close lost actual provider refusal")
				}
				return
			}
			select {
			case <-done:
				t.Fatal("generic read wakeup classified before local close completed")
			case <-time.After(20 * time.Millisecond):
			}
			// Close publishes its result, cancels its reader, then signals done.
			cancel()
			close(stream.gracefulDone)
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("reader did not join completed close")
			}
			if stream.TerminalError() != nil || len(stream.events) != 0 {
				t.Fatal("successful local close became a provider failure")
			}
		})
	}
}

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

// The thinking level is a MODEL property, not a caller preference: the service
// closes the socket 1007 both ways — "must be specified" on the
// extended-thinking model without one, "is not supported" on every other Live
// model with one. buildSetup therefore has to decide, not forward.
func TestThinkingLevelFollowsTheModelNotTheCaller(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		model     string
		requested string
		want      any
	}{
		{"extended model defaults when unnamed", "models/gemini-3.8-live-extended-thinking", "", map[string]any{"thinkingLevel": "low"}},
		{"extended model honours the caller", "models/gemini-3.8-live-extended-thinking", "high", map[string]any{"thinkingLevel": "high"}},
		{"extended model refuses minimal, which the service rejects", "models/gemini-3.8-live-extended-thinking", "minimal", map[string]any{"thinkingLevel": "low"}},
		{"plain live model never carries one", "models/gemini-3.8-live", "high", nil},
		{"preview model never carries one", "models/gemini-3.1-flash-live-preview", "medium", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			options := protocol.RequestOptions{S2S: &protocol.S2SOptions{ThinkingLevel: tc.requested}}
			setup := buildSetup(tc.model, options)["setup"].(map[string]any)
			generation := setup["generationConfig"].(map[string]any)
			got, present := generation["thinkingConfig"]
			if tc.want == nil {
				if present {
					t.Fatalf("thinkingConfig present for %s: %v", tc.model, got)
				}
				return
			}
			if !present {
				t.Fatalf("thinkingConfig missing for %s", tc.model)
			}
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Fatalf("thinkingConfig = %v, want %v", got, tc.want)
			}
		})
	}
}

// The native handshake must carry the caller's level all the way to the setup
// document: reading it only from the plan would silently downgrade every
// `medium`/`high` request to the default.
func TestConfigureCarriesTheThinkingLevelToSetup(t *testing.T) {
	t.Parallel()
	configure := protocol.VoiceSessionConfigure{
		Protocol:      protocol.SpeechProtocolGoogleLiveV1,
		Media:         protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
		OutputMedia:   protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1},
		ThinkingLevel: "high",
	}
	if err := configure.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	setup := buildSetup("models/gemini-3.8-live-extended-thinking", configure.Options())["setup"].(map[string]any)
	generation := setup["generationConfig"].(map[string]any)
	if got := fmt.Sprint(generation["thinkingConfig"]); got != "map[thinkingLevel:high]" {
		t.Fatalf("thinkingConfig = %s, want the caller's level", got)
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
