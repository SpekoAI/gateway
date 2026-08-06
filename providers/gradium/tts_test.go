package gradium

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

// TestTTSAdapterSendsDocumentedSetupAndStreamsAudioBeforeDone pins the opening
// handshake byte-for-byte and proves audio is delivered incrementally: the
// server is held at a barrier until the caller has already consumed the first
// frame, so a buffer-everything-then-flush implementation cannot pass.
func TestTTSAdapterSendsDocumentedSetupAndStreamsAudioBeforeDone(t *testing.T) {
	t.Parallel()

	requests := make(chan *http.Request, 1)
	firstFrameConsumed := make(chan struct{})
	server := newSocketServer(t, "/api/speech/tts", func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		requests <- request.Clone(request.Context())

		setup, err := readFrame(ctx, conn)
		if err != nil {
			t.Errorf("read setup: %v", err)
			return
		}
		// Gradium answers a pre-setup frame with "Session not found. Send
		// setup first." (1002), so setup is the first frame, and the field
		// names are the vendor's: voice_id, model_name, output_format.
		wantSetup := map[string]any{
			"type":          "setup",
			"model_name":    "default",
			"voice_id":      "YTpq7expH9539ERJ",
			"output_format": "pcm_48000",
		}
		if !reflect.DeepEqual(setup, wantSetup) {
			t.Errorf("setup frame = %#v, want %#v", setup, wantSetup)
			return
		}
		if err := writeJSON(ctx, conn, map[string]any{
			"type": "ready", "request_id": "gd_tts_777", "model_name": "default",
			"model_ext": "resolved-model", "sample_rate": 48000, "frame_size": 3840,
		}); err != nil {
			t.Errorf("write ready: %v", err)
			return
		}

		text, err := readFrame(ctx, conn)
		if err != nil {
			t.Errorf("read text: %v", err)
			return
		}
		if !reflect.DeepEqual(text, map[string]any{"type": "text", "text": "Hello, world."}) {
			t.Errorf("text frame = %#v", text)
			return
		}
		end, err := readFrame(ctx, conn)
		if err != nil {
			t.Errorf("read end_of_stream: %v", err)
			return
		}
		if !reflect.DeepEqual(end, map[string]any{"type": "end_of_stream"}) {
			t.Errorf("end frame = %#v", end)
			return
		}

		if err := writeJSON(ctx, conn, map[string]any{
			"type": "audio", "audio": base64.StdEncoding.EncodeToString([]byte{1, 2, 3, 4}),
			"start_s": 0, "stop_s": 0.08, "stream_id": 0,
		}); err != nil {
			t.Errorf("write first audio: %v", err)
			return
		}
		// Barrier: nothing else goes out until the client has already emitted
		// the first frame to its consumer.
		select {
		case <-firstFrameConsumed:
		case <-ctx.Done():
			return
		}
		if err := writeJSON(ctx, conn, map[string]any{
			"type": "audio", "audio": base64.StdEncoding.EncodeToString([]byte{5, 6}),
			"start_s": 0.08, "stop_s": 0.16,
		}); err != nil {
			t.Errorf("write second audio: %v", err)
			return
		}
		if err := writeJSON(ctx, conn, map[string]any{
			"type": "audio", "audio": base64.StdEncoding.EncodeToString([]byte{7}),
			"start_s": 0.16, "stop_s": 0.24,
		}); err != nil {
			t.Errorf("write third audio: %v", err)
			return
		}
		// On the TTS socket `text` is the word-timing surface, not a
		// transcript: it annotates already-generated audio.
		if err := writeJSON(ctx, conn, map[string]any{
			"type": "text", "text": "Hello,", "start_s": 0, "stop_s": 0.16, "stream_id": 0,
		}); err != nil {
			t.Errorf("write alignment: %v", err)
			return
		}
		if err := writeJSON(ctx, conn, map[string]any{"type": "end_of_stream"}); err != nil {
			t.Errorf("write end_of_stream: %v", err)
			return
		}
		if err := conn.Close(websocket.StatusNormalClosure, ""); err != nil {
			t.Errorf("close server socket: %v", err)
		}
	})
	defer server.Close()

	adapter, err := NewTTS(ttsConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), ttsRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	events := stream.Events()

	if err := stream.AppendText(context.Background(), "Hello, world."); err != nil {
		t.Fatalf("append text: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit text: %v", err)
	}

	usage := waitEvent(t, events)
	if usage.Type != protocol.EventUsageObserved || decodeString(t, usage.Data, "provider_request_id") != "gd_tts_777" {
		t.Fatalf("usage event = %s %s", usage.Type, usage.Data)
	}
	started := waitEvent(t, events)
	if started.Type != protocol.EventAudioStarted {
		t.Fatalf("first media event = %s, want audio.started", started.Type)
	}
	firstFrame := waitEvent(t, events)
	if firstFrame.Type != protocol.EventAudioFrame || string(firstFrame.Audio) != string([]byte{1, 2, 3, 4}) {
		t.Fatalf("first audio frame = %s %v", firstFrame.Type, firstFrame.Audio)
	}
	if got := decodeNumber(t, firstFrame.Data, "audio_end_ms"); got != 80 {
		t.Fatalf("first frame audio_end_ms = %v", got)
	}
	// Reaching here means one frame was delivered while the server still had
	// two more to write, plus the terminal event. Release the barrier.
	close(firstFrameConsumed)

	secondFrame := waitEvent(t, events)
	if secondFrame.Type != protocol.EventAudioFrame || string(secondFrame.Audio) != string([]byte{5, 6}) {
		t.Fatalf("second audio frame = %s %v", secondFrame.Type, secondFrame.Audio)
	}
	thirdFrame := waitEvent(t, events)
	if thirdFrame.Type != protocol.EventAudioFrame || string(thirdFrame.Audio) != string([]byte{7}) {
		t.Fatalf("third audio frame = %s %v", thirdFrame.Type, thirdFrame.Audio)
	}
	// audio.started is emitted exactly once, not per frame.
	if secondFrame.Type == protocol.EventAudioStarted || thirdFrame.Type == protocol.EventAudioStarted {
		t.Fatal("audio.started must be emitted once per stream")
	}
	alignment := waitEvent(t, events)
	if alignment.Type != protocol.EventAlignment || decodeString(t, alignment.Data, "text") != "Hello," {
		t.Fatalf("alignment event = %s %s", alignment.Type, alignment.Data)
	}
	if got := decodeNumber(t, alignment.Data, "audio_end_ms"); got != 160 {
		t.Fatalf("alignment audio_end_ms = %v", got)
	}
	done := waitEvent(t, events)
	if done.Type != protocol.EventAudioDone {
		t.Fatalf("terminal event = %s, want audio.done", done.Type)
	}
	if done.Extensions[extensionID] == nil {
		t.Fatal("audio.done must retain the raw Gradium frame")
	}
	if event, ok := <-events; ok {
		t.Fatalf("events must close after end_of_stream, got %s", event.Type)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}

	select {
	case received := <-requests:
		if got := received.Header.Get("x-api-key"); got != "customer-gradium-key" {
			t.Fatalf("x-api-key = %q", got)
		}
		if got := received.Header.Get("Authorization"); got != "" {
			t.Fatalf("Gradium must not receive an Authorization header, got %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not observe the websocket handshake")
	}
}

// TestTTSAdapterFallsBackToPlanVoice covers the "auto" routing case: a caller
// that did not name a vendor cannot name a vendor voice either, so the control
// plane's choice on the plan is the only usable one.
func TestTTSAdapterFallsBackToPlanVoice(t *testing.T) {
	t.Parallel()

	setups := make(chan map[string]any, 1)
	server := newSocketServer(t, "/api/speech/tts", func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		frame, err := readFrame(ctx, conn)
		if err != nil {
			t.Errorf("read setup: %v", err)
			return
		}
		setups <- frame
		<-ctx.Done()
	})
	defer server.Close()

	adapter, err := NewTTS(ttsConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	request := ttsRequest(server.URL)
	request.Options.Voice = ""
	request.Plan.Route.Voice = "LFZvm12tW_z0xfGo"
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()

	select {
	case setup := <-setups:
		if got := setup["voice_id"]; got != "LFZvm12tW_z0xfGo" {
			t.Fatalf("voice_id = %v, want the plan voice", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never received setup")
	}
}

// TestTTSAdapterMapsSampleRateOntoOutputFormat guards against the bare "pcm"
// spelling ever reappearing: Gradium reads it as 48 kHz on this socket and
// 24 kHz on the ASR socket, so only an explicit rate is safe.
func TestTTSAdapterMapsSampleRateOntoOutputFormat(t *testing.T) {
	t.Parallel()

	setups := make(chan map[string]any, 1)
	server := newSocketServer(t, "/api/speech/tts", func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		frame, err := readFrame(ctx, conn)
		if err != nil {
			t.Errorf("read setup: %v", err)
			return
		}
		setups <- frame
		<-ctx.Done()
	})
	defer server.Close()

	adapter, err := NewTTS(ttsConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	request := ttsRequest(server.URL)
	request.Media.SampleRateHz = 8_000
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()

	select {
	case setup := <-setups:
		if got := setup["output_format"]; got != "pcm_8000" {
			t.Fatalf("output_format = %v, want pcm_8000", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never received setup")
	}
}

// TestTTSAdapterClassifiesErrorFrames mirrors the STT table because both
// sockets share one error vocabulary; if the two ever diverge, one of these
// two tables breaks and the divergence gets noticed.
func TestTTSAdapterClassifiesErrorFrames(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name          string
		code          int
		message       string
		wantCode      string
		wantRetryable bool
	}{
		{"protocol violation", 1002, "Session not found. Send setup first.", "invalid_request", false},
		{"revoked key", 1008, "API key is revoked or expired", "authentication_failed", false},
		{"missing subscription", 1008, "Subscription is missing or inactive", "provider_quota_exceeded", false},
		{"unknown voice", 1008, "Unknown voice id", "invalid_request", false},
		{"server fault", 1011, "Internal server error", "provider_unavailable", true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			server := newSocketServer(t, "/api/speech/tts", func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
				if _, err := readFrame(ctx, conn); err != nil {
					t.Errorf("read setup: %v", err)
					return
				}
				if err := writeJSON(ctx, conn, map[string]any{
					"type": "error", "message": testCase.message, "code": testCase.code,
				}); err != nil {
					t.Errorf("write error frame: %v", err)
					return
				}
				<-ctx.Done()
			})
			defer server.Close()

			adapter, err := NewTTS(ttsConfig(server.URL))
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			stream, err := adapter.Open(context.Background(), ttsRequest(server.URL))
			if err != nil {
				t.Fatalf("open stream: %v", err)
			}
			providerError := awaitTerminalError(t, stream.Events())
			if providerError.Code != testCase.wantCode {
				t.Fatalf("code = %q, want %q", providerError.Code, testCase.wantCode)
			}
			if providerError.Retryable != testCase.wantRetryable {
				t.Fatalf("retryable = %v, want %v", providerError.Retryable, testCase.wantRetryable)
			}
			if providerError.ProviderStatus != testCase.code {
				t.Fatalf("provider status = %d, want %d", providerError.ProviderStatus, testCase.code)
			}
			if _, ok := <-stream.Events(); ok {
				t.Fatal("events must close after a terminal error frame")
			}
		})
	}
}

// TestTTSAdapterClassifiesHandshakeFailures keeps provider_rate_limited
// reachable on this socket too: Gradium publishes no in-band 429.
func TestTTSAdapterClassifiesHandshakeFailures(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name     string
		status   int
		wantCode string
	}{
		{"unauthorized", http.StatusUnauthorized, "authentication_failed"},
		{"payment required", http.StatusPaymentRequired, "provider_quota_exceeded"},
		{"rate limited", http.StatusTooManyRequests, "provider_rate_limited"},
		{"bad request", http.StatusBadRequest, "invalid_request"},
		{"upstream down", http.StatusBadGateway, "provider_unavailable"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			server := newSocketServer(t, "/never-upgraded", func(context.Context, *http.Request, *websocket.Conn) {})
			server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(testCase.status)
			})
			defer server.Close()

			adapter, err := NewTTS(ttsConfig(server.URL))
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			_, err = adapter.Open(context.Background(), ttsRequest(server.URL))
			var providerError *runtimepkg.ProviderError
			if !errors.As(err, &providerError) {
				t.Fatalf("open error = %v, want a ProviderError", err)
			}
			if providerError.Code != testCase.wantCode {
				t.Fatalf("code = %q, want %q", providerError.Code, testCase.wantCode)
			}
			if strings.Contains(providerError.Error(), "customer-gradium-key") {
				t.Fatalf("handshake error leaked the credential: %q", providerError.Error())
			}
		})
	}
}

// TestTTSAdapterRejectsUnroutableRequests refuses work that is not this
// adapter's before a socket is opened or a secret is attached.
func TestTTSAdapterRejectsUnroutableRequests(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name    string
		mutate  func(*runtimepkg.AdapterRequest)
		wantSub string
	}{
		{"stt session", func(r *runtimepkg.AdapterRequest) { r.Kind = protocol.SessionKindSTT }, "tts sessions"},
		{"other provider", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Provider = "cartesia" }, "cannot open provider"},
		{"http transport", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Transport = protocol.TransportHTTP }, "websocket transport"},
		{"auto model", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "auto" }, "concrete model"},
		{"empty model", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "" }, "concrete model"},
		// Gradium would happily substitute a house voice; billing for a voice
		// nobody chose is worse than a failed open.
		{"no voice anywhere", func(r *runtimepkg.AdapterRequest) {
			r.Options.Voice = ""
			r.Plan.Route.Voice = ""
		}, "voice id"},
		{"no credential", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential = nil }, "bearer credential"},
		{"blank credential", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential.Value = "  " }, "bearer credential"},
		{"wrong credential kind", func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Credential.Kind = protocol.CredentialKind("basic")
		}, "bearer credential"},
		{"no media", func(r *runtimepkg.AdapterRequest) { r.Media = nil }, "media configuration"},
		{"opus media", func(r *runtimepkg.AdapterRequest) { r.Media.Encoding = "opus" }, "pcm_s16le"},
		{"stereo media", func(r *runtimepkg.AdapterRequest) { r.Media.Channels = 2 }, "mono only"},
		{"unsupported rate", func(r *runtimepkg.AdapterRequest) { r.Media.SampleRateHz = 32_000 }, "32000 Hz pcm"},
		{"endpoint on other socket", func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Endpoint = strings.Replace(r.Plan.Route.Endpoint, "/api/speech/tts", "/api/speech/asr", 1)
		}, "/api/speech/tts"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			adapter, err := NewTTS(TTSConfig{})
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			request := ttsRequest("http://127.0.0.1:1")
			request.Plan.Route.Endpoint = "wss://api.gradium.ai/api/speech/tts"
			request.Plan.Route.Credential.Value = "secret-that-must-not-leak"
			testCase.mutate(&request)
			_, err = adapter.Open(context.Background(), request)
			if err == nil {
				t.Fatal("adapter accepted an unroutable request")
			}
			if !strings.Contains(err.Error(), testCase.wantSub) {
				t.Fatalf("error = %q, want it to mention %q", err.Error(), testCase.wantSub)
			}
			if strings.Contains(err.Error(), "secret-that-must-not-leak") {
				t.Fatalf("validation error leaked the credential: %q", err.Error())
			}
		})
	}
}

// TestTTSAdapterRejectsAudioOperations pins the modality boundary from the
// other side.
func TestTTSAdapterRejectsAudioOperations(t *testing.T) {
	t.Parallel()

	server := newSocketServer(t, "/api/speech/tts", func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		_, _ = readFrame(ctx, conn)
		<-ctx.Done()
	})
	defer server.Close()

	adapter, err := NewTTS(ttsConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), ttsRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()

	if err := stream.WriteAudio(context.Background(), []byte{1}); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Fatalf("write audio = %v", err)
	}
	if err := stream.CommitAudio(context.Background()); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Fatalf("commit audio = %v", err)
	}
	if err := stream.AppendText(context.Background(), "   "); err == nil {
		t.Fatal("blank transcript must be rejected")
	}
}
