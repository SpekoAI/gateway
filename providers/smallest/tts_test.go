package smallest

import (
	"context"
	"encoding/base64"
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

func TestTTSAdapterRequestShapeAndStreamingFrames(t *testing.T) {
	t.Parallel()
	requests := make(chan *http.Request, 1)
	payloads := make(chan []byte, 1)
	server := newSmallestTTSServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		requests <- request.Clone(request.Context())
		messageType, payload, err := conn.Read(ctx)
		if err != nil || messageType != websocket.MessageText {
			t.Errorf("request frame = (%v, %v)", messageType, err)
			return
		}
		payloads <- payload

		// More than one audio frame before the terminator: the point of the
		// streaming surface is that playback starts before synthesis finishes.
		for _, chunk := range []string{"first-audio", "second-audio"} {
			frame := map[string]any{
				"session_id": "ws_1", "request_id": "task_1", "status": "chunk",
				"data": map[string]any{"audio": base64.StdEncoding.EncodeToString([]byte(chunk))},
			}
			if err := writeSmallestJSON(ctx, conn, frame); err != nil {
				t.Errorf("write chunk: %v", err)
				return
			}
		}
		// The terminator carries NO `done` field on the WebSocket. `done`
		// belongs to the SSE twin, so keying on it here would hang forever.
		if err := writeSmallestJSON(ctx, conn, map[string]any{
			"session_id": "ws_1", "request_id": "task_1", "status": "complete",
		}); err != nil {
			t.Errorf("write complete: %v", err)
		}
	})
	defer server.Close()

	adapter, err := New(smallestTTSConfig(server.URL))
	if err != nil {
		t.Fatalf("new TTS adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), smallestTTSRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	// Lightning has no continuation protocol, so two fragments must arrive as
	// one `text` value rather than two requests.
	if err := stream.AppendText(context.Background(), "Hello "); err != nil {
		t.Fatalf("append text: %v", err)
	}
	if err := stream.AppendText(context.Background(), "world."); err != nil {
		t.Fatalf("append text: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit text: %v", err)
	}

	events := collectSmallestEvents(t, stream.Events(), 5)
	want := []protocol.EventType{
		protocol.EventUsageObserved,
		protocol.EventAudioStarted,
		protocol.EventAudioFrame,
		protocol.EventAudioFrame,
		protocol.EventAudioDone,
	}
	for index := range want {
		if events[index].Type != want[index] {
			t.Fatalf("event %d = %q, want %q", index, events[index].Type, want[index])
		}
	}
	// Audio must be base64-decoded from data.audio and handed over as bytes.
	if string(events[2].Audio) != "first-audio" || string(events[3].Audio) != "second-audio" {
		t.Fatalf("audio frames = %q,%q", events[2].Audio, events[3].Audio)
	}
	var usage struct {
		ProviderRequestID string `json:"provider_request_id"`
	}
	// request_id is the per-synthesis task id and the one a support ticket can
	// be traced with; session_id only names the socket.
	if err := json.Unmarshal(events[0].Data, &usage); err != nil || usage.ProviderRequestID != "task_1" {
		t.Fatalf("usage = %+v, err=%v, want the task request id", usage, err)
	}

	select {
	case received := <-requests:
		if got := received.Header.Get("Authorization"); got != "Bearer customer-smallest-key" {
			t.Fatalf("Authorization = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not observe the handshake")
	}

	select {
	case payload := <-payloads:
		// Assert the request by FIELD NAME against the documented wire format.
		// Decoding into a map (rather than the adapter's own struct) is what
		// makes a renamed JSON tag fail here.
		var request map[string]any
		if err := json.Unmarshal(payload, &request); err != nil {
			t.Fatalf("request json: %v", err)
		}
		for key, want := range map[string]any{
			"text":          "Hello world.",
			"voice_id":      "olivia",
			"model":         "lightning_v3.1",
			"sample_rate":   float64(24_000),
			"output_format": "pcm",
			"language":      "en",
		} {
			if got := request[key]; got != want {
				t.Fatalf("request[%q] = %#v, want %#v", key, got, want)
			}
		}
		// A stray field is as much of a bug as a missing one: an undocumented
		// key can be rejected outright by a strict validator upstream.
		if len(request) != 6 {
			t.Fatalf("request carried %d fields (%v), want exactly the 6 documented ones", len(request), request)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not receive a synthesis request")
	}

	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}
}

// The SSE twin of this endpoint returns {"audio": ..., "done": ...} at the top
// level. A parser that accepted that shape here would be reading a format this
// socket never sends; assert the adapter is not quietly bilingual.
func TestTTSAdapterIgnoresTheSSEFrameShape(t *testing.T) {
	t.Parallel()
	server := newSmallestTTSServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		if _, _, err := conn.Read(ctx); err != nil {
			return
		}
		// SSE-shaped: audio at the top level, status as an HTTP-ish code.
		_ = writeSmallestJSON(ctx, conn, map[string]any{
			"audio": base64.StdEncoding.EncodeToString([]byte("sse-audio")), "done": false, "status": "206",
		})
		// And the SSE terminator, which must NOT end this stream either.
		_ = writeSmallestJSON(ctx, conn, map[string]any{"status": "200", "done": true})
		// The real WebSocket terminator.
		_ = writeSmallestJSON(ctx, conn, map[string]any{"status": "complete", "request_id": "task_1"})
	})
	defer server.Close()

	adapter, err := New(smallestTTSConfig(server.URL))
	if err != nil {
		t.Fatalf("new TTS adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), smallestTTSRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.AppendText(context.Background(), "hello"); err != nil {
		t.Fatalf("append text: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit text: %v", err)
	}
	events := collectSmallestEvents(t, stream.Events(), 4)
	want := []protocol.EventType{
		protocol.EventWarning, // "206" is not a WebSocket status value
		protocol.EventWarning, // neither is "200"
		protocol.EventUsageObserved,
		protocol.EventAudioDone,
	}
	for index := range want {
		if events[index].Type != want[index] {
			t.Fatalf("event %d = %q, want %q (SSE frames must not be treated as audio)", index, events[index].Type, want[index])
		}
	}
	for _, event := range events {
		if len(event.Audio) != 0 {
			t.Fatalf("an SSE-shaped frame produced audio: %q", event.Audio)
		}
	}
}

// Barge-in: Lightning documents no interrupt frame, so cancellation has to be
// local. Frames after Cancel must be dropped and no audio.done emitted, or the
// caller plays speech the user already talked over.
func TestTTSAdapterCancelDropsRemainingAudio(t *testing.T) {
	t.Parallel()
	cancelled := make(chan struct{})
	server := newSmallestTTSServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		if _, _, err := conn.Read(ctx); err != nil {
			return
		}
		frame := map[string]any{
			"request_id": "task_1", "status": "chunk",
			"data": map[string]any{"audio": base64.StdEncoding.EncodeToString([]byte("before-cancel"))},
		}
		_ = writeSmallestJSON(ctx, conn, frame)
		<-cancelled
		frame["data"] = map[string]any{"audio": base64.StdEncoding.EncodeToString([]byte("after-cancel"))}
		_ = writeSmallestJSON(ctx, conn, frame)
		_ = writeSmallestJSON(ctx, conn, map[string]any{"status": "complete", "request_id": "task_1"})
	})
	defer server.Close()

	adapter, err := New(smallestTTSConfig(server.URL))
	if err != nil {
		t.Fatalf("new TTS adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), smallestTTSRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.AppendText(context.Background(), "hello"); err != nil {
		t.Fatalf("append text: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit text: %v", err)
	}
	events := collectSmallestEvents(t, stream.Events(), 3)
	if string(events[2].Audio) != "before-cancel" {
		t.Fatalf("pre-cancel audio = %q", events[2].Audio)
	}
	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	close(cancelled)

	// The channel must close without another audio frame and without audio.done.
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-stream.Events():
			if !ok {
				return
			}
			if event.Err != nil {
				t.Fatalf("unexpected error after cancel: %v", event.Err)
			}
			t.Fatalf("event %q leaked after cancel (audio=%q)", event.Type, event.Audio)
		case <-timer.C:
			// Nothing leaked; the provider closed after `complete` and the read
			// loop is idle. Tear down explicitly.
			abortSmallestStream(t, stream)
			return
		}
	}
}

func TestTTSAdapterRejectsUnsupportedRequests(t *testing.T) {
	t.Parallel()
	server := newSmallestTTSServer(t, func(context.Context, *http.Request, *websocket.Conn) {})
	defer server.Close()
	adapter, err := New(smallestTTSConfig(server.URL))
	if err != nil {
		t.Fatalf("new TTS adapter: %v", err)
	}

	for name, testCase := range map[string]struct {
		mutate func(*runtimepkg.AdapterRequest)
		want   string
	}{
		"wrong kind": {
			mutate: func(request *runtimepkg.AdapterRequest) { request.Kind = protocol.SessionKindSTT },
			want:   "supports tts sessions",
		},
		"wrong provider": {
			mutate: func(request *runtimepkg.AdapterRequest) { request.Plan.Route.Provider = "cartesia" },
			want:   "cannot open provider",
		},
		// Same credential story as STT: no delegated credential exists for
		// Waves, so a managed provider-direct plan cannot be honoured honestly.
		"managed credential source": {
			mutate: func(request *runtimepkg.AdapterRequest) {
				request.Plan.Execution.CredentialSource = protocol.CredentialsManaged
			},
			want: "BYOK-only",
		},
		"non-bearer credential": {
			mutate: func(request *runtimepkg.AdapterRequest) {
				request.Plan.Route.Credential.Kind = protocol.CredentialSessionURL
			},
			want: "requires a bearer credential",
		},
		// relay_access is accepted only on the relay route; on provider-direct
		// it means the control plane mislabeled the plan.
		"relay_access kind off the relay route": {
			mutate: func(request *runtimepkg.AdapterRequest) {
				request.Plan.Route.Credential.Kind = protocol.CredentialRelayAccess
			},
			want: "requires a bearer credential",
		},
		"auto model": {
			mutate: func(request *runtimepkg.AdapterRequest) { request.Plan.Route.Model = "auto" },
			want:   "concrete model",
		},
		// voice_id is documented as required and Lightning publishes no
		// per-model default voice.
		"missing voice": {
			mutate: func(request *runtimepkg.AdapterRequest) { request.Options.Voice = " " },
			want:   "voice id",
		},
		// Lightning's output_format set is pcm/mp3/wav/ulaw/alaw — no opus,
		// even though the canonical MediaFormat allows it and Pulse STT
		// accepts it on the way in.
		"opus output": {
			mutate: func(request *runtimepkg.AdapterRequest) { request.Media.Encoding = "opus" },
			want:   "pcm_s16le",
		},
		// 48 kHz is a legal canonical rate and a legal Pulse STT input rate,
		// but Lightning caps sample_rate at 44100.
		"sample rate above the lightning ceiling": {
			mutate: func(request *runtimepkg.AdapterRequest) { request.Media.SampleRateHz = 48_000 },
			want:   "sample rate must be between 8000 and 44100",
		},
		"stereo output": {
			mutate: func(request *runtimepkg.AdapterRequest) { request.Media.Channels = 2 },
			want:   "mono audio",
		},
		"http transport": {
			mutate: func(request *runtimepkg.AdapterRequest) { request.Plan.Route.Transport = protocol.TransportHTTP },
			want:   "websocket transport",
		},
		"wrong endpoint path": {
			mutate: func(request *runtimepkg.AdapterRequest) {
				request.Plan.Route.Endpoint = strings.Replace(request.Plan.Route.Endpoint, "/waves/v1/tts/live", "/waves/v1/tts", 1)
			},
			want: "/waves/v1/tts/live",
		},
	} {
		open := smallestTTSRequest(server.URL)
		testCase.mutate(&open)
		if _, err := adapter.Open(context.Background(), open); err == nil || !strings.Contains(err.Error(), testCase.want) {
			t.Fatalf("%s error = %v, want it to mention %q", name, err, testCase.want)
		}
	}
}

// A relay plan is managed for billing purposes but carries the relay
// connector's permanent Smallest key, which is exactly the account key the
// Waves APIs are documented to take and rides the same Authorization: Bearer
// header as a BYOK key. It is the one managed construction the adapter
// accepts.
func TestTTSAdapterUsesBearerHeaderForRelayRoute(t *testing.T) {
	t.Parallel()
	requests := make(chan *http.Request, 1)
	server := newSmallestTTSServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		requests <- request.Clone(request.Context())
		_, _, _ = conn.Read(ctx)
	})
	defer server.Close()
	adapter, err := New(smallestTTSConfig(server.URL))
	if err != nil {
		t.Fatalf("new TTS adapter: %v", err)
	}
	request := smallestTTSRequest(server.URL)
	request.Plan.Execution.ProviderRoute = protocol.RouteSpekoRelay
	request.Plan.Execution.CredentialSource = protocol.CredentialsManaged
	request.Plan.Route.Credential.Value = "connector-smallest-key"
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open relay stream: %v", err)
	}
	defer abortSmallestStream(t, stream)

	select {
	case received := <-requests:
		if got := received.Header.Get("Authorization"); got != "Bearer connector-smallest-key" {
			t.Fatalf("Authorization = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not observe the relay handshake")
	}
}

// protocol.SessionPlan validation requires a relay plan to label its
// credential relay_access, while the relay connector that synthesizes the
// plan and drives this adapter directly labels the same permanent key bearer.
// The relay arm must accept both spellings, or one of the two constructions
// becomes quietly unreachable.
func TestTTSAdapterAcceptsRelayAccessCredentialKindOnRelayRoute(t *testing.T) {
	t.Parallel()
	server := newSmallestTTSServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		_, _, _ = conn.Read(ctx)
	})
	defer server.Close()
	adapter, err := New(smallestTTSConfig(server.URL))
	if err != nil {
		t.Fatalf("new TTS adapter: %v", err)
	}
	request := smallestTTSRequest(server.URL)
	request.Plan.Execution.ProviderRoute = protocol.RouteSpekoRelay
	request.Plan.Execution.CredentialSource = protocol.CredentialsManaged
	request.Plan.Route.Credential.Kind = protocol.CredentialRelayAccess
	request.Plan.Route.Credential.Value = "connector-smallest-key"
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open relay stream with relay_access credential: %v", err)
	}
	abortSmallestStream(t, stream)
}

func TestTTSAdapterClassifiesErrors(t *testing.T) {
	t.Parallel()
	for name, testCase := range map[string]struct {
		frame         map[string]any
		wantCode      string
		wantStatus    int
		wantRetryable bool
	}{
		"rate limited": {
			frame:         map[string]any{"status": "error", "status_code": 429, "message": "too many requests"},
			wantCode:      "provider_rate_limited",
			wantStatus:    429,
			wantRetryable: true,
		},
		// 403 on Smallest means the key exists but lacks access to the
		// resource — a configuration problem, never worth a retry.
		"forbidden": {
			frame:      map[string]any{"status": "error", "status_code": 403, "message": "no access"},
			wantCode:   "authentication_failed",
			wantStatus: 403,
		},
		"bare error string": {
			frame:         map[string]any{"error": "synthesis failed"},
			wantCode:      "provider_unavailable",
			wantRetryable: true,
		},
	} {
		frame := testCase.frame
		server := newSmallestTTSServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
			if _, _, err := conn.Read(ctx); err != nil {
				return
			}
			_ = writeSmallestJSON(ctx, conn, frame)
		})
		adapter, err := New(smallestTTSConfig(server.URL))
		if err != nil {
			t.Fatalf("new TTS adapter: %v", err)
		}
		stream, err := adapter.Open(context.Background(), smallestTTSRequest(server.URL))
		if err != nil {
			t.Fatalf("%s open: %v", name, err)
		}
		if err := stream.AppendText(context.Background(), "hello"); err != nil {
			t.Fatalf("%s append: %v", name, err)
		}
		if err := stream.CommitText(context.Background()); err != nil {
			t.Fatalf("%s commit: %v", name, err)
		}
		event := awaitSmallestFailure(t, stream.Events())
		var providerErr *runtimepkg.ProviderError
		if !errors.As(event.Err, &providerErr) {
			t.Fatalf("%s error = %#v", name, event.Err)
		}
		if providerErr.Code != testCase.wantCode || providerErr.ProviderStatus != testCase.wantStatus || providerErr.Retryable != testCase.wantRetryable {
			t.Fatalf("%s = code %q status %d retryable %v; want %q/%d/%v",
				name, providerErr.Code, providerErr.ProviderStatus, providerErr.Retryable,
				testCase.wantCode, testCase.wantStatus, testCase.wantRetryable)
		}
		if providerErr.Extensions[extensionID] == nil {
			t.Fatalf("%s dropped the raw provider payload", name)
		}
		server.Close()
	}
}

func TestTTSAdapterRejectsAudioInputAndEmptyUtterances(t *testing.T) {
	t.Parallel()
	server := newSmallestTTSServer(t, func(context.Context, *http.Request, *websocket.Conn) {})
	defer server.Close()
	adapter, err := New(smallestTTSConfig(server.URL))
	if err != nil {
		t.Fatalf("new TTS adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), smallestTTSRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer func() { abortSmallestStream(t, stream) }()

	// A TTS socket has no audio input direction; the runtime must get the
	// canonical sentinel rather than a provider-specific error.
	if err := stream.WriteAudio(context.Background(), []byte{1}); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Fatalf("WriteAudio = %v, want ErrUnsupportedOperation", err)
	}
	if err := stream.CommitAudio(context.Background()); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Fatalf("CommitAudio = %v, want ErrUnsupportedOperation", err)
	}
	// Committing nothing would send {"text":""}, which Lightning bills and
	// rejects; catch it locally.
	if err := stream.CommitText(context.Background()); err == nil || !strings.Contains(err.Error(), "no buffered text") {
		t.Fatalf("empty CommitText = %v", err)
	}
}

// -- fixtures ---------------------------------------------------------------

func newSmallestTTSServer(t *testing.T, callback func(context.Context, *http.Request, *websocket.Conn)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/waves/v1/tts/live" {
			http.NotFound(w, r)
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		go func() {
			defer conn.CloseNow()
			callback(context.Background(), r, conn)
		}()
	}))
}

func smallestTTSRequest(serverURL string) runtimepkg.AdapterRequest {
	now := time.Date(2026, time.August, 7, 12, 0, 0, 0, time.UTC)
	endpoint, _ := url.Parse(serverURL)
	endpoint.Scheme = "ws"
	endpoint.Path = "/waves/v1/tts/live"
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindTTS,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{
				Placement:        protocol.PlacementEmbedded,
				ProviderRoute:    protocol.RouteProviderDirect,
				CredentialSource: protocol.CredentialsBYOK,
			},
			Route: protocol.PlanRoute{
				Provider: "smallest", Model: "lightning_v3.1", Adapter: AdapterID,
				Transport: protocol.TransportWebSocket, Endpoint: endpoint.String(),
				Credential: &protocol.DelegatedCredential{
					Kind: protocol.CredentialBearer, Value: "customer-smallest-key", ExpiresAt: now.Add(time.Hour),
				},
			},
		},
		Options: protocol.RequestOptions{Voice: "olivia", Language: "en-US"},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1},
	}
}

func smallestTTSConfig(serverURL string) Config {
	endpoint, _ := url.Parse(serverURL)
	return Config{AllowedEndpointHosts: []string{endpoint.Hostname()}, AllowInsecureEndpoint: true}
}
