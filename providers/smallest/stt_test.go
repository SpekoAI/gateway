package smallest

import (
	"context"
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

// Every vendor string in this file is transcribed as a literal rather than
// referenced from the adapter's constants. A test that compares the adapter
// against its own constant passes just as happily when the constant is
// misspelled, which defeats the point of asserting a wire format.

func TestSTTAdapterHandshakeAudioAndTranscriptStream(t *testing.T) {
	t.Parallel()
	requests := make(chan *http.Request, 1)
	server := newSmallestSTTServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		requests <- request.Clone(request.Context())

		// Audio must travel as BINARY frames; Pulse reads raw PCM off the same
		// socket that carries the JSON control frames, so a text frame here
		// would be parsed as a (malformed) command.
		messageType, audio, err := conn.Read(ctx)
		if err != nil || messageType != websocket.MessageBinary || string(audio) != "\x01\x02\x03\x04" {
			t.Errorf("audio = (%v, %q, %v), want binary \"\\x01\\x02\\x03\\x04\"", messageType, audio, err)
			return
		}

		// CommitAudio must map to finalize, NOT close_stream. finalize forces an
		// is_final transcript and keeps the session open; close_stream ends it.
		// Confusing the two silently truncates a multi-turn call after one turn.
		if err := assertSmallestText(ctx, conn, `{"type":"finalize"}`); err != nil {
			t.Errorf("commit: %v", err)
			return
		}

		for _, message := range []map[string]any{
			// An empty interim at session start must not surface as an event.
			{"type": "transcription", "status": "success", "session_id": "sess_1", "transcript": "", "is_final": false, "is_last": false},
			{"type": "transcription", "status": "success", "session_id": "sess_1", "transcript": "hello", "is_final": false, "is_last": false},
			{"type": "transcription", "status": "success", "session_id": "sess_1", "transcript": "hello world", "is_final": true, "is_last": false, "language": "en",
				"words": []map[string]any{{"word": "hello", "start": 0.0, "end": 0.3, "confidence": 0.9}}},
		} {
			if err := writeSmallestJSON(ctx, conn, message); err != nil {
				t.Errorf("write transcript: %v", err)
				return
			}
		}

		// Close must send close_stream and then WAIT: Smallest's docs say the
		// last transcript only arrives after it.
		if err := assertSmallestText(ctx, conn, `{"type":"close_stream"}`); err != nil {
			t.Errorf("close: %v", err)
			return
		}
		if err := writeSmallestJSON(ctx, conn, map[string]any{
			"type": "transcription", "status": "success", "session_id": "sess_1",
			"transcript": "goodbye", "is_final": true, "is_last": true, "language": "en",
		}); err != nil {
			t.Errorf("write is_last: %v", err)
		}
	})
	defer server.Close()

	adapter, err := NewSTT(smallestSTTConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), smallestSTTRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.WriteAudio(context.Background(), []byte{1, 2, 3, 4}); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	if err := stream.CommitAudio(context.Background()); err != nil {
		t.Fatalf("commit audio: %v", err)
	}

	// Partials must reach the caller DURING the stream, before anything is
	// finalized — that is the whole reason for using the streaming surface.
	events := collectSmallestEvents(t, stream.Events(), 5)
	want := []protocol.EventType{
		protocol.EventUsageObserved,
		protocol.EventSpeechStarted,
		protocol.EventTranscriptDelta,
		protocol.EventTranscriptFinal,
		protocol.EventSpeechEnded,
	}
	for index := range want {
		if events[index].Type != want[index] {
			t.Fatalf("event %d = %q, want %q", index, events[index].Type, want[index])
		}
	}
	// The empty interim produced no event, so the delta here is "hello".
	var delta struct {
		Text    string `json:"text"`
		IsFinal bool   `json:"is_final"`
	}
	if err := json.Unmarshal(events[2].Data, &delta); err != nil || delta.Text != "hello" || delta.IsFinal {
		t.Fatalf("delta = %+v, err=%v (an empty interim must not have been emitted)", delta, err)
	}
	var final struct {
		Text              string          `json:"text"`
		IsFinal           bool            `json:"is_final"`
		Language          string          `json:"language"`
		Words             json.RawMessage `json:"words"`
		ProviderRequestID string          `json:"provider_request_id"`
	}
	if err := json.Unmarshal(events[3].Data, &final); err != nil {
		t.Fatalf("final data: %v", err)
	}
	if final.Text != "hello world" || !final.IsFinal || final.Language != "en" || final.ProviderRequestID != "sess_1" {
		t.Fatalf("final = %+v", final)
	}
	// Word timings are the precondition for adaptive interruption downstream,
	// so losing them is a silent capability regression.
	if !strings.Contains(string(final.Words), `"hello"`) {
		t.Fatalf("final words = %s, want the per-word array preserved", final.Words)
	}
	if events[3].Extensions[extensionID] == nil {
		t.Fatal("final transcript dropped the raw Smallest payload extension")
	}

	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}
	// The trailing is_last transcript must still be delivered after Close. It
	// is a NEW segment, so it opens its own speech.started: Pulse finalizes
	// per segment and keeps listening, and a turn detector downstream needs
	// the boundary on every turn, not only the first.
	tail := collectSmallestEvents(t, stream.Events(), 3)
	tailWant := []protocol.EventType{
		protocol.EventSpeechStarted,
		protocol.EventTranscriptFinal,
		protocol.EventSpeechEnded,
	}
	for index := range tailWant {
		if tail[index].Type != tailWant[index] {
			t.Fatalf("tail event %d = %q, want %q", index, tail[index].Type, tailWant[index])
		}
	}
	var last struct {
		Text   string `json:"text"`
		IsLast bool   `json:"is_last"`
	}
	if err := json.Unmarshal(tail[1].Data, &last); err != nil || last.Text != "goodbye" || !last.IsLast {
		t.Fatalf("is_last transcript = %+v, err=%v", last, err)
	}
	select {
	case _, ok := <-stream.Events():
		if ok {
			t.Fatal("events remained open after is_last")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the stream to end after is_last")
	}

	select {
	case received := <-requests:
		// Auth is a header, never a query parameter: Smallest documents no
		// token-in-URL form for Waves, and a URL credential lands in proxy logs.
		if got := received.Header.Get("Authorization"); got != "Bearer customer-smallest-key" {
			t.Fatalf("Authorization = %q", got)
		}
		query := received.URL.Query()
		for key, want := range map[string]string{
			"model":           "pulse",
			"encoding":        "linear16",
			"sample_rate":     "16000",
			"word_timestamps": "true",
			"language":        "en",
		} {
			if got := query.Get(key); got != want {
				t.Fatalf("query %s = %q, want %q", key, got, want)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("server did not observe the handshake")
	}
}

// Language handling has two traps: a region subtag Pulse does not understand,
// and the regional aggregators whose names contain a hyphen and must survive
// intact.
func TestSTTAdapterNormalizesLanguageButKeepsAggregators(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct{ in, want string }{
		{in: "en-US", want: "en"},
		{in: "PT-BR", want: "pt"},
		// A naive split on "-" turns this into "multi", which Pulse rejects.
		{in: "multi-south-indic", want: "multi-south-indic"},
		{in: "north_indic", want: "north_indic"},
		{in: "", want: ""},
	} {
		requests := make(chan *http.Request, 1)
		server := newSmallestSTTServer(t, func(_ context.Context, request *http.Request, _ *websocket.Conn) {
			requests <- request.Clone(request.Context())
		})
		adapter, err := NewSTT(smallestSTTConfig(server.URL))
		if err != nil {
			t.Fatalf("new STT adapter: %v", err)
		}
		open := smallestSTTRequest(server.URL)
		open.Options.Language = testCase.in
		stream, err := adapter.Open(context.Background(), open)
		if err != nil {
			t.Fatalf("open %q: %v", testCase.in, err)
		}
		select {
		case received := <-requests:
			if got := received.URL.Query().Get("language"); got != testCase.want {
				t.Fatalf("language %q => query %q, want %q", testCase.in, got, testCase.want)
			}
			// An absent language must be absent, not empty: Pulse then applies
			// its own default instead of being pinned to "".
			if testCase.want == "" && received.URL.Query().Has("language") {
				t.Fatal("empty language should omit the parameter entirely")
			}
		case <-time.After(time.Second):
			t.Fatalf("server did not observe the %q handshake", testCase.in)
		}
		abortSmallestStream(t, stream)
		server.Close()
	}
}

func TestSTTAdapterRejectsUnsupportedRequests(t *testing.T) {
	t.Parallel()
	server := newSmallestSTTServer(t, func(context.Context, *http.Request, *websocket.Conn) {})
	defer server.Close()
	adapter, err := NewSTT(smallestSTTConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}

	for name, testCase := range map[string]struct {
		mutate func(*runtimepkg.AdapterRequest)
		want   string
	}{
		// A TTS plan routed to the STT adapter is a control-plane bug; failing
		// loudly beats opening a socket that will never transcribe.
		"wrong kind": {
			mutate: func(request *runtimepkg.AdapterRequest) { request.Kind = protocol.SessionKindTTS },
			want:   "supports stt sessions",
		},
		// The adapter must never speak for a provider it does not implement,
		// which would send a customer's Smallest key to someone else's host.
		"wrong provider": {
			mutate: func(request *runtimepkg.AdapterRequest) { request.Plan.Route.Provider = "deepgram" },
			want:   "cannot open provider",
		},
		// Waves publishes no delegated credential, so a managed plan could only
		// be carrying the customer's root API key.
		"managed credential source": {
			mutate: func(request *runtimepkg.AdapterRequest) {
				request.Plan.Execution.CredentialSource = protocol.CredentialsManaged
			},
			want: "BYOK-only",
		},
		"non-bearer credential": {
			mutate: func(request *runtimepkg.AdapterRequest) {
				request.Plan.Route.Credential.Kind = protocol.CredentialSignedURL
			},
			want: "requires a bearer credential",
		},
		"empty credential": {
			mutate: func(request *runtimepkg.AdapterRequest) { request.Plan.Route.Credential.Value = "  " },
			want:   "requires a bearer credential",
		},
		// "auto" is a routing placeholder. Forwarding it as ?model=auto would
		// bill a request Smallest rejects.
		"auto model": {
			mutate: func(request *runtimepkg.AdapterRequest) { request.Plan.Route.Model = "auto" },
			want:   "concrete model",
		},
		// pulse-pro has no streaming worker: Smallest answers the live socket
		// with 400. Catching it here converts a billed round trip into a
		// legible local error.
		"pre-recorded-only model": {
			mutate: func(request *runtimepkg.AdapterRequest) { request.Plan.Route.Model = "pulse-pro" },
			want:   "pre-recorded only",
		},
		"stereo audio": {
			mutate: func(request *runtimepkg.AdapterRequest) { request.Media.Channels = 2 },
			want:   "mono audio",
		},
		// 32 kHz is inside the canonical MediaFormat range but is not one of
		// the six rates Pulse's WebSocket documents.
		"undocumented sample rate": {
			mutate: func(request *runtimepkg.AdapterRequest) { request.Media.SampleRateHz = 32_000 },
			want:   "sample rate",
		},
		"http transport": {
			mutate: func(request *runtimepkg.AdapterRequest) { request.Plan.Route.Transport = protocol.TransportHTTP },
			want:   "websocket transport",
		},
		// The endpoint is signed into the plan; pointing it at the wrong path
		// would send audio to a resource with a different protocol.
		"wrong endpoint path": {
			mutate: func(request *runtimepkg.AdapterRequest) {
				request.Plan.Route.Endpoint = strings.Replace(request.Plan.Route.Endpoint, "/waves/v1/stt/live", "/waves/v1/tts/live", 1)
			},
			want: "/waves/v1/stt/live",
		},
	} {
		open := smallestSTTRequest(server.URL)
		testCase.mutate(&open)
		if _, err := adapter.Open(context.Background(), open); err == nil || !strings.Contains(err.Error(), testCase.want) {
			t.Fatalf("%s error = %v, want it to mention %q", name, err, testCase.want)
		}
	}

	// Opus is the other canonical encoding and Pulse genuinely accepts it, so
	// it must NOT be rejected — the encoding gate is a mapping, not a
	// pcm_s16le-only rule.
	opus := smallestSTTRequest(server.URL)
	opus.Media.Encoding = "opus"
	stream, err := adapter.Open(context.Background(), opus)
	if err != nil {
		t.Fatalf("opus open: %v", err)
	}
	abortSmallestStream(t, stream)
}

func TestSTTAdapterClassifiesErrors(t *testing.T) {
	t.Parallel()
	for name, testCase := range map[string]struct {
		frame         map[string]any
		wantCode      string
		wantStatus    int
		wantRetryable bool
	}{
		// 429 is transient: the runtime may retry the attempt.
		"rate limited": {
			frame:         map[string]any{"type": "error", "status_code": 429, "message": "rate limit exceeded"},
			wantCode:      "provider_rate_limited",
			wantStatus:    429,
			wantRetryable: true,
		},
		// A bad key never fixes itself, so retrying just burns the attempt.
		"unauthorized": {
			frame:      map[string]any{"status": "error", "status_code": 401, "message": "invalid api key"},
			wantCode:   "authentication_failed",
			wantStatus: 401,
		},
		// An error carrying only a bare `error` string must still terminate the
		// stream. Swallowing it leaves the caller blocked on a socket that will
		// never produce another transcript.
		"bare error string": {
			frame:         map[string]any{"error": "internal failure"},
			wantCode:      "provider_unavailable",
			wantRetryable: true,
		},
	} {
		frame := testCase.frame
		server := newSmallestSTTServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
			_ = writeSmallestJSON(ctx, conn, frame)
		})
		adapter, err := NewSTT(smallestSTTConfig(server.URL))
		if err != nil {
			t.Fatalf("new STT adapter: %v", err)
		}
		stream, err := adapter.Open(context.Background(), smallestSTTRequest(server.URL))
		if err != nil {
			t.Fatalf("%s open: %v", name, err)
		}
		event := awaitSmallestFailure(t, stream.Events())
		var providerErr *runtimepkg.ProviderError
		if !errors.As(event.Err, &providerErr) {
			t.Fatalf("%s error = %#v, want *runtime.ProviderError", name, event.Err)
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

// A rejected handshake never reaches the read loop, so its classification is a
// separate path from the in-band error frames above.
func TestSTTAdapterClassifiesRejectedHandshake(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "invalid api key", http.StatusUnauthorized)
	}))
	defer server.Close()
	adapter, err := NewSTT(smallestSTTConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	_, err = adapter.Open(context.Background(), smallestSTTRequest(server.URL))
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "authentication_failed" || providerErr.Retryable {
		t.Fatalf("dial error = %#v, want a non-retryable authentication_failed", err)
	}
	if strings.Contains(providerErr.Error(), "customer-smallest-key") {
		t.Fatal("dial error leaked the customer credential")
	}
}

// -- fixtures ---------------------------------------------------------------

func newSmallestSTTServer(t *testing.T, callback func(context.Context, *http.Request, *websocket.Conn)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Assert the path here too: a stand-in that accepts any path cannot
		// catch the adapter dialling the wrong resource.
		if r.URL.Path != "/waves/v1/stt/live" {
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

func smallestSTTRequest(serverURL string) runtimepkg.AdapterRequest {
	now := time.Date(2026, time.August, 7, 12, 0, 0, 0, time.UTC)
	endpoint, _ := url.Parse(serverURL)
	endpoint.Scheme = "ws"
	endpoint.Path = "/waves/v1/stt/live"
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindSTT,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{
				Placement:        protocol.PlacementEmbedded,
				ProviderRoute:    protocol.RouteProviderDirect,
				CredentialSource: protocol.CredentialsBYOK,
			},
			Route: protocol.PlanRoute{
				Provider: "smallest", Model: "pulse", Adapter: STTAdapterID,
				Transport: protocol.TransportWebSocket, Endpoint: endpoint.String(),
				// The engine injects the customer's own key here for a BYOK plan.
				Credential: &protocol.DelegatedCredential{
					Kind: protocol.CredentialBearer, Value: "customer-smallest-key", ExpiresAt: now.Add(time.Hour),
				},
			},
		},
		Options: protocol.RequestOptions{Language: "en"},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
	}
}

func smallestSTTConfig(serverURL string) STTConfig {
	endpoint, _ := url.Parse(serverURL)
	return STTConfig{AllowedEndpointHosts: []string{endpoint.Hostname()}, AllowInsecureEndpoint: true}
}

func assertSmallestText(ctx context.Context, conn *websocket.Conn, want string) error {
	messageType, payload, err := conn.Read(ctx)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}
	if messageType != websocket.MessageText || string(payload) != want {
		return fmt.Errorf("message = (%v, %q), want text %q", messageType, payload, want)
	}
	return nil
}

func writeSmallestJSON(ctx context.Context, conn *websocket.Conn, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, payload)
}

func collectSmallestEvents(t *testing.T, events <-chan runtimepkg.ProviderEvent, count int) []runtimepkg.ProviderEvent {
	t.Helper()
	result := make([]runtimepkg.ProviderEvent, 0, count)
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for len(result) < count {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatalf("events closed after %d of %d events", len(result), count)
			}
			if event.Err != nil {
				t.Fatalf("unexpected provider error after %d events: %v", len(result), event.Err)
			}
			result = append(result, event)
		case <-timer.C:
			t.Fatalf("timed out after %d of %d events", len(result), count)
		}
	}
	return result
}

func awaitSmallestFailure(t *testing.T, events <-chan runtimepkg.ProviderEvent) runtimepkg.ProviderEvent {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatal("events closed before a provider error arrived")
			}
			if event.Err != nil {
				return event
			}
		case <-timer.C:
			t.Fatal("timed out waiting for a provider error")
		}
	}
}

// abortSmallestStream tears a stream down through the optional fast-close
// capability the runtime uses after a terminal failure. Both adapters must
// implement it, so the assertion is part of the contract check.
func abortSmallestStream(t *testing.T, stream runtimepkg.ProviderStream) {
	t.Helper()
	aborter, ok := stream.(runtimepkg.AbortingProviderStream)
	if !ok {
		t.Fatalf("%T does not implement runtime.AbortingProviderStream", stream)
	}
	_ = aborter.Abort(context.Background())
}
