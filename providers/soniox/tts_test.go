package soniox

import (
	"context"
	"encoding/base64"
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

// The start request is asserted as a decoded JSON object rather than through
// ttsStartRequest, so renaming a struct tag cannot keep the test green. Every
// key is transcribed from Soniox's TTS WebSocket API reference, where all of
// stream_id, model, language, voice and audio_format are marked required.
func TestTTSStartRequestIsSentAtOpenWithTheDocumentedWireShape(t *testing.T) {
	t.Parallel()

	starts := make(chan map[string]any, 1)
	server := newTTSTestServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		start, err := readJSONObject(ctx, conn)
		if err != nil {
			t.Errorf("read start request: %v", err)
			return
		}
		starts <- start
		waitForPeer(ctx, conn)
	})
	defer server.Close()

	adapter, err := NewTTS(ttsTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	// Open alone must produce the start message: Soniox closes a connection
	// that has not authenticated within about ten seconds, and a keepalive does
	// not authenticate, so it cannot wait for the caller's first AppendText.
	stream, err := adapter.Open(context.Background(), ttsAdapterRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer abortStream(stream)

	start := mustReceiveObject(t, starts)
	if got := start["api_key"]; got != "customer-soniox-key" {
		t.Errorf("api_key = %v", got)
	}
	if got := start["model"]; got != "tts-rt-v2" {
		t.Errorf("model = %v", got)
	}
	if got := start["voice"]; got != "Adrian" {
		t.Errorf("voice = %v", got)
	}
	// Soniox takes a bare ISO code and rejects a region subtag with HTTP 400
	// "Invalid language '<language>' for model '<model>'."
	if got := start["language"]; got != "es" {
		t.Errorf("language = %v", got)
	}
	if got := start["audio_format"]; got != "pcm_s16le" {
		t.Errorf("audio_format = %v", got)
	}
	if got := start["sample_rate"]; got != float64(24_000) {
		t.Errorf("sample_rate = %v", got)
	}
	if streamID, _ := start["stream_id"].(string); streamID == "" {
		t.Errorf("stream_id = %v, want a client-generated identifier", start["stream_id"])
	}
}

// Soniox is explicit that audio_end only means "no more audio frames" and that
// the stream is complete at terminated. Ending on audio_end would release the
// stream id while the server still owns it.
func TestTTSCompletesOnTerminatedRatherThanAudioEnd(t *testing.T) {
	t.Parallel()

	texts := make(chan map[string]any, 4)
	release := make(chan struct{})
	server := newTTSTestServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		if _, err := readJSONObject(ctx, conn); err != nil {
			t.Errorf("read start request: %v", err)
			return
		}
		first, err := readJSONObject(ctx, conn)
		if err != nil {
			t.Errorf("read text chunk: %v", err)
			return
		}
		texts <- first
		final, err := readJSONObject(ctx, conn)
		if err != nil {
			t.Errorf("read final text chunk: %v", err)
			return
		}
		texts <- final
		streamID := first["stream_id"]
		if err := writeJSONFrame(ctx, conn, map[string]any{
			"stream_id": streamID,
			"audio":     base64.StdEncoding.EncodeToString([]byte{9, 8, 7}),
			"audio_end": false,
			"timestamps": map[string]any{
				"characters":                    []string{"H", "i"},
				"character_start_times_seconds": []float64{0, 0.1},
				"character_end_times_seconds":   []float64{0.1, 0.2},
			},
		}); err != nil {
			t.Errorf("first audio frame: %v", err)
			return
		}
		if err := writeJSONFrame(ctx, conn, map[string]any{
			"stream_id": streamID,
			"audio":     base64.StdEncoding.EncodeToString([]byte{6, 5}),
			"audio_end": true,
		}); err != nil {
			t.Errorf("last audio frame: %v", err)
			return
		}
		<-release
		if err := writeJSONFrame(ctx, conn, map[string]any{"stream_id": streamID, "terminated": true}); err != nil {
			t.Errorf("terminated frame: %v", err)
			return
		}
		waitForPeer(ctx, conn)
	})
	defer server.Close()

	adapter, err := NewTTS(ttsTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), ttsAdapterRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer abortStream(stream)

	if err := stream.AppendText(context.Background(), "Hola, "); err != nil {
		t.Fatalf("append text: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit text: %v", err)
	}

	first := mustReceiveObject(t, texts)
	if first["text"] != "Hola, " || first["text_end"] != false {
		t.Fatalf("first text chunk = %v", first)
	}
	// The end-of-input marker is an empty text chunk carrying text_end, which
	// is exactly what Soniox's own reference client sends after its last chunk.
	final := mustReceiveObject(t, texts)
	if final["text"] != "" || final["text_end"] != true || final["stream_id"] != first["stream_id"] {
		t.Fatalf("final text chunk = %v", final)
	}

	events := collectEvents(t, stream.Events(), 4)
	if got := strings.Join(eventTypeNames(events), ","); got != "audio.started,audio.frame,alignment,audio.frame" {
		t.Fatalf("event types before terminated = %s", got)
	}
	if string(events[1].Audio) != string([]byte{9, 8, 7}) || string(events[3].Audio) != string([]byte{6, 5}) {
		t.Fatalf("audio payloads = %v / %v", events[1].Audio, events[3].Audio)
	}
	if events[1].Extensions["soniox.com/tts/v1"] == nil {
		t.Fatal("audio frames must retain the raw Soniox frame")
	}
	// audio_end has arrived, terminated has not: the stream is not done yet.
	select {
	case event := <-stream.Events():
		t.Fatalf("audio_end must not complete the stream, got %s", event.Type)
	case <-time.After(150 * time.Millisecond):
	}

	close(release)
	done := collectEvents(t, stream.Events(), 1)
	if done[0].Type != protocol.EventAudioDone {
		t.Fatalf("terminal event = %s", done[0].Type)
	}
}

// A finished stream releases its id, and the socket is reusable: the next
// utterance is a fresh start message rather than a new connection.
func TestTTSStartsAFreshStreamForTheNextUtterance(t *testing.T) {
	t.Parallel()

	messages := make(chan map[string]any, 8)
	server := newTTSTestServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		for {
			message, err := readJSONObject(ctx, conn)
			if err != nil {
				return
			}
			messages <- message
			if message["text_end"] == true {
				if err := writeJSONFrame(ctx, conn, map[string]any{"stream_id": message["stream_id"], "terminated": true}); err != nil {
					return
				}
			}
		}
	})
	defer server.Close()

	adapter, err := NewTTS(ttsTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), ttsAdapterRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer abortStream(stream)

	if err := stream.AppendText(context.Background(), "uno"); err != nil {
		t.Fatalf("append first: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit first: %v", err)
	}
	if got := collectEvents(t, stream.Events(), 1); got[0].Type != protocol.EventAudioDone {
		t.Fatalf("first utterance terminal event = %s", got[0].Type)
	}
	if err := stream.AppendText(context.Background(), "dos"); err != nil {
		t.Fatalf("append second: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit second: %v", err)
	}
	if got := collectEvents(t, stream.Events(), 1); got[0].Type != protocol.EventAudioDone {
		t.Fatalf("second utterance terminal event = %s", got[0].Type)
	}

	firstStart := mustReceiveObject(t, messages)
	mustReceiveObject(t, messages) // first text chunk
	mustReceiveObject(t, messages) // first text_end
	secondStart := mustReceiveObject(t, messages)
	// Soniox refuses to reuse a stream id that is still active and refuses more
	// text on one that already saw text_end, so the second utterance needs both
	// a new id and its own start message.
	if secondStart["api_key"] != "customer-soniox-key" || secondStart["voice"] != "Adrian" {
		t.Fatalf("second start message = %v", secondStart)
	}
	if secondStart["stream_id"] == firstStart["stream_id"] {
		t.Fatalf("second utterance reused stream_id %v", firstStart["stream_id"])
	}
}

// Soniox rejects a cancel that also carries text or text_end with HTTP 400
// "The 'cancel' field cannot be combined with 'text' or 'text_end'."
func TestTTSCancelIsSentAloneWithoutTextFields(t *testing.T) {
	t.Parallel()

	messages := make(chan map[string]any, 4)
	server := newTTSTestServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		for {
			message, err := readJSONObject(ctx, conn)
			if err != nil {
				return
			}
			messages <- message
		}
	})
	defer server.Close()

	adapter, err := NewTTS(ttsTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), ttsAdapterRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer abortStream(stream)

	if err := stream.AppendText(context.Background(), "interrumpeme"); err != nil {
		t.Fatalf("append text: %v", err)
	}
	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	start := mustReceiveObject(t, messages)
	mustReceiveObject(t, messages) // text chunk
	cancel := mustReceiveObject(t, messages)
	if cancel["cancel"] != true || cancel["stream_id"] != start["stream_id"] {
		t.Fatalf("cancel message = %v", cancel)
	}
	if _, present := cancel["text"]; present {
		t.Fatalf("cancel carried text: %v", cancel)
	}
	if _, present := cancel["text_end"]; present {
		t.Fatalf("cancel carried text_end: %v", cancel)
	}
}

// The TTS socket carries the same error envelope as the STT socket, plus a
// stream_id. It must classify identically, since the taxonomy is shared.
func TestTTSClassifiesDocumentedErrorTypes(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		errorType string
		errorCode int
		wantCode  string
		retryable bool
	}{
		{errorType: "unauthenticated", errorCode: 401, wantCode: "authentication_failed"},
		// A temporary key scoped to transcribe_websocket is rejected here: the
		// TTS surface needs its own key minted with usage_type "tts_rt".
		{errorType: "temp_api_key_session_expired", errorCode: 403, wantCode: "authentication_failed"},
		{errorType: "organization_monthly_budget_exhausted", errorCode: 402, wantCode: "provider_quota_exceeded"},
		{errorType: "limit_exceeded", errorCode: 429, wantCode: "provider_rate_limited", retryable: true},
		{errorType: "invalid_request", errorCode: 400, wantCode: "invalid_request"},
		{errorType: "invalid_stream_state", errorCode: 400, wantCode: "invalid_request"},
		{errorType: "max_concurrent_streams_reached", errorCode: 400, wantCode: "invalid_request"},
		{errorType: "internal_error", errorCode: 500, wantCode: "provider_unavailable", retryable: true},
		{errorType: "service_unavailable", errorCode: 503, wantCode: "provider_unavailable", retryable: true},
	} {
		t.Run(testCase.errorType, func(t *testing.T) {
			t.Parallel()

			server := newTTSTestServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
				start, err := readJSONObject(ctx, conn)
				if err != nil {
					return
				}
				_ = writeJSONFrame(ctx, conn, map[string]any{
					"stream_id":     start["stream_id"],
					"error_code":    testCase.errorCode,
					"error_type":    testCase.errorType,
					"error_message": "provider text that may change",
					"request_id":    "req_soniox",
				})
				waitForPeer(ctx, conn)
			})
			defer server.Close()

			adapter, err := NewTTS(ttsTestConfig(server.URL))
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			stream, err := adapter.Open(context.Background(), ttsAdapterRequest(server.URL))
			if err != nil {
				t.Fatalf("open stream: %v", err)
			}
			defer abortStream(stream)

			providerError := awaitProviderError(t, stream.Events())
			if providerError.Code != testCase.wantCode {
				t.Errorf("code = %q, want %q", providerError.Code, testCase.wantCode)
			}
			if providerError.Retryable != testCase.retryable {
				t.Errorf("retryable = %v, want %v", providerError.Retryable, testCase.retryable)
			}
			if providerError.ProviderStatus != testCase.errorCode {
				t.Errorf("provider status = %d", providerError.ProviderStatus)
			}
			// The message may quote the provider but must never quote the key.
			if strings.Contains(providerError.Message, "customer-soniox-key") {
				t.Errorf("error message leaked the credential: %q", providerError.Message)
			}
		})
	}
}

func TestTTSRejectsMismatchedRequestsWithoutLeakingTheCredential(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name    string
		mutate  func(*runtimepkg.AdapterRequest)
		wantErr string
	}{
		{
			name:    "wrong kind",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Kind = protocol.SessionKindSTT },
			wantErr: "tts sessions",
		},
		{
			name:    "wrong provider",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Provider = "cartesia" },
			wantErr: "cannot open provider",
		},
		{
			name:    "wrong transport",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Transport = protocol.TransportHTTP },
			wantErr: "websocket transport",
		},
		{
			name:    "unresolved model",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "auto" },
			wantErr: "concrete model",
		},
		{
			// voice is a required start field; a missing one is HTTP 400
			// "Missing voice" once the socket is already open and billing.
			name:    "missing voice",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Options.Voice = "  " },
			wantErr: "voice",
		},
		{
			name:    "missing language",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Options.Language = "" },
			wantErr: "language",
		},
		{
			// "auto" is the planner's placeholder, never a Soniox language code.
			name:    "unresolved language",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Options.Language = "auto" },
			wantErr: "language",
		},
		{
			name:    "missing credential",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential = nil },
			wantErr: "bearer credential",
		},
		{
			// relay_access is a relay-route spelling only; a provider-direct
			// plan carrying it never came from the relay and must be refused.
			name: "relay_access credential outside the relay",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Plan.Route.Credential.Kind = protocol.CredentialRelayAccess
			},
			wantErr: "bearer credential",
		},
		{
			name:    "unsupported encoding",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Media.Encoding = "opus" },
			wantErr: "pcm_s16le",
		},
		{
			// Soniox lists 8000/16000/24000/44100/48000 for pcm_s16le and
			// rejects anything else once the socket is open.
			name:    "unsupported sample rate",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Media.SampleRateHz = 22_050 },
			wantErr: "sample rate",
		},
		{
			name:    "missing media",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Media = nil },
			wantErr: "media configuration",
		},
		{
			name:    "wrong endpoint path",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = "ws://127.0.0.1:1/tts/websocket" },
			wantErr: "endpoint path must be /tts-websocket",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			adapter, err := NewTTS(TTSConfig{AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			request := ttsAdapterRequest("http://127.0.0.1:1")
			if request.Plan.Route.Credential != nil {
				request.Plan.Route.Credential.Value = "secret-that-must-not-leak"
			}
			testCase.mutate(&request)
			_, err = adapter.Open(context.Background(), request)
			if err == nil || !strings.Contains(err.Error(), testCase.wantErr) {
				t.Fatalf("error = %v, want it to mention %q", err, testCase.wantErr)
			}
			if strings.Contains(err.Error(), "secret-that-must-not-leak") {
				t.Fatalf("validation error leaked the credential: %v", err)
			}
		})
	}
}

func TestTTSRefusesTextOutsideTheVendorLimits(t *testing.T) {
	t.Parallel()

	server := newTTSTestServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		if _, err := readJSONObject(ctx, conn); err != nil {
			return
		}
		waitForPeer(ctx, conn)
	})
	defer server.Close()

	adapter, err := NewTTS(ttsTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), ttsAdapterRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer abortStream(stream)

	if err := stream.AppendText(context.Background(), "   "); err == nil {
		t.Error("blank text must be refused rather than billed as an empty chunk")
	}
	// Soniox caps one chunk at 5000 characters and 400s beyond it, which would
	// otherwise kill an in-flight utterance.
	if err := stream.AppendText(context.Background(), strings.Repeat("a", 5_001)); err == nil {
		t.Error("a chunk longer than Soniox's documented 5000-character cap must be refused locally")
	}
	// This surface consumes text, never audio.
	if err := stream.WriteAudio(context.Background(), []byte{1}); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Errorf("write audio = %v", err)
	}
	if err := stream.CommitAudio(context.Background()); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Errorf("commit audio = %v", err)
	}
}

// Same assertion as the STT twin, for the other half of the temporary-key
// scope: TTS also authenticates through api_key in the start message, so the
// managed, BYOK, and relay paths differ only in the secret they carry. The
// relay rows pin that a relay-synthesized plan opens with either credential
// spelling — bearer from the plan-synthesizing connector, relay_access from
// protocol.SessionPlan validation.
func TestTTSEveryRouteUsesTheSameCredentialField(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name       string
		route      protocol.ProviderRoute
		source     protocol.CredentialSource
		kind       protocol.CredentialKind
		credential string
	}{
		{name: "byok", route: protocol.RouteProviderDirect, source: protocol.CredentialsBYOK, kind: protocol.CredentialBearer, credential: "customer-soniox-key"},
		{name: "managed", route: protocol.RouteProviderDirect, source: protocol.CredentialsManaged, kind: protocol.CredentialBearer, credential: "temporary-soniox-key"},
		{name: "relay with bearer kind", route: protocol.RouteSpekoRelay, source: protocol.CredentialsManaged, kind: protocol.CredentialBearer, credential: "connector-soniox-key"},
		{name: "relay with relay_access kind", route: protocol.RouteSpekoRelay, source: protocol.CredentialsManaged, kind: protocol.CredentialRelayAccess, credential: "connector-soniox-key"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			handshakes := make(chan *http.Request, 1)
			starts := make(chan map[string]any, 1)
			server := newTTSTestServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
				handshakes <- request.Clone(request.Context())
				start, err := readJSONObject(ctx, conn)
				if err != nil {
					t.Errorf("read start request: %v", err)
					return
				}
				starts <- start
				waitForPeer(ctx, conn)
			})
			defer server.Close()

			adapter, err := NewTTS(ttsTestConfig(server.URL))
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			request := ttsAdapterRequest(server.URL)
			request.Plan.Execution.ProviderRoute = testCase.route
			request.Plan.Execution.CredentialSource = testCase.source
			request.Plan.Route.Credential.Kind = testCase.kind
			request.Plan.Route.Credential.Value = testCase.credential
			stream, err := adapter.Open(context.Background(), request)
			if err != nil {
				t.Fatalf("open stream: %v", err)
			}
			defer abortStream(stream)

			handshake := mustReceiveRequest(t, handshakes)
			if got := handshake.Header.Get("Authorization"); got != "" {
				t.Errorf("handshake Authorization = %q", got)
			}
			if got := handshake.URL.RawQuery; got != "" {
				t.Errorf("handshake query = %q", got)
			}
			if got := mustReceiveObject(t, starts)["api_key"]; got != testCase.credential {
				t.Errorf("api_key = %v", got)
			}
		})
	}
}

// --- helpers --------------------------------------------------------------

func newTTSTestServer(t *testing.T, callback func(context.Context, *http.Request, *websocket.Conn)) *httptest.Server {
	t.Helper()
	return newWebSocketServer(t, "/tts-websocket", callback)
}

func ttsTestConfig(serverURL string) TTSConfig {
	endpoint, _ := url.Parse(serverURL)
	return TTSConfig{AllowedEndpointHosts: []string{endpoint.Hostname()}, AllowInsecureEndpoint: true}
}

func ttsAdapterRequest(serverURL string) runtimepkg.AdapterRequest {
	now := time.Date(2026, time.August, 7, 12, 0, 0, 0, time.UTC)
	plan := sonioxPlan(now, websocketEndpointFor(serverURL, "/tts-websocket"), "tts-rt-v2")
	plan.Route.Adapter = TTSAdapterID
	plan.Reservation.Usage = protocol.UsageReservation{Unit: protocol.UsageUnitCharacters, AuthorizedUnits: 4_000}
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindTTS,
		Plan: plan,
		// A region subtag exercises the bare-ISO normalization Soniox requires.
		Options: protocol.RequestOptions{Voice: "Adrian", Language: "es-419", MaxInputCharacters: 4_000},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1},
	}
}
