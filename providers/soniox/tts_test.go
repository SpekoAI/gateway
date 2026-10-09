package soniox

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
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

// The start request is asserted as a decoded JSON object rather than through
// ttsStartRequest, so renaming a struct tag cannot keep the test green. Every
// key is transcribed from Soniox's TTS WebSocket API reference, where all of
// stream_id, model, language, voice and audio_format are marked required.
func TestTTSStartRequestIsSentWithTheFirstTextInTheDocumentedWireShape(t *testing.T) {
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
	// The handshake authenticates, so Open sends nothing: Soniox ends a
	// started stream that receives no text within a few seconds with
	// request_timeout, so the start message waits for the first text.
	stream, err := adapter.Open(context.Background(), ttsAdapterRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer abortStream(stream)
	select {
	case start := <-starts:
		t.Fatalf("Open sent a start message before any text: %v", start)
	case <-time.After(50 * time.Millisecond):
	}
	if err := stream.AppendText(context.Background(), "Hola."); err != nil {
		t.Fatalf("append text: %v", err)
	}

	start := mustReceiveObject(t, starts)
	if got, present := start["api_key"]; present {
		t.Errorf("start request carried api_key = %v", got)
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
	// Timestamps are always requested: they ride the same messages as the
	// audio and measured inside the noise, so there is only one start shape.
	if got := start["return_timestamps"]; got != true {
		t.Errorf("return_timestamps = %v, want true", got)
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
	// Soniox measures per CHARACTER and reports start and END times in
	// fractional seconds; the normalized reading is integer milliseconds.
	var timings struct {
		Granularity string                `json:"granularity"`
		Spans       []protocol.TimingSpan `json:"spans"`
	}
	if err := json.Unmarshal(events[2].Data, &timings); err != nil {
		t.Fatalf("decode alignment: %v", err)
	}
	if timings.Granularity != string(protocol.TimingGranularityCharacter) {
		t.Fatalf("granularity = %q", timings.Granularity)
	}
	want := []protocol.TimingSpan{{Text: "H", StartMS: 0, EndMS: 100}, {Text: "i", StartMS: 100, EndMS: 200}}
	if len(timings.Spans) != len(want) || timings.Spans[0] != want[0] || timings.Spans[1] != want[1] {
		t.Fatalf("spans = %+v, want %+v", timings.Spans, want)
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
	if secondStart["voice"] != "Adrian" {
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
		errorType   string
		errorCode   int
		wantCode    string
		retryable   bool
		wantMessage string
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
		// The two-minute per-stream audio cap. The adapter budgets streams
		// to stay under it, so reaching it means the text speaks slower than
		// the budget allows for; the same text would truncate again.
		{errorType: "max_audio_duration_reached", errorCode: 413, wantCode: "invalid_request", wantMessage: "two-minute"},
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
			if err := stream.AppendText(context.Background(), "Hola."); err != nil {
				t.Fatalf("append text: %v", err)
			}

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
			if !strings.Contains(providerError.Message, testCase.wantMessage) {
				t.Errorf("message = %q, want it to mention %q", providerError.Message, testCase.wantMessage)
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

func TestTTSRefusesBlankTextAndAudioInput(t *testing.T) {
	t.Parallel()

	server := newTTSTestServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
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
	// This surface consumes text, never audio.
	if err := stream.WriteAudio(context.Background(), []byte{1}); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Errorf("write audio = %v", err)
	}
	if err := stream.CommitAudio(context.Background()); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Errorf("commit audio = %v", err)
	}
}

// Same assertion as the STT twin, for the other half of the temporary-key
// scope: TTS also authenticates through the handshake's Authorization header,
// so the managed, BYOK, and relay paths differ only in the secret they carry. The
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
			if err := stream.AppendText(context.Background(), "Hola."); err != nil {
				t.Fatalf("append text: %v", err)
			}

			handshake := mustReceiveRequest(t, handshakes)
			if got := handshake.Header.Get("Authorization"); got != "Bearer "+testCase.credential {
				t.Errorf("handshake Authorization = %q", got)
			}
			if got := handshake.URL.RawQuery; got != "" {
				t.Errorf("handshake query = %q", got)
			}
			if got, present := mustReceiveObject(t, starts)["api_key"]; present {
				t.Errorf("start request carried api_key = %v", got)
			}
		})
	}
}

// Synthesis is the bulk of Speko's Soniox spend, and until this field existed
// on the TTS start message not one synthesis request could be attributed to a
// reservation: the start frame simply had no place to put one. Soniox
// documents client_reference_id on the TTS WebSocket configuration message
// with the same semantics as the STT one — "Optional client-defined identifier
// recorded with this request in usage logs" — so the two surfaces stamp the
// same namespaced value and settlement reads them the same way.
func TestTTSStartRequestCarriesTheReservationReference(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name                string
		route               protocol.ProviderRoute
		source              protocol.CredentialSource
		kind                protocol.CredentialKind
		wantClientReference any
	}{
		// A customer-owned key bills the customer's own Soniox project; a
		// Speko reservation id has no meaning there and must not be sent.
		{name: "byok is not tagged", route: protocol.RouteProviderDirect, source: protocol.CredentialsBYOK, kind: protocol.CredentialBearer, wantClientReference: nil},
		{name: "managed", route: protocol.RouteProviderDirect, source: protocol.CredentialsManaged, kind: protocol.CredentialBearer, wantClientReference: "speko_reservation:res_soniox"},
		{name: "relay with managed credential source", route: protocol.RouteSpekoRelay, source: protocol.CredentialsManaged, kind: protocol.CredentialRelayAccess, wantClientReference: "speko_reservation:res_soniox"},
		// The relay connector synthesizes its plan by hand and never runs
		// protocol.SessionPlan.Validate, so the relay arm cannot depend on
		// CredentialSource carrying any particular value.
		{name: "relay with unset credential source", route: protocol.RouteSpekoRelay, source: "", kind: protocol.CredentialBearer, wantClientReference: "speko_reservation:res_soniox"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
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
			request := ttsAdapterRequest(server.URL)
			request.Plan.Execution.ProviderRoute = testCase.route
			request.Plan.Execution.CredentialSource = testCase.source
			request.Plan.Route.Credential.Kind = testCase.kind
			stream, err := adapter.Open(context.Background(), request)
			if err != nil {
				t.Fatalf("open stream: %v", err)
			}
			defer abortStream(stream)
			if err := stream.AppendText(context.Background(), "Hola."); err != nil {
				t.Fatalf("append text: %v", err)
			}

			if got := mustReceiveObject(t, starts)["client_reference_id"]; got != testCase.wantClientReference {
				t.Errorf("client_reference_id = %v, want %v", got, testCase.wantClientReference)
			}
		})
	}
}

// Soniox writes one usage-log entry per STREAM, not per socket, and a TTS
// session runs a fresh stream for every utterance on the same connection. A
// reference sent only on the first start message would leave every later
// utterance of a multi-turn call unattributable — which is most of the spend
// on a voice agent.
func TestTTSEveryStreamOnTheSocketCarriesTheReservationReference(t *testing.T) {
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
	request := ttsAdapterRequest(server.URL)
	request.Plan.Execution.ProviderRoute = protocol.RouteSpekoRelay
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer abortStream(stream)

	for _, text := range []string{"uno", "dos"} {
		if err := stream.AppendText(context.Background(), text); err != nil {
			t.Fatalf("append %q: %v", text, err)
		}
		if err := stream.CommitText(context.Background()); err != nil {
			t.Fatalf("commit %q: %v", text, err)
		}
		if got := collectEvents(t, stream.Events(), 1); got[0].Type != protocol.EventAudioDone {
			t.Fatalf("terminal event for %q = %s", text, got[0].Type)
		}
	}

	firstStart := mustReceiveObject(t, messages)
	mustReceiveObject(t, messages) // first text chunk
	mustReceiveObject(t, messages) // first text_end
	secondStart := mustReceiveObject(t, messages)
	if secondStart["stream_id"] == firstStart["stream_id"] {
		t.Fatalf("second utterance reused stream_id %v", firstStart["stream_id"])
	}
	for name, start := range map[string]map[string]any{"first": firstStart, "second": secondStart} {
		if got := start["client_reference_id"]; got != "speko_reservation:res_soniox" {
			t.Errorf("%s start client_reference_id = %v, want %q", name, got, "speko_reservation:res_soniox")
		}
	}
}

// A long utterance is spread over several Soniox streams, one at a time,
// each well under the two-minute audio cap. The caller sees one utterance:
// one audio.started, every frame in order, alignment measured from the
// utterance's first sample, and one audio.done after the last stream.
func TestTTSRollsALongUtteranceOverSequentialStreams(t *testing.T) {
	t.Parallel()

	const sentence = "The quick brown fox jumps over the lazy dog near the river bank. "
	text := strings.Repeat(sentence, 110) // 7,150 bytes
	fake := newRollingTTSServer(t, true)
	defer fake.close()
	stream := openRollingTTSStream(t, fake)
	defer abortStream(stream)

	if err := stream.AppendText(context.Background(), text); err != nil {
		t.Fatalf("append long text: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit long text: %v", err)
	}

	streams := fake.driveToCompletion(t)
	if len(streams) < 6 {
		t.Fatalf("streams = %d, want the 7,150 bytes spread over at least 6", len(streams))
	}
	var spoken []string
	for index, wire := range streams {
		if wire.start["client_reference_id"] != "speko_reservation:res_soniox" {
			t.Errorf("stream %d start client_reference_id = %v", index, wire.start["client_reference_id"])
		}
		if cost := ttsTextCost(wire.text()); cost > ttsStreamTextBudget {
			t.Errorf("stream %d carried %d cost units, budget %d", index, cost, ttsStreamTextBudget)
		}
		if !wire.ended {
			t.Errorf("stream %d was never sent text_end", index)
		}
		// Sentence-only input must be cut at sentence ends.
		if got := strings.TrimSpace(wire.text()); !strings.HasSuffix(got, ".") {
			t.Errorf("stream %d ends mid-sentence: ...%q", index, got[max(0, len(got)-20):])
		}
		spoken = append(spoken, wire.text())
	}
	if got, want := strings.Join(strings.Fields(strings.Join(spoken, " ")), " "), strings.Join(strings.Fields(text), " "); got != want {
		t.Fatalf("streams did not carry the utterance's text exactly once, in order")
	}
	assertOneUtterance(t, stream.Events(), len(streams))
}

// An LLM streams a word at a time. Past the soft budget the next sentence end
// rolls the stream over, so the hard budget does not land mid-sentence.
func TestTTSRollsTokenStreamsAtSentenceEnds(t *testing.T) {
	t.Parallel()

	fake := newRollingTTSServer(t, false)
	defer fake.close()
	stream := openRollingTTSStream(t, fake)
	defer abortStream(stream)

	var words []string
	for range 90 {
		words = append(words, strings.Fields("Soniox speaks this sentence aloud, one token at a time, for the test.")...)
	}
	for index, word := range words {
		token := word
		if index > 0 {
			token = " " + word
		}
		if err := stream.AppendText(context.Background(), token); err != nil {
			t.Fatalf("append token %d: %v", index, err)
		}
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}

	streams := fake.driveToCompletion(t)
	if len(streams) < 3 {
		t.Fatalf("streams = %d, want the token stream spread over at least 3", len(streams))
	}
	var spoken []string
	for index, wire := range streams {
		if cost := ttsTextCost(wire.text()); cost > ttsStreamTextBudget {
			t.Errorf("stream %d carried %d cost units, budget %d", index, cost, ttsStreamTextBudget)
		}
		if got := strings.TrimSpace(wire.text()); !strings.HasSuffix(got, ".") {
			t.Errorf("stream %d ends mid-sentence: ...%q", index, got[max(0, len(got)-20):])
		}
		spoken = append(spoken, wire.text())
	}
	if got, want := strings.Join(strings.Fields(strings.Join(spoken, " ")), " "), strings.Join(words, " "); got != want {
		t.Fatalf("streams did not carry the token stream exactly once, in order")
	}
	assertOneUtterance(t, stream.Events(), len(streams))
}

// Text with no punctuation and no whitespace still splits, only on rune
// boundaries and never between a base letter and its combining mark.
func TestTTSSplitsUnbrokenTextOnRuneBoundaries(t *testing.T) {
	t.Parallel()

	for name, text := range map[string]string{
		"two-byte runes":  strings.Repeat("ñ", 3_000),
		"combining marks": strings.Repeat("é", 2_000),
		"ideographs":      strings.Repeat("語", 2_000),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fake := newRollingTTSServer(t, false)
			defer fake.close()
			stream := openRollingTTSStream(t, fake)
			defer abortStream(stream)

			if err := stream.AppendText(context.Background(), text); err != nil {
				t.Fatalf("append: %v", err)
			}
			if err := stream.CommitText(context.Background()); err != nil {
				t.Fatalf("commit: %v", err)
			}
			streams := fake.driveToCompletion(t)
			if len(streams) < 2 {
				t.Fatalf("streams = %d, want the text split", len(streams))
			}
			var joined strings.Builder
			for index, wire := range streams {
				chunk := wire.text()
				if !utf8.ValidString(chunk) {
					t.Fatalf("stream %d text is not valid UTF-8", index)
				}
				if first, _ := utf8.DecodeRuneInString(chunk); first == '́' {
					t.Fatalf("stream %d starts with a combining mark split from its letter", index)
				}
				if cost := ttsTextCost(chunk); cost > ttsStreamTextBudget {
					t.Errorf("stream %d carried %d cost units, budget %d", index, cost, ttsStreamTextBudget)
				}
				joined.WriteString(chunk)
			}
			if joined.String() != text {
				t.Fatal("the streams did not carry the text exactly")
			}
			assertOneUtterance(t, stream.Events(), len(streams))
		})
	}
}

// Cancel stops the active stream and drops the utterance's queued streams;
// the next utterance starts cleanly on the same socket.
func TestTTSCancelDropsQueuedStreams(t *testing.T) {
	t.Parallel()

	fake := newRollingTTSServer(t, true)
	defer fake.close()
	stream := openRollingTTSStream(t, fake)
	defer abortStream(stream)

	if err := stream.AppendText(context.Background(), strings.Repeat("One more sentence that keeps the stream busy. ", 150)); err != nil {
		t.Fatalf("append: %v", err)
	}
	first := fake.nextStream(t)
	if !first.ended {
		t.Fatal("the first stream must be full and ended before the rest is queued")
	}
	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	cancel := fake.next(t)
	if cancel["cancel"] != true || cancel["stream_id"] != first.id {
		t.Fatalf("cancel message = %v", cancel)
	}
	done := collectEvents(t, stream.Events(), 1)
	if done[0].Type != protocol.EventAudioDone {
		t.Fatalf("event after cancel = %s", done[0].Type)
	}
	if message, ok := fake.tryNext(150 * time.Millisecond); ok {
		t.Fatalf("queued text reached Soniox after Cancel: %v", message)
	}

	if err := stream.AppendText(context.Background(), "Hola."); err != nil {
		t.Fatalf("append after cancel: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit after cancel: %v", err)
	}
	next := fake.nextStream(t)
	if next.id == first.id || next.text() != "Hola." || !next.ended {
		t.Fatalf("next utterance stream = %+v", next)
	}
	fake.release <- struct{}{}
	events := collectEvents(t, stream.Events(), 4)
	if got := strings.Join(eventTypeNames(events), ","); got != "audio.started,audio.frame,alignment,audio.done" {
		t.Fatalf("next utterance events = %s", got)
	}
}

// Close waits for every queued stream of the utterance, not just the active
// one, so a rolled-over utterance is not cut short.
func TestTTSCloseWaitsForQueuedStreams(t *testing.T) {
	t.Parallel()

	fake := newRollingTTSServer(t, true)
	defer fake.close()
	stream := openRollingTTSStream(t, fake)
	defer abortStream(stream)

	if err := stream.AppendText(context.Background(), strings.Repeat("Close must not cut this utterance short. ", 100)); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	closed := make(chan error, 1)
	go func() { closed <- stream.Close(context.Background()) }()
	go func() {
		for range stream.Events() {
		}
	}()

	streams := 0
	for {
		fake.nextStream(t)
		streams++
		select {
		case err := <-closed:
			t.Fatalf("Close returned %v with stream %d still synthesizing", err, streams)
		default:
		}
		fake.release <- struct{}{}
		if _, more := fake.peek(150 * time.Millisecond); !more {
			break
		}
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after the last stream terminated")
	}
	if streams < 3 {
		t.Fatalf("streams = %d, want the utterance rolled over", streams)
	}
}

func TestTTSSplitPrefersSentenceThenClauseThenSpace(t *testing.T) {
	t.Parallel()

	filler := strings.Repeat("x", ttsStreamTextBudget-100)
	for _, testCase := range []struct {
		name     string
		text     string
		wantHead string
	}{
		{name: "latin sentence", text: filler + " One. Two, three four " + strings.Repeat("y", 200), wantHead: filler + " One."},
		{name: "clause", text: filler + " one, two three " + strings.Repeat("y", 200), wantHead: filler + " one,"},
		{name: "space", text: filler + " one two " + strings.Repeat("y", 200), wantHead: filler + " one two"},
		{name: "decimal is not a sentence end", text: filler + " pi is 3.14 then " + strings.Repeat("y", 200), wantHead: filler + " pi is 3.14 then"},
		{name: "quoted sentence", text: filler + ` he said "stop." Then ` + strings.Repeat("y", 200), wantHead: filler + ` he said "stop."`},
		{name: "arabic question", text: filler + " هل أنت هنا؟ نعم " + strings.Repeat("y", 200), wantHead: filler + " هل أنت هنا؟"},
		{name: "cjk sentence without spaces", text: filler + "你好。再见" + strings.Repeat("y", 200), wantHead: filler + "你好。"},
		{name: "devanagari danda", text: filler + " नमस्ते। फिर " + strings.Repeat("y", 200), wantHead: filler + " नमस्ते।"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			head, tail, roll := ttsSplitForStream(testCase.text, 0, ttsBoundaryNone, false)
			if !roll || head != testCase.wantHead {
				t.Fatalf("head = ...%q roll=%v, want ...%q", head[max(0, len(head)-30):], roll, testCase.wantHead[max(0, len(testCase.wantHead)-30):])
			}
			if strings.TrimSpace(head+" "+tail) != strings.TrimSpace(testCase.text) && head+tail != testCase.text {
				t.Fatalf("split lost text")
			}
		})
	}

	// A stream that already ends a sentence is cut before the next token.
	head, tail, roll := ttsSplitForStream(" Next words", ttsStreamTextBudget-3, ttsBoundarySentence, false)
	if !roll || head != "" || tail != "Next words" {
		t.Fatalf("token at a full stream = (%q, %q, %v)", head, tail, roll)
	}
	// Text that fits and is final is never split early.
	if head, tail, roll := ttsSplitForStream("Short. Text.", 0, ttsBoundaryNone, true); roll || head != "Short. Text." || tail != "" {
		t.Fatalf("fitting final text = (%q, %q, %v)", head, tail, roll)
	}
}

// --- rolling fake server ---------------------------------------------------

// rollingTTSServer is a fake Soniox TTS socket that enforces the adapter's
// one-stream-at-a-time contract: a start while another stream is still
// active is a test failure. For each stream that receives text_end it sends
// one audio frame (with a single-character alignment block timed from the
// stream's own start, as Soniox does), audio_end, then terminated. With hold
// set, terminated waits for a token on release.
type rollingTTSServer struct {
	t        *testing.T
	server   *httptest.Server
	request  runtimepkg.AdapterRequest
	messages chan map[string]any
	release  chan struct{}
	stop     chan struct{}
	hold     bool
	// peeked is a message read ahead of its turn by tryNext.
	peeked map[string]any
}

// rollingTTSFrameBytes is each stream's audio: 100 ms at 24 kHz pcm_s16le.
const rollingTTSFrameBytes = 4_800

type rollingWireStream struct {
	id    string
	start map[string]any
	texts []string
	ended bool
}

func (w rollingWireStream) text() string { return strings.Join(w.texts, "") }

func newRollingTTSServer(t *testing.T, hold bool) *rollingTTSServer {
	t.Helper()
	fake := &rollingTTSServer{
		t:        t,
		messages: make(chan map[string]any, 4_096),
		release:  make(chan struct{}),
		stop:     make(chan struct{}),
		hold:     hold,
	}
	fake.server = newTTSTestServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		var (
			mu       sync.Mutex
			active   string
			canceled = map[string]chan struct{}{}
			index    = 0
		)
		write := func(value any) {
			mu.Lock()
			defer mu.Unlock()
			_ = writeJSONFrame(ctx, conn, value)
		}
		for {
			message, err := readJSONObject(ctx, conn)
			if err != nil {
				return
			}
			fake.messages <- message
			streamID, _ := message["stream_id"].(string)
			switch {
			case message["model"] != nil:
				mu.Lock()
				if active != "" {
					t.Errorf("stream %s started while %s was still active", streamID, active)
				}
				active = streamID
				mu.Unlock()
			case message["cancel"] == true:
				mu.Lock()
				if gone, ok := canceled[streamID]; ok {
					close(gone)
				} else {
					canceled[streamID] = closedChannel()
				}
				if active == streamID {
					active = ""
				}
				mu.Unlock()
				write(map[string]any{"stream_id": streamID, "terminated": true})
			case message["text_end"] == true:
				frameIndex := index
				index++
				gone := make(chan struct{})
				mu.Lock()
				canceled[streamID] = gone
				mu.Unlock()
				go func() {
					if fake.hold {
						select {
						case <-fake.release:
						case <-gone:
							return
						case <-fake.stop:
							return
						}
					}
					audio := make([]byte, rollingTTSFrameBytes)
					audio[0] = byte(frameIndex)
					write(map[string]any{
						"stream_id": streamID,
						"audio":     base64.StdEncoding.EncodeToString(audio),
						"timestamps": map[string]any{
							"characters":                    []string{"a"},
							"character_start_times_seconds": []float64{0},
							"character_end_times_seconds":   []float64{0.05},
						},
					})
					write(map[string]any{"stream_id": streamID, "audio": "", "audio_end": true})
					mu.Lock()
					if active == streamID {
						active = ""
					}
					mu.Unlock()
					write(map[string]any{"stream_id": streamID, "terminated": true})
				}()
			}
		}
	})
	fake.request = ttsAdapterRequest(fake.server.URL)
	fake.request.Plan.Execution.ProviderRoute = protocol.RouteSpekoRelay
	return fake
}

func closedChannel() chan struct{} {
	channel := make(chan struct{})
	close(channel)
	return channel
}

func (f *rollingTTSServer) close() {
	close(f.stop)
	f.server.Close()
}

func openRollingTTSStream(t *testing.T, fake *rollingTTSServer) runtimepkg.ProviderStream {
	t.Helper()
	adapter, err := NewTTS(ttsTestConfig(fake.server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), fake.request)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	return stream
}

// nextStream reads one stream's frames: its start, its text, and its
// text_end. A stream whose text_end is an empty marker after text that did
// not fill the budget is the utterance's final one; the rest are rollovers.
func (f *rollingTTSServer) nextStream(t *testing.T) rollingWireStream {
	t.Helper()
	start := f.next(t)
	if start["model"] == nil {
		t.Fatalf("expected a start message, got %v", start)
	}
	wire := rollingWireStream{id: start["stream_id"].(string), start: start}
	for !wire.ended {
		message := f.next(t)
		if message["stream_id"] != wire.id {
			t.Fatalf("message for %v interleaved into stream %s: %v", message["stream_id"], wire.id, message)
		}
		if message["model"] != nil || message["cancel"] != nil {
			t.Fatalf("unexpected message inside stream %s: %v", wire.id, message)
		}
		text, _ := message["text"].(string)
		if text != "" {
			wire.texts = append(wire.texts, text)
		}
		wire.ended = message["text_end"] == true
	}
	// Nothing for a later stream may arrive before this one terminates.
	if f.hold {
		if message, ok := f.tryNext(50 * time.Millisecond); ok {
			t.Fatalf("Soniox received %v before stream %s terminated", message, wire.id)
		}
	}
	return wire
}

func (f *rollingTTSServer) next(t *testing.T) map[string]any {
	t.Helper()
	if message, ok := f.tryNext(2 * time.Second); ok {
		return message
	}
	t.Fatal("timed out waiting for a provider message")
	return nil
}

// tryNext returns the next client message, or false after wait. A message it
// returns stays readable by the next call when peek is used.
func (f *rollingTTSServer) tryNext(wait time.Duration) (map[string]any, bool) {
	if f.peeked != nil {
		message := f.peeked
		f.peeked = nil
		return message, true
	}
	select {
	case message := <-f.messages:
		return message, true
	case <-time.After(wait):
		return nil, false
	}
}

func (f *rollingTTSServer) peek(wait time.Duration) (map[string]any, bool) {
	message, ok := f.tryNext(wait)
	if ok {
		f.peeked = message
	}
	return message, ok
}

// driveToCompletion reads every stream of one utterance, releasing each one's
// terminated in turn, until no further stream starts.
func (f *rollingTTSServer) driveToCompletion(t *testing.T) []rollingWireStream {
	t.Helper()
	var streams []rollingWireStream
	for {
		wire := f.nextStream(t)
		streams = append(streams, wire)
		if f.hold {
			f.release <- struct{}{}
		}
		message, ok := f.peek(150 * time.Millisecond)
		if !ok {
			return streams
		}
		// A start for the next stream exists; nextStream will read it.
		if message["model"] == nil {
			t.Fatalf("unexpected message after stream %s: %v", wire.id, message)
		}
	}
}

// assertOneUtterance checks the events of one rolled-over utterance: one
// audio.started first, then a frame and an alignment per stream in stream
// order, with alignment moved onto the utterance's timeline, and one
// audio.done last.
func assertOneUtterance(t *testing.T, events <-chan runtimepkg.ProviderEvent, streams int) {
	t.Helper()
	got := collectEvents(t, events, 1+2*streams+1)
	if got[0].Type != protocol.EventAudioStarted {
		t.Fatalf("first event = %s, want audio.started", got[0].Type)
	}
	for index := range streams {
		frame, alignment := got[1+2*index], got[2+2*index]
		if frame.Type != protocol.EventAudioFrame || len(frame.Audio) != rollingTTSFrameBytes || frame.Audio[0] != byte(index) {
			t.Fatalf("event %d = %s (frame %v), want stream %d's audio", 1+2*index, frame.Type, frame.Audio[:1], index)
		}
		if alignment.Type != protocol.EventAlignment {
			t.Fatalf("event %d = %s, want alignment", 2+2*index, alignment.Type)
		}
		var timings struct {
			Spans []protocol.TimingSpan `json:"spans"`
		}
		if err := json.Unmarshal(alignment.Data, &timings); err != nil || len(timings.Spans) != 1 {
			t.Fatalf("alignment %d = %s (%v)", index, alignment.Data, err)
		}
		// Each stream's audio is 100 ms, so stream N starts N*100 ms into
		// the utterance even though Soniox times it from zero.
		if want := (protocol.TimingSpan{Text: "a", StartMS: int64(index) * 100, EndMS: int64(index)*100 + 50}); timings.Spans[0] != want {
			t.Fatalf("stream %d span = %+v, want %+v", index, timings.Spans[0], want)
		}
	}
	if last := got[len(got)-1]; last.Type != protocol.EventAudioDone {
		t.Fatalf("last event = %s, want audio.done", last.Type)
	}
	select {
	case event := <-events:
		if event.Type == protocol.EventAudioDone || event.Type == protocol.EventAudioStarted {
			t.Fatalf("a rolled-over utterance must complete once, got another %s", event.Type)
		}
	case <-time.After(100 * time.Millisecond):
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

// A commit with no text opens no Soniox stream, which would only end in
// request_timeout, and still answers audio.done. The next utterance then runs
// normally on the same socket.
func TestTTSEmptyCommitCompletesWithoutAStream(t *testing.T) {
	t.Parallel()

	fake := newRollingTTSServer(t, false)
	defer fake.close()
	stream := openRollingTTSStream(t, fake)
	defer abortStream(stream)

	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("empty commit: %v", err)
	}
	if got := collectEvents(t, stream.Events(), 1); got[0].Type != protocol.EventAudioDone {
		t.Fatalf("empty commit event = %s, want audio.done", got[0].Type)
	}
	if message, ok := fake.tryNext(50 * time.Millisecond); ok {
		t.Fatalf("empty commit sent %v to Soniox", message)
	}

	if err := stream.AppendText(context.Background(), "Hola."); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	streams := fake.driveToCompletion(t)
	if len(streams) != 1 || streams[0].text() != "Hola." {
		t.Fatalf("streams = %+v, want one carrying the text", streams)
	}
	assertOneUtterance(t, stream.Events(), 1)
}

// An append that fills the stream exactly can end on a letter whose combining
// mark arrives in the next append. The mark stays with its letter instead of
// opening the next stream detached.
func TestTTSKeepsACombiningMarkFromTheNextAppendWithItsLetter(t *testing.T) {
	t.Parallel()

	fake := newRollingTTSServer(t, false)
	defer fake.close()
	stream := openRollingTTSStream(t, fake)
	defer abortStream(stream)

	first := strings.Repeat("e", ttsStreamTextBudget)
	second := "́ y sigue el texto."
	for _, text := range []string{first, second} {
		if err := stream.AppendText(context.Background(), text); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	streams := fake.driveToCompletion(t)
	if len(streams) != 2 {
		t.Fatalf("streams = %d, want 2", len(streams))
	}
	if got := streams[0].text(); !strings.HasSuffix(got, "é") {
		t.Fatalf("first stream ends %q, want the letter with its mark", got[len(got)-4:])
	}
	if got := streams[1].text(); got != "y sigue el texto." {
		t.Fatalf("second stream = %q", got)
	}
	assertOneUtterance(t, stream.Events(), 2)
}

// Appends that arrive while the active stream is closed for input are held
// and carried, in order and exactly once, by the next stream.
func TestTTSAppendsWhileAStreamIsFullReachTheNextStreamInOrder(t *testing.T) {
	t.Parallel()

	fake := newRollingTTSServer(t, true)
	defer fake.close()
	stream := openRollingTTSStream(t, fake)
	defer abortStream(stream)

	full := strings.Repeat("Una frase corta. ", ttsStreamTextBudget/17+1)
	if err := stream.AppendText(context.Background(), full); err != nil {
		t.Fatalf("append: %v", err)
	}
	var want strings.Builder
	want.WriteString(full)
	for index := range 200 {
		chunk := fmt.Sprintf("Parte %d. ", index)
		want.WriteString(chunk)
		if err := stream.AppendText(context.Background(), chunk); err != nil {
			t.Fatalf("append %d: %v", index, err)
		}
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	streams := fake.driveToCompletion(t)
	if len(streams) < 2 {
		t.Fatalf("streams = %d, want the text rolled over", len(streams))
	}
	// Stream boundaries drop the whitespace between sentences, so compare
	// the words.
	var carried []string
	for _, wire := range streams {
		carried = append(carried, strings.Fields(wire.text())...)
	}
	if got, wantText := strings.Join(carried, " "), strings.Join(strings.Fields(want.String()), " "); got != wantText {
		t.Fatalf("streams carried %d bytes, want %d in order", len(got), len(wantText))
	}
	assertOneUtterance(t, stream.Events(), len(streams))
}
