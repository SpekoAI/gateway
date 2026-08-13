package soniox

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

// The start request is asserted as a decoded JSON object rather than through
// sttStartRequest, so that renaming a struct tag cannot keep the test green:
// every key below is transcribed from Soniox's WebSocket API reference.
func TestSTTStartRequestMatchesDocumentedWireShape(t *testing.T) {
	t.Parallel()

	starts := make(chan map[string]any, 1)
	server := newSTTServer(t, func(ctx context.Context, conn *websocket.Conn) {
		start, err := readJSONObject(ctx, conn)
		if err != nil {
			t.Errorf("read start request: %v", err)
			return
		}
		starts <- start
		waitForPeer(ctx, conn)
	})
	defer server.Close()

	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), sttAdapterRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer abortStream(stream)

	start := mustReceiveObject(t, starts)
	// api_key, not a header and not a query parameter: Soniox authenticates the
	// first JSON message on an already-open socket.
	if got := start["api_key"]; got != "customer-soniox-key" {
		t.Errorf("api_key = %v", got)
	}
	if got := start["model"]; got != "stt-rt-v5" {
		t.Errorf("model = %v", got)
	}
	// Raw PCM needs all three of audio_format, sample_rate and num_channels;
	// Soniox 400s with "Audio data channels must be specified for PCM formats"
	// when the pair is missing.
	if got := start["audio_format"]; got != "pcm_s16le" {
		t.Errorf("audio_format = %v", got)
	}
	if got := start["sample_rate"]; got != float64(16_000) {
		t.Errorf("sample_rate = %v", got)
	}
	if got := start["num_channels"]; got != float64(1) {
		t.Errorf("num_channels = %v", got)
	}
	// Endpoint detection is what produces the <end> boundary this adapter turns
	// into a final transcript. Without it a caller that never commits audio
	// would receive word-piece finals and no utterance ever.
	if got := start["enable_endpoint_detection"]; got != true {
		t.Errorf("enable_endpoint_detection = %v", got)
	}
	if got := fmt.Sprint(start["language_hints"]); got != "[en]" {
		t.Errorf("language_hints = %v", got)
	}
	// BYOK plans carry no reservation correlation, and Soniox rejects unknown
	// keys by ignoring them, so the field must simply be absent.
	if _, present := start["client_reference_id"]; present {
		t.Errorf("byok start request carried client_reference_id: %v", start)
	}
}

// Soniox reads the credential out of api_key for a long-lived key and a
// temporary key alike, so a managed route must produce the same message shape
// as a BYOK route. A CredentialSource branch here would be an invented split.
// The relay rows pin the same invariant for the third route: a relay plan
// carries the connector's permanent key in the same field, labelled bearer by
// the plan-synthesizing connector or relay_access by protocol.SessionPlan
// validation, and both spellings must open.
func TestSTTEveryRouteUsesTheSameCredentialField(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name                string
		route               protocol.ProviderRoute
		source              protocol.CredentialSource
		kind                protocol.CredentialKind
		credential          string
		wantClientReference any
	}{
		{name: "byok", route: protocol.RouteProviderDirect, source: protocol.CredentialsBYOK, kind: protocol.CredentialBearer, credential: "customer-soniox-key", wantClientReference: nil},
		{name: "managed", route: protocol.RouteProviderDirect, source: protocol.CredentialsManaged, kind: protocol.CredentialBearer, credential: "temporary-soniox-key", wantClientReference: "res_soniox"},
		{name: "relay with bearer kind", route: protocol.RouteSpekoRelay, source: protocol.CredentialsManaged, kind: protocol.CredentialBearer, credential: "connector-soniox-key", wantClientReference: "res_soniox"},
		{name: "relay with relay_access kind", route: protocol.RouteSpekoRelay, source: protocol.CredentialsManaged, kind: protocol.CredentialRelayAccess, credential: "connector-soniox-key", wantClientReference: "res_soniox"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			handshakes := make(chan *http.Request, 1)
			starts := make(chan map[string]any, 1)
			server := newSTTServerWithRequest(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
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

			adapter, err := NewSTT(sttTestConfig(server.URL))
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			request := sttAdapterRequest(server.URL)
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
			// The secret must never reach the handshake: Soniox has no header
			// or query auth on this endpoint, so anything here would be a leak.
			if got := handshake.Header.Get("Authorization"); got != "" {
				t.Errorf("handshake Authorization = %q", got)
			}
			if got := handshake.URL.RawQuery; got != "" {
				t.Errorf("handshake query = %q", got)
			}
			start := mustReceiveObject(t, starts)
			if got := start["api_key"]; got != testCase.credential {
				t.Errorf("api_key = %v", got)
			}
			if got := start["client_reference_id"]; got != testCase.wantClientReference {
				t.Errorf("client_reference_id = %v, want %v", got, testCase.wantClientReference)
			}
		})
	}
}

// The partial must reach the consumer while audio is still being written, not
// only after the stream is torn down: an interruption-capable caller reacts to
// deltas mid-utterance.
func TestSTTEmitsPartialsDuringTheAudioStreamAndFinalsAtBoundaries(t *testing.T) {
	t.Parallel()

	firstFrame := make(chan struct{})
	server := newSTTServer(t, func(ctx context.Context, conn *websocket.Conn) {
		if _, err := readJSONObject(ctx, conn); err != nil {
			t.Errorf("read start request: %v", err)
			return
		}
		if err := expectBinary(ctx, conn, []byte{1, 2, 3, 4}); err != nil {
			t.Errorf("first audio frame: %v", err)
			return
		}
		// Non-final tokens: Soniox re-sends the whole provisional tail each
		// frame, so the delta is the concatenation of what is on the wire.
		// firstFrame closes BEFORE the write: the client can observe the
		// partial the instant the frame is flushed, so closing afterwards
		// races the test's ordering check against this goroutine's schedule.
		close(firstFrame)
		if err := writeJSONFrame(ctx, conn, map[string]any{
			"tokens": []any{
				map[string]any{"text": "What's ", "is_final": false, "start_ms": 100, "end_ms": 300},
				map[string]any{"text": "the", "is_final": false, "start_ms": 300, "end_ms": 420},
			},
			"total_audio_proc_ms": 420,
		}); err != nil {
			t.Errorf("partial frame: %v", err)
			return
		}
		if err := expectBinary(ctx, conn, []byte{5, 6, 7, 8}); err != nil {
			t.Errorf("second audio frame: %v", err)
			return
		}
		// Finals plus the endpointer's boundary marker in one frame, exactly as
		// documented: the marker always trails the finalized segment.
		if err := writeJSONFrame(ctx, conn, map[string]any{
			"tokens": []any{
				map[string]any{"text": "What's ", "is_final": true, "confidence": 0.9, "start_ms": 100, "end_ms": 300},
				map[string]any{"text": "the time", "is_final": true, "confidence": 0.7, "start_ms": 300, "end_ms": 900},
				map[string]any{"text": "<end>", "is_final": true, "end_ms": 940},
			},
			"final_audio_proc_ms": 900,
		}); err != nil {
			t.Errorf("final frame: %v", err)
			return
		}
		waitForPeer(ctx, conn)
	})
	defer server.Close()

	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), sttAdapterRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer abortStream(stream)

	if err := stream.WriteAudio(context.Background(), []byte{1, 2, 3, 4}); err != nil {
		t.Fatalf("write first audio: %v", err)
	}
	partials := collectEvents(t, stream.Events(), 1)
	if partials[0].Type != protocol.EventTranscriptDelta {
		t.Fatalf("first event type = %s", partials[0].Type)
	}
	assertTranscript(t, partials[0].Data, "What's the", false)
	// The partial arrived before the second frame was written, which is the
	// whole point of the check.
	select {
	case <-firstFrame:
	default:
		t.Fatal("partial was emitted before the server sent it")
	}
	if partials[0].Extensions["soniox.com/stt/v1"] == nil {
		t.Fatal("transcript delta must retain the raw Soniox frame")
	}

	if err := stream.WriteAudio(context.Background(), []byte{5, 6, 7, 8}); err != nil {
		t.Fatalf("write second audio: %v", err)
	}
	events := collectEvents(t, stream.Events(), 2)
	if got := strings.Join(eventTypeNames(events), ","); got != "transcript.final,speech.ended" {
		t.Fatalf("boundary event types = %s", got)
	}
	// Word-piece finals concatenate without added spacing; only the segment as
	// a whole is trimmed.
	assertTranscript(t, events[0].Data, "What's the time", true)
	var final struct {
		Confidence   float64 `json:"confidence"`
		AudioStartMS int64   `json:"audio_start_ms"`
		AudioEndMS   int64   `json:"audio_end_ms"`
	}
	if err := json.Unmarshal(events[0].Data, &final); err != nil {
		t.Fatalf("decode final: %v", err)
	}
	if final.Confidence < 0.79 || final.Confidence > 0.81 {
		t.Errorf("segment confidence = %v, want the mean of 0.9 and 0.7", final.Confidence)
	}
	if final.AudioStartMS != 100 || final.AudioEndMS != 900 {
		t.Errorf("segment span = [%d,%d]", final.AudioStartMS, final.AudioEndMS)
	}
	// <end> is the endpointer's verdict that the speaker stopped, so it is the
	// only marker that also closes the turn.
	var ended struct {
		Reason     string `json:"reason"`
		AudioEndMS int64  `json:"audio_end_ms"`
	}
	if err := json.Unmarshal(events[1].Data, &ended); err != nil {
		t.Fatalf("decode speech ended: %v", err)
	}
	if ended.Reason != "endpoint_detected" || ended.AudioEndMS != 940 {
		t.Errorf("speech ended = %+v", ended)
	}
}

// CommitAudio is the runtime's turn boundary. Soniox answers a finalize with
// <fin>, which finalizes the segment but is not a speaker endpoint, so it must
// not masquerade as one.
func TestSTTCommitAudioSendsFinalizeAndFinReturnsNoSpeechEnded(t *testing.T) {
	t.Parallel()

	controls := make(chan map[string]any, 1)
	server := newSTTServer(t, func(ctx context.Context, conn *websocket.Conn) {
		if _, err := readJSONObject(ctx, conn); err != nil {
			t.Errorf("read start request: %v", err)
			return
		}
		control, err := readJSONObject(ctx, conn)
		if err != nil {
			t.Errorf("read control: %v", err)
			return
		}
		controls <- control
		if err := writeJSONFrame(ctx, conn, map[string]any{
			"tokens": []any{
				map[string]any{"text": "yes", "is_final": true, "confidence": 0.5},
				map[string]any{"text": "<fin>", "is_final": true},
			},
		}); err != nil {
			t.Errorf("fin frame: %v", err)
			return
		}
		if err := writeJSONFrame(ctx, conn, map[string]any{
			"tokens":              []any{},
			"finished":            true,
			"total_audio_proc_ms": 1_680,
		}); err != nil {
			t.Errorf("finished frame: %v", err)
			return
		}
		waitForPeer(ctx, conn)
	})
	defer server.Close()

	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), sttAdapterRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer abortStream(stream)

	if err := stream.CommitAudio(context.Background()); err != nil {
		t.Fatalf("commit audio: %v", err)
	}
	control := mustReceiveObject(t, controls)
	// Transcribed from the manual-finalization reference; a misspelling here is
	// silently ignored by the server and the turn would never commit.
	if got := control["type"]; got != "finalize" {
		t.Fatalf("control message = %v", control)
	}
	events := collectEvents(t, stream.Events(), 2)
	if got := strings.Join(eventTypeNames(events), ","); got != "transcript.final,usage.observed" {
		t.Fatalf("event types = %s (a <fin> must not raise speech.ended)", got)
	}
	assertTranscript(t, events[0].Data, "yes", true)
	// The finished frame carries Soniox's own measure of processed audio, which
	// is the unit an STT reservation is priced in.
	var usage struct {
		AudioProcessedMS int64 `json:"audio_processed_ms"`
	}
	if err := json.Unmarshal(events[1].Data, &usage); err != nil || usage.AudioProcessedMS != 1_680 {
		t.Fatalf("usage = %+v, err=%v", usage, err)
	}
}

// A zero-length frame is Soniox's end-of-stream signal, so forwarding one as
// audio would go deaf mid-call. Close is the only place it may be sent.
func TestSTTRefusesEmptyAudioAndClosesWithFinalizeThenEmptyFrame(t *testing.T) {
	t.Parallel()

	frames := make(chan wireFrame, 4)
	server := newSTTServer(t, func(ctx context.Context, conn *websocket.Conn) {
		if _, err := readJSONObject(ctx, conn); err != nil {
			t.Errorf("read start request: %v", err)
			return
		}
		for range 2 {
			kind, payload, err := conn.Read(ctx)
			if err != nil {
				return
			}
			frames <- wireFrame{kind: kind, payload: payload}
		}
		waitForPeer(ctx, conn)
	})
	defer server.Close()

	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), sttAdapterRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer abortStream(stream)

	if err := stream.WriteAudio(context.Background(), nil); err == nil {
		t.Fatal("an empty audio frame must be refused, not forwarded as end-of-stream")
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}

	// Finalize first because the empty frame alone has been observed not to
	// flush an in-flight segment, then the documented empty end frame.
	first := mustReceiveFrame(t, frames)
	if first.kind != websocket.MessageText || string(first.payload) != `{"type":"finalize"}` {
		t.Fatalf("first close frame = (%v, %q)", first.kind, first.payload)
	}
	second := mustReceiveFrame(t, frames)
	if second.kind != websocket.MessageBinary || len(second.payload) != 0 {
		t.Fatalf("second close frame = (%v, %q)", second.kind, second.payload)
	}
	if err := stream.WriteAudio(context.Background(), []byte{1}); !errors.Is(err, runtimepkg.ErrSessionClosed) {
		t.Fatalf("write after close = %v", err)
	}
}

// Soniox states that error_type is stable across releases while error_message
// is not, so the classifier branches on the type. Each row is a verbatim type
// from the errors reference paired with the status Soniox documents for it.
func TestSTTClassifiesDocumentedErrorTypes(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		errorType string
		errorCode int
		wantCode  string
		retryable bool
	}{
		{errorType: "unauthenticated", errorCode: 401, wantCode: "authentication_failed"},
		{errorType: "temp_api_key_session_expired", errorCode: 403, wantCode: "authentication_failed"},
		// error_type is the discriminator Soniox guarantees, so it has to carry
		// the classification on its own rather than lean on the status code.
		{errorType: "temp_api_key_session_expired", errorCode: 0, wantCode: "authentication_failed"},
		{errorType: "limit_exceeded", errorCode: 0, wantCode: "provider_rate_limited", retryable: true},
		{errorType: "organization_balance_exhausted", errorCode: 402, wantCode: "provider_quota_exceeded"},
		{errorType: "project_monthly_budget_exhausted", errorCode: 402, wantCode: "provider_quota_exceeded"},
		{errorType: "limit_exceeded", errorCode: 429, wantCode: "provider_rate_limited", retryable: true},
		{errorType: "invalid_request", errorCode: 400, wantCode: "invalid_request"},
		{errorType: "model_not_available", errorCode: 400, wantCode: "invalid_request"},
		{errorType: "service_unavailable", errorCode: 503, wantCode: "provider_unavailable", retryable: true},
		{errorType: "internal_error", errorCode: 500, wantCode: "provider_unavailable", retryable: true},
		{errorType: "max_duration_reached", errorCode: 413, wantCode: "provider_unavailable", retryable: true},
		// An error_type Soniox may add later still has to classify sensibly.
		{errorType: "some_future_type", errorCode: 402, wantCode: "provider_quota_exceeded"},
		{errorType: "", errorCode: 429, wantCode: "provider_rate_limited", retryable: true},
	} {
		t.Run(testCase.errorType+"_"+fmt.Sprint(testCase.errorCode), func(t *testing.T) {
			t.Parallel()

			server := newSTTServer(t, func(ctx context.Context, conn *websocket.Conn) {
				if _, err := readJSONObject(ctx, conn); err != nil {
					return
				}
				_ = writeJSONFrame(ctx, conn, map[string]any{
					"tokens":        []any{},
					"error_code":    testCase.errorCode,
					"error_type":    testCase.errorType,
					"error_message": "provider text that may change",
					"request_id":    "req_soniox",
				})
				waitForPeer(ctx, conn)
			})
			defer server.Close()

			adapter, err := NewSTT(sttTestConfig(server.URL))
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			stream, err := adapter.Open(context.Background(), sttAdapterRequest(server.URL))
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
			// A half-finished segment is dropped rather than committed: the
			// route fails over and a truncated user turn would be worse.
			if _, open := <-stream.Events(); open {
				t.Error("no further events may follow a terminal provider error")
			}
		})
	}
}

func TestSTTRejectsMismatchedRequestsWithoutLeakingTheCredential(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name    string
		mutate  func(*runtimepkg.AdapterRequest)
		wantErr string
	}{
		{
			name:    "wrong kind",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Kind = protocol.SessionKindTTS },
			wantErr: "stt sessions",
		},
		{
			name:    "wrong provider",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Provider = "deepgram" },
			wantErr: "cannot open provider",
		},
		{
			name:    "wrong transport",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Transport = protocol.TransportHTTP },
			wantErr: "websocket transport",
		},
		{
			// "auto" is the planner's placeholder; sending it as a model name
			// earns HTTP 400 "The requested model is not available."
			name:    "unresolved model",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "auto" },
			wantErr: "concrete model",
		},
		{
			name:    "empty model",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "  " },
			wantErr: "concrete model",
		},
		{
			name:    "missing credential",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential = nil },
			wantErr: "bearer credential",
		},
		{
			name: "wrong credential kind",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Plan.Route.Credential.Kind = protocol.CredentialSignedURL
			},
			wantErr: "bearer credential",
		},
		{
			// Soniox reads bare Opus only through container auto-detection,
			// which needs an Ogg or WebM header the runtime never produces.
			name:    "unsupported encoding",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Media.Encoding = "opus" },
			wantErr: "media encoding",
		},
		{
			name:    "missing media",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Media = nil },
			wantErr: "media configuration",
		},
		{
			name:    "wrong endpoint path",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = "ws://127.0.0.1:1/v1/listen" },
			wantErr: "endpoint path must be /transcribe-websocket",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			adapter, err := NewSTT(STTConfig{AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			request := sttAdapterRequest("http://127.0.0.1:1")
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

// Soniox's supported-language table spells Norwegian "no" and Tagalog "tl". It
// answers anything outside that table with HTTP 400 "Invalid language hint."
// and closes the socket, so an unaliased platform tag kills the session.
func TestSTTLanguageHintsUseSonioxSpellings(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		language string
		want     string
	}{
		{language: "en", want: "[en]"},
		{language: "en-US", want: "[en]"},
		{language: "nb", want: "[no]"},
		{language: "nb-NO", want: "[no]"},
		{language: "nn", want: "[no]"},
		{language: "fil", want: "[tl]"},
		{language: "fil-PH", want: "[tl]"},
		{language: "tl", want: "[tl]"},
		// Absent hints mean auto-detection across Soniox's whole language set,
		// which is what an unset or "auto" request language asks for.
		{language: "", want: "[]"},
		{language: "auto", want: "[]"},
	} {
		t.Run(testCase.language, func(t *testing.T) {
			t.Parallel()

			starts := make(chan map[string]any, 1)
			server := newSTTServer(t, func(ctx context.Context, conn *websocket.Conn) {
				start, err := readJSONObject(ctx, conn)
				if err != nil {
					t.Errorf("read start request: %v", err)
					return
				}
				starts <- start
				waitForPeer(ctx, conn)
			})
			defer server.Close()

			adapter, err := NewSTT(sttTestConfig(server.URL))
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			request := sttAdapterRequest(server.URL)
			request.Options.Language = testCase.language
			stream, err := adapter.Open(context.Background(), request)
			if err != nil {
				t.Fatalf("open stream: %v", err)
			}
			defer abortStream(stream)

			start := mustReceiveObject(t, starts)
			hints, present := start["language_hints"]
			if !present {
				hints = []any{}
			}
			if got := fmt.Sprint(hints); got != testCase.want {
				t.Fatalf("language_hints = %v, want %v", got, testCase.want)
			}
		})
	}
}

func TestSTTUnsupportedOperationsAreRefused(t *testing.T) {
	t.Parallel()

	server := newSTTServer(t, func(ctx context.Context, conn *websocket.Conn) {
		if _, err := readJSONObject(ctx, conn); err != nil {
			return
		}
		waitForPeer(ctx, conn)
	})
	defer server.Close()

	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), sttAdapterRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer abortStream(stream)

	if err := stream.AppendText(context.Background(), "hello"); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Errorf("append text = %v", err)
	}
	if err := stream.CommitText(context.Background()); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Errorf("commit text = %v", err)
	}
}

// --- shared helpers -------------------------------------------------------

func newSTTServer(t *testing.T, callback func(context.Context, *websocket.Conn)) *httptest.Server {
	t.Helper()
	return newSTTServerWithRequest(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		callback(ctx, conn)
	})
}

func newSTTServerWithRequest(t *testing.T, callback func(context.Context, *http.Request, *websocket.Conn)) *httptest.Server {
	t.Helper()
	return newWebSocketServer(t, "/transcribe-websocket", callback)
}

func newWebSocketServer(t *testing.T, path string, callback func(context.Context, *http.Request, *websocket.Conn)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != path {
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

func sttTestConfig(serverURL string) STTConfig {
	endpoint, _ := url.Parse(serverURL)
	return STTConfig{AllowedEndpointHosts: []string{endpoint.Hostname()}, AllowInsecureEndpoint: true}
}

func sttAdapterRequest(serverURL string) runtimepkg.AdapterRequest {
	now := time.Date(2026, time.August, 7, 12, 0, 0, 0, time.UTC)
	return runtimepkg.AdapterRequest{
		Kind:    protocol.SessionKindSTT,
		Plan:    sonioxPlan(now, websocketEndpointFor(serverURL, "/transcribe-websocket"), "stt-rt-v5"),
		Options: protocol.RequestOptions{Language: "en"},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
	}
}

func sonioxPlan(now time.Time, endpoint, model string) protocol.SessionPlan {
	return protocol.SessionPlan{
		PlanID: "plan_soniox", SessionID: "sess_soniox", AttemptID: "att_1",
		Execution: protocol.Execution{Placement: protocol.PlacementEmbedded, ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK},
		ExpiresAt: now.Add(time.Hour),
		Route: protocol.PlanRoute{
			Provider: "soniox", Model: model, Adapter: STTAdapterID, Transport: protocol.TransportWebSocket, Endpoint: endpoint,
			Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "customer-soniox-key", ExpiresAt: now.Add(30 * time.Minute)},
		},
		Reservation: protocol.Reservation{
			ID: "res_soniox", LeaseDurationSeconds: 60, LeaseExpiresAt: now.Add(time.Minute),
			Concurrency: protocol.ConcurrencyReservation{LeaseID: "conc_soniox", Slots: 1},
			Usage:       protocol.UsageReservation{Unit: protocol.UsageUnitDurationSeconds, AuthorizedUnits: 600},
		},
		Telemetry:    protocol.Telemetry{Endpoint: "https://control.speko.test/v1/runtime-events", Token: "telemetry-token", FlushIntervalMS: 5_000},
		Requirements: protocol.Requirements{Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision, RuntimeVersion: "0.1.0"},
		Signature:    "test-signature",
	}
}

func websocketEndpointFor(serverURL, path string) string {
	endpoint, _ := url.Parse(serverURL)
	endpoint.Scheme = "ws"
	endpoint.Path = path
	return endpoint.String()
}

func readJSONObject(ctx context.Context, conn *websocket.Conn) (map[string]any, error) {
	messageType, payload, err := conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if messageType != websocket.MessageText {
		return nil, fmt.Errorf("expected a text frame, got %v (%q)", messageType, payload)
	}
	object := map[string]any{}
	if err := json.Unmarshal(payload, &object); err != nil {
		return nil, fmt.Errorf("decode %q: %w", payload, err)
	}
	return object, nil
}

func writeJSONFrame(ctx context.Context, conn *websocket.Conn, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, payload)
}

func expectBinary(ctx context.Context, conn *websocket.Conn, want []byte) error {
	messageType, payload, err := conn.Read(ctx)
	if err != nil {
		return err
	}
	if messageType != websocket.MessageBinary || string(payload) != string(want) {
		return fmt.Errorf("frame = (%v, %v), want binary %v", messageType, payload, want)
	}
	return nil
}

func waitForPeer(ctx context.Context, conn *websocket.Conn) {
	for {
		if _, _, err := conn.Read(ctx); err != nil {
			return
		}
	}
}

// abortStream tears a stream down without waiting for a graceful handshake.
// Abort is an optional capability, so the assertion doubles as a check that
// both adapters actually implement it.
func abortStream(stream runtimepkg.ProviderStream) {
	if aborting, ok := stream.(runtimepkg.AbortingProviderStream); ok {
		_ = aborting.Abort(context.Background())
	}
}

// wireFrame preserves the frame type as well as the bytes, because Soniox
// distinguishes the two: an empty binary frame is end-of-stream on the STT
// socket, and the TTS socket refuses binary frames entirely.
type wireFrame struct {
	kind    websocket.MessageType
	payload []byte
}

func mustReceiveFrame(t *testing.T, frames <-chan wireFrame) wireFrame {
	t.Helper()
	select {
	case frame := <-frames:
		return frame
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a client frame")
		return wireFrame{}
	}
}

func mustReceiveObject(t *testing.T, objects <-chan map[string]any) map[string]any {
	t.Helper()
	select {
	case object := <-objects:
		return object
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a provider message")
		return nil
	}
}

func mustReceiveRequest(t *testing.T, requests <-chan *http.Request) *http.Request {
	t.Helper()
	select {
	case request := <-requests:
		return request
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the websocket handshake")
		return nil
	}
}

func collectEvents(t *testing.T, events <-chan runtimepkg.ProviderEvent, want int) []runtimepkg.ProviderEvent {
	t.Helper()
	collected := make([]runtimepkg.ProviderEvent, 0, want)
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for len(collected) < want {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatalf("provider events closed after %d of %d events", len(collected), want)
			}
			if event.Err != nil {
				t.Fatalf("unexpected provider error: %v", event.Err)
			}
			collected = append(collected, event)
		case <-timer.C:
			t.Fatalf("timed out after %d of %d provider events", len(collected), want)
		}
	}
	return collected
}

func awaitProviderError(t *testing.T, events <-chan runtimepkg.ProviderEvent) *runtimepkg.ProviderError {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatal("provider events closed without a terminal error")
			}
			if event.Err == nil {
				continue
			}
			var providerError *runtimepkg.ProviderError
			if !errors.As(event.Err, &providerError) {
				t.Fatalf("error is not a ProviderError: %v", event.Err)
			}
			return providerError
		case <-timer.C:
			t.Fatal("timed out waiting for a terminal provider error")
		}
	}
}

func eventTypeNames(events []runtimepkg.ProviderEvent) []string {
	names := make([]string, len(events))
	for index, event := range events {
		names[index] = string(event.Type)
	}
	return names
}

func assertTranscript(t *testing.T, data json.RawMessage, wantText string, wantFinal bool) {
	t.Helper()
	var transcript struct {
		Text    string `json:"text"`
		IsFinal bool   `json:"is_final"`
	}
	if err := json.Unmarshal(data, &transcript); err != nil {
		t.Fatalf("decode transcript: %v", err)
	}
	if transcript.Text != wantText || transcript.IsFinal != wantFinal {
		t.Fatalf("transcript = %+v, want %q final=%v", transcript, wantText, wantFinal)
	}
}
