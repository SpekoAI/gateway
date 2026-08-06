package xai

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

// ---------------------------------------------------------------------------
// WebSocket surface — the preferred, genuinely streaming route
// ---------------------------------------------------------------------------

// The streaming route only earns its name if audio reaches the caller before
// the utterance finishes. The fake provider therefore refuses to send its
// second chunk or its terminal event until the test has already observed a
// frame from the first: an adapter that accumulated audio and flushed at
// audio.done would deadlock here rather than quietly pass.
func TestSocketDeliversFramesBeforeAudioDone(t *testing.T) {
	t.Parallel()

	requests := make(chan *http.Request, 1)
	release := make(chan struct{})
	server := newSocketServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		requests <- request.Clone(request.Context())
		// Documented client->server protocol: any number of text.delta frames
		// terminated by exactly one text.done.
		first, err := readSocketMessage(ctx, conn)
		if err != nil || first["type"] != "text.delta" || first["delta"] != "Hello, " {
			t.Errorf("first client message = %v, err=%v", first, err)
			return
		}
		second, err := readSocketMessage(ctx, conn)
		if err != nil || second["type"] != "text.delta" || second["delta"] != "world." {
			t.Errorf("second client message = %v, err=%v", second, err)
			return
		}
		done, err := readSocketMessage(ctx, conn)
		if err != nil || done["type"] != "text.done" {
			t.Errorf("commit message = %v, err=%v", done, err)
			return
		}
		if err := writeSocketJSON(ctx, conn, map[string]any{"type": "audio.delta", "delta": base64.StdEncoding.EncodeToString([]byte{1, 2, 3})}); err != nil {
			t.Errorf("first audio delta: %v", err)
			return
		}
		<-release
		if err := writeSocketJSON(ctx, conn, map[string]any{"type": "audio.delta", "delta": base64.StdEncoding.EncodeToString([]byte{4, 5})}); err != nil {
			t.Errorf("second audio delta: %v", err)
			return
		}
		if err := writeSocketJSON(ctx, conn, map[string]any{"type": "audio.done", "trace_id": "xai_trace_123"}); err != nil {
			t.Errorf("audio done: %v", err)
			return
		}
		_, _, _ = conn.Read(ctx)
	})
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), adapterRequest(server.URL, protocol.TransportWebSocket))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.AppendText(context.Background(), "Hello, "); err != nil {
		t.Fatalf("append first delta: %v", err)
	}
	if err := stream.AppendText(context.Background(), "world."); err != nil {
		t.Fatalf("append second delta: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit text: %v", err)
	}

	// Proves audio is delivered incrementally: these two events exist while the
	// provider is still holding the rest of the utterance.
	early := collectEvents(t, stream.Events(), 2)
	if got := strings.Join(eventTypes(early), ","); got != "audio.started,audio.frame" {
		t.Fatalf("early event types = %s", got)
	}
	if string(early[1].Audio) != string([]byte{1, 2, 3}) {
		t.Fatalf("first frame audio = %v", early[1].Audio)
	}
	close(release)

	rest := collectEvents(t, stream.Events(), 3)
	if got := strings.Join(eventTypes(rest), ","); got != "audio.frame,usage.observed,audio.done" {
		t.Fatalf("remaining event types = %s", got)
	}
	if string(rest[0].Audio) != string([]byte{4, 5}) {
		t.Fatalf("second frame audio = %v", rest[0].Audio)
	}
	// trace_id is xAI's documented correlation id for a streamed utterance.
	var usage struct {
		ProviderRequestID string `json:"provider_request_id"`
	}
	if err := json.Unmarshal(rest[1].Data, &usage); err != nil || usage.ProviderRequestID != "xai_trace_123" {
		t.Fatalf("usage correlation = %+v, err=%v", usage, err)
	}
	if rest[2].Extensions[extensionID] == nil {
		t.Fatal("audio.done must retain the raw xAI payload")
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}

	// The handshake carries every generation choice as a query parameter. This
	// asserts the exact documented spelling: `voice`, not `voice_id`, on this
	// surface, and a region subtag that survives intact.
	select {
	case request := <-requests:
		query := request.URL.Query()
		want := map[string]string{
			"language":                   "pt-BR",
			"voice":                      "eve",
			"codec":                      "pcm",
			"sample_rate":                "16000",
			"optimize_streaming_latency": "1",
		}
		for key, value := range want {
			if got := query.Get(key); got != value {
				t.Errorf("handshake %s = %q, want %q", key, got, value)
			}
		}
		if query.Has("voice_id") {
			t.Error("the websocket surface spells the voice `voice`; `voice_id` belongs to the unary body")
		}
		if query.Has("bit_rate") {
			t.Error("bit_rate is documented as MP3-only and must not ride along with a pcm request")
		}
	case <-time.After(time.Second):
		t.Fatal("server never observed the websocket handshake")
	}
}

// xAI documents exactly one server-side auth channel, and its ephemeral client
// secret is used "in the same fashion as an API key". A managed session that
// silently moved the token to a query parameter (as ElevenLabs requires) would
// 401 at the handshake in production while every other test still passed, so
// both sources are asserted to use the identical Bearer header and to leave the
// URL clean.
func TestSocketCredentialSourcesShareTheBearerHeader(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name   string
		source protocol.CredentialSource
		token  string
	}{
		{name: "byok permanent key", source: protocol.CredentialsBYOK, token: "customer-xai-key"},
		{name: "managed ephemeral client secret", source: protocol.CredentialsManaged, token: "ephemeral-client-secret"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			requests := make(chan *http.Request, 1)
			server := newSocketServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
				requests <- request.Clone(request.Context())
				_, _, _ = conn.Read(ctx)
			})
			defer server.Close()

			adapter, err := New(testConfig(server.URL))
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			request := adapterRequest(server.URL, protocol.TransportWebSocket)
			request.Plan.Execution.CredentialSource = testCase.source
			request.Plan.Route.Credential.Value = testCase.token
			stream, err := adapter.Open(context.Background(), request)
			if err != nil {
				t.Fatalf("open stream: %v", err)
			}
			if err := stream.Close(context.Background()); err != nil {
				t.Fatalf("close stream: %v", err)
			}

			select {
			case received := <-requests:
				if got := received.Header.Get("Authorization"); got != "Bearer "+testCase.token {
					t.Fatalf("Authorization = %q", got)
				}
				if strings.Contains(received.URL.RawQuery, testCase.token) {
					t.Fatal("the access token must never appear in the request URL")
				}
				// The `xai-client-secret.` subprotocol channel exists only
				// because browsers cannot set headers; a gateway must not use it.
				if got := received.Header.Get("Sec-Websocket-Protocol"); strings.Contains(got, "xai-client-secret") {
					t.Fatalf("Sec-Websocket-Protocol = %q", got)
				}
			case <-time.After(time.Second):
				t.Fatal("server never observed the websocket handshake")
			}
		})
	}
}

// Cancel is a real wire message on this surface, and the provider's audio.clear
// acknowledgement — not the local call — is what retires the utterance.
func TestSocketCancelSendsTextClear(t *testing.T) {
	t.Parallel()

	server := newSocketServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		if message, err := readSocketMessage(ctx, conn); err != nil || message["type"] != "text.delta" {
			t.Errorf("delta message = %v, err=%v", message, err)
			return
		}
		message, err := readSocketMessage(ctx, conn)
		if err != nil || message["type"] != "text.clear" {
			t.Errorf("cancel message = %v, err=%v", message, err)
			return
		}
		if err := writeSocketJSON(ctx, conn, map[string]any{"type": "audio.clear"}); err != nil {
			t.Errorf("audio clear: %v", err)
			return
		}
		_, _, _ = conn.Read(ctx)
	})
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), adapterRequest(server.URL, protocol.TransportWebSocket))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.AppendText(context.Background(), "Discard me."); err != nil {
		t.Fatalf("append text: %v", err)
	}
	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	events := collectEvents(t, stream.Events(), 1)
	if events[0].Type != protocol.EventWarning {
		t.Fatalf("event type = %s, want a warning for the cleared buffer", events[0].Type)
	}
	// A retired utterance means the next turn may start; a leaked one would
	// make every later AppendText fail with "previous utterance".
	if err := stream.AppendText(context.Background(), "Next turn."); err != nil {
		t.Fatalf("append after cancel: %v", err)
	}
	abortStream(t, stream)
}

// A handshake rejected before the upgrade is the only place a managed token
// that xAI does not honour would show up, so the status taxonomy has to hold
// on the WebSocket route too, not just the unary one.
func TestSocketHandshakeStatusMapsToProviderErrorCode(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		status    int
		code      string
		retryable bool
	}{
		{status: http.StatusUnauthorized, code: "authentication_failed", retryable: false},
		{status: http.StatusTooManyRequests, code: "provider_rate_limited", retryable: true},
		{status: http.StatusServiceUnavailable, code: "provider_unavailable", retryable: true},
	} {
		t.Run(fmt.Sprintf("status %d", testCase.status), func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(testCase.status)
			}))
			defer server.Close()

			adapter, err := New(testConfig(server.URL))
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			_, err = adapter.Open(context.Background(), adapterRequest(server.URL, protocol.TransportWebSocket))
			assertProviderError(t, err, testCase.code, testCase.retryable, testCase.status)
		})
	}
}

// ---------------------------------------------------------------------------
// Unary surface — POST /v1/tts
// ---------------------------------------------------------------------------

// Pins the exact documented request: field names, the nested output_format
// object, the Bearer header, and the region-qualified language tag. This is the
// check that would have caught a wrong parameter name or an audio-format value
// xAI does not accept, because it asserts the bytes on the wire rather than the
// adapter's own view of them. The provider also withholds its second chunk
// until the first frame has been observed, proving the body is streamed rather
// than buffered.
func TestUnaryCommitPostsDocumentedBodyAndStreamsFrames(t *testing.T) {
	t.Parallel()

	type captured struct {
		header http.Header
		body   map[string]any
	}
	requests := make(chan captured, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != ttsPath {
			http.NotFound(w, r)
			return
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request body: %v", err)
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		requests <- captured{header: r.Header.Clone(), body: body}
		w.Header().Set("Content-Type", "audio/pcm")
		w.Header().Set("X-Request-Id", "xai_req_123")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte{1, 2, 3})
		w.(http.Flusher).Flush()
		<-release
		_, _ = w.Write([]byte{4, 5})
		w.(http.Flusher).Flush()
	}))
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), adapterRequest(server.URL, protocol.TransportHTTP))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.AppendText(context.Background(), "Hello, "); err != nil {
		t.Fatalf("append first fragment: %v", err)
	}
	if err := stream.AppendText(context.Background(), "world."); err != nil {
		t.Fatalf("append second fragment: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit text: %v", err)
	}

	early := collectEvents(t, stream.Events(), 3)
	if got := strings.Join(eventTypes(early), ","); got != "usage.observed,audio.started,audio.frame" {
		t.Fatalf("early event types = %s", got)
	}
	if string(early[2].Audio) != string([]byte{1, 2, 3}) {
		t.Fatalf("first frame audio = %v", early[2].Audio)
	}
	close(release)

	rest := collectEvents(t, stream.Events(), 2)
	if got := strings.Join(eventTypes(rest), ","); got != "audio.frame,audio.done" {
		t.Fatalf("remaining event types = %s", got)
	}
	if string(rest[0].Audio) != string([]byte{4, 5}) {
		t.Fatalf("second frame audio = %v", rest[0].Audio)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}
	if _, ok := <-stream.Events(); ok {
		t.Fatal("events must be closed once the stream closes")
	}

	select {
	case received := <-requests:
		// AppendText buffers locally, so the two fragments must arrive as one
		// `text` value: the unary endpoint has no incremental input.
		want := map[string]any{
			"text":                       "Hello, world.",
			"voice_id":                   "eve",
			"language":                   "pt-BR",
			"output_format":              map[string]any{"codec": "pcm", "sample_rate": float64(16_000)},
			"optimize_streaming_latency": float64(1),
		}
		if !reflect.DeepEqual(received.body, want) {
			t.Fatalf("request body = %#v, want %#v", received.body, want)
		}
		// with_timestamps must stay absent: setting it swaps the raw-audio 200
		// for a base64 JSON envelope and would break the whole audio path.
		if _, present := received.body["with_timestamps"]; present {
			t.Fatal("with_timestamps must never be sent")
		}
		// xAI's TTS body has no model field at all; sending one is a guess.
		if _, present := received.body["model"]; present {
			t.Fatal("xAI TTS accepts no model field")
		}
		if got := received.header.Get("Authorization"); got != "Bearer customer-xai-key" {
			t.Fatalf("Authorization = %q", got)
		}
		if got := received.header.Get("Content-Type"); got != "application/json" {
			t.Fatalf("Content-Type = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("server never observed the synthesis request")
	}
}

// Same rationale as the WebSocket credential test: one documented channel, and
// a regression here fails only in production.
func TestUnaryCredentialSourcesShareTheBearerHeader(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name   string
		source protocol.CredentialSource
		token  string
	}{
		{name: "byok permanent key", source: protocol.CredentialsBYOK, token: "customer-xai-key"},
		{name: "managed ephemeral client secret", source: protocol.CredentialsManaged, token: "ephemeral-client-secret"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			requests := make(chan *http.Request, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests <- r.Clone(r.Context())
				w.Header().Set("Content-Type", "audio/pcm")
				_, _ = w.Write([]byte{9})
			}))
			defer server.Close()

			adapter, err := New(testConfig(server.URL))
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			request := adapterRequest(server.URL, protocol.TransportHTTP)
			request.Plan.Execution.CredentialSource = testCase.source
			request.Plan.Route.Credential.Value = testCase.token
			stream, err := adapter.Open(context.Background(), request)
			if err != nil {
				t.Fatalf("open stream: %v", err)
			}
			if err := stream.AppendText(context.Background(), "Hi."); err != nil {
				t.Fatalf("append text: %v", err)
			}
			if err := stream.CommitText(context.Background()); err != nil {
				t.Fatalf("commit text: %v", err)
			}
			if err := stream.Close(context.Background()); err != nil {
				t.Fatalf("close stream: %v", err)
			}

			select {
			case received := <-requests:
				if got := received.Header.Get("Authorization"); got != "Bearer "+testCase.token {
					t.Fatalf("Authorization = %q", got)
				}
				if received.URL.RawQuery != "" {
					t.Fatalf("unary requests carry no query string, got %q", received.URL.RawQuery)
				}
				if got := received.Header.Get("X-Api-Key"); got != "" {
					t.Fatalf("X-Api-Key = %q; xAI documents only a Bearer header", got)
				}
			case <-time.After(time.Second):
				t.Fatal("server never observed the synthesis request")
			}
		})
	}
}

// The status taxonomy is what the runtime routes on: a retryable classification
// decides whether a fallback is attempted at all.
func TestUnaryStatusMapsToProviderErrorCode(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		status    int
		code      string
		retryable bool
	}{
		{status: http.StatusBadRequest, code: "invalid_request", retryable: false},
		{status: http.StatusUnauthorized, code: "authentication_failed", retryable: false},
		{status: http.StatusForbidden, code: "authentication_failed", retryable: false},
		// xAI returns 404 for an unknown voice_id, which is a caller mistake
		// rather than a missing route.
		{status: http.StatusNotFound, code: "invalid_request", retryable: false},
		{status: http.StatusTooManyRequests, code: "provider_rate_limited", retryable: true},
		{status: http.StatusInternalServerError, code: "provider_unavailable", retryable: true},
		{status: http.StatusServiceUnavailable, code: "provider_unavailable", retryable: true},
	} {
		t.Run(fmt.Sprintf("status %d", testCase.status), func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(testCase.status)
				_, _ = w.Write([]byte(`{"error":"upstream detail"}`))
			}))
			defer server.Close()

			adapter, err := New(testConfig(server.URL))
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			stream, err := adapter.Open(context.Background(), adapterRequest(server.URL, protocol.TransportHTTP))
			if err != nil {
				t.Fatalf("open stream: %v", err)
			}
			if err := stream.AppendText(context.Background(), "Hi."); err != nil {
				t.Fatalf("append text: %v", err)
			}
			err = stream.CommitText(context.Background())
			providerError := assertProviderError(t, err, testCase.code, testCase.retryable, testCase.status)
			// xAI publishes no error-body schema, so the raw payload is kept
			// verbatim under the vendor extension instead of being parsed.
			if providerError.Extensions[extensionID] == nil {
				t.Fatal("the raw provider error payload must be retained")
			}
			// A failed commit must retire the utterance, otherwise the stream is
			// wedged for every later turn.
			if err := stream.AppendText(context.Background(), "Retry."); err != nil {
				t.Fatalf("append after failed commit: %v", err)
			}
			abortStream(t, stream)
		})
	}
}

// Cancelling mid-response must tear down the in-flight request without turning
// a deliberate teardown into a provider failure event.
func TestUnaryCancelAbortsInFlightResponse(t *testing.T) {
	t.Parallel()

	serverGone := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "audio/pcm")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte{1, 2, 3})
		w.(http.Flusher).Flush()
		// Hold the body open until the adapter drops the connection.
		<-r.Context().Done()
		close(serverGone)
	}))
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), adapterRequest(server.URL, protocol.TransportHTTP))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.AppendText(context.Background(), "Cancel me."); err != nil {
		t.Fatalf("append text: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit text: %v", err)
	}
	events := collectEvents(t, stream.Events(), 2)
	if got := strings.Join(eventTypes(events), ","); got != "audio.started,audio.frame" {
		t.Fatalf("event types = %s", got)
	}
	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	select {
	case <-serverGone:
	case <-time.After(2 * time.Second):
		t.Fatal("cancel did not abort the in-flight request")
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}
	// A cancelled utterance emits no terminal audio.done and, crucially, no
	// spurious error: the caller asked for this.
	for event := range stream.Events() {
		if event.Err != nil {
			t.Fatalf("cancellation surfaced as an error: %v", event.Err)
		}
		if event.Type == protocol.EventAudioDone {
			t.Fatal("a cancelled utterance must not report audio.done")
		}
	}
}

// A 200 carrying JSON means xAI answered with the with_timestamps envelope.
// Without this guard the adapter would emit base64 JSON text as if it were
// audio samples — silent garbage rather than a loud failure.
func TestUnaryRejectsJSONEnvelopeResponse(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"audio":"AQID","content_type":"audio/pcm","duration":0.1}`))
	}))
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), adapterRequest(server.URL, protocol.TransportHTTP))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.AppendText(context.Background(), "Hi."); err != nil {
		t.Fatalf("append text: %v", err)
	}
	err = stream.CommitText(context.Background())
	if err == nil || !strings.Contains(err.Error(), "JSON envelope") {
		t.Fatalf("commit error = %v", err)
	}
	abortStream(t, stream)
}

// ---------------------------------------------------------------------------
// Shared contract
// ---------------------------------------------------------------------------

// Every rejection here happens before a socket is dialled or a byte is sent, so
// a malformed plan can never spend an upstream request. The credential case
// doubles as a leak check: an error string is logged, and must not carry a key.
func TestOpenRejectsInvalidRequests(t *testing.T) {
	t.Parallel()

	const endpoint = "https://api.x.ai/v1/tts"
	for _, testCase := range []struct {
		name    string
		mutate  func(*runtimepkg.AdapterRequest)
		wantErr string
	}{
		{
			name:    "wrong session kind",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Kind = protocol.SessionKindSTT },
			wantErr: "tts sessions",
		},
		{
			name:    "another vendor's route",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Provider = "elevenlabs" },
			wantErr: "cannot open provider",
		},
		{
			name:    "transport xAI does not serve",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Transport = protocol.TransportGRPC },
			wantErr: "websocket or http transport",
		},
		{
			name:    "missing credential",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential = nil },
			wantErr: "bearer credential",
		},
		{
			name: "credential of the wrong kind",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Plan.Route.Credential.Kind = protocol.CredentialRelayAccess
			},
			wantErr: "bearer credential",
		},
		{
			// "auto" is a planning placeholder; an adapter that accepted it would
			// be routing on an unresolved decision.
			name:    "unresolved model",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "auto" },
			wantErr: "concrete model",
		},
		{
			// xAI makes language required on both surfaces.
			name:    "missing language",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Options.Language = "" },
			wantErr: "requires a language",
		},
		{
			// xAI's codec enum has no opus member.
			name:    "encoding xAI cannot produce",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Media.Encoding = "opus" },
			wantErr: "pcm_s16le",
		},
		{
			name:    "multichannel output",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Media.Channels = 2 },
			wantErr: "mono audio",
		},
		{
			// 44000 is a plausible typo for the documented 44100.
			name:    "undocumented sample rate",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Media.SampleRateHz = 44_000 },
			wantErr: "sample rate",
		},
		{
			name:    "endpoint pointing somewhere else on the host",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = "https://api.x.ai/v1/chat/completions" },
			wantErr: "endpoint path",
		},
		{
			name:    "endpoint on an unapproved host",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = "https://xai.attacker.example/v1/tts" },
			wantErr: "host is not allowed",
		},
		{
			name:    "plaintext endpoint",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = "http://api.x.ai/v1/tts" },
			wantErr: "must use https",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			// No AllowInsecureEndpoint here: this is the production policy.
			adapter, err := New(Config{})
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			request := adapterRequest(endpoint, protocol.TransportHTTP)
			request.Plan.Route.Endpoint = endpoint
			request.Plan.Route.Credential.Value = "secret-that-must-not-leak"
			testCase.mutate(&request)
			_, err = adapter.Open(context.Background(), request)
			if err == nil || !strings.Contains(err.Error(), testCase.wantErr) {
				t.Fatalf("open error = %v, want it to mention %q", err, testCase.wantErr)
			}
			if strings.Contains(err.Error(), "secret-that-must-not-leak") {
				t.Fatalf("open error leaked the credential: %v", err)
			}
		})
	}
}

// The two surfaces enforce xAI's 15,000-character limit at different scopes:
// per request on the unary body, per text.delta on the socket. Getting the
// scope wrong either rejects legal input or lets a 400 through.
func TestAppendTextEnforcesDocumentedCharacterLimits(t *testing.T) {
	t.Parallel()

	adapter, err := New(Config{})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	request := adapterRequest("https://api.x.ai/v1/tts", protocol.TransportHTTP)
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	half := strings.Repeat("a", maxTextCharacters/2)
	if err := stream.AppendText(context.Background(), half); err != nil {
		t.Fatalf("first half: %v", err)
	}
	if err := stream.AppendText(context.Background(), half); err != nil {
		t.Fatalf("second half: %v", err)
	}
	// Cumulative, because the unary endpoint sends one `text` value.
	err = stream.AppendText(context.Background(), "one character too many")
	assertProviderError(t, err, "input_too_large", false, http.StatusRequestEntityTooLarge)
	abortStream(t, stream)
}

func TestAudioInputIsUnsupportedOnBothSurfaces(t *testing.T) {
	t.Parallel()

	server := newSocketServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		_, _, _ = conn.Read(ctx)
	})
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	socket, err := adapter.Open(context.Background(), adapterRequest(server.URL, protocol.TransportWebSocket))
	if err != nil {
		t.Fatalf("open socket stream: %v", err)
	}
	defer func() { _ = socket.Close(context.Background()) }()
	unary, err := adapter.Open(context.Background(), adapterRequest(server.URL, protocol.TransportHTTP))
	if err != nil {
		t.Fatalf("open unary stream: %v", err)
	}
	defer func() { _ = unary.Close(context.Background()) }()

	// A TTS session has no audio input; the runtime relies on this exact
	// sentinel to tell "wrong operation" apart from "provider failed".
	for name, stream := range map[string]runtimepkg.ProviderStream{"websocket": socket, "unary": unary} {
		if err := stream.WriteAudio(context.Background(), []byte{1}); err != runtimepkg.ErrUnsupportedOperation {
			t.Errorf("%s WriteAudio = %v", name, err)
		}
		if err := stream.CommitAudio(context.Background()); err != runtimepkg.ErrUnsupportedOperation {
			t.Errorf("%s CommitAudio = %v", name, err)
		}
	}
}

// A session that never synthesized anything must still release its channel, or
// the runtime's event pump blocks forever on shutdown.
func TestUnaryCloseWithoutCommitReleasesEvents(t *testing.T) {
	t.Parallel()

	adapter, err := New(Config{})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), adapterRequest("https://api.x.ai/v1/tts", protocol.TransportHTTP))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}
	select {
	case _, ok := <-stream.Events():
		if ok {
			t.Fatal("an untouched stream must not emit events")
		}
	case <-time.After(time.Second):
		t.Fatal("close did not release the event channel")
	}
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

func newSocketServer(t *testing.T, callback func(context.Context, *http.Request, *websocket.Conn)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != ttsPath {
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

func readSocketMessage(ctx context.Context, conn *websocket.Conn) (map[string]string, error) {
	messageType, payload, err := conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if messageType != websocket.MessageText {
		return nil, fmt.Errorf("client message type = %v, want text", messageType)
	}
	var message map[string]string
	if err := json.Unmarshal(payload, &message); err != nil {
		return nil, err
	}
	return message, nil
}

func writeSocketJSON(ctx context.Context, conn *websocket.Conn, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, payload)
}

func testConfig(serverURL string) Config {
	endpoint, _ := url.Parse(serverURL)
	return Config{AllowedEndpointHosts: []string{endpoint.Hostname()}, AllowInsecureEndpoint: true}
}

// adapterRequest builds a plan for whichever surface a test exercises. It
// deliberately uses a region-qualified language tag: pt-BR and pt-PT are
// separate xAI voices, so a truncated tag is a real defect.
func adapterRequest(serverURL string, transport protocol.Transport) runtimepkg.AdapterRequest {
	now := time.Date(2026, time.August, 1, 11, 59, 0, 0, time.UTC)
	endpoint := endpointFor(serverURL, transport)
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindTTS,
		Plan: protocol.SessionPlan{
			PlanID: "plan_xai", SessionID: "sess_xai", AttemptID: "att_1",
			Execution: protocol.Execution{Placement: protocol.PlacementEmbedded, ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK},
			ExpiresAt: now.Add(time.Hour),
			Route: protocol.PlanRoute{
				Provider: "xai", Model: DefaultModel, Adapter: AdapterID, Transport: transport, Endpoint: endpoint,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "customer-xai-key", ExpiresAt: now.Add(30 * time.Minute)},
			},
			Reservation: protocol.Reservation{
				ID: "res_xai", LeaseDurationSeconds: 60, LeaseExpiresAt: now.Add(time.Minute),
				RenewalURL:  "https://control.speko.test/v1/sessions/sess_xai/lease-renewals",
				Concurrency: protocol.ConcurrencyReservation{LeaseID: "conc_xai", Slots: 1},
				Usage:       protocol.UsageReservation{Unit: protocol.UsageUnitCharacters, AuthorizedUnits: 4_000},
			},
			Telemetry:    protocol.Telemetry{Endpoint: "https://control.speko.test/v1/runtime-events", Token: "telemetry-token", FlushIntervalMS: 5_000},
			Requirements: protocol.Requirements{Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision, RuntimeVersion: "0.1.0"},
			Signature:    "test-signature",
		},
		Options: protocol.RequestOptions{Language: "pt-BR", MaxInputCharacters: 4_000},
		Media:   &protocol.MediaFormat{Encoding: pcmEncoding, SampleRateHz: 16_000, Channels: 1},
	}
}

func endpointFor(serverURL string, transport protocol.Transport) string {
	endpoint, err := url.Parse(serverURL)
	if err != nil || endpoint.Host == "" {
		return serverURL
	}
	if transport == protocol.TransportWebSocket {
		endpoint.Scheme = "ws"
	}
	endpoint.Path = ttsPath
	endpoint.RawQuery = ""
	return endpoint.String()
}

// Abort is an optional capability the runtime reaches for only after a terminal
// failure, so it lives on a separate interface. Asserting the type here is part
// of the contract: without it the runtime would have no way to force-close.
func abortStream(t *testing.T, stream runtimepkg.ProviderStream) {
	t.Helper()
	aborter, ok := stream.(runtimepkg.AbortingProviderStream)
	if !ok {
		t.Fatal("an xAI stream must implement runtime.AbortingProviderStream")
	}
	if err := aborter.Abort(context.Background()); err != nil {
		t.Fatalf("abort: %v", err)
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
				t.Fatalf("provider event error: %v", event.Err)
			}
			collected = append(collected, event)
		case <-timer.C:
			t.Fatalf("timed out after %d of %d events", len(collected), want)
		}
	}
	return collected
}

func eventTypes(events []runtimepkg.ProviderEvent) []string {
	types := make([]string, len(events))
	for index, event := range events {
		types[index] = string(event.Type)
	}
	return types
}

func assertProviderError(t *testing.T, err error, code string, retryable bool, status int) *runtimepkg.ProviderError {
	t.Helper()
	var providerError *runtimepkg.ProviderError
	if !errors.As(err, &providerError) {
		t.Fatalf("error = %v, want a *runtimepkg.ProviderError", err)
	}
	if providerError.Code != code {
		t.Errorf("error code = %q, want %q", providerError.Code, code)
	}
	if providerError.Retryable != retryable {
		t.Errorf("retryable = %v, want %v", providerError.Retryable, retryable)
	}
	if providerError.ProviderStatus != status {
		t.Errorf("provider status = %d, want %d", providerError.ProviderStatus, status)
	}
	return providerError
}
