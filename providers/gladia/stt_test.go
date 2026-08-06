package gladia

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

const testSessionID = "0f1c2d3e-4a5b-6c7d-8e9f-a0b1c2d3e4f5"

// TestAdapterInitialisesSessionAndStreamsPartialsBeforeCommit is the whole
// two-step contract in one test: the init POST body, the fact that the URL
// Gladia returned is the URL actually dialled, and that partial transcripts
// reach the caller while audio is still flowing.
func TestAdapterInitialisesSessionAndStreamsPartialsBeforeCommit(t *testing.T) {
	t.Parallel()

	inits := make(chan capturedInit, 1)
	handshakes := make(chan *http.Request, 1)
	stopped := make(chan struct{})

	socket := newSocketServer(t, handshakes, func(ctx context.Context, conn *websocket.Conn) {
		messageType, payload, err := conn.Read(ctx)
		if err != nil || messageType != websocket.MessageBinary || string(payload) != "\x01\x02\x03" {
			t.Errorf("audio message = (%v, %q, %v)", messageType, payload, err)
			return
		}
		// Everything below is written before stop_recording is read, so a delta
		// the client observes now can only have arrived mid-stream.
		if err := writeJSON(ctx, conn, frame("speech_start", map[string]any{"time": 0.25, "channel": 0})); err != nil {
			t.Errorf("write speech_start: %v", err)
			return
		}
		if err := writeJSON(ctx, conn, frame("transcript", map[string]any{
			"id": "utt_1", "is_final": false,
			"utterance": map[string]any{"text": " hello", "start": 0.25, "end": 0.75, "language": "fr", "confidence": 0.8, "channel": 0},
		})); err != nil {
			t.Errorf("write partial transcript: %v", err)
			return
		}
		close(stopped)

		if err := assertControl(ctx, conn, "stop_recording"); err != nil {
			t.Errorf("stop_recording: %v", err)
			return
		}
		if err := writeJSON(ctx, conn, frame("transcript", map[string]any{
			"id": "utt_1", "is_final": true,
			"utterance": map[string]any{"text": " hello world", "start": 0.25, "end": 1.0, "language": "fr", "confidence": 0.94, "channel": 0},
		})); err != nil {
			t.Errorf("write final transcript: %v", err)
			return
		}
		if err := writeJSON(ctx, conn, frame("speech_end", map[string]any{"time": 1.0, "channel": 0})); err != nil {
			t.Errorf("write speech_end: %v", err)
			return
		}
		if err := writeJSON(ctx, conn, frame("post_final_transcript", map[string]any{
			"metadata": map[string]any{"audio_duration": 1.25, "billing_time": 1.5},
		})); err != nil {
			t.Errorf("write post_final_transcript: %v", err)
			return
		}
		if err := conn.Close(websocket.StatusNormalClosure, ""); err != nil {
			t.Errorf("close server socket: %v", err)
		}
	})
	defer socket.Close()
	initServer := newInitServer(t, inits, sessionURLFor(socket, "tok-from-init"), http.StatusCreated)
	defer initServer.Close()

	adapter, err := New(testConfig())
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	// "fr-FR" on purpose: Gladia's language enum has no regional variants, so
	// the adapter has to narrow the tag before it reaches the wire.
	stream, err := adapter.Open(context.Background(), byokRequest(initServer, protocol.RequestOptions{Language: "fr-FR"}))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.WriteAudio(context.Background(), []byte{1, 2, 3}); err != nil {
		t.Fatalf("write audio: %v", err)
	}

	// Drain the pre-commit events first. If the adapter only surfaced results
	// after stop_recording, this collect would time out instead.
	before := collectProviderEvents(t, stream.Events(), 3)
	if got := eventTypes(before); strings.Join(got, ",") != strings.Join([]string{
		string(protocol.EventUsageObserved),
		string(protocol.EventSpeechStarted),
		string(protocol.EventTranscriptDelta),
	}, ",") {
		t.Fatalf("pre-commit event types = %v", got)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("server never reached the pre-commit write barrier")
	}

	// The init response's `id` is the provider correlation id, and it must be
	// reported without waiting for a frame from the socket.
	var usage struct {
		ProviderRequestID string `json:"provider_request_id"`
	}
	if err := json.Unmarshal(before[0].Data, &usage); err != nil || usage.ProviderRequestID != testSessionID {
		t.Fatalf("init usage correlation = %+v, err=%v", usage, err)
	}
	var partial transcriptPayload
	if err := json.Unmarshal(before[2].Data, &partial); err != nil {
		t.Fatalf("decode partial: %v", err)
	}
	// The leading space is Gladia's; the adapter must not silently reshape text.
	if partial.Text != " hello" || partial.IsFinal || partial.AudioEndMS != 750 {
		t.Fatalf("partial transcript = %+v", partial)
	}

	if err := stream.CommitAudio(context.Background()); err != nil {
		t.Fatalf("commit audio: %v", err)
	}
	after := collectProviderEvents(t, stream.Events(), 3)
	if got := eventTypes(after); strings.Join(got, ",") != strings.Join([]string{
		string(protocol.EventTranscriptFinal),
		string(protocol.EventSpeechEnded),
		string(protocol.EventUsageObserved),
	}, ",") {
		t.Fatalf("post-commit event types = %v", got)
	}
	var final transcriptPayload
	if err := json.Unmarshal(after[0].Data, &final); err != nil {
		t.Fatalf("decode final: %v", err)
	}
	if final.Text != " hello world" || !final.IsFinal || final.AudioStartMS != 250 || final.AudioEndMS != 1_000 ||
		final.Language != "fr" || final.UtteranceID != "utt_1" || final.ProviderRequestID != testSessionID {
		t.Fatalf("final transcript = %+v", final)
	}
	if after[0].Extensions[extensionID] == nil {
		t.Fatal("final transcript must retain its raw Gladia frame")
	}
	// post_final_transcript is the only place Gladia states what it will bill.
	var billing struct {
		AudioDurationMS  int64 `json:"audio_duration_ms"`
		BilledDurationMS int64 `json:"billed_duration_ms"`
	}
	if err := json.Unmarshal(after[2].Data, &billing); err != nil {
		t.Fatalf("decode billing: %v", err)
	}
	if billing.AudioDurationMS != 1_250 || billing.BilledDurationMS != 1_500 {
		t.Fatalf("billing usage = %+v", billing)
	}

	// Close must be idempotent with CommitAudio: both send the one terminal
	// frame Gladia has, and the server already read exactly one of them.
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}
	if _, ok := <-stream.Events(); ok {
		t.Fatal("events must close after the server closes the websocket")
	}

	select {
	case captured := <-inits:
		if got := captured.header.Get(apiKeyHeader); got != "customer-account-key" {
			t.Fatalf("%s = %q", apiKeyHeader, got)
		}
		if got := captured.header.Get("Content-Type"); got != "application/json" {
			t.Fatalf("content-type = %q", got)
		}
		// The media format and model must reach Gladia at init time; the
		// socket has no place to renegotiate them afterwards.
		if captured.body.Encoding != "wav/pcm" || captured.body.BitDepth != 16 ||
			captured.body.SampleRate != 16_000 || captured.body.Channels != 1 || captured.body.Model != DefaultModel {
			t.Fatalf("init audio/model body = %+v", captured.body)
		}
		// "fr-FR" in, bare "fr" out: anything else is a 422 from Gladia.
		if captured.body.LanguageConfig == nil || len(captured.body.LanguageConfig.Languages) != 1 ||
			captured.body.LanguageConfig.Languages[0] != "fr" {
			t.Fatalf("init language_config = %+v", captured.body.LanguageConfig)
		}
		// Gladia defaults this to false. Without it there are no deltas at all,
		// so this assertion guards the whole streaming contract above.
		if !captured.body.MessagesConfig.ReceivePartialTranscripts {
			t.Fatal("init must opt in to partial transcripts")
		}
		if !captured.body.MessagesConfig.ReceiveSpeechEvents || !captured.body.MessagesConfig.ReceiveFinalTranscripts {
			t.Fatalf("init messages_config = %+v", captured.body.MessagesConfig)
		}
		if captured.body.MessagesConfig.ReceiveAcknowledgments || captured.body.MessagesConfig.ReceiveRealtimeProcessingEvents {
			t.Fatalf("init subscribed to unused message classes = %+v", captured.body.MessagesConfig)
		}
	case <-time.After(time.Second):
		t.Fatal("init endpoint was never called")
	}

	select {
	case handshake := <-handshakes:
		// The dial must use the token from the init response, not a value the
		// adapter derived on its own.
		if got := handshake.URL.Query().Get("token"); got != "tok-from-init" {
			t.Fatalf("dialled token = %q", got)
		}
		// The account key belongs to the init POST only.
		if got := handshake.Header.Get(apiKeyHeader); got != "" {
			t.Fatalf("websocket handshake leaked the account key: %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not observe the websocket handshake")
	}
}

// TestAdapterDialsTheHostGladiaReturnedNotThePlanHost proves the session URL is
// followed rather than reconstructed: the init POST and the websocket live on
// two different listeners, and only the response says where the socket is.
func TestAdapterDialsTheHostGladiaReturnedNotThePlanHost(t *testing.T) {
	t.Parallel()

	handshakes := make(chan *http.Request, 1)
	socket := newSocketServer(t, handshakes, func(ctx context.Context, conn *websocket.Conn) {
		if err := assertControl(ctx, conn, "stop_recording"); err != nil {
			t.Errorf("stop_recording: %v", err)
			return
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	defer socket.Close()
	initServer := newInitServer(t, nil, sessionURLFor(socket, "tok-elsewhere"), http.StatusCreated)
	defer initServer.Close()
	if hostPort(initServer) == hostPort(socket) {
		t.Fatal("test needs two distinct listeners to be meaningful")
	}

	adapter, err := New(testConfig())
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), byokRequest(initServer, protocol.RequestOptions{}))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}

	select {
	case handshake := <-handshakes:
		if handshake.Host != hostPort(socket) {
			t.Fatalf("dialled host = %q, want the host from the init response %q", handshake.Host, hostPort(socket))
		}
	case <-time.After(time.Second):
		t.Fatal("websocket server on the returned host was never dialled")
	}
}

// TestManagedRouteDialsPreMintedSessionURLWithoutInit covers the managed
// credential model: the control plane already spent the account key, so the
// adapter must connect straight through and never touch the init endpoint.
func TestManagedRouteDialsPreMintedSessionURLWithoutInit(t *testing.T) {
	t.Parallel()

	handshakes := make(chan *http.Request, 1)
	socket := newSocketServer(t, handshakes, func(ctx context.Context, conn *websocket.Conn) {
		if err := assertControl(ctx, conn, "stop_recording"); err != nil {
			t.Errorf("stop_recording: %v", err)
			return
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	defer socket.Close()

	var initCalls atomic.Int64
	initServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		initCalls.Add(1)
		http.Error(w, "managed routes must not initialise a session", http.StatusTeapot)
	}))
	defer initServer.Close()

	adapter, err := New(testConfig())
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	request := byokRequest(initServer, protocol.RequestOptions{})
	request.Plan.Execution.CredentialSource = protocol.CredentialsManaged
	request.Plan.Route.Credential = &protocol.DelegatedCredential{
		Kind:      protocol.CredentialSessionURL,
		Value:     sessionURLFor(socket, "tok-minted-by-control-plane"),
		ExpiresAt: time.Now().Add(time.Minute),
	}
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open managed stream: %v", err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close managed stream: %v", err)
	}

	if calls := initCalls.Load(); calls != 0 {
		t.Fatalf("managed route performed %d init calls, want 0", calls)
	}
	select {
	case handshake := <-handshakes:
		if got := handshake.URL.Query().Get("token"); got != "tok-minted-by-control-plane" {
			t.Fatalf("dialled token = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("managed session URL was never dialled")
	}
}

// TestInitFailuresMapToDistinctErrorCodes keeps the vendor error classes apart:
// a wrong key, an exhausted balance, a throttle, and a malformed request all
// need different operator responses, so they must not collapse into one code.
func TestInitFailuresMapToDistinctErrorCodes(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		status    int
		code      string
		retryable bool
	}{
		{status: http.StatusUnauthorized, code: "authentication_failed"},
		{status: http.StatusForbidden, code: "authentication_failed"},
		{status: http.StatusPaymentRequired, code: "provider_quota_exceeded"},
		{status: http.StatusTooManyRequests, code: "provider_rate_limited", retryable: true},
		{status: http.StatusBadRequest, code: "invalid_request"},
		{status: http.StatusUnprocessableEntity, code: "invalid_request"},
		{status: http.StatusInternalServerError, code: "provider_unavailable", retryable: true},
	} {
		t.Run(fmt.Sprintf("status_%d", testCase.status), func(t *testing.T) {
			t.Parallel()
			initServer := newInitServer(t, nil, "", testCase.status)
			defer initServer.Close()

			adapter, err := New(testConfig())
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			_, err = adapter.Open(context.Background(), byokRequest(initServer, protocol.RequestOptions{}))
			providerError := providerErrorFrom(t, err)
			if providerError.Code != testCase.code || providerError.Retryable != testCase.retryable ||
				providerError.ProviderStatus != testCase.status {
				t.Fatalf("init error = %+v", providerError)
			}
			// A failed init must not echo the account key back to the caller.
			if strings.Contains(providerError.Error(), "customer-account-key") {
				t.Fatal("init error leaked the account key")
			}
		})
	}
}

// TestStreamErrorObjectMapsVendorStatusToCode covers the in-band failure path.
// Gladia has no standalone error frame; failures ride as an `error` object on a
// post-processing or acknowledgement message, and its status_code is typed
// oneOf so it arrives as either a number or a string. The carrier below is
// post_final_transcript because the spec gives transcript no error field.
func TestStreamErrorObjectMapsVendorStatusToCode(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name       string
		statusCode any
		code       string
		retryable  bool
	}{
		{name: "numeric_rate_limit", statusCode: 429, code: "provider_rate_limited", retryable: true},
		{name: "string_unauthorized", statusCode: "401", code: "authentication_failed"},
		{name: "numeric_quota", statusCode: 402, code: "provider_quota_exceeded"},
		{name: "numeric_invalid", statusCode: 422, code: "invalid_request"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			socket := newSocketServer(t, nil, func(ctx context.Context, conn *websocket.Conn) {
				_ = writeJSON(ctx, conn, map[string]any{
					"type": "post_final_transcript", "session_id": testSessionID,
					"created_at": "2026-08-07T00:00:00.000Z",
					"error":      map[string]any{"status_code": testCase.statusCode, "exception": "Failure", "message": "upstream said no"},
				})
				<-ctx.Done()
			})
			defer socket.Close()
			initServer := newInitServer(t, nil, sessionURLFor(socket, "tok"), http.StatusCreated)
			defer initServer.Close()

			adapter, err := New(testConfig())
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			stream, err := adapter.Open(context.Background(), byokRequest(initServer, protocol.RequestOptions{}))
			if err != nil {
				t.Fatalf("open stream: %v", err)
			}
			defer stream.Cancel(context.Background())

			providerError := providerErrorFrom(t, awaitStreamError(t, stream.Events()))
			if providerError.Code != testCase.code || providerError.Retryable != testCase.retryable {
				t.Fatalf("stream error = %+v", providerError)
			}
			if !strings.Contains(providerError.Message, "upstream said no") {
				t.Fatalf("stream error dropped the vendor detail: %q", providerError.Message)
			}
		})
	}
}

// TestAdapterRejectsUnusableRequests fails every request that cannot possibly
// work before the customer's account key is put on the wire.
func TestAdapterRejectsUnusableRequests(t *testing.T) {
	t.Parallel()

	// A listener that would panic the test if it were ever reached; every case
	// below must be rejected locally.
	initServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("adapter contacted Gladia for a request it should have rejected: %s", r.URL)
		http.Error(w, "unreachable", http.StatusTeapot)
	}))
	defer initServer.Close()

	for _, testCase := range []struct {
		name    string
		want    string
		mutate  func(*runtimepkg.AdapterRequest)
		forbidI bool
	}{
		{name: "wrong_kind", want: "stt sessions", mutate: func(r *runtimepkg.AdapterRequest) { r.Kind = protocol.SessionKindTTS }},
		{name: "wrong_provider", want: "cannot open provider", mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Provider = "deepgram" }},
		{name: "wrong_transport", want: "websocket transport", mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Transport = protocol.TransportHTTP }},
		{name: "missing_media", want: "media configuration", mutate: func(r *runtimepkg.AdapterRequest) { r.Media = nil }},
		// Gladia's live encoding enum is wav/pcm, wav/alaw, wav/ulaw only.
		{name: "opus_media", want: "media encoding", mutate: func(r *runtimepkg.AdapterRequest) { r.Media.Encoding = "opus" }},
		// 22050 Hz is outside Gladia's documented sample_rate enum.
		{name: "unsupported_sample_rate", want: "sample rate", mutate: func(r *runtimepkg.AdapterRequest) { r.Media.SampleRateHz = 22_050 }},
		// `auto` is a control-plane placeholder; the wire needs a real model.
		{name: "auto_model", want: "concrete model", mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "auto" }},
		{name: "empty_model", want: "concrete model", mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "" }},
		{name: "missing_credential", want: "session credential", mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential = nil }},
		{name: "blank_credential", want: "session credential", mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential.Value = "   " }},
		{
			name: "byok_with_session_url", want: "byok requires a bearer credential",
			mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential.Kind = protocol.CredentialSessionURL },
		},
		{
			// A managed plan holding a raw key would mean the account secret
			// reached the runtime, which is the exact thing this model avoids.
			name: "managed_with_bearer", want: "session_url credential",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Plan.Execution.CredentialSource = protocol.CredentialsManaged
			},
		},
		{
			name: "managed_session_url_on_foreign_host", want: "host is not allowed",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Plan.Execution.CredentialSource = protocol.CredentialsManaged
				r.Plan.Route.Credential = &protocol.DelegatedCredential{
					Kind: protocol.CredentialSessionURL, Value: "wss://attacker.example/v2/live?token=t",
				}
			},
		},
		{
			// A session URL with no query carries no token, so it would fail at
			// the vendor with an opaque 401 instead of here.
			name: "managed_session_url_without_token", want: "must carry its session token",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Plan.Execution.CredentialSource = protocol.CredentialsManaged
				r.Plan.Route.Credential = &protocol.DelegatedCredential{
					Kind: protocol.CredentialSessionURL, Value: "wss://api.gladia.io/v2/live",
				}
			},
		},
		{
			name: "managed_session_url_on_wrong_path", want: "path must be /v2/live",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Plan.Execution.CredentialSource = protocol.CredentialsManaged
				r.Plan.Route.Credential = &protocol.DelegatedCredential{
					Kind: protocol.CredentialSessionURL, Value: "wss://api.gladia.io/v2/other?token=t",
				}
			},
		},
		{
			name: "endpoint_wrong_path", want: "endpoint path must be /v2/live",
			mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = "wss://api.gladia.io/v2/pre-recorded" },
		},
		{
			name: "unsupported_region", want: "region",
			mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Region = "mars-north" },
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			adapter, err := New(testConfig())
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			request := byokRequest(initServer, protocol.RequestOptions{})
			testCase.mutate(&request)
			_, err = adapter.Open(context.Background(), request)
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("open error = %v, want it to mention %q", err, testCase.want)
			}
			// No rejection path may render the credential into its message.
			if strings.Contains(err.Error(), "customer-account-key") {
				t.Fatalf("rejection leaked the credential: %v", err)
			}
		})
	}
}

// TestRegionSelectorReachesTheInitQuery checks the one documented knob that
// lives on the init URL rather than in its body.
func TestRegionSelectorReachesTheInitQuery(t *testing.T) {
	t.Parallel()

	inits := make(chan capturedInit, 1)
	socket := newSocketServer(t, nil, func(ctx context.Context, conn *websocket.Conn) {
		_ = assertControl(ctx, conn, "stop_recording")
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	defer socket.Close()
	initServer := newInitServer(t, inits, sessionURLFor(socket, "tok"), http.StatusCreated)
	defer initServer.Close()

	adapter, err := New(testConfig())
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	request := byokRequest(initServer, protocol.RequestOptions{})
	request.Plan.Route.Region = "us-west"
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}

	select {
	case captured := <-inits:
		if got := captured.query.Get("region"); got != "us-west" {
			t.Fatalf("init region = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("init endpoint was never called")
	}
}

// TestMalformedInitResponsesAreRejected keeps a broken vendor reply from
// turning into a dial against something unvalidated.
func TestMalformedInitResponsesAreRejected(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name string
		body string
		want string
	}{
		{name: "not_json", body: "<html>oops</html>", want: "malformed live session response"},
		{name: "no_url", body: `{"id":"x","created_at":"now"}`, want: "not an absolute URL"},
		{name: "foreign_host", body: `{"id":"x","url":"wss://attacker.example/v2/live?token=t"}`, want: "host is not allowed"},
		{name: "plain_http_url", body: `{"id":"x","url":"https://api.gladia.io/v2/live?token=t"}`, want: "must use wss"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			initServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(testCase.body))
			}))
			defer initServer.Close()

			// A policy without the insecure override so wss is genuinely required.
			adapter, err := New(Config{AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			_, err = adapter.Open(context.Background(), byokRequest(initServer, protocol.RequestOptions{}))
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("open error = %v, want it to mention %q", err, testCase.want)
			}
		})
	}
}

// TestWriteAfterStopIsRejected guards the one-shot nature of stop_recording:
// Gladia ends the session on that frame, so later audio has nowhere to go.
func TestWriteAfterStopIsRejected(t *testing.T) {
	t.Parallel()

	socket := newSocketServer(t, nil, func(ctx context.Context, conn *websocket.Conn) {
		if err := assertControl(ctx, conn, "stop_recording"); err != nil {
			t.Errorf("stop_recording: %v", err)
			return
		}
		<-ctx.Done()
	})
	defer socket.Close()
	initServer := newInitServer(t, nil, sessionURLFor(socket, "tok"), http.StatusCreated)
	defer initServer.Close()

	adapter, err := New(testConfig())
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), byokRequest(initServer, protocol.RequestOptions{}))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer stream.Cancel(context.Background())
	if err := stream.CommitAudio(context.Background()); err != nil {
		t.Fatalf("commit audio: %v", err)
	}
	if err := stream.WriteAudio(context.Background(), []byte{1}); err != runtimepkg.ErrSessionClosed {
		t.Fatalf("write after stop = %v, want ErrSessionClosed", err)
	}
}

// TestTextInputIsUnsupported records that this is an STT-only adapter.
func TestTextInputIsUnsupported(t *testing.T) {
	t.Parallel()

	socket := newSocketServer(t, nil, func(ctx context.Context, conn *websocket.Conn) { <-ctx.Done() })
	defer socket.Close()
	initServer := newInitServer(t, nil, sessionURLFor(socket, "tok"), http.StatusCreated)
	defer initServer.Close()

	adapter, err := New(testConfig())
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	if adapter.ID() != AdapterID {
		t.Fatalf("adapter id = %q", adapter.ID())
	}
	stream, err := adapter.Open(context.Background(), byokRequest(initServer, protocol.RequestOptions{}))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer stream.Cancel(context.Background())
	if err := stream.AppendText(context.Background(), "hi"); err != runtimepkg.ErrUnsupportedOperation {
		t.Fatalf("append text = %v", err)
	}
	if err := stream.CommitText(context.Background()); err != runtimepkg.ErrUnsupportedOperation {
		t.Fatalf("commit text = %v", err)
	}
	if err := stream.WriteAudio(context.Background(), nil); err == nil {
		t.Fatal("empty audio must be rejected")
	}
}

// TestIdleCloseCodesAreNamed turns Gladia's vendor-specific idle close codes
// into something an operator can act on instead of a bare read failure.
func TestIdleCloseCodesAreNamed(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name string
		code websocket.StatusCode
		want string
	}{
		{name: "no_audio", code: closeIdleNoAudio, want: "no audio"},
		{name: "no_transcription", code: closeIdleNoTranscription, want: "no transcribable audio"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			socket := newSocketServer(t, nil, func(ctx context.Context, conn *websocket.Conn) {
				_ = conn.Close(testCase.code, "idle")
			})
			defer socket.Close()
			initServer := newInitServer(t, nil, sessionURLFor(socket, "tok"), http.StatusCreated)
			defer initServer.Close()

			adapter, err := New(testConfig())
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			stream, err := adapter.Open(context.Background(), byokRequest(initServer, protocol.RequestOptions{}))
			if err != nil {
				t.Fatalf("open stream: %v", err)
			}
			defer stream.Cancel(context.Background())

			providerError := providerErrorFrom(t, awaitStreamError(t, stream.Events()))
			if !strings.Contains(providerError.Message, testCase.want) || !providerError.Retryable {
				t.Fatalf("idle close error = %+v", providerError)
			}
		})
	}
}

// TestUnknownMessageTypesBecomeWarnings keeps a new vendor frame from killing a
// healthy session.
func TestUnknownMessageTypesBecomeWarnings(t *testing.T) {
	t.Parallel()

	socket := newSocketServer(t, nil, func(ctx context.Context, conn *websocket.Conn) {
		// post_transcript is dropped outright; it duplicates the billing frame.
		_ = writeJSON(ctx, conn, frame("post_transcript", map[string]any{"full_transcript": "hello"}))
		_ = writeJSON(ctx, conn, frame("some_future_type", map[string]any{}))
		// An empty utterance must not produce a transcript event either.
		_ = writeJSON(ctx, conn, frame("transcript", map[string]any{
			"is_final": true, "utterance": map[string]any{"text": "   "},
		}))
		_ = writeJSON(ctx, conn, frame("speech_end", map[string]any{"time": 2, "channel": 0}))
		<-ctx.Done()
	})
	defer socket.Close()
	initServer := newInitServer(t, nil, sessionURLFor(socket, "tok"), http.StatusCreated)
	defer initServer.Close()

	adapter, err := New(testConfig())
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), byokRequest(initServer, protocol.RequestOptions{}))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer stream.Cancel(context.Background())

	events := collectProviderEvents(t, stream.Events(), 3)
	if got := eventTypes(events); strings.Join(got, ",") != strings.Join([]string{
		string(protocol.EventUsageObserved),
		string(protocol.EventWarning),
		string(protocol.EventSpeechEnded),
	}, ",") {
		t.Fatalf("event types = %v", got)
	}
	var ended struct {
		AudioEndMS int64 `json:"audio_end_ms"`
	}
	if err := json.Unmarshal(events[2].Data, &ended); err != nil || ended.AudioEndMS != 2_000 {
		t.Fatalf("speech end = %+v, err=%v", ended, err)
	}
}

// TestLanguageTagsNarrowToGladiasEnum pins the tag normalisation. Gladia's
// TranscriptionLanguageCodeEnum is 207 bare ISO codes and contains no
// hyphenated regional variant at all, so any tag the rest of the gateway uses
// has to lose its region before it reaches the init body.
func TestLanguageTagsNarrowToGladiasEnum(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct{ in, want string }{
		{in: "en", want: "en"},
		{in: "en-US", want: "en"},
		{in: "EN-GB", want: "en"},
		{in: "zh-Hans-CN", want: "zh"},
		{in: "en_US", want: "en"},
		{in: "  fr-FR  ", want: "fr"},
		// Three-letter codes such as haw are real enum members and must survive.
		{in: "haw", want: "haw"},
		{in: "", want: ""},
	} {
		if got := gladiaLanguage(testCase.in); got != testCase.want {
			t.Errorf("gladiaLanguage(%q) = %q, want %q", testCase.in, got, testCase.want)
		}
	}
}

// --- helpers -----------------------------------------------------------

type transcriptPayload struct {
	Text              string  `json:"text"`
	IsFinal           bool    `json:"is_final"`
	AudioStartMS      int64   `json:"audio_start_ms"`
	AudioEndMS        int64   `json:"audio_end_ms"`
	Language          string  `json:"language"`
	Confidence        float64 `json:"confidence"`
	UtteranceID       string  `json:"utterance_id"`
	ProviderRequestID string  `json:"provider_request_id"`
}

type capturedInit struct {
	header http.Header
	query  url.Values
	body   initRequest
}

func testConfig() Config {
	// httptest listens on 127.0.0.1 with an ephemeral port, so both the host
	// allowlist entry and the insecure override are needed.
	return Config{AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true}
}

func newInitServer(t *testing.T, captured chan capturedInit, sessionURL string, status int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != livePath || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		var body initRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode init body: %v", err)
		}
		if captured != nil {
			captured <- capturedInit{header: r.Header.Clone(), query: r.URL.Query(), body: body}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if status < 200 || status > 299 {
			_, _ = w.Write([]byte(`{"message":"rejected"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(initResponse{
			ID: testSessionID, CreatedAt: "2026-08-07T00:00:00.000Z", URL: sessionURL,
		})
	}))
}

func newSocketServer(t *testing.T, handshakes chan *http.Request, handle func(context.Context, *websocket.Conn)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != livePath {
			http.NotFound(w, r)
			return
		}
		if handshakes != nil {
			handshakes <- r.Clone(context.Background())
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		go func() {
			defer cancel()
			defer conn.CloseNow()
			handle(ctx, conn)
		}()
	}))
}

func hostPort(server *httptest.Server) string {
	parsed, _ := url.Parse(server.URL)
	return parsed.Host
}

func sessionURLFor(socket *httptest.Server, token string) string {
	return "ws://" + hostPort(socket) + livePath + "?token=" + token
}

func byokRequest(initServer *httptest.Server, options protocol.RequestOptions) runtimepkg.AdapterRequest {
	now := time.Date(2026, time.August, 7, 12, 0, 0, 0, time.UTC)
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindSTT,
		Plan: protocol.SessionPlan{
			PlanID:    "plan_gladia",
			SessionID: "sess_gladia",
			AttemptID: "att_1",
			Execution: protocol.Execution{
				Placement: protocol.PlacementEmbedded, ProviderRoute: protocol.RouteProviderDirect,
				CredentialSource: protocol.CredentialsBYOK,
			},
			ExpiresAt: now.Add(time.Hour),
			Route: protocol.PlanRoute{
				Provider: "gladia", Model: DefaultModel, Adapter: AdapterID,
				Transport: protocol.TransportWebSocket,
				// The plan endpoint is the query-free live base; the adapter
				// derives the init POST URL from it by swapping the scheme.
				Endpoint: "ws://" + hostPort(initServer) + livePath,
				Credential: &protocol.DelegatedCredential{
					Kind: protocol.CredentialBearer, Value: "customer-account-key", ExpiresAt: now.Add(30 * time.Minute),
				},
			},
			Reservation: protocol.Reservation{
				ID: "res_gladia", LeaseDurationSeconds: 60, LeaseExpiresAt: now.Add(time.Minute),
				Concurrency: protocol.ConcurrencyReservation{LeaseID: "conc_gladia", Slots: 1},
				Usage:       protocol.UsageReservation{Unit: protocol.UsageUnitDurationSeconds, AuthorizedUnits: 60},
			},
			Requirements: protocol.Requirements{
				Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision, RuntimeVersion: "0.1.0",
			},
			Signature: "test-signature",
		},
		Options: options,
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
	}
}

func frame(messageType string, data map[string]any) map[string]any {
	return map[string]any{
		"type": messageType, "session_id": testSessionID,
		"created_at": "2026-08-07T00:00:00.000Z", "data": data,
	}
}

func writeJSON(ctx context.Context, conn *websocket.Conn, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, payload)
}

func assertControl(ctx context.Context, conn *websocket.Conn, want string) error {
	messageType, payload, err := conn.Read(ctx)
	if err != nil || messageType != websocket.MessageText {
		return fmt.Errorf("control read = (%v, %q, %w)", messageType, payload, err)
	}
	var control struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(payload, &control); err != nil || control.Type != want {
		return fmt.Errorf("control = %q err=%v, want %q", payload, err, want)
	}
	return nil
}

func collectProviderEvents(t *testing.T, events <-chan runtimepkg.ProviderEvent, want int) []runtimepkg.ProviderEvent {
	t.Helper()
	collected := make([]runtimepkg.ProviderEvent, 0, want)
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for len(collected) < want {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatalf("provider events closed after %d events", len(collected))
			}
			if event.Err != nil {
				t.Fatalf("provider event error: %v", event.Err)
			}
			collected = append(collected, event)
		case <-timer.C:
			t.Fatalf("timed out after %d of %d provider events", len(collected), want)
		}
	}
	return collected
}

func awaitStreamError(t *testing.T, events <-chan runtimepkg.ProviderEvent) error {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatal("provider events closed without a terminal error")
			}
			if event.Err != nil {
				return event.Err
			}
		case <-timer.C:
			t.Fatal("timed out waiting for a terminal provider error")
		}
	}
}

func providerErrorFrom(t *testing.T, err error) *runtimepkg.ProviderError {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error")
	}
	var providerError *runtimepkg.ProviderError
	if !asProviderError(err, &providerError) {
		t.Fatalf("error %v is not a *runtime.ProviderError", err)
	}
	return providerError
}

func asProviderError(err error, target **runtimepkg.ProviderError) bool {
	for err != nil {
		if providerError, ok := err.(*runtimepkg.ProviderError); ok {
			*target = providerError
			return true
		}
		unwrapper, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = unwrapper.Unwrap()
	}
	return false
}

func eventTypes(events []runtimepkg.ProviderEvent) []string {
	types := make([]string, len(events))
	for index, event := range events {
		types[index] = string(event.Type)
	}
	return types
}
