package playht

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

const (
	testUserID = "user-abc"
	testAPIKey = "secret-playht-key"
	// testSessionPath and testSessionToken stand in for the opaque, expiring
	// URL PlayHT mints. They are deliberately unlike anything the adapter could
	// construct on its own, so a test that observes them proves the adapter
	// dialled what the auth endpoint returned.
	testSessionPath  = "/playht-fal/playht-tts/stream"
	testSessionToken = "fal-jwt-token-from-auth"
)

// TestTTSAdapterMintsSessionURLAndStreamsBinaryAudio covers the whole BYOK
// path end to end: the auth call, the fact that the returned URL is the one
// dialled, the synthesis command, and the audio.started/frame*/done sequence.
func TestTTSAdapterMintsSessionURLAndStreamsBinaryAudio(t *testing.T) {
	t.Parallel()
	chunks := [][]byte{{1, 2, 3, 4}, {5, 6, 7, 8}, {9, 10}}
	commands := make(chan synthesisCommand, 1)
	harness := newHarness(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		command, err := readSynthesis(ctx, conn)
		if err != nil {
			t.Errorf("read synthesis command: %v", err)
			return
		}
		commands <- command
		if err := writeJSON(ctx, conn, map[string]any{"type": "start", "status": 200, "request_id": command.RequestID}); err != nil {
			t.Errorf("start: %v", err)
			return
		}
		// PlayHT prefixes container formats with a header chunk. Sending one
		// here proves the adapter drops it instead of emitting it as samples.
		if err := conn.Write(ctx, websocket.MessageBinary, []byte("RIFFxxxxWAVE")); err != nil {
			t.Errorf("header chunk: %v", err)
			return
		}
		// Several separate binary messages, because the point of this adapter
		// is streaming: the caller must see audio before synthesis completes.
		for _, chunk := range chunks {
			if err := conn.Write(ctx, websocket.MessageBinary, chunk); err != nil {
				t.Errorf("audio chunk: %v", err)
				return
			}
		}
		if err := writeJSON(ctx, conn, map[string]any{"type": "end", "status": 200, "request_id": command.RequestID}); err != nil {
			t.Errorf("end: %v", err)
			return
		}
		_, _, _ = conn.Read(ctx) // Block until the client closes.
	})
	defer harness.close()

	adapter, err := New(harness.config())
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), harness.request(protocol.CredentialsBYOK, ""))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// Two appends followed by one commit must produce ONE request carrying the
	// joined text: PlayHT has no incremental append, so a per-append send would
	// synthesize two clipped utterances instead of one sentence.
	if err := stream.AppendText(context.Background(), "Hello, "); err != nil {
		t.Fatalf("append first: %v", err)
	}
	if err := stream.AppendText(context.Background(), "world."); err != nil {
		t.Fatalf("append second: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}

	events := collectProviderEvents(t, stream.Events(), 2+len(chunks))
	if got := strings.Join(eventTypes(events), ","); got != "audio.started,audio.frame,audio.frame,audio.frame,audio.done" {
		t.Fatalf("event types = %s", got)
	}
	// Every chunk must arrive as its own frame with byte-exact payload: PlayHT
	// sends real binary frames, so there is no decode step that could corrupt.
	for index, want := range chunks {
		if got := events[1+index].Audio; string(got) != string(want) {
			t.Fatalf("frame %d audio = %v, want %v", index, got, want)
		}
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, ok := <-stream.Events(); ok {
		t.Fatal("events must close after the provider closes")
	}

	// The account credential must travel in PlayHT's two documented headers.
	authRequest := harness.awaitAuth(t)
	if authRequest.Method != http.MethodPost {
		t.Fatalf("auth method = %s", authRequest.Method)
	}
	if got := authRequest.Header.Get("Authorization"); got != "Bearer "+testAPIKey {
		t.Fatalf("authorization = %q", got)
	}
	if got := authRequest.Header.Get("X-User-Id"); got != testUserID {
		t.Fatalf("x-user-id = %q", got)
	}

	// The adapter must dial the URL the auth endpoint handed back -- path,
	// query token and all -- rather than a host it assembled itself.
	wsRequest := harness.awaitWebSocket(t)
	if wsRequest.URL.Path != testSessionPath {
		t.Fatalf("dialled path = %q, want %q", wsRequest.URL.Path, testSessionPath)
	}
	if got := wsRequest.URL.Query().Get("fal_jwt_token"); got != testSessionToken {
		t.Fatalf("session token = %q, want %q", got, testSessionToken)
	}

	var command synthesisCommand
	select {
	case command = <-commands:
	case <-time.After(5 * time.Second):
		t.Fatal("no synthesis command observed")
	}
	if command.Text != "Hello, world." {
		t.Fatalf("text = %q, want the joined buffer", command.Text)
	}
	if command.Voice != testVoice || command.VoiceEngine != defaultVoiceEngine {
		t.Fatalf("voice=%q engine=%q", command.Voice, command.VoiceEngine)
	}
	if command.OutputFormat != "raw" || command.SampleRate != 24_000 {
		t.Fatalf("output_format=%q sample_rate=%d", command.OutputFormat, command.SampleRate)
	}
	// PlayHT rejects BCP-47; "en-US" has to become its own language name.
	if command.Language != "english" {
		t.Fatalf("language = %q, want english", command.Language)
	}
	if command.RequestID == "" {
		t.Fatal("request_id must be set so end messages can be correlated")
	}
}

// TestTTSAdapterManagedPlanDialsPreMintedURLWithoutAuthCall proves the managed
// credential source never spends an account key: the control plane already
// minted the URL, so the adapter must connect straight to it.
func TestTTSAdapterManagedPlanDialsPreMintedURLWithoutAuthCall(t *testing.T) {
	t.Parallel()
	harness := newHarness(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		command, err := readSynthesis(ctx, conn)
		if err != nil {
			t.Errorf("read synthesis command: %v", err)
			return
		}
		_ = writeJSON(ctx, conn, map[string]any{"type": "start", "status": 200, "request_id": command.RequestID})
		_ = conn.Write(ctx, websocket.MessageBinary, []byte{42})
		_ = writeJSON(ctx, conn, map[string]any{"type": "end", "status": 200, "request_id": command.RequestID})
		_, _, _ = conn.Read(ctx)
	})
	defer harness.close()

	// Point the auth endpoint at a server that fails the test if it is used.
	config := harness.config()
	forbidden := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("managed plans must not call the PlayHT auth endpoint")
	}))
	defer forbidden.Close()
	config.AuthURL = forbidden.URL + "/api/v4/websocket-auth"

	adapter, err := New(config)
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), harness.request(protocol.CredentialsManaged, harness.sessionURL()))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := stream.AppendText(context.Background(), "managed"); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	events := collectProviderEvents(t, stream.Events(), 3)
	if got := strings.Join(eventTypes(events), ","); got != "audio.started,audio.frame,audio.done" {
		t.Fatalf("event types = %s", got)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := harness.awaitWebSocket(t).URL.Query().Get("fal_jwt_token"); got != testSessionToken {
		t.Fatalf("managed session token = %q", got)
	}
}

// TestTTSAdapterClassifiesAuthFailures pins the distinct error code per class
// so a caller can tell a bad key from an exhausted balance without parsing
// prose, and so retry logic only fires on genuinely transient failures.
func TestTTSAdapterClassifiesAuthFailures(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		status    int
		wantCode  string
		wantRetry bool
	}{
		{http.StatusUnauthorized, "authentication_failed", false},
		{http.StatusForbidden, "authentication_failed", false},
		{http.StatusPaymentRequired, "provider_quota_exceeded", false},
		{http.StatusTooManyRequests, "provider_rate_limited", true},
		{http.StatusBadRequest, "invalid_request", false},
		{http.StatusServiceUnavailable, "provider_unavailable", true},
	} {
		t.Run(fmt.Sprint(testCase.status), func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(testCase.status)
				_, _ = w.Write([]byte(`{"error":"denied"}`))
			}))
			defer server.Close()

			adapter, err := New(Config{
				AuthURL:               server.URL + "/api/v4/websocket-auth",
				AllowedEndpointHosts:  []string{"127.0.0.1"},
				AllowInsecureEndpoint: true,
			})
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			_, err = adapter.Open(context.Background(), baseRequest(protocol.CredentialsBYOK, "", "ws://127.0.0.1:1/x"))
			var providerErr *runtimepkg.ProviderError
			if !errors.As(err, &providerErr) {
				t.Fatalf("error = %v, want ProviderError", err)
			}
			if providerErr.Code != testCase.wantCode {
				t.Fatalf("code = %q, want %q", providerErr.Code, testCase.wantCode)
			}
			if providerErr.Retryable != testCase.wantRetry {
				t.Fatalf("retryable = %v, want %v", providerErr.Retryable, testCase.wantRetry)
			}
			if providerErr.ProviderStatus != testCase.status {
				t.Fatalf("provider status = %d", providerErr.ProviderStatus)
			}
			// The raw vendor body must survive for debugging.
			if providerErr.Extensions[extensionID] == nil {
				t.Fatal("auth failure must retain the PlayHT payload")
			}
			// A credential must never leak into an error surface.
			if strings.Contains(providerErr.Error(), testAPIKey) {
				t.Fatal("error message leaked the API key")
			}
		})
	}
}

// TestTTSAdapterFailsOnNonSuccessStatusInsideStartMessage covers PlayHT's
// unusual choice to report failures through an ordinary start message whose
// "status" is non-2xx. Without this the session would look successful but
// silently produce no audio.
func TestTTSAdapterFailsOnNonSuccessStatusInsideStartMessage(t *testing.T) {
	t.Parallel()
	harness := newHarness(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		command, err := readSynthesis(ctx, conn)
		if err != nil {
			return
		}
		_ = writeJSON(ctx, conn, map[string]any{
			"type": "start", "status": 401, "request_id": command.RequestID, "message": "Unauthorized",
		})
		_, _, _ = conn.Read(ctx)
	})
	defer harness.close()

	adapter, _ := New(harness.config())
	stream, err := adapter.Open(context.Background(), harness.request(protocol.CredentialsBYOK, ""))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := stream.AppendText(context.Background(), "hello"); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	event := collectProviderEvents(t, stream.Events(), 1)[0]
	var providerErr *runtimepkg.ProviderError
	if !errors.As(event.Err, &providerErr) {
		t.Fatalf("event = %+v, want a terminal ProviderError", event)
	}
	if providerErr.Code != "authentication_failed" || providerErr.ProviderStatus != 401 {
		t.Fatalf("code=%q status=%d", providerErr.Code, providerErr.ProviderStatus)
	}
	abortStream(t, stream)
}

// TestTTSAdapterCancelDropsBufferedTextAndReportsInFlightHonestly exists
// because PlayHT documents no cancel command: the adapter may only discard
// text it has not sent, and must say so rather than fake a barge-in.
func TestTTSAdapterCancelDropsBufferedTextAndReportsInFlightHonestly(t *testing.T) {
	t.Parallel()
	harness := newHarness(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		if _, err := readSynthesis(ctx, conn); err != nil {
			return
		}
		_, _, _ = conn.Read(ctx)
	})
	defer harness.close()

	adapter, _ := New(harness.config())
	stream, err := adapter.Open(context.Background(), harness.request(protocol.CredentialsBYOK, ""))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := stream.AppendText(context.Background(), "discard me"); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatalf("cancel buffered: %v", err)
	}
	// The buffer really is gone, so there is nothing left to synthesize.
	if err := stream.CommitText(context.Background()); err == nil {
		t.Fatal("commit after cancel must fail: the buffer was discarded")
	}
	if err := stream.AppendText(context.Background(), "keep me"); err != nil {
		t.Fatalf("append after cancel: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit second: %v", err)
	}
	if err := stream.Cancel(context.Background()); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Fatalf("in-flight cancel = %v, want ErrUnsupportedOperation", err)
	}
	abortStream(t, stream)
}

// TestTTSAdapterAbortMidSynthesisClosesStream verifies teardown while audio is
// still arriving: a caller that hangs up mid-utterance must not deadlock, and
// the event channel must close instead of leaking the read loop.
func TestTTSAdapterAbortMidSynthesisClosesStream(t *testing.T) {
	t.Parallel()
	harness := newHarness(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		command, err := readSynthesis(ctx, conn)
		if err != nil {
			return
		}
		_ = writeJSON(ctx, conn, map[string]any{"type": "start", "status": 200, "request_id": command.RequestID})
		_ = conn.Write(ctx, websocket.MessageBinary, []byte{1, 2})
		// Deliberately never send "end": the utterance stays in flight.
		<-ctx.Done()
	})
	defer harness.close()

	adapter, _ := New(harness.config())
	stream, err := adapter.Open(context.Background(), harness.request(protocol.CredentialsBYOK, ""))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := stream.AppendText(context.Background(), "mid-synthesis"); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	events := collectProviderEvents(t, stream.Events(), 2)
	if got := strings.Join(eventTypes(events), ","); got != "audio.started,audio.frame" {
		t.Fatalf("event types = %s", got)
	}
	aborting, ok := stream.(runtimepkg.AbortingProviderStream)
	if !ok {
		t.Fatal("stream must implement AbortingProviderStream")
	}
	_ = aborting.Abort(context.Background())
	// Draining must terminate promptly rather than block on a dead socket.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range stream.Events() {
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("events channel did not close after Abort")
	}
	// Writing after Abort must be refused, not panic on a closed socket.
	if err := stream.AppendText(context.Background(), "after abort"); err == nil {
		t.Fatal("append after abort must fail")
	}
}

// TestTTSAdapterRejectsInvalidRequests keeps malformed work local: each case
// would otherwise become a confusing upstream failure after a credential had
// already been spent on PlayHT's auth endpoint.
func TestTTSAdapterRejectsInvalidRequests(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*runtimepkg.AdapterRequest){
		"wrong kind": func(r *runtimepkg.AdapterRequest) {
			r.Kind = protocol.SessionKindSTT
		},
		"wrong provider": func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Provider = "elevenlabs"
		},
		"wrong transport": func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Transport = protocol.TransportHTTP
		},
		// "auto" means the control plane never resolved an engine; the adapter
		// cannot pick one because the auth response is keyed by engine name.
		"auto model": func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Model = "auto"
		},
		"http-only voice engine": func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Model = "PlayDialog-turbo"
		},
		"missing credential": func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Credential = nil
		},
		"empty credential": func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Credential.Value = "   "
		},
		// Without the user id half, the auth call cannot be made at all.
		"byok credential missing user id": func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Credential.Value = testAPIKey
		},
		"missing voice": func(r *runtimepkg.AdapterRequest) {
			r.Options.Voice = ""
		},
		"missing media": func(r *runtimepkg.AdapterRequest) {
			r.Media = nil
		},
		"unsupported encoding": func(r *runtimepkg.AdapterRequest) {
			r.Media.Encoding = "opus"
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			adapter, err := New(Config{AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			request := baseRequest(protocol.CredentialsBYOK, "", "ws://127.0.0.1:1/x")
			mutate(&request)
			if _, err := adapter.Open(context.Background(), request); err == nil {
				t.Fatal("adapter accepted an invalid request")
			}
		})
	}
}

// TestTTSAdapterRejectsManagedPlanWithoutSessionURL guards the managed
// contract: a bearer key in a managed plan means a permanent credential
// escaped into a place that is only supposed to carry a minted URL.
func TestTTSAdapterRejectsManagedPlanWithoutSessionURL(t *testing.T) {
	t.Parallel()
	adapter, err := New(Config{AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	request := baseRequest(protocol.CredentialsManaged, "", "ws://127.0.0.1:1/x")
	request.Plan.Route.Credential.Kind = protocol.CredentialBearer
	if _, err := adapter.Open(context.Background(), request); err == nil {
		t.Fatal("managed plan accepted a bearer credential")
	}
}

// TestParseAuthResponseAcceptsEveryKnownShape exists because PlayHT's own docs
// and SDK disagree: the page promises a flat "websocket_url" in one paragraph
// and a keyed "websocket_urls" object in the next, while the SDK returns
// per-engine objects. Keying off one shape would break the others.
func TestParseAuthResponseAcceptsEveryKnownShape(t *testing.T) {
	t.Parallel()
	const want = "wss://ws.fal.run/playht-fal/playht-tts/stream?fal_jwt_token=abc"
	for name, payload := range map[string]string{
		"documented websocket_urls map": `{"websocket_urls":{"Play3.0-mini":"` + want + `"},"expires_at":"2024-12-11T22:17:37.429Z"}`,
		"sdk per-engine objects":        `{"Play3.0-mini":{"websocket_url":"` + want + `","http_streaming_url":"https://x"},"expires_at":"2024-12-11T22:17:37.429Z"}`,
		"legacy flat websocket_url":     `{"websocket_url":"` + want + `"}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got, expiresAt, err := parseAuthResponse([]byte(payload), defaultVoiceEngine)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got != want {
				t.Fatalf("url = %q, want %q", got, want)
			}
			if strings.Contains(payload, "expires_at") && expiresAt.IsZero() {
				t.Fatal("expires_at must be parsed when present")
			}
		})
	}

	// A response that simply lacks the requested engine has to fail loudly
	// rather than fall back to some other engine's URL.
	if _, _, err := parseAuthResponse([]byte(`{"websocket_urls":{"PlayDialog":"wss://ws.fal.run/x"}}`), defaultVoiceEngine); err == nil {
		t.Fatal("missing engine must be an error")
	}
	if _, _, err := parseAuthResponse([]byte(`{"expires_at":"2024-12-11T22:17:37.429Z"}`), defaultVoiceEngine); err == nil {
		t.Fatal("response without any url must be an error")
	}
}

// TestValidateSessionURLPinsHostWhileKeepingToken is the security-critical
// check. PlayHT puts its session JWT in the query string, so the adapter must
// keep the query while still refusing a URL on an unexpected host -- a
// compromised or spoofed auth response must not redirect customer audio.
func TestValidateSessionURLPinsHostWhileKeepingToken(t *testing.T) {
	t.Parallel()
	adapter, err := New(Config{})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	const official = "wss://ws.fal.run/playht-fal/playht-tts/stream?fal_jwt_token=abc"
	got, err := adapter.validateSessionURL(official)
	if err != nil {
		t.Fatalf("official url rejected: %v", err)
	}
	if got != official {
		t.Fatalf("url = %q, want the token query preserved", got)
	}
	for _, raw := range []string{
		"wss://evil.test/playht-fal/stream?fal_jwt_token=abc",
		"ws://ws.fal.run/stream?fal_jwt_token=abc",
		"wss://user@ws.fal.run/stream?fal_jwt_token=abc",
		"wss://ws.fal.run:8443/stream?fal_jwt_token=abc",
		"https://ws.fal.run/stream",
		"not-a-url",
	} {
		if _, err := adapter.validateSessionURL(raw); err == nil {
			t.Fatalf("unsafe url accepted: %s", raw)
		}
	}
}

// TestPlayHTLanguageMapsBCP47AndOmitsUnknown documents the deliberate choice
// to omit rather than guess: PlayHT rejects unknown language names, so an
// unmapped tag must fall through to the voice instead of failing the request.
func TestPlayHTLanguageMapsBCP47AndOmitsUnknown(t *testing.T) {
	t.Parallel()
	for tag, want := range map[string]string{
		"en": "english", "en-US": "english", "ES-419": "spanish",
		"zh-Hans": "mandarin", "fil": "tagalog", "": "", "kk": "", "klingon": "",
	} {
		if got := playHTLanguage(tag); got != want {
			t.Fatalf("playHTLanguage(%q) = %q, want %q", tag, got, want)
		}
	}
}

// --- harness -------------------------------------------------------------

const testVoice = "s3://voice-cloning-zero-shot/abc/jennifer/manifest.json"

type harness struct {
	authServer *httptest.Server
	wsServer   *httptest.Server
	authSeen   chan *http.Request
	wsSeen     chan *http.Request
	handlerCtx context.Context
	stopAll    context.CancelFunc
}

// newHarness runs the auth endpoint and the synthesis socket on two SEPARATE
// servers. The adapter can only reach the socket by reading the URL out of the
// auth response, which is exactly the behaviour under test.
func newHarness(t *testing.T, handler func(context.Context, *http.Request, *websocket.Conn)) *harness {
	t.Helper()
	h := &harness{
		authSeen: make(chan *http.Request, 4),
		wsSeen:   make(chan *http.Request, 4),
	}
	// Handlers live until close() so a test can hold a socket open, and so
	// httptest.Server.Close never blocks on a handler parked in Read.
	h.handlerCtx, h.stopAll = context.WithCancel(context.Background())
	h.wsServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != testSessionPath {
			http.NotFound(w, request)
			return
		}
		h.wsSeen <- request.Clone(request.Context())
		conn, err := websocket.Accept(w, request, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		go func() {
			defer conn.CloseNow()
			handler(h.handlerCtx, request, conn)
		}()
	}))
	h.authServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		h.authSeen <- request.Clone(request.Context())
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"websocket_urls": map[string]string{defaultVoiceEngine: h.sessionURL()},
			"expires_at":     time.Now().UTC().Add(time.Hour).Format("2006-01-02T15:04:05.000Z"),
		})
	}))
	return h
}

// sessionURL is the opaque, token-bearing URL the fake auth endpoint mints.
func (h *harness) sessionURL() string {
	endpoint, _ := url.Parse(h.wsServer.URL)
	endpoint.Scheme = "ws"
	endpoint.Path = testSessionPath
	endpoint.RawQuery = url.Values{"fal_jwt_token": {testSessionToken}}.Encode()
	return endpoint.String()
}

func (h *harness) config() Config {
	return Config{
		AuthURL:               h.authServer.URL + "/api/v4/websocket-auth",
		AllowedEndpointHosts:  []string{"127.0.0.1"},
		AllowInsecureEndpoint: true,
	}
}

func (h *harness) request(source protocol.CredentialSource, sessionURL string) runtimepkg.AdapterRequest {
	return baseRequest(source, sessionURL, h.sessionURL())
}

func (h *harness) close() {
	h.stopAll()
	h.authServer.Close()
	h.wsServer.Close()
}

func (h *harness) awaitAuth(t *testing.T) *http.Request {
	t.Helper()
	select {
	case request := <-h.authSeen:
		return request
	case <-time.After(5 * time.Second):
		t.Fatal("auth endpoint was never called")
		return nil
	}
}

func (h *harness) awaitWebSocket(t *testing.T) *http.Request {
	t.Helper()
	select {
	case request := <-h.wsSeen:
		return request
	case <-time.After(5 * time.Second):
		t.Fatal("websocket was never dialled")
		return nil
	}
}

type synthesisCommand struct {
	Text         string `json:"text"`
	Voice        string `json:"voice"`
	VoiceEngine  string `json:"voice_engine"`
	OutputFormat string `json:"output_format"`
	SampleRate   int    `json:"sample_rate"`
	Language     string `json:"language"`
	RequestID    string `json:"request_id"`
}

// readSynthesis decodes the one JSON command PlayHT expects per utterance, so
// handlers can both assert on it and echo back its request id.
func readSynthesis(ctx context.Context, conn *websocket.Conn) (synthesisCommand, error) {
	messageType, payload, err := conn.Read(ctx)
	if err != nil {
		return synthesisCommand{}, err
	}
	if messageType != websocket.MessageText {
		return synthesisCommand{}, fmt.Errorf("synthesis command must be text, got %v", messageType)
	}
	var command synthesisCommand
	if err := json.Unmarshal(payload, &command); err != nil {
		return synthesisCommand{}, err
	}
	return command, nil
}

func baseRequest(source protocol.CredentialSource, sessionURL, endpoint string) runtimepkg.AdapterRequest {
	now := time.Date(2026, time.August, 7, 12, 0, 0, 0, time.UTC)
	credential := protocol.DelegatedCredential{
		Kind:      protocol.CredentialBearer,
		Value:     testUserID + ":" + testAPIKey,
		ExpiresAt: now.Add(time.Hour),
	}
	if source == protocol.CredentialsManaged {
		credential = protocol.DelegatedCredential{
			Kind:      protocol.CredentialSessionURL,
			Value:     sessionURL,
			ExpiresAt: now.Add(time.Hour),
		}
	}
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindTTS,
		Plan: protocol.SessionPlan{
			PlanID: "plan_playht", SessionID: "sess_playht", AttemptID: "att_1",
			Execution: protocol.Execution{
				Placement:        protocol.PlacementEmbedded,
				ProviderRoute:    protocol.RouteProviderDirect,
				CredentialSource: source,
			},
			ExpiresAt: now.Add(time.Hour),
			Route: protocol.PlanRoute{
				Provider: "playht", Model: defaultVoiceEngine, Adapter: AdapterID,
				Transport: protocol.TransportWebSocket, Endpoint: endpoint,
				Credential: &credential,
			},
			Reservation: protocol.Reservation{
				ID: "res_playht", LeaseDurationSeconds: 60, LeaseExpiresAt: now.Add(time.Minute),
				Concurrency: protocol.ConcurrencyReservation{LeaseID: "conc_playht", Slots: 1},
				Usage:       protocol.UsageReservation{Unit: protocol.UsageUnitCharacters, AuthorizedUnits: 4_000},
			},
			Telemetry:    protocol.Telemetry{Endpoint: "https://control.speko.test/v1/runtime-events", Token: "telemetry-token", FlushIntervalMS: 5_000},
			Requirements: protocol.Requirements{Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision, RuntimeVersion: "test"},
			Signature:    "test",
		},
		Options: protocol.RequestOptions{Voice: testVoice, Language: "en-US", MaxInputCharacters: 4_000},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1},
	}
}

// abortStream reaches the optional fast-close capability the runtime uses
// after a terminal failure. Every adapter stream here must provide it.
func abortStream(t *testing.T, stream runtimepkg.ProviderStream) {
	t.Helper()
	aborting, ok := stream.(runtimepkg.AbortingProviderStream)
	if !ok {
		t.Fatal("stream must implement AbortingProviderStream")
	}
	_ = aborting.Abort(context.Background())
}

func writeJSON(ctx context.Context, conn *websocket.Conn, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, payload)
}

func collectProviderEvents(t *testing.T, events <-chan runtimepkg.ProviderEvent, want int) []runtimepkg.ProviderEvent {
	t.Helper()
	collected := make([]runtimepkg.ProviderEvent, 0, want)
	for len(collected) < want {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatalf("events closed after %d of %d events", len(collected), want)
			}
			collected = append(collected, event)
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out after %d of %d events", len(collected), want)
		}
	}
	return collected
}

func eventTypes(events []runtimepkg.ProviderEvent) []string {
	types := make([]string, 0, len(events))
	for _, event := range events {
		types = append(types, string(event.Type))
	}
	return types
}
