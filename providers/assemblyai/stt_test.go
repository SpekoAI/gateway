package assemblyai

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

// TestManagedSessionTokenAuthQueryAndRealtimePartials is the wire contract in
// one test. It pins three things that would otherwise fail silently:
//
//  1. Every query parameter by exact name. AssemblyAI ignores unrecognized
//     query parameters instead of rejecting them, so a typo here is a feature
//     that is simply switched off while the session looks perfectly healthy.
//  2. A managed session authenticates with the temporary token in the `token`
//     QUERY parameter and sends no Authorization header at all.
//  3. A partial transcript reaches the consumer DURING the audio stream,
//     before anything is committed. That is what makes this endpoint usable for
//     a realtime voice agent, and it is exactly what would disappear if someone
//     later repointed this adapter at a batch transcription API — the adapter
//     would still "work", just with all output arriving after the last byte.
func TestManagedSessionTokenAuthQueryAndRealtimePartials(t *testing.T) {
	t.Parallel()
	requests := make(chan *http.Request, 1)
	server := newSTTServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		requests <- request.Clone(request.Context())
		// One full 100 ms batch: 16 kHz s16 mono => 3200 bytes.
		if err := assertBinary(ctx, conn, 3200); err != nil {
			t.Errorf("first audio batch: %v", err)
			return
		}
		// Speech is still being written when these land.
		for _, message := range []map[string]any{
			{"type": "Begin", "id": "session-aai-1", "expires_at": 1_780_000_000},
			{"type": "SpeechStarted", "timestamp": 640, "confidence": 0.9},
			{"type": "Turn", "turn_order": 0, "transcript": "my name is", "end_of_turn": false, "turn_is_formatted": false, "words": []any{}},
		} {
			if err := writeJSONFrame(ctx, conn, message); err != nil {
				t.Errorf("write %v: %v", message["type"], err)
				return
			}
		}
		// The 50 ms remainder is held back until CommitAudio flushes it, so the
		// tail arrives as its own 1600-byte message rather than being dropped.
		if err := assertBinary(ctx, conn, 1600); err != nil {
			t.Errorf("flushed tail: %v", err)
			return
		}
		if err := assertControl(ctx, conn, "ForceEndpoint"); err != nil {
			t.Errorf("commit: %v", err)
			return
		}
		// AssemblyAI answers a forced endpoint with a rough unformatted final
		// first, then the corrected formatted one. Only the second may surface.
		for _, message := range []map[string]any{
			{"type": "Turn", "turn_order": 0, "transcript": "my name is keanu reves", "end_of_turn": true, "turn_is_formatted": false, "end_of_turn_confidence": 0.8},
			{"type": "Turn", "turn_order": 0, "transcript": "My name is Keanu Reeves.", "end_of_turn": true, "turn_is_formatted": true, "end_of_turn_confidence": 0.93, "language_code": "en"},
		} {
			if err := writeJSONFrame(ctx, conn, message); err != nil {
				t.Errorf("write turn: %v", err)
				return
			}
		}
		if err := assertControl(ctx, conn, "Terminate"); err != nil {
			t.Errorf("terminate: %v", err)
			return
		}
		if err := writeJSONFrame(ctx, conn, map[string]any{"type": "Termination", "audio_duration_seconds": 2.5, "session_duration_seconds": 3.25}); err != nil {
			t.Errorf("write termination: %v", err)
			return
		}
		// Hold the handler open until the client closes so the Termination frame
		// is never raced by the deferred teardown.
		_, _, _ = conn.Read(ctx)
	})
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), sttRequest(server.URL, protocol.CredentialsManaged))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	if err := stream.WriteAudio(context.Background(), make([]byte, 3_200)); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	// Collected BEFORE CommitAudio: proof the transcript is streaming, not batched.
	live := collectEvents(t, stream.Events(), 3)
	for index, want := range []protocol.EventType{protocol.EventUsageObserved, protocol.EventSpeechStarted, protocol.EventTranscriptDelta} {
		if live[index].Type != want {
			t.Fatalf("live event %d = %q, want %q", index, live[index].Type, want)
		}
	}
	var partial struct {
		Text    string `json:"text"`
		IsFinal bool   `json:"is_final"`
	}
	if err := json.Unmarshal(live[2].Data, &partial); err != nil || partial.Text != "my name is" || partial.IsFinal {
		t.Fatalf("partial = %+v, err=%v", partial, err)
	}
	var speechStarted struct {
		AudioStartMS int64 `json:"audio_start_ms"`
	}
	// AssemblyAI reports SpeechStarted.timestamp in milliseconds, unlike the
	// seconds most vendors use; it must not be multiplied on the way through.
	if err := json.Unmarshal(live[1].Data, &speechStarted); err != nil || speechStarted.AudioStartMS != 640 {
		t.Fatalf("speech started = %+v, err=%v", speechStarted, err)
	}

	if err := stream.WriteAudio(context.Background(), make([]byte, 1_600)); err != nil {
		t.Fatalf("write tail: %v", err)
	}
	if err := stream.CommitAudio(context.Background()); err != nil {
		t.Fatalf("commit audio: %v", err)
	}
	committed := collectEvents(t, stream.Events(), 2)
	if committed[0].Type != protocol.EventTranscriptFinal || committed[1].Type != protocol.EventSpeechEnded {
		t.Fatalf("committed events = %q, %q", committed[0].Type, committed[1].Type)
	}
	if committed[0].Extensions[extensionID] == nil {
		t.Fatal("final transcript did not retain the AssemblyAI raw frame")
	}
	var final struct {
		Text              string  `json:"text"`
		IsFinal           bool    `json:"is_final"`
		Confidence        float64 `json:"end_of_turn_confidence"`
		Language          string  `json:"language"`
		ProviderRequestID string  `json:"provider_request_id"`
	}
	if err := json.Unmarshal(committed[0].Data, &final); err != nil {
		t.Fatalf("decode final: %v", err)
	}
	// The formatted twin wins. Seeing the rough text here would mean the
	// unformatted final leaked through, which is the exact defect that cost the
	// platform 2.0% -> 5.7% word error rate.
	if final.Text != "My name is Keanu Reeves." || !final.IsFinal || final.Confidence != 0.93 || final.Language != "en" || final.ProviderRequestID != "session-aai-1" {
		t.Fatalf("final = %+v", final)
	}

	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}
	// Exactly one more event: the Termination accounting. Anything else here
	// means a duplicate final escaped the de-duplication above.
	trailing := drainEvents(t, stream.Events())
	if len(trailing) != 1 || trailing[0].Type != protocol.EventUsageObserved {
		t.Fatalf("trailing events = %v", eventTypes(trailing))
	}
	var usage struct {
		AudioDurationMS   int64 `json:"audio_duration_ms"`
		SessionDurationMS int64 `json:"session_duration_ms"`
	}
	if err := json.Unmarshal(trailing[0].Data, &usage); err != nil || usage.AudioDurationMS != 2_500 || usage.SessionDurationMS != 3_250 {
		t.Fatalf("usage = %+v, err=%v", usage, err)
	}

	select {
	case received := <-requests:
		if got := received.Header.Get("Authorization"); got != "" {
			t.Fatalf("managed session sent an Authorization header %q; the temporary token belongs in the query string", got)
		}
		query := received.URL.Query()
		want := map[string]string{
			// speech_model, NOT model: the wrong name is ignored and the session
			// silently falls back to the account default.
			"speech_model": "universal-3-5-pro",
			"encoding":     "pcm_s16le",
			"sample_rate":  "16000",
			// Off by default at AssemblyAI; without it the committed final has no
			// punctuation, casing, or number formatting.
			"format_turns": "true",
			// Pins the realtime behaviour so a vendor default flip cannot turn
			// this into a commit-only transcriber.
			"include_partial_turns": "true",
			// language_codes (plural, JSON array). There is no `language`
			// parameter on this API, so the singular spelling every other vendor
			// uses would be accepted and ignored.
			"language_codes": `["en"]`,
			// The managed credential channel.
			"token": "temporary-assemblyai-token",
		}
		assertQuery(t, query, want)
	case <-time.After(time.Second):
		t.Fatal("server did not observe the handshake")
	}
}

// TestBYOKSessionUsesBareAuthorizationHeader covers the other credential
// source. A permanent customer key must ride the Authorization header with NO
// "Bearer" prefix (AssemblyAI takes the bare key), and must never appear in the
// URL, because a URL reaches proxy and access logs while a header does not.
func TestBYOKSessionUsesBareAuthorizationHeader(t *testing.T) {
	t.Parallel()
	const customerKey = "customer-assemblyai-key"
	requests := make(chan *http.Request, 1)
	server := newSTTServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		requests <- request.Clone(request.Context())
		if err := assertControl(ctx, conn, "Terminate"); err != nil {
			t.Errorf("terminate: %v", err)
			return
		}
		_ = writeJSONFrame(ctx, conn, map[string]any{"type": "Termination"})
		_, _, _ = conn.Read(ctx)
	})
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	request := sttRequest(server.URL, protocol.CredentialsBYOK)
	request.Plan.Route.Credential.Value = customerKey
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open BYOK: %v", err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close BYOK: %v", err)
	}

	select {
	case received := <-requests:
		if got := received.Header.Get("Authorization"); got != customerKey {
			t.Fatalf("Authorization = %q, want the bare key %q with no scheme prefix", got, customerKey)
		}
		if strings.Contains(received.URL.RawQuery, customerKey) {
			t.Fatalf("permanent key leaked into the query string: %q", received.URL.RawQuery)
		}
		if got := received.URL.Query().Get("token"); got != "" {
			t.Fatalf("BYOK session used the temporary-token channel: token=%q", got)
		}
		assertQuery(t, received.URL.Query(), map[string]string{
			"speech_model": "universal-3-5-pro", "encoding": "pcm_s16le", "sample_rate": "16000",
			"format_turns": "true", "include_partial_turns": "true", "language_codes": `["en"]`,
		})
	case <-time.After(time.Second):
		t.Fatal("server did not observe the BYOK handshake")
	}
}

// TestCredentialChannelIsKeyedByRoute pins where the credential lands for
// every route/source combination this adapter can be handed. The relay rows
// exist because a relay plan is managed for billing purposes but carries the
// connector's permanent AssemblyAI key, which belongs in the bare
// Authorization header exactly like a BYOK key — the `token` query channel
// would put a permanent key in the URL, where it could reach logs. The relay
// arm accepts the relay_access credential kind alongside bearer because
// protocol.SessionPlan validation and the plan-synthesizing relay connector
// spell the same permanent key differently.
func TestCredentialChannelIsKeyedByRoute(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name           string
		route          protocol.ProviderRoute
		source         protocol.CredentialSource
		kind           protocol.CredentialKind
		credential     string
		wantHeader     string
		wantQueryToken string
	}{
		{
			name:  "byok permanent key rides the bare header",
			route: protocol.RouteProviderDirect, source: protocol.CredentialsBYOK, kind: protocol.CredentialBearer,
			credential: "customer-assemblyai-key", wantHeader: "customer-assemblyai-key",
		},
		{
			name:  "managed temporary token rides the query",
			route: protocol.RouteProviderDirect, source: protocol.CredentialsManaged, kind: protocol.CredentialBearer,
			credential: "temporary-assemblyai-token", wantQueryToken: "temporary-assemblyai-token",
		},
		{
			name:  "relay permanent key rides the bare header",
			route: protocol.RouteSpekoRelay, source: protocol.CredentialsManaged, kind: protocol.CredentialBearer,
			credential: "connector-assemblyai-key", wantHeader: "connector-assemblyai-key",
		},
		{
			name:  "relay accepts the relay_access credential kind",
			route: protocol.RouteSpekoRelay, source: protocol.CredentialsManaged, kind: protocol.CredentialRelayAccess,
			credential: "connector-assemblyai-key", wantHeader: "connector-assemblyai-key",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			requests := make(chan *http.Request, 1)
			server := newSTTServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
				requests <- request.Clone(request.Context())
				if err := assertControl(ctx, conn, "Terminate"); err != nil {
					t.Errorf("terminate: %v", err)
					return
				}
				_ = writeJSONFrame(ctx, conn, map[string]any{"type": "Termination"})
				_, _, _ = conn.Read(ctx)
			})
			defer server.Close()

			adapter, err := New(testConfig(server.URL))
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			request := sttRequest(server.URL, testCase.source)
			request.Plan.Execution.ProviderRoute = testCase.route
			request.Plan.Route.Credential.Kind = testCase.kind
			request.Plan.Route.Credential.Value = testCase.credential
			stream, err := adapter.Open(context.Background(), request)
			if err != nil {
				t.Fatalf("open stream: %v", err)
			}
			if err := stream.Close(context.Background()); err != nil {
				t.Fatalf("close stream: %v", err)
			}

			select {
			case received := <-requests:
				if got := received.Header.Get("Authorization"); got != testCase.wantHeader {
					t.Fatalf("Authorization = %q, want %q", got, testCase.wantHeader)
				}
				if got := received.URL.Query().Get("token"); got != testCase.wantQueryToken {
					t.Fatalf("token query parameter = %q, want %q", got, testCase.wantQueryToken)
				}
				if testCase.wantQueryToken == "" && strings.Contains(received.URL.RawQuery, testCase.credential) {
					t.Fatalf("permanent key leaked into the query string: %q", received.URL.RawQuery)
				}
			case <-time.After(time.Second):
				t.Fatal("server did not observe the handshake")
			}
		})
	}
}

// TestOpenRejectsUnsupportedRequests keeps every refusal local. Each of these
// would otherwise reach AssemblyAI and come back as either a session-killing
// close code or, worse, a healthy session producing wrong text.
func TestOpenRejectsUnsupportedRequests(t *testing.T) {
	t.Parallel()
	server := newSTTServer(t, func(context.Context, *http.Request, *websocket.Conn) {})
	defer server.Close()
	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}

	cases := []struct {
		name    string
		mutate  func(*runtimepkg.AdapterRequest)
		wantErr string
	}{
		{
			name:    "wrong session kind",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Kind = protocol.SessionKindTTS },
			wantErr: "stt sessions",
		},
		{
			name:    "another provider's plan",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Provider = "deepgram" },
			wantErr: "cannot open provider",
		},
		{
			name:    "non-websocket transport",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Transport = protocol.TransportHTTP },
			wantErr: "websocket transport",
		},
		{
			name:    "missing credential",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential = nil },
			wantErr: "bearer credential",
		},
		{
			name:    "blank credential",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential.Value = "   " },
			wantErr: "bearer credential",
		},
		{
			// relay_access is a relay-route spelling only; a provider-direct
			// plan carrying it never came from the relay and must be refused.
			name:    "relay_access credential outside the relay",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential.Kind = protocol.CredentialRelayAccess },
			wantErr: "bearer credential",
		},
		{
			// "auto" is a routing placeholder. AssemblyAI would treat it as an
			// unknown speech_model and quietly serve the account default.
			name:    "unresolved model",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "auto" },
			wantErr: "concrete model",
		},
		{
			name:    "empty model",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "" },
			wantErr: "concrete model",
		},
		{
			// Portable media allows opus; AssemblyAI's audio requirements
			// consistently document PCM16/mu-law only, and a container the vendor
			// cannot decode produces a live session full of garbage, not an error.
			name:    "opus media",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Media.Encoding = "opus" },
			wantErr: "pcm_s16le",
		},
		{
			name:    "stereo media",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Media.Channels = 2 },
			wantErr: "mono audio",
		},
		{
			// The portable format allows up to 192 kHz; AssemblyAI stops at 96 kHz.
			name:    "sample rate above the vendor ceiling",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Media.SampleRateHz = 192_000 },
			wantErr: "sample rate must be between",
		},
		{
			name:    "missing media",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Media = nil },
			wantErr: "media configuration",
		},
		{
			// Not in AssemblyAI's streaming language_codes vocabulary. Passing it
			// through would be accepted-and-ignored, leaving the caller believing
			// the language was pinned.
			name:    "unsupported language",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Options.Language = "uz" },
			wantErr: "does not support language",
		},
		{
			name: "wrong endpoint path",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Plan.Route.Endpoint = strings.TrimSuffix(r.Plan.Route.Endpoint, endpointPath) + "/v2/realtime/ws"
			},
			wantErr: "endpoint path must be /v3/ws",
		},
		{
			name: "endpoint host outside the allowlist",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Plan.Route.Endpoint = "wss://streaming.example.com/v3/ws"
			},
			wantErr: "endpoint host is not allowed",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			request := sttRequest(server.URL, protocol.CredentialsManaged)
			testCase.mutate(&request)
			_, err := adapter.Open(context.Background(), request)
			if err == nil || !strings.Contains(err.Error(), testCase.wantErr) {
				t.Fatalf("error = %v, want it to mention %q", err, testCase.wantErr)
			}
		})
	}
}

// TestErrorFrameCarriesVendorClassification proves the documented Error frame
// becomes a classified ProviderError with the raw frame attached, rather than a
// generic transport failure.
func TestErrorFrameCarriesVendorClassification(t *testing.T) {
	t.Parallel()
	server := newSTTServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		_ = writeJSONFrame(ctx, conn, map[string]any{
			"type": "Error", "error_code": 1008, "error": "Unauthorized Connection: insufficient balance",
		})
		_, _, _ = conn.Read(ctx)
	})
	defer server.Close()
	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), sttRequest(server.URL, protocol.CredentialsManaged))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	select {
	case event := <-stream.Events():
		var providerErr *runtimepkg.ProviderError
		if !errors.As(event.Err, &providerErr) {
			t.Fatalf("event = %#v, want a provider error", event)
		}
		// An empty wallet, not a dead key: the two share close code 1008 and only
		// the reason text separates them.
		if providerErr.Code != "provider_quota_exceeded" || providerErr.Retryable {
			t.Fatalf("error = %+v, want a non-retryable provider_quota_exceeded", providerErr)
		}
		if providerErr.ProviderStatus != 1008 {
			t.Fatalf("provider status = %d, want the vendor close code 1008", providerErr.ProviderStatus)
		}
		if providerErr.Extensions[extensionID] == nil {
			t.Fatal("provider error dropped the raw AssemblyAI frame")
		}
		if !strings.Contains(providerErr.Message, "insufficient balance") {
			t.Fatalf("message = %q, want the vendor reason preserved", providerErr.Message)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the error event")
	}
}

// TestErrorClassSeparatesVendorFailureModes locks the mapping from AssemblyAI's
// close codes onto the gateway's stable vocabulary. The point of the table is
// that these must not collapse into one another: a dead key, an empty balance,
// a concurrency cap and a protocol fault demand four different responses, and
// only two of them are worth retrying.
func TestErrorClassSeparatesVendorFailureModes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name          string
		code          int
		detail        string
		wantCode      string
		wantRetryable bool
	}{
		{"missing or rejected key", closeUnauthorized, "Unauthorized Connection: Missing Authorization header", "authentication_failed", false},
		{"account out of funds shares code 1008", closeUnauthorized, "Unauthorized Connection: insufficient balance", "provider_quota_exceeded", false},
		{"concurrency cap frees up on its own", closeTooManySession, "Unauthorized Connection: Too many concurrent sessions", "provider_rate_limited", true},
		{"audio message outside the 50-1000 ms window", closeInputDuration, "Input duration violation: 15.0 ms. Expected between 50 and 1000 ms", "invalid_request", false},
		{"malformed control message", closeInvalidMessage, "Invalid Message Type: nope", "invalid_request", false},
		{"retired v2 endpoint", closeDeprecatedV2, "Deprecated endpoint", "invalid_request", false},
		{"session hit its maximum duration", closeSessionExpired, "Session Expired: Maximum session duration exceeded", "provider_unavailable", true},
		{"catch-all server fault", closeServerError, "Session Cancelled: An error occurred", "provider_unavailable", true},
		{"internal error during connect", closeInternalError, "Internal error", "provider_unavailable", true},
		{"codeless frame naming a key problem", 0, "Unauthorized: bad api key", "authentication_failed", false},
		{"codeless frame naming a violation", 0, "Input Duration Violation", "invalid_request", false},
		{"codeless frame with nothing to go on", 0, "something surprising", "provider_unavailable", false},
		{"unknown future code", 4999, "who knows", "provider_unavailable", false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			code, retryable := errorClass(testCase.code, testCase.detail)
			if code != testCase.wantCode || retryable != testCase.wantRetryable {
				t.Fatalf("errorClass(%d, %q) = (%q, %t), want (%q, %t)", testCase.code, testCase.detail, code, retryable, testCase.wantCode, testCase.wantRetryable)
			}
		})
	}
}

// TestAudioBelowTheVendorFloorIsNeverSent covers AssemblyAI's hardest
// constraint. A message under 50 ms closes the whole session with code 3007,
// so a sub-floor remainder is dropped rather than sent: losing 10 ms of
// trailing audio beats losing every transcript still in flight.
func TestAudioBelowTheVendorFloorIsNeverSent(t *testing.T) {
	t.Parallel()
	server := newSTTServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		// The FIRST thing on the wire must be the control frame. A binary frame
		// here means a 10 ms message escaped and the session would have died.
		if err := assertControl(ctx, conn, "Terminate"); err != nil {
			t.Errorf("expected no audio before terminate: %v", err)
			return
		}
		_ = writeJSONFrame(ctx, conn, map[string]any{"type": "Termination"})
		_, _, _ = conn.Read(ctx)
	})
	defer server.Close()
	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), sttRequest(server.URL, protocol.CredentialsManaged))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	// 320 bytes is one 10 ms frame at 16 kHz s16 mono — the size a live
	// telephony leg actually produces.
	if err := stream.WriteAudio(context.Background(), make([]byte, 320)); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}
	drainEvents(t, stream.Events())
}

// TestCloseForcesOnlyAPendingTurn covers the teardown asymmetry. With a turn in
// flight, Close must force it to commit or the caller's last utterance is lost;
// with nothing in flight, forcing one just manufactures an empty turn.
func TestCloseForcesOnlyAPendingTurn(t *testing.T) {
	t.Parallel()
	t.Run("pending turn is forced", func(t *testing.T) {
		t.Parallel()
		server := newSTTServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
			_ = writeJSONFrame(ctx, conn, map[string]any{"type": "Turn", "turn_order": 0, "transcript": "my name is", "end_of_turn": false})
			if err := assertControl(ctx, conn, "ForceEndpoint"); err != nil {
				t.Errorf("force endpoint: %v", err)
				return
			}
			if err := assertControl(ctx, conn, "Terminate"); err != nil {
				t.Errorf("terminate: %v", err)
				return
			}
			_ = writeJSONFrame(ctx, conn, map[string]any{"type": "Termination"})
			_, _, _ = conn.Read(ctx)
		})
		defer server.Close()
		stream := openTestStream(t, server)
		if got := collectEvents(t, stream.Events(), 1)[0].Type; got != protocol.EventTranscriptDelta {
			t.Fatalf("event = %q, want a partial", got)
		}
		if err := stream.Close(context.Background()); err != nil {
			t.Fatalf("close: %v", err)
		}
		drainEvents(t, stream.Events())
	})

	t.Run("settled turn is not forced", func(t *testing.T) {
		t.Parallel()
		server := newSTTServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
			_ = writeJSONFrame(ctx, conn, map[string]any{"type": "Turn", "turn_order": 0, "transcript": "All done.", "end_of_turn": true, "turn_is_formatted": true})
			// Terminate must be the next frame; a ForceEndpoint here would be a
			// spurious empty turn on a conversation that already committed.
			if err := assertControl(ctx, conn, "Terminate"); err != nil {
				t.Errorf("terminate: %v", err)
				return
			}
			_ = writeJSONFrame(ctx, conn, map[string]any{"type": "Termination"})
			_, _, _ = conn.Read(ctx)
		})
		defer server.Close()
		stream := openTestStream(t, server)
		events := collectEvents(t, stream.Events(), 2)
		if events[0].Type != protocol.EventTranscriptFinal || events[1].Type != protocol.EventSpeechEnded {
			t.Fatalf("events = %v", eventTypes(events))
		}
		if err := stream.Close(context.Background()); err != nil {
			t.Fatalf("close: %v", err)
		}
		drainEvents(t, stream.Events())
	})
}

// TestFinalWithoutTheFormattedFlagStillSurfaces guards the *bool. An absent
// turn_is_formatted is not the same as false: false promises a corrected twin
// is coming, absent promises nothing. Decoding the field into a plain bool
// makes both look identical and silently swallows the turn.
func TestFinalWithoutTheFormattedFlagStillSurfaces(t *testing.T) {
	t.Parallel()
	server := newSTTServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		_ = writeJSONFrame(ctx, conn, map[string]any{
			"type": "Turn", "turn_order": 0, "transcript": "no flag at all", "end_of_turn": true,
		})
		_, _, _ = conn.Read(ctx)
	})
	defer server.Close()
	stream := openTestStream(t, server)
	events := collectEvents(t, stream.Events(), 2)
	if events[0].Type != protocol.EventTranscriptFinal || events[1].Type != protocol.EventSpeechEnded {
		t.Fatalf("events = %v, want the final to be emitted", eventTypes(events))
	}
	var final struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(events[0].Data, &final); err != nil || final.Text != "no flag at all" {
		t.Fatalf("final = %+v, err=%v", final, err)
	}
	_ = stream.Cancel(context.Background())
}

// TestUnknownFrameBecomesAWarning keeps forward compatibility observable:
// Heartbeat, SpeakerRevision and anything AssemblyAI adds later must surface
// rather than vanish.
func TestUnknownFrameBecomesAWarning(t *testing.T) {
	t.Parallel()
	server := newSTTServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		_ = writeJSONFrame(ctx, conn, map[string]any{"type": "Heartbeat", "total_audio_received_ms": 1_000})
		_, _, _ = conn.Read(ctx)
	})
	defer server.Close()
	stream := openTestStream(t, server)
	event := collectEvents(t, stream.Events(), 1)[0]
	if event.Type != protocol.EventWarning {
		t.Fatalf("event = %q, want a warning", event.Type)
	}
	var warning struct {
		ProviderType string `json:"provider_type"`
	}
	if err := json.Unmarshal(event.Data, &warning); err != nil || warning.ProviderType != "Heartbeat" {
		t.Fatalf("warning = %+v, err=%v", warning, err)
	}
	_ = stream.Cancel(context.Background())
}

// TestAudioBytesClearsTheFloorAtAwkwardSampleRates checks the rounding
// direction. AssemblyAI enforces 50 ms exactly, so a rate where 50 ms is not a
// whole number of bytes must round UP; rounding down produces a 49.98 ms
// message that the vendor rejects and the session dies.
func TestAudioBytesClearsTheFloorAtAwkwardSampleRates(t *testing.T) {
	t.Parallel()
	for _, rate := range []int{8_000, 16_000, 22_050, 24_000, 44_100, 48_000, 96_000} {
		floor := audioBytes(rate, minAudioMS)
		batch := audioBytes(rate, batchAudioMS)
		if floor%bytesPerSample != 0 || batch%bytesPerSample != 0 {
			t.Fatalf("rate %d: byte counts must stay sample-aligned, got floor=%d batch=%d", rate, floor, batch)
		}
		// Duration of the computed buffer, in milliseconds.
		if floorMS := float64(floor) * 1_000 / float64(rate*bytesPerSample); floorMS < minAudioMS {
			t.Fatalf("rate %d: floor buffer is %.3f ms, under the vendor's %d ms minimum", rate, floorMS, minAudioMS)
		}
		if batchMS := float64(batch) * 1_000 / float64(rate*bytesPerSample); batchMS < minAudioMS || batchMS > maxAudioMS {
			t.Fatalf("rate %d: batch buffer is %.3f ms, outside the vendor's %d-%d ms window", rate, batchMS, minAudioMS, maxAudioMS)
		}
	}
}

// TestUnsupportedOperationsAreRefused documents that this is a transcription
// stream: it has no text input side.
func TestUnsupportedOperationsAreRefused(t *testing.T) {
	t.Parallel()
	server := newSTTServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		_, _, _ = conn.Read(ctx)
	})
	defer server.Close()
	stream := openTestStream(t, server)
	if err := stream.AppendText(context.Background(), "hello"); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Fatalf("append text = %v", err)
	}
	if err := stream.CommitText(context.Background()); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Fatalf("commit text = %v", err)
	}
	if err := stream.WriteAudio(context.Background(), nil); err == nil {
		t.Fatal("empty audio was accepted")
	}
	_ = stream.Cancel(context.Background())
	// After a cancel the socket is gone and writes must be refused rather than
	// panicking on a dead connection.
	if err := stream.WriteAudio(context.Background(), make([]byte, 3_200)); !errors.Is(err, runtimepkg.ErrSessionClosed) {
		t.Fatalf("write after cancel = %v", err)
	}
}

func newSTTServer(t *testing.T, callback func(context.Context, *http.Request, *websocket.Conn)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != endpointPath {
			http.NotFound(w, r)
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			defer conn.CloseNow()
			callback(context.Background(), r, conn)
		}()
		<-done
	}))
}

func openTestStream(t *testing.T, server *httptest.Server) runtimepkg.ProviderStream {
	t.Helper()
	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), sttRequest(server.URL, protocol.CredentialsManaged))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	return stream
}

func sttRequest(serverURL string, source protocol.CredentialSource) runtimepkg.AdapterRequest {
	now := time.Date(2026, time.August, 7, 12, 0, 0, 0, time.UTC)
	endpoint, _ := url.Parse(serverURL)
	endpoint.Scheme = "ws"
	endpoint.Path = endpointPath
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindSTT,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{Placement: protocol.PlacementEmbedded, ProviderRoute: protocol.RouteProviderDirect, CredentialSource: source},
			Route: protocol.PlanRoute{
				Provider: "assemblyai", Model: DefaultModel, Adapter: AdapterID,
				Transport: protocol.TransportWebSocket, Endpoint: endpoint.String(),
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "temporary-assemblyai-token", ExpiresAt: now.Add(10 * time.Minute)},
			},
		},
		Options: protocol.RequestOptions{Language: "en"},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
	}
}

func testConfig(serverURL string) Config {
	endpoint, _ := url.Parse(serverURL)
	return Config{AllowedEndpointHosts: []string{endpoint.Hostname()}, AllowInsecureEndpoint: true}
}

// assertQuery checks both directions: every expected parameter is present with
// the expected value, and nothing else was sent. The second half matters
// because an extra parameter is invisible on the wire (AssemblyAI ignores what
// it does not recognize) and would drift unnoticed.
func assertQuery(t *testing.T, query url.Values, want map[string]string) {
	t.Helper()
	for key, expected := range want {
		if got := query.Get(key); got != expected {
			t.Fatalf("query %s = %q, want %q", key, got, expected)
		}
	}
	if len(query) != len(want) {
		t.Fatalf("query = %v, want exactly the %d documented parameters %v", query, len(want), want)
	}
}

func assertBinary(ctx context.Context, conn *websocket.Conn, wantBytes int) error {
	messageType, payload, err := conn.Read(ctx)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}
	if messageType != websocket.MessageBinary {
		return fmt.Errorf("message type = %v, want binary (payload %q)", messageType, payload)
	}
	if len(payload) != wantBytes {
		return fmt.Errorf("audio message = %d bytes, want %d", len(payload), wantBytes)
	}
	return nil
}

func assertControl(ctx context.Context, conn *websocket.Conn, wantType string) error {
	messageType, payload, err := conn.Read(ctx)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}
	if messageType != websocket.MessageText {
		return fmt.Errorf("message type = %v, want text (%d bytes)", messageType, len(payload))
	}
	var control struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(payload, &control); err != nil {
		return fmt.Errorf("decode %q: %w", payload, err)
	}
	if control.Type != wantType {
		return fmt.Errorf("control type = %q, want %q", control.Type, wantType)
	}
	return nil
}

func writeJSONFrame(ctx context.Context, conn *websocket.Conn, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, payload)
}

func collectEvents(t *testing.T, events <-chan runtimepkg.ProviderEvent, count int) []runtimepkg.ProviderEvent {
	t.Helper()
	result := make([]runtimepkg.ProviderEvent, 0, count)
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for len(result) < count {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatalf("events closed after %d events: %v", len(result), eventTypes(result))
			}
			if event.Err != nil {
				t.Fatalf("provider event error: %v", event.Err)
			}
			result = append(result, event)
		case <-timer.C:
			t.Fatalf("timed out after %d events: %v", len(result), eventTypes(result))
		}
	}
	return result
}

func drainEvents(t *testing.T, events <-chan runtimepkg.ProviderEvent) []runtimepkg.ProviderEvent {
	t.Helper()
	var result []runtimepkg.ProviderEvent
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-events:
			if !ok {
				return result
			}
			if event.Err != nil {
				t.Fatalf("provider event error: %v", event.Err)
			}
			result = append(result, event)
		case <-timer.C:
			t.Fatalf("events never closed; collected %v", eventTypes(result))
		}
	}
}

func eventTypes(events []runtimepkg.ProviderEvent) []protocol.EventType {
	types := make([]protocol.EventType, 0, len(events))
	for _, event := range events {
		types = append(types, event.Type)
	}
	return types
}
