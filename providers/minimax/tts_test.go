package minimax

import (
	"context"
	"encoding/hex"
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

// Every MiniMax wire value in this file — event names, the endpoint path, the
// model id, audio_setting tokens — is written as a STRING LITERAL rather than
// the package constant that produces it. Reusing the constant would make the
// test assert the adapter against itself: renaming task_continue to something
// MiniMax has never heard of would keep the suite green while every real
// session failed. The literals here are transcribed from the published
// AsyncAPI source and are the actual contract under test.
//
// The two PCM payloads are deliberately not valid UTF-8 and include a 0x00
// byte, so a test can only pass if the adapter emitted DECODED bytes rather
// than the hex string it received.
var (
	firstPCM  = []byte{0x00, 0x01, 0xfe, 0xff}
	secondPCM = []byte{0x7f, 0x80, 0x10, 0x20}
)

// TestAdapterHandshakeRequestShapeAndDecodedStreamingAudio is the wire-contract
// test: it pins the exact frames the adapter sends to MiniMax, the credential
// channel, the tenant query parameter, the decoded audio bytes, and the event
// ordering for one complete utterance.
func TestAdapterHandshakeRequestShapeAndDecodedStreamingAudio(t *testing.T) {
	t.Parallel()

	handshakes := make(chan *http.Request, 1)
	starts := make(chan taskStart, 1)
	continues := make(chan taskContinue, 1)
	finishes := make(chan string, 1)

	server := newTTSServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		handshakes <- request.Clone(request.Context())
		// The documented open sequence: the server greets, the client starts.
		if err := writeJSON(ctx, conn, map[string]any{"event": "connected_success", "trace_id": "trace_handshake"}); err != nil {
			t.Errorf("connected_success: %v", err)
			return
		}
		var start taskStart
		if err := readFrame(ctx, conn, &start); err != nil {
			t.Errorf("task_start: %v", err)
			return
		}
		starts <- start
		if err := writeJSON(ctx, conn, map[string]any{"event": "task_started"}); err != nil {
			t.Errorf("task_started: %v", err)
			return
		}
		var cont taskContinue
		if err := readFrame(ctx, conn, &cont); err != nil {
			t.Errorf("task_continue: %v", err)
			return
		}
		continues <- cont
		// Two separate audio frames prove the adapter forwards chunks as they
		// arrive rather than concatenating one buffer at the end.
		if err := writeJSON(ctx, conn, audioFrame(firstPCM, "trace_abc")); err != nil {
			t.Errorf("first audio: %v", err)
			return
		}
		if err := writeJSON(ctx, conn, audioFrame(secondPCM, "trace_abc")); err != nil {
			t.Errorf("second audio: %v", err)
			return
		}
		var finish struct {
			Event string `json:"event"`
		}
		if err := readFrame(ctx, conn, &finish); err != nil {
			t.Errorf("task_finish: %v", err)
			return
		}
		finishes <- finish.Event
		if err := writeJSON(ctx, conn, map[string]any{"event": "task_finished", "trace_id": "trace_abc"}); err != nil {
			t.Errorf("task_finished: %v", err)
			return
		}
		waitForClientClose(ctx, conn)
	})
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), adapterRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.AppendText(context.Background(), "Hello there"); err != nil {
		t.Fatalf("append text: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit text: %v", err)
	}

	events := collectEvents(t, stream.Events(), 5)
	// usage.observed carries the provider correlation id; the audio triplet is
	// the canonical TTS shape: started once, a frame per chunk, then done.
	if got := strings.Join(eventTypes(events), ","); got != "usage.observed,audio.started,audio.frame,audio.frame,audio.done" {
		t.Fatalf("event types = %s", got)
	}
	// The whole point of the hex format: these must be real PCM bytes.
	if string(events[2].Audio) != string(firstPCM) {
		t.Fatalf("first frame audio = %v, want %v", events[2].Audio, firstPCM)
	}
	if string(events[3].Audio) != string(secondPCM) {
		t.Fatalf("second frame audio = %v, want %v", events[3].Audio, secondPCM)
	}
	// A frame must never carry the encoded string it arrived as.
	if strings.Contains(string(events[2].Audio), hex.EncodeToString(firstPCM)) {
		t.Fatal("adapter emitted hex text instead of decoded audio")
	}
	if events[2].Extensions[extensionID] == nil {
		t.Fatal("audio frame must retain the raw MiniMax payload as an extension")
	}
	var usage struct {
		ProviderRequestID string `json:"provider_request_id"`
	}
	if err := json.Unmarshal(events[0].Data, &usage); err != nil || usage.ProviderRequestID != "trace_abc" {
		t.Fatalf("usage correlation = %+v, err=%v", usage, err)
	}

	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}
	if _, ok := <-stream.Events(); ok {
		t.Fatal("events must close after a graceful websocket close")
	}

	// Credential channel: MiniMax has no query-parameter auth and no minted
	// token, so the key must be the Authorization bearer header and must never
	// appear in the URL.
	select {
	case request := <-handshakes:
		if got := request.Header.Get("Authorization"); got != "Bearer customer-minimax-key" {
			t.Fatalf("Authorization = %q", got)
		}
		if request.URL.Query().Get("GroupId") != "" {
			t.Fatalf("a bare key must not send GroupId, query = %v", request.URL.Query())
		}
		if strings.Contains(request.URL.RawQuery, "customer-minimax-key") {
			t.Fatalf("credential leaked into the URL: %q", request.URL.RawQuery)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not observe the websocket handshake")
	}

	// task_start carries the stable generation settings exactly once.
	select {
	case start := <-starts:
		if start.Event != "task_start" {
			t.Fatalf("task_start event = %q", start.Event)
		}
		if start.Model != "speech-2.8-hd" {
			t.Fatalf("model = %q", start.Model)
		}
		if start.VoiceSetting.VoiceID != "English_Graceful_Lady" {
			t.Fatalf("voice_id = %q", start.VoiceSetting.VoiceID)
		}
		if start.VoiceSetting.Speed != 1.0 || start.VoiceSetting.Vol != 1.0 || start.VoiceSetting.Pitch != 0 {
			t.Fatalf("voice_setting defaults = %+v", start.VoiceSetting)
		}
		// format must be the documented "pcm" token and the rate must be echoed
		// from the negotiated media, not a hardcoded default.
		if start.AudioSetting.Format != "pcm" || start.AudioSetting.SampleRate != 24_000 || start.AudioSetting.Channel != 1 {
			t.Fatalf("audio_setting = %+v", start.AudioSetting)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not receive task_start")
	}

	// Text travels in task_continue, unmodified.
	select {
	case cont := <-continues:
		if cont.Event != "task_continue" || cont.Text != "Hello there" {
			t.Fatalf("task_continue = %+v", cont)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not receive task_continue")
	}

	select {
	case event := <-finishes:
		if event != "task_finish" {
			t.Fatalf("commit sent %q, want task_finish", event)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not receive task_finish")
	}
}

// TestAdapterEmitsAudioBeforeTheUtteranceCompletes proves the adapter streams
// rather than buffers: the second chunk is only sent after the test has already
// received the first as an audio.frame. If the adapter waited for task_finished
// before emitting, this test would deadlock and time out.
func TestAdapterEmitsAudioBeforeTheUtteranceCompletes(t *testing.T) {
	t.Parallel()

	firstDelivered := make(chan struct{})
	server := newTTSServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		if err := openTask(ctx, conn); err != nil {
			t.Errorf("handshake: %v", err)
			return
		}
		var cont taskContinue
		if err := readFrame(ctx, conn, &cont); err != nil {
			t.Errorf("task_continue: %v", err)
			return
		}
		if err := writeJSON(ctx, conn, audioFrame(firstPCM, "trace_stream")); err != nil {
			t.Errorf("first audio: %v", err)
			return
		}
		// Gate: nothing else is sent until the client has surfaced frame one.
		select {
		case <-firstDelivered:
		case <-time.After(2 * time.Second):
			t.Error("client never surfaced the first audio frame")
			return
		}
		if err := writeJSON(ctx, conn, audioFrame(secondPCM, "trace_stream")); err != nil {
			t.Errorf("second audio: %v", err)
			return
		}
		var finish struct {
			Event string `json:"event"`
		}
		if err := readFrame(ctx, conn, &finish); err != nil {
			t.Errorf("task_finish: %v", err)
			return
		}
		if err := writeJSON(ctx, conn, map[string]any{"event": "task_finished"}); err != nil {
			t.Errorf("task_finished: %v", err)
			return
		}
		waitForClientClose(ctx, conn)
	})
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), adapterRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.AppendText(context.Background(), "streaming please"); err != nil {
		t.Fatalf("append text: %v", err)
	}

	// usage.observed, audio.started, audio.frame — all before commit.
	early := collectEvents(t, stream.Events(), 3)
	if got := strings.Join(eventTypes(early), ","); got != "usage.observed,audio.started,audio.frame" {
		t.Fatalf("early event types = %s", got)
	}
	if string(early[2].Audio) != string(firstPCM) {
		t.Fatalf("first frame audio = %v", early[2].Audio)
	}
	close(firstDelivered)

	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit text: %v", err)
	}
	rest := collectEvents(t, stream.Events(), 2)
	if got := strings.Join(eventTypes(rest), ","); got != "audio.frame,audio.done" {
		t.Fatalf("remaining event types = %s", got)
	}
	if string(rest[0].Audio) != string(secondPCM) {
		t.Fatalf("second frame audio = %v", rest[0].Audio)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}
}

// TestAdapterToleratesNullDataAndFinalFlag covers two documented response
// shapes that a naive decoder gets wrong. `data` is explicitly nullable, and
// task_finished carries NO audio field at all — so an empty or absent payload
// must be skipped quietly rather than becoming an empty audio frame or a
// decode error. This is also the WebSocket counterpart to the HTTP API's
// aggregated-chunk quirk: there is no duplicate clip to filter here.
func TestAdapterToleratesNullDataAndFinalFlag(t *testing.T) {
	t.Parallel()

	server := newTTSServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		if err := openTask(ctx, conn); err != nil {
			t.Errorf("handshake: %v", err)
			return
		}
		var cont taskContinue
		if err := readFrame(ctx, conn, &cont); err != nil {
			t.Errorf("task_continue: %v", err)
			return
		}
		// A keep-alive style frame whose data is null.
		if err := writeJSON(ctx, conn, map[string]any{"event": "task_continued", "trace_id": "trace_null", "data": nil}); err != nil {
			t.Errorf("null data frame: %v", err)
			return
		}
		if err := writeJSON(ctx, conn, audioFrame(firstPCM, "trace_null")); err != nil {
			t.Errorf("audio: %v", err)
			return
		}
		// The documented final content frame still carries real audio.
		final := audioFrame(secondPCM, "trace_null")
		final["is_final"] = true
		if err := writeJSON(ctx, conn, final); err != nil {
			t.Errorf("final audio: %v", err)
			return
		}
		var finish struct {
			Event string `json:"event"`
		}
		if err := readFrame(ctx, conn, &finish); err != nil {
			t.Errorf("task_finish: %v", err)
			return
		}
		// task_finished has no data/audio member at all.
		if err := writeJSON(ctx, conn, map[string]any{
			"event": "task_finished", "trace_id": "trace_null",
			"base_resp": map[string]any{"status_code": 0, "status_msg": "success"},
		}); err != nil {
			t.Errorf("task_finished: %v", err)
			return
		}
		waitForClientClose(ctx, conn)
	})
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), adapterRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.AppendText(context.Background(), "nulls are fine"); err != nil {
		t.Fatalf("append text: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit text: %v", err)
	}
	// The null frame must contribute no event; a base_resp of 0 is not an error.
	events := collectEvents(t, stream.Events(), 5)
	if got := strings.Join(eventTypes(events), ","); got != "usage.observed,audio.started,audio.frame,audio.frame,audio.done" {
		t.Fatalf("event types = %s", got)
	}
	if string(events[2].Audio) != string(firstPCM) || string(events[3].Audio) != string(secondPCM) {
		t.Fatalf("audio frames = %v / %v", events[2].Audio, events[3].Audio)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}
}

// TestAdapterMapsInBandBaseRespErrors covers MiniMax's defining error quirk:
// request-level failures arrive in a base_resp on an otherwise successful frame,
// so a transport-only view of health would report the session as fine.
func TestAdapterMapsInBandBaseRespErrors(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name      string
		status    int
		message   string
		wantCode  string
		wantRetry bool
	}{
		{"invalid api key", 2049, "invalid API Key", "authentication_failed", false},
		{"not authorized", 1004, "not authorized", "authentication_failed", false},
		{"rate limit", 1002, "rate limit", "provider_rate_limited", true},
		{"token limit", 1039, "token limit", "provider_rate_limited", true},
		{"internal error", 1024, "internal error", "provider_unavailable", true},
		// 2054 is undocumented but is what a bad voice id actually returns.
		{"voice id not exist", 2054, "voice id not exist", "invalid_request", false},
		{"insufficient balance", 1008, "insufficient balance", "invalid_request", false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			server := newTTSServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
				if err := openTask(ctx, conn); err != nil {
					t.Errorf("handshake: %v", err)
					return
				}
				var cont taskContinue
				if err := readFrame(ctx, conn, &cont); err != nil {
					t.Errorf("task_continue: %v", err)
					return
				}
				// HTTP/WS transport is healthy; the failure is in the body.
				if err := writeJSON(ctx, conn, map[string]any{
					"event":     "task_failed",
					"base_resp": map[string]any{"status_code": testCase.status, "status_msg": testCase.message},
				}); err != nil {
					t.Errorf("task_failed: %v", err)
					return
				}
				waitForClientClose(ctx, conn)
			})
			defer server.Close()

			adapter, err := New(testConfig(server.URL))
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			stream, err := adapter.Open(context.Background(), adapterRequest(server.URL))
			if err != nil {
				t.Fatalf("open stream: %v", err)
			}
			if err := stream.AppendText(context.Background(), "boom"); err != nil {
				t.Fatalf("append text: %v", err)
			}
			providerErr := awaitError(t, stream.Events())
			if providerErr.Code != testCase.wantCode {
				t.Fatalf("code = %q, want %q", providerErr.Code, testCase.wantCode)
			}
			if providerErr.Retryable != testCase.wantRetry {
				t.Fatalf("retryable = %v, want %v", providerErr.Retryable, testCase.wantRetry)
			}
			// The vendor's own words must survive so the cause is diagnosable.
			if !strings.Contains(providerErr.Message, fmt.Sprint(testCase.status)) || !strings.Contains(providerErr.Message, testCase.message) {
				t.Fatalf("message = %q", providerErr.Message)
			}
			if providerErr.Extensions[extensionID] == nil {
				t.Fatal("error must retain the raw MiniMax payload")
			}
			_ = stream.Close(context.Background())
		})
	}
}

// TestAdapterMapsHandshakeStatusCodes pins the HTTP status taxonomy for a
// rejected upgrade, including which failures the runtime may retry.
func TestAdapterMapsHandshakeStatusCodes(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		status    int
		wantCode  string
		wantRetry bool
	}{
		{http.StatusUnauthorized, "authentication_failed", false},
		{http.StatusForbidden, "authentication_failed", false},
		{http.StatusTooManyRequests, "provider_rate_limited", true},
		{http.StatusInternalServerError, "provider_unavailable", true},
		{http.StatusBadRequest, "invalid_request", false},
	} {
		t.Run(fmt.Sprint(testCase.status), func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "rejected", testCase.status)
			}))
			defer server.Close()

			adapter, err := New(testConfig(server.URL))
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			_, err = adapter.Open(context.Background(), adapterRequest(server.URL))
			var providerErr *runtimepkg.ProviderError
			if !errors.As(err, &providerErr) {
				t.Fatalf("open error = %v, want *runtimepkg.ProviderError", err)
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
			// A failure message must never carry the customer's key.
			if strings.Contains(providerErr.Message, "customer-minimax-key") {
				t.Fatal("credential leaked into the provider error")
			}
		})
	}
}

// TestAdapterCancelStopsTheTaskAndDropsLateAudio covers barge-in. MiniMax has no
// interrupt frame, so cancellation must be two-sided: task_finish upstream and a
// local drop of anything still in flight. A cancelled utterance must NOT report
// audio.done, which would tell the caller it played to the end.
func TestAdapterCancelStopsTheTaskAndDropsLateAudio(t *testing.T) {
	t.Parallel()

	finishes := make(chan string, 1)
	server := newTTSServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		if err := openTask(ctx, conn); err != nil {
			t.Errorf("handshake: %v", err)
			return
		}
		var cont taskContinue
		if err := readFrame(ctx, conn, &cont); err != nil {
			t.Errorf("task_continue: %v", err)
			return
		}
		if err := writeJSON(ctx, conn, audioFrame(firstPCM, "trace_cancel")); err != nil {
			t.Errorf("first audio: %v", err)
			return
		}
		var finish struct {
			Event string `json:"event"`
		}
		if err := readFrame(ctx, conn, &finish); err != nil {
			t.Errorf("task_finish: %v", err)
			return
		}
		finishes <- finish.Event
		// Audio already generated keeps arriving after the cancel; the adapter
		// must swallow it rather than play over the user.
		if err := writeJSON(ctx, conn, audioFrame(secondPCM, "trace_cancel")); err != nil {
			t.Errorf("late audio: %v", err)
			return
		}
		if err := writeJSON(ctx, conn, map[string]any{"event": "task_finished"}); err != nil {
			t.Errorf("task_finished: %v", err)
			return
		}
		waitForClientClose(ctx, conn)
	})
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), adapterRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.AppendText(context.Background(), "interrupt me"); err != nil {
		t.Fatalf("append text: %v", err)
	}
	events := collectEvents(t, stream.Events(), 3)
	if got := strings.Join(eventTypes(events), ","); got != "usage.observed,audio.started,audio.frame" {
		t.Fatalf("pre-cancel event types = %s", got)
	}
	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	select {
	case event := <-finishes:
		if event != "task_finish" {
			t.Fatalf("cancel sent %q, want task_finish", event)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not tell MiniMax to stop the task")
	}
	// Nothing more may reach the caller: not the late frame, not audio.done.
	select {
	case event, ok := <-stream.Events():
		if ok {
			t.Fatalf("cancelled utterance emitted %q", event.Type)
		}
	case <-time.After(250 * time.Millisecond):
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}
}

// TestAdapterAbortsInFlightSynthesis checks that Abort tears the socket down
// immediately and closes the event channel instead of hanging on a provider
// that is still streaming.
func TestAdapterAbortsInFlightSynthesis(t *testing.T) {
	t.Parallel()

	server := newTTSServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		if err := openTask(ctx, conn); err != nil {
			return
		}
		var cont taskContinue
		if err := readFrame(ctx, conn, &cont); err != nil {
			return
		}
		// Keep streaming until the client disappears.
		for {
			if err := writeJSON(ctx, conn, audioFrame(firstPCM, "trace_abort")); err != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	})
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), adapterRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.AppendText(context.Background(), "abort me"); err != nil {
		t.Fatalf("append text: %v", err)
	}
	aborter, ok := stream.(runtimepkg.AbortingProviderStream)
	if !ok {
		t.Fatal("stream must implement AbortingProviderStream")
	}
	_ = aborter.Abort(context.Background())

	// Draining must terminate; a leaked read loop would hang here.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, open := <-stream.Events():
			if !open {
				return
			}
		case <-deadline:
			t.Fatal("event channel did not close after Abort")
		}
	}
}

// TestAdapterUsesTheBearerHeaderForBothCredentialSources locks in the credential
// decision. MiniMax publishes no minted/ephemeral token and no query-parameter
// auth, so unlike ElevenLabs and Cartesia there is no managed-vs-BYOK split:
// both must use the Authorization header, and neither may put the secret in the
// URL where it would be logged.
func TestAdapterUsesTheBearerHeaderForBothCredentialSources(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name   string
		source protocol.CredentialSource
		value  string
	}{
		{"byok", protocol.CredentialsBYOK, "customer-permanent-key"},
		{"managed", protocol.CredentialsManaged, "control-plane-delegated-key"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			handshakes := make(chan *http.Request, 1)
			server := newTTSServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
				handshakes <- request.Clone(request.Context())
				_ = openTask(ctx, conn)
				waitForClientClose(ctx, conn)
			})
			defer server.Close()

			adapter, err := New(testConfig(server.URL))
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			request := adapterRequest(server.URL)
			request.Plan.Execution.CredentialSource = testCase.source
			request.Plan.Route.Credential.Value = testCase.value
			stream, err := adapter.Open(context.Background(), request)
			if err != nil {
				t.Fatalf("open stream: %v", err)
			}
			_ = stream.Close(context.Background())

			select {
			case received := <-handshakes:
				if got := received.Header.Get("Authorization"); got != "Bearer "+testCase.value {
					t.Fatalf("Authorization = %q", got)
				}
				if strings.Contains(received.URL.RawQuery, testCase.value) {
					t.Fatalf("credential leaked into the query string: %q", received.URL.RawQuery)
				}
				if received.URL.Query().Get("token") != "" || received.URL.Query().Get("access_token") != "" {
					t.Fatalf("unexpected query-parameter auth: %v", received.URL.Query())
				}
			case <-time.After(time.Second):
				t.Fatal("server did not observe the handshake")
			}
		})
	}
}

// TestAdapterUsesTheBearerHeaderOnTheRelayRoute pins the relay arm. A relay
// plan carries the connector's permanent key through the same Authorization
// header as every other source — MiniMax has no other channel — and the arm
// must accept both credential spellings: protocol.SessionPlan validation
// requires relay plans to label the credential relay_access, while a
// connector that synthesizes the plan and drives the adapter directly labels
// the same permanent key bearer.
func TestAdapterUsesTheBearerHeaderOnTheRelayRoute(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name string
		kind protocol.CredentialKind
	}{
		{"bearer", protocol.CredentialBearer},
		{"relay_access", protocol.CredentialRelayAccess},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			handshakes := make(chan *http.Request, 1)
			server := newTTSServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
				handshakes <- request.Clone(request.Context())
				_ = openTask(ctx, conn)
				waitForClientClose(ctx, conn)
			})
			defer server.Close()

			adapter, err := New(testConfig(server.URL))
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			request := adapterRequest(server.URL)
			request.Plan.Execution.ProviderRoute = protocol.RouteSpekoRelay
			request.Plan.Execution.CredentialSource = protocol.CredentialsManaged
			request.Plan.Route.Credential.Kind = testCase.kind
			request.Plan.Route.Credential.Value = "connector-minimax-key"
			stream, err := adapter.Open(context.Background(), request)
			if err != nil {
				t.Fatalf("open relay stream: %v", err)
			}
			_ = stream.Close(context.Background())

			select {
			case received := <-handshakes:
				if got := received.Header.Get("Authorization"); got != "Bearer connector-minimax-key" {
					t.Fatalf("Authorization = %q", got)
				}
				if strings.Contains(received.URL.RawQuery, "connector-minimax-key") {
					t.Fatalf("credential leaked into the query string: %q", received.URL.RawQuery)
				}
			case <-time.After(time.Second):
				t.Fatal("server did not observe the relay handshake")
			}
		})
	}
}

// TestAdapterUnwrapsAPackedCredentialAndSendsNoQueryParameters covers the
// tenant question. The current T2A references contain no GroupId or any other
// query parameter, so the handshake URL must stay exactly as the plan pinned
// it. A packed credential envelope must still be unwrapped, because sending
// the raw JSON blob as the bearer token would fail auth confusingly.
func TestAdapterUnwrapsAPackedCredentialAndSendsNoQueryParameters(t *testing.T) {
	t.Parallel()

	handshakes := make(chan *http.Request, 1)
	server := newTTSServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		handshakes <- request.Clone(request.Context())
		_ = openTask(ctx, conn)
		waitForClientClose(ctx, conn)
	})
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	request := adapterRequest(server.URL)
	request.Plan.Route.Credential.Value = `{"apiKey":"legacy-key","groupId":"1899"}`
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	_ = stream.Close(context.Background())

	select {
	case received := <-handshakes:
		if got := received.Header.Get("Authorization"); got != "Bearer legacy-key" {
			t.Fatalf("Authorization = %q (the envelope must be unwrapped)", got)
		}
		// No undocumented parameters may ride along on the handshake.
		if received.URL.RawQuery != "" {
			t.Fatalf("handshake query must be empty, got %q", received.URL.RawQuery)
		}
		if received.URL.Path != "/ws/v1/t2a_v2" {
			t.Fatalf("handshake path = %q", received.URL.Path)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not observe the handshake")
	}
}

// TestAdapterRejectsUnusableRequests keeps a malformed plan from ever reaching
// MiniMax with a customer credential attached.
func TestAdapterRejectsUnusableRequests(t *testing.T) {
	t.Parallel()

	const secret = "secret-that-must-not-leak"
	for _, testCase := range []struct {
		name    string
		mutate  func(*runtimepkg.AdapterRequest)
		wantSub string
	}{
		{
			name:    "wrong session kind",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Kind = protocol.SessionKindSTT },
			wantSub: "tts sessions",
		},
		{
			name:    "wrong provider",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Provider = "elevenlabs" },
			wantSub: "cannot open provider",
		},
		{
			// The plan must not be able to route a websocket adapter over HTTP.
			name:    "wrong transport",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Transport = protocol.TransportHTTP },
			wantSub: "websocket transport",
		},
		{
			// "auto" is a routing placeholder; only the control plane resolves it.
			name:    "unresolved auto model",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "auto" },
			wantSub: "concrete model",
		},
		{
			name:    "model outside the published lineup",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "speech-9.9-hd" },
			wantSub: "does not support model",
		},
		{
			// voice_id is required by task_start; omitting it fails upstream.
			name:    "missing voice",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Options.Voice = "" },
			wantSub: "voice id",
		},
		{
			name:    "missing credential",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential = nil },
			wantSub: "bearer credential",
		},
		{
			// A relay-access credential cannot authenticate a provider-direct call.
			name: "wrong credential kind",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Plan.Route.Credential.Kind = protocol.CredentialRelayAccess
			},
			wantSub: "bearer credential",
		},
		{
			name:    "missing media",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Media = nil },
			wantSub: "media configuration",
		},
		{
			// MiniMax's audio_setting accepts a fixed rate set; 48k is not in it,
			// and sending it would yield audio at the wrong rate.
			name:    "sample rate outside the accepted set",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Media.SampleRateHz = 48_000 },
			wantSub: "sample rate",
		},
		{
			name:    "non-pcm encoding",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Media.Encoding = "opus" },
			wantSub: "pcm_s16le",
		},
		{
			name:    "endpoint path outside the websocket API",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = "ws://127.0.0.1:1/v1/t2a_v2" },
			wantSub: "endpoint path",
		},
		{
			// Host pinning: a plan must not aim a customer key at another host.
			name:    "endpoint host outside the allowlist",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = "ws://evil.test/ws/v1/t2a_v2" },
			wantSub: "endpoint",
		},
		{
			name: "malformed credential envelope",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Plan.Route.Credential.Value = `{"groupId":"123"}`
			},
			wantSub: "apiKey",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			adapter, err := New(Config{AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			request := adapterRequest("http://127.0.0.1:1")
			if request.Plan.Route.Credential != nil {
				request.Plan.Route.Credential.Value = secret
			}
			testCase.mutate(&request)
			_, err = adapter.Open(context.Background(), request)
			if err == nil {
				t.Fatal("expected the request to be rejected before dialing")
			}
			if !strings.Contains(err.Error(), testCase.wantSub) {
				t.Fatalf("error = %v, want it to mention %q", err, testCase.wantSub)
			}
			// Validation errors are logged; they must never carry the key.
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("credential leaked into a validation error: %v", err)
			}
		})
	}
}

// TestStreamRejectsInboundAudioOperations documents that a TTS session cannot
// accept audio: silently ignoring it would strand a caller's buffer.
func TestStreamRejectsInboundAudioOperations(t *testing.T) {
	t.Parallel()

	subject := &stream{}
	if err := subject.WriteAudio(context.Background(), []byte{1}); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Fatalf("WriteAudio = %v", err)
	}
	if err := subject.CommitAudio(context.Background()); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Fatalf("CommitAudio = %v", err)
	}
}

// TestDecodeHexAudioTolueratesAnOddNibble: MiniMax emits unseparated lowercase
// hex. A truncated trailing nibble cannot form a byte, and dropping it beats
// discarding an otherwise good chunk.
func TestDecodeHexAudioToleratesAnOddNibble(t *testing.T) {
	t.Parallel()

	decoded, err := decodeHexAudio("0001feff" + "a")
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(decoded) != string(firstPCM) {
		t.Fatalf("decoded = %v, want %v", decoded, firstPCM)
	}
	if _, err := decodeHexAudio("zz"); err == nil {
		t.Fatal("non-hex audio must be reported, not silently dropped")
	}
}

// TestAppendTextRejectsOversizedInput guards the documented 10,000-character
// ceiling locally so the customer is not billed for a request MiniMax rejects.
func TestAppendTextRejectsOversizedInput(t *testing.T) {
	t.Parallel()

	subject := &stream{}
	err := subject.acceptCharacters(minimaxTTSMaxCharacters + 1)
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "input_too_large" {
		t.Fatalf("oversized input error = %v", err)
	}
	if providerErr.ProviderStatus != http.StatusRequestEntityTooLarge {
		t.Fatalf("provider status = %d", providerErr.ProviderStatus)
	}
}

// ---- helpers ----

// audioFrame builds a task_continued frame the way MiniMax documents it: the
// PCM is HEX-encoded inside data.audio. Hex and base64 are trivially conflated,
// and the assertions compare DECODED bytes so a base64 implementation could not
// pass. The frame shape mirrors the published schema exactly, which has no
// status field — that belongs to the HTTP SSE API, not this one.
func audioFrame(pcm []byte, traceID string) map[string]any {
	return map[string]any{
		"event":    "task_continued",
		"trace_id": traceID,
		"is_final": false,
		"data":     map[string]any{"audio": hex.EncodeToString(pcm)},
	}
}

// openTask performs the server side of the documented opening handshake.
func openTask(ctx context.Context, conn *websocket.Conn) error {
	if err := writeJSON(ctx, conn, map[string]any{"event": "connected_success"}); err != nil {
		return err
	}
	var start taskStart
	if err := readFrame(ctx, conn, &start); err != nil {
		return err
	}
	return writeJSON(ctx, conn, map[string]any{"event": "task_started"})
}

func newTTSServer(t *testing.T, callback func(context.Context, *http.Request, *websocket.Conn)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ws/v1/t2a_v2" {
			http.NotFound(w, r)
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			defer cancel()
			defer conn.CloseNow()
			callback(ctx, r, conn)
		}()
	}))
}

func readFrame(ctx context.Context, conn *websocket.Conn, target any) error {
	messageType, payload, err := conn.Read(ctx)
	if err != nil {
		return err
	}
	if messageType != websocket.MessageText {
		return fmt.Errorf("unexpected message type %v", messageType)
	}
	return json.Unmarshal(payload, target)
}

func writeJSON(ctx context.Context, conn *websocket.Conn, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, payload)
}

func waitForClientClose(ctx context.Context, conn *websocket.Conn) {
	for {
		if _, _, err := conn.Read(ctx); err != nil {
			return
		}
	}
}

func testConfig(serverURL string) Config {
	endpoint, _ := url.Parse(serverURL)
	return Config{AllowedEndpointHosts: []string{endpoint.Hostname()}, AllowInsecureEndpoint: true}
}

func adapterRequest(serverURL string) runtimepkg.AdapterRequest {
	now := time.Date(2026, time.August, 1, 11, 59, 0, 0, time.UTC)
	media := &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1}
	return runtimepkg.AdapterRequest{
		Kind:    protocol.SessionKindTTS,
		Plan:    planFor(now, endpointFromServer(serverURL)),
		Options: protocol.RequestOptions{Voice: "English_Graceful_Lady", Language: "en"},
		Media:   media,
	}
}

func planFor(now time.Time, endpoint string) protocol.SessionPlan {
	return protocol.SessionPlan{
		PlanID: "plan_minimax", SessionID: "sess_minimax", AttemptID: "att_1",
		Execution: protocol.Execution{Placement: protocol.PlacementEmbedded, ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK},
		ExpiresAt: now.Add(time.Hour),
		Route: protocol.PlanRoute{
			Provider: "minimax", Model: DefaultModel, Adapter: AdapterID, Transport: protocol.TransportWebSocket, Endpoint: endpoint,
			Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "customer-minimax-key", ExpiresAt: now.Add(30 * time.Minute)},
		},
		Reservation:  protocol.Reservation{ID: "res_minimax", LeaseDurationSeconds: 60, LeaseExpiresAt: now.Add(time.Minute), Concurrency: protocol.ConcurrencyReservation{LeaseID: "conc_minimax", Slots: 1}, Usage: protocol.UsageReservation{Unit: protocol.UsageUnitCharacters, AuthorizedUnits: 4_000}},
		Requirements: protocol.Requirements{Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision, RuntimeVersion: "0.1.0"},
		Signature:    "test-signature",
	}
}

func endpointFromServer(serverURL string) string {
	endpoint, _ := url.Parse(serverURL)
	endpoint.Scheme = "ws"
	endpoint.Path = "/ws/v1/t2a_v2"
	return endpoint.String()
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

func awaitError(t *testing.T, events <-chan runtimepkg.ProviderEvent) *runtimepkg.ProviderError {
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
			var providerErr *runtimepkg.ProviderError
			if !errors.As(event.Err, &providerErr) {
				t.Fatalf("terminal error = %v, want *runtimepkg.ProviderError", event.Err)
			}
			return providerErr
		case <-timer.C:
			t.Fatal("timed out waiting for a terminal error")
		}
	}
}

func eventTypes(events []runtimepkg.ProviderEvent) []string {
	types := make([]string, len(events))
	for index, event := range events {
		types[index] = string(event.Type)
	}
	return types
}
