package gradium

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

// Every wire literal in this file is transcribed from Gradium's raw docs
// (docs.gradium.ai/**.md, read 2026-08-07) rather than referenced from the
// adapter's own constants. A test that asserted against `sttPath` or the
// `sttSetup` struct tags would still pass if those were misspelled, which is
// exactly the failure this file exists to catch.

// TestSTTAdapterSendsDocumentedSetupAndStreamsPartialsDuringAudio pins the two
// facts that make this adapter usable at all: the opening handshake is byte-
// exact, and transcripts come back while audio is still being written rather
// than only after the input side closes.
func TestSTTAdapterSendsDocumentedSetupAndStreamsPartialsDuringAudio(t *testing.T) {
	t.Parallel()

	requests := make(chan *http.Request, 1)
	server := newSocketServer(t, "/api/speech/asr", func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		requests <- request.Clone(request.Context())

		setup, err := readFrame(ctx, conn)
		if err != nil {
			t.Errorf("read setup: %v", err)
			return
		}
		// Gradium rejects any input frame that arrives before setup with code
		// 1002, so setup must be the very first frame on the socket, and every
		// field name below is spelled the way the vendor spells it.
		wantSetup := map[string]any{
			"type":         "setup",
			"model_name":   "default",
			"input_format": "pcm_16000",
			"json_config": map[string]any{
				"language":        "en",
				"delay_in_frames": float64(16),
			},
		}
		if !reflect.DeepEqual(setup, wantSetup) {
			t.Errorf("setup frame = %#v, want %#v", setup, wantSetup)
			return
		}
		if err := writeJSON(ctx, conn, map[string]any{
			"type": "ready", "request_id": "gd_req_123", "model_name": "default",
			"sample_rate": 16000, "frame_size": 1280, "delay_in_frames": 16,
		}); err != nil {
			t.Errorf("write ready: %v", err)
			return
		}

		first, err := readFrame(ctx, conn)
		if err != nil {
			t.Errorf("read first audio: %v", err)
			return
		}
		// Audio rides inside the JSON envelope as base64 on this socket; a
		// binary frame would be silently ignored by the vendor.
		wantFirst := map[string]any{"type": "audio", "audio": base64.StdEncoding.EncodeToString([]byte{1, 2, 3})}
		if !reflect.DeepEqual(first, wantFirst) {
			t.Errorf("first audio frame = %#v, want %#v", first, wantFirst)
			return
		}
		if err := writeJSON(ctx, conn, map[string]any{"type": "text", "text": "hello", "start_s": 0.25, "stream_id": 0}); err != nil {
			t.Errorf("write first partial: %v", err)
			return
		}

		second, err := readFrame(ctx, conn)
		if err != nil {
			t.Errorf("read second audio: %v", err)
			return
		}
		wantSecond := map[string]any{"type": "audio", "audio": base64.StdEncoding.EncodeToString([]byte{4, 5, 6})}
		if !reflect.DeepEqual(second, wantSecond) {
			t.Errorf("second audio frame = %#v, want %#v", second, wantSecond)
			return
		}
		if err := writeJSON(ctx, conn, map[string]any{"type": "text", "text": "world", "start_s": 0.5, "stream_id": 0}); err != nil {
			t.Errorf("write second partial: %v", err)
			return
		}
		if err := writeJSON(ctx, conn, map[string]any{"type": "end_text", "stop_s": 1.25, "stream_id": 0}); err != nil {
			t.Errorf("write end_text: %v", err)
			return
		}

		flush, err := readFrame(ctx, conn)
		if err != nil {
			t.Errorf("read flush: %v", err)
			return
		}
		wantFlush := map[string]any{"type": "flush", "flush_id": float64(1)}
		if !reflect.DeepEqual(flush, wantFlush) {
			t.Errorf("flush frame = %#v, want %#v", flush, wantFlush)
			return
		}
		if err := writeJSON(ctx, conn, map[string]any{"type": "flushed", "flush_id": 1}); err != nil {
			t.Errorf("write flushed: %v", err)
			return
		}

		end, err := readFrame(ctx, conn)
		if err != nil {
			t.Errorf("read end_of_stream: %v", err)
			return
		}
		if !reflect.DeepEqual(end, map[string]any{"type": "end_of_stream"}) {
			t.Errorf("end frame = %#v", end)
			return
		}
		if err := writeJSON(ctx, conn, map[string]any{"type": "end_of_stream"}); err != nil {
			t.Errorf("write end_of_stream: %v", err)
			return
		}
		if err := conn.Close(websocket.StatusNormalClosure, ""); err != nil {
			t.Errorf("close server socket: %v", err)
		}
	})
	defer server.Close()

	adapter, err := NewSTT(sttConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), sttRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	events := stream.Events()

	// ready carries the only request id Gradium ever gives us, and managed
	// metering has no other correlation handle, so it must land before media.
	usage := waitEvent(t, events)
	if usage.Type != protocol.EventUsageObserved || decodeString(t, usage.Data, "provider_request_id") != "gd_req_123" {
		t.Fatalf("usage event = %s %s", usage.Type, usage.Data)
	}

	if err := stream.WriteAudio(context.Background(), []byte{1, 2, 3}); err != nil {
		t.Fatalf("write first audio: %v", err)
	}
	// Blocking on the partial here, before the second WriteAudio, is the
	// assertion: the transcript cannot be an artefact of end-of-audio because
	// the caller has not finished sending audio yet.
	first := waitEvent(t, events)
	if first.Type != protocol.EventTranscriptDelta || decodeString(t, first.Data, "text") != "hello" {
		t.Fatalf("first partial = %s %s", first.Type, first.Data)
	}
	if got := decodeNumber(t, first.Data, "audio_start_ms"); got != 250 {
		t.Fatalf("first partial audio_start_ms = %v", got)
	}

	if err := stream.WriteAudio(context.Background(), []byte{4, 5, 6}); err != nil {
		t.Fatalf("write second audio: %v", err)
	}
	second := waitEvent(t, events)
	if second.Type != protocol.EventTranscriptDelta || decodeString(t, second.Data, "text") != "world" {
		t.Fatalf("second partial = %s %s", second.Type, second.Data)
	}

	// Gradium `text` frames are additive fragments, so the final transcript is
	// the join of the segment, not the last fragment.
	final := waitEvent(t, events)
	if final.Type != protocol.EventTranscriptFinal || decodeString(t, final.Data, "text") != "hello world" {
		t.Fatalf("final transcript = %s %s", final.Type, final.Data)
	}
	if got := decodeNumber(t, final.Data, "audio_end_ms"); got != 1250 {
		t.Fatalf("final audio_end_ms = %v", got)
	}
	if final.Extensions[extensionID] == nil {
		t.Fatal("final transcript must retain the raw Gradium frame")
	}
	// end_text is the only endpoint signal this socket has: there is no
	// speech_final flag, so speech.ended has to be derived from it.
	ended := waitEvent(t, events)
	if ended.Type != protocol.EventSpeechEnded || decodeString(t, ended.Data, "reason") != "end_text" {
		t.Fatalf("speech ended = %s %s", ended.Type, ended.Data)
	}

	if err := stream.CommitAudio(context.Background()); err != nil {
		t.Fatalf("commit audio: %v", err)
	}
	flushed := waitEvent(t, events)
	if flushed.Type != protocol.EventWarning || decodeNumber(t, flushed.Data, "flush_id") != 1 {
		t.Fatalf("flushed event = %s %s", flushed.Type, flushed.Data)
	}

	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}
	if event, ok := <-events; ok {
		t.Fatalf("events must close after the server end_of_stream, got %s", event.Type)
	}

	select {
	case received := <-requests:
		// Gradium authenticates with this header and no other; a Bearer
		// Authorization header would open a socket that never gets past setup.
		if got := received.Header.Get("x-api-key"); got != "customer-gradium-key" {
			t.Fatalf("x-api-key = %q", got)
		}
		if got := received.Header.Get("Authorization"); got != "" {
			t.Fatalf("Gradium must not receive an Authorization header, got %q", got)
		}
		if got := received.URL.Query().Get("token"); got != "" {
			t.Fatalf("BYOK route must not use the browser token parameter, got %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not observe the websocket handshake")
	}
}

// TestSTTAdapterUsesOneCredentialPathForManagedAndBYOK pins the package's most
// unusual decision. Every sibling adapter swaps header or query by credential
// source; Gradium has a single account key for both modalities and no header
// alternative, so a future refactor that "restores symmetry" here would break
// managed routes silently.
func TestSTTAdapterUsesOneCredentialPathForManagedAndBYOK(t *testing.T) {
	t.Parallel()

	observed := make(chan *http.Request, 2)
	server := newSocketServer(t, "/api/speech/asr", func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		observed <- request.Clone(request.Context())
		_, _ = readFrame(ctx, conn)
		<-ctx.Done()
	})
	defer server.Close()

	adapter, err := NewSTT(sttConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	for _, source := range []protocol.CredentialSource{protocol.CredentialsBYOK, protocol.CredentialsManaged} {
		request := sttRequest(server.URL)
		request.Plan.Execution.CredentialSource = source
		stream, err := adapter.Open(context.Background(), request)
		if err != nil {
			t.Fatalf("open %s stream: %v", source, err)
		}
		_ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background())
	}

	handshakes := make([]*http.Request, 0, 2)
	for len(handshakes) < 2 {
		select {
		case received := <-observed:
			handshakes = append(handshakes, received)
		case <-time.After(2 * time.Second):
			t.Fatalf("only observed %d handshakes", len(handshakes))
		}
	}
	for index, received := range handshakes {
		if got := received.Header.Get("x-api-key"); got != "customer-gradium-key" {
			t.Fatalf("handshake %d x-api-key = %q", index, got)
		}
		if got := received.URL.RawQuery; got != "" {
			t.Fatalf("handshake %d carried query %q", index, got)
		}
	}
}

// TestSTTAdapterDropsStepFramesWithoutEmitting guards a deliberate silence.
// `step` arrives every 80 ms; forwarding it would emit ~12.5 events per second
// into a 32-slot buffer and starve transcripts. An unknown type still surfaces
// as a warning, so this is a targeted drop rather than a blanket one.
func TestSTTAdapterDropsStepFramesWithoutEmitting(t *testing.T) {
	t.Parallel()

	server := newSocketServer(t, "/api/speech/asr", func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		if _, err := readFrame(ctx, conn); err != nil {
			t.Errorf("read setup: %v", err)
			return
		}
		for index := 0; index < 5; index++ {
			if err := writeJSON(ctx, conn, map[string]any{
				"type":       "step",
				"vad":        []map[string]any{{"horizon_s": 0.5, "inactivity_prob": 0.05}},
				"step_idx":   index,
				"step_dur_s": 0.08,
			}); err != nil {
				t.Errorf("write step %d: %v", index, err)
				return
			}
		}
		if err := writeJSON(ctx, conn, map[string]any{"type": "vad", "step_idx": 6}); err != nil {
			t.Errorf("write legacy vad: %v", err)
			return
		}
		if err := writeJSON(ctx, conn, map[string]any{"type": "text", "text": "after the steps", "start_s": 1}); err != nil {
			t.Errorf("write text: %v", err)
			return
		}
		<-ctx.Done()
	})
	defer server.Close()

	adapter, err := NewSTT(sttConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), sttRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()

	// `vad` is the legacy spelling of `step` but is not in the switch, so it
	// falls through to the warning arm. That asymmetry is intentional: an
	// unrecognised type must stay visible.
	warning := waitEvent(t, stream.Events())
	if warning.Type != protocol.EventWarning || decodeString(t, warning.Data, "provider_type") != "vad" {
		t.Fatalf("legacy vad event = %s %s", warning.Type, warning.Data)
	}
	// The five `step` frames produced nothing, so the very next event is the
	// transcript.
	delta := waitEvent(t, stream.Events())
	if delta.Type != protocol.EventTranscriptDelta || decodeString(t, delta.Data, "text") != "after the steps" {
		t.Fatalf("event after step frames = %s %s", delta.Type, delta.Data)
	}
}

// TestSTTAdapterClassifiesErrorFrames covers the in-band half of the error
// contract. Gradium publishes only 1002/1008/1011, and 1008 is a single bucket
// that its own docs say spans auth, subscription, and malformed input, so the
// split below is the only thing standing between three distinct gateway codes
// and one useless one.
func TestSTTAdapterClassifiesErrorFrames(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name          string
		code          int
		message       string
		wantCode      string
		wantRetryable bool
	}{
		// Documented cause: "sending input before setup".
		{"protocol violation", 1002, "Session not found. Send setup first.", "invalid_request", false},
		// Documented body: "error from server 1008: API key is revoked or expired".
		{"revoked key", 1008, "API key is revoked or expired", "authentication_failed", false},
		// Documented 1008 cause: "missing subscription".
		{"missing subscription", 1008, "Subscription is missing or inactive", "provider_quota_exceeded", false},
		{"exhausted credits", 1008, "Not enough credits remaining", "provider_quota_exceeded", false},
		// Documented 1008 cause: "invalid audio format" — a caller mistake,
		// and the conservative default for any 1008 text we do not recognise.
		{"bad audio format", 1008, "Invalid audio format", "invalid_request", false},
		{"server fault", 1011, "Internal server error", "provider_unavailable", true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			server := newSocketServer(t, "/api/speech/asr", func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
				if _, err := readFrame(ctx, conn); err != nil {
					t.Errorf("read setup: %v", err)
					return
				}
				if err := writeJSON(ctx, conn, map[string]any{
					"type": "error", "message": testCase.message, "code": testCase.code,
				}); err != nil {
					t.Errorf("write error frame: %v", err)
					return
				}
				<-ctx.Done()
			})
			defer server.Close()

			adapter, err := NewSTT(sttConfig(server.URL))
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			stream, err := adapter.Open(context.Background(), sttRequest(server.URL))
			if err != nil {
				t.Fatalf("open stream: %v", err)
			}
			providerError := awaitTerminalError(t, stream.Events())
			if providerError.Code != testCase.wantCode {
				t.Fatalf("code = %q, want %q", providerError.Code, testCase.wantCode)
			}
			if providerError.Retryable != testCase.wantRetryable {
				t.Fatalf("retryable = %v, want %v", providerError.Retryable, testCase.wantRetryable)
			}
			if providerError.ProviderStatus != testCase.code {
				t.Fatalf("provider status = %d, want %d", providerError.ProviderStatus, testCase.code)
			}
			if !strings.Contains(providerError.Message, testCase.message) {
				t.Fatalf("message = %q, want it to carry %q", providerError.Message, testCase.message)
			}
			if providerError.Extensions[extensionID] == nil {
				t.Fatal("terminal error must retain the raw Gradium frame")
			}
			if _, ok := <-stream.Events(); ok {
				t.Fatal("events must close after a terminal error frame")
			}
		})
	}
}

// TestSTTAdapterClassifiesHandshakeFailures covers the other half. The
// handshake is the only surface on which Gradium can answer 429 at all: its
// in-band error frame has no rate-limit code, so without this path
// provider_rate_limited would be unreachable.
func TestSTTAdapterClassifiesHandshakeFailures(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name          string
		status        int
		wantCode      string
		wantRetryable bool
	}{
		{"unauthorized", http.StatusUnauthorized, "authentication_failed", false},
		{"forbidden", http.StatusForbidden, "authentication_failed", false},
		{"payment required", http.StatusPaymentRequired, "provider_quota_exceeded", false},
		{"rate limited", http.StatusTooManyRequests, "provider_rate_limited", true},
		{"bad request", http.StatusBadRequest, "invalid_request", false},
		{"upstream down", http.StatusServiceUnavailable, "provider_unavailable", true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(testCase.status)
			}))
			defer server.Close()

			adapter, err := NewSTT(sttConfig(server.URL))
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			_, err = adapter.Open(context.Background(), sttRequest(server.URL))
			var providerError *runtimepkg.ProviderError
			if !errors.As(err, &providerError) {
				t.Fatalf("open error = %v, want a ProviderError", err)
			}
			if providerError.Code != testCase.wantCode {
				t.Fatalf("code = %q, want %q", providerError.Code, testCase.wantCode)
			}
			if providerError.Retryable != testCase.wantRetryable {
				t.Fatalf("retryable = %v, want %v", providerError.Retryable, testCase.wantRetryable)
			}
			if providerError.ProviderStatus != testCase.status {
				t.Fatalf("provider status = %d, want %d", providerError.ProviderStatus, testCase.status)
			}
			// A failed handshake is exactly where a naive implementation
			// echoes the request URL or headers into the error.
			if strings.Contains(providerError.Error(), "customer-gradium-key") {
				t.Fatalf("handshake error leaked the credential: %q", providerError.Error())
			}
		})
	}
}

// TestSTTAdapterRejectsUnroutableRequests makes sure the adapter refuses work
// that does not belong to it before it opens a socket or attaches a secret.
func TestSTTAdapterRejectsUnroutableRequests(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name    string
		mutate  func(*runtimepkg.AdapterRequest)
		wantSub string
	}{
		{"tts session", func(r *runtimepkg.AdapterRequest) { r.Kind = protocol.SessionKindTTS }, "stt sessions"},
		{"other provider", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Provider = "deepgram" }, "cannot open provider"},
		{"http transport", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Transport = protocol.TransportHTTP }, "websocket transport"},
		// "auto" is a routing placeholder; resolving it here would pick a
		// route the control plane never signed.
		{"auto model", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "auto" }, "concrete model"},
		{"empty model", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "" }, "concrete model"},
		{"no credential", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential = nil }, "bearer credential"},
		{"blank credential", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential.Value = "   " }, "bearer credential"},
		{"wrong credential kind", func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Credential.Kind = protocol.CredentialKind("basic")
		}, "bearer credential"},
		{"no media", func(r *runtimepkg.AdapterRequest) { r.Media = nil }, "media configuration"},
		// Gradium's `opus` is Ogg-wrapped, not the bare frames this runtime
		// moves, so accepting it would hand consumers undecodable bytes.
		{"opus media", func(r *runtimepkg.AdapterRequest) { r.Media.Encoding = "opus" }, "pcm_s16le"},
		{"stereo media", func(r *runtimepkg.AdapterRequest) { r.Media.Channels = 2 }, "mono only"},
		{"unsupported rate", func(r *runtimepkg.AdapterRequest) { r.Media.SampleRateHz = 32_000 }, "32000 Hz pcm"},
		// Gradium transcribes en/fr/de/es/pt only; asking for anything else
		// must fail rather than quietly transcribe into the wrong language.
		{"unsupported language", func(r *runtimepkg.AdapterRequest) { r.Options.Language = "ja" }, `language "ja"`},
		{"endpoint on other socket", func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Endpoint = strings.Replace(r.Plan.Route.Endpoint, "/api/speech/asr", "/api/speech/tts", 1)
		}, "/api/speech/asr"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			adapter, err := NewSTT(STTConfig{})
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			request := sttRequest("http://127.0.0.1:1")
			request.Plan.Route.Endpoint = "wss://api.gradium.ai/api/speech/asr"
			request.Plan.Route.Credential.Value = "secret-that-must-not-leak"
			testCase.mutate(&request)
			_, err = adapter.Open(context.Background(), request)
			if err == nil {
				t.Fatal("adapter accepted an unroutable request")
			}
			if !strings.Contains(err.Error(), testCase.wantSub) {
				t.Fatalf("error = %q, want it to mention %q", err.Error(), testCase.wantSub)
			}
			if strings.Contains(err.Error(), "secret-that-must-not-leak") {
				t.Fatalf("validation error leaked the credential: %q", err.Error())
			}
		})
	}
}

// TestSTTAdapterOmitsLanguageWhenUnset pins the documented "do not ground on a
// single language" behavior: the key must be absent, not present and empty,
// because an empty string is not in Gradium's accepted enumeration.
func TestSTTAdapterOmitsLanguageWhenUnset(t *testing.T) {
	t.Parallel()

	setups := make(chan map[string]any, 1)
	server := newSocketServer(t, "/api/speech/asr", func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		frame, err := readFrame(ctx, conn)
		if err != nil {
			t.Errorf("read setup: %v", err)
			return
		}
		setups <- frame
		<-ctx.Done()
	})
	defer server.Close()

	adapter, err := NewSTT(sttConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	request := sttRequest(server.URL)
	request.Options.Language = ""
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()

	select {
	case setup := <-setups:
		want := map[string]any{
			"type":         "setup",
			"model_name":   "default",
			"input_format": "pcm_16000",
			"json_config":  map[string]any{"delay_in_frames": float64(16)},
		}
		if !reflect.DeepEqual(setup, want) {
			t.Fatalf("setup frame = %#v, want %#v", setup, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never received setup")
	}
}

// TestSTTAdapterRejectsTextOperations pins the modality boundary: an STT
// stream that silently accepted text would let a misconfigured pipeline appear
// to work while synthesising nothing.
func TestSTTAdapterRejectsTextOperations(t *testing.T) {
	t.Parallel()

	server := newSocketServer(t, "/api/speech/asr", func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		_, _ = readFrame(ctx, conn)
		<-ctx.Done()
	})
	defer server.Close()

	adapter, err := NewSTT(sttConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), sttRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()

	if err := stream.AppendText(context.Background(), "hello"); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Fatalf("append text = %v", err)
	}
	if err := stream.CommitText(context.Background()); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Fatalf("commit text = %v", err)
	}
	if err := stream.WriteAudio(context.Background(), nil); err == nil {
		t.Fatal("empty audio must be rejected")
	}
}

// ---------------------------------------------------------------------------
// Shared test scaffolding, used by tts_test.go too.
// ---------------------------------------------------------------------------

func newSocketServer(t *testing.T, path string, callback func(context.Context, *http.Request, *websocket.Conn)) *httptest.Server {
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
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		go func() {
			defer cancel()
			defer conn.CloseNow()
			callback(ctx, r, conn)
		}()
	}))
}

// readFrame decodes into an untyped map on purpose. Unmarshalling into the
// adapter's own structs would make a misspelled json tag invisible.
func readFrame(ctx context.Context, conn *websocket.Conn) (map[string]any, error) {
	messageType, payload, err := conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if messageType != websocket.MessageText {
		return nil, fmt.Errorf("frame type = %v, want text", messageType)
	}
	frame := map[string]any{}
	if err := json.Unmarshal(payload, &frame); err != nil {
		return nil, fmt.Errorf("decode %q: %w", payload, err)
	}
	return frame, nil
}

func writeJSON(ctx context.Context, conn *websocket.Conn, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, payload)
}

func waitEvent(t *testing.T, events <-chan runtimepkg.ProviderEvent) runtimepkg.ProviderEvent {
	t.Helper()
	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("provider events closed early")
		}
		if event.Err != nil {
			t.Fatalf("provider event error: %v", event.Err)
		}
		return event
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a provider event")
		return runtimepkg.ProviderEvent{}
	}
}

func awaitTerminalError(t *testing.T, events <-chan runtimepkg.ProviderEvent) *runtimepkg.ProviderError {
	t.Helper()
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
				t.Fatalf("terminal error = %v, want a ProviderError", event.Err)
			}
			return providerError
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for a terminal error")
			return nil
		}
	}
}

func decodeString(t *testing.T, data json.RawMessage, field string) string {
	t.Helper()
	decoded := map[string]any{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("decode event data %q: %v", data, err)
	}
	value, _ := decoded[field].(string)
	return value
}

func decodeNumber(t *testing.T, data json.RawMessage, field string) float64 {
	t.Helper()
	decoded := map[string]any{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("decode event data %q: %v", data, err)
	}
	value, ok := decoded[field].(float64)
	if !ok {
		t.Fatalf("event data %q has no numeric %q", data, field)
	}
	return value
}

func sttConfig(serverURL string) STTConfig {
	endpoint, _ := url.Parse(serverURL)
	return STTConfig{AllowedEndpointHosts: []string{endpoint.Hostname()}, AllowInsecureEndpoint: true}
}

func ttsConfig(serverURL string) TTSConfig {
	endpoint, _ := url.Parse(serverURL)
	return TTSConfig{AllowedEndpointHosts: []string{endpoint.Hostname()}, AllowInsecureEndpoint: true}
}

func sttRequest(serverURL string) runtimepkg.AdapterRequest {
	return runtimepkg.AdapterRequest{
		Kind:    protocol.SessionKindSTT,
		Plan:    planFor(STTAdapterID, endpointFor(serverURL, "/api/speech/asr")),
		Options: protocol.RequestOptions{Language: "en-US"},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
	}
}

func ttsRequest(serverURL string) runtimepkg.AdapterRequest {
	return runtimepkg.AdapterRequest{
		Kind:    protocol.SessionKindTTS,
		Plan:    planFor(TTSAdapterID, endpointFor(serverURL, "/api/speech/tts")),
		Options: protocol.RequestOptions{Voice: "YTpq7expH9539ERJ", Language: "en"},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 48_000, Channels: 1},
	}
}

func planFor(adapterID, endpoint string) protocol.SessionPlan {
	now := time.Date(2026, time.August, 7, 12, 0, 0, 0, time.UTC)
	return protocol.SessionPlan{
		PlanID: "plan_gradium", SessionID: "sess_gradium", AttemptID: "att_1",
		Execution: protocol.Execution{
			Placement:        protocol.PlacementEmbedded,
			ProviderRoute:    protocol.RouteProviderDirect,
			CredentialSource: protocol.CredentialsBYOK,
		},
		ExpiresAt: now.Add(time.Hour),
		Route: protocol.PlanRoute{
			// "default" is Gradium's own documented model alias for both
			// modalities; it still has to arrive in the plan.
			Provider: "gradium", Model: "default", Adapter: adapterID,
			Transport: protocol.TransportWebSocket, Endpoint: endpoint,
			Credential: &protocol.DelegatedCredential{
				Kind: protocol.CredentialBearer, Value: "customer-gradium-key", ExpiresAt: now.Add(30 * time.Minute),
			},
		},
		Reservation: protocol.Reservation{
			ID: "res_gradium", LeaseDurationSeconds: 60, LeaseExpiresAt: now.Add(time.Minute),
			Concurrency: protocol.ConcurrencyReservation{LeaseID: "conc_gradium", Slots: 1},
			Usage:       protocol.UsageReservation{Unit: protocol.UsageUnitCharacters, AuthorizedUnits: 4_000},
		},
		Telemetry: protocol.Telemetry{
			Endpoint: "https://control.speko.test/v1/runtime-events", Token: "telemetry-token", FlushIntervalMS: 5_000,
		},
		Requirements: protocol.Requirements{
			Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision, RuntimeVersion: "0.1.0",
		},
		Signature: "test-signature",
	}
}

func endpointFor(serverURL, path string) string {
	endpoint, _ := url.Parse(serverURL)
	endpoint.Scheme = "ws"
	endpoint.Path = path
	return endpoint.String()
}
