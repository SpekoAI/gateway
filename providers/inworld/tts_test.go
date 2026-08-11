package inworld

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

// TestCommitTextSendsDocumentedRequest pins every wire detail that was verified
// against Inworld's TTS OpenAPI document. Each one is a field a previous port of
// this provider got wrong in a way no test caught, because the tests asserted
// what our own code emitted instead of what the vendor documents.
func TestCommitTextSendsDocumentedRequest(t *testing.T) {
	t.Parallel()

	type observed struct {
		method string
		path   string
		header http.Header
		body   []byte
	}
	requests := make(chan observed, 1)
	server := newStreamServer(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		requests <- observed{method: r.Method, path: r.URL.Path, header: r.Header.Clone(), body: body}
		writeMessages(t, w, audioMessage(pcm(1, 2, 3, 4)))
	})
	defer server.Close()

	stream := openStream(t, server.URL, nil)
	defer func() { _ = stream.Abort(context.Background()) }()

	if err := stream.AppendText(context.Background(), "Hello, "); err != nil {
		t.Fatalf("append text: %v", err)
	}
	if err := stream.AppendText(context.Background(), "world!"); err != nil {
		t.Fatalf("append second fragment: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit text: %v", err)
	}

	got := <-requests
	if got.method != http.MethodPost {
		t.Errorf("method = %q, want POST", got.method)
	}
	// The colon is part of the resource name, not a port. A URL builder that
	// escapes or splits it silently 404s.
	if got.path != "/tts/v1/voice:stream" {
		t.Errorf("path = %q, want /tts/v1/voice:stream", got.path)
	}
	if want := "Basic customer-inworld-key"; got.header.Get("Authorization") != want {
		t.Errorf("Authorization = %q, want %q", got.header.Get("Authorization"), want)
	}
	if got.header.Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type = %q", got.header.Get("Content-Type"))
	}

	// Compared as decoded JSON so the assertion is about field NAMES and values,
	// not about Go's struct ordering. camelCase is what the OpenAPI schema
	// declares; the platform's TypeScript port sent snake_case.
	var body map[string]any
	if err := json.Unmarshal(got.body, &body); err != nil {
		t.Fatalf("request body is not JSON: %v (%s)", err, got.body)
	}
	want := map[string]any{
		"text":    "Hello, world!",
		"voiceId": "Dennis",
		"modelId": "inworld-tts-2",
		"audioConfig": map[string]any{
			// LINEAR16 is the enum value for raw 16-bit PCM. "pcm_s16le",
			// "linear16", and "PCM_S16LE" are all rejected upstream.
			"audioEncoding":   "LINEAR16",
			"sampleRateHertz": float64(24_000),
		},
		"language": "en-US",
	}
	if diff := fmt.Sprint(body); diff != fmt.Sprint(want) {
		t.Errorf("request body =\n%s\nwant\n%s", diff, fmt.Sprint(want))
	}
}

// TestStreamEmitsStartedFramesThenDone covers the canonical event contract and
// the two decoding steps between the wire and Audio: base64 and the per-chunk
// WAV header Inworld documents for LINEAR16 streaming.
func TestStreamEmitsStartedFramesThenDone(t *testing.T) {
	t.Parallel()

	first := pcm(1, 2, 3, 4)
	second := pcm(5, 6)
	server := newStreamServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeMessages(t, w,
			// Every LINEAR16 streaming chunk carries a complete WAV header so it
			// can play standalone. Emitting it verbatim would splice RIFF bytes
			// into the PCM the consumer was promised.
			audioMessage(withWAVHeader(first)),
			audioMessage(withWAVHeader(second)),
			usageMessage(13),
		)
	})
	defer server.Close()

	stream := openStream(t, server.URL, nil)
	defer func() { _ = stream.Abort(context.Background()) }()
	synthesize(t, stream, "Hello, world!")

	events := collect(t, stream.Events(), 5)
	if got := eventTypes(events); got != "audio.started,audio.frame,audio.frame,usage.observed,audio.done" {
		t.Fatalf("event types = %s", got)
	}
	if string(events[1].Audio) != string(first) {
		t.Errorf("frame 1 audio = %v, want %v", events[1].Audio, first)
	}
	if string(events[2].Audio) != string(second) {
		t.Errorf("frame 2 audio = %v, want %v", events[2].Audio, second)
	}
	// audio.started carries no audio; a consumer that plays Data would emit noise.
	if events[0].Audio != nil {
		t.Errorf("audio.started carried %d audio bytes", len(events[0].Audio))
	}
	if events[1].Extensions[extensionID] == nil {
		t.Error("audio frames must retain the raw Inworld payload as an extension")
	}
	var usage struct {
		Usage struct {
			ProcessedCharactersCount int `json:"processedCharactersCount"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(events[3].Data, &usage); err != nil || usage.Usage.ProcessedCharactersCount != 13 {
		t.Errorf("usage data = %s, err=%v", events[3].Data, err)
	}
}

// TestStreamDeliversFramesBeforeResponseCompletes is the load-bearing test for
// an HTTP adapter: it proves the body is consumed incrementally. The handler
// refuses to write the second chunk until the client has already surfaced the
// first, so an implementation that reads the body to EOF before decoding
// deadlocks here instead of quietly costing a full utterance of latency.
func TestStreamDeliversFramesBeforeResponseCompletes(t *testing.T) {
	t.Parallel()

	sawFirstFrame := make(chan struct{})
	handlerDone := make(chan struct{})
	server := newStreamServer(t, func(w http.ResponseWriter, _ *http.Request) {
		defer close(handlerDone)
		writeMessages(t, w, audioMessage(pcm(1, 2)))
		select {
		case <-sawFirstFrame:
		case <-time.After(2 * time.Second):
			t.Error("client never surfaced the first frame; the response body is being buffered")
			return
		}
		writeMessages(t, w, audioMessage(pcm(3, 4)))
	})
	defer server.Close()

	stream := openStream(t, server.URL, nil)
	defer func() { _ = stream.Abort(context.Background()) }()
	synthesize(t, stream, "Hello")

	started := collect(t, stream.Events(), 2)
	if got := eventTypes(started); got != "audio.started,audio.frame" {
		t.Fatalf("event types before the second write = %s", got)
	}
	close(sawFirstFrame)

	rest := collect(t, stream.Events(), 2)
	if got := eventTypes(rest); got != "audio.frame,audio.done" {
		t.Fatalf("event types after the second write = %s", got)
	}
	// Multiple frames, not one concatenated blob: chunking is what makes
	// time-to-first-audio track the provider rather than the utterance length.
	if string(rest[0].Audio) != string(pcm(3, 4)) {
		t.Errorf("second frame audio = %v", rest[0].Audio)
	}
	<-handlerDone
}

// TestCredentialSourceSelectsAuthScheme guards the failure mode where managed
// sessions authenticate wrongly while every other test still passes: BYOK keys
// are Basic credentials, minted control-plane tokens are Bearer JWTs, and
// neither works when sent as the other.
func TestCredentialSourceSelectsAuthScheme(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name   string
		route  protocol.ProviderRoute
		source protocol.CredentialSource
		value  string
		want   string
	}{
		{"byok sends the portal key as Basic", protocol.RouteProviderDirect, protocol.CredentialsBYOK, "customer-inworld-key", "Basic customer-inworld-key"},
		{"managed sends the minted jwt as Bearer", protocol.RouteProviderDirect, protocol.CredentialsManaged, "minted.jwt.value", "Bearer minted.jwt.value"},
		// A relay plan is managed for billing purposes but carries the relay
		// connector's permanent portal key, which is a Basic credential exactly
		// like a customer's own.
		{"relay sends the connector key as Basic", protocol.RouteSpekoRelay, protocol.CredentialsManaged, "connector-inworld-key", "Basic connector-inworld-key"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			headers := make(chan http.Header, 1)
			queries := make(chan url.Values, 1)
			server := newStreamServer(t, func(w http.ResponseWriter, r *http.Request) {
				headers <- r.Header.Clone()
				queries <- r.URL.Query()
				writeMessages(t, w, audioMessage(pcm(1)))
			})
			defer server.Close()

			stream := openStream(t, server.URL, func(request *runtimepkg.AdapterRequest) {
				request.Plan.Execution.ProviderRoute = testCase.route
				request.Plan.Execution.CredentialSource = testCase.source
				request.Plan.Route.Credential.Value = testCase.value
			})
			defer func() { _ = stream.Abort(context.Background()) }()
			synthesize(t, stream, "Hello")

			if got := (<-headers).Get("Authorization"); got != testCase.want {
				t.Errorf("Authorization = %q, want %q", got, testCase.want)
			}
			// The query-parameter credential channel belongs to Inworld's
			// WebSocket resource. On this endpoint it authenticates nothing and
			// would leak the secret into request logs.
			if query := <-queries; len(query) != 0 {
				t.Errorf("request carried query parameters %v; the credential belongs in the header", query)
			}
		})
	}
}

// TestRelayRouteAcceptsRelayAccessCredentialKind pins the relay arm's other
// credential spelling: protocol.SessionPlan validation requires a relay plan
// to label its credential relay_access, while the relay connector that
// synthesizes the plan and drives this adapter directly labels the same
// permanent key bearer. Both must open, and both take the Basic channel.
func TestRelayRouteAcceptsRelayAccessCredentialKind(t *testing.T) {
	t.Parallel()

	headers := make(chan http.Header, 1)
	server := newStreamServer(t, func(w http.ResponseWriter, r *http.Request) {
		headers <- r.Header.Clone()
		writeMessages(t, w, audioMessage(pcm(1)))
	})
	defer server.Close()

	stream := openStream(t, server.URL, func(request *runtimepkg.AdapterRequest) {
		request.Plan.Execution.ProviderRoute = protocol.RouteSpekoRelay
		request.Plan.Execution.CredentialSource = protocol.CredentialsManaged
		request.Plan.Route.Credential.Kind = protocol.CredentialRelayAccess
		request.Plan.Route.Credential.Value = "connector-inworld-key"
	})
	defer func() { _ = stream.Abort(context.Background()) }()
	synthesize(t, stream, "Hello")

	if got := (<-headers).Get("Authorization"); got != "Basic connector-inworld-key" {
		t.Errorf("Authorization = %q, want the Basic channel", got)
	}
}

// TestStatusCodesMapToProtocolErrors pins the classification the runtime uses to
// decide whether an attempt may be retried or the credential must be replaced.
func TestStatusCodesMapToProtocolErrors(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		status    int
		code      string
		retryable bool
	}{
		{http.StatusUnauthorized, "authentication_failed", false},
		{http.StatusForbidden, "authentication_failed", false},
		{http.StatusTooManyRequests, "provider_rate_limited", true},
		{http.StatusInternalServerError, "provider_unavailable", true},
		{http.StatusServiceUnavailable, "provider_unavailable", true},
		{http.StatusBadRequest, "invalid_request", false},
		{http.StatusNotFound, "invalid_request", false},
	} {
		t.Run(fmt.Sprint(testCase.status), func(t *testing.T) {
			t.Parallel()
			server := newStreamServer(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(testCase.status)
				_, _ = w.Write([]byte(`{"error":{"code":3,"message":"nope"}}`))
			})
			defer server.Close()

			stream := openStream(t, server.URL, nil)
			defer func() { _ = stream.Abort(context.Background()) }()
			if err := stream.AppendText(context.Background(), "Hello"); err != nil {
				t.Fatalf("append text: %v", err)
			}

			// The status is known before any audio, so CommitText reports it
			// synchronously rather than as an event the caller must correlate.
			err := stream.CommitText(context.Background())
			var providerErr *runtimepkg.ProviderError
			if !errors.As(err, &providerErr) {
				t.Fatalf("commit error = %v, want *runtimepkg.ProviderError", err)
			}
			if providerErr.Code != testCase.code || providerErr.Retryable != testCase.retryable || providerErr.ProviderStatus != testCase.status {
				t.Fatalf("error = {code:%s retryable:%t status:%d}, want {code:%s retryable:%t status:%d}",
					providerErr.Code, providerErr.Retryable, providerErr.ProviderStatus, testCase.code, testCase.retryable, testCase.status)
			}
			if providerErr.Extensions[extensionID] == nil {
				t.Error("provider error must retain the raw Inworld error payload")
			}
			// A failed request must leave the stream reusable, not wedged
			// behind an utterance that never completed.
			if err := stream.AppendText(context.Background(), "retry"); err != nil {
				t.Errorf("stream unusable after a rejected request: %v", err)
			}
		})
	}
}

// TestInStreamErrorTerminatesTheAttempt covers the error object Inworld can send
// mid-stream after a 200. Its `code` is a gRPC status, not an HTTP one, so it
// must not be reported as ProviderStatus — 7 is PERMISSION_DENIED, not a
// redirect.
func TestInStreamErrorTerminatesTheAttempt(t *testing.T) {
	t.Parallel()

	server := newStreamServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeMessages(t, w, audioMessage(pcm(1, 2)), json.RawMessage(`{"error":{"code":7,"message":"quota"}}`))
	})
	defer server.Close()

	stream := openStream(t, server.URL, nil)
	defer func() { _ = stream.Abort(context.Background()) }()
	synthesize(t, stream, "Hello")

	_ = collect(t, stream.Events(), 2) // audio.started, audio.frame
	event := next(t, stream.Events())
	var providerErr *runtimepkg.ProviderError
	if !errors.As(event.Err, &providerErr) {
		t.Fatalf("terminal event = %+v, want a provider error", event)
	}
	if providerErr.Code != "authentication_failed" {
		t.Errorf("code = %q, want authentication_failed", providerErr.Code)
	}
	if providerErr.ProviderStatus != 0 {
		t.Errorf("ProviderStatus = %d; a gRPC status code must not be reported as an HTTP status", providerErr.ProviderStatus)
	}
}

// TestCancelAbortsTheInFlightRequest proves cancellation reaches the wire. HTTP
// has no cancel message, so the only way to stop the provider billing and
// generating is to drop the request — the handler observes its context close.
func TestCancelAbortsTheInFlightRequest(t *testing.T) {
	t.Parallel()

	handlerContextDone := make(chan error, 1)
	server := newStreamServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeMessages(t, w, audioMessage(pcm(1, 2)))
		select {
		case <-r.Context().Done():
			handlerContextDone <- r.Context().Err()
		case <-time.After(2 * time.Second):
			handlerContextDone <- errors.New("request was never cancelled")
		}
	})
	defer server.Close()

	stream := openStream(t, server.URL, nil)
	defer func() { _ = stream.Abort(context.Background()) }()
	synthesize(t, stream, "Hello")
	_ = collect(t, stream.Events(), 2) // audio.started, audio.frame

	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if err := <-handlerContextDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("upstream request context = %v, want context.Canceled", err)
	}
	// A cancel is the caller's own decision, so it must not surface as a
	// provider failure, and the stream must accept the next utterance.
	if err := stream.AppendText(context.Background(), "next"); err != nil {
		t.Fatalf("stream unusable after cancel: %v", err)
	}
}

// TestAbortClosesTheStream checks the teardown contract the runtime relies on
// after a terminal failure: the request dies and the event channel closes
// exactly once, with no send-on-closed-channel race from the reader.
func TestAbortClosesTheStream(t *testing.T) {
	t.Parallel()

	released := make(chan struct{})
	server := newStreamServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeMessages(t, w, audioMessage(pcm(1, 2)))
		select {
		case <-r.Context().Done():
		case <-released:
		}
	})
	defer func() { close(released); server.Close() }()

	stream := openStream(t, server.URL, nil)
	synthesize(t, stream, "Hello")
	_ = collect(t, stream.Events(), 2)

	if err := stream.Abort(context.Background()); err != nil {
		t.Fatalf("abort: %v", err)
	}
	drain(t, stream.Events())
	// Abort is idempotent: the runtime may call it after a failed Close.
	if err := stream.Abort(context.Background()); err != nil {
		t.Fatalf("second abort: %v", err)
	}
}

// TestCloseWaitsForFinalAudio: a caller that closes right after CommitText must
// still receive the audio it already paid for.
func TestCloseWaitsForFinalAudio(t *testing.T) {
	t.Parallel()

	server := newStreamServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeMessages(t, w, audioMessage(pcm(1, 2)), audioMessage(pcm(3, 4)))
	})
	defer server.Close()

	stream := openStream(t, server.URL, nil)
	synthesize(t, stream, "Hello")

	closed := make(chan error, 1)
	go func() { closed <- stream.Close(context.Background()) }()

	events := collect(t, stream.Events(), 4)
	if got := eventTypes(events); got != "audio.started,audio.frame,audio.frame,audio.done" {
		t.Fatalf("event types = %s", got)
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("close did not return after the response completed")
	}
	if _, ok := <-stream.Events(); ok {
		t.Fatal("events must be closed after Close")
	}
}

// TestOpenRejectsUnusableRequests keeps validation at Open, where a bad plan
// fails before a credential is attached to a request. HTTP has no handshake, so
// nothing else would catch these until the first synthesis.
func TestOpenRejectsUnusableRequests(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name    string
		mutate  func(*runtimepkg.AdapterRequest)
		wantErr string
	}{
		{"stt session", func(r *runtimepkg.AdapterRequest) { r.Kind = protocol.SessionKindSTT }, "tts sessions"},
		{"another provider", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Provider = "cartesia" }, "cannot open provider"},
		// Every other adapter in this repo wants websocket; this one must not
		// accept a websocket route it cannot speak.
		{"websocket transport", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Transport = protocol.TransportWebSocket }, "requires http transport"},
		{"missing credential", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential = nil }, "bearer credential"},
		{"blank credential", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential.Value = "  " }, "bearer credential"},
		{"wrong credential kind", func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Credential.Kind = protocol.CredentialRelayAccess
		}, "bearer credential"},
		// "auto" is a routing request, not a model. Sending it upstream would
		// synthesize with whatever Inworld defaults to.
		{"auto model", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "auto" }, "concrete model"},
		{"empty model", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "" }, "concrete model"},
		{"unknown model", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "inworld-tts-9" }, "does not support model"},
		// Inworld silently reroutes discontinued ids to a successor. Accepting
		// one would make the plan's model a lie.
		{"discontinued model", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "inworld-tts-1" }, "discontinued"},
		{"missing voice", func(r *runtimepkg.AdapterRequest) { r.Options.Voice = " " }, "voice id"},
		{"missing media", func(r *runtimepkg.AdapterRequest) { r.Media = nil }, "media configuration"},
		{"opus media", func(r *runtimepkg.AdapterRequest) { r.Media.Encoding = "opus" }, "mono pcm_s16le"},
		{"stereo media", func(r *runtimepkg.AdapterRequest) { r.Media.Channels = 2 }, "mono pcm_s16le"},
		// 11025 is not in Inworld's supported sampleRateHertz set; it would fail
		// upstream once LINEAR16 could not honour it.
		{"unsupported sample rate", func(r *runtimepkg.AdapterRequest) { r.Media.SampleRateHz = 11_025 }, "sample rate"},
		{"wrong path", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = "https://api.inworld.ai/tts/v1/voice" }, "endpoint path"},
		{"foreign host", func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = "https://evil.example/tts/v1/voice:stream" }, "host is not allowed"},
		{"credential in the query string", func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Endpoint = "https://api.inworld.ai/tts/v1/voice:stream?authorization=leak"
		}, "clean absolute HTTPS URL"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			adapter, err := New(Config{})
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			request := adapterRequest("https://api.inworld.ai/tts/v1/voice:stream")
			testCase.mutate(&request)
			_, err = adapter.Open(context.Background(), request)
			if err == nil || !strings.Contains(err.Error(), testCase.wantErr) {
				t.Fatalf("open error = %v, want one containing %q", err, testCase.wantErr)
			}
			// Validation messages are surfaced to callers and logs.
			if strings.Contains(err.Error(), "customer-inworld-key") {
				t.Fatalf("open error leaked the credential: %v", err)
			}
		})
	}
}

// TestSynthesizerRejectsAudioInput: a TTS stream is not a transcriber. The
// runtime distinguishes "unsupported for this kind" from a provider fault.
func TestSynthesizerRejectsAudioInput(t *testing.T) {
	t.Parallel()

	stream := openStream(t, "https://api.inworld.ai/tts/v1/voice:stream", nil)
	defer func() { _ = stream.Abort(context.Background()) }()
	if err := stream.WriteAudio(context.Background(), []byte{1}); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Errorf("WriteAudio = %v, want ErrUnsupportedOperation", err)
	}
	if err := stream.CommitAudio(context.Background()); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Errorf("CommitAudio = %v, want ErrUnsupportedOperation", err)
	}
}

// TestAppendTextEnforcesInworldCharacterCeiling: `text` is capped at 2,000
// characters per request. Catching it locally turns a wasted round trip into a
// classified error, and the count is over runes because the cap is characters.
func TestAppendTextEnforcesInworldCharacterCeiling(t *testing.T) {
	t.Parallel()

	stream := openStream(t, "https://api.inworld.ai/tts/v1/voice:stream", nil)
	defer func() { _ = stream.Abort(context.Background()) }()

	if err := stream.AppendText(context.Background(), strings.Repeat("é", maxInputCharacters)); err != nil {
		t.Fatalf("2000 multi-byte characters must fit: %v", err)
	}
	err := stream.AppendText(context.Background(), "!")
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "input_too_large" {
		t.Fatalf("append past the ceiling = %v, want an input_too_large provider error", err)
	}
	if err := stream.CommitText(context.Background()); err == nil {
		t.Fatal("commit must fail without a reachable endpoint")
	}
}

// TestCommitTextRequiresBufferedText: AppendText is what fills the request body,
// so committing nothing would POST an empty `text` and be rejected upstream.
func TestCommitTextRequiresBufferedText(t *testing.T) {
	t.Parallel()

	stream := openStream(t, "https://api.inworld.ai/tts/v1/voice:stream", nil)
	defer func() { _ = stream.Abort(context.Background()) }()
	if err := stream.CommitText(context.Background()); err == nil || !strings.Contains(err.Error(), "no buffered text") {
		t.Fatalf("commit with an empty buffer = %v", err)
	}
	if err := stream.AppendText(context.Background(), ""); err == nil {
		t.Fatal("appending an empty fragment must fail")
	}
}

// TestSuccessWithoutAudioIsAFailure: a 200 that yields no audio is a failed
// synthesis wearing a success status. Reporting it keeps failure and success
// distinguishable to the caller.
func TestSuccessWithoutAudioIsAFailure(t *testing.T) {
	t.Parallel()

	server := newStreamServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeMessages(t, w, usageMessage(0))
	})
	defer server.Close()

	stream := openStream(t, server.URL, nil)
	defer func() { _ = stream.Abort(context.Background()) }()
	synthesize(t, stream, "Hello")

	event := next(t, stream.Events())
	var providerErr *runtimepkg.ProviderError
	if !errors.As(event.Err, &providerErr) || providerErr.Code != "provider_unavailable" {
		t.Fatalf("terminal event = %+v, want a provider_unavailable error", event)
	}
}

// TestSequentialUtterancesReuseTheStream: one utterance is one request, and the
// stream must be reusable for the next one.
func TestSequentialUtterancesReuseTheStream(t *testing.T) {
	t.Parallel()

	texts := make(chan string, 2)
	server := newStreamServer(t, func(w http.ResponseWriter, r *http.Request) {
		var body synthesizeRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		texts <- body.Text
		writeMessages(t, w, audioMessage(pcm(1)))
	})
	defer server.Close()

	stream := openStream(t, server.URL, nil)
	defer func() { _ = stream.Abort(context.Background()) }()

	for _, text := range []string{"first", "second"} {
		synthesize(t, stream, text)
		if got := eventTypes(collect(t, stream.Events(), 3)); got != "audio.started,audio.frame,audio.done" {
			t.Fatalf("%s utterance events = %s", text, got)
		}
		if got := <-texts; got != text {
			t.Fatalf("upstream text = %q, want %q", got, text)
		}
	}
}

// --- helpers ---------------------------------------------------------------

// newStreamServer stands in for api.inworld.ai. It asserts the resource path so
// a misrouted request fails loudly instead of hitting a catch-all handler, and
// flushes each write so chunk boundaries survive to the client.
func newStreamServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != streamPath {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		handler(w, r)
	}))
}

// writeMessages emits newline-delimited JSON objects and flushes, which is the
// framing Inworld's own streaming examples parse.
func writeMessages(t *testing.T, w http.ResponseWriter, messages ...json.RawMessage) {
	t.Helper()
	for _, message := range messages {
		if _, err := w.Write(append(append([]byte(nil), message...), '\n')); err != nil {
			t.Errorf("write message: %v", err)
			return
		}
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}
}

func audioMessage(audio []byte) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"result":{"audioContent":%q}}`, base64.StdEncoding.EncodeToString(audio)))
}

func usageMessage(characters int) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"result":{"audioContent":"","usage":{"processedCharactersCount":%d,"modelId":"inworld-tts-2"}}}`, characters))
}

func pcm(samples ...byte) []byte { return samples }

// withWAVHeader wraps PCM in the canonical 44-byte RIFF/WAVE container Inworld
// prefixes to every LINEAR16 streaming chunk.
func withWAVHeader(audio []byte) []byte {
	header := make([]byte, 0, 44+len(audio))
	header = append(header, "RIFF"...)
	header = append(header, 0, 0, 0, 0)
	header = append(header, "WAVE"...)
	header = append(header, "fmt "...)
	header = append(header, 16, 0, 0, 0)
	header = append(header, make([]byte, 16)...)
	header = append(header, "data"...)
	header = append(header, byte(len(audio)), 0, 0, 0)
	return append(header, audio...)
}

// testStream is the contract the runtime uses for a provider that can be torn
// down after a terminal failure: the standard stream plus the optional
// AbortingProviderStream. Asserting it here keeps every test honest that this
// adapter actually implements both.
type testStream interface {
	runtimepkg.ProviderStream
	runtimepkg.AbortingProviderStream
}

func openStream(t *testing.T, serverURL string, mutate func(*runtimepkg.AdapterRequest)) testStream {
	t.Helper()
	endpoint := serverURL
	allowedHost := officialAPIHost
	if parsed, err := url.Parse(serverURL); err == nil && parsed.Host != "" && parsed.Path == "" {
		parsed.Path = streamPath
		endpoint = parsed.String()
		allowedHost = parsed.Hostname()
	}
	adapter, err := New(Config{AllowedEndpointHosts: []string{allowedHost}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	request := adapterRequest(endpoint)
	if mutate != nil {
		mutate(&request)
	}
	opened, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	stream, ok := opened.(testStream)
	if !ok {
		t.Fatalf("adapter stream %T does not implement AbortingProviderStream", opened)
	}
	return stream
}

func synthesize(t *testing.T, stream runtimepkg.ProviderStream, text string) {
	t.Helper()
	if err := stream.AppendText(context.Background(), text); err != nil {
		t.Fatalf("append text: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit text: %v", err)
	}
}

func adapterRequest(endpoint string) runtimepkg.AdapterRequest {
	now := time.Date(2026, time.August, 1, 11, 59, 0, 0, time.UTC)
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindTTS,
		Plan: protocol.SessionPlan{
			PlanID: "plan_inworld", SessionID: "sess_inworld", AttemptID: "att_1",
			Execution: protocol.Execution{Placement: protocol.PlacementEmbedded, ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK},
			ExpiresAt: now.Add(time.Hour),
			Route: protocol.PlanRoute{
				Provider: "inworld", Model: DefaultModel, Adapter: AdapterID,
				Transport: protocol.TransportHTTP, Endpoint: endpoint,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "customer-inworld-key", ExpiresAt: now.Add(30 * time.Minute)},
			},
			Reservation: protocol.Reservation{
				ID: "res_inworld", LeaseDurationSeconds: 60, LeaseExpiresAt: now.Add(time.Minute),
				Concurrency: protocol.ConcurrencyReservation{LeaseID: "conc_inworld", Slots: 1},
				Usage:       protocol.UsageReservation{Unit: protocol.UsageUnitCharacters, AuthorizedUnits: 4_000},
			},
			Requirements: protocol.Requirements{Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision, RuntimeVersion: "0.1.0"},
			Signature:    "test-signature",
		},
		Options: protocol.RequestOptions{Voice: "Dennis", Language: "en-US", MaxInputCharacters: 4_000},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1},
	}
}

func collect(t *testing.T, events <-chan runtimepkg.ProviderEvent, want int) []runtimepkg.ProviderEvent {
	t.Helper()
	collected := make([]runtimepkg.ProviderEvent, 0, want)
	for len(collected) < want {
		collected = append(collected, next(t, events))
	}
	return collected
}

func next(t *testing.T, events <-chan runtimepkg.ProviderEvent) runtimepkg.ProviderEvent {
	t.Helper()
	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("provider events closed early")
		}
		return event
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for a provider event")
		return runtimepkg.ProviderEvent{}
	}
}

func drain(t *testing.T, events <-chan runtimepkg.ProviderEvent) {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		select {
		case _, ok := <-events:
			if !ok {
				return
			}
		case <-timer.C:
			t.Fatal("provider events never closed")
		}
	}
}

func eventTypes(events []runtimepkg.ProviderEvent) string {
	types := make([]string, len(events))
	for index, event := range events {
		types[index] = string(event.Type)
	}
	return strings.Join(types, ",")
}
