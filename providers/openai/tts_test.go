package openai

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

// TestTTSCommitTextSendsDocumentedRequest pins every wire detail of the
// synthesis call. The literals are transcribed from OpenAI's CreateSpeechRequest
// schema rather than referenced from package constants, so a misspelled field
// name or enum value fails here rather than shipping as an upstream 400.
func TestTTSCommitTextSendsDocumentedRequest(t *testing.T) {
	t.Parallel()

	type observed struct {
		method string
		path   string
		query  string
		header http.Header
		body   []byte
	}
	requests := make(chan observed, 1)
	server := newSpeechServer(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		requests <- observed{method: r.Method, path: r.URL.Path, query: r.URL.RawQuery, header: r.Header.Clone(), body: body}
		_, _ = w.Write([]byte{1, 2, 3, 4})
	})
	defer server.Close()

	stream := openTTS(t, server.URL, nil, nil)
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
	if got.path != "/v1/audio/speech" {
		t.Errorf("path = %q, want /v1/audio/speech", got.path)
	}
	if got.query != "" {
		t.Errorf("query = %q, want none: every parameter belongs in the JSON body", got.query)
	}
	if want := "Bearer customer-openai-key"; got.header.Get("Authorization") != want {
		t.Errorf("Authorization = %q, want %q", got.header.Get("Authorization"), want)
	}
	if got.header.Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type = %q", got.header.Get("Content-Type"))
	}

	// Compared as decoded JSON so the assertion is about field NAMES and values.
	var body map[string]any
	if err := json.Unmarshal(got.body, &body); err != nil {
		t.Fatalf("request body is not JSON: %v (%s)", err, got.body)
	}
	want := map[string]any{
		"model": "gpt-4o-mini-tts",
		// The fragments were concatenated, not sent separately: this endpoint
		// takes one whole utterance per request.
		"input": "Hello, world!",
		"voice": "ash",
		// `pcm` is documented as raw 24 kHz 16-bit signed little-endian with no
		// header — the one format that needs no container stripping.
		"response_format": "pcm",
		// `audio` streams the samples themselves under chunked transfer encoding;
		// `sse` would base64-inflate them and is rejected for tts-1/tts-1-hd.
		"stream_format": "audio",
	}
	if !reflect.DeepEqual(body, want) {
		t.Fatalf("request body =\n%#v\nwant\n%#v", body, want)
	}
}

// TestTTSRelayRouteSendsTheConnectorKeyAsBearer: a relay plan is managed for
// billing purposes but carries the relay connector's permanent OpenAI key.
// OpenAI has exactly one credential channel, so the key travels in the same
// Authorization: Bearer header as every other source and never in the URL.
// Both credential-kind spellings must synthesize, because protocol.SessionPlan
// validation labels a relay credential relay_access while the relay connector
// that synthesizes plans and drives this adapter directly labels the same
// permanent key bearer.
func TestTTSRelayRouteSendsTheConnectorKeyAsBearer(t *testing.T) {
	t.Parallel()

	for _, kind := range []protocol.CredentialKind{protocol.CredentialBearer, protocol.CredentialRelayAccess} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			requests := make(chan *http.Request, 1)
			server := newSpeechServer(t, func(w http.ResponseWriter, r *http.Request) {
				requests <- r.Clone(r.Context())
				_, _ = w.Write([]byte{1, 2, 3, 4})
			})
			defer server.Close()

			stream := openTTS(t, server.URL, nil, func(request *runtimepkg.AdapterRequest) {
				request.Plan.Execution.ProviderRoute = protocol.RouteSpekoRelay
				request.Plan.Execution.CredentialSource = protocol.CredentialsManaged
				request.Plan.Route.Credential.Kind = kind
				request.Plan.Route.Credential.Value = "connector-openai-key"
			})
			defer func() { _ = stream.Abort(context.Background()) }()
			synthesizeTTS(t, stream, "Hello")

			select {
			case received := <-requests:
				if got := received.Header.Get("Authorization"); got != "Bearer connector-openai-key" {
					t.Errorf("Authorization = %q", got)
				}
				if received.URL.RawQuery != "" {
					t.Errorf("relay request query = %q, want none", received.URL.RawQuery)
				}
			case <-time.After(time.Second):
				t.Fatal("server never observed the synthesis request")
			}
		})
	}
}

// TestTTSStreamsFramesBeforeSynthesisCompletes is the streaming contract. The
// server holds the response open after the first chunk and only continues once
// the adapter has already surfaced a frame, so a buffer-the-whole-body
// implementation deadlocks here instead of quietly passing.
func TestTTSStreamsFramesBeforeSynthesisCompletes(t *testing.T) {
	t.Parallel()

	firstFrameSeen := make(chan struct{})
	server := newSpeechServer(t, func(w http.ResponseWriter, _ *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("test server response does not support flushing")
			return
		}
		_, _ = w.Write([]byte{1, 2, 3, 4})
		flusher.Flush()
		select {
		case <-firstFrameSeen:
		case <-time.After(3 * time.Second):
			t.Error("adapter never emitted a frame from the first flush")
			return
		}
		_, _ = w.Write([]byte{5, 6, 7, 8})
		flusher.Flush()
		_, _ = w.Write([]byte{9, 10, 11, 12})
	})
	defer server.Close()

	// A 4-byte read window makes the frame count deterministic.
	stream := openTTS(t, server.URL, func(config *TTSConfig) { config.AudioChunkBytes = 4 }, nil)
	defer func() { _ = stream.Abort(context.Background()) }()

	synthesizeTTS(t, stream, "Hello, world!")

	started := nextTTS(t, stream.Events())
	if started.Type != protocol.EventAudioStarted {
		t.Fatalf("first event = %q, want audio.started", started.Type)
	}
	first := nextTTS(t, stream.Events())
	if first.Type != protocol.EventAudioFrame || !reflect.DeepEqual(first.Audio, []byte{1, 2, 3, 4}) {
		t.Fatalf("first frame = %q/%v", first.Type, first.Audio)
	}
	close(firstFrameSeen)

	second := nextTTS(t, stream.Events())
	third := nextTTS(t, stream.Events())
	if second.Type != protocol.EventAudioFrame || third.Type != protocol.EventAudioFrame {
		t.Fatalf("later events = %q, %q, want two more audio frames", second.Type, third.Type)
	}
	// Each frame owns its bytes: the adapter reuses one read buffer, so a missing
	// copy would make every frame alias the last chunk read.
	if !reflect.DeepEqual(first.Audio, []byte{1, 2, 3, 4}) {
		t.Fatalf("the first frame was overwritten by a later read: %v", first.Audio)
	}
	if !reflect.DeepEqual(second.Audio, []byte{5, 6, 7, 8}) || !reflect.DeepEqual(third.Audio, []byte{9, 10, 11, 12}) {
		t.Fatalf("frames = %v, %v", second.Audio, third.Audio)
	}

	done := nextTTS(t, stream.Events())
	if done.Type != protocol.EventAudioDone {
		t.Fatalf("last event = %q, want audio.done", done.Type)
	}
	// Every event of one utterance carries the same correlation id.
	if utteranceID(t, started) == "" || utteranceID(t, started) != utteranceID(t, done) {
		t.Fatalf("utterance ids differ: %q vs %q", utteranceID(t, started), utteranceID(t, done))
	}
}

// TestTTSClassifiesRejections keeps a dead key, an exhausted balance, a
// throttle, a bad request, and a vendor fault distinguishable. OpenAI returns
// 429 for BOTH an exhausted balance and a throttle, so the status alone is not
// enough and the error body has to be consulted.
func TestTTSClassifiesRejections(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		status  int
		body    string
		code    string
		retries bool
		// rawKept records that the provider's own JSON body must survive on the
		// canonical error under the vendor extension namespace, which is the only
		// place a consumer can recover the vendor's wording and param.
		rawKept bool
	}{
		{
			name:    "revoked key",
			status:  http.StatusUnauthorized,
			body:    `{"error":{"message":"Incorrect API key provided.","type":"invalid_request_error","param":null,"code":"invalid_api_key"}}`,
			code:    "authentication_failed",
			rawKept: true,
		},
		{
			name:    "exhausted balance behind a 429",
			status:  http.StatusTooManyRequests,
			body:    `{"error":{"message":"You exceeded your current quota.","type":"insufficient_quota","param":null,"code":"insufficient_quota"}}`,
			code:    "provider_quota_exceeded",
			rawKept: true,
		},
		{
			name:    "throttle behind the same 429",
			status:  http.StatusTooManyRequests,
			body:    `{"error":{"message":"Rate limit reached.","type":"rate_limit_error","param":null,"code":"rate_limit_exceeded"}}`,
			code:    "provider_rate_limited",
			retries: true,
			rawKept: true,
		},
		{
			name:    "bad parameter",
			status:  http.StatusBadRequest,
			body:    `{"error":{"message":"Invalid value for 'voice'.","type":"invalid_request_error","param":"voice","code":null}}`,
			code:    "invalid_request",
			rawKept: true,
		},
		{
			name:    "vendor fault",
			status:  http.StatusServiceUnavailable,
			body:    `{"error":{"message":"The server is overloaded.","type":"server_error","param":null,"code":null}}`,
			code:    "provider_unavailable",
			retries: true,
			rawKept: true,
		},
		{
			name:    "vendor fault with no parseable body",
			status:  http.StatusBadGateway,
			body:    `<html>502 Bad Gateway</html>`,
			code:    "provider_unavailable",
			retries: true,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			server := newSpeechServer(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(testCase.status)
				_, _ = w.Write([]byte(testCase.body))
			})
			defer server.Close()

			stream := openTTS(t, server.URL, nil, nil)
			defer func() { _ = stream.Abort(context.Background()) }()

			if err := stream.AppendText(context.Background(), "Hello"); err != nil {
				t.Fatalf("append text: %v", err)
			}
			err := stream.CommitText(context.Background())
			providerErr, ok := err.(*runtimepkg.ProviderError)
			if !ok {
				t.Fatalf("error = %T (%v), want *runtime.ProviderError", err, err)
			}
			if providerErr.Code != testCase.code {
				t.Fatalf("code = %q, want %q", providerErr.Code, testCase.code)
			}
			if providerErr.Retryable != testCase.retries {
				t.Fatalf("retryable = %v, want %v", providerErr.Retryable, testCase.retries)
			}
			if providerErr.ProviderStatus != testCase.status {
				t.Fatalf("provider status = %d, want %d", providerErr.ProviderStatus, testCase.status)
			}
			// The synthesized text can be echoed in a provider message, so the
			// canonical Message must stay generic and the raw body live in
			// Extensions instead.
			if strings.Contains(providerErr.Message, "Hello") {
				t.Fatalf("error message leaked the synthesis input: %q", providerErr.Message)
			}
			raw := providerErr.Extensions["openai.com/audio/speech/v1"]
			if testCase.rawKept != (raw != nil) {
				t.Fatalf("raw provider payload present = %v, want %v", raw != nil, testCase.rawKept)
			}
			if testCase.rawKept && string(raw) != testCase.body {
				t.Fatalf("raw payload = %s, want it byte-identical to the provider body", raw)
			}
		})
	}
}

// TestTTSReportsSuccessWithoutAudio: a 200 that returns nothing is a failed
// synthesis wearing a success status, and would otherwise look like a very short
// utterance.
func TestTTSReportsSuccessWithoutAudio(t *testing.T) {
	t.Parallel()

	server := newSpeechServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	defer server.Close()

	stream := openTTS(t, server.URL, nil, nil)
	defer func() { _ = stream.Abort(context.Background()) }()
	synthesizeTTS(t, stream, "Hello")

	event := nextTTS(t, stream.Events())
	if event.Err == nil {
		t.Fatalf("event = %q, want a terminal error", event.Type)
	}
	providerErr, ok := event.Err.(*runtimepkg.ProviderError)
	if !ok || providerErr.Code != "provider_unavailable" {
		t.Fatalf("error = %#v", event.Err)
	}
}

// TestTTSRejectsInputOverCharacterLimit counts RUNES, not bytes: the vendor caps
// `input` at 4096 characters, so a byte count would reject valid non-Latin text
// at roughly a third of the real limit.
func TestTTSRejectsInputOverCharacterLimit(t *testing.T) {
	t.Parallel()

	server := newSpeechServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte{1, 2})
	})
	defer server.Close()

	stream := openTTS(t, server.URL, nil, nil)
	defer func() { _ = stream.Abort(context.Background()) }()

	// 4096 three-byte runes: 4096 characters, 12288 bytes. Accepted.
	if err := stream.AppendText(context.Background(), strings.Repeat("あ", 4_096)); err != nil {
		t.Fatalf("4096 characters must be accepted: %v", err)
	}
	err := stream.AppendText(context.Background(), "あ")
	providerErr, ok := err.(*runtimepkg.ProviderError)
	if !ok || providerErr.Code != "input_too_large" {
		t.Fatalf("error = %#v, want an input_too_large ProviderError", err)
	}
}

// TestTTSVoicePrecedence: a caller sending `provider: "auto"` cannot know which
// vendor it will get, so the control plane's choice has to be usable, and
// `voice` is a REQUIRED field so some value must always exist.
func TestTTSVoicePrecedence(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		requestVoice  string
		planVoice     string
		wantVoiceSent string
	}{
		{name: "caller wins", requestVoice: "ash", planVoice: "sage", wantVoiceSent: "ash"},
		{name: "control plane fills in", requestVoice: "", planVoice: "sage", wantVoiceSent: "sage"},
		{name: "neither named one", requestVoice: "", planVoice: "", wantVoiceSent: "coral"},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			bodies := make(chan []byte, 1)
			server := newSpeechServer(t, func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				bodies <- body
				_, _ = w.Write([]byte{1, 2})
			})
			defer server.Close()

			stream := openTTS(t, server.URL, nil, func(request *runtimepkg.AdapterRequest) {
				request.Options.Voice = testCase.requestVoice
				request.Plan.Route.Voice = testCase.planVoice
			})
			defer func() { _ = stream.Abort(context.Background()) }()
			synthesizeTTS(t, stream, "Hello")

			var body struct {
				Voice string `json:"voice"`
			}
			if err := json.Unmarshal(<-bodies, &body); err != nil {
				t.Fatalf("request body is not JSON: %v", err)
			}
			if body.Voice != testCase.wantVoiceSent {
				t.Fatalf("voice = %q, want %q", body.Voice, testCase.wantVoiceSent)
			}
		})
	}
}

// TestTTSRejectsMismatchedPlans covers every way a plan can be wrong for this
// adapter, so a misrouted session fails at Open where the runtime can fail over.
func TestTTSRejectsMismatchedPlans(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		mutate  func(*runtimepkg.AdapterRequest)
		wantSub string
	}{
		{
			name:    "wrong kind",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Kind = protocol.SessionKindSTT },
			wantSub: "tts sessions",
		},
		{
			name:    "another vendor's plan",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Provider = "elevenlabs" },
			wantSub: "cannot open provider",
		},
		{
			// OpenAI publishes no streaming-TTS socket, so a websocket route means
			// the control plane picked a transport that does not exist.
			name:    "websocket transport",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Transport = protocol.TransportWebSocket },
			wantSub: "http transport",
		},
		{
			name:    "unresolved model",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "auto" },
			wantSub: "concrete model",
		},
		{
			name:    "a transcription model on the synthesis endpoint",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "gpt-4o-transcribe" },
			wantSub: "does not support model",
		},
		{
			name: "non-bearer credential",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Plan.Route.Credential = &protocol.DelegatedCredential{Kind: protocol.CredentialSessionURL, Value: "secret-that-must-not-leak"}
			},
			wantSub: "bearer credential",
		},
		{
			// relay_access is protocol.SessionPlan.Validate's label for a relay
			// credential; the same validation forbids it on provider_direct, so
			// the adapter refuses it there rather than treating it as a bearer
			// synonym.
			name: "relay_access credential off the relay route",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Plan.Route.Credential = &protocol.DelegatedCredential{Kind: protocol.CredentialRelayAccess, Value: "secret-that-must-not-leak"}
			},
			wantSub: "bearer credential",
		},
		{
			name:    "missing credential",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential = nil },
			wantSub: "bearer credential",
		},
		{
			name:    "no media",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Media = nil },
			wantSub: "media configuration",
		},
		{
			name: "stereo output",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Media = &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 2}
			},
			wantSub: "mono pcm_s16le",
		},
		{
			// The vendor's `opus` is Ogg-encapsulated, not the raw Opus this
			// protocol's `opus` encoding means.
			name: "opus output",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Media = &protocol.MediaFormat{Encoding: "opus", SampleRateHz: 24_000, Channels: 1}
			},
			wantSub: "mono pcm_s16le",
		},
		{
			// `response_format: "pcm"` is documented as 24 kHz and the endpoint has
			// no rate parameter, so any other rate would mislabel the samples.
			name: "16 kHz output",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Media = &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1}
			},
			wantSub: "emits 24000 Hz audio",
		},
		{
			name:    "endpoint on another path",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = "http://127.0.0.1:1/v1/realtime" },
			wantSub: "endpoint path must be /v1/audio/speech",
		},
		{
			name: "endpoint with a preexisting query",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Plan.Route.Endpoint = "http://127.0.0.1:1/v1/audio/speech?api-key=leak"
			},
			wantSub: "clean absolute HTTPS URL",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			adapter, err := NewTTS(TTSConfig{AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			request := ttsAdapterRequest("http://127.0.0.1:1/v1/audio/speech")
			testCase.mutate(&request)
			_, err = adapter.Open(context.Background(), request)
			if err == nil {
				t.Fatal("expected the plan to be rejected")
			}
			if !strings.Contains(err.Error(), testCase.wantSub) {
				t.Fatalf("error = %q, want it to mention %q", err, testCase.wantSub)
			}
			if strings.Contains(err.Error(), "secret-that-must-not-leak") {
				t.Fatalf("rejection leaked the credential: %v", err)
			}
		})
	}
}

// TestTTSRejectsTranscriptionOperations: a synthesizer that silently ignored
// caller audio would leave the runtime unable to report the mismatch.
func TestTTSRejectsTranscriptionOperations(t *testing.T) {
	t.Parallel()

	server := newSpeechServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte{1})
	})
	defer server.Close()

	stream := openTTS(t, server.URL, nil, nil)
	defer func() { _ = stream.Abort(context.Background()) }()

	if err := stream.WriteAudio(context.Background(), []byte{1}); err != runtimepkg.ErrUnsupportedOperation {
		t.Fatalf("WriteAudio = %v", err)
	}
	if err := stream.CommitAudio(context.Background()); err != runtimepkg.ErrUnsupportedOperation {
		t.Fatalf("CommitAudio = %v", err)
	}
}

// TestTTSCancelStopsSynthesisWithoutReportingAFault: HTTP has no cancel message,
// so cancelling drops the connection. The resulting read error is the caller's
// own doing and must not surface as a provider failure.
func TestTTSCancelStopsSynthesisWithoutReportingAFault(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	server := newSpeechServer(t, func(w http.ResponseWriter, _ *http.Request) {
		flusher, _ := w.(http.Flusher)
		_, _ = w.Write([]byte{1, 2, 3, 4})
		if flusher != nil {
			flusher.Flush()
		}
		<-release
	})
	defer server.Close()
	defer close(release)

	stream := openTTS(t, server.URL, func(config *TTSConfig) { config.AudioChunkBytes = 4 }, nil)
	defer func() { _ = stream.Abort(context.Background()) }()
	synthesizeTTS(t, stream, "Hello")

	if event := nextTTS(t, stream.Events()); event.Type != protocol.EventAudioStarted {
		t.Fatalf("first event = %q", event.Type)
	}
	if event := nextTTS(t, stream.Events()); event.Type != protocol.EventAudioFrame {
		t.Fatalf("second event = %q", event.Type)
	}
	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	select {
	case event, ok := <-stream.Events():
		if ok {
			t.Fatalf("cancel produced %q (err=%v); it is not a provider fault", event.Type, event.Err)
		}
	case <-time.After(200 * time.Millisecond):
		// No event is the correct outcome: the stream is idle, not failed.
	}
}

// --- helpers -------------------------------------------------------------

func newSpeechServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/audio/speech" {
			http.NotFound(w, r)
			return
		}
		handler(w, r)
	}))
}

type ttsTestStream interface {
	runtimepkg.ProviderStream
	runtimepkg.AbortingProviderStream
}

func openTTS(
	t *testing.T,
	serverURL string,
	configure func(*TTSConfig),
	mutate func(*runtimepkg.AdapterRequest),
) ttsTestStream {
	t.Helper()
	endpoint, _ := url.Parse(serverURL)
	config := TTSConfig{AllowedEndpointHosts: []string{endpoint.Hostname()}, AllowInsecureEndpoint: true}
	if configure != nil {
		configure(&config)
	}
	adapter, err := NewTTS(config)
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	endpoint.Path = "/v1/audio/speech"
	request := ttsAdapterRequest(endpoint.String())
	if mutate != nil {
		mutate(&request)
	}
	opened, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	stream, ok := opened.(ttsTestStream)
	if !ok {
		t.Fatalf("adapter stream %T does not implement AbortingProviderStream", opened)
	}
	return stream
}

func ttsAdapterRequest(endpoint string) runtimepkg.AdapterRequest {
	now := time.Date(2026, time.August, 1, 11, 59, 0, 0, time.UTC)
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindTTS,
		Plan: protocol.SessionPlan{
			PlanID: "plan_openai_tts", SessionID: "sess_openai_tts", AttemptID: "att_1",
			Execution: protocol.Execution{Placement: protocol.PlacementEmbedded, ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK},
			ExpiresAt: now.Add(time.Hour),
			Route: protocol.PlanRoute{
				Provider: "openai", Model: "gpt-4o-mini-tts", Adapter: TTSAdapterID,
				Transport: protocol.TransportHTTP, Endpoint: endpoint,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "customer-openai-key", ExpiresAt: now.Add(30 * time.Minute)},
			},
			Reservation: protocol.Reservation{
				ID: "res_openai_tts", LeaseDurationSeconds: 60, LeaseExpiresAt: now.Add(time.Minute),
				Concurrency: protocol.ConcurrencyReservation{LeaseID: "conc_openai_tts", Slots: 1},
				Usage:       protocol.UsageReservation{Unit: protocol.UsageUnitCharacters, AuthorizedUnits: 4_096},
			},
			Requirements: protocol.Requirements{Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision, RuntimeVersion: "0.1.0"},
			Signature:    "test-signature",
		},
		Options: protocol.RequestOptions{Voice: "ash", Language: "en-US", MaxInputCharacters: 4_096},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1},
	}
}

func synthesizeTTS(t *testing.T, stream runtimepkg.ProviderStream, text string) {
	t.Helper()
	if err := stream.AppendText(context.Background(), text); err != nil {
		t.Fatalf("append text: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit text: %v", err)
	}
}

func nextTTS(t *testing.T, events <-chan runtimepkg.ProviderEvent) runtimepkg.ProviderEvent {
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

func utteranceID(t *testing.T, event runtimepkg.ProviderEvent) string {
	t.Helper()
	var data struct {
		UtteranceID string `json:"utterance_id"`
	}
	if err := json.Unmarshal(event.Data, &data); err != nil {
		t.Fatalf("decode utterance data: %v", err)
	}
	return data.UtteranceID
}
