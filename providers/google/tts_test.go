package google

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
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

// TestSynthesizeUsesDocumentedRESTContract pins every wire detail that a unit
// test can pin and that a production incident could not: the method, the path,
// the auth channel, and the exact nesting of input/voice/audioConfig. The
// previous port of this provider shipped a wrong parameter name and an
// unaccepted audioEncoding, neither of which any behavioural test noticed —
// so the body is compared as a whole document, not field by field.
func TestSynthesizeUsesDocumentedRESTContract(t *testing.T) {
	t.Parallel()
	pcm := samplePCM(2048)
	observed := make(chan *http.Request, 1)
	bodies := make(chan []byte, 1)
	server := newSynthesizeServer(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		observed <- r.Clone(r.Context())
		bodies <- body
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		writeAudioResponse(t, w, wavContainer(pcm, 24_000), 0, nil)
	})
	defer server.Close()

	stream := openStream(t, server, protocol.CredentialsBYOK, nil)
	speak(t, stream, "नमस्ते")

	events := collectEvents(t, stream.Events(), 3)
	if got := strings.Join(eventTypes(events), ","); got != "audio.started,audio.frame,audio.done" {
		t.Fatalf("event order = %s", got)
	}
	// The wire carries base64 inside JSON. Asserting the DECODED bytes is the
	// whole point: emitting the encoded string would still "work" end to end
	// and produce nothing but noise in a speaker.
	if got := concatAudio(events); !bytes.Equal(got, pcm) {
		t.Fatalf("decoded audio = %d bytes, want the %d PCM bytes the server encoded", len(got), len(pcm))
	}

	request := <-observed
	if request.Method != http.MethodPost {
		t.Fatalf("method = %s, want POST", request.Method)
	}
	if request.URL.Path != "/v1/text:synthesize" {
		t.Fatalf("path = %q, want /v1/text:synthesize", request.URL.Path)
	}
	if got := request.Header.Get("Content-Type"); got != "application/json; charset=utf-8" {
		t.Fatalf("content-type = %q", got)
	}

	var got map[string]any
	if err := json.Unmarshal(<-bodies, &got); err != nil {
		t.Fatalf("request body is not JSON: %v", err)
	}
	want := map[string]any{
		"input": map[string]any{"text": "नमस्ते"},
		"voice": map[string]any{"languageCode": "hi-IN", "name": "hi-IN-Chirp3-HD-Charon"},
		// audioConfig is a TOP-LEVEL sibling of input and voice, the encoding is
		// the exact string LINEAR16, and the sample-rate key is sampleRateHertz
		// (not sample_rate, sampleRate, or sampleRateHz).
		"audioConfig": map[string]any{"audioEncoding": "LINEAR16", "sampleRateHertz": float64(24_000)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("request body =\n%#v\nwant\n%#v", got, want)
	}
	closeStream(t, stream)
}

// TestStreamsFramesWhileResponseIsStillArriving proves the adapter is
// incremental rather than buffer-then-chunk. The handler deliberately withholds
// the tail of the JSON body until the client has already published an audio
// frame; if the adapter waited for the whole response, this deadlocks and the
// test times out.
func TestStreamsFramesWhileResponseIsStillArriving(t *testing.T) {
	t.Parallel()
	pcm := samplePCM(8192)
	encoded := base64.StdEncoding.EncodeToString(wavContainer(pcm, 24_000))
	split := (len(encoded) * 3 / 4) &^ 3
	firstFrameSeen := make(chan struct{})

	server := newSynthesizeServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("test server response writer cannot flush")
			return
		}
		_, _ = io.WriteString(w, `{"audioContent":"`+encoded[:split])
		flusher.Flush()
		select {
		case <-firstFrameSeen:
		case <-time.After(3 * time.Second):
			t.Error("adapter buffered the whole body instead of streaming it")
		}
		_, _ = io.WriteString(w, encoded[split:]+`"}`)
	})
	defer server.Close()

	stream := openStream(t, server, protocol.CredentialsBYOK, func(config *Config) {
		config.MaxFrameBytes = 1024
	})
	speak(t, stream, "stream me")

	var (
		frames   [][]byte
		signaled bool
		done     bool
	)
	deadline := time.After(5 * time.Second)
	for !done {
		select {
		case event, ok := <-stream.Events():
			if !ok {
				t.Fatal("events closed before audio.done")
			}
			if event.Err != nil {
				t.Fatalf("provider error: %v", event.Err)
			}
			switch event.Type {
			case protocol.EventAudioFrame:
				frames = append(frames, event.Audio)
				if !signaled {
					signaled = true
					close(firstFrameSeen)
				}
			case protocol.EventAudioDone:
				done = true
			}
		case <-deadline:
			t.Fatal("timed out waiting for audio.done")
		}
	}
	// Multiple frames must precede audio.done, and every one of them must be an
	// even number of bytes so an s16le sample is never split.
	if len(frames) < 2 {
		t.Fatalf("frame count = %d, want more than one frame before audio.done", len(frames))
	}
	var assembled []byte
	for _, frame := range frames {
		if len(frame)%2 != 0 {
			t.Fatalf("frame of %d bytes splits an s16le sample", len(frame))
		}
		if len(frame) > 1024 {
			t.Fatalf("frame of %d bytes exceeds the configured cap", len(frame))
		}
		assembled = append(assembled, frame...)
	}
	if !bytes.Equal(assembled, pcm) {
		t.Fatalf("reassembled audio = %d bytes, want %d", len(assembled), len(pcm))
	}
	closeStream(t, stream)
}

// TestLanguageCodeIsRegionQualified guards the field that made Hindi, Tamil and
// Telugu unreachable in the first place: Google needs hi-IN, not hi, and the
// curated Chirp voices carry their own locale prefix.
func TestLanguageCodeIsRegionQualified(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name     string
		voice    string
		language string
		want     string
	}{
		{name: "hindi voice supplies the region", voice: "hi-IN-Chirp3-HD-Charon", language: "hi", want: "hi-IN"},
		{name: "tamil voice with no language option", voice: "ta-IN-Chirp3-HD-Kore", language: "", want: "ta-IN"},
		{name: "telugu full tag agrees with the voice", voice: "te-IN-Chirp3-HD-Leda", language: "te-IN", want: "te-IN"},
		// Google's own docs show a lowercase locale in one sample; the canonical
		// tag is normalized so logs and comparisons stay stable.
		{name: "lowercase locale is canonicalized", voice: "en-us-Chirp3-HD-Leda", language: "", want: "en-US"},
		// UN M.49 numeric regions ("es-419") must survive uppercasing untouched.
		{name: "numeric region is preserved", voice: "es-419-Chirp3-HD-Puck", language: "", want: "es-419"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			bodies := make(chan []byte, 1)
			server := newSynthesizeServer(t, func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				bodies <- body
				writeAudioResponse(t, w, wavContainer(samplePCM(64), 24_000), 0, nil)
			})
			defer server.Close()

			stream := openStreamWith(t, server, protocol.CredentialsBYOK, nil, func(request *runtimepkg.AdapterRequest) {
				request.Options.Voice = testCase.voice
				request.Options.Language = testCase.language
			})
			speak(t, stream, "hello")
			collectEvents(t, stream.Events(), 3)

			var body struct {
				Voice struct {
					LanguageCode string `json:"languageCode"`
					Name         string `json:"name"`
				} `json:"voice"`
			}
			if err := json.Unmarshal(<-bodies, &body); err != nil {
				t.Fatalf("body: %v", err)
			}
			if body.Voice.LanguageCode != testCase.want {
				t.Fatalf("languageCode = %q, want %q", body.Voice.LanguageCode, testCase.want)
			}
			if body.Voice.Name != testCase.voice {
				t.Fatalf("voice name = %q, want the caller's voice verbatim", body.Voice.Name)
			}
			closeStream(t, stream)
		})
	}
}

// TestBothCredentialSourcesUseTheBearerHeader records that managed and BYOK are
// the SAME mechanism here. Google documents one header for this API, so the two
// sources differ only in who minted the token. The assertion that matters is
// the negative one: the secret must never reach the query string, where it
// would be captured by every proxy and access log on the path.
func TestBothCredentialSourcesUseTheBearerHeader(t *testing.T) {
	t.Parallel()
	for _, source := range []protocol.CredentialSource{protocol.CredentialsManaged, protocol.CredentialsBYOK} {
		t.Run(string(source), func(t *testing.T) {
			t.Parallel()
			observed := make(chan *http.Request, 1)
			server := newSynthesizeServer(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				observed <- r.Clone(r.Context())
				writeAudioResponse(t, w, wavContainer(samplePCM(64), 24_000), 0, nil)
			})
			defer server.Close()

			stream := openStream(t, server, source, nil)
			speak(t, stream, "hello")
			collectEvents(t, stream.Events(), 3)

			request := <-observed
			token := credentialFor(source)
			if got := request.Header.Get("Authorization"); got != "Bearer "+token {
				t.Fatalf("authorization = %q, want the bearer header", got)
			}
			if request.URL.RawQuery != "" {
				t.Fatalf("query string = %q, want no credential in the URL", request.URL.RawQuery)
			}
			if strings.Contains(request.URL.String(), token) {
				t.Fatal("credential leaked into the request URL")
			}
			closeStream(t, stream)
		})
	}
}

// TestQuotaProjectHeaderIsSentWhenConfigured mirrors Google's own curl example,
// which pairs the bearer token with x-goog-user-project so usage bills to the
// caller's project rather than the token's home project.
func TestQuotaProjectHeaderIsSentWhenConfigured(t *testing.T) {
	t.Parallel()
	observed := make(chan *http.Request, 1)
	server := newSynthesizeServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		observed <- r.Clone(r.Context())
		writeAudioResponse(t, w, wavContainer(samplePCM(64), 24_000), 0, nil)
	})
	defer server.Close()

	stream := openStream(t, server, protocol.CredentialsManaged, func(config *Config) {
		config.QuotaProject = "speko-voice-prod"
	})
	speak(t, stream, "hello")
	collectEvents(t, stream.Events(), 3)
	if got := (<-observed).Header.Get("X-Goog-User-Project"); got != "speko-voice-prod" {
		t.Fatalf("x-goog-user-project = %q", got)
	}
	closeStream(t, stream)
}

// TestUsageObservedCarriesGoogleRequestID exercises the correlation header. It
// is not contractual for this API, so its ABSENCE must not fabricate an event —
// that is asserted by every other success test, which sees exactly three.
func TestUsageObservedCarriesGoogleRequestID(t *testing.T) {
	t.Parallel()
	server := newSynthesizeServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("X-Goog-Request-Id", "req-abc-123")
		writeAudioResponse(t, w, wavContainer(samplePCM(64), 24_000), 0, nil)
	})
	defer server.Close()

	stream := openStream(t, server, protocol.CredentialsBYOK, nil)
	speak(t, stream, "hello")
	events := collectEvents(t, stream.Events(), 4)
	if got := strings.Join(eventTypes(events), ","); got != "usage.observed,audio.started,audio.frame,audio.done" {
		t.Fatalf("event order = %s", got)
	}
	var usage struct {
		ProviderRequestID string `json:"provider_request_id"`
	}
	if err := json.Unmarshal(events[0].Data, &usage); err != nil || usage.ProviderRequestID != "req-abc-123" {
		t.Fatalf("usage data = %s, err = %v", events[0].Data, err)
	}
	closeStream(t, stream)
}

// TestStatusMappingMatchesTheProtocolContract locks the HTTP-status-to-code
// table and keeps Google's google.rpc.Status payload on the error so a 400 can
// be diagnosed without a packet capture.
func TestStatusMappingMatchesTheProtocolContract(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		status    int
		code      string
		retryable bool
	}{
		{status: http.StatusUnauthorized, code: "authentication_failed"},
		{status: http.StatusForbidden, code: "authentication_failed"},
		{status: http.StatusTooManyRequests, code: "provider_rate_limited", retryable: true},
		{status: http.StatusInternalServerError, code: "provider_unavailable", retryable: true},
		{status: http.StatusServiceUnavailable, code: "provider_unavailable", retryable: true},
		{status: http.StatusBadRequest, code: "invalid_request"},
		{status: http.StatusNotFound, code: "invalid_request"},
	} {
		t.Run(fmt.Sprint(testCase.status), func(t *testing.T) {
			t.Parallel()
			server := newSynthesizeServer(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(testCase.status)
				_, _ = io.WriteString(w, `{"error":{"code":`+fmt.Sprint(testCase.status)+`,"message":"voice not found","status":"NOT_FOUND"}}`)
			})
			defer server.Close()

			stream := openStream(t, server, protocol.CredentialsBYOK, nil)
			speak(t, stream, "hello")

			providerErr := awaitProviderError(t, stream.Events())
			if providerErr.Code != testCase.code {
				t.Fatalf("code = %q, want %q", providerErr.Code, testCase.code)
			}
			if providerErr.Retryable != testCase.retryable {
				t.Fatalf("retryable = %v, want %v", providerErr.Retryable, testCase.retryable)
			}
			if providerErr.ProviderStatus != testCase.status {
				t.Fatalf("provider status = %d, want %d", providerErr.ProviderStatus, testCase.status)
			}
			if !strings.Contains(providerErr.Message, "voice not found") {
				t.Fatalf("message = %q, want Google's own text preserved", providerErr.Message)
			}
			if providerErr.Extensions[extensionID] == nil {
				t.Fatal("error must retain the raw google.rpc.Status payload")
			}
			closeStream(t, stream)
		})
	}
}

// TestCancelAbortsTheInFlightRequestAndKeepsTheSession checks that Cancel
// aborts through the request context (the handler observes its own context
// being cancelled) and that the session survives so a barge-in can be followed
// by new speech.
func TestCancelAbortsTheInFlightRequestAndKeepsTheSession(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	requestStarted := make(chan struct{}, 1)
	serverSawCancel := make(chan struct{}, 1)
	var calls int
	server := newSynthesizeServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		calls++
		if calls == 1 {
			requestStarted <- struct{}{}
			select {
			case <-r.Context().Done():
				serverSawCancel <- struct{}{}
			case <-release:
			}
			return
		}
		writeAudioResponse(t, w, wavContainer(samplePCM(64), 24_000), 0, nil)
	})
	defer close(release)
	defer server.Close()

	stream := openStream(t, server, protocol.CredentialsBYOK, nil)
	speak(t, stream, "discard me")
	// Cancelling before the request leaves the process would abort it locally
	// and prove nothing about the upstream, so wait until Google's stand-in is
	// actually holding it.
	awaitSignal(t, requestStarted, "upstream never received the request")
	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	awaitSignal(t, serverSawCancel, "upstream request was not aborted through its context")

	event := collectEvents(t, stream.Events(), 1)[0]
	if event.Type != protocol.EventWarning {
		t.Fatalf("cancel event = %s, want a non-terminal warning", event.Type)
	}
	if !strings.Contains(string(event.Data), "provider_request_cancelled") {
		t.Fatalf("cancel data = %s", event.Data)
	}
	// The in-flight slot is released before the terminal event is published, so
	// a consumer reacting to the warning can immediately speak again.
	speak(t, stream, "keep me")
	if got := strings.Join(eventTypes(collectEvents(t, stream.Events(), 3)), ","); got != "audio.started,audio.frame,audio.done" {
		t.Fatalf("second utterance = %s", got)
	}
	closeStream(t, stream)
}

// TestAbortTearsDownAnInFlightRequest covers the runtime's terminal-failure
// path: the upstream request must not outlive the session, and Events must
// close so the engine's reader loop finishes.
func TestAbortTearsDownAnInFlightRequest(t *testing.T) {
	t.Parallel()
	serverSawCancel := make(chan struct{}, 1)
	requestStarted := make(chan struct{}, 1)
	release := make(chan struct{})
	server := newSynthesizeServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		requestStarted <- struct{}{}
		select {
		case <-r.Context().Done():
			serverSawCancel <- struct{}{}
		case <-release:
		}
	})
	defer close(release)
	defer server.Close()

	stream := openStream(t, server, protocol.CredentialsBYOK, nil)
	speak(t, stream, "abort me")
	awaitSignal(t, requestStarted, "upstream never received the request")
	aborter, ok := stream.(runtimepkg.AbortingProviderStream)
	if !ok {
		t.Fatal("google tts stream must implement AbortingProviderStream")
	}
	if err := aborter.Abort(context.Background()); err != nil {
		t.Fatalf("abort: %v", err)
	}
	awaitSignal(t, serverSawCancel, "abort did not cancel the upstream request")
	for range stream.Events() {
		// Drain whatever raced out before the teardown; the assertion is that
		// the channel closes rather than what it carried.
	}
	if err := stream.AppendText(context.Background(), "too late"); !errors.Is(err, runtimepkg.ErrSessionClosed) {
		t.Fatalf("append after abort = %v, want ErrSessionClosed", err)
	}
}

// TestOpenRejectsUnsupportedRequests keeps a misrouted plan from ever reaching
// Google with a customer credential attached.
func TestOpenRejectsUnsupportedRequests(t *testing.T) {
	t.Parallel()
	server := newSynthesizeServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		writeAudioResponse(t, w, wavContainer(samplePCM(64), 24_000), 0, nil)
	})
	defer server.Close()

	for _, testCase := range []struct {
		name   string
		mutate func(*runtimepkg.AdapterRequest)
		reason string
	}{
		{name: "wrong kind", reason: "tts sessions", mutate: func(r *runtimepkg.AdapterRequest) { r.Kind = protocol.SessionKindSTT }},
		{name: "wrong provider", reason: "cannot open provider", mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Provider = "deepgram" }},
		{name: "websocket transport", reason: "http transport", mutate: func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Transport = protocol.TransportWebSocket
		}},
		{name: "auto model", reason: "concrete model", mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "auto" }},
		{name: "empty model", reason: "concrete model", mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "  " }},
		{name: "missing credential", reason: "bearer credential", mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential = nil }},
		{name: "wrong credential kind", reason: "bearer credential", mutate: func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Credential.Kind = protocol.CredentialSignedURL
		}},
		{name: "blank credential", reason: "non-empty bearer", mutate: func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Credential.Value = "   "
		}},
		// An API key sent as a bearer token 401s upstream with a message that
		// reads exactly like an expired OAuth token. Catch it locally instead.
		{name: "api key mistaken for a token", reason: "looks like an API key", mutate: func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Credential.Value = "AIzaSyD-not-an-access-token"
		}},
		{name: "no voice", reason: "requires a voice name", mutate: func(r *runtimepkg.AdapterRequest) {
			r.Options.Voice = ""
			r.Plan.Route.Voice = ""
		}},
		{name: "language contradicts the voice", reason: "contradicts voice", mutate: func(r *runtimepkg.AdapterRequest) {
			r.Options.Language = "en-US"
		}},
		{name: "region-less language on a locale-free voice", reason: "region subtag", mutate: func(r *runtimepkg.AdapterRequest) {
			r.Options.Voice = "custom-clone-7"
			r.Options.Language = "hi"
		}},
		{name: "missing media", reason: "media configuration", mutate: func(r *runtimepkg.AdapterRequest) { r.Media = nil }},
		{name: "opus media", reason: "mono pcm_s16le", mutate: func(r *runtimepkg.AdapterRequest) { r.Media.Encoding = "opus" }},
		{name: "stereo media", reason: "mono pcm_s16le", mutate: func(r *runtimepkg.AdapterRequest) { r.Media.Channels = 2 }},
		{name: "disallowed host", reason: "host is not allowed", mutate: func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Endpoint = "https://evil.example.com/v1/text:synthesize"
		}},
		{name: "wrong path", reason: "endpoint path must be", mutate: func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Endpoint = strings.Replace(r.Plan.Route.Endpoint, "/v1/text:synthesize", "/v1/voices", 1)
		}},
		{name: "credential in the query string", reason: "clean absolute https URL", mutate: func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Endpoint += "?key=AIzaSyD"
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			adapter, err := New(testConfig(server))
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			request := googleRequest(server, protocol.CredentialsBYOK)
			testCase.mutate(&request)
			stream, err := adapter.Open(context.Background(), request)
			if err == nil {
				_ = stream.Close(context.Background())
				t.Fatalf("open succeeded, want rejection mentioning %q", testCase.reason)
			}
			if !strings.Contains(err.Error(), testCase.reason) {
				t.Fatalf("error = %v, want it to mention %q", err, testCase.reason)
			}
		})
	}
}

// TestAudioInputOperationsAreUnsupported: a TTS session has no input audio, and
// the runtime distinguishes "wrong operation" from "failed operation".
func TestAudioInputOperationsAreUnsupported(t *testing.T) {
	t.Parallel()
	server := newSynthesizeServer(t, func(w http.ResponseWriter, r *http.Request) {})
	defer server.Close()
	stream := openStream(t, server, protocol.CredentialsBYOK, nil)
	if err := stream.WriteAudio(context.Background(), []byte{0, 0}); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Fatalf("write audio = %v", err)
	}
	if err := stream.CommitAudio(context.Background()); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Fatalf("commit audio = %v", err)
	}
	closeStream(t, stream)
}

// TestAppendTextEnforcesTheDocumentedByteQuota checks the published limit
// "Total bytes per request: 5,000". It is measured in BYTES: the Devanagari
// case below is well under 5000 runes and well over 5000 bytes, which is
// exactly the request a rune-counting implementation would let through.
func TestAppendTextEnforcesTheDocumentedByteQuota(t *testing.T) {
	t.Parallel()
	server := newSynthesizeServer(t, func(w http.ResponseWriter, r *http.Request) {})
	defer server.Close()
	stream := openStream(t, server, protocol.CredentialsBYOK, nil)

	devanagari := strings.Repeat("न", 2_000) // 3 bytes each: 6000 bytes, 2000 runes.
	var providerErr *runtimepkg.ProviderError
	if err := stream.AppendText(context.Background(), devanagari); !errors.As(err, &providerErr) || providerErr.Code != "input_too_large" {
		t.Fatalf("oversized append = %v, want input_too_large", err)
	}
	if err := stream.AppendText(context.Background(), "   "); err == nil {
		t.Fatal("blank append must be rejected")
	}
	if err := stream.CommitText(context.Background()); err == nil {
		t.Fatal("commit with nothing buffered must be rejected")
	}
	closeStream(t, stream)
}

// TestEmptyAudioIsReportedAsAFailure ports a decision from the platform
// adapter: Cloud TTS can answer 200 with no audio, and a silent success there
// looks like a working provider that produces silence.
func TestEmptyAudioIsReportedAsAFailure(t *testing.T) {
	t.Parallel()
	server := newSynthesizeServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"audioContent":""}`)
	})
	defer server.Close()

	stream := openStream(t, server, protocol.CredentialsBYOK, nil)
	speak(t, stream, "hello")
	if err := awaitProviderError(t, stream.Events()); !strings.Contains(err.Message, "no audio") {
		t.Fatalf("error = %v, want an explicit no-audio failure", err)
	}
	closeStream(t, stream)
}

// TestAudioDoneCarriesAVendorExtension checks the raw-payload slot. v1's
// success body has exactly one documented member and it is the audio itself,
// so the extension records the synthesis descriptor plus anything else the
// service happened to return.
func TestAudioDoneCarriesAVendorExtension(t *testing.T) {
	t.Parallel()
	pcm := samplePCM(128)
	server := newSynthesizeServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		writeAudioResponse(t, w, wavContainer(pcm, 24_000), 0, json.RawMessage(`"timepoints":[{"markName":"m1"}]`))
	})
	defer server.Close()

	stream := openStream(t, server, protocol.CredentialsBYOK, nil)
	speak(t, stream, "hello")
	events := collectEvents(t, stream.Events(), 3)
	raw := events[2].Extensions[extensionID]
	if raw == nil {
		t.Fatal("audio.done must carry a vendor extension")
	}
	var extension struct {
		AudioEncoding string          `json:"audio_encoding"`
		AudioBytes    int             `json:"audio_bytes"`
		Timepoints    json.RawMessage `json:"timepoints"`
	}
	if err := json.Unmarshal(raw, &extension); err != nil {
		t.Fatalf("extension: %v", err)
	}
	if extension.AudioEncoding != "LINEAR16" || extension.AudioBytes != len(pcm) {
		t.Fatalf("extension = %s", raw)
	}
	if extension.Timepoints == nil {
		t.Fatalf("undocumented members must be preserved, got %s", raw)
	}
	closeStream(t, stream)
}

// TestWAVStripperFindsTheDataChunk covers the container handling directly. The
// canonical Cloud TTS header is 44 bytes, but nothing in the format forbids an
// extra chunk before "data", so assuming a fixed offset would silently prepend
// metadata bytes to the audio.
func TestWAVStripperFindsTheDataChunk(t *testing.T) {
	t.Parallel()
	pcm := samplePCM(64)
	for _, testCase := range []struct {
		name      string
		container []byte
		want      []byte
	}{
		{name: "canonical header", container: wavContainer(pcm, 24_000), want: pcm},
		{name: "extra LIST chunk before data", container: wavContainerWithExtraChunk(pcm, 24_000), want: pcm},
		// A headerless body (a PCM-encoded response, were the encoding ever
		// switched) must pass through untouched rather than lose its first
		// samples to a header that is not there.
		{name: "already headerless", container: pcm, want: pcm},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			// Feed one byte at a time: the stripper has to work across arbitrary
			// chunk boundaries, which is the whole reason it is a state machine.
			stripper := &wavStripper{}
			var got []byte
			for index := range testCase.container {
				payload, err := stripper.write(testCase.container[index : index+1])
				if err != nil {
					t.Fatalf("byte %d: %v", index, err)
				}
				got = append(got, payload...)
			}
			if !bytes.Equal(got, testCase.want) {
				t.Fatalf("payload = %d bytes, want %d", len(got), len(testCase.want))
			}
		})
	}
}

// TestScannerAndDecoderSurviveChunkBoundaries feeds the JSON envelope in
// single-byte writes. Base64 decodes four characters at a time and the member
// name can straddle a TCP segment, so both have to carry state.
func TestScannerAndDecoderSurviveChunkBoundaries(t *testing.T) {
	t.Parallel()
	payload := samplePCM(301) // Deliberately not a multiple of three.
	body := `{ "audioContent" : "` + base64.StdEncoding.EncodeToString(payload) + `" , "extra": 1 }`

	scanner := &audioContentScanner{}
	decoder := &base64StreamDecoder{}
	var got []byte
	for index := range body {
		encoded, err := scanner.write([]byte(body[index : index+1]))
		if err != nil {
			t.Fatalf("scan byte %d: %v", index, err)
		}
		decoded, err := decoder.write(encoded)
		if err != nil {
			t.Fatalf("decode byte %d: %v", index, err)
		}
		got = append(got, decoded...)
	}
	remainder, err := decoder.flush()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	got = append(got, remainder...)
	if !scanner.complete() {
		t.Fatal("scanner did not find the audioContent member")
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("decoded %d bytes, want %d", len(got), len(payload))
	}
	if !strings.Contains(string(trailerObject(scanner.trailer())), `"extra"`) {
		t.Fatalf("trailer = %s", scanner.trailer())
	}
}

// --- helpers -------------------------------------------------------------

func credentialFor(source protocol.CredentialSource) string {
	if source == protocol.CredentialsBYOK {
		return "ya29.customer-owned-access-token"
	}
	return "ya29.control-plane-minted-token"
}

func testConfig(server *httptest.Server) Config {
	host, _, _ := strings.Cut(strings.TrimPrefix(server.URL, "http://"), ":")
	return Config{
		HTTPClient:            server.Client(),
		AllowedEndpointHosts:  []string{host},
		AllowInsecureEndpoint: true,
		RequestTimeout:        5 * time.Second,
	}
}

func googleRequest(server *httptest.Server, source protocol.CredentialSource) runtimepkg.AdapterRequest {
	now := time.Date(2026, time.August, 7, 12, 0, 0, 0, time.UTC)
	endpoint, _ := url.Parse(server.URL)
	endpoint.Path = "/v1/text:synthesize"
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindTTS,
		Plan: protocol.SessionPlan{
			PlanID: "plan_google_tts", SessionID: "sess_google_tts", AttemptID: "att_1",
			Execution: protocol.Execution{Placement: protocol.PlacementEmbedded, ProviderRoute: protocol.RouteProviderDirect, CredentialSource: source},
			ExpiresAt: now.Add(time.Hour),
			Route: protocol.PlanRoute{
				Provider: "google", Model: DefaultModel, Adapter: AdapterID,
				Transport: protocol.TransportHTTP, Endpoint: endpoint.String(),
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: credentialFor(source), ExpiresAt: now.Add(time.Hour)},
			},
			Reservation:  protocol.Reservation{ID: "res_google_tts", LeaseDurationSeconds: 60, LeaseExpiresAt: now.Add(time.Minute), Concurrency: protocol.ConcurrencyReservation{LeaseID: "conc_google_tts", Slots: 1}, Usage: protocol.UsageReservation{Unit: protocol.UsageUnitCharacters, AuthorizedUnits: 5_000}},
			Telemetry:    protocol.Telemetry{Endpoint: "https://control.speko.test/v1/runtime-events", Token: "telemetry-token", FlushIntervalMS: 5_000},
			Requirements: protocol.Requirements{Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision, RuntimeVersion: "test"},
			Signature:    "test",
		},
		Options: protocol.RequestOptions{Voice: "hi-IN-Chirp3-HD-Charon", Language: "hi-IN"},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1},
	}
}

func openStream(t *testing.T, server *httptest.Server, source protocol.CredentialSource, configure func(*Config)) runtimepkg.ProviderStream {
	t.Helper()
	return openStreamWith(t, server, source, configure, nil)
}

func openStreamWith(t *testing.T, server *httptest.Server, source protocol.CredentialSource, configure func(*Config), mutate func(*runtimepkg.AdapterRequest)) runtimepkg.ProviderStream {
	t.Helper()
	config := testConfig(server)
	if configure != nil {
		configure(&config)
	}
	adapter, err := New(config)
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	if adapter.ID() != AdapterID {
		t.Fatalf("adapter id = %q, want %q", adapter.ID(), AdapterID)
	}
	request := googleRequest(server, source)
	if mutate != nil {
		mutate(&request)
	}
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return stream
}

func speak(t *testing.T, stream runtimepkg.ProviderStream, text string) {
	t.Helper()
	if err := stream.AppendText(context.Background(), text); err != nil {
		t.Fatalf("append text: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit text: %v", err)
	}
}

func closeStream(t *testing.T, stream runtimepkg.ProviderStream) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := stream.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, open := <-stream.Events(); open {
		t.Fatal("events must be closed after Close")
	}
}

func newSynthesizeServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/text:synthesize" {
			http.NotFound(w, r)
			return
		}
		handler(w, r)
	}))
}

// writeAudioResponse renders a SynthesizeSpeechResponse the way the service
// does: one JSON object whose audioContent member is base64.
func writeAudioResponse(t *testing.T, w http.ResponseWriter, audio []byte, status int, extraMembers json.RawMessage) {
	t.Helper()
	if status != 0 {
		w.WriteHeader(status)
	}
	body := `{"audioContent":"` + base64.StdEncoding.EncodeToString(audio) + `"`
	if len(extraMembers) > 0 {
		body += "," + string(extraMembers)
	}
	body += "}"
	if _, err := io.WriteString(w, body); err != nil {
		t.Errorf("write response: %v", err)
	}
}

func samplePCM(size int) []byte {
	payload := make([]byte, size)
	for index := range payload {
		payload[index] = byte(index*7 + 3)
	}
	return payload
}

func wavContainer(pcm []byte, sampleRate int) []byte {
	var buffer bytes.Buffer
	buffer.WriteString("RIFF")
	_ = binary.Write(&buffer, binary.LittleEndian, uint32(36+len(pcm)))
	buffer.WriteString("WAVE")
	buffer.WriteString("fmt ")
	_ = binary.Write(&buffer, binary.LittleEndian, uint32(16))
	_ = binary.Write(&buffer, binary.LittleEndian, uint16(1))
	_ = binary.Write(&buffer, binary.LittleEndian, uint16(1))
	_ = binary.Write(&buffer, binary.LittleEndian, uint32(sampleRate))
	_ = binary.Write(&buffer, binary.LittleEndian, uint32(sampleRate*2))
	_ = binary.Write(&buffer, binary.LittleEndian, uint16(2))
	_ = binary.Write(&buffer, binary.LittleEndian, uint16(16))
	buffer.WriteString("data")
	_ = binary.Write(&buffer, binary.LittleEndian, uint32(len(pcm)))
	buffer.Write(pcm)
	return buffer.Bytes()
}

// wavContainerWithExtraChunk inserts an odd-sized LIST chunk (plus its RIFF pad
// byte) between "fmt " and "data".
func wavContainerWithExtraChunk(pcm []byte, sampleRate int) []byte {
	canonical := wavContainer(pcm, sampleRate)
	var extra bytes.Buffer
	extra.WriteString("LIST")
	note := []byte("speko")
	_ = binary.Write(&extra, binary.LittleEndian, uint32(len(note)))
	extra.Write(note)
	extra.WriteByte(0)

	var buffer bytes.Buffer
	buffer.Write(canonical[:36]) // RIFF + WAVE + the fmt chunk.
	buffer.Write(extra.Bytes())
	buffer.Write(canonical[36:])
	patched := buffer.Bytes()
	binary.LittleEndian.PutUint32(patched[4:8], uint32(len(patched)-8))
	return patched
}

func collectEvents(t *testing.T, events <-chan runtimepkg.ProviderEvent, want int) []runtimepkg.ProviderEvent {
	t.Helper()
	collected := make([]runtimepkg.ProviderEvent, 0, want)
	timer := time.NewTimer(5 * time.Second)
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
			t.Fatalf("timed out after %d of %d events", len(collected), want)
		}
	}
	return collected
}

func awaitProviderError(t *testing.T, events <-chan runtimepkg.ProviderEvent) *runtimepkg.ProviderError {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatal("events closed before a provider error arrived")
			}
			if event.Err == nil {
				continue
			}
			var providerErr *runtimepkg.ProviderError
			if !errors.As(event.Err, &providerErr) {
				t.Fatalf("error %v is not a *runtime.ProviderError", event.Err)
			}
			return providerErr
		case <-timer.C:
			t.Fatal("timed out waiting for a provider error")
		}
	}
}

func awaitSignal(t *testing.T, signal <-chan struct{}, reason string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal(reason)
	}
}

func eventTypes(events []runtimepkg.ProviderEvent) []string {
	types := make([]string, len(events))
	for index, event := range events {
		types[index] = string(event.Type)
	}
	return types
}

func concatAudio(events []runtimepkg.ProviderEvent) []byte {
	var audio []byte
	for _, event := range events {
		if event.Type == protocol.EventAudioFrame {
			audio = append(audio, event.Audio...)
		}
	}
	return audio
}
