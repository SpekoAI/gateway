package hume

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
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

// Vendor strings are transcribed here as literals rather than referenced from
// the package's own constants. Comparing production output against production
// constants keeps a misspelled wire string green; comparing it against an
// independently typed copy of Hume's documentation does not.
const (
	vendorStreamPath   = "/v0/tts/stream/json"
	vendorAPIKeyHeader = "X-Hume-Api-Key"
	vendorOctave2Model = "octave-2"
)

// TestCommitTextSendsDocumentedRequest pins every wire detail read out of
// Hume's TTS OpenAPI document for POST /v0/tts/stream/json. Each field here is
// one an upstream 422 would only reveal at runtime.
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
	if got.path != vendorStreamPath {
		t.Errorf("path = %q, want %q", got.path, vendorStreamPath)
	}
	// BYOK is the default in adapterRequest: the customer's permanent key rides
	// the vendor's own apiKey header, and NOT Authorization.
	if want := "customer-hume-key"; got.header.Get(vendorAPIKeyHeader) != want {
		t.Errorf("%s = %q, want %q", vendorAPIKeyHeader, got.header.Get(vendorAPIKeyHeader), want)
	}
	if authorization := got.header.Get("Authorization"); authorization != "" {
		t.Errorf("Authorization = %q, want empty for a BYOK session", authorization)
	}
	if got.header.Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type = %q", got.header.Get("Content-Type"))
	}

	// Compared as decoded JSON so the assertion is about field NAMES, values,
	// and JSON TYPES, not about Go's struct ordering. reflect.DeepEqual is used
	// instead of a printed diff because fmt renders the string "2" and the
	// number 2 identically, and `version` is exactly that trap.
	var body map[string]any
	if err := json.Unmarshal(got.body, &body); err != nil {
		t.Fatalf("request body is not JSON: %v (%s)", err, got.body)
	}
	want := map[string]any{
		"utterances": []any{map[string]any{
			// Fragments are concatenated into one Utterance; Hume's `text` is
			// per-utterance, not per-request.
			"text": "Hello, world!",
			// Voice Library voices must name their provider: the documented
			// default is CUSTOM_VOICE, which would look up a private voice.
			"voice": map[string]any{"name": "Colton Rivers", "provider": "HUME_AI"},
		}},
		// Format is the discriminated union object, not the bare string "pcm".
		"format": map[string]any{"type": "pcm"},
		// OctaveVersion is a STRING enum. A numeric 2 fails validation upstream.
		"version":       "2",
		"instant_mode":  true,
		"strip_headers": true,
	}
	if !reflect.DeepEqual(body, want) {
		t.Errorf("request body =\n%#v\nwant\n%#v", body, want)
	}
	// Restated on its own because it is the single field most likely to be
	// mangled by a Go author reaching for an int, and DeepEqual's failure
	// message on a nested map is easy to misread.
	if version, ok := body["version"].(string); !ok || version != "2" {
		t.Errorf("version = %#v, want the JSON string \"2\"", body["version"])
	}
}

// TestManagedCredentialUsesBearerAuthorization covers the second credential
// channel. Hume's two auth strategies are NOT the same header with different
// origins: a managed access token minted at /oauth2-cc/token is spent as a
// standard bearer token, and the apiKey header must be absent so a stale key
// cannot win the race if both were ever set.
func TestManagedCredentialUsesBearerAuthorization(t *testing.T) {
	t.Parallel()

	headers := make(chan http.Header, 1)
	server := newStreamServer(t, func(w http.ResponseWriter, r *http.Request) {
		headers <- r.Header.Clone()
		writeMessages(t, w, audioMessage(pcm(7)))
	})
	defer server.Close()

	stream := openStream(t, server.URL, func(request *runtimepkg.AdapterRequest) {
		request.Plan.Execution.CredentialSource = protocol.CredentialsManaged
		request.Plan.Route.Credential.Value = "minted-access-token"
	})
	defer func() { _ = stream.Abort(context.Background()) }()
	synthesize(t, stream, "hello")

	got := <-headers
	if want := "Bearer minted-access-token"; got.Get("Authorization") != want {
		t.Errorf("Authorization = %q, want %q", got.Get("Authorization"), want)
	}
	if key := got.Get(vendorAPIKeyHeader); key != "" {
		t.Errorf("%s = %q, want empty for a managed session", vendorAPIKeyHeader, key)
	}
}

// TestRelayRouteUsesTheAPIKeyHeader covers the third arm. A relay plan is
// managed for billing purposes but carries the connector's permanent Hume key,
// which is a portal API key and belongs in X-Hume-Api-Key exactly like a BYOK
// key — spending it as a bearer token is not a documented form, and the
// Authorization channel stays reserved for minted access tokens.
func TestRelayRouteUsesTheAPIKeyHeader(t *testing.T) {
	t.Parallel()

	headers := make(chan http.Header, 1)
	server := newStreamServer(t, func(w http.ResponseWriter, r *http.Request) {
		headers <- r.Header.Clone()
		writeMessages(t, w, audioMessage(pcm(7)))
	})
	defer server.Close()

	stream := openStream(t, server.URL, func(request *runtimepkg.AdapterRequest) {
		request.Plan.Execution.ProviderRoute = protocol.RouteSpekoRelay
		request.Plan.Execution.CredentialSource = protocol.CredentialsManaged
		request.Plan.Route.Credential.Value = "connector-hume-key"
	})
	defer func() { _ = stream.Abort(context.Background()) }()
	synthesize(t, stream, "hello")

	got := <-headers
	if want := "connector-hume-key"; got.Get(vendorAPIKeyHeader) != want {
		t.Errorf("%s = %q, want %q", vendorAPIKeyHeader, got.Get(vendorAPIKeyHeader), want)
	}
	if authorization := got.Get("Authorization"); authorization != "" {
		t.Errorf("Authorization = %q, want empty for a relay session", authorization)
	}
}

// TestRelayRouteAcceptsRelayAccessCredentialKind: protocol.SessionPlan
// validation requires a relay plan to label its credential relay_access, while
// a connector that synthesizes the plan and drives the adapter directly labels
// the same permanent key bearer. The relay arm must accept both spellings, or
// one of the two constructions becomes quietly unreachable.
func TestRelayRouteAcceptsRelayAccessCredentialKind(t *testing.T) {
	t.Parallel()

	headers := make(chan http.Header, 1)
	server := newStreamServer(t, func(w http.ResponseWriter, r *http.Request) {
		headers <- r.Header.Clone()
		writeMessages(t, w, audioMessage(pcm(7)))
	})
	defer server.Close()

	stream := openStream(t, server.URL, func(request *runtimepkg.AdapterRequest) {
		request.Plan.Execution.ProviderRoute = protocol.RouteSpekoRelay
		request.Plan.Execution.CredentialSource = protocol.CredentialsManaged
		request.Plan.Route.Credential.Kind = protocol.CredentialRelayAccess
		request.Plan.Route.Credential.Value = "connector-hume-key"
	})
	defer func() { _ = stream.Abort(context.Background()) }()
	synthesize(t, stream, "hello")

	if got := (<-headers).Get(vendorAPIKeyHeader); got != "connector-hume-key" {
		t.Errorf("%s = %q, want the permanent connector key", vendorAPIKeyHeader, got)
	}
}

// TestStreamEmitsStartedFramesThenDone covers the canonical event contract and
// the decode step between the wire and Audio. The payload bytes are chosen so
// that base64 encoding and decoding cannot be confused: 0x00/0xFF/0x10/0x7F is
// not printable ASCII, so a pass-through of the encoded string would not match.
func TestStreamEmitsStartedFramesThenDone(t *testing.T) {
	t.Parallel()

	first := pcm(0x00, 0xFF, 0x10, 0x7F)
	second := pcm(0xDE, 0xAD, 0xBE, 0xEF)
	server := newStreamServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeMessages(t, w,
			audioMessage(first),
			audioMessage(second),
			// A member whose discriminator this adapter does not act on must be
			// skipped, not treated as a failure: the union is documented as
			// extensible and a future variant must not break synthesis.
			json.RawMessage(`{"type":"something_new","request_id":"req_1"}`),
		)
	})
	defer server.Close()

	stream := openStream(t, server.URL, nil)
	defer func() { _ = stream.Abort(context.Background()) }()
	synthesize(t, stream, "Hello, world!")

	events := collect(t, stream.Events(), 4)
	if got := eventTypes(events); got != "audio.started,audio.frame,audio.frame,audio.done" {
		t.Fatalf("event types = %s", got)
	}
	// Two distinct frames, in order, each carrying the DECODED bytes. Asserting
	// the raw base64 string here would let a missing decode pass.
	if !reflect.DeepEqual(events[1].Audio, first) {
		t.Errorf("first frame audio = %v, want %v", events[1].Audio, first)
	}
	if !reflect.DeepEqual(events[2].Audio, second) {
		t.Errorf("second frame audio = %v, want %v", events[2].Audio, second)
	}
	// The raw provider message is preserved so nothing in the union is lost.
	if _, ok := events[1].Extensions["hume.ai/tts/v1"]; !ok {
		t.Errorf("audio frame extensions = %v, want a hume.ai/tts/v1 member", events[1].Extensions)
	}
}

// TestStrayWAVHeaderIsStripped guards the concatenation contract. The gateway
// promises consumers raw pcm_s16le; a container spliced between chunks is
// audible as a click per chunk. `strip_headers: true` should prevent this, but
// the flag is documented as applying "if applicable", so the reader defends.
func TestStrayWAVHeaderIsStripped(t *testing.T) {
	t.Parallel()

	samples := pcm(0x01, 0x02, 0x03, 0x04)
	server := newStreamServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeMessages(t, w, audioMessage(withWAVHeader(samples)))
	})
	defer server.Close()

	stream := openStream(t, server.URL, nil)
	defer func() { _ = stream.Abort(context.Background()) }()
	synthesize(t, stream, "hi")

	events := collect(t, stream.Events(), 3)
	if !reflect.DeepEqual(events[1].Audio, samples) {
		t.Errorf("frame audio = %v, want the header stripped to %v", events[1].Audio, samples)
	}
}

// TestTimestampMessageBecomesAlignment covers the other documented member of
// the TtsOutput union. Octave 2 returns these when include_timestamp_types is
// requested; dropping them would silently lose word timings a caller paid for.
func TestTimestampMessageBecomesAlignment(t *testing.T) {
	t.Parallel()

	server := newStreamServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeMessages(t, w,
			json.RawMessage(`{"type":"timestamp","generation_id":"gen_1","request_id":"req_1","snippet_id":"snip_1","timestamp":{"text":"Hello","time":{"begin":0,"end":420},"type":"word"}}`),
			audioMessage(pcm(1, 2)),
		)
	})
	defer server.Close()

	stream := openStream(t, server.URL, nil)
	defer func() { _ = stream.Abort(context.Background()) }()
	synthesize(t, stream, "Hello")

	events := collect(t, stream.Events(), 4)
	if got := eventTypes(events); got != "alignment,audio.started,audio.frame,audio.done" {
		t.Fatalf("event types = %s", got)
	}
	if !strings.Contains(string(events[0].Data), `"begin":0`) || !strings.Contains(string(events[0].Data), `"end":420`) {
		t.Errorf("alignment data = %s, want the vendor timestamp interval", events[0].Data)
	}
}

// TestStatusErrorsClassify checks that CommitText fails synchronously with a
// distinct Code per upstream status. Collapsing these would make a dead
// credential indistinguishable from a rate limit to every caller downstream.
func TestStatusErrorsClassify(t *testing.T) {
	t.Parallel()

	cases := []struct {
		status    int
		body      string
		wantCode  string
		retryable bool
	}{
		// 401/403: a bad BYOK key, or a managed access token past its 30 minutes.
		{status: http.StatusUnauthorized, body: `{"message":"nope"}`, wantCode: "authentication_failed"},
		{status: http.StatusForbidden, body: `{"message":"nope"}`, wantCode: "authentication_failed"},
		{status: http.StatusPaymentRequired, body: `{"message":"no credits"}`, wantCode: "provider_quota_exceeded"},
		// 422 is the documented HTTPValidationError for a body Hume rejects.
		{status: http.StatusUnprocessableEntity, body: `{"detail":[{"loc":["body"],"msg":"bad","type":"value_error"}]}`, wantCode: "invalid_request"},
		{status: http.StatusTooManyRequests, body: `{"message":"slow down"}`, wantCode: "provider_rate_limited", retryable: true},
		{status: http.StatusInternalServerError, body: `oops`, wantCode: "provider_unavailable", retryable: true},
	}
	for _, testCase := range cases {
		t.Run(fmt.Sprint(testCase.status), func(t *testing.T) {
			t.Parallel()
			server := newStreamServer(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(testCase.status)
				_, _ = io.WriteString(w, testCase.body)
			})
			defer server.Close()

			stream := openStream(t, server.URL, nil)
			defer func() { _ = stream.Abort(context.Background()) }()

			if err := stream.AppendText(context.Background(), "hello"); err != nil {
				t.Fatalf("append text: %v", err)
			}
			err := stream.CommitText(context.Background())
			var providerErr *runtimepkg.ProviderError
			if !errors.As(err, &providerErr) {
				t.Fatalf("commit text error = %v, want a ProviderError", err)
			}
			if providerErr.Code != testCase.wantCode {
				t.Errorf("code = %q, want %q", providerErr.Code, testCase.wantCode)
			}
			if providerErr.Retryable != testCase.retryable {
				t.Errorf("retryable = %v, want %v", providerErr.Retryable, testCase.retryable)
			}
			if providerErr.ProviderStatus != testCase.status {
				t.Errorf("provider status = %d, want %d", providerErr.ProviderStatus, testCase.status)
			}
			// The failure must not consume the utterance slot: the caller has
			// to be able to retry on the same stream.
			if err := stream.AppendText(context.Background(), "again"); err != nil {
				t.Errorf("append after a rejected request: %v", err)
			}
		})
	}
}

// TestStreamFailuresClassify covers everything that goes wrong AFTER a 200,
// where the only channel left is an error event. Each of these was reachable
// with a plausible upstream and would otherwise end as a truncated utterance
// that looked successful.
func TestStreamFailuresClassify(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		write       func(t *testing.T, w http.ResponseWriter)
		wantMessage string
	}{{
		name: "malformed_stream",
		// Not newline-delimited JSON at all. Also the shape an SSE `data:`
		// prefix would take if Hume ever changed framing.
		write:       func(t *testing.T, w http.ResponseWriter) { writeRaw(t, w, "data: {\"type\":\"audio\"}\n") },
		wantMessage: "Hume TTS sent a malformed synthesis stream",
	}, {
		name:        "invalid_base64",
		write:       func(t *testing.T, w http.ResponseWriter) { writeRaw(t, w, `{"type":"audio","audio":"!!!!"}`+"\n") },
		wantMessage: "Hume TTS sent invalid audio data",
	}, {
		name:        "in_band_error",
		write:       func(t *testing.T, w http.ResponseWriter) { writeRaw(t, w, `{"error":"generation failed"}`+"\n") },
		wantMessage: "Hume reported a synthesis error",
	}, {
		name: "success_without_audio",
		// A 200 that produced nothing is a failed synthesis wearing a success
		// status; emitting audio.done here would tell the caller it worked.
		write:       func(t *testing.T, w http.ResponseWriter) {},
		wantMessage: "Hume TTS completed without returning audio",
	}}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			server := newStreamServer(t, func(w http.ResponseWriter, _ *http.Request) { testCase.write(t, w) })
			defer server.Close()

			stream := openStream(t, server.URL, nil)
			defer func() { _ = stream.Abort(context.Background()) }()
			synthesize(t, stream, "hello")

			event := next(t, stream.Events())
			if event.Err == nil {
				t.Fatalf("event = %+v, want an error", event)
			}
			var providerErr *runtimepkg.ProviderError
			if !errors.As(event.Err, &providerErr) {
				t.Fatalf("error %v is not a ProviderError", event.Err)
			}
			if providerErr.Message != testCase.wantMessage {
				t.Errorf("message = %q, want %q", providerErr.Message, testCase.wantMessage)
			}
			if providerErr.Code != "provider_unavailable" {
				t.Errorf("code = %q, want provider_unavailable", providerErr.Code)
			}
		})
	}
}

// TestCancelAbortsInFlightSynthesisSilently is the barge-in path. Hume exposes
// no cancel verb on this resource, so cancellation means dropping the
// connection — and the torn body must not be reported as a provider fault.
func TestCancelAbortsInFlightSynthesisSilently(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	server := newStreamServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeMessages(t, w, audioMessage(pcm(1, 2)))
		// Hold the body open so Cancel has something live to tear down.
		select {
		case <-r.Context().Done():
		case <-release:
		}
	})
	defer server.Close()
	// Deferred LIFO: this frees the parked handler before server.Close waits on
	// it. Without it a t.Fatal anywhere below would deadlock the test binary
	// instead of failing it.
	defer close(release)

	stream := openStream(t, server.URL, nil)
	defer func() { _ = stream.Abort(context.Background()) }()
	synthesize(t, stream, "a long sentence the caller interrupts")

	// Consume the frames that already arrived so the reader is parked on the
	// body read, not on a full event channel.
	if got := eventTypes(collect(t, stream.Events(), 2)); got != "audio.started,audio.frame" {
		t.Fatalf("event types before cancel = %s", got)
	}
	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	// Cancel waits for the reader to unwind, so the stream must accept a fresh
	// utterance immediately rather than reporting one still in flight.
	if err := stream.AppendText(context.Background(), "next"); err != nil {
		t.Errorf("append after cancel: %v", err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	// No audio.done and no error: the caller asked for the stop.
	for event := range stream.Events() {
		t.Errorf("unexpected event after cancel: %+v", event)
	}
}

// TestCancelWithoutWorkReportsClosed distinguishes "nothing to cancel" from a
// successful barge-in, so the runtime does not treat an idle stream as one it
// just interrupted.
func TestCancelWithoutWorkReportsClosed(t *testing.T) {
	t.Parallel()

	server := newStreamServer(t, func(w http.ResponseWriter, _ *http.Request) {})
	defer server.Close()

	stream := openStream(t, server.URL, nil)
	defer func() { _ = stream.Abort(context.Background()) }()

	if err := stream.Cancel(context.Background()); !errors.Is(err, runtimepkg.ErrSessionClosed) {
		t.Errorf("cancel with nothing in flight = %v, want ErrSessionClosed", err)
	}
	if err := stream.AppendText(context.Background(), "buffered"); err != nil {
		t.Fatalf("append text: %v", err)
	}
	// Buffered-but-unsent text is real work, so discarding it is a success.
	if err := stream.Cancel(context.Background()); err != nil {
		t.Errorf("cancel with buffered text = %v, want nil", err)
	}
}

// TestOpenRejectsUnusableRequests keeps every precondition a plan could get
// wrong at the boundary, where the error names the field, instead of letting it
// surface as an opaque upstream rejection mid-call.
func TestOpenRejectsUnusableRequests(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		mutate  func(*runtimepkg.AdapterRequest)
		wantSub string
	}{{
		name:    "wrong_kind",
		mutate:  func(r *runtimepkg.AdapterRequest) { r.Kind = protocol.SessionKindSTT },
		wantSub: "supports tts sessions",
	}, {
		name:    "wrong_provider",
		mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Provider = "elevenlabs" },
		wantSub: "cannot open provider",
	}, {
		// Hume really does have a WebSocket TTS surface, so a websocket route
		// is a plausible control-plane decision — and a different adapter.
		name:    "wrong_transport",
		mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Transport = protocol.TransportWebSocket },
		wantSub: "requires http transport",
	}, {
		// "auto" means the control plane never resolved a model. Sending it
		// would make the version field meaningless.
		name:    "auto_model",
		mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "auto" },
		wantSub: "requires a concrete model",
	}, {
		name:    "unknown_model",
		mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "octave-3" },
		wantSub: `does not support model "octave-3"`,
	}, {
		name:    "missing_credential",
		mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential = nil },
		wantSub: "requires a bearer credential",
	}, {
		name:    "empty_credential",
		mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential.Value = "   " },
		wantSub: "requires a bearer credential",
	}, {
		name:    "wrong_credential_kind",
		mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential.Kind = protocol.CredentialSignedURL },
		wantSub: "requires a bearer credential",
	}, {
		// A relay-access credential cannot authenticate a provider-direct call.
		name:    "relay_access_kind_on_provider_direct",
		mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential.Kind = protocol.CredentialRelayAccess },
		wantSub: "requires a bearer credential",
	}, {
		// Octave has no sample-rate parameter and always returns 48 kHz, so a
		// plan asking for 24 kHz would play back at half speed.
		name:    "wrong_sample_rate",
		mutate:  func(r *runtimepkg.AdapterRequest) { r.Media.SampleRateHz = 24_000 },
		wantSub: "only outputs 48000 Hz",
	}, {
		name:    "stereo",
		mutate:  func(r *runtimepkg.AdapterRequest) { r.Media.Channels = 2 },
		wantSub: "requires mono pcm_s16le",
	}, {
		name:    "opus",
		mutate:  func(r *runtimepkg.AdapterRequest) { r.Media.Encoding = "opus" },
		wantSub: "requires mono pcm_s16le",
	}, {
		name:    "missing_media",
		mutate:  func(r *runtimepkg.AdapterRequest) { r.Media = nil },
		wantSub: "requires media configuration",
	}, {
		name:    "wrong_endpoint_path",
		mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = "https://api.hume.ai/v0/tts/json" },
		wantSub: "endpoint path must be " + vendorStreamPath,
	}, {
		// A credential in the query string would be logged by every proxy in
		// between; Hume only accepts one there on its WebSocket resources.
		name: "endpoint_with_query",
		mutate: func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Endpoint = "https://api.hume.ai" + vendorStreamPath + "?api_key=leak"
		},
		wantSub: "clean absolute HTTPS URL",
	}, {
		name:    "endpoint_host_not_allowed",
		mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = "https://evil.example" + vendorStreamPath },
		wantSub: "host is not allowed",
	}}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			adapter, err := New(Config{})
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			request := adapterRequest("https://api.hume.ai" + vendorStreamPath)
			testCase.mutate(&request)
			if _, err := adapter.Open(context.Background(), request); err == nil {
				t.Fatalf("open succeeded, want an error containing %q", testCase.wantSub)
			} else if !strings.Contains(err.Error(), testCase.wantSub) {
				t.Errorf("open error = %v, want it to contain %q", err, testCase.wantSub)
			}
		})
	}
}

// TestDefaultVoiceWhenPlanNamesNone: Octave 2 and instant mode both reject a
// request with no voice, so the adapter must substitute one rather than emit a
// body that is guaranteed to 422.
func TestDefaultVoiceWhenPlanNamesNone(t *testing.T) {
	t.Parallel()

	bodies := make(chan []byte, 1)
	server := newStreamServer(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodies <- body
		writeMessages(t, w, audioMessage(pcm(1)))
	})
	defer server.Close()

	stream := openStream(t, server.URL, func(request *runtimepkg.AdapterRequest) {
		request.Options.Voice = ""
	})
	defer func() { _ = stream.Abort(context.Background()) }()
	synthesize(t, stream, "hello")

	if got := string(<-bodies); !strings.Contains(got, `"name":"Colton Rivers"`) {
		t.Errorf("request body = %s, want the default voice", got)
	}
}

// TestInputCeilingIsMeasuredInCharacters: Hume documents 5,000 characters per
// Utterance. A byte-length check would reject a Devanagari or CJK utterance
// less than half that long.
func TestInputCeilingIsMeasuredInCharacters(t *testing.T) {
	t.Parallel()

	server := newStreamServer(t, func(w http.ResponseWriter, _ *http.Request) {})
	defer server.Close()

	stream := openStream(t, server.URL, nil)
	defer func() { _ = stream.Abort(context.Background()) }()

	// 4,000 three-byte runes: 12,000 bytes but only 4,000 characters, so this
	// must be accepted.
	if err := stream.AppendText(context.Background(), strings.Repeat("あ", 4_000)); err != nil {
		t.Fatalf("append 4000 multi-byte characters: %v", err)
	}
	err := stream.AppendText(context.Background(), strings.Repeat("あ", 1_001))
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "input_too_large" {
		t.Fatalf("append past the ceiling = %v, want an input_too_large ProviderError", err)
	}
}

// TestTranscriptionOperationsAreRejected: a synthesizer must refuse caller
// audio loudly so the runtime can report the mismatch rather than silently
// dropping it.
func TestTranscriptionOperationsAreRejected(t *testing.T) {
	t.Parallel()

	server := newStreamServer(t, func(w http.ResponseWriter, _ *http.Request) {})
	defer server.Close()

	stream := openStream(t, server.URL, nil)
	defer func() { _ = stream.Abort(context.Background()) }()

	if err := stream.WriteAudio(context.Background(), []byte{1}); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Errorf("write audio = %v, want ErrUnsupportedOperation", err)
	}
	if err := stream.CommitAudio(context.Background()); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Errorf("commit audio = %v, want ErrUnsupportedOperation", err)
	}
}

// TestSequentialUtterancesReuseTheStream: one utterance is one HTTP request, so
// the second must go out with only the second text — a leaked buffer would
// resynthesize the first.
func TestSequentialUtterancesReuseTheStream(t *testing.T) {
	t.Parallel()

	texts := make(chan string, 2)
	server := newStreamServer(t, func(w http.ResponseWriter, r *http.Request) {
		var body synthesizeRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		if len(body.Utterances) != 1 {
			t.Errorf("utterances = %d, want exactly 1", len(body.Utterances))
			texts <- ""
			return
		}
		texts <- body.Utterances[0].Text
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

// TestCloseWaitsForInFlightAudio: a caller that closes right after CommitText
// must still receive the audio it paid for, and the event channel must close
// exactly once afterwards.
func TestCloseWaitsForInFlightAudio(t *testing.T) {
	t.Parallel()

	server := newStreamServer(t, func(w http.ResponseWriter, _ *http.Request) {
		writeMessages(t, w, audioMessage(pcm(1, 2)))
	})
	defer server.Close()

	stream := openStream(t, server.URL, nil)
	synthesize(t, stream, "hello")
	if got := eventTypes(collect(t, stream.Events(), 3)); got != "audio.started,audio.frame,audio.done" {
		t.Fatalf("event types = %s", got)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	drain(t, stream.Events())
	// Close is idempotent: the runtime may close after an Abort.
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if err := stream.AppendText(context.Background(), "after"); !errors.Is(err, runtimepkg.ErrSessionClosed) {
		t.Errorf("append after close = %v, want ErrSessionClosed", err)
	}
}

// --- helpers ---------------------------------------------------------------

// newStreamServer stands in for api.hume.ai. It asserts the resource path so a
// misrouted request fails loudly instead of hitting a catch-all handler.
func newStreamServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != vendorStreamPath {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		handler(w, r)
	}))
}

// writeMessages emits newline-delimited JSON objects and flushes each one, which
// is the framing both of Hume's official SDKs parse (TypeScript sets
// messageTerminator "\n"; Python iterates lines and json.loads each).
func writeMessages(t *testing.T, w http.ResponseWriter, messages ...json.RawMessage) {
	t.Helper()
	for _, message := range messages {
		writeRaw(t, w, string(message)+"\n")
	}
}

func writeRaw(t *testing.T, w http.ResponseWriter, payload string) {
	t.Helper()
	if _, err := io.WriteString(w, payload); err != nil {
		t.Errorf("write payload: %v", err)
		return
	}
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}

// audioMessage builds the documented `audio` member of the TtsOutput union with
// its full required field set, so the decoder is exercised against the real
// envelope rather than a two-field stub.
func audioMessage(audio []byte) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(
		`{"type":"audio","audio":%q,"audio_format":"pcm","chunk_index":0,"generation_id":"gen_1","is_last_chunk":false,"request_id":"req_1","snippet_id":"snip_1","text":"hello","transcribed_text":null,"utterance_index":0}`,
		base64.StdEncoding.EncodeToString(audio),
	))
}

func pcm(samples ...byte) []byte { return samples }

// withWAVHeader wraps PCM in the canonical 44-byte RIFF/WAVE container.
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
		parsed.Path = vendorStreamPath
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
	now := time.Date(2026, time.August, 7, 11, 59, 0, 0, time.UTC)
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindTTS,
		Plan: protocol.SessionPlan{
			PlanID: "plan_hume", SessionID: "sess_hume", AttemptID: "att_1",
			Execution: protocol.Execution{Placement: protocol.PlacementEmbedded, ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK},
			ExpiresAt: now.Add(time.Hour),
			Route: protocol.PlanRoute{
				Provider: "hume", Model: vendorOctave2Model, Adapter: AdapterID,
				Transport: protocol.TransportHTTP, Endpoint: endpoint,
				// Managed Hume access tokens live 30 minutes; the BYOK key in
				// this fixture inherits the same lease shape.
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "customer-hume-key", ExpiresAt: now.Add(30 * time.Minute)},
			},
			Reservation: protocol.Reservation{
				ID: "res_hume", LeaseDurationSeconds: 60, LeaseExpiresAt: now.Add(time.Minute),
				Concurrency: protocol.ConcurrencyReservation{LeaseID: "conc_hume", Slots: 1},
				Usage:       protocol.UsageReservation{Unit: protocol.UsageUnitCharacters, AuthorizedUnits: 5_000},
			},
			Requirements: protocol.Requirements{Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision, RuntimeVersion: "0.1.0"},
			Signature:    "test-signature",
		},
		Options: protocol.RequestOptions{Voice: "Colton Rivers", Language: "en-US", MaxInputCharacters: 5_000},
		// 48 kHz is Hume's fixed output rate; anything else is rejected at Open.
		Media: &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 48_000, Channels: 1},
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
