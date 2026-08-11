package rime

import (
	"context"
	"encoding/base64"
	"encoding/json"
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

// Every wire string in this file is transcribed as a literal from Rime's
// reference pages rather than referenced from the adapter's own constants. If
// the two ever disagree the test must fail; comparing the adapter against
// itself would keep a misspelled parameter or message type green forever.

func TestAdapterSendsDocumentedHandshakeAndStreamsAudio(t *testing.T) {
	t.Parallel()

	requests := make(chan *http.Request, 1)
	sent := make(chan map[string]any, 3)
	server := newTTSServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		requests <- request.Clone(request.Context())
		first, err := readMessage(ctx, conn)
		if err != nil {
			t.Errorf("first text frame: %v", err)
			return
		}
		sent <- first
		second, err := readMessage(ctx, conn)
		if err != nil {
			t.Errorf("second text frame: %v", err)
			return
		}
		sent <- second
		flush, err := readMessage(ctx, conn)
		if err != nil {
			t.Errorf("flush frame: %v", err)
			return
		}
		sent <- flush

		contextID, _ := first["contextId"].(string)
		// Two chunks before `done`: a caller must see incremental audio, not one
		// buffered blob delivered at the end.
		if err := writeJSON(ctx, conn, map[string]any{"type": "chunk", "contextId": contextID, "data": base64.StdEncoding.EncodeToString([]byte{0x01, 0x02, 0x03})}); err != nil {
			t.Errorf("first chunk: %v", err)
			return
		}
		if err := writeJSON(ctx, conn, map[string]any{"type": "chunk", "contextId": contextID, "data": base64.StdEncoding.EncodeToString([]byte{0x04, 0x05})}); err != nil {
			t.Errorf("second chunk: %v", err)
			return
		}
		if err := writeJSON(ctx, conn, map[string]any{
			"type":      "timestamps",
			"contextId": contextID,
			"word_timestamps": map[string]any{
				"words": []string{"Hello", "there."},
				"start": []float64{0, 0.36106},
				"end":   []float64{0.36106, 0.54159},
			},
		}); err != nil {
			t.Errorf("timestamps: %v", err)
			return
		}
		if err := writeJSON(ctx, conn, map[string]any{"type": "done", "contextId": contextID}); err != nil {
			t.Errorf("done: %v", err)
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
	if err := stream.AppendText(context.Background(), "Hello "); err != nil {
		t.Fatalf("append first token: %v", err)
	}
	if err := stream.AppendText(context.Background(), "there."); err != nil {
		t.Fatalf("append second token: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit text: %v", err)
	}

	events := collectProviderEvents(t, stream.Events(), 5)
	if got := strings.Join(eventTypes(events), ","); got != "audio.started,audio.frame,audio.frame,alignment,audio.done" {
		t.Fatalf("event types = %s", got)
	}
	// Audio must be decoded from base64 into real bytes; handing the runtime the
	// base64 text would still "stream" and would still be silence.
	if got := events[1].Audio; string(got) != string([]byte{0x01, 0x02, 0x03}) {
		t.Fatalf("first frame audio = %v", got)
	}
	if got := events[2].Audio; string(got) != string([]byte{0x04, 0x05}) {
		t.Fatalf("second frame audio = %v", got)
	}
	// The alignment payload must survive verbatim: word timings are the only
	// thing a caller can use to map a barge-in back to the last spoken word.
	if events[3].Extensions["rime.ai/ws3"] == nil {
		t.Fatal("alignment must retain the raw Rime payload")
	}
	var alignment struct {
		WordTimestamps struct {
			Words []string  `json:"words"`
			Start []float64 `json:"start"`
			End   []float64 `json:"end"`
		} `json:"word_timestamps"`
	}
	if err := json.Unmarshal(events[3].Data, &alignment); err != nil {
		t.Fatalf("alignment data: %v", err)
	}
	if strings.Join(alignment.WordTimestamps.Words, "|") != "Hello|there." || len(alignment.WordTimestamps.Start) != 2 || alignment.WordTimestamps.End[1] != 0.54159 {
		t.Fatalf("alignment word timestamps = %+v", alignment.WordTimestamps)
	}

	if err := closeStream(t, stream); err != nil {
		t.Fatalf("close stream: %v", err)
	}
	if _, ok := <-stream.Events(); ok {
		t.Fatal("events must close after graceful websocket close")
	}

	// /ws3 reads every synthesis argument from the handshake query; there is no
	// opening JSON frame, so a wrong key name here is silently ignored upstream
	// and the request is synthesized with the wrong model, voice, or rate.
	select {
	case request := <-requests:
		query := request.URL.Query()
		for name, want := range map[string]string{
			"speaker":      "astra",
			"modelId":      "coda",
			"audioFormat":  "pcm",
			"lang":         "eng",
			"samplingRate": "16000",
			"segment":      "never",
		} {
			if got := query.Get(name); got != want {
				t.Errorf("handshake query %q = %q, want %q", name, got, want)
			}
		}
		// Rime rejects an unauthenticated connection with 401; the token is a
		// header, never a query parameter, so it must not appear in the URL.
		if got := request.Header.Get("Authorization"); got != "Bearer customer-rime-key" {
			t.Errorf("Authorization = %q", got)
		}
		if strings.Contains(request.URL.RawQuery, "customer-rime-key") {
			t.Errorf("handshake query leaked the api key: %s", request.URL.RawQuery)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not observe websocket handshake")
	}

	first := <-sent
	second := <-sent
	flush := <-sent
	// A text frame is exactly {text, contextId}. Asserting the whole key set,
	// not just the values, catches a renamed or added field.
	if got := keysOf(first); got != "contextId,text" {
		t.Fatalf("first text frame keys = %s", got)
	}
	if first["text"] != "Hello " || second["text"] != "there." {
		t.Fatalf("text frames = %v, %v", first, second)
	}
	// Rime keeps at most one context id and echoes it on the events it sends
	// back, so both tokens of one utterance must carry the same non-empty id.
	contextID, _ := first["contextId"].(string)
	if contextID == "" || second["contextId"] != contextID {
		t.Fatalf("context ids = %q, %q", contextID, second["contextId"])
	}
	// CommitText is a flush, not another text frame: under segment=never Rime
	// synthesizes nothing until it sees this exact operation.
	if got := keysOf(flush); got != "operation" || flush["operation"] != "flush" {
		t.Fatalf("commit frame = %v", flush)
	}
}

func TestCancelSendsClearOperationForBargeIn(t *testing.T) {
	t.Parallel()

	sent := make(chan map[string]any, 4)
	server := newTTSServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		for index := 0; index < 3; index++ {
			message, err := readMessage(ctx, conn)
			if err != nil {
				t.Errorf("frame %d: %v", index, err)
				return
			}
			sent <- message
		}
		// Rime documents that `clear` discards the buffer but does not cancel
		// audio already being synthesized, so the flush still terminates in a
		// `done`. Close must therefore keep waiting for it after a barge-in.
		if err := writeJSON(ctx, conn, map[string]any{"type": "done", "contextId": nil}); err != nil {
			t.Errorf("done: %v", err)
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
	if err := stream.AppendText(context.Background(), "Your order has been confirmed."); err != nil {
		t.Fatalf("append text: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit text: %v", err)
	}
	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	events := collectProviderEvents(t, stream.Events(), 1)
	if events[0].Type != protocol.EventAudioDone {
		t.Fatalf("event after clear = %s", events[0].Type)
	}
	if err := closeStream(t, stream); err != nil {
		t.Fatalf("close stream: %v", err)
	}

	<-sent
	<-sent
	clear := <-sent
	if got := keysOf(clear); got != "operation" || clear["operation"] != "clear" {
		t.Fatalf("cancel frame = %v", clear)
	}
}

func TestCancelBeforeFlushDoesNotStrandClose(t *testing.T) {
	t.Parallel()

	server := newTTSServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		for index := 0; index < 2; index++ {
			if _, err := readMessage(ctx, conn); err != nil {
				return
			}
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
	if err := stream.AppendText(context.Background(), "half a sentence"); err != nil {
		t.Fatalf("append text: %v", err)
	}
	// Nothing was flushed, so Rime was never asked for audio and will never
	// send `done`. Close must not sit waiting for a terminal event that the
	// protocol does not promise.
	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if err := closeStream(t, stream); err != nil {
		t.Fatalf("close after unflushed cancel: %v", err)
	}
	// A cleared buffer must be reusable: the next utterance starts fresh.
	if err := stream.Cancel(context.Background()); err != runtimepkg.ErrSessionClosed {
		t.Fatalf("second cancel = %v, want ErrSessionClosed", err)
	}
}

func TestCloseWithUnflushedTextDoesNotBlock(t *testing.T) {
	t.Parallel()

	server := newTTSServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		if _, err := readMessage(ctx, conn); err != nil {
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
	// Text was appended but never committed, so under segment=never Rime was
	// never asked to synthesize and will never send `done`. Close must only
	// wait on a terminal event when a flush is actually in flight; waiting on
	// an open utterance would hang every session torn down mid-turn until its
	// context deadline. The unflushed text is dropped, which is the documented
	// trade-off for not using `eos`.
	if err := stream.AppendText(context.Background(), "an abandoned half sentence"); err != nil {
		t.Fatalf("append text: %v", err)
	}
	if err := closeStream(t, stream); err != nil {
		t.Fatalf("close with unflushed text: %v", err)
	}
}

func TestHandshakeFailuresMapToDistinctCodes(t *testing.T) {
	t.Parallel()

	// Rime documents 401 for a missing or invalid bearer token. The rest are the
	// standard HTTP classes a caller must be able to tell apart: an auth failure
	// must not be retried, a rate limit must be.
	for _, testCase := range []struct {
		status    int
		code      string
		retryable bool
	}{
		{status: http.StatusUnauthorized, code: "authentication_failed", retryable: false},
		{status: http.StatusForbidden, code: "authentication_failed", retryable: false},
		{status: http.StatusTooManyRequests, code: "provider_rate_limited", retryable: true},
		{status: http.StatusBadGateway, code: "provider_unavailable", retryable: true},
		{status: http.StatusBadRequest, code: "provider_unavailable", retryable: false},
	} {
		t.Run(fmt.Sprintf("status_%d", testCase.status), func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(testCase.status)
			}))
			defer server.Close()

			adapter, err := New(testConfig(server.URL))
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			_, err = adapter.Open(context.Background(), adapterRequest(server.URL))
			var providerError *runtimepkg.ProviderError
			if !errorsAs(err, &providerError) {
				t.Fatalf("open error = %v", err)
			}
			if providerError.Code != testCase.code || providerError.Retryable != testCase.retryable || providerError.ProviderStatus != testCase.status {
				t.Fatalf("provider error = %+v", providerError)
			}
			// A handshake failure body can echo the request; the key must not
			// travel into a log line via the error message.
			if strings.Contains(providerError.Error(), "customer-rime-key") {
				t.Fatalf("dial error leaked the api key: %s", providerError.Error())
			}
		})
	}
}

func TestInBandErrorEventEndsTheAttempt(t *testing.T) {
	t.Parallel()

	server := newTTSServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		if _, err := readMessage(ctx, conn); err != nil {
			return
		}
		// Rime's error event carries no status code and keeps the socket open.
		// The adapter still has to end the attempt: no audio and no `done` are
		// coming for this utterance.
		if err := writeJSON(ctx, conn, map[string]any{"type": "error", "message": "Speaker not found"}); err != nil {
			t.Errorf("error event: %v", err)
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
	if err := stream.AppendText(context.Background(), "Hello."); err != nil {
		t.Fatalf("append text: %v", err)
	}
	select {
	case event := <-stream.Events():
		var providerError *runtimepkg.ProviderError
		if !errorsAs(event.Err, &providerError) {
			t.Fatalf("event = %+v", event)
		}
		// "malformed or unexpected input" is a request defect, so it classifies
		// apart from the transport failures above and must not be retried.
		if providerError.Code != "invalid_request" || providerError.Retryable {
			t.Fatalf("provider error = %+v", providerError)
		}
		if !strings.Contains(providerError.Message, "Speaker not found") {
			t.Fatalf("provider error message = %q", providerError.Message)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the error event")
	}
	_ = closeStream(t, stream)
}

func TestMalformedServerJSONIsRetryable(t *testing.T) {
	t.Parallel()

	server := newTTSServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		if _, err := readMessage(ctx, conn); err != nil {
			return
		}
		if err := conn.Write(ctx, websocket.MessageText, []byte("{not json")); err != nil {
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
	if err := stream.AppendText(context.Background(), "Hello."); err != nil {
		t.Fatalf("append text: %v", err)
	}
	select {
	case event := <-stream.Events():
		var providerError *runtimepkg.ProviderError
		// A garbled frame is a transport problem, not a bad request: unlike the
		// in-band error event it is worth retrying.
		if !errorsAs(event.Err, &providerError) || providerError.Code != "provider_unavailable" || !providerError.Retryable {
			t.Fatalf("event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the malformed-json error")
	}
	_ = closeStream(t, stream)
}

func TestAppendTextRejectsTextBeyondRimeCharacterLimit(t *testing.T) {
	t.Parallel()

	server := newTTSServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
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
	// Rime documents 1,000 characters per synthesis request. Under segment=never
	// the buffer accumulates across tokens, so the budget is a running total and
	// has to be checked before the token goes out, not per token.
	if err := stream.AppendText(context.Background(), strings.Repeat("a", 600)); err != nil {
		t.Fatalf("first token: %v", err)
	}
	err = stream.AppendText(context.Background(), strings.Repeat("b", 401))
	var providerError *runtimepkg.ProviderError
	if !errorsAs(err, &providerError) || providerError.Code != "input_too_large" {
		t.Fatalf("over-budget append = %v", err)
	}
	// Characters, not bytes: 400 multi-byte runes still fit in the remaining
	// 400-character allowance.
	if err := stream.AppendText(context.Background(), strings.Repeat("é", 400)); err != nil {
		t.Fatalf("multi-byte token within budget: %v", err)
	}
	_ = closeStream(t, stream)
}

func TestOpenRejectsInvalidRequests(t *testing.T) {
	t.Parallel()

	const secret = "secret-that-must-not-leak"
	for name, testCase := range map[string]struct {
		mutate func(*runtimepkg.AdapterRequest)
		want   string
	}{
		// The adapter is registered by id, but a mis-wired planner could hand it
		// another modality or vendor; both must fail loudly rather than dial.
		"wrong kind":     {mutate: func(r *runtimepkg.AdapterRequest) { r.Kind = protocol.SessionKindSTT }, want: "tts sessions"},
		"wrong provider": {mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Provider = "cartesia" }, want: "cannot open provider"},
		// /ws3 is a socket. An http-transport plan would mean the planner picked
		// the HTTP endpoint, which this adapter does not implement.
		"wrong transport": {mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Transport = protocol.TransportHTTP }, want: "websocket transport"},
		// Rime warns that a missing modelId silently routes to Mist v3, where a
		// Coda speaker fails with "Speaker not found". An unresolved `auto` is
		// the same bug arriving from our side.
		"auto model":  {mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "auto" }, want: "concrete model"},
		"empty model": {mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "" }, want: "concrete model"},
		// `speaker` is a required query parameter with no server-side default.
		"missing voice": {mutate: func(r *runtimepkg.AdapterRequest) { r.Options.Voice = "" }, want: "voice id"},
		// `lang` must match the speaker's language and Rime publishes a closed
		// table of eight; a language outside it is a guaranteed upstream failure.
		"missing language":      {mutate: func(r *runtimepkg.AdapterRequest) { r.Options.Language = "" }, want: "language in request options"},
		"undocumented language": {mutate: func(r *runtimepkg.AdapterRequest) { r.Options.Language = "nl" }, want: "does not document language"},
		// /ws3 offers mp3, mulaw and pcm. Opus is a protocol-legal encoding with
		// no Rime equivalent on this socket.
		"opus encoding": {mutate: func(r *runtimepkg.AdapterRequest) { r.Media.Encoding = "opus" }, want: "pcm_s16le"},
		// Rime documents no channel parameter, so anything but mono would be a
		// promise the adapter cannot keep.
		"stereo": {mutate: func(r *runtimepkg.AdapterRequest) { r.Media.Channels = 2 }, want: "mono"},
		"nil media": {mutate: func(r *runtimepkg.AdapterRequest) {
			r.Media = nil
		}, want: "media configuration"},
		// Credentials: only a non-empty bearer works, because the token goes into
		// an Authorization header and Rime has no other auth mode.
		"nil credential": {mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential = nil }, want: "bearer credential"},
		"wrong credential kind": {mutate: func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Credential.Kind = protocol.CredentialSignedURL
		}, want: "bearer credential"},
		// A relay-access credential cannot authenticate a provider-direct call.
		"relay_access kind on provider-direct": {mutate: func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Credential.Kind = protocol.CredentialRelayAccess
		}, want: "bearer credential"},
		"empty credential": {mutate: func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Credential.Value = "   "
		}, want: "bearer credential"},
		"unknown credential source": {mutate: func(r *runtimepkg.AdapterRequest) {
			r.Plan.Execution.CredentialSource = protocol.CredentialSource("delegated")
		}, want: "credential source"},
		// The endpoint must be the flagship JSON socket. /ws is raw binary with
		// no timestamps, context ids, or structured errors, so decoding it with
		// this adapter would yield silence.
		"wrong path": {mutate: func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Endpoint = strings.Replace(r.Plan.Route.Endpoint, "/ws3", "/ws", 1)
		}, want: "/ws3"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			adapter, err := New(testConfig("http://127.0.0.1:1"))
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			request := adapterRequest("http://127.0.0.1:1")
			request.Plan.Route.Credential.Value = secret
			testCase.mutate(&request)
			_, err = adapter.Open(context.Background(), request)
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("open error = %v, want it to mention %q", err, testCase.want)
			}
			// Validation runs before the dial, so no credential should ever have
			// reached a message that a caller might log.
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("validation error leaked the credential: %v", err)
			}
		})
	}
}

func TestCloudSamplingRateIsEnforcedOnlyForRimesOwnHost(t *testing.T) {
	t.Parallel()

	// Rime's hosted API accepts a fixed set of rates; on-prem accepts any. A
	// blanket whitelist would break licensed on-prem deployments, and no
	// whitelist would turn a typo into an upstream failure mid-session.
	// Rejection is asserted through Open, which reaches validation before it
	// dials. The accepting cases are asserted against the validator directly:
	// driving them through Open would mean really connecting to Rime.
	adapter, err := New(Config{})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	request := adapterRequest("http://127.0.0.1:1")
	request.Plan.Route.Endpoint = "wss://users-ws.rime.ai/ws3"
	request.Media.SampleRateHz = 44_101
	if _, err := adapter.Open(context.Background(), request); err == nil || !strings.Contains(err.Error(), "sampling rate") {
		t.Fatalf("cloud open error = %v", err)
	}

	options := protocol.RequestOptions{Voice: "astra", Language: "eng"}
	// The seven rates Rime lists for its hosted API, transcribed from the docs.
	for _, rate := range []int{8_000, 16_000, 22_050, 24_000, 44_100, 48_000, 96_000} {
		media := protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: rate, Channels: 1}
		if err := validateGenerationOptions("coda", options, media, "users-ws.rime.ai"); err != nil {
			t.Errorf("cloud rate %d must be accepted: %v", rate, err)
		}
	}
	offCloud := protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 44_101, Channels: 1}
	if err := validateGenerationOptions("coda", options, offCloud, "tts.internal.example"); err != nil {
		t.Errorf("on-prem must accept any rate: %v", err)
	}
}

// TestRelayRouteUsesTheBearerHeaderAndAcceptsBothCredentialKinds pins the
// relay arm. Rime is bearer-only, so a relay plan sends the connector's
// permanent key through the same Authorization header as every other source —
// and the arm must accept both credential spellings: protocol.SessionPlan
// validation requires relay plans to label the credential relay_access, while
// a connector that synthesizes the plan and drives the adapter directly
// labels the same permanent key bearer.
func TestRelayRouteUsesTheBearerHeaderAndAcceptsBothCredentialKinds(t *testing.T) {
	t.Parallel()

	for _, kind := range []protocol.CredentialKind{protocol.CredentialBearer, protocol.CredentialRelayAccess} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			requests := make(chan *http.Request, 1)
			server := newTTSServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
				requests <- request.Clone(request.Context())
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
			request.Plan.Route.Credential.Kind = kind
			request.Plan.Route.Credential.Value = "connector-rime-key"
			stream, err := adapter.Open(context.Background(), request)
			if err != nil {
				t.Fatalf("open relay stream: %v", err)
			}
			defer func() { _ = closeStream(t, stream) }()

			select {
			case received := <-requests:
				if got := received.Header.Get("Authorization"); got != "Bearer connector-rime-key" {
					t.Errorf("Authorization = %q", got)
				}
				if strings.Contains(received.URL.RawQuery, "connector-rime-key") {
					t.Errorf("handshake query leaked the connector key: %s", received.URL.RawQuery)
				}
			case <-time.After(time.Second):
				t.Fatal("server did not observe the relay handshake")
			}
		})
	}
}

func TestOpenHonoursCallerContextCancellation(t *testing.T) {
	t.Parallel()

	server := newTTSServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		waitForClientClose(ctx, conn)
	})
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = adapter.Open(ctx, adapterRequest(server.URL))
	var providerError *runtimepkg.ProviderError
	// A cancelled dial has no HTTP status, so it must classify as a transport
	// failure rather than be mistaken for an auth or quota problem.
	if !errorsAs(err, &providerError) || providerError.Code != "provider_unavailable" || providerError.ProviderStatus != 0 || !providerError.Retryable {
		t.Fatalf("open with cancelled context = %v", err)
	}
}

func TestAbortTearsDownTheSocketImmediately(t *testing.T) {
	t.Parallel()

	server := newTTSServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
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
	aborting, ok := stream.(runtimepkg.AbortingProviderStream)
	if !ok {
		t.Fatal("rime stream must support Abort for terminal runtime failures")
	}
	if err := aborting.Abort(context.Background()); err != nil {
		t.Fatalf("abort: %v", err)
	}
	select {
	case _, open := <-stream.Events():
		if open {
			// Drain: an in-flight event may already be queued, but the channel
			// must still reach closed.
			for range stream.Events() {
			}
		}
	case <-time.After(time.Second):
		t.Fatal("events must close after abort")
	}
	// Writes after a teardown must be refused rather than silently dropped.
	if err := stream.AppendText(context.Background(), "too late"); err != runtimepkg.ErrSessionClosed {
		t.Fatalf("append after abort = %v, want ErrSessionClosed", err)
	}
}

func TestUnsupportedOperationsAreRejected(t *testing.T) {
	t.Parallel()

	server := newTTSServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
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
	defer func() { _ = closeStream(t, stream) }()
	// Rime is TTS only: the audio-input half of ProviderStream has no meaning
	// here and must not look like a silent success.
	if err := stream.WriteAudio(context.Background(), []byte{0}); err != runtimepkg.ErrUnsupportedOperation {
		t.Fatalf("write audio = %v", err)
	}
	if err := stream.CommitAudio(context.Background()); err != runtimepkg.ErrUnsupportedOperation {
		t.Fatalf("commit audio = %v", err)
	}
	if err := stream.AppendText(context.Background(), "   "); err == nil {
		t.Fatal("blank transcript must be rejected before it reaches Rime")
	}
}

func TestNewRejectsUndocumentedSegmentMode(t *testing.T) {
	t.Parallel()

	// segment takes exactly three documented values; anything else is silently
	// ignored upstream and falls back to bySentence, which would break the
	// one-flush-one-done contract the stream state machine depends on.
	if _, err := New(Config{Segment: "perSentence"}); err == nil {
		t.Fatal("undocumented segment mode must be rejected")
	}
	for _, segment := range []string{"never", "bySentence", "immediate"} {
		if _, err := New(Config{Segment: segment}); err != nil {
			t.Fatalf("segment %q must be accepted: %v", segment, err)
		}
	}
	adapter, err := New(Config{})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	if adapter.ID() != "rime.tts.v1" {
		t.Fatalf("adapter id = %q", adapter.ID())
	}
}

// closeStream bounds every Close in this file. A regression that leaves Close
// waiting for a terminal event Rime is never going to send must fail the test,
// not hang the suite until the whole run times out.
func closeStream(t *testing.T, stream runtimepkg.ProviderStream) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return stream.Close(ctx)
}

func newTTSServer(t *testing.T, callback func(context.Context, *http.Request, *websocket.Conn)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ws3" {
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

// readMessage decodes into a map rather than the adapter's own outbound structs
// so the assertions see the literal JSON keys that go over the wire.
func readMessage(ctx context.Context, conn *websocket.Conn) (map[string]any, error) {
	messageType, payload, err := conn.Read(ctx)
	if err != nil || messageType != websocket.MessageText {
		return nil, fmt.Errorf("read = (%v, %q, %v)", messageType, payload, err)
	}
	var message map[string]any
	if err := json.Unmarshal(payload, &message); err != nil {
		return nil, err
	}
	return message, nil
}

func keysOf(message map[string]any) string {
	keys := make([]string, 0, len(message))
	for key := range message {
		keys = append(keys, key)
	}
	sortStrings(keys)
	return strings.Join(keys, ",")
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}

func writeJSON(ctx context.Context, conn *websocket.Conn, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, payload)
}

// waitForClientClose keeps the server side of the socket open, draining any
// frames the client sends, until the client closes or the context ends. A
// single Read is not enough: it returns on the first inbound message, the
// callback exits, and the deferred CloseNow tears the connection down under a
// test that is still mid-conversation.
func waitForClientClose(ctx context.Context, conn *websocket.Conn) {
	for {
		if _, _, err := conn.Read(ctx); err != nil {
			return
		}
	}
}

func adapterRequest(serverURL string) runtimepkg.AdapterRequest {
	now := time.Date(2026, time.August, 7, 11, 59, 0, 0, time.UTC)
	mediaFormat := protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1}
	return runtimepkg.AdapterRequest{
		Kind:    protocol.SessionKindTTS,
		Plan:    planFor(now, endpointFromServer(serverURL)),
		Options: protocol.RequestOptions{Voice: "astra", Language: "eng"},
		Media:   &mediaFormat,
	}
}

func testConfig(serverURL string) Config {
	endpoint, _ := url.Parse(serverURL)
	return Config{AllowedEndpointHosts: []string{endpoint.Hostname()}, AllowInsecureEndpoint: true}
}

func planFor(now time.Time, endpoint string) protocol.SessionPlan {
	return protocol.SessionPlan{
		PlanID: "plan_rime", SessionID: "sess_rime", AttemptID: "att_1",
		Execution: protocol.Execution{Placement: protocol.PlacementEmbedded, ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK},
		ExpiresAt: now.Add(time.Hour),
		Route: protocol.PlanRoute{
			Provider: "rime", Model: "coda", Adapter: AdapterID, Transport: protocol.TransportWebSocket, Endpoint: endpoint,
			Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "customer-rime-key", ExpiresAt: now.Add(30 * time.Minute)},
		},
		Reservation:  protocol.Reservation{ID: "res_rime", LeaseDurationSeconds: 60, LeaseExpiresAt: now.Add(time.Minute), RenewalURL: "https://control.speko.test/v1/sessions/sess_rime/lease-renewals", Concurrency: protocol.ConcurrencyReservation{LeaseID: "conc_rime", Slots: 1}, Usage: protocol.UsageReservation{Unit: protocol.UsageUnitCharacters, AuthorizedUnits: 4_000}},
		Telemetry:    protocol.Telemetry{Endpoint: "https://control.speko.test/v1/runtime-events", Token: "telemetry-token", FlushIntervalMS: 5_000},
		Requirements: protocol.Requirements{Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision, RuntimeVersion: "0.1.0"},
		Signature:    "test-signature",
	}
}

func endpointFromServer(serverURL string) string {
	endpoint, _ := url.Parse(serverURL)
	endpoint.Scheme = "ws"
	endpoint.Path = "/ws3"
	return endpoint.String()
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
			t.Fatal("timed out waiting for provider events")
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

func errorsAs(err error, target **runtimepkg.ProviderError) bool {
	providerError, ok := err.(*runtimepkg.ProviderError)
	if !ok {
		return false
	}
	*target = providerError
	return true
}
