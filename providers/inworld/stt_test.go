package inworld

import (
	"context"
	"encoding/json"
	"errors"
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

// Every Inworld wire string in this file is written out as a literal, never
// referenced from the adapter's own constants. An assertion that compares the
// adapter against itself passes just as happily on a misspelling, so the
// spelling has to be restated here from the AsyncAPI document to have any
// power. Mutating `modelId` to `model_id`, `isFinal` to `is_final`, or
// `streamBidirectional` to `streamBidirectionally` in stt.go must fail a test.
const (
	sttWirePath       = "/stt/v1/transcribe:streamBidirectional"
	sttWireEndTurn    = `{"endTurn":{}}`
	sttWireCloseFrame = `{"closeStream":{}}`
)

// TestSTTOpenSendsDocumentedConfigAndStreamsPartials is the end-to-end shape
// check: the mandatory first frame, base64 audio chunks, interim results that
// land WHILE audio is still being written, the manual finalize, and teardown.
func TestSTTOpenSendsDocumentedConfigAndStreamsPartials(t *testing.T) {
	t.Parallel()
	// The server answers every audioChunk with an interim transcript. That is
	// what makes this a streaming assertion rather than a batch one: the test
	// consumes a transcript.delta between two WriteAudio calls, so a delta that
	// only arrived after the last byte of audio would fail here.
	harness := newSTTHarness(t, func(ctx context.Context, conn *websocket.Conn, frame string) {
		if !strings.Contains(frame, `"audioChunk"`) {
			return
		}
		sttSend(t, ctx, conn, `{"result":{"transcription":{"transcript":"open the","isFinal":false,"silenceDurationMs":0}}}`)
	})

	adapter, err := NewSTT(sttTestSTTConfig(harness.server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), sttAdapterRequest(harness.server.URL, protocol.CredentialsBYOK))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	// The configuration frame must be the FIRST message and must carry the
	// lowerCamelCase names the AsyncAPI declares, wrapped in `transcribeConfig`.
	// Exact-string equality is deliberate: a field renamed, reordered, or
	// dropped changes this line.
	wantConfig := `{"transcribeConfig":{"modelId":"inworld/inworld-stt-1","audioEncoding":"LINEAR16","sampleRateHertz":16000,"numberOfChannels":1,"language":"en"}}`
	if got := harness.nextFrame(t); got != wantConfig {
		t.Fatalf("config frame =\n  %s\nwant\n  %s", got, wantConfig)
	}

	// Audio travels as standard padded base64 inside a JSON text frame, never
	// as a binary WebSocket frame. base64([]byte{1,2,3,4}) is "AQIDBA==".
	if err := stream.WriteAudio(context.Background(), []byte{1, 2, 3, 4}); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	if got, want := harness.nextFrame(t), `{"audioChunk":{"content":"AQIDBA=="}}`; got != want {
		t.Fatalf("audio frame = %s, want %s", got, want)
	}

	first := sttNextEvent(t, stream.Events())
	if first.Type != protocol.EventTranscriptDelta {
		t.Fatalf("first event = %q, want transcript.delta", first.Type)
	}
	if first.Extensions[sttExtensionID] == nil {
		t.Fatalf("delta lost the raw frame; extensions = %v", first.Extensions)
	}

	// Second chunk after the first partial was already consumed. This ordering
	// is the streaming proof.
	if err := stream.WriteAudio(context.Background(), []byte{5, 6}); err != nil {
		t.Fatalf("write second audio: %v", err)
	}
	if got, want := harness.nextFrame(t), `{"audioChunk":{"content":"BQY="}}`; got != want {
		t.Fatalf("second audio frame = %s, want %s", got, want)
	}
	if second := sttNextEvent(t, stream.Events()); second.Type != protocol.EventTranscriptDelta {
		t.Fatalf("second event = %q, want transcript.delta", second.Type)
	}

	// CommitAudio is the documented manual turn boundary. Inworld does not
	// finalize on socket close, so this frame is the only way a push-to-talk
	// caller gets its transcript.
	if err := stream.CommitAudio(context.Background()); err != nil {
		t.Fatalf("commit audio: %v", err)
	}
	if got := harness.nextFrame(t); got != sttWireEndTurn {
		t.Fatalf("commit frame = %s, want %s", got, sttWireEndTurn)
	}

	harness.push(t, `{"result":{"transcription":{"transcript":"open the door","isFinal":true,"silenceDurationMs":750}}}`)
	final := sttNextEvent(t, stream.Events())
	if final.Type != protocol.EventTranscriptFinal {
		t.Fatalf("final event = %q, want transcript.final", final.Type)
	}
	var decoded struct {
		Text              string `json:"text"`
		IsFinal           bool   `json:"is_final"`
		SilenceDurationMS int64  `json:"silence_duration_ms"`
	}
	if err := json.Unmarshal(final.Data, &decoded); err != nil {
		t.Fatalf("decode final data: %v", err)
	}
	if decoded.Text != "open the door" || !decoded.IsFinal || decoded.SilenceDurationMS != 750 {
		t.Fatalf("final data = %+v", decoded)
	}
	// A committed turn is also a speech boundary, so a consumer that only
	// watches speech events still learns the utterance ended.
	if ended := sttNextEvent(t, stream.Events()); ended.Type != protocol.EventSpeechEnded {
		t.Fatalf("event after final = %q, want speech.ended", ended.Type)
	}

	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}
	// The final already landed, so nothing is pending: Close must not force
	// another turn, only signal end of input.
	if got := harness.nextFrame(t); got != sttWireCloseFrame {
		t.Fatalf("close frame = %s, want %s", got, sttWireCloseFrame)
	}
}

// TestSTTCredentialSourceSelectsAuthChannel pins both credential channels. The
// two schemes are not interchangeable, and neither may reach the query string:
// this resource's AsyncAPI declares a query-parameter fallback for browsers,
// and using it from a server would publish the secret into every access log.
func TestSTTCredentialSourceSelectsAuthChannel(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name   string
		route  protocol.ProviderRoute
		source protocol.CredentialSource
		value  string
		want   string
	}{
		{
			// The portal key is already base64("key:secret"), so Inworld reads
			// it as a Basic credential. Sending it as a bearer token fails.
			name: "byok sends the portal key as Basic", route: protocol.RouteProviderDirect,
			source: protocol.CredentialsBYOK,
			value:  "customer-inworld-key", want: "Basic customer-inworld-key",
		},
		{
			// The control-plane JWT response is typed "type":"Bearer".
			name: "managed sends the minted jwt as Bearer", route: protocol.RouteProviderDirect,
			source: protocol.CredentialsManaged,
			value:  "minted.inworld.jwt", want: "Bearer minted.inworld.jwt",
		},
		{
			// A relay plan is managed for billing purposes but carries the relay
			// connector's permanent portal key, which is a Basic credential
			// exactly like a customer's own.
			name: "relay sends the connector key as Basic", route: protocol.RouteSpekoRelay,
			source: protocol.CredentialsManaged,
			value:  "connector-inworld-key", want: "Basic connector-inworld-key",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			harness := newSTTHarness(t, nil)
			adapter, err := NewSTT(sttTestSTTConfig(harness.server.URL))
			if err != nil {
				t.Fatalf("new STT adapter: %v", err)
			}
			request := sttAdapterRequest(harness.server.URL, testCase.source)
			request.Plan.Execution.ProviderRoute = testCase.route
			request.Plan.Route.Credential.Value = testCase.value
			if _, err := adapter.Open(context.Background(), request); err != nil {
				t.Fatalf("open stream: %v", err)
			}
			received := harness.nextRequest(t)
			if got := received.Header.Get("Authorization"); got != testCase.want {
				t.Fatalf("Authorization header = %q, want %q", got, testCase.want)
			}
			// Inworld names the query-parameter scheme `authorization`. It must
			// stay empty for both sources.
			if got := received.URL.Query().Get("authorization"); got != "" {
				t.Fatalf("credential leaked into the query string: %q", got)
			}
			if received.URL.RawQuery != "" {
				t.Fatalf("handshake carried an unexpected query string: %q", received.URL.RawQuery)
			}
			if got := received.URL.Path; got != sttWirePath {
				t.Fatalf("handshake path = %q, want %q", got, sttWirePath)
			}
		})
	}
}

// TestSTTRelayRouteAcceptsRelayAccessCredentialKind pins the relay arm's other
// credential spelling: protocol.SessionPlan validation requires a relay plan
// to label its credential relay_access, while the relay connector that
// synthesizes the plan and drives this adapter directly labels the same
// permanent key bearer. Both must open, and both take the Basic channel.
func TestSTTRelayRouteAcceptsRelayAccessCredentialKind(t *testing.T) {
	t.Parallel()
	harness := newSTTHarness(t, nil)
	sttOpenStream(t, harness, protocol.CredentialsManaged, func(request *runtimepkg.AdapterRequest) {
		request.Plan.Execution.ProviderRoute = protocol.RouteSpekoRelay
		request.Plan.Route.Credential.Kind = protocol.CredentialRelayAccess
		request.Plan.Route.Credential.Value = "connector-inworld-key"
	})
	received := harness.nextRequest(t)
	if got := received.Header.Get("Authorization"); got != "Basic connector-inworld-key" {
		t.Fatalf("Authorization header = %q, want the Basic channel", got)
	}
}

// TestSTTMapsEveryDocumentedResultFrame walks the four server messages the
// AsyncAPI declares. Each is written here in its documented shape — wrapped in
// a top-level `result` — so a wrong wrapper or a wrong field name shows up as a
// missing or misclassified event rather than as a silently ignored frame.
func TestSTTMapsEveryDocumentedResultFrame(t *testing.T) {
	t.Parallel()
	harness := newSTTHarness(t, nil)
	stream := sttOpenStream(t, harness, protocol.CredentialsManaged, nil)
	harness.nextFrame(t) // configuration frame

	harness.push(t, `{"result":{"speechStarted":{"startTimeMs":120,"confidence":0.87}}}`)
	started := sttNextEvent(t, stream.Events())
	if started.Type != protocol.EventSpeechStarted {
		t.Fatalf("speechStarted mapped to %q", started.Type)
	}
	var startData struct {
		AudioStartMS int64   `json:"audio_start_ms"`
		Confidence   float64 `json:"confidence"`
	}
	if err := json.Unmarshal(started.Data, &startData); err != nil || startData.AudioStartMS != 120 || startData.Confidence != 0.87 {
		t.Fatalf("speechStarted data = %+v, err=%v", startData, err)
	}

	// speechStopped is voice-activity silence, not the turn boundary; it maps
	// to speech.ended with a reason that keeps it distinguishable from the
	// end_of_turn one a final produces.
	harness.push(t, `{"result":{"speechStopped":{"silenceDurationMs":750}}}`)
	stopped := sttNextEvent(t, stream.Events())
	if stopped.Type != protocol.EventSpeechEnded {
		t.Fatalf("speechStopped mapped to %q", stopped.Type)
	}
	var stopData struct {
		Reason            string `json:"reason"`
		SilenceDurationMS int64  `json:"silence_duration_ms"`
	}
	if err := json.Unmarshal(stopped.Data, &stopData); err != nil || stopData.Reason != "speech_stopped" || stopData.SilenceDurationMS != 750 {
		t.Fatalf("speechStopped data = %+v, err=%v", stopData, err)
	}

	// Usage is what meters the session; a renamed field here would bill zero.
	harness.push(t, `{"result":{"usage":{"transcribedAudioMs":2400,"modelId":"inworld/inworld-stt-1"}}}`)
	usage := sttNextEvent(t, stream.Events())
	if usage.Type != protocol.EventUsageObserved {
		t.Fatalf("usage mapped to %q", usage.Type)
	}
	var usageData struct {
		AudioDurationMS int64  `json:"audio_duration_ms"`
		ModelID         string `json:"model_id"`
	}
	if err := json.Unmarshal(usage.Data, &usageData); err != nil || usageData.AudioDurationMS != 2400 || usageData.ModelID != "inworld/inworld-stt-1" {
		t.Fatalf("usage data = %+v, err=%v", usageData, err)
	}

	// A frame this adapter does not model is surfaced, not dropped, so a
	// message type Inworld adds later is visible in the event stream.
	harness.push(t, `{"result":{"somethingNewInworldAdded":{}}}`)
	if warning := sttNextEvent(t, stream.Events()); warning.Type != protocol.EventWarning {
		t.Fatalf("unknown result mapped to %q, want warning", warning.Type)
	}
}

// TestSTTDropsTheEmptyFinalMarker guards the quirk that costs the most if it
// regresses. Inworld emits `isFinal: true` with an empty transcript at the
// start of a turn; forwarding it hands the runtime a completed empty utterance,
// which a voice agent answers.
func TestSTTDropsTheEmptyFinalMarker(t *testing.T) {
	t.Parallel()
	harness := newSTTHarness(t, nil)
	stream := sttOpenStream(t, harness, protocol.CredentialsManaged, nil)
	harness.nextFrame(t)

	harness.push(t, `{"result":{"transcription":{"transcript":"","isFinal":true,"silenceDurationMs":0}}}`)
	harness.push(t, `{"result":{"transcription":{"transcript":"   ","isFinal":false,"silenceDurationMs":0}}}`)
	harness.push(t, `{"result":{"transcription":{"transcript":"hello","isFinal":true,"silenceDurationMs":0}}}`)

	// The first real event must be the "hello" final. If either marker leaked
	// through it arrives here first.
	event := sttNextEvent(t, stream.Events())
	if event.Type != protocol.EventTranscriptFinal {
		t.Fatalf("first event = %q, want transcript.final", event.Type)
	}
	var decoded struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(event.Data, &decoded); err != nil || decoded.Text != "hello" {
		t.Fatalf("first transcript = %+v, err=%v", decoded, err)
	}
}

// TestSTTEmitsEmptyFinalAfterExplicitCommit keeps silent batch audio from
// hanging. The startup empty marker is noise, but the same shape after our
// endTurn is the provider's acknowledgement that the requested turn finished.
func TestSTTEmitsEmptyFinalAfterExplicitCommit(t *testing.T) {
	t.Parallel()
	harness := newSTTHarness(t, nil)
	stream := sttOpenStream(t, harness, protocol.CredentialsManaged, nil)
	harness.nextFrame(t)

	if err := stream.CommitAudio(context.Background()); err != nil {
		t.Fatalf("commit audio: %v", err)
	}
	if got := harness.nextFrame(t); got != sttWireEndTurn {
		t.Fatalf("commit frame = %s, want %s", got, sttWireEndTurn)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}
	if got := harness.nextFrame(t); got != sttWireCloseFrame {
		t.Fatalf("close frame = %s, want %s", got, sttWireCloseFrame)
	}
	harness.push(t, `{"result":{"transcription":{"transcript":"","isFinal":true,"silenceDurationMs":0}}}`)

	final := sttNextEvent(t, stream.Events())
	if final.Type != protocol.EventTranscriptFinal {
		t.Fatalf("event = %q, want transcript.final", final.Type)
	}
	var decoded struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(final.Data, &decoded); err != nil || decoded.Text != "" {
		t.Fatalf("empty final = %+v, err=%v", decoded, err)
	}
	if ended := sttNextEvent(t, stream.Events()); ended.Type != protocol.EventSpeechEnded {
		t.Fatalf("second event = %q, want speech.ended", ended.Type)
	}
	select {
	case _, ok := <-stream.Events():
		if ok {
			t.Fatal("events remained open after the committed final acknowledged Close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("events did not close after the committed final")
	}
}

func TestSTTReadySilentSessionEventuallyCloses(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &sttStream{ctx: ctx, cancel: cancel}
	stream.closing.Store(true)
	stream.readySeen.Store(true)
	go stream.finishSilentClose()

	select {
	case <-ctx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("ready silent Inworld session did not close after its grace period")
	}
}

// TestSTTCloseForcesAPendingTurnBeforeEndOfInput covers the most visible
// transcriber failure: tearing down with a turn in flight. Inworld does not
// commit on close, so the trailing utterance is lost unless endTurn goes first.
func TestSTTCloseForcesAPendingTurnBeforeEndOfInput(t *testing.T) {
	t.Parallel()
	harness := newSTTHarness(t, nil)
	stream := sttOpenStream(t, harness, protocol.CredentialsManaged, nil)
	harness.nextFrame(t)

	harness.push(t, `{"result":{"transcription":{"transcript":"half a sen","isFinal":false,"silenceDurationMs":0}}}`)
	if event := sttNextEvent(t, stream.Events()); event.Type != protocol.EventTranscriptDelta {
		t.Fatalf("event = %q, want transcript.delta", event.Type)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}
	if got := harness.nextFrame(t); got != sttWireEndTurn {
		t.Fatalf("first teardown frame = %s, want %s", got, sttWireEndTurn)
	}
	if got := harness.nextFrame(t); got != sttWireCloseFrame {
		t.Fatalf("second teardown frame = %s, want %s", got, sttWireCloseFrame)
	}
}

// TestSTTCloseWithoutASpokenTurnOnlySignalsEndOfInput is the other half: an
// endTurn with nothing in flight just makes Inworld emit another empty marker.
func TestSTTCloseWithoutASpokenTurnOnlySignalsEndOfInput(t *testing.T) {
	t.Parallel()
	harness := newSTTHarness(t, nil)
	stream := sttOpenStream(t, harness, protocol.CredentialsManaged, nil)
	harness.nextFrame(t)

	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}
	if got := harness.nextFrame(t); got != sttWireCloseFrame {
		t.Fatalf("teardown frame = %s, want %s", got, sttWireCloseFrame)
	}
	harness.expectNoFrame(t)
}

// TestSTTCancelDiscardsTheTranscript pins the difference from Close: a caller
// that cancels does not want the pending utterance, so nothing is flushed.
func TestSTTCancelDiscardsTheTranscript(t *testing.T) {
	t.Parallel()
	harness := newSTTHarness(t, nil)
	stream := sttOpenStream(t, harness, protocol.CredentialsManaged, nil)
	harness.nextFrame(t)

	harness.push(t, `{"result":{"transcription":{"transcript":"do not want","isFinal":false,"silenceDurationMs":0}}}`)
	if event := sttNextEvent(t, stream.Events()); event.Type != protocol.EventTranscriptDelta {
		t.Fatalf("event = %q, want transcript.delta", event.Type)
	}
	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatalf("cancel stream: %v", err)
	}
	harness.expectNoFrame(t)
	// A cancelled stream is closed for writes.
	if err := stream.WriteAudio(context.Background(), []byte{1, 2, 3, 4}); !errors.Is(err, runtimepkg.ErrSessionClosed) {
		t.Fatalf("write after cancel = %v, want ErrSessionClosed", err)
	}
}

// TestSTTErrorFramesMapToDistinctCodes covers the gateway's gRPC-transcoded
// error object. The AsyncAPI declares no error frame for this channel, so the
// shape comes from the REST twin and from the schema's own note that a rejected
// `prompts` value returns INVALID_ARGUMENT (code 3). Collapsing these onto one
// code would make a dead credential look retryable.
func TestSTTErrorFramesMapToDistinctCodes(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name      string
		frame     string
		code      string
		retryable bool
	}{
		{"unauthenticated", `{"error":{"code":16,"message":"invalid api key"}}`, "authentication_failed", false},
		{"permission denied", `{"error":{"code":7,"message":"forbidden"}}`, "authentication_failed", false},
		{"resource exhausted", `{"error":{"code":8,"message":"quota"}}`, "provider_rate_limited", true},
		{"internal", `{"error":{"code":13,"message":"boom"}}`, "provider_unavailable", true},
		{"invalid argument", `{"error":{"code":3,"message":"bad prompts"}}`, "invalid_request", false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			harness := newSTTHarness(t, nil)
			stream := sttOpenStream(t, harness, protocol.CredentialsManaged, nil)
			harness.nextFrame(t)
			harness.push(t, testCase.frame)

			event := sttNextEventAllowingError(t, stream.Events())
			var providerErr *runtimepkg.ProviderError
			if !errors.As(event.Err, &providerErr) {
				t.Fatalf("event = %#v, want a ProviderError", event)
			}
			if providerErr.Code != testCase.code || providerErr.Retryable != testCase.retryable {
				t.Fatalf("error = (%q, retryable=%v), want (%q, %v)", providerErr.Code, providerErr.Retryable, testCase.code, testCase.retryable)
			}
			// A gRPC status is not an HTTP status. Reporting it as one would
			// have a consumer read code 13 as "HTTP 13".
			if providerErr.ProviderStatus != 0 {
				t.Fatalf("gRPC code leaked into ProviderStatus as %d", providerErr.ProviderStatus)
			}
			if providerErr.Extensions[sttExtensionID] == nil {
				t.Fatalf("error dropped the raw frame; extensions = %v", providerErr.Extensions)
			}
		})
	}
}

// TestSTTHandshakeFailuresMapToDistinctCodes covers the other error surface.
// These ARE HTTP statuses, so ProviderStatus carries them.
func TestSTTHandshakeFailuresMapToDistinctCodes(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		status    int
		code      string
		retryable bool
	}{
		{http.StatusUnauthorized, "authentication_failed", false},
		{http.StatusForbidden, "authentication_failed", false},
		{http.StatusPaymentRequired, "provider_quota_exceeded", false},
		{http.StatusTooManyRequests, "provider_rate_limited", true},
		{http.StatusBadRequest, "invalid_request", false},
		{http.StatusBadGateway, "provider_unavailable", true},
	} {
		t.Run(http.StatusText(testCase.status), func(t *testing.T) {
			t.Parallel()
			status := testCase.status
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
			}))
			defer server.Close()
			adapter, err := NewSTT(sttTestSTTConfig(server.URL))
			if err != nil {
				t.Fatalf("new STT adapter: %v", err)
			}
			_, err = adapter.Open(context.Background(), sttAdapterRequest(server.URL, protocol.CredentialsManaged))
			var providerErr *runtimepkg.ProviderError
			if !errors.As(err, &providerErr) {
				t.Fatalf("open error = %v, want a ProviderError", err)
			}
			if providerErr.Code != testCase.code || providerErr.Retryable != testCase.retryable || providerErr.ProviderStatus != status {
				t.Fatalf("dial error = (%q, retryable=%v, status=%d), want (%q, %v, %d)",
					providerErr.Code, providerErr.Retryable, providerErr.ProviderStatus, testCase.code, testCase.retryable, status)
			}
		})
	}
}

// TestSTTOpenRejectsUnusableRequests keeps every precondition failing locally
// instead of as an upstream surprise mid-call.
func TestSTTOpenRejectsUnusableRequests(t *testing.T) {
	t.Parallel()
	harness := newSTTHarness(t, nil)
	adapter, err := NewSTT(sttTestSTTConfig(harness.server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}

	for _, testCase := range []struct {
		name    string
		mutate  func(*runtimepkg.AdapterRequest)
		wantSub string
	}{
		{
			// A synthesis plan reaching a transcriber means the control plane
			// chose the wrong adapter.
			name: "tts kind", wantSub: "stt sessions",
			mutate: func(r *runtimepkg.AdapterRequest) { r.Kind = protocol.SessionKindTTS },
		},
		{
			name: "foreign provider", wantSub: "cannot open provider",
			mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Provider = "deepgram" },
		},
		{
			// The sibling Inworld TTS adapter is the HTTP one. This resource is
			// a socket, so an http plan selected an adapter that cannot run it.
			name: "http transport", wantSub: "websocket transport",
			mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Transport = protocol.TransportHTTP },
		},
		{
			name: "missing media", wantSub: "media configuration",
			mutate: func(r *runtimepkg.AdapterRequest) { r.Media = nil },
		},
		{
			// LINEAR16 is the only streaming encoding. Declaring opus would
			// produce a healthy-looking session transcribing garbage.
			name: "opus media", wantSub: "pcm_s16le",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Media = &protocol.MediaFormat{Encoding: "opus", SampleRateHz: 16_000, Channels: 1}
			},
		},
		{
			// "auto" is the control plane's placeholder. An adapter must be
			// given the resolved model, not asked to invent one.
			name: "auto model", wantSub: "concrete model",
			mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "auto" },
		},
		{
			name: "empty model", wantSub: "concrete model",
			mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "  " },
		},
		{
			name: "unknown model", wantSub: `inworld/inworld-stt-9`,
			mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "inworld-stt-9" },
		},
		{
			// Documented on the STT API but not on this resource. The rejection
			// names the reason so the caller knows to use the sync endpoint.
			name: "sync-only model", wantSub: "synchronous transcribe endpoint",
			mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "groq/whisper-large-v3" },
		},
		{
			name: "unsupported language", wantSub: `language "bn"`,
			mutate: func(r *runtimepkg.AdapterRequest) { r.Options.Language = "bn" },
		},
		{
			name: "missing credential", wantSub: "bearer credential",
			mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential = nil },
		},
		{
			// relay_access is accepted only on the relay route; on
			// provider-direct it means the control plane mislabeled the plan.
			name: "relay credential off the relay route", wantSub: "bearer credential",
			mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential.Kind = protocol.CredentialRelayAccess },
		},
		{
			name: "blank credential", wantSub: "bearer credential",
			mutate: func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential.Value = "   " },
		},
		{
			// A plan pointing at another path on the same host is still wrong:
			// this adapter only speaks the bidirectional transcription resource.
			name: "wrong endpoint path", wantSub: "endpoint path must be",
			mutate: func(r *runtimepkg.AdapterRequest) {
				endpoint, _ := url.Parse(r.Plan.Route.Endpoint)
				endpoint.Path = "/tts/v1/voice:streamBidirectional"
				r.Plan.Route.Endpoint = endpoint.String()
			},
		},
		{
			// The endpoint allowlist is the last line before a customer key is
			// attached to a request.
			name: "foreign endpoint host", wantSub: "endpoint",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Plan.Route.Endpoint = "wss://attacker.example.com" + sttWirePath
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			request := sttAdapterRequest(harness.server.URL, protocol.CredentialsManaged)
			testCase.mutate(&request)
			_, err := adapter.Open(context.Background(), request)
			if err == nil {
				t.Fatalf("open succeeded, want an error containing %q", testCase.wantSub)
			}
			if !strings.Contains(err.Error(), testCase.wantSub) {
				t.Fatalf("open error = %q, want it to contain %q", err.Error(), testCase.wantSub)
			}
		})
	}
}

// TestSTTModelIDQualification pins how a plan's model reaches the wire. The
// platform carries the bare first-party name; a fully qualified third-party id
// must survive untouched, because re-qualifying it would produce
// `inworld/deepgram/...`.
func TestSTTModelIDQualification(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct{ planModel, wantModelID string }{
		{"inworld-stt-1", "inworld/inworld-stt-1"},
		{"inworld/inworld-stt-1", "inworld/inworld-stt-1"},
		{"assemblyai/u3-rt-pro", "assemblyai/u3-rt-pro"},
		{"soniox/stt-rt-v5", "soniox/stt-rt-v5"},
		{"deepgram/flux-general-multi", "deepgram/flux-general-multi"},
	} {
		t.Run(testCase.planModel, func(t *testing.T) {
			t.Parallel()
			harness := newSTTHarness(t, nil)
			planModel := testCase.planModel
			sttOpenStream(t, harness, protocol.CredentialsManaged, func(r *runtimepkg.AdapterRequest) {
				r.Plan.Route.Model = planModel
				// Cleared so the assertion below is only about modelId.
				r.Options.Language = ""
			})
			want := `{"transcribeConfig":{"modelId":"` + testCase.wantModelID + `","audioEncoding":"LINEAR16","sampleRateHertz":16000,"numberOfChannels":1}}`
			if got := harness.nextFrame(t); got != want {
				t.Fatalf("config frame =\n  %s\nwant\n  %s", got, want)
			}
		})
	}
}

// TestSTTLanguageHintNormalization pins the hint. It is not cosmetic: on the
// first-party model the hint also pins the output SCRIPT, so a dropped hint
// changes the transcript. The 30-language check applies to that model only —
// third-party models keep their own coverage.
func TestSTTLanguageHintNormalization(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name, model, language, wantJSON string
	}{
		// BCP-47 is reduced locally to the base subtag Inworld stores anyway,
		// so the value validated is the value sent.
		{"regional tag reduces to base", "inworld-stt-1", "en-US", `,"language":"en"`},
		{"underscore form reduces too", "inworld-stt-1", "es_419", `,"language":"es"`},
		{"three letter subtag survives", "inworld-stt-1", "yue", `,"language":"yue"`},
		{"empty hint is omitted entirely", "inworld-stt-1", "", ``},
		// `bn` is outside Inworld's 30, but Deepgram Flux multi has its own
		// coverage and Inworld publishes no unified list, so it passes through.
		{"third party hint passes through", "deepgram/flux-general-multi", "bn", `,"language":"bn"`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			harness := newSTTHarness(t, nil)
			model, language := testCase.model, testCase.language
			sttOpenStream(t, harness, protocol.CredentialsManaged, func(r *runtimepkg.AdapterRequest) {
				r.Plan.Route.Model = model
				r.Options.Language = language
			})
			frame := harness.nextFrame(t)
			if !strings.HasSuffix(frame, `"numberOfChannels":1`+testCase.wantJSON+`}}`) {
				t.Fatalf("config frame = %s, want it to end with %q", frame, `"numberOfChannels":1`+testCase.wantJSON+`}}`)
			}
		})
	}
}

// TestSTTDeclaresTheCallersRealMediaShape guards the two fields Inworld cannot
// infer from raw LINEAR16. Omitting them silently falls back to 16 kHz mono,
// which transcribes an 8 kHz telephony leg at the wrong rate.
func TestSTTDeclaresTheCallersRealMediaShape(t *testing.T) {
	t.Parallel()
	harness := newSTTHarness(t, nil)
	sttOpenStream(t, harness, protocol.CredentialsManaged, func(r *runtimepkg.AdapterRequest) {
		r.Media = &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 8_000, Channels: 2}
		r.Options.Language = ""
	})
	want := `{"transcribeConfig":{"modelId":"inworld/inworld-stt-1","audioEncoding":"LINEAR16","sampleRateHertz":8000,"numberOfChannels":2}}`
	if got := harness.nextFrame(t); got != want {
		t.Fatalf("config frame =\n  %s\nwant\n  %s", got, want)
	}
}

// TestSTTRejectsTextInput proves the transcriber reports the mismatch rather
// than quietly discarding a caller's synthesis text.
func TestSTTRejectsTextInput(t *testing.T) {
	t.Parallel()
	harness := newSTTHarness(t, nil)
	stream := sttOpenStream(t, harness, protocol.CredentialsManaged, nil)
	harness.nextFrame(t)

	if err := stream.AppendText(context.Background(), "speak this"); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Fatalf("AppendText = %v, want ErrUnsupportedOperation", err)
	}
	if err := stream.CommitText(context.Background()); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Fatalf("CommitText = %v, want ErrUnsupportedOperation", err)
	}
	if err := stream.WriteAudio(context.Background(), nil); err == nil {
		t.Fatal("empty WriteAudio succeeded")
	}
}

// TestSTTStripsAContainerHeaderFromInput covers the measured silent failure:
// Inworld's LINEAR16 decoder treats a RIFF header delivered as audio as fatal
// and then transcribes nothing at all, with no error to notice.
func TestSTTStripsAContainerHeaderFromInput(t *testing.T) {
	t.Parallel()
	t.Run("whole header in the first chunk", func(t *testing.T) {
		t.Parallel()
		harness := newSTTHarness(t, nil)
		stream := sttOpenStream(t, harness, protocol.CredentialsManaged, nil)
		harness.nextFrame(t)
		if err := stream.WriteAudio(context.Background(), sttWAV([]byte{1, 2, 3, 4})); err != nil {
			t.Fatalf("write audio: %v", err)
		}
		if got, want := harness.nextFrame(t), `{"audioChunk":{"content":"AQIDBA=="}}`; got != want {
			t.Fatalf("audio frame = %s, want %s", got, want)
		}
	})

	t.Run("header split across chunks", func(t *testing.T) {
		t.Parallel()
		// WebSocket preserves frame boundaries, so the header really can arrive
		// on its own. Nothing may be forwarded until the guard can locate the
		// `data` tag.
		harness := newSTTHarness(t, nil)
		stream := sttOpenStream(t, harness, protocol.CredentialsManaged, nil)
		harness.nextFrame(t)
		wav := sttWAV([]byte{1, 2, 3, 4})
		if err := stream.WriteAudio(context.Background(), wav[:20]); err != nil {
			t.Fatalf("write header prefix: %v", err)
		}
		harness.expectNoFrame(t)
		if err := stream.WriteAudio(context.Background(), wav[20:]); err != nil {
			t.Fatalf("write header tail: %v", err)
		}
		if got, want := harness.nextFrame(t), `{"audioChunk":{"content":"AQIDBA=="}}`; got != want {
			t.Fatalf("audio frame = %s, want %s", got, want)
		}
	})

	t.Run("raw pcm is forwarded untouched", func(t *testing.T) {
		t.Parallel()
		// The guard must be inert on the contract-conforming path: this is what
		// every real session sends.
		harness := newSTTHarness(t, nil)
		stream := sttOpenStream(t, harness, protocol.CredentialsManaged, nil)
		harness.nextFrame(t)
		if err := stream.WriteAudio(context.Background(), []byte{1, 2, 3, 4}); err != nil {
			t.Fatalf("write audio: %v", err)
		}
		if got, want := harness.nextFrame(t), `{"audioChunk":{"content":"AQIDBA=="}}`; got != want {
			t.Fatalf("first frame = %s, want %s", got, want)
		}
		if err := stream.WriteAudio(context.Background(), []byte{5, 6, 7, 8}); err != nil {
			t.Fatalf("write second audio: %v", err)
		}
		if got, want := harness.nextFrame(t), `{"audioChunk":{"content":"BQYHCA=="}}`; got != want {
			t.Fatalf("second frame = %s, want %s", got, want)
		}
	})

	t.Run("sub magic-word chunks are buffered not dropped", func(t *testing.T) {
		t.Parallel()
		// A chunk shorter than the 4-byte magic cannot be classified yet. It
		// must be held, not discarded, or the first samples vanish.
		harness := newSTTHarness(t, nil)
		stream := sttOpenStream(t, harness, protocol.CredentialsManaged, nil)
		harness.nextFrame(t)
		if err := stream.WriteAudio(context.Background(), []byte{1, 2}); err != nil {
			t.Fatalf("write first pair: %v", err)
		}
		harness.expectNoFrame(t)
		if err := stream.WriteAudio(context.Background(), []byte{3, 4}); err != nil {
			t.Fatalf("write second pair: %v", err)
		}
		if got, want := harness.nextFrame(t), `{"audioChunk":{"content":"AQIDBA=="}}`; got != want {
			t.Fatalf("audio frame = %s, want %s", got, want)
		}
	})
}

// TestSTTMalformedFrameTerminatesTheAttempt keeps a broken upstream from
// looking like a stream that simply went quiet.
func TestSTTMalformedFrameTerminatesTheAttempt(t *testing.T) {
	t.Parallel()
	harness := newSTTHarness(t, nil)
	stream := sttOpenStream(t, harness, protocol.CredentialsManaged, nil)
	harness.nextFrame(t)
	harness.push(t, `{"result":`)

	event := sttNextEventAllowingError(t, stream.Events())
	var providerErr *runtimepkg.ProviderError
	if !errors.As(event.Err, &providerErr) || providerErr.Code != "provider_unavailable" || !providerErr.Retryable {
		t.Fatalf("malformed frame error = %#v", event.Err)
	}
}

// TestSTTAdapterIdentity pins the two values a session plan is matched against.
func TestSTTAdapterIdentity(t *testing.T) {
	t.Parallel()
	adapter, err := NewSTT(STTConfig{})
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	if got := adapter.ID(); got != "inworld.stt.v1" {
		t.Fatalf("adapter id = %q, want inworld.stt.v1", got)
	}
	// The TTS adapter in this package must keep its own identity.
	if STTAdapterID == AdapterID {
		t.Fatalf("STT and TTS adapters share the id %q", STTAdapterID)
	}
	if sttExtensionID == extensionID {
		t.Fatalf("STT and TTS share the extension namespace %q", sttExtensionID)
	}
	if got := STTDefaultModel; got != "inworld-stt-1" {
		t.Fatalf("default model = %q, want inworld-stt-1", got)
	}
	if _, err := NewSTT(STTConfig{EventBuffer: -1}); err == nil {
		t.Fatal("negative event buffer accepted")
	}
	if _, err := NewSTT(STTConfig{MaxMessageBytes: -1}); err == nil {
		t.Fatal("negative message limit accepted")
	}
}

// --- harness -----------------------------------------------------------

// sttHarness is a fake Inworld socket. It records the handshake and every
// client frame, and lets a test push server frames back.
type sttHarness struct {
	server   *httptest.Server
	requests chan *http.Request
	frames   chan string
	conns    chan *websocket.Conn
	conn     *websocket.Conn
}

// newSTTHarness serves the documented path only, so a path mistake in the
// adapter shows up as a 404 handshake rather than a passing test. respond, when
// set, is called for each client frame and may write server frames.
func newSTTHarness(t *testing.T, respond func(context.Context, *websocket.Conn, string)) *sttHarness {
	t.Helper()
	harness := &sttHarness{
		requests: make(chan *http.Request, 1),
		frames:   make(chan string, 64),
		conns:    make(chan *websocket.Conn, 1),
	}
	harness.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != sttWirePath {
			http.NotFound(w, r)
			return
		}
		harness.requests <- r.Clone(r.Context())
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		harness.conns <- conn
		go func() {
			ctx := context.Background()
			for {
				messageType, payload, err := conn.Read(ctx)
				if err != nil {
					return
				}
				if messageType != websocket.MessageText {
					t.Errorf("client sent a %v frame; Inworld STT reads JSON text only", messageType)
					return
				}
				harness.frames <- string(payload)
				if respond != nil {
					respond(ctx, conn, string(payload))
				}
			}
		}()
	}))
	t.Cleanup(harness.server.Close)
	return harness
}

func (h *sttHarness) nextRequest(t *testing.T) *http.Request {
	t.Helper()
	select {
	case request := <-h.requests:
		return request
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the handshake")
		return nil
	}
}

func (h *sttHarness) nextFrame(t *testing.T) string {
	t.Helper()
	select {
	case frame := <-h.frames:
		return frame
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a client frame")
		return ""
	}
}

// expectNoFrame asserts the adapter stayed silent. The window is short on
// purpose: this is a negative assertion, and a long one only slows the suite.
func (h *sttHarness) expectNoFrame(t *testing.T) {
	t.Helper()
	select {
	case frame := <-h.frames:
		t.Fatalf("unexpected client frame %s", frame)
	case <-time.After(150 * time.Millisecond):
	}
}

// push writes one server frame, waiting for the connection if the test has not
// consumed it yet.
func (h *sttHarness) push(t *testing.T, frame string) {
	t.Helper()
	if h.conn == nil {
		select {
		case conn := <-h.conns:
			h.conn = conn
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for the server connection")
		}
	}
	sttSend(t, context.Background(), h.conn, frame)
}

func sttSend(t *testing.T, ctx context.Context, conn *websocket.Conn, frame string) {
	t.Helper()
	if err := conn.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
		t.Errorf("write server frame: %v", err)
	}
}

func sttOpenStream(t *testing.T, harness *sttHarness, source protocol.CredentialSource, mutate func(*runtimepkg.AdapterRequest)) runtimepkg.ProviderStream {
	t.Helper()
	adapter, err := NewSTT(sttTestSTTConfig(harness.server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	request := sttAdapterRequest(harness.server.URL, source)
	if mutate != nil {
		mutate(&request)
	}
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	t.Cleanup(func() {
		if aborting, ok := stream.(runtimepkg.AbortingProviderStream); ok {
			_ = aborting.Abort(context.Background())
		}
	})
	return stream
}

func sttTestSTTConfig(serverURL string) STTConfig {
	endpoint, _ := url.Parse(serverURL)
	return STTConfig{AllowedEndpointHosts: []string{endpoint.Hostname()}, AllowInsecureEndpoint: true}
}

func sttAdapterRequest(serverURL string, source protocol.CredentialSource) runtimepkg.AdapterRequest {
	now := time.Date(2026, time.August, 7, 12, 0, 0, 0, time.UTC)
	endpoint, _ := url.Parse(serverURL)
	endpoint.Scheme = "ws"
	endpoint.Path = sttWirePath
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindSTT,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{
				Placement:        protocol.PlacementEmbedded,
				ProviderRoute:    protocol.RouteProviderDirect,
				CredentialSource: source,
			},
			Route: protocol.PlanRoute{
				Provider:  "inworld",
				Model:     "inworld-stt-1",
				Adapter:   STTAdapterID,
				Transport: protocol.TransportWebSocket,
				Endpoint:  endpoint.String(),
				Credential: &protocol.DelegatedCredential{
					Kind:      protocol.CredentialBearer,
					Value:     "inworld-credential",
					ExpiresAt: now.Add(time.Minute),
				},
			},
		},
		Options: protocol.RequestOptions{Language: "en"},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
	}
}

func sttNextEvent(t *testing.T, events <-chan runtimepkg.ProviderEvent) runtimepkg.ProviderEvent {
	t.Helper()
	event := sttNextEventAllowingError(t, events)
	if event.Err != nil {
		t.Fatalf("unexpected provider error: %v", event.Err)
	}
	return event
}

func sttNextEventAllowingError(t *testing.T, events <-chan runtimepkg.ProviderEvent) runtimepkg.ProviderEvent {
	t.Helper()
	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("event channel closed early")
		}
		return event
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a provider event")
		return runtimepkg.ProviderEvent{}
	}
}

// sttWAV wraps PCM in a canonical 44-byte RIFF container. The `data` tag sits
// at offset 36, past the fmt chunk, which is why the guard searches for it
// rather than assuming a fixed offset.
func sttWAV(pcm []byte) []byte {
	header := make([]byte, 0, 44+len(pcm))
	header = append(header, "RIFF"...)
	header = append(header, byte(36+len(pcm)), 0, 0, 0)
	header = append(header, "WAVE"...)
	header = append(header, "fmt "...)
	header = append(header, 16, 0, 0, 0)
	header = append(header, 1, 0)             // PCM
	header = append(header, 1, 0)             // mono
	header = append(header, 0x80, 0x3e, 0, 0) // 16000 Hz
	header = append(header, 0x00, 0x7d, 0, 0) // byte rate
	header = append(header, 2, 0)             // block align
	header = append(header, 16, 0)            // bits per sample
	header = append(header, "data"...)
	header = append(header, byte(len(pcm)), 0, 0, 0)
	return append(header, pcm...)
}
