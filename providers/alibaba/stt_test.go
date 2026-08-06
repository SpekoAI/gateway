package alibaba

import (
	"context"
	"encoding/base64"
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

// Every vendor string in this file is transcribed as a literal rather than
// referenced from the adapter's own constants. An assertion written against
// the constant it is meant to verify passes even when the constant is
// misspelled, which is exactly the bug these tests exist to catch.

func TestSTTHandshakeAndSessionUpdateMatchTheDocumentedWireShape(t *testing.T) {
	t.Parallel()
	harness := newRealtimeHarness(t)
	defer harness.Close()

	adapter, err := NewSTT(STTConfig{AllowedEndpointHosts: []string{harness.host}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), sttRequest(harness.endpoint, protocol.CredentialsManaged))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()

	request := harness.handshake(t)
	// The model is a query parameter on a shared resource: the STT and TTS
	// endpoints differ only by this value, so a dropped parameter would open a
	// TTS session and silently never transcribe.
	if got := request.URL.Path; got != "/api-ws/v1/realtime" {
		t.Fatalf("handshake path = %q", got)
	}
	if got := request.URL.Query().Get("model"); got != "qwen3-asr-flash-realtime" {
		t.Fatalf("handshake model = %q", got)
	}
	// DashScope verifies Authorization during the handshake. A managed plan
	// carries a temporary API key (st-) and a BYOK plan a permanent one (sk-);
	// both ride this one header, so there is no second channel to check.
	if got := request.Header.Get("Authorization"); got != "Bearer st-temporary-dashscope-token" {
		t.Fatalf("Authorization = %q", got)
	}
	// Sent because every Qwen-ASR-Realtime sample Alibaba ships sets it, even
	// though the reference header table omits it.
	if got := request.Header.Get("OpenAI-Beta"); got != "realtime=v1" {
		t.Fatalf("OpenAI-Beta = %q", got)
	}
	// Content inspection is opt-in and applies to customer audio; the adapter
	// must never enable it on the customer's behalf.
	if got := request.Header.Get("X-DashScope-DataInspection"); got != "" {
		t.Fatalf("X-DashScope-DataInspection was sent as %q", got)
	}

	update := harness.nextFrame(t)
	if got := update["type"]; got != "session.update" {
		t.Fatalf("first client frame type = %v, want the configuration frame", got)
	}
	// event_id is required on every client frame; DashScope only asks that it
	// be unique within the session.
	if got, _ := update["event_id"].(string); got == "" {
		t.Fatal("session.update carried no event_id")
	}
	session, ok := update["session"].(map[string]any)
	if !ok {
		t.Fatalf("session.update carried no session object: %v", update)
	}
	if got := session["input_audio_format"]; got != "pcm" {
		t.Fatalf("input_audio_format = %v", got)
	}
	if got := session["sample_rate"]; got != float64(16000) {
		t.Fatalf("sample_rate = %v", got)
	}
	transcription, ok := session["input_audio_transcription"].(map[string]any)
	if !ok || transcription["language"] != "en" {
		t.Fatalf("input_audio_transcription = %v, want the bare primary subtag", session["input_audio_transcription"])
	}
	turn, ok := session["turn_detection"].(map[string]any)
	if !ok || turn["type"] != "server_vad" {
		t.Fatalf("turn_detection = %v", session["turn_detection"])
	}
	// `modalities` appears in Alibaba's sample code but not in the documented
	// session-configuration table, and the server reports it as fixed anyway.
	// Sending undocumented fields on a handshake is how silent failures start.
	if _, present := session["modalities"]; present {
		t.Fatal("session.update sent an undocumented modalities field")
	}
	// threshold and silence_duration_ms have documented defaults and the
	// framework, not the vendor, owns turn policy.
	if _, present := turn["threshold"]; present {
		t.Fatal("turn_detection pinned a VAD threshold the adapter does not own")
	}
}

func TestSTTEmitsPartialsWhileAudioIsStillStreaming(t *testing.T) {
	t.Parallel()
	harness := newRealtimeHarness(t)
	defer harness.Close()

	adapter, err := NewSTT(STTConfig{AllowedEndpointHosts: []string{harness.host}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), sttRequest(harness.endpoint, protocol.CredentialsManaged))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	harness.handshake(t)
	harness.nextFrame(t) // session.update

	harness.send(t, map[string]any{"type": "session.created", "event_id": "event_1", "session": map[string]any{"id": "sess_001"}})
	usage := nextEvent(t, stream.Events())
	if usage.Type != protocol.EventUsageObserved {
		t.Fatalf("first event = %q, want usage.observed", usage.Type)
	}
	if got := dataString(t, usage.Data, "provider_request_id"); got != "sess_001" {
		t.Fatalf("provider_request_id = %q", got)
	}

	// First half of the utterance.
	if err := stream.WriteAudio(context.Background(), []byte{0x01, 0x02, 0x03, 0x04}); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	appended := harness.nextFrame(t)
	if appended["type"] != "input_audio_buffer.append" {
		t.Fatalf("audio frame type = %v", appended["type"])
	}
	// Realtime Scribe-style endpoints have no binary input frame here: audio
	// is base64 inside a JSON text frame under `audio`.
	decoded, err := base64.StdEncoding.DecodeString(appended["audio"].(string))
	if err != nil || string(decoded) != "\x01\x02\x03\x04" {
		t.Fatalf("audio payload = %q, err=%v", decoded, err)
	}

	harness.send(t, map[string]any{"type": "input_audio_buffer.speech_started", "audio_start_ms": 64, "item_id": "item_1"})
	if got := nextEvent(t, stream.Events()); got.Type != protocol.EventSpeechStarted {
		t.Fatalf("event = %q, want speech.started", got.Type)
	}

	// The interim frame splits one hypothesis across two fields. `text` is the
	// confirmed prefix and `stash` the draft suffix; the caller-visible
	// hypothesis is their concatenation. Reading only `text` here yields "The"
	// and drops everything the model has heard since.
	harness.send(t, map[string]any{
		"type":     "conversation.item.input_audio_transcription.text",
		"item_id":  "item_1",
		"language": "en",
		"emotion":  "neutral",
		"text":     "The ",
		"stash":    "weather is",
	})
	delta := nextEvent(t, stream.Events())
	if delta.Type != protocol.EventTranscriptDelta {
		t.Fatalf("event = %q, want transcript.delta", delta.Type)
	}
	if got := dataString(t, delta.Data, "text"); got != "The weather is" {
		t.Fatalf("interim text = %q, want text+stash concatenated", got)
	}
	if delta.Extensions[extensionID] == nil {
		t.Fatal("interim transcript dropped the raw DashScope frame")
	}

	// Second half arrives AFTER the partial: this is what makes the session
	// streaming rather than buffered.
	if err := stream.WriteAudio(context.Background(), []byte{0x05, 0x06}); err != nil {
		t.Fatalf("write second chunk: %v", err)
	}
	if got := harness.nextFrame(t)["type"]; got != "input_audio_buffer.append" {
		t.Fatalf("second audio frame type = %v", got)
	}

	harness.send(t, map[string]any{
		"type":       "conversation.item.input_audio_transcription.completed",
		"item_id":    "item_1",
		"language":   "en",
		"emotion":    "neutral",
		"transcript": "The weather is nice today.",
	})
	final := nextEvent(t, stream.Events())
	if final.Type != protocol.EventTranscriptFinal {
		t.Fatalf("event = %q, want transcript.final", final.Type)
	}
	// The final rides `transcript`, a different field from the interim's
	// text/stash pair. Reusing `text` here would emit an empty final.
	if got := dataString(t, final.Data, "text"); got != "The weather is nice today." {
		t.Fatalf("final text = %q", got)
	}

	// In VAD mode input_audio_buffer.commit is disabled, so session.finish is
	// the only flush the protocol offers and CommitAudio converges on it.
	if err := stream.CommitAudio(context.Background()); err != nil {
		t.Fatalf("commit audio: %v", err)
	}
	if got := harness.nextFrame(t)["type"]; got != "session.finish" {
		t.Fatalf("commit sent %v, want session.finish", got)
	}
	// The session is over upstream; accepting more audio would write into a
	// socket DashScope has already stopped transcribing.
	if err := stream.WriteAudio(context.Background(), []byte{0x07}); !errors.Is(err, runtimepkg.ErrSessionClosed) {
		t.Fatalf("post-commit write = %v, want ErrSessionClosed", err)
	}

	harness.send(t, map[string]any{"type": "session.finished", "event_id": "event_2239"})
	if _, open := <-stream.Events(); open {
		t.Fatal("events stayed open after session.finished")
	}
}

func TestSTTChunksLargeWritesAtTheVendorFrameSize(t *testing.T) {
	t.Parallel()
	harness := newRealtimeHarness(t)
	defer harness.Close()

	adapter, err := NewSTT(STTConfig{AllowedEndpointHosts: []string{harness.host}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), sttRequest(harness.endpoint, protocol.CredentialsBYOK))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()
	harness.handshake(t)
	harness.nextFrame(t)

	// 8000 B is more than two vendor frames. A caller handing over a whole
	// utterance in one write must not produce one oversized append.
	if err := stream.WriteAudio(context.Background(), make([]byte, 8_000)); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	for _, want := range []int{3_200, 3_200, 1_600} {
		frame := harness.nextFrame(t)
		if frame["type"] != "input_audio_buffer.append" {
			t.Fatalf("frame type = %v", frame["type"])
		}
		decoded, err := base64.StdEncoding.DecodeString(frame["audio"].(string))
		if err != nil {
			t.Fatalf("decode audio: %v", err)
		}
		if len(decoded) != want {
			t.Fatalf("append payload = %d bytes, want %d", len(decoded), want)
		}
	}
}

func TestSTTSurfacesAnErrorFrameAsATerminalProviderError(t *testing.T) {
	t.Parallel()
	harness := newRealtimeHarness(t)
	defer harness.Close()

	adapter, err := NewSTT(STTConfig{AllowedEndpointHosts: []string{harness.host}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), sttRequest(harness.endpoint, protocol.CredentialsManaged))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()
	harness.handshake(t)
	harness.nextFrame(t)

	// A revoked or expired key can only surface as a frame once the socket is
	// up, because the handshake succeeded with whatever was still valid then.
	harness.send(t, map[string]any{
		"type":     "error",
		"event_id": "event_B2uoU7VOt1AAITsPRPH9n",
		"error": map[string]any{
			"type":    "invalid_request_error",
			"code":    "InvalidApiKey",
			"message": "Invalid API-key provided.",
		},
	})
	event := nextRawEvent(t, stream.Events())
	var providerErr *runtimepkg.ProviderError
	if !errors.As(event.Err, &providerErr) {
		t.Fatalf("event = %#v, want a terminal provider error", event)
	}
	if providerErr.Code != "authentication_failed" || providerErr.Retryable {
		t.Fatalf("provider error = %+v", providerErr)
	}
	if providerErr.Extensions[extensionID] == nil {
		t.Fatal("terminal error dropped the raw DashScope frame")
	}
	if !strings.Contains(providerErr.Message, "Invalid API-key provided.") {
		t.Fatalf("message = %q, want the vendor detail preserved", providerErr.Message)
	}
	if _, open := <-stream.Events(); open {
		t.Fatal("events stayed open after a terminal error")
	}
}

func TestRealtimeErrorCodesMapToDistinctCanonicalCodes(t *testing.T) {
	t.Parallel()
	// Model Studio's documented error codes. Collapsing these would make a
	// revoked key, an empty balance, and a throttled tenant indistinguishable,
	// and only some of them are worth retrying elsewhere.
	for _, testCase := range []struct {
		vendor    string
		code      string
		retryable bool
	}{
		{"InvalidApiKey", "authentication_failed", false},
		{"invalid_api_key", "authentication_failed", false},
		{"AccessDenied", "authentication_failed", false},
		{"Model.AccessDenied", "authentication_failed", false},
		{"Throttling", "provider_rate_limited", true},
		{"Throttling.RateQuota", "provider_rate_limited", true},
		{"Throttling.BurstRate", "provider_rate_limited", true},
		{"limit_requests", "provider_rate_limited", true},
		// Documented under 400 and 429 both, but it is a rate limit that
		// happens to mention quota, so the throttling family must win.
		{"Throttling.AllocationQuota", "provider_rate_limited", true},
		{"Arrearage", "provider_quota_exceeded", false},
		{"AllocationQuota.FreeTierOnly", "provider_quota_exceeded", false},
		{"InternalError", "provider_unavailable", true},
		{"InternalError.Timeout", "provider_unavailable", true},
		{"SystemError", "provider_unavailable", true},
		{"ModelUnavailable", "provider_unavailable", true},
		{"invalid_value", "invalid_request", false},
		{"InvalidParameter", "invalid_request", false},
	} {
		code, retryable := classifyRealtimeError(testCase.vendor)
		if code != testCase.code || retryable != testCase.retryable {
			t.Errorf("%s -> (%q, %v), want (%q, %v)", testCase.vendor, code, retryable, testCase.code, testCase.retryable)
		}
	}
}

func TestSTTOpenRejectsPlansItCannotHonor(t *testing.T) {
	t.Parallel()
	harness := newRealtimeHarness(t)
	defer harness.Close()
	adapter, err := NewSTT(STTConfig{AllowedEndpointHosts: []string{harness.host}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}

	for name, mutate := range map[string]func(*runtimepkg.AdapterRequest){
		// A TTS plan reaching the STT adapter would configure an ASR session
		// and then never receive text.
		"wrong kind": func(r *runtimepkg.AdapterRequest) { r.Kind = protocol.SessionKindTTS },
		// The endpoint is shared across DashScope models but not across
		// vendors; opening someone else's plan would leak their credential.
		"wrong provider": func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Provider = "deepgram" },
		"wrong transport": func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Transport = protocol.TransportHTTP
		},
		// A session URL or signed URL means the control plane believed in a
		// delegation mechanism DashScope does not have.
		"wrong credential kind": func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Credential.Kind = protocol.CredentialSessionURL
		},
		"empty credential": func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential.Value = "  " },
		// `auto` never reaches a provider: the control plane resolves it.
		"auto model": func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "auto" },
		// A stale pin should fail locally rather than after a billed round trip.
		"unknown model": func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "qwen3-asr-turbo" },
		// The TTS resource lives at the same path with a different model, so a
		// wrong path is a real risk rather than a theoretical one.
		"wrong endpoint path": func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Endpoint = strings.Replace(r.Plan.Route.Endpoint, "/api-ws/v1/realtime", "/api-ws/v1/inference", 1)
		},
		"non pcm encoding": func(r *runtimepkg.AdapterRequest) { r.Media.Encoding = "opus" },
		// Interleaved stereo read as mono transcribes into gibberish while the
		// session still looks healthy.
		"stereo": func(r *runtimepkg.AdapterRequest) { r.Media.Channels = 2 },
		// Only 8000 and 16000 are accepted; 48000 would be read at the wrong
		// rate rather than resampled.
		"unsupported sample rate": func(r *runtimepkg.AdapterRequest) { r.Media.SampleRateHz = 48_000 },
		// Sending an unsupported code errors mid-session; omitting it would
		// auto-detect, which is not what the caller asked for.
		"unsupported language": func(r *runtimepkg.AdapterRequest) { r.Options.Language = "sw" },
	} {
		request := sttRequest(harness.endpoint, protocol.CredentialsManaged)
		mutate(&request)
		if _, err := adapter.Open(context.Background(), request); err == nil {
			t.Errorf("%s: Open accepted an unusable plan", name)
		}
	}

	// `auto` is rejected with its OWN message. The allowlist would refuse it
	// anyway, but "unsupported model" implies a stale pin, whereas an unresolved
	// `auto` means the control plane failed to pick a model at all.
	autoRequest := sttRequest(harness.endpoint, protocol.CredentialsManaged)
	autoRequest.Plan.Route.Model = "auto"
	if _, err := adapter.Open(context.Background(), autoRequest); err == nil || !strings.Contains(err.Error(), "requires a concrete model") {
		t.Fatalf("auto model error = %v, want the unresolved-route message", err)
	}

	// A regional tag has to be narrowed: the vendor's language list is bare
	// subtags and `en-US` is not in it.
	request := sttRequest(harness.endpoint, protocol.CredentialsManaged)
	request.Options.Language = "en-US"
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("regional tag rejected: %v", err)
	}
	harness.handshake(t)
	session := harness.nextFrame(t)["session"].(map[string]any)
	transcription := session["input_audio_transcription"].(map[string]any)
	if transcription["language"] != "en" {
		t.Fatalf("language = %v, want the narrowed subtag", transcription["language"])
	}
	_ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background())

	// No language means auto-detect, which is expressed by omitting the object
	// rather than by sending an empty one.
	request = sttRequest(harness.endpoint, protocol.CredentialsManaged)
	request.Options.Language = ""
	stream, err = adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open without language: %v", err)
	}
	harness.handshake(t)
	session = harness.nextFrame(t)["session"].(map[string]any)
	if _, present := session["input_audio_transcription"]; present {
		t.Fatalf("auto-detect session still pinned a language: %v", session)
	}
	_ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background())
}

func TestSTTTextOperationsAreUnsupported(t *testing.T) {
	t.Parallel()
	harness := newRealtimeHarness(t)
	defer harness.Close()
	adapter, err := NewSTT(STTConfig{AllowedEndpointHosts: []string{harness.host}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), sttRequest(harness.endpoint, protocol.CredentialsManaged))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()
	// Reported rather than silently discarded, so a misrouted TTS caller sees
	// a failure instead of a session that never produces audio.
	if err := stream.AppendText(context.Background(), "hello"); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Fatalf("AppendText = %v", err)
	}
	if err := stream.CommitText(context.Background()); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Fatalf("CommitText = %v", err)
	}
}

func TestNewSTTPinsTheInternationalHostByDefault(t *testing.T) {
	t.Parallel()
	// A refusing transport keeps this test offline. Without it a rejected host
	// and a real 401 from the live endpoint are indistinguishable, and the
	// assertion would pass even if the default host were wrong.
	adapter, err := NewSTT(STTConfig{HTTPClient: refusingHTTPClient()})
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	// An API key is region-scoped, so reaching the mainland host has to be a
	// deliberate configuration choice rather than something a plan can assert.
	for name, endpoint := range map[string]string{
		"mainland":  "wss://dashscope.aliyuncs.com/api-ws/v1/realtime",
		"workspace": "wss://ws-123.ap-southeast-1.maas.aliyuncs.com/api-ws/v1/realtime",
		"lookalike": "wss://dashscope-intl.aliyuncs.com.evil.test/api-ws/v1/realtime",
	} {
		_, err := adapter.Open(context.Background(), sttRequest(endpoint, protocol.CredentialsManaged))
		if err == nil || !strings.Contains(err.Error(), "endpoint host is not allowed") {
			t.Errorf("%s host: err = %v, want an endpoint-policy rejection", name, err)
		}
	}
	// The default itself has to be the international host, and it must be
	// reached by policy rather than by a dial that happens to fail.
	_, err = adapter.Open(context.Background(), sttRequest("wss://dashscope-intl.aliyuncs.com/api-ws/v1/realtime", protocol.CredentialsManaged))
	if err == nil || strings.Contains(err.Error(), "endpoint host is not allowed") {
		t.Fatalf("international host was rejected by policy: %v", err)
	}
	// ProviderError.Error reports only its own message, so the transport
	// failure has to be unwrapped to prove the dial was actually attempted.
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || !strings.Contains(fmt.Sprint(providerErr.Cause), refusedTransportMarker) {
		t.Fatalf("international host did not reach the dial step: %v", err)
	}
}

const refusedTransportMarker = "refused by test transport"

// refusingHTTPClient fails every request so a host-policy test never reaches
// the network. Reaching it would make a policy rejection and a live
// authentication failure look identical.
func refusingHTTPClient() *http.Client {
	return &http.Client{Transport: refusingTransport{}}
}

type refusingTransport struct{}

func (refusingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New(refusedTransportMarker)
}

// --- harness -------------------------------------------------------------

// realtimeHarness stands in for the DashScope realtime endpoint. Inbound
// frames are captured for assertion and outbound frames are scripted by the
// test, so interleaving (a partial arriving between two audio writes) can be
// asserted directly rather than encoded in a server-side callback.
type realtimeHarness struct {
	server    *httptest.Server
	host      string
	endpoint  string
	requests  chan *http.Request
	inbound   chan map[string]any
	outbound  chan any
	readError chan error
}

func newRealtimeHarness(t *testing.T) *realtimeHarness {
	t.Helper()
	harness := &realtimeHarness{
		requests:  make(chan *http.Request, 4),
		inbound:   make(chan map[string]any, 64),
		outbound:  make(chan any, 64),
		readError: make(chan error, 4),
	}
	harness.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api-ws/v1/realtime" {
			http.NotFound(w, r)
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		harness.requests <- r.Clone(context.Background())
		go func() {
			for {
				_, payload, err := conn.Read(context.Background())
				if err != nil {
					harness.readError <- err
					return
				}
				var frame map[string]any
				if err := json.Unmarshal(payload, &frame); err != nil {
					t.Errorf("client frame was not JSON: %v", err)
					return
				}
				harness.inbound <- frame
			}
		}()
		go func() {
			defer conn.CloseNow()
			for value := range harness.outbound {
				payload, err := json.Marshal(value)
				if err != nil {
					t.Errorf("marshal server frame: %v", err)
					return
				}
				if err := conn.Write(context.Background(), websocket.MessageText, payload); err != nil {
					return
				}
			}
		}()
	}))
	endpoint, _ := url.Parse(harness.server.URL)
	harness.host = endpoint.Hostname()
	endpoint.Scheme = "ws"
	endpoint.Path = "/api-ws/v1/realtime"
	harness.endpoint = endpoint.String()
	return harness
}

func (h *realtimeHarness) Close() {
	close(h.outbound)
	h.server.Close()
}

func (h *realtimeHarness) handshake(t *testing.T) *http.Request {
	t.Helper()
	select {
	case request := <-h.requests:
		return request
	case <-time.After(2 * time.Second):
		t.Fatal("server never observed a handshake")
		return nil
	}
}

func (h *realtimeHarness) nextFrame(t *testing.T) map[string]any {
	t.Helper()
	select {
	case frame := <-h.inbound:
		return frame
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a client frame")
		return nil
	}
}

func (h *realtimeHarness) send(t *testing.T, frame any) {
	t.Helper()
	select {
	case h.outbound <- frame:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out queueing a server frame")
	}
}

func nextEvent(t *testing.T, events <-chan runtimepkg.ProviderEvent) runtimepkg.ProviderEvent {
	t.Helper()
	event := nextRawEvent(t, events)
	if event.Err != nil {
		t.Fatalf("unexpected terminal error: %v", event.Err)
	}
	return event
}

func nextRawEvent(t *testing.T, events <-chan runtimepkg.ProviderEvent) runtimepkg.ProviderEvent {
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

func dataString(t *testing.T, data json.RawMessage, key string) string {
	t.Helper()
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("decode event data: %v", err)
	}
	value, _ := decoded[key].(string)
	return value
}

func sttRequest(endpoint string, source protocol.CredentialSource) runtimepkg.AdapterRequest {
	value := "st-temporary-dashscope-token"
	if source == protocol.CredentialsBYOK {
		value = "sk-customer-dashscope-key"
	}
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindSTT,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{
				Placement:        protocol.PlacementEmbedded,
				ProviderRoute:    protocol.RouteProviderDirect,
				CredentialSource: source,
			},
			Route: protocol.PlanRoute{
				Provider:  "alibaba",
				Model:     "qwen3-asr-flash-realtime",
				Adapter:   STTAdapterID,
				Transport: protocol.TransportWebSocket,
				Endpoint:  endpoint,
				Credential: &protocol.DelegatedCredential{
					Kind:      protocol.CredentialBearer,
					Value:     value,
					ExpiresAt: time.Now().Add(30 * time.Minute),
				},
			},
		},
		Options: protocol.RequestOptions{Language: "en"},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
	}
}
