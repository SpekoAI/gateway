package alibaba

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

// As in stt_test.go, every vendor string here is a literal. Comparing an
// assertion against the adapter's own constant would keep a misspelled wire
// string green, which is the failure mode these tests are written to catch.

func TestTTSHandshakeAndSessionUpdateMatchTheDocumentedWireShape(t *testing.T) {
	t.Parallel()
	harness := newRealtimeHarness(t)
	defer harness.Close()

	adapter, err := NewTTS(TTSConfig{AllowedEndpointHosts: []string{harness.host}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("new TTS adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), ttsRequest(harness.endpoint, protocol.CredentialsManaged))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()

	request := harness.handshake(t)
	// STT and TTS share this resource and are told apart only by the model
	// parameter, so losing it opens the wrong kind of session.
	if got := request.URL.Path; got != "/api-ws/v1/realtime" {
		t.Fatalf("handshake path = %q", got)
	}
	if got := request.URL.Query().Get("model"); got != "qwen3-tts-flash-realtime" {
		t.Fatalf("handshake model = %q", got)
	}
	if got := request.Header.Get("Authorization"); got != "Bearer st-temporary-dashscope-token" {
		t.Fatalf("Authorization = %q", got)
	}
	// Deliberately absent, unlike the STT twin. No Qwen-TTS-Realtime sample or
	// reference header table mentions it, and copying it across from the ASR
	// adapter would be an invented wire detail.
	if got := request.Header.Get("OpenAI-Beta"); got != "" {
		t.Fatalf("OpenAI-Beta was sent as %q on the TTS endpoint", got)
	}

	update := harness.nextFrame(t)
	if got := update["type"]; got != "session.update" {
		t.Fatalf("first client frame type = %v", got)
	}
	if got, _ := update["event_id"].(string); got == "" {
		t.Fatal("session.update carried no event_id")
	}
	session, ok := update["session"].(map[string]any)
	if !ok {
		t.Fatalf("session.update carried no session object: %v", update)
	}
	// `voice` is required with no server default.
	if got := session["voice"]; got != "Cherry" {
		t.Fatalf("voice = %v", got)
	}
	// server_commit lets audio start during append instead of waiting for the
	// utterance to close, and still honours an explicit commit.
	if got := session["mode"]; got != "server_commit" {
		t.Fatalf("mode = %v", got)
	}
	// language_type takes English language NAMES, not BCP-47 codes. Sending
	// "en" here is silently wrong: it is not an accepted value.
	if got := session["language_type"]; got != "English" {
		t.Fatalf("language_type = %v, want the vendor's language name", got)
	}
	if got := session["response_format"]; got != "pcm" {
		t.Fatalf("response_format = %v", got)
	}
	if got := session["sample_rate"]; got != float64(24000) {
		t.Fatalf("sample_rate = %v", got)
	}
}

// A relay plan is managed for billing purposes but carries the relay
// connector's permanent DashScope key, which rides the same
// `Authorization: Bearer` header as every other API key on this endpoint.
func TestTTSRelayRouteUsesTheSameBearerHeader(t *testing.T) {
	t.Parallel()
	harness := newRealtimeHarness(t)
	defer harness.Close()
	adapter, err := NewTTS(TTSConfig{AllowedEndpointHosts: []string{harness.host}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("new TTS adapter: %v", err)
	}
	request := ttsRequest(harness.endpoint, protocol.CredentialsManaged)
	request.Plan.Execution.ProviderRoute = protocol.RouteSpekoRelay
	request.Plan.Route.Credential.Value = "sk-relay-connector-key"
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open relay stream: %v", err)
	}
	defer func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()
	received := harness.handshake(t)
	if got := received.Header.Get("Authorization"); got != "Bearer sk-relay-connector-key" {
		t.Fatalf("Authorization = %q", got)
	}
	if got := received.URL.Query().Get("model"); got != "qwen3-tts-flash-realtime" {
		t.Fatalf("handshake model = %q", got)
	}
}

// protocol.SessionPlan validation requires a relay plan to label its
// credential relay_access, while the relay connector that synthesizes the
// plan and drives this adapter directly labels the same permanent key bearer.
// The relay arm must accept both spellings, or one of the two constructions
// becomes quietly unreachable.
func TestTTSRelayRouteAcceptsRelayAccessCredentialKind(t *testing.T) {
	t.Parallel()
	harness := newRealtimeHarness(t)
	defer harness.Close()
	adapter, err := NewTTS(TTSConfig{AllowedEndpointHosts: []string{harness.host}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("new TTS adapter: %v", err)
	}
	request := ttsRequest(harness.endpoint, protocol.CredentialsManaged)
	request.Plan.Execution.ProviderRoute = protocol.RouteSpekoRelay
	request.Plan.Route.Credential.Kind = protocol.CredentialRelayAccess
	request.Plan.Route.Credential.Value = "sk-relay-connector-key"
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open relay stream with relay_access credential: %v", err)
	}
	_ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background())
}

func TestTTSStreamsSeveralAudioFramesBeforeDone(t *testing.T) {
	t.Parallel()
	harness := newRealtimeHarness(t)
	defer harness.Close()

	adapter, err := NewTTS(TTSConfig{AllowedEndpointHosts: []string{harness.host}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("new TTS adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), ttsRequest(harness.endpoint, protocol.CredentialsBYOK))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	harness.handshake(t)
	harness.nextFrame(t) // session.update

	harness.send(t, map[string]any{"type": "session.created", "event_id": "event_x", "session": map[string]any{"id": "sess_tts_1"}})
	if got := nextEvent(t, stream.Events()); got.Type != protocol.EventUsageObserved {
		t.Fatalf("first event = %q, want usage.observed", got.Type)
	}

	if err := stream.AppendText(context.Background(), "Hello, I am Qwen."); err != nil {
		t.Fatalf("append text: %v", err)
	}
	appended := harness.nextFrame(t)
	if appended["type"] != "input_text_buffer.append" {
		t.Fatalf("append frame type = %v", appended["type"])
	}
	// The text rides a top-level `text` field, not a nested input object the
	// way the /api-ws/v1/inference protocol does it.
	if appended["text"] != "Hello, I am Qwen." {
		t.Fatalf("append text = %v", appended["text"])
	}

	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit text: %v", err)
	}
	if got := harness.nextFrame(t)["type"]; got != "input_text_buffer.commit" {
		t.Fatalf("commit frame type = %v", got)
	}

	harness.send(t, map[string]any{"type": "response.created", "response": map[string]any{"id": "resp_1"}})
	// Three deltas with distinct payloads: audio has to reach the caller
	// incrementally, not as one buffered clip at the end.
	chunks := [][]byte{{0x11, 0x22}, {0x33, 0x44}, {0x55, 0x66}}
	for _, chunk := range chunks {
		harness.send(t, map[string]any{
			"type":        "response.audio.delta",
			"response_id": "resp_1",
			"item_id":     "item_1",
			"delta":       base64.StdEncoding.EncodeToString(chunk),
		})
	}
	started := nextEvent(t, stream.Events())
	if started.Type != protocol.EventAudioStarted {
		t.Fatalf("event = %q, want audio.started", started.Type)
	}
	for index, want := range chunks {
		frame := nextEvent(t, stream.Events())
		if frame.Type != protocol.EventAudioFrame {
			t.Fatalf("event %d = %q, want audio.frame", index, frame.Type)
		}
		if string(frame.Audio) != string(want) {
			t.Fatalf("audio %d = %x, want %x", index, frame.Audio, want)
		}
	}

	harness.send(t, map[string]any{"type": "response.audio.done", "response_id": "resp_1"})
	if got := nextEvent(t, stream.Events()); got.Type != protocol.EventAudioDone {
		t.Fatalf("event = %q, want audio.done", got.Type)
	}

	harness.send(t, map[string]any{
		"type":     "response.done",
		"response": map[string]any{"id": "resp_1", "usage": map[string]any{"characters": 17}},
	})
	usage := nextEvent(t, stream.Events())
	if usage.Type != protocol.EventUsageObserved {
		t.Fatalf("event = %q, want usage.observed", usage.Type)
	}
	// characters is the only billing signal this endpoint reports.
	var decoded map[string]any
	if err := json.Unmarshal(usage.Data, &decoded); err != nil {
		t.Fatalf("decode usage: %v", err)
	}
	if decoded["characters"] != float64(17) {
		t.Fatalf("characters = %v", decoded["characters"])
	}

	// Close must send session.finish and then wait: DashScope flushes the tail
	// of the audio between session.finish and session.finished.
	closed := make(chan error, 1)
	go func() { closed <- stream.Close(context.Background()) }()
	if got := harness.nextFrame(t)["type"]; got != "session.finish" {
		t.Fatalf("close sent %v, want session.finish", got)
	}
	select {
	case err := <-closed:
		t.Fatalf("Close returned %v before session.finished arrived", err)
	case <-time.After(50 * time.Millisecond):
	}
	harness.send(t, map[string]any{"type": "session.finished", "event_id": "event_2239"})
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close never returned after session.finished")
	}
	if _, open := <-stream.Events(); open {
		t.Fatal("events stayed open after session.finished")
	}
}

func TestTTSCancelClearsTheBufferAndWithholdsCancelledAudio(t *testing.T) {
	t.Parallel()
	harness := newRealtimeHarness(t)
	defer harness.Close()

	adapter, err := NewTTS(TTSConfig{AllowedEndpointHosts: []string{harness.host}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("new TTS adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), ttsRequest(harness.endpoint, protocol.CredentialsManaged))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()
	harness.handshake(t)
	harness.nextFrame(t)

	if err := stream.AppendText(context.Background(), "first utterance"); err != nil {
		t.Fatalf("append: %v", err)
	}
	harness.nextFrame(t)
	harness.send(t, map[string]any{"type": "response.created", "response": map[string]any{"id": "resp_1"}})
	harness.send(t, map[string]any{"type": "response.audio.delta", "response_id": "resp_1", "delta": base64.StdEncoding.EncodeToString([]byte{0x01})})
	if got := nextEvent(t, stream.Events()); got.Type != protocol.EventAudioStarted {
		t.Fatalf("event = %q, want audio.started", got.Type)
	}
	if got := nextEvent(t, stream.Events()); got.Type != protocol.EventAudioFrame {
		t.Fatalf("event = %q, want audio.frame", got.Type)
	}

	// Barge-in. input_text_buffer.clear is the only interrupt the protocol
	// defines; it discards unsynthesized text but cannot recall audio the
	// server already produced, so the rest is dropped locally.
	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if got := harness.nextFrame(t)["type"]; got != "input_text_buffer.clear" {
		t.Fatalf("cancel sent %v, want input_text_buffer.clear", got)
	}

	harness.send(t, map[string]any{"type": "response.audio.delta", "response_id": "resp_1", "delta": base64.StdEncoding.EncodeToString([]byte{0x02})})
	// audio.done for a cancelled utterance would tell the caller the barge-in
	// played all the way through.
	harness.send(t, map[string]any{"type": "response.audio.done", "response_id": "resp_1"})
	harness.send(t, map[string]any{"type": "response.done", "response": map[string]any{"id": "resp_1"}})

	// The next observable event must be the response.done usage, proving the
	// post-cancel delta and audio.done were both withheld.
	usage := nextEvent(t, stream.Events())
	if usage.Type != protocol.EventUsageObserved {
		t.Fatalf("event after cancel = %q, want usage.observed; cancelled audio leaked", usage.Type)
	}

	// A new utterance is unaffected by the earlier barge-in.
	if err := stream.AppendText(context.Background(), "second utterance"); err != nil {
		t.Fatalf("append after cancel: %v", err)
	}
	harness.nextFrame(t)
	harness.send(t, map[string]any{"type": "response.created", "response": map[string]any{"id": "resp_2"}})
	harness.send(t, map[string]any{"type": "response.audio.delta", "response_id": "resp_2", "delta": base64.StdEncoding.EncodeToString([]byte{0x09})})
	if got := nextEvent(t, stream.Events()); got.Type != protocol.EventAudioStarted {
		t.Fatalf("event = %q, want audio.started for the new utterance", got.Type)
	}
	frame := nextEvent(t, stream.Events())
	if frame.Type != protocol.EventAudioFrame || string(frame.Audio) != "\x09" {
		t.Fatalf("post-cancel utterance audio = (%q, %x)", frame.Type, frame.Audio)
	}
}

func TestTTSSurfacesAnErrorFrameAsATerminalProviderError(t *testing.T) {
	t.Parallel()
	harness := newRealtimeHarness(t)
	defer harness.Close()

	adapter, err := NewTTS(TTSConfig{AllowedEndpointHosts: []string{harness.host}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("new TTS adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), ttsRequest(harness.endpoint, protocol.CredentialsManaged))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()
	harness.handshake(t)
	harness.nextFrame(t)

	// A rejected voice or a session.update after synthesis has begun arrives
	// as this frame; it is not a transport failure and must not be retried.
	harness.send(t, map[string]any{
		"type":     "error",
		"event_id": "event_QzAVZRVa9hKqM5VOaHunh",
		"error": map[string]any{
			"code":    "invalid_value",
			"message": "Session update error: session already started or finished or failed.",
		},
	})
	event := nextRawEvent(t, stream.Events())
	var providerErr *runtimepkg.ProviderError
	if !errors.As(event.Err, &providerErr) {
		t.Fatalf("event = %#v, want a terminal provider error", event)
	}
	if providerErr.Code != "invalid_request" || providerErr.Retryable {
		t.Fatalf("provider error = %+v", providerErr)
	}
	if providerErr.Extensions[extensionID] == nil {
		t.Fatal("terminal error dropped the raw DashScope frame")
	}
	if !strings.Contains(providerErr.Message, "session already started") {
		t.Fatalf("message = %q, want the vendor detail preserved", providerErr.Message)
	}
}

func TestTTSOpenRejectsPlansItCannotHonor(t *testing.T) {
	t.Parallel()
	harness := newRealtimeHarness(t)
	defer harness.Close()
	adapter, err := NewTTS(TTSConfig{AllowedEndpointHosts: []string{harness.host}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("new TTS adapter: %v", err)
	}

	for name, mutate := range map[string]func(*runtimepkg.AdapterRequest){
		"wrong kind":      func(r *runtimepkg.AdapterRequest) { r.Kind = protocol.SessionKindSTT },
		"wrong provider":  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Provider = "minimax" },
		"wrong transport": func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Transport = protocol.TransportHTTP },
		// DashScope mints temporary API keys, not session URLs; a session_url
		// credential means the control plane invented a delegation mechanism.
		"wrong credential kind": func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Credential.Kind = protocol.CredentialSignedURL
		},
		// relay_access is accepted only on the relay route; on provider-direct
		// it means the control plane mislabeled the plan.
		"relay_access kind off the relay route": func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Credential.Kind = protocol.CredentialRelayAccess
		},
		"empty credential": func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential.Value = "" },
		"auto model":       func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "auto" },
		"unknown model":    func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "qwen3-tts-turbo-realtime" },
		// `voice` has no server-side default, so an unset voice is a 400 after
		// the socket is already up.
		"missing voice":    func(r *runtimepkg.AdapterRequest) { r.Options.Voice = "  " },
		"non pcm encoding": func(r *runtimepkg.AdapterRequest) { r.Media.Encoding = "mp3" },
		"stereo":           func(r *runtimepkg.AdapterRequest) { r.Media.Channels = 2 },
		"unsupported sample rate": func(r *runtimepkg.AdapterRequest) {
			r.Media.SampleRateHz = 22_050
		},
		// The older qwen-tts-realtime series is documented as 24 kHz only; a
		// mismatch there plays back at the wrong pitch.
		"legacy series off its only rate": func(r *runtimepkg.AdapterRequest) {
			r.Plan.Route.Model = "qwen-tts-realtime"
			r.Media.SampleRateHz = 16_000
		},
	} {
		request := ttsRequest(harness.endpoint, protocol.CredentialsManaged)
		mutate(&request)
		if _, err := adapter.Open(context.Background(), request); err == nil {
			t.Errorf("%s: Open accepted an unusable plan", name)
		}
	}

	// `auto` gets its own message for the same reason as on the STT side: the
	// allowlist would refuse it regardless, but an unresolved route and a stale
	// pin are different operational problems.
	autoRequest := ttsRequest(harness.endpoint, protocol.CredentialsManaged)
	autoRequest.Plan.Route.Model = "auto"
	if _, err := adapter.Open(context.Background(), autoRequest); err == nil || !strings.Contains(err.Error(), "requires a concrete model") {
		t.Fatalf("auto model error = %v, want the unresolved-route message", err)
	}
}

func TestTTSAudioOperationsAreUnsupported(t *testing.T) {
	t.Parallel()
	harness := newRealtimeHarness(t)
	defer harness.Close()
	adapter, err := NewTTS(TTSConfig{AllowedEndpointHosts: []string{harness.host}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("new TTS adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), ttsRequest(harness.endpoint, protocol.CredentialsManaged))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()
	// Reported rather than silently discarded, so a misrouted STT caller sees
	// a failure instead of a session that never transcribes.
	if err := stream.WriteAudio(context.Background(), []byte{1}); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Fatalf("WriteAudio = %v", err)
	}
	if err := stream.CommitAudio(context.Background()); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Fatalf("CommitAudio = %v", err)
	}
}

func TestTTSLanguageTypeUsesVendorLanguageNames(t *testing.T) {
	t.Parallel()
	// The accepted vocabulary is English language names. Anything outside it
	// falls back to the documented default rather than reaching the wire as a
	// code the endpoint rejects.
	for tag, want := range map[string]string{
		"zh":    "Chinese",
		"en":    "English",
		"en-GB": "English",
		"es":    "Spanish",
		"ja":    "Japanese",
		"ru":    "Russian",
		// Not in the language_type list even though Qwen-ASR supports it.
		"ar": "Auto",
		"":   "Auto",
	} {
		if got := ttsLanguageType(tag); got != want {
			t.Errorf("ttsLanguageType(%q) = %q, want %q", tag, got, want)
		}
	}
}

func TestNewTTSPinsTheInternationalHostByDefault(t *testing.T) {
	t.Parallel()
	adapter, err := NewTTS(TTSConfig{HTTPClient: refusingHTTPClient()})
	if err != nil {
		t.Fatalf("new TTS adapter: %v", err)
	}
	// The English TTS reference prints the international host under its
	// "China (Beijing)" heading; the Chinese edition correctly prints
	// dashscope.aliyuncs.com. Reaching the mainland host must be a deliberate
	// configuration choice either way, because API keys are region-scoped.
	// Workspace-scoped hosts are per tenant and can only arrive as config too.
	for name, endpoint := range map[string]string{
		"mainland":  "wss://dashscope.aliyuncs.com/api-ws/v1/realtime",
		"workspace": "wss://ws-123.ap-southeast-1.maas.aliyuncs.com/api-ws/v1/realtime",
	} {
		_, err := adapter.Open(context.Background(), ttsRequest(endpoint, protocol.CredentialsManaged))
		if err == nil || !strings.Contains(err.Error(), "endpoint host is not allowed") {
			t.Errorf("%s host: err = %v, want an endpoint-policy rejection", name, err)
		}
	}
	_, err = adapter.Open(context.Background(), ttsRequest("wss://dashscope-intl.aliyuncs.com/api-ws/v1/realtime", protocol.CredentialsManaged))
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

func ttsRequest(endpoint string, source protocol.CredentialSource) runtimepkg.AdapterRequest {
	value := "st-temporary-dashscope-token"
	if source == protocol.CredentialsBYOK {
		value = "sk-customer-dashscope-key"
	}
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindTTS,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{
				Placement:        protocol.PlacementEmbedded,
				ProviderRoute:    protocol.RouteProviderDirect,
				CredentialSource: source,
			},
			Route: protocol.PlanRoute{
				Provider:  "alibaba",
				Model:     "qwen3-tts-flash-realtime",
				Adapter:   TTSAdapterID,
				Transport: protocol.TransportWebSocket,
				Endpoint:  endpoint,
				Credential: &protocol.DelegatedCredential{
					Kind:      protocol.CredentialBearer,
					Value:     value,
					ExpiresAt: time.Now().Add(30 * time.Minute),
				},
			},
		},
		Options: protocol.RequestOptions{Language: "en", Voice: "Cherry"},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1},
	}
}
