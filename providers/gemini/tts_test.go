package gemini

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

func ttsAdapterRequest(endpoint, model, voice string) runtimepkg.AdapterRequest {
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindTTS,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK},
			Route: protocol.PlanRoute{
				Provider: "gemini", Model: model, Voice: voice, Adapter: TTSAdapterID,
				Transport: protocol.TransportHTTP, Endpoint: endpoint,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "aistudio-key"},
			},
		},
		Media: &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1},
	}
}

func ttsEventWithin(t *testing.T, events <-chan runtimepkg.ProviderEvent) runtimepkg.ProviderEvent {
	t.Helper()
	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("events closed")
		}
		if event.Err != nil {
			t.Fatalf("provider event error: %v", event.Err)
		}
		return event
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
		return runtimepkg.ProviderEvent{}
	}
}

func ttsErrorWithin(t *testing.T, events <-chan runtimepkg.ProviderEvent) *runtimepkg.ProviderError {
	t.Helper()
	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("events closed before an error event")
		}
		if event.Err == nil {
			t.Fatalf("event = %q, want an error event", event.Type)
		}
		var providerErr *runtimepkg.ProviderError
		if !errors.As(event.Err, &providerErr) {
			t.Fatalf("error = %v, want a ProviderError", event.Err)
		}
		return providerErr
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for an error event")
		return nil
	}
}

func sseFrame(t *testing.T, audio []byte, finishReason string) string {
	t.Helper()
	part := map[string]any{}
	if audio != nil {
		part["inlineData"] = map[string]any{"mimeType": "audio/L16;rate=24000", "data": base64.StdEncoding.EncodeToString(audio)}
	}
	candidate := map[string]any{"content": map[string]any{"parts": []any{part}}}
	if finishReason != "" {
		candidate["finishReason"] = finishReason
	}
	payload, err := json.Marshal(map[string]any{"candidates": []any{candidate}})
	if err != nil {
		t.Fatal(err)
	}
	return "data: " + string(payload) + "\n\n"
}

func newTTSFixture(t *testing.T, handler http.HandlerFunc) (runtimepkg.ProviderStream, func()) {
	t.Helper()
	server := httptest.NewServer(handler)
	parsed, _ := url.Parse(server.URL)
	adapter, err := NewTTS(TTSConfig{AllowedEndpointHosts: []string{parsed.Hostname()}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := adapter.Open(context.Background(), ttsAdapterRequest(server.URL+"/v1beta/models", TTSModel, ""))
	if err != nil {
		server.Close()
		t.Fatalf("open: %v", err)
	}
	return stream, func() {
		_ = stream.Close(context.Background())
		server.Close()
	}
}

func TestCommitTextStreamsSSEAudio(t *testing.T) {
	requestSeen := make(chan *http.Request, 1)
	bodySeen := make(chan map[string]any, 1)
	stream, cleanup := newTTSFixture(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var decoded map[string]any
		_ = json.Unmarshal(body, &decoded)
		bodySeen <- decoded
		requestSeen <- r
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, sseFrame(t, []byte{1, 2}, ""))
		_, _ = io.WriteString(w, sseFrame(t, []byte{3, 4}, finishReasonStop))
	})
	defer cleanup()

	if err := stream.AppendText(context.Background(), "hello "); err != nil {
		t.Fatal(err)
	}
	if err := stream.AppendText(context.Background(), "world"); err != nil {
		t.Fatal(err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}

	request := <-requestSeen
	wantPath := "/v1beta/models/" + TTSModel + streamGenerateContentSuffix
	if request.Method != http.MethodPost || request.URL.Path != wantPath || request.URL.RawQuery != "alt=sse" {
		t.Fatalf("request = %s %s?%s, want POST %s?alt=sse", request.Method, request.URL.Path, request.URL.RawQuery, wantPath)
	}
	if request.Header.Get(APIKeyHeader) != "aistudio-key" || request.Header.Get("Authorization") != "" {
		t.Fatalf("credential headers = %v, want the AI Studio key header only", request.Header)
	}
	body := <-bodySeen
	payload, _ := json.Marshal(body)
	if !strings.Contains(string(payload), `"text":"hello world"`) ||
		!strings.Contains(string(payload), `"voiceName":"Aoede"`) ||
		!strings.Contains(string(payload), `"responseModalities":["AUDIO"]`) {
		t.Fatalf("body = %s", payload)
	}

	if event := ttsEventWithin(t, stream.Events()); event.Type != protocol.EventAudioStarted {
		t.Fatalf("event = %q, want audio started", event.Type)
	}
	first := ttsEventWithin(t, stream.Events())
	second := ttsEventWithin(t, stream.Events())
	if string(first.Audio) != string([]byte{1, 2}) || string(second.Audio) != string([]byte{3, 4}) {
		t.Fatalf("audio frames = %v, %v", first.Audio, second.Audio)
	}
	done := ttsEventWithin(t, stream.Events())
	if done.Type != protocol.EventAudioDone || done.Data != nil {
		t.Fatalf("done = %q data %s, want a bare AudioDone for a STOP finish", done.Type, done.Data)
	}
}

func TestLongTextTakesTheNonStreamingArm(t *testing.T) {
	requestSeen := make(chan *http.Request, 1)
	audio := []byte{9, 8, 7, 6}
	stream, cleanup := newTTSFixture(t, func(w http.ResponseWriter, r *http.Request) {
		requestSeen <- r
		w.Header().Set("Content-Type", "application/json")
		payload, _ := json.Marshal(map[string]any{"candidates": []any{map[string]any{
			"content":      map[string]any{"parts": []any{map[string]any{"inlineData": map[string]any{"data": base64.StdEncoding.EncodeToString(audio)}}}},
			"finishReason": finishReasonStop,
		}}})
		_, _ = w.Write(payload)
	})
	defer cleanup()

	long := strings.Repeat("a long sentence. ", ttsStreamMaxChars/16+1)
	if err := stream.AppendText(context.Background(), long); err != nil {
		t.Fatal(err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	request := <-requestSeen
	wantPath := "/v1beta/models/" + TTSModel + generateContentSuffix
	if request.URL.Path != wantPath || request.URL.RawQuery != "" {
		t.Fatalf("request = %s?%s, want the non-streaming arm %s", request.URL.Path, request.URL.RawQuery, wantPath)
	}
	wantTypes := []protocol.EventType{protocol.EventAudioStarted, protocol.EventAudioFrame, protocol.EventAudioDone}
	for _, want := range wantTypes {
		event := ttsEventWithin(t, stream.Events())
		if event.Type != want {
			t.Fatalf("event = %q, want %q", event.Type, want)
		}
		if want == protocol.EventAudioFrame && string(event.Audio) != string(audio) {
			t.Fatalf("audio = %v", event.Audio)
		}
	}
}

func TestTruncatedStreamCompletesAndRecordsTheFinishReason(t *testing.T) {
	stream, cleanup := newTTSFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, sseFrame(t, []byte{5, 5}, "OTHER"))
	})
	defer cleanup()
	if err := stream.AppendText(context.Background(), "hello"); err != nil {
		t.Fatal(err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}
	if event := ttsEventWithin(t, stream.Events()); event.Type != protocol.EventAudioStarted {
		t.Fatalf("event = %q", event.Type)
	}
	if event := ttsEventWithin(t, stream.Events()); event.Type != protocol.EventAudioFrame {
		t.Fatalf("event = %q", event.Type)
	}
	done := ttsEventWithin(t, stream.Events())
	if done.Type != protocol.EventAudioDone || !strings.Contains(string(done.Data), `"finish_reason":"OTHER"`) {
		t.Fatalf("done = %q data %s, want the truncation recorded", done.Type, done.Data)
	}
}

func TestStreamWithNoAudioIsAnErrorEvent(t *testing.T) {
	stream, cleanup := newTTSFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, sseFrame(t, nil, finishReasonStop))
	})
	defer cleanup()
	if err := stream.AppendText(context.Background(), "hello"); err != nil {
		t.Fatal(err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}
	if providerErr := ttsErrorWithin(t, stream.Events()); providerErr.Code != "provider_unavailable" || !providerErr.Retryable {
		t.Fatalf("error = %+v, want retryable provider_unavailable", providerErr)
	}
}

func TestInBandErrorPayloadMapsToTheCanonicalVocabulary(t *testing.T) {
	stream, cleanup := newTTSFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"error":{"code":429,"message":"quota exhausted","status":"RESOURCE_EXHAUSTED"}}`+"\n\n")
	})
	defer cleanup()
	if err := stream.AppendText(context.Background(), "hello"); err != nil {
		t.Fatal(err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}
	providerErr := ttsErrorWithin(t, stream.Events())
	if providerErr.Code != "provider_rate_limited" || !providerErr.Retryable || !strings.Contains(providerErr.Message, "quota exhausted") {
		t.Fatalf("error = %+v, want provider_rate_limited carrying the vendor message", providerErr)
	}
}

func TestHTTPStatusesMapToTheCanonicalVocabulary(t *testing.T) {
	for status, want := range map[int]string{
		http.StatusUnauthorized:        "provider_authentication_failed",
		http.StatusTooManyRequests:     "provider_rate_limited",
		http.StatusInternalServerError: "provider_unavailable",
		http.StatusBadRequest:          "provider_rejected_request",
	} {
		stream, cleanup := newTTSFixture(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = fmt.Fprintf(w, `{"error":{"code":%d}}`, status)
		})
		if err := stream.AppendText(context.Background(), "hello"); err != nil {
			t.Fatal(err)
		}
		err := stream.CommitText(context.Background())
		var providerErr *runtimepkg.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != want || providerErr.ProviderStatus != status {
			t.Fatalf("status %d: error = %v, want %s", status, err, want)
		}
		cleanup()
	}
}

func TestOpenValidatesPlanAndMedia(t *testing.T) {
	adapter, err := NewTTS(TTSConfig{})
	if err != nil {
		t.Fatal(err)
	}
	valid := func() runtimepkg.AdapterRequest { return ttsAdapterRequest(TTSEndpoint, TTSModel, "") }

	if stream, err := adapter.Open(context.Background(), valid()); err != nil {
		t.Fatalf("valid request refused: %v", err)
	} else {
		_ = stream.Close(context.Background())
	}

	wrongKind := valid()
	wrongKind.Kind = protocol.SessionKindSTT
	wrongProvider := valid()
	wrongProvider.Plan.Route.Provider = "google"
	wrongTransport := valid()
	wrongTransport.Plan.Route.Transport = protocol.TransportWebSocket
	wrongModel := valid()
	wrongModel.Plan.Route.Model = "gemini-3.5-transcribe-live"
	wrongRate := valid()
	wrongRate.Media = &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1}
	noCredential := valid()
	noCredential.Plan.Route.Credential = nil
	wrongPath := valid()
	wrongPath.Plan.Route.Endpoint = "https://generativelanguage.googleapis.com/v1beta/interactions"
	wrongHost := valid()
	wrongHost.Plan.Route.Endpoint = "https://example.com/v1beta/models"

	for name, request := range map[string]runtimepkg.AdapterRequest{
		"kind": wrongKind, "provider": wrongProvider, "transport": wrongTransport, "model": wrongModel,
		"rate": wrongRate, "credential": noCredential, "path": wrongPath, "host": wrongHost,
	} {
		if _, err := adapter.Open(context.Background(), request); err == nil {
			t.Errorf("invalid %s request was accepted", name)
		}
	}
}

func TestVoicePrecedenceIsOptionsThenRouteThenDefault(t *testing.T) {
	voices := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var decoded struct {
			GenerationConfig struct {
				SpeechConfig struct {
					VoiceConfig struct {
						PrebuiltVoiceConfig struct {
							VoiceName string `json:"voiceName"`
						} `json:"prebuiltVoiceConfig"`
					} `json:"voiceConfig"`
				} `json:"speechConfig"`
			} `json:"generationConfig"`
		}
		_ = json.Unmarshal(body, &decoded)
		voices <- decoded.GenerationConfig.SpeechConfig.VoiceConfig.PrebuiltVoiceConfig.VoiceName
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, sseFrame(t, []byte{1}, finishReasonStop))
	}))
	defer server.Close()
	parsed, _ := url.Parse(server.URL)
	adapter, err := NewTTS(TTSConfig{AllowedEndpointHosts: []string{parsed.Hostname()}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatal(err)
	}

	for _, testCase := range []struct {
		optionsVoice string
		routeVoice   string
		want         string
	}{
		{optionsVoice: "Puck", routeVoice: "Kore", want: "Puck"},
		{optionsVoice: "", routeVoice: "Kore", want: "Kore"},
		{optionsVoice: "", routeVoice: "", want: TTSDefaultVoice},
	} {
		request := ttsAdapterRequest(server.URL+"/v1beta/models", TTSModel, testCase.routeVoice)
		request.Options.Voice = testCase.optionsVoice
		stream, err := adapter.Open(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.AppendText(context.Background(), "hi"); err != nil {
			t.Fatal(err)
		}
		if err := stream.CommitText(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := <-voices; got != testCase.want {
			t.Fatalf("voice = %q, want %q", got, testCase.want)
		}
		_ = stream.Close(context.Background())
	}
}

func TestCancelInterruptsAnInFlightUtterance(t *testing.T) {
	release := make(chan struct{})
	stream, cleanup := newTTSFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, sseFrame(t, []byte{1, 2}, ""))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	defer cleanup()
	defer close(release)
	if err := stream.AppendText(context.Background(), "hello"); err != nil {
		t.Fatal(err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}
	if event := ttsEventWithin(t, stream.Events()); event.Type != protocol.EventAudioStarted {
		t.Fatalf("event = %q", event.Type)
	}
	if event := ttsEventWithin(t, stream.Events()); event.Type != protocol.EventAudioFrame {
		t.Fatalf("event = %q", event.Type)
	}
	cancelCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := stream.Cancel(cancelCtx); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	// A canceled utterance emits no error and no AudioDone; the next
	// utterance is accepted immediately.
	if err := stream.AppendText(context.Background(), "again"); err != nil {
		t.Fatalf("append after cancel: %v", err)
	}
}

func TestInputCapIsEnforcedBeforeTheRequest(t *testing.T) {
	stream, cleanup := newTTSFixture(t, func(http.ResponseWriter, *http.Request) {
		t.Error("no request should be sent")
	})
	defer cleanup()
	err := stream.AppendText(context.Background(), strings.Repeat("a", ttsMaxInputCharacters+1))
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "input_too_large" {
		t.Fatalf("error = %v, want input_too_large", err)
	}
}
