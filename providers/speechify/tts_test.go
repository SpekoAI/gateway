package speechify

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

func TestCommitTextSendsSimbaRequestAndStreamsPCM(t *testing.T) {
	requestSeen := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != streamPath {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer speechify-key" || r.Header.Get("Accept") != "audio/pcm" || r.Header.Get("Speechify-Version") != apiVersion {
			t.Errorf("headers = %v", r.Header)
		}
		body, _ := io.ReadAll(r.Body)
		var decoded map[string]any
		if err := json.Unmarshal(body, &decoded); err != nil {
			t.Errorf("decode body: %v", err)
		}
		requestSeen <- decoded
		w.Header().Set("Speechify-Request-Id", "speechify-request-1")
		w.Header().Set("Content-Type", "audio/L16;rate=24000;channels=1")
		_, _ = w.Write([]byte{0, 1, 2, 3})
	}))
	defer server.Close()

	parsed, _ := url.Parse(server.URL)
	adapter, err := New(Config{AllowedEndpointHosts: []string{parsed.Hostname()}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := adapter.Open(context.Background(), adapterRequest(server.URL+streamPath, "simba-3.2"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = stream.Close(context.Background()) }()
	if err := stream.AppendText(context.Background(), "hello "); err != nil {
		t.Fatal(err)
	}
	if err := stream.AppendText(context.Background(), "world"); err != nil {
		t.Fatal(err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	body := <-requestSeen
	if body["input"] != "hello world" || body["voice_id"] != "geffen_32" || body["model"] != "simba-3.2" || body["language"] != "en-US" {
		t.Fatalf("body = %#v", body)
	}

	wantTypes := []protocol.EventType{protocol.EventUsageObserved, protocol.EventAudioStarted, protocol.EventAudioFrame, protocol.EventAudioDone}
	for _, want := range wantTypes {
		event := speechifyEventWithin(t, stream.Events())
		if event.Type != want {
			t.Fatalf("event = %q, want %q", event.Type, want)
		}
		if want == protocol.EventAudioFrame && string(event.Audio) != string([]byte{0, 1, 2, 3}) {
			t.Fatalf("audio = %v", event.Audio)
		}
	}
}

func TestAdapterAcceptsCompletePublishedSimbaFamily(t *testing.T) {
	adapter, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, model := range []string{"simba-3.2", "simba-3.0", "simba-multilingual", "simba-english"} {
		stream, err := adapter.Open(context.Background(), adapterRequest("https://api.speechify.ai/v1/audio/stream", model))
		if err != nil {
			t.Errorf("model %s: %v", model, err)
			continue
		}
		_ = stream.Close(context.Background())
	}
	if _, err := adapter.Open(context.Background(), adapterRequest("https://api.speechify.ai/v1/audio/stream", "simba-turbo")); err == nil {
		t.Fatal("deprecated simba-turbo was accepted")
	}
}

func TestCommitTextRejectsNonAudioSuccessResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"unexpected":true}`))
	}))
	defer server.Close()
	parsed, _ := url.Parse(server.URL)
	adapter, _ := New(Config{AllowedEndpointHosts: []string{parsed.Hostname()}, AllowInsecureEndpoint: true})
	stream, err := adapter.Open(context.Background(), adapterRequest(server.URL+streamPath, DefaultModel))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close(context.Background()) }()
	if err := stream.AppendText(context.Background(), "hello"); err != nil {
		t.Fatal(err)
	}
	err = stream.CommitText(context.Background())
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != "provider_unavailable" {
		t.Fatalf("commit error = %v, want provider_unavailable", err)
	}
}

func TestEmptyAudioResponseIsAnErrorEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "audio/L16;rate=24000;channels=1")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	parsed, _ := url.Parse(server.URL)
	adapter, _ := New(Config{AllowedEndpointHosts: []string{parsed.Hostname()}, AllowInsecureEndpoint: true})
	stream, err := adapter.Open(context.Background(), adapterRequest(server.URL+streamPath, DefaultModel))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close(context.Background()) }()
	_ = stream.AppendText(context.Background(), "hello")
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-stream.Events():
		var providerErr *runtimepkg.ProviderError
		if !errors.As(event.Err, &providerErr) || providerErr.Code != "provider_unavailable" {
			t.Fatalf("event = %+v, want provider_unavailable", event)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for empty-audio error")
	}
}

func TestCloseCancelsReaderBlockedOnUndrainedEvents(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Speechify-Request-Id", "speechify-request-blocked")
		w.Header().Set("Content-Type", "audio/L16;rate=24000;channels=1")
		_, _ = w.Write([]byte{0, 1, 2, 3})
	}))
	defer server.Close()
	parsed, _ := url.Parse(server.URL)
	adapter, err := New(Config{
		EventBuffer:           1,
		AllowedEndpointHosts:  []string{parsed.Hostname()},
		AllowInsecureEndpoint: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := adapter.Open(context.Background(), adapterRequest(server.URL+streamPath, DefaultModel))
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.AppendText(context.Background(), "hello"); err != nil {
		t.Fatal(err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for len(stream.Events()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(stream.Events()) == 0 {
		t.Fatal("reader did not fill the event channel")
	}

	closed := make(chan error, 1)
	go func() { closed <- stream.Close(context.Background()) }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("close blocked on the undrained event channel")
	}
}

func TestClosePreservesActiveResponseForDrainingConsumer(t *testing.T) {
	releaseAudio := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Speechify-Request-Id", "speechify-request-graceful")
		w.Header().Set("Content-Type", "audio/L16;rate=24000;channels=1")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-releaseAudio
		_, _ = w.Write([]byte{0, 1, 2, 3})
	}))
	defer server.Close()
	parsed, _ := url.Parse(server.URL)
	adapter, err := New(Config{AllowedEndpointHosts: []string{parsed.Hostname()}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatal(err)
	}
	providerStream, err := adapter.Open(context.Background(), adapterRequest(server.URL+streamPath, DefaultModel))
	if err != nil {
		t.Fatal(err)
	}
	if err := providerStream.AppendText(context.Background(), "hello"); err != nil {
		t.Fatal(err)
	}
	if err := providerStream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}

	closed := make(chan error, 1)
	go func() { closed <- providerStream.Close(context.Background()) }()
	stream := providerStream.(*stream)
	deadline := time.Now().Add(2 * time.Second)
	for {
		stream.stateMu.Lock()
		closing := stream.closed
		stream.stateMu.Unlock()
		if closing {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("close did not begin")
		}
		time.Sleep(time.Millisecond)
	}
	close(releaseAudio)

	wantTypes := []protocol.EventType{protocol.EventUsageObserved, protocol.EventAudioStarted, protocol.EventAudioFrame, protocol.EventAudioDone}
	for _, want := range wantTypes {
		event := speechifyEventWithin(t, providerStream.Events())
		if event.Type != want {
			t.Fatalf("event = %q, want %q", event.Type, want)
		}
		if want == protocol.EventAudioFrame && string(event.Audio) != string([]byte{0, 1, 2, 3}) {
			t.Fatalf("audio = %v", event.Audio)
		}
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("close did not finish after the response completed")
	}
}

func adapterRequest(endpoint, model string) runtimepkg.AdapterRequest {
	return runtimepkg.AdapterRequest{
		Kind:    protocol.SessionKindTTS,
		Plan:    protocol.SessionPlan{Execution: protocol.Execution{ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK}, Route: protocol.PlanRoute{Provider: "speechify", Model: model, Voice: "geffen_32", Adapter: AdapterID, Transport: protocol.TransportHTTP, Endpoint: endpoint, Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "speechify-key"}}},
		Options: protocol.RequestOptions{Language: "en-US"},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1},
	}
}

func speechifyEventWithin(t *testing.T, events <-chan runtimepkg.ProviderEvent) runtimepkg.ProviderEvent {
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
