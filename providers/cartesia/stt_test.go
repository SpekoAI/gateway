package cartesia

import (
	"context"
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

func TestSTTAdapterUsesManualFinalizeAndMapsTranscripts(t *testing.T) {
	t.Parallel()
	requests := make(chan *http.Request, 1)
	server := newCartesiaSTTServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		requests <- request.Clone(request.Context())
		messageType, audio, err := conn.Read(ctx)
		if err != nil || messageType != websocket.MessageBinary || string(audio) != "\x01\x02\x03\x04" {
			t.Errorf("audio = (%v, %q, %v)", messageType, audio, err)
			return
		}
		if err := assertCartesiaText(ctx, conn, "finalize"); err != nil {
			t.Errorf("finalize: %v", err)
			return
		}
		for _, message := range []map[string]any{
			{"type": "transcript", "text": "hello", "is_final": false, "request_id": "cart-stt-1", "duration": 0.25, "language": "en"},
			{"type": "transcript", "text": "hello world", "is_final": true, "request_id": "cart-stt-1", "duration": 0.5, "language": "en", "words": []map[string]any{{"word": "hello"}}},
		} {
			if err := writeCartesiaJSON(ctx, conn, message); err != nil {
				t.Errorf("write transcript: %v", err)
				return
			}
		}
		if err := assertCartesiaText(ctx, conn, "close"); err != nil {
			t.Errorf("close: %v", err)
			return
		}
		if err := writeCartesiaJSON(ctx, conn, map[string]any{"type": "done", "request_id": "cart-stt-1"}); err != nil {
			t.Errorf("write done: %v", err)
		}
	})
	defer server.Close()

	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	request := cartesiaSTTRequest(server.URL, protocol.CredentialsManaged)
	providerStream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := providerStream.WriteAudio(context.Background(), []byte{1, 2, 3, 4}); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	if err := providerStream.CommitAudio(context.Background()); err != nil {
		t.Fatalf("commit audio: %v", err)
	}
	events := collectCartesiaSTTEvents(t, providerStream.Events(), 5)
	want := []protocol.EventType{
		protocol.EventUsageObserved, protocol.EventSpeechStarted, protocol.EventTranscriptDelta,
		protocol.EventTranscriptFinal, protocol.EventSpeechEnded,
	}
	for index := range want {
		if events[index].Type != want[index] {
			t.Fatalf("event %d = %q, want %q", index, events[index].Type, want[index])
		}
	}
	if events[3].Extensions[extensionID] == nil {
		t.Fatal("final transcript did not retain the Cartesia extension")
	}
	var final struct {
		Text              string `json:"text"`
		IsFinal           bool   `json:"is_final"`
		AudioEndMS        int64  `json:"audio_end_ms"`
		ProviderRequestID string `json:"provider_request_id"`
	}
	if err := json.Unmarshal(events[3].Data, &final); err != nil || final.Text != "hello world" || !final.IsFinal || final.AudioEndMS != 500 || final.ProviderRequestID != "cart-stt-1" {
		t.Fatalf("final transcript = %+v, err=%v", final, err)
	}
	if err := providerStream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}
	select {
	case _, ok := <-providerStream.Events():
		if ok {
			t.Fatal("events remained open after Cartesia done")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Cartesia done")
	}

	select {
	case received := <-requests:
		if received.Header.Get("X-API-Key") != "" {
			t.Fatal("managed STT exposed the root API key header")
		}
		query := received.URL.Query()
		for key, want := range map[string]string{
			"model": "ink-2", "encoding": "pcm_s16le", "sample_rate": "16000",
			"language": "en", "cartesia_version": defaultVersion, "access_token": "temporary-cartesia-token",
		} {
			if got := query.Get(key); got != want {
				t.Fatalf("query %s = %q, want %q", key, got, want)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("server did not observe handshake")
	}
}

func TestSTTAdapterUsesAPIKeyForBYOKAndRejectsUnsupportedMedia(t *testing.T) {
	t.Parallel()
	requests := make(chan *http.Request, 1)
	server := newCartesiaSTTServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		requests <- request.Clone(request.Context())
		if err := assertCartesiaText(ctx, conn, "close"); err != nil {
			t.Errorf("close: %v", err)
			return
		}
		_ = writeCartesiaJSON(ctx, conn, map[string]any{"type": "done"})
	})
	defer server.Close()
	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	request := cartesiaSTTRequest(server.URL, protocol.CredentialsBYOK)
	request.Plan.Route.Credential.Value = "customer-cartesia-key"
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open BYOK: %v", err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close BYOK: %v", err)
	}
	select {
	case received := <-requests:
		if got := received.Header.Get("X-API-Key"); got != "customer-cartesia-key" {
			t.Fatalf("X-API-Key = %q", got)
		}
		if got := received.URL.Query().Get("access_token"); got != "" {
			t.Fatalf("BYOK URL contained access token %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not observe BYOK handshake")
	}

	bad := cartesiaSTTRequest(server.URL, protocol.CredentialsBYOK)
	bad.Media = &protocol.MediaFormat{Encoding: "opus", SampleRateHz: 16_000, Channels: 1}
	if _, err := adapter.Open(context.Background(), bad); err == nil || !strings.Contains(err.Error(), "pcm_s16le") {
		t.Fatalf("unsupported-media error = %v", err)
	}
}

// A relay plan is managed for billing purposes but carries the connector's
// permanent Cartesia key, which belongs in the X-API-Key header exactly like a
// BYOK key. The access_token query channel would put the permanent key in the
// URL, where it could reach logs.
func TestSTTAdapterUsesAPIKeyHeaderForRelayRoute(t *testing.T) {
	t.Parallel()
	requests := make(chan *http.Request, 1)
	server := newCartesiaSTTServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		requests <- request.Clone(request.Context())
		if err := assertCartesiaText(ctx, conn, "close"); err != nil {
			t.Errorf("close: %v", err)
			return
		}
		_ = writeCartesiaJSON(ctx, conn, map[string]any{"type": "done"})
	})
	defer server.Close()
	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	request := cartesiaSTTRequest(server.URL, protocol.CredentialsManaged)
	request.Plan.Execution.ProviderRoute = protocol.RouteSpekoRelay
	request.Plan.Route.Credential.Value = "connector-cartesia-key"
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open relay stream: %v", err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close relay stream: %v", err)
	}
	select {
	case received := <-requests:
		if got := received.Header.Get("X-API-Key"); got != "connector-cartesia-key" {
			t.Fatalf("X-API-Key = %q", got)
		}
		if got := received.URL.Query().Get("access_token"); got != "" {
			t.Fatalf("relay URL contained access token %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not observe relay handshake")
	}
}

// protocol.SessionPlan validation requires a relay plan to label its
// credential relay_access, while a connector that synthesizes the plan and
// drives the adapter directly labels the same permanent key bearer. The relay
// arm must accept both spellings, or one of the two constructions becomes
// quietly unreachable.
func TestSTTAdapterAcceptsRelayAccessCredentialKindOnRelayRoute(t *testing.T) {
	t.Parallel()
	server := newCartesiaSTTServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		if err := assertCartesiaText(ctx, conn, "close"); err != nil {
			t.Errorf("close: %v", err)
			return
		}
		_ = writeCartesiaJSON(ctx, conn, map[string]any{"type": "done"})
	})
	defer server.Close()
	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	request := cartesiaSTTRequest(server.URL, protocol.CredentialsManaged)
	request.Plan.Execution.ProviderRoute = protocol.RouteSpekoRelay
	request.Plan.Route.Credential.Kind = protocol.CredentialRelayAccess
	request.Plan.Route.Credential.Value = "connector-cartesia-key"
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open relay stream with relay_access credential: %v", err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close relay stream: %v", err)
	}
}

func TestSTTAdapterMapsProviderError(t *testing.T) {
	t.Parallel()
	server := newCartesiaSTTServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		_ = writeCartesiaJSON(ctx, conn, map[string]any{
			"type": "error", "status_code": 429, "message": "rate limited", "request_id": "cart-stt-error",
		})
	})
	defer server.Close()
	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), cartesiaSTTRequest(server.URL, protocol.CredentialsManaged))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	events := stream.Events()
	first := <-events
	if first.Type != protocol.EventUsageObserved {
		t.Fatalf("first event = %q, want usage.observed", first.Type)
	}
	providerFailure := <-events
	var providerErr *runtimepkg.ProviderError
	if !errors.As(providerFailure.Err, &providerErr) || providerErr.ProviderStatus != 429 || providerErr.Code != "provider_rate_limited" || !providerErr.Retryable || providerErr.Extensions[extensionID] == nil {
		t.Fatalf("provider error = %#v", providerFailure.Err)
	}
}

func newCartesiaSTTServer(t *testing.T, callback func(context.Context, *http.Request, *websocket.Conn)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/stt/websocket" {
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

func cartesiaSTTRequest(serverURL string, source protocol.CredentialSource) runtimepkg.AdapterRequest {
	now := time.Date(2026, time.August, 6, 12, 0, 0, 0, time.UTC)
	endpoint, _ := url.Parse(serverURL)
	endpoint.Scheme = "ws"
	endpoint.Path = "/stt/websocket"
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindSTT,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{Placement: protocol.PlacementEmbedded, ProviderRoute: protocol.RouteProviderDirect, CredentialSource: source},
			Route: protocol.PlanRoute{
				Provider: "cartesia", Model: "ink-2", Adapter: STTAdapterID, Transport: protocol.TransportWebSocket, Endpoint: endpoint.String(),
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "temporary-cartesia-token", ExpiresAt: now.Add(time.Minute)},
			},
		},
		Options: protocol.RequestOptions{Language: "en"},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
	}
}

func sttTestConfig(serverURL string) STTConfig {
	endpoint, _ := url.Parse(serverURL)
	return STTConfig{AllowedEndpointHosts: []string{endpoint.Hostname()}, AllowInsecureEndpoint: true}
}

func assertCartesiaText(ctx context.Context, conn *websocket.Conn, want string) error {
	messageType, payload, err := conn.Read(ctx)
	if err != nil || messageType != websocket.MessageText || string(payload) != want {
		return fmt.Errorf("message = (%v, %q, %w), want text %q", messageType, payload, err, want)
	}
	return nil
}

func writeCartesiaJSON(ctx context.Context, conn *websocket.Conn, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, payload)
}

func collectCartesiaSTTEvents(t *testing.T, events <-chan runtimepkg.ProviderEvent, count int) []runtimepkg.ProviderEvent {
	t.Helper()
	result := make([]runtimepkg.ProviderEvent, 0, count)
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for len(result) < count {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatalf("events closed after %d events", len(result))
			}
			if event.Err != nil {
				t.Fatalf("provider event error: %v", event.Err)
			}
			result = append(result, event)
		case <-timer.C:
			t.Fatalf("timed out after %d events", len(result))
		}
	}
	return result
}
