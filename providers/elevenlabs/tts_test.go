package elevenlabs

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

func TestAdapterReusesOneSocketForMultipleContextsAndMapsAlignment(t *testing.T) {
	t.Parallel()
	requests := make(chan *http.Request, 1)
	server := newMultiContextServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		requests <- request.Clone(request.Context())
		appendMessage, err := readClientMessage(ctx, conn)
		if err != nil || appendMessage.ContextID == "" || appendMessage.Text != "Hello, " || appendMessage.Flush || appendMessage.CloseContext {
			t.Errorf("append = %+v, err=%v", appendMessage, err)
			return
		}
		flushMessage, err := readClientMessage(ctx, conn)
		if err != nil || flushMessage.ContextID != appendMessage.ContextID || flushMessage.Text != "" || !flushMessage.Flush {
			t.Errorf("flush = %+v, err=%v", flushMessage, err)
			return
		}
		closeContext, err := readClientMessage(ctx, conn)
		if err != nil || closeContext.ContextID != appendMessage.ContextID || !closeContext.CloseContext {
			t.Errorf("close context = %+v, err=%v", closeContext, err)
			return
		}
		if err := writeServerJSON(ctx, conn, map[string]any{
			"contextId": appendMessage.ContextID,
			"audio":     base64.StdEncoding.EncodeToString([]byte{5, 6, 7}),
			"normalizedAlignment": map[string]any{
				"chars": []string{"H", "i"}, "charStartTimesMs": []int{0, 50}, "charDurationsMs": []int{50, 75},
			},
			"isFinal": true,
		}); err != nil {
			t.Errorf("write response: %v", err)
			return
		}
		secondAppend, err := readClientMessage(ctx, conn)
		if err != nil || secondAppend.ContextID == "" || secondAppend.ContextID == appendMessage.ContextID || secondAppend.Text != "Again" {
			t.Errorf("second append = %+v, err=%v", secondAppend, err)
			return
		}
		secondFlush, err := readClientMessage(ctx, conn)
		if err != nil || secondFlush.ContextID != secondAppend.ContextID || !secondFlush.Flush {
			t.Errorf("second flush = %+v, err=%v", secondFlush, err)
			return
		}
		secondClose, err := readClientMessage(ctx, conn)
		if err != nil || secondClose.ContextID != secondAppend.ContextID || !secondClose.CloseContext {
			t.Errorf("second close context = %+v, err=%v", secondClose, err)
			return
		}
		if err := writeServerJSON(ctx, conn, map[string]any{
			"contextId": secondAppend.ContextID,
			"audio":     base64.StdEncoding.EncodeToString([]byte{8, 9}),
			"isFinal":   true,
		}); err != nil {
			t.Errorf("write second response: %v", err)
			return
		}
		closeSocket, err := readClientMessage(ctx, conn)
		if err != nil || !closeSocket.CloseSocket {
			t.Errorf("close socket = %+v, err=%v", closeSocket, err)
			return
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), elevenLabsRequest(server.URL, protocol.CredentialsManaged))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := stream.AppendText(context.Background(), "Hello, "); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	events := collectEvents(t, stream.Events(), 4)
	if got := strings.Join(eventTypes(events), ","); got != "audio.started,audio.frame,alignment,audio.done" {
		t.Fatalf("event types = %s", got)
	}
	if string(events[1].Audio) != string([]byte{5, 6, 7}) {
		t.Fatalf("audio = %v", events[1].Audio)
	}
	var alignment struct {
		Characters   []string `json:"characters"`
		StartTimesMS []int    `json:"character_start_times_ms"`
		DurationsMS  []int    `json:"character_durations_ms"`
		Normalized   bool     `json:"normalized"`
	}
	if err := json.Unmarshal(events[2].Data, &alignment); err != nil || !alignment.Normalized || strings.Join(alignment.Characters, "") != "Hi" || len(alignment.StartTimesMS) != 2 || len(alignment.DurationsMS) != 2 {
		t.Fatalf("alignment = %+v, err=%v", alignment, err)
	}
	if events[2].Extensions[extensionID] == nil {
		t.Fatal("alignment must retain ElevenLabs payload")
	}
	if err := stream.AppendText(context.Background(), "Again"); err != nil {
		t.Fatalf("append second utterance: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit second utterance: %v", err)
	}
	secondEvents := collectEvents(t, stream.Events(), 3)
	if got := strings.Join(eventTypes(secondEvents), ","); got != "audio.started,audio.frame,audio.done" {
		t.Fatalf("second event types = %s", got)
	}
	if string(secondEvents[1].Audio) != string([]byte{8, 9}) {
		t.Fatalf("second audio = %v", secondEvents[1].Audio)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, ok := <-stream.Events(); ok {
		t.Fatal("events must close after provider closes")
	}

	select {
	case request := <-requests:
		if got := request.Header.Get("xi-api-key"); got != "" {
			t.Fatalf("xi-api-key = %q", got)
		}
		query := request.URL.Query()
		if got := query.Get("single_use_token"); got != "temporary-elevenlabs-token" {
			t.Fatalf("single_use_token = %q", got)
		}
		for key, want := range map[string]string{
			"model_id": "eleven_flash_v2_5", "output_format": "pcm_16000", "sync_alignment": "true", "language_code": "en",
		} {
			if got := query.Get(key); got != want {
				t.Fatalf("query %s = %q, want %q", key, got, want)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("server did not observe handshake")
	}
}

func TestAdapterUsesAPIKeyHeaderForBYOK(t *testing.T) {
	t.Parallel()
	requests := make(chan *http.Request, 1)
	server := newMultiContextServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		requests <- request.Clone(request.Context())
		message, _ := readClientMessage(ctx, conn)
		if !message.CloseSocket {
			t.Errorf("close socket = %+v", message)
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	defer server.Close()

	adapter, _ := New(testConfig(server.URL))
	stream, err := adapter.Open(context.Background(), elevenLabsRequest(server.URL, protocol.CredentialsBYOK))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case request := <-requests:
		if got := request.Header.Get("xi-api-key"); got != "customer-elevenlabs-key" {
			t.Fatalf("xi-api-key = %q", got)
		}
		if token := request.URL.Query().Get("single_use_token"); token != "" {
			t.Fatalf("BYOK handshake carried single-use token %q", token)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not observe handshake")
	}
}

// A relay plan is managed for billing purposes but carries the connector's
// permanent ElevenLabs key, which belongs in the xi-api-key header exactly like
// a BYOK key. The single_use_token query channel would put the permanent key in
// the URL and fail authentication besides.
func TestAdapterUsesAPIKeyHeaderForRelayRoute(t *testing.T) {
	t.Parallel()
	requests := make(chan *http.Request, 1)
	server := newMultiContextServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		requests <- request.Clone(request.Context())
		message, _ := readClientMessage(ctx, conn)
		if !message.CloseSocket {
			t.Errorf("close socket = %+v", message)
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	defer server.Close()

	adapter, _ := New(testConfig(server.URL))
	request := elevenLabsRequest(server.URL, protocol.CredentialsManaged)
	request.Plan.Execution.ProviderRoute = protocol.RouteSpekoRelay
	request.Plan.Route.Credential.Value = "connector-elevenlabs-key"
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case request := <-requests:
		if got := request.Header.Get("xi-api-key"); got != "connector-elevenlabs-key" {
			t.Fatalf("xi-api-key = %q", got)
		}
		if token := request.URL.Query().Get("single_use_token"); token != "" {
			t.Fatalf("relay handshake leaked the key into the query string: %q", token)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not observe handshake")
	}
}

// protocol.SessionPlan validation requires a relay plan to label its
// credential relay_access, while a connector that synthesizes the plan and
// drives the adapter directly labels the same permanent key bearer. The relay
// arm must accept both spellings, or one of the two constructions becomes
// quietly unreachable.
func TestAdapterAcceptsRelayAccessCredentialKindOnRelayRoute(t *testing.T) {
	t.Parallel()
	server := newMultiContextServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		message, _ := readClientMessage(ctx, conn)
		if !message.CloseSocket {
			t.Errorf("close socket = %+v", message)
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	defer server.Close()

	adapter, _ := New(testConfig(server.URL))
	request := elevenLabsRequest(server.URL, protocol.CredentialsManaged)
	request.Plan.Execution.ProviderRoute = protocol.RouteSpekoRelay
	request.Plan.Route.Credential.Kind = protocol.CredentialRelayAccess
	request.Plan.Route.Credential.Value = "connector-elevenlabs-key"
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open relay stream with relay_access credential: %v", err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close relay stream: %v", err)
	}
}

func TestAdapterCancelClosesContextWithoutFlush(t *testing.T) {
	t.Parallel()
	server := newMultiContextServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		first, err := readClientMessage(ctx, conn)
		if err != nil || first.ContextID == "" || first.Text != "discard" {
			t.Errorf("first = %+v, err=%v", first, err)
			return
		}
		cancel, err := readClientMessage(ctx, conn)
		if err != nil || cancel.ContextID != first.ContextID || !cancel.CloseContext || cancel.Flush {
			t.Errorf("cancel = %+v, err=%v", cancel, err)
			return
		}
		second, err := readClientMessage(ctx, conn)
		if err != nil || second.ContextID == "" || second.ContextID == first.ContextID || second.Text != "keep" {
			t.Errorf("second = %+v, err=%v", second, err)
			return
		}
		_, _ = readClientMessage(ctx, conn) // close_socket
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	defer server.Close()

	adapter, _ := New(testConfig(server.URL))
	stream, err := adapter.Open(context.Background(), elevenLabsRequest(server.URL, protocol.CredentialsBYOK))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := stream.AppendText(context.Background(), "discard"); err != nil {
		t.Fatalf("append first: %v", err)
	}
	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if err := stream.AppendText(context.Background(), "keep"); err != nil {
		t.Fatalf("append second: %v", err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestAdapterRejectsUnsupportedModelWithoutLeakingCredential(t *testing.T) {
	t.Parallel()
	adapter, _ := New(testConfig("http://127.0.0.1:1"))
	request := elevenLabsRequest("http://127.0.0.1:1", protocol.CredentialsBYOK)
	request.Plan.Route.Model = "eleven_v3"
	request.Plan.Route.Credential.Value = "secret-that-must-not-leak"
	_, err := adapter.Open(context.Background(), request)
	if err == nil || !strings.Contains(err.Error(), "does not support eleven_v3") || strings.Contains(err.Error(), "secret-that-must-not-leak") {
		t.Fatalf("validation error = %v", err)
	}
}

type clientMessage struct {
	ContextID    string `json:"context_id"`
	Text         string `json:"text"`
	Flush        bool   `json:"flush"`
	CloseContext bool   `json:"close_context"`
	CloseSocket  bool   `json:"close_socket"`
}

func readClientMessage(ctx context.Context, conn *websocket.Conn) (clientMessage, error) {
	messageType, payload, err := conn.Read(ctx)
	if err != nil || messageType != websocket.MessageText {
		return clientMessage{}, fmt.Errorf("client message = (%v, %q, %w)", messageType, payload, err)
	}
	var message clientMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return clientMessage{}, err
	}
	return message, nil
}

func writeServerJSON(ctx context.Context, conn *websocket.Conn, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, payload)
}

func newMultiContextServer(t *testing.T, callback func(context.Context, *http.Request, *websocket.Conn)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/text-to-speech/voice_123/multi-stream-input" {
			http.NotFound(w, request)
			return
		}
		conn, err := websocket.Accept(w, request, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		go func() {
			defer conn.CloseNow()
			callback(context.Background(), request, conn)
		}()
	}))
}

func elevenLabsRequest(serverURL string, source protocol.CredentialSource) runtimepkg.AdapterRequest {
	now := time.Date(2026, time.August, 3, 12, 0, 0, 0, time.UTC)
	endpoint, _ := url.Parse(serverURL)
	endpoint.Scheme = "ws"
	endpoint.Path = "/v1/text-to-speech"
	credential := "temporary-elevenlabs-token"
	if source == protocol.CredentialsBYOK {
		credential = "customer-elevenlabs-key"
	}
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindTTS,
		Plan: protocol.SessionPlan{
			PlanID: "plan_el_tts", SessionID: "sess_el_tts", AttemptID: "att_1",
			Execution:    protocol.Execution{Placement: protocol.PlacementEmbedded, ProviderRoute: protocol.RouteProviderDirect, CredentialSource: source},
			ExpiresAt:    now.Add(time.Hour),
			Route:        protocol.PlanRoute{Provider: "elevenlabs", Model: "eleven_flash_v2_5", Adapter: AdapterID, Transport: protocol.TransportWebSocket, Endpoint: endpoint.String(), Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: credential, ExpiresAt: now.Add(time.Hour)}},
			Reservation:  protocol.Reservation{ID: "res_el_tts", LeaseDurationSeconds: 60, LeaseExpiresAt: now.Add(time.Minute), Concurrency: protocol.ConcurrencyReservation{LeaseID: "conc_el_tts", Slots: 1}, Usage: protocol.UsageReservation{Unit: protocol.UsageUnitCharacters, AuthorizedUnits: 4_000}},
			Telemetry:    protocol.Telemetry{Endpoint: "https://control.speko.test/v1/runtime-events", Token: "telemetry-token", FlushIntervalMS: 5_000},
			Requirements: protocol.Requirements{Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision, RuntimeVersion: "test"}, Signature: "test",
		},
		Options: protocol.RequestOptions{Voice: "voice_123", Language: "en", MaxInputCharacters: 4_000},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
	}
}

func testConfig(serverURL string) Config {
	endpoint, _ := url.Parse(serverURL)
	return Config{AllowedEndpointHosts: []string{endpoint.Hostname()}, AllowInsecureEndpoint: true}
}

func collectEvents(t *testing.T, events <-chan runtimepkg.ProviderEvent, want int) []runtimepkg.ProviderEvent {
	t.Helper()
	collected := make([]runtimepkg.ProviderEvent, 0, want)
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for len(collected) < want {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatalf("events closed after %d", len(collected))
			}
			if event.Err != nil {
				t.Fatalf("provider error: %v", event.Err)
			}
			collected = append(collected, event)
		case <-timer.C:
			t.Fatal("timed out waiting for events")
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
