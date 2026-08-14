package deepgram

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

func TestTTSAdapterUsesAura2WireContractAndMapsAudio(t *testing.T) {
	t.Parallel()
	requests := make(chan *http.Request, 1)
	server := newSpeakServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		requests <- request.Clone(request.Context())
		if message, err := readTTSControl(ctx, conn); err != nil || message.Type != "Speak" || message.Text != "Hello, " {
			t.Errorf("speak = %+v, err=%v", message, err)
			return
		}
		if message, err := readTTSControl(ctx, conn); err != nil || message.Type != "Flush" {
			t.Errorf("flush = %+v, err=%v", message, err)
			return
		}
		if err := writeJSON(ctx, conn, map[string]any{"type": "Metadata", "request_id": "dg_tts_123", "model_name": "aura-2-thalia-en"}); err != nil {
			t.Errorf("metadata: %v", err)
			return
		}
		if err := conn.Write(ctx, websocket.MessageBinary, []byte{1, 2, 3, 4}); err != nil {
			t.Errorf("audio: %v", err)
			return
		}
		if err := writeJSON(ctx, conn, map[string]any{"type": "Flushed", "sequence_id": 0}); err != nil {
			t.Errorf("flushed: %v", err)
			return
		}
		if message, err := readTTSControl(ctx, conn); err != nil || message.Type != "Close" {
			t.Errorf("close = %+v, err=%v", message, err)
			return
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	defer server.Close()

	adapter, err := NewTTS(testTTSConfig(server.URL))
	if err != nil {
		t.Fatalf("new TTS adapter: %v", err)
	}
	request := deepgramTTSRequest(server.URL, protocol.CredentialsManaged)
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open TTS stream: %v", err)
	}
	if err := stream.AppendText(context.Background(), "Hello, "); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	events := collectProviderEvents(t, stream.Events(), 4)
	if got := strings.Join(eventTypes(events), ","); got != "usage.observed,audio.started,audio.frame,audio.done" {
		t.Fatalf("event types = %s", got)
	}
	if string(events[2].Audio) != string([]byte{1, 2, 3, 4}) {
		t.Fatalf("audio = %v", events[2].Audio)
	}
	var usage struct {
		ProviderRequestID string `json:"provider_request_id"`
	}
	if err := json.Unmarshal(events[0].Data, &usage); err != nil || usage.ProviderRequestID != "dg_tts_123" {
		t.Fatalf("usage = %+v, err=%v", usage, err)
	}
	if events[3].Extensions[extensionID] == nil {
		t.Fatal("flushed event must retain Deepgram payload")
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, ok := <-stream.Events(); ok {
		t.Fatal("events must close after provider closes")
	}

	select {
	case request := <-requests:
		if got := request.Header.Get("Authorization"); got != "Bearer temporary-deepgram-token" {
			t.Fatalf("authorization = %q", got)
		}
		query := request.URL.Query()
		for key, want := range map[string]string{"model": "aura-2-thalia-en", "encoding": "linear16", "sample_rate": "16000"} {
			if got := query.Get(key); got != want {
				t.Fatalf("query %s = %q, want %q", key, got, want)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("server did not observe handshake")
	}
}

func TestTTSAdapterUsesFluxV2TurnLifecycle(t *testing.T) {
	t.Parallel()
	requests := make(chan *http.Request, 1)
	server := newSpeakServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		requests <- request.Clone(request.Context())
		if message, err := readTTSControl(ctx, conn); err != nil || message.Type != "Speak" || message.Text != "Hello Flux" {
			t.Errorf("speak = %+v, err=%v", message, err)
			return
		}
		if message, err := readTTSControl(ctx, conn); err != nil || message.Type != "Flush" {
			t.Errorf("flush = %+v, err=%v", message, err)
			return
		}
		_ = writeJSON(ctx, conn, map[string]any{"type": "Connected", "request_id": "dg_flux_tts_1", "model_name": "flux-haley-en"})
		_ = writeJSON(ctx, conn, map[string]any{"type": "SpeechStarted", "speech_id": "speech_1"})
		_ = conn.Write(ctx, websocket.MessageBinary, []byte{1, 2})
		_ = writeJSON(ctx, conn, map[string]any{"type": "Flushed", "speech_id": "speech_1"})
		_ = conn.Write(ctx, websocket.MessageBinary, []byte{3, 4})
		_ = writeJSON(ctx, conn, map[string]any{"type": "SpeechMetadata", "speech_id": "speech_1", "audio_duration_ms": 1000, "billable_character_count": 10})
		if message, err := readTTSControl(ctx, conn); err != nil || message.Type != "Close" {
			t.Errorf("close = %+v, err=%v", message, err)
			return
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	defer server.Close()

	adapter, err := NewTTS(testTTSConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	request := deepgramTTSRequest(server.URL, protocol.CredentialsBYOK)
	request.Plan.Route.Model = "flux-haley-en"
	request.Plan.Route.Endpoint = strings.Replace(request.Plan.Route.Endpoint, "/v1/speak", "/v2/speak", 1)
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := stream.AppendText(context.Background(), "Hello Flux"); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	events := collectProviderEvents(t, stream.Events(), 5)
	if got := strings.Join(eventTypes(events), ","); got != "usage.observed,audio.started,audio.frame,audio.frame,audio.done" {
		t.Fatalf("events = %s", got)
	}
	if events[4].Extensions[fluxExtensionID] == nil || string(events[2].Audio) != string([]byte{1, 2}) || string(events[3].Audio) != string([]byte{3, 4}) {
		t.Fatalf("Flux events = %+v", events)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	requestSeen := <-requests
	if requestSeen.URL.Path != "/v2/speak" || requestSeen.URL.Query().Get("model") != "flux-haley-en" {
		t.Fatalf("Flux handshake = %s", requestSeen.URL.String())
	}
}

func TestTTSAdapterCancelUsesClearAndAllowsAnotherUtterance(t *testing.T) {
	t.Parallel()
	server := newSpeakServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		first, err := readTTSControl(ctx, conn)
		if err != nil || first.Type != "Speak" || first.Text != "discard me" {
			t.Errorf("first speak = %+v, err=%v", first, err)
			return
		}
		clear, err := readTTSControl(ctx, conn)
		if err != nil || clear.Type != "Clear" {
			t.Errorf("clear = %+v, err=%v", clear, err)
			return
		}
		if err := writeJSON(ctx, conn, map[string]any{"type": "Cleared", "sequence_id": 0}); err != nil {
			t.Errorf("cleared: %v", err)
			return
		}
		second, err := readTTSControl(ctx, conn)
		if err != nil || second.Type != "Speak" || second.Text != "keep me" {
			t.Errorf("second speak = %+v, err=%v", second, err)
			return
		}
		_, _ = readTTSControl(ctx, conn) // Close
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	defer server.Close()

	adapter, _ := NewTTS(testTTSConfig(server.URL))
	stream, err := adapter.Open(context.Background(), deepgramTTSRequest(server.URL, protocol.CredentialsBYOK))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := stream.AppendText(context.Background(), "discard me"); err != nil {
		t.Fatalf("append first: %v", err)
	}
	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if err := stream.AppendText(context.Background(), "keep me"); err != nil {
		t.Fatalf("append second: %v", err)
	}
	event := collectProviderEvents(t, stream.Events(), 1)[0]
	if event.Type != protocol.EventWarning {
		t.Fatalf("cleared event = %s", event.Type)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestTTSAdapterEnforcesDocumentedLocalLimits(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 3, 12, 0, 0, 0, time.UTC)
	stream := &ttsStream{now: func() time.Time { return now }}
	if err := stream.acceptCharacters(2_000); err != nil {
		t.Fatalf("accept first characters: %v", err)
	}
	if err := stream.acceptCharacters(400); err != nil {
		t.Fatalf("accept remaining characters: %v", err)
	}
	var providerErr *runtimepkg.ProviderError
	if err := stream.acceptCharacters(1); !errors.As(err, &providerErr) || providerErr.Code != "provider_rate_limited" {
		t.Fatalf("character limit error = %v", err)
	}
	for index := 0; index < deepgramTTSFlushesPerMinute; index++ {
		if err := stream.acceptFlush(); err != nil {
			t.Fatalf("flush %d: %v", index, err)
		}
	}
	if err := stream.acceptFlush(); !errors.As(err, &providerErr) || providerErr.Code != "provider_rate_limited" {
		t.Fatalf("flush limit error = %v", err)
	}
	now = now.Add(time.Minute)
	if err := stream.acceptCharacters(1); err != nil {
		t.Fatalf("new window characters: %v", err)
	}
	if err := stream.acceptFlush(); err != nil {
		t.Fatalf("new window flush: %v", err)
	}
}

// A relay plan is managed for billing purposes but carries the connector's
// permanent Deepgram key, which authenticates with the Token scheme exactly
// like a customer-owned key — the managed Bearer scheme would be refused.
func TestTTSAdapterUsesTokenSchemeForRelayRoute(t *testing.T) {
	t.Parallel()
	requests := make(chan *http.Request, 1)
	server := newSpeakServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		requests <- request.Clone(request.Context())
		if message, err := readTTSControl(ctx, conn); err != nil || message.Type != "Close" {
			t.Errorf("close = %+v, err=%v", message, err)
			return
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	defer server.Close()

	adapter, err := NewTTS(testTTSConfig(server.URL))
	if err != nil {
		t.Fatalf("new TTS adapter: %v", err)
	}
	request := deepgramTTSRequest(server.URL, protocol.CredentialsManaged)
	request.Plan.Execution.ProviderRoute = protocol.RouteSpekoRelay
	request.Plan.Route.Credential.Value = "connector-deepgram-key"
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open relay stream: %v", err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close relay stream: %v", err)
	}

	select {
	case received := <-requests:
		if got := received.Header.Get("Authorization"); got != "Token connector-deepgram-key" {
			t.Fatalf("authorization = %q", got)
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
func TestTTSAdapterAcceptsRelayAccessCredentialKindOnRelayRoute(t *testing.T) {
	t.Parallel()
	server := newSpeakServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		if message, err := readTTSControl(ctx, conn); err != nil || message.Type != "Close" {
			t.Errorf("close = %+v, err=%v", message, err)
			return
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	defer server.Close()

	adapter, err := NewTTS(testTTSConfig(server.URL))
	if err != nil {
		t.Fatalf("new TTS adapter: %v", err)
	}
	request := deepgramTTSRequest(server.URL, protocol.CredentialsManaged)
	request.Plan.Execution.ProviderRoute = protocol.RouteSpekoRelay
	request.Plan.Route.Credential.Kind = protocol.CredentialRelayAccess
	request.Plan.Route.Credential.Value = "connector-deepgram-key"
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open relay stream with relay_access credential: %v", err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close relay stream: %v", err)
	}
}

type ttsControl struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func readTTSControl(ctx context.Context, conn *websocket.Conn) (ttsControl, error) {
	messageType, payload, err := conn.Read(ctx)
	if err != nil || messageType != websocket.MessageText {
		return ttsControl{}, fmt.Errorf("control read = (%v, %q, %w)", messageType, payload, err)
	}
	var message ttsControl
	if err := json.Unmarshal(payload, &message); err != nil {
		return ttsControl{}, err
	}
	return message, nil
}

func newSpeakServer(t *testing.T, callback func(context.Context, *http.Request, *websocket.Conn)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/speak" && request.URL.Path != "/v2/speak" {
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

func deepgramTTSRequest(serverURL string, source protocol.CredentialSource) runtimepkg.AdapterRequest {
	now := time.Date(2026, time.August, 3, 12, 0, 0, 0, time.UTC)
	endpoint, _ := url.Parse(serverURL)
	endpoint.Scheme = "ws"
	endpoint.Path = "/v1/speak"
	credential := "temporary-deepgram-token"
	if source == protocol.CredentialsBYOK {
		credential = "customer-deepgram-key"
	}
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindTTS,
		Plan: protocol.SessionPlan{
			PlanID: "plan_dg_tts", SessionID: "sess_dg_tts", AttemptID: "att_1",
			Execution:    protocol.Execution{Placement: protocol.PlacementEmbedded, ProviderRoute: protocol.RouteProviderDirect, CredentialSource: source},
			ExpiresAt:    now.Add(time.Hour),
			Route:        protocol.PlanRoute{Provider: "deepgram", Model: "aura-2-thalia-en", Adapter: TTSAdapterID, Transport: protocol.TransportWebSocket, Endpoint: endpoint.String(), Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: credential, ExpiresAt: now.Add(time.Hour)}},
			Reservation:  protocol.Reservation{ID: "res_dg_tts", LeaseDurationSeconds: 60, LeaseExpiresAt: now.Add(time.Minute), Concurrency: protocol.ConcurrencyReservation{LeaseID: "conc_dg_tts", Slots: 1}, Usage: protocol.UsageReservation{Unit: protocol.UsageUnitCharacters, AuthorizedUnits: 4_000}},
			Telemetry:    protocol.Telemetry{Endpoint: "https://control.speko.test/v1/runtime-events", Token: "telemetry-token", FlushIntervalMS: 5_000},
			Requirements: protocol.Requirements{Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision, RuntimeVersion: "test"}, Signature: "test",
		},
		Options: protocol.RequestOptions{Voice: "aura-2-thalia-en", MaxInputCharacters: 4_000},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
	}
}
