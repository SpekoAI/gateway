package cartesia

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

func TestAdapterReusesOneSocketForMultipleContextsAndMapsAudio(t *testing.T) {
	t.Parallel()

	requests := make(chan *http.Request, 1)
	server := newTTSServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		requests <- request.Clone(request.Context())
		first, err := readGeneration(ctx, conn)
		if err != nil {
			t.Errorf("first generation: %v", err)
			return
		}
		if first.Transcript != "Hello, " || !first.Continue || first.ModelID != "sonic-3" || first.Voice.ID != "voice_123" || first.Language != "en" || first.OutputFormat.Encoding != "pcm_s16le" || first.OutputFormat.Container != "raw" || first.OutputFormat.SampleRate != 16_000 || !first.AddTimestamps || first.ContextID == "" {
			t.Errorf("first generation = %+v", first)
			return
		}
		second, err := readGeneration(ctx, conn)
		if err != nil {
			t.Errorf("final generation: %v", err)
			return
		}
		if second.Transcript != "" || second.Continue || second.ContextID != first.ContextID {
			t.Errorf("final generation = %+v", second)
			return
		}
		if err := writeJSON(ctx, conn, map[string]any{"type": "chunk", "context_id": first.ContextID, "request_id": "cart_req_123", "data": base64.StdEncoding.EncodeToString([]byte{1, 2, 3})}); err != nil {
			t.Errorf("audio chunk: %v", err)
			return
		}
		if err := writeJSON(ctx, conn, map[string]any{"type": "timestamps", "context_id": first.ContextID, "word_timestamps": map[string]any{"words": []string{"Hello"}, "start": []float64{0}, "end": []float64{0.2}}}); err != nil {
			t.Errorf("timestamps: %v", err)
			return
		}
		if err := writeJSON(ctx, conn, map[string]any{"type": "done", "context_id": first.ContextID, "done": true}); err != nil {
			t.Errorf("done: %v", err)
			return
		}
		third, err := readGeneration(ctx, conn)
		if err != nil || third.Transcript != "Again" || !third.Continue || third.ContextID == "" || third.ContextID == first.ContextID {
			t.Errorf("second context generation = %+v, err=%v", third, err)
			return
		}
		fourth, err := readGeneration(ctx, conn)
		if err != nil || fourth.Transcript != "" || fourth.Continue || fourth.ContextID != third.ContextID {
			t.Errorf("second context final generation = %+v, err=%v", fourth, err)
			return
		}
		if err := writeJSON(ctx, conn, map[string]any{"type": "chunk", "context_id": third.ContextID, "request_id": "cart_req_456", "data": base64.StdEncoding.EncodeToString([]byte{4, 5})}); err != nil {
			t.Errorf("second audio chunk: %v", err)
			return
		}
		if err := writeJSON(ctx, conn, map[string]any{"type": "done", "context_id": third.ContextID, "done": true}); err != nil {
			t.Errorf("second done: %v", err)
			return
		}
		waitForClientClose(ctx, conn)
	})
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	providerStream, err := adapter.Open(context.Background(), adapterRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := providerStream.AppendText(context.Background(), "Hello, "); err != nil {
		t.Fatalf("append text: %v", err)
	}
	if err := providerStream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit text: %v", err)
	}
	events := collectProviderEvents(t, providerStream.Events(), 5)
	if got := eventTypes(events); strings.Join(got, ",") != strings.Join([]string{
		string(protocol.EventUsageObserved),
		string(protocol.EventAudioStarted),
		string(protocol.EventAudioFrame),
		string(protocol.EventAlignment),
		string(protocol.EventAudioDone),
	}, ",") {
		t.Fatalf("event types = %v", got)
	}
	if got := events[2].Audio; string(got) != string([]byte{1, 2, 3}) {
		t.Fatalf("audio = %v", got)
	}
	if events[3].Extensions[extensionID] == nil {
		t.Fatal("alignment must retain Cartesia extension data")
	}
	var usage struct {
		ProviderRequestID string `json:"provider_request_id"`
	}
	if err := json.Unmarshal(events[0].Data, &usage); err != nil || usage.ProviderRequestID != "cart_req_123" {
		t.Fatalf("usage correlation = %+v, err=%v", usage, err)
	}
	if err := providerStream.AppendText(context.Background(), "Again"); err != nil {
		t.Fatalf("append second context: %v", err)
	}
	if err := providerStream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit second context: %v", err)
	}
	secondEvents := collectProviderEvents(t, providerStream.Events(), 4)
	if got := strings.Join(eventTypes(secondEvents), ","); got != "usage.observed,audio.started,audio.frame,audio.done" {
		t.Fatalf("second context event types = %s", got)
	}
	if string(secondEvents[2].Audio) != string([]byte{4, 5}) {
		t.Fatalf("second context audio = %v", secondEvents[2].Audio)
	}
	if err := providerStream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}
	if _, ok := <-providerStream.Events(); ok {
		t.Fatal("events must close after graceful websocket close")
	}

	select {
	case request := <-requests:
		query := request.URL.Query()
		if query.Get("access_token") != "" {
			t.Fatalf("handshake query = %v", query)
		}
		if got := request.Header.Get("X-API-Key"); got != "customer-cartesia-key" {
			t.Fatalf("X-API-Key = %q", got)
		}
		if got := request.Header.Get("Cartesia-Version"); got != defaultTTSVersion {
			t.Fatalf("Cartesia-Version = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not observe websocket handshake")
	}
}

func TestAdapterRejectsMissingVoiceAndDoesNotLeakToken(t *testing.T) {
	t.Parallel()

	adapter, err := New(Config{})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	request := adapterRequest("ws://127.0.0.1:1")
	request.Options.Voice = ""
	request.Plan.Route.Credential.Value = "secret-that-must-not-leak"
	_, err = adapter.Open(context.Background(), request)
	if err == nil || !strings.Contains(err.Error(), "voice id") || strings.Contains(err.Error(), "secret-that-must-not-leak") {
		t.Fatalf("option validation error = %v", err)
	}
}

func TestAdapterUsesShortLivedQueryTokenForManagedRoute(t *testing.T) {
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
	request.Plan.Execution.CredentialSource = protocol.CredentialsManaged
	request.Plan.Route.Credential.Value = "temporary-cartesia-token"
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open managed stream: %v", err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close managed stream: %v", err)
	}

	select {
	case received := <-requests:
		if got := received.URL.Query().Get("access_token"); got != "temporary-cartesia-token" {
			t.Fatalf("access_token = %q", got)
		}
		if got := received.Header.Get("X-API-Key"); got != "" {
			t.Fatalf("managed request carried X-API-Key %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not observe managed websocket handshake")
	}
}

// A relay plan is managed for billing purposes but carries the connector's
// permanent Cartesia key, which belongs in the X-API-Key header exactly like a
// BYOK key. The access_token query channel would put the permanent key in the
// URL, where it could reach logs.
func TestAdapterUsesAPIKeyHeaderForRelayRoute(t *testing.T) {
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
		t.Fatal("server did not observe relay websocket handshake")
	}
}

// protocol.SessionPlan validation requires a relay plan to label its
// credential relay_access, while a connector that synthesizes the plan and
// drives the adapter directly labels the same permanent key bearer. The relay
// arm must accept both spellings, or one of the two constructions becomes
// quietly unreachable.
func TestAdapterAcceptsRelayAccessCredentialKindOnRelayRoute(t *testing.T) {
	t.Parallel()

	server := newTTSServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
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

func newTTSServer(t *testing.T, callback func(context.Context, *http.Request, *websocket.Conn)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/tts/websocket" {
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

func readGeneration(ctx context.Context, conn *websocket.Conn) (generation, error) {
	messageType, payload, err := conn.Read(ctx)
	if err != nil || messageType != websocket.MessageText {
		return generation{}, fmt.Errorf("generation read = (%v, %q, %v)", messageType, payload, err)
	}
	var generation generation
	if err := json.Unmarshal(payload, &generation); err != nil {
		return generation, err
	}
	return generation, nil
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
	now := time.Date(2026, time.August, 1, 11, 59, 0, 0, time.UTC)
	return runtimepkg.AdapterRequest{
		Kind:    protocol.SessionKindTTS,
		Plan:    planFor(now, endpointFromServer(serverURL)),
		Options: protocol.RequestOptions{Voice: "voice_123", Language: "en"},
		Media:   media(),
	}
}

func testConfig(serverURL string) Config {
	endpoint, _ := url.Parse(serverURL)
	return Config{AllowedEndpointHosts: []string{endpoint.Hostname()}, AllowInsecureEndpoint: true}
}

func planFor(now time.Time, endpoint string) protocol.SessionPlan {
	return protocol.SessionPlan{
		PlanID: "plan_cartesia", SessionID: "sess_cartesia", AttemptID: "att_1",
		Execution: protocol.Execution{Placement: protocol.PlacementEmbedded, ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK},
		ExpiresAt: now.Add(time.Hour),
		Route: protocol.PlanRoute{
			Provider: "cartesia", Model: "sonic-3", Adapter: AdapterID, Transport: protocol.TransportWebSocket, Endpoint: endpoint,
			Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "customer-cartesia-key", ExpiresAt: now.Add(30 * time.Minute)},
		},
		Reservation:  protocol.Reservation{ID: "res_cartesia", LeaseDurationSeconds: 60, LeaseExpiresAt: now.Add(time.Minute), Concurrency: protocol.ConcurrencyReservation{LeaseID: "conc_cartesia", Slots: 1}, Usage: protocol.UsageReservation{Unit: protocol.UsageUnitCharacters, AuthorizedUnits: 4_000}},
		Telemetry:    protocol.Telemetry{Endpoint: "https://control.speko.test/v1/runtime-events", Token: "telemetry-token", FlushIntervalMS: 5_000},
		Requirements: protocol.Requirements{Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision, RuntimeVersion: "0.1.0"},
		Signature:    "test-signature",
	}
}

func media() *protocol.MediaFormat {
	return &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1}
}

func endpointFromServer(serverURL string) string {
	endpoint, _ := url.Parse(serverURL)
	endpoint.Scheme = "ws"
	endpoint.Path = "/tts/websocket"
	return endpoint.String()
}

func collectProviderEvents(t *testing.T, events <-chan runtimepkg.ProviderEvent, want int) []runtimepkg.ProviderEvent {
	t.Helper()
	collected := make([]runtimepkg.ProviderEvent, 0, want)
	timer := time.NewTimer(time.Second)
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
