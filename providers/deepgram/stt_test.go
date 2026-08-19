package deepgram

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

func TestAdapterUsesDeepgramWireContractAndMapsEvents(t *testing.T) {
	t.Parallel()

	requests := make(chan *http.Request, 1)
	server := newListenServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		requests <- request.Clone(request.Context())
		messageType, payload, err := conn.Read(ctx)
		if err != nil || messageType != websocket.MessageBinary || string(payload) != "\x01\x02\x03" {
			t.Errorf("audio message = (%v, %q, %v)", messageType, payload, err)
			return
		}
		if err := assertControl(ctx, conn, "Finalize"); err != nil {
			t.Errorf("finalize: %v", err)
			return
		}
		if err := writeJSON(ctx, conn, map[string]any{"type": "SpeechStarted", "timestamp": 0.25}); err != nil {
			t.Errorf("write speech started: %v", err)
			return
		}
		if err := writeJSON(ctx, conn, map[string]any{
			"type": "Results", "start": 0.25, "duration": 0.5, "is_final": false,
			"metadata": map[string]any{"request_id": "dg_req_123"},
			"channel":  map[string]any{"alternatives": []map[string]any{{"transcript": "hello"}}},
		}); err != nil {
			t.Errorf("write partial result: %v", err)
			return
		}
		if err := writeJSON(ctx, conn, map[string]any{
			"type": "Results", "start": 0.25, "duration": 0.75, "is_final": true, "speech_final": true,
			"metadata": map[string]any{"request_id": "dg_req_123"},
			"channel":  map[string]any{"alternatives": []map[string]any{{"transcript": "hello world"}}},
		}); err != nil {
			t.Errorf("write final result: %v", err)
			return
		}
		if err := assertControl(ctx, conn, "CloseStream"); err != nil {
			t.Errorf("close stream: %v", err)
			return
		}
		if err := conn.Close(websocket.StatusNormalClosure, ""); err != nil {
			t.Errorf("close server socket: %v", err)
		}
	})
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	request := adapterRequest(server.URL, protocol.RequestOptions{Language: "en-US"})
	providerStream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := providerStream.WriteAudio(context.Background(), []byte{1, 2, 3}); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	if err := providerStream.CommitAudio(context.Background()); err != nil {
		t.Fatalf("commit audio: %v", err)
	}

	events := collectProviderEvents(t, providerStream.Events(), 5)
	if got := eventTypes(events); strings.Join(got, ",") != strings.Join([]string{
		string(protocol.EventSpeechStarted),
		string(protocol.EventUsageObserved),
		string(protocol.EventTranscriptDelta),
		string(protocol.EventTranscriptFinal),
		string(protocol.EventSpeechEnded),
	}, ",") {
		t.Fatalf("event types = %v", got)
	}
	if events[3].Extensions[extensionID] == nil {
		t.Fatal("final transcript must retain its Deepgram extension")
	}
	var final struct {
		Text              string `json:"text"`
		AudioStartMS      int64  `json:"audio_start_ms"`
		AudioEndMS        int64  `json:"audio_end_ms"`
		ProviderRequestID string `json:"provider_request_id"`
	}
	if err := json.Unmarshal(events[3].Data, &final); err != nil {
		t.Fatalf("decode final transcript: %v", err)
	}
	if final.Text != "hello world" || final.AudioStartMS != 250 || final.AudioEndMS != 1000 || final.ProviderRequestID != "dg_req_123" {
		t.Fatalf("final transcript = %+v", final)
	}
	var usage struct {
		ProviderRequestID string `json:"provider_request_id"`
	}
	if err := json.Unmarshal(events[1].Data, &usage); err != nil || usage.ProviderRequestID != "dg_req_123" {
		t.Fatalf("usage correlation = %+v, err=%v", usage, err)
	}
	if err := providerStream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}
	if _, ok := <-providerStream.Events(); ok {
		t.Fatal("events must close after server closes the websocket")
	}

	select {
	case received := <-requests:
		if got := received.Header.Get("Authorization"); got != "Token customer-api-key" {
			t.Fatalf("authorization = %q", got)
		}
		query := received.URL.Query()
		for key, want := range map[string]string{
			"model": "nova-3", "language": "en-US", "encoding": "linear16", "sample_rate": "16000", "channels": "1", "interim_results": "true", "endpointing": "false",
		} {
			if got := query.Get(key); got != want {
				t.Fatalf("query %s = %q, want %q", key, got, want)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("server did not observe websocket handshake")
	}
}

func TestBufferedAdapterLeavesDeepgramEndpointingEnabled(t *testing.T) {
	t.Parallel()

	requests := make(chan *http.Request, 1)
	done := make(chan struct{})
	server := newListenServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		defer close(done)
		requests <- request.Clone(request.Context())
		if err := assertControl(ctx, conn, "CloseStream"); err != nil {
			t.Errorf("close stream: %v", err)
			return
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	request := adapterRequest(server.URL, protocol.RequestOptions{})
	request.Delivery = runtimepkg.AudioDeliveryBuffered
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}
	for range stream.Events() {
	}
	<-done

	query := (<-requests).URL.Query()
	if query.Has("endpointing") {
		t.Fatalf("buffered query disables endpointing: %v", query)
	}
	if query.Get("interim_results") != "true" {
		t.Fatalf("buffered query lost interim results: %v", query)
	}
}

func TestAdapterUsesFluxV2TurnProtocol(t *testing.T) {
	t.Parallel()
	requests := make(chan *http.Request, 1)
	server := newListenServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		requests <- request.Clone(request.Context())
		messageType, payload, err := conn.Read(ctx)
		if err != nil || messageType != websocket.MessageBinary || string(payload) != "\x01\x02" {
			t.Errorf("audio message = (%v, %q, %v)", messageType, payload, err)
			return
		}
		for _, message := range []map[string]any{
			{"type": "Connected", "request_id": "flux_req_1", "sequence_id": 0},
			{"type": "TurnInfo", "request_id": "flux_req_1", "event": "StartOfTurn", "turn_index": 0, "audio_window_start": 0.2},
			{"type": "TurnInfo", "request_id": "flux_req_1", "event": "Update", "turn_index": 0, "audio_window_start": 0.2, "audio_window_end": 0.7, "transcript": "hello"},
			{"type": "TurnInfo", "request_id": "flux_req_1", "event": "EagerEndOfTurn", "turn_index": 0, "transcript": "hello there"},
			{"type": "TurnInfo", "request_id": "flux_req_1", "event": "TurnResumed", "turn_index": 0},
			{"type": "TurnInfo", "request_id": "flux_req_1", "event": "EndOfTurn", "turn_index": 0, "audio_window_start": 0.2, "audio_window_end": 1.1, "transcript": "hello there", "end_of_turn_confidence": 0.91},
		} {
			if err := writeJSON(ctx, conn, message); err != nil {
				t.Errorf("write Flux event: %v", err)
				return
			}
		}
		if err := assertControl(ctx, conn, "CloseStream"); err != nil {
			t.Errorf("close stream: %v", err)
			return
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	request := adapterRequest(server.URL, protocol.RequestOptions{Language: "es"})
	request.Plan.Route.Model = fluxMultilingual
	request.Plan.Route.Endpoint = strings.Replace(request.Plan.Route.Endpoint, "/v1/listen", "/v2/listen", 1)
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := stream.WriteAudio(context.Background(), []byte{1, 2}); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	if err := stream.CommitAudio(context.Background()); err != nil {
		t.Fatalf("Flux commit is a no-op: %v", err)
	}
	events := collectProviderEvents(t, stream.Events(), 7)
	wantTypes := "usage.observed,speech.started,transcript.delta,warning,warning,transcript.final,speech.ended"
	if got := strings.Join(eventTypes(events), ","); got != wantTypes {
		t.Fatalf("event types = %s, want %s", got, wantTypes)
	}
	if events[5].Extensions[fluxExtensionID] == nil {
		t.Fatal("Flux final transcript must retain the v2 extension")
	}
	var final struct {
		Text                string  `json:"text"`
		AudioEndMS          int64   `json:"audio_end_ms"`
		TurnIndex           int     `json:"turn_index"`
		EndOfTurnConfidence float64 `json:"end_of_turn_confidence"`
	}
	if err := json.Unmarshal(events[5].Data, &final); err != nil || final.Text != "hello there" || final.AudioEndMS != 1100 || final.TurnIndex != 0 || final.EndOfTurnConfidence != 0.91 {
		t.Fatalf("final = %+v, err=%v", final, err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	requestSeen := <-requests
	query := requestSeen.URL.Query()
	if query.Get("model") != fluxMultilingual || query.Get("language_hint") != "es" {
		t.Fatalf("Flux query = %v", query)
	}
	for _, forbidden := range []string{"channels", "interim_results", "endpointing", "language", "extra"} {
		if query.Has(forbidden) {
			t.Fatalf("Flux query carries v1 parameter %q: %v", forbidden, query)
		}
	}
}

func TestAdapterRejectsNonBearerCredentialWithoutLeakingIt(t *testing.T) {
	t.Parallel()

	adapter, err := New(Config{})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	request := adapterRequest("ws://127.0.0.1:1", protocol.RequestOptions{})
	request.Plan.Route.Credential = &protocol.DelegatedCredential{Kind: protocol.CredentialSignedURL, Value: "secret-that-must-not-leak"}
	_, err = adapter.Open(context.Background(), request)
	if err == nil || !strings.Contains(err.Error(), "bearer credential") || strings.Contains(err.Error(), "secret-that-must-not-leak") {
		t.Fatalf("credential validation error = %v", err)
	}
}

func TestAdapterUsesDeepgramTokenSchemeForBYOK(t *testing.T) {
	t.Parallel()

	requests := make(chan *http.Request, 1)
	server := newListenServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		requests <- request.Clone(request.Context())
		if err := assertControl(ctx, conn, "CloseStream"); err != nil {
			t.Errorf("close stream: %v", err)
			return
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	request := adapterRequest(server.URL, protocol.RequestOptions{})
	request.Plan.Execution.CredentialSource = protocol.CredentialsBYOK
	request.Plan.Route.Credential.Value = "customer-api-key"
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open BYOK stream: %v", err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close BYOK stream: %v", err)
	}

	select {
	case received := <-requests:
		if got := received.Header.Get("Authorization"); got != "Token customer-api-key" {
			t.Fatalf("authorization = %q", got)
		}
		if got := received.URL.Query().Get("extra"); got != "" {
			t.Fatalf("BYOK request carried unexpected extra metadata %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not observe BYOK websocket handshake")
	}
}

func TestAdapterUsesDelegatedBearerForManagedRoute(t *testing.T) {
	t.Parallel()

	requests := make(chan *http.Request, 1)
	server := newListenServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		requests <- request.Clone(request.Context())
		if err := assertControl(ctx, conn, "CloseStream"); err != nil {
			t.Errorf("close stream: %v", err)
			return
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	request := adapterRequest(server.URL, protocol.RequestOptions{})
	request.Plan.Execution.CredentialSource = protocol.CredentialsManaged
	request.Plan.Route.Credential.Value = "temporary-deepgram-token"
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open managed stream: %v", err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close managed stream: %v", err)
	}

	select {
	case received := <-requests:
		if got := received.Header.Get("Authorization"); got != "Bearer temporary-deepgram-token" {
			t.Fatalf("authorization = %q", got)
		}
		if got := received.URL.Query().Get("extra"); got != "speko_reservation:res_deepgram" {
			t.Fatalf("managed request metadata = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not observe managed websocket handshake")
	}
}

// A relay plan is managed for billing purposes but carries the connector's
// permanent Deepgram key. Both halves of that split must survive together: the
// Token scheme authenticates the permanent key, and the reservation extra keeps
// provider-side usage bound to the Speko reservation.
func TestAdapterUsesTokenSchemeAndKeepsReservationTagForRelayRoute(t *testing.T) {
	t.Parallel()

	requests := make(chan *http.Request, 1)
	server := newListenServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		requests <- request.Clone(request.Context())
		if err := assertControl(ctx, conn, "CloseStream"); err != nil {
			t.Errorf("close stream: %v", err)
			return
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	request := adapterRequest(server.URL, protocol.RequestOptions{})
	request.Plan.Execution.ProviderRoute = protocol.RouteSpekoRelay
	request.Plan.Execution.CredentialSource = protocol.CredentialsManaged
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
		if got := received.URL.Query().Get("extra"); got != "speko_reservation:res_deepgram" {
			t.Fatalf("relay request metadata = %q", got)
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

	server := newListenServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		if err := assertControl(ctx, conn, "CloseStream"); err != nil {
			t.Errorf("close stream: %v", err)
			return
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	request := adapterRequest(server.URL, protocol.RequestOptions{})
	request.Plan.Execution.ProviderRoute = protocol.RouteSpekoRelay
	request.Plan.Execution.CredentialSource = protocol.CredentialsManaged
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

func newListenServer(t *testing.T, callback func(context.Context, *http.Request, *websocket.Conn)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		listenHandler(t, w, r, callback)
	}))
}

func listenHandler(t *testing.T, w http.ResponseWriter, r *http.Request, callback func(context.Context, *http.Request, *websocket.Conn)) {
	t.Helper()
	if r.URL.Path != "/v1/listen" && r.URL.Path != "/v2/listen" {
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
}

func adapterRequest(serverURL string, options protocol.RequestOptions) runtimepkg.AdapterRequest {
	now := time.Date(2026, time.August, 1, 11, 59, 0, 0, time.UTC)
	return runtimepkg.AdapterRequest{
		Kind:     protocol.SessionKindSTT,
		Plan:     planFor(now, endpointFromServer(serverURL)),
		Options:  options,
		Media:    media(),
		Delivery: runtimepkg.AudioDeliveryLive,
	}
}

func planFor(now time.Time, endpoint string) protocol.SessionPlan {
	return protocol.SessionPlan{
		PlanID:    "plan_deepgram",
		SessionID: "sess_deepgram",
		AttemptID: "att_1",
		Execution: protocol.Execution{Placement: protocol.PlacementEmbedded, ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK},
		ExpiresAt: now.Add(time.Hour),
		Route: protocol.PlanRoute{
			Provider: "deepgram", Model: "nova-3", Adapter: AdapterID, Transport: protocol.TransportWebSocket, Endpoint: endpoint,
			Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "customer-api-key", ExpiresAt: now.Add(30 * time.Minute)},
		},
		Reservation:  protocol.Reservation{ID: "res_deepgram", LeaseDurationSeconds: 60, LeaseExpiresAt: now.Add(time.Minute), Concurrency: protocol.ConcurrencyReservation{LeaseID: "conc_deepgram", Slots: 1}, Usage: protocol.UsageReservation{Unit: protocol.UsageUnitDurationSeconds, AuthorizedUnits: 60}},
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
	endpoint.Path = "/v1/listen"
	return endpoint.String()
}

func testConfig(serverURL string) Config {
	endpoint, _ := url.Parse(serverURL)
	return Config{AllowedEndpointHosts: []string{endpoint.Hostname()}, AllowInsecureEndpoint: true}
}

func testTTSConfig(serverURL string) TTSConfig {
	endpoint, _ := url.Parse(serverURL)
	return TTSConfig{AllowedEndpointHosts: []string{endpoint.Hostname()}, AllowInsecureEndpoint: true}
}

func assertControl(ctx context.Context, conn *websocket.Conn, want string) error {
	messageType, payload, err := conn.Read(ctx)
	if err != nil || messageType != websocket.MessageText {
		return fmt.Errorf("control read = (%v, %q, %w)", messageType, payload, err)
	}
	var control struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(payload, &control); err != nil || control.Type != want {
		return fmt.Errorf("control = %q err=%v, want %q", payload, err, want)
	}
	return nil
}

func writeJSON(ctx context.Context, conn *websocket.Conn, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if err := conn.Write(ctx, websocket.MessageText, payload); err != nil {
		return err
	}
	return nil
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

func TestListenEndpointRejectsInvalidMediaAndPath(t *testing.T) {
	t.Parallel()
	policy, err := upstream.NewWebSocketPolicy(officialAPIHost, nil, false)
	if err != nil {
		t.Fatalf("endpoint policy: %v", err)
	}

	_, err = listenEndpoint(policy, "wss://api.deepgram.com/not-listen", "nova-3", protocol.RequestOptions{}, *media(), runtimepkg.AudioDeliveryLive, "")
	if err == nil {
		t.Fatal("expected path validation failure")
	}
	_, err = listenEndpoint(policy, "wss://api.deepgram.com/v1/listen", "nova-3", protocol.RequestOptions{}, protocol.MediaFormat{Encoding: "mulaw", SampleRateHz: 8_000, Channels: 1}, runtimepkg.AudioDeliveryLive, "")
	if err == nil {
		t.Fatalf("expected media validation failure, got %v", err)
	}
}
