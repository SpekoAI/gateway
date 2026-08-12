package gateway_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/gateway"
	"github.com/SpekoAI/gateway/protocol"
	"github.com/SpekoAI/gateway/providers/mock"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const testConversationID = "conv_0123456789abcdef0123456789abcdef"

// fakeTelemetrySink captures every recorded event so tests can assert the
// exact exporter contract without any network delivery.
type fakeTelemetrySink struct {
	mu     sync.Mutex
	events []runtimepkg.TelemetryEvent
	full   bool
}

func (s *fakeTelemetrySink) TryRecord(event runtimepkg.TelemetryEvent) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.full {
		return false
	}
	s.events = append(s.events, event)
	return true
}

func (s *fakeTelemetrySink) recorded() []runtimepkg.TelemetryEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]runtimepkg.TelemetryEvent(nil), s.events...)
}

func newTurnEventServer(t *testing.T, sink runtimepkg.TelemetrySink, destinations gateway.TurnEventDestinations, workload *protocol.Workload) *httptest.Server {
	t.Helper()
	config, _ := newServerConfigWithAdapter(t, mock.NewSTTAdapter("mock.stt.v1"), 0, 0, 0, 0)
	config.Telemetry = sink
	config.TurnEvents = destinations
	config.Workload = workload
	server, err := gateway.New(config)
	if err != nil {
		t.Fatalf("new gateway: %v", err)
	}
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)
	return httpServer
}

func turnEventPayload(eventType, turnID string, seq uint64, data map[string]any) map[string]any {
	event := map[string]any{
		"type":            eventType,
		"conversation_id": testConversationID,
		"seq":             seq,
		"created_at_ms":   1_765_432_100_123,
		"data":            data,
	}
	if turnID != "" {
		event["turn_id"] = turnID
	}
	return event
}

func postTurnEvents(t *testing.T, baseURL, token string, events []map[string]any) *http.Response {
	t.Helper()
	response := postJSON(t, baseURL+"/v1/turn-events", map[string]any{"events": events}, token, "")
	return response
}

func decodeTurnEventResponse(t *testing.T, response *http.Response) (accepted, dropped int) {
	t.Helper()
	defer response.Body.Close()
	var body struct {
		Accepted int `json:"accepted"`
		Dropped  int `json:"dropped"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode turn event response: %v", err)
	}
	return body.Accepted, body.Dropped
}

func TestTurnEventsRequireLocalAuth(t *testing.T) {
	t.Parallel()
	sink := &fakeTelemetrySink{}
	httpServer := newTurnEventServer(t, sink, gateway.TurnEventDestinations{AnonymousEndpoint: "https://telemetry.speko.test/v1/anonymous-turn-events"}, nil)

	events := []map[string]any{turnEventPayload("turn.completed", "turn_000001", 1, map[string]any{"mono_ms": 10})}
	response := postTurnEvents(t, httpServer.URL, "", events)
	defer response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated turn events status = %d", response.StatusCode)
	}
	wrongToken := postTurnEvents(t, httpServer.URL, "not-the-token", events)
	defer wrongToken.Body.Close()
	if wrongToken.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong-token turn events status = %d", wrongToken.StatusCode)
	}
	if len(sink.recorded()) != 0 {
		t.Fatal("unauthenticated request must not reach the telemetry sink")
	}
}

func TestTurnEventsValidationRejectsWholeBatch(t *testing.T) {
	t.Parallel()
	valid := turnEventPayload("turn.completed", "turn_000001", 2, map[string]any{"mono_ms": 10})
	tests := []struct {
		name  string
		event map[string]any
	}{
		{"unknown type", turnEventPayload("turn.exploded", "turn_000001", 1, map[string]any{"mono_ms": 1})},
		{"bad conversation id", func() map[string]any {
			event := turnEventPayload("turn.completed", "turn_000001", 1, map[string]any{"mono_ms": 1})
			event["conversation_id"] = "conv_SHOUTING"
			return event
		}()},
		{"bad turn id shape", turnEventPayload("turn.completed", "turn_1", 1, map[string]any{"mono_ms": 1})},
		{"missing turn id on turn-scoped", turnEventPayload("turn.completed", "", 1, map[string]any{"mono_ms": 1})},
		{"turn id on conversation-scoped", turnEventPayload("conversation.ended", "turn_000001", 1, map[string]any{"mono_ms": 1, "reason": "hangup", "turn_count": 1})},
		{"zero seq", turnEventPayload("turn.completed", "turn_000001", 0, map[string]any{"mono_ms": 1})},
		{"zero created_at_ms", func() map[string]any {
			event := turnEventPayload("turn.completed", "turn_000001", 1, map[string]any{"mono_ms": 1})
			event["created_at_ms"] = 0
			return event
		}()},
		{"missing mono_ms", turnEventPayload("turn.completed", "turn_000001", 1, map[string]any{})},
		{"negative mono_ms", turnEventPayload("turn.completed", "turn_000001", 1, map[string]any{"mono_ms": -1})},
		{"fractional mono_ms", turnEventPayload("turn.completed", "turn_000001", 1, map[string]any{"mono_ms": 10.5})},
		{"unknown data field", turnEventPayload("turn.completed", "turn_000001", 1, map[string]any{"mono_ms": 1, "transcript": "hello"})},
		{"spoofed enrichment field", turnEventPayload("conversation.started", "", 1, map[string]any{"mono_ms": 1, "integration": "livekit-python", "integration_version": "0.1.0", "workload_id": "victim"})},
		{"bad end reason", turnEventPayload("conversation.ended", "", 1, map[string]any{"mono_ms": 1, "reason": "boredom", "turn_count": 1})},
		{"bad initiator", turnEventPayload("turn.started", "turn_000001", 1, map[string]any{"mono_ms": 1, "initiator": "ghost"})},
		{"zero tool index", turnEventPayload("tool.started", "turn_000001", 1, map[string]any{"mono_ms": 1, "tool_index": 0})},
		{"non-boolean ok", turnEventPayload("llm.completed", "turn_000001", 1, map[string]any{"mono_ms": 1, "ok": "yes"})},
		{"llm leg with session id", turnEventPayload("leg.attached", "turn_000001", 1, map[string]any{"mono_ms": 1, "kind": "llm", "request_id": "req-1", "session_id": "sess-1"})},
		{"stt leg missing attempt id", turnEventPayload("leg.attached", "turn_000001", 1, map[string]any{"mono_ms": 1, "kind": "stt", "session_id": "sess-1"})},
		{"leg with oversized identifier", turnEventPayload("leg.attached", "turn_000001", 1, map[string]any{"mono_ms": 1, "kind": "llm", "request_id": strings.Repeat("r", 257)})},
	}
	sink := &fakeTelemetrySink{}
	httpServer := newTurnEventServer(t, sink, gateway.TurnEventDestinations{AnonymousEndpoint: "https://telemetry.speko.test/v1/anonymous-turn-events"}, nil)
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			// The valid trailing event proves the invalid one aborts the batch.
			response := postTurnEvents(t, httpServer.URL, "local-token", []map[string]any{test.event, valid})
			defer response.Body.Close()
			if response.StatusCode != http.StatusBadRequest {
				body, _ := io.ReadAll(response.Body)
				t.Fatalf("status = %d body = %s", response.StatusCode, body)
			}
		})
	}
	if len(sink.recorded()) != 0 {
		t.Fatal("rejected batches must not reach the telemetry sink")
	}

	t.Run("empty batch", func(t *testing.T) {
		response := postTurnEvents(t, httpServer.URL, "local-token", []map[string]any{})
		defer response.Body.Close()
		if response.StatusCode != http.StatusBadRequest {
			t.Fatalf("empty batch status = %d", response.StatusCode)
		}
	})
	t.Run("oversize batch", func(t *testing.T) {
		events := make([]map[string]any, 0, 65)
		for index := 0; index < 65; index++ {
			events = append(events, turnEventPayload("turn.completed", "turn_000001", uint64(index+1), map[string]any{"mono_ms": index}))
		}
		response := postTurnEvents(t, httpServer.URL, "local-token", events)
		defer response.Body.Close()
		if response.StatusCode != http.StatusBadRequest {
			t.Fatalf("oversize batch status = %d", response.StatusCode)
		}
	})
	t.Run("unknown envelope field", func(t *testing.T) {
		response := postJSON(t, httpServer.URL+"/v1/turn-events", map[string]any{"events": []map[string]any{valid}, "metadata": "nope"}, "local-token", "")
		defer response.Body.Close()
		if response.StatusCode != http.StatusBadRequest {
			t.Fatalf("unknown envelope field status = %d", response.StatusCode)
		}
	})
}

func TestTurnEventsMapToTelemetryEvents(t *testing.T) {
	t.Parallel()
	sink := &fakeTelemetrySink{}
	destinations := gateway.TurnEventDestinations{
		AuthenticatedEndpoint: "https://control.speko.test/v1/turn-events",
		AuthenticatedToken:    "api-key-secret",
		AnonymousEndpoint:     "https://telemetry.speko.test/v1/anonymous-turn-events",
	}
	httpServer := newTurnEventServer(t, sink, destinations, &protocol.Workload{Type: "agent", ID: "agent-7"})

	events := []map[string]any{
		turnEventPayload("conversation.started", "", 1, map[string]any{"mono_ms": 0, "integration": "livekit-python", "integration_version": "0.1.0"}),
		turnEventPayload("turn.started", "turn_000001", 2, map[string]any{"mono_ms": 12, "initiator": "user"}),
		turnEventPayload("user.speech.ended", "turn_000001", 3, map[string]any{"mono_ms": 812}),
		turnEventPayload("leg.attached", "turn_000001", 4, map[string]any{"mono_ms": 813, "kind": "stt", "session_id": "sess-1", "attempt_id": "att-1", "provider": "deepgram"}),
		turnEventPayload("leg.attached", "turn_000001", 5, map[string]any{"mono_ms": 814, "kind": "llm", "request_id": "req-9"}),
		turnEventPayload("llm.completed", "turn_000001", 6, map[string]any{"mono_ms": 1200, "ok": true}),
		turnEventPayload("playback.stopped", "turn_000001", 7, map[string]any{"mono_ms": 2400, "interrupted": false, "playback_position_ms": 1500}),
		turnEventPayload("turn.completed", "turn_000001", 8, map[string]any{"mono_ms": 2401}),
		turnEventPayload("conversation.ended", "", 9, map[string]any{"mono_ms": 2500, "reason": "hangup", "turn_count": 1}),
	}
	response := postTurnEvents(t, httpServer.URL, "local-token", events)
	if response.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("status = %d body = %s", response.StatusCode, body)
	}
	accepted, dropped := decodeTurnEventResponse(t, response)
	if accepted != len(events) || dropped != 0 {
		t.Fatalf("accepted=%d dropped=%d", accepted, dropped)
	}

	recorded := sink.recorded()
	if len(recorded) != len(events) {
		t.Fatalf("sink recorded %d events, want %d", len(recorded), len(events))
	}
	first := recorded[0]
	if first.Name != "conversation.started" || first.SessionID != testConversationID || first.AttemptID != "" {
		t.Fatalf("conversation.started envelope = %+v", first)
	}
	if first.EventID != "tev_"+testConversationID+"_1" {
		t.Fatalf("event id = %q", first.EventID)
	}
	if !first.At.Equal(time.UnixMilli(1_765_432_100_123)) {
		t.Fatalf("event at = %v", first.At)
	}
	if first.Required {
		t.Fatal("turn events must never be Required")
	}
	if first.Destination.Endpoint != destinations.AuthenticatedEndpoint || first.Destination.Token != destinations.AuthenticatedToken {
		t.Fatalf("destination = %+v", first.Destination)
	}
	var startedData map[string]any
	if err := json.Unmarshal(first.Data, &startedData); err != nil {
		t.Fatalf("decode enriched data: %v", err)
	}
	expectedStarted := map[string]any{
		"seq": float64(1), "mono_ms": float64(0),
		"integration": "livekit-python", "integration_version": "0.1.0",
		"workload_type": "agent", "workload_id": "agent-7",
		"instance_id": "gateway-test", "gateway_version": "test",
	}
	for field, want := range expectedStarted {
		if startedData[field] != want {
			t.Fatalf("conversation.started data[%s] = %v, want %v", field, startedData[field], want)
		}
	}
	if len(startedData) != len(expectedStarted) {
		t.Fatalf("conversation.started data = %v", startedData)
	}

	turnScoped := recorded[1]
	if turnScoped.Name != "turn.started" || turnScoped.AttemptID != "turn_000001" || turnScoped.EventID != "tev_"+testConversationID+"_2" {
		t.Fatalf("turn.started envelope = %+v", turnScoped)
	}
	var turnData map[string]any
	if err := json.Unmarshal(turnScoped.Data, &turnData); err != nil {
		t.Fatalf("decode turn data: %v", err)
	}
	if turnData["seq"] != float64(2) || turnData["initiator"] != "user" || turnData["mono_ms"] != float64(12) {
		t.Fatalf("turn.started data = %v", turnData)
	}
}

func TestTurnEventsWithoutAPIKeyUseAnonymousDestination(t *testing.T) {
	t.Parallel()
	sink := &fakeTelemetrySink{}
	destinations := gateway.TurnEventDestinations{AnonymousEndpoint: "https://telemetry.speko.test/v1/anonymous-turn-events"}
	httpServer := newTurnEventServer(t, sink, destinations, nil)

	response := postTurnEvents(t, httpServer.URL, "local-token", []map[string]any{
		turnEventPayload("conversation.started", "", 1, map[string]any{"mono_ms": 0, "integration": "livekit-python", "integration_version": "0.1.0"}),
	})
	accepted, dropped := decodeTurnEventResponse(t, response)
	if accepted != 1 || dropped != 0 {
		t.Fatalf("accepted=%d dropped=%d", accepted, dropped)
	}
	recorded := sink.recorded()
	if len(recorded) != 1 {
		t.Fatalf("sink recorded %d events", len(recorded))
	}
	if recorded[0].Destination.Endpoint != destinations.AnonymousEndpoint || recorded[0].Destination.Token != "" {
		t.Fatalf("anonymous destination = %+v", recorded[0].Destination)
	}
	// Without a configured workload only the instance identity is enriched.
	var data map[string]any
	if err := json.Unmarshal(recorded[0].Data, &data); err != nil {
		t.Fatalf("decode data: %v", err)
	}
	if _, present := data["workload_id"]; present {
		t.Fatalf("workload enrichment without workload config: %v", data)
	}
	if data["instance_id"] != "gateway-test" || data["gateway_version"] != "test" {
		t.Fatalf("instance enrichment = %v", data)
	}
}

func TestTurnEventsCountSinkRejectionsAsDropped(t *testing.T) {
	t.Parallel()
	sink := &fakeTelemetrySink{full: true}
	httpServer := newTurnEventServer(t, sink, gateway.TurnEventDestinations{AnonymousEndpoint: "https://telemetry.speko.test/v1/anonymous-turn-events"}, nil)

	response := postTurnEvents(t, httpServer.URL, "local-token", []map[string]any{
		turnEventPayload("turn.completed", "turn_000001", 1, map[string]any{"mono_ms": 10}),
		turnEventPayload("turn.completed", "turn_000002", 2, map[string]any{"mono_ms": 20}),
	})
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d", response.StatusCode)
	}
	accepted, dropped := decodeTurnEventResponse(t, response)
	if accepted != 0 || dropped != 2 {
		t.Fatalf("accepted=%d dropped=%d", accepted, dropped)
	}
}

// TestTurnEventsSuppressedWhenOptionalTelemetryDisabled pins the privacy
// contract: SPEKO_TELEMETRY_DISABLED suppresses the profiler entirely because
// no turn event is ever Required.
func TestTurnEventsSuppressedWhenOptionalTelemetryDisabled(t *testing.T) {
	t.Parallel()
	receiver := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		t.Errorf("suppressed telemetry reached %s", request.URL.Path)
		writer.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(receiver.Close)
	exporter, err := runtimepkg.NewTelemetryExporter(runtimepkg.TelemetryExporterConfig{
		AnonymousEndpoint:        receiver.URL + "/v1/anonymous-turn-events",
		DisableOptionalTelemetry: true,
	})
	if err != nil {
		t.Fatalf("new exporter: %v", err)
	}
	httpServer := newTurnEventServer(t, exporter, gateway.TurnEventDestinations{AnonymousEndpoint: receiver.URL + "/v1/anonymous-turn-events"}, nil)

	response := postTurnEvents(t, httpServer.URL, "local-token", []map[string]any{
		turnEventPayload("turn.completed", "turn_000001", 1, map[string]any{"mono_ms": 10}),
	})
	accepted, dropped := decodeTurnEventResponse(t, response)
	if accepted != 1 || dropped != 0 {
		t.Fatalf("accepted=%d dropped=%d", accepted, dropped)
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := exporter.Close(closeCtx); err != nil {
		t.Fatalf("close exporter: %v", err)
	}
	stats := exporter.Stats()
	if stats.Suppressed != 1 || stats.Exported != 0 {
		t.Fatalf("exporter stats = %+v", stats)
	}
}

// TestTurnEventsExportThroughTelemetryExporter proves the route needs no
// exporter changes: markers ride the existing batch wire format with their
// names intact.
func TestTurnEventsExportThroughTelemetryExporter(t *testing.T) {
	t.Parallel()
	type wireBatch struct {
		Events []struct {
			Type        string          `json:"type"`
			EventID     string          `json:"event_id"`
			SessionID   string          `json:"session_id"`
			AttemptID   string          `json:"attempt_id"`
			CreatedAtMS int64           `json:"created_at_ms"`
			Data        json.RawMessage `json:"data"`
		} `json:"events"`
	}
	received := make(chan wireBatch, 1)
	var authorization string
	receiver := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var batch wireBatch
		if err := json.NewDecoder(request.Body).Decode(&batch); err != nil {
			t.Errorf("decode exported batch: %v", err)
		}
		authorization = request.Header.Get("Authorization")
		received <- batch
		writer.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(receiver.Close)
	exporter, err := runtimepkg.NewTelemetryExporter(runtimepkg.TelemetryExporterConfig{})
	if err != nil {
		t.Fatalf("new exporter: %v", err)
	}
	destinations := gateway.TurnEventDestinations{
		AuthenticatedEndpoint: receiver.URL + "/v1/turn-events",
		AuthenticatedToken:    "api-key-secret",
	}
	httpServer := newTurnEventServer(t, exporter, destinations, nil)

	response := postTurnEvents(t, httpServer.URL, "local-token", []map[string]any{
		turnEventPayload("user.speech.ended", "turn_000001", 17, map[string]any{"mono_ms": 48211}),
	})
	accepted, dropped := decodeTurnEventResponse(t, response)
	if accepted != 1 || dropped != 0 {
		t.Fatalf("accepted=%d dropped=%d", accepted, dropped)
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := exporter.Close(closeCtx); err != nil {
		t.Fatalf("close exporter: %v", err)
	}
	batch := <-received
	if authorization != "Bearer api-key-secret" {
		t.Fatalf("authorization = %q", authorization)
	}
	if len(batch.Events) != 1 {
		t.Fatalf("exported %d events", len(batch.Events))
	}
	exported := batch.Events[0]
	if exported.Type != "user.speech.ended" || exported.SessionID != testConversationID || exported.AttemptID != "turn_000001" {
		t.Fatalf("exported envelope = %+v", exported)
	}
	if exported.EventID != "tev_"+testConversationID+"_17" || exported.CreatedAtMS != 1_765_432_100_123 {
		t.Fatalf("exported identity = %+v", exported)
	}
	var data map[string]any
	if err := json.Unmarshal(exported.Data, &data); err != nil {
		t.Fatalf("decode exported data: %v", err)
	}
	if data["seq"] != float64(17) || data["mono_ms"] != float64(48211) {
		t.Fatalf("exported data = %v", data)
	}
}
