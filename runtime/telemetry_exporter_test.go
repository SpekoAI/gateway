package runtime_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

type telemetryCapture struct {
	mu       sync.Mutex
	requests []capturedTelemetryRequest
	failures int
}

type capturedTelemetryRequest struct {
	path          string
	authorization string
	events        []protocol.Event
}

func (c *telemetryCapture) handler(t *testing.T) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		var batch struct {
			Events []protocol.Event `json:"events"`
		}
		if err := json.NewDecoder(request.Body).Decode(&batch); err != nil {
			t.Errorf("decode telemetry batch: %v", err)
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.failures > 0 {
			c.failures--
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		c.requests = append(c.requests, capturedTelemetryRequest{
			path:          request.URL.Path,
			authorization: request.Header.Get("Authorization"),
			events:        batch.Events,
		})
		writer.WriteHeader(http.StatusAccepted)
	}
}

func (c *telemetryCapture) snapshot() []capturedTelemetryRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]capturedTelemetryRequest(nil), c.requests...)
}

func telemetryTestEvent(name, attemptID, endpoint, token string, data json.RawMessage) runtimepkg.TelemetryEvent {
	return runtimepkg.TelemetryEvent{
		Name: name, SessionID: "sess-1", AttemptID: attemptID, At: time.Date(2026, time.August, 1, 12, 0, 0, 0, time.UTC),
		EventID:     "tel_" + attemptID + "_" + name,
		Destination: protocol.Telemetry{Endpoint: endpoint, Token: token, FlushIntervalMS: 1_000},
		Data:        data,
	}
}

func waitForRequests(t *testing.T, capture *telemetryCapture, want int) []capturedTelemetryRequest {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if requests := capture.snapshot(); len(requests) >= want {
			return requests
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("telemetry requests = %d, want at least %d", len(capture.snapshot()), want)
	return nil
}

func TestTelemetryExporterBatchesByDestinationAndMapsTerminalFailure(t *testing.T) {
	t.Parallel()
	capture := &telemetryCapture{}
	server := httptest.NewServer(capture.handler(t))
	t.Cleanup(server.Close)
	exporter, err := runtimepkg.NewTelemetryExporter(runtimepkg.TelemetryExporterConfig{})
	if err != nil {
		t.Fatalf("new exporter: %v", err)
	}
	events := []runtimepkg.TelemetryEvent{
		telemetryTestEvent("session.opened", "att-1", server.URL+"/a/v1/runtime-events", "token-a", json.RawMessage(`{"provider_open_ms":42}`)),
		telemetryTestEvent("usage.observed", "att-1", server.URL+"/a/v1/runtime-events", "token-a", json.RawMessage(`{"provider_request_id":"dg-1"}`)),
		telemetryTestEvent("session.failed", "att-2", server.URL+"/b/v1/runtime-events", "token-b", json.RawMessage(`{"code":"provider_unavailable","terminal":true}`)),
	}
	for _, event := range events {
		if !exporter.TryRecord(event) {
			t.Fatalf("TryRecord(%s) = false", event.Name)
		}
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := exporter.Close(closeCtx); err != nil {
		t.Fatalf("close exporter: %v", err)
	}
	requests := waitForRequests(t, capture, 2)
	byPath := map[string]capturedTelemetryRequest{}
	for _, request := range requests {
		byPath[request.path] = request
	}
	destinationA, ok := byPath["/a/v1/runtime-events"]
	if !ok || destinationA.authorization != "Bearer token-a" || len(destinationA.events) != 2 {
		t.Fatalf("destination A batch = %+v", destinationA)
	}
	if destinationA.events[1].Type != protocol.EventUsageObserved || string(destinationA.events[1].Data) != `{"provider_request_id":"dg-1"}` {
		t.Fatalf("usage event = %+v", destinationA.events[1])
	}
	destinationB, ok := byPath["/b/v1/runtime-events"]
	if !ok || destinationB.authorization != "Bearer token-b" || len(destinationB.events) != 1 {
		t.Fatalf("destination B batch = %+v", destinationB)
	}
	if destinationB.events[0].Type != protocol.EventError || destinationB.events[0].EventID != "tel_att-2_session.failed" {
		t.Fatalf("failed session must export as terminal error event, got %+v", destinationB.events[0])
	}
	stats := exporter.Stats()
	if stats.Exported != 3 || stats.Dropped != 0 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestTelemetryExporterRetriesWithinBoundsAndCounts(t *testing.T) {
	t.Parallel()
	capture := &telemetryCapture{failures: 2}
	server := httptest.NewServer(capture.handler(t))
	t.Cleanup(server.Close)
	exporter, err := runtimepkg.NewTelemetryExporter(runtimepkg.TelemetryExporterConfig{
		MaxAttempts: 3, RetryBackoff: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new exporter: %v", err)
	}
	if !exporter.TryRecord(telemetryTestEvent("session.closed", "att-1", server.URL, "token", nil)) {
		t.Fatal("TryRecord = false")
	}
	requests := waitForRequests(t, capture, 1)
	if len(requests[0].events) != 1 || requests[0].events[0].Type != protocol.EventSessionClosed {
		t.Fatalf("delivered batch = %+v", requests[0])
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := exporter.Close(closeCtx); err != nil {
		t.Fatalf("close exporter: %v", err)
	}
	stats := exporter.Stats()
	if stats.Retried != 2 || stats.Exported != 1 || stats.FailedBatches != 0 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestTelemetryExporterDropsAfterRetryBudgetIsExhausted(t *testing.T) {
	t.Parallel()
	capture := &telemetryCapture{failures: 100}
	server := httptest.NewServer(capture.handler(t))
	t.Cleanup(server.Close)
	exporter, err := runtimepkg.NewTelemetryExporter(runtimepkg.TelemetryExporterConfig{
		MaxAttempts: 2, RetryBackoff: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new exporter: %v", err)
	}
	if !exporter.TryRecord(telemetryTestEvent("session.closed", "att-1", server.URL, "token", nil)) {
		t.Fatal("TryRecord = false")
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := exporter.Close(closeCtx); err != nil {
		t.Fatalf("close exporter: %v", err)
	}
	stats := exporter.Stats()
	if stats.Dropped != 1 || stats.FailedBatches != 1 || stats.Exported != 0 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestTelemetryExporterNeverBlocksWhenQueueIsFull(t *testing.T) {
	t.Parallel()
	// An unroutable destination keeps the queue from draining meaningfully;
	// the small queue must reject overflow immediately instead of blocking.
	exporter, err := runtimepkg.NewTelemetryExporter(runtimepkg.TelemetryExporterConfig{
		QueueSize: 2, RequestTimeout: 50 * time.Millisecond, RetryBackoff: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new exporter: %v", err)
	}
	accepted, rejected := 0, 0
	start := time.Now()
	for i := 0; i < 64; i++ {
		if exporter.TryRecord(telemetryTestEvent("session.opened", "att-1", "https://unroutable.speko.invalid/v1/runtime-events", "token", nil)) {
			accepted++
		} else {
			rejected++
		}
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("TryRecord loop took %s; it must never block", elapsed)
	}
	if rejected == 0 {
		t.Fatalf("accepted=%d rejected=%d; the bounded queue must shed load", accepted, rejected)
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := exporter.Close(closeCtx); err != nil {
		t.Fatalf("close exporter: %v", err)
	}
	if stats := exporter.Stats(); stats.Dropped == 0 {
		t.Fatalf("stats = %+v, want dropped > 0", stats)
	}
}

func TestTelemetryExporterDropsEventsWithoutADestination(t *testing.T) {
	t.Parallel()
	exporter, err := runtimepkg.NewTelemetryExporter(runtimepkg.TelemetryExporterConfig{})
	if err != nil {
		t.Fatalf("new exporter: %v", err)
	}
	event := runtimepkg.TelemetryEvent{Name: "session.opened", SessionID: "sess-1", AttemptID: "att-1", At: time.Now()}
	if !exporter.TryRecord(event) {
		t.Fatal("TryRecord = false; the queue itself had room")
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := exporter.Close(closeCtx); err != nil {
		t.Fatalf("close exporter: %v", err)
	}
	if stats := exporter.Stats(); stats.Dropped != 1 || stats.Exported != 0 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestTelemetryOptOutSuppressesOptionalEventsButKeepsManagedMetering(t *testing.T) {
	t.Parallel()
	capture := &telemetryCapture{}
	server := httptest.NewServer(capture.handler(t))
	t.Cleanup(server.Close)
	exporter, err := runtimepkg.NewTelemetryExporter(runtimepkg.TelemetryExporterConfig{DisableOptionalTelemetry: true})
	if err != nil {
		t.Fatalf("new exporter: %v", err)
	}
	optional := telemetryTestEvent("agent.event", "att-1", server.URL, "token", json.RawMessage(`{"event_type":"speech.started"}`))
	required := telemetryTestEvent("usage.observed", "att-1", server.URL, "token", json.RawMessage(`{"provider_request_id":"provider-1"}`))
	required.Required = true
	if !exporter.TryRecord(optional) || !exporter.TryRecord(required) {
		t.Fatal("telemetry enqueue failed")
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := exporter.Close(closeCtx); err != nil {
		t.Fatalf("close exporter: %v", err)
	}
	requests := waitForRequests(t, capture, 1)
	if len(requests[0].events) != 1 || requests[0].events[0].Type != protocol.EventUsageObserved {
		t.Fatalf("opt-out batch = %+v", requests[0].events)
	}
	if stats := exporter.Stats(); stats.Suppressed != 1 || stats.Exported != 1 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestTelemetryExporterUsesAnonymousDefaultDestination(t *testing.T) {
	t.Parallel()
	capture := &telemetryCapture{}
	server := httptest.NewServer(capture.handler(t))
	t.Cleanup(server.Close)
	exporter, err := runtimepkg.NewTelemetryExporter(runtimepkg.TelemetryExporterConfig{
		AnonymousEndpoint: server.URL,
	})
	if err != nil {
		t.Fatalf("new exporter: %v", err)
	}
	event := telemetryTestEvent("session.opened", "att-anonymous", "", "", json.RawMessage(`{"provider_open_ms":12}`))
	if !exporter.TryRecord(event) {
		t.Fatal("anonymous telemetry enqueue failed")
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := exporter.Close(closeCtx); err != nil {
		t.Fatalf("close exporter: %v", err)
	}
	requests := waitForRequests(t, capture, 1)
	if requests[0].authorization != "" || len(requests[0].events) != 1 {
		t.Fatalf("anonymous telemetry request = %+v", requests[0])
	}
}

func TestTelemetryExporterFlushesOnTheDestinationInterval(t *testing.T) {
	t.Parallel()
	capture := &telemetryCapture{}
	server := httptest.NewServer(capture.handler(t))
	t.Cleanup(server.Close)
	exporter, err := runtimepkg.NewTelemetryExporter(runtimepkg.TelemetryExporterConfig{})
	if err != nil {
		t.Fatalf("new exporter: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = exporter.Close(closeCtx)
	})
	if !exporter.TryRecord(telemetryTestEvent("session.opened", "att-1", server.URL, "token", nil)) {
		t.Fatal("TryRecord = false")
	}
	// FlushIntervalMS is 1000: the batch must arrive without Close being called.
	requests := waitForRequests(t, capture, 1)
	if !strings.HasPrefix(requests[0].authorization, "Bearer ") {
		t.Fatalf("authorization = %q", requests[0].authorization)
	}
}
