package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SpekoAI/gateway/protocol"
)

const (
	defaultTelemetryQueueSize      = 1024
	defaultTelemetryMaxBatch       = 64
	defaultTelemetryMaxAttempts    = 3
	defaultTelemetryRetryBackoff   = time.Second
	defaultTelemetryRequestTimeout = 5 * time.Second
	defaultTelemetryFlushInterval  = 5 * time.Second
	telemetryFlushQueueSize        = 8
	telemetryTickInterval          = 250 * time.Millisecond
)

// TelemetryExporterConfig bounds every exporter resource explicitly. All
// fields are optional; zero values receive conservative defaults.
type TelemetryExporterConfig struct {
	HTTPClient               *http.Client
	QueueSize                int
	MaxBatchEvents           int
	MaxAttempts              int
	RetryBackoff             time.Duration
	RequestTimeout           time.Duration
	UserAgent                string
	Now                      func() time.Time
	AnonymousEndpoint        string
	DisableOptionalTelemetry bool
}

// TelemetryExporterStats are monotonic process counters. They deliberately
// carry no session, tenant, or credential detail.
type TelemetryExporterStats struct {
	Enqueued      uint64
	Exported      uint64
	Dropped       uint64
	Retried       uint64
	FailedBatches uint64
	Suppressed    uint64
}

// TelemetryExporter is the production TelemetrySink: TryRecord never performs
// I/O, events batch per plan-carried or anonymous default destination, and
// delivery retries within bounded attempts and memory. Deterministic event IDs
// make at-least-once delivery safe against idempotent ingest; a full queue or
// exhausted retry drops events and counts them rather than blocking a session.
type TelemetryExporter struct {
	config TelemetryExporterConfig

	queue chan TelemetryEvent
	flush chan telemetryBatch
	stop  chan struct{}
	done  chan struct{}

	closeOnce sync.Once
	closed    atomic.Bool

	enqueued      atomic.Uint64
	exported      atomic.Uint64
	dropped       atomic.Uint64
	retried       atomic.Uint64
	failedBatches atomic.Uint64
	suppressed    atomic.Uint64
}

type telemetryBatch struct {
	endpoint string
	token    string
	events   []TelemetryEvent
}

type pendingTelemetry struct {
	batch telemetryBatch
	dueAt time.Time
}

// NewTelemetryExporter starts the exporter's background batching and delivery
// goroutines. Callers own its lifetime and must call Close during shutdown to
// flush buffered events.
func NewTelemetryExporter(config TelemetryExporterConfig) (*TelemetryExporter, error) {
	if config.QueueSize == 0 {
		config.QueueSize = defaultTelemetryQueueSize
	}
	if config.MaxBatchEvents == 0 {
		config.MaxBatchEvents = defaultTelemetryMaxBatch
	}
	if config.MaxAttempts == 0 {
		config.MaxAttempts = defaultTelemetryMaxAttempts
	}
	if config.RetryBackoff == 0 {
		config.RetryBackoff = defaultTelemetryRetryBackoff
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = defaultTelemetryRequestTimeout
	}
	if config.QueueSize < 1 || config.MaxBatchEvents < 1 || config.MaxAttempts < 1 || config.RetryBackoff <= 0 || config.RequestTimeout <= 0 {
		return nil, errors.New("runtime: telemetry exporter bounds must be positive")
	}
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{}
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	exporter := &TelemetryExporter{
		config: config,
		queue:  make(chan TelemetryEvent, config.QueueSize),
		flush:  make(chan telemetryBatch, telemetryFlushQueueSize),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	go exporter.run()
	return exporter, nil
}

// TryRecord enqueues one event without blocking. It returns false when the
// exporter is shut down or its bounded queue is full.
func (e *TelemetryExporter) TryRecord(event TelemetryEvent) bool {
	if e.config.DisableOptionalTelemetry && !event.Required {
		e.suppressed.Add(1)
		return true
	}
	if event.Destination.Endpoint == "" {
		event.Destination.Endpoint = e.config.AnonymousEndpoint
	}
	if e.closed.Load() {
		return false
	}
	select {
	case e.queue <- event:
		e.enqueued.Add(1)
		return true
	default:
		e.dropped.Add(1)
		return false
	}
}

// Close stops intake, flushes buffered events, and waits for delivery to
// finish or ctx to expire. In-flight requests still respect their own bounded
// timeout after ctx expires.
func (e *TelemetryExporter) Close(ctx context.Context) error {
	e.closeOnce.Do(func() {
		e.closed.Store(true)
		close(e.stop)
	})
	select {
	case <-e.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stats returns a point-in-time snapshot of the drop, retry, and delivery
// counters.
func (e *TelemetryExporter) Stats() TelemetryExporterStats {
	return TelemetryExporterStats{
		Enqueued:      e.enqueued.Load(),
		Exported:      e.exported.Load(),
		Dropped:       e.dropped.Load(),
		Retried:       e.retried.Load(),
		FailedBatches: e.failedBatches.Load(),
		Suppressed:    e.suppressed.Load(),
	}
}

func (e *TelemetryExporter) run() {
	defer close(e.done)
	var flusher sync.WaitGroup
	flusher.Add(1)
	go func() {
		defer flusher.Done()
		for batch := range e.flush {
			e.send(batch)
		}
	}()

	pending := make(map[string]*pendingTelemetry)
	ticker := time.NewTicker(telemetryTickInterval)
	defer ticker.Stop()
	for {
		select {
		case event := <-e.queue:
			e.buffer(pending, event)
		case <-ticker.C:
			now := e.config.Now()
			for key, entry := range pending {
				if !entry.dueAt.After(now) {
					e.dispatch(entry.batch)
					delete(pending, key)
				}
			}
		case <-e.stop:
			// Drain whatever intake raced shutdown, flush every buffered
			// batch, then stop the flusher and wait for its last delivery.
			for {
				select {
				case event := <-e.queue:
					e.buffer(pending, event)
					continue
				default:
				}
				break
			}
			for _, entry := range pending {
				e.dispatch(entry.batch)
			}
			close(e.flush)
			flusher.Wait()
			return
		}
	}
}

func (e *TelemetryExporter) buffer(pending map[string]*pendingTelemetry, event TelemetryEvent) {
	if event.Destination.Endpoint == "" {
		e.dropped.Add(1)
		return
	}
	key := event.Destination.Endpoint + "\x00" + event.Destination.Token
	entry, exists := pending[key]
	if !exists {
		entry = &pendingTelemetry{
			batch: telemetryBatch{endpoint: event.Destination.Endpoint, token: event.Destination.Token},
			dueAt: e.config.Now().Add(flushInterval(event.Destination.FlushIntervalMS)),
		}
		pending[key] = entry
	}
	entry.batch.events = append(entry.batch.events, event)
	if len(entry.batch.events) >= e.config.MaxBatchEvents {
		e.dispatch(entry.batch)
		delete(pending, key)
	}
}

// dispatch hands a batch to the delivery goroutine without blocking the
// batching loop. A saturated delivery queue sheds the whole batch: dropped
// telemetry is always preferred over a stalled or unbounded exporter.
func (e *TelemetryExporter) dispatch(batch telemetryBatch) {
	if len(batch.events) == 0 {
		return
	}
	select {
	case e.flush <- batch:
	default:
		e.dropped.Add(uint64(len(batch.events)))
		e.failedBatches.Add(1)
	}
}

func (e *TelemetryExporter) send(batch telemetryBatch) {
	body, err := json.Marshal(map[string]any{"events": wireTelemetryEvents(batch.events)})
	if err != nil {
		e.dropped.Add(uint64(len(batch.events)))
		e.failedBatches.Add(1)
		return
	}
	for attempt := 1; ; attempt++ {
		if e.postBatch(batch.endpoint, batch.token, body) == nil {
			e.exported.Add(uint64(len(batch.events)))
			return
		}
		if attempt >= e.config.MaxAttempts {
			e.dropped.Add(uint64(len(batch.events)))
			e.failedBatches.Add(1)
			return
		}
		e.retried.Add(1)
		select {
		case <-time.After(e.config.RetryBackoff * time.Duration(attempt)):
		case <-e.stop:
			// One immediate final attempt during shutdown instead of backoff.
			if e.postBatch(batch.endpoint, batch.token, body) == nil {
				e.exported.Add(uint64(len(batch.events)))
				return
			}
			e.dropped.Add(uint64(len(batch.events)))
			e.failedBatches.Add(1)
			return
		}
	}
}

func (e *TelemetryExporter) postBatch(endpoint, token string, body []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), e.config.RequestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("runtime: create telemetry request: %w", err)
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	request.Header.Set("Content-Type", "application/json")
	if e.config.UserAgent != "" {
		request.Header.Set("User-Agent", e.config.UserAgent)
	}
	response, err := e.config.HTTPClient.Do(request)
	if err != nil {
		// The transport error is intentionally not retained or logged: the
		// URL may embed the deployment's internal topology and errors count
		// toward Stats instead.
		return errors.New("runtime: telemetry request failed")
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("runtime: telemetry request rejected with HTTP %d", response.StatusCode)
	}
	return nil
}

// wireTelemetryEvents maps internal lifecycle names onto the public
// control-plane event contract.
func wireTelemetryEvents(events []TelemetryEvent) []protocol.Event {
	wire := make([]protocol.Event, 0, len(events))
	for _, event := range events {
		name := event.Name
		if name == "session.failed" {
			name = string(protocol.EventError)
		}
		eventID := event.EventID
		if eventID == "" {
			eventID = telemetryEventID(event.AttemptID, event.Name, event.Data)
		}
		wire = append(wire, protocol.Event{
			Type:        protocol.EventType(name),
			EventID:     eventID,
			SessionID:   event.SessionID,
			AttemptID:   event.AttemptID,
			CreatedAtMS: event.At.UnixMilli(),
			Data:        event.Data,
		})
	}
	return wire
}

func flushInterval(hintMS int) time.Duration {
	if hintMS < 1_000 || hintMS > 60_000 {
		return defaultTelemetryFlushInterval
	}
	return time.Duration(hintMS) * time.Millisecond
}
