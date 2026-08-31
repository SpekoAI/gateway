package gateway_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/controlplane"
	"github.com/SpekoAI/gateway/gateway"
	"github.com/SpekoAI/gateway/protocol"
	"github.com/SpekoAI/gateway/providers/mock"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

var gatewayNow = time.Date(2026, time.August, 1, 12, 0, 0, 0, time.UTC)

func TestGatewayCreatesAuthenticatedCanonicalWebSocketSession(t *testing.T) {
	t.Parallel()
	gatewayServer, plans := newServer(t)
	httpServer := httptest.NewServer(gatewayServer.Handler())
	t.Cleanup(httpServer.Close)

	requestBody := map[string]any{
		"kind":      "stt",
		"execution": map[string]string{"provider_route": "provider_direct", "credential_source": "byok", "relay_policy": "forbidden"},
		"request":   map[string]string{"model": "mock-model"},
		"media":     map[string]any{"encoding": "pcm_s16le", "sample_rate_hz": 16000, "channels": 1},
	}
	response := postJSON(t, httpServer.URL+"/v1/sessions", requestBody, "local-token", "local-create-1")
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("create status = %d: %s", response.StatusCode, body)
	}
	var created struct {
		SessionID string `json:"session_id"`
		AttemptID string `json:"attempt_id"`
		StreamURL string `json:"stream_url"`
		Route     struct {
			Provider string `json:"provider"`
		} `json:"route"`
	}
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if created.SessionID != "session-gateway" || created.AttemptID != "attempt-gateway" || created.Route.Provider != "mock" {
		t.Fatalf("create response = %+v", created)
	}
	if strings.Contains(created.StreamURL, "temporary") {
		t.Fatalf("create response leaked provider credential: %q", created.StreamURL)
	}
	if calls := plans.calls(); len(calls) != 1 || calls[0].Runtime.Placement != protocol.PlacementSidecar || calls[0].Runtime.InstanceID != "gateway-test" {
		t.Fatalf("plan requests = %+v", calls)
	}

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + created.StreamURL
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer local-token")
	connection, _, err := websocket.Dial(context.Background(), wsURL, &websocket.DialOptions{HTTPHeader: headers, Subprotocols: []string{gateway.WebSocketSubprotocol}})
	if err != nil {
		t.Fatalf("dial stream: %v", err)
	}
	defer connection.Close(websocket.StatusNormalClosure, "")
	if got := connection.Subprotocol(); got != gateway.WebSocketSubprotocol {
		t.Fatalf("subprotocol = %q", got)
	}
	expectEvent(t, connection, protocol.EventSessionReady)
	if err := connection.Write(context.Background(), websocket.MessageBinary, []byte{1, 2, 3}); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	if err := writeCommand(connection, "audio.commit", nil); err != nil {
		t.Fatalf("commit audio: %v", err)
	}
	expectEvent(t, connection, protocol.EventSpeechStarted)
	expectEvent(t, connection, protocol.EventTranscriptDelta)
	expectEvent(t, connection, protocol.EventSpeechEnded)
	expectEvent(t, connection, protocol.EventTranscriptFinal)
	second := postJSON(t, httpServer.URL+"/v1/sessions", requestBody, "local-token", "local-create-1")
	defer second.Body.Close()
	if second.StatusCode != http.StatusOK || len(plans.calls()) != 1 {
		t.Fatalf("active idempotent retry status=%d calls=%d", second.StatusCode, len(plans.calls()))
	}
	if err := writeCommand(connection, "session.close", nil); err != nil {
		t.Fatalf("close session: %v", err)
	}
	expectEvent(t, connection, protocol.EventSessionClosed)
}

func TestGatewayEnforcesLocalAuthAndDrainReadiness(t *testing.T) {
	t.Parallel()
	gatewayServer, _ := newServer(t)
	httpServer := httptest.NewServer(gatewayServer.Handler())
	t.Cleanup(httpServer.Close)

	response := postJSON(t, httpServer.URL+"/v1/sessions", map[string]any{}, "", "missing-auth")
	defer response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated create status = %d", response.StatusCode)
	}
	metrics := postGet(t, httpServer.URL+"/metrics", "local-token")
	defer metrics.Body.Close()
	body, _ := io.ReadAll(metrics.Body)
	if metrics.StatusCode != http.StatusOK || !strings.Contains(string(body), "speko_gateway_sessions_active") {
		t.Fatalf("metrics status=%d body=%s", metrics.StatusCode, body)
	}
	gatewayServer.BeginDrain()
	if !gatewayServer.Stats().Draining {
		t.Fatal("begin drain did not publish the draining state")
	}
	if err := gatewayServer.Drain(context.Background()); err != nil {
		t.Fatalf("drain empty server: %v", err)
	}
	ready := postGet(t, httpServer.URL+"/readyz", "")
	defer ready.Body.Close()
	if ready.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("ready while draining = %d", ready.StatusCode)
	}
}

func TestGatewayRejectsOversizedWorkloadIdentity(t *testing.T) {
	t.Parallel()
	tests := []protocol.Workload{
		{Type: strings.Repeat("t", 65), ID: "agent-1"},
		{Type: "agent", ID: strings.Repeat("i", 257)},
	}
	for _, workload := range tests {
		workload := workload
		t.Run(workload.Type[:min(len(workload.Type), 8)], func(t *testing.T) {
			config, _ := newServerConfigWithAdapter(t, mock.NewSTTAdapter("mock.stt.v1"), 0, 0, 0, 0)
			config.Workload = &workload
			if _, err := gateway.New(config); err == nil {
				t.Fatalf("oversized workload was accepted: type=%d id=%d", len(workload.Type), len(workload.ID))
			}
		})
	}
}

func TestGatewayReleasesIdempotencyWhenSessionTerminates(t *testing.T) {
	t.Parallel()
	gatewayServer, plans := newServer(t)
	httpServer := httptest.NewServer(gatewayServer.Handler())
	t.Cleanup(httpServer.Close)

	body := gatewayRequestBody()
	first := postJSON(t, httpServer.URL+"/v1/sessions", body, "local-token", "completed-key")
	if first.StatusCode != http.StatusCreated {
		first.Body.Close()
		t.Fatalf("first create status = %d", first.StatusCode)
	}
	first.Body.Close()
	deleteResponse := deleteSession(t, httpServer.URL+"/v1/sessions/session-gateway", "local-token")
	if deleteResponse.StatusCode != http.StatusNoContent {
		deleteResponse.Body.Close()
		t.Fatalf("delete status = %d", deleteResponse.StatusCode)
	}
	deleteResponse.Body.Close()

	// DELETE removes the local replay record before returning, so a retry can
	// never receive a stream URL for the session it just deleted.
	response := postJSON(t, httpServer.URL+"/v1/sessions", body, "local-token", "completed-key")
	if response.StatusCode != http.StatusCreated {
		response.Body.Close()
		t.Fatalf("create after delete status = %d, want %d", response.StatusCode, http.StatusCreated)
	}
	response.Body.Close()
	if got := len(plans.calls()); got != 2 {
		t.Fatalf("plan calls = %d, want 2 after completed idempotency key cleanup", got)
	}
	cleanup := deleteSession(t, httpServer.URL+"/v1/sessions/session-gateway", "local-token")
	cleanup.Body.Close()
}

func TestGatewayAllowsOnlyOneStreamAttachment(t *testing.T) {
	t.Parallel()
	gatewayServer, _ := newServer(t)
	httpServer := httptest.NewServer(gatewayServer.Handler())
	t.Cleanup(httpServer.Close)

	created := postJSON(t, httpServer.URL+"/v1/sessions", gatewayRequestBody(), "local-token", "stream-claim")
	defer created.Body.Close()
	if created.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d", created.StatusCode)
	}
	var response struct {
		StreamURL string `json:"stream_url"`
	}
	if err := json.NewDecoder(created.Body).Decode(&response); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + response.StreamURL
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer local-token")
	first, _, err := websocket.Dial(context.Background(), wsURL, &websocket.DialOptions{HTTPHeader: headers, Subprotocols: []string{gateway.WebSocketSubprotocol}})
	if err != nil {
		t.Fatalf("dial first stream: %v", err)
	}
	defer first.Close(websocket.StatusNormalClosure, "")
	expectEvent(t, first, protocol.EventSessionReady)

	second, rejected, err := websocket.Dial(context.Background(), wsURL, &websocket.DialOptions{HTTPHeader: headers, Subprotocols: []string{gateway.WebSocketSubprotocol}})
	if err == nil {
		second.Close(websocket.StatusNormalClosure, "")
		t.Fatal("second stream attachment unexpectedly succeeded")
	}
	if rejected == nil {
		t.Fatalf("second stream attachment error did not include an HTTP response: %v", err)
	}
	defer rejected.Body.Close()
	if rejected.StatusCode != http.StatusConflict {
		t.Fatalf("second stream status = %d, want %d", rejected.StatusCode, http.StatusConflict)
	}
}

func TestGatewayExpiresUnattachedSessions(t *testing.T) {
	t.Parallel()
	gatewayServer, plans := newServerWithOptions(t, 1, 25*time.Millisecond)
	httpServer := httptest.NewServer(gatewayServer.Handler())
	t.Cleanup(httpServer.Close)

	body := gatewayRequestBody()
	first := postJSON(t, httpServer.URL+"/v1/sessions", body, "local-token", "unattached-1")
	if first.StatusCode != http.StatusCreated {
		first.Body.Close()
		t.Fatalf("first create status = %d", first.StatusCode)
	}
	first.Body.Close()

	// The first session is deliberately never upgraded. Its attachment timeout
	// must reclaim the only local session slot for a later create request.
	eventually(t, time.Second, func() bool {
		response := postJSON(t, httpServer.URL+"/v1/sessions", body, "local-token", "unattached-2")
		status := response.StatusCode
		response.Body.Close()
		return status == http.StatusCreated
	})
	if got := len(plans.calls()); got != 2 {
		t.Fatalf("plan calls = %d, want 2 after attachment timeout", got)
	}
	cleanup := deleteSession(t, httpServer.URL+"/v1/sessions/session-gateway", "local-token")
	cleanup.Body.Close()
}

func TestGatewayAttachmentClaimSurvivesAttachmentTimeout(t *testing.T) {
	t.Parallel()
	gatewayServer, _ := newServerWithOptions(t, 1, 25*time.Millisecond)
	httpServer := httptest.NewServer(gatewayServer.Handler())
	t.Cleanup(httpServer.Close)

	created := postJSON(t, httpServer.URL+"/v1/sessions", gatewayRequestBody(), "local-token", "attached-before-timeout")
	defer created.Body.Close()
	if created.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d", created.StatusCode)
	}
	var response struct {
		StreamURL string `json:"stream_url"`
	}
	if err := json.NewDecoder(created.Body).Decode(&response); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer local-token")
	connection, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(httpServer.URL, "http")+response.StreamURL, &websocket.DialOptions{HTTPHeader: headers, Subprotocols: []string{gateway.WebSocketSubprotocol}})
	if err != nil {
		t.Fatalf("dial stream: %v", err)
	}
	defer connection.Close(websocket.StatusNormalClosure, "")
	expectEvent(t, connection, protocol.EventSessionReady)

	time.Sleep(75 * time.Millisecond)
	if err := connection.Write(context.Background(), websocket.MessageBinary, []byte{1}); err != nil {
		t.Fatalf("write after attachment timeout: %v", err)
	}
	if err := writeCommand(connection, "audio.commit", nil); err != nil {
		t.Fatalf("commit after attachment timeout: %v", err)
	}
	expectEvent(t, connection, protocol.EventSpeechStarted)
	expectEvent(t, connection, protocol.EventTranscriptDelta)
}

func TestGatewayReleaseSessionAttachmentAfterTimeoutReclaimsCapacityAndDrains(t *testing.T) {
	t.Parallel()
	now := time.Now()
	gatewayServer, _ := newServerWithOptions(t, 1, 50*time.Millisecond)
	httpServer := httptest.NewServer(gatewayServer.Handler())
	t.Cleanup(httpServer.Close)

	created := postJSON(t, httpServer.URL+"/v1/sessions", gatewayRequestBody(), "local-token", "attachment-release-expired")
	defer created.Body.Close()
	if created.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d", created.StatusCode)
	}
	var response struct {
		StreamURL string `json:"stream_url"`
	}
	if err := json.NewDecoder(created.Body).Decode(&response); err != nil {
		t.Fatalf("decode create response: %v", err)
	}

	// Wait past the attachment timeout so remaining time is <= 0 when WebSocket fails.
	time.Sleep(100 * time.Millisecond)

	// Attempt a WebSocket dial with an invalid subprotocol, causing websocket.Accept to fail
	// and invoke releaseSessionAttachment with remaining <= 0.
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer local-token")
	_, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(httpServer.URL, "http")+response.StreamURL, &websocket.DialOptions{
		HTTPHeader:   headers,
		Subprotocols: []string{"invalid-subprotocol"},
	})
	if err == nil {
		t.Fatal("expected websocket dial with invalid subprotocol to fail")
	}

	// Verify that the session was immediately detached and drained without hanging.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := gatewayServer.Drain(ctx); err != nil {
		t.Fatalf("gateway failed to drain after attachment release: %v", err)
	}

	stats := gatewayServer.Stats()
	if stats.ActiveSessions != 0 {
		t.Fatalf("active sessions = %d, want 0", stats.ActiveSessions)
	}
	_ = now
}


func TestGatewayCoalescesConcurrentIdempotentCreates(t *testing.T) {
	t.Parallel()
	gatewayServer, plans := newServer(t)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer func() { releaseOnce.Do(func() { close(release) }) }()
	plans.started = started
	plans.release = release
	httpServer := httptest.NewServer(gatewayServer.Handler())
	t.Cleanup(httpServer.Close)

	payload, err := json.Marshal(gatewayRequestBody())
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	type result struct {
		status int
		err    error
	}
	const callers = 8
	results := make(chan result, callers)
	start := make(chan struct{})
	for range callers {
		go func() {
			<-start
			request, err := http.NewRequest(http.MethodPost, httpServer.URL+"/v1/sessions", bytes.NewReader(payload))
			if err != nil {
				results <- result{err: err}
				return
			}
			request.Header.Set("Authorization", "Bearer local-token")
			request.Header.Set("Idempotency-Key", "concurrent-create")
			request.Header.Set("Content-Type", "application/json")
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				results <- result{err: err}
				return
			}
			response.Body.Close()
			results <- result{status: response.StatusCode}
		}()
	}
	close(start)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("control-plane call did not start")
	}

	// Keep the leader blocked long enough for all retries to reach the
	// gateway. A second control-plane call here would regress coalescing.
	time.Sleep(100 * time.Millisecond)
	if got := len(plans.calls()); got != 1 {
		releaseOnce.Do(func() { close(release) })
		t.Fatalf("in-flight plan calls = %d, want 1", got)
	}
	releaseOnce.Do(func() { close(release) })
	created := 0
	for range callers {
		outcome := <-results
		if outcome.err != nil {
			t.Fatalf("concurrent create: %v", outcome.err)
		}
		switch outcome.status {
		case http.StatusCreated:
			created++
		case http.StatusOK:
		default:
			t.Fatalf("concurrent create status = %d", outcome.status)
		}
	}
	if created != 1 {
		t.Fatalf("created responses = %d, want 1", created)
	}
	if got := len(plans.calls()); got != 1 {
		t.Fatalf("plan calls = %d, want 1", got)
	}
	cleanup := deleteSession(t, httpServer.URL+"/v1/sessions/session-gateway", "local-token")
	cleanup.Body.Close()
}

func TestGatewayCoalescedCreateOutlivesLeaderCancellation(t *testing.T) {
	t.Parallel()
	gatewayServer, plans := newServer(t)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer func() { releaseOnce.Do(func() { close(release) }) }()
	plans.started = started
	plans.release = release
	httpServer := httptest.NewServer(gatewayServer.Handler())
	t.Cleanup(httpServer.Close)

	leaderContext, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		response, err := postJSONRequestWithContext(leaderContext, httpServer.URL+"/v1/sessions", gatewayRequestBody(), "local-token", "leader-cancel")
		if response != nil {
			response.Body.Close()
		}
		leaderDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("control-plane call did not start")
	}

	type result struct {
		status int
		err    error
	}
	follower := make(chan result, 1)
	go func() {
		response, err := postJSONRequest(httpServer.URL+"/v1/sessions", gatewayRequestBody(), "local-token", "leader-cancel")
		if err != nil {
			follower <- result{err: err}
			return
		}
		response.Body.Close()
		follower <- result{status: response.StatusCode}
	}()
	time.Sleep(50 * time.Millisecond)
	cancelLeader()
	select {
	case <-leaderDone:
	case <-time.After(time.Second):
		t.Fatal("leader request did not observe cancellation")
	}
	if got := len(plans.calls()); got != 1 {
		t.Fatalf("plan calls after leader cancellation = %d, want 1", got)
	}

	releaseOnce.Do(func() { close(release) })
	if outcome := <-follower; outcome.err != nil || outcome.status != http.StatusOK {
		t.Fatalf("follower outcome = %+v, want OK", outcome)
	}
	if got := len(plans.calls()); got != 1 {
		t.Fatalf("plan calls = %d, want 1", got)
	}
	cleanup := deleteSession(t, httpServer.URL+"/v1/sessions/session-gateway", "local-token")
	cleanup.Body.Close()
}

func TestGatewayRejectsConflictingIdempotencyRequests(t *testing.T) {
	t.Parallel()
	gatewayServer, plans := newServer(t)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer func() { releaseOnce.Do(func() { close(release) }) }()
	plans.started = started
	plans.release = release
	httpServer := httptest.NewServer(gatewayServer.Handler())
	t.Cleanup(httpServer.Close)

	type result struct {
		status int
		err    error
	}
	leader := make(chan result, 1)
	go func() {
		response, err := postJSONRequest(httpServer.URL+"/v1/sessions", gatewayRequestBody(), "local-token", "shared-key")
		if err != nil {
			leader <- result{err: err}
			return
		}
		response.Body.Close()
		leader <- result{status: response.StatusCode}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("control-plane call did not start")
	}

	conflicting := gatewayRequestBody()
	conflicting["request"] = map[string]string{"model": "different-model"}
	conflict := postJSON(t, httpServer.URL+"/v1/sessions", conflicting, "local-token", "shared-key")
	if conflict.StatusCode != http.StatusConflict {
		conflict.Body.Close()
		t.Fatalf("in-flight conflict status = %d, want %d", conflict.StatusCode, http.StatusConflict)
	}
	var failure struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(conflict.Body).Decode(&failure); err != nil {
		conflict.Body.Close()
		t.Fatalf("decode in-flight conflict: %v", err)
	}
	conflict.Body.Close()
	if failure.Error.Code != "idempotency_key_conflict" {
		t.Fatalf("in-flight conflict code = %q", failure.Error.Code)
	}
	if got := len(plans.calls()); got != 1 {
		t.Fatalf("in-flight plan calls = %d, want 1", got)
	}

	releaseOnce.Do(func() { close(release) })
	if outcome := <-leader; outcome.err != nil || outcome.status != http.StatusCreated {
		t.Fatalf("leader outcome = %+v, want created", outcome)
	}
	completed := postJSON(t, httpServer.URL+"/v1/sessions", conflicting, "local-token", "shared-key")
	defer completed.Body.Close()
	if completed.StatusCode != http.StatusConflict {
		t.Fatalf("completed conflict status = %d, want %d", completed.StatusCode, http.StatusConflict)
	}
	if got := len(plans.calls()); got != 1 {
		t.Fatalf("plan calls = %d, want 1", got)
	}
	cleanup := deleteSession(t, httpServer.URL+"/v1/sessions/session-gateway", "local-token")
	cleanup.Body.Close()
}

func TestGatewayWriteTimeoutReclaimsStalledStream(t *testing.T) {
	t.Parallel()
	adapter := mock.NewAdapter("mock.stalled.stt", func(_ runtimepkg.AdapterRequest) *mock.Stream {
		return mock.NewStream(4)
	})
	gatewayServer, _ := newServerWithAdapter(t, adapter, 1, 0, 25*time.Millisecond, 0)
	httpServer := httptest.NewServer(gatewayServer.Handler())
	t.Cleanup(httpServer.Close)

	created := postJSON(t, httpServer.URL+"/v1/sessions", gatewayRequestBody(), "local-token", "stalled-stream")
	defer created.Body.Close()
	if created.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d", created.StatusCode)
	}
	var response struct {
		StreamURL string `json:"stream_url"`
	}
	if err := json.NewDecoder(created.Body).Decode(&response); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer local-token")
	connection, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(httpServer.URL, "http")+response.StreamURL, &websocket.DialOptions{HTTPHeader: headers, Subprotocols: []string{gateway.WebSocketSubprotocol}})
	if err != nil {
		t.Fatalf("dial stream: %v", err)
	}
	defer connection.Close(websocket.StatusNormalClosure, "")
	stream := adapter.LastStream()
	if stream == nil {
		t.Fatal("provider stream was not opened")
	}
	if err := stream.Emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Audio: make([]byte, 8<<20)}); err != nil {
		t.Fatalf("emit large audio frame: %v", err)
	}

	// The client deliberately never reads. The write deadline must abort the
	// stream and release the only local session slot instead of retaining it.
	eventually(t, 2*time.Second, func() bool {
		created := postJSON(t, httpServer.URL+"/v1/sessions", gatewayRequestBody(), "local-token", "after-stall")
		status := created.StatusCode
		created.Body.Close()
		return status == http.StatusCreated
	})
	cleanup := deleteSession(t, httpServer.URL+"/v1/sessions/session-gateway", "local-token")
	cleanup.Body.Close()
}

func TestGatewayBoundsSetupWork(t *testing.T) {
	t.Parallel()
	adapter := mock.NewSTTAdapter("mock.setup.stt")
	gatewayServer, plans := newServerWithAdapter(t, adapter, 1, 0, 0, 25*time.Millisecond)
	release := make(chan struct{})
	defer close(release)
	plans.release = release
	httpServer := httptest.NewServer(gatewayServer.Handler())
	t.Cleanup(httpServer.Close)

	first := postJSON(t, httpServer.URL+"/v1/sessions", gatewayRequestBody(), "local-token", "setup-timeout-1")
	defer first.Body.Close()
	if first.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("setup timeout status = %d, want %d", first.StatusCode, http.StatusGatewayTimeout)
	}
	second := postJSON(t, httpServer.URL+"/v1/sessions", gatewayRequestBody(), "local-token", "setup-timeout-2")
	defer second.Body.Close()
	if second.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("second setup timeout status = %d, want %d", second.StatusCode, http.StatusGatewayTimeout)
	}
	if got := len(plans.calls()); got != 2 {
		t.Fatalf("plan calls = %d, want 2 after pending slot cleanup", got)
	}
}

func TestListenUnixCreatesOwnerOnlySocket(t *testing.T) {
	t.Parallel()
	directory, err := os.MkdirTemp("/tmp", "spk-")
	if err != nil {
		t.Fatalf("create short temporary directory: %v", err)
	}
	socketPath := directory + "/runtime.sock"
	t.Cleanup(func() {
		_ = os.Remove(socketPath)
		_ = os.Remove(directory)
	})
	listener, err := gateway.ListenUnix(socketPath)
	if err != nil {
		t.Fatalf("ListenUnix: %v", err)
	}
	defer listener.Close()
	info, err := os.Stat(socketPath)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("socket mode = %o, want 600", got)
	}
}

// flakyOpenAdapter fails the first N provider opens with a classified
// ProviderError, then delegates to the inner mock adapter.
type flakyOpenAdapter struct {
	inner    *mock.Adapter
	openErr  error
	failures int

	mu    sync.Mutex
	opens int
}

func (a *flakyOpenAdapter) ID() string { return a.inner.ID() }

func (a *flakyOpenAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	a.mu.Lock()
	a.opens++
	shouldFail := a.opens <= a.failures
	a.mu.Unlock()
	if shouldFail {
		return nil, a.openErr
	}
	return a.inner.Open(ctx, request)
}

func TestGatewayPerformsOneBoundedFallbackExchangeWhenProviderOpenFails(t *testing.T) {
	t.Parallel()
	adapter := &flakyOpenAdapter{
		inner:    mock.NewSTTAdapter("mock.stt.v1"),
		openErr:  &runtimepkg.ProviderError{Code: "provider_unavailable", Retryable: true, ProviderStatus: 503},
		failures: 1,
	}
	gatewayServer, plans := newServerWithAdapter(t, adapter, 0, 0, 0, 0)
	basePlan := gatewayPlan()
	basePlan.Fallback = &protocol.Fallback{ExchangeURL: "https://control.speko.test/v1/sessions/session-gateway/fallback-plans"}
	fallbackPlan := basePlan
	fallbackPlan.PlanID = "plan-gateway-fallback"
	fallbackPlan.AttemptID = "attempt-gateway-fallback"
	plans.mu.Lock()
	plans.plan = basePlan
	plans.fallbackPlan = &fallbackPlan
	plans.mu.Unlock()
	httpServer := httptest.NewServer(gatewayServer.Handler())
	t.Cleanup(httpServer.Close)

	response := postJSON(t, httpServer.URL+"/v1/sessions", gatewayRequestBody(), "local-token", "fallback-create-1")
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("create status = %d: %s", response.StatusCode, body)
	}
	var created struct {
		SessionID string `json:"session_id"`
		AttemptID string `json:"attempt_id"`
		RequestID string `json:"control_plane_request_id"`
	}
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if created.SessionID != "session-gateway" || created.AttemptID != "attempt-gateway-fallback" || created.RequestID != "cp_gateway_fb_123" {
		t.Fatalf("create response after fallback = %+v", created)
	}
	exchanges := plans.fallbackCalls()
	if len(exchanges) != 1 {
		t.Fatalf("fallback exchanges = %d, want exactly 1", len(exchanges))
	}
	exchange := exchanges[0]
	if exchange.request.AttemptID != "attempt-gateway" || exchange.request.Reason != "provider_open_failed" || exchange.request.ProviderCode != "provider_unavailable" || exchange.request.ProviderStatus != 503 {
		t.Fatalf("fallback request = %+v", exchange.request)
	}
	if exchange.idempotencyKey != "fallback-create-1:fallback:attempt-gateway" {
		t.Fatalf("fallback idempotency key = %q", exchange.idempotencyKey)
	}
	if exchange.current.PlanID != "plan-gateway" {
		t.Fatalf("fallback exchanged from plan %q", exchange.current.PlanID)
	}
}

func TestGatewayFallbackIsSingleAttemptAndFailsClosed(t *testing.T) {
	t.Parallel()
	adapter := &flakyOpenAdapter{
		inner:    mock.NewSTTAdapter("mock.stt.v1"),
		openErr:  &runtimepkg.ProviderError{Code: "provider_unavailable", Retryable: true, ProviderStatus: 503},
		failures: 2,
	}
	gatewayServer, plans := newServerWithAdapter(t, adapter, 0, 0, 0, 0)
	basePlan := gatewayPlan()
	basePlan.Fallback = &protocol.Fallback{ExchangeURL: "https://control.speko.test/v1/sessions/session-gateway/fallback-plans"}
	fallbackPlan := basePlan
	fallbackPlan.PlanID = "plan-gateway-fallback"
	fallbackPlan.AttemptID = "attempt-gateway-fallback"
	plans.mu.Lock()
	plans.plan = basePlan
	plans.fallbackPlan = &fallbackPlan
	plans.mu.Unlock()
	httpServer := httptest.NewServer(gatewayServer.Handler())
	t.Cleanup(httpServer.Close)

	response := postJSON(t, httpServer.URL+"/v1/sessions", gatewayRequestBody(), "local-token", "fallback-create-2")
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadGateway {
		t.Fatalf("create status = %d, want 502 when the single fallback also fails", response.StatusCode)
	}
	var failure struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(response.Body).Decode(&failure); err != nil {
		t.Fatalf("decode failure: %v", err)
	}
	if failure.Error.Code != "session_open_failed" {
		t.Fatalf("failure code = %q", failure.Error.Code)
	}
	if exchanges := plans.fallbackCalls(); len(exchanges) != 1 {
		t.Fatalf("fallback exchanges = %d, want exactly 1 (bounded)", len(exchanges))
	}
}

func TestGatewayDoesNotFallBackForNonProviderFailuresOrWithoutPermission(t *testing.T) {
	t.Parallel()
	t.Run("non-provider open failure", func(t *testing.T) {
		t.Parallel()
		adapter := &flakyOpenAdapter{
			inner:    mock.NewSTTAdapter("mock.stt.v1"),
			openErr:  errors.New("adapter wiring failure"),
			failures: 1,
		}
		gatewayServer, plans := newServerWithAdapter(t, adapter, 0, 0, 0, 0)
		basePlan := gatewayPlan()
		basePlan.Fallback = &protocol.Fallback{ExchangeURL: "https://control.speko.test/v1/sessions/session-gateway/fallback-plans"}
		plans.mu.Lock()
		plans.plan = basePlan
		plans.mu.Unlock()
		httpServer := httptest.NewServer(gatewayServer.Handler())
		t.Cleanup(httpServer.Close)
		response := postJSON(t, httpServer.URL+"/v1/sessions", gatewayRequestBody(), "local-token", "fallback-create-3")
		defer response.Body.Close()
		if response.StatusCode != http.StatusBadGateway || len(plans.fallbackCalls()) != 0 {
			t.Fatalf("status=%d exchanges=%d; non-provider failures must not consume the fallback", response.StatusCode, len(plans.fallbackCalls()))
		}
	})
	t.Run("plan without fallback permission", func(t *testing.T) {
		t.Parallel()
		adapter := &flakyOpenAdapter{
			inner:    mock.NewSTTAdapter("mock.stt.v1"),
			openErr:  &runtimepkg.ProviderError{Code: "provider_unavailable", Retryable: true, ProviderStatus: 503},
			failures: 1,
		}
		gatewayServer, plans := newServerWithAdapter(t, adapter, 0, 0, 0, 0)
		httpServer := httptest.NewServer(gatewayServer.Handler())
		t.Cleanup(httpServer.Close)
		response := postJSON(t, httpServer.URL+"/v1/sessions", gatewayRequestBody(), "local-token", "fallback-create-4")
		defer response.Body.Close()
		if response.StatusCode != http.StatusBadGateway || len(plans.fallbackCalls()) != 0 {
			t.Fatalf("status=%d exchanges=%d; a plan without Fallback must not exchange", response.StatusCode, len(plans.fallbackCalls()))
		}
	})
}

type fakePlanClient struct {
	mu       sync.Mutex
	plan     protocol.SessionPlan
	requests []protocol.SessionPlanRequest
	started  chan struct{}
	release  <-chan struct{}

	batchRequests []protocol.SessionPlanRequest
	batchErr      error
	batchSerial   int

	fallbackPlan     *protocol.SessionPlan
	fallbackErr      error
	fallbackRequests []fallbackExchange
}

type fallbackExchange struct {
	current        protocol.SessionPlan
	request        controlplane.FallbackRequest
	idempotencyKey string
}

func (c *fakePlanClient) CreateSessionPlan(ctx context.Context, request protocol.SessionPlanRequest, _ controlplane.CreateOptions) (protocol.SessionPlan, string, error) {
	c.mu.Lock()
	c.requests = append(c.requests, request)
	started, release := c.started, c.release
	c.mu.Unlock()
	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return protocol.SessionPlan{}, "", ctx.Err()
		}
	}
	return c.plan, "cp_gateway_123", nil
}

// CreateSessionPlanBatch returns count copies of the fixture plan with distinct
// identifiers, so a pool built on this client behaves like one built on a real
// control plane: every warm plan is independently usable exactly once.
func (c *fakePlanClient) CreateSessionPlanBatch(_ context.Context, request protocol.SessionPlanRequest, count int, _ controlplane.CreateOptions) ([]protocol.SessionPlan, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.batchErr != nil {
		return nil, "", c.batchErr
	}
	c.batchRequests = append(c.batchRequests, request)
	plans := make([]protocol.SessionPlan, 0, count)
	for index := 0; index < count; index++ {
		c.batchSerial++
		plan := c.plan
		plan.PlanID = fmt.Sprintf("%s_warm_%d", c.plan.PlanID, c.batchSerial)
		plan.SessionID = fmt.Sprintf("%s_warm_%d", c.plan.SessionID, c.batchSerial)
		plan.AttemptID = fmt.Sprintf("%s_warm_%d", c.plan.AttemptID, c.batchSerial)
		plans = append(plans, plan)
	}
	return plans, "cp_gateway_batch_123", nil
}

func (c *fakePlanClient) batchCallCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.batchRequests)
}

func (c *fakePlanClient) createCallCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.requests)
}

func (c *fakePlanClient) ExchangeFallbackPlan(_ context.Context, current protocol.SessionPlan, request controlplane.FallbackRequest, idempotencyKey string) (protocol.SessionPlan, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fallbackRequests = append(c.fallbackRequests, fallbackExchange{current: current, request: request, idempotencyKey: idempotencyKey})
	if c.fallbackErr != nil {
		return protocol.SessionPlan{}, "", c.fallbackErr
	}
	if c.fallbackPlan == nil {
		return protocol.SessionPlan{}, "", errors.New("fake plan client has no fallback plan")
	}
	return *c.fallbackPlan, "cp_gateway_fb_123", nil
}

func (c *fakePlanClient) fallbackCalls() []fallbackExchange {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]fallbackExchange(nil), c.fallbackRequests...)
}

func (c *fakePlanClient) calls() []protocol.SessionPlanRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]protocol.SessionPlanRequest(nil), c.requests...)
}

func newServer(t *testing.T) (*gateway.Server, *fakePlanClient) {
	return newServerWithOptions(t, 0, 0)
}

func newServerWithOptions(t *testing.T, maxSessions int, attachTimeout time.Duration) (*gateway.Server, *fakePlanClient) {
	t.Helper()
	adapter := mock.NewSTTAdapter("mock.stt.v1")
	return newServerWithAdapter(t, adapter, maxSessions, attachTimeout, 0, 0)
}

func newServerWithAdapter(t *testing.T, adapter runtimepkg.Adapter, maxSessions int, attachTimeout, streamWriteTimeout, setupTimeout time.Duration) (*gateway.Server, *fakePlanClient) {
	t.Helper()
	config, plans := newServerConfigWithAdapter(t, adapter, maxSessions, attachTimeout, streamWriteTimeout, setupTimeout)
	server, err := gateway.New(config)
	if err != nil {
		t.Fatalf("new gateway: %v", err)
	}
	return server, plans
}

func newServerConfigWithAdapter(t *testing.T, adapter runtimepkg.Adapter, maxSessions int, attachTimeout, streamWriteTimeout, setupTimeout time.Duration) (gateway.Config, *fakePlanClient) {
	t.Helper()
	engine, err := runtimepkg.New(runtimepkg.Config{
		Adapters:         []runtimepkg.Adapter{adapter},
		Verifier:         runtimepkg.PlanVerifierFunc(func(context.Context, protocol.SessionPlan) error { return nil }),
		LocalCredentials: map[string]runtimepkg.LocalCredential{"mock": {Kind: protocol.CredentialBearer, Value: "customer-mock-key"}},
		Now:              func() time.Time { return gatewayNow },
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	plan := gatewayPlan()
	plan.Route.Adapter = adapter.ID()
	plans := &fakePlanClient{plan: plan}
	config := gateway.Config{
		Engine:             engine,
		Plans:              plans,
		LocalAuthToken:     "local-token",
		Runtime:            protocol.RuntimeDescriptor{Name: "go-gateway", Version: "test", InstanceID: "gateway-test", Placement: protocol.PlacementSidecar, ProviderRoutes: []protocol.ProviderRoute{protocol.RouteProviderDirect}, Adapters: []string{adapter.ID()}},
		MaxSessions:        maxSessions,
		AttachTimeout:      attachTimeout,
		StreamWriteTimeout: streamWriteTimeout,
		SetupTimeout:       setupTimeout,
		Now:                func() time.Time { return gatewayNow },
	}
	return config, plans
}

func gatewayRequestBody() map[string]any {
	return map[string]any{
		"kind":      "stt",
		"execution": map[string]string{"provider_route": "provider_direct", "credential_source": "byok", "relay_policy": "forbidden"},
		"request":   map[string]string{"model": "mock-model"},
		"media":     map[string]any{"encoding": "pcm_s16le", "sample_rate_hz": 16000, "channels": 1},
	}
}

func gatewayPlan() protocol.SessionPlan {
	return protocol.SessionPlan{
		PlanID: "plan-gateway", SessionID: "session-gateway", AttemptID: "attempt-gateway", Signature: "test-signature", ExpiresAt: gatewayNow.Add(time.Hour),
		Execution:    protocol.Execution{Placement: protocol.PlacementSidecar, ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK},
		Route:        protocol.PlanRoute{Provider: "mock", Model: "mock-model", Adapter: "mock.stt.v1", Transport: protocol.TransportWebSocket, Endpoint: "wss://provider.speko.test/stream"},
		Reservation:  protocol.Reservation{ID: "reservation-gateway", LeaseDurationSeconds: 60, LeaseExpiresAt: gatewayNow.Add(time.Minute), Concurrency: protocol.ConcurrencyReservation{LeaseID: "lease-gateway", Slots: 1}, Usage: protocol.UsageReservation{Unit: protocol.UsageUnitDurationSeconds, AuthorizedUnits: 60}},
		Telemetry:    protocol.Telemetry{Endpoint: "https://control.speko.test/v1/runtime-events", Token: "telemetry", FlushIntervalMS: 5_000},
		Requirements: protocol.Requirements{Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision, RuntimeVersion: "test"},
	}
}

func postJSON(t *testing.T, endpoint string, value any, token, idempotencyKey string) *http.Response {
	t.Helper()
	response, err := postJSONRequest(endpoint, value, token, idempotencyKey)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	return response
}

func postJSONRequest(endpoint string, value any, token, idempotencyKey string) (*http.Response, error) {
	return postJSONRequestWithContext(context.Background(), endpoint, value, token, idempotencyKey)
}

func postJSONRequestWithContext(ctx context.Context, endpoint string, value any, token, idempotencyKey string) (*http.Response, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	request.Header.Set("Content-Type", "application/json")
	return http.DefaultClient.Do(request)
}

func postGet(t *testing.T, endpoint, token string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	return response
}

func deleteSession(t *testing.T, endpoint, token string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(http.MethodDelete, endpoint, nil)
	if err != nil {
		t.Fatalf("new delete request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	return response
}

func eventually(t *testing.T, timeout time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if check() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true before timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func writeCommand(connection *websocket.Conn, kind string, data any) error {
	payload, err := json.Marshal(map[string]any{"type": kind, "data": data})
	if err != nil {
		return err
	}
	return connection.Write(context.Background(), websocket.MessageText, payload)
}

func expectEvent(t *testing.T, connection *websocket.Conn, wanted protocol.EventType) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	messageType, payload, err := connection.Read(ctx)
	if err != nil {
		t.Fatalf("read %s: %v", wanted, err)
	}
	if messageType != websocket.MessageText {
		t.Fatalf("message for %s type=%v, want text", wanted, messageType)
	}
	var event protocol.Event
	if err := json.Unmarshal(payload, &event); err != nil {
		t.Fatalf("decode %s: %v", wanted, err)
	}
	if event.Type != wanted {
		t.Fatalf("event type = %q, want %q", event.Type, wanted)
	}
}
