package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/controlplane"
	"github.com/SpekoAI/gateway/gateway"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

type instanceWire struct {
	method string
	body   string
	value  struct {
		Incarnation string    `json:"incarnation_id"`
		Sequence    uint32    `json:"sequence"`
		StartedAt   time.Time `json:"started_at"`
		Draining    bool      `json:"draining"`
	}
}

func decodeInstanceWire(t *testing.T, request *http.Request) instanceWire {
	t.Helper()
	if request.Header.Get(controlplane.InstanceProtocolHeader) != "2" || request.Header.Get("Authorization") != "Bearer sk_speko_fixture" {
		t.Error("protocol or authenticated wire changed")
	}
	body, err := io.ReadAll(request.Body)
	if err != nil {
		t.Error(err)
	}
	r := instanceWire{method: request.Method, body: string(body)}
	if err := json.Unmarshal(body, &r.value); err != nil || r.value.Sequence == 0 || r.value.Incarnation == "" || r.value.StartedAt.IsZero() {
		t.Error("invalid lifetime wire")
	}
	return r
}

func acknowledgeInstance(writer http.ResponseWriter) {
	writer.Header().Set(controlplane.InstanceProtocolHeader, "2")
	writer.WriteHeader(http.StatusNoContent)
}

func reporterClient(t *testing.T, peer *httptest.Server) *controlplane.Client {
	t.Helper()
	client, err := controlplane.New(controlplane.Config{BaseURL: peer.URL, APIKey: "sk_speko_fixture", HTTPClient: peer.Client()})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestInstanceProducerRetainsRetryBodiesAndTerminalLifetime(t *testing.T) {
	var mu sync.Mutex
	var records []instanceWire
	puts, deletes := 0, 0
	const instanceID = "team/%2F worker🙂"
	peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() != "/v1/runtime-instances/"+url.PathEscape(instanceID) {
			t.Error("escaped instance identity changed")
		}
		record := decodeInstanceWire(t, r)
		mu.Lock()
		defer mu.Unlock()
		records = append(records, record)
		if r.Method == http.MethodPut {
			puts++
			if puts <= 2 {
				w.WriteHeader(503)
				return
			}
		} else {
			deletes++
			if deletes <= 2 {
				w.WriteHeader(503)
				return
			}
		}
		acknowledgeInstance(w)
	}))
	defer peer.Close()
	started := time.Date(2026, 11, 2, 0, 0, 0, 123456789, time.UTC)
	reporter, err := newInstanceReporter(reporterClient(t, peer), instanceID, started)
	if err != nil {
		t.Fatal(err)
	}
	heartbeat := controlplane.InstanceHeartbeat{RuntimeName: "go", RuntimeVersion: "test", StartedAt: time.Now(), SessionCapacity: 1,
		ActiveSessions: 9007199254740993, TelemetryDropped: math.MaxUint64, Labels: map[string]string{"fixture": "original"}}
	if reporter.report(context.Background(), heartbeat) == nil {
		t.Fatal("first report unexpectedly succeeded")
	}
	heartbeat.Labels["fixture"] = "changed"
	heartbeat.ActiveSessions = 0
	if err := reporter.report(context.Background(), heartbeat); err != nil {
		t.Fatal(err)
	}
	if reporter.close(context.Background()) == nil {
		t.Fatal("first DELETE unexpectedly succeeded")
	}
	if err := reporter.report(context.Background(), heartbeat); !errors.Is(err, errInstanceClosed) {
		t.Fatal("PUT after terminal DELETE")
	}
	if err := reporter.close(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(records) != 7 || records[0].body != records[1].body || records[0].body != records[2].body || records[4].body != records[5].body || records[4].body != records[6].body {
		t.Fatal("uncertain retry changed its exact body or sent extra work")
	}
	for i, sequence := range []uint32{1, 1, 1, 2, 3, 3, 3} {
		if records[i].value.Sequence != sequence || records[i].value.Incarnation != reporter.incarnation || !records[i].value.StartedAt.Equal(started) {
			t.Fatal("process lifetime or sequence changed")
		}
	}
	var raw map[string]json.RawMessage
	_ = json.Unmarshal([]byte(records[0].body), &raw)
	if string(raw["active_sessions"]) != "9007199254740993" || string(raw["telemetry_dropped"]) != "18446744073709551615" {
		t.Fatal("numeric int64 wire lost precision")
	}
}

func TestUnacknowledgedFinalReportStillSignalsExhaustion(t *testing.T) {
	var mu sync.Mutex
	var records []instanceWire
	peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		records = append(records, decodeInstanceWire(t, r))
		mu.Unlock()
		if r.Method == http.MethodPut {
			w.Header().Set("X-Request-ID", "private-fixture-request")
			w.WriteHeader(503)
			return
		}
		acknowledgeInstance(w)
	}))
	defer peer.Close()
	reporter, err := newInstanceReporter(reporterClient(t, peer), "worker", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	reporter.sequence = math.MaxUint32 - 2
	err = reporter.report(context.Background(), controlplane.InstanceHeartbeat{RuntimeName: "go", RuntimeVersion: "test", SessionCapacity: 1})
	if !errors.Is(err, errInstanceSequenceExhausted) {
		t.Fatal("unacknowledged last sequence did not trigger process drain")
	}
	if err.Error() != errInstanceSequenceExhausted.Error() {
		t.Fatal("exhaustion exit exposed private transport diagnostics")
	}
	if err := reporter.close(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(records) != 3 || records[0].body != records[1].body || records[2].method != http.MethodDelete || records[2].value.Sequence != math.MaxUint32 {
		t.Fatal("uncertain final report did not retain the reserved terminal sequence")
	}
}

func testServingGateway(t *testing.T, client *controlplane.Client, id string) *gateway.Server {
	t.Helper()
	engine, err := runtimepkg.New(runtimepkg.Config{Verifier: runtimepkg.PlanVerifierFunc(func(context.Context, protocol.SessionPlan) error { return errors.New("unexpected plan") })})
	if err != nil {
		t.Fatal(err)
	}
	server, err := gateway.New(gateway.Config{Engine: engine, Plans: client, LocalAuthToken: "fixture-local-token", Runtime: protocol.RuntimeDescriptor{
		Name: "go-gateway", Version: "test", InstanceID: id, Placement: protocol.PlacementSidecar, ProviderRoutes: []protocol.ProviderRoute{protocol.RouteProviderDirect},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func TestServingGatewayClosesLifetimeOnEveryExit(t *testing.T) {
	for _, cause := range []string{"signal-context", "serve-error", "sequence-exhaustion", "sequence-exhaustion-unacked"} {
		t.Run(cause, func(t *testing.T) {
			var mu sync.Mutex
			var records []instanceWire
			first := make(chan struct{}, 1)
			peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				record := decodeInstanceWire(t, r)
				mu.Lock()
				records = append(records, record)
				mu.Unlock()
				if cause == "sequence-exhaustion-unacked" && r.Method == http.MethodPut {
					w.WriteHeader(503)
				} else {
					acknowledgeInstance(w)
				}
				select {
				case first <- struct{}{}:
				default:
				}
			}))
			defer peer.Close()
			client := reporterClient(t, peer)
			server := testServingGateway(t, client, "worker")
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = listener.Close() })
			reporter, err := newInstanceReporter(client, "worker", time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(cause, "sequence-exhaustion") {
				reporter.sequence = math.MaxUint32 - 2
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			heartbeat := func() controlplane.InstanceHeartbeat {
				return instanceHeartbeat(server.Stats(), 0, protocol.RuntimeDescriptor{Name: "go", Version: "test"}, nil, reporter.startedAt)
			}
			done := make(chan error, 1)
			warmStopped := make(chan struct{})
			warm := func(ctx context.Context) { <-ctx.Done(); close(warmStopped) }
			go func() { done <- serveGateway(ctx, server, listener, reporter, heartbeat, time.Millisecond, warm) }()
			select {
			case <-first:
			case <-time.After(5 * time.Second):
				t.Fatal("first report was not sent")
			}
			if cause == "signal-context" {
				cancel()
			}
			if cause == "serve-error" {
				_ = listener.Close()
			}
			select {
			case err := <-done:
				if cause == "signal-context" && err != nil {
					t.Fatal(err)
				}
				if cause == "serve-error" && err == nil {
					t.Fatal("lost Serve error")
				}
				if strings.HasPrefix(cause, "sequence-exhaustion") && !errors.Is(err, errInstanceSequenceExhausted) {
					t.Fatal("lost sequence exhaustion")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("process cleanup did not finish")
			}
			select {
			case <-warmStopped:
			default:
				t.Fatal("warm worker was not joined")
			}
			mu.Lock()
			defer mu.Unlock()
			if records[len(records)-1].method != http.MethodDelete {
				t.Fatal("exit did not end in terminal DELETE")
			}
			for i := 1; i < len(records); i++ {
				if records[i].value.Sequence < records[i-1].value.Sequence || (records[i].value.Sequence == records[i-1].value.Sequence && records[i].body != records[i-1].body) || records[i].value.Incarnation != records[0].value.Incarnation {
					t.Fatal("exit reordered mutations")
				}
			}
			if strings.HasPrefix(cause, "sequence-exhaustion") {
				want := 2
				if cause == "sequence-exhaustion-unacked" {
					want = 3
				}
				if len(records) != want || records[len(records)-1].value.Sequence != math.MaxUint32 {
					t.Fatal("exhaustion wrapped or spent the terminal sequence")
				}
			} else if len(records) < 3 || !records[len(records)-2].value.Draining {
				t.Fatal("missing draining report")
			}
		})
	}
}
