package controlplane_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/controlplane"
)

const incarnation = "11111111-1111-4111-8111-111111111111"

func TestVersionedInstanceWireAndRetryIdentity(t *testing.T) {
	for _, id := range []string{"team/worker", "team%2Fworker", "team worker", "worker🙂"} {
		t.Run(id, func(t *testing.T) {
			start := time.Date(2026, 11, 2, 0, 0, 0, 123456789, time.UTC)
			var bodies []string
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.EscapedPath() != "/control%2Fplane/v1/runtime-instances/"+url.PathEscape(id) || r.Header.Get(controlplane.InstanceProtocolHeader) != "2" || r.Header.Get("Authorization") != "Bearer sk_speko_test" {
					t.Error("identity or protocol changed")
				}
				b, _ := io.ReadAll(r.Body)
				bodies = append(bodies, string(b))
				var raw map[string]json.RawMessage
				if err := json.Unmarshal(b, &raw); err != nil {
					t.Error(err)
				}
				if string(raw["sequence"]) != "4294967295" || string(raw["incarnation_id"]) != `"`+incarnation+`"` || string(raw["started_at"]) != `"2026-11-02T00:00:00.123456789Z"` {
					t.Errorf("lifetime fields = %s", b)
				}
				if r.Method == http.MethodDelete && len(raw) != 3 {
					t.Error("DELETE must contain only lifetime fields")
				}
				w.Header().Set(controlplane.InstanceProtocolHeader, "2")
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			c, err := controlplane.New(controlplane.Config{BaseURL: server.URL + "/control%2Fplane/", APIKey: "sk_speko_test", HTTPClient: server.Client()})
			if err != nil {
				t.Fatal(err)
			}
			report := controlplane.InstanceHeartbeatV2{InstanceHeartbeat: controlplane.InstanceHeartbeat{RuntimeName: "go", RuntimeVersion: "test", StartedAt: start, SessionCapacity: 1}, IncarnationID: incarnation, Sequence: math.MaxUint32}
			for i := 0; i < 2; i++ {
				if err := c.ReportInstanceV2(context.Background(), id, report); err != nil {
					t.Fatal(err)
				}
			}
			if bodies[0] != bodies[1] {
				t.Fatal("retry bytes changed")
			}
			if err := c.DeregisterInstanceV2(context.Background(), id, controlplane.InstanceLifetimeV2{IncarnationID: incarnation, StartedAt: start, Sequence: math.MaxUint32}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestVersionedInstanceRequiresAcknowledgementAndNeverDowngrades(t *testing.T) {
	for _, code := range []int{204, 200, 409} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			var calls atomic.Int64
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Header.Get(controlplane.InstanceProtocolHeader) != "2" {
					t.Error("unversioned fallback")
				}
				w.Header().Set("X-Request-ID", "cp_conflict")
				if code == 200 {
					w.Header().Set(controlplane.InstanceProtocolHeader, "2")
				}
				w.WriteHeader(code)
			}))
			defer server.Close()
			c := newClient(t, server, "sk_speko_test")
			err := c.DeregisterInstanceV2(context.Background(), "worker", controlplane.InstanceLifetimeV2{IncarnationID: incarnation, StartedAt: time.Now(), Sequence: 1})
			if err == nil || calls.Load() != 1 {
				t.Fatalf("error=%v calls=%d", err, calls.Load())
			}
			if code == 409 {
				var h *controlplane.HTTPError
				if !errors.As(err, &h) || h.Status != 409 || h.RequestID != "cp_conflict" {
					t.Fatalf("error=%v", err)
				}
			}
		})
	}
}

func TestInvalidVersionedInstancesDoNotSend(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(204) }))
	defer server.Close()
	c := newClient(t, server, "sk_speko_test")
	good := controlplane.InstanceHeartbeatV2{InstanceHeartbeat: controlplane.InstanceHeartbeat{RuntimeName: "go", RuntimeVersion: "test", StartedAt: time.Now(), SessionCapacity: 1}, IncarnationID: incarnation, Sequence: 1}
	for _, mutate := range []func(*controlplane.InstanceHeartbeatV2){func(h *controlplane.InstanceHeartbeatV2) { h.Sequence = 0 }, func(h *controlplane.InstanceHeartbeatV2) {
		h.IncarnationID = strings.ToUpper("aaaaaaaa-1111-4111-8111-111111111111")
	}, func(h *controlplane.InstanceHeartbeatV2) { h.StartedAt = time.Time{} }, func(h *controlplane.InstanceHeartbeatV2) { h.ActiveSessions = -1 }, func(h *controlplane.InstanceHeartbeatV2) {
		h.Labels = map[string]string{"large": strings.Repeat("x", 16384)}
	}} {
		report := good
		mutate(&report)
		if err := c.ReportInstanceV2(context.Background(), "worker", report); err == nil {
			t.Fatal("invalid report accepted")
		}
	}
	if err := c.DeregisterInstanceV2(context.Background(), "worker", controlplane.InstanceLifetimeV2{IncarnationID: incarnation, StartedAt: time.Now()}); err == nil {
		t.Fatal("zero DELETE sequence")
	}
	if calls.Load() != 0 {
		t.Fatal("invalid report sent")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.ReportInstanceV2(ctx, "worker", good); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation=%v", err)
	}
	if calls.Load() != 0 {
		t.Fatal("cancelled report sent")
	}
}
