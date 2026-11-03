package controlplane_test

import (
	"context"
	"encoding/json"
	"github.com/SpekoAI/gateway/controlplane"
	"github.com/SpekoAI/gateway/protocol"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestAttributedInstanceRequiresExactV3AcknowledgementAndPreservesInt64(t *testing.T) {
	for _, ack := range []string{"3", "2", ""} {
		t.Run("ack-"+ack, func(t *testing.T) {
			var calls atomic.Int64
			peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Header.Get(controlplane.InstanceProtocolHeader) != "3" {
					t.Error("protocol downgrade")
				}
				body, _ := io.ReadAll(r.Body)
				if r.Method == http.MethodPut {
					if !strings.Contains(string(body), `"active":9007199254740993`) {
						t.Error("rounded account occupancy")
					}
					var raw map[string]json.RawMessage
					if json.Unmarshal(body, &raw) != nil || len(raw["capacity"]) == 0 {
						t.Error("missing attribution")
					}
				}
				w.Header().Set(controlplane.InstanceProtocolHeader, ack)
				w.WriteHeader(204)
			}))
			defer peer.Close()
			client := newClient(t, peer, "sk_speko_test")
			h := controlplane.InstanceHeartbeatV3{InstanceHeartbeatV2: controlplane.InstanceHeartbeatV2{InstanceHeartbeat: controlplane.InstanceHeartbeat{RuntimeName: "go", RuntimeVersion: "test", StartedAt: time.Now(), SessionCapacity: 1, ActiveSessions: 9007199254740993}, IncarnationID: incarnation, Sequence: 1},
				Capacity: protocol.CapacitySnapshot{Complete: true, Accounts: []protocol.CapacityOccupancy{{Account: "account/one", Capability: "stt_streams", Model: "nova-3", Active: 9007199254740993}}}}
			err := client.ReportInstanceV3(context.Background(), "worker", h)
			if (err == nil) != (ack == "3") || calls.Load() != 1 {
				t.Fatal("acknowledgement or retry contract changed")
			}
			h.Capacity.Accounts[0].Active = 0
			if client.ReportInstanceV3(context.Background(), "worker", h) == nil || calls.Load() != 1 {
				t.Fatal("incomplete state crossed physical send boundary")
			}
		})
	}
}
