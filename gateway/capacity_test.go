package gateway_test

import (
	"context"
	"github.com/SpekoAI/gateway/gateway"
	"github.com/SpekoAI/gateway/protocol"
	"github.com/SpekoAI/gateway/providers/mock"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type capacityGateAdapter struct {
	runtimepkg.Adapter
	entered chan struct{}
	release chan struct{}
}

func (a *capacityGateAdapter) Open(ctx context.Context, r runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	close(a.entered)
	select {
	case <-a.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return a.Adapter.Open(ctx, r)
}
func TestVerifiedCapacityFollowsRealPendingActiveAndClosedSessions(t *testing.T) {
	adapter := &capacityGateAdapter{Adapter: mock.NewSTTAdapter("mock.stt.v1"), entered: make(chan struct{}), release: make(chan struct{})}
	config, plans := newServerConfigWithAdapter(t, adapter, 2, time.Second, 0, 0)
	plans.plan.Execution.CredentialSource = protocol.CredentialsManaged
	plans.plan.Route.Credential = &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "private-test-credential", ExpiresAt: plans.plan.ExpiresAt}
	plans.plan.Capacity = &protocol.CapacityAttribution{Account: "account/one", Capability: "stt_streams", Model: plans.plan.Route.Model, Slots: 1}
	server, err := gateway.New(config)
	if err != nil {
		t.Fatal(err)
	}
	peer := httptest.NewServer(server.Handler())
	defer peer.Close()
	body := gatewayRequestBody()
	body["execution"] = map[string]string{"provider_route": "provider_direct", "credential_source": "managed", "relay_policy": "forbidden"}
	done := make(chan *http.Response, 1)
	go func() {
		r, err := postJSONRequest(peer.URL+"/v1/sessions", body, "local-token", "capacity-create")
		if err != nil {
			done <- nil
			return
		}
		done <- r
	}()
	select {
	case <-adapter.entered:
	case <-time.After(time.Second):
		t.Fatal("provider open was never reached")
	}
	before := server.Stats()
	if before.ActiveSessions != 0 || before.PendingSessions != 1 || !before.Capacity.Complete || len(before.Capacity.Accounts) != 1 || before.Capacity.Accounts[0].Pending != 1 {
		t.Fatalf("opening was not attributed: %+v", before)
	}
	close(adapter.release)
	response := <-done
	if response == nil {
		t.Fatal("create failed")
	}
	defer response.Body.Close()
	io.Copy(io.Discard, response.Body)
	if response.StatusCode != 201 {
		t.Fatal("session creation failed")
	}
	active := server.Stats()
	if active.PendingSessions != 0 || active.ActiveSessions != 1 || !active.Capacity.Complete || active.Capacity.Accounts[0].Active != 1 || active.Capacity.Accounts[0].Pending != 0 {
		t.Fatalf("handoff counted work twice: %+v", active)
	}
	// A returned metrics snapshot cannot mutate retained signed attribution.
	active.Capacity.Accounts[0].Account = "other"
	if server.Stats().Capacity.Accounts[0].Account != "account/one" {
		t.Fatal("snapshot changed process accounting")
	}
	request, _ := http.NewRequest(http.MethodDelete, peer.URL+"/v1/sessions/"+plans.plan.SessionID, nil)
	request.Header.Set("Authorization", "Bearer local-token")
	closed, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	closed.Body.Close()
	after := server.Stats()
	if after.ActiveSessions != 0 || after.PendingSessions != 0 || len(after.Capacity.Accounts) != 0 || !after.Capacity.Complete {
		t.Fatalf("closed work stayed in occupancy: %+v", after)
	}
}
func TestBYOKOccupancyNeverBecomesManagedAccountUse(t *testing.T) {
	server, _ := newServerWithOptions(t, 1, time.Second)
	peer := httptest.NewServer(server.Handler())
	defer peer.Close()
	response := postJSON(t, peer.URL+"/v1/sessions", gatewayRequestBody(), "local-token", "byok-capacity")
	defer response.Body.Close()
	io.Copy(io.Discard, response.Body)
	stats := server.Stats()
	if !stats.Capacity.Complete || stats.Capacity.BYOKActive != 1 || len(stats.Capacity.Accounts) != 0 {
		t.Fatalf("customer key consumed managed use: %+v", stats)
	}
}

func TestManagedPlansWithoutAccountingRemainExplicitlyUnknown(t *testing.T) {
	adapter := mock.NewSTTAdapter("mock.stt.v1")
	config, plans := newServerConfigWithAdapter(t, adapter, 1, time.Second, 0, 0)
	plans.plan.Execution.CredentialSource = protocol.CredentialsManaged
	plans.plan.Route.Credential = &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "private-test-token", ExpiresAt: plans.plan.ExpiresAt}
	server, err := gateway.New(config)
	if err != nil {
		t.Fatal(err)
	}
	peer := httptest.NewServer(server.Handler())
	defer peer.Close()
	body := gatewayRequestBody()
	body["execution"] = map[string]string{"provider_route": "provider_direct", "credential_source": "managed", "relay_policy": "forbidden"}
	response := postJSON(t, peer.URL+"/v1/sessions", body, "local-token", "unknown-accounting")
	defer response.Body.Close()
	io.Copy(io.Discard, response.Body)
	if response.StatusCode != 201 {
		t.Fatal("existing plan should retain compatibility")
	}
	stats := server.Stats()
	if stats.Capacity.Complete || stats.Capacity.UnknownActive != 1 || len(stats.Capacity.Accounts) != 0 || stats.Capacity.BYOKActive != 0 {
		t.Fatal("missing attribution was treated as zero use")
	}
}
