package gateway_test

import (
	"context"
	"errors"
	"github.com/SpekoAI/gateway/controlplane"
	"github.com/SpekoAI/gateway/gateway"
	"github.com/SpekoAI/gateway/protocol"
	"github.com/SpekoAI/gateway/providers/mock"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
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
	awaitCapacity(t, func() bool { return server.Stats().ActiveSessions == 0 })
	after := server.Stats()
	if after.ActiveSessions != 0 || after.PendingSessions != 0 || len(after.Capacity.Accounts) != 0 || !after.Capacity.Complete {
		t.Fatalf("closed work stayed in occupancy: %+v", after)
	}
}

func awaitCapacity(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("provider lifecycle did not reach the expected boundary")
}

type capacityOwnedAdapter struct {
	open func(context.Context, runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error)
}

func (*capacityOwnedAdapter) ID() string { return "owned.capacity.stt.v1" }
func (a *capacityOwnedAdapter) Open(ctx context.Context, r runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	return a.open(ctx, r)
}

type capacityClosingStream struct {
	runtimepkg.ProviderStream
	once    sync.Once
	entered chan struct{}
	release <-chan struct{}
}

func (s *capacityClosingStream) Close(ctx context.Context) error {
	s.once.Do(func() { _ = s.ProviderStream.Close(ctx); close(s.entered) })
	<-s.release
	return nil
}

type capacityAbortingStream struct{ *capacityClosingStream }

func (s *capacityAbortingStream) Abort(ctx context.Context) error { return s.Close(ctx) }

type capacityPublishingClose struct {
	runtimepkg.ProviderStream
	events    chan runtimepkg.ProviderEvent
	once      sync.Once
	delivered chan struct{}
	release   <-chan struct{}
}

func (s *capacityPublishingClose) Events() <-chan runtimepkg.ProviderEvent { return s.events }
func (s *capacityPublishingClose) Close(context.Context) error {
	s.once.Do(func() {
		s.events <- runtimepkg.ProviderEvent{Type: protocol.EventTranscriptFinal}
		close(s.events)
		close(s.delivered)
		<-s.release
	})
	return nil
}

type capacityPublishingAbort struct{ *capacityPublishingClose }

func (s *capacityPublishingAbort) Abort(ctx context.Context) error { return s.Close(ctx) }

func TestCapacityDrainsTeardownEventsWithoutReleasingBeforeStopReturns(t *testing.T) {
	for _, partial := range []bool{false, true} {
		for _, fast := range []bool{false, true} {
			name := "session/close"
			if partial {
				name = "partial-open/close"
			}
			if fast {
				name += "/abort"
			}
			t.Run(name, func(t *testing.T) {
				release, delivered := make(chan struct{}), make(chan struct{})
				var once sync.Once
				finish := func() { once.Do(func() { close(release) }) }
				defer finish()
				adapter := &capacityOwnedAdapter{open: func(context.Context, runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
					base := &capacityPublishingClose{ProviderStream: mock.NewStream(8), events: make(chan runtimepkg.ProviderEvent), delivered: delivered, release: release}
					var stream runtimepkg.ProviderStream = base
					if fast {
						stream = &capacityPublishingAbort{base}
					}
					if partial {
						return stream, &runtimepkg.ProviderError{Code: "provider_unavailable", Retryable: true}
					}
					return stream, nil
				}}
				config, plans := newServerConfigWithAdapter(t, adapter, 1, time.Second, 0, 0)
				plans.plan.Execution.CredentialSource = protocol.CredentialsManaged
				plans.plan.Route.Credential = &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "owned-provider", ExpiresAt: plans.plan.ExpiresAt}
				plans.plan.Capacity = &protocol.CapacityAttribution{Account: "owned-account", Capability: "stt_streams", Model: plans.plan.Route.Model, Slots: 1}
				server, err := gateway.New(config)
				if err != nil {
					t.Fatal(err)
				}
				peer := httptest.NewServer(server.Handler())
				defer peer.Close()
				body := gatewayRequestBody()
				body["execution"] = map[string]string{"provider_route": "provider_direct", "credential_source": "managed", "relay_policy": "forbidden"}
				response := postJSON(t, peer.URL+"/v1/sessions", body, "local-token", "owned-publishing-stop")
				_ = response.Body.Close()
				if partial {
					if response.StatusCode != 502 {
						t.Fatal("partial open unexpectedly succeeded")
					}
				} else {
					if response.StatusCode != 201 {
						t.Fatal("create failed")
					}
					deleted := deleteSession(t, peer.URL+"/v1/sessions/"+plans.plan.SessionID, "local-token")
					_ = deleted.Body.Close()
				}
				select {
				case <-delivered:
				case <-time.After(3 * time.Second):
					t.Fatal("teardown event blocked after public reader stopped")
				}
				stats := server.Stats()
				if stats.ActiveSessions+int64(stats.PendingSessions) != 1 || stats.Capacity.Validate(stats.ActiveSessions, stats.PendingSessions) != nil {
					t.Fatalf("event closure released an unfinished stop: %+v", stats)
				}
				finish()
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				if err := server.Drain(ctx); err != nil {
					t.Fatal(err)
				}
				if server.Stats().ActiveSessions+int64(server.Stats().PendingSessions) != 0 {
					t.Fatal("completed teardown retained capacity")
				}
			})
		}
	}
}

func TestCapacityRetainsDetachedExpiredAndRejectedProviderWorkThroughTeardown(t *testing.T) {
	for _, mode := range []string{"delete", "expiry", "rejected-create"} {
		for _, fast := range []bool{false, true} {
			name := mode + "/close"
			if fast {
				name = mode + "/abort"
			}
			t.Run(name, func(t *testing.T) {
				release, closing, opened, start := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
				var once sync.Once
				finish := func() { once.Do(func() { close(release) }) }
				defer finish()
				adapter := &capacityOwnedAdapter{open: func(ctx context.Context, _ runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
					close(opened)
					if mode == "rejected-create" {
						select {
						case <-start:
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					}
					stream := &capacityClosingStream{ProviderStream: mock.NewStream(8), entered: closing, release: release}
					if fast {
						return &capacityAbortingStream{stream}, nil
					}
					return stream, nil
				}}
				attach := time.Second
				if mode == "expiry" {
					attach = 50 * time.Millisecond
				}
				config, plans := newServerConfigWithAdapter(t, adapter, 1, attach, 0, 0)
				plans.plan.Execution.CredentialSource = protocol.CredentialsManaged
				plans.plan.Route.Credential = &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "owned-provider", ExpiresAt: plans.plan.ExpiresAt}
				plans.plan.Capacity = &protocol.CapacityAttribution{Account: "owned-account", Capability: "stt_streams", Model: plans.plan.Route.Model, Slots: 1}
				server, err := gateway.New(config)
				if err != nil {
					t.Fatal(err)
				}
				peer := httptest.NewServer(server.Handler())
				defer peer.Close()
				body := gatewayRequestBody()
				body["execution"] = map[string]string{"provider_route": "provider_direct", "credential_source": "managed", "relay_policy": "forbidden"}
				created := make(chan *http.Response, 1)
				go func() {
					response, _ := postJSONRequest(peer.URL+"/v1/sessions", body, "local-token", "owned-teardown")
					created <- response
				}()
				select {
				case <-opened:
				case <-time.After(3 * time.Second):
					t.Fatal("provider did not open")
				}
				if mode == "rejected-create" {
					server.BeginDrain()
					close(start)
				}
				response := <-created
				if response == nil {
					t.Fatal("create response missing")
				}
				_ = response.Body.Close()
				want := 201
				if mode == "rejected-create" {
					want = 503
				}
				if response.StatusCode != want {
					t.Fatalf("create status: %d", response.StatusCode)
				}
				if mode == "delete" {
					deleted := deleteSession(t, peer.URL+"/v1/sessions/"+plans.plan.SessionID, "local-token")
					_ = deleted.Body.Close()
					if deleted.StatusCode != 204 {
						t.Fatal("delete failed")
					}
				}
				select {
				case <-closing:
				case <-time.After(3 * time.Second):
					t.Fatal("teardown did not begin")
				}
				stats := server.Stats()
				if stats.ActiveSessions != 1 || stats.PendingSessions != 0 || len(stats.Capacity.Accounts) != 1 || stats.Capacity.Accounts[0].Active != 1 || stats.Capacity.Validate(stats.ActiveSessions, stats.PendingSessions) != nil {
					t.Fatalf("physical teardown disappeared: %+v", stats)
				}
				if mode != "rejected-create" {
					refused := postJSON(t, peer.URL+"/v1/sessions", body, "local-token", "owned-replacement")
					_ = refused.Body.Close()
					if refused.StatusCode != 429 {
						t.Fatal("teardown allowed capacity reuse")
					}
				}
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
				if err := server.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("drain released physical work: %v", err)
				}
				cancel()
				finish()
				ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				if err := server.Drain(ctx); err != nil {
					t.Fatal(err)
				}
				stats = server.Stats()
				if stats.ActiveSessions != 0 || stats.PendingSessions != 0 || len(stats.Capacity.Accounts) != 0 || stats.Capacity.Validate(stats.ActiveSessions, stats.PendingSessions) != nil {
					t.Fatalf("teardown did not release exactly once: %+v", stats)
				}
			})
		}
	}
}

type capacityFallbackGate struct {
	gateway.PlanClient
	entered chan struct{}
	release <-chan struct{}
}

func TestFailedCreateRetainsAnOpenedStreamUntilItsTeardownCompletes(t *testing.T) {
	release, closing := make(chan struct{}), make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	defer finish()
	adapter := &capacityOwnedAdapter{open: func(context.Context, runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
		return &capacityClosingStream{ProviderStream: mock.NewStream(8), entered: closing, release: release}, &runtimepkg.ProviderError{Code: "provider_unavailable", Retryable: true}
	}}
	config, plans := newServerConfigWithAdapter(t, adapter, 1, time.Second, 0, 0)
	plans.plan.Execution.CredentialSource = protocol.CredentialsManaged
	plans.plan.Route.Credential = &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "owned-provider", ExpiresAt: plans.plan.ExpiresAt}
	plans.plan.Capacity = &protocol.CapacityAttribution{Account: "owned-account", Capability: "stt_streams", Model: plans.plan.Route.Model, Slots: 1}
	server, err := gateway.New(config)
	if err != nil {
		t.Fatal(err)
	}
	peer := httptest.NewServer(server.Handler())
	defer peer.Close()
	body := gatewayRequestBody()
	body["execution"] = map[string]string{"provider_route": "provider_direct", "credential_source": "managed", "relay_policy": "forbidden"}
	response := postJSON(t, peer.URL+"/v1/sessions", body, "local-token", "owned-partial-open")
	_ = response.Body.Close()
	if response.StatusCode != 502 {
		t.Fatal("partial provider open unexpectedly succeeded")
	}
	select {
	case <-closing:
	case <-time.After(3 * time.Second):
		t.Fatal("partial provider stream was not stopped")
	}
	stats := server.Stats()
	if stats.ActiveSessions != 0 || stats.PendingSessions != 1 || len(stats.Capacity.Accounts) != 1 || stats.Capacity.Accounts[0].Pending != 1 || stats.Capacity.Validate(stats.ActiveSessions, stats.PendingSessions) != nil {
		t.Fatalf("failed create lost physical resource: %+v", stats)
	}
	response = postJSON(t, peer.URL+"/v1/sessions", body, "local-token", "owned-retry")
	_ = response.Body.Close()
	if response.StatusCode != 429 {
		t.Fatal("partial open's closing resource was reused")
	}
	finish()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := server.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if server.Stats().PendingSessions != 0 {
		t.Fatal("partial opening did not release after physical close")
	}
}

func (c *capacityFallbackGate) ExchangeFallbackPlan(ctx context.Context, plan protocol.SessionPlan, request controlplane.FallbackRequest, key string) (protocol.SessionPlan, string, error) {
	close(c.entered)
	select {
	case <-c.release:
		return c.PlanClient.ExchangeFallbackPlan(ctx, plan, request, key)
	case <-ctx.Done():
		return protocol.SessionPlan{}, "", ctx.Err()
	}
}

func TestFailedOpeningAttributionIsUnknownDuringColdAndWarmFallbackExchange(t *testing.T) {
	for _, warm := range []bool{false, true} {
		name, failures := "cold", 1
		if warm {
			name, failures = "warm", 2
		}
		t.Run(name, func(t *testing.T) {
			adapter := &flakyOpenAdapter{inner: mock.NewSTTAdapter("mock.stt.v1"), failures: failures, openErr: &runtimepkg.ProviderError{Code: "provider_unavailable", Retryable: true, ProviderStatus: 503}}
			config, plans := newServerConfigWithAdapter(t, adapter, 2, time.Second, 0, 0)
			plans.plan.Execution.CredentialSource = protocol.CredentialsManaged
			plans.plan.Route.Credential = &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "owned-provider", ExpiresAt: plans.plan.ExpiresAt}
			plans.plan.Capacity = &protocol.CapacityAttribution{Account: "failed-account", Capability: "stt_streams", Model: plans.plan.Route.Model, Slots: 1}
			plans.plan.Fallback = &protocol.Fallback{ExchangeURL: "https://owned.invalid/fallback"}
			fallback := plans.plan
			fallback.AttemptID = "owned-fallback"
			fallback.Capacity = &protocol.CapacityAttribution{Account: "next-account", Capability: "stt_streams", Model: plans.plan.Route.Model, Slots: 1}
			plans.fallbackPlan = &fallback
			if warm {
				pool := newWarmPool(t, plans, 2)
				if err := pool.Warm(context.Background(), managedWarmRequest()); err != nil {
					t.Fatal(err)
				}
				config.WarmPlans = pool
			}
			release := make(chan struct{})
			var once sync.Once
			finish := func() { once.Do(func() { close(release) }) }
			defer finish()
			gate := &capacityFallbackGate{PlanClient: plans, entered: make(chan struct{}), release: release}
			config.Plans = gate
			server, err := gateway.New(config)
			if err != nil {
				t.Fatal(err)
			}
			peer := httptest.NewServer(server.Handler())
			defer peer.Close()
			body := gatewayRequestBody()
			body["request"] = map[string]string{"model": "mock-model", "language": "en"}
			body["execution"] = map[string]string{"provider_route": "provider_direct", "credential_source": "managed", "relay_policy": "forbidden"}
			done := make(chan *http.Response, 1)
			go func() {
				response, _ := postJSONRequest(peer.URL+"/v1/sessions", body, "local-token", "owned-fallback")
				done <- response
			}()
			select {
			case <-gate.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("fallback exchange did not begin")
			}
			stats := server.Stats()
			if stats.Capacity.Complete || stats.Capacity.UnknownPending != 1 || len(stats.Capacity.Accounts) != 0 || stats.Capacity.Validate(stats.ActiveSessions, stats.PendingSessions) != nil {
				t.Fatalf("failed account survived its opening: %+v", stats)
			}
			finish()
			response := <-done
			if response == nil {
				t.Fatal("missing fallback create")
			}
			_ = response.Body.Close()
			if response.StatusCode != 201 {
				t.Fatalf("fallback failed: %d", response.StatusCode)
			}
			stats = server.Stats()
			if len(stats.Capacity.Accounts) != 1 || stats.Capacity.Accounts[0].Account != "next-account" || stats.Capacity.Accounts[0].Active != 1 {
				t.Fatalf("fallback attribution: %+v", stats)
			}
			deleted := deleteSession(t, peer.URL+"/v1/sessions/"+plans.plan.SessionID, "local-token")
			_ = deleted.Body.Close()
		})
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
