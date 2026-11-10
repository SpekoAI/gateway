package cartesia

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

type bytesCapturingAdapter struct {
	*BytesTTSAdapter
	opened chan *bytesTTSStream
}

func (a bytesCapturingAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	stream, err := a.BytesTTSAdapter.Open(ctx, request)
	if err == nil {
		a.opened <- stream.(*bytesTTSStream)
	}
	return stream, err
}
func bytesRuntime(t *testing.T, handler http.HandlerFunc, config BytesTTSConfig) (*runtimepkg.Session, *bytesTTSStream) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	config.HTTPClient = server.Client()
	config.AllowedEndpointHosts = []string{"127.0.0.1"}
	config.AllowInsecureEndpoint = true
	adapter, err := NewBytesTTS(config)
	if err != nil {
		t.Fatal(err)
	}
	captured := bytesCapturingAdapter{adapter, make(chan *bytesTTSStream, 1)}
	engine, err := runtimepkg.New(runtimepkg.Config{Adapters: []runtimepkg.Adapter{captured}, Verifier: runtimepkg.PlanVerifierFunc(func(context.Context, protocol.SessionPlan) error { return nil }), LocalCredentials: map[string]runtimepkg.LocalCredential{"cartesia": {Kind: protocol.CredentialBearer, Value: "permanent-test-key"}}, Limits: runtimepkg.DefaultLimits(), Telemetry: runtimepkg.NopTelemetry{}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	request := bytesRequest()
	plan := request.Plan
	plan.PlanID = "plan"
	plan.SessionID = "session"
	plan.AttemptID = "attempt"
	plan.ExpiresAt = now.Add(time.Minute)
	plan.Signature = "synthetic"
	plan.Execution = protocol.Execution{Placement: protocol.PlacementEmbedded, ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK}
	plan.Route.Credential = nil
	plan.Route.Endpoint = server.URL + bytesTTSPath
	plan.Requirements = protocol.Requirements{Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision, RuntimeVersion: "0.1.0"}
	plan.Reservation = protocol.Reservation{ID: "reservation", LeaseDurationSeconds: 60, LeaseExpiresAt: now.Add(time.Minute), Concurrency: protocol.ConcurrencyReservation{LeaseID: "concurrency", Slots: 1}, Usage: protocol.UsageReservation{Unit: protocol.UsageUnitCharacters, AuthorizedUnits: 4000}}
	session, err := engine.Open(context.Background(), runtimepkg.OpenRequest{Kind: request.Kind, Plan: plan, Options: request.Options, Media: request.Media})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(session.Abort)
	return session, <-captured.opened
}
func awaitBytesRuntime(t *testing.T, session *runtimepkg.Session, wanted protocol.EventType) protocol.Event {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-session.Events():
			if !ok {
				t.Fatal("runtime closed before expected event")
			}
			if event.Type == protocol.EventError {
				t.Fatalf("runtime failed while waiting for %s", wanted)
			}
			if event.Type == wanted {
				return event
			}
		case <-timer.C:
			t.Fatalf("runtime did not produce %s", wanted)
		}
	}
}
func commitBytesRuntime(t *testing.T, session *runtimepkg.Session, text string) {
	t.Helper()
	if err := session.AppendText(text); err != nil {
		t.Fatal(err)
	}
	if err := session.CommitText(); err != nil {
		t.Fatal(err)
	}
}

func TestBytesTTSRuntimeCancellationInterruptsStalledHeadersAndAllowsNextUtterance(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{})
	var calls atomic.Int64
	session, _ := bytesRuntime(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if calls.Add(1) == 1 {
			close(entered)
			<-r.Context().Done()
			return
		}
		w.Header().Set("Content-Type", "audio/pcm")
		_, _ = w.Write([]byte{1, 2, 3, 4})
	}, BytesTTSConfig{HeaderTimeout: time.Minute})
	awaitBytesRuntime(t, session, protocol.EventSessionReady)
	commitBytesRuntime(t, session, "first")
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first provider POST did not start")
	}
	if err := session.Cancel(); err != nil {
		t.Fatal(err)
	}
	awaitBytesRuntime(t, session, protocol.EventResponseCanceled)
	if session.Err() != nil {
		t.Fatal("interrupting headers failed the session")
	}
	commitBytesRuntime(t, session, "second")
	awaitBytesRuntime(t, session, protocol.EventAudioDone)
	session.Close()
	awaitBytesRuntime(t, session, protocol.EventSessionClosed)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := session.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatal("cancel did not preserve the next utterance")
	}
}

func TestBytesTTSRuntimeCloseTimeoutCannotBecomeCleanSessionClosure(t *testing.T) {
	t.Parallel()
	session, provider := bytesRuntime(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "audio/pcm")
		_, _ = w.Write([]byte{1, 2})
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}, BytesTTSConfig{GracefulCloseIdleTimeout: 20 * time.Millisecond, ResponseIdleTimeout: time.Minute})
	awaitBytesRuntime(t, session, protocol.EventSessionReady)
	commitBytesRuntime(t, session, "first")
	awaitBytesRuntime(t, session, protocol.EventAudioFrame)
	session.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := session.Wait(ctx); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("close timeout became success or a stuck session")
	}
	if provider.TerminalError() == nil {
		t.Fatal("close error was not independently retained")
	}
	sawError := false
	for event := range session.Events() {
		if event.Type == protocol.EventSessionClosed {
			t.Fatal("timeout published session.closed")
		}
		sawError = sawError || event.Type == protocol.EventError
	}
	if !sawError {
		t.Fatal("runtime lost its terminal error event")
	}
}

func TestBytesTTSActiveBodyInactivityFailsWithoutAClosingCaller(t *testing.T) {
	t.Parallel()
	session, _ := bytesRuntime(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "audio/pcm")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}, BytesTTSConfig{ResponseIdleTimeout: 20 * time.Millisecond})
	awaitBytesRuntime(t, session, protocol.EventSessionReady)
	commitBytesRuntime(t, session, "first")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := session.Wait(ctx); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("stalled active response was not bounded")
	}
	for event := range session.Events() {
		if event.Type == protocol.EventSessionClosed || event.Type == protocol.EventAudioDone {
			t.Fatal("stalled response became a clean completion")
		}
	}
}

func TestBytesTTSCredentialExpiryRenewsManagedAndPreservesPermanentKeys(t *testing.T) {
	t.Parallel()
	for _, source := range []string{"managed-direct", "byok", "relay"} {
		t.Run(source, func(t *testing.T) {
			headers := make(chan string, 2)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				headers <- r.Header.Get("Authorization")
				w.Header().Set("Content-Type", "audio/pcm")
				_, _ = w.Write([]byte{1, 2})
			}))
			defer server.Close()
			adapter, err := NewBytesTTS(BytesTTSConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
			if err != nil {
				t.Fatal(err)
			}
			base := time.Now()
			var current atomic.Int64
			current.Store(base.UnixNano())
			adapter.now = func() time.Time { return time.Unix(0, current.Load()) }
			request := bytesRequest()
			request.Plan.Route.Endpoint = server.URL + bytesTTSPath
			request.Plan.Route.Credential.ExpiresAt = base.Add(time.Second)
			request.Plan.Reservation.LeaseExpiresAt = base.Add(time.Minute)
			request.Plan.Execution.ProviderRoute = protocol.RouteProviderDirect
			if source == "byok" {
				request.Plan.Execution.CredentialSource = protocol.CredentialsBYOK
			}
			if source == "relay" {
				request.Plan.Execution.ProviderRoute = protocol.RouteSpekoRelay
			}
			var refreshed atomic.Int64
			request.RefreshCredential = func(ctx context.Context) (protocol.DelegatedCredential, error) {
				refreshed.Add(1)
				if ctx.Err() != nil {
					return protocol.DelegatedCredential{}, ctx.Err()
				}
				return protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "fresh-token", ExpiresAt: base.Add(30 * time.Second)}, nil
			}
			stream, err := adapter.Open(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			defer stream.(runtimepkg.AbortingProviderStream).Abort(context.Background())
			for index := range 2 {
				if index == 1 {
					current.Store(base.Add(2 * time.Second).UnixNano())
					if !request.Plan.Reservation.LeaseExpiresAt.After(adapter.now()) {
						t.Fatal("fixture lease expired")
					}
				}
				if err := stream.AppendText(context.Background(), "text"); err != nil {
					t.Fatal(err)
				}
				if err := stream.CommitText(context.Background()); err != nil {
					t.Fatal(err)
				}
				if _, done := bytesDrain(t, stream); done.Err != nil {
					t.Fatal(done.Err)
				}
			}
			first, second := <-headers, <-headers
			if first != "Bearer sk_car_synthetic" {
				t.Fatal("initial credential changed")
			}
			if source == "managed-direct" {
				if refreshed.Load() != 1 || second != "Bearer fresh-token" {
					t.Fatal("managed POST did not renew its expired credential")
				}
			} else if refreshed.Load() != 0 || second != first {
				t.Fatal("permanent key was renewed or expired with its plan envelope")
			}
		})
	}
}

func TestBytesTTSAllPotentiallyDispatchedPostsRemainInBillingAfterHeaderCancellation(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{})
	var calls atomic.Int64
	stream := openBytes(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if calls.Add(1) == 2 {
			close(entered)
			<-r.Context().Done()
			return
		}
		w.Header().Set("Content-Type", "audio/pcm")
		_, _ = w.Write([]byte{1, 2})
	}, BytesTTSConfig{}, bytesRequest())
	observations := map[string]protocol.BillingObservation{}
	record := func(event runtimepkg.ProviderEvent) {
		if event.Billing != nil {
			merged, err := protocol.MergeBillingObservation(observations[event.Billing.OperationID], *event.Billing)
			if err != nil {
				t.Fatal(err)
			}
			observations[event.Billing.OperationID] = merged
		}
	}
	_ = stream.AppendText(context.Background(), "first")
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}
	for {
		event := bytesEvent(t, stream)
		record(event)
		if event.Type == protocol.EventAudioDone {
			break
		}
		if event.Err != nil {
			t.Fatal(event.Err)
		}
	}
	_ = stream.AppendText(context.Background(), "second")
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("second POST did not dispatch")
	}
	record(bytesEvent(t, stream))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := stream.Cancel(ctx); err != nil {
		t.Fatal(err)
	}
	record(bytesEvent(t, stream))
	if len(observations) != 2 {
		t.Fatal("a dispatched operation disappeared")
	}
	complete, pending := 0, 0
	for _, observation := range observations {
		if observation.Complete {
			complete++
		} else {
			pending++
			if len(observation.Quantities) != 0 {
				t.Fatal("unreceived audio became a measured zero or quantity")
			}
		}
	}
	if complete != 1 || pending != 1 {
		t.Fatal("one finished utterance was promoted into complete attempt evidence")
	}
}

func TestBytesTTSCancelRetainsOwnershipOfBackpressuredTerminalPublication(t *testing.T) {
	t.Parallel()
	stream := openBytes(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "audio/pcm")
		_, _ = w.Write([]byte{1, 2})
	}, BytesTTSConfig{EventBuffer: 3}, bytesRequest())
	_ = stream.AppendText(context.Background(), "first")
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}
	raw := stream.(*bytesTTSStream)
	deadline := time.Now().Add(time.Second)
	for {
		locked := raw.stateMu.TryLock()
		if locked {
			raw.stateMu.Unlock()
		}
		if len(raw.events) == 3 && !locked {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("completion did not reach the saturated event queue")
		}
		runtime.Gosched()
	}
	op := raw.active.Load()
	if op == nil {
		t.Fatal("terminal publication dropped its cancellation handle")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	canceled := make(chan error, 1)
	go func() { canceled <- stream.Cancel(ctx) }()
	select {
	case <-op.done:
	case <-ctx.Done():
		t.Fatal("cancel could not interrupt the blocked completion")
	}
	for range 3 {
		if event := bytesEvent(t, stream); event.Type == protocol.EventAudioDone {
			t.Fatal("canceled completion escaped the saturated queue")
		}
	}
	if event := bytesEvent(t, stream); event.Type != protocol.EventResponseCanceled {
		t.Fatal("cancellation acknowledgement was reordered")
	}
	if err := <-canceled; err != nil {
		t.Fatal(err)
	}
	if err := stream.AppendText(context.Background(), "next"); err != nil {
		t.Fatal(err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, done := bytesDrain(t, stream); done.Err != nil {
		t.Fatal(done.Err)
	}
}
