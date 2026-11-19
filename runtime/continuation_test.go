package runtime_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/controlplane"
	"github.com/SpekoAI/gateway/protocol"
	"github.com/SpekoAI/gateway/providers/geminirealtime"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

const ownedGooglePath = "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContentConstrained"

type continuityPeer struct {
	verifier                                      *runtimepkg.JWKSPlanVerifier
	adapter                                       *geminirealtime.Adapter
	private                                       ed25519.PrivateKey
	envelope                                      protocol.SessionPlanEnvelope
	t                                             *testing.T
	server                                        *httptest.Server
	plan                                          protocol.SessionPlan
	mu                                            sync.Mutex
	calls, mints, sockets                         int
	key, body                                     string
	setups                                        chan map[string]any
	connected                                     chan *websocket.Conn
	closed                                        chan string
	closeStatuses                                 chan websocket.StatusCode
	inputs                                        chan string
	renewalStarted                                chan struct{}
	renewalRelease                                chan struct{}
	clockStarted                                  chan struct{}
	clockRelease                                  chan struct{}
	loseResponse, refuse                          bool
	setupRefusal, clockRefusal                    bool
	initialClockRefusal, holdSetup, holdHandshake bool
	badHandshake                                  bool
	holdClose                                     <-chan struct{}
	closeWithStatus                               websocket.StatusCode
	handshakeStarted, handshakeClosed             chan struct{}
	clockCalls                                    atomic.Int32
	sqlOffset                                     atomic.Int64
	clockSamples                                  chan time.Time
	wallOffset                                    atomic.Int64
	telemetry                                     collectingTelemetry
	renewalHandler                                http.HandlerFunc
}

// ownedContinuationAdapter uses Google's actual socket parser with a
// customer-owned credential model in this fixture. It establishes Engine/SDK
// state transitions, not acceptance of managed Google resumption. The real
// managed adapter is tested separately and cannot resume or mint a new slice.
type ownedContinuationAdapter struct{ *geminirealtime.Adapter }

func installedGoogleCapability(adapter runtimepkg.Adapter) bool {
	google, ok := adapter.(runtimepkg.GoogleRealtimeAdapter)
	return ok && google.GoogleRealtimeAdapter()
}

func (a ownedContinuationAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	request.Plan.Execution.CredentialSource = protocol.CredentialsBYOK
	return a.Adapter.Open(ctx, request)
}

func newContinuityPeer(t *testing.T, lease time.Duration, skew time.Duration, actual ...bool) (*continuityPeer, *runtimepkg.Engine, *controlplane.Client) {
	t.Helper()
	p := &continuityPeer{t: t, setups: make(chan map[string]any, 4), connected: make(chan *websocket.Conn, 4), closed: make(chan string, 4), closeStatuses: make(chan websocket.StatusCode, 4), inputs: make(chan string, 8), renewalStarted: make(chan struct{}, 4), handshakeStarted: make(chan struct{}, 1), handshakeClosed: make(chan struct{}, 1)}
	p.wallOffset.Store(int64(skew))
	public, private, _ := ed25519.GenerateKey(rand.Reader)
	jwks := newJWKSFixture(t, "continuity", public)
	p.server = httptest.NewTLSServer(http.HandlerFunc(p.handle))
	t.Cleanup(p.server.Close)
	now := time.Now()
	p.plan = testBYOKPlan(now, now.Add(lease))
	p.plan.Route = protocol.PlanRoute{Provider: "google", Adapter: geminirealtime.AdapterID, Model: "gemini-3.1-flash-live-preview", Transport: protocol.TransportWebSocket, Endpoint: "wss" + strings.TrimPrefix(p.server.URL, "https") + ownedGooglePath,
		Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "root-token", ExpiresAt: now.Add(lease)}}
	p.plan.Execution.CredentialSource = protocol.CredentialsManaged
	p.plan.Reservation.LeaseExpiresAt = now.Add(lease)
	p.plan.Reservation.Usage.AuthorizedUnits = int64(math.Ceil(lease.Seconds()))
	p.plan.Reservation.Billing = &protocol.BillingAuthorization{Mode: "direct_entitlement", State: "estimated", Currency: "USD", MaximumAmountMicros: 100, RenewalURL: p.server.URL + "/v1/sessions/session-test/entitlements/renew", RenewableUntil: now.Add(lease + 4*time.Second)}
	p.plan.Telemetry = protocol.Telemetry{Token: "gwt1." + strings.Repeat("a", 43), Endpoint: p.server.URL + "/v1/runtime-events", FlushIntervalMS: 1000}
	envelope := protocol.SessionPlanEnvelope{Issuer: "https://control.speko.test", Audience: []string{"speko-runtime"}, IssuedAt: now.Add(-15 * time.Second), ExpiresAt: p.plan.ExpiresAt, ID: "continuity-root", Plan: p.plan.Unsigned()}
	p.plan = resignPlan(t, private, "continuity", envelope, p.plan)
	wall := func() time.Time { return time.Now().Add(time.Duration(p.wallOffset.Load())) }
	verifier, err := runtimepkg.NewPlanVerifier(runtimepkg.PlanVerifierConfig{JWKSURL: jwks.URL, Issuer: envelope.Issuer, Audience: "speko-runtime", Now: wall, ClockSkew: 10 * time.Second, AllowInsecureJWKS: true})
	if err != nil {
		t.Fatal(err)
	}
	p.verifier, p.private, p.envelope = verifier, private, envelope
	adapter, err := geminirealtime.New(geminirealtime.Config{HTTPClient: p.server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatal(err)
	}
	p.adapter = adapter
	var selected runtimepkg.Adapter = ownedContinuationAdapter{adapter}
	if len(actual) > 0 && actual[0] {
		selected = adapter
	}
	engine, err := runtimepkg.New(runtimepkg.Config{Adapters: []runtimepkg.Adapter{selected}, Verifier: verifier, Now: wall, Telemetry: &p.telemetry})
	if err != nil {
		t.Fatal(err)
	}
	client, err := controlplane.New(controlplane.Config{BaseURL: p.server.URL, APIKey: "sk_speko_setup-never-continuation", HTTPClient: p.server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	return p, engine, client
}

func (p *continuityPeer) handle(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/v1/session-plans" {
		if r.Header.Get("Authorization") != "Bearer sk_speko_setup-never-continuation" {
			w.WriteHeader(401)
			return
		}
		_ = json.NewEncoder(w).Encode(p.plan)
		return
	}
	if r.URL.Path == ownedGooglePath {
		p.provider(w, r)
		return
	}
	if r.Header.Get("Authorization") != "Bearer "+p.plan.Telemetry.Token || r.Header.Get("Speko-Protocol-Revision") != "3" {
		p.t.Error("continuation lost plan-token family/revision")
		w.WriteHeader(401)
		return
	}
	if strings.HasSuffix(r.URL.Path, "/clock") {
		calls := p.clockCalls.Add(1)
		if p.initialClockRefusal || (calls > 1 && p.clockRefusal) {
			w.WriteHeader(503)
			return
		}
		if calls > 1 && p.clockRelease != nil {
			select {
			case p.clockStarted <- struct{}{}:
			default:
			}
			select {
			case <-p.clockRelease:
			case <-r.Context().Done():
				return
			}
		}
		sample := time.Now().UTC().Add(time.Duration(p.sqlOffset.Load()))
		_ = json.NewEncoder(w).Encode(map[string]any{"server_time": sample})
		if p.clockSamples != nil {
			p.clockSamples <- sample
		}
		return
	}
	if p.renewalHandler != nil {
		p.renewalHandler(w, r)
		return
	}
	var body map[string]string
	_ = json.NewDecoder(r.Body).Decode(&body)
	encoded, _ := json.Marshal(body)
	p.mu.Lock()
	p.calls++
	call := p.calls
	if call == 1 {
		p.key, p.body = r.Header.Get("Idempotency-Key"), string(encoded)
	} else if p.key != r.Header.Get("Idempotency-Key") || p.body != string(encoded) {
		p.t.Error("response-loss retry changed mint intent")
	}
	parsed, parseErr := time.Parse(time.RFC3339Nano, body["previous_expires_at"])
	if len(body) != 1 || parseErr != nil || !parsed.Equal(p.plan.Reservation.LeaseExpiresAt) {
		p.t.Error("changed continuation body")
	}
	if !p.refuse && p.mints == 0 {
		p.mints++
	}
	p.mu.Unlock()
	p.renewalStarted <- struct{}{}
	if p.refuse {
		w.WriteHeader(401)
		return
	}
	if p.loseResponse && call == 1 {
		conn, _, _ := w.(http.Hijacker).Hijack()
		_ = conn.Close()
		return
	}
	if p.renewalRelease != nil {
		select {
		case <-p.renewalRelease:
		case <-r.Context().Done():
			return
		}
	}
	end := p.plan.Reservation.Billing.RenewableUntil
	reply := controlplane.EntitlementRenewal{EntitlementID: "slice-1", SessionID: p.plan.SessionID, AttemptID: p.plan.AttemptID, ReservationID: p.plan.Reservation.ID, Sequence: 1, PreviousExpiresAt: p.plan.Reservation.LeaseExpiresAt, LeaseExpiresAt: end, AuthorizedUnits: 4, MaximumAmountMicros: 100, Currency: "USD", Credential: protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "renewed-token", ExpiresAt: end}}
	if call > 1 {
		w.WriteHeader(200)
	} else {
		w.WriteHeader(201)
	}
	_ = json.NewEncoder(w).Encode(reply)
}
func (p *continuityPeer) provider(w http.ResponseWriter, r *http.Request) {
	if p.badHandshake {
		conn, buffer, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = buffer.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: invalid\r\n\r\n")
		_ = buffer.Flush()
		p.handshakeStarted <- struct{}{}
		_, _ = io.Copy(io.Discard, conn)
		p.handshakeClosed <- struct{}{}
		return
	}
	if p.holdHandshake {
		p.handshakeStarted <- struct{}{}
		<-r.Context().Done()
		p.handshakeClosed <- struct{}{}
		return
	}
	token := r.URL.Query().Get("access_token")
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer conn.CloseNow()
	conn.SetReadLimit(1 << 20)
	_, payload, err := conn.Read(r.Context())
	if err != nil {
		return
	}
	var setup map[string]any
	_ = json.Unmarshal(payload, &setup)
	p.mu.Lock()
	p.sockets++
	p.mu.Unlock()
	p.setups <- setup
	p.connected <- conn
	if token == "renewed-token" && p.setupRefusal {
		_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"error":{"code":403}}`))
		return
	}
	if !p.holdSetup {
		err = conn.Write(r.Context(), websocket.MessageText, []byte(`{"setupComplete":{},"sessionResumptionUpdate":{"resumable":true,"newHandle":"owned-handle"}}`))
	}
	if err != nil {
		return
	}
	if p.holdClose != nil {
		<-p.holdClose
	}
	if p.closeWithStatus != 0 {
		_ = conn.Close(p.closeWithStatus, "owned peer shutdown")
		p.closed <- token
		return
	}
	for {
		_, payload, err = conn.Read(r.Context())
		if err != nil {
			p.closeStatuses <- websocket.CloseStatus(err)
			p.closed <- token
			return
		}
		p.inputs <- string(payload)
	}
}
func openContinuity(t *testing.T, p *continuityPeer, e *runtimepkg.Engine, c *controlplane.Client) *runtimepkg.Session {
	t.Helper()
	session, err := e.Open(context.Background(), continuityRequest(p, c))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(session.Abort)
	if event := <-session.Events(); event.Type != protocol.EventSessionReady {
		t.Fatalf("first event %s", event.Type)
	}
	return session
}
func continuityRequest(p *continuityPeer, c *controlplane.Client) runtimepkg.OpenRequest {
	return runtimepkg.OpenRequest{Kind: protocol.SessionKindRealtime, Plan: p.plan, Media: &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16000, Channels: 1}, Options: protocol.RequestOptions{S2S: &protocol.S2SOptions{OutputMedia: &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24000, Channels: 1}}}, Entitlements: c}
}
func awaitContinuity[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(6 * time.Second):
		t.Fatal("continuity event timed out")
		var zero T
		return zero
	}
}

func TestEngineOwnedContinuationRenewsTheSameAttemptThroughSDKAndWebSocket(t *testing.T) {
	for _, skew := range []time.Duration{-10 * time.Second, 0, 10 * time.Second} {
		t.Run(skew.String(), func(t *testing.T) {
			t.Parallel()
			p, e, c := newContinuityPeer(t, 12*time.Second, skew)
			session := openContinuity(t, p, e, c)
			_ = awaitContinuity(t, p.setups)
			_ = awaitContinuity(t, p.connected)
			setup := awaitContinuity(t, p.setups)
			nested := setup["setup"].(map[string]any)
			if nested["sessionResumption"].(map[string]any)["handle"] != "owned-handle" {
				t.Fatal("conversation handle missing from resumed setup")
			}
			conn := awaitContinuity(t, p.connected)
			if err := session.SubmitAudio(runtimepkg.AudioInput{Data: make([]byte, 320)}); err != nil {
				t.Fatal(err)
			}
			_ = awaitContinuity(t, p.inputs)
			_ = conn.Write(context.Background(), websocket.MessageText, []byte(`{"serverContent":{"inputTranscription":{"text":"after renewal"}}}`))
			event := awaitContinuity(t, session.Events())
			if event.Type != protocol.EventTranscriptFinal || event.SessionID != p.plan.SessionID || event.AttemptID != p.plan.AttemptID || event.Sequence != 2 {
				t.Fatalf("lost attempt/order: %+v", event)
			}
			_ = conn.Write(context.Background(), websocket.MessageText, []byte(`{"usageMetadata":{}}`))
			if event = awaitContinuity(t, session.Events()); event.Type != protocol.EventUsageObserved {
				t.Fatalf("lost usage occurrence %s", event.Type)
			}
			session.Close()
			_ = collectEvents(t, session)
			finals := 0
			for _, receipt := range p.telemetry.snapshot() {
				if receipt.SessionID != p.plan.SessionID || receipt.AttemptID != p.plan.AttemptID || receipt.Destination.Token != p.plan.Telemetry.Token {
					t.Fatal("renewal changed receipt authority")
				}
				if receipt.Name == "usage.finalized" {
					finals++
					if !strings.Contains(string(receipt.Data), `"quantity_millis":10`) {
						t.Fatalf("usage reset across renewal: %s", receipt.Data)
					}
				}
			}
			if finals != 1 {
				t.Fatalf("final receipt count %d", finals)
			}
			p.mu.Lock()
			defer p.mu.Unlock()
			if p.mints != 1 || p.sockets != 2 {
				t.Fatalf("mints=%d sockets=%d", p.mints, p.sockets)
			}
			if p.plan.Route.Credential.Value != "root-token" || !p.plan.ExpiresAt.Equal(p.plan.Reservation.LeaseExpiresAt) {
				t.Fatal("mutated signed original plan")
			}
		})
	}
}

func TestEngineOwnedContinuationResponseLossDetachesOldSocketAndReplaysOneMint(t *testing.T) {
	p, e, c := newContinuityPeer(t, time.Second, 0)
	p.loseResponse = true
	p.renewalRelease = make(chan struct{})
	session := openContinuity(t, p, e, c)
	_ = awaitContinuity(t, p.setups)
	_ = awaitContinuity(t, p.connected)
	_ = awaitContinuity(t, p.renewalStarted)
	_ = awaitContinuity(t, p.renewalStarted)
	if token := awaitContinuity(t, p.closed); token != "root-token" {
		t.Fatalf("expired wrong socket %s", token)
	}
	select {
	case <-session.Done():
		t.Fatal("abandoned committed response recovery")
	default:
	}
	close(p.renewalRelease)
	_ = awaitContinuity(t, p.setups)
	_ = awaitContinuity(t, p.connected)
	if err := session.CommitAudio(); err != nil {
		t.Fatal(err)
	}
	_ = awaitContinuity(t, p.inputs)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mints != 1 || p.calls != 2 || p.sockets != 2 {
		t.Fatalf("calls=%d mints=%d sockets=%d", p.calls, p.mints, p.sockets)
	}
}

func TestEngineOwnedContinuationExplicitRefusalKeepsOriginalHardExpiry(t *testing.T) {
	p, e, c := newContinuityPeer(t, time.Second, 0)
	p.refuse = true
	session := openContinuity(t, p, e, c)
	_ = awaitContinuity(t, p.setups)
	_ = awaitContinuity(t, p.connected)
	_ = awaitContinuity(t, p.renewalStarted)
	_ = awaitContinuity(t, session.Done())
	_ = awaitContinuity(t, p.closed)
	if !errors.Is(session.Err(), runtimepkg.ErrSessionLeaseExpired) {
		t.Fatalf("error %v", session.Err())
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.calls != 1 || p.mints != 0 || p.sockets != 1 {
		t.Fatalf("refusal minted or retried: %d/%d/%d", p.calls, p.mints, p.sockets)
	}
}

func TestEngineGoogleClockChangePausesTrafficAndReplaysProviderEvents(t *testing.T) {
	p, e, c := newContinuityPeer(t, 25*time.Second, 0, true)
	p.clockRelease = make(chan struct{})
	p.clockStarted = make(chan struct{}, 4)
	session := openContinuity(t, p, e, c)
	_ = awaitContinuity(t, p.setups)
	conn := awaitContinuity(t, p.connected)
	p.wallOffset.Store(int64(time.Minute))
	if err := session.SubmitAudio(runtimepkg.AudioInput{Data: []byte{1, 0}}); err != nil {
		t.Fatal(err)
	}
	_ = awaitContinuity(t, p.clockStarted)
	_ = conn.Write(context.Background(), websocket.MessageText, []byte(`{"serverContent":{"inputTranscription":{"text":"retained while clock synchronizes"}}}`))
	select {
	case <-p.inputs:
		t.Fatal("unverified clock allowed provider I/O")
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case <-session.Events():
		t.Fatal("unverified clock forwarded provider output")
	default:
	}
	close(p.clockRelease)
	_ = awaitContinuity(t, p.inputs)
	if event := awaitContinuity(t, session.Events()); event.Type != protocol.EventTranscriptFinal {
		t.Fatalf("event lost %s", event.Type)
	}
	if p.clockCalls.Load() != 2 {
		t.Fatalf("clock requests were not shared: %d", p.clockCalls.Load())
	}
}

func TestEngineOwnedContinuationAbortFencesLateFundedResponse(t *testing.T) {
	p, e, c := newContinuityPeer(t, 12*time.Second, 0)
	p.renewalRelease = make(chan struct{})
	session := openContinuity(t, p, e, c)
	_ = awaitContinuity(t, p.setups)
	_ = awaitContinuity(t, p.connected)
	_ = awaitContinuity(t, p.renewalStarted)
	session.Abort()
	_ = awaitContinuity(t, session.Done())
	close(p.renewalRelease)
	_ = awaitContinuity(t, p.closed)
	select {
	case <-p.setups:
		t.Fatal("late response reopened canceled session")
	case <-time.After(150 * time.Millisecond):
	}
	if !errors.Is(session.Err(), runtimepkg.ErrSessionAborted) {
		t.Fatalf("terminal %v", session.Err())
	}
}

func TestEngineManagedGoogleDoesNotMintOrResumeWithoutProviderBound(t *testing.T) {
	p, e, c := newContinuityPeer(t, time.Second, 0, true)
	session := openContinuity(t, p, e, c)
	_ = awaitContinuity(t, p.setups)
	_ = awaitContinuity(t, p.connected)
	_ = awaitContinuity(t, session.Done())
	_ = awaitContinuity(t, p.closed)
	if !errors.Is(session.Err(), runtimepkg.ErrSessionLeaseExpired) {
		t.Fatalf("terminal error %v", session.Err())
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mints != 0 || p.calls != 0 || p.sockets != 1 {
		t.Fatalf("unsupported managed resumption activated: calls=%d mints=%d sockets=%d", p.calls, p.mints, p.sockets)
	}
}

func TestEngineOwnedContinuationRejectsExpiredDeliveryAndFailedSetup(t *testing.T) {
	for _, mode := range []string{"expired_delivery", "failed_setup"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			p, e, c := newContinuityPeer(t, time.Second, 0)
			if mode == "expired_delivery" {
				p.renewalRelease = make(chan struct{})
			} else {
				p.setupRefusal = true
			}
			session := openContinuity(t, p, e, c)
			_ = awaitContinuity(t, p.setups)
			_ = awaitContinuity(t, p.connected)
			_ = awaitContinuity(t, p.renewalStarted)
			if mode == "expired_delivery" {
				_ = awaitContinuity(t, p.closed)
				// The independently funded credential expires five seconds after root
				// issuance. A held ACK cannot turn it into another provider connection.
				timer := time.NewTimer(time.Until(p.plan.Reservation.Billing.RenewableUntil.Add(20 * time.Millisecond)))
				<-timer.C
				close(p.renewalRelease)
			}
			_ = awaitContinuity(t, session.Done())
			if session.Err() == nil {
				t.Fatal("failed continuation became clean completion")
			}
			p.mu.Lock()
			defer p.mu.Unlock()
			want := 1
			if mode == "failed_setup" {
				want = 2
			}
			if p.sockets != want || p.mints != 1 {
				t.Fatalf("sockets=%d mints=%d", p.sockets, p.mints)
			}
		})
	}
}

func TestEngineOwnedContinuationDoesNotResetOrExceedFundedAllowance(t *testing.T) {
	p, e, c := newContinuityPeer(t, 12*time.Second, 0)
	session := openContinuity(t, p, e, c)
	_ = awaitContinuity(t, p.setups)
	_ = awaitContinuity(t, p.connected)
	_ = awaitContinuity(t, p.setups)
	_ = awaitContinuity(t, p.connected)
	// Exactly the original twelve seconds plus the accepted four-second slice.
	if err := session.SubmitAudio(runtimepkg.AudioInput{Data: make([]byte, 16*32000)}); err != nil {
		t.Fatal(err)
	}
	_ = awaitContinuity(t, p.inputs)
	var released atomic.Int32
	if err := session.SubmitAudio(runtimepkg.AudioInput{Data: []byte{1, 0}, Release: func() { released.Add(1) }}); err != nil {
		t.Fatal(err)
	}
	_ = awaitContinuity(t, session.Done())
	if !errors.Is(session.Err(), runtimepkg.ErrUsageLimitExceeded) || released.Load() != 1 {
		t.Fatalf("allowance error=%v releases=%d", session.Err(), released.Load())
	}
}

func TestEngineGoogleHardDeadlineSurvivesPendingClockRefresh(t *testing.T) {
	p, e, c := newContinuityPeer(t, time.Second, 0, true)
	p.clockRelease = make(chan struct{})
	p.clockStarted = make(chan struct{}, 2)
	session := openContinuity(t, p, e, c)
	_ = awaitContinuity(t, p.setups)
	_ = awaitContinuity(t, p.connected)
	p.wallOffset.Store(int64(-time.Minute))
	released := make(chan struct{}, 1)
	if err := session.SubmitAudio(runtimepkg.AudioInput{Data: []byte{1, 0}, Release: func() { released <- struct{}{} }}); err != nil {
		t.Fatal(err)
	}
	_ = awaitContinuity(t, p.clockStarted)
	_ = awaitContinuity(t, session.Done())
	_ = awaitContinuity(t, p.closed)
	close(p.clockRelease)
	if !errors.Is(session.Err(), runtimepkg.ErrSessionLeaseExpired) {
		t.Fatalf("pending clock suspended hard expiry: %v", session.Err())
	}
	select {
	case <-p.inputs:
		t.Fatal("expired socket accepted input")
	default:
	}
	_ = awaitContinuity(t, released)
}

func TestEngineOwnedContinuationGracefulCloseDrainsAndFencesRenewal(t *testing.T) {
	p, e, c := newContinuityPeer(t, 12*time.Second, 0)
	p.renewalRelease = make(chan struct{})
	session := openContinuity(t, p, e, c)
	_ = awaitContinuity(t, p.setups)
	_ = awaitContinuity(t, p.connected)
	_ = awaitContinuity(t, p.renewalStarted)
	if err := session.SubmitAudio(runtimepkg.AudioInput{Data: []byte{1, 0}}); err != nil {
		t.Fatal(err)
	}
	session.Close()
	_ = awaitContinuity(t, p.inputs)
	_ = awaitContinuity(t, session.Done())
	_ = awaitContinuity(t, p.closed)
	close(p.renewalRelease)
	if session.Err() != nil {
		t.Fatalf("graceful close %v", session.Err())
	}
	select {
	case <-p.setups:
		t.Fatal("late renewal reopened gracefully closed session")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestEngineGoogleClockFailureRetainsSQLReceiptTime(t *testing.T) {
	p, e, c := newContinuityPeer(t, 25*time.Second, 0, true)
	p.clockRefusal = true
	session := openContinuity(t, p, e, c)
	_ = awaitContinuity(t, p.setups)
	_ = awaitContinuity(t, p.connected)
	p.wallOffset.Store(int64(time.Hour))
	if err := session.SubmitAudio(runtimepkg.AudioInput{Data: []byte{1, 0}}); err != nil {
		t.Fatal(err)
	}
	_ = awaitContinuity(t, session.Done())
	_ = awaitContinuity(t, p.closed)
	if session.Err() == nil {
		t.Fatal("clock failure became clean completion")
	}
	for _, receipt := range p.telemetry.snapshot() {
		if receipt.At.After(time.Now().Add(time.Second)) {
			t.Fatal("wall-clock jump corrupted SQL-aligned receipt time")
		}
	}
	select {
	case <-p.inputs:
		t.Fatal("clock refusal allowed provider input")
	default:
	}
}

func TestEngineOwnedContinuationKeepsAnActiveResponseAcrossSockets(t *testing.T) {
	p, e, c := newContinuityPeer(t, 12*time.Second, 0)
	p.renewalRelease = make(chan struct{})
	session := openContinuity(t, p, e, c)
	_ = awaitContinuity(t, p.setups)
	conn := awaitContinuity(t, p.connected)
	_ = awaitContinuity(t, p.renewalStarted)
	_ = conn.Write(context.Background(), websocket.MessageText, []byte(`{"serverContent":{"outputTranscription":{"text":"before"}}}`))
	if event := awaitContinuity(t, session.Events()); event.Type != protocol.EventResponseStarted {
		t.Fatalf("start %s", event.Type)
	}
	if event := awaitContinuity(t, session.Events()); event.Type != protocol.EventTextDelta {
		t.Fatalf("delta %s", event.Type)
	}
	close(p.renewalRelease)
	_ = awaitContinuity(t, p.setups)
	conn = awaitContinuity(t, p.connected)
	_ = conn.Write(context.Background(), websocket.MessageText, []byte(`{"serverContent":{"outputTranscription":{"text":"after"},"turnComplete":true}}`))
	for _, want := range []protocol.EventType{protocol.EventTextDelta, protocol.EventResponseDone} {
		if event := awaitContinuity(t, session.Events()); event.Type != want {
			t.Fatalf("response state reset: got %s want %s", event.Type, want)
		}
	}
}
