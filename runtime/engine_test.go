package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	"github.com/SpekoAI/gateway/providers/mock"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

var fixedNow = time.Date(2026, time.August, 1, 11, 59, 0, 0, time.UTC)

func TestSTTSessionPreservesInputOwnershipAndOrdering(t *testing.T) {
	t.Parallel()

	adapter := mock.NewSTTAdapter("mock.stt.v1")
	engine := newEngine(t, adapter, runtimepkg.DefaultLimits(), runtimepkg.NopTelemetry{})
	session := openSession(t, engine, protocol.SessionKindSTT, adapter.ID())

	audio := []byte{1, 2, 3, 4}
	var releases atomic.Int32
	if err := session.SubmitAudio(runtimepkg.AudioInput{Data: audio, Release: func() { releases.Add(1) }}); err != nil {
		t.Fatalf("submit audio: %v", err)
	}
	if err := session.CommitAudio(); err != nil {
		t.Fatalf("commit audio: %v", err)
	}
	session.Close()

	events := collectEvents(t, session)
	if err := session.Err(); err != nil {
		t.Fatalf("session failed: %v", err)
	}
	if got := releases.Load(); got != 1 {
		t.Fatalf("release calls = %d, want 1", got)
	}
	assertTypes(t, events, []protocol.EventType{
		protocol.EventSessionReady,
		protocol.EventSpeechStarted,
		protocol.EventTranscriptDelta,
		protocol.EventSpeechEnded,
		protocol.EventTranscriptFinal,
		protocol.EventSessionClosed,
	})

	calls := adapter.LastStream().Calls()
	if got := []string{calls[0].Kind, calls[1].Kind, calls[2].Kind}; !reflect.DeepEqual(got, []string{"audio", "audio.commit", "session.close"}) {
		t.Fatalf("provider call order = %v", got)
	}
	if &calls[0].Audio[0] != &audio[0] {
		t.Fatal("runtime copied the accepted audio buffer")
	}
	assertEventEnvelope(t, events, "sess_test", "att_1")
}

func TestTTSSessionEmitsBinaryAudioWithoutProviderLogicInEngine(t *testing.T) {
	t.Parallel()

	adapter := mock.NewTTSAdapter("mock.tts.v1")
	engine := newEngine(t, adapter, runtimepkg.DefaultLimits(), runtimepkg.NopTelemetry{})
	session := openSession(t, engine, protocol.SessionKindTTS, adapter.ID())

	if err := session.AppendText("hello"); err != nil {
		t.Fatalf("append text: %v", err)
	}
	if err := session.CommitText(); err != nil {
		t.Fatalf("commit text: %v", err)
	}
	session.Close()

	events := collectEvents(t, session)
	assertTypes(t, events, []protocol.EventType{
		protocol.EventSessionReady,
		protocol.EventAudioStarted,
		protocol.EventAudioFrame,
		protocol.EventAudioDone,
		protocol.EventSessionClosed,
	})
	if got := string(events[2].Audio); got != "mock-audio:hello" {
		t.Fatalf("audio = %q", got)
	}
}

func TestTTSSessionEnforcesFixedUnicodeCharacterAllowance(t *testing.T) {
	t.Parallel()

	adapter := mock.NewTTSAdapter("mock.limited.tts")
	telemetry := &collectingTelemetry{}
	engine := newEngine(t, adapter, runtimepkg.DefaultLimits(), telemetry)
	plan := validPlan(protocol.SessionKindTTS, adapter.ID(), 60)
	plan.Reservation.Usage.AuthorizedUnits = 3
	plan.Execution.CredentialSource = protocol.CredentialsManaged
	plan.Route.Credential = &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "short-lived", ExpiresAt: fixedNow.Add(time.Minute)}
	session, err := engine.Open(context.Background(), runtimepkg.OpenRequest{
		Kind: protocol.SessionKindTTS, Plan: plan,
		Media: &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
	})
	if err != nil {
		t.Fatalf("open TTS session: %v", err)
	}
	if err := session.AppendText("é😊"); err != nil {
		t.Fatalf("append two Unicode characters: %v", err)
	}
	if err := session.AppendText("x"); err != nil {
		t.Fatalf("append final authorized character: %v", err)
	}
	if err := session.AppendText("y"); !errors.Is(err, runtimepkg.ErrUsageLimitExceeded) {
		t.Fatalf("over-limit append = %v", err)
	}
	session.Close()
	collectEvents(t, session)
	assertUsageReported(t, telemetry.snapshot(), protocol.UsageUnitCharacters, 3_000, true)
}

func TestSTTSessionReportsAcceptedPCMDurationFromCompleteSamples(t *testing.T) {
	t.Parallel()
	adapter := mock.NewSTTAdapter("mock.usage-duration.stt")
	telemetry := &collectingTelemetry{}
	engine := newEngine(t, adapter, runtimepkg.DefaultLimits(), telemetry)
	plan := validPlan(protocol.SessionKindSTT, adapter.ID(), 60)
	plan.Execution.CredentialSource = protocol.CredentialsManaged
	plan.Route.Credential = &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "short-lived", ExpiresAt: fixedNow.Add(time.Minute)}
	session, err := engine.Open(context.Background(), runtimepkg.OpenRequest{
		Kind: protocol.SessionKindSTT, Plan: plan,
		Media: &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
	})
	if err != nil {
		t.Fatalf("open STT session: %v", err)
	}
	if err := session.SubmitAudio(runtimepkg.AudioInput{Data: make([]byte, 32_001)}); err != nil {
		t.Fatalf("submit PCM: %v", err)
	}
	session.Close()
	collectEvents(t, session)
	assertUsageReported(t, telemetry.snapshot(), protocol.UsageUnitDurationSeconds, 1_000, true)
}

func TestEngineInjectsBYOKCredentialOnlyIntoAdapterRequest(t *testing.T) {
	t.Parallel()

	opened := make(chan runtimepkg.AdapterRequest, 1)
	adapter := mock.NewAdapter("mock.byok.tts", func(request runtimepkg.AdapterRequest) *mock.Stream {
		opened <- request
		return mock.NewStream(4)
	})
	engine, err := runtimepkg.New(runtimepkg.Config{
		Adapters: []runtimepkg.Adapter{adapter},
		Verifier: runtimepkg.PlanVerifierFunc(func(context.Context, protocol.SessionPlan) error { return nil }),
		LocalCredentials: map[string]runtimepkg.LocalCredential{
			"cartesia": {Kind: protocol.CredentialBearer, Value: "customer-owned-key"},
		},
		Now: func() time.Time { return fixedNow },
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	plan := validPlan(protocol.SessionKindTTS, adapter.ID(), 60)
	plan.Execution.CredentialSource = protocol.CredentialsBYOK
	plan.Route.Provider = "cartesia"
	plan.Route.Credential = nil
	media := &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1}
	session, err := engine.Open(context.Background(), runtimepkg.OpenRequest{Kind: protocol.SessionKindTTS, Plan: plan, Media: media})
	if err != nil {
		t.Fatalf("open BYOK session: %v", err)
	}
	request := <-opened
	if request.Plan.Route.Credential == nil || request.Plan.Route.Credential.Value != "customer-owned-key" {
		t.Fatalf("adapter credential = %v, want locally installed key", request.Plan.Route.Credential)
	}
	if plan.Route.Credential != nil {
		t.Fatal("engine mutated the verified BYOK plan")
	}
	session.Close()
	collectEvents(t, session)
}

func TestEngineUsesDelegatedCredentialForManagedRoute(t *testing.T) {
	t.Parallel()

	opened := make(chan runtimepkg.AdapterRequest, 1)
	adapter := mock.NewAdapter("mock.managed.tts", func(request runtimepkg.AdapterRequest) *mock.Stream {
		opened <- request
		return mock.NewStream(4)
	})
	telemetry := &collectingTelemetry{}
	engine := newEngine(t, adapter, runtimepkg.DefaultLimits(), telemetry)
	plan := validPlan(protocol.SessionKindTTS, adapter.ID(), 60)
	plan.Execution.CredentialSource = protocol.CredentialsManaged
	plan.Route.Credential = &protocol.DelegatedCredential{
		Kind: protocol.CredentialBearer, Value: "short-lived-managed-token", ExpiresAt: fixedNow.Add(30 * time.Minute),
	}
	media := &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1}
	session, err := engine.Open(context.Background(), runtimepkg.OpenRequest{Kind: protocol.SessionKindTTS, Plan: plan, Media: media})
	if err != nil {
		t.Fatalf("open managed session: %v", err)
	}
	request := <-opened
	if request.Plan.Route.Credential == nil || request.Plan.Route.Credential.Value != "short-lived-managed-token" {
		t.Fatalf("adapter credential = %v, want delegated managed token", request.Plan.Route.Credential)
	}
	if request.Plan.Route.Credential.Value == "customer-mock-key" {
		t.Fatal("managed route was overwritten with local BYOK credential")
	}
	session.Close()
	collectEvents(t, session)
	byName := make(map[string]runtimepkg.TelemetryEvent)
	for _, event := range telemetry.snapshot() {
		byName[event.Name] = event
	}
	if !byName["session.closed"].Required || byName["session.opened"].Required {
		t.Fatalf("managed telemetry requirement flags = %+v", byName)
	}
}

func TestEngineFailsClosedWithoutBYOKCredential(t *testing.T) {
	t.Parallel()

	adapter := mock.NewTTSAdapter("mock.byok.tts")
	engine := newEngine(t, adapter, runtimepkg.DefaultLimits(), runtimepkg.NopTelemetry{})
	plan := validPlan(protocol.SessionKindTTS, adapter.ID(), 60)
	plan.Execution.CredentialSource = protocol.CredentialsBYOK
	plan.Route.Provider = "cartesia"
	plan.Route.Credential = nil
	media := &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1}
	if _, err := engine.Open(context.Background(), runtimepkg.OpenRequest{Kind: protocol.SessionKindTTS, Plan: plan, Media: media}); err == nil {
		t.Fatal("BYOK session without a local credential was accepted")
	}
}

func TestInputBackpressureRetainsRejectedAudioAndReleasesAcceptedAudio(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	allowWrite := make(chan struct{})
	adapter := mock.NewAdapter("mock.blocking.stt", func(_ runtimepkg.AdapterRequest) *mock.Stream {
		stream := mock.NewStream(8)
		stream.WriteAudioHook = func(_ context.Context, _ []byte) error {
			select {
			case <-started:
			default:
				close(started)
			}
			<-allowWrite
			return nil
		}
		return stream
	})
	limits := runtimepkg.Limits{MaxInputMessages: 1, MaxInputBytes: 8_000, MaxOutputEvents: 8}
	telemetry := &collectingTelemetry{}
	engine := newEngine(t, adapter, limits, telemetry)
	session := openSession(t, engine, protocol.SessionKindSTT, adapter.ID())

	var firstReleased, secondReleased, rejectedReleased atomic.Int32
	if err := session.SubmitAudio(runtimepkg.AudioInput{Data: make([]byte, 8_000), Release: func() { firstReleased.Add(1) }}); err != nil {
		t.Fatalf("first submit: %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("provider did not receive first frame")
	}
	if err := session.SubmitAudio(runtimepkg.AudioInput{Data: make([]byte, 8_000), Release: func() { secondReleased.Add(1) }}); err != nil {
		t.Fatalf("second submit: %v", err)
	}
	err := session.SubmitAudio(runtimepkg.AudioInput{Data: make([]byte, 8_000), Release: func() { rejectedReleased.Add(1) }})
	if !errors.Is(err, runtimepkg.ErrBackpressure) {
		t.Fatalf("third submit error = %v, want ErrBackpressure", err)
	}
	if got := rejectedReleased.Load(); got != 0 {
		t.Fatalf("rejected frame release calls = %d, want 0", got)
	}

	session.Close()
	close(allowWrite)
	collectEvents(t, session)
	if got := firstReleased.Load(); got != 1 {
		t.Fatalf("first frame release calls = %d, want 1", got)
	}
	if got := secondReleased.Load(); got != 1 {
		t.Fatalf("second frame release calls = %d, want 1", got)
	}
	assertUsageReported(t, telemetry.snapshot(), protocol.UsageUnitDurationSeconds, 500, false)
}

func TestSlowConsumerGetsTerminalBackpressureError(t *testing.T) {
	t.Parallel()

	adapter := mock.NewAdapter("mock.flood.stt", func(_ runtimepkg.AdapterRequest) *mock.Stream {
		stream := mock.NewStream(8)
		stream.WriteAudioHook = func(_ context.Context, _ []byte) error {
			if err := stream.Emit(runtimepkg.ProviderEvent{Type: protocol.EventTranscriptDelta}); err != nil {
				return err
			}
			return stream.Emit(runtimepkg.ProviderEvent{Type: protocol.EventTranscriptDelta})
		}
		return stream
	})
	limits := runtimepkg.Limits{MaxInputMessages: 4, MaxInputBytes: 16, MaxOutputEvents: 2}
	engine := newEngine(t, adapter, limits, runtimepkg.NopTelemetry{})
	session := openSession(t, engine, protocol.SessionKindSTT, adapter.ID())

	if err := session.SubmitAudio(runtimepkg.AudioInput{Data: []byte{1}}); err != nil {
		t.Fatalf("submit audio: %v", err)
	}
	if err := session.Wait(context.Background()); !errors.Is(err, runtimepkg.ErrOutputBackpressure) {
		t.Fatalf("session error = %v, want ErrOutputBackpressure", err)
	}
	events := collectEvents(t, session)
	if got := events[len(events)-1].Type; got != protocol.EventError {
		t.Fatalf("terminal event = %q, want error", got)
	}
}

func TestCancelIsOrderedAndTelemetryDropsNeverFailSession(t *testing.T) {
	t.Parallel()

	adapter := mock.NewSTTAdapter("mock.cancel.stt")
	telemetry := rejectingTelemetry{}
	engine := newEngine(t, adapter, runtimepkg.DefaultLimits(), telemetry)
	session := openSession(t, engine, protocol.SessionKindSTT, adapter.ID())

	if err := session.SubmitAudio(runtimepkg.AudioInput{Data: []byte{1}}); err != nil {
		t.Fatalf("submit audio: %v", err)
	}
	if err := session.Cancel(); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	session.Close()
	events := collectEvents(t, session)
	if err := session.Err(); err != nil {
		t.Fatalf("session failed: %v", err)
	}
	if got := session.Stats().TelemetryDropped; got < 2 {
		t.Fatalf("dropped telemetry = %d, want at least 2", got)
	}
	assertTypes(t, events, []protocol.EventType{
		protocol.EventSessionReady,
		protocol.EventSpeechStarted,
		protocol.EventTranscriptDelta,
		protocol.EventResponseCanceled,
		protocol.EventSessionClosed,
	})
}

func TestAbortMakesSessionTerminalWithoutGracefulDrain(t *testing.T) {
	t.Parallel()

	adapter := mock.NewSTTAdapter("mock.abort.stt")
	engine := newEngine(t, adapter, runtimepkg.DefaultLimits(), runtimepkg.NopTelemetry{})
	session := openSession(t, engine, protocol.SessionKindSTT, adapter.ID())

	session.Abort()
	if err := session.Wait(context.Background()); !errors.Is(err, runtimepkg.ErrSessionAborted) {
		t.Fatalf("abort error = %v, want ErrSessionAborted", err)
	}
	if err := session.SubmitAudio(runtimepkg.AudioInput{Data: []byte{1}}); !errors.Is(err, runtimepkg.ErrSessionClosed) {
		t.Fatalf("submit after abort error = %v, want ErrSessionClosed", err)
	}
	events := collectEvents(t, session)
	assertTypes(t, events, []protocol.EventType{protocol.EventSessionReady, protocol.EventError})
}

func TestSessionExpiresWhenRenewableLeaseIsNotExtended(t *testing.T) {
	t.Parallel()

	adapter := mock.NewSTTAdapter("mock.reservation.stt")
	engine := newEngine(t, adapter, runtimepkg.DefaultLimits(), runtimepkg.NopTelemetry{})
	session := openSessionWithReservation(t, engine, protocol.SessionKindSTT, adapter.ID(), 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := session.Wait(ctx); !errors.Is(err, runtimepkg.ErrSessionLifetimeExceeded) {
		t.Fatalf("reservation expiry error = %v, want ErrSessionLifetimeExceeded", err)
	}
	events := collectEvents(t, session)
	assertTypes(t, events, []protocol.EventType{protocol.EventSessionReady, protocol.EventError})
	var data struct {
		Code      string `json:"code"`
		Source    string `json:"source"`
		Retryable bool   `json:"retryable"`
	}
	if err := json.Unmarshal(events[len(events)-1].Data, &data); err != nil {
		t.Fatalf("decode lifetime error: %v", err)
	}
	if data.Code != "session_lease_expired" || data.Source != "runtime" || !data.Retryable {
		t.Fatalf("terminal data = %+v", data)
	}
}

func TestLeaseRenewalKeepsSameProviderStreamPastPlanAndInitialLease(t *testing.T) {
	t.Parallel()

	var opens atomic.Int32
	adapter := mock.NewAdapter("mock.renewable.stt", func(_ runtimepkg.AdapterRequest) *mock.Stream {
		opens.Add(1)
		return mock.NewStream(8)
	})
	engine := newEngine(t, adapter, runtimepkg.DefaultLimits(), runtimepkg.NopTelemetry{})
	plan := validPlan(protocol.SessionKindSTT, adapter.ID(), 1)
	plan.ExpiresAt = fixedNow.Add(40 * time.Millisecond)
	plan.Reservation.LeaseExpiresAt = fixedNow.Add(80 * time.Millisecond)
	session, err := engine.Open(context.Background(), runtimepkg.OpenRequest{
		Kind: protocol.SessionKindSTT, Plan: plan,
		Media: &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
	})
	if err != nil {
		t.Fatalf("open renewable session: %v", err)
	}
	if err := session.RenewLease(protocol.SessionLease{
		ReservationID: plan.Reservation.ID, SessionID: plan.SessionID, AttemptID: plan.AttemptID,
		ConcurrencyLeaseID: plan.Reservation.Concurrency.LeaseID, Sequence: 1,
		ExpiresAt: fixedNow.Add(250 * time.Millisecond), RenewAfter: fixedNow.Add(180 * time.Millisecond),
	}); err != nil {
		t.Fatalf("renew lease: %v", err)
	}
	time.Sleep(130 * time.Millisecond) // beyond both the grant/plan and first lease
	select {
	case <-session.Done():
		t.Fatalf("renewed session ended early: %v", session.Err())
	default:
	}
	if got := opens.Load(); got != 1 {
		t.Fatalf("provider opens = %d, want the original stream only", got)
	}
	session.Close()
	collectEvents(t, session)
}

func TestEngineRequiresVerifier(t *testing.T) {
	t.Parallel()

	_, err := runtimepkg.New(runtimepkg.Config{})
	if !errors.Is(err, runtimepkg.ErrPlanUnverified) {
		t.Fatalf("New error = %v, want ErrPlanUnverified", err)
	}
}

func TestProviderFailureBecomesStructuredProviderError(t *testing.T) {
	t.Parallel()

	adapter := mock.NewAdapter("mock.failing.stt", func(_ runtimepkg.AdapterRequest) *mock.Stream {
		stream := mock.NewStream(4)
		if err := stream.Emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{
			Code:           "provider_rate_limited",
			Message:        "provider limited this request",
			Retryable:      true,
			ProviderStatus: 429,
			Extensions:     map[string]json.RawMessage{"provider.test/v1": json.RawMessage(`{"status_code":429}`)},
		}}); err != nil {
			t.Fatalf("emit provider failure: %v", err)
		}
		return stream
	})
	engine := newEngine(t, adapter, runtimepkg.DefaultLimits(), runtimepkg.NopTelemetry{})
	session := openSession(t, engine, protocol.SessionKindSTT, adapter.ID())
	if err := session.Wait(context.Background()); err == nil {
		t.Fatal("expected provider failure")
	}
	events := collectEvents(t, session)
	if got := events[len(events)-1].Type; got != protocol.EventError {
		t.Fatalf("terminal event = %q, want error", got)
	}
	var data struct {
		Code           string `json:"code"`
		Source         string `json:"source"`
		Retryable      bool   `json:"retryable"`
		ProviderStatus int    `json:"provider_status"`
	}
	if err := json.Unmarshal(events[len(events)-1].Data, &data); err != nil {
		t.Fatalf("decode terminal error: %v", err)
	}
	if data.Code != "provider_rate_limited" || data.Source != "provider" || !data.Retryable || data.ProviderStatus != 429 {
		t.Fatalf("terminal data = %+v", data)
	}
	if events[len(events)-1].Extensions["provider.test/v1"] == nil {
		t.Fatal("terminal provider error omitted its local raw extension")
	}
}

func TestEngineRecordsUsageObservedAndOpenLatencyTelemetry(t *testing.T) {
	t.Parallel()

	adapter := mock.NewAdapter("mock.usage.stt", func(_ runtimepkg.AdapterRequest) *mock.Stream {
		stream := mock.NewStream(4)
		if err := stream.Emit(runtimepkg.ProviderEvent{
			Type: protocol.EventUsageObserved,
			Data: json.RawMessage(`{"provider_request_id":"dg-request-1"}`),
		}); err != nil {
			t.Fatalf("emit usage event: %v", err)
		}
		return stream
	})
	telemetry := &collectingTelemetry{}
	engine := newEngine(t, adapter, runtimepkg.DefaultLimits(), telemetry)
	session := openSession(t, engine, protocol.SessionKindSTT, adapter.ID())
	session.Close()
	if err := session.Wait(context.Background()); err != nil {
		t.Fatalf("session failed: %v", err)
	}
	collectEvents(t, session)
	recorded := telemetry.snapshot()
	byName := map[string]runtimepkg.TelemetryEvent{}
	for _, event := range recorded {
		byName[event.Name] = event
	}
	opened, ok := byName["session.opened"]
	if !ok || opened.EventID == "" || opened.Destination.Endpoint == "" || opened.Destination.Token == "" {
		t.Fatalf("session.opened telemetry = %+v", opened)
	}
	var latency struct {
		ProviderOpenMS *int64 `json:"provider_open_ms"`
	}
	if err := json.Unmarshal(opened.Data, &latency); err != nil || latency.ProviderOpenMS == nil {
		t.Fatalf("session.opened data = %s, err=%v; want provider_open_ms", opened.Data, err)
	}
	usage, ok := byName["usage.observed"]
	if !ok || string(usage.Data) != `{"provider_request_id":"dg-request-1"}` {
		t.Fatalf("usage.observed telemetry = %+v", usage)
	}
	if _, ok := byName["session.closed"]; !ok {
		t.Fatalf("recorded names = %v, want session.closed", names(recorded))
	}
}

func TestTelemetryExcludesProviderContentAndCredentials(t *testing.T) {
	t.Parallel()

	adapter := mock.NewAdapter("mock.private.stt", func(_ runtimepkg.AdapterRequest) *mock.Stream {
		stream := mock.NewStream(8)
		for _, event := range []runtimepkg.ProviderEvent{
			{Type: protocol.EventTranscriptFinal, Data: json.RawMessage(`{"text":"private transcript"}`)},
			{Type: protocol.EventAudioFrame, Audio: []byte("private audio")},
			{Type: protocol.EventUsageObserved, Data: json.RawMessage(`{"provider_request_id":"provider-request-1"}`)},
		} {
			if err := stream.Emit(event); err != nil {
				t.Fatalf("emit provider event: %v", err)
			}
		}
		return stream
	})
	telemetry := &collectingTelemetry{}
	engine := newEngine(t, adapter, runtimepkg.DefaultLimits(), telemetry)
	session := openSession(t, engine, protocol.SessionKindSTT, adapter.ID())
	session.Close()
	collectEvents(t, session)

	encoded, err := json.Marshal(telemetry.snapshot())
	if err != nil {
		t.Fatalf("marshal telemetry: %v", err)
	}
	for _, forbidden := range []string{"private transcript", "private audio", "customer-mock-key"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("telemetry leaked %q: %s", forbidden, encoded)
		}
	}
	if !strings.Contains(string(encoded), "provider-request-1") {
		t.Fatalf("telemetry omitted provider correlation id: %s", encoded)
	}
}

func names(events []runtimepkg.TelemetryEvent) []string {
	result := make([]string, 0, len(events))
	for _, event := range events {
		result = append(result, event.Name)
	}
	return result
}

func assertUsageReported(t *testing.T, events []runtimepkg.TelemetryEvent, unit protocol.UsageUnit, quantityMillis int64, required bool) {
	t.Helper()
	count := 0
	for _, event := range events {
		if event.Name != "usage.reported" {
			continue
		}
		count++
		var payload struct {
			Unit           protocol.UsageUnit `json:"unit"`
			QuantityMillis int64              `json:"quantity_millis"`
		}
		if err := json.Unmarshal(event.Data, &payload); err != nil || payload.Unit != unit || payload.QuantityMillis != quantityMillis || event.Required != required {
			t.Fatalf("usage.reported = %+v payload=%+v err=%v", event, payload, err)
		}
	}
	if count != 1 {
		t.Fatalf("usage.reported count = %d, events=%v", count, names(events))
	}
}

type collectingTelemetry struct {
	mu     sync.Mutex
	events []runtimepkg.TelemetryEvent
}

func (c *collectingTelemetry) TryRecord(event runtimepkg.TelemetryEvent) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, event)
	return true
}

func (c *collectingTelemetry) snapshot() []runtimepkg.TelemetryEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]runtimepkg.TelemetryEvent(nil), c.events...)
}

type rejectingTelemetry struct{}

func (rejectingTelemetry) TryRecord(runtimepkg.TelemetryEvent) bool { return false }

func newEngine(t *testing.T, adapter runtimepkg.Adapter, limits runtimepkg.Limits, telemetry runtimepkg.TelemetrySink) *runtimepkg.Engine {
	t.Helper()
	engine, err := runtimepkg.New(runtimepkg.Config{
		Adapters:         []runtimepkg.Adapter{adapter},
		Verifier:         runtimepkg.PlanVerifierFunc(func(context.Context, protocol.SessionPlan) error { return nil }),
		LocalCredentials: map[string]runtimepkg.LocalCredential{"mock": {Kind: protocol.CredentialBearer, Value: "customer-mock-key"}},
		Telemetry:        telemetry,
		Limits:           limits,
		Now:              func() time.Time { return fixedNow },
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	return engine
}

func openSession(t *testing.T, engine *runtimepkg.Engine, kind protocol.SessionKind, adapterID string) *runtimepkg.Session {
	return openSessionWithReservation(t, engine, kind, adapterID, 60)
}

func openSessionWithReservation(t *testing.T, engine *runtimepkg.Engine, kind protocol.SessionKind, adapterID string, leaseDurationSeconds int) *runtimepkg.Session {
	t.Helper()
	plan := validPlan(kind, adapterID, leaseDurationSeconds)
	media := &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1}
	if kind == protocol.SessionKindLLM {
		media = nil
	}
	session, err := engine.Open(context.Background(), runtimepkg.OpenRequest{Kind: kind, Plan: plan, Media: media})
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	return session
}

func validPlan(kind protocol.SessionKind, adapterID string, leaseDurationSeconds int) protocol.SessionPlan {
	usage := protocol.UsageReservation{Unit: protocol.UsageUnitDurationSeconds, AuthorizedUnits: int64(leaseDurationSeconds)}
	if kind == protocol.SessionKindTTS {
		usage = protocol.UsageReservation{Unit: protocol.UsageUnitCharacters, AuthorizedUnits: 4_000}
	}
	return protocol.SessionPlan{
		PlanID:    "plan_test",
		SessionID: "sess_test",
		AttemptID: "att_1",
		Execution: protocol.Execution{
			Placement:        protocol.PlacementEmbedded,
			ProviderRoute:    protocol.RouteProviderDirect,
			CredentialSource: protocol.CredentialsBYOK,
		},
		ExpiresAt: fixedNow.Add(time.Hour),
		Route: protocol.PlanRoute{
			Provider:  "mock",
			Model:     "mock-model",
			Adapter:   adapterID,
			Transport: protocol.TransportWebSocket,
			Endpoint:  "wss://mock.speko.test/session",
		},
		Reservation: protocol.Reservation{ID: "res_test", LeaseDurationSeconds: leaseDurationSeconds, LeaseExpiresAt: fixedNow.Add(time.Duration(leaseDurationSeconds) * time.Second), RenewalURL: "https://control.speko.test/v1/sessions/sess_test/lease-renewals", Concurrency: protocol.ConcurrencyReservation{LeaseID: "conc_test", Slots: 1}, Usage: usage},
		Telemetry: protocol.Telemetry{
			Endpoint:        "https://control.speko.test/v1/runtime-events",
			Token:           "telemetry-token",
			FlushIntervalMS: 5_000,
		},
		Requirements: protocol.Requirements{
			Protocol:         protocol.VoiceV0,
			ProtocolRevision: protocol.CurrentRevision,
			RuntimeVersion:   "0.1.0",
		},
		Signature: "test-signature",
	}
}

func collectEvents(t *testing.T, session *runtimepkg.Session) []protocol.Event {
	t.Helper()
	events := make(chan []protocol.Event, 1)
	go func() {
		var collected []protocol.Event
		for event := range session.Events() {
			collected = append(collected, event)
		}
		events <- collected
	}()
	select {
	case collected := <-events:
		return collected
	case <-time.After(time.Second):
		t.Fatal("session events did not close")
		return nil
	}
}

func assertTypes(t *testing.T, events []protocol.Event, want []protocol.EventType) {
	t.Helper()
	got := make([]protocol.EventType, len(events))
	for index, event := range events {
		got[index] = event.Type
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("event types = %v, want %v", got, want)
	}
}

func assertEventEnvelope(t *testing.T, events []protocol.Event, sessionID, attemptID string) {
	t.Helper()
	for index, event := range events {
		if event.SessionID != sessionID || event.AttemptID != attemptID || event.Sequence != uint64(index+1) || event.EventID == "" {
			t.Fatalf("invalid event envelope at %d: %+v", index, event)
		}
	}
}

// A caller that sends `provider: "auto"` cannot know which vendor it will get,
// so it cannot send a voice id from that vendor's id space. Before the signed
// route carried a voice, `auto` plus TTS was unusable: every voice-taking
// adapter rejects an empty voice id.
func TestEngineFallsBackToThePlanVoiceOnlyWhenTheCallerSentNone(t *testing.T) {
	t.Parallel()

	opened := make(chan runtimepkg.AdapterRequest, 2)
	adapter := mock.NewAdapter("mock.voice.tts", func(request runtimepkg.AdapterRequest) *mock.Stream {
		opened <- request
		return mock.NewStream(4)
	})
	engine, err := runtimepkg.New(runtimepkg.Config{
		Adapters: []runtimepkg.Adapter{adapter},
		Verifier: runtimepkg.PlanVerifierFunc(func(context.Context, protocol.SessionPlan) error { return nil }),
		LocalCredentials: map[string]runtimepkg.LocalCredential{
			"mock": {Kind: protocol.CredentialBearer, Value: "customer-owned-key"},
		},
		Now: func() time.Time { return fixedNow },
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	media := &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1}
	open := func(options protocol.RequestOptions) protocol.RequestOptions {
		plan := validPlan(protocol.SessionKindTTS, adapter.ID(), 60)
		plan.Route.Voice = "plan-chosen-voice"
		session, err := engine.Open(context.Background(), runtimepkg.OpenRequest{
			Kind: protocol.SessionKindTTS, Plan: plan, Options: options, Media: media,
		})
		if err != nil {
			t.Fatalf("open TTS session: %v", err)
		}
		defer func() {
			session.Close()
			collectEvents(t, session)
		}()
		if plan.Route.Voice != "plan-chosen-voice" {
			t.Fatal("engine mutated the verified plan")
		}
		return (<-opened).Options
	}

	if got := open(protocol.RequestOptions{}).Voice; got != "plan-chosen-voice" {
		t.Fatalf("adapter voice with no caller voice = %q, want the plan's", got)
	}
	// Whitespace is not a choice: an adapter would reject it, so it is a blank.
	if got := open(protocol.RequestOptions{Voice: "  "}).Voice; got != "plan-chosen-voice" {
		t.Fatalf("adapter voice for a blank caller voice = %q, want the plan's", got)
	}
	if got := open(protocol.RequestOptions{Voice: "caller-chosen-voice"}).Voice; got != "caller-chosen-voice" {
		t.Fatalf("adapter voice = %q, want the caller's override to win", got)
	}
}
