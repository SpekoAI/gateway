package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/SpekoAI/gateway/protocol"
)

// Config configures an embedded Engine. Verifier is required because a runtime
// must not trust a customer-provided plan merely because it is well formed.
type Config struct {
	Adapters []Adapter
	Verifier PlanVerifier
	// LocalCredentials maps a signed provider name to a customer-owned BYOK
	// credential. The engine copies a matching credential only into the
	// ephemeral adapter request; the verified plan and session retain no secret.
	LocalCredentials map[string]LocalCredential
	Telemetry        TelemetrySink
	Limits           Limits
	Now              func() time.Time
}

// Engine opens provider-neutral sessions through registered provider adapters.
type Engine struct {
	adapters         map[string]Adapter
	verifier         PlanVerifier
	localCredentials map[string]LocalCredential
	telemetry        TelemetrySink
	limits           Limits
	now              func() time.Time
}

// New constructs an Engine with fixed per-session limits.
func New(config Config) (*Engine, error) {
	limits := config.Limits
	if limits == (Limits{}) {
		limits = DefaultLimits()
	}
	if err := limits.validate(); err != nil {
		return nil, fmt.Errorf("runtime limits: %w", err)
	}
	if config.Verifier == nil {
		return nil, ErrPlanUnverified
	}
	adapters := make(map[string]Adapter, len(config.Adapters))
	for _, adapter := range config.Adapters {
		if adapter == nil || strings.TrimSpace(adapter.ID()) == "" {
			return nil, errors.New("runtime adapter must have an id")
		}
		if _, exists := adapters[adapter.ID()]; exists {
			return nil, fmt.Errorf("duplicate runtime adapter %q", adapter.ID())
		}
		adapters[adapter.ID()] = adapter
	}
	if config.Telemetry == nil {
		config.Telemetry = NopTelemetry{}
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	localCredentials := make(map[string]LocalCredential, len(config.LocalCredentials))
	for provider, credential := range config.LocalCredentials {
		provider = strings.TrimSpace(provider)
		credential.Value = strings.TrimSpace(credential.Value)
		credential.ValueFile = strings.TrimSpace(credential.ValueFile)
		if provider == "" || credential.Kind == "" || (credential.Value == "") == (credential.ValueFile == "") {
			return nil, errors.New("runtime local credentials need a provider, kind, and exactly one value source")
		}
		if _, err := resolveLocalCredential(credential); err != nil {
			return nil, fmt.Errorf("runtime local credential for provider %q: %w", provider, err)
		}
		localCredentials[provider] = credential
	}
	return &Engine{
		adapters:         adapters,
		verifier:         config.Verifier,
		localCredentials: localCredentials,
		telemetry:        config.Telemetry,
		limits:           limits,
		now:              config.Now,
	}, nil
}

// Open verifies a concrete session plan, opens its selected adapter, and emits
// session.ready only after the upstream stream is ready to accept work.
func (e *Engine) Open(ctx context.Context, request OpenRequest) (*Session, error) {
	if err := validateOpenRequest(request, e.now()); err != nil {
		return nil, err
	}
	if err := e.verifier.Verify(ctx, request.Plan); err != nil {
		return nil, fmt.Errorf("verify session plan: %w", err)
	}
	adapter, ok := e.adapters[request.Plan.Route.Adapter]
	if !ok {
		return nil, fmt.Errorf("runtime adapter %q is not installed", request.Plan.Route.Adapter)
	}

	sessionCtx, cancel := context.WithCancel(context.Background())
	openStarted := e.now()
	adapterPlan := request.Plan
	if request.Plan.Execution.CredentialSource == protocol.CredentialsBYOK {
		credential, ok := e.localCredentials[request.Plan.Route.Provider]
		if !ok {
			cancel()
			return nil, fmt.Errorf("runtime: BYOK credential for provider %q is not configured", request.Plan.Route.Provider)
		}
		value, err := resolveLocalCredential(credential)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("runtime: load BYOK credential for provider %q: %w", request.Plan.Route.Provider, err)
		}
		adapterPlan.Route.Credential = &protocol.DelegatedCredential{
			Kind: credential.Kind, Value: value, ExpiresAt: request.Plan.ExpiresAt,
		}
	}
	// The caller's own voice always wins; the signed route only fills a blank.
	// Without this, `provider: "auto"` plus TTS is unusable: every voice-taking
	// adapter rejects an empty voice id, and a caller that delegated the vendor
	// choice has no way to know which vendor's id space to send one from.
	adapterOptions := request.Options
	if strings.TrimSpace(adapterOptions.Voice) == "" {
		adapterOptions.Voice = adapterPlan.Route.Voice
	}
	stream, err := adapter.Open(ctx, AdapterRequest{Kind: request.Kind, Plan: adapterPlan, Options: adapterOptions, Media: request.Media})
	if err != nil {
		cancel()
		e.recordOpenFailure(request, err)
		return nil, fmt.Errorf("open provider stream: %w", err)
	}
	openLatency := e.now().Sub(openStarted)
	if stream == nil {
		cancel()
		e.recordOpenFailure(request, errors.New("provider stream is required"))
		return nil, errors.New("provider stream is required")
	}
	providerEvents := stream.Events()
	if providerEvents == nil {
		cancel()
		e.recordOpenFailure(request, errors.New("provider stream must expose an events channel"))
		return nil, errors.New("provider stream must expose an events channel")
	}

	session := &Session{
		kind:           request.Kind,
		plan:           request.Plan,
		stream:         stream,
		providerEvents: providerEvents,
		ctx:            sessionCtx,
		cancel:         cancel,
		input:          newInputQueue(e.limits.MaxInputMessages, e.limits.MaxInputBytes),
		events:         make(chan protocol.Event, e.limits.MaxOutputEvents+1),
		done:           make(chan struct{}),
		limits:         e.limits,
		telemetry:      e.telemetry,
		now:            e.now,
	}
	if request.Media != nil {
		session.media = *request.Media
	}
	session.setLeaseDeadline(request.Plan.Reservation.LeaseExpiresAt)
	session.emitRegular(protocol.EventSessionReady, nil, nil, nil)
	session.record("session.opened", openLatencyData(openLatency))
	go session.runInput()
	go session.runEvents()
	return session, nil
}

const maxLocalCredentialBytes = 64 << 10

func resolveLocalCredential(credential LocalCredential) (string, error) {
	if credential.ValueFile == "" {
		if credential.Value == "" {
			return "", errors.New("credential value is empty")
		}
		return credential.Value, nil
	}
	file, err := os.Open(credential.ValueFile)
	if err != nil {
		return "", err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxLocalCredentialBytes+1))
	if err != nil {
		return "", err
	}
	if len(data) > maxLocalCredentialBytes {
		return "", fmt.Errorf("credential file exceeds %d bytes", maxLocalCredentialBytes)
	}
	value := strings.TrimSpace(string(data))
	if value == "" {
		return "", errors.New("credential file is empty")
	}
	return value, nil
}

func validateOpenRequest(request OpenRequest, now time.Time) error {
	if err := request.Plan.Validate(now); err != nil {
		return fmt.Errorf("invalid session plan: %w", err)
	}
	switch request.Kind {
	case protocol.SessionKindSTT, protocol.SessionKindRealtime:
		if request.Plan.Reservation.Usage.Unit != protocol.UsageUnitDurationSeconds {
			return fmt.Errorf("invalid session plan: %s requires duration_seconds usage", request.Kind)
		}
		if request.Media == nil {
			return fmt.Errorf("media is required for %s", request.Kind)
		}
		if err := request.Media.Validate(); err != nil {
			return fmt.Errorf("invalid media: %w", err)
		}
	case protocol.SessionKindTTS:
		if request.Plan.Reservation.Usage.Unit != protocol.UsageUnitCharacters {
			return fmt.Errorf("invalid session plan: tts requires characters usage")
		}
		if request.Media == nil {
			return fmt.Errorf("media is required for %s", request.Kind)
		}
		if err := request.Media.Validate(); err != nil {
			return fmt.Errorf("invalid media: %w", err)
		}
	case protocol.SessionKindLLM:
		if request.Plan.Reservation.Usage.Unit != protocol.UsageUnitDurationSeconds {
			return fmt.Errorf("invalid session plan: llm requires duration_seconds usage")
		}
		if request.Media != nil {
			return fmt.Errorf("media is not valid for llm sessions")
		}
	default:
		return fmt.Errorf("unsupported session kind %q", request.Kind)
	}
	return nil
}

// Session is an active, single-provider attempt. Its methods are safe to call
// from multiple goroutines; provider calls remain serialized by runInput.
type Session struct {
	kind           protocol.SessionKind
	media          protocol.MediaFormat
	plan           protocol.SessionPlan
	stream         ProviderStream
	providerEvents <-chan ProviderEvent
	ctx            context.Context
	cancel         context.CancelFunc
	input          *inputQueue
	events         chan protocol.Event
	done           chan struct{}
	limits         Limits
	telemetry      TelemetrySink
	now            func() time.Time

	sequence         atomic.Uint64
	telemetryDropped atomic.Uint64

	errMu sync.RWMutex
	err   error

	providerMu         sync.Mutex
	finishOnce         sync.Once
	providerOnce       sync.Once
	outputMu           sync.Mutex
	outputClosed       bool
	leaseMu            sync.Mutex
	leaseTimer         *time.Timer
	leaseDeadline      time.Time
	leaseSequence      uint64
	usageMu            sync.Mutex
	usedUnits          int64
	acceptedAudioBytes int64
	usageOnce          sync.Once
}

// AudioInput transfers ownership of Data to the session after SubmitAudio
// returns nil. Release is called once when the provider consumes or the
// runtime discards the input. When SubmitAudio returns an error, the caller
// retains ownership and Release is not invoked.
type AudioInput struct {
	Data    []byte
	Release func()
}

// Events contains canonical events in provider order. It closes after a
// terminal event. The session owns event Audio buffers until delivery.
func (s *Session) Events() <-chan protocol.Event { return s.events }

// Done closes when the session becomes terminal. Events may still contain the
// already-enqueued terminal event and should be drained independently.
func (s *Session) Done() <-chan struct{} { return s.done }

// Err returns the terminal error, if the session ended abnormally.
func (s *Session) Err() error {
	s.errMu.RLock()
	defer s.errMu.RUnlock()
	return s.err
}

// Wait blocks until the session is terminal or ctx expires.
func (s *Session) Wait(ctx context.Context) error {
	select {
	case <-s.done:
		return s.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SubmitAudio enqueues an audio frame without copying it.
func (s *Session) SubmitAudio(input AudioInput) error {
	if s.kind != protocol.SessionKindSTT && s.kind != protocol.SessionKindRealtime {
		return ErrUnsupportedOperation
	}
	if len(input.Data) == 0 {
		return fmt.Errorf("runtime: audio input is empty")
	}
	return s.input.tryPush(inputMessage{kind: inputAudio, audio: audioInput{data: input.Data, release: input.Release}})
}

// CommitAudio marks an STT boundary after all earlier audio frames.
func (s *Session) CommitAudio() error {
	if s.kind != protocol.SessionKindSTT && s.kind != protocol.SessionKindRealtime {
		return ErrUnsupportedOperation
	}
	return s.input.tryPush(inputMessage{kind: inputAudioCommit})
}

// AppendText queues TTS or LLM input after prior session input.
func (s *Session) AppendText(text string) error {
	if s.kind != protocol.SessionKindTTS && s.kind != protocol.SessionKindLLM && s.kind != protocol.SessionKindRealtime {
		return ErrUnsupportedOperation
	}
	if text == "" {
		return fmt.Errorf("runtime: text input is empty")
	}
	if s.kind != protocol.SessionKindTTS {
		return s.input.tryPush(inputMessage{kind: inputTextAppend, text: text})
	}
	units := int64(utf8.RuneCountInString(text))
	s.usageMu.Lock()
	defer s.usageMu.Unlock()
	if units <= 0 || units > s.plan.Reservation.Usage.AuthorizedUnits-s.usedUnits {
		return ErrUsageLimitExceeded
	}
	if err := s.input.tryPush(inputMessage{kind: inputTextAppend, text: text}); err != nil {
		return err
	}
	s.usedUnits += units
	return nil
}

// CommitText marks a TTS or LLM utterance boundary.
func (s *Session) CommitText() error {
	if s.kind != protocol.SessionKindTTS && s.kind != protocol.SessionKindLLM && s.kind != protocol.SessionKindRealtime {
		return ErrUnsupportedOperation
	}
	return s.input.tryPush(inputMessage{kind: inputTextCommit})
}

// Cancel requests best-effort cancellation after all earlier queued input.
func (s *Session) Cancel() error {
	return s.input.tryPush(inputMessage{kind: inputCancel})
}

// Close begins a graceful close. Queued input is sent in order and the stream
// then receives Close. It never waits for provider or telemetry I/O.
func (s *Session) Close() {
	s.input.close()
}

// Abort immediately makes the session terminal and asks the provider stream
// to stop asynchronously. Use it when a local transport has failed and
// graceful input draining would retain capacity without a viable consumer.
func (s *Session) Abort() {
	s.fail(ErrSessionAborted)
}

func (s *Session) setLeaseDeadline(deadline time.Time) {
	s.leaseMu.Lock()
	s.setLeaseDeadlineLocked(deadline)
	s.leaseMu.Unlock()
}

func (s *Session) setLeaseDeadlineLocked(deadline time.Time) {
	s.leaseSequence++
	sequence := s.leaseSequence
	s.leaseDeadline = deadline
	if s.leaseTimer != nil {
		s.leaseTimer.Stop()
	}
	delay := deadline.Sub(s.now())
	if delay < 0 {
		delay = 0
	}
	s.leaseTimer = time.AfterFunc(delay, func() {
		s.leaseMu.Lock()
		expired := sequence == s.leaseSequence
		s.leaseMu.Unlock()
		if expired {
			s.fail(ErrSessionLeaseExpired)
		}
	})
}

func (s *Session) stopLeaseTimer() {
	s.leaseMu.Lock()
	s.leaseSequence++
	if s.leaseTimer != nil {
		s.leaseTimer.Stop()
	}
	s.leaseMu.Unlock()
}

// Stats returns a point-in-time view of bounded queue usage.
func (s *Session) Stats() Stats {
	messages, bytes := s.input.stats()
	return Stats{
		InputMessages:    messages,
		InputBytes:       bytes,
		TelemetryDropped: s.telemetryDropped.Load(),
		EventSequence:    s.sequence.Load(),
	}
}

func (s *Session) runInput() {
	for {
		message, ok := s.input.pop(s.ctx)
		if !ok {
			if s.ctx.Err() == nil {
				s.closeProvider()
			}
			return
		}
		if err := s.dispatch(message); err != nil {
			s.fail(err)
			return
		}
	}
}

func (s *Session) dispatch(message inputMessage) error {
	s.providerMu.Lock()
	defer s.providerMu.Unlock()

	switch message.kind {
	case inputAudio:
		err := s.stream.WriteAudio(s.ctx, message.audio.data)
		if err == nil {
			s.usageMu.Lock()
			if frameBytes := int64(len(message.audio.data)); frameBytes > math.MaxInt64-s.acceptedAudioBytes {
				s.acceptedAudioBytes = math.MaxInt64
			} else {
				s.acceptedAudioBytes += frameBytes
			}
			s.usageMu.Unlock()
		}
		if message.audio.release != nil {
			message.audio.release()
		}
		return err
	case inputAudioCommit:
		return s.stream.CommitAudio(s.ctx)
	case inputTextAppend:
		return s.stream.AppendText(s.ctx, message.text)
	case inputTextCommit:
		return s.stream.CommitText(s.ctx)
	case inputCancel:
		return s.stream.Cancel(s.ctx)
	default:
		return errors.New("runtime: unknown input message")
	}
}

func (s *Session) runEvents() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case event, ok := <-s.providerEvents:
			if !ok {
				s.finish(nil)
				return
			}
			if event.Err != nil {
				s.fail(fmt.Errorf("provider stream: %w", event.Err))
				return
			}
			if event.Type == "" {
				s.fail(errors.New("provider emitted an event without a type"))
				return
			}
			// usage.observed is a provider request correlation hint. Its payload
			// is exactly {"provider_request_id": ...}; other event payloads may
			// carry transcripts or audio and are never recorded.
			if event.Type == protocol.EventUsageObserved {
				s.record("usage.observed", event.Data)
			}
			s.emitRegular(event.Type, event.Data, event.Extensions, event.Audio)
		}
	}
}

func (s *Session) closeProvider() {
	s.providerOnce.Do(func() {
		s.providerMu.Lock()
		err := s.stream.Close(context.Background())
		s.providerMu.Unlock()
		if err != nil {
			s.fail(fmt.Errorf("close provider stream: %w", err))
		}
	})
}

func (s *Session) abortProvider() {
	if aborter, ok := s.stream.(AbortingProviderStream); ok {
		s.providerMu.Lock()
		_ = aborter.Abort(context.Background())
		s.providerMu.Unlock()
		return
	}
	s.closeProvider()
}

func (s *Session) fail(err error) {
	if err == nil {
		return
	}
	s.finishOnce.Do(func() {
		s.stopLeaseTimer()
		s.errMu.Lock()
		s.err = err
		s.errMu.Unlock()
		s.input.abort()
		s.cancel()
		s.recordUsageReported()
		s.record("session.failed", telemetryErrorData(err))
		s.emitTerminal(protocol.EventError, errorData(err), providerErrorExtensions(err), nil)
		go s.abortProvider()
	})
}

func (s *Session) finish(err error) {
	if err != nil {
		s.fail(err)
		return
	}
	s.finishOnce.Do(func() {
		s.stopLeaseTimer()
		s.input.abort()
		s.cancel()
		s.recordUsageReported()
		s.record("session.closed", nil)
		s.emitTerminal(protocol.EventSessionClosed, nil, nil, nil)
	})
}

func (s *Session) emitRegular(kind protocol.EventType, data json.RawMessage, extensions map[string]json.RawMessage, audio []byte) {
	s.outputMu.Lock()
	if s.outputClosed {
		s.outputMu.Unlock()
		return
	}
	if len(s.events) >= s.limits.MaxOutputEvents {
		s.outputMu.Unlock()
		s.fail(ErrOutputBackpressure)
		return
	}
	event := s.newEvent(kind, data, extensions, audio)
	s.events <- event
	s.outputMu.Unlock()
	s.recordAgentEvent(event)
}

func (s *Session) emitTerminal(kind protocol.EventType, data json.RawMessage, extensions map[string]json.RawMessage, audio []byte) {
	s.outputMu.Lock()
	defer s.outputMu.Unlock()
	if s.outputClosed {
		return
	}
	// The channel reserves one slot beyond MaxOutputEvents for this event.
	s.events <- s.newEvent(kind, data, extensions, audio)
	close(s.events)
	s.outputClosed = true
	close(s.done)
}

func (s *Session) newEvent(kind protocol.EventType, data json.RawMessage, extensions map[string]json.RawMessage, audio []byte) protocol.Event {
	sequence := s.sequence.Add(1)
	return protocol.Event{
		Type:        kind,
		EventID:     fmt.Sprintf("evt_%s_%s_%d", s.plan.SessionID, s.plan.AttemptID, sequence),
		SessionID:   s.plan.SessionID,
		AttemptID:   s.plan.AttemptID,
		Sequence:    sequence,
		CreatedAtMS: s.now().UnixMilli(),
		Data:        data,
		Extensions:  extensions,
		Audio:       audio,
	}
}

func (s *Session) record(name string, data json.RawMessage) {
	if !s.telemetry.TryRecord(TelemetryEvent{
		Name:        name,
		SessionID:   s.plan.SessionID,
		AttemptID:   s.plan.AttemptID,
		At:          s.now(),
		EventID:     telemetryEventID(s.plan.AttemptID, name, data),
		Destination: s.plan.Telemetry,
		Data:        data,
		Required:    s.plan.Execution.CredentialSource == protocol.CredentialsManaged && requiredMeteringEvent(name),
	}) {
		s.telemetryDropped.Add(1)
	}
}

// recordAgentEvent exports content-free canonical timing markers. The same
// event names work across LiveKit, Pipecat, and custom workers; payloads from
// providers and frameworks are deliberately not copied into telemetry.
func (s *Session) recordAgentEvent(event protocol.Event) {
	switch event.Type {
	case protocol.EventSpeechStarted,
		protocol.EventSpeechEnded,
		protocol.EventTranscriptFinal,
		protocol.EventResponseStarted,
		protocol.EventTextDone,
		protocol.EventToolCall,
		protocol.EventResponseDone,
		protocol.EventResponseCanceled,
		protocol.EventAudioStarted,
		protocol.EventAudioDone:
	default:
		return
	}
	s.record("agent.event", json.RawMessage(fmt.Sprintf(`{"event_type":%q,"sequence":%d}`, event.Type, event.Sequence)))
}

func requiredMeteringEvent(name string) bool {
	return name == "usage.observed" || name == "usage.reported" || name == "session.closed" || name == "session.failed"
}

func (s *Session) recordUsageReported() {
	s.usageOnce.Do(func() {
		s.usageMu.Lock()
		usedUnits := s.usedUnits
		acceptedAudioBytes := s.acceptedAudioBytes
		s.usageMu.Unlock()
		s.record("usage.reported", usageReportedData(s.kind, s.media, usedUnits, acceptedAudioBytes))
	})
}

func (e *Engine) recordOpenFailure(request OpenRequest, openErr error) {
	usage := usageReportedData(request.Kind, mediaValue(request.Media), 0, 0)
	for _, event := range []TelemetryEvent{
		{
			Name: "usage.reported", SessionID: request.Plan.SessionID, AttemptID: request.Plan.AttemptID,
			At: e.now(), EventID: telemetryEventID(request.Plan.AttemptID, "usage.reported", usage),
			Destination: request.Plan.Telemetry, Data: usage,
			Required: request.Plan.Execution.CredentialSource == protocol.CredentialsManaged,
		},
		{
			Name: "session.failed", SessionID: request.Plan.SessionID, AttemptID: request.Plan.AttemptID,
			At: e.now(), EventID: telemetryEventID(request.Plan.AttemptID, "session.failed", telemetryErrorData(openErr)),
			Destination: request.Plan.Telemetry, Data: telemetryErrorData(openErr),
			Required: request.Plan.Execution.CredentialSource == protocol.CredentialsManaged,
		},
	} {
		e.telemetry.TryRecord(event)
	}
}

func mediaValue(media *protocol.MediaFormat) protocol.MediaFormat {
	if media == nil {
		return protocol.MediaFormat{}
	}
	return *media
}

func usageReportedData(kind protocol.SessionKind, media protocol.MediaFormat, usedUnits, acceptedAudioBytes int64) json.RawMessage {
	unit := protocol.UsageUnitDurationSeconds
	quantityMillis := int64(0)
	if kind == protocol.SessionKindTTS {
		unit = protocol.UsageUnitCharacters
		if usedUnits > 0 {
			quantityMillis = (math.MaxInt64 / 1_000) * 1_000
			if usedUnits <= math.MaxInt64/1_000 {
				quantityMillis = usedUnits * 1_000
			}
		}
	} else if media.Encoding == "pcm_s16le" && media.SampleRateHz > 0 && media.Channels > 0 && acceptedAudioBytes > 0 {
		bytesPerFrame := int64(media.Channels * 2)
		completeFrames := acceptedAudioBytes / bytesPerFrame
		sampleRate := int64(media.SampleRateHz)
		wholeSeconds, remainingFrames := completeFrames/sampleRate, completeFrames%sampleRate
		remainingMillis := remainingFrames * 1_000 / sampleRate
		quantityMillis = math.MaxInt64
		if wholeSeconds <= (math.MaxInt64-remainingMillis)/1_000 {
			quantityMillis = wholeSeconds*1_000 + remainingMillis
		}
	}
	payload, err := json.Marshal(map[string]any{"unit": unit, "quantity_millis": quantityMillis})
	if err != nil {
		return json.RawMessage(`{"unit":"duration_seconds","quantity_millis":0}`)
	}
	return payload
}

// telemetryEventID is deterministic so a resent event deduplicates in the
// control plane's idempotent ingest. Lifecycle events occur at most once per
// attempt; payload-bearing events additionally key on their content.
func telemetryEventID(attemptID, name string, data json.RawMessage) string {
	if len(data) == 0 {
		return "tel_" + attemptID + "_" + name
	}
	sum := sha256.Sum256(data)
	return "tel_" + attemptID + "_" + name + "_" + hex.EncodeToString(sum[:8])
}

func openLatencyData(latency time.Duration) json.RawMessage {
	if latency < 0 {
		latency = 0
	}
	return json.RawMessage(fmt.Sprintf(`{"provider_open_ms":%d}`, latency.Milliseconds()))
}

// telemetryErrorData exports the failure classification without the error
// message: provider messages can quote request content, and telemetry must
// never carry media, transcripts, or credentials.
func telemetryErrorData(err error) json.RawMessage {
	code := "internal"
	source := "runtime"
	retryable := false
	providerStatus := 0
	if errors.Is(err, ErrSessionLeaseExpired) {
		code = "session_lease_expired"
		retryable = true
	}
	var providerError *ProviderError
	if errors.As(err, &providerError) {
		code = providerError.Code
		if code == "" {
			code = "provider_unavailable"
		}
		source = "provider"
		retryable = providerError.Retryable
		providerStatus = providerError.ProviderStatus
	}
	payload := map[string]any{
		"code":      code,
		"retryable": retryable,
		"terminal":  true,
		"source":    source,
	}
	if providerStatus != 0 {
		payload["provider_status"] = providerStatus
	}
	data, marshalErr := json.Marshal(payload)
	if marshalErr != nil {
		return json.RawMessage(`{"code":"internal","terminal":true,"source":"runtime"}`)
	}
	return data
}

func errorData(err error) json.RawMessage {
	code := "internal"
	source := "runtime"
	retryable := false
	providerStatus := 0
	message := err.Error()
	if errors.Is(err, ErrSessionLeaseExpired) {
		code = "session_lease_expired"
		retryable = true
	}
	var providerError *ProviderError
	if errors.As(err, &providerError) {
		code = providerError.Code
		if code == "" {
			code = "provider_unavailable"
		}
		source = "provider"
		retryable = providerError.Retryable
		providerStatus = providerError.ProviderStatus
		message = providerError.Error()
	}
	payload := map[string]any{
		"code":      code,
		"message":   message,
		"retryable": retryable,
		"terminal":  true,
		"source":    source,
	}
	if providerStatus != 0 {
		payload["provider_status"] = providerStatus
	}
	data, marshalErr := json.Marshal(payload)
	if marshalErr != nil {
		return json.RawMessage(`{"code":"internal","terminal":true,"source":"runtime"}`)
	}
	return data
}

func providerErrorExtensions(err error) map[string]json.RawMessage {
	var providerError *ProviderError
	if !errors.As(err, &providerError) || len(providerError.Extensions) == 0 {
		return nil
	}
	return providerError.Extensions
}
