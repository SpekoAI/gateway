// Package openailive implements GPT-Live speech-to-speech sessions for
// OpenAI's gpt-live-1: a full-duplex voice model that listens while it speaks
// and delegates reasoning to a backend (the caller's own application, or a
// Responses model it is configured with).
//
// The Live protocol is deliberately implemented apart from the Realtime
// adapter (providers/openairealtime): it connects to
// wss://api.openai.com/v1/live/sessions with no query parameters, opens with a
// session.start command naming the model, streams audio continuously without
// input commits or response.create voice turns, reports transcripts as
// session-timeline intervals that may overlap, and finalizes with a
// session.closed event carrying the final cumulative voice usage. None of the
// Realtime turn machinery (input_audio_buffer.commit, response.cancel,
// server_vad) exists here, and this adapter never manufactures a speech or
// turn boundary the vendor did not send.
//
// Credentials and endpoints come only from the SessionPlan. Provider-direct
// plans carry the customer's own bearer (BYOK): the documented Live primary
// WebSocket authenticates with a server-side project key and mints no
// ephemeral Realtime-style client secret, so a managed session never reaches
// api.openai.com from a customer runtime. Managed sessions instead carry a
// speko_relay route whose endpoint is the Speko Router's /v1/live, which
// speaks this same protocol natively; the Router's connector holds the
// permanent key and dials the vendor with this adapter.
package openailive

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/SpekoAI/gateway/protocol"
	"github.com/SpekoAI/gateway/relayapi"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// AdapterID is the stable adapter identifier and the speech protocol name.
	AdapterID = string(protocol.SpeechProtocolOpenAILiveV1)
	// ExtensionID namespaces the raw vendor payloads this adapter attaches to
	// canonical events.
	ExtensionID = "openai.com/live/v1"
	// DefaultVoice is the vendor default and the route default.
	DefaultVoice = "marin"
	// DefaultModel is the only GPT-Live model at launch.
	DefaultModel = "gpt-live-1"

	officialHost = "api.openai.com"
	sessionsPath = "/v1/live/sessions"
	// relayPath is the Speko Router route that speaks this protocol.
	relayPath = "/v1/live"

	// appendChunkBytes is 0.5 s of 24 kHz mono PCM16; chunks stay even so a
	// 16-bit sample is never split across appends.
	appendChunkBytes         = 24_000
	defaultEventBuffer       = 128
	defaultMaxMessageBytes   = 2 << 20 // == protocol.MaxProviderEventBytes
	defaultSetupTimeout      = 15 * time.Second
	defaultCloseDrainTimeout = 15 * time.Second
	startEventID             = "speko_session_start"
)

// SupportedSampleRates are the PCM16 rates the adapter accepts. Input and
// output share one format, as in the vendor protocol.
var SupportedSampleRates = []int{16_000, 24_000}

// Config configures the adapter.
type Config struct {
	HTTPClient      *http.Client
	EventBuffer     int
	MaxMessageBytes int64
	// AllowedEndpointHosts extends the vendor host allowlist (tests).
	AllowedEndpointHosts []string
	// RelayEndpointHosts lists hosts that serve this protocol on a speko_relay
	// route — the Speko Router. A relay route to any other host is refused.
	RelayEndpointHosts    []string
	AllowInsecureEndpoint bool
	// SetupTimeout bounds the wait for session.started.
	SetupTimeout time.Duration
	// CloseDrainTimeout bounds the wait for session.closed after
	// session.close was sent. A session whose final event never arrives ends
	// with a finalization_incomplete terminal error rather than a silent
	// success: its final usage is unconfirmed.
	CloseDrainTimeout time.Duration
	// NativeEvents surfaces EVERY vendor server event — audio deltas included
	// — as protocol.EventProviderEvent envelopes and suppresses canonical
	// audio frames. Relay connectors set it so the public /v1/live socket can
	// forward the vendor protocol verbatim. The local gateway leaves it
	// false: audio arrives as canonical binary frames, and every non-audio
	// vendor event still rides a provider.event envelope beside its
	// canonical translation.
	NativeEvents bool
}

// Adapter implements runtime.Adapter for gpt-live-1.
type Adapter struct {
	httpClient        *http.Client
	eventBuffer       int
	maxMessageBytes   int64
	setupTimeout      time.Duration
	closeDrainTimeout time.Duration
	hosts             map[string]struct{}
	relayHosts        map[string]struct{}
	allowInsecure     bool
	nativeEvents      bool
}

// New constructs the adapter.
func New(config Config) (*Adapter, error) {
	if config.EventBuffer == 0 {
		config.EventBuffer = defaultEventBuffer
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = defaultMaxMessageBytes
	}
	if config.SetupTimeout == 0 {
		config.SetupTimeout = defaultSetupTimeout
	}
	if config.CloseDrainTimeout == 0 {
		config.CloseDrainTimeout = defaultCloseDrainTimeout
	}
	if config.EventBuffer < 1 || config.MaxMessageBytes < 1 || config.SetupTimeout <= 0 || config.CloseDrainTimeout <= 0 {
		return nil, errors.New("openai live: event buffer, message bound, setup and drain timeouts must be positive")
	}
	hosts, err := hostSet(officialHost, config.AllowedEndpointHosts)
	if err != nil {
		return nil, err
	}
	relayHosts, err := hostSet("", config.RelayEndpointHosts)
	if err != nil {
		return nil, err
	}
	return &Adapter{
		httpClient: config.HTTPClient, eventBuffer: config.EventBuffer, maxMessageBytes: config.MaxMessageBytes,
		setupTimeout: config.SetupTimeout, closeDrainTimeout: config.CloseDrainTimeout,
		hosts: hosts, relayHosts: relayHosts, allowInsecure: config.AllowInsecureEndpoint, nativeEvents: config.NativeEvents,
	}, nil
}

func hostSet(official string, extra []string) (map[string]struct{}, error) {
	hosts := map[string]struct{}{}
	if official != "" {
		hosts[official] = struct{}{}
	}
	for _, host := range extra {
		host = strings.ToLower(strings.TrimSpace(host))
		if host == "" || strings.ContainsAny(host, "/@?#") {
			return nil, errors.New("openai live: endpoint host is invalid")
		}
		hosts[host] = struct{}{}
	}
	return hosts, nil
}

// ID returns the adapter identifier.
func (a *Adapter) ID() string { return AdapterID }

// Open dials the Live socket, sends session.start, and waits for
// session.started. The Engine emits the canonical session.ready after this
// returns; the adapter emits no second ready event.
func (a *Adapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindRealtime && request.Kind != protocol.SessionKindS2S {
		return nil, fmt.Errorf("openai live supports speech-to-speech sessions, got %q", request.Kind)
	}
	route := request.Plan.Route
	if route.Provider != "openai" {
		return nil, fmt.Errorf("openai live adapter cannot open provider %q", route.Provider)
	}
	if route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("openai live requires websocket transport, got %q", route.Transport)
	}
	model := strings.TrimSpace(route.Model)
	if model == "" {
		return nil, errors.New("openai live requires a model")
	}
	if request.Media == nil {
		return nil, errors.New("openai live requires input media configuration")
	}
	if err := validatePCM("input", *request.Media); err != nil {
		return nil, err
	}
	if request.Options.S2S == nil || request.Options.S2S.OutputMedia == nil {
		return nil, errors.New("openai live requires output media configuration")
	}
	output := *request.Options.S2S.OutputMedia
	if err := validatePCM("output", output); err != nil {
		return nil, err
	}
	if output.SampleRateHz != request.Media.SampleRateHz {
		return nil, &runtimepkg.ProviderError{
			Code:    "unsupported_media",
			Message: fmt.Sprintf("openai live uses one audio format in both directions, got %d Hz in and %d Hz out", request.Media.SampleRateHz, output.SampleRateHz),
			Hint:    "Declare the same PCM16 sample rate (16000 or 24000) for input and output.",
		}
	}
	if request.Options.S2S.Live != nil {
		if err := request.Options.S2S.Live.Validate(); err != nil {
			return nil, fmt.Errorf("openai live options: %w", err)
		}
	}
	endpoint, viaRelay, err := a.parseEndpoint(route.Endpoint, request.Plan.Execution.ProviderRoute)
	if err != nil {
		return nil, fmt.Errorf("openai live endpoint: %w", err)
	}
	credential := route.Credential
	if credential == nil || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("openai live requires a delegated credential")
	}
	switch {
	case credential.Kind == protocol.CredentialBearer:
	case credential.Kind == protocol.CredentialRelayAccess && viaRelay:
	default:
		return nil, fmt.Errorf("openai live requires a bearer credential (or relay_access on the Router route), got %q", credential.Kind)
	}

	headers := make(http.Header)
	headers.Set("Authorization", "Bearer "+credential.Value)
	if viaRelay {
		// The Router requires an idempotency key on every session upgrade; the
		// attempt id is unique per dispatch and content-free.
		headers.Set("Idempotency-Key", request.Plan.AttemptID)
	}
	conn, response, err := websocket.Dial(ctx, endpoint.String(), &websocket.DialOptions{HTTPClient: a.httpClient, HTTPHeader: headers})
	if err != nil {
		return nil, dialError(response, err)
	}
	conn.SetReadLimit(a.maxMessageBytes)

	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &liveStream{
		conn: conn, ctx: streamCtx, cancel: cancel, nativeEvents: a.nativeEvents,
		drainTimeout: a.closeDrainTimeout,
		events:       make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		setupDone:    make(chan error, 1),
		closedEvent:  make(chan struct{}),
		usage:        Usage{Backend: map[string]BackendUsage{}},
	}
	voice := strings.TrimSpace(request.Options.Voice)
	if voice == "" {
		voice = DefaultVoice
	}
	start := BuildSessionStart(model, voice, request.Media.SampleRateHz, request.Options.S2S)
	if err := stream.writeJSON(ctx, start); err != nil {
		_ = stream.abort()
		return nil, err
	}
	go stream.readLoop()

	setupCtx, cancelSetup := context.WithTimeout(ctx, a.setupTimeout)
	defer cancelSetup()
	select {
	case err := <-stream.setupDone:
		if err != nil {
			_ = stream.abort()
			return nil, err
		}
	case <-setupCtx.Done():
		_ = stream.abort()
		return nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "openai live did not acknowledge session.start", Retryable: true, Cause: setupCtx.Err()}
	}
	return stream, nil
}

func validatePCM(direction string, media protocol.MediaFormat) error {
	if err := media.Validate(); err != nil {
		return fmt.Errorf("openai live %s media: %w", direction, err)
	}
	supported := false
	for _, rate := range SupportedSampleRates {
		if media.SampleRateHz == rate {
			supported = true
		}
	}
	if media.Encoding != "pcm_s16le" || media.Channels != 1 || !supported {
		return &runtimepkg.ProviderError{
			Code:    "unsupported_media",
			Message: fmt.Sprintf("openai live requires mono pcm_s16le at 16 or 24 kHz for %s, got %s/%d Hz/%d channels", direction, media.Encoding, media.SampleRateHz, media.Channels),
			Hint:    "Convert audio to mono pcm_s16le at 16000 or 24000 Hz before opening the live session.",
		}
	}
	return nil
}

// parseEndpoint admits exactly two shapes: the vendor sessions socket on an
// allowed vendor host (provider-direct, or the Router connector's own
// speko_relay plan) and the Router's /v1/live on a configured relay host
// (a customer runtime's managed speko_relay plan). Query strings are refused
// on both — the Live socket takes none.
func (a *Adapter) parseEndpoint(raw string, providerRoute protocol.ProviderRoute) (*url.URL, bool, error) {
	endpoint, err := url.Parse(raw)
	if err != nil || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.Fragment != "" || endpoint.RawQuery != "" {
		return nil, false, errors.New("endpoint must be a clean absolute WebSocket URL without a query")
	}
	if endpoint.Scheme != "wss" && !(a.allowInsecure && endpoint.Scheme == "ws") {
		return nil, false, errors.New("endpoint must use wss")
	}
	if !a.allowInsecure && endpoint.Port() != "" && endpoint.Port() != "443" {
		return nil, false, errors.New("endpoint uses a non-standard port")
	}
	host := strings.ToLower(endpoint.Hostname())
	if _, relay := a.relayHosts[host]; relay {
		if providerRoute != protocol.RouteSpekoRelay {
			return nil, false, errors.New("the Speko Router endpoint is valid only on a speko_relay route")
		}
		if endpoint.Path != relayPath {
			return nil, false, fmt.Errorf("Router endpoint path must be %s, got %q", relayPath, endpoint.Path)
		}
		return endpoint, true, nil
	}
	if _, ok := a.hosts[host]; !ok {
		return nil, false, errors.New("endpoint host is not allowed")
	}
	if providerRoute != protocol.RouteProviderDirect && providerRoute != protocol.RouteSpekoRelay {
		return nil, false, fmt.Errorf("unsupported provider route %q", providerRoute)
	}
	if endpoint.Path != sessionsPath {
		return nil, false, fmt.Errorf("endpoint path must be %s, got %q", sessionsPath, endpoint.Path)
	}
	return endpoint, false, nil
}

// BuildSessionStart assembles the session.start command from the canonical
// options. Voice instructions ride session.instructions; backend
// instructions ride delegation.responses.instructions and are never merged.
func BuildSessionStart(model, voice string, sampleRateHz int, options *protocol.S2SOptions) map[string]any {
	session := map[string]any{
		"model": model,
		"audio": map[string]any{
			"format": map[string]any{"type": "audio/pcm", "rate": sampleRateHz},
			"output": map[string]any{"voice": voice},
		},
	}
	var live *protocol.LiveOptions
	if options != nil {
		if strings.TrimSpace(options.Instructions) != "" {
			session["instructions"] = options.Instructions
		}
		live = options.Live
	}
	if live != nil && len(live.History) > 0 {
		input := make([]map[string]any, 0, len(live.History))
		for _, item := range live.History {
			partType := "input_text"
			if item.Role == "assistant" {
				partType = "output_text"
			}
			input = append(input, map[string]any{
				"type": "message", "role": item.Role,
				"content": []map[string]any{{"type": partType, "text": item.Text}},
			})
		}
		session["input"] = input
	}
	delegation := map[string]any{"type": string(protocol.LiveDelegationClient)}
	if live != nil && live.Delegation != nil && live.Delegation.Mode() == protocol.LiveDelegationResponses && live.Delegation.Responses != nil {
		backend := live.Delegation.Responses
		responses := map[string]any{"model": backend.Model}
		if backend.Instructions != "" {
			responses["instructions"] = backend.Instructions
		}
		if len(backend.Tools) > 0 {
			tools := make([]json.RawMessage, 0, len(backend.Tools))
			for _, tool := range backend.Tools {
				tools = append(tools, tool.Raw())
			}
			responses["tools"] = tools
		}
		if len(backend.ToolChoice) > 0 {
			responses["tool_choice"] = backend.ToolChoice
		}
		if backend.ParallelToolCalls != nil {
			responses["parallel_tool_calls"] = *backend.ParallelToolCalls
		}
		if backend.MaxOutputTokens > 0 {
			responses["max_output_tokens"] = backend.MaxOutputTokens
		}
		if backend.ServiceTier != "" {
			responses["service_tier"] = backend.ServiceTier
		}
		if len(backend.Reasoning) > 0 {
			responses["reasoning"] = backend.Reasoning
		}
		if len(backend.Text) > 0 {
			responses["text"] = backend.Text
		}
		delegation = map[string]any{"type": string(protocol.LiveDelegationResponses), "responses": responses}
	}
	session["delegation"] = delegation
	return map[string]any{"type": "session.start", "event_id": startEventID, "session": session}
}

// Usage is the adapter's content-free account of a session: the latest
// cumulative voice-duration snapshot, whether the vendor's final
// session.closed confirmed it, and backend usage keyed by response id so a
// replayed completion event is never counted twice.
type Usage struct {
	// VoiceSeconds is the latest cumulative snapshot. Snapshots replace one
	// another; they are never summed.
	VoiceSeconds float64
	// Final reports that VoiceSeconds came from session.closed.
	Final bool
	// CloseReason is session.closed's reason when Final.
	CloseReason string
	// Backend holds one entry per completed backend response.
	Backend map[string]BackendUsage
}

// BackendUsage is one backend response's token usage plus its billable tool
// calls.
type BackendUsage struct {
	InputTokens       int64
	CachedInputTokens int64
	OutputTokens      int64
	ReasoningTokens   int64
	ToolCalls         int64
	// Completed reports that the response's terminal usage was recorded; a
	// later completion for the same id is a replay and is ignored.
	Completed bool
}

// Clone returns a copy safe to hold across further events.
func (u Usage) Clone() Usage {
	backend := make(map[string]BackendUsage, len(u.Backend))
	for id, usage := range u.Backend {
		backend[id] = usage
	}
	u.Backend = backend
	return u
}

// BackendTotals sums the per-response backend usage into one normalized
// relayapi.Usage (token lines plus tool calls; no duration). Deduplication by
// response id already happened as the completions arrived, so this is a plain
// sum over distinct responses.
func (u Usage) BackendTotals() relayapi.Usage {
	var total relayapi.Usage
	for _, backend := range u.Backend {
		total.InputTokens += backend.InputTokens
		total.CachedInputTokens += backend.CachedInputTokens
		total.OutputTokens += backend.OutputTokens
		total.ReasoningTokens += backend.ReasoningTokens
		total.ToolCalls += backend.ToolCalls
	}
	return total
}

// UsageStream is the optional capability relay connectors read final usage
// through after the stream ends: the authoritative final voice seconds from
// session.closed and the backend token/tool totals.
type UsageStream interface {
	Usage() Usage
}

type liveStream struct {
	conn         *websocket.Conn
	ctx          context.Context
	cancel       context.CancelFunc
	nativeEvents bool
	drainTimeout time.Duration
	events       chan runtimepkg.ProviderEvent
	setupDone    chan error
	closedEvent  chan struct{}

	writeMu      sync.Mutex
	pending      []byte
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	closing      atomic.Bool
	closeErr     error
	setupOnce    sync.Once
	ready        atomic.Bool
	closedOnce   sync.Once

	terminalMu  sync.Mutex
	terminalErr error

	usageMu sync.Mutex
	usage   Usage
}

func (s *liveStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

// WriteAudio appends PCM in even-length chunks; a trailing odd byte is
// carried into the next append so no 16-bit sample is split.
func (s *liveStream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("openai live audio is empty")
	}
	s.writeMu.Lock()
	buffer := append(s.pending, audio...)
	complete := len(buffer) - len(buffer)%2
	s.pending = append([]byte(nil), buffer[complete:]...)
	buffer = buffer[:complete]
	s.writeMu.Unlock()
	for offset := 0; offset < len(buffer); offset += appendChunkBytes {
		end := min(offset+appendChunkBytes, len(buffer))
		if err := s.writeJSON(ctx, map[string]any{"type": "session.input_audio.append", "audio": base64.StdEncoding.EncodeToString(buffer[offset:end])}); err != nil {
			return err
		}
	}
	return nil
}

// CommitAudio is a Realtime control. GPT-Live streams audio continuously and
// decides when to speak; there is no commit to forward.
func (s *liveStream) CommitAudio(context.Context) error {
	return &runtimepkg.ProviderError{Code: "unsupported_operation", Message: "input_audio_buffer.commit is a Realtime control; GPT-Live streams audio continuously and has no input commit", Hint: "Stream audio without commits; GPT-Live manages when to listen and speak."}
}

func (s *liveStream) AppendText(context.Context, string) error {
	return runtimepkg.ErrUnsupportedOperation
}
func (s *liveStream) CommitText(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// Cancel is a Realtime control (response.cancel). GPT-Live has no cancel:
// speech is interrupted by speaking, and backend work is the application's
// to cancel or supersede.
func (s *liveStream) Cancel(context.Context) error {
	return &runtimepkg.ProviderError{Code: "unsupported_operation", Message: "response.cancel is a Realtime control; GPT-Live sessions have no response cancel", Hint: "Use session.instructions.append to redirect the conversation; supersede backend work in your application."}
}

// SendProviderControl forwards one allowlisted native command. A
// session.update may not move the model or the audio format: those are fixed
// at startup by the vendor and pinned by the plan.
func (s *liveStream) SendProviderControl(ctx context.Context, control protocol.ProviderControl) error {
	if err := control.Validate(protocol.SpeechProtocolOpenAILiveV1); err != nil {
		return fmt.Errorf("%w: %v", runtimepkg.ErrUnsupportedOperation, err)
	}
	if control.Type == "session.update" {
		var update struct {
			Session map[string]json.RawMessage `json:"session"`
		}
		if json.Unmarshal(control.Payload, &update) != nil {
			return fmt.Errorf("%w: session.update requires a session object", runtimepkg.ErrUnsupportedOperation)
		}
		for _, immutable := range []string{"model", "audio", "input", "store"} {
			if _, present := update.Session[immutable]; present {
				return fmt.Errorf("%w: session.update cannot change %s after session.start", runtimepkg.ErrUnsupportedOperation, immutable)
			}
		}
		if raw, present := update.Session["delegation"]; present {
			var delegation struct {
				Type string `json:"type"`
			}
			if json.Unmarshal(raw, &delegation) != nil || delegation.Type != "" {
				return fmt.Errorf("%w: session.update cannot change the delegation type", runtimepkg.ErrUnsupportedOperation)
			}
		}
	}
	return s.writeRaw(ctx, control.Payload)
}

// Close sends session.close and drains until session.closed or the drain
// timeout. Without the final event the session's usage is unconfirmed, so the
// stream records a finalization_incomplete terminal error instead of ending
// cleanly.
func (s *liveStream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		if s.closed.Load() {
			return
		}
		s.closing.Store(true)
		if err := s.writeJSON(ctx, map[string]any{"type": "session.close"}); err != nil && !errors.Is(err, runtimepkg.ErrSessionClosed) {
			s.closeErr = err
		}
		timer := time.NewTimer(s.drainTimeout)
		defer timer.Stop()
		select {
		case <-s.closedEvent:
		case <-s.ctx.Done():
		case <-timer.C:
			s.setTerminal(&runtimepkg.ProviderError{
				Code:    "finalization_incomplete",
				Message: "openai live did not deliver session.closed within the drain window; final voice usage is unconfirmed",
				Hint:    "The session ended, but its final usage could not be confirmed with the provider.",
			})
		}
		s.closed.Store(true)
		s.writeMu.Lock()
		if err := s.conn.Close(websocket.StatusNormalClosure, ""); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
		s.writeMu.Unlock()
		s.cancel()
	})
	return s.closeErr
}

func (s *liveStream) Abort(context.Context) error { return s.abort() }

func (s *liveStream) abort() error {
	s.abortOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		if err := s.conn.CloseNow(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
	})
	return s.closeErr
}

func (s *liveStream) TerminalError() error {
	s.terminalMu.Lock()
	defer s.terminalMu.Unlock()
	return s.terminalErr
}

func (s *liveStream) setTerminal(err error) {
	s.terminalMu.Lock()
	defer s.terminalMu.Unlock()
	if s.terminalErr == nil {
		s.terminalErr = err
	}
}

// Usage returns the current content-free usage account.
func (s *liveStream) Usage() Usage {
	s.usageMu.Lock()
	defer s.usageMu.Unlock()
	return s.usage.Clone()
}

func (s *liveStream) writeJSON(ctx context.Context, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return s.writeRaw(ctx, payload)
}

func (s *liveStream) writeRaw(ctx context.Context, payload []byte) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.conn.Write(ctx, websocket.MessageText, payload); err != nil {
		if s.closed.Load() {
			return runtimepkg.ErrSessionClosed
		}
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "openai live socket write failed", Retryable: true, Cause: err}
	}
	return nil
}

func (s *liveStream) emit(event runtimepkg.ProviderEvent) {
	if event.Err != nil {
		s.setTerminal(event.Err)
	}
	select {
	case s.events <- event:
	case <-s.ctx.Done():
	}
}

func (s *liveStream) settleSetup(err error) {
	s.setupOnce.Do(func() {
		if err == nil {
			s.ready.Store(true)
		}
		s.setupDone <- err
	})
}

// serverEvent is the subset of every Live server event the adapter reads.
// The raw payload is forwarded untouched; these fields only drive canonical
// translation and metering.
type serverEvent struct {
	Type    string `json:"type"`
	EventID string `json:"event_id"`
	Delta   string `json:"delta"`
	StartMS *int64 `json:"start_ms"`
	EndMS   *int64 `json:"end_ms"`
	Session *struct {
		ID string `json:"id"`
	} `json:"session"`
	Usage *struct {
		Seconds float64 `json:"seconds"`
	} `json:"usage"`
	Reason       string          `json:"reason"`
	DelegationID string          `json:"delegation_id"`
	Event        json.RawMessage `json:"event"`
	Error        *struct {
		Type          string `json:"type"`
		Code          string `json:"code"`
		Message       string `json:"message"`
		ClientEventID string `json:"client_event_id"`
	} `json:"error"`
}

// nestedResponseEvent is the subset of a response.event envelope's inner
// Responses event the adapter reads for backend usage.
type nestedResponseEvent struct {
	Type     string `json:"type"`
	Response *struct {
		ID    string `json:"id"`
		Usage *struct {
			InputTokens        int64 `json:"input_tokens"`
			OutputTokens       int64 `json:"output_tokens"`
			InputTokensDetails *struct {
				CachedTokens int64 `json:"cached_tokens"`
			} `json:"input_tokens_details"`
			OutputTokensDetails *struct {
				ReasoningTokens int64 `json:"reasoning_tokens"`
			} `json:"output_tokens_details"`
		} `json:"usage"`
		Output []struct {
			Type string `json:"type"`
		} `json:"output"`
	} `json:"response"`
	Item *struct {
		Type string `json:"type"`
	} `json:"item"`
}

func (s *liveStream) readLoop() {
	defer close(s.events)
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			s.finish(err)
			return
		}
		if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
			continue
		}
		var event serverEvent
		if json.Unmarshal(payload, &event) != nil || event.Type == "" {
			continue
		}
		s.handle(event, payload)
	}
}

func (s *liveStream) providerEvent(eventType string, raw []byte) {
	envelope, err := json.Marshal(protocol.ProviderEvent{Type: eventType, Payload: append(json.RawMessage(nil), raw...)})
	if err != nil {
		return
	}
	s.emit(runtimepkg.ProviderEvent{Type: protocol.EventProviderEvent, Data: envelope})
}

func (s *liveStream) handle(event serverEvent, raw []byte) {
	switch event.Type {
	case "session.started":
		s.providerEvent(event.Type, raw)
		s.settleSetup(nil)
	case "error":
		s.handleError(event, raw)
	case "session.output_audio.delta":
		if s.nativeEvents {
			s.providerEvent(event.Type, raw)
			return
		}
		audio, err := base64.StdEncoding.DecodeString(event.Delta)
		if err == nil && len(audio) > 0 {
			s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Audio: audio})
		}
	case "session.input_transcript.delta":
		s.providerEvent(event.Type, raw)
		if event.Delta != "" {
			s.emit(runtimepkg.ProviderEvent{Type: protocol.EventTranscriptDelta, Data: transcriptData(event)})
		}
	case "session.output_transcript.delta":
		s.providerEvent(event.Type, raw)
		if event.Delta != "" {
			s.emit(runtimepkg.ProviderEvent{Type: protocol.EventTextDelta, Data: transcriptData(event)})
		}
	case "session.usage.updated":
		s.providerEvent(event.Type, raw)
		if event.Usage != nil {
			s.recordVoiceSnapshot(event.Usage.Seconds, false, "")
			s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: marshalData(map[string]string{"provider_request_id": s.sessionRequestID(event)}), Extensions: s.extension(raw)})
		}
	case "response.event":
		s.providerEvent(event.Type, raw)
		s.recordBackendUsage(event)
	case "session.closed":
		s.providerEvent(event.Type, raw)
		seconds := 0.0
		if event.Usage != nil {
			seconds = event.Usage.Seconds
		}
		s.recordVoiceSnapshot(seconds, true, event.Reason)
		s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: marshalData(map[string]string{"provider_request_id": s.sessionRequestID(event)}), Extensions: s.extension(raw)})
		s.closedOnce.Do(func() { close(s.closedEvent) })
	default:
		// Delegation notices, append acknowledgments, session.updated,
		// mute/unmute acknowledgments, and anything the vendor adds later
		// all pass through untouched. Nothing here infers a turn boundary.
		s.providerEvent(event.Type, raw)
	}
}

func (s *liveStream) sessionRequestID(event serverEvent) string {
	if event.Session != nil && event.Session.ID != "" {
		return event.Session.ID
	}
	return event.EventID
}

func transcriptData(event serverEvent) json.RawMessage {
	data := map[string]any{"text": event.Delta}
	if event.StartMS != nil {
		data["start_ms"] = *event.StartMS
	}
	if event.EndMS != nil {
		data["end_ms"] = *event.EndMS
	}
	return marshalData(data)
}

func (s *liveStream) recordVoiceSnapshot(seconds float64, final bool, reason string) {
	s.usageMu.Lock()
	defer s.usageMu.Unlock()
	if s.usage.Final && !final {
		return
	}
	// Snapshots are cumulative: a later, smaller reading (a reordered
	// frame) never lowers the account, and the final reading is authoritative.
	if final || seconds > s.usage.VoiceSeconds {
		s.usage.VoiceSeconds = seconds
	}
	if final {
		s.usage.Final = true
		s.usage.CloseReason = reason
	}
}

// recordBackendUsage reads a nested response.completed (or legacy
// response.done) and records its usage once per response id. Function calls
// are the application's to run; only hosted web_search calls are counted as
// billable tool calls, from the completed output items.
func (s *liveStream) recordBackendUsage(event serverEvent) {
	if len(event.Event) == 0 {
		return
	}
	var nested nestedResponseEvent
	if json.Unmarshal(event.Event, &nested) != nil || nested.Response == nil {
		return
	}
	switch nested.Type {
	case "response.completed", "response.done", "response.incomplete":
	default:
		if nested.Type == "response.output_item.done" && nested.Item != nil && nested.Item.Type == "web_search_call" && nested.Response != nil && nested.Response.ID != "" {
			s.usageMu.Lock()
			usage := s.usage.Backend[nested.Response.ID]
			usage.ToolCalls++
			s.usage.Backend[nested.Response.ID] = usage
			s.usageMu.Unlock()
		}
		return
	}
	if nested.Response.ID == "" || nested.Response.Usage == nil {
		return
	}
	s.usageMu.Lock()
	usage, seen := s.usage.Backend[nested.Response.ID]
	if seen && usage.Completed {
		// A replayed completion for a response already accounted: keep the
		// native envelope (already forwarded) but do not re-observe usage.
		s.usageMu.Unlock()
		return
	}
	usage.Completed = true
	usage.InputTokens = nested.Response.Usage.InputTokens
	usage.OutputTokens = nested.Response.Usage.OutputTokens
	if nested.Response.Usage.InputTokensDetails != nil {
		usage.CachedInputTokens = nested.Response.Usage.InputTokensDetails.CachedTokens
	}
	if nested.Response.Usage.OutputTokensDetails != nil {
		usage.ReasoningTokens = nested.Response.Usage.OutputTokensDetails.ReasoningTokens
	}
	// The completed output list is authoritative for hosted tool calls when
	// the vendor includes it; forwarded lifecycle snapshots may carry an empty
	// output, in which case the per-item count gathered along the way stands.
	listed := int64(0)
	for _, item := range nested.Response.Output {
		if item.Type == "web_search_call" {
			listed++
		}
	}
	if listed > usage.ToolCalls {
		usage.ToolCalls = listed
	}
	s.usage.Backend[nested.Response.ID] = usage
	s.usageMu.Unlock()
	s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: marshalData(map[string]string{"provider_request_id": nested.Response.ID}), Extensions: s.extension(event.Event)})
}

func (s *liveStream) handleError(event serverEvent, raw []byte) {
	code, kind, clientEventID := "", "", ""
	if event.Error != nil {
		code, kind, clientEventID = event.Error.Code, event.Error.Type, event.Error.ClientEventID
	}
	// A rejected command names the command it rejects; the session goes on.
	// Moderation cut-offs also arrive as non-fatal errors and are followed by
	// session.closed only when the vendor actually ends the session.
	fatal := !s.ready.Load() || kind == "server_error" || code == "session_expired" || code == "invalid_api_key" || code == "insufficient_quota" || code == "rate_limit_exceeded"
	if !fatal || clientEventID != "" && s.ready.Load() {
		s.providerEvent(event.Type, raw)
		s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Extensions: s.extension(raw)})
		return
	}
	stable, retryable := "provider_rejected_request", false
	switch {
	case code == "invalid_api_key" || kind == "authentication_error":
		stable = "provider_authentication_failed"
	case code == "rate_limit_exceeded" || code == "insufficient_quota":
		stable, retryable = "provider_rate_limited", true
	case kind == "server_error" || code == "session_expired":
		stable, retryable = "provider_unavailable", true
	}
	err := &runtimepkg.ProviderError{Code: stable, Message: "openai live reported an error", Retryable: retryable, Extensions: s.extension(raw)}
	s.settleSetup(err)
	s.emit(runtimepkg.ProviderEvent{Err: err})
}

// finish classifies the socket ending. A close after session.closed is the
// clean end; a close while we were draining is finalization_incomplete; any
// other close is the provider going away with usage unconfirmed.
func (s *liveStream) finish(err error) {
	select {
	case <-s.closedEvent:
		return
	default:
	}
	if s.closing.Load() {
		s.setTerminal(&runtimepkg.ProviderError{
			Code:    "finalization_incomplete",
			Message: "openai live closed the socket before session.closed; final voice usage is unconfirmed",
			Hint:    "The session ended, but its final usage could not be confirmed with the provider.",
		})
		return
	}
	if s.closed.Load() && (isNormalClose(err) || s.ctx.Err() != nil) {
		return
	}
	failure := &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "openai live closed the session without session.closed; final voice usage is unconfirmed", Retryable: true, Cause: err}
	if status := websocket.CloseStatus(err); status != -1 {
		failure.Extensions = map[string]json.RawMessage{ExtensionID: marshalData(map[string]any{"close_status": int(status)})}
	}
	s.settleSetup(failure)
	s.emit(runtimepkg.ProviderEvent{Err: failure})
}

func (s *liveStream) extension(raw []byte) map[string]json.RawMessage {
	return map[string]json.RawMessage{ExtensionID: append(json.RawMessage(nil), raw...)}
}

func dialError(response *http.Response, err error) error {
	status := 0
	if response != nil {
		status = response.StatusCode
	}
	code := "provider_unavailable"
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		code = "provider_authentication_failed"
	case status == http.StatusBadRequest || status == http.StatusNotFound || status == http.StatusUnprocessableEntity:
		code = "provider_rejected_request"
	case status == http.StatusTooManyRequests:
		code = "provider_rate_limited"
	}
	return &runtimepkg.ProviderError{
		Code: code, Message: "openai live connection could not be established",
		Retryable:      status == 0 || status == http.StatusTooManyRequests || status >= 500,
		ProviderStatus: status, Cause: err,
	}
}

func marshalData(value any) json.RawMessage {
	payload, _ := json.Marshal(value)
	return payload
}

func isNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}
