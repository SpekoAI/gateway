package cartesia

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

const (
	// AdapterID is the identifier returned by a Cartesia TTS session plan.
	AdapterID   = "cartesia.tts.v1"
	extensionID = "cartesia.ai/v1"

	defaultTTSVersion = "2026-03-01"
	officialAPIHost   = "api.cartesia.ai"
)

// Config controls local transport limits and the explicitly selected Cartesia
// API version. Provider identity, model, voice, and access token come from a
// session plan and its provider-neutral request options.
type Config struct {
	AdapterID             string
	Version               string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// Adapter implements Cartesia's /tts/websocket streaming API.
type Adapter struct {
	id              string
	version         string
	httpClient      *http.Client
	eventBuffer     int
	maxMessageBytes int64
	endpointPolicy  upstream.WebSocketPolicy
}

// New creates a bounded Cartesia TTS adapter.
func New(config Config) (*Adapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = AdapterID
	}
	if config.Version == "" {
		config.Version = defaultTTSVersion
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = 32
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = 1 << 20
	}
	if config.EventBuffer < 1 {
		return nil, errors.New("cartesia event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("cartesia maximum message bytes must be positive")
	}
	endpointPolicy, err := upstream.NewWebSocketPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &Adapter{
		id:              config.AdapterID,
		version:         config.Version,
		httpClient:      config.HTTPClient,
		eventBuffer:     config.EventBuffer,
		maxMessageBytes: config.MaxMessageBytes,
		endpointPolicy:  endpointPolicy,
	}, nil
}

func (a *Adapter) ID() string { return a.id }

// Open opens a Cartesia TTS WebSocket. Customer API keys use the X-API-Key
// header; short-lived managed access tokens use the documented handshake query
// parameter. Relay plans carry the connector's permanent key and therefore use
// the header form as well.
func (a *Adapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindTTS {
		return nil, fmt.Errorf("cartesia supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "cartesia" {
		return nil, fmt.Errorf("cartesia adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("cartesia requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("cartesia requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("cartesia media: %w", err)
	}
	if err := validateGenerationOptions(request.Plan.Route.Model, request.Options, *request.Media); err != nil {
		return nil, err
	}
	settings, err := ttsOptions(request.Plan.Route.Model, request.Options.TTS.Provider("cartesia"))
	if err != nil {
		return nil, err
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("cartesia requires a bearer credential")
	}

	endpoint, err := websocketEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}
	headers := make(http.Header)
	headers.Set("Cartesia-Version", a.version)
	// A relay plan is managed for billing purposes but carries the connector's
	// permanent Cartesia key, which belongs in the X-API-Key header exactly like
	// a BYOK key. The access_token query channel stays reserved for the
	// short-lived tokens of managed provider-direct routes.
	if request.Plan.Execution.ProviderRoute == protocol.RouteSpekoRelay {
		headers.Set("X-API-Key", credential.Value)
	} else if request.Plan.Execution.CredentialSource == protocol.CredentialsBYOK {
		headers.Set("X-API-Key", credential.Value)
	} else {
		endpoint, err = addAccessToken(endpoint, credential.Value)
		if err != nil {
			return nil, err
		}
	}
	dial := func(ctx context.Context) (*websocket.Conn, string, error) {
		conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: httpClient(a.httpClient), HTTPHeader: headers})
		if err != nil {
			status := 0
			if response != nil {
				status = response.StatusCode
			}
			return nil, "", &runtimepkg.ProviderError{
				Code:           dialErrorCode(status),
				Message:        "Cartesia streaming connection could not be established",
				Retryable:      status == 0 || status == http.StatusTooManyRequests || status >= 500,
				ProviderStatus: status,
				Cause:          err,
			}
		}
		conn.SetReadLimit(a.maxMessageBytes)
		requestID := ""
		if response != nil {
			requestID = response.Header.Get("X-Request-ID")
		}
		return conn, requestID, nil
	}
	conn, requestID, err := dial(ctx)
	if err != nil {
		return nil, err
	}
	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &stream{
		dial:     dial,
		conn:     conn,
		ctx:      streamCtx,
		cancel:   cancel,
		events:   make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		modelID:  request.Plan.Route.Model,
		voiceID:  request.Options.Voice,
		language: request.Options.Language,
		media:    *request.Media,

		settings: settings,
	}
	if stream.setProviderRequestID(requestID) {
		if err := stream.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: usageData(requestID)}); err != nil {
			_ = stream.abort()
			return nil, err
		}
	}
	go stream.readLoop(conn)
	return stream, nil
}

func httpClient(client *http.Client) *http.Client {
	if client != nil {
		return client
	}
	return http.DefaultClient
}

// acceptableCredentialKind reports whether a delegated credential's kind may
// authenticate the plan's route. Bearer is the norm everywhere; the relay arm
// additionally accepts relay_access, because protocol.SessionPlan validation
// requires relay plans to label their credential relay_access while a relay
// connector that synthesizes the plan and drives this adapter directly — no
// Engine, no SessionPlan.Validate — labels the same permanent Cartesia key
// bearer. Both spellings carry a permanent key; the header-versus-query
// channel split keys off the route, not the kind, so nothing else on the
// relay arm changes.
func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

func websocketEndpoint(policy upstream.WebSocketPolicy, rawEndpoint string) (string, error) {
	endpoint, err := policy.Parse(rawEndpoint)
	if err != nil {
		return "", fmt.Errorf("cartesia endpoint: %w", err)
	}
	if endpoint.Path != "/tts/websocket" {
		return "", fmt.Errorf("cartesia endpoint path must be /tts/websocket, got %q", endpoint.Path)
	}
	return endpoint.String(), nil
}

func addAccessToken(rawEndpoint, token string) (string, error) {
	endpoint, err := url.Parse(rawEndpoint)
	if err != nil {
		return "", errors.New("cartesia endpoint could not be prepared")
	}
	query := endpoint.Query()
	query.Set("access_token", token)
	endpoint.RawQuery = query.Encode()
	return endpoint.String(), nil
}

func validateGenerationOptions(model string, options protocol.RequestOptions, media protocol.MediaFormat) error {
	if strings.TrimSpace(model) == "" || model == "auto" {
		return errors.New("cartesia requires a concrete model in the session plan")
	}
	if strings.TrimSpace(options.Voice) == "" {
		return errors.New("cartesia requires a voice id in request options")
	}
	if strings.TrimSpace(options.Language) == "" {
		return errors.New("cartesia requires a language in request options")
	}
	if media.Encoding != "pcm_s16le" {
		return fmt.Errorf("cartesia streaming output requires pcm_s16le, got %q", media.Encoding)
	}
	return nil
}

// stream keeps one client session on one Cartesia socket at a time. Two
// things Cartesia ends on its own are absorbed here instead of ending the
// session:
//
//   - A context expires one second after its last audio output
//     (https://docs.cartesia.ai/use-the-api/tts-websocket/contexts.md), so a
//     caller who pauses inside an utterance outlives it. The caller's
//     utterance stays open: its next fragment opens a fresh context and its
//     commit finishes the utterance, sending nothing when no context is left.
//   - A socket that closes while no context is in flight is replaced by the
//     next fragment. A socket that closes with a context in flight fails the
//     session with a retryable error, since that audio is lost.
type stream struct {
	dial   func(context.Context) (*websocket.Conn, string, error)
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent
	// eventsMu lets the writer side emit (a commit that finishes an expired
	// utterance) while a reader may close events: emitters hold it shared,
	// finish holds it exclusively after cancelling ctx.
	eventsMu     sync.RWMutex
	eventsClosed bool
	finishOnce   sync.Once

	modelID  string
	voiceID  string
	language string
	media    protocol.MediaFormat
	// settings are the caller's provider_options.cartesia, sent on every
	// generation of every context; the zero value leaves vendor defaults.
	settings ttsSettings

	writeMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	closing      atomic.Bool
	closeErr     error

	stateMu sync.Mutex
	// conn is nil after Cartesia closed an idle socket, until the next
	// fragment dials again.
	conn *websocket.Conn
	// contextID is the live Cartesia context; contextDone closes when it
	// ends. contextSent records that Cartesia has seen it, which is what
	// makes a lost socket lose speech.
	contextID     string
	contextDone   chan struct{}
	contextCommit bool
	contextCancel bool
	contextSent   bool
	// utterance is the caller's utterance, which outlives a context Cartesia
	// expired; lastContextID names that expired context for the done event.
	utterance     bool
	lastContextID string
	audioStarted  bool
	// audioBytes counts the utterance's audio so far; timeOffset is where
	// the live context's own clock starts within the utterance, in seconds.
	audioBytes        int
	timeOffset        float64
	providerRequestID string
}

func (s *stream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

func (s *stream) WriteAudio(context.Context, []byte) error { return runtimepkg.ErrUnsupportedOperation }

func (s *stream) CommitAudio(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// AppendText starts or continues the current Cartesia context. It deliberately
// sets continue=true until CommitText supplies the explicit utterance boundary.
func (s *stream) AppendText(ctx context.Context, text string) error {
	// A streaming LLM may emit word separators as their own chunks. Preserve
	// them exactly: trimming or rejecting them breaks valid streamed speech.
	if text == "" {
		return errors.New("cartesia transcript is empty")
	}
	contextID, fresh, err := s.startOrCurrentContext()
	if err != nil {
		return err
	}
	request := s.generationRequest(contextID, text, true)
	err = s.writeJSON(ctx, request)
	if err != nil && fresh && ctx.Err() == nil && !errors.Is(err, runtimepkg.ErrSessionClosed) {
		// Cartesia has seen nothing of a fresh context, so the socket it
		// could not reach is replaced without losing speech.
		if err = s.redial(ctx); err == nil {
			err = s.writeJSON(ctx, request)
		}
	}
	if err != nil {
		s.finishContext(contextID)
		return err
	}
	s.markContextSent(contextID)
	return nil
}

// CommitText finalizes the active context without appending another transcript
// fragment, matching Cartesia's documented continuation protocol. When
// Cartesia already expired the utterance's context, all of its audio has been
// delivered, so the utterance finishes here without a vendor frame.
func (s *stream) CommitText(ctx context.Context) error {
	contextID, finished, err := s.commitContext()
	if err != nil {
		return err
	}
	if finished {
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: s.contextData(contextID)})
	}
	if err := s.writeJSON(ctx, s.generationRequest(contextID, "", false)); err != nil {
		s.finishContext(contextID)
		return err
	}
	return nil
}

// Cancel requests Cartesia to cancel queued work for the active context. The
// provider documents that currently generating audio can continue, so callers
// must still await its terminal event or close the session.
func (s *stream) Cancel(ctx context.Context) error {
	contextID, live, finished := s.cancelContext()
	if finished {
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: s.contextData(contextID)})
	}
	if !live {
		return runtimepkg.ErrSessionClosed
	}
	return s.writeJSON(ctx, map[string]any{"context_id": contextID, "cancel": true})
}

// Close waits for the active context's done response before closing the
// socket, preserving final audio when an application gracefully closes right
// after text.commit.
func (s *stream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		s.closing.Store(true)
		if done := s.activeDone(); done != nil {
			select {
			case <-done:
			case <-ctx.Done():
				s.closeErr = ctx.Err()
			}
		}
		if s.closeErr == nil {
			s.closed.Store(true)
			if conn := s.currentConn(); conn == nil {
				s.finish()
			} else if err := conn.Close(websocket.StatusNormalClosure, ""); err != nil {
				s.closeErr = err
			}
		}
		if s.closeErr != nil {
			_ = s.abort()
		}
	})
	return s.closeErr
}

// Abort immediately tears down the socket after a terminal runtime failure.
func (s *stream) Abort(context.Context) error { return s.abort() }

func (s *stream) abort() error {
	s.abortOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		conn := s.currentConn()
		if conn == nil {
			// No reader is left to close events.
			s.finish()
			return
		}
		if err := conn.CloseNow(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
		s.finishContext("")
	})
	return s.closeErr
}

// finish ends the stream once: whoever sees it end last — the reader of the
// current socket, or Close/Abort when Cartesia already closed an idle one —
// closes events.
func (s *stream) finish() {
	s.finishOnce.Do(func() {
		s.cancel()
		s.finishContext("")
		s.eventsMu.Lock()
		s.eventsClosed = true
		close(s.events)
		s.eventsMu.Unlock()
	})
}

func (s *stream) currentConn() *websocket.Conn {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.conn
}

// redial replaces the socket. The caller guarantees no context is in flight
// on the old one; its reader sees it was replaced and exits quietly.
func (s *stream) redial(ctx context.Context) error {
	s.stateMu.Lock()
	if s.closed.Load() || s.closing.Load() {
		s.stateMu.Unlock()
		return runtimepkg.ErrSessionClosed
	}
	old := s.conn
	s.conn = nil
	s.stateMu.Unlock()
	if old != nil {
		_ = old.CloseNow()
	}
	conn, requestID, err := s.dial(ctx)
	if err != nil {
		return err
	}
	s.stateMu.Lock()
	if s.closed.Load() || s.closing.Load() {
		s.stateMu.Unlock()
		_ = conn.CloseNow()
		return runtimepkg.ErrSessionClosed
	}
	s.conn = conn
	s.stateMu.Unlock()
	go s.readLoop(conn)
	if s.setProviderRequestID(requestID) {
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: usageData(requestID)})
	}
	return nil
}

// startOrCurrentContext returns the live context, or opens a fresh one —
// for a new utterance, or to continue one whose context Cartesia expired.
func (s *stream) startOrCurrentContext() (contextID string, fresh bool, err error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() || s.closing.Load() {
		return "", false, runtimepkg.ErrSessionClosed
	}
	if s.contextID != "" {
		if s.contextCommit {
			return "", false, errors.New("cartesia previous utterance has not completed")
		}
		return s.contextID, false, nil
	}
	contextID, err = newContextID()
	if err != nil {
		return "", false, err
	}
	if !s.utterance {
		s.utterance = true
		s.audioStarted = false
		s.audioBytes = 0
	}
	s.contextID = contextID
	s.contextDone = make(chan struct{})
	s.contextCommit = false
	s.contextCancel = false
	s.contextSent = false
	// A fresh context's timestamps start at zero; within a continued
	// utterance they start after the audio already sent (pcm_s16le mono).
	s.timeOffset = 0
	if s.media.SampleRateHz > 0 {
		s.timeOffset = float64(s.audioBytes) / float64(2*s.media.SampleRateHz)
	}
	return contextID, true, nil
}

// commitContext marks the live context committed. With no live context but
// an open utterance, Cartesia expired the context after its last audio, so the
// utterance ends here (finished) and names that context for the done event.
func (s *stream) commitContext() (contextID string, finished bool, err error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() || s.closing.Load() {
		return "", false, runtimepkg.ErrSessionClosed
	}
	if s.contextID == "" && s.utterance {
		contextID = s.lastContextID
		s.endUtteranceLocked()
		return contextID, true, nil
	}
	if s.contextID == "" || s.contextCommit {
		return "", false, runtimepkg.ErrSessionClosed
	}
	s.contextCommit = true
	return s.contextID, false, nil
}

// cancelContext marks the live context cancelled, or ends an utterance whose
// context Cartesia already expired (finished).
func (s *stream) cancelContext() (contextID string, live, finished bool) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.contextID != "" {
		s.contextCancel = true
		return s.contextID, true, false
	}
	if s.utterance {
		contextID = s.lastContextID
		s.endUtteranceLocked()
		return contextID, false, true
	}
	return "", false, false
}

func (s *stream) markContextSent(contextID string) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.contextID == contextID {
		s.contextSent = true
	}
}

func (s *stream) activeDone() <-chan struct{} {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.contextDone
}

// finishContext ends the live context and the utterance; an empty id ends
// whatever is open.
func (s *stream) finishContext(contextID string) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if contextID != "" && s.contextID != contextID {
		return
	}
	s.endUtteranceLocked()
}

func (s *stream) endUtteranceLocked() {
	if s.contextDone != nil {
		close(s.contextDone)
	}
	s.contextID = ""
	s.contextDone = nil
	s.contextCommit = false
	s.contextCancel = false
	s.contextSent = false
	s.utterance = false
	s.lastContextID = ""
	s.audioStarted = false
}

// expireContext reports whether a done frame is Cartesia expiring a context
// the caller has not finished. That context ends; the utterance stays open.
func (s *stream) expireContext(contextID string) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if contextID == "" || contextID != s.contextID || s.contextCommit || s.contextCancel || s.closing.Load() {
		return false
	}
	close(s.contextDone)
	s.lastContextID = contextID
	s.contextID = ""
	s.contextDone = nil
	s.contextSent = false
	return true
}

// observeAudio counts the live context's audio and reports whether it starts
// the utterance's audio.
func (s *stream) observeAudio(contextID string, bytes int) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.contextID != contextID {
		return false
	}
	s.audioBytes += bytes
	if s.audioStarted {
		return false
	}
	s.audioStarted = true
	return true
}

func (s *stream) contextTimeOffset(contextID string) float64 {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if contextID != s.contextID {
		return 0
	}
	return s.timeOffset
}

func (s *stream) generationRequest(contextID, transcript string, continuation bool) generation {
	return generation{
		ModelID:          s.modelID,
		Transcript:       transcript,
		Voice:            voice{Mode: "id", ID: s.voiceID},
		Language:         s.language,
		Normalization:    s.settings.normalization,
		MaxBufferDelayMs: s.settings.maxBufferDelay,
		GenerationConfig: s.settings.generationConfig(),
		ContextID:        contextID,
		OutputFormat: outputFormat{
			Container:  "raw",
			Encoding:   "pcm_s16le",
			SampleRate: s.media.SampleRateHz,
		},
		AddTimestamps: true,
		Continue:      continuation,
	}
}

func (s *stream) writeJSON(ctx context.Context, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	conn := s.currentConn()
	if conn == nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Cartesia streaming connection is closed", Retryable: true}
	}
	if err := conn.Write(ctx, websocket.MessageText, payload); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Cartesia streaming write failed", Retryable: true, Cause: err}
	}
	return nil
}

// readLoop reads one socket until it ends.
func (s *stream) readLoop(conn *websocket.Conn) {
	for {
		messageType, payload, err := conn.Read(s.ctx)
		if err != nil {
			s.socketEnded(conn, err)
			return
		}
		if messageType != websocket.MessageText {
			continue
		}
		if err := s.handleMessage(payload); err != nil {
			_ = s.emit(runtimepkg.ProviderEvent{Err: err})
			s.finish()
			return
		}
	}
}

// socketEnded decides what a closed socket means. Our own close ends the
// stream quietly; a socket already replaced by redial means nothing; Cartesia
// closing a socket with no context in flight leaves the stream waiting for
// the next fragment to dial again; and with a context in flight the speech is
// lost, so the stream fails with a retryable error.
func (s *stream) socketEnded(conn *websocket.Conn, err error) {
	s.stateMu.Lock()
	if s.conn != conn {
		s.stateMu.Unlock()
		return
	}
	ours := s.closed.Load() || s.ctx.Err() != nil
	inFlight := s.contextID != "" && s.contextSent
	if !ours && !inFlight && !s.closing.Load() {
		s.conn = nil
		s.stateMu.Unlock()
		return
	}
	s.stateMu.Unlock()
	if !ours && inFlight {
		_ = s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "Cartesia closed the stream mid-utterance",
			Hint:      "The provider connection closed while this utterance was being synthesized; retry the utterance on a new session.",
			Retryable: true,
			Cause:     err,
		}})
	}
	s.finish()
}

func (s *stream) handleMessage(payload []byte) error {
	var message inbound
	if err := json.Unmarshal(payload, &message); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Cartesia sent malformed streaming JSON", Retryable: true, Cause: err}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))
	if message.RequestID != "" && s.setProviderRequestID(message.RequestID) {
		if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: usageData(message.RequestID)}); err != nil {
			return err
		}
	}
	switch message.Type {
	case "chunk":
		audio, err := base64.StdEncoding.DecodeString(message.Data)
		if err != nil {
			return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Cartesia sent invalid audio data", Retryable: true, Cause: err}
		}
		if s.observeAudio(message.ContextID, len(audio)) {
			if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: s.contextData(message.ContextID), Extensions: extension(raw)}); err != nil {
				return err
			}
		}
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Data: s.contextData(message.ContextID), Extensions: extension(raw), Audio: audio})
	case "timestamps":
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAlignment, Data: s.alignmentData(message.ContextID, message.WordTimestamps), Extensions: extension(raw)})
	case "done":
		if s.expireContext(message.ContextID) {
			return nil
		}
		s.finishContext(message.ContextID)
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: s.contextData(message.ContextID), Extensions: extension(raw)})
	case "flush_done":
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: s.warningData(message.ContextID, "flush_done"), Extensions: extension(raw)})
	case "error":
		if message.StatusCode == http.StatusBadRequest && (isUnsupportedLanguage(message.Message) || isUnsupportedLanguage(errorText(message.Error))) {
			return &runtimepkg.ProviderError{
				Code:           "invalid_request",
				Message:        fmt.Sprintf("Cartesia model %s does not support language %q", s.modelID, s.language),
				Hint:           "Choose a Cartesia model that supports this language, or use another TTS provider for it.",
				Retryable:      false,
				ProviderStatus: message.StatusCode,
				Extensions:     extension(raw),
			}
		}
		// The frame is kept for its error_code (Cartesia-Version 2026-03-01 and
		// later), which SafeProviderCode reads.
		return &runtimepkg.ProviderError{Code: cartesiaErrorCode(message.StatusCode), Message: cartesiaErrorMessage(message.Message), Retryable: message.StatusCode >= 500, ProviderStatus: message.StatusCode, Extensions: extension(raw)}
	default:
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: s.warningData(message.ContextID, message.Type), Extensions: extension(raw)})
	}
}

func (s *stream) emit(event runtimepkg.ProviderEvent) error {
	s.eventsMu.RLock()
	defer s.eventsMu.RUnlock()
	if s.eventsClosed {
		return runtimepkg.ErrSessionClosed
	}
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func newContextID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("generate Cartesia context id: %w", err)
	}
	return hex.EncodeToString(bytes), nil
}

func isNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}

func dialErrorCode(status int) string {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return "authentication_failed"
	case http.StatusTooManyRequests:
		return "provider_rate_limited"
	default:
		return "provider_unavailable"
	}
}

func cartesiaErrorCode(status int) string {
	if status == http.StatusTooManyRequests {
		return "provider_rate_limited"
	}
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		return "authentication_failed"
	}
	return "provider_unavailable"
}

func extension(raw json.RawMessage) map[string]json.RawMessage {
	return map[string]json.RawMessage{extensionID: raw}
}

func (s *stream) setProviderRequestID(value string) bool {
	if value == "" {
		return false
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.providerRequestID == value {
		return false
	}
	s.providerRequestID = value
	return true
}

func (s *stream) currentProviderRequestID() string {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.providerRequestID
}

func (s *stream) contextData(contextID string) json.RawMessage {
	return marshalData(map[string]any{"context_id": contextID, "provider_request_id": s.currentProviderRequestID()})
}

// alignmentData carries Cartesia's own word_timestamps block verbatim and
// adds the normalized span reading beside it. Cartesia measures per WORD and
// reports start and END times in fractional seconds.
//
// A block that does not parse, or whose three parallel arrays disagree, is
// carried raw with no spans: the consumer's contract is that absent spans
// mean "nothing measured", so a garbled frame goes out silent rather than
// wrong.
//
// Each Cartesia context starts its clock at zero, so the spans of a context
// that continues an utterance after an expired one are shifted by the audio
// already sent; the raw block keeps Cartesia's own clock.
func (s *stream) alignmentData(contextID string, timestamps json.RawMessage) json.RawMessage {
	payload := map[string]any{"context_id": contextID, "word_timestamps": timestamps, "provider_request_id": s.currentProviderRequestID()}
	var block struct {
		Words []string  `json:"words"`
		Start []float64 `json:"start"`
		End   []float64 `json:"end"`
	}
	if err := json.Unmarshal(timestamps, &block); err == nil {
		if offset := s.contextTimeOffset(contextID); offset > 0 {
			for index := range block.Start {
				block.Start[index] += offset
			}
			for index := range block.End {
				block.End[index] += offset
			}
		}
		timings := protocol.TimingSpansFromSeconds(block.Words, block.Start, block.End, protocol.TimingGranularityWord)
		if len(timings.Spans) > 0 {
			payload["granularity"], payload["spans"] = timings.Granularity, timings.Spans
		}
	}
	return marshalData(payload)
}

func (s *stream) warningData(contextID, messageType string) json.RawMessage {
	return marshalData(map[string]any{"context_id": contextID, "provider_type": messageType, "provider_request_id": s.currentProviderRequestID()})
}

func usageData(requestID string) json.RawMessage {
	return marshalData(map[string]any{"provider_request_id": requestID})
}

func marshalData(value any) json.RawMessage {
	payload, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{"code":"internal"}`)
	}
	return payload
}

// isUnsupportedLanguage recognizes Cartesia's explicit language refusal, a 400
// error frame reading "Your request was invalid: unsupported language 'sw'. See
// …/supported-locales …" (verified live 2026-10-09). The adapter's API version
// carries it in `message`; older versions put it in `error`, so both are read.
func isUnsupportedLanguage(text string) bool {
	return strings.Contains(strings.ToLower(text), "unsupported language")
}

// errorText reads the error frame's `error` field when it is a plain string.
func errorText(raw json.RawMessage) string {
	var text string
	if json.Unmarshal(raw, &text) != nil {
		return ""
	}
	return text
}

func cartesiaErrorMessage(message string) string {
	if strings.TrimSpace(message) == "" {
		return "Cartesia reported a streaming error"
	}
	return "Cartesia reported a streaming error: " + message
}

type voice struct {
	Mode string `json:"mode"`
	ID   string `json:"id"`
}

type outputFormat struct {
	Container  string `json:"container"`
	Encoding   string `json:"encoding"`
	SampleRate int    `json:"sample_rate"`
}

type generation struct {
	ModelID          string            `json:"model_id"`
	Transcript       string            `json:"transcript"`
	Voice            voice             `json:"voice"`
	Language         string            `json:"language"`
	Normalization    string            `json:"normalization,omitempty"`
	MaxBufferDelayMs *int              `json:"max_buffer_delay_ms,omitempty"`
	GenerationConfig *generationConfig `json:"generation_config,omitempty"`
	ContextID        string            `json:"context_id"`
	OutputFormat     outputFormat      `json:"output_format"`
	AddTimestamps    bool              `json:"add_timestamps"`
	Continue         bool              `json:"continue"`
}

// generationConfig is the request's generation_config object.
type generationConfig struct {
	Speed float64 `json:"speed"`
}

type inbound struct {
	Type           string          `json:"type"`
	Data           string          `json:"data"`
	Done           bool            `json:"done"`
	StatusCode     int             `json:"status_code"`
	ContextID      string          `json:"context_id"`
	Message        string          `json:"message"`
	Error          json.RawMessage `json:"error"`
	RequestID      string          `json:"request_id"`
	WordTimestamps json.RawMessage `json:"word_timestamps"`
}
