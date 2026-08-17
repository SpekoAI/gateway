package maya

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

const (
	AdapterID               = "maya.tts.v1"
	DefaultModel            = "Maya 2 Native"
	DefaultVoice            = "Ananya"
	officialHost            = "tts.mayaresearch.ai"
	streamPath              = "/v1/tts/stream"
	extensionID             = "mayaresearch.ai/tts/v2"
	outputSampleHz          = 24_000
	defaultMaxBytes         = 16 << 20
	defaultCloseIdleTimeout = 30 * time.Second
)

var (
	streamingModels = map[string]struct{}{
		"Maya 2 Native": {}, "Maya 2 Native Emotional": {},
	}
	voices    = map[string]struct{}{"Ananya": {}, "Arjun": {}}
	languages = map[string]struct{}{
		"hi": {}, "te": {}, "bn": {}, "gu": {}, "kn": {}, "ml": {},
		"mr": {}, "or": {}, "pa": {}, "ta": {}, "en": {},
	}
)

type Config struct {
	AdapterID                string
	HTTPClient               *http.Client
	EventBuffer              int
	MaxMessageBytes          int64
	GracefulCloseIdleTimeout time.Duration
	AllowedEndpointHosts     []string
	AllowInsecureEndpoint    bool
}

type Adapter struct {
	id                       string
	httpClient               *http.Client
	eventBuffer              int
	maxMessageBytes          int64
	gracefulCloseIdleTimeout time.Duration
	endpointPolicy           upstream.WebSocketPolicy
}

func New(config Config) (*Adapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = AdapterID
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = 32
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = defaultMaxBytes
	}
	if config.GracefulCloseIdleTimeout == 0 {
		config.GracefulCloseIdleTimeout = defaultCloseIdleTimeout
	}
	if config.EventBuffer < 1 {
		return nil, errors.New("maya event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("maya maximum message bytes must be positive")
	}
	if config.GracefulCloseIdleTimeout < 0 {
		return nil, errors.New("maya graceful close idle timeout must be positive")
	}
	policy, err := upstream.NewWebSocketPolicy(officialHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &Adapter{
		id: config.AdapterID, httpClient: config.HTTPClient, eventBuffer: config.EventBuffer,
		maxMessageBytes: config.MaxMessageBytes, gracefulCloseIdleTimeout: config.GracefulCloseIdleTimeout,
		endpointPolicy: policy,
	}, nil
}

func (a *Adapter) ID() string { return a.id }

func (a *Adapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindTTS {
		return nil, fmt.Errorf("maya supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "maya" {
		return nil, fmt.Errorf("maya adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("maya tts requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Plan.Execution.CredentialSource == protocol.CredentialsManaged && request.Plan.Execution.ProviderRoute != protocol.RouteSpekoRelay {
		return nil, errors.New("maya managed provider-direct routing is unavailable because Maya does not issue short-lived session credentials")
	}
	if request.Media == nil {
		return nil, errors.New("maya tts requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("maya tts media: %w", err)
	}
	if request.Media.Encoding != "pcm_s16le" || request.Media.Channels != 1 || request.Media.SampleRateHz != outputSampleHz {
		return nil, fmt.Errorf("maya tts requires mono pcm_s16le output at %d Hz", outputSampleHz)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if _, ok := streamingModels[model]; !ok {
		return nil, fmt.Errorf("maya realtime tts does not support model %q", model)
	}
	voice := strings.TrimSpace(request.Options.Voice)
	if voice == "" {
		voice = strings.TrimSpace(request.Plan.Route.Voice)
	}
	if voice == "" {
		voice = DefaultVoice
	}
	if _, ok := voices[voice]; !ok {
		return nil, fmt.Errorf("maya tts does not support voice %q", voice)
	}
	language := strings.ToLower(strings.TrimSpace(request.Options.Language))
	if language == "auto" {
		language = ""
	}
	if language != "" {
		if _, ok := languages[language]; !ok {
			return nil, fmt.Errorf("maya tts does not support language %q", language)
		}
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("maya tts requires a bearer credential")
	}
	endpoint, err := mayaEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}

	header := http.Header{}
	header.Set("Authorization", "Bearer "+credential.Value)
	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: a.httpClient, HTTPHeader: header})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		code, retryable := statusCode(status)
		return nil, &runtimepkg.ProviderError{Code: code, Message: "Maya realtime TTS connection could not be established", Retryable: retryable, ProviderStatus: status, Cause: err}
	}
	conn.SetReadLimit(a.maxMessageBytes)

	start := map[string]any{"type": "start", "v2": true, "voice": voice, "model": model}
	if language != "" {
		start["language"] = language
	}
	if err := writeJSON(ctx, conn, start); err != nil {
		_ = conn.CloseNow()
		return nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Maya realtime TTS start failed", Retryable: true, Cause: err}
	}
	messageType, payload, err := conn.Read(ctx)
	if err != nil {
		_ = conn.CloseNow()
		return nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Maya realtime TTS metadata could not be read", Retryable: true, Cause: err}
	}
	metadata, err := parseMetadata(messageType, payload)
	if err != nil {
		_ = conn.CloseNow()
		return nil, err
	}

	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &stream{
		conn: conn, ctx: streamCtx, cancel: cancel, events: make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		progress: make(chan struct{}, 1), readDone: make(chan struct{}), sessionID: metadata.SessionID,
		gracefulCloseIdleTimeout: a.gracefulCloseIdleTimeout,
	}
	stream.events <- runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: marshalData(map[string]any{"provider_request_id": metadata.SessionID}), Extensions: extension(payload)}
	go stream.readLoop()
	return stream, nil
}

func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

func mayaEndpoint(policy upstream.WebSocketPolicy, raw string) (string, error) {
	endpoint, err := policy.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("maya tts endpoint: %w", err)
	}
	if endpoint.Path != streamPath {
		return "", fmt.Errorf("maya tts endpoint path must be %s, got %q", streamPath, endpoint.Path)
	}
	return endpoint.String(), nil
}

type stream struct {
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent

	sessionID                string
	gracefulCloseIdleTimeout time.Duration
	progress                 chan struct{}
	readDone                 chan struct{}
	writeMu                  sync.Mutex
	closeOnce                sync.Once
	abortOnce                sync.Once
	closed                   atomic.Bool
	closeErr                 error

	stateMu      sync.Mutex
	contextID    string
	turnDone     chan struct{}
	inputClosed  bool
	audioStarted bool
	cancelled    bool
}

func (s *stream) Events() <-chan runtimepkg.ProviderEvent  { return s.events }
func (s *stream) WriteAudio(context.Context, []byte) error { return runtimepkg.ErrUnsupportedOperation }
func (s *stream) CommitAudio(context.Context) error        { return runtimepkg.ErrUnsupportedOperation }

func (s *stream) AppendText(ctx context.Context, text string) error {
	if strings.TrimSpace(text) == "" {
		return errors.New("maya tts text is empty")
	}
	contextID, err := s.openOrCurrentTurn()
	if err != nil {
		return err
	}
	return s.send(ctx, map[string]any{"type": "text", "context_id": contextID, "text": text, "continue": true})
}

func (s *stream) CommitText(ctx context.Context) error {
	contextID, err := s.markInputClosed()
	if err != nil {
		return err
	}
	return s.send(ctx, map[string]any{"type": "text", "context_id": contextID, "continue": false})
}

func (s *stream) Cancel(ctx context.Context) error {
	s.stateMu.Lock()
	if s.contextID == "" || s.closed.Load() {
		s.stateMu.Unlock()
		return runtimepkg.ErrSessionClosed
	}
	contextID := s.contextID
	s.cancelled = true
	s.stateMu.Unlock()
	return s.send(ctx, map[string]any{"type": "cancel", "context_id": contextID})
}

func (s *stream) Close(ctx context.Context) error {
	s.closeOnce.Do(func() {
		if done := s.activeDone(); done != nil {
			s.closeErr = s.waitForTurn(ctx, done)
		}
		s.closed.Store(true)
		if s.closeErr == nil {
			if err := s.conn.Close(websocket.StatusNormalClosure, ""); err != nil {
				s.closeErr = err
			}
		}
		if s.closeErr != nil {
			_ = s.abort()
		}
		s.cancel()
		<-s.readDone
	})
	return s.closeErr
}

func (s *stream) Abort(context.Context) error { return s.abort() }

func (s *stream) abort() error {
	s.abortOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		if err := s.conn.CloseNow(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
	})
	return s.closeErr
}

func (s *stream) send(ctx context.Context, value any) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := writeJSON(ctx, s.conn, value); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Maya realtime TTS write failed", Retryable: true, Cause: err}
	}
	return nil
}

func writeJSON(ctx context.Context, conn *websocket.Conn, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, payload)
}

func (s *stream) readLoop() {
	defer func() {
		s.cancel()
		s.finishTurn("")
		close(s.events)
		close(s.readDone)
	}()
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && s.ctx.Err() == nil && !isNormalClose(err) {
				s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Maya realtime TTS read failed", Retryable: true, Cause: err}})
			}
			return
		}
		s.reportProgress()
		if messageType != websocket.MessageText {
			continue
		}
		if err := s.handleMessage(payload); err != nil {
			s.emit(runtimepkg.ProviderEvent{Err: err})
			return
		}
	}
}

func (s *stream) handleMessage(payload []byte) error {
	var message inboundMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Maya sent malformed realtime TTS JSON", Retryable: true, Cause: err}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))
	switch message.Type {
	case "audio":
		if !s.isActive(message.ContextID) {
			return nil
		}
		audio, err := base64.StdEncoding.DecodeString(message.Audio)
		if err != nil {
			return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Maya sent invalid realtime TTS audio", Retryable: true, Cause: err}
		}
		data := s.turnData(message.ContextID, false)
		if s.markAudioStarted(message.ContextID) {
			if !s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: data, Extensions: extension(raw)}) {
				return s.ctx.Err()
			}
		}
		if !s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Data: data, Extensions: extension(raw), Audio: audio}) {
			return s.ctx.Err()
		}
	case "end", "cancelled":
		if !s.isActive(message.ContextID) {
			return nil
		}
		cancelled := message.Type == "cancelled"
		if !cancelled && !s.hasAudioStarted(message.ContextID) {
			return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Maya realtime TTS completed without returning audio", Retryable: true, Extensions: extension(raw)}
		}
		if !s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: s.turnData(message.ContextID, cancelled), Extensions: extension(raw)}) {
			return s.ctx.Err()
		}
		s.finishTurn(message.ContextID)
	case "error":
		code, retryable := messageErrorCode(message.Error)
		return &runtimepkg.ProviderError{Code: code, Message: providerMessage("Maya reported a realtime TTS error", message.Error), Retryable: retryable, Extensions: extension(raw)}
	case "metadata", "pong":
		return nil
	default:
		return nil
	}
	return nil
}

func (s *stream) openOrCurrentTurn() (string, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() {
		return "", runtimepkg.ErrSessionClosed
	}
	if s.contextID != "" {
		if s.inputClosed || s.cancelled {
			return "", errors.New("maya tts previous turn has not completed")
		}
		return s.contextID, nil
	}
	contextID, err := newContextID()
	if err != nil {
		return "", err
	}
	s.contextID = contextID
	s.turnDone = make(chan struct{})
	s.inputClosed = false
	s.audioStarted = false
	s.cancelled = false
	return contextID, nil
}

func (s *stream) markInputClosed() (string, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() || s.contextID == "" || s.inputClosed || s.cancelled {
		return "", runtimepkg.ErrSessionClosed
	}
	s.inputClosed = true
	return s.contextID, nil
}

func (s *stream) activeDone() <-chan struct{} {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.turnDone
}

func (s *stream) isActive(contextID string) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return contextID != "" && s.contextID == contextID
}

func (s *stream) markAudioStarted(contextID string) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.contextID != contextID || s.audioStarted {
		return false
	}
	s.audioStarted = true
	return true
}

func (s *stream) hasAudioStarted(contextID string) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.contextID == contextID && s.audioStarted
}

func (s *stream) finishTurn(contextID string) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.contextID == "" || (contextID != "" && s.contextID != contextID) {
		return
	}
	close(s.turnDone)
	s.contextID = ""
	s.turnDone = nil
	s.inputClosed = false
	s.audioStarted = false
	s.cancelled = false
}

func (s *stream) turnData(contextID string, cancelled bool) json.RawMessage {
	return marshalData(map[string]any{"provider_request_id": s.sessionID, "context_id": contextID, "cancelled": cancelled})
}

func (s *stream) emit(event runtimepkg.ProviderEvent) bool {
	select {
	case s.events <- event:
		s.reportProgress()
		return true
	default:
	}
	select {
	case s.events <- event:
		s.reportProgress()
		return true
	case <-s.ctx.Done():
		return false
	}
}

func (s *stream) reportProgress() {
	select {
	case s.progress <- struct{}{}:
	default:
	}
}

func (s *stream) waitForTurn(ctx context.Context, done <-chan struct{}) error {
	timer := time.NewTimer(s.gracefulCloseIdleTimeout)
	defer timer.Stop()
	for {
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-s.progress:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(s.gracefulCloseIdleTimeout)
		case <-timer.C:
			select {
			case <-done:
				return nil
			default:
			}
			return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Maya realtime TTS stalled during graceful close", Retryable: true, Cause: context.DeadlineExceeded}
		}
	}
}

type inboundMessage struct {
	Type       string `json:"type"`
	SessionID  string `json:"session_id"`
	ContextID  string `json:"context_id"`
	Audio      string `json:"audio"`
	Error      string `json:"error"`
	SampleRate int    `json:"sample_rate"`
	Channels   int    `json:"channels"`
	Encoding   string `json:"encoding"`
}

func parseMetadata(messageType websocket.MessageType, payload []byte) (inboundMessage, error) {
	if messageType != websocket.MessageText {
		return inboundMessage{}, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Maya realtime TTS metadata was not JSON", Retryable: true}
	}
	var message inboundMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return inboundMessage{}, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Maya sent malformed realtime TTS metadata", Retryable: true, Cause: err}
	}
	if message.Type == "error" {
		code, retryable := messageErrorCode(message.Error)
		return inboundMessage{}, &runtimepkg.ProviderError{Code: code, Message: providerMessage("Maya rejected realtime TTS settings", message.Error), Retryable: retryable, Extensions: extension(payload)}
	}
	if message.Type != "metadata" || strings.TrimSpace(message.SessionID) == "" || message.SampleRate != outputSampleHz || message.Channels != 1 || message.Encoding != "pcm_s16le" {
		return inboundMessage{}, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Maya realtime TTS returned incompatible metadata", Retryable: true, Extensions: extension(payload)}
	}
	return message, nil
}

func statusCode(status int) (string, bool) {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return "authentication_failed", false
	case http.StatusTooManyRequests:
		return "rate_limited", true
	case http.StatusBadRequest:
		return "invalid_request", false
	default:
		return "provider_unavailable", status == 0 || status >= 500
	}
}

func messageErrorCode(message string) (string, bool) {
	lower := strings.ToLower(message)
	if strings.Contains(lower, "invalid") || strings.Contains(lower, "required") || strings.Contains(lower, "already completed") {
		return "invalid_request", false
	}
	return "provider_unavailable", true
}

func providerMessage(prefix, detail string) string {
	detail = strings.TrimSpace(detail)
	if detail == "" {
		return prefix
	}
	return prefix + ": " + detail
}

func extension(raw []byte) map[string]json.RawMessage {
	return map[string]json.RawMessage{extensionID: json.RawMessage(append([]byte(nil), raw...))}
}

func marshalData(value any) json.RawMessage {
	data, _ := json.Marshal(value)
	return data
}

func newContextID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate Maya context id: %w", err)
	}
	return hex.EncodeToString(value), nil
}

func isNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}
