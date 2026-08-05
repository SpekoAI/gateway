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

	defaultVersion  = "2026-03-01"
	officialAPIHost = "api.cartesia.ai"
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
		config.Version = defaultVersion
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

// Open opens a provider-direct Cartesia TTS WebSocket. Customer API keys use
// the X-API-Key header; short-lived managed access tokens use the documented
// handshake query parameter.
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
	credential := request.Plan.Route.Credential
	if credential == nil || credential.Kind != protocol.CredentialBearer || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("cartesia requires a bearer credential")
	}

	endpoint, err := websocketEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}
	headers := make(http.Header)
	headers.Set("Cartesia-Version", a.version)
	if request.Plan.Execution.CredentialSource == protocol.CredentialsBYOK {
		headers.Set("X-API-Key", credential.Value)
	} else {
		endpoint, err = addAccessToken(endpoint, credential.Value)
		if err != nil {
			return nil, err
		}
	}
	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: httpClient(a.httpClient), HTTPHeader: headers})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		return nil, &runtimepkg.ProviderError{
			Code:           dialErrorCode(status),
			Message:        "Cartesia streaming connection could not be established",
			Retryable:      status == 0 || status == http.StatusTooManyRequests || status >= 500,
			ProviderStatus: status,
			Cause:          err,
		}
	}
	conn.SetReadLimit(a.maxMessageBytes)
	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &stream{
		conn:     conn,
		ctx:      streamCtx,
		cancel:   cancel,
		events:   make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		modelID:  request.Plan.Route.Model,
		voiceID:  request.Options.Voice,
		language: request.Options.Language,
		media:    *request.Media,
	}
	if response != nil {
		stream.setProviderRequestID(response.Header.Get("X-Request-ID"))
	}
	if requestID := stream.currentProviderRequestID(); requestID != "" {
		if err := stream.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: usageData(requestID)}); err != nil {
			_ = stream.abort()
			return nil, err
		}
	}
	go stream.readLoop()
	return stream, nil
}

func httpClient(client *http.Client) *http.Client {
	if client != nil {
		return client
	}
	return http.DefaultClient
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

type stream struct {
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent

	modelID  string
	voiceID  string
	language string
	media    protocol.MediaFormat

	writeMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	closing      atomic.Bool
	closeErr     error

	stateMu           sync.Mutex
	contextID         string
	contextDone       chan struct{}
	contextCommit     bool
	audioStarted      bool
	providerRequestID string
}

func (s *stream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

func (s *stream) WriteAudio(context.Context, []byte) error { return runtimepkg.ErrUnsupportedOperation }

func (s *stream) CommitAudio(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// AppendText starts or continues the current Cartesia context. It deliberately
// sets continue=true until CommitText supplies the explicit utterance boundary.
func (s *stream) AppendText(ctx context.Context, text string) error {
	if strings.TrimSpace(text) == "" {
		return errors.New("cartesia transcript is empty")
	}
	contextID, err := s.startOrCurrentContext()
	if err != nil {
		return err
	}
	if err := s.writeJSON(ctx, s.generationRequest(contextID, text, true)); err != nil {
		s.finishContext(contextID)
		return err
	}
	return nil
}

// CommitText finalizes the active context without appending another transcript
// fragment, matching Cartesia's documented continuation protocol.
func (s *stream) CommitText(ctx context.Context) error {
	contextID, err := s.commitContext()
	if err != nil {
		return err
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
	contextID, ok := s.activeContext()
	if !ok {
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
			if err := s.conn.Close(websocket.StatusNormalClosure, ""); err != nil {
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
		if err := s.conn.CloseNow(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
		s.finishContext("")
	})
	return s.closeErr
}

func (s *stream) startOrCurrentContext() (string, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() || s.closing.Load() {
		return "", runtimepkg.ErrSessionClosed
	}
	if s.contextID != "" {
		if s.contextCommit {
			return "", errors.New("cartesia previous utterance has not completed")
		}
		return s.contextID, nil
	}
	contextID, err := newContextID()
	if err != nil {
		return "", err
	}
	s.contextID = contextID
	s.contextDone = make(chan struct{})
	s.contextCommit = false
	s.audioStarted = false
	return contextID, nil
}

func (s *stream) commitContext() (string, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() || s.closing.Load() || s.contextID == "" || s.contextCommit {
		return "", runtimepkg.ErrSessionClosed
	}
	s.contextCommit = true
	return s.contextID, nil
}

func (s *stream) activeContext() (string, bool) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.contextID, s.contextID != ""
}

func (s *stream) activeDone() <-chan struct{} {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.contextDone
}

func (s *stream) finishContext(contextID string) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.contextID == "" || (contextID != "" && s.contextID != contextID) {
		return
	}
	close(s.contextDone)
	s.contextID = ""
	s.contextDone = nil
	s.contextCommit = false
	s.audioStarted = false
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

func (s *stream) generationRequest(contextID, transcript string, continuation bool) generation {
	return generation{
		ModelID:    s.modelID,
		Transcript: transcript,
		Voice:      voice{Mode: "id", ID: s.voiceID},
		Language:   s.language,
		ContextID:  contextID,
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
	if err := s.conn.Write(ctx, websocket.MessageText, payload); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Cartesia streaming write failed", Retryable: true, Cause: err}
	}
	return nil
}

func (s *stream) readLoop() {
	defer func() {
		s.cancel()
		s.finishContext("")
		close(s.events)
	}()
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && s.ctx.Err() == nil && !isNormalClose(err) {
				s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Cartesia streaming read failed", Retryable: true, Cause: err}})
			}
			return
		}
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
		if s.markAudioStarted(message.ContextID) {
			if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: s.contextData(message.ContextID), Extensions: extension(raw)}); err != nil {
				return err
			}
		}
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Data: s.contextData(message.ContextID), Extensions: extension(raw), Audio: audio})
	case "timestamps":
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAlignment, Data: s.alignmentData(message.ContextID, message.WordTimestamps), Extensions: extension(raw)})
	case "done":
		s.finishContext(message.ContextID)
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: s.contextData(message.ContextID), Extensions: extension(raw)})
	case "flush_done":
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: s.warningData(message.ContextID, "flush_done"), Extensions: extension(raw)})
	case "error":
		return &runtimepkg.ProviderError{Code: cartesiaErrorCode(message.StatusCode), Message: cartesiaErrorMessage(message.Message), Retryable: message.StatusCode >= 500, ProviderStatus: message.StatusCode}
	default:
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: s.warningData(message.ContextID, message.Type), Extensions: extension(raw)})
	}
}

func (s *stream) emit(event runtimepkg.ProviderEvent) error {
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

func (s *stream) alignmentData(contextID string, timestamps json.RawMessage) json.RawMessage {
	return marshalData(map[string]any{"context_id": contextID, "word_timestamps": timestamps, "provider_request_id": s.currentProviderRequestID()})
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
	ModelID       string       `json:"model_id"`
	Transcript    string       `json:"transcript"`
	Voice         voice        `json:"voice"`
	Language      string       `json:"language"`
	ContextID     string       `json:"context_id"`
	OutputFormat  outputFormat `json:"output_format"`
	AddTimestamps bool         `json:"add_timestamps"`
	Continue      bool         `json:"continue"`
}

type inbound struct {
	Type           string          `json:"type"`
	Data           string          `json:"data"`
	Done           bool            `json:"done"`
	StatusCode     int             `json:"status_code"`
	ContextID      string          `json:"context_id"`
	Message        string          `json:"message"`
	RequestID      string          `json:"request_id"`
	WordTimestamps json.RawMessage `json:"word_timestamps"`
}
