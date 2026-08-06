package cartesia

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

const STTAdapterID = "cartesia.stt.v1"

// STTConfig controls Cartesia's manual-finalize streaming transcription
// adapter. Provider credentials always come from the verified session plan.
type STTConfig struct {
	AdapterID             string
	Version               string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// STTAdapter implements Cartesia's /stt/websocket protocol.
type STTAdapter struct {
	id              string
	version         string
	httpClient      *http.Client
	eventBuffer     int
	maxMessageBytes int64
	endpointPolicy  upstream.WebSocketPolicy
}

func NewSTT(config STTConfig) (*STTAdapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = STTAdapterID
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
		return nil, errors.New("cartesia STT event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("cartesia STT maximum message bytes must be positive")
	}
	policy, err := upstream.NewWebSocketPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &STTAdapter{
		id: config.AdapterID, version: config.Version, httpClient: config.HTTPClient,
		eventBuffer: config.EventBuffer, maxMessageBytes: config.MaxMessageBytes, endpointPolicy: policy,
	}, nil
}

func (a *STTAdapter) ID() string { return a.id }

func (a *STTAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindSTT {
		return nil, fmt.Errorf("cartesia STT supports stt sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "cartesia" {
		return nil, fmt.Errorf("cartesia STT adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("cartesia STT requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("cartesia STT requires media configuration")
	}
	if err := validateSTTOptions(request.Plan.Route.Model, request.Options, *request.Media); err != nil {
		return nil, err
	}
	credential := request.Plan.Route.Credential
	if credential == nil || credential.Kind != protocol.CredentialBearer || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("cartesia STT requires a bearer credential")
	}
	endpoint, err := cartesiaSTTEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint, request.Plan.Route.Model, request.Options, *request.Media, a.version)
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
			Code: dialErrorCode(status), Message: "Cartesia transcription connection could not be established",
			Retryable: status == 0 || status == http.StatusTooManyRequests || status >= 500, ProviderStatus: status, Cause: err,
		}
	}
	conn.SetReadLimit(a.maxMessageBytes)
	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &sttStream{
		conn: conn, ctx: streamCtx, cancel: cancel,
		events:         make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		seenRequestIDs: make(map[string]struct{}), speechStarted: make(map[string]struct{}),
	}
	if response != nil {
		stream.observeRequestID(response.Header.Get("X-Request-ID"), nil)
	}
	go stream.readLoop()
	return stream, nil
}

func validateSTTOptions(model string, options protocol.RequestOptions, media protocol.MediaFormat) error {
	if strings.TrimSpace(model) == "" || model == "auto" {
		return errors.New("cartesia STT requires a concrete model in the session plan")
	}
	if media.Encoding != "pcm_s16le" {
		return fmt.Errorf("cartesia STT requires pcm_s16le, got %q", media.Encoding)
	}
	if media.Channels != 1 {
		return fmt.Errorf("cartesia STT requires mono audio, got %d channels", media.Channels)
	}
	if media.SampleRateHz < 8_000 || media.SampleRateHz > 48_000 {
		return fmt.Errorf("cartesia STT sample rate must be between 8000 and 48000 Hz, got %d", media.SampleRateHz)
	}
	language := strings.TrimSpace(options.Language)
	if language != "" && language != "en" {
		return fmt.Errorf("cartesia STT currently supports language en, got %q", language)
	}
	return nil
}

func cartesiaSTTEndpoint(policy upstream.WebSocketPolicy, rawEndpoint, model string, options protocol.RequestOptions, media protocol.MediaFormat, version string) (string, error) {
	endpoint, err := policy.Parse(rawEndpoint)
	if err != nil {
		return "", fmt.Errorf("cartesia STT endpoint: %w", err)
	}
	if endpoint.Path != "/stt/websocket" {
		return "", fmt.Errorf("cartesia STT endpoint path must be /stt/websocket, got %q", endpoint.Path)
	}
	query := endpoint.Query()
	query.Set("model", model)
	query.Set("encoding", media.Encoding)
	query.Set("sample_rate", strconv.Itoa(media.SampleRateHz))
	query.Set("cartesia_version", version)
	if language := strings.TrimSpace(options.Language); language != "" {
		query.Set("language", language)
	}
	endpoint.RawQuery = query.Encode()
	return endpoint.String(), nil
}

type sttStream struct {
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent

	writeMu      sync.Mutex
	stateMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	closing      atomic.Bool
	closeErr     error

	seenRequestIDs map[string]struct{}
	speechStarted  map[string]struct{}
}

func (s *sttStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

func (s *sttStream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("cartesia STT audio is empty")
	}
	return s.write(ctx, websocket.MessageBinary, audio)
}

func (s *sttStream) CommitAudio(ctx context.Context) error {
	return s.write(ctx, websocket.MessageText, []byte("finalize"))
}

func (s *sttStream) AppendText(context.Context, string) error {
	return runtimepkg.ErrUnsupportedOperation
}
func (s *sttStream) CommitText(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

func (s *sttStream) Cancel(context.Context) error { return s.abort() }
func (s *sttStream) Abort(context.Context) error  { return s.abort() }

func (s *sttStream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		s.closing.Store(true)
		if err := s.write(ctx, websocket.MessageText, []byte("close")); err != nil {
			s.closeErr = err
			_ = s.abort()
		}
	})
	return s.closeErr
}

func (s *sttStream) abort() error {
	s.abortOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		if err := s.conn.CloseNow(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
	})
	return s.closeErr
}

func (s *sttStream) write(ctx context.Context, messageType websocket.MessageType, payload []byte) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if err := s.conn.Write(ctx, messageType, payload); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Cartesia transcription write failed", Retryable: true, Cause: err}
	}
	return nil
}

func (s *sttStream) readLoop() {
	defer func() {
		s.closed.Store(true)
		s.cancel()
		close(s.events)
	}()
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && !s.closing.Load() && s.ctx.Err() == nil && !isNormalClose(err) {
				s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Cartesia transcription read failed", Retryable: true, Cause: err}})
			}
			return
		}
		if messageType != websocket.MessageText {
			continue
		}
		done, err := s.handleMessage(payload)
		if err != nil {
			s.emit(runtimepkg.ProviderEvent{Err: err})
			return
		}
		if done {
			_ = s.conn.Close(websocket.StatusNormalClosure, "")
			return
		}
	}
}

func (s *sttStream) handleMessage(payload []byte) (bool, error) {
	var message sttInbound
	if err := json.Unmarshal(payload, &message); err != nil {
		return false, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Cartesia sent malformed transcription JSON", Retryable: true, Cause: err}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))
	if message.RequestID != "" {
		if err := s.observeRequestID(message.RequestID, raw); err != nil {
			return false, err
		}
	}
	switch message.Type {
	case "transcript":
		if message.Text == "" {
			return false, nil
		}
		if s.markSpeechStarted(message.RequestID) {
			if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventSpeechStarted, Data: marshalData(map[string]any{"provider_request_id": message.RequestID}), Extensions: extension(raw)}); err != nil {
				return false, err
			}
		}
		kind := protocol.EventTranscriptDelta
		if message.IsFinal {
			kind = protocol.EventTranscriptFinal
		}
		if err := s.emit(runtimepkg.ProviderEvent{Type: kind, Data: cartesiaTranscriptData(message), Extensions: extension(raw)}); err != nil {
			return false, err
		}
		if message.IsFinal {
			return false, s.emit(runtimepkg.ProviderEvent{Type: protocol.EventSpeechEnded, Data: marshalData(map[string]any{"audio_end_ms": millisecondsSTT(message.Duration), "provider_request_id": message.RequestID}), Extensions: extension(raw)})
		}
		return false, nil
	case "flush_done":
		return false, s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: marshalData(map[string]any{"provider_type": message.Type, "provider_request_id": message.RequestID}), Extensions: extension(raw)})
	case "done":
		return true, nil
	case "error":
		return false, &runtimepkg.ProviderError{
			Code: cartesiaErrorCode(message.StatusCode), Message: cartesiaErrorMessage(message.Message),
			Retryable:      message.StatusCode == 0 || message.StatusCode == http.StatusTooManyRequests || message.StatusCode >= 500,
			ProviderStatus: message.StatusCode, Extensions: extension(raw),
		}
	default:
		return false, s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: marshalData(map[string]any{"provider_type": message.Type, "provider_request_id": message.RequestID}), Extensions: extension(raw)})
	}
}

func (s *sttStream) observeRequestID(requestID string, raw json.RawMessage) error {
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return nil
	}
	s.stateMu.Lock()
	if _, exists := s.seenRequestIDs[requestID]; exists {
		s.stateMu.Unlock()
		return nil
	}
	s.seenRequestIDs[requestID] = struct{}{}
	s.stateMu.Unlock()
	event := runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: usageData(requestID)}
	if len(raw) > 0 {
		event.Extensions = extension(raw)
	}
	return s.emit(event)
}

func (s *sttStream) markSpeechStarted(requestID string) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if _, exists := s.speechStarted[requestID]; exists {
		return false
	}
	s.speechStarted[requestID] = struct{}{}
	return true
}

func (s *sttStream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func cartesiaTranscriptData(message sttInbound) json.RawMessage {
	return marshalData(map[string]any{
		"text": message.Text, "is_final": message.IsFinal, "audio_end_ms": millisecondsSTT(message.Duration),
		"language": message.Language, "words": message.Words, "provider_request_id": message.RequestID,
	})
}

func millisecondsSTT(seconds float64) int64 { return int64(math.Round(seconds * 1_000)) }

type sttInbound struct {
	Type       string          `json:"type"`
	Text       string          `json:"text"`
	IsFinal    bool            `json:"is_final"`
	RequestID  string          `json:"request_id"`
	Duration   float64         `json:"duration"`
	Language   string          `json:"language"`
	Words      json.RawMessage `json:"words"`
	StatusCode int             `json:"status_code"`
	Message    string          `json:"message"`
}
