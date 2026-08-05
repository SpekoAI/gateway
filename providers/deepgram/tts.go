package deepgram

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

const (
	// TTSAdapterID is the stable Aura-2 streaming TTS adapter identifier.
	TTSAdapterID = "deepgram.tts.v1"

	deepgramTTSMaxCharactersPerMessage = 2_000
	deepgramTTSCharactersPerMinute     = 2_400
	deepgramTTSFlushesPerMinute        = 20
)

// TTSConfig controls bounded transport state and allows deterministic rate
// limit tests. Provider credentials and generation choices stay plan-bound.
type TTSConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	Now                   func() time.Time
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// TTSAdapter implements Deepgram Aura's stable /v1/speak WebSocket protocol.
type TTSAdapter struct {
	id              string
	httpClient      *http.Client
	eventBuffer     int
	maxMessageBytes int64
	now             func() time.Time
	endpointPolicy  upstream.WebSocketPolicy
}

func NewTTS(config TTSConfig) (*TTSAdapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = TTSAdapterID
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = 32
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = 1 << 20
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.EventBuffer < 1 || config.MaxMessageBytes < 1 {
		return nil, errors.New("deepgram tts buffers must be positive")
	}
	endpointPolicy, err := upstream.NewWebSocketPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &TTSAdapter{id: config.AdapterID, httpClient: config.HTTPClient, eventBuffer: config.EventBuffer, maxMessageBytes: config.MaxMessageBytes, now: config.Now, endpointPolicy: endpointPolicy}, nil
}

func (a *TTSAdapter) ID() string { return a.id }

func (a *TTSAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindTTS {
		return nil, fmt.Errorf("deepgram tts supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "deepgram" || request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, errors.New("deepgram tts requires a Deepgram websocket route")
	}
	if request.Media == nil {
		return nil, errors.New("deepgram tts requires media configuration")
	}
	endpoint, err := speakEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint, request.Plan.Route.Model, *request.Media)
	if err != nil {
		return nil, err
	}
	credential := request.Plan.Route.Credential
	if credential == nil || credential.Kind != protocol.CredentialBearer || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("deepgram tts requires a bearer credential")
	}
	headers := make(http.Header)
	authorizationScheme := "Bearer"
	if request.Plan.Execution.CredentialSource == protocol.CredentialsBYOK {
		authorizationScheme = "Token"
	}
	headers.Set("Authorization", authorizationScheme+" "+credential.Value)
	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: configHTTPClient(a.httpClient), HTTPHeader: headers})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		return nil, &runtimepkg.ProviderError{Code: dialErrorCode(status), Message: "Deepgram TTS connection could not be established", Retryable: status == 0 || status == http.StatusTooManyRequests || status >= 500, ProviderStatus: status, Cause: err}
	}
	conn.SetReadLimit(a.maxMessageBytes)
	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &ttsStream{conn: conn, ctx: streamCtx, cancel: cancel, events: make(chan runtimepkg.ProviderEvent, a.eventBuffer), now: a.now}
	if response != nil {
		stream.setRequestID(response.Header.Get("dg-request-id"))
	}
	if requestID := stream.currentRequestID(); requestID != "" {
		if err := stream.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: usageData(requestID)}); err != nil {
			_ = stream.abort()
			return nil, err
		}
	}
	go stream.readLoop()
	return stream, nil
}

func speakEndpoint(policy upstream.WebSocketPolicy, rawEndpoint, model string, media protocol.MediaFormat) (string, error) {
	endpoint, err := policy.Parse(rawEndpoint)
	if err != nil {
		return "", fmt.Errorf("deepgram tts endpoint: %w", err)
	}
	if endpoint.Path != "/v1/speak" {
		return "", fmt.Errorf("deepgram tts endpoint path must be /v1/speak, got %q", endpoint.Path)
	}
	if strings.TrimSpace(model) == "" || model == "auto" || !strings.HasPrefix(model, "aura-2-") {
		return "", errors.New("deepgram tts requires a concrete Aura-2 model")
	}
	if media.Encoding != "pcm_s16le" || media.Channels != 1 {
		return "", errors.New("deepgram tts requires mono pcm_s16le output")
	}
	switch media.SampleRateHz {
	case 8_000, 16_000, 24_000, 32_000, 48_000:
	default:
		return "", fmt.Errorf("deepgram tts does not support sample rate %d", media.SampleRateHz)
	}
	query := endpoint.Query()
	query.Set("model", model)
	query.Set("encoding", "linear16")
	query.Set("sample_rate", strconv.Itoa(media.SampleRateHz))
	endpoint.RawQuery = query.Encode()
	return endpoint.String(), nil
}

type ttsStream struct {
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent
	now    func() time.Time

	writeMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	closeErr     error

	stateMu       sync.Mutex
	utteranceID   string
	committed     bool
	audioStarted  bool
	requestID     string
	windowStarted time.Time
	windowChars   int
	windowFlushes int
}

func (s *ttsStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }
func (s *ttsStream) WriteAudio(context.Context, []byte) error {
	return runtimepkg.ErrUnsupportedOperation
}
func (s *ttsStream) CommitAudio(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

func (s *ttsStream) AppendText(ctx context.Context, text string) error {
	characters := utf8.RuneCountInString(text)
	if characters == 0 {
		return errors.New("deepgram tts text is empty")
	}
	if characters > deepgramTTSMaxCharactersPerMessage {
		return &runtimepkg.ProviderError{Code: "input_too_large", Message: "Deepgram TTS input exceeds 2000 characters", Retryable: false, ProviderStatus: http.StatusRequestEntityTooLarge}
	}
	if err := s.acceptCharacters(characters); err != nil {
		return err
	}
	if _, err := s.startOrCurrentUtterance(); err != nil {
		return err
	}
	return s.writeJSON(ctx, map[string]any{"type": "Speak", "text": text})
}

func (s *ttsStream) CommitText(ctx context.Context) error {
	if err := s.commitUtterance(); err != nil {
		return err
	}
	if err := s.acceptFlush(); err != nil {
		s.uncommitUtterance()
		return err
	}
	if err := s.writeJSON(ctx, map[string]string{"type": "Flush"}); err != nil {
		s.uncommitUtterance()
		return err
	}
	return nil
}

func (s *ttsStream) Cancel(ctx context.Context) error {
	if !s.hasUtterance() {
		return runtimepkg.ErrSessionClosed
	}
	if err := s.writeJSON(ctx, map[string]string{"type": "Clear"}); err != nil {
		return err
	}
	s.finishUtterance()
	return nil
}

func (s *ttsStream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		s.closed.Store(true)
		s.writeMu.Lock()
		s.closeErr = writeControl(ctx, s.conn, map[string]string{"type": "Close"})
		s.writeMu.Unlock()
		if s.closeErr != nil {
			_ = s.abort()
		}
	})
	return s.closeErr
}

func (s *ttsStream) Abort(context.Context) error { return s.abort() }

func (s *ttsStream) abort() error {
	s.abortOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		if err := s.conn.CloseNow(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
		s.finishUtterance()
	})
	return s.closeErr
}

func (s *ttsStream) acceptCharacters(characters int) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	now := s.now().UTC()
	if s.windowStarted.IsZero() || now.Sub(s.windowStarted) >= time.Minute {
		s.windowStarted, s.windowChars, s.windowFlushes = now, 0, 0
	}
	if characters > deepgramTTSCharactersPerMinute-s.windowChars {
		return &runtimepkg.ProviderError{Code: "provider_rate_limited", Message: "Deepgram TTS character throughput limit exceeded", Retryable: true, ProviderStatus: http.StatusTooManyRequests}
	}
	s.windowChars += characters
	return nil
}

func (s *ttsStream) acceptFlush() error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	now := s.now().UTC()
	if s.windowStarted.IsZero() || now.Sub(s.windowStarted) >= time.Minute {
		s.windowStarted, s.windowChars, s.windowFlushes = now, 0, 0
	}
	if s.windowFlushes >= deepgramTTSFlushesPerMinute {
		return &runtimepkg.ProviderError{Code: "provider_rate_limited", Message: "Deepgram TTS flush limit exceeded", Retryable: true, ProviderStatus: http.StatusTooManyRequests}
	}
	s.windowFlushes++
	return nil
}

func (s *ttsStream) startOrCurrentUtterance() (string, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() {
		return "", runtimepkg.ErrSessionClosed
	}
	if s.utteranceID != "" {
		if s.committed {
			return "", errors.New("deepgram tts previous utterance has not completed")
		}
		return s.utteranceID, nil
	}
	id, err := newTTSUtteranceID()
	if err != nil {
		return "", err
	}
	s.utteranceID, s.committed, s.audioStarted = id, false, false
	return id, nil
}

func (s *ttsStream) commitUtterance() error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() || s.utteranceID == "" || s.committed {
		return runtimepkg.ErrSessionClosed
	}
	s.committed = true
	return nil
}

func (s *ttsStream) uncommitUtterance() {
	s.stateMu.Lock()
	if s.utteranceID != "" {
		s.committed = false
	}
	s.stateMu.Unlock()
}

func (s *ttsStream) hasUtterance() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.utteranceID != ""
}

func (s *ttsStream) finishUtterance() string {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	id := s.utteranceID
	s.utteranceID, s.committed, s.audioStarted = "", false, false
	return id
}

func (s *ttsStream) currentUtterance(markStarted bool) (string, bool) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	started := false
	if markStarted && s.utteranceID != "" && !s.audioStarted {
		s.audioStarted = true
		started = true
	}
	return s.utteranceID, started
}

func (s *ttsStream) writeJSON(ctx context.Context, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if err := s.conn.Write(ctx, websocket.MessageText, payload); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Deepgram TTS write failed", Retryable: true, Cause: err}
	}
	return nil
}

func (s *ttsStream) readLoop() {
	defer func() { s.cancel(); close(s.events) }()
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && s.ctx.Err() == nil && !isNormalClose(err) {
				_ = s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Deepgram TTS read failed", Retryable: true, Cause: err}})
			}
			return
		}
		if messageType == websocket.MessageBinary {
			if err := s.handleAudio(payload); err != nil {
				_ = s.emit(runtimepkg.ProviderEvent{Err: err})
				return
			}
			continue
		}
		if messageType == websocket.MessageText {
			if err := s.handleTTSMessage(payload); err != nil {
				_ = s.emit(runtimepkg.ProviderEvent{Err: err})
				return
			}
		}
	}
}

func (s *ttsStream) handleAudio(payload []byte) error {
	id, started := s.currentUtterance(true)
	if started {
		if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: ttsContextData(id, s.currentRequestID())}); err != nil {
			return err
		}
	}
	audio := append([]byte(nil), payload...)
	return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Data: ttsContextData(id, s.currentRequestID()), Audio: audio})
}

func (s *ttsStream) handleTTSMessage(payload []byte) error {
	var message struct {
		Type        string `json:"type"`
		RequestID   string `json:"request_id"`
		SequenceID  int    `json:"sequence_id"`
		Description string `json:"description"`
		Code        string `json:"code"`
	}
	if err := json.Unmarshal(payload, &message); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Deepgram TTS sent malformed JSON", Retryable: true, Cause: err}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))
	switch message.Type {
	case "Metadata":
		if s.setRequestID(message.RequestID) {
			return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: usageData(message.RequestID), Extensions: extension(raw)})
		}
		return nil
	case "Flushed":
		id := s.finishUtterance()
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: ttsContextData(id, s.currentRequestID()), Extensions: extension(raw)})
	case "Cleared":
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: marshalData(map[string]any{"code": "provider_buffer_cleared", "sequence_id": message.SequenceID}), Extensions: extension(raw)})
	case "Warning":
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: marshalData(map[string]any{"code": message.Code, "message": message.Description}), Extensions: extension(raw)})
	case "Error":
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: deepgramTTSErrorMessage(message.Description), Retryable: false}
	default:
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: warningData(message.Type), Extensions: extension(raw)})
	}
}

func (s *ttsStream) setRequestID(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.requestID == value {
		return false
	}
	s.requestID = value
	return true
}

func (s *ttsStream) currentRequestID() string {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.requestID
}

func (s *ttsStream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func ttsContextData(utteranceID, requestID string) json.RawMessage {
	return marshalData(map[string]any{"utterance_id": utteranceID, "provider_request_id": requestID})
}

func deepgramTTSErrorMessage(description string) string {
	if strings.TrimSpace(description) == "" {
		return "Deepgram reported a TTS streaming error"
	}
	return "Deepgram reported a TTS streaming error: " + description
}

func newTTSUtteranceID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate Deepgram TTS utterance id: %w", err)
	}
	return hex.EncodeToString(value), nil
}
