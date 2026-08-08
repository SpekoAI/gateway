package elevenlabs

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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

const (
	// AdapterID is the identifier selected for ElevenLabs multi-context TTS.
	AdapterID       = "elevenlabs.tts.v1"
	extensionID     = "elevenlabs.ai/v1"
	officialAPIHost = "api.elevenlabs.io"
)

type Config struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

type Adapter struct {
	id              string
	httpClient      *http.Client
	eventBuffer     int
	maxMessageBytes int64
	endpointPolicy  upstream.WebSocketPolicy
}

func New(config Config) (*Adapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = AdapterID
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = 32
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = 1 << 20
	}
	if config.EventBuffer < 1 || config.MaxMessageBytes < 1 {
		return nil, errors.New("elevenlabs tts buffers must be positive")
	}
	endpointPolicy, err := upstream.NewWebSocketPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &Adapter{id: config.AdapterID, httpClient: config.HTTPClient, eventBuffer: config.EventBuffer, maxMessageBytes: config.MaxMessageBytes, endpointPolicy: endpointPolicy}, nil
}

func (a *Adapter) ID() string { return a.id }

func (a *Adapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindTTS {
		return nil, fmt.Errorf("elevenlabs supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "elevenlabs" || request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, errors.New("elevenlabs tts requires an ElevenLabs websocket route")
	}
	if request.Media == nil {
		return nil, errors.New("elevenlabs tts requires media configuration")
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("elevenlabs tts requires a bearer credential")
	}
	endpoint, err := multiContextEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint, request.Plan.Route.Model, request.Options, *request.Media, request.Plan.Execution.ProviderRoute, request.Plan.Execution.CredentialSource, credential.Value)
	if err != nil {
		return nil, err
	}
	headers := make(http.Header)
	if request.Plan.Execution.CredentialSource == protocol.CredentialsBYOK {
		headers.Set("xi-api-key", credential.Value)
	}
	// A relay plan is managed for billing purposes but carries the connector's
	// permanent ElevenLabs key, which belongs in the xi-api-key header exactly
	// like a BYOK key. The single_use_token query channel stays reserved for the
	// control-plane-minted tokens of managed provider-direct routes.
	if request.Plan.Execution.ProviderRoute == protocol.RouteSpekoRelay {
		headers.Set("xi-api-key", credential.Value)
	}
	client := a.httpClient
	if client == nil {
		client = http.DefaultClient
	}
	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: client, HTTPHeader: headers})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		return nil, &runtimepkg.ProviderError{Code: dialErrorCode(status), Message: "ElevenLabs TTS connection could not be established", Retryable: status == 0 || status == http.StatusTooManyRequests || status >= 500, ProviderStatus: status, Cause: err}
	}
	conn.SetReadLimit(a.maxMessageBytes)
	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &stream{conn: conn, ctx: streamCtx, cancel: cancel, events: make(chan runtimepkg.ProviderEvent, a.eventBuffer)}
	go stream.readLoop()
	return stream, nil
}

func multiContextEndpoint(policy upstream.WebSocketPolicy, rawEndpoint, model string, options protocol.RequestOptions, media protocol.MediaFormat, route protocol.ProviderRoute, source protocol.CredentialSource, credential string) (string, error) {
	endpoint, err := policy.Parse(rawEndpoint)
	if err != nil {
		return "", fmt.Errorf("elevenlabs endpoint: %w", err)
	}
	voiceID := strings.TrimSpace(options.Voice)
	if voiceID == "" {
		return "", errors.New("elevenlabs requires a voice id")
	}
	if strings.TrimSpace(model) == "" || model == "auto" {
		return "", errors.New("elevenlabs requires a concrete model")
	}
	if model == "eleven_v3" {
		return "", errors.New("elevenlabs multi-context websocket does not support eleven_v3")
	}
	if media.Encoding != "pcm_s16le" || media.Channels != 1 {
		return "", errors.New("elevenlabs streaming output requires mono pcm_s16le")
	}
	switch media.SampleRateHz {
	case 8_000, 16_000, 22_050, 24_000, 44_100, 48_000:
	default:
		return "", fmt.Errorf("elevenlabs does not support pcm output at %d Hz", media.SampleRateHz)
	}
	basePath := strings.TrimRight(endpoint.Path, "/")
	wantSuffix := "/" + url.PathEscape(voiceID) + "/multi-stream-input"
	if basePath == "/v1/text-to-speech" {
		endpoint.Path = basePath + wantSuffix
	} else if !strings.HasSuffix(basePath, wantSuffix) {
		return "", fmt.Errorf("elevenlabs endpoint path must be /v1/text-to-speech or end in %s", wantSuffix)
	}
	query := endpoint.Query()
	query.Set("model_id", model)
	query.Set("output_format", "pcm_"+strconv.Itoa(media.SampleRateHz))
	query.Set("sync_alignment", "true")
	if language := strings.TrimSpace(options.Language); language != "" {
		query.Set("language_code", language)
	}
	// A relay plan is managed but carries a permanent key that dials via the
	// xi-api-key header; only managed provider-direct tokens ride the query
	// string, keeping the permanent key out of URLs where it could reach logs.
	if source == protocol.CredentialsManaged && route != protocol.RouteSpekoRelay {
		query.Set("single_use_token", credential)
	}
	endpoint.RawQuery = query.Encode()
	return endpoint.String(), nil
}

type stream struct {
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent

	writeMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	closing      atomic.Bool
	closeErr     error

	stateMu      sync.Mutex
	contextID    string
	committed    bool
	audioStarted bool
}

func (s *stream) Events() <-chan runtimepkg.ProviderEvent  { return s.events }
func (s *stream) WriteAudio(context.Context, []byte) error { return runtimepkg.ErrUnsupportedOperation }
func (s *stream) CommitAudio(context.Context) error        { return runtimepkg.ErrUnsupportedOperation }

func (s *stream) AppendText(ctx context.Context, text string) error {
	if text == "" {
		return errors.New("elevenlabs text is empty")
	}
	contextID, err := s.startOrCurrentContext()
	if err != nil {
		return err
	}
	return s.writeJSON(ctx, map[string]any{"context_id": contextID, "text": text})
}

func (s *stream) CommitText(ctx context.Context) error {
	contextID, err := s.commitContext()
	if err != nil {
		return err
	}
	if err := s.writeJSON(ctx, map[string]any{"context_id": contextID, "text": "", "flush": true}); err != nil {
		s.uncommitContext(contextID)
		return err
	}
	if err := s.writeJSON(ctx, map[string]any{"context_id": contextID, "close_context": true}); err != nil {
		return err
	}
	return nil
}

func (s *stream) Cancel(ctx context.Context) error {
	contextID, ok := s.activeContext()
	if !ok {
		return runtimepkg.ErrSessionClosed
	}
	if err := s.writeJSON(ctx, map[string]any{"context_id": contextID, "close_context": true}); err != nil {
		return err
	}
	s.finishContext(contextID)
	return nil
}

func (s *stream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		s.closing.Store(true)
		if err := s.writeJSON(ctx, map[string]any{"close_socket": true}); err != nil {
			s.closeErr = err
			_ = s.abort()
			return
		}
		s.closed.Store(true)
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
		if s.committed {
			return "", errors.New("elevenlabs previous utterance has not completed")
		}
		return s.contextID, nil
	}
	id, err := newContextID()
	if err != nil {
		return "", err
	}
	s.contextID, s.committed, s.audioStarted = id, false, false
	return id, nil
}

func (s *stream) commitContext() (string, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() || s.closing.Load() || s.contextID == "" || s.committed {
		return "", runtimepkg.ErrSessionClosed
	}
	s.committed = true
	return s.contextID, nil
}

func (s *stream) uncommitContext(contextID string) {
	s.stateMu.Lock()
	if s.contextID == contextID {
		s.committed = false
	}
	s.stateMu.Unlock()
}

func (s *stream) activeContext() (string, bool) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.contextID, s.contextID != ""
}

func (s *stream) finishContext(contextID string) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.contextID == "" || (contextID != "" && s.contextID != contextID) {
		return
	}
	s.contextID, s.committed, s.audioStarted = "", false, false
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
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if err := s.conn.Write(ctx, websocket.MessageText, payload); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "ElevenLabs TTS write failed", Retryable: true, Cause: err}
	}
	return nil
}

func (s *stream) readLoop() {
	defer func() { s.cancel(); s.finishContext(""); close(s.events) }()
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && s.ctx.Err() == nil && !isNormalClose(err) {
				_ = s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "ElevenLabs TTS read failed", Retryable: true, Cause: err}})
			}
			return
		}
		if messageType != websocket.MessageText {
			continue
		}
		if err := s.handleMessage(payload); err != nil {
			_ = s.emit(runtimepkg.ProviderEvent{Err: err})
			return
		}
	}
}

type alignment struct {
	Characters      []string `json:"chars"`
	StartTimesMS    []int    `json:"charStartTimesMs"`
	DurationsMS     []int    `json:"charDurationsMs"`
	StartTimesSnake []int    `json:"char_start_times_ms"`
	DurationsSnake  []int    `json:"char_durations_ms"`
}

type inbound struct {
	Audio               string          `json:"audio"`
	ContextID           string          `json:"context_id"`
	ContextIDCamel      string          `json:"contextId"`
	IsFinal             bool            `json:"is_final"`
	IsFinalCamel        bool            `json:"isFinal"`
	Alignment           *alignment      `json:"alignment"`
	NormalizedAlignment *alignment      `json:"normalizedAlignment"`
	Error               json.RawMessage `json:"error"`
}

func (s *stream) handleMessage(payload []byte) error {
	var message inbound
	if err := json.Unmarshal(payload, &message); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "ElevenLabs sent malformed streaming JSON", Retryable: true, Cause: err}
	}
	contextID := message.ContextID
	if contextID == "" {
		contextID = message.ContextIDCamel
	}
	raw := json.RawMessage(append([]byte(nil), payload...))
	if len(message.Error) > 0 && string(message.Error) != "null" {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "ElevenLabs reported a streaming error", Retryable: false}
	}
	if message.Audio != "" {
		audio, err := base64.StdEncoding.DecodeString(message.Audio)
		if err != nil {
			return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "ElevenLabs sent invalid base64 audio", Retryable: true, Cause: err}
		}
		if s.markAudioStarted(contextID) {
			if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: contextData(contextID), Extensions: extension(raw)}); err != nil {
				return err
			}
		}
		if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Data: contextData(contextID), Extensions: extension(raw), Audio: audio}); err != nil {
			return err
		}
	}
	if message.Alignment != nil || message.NormalizedAlignment != nil {
		value, normalized := message.Alignment, false
		if value == nil {
			value, normalized = message.NormalizedAlignment, true
		}
		if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAlignment, Data: alignmentData(contextID, value, normalized), Extensions: extension(raw)}); err != nil {
			return err
		}
	}
	if message.IsFinal || message.IsFinalCamel {
		s.finishContext(contextID)
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: contextData(contextID), Extensions: extension(raw)})
	}
	if message.Audio == "" && message.Alignment == nil && message.NormalizedAlignment == nil {
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: marshalData(map[string]any{"message": "ignored ElevenLabs message"}), Extensions: extension(raw)})
	}
	return nil
}

func (s *stream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func alignmentData(contextID string, value *alignment, normalized bool) json.RawMessage {
	if value == nil {
		return marshalData(map[string]any{"context_id": contextID})
	}
	starts := value.StartTimesMS
	if len(starts) == 0 {
		starts = value.StartTimesSnake
	}
	durations := value.DurationsMS
	if len(durations) == 0 {
		durations = value.DurationsSnake
	}
	return marshalData(map[string]any{
		"context_id": contextID, "characters": value.Characters,
		"character_start_times_ms": starts, "character_durations_ms": durations,
		"normalized": normalized,
	})
}

func contextData(contextID string) json.RawMessage {
	return marshalData(map[string]any{"context_id": contextID})
}

func extension(raw json.RawMessage) map[string]json.RawMessage {
	return map[string]json.RawMessage{extensionID: raw}
}

func marshalData(value any) json.RawMessage {
	payload, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{"code":"internal"}`)
	}
	return payload
}

func newContextID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate ElevenLabs context id: %w", err)
	}
	return hex.EncodeToString(value), nil
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
