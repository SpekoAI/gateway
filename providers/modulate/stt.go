// Package modulate implements Modulate Velma-2 streaming speech-to-text.
package modulate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
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
	// AdapterID is the stable identifier carried by Modulate STT session plans.
	AdapterID = "modulate.stt.v1"

	// EnglishFastModel is Modulate's lowest-latency English streaming model.
	EnglishFastModel = "velma-2-stt-streaming-english-v2"
	// MultilingualModel adds automatic language detection, diarization, and
	// optional acoustic enrichments. Speko enables partials and diarization but
	// leaves the separately billed enrichment signals off.
	MultilingualModel = "velma-2-stt-streaming"

	officialAPIHost        = "platform.modulate.ai"
	englishFastPath        = "/api/velma-2-stt-streaming-english-v2"
	multilingualPath       = "/api/velma-2-stt-streaming"
	extensionID            = "modulate.ai/velma-2"
	defaultEventBuffer     = 32
	defaultMaxMessageSize  = 1 << 20
	defaultShutdownTimeout = 30 * time.Second
)

var (
	supportedSampleRates = map[int]struct{}{
		8_000: {}, 11_025: {}, 16_000: {}, 22_050: {}, 32_000: {},
		44_100: {}, 48_000: {}, 96_000: {},
	}
	languageTagPattern = regexp.MustCompile(`^[A-Za-z]{2,3}(?:[-_][A-Za-z0-9]+)*$`)
)

// Config controls bounded transport resources and test-only endpoint policy.
type Config struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	ShutdownTimeout       time.Duration
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// Adapter implements both current Modulate streaming transcription endpoints.
type Adapter struct {
	id              string
	httpClient      *http.Client
	eventBuffer     int
	maxMessageBytes int64
	shutdownTimeout time.Duration
	endpointPolicy  upstream.WebSocketPolicy
}

func New(config Config) (*Adapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = AdapterID
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = defaultEventBuffer
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = defaultMaxMessageSize
	}
	if config.ShutdownTimeout == 0 {
		config.ShutdownTimeout = defaultShutdownTimeout
	}
	if config.EventBuffer < 1 || config.MaxMessageBytes < 1 || config.ShutdownTimeout < 0 {
		return nil, errors.New("modulate stt buffers and shutdown timeout must be positive")
	}
	policy, err := upstream.NewWebSocketPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &Adapter{
		id: config.AdapterID, httpClient: config.HTTPClient,
		eventBuffer: config.EventBuffer, maxMessageBytes: config.MaxMessageBytes,
		shutdownTimeout: config.ShutdownTimeout,
		endpointPolicy:  policy,
	}, nil
}

func (a *Adapter) ID() string { return a.id }

func (a *Adapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindSTT {
		return nil, fmt.Errorf("modulate supports stt sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "modulate" || request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, errors.New("modulate stt requires a Modulate websocket route")
	}
	// The Models API publishes only organization API-key authentication. There
	// is no scoped/session credential minting endpoint. A managed direct plan
	// must therefore fail closed instead of exposing Speko's permanent key to a
	// customer runtime. BYOK and Relay keep that key on its owner's server.
	if request.Plan.Execution.ProviderRoute == protocol.RouteProviderDirect && request.Plan.Execution.CredentialSource == protocol.CredentialsManaged {
		return nil, errors.New("modulate stt does not support delegated provider-direct credentials")
	}
	if request.Media == nil {
		return nil, errors.New("modulate stt requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("modulate stt media: %w", err)
	}
	credential := request.Plan.Route.Credential
	if credential == nil || strings.TrimSpace(credential.Value) == "" || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) {
		return nil, errors.New("modulate stt requires an API-key credential")
	}
	endpoint, err := streamEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint, request.Plan.Route.Model, request.Options, *request.Media, credential.Value)
	if err != nil {
		return nil, err
	}
	client := a.httpClient
	if client == nil {
		client = http.DefaultClient
	}
	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: client})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		// endpoint contains the permanent API key because Modulate requires it
		// in the query string. Deliberately omit the dial error as a Cause so an
		// HTTP/WebSocket library can never copy the URL into logs.
		return nil, &runtimepkg.ProviderError{
			Code: dialErrorCode(status), Message: "Modulate streaming connection could not be established",
			Retryable: status == 0 || status == http.StatusTooManyRequests || status >= 500, ProviderStatus: status,
		}
	}
	conn.SetReadLimit(a.maxMessageBytes)
	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &stream{
		conn: conn, ctx: streamCtx, cancel: cancel,
		events: make(chan runtimepkg.ProviderEvent, a.eventBuffer), done: make(chan struct{}),
		shutdownTimeout: a.shutdownTimeout,
	}
	go stream.readLoop()
	return stream, nil
}

func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

func streamEndpoint(policy upstream.WebSocketPolicy, rawEndpoint, model string, options protocol.RequestOptions, media protocol.MediaFormat, apiKey string) (string, error) {
	endpoint, err := policy.Parse(rawEndpoint)
	if err != nil {
		return "", fmt.Errorf("modulate stt endpoint: %w", err)
	}
	expectedPath := ""
	switch model {
	case EnglishFastModel:
		expectedPath = englishFastPath
	case MultilingualModel:
		expectedPath = multilingualPath
	default:
		return "", fmt.Errorf("modulate stt model %q is not supported", model)
	}
	if endpoint.Path != expectedPath {
		return "", fmt.Errorf("modulate stt model %q requires endpoint path %s", model, expectedPath)
	}
	if media.Encoding != "pcm_s16le" {
		return "", fmt.Errorf("modulate stt requires pcm_s16le, got %q", media.Encoding)
	}
	if _, ok := supportedSampleRates[media.SampleRateHz]; !ok {
		return "", fmt.Errorf("modulate stt does not accept sample rate %d Hz", media.SampleRateHz)
	}
	if media.Channels < 1 || media.Channels > 8 {
		return "", fmt.Errorf("modulate stt requires between 1 and 8 channels, got %d", media.Channels)
	}
	language := strings.TrimSpace(options.Language)
	if language != "" && !languageTagPattern.MatchString(language) {
		return "", fmt.Errorf("modulate stt language %q is not a valid language tag", options.Language)
	}
	if model == EnglishFastModel && language != "" && primaryLanguage(language) != "en" {
		return "", fmt.Errorf("modulate English Fast does not support language %q", options.Language)
	}

	query := endpoint.Query()
	query.Set("api_key", apiKey)
	query.Set("audio_format", "s16le")
	query.Set("sample_rate", strconv.Itoa(media.SampleRateHz))
	query.Set("num_channels", strconv.Itoa(media.Channels))
	if model == EnglishFastModel {
		// The canonical CommitAudio cannot force a boundary on this API, so pin
		// server endpointing for conversational turns.
		query.Set("endpointing", "true")
	} else {
		query.Set("partial_results", "true")
		query.Set("speaker_diarization", "true")
		if language != "" {
			query.Set("language", language)
		}
	}
	endpoint.RawQuery = query.Encode()
	return endpoint.String(), nil
}

func primaryLanguage(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if index := strings.IndexAny(value, "-_"); index >= 0 {
		value = value[:index]
	}
	return value
}

type stream struct {
	conn            *websocket.Conn
	ctx             context.Context
	cancel          context.CancelFunc
	events          chan runtimepkg.ProviderEvent
	done            chan struct{}
	shutdownTimeout time.Duration

	writeMu    sync.Mutex
	doneOnce   sync.Once
	closeOnce  sync.Once
	abortOnce  sync.Once
	eventsOnce sync.Once
	closing    atomic.Bool
	closed     atomic.Bool
	closeErr   error
	speechOpen bool
}

func (s *stream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

func (s *stream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("modulate stt audio is empty")
	}
	return s.write(ctx, websocket.MessageBinary, audio)
}

// Modulate exposes no mid-stream finalize control. Both supported endpoints
// perform server-side utterance segmentation, so an explicit caller boundary
// is advisory and the socket remains warm.
func (s *stream) CommitAudio(context.Context) error {
	if s.closed.Load() || s.closing.Load() {
		return runtimepkg.ErrSessionClosed
	}
	return nil
}

func (s *stream) AppendText(context.Context, string) error { return runtimepkg.ErrUnsupportedOperation }
func (s *stream) CommitText(context.Context) error         { return runtimepkg.ErrUnsupportedOperation }
func (s *stream) Cancel(context.Context) error             { return s.abort() }
func (s *stream) Abort(context.Context) error              { return s.abort() }

func (s *stream) Close(ctx context.Context) error {
	s.closeOnce.Do(func() {
		shutdownCtx, cancel := context.WithTimeout(ctx, s.shutdownTimeout)
		defer cancel()
		s.closing.Store(true)
		select {
		case <-s.done:
			_ = s.abort()
			return
		default:
		}
		// The empty TEXT frame is Modulate's only end-of-stream signal. A zero-
		// length binary frame is audio and does not finish the request.
		if err := s.write(shutdownCtx, websocket.MessageText, []byte{}); err != nil {
			s.closeErr = err
			_ = s.abort()
			return
		}
		select {
		case <-s.done:
		case <-shutdownCtx.Done():
			s.closeErr = shutdownCtx.Err()
		}
		_ = s.abort()
	})
	return s.closeErr
}

func (s *stream) write(ctx context.Context, messageType websocket.MessageType, payload []byte) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	return s.conn.Write(ctx, messageType, payload)
}

func (s *stream) abort() error {
	s.abortOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		if err := s.conn.CloseNow(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
		s.signalDone()
	})
	return s.closeErr
}

func (s *stream) signalDone() { s.doneOnce.Do(func() { close(s.done) }) }

func (s *stream) readLoop() {
	defer func() {
		s.closed.Store(true)
		s.cancel()
		s.signalDone()
		s.eventsOnce.Do(func() { close(s.events) })
	}()
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closing.Load() && !isNormalClose(err) {
				_ = s.emit(runtimepkg.ProviderEvent{Err: readError(err)})
			}
			return
		}
		if messageType != websocket.MessageText {
			_ = s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Modulate returned an unexpected binary frame"}})
			return
		}
		keepReading, err := s.handleMessage(payload)
		if err != nil {
			_ = s.emit(runtimepkg.ProviderEvent{Err: err})
			return
		}
		if !keepReading {
			return
		}
	}
}

func (s *stream) handleMessage(raw json.RawMessage) (bool, error) {
	var message inboundMessage
	if err := json.Unmarshal(raw, &message); err != nil {
		return false, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Modulate returned malformed JSON"}
	}
	switch message.Type {
	case "partial_utterance":
		if message.Partial == nil {
			return false, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Modulate returned an incomplete partial utterance"}
		}
		if strings.TrimSpace(message.Partial.Text) != "" && !s.speechOpen {
			s.speechOpen = true
			if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventSpeechStarted, Data: timingData(*message.Partial), Extensions: extension(raw)}); err != nil {
				return false, err
			}
		}
		return true, s.emit(runtimepkg.ProviderEvent{Type: protocol.EventTranscriptDelta, Data: transcriptData(*message.Partial, false), Extensions: extension(raw)})
	case "utterance":
		if message.Utterance == nil {
			return false, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Modulate returned an incomplete final utterance"}
		}
		if strings.TrimSpace(message.Utterance.Text) != "" && !s.speechOpen {
			s.speechOpen = true
			if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventSpeechStarted, Data: timingData(*message.Utterance), Extensions: extension(raw)}); err != nil {
				return false, err
			}
		}
		if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventTranscriptFinal, Data: transcriptData(*message.Utterance, true), Extensions: extension(raw)}); err != nil {
			return false, err
		}
		if s.speechOpen {
			s.speechOpen = false
			if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventSpeechEnded, Data: timingData(*message.Utterance), Extensions: extension(raw)}); err != nil {
				return false, err
			}
		}
		return true, nil
	case "done":
		return false, s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: marshalData(map[string]any{"duration_ms": message.DurationMS}), Extensions: extension(raw)})
	case "error":
		return false, providerMessageError(message.Error)
	default:
		return true, s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: marshalData(map[string]any{"message": "ignored Modulate message type", "provider_type": message.Type}), Extensions: extension(raw)})
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

type inboundMessage struct {
	Type       string             `json:"type"`
	Partial    *transcriptPayload `json:"partial_utterance"`
	Utterance  *transcriptPayload `json:"utterance"`
	Error      string             `json:"error"`
	DurationMS int64              `json:"duration_ms"`
}

type transcriptPayload struct {
	UtteranceID string          `json:"utterance_uuid"`
	Text        string          `json:"text"`
	StartMS     int64           `json:"start_ms"`
	DurationMS  int64           `json:"duration_ms"`
	Speaker     int             `json:"speaker"`
	Language    string          `json:"language"`
	Emotion     json.RawMessage `json:"emotion"`
	Accent      json.RawMessage `json:"accent"`
	Deepfake    json.RawMessage `json:"deepfake_score"`
}

func transcriptData(payload transcriptPayload, final bool) json.RawMessage {
	data := map[string]any{
		"text": payload.Text, "is_final": final,
		"audio_start_ms": payload.StartMS, "audio_end_ms": payload.StartMS + payload.DurationMS,
	}
	if payload.UtteranceID != "" {
		data["provider_request_id"] = payload.UtteranceID
	}
	if payload.Speaker != 0 {
		data["speaker"] = payload.Speaker
	}
	if payload.Language != "" {
		data["language"] = payload.Language
	}
	if len(payload.Emotion) > 0 && string(payload.Emotion) != "null" {
		data["emotion"] = payload.Emotion
	}
	if len(payload.Accent) > 0 && string(payload.Accent) != "null" {
		data["accent"] = payload.Accent
	}
	if len(payload.Deepfake) > 0 && string(payload.Deepfake) != "null" {
		data["deepfake_score"] = payload.Deepfake
	}
	return marshalData(data)
}

func timingData(payload transcriptPayload) json.RawMessage {
	return marshalData(map[string]any{"audio_start_ms": payload.StartMS, "audio_end_ms": payload.StartMS + payload.DurationMS})
}

func extension(raw json.RawMessage) map[string]json.RawMessage {
	return map[string]json.RawMessage{extensionID: append(json.RawMessage(nil), raw...)}
}

func marshalData(value any) json.RawMessage {
	payload, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{"code":"internal"}`)
	}
	return payload
}

func providerMessageError(message string) *runtimepkg.ProviderError {
	lower := strings.ToLower(message)
	result := &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Modulate reported a transcription error"}
	switch {
	case strings.Contains(lower, "api key") || strings.Contains(lower, "not permitted") || strings.Contains(lower, "access to this model"):
		result.Code = "authentication_failed"
	case strings.Contains(lower, "insufficient credits") || strings.Contains(lower, "monthly usage limit"):
		result.Code = "provider_quota_exceeded"
	case strings.Contains(lower, "concurrent request limit"):
		result.Code, result.Retryable = "provider_rate_limited", true
	case strings.Contains(lower, "temporarily unavailable") || strings.Contains(lower, "internal server") || strings.Contains(lower, "unable to validate"):
		result.Retryable = true
	case strings.Contains(lower, "audio") || strings.Contains(lower, "configuration") || strings.Contains(lower, "parameter"):
		result.Code = "invalid_request"
	}
	return result
}

func readError(err error) *runtimepkg.ProviderError {
	status := int(websocket.CloseStatus(err))
	result := &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Modulate streaming connection ended unexpectedly", ProviderStatus: status}
	switch status {
	case 4001, 4003, 4004:
		result.Code = "authentication_failed"
	case 4002, 1003:
		result.Code = "invalid_request"
	case 4029, 4031:
		result.Code = "provider_quota_exceeded"
	case 4030:
		result.Code, result.Retryable = "provider_rate_limited", true
	case 1011, 1013, -1:
		result.Retryable = true
	}
	return result
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

func isNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}
