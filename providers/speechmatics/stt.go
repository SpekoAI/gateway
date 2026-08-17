package speechmatics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

const (
	AdapterID     = "speechmatics.stt.v1"
	DefaultModel  = "standard"
	extensionID   = "speechmatics.com/realtime/v2"
	globalAPIHost = "global.rt.speechmatics.com"
	euAPIHost     = "eu.rt.speechmatics.com"
	usAPIHost     = "us.rt.speechmatics.com"
	endpointPath  = "/v2/"
)

var supportedModels = map[string]struct{}{
	"standard": {},
	"enhanced": {},
}

var supportedEncodings = map[string]struct{}{
	"pcm_s16le": {},
}

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
	if config.EventBuffer < 1 {
		return nil, errors.New("speechmatics event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("speechmatics maximum message bytes must be positive")
	}
	allowed := append([]string{euAPIHost, usAPIHost}, config.AllowedEndpointHosts...)
	policy, err := upstream.NewWebSocketPolicy(globalAPIHost, allowed, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &Adapter{
		id:              config.AdapterID,
		httpClient:      config.HTTPClient,
		eventBuffer:     config.EventBuffer,
		maxMessageBytes: config.MaxMessageBytes,
		endpointPolicy:  policy,
	}, nil
}

func (a *Adapter) ID() string { return a.id }

func (a *Adapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindSTT {
		return nil, fmt.Errorf("speechmatics supports stt sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "speechmatics" {
		return nil, fmt.Errorf("speechmatics adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("speechmatics requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("speechmatics requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("speechmatics media: %w", err)
	}
	if err := validateMedia(*request.Media); err != nil {
		return nil, err
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model == "" || model == "auto" {
		return nil, errors.New("speechmatics requires a concrete model in the session plan")
	}
	if _, ok := supportedModels[model]; !ok {
		return nil, fmt.Errorf("speechmatics does not support realtime model %q", model)
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("speechmatics requires a bearer credential")
	}
	endpoint, err := a.endpointPolicy.Parse(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("speechmatics endpoint: %w", err)
	}
	if endpoint.Path != endpointPath && endpoint.Path != strings.TrimSuffix(endpointPath, "/") {
		return nil, fmt.Errorf("speechmatics endpoint path must be %s, got %q", endpointPath, endpoint.Path)
	}

	headers := make(http.Header)
	headers.Set("Authorization", "Bearer "+credential.Value)
	conn, response, err := websocket.Dial(ctx, endpoint.String(), &websocket.DialOptions{
		HTTPClient: httpClientOrDefault(a.httpClient),
		HTTPHeader: headers,
	})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		return nil, &runtimepkg.ProviderError{
			Code:           statusErrorCode(status),
			Message:        "Speechmatics realtime connection could not be established",
			Retryable:      retryableStatus(status),
			ProviderStatus: status,
			Cause:          err,
		}
	}
	conn.SetReadLimit(a.maxMessageBytes)

	start := startRecognition(request.Plan.Route.Model, request.Options, *request.Media)
	if err := writeJSON(ctx, conn, start); err != nil {
		_ = conn.CloseNow()
		return nil, writeError(err)
	}
	messageType, payload, err := conn.Read(ctx)
	if err != nil {
		_ = conn.CloseNow()
		return nil, readError(err)
	}
	if messageType != websocket.MessageText {
		_ = conn.CloseNow()
		return nil, errors.New("speechmatics sent a non-JSON recognition acknowledgement")
	}
	started, err := recognitionStarted(payload)
	if err != nil {
		_ = conn.CloseNow()
		return nil, err
	}

	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &stream{
		conn:      conn,
		ctx:       streamCtx,
		cancel:    cancel,
		events:    make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		requestID: started.ID,
		language:  speechmaticsLanguage(request.Options.Language),
	}
	go stream.readLoop()
	stream.emit(runtimepkg.ProviderEvent{
		Type:       protocol.EventUsageObserved,
		Data:       marshalData(map[string]any{"provider_request_id": started.ID}),
		Extensions: extension(payload),
	})
	return stream, nil
}

func httpClientOrDefault(client *http.Client) *http.Client {
	if client != nil {
		return client
	}
	return http.DefaultClient
}

func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

func validateMedia(media protocol.MediaFormat) error {
	if _, ok := supportedEncodings[media.Encoding]; !ok {
		return fmt.Errorf("speechmatics does not support media encoding %q", media.Encoding)
	}
	if media.Channels != 1 {
		return fmt.Errorf("speechmatics raw audio requires mono input, got %d channels", media.Channels)
	}
	return nil
}

func speechmaticsLanguage(raw string) string {
	language := strings.ToLower(strings.TrimSpace(raw))
	if language == "" {
		return "en"
	}
	if index := strings.IndexAny(language, "-_"); index > 0 {
		language = language[:index]
	}
	if language == "zh" {
		return "cmn"
	}
	return language
}

func outputLocale(raw string) string {
	locale := strings.TrimSpace(raw)
	if len(locale) < 4 {
		return ""
	}
	parts := strings.FieldsFunc(locale, func(r rune) bool { return r == '-' || r == '_' })
	if len(parts) != 2 {
		return ""
	}
	base := strings.ToLower(parts[0])
	variant := strings.ToUpper(parts[1])
	if base == "en" && (variant == "AU" || variant == "GB" || variant == "US") {
		return "en-" + variant
	}
	if (base == "cmn" || base == "zh") && (variant == "HANS" || variant == "HANT") {
		return "cmn-" + strings.ToUpper(variant[:1]) + strings.ToLower(variant[1:])
	}
	return ""
}

type audioFormat struct {
	Type       string `json:"type"`
	Encoding   string `json:"encoding"`
	SampleRate int    `json:"sample_rate"`
}

type transcriptionConfig struct {
	Language        string   `json:"language"`
	OutputLocale    string   `json:"output_locale,omitempty"`
	Model           string   `json:"model"`
	EnablePartials  bool     `json:"enable_partials"`
	MaxDelay        float64  `json:"max_delay"`
	MaxDelayMode    string   `json:"max_delay_mode"`
	Diarization     string   `json:"diarization,omitempty"`
	AdditionalVocab []string `json:"additional_vocab,omitempty"`
}

type startRecognitionFrame struct {
	Message             string              `json:"message"`
	AudioFormat         audioFormat         `json:"audio_format"`
	TranscriptionConfig transcriptionConfig `json:"transcription_config"`
}

func startRecognition(model string, options protocol.RequestOptions, media protocol.MediaFormat) startRecognitionFrame {
	config := transcriptionConfig{
		Language:       speechmaticsLanguage(options.Language),
		OutputLocale:   outputLocale(options.Language),
		Model:          model,
		EnablePartials: true,
		MaxDelay:       0.7,
		MaxDelayMode:   "fixed",
	}
	if options.STT.Diarize() {
		config.Diarization = "speaker"
	}
	config.AdditionalVocab = options.STT.GetKeywords()
	return startRecognitionFrame{
		Message: "StartRecognition",
		AudioFormat: audioFormat{
			Type:       "raw",
			Encoding:   media.Encoding,
			SampleRate: media.SampleRateHz,
		},
		TranscriptionConfig: config,
	}
}

type startedMessage struct {
	Message string `json:"message"`
	ID      string `json:"id"`
}

func recognitionStarted(payload []byte) (startedMessage, error) {
	var message inboundMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return startedMessage{}, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Speechmatics sent malformed recognition JSON", Retryable: true, Cause: err}
	}
	if message.Message == "Error" {
		return startedMessage{}, providerError(message, payload)
	}
	if message.Message != "RecognitionStarted" || strings.TrimSpace(message.ID) == "" {
		return startedMessage{}, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Speechmatics did not acknowledge StartRecognition", Retryable: true, Extensions: extension(payload)}
	}
	return startedMessage{Message: message.Message, ID: message.ID}, nil
}

type stream struct {
	conn      *websocket.Conn
	ctx       context.Context
	cancel    context.CancelFunc
	events    chan runtimepkg.ProviderEvent
	requestID string
	language  string

	writeMu     sync.Mutex
	closeOnce   sync.Once
	abortOnce   sync.Once
	closeErr    error
	chunks      atomic.Uint64
	inputClosed atomic.Bool
	closed      atomic.Bool
}

func (s *stream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

func (s *stream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("speechmatics audio is empty")
	}
	if s.inputClosed.Load() || s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.inputClosed.Load() || s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if err := s.conn.Write(ctx, websocket.MessageBinary, audio); err != nil {
		return writeError(err)
	}
	s.chunks.Add(1)
	return nil
}

func (s *stream) CommitAudio(ctx context.Context) error {
	if s.inputClosed.Load() || s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	return s.writeJSON(ctx, map[string]any{"message": "ForceEndOfUtterance"})
}

func (s *stream) AppendText(context.Context, string) error { return runtimepkg.ErrUnsupportedOperation }

func (s *stream) CommitText(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

func (s *stream) Close(ctx context.Context) error {
	s.closeOnce.Do(func() {
		s.inputClosed.Store(true)
		s.closeErr = s.writeJSON(ctx, map[string]any{
			"message":     "EndOfStream",
			"last_seq_no": s.chunks.Load(),
		})
		if s.closeErr != nil {
			_ = s.abort()
		}
	})
	return s.closeErr
}

func (s *stream) Cancel(context.Context) error { return s.abort() }

func (s *stream) Abort(context.Context) error { return s.abort() }

func (s *stream) abort() error {
	s.abortOnce.Do(func() {
		s.closed.Store(true)
		s.inputClosed.Store(true)
		s.cancel()
		if err := s.conn.CloseNow(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
	})
	return s.closeErr
}

func (s *stream) writeJSON(ctx context.Context, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if err := s.conn.Write(ctx, websocket.MessageText, payload); err != nil {
		return writeError(err)
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
		s.closed.Store(true)
		s.cancel()
		_ = s.conn.CloseNow()
		close(s.events)
	}()
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && s.ctx.Err() == nil && !isNormalClose(err) {
				s.emit(runtimepkg.ProviderEvent{Err: readError(err)})
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
			return
		}
	}
}

type transcriptMetadata struct {
	StartTime  float64 `json:"start_time"`
	EndTime    float64 `json:"end_time"`
	Transcript string  `json:"transcript"`
}

type alternative struct {
	Content    string  `json:"content"`
	Confidence float64 `json:"confidence"`
	Language   string  `json:"language"`
	Speaker    string  `json:"speaker"`
}

type result struct {
	Type         string        `json:"type"`
	StartTime    float64       `json:"start_time"`
	EndTime      float64       `json:"end_time"`
	Alternatives []alternative `json:"alternatives"`
}

type inboundMessage struct {
	Message  string             `json:"message"`
	ID       string             `json:"id"`
	Type     string             `json:"type"`
	Reason   string             `json:"reason"`
	Code     int                `json:"code"`
	SeqNo    int                `json:"seq_no"`
	Metadata transcriptMetadata `json:"metadata"`
	Results  []result           `json:"results"`
}

func (s *stream) handleMessage(payload []byte) (bool, error) {
	var message inboundMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return false, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Speechmatics sent malformed streaming JSON", Retryable: true, Cause: err}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))
	switch message.Message {
	case "AudioAdded":
		return false, nil
	case "AddPartialTranscript":
		return false, s.emitTranscript(protocol.EventTranscriptDelta, message, raw)
	case "AddTranscript":
		return false, s.emitTranscript(protocol.EventTranscriptFinal, message, raw)
	case "EndOfUtterance":
		return false, s.emit(runtimepkg.ProviderEvent{
			Type: protocol.EventSpeechEnded,
			Data: marshalData(map[string]any{
				"audio_end_ms":        milliseconds(message.Metadata.EndTime),
				"provider_request_id": s.requestID,
			}),
			Extensions: extension(raw),
		})
	case "EndOfTranscript":
		return true, nil
	case "Info", "Warning":
		return false, s.emit(runtimepkg.ProviderEvent{
			Type: protocol.EventWarning,
			Data: marshalData(map[string]any{
				"message":             message.Reason,
				"provider_type":       message.Type,
				"provider_request_id": s.requestID,
			}),
			Extensions: extension(raw),
		})
	case "Error":
		return false, providerError(message, raw)
	default:
		return false, s.emit(runtimepkg.ProviderEvent{
			Type: protocol.EventWarning,
			Data: marshalData(map[string]any{
				"message":             "ignored Speechmatics message type",
				"provider_type":       message.Message,
				"provider_request_id": s.requestID,
			}),
			Extensions: extension(raw),
		})
	}
}

func (s *stream) emitTranscript(kind protocol.EventType, message inboundMessage, raw json.RawMessage) error {
	if strings.TrimSpace(message.Metadata.Transcript) == "" {
		return nil
	}
	final := kind == protocol.EventTranscriptFinal
	data := map[string]any{
		"text":                message.Metadata.Transcript,
		"is_final":            final,
		"speech_final":        final,
		"audio_start_ms":      milliseconds(message.Metadata.StartTime),
		"audio_end_ms":        milliseconds(message.Metadata.EndTime),
		"language":            s.language,
		"provider_request_id": s.requestID,
		"words":               message.Results,
	}
	if confidence, ok := transcriptConfidence(message.Results); ok {
		data["confidence"] = confidence
	}
	return s.emit(runtimepkg.ProviderEvent{Type: kind, Data: marshalData(data), Extensions: extension(raw)})
}

func transcriptConfidence(results []result) (float64, bool) {
	var total float64
	var count int
	for _, item := range results {
		if item.Type != "word" || len(item.Alternatives) == 0 {
			continue
		}
		total += item.Alternatives[0].Confidence
		count++
	}
	if count == 0 {
		return 0, false
	}
	return total / float64(count), true
}

func (s *stream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func providerError(message inboundMessage, raw json.RawMessage) *runtimepkg.ProviderError {
	code, retryable := classifyError(message.Type)
	detail := strings.TrimSpace(message.Reason)
	if detail == "" {
		detail = "Speechmatics reported a realtime transcription error"
	}
	return &runtimepkg.ProviderError{
		Code:           code,
		Message:        detail,
		Retryable:      retryable,
		ProviderStatus: message.Code,
		Extensions:     extension(raw),
	}
}

func classifyError(kind string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "not_authorised":
		return "authentication_failed", false
	case "quota_exceeded":
		return "provider_quota_exceeded", true
	case "invalid_message", "invalid_model", "invalid_language", "invalid_config", "invalid_audio_type", "invalid_audio", "data_error", "buffer_error":
		return "invalid_request", false
	case "job_error", "unknown_error", "internal_error", "timelimit_exceeded":
		return "provider_unavailable", true
	default:
		return "provider_unavailable", false
	}
}

func statusErrorCode(status int) string {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return "authentication_failed"
	case http.StatusPaymentRequired:
		return "provider_quota_exceeded"
	case http.StatusTooManyRequests:
		return "provider_rate_limited"
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return "invalid_request"
	default:
		return "provider_unavailable"
	}
}

func retryableStatus(status int) bool {
	return status == 0 || status == http.StatusTooManyRequests || status >= 500
}

func writeError(err error) *runtimepkg.ProviderError {
	return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Speechmatics streaming write failed", Retryable: true, Cause: err}
}

func readError(err error) *runtimepkg.ProviderError {
	return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Speechmatics streaming read failed", Retryable: true, Cause: err}
}

func isNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}

func extension(raw []byte) map[string]json.RawMessage {
	return map[string]json.RawMessage{extensionID: json.RawMessage(append([]byte(nil), raw...))}
}

func marshalData(value any) json.RawMessage {
	payload, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{"code":"internal"}`)
	}
	return payload
}

func milliseconds(seconds float64) int64 { return int64(math.Round(seconds * 1_000)) }
