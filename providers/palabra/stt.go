package palabra

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

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

const (
	STTAdapterID = "palabra.stt.v1"
	sttPath      = "/asr/v1/speech-to-text/stream"
	STTModel     = "default"
)

var sttLanguages = map[string]struct{}{
	"ar": {}, "de": {}, "en": {}, "es": {}, "fr": {}, "hi": {}, "it": {},
	"ja": {}, "ko": {}, "nl": {}, "pt": {}, "ru": {}, "zh": {},
}

type STTConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

type STTAdapter struct {
	id              string
	httpClient      *http.Client
	eventBuffer     int
	maxMessageBytes int64
	endpointPolicy  endpointPolicy
}

func NewSTT(config STTConfig) (*STTAdapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = STTAdapterID
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = 32
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = 1 << 20
	}
	if config.EventBuffer < 1 {
		return nil, errors.New("palabra stt event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("palabra stt maximum message bytes must be positive")
	}
	policy, err := newEndpointPolicy([]string{"stream.palabra.ai", "api.palabra.ai"}, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &STTAdapter{id: config.AdapterID, httpClient: config.HTTPClient, eventBuffer: config.EventBuffer, maxMessageBytes: config.MaxMessageBytes, endpointPolicy: policy}, nil
}

func (a *STTAdapter) ID() string { return a.id }

func (a *STTAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindSTT {
		return nil, fmt.Errorf("palabra stt supports stt sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "palabra" {
		return nil, fmt.Errorf("palabra stt adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("palabra stt requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model != STTModel && model != "auto" {
		return nil, fmt.Errorf("palabra stt does not expose selectable models; got %q", model)
	}
	if request.Media == nil {
		return nil, errors.New("palabra stt requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("palabra stt media: %w", err)
	}
	if request.Media.Encoding != "pcm_s16le" || request.Media.Channels != 1 {
		return nil, fmt.Errorf("palabra stt requires mono pcm_s16le input, got %s/%d channels", request.Media.Encoding, request.Media.Channels)
	}
	language, err := sttLanguage(request.Options.Language)
	if err != nil {
		return nil, err
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("palabra stt requires a bearer credential")
	}
	endpoint, err := a.endpointPolicy.parse(request.Plan.Route.Endpoint, sttPath)
	if err != nil {
		return nil, fmt.Errorf("palabra stt endpoint: %w", err)
	}
	query := endpoint.Query()
	query.Set("format", "pcm_s16le")
	query.Set("sample_rate", fmt.Sprint(request.Media.SampleRateHz))
	if language != "auto" {
		query.Set("language", language)
	}
	endpoint.RawQuery = query.Encode()

	conn, response, err := websocket.Dial(ctx, endpoint.String(), &websocket.DialOptions{
		HTTPClient: a.httpClient, HTTPHeader: authHeaders(credential.Value),
	})
	if err != nil {
		return nil, dialError("Palabra STT", response, err)
	}
	conn.SetReadLimit(a.maxMessageBytes)
	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &sttStream{conn: conn, ctx: streamCtx, cancel: cancel, events: make(chan runtimepkg.ProviderEvent, a.eventBuffer)}
	go stream.readLoop()
	return stream, nil
}

func sttLanguage(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" || value == "auto" {
		return "auto", nil
	}
	if index := strings.IndexByte(value, '-'); index >= 0 {
		value = value[:index]
	}
	if _, ok := sttLanguages[value]; !ok {
		return "", fmt.Errorf("palabra stt does not support language %q", value)
	}
	return value, nil
}

type sttStream struct {
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent

	writeMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	closeErr     error
}

func (s *sttStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

func (s *sttStream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("palabra stt audio is empty")
	}
	return s.write(ctx, websocket.MessageBinary, audio)
}

// Palabra's dedicated ASR socket has no flush/finalize command. Its own
// endpointer emits is_eos boundaries, so a caller commit is intentionally a
// no-op rather than an invented wire message.
func (s *sttStream) CommitAudio(context.Context) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	return nil
}

func (s *sttStream) AppendText(context.Context, string) error {
	return runtimepkg.ErrUnsupportedOperation
}
func (s *sttStream) CommitText(context.Context) error { return runtimepkg.ErrUnsupportedOperation }
func (s *sttStream) Cancel(context.Context) error     { return s.abort() }
func (s *sttStream) Abort(context.Context) error      { return s.abort() }

func (s *sttStream) Close(context.Context) error {
	s.gracefulOnce.Do(func() {
		s.closed.Store(true)
		s.writeMu.Lock()
		s.closeErr = s.conn.Close(websocket.StatusNormalClosure, "")
		s.writeMu.Unlock()
		s.cancel()
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
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Palabra STT streaming write failed", Retryable: true, Cause: err}
	}
	return nil
}

func (s *sttStream) readLoop() {
	defer close(s.events)
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && !isNormalClose(err) && s.ctx.Err() == nil {
				s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Palabra STT streaming read failed", Retryable: true, Cause: err}})
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

func (s *sttStream) handleMessage(payload []byte) error {
	var message sttInbound
	if err := json.Unmarshal(payload, &message); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Palabra STT sent malformed streaming JSON", Retryable: true, Cause: err}
	}
	raw := append([]byte(nil), payload...)
	switch message.MessageType {
	case "transcription":
		kind := protocol.EventTranscriptDelta
		if message.IsEOS {
			kind = protocol.EventTranscriptFinal
		}
		return s.emit(runtimepkg.ProviderEvent{
			Type: kind,
			Data: marshalData(map[string]any{
				"text": message.Segment.Text, "is_final": message.IsEOS,
				"language": message.Language, "audio_start_ms": milliseconds(message.Segment.StartTime),
				"audio_end_ms": milliseconds(message.Segment.EndTime), "provider_request_id": message.TranscriptionID,
			}),
			Extensions: extension(raw),
		})
	case "error":
		return providerFrameError(message.Data.Code, message.Data.Description, raw)
	default:
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: marshalData(map[string]any{"message": "ignored Palabra STT message type", "provider_type": message.MessageType}), Extensions: extension(raw)})
	}
}

func (s *sttStream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func milliseconds(seconds float64) int64 { return int64(math.Round(seconds * 1000)) }

type sttInbound struct {
	MessageType     string `json:"message_type"`
	TranscriptionID string `json:"transcription_id"`
	Language        string `json:"language"`
	IsEOS           bool   `json:"is_eos"`
	Segment         struct {
		Text      string  `json:"text"`
		StartTime float64 `json:"start_time"`
		EndTime   float64 `json:"end_time"`
	} `json:"segment"`
	Data struct {
		Code        string `json:"code"`
		Description string `json:"desc"`
	} `json:"data"`
}
