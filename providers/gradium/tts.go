package gradium

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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
	// TTSAdapterID is the identifier returned by a Gradium TTS session plan.
	TTSAdapterID = "gradium.tts.v1"

	ttsPath = "/api/speech/tts"
)

// TTSConfig controls local transport limits. Credentials, model, and voice
// always come from the signed session plan and its provider-neutral request
// options, never from this configuration.
type TTSConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// TTSAdapter implements Gradium's /api/speech/tts WebSocket API.
type TTSAdapter struct {
	id              string
	httpClient      *http.Client
	eventBuffer     int
	maxMessageBytes int64
	endpointPolicy  upstream.WebSocketPolicy
}

// NewTTS creates a bounded Gradium TTS adapter.
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
	if config.EventBuffer < 1 {
		return nil, errors.New("gradium event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("gradium maximum message bytes must be positive")
	}
	endpointPolicy, err := upstream.NewWebSocketPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &TTSAdapter{
		id:              config.AdapterID,
		httpClient:      config.HTTPClient,
		eventBuffer:     config.EventBuffer,
		maxMessageBytes: config.MaxMessageBytes,
		endpointPolicy:  endpointPolicy,
	}, nil
}

func (a *TTSAdapter) ID() string { return a.id }

// Open opens a provider-direct Gradium TTS WebSocket and sends the mandatory
// `setup` frame before returning. Gradium answers any pre-setup frame with
// "Session not found. Send setup first." (code 1002), so setup cannot be
// deferred to the first AppendText.
func (a *TTSAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindTTS {
		return nil, fmt.Errorf("gradium supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "gradium" {
		return nil, fmt.Errorf("gradium adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("gradium requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("gradium requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("gradium media: %w", err)
	}
	model, err := concreteModel(request.Plan.Route.Model)
	if err != nil {
		return nil, err
	}
	outputFormat, err := pcmFormat(*request.Media)
	if err != nil {
		return nil, err
	}
	voice, err := synthesisVoice(request.Options.Voice, request.Plan.Route.Voice)
	if err != nil {
		return nil, err
	}
	credential := request.Plan.Route.Credential
	if credential == nil || credential.Kind != protocol.CredentialBearer || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("gradium requires a bearer credential")
	}
	endpoint, err := socketEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint, ttsPath)
	if err != nil {
		return nil, err
	}

	headers := make(http.Header)
	// Same single header for BYOK and managed; see doc.go, "One credential,
	// one code path".
	headers.Set("x-api-key", credential.Value)

	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{
		HTTPClient: httpClientOrDefault(a.httpClient),
		HTTPHeader: headers,
	})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		return nil, &runtimepkg.ProviderError{
			Code:           dialErrorCode(status),
			Message:        "Gradium streaming connection could not be established",
			Retryable:      status == 0 || status == http.StatusTooManyRequests || status >= 500,
			ProviderStatus: status,
			Cause:          err,
		}
	}
	conn.SetReadLimit(a.maxMessageBytes)

	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &ttsStream{
		conn:   conn,
		ctx:    streamCtx,
		cancel: cancel,
		events: make(chan runtimepkg.ProviderEvent, a.eventBuffer),
	}
	setup := ttsSetup{
		Type:         "setup",
		ModelName:    model,
		VoiceID:      voice,
		OutputFormat: outputFormat,
	}
	if err := stream.writeJSON(ctx, setup); err != nil {
		_ = stream.abort()
		return nil, err
	}
	go stream.readLoop()
	return stream, nil
}

// synthesisVoice resolves the voice for this utterance. Plan.Route.Voice is
// the fallback because a caller that routed with provider "auto" cannot know
// which vendor it got, so the control plane is the only party able to name a
// Gradium voice id. Gradium would accept a socket with no voice at all and
// silently substitute its own house voice; the gateway refuses instead, since
// billing a customer for a voice nobody chose is worse than failing loudly.
func synthesisVoice(requested, planned string) (string, error) {
	if voice := strings.TrimSpace(requested); voice != "" {
		return voice, nil
	}
	if voice := strings.TrimSpace(planned); voice != "" {
		return voice, nil
	}
	return "", errors.New("gradium requires a voice id in request options or the session plan")
}

// ttsSetup is Gradium's mandatory first frame on /api/speech/tts.
type ttsSetup struct {
	Type         string `json:"type"`
	ModelName    string `json:"model_name"`
	VoiceID      string `json:"voice_id"`
	OutputFormat string `json:"output_format"`
}

type ttsTextFrame struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type ttsStream struct {
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent

	writeMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	closeErr     error

	// Read only by readLoop, the single reader goroutine.
	requestID    string
	audioStarted bool
}

func (s *ttsStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

func (s *ttsStream) WriteAudio(context.Context, []byte) error {
	return runtimepkg.ErrUnsupportedOperation
}

func (s *ttsStream) CommitAudio(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// AppendText streams one text chunk. Gradium treats successive `text` frames
// as separate chunks and inserts spacing between them, so the caller's own
// chunk boundaries are preserved on the wire rather than concatenated locally.
func (s *ttsStream) AppendText(ctx context.Context, text string) error {
	if strings.TrimSpace(text) == "" {
		return errors.New("gradium transcript is empty")
	}
	return s.writeJSON(ctx, ttsTextFrame{Type: "text", Text: text})
}

// CommitText closes the input side of the utterance. Gradium then drains
// whatever audio it still owes, sends its own `end_of_stream`, and closes.
func (s *ttsStream) CommitText(ctx context.Context) error {
	return s.writeJSON(ctx, map[string]string{"type": "end_of_stream"})
}

// Cancel aborts the socket. Gradium's TTS protocol documents no cancel command
// and no way to discard queued audio, so dropping the connection is the only
// mechanism that actually stops generation.
func (s *ttsStream) Cancel(context.Context) error { return s.abort() }

// Abort immediately tears down the socket after a terminal runtime failure.
func (s *ttsStream) Abort(context.Context) error { return s.abort() }

// Close performs a normal WebSocket close. Unlike the STT adapter this does
// not write `end_of_stream` first: on the TTS socket that frame is the
// utterance boundary and CommitText already owns it, so writing it here would
// commit an utterance the caller deliberately did not commit.
func (s *ttsStream) Close(context.Context) error {
	s.gracefulOnce.Do(func() {
		s.closed.Store(true)
		s.writeMu.Lock()
		err := s.conn.Close(websocket.StatusNormalClosure, "")
		s.writeMu.Unlock()
		if err != nil {
			s.closeErr = err
			_ = s.abort()
		}
	})
	return s.closeErr
}

func (s *ttsStream) abort() error {
	s.abortOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		if err := s.conn.CloseNow(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
	})
	return s.closeErr
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
		return &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "Gradium streaming write failed",
			Retryable: true,
			Cause:     err,
		}
	}
	return nil
}

func (s *ttsStream) readLoop() {
	defer func() {
		s.cancel()
		close(s.events)
	}()
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && !isNormalClose(err) && s.ctx.Err() == nil {
				s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{
					Code:      "provider_unavailable",
					Message:   "Gradium streaming read failed",
					Retryable: true,
					Cause:     err,
				}})
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

func (s *ttsStream) handleMessage(payload []byte) (bool, error) {
	var message ttsInbound
	if err := json.Unmarshal(payload, &message); err != nil {
		return false, &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "Gradium sent malformed streaming JSON",
			Retryable: true,
			Cause:     err,
		}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))
	switch message.Type {
	case "ready":
		s.requestID = message.RequestID
		return false, s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventUsageObserved,
			Data:       usageData(message.RequestID),
			Extensions: extension(raw),
		})
	case "audio":
		return false, s.handleAudio(message, raw)
	case "text":
		// Gradium's TTS `text` frame is the word-timing surface: the text it
		// echoes is annotated with the span of audio that renders it, which is
		// alignment, not a transcript.
		return false, s.emit(runtimepkg.ProviderEvent{
			Type: protocol.EventAlignment,
			Data: marshalData(map[string]any{
				"text":                message.Text,
				"audio_start_ms":      milliseconds(message.StartS),
				"audio_end_ms":        milliseconds(message.StopS),
				"provider_request_id": s.requestID,
			}),
			Extensions: extension(raw),
		})
	case "flushed":
		return false, s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventWarning,
			Data:       s.warningData("flushed"),
			Extensions: extension(raw),
		})
	case "end_of_stream":
		return true, s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventAudioDone,
			Data:       marshalData(map[string]any{"provider_request_id": s.requestID}),
			Extensions: extension(raw),
		})
	case "error":
		return false, providerFrameError(message.Code, message.Message, raw)
	default:
		return false, s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventWarning,
			Data:       s.warningData(message.Type),
			Extensions: extension(raw),
		})
	}
}

func (s *ttsStream) handleAudio(message ttsInbound, raw json.RawMessage) error {
	audio, err := base64.StdEncoding.DecodeString(message.Audio)
	if err != nil {
		return &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "Gradium sent invalid audio data",
			Retryable: true,
			Cause:     err,
		}
	}
	if !s.audioStarted {
		s.audioStarted = true
		if err := s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventAudioStarted,
			Data:       marshalData(map[string]any{"provider_request_id": s.requestID}),
			Extensions: extension(raw),
		}); err != nil {
			return err
		}
	}
	return s.emit(runtimepkg.ProviderEvent{
		Type: protocol.EventAudioFrame,
		Data: marshalData(map[string]any{
			"audio_start_ms":      milliseconds(message.StartS),
			"audio_end_ms":        milliseconds(message.StopS),
			"provider_request_id": s.requestID,
		}),
		Extensions: extension(raw),
		Audio:      audio,
	})
}

func (s *ttsStream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *ttsStream) warningData(messageType string) json.RawMessage {
	return marshalData(map[string]any{
		"message":             "ignored Gradium message type",
		"provider_type":       messageType,
		"provider_request_id": s.requestID,
	})
}

// ttsInbound covers every documented server frame on /api/speech/tts.
type ttsInbound struct {
	Type      string  `json:"type"`
	RequestID string  `json:"request_id"`
	Audio     string  `json:"audio"`
	Text      string  `json:"text"`
	StartS    float64 `json:"start_s"`
	StopS     float64 `json:"stop_s"`
	StreamID  int     `json:"stream_id"`
	Message   string  `json:"message"`
	Code      int     `json:"code"`
}
