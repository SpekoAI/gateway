package palabra

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
	"unicode/utf8"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

const (
	TTSAdapterID  = "palabra.tts.v1"
	ttsPath       = "/tts-api/v1/text-to-speech/stream"
	TTSModel      = "auto"
	DefaultVoice  = "default_low"
	maxTextRunes  = 1024
	minSampleRate = 8_000
	maxSampleRate = 48_000
)

type TTSConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

type TTSAdapter struct {
	id              string
	httpClient      *http.Client
	eventBuffer     int
	maxMessageBytes int64
	endpointPolicy  endpointPolicy
}

func NewTTS(config TTSConfig) (*TTSAdapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = TTSAdapterID
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = 32
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = 8 << 20
	}
	if config.EventBuffer < 1 {
		return nil, errors.New("palabra tts event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("palabra tts maximum message bytes must be positive")
	}
	policy, err := newEndpointPolicy([]string{"stream.palabra.ai", "stream.us.palabra.ai"}, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &TTSAdapter{id: config.AdapterID, httpClient: config.HTTPClient, eventBuffer: config.EventBuffer, maxMessageBytes: config.MaxMessageBytes, endpointPolicy: policy}, nil
}

func (a *TTSAdapter) ID() string { return a.id }

func (a *TTSAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindTTS {
		return nil, fmt.Errorf("palabra tts supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "palabra" {
		return nil, fmt.Errorf("palabra tts adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("palabra tts requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model != TTSModel {
		return nil, fmt.Errorf("palabra tts supports only model %q, got %q", TTSModel, model)
	}
	if request.Media == nil {
		return nil, errors.New("palabra tts requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("palabra tts media: %w", err)
	}
	if request.Media.Encoding != "pcm_s16le" || request.Media.Channels != 1 {
		return nil, fmt.Errorf("palabra tts requires mono pcm_s16le output, got %s/%d channels", request.Media.Encoding, request.Media.Channels)
	}
	if request.Media.SampleRateHz < minSampleRate || request.Media.SampleRateHz > maxSampleRate {
		return nil, fmt.Errorf("palabra tts sample rate must be between %d and %d Hz", minSampleRate, maxSampleRate)
	}
	voice := strings.TrimSpace(request.Options.Voice)
	if voice == "" {
		voice = strings.TrimSpace(request.Plan.Route.Voice)
	}
	if voice == "" {
		voice = DefaultVoice
	}
	language := strings.ToLower(strings.TrimSpace(request.Options.Language))
	if language == "" || language == "auto" {
		language = "en"
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("palabra tts requires a bearer credential")
	}
	endpoint, err := a.endpointPolicy.parse(request.Plan.Route.Endpoint, ttsPath)
	if err != nil {
		return nil, fmt.Errorf("palabra tts endpoint: %w", err)
	}
	client := a.httpClient
	if client == nil {
		client = http.DefaultClient
	}
	conn, response, err := websocket.Dial(ctx, endpoint.String(), &websocket.DialOptions{HTTPClient: client, HTTPHeader: authHeaders(credential.Value)})
	if err != nil {
		return nil, dialError("Palabra TTS", response, err)
	}
	conn.SetReadLimit(a.maxMessageBytes)
	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &ttsStream{conn: conn, ctx: streamCtx, cancel: cancel, events: make(chan runtimepkg.ProviderEvent, a.eventBuffer)}
	if err := stream.writeJSON(ctx, ttsInit{
		Type: "init", Language: language, Model: model,
		VoiceOptions: ttsVoiceOptions{VoiceID: voice, Speed: 1, DeaccentStrength: 1},
		Output:       ttsOutput{Format: "pcm", SampleRate: request.Media.SampleRateHz},
	}); err != nil {
		_ = stream.abort()
		return nil, err
	}
	go stream.readLoop()
	return stream, nil
}

type ttsInit struct {
	Type         string          `json:"type"`
	Language     string          `json:"language"`
	Model        string          `json:"model"`
	VoiceOptions ttsVoiceOptions `json:"voice_options"`
	Output       ttsOutput       `json:"output"`
}

type ttsVoiceOptions struct {
	VoiceID          string  `json:"voice_id"`
	Speed            float64 `json:"speed"`
	DeaccentStrength float64 `json:"deaccent_strength"`
}

type ttsOutput struct {
	Format     string `json:"format"`
	SampleRate int    `json:"sample_rate"`
}

type ttsText struct {
	Type  string `json:"type"`
	Text  string `json:"text"`
	IsEOS bool   `json:"is_eos"`
}

type ttsStream struct {
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent

	writeMu      sync.Mutex
	stateMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	closeErr     error
	pending      string
	audioStarted bool // read-loop owned
}

func (s *ttsStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }
func (s *ttsStream) WriteAudio(context.Context, []byte) error {
	return runtimepkg.ErrUnsupportedOperation
}
func (s *ttsStream) CommitAudio(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// AppendText keeps one tail chunk locally. When another fragment arrives the
// previous one can be sent with is_eos=false; CommitText then marks the actual
// last fragment without inventing an empty text message the API rejects.
func (s *ttsStream) AppendText(ctx context.Context, text string) error {
	if strings.TrimSpace(text) == "" {
		return errors.New("palabra tts text is empty")
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if s.pending != "" {
		if err := s.writeJSON(ctx, ttsText{Type: "text", Text: s.pending, IsEOS: false}); err != nil {
			return err
		}
	}
	chunks := splitRunes(text, maxTextRunes)
	for _, chunk := range chunks[:len(chunks)-1] {
		if err := s.writeJSON(ctx, ttsText{Type: "text", Text: chunk, IsEOS: false}); err != nil {
			return err
		}
	}
	s.pending = chunks[len(chunks)-1]
	return nil
}

func (s *ttsStream) CommitText(ctx context.Context) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if s.pending == "" {
		return errors.New("palabra tts has no buffered text to synthesize")
	}
	text := s.pending
	if err := s.writeJSON(ctx, ttsText{Type: "text", Text: text, IsEOS: true}); err != nil {
		return err
	}
	s.pending = ""
	return nil
}

func splitRunes(value string, limit int) []string {
	if utf8.RuneCountInString(value) <= limit {
		return []string{value}
	}
	runes := []rune(value)
	chunks := make([]string, 0, (len(runes)+limit-1)/limit)
	for len(runes) > 0 {
		count := min(limit, len(runes))
		chunks = append(chunks, string(runes[:count]))
		runes = runes[count:]
	}
	return chunks
}

func (s *ttsStream) Cancel(ctx context.Context) error {
	s.stateMu.Lock()
	s.pending = ""
	s.stateMu.Unlock()
	return s.writeJSON(ctx, map[string]string{"type": "cancel"})
}

func (s *ttsStream) Abort(context.Context) error { return s.abort() }

func (s *ttsStream) Close(context.Context) error {
	s.gracefulOnce.Do(func() {
		s.closed.Store(true)
		s.writeMu.Lock()
		s.closeErr = s.conn.Close(websocket.StatusNormalClosure, "")
		s.writeMu.Unlock()
		s.cancel()
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
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Palabra TTS streaming write failed", Retryable: true, Cause: err}
	}
	return nil
}

func (s *ttsStream) readLoop() {
	defer close(s.events)
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && !isNormalClose(err) && s.ctx.Err() == nil {
				s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Palabra TTS streaming read failed", Retryable: true, Cause: err}})
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

func (s *ttsStream) handleMessage(payload []byte) error {
	var message ttsInbound
	if err := json.Unmarshal(payload, &message); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Palabra TTS sent malformed streaming JSON", Retryable: true, Cause: err}
	}
	raw := append([]byte(nil), payload...)
	switch message.MessageType {
	case "audio_chunk":
		if message.Data.Audio != "" {
			audio, err := base64.StdEncoding.DecodeString(message.Data.Audio)
			if err != nil {
				return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Palabra TTS sent invalid audio data", Retryable: true, Cause: err}
			}
			if !s.audioStarted {
				s.audioStarted = true
				if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: marshalData(map[string]any{"provider_request_id": message.Data.GenerationID}), Extensions: extension(raw)}); err != nil {
					return err
				}
			}
			if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Data: marshalData(map[string]any{"provider_request_id": message.Data.GenerationID}), Extensions: extension(raw), Audio: audio}); err != nil {
				return err
			}
		}
		if message.Data.LastChunk {
			s.audioStarted = false
			return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: marshalData(map[string]any{"provider_request_id": message.Data.GenerationID}), Extensions: extension(raw)})
		}
		return nil
	case "error":
		return providerFrameError(message.Data.Code, message.Data.Description, raw)
	default:
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: marshalData(map[string]any{"message": "ignored Palabra TTS message type", "provider_type": message.MessageType}), Extensions: extension(raw)})
	}
}

func (s *ttsStream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

type ttsInbound struct {
	MessageType string `json:"message_type"`
	Data        struct {
		Audio        string `json:"audio"`
		GenerationID string `json:"generation_id"`
		LastChunk    bool   `json:"last_chunk"`
		Code         string `json:"code"`
		Description  string `json:"desc"`
	} `json:"data"`
}
