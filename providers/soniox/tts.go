package soniox

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

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

const (
	// TTSAdapterID is the identifier returned by a Soniox TTS session plan.
	TTSAdapterID = "soniox.tts.v1"

	ttsExtensionID  = "soniox.com/tts/v1"
	ttsOfficialHost = "tts-rt.soniox.com"
	ttsEndpointPath = "/tts-websocket"

	// Soniox rejects a longer text chunk with HTTP 400
	// "Text is too long (max length 5000)."
	ttsMaxTextCharacters = 5_000
)

// ttsSampleRates are the output rates Soniox documents for pcm_s16le. Anything
// else is refused with HTTP 400 after the socket is already open, so it is
// cheaper to refuse it here.
var ttsSampleRates = map[int]struct{}{
	8_000:  {},
	16_000: {},
	24_000: {},
	44_100: {},
	48_000: {},
}

// TTSConfig controls local transport limits. Credentials, model, voice, and
// language always come from the signed session plan and its request options.
type TTSConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// TTSAdapter implements Soniox's /tts-websocket realtime API.
type TTSAdapter struct {
	id              string
	httpClient      *http.Client
	eventBuffer     int
	maxMessageBytes int64
	endpointPolicy  upstream.WebSocketPolicy
}

// NewTTS creates a bounded Soniox TTS adapter.
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
		return nil, errors.New("soniox event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("soniox maximum message bytes must be positive")
	}
	endpointPolicy, err := upstream.NewWebSocketPolicy(ttsOfficialHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
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

// Open dials the socket and immediately starts one stream on it. Soniox closes
// a connection that has not sent a start message with a valid API key within
// about ten seconds, and a keepalive does not count as authentication, so the
// start cannot wait for the caller's first AppendText the way Cartesia's
// context can. A stream that finishes releases its id; the next AppendText
// starts a fresh stream on the same socket.
func (a *TTSAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindTTS {
		return nil, fmt.Errorf("soniox tts supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "soniox" {
		return nil, fmt.Errorf("soniox adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("soniox tts requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("soniox tts requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("soniox tts media: %w", err)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model == "" || model == "auto" {
		return nil, errors.New("soniox tts requires a concrete model in the session plan")
	}
	// voice, language, and audio_format are all documented as required start
	// fields; Soniox answers a missing one with HTTP 400 on an open socket.
	voice := strings.TrimSpace(request.Options.Voice)
	if voice == "" {
		return nil, errors.New("soniox tts requires a voice in request options")
	}
	language, ok := sonioxPrimaryLanguage(request.Options.Language)
	if !ok {
		return nil, errors.New("soniox tts requires a concrete language in request options")
	}
	if request.Media.Encoding != "pcm_s16le" {
		return nil, fmt.Errorf("soniox tts streaming output requires pcm_s16le, got %q", request.Media.Encoding)
	}
	if _, ok := ttsSampleRates[request.Media.SampleRateHz]; !ok {
		return nil, fmt.Errorf("soniox tts does not support sample rate %d", request.Media.SampleRateHz)
	}
	credential := request.Plan.Route.Credential
	if credential == nil || credential.Kind != protocol.CredentialBearer || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("soniox tts requires a bearer credential")
	}
	endpoint, err := ttsEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}

	// No Authorization header: the api_key travels inside the start message for
	// managed and BYOK credentials alike. See doc.go.
	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: sttHTTPClient(a.httpClient)})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		code, retryable := sonioxStatusCode(status)
		return nil, &runtimepkg.ProviderError{
			Code:           code,
			Message:        "Soniox streaming connection could not be established",
			Retryable:      retryable,
			ProviderStatus: status,
			Cause:          err,
		}
	}
	conn.SetReadLimit(a.maxMessageBytes)

	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &ttsStream{
		conn:       conn,
		ctx:        streamCtx,
		cancel:     cancel,
		events:     make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		apiKey:     credential.Value,
		model:      model,
		voice:      voice,
		language:   language,
		sampleRate: request.Media.SampleRateHz,
	}
	if _, err := stream.startStream(ctx); err != nil {
		cancel()
		_ = conn.CloseNow()
		return nil, err
	}
	go stream.readLoop()
	return stream, nil
}

func ttsEndpoint(policy upstream.WebSocketPolicy, rawEndpoint string) (string, error) {
	endpoint, err := policy.Parse(rawEndpoint)
	if err != nil {
		return "", fmt.Errorf("soniox tts endpoint: %w", err)
	}
	if endpoint.Path != ttsEndpointPath {
		return "", fmt.Errorf("soniox tts endpoint path must be %s, got %q", ttsEndpointPath, endpoint.Path)
	}
	return endpoint.String(), nil
}

type ttsStream struct {
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent

	apiKey     string
	model      string
	voice      string
	language   string
	sampleRate int

	writeMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	closing      atomic.Bool
	closeErr     error

	stateMu      sync.Mutex
	streamID     string
	streamDone   chan struct{}
	textEnded    bool
	audioStarted bool
	requestID    string
}

func (s *ttsStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

func (s *ttsStream) WriteAudio(context.Context, []byte) error {
	return runtimepkg.ErrUnsupportedOperation
}

func (s *ttsStream) CommitAudio(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// AppendText sends one text chunk for the active stream. A stream that already
// received text_end is closed for input, so a new one is started first.
func (s *ttsStream) AppendText(ctx context.Context, text string) error {
	if strings.TrimSpace(text) == "" {
		return errors.New("soniox tts text is empty")
	}
	if len(text) > ttsMaxTextCharacters {
		return fmt.Errorf("soniox tts text exceeds %d characters", ttsMaxTextCharacters)
	}
	streamID, needsStart, err := s.currentOrNewStream()
	if err != nil {
		return err
	}
	if needsStart {
		if streamID, err = s.startStream(ctx); err != nil {
			return err
		}
	}
	return s.writeJSON(ctx, ttsTextRequest{Text: text, TextEnd: false, StreamID: streamID})
}

// CommitText closes the stream for input with the documented end-of-input
// marker: an empty text chunk carrying text_end. Soniox's own reference client
// sends exactly this after its last real chunk.
func (s *ttsStream) CommitText(ctx context.Context) error {
	streamID, err := s.markTextEnded()
	if err != nil {
		return err
	}
	return s.writeJSON(ctx, ttsTextRequest{Text: "", TextEnd: true, StreamID: streamID})
}

// Cancel stops generation for the active stream. Soniox rejects a cancel that
// also carries text or text_end, so it is sent on its own; the server answers
// with terminated and sends no further audio.
func (s *ttsStream) Cancel(ctx context.Context) error {
	streamID, ok := s.activeStream()
	if !ok {
		return runtimepkg.ErrSessionClosed
	}
	return s.writeJSON(ctx, map[string]any{"stream_id": streamID, "cancel": true})
}

// Close waits for the active stream's terminated event before closing the
// socket, so audio still in flight after CommitText is not discarded.
func (s *ttsStream) Close(ctx context.Context) error {
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
func (s *ttsStream) Abort(context.Context) error { return s.abort() }

func (s *ttsStream) abort() error {
	s.abortOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		if err := s.conn.CloseNow(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
		s.finishStream("")
	})
	return s.closeErr
}

// startStream allocates a stream id and sends the start message that both
// authenticates the connection and configures the new stream.
func (s *ttsStream) startStream(ctx context.Context) (string, error) {
	streamID, err := newStreamID()
	if err != nil {
		return "", err
	}
	s.stateMu.Lock()
	if s.streamID != "" {
		s.stateMu.Unlock()
		return "", errors.New("soniox tts stream is already active")
	}
	s.streamID = streamID
	s.streamDone = make(chan struct{})
	s.textEnded = false
	s.audioStarted = false
	s.stateMu.Unlock()

	if err := s.writeJSON(ctx, ttsStartRequest{
		APIKey:      s.apiKey,
		StreamID:    streamID,
		Model:       s.model,
		Language:    s.language,
		Voice:       s.voice,
		AudioFormat: "pcm_s16le",
		SampleRate:  s.sampleRate,
	}); err != nil {
		s.finishStream(streamID)
		return "", err
	}
	return streamID, nil
}

// currentOrNewStream reports the active stream id, or asks the caller to start
// a new one when the previous stream already saw text_end and terminated.
func (s *ttsStream) currentOrNewStream() (string, bool, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() || s.closing.Load() {
		return "", false, runtimepkg.ErrSessionClosed
	}
	if s.streamID == "" {
		return "", true, nil
	}
	if s.textEnded {
		return "", false, errors.New("soniox tts stream is closed for input until it terminates")
	}
	return s.streamID, false, nil
}

func (s *ttsStream) markTextEnded() (string, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() || s.closing.Load() || s.streamID == "" || s.textEnded {
		return "", runtimepkg.ErrSessionClosed
	}
	s.textEnded = true
	return s.streamID, nil
}

func (s *ttsStream) activeStream() (string, bool) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.streamID, s.streamID != ""
}

func (s *ttsStream) activeDone() <-chan struct{} {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.streamDone
}

func (s *ttsStream) finishStream(streamID string) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.streamID == "" || (streamID != "" && s.streamID != streamID) {
		return
	}
	close(s.streamDone)
	s.streamID = ""
	s.streamDone = nil
	s.textEnded = false
	s.audioStarted = false
}

func (s *ttsStream) markAudioStarted(streamID string) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.streamID != streamID || s.audioStarted {
		return false
	}
	s.audioStarted = true
	return true
}

func (s *ttsStream) setRequestID(value string) bool {
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
	// Soniox refuses binary frames on this endpoint; every client message is a
	// JSON text frame.
	if err := s.conn.Write(ctx, websocket.MessageText, payload); err != nil {
		return &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "Soniox streaming write failed",
			Retryable: true,
			Cause:     err,
		}
	}
	return nil
}

func (s *ttsStream) readLoop() {
	defer func() {
		s.cancel()
		s.finishStream("")
		close(s.events)
	}()
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && s.ctx.Err() == nil && !sonioxIsNormalClose(err) {
				s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{
					Code:      "provider_unavailable",
					Message:   "Soniox streaming read failed",
					Retryable: true,
					Cause:     err,
				}})
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
	var message ttsInboundMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "Soniox sent malformed streaming JSON",
			Retryable: true,
			Cause:     err,
		}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))

	if message.ErrorType != "" || message.ErrorCode != 0 {
		// Soniox keeps the connection open and terminates only the failed
		// stream, but this adapter runs one stream per session, so the failure
		// is the attempt's failure and the runtime fails it over.
		code, retryable := sonioxErrorCode(message.ErrorType, message.ErrorCode)
		return &runtimepkg.ProviderError{
			Code:           code,
			Message:        sonioxErrorMessage("Soniox reported a streaming error", message.ErrorMessage),
			Retryable:      retryable,
			ProviderStatus: message.ErrorCode,
			Extensions:     ttsExtension(raw),
		}
	}
	if s.setRequestID(message.RequestID) {
		if err := s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventUsageObserved,
			Data:       sonioxUsageData(message.RequestID, nil),
			Extensions: ttsExtension(raw),
		}); err != nil {
			return err
		}
	}

	if message.Audio != "" {
		audio, err := base64.StdEncoding.DecodeString(message.Audio)
		if err != nil {
			return &runtimepkg.ProviderError{
				Code:      "provider_unavailable",
				Message:   "Soniox sent invalid audio data",
				Retryable: true,
				Cause:     err,
			}
		}
		if s.markAudioStarted(message.StreamID) {
			if err := s.emit(runtimepkg.ProviderEvent{
				Type:       protocol.EventAudioStarted,
				Data:       s.ttsStreamData(message.StreamID),
				Extensions: ttsExtension(raw),
			}); err != nil {
				return err
			}
		}
		if err := s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventAudioFrame,
			Data:       s.ttsStreamData(message.StreamID),
			Extensions: ttsExtension(raw),
			Audio:      audio,
		}); err != nil {
			return err
		}
	}
	if message.Timestamps != nil {
		if err := s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventAlignment,
			Data:       s.ttsAlignmentData(message.StreamID, message.Timestamps),
			Extensions: ttsExtension(raw),
		}); err != nil {
			return err
		}
	}
	// audio_end only promises that no further audio frames follow; Soniox is
	// explicit that the stream is complete at terminated, not before, so the
	// terminal event is bound to terminated alone.
	if message.Terminated {
		s.finishStream(message.StreamID)
		return s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventAudioDone,
			Data:       s.ttsStreamData(message.StreamID),
			Extensions: ttsExtension(raw),
		})
	}
	return nil
}

func (s *ttsStream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *ttsStream) ttsStreamData(streamID string) json.RawMessage {
	return sonioxMarshalData(map[string]any{"stream_id": streamID, "provider_request_id": s.currentRequestID()})
}

func (s *ttsStream) ttsAlignmentData(streamID string, timestamps json.RawMessage) json.RawMessage {
	return sonioxMarshalData(map[string]any{
		"stream_id":            streamID,
		"character_timestamps": timestamps,
		"provider_request_id":  s.currentRequestID(),
	})
}

func ttsExtension(raw json.RawMessage) map[string]json.RawMessage {
	return map[string]json.RawMessage{ttsExtensionID: raw}
}

func newStreamID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("generate Soniox stream id: %w", err)
	}
	return hex.EncodeToString(bytes), nil
}

type ttsStartRequest struct {
	APIKey      string `json:"api_key"`
	StreamID    string `json:"stream_id"`
	Model       string `json:"model"`
	Language    string `json:"language"`
	Voice       string `json:"voice"`
	AudioFormat string `json:"audio_format"`
	SampleRate  int    `json:"sample_rate"`
}

type ttsTextRequest struct {
	Text     string `json:"text"`
	TextEnd  bool   `json:"text_end"`
	StreamID string `json:"stream_id"`
}

type ttsInboundMessage struct {
	StreamID     string          `json:"stream_id"`
	Audio        string          `json:"audio"`
	AudioEnd     bool            `json:"audio_end"`
	Terminated   bool            `json:"terminated"`
	Timestamps   json.RawMessage `json:"timestamps"`
	ErrorCode    int             `json:"error_code"`
	ErrorType    string          `json:"error_type"`
	ErrorMessage string          `json:"error_message"`
	RequestID    string          `json:"request_id"`
}
