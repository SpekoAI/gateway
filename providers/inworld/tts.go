package inworld

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
	"time"
	"unicode/utf8"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

const (
	// AdapterID is versioned because v2 replaces one HTTP request per utterance
	// with Inworld's persistent bidirectional WebSocket. That distinction is
	// security-sensitive for managed routes: a one-time token authenticates one
	// connection and cannot be replayed for a second HTTP request.
	AdapterID   = "inworld.tts.v2"
	extensionID = "inworld.ai/tts/v1"

	officialAPIHost = "api.inworld.ai"
	streamPath      = "/tts/v1/voice:streamBidirectional"

	DefaultModel            = "inworld-tts-2"
	maxInputCharacters      = 2_000
	defaultMaxMessageBytes  = 64 << 20
	defaultCloseIdleTimeout = 30 * time.Second
)

var supportedModels = map[string]struct{}{
	"inworld-tts-2": {}, "inworld-tts-2-flash": {},
	"inworld-tts-1.5-max": {}, "inworld-tts-1.5-mini": {},
}

var discontinuedModels = map[string]string{
	"inworld-tts-1": "inworld-tts-1.5-mini", "inworld-tts-1-max": "inworld-tts-1.5-max",
}

// Config controls local transport limits. Provider identity, model, voice, and
// credential come from a verified session plan and its request options.
type Config struct {
	AdapterID       string
	HTTPClient      *http.Client
	EventBuffer     int
	MaxMessageBytes int64
	// GracefulCloseIdleTimeout bounds a Close whose provider reader cannot
	// make progress, including when downstream has stopped draining Events.
	GracefulCloseIdleTimeout time.Duration
	// MaxResponseBytes is retained as a compatibility alias from v1. New code
	// should use MaxMessageBytes; if both are set MaxMessageBytes wins.
	MaxResponseBytes      int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// Adapter implements Inworld's /tts/v1/voice:streamBidirectional API.
type Adapter struct {
	id                       string
	httpClient               *http.Client
	eventBuffer              int
	maxMessageBytes          int64
	gracefulCloseIdleTimeout time.Duration
	endpointPolicy           upstream.WebSocketPolicy
}

func New(config Config) (*Adapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = AdapterID
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = 32
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = config.MaxResponseBytes
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = defaultMaxMessageBytes
	}
	if config.GracefulCloseIdleTimeout == 0 {
		config.GracefulCloseIdleTimeout = defaultCloseIdleTimeout
	}
	if config.EventBuffer < 1 {
		return nil, errors.New("inworld event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("inworld maximum message bytes must be positive")
	}
	if config.GracefulCloseIdleTimeout < 0 {
		return nil, errors.New("inworld graceful close idle timeout must be positive")
	}
	policy, err := upstream.NewWebSocketPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &Adapter{
		id: config.AdapterID, httpClient: config.HTTPClient, eventBuffer: config.EventBuffer,
		maxMessageBytes: config.MaxMessageBytes, gracefulCloseIdleTimeout: config.GracefulCloseIdleTimeout,
		endpointPolicy: policy,
	}, nil
}

func (a *Adapter) ID() string { return a.id }

// Open consumes the credential exactly once, in the WebSocket handshake. The
// connection then carries every synthesis request in the gateway session.
func (a *Adapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindTTS {
		return nil, fmt.Errorf("inworld supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "inworld" {
		return nil, fmt.Errorf("inworld adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("inworld tts requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("inworld tts requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("inworld media: %w", err)
	}
	if err := validateMedia(*request.Media); err != nil {
		return nil, err
	}
	model, err := validateModel(request.Plan.Route.Model)
	if err != nil {
		return nil, err
	}
	voice := strings.TrimSpace(request.Options.Voice)
	if voice == "" {
		return nil, errors.New("inworld tts requires a voice id in request options")
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("inworld tts requires a bearer credential")
	}
	endpoint, err := synthesisEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}

	headers := make(http.Header)
	headers.Set("Authorization", authorizationHeader(request.Plan.Execution.ProviderRoute, request.Plan.Execution.CredentialSource, credential.Value))
	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: httpClient(a.httpClient), HTTPHeader: headers})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		return nil, &runtimepkg.ProviderError{
			Code: statusErrorCode(status), Message: "Inworld TTS connection could not be established",
			Retryable: status == 0 || status == http.StatusTooManyRequests || status >= 500, ProviderStatus: status, Cause: err,
		}
	}
	conn.SetReadLimit(a.maxMessageBytes)
	streamCtx, cancel := context.WithCancel(context.Background())
	s := &stream{
		conn: conn, ctx: streamCtx, cancel: cancel,
		events: make(chan runtimepkg.ProviderEvent, a.eventBuffer), readDone: make(chan struct{}), responseProgress: make(chan struct{}, 1),
		gracefulCloseIdleTimeout: a.gracefulCloseIdleTimeout,
		model:                    model, voice: voice, language: strings.TrimSpace(request.Options.Language), media: *request.Media,
	}
	go s.readLoop()
	return s, nil
}

// authorizationHeader keeps permanent portal keys on the Basic channel and
// managed one-time tokens on Bearer. Both remain in a header; credentials are
// never copied into a URL or WebSocket subprotocol where infrastructure may log them.
func authorizationHeader(route protocol.ProviderRoute, source protocol.CredentialSource, value string) string {
	if route == protocol.RouteSpekoRelay || source == protocol.CredentialsBYOK {
		return "Basic " + value
	}
	return "Bearer " + value
}

func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

func httpClient(client *http.Client) *http.Client {
	if client != nil {
		return client
	}
	return http.DefaultClient
}

func validateMedia(media protocol.MediaFormat) error {
	if media.Encoding != "pcm_s16le" || media.Channels != 1 {
		return fmt.Errorf("inworld tts requires mono pcm_s16le output, got %s/%d channels", media.Encoding, media.Channels)
	}
	switch media.SampleRateHz {
	case 8_000, 16_000, 22_050, 24_000, 32_000, 44_100, 48_000:
		return nil
	default:
		return fmt.Errorf("inworld tts does not support sample rate %d", media.SampleRateHz)
	}
}

func validateModel(model string) (string, error) {
	model = strings.TrimSpace(model)
	if model == "" || model == "auto" {
		return "", errors.New("inworld tts requires a concrete model in the session plan")
	}
	if successor, discontinued := discontinuedModels[model]; discontinued {
		return "", fmt.Errorf("inworld tts model %q was discontinued; use %q", model, successor)
	}
	if _, ok := supportedModels[model]; !ok {
		return "", fmt.Errorf("inworld tts does not support model %q", model)
	}
	return model, nil
}

func synthesisEndpoint(policy upstream.WebSocketPolicy, rawEndpoint string) (string, error) {
	endpoint, err := policy.Parse(rawEndpoint)
	if err != nil {
		return "", fmt.Errorf("inworld endpoint: %w", err)
	}
	if endpoint.Path != streamPath {
		return "", fmt.Errorf("inworld tts endpoint path must be %s, got %q", streamPath, endpoint.Path)
	}
	return endpoint.String(), nil
}

type generation struct {
	utteranceID  string
	done         chan struct{}
	audioStarted bool
	audioBytes   int
	canceled     bool
	usage        json.RawMessage
}

type stream struct {
	conn             *websocket.Conn
	ctx              context.Context
	cancel           context.CancelFunc
	events           chan runtimepkg.ProviderEvent
	readDone         chan struct{}
	responseProgress chan struct{}

	gracefulCloseIdleTimeout time.Duration

	model    string
	voice    string
	language string
	media    protocol.MediaFormat

	writeMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	closing      atomic.Bool
	closeErr     error

	stateMu        sync.Mutex
	pending        strings.Builder
	contextID      string
	contextDone    chan struct{}
	contextClosing bool
	generation     *generation
}

func (s *stream) Events() <-chan runtimepkg.ProviderEvent  { return s.events }
func (s *stream) WriteAudio(context.Context, []byte) error { return runtimepkg.ErrUnsupportedOperation }
func (s *stream) CommitAudio(context.Context) error        { return runtimepkg.ErrUnsupportedOperation }

func (s *stream) AppendText(_ context.Context, text string) error {
	if text == "" {
		return errors.New("inworld tts text is empty")
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() || s.closing.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if s.generation != nil || s.contextClosing {
		return errors.New("inworld tts previous utterance has not completed")
	}
	if utf8.RuneCountInString(s.pending.String())+utf8.RuneCountInString(text) > maxInputCharacters {
		return &runtimepkg.ProviderError{Code: "input_too_large", Message: "Inworld TTS input exceeds 2000 characters", Retryable: false, ProviderStatus: http.StatusRequestEntityTooLarge}
	}
	s.pending.WriteString(text)
	return nil
}

// CommitText creates one reusable context lazily, then explicitly flushes one
// buffered utterance. Inworld processes the messages in order, so waiting for
// contextCreated would only add a network round trip.
func (s *stream) CommitText(ctx context.Context) error {
	contextID, utteranceID, text, create, err := s.beginGeneration()
	if err != nil {
		return err
	}
	if create {
		request := createContextRequest{ContextID: contextID, Create: createContext{
			VoiceID: s.voice, ModelID: s.model, Language: s.language,
			// PCM is headerless signed 16-bit little-endian audio. Requesting it
			// avoids forwarding a WAV container header as a gateway audio frame.
			AudioConfig: socketAudioConfig{AudioEncoding: "PCM", SampleRateHertz: s.media.SampleRateHz},
		}}
		if err := s.writeJSON(ctx, request); err != nil {
			s.failWrite(contextID, utteranceID)
			return err
		}
	}
	request := sendTextRequest{ContextID: contextID, SendText: sendText{Text: text, FlushContext: struct{}{}}}
	if err := s.writeJSON(ctx, request); err != nil {
		s.failWrite(contextID, utteranceID)
		return err
	}
	return nil
}

// Cancel closes the active provider context. A later utterance creates a fresh
// context on the same authenticated connection, so cancellation does not need
// another one-time credential.
func (s *stream) Cancel(ctx context.Context) error {
	s.stateMu.Lock()
	hadPending := s.pending.Len() > 0
	s.pending.Reset()
	if s.generation == nil {
		s.stateMu.Unlock()
		if hadPending {
			return nil
		}
		return runtimepkg.ErrSessionClosed
	}
	s.generation.canceled = true
	contextID, done := s.contextID, s.contextDone
	shouldWrite := !s.contextClosing
	s.contextClosing = true
	s.stateMu.Unlock()

	if shouldWrite {
		if err := s.writeJSON(ctx, closeContextRequest{ContextID: contextID, CloseContext: struct{}{}}); err != nil {
			s.abort()
			return err
		}
	}
	select {
	case <-done:
		return nil
	case <-s.ctx.Done():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *stream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		s.closing.Store(true)
		s.stateMu.Lock()
		s.pending.Reset()
		generationDone := s.activeGenerationDoneLocked()
		s.stateMu.Unlock()
		if generationDone != nil {
			s.closeErr = s.waitForCloseProgress(ctx, generationDone, "synthesis")
		}
		if s.closeErr == nil {
			contextID, contextDone, shouldWrite := s.beginContextClose()
			if shouldWrite {
				if err := s.writeJSON(ctx, closeContextRequest{ContextID: contextID, CloseContext: struct{}{}}); err != nil {
					s.closeErr = err
				} else {
					s.closeErr = s.waitForCloseProgress(ctx, contextDone, "context shutdown")
				}
			}
		}
		if s.closeErr == nil {
			s.closed.Store(true)
			// The provider context close acknowledgment is the authoritative
			// graceful completion. The WebSocket close handshake is best-effort:
			// some servers drop the transport immediately after that acknowledgment.
			_ = s.conn.Close(websocket.StatusNormalClosure, "")
		}
		if s.closeErr != nil {
			s.abort()
		}
		select {
		case <-s.readDone:
		case <-ctx.Done():
			if s.closeErr == nil {
				s.closeErr = ctx.Err()
			}
			s.abort()
		}
	})
	return s.closeErr
}

// waitForCloseProgress preserves graceful delivery while bounding abandoned
// consumers. A blocked event send prevents the socket reader from reaching the
// flush or context-close acknowledgement, so a caller using Background cannot
// rely on its context alone to break the runtime/provider lock cycle.
func (s *stream) waitForCloseProgress(ctx context.Context, done <-chan struct{}, phase string) error {
	timer := time.NewTimer(s.gracefulCloseIdleTimeout)
	defer timer.Stop()
	for {
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-s.responseProgress:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(s.gracefulCloseIdleTimeout)
		case <-timer.C:
			select {
			case <-done:
				return nil
			default:
			}
			return &runtimepkg.ProviderError{
				Code: "provider_unavailable", Message: "Inworld TTS " + phase + " stalled during graceful close",
				Retryable: true, Cause: context.DeadlineExceeded,
			}
		}
	}
}

func (s *stream) Abort(context.Context) error {
	s.abort()
	return nil
}

func (s *stream) abort() {
	s.abortOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		_ = s.conn.CloseNow()
		s.finishAll()
	})
}

func (s *stream) beginGeneration() (contextID, utteranceID, text string, create bool, err error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() || s.closing.Load() {
		return "", "", "", false, runtimepkg.ErrSessionClosed
	}
	if s.generation != nil || s.contextClosing {
		return "", "", "", false, errors.New("inworld tts previous utterance has not completed")
	}
	text = s.pending.String()
	if text == "" {
		return "", "", "", false, errors.New("inworld tts has no buffered text to synthesize")
	}
	utteranceID, err = newUtteranceID()
	if err != nil {
		return "", "", "", false, err
	}
	if s.contextID == "" {
		contextID, err = newContextID()
		if err != nil {
			return "", "", "", false, err
		}
		s.contextID, s.contextDone, create = contextID, make(chan struct{}), true
	} else {
		contextID = s.contextID
	}
	s.pending.Reset()
	s.generation = &generation{utteranceID: utteranceID, done: make(chan struct{})}
	return contextID, utteranceID, text, create, nil
}

func (s *stream) activeGenerationDoneLocked() <-chan struct{} {
	if s.generation == nil {
		return nil
	}
	return s.generation.done
}

func (s *stream) beginContextClose() (string, <-chan struct{}, bool) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.contextID == "" {
		return "", nil, false
	}
	shouldWrite := !s.contextClosing
	s.contextClosing = true
	return s.contextID, s.contextDone, shouldWrite
}

func (s *stream) failWrite(contextID, utteranceID string) {
	s.stateMu.Lock()
	if s.contextID == contextID && s.generation != nil && s.generation.utteranceID == utteranceID {
		close(s.generation.done)
		s.generation = nil
	}
	s.stateMu.Unlock()
	s.abort()
}

func (s *stream) finishAll() {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.generation != nil {
		close(s.generation.done)
		s.generation = nil
	}
	if s.contextDone != nil {
		close(s.contextDone)
	}
	s.contextID, s.contextDone, s.contextClosing = "", nil, false
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
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Inworld TTS streaming write failed", Retryable: true, Cause: err}
	}
	return nil
}

func (s *stream) readLoop() {
	defer func() {
		s.closed.Store(true)
		s.cancel()
		s.finishAll()
		close(s.events)
		close(s.readDone)
	}()
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && !s.closing.Load() && s.ctx.Err() == nil && !isNormalClose(err) {
				s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Inworld TTS streaming read failed", Retryable: true, Cause: err}})
			}
			return
		}
		if messageType != websocket.MessageText {
			continue
		}
		s.reportResponseProgress()
		if err := s.handleMessage(payload); err != nil {
			s.emit(runtimepkg.ProviderEvent{Err: err})
			return
		}
	}
}

func (s *stream) handleMessage(payload []byte) error {
	var message streamMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Inworld TTS sent malformed streaming JSON", Retryable: true, Cause: err}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))
	if message.Error != nil {
		return streamError(message.Error, raw)
	}
	if message.Result == nil {
		if message.Done {
			return s.handleFlush(raw)
		}
		return nil
	}
	result := message.Result
	if result.Status != nil && result.Status.Code != 0 {
		return streamError(result.Status, raw)
	}
	contextID := result.ContextID
	if contextID == "" {
		contextID = result.ContextIDSnake
	}
	if result.AudioChunk != nil {
		if err := s.handleAudio(contextID, result.AudioChunk.AudioContent, result.AudioChunk.TimestampInfo, result.AudioChunk.Usage, raw); err != nil {
			return err
		}
	} else if result.AudioContent != "" {
		if err := s.handleAudio(contextID, result.AudioContent, result.TimestampInfo, result.Usage, raw); err != nil {
			return err
		}
	}
	if len(result.TimestampInfo) > 0 && result.AudioContent == "" && result.AudioChunk == nil {
		if utteranceID, ok := s.currentUtterance(contextID); ok && !s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAlignment, Data: alignmentData(contextID, utteranceID, result.TimestampInfo), Extensions: extension(raw)}) {
			return s.ctx.Err()
		}
	}
	if len(result.Usage) > 0 {
		s.rememberUsage(contextID, result.Usage)
	}
	if len(result.FlushCompleted) > 0 {
		return s.handleFlush(raw)
	}
	if len(result.ContextClosed) > 0 {
		return s.handleContextClosed(contextID, raw)
	}
	return nil
}

func (s *stream) handleAudio(contextID, encoded string, timestamps, usage json.RawMessage, raw json.RawMessage) error {
	utteranceID, canceled, ok := s.observeGeneration(contextID, usage)
	if !ok {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Inworld TTS returned audio for an unknown context", Retryable: true, Extensions: extension(raw)}
	}
	if canceled {
		return nil
	}
	if len(timestamps) > 0 && !s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAlignment, Data: alignmentData(contextID, utteranceID, timestamps), Extensions: extension(raw)}) {
		return s.ctx.Err()
	}
	if encoded == "" {
		return nil
	}
	chunk, err := decodeAudio(encoded)
	if err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Inworld TTS sent invalid audio data", Retryable: true, Cause: err, Extensions: extension(raw)}
	}
	if len(chunk) == 0 {
		return nil
	}
	utteranceID, started, ok := s.recordAudio(contextID, len(chunk))
	if !ok {
		// Cancel may race base64 decoding; once it wins, discard the chunk.
		return nil
	}
	if started && !s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: utteranceData(contextID, utteranceID), Extensions: extension(raw)}) {
		return s.ctx.Err()
	}
	if !s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Data: utteranceData(contextID, utteranceID), Extensions: extension(raw), Audio: chunk}) {
		return s.ctx.Err()
	}
	return nil
}

func (s *stream) observeGeneration(contextID string, usage json.RawMessage) (string, bool, bool) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.generation == nil || (contextID != "" && contextID != s.contextID) {
		return "", false, false
	}
	if len(usage) > 0 {
		s.generation.usage = append(json.RawMessage(nil), usage...)
	}
	return s.generation.utteranceID, s.generation.canceled, true
}

func (s *stream) recordAudio(contextID string, count int) (string, bool, bool) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.generation == nil || s.generation.canceled || (contextID != "" && contextID != s.contextID) {
		return "", false, false
	}
	started := !s.generation.audioStarted
	s.generation.audioStarted = true
	s.generation.audioBytes += count
	return s.generation.utteranceID, started, true
}

func (s *stream) rememberUsage(contextID string, usage json.RawMessage) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.generation != nil && (contextID == "" || contextID == s.contextID) {
		s.generation.usage = append(json.RawMessage(nil), usage...)
	}
}

func (s *stream) currentUtterance(contextID string) (string, bool) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.generation == nil || (contextID != "" && contextID != s.contextID) {
		return "", false
	}
	return s.generation.utteranceID, true
}

func (s *stream) handleFlush(raw json.RawMessage) error {
	s.stateMu.Lock()
	if s.generation == nil {
		s.stateMu.Unlock()
		return nil
	}
	generation, contextID := s.generation, s.contextID
	s.generation = nil
	close(generation.done)
	s.stateMu.Unlock()

	if generation.canceled {
		return nil
	}
	if generation.audioBytes == 0 {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Inworld TTS completed without returning audio", Retryable: true, Extensions: extension(raw)}
	}
	if len(generation.usage) > 0 && !s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: usageData(contextID, generation.utteranceID, generation.usage)}) {
		return s.ctx.Err()
	}
	if !s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: utteranceData(contextID, generation.utteranceID), Extensions: extension(raw)}) {
		return s.ctx.Err()
	}
	return nil
}

func (s *stream) handleContextClosed(contextID string, raw json.RawMessage) error {
	s.stateMu.Lock()
	if s.contextID == "" || (contextID != "" && contextID != s.contextID) {
		s.stateMu.Unlock()
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Inworld TTS closed an unknown context", Retryable: true, Extensions: extension(raw)}
	}
	unexpected := s.generation != nil && !s.generation.canceled
	if s.generation != nil {
		close(s.generation.done)
		s.generation = nil
	}
	close(s.contextDone)
	s.contextID, s.contextDone, s.contextClosing = "", nil, false
	s.stateMu.Unlock()
	if unexpected {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Inworld TTS closed the context before synthesis completed", Retryable: true, Extensions: extension(raw)}
	}
	return nil
}

func (s *stream) emit(event runtimepkg.ProviderEvent) bool {
	select {
	case s.events <- event:
		s.reportResponseProgress()
		return true
	case <-s.ctx.Done():
		return false
	}
}

func (s *stream) reportResponseProgress() {
	select {
	case s.responseProgress <- struct{}{}:
	default:
	}
}

func statusErrorCode(status int) string {
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return "authentication_failed"
	case status == http.StatusTooManyRequests:
		return "provider_rate_limited"
	case status == 0 || status >= 500:
		return "provider_unavailable"
	default:
		return "invalid_request"
	}
}

func streamError(status *rpcStatus, raw json.RawMessage) *runtimepkg.ProviderError {
	code, retryable := "invalid_request", false
	switch status.Code {
	case 7, 16:
		code = "authentication_failed"
	case 8:
		code, retryable = "provider_rate_limited", true
	case 2, 4, 13, 14:
		code, retryable = "provider_unavailable", true
	}
	message := "Inworld reported a synthesis error"
	if strings.TrimSpace(status.Message) != "" {
		message += ": " + status.Message
	}
	return &runtimepkg.ProviderError{Code: code, Message: message, Retryable: retryable, Extensions: extension(raw)}
}

func isNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}

func decodeAudio(value string) ([]byte, error) {
	if decoded, err := base64.StdEncoding.DecodeString(value); err == nil {
		return decoded, nil
	}
	return base64.RawStdEncoding.DecodeString(value)
}

func newUtteranceID() (string, error) { return randomID("utterance") }
func newContextID() (string, error)   { return randomID("context") }

func randomID(kind string) (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate Inworld TTS %s id: %w", kind, err)
	}
	return hex.EncodeToString(value), nil
}

func extension(raw json.RawMessage) map[string]json.RawMessage {
	return map[string]json.RawMessage{extensionID: append(json.RawMessage(nil), raw...)}
}

func utteranceData(contextID, utteranceID string) json.RawMessage {
	return marshalData(map[string]any{"context_id": contextID, "utterance_id": utteranceID})
}

func alignmentData(contextID, utteranceID string, timestamps json.RawMessage) json.RawMessage {
	return marshalData(map[string]any{"context_id": contextID, "utterance_id": utteranceID, "timestamp_info": timestamps})
}

func usageData(contextID, utteranceID string, usage json.RawMessage) json.RawMessage {
	return marshalData(map[string]any{"context_id": contextID, "utterance_id": utteranceID, "usage": usage})
}

func marshalData(value any) json.RawMessage {
	payload, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{"code":"internal"}`)
	}
	return payload
}

type socketAudioConfig struct {
	AudioEncoding   string `json:"audio_encoding"`
	SampleRateHertz int    `json:"sample_rate_hertz"`
}

type createContext struct {
	VoiceID     string            `json:"voice_id"`
	ModelID     string            `json:"model_id"`
	AudioConfig socketAudioConfig `json:"audio_config"`
	Language    string            `json:"language,omitempty"`
}

type createContextRequest struct {
	ContextID string        `json:"context_id"`
	Create    createContext `json:"create"`
}

type sendText struct {
	Text         string   `json:"text"`
	FlushContext struct{} `json:"flush_context"`
}

type sendTextRequest struct {
	ContextID string   `json:"context_id"`
	SendText  sendText `json:"send_text"`
}

type closeContextRequest struct {
	ContextID    string   `json:"context_id"`
	CloseContext struct{} `json:"close_context"`
}

type rpcStatus struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type socketAudioChunk struct {
	AudioContent  string          `json:"audioContent"`
	TimestampInfo json.RawMessage `json:"timestampInfo"`
	Usage         json.RawMessage `json:"usage"`
}

type socketResult struct {
	ContextID      string            `json:"contextId"`
	ContextIDSnake string            `json:"context_id"`
	ContextCreated json.RawMessage   `json:"contextCreated"`
	AudioChunk     *socketAudioChunk `json:"audioChunk"`
	FlushCompleted json.RawMessage   `json:"flushCompleted"`
	ContextClosed  json.RawMessage   `json:"contextClosed"`
	Status         *rpcStatus        `json:"status"`
	AudioContent   string            `json:"audioContent"`
	TimestampInfo  json.RawMessage   `json:"timestampInfo"`
	Usage          json.RawMessage   `json:"usage"`
}

type streamMessage struct {
	Result *socketResult `json:"result"`
	Error  *rpcStatus    `json:"error"`
	Done   bool          `json:"done"`
}
