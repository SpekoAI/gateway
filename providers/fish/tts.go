// Package fish implements Fish Audio's MessagePack WebSocket TTS protocol.
package fish

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	// TTSAdapterID is the stable Fish Audio streaming adapter identifier.
	TTSAdapterID = "fish.tts.v1"

	officialAPIHost        = "api.fish.audio"
	endpointPath           = "/v1/tts/live"
	extensionID            = "fish.audio/v1"
	defaultShutdownTimeout = 30 * time.Second
)

var supportedModels = map[string]struct{}{
	"s1":            {},
	"s2-pro":        {},
	"s2.1-pro":      {},
	"s2.1-pro-free": {},
}

var supportedSampleRates = map[int]struct{}{
	8_000: {}, 16_000: {}, 24_000: {}, 32_000: {}, 44_100: {},
}

type Config struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	ShutdownTimeout       time.Duration
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

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
		config.AdapterID = TTSAdapterID
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = 32
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = 1 << 20
	}
	if config.ShutdownTimeout == 0 {
		config.ShutdownTimeout = defaultShutdownTimeout
	}
	if config.EventBuffer < 1 || config.MaxMessageBytes < 1 || config.ShutdownTimeout < 0 {
		return nil, errors.New("fish tts buffers and shutdown timeout must be positive")
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
	if request.Kind != protocol.SessionKindTTS {
		return nil, fmt.Errorf("fish supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "fish" || request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, errors.New("fish tts requires a Fish Audio websocket route")
	}
	// Fish's short-lived session token belongs to its agent/LiveKit product and
	// is not accepted by /v1/tts/live. Fail closed if a control plane ever tries
	// to place a managed provider-direct credential on this adapter. BYOK and
	// Relay remain valid because each keeps the permanent API key server-side.
	if request.Plan.Execution.ProviderRoute == protocol.RouteProviderDirect && request.Plan.Execution.CredentialSource == protocol.CredentialsManaged {
		return nil, errors.New("fish tts does not support delegated provider-direct credentials")
	}
	if request.Media == nil {
		return nil, errors.New("fish tts requires media configuration")
	}
	if request.Media.Encoding != "pcm_s16le" || request.Media.Channels != 1 {
		return nil, errors.New("fish tts requires mono pcm_s16le output")
	}
	if _, ok := supportedSampleRates[request.Media.SampleRateHz]; !ok {
		return nil, fmt.Errorf("fish tts does not support sample rate %d", request.Media.SampleRateHz)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if _, ok := supportedModels[model]; !ok {
		return nil, fmt.Errorf("fish tts model %q is not supported", model)
	}
	endpoint, err := a.endpointPolicy.Parse(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("fish tts endpoint: %w", err)
	}
	if endpoint.Path != endpointPath {
		return nil, fmt.Errorf("fish tts endpoint path must be %s, got %q", endpointPath, endpoint.Path)
	}
	credential := request.Plan.Route.Credential
	if credential == nil || strings.TrimSpace(credential.Value) == "" || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) {
		return nil, errors.New("fish tts requires a bearer credential")
	}

	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &stream{
		ctx: streamCtx, cancel: cancel,
		events:     make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		httpClient: a.httpClient, endpoint: endpoint.String(), apiKey: credential.Value,
		model: model, voice: strings.TrimSpace(request.Options.Voice),
		sampleRate: request.Media.SampleRateHz, maxMessageBytes: a.maxMessageBytes,
		shutdownTimeout: a.shutdownTimeout,
	}
	if err := stream.startGeneration(ctx); err != nil {
		cancel()
		return nil, err
	}
	return stream, nil
}

func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

type generation struct {
	conn         *websocket.Conn
	done         chan struct{}
	committed    bool
	hasText      bool
	audioStarted bool
	cancelled    atomic.Bool
	doneOnce     sync.Once
}

type stream struct {
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent

	httpClient      *http.Client
	endpoint        string
	apiKey          string
	model           string
	voice           string
	sampleRate      int
	maxMessageBytes int64
	shutdownTimeout time.Duration

	stateMu sync.Mutex
	active  *generation
	closed  bool
	closing bool

	writeMu    sync.Mutex
	readers    sync.WaitGroup
	closeOnce  sync.Once
	abortOnce  sync.Once
	eventsOnce sync.Once
	closeErr   error
}

func (s *stream) Events() <-chan runtimepkg.ProviderEvent  { return s.events }
func (s *stream) WriteAudio(context.Context, []byte) error { return runtimepkg.ErrUnsupportedOperation }
func (s *stream) CommitAudio(context.Context) error        { return runtimepkg.ErrUnsupportedOperation }

func (s *stream) AppendText(ctx context.Context, text string) error {
	if strings.TrimSpace(text) == "" {
		return errors.New("fish tts text is empty")
	}
	generation, err := s.currentGeneration(ctx)
	if err != nil {
		return err
	}
	s.stateMu.Lock()
	if generation.committed {
		s.stateMu.Unlock()
		return errors.New("fish tts previous utterance has not completed")
	}
	generation.hasText = true
	s.stateMu.Unlock()
	return s.writeMessage(ctx, generation, clientEvent{Event: "text", Text: text})
}

func (s *stream) CommitText(ctx context.Context) error {
	s.stateMu.Lock()
	generation := s.active
	if s.closed || s.closing || generation == nil || generation.committed || !generation.hasText {
		s.stateMu.Unlock()
		return runtimepkg.ErrSessionClosed
	}
	generation.committed = true
	s.stateMu.Unlock()
	if err := s.writeMessage(ctx, generation, clientEvent{Event: "stop"}); err != nil {
		s.stateMu.Lock()
		generation.committed = false
		s.stateMu.Unlock()
		return err
	}
	return nil
}

func (s *stream) Cancel(context.Context) error {
	s.stateMu.Lock()
	generation := s.active
	if generation == nil {
		s.stateMu.Unlock()
		return runtimepkg.ErrSessionClosed
	}
	generation.cancelled.Store(true)
	s.stateMu.Unlock()
	err := generation.conn.CloseNow()
	s.completeGeneration(generation)
	return err
}

func (s *stream) Close(ctx context.Context) error {
	s.closeOnce.Do(func() {
		shutdownCtx, cancel := context.WithTimeout(ctx, s.shutdownTimeout)
		defer cancel()
		s.stateMu.Lock()
		s.closing = true
		generation := s.active
		needsStop := generation != nil && !generation.committed
		if needsStop {
			generation.committed = true
		}
		s.stateMu.Unlock()
		if needsStop {
			if err := s.writeMessage(shutdownCtx, generation, clientEvent{Event: "stop"}); err != nil {
				s.closeErr = err
				_ = generation.conn.CloseNow()
				s.completeGeneration(generation)
			}
		}
		if generation != nil {
			select {
			case <-generation.done:
			case <-shutdownCtx.Done():
				s.closeErr = shutdownCtx.Err()
				_ = generation.conn.CloseNow()
				s.completeGeneration(generation)
			}
		}
		s.stateMu.Lock()
		s.closed = true
		s.stateMu.Unlock()
		s.cancel()
		s.readers.Wait()
		s.closeEvents()
	})
	return s.closeErr
}

func (s *stream) Abort(context.Context) error {
	s.abortOnce.Do(func() {
		s.stateMu.Lock()
		s.closed = true
		generation := s.active
		if generation != nil {
			generation.cancelled.Store(true)
		}
		s.stateMu.Unlock()
		s.cancel()
		if generation != nil {
			if err := generation.conn.CloseNow(); err != nil {
				s.closeErr = err
			}
			s.completeGeneration(generation)
		}
		s.readers.Wait()
		s.closeEvents()
	})
	return s.closeErr
}

func (s *stream) currentGeneration(ctx context.Context) (*generation, error) {
	s.stateMu.Lock()
	if s.closed || s.closing {
		s.stateMu.Unlock()
		return nil, runtimepkg.ErrSessionClosed
	}
	if s.active != nil {
		generation := s.active
		s.stateMu.Unlock()
		return generation, nil
	}
	s.stateMu.Unlock()
	if err := s.startGeneration(ctx); err != nil {
		return nil, err
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.active, nil
}

func (s *stream) startGeneration(ctx context.Context) error {
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer "+s.apiKey)
	headers.Set("model", s.model)
	conn, response, err := websocket.Dial(ctx, s.endpoint, &websocket.DialOptions{HTTPClient: configHTTPClient(s.httpClient), HTTPHeader: headers})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		return &runtimepkg.ProviderError{
			Code: fishDialErrorCode(status), Message: "Fish Audio streaming connection could not be established",
			Retryable:      status == 0 || status == http.StatusTooManyRequests || status >= 500,
			ProviderStatus: status, Cause: err,
		}
	}
	conn.SetReadLimit(s.maxMessageBytes)
	generation := &generation{conn: conn, done: make(chan struct{})}
	s.stateMu.Lock()
	if s.closed || s.closing || s.active != nil {
		s.stateMu.Unlock()
		_ = conn.CloseNow()
		return runtimepkg.ErrSessionClosed
	}
	s.active = generation
	s.stateMu.Unlock()
	start := clientEvent{Event: "start", Request: &startRequest{
		Text: "", Format: "pcm", SampleRate: s.sampleRate, ReferenceID: s.voice,
	}}
	if err := s.writeMessage(ctx, generation, start); err != nil {
		_ = conn.CloseNow()
		s.completeGeneration(generation)
		return err
	}
	s.readers.Add(1)
	go s.readLoop(generation)
	return nil
}

func (s *stream) writeMessage(ctx context.Context, generation *generation, event clientEvent) error {
	payload, err := msgpack.Marshal(event)
	if err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := generation.conn.Write(ctx, websocket.MessageBinary, payload); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Fish Audio streaming write failed", Retryable: true, Cause: err}
	}
	return nil
}

func (s *stream) readLoop(generation *generation) {
	defer s.readers.Done()
	for {
		messageType, payload, err := generation.conn.Read(s.ctx)
		if err != nil {
			s.completeGeneration(generation)
			if generation.cancelled.Load() || s.isClosing() || isNormalClose(err) {
				return
			}
			_ = s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Fish Audio streaming read failed", Retryable: true, Cause: err}})
			s.terminalClose()
			return
		}
		if messageType != websocket.MessageBinary {
			continue
		}
		var event serverEvent
		if err := msgpack.Unmarshal(payload, &event); err != nil {
			_ = s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Fish Audio sent malformed MessagePack", Retryable: true, Cause: err}})
			s.completeGeneration(generation)
			s.terminalClose()
			return
		}
		switch event.Event {
		case "audio":
			if len(event.Audio) == 0 {
				continue
			}
			if s.markAudioStarted(generation) {
				if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Extensions: fishExtension(event)}); err != nil {
					return
				}
			}
			if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Audio: append([]byte(nil), event.Audio...), Extensions: fishExtension(event)}); err != nil {
				return
			}
		case "finish":
			_ = generation.conn.CloseNow()
			if event.Reason == "error" {
				_ = s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: fishErrorMessage(event.Message), Retryable: false}})
				s.terminalClose()
				s.completeGeneration(generation)
				return
			}
			// Keep this generation active until its terminal event is enqueued.
			// Otherwise a concurrent AppendText can expose the next generation's
			// audio.started ahead of this utterance's audio.done.
			_ = s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Extensions: fishExtension(event)})
			s.completeGeneration(generation)
			return
		default:
			// Fish explicitly asks clients to ignore unknown server events for
			// forward compatibility.
		}
	}
}

func (s *stream) markAudioStarted(generation *generation) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.active != generation || generation.audioStarted {
		return false
	}
	generation.audioStarted = true
	return true
}

func (s *stream) completeGeneration(generation *generation) {
	s.stateMu.Lock()
	if s.active == generation {
		s.active = nil
	}
	s.stateMu.Unlock()
	generation.doneOnce.Do(func() { close(generation.done) })
}

func (s *stream) isClosing() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.closed || s.closing
}

func (s *stream) terminalClose() {
	s.stateMu.Lock()
	s.closed = true
	s.stateMu.Unlock()
	s.cancel()
	s.closeEvents()
}

func (s *stream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *stream) closeEvents() {
	s.eventsOnce.Do(func() { close(s.events) })
}

type clientEvent struct {
	Event   string        `msgpack:"event"`
	Request *startRequest `msgpack:"request,omitempty"`
	Text    string        `msgpack:"text,omitempty"`
}

type startRequest struct {
	Text        string `msgpack:"text"`
	Format      string `msgpack:"format"`
	SampleRate  int    `msgpack:"sample_rate"`
	ReferenceID string `msgpack:"reference_id,omitempty"`
}

type serverEvent struct {
	Event   string `msgpack:"event" json:"event"`
	Audio   []byte `msgpack:"audio" json:"-"`
	Reason  string `msgpack:"reason" json:"reason,omitempty"`
	Message string `msgpack:"message" json:"message,omitempty"`
}

func fishExtension(event serverEvent) map[string]json.RawMessage {
	payload, _ := json.Marshal(event)
	return map[string]json.RawMessage{extensionID: payload}
}

func fishDialErrorCode(status int) string {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return "authentication_failed"
	case http.StatusTooManyRequests:
		return "provider_rate_limited"
	default:
		return "provider_unavailable"
	}
}

func fishErrorMessage(message string) string {
	if strings.TrimSpace(message) == "" {
		return "Fish Audio reported a streaming error"
	}
	return "Fish Audio reported a streaming error: " + message
}

func configHTTPClient(client *http.Client) *http.Client {
	if client != nil {
		return client
	}
	return http.DefaultClient
}

func isNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}
