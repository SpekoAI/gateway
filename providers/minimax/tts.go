package minimax

import (
	"context"
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
	// AdapterID is the identifier returned by a MiniMax TTS session plan.
	AdapterID = "minimax.tts.v1"
	// extensionID namespaces the raw MiniMax payload retained on events that
	// carry vendor detail the canonical protocol does not model.
	extensionID = "minimax.io/t2a_v2"

	// DefaultModel is the current MiniMax flagship and the model id the control
	// plane should place in a plan when a caller does not pin one. The adapter
	// itself never defaults: a plan must always name a concrete model.
	DefaultModel = "speech-2.8-hd"

	// officialAPIHost is MiniMax's international API host. The US-West host
	// (api-uw.minimax.io) and the mainland-China host are reachable only by
	// adding them to Config.AllowedEndpointHosts, so a plan cannot silently
	// retarget a customer credential at an unexpected host.
	officialAPIHost = "api.minimax.io"
	// endpointPath is the documented T2A v2 WebSocket path. It is deliberately
	// distinct from the HTTP API's /v1/t2a_v2.
	endpointPath = "/ws/v1/t2a_v2"

	// minimaxTTSMaxCharacters mirrors the documented per-request ceiling: T2A
	// requires text to be *less than* 10,000 characters, so 9,999 is the last
	// accepted length. It is applied per task, across all appended fragments.
	minimaxTTSMaxCharacters = 9_999

	defaultHandshakeTimeout = 10 * time.Second
)

// MiniMax names every frame with an `event` field. These are the documented
// values for the task lifecycle: connect, start, stream text, finish.
const (
	eventConnectedSuccess = "connected_success"
	eventTaskStart        = "task_start"
	eventTaskStarted      = "task_started"
	eventTaskContinue     = "task_continue"
	eventTaskContinued    = "task_continued"
	eventTaskFinish       = "task_finish"
	eventTaskFinished     = "task_finished"
	eventTaskFailed       = "task_failed"
)

// maxHandshakeMessages bounds how many frames the opening handshake will read
// before giving up, so a chatty or hostile upstream cannot stall Open forever.
const maxHandshakeMessages = 8

// supportedModels is the documented T2A model lineup. Validating against it
// turns a stale control-plane pin into a local error instead of a provider
// round trip that bills the customer for a rejected request.
var supportedModels = map[string]struct{}{
	"speech-2.8-hd":    {},
	"speech-2.8-turbo": {},
	"speech-2.6-hd":    {},
	"speech-2.6-turbo": {},
	"speech-02-hd":     {},
	"speech-02-turbo":  {},
	"speech-01-hd":     {},
	"speech-01-turbo":  {},
}

// supportedSampleRates is the fixed set audio_setting accepts. The canonical
// MediaFormat allows 8000-192000, so the adapter has to narrow it.
var supportedSampleRates = map[int]struct{}{
	8_000: {}, 16_000: {}, 22_050: {}, 24_000: {}, 32_000: {}, 44_100: {},
}

// Config controls local transport limits and endpoint policy. Provider
// identity, model, voice, and credential all come from the signed session plan.
type Config struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	HandshakeTimeout      time.Duration
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// Adapter implements MiniMax's /ws/v1/t2a_v2 streaming API.
type Adapter struct {
	id               string
	httpClient       *http.Client
	eventBuffer      int
	maxMessageBytes  int64
	handshakeTimeout time.Duration
	endpointPolicy   upstream.WebSocketPolicy
}

// New creates a bounded MiniMax TTS adapter.
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
	if config.HandshakeTimeout == 0 {
		config.HandshakeTimeout = defaultHandshakeTimeout
	}
	if config.EventBuffer < 1 {
		return nil, errors.New("minimax event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("minimax maximum message bytes must be positive")
	}
	if config.HandshakeTimeout < 0 {
		return nil, errors.New("minimax handshake timeout must not be negative")
	}
	endpointPolicy, err := upstream.NewWebSocketPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &Adapter{
		id:               config.AdapterID,
		httpClient:       config.HTTPClient,
		eventBuffer:      config.EventBuffer,
		maxMessageBytes:  config.MaxMessageBytes,
		handshakeTimeout: config.HandshakeTimeout,
		endpointPolicy:   endpointPolicy,
	}, nil
}

func (a *Adapter) ID() string { return a.id }

// Open dials MiniMax and completes the documented opening handshake: await
// connected_success, send task_start, await task_started. Doing it here rather
// than lazily means a rejected model, voice, or credential fails Open instead
// of arriving later as a terminal event mid-utterance.
func (a *Adapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindTTS {
		return nil, fmt.Errorf("minimax supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "minimax" {
		return nil, fmt.Errorf("minimax adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("minimax requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("minimax requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("minimax media: %w", err)
	}
	if err := validateGenerationOptions(request.Plan.Route.Model, request.Options.Voice, *request.Media); err != nil {
		return nil, err
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("minimax requires a bearer credential")
	}
	apiKey, err := parseCredential(credential.Value)
	if err != nil {
		return nil, err
	}
	endpoint, err := t2aEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}

	// One credential channel, deliberately, and it is NOT an oversight. Cartesia
	// and ElevenLabs split BYOK into a header and a managed token into a query
	// parameter because those vendors mint short-lived tokens that are accepted
	// only that way. MiniMax documents no ephemeral credential at all: every
	// interface (HTTP, WebSocket, realtime) authenticates with the same bearer
	// key in the Authorization header, and there is no query-parameter auth on
	// this endpoint. A managed route therefore carries a control-plane-delegated
	// value through the identical header, and a relay plan carries the
	// connector's permanent key through it too — the routes differ only in
	// which credential kinds they accept, never in placement. Putting a token
	// in the query string here would silently fail the handshake, so every
	// source shares this path until MiniMax ships a token mint.
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer "+apiKey)

	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{
		HTTPClient: httpClient(a.httpClient),
		HTTPHeader: headers,
	})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		return nil, &runtimepkg.ProviderError{
			Code:           dialErrorCode(status),
			Message:        "MiniMax streaming connection could not be established",
			Retryable:      status == 0 || status == http.StatusTooManyRequests || status >= 500,
			ProviderStatus: status,
			Cause:          err,
		}
	}
	conn.SetReadLimit(a.maxMessageBytes)

	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &stream{
		conn:    conn,
		ctx:     streamCtx,
		cancel:  cancel,
		events:  make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		model:   strings.TrimSpace(request.Plan.Route.Model),
		voice:   strings.TrimSpace(request.Options.Voice),
		media:   *request.Media,
		taskAck: make(chan struct{}),
	}

	handshakeCtx := ctx
	if a.handshakeTimeout > 0 {
		var cancelHandshake context.CancelFunc
		handshakeCtx, cancelHandshake = context.WithTimeout(ctx, a.handshakeTimeout)
		defer cancelHandshake()
	}
	if err := stream.handshake(handshakeCtx); err != nil {
		_ = stream.abort()
		return nil, err
	}
	go stream.readLoop()
	return stream, nil
}

func httpClient(client *http.Client) *http.Client {
	if client != nil {
		return client
	}
	return http.DefaultClient
}

// t2aEndpoint pins the handshake URL. No query parameters are ever added: the
// current T2A references (both WebSocket and HTTP) contain no GroupId or any
// other tenant selector, and authentication is entirely in the Authorization
// header. Older third-party integrations append ?GroupId= against the legacy
// China host; sending an undocumented parameter on this handshake is exactly
// the kind of unverified detail that fails silently, so it is not sent.
func t2aEndpoint(policy upstream.WebSocketPolicy, rawEndpoint string) (string, error) {
	endpoint, err := policy.Parse(rawEndpoint)
	if err != nil {
		return "", fmt.Errorf("minimax tts endpoint: %w", err)
	}
	if endpoint.Path != endpointPath {
		return "", fmt.Errorf("minimax tts endpoint path must be %s, got %q", endpointPath, endpoint.Path)
	}
	return endpoint.String(), nil
}

func validateGenerationOptions(model, voice string, media protocol.MediaFormat) error {
	model = strings.TrimSpace(model)
	if model == "" || model == "auto" {
		return errors.New("minimax requires a concrete model in the session plan")
	}
	if _, ok := supportedModels[model]; !ok {
		return fmt.Errorf("minimax does not support model %q", model)
	}
	if strings.TrimSpace(voice) == "" {
		return errors.New("minimax requires a voice id in request options")
	}
	// audio_setting.format is "pcm", which MiniMax documents as raw signed
	// 16-bit little-endian samples — the gateway's pcm_s16le. No other canonical
	// encoding maps onto a raw MiniMax format.
	if media.Encoding != "pcm_s16le" {
		return fmt.Errorf("minimax streaming output requires pcm_s16le, got %q", media.Encoding)
	}
	if media.Channels != 1 && media.Channels != 2 {
		return fmt.Errorf("minimax supports 1 or 2 channels, got %d", media.Channels)
	}
	if _, ok := supportedSampleRates[media.SampleRateHz]; !ok {
		return fmt.Errorf("minimax does not support sample rate %d", media.SampleRateHz)
	}
	return nil
}

// parseCredential resolves the bare MiniMax bearer key.
func parseCredential(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", errors.New("minimax credential is empty")
	}
	return trimmed, nil
}

// acceptableCredentialKind reports whether a delegated credential's kind may
// authenticate the plan's route. Bearer is the norm everywhere; the relay arm
// additionally accepts relay_access, because protocol.SessionPlan validation
// requires relay plans to label their credential relay_access while a relay
// connector that synthesizes the plan and drives this adapter directly — no
// Engine, no SessionPlan.Validate — labels the same permanent MiniMax key
// bearer. Both spellings carry a permanent key destined for the identical
// Authorization header, so nothing else on the relay arm changes.
func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

type stream struct {
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent

	model string
	voice string
	media protocol.MediaFormat

	writeMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	closing      atomic.Bool
	closeErr     error

	stateMu      sync.Mutex
	taskActive   bool
	taskCommit   bool
	taskCanceled bool
	audioStarted bool
	characters   int
	traceID      string
	// taskAck is closed when the current task's task_finished arrives, letting
	// Close flush the tail of an utterance before tearing the socket down.
	taskAck chan struct{}
}

func (s *stream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

// WriteAudio and CommitAudio are inbound-audio operations. A TTS session never
// carries them, so the adapter reports them as unsupported rather than silently
// discarding a caller's buffer.
func (s *stream) WriteAudio(context.Context, []byte) error { return runtimepkg.ErrUnsupportedOperation }

func (s *stream) CommitAudio(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// handshake performs the documented open sequence synchronously, before the
// read loop takes ownership of the socket.
func (s *stream) handshake(ctx context.Context) error {
	if _, err := s.awaitEvent(ctx, eventConnectedSuccess); err != nil {
		return err
	}
	if err := s.writeJSON(ctx, s.taskStart()); err != nil {
		return err
	}
	if _, err := s.awaitEvent(ctx, eventTaskStarted); err != nil {
		return err
	}
	s.stateMu.Lock()
	s.taskActive = true
	s.stateMu.Unlock()
	return nil
}

// awaitEvent reads until the wanted event arrives, failing fast on task_failed
// or any frame carrying a non-zero base_resp.
func (s *stream) awaitEvent(ctx context.Context, want string) (inbound, error) {
	for attempt := 0; attempt < maxHandshakeMessages; attempt++ {
		messageType, payload, err := s.conn.Read(ctx)
		if err != nil {
			return inbound{}, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "MiniMax handshake read failed", Retryable: true, Cause: err}
		}
		if messageType != websocket.MessageText {
			continue
		}
		var message inbound
		if err := json.Unmarshal(payload, &message); err != nil {
			return inbound{}, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "MiniMax sent malformed handshake JSON", Retryable: true, Cause: err}
		}
		// Copy before retaining: the error keeps the payload as an extension.
		if err := message.err(json.RawMessage(append([]byte(nil), payload...))); err != nil {
			return inbound{}, err
		}
		if message.Event == want {
			return message, nil
		}
	}
	return inbound{}, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "MiniMax did not complete the " + want + " handshake", Retryable: true}
}

// AppendText streams one text fragment into the live task. MiniMax synthesizes
// incrementally per task_continue, so no local buffering is needed and audio
// starts flowing before the utterance is complete.
func (s *stream) AppendText(ctx context.Context, text string) error {
	if strings.TrimSpace(text) == "" {
		return errors.New("minimax tts text is empty")
	}
	characters := utf8.RuneCountInString(text)
	if err := s.acceptCharacters(characters); err != nil {
		return err
	}
	if err := s.ensureTask(ctx); err != nil {
		return err
	}
	return s.writeJSON(ctx, taskContinue{Event: eventTaskContinue, Text: text})
}

// ensureTask restarts a task after a previous one finished, so one socket can
// serve several utterances. The first task is started synchronously by
// handshake; a restart is fire-and-forget because the read loop now owns reads
// and will absorb the task_started acknowledgement.
func (s *stream) ensureTask(ctx context.Context) error {
	s.stateMu.Lock()
	if s.closed.Load() || s.closing.Load() {
		s.stateMu.Unlock()
		return runtimepkg.ErrSessionClosed
	}
	if s.taskActive {
		if s.taskCommit {
			s.stateMu.Unlock()
			return errors.New("minimax tts previous utterance has not completed")
		}
		s.stateMu.Unlock()
		return nil
	}
	s.taskActive = true
	s.taskCommit = false
	s.taskCanceled = false
	s.audioStarted = false
	s.taskAck = make(chan struct{})
	s.stateMu.Unlock()
	return s.writeJSON(ctx, s.taskStart())
}

func (s *stream) acceptCharacters(characters int) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	// Reject locally rather than letting MiniMax bill and then reject the task.
	if s.characters+characters > minimaxTTSMaxCharacters {
		return &runtimepkg.ProviderError{
			Code:           "input_too_large",
			Message:        "MiniMax TTS input exceeds 10000 characters",
			Retryable:      false,
			ProviderStatus: http.StatusRequestEntityTooLarge,
		}
	}
	s.characters += characters
	return nil
}

// CommitText closes the utterance with task_finish. MiniMax replies with
// task_finished once the tail of the audio has been sent.
func (s *stream) CommitText(ctx context.Context) error {
	if err := s.commitTask(); err != nil {
		return err
	}
	if err := s.writeJSON(ctx, map[string]string{"event": eventTaskFinish}); err != nil {
		s.uncommitTask()
		return err
	}
	return nil
}

// Cancel implements barge-in. MiniMax documents no interrupt or clear frame for
// T2A, so the only honest cancellation is two-sided: tell the provider the task
// is over with task_finish, and drop every remaining frame of that task locally
// so already-generated audio never reaches the caller. No audio.done is emitted
// for a cancelled utterance.
func (s *stream) Cancel(ctx context.Context) error {
	s.stateMu.Lock()
	if !s.taskActive {
		s.stateMu.Unlock()
		return runtimepkg.ErrSessionClosed
	}
	s.taskCanceled = true
	alreadyCommitted := s.taskCommit
	s.taskCommit = true
	s.stateMu.Unlock()
	if alreadyCommitted {
		// task_finish was already sent; the local drop is the whole cancel.
		return nil
	}
	return s.writeJSON(ctx, map[string]string{"event": eventTaskFinish})
}

// Close finishes any open task and waits for its acknowledgement so a caller
// that closes right after CommitText still receives the trailing audio.
func (s *stream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		s.closing.Store(true)
		if ack := s.pendingAck(); ack != nil {
			select {
			case <-ack:
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
func (s *stream) Abort(context.Context) error { return s.abort() }

func (s *stream) abort() error {
	s.abortOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		if err := s.conn.CloseNow(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
		s.finishTask()
	})
	return s.closeErr
}

func (s *stream) commitTask() error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() || s.closing.Load() || !s.taskActive || s.taskCommit {
		return runtimepkg.ErrSessionClosed
	}
	s.taskCommit = true
	return nil
}

func (s *stream) uncommitTask() {
	s.stateMu.Lock()
	if s.taskActive {
		s.taskCommit = false
	}
	s.stateMu.Unlock()
}

// finishTask ends the current task and releases anyone waiting in Close.
func (s *stream) finishTask() (canceled bool) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	canceled = s.taskCanceled
	if !s.taskActive {
		return canceled
	}
	s.taskActive = false
	s.taskCommit = false
	s.taskCanceled = false
	s.audioStarted = false
	s.characters = 0
	if s.taskAck != nil {
		select {
		case <-s.taskAck:
		default:
			close(s.taskAck)
		}
	}
	return canceled
}

// pendingAck reports the acknowledgement Close should wait for. It waits only
// on a task that has actually been finished by the caller: an open task nobody
// committed will never produce task_finished, so waiting on it would stall
// Close until its context expired.
func (s *stream) pendingAck() <-chan struct{} {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if !s.taskActive || !s.taskCommit {
		return nil
	}
	return s.taskAck
}

// markAudioStarted reports whether this is the first audio of the task, and
// whether the task was cancelled and its audio must be dropped.
func (s *stream) markAudioStarted() (first bool, canceled bool) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.taskCanceled {
		return false, true
	}
	if s.audioStarted {
		return false, false
	}
	s.audioStarted = true
	return true, false
}

func (s *stream) taskStart() taskStart {
	return taskStart{
		Event: eventTaskStart,
		Model: s.model,
		VoiceSetting: voiceSetting{
			VoiceID: s.voice,
			Speed:   1.0,
			Vol:     1.0,
			Pitch:   0,
		},
		AudioSetting: audioSetting{
			SampleRate: s.media.SampleRateHz,
			Format:     "pcm",
			Channel:    s.media.Channels,
		},
	}
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
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "MiniMax streaming write failed", Retryable: true, Cause: err}
	}
	return nil
}

func (s *stream) readLoop() {
	defer func() {
		s.cancel()
		s.finishTask()
		close(s.events)
	}()
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && s.ctx.Err() == nil && !isNormalClose(err) {
				s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "MiniMax streaming read failed", Retryable: true, Cause: err}})
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

func (s *stream) handleMessage(payload []byte) error {
	var message inbound
	if err := json.Unmarshal(payload, &message); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "MiniMax sent malformed streaming JSON", Retryable: true, Cause: err}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))

	if message.TraceID != "" && s.setTraceID(message.TraceID) {
		if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: usageData(message.TraceID)}); err != nil {
			return err
		}
	}
	// A non-zero base_resp is terminal wherever it appears, including on a frame
	// that is otherwise shaped like a normal task event.
	if err := message.err(raw); err != nil {
		return err
	}

	switch message.Event {
	case eventTaskContinued:
		return s.handleAudio(message, raw)
	case eventTaskFinished:
		canceled := s.finishTask()
		if canceled {
			// A cancelled utterance is not a completed one; emitting audio.done
			// would tell the caller a barge-in actually played to the end.
			return nil
		}
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: s.contextData(), Extensions: extension(raw)})
	case eventTaskStarted, eventConnectedSuccess:
		// Acknowledgements for a restarted task carry no caller-visible state.
		return nil
	default:
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: s.warningData(message.Event), Extensions: extension(raw)})
	}
}

// handleAudio forwards one synthesized chunk.
//
// Note for anyone porting from the HTTP API: that one needs
// stream_options.exclude_aggregated_audio, because its terminal SSE chunk
// repeats the COMPLETE clip and forwarding it ships every utterance doubled.
// The WebSocket API has no such chunk — task_continued carries no status field
// and task_finished carries no audio at all — so no de-duplication is needed
// here, and adding a speculative guard would only imply a frame shape MiniMax
// never sends.
func (s *stream) handleAudio(message inbound, raw json.RawMessage) error {
	// data is documented as nullable, which decodes to an empty audio string.
	if message.Data.Audio == "" {
		return nil
	}
	audio, err := decodeHexAudio(message.Data.Audio)
	if err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "MiniMax sent invalid audio data", Retryable: true, Cause: err}
	}
	if len(audio) == 0 {
		return nil
	}
	first, canceled := s.markAudioStarted()
	if canceled {
		return nil
	}
	if first {
		if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: s.contextData()}); err != nil {
			return err
		}
	}
	return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Data: s.contextData(), Extensions: extension(raw), Audio: audio})
}

func (s *stream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *stream) setTraceID(value string) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.traceID == value {
		return false
	}
	s.traceID = value
	return true
}

func (s *stream) currentTraceID() string {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.traceID
}

func (s *stream) contextData() json.RawMessage {
	return marshalData(map[string]any{"provider_request_id": s.currentTraceID()})
}

func (s *stream) warningData(event string) json.RawMessage {
	return marshalData(map[string]any{"provider_type": event, "provider_request_id": s.currentTraceID()})
}

// decodeHexAudio converts a MiniMax audio chunk into raw PCM. The wire value is
// unseparated hex; an odd trailing nibble cannot form a byte and is dropped
// rather than failing the whole utterance.
func decodeHexAudio(encoded string) ([]byte, error) {
	if len(encoded)%2 == 1 {
		encoded = encoded[:len(encoded)-1]
	}
	return hex.DecodeString(encoded)
}

func isNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}

// dialErrorCode maps a handshake HTTP status onto the canonical taxonomy.
func dialErrorCode(status int) string {
	switch {
	case status == http.StatusUnauthorized, status == http.StatusForbidden:
		return "authentication_failed"
	case status == http.StatusTooManyRequests:
		return "provider_rate_limited"
	case status == 0 || status >= 500:
		return "provider_unavailable"
	default:
		return "invalid_request"
	}
}

// baseRespError maps MiniMax's in-band status_code onto the same taxonomy. The
// vendor reports failures this way even on a successful transport frame, so
// these codes are the only signal for a bad voice id, an exhausted quota, or a
// revoked key once the socket is up.
func baseRespError(status int, message string, raw json.RawMessage) *runtimepkg.ProviderError {
	code := "invalid_request"
	retryable := false
	switch status {
	case 1004, 2049: // not authorized / invalid API key
		code = "authentication_failed"
	case 1002, 1039, 1041, 2045: // rate limit / token limit / conn limit / rate growth limit
		code, retryable = "provider_rate_limited", true
	case 1000, 1001, 1024, 1033: // unknown / timeout / internal / system error
		code, retryable = "provider_unavailable", true
	}
	if strings.TrimSpace(message) == "" {
		message = "unknown"
	}
	return &runtimepkg.ProviderError{
		Code:       code,
		Message:    fmt.Sprintf("MiniMax reported a TTS error %d: %s", status, message),
		Retryable:  retryable,
		Extensions: extension(raw),
	}
}

func extension(raw json.RawMessage) map[string]json.RawMessage {
	return map[string]json.RawMessage{extensionID: raw}
}

func usageData(traceID string) json.RawMessage {
	return marshalData(map[string]any{"provider_request_id": traceID})
}

func marshalData(value any) json.RawMessage {
	payload, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{"code":"internal"}`)
	}
	return payload
}

type voiceSetting struct {
	VoiceID string  `json:"voice_id"`
	Speed   float64 `json:"speed"`
	Vol     float64 `json:"vol"`
	Pitch   int     `json:"pitch"`
}

type audioSetting struct {
	SampleRate int    `json:"sample_rate"`
	Format     string `json:"format"`
	Channel    int    `json:"channel"`
}

type taskStart struct {
	Event        string       `json:"event"`
	Model        string       `json:"model"`
	VoiceSetting voiceSetting `json:"voice_setting"`
	AudioSetting audioSetting `json:"audio_setting"`
}

type taskContinue struct {
	Event string `json:"event"`
	Text  string `json:"text"`
}

type baseResp struct {
	StatusCode int    `json:"status_code"`
	StatusMsg  string `json:"status_msg"`
}

// inbound mirrors the documented server frames. Data is nullable, so it is a
// value type: a JSON null decodes to the zero value rather than panicking.
type inbound struct {
	Event     string `json:"event"`
	TraceID   string `json:"trace_id"`
	SessionID string `json:"session_id"`
	IsFinal   bool   `json:"is_final"`
	Data      struct {
		Audio string `json:"audio"`
	} `json:"data"`
	BaseResp *baseResp `json:"base_resp"`
}

// err reports the terminal provider error this frame represents, if any. A
// task_failed almost always carries a base_resp explaining why, so that branch
// is preferred; the bare fallback exists so a task_failed with no detail still
// terminates the attempt instead of being reported as an unknown event.
func (m inbound) err(raw json.RawMessage) *runtimepkg.ProviderError {
	if m.BaseResp != nil && m.BaseResp.StatusCode != 0 {
		return baseRespError(m.BaseResp.StatusCode, m.BaseResp.StatusMsg, raw)
	}
	if m.Event == eventTaskFailed {
		return &runtimepkg.ProviderError{
			Code:       "invalid_request",
			Message:    "MiniMax reported a TTS task failure",
			Retryable:  false,
			Extensions: extension(raw),
		}
	}
	return nil
}
