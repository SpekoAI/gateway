package omi

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/metering"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

const (
	// STTAdapterID is the identifier the connector registers this adapter under.
	STTAdapterID = "omi.stt.v1"
	// RealtimeEndpoint is the US realtime socket, the catalog's endpoint.
	RealtimeEndpoint = "wss://api.us.omi.health/v1/stream"
	// SampleRateHz is the only rate the socket accepts.
	SampleRateHz = 16_000

	streamPath = "/v1/stream"

	// frameMillis sizes each binary frame at 100 ms, whatever size the
	// runtime writes.
	frameMillis = 100

	// silenceMin and silenceMax bound endpointing.silence_ms.
	silenceMin = 200
	silenceMax = 1100

	defaultSTTEventBuffer  = 64
	defaultMaxMessageBytes = 1 << 20
	// defaultSetupTimeout bounds the wait for ready after the start frame.
	defaultSetupTimeout = 12 * time.Second
	// defaultCloseDrainTimeout bounds how long Close waits for the
	// canonical_record that follows stop.
	defaultCloseDrainTimeout = 10 * time.Second
	// defaultKeepAliveInterval is the input silence after which a
	// keep_alive goes out. Omi's idle timeout is not documented; this holds
	// a paused caller well inside any plausible one.
	defaultKeepAliveInterval = 5 * time.Second
	// sessionCapSlack is how close to max_session_s a server close counts as
	// the session cap rather than a failure.
	sessionCapSlack = 2 * time.Second
	// handshakeErrorBytes is all coder/websocket keeps of a refused
	// handshake's body.
	handshakeErrorBytes = 1 << 10
)

var (
	stopFrame      = []byte(`{"type":"stop"}`)
	keepAliveFrame = []byte(`{"type":"keep_alive"}`)
	// redactedKey replaces the key wherever a vendor frame echoes it.
	redactedKey = []byte("[redacted]")
)

// quietFrames are hints the adapter reads past without an event: turn
// boundary guesses, the stop sequence before canonical_record, control
// acknowledgements and notices such as UTTERANCE_DROPPED.
var quietFrames = map[string]struct{}{
	"turn_start": {}, "turn_end_candidate": {}, "turn_continuation": {},
	"confirmed_complete": {}, "finalizing": {}, "canonical_revision": {},
	"finalize_ack": {}, "keep_alive_ack": {}, "configuration_updated": {},
	"notice": {}, "flush_ack": {},
}

// STTConfig controls local transport bounds. Provider identity, model,
// credential and endpoint always come from the verified plan.
type STTConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
	// SetupTimeout bounds the wait for ready.
	SetupTimeout time.Duration
	// CloseDrainTimeout bounds how long Close waits for canonical_record.
	CloseDrainTimeout time.Duration
	// KeepAliveInterval is the input silence after which a keep_alive is sent.
	KeepAliveInterval time.Duration
}

// STTAdapter opens Omi realtime transcription sessions.
type STTAdapter struct {
	id                string
	httpClient        *http.Client
	eventBuffer       int
	maxMessageBytes   int64
	setupTimeout      time.Duration
	closeDrainTimeout time.Duration
	keepAliveInterval time.Duration
	endpointPolicy    upstream.WebSocketPolicy
}

// NewSTT validates the configuration and builds the adapter.
func NewSTT(config STTConfig) (*STTAdapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = STTAdapterID
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = defaultSTTEventBuffer
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = defaultMaxMessageBytes
	}
	if config.SetupTimeout == 0 {
		config.SetupTimeout = defaultSetupTimeout
	}
	if config.CloseDrainTimeout == 0 {
		config.CloseDrainTimeout = defaultCloseDrainTimeout
	}
	if config.KeepAliveInterval == 0 {
		config.KeepAliveInterval = defaultKeepAliveInterval
	}
	if config.EventBuffer < 1 || config.MaxMessageBytes < 1 || config.SetupTimeout <= 0 || config.CloseDrainTimeout <= 0 || config.KeepAliveInterval <= 0 {
		return nil, errors.New("omi stt: event buffer, message bound, timeouts and keepalive interval must be positive")
	}
	policy, err := upstream.NewWebSocketPolicy(officialHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &STTAdapter{
		id: config.AdapterID, httpClient: noRedirects(config.HTTPClient), eventBuffer: config.EventBuffer,
		maxMessageBytes: config.MaxMessageBytes, setupTimeout: config.SetupTimeout,
		closeDrainTimeout: config.CloseDrainTimeout, keepAliveInterval: config.KeepAliveInterval,
		endpointPolicy: policy,
	}, nil
}

// ID returns the adapter identifier.
func (a *STTAdapter) ID() string { return a.id }

// startFrame is the socket's first text frame. It holds the key, so it is
// marshalled once onto the wire and nowhere else; String and GoString keep
// an accidental %v from printing it.
type startFrame struct {
	Type          string           `json:"type"`
	APIKey        string           `json:"api_key"`
	SessionID     string           `json:"session_id"`
	Model         string           `json:"model"`
	StreamingMode string           `json:"streaming_mode"`
	EventSchema   string           `json:"event_schema"`
	AudioFormat   string           `json:"audio_format"`
	SampleRateHz  int              `json:"sample_rate_hz"`
	Channels      int              `json:"channels"`
	Language      string           `json:"language"`
	Diarization   startDiarization `json:"diarization"`
	Endpointing   *startEndpoint   `json:"endpointing,omitempty"`
}

type startDiarization struct {
	Enabled bool `json:"enabled"`
}

type startEndpoint struct {
	SilenceMS int `json:"silence_ms"`
}

func (startFrame) String() string   { return "omi start frame (redacted)" }
func (startFrame) GoString() string { return "omi start frame (redacted)" }

// Open dials the socket, sends the start frame, and returns once ready has
// arrived, so a refused key, language or rate fails Open where the runtime
// can fail over rather than on the first WriteAudio.
func (a *STTAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindSTT {
		return nil, fmt.Errorf("omi stt supports stt sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != ProviderName {
		return nil, fmt.Errorf("omi stt adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("omi stt requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model != Model {
		return nil, fmt.Errorf("omi stt does not support model %q", model)
	}
	language, err := realtimeLanguage(request.Options.Language)
	if err != nil {
		return nil, err
	}
	if err := realtimeOptions(request.Options.STT); err != nil {
		return nil, err
	}
	silence, err := silenceMS(request.Options.STT)
	if err != nil {
		return nil, err
	}
	if request.Media == nil {
		return nil, errors.New("omi stt requires input media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("omi stt input media: %w", err)
	}
	if request.Media.SampleRateHz != SampleRateHz || request.Media.Encoding != "pcm_s16le" || request.Media.Channels != 1 {
		return nil, &runtimepkg.ProviderError{
			Code:    "unsupported_media",
			Message: fmt.Sprintf("Omi realtime listens to mono pcm_s16le at 16000 Hz only, got %s/%d Hz/%d channels", request.Media.Encoding, request.Media.SampleRateHz, request.Media.Channels),
			Hint:    "Resample the input to mono pcm_s16le at 16000 Hz.",
		}
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("omi stt requires an api key credential")
	}
	key := strings.TrimSpace(credential.Value)
	endpoint, err := a.endpointPolicy.Parse(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("omi stt endpoint: %w", err)
	}
	if endpoint.Path != streamPath {
		return nil, fmt.Errorf("omi stt endpoint path must be %s, got %q", streamPath, endpoint.Path)
	}
	sessionID, err := newSessionID()
	if err != nil {
		return nil, fmt.Errorf("omi stt session id: %w", err)
	}
	start := startFrame{
		Type: "start", APIKey: key, SessionID: sessionID, Model: model,
		StreamingMode: "realtime", EventSchema: "omi-agent-v1",
		AudioFormat: "pcm_s16le", SampleRateHz: SampleRateHz, Channels: 1,
		Language: language,
	}
	if silence > 0 {
		start.Endpointing = &startEndpoint{SilenceMS: silence}
	}
	payload, err := json.Marshal(start)
	if err != nil {
		return nil, errors.New("omi stt start frame could not be encoded")
	}

	// The key travels in the start frame, so the dial refuses a redirect:
	// following one would send the frame to whatever host the response named.
	conn, response, err := websocket.Dial(ctx, endpoint.String(), &websocket.DialOptions{HTTPClient: a.httpClient})
	if err != nil {
		return nil, handshakeError(response, err)
	}
	conn.SetReadLimit(a.maxMessageBytes)

	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &sttStream{
		conn: conn, ctx: streamCtx, cancel: cancel,
		events:            make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		setupDone:         make(chan error, 1),
		done:              make(chan struct{}),
		drainTimeout:      a.closeDrainTimeout,
		keepAliveInterval: a.keepAliveInterval,
		model:             model,
		language:          language,
		sessionID:         sessionID,
		redactKey:         []byte(key),
		frameBytes:        SampleRateHz * 2 * frameMillis / 1000,
		finalized:         map[string]struct{}{},
	}
	stream.lastWrite.Store(time.Now().UnixNano())
	if err := conn.Write(ctx, websocket.MessageText, payload); err != nil {
		_ = conn.CloseNow()
		cancel()
		return nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Omi STT start frame could not be sent", Retryable: true, Cause: err}
	}
	clear(payload)
	go stream.readLoop()

	setupCtx, cancelSetup := context.WithTimeout(ctx, a.setupTimeout)
	defer cancelSetup()
	select {
	case err := <-stream.setupDone:
		if err != nil {
			_ = stream.abort()
			return nil, err
		}
	case <-setupCtx.Done():
		_ = stream.abort()
		return nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: fmt.Sprintf("Omi STT sent no ready within %s", a.setupTimeout), Retryable: true, Cause: setupCtx.Err()}
	}
	// The reader sends session.ready and the opening usage itself: it is the
	// only goroutine that sends on or closes the events channel.
	go stream.keepAlive()
	return stream, nil
}

// realtimeOptions refuses asks the socket cannot honor. The gateway's
// support table refuses them at create; this holds for any other caller.
func realtimeOptions(options *protocol.SttOptions) error {
	switch {
	case options.Diarize():
		return &runtimepkg.ProviderError{Code: "invalid_request", Message: "Omi realtime speaker labels are provisional and not offered", Hint: "Drop the diarization option, or use prerecorded transcription."}
	case len(options.GetKeywords()) > 0:
		return &runtimepkg.ProviderError{Code: "invalid_request", Message: "Omi realtime takes no keyword list", Hint: "Drop the keywords option or choose a provider that supports vocabulary biasing."}
	case options.WantsWordTimestamps():
		return &runtimepkg.ProviderError{Code: "invalid_request", Message: "Omi realtime returns no word timings", Hint: "Drop the word_timestamps option."}
	case options.ReduceNoise():
		return &runtimepkg.ProviderError{Code: "invalid_request", Message: "Omi realtime has no noise-reduction option", Hint: "Drop the noise-reduction option."}
	case options.TranslationTarget() != "":
		return &runtimepkg.ProviderError{Code: "invalid_request", Message: "Omi realtime does not translate", Hint: "Drop the translation option."}
	}
	return nil
}

// silenceMS reads provider_options.omi.silence_ms, the endpointer's silence
// window, or 0 when the caller left Omi's default.
func silenceMS(options *protocol.SttOptions) (int, error) {
	value, ok := options.Provider(ProviderName)["silence_ms"]
	if !ok {
		return 0, nil
	}
	var number float64
	switch typed := value.(type) {
	case float64:
		number = typed
	case int:
		number = float64(typed)
	case int64:
		number = float64(typed)
	default:
		number = math.NaN()
	}
	if math.IsNaN(number) || number != math.Trunc(number) || number < silenceMin || number > silenceMax {
		return 0, &runtimepkg.ProviderError{
			Code:    "invalid_request",
			Message: fmt.Sprintf("Omi silence_ms must be an integer from %d to %d, got %s", silenceMin, silenceMax, protocol.SttOptionString(value)),
			Hint:    "Set provider_options.omi.silence_ms to an integer from 200 to 1100.",
		}
	}
	return int(number), nil
}

// newSessionID is a random RFC 4122 version 4 UUID. It names the session to
// Omi and becomes the provider request id.
func newSessionID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	id[6] = id[6]&0x0f | 0x40
	id[8] = id[8]&0x3f | 0x80
	text := hex.EncodeToString(id[:])
	return text[0:8] + "-" + text[8:12] + "-" + text[12:16] + "-" + text[16:20] + "-" + text[20:], nil
}

// handshakeError classifies a refused upgrade, of which the dialer keeps the
// first kilobyte. A redirect is refused rather than followed.
func handshakeError(response *http.Response, cause error) error {
	status := 0
	var body []byte
	if response != nil {
		status = response.StatusCode
		if response.Body != nil {
			body, _ = io.ReadAll(io.LimitReader(response.Body, handshakeErrorBytes))
			_ = response.Body.Close()
		}
	}
	dialErr := providerError("Omi STT connection could not be established", parseError(body), status, body)
	dialErr.Cause = cause
	return dialErr
}

// sttStream is one open realtime socket.
type sttStream struct {
	conn              *websocket.Conn
	ctx               context.Context
	cancel            context.CancelFunc
	events            chan runtimepkg.ProviderEvent
	setupDone         chan error
	done              chan struct{}
	drainTimeout      time.Duration
	keepAliveInterval time.Duration
	model             string
	language          string
	sessionID         string
	// redactKey is the credential, kept only to scrub it from any vendor
	// frame that echoes it before the frame reaches an event or error.
	redactKey  []byte
	frameBytes int

	writeMu sync.Mutex
	// carry holds an odd trailing byte until the next write: frames must be
	// whole two-byte samples. Guarded by writeMu.
	carry []byte
	// sentBytes is the PCM written to the socket. Omi meters the audio it
	// receives and reports it as audio_ms; this is the fallback estimate.
	sentBytes atomic.Int64
	// lastWrite is the UnixNano of the last frame sent, read by keepAlive.
	lastWrite atomic.Int64
	// finalizeID numbers the finalize controls.
	finalizeID atomic.Int64

	gracefulOnce sync.Once
	abortOnce    sync.Once
	setupOnce    sync.Once
	inputClosed  atomic.Bool
	// estimated is set under writeMu when the bytes-sent estimate is taken,
	// so no write can land after the count it reports.
	estimated     atomic.Bool
	closed        atomic.Bool
	drainTimedOut atomic.Bool
	readyOnce     atomic.Bool
	closeErr      error

	// The fields below are owned by readLoop.
	readyAt       time.Time
	maxSession    time.Duration
	recordSeen    bool
	billed        bool
	endedReason   string
	turnOpen      bool
	turnID        int
	providerTurn  string
	lastPartial   string
	finalized     map[string]struct{}
	anonymousTurn int

	terminalMu  sync.Mutex
	terminalErr error
}

func (s *sttStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

// WriteAudio forwards PCM as binary frames of at most 100 ms. Pacing is the
// caller's.
func (s *sttStream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("omi stt audio is empty")
	}
	if s.inputClosed.Load() || s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.inputClosed.Load() || s.closed.Load() || s.estimated.Load() {
		return runtimepkg.ErrSessionClosed
	}
	pcm := append(s.carry, audio...)
	s.carry = nil
	if len(pcm)%2 == 1 {
		s.carry = []byte{pcm[len(pcm)-1]}
		pcm = pcm[:len(pcm)-1]
	}
	for offset := 0; offset < len(pcm); offset += s.frameBytes {
		end := min(offset+s.frameBytes, len(pcm))
		if err := s.writeLocked(ctx, websocket.MessageBinary, pcm[offset:end]); err != nil {
			return err
		}
		s.sentBytes.Add(int64(end - offset))
	}
	return nil
}

// CommitAudio sends finalize, which flushes the open turn as a turn_final
// and leaves the session open. Each id is new, so a late finalize_ack cannot
// be mistaken for a later commit's.
func (s *sttStream) CommitAudio(ctx context.Context) error {
	if s.inputClosed.Load() || s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	frame := marshalData(map[string]string{"type": "finalize", "id": strconv.FormatInt(s.finalizeID.Add(1), 10)})
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.inputClosed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	return s.writeLocked(ctx, websocket.MessageText, frame)
}

func (s *sttStream) AppendText(context.Context, string) error {
	return runtimepkg.ErrUnsupportedOperation
}

func (s *sttStream) CommitText(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// Cancel has nothing to suppress: a transcription session produces no answer
// to interrupt, and stop would end the session.
func (s *sttStream) Cancel(context.Context) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	return nil
}

// Close sends stop, refuses further input, and keeps reading until
// canonical_record arrives or the drain timeout passes.
func (s *sttStream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		if s.closed.Load() {
			return
		}
		s.writeMu.Lock()
		s.inputClosed.Store(true)
		err := s.conn.Write(ctx, websocket.MessageText, stopFrame)
		s.writeMu.Unlock()
		if err != nil {
			if s.TerminalError() == nil {
				s.closeErr = &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Omi STT stop could not be sent", Retryable: true, Cause: err}
			}
			_ = s.abort()
			return
		}
		go s.drainThenClose()
	})
	return s.closeErr
}

func (s *sttStream) drainThenClose() {
	timer := time.NewTimer(s.drainTimeout)
	defer timer.Stop()
	select {
	case <-s.done:
	case <-timer.C:
		s.drainTimedOut.Store(true)
		s.writeMu.Lock()
		_ = s.conn.Close(websocket.StatusNormalClosure, "")
		s.writeMu.Unlock()
		// The close ends the reader's Read; give it a moment to report the
		// timeout before the context cancels its send.
		select {
		case <-s.done:
		case <-time.After(time.Second):
		}
	}
	s.closed.Store(true)
	s.cancel()
}

// Abort tears the socket down immediately after a terminal runtime failure.
func (s *sttStream) Abort(context.Context) error { return s.abort() }

func (s *sttStream) abort() error {
	s.abortOnce.Do(func() {
		s.inputClosed.Store(true)
		s.closed.Store(true)
		s.cancel()
		if err := s.conn.CloseNow(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
	})
	return s.closeErr
}

// TerminalError preserves the failure that ended the socket, independently of
// the bounded event queue.
func (s *sttStream) TerminalError() error {
	s.terminalMu.Lock()
	defer s.terminalMu.Unlock()
	return s.terminalErr
}

func (s *sttStream) setTerminal(err error) bool {
	s.terminalMu.Lock()
	defer s.terminalMu.Unlock()
	if s.terminalErr != nil {
		return false
	}
	s.terminalErr = err
	return true
}

func (s *sttStream) writeLocked(ctx context.Context, messageType websocket.MessageType, payload []byte) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if err := s.conn.Write(ctx, messageType, payload); err != nil {
		if s.closed.Load() {
			return runtimepkg.ErrSessionClosed
		}
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Omi STT socket write failed", Retryable: true, Cause: err}
	}
	s.lastWrite.Store(time.Now().UnixNano())
	return nil
}

// keepAlive sends keep_alive whenever no frame has gone out for the
// interval, so a caller that pauses its audio keeps the socket. It stops
// once input is closed.
func (s *sttStream) keepAlive() {
	ticker := time.NewTicker(max(s.keepAliveInterval/4, time.Millisecond))
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
		if time.Since(time.Unix(0, s.lastWrite.Load())) < s.keepAliveInterval {
			continue
		}
		s.writeMu.Lock()
		if s.inputClosed.Load() || s.closed.Load() {
			s.writeMu.Unlock()
			return
		}
		err := s.writeLocked(s.ctx, websocket.MessageText, keepAliveFrame)
		s.writeMu.Unlock()
		if err != nil {
			return
		}
	}
}

func (s *sttStream) emit(event runtimepkg.ProviderEvent) {
	select {
	case s.events <- event:
	case <-s.ctx.Done():
	}
}

// fail reports the session's terminal error once, whether it arrived as an
// error frame or as the close that follows it.
func (s *sttStream) fail(err *runtimepkg.ProviderError) {
	s.settleSetup(err)
	if s.setTerminal(err) {
		s.emit(runtimepkg.ProviderEvent{Err: err})
	}
}

func (s *sttStream) settleSetup(err error) {
	s.setupOnce.Do(func() { s.setupDone <- err })
}

// redact scrubs the credential from a vendor frame before it is attached to
// an event or error. Omi is not known to echo it; this keeps an echo from
// leaking if it ever does.
func (s *sttStream) redact(raw []byte) []byte {
	if len(s.redactKey) == 0 || !bytes.Contains(raw, s.redactKey) {
		return raw
	}
	return bytes.ReplaceAll(raw, s.redactKey, redactedKey)
}

// frameHead is read first to dispatch on the frame type; each type then
// decodes only its own fields, so one frame's shape cannot break another's.
type frameHead struct {
	Type string `json:"type"`
}

func (s *sttStream) readLoop() {
	defer close(s.events)
	defer close(s.done)
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			s.finish(err)
			return
		}
		if messageType != websocket.MessageText {
			continue
		}
		payload = s.redact(payload)
		var head frameHead
		if json.Unmarshal(payload, &head) != nil {
			continue
		}
		if s.handle(head.Type, payload) {
			return
		}
	}
}

// handle dispatches one frame and reports whether the session is over.
func (s *sttStream) handle(frameType string, raw []byte) bool {
	switch frameType {
	case "ready":
		s.opened(raw)
	case "partial":
		s.partial(raw)
	case "turn_final":
		s.turnFinal(raw)
	case "canonical_record":
		return s.record(raw)
	case "error":
		var detail errorDetail
		_ = json.Unmarshal(raw, &detail)
		// The runtime aborts on the terminal error, so the bytes already sent
		// must be reported first or finish never gets the chance.
		s.reportEstimate()
		s.fail(providerError("Omi STT reported an error", &detail, 0, raw))
	default:
		if _, quiet := quietFrames[frameType]; quiet {
			return false
		}
		s.emit(runtimepkg.ProviderEvent{
			Type: protocol.EventWarning, Data: marshalData(map[string]any{"message": "ignored Omi STT event type", "provider_type": frameType, "provider_request_id": s.sessionID}),
		})
	}
	return false
}

// readyFrame is the part of ready this adapter reads.
type readyFrame struct {
	SessionID   string   `json:"session_id"`
	MaxSessionS *float64 `json:"max_session_s"`
	Model       *struct {
		Requested string `json:"requested"`
		Served    string `json:"served"`
	} `json:"model"`
}

// opened handles ready: a model other than the one asked for fails Open,
// because Omi serves an unknown model silently as omi-medical-1. Otherwise it
// releases Open, then queues session.ready and the opening usage. The sends
// follow settleSetup because the runtime only starts draining Events once
// Open has returned. A repeated ready announces nothing new.
func (s *sttStream) opened(raw []byte) {
	if !s.readyOnce.CompareAndSwap(false, true) {
		return
	}
	var ready readyFrame
	_ = json.Unmarshal(raw, &ready)
	served := ""
	if ready.Model != nil {
		served = strings.TrimSpace(ready.Model.Served)
	}
	if served != s.model {
		s.fail(&runtimepkg.ProviderError{
			Code:    "provider_unavailable",
			Message: fmt.Sprintf("Omi STT served model %q, requested %q", served, s.model),
		})
		return
	}
	if ready.MaxSessionS != nil && *ready.MaxSessionS > 0 && !math.IsInf(*ready.MaxSessionS, 0) {
		s.maxSession = time.Duration(*ready.MaxSessionS * float64(time.Second))
	}
	s.readyAt = time.Now()
	s.settleSetup(nil)
	data := s.baseData()
	data["provider_model"] = served
	data["language"] = s.language
	if s.maxSession > 0 {
		data["max_session_s"] = s.maxSession.Seconds()
	}
	s.emit(runtimepkg.ProviderEvent{Type: protocol.EventSessionReady, Data: marshalData(data), Extensions: extension(raw)})
	// The opening observation stays incomplete until canonical_record
	// reports the metered audio.
	s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: marshalData(s.usageData()), Billing: s.observation()})
}

// usageData is the public payload of a usage.observed event: the provider
// correlation ID and the bytes sent, as other adapters report. Omi's own
// session statistics travel in the namespaced extension, which telemetry
// never copies.
func (s *sttStream) usageData() map[string]any {
	return map[string]any{"provider_request_id": s.sessionID}
}

func (s *sttStream) baseData() map[string]any {
	return map[string]any{"provider_request_id": s.sessionID, "session_id": s.sessionID}
}

// transcriptFrame is a partial or a turn_final.
type transcriptFrame struct {
	Text   string          `json:"text"`
	TurnID json.RawMessage `json:"turn_id"`
	StartS *float64        `json:"start_s"`
	EndS   *float64        `json:"end_s"`
}

// turnKey reads turn_id, a string or a number, as text, or "" when absent.
func turnKey(raw json.RawMessage) string {
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return strings.TrimSpace(text)
	}
	var number json.Number
	if json.Unmarshal(raw, &number) == nil {
		return number.String()
	}
	return ""
}

// partial maps a draft revision of the open turn onto transcript.delta. Each
// carries the turn's transcript so far and replaces the one before. A partial
// for a turn that already has its final is stale and dropped, as is one that
// repeats the previous text.
func (s *sttStream) partial(raw []byte) {
	var frame transcriptFrame
	if json.Unmarshal(raw, &frame) != nil {
		return
	}
	key := turnKey(frame.TurnID)
	if key != "" {
		if _, done := s.finalized[key]; done {
			return
		}
	}
	text := strings.TrimSpace(frame.Text)
	if text == "" {
		return
	}
	s.beginTurn(key, frame, raw)
	if text == s.lastPartial {
		return
	}
	s.lastPartial = text
	s.emit(runtimepkg.ProviderEvent{Type: protocol.EventTranscriptDelta, Data: s.transcriptData(frame, text, false), Extensions: extension(raw)})
}

// turnFinal maps the turn's authoritative text onto transcript.final and
// ends the turn. A second final for the same turn is dropped.
func (s *sttStream) turnFinal(raw []byte) {
	var frame transcriptFrame
	if json.Unmarshal(raw, &frame) != nil {
		return
	}
	key := turnKey(frame.TurnID)
	if key == "" {
		s.anonymousTurn++
		key = "anonymous-" + strconv.Itoa(s.anonymousTurn)
	}
	if _, done := s.finalized[key]; done {
		return
	}
	s.finalized[key] = struct{}{}
	text := strings.TrimSpace(frame.Text)
	if text != "" {
		s.beginTurn(key, frame, raw)
	}
	if s.turnOpen && s.providerTurn != key {
		// A final for a turn whose partials never came, while another turn
		// is open: that one is over too.
		s.endTurn(frame, raw, "superseded")
	}
	// An empty final is still emitted: silence legitimately completes a turn,
	// and it is the evidence a caller needs to tell silence from loss.
	s.providerTurn = key
	s.emit(runtimepkg.ProviderEvent{Type: protocol.EventTranscriptFinal, Data: s.transcriptData(frame, text, true), Extensions: extension(raw)})
	if s.turnOpen {
		s.endTurn(frame, raw, "end_of_turn")
		return
	}
	s.turnID++
	s.providerTurn, s.lastPartial = "", ""
}

// beginTurn opens the turn on its first words. A different turn still open
// (its final dropped by Omi) is ended first.
func (s *sttStream) beginTurn(key string, frame transcriptFrame, raw []byte) {
	if s.turnOpen && s.providerTurn == key {
		return
	}
	if s.turnOpen {
		s.endTurn(frame, raw, "superseded")
	}
	s.turnOpen = true
	s.providerTurn = key
	s.lastPartial = ""
	data := s.baseData()
	data["turn_id"] = s.turnID
	if key != "" {
		data["provider_turn_id"] = key
	}
	setMilliseconds(data, "audio_start_ms", frame.StartS)
	s.emit(runtimepkg.ProviderEvent{Type: protocol.EventSpeechStarted, Data: marshalData(data), Extensions: extension(raw)})
}

func (s *sttStream) endTurn(frame transcriptFrame, raw []byte, reason string) {
	data := s.baseData()
	data["turn_id"] = s.turnID
	if s.providerTurn != "" {
		data["provider_turn_id"] = s.providerTurn
	}
	data["reason"] = reason
	if reason == "end_of_turn" {
		setMilliseconds(data, "audio_end_ms", frame.EndS)
	}
	s.emit(runtimepkg.ProviderEvent{Type: protocol.EventSpeechEnded, Data: marshalData(data), Extensions: extension(raw)})
	s.turnOpen = false
	s.turnID++
	s.providerTurn, s.lastPartial = "", ""
}

// transcriptData carries the text and the turn's audio window. Realtime
// frames carry no word timings and no confidence.
func (s *sttStream) transcriptData(frame transcriptFrame, text string, final bool) json.RawMessage {
	data := s.baseData()
	data["text"] = text
	data["is_final"] = final
	data["speech_final"] = final
	data["turn_id"] = s.turnID
	if key := turnKey(frame.TurnID); key != "" {
		data["provider_turn_id"] = key
	}
	data["language"] = s.language
	setMilliseconds(data, "audio_start_ms", frame.StartS)
	setMilliseconds(data, "audio_end_ms", frame.EndS)
	return marshalData(data)
}

// setMilliseconds writes an optional time in seconds as milliseconds, and
// leaves the key out when the frame did not carry it.
func setMilliseconds(data map[string]any, key string, seconds *float64) {
	if seconds != nil {
		data[key] = int64(math.Round(*seconds * 1_000))
	}
}

// recordFrame is the part of canonical_record this adapter reads. Its
// transcripts are a repaired text that differs from the finals already
// delivered, so they are never read.
type recordFrame struct {
	Record *struct {
		Status      string `json:"status"`
		EmptyReason string `json:"empty_reason"`
	} `json:"record"`
	SessionStats *struct {
		Utterances  *int64 `json:"utterances"`
		EndedReason string `json:"ended_reason"`
	} `json:"session_stats"`
}

// record handles canonical_record, the stop sequence's last frame. It
// completes the duration observation with session_stats.audio_ms. A record
// status other than complete is reported as a warning: the finals already
// delivered stand, and the audio was metered either way. After the caller's
// stop the session is over and the socket is closed; an unsolicited record
// is followed by the server's own close, which finish classifies.
func (s *sttStream) record(raw []byte) bool {
	if s.recordSeen {
		return false
	}
	s.recordSeen = true
	var frame recordFrame
	_ = json.Unmarshal(raw, &frame)
	if frame.SessionStats != nil {
		s.endedReason = strings.TrimSpace(frame.SessionStats.EndedReason)
	}
	if frame.Record != nil && frame.Record.Status != "" && frame.Record.Status != "complete" {
		data := s.baseData()
		data["message"] = "Omi canonical record is not complete"
		data["record_status"] = frame.Record.Status
		if frame.Record.EmptyReason != "" {
			data["empty_reason"] = frame.Record.EmptyReason
		}
		s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: marshalData(data)})
	}
	// Taking the write lock waits out a frame in flight, so the byte count
	// below is final.
	s.writeMu.Lock()
	stopped := s.inputClosed.Load()
	s.writeMu.Unlock()
	observation := metering.Duration("stream", s.model, "streaming", raw, 1, "session_stats", "audio_ms")
	observation.ProviderRequestID = s.sessionID
	data := s.usageData()
	data["audio_bytes"] = s.sentBytes.Load()
	stats := map[string]any{}
	if audioMS, ok := metering.Quantity(raw, 1, "session_stats", "audio_ms"); ok {
		stats["audio_ms"] = audioMS
	}
	if frame.SessionStats != nil && frame.SessionStats.Utterances != nil {
		stats["utterances"] = *frame.SessionStats.Utterances
	}
	if !observation.Complete {
		// A record without audio_ms is not evidence of the metered audio:
		// fall back to the bytes sent, still incomplete.
		observation = s.estimate()
		data["billing_evidence"] = "pcm_bytes_sent"
	}
	s.billed = true
	s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: marshalData(data), Extensions: extension(marshalData(map[string]any{"session_stats": stats})), Billing: observation})
	if !stopped {
		return false
	}
	s.writeMu.Lock()
	_ = s.conn.Close(websocket.StatusNormalClosure, "")
	s.writeMu.Unlock()
	return true
}

// observation is the session's one duration observation before any
// evidence: incomplete, so a session lost before canonical_record settles
// as unresolved rather than free. Language stays empty because every
// language bills alike.
func (s *sttStream) observation() *protocol.BillingObservation {
	return &protocol.BillingObservation{OperationID: "stream", Model: s.model, Mode: "streaming", ProviderRequestID: s.sessionID, Quantities: map[string]int64{}}
}

// estimate is the observation for a session that ended without Omi's
// metered audio_ms: the PCM sent as an exact fraction of a second, kept
// INCOMPLETE. Omi meters the audio it receives, so the bytes are the best
// estimate, but they are the relay's count rather than the vendor's, and
// settlement decides what an unconfirmed quantity is worth.
func (s *sttStream) estimate() *protocol.BillingObservation {
	observation := s.observation()
	if sent := s.sentBytes.Load(); sent > 0 {
		observation.Quantities["duration_seconds"] = sent
		observation.QuantityDenominators = map[string]int64{"duration_seconds": SampleRateHz * 2}
	}
	return observation
}

// reportEstimate reports, once, the bytes sent as incomplete evidence for a
// ready session that will not get its canonical_record: the socket closed or
// Omi sent a terminal error frame. It is a no-op once a record billed the
// session.
func (s *sttStream) reportEstimate() {
	if !s.readyOnce.Load() || s.billed {
		return
	}
	s.billed = true
	// Taking the write lock waits out a frame in flight and closes input,
	// so the count below covers every byte that reached the socket.
	s.writeMu.Lock()
	s.estimated.Store(true)
	s.writeMu.Unlock()
	data := s.usageData()
	data["audio_bytes"] = s.sentBytes.Load()
	data["billing_evidence"] = "pcm_bytes_sent"
	s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: marshalData(data), Billing: s.estimate()})
}

// finish classifies how the socket ended. A close after Abort, or after the
// canonical_record that answered stop, is normal. A session that ends
// without its record reports the bytes it sent as incomplete evidence. A
// failure an error frame already explained is not reported twice; anything
// else is classified from the close status.
func (s *sttStream) finish(err error) {
	if !s.closed.Load() {
		s.reportEstimate()
	}
	if s.drainTimedOut.Load() {
		s.fail(&runtimepkg.ProviderError{Code: "request_timeout", Message: fmt.Sprintf("Omi STT sent no canonical_record within %s of stop", s.drainTimeout), Retryable: false, Cause: err})
		return
	}
	if s.closed.Load() {
		s.settleSetup(&runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Omi STT closed before sending ready", Retryable: true, Cause: err})
		return
	}
	if s.TerminalError() != nil {
		return
	}
	status := websocket.CloseStatus(err)
	reason := ""
	cause := err
	var closeErr websocket.CloseError
	if errors.As(err, &closeErr) {
		// A close reason is vendor text: scrub the credential from it before
		// it reaches the message, the cause or the extension.
		reason = strings.TrimSpace(string(s.redact([]byte(closeErr.Reason))))
		cause = websocket.CloseError{Code: closeErr.Code, Reason: reason}
	}
	failure := socketFailure(int(status), reason)
	switch {
	case s.reachedSessionCap():
		failure = &runtimepkg.ProviderError{Code: "input_too_large", Message: fmt.Sprintf("Omi STT ended the session at its %s cap", s.maxSession)}
	case s.inputClosed.Load() && status == websocket.StatusNormalClosure:
		failure.Message = "Omi STT closed the session after stop without a canonical_record"
	}
	failure.Cause = cause
	if status != -1 {
		failure.Extensions = map[string]json.RawMessage{extensionID: marshalData(map[string]any{"close_status": int(status), "close_reason": reason})}
	}
	s.fail(failure)
}

// reachedSessionCap reports whether the server ended a ready session at
// max_session_s, by the record's ended_reason or by the clock.
func (s *sttStream) reachedSessionCap() bool {
	if s.readyAt.IsZero() || s.inputClosed.Load() {
		return false
	}
	reason := strings.ToLower(s.endedReason)
	if strings.Contains(reason, "max_session") || strings.Contains(reason, "session_limit") || strings.Contains(reason, "max_duration") {
		return true
	}
	return s.maxSession > 0 && time.Since(s.readyAt) >= s.maxSession-sessionCapSlack
}

// socketFailure maps a close status onto the protocol classification. 1013
// is Omi's stream-capacity refusal (TOO_MANY_STREAMS, SPEAKER_CAPACITY_FULL),
// sent before any audio, so another provider can take the session. 1008
// follows an error frame, which fail has already reported; a 1008 without
// one is read from its reason.
func socketFailure(code int, reason string) *runtimepkg.ProviderError {
	switch code {
	case int(websocket.StatusTryAgainLater):
		message := "Omi STT is at its stream capacity (1013)"
		if reason != "" && len(reason) <= 64 {
			message = fmt.Sprintf("Omi STT is at its stream capacity (1013 %s)", reason)
		}
		return &runtimepkg.ProviderError{Code: "provider_rate_limited", Message: message, Retryable: true}
	case int(websocket.StatusPolicyViolation):
		canonical, retryable := classify(&errorDetail{Code: reason}, 0)
		if reason == "" {
			canonical, retryable = "invalid_request", false
		}
		return &runtimepkg.ProviderError{Code: canonical, Message: "Omi STT refused the session (1008)", Retryable: retryable}
	case int(websocket.StatusMessageTooBig):
		return &runtimepkg.ProviderError{Code: "input_too_large", Message: "Omi STT rejected an oversized message (1009)"}
	case int(websocket.StatusUnsupportedData), int(websocket.StatusInvalidFramePayloadData):
		return &runtimepkg.ProviderError{Code: "invalid_request", Message: fmt.Sprintf("Omi STT rejected the audio (%d)", code)}
	default:
		message := "Omi STT closed the session"
		if code > 0 {
			message = fmt.Sprintf("Omi STT closed the session (%d)", code)
		}
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: message, Retryable: true}
	}
}
