package mistral

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
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
	// STTAdapterID is the identifier the connector registers this adapter under.
	STTAdapterID = "mistral.stt.v1"
	// DefaultSTTModel is the streaming Voxtral 3 transcription model.
	DefaultSTTModel = "voxtral-mini-transcribe-realtime-3-0"
	// DefaultStreamingDelayMS is the socket's lookahead: how far behind the
	// audio the text may run in exchange for accuracy. 480 is Mistral's
	// recommended balance.
	DefaultStreamingDelayMS = 480

	realtimePath = "/v1/audio/transcriptions/realtime"

	// maxAppendBytes is the documented cap on the decoded audio of one
	// input_audio.append.
	maxAppendBytes = 262_144

	// finalGrace sets the finalization rule. turn.end runs on the audio
	// clock while text lags it by up to the streaming delay ("the last words
	// arrive after the turn event", Mistral's turn-detection docs): at 480 ms
	// a turn's last words arrived 10 to 240 ms after turn.end live, at 1200
	// ms up to 800 ms after. Punctuation is no completion signal: at 1200 ms
	// a turn.end once arrived 250 ms after a mid-turn "Voice." and just ahead
	// of the next sentence. So a turn's final goes out once the delay plus
	// finalGrace has passed since turn.end.
	finalGrace = 150 * time.Millisecond

	defaultSTTEventBuffer  = 64
	defaultMaxMessageBytes = 1 << 20
	// defaultSetupTimeout bounds session.created and the session.updated
	// that acknowledges the configuration, both of which arrive within a
	// second live.
	defaultSetupTimeout = 15 * time.Second
	// defaultMaxPendingFrameBytes bounds the frames read off the socket and
	// not yet published, while a consumer that stops draining Events holds
	// readLoop up.
	defaultMaxPendingFrameBytes = 4 << 20
	// closeDeliveryTimeout bounds how long a drain timeout waits for the last
	// turn's final and the failure to reach a consumer still draining Events.
	closeDeliveryTimeout = 10 * time.Second
	// defaultCloseDrainTimeout bounds how long Close waits for
	// transcription.done after the flush. It arrived about a second after
	// the flush live.
	defaultCloseDrainTimeout = 10 * time.Second
	// handshakeErrorBytes is all coder/websocket keeps of a refused
	// handshake's body.
	handshakeErrorBytes = 1 << 10
)

// errInboxFull marks the read that would have pushed the unpublished frames
// past their bound.
var errInboxFull = errors.New("mistral stt: unpublished frames exceeded their bound")

// sampleRates are the pcm_s16le rates the session's audio_format takes.
var sampleRates = map[int]struct{}{8_000: {}, 16_000: {}, 24_000: {}, 44_100: {}, 48_000: {}}

// STTConfig controls local transport bounds. Provider identity, model,
// credential and endpoint always come from the verified plan.
type STTConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
	// SetupTimeout bounds the wait for session.created and session.updated.
	SetupTimeout time.Duration
	// CloseDrainTimeout bounds how long Close waits for transcription.done.
	CloseDrainTimeout time.Duration
	// MaxPendingFrameBytes bounds the server frames read but not yet
	// published. Past it the session fails rather than buffer without end.
	MaxPendingFrameBytes int
}

// STTAdapter opens Voxtral realtime transcription sessions.
type STTAdapter struct {
	id                   string
	httpClient           *http.Client
	eventBuffer          int
	maxMessageBytes      int64
	setupTimeout         time.Duration
	closeDrainTimeout    time.Duration
	maxPendingFrameBytes int
	endpointPolicy       upstream.WebSocketPolicy
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
	if config.MaxPendingFrameBytes == 0 {
		config.MaxPendingFrameBytes = defaultMaxPendingFrameBytes
	}
	if config.EventBuffer < 1 || config.MaxMessageBytes < 1 || config.SetupTimeout <= 0 || config.CloseDrainTimeout <= 0 || config.MaxPendingFrameBytes < 1 {
		return nil, errors.New("mistral stt: event buffer, message bound and timeouts must be positive")
	}
	policy, err := upstream.NewWebSocketPolicy(officialHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &STTAdapter{
		id: config.AdapterID, httpClient: config.HTTPClient, eventBuffer: config.EventBuffer,
		maxMessageBytes: config.MaxMessageBytes, setupTimeout: config.SetupTimeout,
		closeDrainTimeout: config.CloseDrainTimeout, maxPendingFrameBytes: config.MaxPendingFrameBytes, endpointPolicy: policy,
	}, nil
}

// ID returns the adapter identifier.
func (a *STTAdapter) ID() string { return a.id }

// Open dials the socket, configures the session and returns once
// session.updated has acknowledged it, so a refused key, language or delay
// fails Open where the runtime can fail over rather than on the first
// WriteAudio. Settings lock at the first append, so nothing is appended
// before the acknowledgement.
func (a *STTAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindSTT {
		return nil, fmt.Errorf("mistral stt supports stt sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != ProviderName {
		return nil, fmt.Errorf("mistral stt adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("mistral stt requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model != DefaultSTTModel {
		return nil, fmt.Errorf("mistral stt does not support model %q", model)
	}
	language, err := sttLanguage(request.Options.Language)
	if err != nil {
		return nil, err
	}
	if request.Media == nil {
		return nil, errors.New("mistral stt requires input media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("mistral stt input media: %w", err)
	}
	if _, ok := sampleRates[request.Media.SampleRateHz]; !ok || request.Media.Encoding != "pcm_s16le" || request.Media.Channels != 1 {
		return nil, &runtimepkg.ProviderError{
			Code:    "unsupported_media",
			Message: fmt.Sprintf("Mistral STT listens to mono pcm_s16le at 8000, 16000, 24000, 44100 or 48000 Hz, got %s/%d Hz/%d channels", request.Media.Encoding, request.Media.SampleRateHz, request.Media.Channels),
			Hint:    "Resample the input to mono pcm_s16le at 16000 Hz.",
		}
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("mistral stt requires an api key credential")
	}
	delayMS, err := streamingDelay(request.Options.STT)
	if err != nil {
		return nil, err
	}
	endpoint, err := a.realtimeEndpoint(request.Plan.Route.Endpoint, model)
	if err != nil {
		return nil, fmt.Errorf("mistral stt endpoint: %w", err)
	}

	headers := make(http.Header)
	headers.Set("Authorization", "Bearer "+strings.TrimSpace(credential.Value))
	// The upgrade never follows a redirect: it would replay the key to a URL
	// the endpoint policy never checked, possibly over plain HTTP.
	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: noRedirectClient(a.httpClient), HTTPHeader: headers})
	if err != nil {
		return nil, handshakeError(response, err)
	}
	conn.SetReadLimit(a.maxMessageBytes)

	update := sessionUpdate{Type: "session.update", Session: sessionConfig{
		AudioFormat:            audioFormat{Encoding: "pcm_s16le", SampleRate: request.Media.SampleRateHz},
		TargetStreamingDelayMS: delayMS,
		Language:               language,
		ContextBias:            request.Options.STT.GetKeywords(),
	}}
	setupCtx, cancelSetup := context.WithTimeout(ctx, a.setupTimeout)
	defer cancelSetup()
	requestID, err := configure(setupCtx, conn, update)
	if err != nil {
		_ = conn.CloseNow()
		return nil, err
	}

	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &sttStream{
		conn: conn, ctx: streamCtx, cancel: cancel,
		events:         make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		done:           make(chan struct{}),
		inboxReady:     make(chan struct{}, 1),
		maxInboxBytes:  a.maxPendingFrameBytes,
		drainTimeout:   a.closeDrainTimeout,
		model:          model,
		language:       language,
		requestID:      requestID,
		finalDeadline:  time.Duration(delayMS)*time.Millisecond + finalGrace,
		bytesPerSecond: int64(request.Media.SampleRateHz * 2),
	}
	// The reader sends session.ready and the opening usage itself: it is the
	// only goroutine that sends on or closes the events channel.
	go stream.readLoop()
	return stream, nil
}

// handshakeError classifies a refused upgrade. A bad key answers 401 with
// {"detail":"Invalid API Key"}; a model the key cannot open answers 403 with
// an empty body.
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
	dialErr := providerError("Mistral STT connection could not be established", parseError(body), status, body)
	dialErr.Cause = cause
	return dialErr
}

// streamingDelay reads target_streaming_delay_ms from provider_options. The
// socket accepts 80 to 1200 ms and exactly 2400 and refuses anything else
// with an error frame, so the same set is refused here, by name.
func streamingDelay(options *protocol.SttOptions) (int, error) {
	value, ok := options.Provider(ProviderName)["target_streaming_delay_ms"]
	if !ok {
		return DefaultStreamingDelayMS, nil
	}
	delay, err := strconv.Atoi(strings.TrimSpace(protocol.SttOptionString(value)))
	if err != nil || !(delay >= 80 && delay <= 1200 || delay == 2400) {
		return 0, &runtimepkg.ProviderError{
			Code:    "invalid_request",
			Message: fmt.Sprintf("Mistral STT target_streaming_delay_ms must be 80 to 1200 or 2400, got %q", protocol.SttOptionString(value)),
			Hint:    "Set provider_options.mistral.target_streaming_delay_ms to an integer from 80 to 1200, or 2400.",
		}
	}
	return delay, nil
}

// realtimeEndpoint applies the shared allowlist, pins the path, and writes
// the model onto the query, the only parameter the URL carries.
func (a *STTAdapter) realtimeEndpoint(raw, model string) (string, error) {
	endpoint, err := a.endpointPolicy.Parse(raw)
	if err != nil {
		return "", err
	}
	if endpoint.Path != realtimePath {
		return "", fmt.Errorf("endpoint path must be %s, got %q", realtimePath, endpoint.Path)
	}
	endpoint.RawQuery = url.Values{"model": {model}}.Encode()
	return endpoint.String(), nil
}

// configure waits for session.created, sends session.update and reads until
// session.updated. session.created carries the request id, which becomes
// the provider request id. An error frame here is a refused setting.
func configure(ctx context.Context, conn *websocket.Conn, update sessionUpdate) (string, error) {
	payload, err := json.Marshal(update)
	if err != nil {
		return "", err
	}
	requestID := ""
	sent := false
	for {
		messageType, frame, err := conn.Read(ctx)
		if err != nil {
			failure := &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Mistral STT closed before acknowledging the session", Retryable: true, Cause: err}
			if ctx.Err() != nil {
				failure.Message = "Mistral STT did not acknowledge the session"
			}
			return "", failure
		}
		if messageType != websocket.MessageText {
			continue
		}
		var message serverMessage
		if json.Unmarshal(frame, &message) != nil {
			continue
		}
		if message.Session != nil && requestID == "" {
			requestID = requestIdentifier(message.Session.RequestID)
		}
		switch message.Type {
		case "session.created":
			if sent {
				continue
			}
			if err := conn.Write(ctx, websocket.MessageText, payload); err != nil {
				return "", &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Mistral STT session could not be configured", Retryable: true, Cause: err}
			}
			sent = true
		case "session.updated":
			if sent {
				return requestID, nil
			}
		case "error":
			return "", providerError("Mistral STT refused the session", message.Error, 0, frame)
		}
	}
}

// requestIdentifier drops an implausibly long id: billing caps request ids
// at 512 bytes.
func requestIdentifier(id string) string {
	id = strings.TrimSpace(id)
	if len(id) > 128 {
		return ""
	}
	return id
}

// sttStream is one open realtime socket.
type sttStream struct {
	conn           *websocket.Conn
	ctx            context.Context
	cancel         context.CancelFunc
	events         chan runtimepkg.ProviderEvent
	done           chan struct{}
	drainTimeout   time.Duration
	model          string
	language       string
	requestID      string
	finalDeadline  time.Duration
	bytesPerSecond int64

	writeMu sync.Mutex
	// carry holds an odd trailing byte until the next write: appends must be
	// whole two-byte samples. Guarded by writeMu.
	carry []byte
	// sentBytes is the PCM appended to the socket, which is what Mistral
	// bills.
	sentBytes atomic.Int64

	gracefulOnce  sync.Once
	abortOnce     sync.Once
	inputClosed   atomic.Bool
	closed        atomic.Bool
	drainTimedOut atomic.Bool
	closeErr      error

	// doneReceived is transcription.done. inputClosed only says the client
	// flushed; billing completes on transcription.done alone.
	doneReceived atomic.Bool

	// inbox holds the frames the socket reader has taken off the wire, each
	// stamped with when it arrived, until readLoop processes them. The reader
	// never waits on readLoop, so a consumer that stalls readLoop cannot delay
	// a frame's stamp, and a turn's membership is decided on arrival times
	// alone. inboxReady wakes readLoop.
	inboxMu       sync.Mutex
	inbox         []socketRead
	inboxBytes    int
	maxInboxBytes int
	// overflow is the failure the reader recorded when the inbox passed its
	// bound. Guarded by inboxMu until readLoop takes the entry it marks.
	overflow   *runtimepkg.ProviderError
	inboxReady chan struct{}

	// turn, held and the turn ids are owned by readLoop.
	turn turnState
	// held are the turn boundaries (a turn.start and possibly its turn.end)
	// that arrived while an earlier turn's final was still owed, oldest
	// first. Each speech.started goes out right after the final before it,
	// so the deltas in between stay with the turn that was ended.
	held []heldTurn

	terminalMu  sync.Mutex
	terminalErr error
}

// turnState is the open turn. text accumulates the append-only deltas
// verbatim, spacing included. ended is set by turn.end: the final is then
// owed, and deltas until it goes out still belong to this turn.
type turnState struct {
	id          int
	open        bool
	text        strings.Builder
	ended       bool
	endedAt     time.Time
	startTimeMS *int64
	endTimeMS   *int64
}

type heldTurn struct {
	raw         []byte
	startTimeMS *int64
	ended       bool
	endedAt     time.Time
	endTimeMS   *int64
}

func (s *sttStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

// WriteAudio forwards PCM as base64 input_audio.append frames of at most
// 262,144 decoded bytes. Pacing is the caller's.
func (s *sttStream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("mistral stt audio is empty")
	}
	if s.inputClosed.Load() || s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.inputClosed.Load() || s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	pcm := append(s.carry, audio...)
	s.carry = nil
	if len(pcm)%2 == 1 {
		s.carry = []byte{pcm[len(pcm)-1]}
		pcm = pcm[:len(pcm)-1]
	}
	for offset := 0; offset < len(pcm); offset += maxAppendBytes {
		end := min(offset+maxAppendBytes, len(pcm))
		if err := s.writeLocked(ctx, appendMessage{Type: "input_audio.append", Audio: base64.StdEncoding.EncodeToString(pcm[offset:end])}); err != nil {
			return err
		}
		s.sentBytes.Add(int64(end - offset))
	}
	return nil
}

// CommitAudio sends nothing. input_audio.flush finalizes the whole stream,
// not the turn: audio sent after it is ignored. The socket's own turn
// detection ends turns, as with Meta and Hopper, so the call is accepted and
// does nothing.
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

// Cancel has nothing to suppress: a transcription session produces no answer
// to interrupt, and a flush would end the session.
func (s *sttStream) Cancel(context.Context) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	return nil
}

// Close sends input_audio.flush then input_audio.end, refuses further input,
// and keeps reading until transcription.done or the drain timeout.
func (s *sttStream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		if s.closed.Load() {
			return
		}
		s.writeMu.Lock()
		s.inputClosed.Store(true)
		err := s.writeControl(ctx, "input_audio.flush")
		if err == nil {
			err = s.writeControl(ctx, "input_audio.end")
		}
		s.writeMu.Unlock()
		if err != nil {
			if s.TerminalError() == nil {
				s.closeErr = &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Mistral STT flush could not be sent", Retryable: true, Cause: err}
			}
			_ = s.abort()
			return
		}
		go s.drainThenClose()
	})
	return s.closeErr
}

func (s *sttStream) writeControl(ctx context.Context, messageType string) error {
	payload, _ := json.Marshal(controlMessage{Type: messageType})
	return s.conn.Write(ctx, websocket.MessageText, payload)
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
		// The close ends the reader's Read. readLoop then publishes the open
		// turn's final and the failure, which a slow consumer may take a
		// while to drain; the context that cancels those sends waits for it,
		// within a bound.
		select {
		case <-s.done:
		case <-time.After(closeDeliveryTimeout):
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

func (s *sttStream) writeLocked(ctx context.Context, message any) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	payload, err := json.Marshal(message)
	if err != nil {
		return err
	}
	if err := s.conn.Write(ctx, websocket.MessageText, payload); err != nil {
		if s.closed.Load() {
			return runtimepkg.ErrSessionClosed
		}
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Mistral STT socket write failed", Retryable: true, Cause: err}
	}
	return nil
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
	if s.setTerminal(err) {
		s.emit(runtimepkg.ProviderEvent{Err: err})
	}
}

// serverMessage is the union of the server frames this adapter reads.
type serverMessage struct {
	Type      string        `json:"type"`
	Session   *sessionInfo  `json:"session"`
	Text      string        `json:"text"`
	TimeS     *float64      `json:"time_s"`
	EndReason string        `json:"end_reason"`
	Error     *errorDetail  `json:"error"`
	Usage     *sessionUsage `json:"usage"`
}

type sessionInfo struct {
	RequestID string `json:"request_id"`
}

type sessionUsage struct {
	PromptAudioSeconds *float64 `json:"prompt_audio_seconds"`
}

type socketRead struct {
	messageType websocket.MessageType
	payload     []byte
	err         error
	// received is when the frame came off the socket.
	received time.Time
}

// readLoop owns the turn state and is the only sender on Events. Reads run
// on their own goroutine so the finalization deadline can fire while the
// socket is quiet, and every decision about which turn a frame belongs to
// uses the frame's arrival time, never the time readLoop got round to it.
func (s *sttStream) readLoop() {
	defer close(s.events)
	defer close(s.done)
	go func() {
		for {
			messageType, payload, err := s.conn.Read(s.ctx)
			// The stamp is taken under the lock that takeInbox holds, so a
			// frame takeInbox does not return arrived after its cutoff.
			s.inboxMu.Lock()
			if err == nil && s.inboxBytes+len(payload) > s.maxInboxBytes {
				// Events is not being drained, so readLoop may never get to
				// this entry: the failure is recorded here, where
				// TerminalError reports it at once, and the socket stops.
				err = errInboxFull
				s.overflow = &runtimepkg.ProviderError{Code: "provider_unavailable", Message: fmt.Sprintf("Mistral STT frames backed up past %d bytes because Events was not drained", s.maxInboxBytes), Retryable: false, Cause: err}
				s.setTerminal(s.overflow)
				_ = s.conn.CloseNow()
			}
			s.inboxBytes += len(payload)
			s.inbox = append(s.inbox, socketRead{messageType: messageType, payload: payload, err: err, received: time.Now()})
			s.inboxMu.Unlock()
			select {
			case s.inboxReady <- struct{}{}:
			default:
			}
			if err != nil {
				return
			}
		}
	}()

	s.opened()
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	for {
		var wake <-chan time.Time
		if due, ok := s.finalDue(); ok {
			timer.Reset(max(time.Until(due), 0))
			wake = timer.C
		}
		select {
		case <-s.inboxReady:
		case <-wake:
		}
		timer.Stop()
		reads, cutoff := s.takeInbox()
		for _, read := range reads {
			if s.process(read) {
				return
			}
		}
		// Every frame that arrived before cutoff has been processed, so a
		// deadline at or before it has passed with nothing more owed to the
		// turn.
		s.finalizeDue(cutoff)
	}
}

// finalizeDue publishes every turn whose final was due at or before at. A
// held turn promoted by one final can itself be overdue: its turn.end came
// in long before.
func (s *sttStream) finalizeDue(at time.Time) {
	for {
		due, ok := s.finalDue()
		if !ok || at.Before(due) {
			return
		}
		s.finalize(nil)
	}
}

// takeInbox returns the frames read so far and the instant they were taken:
// any frame not returned arrived after it.
func (s *sttStream) takeInbox() ([]socketRead, time.Time) {
	s.inboxMu.Lock()
	defer s.inboxMu.Unlock()
	reads := s.inbox
	s.inbox, s.inboxBytes = nil, 0
	return reads, time.Now()
}

// process handles one frame and reports whether the session is over. A
// final whose deadline passed before the frame arrived goes out first: the
// frame belongs to whatever comes next.
func (s *sttStream) process(read socketRead) bool {
	s.finalizeDue(read.received)
	if read.err != nil {
		s.finish(read.err)
		return true
	}
	if read.messageType != websocket.MessageText {
		return false
	}
	var message serverMessage
	if err := json.Unmarshal(read.payload, &message); err != nil {
		return false
	}
	return s.handle(message, read.payload, read.received)
}

// handle maps one server frame, which arrived at received, and reports
// whether the session is over.
func (s *sttStream) handle(message serverMessage, raw []byte, received time.Time) bool {
	switch message.Type {
	case "transcription.text.delta":
		s.delta(message, raw)
	case "turn.start":
		switch {
		case s.turn.ended:
			// The next turn began while this one's final is still owed. Its
			// trailing words may still be on their way, so the final keeps
			// its own rule and the new turn waits for it. A turn.start that
			// repeats one still open is ignored.
			if last := len(s.held) - 1; last < 0 || s.held[last].ended {
				s.held = append(s.held, heldTurn{raw: raw, startTimeMS: milliseconds(message.TimeS)})
			}
		case !s.turn.open:
			s.startTurn(milliseconds(message.TimeS), raw)
		}
	case "turn.end":
		switch {
		case len(s.held) > 0:
			if last := &s.held[len(s.held)-1]; !last.ended {
				last.ended, last.endedAt, last.endTimeMS = true, received, milliseconds(message.TimeS)
			}
		case s.turn.open && !s.turn.ended:
			s.turn.ended, s.turn.endedAt, s.turn.endTimeMS = true, received, milliseconds(message.TimeS)
		}
	case "turn.eager_end", "turn.resume", "session.created", "session.updated":
	case "transcription.done":
		s.completed(raw)
		return true
	case "error":
		s.fail(providerError("Mistral STT reported an error", message.Error, 0, raw))
	default:
		s.emit(runtimepkg.ProviderEvent{
			Type: protocol.EventWarning, Data: marshalData(map[string]any{"message": "ignored Mistral STT event type", "provider_type": message.Type}),
			Extensions: extension(raw),
		})
	}
	return false
}

// opened queues session.ready and the opening usage. The opening
// observation stays incomplete until transcription.done, so a socket lost
// before then settles as unresolved rather than as free audio.
func (s *sttStream) opened() {
	data := s.baseData()
	data["provider_model"] = s.model
	s.emit(runtimepkg.ProviderEvent{Type: protocol.EventSessionReady, Data: marshalData(data)})
	s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: marshalData(s.baseData()), Billing: s.billing(false)})
}

func (s *sttStream) baseData() map[string]any {
	return map[string]any{"provider_request_id": s.requestID}
}

// startTurn opens a turn on turn.start, or on a delta that arrives without
// one.
func (s *sttStream) startTurn(startMS *int64, raw []byte) {
	s.turn.open = true
	s.turn.startTimeMS = startMS
	data := s.baseData()
	data["turn_id"] = s.turn.id
	if startMS != nil {
		data["audio_start_ms"] = *startMS
	}
	s.emit(runtimepkg.ProviderEvent{Type: protocol.EventSpeechStarted, Data: marshalData(data), Extensions: extension(raw)})
}

// delta appends one append-only fragment to the turn and publishes the
// turn's text so far, the cumulative shape transcript.delta carries.
// Fragments split words (" V", "ox", "tral") and carry their own spacing, so
// they are joined verbatim.
func (s *sttStream) delta(message serverMessage, raw []byte) {
	if message.Text == "" {
		return
	}
	if !s.turn.open {
		s.startTurn(nil, raw)
	}
	s.turn.text.WriteString(message.Text)
	text := strings.TrimSpace(s.turn.text.String())
	if text == "" {
		return
	}
	s.emit(runtimepkg.ProviderEvent{Type: protocol.EventTranscriptDelta, Data: s.transcriptData(text, false), Extensions: extension(raw)})
}

// finalDue is when the ended turn's final goes out: the streaming delay
// plus finalGrace after turn.end arrived.
func (s *sttStream) finalDue() (time.Time, bool) {
	if !s.turn.ended {
		return time.Time{}, false
	}
	return s.turn.endedAt.Add(s.finalDeadline), true
}

// finalize publishes the open turn's final then speech.ended, then opens a
// held turn. An empty final is still emitted: silence legitimately completes
// a turn, and it is the evidence a caller needs to tell silence from loss.
func (s *sttStream) finalize(raw []byte) {
	if !s.turn.open {
		return
	}
	var extensions map[string]json.RawMessage
	if raw != nil {
		extensions = extension(raw)
	}
	s.emit(runtimepkg.ProviderEvent{Type: protocol.EventTranscriptFinal, Data: s.transcriptData(strings.TrimSpace(s.turn.text.String()), true), Extensions: extensions})
	data := s.baseData()
	data["turn_id"] = s.turn.id
	data["reason"] = "end_of_turn"
	if s.turn.endTimeMS != nil {
		data["audio_end_ms"] = *s.turn.endTimeMS
	}
	s.emit(runtimepkg.ProviderEvent{Type: protocol.EventSpeechEnded, Data: marshalData(data)})
	s.turn = turnState{id: s.turn.id + 1}
	if len(s.held) > 0 {
		held := s.held[0]
		s.held = s.held[1:]
		s.startTurn(held.startTimeMS, held.raw)
		if held.ended {
			s.turn.ended, s.turn.endedAt, s.turn.endTimeMS = true, held.endedAt, held.endTimeMS
		}
	}
}

// finalizeAll publishes every turn still open, a held one included, so no
// speech.started is left without its final.
func (s *sttStream) finalizeAll(raw []byte) {
	for s.turn.open {
		s.finalize(raw)
	}
}

// transcriptData carries the text and the turn's audio window where the
// turn events gave one. Voxtral reports no confidence and no word timings on
// the socket.
func (s *sttStream) transcriptData(text string, final bool) json.RawMessage {
	data := s.baseData()
	data["text"] = text
	data["is_final"] = final
	data["speech_final"] = final
	data["turn_id"] = s.turn.id
	if s.language != "" {
		data["language"] = s.language
	}
	if s.turn.startTimeMS != nil {
		data["audio_start_ms"] = *s.turn.startTimeMS
	}
	if final && s.turn.endTimeMS != nil {
		data["audio_end_ms"] = *s.turn.endTimeMS
	}
	return marshalData(data)
}

// completed handles transcription.done, which follows the flush once the
// whole stream is transcribed. A turn still open (its turn.end never came,
// or its final was still owed) is published as final, then the duration
// observation completes and the socket closes.
func (s *sttStream) completed(raw []byte) {
	s.finalizeAll(raw)
	// Taking the write lock waits out a frame in flight, so the count below
	// is final.
	s.writeMu.Lock()
	s.inputClosed.Store(true)
	s.writeMu.Unlock()
	s.doneReceived.Store(true)
	data := s.baseData()
	data["audio_bytes"] = s.sentBytes.Load()
	s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: marshalData(data), Billing: s.billing(true), Extensions: extension(raw)})
	s.writeMu.Lock()
	_ = s.conn.Close(websocket.StatusNormalClosure, "")
	s.writeMu.Unlock()
}

// billing is the session's one duration observation. transcription.done
// reports prompt_audio_seconds as a whole number of seconds, floored, so the
// complete observation is the PCM this stream sent, an exact fraction of a
// second.
func (s *sttStream) billing(complete bool) *protocol.BillingObservation {
	observation := &protocol.BillingObservation{OperationID: "stream", Model: s.model, Mode: "streaming", Language: s.language, ProviderRequestID: s.requestID, Quantities: map[string]int64{}}
	if complete {
		observation.Quantities["duration_seconds"] = s.sentBytes.Load()
		observation.QuantityDenominators = map[string]int64{"duration_seconds": s.bytesPerSecond}
		observation.Complete = true
	}
	return observation
}

// finish classifies how the socket ended. A close after Abort or after
// transcription.done is normal; a failure an error frame already explained
// is not reported twice; anything else is classified from the close status.
// A normal close after the flush but before transcription.done is not a
// clean finish: the last turn may be missing and the billed audio is
// unconfirmed. Text the socket did deliver is still published as final
// before the failure, so a lost socket does not swallow the caller's words.
func (s *sttStream) finish(err error) {
	if errors.Is(err, errInboxFull) {
		// The reader recorded the failure; it is published here unless an
		// earlier one already was.
		if s.TerminalError() == s.overflow {
			s.finalizeAll(nil)
			s.emit(runtimepkg.ProviderEvent{Err: s.overflow})
		}
		return
	}
	if s.doneReceived.Load() || s.TerminalError() != nil {
		return
	}
	if s.drainTimedOut.Load() {
		s.failClosing(&runtimepkg.ProviderError{Code: "request_timeout", Message: fmt.Sprintf("Mistral STT sent no transcription.done within %s", s.drainTimeout), Retryable: false, Cause: err})
		return
	}
	if s.closed.Load() {
		return
	}
	status := websocket.CloseStatus(err)
	reason := ""
	var closeErr websocket.CloseError
	if errors.As(err, &closeErr) {
		reason = strings.TrimSpace(closeErr.Reason)
	}
	failure := socketFailure(int(status))
	if s.inputClosed.Load() && status == websocket.StatusNormalClosure {
		failure.Message = "Mistral STT closed the session after the flush without transcription.done"
	}
	failure.Cause = err
	if status != -1 {
		failure.Extensions = map[string]json.RawMessage{extensionID: marshalData(map[string]any{"close_status": int(status), "close_reason": reason})}
	}
	s.failClosing(failure)
}

// failClosing ends a session that lost its socket. The terminal error is
// recorded first, so TerminalError holds it even if the consumer never takes
// another event; then the open turn's final and speech.ended go out, and
// the failure last.
func (s *sttStream) failClosing(err *runtimepkg.ProviderError) {
	if !s.setTerminal(err) {
		return
	}
	s.finalizeAll(nil)
	s.emit(runtimepkg.ProviderEvent{Err: err})
}

// socketFailure maps a close status onto the protocol classification.
// Mistral documents no close codes; it reports failures as error frames.
func socketFailure(code int) *runtimepkg.ProviderError {
	switch code {
	case int(websocket.StatusMessageTooBig):
		return &runtimepkg.ProviderError{Code: "input_too_large", Message: "Mistral STT rejected an oversized message (1009)"}
	case int(websocket.StatusUnsupportedData), int(websocket.StatusInvalidFramePayloadData):
		return &runtimepkg.ProviderError{Code: "invalid_request", Message: fmt.Sprintf("Mistral STT rejected the audio (%d)", code)}
	default:
		message := "Mistral STT closed the session"
		if code > 0 {
			message = fmt.Sprintf("Mistral STT closed the session (%d)", code)
		}
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: message, Retryable: true}
	}
}

// milliseconds converts a turn event's time_s, on the audio clock, or nil
// when the event did not carry one.
func milliseconds(seconds *float64) *int64 {
	if seconds == nil {
		return nil
	}
	value := int64(math.Round(*seconds * 1_000))
	return &value
}

// Outbound client frames. turn_detection is always an empty object: it
// switches the socket's turn events on with their defaults.
type sessionUpdate struct {
	Type    string        `json:"type"`
	Session sessionConfig `json:"session"`
}

type sessionConfig struct {
	AudioFormat            audioFormat `json:"audio_format"`
	TargetStreamingDelayMS int         `json:"target_streaming_delay_ms"`
	Language               string      `json:"language,omitempty"`
	ContextBias            []string    `json:"context_bias,omitempty"`
	TurnDetection          struct{}    `json:"turn_detection"`
}

type audioFormat struct {
	Encoding   string `json:"encoding"`
	SampleRate int    `json:"sample_rate"`
}

type appendMessage struct {
	Type  string `json:"type"`
	Audio string `json:"audio"`
}

type controlMessage struct {
	Type string `json:"type"`
}
