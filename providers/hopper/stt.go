package hopper

import (
	"context"
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
	STTAdapterID = "hopper.stt.v1"
	// DefaultSTTModel is the only transcription model Hopper serves. It
	// transcribes English only.
	DefaultSTTModel = "nemotron-asr"
	// DefaultTurnDetection is the socket's own default, sent explicitly.
	DefaultTurnDetection = "balanced"

	listenPath = "/stt/websocket"

	// frameMillis sizes each binary frame at the 100 ms Hopper's example
	// sends, whatever size the runtime writes.
	frameMillis = 100

	defaultSTTEventBuffer  = 64
	defaultMaxMessageBytes = 1 << 20
	// defaultSetupTimeout bounds the wait for Ready, which arrives right
	// after the handshake.
	defaultSetupTimeout = 15 * time.Second
	// defaultCloseDrainTimeout bounds how long Close waits for the flushed
	// finals and the END reply that follow the client's END.
	defaultCloseDrainTimeout = 10 * time.Second
	// handshakeErrorBytes is all coder/websocket keeps of a refused
	// handshake's body.
	handshakeErrorBytes = 1 << 10
)

// endFrame is the text frame that flushes the socket. The server answers
// with the same text once every final has gone out.
var endFrame = []byte("END")

// turnDetections are the documented turn_detection values: patient waits
// longer before ending a turn, responsive ends it sooner.
var turnDetections = map[string]struct{}{"balanced": {}, "patient": {}, "responsive": {}}

// STTConfig controls local transport bounds. Provider identity, model,
// credential and endpoint always come from the verified plan.
type STTConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
	// SetupTimeout bounds the wait for Ready.
	SetupTimeout time.Duration
	// CloseDrainTimeout bounds how long Close waits for the socket to flush
	// its finals and answer END.
	CloseDrainTimeout time.Duration
}

// STTAdapter opens Hopper streaming transcription sessions.
type STTAdapter struct {
	id                string
	httpClient        *http.Client
	eventBuffer       int
	maxMessageBytes   int64
	setupTimeout      time.Duration
	closeDrainTimeout time.Duration
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
	if config.EventBuffer < 1 || config.MaxMessageBytes < 1 || config.SetupTimeout <= 0 || config.CloseDrainTimeout <= 0 {
		return nil, errors.New("hopper stt: event buffer, message bound and timeouts must be positive")
	}
	policy, err := upstream.NewWebSocketPolicy(officialHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &STTAdapter{
		id: config.AdapterID, httpClient: config.HTTPClient, eventBuffer: config.EventBuffer,
		maxMessageBytes: config.MaxMessageBytes, setupTimeout: config.SetupTimeout,
		closeDrainTimeout: config.CloseDrainTimeout, endpointPolicy: policy,
	}, nil
}

// ID returns the adapter identifier.
func (a *STTAdapter) ID() string { return a.id }

// Open dials the socket and returns once Ready has arrived, so a refused key,
// rate or setting fails Open where the runtime can fail over rather than on
// the first WriteAudio.
func (a *STTAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindSTT {
		return nil, fmt.Errorf("hopper stt supports stt sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != ProviderName {
		return nil, fmt.Errorf("hopper stt adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("hopper stt requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model != DefaultSTTModel {
		return nil, fmt.Errorf("hopper stt does not support model %q", model)
	}
	language, err := sttLanguage(request.Options.Language)
	if err != nil {
		return nil, err
	}
	if request.Media == nil {
		return nil, errors.New("hopper stt requires input media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("hopper stt input media: %w", err)
	}
	if _, ok := sampleRates[request.Media.SampleRateHz]; !ok || request.Media.Encoding != "pcm_s16le" || request.Media.Channels != 1 {
		return nil, &runtimepkg.ProviderError{
			Code:    "unsupported_media",
			Message: fmt.Sprintf("Hopper STT listens to mono pcm_s16le at 8000, 16000, 22050, 24000, 44100 or 48000 Hz, got %s/%d Hz/%d channels", request.Media.Encoding, request.Media.SampleRateHz, request.Media.Channels),
			Hint:    "Resample the input to mono pcm_s16le at 16000 Hz.",
		}
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("hopper stt requires an api key credential")
	}
	turnDetection, err := sttTurnDetection(request.Options.STT)
	if err != nil {
		return nil, err
	}
	endpoint, err := a.listenEndpoint(request.Plan.Route.Endpoint, model, request.Media.SampleRateHz, turnDetection)
	if err != nil {
		return nil, fmt.Errorf("hopper stt endpoint: %w", err)
	}

	// The key rides the Authorization header. The api_key query parameter
	// is the fallback for clients that cannot set headers, and a key in a
	// URL ends up in logs.
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer "+strings.TrimSpace(credential.Value))
	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: a.httpClient, HTTPHeader: headers})
	if err != nil {
		return nil, handshakeError(response, err)
	}
	conn.SetReadLimit(a.maxMessageBytes)

	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &sttStream{
		conn: conn, ctx: streamCtx, cancel: cancel,
		events:         make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		setupDone:      make(chan error, 1),
		done:           make(chan struct{}),
		drainTimeout:   a.closeDrainTimeout,
		model:          model,
		language:       language,
		frameBytes:     request.Media.SampleRateHz * 2 * frameMillis / 1000,
		bytesPerSecond: int64(request.Media.SampleRateHz * 2),
	}
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
		return nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Hopper STT sent no Ready", Retryable: true, Cause: setupCtx.Err()}
	}
	// The reader sends session.ready and the opening usage itself: it is the
	// only goroutine that sends on or closes the events channel, so Open
	// neither races its close nor blocks on a buffer nobody drains yet.
	return stream, nil
}

// handshakeError classifies a refused upgrade. Hopper answers it with the
// same error envelope as its REST surface, of which the dialer keeps the
// first kilobyte.
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
	dialErr := providerError("Hopper STT connection could not be established", parseError(body), status, body)
	dialErr.Cause = cause
	return dialErr
}

// sttTurnDetection reads the caller's turn_detection from provider_options,
// refusing a value the socket would reject, and defaults to balanced.
func sttTurnDetection(options *protocol.SttOptions) (string, error) {
	value, ok := options.Provider(ProviderName)["turn_detection"]
	if !ok {
		return DefaultTurnDetection, nil
	}
	mode := strings.ToLower(strings.TrimSpace(protocol.SttOptionString(value)))
	if _, ok := turnDetections[mode]; !ok {
		return "", &runtimepkg.ProviderError{
			Code:    "invalid_request",
			Message: fmt.Sprintf("Hopper STT turn_detection must be balanced, patient or responsive, got %q", protocol.SttOptionString(value)),
			Hint:    "Set provider_options.hopper.turn_detection to balanced, patient or responsive.",
		}
	}
	return mode, nil
}

// listenEndpoint applies the shared allowlist, pins the path, and writes the
// session's query. Nothing else from provider_options reaches the URL.
func (a *STTAdapter) listenEndpoint(raw, model string, sampleRate int, turnDetection string) (string, error) {
	endpoint, err := a.endpointPolicy.Parse(raw)
	if err != nil {
		return "", err
	}
	if endpoint.Path != listenPath {
		return "", fmt.Errorf("endpoint path must be %s, got %q", listenPath, endpoint.Path)
	}
	query := url.Values{}
	query.Set("model", model)
	query.Set("sample_rate", strconv.Itoa(sampleRate))
	query.Set("turn_detection", turnDetection)
	endpoint.RawQuery = query.Encode()
	return endpoint.String(), nil
}

// sttStream is one open listen socket.
type sttStream struct {
	conn           *websocket.Conn
	ctx            context.Context
	cancel         context.CancelFunc
	events         chan runtimepkg.ProviderEvent
	setupDone      chan error
	done           chan struct{}
	drainTimeout   time.Duration
	model          string
	language       string
	frameBytes     int
	bytesPerSecond int64

	writeMu sync.Mutex
	// carry holds an odd trailing byte until the next write: frames must be
	// whole two-byte samples. Guarded by writeMu.
	carry []byte
	// sentBytes is the PCM written to the socket, which is what Hopper bills.
	sentBytes atomic.Int64

	gracefulOnce  sync.Once
	abortOnce     sync.Once
	setupOnce     sync.Once
	inputClosed   atomic.Bool
	closed        atomic.Bool
	drainTimedOut atomic.Bool
	readyOnce     atomic.Bool
	closeErr      error

	// streamID and the turn state are owned by readLoop.
	streamID string
	turnOpen bool
	turnID   int

	terminalMu  sync.Mutex
	terminalErr error
}

func (s *sttStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

// WriteAudio forwards PCM as binary frames of at most 100 ms. Pacing is the
// caller's: Hopper documents no lead limit and no idle close.
func (s *sttStream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("hopper stt audio is empty")
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
	for offset := 0; offset < len(pcm); offset += s.frameBytes {
		end := min(offset+s.frameBytes, len(pcm))
		if err := s.writeLocked(ctx, websocket.MessageBinary, pcm[offset:end]); err != nil {
			return err
		}
		s.sentBytes.Add(int64(end - offset))
	}
	return nil
}

// CommitAudio sends nothing. The socket has no finalize, and END ends the
// whole session rather than the turn, so Hopper's own turn detection decides
// where a turn ends, as with Munsit and Deepgram Flux. A live stream keeps
// sending the silence that detector needs; audio that simply stops leaves its
// turn open until Close, whose END flushes the final. Padding silence here
// would force the final, but Hopper bills every byte it receives, so the
// relay does not invent audio the caller never sent.
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
// to interrupt, and END would end the session.
func (s *sttStream) Cancel(context.Context) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	return nil
}

// Close sends END, refuses further input, and keeps reading until the server
// answers END after the flushed finals, or the drain timeout passes.
func (s *sttStream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		if s.closed.Load() {
			return
		}
		s.writeMu.Lock()
		s.inputClosed.Store(true)
		err := s.conn.Write(ctx, websocket.MessageText, endFrame)
		s.writeMu.Unlock()
		if err != nil {
			if s.TerminalError() == nil {
				s.closeErr = &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Hopper STT END could not be sent", Retryable: true, Cause: err}
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
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Hopper STT socket write failed", Retryable: true, Cause: err}
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
	s.settleSetup(err)
	if s.setTerminal(err) {
		s.emit(runtimepkg.ProviderEvent{Err: err})
	}
}

func (s *sttStream) settleSetup(err error) {
	s.setupOnce.Do(func() { s.setupDone <- err })
}

// serverMessage is the union of the server frames this adapter reads.
// Transcript events carry no type, so is_final is what marks one. A final
// flushed by END carries no words, audio_start or audio_end, so the timings
// are optional. timestamp, audio_timestamp and frames_processed are not read.
type serverMessage struct {
	Type       string          `json:"type"`
	StreamID   json.RawMessage `json:"stream_id"`
	Text       string          `json:"text"`
	IsFinal    *bool           `json:"is_final"`
	AudioStart *float64        `json:"audio_start"`
	AudioEnd   *float64        `json:"audio_end"`
	Words      []word          `json:"words"`
	Error      *errorDetail    `json:"error"`
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
		if messageType == websocket.MessageText && strings.TrimSpace(string(payload)) == string(endFrame) {
			s.ended()
			return
		}
		var message serverMessage
		if err := json.Unmarshal(payload, &message); err != nil {
			continue
		}
		s.handle(message, payload)
	}
}

func (s *sttStream) handle(message serverMessage, raw []byte) {
	switch {
	case message.Type == "Ready":
		s.opened(message, raw)
	case message.Error != nil:
		failure := providerError("Hopper STT reported an error", message.Error, 0, raw)
		s.fail(failure)
	case message.IsFinal != nil:
		s.transcript(message, *message.IsFinal, raw)
	default:
		s.emit(runtimepkg.ProviderEvent{
			Type: protocol.EventWarning, Data: marshalData(map[string]any{"message": "ignored Hopper STT event type", "provider_type": message.Type}),
			Extensions: extension(raw),
		})
	}
}

// opened handles Ready: it releases Open, then queues session.ready and the
// opening usage. The sends follow settleSetup because the runtime only starts
// draining Events once Open has returned. A repeated Ready announces nothing
// new. Ready's stream_id is the only identifier the socket gives, so it
// becomes the provider request id.
func (s *sttStream) opened(message serverMessage, raw []byte) {
	if !s.readyOnce.CompareAndSwap(false, true) {
		return
	}
	s.streamID = streamIdentifier(message.StreamID)
	s.settleSetup(nil)
	data := s.baseData()
	data["provider_model"] = s.model
	s.emit(runtimepkg.ProviderEvent{Type: protocol.EventSessionReady, Data: marshalData(data), Extensions: extension(raw)})
	// The opening observation stays incomplete until the END reply closes
	// the session, so a socket lost before then settles as unresolved rather
	// than as free audio.
	s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: marshalData(data), Billing: s.billing(false)})
}

// streamIdentifier reads stream_id, a number on the live socket, as text. An
// absent, null or implausibly long one is dropped: billing caps request ids
// at 512 bytes.
func streamIdentifier(raw json.RawMessage) string {
	var number json.Number
	if json.Unmarshal(raw, &number) == nil && len(number) <= 128 {
		return number.String()
	}
	var text string
	if json.Unmarshal(raw, &text) == nil && len(text) <= 128 {
		return strings.TrimSpace(text)
	}
	return ""
}

func (s *sttStream) baseData() map[string]any {
	data := map[string]any{"provider_request_id": s.streamID}
	if s.streamID != "" {
		data["stream_id"] = s.streamID
	}
	return data
}

// transcript maps one transcript event. A partial carries the turn's
// transcript so far and is replaced by the next event, which is the
// cumulative shape transcript.delta carries. A final is the last event of
// its turn, so it also ends the turn. Hopper sends no speech-start event;
// the turn's first words stand in for one.
func (s *sttStream) transcript(message serverMessage, final bool, raw []byte) {
	// Text arrives with a leading space.
	text := strings.TrimSpace(message.Text)
	if text != "" && !s.turnOpen {
		s.turnOpen = true
		data := s.baseData()
		data["turn_id"] = s.turnID
		setMilliseconds(data, "audio_start_ms", message.AudioStart)
		s.emit(runtimepkg.ProviderEvent{Type: protocol.EventSpeechStarted, Data: marshalData(data), Extensions: extension(raw)})
	}
	if !final {
		if text == "" {
			return
		}
		s.emit(runtimepkg.ProviderEvent{Type: protocol.EventTranscriptDelta, Data: s.transcriptData(message, text, false), Extensions: extension(raw)})
		return
	}
	// An empty final is still emitted: silence legitimately completes a turn,
	// and it is the evidence a caller needs to tell silence from loss.
	s.emit(runtimepkg.ProviderEvent{Type: protocol.EventTranscriptFinal, Data: s.transcriptData(message, text, true), Extensions: extension(raw)})
	if s.turnOpen {
		data := s.baseData()
		data["turn_id"] = s.turnID
		data["reason"] = "end_of_turn"
		setMilliseconds(data, "audio_end_ms", message.AudioEnd)
		s.emit(runtimepkg.ProviderEvent{Type: protocol.EventSpeechEnded, Data: marshalData(data), Extensions: extension(raw)})
	}
	s.turnOpen = false
	s.turnID++
}

// transcriptData carries the text, the turn's audio window when the event
// has one and, on a final, its words. Hopper's text is lowercase and
// unpunctuated, and its conf is reserved and always 0, so no confidence is
// reported.
func (s *sttStream) transcriptData(message serverMessage, text string, final bool) json.RawMessage {
	data := s.baseData()
	data["text"] = text
	data["is_final"] = final
	data["speech_final"] = final
	data["turn_id"] = s.turnID
	data["language"] = s.language
	setMilliseconds(data, "audio_start_ms", message.AudioStart)
	setMilliseconds(data, "audio_end_ms", message.AudioEnd)
	if final {
		if timings := wordTimings(message.Words); len(timings) > 0 {
			data["words"] = timings
		}
	}
	return marshalData(data)
}

// wordTimings trims each word, which may arrive with a leading space, and
// drops empty or untimed ones.
func wordTimings(words []word) []map[string]any {
	var timings []map[string]any
	for _, word := range words {
		text, start, end, ok := word.timed()
		if !ok {
			continue
		}
		timings = append(timings, map[string]any{"text": text, "start_ms": milliseconds(start), "end_ms": milliseconds(end)})
	}
	return timings
}

// setMilliseconds writes an optional time in seconds as milliseconds, and
// leaves the key out when the event did not carry it.
func setMilliseconds(data map[string]any, key string, seconds *float64) {
	if seconds != nil {
		data[key] = milliseconds(*seconds)
	}
}

// ended handles the server's END reply: every final has been delivered, so
// the billed audio is known. It completes the duration observation, then
// closes the socket.
func (s *sttStream) ended() {
	// Taking the write lock waits out a frame in flight, so the count below
	// is final.
	s.writeMu.Lock()
	s.inputClosed.Store(true)
	s.writeMu.Unlock()
	data := s.baseData()
	data["audio_bytes"] = s.sentBytes.Load()
	s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: marshalData(data), Billing: s.billing(true)})
	s.writeMu.Lock()
	_ = s.conn.Close(websocket.StatusNormalClosure, "")
	s.writeMu.Unlock()
}

// billing is the session's one duration observation. Hopper bills the audio
// bytes it receives and reports no count of its own, so the complete
// observation is the PCM this stream sent, as an exact fraction of a second.
func (s *sttStream) billing(complete bool) *protocol.BillingObservation {
	observation := &protocol.BillingObservation{OperationID: "stream", Model: s.model, Mode: "streaming", Language: s.language, ProviderRequestID: s.streamID, Quantities: map[string]int64{}}
	if complete {
		observation.Quantities["duration_seconds"] = s.sentBytes.Load()
		observation.QuantityDenominators = map[string]int64{"duration_seconds": s.bytesPerSecond}
		observation.Complete = true
	}
	return observation
}

// finish classifies how the socket ended. A close after Close or Abort is
// normal; a failure an error frame already explained is not reported twice;
// anything else is classified from the close status.
func (s *sttStream) finish(err error) {
	if s.drainTimedOut.Load() {
		s.fail(&runtimepkg.ProviderError{Code: "request_timeout", Message: fmt.Sprintf("Hopper STT did not answer END within %s", s.drainTimeout), Retryable: false, Cause: err})
		return
	}
	status := websocket.CloseStatus(err)
	if s.closed.Load() || (s.inputClosed.Load() && status == websocket.StatusNormalClosure) {
		s.settleSetup(&runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Hopper STT closed before sending Ready", Retryable: true, Cause: err})
		return
	}
	if s.TerminalError() != nil {
		return
	}
	reason := ""
	var closeErr websocket.CloseError
	if errors.As(err, &closeErr) {
		reason = strings.TrimSpace(closeErr.Reason)
	}
	failure := socketFailure(int(status), reason)
	failure.Cause = err
	if status != -1 {
		failure.Extensions = map[string]json.RawMessage{extensionID: marshalData(map[string]any{"close_status": int(status), "close_reason": reason})}
	}
	s.fail(failure)
}

// socketFailure maps a close status onto the protocol classification. Hopper
// documents 1008 for a balance that reached zero mid-session; a 1008 whose
// reason names the key is read as a refused credential instead.
func socketFailure(code int, reason string) *runtimepkg.ProviderError {
	lower := strings.ToLower(reason)
	switch code {
	case int(websocket.StatusPolicyViolation):
		if strings.Contains(lower, "key") || strings.Contains(lower, "auth") {
			return &runtimepkg.ProviderError{Code: "authentication_failed", Message: "Hopper STT refused the credential (1008)"}
		}
		return &runtimepkg.ProviderError{Code: "provider_quota_exceeded", Message: "Hopper STT closed the session for insufficient balance (1008)"}
	case int(websocket.StatusMessageTooBig):
		return &runtimepkg.ProviderError{Code: "input_too_large", Message: "Hopper STT rejected an oversized message (1009)"}
	case int(websocket.StatusUnsupportedData), int(websocket.StatusInvalidFramePayloadData):
		return &runtimepkg.ProviderError{Code: "invalid_request", Message: fmt.Sprintf("Hopper STT rejected the audio (%d)", code)}
	default:
		// 1011, a plain close the relay did not ask for, and anything
		// undocumented.
		message := "Hopper STT closed the session"
		if code > 0 {
			message = fmt.Sprintf("Hopper STT closed the session (%d)", code)
		}
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: message, Retryable: true}
	}
}

func milliseconds(seconds float64) int64 {
	return int64(math.Round(seconds * 1_000))
}
