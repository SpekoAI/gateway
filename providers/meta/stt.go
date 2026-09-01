package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
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
	// STTAdapterID is the identifier the connector registers this adapter
	// under.
	STTAdapterID = "meta.stt.v1"
	// ProviderName is the only provider this package serves.
	ProviderName = "meta"
	// DefaultModel is the one Muse Voice Transcribe model the service publishes.
	DefaultModel = "muse-voice-transcribe-1.0"

	// extensionID namespaces raw vendor payloads attached to canonical events.
	extensionID = "api.meta.ai/v1/asr"

	officialHost = "api.meta.ai"
	socketPath   = "/v1/asr/realtime"

	// Wire literals from the realtime handshake and event stream.
	modeEndpointing   = "ENDPOINTING"
	modeDiarization   = "DIARIZATION"
	partialCumulative = "CUMULATIVE"
	encoding16k       = "PCM_16KHZ"
	encoding24k       = "PCM_24KHZ"

	eventSpeechStart    = "speechStart"
	eventTranscript     = "transcript"
	eventSpeechEnd      = "speechEnd"
	eventSpeechComplete = "speechComplete"
	eventSpeaker        = "speaker"
	eventAudioProgress  = "audioProgress"
	eventError          = "error"
	clientEndStream     = "endStream"

	defaultEventBuffer     = 64
	defaultMaxMessageBytes = 4 << 20
	// defaultSetupTimeout bounds the wait for the sessionId acknowledgement.
	// The service itself allows ten seconds for the handshake to ARRIVE, so
	// a slightly longer wait for its answer keeps a slow accept from being
	// misread as a refusal.
	defaultSetupTimeout = 15 * time.Second
	// defaultCloseDrainTimeout bounds how long a closed session keeps its
	// socket open for the trailing speechComplete after endStream.
	defaultCloseDrainTimeout = 10 * time.Second
)

// sampleRateEncodings are the only rates the realtime socket listens at,
// keyed to the audioEncoding literal that declares each.
var sampleRateEncodings = map[int]string{
	16_000: encoding16k,
	24_000: encoding24k,
}

// STTConfig controls local transport bounds. Provider identity, model,
// credential and endpoint always come from the verified plan, never from here.
type STTConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
	// SetupTimeout bounds the wait for the session acknowledgement after the
	// handshake frame is sent.
	SetupTimeout time.Duration
	// CloseDrainTimeout bounds how long Close waits for the service to finish
	// the trailing turn and hang up after endStream before the socket is
	// torn down locally.
	CloseDrainTimeout time.Duration
}

// STTAdapter opens Muse Voice Transcribe realtime sessions.
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
		config.EventBuffer = defaultEventBuffer
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
		return nil, errors.New("meta transcribe: event buffer, message bound, setup timeout and close drain timeout must be positive")
	}
	policy, err := upstream.NewWebSocketPolicy(officialHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &STTAdapter{
		id: config.AdapterID, httpClient: config.HTTPClient, eventBuffer: config.EventBuffer,
		maxMessageBytes: config.MaxMessageBytes, setupTimeout: config.SetupTimeout,
		closeDrainTimeout: config.CloseDrainTimeout,
		endpointPolicy:    policy,
	}, nil
}

// ID returns the adapter identifier.
func (a *STTAdapter) ID() string { return a.id }

// Open dials the socket, sends the handshake, and returns once the service
// has acknowledged it with a sessionId — so the first WriteAudio after Open
// is always accepted. The returned stream's first event is session.ready.
func (a *STTAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindSTT {
		return nil, fmt.Errorf("meta transcribe supports stt sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != ProviderName {
		return nil, fmt.Errorf("meta transcribe adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("meta transcribe requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model == "" {
		return nil, errors.New("meta transcribe requires a model")
	}
	if request.Media == nil {
		return nil, errors.New("meta transcribe requires input media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("meta transcribe input media: %w", err)
	}
	encoding, ok := sampleRateEncodings[request.Media.SampleRateHz]
	if request.Media.Encoding != "pcm_s16le" || request.Media.Channels != 1 || !ok {
		return nil, fmt.Errorf("meta transcribe listens to mono pcm_s16le at 16000 or 24000 Hz only, got %s/%d Hz/%d channels",
			request.Media.Encoding, request.Media.SampleRateHz, request.Media.Channels)
	}
	credential := request.Plan.Route.Credential
	if credential == nil || strings.TrimSpace(credential.Value) == "" ||
		(credential.Kind != protocol.CredentialBearer && !(request.Plan.Execution.ProviderRoute == protocol.RouteSpekoRelay && credential.Kind == protocol.CredentialRelayAccess)) {
		return nil, errors.New("meta transcribe requires a bearer credential")
	}
	endpoint, err := a.parseEndpoint(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("meta transcribe endpoint: %w", err)
	}

	// The credential rides the handshake frame, which is the documented
	// channel for this socket; no header is added because none is documented
	// and an undocumented header is one more place a key could be logged.
	conn, response, err := websocket.Dial(ctx, endpoint.String(), &websocket.DialOptions{HTTPClient: a.httpClient})
	if err != nil {
		return nil, dialError(response, err)
	}
	conn.SetReadLimit(a.maxMessageBytes)

	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &sttStream{
		conn: conn, ctx: streamCtx, cancel: cancel,
		events:       make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		setupDone:    make(chan error, 1),
		done:         make(chan struct{}),
		drainTimeout: a.closeDrainTimeout,
	}
	if err := stream.writeJSON(ctx, buildHandshake(credential.Value, model, encoding, request.Options)); err != nil {
		stream.abort()
		return nil, err
	}
	go stream.readLoop()

	setupCtx, cancelSetup := context.WithTimeout(ctx, a.setupTimeout)
	defer cancelSetup()
	select {
	case err := <-stream.setupDone:
		if err != nil {
			stream.abort()
			return nil, err
		}
	case <-setupCtx.Done():
		stream.abort()
		return nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Meta transcribe did not acknowledge the session", Retryable: true, Cause: setupCtx.Err()}
	}
	sessionID := stream.session()
	stream.emit(runtimepkg.ProviderEvent{Type: protocol.EventSessionReady, Data: marshalData(map[string]string{"provider_request_id": sessionID})})
	stream.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: marshalData(map[string]string{"provider_request_id": sessionID})})
	return stream, nil
}

// parseEndpoint applies the shared upstream allowlist and then pins the path.
// The policy validates scheme, host, port and cleanliness, but only this
// package knows which path on that host it is allowed to dial.
func (a *STTAdapter) parseEndpoint(raw string) (*url.URL, error) {
	endpoint, err := a.endpointPolicy.Parse(raw)
	if err != nil {
		return nil, err
	}
	if endpoint.Path != socketPath {
		return nil, fmt.Errorf("endpoint path must be %s, got %q", socketPath, endpoint.Path)
	}
	return endpoint, nil
}

// buildHandshake is the first client frame.
//
// mode is ENDPOINTING unless the caller asked for diarization, which is its
// own mode on this socket rather than a flag. PUSH_TO_TALK is never sent: it
// withholds every final until the stream ends, which is not what a
// continuous transcription session means.
//
// partialMode is CUMULATIVE so each partial is the whole turn so far, the
// shape transcript.delta already carries for Deepgram's interim results.
// emitAudioProgress stays off: the progress events carry no transcript and
// would only be dropped.
func buildHandshake(apiKey, model, encoding string, options protocol.RequestOptions) map[string]any {
	frame := map[string]any{
		"authorization":     map[string]string{"accessToken": "Bearer " + apiKey},
		"audioEncoding":     encoding,
		"model":             model,
		"mode":              modeFor(options),
		"partialMode":       partialCumulative,
		"emitAudioProgress": false,
	}
	if bias := languageBias(options.Language); bias != "" {
		frame["languageBias"] = []string{bias}
	}
	if options.STT != nil {
		if keywords := trimmedKeywords(options.STT.Keywords); len(keywords) > 0 {
			frame["keywords"] = keywords
		}
	}
	return frame
}

func modeFor(options protocol.RequestOptions) string {
	if options.STT.Diarize() {
		return modeDiarization
	}
	return modeEndpointing
}

func trimmedKeywords(keywords []string) []string {
	trimmed := make([]string, 0, len(keywords))
	for _, keyword := range keywords {
		if keyword = strings.TrimSpace(keyword); keyword != "" {
			trimmed = append(trimmed, keyword)
		}
	}
	return trimmed
}

// sttStream is one open Muse Voice Transcribe realtime socket.
type sttStream struct {
	conn         *websocket.Conn
	ctx          context.Context
	cancel       context.CancelFunc
	events       chan runtimepkg.ProviderEvent
	setupDone    chan error
	done         chan struct{}
	drainTimeout time.Duration

	writeMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	inputClosed  atomic.Bool
	closed       atomic.Bool
	closeErr     error

	setupOnce sync.Once

	// turn state. CUMULATIVE partials each carry the whole turn so far; the
	// last one seen is the fallback final for a turn whose speechComplete
	// arrives empty, and the speaker label rides every event of its turn.
	turnMu         sync.Mutex
	sessionID      string
	turnID         int64
	lastPartial    string
	currentSpeaker string

	terminalMu  sync.Mutex
	terminalErr error
}

func (s *sttStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

// WriteAudio forwards one PCM chunk as a binary frame.
func (s *sttStream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("meta transcribe audio is empty")
	}
	if s.inputClosed.Load() || s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.inputClosed.Load() || s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if err := s.conn.Write(ctx, websocket.MessageBinary, audio); err != nil {
		return s.writeError(err)
	}
	return nil
}

// CommitAudio has no wire message on this socket short of endStream, which
// ends the session rather than the turn. The service's own endpointing
// decides the turn, so the call is accepted and does nothing.
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

// Cancel has nothing to suppress: a transcription session produces no model
// answer to interrupt. Flushing what has accumulated would invent a final the
// service never marked, so the call is accepted and does nothing.
func (s *sttStream) Cancel(context.Context) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	return nil
}

// Close tells the service the audio is over and lets it finish the trailing
// turn. The socket stays open until the service hangs up or the drain
// timeout passes, whichever is first, so a speechComplete still in flight is
// published rather than dropped; further audio is refused immediately.
func (s *sttStream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		s.inputClosed.Store(true)
		s.closeErr = s.writeJSON(ctx, map[string]string{"type": clientEndStream})
		if s.closeErr != nil {
			if errors.Is(s.closeErr, runtimepkg.ErrSessionClosed) {
				s.closeErr = nil
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
	}
	s.closed.Store(true)
	s.writeMu.Lock()
	_ = s.conn.Close(websocket.StatusNormalClosure, "")
	s.writeMu.Unlock()
	s.cancel()
}

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

func (s *sttStream) setTerminal(err error) {
	s.terminalMu.Lock()
	defer s.terminalMu.Unlock()
	if s.terminalErr == nil {
		s.terminalErr = err
	}
}

func (s *sttStream) writeJSON(ctx context.Context, value any) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.conn.Write(ctx, websocket.MessageText, payload); err != nil {
		return s.writeError(err)
	}
	return nil
}

func (s *sttStream) writeError(err error) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Meta transcribe socket write failed", Retryable: true, Cause: err}
}

func (s *sttStream) emit(event runtimepkg.ProviderEvent) {
	if event.Err != nil {
		s.setTerminal(event.Err)
	}
	select {
	case s.events <- event:
	case <-s.ctx.Done():
	}
}

func (s *sttStream) settleSetup(err error) {
	s.setupOnce.Do(func() { s.setupDone <- err })
}

func (s *sttStream) session() string {
	s.turnMu.Lock()
	defer s.turnMu.Unlock()
	return s.sessionID
}

// serverMessage is the union of every frame the socket sends: the
// acknowledgement (sessionId alone) and the typed events.
type serverMessage struct {
	SessionID        string `json:"sessionId"`
	Type             string `json:"type"`
	TurnID           int64  `json:"turnId"`
	Transcript       string `json:"transcript"`
	Final            bool   `json:"final"`
	Label            string `json:"label"`
	AudioProcessedMs int64  `json:"audioProcessedMs"`
	Message          string `json:"message"`
}

func (s *sttStream) readLoop() {
	defer close(s.events)
	defer close(s.done)
	for {
		// The frame type is not checked: the documented events are text, but
		// a service that answered JSON in a binary frame would otherwise wait
		// out the setup timeout, which is how the Gemini socket behaved.
		_, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			s.finish(err)
			return
		}
		var message serverMessage
		if err := json.Unmarshal(payload, &message); err != nil {
			continue
		}
		s.handle(message, payload)
	}
}

// handle dispatches one server frame.
func (s *sttStream) handle(message serverMessage, raw []byte) {
	if message.Type == "" {
		if message.SessionID != "" {
			s.turnMu.Lock()
			s.sessionID = message.SessionID
			s.turnMu.Unlock()
			s.settleSetup(nil)
		}
		return
	}
	switch message.Type {
	case eventError:
		err := &runtimepkg.ProviderError{
			Code: "provider_rejected_request", Message: "Meta transcribe reported an error",
			Retryable: false, Extensions: extension(raw),
		}
		if message.Message != "" {
			err.Hint = message.Message
		}
		s.settleSetup(err)
		s.emit(runtimepkg.ProviderEvent{Err: err})
	case eventSpeechStart:
		s.turnMu.Lock()
		s.turnID = message.TurnID
		s.lastPartial = ""
		s.turnMu.Unlock()
		s.emit(runtimepkg.ProviderEvent{Type: protocol.EventSpeechStarted, Data: s.turnData(message, nil), Extensions: extension(raw)})
	case eventSpeaker:
		s.turnMu.Lock()
		s.currentSpeaker = message.Label
		s.turnMu.Unlock()
	case eventTranscript:
		s.handleTranscript(message, raw)
	case eventSpeechEnd:
		s.emit(runtimepkg.ProviderEvent{Type: protocol.EventSpeechEnded, Data: s.turnData(message, map[string]any{"reason": "end_of_turn"}), Extensions: extension(raw)})
	case eventSpeechComplete:
		s.flushTurn(message, raw)
	case eventAudioProgress:
		// Progress carries no transcript; it is not requested and is ignored
		// if it arrives anyway.
	}
}

// handleTranscript publishes a partial as a delta. A `final: true` transcript
// is documented for PUSH_TO_TALK only, which this adapter never selects, but
// a service that sends one anyway is marking the turn's end and is honored
// as such rather than dropped.
func (s *sttStream) handleTranscript(message serverMessage, raw []byte) {
	text := strings.TrimSpace(message.Transcript)
	if text == "" {
		return
	}
	if message.Final {
		s.flushTurn(message, raw)
		return
	}
	s.turnMu.Lock()
	s.lastPartial = text
	s.turnMu.Unlock()
	s.emit(runtimepkg.ProviderEvent{Type: protocol.EventTranscriptDelta, Data: s.transcriptData(text, false, message), Extensions: extension(raw)})
}

// flushTurn publishes the turn's final. speechComplete carries the cleaned
// text; a turn whose completion arrives empty falls back to the last
// cumulative partial, and a turn that carried no text at all publishes
// nothing, because an empty final is indistinguishable from silence to a
// caller.
func (s *sttStream) flushTurn(message serverMessage, raw []byte) {
	s.turnMu.Lock()
	text := strings.TrimSpace(message.Transcript)
	if text == "" {
		text = s.lastPartial
	}
	s.lastPartial = ""
	s.turnMu.Unlock()
	if text == "" {
		return
	}
	s.emit(runtimepkg.ProviderEvent{Type: protocol.EventTranscriptFinal, Data: s.transcriptData(text, true, message), Extensions: extension(raw)})
	s.turnMu.Lock()
	s.currentSpeaker = ""
	s.turnMu.Unlock()
}

func (s *sttStream) transcriptData(text string, isFinal bool, message serverMessage) json.RawMessage {
	return s.turnData(message, map[string]any{"text": text, "is_final": isFinal})
}

// turnData is the common envelope: the session id as provider_request_id,
// the turn index when the frame names one, the processed-audio position, and
// the current speaker label when diarization has named one.
func (s *sttStream) turnData(message serverMessage, fields map[string]any) json.RawMessage {
	s.turnMu.Lock()
	data := map[string]any{"provider_request_id": s.sessionID}
	turnID := message.TurnID
	if turnID == 0 {
		turnID = s.turnID
	}
	if turnID != 0 {
		data["turn_id"] = turnID
	}
	if s.currentSpeaker != "" {
		data["speaker"] = s.currentSpeaker
	}
	s.turnMu.Unlock()
	if message.AudioProcessedMs != 0 {
		data["audio_end_ms"] = message.AudioProcessedMs
	}
	for key, value := range fields {
		data[key] = value
	}
	return marshalData(data)
}

// finish classifies how the socket ended. A clean close after the caller
// closed is normal; anything else is a provider failure whose close code is
// the only diagnostic the service offers.
func (s *sttStream) finish(err error) {
	if isNormalClose(err) || (s.closed.Load() && s.ctx.Err() != nil) || (s.inputClosed.Load() && isNormalClose(err)) {
		return
	}
	status := websocket.CloseStatus(err)
	failure := &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Meta transcribe closed the session", Retryable: true, Cause: err}
	switch status {
	case websocket.StatusPolicyViolation, websocket.StatusUnsupportedData, websocket.StatusInvalidFramePayloadData:
		failure = &runtimepkg.ProviderError{Code: "provider_rejected_request", Message: "Meta transcribe rejected the session configuration", Retryable: false, Cause: err}
	case websocket.StatusInternalError:
		failure = &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Meta transcribe reported an internal error", Retryable: true, Cause: err}
	}
	if status != -1 {
		failure.Extensions = map[string]json.RawMessage{extensionID: marshalData(map[string]any{"close_status": int(status)})}
	}
	s.settleSetup(failure)
	s.emit(runtimepkg.ProviderEvent{Err: failure})
}

func dialError(response *http.Response, err error) error {
	status := 0
	if response != nil {
		status = response.StatusCode
	}
	code := "provider_unavailable"
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		code = "provider_authentication_failed"
	case status == http.StatusBadRequest || status == http.StatusNotFound:
		code = "provider_rejected_request"
	case status == http.StatusTooManyRequests:
		code = "provider_rate_limited"
	}
	return &runtimepkg.ProviderError{
		Code: code, Message: "Meta transcribe connection could not be established",
		Retryable: status == 0 || status == http.StatusTooManyRequests || status >= 500, ProviderStatus: status, Cause: err,
	}
}

func extension(raw []byte) map[string]json.RawMessage {
	return map[string]json.RawMessage{extensionID: append(json.RawMessage(nil), raw...)}
}

func marshalData(value any) json.RawMessage {
	payload, _ := json.Marshal(value)
	return payload
}

func isNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}
