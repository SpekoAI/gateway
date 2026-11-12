package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
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
	// finalUsageTimeout bounds how long the read loop waits to hand the
	// terminal usage observation to a consumer. The consumer normally reads
	// until the channel closes, so the wait is immediate; the bound only stops
	// a consumer that already walked away from pinning the goroutine.
	finalUsageTimeout = 2 * time.Second
	// processedCoverageSlackMs is how far the service's processed-audio
	// position may trail the audio actually forwarded and still count as
	// having processed all of it. The service reports progress in 80 ms
	// steps, so a tail shorter than one step is never reported; the slack
	// only ever lowers the bill, because the billed figure is the reported
	// position, never the forwarded total.
	processedCoverageSlackMs = 100

	// paddingFrame is the silence one padding write carries and the padding
	// loop's tick, so padding runs at exactly real time.
	paddingFrame = 100 * time.Millisecond
	// paddingGap is how long the caller may send no audio before padding
	// starts: above ordinary network jitter, so silence is not spliced into
	// speech that is merely late, and well under the ~2 s without audio after
	// which the service was seen closing a session.
	paddingGap = 400 * time.Millisecond
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
	// now and newTicker drive silence padding; tests replace them.
	now       func() time.Time
	newTicker func(time.Duration) (<-chan time.Time, func())
	// afterPad, when set, is told after every padding iteration whether it
	// wrote silence. Tests only.
	afterPad func(wrote bool)
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
		now:               time.Now,
		newTicker: func(period time.Duration) (<-chan time.Time, func()) {
			ticker := time.NewTicker(period)
			return ticker.C, ticker.Stop
		},
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
		now:          a.now,
		afterPad:     a.afterPad,
		padKick:      make(chan struct{}, 1),
		// mono pcm_s16le, validated above
		bytesPerSecond: int64(request.Media.SampleRateHz) * 2,
		billing:        protocol.BillingObservation{OperationID: "session", Model: model, Mode: "streaming", Quantities: map[string]int64{}},
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
	stream.writeMu.Lock()
	stream.padFrom = a.now().Add(paddingGap)
	stream.writeMu.Unlock()
	stream.padFrame = make([]byte, stream.bytesPerSecond*int64(paddingFrame)/int64(time.Second))
	go stream.padLoop(a.newTicker(paddingFrame))
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
// Progress measures processed audio for hosted billing, independently of transcripts.
func buildHandshake(apiKey, model, encoding string, options protocol.RequestOptions) map[string]any {
	frame := map[string]any{
		"authorization":     map[string]string{"accessToken": "Bearer " + apiKey},
		"audioEncoding":     encoding,
		"model":             model,
		"mode":              modeFor(options),
		"partialMode":       partialCumulative,
		"emitAudioProgress": true,
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
	// Billing state is owned by readLoop.
	billing             protocol.BillingObservation
	billingInvalid      bool
	billingDurationSeen bool
	// bytesPerSecond converts forwarded PCM bytes to audio time. The billed
	// duration is never allowed above the audio this stream actually wrote
	// to the service.
	bytesPerSecond int64
	forwardedBytes atomic.Int64
	// endStreamSent records that Close delivered endStream: the caller ended
	// the input, so no further audio can reach the service.
	endStreamSent atomic.Bool
	// aborted records a local abort (caller or connector gave up on the
	// session); its evidence is never promoted to complete.
	aborted      atomic.Bool
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

	// Silence padding. The service closes a stream whose audio stops, and its
	// endpointing only ends a turn while audio keeps arriving, so padLoop
	// fills every gap in the caller's audio with PCM silence at real time.
	// padFrom and audioUntil are guarded by writeMu, the lock every socket
	// write takes, so a padding decision and its write never interleave with
	// caller audio.
	now      func() time.Time
	afterPad func(wrote bool)
	padFrame []byte
	// padKick asks the loop to pad at once (CommitAudio).
	padKick chan struct{}
	// padFrom is the earliest instant padding may be written.
	padFrom time.Time
	// audioUntil is the wall-clock instant by which everything written so
	// far, caller audio and padding, has played out at real time.
	audioUntil time.Time
	// paddedBytes is the silence written. It is never billed: see
	// billedObservation.
	paddedBytes atomic.Int64
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
	// Counted before the write so the cap can never trail what the service
	// has received: a reply to this frame may be read before Write returns.
	// A frame whose write fails ends the session, whose evidence then stays
	// partial, and the service cannot report audio it never got.
	s.forwardedBytes.Add(int64(len(audio)))
	now := s.now()
	s.padFrom = now.Add(paddingGap)
	s.audioUntil = laterOf(s.audioUntil, now).Add(time.Duration(int64(len(audio)) * int64(time.Second) / s.bytesPerSecond))
	if err := s.conn.Write(ctx, websocket.MessageBinary, audio); err != nil {
		return s.writeError(err)
	}
	return nil
}

// CommitAudio has no wire message on this socket short of endStream, which
// ends the session rather than the turn. The service's own endpointing
// decides the turn, and it only does so while audio keeps arriving, so a
// commit starts silence padding at once instead of after paddingGap.
func (s *sttStream) CommitAudio(context.Context) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	s.writeMu.Lock()
	s.padFrom = s.now()
	s.writeMu.Unlock()
	select {
	case s.padKick <- struct{}{}:
	default:
	}
	return nil
}

// padLoop writes silence while the caller sends none, one frame per tick,
// until the input is closed or the session ends.
func (s *sttStream) padLoop(ticks <-chan time.Time, stop func()) {
	defer stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.done:
			return
		case <-ticks:
		case <-s.padKick:
		}
		wrote, open := s.padOnce()
		if s.afterPad != nil {
			s.afterPad(wrote)
		}
		if !open {
			return
		}
	}
}

// padOnce writes one frame of silence when the caller's audio has a gap and
// everything already written has played out, so padding never runs ahead of
// the clock by more than the frame it writes. It reports whether it wrote,
// and false for open once the input is closed or a write fails; a failed
// write is left to the read loop, which reports the socket's end.
func (s *sttStream) padOnce() (wrote, open bool) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.inputClosed.Load() || s.closed.Load() {
		return false, false
	}
	now := s.now()
	if now.Before(s.padFrom) || s.audioUntil.Sub(now) >= paddingFrame/2 {
		return false, true
	}
	if err := s.conn.Write(s.ctx, websocket.MessageBinary, s.padFrame); err != nil {
		return false, false
	}
	s.paddedBytes.Add(int64(len(s.padFrame)))
	s.audioUntil = laterOf(s.audioUntil, now).Add(paddingFrame)
	return true, true
}

func laterOf(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
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
		s.endStreamSent.Store(true)
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
		s.aborted.Store(true)
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
			s.billing.ProviderRequestID = message.SessionID
			s.emitBilling(false)
			s.settleSetup(nil)
		}
		return
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(raw, &fields)
	if _, present := fields["audioProcessedMs"]; present {
		s.observeProcessedAudio(raw)
		s.emitBilling(false)
	}
	switch message.Type {
	case eventError:
		s.billingInvalid = true
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
		// Billing was recorded above; no public transcript event is needed.
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
	if text != "" {
		s.emit(runtimepkg.ProviderEvent{Type: protocol.EventTranscriptFinal, Data: s.transcriptData(text, true, message), Extensions: extension(raw)})
	}
	// The speaker label belongs to the turn that just ended, whether or not it
	// carried text: a silent diarized turn must not lend its label to the next.
	s.turnMu.Lock()
	s.currentSpeaker = ""
	s.turnMu.Unlock()
}

func (s *sttStream) transcriptData(text string, isFinal bool, message serverMessage) json.RawMessage {
	return s.turnData(message, map[string]any{"text": text, "is_final": isFinal, "speech_final": isFinal})
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

// observeProcessedAudio folds one frame's audioProcessedMs into the session's
// cumulative billing evidence.
//
// The field rides more than audioProgress: speechStart, transcript, speechEnd
// and speechComplete carry it too, as the position of the turn they describe
// (it is published as audio_end_ms). A turn event therefore routinely names a
// position BEHIND the latest audioProgress — speechComplete arrives after the
// turn's cleanup pass, by which time progress has moved on. That is not
// contradictory evidence: every value is a point the service has processed,
// so the session total is the largest one seen. A smaller valid value is
// ignored instead of being merged, which would fail as a regression and mark
// the whole session unbillable. A value that is not a non-negative number is
// still malformed evidence and still invalidates the session.
func (s *sttStream) observeProcessedAudio(raw []byte) {
	next := metering.Duration("session", s.billing.Model, "streaming", raw, 1, "audioProcessedMs")
	next.ProviderRequestID = s.billing.ProviderRequestID
	next.Complete = false
	if _, measured := next.Quantities["duration_seconds"]; !measured {
		s.billingInvalid = true
		return
	}
	if s.billingDurationSeen && durationOf(*next).Cmp(durationOf(s.billing)) < 0 {
		return
	}
	merged, err := protocol.MergeBillingObservation(s.billing, *next)
	if err != nil {
		s.billingInvalid = true
		return
	}
	s.billing = merged
	s.billingDurationSeen = true
}

// durationOf is an observation's duration_seconds as an exact number of
// seconds, so integer-millisecond and fractional reports compare correctly.
func durationOf(o protocol.BillingObservation) *big.Rat {
	return new(big.Rat).SetFrac(big.NewInt(o.Quantities["duration_seconds"]), big.NewInt(o.QuantityDenominator("duration_seconds")))
}

// forwardedDuration is the audio this stream has written to the service, in
// seconds.
func (s *sttStream) forwardedDuration() *big.Rat {
	if s.bytesPerSecond <= 0 {
		return new(big.Rat)
	}
	return new(big.Rat).SetFrac(big.NewInt(s.forwardedBytes.Load()), big.NewInt(s.bytesPerSecond))
}

// billedObservation is the evidence as published: the service's processed
// position, capped at the audio actually forwarded. The service cannot have
// processed audio it was never sent, so the cap only matters if its counter
// ever ran ahead of the input — and then the customer pays for what was sent.
//
// forwardedBytes is the caller's audio only. The service processes, and
// charges for, the silence padLoop writes too, so its position runs past the
// caller's audio by the padding; the cap holds the bill to what the caller
// sent and the relay absorbs the padding. The relay's stt_duration budget is
// debited by the connector on caller audio before WriteAudio and never sees
// padding either.
func (s *sttStream) billedObservation() protocol.BillingObservation {
	o := s.billing.Clone()
	if _, ok := o.Quantities["duration_seconds"]; !ok {
		return o
	}
	if forwarded := s.forwardedDuration(); durationOf(o).Cmp(forwarded) > 0 {
		denominator := o.QuantityDenominator("duration_seconds")
		capped := new(big.Int).Quo(new(big.Int).Mul(forwarded.Num(), big.NewInt(denominator)), forwarded.Denom())
		o.Quantities["duration_seconds"] = capped.Int64()
	}
	return o
}

// processedCoversForwarded reports whether the service's processed position
// has reached the forwarded audio, within one progress step. Once endStream
// is out no further audio can be forwarded, so this is the point at which the
// reported figure is final for everything that was served. The position
// includes padding, so it can reach the caller's total before the service has
// processed the caller's last frames; the billed figure is capped at that
// total, so from then on it cannot change, which is what final means here.
func (s *sttStream) processedCoversForwarded() bool {
	if !s.billingDurationSeen {
		return false
	}
	slack := new(big.Rat).SetFrac64(processedCoverageSlackMs, 1000)
	return new(big.Rat).Add(durationOf(s.billing), slack).Cmp(s.forwardedDuration()) >= 0
}

func (s *sttStream) billingEvent(complete bool) runtimepkg.ProviderEvent {
	o := s.billedObservation()
	o.Complete = complete && s.billingDurationSeen && !s.billingInvalid
	return runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Billing: &o}
}

func (s *sttStream) emitBilling(complete bool) {
	s.emit(s.billingEvent(complete))
}

// emitFinalBilling hands over the terminal observation even when the stream
// context is already cancelled — the drain timeout cancels it before the read
// loop unwinds — because dropping it would leave the consumer holding the
// last interim, incomplete snapshot.
func (s *sttStream) emitFinalBilling(complete bool) {
	timer := time.NewTimer(finalUsageTimeout)
	defer timer.Stop()
	select {
	case s.events <- s.billingEvent(complete):
	case <-timer.C:
	}
}

// finish classifies how the socket ended. A clean close after the caller
// closed is normal; anything else is a provider failure whose close code is
// the only diagnostic the service offers.
func (s *sttStream) finish(err error) {
	providerFailed := !(isNormalClose(err) || (s.closed.Load() && s.ctx.Err() != nil))
	// The total is final when the service hung up cleanly after endStream, or
	// when the caller ended the input (endStream delivered) and the service's
	// processed position had already reached every byte forwarded — which is
	// the client-close case where the service does not hang up within the
	// drain window and the socket is closed locally. A provider failure, an
	// abort, or a position still short of the forwarded audio keeps the
	// evidence partial.
	cleanClose := websocket.CloseStatus(err) == websocket.StatusNormalClosure && s.inputClosed.Load() && !s.closed.Load()
	covered := s.endStreamSent.Load() && !s.aborted.Load() && !providerFailed && s.processedCoversForwarded()
	s.emitFinalBilling(cleanClose || covered)
	if !providerFailed {
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
