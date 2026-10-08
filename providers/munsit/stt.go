package munsit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
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
	STTAdapterID = "munsit.stt.v1"
	// DefaultSTTModel is the Arabic-only model; the socket reports it as
	// munsit-v2.
	DefaultSTTModel = "munsit"
	// CodeSwitchSTTModel transcribes Arabic, English and speech that mixes
	// them; the socket reports it as munsit-en-ar-v1.
	CodeSwitchSTTModel = "munsit-en-ar"

	listenPath = "/api/v1/listen"

	// frameMillis sizes each binary frame inside the socket's 20 to 200 ms
	// window, whatever size the runtime writes.
	frameMillis = 100

	defaultSTTEventBuffer  = 64
	defaultMaxMessageBytes = 1 << 20
	// defaultSetupTimeout bounds the wait for the opening Metadata, which
	// arrives a few hundred milliseconds after the handshake.
	defaultSetupTimeout = 15 * time.Second
	// defaultCloseDrainTimeout bounds how long Close waits for the flushed
	// final and the billing Metadata that follow CloseStream.
	defaultCloseDrainTimeout = 10 * time.Second
	// defaultKeepAliveInterval is the silence after which a KeepAlive goes
	// out. The socket closes 1011 after 12 s without input.
	defaultKeepAliveInterval = 5 * time.Second
	// defaultMaxAudioLead keeps buffered input well inside the 60 s the
	// socket lets audio run ahead of real time before it closes 4008.
	defaultMaxAudioLead = 30 * time.Second
)

var (
	keepAliveFrame   = []byte(`{"type":"KeepAlive"}`)
	closeStreamFrame = []byte(`{"type":"CloseStream"}`)
)

// sttSampleRates are the only rates the socket accepts.
var sttSampleRates = map[int]struct{}{8_000: {}, 16_000: {}}

// sttProviderKeys are the documented listen parameters a caller may set
// through provider_options. Anything else is never written onto the URL.
var sttProviderKeys = map[string]struct{}{"endpointing": {}, "smart_turn": {}}

// sttLanguage maps a session language onto the `language` value a model
// accepts, refusing any other at Open. munsit reads Arabic only, so an empty
// language is sent as ar; munsit-en-ar detects between ar and en when the
// language is empty or auto.
func sttLanguage(model, language string) (string, error) {
	base := baseLanguage(language)
	switch model {
	case DefaultSTTModel:
		if base == "" || base == "ar" {
			return "ar", nil
		}
	case CodeSwitchSTTModel:
		switch base {
		case "", "auto":
			return "auto", nil
		case "ar", "en":
			return base, nil
		}
	default:
		return "", fmt.Errorf("munsit stt does not support model %q", model)
	}
	return "", &runtimepkg.ProviderError{Code: "unsupported_language", Message: fmt.Sprintf("Munsit %s does not transcribe %q", model, language)}
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
	// SetupTimeout bounds the wait for the opening Metadata.
	SetupTimeout time.Duration
	// CloseDrainTimeout bounds how long Close waits for the socket to flush
	// its final and close after CloseStream.
	CloseDrainTimeout time.Duration
	// KeepAliveInterval is the input silence after which a KeepAlive is sent.
	KeepAliveInterval time.Duration
	// MaxAudioLead bounds how far written audio may run ahead of wall-clock
	// time; WriteAudio waits past it.
	MaxAudioLead time.Duration
}

// STTAdapter opens Munsit streaming transcription sessions.
type STTAdapter struct {
	id                string
	httpClient        *http.Client
	eventBuffer       int
	maxMessageBytes   int64
	setupTimeout      time.Duration
	closeDrainTimeout time.Duration
	keepAliveInterval time.Duration
	maxAudioLead      time.Duration
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
	if config.MaxAudioLead == 0 {
		config.MaxAudioLead = defaultMaxAudioLead
	}
	if config.EventBuffer < 1 || config.MaxMessageBytes < 1 || config.SetupTimeout <= 0 || config.CloseDrainTimeout <= 0 || config.KeepAliveInterval <= 0 || config.MaxAudioLead <= 0 {
		return nil, errors.New("munsit stt: event buffer, message bound, timeouts, keepalive interval and audio lead must be positive")
	}
	policy, err := upstream.NewWebSocketPolicy(officialHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &STTAdapter{
		id: config.AdapterID, httpClient: config.HTTPClient, eventBuffer: config.EventBuffer,
		maxMessageBytes: config.MaxMessageBytes, setupTimeout: config.SetupTimeout,
		closeDrainTimeout: config.CloseDrainTimeout, keepAliveInterval: config.KeepAliveInterval,
		maxAudioLead: config.MaxAudioLead, endpointPolicy: policy,
	}, nil
}

// ID returns the adapter identifier.
func (a *STTAdapter) ID() string { return a.id }

// Open dials the socket and returns once the opening Metadata has arrived, so
// a refused key, model, language or rate fails Open where the runtime can
// fail over rather than on the first WriteAudio.
func (a *STTAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindSTT {
		return nil, fmt.Errorf("munsit stt supports stt sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != ProviderName {
		return nil, fmt.Errorf("munsit stt adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("munsit stt requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	language, err := sttLanguage(model, request.Options.Language)
	if err != nil {
		return nil, err
	}
	if request.Media == nil {
		return nil, errors.New("munsit stt requires input media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("munsit stt input media: %w", err)
	}
	if _, ok := sttSampleRates[request.Media.SampleRateHz]; !ok || request.Media.Encoding != "pcm_s16le" || request.Media.Channels != 1 {
		return nil, &runtimepkg.ProviderError{
			Code:    "unsupported_media",
			Message: fmt.Sprintf("Munsit STT listens to mono pcm_s16le at 8000 or 16000 Hz only, got %s/%d Hz/%d channels", request.Media.Encoding, request.Media.SampleRateHz, request.Media.Channels),
			Hint:    "Resample the input to mono pcm_s16le at 16000 Hz.",
		}
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("munsit stt requires an api key credential")
	}
	endpoint, err := a.listenEndpoint(request.Plan.Route.Endpoint, model, language, request.Media.SampleRateHz, request.Options.STT)
	if err != nil {
		return nil, fmt.Errorf("munsit stt endpoint: %w", err)
	}

	headers := make(http.Header)
	headers.Set(apiKeyHeader, strings.TrimSpace(credential.Value))
	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: a.httpClient, HTTPHeader: headers})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		dialErr := providerError("Munsit STT connection could not be established", restError{}, status, nil)
		dialErr.Cause = err
		return nil, dialErr
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
		maxAudioLead:      a.maxAudioLead,
		model:             model,
		language:          language,
		frameBytes:        request.Media.SampleRateHz * 2 * frameMillis / 1000,
		bytesPerSecond:    request.Media.SampleRateHz * 2,
	}
	stream.lastWrite.Store(time.Now().UnixNano())
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
		return nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Munsit STT sent no session Metadata", Retryable: true, Cause: setupCtx.Err()}
	}
	// The reader sends session.ready and the opening usage itself: it is the
	// only goroutine that sends on or closes the events channel, so Open
	// neither races its close nor blocks on a buffer nobody drains yet.
	go stream.keepAlive()
	return stream, nil
}

// listenEndpoint applies the shared allowlist, pins the path, and writes the
// session's query. Caller provider_options are copied only for the
// documented listen parameters.
func (a *STTAdapter) listenEndpoint(raw, model, language string, sampleRate int, options *protocol.SttOptions) (string, error) {
	endpoint, err := a.endpointPolicy.Parse(raw)
	if err != nil {
		return "", err
	}
	if endpoint.Path != listenPath {
		return "", fmt.Errorf("endpoint path must be %s, got %q", listenPath, endpoint.Path)
	}
	query := url.Values{}
	for _, key := range options.ProviderKeys(ProviderName) {
		if _, ok := sttProviderKeys[key]; ok {
			query.Set(key, protocol.SttOptionString(options.Provider(ProviderName)[key]))
		}
	}
	query.Set("encoding", "linear16")
	query.Set("sample_rate", strconv.Itoa(sampleRate))
	query.Set("model", model)
	query.Set("language", language)
	query.Set("interim_results", "true")
	endpoint.RawQuery = query.Encode()
	return endpoint.String(), nil
}

// sttStream is one open listen socket.
type sttStream struct {
	conn              *websocket.Conn
	ctx               context.Context
	cancel            context.CancelFunc
	events            chan runtimepkg.ProviderEvent
	setupDone         chan error
	done              chan struct{}
	drainTimeout      time.Duration
	keepAliveInterval time.Duration
	maxAudioLead      time.Duration
	model             string
	language          string
	frameBytes        int
	bytesPerSecond    int

	writeMu sync.Mutex
	// carry holds an odd trailing byte until the next write: frames must be
	// whole two-byte samples. audioStart and audioBytes clock the audio sent
	// against wall time. All three are guarded by writeMu.
	carry      []byte
	audioStart time.Time
	audioBytes int64
	// lastWrite is the UnixNano of the last frame sent, read by keepAlive.
	lastWrite atomic.Int64

	gracefulOnce  sync.Once
	abortOnce     sync.Once
	setupOnce     sync.Once
	inputClosed   atomic.Bool
	closed        atomic.Bool
	drainTimedOut atomic.Bool
	readyOnce     atomic.Bool
	closeErr      error

	stateMu    sync.Mutex
	sessionID  string
	serveModel string

	terminalMu  sync.Mutex
	terminalErr error
}

func (s *sttStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

// WriteAudio forwards PCM as binary frames of at most 100 ms. Input that runs
// more than the configured lead ahead of real time waits, because the socket
// closes 4008 once audio is 60 s ahead.
func (s *sttStream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("munsit stt audio is empty")
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
		if err := s.pace(ctx); err != nil {
			return err
		}
		if err := s.writeLocked(ctx, websocket.MessageBinary, pcm[offset:end]); err != nil {
			return err
		}
		s.audioBytes += int64(end - offset)
	}
	return nil
}

// pace waits until the audio already sent is no more than maxAudioLead ahead
// of the wall time since the first frame. Live input never waits.
func (s *sttStream) pace(ctx context.Context) error {
	if s.audioStart.IsZero() {
		s.audioStart = time.Now()
		return nil
	}
	sent := time.Duration(s.audioBytes) * time.Second / time.Duration(s.bytesPerSecond)
	wait := sent - time.Since(s.audioStart) - s.maxAudioLead
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ctx.Done():
		return runtimepkg.ErrSessionClosed
	}
}

// CommitAudio sends nothing. The socket has no Finalize, and CloseStream ends
// the whole session rather than the turn, so Munsit's own endpointer decides
// where a turn ends, as with Deepgram Flux and Meta. A live stream keeps
// sending the silence that endpointer needs; audio that simply stops leaves
// its turn open until Close, whose CloseStream flushes the final. Padding
// silence here would force the final, but Munsit bills every second it
// receives, so the relay does not invent audio the caller never sent.
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
// to interrupt, and CloseStream would end the session.
func (s *sttStream) Cancel(context.Context) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	return nil
}

// Close sends CloseStream, refuses further input, and keeps reading until the
// socket closes after the flushed final and the billing Metadata, or the
// drain timeout passes.
func (s *sttStream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		if s.closed.Load() {
			return
		}
		s.writeMu.Lock()
		s.inputClosed.Store(true)
		err := s.conn.Write(ctx, websocket.MessageText, closeStreamFrame)
		s.writeMu.Unlock()
		if err != nil {
			if s.TerminalError() == nil {
				s.closeErr = &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Munsit STT CloseStream could not be sent", Retryable: true, Cause: err}
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
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Munsit STT socket write failed", Retryable: true, Cause: err}
	}
	s.lastWrite.Store(time.Now().UnixNano())
	return nil
}

// keepAlive sends a KeepAlive whenever no frame has gone out for the
// interval, so a caller that pauses its audio does not hit the socket's 12 s
// idle close. It stops once input is closed.
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
// Error event or as the close that follows it.
func (s *sttStream) fail(err *runtimepkg.ProviderError) {
	s.settleSetup(err)
	if s.setTerminal(err) {
		s.emit(runtimepkg.ProviderEvent{Err: err})
	}
}

func (s *sttStream) settleSetup(err error) {
	s.setupOnce.Do(func() { s.setupDone <- err })
}

// serverMessage is the union of the server events this adapter reads.
type serverMessage struct {
	Type               string          `json:"type"`
	SessionID          string          `json:"session_id"`
	Model              string          `json:"model"`
	AudioSecondsBilled json.RawMessage `json:"audio_seconds_billed"`
	TurnCount          *int            `json:"turn_count"`
	TurnID             int             `json:"turn_id"`
	TS                 float64         `json:"ts"`
	LastWordEnd        float64         `json:"last_word_end"`
	Transcript         string          `json:"transcript"`
	IsFinal            bool            `json:"is_final"`
	SpeechFinal        bool            `json:"speech_final"`
	Language           string          `json:"language"`
	Confidence         *float64        `json:"confidence"`
	Words              []struct {
		Start float64 `json:"start"`
		End   float64 `json:"end"`
	} `json:"words"`
	Code        int    `json:"code"`
	Message     string `json:"message"`
	Recoverable bool   `json:"recoverable"`
}

func (s *sttStream) readLoop() {
	defer close(s.events)
	defer close(s.done)
	for {
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

func (s *sttStream) handle(message serverMessage, raw []byte) {
	switch message.Type {
	case "Metadata":
		s.stateMu.Lock()
		if message.SessionID != "" {
			s.sessionID = message.SessionID
		}
		if message.Model != "" {
			s.serveModel = message.Model
		}
		s.stateMu.Unlock()
		if len(message.AudioSecondsBilled) == 0 {
			s.opened(raw)
			return
		}
		// The closing Metadata, sent after CloseStream.
		data := s.baseData()
		data["audio_seconds_billed"] = message.AudioSecondsBilled
		if message.TurnCount != nil {
			data["turn_count"] = *message.TurnCount
		}
		s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: marshalData(data), Billing: s.billing(raw), Extensions: extension(raw)})
	case "SpeechStarted":
		s.emit(runtimepkg.ProviderEvent{Type: protocol.EventSpeechStarted, Data: marshalData(map[string]any{"audio_start_ms": milliseconds(message.TS)}), Extensions: extension(raw)})
	case "Results":
		text := strings.TrimSpace(message.Transcript)
		if message.IsFinal {
			// An empty final is still emitted: silence legitimately completes a
			// turn, and it is the evidence a caller needs to tell silence from
			// loss.
			s.emit(runtimepkg.ProviderEvent{Type: protocol.EventTranscriptFinal, Data: s.transcriptData(message, text), Extensions: extension(raw)})
			return
		}
		if text == "" {
			return
		}
		// Each partial repeats the turn's whole transcript so far, which is
		// the cumulative shape transcript.delta carries.
		s.emit(runtimepkg.ProviderEvent{Type: protocol.EventTranscriptDelta, Data: s.transcriptData(message, text), Extensions: extension(raw)})
	case "UtteranceEnd":
		s.emit(runtimepkg.ProviderEvent{
			Type: protocol.EventSpeechEnded, Data: marshalData(map[string]any{"audio_end_ms": milliseconds(message.LastWordEnd), "turn_id": message.TurnID, "reason": "utterance_end"}),
			Extensions: extension(raw),
		})
	case "Error":
		if message.Recoverable {
			s.emit(runtimepkg.ProviderEvent{
				Type: protocol.EventWarning, Data: marshalData(map[string]any{"message": "Munsit STT reported a recoverable error", "provider_code": message.Code}),
				Extensions: extension(raw),
			})
			return
		}
		failure := socketFailure(message.Code, message.Message)
		failure.Extensions = extension(raw)
		s.fail(failure)
	case "Gender", "Sentiment":
		// Per-turn classifications with no canonical counterpart.
	default:
		s.emit(runtimepkg.ProviderEvent{
			Type: protocol.EventWarning, Data: marshalData(map[string]any{"message": "ignored Munsit STT event type", "provider_type": message.Type}),
			Extensions: extension(raw),
		})
	}
}

// opened handles the opening Metadata: it releases Open, then queues
// session.ready and the opening usage. The sends follow settleSetup because
// the runtime only starts draining Events once Open has returned. A repeated
// opening Metadata announces nothing new.
func (s *sttStream) opened(raw []byte) {
	if !s.readyOnce.CompareAndSwap(false, true) {
		return
	}
	s.settleSetup(nil)
	data := s.baseData()
	s.emit(runtimepkg.ProviderEvent{Type: protocol.EventSessionReady, Data: marshalData(data), Extensions: extension(raw)})
	// The opening observation stays incomplete until the closing Metadata
	// reports the billed seconds, so a socket lost before then settles as
	// unresolved rather than as free audio.
	s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: marshalData(data), Billing: s.billing(nil)})
}

// billing is the session's one duration observation. Without the closing
// Metadata it is incomplete.
func (s *sttStream) billing(raw []byte) *protocol.BillingObservation {
	var observation *protocol.BillingObservation
	if raw == nil {
		observation = &protocol.BillingObservation{OperationID: "stream", Model: s.model, Mode: "streaming", Quantities: map[string]int64{}}
	} else {
		observation = metering.Duration("stream", s.model, "streaming", raw, 1000, "audio_seconds_billed")
	}
	s.stateMu.Lock()
	observation.ProviderRequestID = s.sessionID
	s.stateMu.Unlock()
	observation.Language = s.language
	return observation
}

func (s *sttStream) baseData() map[string]any {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	data := map[string]any{"provider_request_id": s.sessionID}
	if s.sessionID != "" {
		data["session_id"] = s.sessionID
	}
	if s.serveModel != "" {
		data["provider_model"] = s.serveModel
	}
	return data
}

func (s *sttStream) transcriptData(message serverMessage, text string) json.RawMessage {
	data := s.baseData()
	data["text"] = text
	data["is_final"] = message.IsFinal
	data["speech_final"] = message.SpeechFinal
	data["turn_id"] = message.TurnID
	if message.Language != "" {
		data["language"] = message.Language
	}
	if message.Confidence != nil {
		data["confidence"] = *message.Confidence
	}
	if len(message.Words) > 0 {
		data["audio_start_ms"] = milliseconds(message.Words[0].Start)
		data["audio_end_ms"] = milliseconds(message.Words[len(message.Words)-1].End)
	}
	return marshalData(data)
}

// finish classifies how the socket ended. A close after Close or Abort is
// normal; a failure an Error event already explained is not reported twice;
// anything else is classified from the close status, which mirrors the
// Error codes.
func (s *sttStream) finish(err error) {
	if s.drainTimedOut.Load() {
		s.fail(&runtimepkg.ProviderError{Code: "request_timeout", Message: fmt.Sprintf("Munsit STT did not close within %s of CloseStream", s.drainTimeout), Retryable: false, Cause: err})
		return
	}
	status := websocket.CloseStatus(err)
	if s.closed.Load() || (s.inputClosed.Load() && status == websocket.StatusNormalClosure) {
		s.settleSetup(&runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Munsit STT closed before sending session Metadata", Retryable: true, Cause: err})
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

// socketFailure maps a socket Error code or close status onto the protocol
// classification. 1008 covers three refusals, told apart by the message.
func socketFailure(code int, message string) *runtimepkg.ProviderError {
	lower := strings.ToLower(message)
	switch code {
	case int(websocket.StatusPolicyViolation):
		switch {
		case strings.Contains(lower, "wallet") || strings.Contains(lower, "balance") || strings.Contains(lower, "credit"):
			return &runtimepkg.ProviderError{Code: "provider_quota_exceeded", Message: "Munsit STT refused the session for insufficient balance (1008)"}
		case strings.Contains(lower, "limit") || strings.Contains(lower, "concurren"):
			return &runtimepkg.ProviderError{Code: "provider_rate_limited", Message: "Munsit STT refused the session at its session limit (1008)", Retryable: true}
		default:
			return &runtimepkg.ProviderError{Code: "authentication_failed", Message: "Munsit STT refused the credential (1008)"}
		}
	case 4002:
		return &runtimepkg.ProviderError{Code: "invalid_request", Message: "Munsit STT rejected a session parameter (4002)"}
	case 4008:
		return &runtimepkg.ProviderError{Code: "invalid_request", Message: "Munsit STT audio ran more than 60 s ahead of real time (4008)"}
	case int(websocket.StatusMessageTooBig):
		return &runtimepkg.ProviderError{Code: "input_too_large", Message: "Munsit STT rejected an oversized message"}
	default:
		// 1011 (internal failure or idle input), a plain close the relay did
		// not ask for, and anything undocumented.
		message := "Munsit STT closed the session"
		if code > 0 {
			message = fmt.Sprintf("Munsit STT closed the session (%d)", code)
		}
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: message, Retryable: true}
	}
}

func milliseconds(seconds float64) int64 {
	return int64(math.Round(seconds * 1_000))
}
