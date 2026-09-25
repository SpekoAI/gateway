package nari

import (
	"context"
	"encoding/base64"
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
	// STTAdapterID is the identifier the connector registers this adapter under.
	STTAdapterID = "nari.stt.v1"
	// DefaultSTTModel is the latency-optimized serving class.
	DefaultSTTModel = "qwen3-asr-fast"

	realtimePath      = "/v1/realtime"
	inputSampleRateHz = 16_000
	// appendFrameBytes is the vendor's recommended append: 100 ms of 16 kHz
	// mono s16le. Larger appends are accepted, but the recommended size keeps
	// partials arriving at the pace the service is tuned for.
	appendFrameBytes = 3_200

	defaultSTTEventBuffer  = 64
	defaultMaxMessageBytes = 1 << 20
	// defaultSetupTimeout bounds the wait for session.configured. The service
	// closes a socket that has not configured within ten seconds, so a slightly
	// longer wait lets that refusal arrive as its own error.
	defaultSetupTimeout = 15 * time.Second
	// defaultCloseDrainTimeout bounds how long Close keeps the socket open for
	// the finals of items still being transcribed.
	defaultCloseDrainTimeout = 10 * time.Second
)

// sttModels are the two serving classes of Qwen3-ASR 1.7B.
var sttModels = map[string]struct{}{"qwen3-asr": {}, "qwen3-asr-fast": {}}

// sttLanguages are the 30 languages the models transcribe, by the code the
// socket accepts. An unsupported code does not fail validation vendor-side:
// it fails session setup as UPSTREAM_UNAVAILABLE, which reads as an outage.
var sttLanguages = map[string]struct{}{
	"ar": {}, "cs": {}, "da": {}, "de": {}, "el": {}, "en": {}, "es": {}, "fa": {}, "fi": {}, "fil": {},
	"fr": {}, "hi": {}, "hu": {}, "id": {}, "it": {}, "ja": {}, "ko": {}, "mk": {}, "ms": {}, "nl": {},
	"pl": {}, "pt": {}, "ro": {}, "ru": {}, "sv": {}, "th": {}, "tr": {}, "vi": {}, "yue": {}, "zh": {},
}

// sttLanguageAliases folds tags a caller may send onto the code the socket
// accepts. `tl` and `cmn` were refused live; `in` is the legacy Indonesian tag.
var sttLanguageAliases = map[string]string{"tl": "fil", "cmn": "zh", "in": "id"}

// STTConfig controls local transport bounds. Provider identity, model,
// credential and endpoint always come from the verified plan.
type STTConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
	// SetupTimeout bounds the wait for session.configured.
	SetupTimeout time.Duration
	// CloseDrainTimeout bounds how long Close waits for outstanding finals
	// before the socket is torn down locally.
	CloseDrainTimeout time.Duration
}

// STTAdapter opens Qwen3-ASR realtime transcription sessions.
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
		return nil, errors.New("nari stt: event buffer, message bound, setup timeout and close drain timeout must be positive")
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

// Open dials the socket, configures the session, and returns once the service
// has answered session.configured, so the first WriteAudio is always heard
// and a refused model or key fails Open where the runtime can fail over.
func (a *STTAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindSTT {
		return nil, fmt.Errorf("nari stt supports stt sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != ProviderName {
		return nil, fmt.Errorf("nari stt adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("nari stt requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if _, ok := sttModels[model]; !ok {
		return nil, fmt.Errorf("nari stt does not support model %q", model)
	}
	if request.Media == nil {
		return nil, errors.New("nari stt requires input media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("nari stt input media: %w", err)
	}
	if request.Media.Encoding != "pcm_s16le" || request.Media.Channels != 1 || request.Media.SampleRateHz != inputSampleRateHz {
		return nil, &runtimepkg.ProviderError{
			Code:    "unsupported_media",
			Message: fmt.Sprintf("Nari STT listens to mono pcm_s16le at %d Hz only, got %s/%d Hz/%d channels", inputSampleRateHz, request.Media.Encoding, request.Media.SampleRateHz, request.Media.Channels),
			Hint:    "Resample the input to mono pcm_s16le at 16000 Hz.",
		}
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("nari stt requires a bearer credential")
	}
	endpoint, err := a.realtimeEndpoint(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("nari stt endpoint: %w", err)
	}

	headers := make(http.Header)
	headers.Set("Authorization", "Bearer "+credential.Value)
	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: a.httpClient, HTTPHeader: headers})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		dialErr := providerError("Nari STT connection could not be established", nil, status, nil)
		dialErr.Cause = err
		return nil, dialErr
	}
	conn.SetReadLimit(a.maxMessageBytes)
	requestID := ""
	if response != nil {
		requestID = strings.TrimSpace(response.Header.Get(requestIDHeader))
	}

	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &sttStream{
		conn: conn, ctx: streamCtx, cancel: cancel,
		events:       make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		setupDone:    make(chan error, 1),
		done:         make(chan struct{}),
		settled:      make(chan struct{}, 1),
		drainTimeout: a.closeDrainTimeout,
		requestID:    requestID,
		openItems:    map[string]struct{}{},
	}
	if err := stream.writeJSON(ctx, sessionConfigure(model, request.Options.Language)); err != nil {
		_ = stream.abort()
		return nil, err
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
		return nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Nari STT did not acknowledge the session configuration", Retryable: true, Cause: setupCtx.Err()}
	}
	data := stream.baseData()
	stream.emit(runtimepkg.ProviderEvent{Type: protocol.EventSessionReady, Data: marshalData(data)})
	stream.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: marshalData(data)})
	return stream, nil
}

// realtimeEndpoint applies the shared allowlist, pins the path, and adds the
// transcription intent the handshake requires.
func (a *STTAdapter) realtimeEndpoint(raw string) (string, error) {
	endpoint, err := a.endpointPolicy.Parse(raw)
	if err != nil {
		return "", err
	}
	if endpoint.Path != realtimePath {
		return "", fmt.Errorf("endpoint path must be %s, got %q", realtimePath, endpoint.Path)
	}
	endpoint.RawQuery = url.Values{"intent": {"transcription"}}.Encode()
	return endpoint.String(), nil
}

// sessionConfigure is the first client frame. turn_detection is explicitly
// null, never omitted: the runtime owns turn boundaries and commits each one.
func sessionConfigure(model, language string) map[string]any {
	session := map[string]any{"model": model, "turn_detection": nil}
	if code := languageCode(language); code != "" {
		session["language"] = code
	}
	return map[string]any{"type": "session.configure", "session": session}
}

// languageCode reduces a caller tag to the code the socket accepts, or "" to
// leave detection automatic. "auto" and unsupported languages are left unsent
// rather than failing setup under a code that reads as an outage.
func languageCode(language string) string {
	tag := strings.ToLower(strings.TrimSpace(language))
	if index := strings.IndexAny(tag, "-_"); index > 0 {
		tag = tag[:index]
	}
	if alias, ok := sttLanguageAliases[tag]; ok {
		tag = alias
	}
	if _, ok := sttLanguages[tag]; ok {
		return tag
	}
	return ""
}

// sttStream is one open realtime transcription socket.
type sttStream struct {
	conn         *websocket.Conn
	ctx          context.Context
	cancel       context.CancelFunc
	events       chan runtimepkg.ProviderEvent
	setupDone    chan error
	done         chan struct{}
	settled      chan struct{}
	drainTimeout time.Duration
	requestID    string

	writeMu sync.Mutex
	// carry holds an odd trailing byte until the next write: appends must be
	// whole two-byte samples. Guarded by writeMu.
	carry        []byte
	gracefulOnce sync.Once
	abortOnce    sync.Once
	setupOnce    sync.Once
	inputClosed  atomic.Bool
	closed       atomic.Bool
	closeErr     error

	// Drain state. Every commit is answered by exactly one committed (reason
	// manual) or commit_empty, and every item the service opens — including
	// the ones it commits itself at 36 s — announces itself before its final.
	stateMu        sync.Mutex
	sessionID      string
	uncommitted    bool
	pendingCommits int
	openItems      map[string]struct{}

	terminalMu  sync.Mutex
	terminalErr error
}

func (s *sttStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

// WriteAudio forwards PCM as base64 input_audio_buffer.append events of the
// recommended size.
func (s *sttStream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("nari stt audio is empty")
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
	for offset := 0; offset < len(pcm); offset += appendFrameBytes {
		end := min(offset+appendFrameBytes, len(pcm))
		if err := s.writeLocked(ctx, map[string]string{"type": "input_audio_buffer.append", "audio": base64.StdEncoding.EncodeToString(pcm[offset:end])}); err != nil {
			return err
		}
	}
	if len(pcm) > 0 {
		s.stateMu.Lock()
		s.uncommitted = true
		s.stateMu.Unlock()
	}
	return nil
}

// CommitAudio ends the current utterance. With turn detection off it is the
// only thing that makes the service finalize, so the runtime's end-of-speech
// has to reach the socket through here.
func (s *sttStream) CommitAudio(ctx context.Context) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	s.stateMu.Lock()
	s.pendingCommits++
	s.uncommitted = false
	s.stateMu.Unlock()
	if err := s.writeJSON(ctx, map[string]string{"type": "input_audio_buffer.commit"}); err != nil {
		s.stateMu.Lock()
		s.pendingCommits--
		s.stateMu.Unlock()
		return err
	}
	return nil
}

func (s *sttStream) AppendText(context.Context, string) error {
	return runtimepkg.ErrUnsupportedOperation
}

func (s *sttStream) CommitText(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// Cancel has nothing to suppress: a transcription session produces no answer
// to interrupt, and committing would invent a final the caller did not mark.
func (s *sttStream) Cancel(context.Context) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	return nil
}

// Close commits any uncommitted audio, refuses further input, and keeps the
// socket open until every outstanding item has its final or the drain timeout
// passes. The service never closes a transcription socket on its own, so the
// socket is closed here once the finals are in.
func (s *sttStream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		s.stateMu.Lock()
		uncommitted := s.uncommitted
		s.stateMu.Unlock()
		if uncommitted {
			if err := s.CommitAudio(ctx); err != nil && !errors.Is(err, runtimepkg.ErrSessionClosed) {
				s.closeErr = err
				_ = s.abort()
				return
			}
		}
		s.inputClosed.Store(true)
		go s.drainThenClose()
	})
	return s.closeErr
}

func (s *sttStream) drainThenClose() {
	timer := time.NewTimer(s.drainTimeout)
	defer timer.Stop()
	for !s.drained() {
		select {
		case <-s.settled:
			continue
		case <-s.done:
		case <-timer.C:
		}
		break
	}
	s.closed.Store(true)
	s.writeMu.Lock()
	_ = s.conn.Close(websocket.StatusNormalClosure, "")
	s.writeMu.Unlock()
	s.cancel()
}

func (s *sttStream) drained() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.pendingCommits == 0 && len(s.openItems) == 0
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

func (s *sttStream) writeJSON(ctx context.Context, value any) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.writeLocked(ctx, value)
}

func (s *sttStream) writeLocked(ctx context.Context, value any) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if err := s.conn.Write(ctx, websocket.MessageText, payload); err != nil {
		if s.closed.Load() {
			return runtimepkg.ErrSessionClosed
		}
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Nari STT socket write failed", Retryable: true, Cause: err}
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
// error event or as the close that follows it.
func (s *sttStream) fail(err *runtimepkg.ProviderError) {
	s.settleSetup(err)
	if s.setTerminal(err) {
		s.emit(runtimepkg.ProviderEvent{Err: err})
	}
}

func (s *sttStream) settleSetup(err error) {
	s.setupOnce.Do(func() { s.setupDone <- err })
}

func (s *sttStream) poke() {
	select {
	case s.settled <- struct{}{}:
	default:
	}
}

// serverMessage is the union of the server events this adapter reads.
type serverMessage struct {
	Type         string          `json:"type"`
	ItemID       string          `json:"item_id"`
	Transcript   string          `json:"transcript"`
	Language     string          `json:"language"`
	CommitReason string          `json:"commit_reason"`
	Usage        json.RawMessage `json:"usage"`
	Session      *struct {
		ID string `json:"id"`
	} `json:"session"`
	Error *errorDetail `json:"error"`
}

func (s *sttStream) readLoop() {
	defer close(s.events)
	defer close(s.done)
	for {
		// The frame type is not checked: the documented events are text, but a
		// service that answered JSON in a binary frame would otherwise wait out
		// the setup timeout.
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
	case "session.configured":
		if message.Session != nil {
			s.stateMu.Lock()
			s.sessionID = message.Session.ID
			s.stateMu.Unlock()
		}
		s.settleSetup(nil)
	case "input_audio_buffer.committed":
		s.stateMu.Lock()
		if message.CommitReason != "max_duration" && s.pendingCommits > 0 {
			s.pendingCommits--
		}
		if message.ItemID != "" {
			s.openItems[message.ItemID] = struct{}{}
		}
		s.stateMu.Unlock()
		s.poke()
	case "input_audio_buffer.commit_empty":
		// A commit with nothing buffered is not a failure: there was nothing
		// to transcribe, and no final will follow it.
		s.stateMu.Lock()
		if s.pendingCommits > 0 {
			s.pendingCommits--
		}
		s.stateMu.Unlock()
		s.poke()
	case "transcript.partial":
		text := strings.TrimSpace(message.Transcript)
		if message.ItemID != "" {
			s.stateMu.Lock()
			s.openItems[message.ItemID] = struct{}{}
			s.stateMu.Unlock()
		}
		if text == "" {
			return
		}
		// Each partial is the item's whole hypothesis so far, which is the
		// cumulative shape transcript.delta carries.
		s.emit(runtimepkg.ProviderEvent{Type: protocol.EventTranscriptDelta, Data: s.transcriptData(message, text, false), Extensions: extension(raw)})
	case "transcript.completed":
		// An empty final is still emitted: silence legitimately completes an
		// item, and it is the evidence a caller needs to tell silence from loss.
		s.emit(runtimepkg.ProviderEvent{Type: protocol.EventTranscriptFinal, Data: s.transcriptData(message, strings.TrimSpace(message.Transcript), true), Extensions: extension(raw)})
		s.stateMu.Lock()
		delete(s.openItems, message.ItemID)
		s.stateMu.Unlock()
		s.poke()
	case "error":
		s.fail(providerError("Nari STT reported an error", message.Error, 0, raw))
	case "transcript.words", "session.updated", "input_audio_buffer.cleared":
		// Documented events with no canonical counterpart for this session
		// shape; word timings are never requested.
	default:
		s.emit(runtimepkg.ProviderEvent{
			Type: protocol.EventWarning, Data: marshalData(map[string]any{"message": "ignored Nari STT event type", "provider_type": message.Type}),
			Extensions: extension(raw),
		})
	}
}

func (s *sttStream) baseData() map[string]any {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	data := map[string]any{"provider_request_id": s.requestID}
	if s.requestID == "" {
		data["provider_request_id"] = s.sessionID
	}
	if s.sessionID != "" {
		data["session_id"] = s.sessionID
	}
	return data
}

func (s *sttStream) transcriptData(message serverMessage, text string, final bool) json.RawMessage {
	data := s.baseData()
	data["text"] = text
	data["is_final"] = final
	data["speech_final"] = final
	if message.ItemID != "" {
		data["item_id"] = message.ItemID
	}
	if message.Language != "" {
		data["language"] = message.Language
	}
	if message.CommitReason != "" {
		data["commit_reason"] = message.CommitReason
	}
	if len(message.Usage) > 0 {
		data["usage"] = message.Usage
	}
	return marshalData(data)
}

// finish classifies how the socket ended. A close after Close or Abort is
// normal; a failure the service already explained in an error event is not
// reported twice; anything else is classified from the close frame, whose
// reason carries the error code when the service sets one.
func (s *sttStream) finish(err error) {
	if s.closed.Load() || (s.inputClosed.Load() && isNormalClose(err)) {
		s.settleSetup(&runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Nari STT closed before the session was configured", Retryable: true, Cause: err})
		return
	}
	status := websocket.CloseStatus(err)
	var closeErr websocket.CloseError
	reason := ""
	if errors.As(err, &closeErr) {
		reason = strings.TrimSpace(closeErr.Reason)
	}
	var failure *runtimepkg.ProviderError
	switch code := reasonCode(reason); {
	case code != "":
		failure = providerError("Nari STT closed the session", &errorDetail{Code: code}, 0, nil)
	case status == websocket.StatusPolicyViolation:
		failure = &runtimepkg.ProviderError{Code: "invalid_request", Message: "Nari STT rejected the session", Retryable: false}
	case status == websocket.StatusMessageTooBig:
		failure = &runtimepkg.ProviderError{Code: "input_too_large", Message: "Nari STT rejected an oversized message", Retryable: false}
	default:
		failure = &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Nari STT closed the session", Retryable: true}
	}
	failure.Cause = err
	if status != -1 {
		failure.Extensions = map[string]json.RawMessage{extensionID: marshalData(map[string]any{"close_status": int(status), "close_reason": reason})}
	}
	s.fail(failure)
}

// reasonCode returns the close reason when it is an UPPER_SNAKE error code.
func reasonCode(reason string) string {
	if reason == "" || strings.ToUpper(reason) != reason || strings.ContainsAny(reason, " .:") {
		return ""
	}
	return reason
}

func isNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}
