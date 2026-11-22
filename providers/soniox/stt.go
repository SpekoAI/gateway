package soniox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
	"github.com/rivo/uniseg"
)

const (
	// STTAdapterID is the identifier returned by a Soniox STT session plan.
	STTAdapterID = "soniox.stt.v1"

	sttExtensionID  = "soniox.com/stt/v1"
	sttOfficialHost = "stt-rt.soniox.com"
	sttEndpointPath = "/transcribe-websocket"

	// endToken is emitted once, always final, when Soniox's semantic endpointer
	// decides the speaker finished an utterance. finToken is the reply to a
	// client-issued finalize. They are the only two segment boundaries in the
	// protocol: every other token is a word or word piece.
	endToken = "<end>"
	finToken = "<fin>"

	sttGracefulCloseTimeout = 30 * time.Second

	// sttKeepAliveInterval is how long the stream may go without audio before
	// the adapter sends {"type":"keepalive"}. Soniox ends a started stream
	// with 408 request_timeout after 20 seconds without audio or a keepalive
	// (docs.soniox.com/stt/rt/connection-keepalive), so this keeps a wide
	// margin under that limit.
	sttKeepAliveInterval = 10 * time.Second
)

const (
	sttClosePending uint32 = iota
	sttCloseFinished
	sttCloseTimedOut
)

// STTConfig controls local transport limits. Credentials and provider
// selection always come from the signed session plan, never from here.
type STTConfig struct {
	AdapterID       string
	HTTPClient      *http.Client
	EventBuffer     int
	MaxMessageBytes int64
	// KeepAliveInterval overrides sttKeepAliveInterval; zero uses the default.
	KeepAliveInterval     time.Duration
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// STTAdapter implements Soniox's /transcribe-websocket realtime API.
type STTAdapter struct {
	id              string
	httpClient      *http.Client
	eventBuffer     int
	maxMessageBytes int64
	keepalive       time.Duration
	endpointPolicy  upstream.WebSocketPolicy
}

// NewSTT creates an STT adapter with bounded provider-event buffering.
func NewSTT(config STTConfig) (*STTAdapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = STTAdapterID
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = 32
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = 1 << 20
	}
	if config.EventBuffer < 1 {
		return nil, errors.New("soniox event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("soniox maximum message bytes must be positive")
	}
	if config.KeepAliveInterval == 0 {
		config.KeepAliveInterval = sttKeepAliveInterval
	}
	if config.KeepAliveInterval < 0 {
		return nil, errors.New("soniox keepalive interval must be positive")
	}
	endpointPolicy, err := upstream.NewWebSocketPolicy(sttOfficialHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &STTAdapter{
		id:              config.AdapterID,
		httpClient:      config.HTTPClient,
		eventBuffer:     config.EventBuffer,
		maxMessageBytes: config.MaxMessageBytes,
		keepalive:       config.KeepAliveInterval,
		endpointPolicy:  endpointPolicy,
	}, nil
}

func (a *STTAdapter) ID() string { return a.id }

// Open dials the realtime socket and completes Soniox's start handshake before
// returning. The handshake is a JSON message rather than an HTTP header, so a
// rejected credential or an unsupported model surfaces asynchronously as an
// error frame; sending the start message here at least guarantees the socket is
// configured before the runtime is allowed to write the first audio frame, and
// beats the roughly ten-second deadline Soniox gives an unconfigured socket.
func (a *STTAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindSTT {
		return nil, fmt.Errorf("soniox stt supports stt sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "soniox" {
		return nil, fmt.Errorf("soniox adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("soniox stt requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("soniox stt requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("soniox stt media: %w", err)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model == "" || model == "auto" {
		return nil, errors.New("soniox stt requires a concrete model in the session plan")
	}
	audioFormat, err := sttAudioFormat(request.Media.Encoding)
	if err != nil {
		return nil, err
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("soniox stt requires a bearer credential")
	}
	endpoint, err := sttEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}

	// The key rides the handshake's Authorization header for managed, BYOK,
	// and relay credentials alike. See doc.go.
	conn, response, err := websocket.Dial(ctx, endpoint, sonioxDialOptions(a.httpClient, credential.Value))
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		code, retryable := sonioxStatusCode(status)
		return nil, &runtimepkg.ProviderError{
			Code:           code,
			Message:        "Soniox streaming connection could not be established",
			Retryable:      retryable,
			ProviderStatus: status,
			Cause:          err,
		}
	}
	conn.SetReadLimit(a.maxMessageBytes)

	identifyLanguage := request.Options.STT.IdentifyLanguage()
	languageHints := sttLanguageHints(request.Options.Language)
	start := sttStartRequest{
		Model:       model,
		AudioFormat: audioFormat,
		SampleRate:  request.Media.SampleRateHz,
		NumChannels: request.Media.Channels,
		// Soniox is the endpointer for this route. Its segment boundaries are
		// the only way a caller that never issues CommitAudio can ever receive
		// a final transcript, because Soniox commits tokens at word-piece
		// granularity and never marks an utterance complete on its own.
		EnableEndpointDetection: true,
		LanguageHints:           languageHints,
		// Keep legacy automatic-language sessions as they were. An opt-in
		// also asks for labels when the caller supplied a language hint.
		EnableLanguageIdentification: identifyLanguage || len(languageHints) == 0,
		ClientReferenceID:            reservationReference(request.Plan),
		// Speaker labels and vocabulary biasing both ride this one start
		// frame; the gateway has already refused asks Soniox cannot serve.
		EnableSpeakerDiariz: request.Options.STT.Diarize(),
		Context:             sttContextTerms(request.Options.STT.GetKeywords()),
		// One-way translation rides the same frame. Soniox then interleaves
		// translated tokens (translation_status "translation") with the
		// original ones, and the read loop keeps them apart.
		Translation: sttTranslationFor(request.Options.STT.TranslationTarget()),
	}
	payload, err := sttStartPayload(start, request.Options.STT.Provider("soniox"))
	if err != nil {
		_ = conn.CloseNow()
		return nil, err
	}
	if err := conn.Write(ctx, websocket.MessageText, payload); err != nil {
		_ = conn.CloseNow()
		return nil, &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "Soniox streaming start request failed",
			Retryable: true,
			Cause:     err,
		}
	}

	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &sttStream{
		conn:             conn,
		ctx:              streamCtx,
		cancel:           cancel,
		events:           make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		closeTimeout:     sttGracefulCloseTimeout,
		diarize:          start.EnableSpeakerDiariz,
		identifyLanguage: identifyLanguage,
		translating:      start.Translation != nil,
	}
	stream.lastAudio.Store(time.Now().UnixNano())
	go stream.readLoop()
	go stream.keepaliveLoop(a.keepalive)
	return stream, nil
}

func sttHTTPClient(client *http.Client) *http.Client {
	if client != nil {
		return client
	}
	return http.DefaultClient
}

// acceptableCredentialKind reports whether a delegated credential's kind may
// authenticate the plan's route. Bearer is the norm everywhere; the relay arm
// additionally accepts relay_access, because protocol.SessionPlan validation
// requires relay plans to label their credential relay_access while a relay
// connector that synthesizes the plan and drives these adapters directly — no
// Engine, no SessionPlan.Validate — labels the same permanent Soniox key
// bearer. Both spellings ride the same Authorization header: Soniox has no
// second channel to split on, so nothing else on the relay arm changes.
func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

// sonioxDialOptions authenticates the handshake. Soniox deprecated the
// api_key field of the first message, and refuses a connection that sends
// its key only there from 2027-01-15; long-lived and temporary keys both
// work in the Authorization header.
func sonioxDialOptions(client *http.Client, key string) *websocket.DialOptions {
	header := make(http.Header, 1)
	header.Set("Authorization", "Bearer "+key)
	return &websocket.DialOptions{HTTPClient: sttHTTPClient(client), HTTPHeader: header}
}

func sttEndpoint(policy upstream.WebSocketPolicy, rawEndpoint string) (string, error) {
	endpoint, err := policy.Parse(rawEndpoint)
	if err != nil {
		return "", fmt.Errorf("soniox stt endpoint: %w", err)
	}
	if endpoint.Path != sttEndpointPath {
		return "", fmt.Errorf("soniox stt endpoint path must be %s, got %q", sttEndpointPath, endpoint.Path)
	}
	return endpoint.String(), nil
}

// sttAudioFormat maps the portable media encoding onto a Soniox raw-audio
// format name. Soniox's raw list covers signed, unsigned, float, mulaw and alaw
// PCM but no bare Opus; Opus is only readable through its "auto" container
// detection, which needs an Ogg or WebM header the runtime does not produce.
func sttAudioFormat(encoding string) (string, error) {
	if encoding == "pcm_s16le" {
		return "pcm_s16le", nil
	}
	return "", fmt.Errorf("soniox stt does not support media encoding %q", encoding)
}

// sonioxLanguageAliases maps platform language subtags onto the spellings
// Soniox's supported-language table actually lists. Soniox answers an unlisted
// hint with HTTP 400 "Invalid language hint." and closes the socket, so an
// unaliased `nb` would take down the whole session rather than degrade.
var sonioxLanguageAliases = map[string]string{
	"fil": "tl",
	"nb":  "no",
	"nn":  "no",
}

// sttLanguageHints biases recognition without restricting it. Soniox
// auto-detects across 60+ languages when the field is absent, which is the
// right behaviour for an unset or "auto" request language.
func sttLanguageHints(language string) []string {
	primary, ok := sonioxPrimaryLanguage(language)
	if !ok {
		return nil
	}
	return []string{primary}
}

func sonioxPrimaryLanguage(language string) (string, bool) {
	primary := strings.ToLower(strings.TrimSpace(language))
	if index := strings.IndexByte(primary, '-'); index >= 0 {
		primary = primary[:index]
	}
	// "multi" is the multilingual spelling other STT vendors (Deepgram) take;
	// Soniox refuses it as a hint, and auto-detection is what it asks for.
	if primary == "" || primary == "auto" || primary == "multi" {
		return "", false
	}
	if alias, ok := sonioxLanguageAliases[primary]; ok {
		return alias, true
	}
	return primary, true
}

type sttStream struct {
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent

	writeMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	closeErr     error
	closeTimeout time.Duration
	closeState   atomic.Uint32
	finishedSeen atomic.Bool
	terminalMu   sync.RWMutex
	terminalErr  error

	// diarize is set once at Open: the start frame asked Soniox to label
	// speakers, so transcript events carry the labels. A session that did
	// not ask emits the same bytes it always did.
	diarize bool
	// identifyLanguage controls the opt-in runs, independently of legacy
	// upstream language identification on sessions without a language hint.
	identifyLanguage bool
	// translating is set once at Open: the start frame asked Soniox for its
	// own translation. Such a session is never cut (cutLongSegment).
	translating bool

	// Read-loop owned; never touched by the write side.
	segment sttSegment
	// translated accumulates finalized translated tokens. Kept apart from
	// segment so a translated word can never land in transcript text, and
	// never reset by a boundary that finds it empty — see flushSegment.
	translated sttTranslated
	requestID  string
	// commitPending makes an empty <fin> a real finalized silent turn without
	// changing how spontaneous empty endpointer markers are handled.
	commitPending atomic.Bool
	// finalizeSent prevents Close from issuing a duplicate finalize after the
	// runtime has already committed the batch turn. Soniox treats the empty
	// binary frame as end-of-stream, so only uncommitted audio needs a finalize
	// immediately before it.
	finalizeSent atomic.Bool
	// lastAudio is the UnixNano time of the last audio frame or keepalive.
	lastAudio atomic.Int64
}

func (s *sttStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

// WriteAudio forwards a raw PCM frame. An empty frame is refused rather than
// forwarded because Soniox reads a zero-length WebSocket frame as end-of-stream
// and would close the socket mid-call.
func (s *sttStream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("soniox stt audio is empty")
	}
	if err := s.write(ctx, websocket.MessageBinary, audio); err != nil {
		return err
	}
	s.lastAudio.Store(time.Now().UnixNano())
	s.finalizeSent.Store(false)
	return nil
}

// keepaliveLoop sends Soniox's {"type":"keepalive"} control whenever no audio
// has been written for interval, so a caller that pauses audio (local VAD,
// push-to-talk, a long agent turn) keeps its stream. It stops once Close has
// sent the end-of-stream frame, on Abort, or when a write fails because the
// socket is gone.
func (s *sttStream) keepaliveLoop(interval time.Duration) {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-timer.C:
		}
		if s.closed.Load() {
			return
		}
		if idle := time.Since(time.Unix(0, s.lastAudio.Load())); idle < interval {
			timer.Reset(interval - idle)
			continue
		}
		if err := s.writeJSON(s.ctx, map[string]string{"type": "keepalive"}); err != nil {
			return
		}
		s.lastAudio.Store(time.Now().UnixNano())
		timer.Reset(interval)
	}
}

// CommitAudio issues Soniox's manual finalization control. Soniox finalizes
// everything received so far and answers with a <fin> token, which the read
// loop turns into a final transcript. The socket stays open for the next turn.
func (s *sttStream) CommitAudio(ctx context.Context) error {
	s.commitPending.Store(true)
	if err := s.writeJSON(ctx, map[string]string{"type": "finalize"}); err != nil {
		s.commitPending.Store(false)
		return err
	}
	s.finalizeSent.Store(true)
	return nil
}

func (s *sttStream) AppendText(context.Context, string) error {
	return runtimepkg.ErrUnsupportedOperation
}

func (s *sttStream) CommitText(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// Cancel closes the stream because the transcribe protocol has no cancel
// command, then aborts rather than waiting for a trailing final result.
func (s *sttStream) Cancel(ctx context.Context) error {
	if err := s.Close(ctx); err != nil {
		return err
	}
	return s.abort()
}

// Abort immediately tears down the socket after a terminal runtime failure.
func (s *sttStream) Abort(context.Context) error { return s.abort() }

// Close performs Soniox's documented shutdown: an empty frame, after which the
// server replies with a finished response and closes. A finalize is sent first
// only when audio has not already been committed: repeating finalize between a
// caller's commit and the empty end frame can make the provider reject the
// otherwise valid shutdown sequence.
func (s *sttStream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		s.writeMu.Lock()
		if !s.finalizeSent.Load() {
			s.commitPending.Store(true)
			if err := sttWriteControl(ctx, s.conn, map[string]string{"type": "finalize"}); err != nil {
				s.commitPending.Store(false)
				s.closeErr = err
			} else {
				s.finalizeSent.Store(true)
			}
		}
		if s.closeErr == nil {
			// An empty TEXT frame ends the stream. An empty binary frame is an
			// empty audio chunk to Soniox: no finished reply, so Close waited
			// out the idle timeout and Soniox billed the idle tail.
			if err := s.conn.Write(ctx, websocket.MessageText, []byte{}); err != nil {
				s.closeErr = err
			}
		}
		s.closed.Store(true)
		s.writeMu.Unlock()
		if s.closeErr == nil {
			if s.finishedSeen.Load() {
				s.completeGracefulClose()
			} else {
				go s.finishGracefulClose()
			}
		} else {
			_ = s.abort()
		}
	})
	return s.closeErr
}

func (s *sttStream) abort() error {
	s.abortOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		if err := s.conn.CloseNow(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
	})
	return s.closeErr
}

func (s *sttStream) finishGracefulClose() {
	timeout := s.closeTimeout
	if timeout <= 0 {
		timeout = sttGracefulCloseTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-timer.C:
		if !s.closeState.CompareAndSwap(sttClosePending, sttCloseTimedOut) {
			return
		}
		s.setTerminal(sttCloseTimeoutError())
		s.cancel()
	case <-s.ctx.Done():
	}
}

func (s *sttStream) completeGracefulClose() {
	s.closeState.CompareAndSwap(sttClosePending, sttCloseFinished)
	if s.closeState.Load() == sttCloseFinished {
		s.cancel()
	}
}

func sttCloseTimeoutError() error {
	return &runtimepkg.ProviderError{
		Code:      "request_timeout",
		Message:   "Soniox STT did not finish graceful shutdown before the timeout",
		Hint:      "Retry the request; if it recurs, route to another STT provider.",
		Retryable: true,
	}
}

func (s *sttStream) TerminalError() error {
	s.terminalMu.RLock()
	defer s.terminalMu.RUnlock()
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
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return s.write(ctx, websocket.MessageText, payload)
}

func (s *sttStream) write(ctx context.Context, messageType websocket.MessageType, payload []byte) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if err := s.conn.Write(ctx, messageType, payload); err != nil {
		return &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "Soniox streaming write failed",
			Retryable: true,
			Cause:     err,
		}
	}
	return nil
}

func sttWriteControl(ctx context.Context, conn *websocket.Conn, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, payload)
}

func (s *sttStream) readLoop() {
	defer func() {
		if s.closeState.Load() == sttCloseTimedOut {
			select {
			case s.events <- runtimepkg.ProviderEvent{Err: s.TerminalError()}:
			default:
			}
		}
		close(s.events)
	}()
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			// Only 1000 after a caller-requested close (or after Soniox's own
			// finished reply) is completion. Soniox uses 1001 for connections
			// it closes as idle, so an unrequested 1001 lost the stream.
			if !s.closed.Load() && s.ctx.Err() == nil && websocket.CloseStatus(err) != websocket.StatusNormalClosure {
				message, hint := "Soniox streaming read failed", ""
				if websocket.CloseStatus(err) == websocket.StatusGoingAway {
					message = "Soniox closed the streaming connection as idle"
					hint = "Soniox closed the stream with WebSocket code 1001; retry the request, and if it recurs, route to another STT provider."
				}
				s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{
					Code:      "provider_unavailable",
					Message:   message,
					Hint:      hint,
					Retryable: true,
					Cause:     err,
				}})
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
		if s.finishedSeen.Load() {
			// Finished is terminal evidence. A provider disconnect after it
			// cannot turn the completed transcript and usage into a failure.
			return
		}
	}
}

func (s *sttStream) handleMessage(payload []byte) error {
	var message sttInboundMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "Soniox sent malformed streaming JSON",
			Retryable: true,
			Cause:     err,
		}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))

	if message.ErrorType != "" || message.ErrorCode != 0 {
		// An error invalidates the turn: Soniox closes the socket immediately
		// after this frame and the runtime fails the attempt over. Committing
		// the half-finished segment first would land a truncated user turn, so
		// it is dropped along with the stream.
		code, retryable := sonioxErrorCode(message.ErrorType, message.ErrorCode)
		return &runtimepkg.ProviderError{
			Code:           code,
			Message:        sonioxErrorMessage("Soniox reported a streaming error", message.ErrorMessage),
			Retryable:      retryable,
			ProviderStatus: message.ErrorCode,
			Extensions:     sttExtension(raw),
			// error_type is Soniox's stable code; error_code is the status.
			ProviderCode: message.ErrorType,
		}
	}
	if message.RequestID != "" && s.requestID != message.RequestID {
		s.requestID = message.RequestID
		if err := s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventUsageObserved,
			Data:       sonioxUsageData(message.RequestID, nil),
			Extensions: sttExtension(raw),
		}); err != nil {
			return err
		}
	}

	if err := s.handleTokens(message, raw); err != nil {
		return err
	}

	if message.Finished {
		if s.closed.Load() && !s.closeState.CompareAndSwap(sttClosePending, sttCloseFinished) && s.closeState.Load() == sttCloseTimedOut {
			return s.TerminalError()
		}
		// The socket is about to close. Flush whatever the endpointer never
		// got round to marking, then report the provider's own measure of
		// processed audio, which is the unit an STT reservation is priced in.
		if _, err := s.flushSegment(raw); err != nil {
			return err
		}
		processed := message.TotalAudioProcMS
		if err := s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventUsageObserved,
			Data:       sonioxUsageData(s.requestID, &processed),
			Extensions: sttExtension(raw),
		}); err != nil {
			return err
		}
		s.finishedSeen.Store(true)
		if s.closed.Load() {
			s.completeGracefulClose()
		}
		return nil
	}
	return nil
}

// handleTokens walks one frame in wire order. Soniox re-sends the entire
// non-final tail on every frame but sends each final token exactly once, so
// finals accumulate into the segment while the tail is rebuilt per frame.
func (s *sttStream) handleTokens(message sttInboundMessage, raw json.RawMessage) error {
	tail := strings.Builder{}
	translatedTail := strings.Builder{}
	sawContent := false
	// tailEndMS is the last end any tail piece carried, the bound a session
	// without diarization reports. tailOuterEndMS is the end of the tail's
	// last piece with text, absent when that piece has none, the bound a
	// diarized session reports; tailHasText says there is such a piece.
	var tailEndMS, tailOuterEndMS *int64
	tailHasText := false
	// Keep tail tokens only when building speaker or language runs.
	var tailSpoken []sttToken

	for _, token := range message.Tokens {
		if token.Text == endToken || token.Text == finToken {
			answered, err := s.flushSegment(raw)
			if err != nil {
				return err
			}
			if token.Text == finToken {
				committed := s.commitPending.Swap(false)
				if !answered && committed {
					if err := s.emit(runtimepkg.ProviderEvent{
						Type:       protocol.EventTranscriptFinal,
						Data:       sttTranscriptData("", "", "", true, nil, nil, nil, s.requestID),
						Extensions: sttExtension(raw),
					}); err != nil {
						return err
					}
				}
			}
			// A provisional tail cannot outlive the boundary that closed its
			// segment; Soniox restarts the tail from scratch afterwards.
			tail.Reset()
			translatedTail.Reset()
			tailEndMS, tailOuterEndMS = nil, nil
			tailHasText = false
			tailSpoken = nil
			sawContent = false
			if token.Text == endToken {
				// <end> is the semantic endpointer's verdict that the speaker
				// finished. <fin> is only this adapter's own finalize coming
				// back, and the runtime already knows it asked for that.
				if err := s.emit(runtimepkg.ProviderEvent{
					Type:       protocol.EventSpeechEnded,
					Data:       sttSpeechEndedData(token.EndMS),
					Extensions: sttExtension(raw),
				}); err != nil {
					return err
				}
			}
			continue
		}
		sawContent = true
		if token.TranslationStatus == sttTranslationStatusTranslation {
			// Translated tokens carry no timestamps and no confidence the
			// transcript should average, so they touch neither the segment
			// nor tailEndMS.
			if token.IsFinal {
				s.translated.text.WriteString(token.Text)
			} else {
				translatedTail.WriteString(token.Text)
			}
			continue
		}
		if len(token.Speaker) > sttSpeakerLabelMaxBytes {
			// Not a label anyone can use: the piece counts as unlabelled.
			token.Speaker = ""
		}
		if token.IsFinal {
			s.appendFinal(token)
			continue
		}
		tail.WriteString(token.Text)
		if token.EndMS != nil {
			tailEndMS = token.EndMS
		}
		if strings.TrimSpace(token.Text) != "" {
			tailOuterEndMS, tailHasText = token.EndMS, true
		}
		if s.diarize || s.identifyLanguage {
			tailSpoken = append(tailSpoken, token)
		}
	}

	if (s.diarize || s.identifyLanguage) && !s.translating {
		if err := s.cutLongSegment(raw); err != nil {
			return err
		}
	}
	if !sawContent {
		return nil
	}
	interim := strings.TrimSpace(s.segment.text.String() + tail.String())
	interimTranslation := strings.TrimSpace(s.translated.text.String() + translatedTail.String())
	if interim == "" && interimTranslation == "" {
		return nil
	}
	// Without diarization the span is the first start and last end any piece
	// carried, exactly as before speaker labels existed.
	startMS, endMS := s.segment.startMS, tailEndMS
	if endMS == nil {
		endMS = s.segment.endMS
	}
	if s.diarize {
		// Each bound is the outer piece's own: the segment's first finalized
		// piece starts the hypothesis, and the tail's last piece, or the
		// segment's when the tail has none, ends it. An inner piece's time
		// never stands in for an outer one that has none.
		startMS, endMS = s.segment.firstStartMS, s.segment.lastEndMS
		if tailHasText {
			endMS = tailOuterEndMS
		}
	}
	data := sttTranscriptFields(interim, interimTranslation, "", false, startMS, endMS, nil, s.requestID)
	if s.diarize {
		// The hypothesis is the segment's finals so far plus the tail. The
		// finals' runs were built as they arrived; only the tail, which
		// Soniox re-sends whole on every frame, is added here.
		sttSetSpeakerRuns(data, s.segment.runs.with(tailSpoken, false, true))
	}
	if s.identifyLanguage {
		sttSetRuns(data, "language_runs", s.segment.languageRuns.with(tailSpoken, true, s.diarize))
	}
	return s.emit(runtimepkg.ProviderEvent{
		Type:       protocol.EventTranscriptDelta,
		Data:       sonioxMarshalData(data),
		Extensions: sttExtension(raw),
	})
}

func (s *sttStream) appendFinal(token sttToken) {
	s.segment.appendFinal(token)
	if s.diarize || (s.identifyLanguage && !s.translating) {
		s.segment.spoken = append(s.segment.spoken, token)
	}
	if s.diarize {
		s.segment.runs.add(token, false, true)
	}
	if s.identifyLanguage {
		s.segment.languageRuns.add(token, true, s.diarize)
	}
}

// flushSegment emits the segment's final. On a translating session the final
// also carries every translated token finalized since the previous final.
// Soniox generates a translation AFTER the words it translates and does not
// promise the translation of a segment's last words arrives before that
// segment's <end>, so late translated tokens ride the NEXT final rather than
// being re-attributed. A boundary with translated tokens but no original
// words (the translation of the previous segment's tail, or the stream
// finishing) still emits a final, with empty text: dropping it would lose
// translated words the caller asked, and pays, for. A segment that already
// sent an interim final (cutLongSegment) gets its closing final even with
// nothing left to say: empty, as a commit's answer to silence is, so a
// consumer holding the interim text until its segment closes is not left
// waiting. It reports whether it emitted a final.
func (s *sttStream) flushSegment(raw json.RawMessage) (bool, error) {
	spoken, cut, languageRuns := s.segment.spoken, s.segment.cut, s.segment.languageRuns
	text, language, confidence, startMS, endMS := s.segment.flush(s.diarize)
	translation := s.translated.flush()
	if text == "" && translation == "" {
		if !cut {
			return false, nil
		}
		// Whitespace has no confidence worth reporting.
		confidence = nil
	}
	data := sttTranscriptFields(text, translation, language, true, startMS, endMS, confidence, s.requestID)
	if s.diarize {
		sttSetSpeakers(data, spoken)
	}
	if s.identifyLanguage {
		sttSetRuns(data, "language_runs", languageRuns)
	}
	return true, s.emit(runtimepkg.ProviderEvent{
		Type:       protocol.EventTranscriptFinal,
		Data:       sonioxMarshalData(data),
		Extensions: sttExtension(raw),
	})
}

// A labelled segment's finalized part is cut once it spans more than
// sttCutAudioMS of audio or sttCutCharacters characters, so that a long talk
// without an endpoint does not resend an ever longer hypothesis on every
// delta (docs/PROTOCOL.md). A session without labels is never cut, and
// neither is one with Soniox's own translation: the translation trails the
// words and is not aligned to them, so a prefix final would take the
// translation of words still in the tail.
const (
	sttCutAudioMS    = 20_000
	sttCutCharacters = 2_000
)

// cutLongSegment emits the finalized prefix of an over-long segment once, as
// a final that does not end the utterance, and keeps the rest as the open
// segment. The prefix ends at the last sentence end in the finalized text's
// last quarter, else at the last word boundary; never inside a word or a
// character (sttCutPoint).
func (s *sttStream) cutLongSegment(raw json.RawMessage) error {
	spoken := s.segment.spoken
	if !sttOverBound(s.segment.text.String(), spoken) {
		return nil
	}
	cut := sttCutPoint(spoken)
	if cut == 0 {
		return nil
	}
	prefix, rest := spoken[:cut], slices.Clone(spoken[cut:])
	var head sttSegment
	for _, token := range prefix {
		head.appendFinal(token)
		if s.identifyLanguage {
			head.languageRuns.add(token, true, s.diarize)
		}
	}
	languageRuns := head.languageRuns
	text, language, confidence, startMS, endMS := head.flush(true)
	data := sttTranscriptFields(text, s.translated.flush(), language, true, startMS, endMS, confidence, s.requestID)
	// The speaker has not finished: this only closes a long stretch. interim
	// says so explicitly, because speech_final false alone is also an
	// ordinary final on other providers: a consumer answering input.commit
	// with finals must not take this one as the answer (docs/PROTOCOL.md).
	data["speech_final"] = false
	data["interim"] = true
	if s.diarize {
		sttSetSpeakers(data, prefix)
	}
	if s.identifyLanguage {
		sttSetRuns(data, "language_runs", languageRuns)
	}
	s.segment = sttSegment{cut: true}
	for _, token := range rest {
		s.appendFinal(token)
	}
	return s.emit(runtimepkg.ProviderEvent{
		Type:       protocol.EventTranscriptFinal,
		Data:       sonioxMarshalData(data),
		Extensions: sttExtension(raw),
	})
}

func sttOverBound(text string, tokens []sttToken) bool {
	if utf8.RuneCountInString(text) > sttCutCharacters {
		return true
	}
	var startMS, endMS *int64
	for _, token := range tokens {
		if strings.TrimSpace(token.Text) == "" {
			continue
		}
		if startMS == nil {
			startMS = token.StartMS
		}
		if token.EndMS != nil {
			endMS = token.EndMS
		}
	}
	return startMS != nil && endMS != nil && *endMS-*startMS > sttCutAudioMS
}

// sttCutPoint returns how many of the finalized tokens to emit, 0 when there
// is nowhere to cut. A cut lies between two tokens where the second starts a
// word or the first ends one or a sentence; text with no such boundary, as
// Chinese or Japanese written without spaces, is cut by sttCharacterCut. The
// end of the last finalized token is never one: the next piece is not final
// yet and may continue the word. Nor is any boundary inside a character
// (sttGraphemeBreaks): such a candidate is skipped, so the cut steps back to
// the previous one that is a character boundary.
func sttCutPoint(tokens []sttToken) int {
	var text strings.Builder
	total := 0
	for _, token := range tokens {
		text.WriteString(token.Text)
		total += utf8.RuneCountInString(token.Text)
	}
	breaks := sttGraphemeBreaks(text.String())
	offset, at, sentence, word, hasText := 0, 0, 0, 0, false
	for index := 1; index < len(tokens); index++ {
		previous := tokens[index-1].Text
		offset += utf8.RuneCountInString(previous)
		at += len(previous)
		hasText = hasText || strings.TrimSpace(previous) != ""
		endsSentence := sttEndsSentence(previous)
		if !hasText || !breaks[at] || !(startsWithSpace(tokens[index].Text) || endsWithSpace(previous) || endsSentence) {
			continue
		}
		word = index
		if endsSentence && 4*offset >= 3*total {
			sentence = index
		}
	}
	if sentence > 0 {
		return sentence
	}
	if word > 0 {
		return word
	}
	return sttCharacterCut(tokens, breaks)
}

// sttCharacterCut is the cut for finalized text with no word or sentence
// boundary: the last boundary between finalized tokens, within the first
// sttCutCharacters characters, that is also a character boundary. breaks are
// the tokens' text's sttGraphemeBreaks.
func sttCharacterCut(tokens []sttToken, breaks []bool) int {
	cut, offset, at, hasText := 0, 0, 0, false
	for index := 1; index < len(tokens); index++ {
		previous := tokens[index-1].Text
		offset += utf8.RuneCountInString(previous)
		at += len(previous)
		if offset > sttCutCharacters {
			break
		}
		hasText = hasText || strings.TrimSpace(previous) != ""
		if hasText && breaks[at] {
			cut = index
		}
	}
	return cut
}

// sttGraphemeBreaks marks the byte offsets of text where one user-perceived
// character ends and the next begins, so breaks has len(text)+1 entries and
// both ends are breaks. These are Unicode's extended grapheme cluster
// boundaries (UAX #29) as uniseg finds them, with its Unicode 15.0 tables,
// less the boundaries inside an Indic conjunct (sttInsideConjunct), a rule
// newer than those tables.
func sttGraphemeBreaks(text string) []bool {
	breaks := make([]bool, len(text)+1)
	breaks[0] = true
	rest, offset, state := text, 0, -1
	for rest != "" {
		var cluster string
		cluster, rest, _, state = uniseg.FirstGraphemeClusterInString(rest, state)
		offset += len(cluster)
		breaks[offset] = true
	}
	for offset := 1; offset < len(text); offset++ {
		if breaks[offset] && sttInsideConjunct(text, offset) {
			breaks[offset] = false
		}
	}
	return breaks
}

// sttConjunctLinkers are the viramas that join two consonants into one
// conjunct (Unicode 15.1 Indic_Conjunct_Break=Linker): Devanagari, Bengali,
// Gujarati, Oriya, Telugu and Malayalam.
const sttConjunctLinkers = "\u094d\u09cd\u0acd\u0b4d\u0c4d\u0d4d"

// sttInsideConjunct reports whether offset falls inside an Indic conjunct,
// such as क्ष: a consonant at offset, and before it a consonant followed by
// a linker, with only combining marks and joiners around the linker
// (UAX #29 rule GB9c). Every letter of the six scripts counts as a
// consonant; a vowel letter after a virama is rare, and keeping it joined
// only moves a cut one character back.
func sttInsideConjunct(text string, offset int) bool {
	next, _ := utf8.DecodeRuneInString(text[offset:])
	if !sttConjunctLetter(next) {
		return false
	}
	linked := false
	for before := text[:offset]; before != ""; {
		previous, size := utf8.DecodeLastRuneInString(before)
		before = before[:len(before)-size]
		switch {
		case strings.ContainsRune(sttConjunctLinkers, previous):
			linked = true
		case previous == '\u200d' || unicode.In(previous, unicode.Mn, unicode.Me):
		default:
			return linked && sttConjunctLetter(previous)
		}
	}
	return false
}

func sttConjunctLetter(r rune) bool {
	return unicode.IsLetter(r) && unicode.In(r, unicode.Devanagari, unicode.Bengali, unicode.Gujarati, unicode.Oriya, unicode.Telugu, unicode.Malayalam)
}

func sttEndsSentence(text string) bool {
	last, _ := utf8.DecodeLastRuneInString(strings.TrimRightFunc(text, unicode.IsSpace))
	return strings.ContainsRune(".!?…。！？", last)
}

func (s *sttStream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// sttSegment accumulates the committed tokens of one Soniox segment until an
// <end> or <fin> closes it. Confidence is averaged over the tokens that carried
// one; timestamps are absent on translated tokens, hence the pointers.
type sttSegment struct {
	text          strings.Builder
	confidenceSum float64
	confidenceN   int
	// startMS is the first start and endMS the last end that any piece
	// carried: the span a session without diarization has always reported.
	startMS *int64
	endMS   *int64
	// firstStartMS is the start of the first piece with text and lastEndMS
	// the end of the last, each absent when that piece has none; hasText
	// says there is such a piece. A diarized session reports these outer
	// bounds.
	firstStartMS *int64
	lastEndMS    *int64
	hasText      bool
	// cut says an interim final already carried part of this segment, so its
	// end owes a closing final even with nothing left to say.
	cut bool
	// languages counts tokens per identified language; only a session with
	// language identification on gets any.
	languages map[string]int
	// Keep pieces for the long-segment cut and, when diarizing, final words.
	spoken       []sttToken
	runs         sttRuns
	languageRuns sttRuns
}

func (a *sttSegment) appendFinal(token sttToken) {
	a.text.WriteString(token.Text)
	if token.Language != "" && strings.TrimSpace(token.Text) != "" {
		if a.languages == nil {
			a.languages = map[string]int{}
		}
		a.languages[token.Language]++
	}
	if token.Confidence != nil {
		a.confidenceSum += *token.Confidence
		a.confidenceN++
	}
	if token.StartMS != nil && a.startMS == nil {
		a.startMS = token.StartMS
	}
	if token.EndMS != nil {
		a.endMS = token.EndMS
	}
	if strings.TrimSpace(token.Text) != "" {
		if !a.hasText {
			a.firstStartMS, a.hasText = token.StartMS, true
		}
		a.lastEndMS = token.EndMS
	}
}

// flush returns the segment's text, its majority identified language ("" when
// none was identified), average confidence, and audio span. With outer, the
// span covers the whole text, so it is reported only when the first piece's
// start and the last piece's end are both known; otherwise neither is, and
// only the words' own bounds say when anything was said. Without outer it is
// the first start and last end any piece carried, as it always was.
func (a *sttSegment) flush(outer bool) (string, string, *float64, *int64, *int64) {
	text := strings.TrimSpace(a.text.String())
	language, most := "", 0
	for candidate, count := range a.languages {
		if count > most || count == most && candidate < language {
			language, most = candidate, count
		}
	}
	var confidence *float64
	if a.confidenceN > 0 {
		average := a.confidenceSum / float64(a.confidenceN)
		confidence = &average
	}
	startMS, endMS := a.startMS, a.endMS
	if outer {
		startMS, endMS = a.firstStartMS, a.lastEndMS
		if startMS == nil || endMS == nil {
			startMS, endMS = nil, nil
		}
	}
	*a = sttSegment{}
	return text, language, confidence, startMS, endMS
}

// sttTranslated accumulates finalized translated tokens between finals.
type sttTranslated struct {
	text strings.Builder
}

func (t *sttTranslated) empty() bool { return strings.TrimSpace(t.text.String()) == "" }

func (t *sttTranslated) flush() string {
	text := strings.TrimSpace(t.text.String())
	t.text.Reset()
	return text
}

func sttTranscriptData(text, translation, language string, isFinal bool, startMS, endMS *int64, confidence *float64, requestID string) json.RawMessage {
	return sonioxMarshalData(sttTranscriptFields(text, translation, language, isFinal, startMS, endMS, confidence, requestID))
}

func sttTranscriptFields(text, translation, language string, isFinal bool, startMS, endMS *int64, confidence *float64, requestID string) map[string]any {
	// Finals are emitted only after <end>, <fin>, or stream completion has
	// flushed the segment. Unlike locked token chunks, these complete an
	// utterance and let consumers skip their missing-final safety timeout.
	data := map[string]any{"text": text, "is_final": isFinal, "speech_final": isFinal, "provider_request_id": requestID}
	// Omitted, not empty, on a session that never asked: every event of an
	// untranslated session keeps its exact bytes.
	if translation != "" {
		data["translation"] = translation
	}
	if language != "" {
		data["language"] = language
	}
	if startMS != nil {
		data["audio_start_ms"] = *startMS
	}
	if endMS != nil {
		data["audio_end_ms"] = *endMS
	}
	if confidence != nil {
		data["confidence"] = *confidence
	}
	return data
}

// sttSpeakerLabelMaxBytes bounds the vendor's speaker label, which events copy
// verbatim into every run and word it names. A longer label is dropped and
// its piece counts as unlabelled.
const sttSpeakerLabelMaxBytes = 64

// sttSetSpeakers adds a diarized session's labels to a final, in the shape
// ElevenLabs Scribe v3 Live finals already use (docs/PROTOCOL.md): words, each
// with the vendor's speaker label verbatim and its span, and a top-level
// speaker when one speaker said every word. A word Soniox left unlabelled could be anyone's,
// so it has no speaker and the event then names no one. Nothing is added
// when the tokens hold no words.
func sttSetSpeakers(data map[string]any, tokens []sttToken) {
	words := sttSpeakerWords(tokens)
	if len(words) == 0 {
		return
	}
	entries := make([]map[string]any, len(words))
	speaker := words[0].speaker
	for index, word := range words {
		entry := map[string]any{"text": word.text}
		if word.startMS != nil {
			entry["start_ms"] = *word.startMS
		}
		if word.endMS != nil {
			entry["end_ms"] = *word.endMS
		}
		if word.speaker != "" {
			entry["speaker"] = word.speaker
		}
		if word.speaker != speaker {
			speaker = ""
		}
		entries[index] = entry
	}
	data["words"] = entries
	if speaker != "" {
		data["speaker"] = speaker
	}
}

// sttSpeakerWord is one word of a diarized transcript event.
type sttSpeakerWord struct {
	text           string
	startMS, endMS *int64
	speaker        string
}

// sttSpeakerWords joins Soniox's word pieces into words. A piece carries its
// own leading space, so a piece starts a new word when it begins with
// whitespace, follows a piece that ended with whitespace, or has a different
// speaker than the word so far; any other piece continues the word
// ("fan" "tastic", "you" "?"). Word text is cut from the pieces in order, so
// the words of an event appear in its text in the same order. A word's span
// runs from its first piece's start to its last piece's end, either absent
// when that piece carries no time.
func sttSpeakerWords(tokens []sttToken) []sttSpeakerWord {
	var words []sttSpeakerWord
	continues := false
	for _, token := range tokens {
		if strings.TrimSpace(token.Text) == "" {
			continues = false
			continue
		}
		last := len(words) - 1
		if !continues || startsWithSpace(token.Text) || words[last].speaker != token.Speaker {
			words = append(words, sttSpeakerWord{startMS: token.StartMS, speaker: token.Speaker})
			last++
		}
		words[last].text += strings.TrimSpace(token.Text)
		// A word ends where its last piece ends; a last piece with no time
		// leaves the end unknown rather than borrowing an inner piece's.
		words[last].endMS = token.EndMS
		continues = !endsWithSpace(token.Text)
	}
	return words
}

// sttRun is one stretch with consistent speaker and/or language labels,
// the span from its first piece's start to its last piece's end (each
// absent when that piece has no time), and where it sits in the text, in
// characters (Unicode code points) from the start of the untrimmed segment,
// from its first non-space character to just past its last.
type sttRun struct {
	speaker        string
	language       string
	startMS, endMS *int64
	from, to       int
}

// sttRuns builds a segment's attribution runs one piece at a time, so a delta
// costs one entry per run however long the segment grows. A new run starts
// wherever the label changes, inside a word too: labels are the vendor's,
// verbatim.
type sttRuns struct {
	runs []sttRun
	// characters counts the code points of every piece added so far, which
	// is where the next piece starts in the segment's text.
	characters int
}

func (r *sttRuns) add(token sttToken, identify, diarize bool) {
	length := utf8.RuneCountInString(token.Text)
	if content := strings.TrimSpace(token.Text); content != "" {
		speaker, language := "", ""
		if diarize {
			speaker = token.Speaker
		}
		if identify && protocol.ValidSTTLanguageTag(token.Language) && !strings.EqualFold(token.Language, "und") {
			language = token.Language
		}
		from := r.characters + length - utf8.RuneCountInString(strings.TrimLeftFunc(token.Text, unicode.IsSpace))
		last := len(r.runs) - 1
		if last < 0 || r.runs[last].speaker != speaker || !strings.EqualFold(r.runs[last].language, language) {
			r.runs = append(r.runs, sttRun{speaker: speaker, language: language, startMS: token.StartMS, from: from})
			last++
		}
		r.runs[last].endMS = token.EndMS
		r.runs[last].to = from + utf8.RuneCountInString(content)
	}
	r.characters += length
}

// with returns the runs extended by the tail's pieces, leaving the segment's
// own runs as they were.
func (r sttRuns) with(tail []sttToken, identify, diarize bool) sttRuns {
	extended := sttRuns{runs: slices.Clone(r.runs), characters: r.characters}
	for _, token := range tail {
		extended.add(token, identify, diarize)
	}
	return extended
}

// sttSetSpeakerRuns adds a diarized session's labels to a delta: speaker_runs,
// one entry per run with the vendor's label verbatim, its span, and
// text_start/text_end, the run's character offsets into the delta's text
// (end exclusive). Runs carry offsets rather than their own text so labels add
// a few bytes per run to a delta, not a second copy of the hypothesis. A
// top-level speaker names the one speaker of a single-run delta. A run Soniox
// left unlabelled has no speaker. Nothing is added when there are no runs.
func sttSetSpeakerRuns(data map[string]any, runs sttRuns) {
	sttSetRuns(data, "speaker_runs", runs)
	if len(runs.runs) == 1 && runs.runs[0].speaker != "" {
		data["speaker"] = runs.runs[0].speaker
	}
}

func sttSetRuns(data map[string]any, key string, runs sttRuns) {
	if len(runs.runs) == 0 {
		return
	}
	// The delta's text is trimmed, and only whitespace precedes the first
	// run, so offsets count from the first run's first character.
	base := runs.runs[0].from
	entries := make([]map[string]any, len(runs.runs))
	for index, run := range runs.runs {
		entry := map[string]any{"text_start": run.from - base, "text_end": run.to - base}
		if run.speaker != "" {
			entry["speaker"] = run.speaker
		}
		if run.language != "" {
			entry["language"] = run.language
		}
		if run.startMS != nil {
			entry["start_ms"] = *run.startMS
		}
		if run.endMS != nil {
			entry["end_ms"] = *run.endMS
		}
		entries[index] = entry
	}
	data[key] = entries
}

func startsWithSpace(text string) bool {
	return strings.TrimLeftFunc(text, unicode.IsSpace) != text
}

func endsWithSpace(text string) bool {
	return strings.TrimRightFunc(text, unicode.IsSpace) != text
}

func sttSpeechEndedData(endMS *int64) json.RawMessage {
	data := map[string]any{"reason": "endpoint_detected"}
	if endMS != nil {
		data["audio_end_ms"] = *endMS
	}
	return sonioxMarshalData(data)
}

func sttExtension(raw json.RawMessage) map[string]json.RawMessage {
	return map[string]json.RawMessage{sttExtensionID: raw}
}

type sttStartRequest struct {
	Model                        string                `json:"model"`
	AudioFormat                  string                `json:"audio_format"`
	SampleRate                   int                   `json:"sample_rate"`
	NumChannels                  int                   `json:"num_channels"`
	EnableEndpointDetection      bool                  `json:"enable_endpoint_detection"`
	LanguageHints                []string              `json:"language_hints,omitempty"`
	EnableLanguageIdentification bool                  `json:"enable_language_identification,omitempty"`
	ClientReferenceID            string                `json:"client_reference_id,omitempty"`
	EnableSpeakerDiariz          bool                  `json:"enable_speaker_diarization,omitempty"`
	Context                      *sttContext           `json:"context,omitempty"`
	Translation                  *sttTranslationConfig `json:"translation,omitempty"`
}

// sttStartPayload marshals the start frame with the caller's allow-listed
// provider_options (see gateway/stt_support.go) written beside the adapter's
// own fields. The allow-list holds only endpoint tuning, which the adapter
// never sets itself, so a caller value cannot shadow a field above.
func sttStartPayload(start sttStartRequest, settings map[string]any) ([]byte, error) {
	payload, err := json.Marshal(start)
	if err != nil || len(settings) == 0 {
		return payload, err
	}
	frame := map[string]any{}
	if err := json.Unmarshal(payload, &frame); err != nil {
		return nil, err
	}
	for key, value := range settings {
		if _, owned := frame[key]; !owned {
			frame[key] = value
		}
	}
	return json.Marshal(frame)
}

// sttTranslationConfig is Soniox's translation block. Only one_way is sent:
// two_way needs both languages named, and the canonical option names one.
type sttTranslationConfig struct {
	Type           string `json:"type"`
	TargetLanguage string `json:"target_language"`
}

// sttTranslationStatusTranslation marks a translated token. "original"
// (spoken words that were translated), "none" (spoken words that were not)
// and an absent status are all transcript.
const sttTranslationStatusTranslation = "translation"

// sttTranslationFor builds the one-way block, or nil when no translation
// was asked. The target goes through the same subtag-and-alias rule as the
// language hint: Soniox lists bare codes ("es", "zh", "no") and refuses
// anything else, so "pt-BR" must be sent as "pt" and "nb" as "no".
func sttTranslationFor(target string) *sttTranslationConfig {
	language, ok := sonioxPrimaryLanguage(target)
	if !ok {
		return nil
	}
	return &sttTranslationConfig{Type: "one_way", TargetLanguage: language}
}

// sttContext biases recognition toward the caller's domain terms. Soniox's
// complete context envelope is capped at roughly 10k characters, so terms are
// deduplicated and cut off well below that ceiling.
type sttContext struct {
	Terms []string `json:"terms"`
}

// sttContextTerms deduplicates the caller's keywords for the start frame's
// context envelope. No truncation: the protocol's own bounds (at most 100
// keywords of 64 characters) keep the total under Soniox's ~10k-character
// context ceiling, and silently dropping a term would be the
// accepted-but-unfulfilled outcome these options exist to prevent.
func sttContextTerms(keywords []string) *sttContext {
	if len(keywords) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(keywords))
	terms := make([]string, 0, len(keywords))
	for _, keyword := range keywords {
		if _, duplicate := seen[keyword]; duplicate {
			continue
		}
		seen[keyword] = struct{}{}
		terms = append(terms, keyword)
	}
	return &sttContext{Terms: terms}
}

type sttToken struct {
	Text       string   `json:"text"`
	StartMS    *int64   `json:"start_ms"`
	EndMS      *int64   `json:"end_ms"`
	Confidence *float64 `json:"confidence"`
	IsFinal    bool     `json:"is_final"`
	// TranslationStatus is "original", "translation" or "none" on a
	// translating session and absent otherwise.
	TranslationStatus string `json:"translation_status"`
	// Language is the identified language, present only when the start
	// frame enabled language identification.
	Language string `json:"language"`
	// Speaker is the vendor's label ("1", "2", ...), present on every token
	// when the start frame enabled speaker diarization
	// (soniox.com/docs/stt/concepts/speaker-diarization, read 2026-11-19).
	// Soniox warns that real-time labels can switch and then settle as it
	// hears more, so a non-final token's label is provisional.
	Speaker string `json:"speaker"`
}

type sttInboundMessage struct {
	Tokens           []sttToken `json:"tokens"`
	FinalAudioProcMS int64      `json:"final_audio_proc_ms"`
	TotalAudioProcMS int64      `json:"total_audio_proc_ms"`
	Finished         bool       `json:"finished"`
	ErrorCode        int        `json:"error_code"`
	ErrorType        string     `json:"error_type"`
	ErrorMessage     string     `json:"error_message"`
	RequestID        string     `json:"request_id"`
}

// sonioxErrorCode maps a Soniox error frame onto the gateway's stable
// classification. Soniox states that error_type is stable across releases and
// error_message is not, so the type is the discriminator and the HTTP code is
// only a fallback for a type this adapter has not seen.
func sonioxErrorCode(errorType string, status int) (string, bool) {
	switch errorType {
	case "unauthenticated", "temp_api_key_session_expired":
		// A temporary key whose max_session_duration_seconds elapsed is not a
		// transient failure: only minting a new key clears it.
		return "authentication_failed", false
	case "organization_balance_exhausted", "organization_monthly_budget_exhausted", "project_monthly_budget_exhausted":
		return "provider_quota_exceeded", false
	case "limit_exceeded":
		return "provider_rate_limited", true
	case "invalid_request", "invalid_stream_state", "model_not_available", "max_concurrent_streams_reached", "invalid_audio_file", "invalid_cursor":
		return "invalid_request", false
	case "max_audio_duration_reached":
		// TTS only (HTTP 413): the stream hit Soniox's fixed two-minute audio
		// cap and its output was truncated. Retrying the same text on the
		// same budget would truncate again, so it is the request's fault.
		return "invalid_request", false
	case "request_timeout", "internal_error", "service_unavailable", "max_duration_reached":
		// max_duration_reached is retryable in the sense Soniox documents:
		// open a new connection and keep going.
		return "provider_unavailable", true
	}
	return sonioxStatusCode(status)
}

func sonioxStatusCode(status int) (string, bool) {
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return "authentication_failed", false
	case status == http.StatusPaymentRequired:
		return "provider_quota_exceeded", false
	case status == http.StatusTooManyRequests:
		return "provider_rate_limited", true
	case status == http.StatusRequestTimeout:
		return "provider_unavailable", true
	case status >= 400 && status < 500:
		return "invalid_request", false
	}
	return "provider_unavailable", true
}

func sonioxErrorMessage(prefix, message string) string {
	if strings.TrimSpace(message) == "" {
		return prefix
	}
	return prefix + ": " + message
}

func sonioxIsNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}

func sonioxUsageData(requestID string, processedMS *int64) json.RawMessage {
	data := map[string]any{"provider_request_id": requestID}
	if processedMS != nil {
		data["audio_processed_ms"] = *processedMS
	}
	return sonioxMarshalData(data)
}

func sonioxMarshalData(value any) json.RawMessage {
	payload, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{"code":"internal"}`)
	}
	return payload
}
