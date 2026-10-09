package soniox

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

const (
	// TTSAdapterID is the identifier returned by a Soniox TTS session plan.
	TTSAdapterID = "soniox.tts.v1"

	ttsExtensionID  = "soniox.com/tts/v1"
	ttsOfficialHost = "tts-rt.soniox.com"
	ttsEndpointPath = "/tts-websocket"
)

// ttsSampleRates are the output rates Soniox documents for pcm_s16le. Anything
// else is refused with HTTP 400 after the socket is already open, so it is
// cheaper to refuse it here.
var ttsSampleRates = map[int]struct{}{
	8_000:  {},
	16_000: {},
	24_000: {},
	44_100: {},
	48_000: {},
}

// TTSConfig controls local transport limits. Credentials, model, voice, and
// language always come from the signed session plan and its request options.
type TTSConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// TTSAdapter implements Soniox's /tts-websocket realtime API.
type TTSAdapter struct {
	id              string
	httpClient      *http.Client
	eventBuffer     int
	maxMessageBytes int64
	endpointPolicy  upstream.WebSocketPolicy
}

// NewTTS creates a bounded Soniox TTS adapter.
func NewTTS(config TTSConfig) (*TTSAdapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = TTSAdapterID
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
	endpointPolicy, err := upstream.NewWebSocketPolicy(ttsOfficialHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &TTSAdapter{
		id:              config.AdapterID,
		httpClient:      config.HTTPClient,
		eventBuffer:     config.EventBuffer,
		maxMessageBytes: config.MaxMessageBytes,
		endpointPolicy:  endpointPolicy,
	}, nil
}

func (a *TTSAdapter) ID() string { return a.id }

// Open dials the socket, authenticated by the handshake's Authorization
// header, and starts no stream: the first AppendText does. A finished
// utterance releases its streams, and the next AppendText starts a fresh one
// on the same socket.
func (a *TTSAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindTTS {
		return nil, fmt.Errorf("soniox tts supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "soniox" {
		return nil, fmt.Errorf("soniox adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("soniox tts requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("soniox tts requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("soniox tts media: %w", err)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model == "" || model == "auto" {
		return nil, errors.New("soniox tts requires a concrete model in the session plan")
	}
	// voice, language, and audio_format are all documented as required start
	// fields; Soniox answers a missing one with HTTP 400 on an open socket.
	voice := strings.TrimSpace(request.Options.Voice)
	if voice == "" {
		return nil, errors.New("soniox tts requires a voice in request options")
	}
	language, ok := sonioxPrimaryLanguage(request.Options.Language)
	if !ok {
		return nil, errors.New("soniox tts requires a concrete language in request options")
	}
	if request.Media.Encoding != "pcm_s16le" {
		return nil, fmt.Errorf("soniox tts streaming output requires pcm_s16le, got %q", request.Media.Encoding)
	}
	if _, ok := ttsSampleRates[request.Media.SampleRateHz]; !ok {
		return nil, fmt.Errorf("soniox tts does not support sample rate %d", request.Media.SampleRateHz)
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("soniox tts requires a bearer credential")
	}
	endpoint, err := ttsEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}

	// The key rides the handshake's Authorization header for managed, BYOK,
	// and relay credentials alike, and authenticates every stream the socket
	// runs. See doc.go.
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

	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &ttsStream{
		conn:       conn,
		ctx:        streamCtx,
		cancel:     cancel,
		events:     make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		model:      model,
		voice:      voice,
		language:   language,
		sampleRate: request.Media.SampleRateHz,
		// Resolved once at Open and replayed on every start message: one
		// socket can run several streams in sequence, and Soniox writes one
		// usage-log entry per STREAM, not per socket. A reference captured
		// only for the first stream would leave every later stream on the
		// same session unattributable.
		clientReferenceID: reservationReference(request.Plan),
	}
	// No stream starts here. The handshake authenticates the connection, and
	// Soniox ends a started stream that receives no text within a few seconds
	// with request_timeout, so the first AppendText starts the first stream.
	go stream.readLoop()
	return stream, nil
}

func ttsEndpoint(policy upstream.WebSocketPolicy, rawEndpoint string) (string, error) {
	endpoint, err := policy.Parse(rawEndpoint)
	if err != nil {
		return "", fmt.Errorf("soniox tts endpoint: %w", err)
	}
	if endpoint.Path != ttsEndpointPath {
		return "", fmt.Errorf("soniox tts endpoint path must be %s, got %q", ttsEndpointPath, endpoint.Path)
	}
	return endpoint.String(), nil
}

type ttsStream struct {
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent

	model             string
	voice             string
	language          string
	sampleRate        int
	clientReferenceID string

	// sendMu orders every decide-then-write sequence. The caller's
	// AppendText and the read loop starting a queued stream both put frames
	// on the socket, and a stream's start must reach Soniox before its text,
	// and its text before its text_end, whichever goroutine sends them.
	sendMu sync.Mutex
	// eventsMu orders events emitted on a caller's goroutine (CommitText)
	// against readLoop closing events; eventsClosed is set under it.
	eventsMu     sync.RWMutex
	eventsClosed bool
	writeMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	closing      atomic.Bool
	closeErr     error

	stateMu   sync.Mutex
	utterance *ttsUtterance
	requestID string
	// canceledStreamID is the last stream Cancel targeted. A cancel can
	// cross that stream's terminated on the wire, and Soniox then rejects it
	// as addressed to an unknown stream.
	canceledStreamID string
}

// ttsUtterance is the caller's unit of speech: everything appended between
// one CommitText and the next. Soniox caps one stream at two minutes of audio,
// so an utterance longer than ttsStreamTextBudget is spread over several
// Soniox streams run back to back on this socket, one at a time. The runtime
// sees one utterance: one audio.started, frames in order, alignment measured
// from the utterance's first sample, and one audio.done when its last stream
// terminates.
type ttsUtterance struct {
	done chan struct{}

	committed    bool
	canceled     bool
	audioStarted bool
	// pending is text accepted for this utterance that no stream carries
	// yet, because the active stream is full and has been sent text_end. It
	// never starts with whitespace.
	pending string
	// queued holds appends that arrive while the active stream is closed for
	// input. They are joined onto pending once, when the next stream starts,
	// so a long reply arriving in small chunks is not recopied per append.
	queued []string
	// priorAudioBytes is the PCM this utterance's finished streams produced.
	// Soniox times each stream from its own first sample, so it is the
	// offset that places the active stream's timestamps on the utterance.
	priorAudioBytes int64
	lastStreamID    string

	// The active Soniox stream. streamID is empty only between the
	// utterance's last stream terminating and the utterance completing.
	streamID         string
	textEnded        bool
	streamCost       int
	streamBoundary   ttsBoundary
	streamAudioBytes int64
}

func (s *ttsStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

func (s *ttsStream) WriteAudio(context.Context, []byte) error {
	return runtimepkg.ErrUnsupportedOperation
}

func (s *ttsStream) CommitAudio(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// AppendText adds text to the current utterance, starting one if none is in
// flight. Text never fails for length: whatever does not fit the active
// stream's budget waits for the next stream of the same utterance.
func (s *ttsStream) AppendText(ctx context.Context, text string) error {
	if strings.TrimSpace(text) == "" {
		return errors.New("soniox tts text is empty")
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	s.stateMu.Lock()
	if s.closed.Load() || s.closing.Load() {
		s.stateMu.Unlock()
		return runtimepkg.ErrSessionClosed
	}
	utterance := s.utterance
	if utterance != nil && (utterance.committed || utterance.canceled) {
		s.stateMu.Unlock()
		return errors.New("soniox tts stream is closed for input until it terminates")
	}
	if utterance == nil {
		utterance = &ttsUtterance{done: make(chan struct{})}
		s.utterance = utterance
	}
	if utterance.textEnded {
		utterance.queued = append(utterance.queued, text)
		s.stateMu.Unlock()
		return nil
	}
	utterance.pending += text
	messages, err := s.planLocked(utterance)
	s.stateMu.Unlock()
	if err != nil {
		return err
	}
	return s.send(ctx, messages)
}

// CommitText ends the utterance's input with the documented end-of-input
// marker, an empty text chunk carrying text_end, which Soniox's own reference
// client sends after its last real chunk. When the utterance has rolled over
// and text is still queued, the marker goes to the utterance's last stream
// once the read loop starts it.
func (s *ttsStream) CommitText(ctx context.Context) error {
	s.sendMu.Lock()
	s.stateMu.Lock()
	utterance := s.utterance
	if s.closed.Load() || s.closing.Load() || (utterance != nil && (utterance.committed || utterance.canceled)) {
		s.stateMu.Unlock()
		s.sendMu.Unlock()
		return runtimepkg.ErrSessionClosed
	}
	if utterance == nil {
		// A commit with no text: nothing to synthesize, so no Soniox stream
		// is opened (one with no text would end in request_timeout). The
		// caller gets the same audio.done an empty stream used to produce.
		s.stateMu.Unlock()
		s.sendMu.Unlock()
		return s.emitFromCaller(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: s.ttsStreamData("")})
	}
	utterance.committed = true
	messages, err := s.planLocked(utterance)
	// Unreachable while the invariants hold: a stream that ends early always
	// leaves text queued for the next one. Completing here keeps a broken
	// invariant from leaving Close waiting forever.
	complete := err == nil && utterance.streamID == "" && utterance.pending == ""
	if complete {
		s.completeLocked(utterance)
	}
	s.stateMu.Unlock()
	if err == nil {
		err = s.send(ctx, messages)
	}
	s.sendMu.Unlock()
	if err != nil {
		return err
	}
	if complete {
		return s.emitFromCaller(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: s.ttsStreamData(utterance.lastStreamID)})
	}
	return nil
}

// Cancel stops the utterance: the active stream is canceled and text queued
// for its later streams is dropped. Soniox rejects a cancel that also carries
// text or text_end, so it is sent on its own; the server answers with
// terminated and sends no further audio.
func (s *ttsStream) Cancel(ctx context.Context) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	s.stateMu.Lock()
	utterance := s.utterance
	if utterance == nil || utterance.streamID == "" {
		s.stateMu.Unlock()
		return runtimepkg.ErrSessionClosed
	}
	utterance.canceled = true
	utterance.pending = ""
	utterance.queued = nil
	streamID := utterance.streamID
	s.canceledStreamID = streamID
	s.stateMu.Unlock()
	return s.writeJSON(ctx, map[string]any{"stream_id": streamID, "cancel": true})
}

// Close waits for the utterance in flight, every queued stream of it
// included, before closing the socket, so audio still owed after CommitText
// is not discarded.
func (s *ttsStream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		s.closing.Store(true)
		if done := s.utteranceDone(); done != nil {
			select {
			case <-done:
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
func (s *ttsStream) Abort(context.Context) error { return s.abort() }

func (s *ttsStream) abort() error {
	s.abortOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		if err := s.conn.CloseNow(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
		s.dropUtterance()
	})
	return s.closeErr
}

// planLocked moves the utterance's queued text onto Soniox streams and
// returns the frames that do it, in wire order. It starts a stream when none
// is active, fills the active stream up to ttsStreamTextBudget, and when the
// text does not fit, sends what does and ends the stream with text_end; the
// rest stays queued until that stream terminates, because audio must reach
// the caller in order and this adapter runs one Soniox stream at a time.
//
// Called with stateMu held, and under sendMu so the frames are written before
// anyone plans further.
func (s *ttsStream) planLocked(utterance *ttsUtterance) ([]any, error) {
	var messages []any
	// Queued appends join once a stream can take them: when the next
	// stream is about to start, or the active one is still open for input.
	if len(utterance.queued) > 0 && (utterance.streamID == "" || !utterance.textEnded) {
		utterance.pending = strings.TrimLeftFunc(utterance.pending+strings.Join(utterance.queued, ""), unicode.IsSpace)
		utterance.queued = nil
	}
	for {
		if utterance.streamID == "" {
			if utterance.canceled || utterance.pending == "" {
				return messages, nil
			}
			streamID, err := newStreamID()
			if err != nil {
				return nil, err
			}
			utterance.streamID = streamID
			utterance.textEnded = false
			utterance.streamCost = 0
			utterance.streamBoundary = ttsBoundaryNone
			utterance.streamAudioBytes = 0
			// Every stream carries the reservation reference: Soniox writes
			// one usage-log entry per stream, so a rolled-over utterance is
			// several entries, and each must be attributable.
			messages = append(messages, ttsStartRequest{
				StreamID:          streamID,
				Model:             s.model,
				Language:          s.language,
				Voice:             s.voice,
				AudioFormat:       "pcm_s16le",
				SampleRate:        s.sampleRate,
				ClientReferenceID: s.clientReferenceID,
				ReturnTimestamps:  true,
			})
		}
		if utterance.textEnded {
			return messages, nil
		}
		if utterance.pending == "" {
			if utterance.committed {
				messages = append(messages, ttsTextRequest{Text: "", TextEnd: true, StreamID: utterance.streamID})
				utterance.textEnded = true
			}
			return messages, nil
		}
		head, tail, roll := ttsSplitForStream(utterance.pending, utterance.streamCost, utterance.streamBoundary, utterance.committed)
		if head != "" {
			messages = append(messages, ttsTextRequest{Text: head, StreamID: utterance.streamID})
			utterance.streamCost += ttsTextCost(head)
			utterance.streamBoundary = ttsBoundaryAfter(utterance.streamBoundary, head)
		}
		utterance.pending = tail
		if !roll {
			continue
		}
		messages = append(messages, ttsTextRequest{Text: "", TextEnd: true, StreamID: utterance.streamID})
		utterance.textEnded = true
		return messages, nil
	}
}

func (s *ttsStream) send(ctx context.Context, messages []any) error {
	for _, message := range messages {
		if err := s.writeJSON(ctx, message); err != nil {
			return err
		}
	}
	return nil
}

// streamTerminated handles Soniox's terminated for streamID. The stream's
// audio is complete. If the utterance still has text queued, its next stream
// starts now; otherwise the utterance is complete and the caller is told so,
// exactly once.
func (s *ttsStream) streamTerminated(streamID string, raw json.RawMessage) error {
	s.sendMu.Lock()
	s.stateMu.Lock()
	utterance := s.utterance
	if utterance == nil || utterance.streamID != streamID {
		// Not a stream this adapter is waiting on; nothing to finish.
		s.stateMu.Unlock()
		s.sendMu.Unlock()
		return nil
	}
	utterance.priorAudioBytes += utterance.streamAudioBytes
	utterance.lastStreamID = streamID
	utterance.streamID = ""
	var (
		messages []any
		err      error
	)
	// A stream that ends without having been sent text_end was canceled or
	// ended by the server; either way nothing queued behind it is wanted.
	complete := utterance.canceled || !utterance.textEnded || (utterance.pending == "" && len(utterance.queued) == 0)
	if complete {
		s.completeLocked(utterance)
	} else {
		messages, err = s.planLocked(utterance)
	}
	s.stateMu.Unlock()
	if err == nil {
		err = s.send(s.ctx, messages)
	}
	s.sendMu.Unlock()
	if err != nil {
		return err
	}
	if !complete {
		return nil
	}
	return s.emit(runtimepkg.ProviderEvent{
		Type:       protocol.EventAudioDone,
		Data:       s.ttsStreamData(streamID),
		Extensions: ttsExtension(raw),
	})
}

// completeLocked retires the utterance. Called with stateMu held.
func (s *ttsStream) completeLocked(utterance *ttsUtterance) {
	if s.utterance == utterance {
		s.utterance = nil
	}
	close(utterance.done)
}

func (s *ttsStream) dropUtterance() {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.utterance != nil {
		s.completeLocked(s.utterance)
	}
}

func (s *ttsStream) utteranceDone() <-chan struct{} {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.utterance == nil {
		return nil
	}
	return s.utterance.done
}

// observeAudio counts a frame of the active stream's PCM and reports whether
// it is the first audio of the utterance, which is when audio.started fires:
// later streams of a rolled-over utterance continue the same audio.
func (s *ttsStream) observeAudio(streamID string, bytes int) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	utterance := s.utterance
	if utterance == nil || utterance.streamID != streamID {
		return false
	}
	utterance.streamAudioBytes += int64(bytes)
	if utterance.audioStarted {
		return false
	}
	utterance.audioStarted = true
	return true
}

// audioOffsetSeconds is where streamID's first sample falls in its
// utterance's audio.
func (s *ttsStream) audioOffsetSeconds(streamID string) float64 {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	utterance := s.utterance
	if utterance == nil || utterance.streamID != streamID || s.sampleRate <= 0 {
		return 0
	}
	// pcm_s16le is two bytes a sample, and Soniox synthesizes mono.
	return float64(utterance.priorAudioBytes) / float64(2*s.sampleRate)
}

// isLateCancelRejection reports an error about the stream Cancel last
// targeted after that stream has terminated: the only thing still addressed
// to it is the cancel itself.
func (s *ttsStream) isLateCancelRejection(streamID string) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if streamID == "" || streamID != s.canceledStreamID {
		return false
	}
	return s.utterance == nil || s.utterance.streamID != streamID
}

func (s *ttsStream) setRequestID(value string) bool {
	if value == "" {
		return false
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.requestID == value {
		return false
	}
	s.requestID = value
	return true
}

func (s *ttsStream) currentRequestID() string {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.requestID
}

func (s *ttsStream) writeJSON(ctx context.Context, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	// Soniox refuses binary frames on this endpoint; every client message is a
	// JSON text frame.
	if err := s.conn.Write(ctx, websocket.MessageText, payload); err != nil {
		return &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "Soniox streaming write failed",
			Retryable: true,
			Cause:     err,
		}
	}
	return nil
}

func (s *ttsStream) readLoop() {
	defer func() {
		// Cancel first: a caller blocked in emitFromCaller then returns and
		// releases its read lock, so the close below cannot deadlock.
		s.cancel()
		s.dropUtterance()
		s.eventsMu.Lock()
		s.eventsClosed = true
		close(s.events)
		s.eventsMu.Unlock()
	}()
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && s.ctx.Err() == nil && !sonioxIsNormalClose(err) {
				s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{
					Code:      "provider_unavailable",
					Message:   "Soniox streaming read failed",
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
	}
}

func (s *ttsStream) handleMessage(payload []byte) error {
	var message ttsInboundMessage
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
		if s.isLateCancelRejection(message.StreamID) {
			// Soniox refused a cancel because the stream had already
			// terminated on its own; the utterance outcome was decided by
			// that terminated, and no audio is affected.
			return nil
		}
		// Soniox keeps the connection open and terminates only the failed
		// stream, but a failed stream leaves its utterance with a hole in
		// it, so the failure is the attempt's failure and the runtime fails
		// it over.
		code, retryable := sonioxErrorCode(message.ErrorType, message.ErrorCode)
		providerError := &runtimepkg.ProviderError{
			Code:           code,
			Message:        sonioxErrorMessage("Soniox reported a streaming error", message.ErrorMessage),
			Retryable:      retryable,
			ProviderStatus: message.ErrorCode,
			Extensions:     ttsExtension(raw),
		}
		if message.ErrorType == "max_audio_duration_reached" {
			// The adapter budgets each stream well under the cap, so this
			// means text that speaks far slower than the budget assumes.
			providerError.Message = sonioxErrorMessage("Soniox truncated the synthesis at its two-minute per-stream audio cap", message.ErrorMessage)
			providerError.Hint = "Soniox caps each stream at two minutes of audio; this text produced more audio per character than the adapter's per-stream budget allows for. Split it into shorter utterances."
		}
		return providerError
	}
	if s.setRequestID(message.RequestID) {
		if err := s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventUsageObserved,
			Data:       sonioxUsageData(message.RequestID, nil),
			Extensions: ttsExtension(raw),
		}); err != nil {
			return err
		}
	}

	// The offset is read before this frame's bytes are counted: it covers the
	// utterance's earlier streams only, and Soniox times this frame's
	// characters from its own stream's first sample.
	offset := s.audioOffsetSeconds(message.StreamID)
	if message.Audio != "" {
		audio, err := base64.StdEncoding.DecodeString(message.Audio)
		if err != nil {
			return &runtimepkg.ProviderError{
				Code:      "provider_unavailable",
				Message:   "Soniox sent invalid audio data",
				Retryable: true,
				Cause:     err,
			}
		}
		if s.observeAudio(message.StreamID, len(audio)) {
			if err := s.emit(runtimepkg.ProviderEvent{
				Type:       protocol.EventAudioStarted,
				Data:       s.ttsStreamData(message.StreamID),
				Extensions: ttsExtension(raw),
			}); err != nil {
				return err
			}
		}
		if err := s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventAudioFrame,
			Data:       s.ttsStreamData(message.StreamID),
			Extensions: ttsExtension(raw),
			Audio:      audio,
		}); err != nil {
			return err
		}
	}
	if message.Timestamps != nil {
		if err := s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventAlignment,
			Data:       s.ttsAlignmentData(message.StreamID, message.Timestamps, offset),
			Extensions: ttsExtension(raw),
		}); err != nil {
			return err
		}
	}
	// audio_end only promises that no further audio frames follow; Soniox is
	// explicit that the stream is complete at terminated, not before, so the
	// terminal event is bound to terminated alone, and to the utterance's
	// last stream.
	if message.Terminated {
		return s.streamTerminated(message.StreamID, raw)
	}
	return nil
}

// emitFromCaller is emit for a goroutine other than readLoop. readLoop owns
// events and closes it on exit, and a send on a closed channel panics even
// when ctx is also done, so the send happens under eventsMu.
func (s *ttsStream) emitFromCaller(event runtimepkg.ProviderEvent) error {
	s.eventsMu.RLock()
	defer s.eventsMu.RUnlock()
	if s.eventsClosed || s.ctx.Err() != nil {
		return runtimepkg.ErrSessionClosed
	}
	if err := s.emit(event); err != nil {
		// The reader exited while this send waited.
		return runtimepkg.ErrSessionClosed
	}
	return nil
}

func (s *ttsStream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *ttsStream) ttsStreamData(streamID string) json.RawMessage {
	return sonioxMarshalData(map[string]any{"stream_id": streamID, "provider_request_id": s.currentRequestID()})
}

// ttsAlignmentData carries Soniox's timestamps block and adds the normalized
// span reading beside it. Soniox measures per CHARACTER and reports start and
// END times in fractional seconds, from the first sample of its own stream.
//
// A rolled-over utterance is several streams, so offsetSeconds, the audio its
// earlier streams produced, is added to every time in both readings: the
// caller plays one continuous utterance, and a span must point into it. The
// unshifted block stays in the raw frame under the extension key.
//
// A block that does not parse, or whose three parallel arrays disagree, is
// carried raw with no spans, so a garbled frame goes out silent rather than
// wrong.
func (s *ttsStream) ttsAlignmentData(streamID string, timestamps json.RawMessage, offsetSeconds float64) json.RawMessage {
	payload := map[string]any{
		"stream_id":            streamID,
		"character_timestamps": timestamps,
		"provider_request_id":  s.currentRequestID(),
	}
	var block struct {
		Characters []string  `json:"characters"`
		Start      []float64 `json:"character_start_times_seconds"`
		End        []float64 `json:"character_end_times_seconds"`
	}
	if err := json.Unmarshal(timestamps, &block); err == nil {
		if offsetSeconds > 0 {
			for index := range block.Start {
				block.Start[index] += offsetSeconds
			}
			for index := range block.End {
				block.End[index] += offsetSeconds
			}
			payload["character_timestamps"] = block
		}
		timings := protocol.TimingSpansFromSeconds(block.Characters, block.Start, block.End, protocol.TimingGranularityCharacter)
		if len(timings.Spans) > 0 {
			payload["granularity"], payload["spans"] = timings.Granularity, timings.Spans
		}
	}
	return sonioxMarshalData(payload)
}

func ttsExtension(raw json.RawMessage) map[string]json.RawMessage {
	return map[string]json.RawMessage{ttsExtensionID: raw}
}

func newStreamID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("generate Soniox stream id: %w", err)
	}
	return hex.EncodeToString(bytes), nil
}

type ttsStartRequest struct {
	StreamID    string `json:"stream_id"`
	Model       string `json:"model"`
	Language    string `json:"language"`
	Voice       string `json:"voice"`
	AudioFormat string `json:"audio_format"`
	SampleRate  int    `json:"sample_rate"`
	// client_reference_id is what binds this synthesis to a Speko
	// reservation in Soniox's usage log; omitempty keeps it off the wire for
	// BYOK plans, which bill the customer's own project. Documented in the
	// TTS WebSocket reference between sample_rate/bitrate and
	// return_timestamps, with the same semantics as the STT start message.
	ClientReferenceID string `json:"client_reference_id,omitempty"`
	// ReturnTimestamps asks for the per-character timing block. It is always
	// on: the timestamps ride the same messages as the audio, and measuring
	// five runs each way found the cost inside the noise, so making it
	// conditional would buy nothing and add a second start-frame shape.
	ReturnTimestamps bool `json:"return_timestamps"`
}

type ttsTextRequest struct {
	Text     string `json:"text"`
	TextEnd  bool   `json:"text_end"`
	StreamID string `json:"stream_id"`
}

type ttsInboundMessage struct {
	StreamID     string          `json:"stream_id"`
	Audio        string          `json:"audio"`
	AudioEnd     bool            `json:"audio_end"`
	Terminated   bool            `json:"terminated"`
	Timestamps   json.RawMessage `json:"timestamps"`
	ErrorCode    int             `json:"error_code"`
	ErrorType    string          `json:"error_type"`
	ErrorMessage string          `json:"error_message"`
	RequestID    string          `json:"request_id"`
}
