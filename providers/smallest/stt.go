package smallest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

const (
	// STTAdapterID is the identifier returned by a Smallest STT session plan.
	STTAdapterID = "smallest.stt.v1"

	// sttEndpointPath is the Pulse realtime socket. The pre-recorded twin is a
	// different resource on a different transport (HTTP POST to
	// /waves/v1/stt/ and /waves/v1/pulse/get_text) and is not this adapter.
	sttEndpointPath = "/waves/v1/stt/live"

	// DefaultSTTModel is the only model that drives the live socket. The
	// adapter never defaults — a plan must name a concrete model — but the
	// control plane and the tests need one agreed name for "unpinned Pulse".
	DefaultSTTModel = "pulse"

	// sttPrerecordedOnlyModel has no streaming worker. Smallest answers a live
	// connection pinned to it with HTTP 400 and a directive to use the
	// pre-recorded endpoint, so catching it locally turns a billed round trip
	// into an immediate, legible error.
	sttPrerecordedOnlyModel = "pulse-pro"
)

// Pulse control frames. Both are JSON text messages on the same socket that
// carries binary audio.
const (
	// sttFinalize forces an immediate is_final transcript and keeps the session
	// open. It explicitly does NOT set is_last.
	sttFinalize = `{"type":"finalize"}`
	// sttCloseStream flushes remaining audio, produces one last transcript with
	// is_last true, and ends the session.
	sttCloseStream = `{"type":"close_stream"}`
)

// sttEncodings maps the canonical MediaFormat encoding onto Pulse's `encoding`
// query value. Pulse also accepts linear32, alaw, mulaw, and ogg_opus, none of
// which the canonical protocol can express today.
var sttEncodings = map[string]string{
	"pcm_s16le": "linear16",
	"opus":      "opus",
}

// sttSampleRates is the documented set for the WebSocket surface. The canonical
// MediaFormat range is far wider, so the adapter has to narrow it.
var sttSampleRates = map[int]struct{}{
	8_000: {}, 16_000: {}, 22_050: {}, 24_000: {}, 44_100: {}, 48_000: {},
}

// STTConfig controls local transport limits and endpoint policy. Provider
// identity, model, and credential all come from the signed session plan.
type STTConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// STTAdapter implements Smallest's Pulse /waves/v1/stt/live streaming API.
type STTAdapter struct {
	id              string
	httpClient      *http.Client
	eventBuffer     int
	maxMessageBytes int64
	endpointPolicy  upstream.WebSocketPolicy
}

// NewSTT creates a bounded Smallest STT adapter.
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
		return nil, errors.New("smallest stt event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("smallest stt maximum message bytes must be positive")
	}
	endpointPolicy, err := upstream.NewWebSocketPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &STTAdapter{
		id:              config.AdapterID,
		httpClient:      config.HTTPClient,
		eventBuffer:     config.EventBuffer,
		maxMessageBytes: config.MaxMessageBytes,
		endpointPolicy:  endpointPolicy,
	}, nil
}

func (a *STTAdapter) ID() string { return a.id }

// Open dials the Pulse realtime socket. Pulse does its own server-side
// endpointing, so this is a plain forwarder: no local VAD, no segmenter.
func (a *STTAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindSTT {
		return nil, fmt.Errorf("smallest stt supports stt sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "smallest" {
		return nil, fmt.Errorf("smallest stt adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("smallest stt requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("smallest stt requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("smallest stt media: %w", err)
	}
	encoding, err := validateSTTOptions(request.Plan.Route.Model, *request.Media)
	if err != nil {
		return nil, err
	}
	credential, err := requireBYOK(request, "smallest stt")
	if err != nil {
		return nil, err
	}
	endpoint, err := sttEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint, request.Plan.Route.Model, request.Options, *request.Media, encoding)
	if err != nil {
		return nil, err
	}
	headers := make(http.Header, 1)
	headers.Set("Authorization", "Bearer "+credential)

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
			Message:        "Smallest transcription connection could not be established",
			Retryable:      status == 0 || status == http.StatusTooManyRequests || status >= 500,
			ProviderStatus: status,
			Cause:          err,
		}
	}
	conn.SetReadLimit(a.maxMessageBytes)
	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &sttStream{
		conn:   conn,
		ctx:    streamCtx,
		cancel: cancel,
		events: make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		done:   make(chan struct{}),
	}
	go stream.readLoop()
	return stream, nil
}

func validateSTTOptions(model string, media protocol.MediaFormat) (string, error) {
	if strings.TrimSpace(model) == "" || model == "auto" {
		return "", errors.New("smallest stt requires a concrete model in the session plan")
	}
	if model == sttPrerecordedOnlyModel {
		return "", fmt.Errorf("smallest stt model %q is pre-recorded only and cannot drive the live socket; use %q", sttPrerecordedOnlyModel, DefaultSTTModel)
	}
	encoding, ok := sttEncodings[media.Encoding]
	if !ok {
		return "", fmt.Errorf("smallest stt cannot stream encoding %q", media.Encoding)
	}
	if media.Channels != 1 {
		// Pulse states single-channel only; multi-channel is "coming soon".
		return "", fmt.Errorf("smallest stt requires mono audio, got %d channels", media.Channels)
	}
	if _, ok := sttSampleRates[media.SampleRateHz]; !ok {
		return "", fmt.Errorf("smallest stt does not accept sample rate %d Hz", media.SampleRateHz)
	}
	return encoding, nil
}

func sttEndpoint(policy upstream.WebSocketPolicy, rawEndpoint, model string, options protocol.RequestOptions, media protocol.MediaFormat, encoding string) (string, error) {
	endpoint, err := policy.Parse(rawEndpoint)
	if err != nil {
		return "", fmt.Errorf("smallest stt endpoint: %w", err)
	}
	if endpoint.Path != sttEndpointPath {
		return "", fmt.Errorf("smallest stt endpoint path must be %s, got %q", sttEndpointPath, endpoint.Path)
	}
	query := endpoint.Query()
	query.Set("model", model)
	query.Set("encoding", encoding)
	query.Set("sample_rate", strconv.Itoa(media.SampleRateHz))
	// Word timings are what let a downstream turn detector do adaptive
	// interruption, and Pulse only sends them when asked.
	query.Set("word_timestamps", "true")
	if language := normalizeLanguage(options.Language); language != "" {
		// Omitted entirely when the caller did not ask, so Pulse applies its own
		// default rather than this adapter pinning one.
		query.Set("language", language)
	}
	endpoint.RawQuery = query.Encode()
	return endpoint.String(), nil
}

type sttStream struct {
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent
	// done closes once the terminal is_last transcript has been seen, or the
	// read loop has given up. Close blocks on it because Smallest's docs are
	// explicit: do not close the socket straight after close_stream, or the
	// tail of the transcript is lost.
	done     chan struct{}
	doneOnce sync.Once
	// ended records that the read loop owns the shutdown — it saw is_last, or
	// it exited. It is always stored before `done` is signalled, so a Close
	// woken by `done` can distinguish "the socket is already gone, as designed"
	// from a genuine local close failure.
	ended atomic.Bool

	writeMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	closing      atomic.Bool
	closeErr     error

	stateMu           sync.Mutex
	speechOpen        bool
	providerRequestID string
}

func (s *sttStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

// WriteAudio forwards one frame of raw audio as a binary message. Smallest
// recommends ~4096-byte chunks for the latency/throughput trade-off; the
// runtime owns framing, so this deliberately does not re-chunk and hide what
// the caller actually sent.
func (s *sttStream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("smallest stt audio is empty")
	}
	return s.write(ctx, websocket.MessageBinary, audio)
}

// CommitAudio maps the canonical turn boundary onto Pulse's finalize control
// frame: it forces an immediate is_final transcript and leaves the socket warm
// for the next turn.
func (s *sttStream) CommitAudio(ctx context.Context) error {
	return s.write(ctx, websocket.MessageText, []byte(sttFinalize))
}

func (s *sttStream) AppendText(context.Context, string) error {
	return runtimepkg.ErrUnsupportedOperation
}

func (s *sttStream) CommitText(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

func (s *sttStream) Cancel(context.Context) error { return s.abort() }

func (s *sttStream) Abort(context.Context) error { return s.abort() }

// Close sends close_stream and waits for the is_last transcript before tearing
// the socket down.
func (s *sttStream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		s.closing.Store(true)
		select {
		case <-s.done:
			// The session already ended — is_last arrived, or the socket
			// dropped. close_stream would go to a dead peer, and a caller that
			// closes after a completed session has done nothing wrong.
			_ = s.abort()
			return
		default:
		}
		if err := s.write(ctx, websocket.MessageText, []byte(sttCloseStream)); err != nil {
			s.closeErr = err
			_ = s.abort()
			return
		}
		select {
		case <-s.done:
		case <-ctx.Done():
			s.closeErr = ctx.Err()
		}
		_ = s.abort()
	})
	return s.closeErr
}

func (s *sttStream) abort() error {
	s.abortOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		if err := s.conn.CloseNow(); err != nil && s.closeErr == nil && !s.ended.Load() {
			s.closeErr = err
		}
		s.signalDone()
	})
	return s.closeErr
}

func (s *sttStream) signalDone() {
	s.ended.Store(true)
	s.doneOnce.Do(func() { close(s.done) })
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
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Smallest transcription write failed", Retryable: true, Cause: err}
	}
	return nil
}

func (s *sttStream) readLoop() {
	defer func() {
		s.closed.Store(true)
		s.cancel()
		s.signalDone()
		close(s.events)
	}()
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && !s.closing.Load() && s.ctx.Err() == nil && !isNormalClose(err) {
				s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Smallest transcription read failed", Retryable: true, Cause: err}})
			}
			return
		}
		if messageType != websocket.MessageText {
			continue
		}
		terminal, err := s.handleMessage(payload)
		if err != nil {
			s.emit(runtimepkg.ProviderEvent{Err: err})
			return
		}
		if terminal {
			// Release Close before the deferred teardown so a caller waiting on
			// is_last does not also wait on the socket handshake.
			s.signalDone()
			_ = s.conn.Close(websocket.StatusNormalClosure, "")
			return
		}
	}
}

func (s *sttStream) handleMessage(payload []byte) (bool, error) {
	var message sttInbound
	if err := json.Unmarshal(payload, &message); err != nil {
		return false, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Smallest sent malformed transcription JSON", Retryable: true, Cause: err}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))
	// session_id is the only correlation id Pulse puts on a transcript frame.
	if message.SessionID != "" && s.setProviderRequestID(message.SessionID) {
		if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: usageData(message.SessionID)}); err != nil {
			return false, err
		}
	}
	// A transcript frame carries status "success". An error can arrive as
	// type "error", status "error", or a bare `error` string; Smallest
	// documents none of those shapes for this socket, so all three are
	// accepted rather than swallowed into a mystery timeout.
	if message.Type == "error" || message.Status == "error" || strings.TrimSpace(message.Error) != "" {
		return false, &runtimepkg.ProviderError{
			Code:           providerErrorCode(message.StatusCode),
			Message:        sttErrorMessage(firstNonEmpty(message.Error, message.Message)),
			Retryable:      message.StatusCode == 0 || message.StatusCode == http.StatusTooManyRequests || message.StatusCode >= 500,
			ProviderStatus: message.StatusCode,
			Extensions:     extension(raw),
		}
	}
	if message.Transcript != "" {
		// Pulse can emit an empty-string interim at session start; the guard
		// above keeps that from surfacing as a spurious transcript event.
		if s.openSpeech() {
			if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventSpeechStarted, Data: s.transcriptMetadata(), Extensions: extension(raw)}); err != nil {
				return false, err
			}
		}
		kind := protocol.EventTranscriptDelta
		if message.IsFinal {
			kind = protocol.EventTranscriptFinal
		}
		if err := s.emit(runtimepkg.ProviderEvent{Type: kind, Data: s.transcriptData(message), Extensions: extension(raw)}); err != nil {
			return false, err
		}
		if message.IsFinal {
			// Pulse finalizes a segment and keeps listening, so speech.ended
			// pairs with each final, not only with the end of the session.
			s.closeSpeech()
			if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventSpeechEnded, Data: s.transcriptMetadata(), Extensions: extension(raw)}); err != nil {
				return false, err
			}
		}
	}
	// is_last is the session terminator and always accompanies is_final.
	return message.IsLast, nil
}

func (s *sttStream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// openSpeech reports the first transcript of a new segment, so speech.started
// is emitted once per segment rather than once per interim frame.
func (s *sttStream) openSpeech() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.speechOpen {
		return false
	}
	s.speechOpen = true
	return true
}

func (s *sttStream) closeSpeech() {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.speechOpen = false
}

func (s *sttStream) setProviderRequestID(value string) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.providerRequestID == value {
		return false
	}
	s.providerRequestID = value
	return true
}

func (s *sttStream) currentProviderRequestID() string {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.providerRequestID
}

func (s *sttStream) transcriptData(message sttInbound) json.RawMessage {
	return marshalData(map[string]any{
		"text":                message.Transcript,
		"is_final":            message.IsFinal,
		"is_last":             message.IsLast,
		"language":            message.Language,
		"words":               message.Words,
		"provider_request_id": s.currentProviderRequestID(),
	})
}

func (s *sttStream) transcriptMetadata() json.RawMessage {
	return marshalData(map[string]any{"provider_request_id": s.currentProviderRequestID()})
}

func sttErrorMessage(message string) string {
	if strings.TrimSpace(message) == "" {
		return "Smallest reported a transcription error"
	}
	return "Smallest reported a transcription error: " + message
}

type sttInbound struct {
	Type       string `json:"type"`
	Status     string `json:"status"`
	SessionID  string `json:"session_id"`
	Transcript string `json:"transcript"`
	IsFinal    bool   `json:"is_final"`
	IsLast     bool   `json:"is_last"`
	// Language is only populated on is_final frames.
	Language string `json:"language"`
	// Words is present only when word_timestamps=true, which this adapter
	// always sets. Kept raw: the per-word object grows extra fields
	// (speaker, speaker_confidence) when diarization is on.
	Words      json.RawMessage `json:"words"`
	Message    string          `json:"message"`
	Error      string          `json:"error"`
	StatusCode int             `json:"status_code"`
}
