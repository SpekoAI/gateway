package gradium

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
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
	// STTAdapterID is the identifier returned by a Gradium STT session plan.
	STTAdapterID = "gradium.stt.v1"

	// extensionID namespaces the raw provider frame retained on every event.
	extensionID = "gradium.ai/v1"
	// officialAPIHost is the only hostname Gradium's published documentation
	// names for either socket. Regional hosts must be opted into explicitly.
	officialAPIHost = "api.gradium.ai"

	sttPath = "/api/speech/asr"

	// defaultDelayInFrames is the endpointing lookahead, counted in Gradium's
	// own 80 ms frames, that the documented STT setup example carries and that
	// the server echoes back in `ready`. It is exposed on STTConfig because it
	// is a deployment-wide latency/accuracy tradeoff, not a per-session route
	// choice the control plane makes.
	defaultDelayInFrames = 16
)

// sttLanguages is the exact set Gradium's transcription settings page
// enumerates for `json_config.language`. Anything else is rejected at Open
// rather than silently dropped: dropping it would leave the model to guess,
// and a caller that explicitly asked for a language Gradium cannot transcribe
// deserves a failed session rather than confident nonsense in another one.
var sttLanguages = map[string]struct{}{
	"en": {}, "fr": {}, "de": {}, "es": {}, "pt": {},
}

// STTConfig controls local transport limits and one provider model setting.
// Credentials and provider selection always come from the signed session plan,
// never from this configuration.
type STTConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	DelayInFrames         int
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// STTAdapter implements Gradium's /api/speech/asr WebSocket API.
type STTAdapter struct {
	id              string
	httpClient      *http.Client
	eventBuffer     int
	maxMessageBytes int64
	delayInFrames   int
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
	if config.DelayInFrames == 0 {
		config.DelayInFrames = defaultDelayInFrames
	}
	if config.EventBuffer < 1 {
		return nil, errors.New("gradium event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("gradium maximum message bytes must be positive")
	}
	if config.DelayInFrames < 0 {
		return nil, errors.New("gradium delay in frames must not be negative")
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
		delayInFrames:   config.DelayInFrames,
		endpointPolicy:  endpointPolicy,
	}, nil
}

func (a *STTAdapter) ID() string { return a.id }

// Open opens a provider-direct STT stream and completes Gradium's mandatory
// opening handshake before returning. Gradium rejects any input frame that
// arrives before `setup` (documented code 1002), so the setup frame is written
// synchronously here rather than lazily on the first WriteAudio.
func (a *STTAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindSTT {
		return nil, fmt.Errorf("gradium supports stt sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "gradium" {
		return nil, fmt.Errorf("gradium adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("gradium requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("gradium requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("gradium media: %w", err)
	}
	model, err := concreteModel(request.Plan.Route.Model)
	if err != nil {
		return nil, err
	}
	inputFormat, err := pcmFormat(*request.Media)
	if err != nil {
		return nil, err
	}
	language, err := transcriptionLanguage(request.Options.Language)
	if err != nil {
		return nil, err
	}
	credential := request.Plan.Route.Credential
	if credential == nil || credential.Kind != protocol.CredentialBearer || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("gradium requires a bearer credential")
	}
	endpoint, err := socketEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint, sttPath)
	if err != nil {
		return nil, err
	}

	headers := make(http.Header)
	// Deliberately not branching on Plan.Execution.CredentialSource: Gradium
	// authenticates both sockets and both credential sources with the same
	// account API key in the same header. See doc.go, "One credential, one
	// code path".
	headers.Set("x-api-key", credential.Value)

	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{
		HTTPClient: httpClientOrDefault(a.httpClient),
		HTTPHeader: headers,
	})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		return nil, &runtimepkg.ProviderError{
			Code:           dialErrorCode(status),
			Message:        "Gradium streaming connection could not be established",
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
	}
	setup := sttSetup{
		Type:        "setup",
		ModelName:   model,
		InputFormat: inputFormat,
		JSONConfig: sttJSONConfig{
			Language:      language,
			DelayInFrames: a.delayInFrames,
		},
	}
	if err := stream.writeJSON(ctx, setup); err != nil {
		_ = stream.abort()
		return nil, err
	}
	go stream.readLoop()
	return stream, nil
}

// sttSetup is Gradium's mandatory first frame on /api/speech/asr.
type sttSetup struct {
	Type        string        `json:"type"`
	ModelName   string        `json:"model_name"`
	InputFormat string        `json:"input_format"`
	JSONConfig  sttJSONConfig `json:"json_config"`
}

// sttJSONConfig is the nested advanced-settings object. Gradium accepts either
// an object or a JSON string here; the object form is what its own SDK sends.
type sttJSONConfig struct {
	// Language is omitted when the caller named none, which is Gradium's
	// documented way of asking the model not to ground on a single language.
	Language      string `json:"language,omitempty"`
	DelayInFrames int    `json:"delay_in_frames"`
}

type sttAudioFrame struct {
	Type  string `json:"type"`
	Audio string `json:"audio"`
}

type sttFlushFrame struct {
	Type    string `json:"type"`
	FlushID int64  `json:"flush_id"`
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
	flushID      atomic.Int64

	// Segment state below is touched only by readLoop, which is the single
	// reader goroutine, so it needs no lock.
	requestID    string
	segment      []string
	segmentStart float64
	segmentOpen  bool
}

func (s *sttStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

// WriteAudio sends one base64 audio frame as a JSON text message. Gradium's
// ASR socket has no binary input frame at all: audio is carried inside the
// same JSON envelope as control, which is why this is not a MessageBinary
// write like the Deepgram adapter's.
func (s *sttStream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("gradium audio is empty")
	}
	return s.writeJSON(ctx, sttAudioFrame{
		Type:  "audio",
		Audio: base64.StdEncoding.EncodeToString(audio),
	})
}

// CommitAudio maps onto Gradium's `flush`, which forces the server to process
// buffered audio and answer with a matching `flushed`. The id increments so a
// late `flushed` cannot be mistaken for the acknowledgement of a later commit.
func (s *sttStream) CommitAudio(ctx context.Context) error {
	return s.writeJSON(ctx, sttFlushFrame{Type: "flush", FlushID: s.flushID.Add(1)})
}

func (s *sttStream) AppendText(context.Context, string) error {
	return runtimepkg.ErrUnsupportedOperation
}

func (s *sttStream) CommitText(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// Cancel tears the STT stream down because Gradium's documented ASR protocol
// has no cancel command. It aborts rather than waiting for trailing results.
func (s *sttStream) Cancel(ctx context.Context) error {
	if err := s.Close(ctx); err != nil {
		return err
	}
	return s.abort()
}

// Abort immediately tears down the socket after a terminal runtime failure.
func (s *sttStream) Abort(context.Context) error { return s.abort() }

// Close sends `end_of_stream` and leaves the socket open to read. Gradium
// answers it with any outstanding transcripts, then its own `end_of_stream`,
// then closes — so closing the socket here would discard the tail of the
// final utterance.
func (s *sttStream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		s.writeMu.Lock()
		if err := writeControl(ctx, s.conn, map[string]string{"type": "end_of_stream"}); err != nil {
			s.closeErr = err
		}
		s.closed.Store(true)
		s.writeMu.Unlock()
		if s.closeErr != nil {
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

func (s *sttStream) writeJSON(ctx context.Context, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if err := s.conn.Write(ctx, websocket.MessageText, payload); err != nil {
		return &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "Gradium streaming write failed",
			Retryable: true,
			Cause:     err,
		}
	}
	return nil
}

func (s *sttStream) readLoop() {
	defer func() {
		s.cancel()
		close(s.events)
	}()
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && !isNormalClose(err) && s.ctx.Err() == nil {
				s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{
					Code:      "provider_unavailable",
					Message:   "Gradium streaming read failed",
					Retryable: true,
					Cause:     err,
				}})
			}
			return
		}
		if messageType != websocket.MessageText {
			continue
		}
		done, err := s.handleMessage(payload)
		if err != nil {
			s.emit(runtimepkg.ProviderEvent{Err: err})
			return
		}
		if done {
			return
		}
	}
}

func (s *sttStream) handleMessage(payload []byte) (bool, error) {
	var message sttInbound
	if err := json.Unmarshal(payload, &message); err != nil {
		return false, &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "Gradium sent malformed streaming JSON",
			Retryable: true,
			Cause:     err,
		}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))
	switch message.Type {
	case "ready":
		// The only correlation handle Gradium gives a managed route: there is
		// no reservation passthrough on this socket, so this must reach the
		// runtime before any media event.
		s.requestID = message.RequestID
		return false, s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventUsageObserved,
			Data:       usageData(message.RequestID),
			Extensions: extension(raw),
		})
	case "text":
		return false, s.handleText(message, raw)
	case "end_text":
		return false, s.handleEndText(message, raw)
	case "step":
		// Semantic-VAD horizons at 12.5 frames per second. Dropped on purpose;
		// see doc.go, "Known gaps".
		return false, nil
	case "flushed":
		return false, s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventWarning,
			Data:       s.flushedData(message.FlushID),
			Extensions: extension(raw),
		})
	case "end_of_stream":
		return true, nil
	case "error":
		return false, providerFrameError(message.Code, message.Message, raw)
	default:
		return false, s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventWarning,
			Data:       s.warningData(message.Type),
			Extensions: extension(raw),
		})
	}
}

// handleText accumulates one incremental piece of the utterance in progress.
// Gradium's `text` frames are additive fragments, not a growing snapshot, so a
// consumer that treated each one as the whole partial would see the transcript
// flicker between single words.
func (s *sttStream) handleText(message sttInbound, raw json.RawMessage) error {
	text := strings.TrimSpace(message.Text)
	if text == "" {
		return nil
	}
	if !s.segmentOpen {
		s.segmentOpen = true
		s.segmentStart = message.StartS
	}
	s.segment = append(s.segment, text)
	return s.emit(runtimepkg.ProviderEvent{
		Type: protocol.EventTranscriptDelta,
		Data: marshalData(map[string]any{
			"text":                text,
			"is_final":            false,
			"audio_start_ms":      milliseconds(message.StartS),
			"provider_request_id": s.requestID,
		}),
		Extensions: extension(raw),
	})
}

// handleEndText closes the utterance. Gradium has no `speech_final` flag and
// no discrete speech-ended frame, so `end_text` is the only endpoint signal on
// this socket and both terminal events are derived from it.
func (s *sttStream) handleEndText(message sttInbound, raw json.RawMessage) error {
	if !s.segmentOpen || len(s.segment) == 0 {
		s.segmentOpen = false
		s.segment = nil
		return nil
	}
	transcript := strings.TrimSpace(strings.Join(s.segment, " "))
	start := s.segmentStart
	s.segment = nil
	s.segmentOpen = false
	if transcript == "" {
		return nil
	}
	if err := s.emit(runtimepkg.ProviderEvent{
		Type: protocol.EventTranscriptFinal,
		Data: marshalData(map[string]any{
			"text":                transcript,
			"is_final":            true,
			"audio_start_ms":      milliseconds(start),
			"audio_end_ms":        milliseconds(message.StopS),
			"provider_request_id": s.requestID,
		}),
		Extensions: extension(raw),
	}); err != nil {
		return err
	}
	return s.emit(runtimepkg.ProviderEvent{
		Type: protocol.EventSpeechEnded,
		Data: marshalData(map[string]any{
			"audio_end_ms": milliseconds(message.StopS),
			"reason":       "end_text",
		}),
		Extensions: extension(raw),
	})
}

func (s *sttStream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *sttStream) flushedData(flushID int64) json.RawMessage {
	return marshalData(map[string]any{
		"provider_type":       "flushed",
		"flush_id":            flushID,
		"provider_request_id": s.requestID,
	})
}

func (s *sttStream) warningData(messageType string) json.RawMessage {
	return marshalData(map[string]any{
		"message":             "ignored Gradium message type",
		"provider_type":       messageType,
		"provider_request_id": s.requestID,
	})
}

// sttInbound covers every documented server frame on /api/speech/asr. Fields
// that only some frame types carry are left zero on the others.
type sttInbound struct {
	Type      string  `json:"type"`
	RequestID string  `json:"request_id"`
	Text      string  `json:"text"`
	StartS    float64 `json:"start_s"`
	StopS     float64 `json:"stop_s"`
	StreamID  int     `json:"stream_id"`
	FlushID   int64   `json:"flush_id"`
	Message   string  `json:"message"`
	Code      int     `json:"code"`
}

// ---------------------------------------------------------------------------
// Helpers shared with tts.go. Both sockets speak one framing and one error
// vocabulary, so these are provider-wide rather than modality-specific.
// ---------------------------------------------------------------------------

func httpClientOrDefault(client *http.Client) *http.Client {
	if client != nil {
		return client
	}
	return http.DefaultClient
}

// socketEndpoint validates the planned endpoint against the host allowlist and
// then pins the path, so a plan cannot point an STT session at the TTS socket
// (or at an undocumented path on an allowed host).
func socketEndpoint(policy upstream.WebSocketPolicy, rawEndpoint, wantPath string) (string, error) {
	endpoint, err := policy.Parse(rawEndpoint)
	if err != nil {
		return "", fmt.Errorf("gradium endpoint: %w", err)
	}
	if endpoint.Path != wantPath {
		return "", fmt.Errorf("gradium endpoint path must be %s, got %q", wantPath, endpoint.Path)
	}
	return endpoint.String(), nil
}

func concreteModel(model string) (string, error) {
	trimmed := strings.TrimSpace(model)
	if trimmed == "" || trimmed == "auto" {
		return "", errors.New("gradium requires a concrete model in the session plan")
	}
	return trimmed, nil
}

// pcmFormat maps the portable media format onto Gradium's format vocabulary,
// which is shared byte-for-byte between STT `input_format` and TTS
// `output_format`. The bare value "pcm" is never emitted: it means 48 kHz on
// the TTS side and 24 kHz on the STT side, so an explicit rate is the only
// unambiguous spelling.
func pcmFormat(media protocol.MediaFormat) (string, error) {
	if media.Encoding != "pcm_s16le" {
		return "", fmt.Errorf("gradium streaming requires pcm_s16le, got %q", media.Encoding)
	}
	if media.Channels != 1 {
		return "", fmt.Errorf("gradium streaming is mono only, got %d channels", media.Channels)
	}
	switch media.SampleRateHz {
	case 8_000, 16_000, 22_050, 24_000, 44_100, 48_000:
		return "pcm_" + strconv.Itoa(media.SampleRateHz), nil
	default:
		return "", fmt.Errorf("gradium does not support %d Hz pcm", media.SampleRateHz)
	}
}

// transcriptionLanguage narrows a BCP-47 tag to the bare subtag Gradium
// accepts. An empty language is legal and means "do not ground on one".
func transcriptionLanguage(language string) (string, error) {
	trimmed := strings.TrimSpace(language)
	if trimmed == "" {
		return "", nil
	}
	base := strings.ToLower(trimmed)
	if index := strings.IndexAny(base, "-_"); index > 0 {
		base = base[:index]
	}
	if _, ok := sttLanguages[base]; !ok {
		return "", fmt.Errorf("gradium does not support language %q", language)
	}
	return base, nil
}

func writeControl(ctx context.Context, conn *websocket.Conn, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, payload)
}

func isNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}

// dialErrorCode classifies a failed WebSocket handshake. The handshake is the
// only surface on which Gradium can answer 429: its in-band error frame
// publishes no rate-limit code.
func dialErrorCode(status int) string {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return "authentication_failed"
	case http.StatusPaymentRequired:
		return "provider_quota_exceeded"
	case http.StatusTooManyRequests:
		return "provider_rate_limited"
	case http.StatusBadRequest, http.StatusNotFound, http.StatusUnprocessableEntity:
		return "invalid_request"
	default:
		return "provider_unavailable"
	}
}

// providerFrameError converts Gradium's terminal `error` frame into the
// gateway's stable classification.
//
// Gradium publishes exactly three numeric codes for its sockets, and only one
// of them is unambiguous. 1002 is always a protocol violation and 1011 is
// always a server fault, but 1008 is a single "policy violation" bucket that
// its own docs say covers invalid auth, a missing subscription, a missing or
// malformed setup message, and an invalid audio format at once. Those need
// three different gateway codes, so 1008 is split on the message text — a
// documented-but-unversioned string, hence the conservative default of
// invalid_request for anything not recognised.
func providerFrameError(code int, message string, raw json.RawMessage) *runtimepkg.ProviderError {
	classified, retryable := frameErrorCode(code, message)
	return &runtimepkg.ProviderError{
		Code:           classified,
		Message:        frameErrorMessage(message),
		Retryable:      retryable,
		ProviderStatus: code,
		Extensions:     extension(raw),
	}
}

func frameErrorCode(code int, message string) (string, bool) {
	switch code {
	case 1002:
		return "invalid_request", false
	case 1008:
		return policyErrorCode(message), false
	case 1011:
		return "provider_unavailable", true
	default:
		return "provider_unavailable", true
	}
}

func policyErrorCode(message string) string {
	lowered := strings.ToLower(message)
	switch {
	// Gradium states authentication failures "always produce code 1008", and
	// its documented example body is "API key is revoked or expired".
	case strings.Contains(lowered, "api key"):
		return "authentication_failed"
	// "missing subscription" is named in the 1008 cause list; "credit" is the
	// unit Gradium meters in, so an exhausted balance lands here too.
	case strings.Contains(lowered, "subscription"), strings.Contains(lowered, "credit"):
		return "provider_quota_exceeded"
	default:
		return "invalid_request"
	}
}

func frameErrorMessage(message string) string {
	if strings.TrimSpace(message) == "" {
		return "Gradium reported a streaming error"
	}
	return "Gradium reported a streaming error: " + message
}

func extension(raw json.RawMessage) map[string]json.RawMessage {
	return map[string]json.RawMessage{extensionID: raw}
}

func usageData(requestID string) json.RawMessage {
	return marshalData(map[string]any{"provider_request_id": requestID})
}

func marshalData(value any) json.RawMessage {
	payload, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{"code":"internal"}`)
	}
	return payload
}

func milliseconds(seconds float64) int64 {
	return int64(math.Round(seconds * 1_000))
}
