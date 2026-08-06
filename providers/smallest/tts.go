package smallest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

const (
	// AdapterID is the identifier returned by a Smallest TTS session plan.
	AdapterID = "smallest.tts.v1"
	// extensionID namespaces the raw Smallest payload retained on events whose
	// vendor detail the canonical protocol does not model.
	extensionID = "smallest.ai/waves_v1"

	// officialAPIHost is the India-region Waves host. The US region lives at
	// api.us.smallest.ai and is reachable only through
	// Config.AllowedEndpointHosts; see the package comment for why.
	officialAPIHost = "api.smallest.ai"

	// ttsEndpointPath is the streaming Lightning socket. The same path over
	// HTTPS is the SSE twin, and POST /waves/v1/tts is the buffered one-shot
	// resource. Only the socket belongs here.
	ttsEndpointPath = "/waves/v1/tts/live"

	// DefaultTTSModel is the pool Smallest applies when a request omits
	// `model`. The adapter itself never defaults; a plan must name a concrete
	// model. This constant exists so the control plane and the tests agree on
	// what "unpinned Smallest TTS" means.
	DefaultTTSModel = "lightning_v3.1"

	// ttsMinSampleRate and ttsMaxSampleRate are Lightning's documented
	// `sample_rate` bounds (8000-44100). Note the ceiling: the canonical
	// MediaFormat allows up to 192 kHz and the Pulse STT socket accepts 48 kHz,
	// but Lightning does not, so 48 kHz output has to be rejected locally
	// instead of becoming a provider round trip the customer pays for.
	ttsMinSampleRate = 8_000
	ttsMaxSampleRate = 44_100

	// ttsOutputFormat is the only format worth streaming into a realtime
	// pipeline. Lightning also offers mp3/wav/ulaw/alaw; `pcm` is raw
	// little-endian 16-bit mono and needs no decode on our side.
	ttsOutputFormat = "pcm"

	// defaultMaxTextBytes bounds locally buffered utterance text. Smallest
	// publishes no per-request character ceiling for the streaming socket, so
	// this is our memory bound, not a vendor rule: AppendText can be called
	// repeatedly before CommitText and something has to stop an unbounded
	// buffer.
	defaultMaxTextBytes = 1 << 16
)

// Lightning names every inbound frame with a `status` field. These are the
// documented WebSocket values. They are NOT the SSE values: the SSE twin sends
// HTTP-style codes ("206"/"200") plus a `done` boolean, and Smallest's own docs
// warn the two shapes are not interchangeable.
const (
	ttsStatusChunk         = "chunk"
	ttsStatusComplete      = "complete"
	ttsStatusWordTimestamp = "word_timestamp"
	ttsStatusError         = "error"
)

// Config controls local transport limits and endpoint policy. Provider
// identity, model, voice, and credential all come from the signed session plan.
type Config struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	MaxTextBytes          int
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// Adapter implements Smallest's /waves/v1/tts/live streaming API.
type Adapter struct {
	id              string
	httpClient      *http.Client
	eventBuffer     int
	maxMessageBytes int64
	maxTextBytes    int
	endpointPolicy  upstream.WebSocketPolicy
}

// New creates a bounded Smallest TTS adapter.
func New(config Config) (*Adapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = AdapterID
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = 32
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = 1 << 20
	}
	if config.MaxTextBytes == 0 {
		config.MaxTextBytes = defaultMaxTextBytes
	}
	if config.EventBuffer < 1 {
		return nil, errors.New("smallest event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("smallest maximum message bytes must be positive")
	}
	if config.MaxTextBytes < 1 {
		return nil, errors.New("smallest maximum text bytes must be positive")
	}
	endpointPolicy, err := upstream.NewWebSocketPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &Adapter{
		id:              config.AdapterID,
		httpClient:      config.HTTPClient,
		eventBuffer:     config.EventBuffer,
		maxMessageBytes: config.MaxMessageBytes,
		maxTextBytes:    config.MaxTextBytes,
		endpointPolicy:  endpointPolicy,
	}, nil
}

func (a *Adapter) ID() string { return a.id }

// Open dials the Lightning streaming socket. Smallest authenticates the Waves
// APIs with one long-lived account key in an Authorization header, so this is a
// BYOK-only adapter; see requireBYOK.
func (a *Adapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindTTS {
		return nil, fmt.Errorf("smallest tts supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "smallest" {
		return nil, fmt.Errorf("smallest tts adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("smallest tts requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("smallest tts requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("smallest tts media: %w", err)
	}
	if err := validateTTSOptions(request.Plan.Route.Model, request.Options, *request.Media); err != nil {
		return nil, err
	}
	credential, err := requireBYOK(request, "smallest tts")
	if err != nil {
		return nil, err
	}
	endpoint, err := ttsEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint)
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
			Message:        "Smallest streaming connection could not be established",
			Retryable:      status == 0 || status == http.StatusTooManyRequests || status >= 500,
			ProviderStatus: status,
			Cause:          err,
		}
	}
	conn.SetReadLimit(a.maxMessageBytes)
	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &ttsStream{
		conn:         conn,
		ctx:          streamCtx,
		cancel:       cancel,
		events:       make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		model:        request.Plan.Route.Model,
		voice:        strings.TrimSpace(request.Options.Voice),
		language:     normalizeLanguage(request.Options.Language),
		sampleRate:   request.Media.SampleRateHz,
		maxTextBytes: a.maxTextBytes,
	}
	go stream.readLoop()
	return stream, nil
}

func validateTTSOptions(model string, options protocol.RequestOptions, media protocol.MediaFormat) error {
	if strings.TrimSpace(model) == "" || model == "auto" {
		return errors.New("smallest tts requires a concrete model in the session plan")
	}
	if strings.TrimSpace(options.Voice) == "" {
		// `voice_id` is documented as required and Lightning has no documented
		// per-model default, so an empty voice is a local error rather than a
		// billed 4xx.
		return errors.New("smallest tts requires a voice id in request options")
	}
	if media.Encoding != "pcm_s16le" {
		// Lightning's output_format set is pcm/mp3/wav/ulaw/alaw. Opus, the
		// other canonical MediaFormat encoding, is not in it.
		return fmt.Errorf("smallest tts requires pcm_s16le, got %q", media.Encoding)
	}
	if media.Channels != 1 {
		return fmt.Errorf("smallest tts produces mono audio, got %d channels", media.Channels)
	}
	if media.SampleRateHz < ttsMinSampleRate || media.SampleRateHz > ttsMaxSampleRate {
		return fmt.Errorf("smallest tts sample rate must be between %d and %d Hz, got %d", ttsMinSampleRate, ttsMaxSampleRate, media.SampleRateHz)
	}
	return nil
}

func ttsEndpoint(policy upstream.WebSocketPolicy, rawEndpoint string) (string, error) {
	endpoint, err := policy.Parse(rawEndpoint)
	if err != nil {
		return "", fmt.Errorf("smallest tts endpoint: %w", err)
	}
	if endpoint.Path != ttsEndpointPath {
		return "", fmt.Errorf("smallest tts endpoint path must be %s, got %q", ttsEndpointPath, endpoint.Path)
	}
	return endpoint.String(), nil
}

type ttsStream struct {
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent

	model        string
	voice        string
	language     string
	sampleRate   int
	maxTextBytes int

	writeMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	closing      atomic.Bool
	closeErr     error
	// completed records that the provider sent its terminal `complete` frame.
	// Smallest's docs say the server closes the socket after that frame, so a
	// graceful close handshake afterwards would race a socket that is already
	// gone.
	completed atomic.Bool

	stateMu           sync.Mutex
	pending           strings.Builder
	inFlight          bool
	canceled          bool
	audioStarted      bool
	utteranceDone     chan struct{}
	providerRequestID string
}

func (s *ttsStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

func (s *ttsStream) WriteAudio(context.Context, []byte) error {
	return runtimepkg.ErrUnsupportedOperation
}

func (s *ttsStream) CommitAudio(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// AppendText buffers a fragment. Unlike Cartesia or MiniMax, the Lightning
// socket has no continuation protocol: one utterance is one JSON request
// carrying the whole `text`, so nothing can go on the wire until CommitText.
func (s *ttsStream) AppendText(_ context.Context, text string) error {
	if strings.TrimSpace(text) == "" {
		return errors.New("smallest tts text is empty")
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() || s.closing.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if s.inFlight {
		return errors.New("smallest tts previous utterance has not completed")
	}
	if s.pending.Len()+len(text) > s.maxTextBytes {
		return &runtimepkg.ProviderError{
			Code:           "input_too_large",
			Message:        "Smallest TTS buffered text exceeds the configured limit",
			Retryable:      false,
			ProviderStatus: http.StatusRequestEntityTooLarge,
		}
	}
	s.pending.WriteString(text)
	return nil
}

// CommitText sends the buffered utterance as one Lightning request.
func (s *ttsStream) CommitText(ctx context.Context) error {
	text, err := s.beginUtterance()
	if err != nil {
		return err
	}
	if err := s.writeJSON(ctx, ttsRequest{
		Text:         text,
		VoiceID:      s.voice,
		Model:        s.model,
		SampleRate:   s.sampleRate,
		OutputFormat: ttsOutputFormat,
		Language:     s.language,
	}); err != nil {
		s.finishUtterance()
		return err
	}
	return nil
}

// Cancel implements barge-in. Lightning documents no interrupt, flush, or
// clear frame on this socket, so the only honest cancellation is one-sided:
// drop every remaining frame of the current utterance locally so already
// synthesized audio never reaches the caller, and emit no audio.done for it.
// The provider keeps generating and keeps billing; pretending otherwise by
// inventing a control frame would be worse.
func (s *ttsStream) Cancel(context.Context) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if !s.inFlight {
		return runtimepkg.ErrSessionClosed
	}
	s.canceled = true
	return nil
}

// Close waits for the in-flight utterance's terminal frame so a caller that
// closes right after CommitText still receives the tail of the audio.
func (s *ttsStream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		s.closing.Store(true)
		if done := s.activeDone(); done != nil {
			select {
			case <-done:
			case <-ctx.Done():
				s.closeErr = ctx.Err()
			}
		}
		if s.closeErr != nil {
			_ = s.abort()
			return
		}
		if s.completed.Load() {
			// The provider already closed its side after `complete`; a close
			// handshake here would just time out against a dead socket.
			_ = s.abort()
			return
		}
		s.closed.Store(true)
		if err := s.conn.Close(websocket.StatusNormalClosure, ""); err != nil {
			s.closeErr = err
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
		// After `complete` the provider has already closed its side, so a local
		// close failure there is the documented outcome, not an error worth
		// reporting to the caller.
		if err := s.conn.CloseNow(); err != nil && s.closeErr == nil && !s.completed.Load() {
			s.closeErr = err
		}
		s.finishUtterance()
	})
	return s.closeErr
}

func (s *ttsStream) beginUtterance() (string, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() || s.closing.Load() {
		return "", runtimepkg.ErrSessionClosed
	}
	if s.inFlight {
		return "", errors.New("smallest tts previous utterance has not completed")
	}
	text := s.pending.String()
	if strings.TrimSpace(text) == "" {
		return "", errors.New("smallest tts has no buffered text to synthesize")
	}
	s.pending.Reset()
	s.inFlight = true
	s.canceled = false
	s.audioStarted = false
	s.utteranceDone = make(chan struct{})
	return text, nil
}

func (s *ttsStream) finishUtterance() {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if !s.inFlight {
		return
	}
	s.inFlight = false
	close(s.utteranceDone)
	s.utteranceDone = nil
}

func (s *ttsStream) activeDone() <-chan struct{} {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.utteranceDone
}

// markAudioStarted reports the first audio frame of an utterance that has not
// been cancelled. A cancelled utterance yields false forever, which is what
// suppresses its remaining frames.
func (s *ttsStream) markAudioStarted() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if !s.inFlight || s.canceled || s.audioStarted {
		return false
	}
	s.audioStarted = true
	return true
}

func (s *ttsStream) dropFrame() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.canceled
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
	if err := s.conn.Write(ctx, websocket.MessageText, payload); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Smallest streaming write failed", Retryable: true, Cause: err}
	}
	return nil
}

func (s *ttsStream) readLoop() {
	defer func() {
		s.cancel()
		s.finishUtterance()
		close(s.events)
	}()
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && !s.closing.Load() && !s.completed.Load() && s.ctx.Err() == nil && !isNormalClose(err) {
				s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Smallest streaming read failed", Retryable: true, Cause: err}})
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
	var message ttsInbound
	if err := json.Unmarshal(payload, &message); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Smallest sent malformed streaming JSON", Retryable: true, Cause: err}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))
	// `request_id` is the per-synthesis task id; `session_id` names the socket.
	// The task id is the one a support ticket can be traced with, so prefer it.
	if identifier := firstNonEmpty(message.RequestID, message.SessionID); identifier != "" && s.setProviderRequestID(identifier) {
		if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: usageData(identifier)}); err != nil {
			return err
		}
	}
	// An error can arrive with `status:"error"` or with only an `error` string;
	// Smallest documents neither shape for this socket, so both are accepted.
	if message.Status == ttsStatusError || strings.TrimSpace(message.Error) != "" {
		return &runtimepkg.ProviderError{
			Code:           providerErrorCode(message.StatusCode),
			Message:        ttsErrorMessage(firstNonEmpty(message.Error, message.Message)),
			Retryable:      message.StatusCode == 0 || message.StatusCode == http.StatusTooManyRequests || message.StatusCode >= 500,
			ProviderStatus: message.StatusCode,
			Extensions:     extension(raw),
		}
	}
	switch message.Status {
	case ttsStatusChunk:
		// Audio lives at data.audio on the WebSocket. The SSE twin puts it at
		// the top level; a parser that looked there would silently drop every
		// frame here.
		if message.Data.Audio == "" {
			return nil
		}
		audio, err := base64.StdEncoding.DecodeString(message.Data.Audio)
		if err != nil {
			return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Smallest sent invalid audio data", Retryable: true, Cause: err}
		}
		if s.markAudioStarted() {
			if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: s.utteranceData(), Extensions: extension(raw)}); err != nil {
				return err
			}
		}
		if s.dropFrame() {
			return nil
		}
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Data: s.utteranceData(), Extensions: extension(raw), Audio: audio})
	case ttsStatusComplete:
		// Terminal for the utterance. There is no `done` field on Lightning
		// WebSocket frames — that belongs to the SSE shape — so `complete` is
		// the only completion signal to key on.
		s.completed.Store(true)
		canceled := s.dropFrame()
		s.finishUtterance()
		if canceled {
			return nil
		}
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: s.utteranceData(), Extensions: extension(raw)})
	case ttsStatusWordTimestamp:
		// Only sent when the request opts into word_timestamps, which this
		// adapter does not. Handled anyway so an opt-in added later surfaces as
		// alignment instead of a warning. The payload shape is carried verbatim
		// in the extension because it is not documented on the streaming page.
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAlignment, Data: s.utteranceData(), Extensions: extension(raw)})
	default:
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: s.warningData(message.Status), Extensions: extension(raw)})
	}
}

func (s *ttsStream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *ttsStream) setProviderRequestID(value string) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.providerRequestID == value {
		return false
	}
	s.providerRequestID = value
	return true
}

func (s *ttsStream) currentProviderRequestID() string {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.providerRequestID
}

func (s *ttsStream) utteranceData() json.RawMessage {
	return marshalData(map[string]any{"provider_request_id": s.currentProviderRequestID()})
}

func (s *ttsStream) warningData(status string) json.RawMessage {
	return marshalData(map[string]any{"provider_status": status, "provider_request_id": s.currentProviderRequestID()})
}

func ttsErrorMessage(message string) string {
	if strings.TrimSpace(message) == "" {
		return "Smallest reported a streaming error"
	}
	return "Smallest reported a streaming error: " + message
}

type ttsRequest struct {
	Text         string `json:"text"`
	VoiceID      string `json:"voice_id"`
	Model        string `json:"model"`
	SampleRate   int    `json:"sample_rate"`
	OutputFormat string `json:"output_format"`
	// Lightning defaults `language` to en. Omitting it entirely when the caller
	// did not ask for one keeps the vendor default rather than pinning it here.
	Language string `json:"language,omitempty"`
}

type ttsInbound struct {
	Status    string `json:"status"`
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id"`
	Data      struct {
		Audio string `json:"audio"`
	} `json:"data"`
	Message    string `json:"message"`
	Error      string `json:"error"`
	StatusCode int    `json:"status_code"`
}

// -- helpers shared with the STT adapter ------------------------------------

// requireBYOK enforces the credential policy explained in the package comment:
// Smallest publishes no delegated, session-scoped credential for the Waves
// APIs, so a "managed" plan could only carry the customer's root account key
// and must be refused rather than forwarded.
func requireBYOK(request runtimepkg.AdapterRequest, adapter string) (string, error) {
	if request.Plan.Execution.CredentialSource != protocol.CredentialsBYOK {
		return "", fmt.Errorf("%s is BYOK-only: Smallest publishes no delegated credential for the Waves APIs, got credential source %q", adapter, request.Plan.Execution.CredentialSource)
	}
	credential := request.Plan.Route.Credential
	if credential == nil || credential.Kind != protocol.CredentialBearer || strings.TrimSpace(credential.Value) == "" {
		return "", fmt.Errorf("%s requires a bearer credential", adapter)
	}
	return strings.TrimSpace(credential.Value), nil
}

// normalizeLanguage strips a region subtag, because Smallest takes bare
// ISO-639 codes, while leaving its regional aggregators intact. The aggregators
// matter: a naive strip on "multi-south-indic" yields "multi", which Smallest
// does not know.
func normalizeLanguage(language string) string {
	language = strings.ToLower(strings.TrimSpace(language))
	if language == "" {
		return ""
	}
	switch language {
	case "north_indic", "multi-asian", "multi-south-indic", "multi-eu":
		return language
	}
	base, _, _ := strings.Cut(language, "-")
	return base
}

func httpClient(client *http.Client) *http.Client {
	if client != nil {
		return client
	}
	return http.DefaultClient
}

func isNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}

func dialErrorCode(status int) string {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return "authentication_failed"
	case http.StatusTooManyRequests:
		return "provider_rate_limited"
	default:
		return "provider_unavailable"
	}
}

// providerErrorCode maps an in-band status code. Smallest documents 401
// (missing/invalid key), 403 (key lacks access), and 429 (rate limit) for its
// HTTP surface; the socket reuses the same numbers when it reports one.
func providerErrorCode(status int) string {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return "authentication_failed"
	case http.StatusTooManyRequests:
		return "provider_rate_limited"
	default:
		return "provider_unavailable"
	}
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

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
