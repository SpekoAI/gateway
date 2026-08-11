package rime

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

const (
	// AdapterID is the identifier returned by a Rime TTS session plan.
	AdapterID   = "rime.tts.v1"
	extensionID = "rime.ai/ws3"

	officialAPIHost = "users-ws.rime.ai"
	websocketPath   = "/ws3"

	// segmentNever is Rime's documented recommendation for production voice
	// agents: never synthesize automatically, only on an explicit flush.
	segmentNever         = "never"
	segmentBySentence    = "bySentence"
	segmentImmediate     = "immediate"
	operationFlush       = "flush"
	operationClear       = "clear"
	maxRequestCharacters = 1_000
)

// documentedLanguages is the exact table Rime publishes for the `lang`
// parameter, in both the ISO 639-1 and ISO 639-2/3 spellings it accepts. It is
// enforced so an unsupported language fails locally with a clear message
// instead of becoming a mid-utterance provider error. Extend it when Rime adds
// a language; the docs table is the source of truth.
var documentedLanguages = map[string]struct{}{
	"en": {}, "eng": {},
	"es": {}, "spa": {},
	"fr": {}, "fra": {},
	"pt": {}, "por": {},
	"de": {}, "ger": {},
	"ja": {}, "jpn": {},
	"ar": {}, "ara": {},
	"hi": {}, "hin": {},
}

// cloudSamplingRates is the set Rime accepts on its hosted API. On-prem
// deployments accept any rate, so this is enforced only for the official host.
var cloudSamplingRates = map[int]struct{}{
	8_000: {}, 16_000: {}, 22_050: {}, 24_000: {}, 44_100: {}, 48_000: {}, 96_000: {},
}

// Config controls local transport limits and the socket's segmentation mode.
// Provider identity, model, voice, language, and the API key all come from a
// session plan and its provider-neutral request options.
type Config struct {
	AdapterID string
	// Segment selects Rime's `segment` query parameter. Empty means
	// segmentNever, which is what makes CommitText the single utterance
	// boundary; see the package doc.
	Segment               string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// Adapter implements Rime's /ws3 JSON streaming TTS API.
type Adapter struct {
	id              string
	segment         string
	httpClient      *http.Client
	eventBuffer     int
	maxMessageBytes int64
	endpointPolicy  upstream.WebSocketPolicy
}

// New creates a bounded Rime TTS adapter.
func New(config Config) (*Adapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = AdapterID
	}
	if config.Segment == "" {
		config.Segment = segmentNever
	}
	if config.Segment != segmentNever && config.Segment != segmentBySentence && config.Segment != segmentImmediate {
		return nil, fmt.Errorf("rime segment must be one of %q, %q, or %q", segmentNever, segmentBySentence, segmentImmediate)
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = 32
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = 1 << 20
	}
	if config.EventBuffer < 1 {
		return nil, errors.New("rime event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("rime maximum message bytes must be positive")
	}
	endpointPolicy, err := upstream.NewWebSocketPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &Adapter{
		id:              config.AdapterID,
		segment:         config.Segment,
		httpClient:      config.HTTPClient,
		eventBuffer:     config.EventBuffer,
		maxMessageBytes: config.MaxMessageBytes,
		endpointPolicy:  endpointPolicy,
	}, nil
}

func (a *Adapter) ID() string { return a.id }

// Open opens a provider-direct Rime /ws3 WebSocket. Every synthesis argument
// travels as a handshake query parameter; only the bearer token is a header.
func (a *Adapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindTTS {
		return nil, fmt.Errorf("rime supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "rime" {
		return nil, fmt.Errorf("rime adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("rime requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("rime requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("rime media: %w", err)
	}
	endpoint, err := websocketEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}
	if err := validateGenerationOptions(request.Plan.Route.Model, request.Options, *request.Media, endpoint.Hostname()); err != nil {
		return nil, err
	}
	token, err := accessToken(request.Plan)
	if err != nil {
		return nil, err
	}

	handshake, err := handshakeURL(endpoint, a.segment, request.Plan.Route.Model, request.Options, *request.Media)
	if err != nil {
		return nil, err
	}
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer "+token)
	conn, response, err := websocket.Dial(ctx, handshake, &websocket.DialOptions{HTTPClient: httpClient(a.httpClient), HTTPHeader: headers})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		return nil, &runtimepkg.ProviderError{
			Code:           dialErrorCode(status),
			Message:        "Rime streaming connection could not be established",
			Retryable:      status == 0 || status == http.StatusTooManyRequests || status >= 500,
			ProviderStatus: status,
			Cause:          err,
		}
	}
	conn.SetReadLimit(a.maxMessageBytes)
	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &stream{
		conn:   conn,
		ctx:    streamCtx,
		cancel: cancel,
		events: make(chan runtimepkg.ProviderEvent, a.eventBuffer),
	}
	go stream.readLoop()
	return stream, nil
}

func httpClient(client *http.Client) *http.Client {
	if client != nil {
		return client
	}
	return http.DefaultClient
}

func websocketEndpoint(policy upstream.WebSocketPolicy, rawEndpoint string) (*url.URL, error) {
	endpoint, err := policy.Parse(rawEndpoint)
	if err != nil {
		return nil, fmt.Errorf("rime endpoint: %w", err)
	}
	// The policy already rejects a preexisting query string, so the plan cannot
	// smuggle synthesis parameters past validateGenerationOptions.
	if endpoint.Path != websocketPath {
		return nil, fmt.Errorf("rime endpoint path must be %s, got %q", websocketPath, endpoint.Path)
	}
	return endpoint, nil
}

// accessToken resolves the single Rime credential.
//
// Rime is bearer-API-key only: it publishes no temporary-token or
// ephemeral-credential endpoint (re-verified against docs.rime.ai on
// 2026-08-07). So there is no managed/BYOK/relay split to make here, and
// inventing one would only add a branch that can never be exercised. Every
// source takes the same path deliberately: a relay plan carries the
// connector's permanent key through the identical Authorization header.
//
// The consequence belongs to the control plane, not to this file: because no
// mint endpoint exists, a managed Rime route has to hand the runtime a real,
// long-lived Rime key rather than a scoped short-lived one. Every other
// managed credential in this repository is bounded in time; this one cannot
// be.
func accessToken(plan protocol.SessionPlan) (string, error) {
	credential := plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return "", errors.New("rime requires a bearer credential")
	}
	switch plan.Execution.CredentialSource {
	case protocol.CredentialsBYOK, protocol.CredentialsManaged:
		return credential.Value, nil
	default:
		return "", fmt.Errorf("rime cannot use credential source %q", plan.Execution.CredentialSource)
	}
}

// acceptableCredentialKind reports whether a delegated credential's kind may
// authenticate the plan's route. Bearer is the norm everywhere; the relay arm
// additionally accepts relay_access, because protocol.SessionPlan validation
// requires relay plans to label their credential relay_access while a relay
// connector that synthesizes the plan and drives this adapter directly — no
// Engine, no SessionPlan.Validate — labels the same permanent Rime key
// bearer. Both spellings carry a permanent key destined for the identical
// Authorization header, so nothing else on the relay arm changes.
func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

func validateGenerationOptions(model string, options protocol.RequestOptions, media protocol.MediaFormat, host string) error {
	// Rime warns that omitting modelId silently routes the request to Mist v3,
	// where a Coda speaker fails with "Speaker not found". A plan that has not
	// resolved `auto` to a concrete model would reproduce exactly that.
	if strings.TrimSpace(model) == "" || model == "auto" {
		return errors.New("rime requires a concrete model in the session plan")
	}
	if strings.TrimSpace(options.Voice) == "" {
		return errors.New("rime requires a voice id in request options")
	}
	language := strings.TrimSpace(options.Language)
	if language == "" {
		return errors.New("rime requires a language in request options")
	}
	if _, ok := documentedLanguages[language]; !ok {
		return fmt.Errorf("rime does not document language %q", language)
	}
	if media.Encoding != "pcm_s16le" {
		// /ws3 offers mp3, mulaw, and pcm. Only pcm is raw enough to hand
		// straight to the runtime, and the protocol's other encoding (opus) has
		// no Rime equivalent on this socket.
		return fmt.Errorf("rime streaming output requires pcm_s16le, got %q", media.Encoding)
	}
	if media.Channels != 1 {
		// Rime documents no channel parameter anywhere on /ws3. Accepting a
		// multi-channel request would mean returning mono while claiming stereo.
		return fmt.Errorf("rime streaming output is mono, got %d channels", media.Channels)
	}
	if host == officialAPIHost {
		if _, ok := cloudSamplingRates[media.SampleRateHz]; !ok {
			return fmt.Errorf("rime cloud does not accept sampling rate %d", media.SampleRateHz)
		}
	}
	return nil
}

// handshakeURL puts every synthesis argument in the query string, which is the
// only place /ws3 reads them: there is no opening JSON handshake frame.
func handshakeURL(endpoint *url.URL, segment, model string, options protocol.RequestOptions, media protocol.MediaFormat) (string, error) {
	handshake := *endpoint
	query := make(url.Values, 6)
	query.Set("speaker", options.Voice)
	query.Set("modelId", model)
	query.Set("audioFormat", "pcm")
	query.Set("lang", strings.TrimSpace(options.Language))
	query.Set("samplingRate", strconv.Itoa(media.SampleRateHz))
	query.Set("segment", segment)
	handshake.RawQuery = query.Encode()
	return handshake.String(), nil
}

type stream struct {
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent

	writeMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	closing      atomic.Bool
	closeErr     error

	stateMu sync.Mutex
	// contextID is Rime's correlation token for the current utterance. Rime
	// keeps at most one at a time, so this adapter does too.
	contextID string
	// contextDone closes when the utterance reaches a terminal state.
	contextDone chan struct{}
	// contextFlushed records that a flush is in flight. Close waits for a
	// terminal `done` only in that case: without a flush, Rime has not been
	// asked for audio and no `done` is coming.
	contextFlushed bool
	audioStarted   bool
	// bufferedCharacters tracks Rime's documented 1,000-character-per-request
	// limit across the AppendText calls that accumulate into one flush.
	bufferedCharacters int
}

func (s *stream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

func (s *stream) WriteAudio(context.Context, []byte) error { return runtimepkg.ErrUnsupportedOperation }

func (s *stream) CommitAudio(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// AppendText adds a token to Rime's buffer. Under the default segment=never
// this never triggers synthesis; CommitText does.
func (s *stream) AppendText(ctx context.Context, text string) error {
	if strings.TrimSpace(text) == "" {
		return errors.New("rime transcript is empty")
	}
	contextID, err := s.reserveText(text)
	if err != nil {
		return err
	}
	if err := s.writeJSON(ctx, textMessage{Text: text, ContextID: contextID}); err != nil {
		s.finishContext(contextID)
		return err
	}
	return nil
}

// CommitText flushes the accumulated buffer. Rime documents exactly one `done`
// per flush, which is what makes one CommitText yield one audio.done.
func (s *stream) CommitText(ctx context.Context) error {
	contextID, err := s.flushContext()
	if err != nil {
		return err
	}
	if err := s.writeJSON(ctx, operationMessage{Operation: operationFlush}); err != nil {
		s.finishContext(contextID)
		return err
	}
	return nil
}

// Cancel sends Rime's documented `clear` operation, which discards the
// accumulated text buffer without synthesizing it — the barge-in primitive.
//
// It does not silence audio already in flight: a flush that Rime is still
// synthesizing keeps streaming and still ends with `done`, so the utterance
// stays open locally. When nothing has been flushed there is no `done` to wait
// for, so the utterance is closed here instead; otherwise Close would block on
// a terminal event that Rime is never going to send.
func (s *stream) Cancel(ctx context.Context) error {
	contextID, flushed, ok := s.cancelContext()
	if !ok {
		return runtimepkg.ErrSessionClosed
	}
	err := s.writeJSON(ctx, operationMessage{Operation: operationClear})
	if !flushed {
		s.finishContext(contextID)
	}
	return err
}

// Close waits for an in-flight flush's `done` before closing the socket so a
// caller that closes immediately after CommitText still receives its audio.
func (s *stream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		s.closing.Store(true)
		if done := s.activeDone(); done != nil {
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
func (s *stream) Abort(context.Context) error { return s.abort() }

func (s *stream) abort() error {
	s.abortOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		if err := s.conn.CloseNow(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
		s.finishContext("")
	})
	return s.closeErr
}

// reserveText opens or continues the current utterance and charges the text
// against Rime's per-request character budget in one critical section, so a
// concurrent caller cannot slip past the limit between check and write.
func (s *stream) reserveText(text string) (string, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() || s.closing.Load() {
		return "", runtimepkg.ErrSessionClosed
	}
	if s.contextID != "" && s.contextFlushed {
		return "", errors.New("rime previous utterance has not completed")
	}
	// Rime counts characters, not bytes: a multi-byte grapheme must not consume
	// the budget several times over.
	pending := s.bufferedCharacters + utf8.RuneCountInString(text)
	if pending > maxRequestCharacters {
		return "", &runtimepkg.ProviderError{
			Code:    "input_too_large",
			Message: fmt.Sprintf("Rime accepts %d characters per synthesis request", maxRequestCharacters),
		}
	}
	if s.contextID == "" {
		contextID, err := newContextID()
		if err != nil {
			return "", err
		}
		s.contextID = contextID
		s.contextDone = make(chan struct{})
		s.contextFlushed = false
		s.audioStarted = false
		s.bufferedCharacters = 0
		pending = utf8.RuneCountInString(text)
	}
	s.bufferedCharacters = pending
	return s.contextID, nil
}

func (s *stream) flushContext() (string, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() || s.closing.Load() || s.contextID == "" || s.contextFlushed {
		return "", runtimepkg.ErrSessionClosed
	}
	s.contextFlushed = true
	return s.contextID, nil
}

func (s *stream) cancelContext() (string, bool, bool) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.contextID == "" {
		return "", false, false
	}
	s.bufferedCharacters = 0
	return s.contextID, s.contextFlushed, true
}

func (s *stream) activeDone() <-chan struct{} {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if !s.contextFlushed {
		return nil
	}
	return s.contextDone
}

func (s *stream) finishContext(contextID string) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.contextID == "" || (contextID != "" && s.contextID != contextID) {
		return
	}
	close(s.contextDone)
	s.contextID = ""
	s.contextDone = nil
	s.contextFlushed = false
	s.audioStarted = false
	s.bufferedCharacters = 0
}

func (s *stream) markAudioStarted(contextID string) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.contextID != contextID || s.audioStarted {
		return false
	}
	s.audioStarted = true
	return true
}

func (s *stream) writeJSON(ctx context.Context, value any) error {
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
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Rime streaming write failed", Retryable: true, Cause: err}
	}
	return nil
}

func (s *stream) readLoop() {
	defer func() {
		s.cancel()
		s.finishContext("")
		close(s.events)
	}()
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && s.ctx.Err() == nil && !isNormalClose(err) {
				s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Rime streaming read failed", Retryable: true, Cause: err}})
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

func (s *stream) handleMessage(payload []byte) error {
	var message inbound
	if err := json.Unmarshal(payload, &message); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Rime sent malformed streaming JSON", Retryable: true, Cause: err}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))
	switch message.Type {
	case "chunk":
		audio, err := base64.StdEncoding.DecodeString(message.Data)
		if err != nil {
			return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Rime sent invalid audio data", Retryable: true, Cause: err}
		}
		if s.markAudioStarted(message.ContextID) {
			if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: contextData(message.ContextID), Extensions: extension(raw)}); err != nil {
				return err
			}
		}
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Data: contextData(message.ContextID), Extensions: extension(raw), Audio: audio})
	case "timestamps":
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAlignment, Data: alignmentData(message.ContextID, message.WordTimestamps), Extensions: extension(raw)})
	case "done":
		s.finishContext(message.ContextID)
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: contextData(message.ContextID), Extensions: extension(raw)})
	case "error":
		// Rime documents that it does NOT close the socket on an error and will
		// keep accepting well-formed messages. The attempt still ends here: an
		// error arrives in place of the utterance's audio and `done`, and this
		// interface has no way to fail one utterance without failing the
		// session, so returning the error is better than leaving a caller
		// waiting on a terminal event that will never come.
		//
		// The event carries only {type, message} — no status code — so there is
		// nothing to classify on. Rime describes the trigger as "malformed or
		// unexpected input", which is a request defect, not a transient one.
		return &runtimepkg.ProviderError{Code: "invalid_request", Message: errorMessage(message.Message), Extensions: extension(raw)}
	default:
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: warningData(message.ContextID, message.Type), Extensions: extension(raw)})
	}
}

func (s *stream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func newContextID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("generate Rime context id: %w", err)
	}
	return hex.EncodeToString(bytes), nil
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

func errorMessage(message string) string {
	if strings.TrimSpace(message) == "" {
		return "Rime reported a streaming error"
	}
	return "Rime reported a streaming error: " + message
}

func extension(raw json.RawMessage) map[string]json.RawMessage {
	return map[string]json.RawMessage{extensionID: raw}
}

func contextData(contextID string) json.RawMessage {
	return marshalData(map[string]any{"context_id": contextID})
}

func alignmentData(contextID string, timestamps json.RawMessage) json.RawMessage {
	return marshalData(map[string]any{"context_id": contextID, "word_timestamps": timestamps})
}

func warningData(contextID, messageType string) json.RawMessage {
	return marshalData(map[string]any{"context_id": contextID, "provider_type": messageType})
}

func marshalData(value any) json.RawMessage {
	payload, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{"code":"internal"}`)
	}
	return payload
}

// textMessage is Rime's text frame. The field is `contextId` — camelCase,
// unlike the snake_case `word_timestamps` Rime sends back. The socket mixes
// conventions; both spellings are transcribed from Rime's reference page.
type textMessage struct {
	Text      string `json:"text"`
	ContextID string `json:"contextId"`
}

type operationMessage struct {
	Operation string `json:"operation"`
}

type inbound struct {
	Type      string `json:"type"`
	Data      string `json:"data"`
	ContextID string `json:"contextId"`
	Message   string `json:"message"`
	// WordTimestamps is snake_case on the wire while ContextID beside it is
	// camelCase. Not a typo: that is what /ws3 sends.
	WordTimestamps json.RawMessage `json:"word_timestamps"`
}
