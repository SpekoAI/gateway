package gladia

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
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
	// AdapterID is the identifier returned by a Gladia STT session plan.
	AdapterID = "gladia.stt.v1"
	// DefaultModel is Gladia's documented default (and, as of this writing,
	// only) live streaming model. A session plan still has to name a concrete
	// model; this constant exists so the control plane and tests agree on which
	// one that is.
	DefaultModel = "solaria-1"

	extensionID     = "gladia.io/v2"
	officialAPIHost = "api.gladia.io"
	// livePath is both the init POST path and the path of the session URL that
	// the init call returns.
	livePath = "/v2/live"
	// apiKeyHeader carries the account credential on the init POST only. It is
	// never sent on the WebSocket, which authenticates with the session token
	// embedded in its URL.
	apiKeyHeader = "x-gladia-key"

	// maxInitBodyBytes bounds how much of an init response this adapter will
	// read, so a misbehaving upstream cannot force unbounded allocation.
	maxInitBodyBytes = 1 << 20
	// maxErrorDetailBytes bounds how much of a rejected init body is retained
	// for the local error extension.
	maxErrorDetailBytes = 1 << 10
)

// Gladia closes idle live sessions with vendor-specific codes. Distinguishing
// them turns an opaque "read failed" into an actionable operator message.
const (
	closeIdleNoAudio         websocket.StatusCode = 4408
	closeIdleNoTranscription websocket.StatusCode = 4504
)

// stopRecordingFrame is Gladia's only documented terminal client command.
var stopRecordingFrame = []byte(`{"type":"stop_recording"}`)

// supportedSampleRates mirrors the documented `sample_rate` enum on
// POST /v2/live. Gladia rejects anything else at init time; failing locally
// keeps the account key off the wire for a request that cannot succeed.
var supportedSampleRates = map[int]struct{}{
	8_000:  {},
	16_000: {},
	32_000: {},
	44_100: {},
	48_000: {},
}

// supportedRegions mirrors the documented `region` query values used to pin
// live transcription to a Gladia deployment.
var supportedRegions = map[string]struct{}{
	"eu-west": {},
	"us-west": {},
}

// Config controls local transport limits. Credentials and provider selection
// always come from the signed session plan, never from this configuration.
type Config struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// Adapter implements Gladia's two-step live transcription API.
type Adapter struct {
	id              string
	httpClient      *http.Client
	eventBuffer     int
	maxMessageBytes int64
	endpointPolicy  upstream.WebSocketPolicy
}

// New creates an STT adapter with bounded provider-event buffering.
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
	if config.EventBuffer < 1 {
		return nil, errors.New("gladia event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("gladia maximum message bytes must be positive")
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
		endpointPolicy:  endpointPolicy,
	}, nil
}

func (a *Adapter) ID() string { return a.id }

// Open opens a provider-direct STT stream. BYOK plans perform the documented
// init POST with the customer's account key and then dial the session URL that
// Gladia returns; managed plans receive that session URL pre-minted by the
// control plane and dial it directly.
func (a *Adapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindSTT {
		return nil, fmt.Errorf("gladia supports stt sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "gladia" {
		return nil, fmt.Errorf("gladia adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("gladia requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("gladia requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("gladia media: %w", err)
	}
	if err := validateMedia(*request.Media); err != nil {
		return nil, err
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model == "" || model == "auto" {
		return nil, errors.New("gladia requires a concrete model in the session plan")
	}
	credential := request.Plan.Route.Credential
	if credential == nil || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("gladia requires a session credential")
	}

	// The plan endpoint is the clean, query-free base for both steps. The
	// policy pins scheme, host, and port before the account key is attached.
	base, err := a.endpointPolicy.Parse(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("gladia endpoint: %w", err)
	}
	if base.Path != livePath {
		return nil, fmt.Errorf("gladia endpoint path must be %s, got %q", livePath, base.Path)
	}

	var session initResponse
	switch request.Plan.Execution.CredentialSource {
	case protocol.CredentialsBYOK:
		if credential.Kind != protocol.CredentialBearer {
			return nil, fmt.Errorf("gladia byok requires a bearer credential, got %q", credential.Kind)
		}
		session, err = a.initSession(ctx, base, credential.Value, model, request)
		if err != nil {
			return nil, err
		}
	case protocol.CredentialsManaged:
		// The control plane already spent the account key on POST /v2/live, so
		// the only thing that reaches this runtime is the short-lived session
		// URL. There is nothing left to initialise.
		if credential.Kind != protocol.CredentialSessionURL {
			return nil, fmt.Errorf("gladia managed routes require a session_url credential, got %q", credential.Kind)
		}
		session.URL, err = a.sessionURL(credential.Value)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("gladia does not support credential source %q", request.Plan.Execution.CredentialSource)
	}

	conn, response, err := websocket.Dial(ctx, session.URL, &websocket.DialOptions{
		HTTPClient: configHTTPClient(a.httpClient),
	})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		return nil, &runtimepkg.ProviderError{
			Code:           statusErrorCode(status),
			Message:        "Gladia streaming connection could not be established",
			Retryable:      retryableStatus(status),
			ProviderStatus: status,
			Cause:          err,
		}
	}
	conn.SetReadLimit(a.maxMessageBytes)
	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &stream{
		conn:           conn,
		ctx:            streamCtx,
		cancel:         cancel,
		events:         make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		seenSessionIDs: make(map[string]struct{}),
	}
	// A BYOK open already knows the provider-side correlation id, so report it
	// before any audio flows. Managed opens learn the same id from the
	// `session_id` on the first inbound frame.
	if err := stream.observeSessionID(session.ID, nil); err != nil {
		_ = stream.abort()
		return nil, err
	}
	go stream.readLoop()
	return stream, nil
}

func configHTTPClient(client *http.Client) *http.Client {
	if client != nil {
		return client
	}
	return http.DefaultClient
}

// validateMedia applies Gladia's documented live audio constraints on top of
// the portable protocol bounds.
func validateMedia(media protocol.MediaFormat) error {
	if _, _, err := gladiaEncoding(media.Encoding); err != nil {
		return err
	}
	if _, ok := supportedSampleRates[media.SampleRateHz]; !ok {
		return fmt.Errorf("gladia does not support sample rate %d Hz", media.SampleRateHz)
	}
	if media.Channels < 1 || media.Channels > 8 {
		return fmt.Errorf("gladia supports 1 to 8 channels, got %d", media.Channels)
	}
	return nil
}

// gladiaEncoding maps a portable encoding onto the documented `encoding` and
// `bit_depth` pair. Gladia's live encoding enum is wav/pcm, wav/alaw, and
// wav/ulaw; Opus has no representation there.
func gladiaEncoding(encoding string) (string, int, error) {
	switch encoding {
	case "pcm_s16le":
		return "wav/pcm", 16, nil
	default:
		return "", 0, fmt.Errorf("gladia does not support media encoding %q", encoding)
	}
}

// initResponse is the documented 201 body of POST /v2/live.
type initResponse struct {
	ID        string `json:"id"`
	CreatedAt string `json:"created_at"`
	URL       string `json:"url"`
}

// initSession performs step one of Gladia's live API and returns the
// session-scoped WebSocket URL to dial.
func (a *Adapter) initSession(ctx context.Context, base *url.URL, apiKey, model string, request runtimepkg.AdapterRequest) (initResponse, error) {
	endpoint, err := initEndpoint(base, request.Plan.Route.Region)
	if err != nil {
		return initResponse{}, err
	}
	body, err := json.Marshal(newInitRequest(model, request.Options, *request.Media))
	if err != nil {
		return initResponse{}, err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return initResponse{}, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set(apiKeyHeader, apiKey)

	response, err := configHTTPClient(a.httpClient).Do(httpRequest)
	if err != nil {
		return initResponse{}, &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "Gladia live session could not be initiated",
			Retryable: true,
			Cause:     err,
		}
	}
	defer response.Body.Close()
	payload, readErr := io.ReadAll(io.LimitReader(response.Body, maxInitBodyBytes))
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return initResponse{}, &runtimepkg.ProviderError{
			Code:           statusErrorCode(response.StatusCode),
			Message:        "Gladia rejected the live session initiation",
			Retryable:      retryableStatus(response.StatusCode),
			ProviderStatus: response.StatusCode,
			// The rejected body explains which field Gladia disliked. It is a
			// response, so it cannot echo the request header that held the key.
			Extensions: extension(marshalData(map[string]any{"body": truncate(string(payload), maxErrorDetailBytes)})),
		}
	}
	if readErr != nil {
		return initResponse{}, &runtimepkg.ProviderError{
			Code:           "provider_unavailable",
			Message:        "Gladia live session response could not be read",
			Retryable:      true,
			ProviderStatus: response.StatusCode,
			Cause:          readErr,
		}
	}
	var session initResponse
	if err := json.Unmarshal(payload, &session); err != nil {
		return initResponse{}, &runtimepkg.ProviderError{
			Code:           "provider_unavailable",
			Message:        "Gladia sent a malformed live session response",
			Retryable:      true,
			ProviderStatus: response.StatusCode,
			Cause:          err,
		}
	}
	// Everything downstream depends on this URL, and it is the credential, so
	// validate it exactly as strictly as a control-plane-minted one.
	session.URL, err = a.sessionURL(session.URL)
	if err != nil {
		return initResponse{}, err
	}
	return session, nil
}

// initEndpoint converts the plan's WebSocket base into the HTTPS init URL.
// Gladia serves both steps from the same host and path, so the only difference
// is the scheme plus the optional documented region selector.
func initEndpoint(base *url.URL, region string) (string, error) {
	endpoint := *base
	switch base.Scheme {
	case "wss":
		endpoint.Scheme = "https"
	case "ws":
		endpoint.Scheme = "http"
	default:
		return "", fmt.Errorf("gladia endpoint scheme %q cannot be used for session initiation", base.Scheme)
	}
	if region = strings.TrimSpace(region); region != "" {
		if _, ok := supportedRegions[region]; !ok {
			return "", fmt.Errorf("gladia does not support region %q", region)
		}
		query := endpoint.Query()
		query.Set("region", region)
		endpoint.RawQuery = query.Encode()
	}
	return endpoint.String(), nil
}

// sessionURL validates a Gladia session URL before it is dialled, whether this
// adapter just received it from the init POST or the control plane minted it.
//
// upstream.WebSocketPolicy deliberately refuses any URL that already carries a
// query, because for every other provider a query is attacker-controllable
// routing. Gladia's token lives in exactly that position, so the query is
// detached for validation and restored afterwards; scheme, host, port,
// userinfo, and fragment are still policy-checked.
func (a *Adapter) sessionURL(raw string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Host == "" {
		return "", errors.New("gladia session url is not an absolute URL")
	}
	if strings.TrimSpace(parsed.RawQuery) == "" {
		return "", errors.New("gladia session url must carry its session token")
	}
	bare := *parsed
	bare.RawQuery = ""
	validated, err := a.endpointPolicy.Parse(bare.String())
	if err != nil {
		return "", fmt.Errorf("gladia session url: %w", err)
	}
	if validated.Path != livePath {
		return "", fmt.Errorf("gladia session url path must be %s, got %q", livePath, validated.Path)
	}
	validated.RawQuery = parsed.RawQuery
	return validated.String(), nil
}

// initRequest is the documented POST /v2/live body. Only fields this adapter
// actually relies on are sent; every omitted field keeps its vendor default.
type initRequest struct {
	Encoding           string              `json:"encoding"`
	BitDepth           int                 `json:"bit_depth"`
	SampleRate         int                 `json:"sample_rate"`
	Channels           int                 `json:"channels"`
	Model              string              `json:"model"`
	LanguageConfig     *languageConfig     `json:"language_config,omitempty"`
	PreProcessing      *preProcessing      `json:"pre_processing,omitempty"`
	RealtimeProcessing *realtimeProcessing `json:"realtime_processing,omitempty"`
	MessagesConfig     messagesConfig      `json:"messages_config"`
}

// preProcessing carries the documented POST /v2/live audio clean-up options.
// audio_enhancer is the wire shape behind the portable noise_reduction ask —
// the one evidenced noise-removal parameter across this catalog's vendors.
type preProcessing struct {
	AudioEnhancer bool `json:"audio_enhancer"`
}

// realtimeProcessing switches on live add-ons. Only custom vocabulary is used:
// the flag and its config ride together because the vendor ignores the config
// without the flag — a silently unbiased session, not an error.
type realtimeProcessing struct {
	CustomVocabulary       bool                    `json:"custom_vocabulary"`
	CustomVocabularyConfig *customVocabularyConfig `json:"custom_vocabulary_config,omitempty"`
}

type customVocabularyConfig struct {
	Vocabulary []string `json:"vocabulary"`
}

type languageConfig struct {
	Languages     []string `json:"languages"`
	CodeSwitching bool     `json:"code_switching"`
}

type messagesConfig struct {
	ReceivePartialTranscripts       bool `json:"receive_partial_transcripts"`
	ReceiveFinalTranscripts         bool `json:"receive_final_transcripts"`
	ReceiveSpeechEvents             bool `json:"receive_speech_events"`
	ReceiveErrors                   bool `json:"receive_errors"`
	ReceiveAcknowledgments          bool `json:"receive_acknowledgments"`
	ReceiveLifecycleEvents          bool `json:"receive_lifecycle_events"`
	ReceivePreProcessingEvents      bool `json:"receive_pre_processing_events"`
	ReceiveRealtimeProcessingEvents bool `json:"receive_realtime_processing_events"`
	ReceivePostProcessingEvents     bool `json:"receive_post_processing_events"`
}

// gladiaLanguage narrows a portable language tag to the primary subtag Gladia
// accepts. Its TranscriptionLanguageCodeEnum is 207 bare ISO codes with no
// regional variant anywhere in the list, so a caller's "en-US" would be
// rejected at init while "en" is exactly what the vendor asked for.
func gladiaLanguage(tag string) string {
	primary := strings.TrimSpace(tag)
	if index := strings.IndexAny(primary, "-_"); index >= 0 {
		primary = primary[:index]
	}
	return strings.ToLower(primary)
}

func newInitRequest(model string, options protocol.RequestOptions, media protocol.MediaFormat) initRequest {
	encoding, bitDepth, _ := gladiaEncoding(media.Encoding)
	request := initRequest{
		Encoding:   encoding,
		BitDepth:   bitDepth,
		SampleRate: media.SampleRateHz,
		Channels:   media.Channels,
		Model:      model,
		MessagesConfig: messagesConfig{
			// Gladia defaults partials to false. transcript.delta only exists
			// because this flag is true, so it is not optional here.
			ReceivePartialTranscripts: true,
			ReceiveFinalTranscripts:   true,
			ReceiveSpeechEvents:       true,
			ReceiveErrors:             true,
			// Per-chunk acknowledgements would be pure noise: the runtime
			// already knows what it wrote and cannot act on a byte range.
			ReceiveAcknowledgments:     false,
			ReceiveLifecycleEvents:     false,
			ReceivePreProcessingEvents: false,
			// Realtime add-on events stay off even when custom vocabulary is
			// requested below: vocabulary biases recognition inside the
			// transcript stream rather than emitting events of its own, and
			// no event-emitting add-on (translation, NER, sentiment) is used.
			ReceiveRealtimeProcessingEvents: false,
			// post_final_transcript carries Gladia's own billed duration,
			// which is the one usage number worth reporting upstream.
			ReceivePostProcessingEvents: true,
		},
	}
	if language := gladiaLanguage(options.Language); language != "" {
		request.LanguageConfig = &languageConfig{Languages: []string{language}, CodeSwitching: false}
	}
	if options.STT.ReduceNoise() {
		request.PreProcessing = &preProcessing{AudioEnhancer: true}
	}
	if keywords := options.STT.GetKeywords(); len(keywords) > 0 {
		request.RealtimeProcessing = &realtimeProcessing{
			CustomVocabulary:       true,
			CustomVocabularyConfig: &customVocabularyConfig{Vocabulary: keywords},
		}
	}
	// `endpointing` is deliberately omitted. The framework owns turn detection,
	// and Gladia's endpointing cannot be disabled (its documented range starts
	// at 0.01s), so pinning a value here would only fight the vendor default.
	return request
}

type stream struct {
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent

	writeMu   sync.Mutex
	stateMu   sync.Mutex
	stopOnce  sync.Once
	abortOnce sync.Once
	stopping  atomic.Bool
	closed    atomic.Bool
	closeErr  error

	seenSessionIDs map[string]struct{}
}

func (s *stream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

// WriteAudio sends raw audio as a binary frame. Gladia accepts either binary
// frames or a base64 payload inside {"type":"audio_chunk","data":{"chunk":...}};
// binary is chosen because it avoids a 33% base64 expansion on every frame.
func (s *stream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("gladia audio is empty")
	}
	if s.stopping.Load() {
		return runtimepkg.ErrSessionClosed
	}
	return s.write(ctx, websocket.MessageBinary, audio)
}

// CommitAudio sends stop_recording. Gladia has no non-terminal flush command,
// so this is the same frame Close sends and it is guarded to fire once.
func (s *stream) CommitAudio(ctx context.Context) error { return s.stopRecording(ctx) }

func (s *stream) AppendText(context.Context, string) error { return runtimepkg.ErrUnsupportedOperation }

func (s *stream) CommitText(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// Cancel tears the socket down immediately. Gladia has no cancel command, and
// the only alternative frame, stop_recording, asks for a final transcript that
// a cancelling caller has already said it does not want.
func (s *stream) Cancel(context.Context) error { return s.abort() }

// Abort immediately tears down the socket after a terminal runtime failure.
func (s *stream) Abort(context.Context) error { return s.abort() }

// Close asks Gladia to flush and finish. The server answers with any pending
// final transcript and then closes with 1000, which ends the read loop.
func (s *stream) Close(ctx context.Context) error { return s.stopRecording(ctx) }

func (s *stream) stopRecording(ctx context.Context) error {
	s.stopOnce.Do(func() {
		s.stopping.Store(true)
		if err := s.write(ctx, websocket.MessageText, stopRecordingFrame); err != nil {
			s.closeErr = err
			_ = s.abort()
		}
	})
	return s.closeErr
}

func (s *stream) abort() error {
	s.abortOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		if err := s.conn.CloseNow(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
	})
	return s.closeErr
}

func (s *stream) write(ctx context.Context, messageType websocket.MessageType, payload []byte) error {
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
			Message:   "Gladia streaming write failed",
			Retryable: true,
			Cause:     err,
		}
	}
	return nil
}

func (s *stream) readLoop() {
	defer func() {
		s.closed.Store(true)
		s.cancel()
		close(s.events)
	}()
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && !s.stopping.Load() && s.ctx.Err() == nil && !isNormalClose(err) {
				s.emit(runtimepkg.ProviderEvent{Err: readError(err)})
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
	var message inboundMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "Gladia sent malformed streaming JSON",
			Retryable: true,
			Cause:     err,
		}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))
	if err := s.observeSessionID(message.SessionID, raw); err != nil {
		return err
	}
	// Gladia's AsyncAPI defines no standalone error frame. Failures ride as an
	// optional `error` object, and only on the acknowledgement, add-on, and
	// post-processing messages: transcript, speech_start, and speech_end have
	// no error field at all. Acknowledgements and add-ons are switched off for
	// this session, so the only frames that can carry one are post-processing
	// results the session genuinely depends on, making any error here terminal.
	if message.Error != nil {
		status := errorStatus(message.Error.StatusCode)
		return &runtimepkg.ProviderError{
			Code:           statusErrorCode(status),
			Message:        gladiaErrorMessage(message.Error),
			Retryable:      retryableStatus(status),
			ProviderStatus: status,
			Extensions:     extension(raw),
		}
	}
	switch message.Type {
	case "transcript":
		return s.emitTranscript(message, raw)
	case "speech_start":
		return s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventSpeechStarted,
			Data:       speechData("audio_start_ms", message),
			Extensions: extension(raw),
		})
	case "speech_end":
		return s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventSpeechEnded,
			Data:       speechData("audio_end_ms", message),
			Extensions: extension(raw),
		})
	case "post_final_transcript":
		return s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventUsageObserved,
			Data:       billingData(message),
			Extensions: extension(raw),
		})
	case "post_transcript":
		// The same content that post_final_transcript reports with billing
		// metadata attached. Dropping it avoids a duplicate usage event.
		return nil
	default:
		return s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventWarning,
			Data:       warningData(message),
			Extensions: extension(raw),
		})
	}
}

func (s *stream) emitTranscript(message inboundMessage, raw json.RawMessage) error {
	// Gladia pads utterance text with a leading space; only the emptiness test
	// trims, so the provider's own spacing survives into the transcript.
	if strings.TrimSpace(message.Data.Utterance.Text) == "" {
		return nil
	}
	kind := protocol.EventTranscriptDelta
	if message.Data.IsFinal {
		kind = protocol.EventTranscriptFinal
	}
	// No speech.ended is synthesised from a final transcript: this session
	// requests real speech events, so Gladia sends its own speech_end.
	return s.emit(runtimepkg.ProviderEvent{
		Type:       kind,
		Data:       transcriptData(message),
		Extensions: extension(raw),
	})
}

// observeSessionID reports the Gladia session id once per stream as the
// provider request correlation id.
func (s *stream) observeSessionID(sessionID string, raw json.RawMessage) error {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil
	}
	s.stateMu.Lock()
	if _, exists := s.seenSessionIDs[sessionID]; exists {
		s.stateMu.Unlock()
		return nil
	}
	s.seenSessionIDs[sessionID] = struct{}{}
	s.stateMu.Unlock()
	event := runtimepkg.ProviderEvent{
		Type: protocol.EventUsageObserved,
		Data: marshalData(map[string]any{"provider_request_id": sessionID}),
	}
	if len(raw) > 0 {
		event.Extensions = extension(raw)
	}
	return s.emit(event)
}

func (s *stream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func isNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}

func readError(err error) *runtimepkg.ProviderError {
	message := "Gladia streaming read failed"
	switch websocket.CloseStatus(err) {
	case closeIdleNoAudio:
		message = "Gladia closed the session after receiving no audio"
	case closeIdleNoTranscription:
		message = "Gladia closed the session after receiving no transcribable audio"
	}
	return &runtimepkg.ProviderError{
		Code:      "provider_unavailable",
		Message:   message,
		Retryable: true,
		Cause:     err,
	}
}

// statusErrorCode maps an HTTP-shaped status onto the stable protocol error
// classification. Gladia documents 400/401/422 on init and 429 on the socket;
// 402 and 403 follow the same HTTP semantics the rest of the gateway uses.
func statusErrorCode(status int) string {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return "authentication_failed"
	case http.StatusPaymentRequired:
		return "provider_quota_exceeded"
	case http.StatusTooManyRequests:
		return "provider_rate_limited"
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return "invalid_request"
	default:
		return "provider_unavailable"
	}
}

func retryableStatus(status int) bool {
	return status == 0 || status == http.StatusTooManyRequests || status >= 500
}

// errorStatus reads Gladia's `status_code`, which the schema types as either a
// number or a string.
func errorStatus(raw json.RawMessage) int {
	if len(raw) == 0 {
		return 0
	}
	var number int
	if err := json.Unmarshal(raw, &number); err == nil {
		return number
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		if parsed, err := strconv.Atoi(strings.TrimSpace(text)); err == nil {
			return parsed
		}
	}
	return 0
}

func gladiaErrorMessage(failure *errorObject) string {
	detail := strings.TrimSpace(failure.Message)
	if detail == "" {
		detail = strings.TrimSpace(failure.Exception)
	}
	if detail == "" {
		return "Gladia reported a transcription error"
	}
	return "Gladia reported a transcription error: " + detail
}

func extension(raw json.RawMessage) map[string]json.RawMessage {
	return map[string]json.RawMessage{extensionID: raw}
}

func transcriptData(message inboundMessage) json.RawMessage {
	utterance := message.Data.Utterance
	return marshalData(map[string]any{
		"text":                utterance.Text,
		"is_final":            message.Data.IsFinal,
		"audio_start_ms":      milliseconds(utterance.Start),
		"audio_end_ms":        milliseconds(utterance.End),
		"language":            utterance.Language,
		"confidence":          utterance.Confidence,
		"channel":             utterance.Channel,
		"words":               utterance.Words,
		"utterance_id":        message.Data.ID,
		"provider_request_id": message.SessionID,
	})
}

func speechData(key string, message inboundMessage) json.RawMessage {
	return marshalData(map[string]any{
		key:                   milliseconds(message.Data.Time),
		"channel":             message.Data.Channel,
		"provider_request_id": message.SessionID,
	})
}

func billingData(message inboundMessage) json.RawMessage {
	return marshalData(map[string]any{
		"provider_request_id": message.SessionID,
		"audio_duration_ms":   milliseconds(message.Data.Metadata.AudioDuration),
		"billed_duration_ms":  milliseconds(message.Data.Metadata.BillingTime),
	})
}

func warningData(message inboundMessage) json.RawMessage {
	return marshalData(map[string]any{
		"message":             "ignored Gladia message type",
		"provider_type":       message.Type,
		"provider_request_id": message.SessionID,
	})
}

func marshalData(value any) json.RawMessage {
	payload, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{"code":"internal"}`)
	}
	return payload
}

func milliseconds(seconds float64) int64 { return int64(math.Round(seconds * 1_000)) }

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}

// inboundMessage is the shared envelope every documented server frame uses.
type inboundMessage struct {
	Type      string       `json:"type"`
	SessionID string       `json:"session_id"`
	CreatedAt string       `json:"created_at"`
	Error     *errorObject `json:"error"`
	Data      inboundData  `json:"data"`
}

// inboundData unions the `data` payloads of the frame types this adapter
// consumes. Unused members stay zero for frames that do not carry them.
type inboundData struct {
	ID        string    `json:"id"`
	IsFinal   bool      `json:"is_final"`
	Utterance utterance `json:"utterance"`
	Time      float64   `json:"time"`
	Channel   int       `json:"channel"`
	Metadata  struct {
		AudioDuration float64 `json:"audio_duration"`
		BillingTime   float64 `json:"billing_time"`
	} `json:"metadata"`
}

type utterance struct {
	Text       string          `json:"text"`
	Start      float64         `json:"start"`
	End        float64         `json:"end"`
	Language   string          `json:"language"`
	Confidence float64         `json:"confidence"`
	Channel    int             `json:"channel"`
	Words      json.RawMessage `json:"words"`
}

type errorObject struct {
	StatusCode json.RawMessage `json:"status_code"`
	Exception  string          `json:"exception"`
	Message    string          `json:"message"`
}
