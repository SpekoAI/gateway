package alibaba

import (
	"context"
	"encoding/base64"
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
	// STTAdapterID is the identifier returned by an Alibaba STT session plan.
	STTAdapterID = "alibaba.stt.v1"

	// STTDefaultModel is the model a control plane should place in a plan when
	// the caller does not pin one. The adapter itself never defaults: a plan
	// must always name a concrete model.
	STTDefaultModel = "qwen3-asr-flash-realtime"

	// InternationalAPIHost is DashScope's Singapore host and the only endpoint
	// host allowed without configuration. An API key is region-scoped, so
	// pointing a Singapore credential at Beijing fails authentication.
	InternationalAPIHost = "dashscope-intl.aliyuncs.com"
	// MainlandAPIHost is DashScope's China (Beijing) host. It must be named in
	// Config.AllowedEndpointHosts to be reachable. Workspace-scoped hosts
	// ({WorkspaceId}.ap-southeast-1.maas.aliyuncs.com and the cn-beijing twin)
	// are per tenant and can only ever arrive the same way.
	MainlandAPIHost = "dashscope.aliyuncs.com"

	// realtimeEndpointPath is shared by the STT and TTS realtime endpoints;
	// the `model` query parameter is what distinguishes them.
	realtimeEndpointPath = "/api-ws/v1/realtime"

	// extensionID namespaces the raw DashScope frame retained on events that
	// carry vendor detail the canonical protocol does not model.
	extensionID = "dashscope.aliyuncs.com/api-ws/v1/realtime"

	// sttFrameBytes caps one input_audio_buffer.append payload. 3200 B is the
	// chunk size Alibaba's own reference client uses (100 ms of 16 kHz s16le
	// mono) and is sample-aligned, so a caller handing over a whole utterance
	// in one write does not produce an oversized frame.
	sttFrameBytes = 3_200
)

// sttSupportedModels is the documented Qwen-ASR-Realtime lineup. Validating
// locally turns a stale control-plane pin into an Open error rather than a
// handshake that succeeds and then fails on the first frame.
var sttSupportedModels = map[string]struct{}{
	"qwen3-asr-flash-realtime":            {},
	"qwen3-asr-flash-realtime-2026-02-10": {},
	"qwen3-asr-flash-realtime-2025-10-27": {},
}

// sttSupportedSampleRates is the fixed set session.sample_rate accepts. The
// canonical MediaFormat is far wider, so the adapter has to narrow it. 8000 is
// accepted but upsampled server side, so it is only worth using for audio that
// is natively 8 kHz.
var sttSupportedSampleRates = map[int]struct{}{8_000: {}, 16_000: {}}

// sttSupportedLanguages is the input vocabulary from the client-events
// reference. It is deliberately taken from THAT table rather than from the
// `language` field documented on the server events, which lists only 18 of
// these 27 codes; the client table is the request contract. Every entry is a
// bare subtag, so a regional tag has to be narrowed before it is sent.
var sttSupportedLanguages = map[string]struct{}{
	"zh": {}, "yue": {}, "en": {}, "ja": {}, "de": {}, "ko": {}, "ru": {},
	"fr": {}, "pt": {}, "ar": {}, "it": {}, "es": {}, "hi": {}, "id": {},
	"th": {}, "tr": {}, "uk": {}, "vi": {}, "cs": {}, "da": {}, "fil": {},
	"fi": {}, "is": {}, "ms": {}, "no": {}, "pl": {}, "sv": {},
}

// STTConfig controls local transport limits. Credentials and provider
// selection always come from the signed session plan, never from here.
type STTConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// STTAdapter implements DashScope's Qwen-ASR-Realtime WebSocket API.
type STTAdapter struct {
	id              string
	httpClient      *http.Client
	eventBuffer     int
	maxMessageBytes int64
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
		return nil, errors.New("alibaba event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("alibaba maximum message bytes must be positive")
	}
	endpointPolicy, err := upstream.NewWebSocketPolicy(InternationalAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
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

// Open dials Qwen-ASR-Realtime and configures the session. The session.update
// is written here rather than lazily so a rejected audio format or language
// surfaces before the runtime starts pushing media at the socket.
func (a *STTAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindSTT {
		return nil, fmt.Errorf("alibaba stt supports stt sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "alibaba" {
		return nil, fmt.Errorf("alibaba stt adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("alibaba stt requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("alibaba stt requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("alibaba stt media: %w", err)
	}
	if err := sttValidateMedia(*request.Media); err != nil {
		return nil, err
	}
	language, err := sttLanguage(request.Options.Language)
	if err != nil {
		return nil, err
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("alibaba stt requires a bearer credential")
	}
	endpoint, err := realtimeEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint, request.Plan.Route.Model, sttSupportedModels, "alibaba stt")
	if err != nil {
		return nil, err
	}

	// One credential channel, deliberately. A managed plan carries DashScope's
	// short-lived "temporary API key" (st-...), a BYOK plan carries the
	// customer's permanent key (sk-...), and a relay plan carries the relay
	// connector's permanent key. All are API keys and all are presented as
	// `Authorization: Bearer`; DashScope documents no query parameter, no
	// second header, and no STS material on this endpoint, so the relay arm
	// changes nothing about placement.
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer "+credential.Value)
	// Sent because every Qwen-ASR-Realtime sample Alibaba publishes sets it,
	// even though its reference header table omits it. The TTS twin has no
	// such sample and does not send it.
	headers.Set("OpenAI-Beta", "realtime=v1")

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
			Message:        "Alibaba streaming connection could not be established",
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
	if err := stream.writeJSON(ctx, sttSessionUpdate(stream.ids.next(), request.Media.SampleRateHz, language)); err != nil {
		_ = stream.abort()
		return nil, err
	}
	go stream.readLoop()
	return stream, nil
}

func httpClientOrDefault(client *http.Client) *http.Client {
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
// Engine, no SessionPlan.Validate — labels the same permanent DashScope key
// bearer. Both spellings carry a permanent key on the one Authorization:
// Bearer channel this endpoint has, so nothing else on the relay arm changes.
func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

// realtimeEndpoint validates the plan endpoint and appends the model selector.
// Both realtime adapters share the resource, so they share this builder.
func realtimeEndpoint(policy upstream.WebSocketPolicy, rawEndpoint, model string, supported map[string]struct{}, subject string) (string, error) {
	endpoint, err := policy.Parse(rawEndpoint)
	if err != nil {
		return "", fmt.Errorf("%s endpoint: %w", subject, err)
	}
	if endpoint.Path != realtimeEndpointPath {
		return "", fmt.Errorf("%s endpoint path must be %s, got %q", subject, realtimeEndpointPath, endpoint.Path)
	}
	model = strings.TrimSpace(model)
	if model == "" || model == "auto" {
		return "", fmt.Errorf("%s requires a concrete model in the session plan", subject)
	}
	if _, ok := supported[model]; !ok {
		return "", fmt.Errorf("%s does not support model %q", subject, model)
	}
	query := endpoint.Query()
	query.Set("model", model)
	endpoint.RawQuery = query.Encode()
	return endpoint.String(), nil
}

func sttValidateMedia(media protocol.MediaFormat) error {
	// session.input_audio_format is "pcm", which DashScope documents as signed
	// 16-bit little-endian samples. `opus` is the only other accepted value and
	// no canonical encoding maps onto it here.
	if media.Encoding != "pcm_s16le" {
		return fmt.Errorf("alibaba stt requires pcm_s16le, got %q", media.Encoding)
	}
	// The vendor's own preparation step is `ffmpeg -ar 16000 -ac 1 -f s16le`.
	// Interleaved stereo would be read as twice the sample rate and transcribe
	// into gibberish while the session still looks healthy.
	if media.Channels != 1 {
		return fmt.Errorf("alibaba stt requires mono audio, got %d channels", media.Channels)
	}
	if _, ok := sttSupportedSampleRates[media.SampleRateHz]; !ok {
		return fmt.Errorf("alibaba stt does not accept a %d Hz sample rate", media.SampleRateHz)
	}
	return nil
}

// sttLanguage narrows a portable tag to the bare subtag DashScope accepts. An
// empty language is valid and means auto-detect; an unsupported one is refused
// rather than sent, because the vendor answers an unknown code with an error
// frame after the socket is already up.
func sttLanguage(language string) (string, error) {
	narrowed := baseLanguageTag(language)
	if narrowed == "" {
		return "", nil
	}
	if _, ok := sttSupportedLanguages[narrowed]; !ok {
		return "", fmt.Errorf("alibaba stt does not support language %q", language)
	}
	return narrowed, nil
}

func baseLanguageTag(language string) string {
	lowered := strings.ToLower(strings.TrimSpace(language))
	if index := strings.IndexAny(lowered, "-_"); index > 0 {
		return lowered[:index]
	}
	return lowered
}

// sttSessionUpdate builds the one configuration frame. Three omissions are
// deliberate:
//
//   - `modalities` is not sent. Alibaba's sample includes it but the reference
//     session-configuration table does not document it as a client field; the
//     server reports it as fixed to ["text"] anyway.
//   - turn_detection carries only `type`. threshold and silence_duration_ms
//     have documented defaults (0.2 and 800 ms) and the framework, not the
//     vendor, owns turn policy.
//   - input_audio_transcription is omitted entirely when no language is
//     pinned, which is how auto-detect is requested.
func sttSessionUpdate(eventID string, sampleRateHz int, language string) sttSessionUpdateFrame {
	session := sttSessionConfig{
		InputAudioFormat: "pcm",
		SampleRate:       sampleRateHz,
		TurnDetection:    &sttTurnDetection{Type: "server_vad"},
	}
	if language != "" {
		session.InputAudioTranscription = &sttTranscriptionConfig{Language: language}
	}
	return sttSessionUpdateFrame{EventID: eventID, Type: "session.update", Session: session}
}

type sttTurnDetection struct {
	Type string `json:"type"`
}

type sttTranscriptionConfig struct {
	Language string `json:"language"`
}

type sttSessionConfig struct {
	InputAudioFormat        string                  `json:"input_audio_format"`
	SampleRate              int                     `json:"sample_rate"`
	InputAudioTranscription *sttTranscriptionConfig `json:"input_audio_transcription,omitempty"`
	TurnDetection           *sttTurnDetection       `json:"turn_detection"`
}

type sttSessionUpdateFrame struct {
	EventID string           `json:"event_id"`
	Type    string           `json:"type"`
	Session sttSessionConfig `json:"session"`
}

type sttAudioAppendFrame struct {
	EventID string `json:"event_id"`
	Type    string `json:"type"`
	Audio   string `json:"audio"`
}

type sttStream struct {
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent
	ids    eventIDs

	writeMu      sync.Mutex
	finishOnce   sync.Once
	gracefulOnce sync.Once
	abortOnce    sync.Once
	finishErr    error
	closeErr     error
	inputClosed  atomic.Bool
	closed       atomic.Bool

	// sessionID is written and read only by readLoop.
	sessionID string
}

func (s *sttStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

// WriteAudio forwards PCM as base64 inside JSON frames. Qwen-ASR-Realtime has
// no binary input frame at all, so this is a text write even though the
// payload is audio.
func (s *sttStream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("alibaba stt audio is empty")
	}
	// session.finish has already gone out; DashScope considers the session over
	// and will not transcribe anything further.
	if s.inputClosed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	for offset := 0; offset < len(audio); offset += sttFrameBytes {
		end := min(offset+sttFrameBytes, len(audio))
		frame := sttAudioAppendFrame{
			EventID: s.ids.next(),
			Type:    "input_audio_buffer.append",
			Audio:   base64.StdEncoding.EncodeToString(audio[offset:end]),
		}
		if err := s.writeJSON(ctx, frame); err != nil {
			return err
		}
	}
	return nil
}

// CommitAudio ends the session, because in VAD mode that is the only flush the
// protocol has. input_audio_buffer.commit is documented as disabled while
// turn_detection is set, and session.finish is answered by a final transcript
// followed by session.finished. See the package doc for why these converge.
func (s *sttStream) CommitAudio(ctx context.Context) error { return s.finishSession(ctx) }

func (s *sttStream) AppendText(context.Context, string) error {
	return runtimepkg.ErrUnsupportedOperation
}

func (s *sttStream) CommitText(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// Cancel abandons the session. There is no distinct cancel frame, so this
// closes and then tears down rather than waiting for a final transcript.
func (s *sttStream) Cancel(ctx context.Context) error {
	if err := s.Close(ctx); err != nil {
		return err
	}
	return s.abort()
}

// Abort immediately tears down the socket after a terminal runtime failure.
func (s *sttStream) Abort(context.Context) error { return s.abort() }

// Close finishes the session and leaves the socket readable. Dropping it here
// would discard the final transcript DashScope emits between session.finish
// and session.finished; the read loop ends when the server closes.
func (s *sttStream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		if err := s.finishSession(ctx); err != nil && !errors.Is(err, runtimepkg.ErrSessionClosed) {
			s.closeErr = err
		}
		s.closed.Store(true)
		if s.closeErr != nil {
			_ = s.abort()
		}
	})
	return s.closeErr
}

// finishSession writes session.finish at most once and refuses further audio.
func (s *sttStream) finishSession(ctx context.Context) error {
	s.finishOnce.Do(func() {
		s.inputClosed.Store(true)
		if s.closed.Load() {
			s.finishErr = runtimepkg.ErrSessionClosed
			return
		}
		s.writeMu.Lock()
		defer s.writeMu.Unlock()
		payload, err := json.Marshal(sessionFinishFrame{EventID: s.ids.next(), Type: "session.finish"})
		if err != nil {
			s.finishErr = err
			return
		}
		if err := s.conn.Write(ctx, websocket.MessageText, payload); err != nil {
			s.finishErr = &runtimepkg.ProviderError{
				Code:      "provider_unavailable",
				Message:   "Alibaba streaming write failed",
				Retryable: true,
				Cause:     err,
			}
		}
	})
	return s.finishErr
}

func (s *sttStream) abort() error {
	s.abortOnce.Do(func() {
		s.closed.Store(true)
		s.inputClosed.Store(true)
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
			Message:   "Alibaba streaming write failed",
			Retryable: true,
			Cause:     err,
		}
	}
	return nil
}

func (s *sttStream) readLoop() {
	defer close(s.events)
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && !isNormalClose(err) && s.ctx.Err() == nil {
				s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{
					Code:      "provider_unavailable",
					Message:   "Alibaba streaming read failed",
					Retryable: true,
					Cause:     err,
				}})
			}
			return
		}
		// The realtime protocol is JSON only in both directions; a binary frame
		// would be a protocol violation rather than audio.
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

// handleMessage reports whether the session is over in addition to any
// terminal error, because session.finished is a clean end rather than a
// failure and must not be reported as one.
func (s *sttStream) handleMessage(payload []byte) (bool, error) {
	var message sttInbound
	if err := json.Unmarshal(payload, &message); err != nil {
		return false, &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "Alibaba sent malformed streaming JSON",
			Retryable: true,
			Cause:     err,
		}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))
	switch message.Type {
	case "session.created":
		if message.Session != nil {
			s.sessionID = message.Session.ID
		}
		return false, s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventUsageObserved,
			Data:       marshalData(map[string]any{"provider_request_id": s.sessionID}),
			Extensions: extension(raw),
		})
	case "session.updated":
		// Configuration acknowledged; it carries no caller-visible state.
		return false, nil
	case "input_audio_buffer.speech_started":
		return false, s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventSpeechStarted,
			Data:       marshalData(map[string]any{"audio_start_ms": message.AudioStartMS}),
			Extensions: extension(raw),
		})
	case "input_audio_buffer.speech_stopped":
		return false, s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventSpeechEnded,
			Data:       marshalData(map[string]any{"audio_end_ms": message.AudioEndMS}),
			Extensions: extension(raw),
		})
	case "input_audio_buffer.committed", "conversation.item.created":
		// Turn bookkeeping. The transcript rides its own events.
		return false, nil
	case "conversation.item.input_audio_transcription.text":
		// `text` is the confirmed prefix and `stash` the draft suffix the model
		// may still revise. The hypothesis is their concatenation: early in an
		// utterance `text` is empty and the whole preview lives in `stash`, so
		// reading only `text` silently emits nothing.
		hypothesis := message.Text + message.Stash
		if strings.TrimSpace(hypothesis) == "" {
			return false, nil
		}
		return false, s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventTranscriptDelta,
			Data:       sttTranscriptData(hypothesis, false, message, s.sessionID),
			Extensions: extension(raw),
		})
	case "conversation.item.input_audio_transcription.completed":
		if strings.TrimSpace(message.Transcript) == "" {
			return false, nil
		}
		return false, s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventTranscriptFinal,
			Data:       sttTranscriptData(message.Transcript, true, message, s.sessionID),
			Extensions: extension(raw),
		})
	case "conversation.item.input_audio_transcription.failed":
		// A per-item failure, kept distinct from the session-level `error`
		// frame so a single unrecognizable turn is not reported as a dead key.
		return false, realtimeProviderError("Alibaba failed to transcribe an item", message.Error, raw)
	case "error":
		return false, realtimeProviderError("Alibaba reported a streaming error", message.Error, raw)
	case "session.finished":
		// Recognition is complete and the vendor expects the client to
		// disconnect. Ending the loop closes the event channel.
		return true, nil
	default:
		return false, s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventWarning,
			Data:       marshalData(map[string]any{"message": "ignored Alibaba message type", "provider_type": message.Type}),
			Extensions: extension(raw),
		})
	}
}

func (s *sttStream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// sttTranscriptData carries no word timings on purpose: Qwen-ASR-Realtime
// documents that it returns no timestamps at any granularity.
func sttTranscriptData(text string, final bool, message sttInbound, sessionID string) json.RawMessage {
	data := map[string]any{
		"text":                text,
		"is_final":            final,
		"speech_final":        final,
		"provider_request_id": sessionID,
	}
	if message.Language != "" {
		data["language"] = message.Language
	}
	// Emotion is always on for this model family and has no canonical field,
	// so it rides the provider-neutral payload rather than being dropped.
	if message.Emotion != "" {
		data["emotion"] = message.Emotion
	}
	return marshalData(data)
}

type sessionFinishFrame struct {
	EventID string `json:"event_id"`
	Type    string `json:"type"`
}

type realtimeError struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
	Param   string `json:"param"`
}

type sttInbound struct {
	Type         string `json:"type"`
	EventID      string `json:"event_id"`
	ItemID       string `json:"item_id"`
	Language     string `json:"language"`
	Emotion      string `json:"emotion"`
	Text         string `json:"text"`
	Stash        string `json:"stash"`
	Transcript   string `json:"transcript"`
	AudioStartMS int64  `json:"audio_start_ms"`
	AudioEndMS   int64  `json:"audio_end_ms"`
	Session      *struct {
		ID string `json:"id"`
	} `json:"session"`
	Error *realtimeError `json:"error"`
}

// eventIDs supplies the `event_id` every client frame requires. DashScope asks
// only that it be unique within the session, so a per-stream counter satisfies
// the contract without pulling in a UUID dependency.
type eventIDs struct{ counter atomic.Uint64 }

func (e *eventIDs) next() string {
	return "event_" + strconv.FormatUint(e.counter.Add(1), 10)
}

func isNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}

// dialErrorCode maps a handshake status onto the canonical taxonomy.
// DashScope verifies Authorization during the WebSocket handshake, so a bad or
// expired key fails here with 401/403 rather than as a frame.
func dialErrorCode(status int) string {
	switch {
	case status == http.StatusUnauthorized, status == http.StatusForbidden:
		return "authentication_failed"
	case status == http.StatusTooManyRequests:
		return "provider_rate_limited"
	case status == 0 || status >= 500:
		return "provider_unavailable"
	default:
		return "invalid_request"
	}
}

// realtimeProviderError turns an error-bearing frame into a terminal provider
// error. The vendor's code is the only usable signal once the socket is up.
func realtimeProviderError(summary string, detail *realtimeError, raw json.RawMessage) *runtimepkg.ProviderError {
	code, message := "", ""
	if detail != nil {
		code, message = detail.Code, detail.Message
	}
	canonical, retryable := classifyRealtimeError(code)
	text := summary
	if strings.TrimSpace(code) != "" {
		text += " (" + code + ")"
	}
	if strings.TrimSpace(message) != "" {
		text += ": " + message
	}
	return &runtimepkg.ProviderError{
		Code:       canonical,
		Message:    text,
		Retryable:  retryable,
		Extensions: extension(raw),
	}
}

// classifyRealtimeError maps Model Studio's documented error codes onto the
// canonical taxonomy. Collapsing these would make a revoked key, an empty
// balance, and a throttled tenant indistinguishable, and only some of those
// are worth retrying anywhere.
//
// Order matters: Throttling.AllocationQuota is a rate limit that happens to
// mention quota, so the throttling family is matched before the quota family.
func classifyRealtimeError(code string) (string, bool) {
	normalized := strings.ToLower(strings.TrimSpace(code))
	switch {
	case normalized == "":
		return "provider_unavailable", false
	case strings.HasPrefix(normalized, "throttling"),
		normalized == "limit_requests",
		normalized == "limit_burst_rate":
		return "provider_rate_limited", true
	case normalized == "arrearage",
		strings.HasPrefix(normalized, "allocationquota"):
		return "provider_quota_exceeded", false
	case normalized == "invalidapikey",
		normalized == "invalid_api_key",
		normalized == "access_denied",
		strings.HasPrefix(normalized, "accessdenied"),
		strings.HasSuffix(normalized, ".accessdenied"):
		return "authentication_failed", false
	case strings.HasPrefix(normalized, "internalerror"),
		normalized == "internal_error",
		normalized == "systemerror",
		normalized == "modelservicefailed",
		normalized == "modelservingerror",
		normalized == "modelunavailable",
		normalized == "requesttimeout",
		normalized == "responsetimeout":
		return "provider_unavailable", true
	default:
		return "invalid_request", false
	}
}

func extension(raw json.RawMessage) map[string]json.RawMessage {
	return map[string]json.RawMessage{extensionID: raw}
}

func marshalData(value any) json.RawMessage {
	payload, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{"code":"internal"}`)
	}
	return payload
}
