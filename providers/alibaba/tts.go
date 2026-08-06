package alibaba

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
	// TTSAdapterID is the identifier returned by an Alibaba TTS session plan.
	TTSAdapterID = "alibaba.tts.v1"

	// TTSDefaultModel is the model a control plane should place in a plan when
	// the caller does not pin one. The adapter itself never defaults.
	TTSDefaultModel = "qwen3-tts-flash-realtime"

	// ttsDefaultLanguageType is DashScope's own default. It detects language
	// per segment, which is the only sane behaviour for mixed-language text.
	ttsDefaultLanguageType = "Auto"
)

// ttsSupportedModels is the documented Qwen-TTS-Realtime lineup across both
// regions. The qwen-tts-realtime series is listed for China (Beijing) only,
// so reaching it also requires MainlandAPIHost in AllowedEndpointHosts.
var ttsSupportedModels = map[string]struct{}{
	"qwen3-tts-flash-realtime":                     {},
	"qwen3-tts-flash-realtime-2025-11-27":          {},
	"qwen3-tts-flash-realtime-2025-09-18":          {},
	"qwen3-tts-instruct-flash-realtime":            {},
	"qwen3-tts-instruct-flash-realtime-2026-01-22": {},
	"qwen3-tts-vc-realtime-2026-01-15":             {},
	"qwen3-tts-vc-realtime-2025-11-27":             {},
	"qwen3-tts-vd-realtime-2026-01-15":             {},
	"qwen3-tts-vd-realtime-2025-12-16":             {},
	"qwen-tts-realtime":                            {},
	"qwen-tts-realtime-latest":                     {},
	"qwen-tts-realtime-2025-07-15":                 {},
}

// ttsSupportedSampleRates is the documented session.sample_rate set.
var ttsSupportedSampleRates = map[int]struct{}{8_000: {}, 16_000: {}, 24_000: {}, 48_000: {}}

// ttsLanguageNames maps a portable primary subtag onto DashScope's
// language_type vocabulary, which is English language NAMES rather than
// language codes. Sending "en" instead of "English" is the trap here: it is
// not one of the accepted values.
var ttsLanguageNames = map[string]string{
	"zh": "Chinese",
	"en": "English",
	"de": "German",
	"it": "Italian",
	"pt": "Portuguese",
	"es": "Spanish",
	"ja": "Japanese",
	"ko": "Korean",
	"fr": "French",
	"ru": "Russian",
}

// TTSConfig controls local transport limits. Credentials, model, and voice
// always come from the signed session plan, never from here.
type TTSConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// TTSAdapter implements DashScope's Qwen-TTS-Realtime WebSocket API.
type TTSAdapter struct {
	id              string
	httpClient      *http.Client
	eventBuffer     int
	maxMessageBytes int64
	endpointPolicy  upstream.WebSocketPolicy
}

// NewTTS creates a TTS adapter with bounded provider-event buffering.
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
		return nil, errors.New("alibaba event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("alibaba maximum message bytes must be positive")
	}
	endpointPolicy, err := upstream.NewWebSocketPolicy(InternationalAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
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

// Open dials Qwen-TTS-Realtime and configures the session up front, so a
// rejected voice or output format fails Open rather than arriving later as a
// terminal event partway through an utterance.
func (a *TTSAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindTTS {
		return nil, fmt.Errorf("alibaba tts supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "alibaba" {
		return nil, fmt.Errorf("alibaba tts adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("alibaba tts requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("alibaba tts requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("alibaba tts media: %w", err)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if err := ttsValidateMedia(model, *request.Media); err != nil {
		return nil, err
	}
	// `voice` is a required session field with no server-side default, so an
	// unset voice would be a 400 after the socket is already up.
	voice := strings.TrimSpace(request.Options.Voice)
	if voice == "" {
		return nil, errors.New("alibaba tts requires a voice in request options")
	}
	credential := request.Plan.Route.Credential
	if credential == nil || credential.Kind != protocol.CredentialBearer || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("alibaba tts requires a bearer credential")
	}
	endpoint, err := realtimeEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint, model, ttsSupportedModels, "alibaba tts")
	if err != nil {
		return nil, err
	}

	// One credential channel, same as the STT twin: a managed plan carries a
	// DashScope temporary API key (st-...) and a BYOK plan the customer's
	// permanent key (sk-...), and both are bearer API keys on this header.
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer "+credential.Value)
	// No OpenAI-Beta here. Unlike the ASR endpoint, neither the Qwen-TTS
	// reference header table nor any published Qwen-TTS sample sends it.

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
	stream := &ttsStream{
		conn:      conn,
		ctx:       streamCtx,
		cancel:    cancel,
		events:    make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		finishAck: make(chan struct{}),
	}
	update := ttsSessionUpdate(stream.ids.next(), voice, ttsLanguageType(request.Options.Language), request.Media.SampleRateHz)
	if err := stream.writeJSON(ctx, update); err != nil {
		_ = stream.abort()
		return nil, err
	}
	go stream.readLoop()
	return stream, nil
}

func ttsValidateMedia(model string, media protocol.MediaFormat) error {
	// session.response_format is "pcm": 16-bit little-endian samples. wav, mp3,
	// and opus are also accepted by the vendor but are containers/codecs the
	// canonical MediaFormat does not describe.
	if media.Encoding != "pcm_s16le" {
		return fmt.Errorf("alibaba tts requires pcm_s16le, got %q", media.Encoding)
	}
	if media.Channels != 1 {
		return fmt.Errorf("alibaba tts produces mono audio, got %d channels", media.Channels)
	}
	if _, ok := ttsSupportedSampleRates[media.SampleRateHz]; !ok {
		return fmt.Errorf("alibaba tts does not accept a %d Hz sample rate", media.SampleRateHz)
	}
	// The qwen-tts-realtime series is documented as 24 kHz only. Requesting
	// anything else there is rejected by the vendor, and a silent mismatch
	// would play back at the wrong pitch.
	if isQwenTTSRealtimeSeries(model) && media.SampleRateHz != 24_000 {
		return fmt.Errorf("alibaba tts model %q supports only a 24000 Hz sample rate, got %d", model, media.SampleRateHz)
	}
	return nil
}

// isQwenTTSRealtimeSeries matches the older qwen-tts-realtime family without
// matching the qwen3- generation, whose ids share no prefix with it.
func isQwenTTSRealtimeSeries(model string) bool {
	return strings.HasPrefix(model, "qwen-tts-realtime")
}

// ttsLanguageType maps a portable tag onto DashScope's language_type value.
// An unmapped or absent language falls back to the vendor default rather than
// reaching the wire as a code the endpoint does not accept.
func ttsLanguageType(language string) string {
	if name, ok := ttsLanguageNames[baseLanguageTag(language)]; ok {
		return name
	}
	return ttsDefaultLanguageType
}

// ttsSessionUpdate builds the one configuration frame.
//
// `mode` is server_commit, the vendor's recommended default: the server
// decides when to synthesize buffered text, so audio starts flowing during
// AppendText instead of waiting for the utterance to close. Committing is
// still possible in that mode and means "synthesize everything buffered now",
// which is exactly what CommitText needs.
//
// speech_rate, volume, pitch_rate, bit_rate, instructions, and
// optimize_instructions are all omitted. Each is optional with a documented
// default, several are unsupported on parts of the lineup, and nothing in the
// portable request options maps onto them.
func ttsSessionUpdate(eventID, voice, languageType string, sampleRateHz int) ttsSessionUpdateFrame {
	return ttsSessionUpdateFrame{
		EventID: eventID,
		Type:    "session.update",
		Session: ttsSessionConfig{
			Voice:          voice,
			Mode:           "server_commit",
			LanguageType:   languageType,
			ResponseFormat: "pcm",
			SampleRate:     sampleRateHz,
		},
	}
}

type ttsSessionConfig struct {
	Voice          string `json:"voice"`
	Mode           string `json:"mode"`
	LanguageType   string `json:"language_type"`
	ResponseFormat string `json:"response_format"`
	SampleRate     int    `json:"sample_rate"`
}

type ttsSessionUpdateFrame struct {
	EventID string           `json:"event_id"`
	Type    string           `json:"type"`
	Session ttsSessionConfig `json:"session"`
}

type ttsTextAppendFrame struct {
	EventID string `json:"event_id"`
	Type    string `json:"type"`
	Text    string `json:"text"`
}

type ttsControlFrame struct {
	EventID string `json:"event_id"`
	Type    string `json:"type"`
}

type ttsStream struct {
	conn      *websocket.Conn
	ctx       context.Context
	cancel    context.CancelFunc
	events    chan runtimepkg.ProviderEvent
	ids       eventIDs
	finishAck chan struct{}

	writeMu      sync.Mutex
	finishOnce   sync.Once
	gracefulOnce sync.Once
	abortOnce    sync.Once
	ackOnce      sync.Once
	finishErr    error
	closeErr     error
	closed       atomic.Bool
	textClosed   atomic.Bool

	stateMu sync.Mutex
	// currentResponseID and audioStarted are the per-response bookkeeping that
	// decides when audio.started fires. suppressResponseID is the response a
	// Cancel disowned, whose remaining frames must never reach the caller.
	currentResponseID  string
	suppressResponseID string
	audioStarted       bool
	sessionID          string
}

func (s *ttsStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

// WriteAudio and CommitAudio are inbound-audio operations. A TTS session never
// carries them, so they are reported as unsupported rather than silently
// discarding a caller's buffer.
func (s *ttsStream) WriteAudio(context.Context, []byte) error {
	return runtimepkg.ErrUnsupportedOperation
}

func (s *ttsStream) CommitAudio(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// AppendText streams one fragment into the session buffer. In server_commit
// mode the server begins synthesizing on its own schedule, so no local
// buffering is needed and audio starts before the utterance is complete.
func (s *ttsStream) AppendText(ctx context.Context, text string) error {
	if strings.TrimSpace(text) == "" {
		return errors.New("alibaba tts text is empty")
	}
	if s.textClosed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	// A new fragment starts a new utterance, so a previous barge-in stops
	// applying. Without this, the suppression from an earlier Cancel would
	// swallow audio the caller did ask for.
	s.clearSuppression()
	return s.writeJSON(ctx, ttsTextAppendFrame{
		EventID: s.ids.next(),
		Type:    "input_text_buffer.append",
		Text:    text,
	})
}

// CommitText flushes the buffer. In server_commit mode this synthesizes
// everything buffered immediately; the session then resumes server_commit,
// so one socket serves many utterances.
func (s *ttsStream) CommitText(ctx context.Context) error {
	if s.textClosed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	return s.writeJSON(ctx, ttsControlFrame{
		EventID: s.ids.next(),
		Type:    "input_text_buffer.commit",
	})
}

// Cancel implements barge-in. input_text_buffer.clear is the only interrupt
// the protocol defines and it discards buffered text, not audio the server has
// already produced, so cancellation is two-sided: clear the buffer upstream
// and drop the in-flight response's remaining frames locally. No audio.done is
// emitted for a cancelled utterance, because reporting completion would tell
// the caller the barge-in played all the way through.
func (s *ttsStream) Cancel(ctx context.Context) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	s.stateMu.Lock()
	// An empty currentResponseID means nothing has been generated yet, so
	// there is nothing to withhold and no later response gets suppressed.
	s.suppressResponseID = s.currentResponseID
	s.stateMu.Unlock()
	return s.writeJSON(ctx, ttsControlFrame{
		EventID: s.ids.next(),
		Type:    "input_text_buffer.clear",
	})
}

// Abort immediately tears down the socket after a terminal runtime failure.
func (s *ttsStream) Abort(context.Context) error { return s.abort() }

// Close signals end of input and waits for session.finished, because the
// vendor flushes the tail of the audio between the two. Dropping the socket
// after session.finish would truncate the last utterance.
func (s *ttsStream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		if err := s.finishSession(ctx); err != nil && !errors.Is(err, runtimepkg.ErrSessionClosed) {
			s.closeErr = err
		}
		if s.closeErr == nil {
			select {
			case <-s.finishAck:
			case <-ctx.Done():
				s.closeErr = ctx.Err()
			}
		}
		s.closed.Store(true)
		if s.closeErr != nil {
			_ = s.abort()
			return
		}
		if err := s.conn.Close(websocket.StatusNormalClosure, ""); err != nil {
			s.closeErr = err
		}
	})
	return s.closeErr
}

// finishSession writes session.finish at most once and refuses further text.
func (s *ttsStream) finishSession(ctx context.Context) error {
	s.finishOnce.Do(func() {
		s.textClosed.Store(true)
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

func (s *ttsStream) abort() error {
	s.abortOnce.Do(func() {
		s.closed.Store(true)
		s.textClosed.Store(true)
		s.cancel()
		if err := s.conn.CloseNow(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
		s.releaseFinishAck()
	})
	return s.closeErr
}

// releaseFinishAck unblocks Close. It fires on session.finished and again from
// the read loop's exit, so a socket that dies without the acknowledgement does
// not strand a caller inside Close.
func (s *ttsStream) releaseFinishAck() {
	s.ackOnce.Do(func() { close(s.finishAck) })
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

func (s *ttsStream) readLoop() {
	defer func() {
		s.cancel()
		s.releaseFinishAck()
		close(s.events)
	}()
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
		// Audio rides base64 inside response.audio.delta on this endpoint; the
		// binary channel is unused, unlike the /api-ws/v1/inference protocol.
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

func (s *ttsStream) handleMessage(payload []byte) (bool, error) {
	var message ttsInbound
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
			s.setSessionID(message.Session.ID)
		}
		return false, s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventUsageObserved,
			Data:       marshalData(map[string]any{"provider_request_id": s.providerRequestID()}),
			Extensions: extension(raw),
		})
	case "session.updated",
		"input_text_buffer.committed",
		"input_text_buffer.cleared",
		"response.output_item.added",
		"response.content_part.added",
		"response.content_part.done",
		"response.output_item.done":
		// Documented lifecycle acknowledgements with no caller-visible state.
		// Naming them keeps the warning branch meaningful for frames that are
		// genuinely unknown.
		return false, nil
	case "response.created":
		s.beginResponse(message.Response.ID)
		return false, nil
	case "response.audio.delta":
		return false, s.handleAudioDelta(message, raw)
	case "response.audio.done":
		if s.suppressed(message.ResponseID) {
			return false, nil
		}
		return false, s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventAudioDone,
			Data:       marshalData(map[string]any{"provider_request_id": s.providerRequestID()}),
			Extensions: extension(raw),
		})
	case "response.done":
		// Carries the billed character count, which is the only usage signal
		// this endpoint reports.
		data := map[string]any{"provider_request_id": s.providerRequestID()}
		if message.Response.Usage != nil {
			data["characters"] = message.Response.Usage.Characters
		}
		s.endResponse(message.Response.ID)
		return false, s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventUsageObserved,
			Data:       marshalData(data),
			Extensions: extension(raw),
		})
	case "error":
		return false, realtimeProviderError("Alibaba reported a streaming error", message.Error, raw)
	case "session.finished":
		// All responses generated; the vendor closes after this.
		return true, nil
	default:
		return false, s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventWarning,
			Data:       marshalData(map[string]any{"message": "ignored Alibaba message type", "provider_type": message.Type}),
			Extensions: extension(raw),
		})
	}
}

func (s *ttsStream) handleAudioDelta(message ttsInbound, raw json.RawMessage) error {
	if message.Delta == "" {
		return nil
	}
	if s.suppressed(message.ResponseID) {
		return nil
	}
	audio, err := base64.StdEncoding.DecodeString(message.Delta)
	if err != nil {
		return &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "Alibaba sent invalid audio data",
			Retryable: true,
			Cause:     err,
		}
	}
	if len(audio) == 0 {
		return nil
	}
	if s.markAudioStarted() {
		if err := s.emit(runtimepkg.ProviderEvent{
			Type: protocol.EventAudioStarted,
			Data: marshalData(map[string]any{"provider_request_id": s.providerRequestID()}),
		}); err != nil {
			return err
		}
	}
	return s.emit(runtimepkg.ProviderEvent{
		Type:       protocol.EventAudioFrame,
		Data:       marshalData(map[string]any{"provider_request_id": s.providerRequestID()}),
		Extensions: extension(raw),
		Audio:      audio,
	})
}

func (s *ttsStream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *ttsStream) setSessionID(value string) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.sessionID = value
}

// providerRequestID prefers the response id, which identifies one synthesis,
// and falls back to the session id before any response exists.
func (s *ttsStream) providerRequestID() string {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.currentResponseID != "" {
		return s.currentResponseID
	}
	return s.sessionID
}

func (s *ttsStream) beginResponse(id string) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.currentResponseID = id
	s.audioStarted = false
}

func (s *ttsStream) endResponse(id string) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.suppressResponseID != "" && s.suppressResponseID == id {
		s.suppressResponseID = ""
	}
	s.audioStarted = false
}

func (s *ttsStream) clearSuppression() {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.suppressResponseID = ""
}

// suppressed reports whether a frame belongs to a cancelled response. Frames
// carry response_id; an empty suppression target matches nothing, which is
// what a Cancel with no synthesis in flight should do.
func (s *ttsStream) suppressed(responseID string) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.suppressResponseID != "" && s.suppressResponseID == responseID
}

func (s *ttsStream) markAudioStarted() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.audioStarted {
		return false
	}
	s.audioStarted = true
	return true
}

type ttsUsage struct {
	Characters int64 `json:"characters"`
}

type ttsResponse struct {
	ID    string    `json:"id"`
	Usage *ttsUsage `json:"usage"`
}

// ttsInbound mirrors the documented server frames. Response is a value type so
// a frame without one decodes to the zero value rather than needing a nil
// check at every use.
type ttsInbound struct {
	Type       string `json:"type"`
	EventID    string `json:"event_id"`
	ResponseID string `json:"response_id"`
	ItemID     string `json:"item_id"`
	Delta      string `json:"delta"`
	Response   ttsResponse
	Session    *struct {
		ID string `json:"id"`
	} `json:"session"`
	Error *realtimeError `json:"error"`
}

// UnmarshalJSON keeps Response a value while still tolerating `"response":
// null`, which the vendor sends on frames that have no response context.
func (m *ttsInbound) UnmarshalJSON(data []byte) error {
	type alias ttsInbound
	var raw struct {
		alias
		Response *ttsResponse `json:"response"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*m = ttsInbound(raw.alias)
	if raw.Response != nil {
		m.Response = *raw.Response
	}
	return nil
}
