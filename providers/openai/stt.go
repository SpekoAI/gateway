package openai

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
	// STTAdapterID is the identifier returned by an OpenAI STT session plan.
	STTAdapterID = "openai.stt.v1"
	// sttExtensionID namespaces raw Realtime payloads carried on canonical events.
	sttExtensionID = "openai.com/realtime/v1"

	// officialAPIHost serves every surface this package speaks: the Realtime
	// WebSocket, /v1/audio/speech, and the client-secret mint endpoint. The
	// OpenAPI document declares a single server, `https://api.openai.com/v1`.
	officialAPIHost = "api.openai.com"

	// sttRealtimePath is the Realtime socket. Transcription sessions require
	// `intent=transcription` on the handshake URL in addition to
	// `session.type="transcription"` in the update body. Without the intent the
	// service creates a regular realtime session and rejects the update; observed
	// live as `missing_model` followed by "Passing a transcription session update
	// to a realtime session is not allowed" when a regular model is supplied.
	sttRealtimePath = "/v1/realtime"

	// DefaultSTTModel is OpenAI's recommended realtime transcription model.
	// CONFIRMED raw, Realtime transcription guide: "Start with
	// `gpt-live-transcribe`."
	DefaultSTTModel = "gpt-live-transcribe"

	// sttLiveModel is the one model whose language hint uses the PLURAL field.
	// It is referenced by name in several places, so it is a constant rather
	// than a repeated string literal that a typo could desynchronize.
	sttLiveModel = "gpt-live-transcribe"

	// sttSampleRateHz is the ONLY rate the Realtime API accepts for PCM input.
	// CONFIRMED raw, RealtimeAudioFormats: the PCM variant documents
	// "Only a 24kHz sample rate is supported" and its `rate` property is an
	// enum whose single member is 24000.
	//
	// The TypeScript provider upsamples sub-24 kHz audio and then declares
	// `rate: max(inputRate, 24000)`. That is wrong above 24 kHz: a 48 kHz stream
	// declares `rate: 48000`, which is not in the enum. This adapter refuses a
	// rate it cannot declare instead of resampling, matching how the ElevenLabs
	// STT adapter handles an unsupported rate. Resampling is a runtime concern,
	// not an adapter one.
	sttSampleRateHz = 24_000

	// sttPCMFrameBytes caps one input_audio_buffer.append. 24000 B is 0.5 s of
	// 24 kHz s16le mono and is sample-aligned, so a caller handing over a whole
	// utterance in one write does not produce an oversized WebSocket message.
	sttPCMFrameBytes = 24_000
)

// sttSupportedModels is the `AudioTranscription.model` enum, transcribed
// verbatim from OpenAI's OpenAPI document. Keeping it as an explicit set makes
// a typo in a plan fail at Open rather than as a mid-session `error` frame.
//
// `whisper-1` and `gpt-4o-transcribe-diarize` are in the vendor's enum for this
// field, so they are accepted here, but the platform's TypeScript provider
// deliberately routes both to the buffered file API instead and neither has
// been observed on this socket. See the report.
var sttSupportedModels = map[string]struct{}{
	"whisper-1":                         {},
	"gpt-transcribe":                    {},
	"gpt-live-transcribe":               {},
	"gpt-4o-mini-transcribe":            {},
	"gpt-4o-mini-transcribe-2025-12-15": {},
	"gpt-4o-transcribe":                 {},
	"gpt-4o-transcribe-diarize":         {},
	"gpt-realtime-whisper":              {},
}

// STTConfig controls local transport limits. Credentials and provider selection
// always come from the signed session plan, never from this configuration.
type STTConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// STTAdapter implements OpenAI's Realtime transcription session over WebSocket.
//
// The Realtime socket is the only OpenAI surface with a genuinely INCREMENTAL
// transcription input. `/v1/audio/transcriptions` streams its RESPONSE (SSE
// `transcript.text.delta`) but its request is `multipart/form-data` carrying a
// complete file, so it cannot start before the caller's audio ends. A live
// gateway session needs both halves incremental, so this adapter is WebSocket
// only and refuses an HTTP route.
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
		return nil, errors.New("openai event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("openai maximum message bytes must be positive")
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

// Open dials the Realtime socket and configures a transcription session.
//
// The session configuration is sent before Open returns so that a rejected
// model, language, or format surfaces as an open failure the runtime can fail
// over, rather than as an `error` frame the caller has to correlate later.
func (a *STTAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindSTT {
		return nil, fmt.Errorf("openai stt supports stt sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "openai" {
		return nil, fmt.Errorf("openai adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("openai stt requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("openai stt requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("openai stt media: %w", err)
	}
	if err := sttValidateMedia(*request.Media); err != nil {
		return nil, err
	}
	model, err := sttValidateModel(request.Plan.Route.Model)
	if err != nil {
		return nil, err
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("openai stt requires a bearer credential")
	}
	endpoint, err := sttRealtimeEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}

	// ONE credential channel, deliberately. OpenAI documents `Authorization:
	// Bearer <token>` for a server-to-server Realtime WebSocket, and the same
	// header carrying an ephemeral `ek_...` secret on the WebRTC call resource.
	// The alternative channel — the `openai-insecure-api-key.<token>` WebSocket
	// SUBPROTOCOL — exists only because a browser cannot set headers; OpenAI
	// presents it as the browser workaround, not as the ephemeral-credential
	// mechanism. Nothing in OpenAI's documentation distinguishes managed from
	// BYOK here, so there is no split to implement: inventing one would be a
	// wire bug. Contrast the ElevenLabs STT adapter, where the vendor really
	// does accept a minted token ONLY as a query parameter. A relay plan
	// changes nothing here either: it carries the relay connector's permanent
	// OpenAI key, which belongs in this same header and never in a URL — only
	// the accepted credential-kind label widens on that route (see
	// acceptableCredentialKind).
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer "+credential.Value)

	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{
		HTTPClient: sttHTTPClient(a.httpClient),
		HTTPHeader: headers,
	})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		return nil, &runtimepkg.ProviderError{
			Code:           sttDialErrorCode(status),
			Message:        "OpenAI Realtime connection could not be established",
			Retryable:      status == 0 || status == http.StatusTooManyRequests || status >= 500,
			ProviderStatus: status,
			Cause:          err,
		}
	}
	conn.SetReadLimit(a.maxMessageBytes)

	if err := sttWriteSessionUpdate(ctx, conn, model, request.Options.Language); err != nil {
		_ = conn.CloseNow()
		return nil, err
	}

	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &sttStream{
		conn:   conn,
		ctx:    streamCtx,
		cancel: cancel,
		events: make(chan runtimepkg.ProviderEvent, a.eventBuffer),
	}
	go stream.readLoop()
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
// connector that synthesizes the plan and drives this adapter directly — no
// Engine, no SessionPlan.Validate — labels the same permanent OpenAI key
// bearer. Both spellings carry a permanent key, and both travel in the one
// Authorization: Bearer header this package sends everywhere, so nothing else
// on the relay arm changes. Shared by the STT and TTS adapters.
func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

func sttRealtimeEndpoint(policy upstream.WebSocketPolicy, rawEndpoint string) (string, error) {
	endpoint, err := policy.Parse(rawEndpoint)
	if err != nil {
		return "", fmt.Errorf("openai stt endpoint: %w", err)
	}
	if endpoint.Path != sttRealtimePath {
		return "", fmt.Errorf("openai stt endpoint path must be %s, got %q", sttRealtimePath, endpoint.Path)
	}
	query := endpoint.Query()
	query.Set("intent", "transcription")
	endpoint.RawQuery = query.Encode()
	return endpoint.String(), nil
}

func sttValidateModel(model string) (string, error) {
	model = strings.TrimSpace(model)
	if model == "" || model == "auto" {
		return "", errors.New("openai stt requires a concrete model in the session plan")
	}
	if _, ok := sttSupportedModels[model]; !ok {
		return "", fmt.Errorf("openai stt does not support model %q", model)
	}
	return model, nil
}

// sttValidateMedia enforces the intersection of the canonical protocol media
// format and the Realtime API's PCM input format. `audio/pcm` is documented as
// 16-bit little-endian mono, which is exactly pcm_s16le at one channel.
func sttValidateMedia(media protocol.MediaFormat) error {
	if media.Encoding != "pcm_s16le" || media.Channels != 1 {
		return fmt.Errorf("openai stt requires mono pcm_s16le input, got %s/%d channels", media.Encoding, media.Channels)
	}
	if media.SampleRateHz != sttSampleRateHz {
		return fmt.Errorf("openai stt requires a %d Hz sample rate, got %d", sttSampleRateHz, media.SampleRateHz)
	}
	return nil
}

// sttWriteSessionUpdate sends the one configuration message a transcription
// session needs. Everything about the session lives in this body: there is no
// query-parameter configuration channel on the Realtime socket.
func sttWriteSessionUpdate(ctx context.Context, conn *websocket.Conn, model, language string) error {
	transcription := sttTranscriptionConfig{Model: model}
	if normalized := sttLanguageTag(language); normalized != "" {
		if model == sttLiveModel {
			// CONFIRMED raw, Realtime transcription guide: "`gpt-live-transcribe`
			// uses `languages` instead of the singular `language` field. Don't
			// send both." Sending `language` to this model pins nothing, because
			// the API ignores the field it does not read for that model.
			transcription.Languages = []string{normalized}
		} else {
			// Every other transcription model documents the singular ISO-639-1
			// `language`, so a regional tag is reduced to its primary subtag.
			transcription.Language = sttPrimarySubtag(normalized)
		}
	}
	update := sttSessionUpdate{
		Type: "session.update",
		Session: sttSessionConfig{
			Type: "transcription",
			Audio: sttAudioConfig{
				Input: sttAudioInputConfig{
					Format:        sttAudioFormatConfig{Type: "audio/pcm", Rate: sttSampleRateHz},
					Transcription: transcription,
					// turn_detection is explicitly null, never omitted. The
					// framework owns turn detection, and the raw transcription
					// guide's own example disables it so the caller commits each
					// turn. It also sidesteps a vendor asymmetry the platform hit
					// in production: `gpt-live-transcribe` REJECTS a populated
					// turn_detection object outright ("Turn detection is not
					// supported for this transcription model"), while the gpt-4o
					// models accept server_vad. Null is valid for both.
					TurnDetection: nil,
				},
			},
		},
	}
	payload, err := json.Marshal(update)
	if err != nil {
		return err
	}
	if err := conn.Write(ctx, websocket.MessageText, payload); err != nil {
		return &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "OpenAI Realtime session could not be configured",
			Retryable: true,
			Cause:     err,
		}
	}
	return nil
}

// sttLanguageTag lowercases a caller tag and preserves the regional forms the
// Realtime API accepts. CONFIRMED raw, Realtime transcription guide: supported
// formats are ISO-639-1 (`en`), selected ISO-639-3 (`eng`, `yue`, `cmn`), and
// the regional `zh` locales `zh-cn`, `zh-tw`, `zh-hk`. Blindly reducing every
// tag to its primary subtag would collapse zh-tw to `zh` and silently change
// which Chinese variant is transcribed, so `zh-*` is passed through whole.
func sttLanguageTag(language string) string {
	lowered := strings.ToLower(strings.TrimSpace(language))
	if lowered == "" {
		return ""
	}
	normalized := strings.ReplaceAll(lowered, "_", "-")
	if strings.HasPrefix(normalized, "zh-") {
		return normalized
	}
	return normalized
}

func sttPrimarySubtag(language string) string {
	if index := strings.IndexByte(language, '-'); index > 0 {
		return language[:index]
	}
	return language
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
	// committed records that the stream-end commit has been sent, so the benign
	// empty-buffer error it can provoke is swallowed rather than failing a turn.
	committed atomic.Bool

	// partial is the running text of the CURRENT turn, owned solely by readLoop.
	partial   string
	sessionID string
}

func (s *sttStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

// WriteAudio forwards PCM as base64 inside `input_audio_buffer.append`. The
// Realtime socket has no binary input frame — audio is a base64 string field on
// a JSON event — so this is a text write even though the payload is audio.
func (s *sttStream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("openai stt audio is empty")
	}
	for offset := 0; offset < len(audio); offset += sttPCMFrameBytes {
		end := offset + sttPCMFrameBytes
		if end > len(audio) {
			end = len(audio)
		}
		frame := sttAppendEvent{
			Type:  "input_audio_buffer.append",
			Audio: base64.StdEncoding.EncodeToString(audio[offset:end]),
		}
		if err := s.writeJSON(ctx, frame); err != nil {
			return err
		}
	}
	return nil
}

// CommitAudio closes the current turn. With turn_detection disabled this is the
// ONLY thing that makes the model emit an utterance final, so the runtime's
// end-of-speech signal has to reach the provider through this method.
func (s *sttStream) CommitAudio(ctx context.Context) error {
	return s.writeJSON(ctx, sttControlEvent{Type: "input_audio_buffer.commit"})
}

func (s *sttStream) AppendText(context.Context, string) error {
	return runtimepkg.ErrUnsupportedOperation
}

func (s *sttStream) CommitText(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// Cancel tears the session down. `input_audio_buffer.clear` discards pending
// audio but leaves the session running, so it is not a session cancel; the
// Realtime protocol has no cancel event for a transcription session, exactly as
// with the Deepgram and ElevenLabs sockets.
func (s *sttStream) Cancel(ctx context.Context) error {
	if err := s.Close(ctx); err != nil {
		return err
	}
	return s.abort()
}

// Abort immediately tears down the socket after a terminal runtime failure.
func (s *sttStream) Abort(context.Context) error { return s.abort() }

// Close commits any buffered audio and then stops accepting writes. Committing
// first is what makes the last turn arrive: with turn detection off, dropping
// the socket without a commit discards whatever audio was still buffered. The
// socket stays open so that final can land; the runtime aborts afterwards.
func (s *sttStream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		s.committed.Store(true)
		if err := s.CommitAudio(ctx); err != nil && !errors.Is(err, runtimepkg.ErrSessionClosed) {
			s.closeErr = err
		}
		s.closed.Store(true)
		if s.closeErr != nil {
			_ = s.abort()
		}
	})
	return s.closeErr
}

func (s *sttStream) abort() error {
	s.abortOnce.Do(func() {
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
			Message:   "OpenAI Realtime write failed",
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
			if !s.closed.Load() && !sttIsNormalClose(err) && s.ctx.Err() == nil {
				s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{
					Code:      "provider_unavailable",
					Message:   "OpenAI Realtime read failed",
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

func (s *sttStream) handleMessage(payload []byte) error {
	var message sttInboundMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "OpenAI Realtime sent malformed JSON",
			Retryable: true,
			Cause:     err,
		}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))
	switch message.Type {
	case "session.created", "session.updated":
		// The protocol-level ack. Authentication and configuration failures can
		// arrive as frames on an ALREADY-OPEN socket, so session identity is
		// captured here rather than at dial.
		if message.Session != nil && message.Session.ID != "" {
			s.sessionID = message.Session.ID
		}
		return s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventUsageObserved,
			Data:       marshalData(map[string]any{"provider_request_id": s.sessionID}),
			Extensions: sttExtension(raw),
		})
	case "input_audio_buffer.speech_started":
		return s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventSpeechStarted,
			Data:       marshalData(map[string]any{"audio_start_ms": message.AudioStartMS, "item_id": message.ItemID}),
			Extensions: sttExtension(raw),
		})
	case "input_audio_buffer.speech_stopped":
		return s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventSpeechEnded,
			Data:       marshalData(map[string]any{"audio_end_ms": message.AudioEndMS, "item_id": message.ItemID}),
			Extensions: sttExtension(raw),
		})
	case "conversation.item.input_audio_transcription.delta":
		// OpenAI Realtime sends INCREMENTAL deltas ("Hello," then " how" then
		// " are you"), but every canonical transcript.delta in this protocol is
		// CUMULATIVE and consumers REPLACE the displayed partial on each one.
		// Forwarding a raw delta therefore renders as a single word and throws
		// the utterance-so-far away on every frame. Accumulate here.
		//
		// Deltas carry their own leading spaces, so the raw text is concatenated
		// untrimmed and only the emitted value is trimmed; trimming each delta
		// first would weld the words together ("Hellohowareyou").
		if message.Delta == "" {
			return nil
		}
		s.partial += message.Delta
		text := strings.TrimSpace(s.partial)
		if text == "" {
			return nil
		}
		return s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventTranscriptDelta,
			Data:       sttTranscriptData(text, false, message),
			Extensions: sttExtension(raw),
		})
	case "conversation.item.input_audio_transcription.completed":
		// The turn is closed: the next delta starts a fresh utterance. Without
		// this reset a multi-turn session keeps prepending every earlier turn to
		// the running partial, because the socket outlives mid-session finals.
		s.partial = ""
		text := strings.TrimSpace(message.Transcript)
		// An empty transcript is still a completed turn. Silence legitimately
		// produces one, and suppressing it leaves batch callers waiting forever
		// for the final event that closes their response.
		return s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventTranscriptFinal,
			Data:       sttTranscriptData(text, true, message),
			Extensions: sttExtension(raw),
		})
	case "conversation.item.input_audio_transcription.failed":
		// A per-item transcription failure. OpenAI separates it from the generic
		// `error` event precisely so a client can attribute it to one item, so
		// the item id is preserved in the message.
		return sttItemFailureError(message, raw)
	case "error":
		// A stream-end commit with nothing buffered errors benignly: the buffer
		// was already empty. Swallowing it only after our own commit keeps a real
		// mid-stream error visible.
		if s.committed.Load() && sttIsEmptyBufferError(message.Error) {
			return nil
		}
		return sttEventError(message.Error, raw)
	case "input_audio_buffer.committed", "input_audio_buffer.cleared",
		"conversation.item.created", "conversation.item.added", "conversation.item.done":
		// Documented acknowledgements with no canonical counterpart. They are
		// listed explicitly so they do not produce a warning per turn.
		return nil
	default:
		return s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventWarning,
			Data:       marshalData(map[string]any{"message": "ignored OpenAI Realtime event type", "provider_type": message.Type}),
			Extensions: sttExtension(raw),
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

func sttIsNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}

func sttDialErrorCode(status int) string {
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return "authentication_failed"
	case status == http.StatusTooManyRequests:
		return "provider_rate_limited"
	case status >= 500 || status == 0:
		return "provider_unavailable"
	case status >= 400:
		return "invalid_request"
	default:
		return "provider_unavailable"
	}
}

// sttErrorCode keeps OpenAI's own distinction between a dead key, an exhausted
// balance, and a throttle. Collapsing them into one code would make a credential
// problem indistinguishable from a billing one, and only one of the three is
// worth retrying against the same account.
//
// `code` is the more specific of the two fields and is checked first; `type`
// ("invalid_request_error", "server_error") is the coarse fallback.
func sttErrorCode(errorType, code string) string {
	switch code {
	case "invalid_api_key", "invalid_authentication":
		return "authentication_failed"
	case "insufficient_quota":
		return "provider_quota_exceeded"
	case "rate_limit_exceeded":
		return "provider_rate_limited"
	}
	switch errorType {
	case "invalid_request_error":
		return "invalid_request"
	case "authentication_error":
		return "authentication_failed"
	case "rate_limit_error":
		return "provider_rate_limited"
	case "insufficient_quota":
		return "provider_quota_exceeded"
	default:
		return "provider_unavailable"
	}
}

func sttEventError(detail *sttErrorDetail, raw json.RawMessage) *runtimepkg.ProviderError {
	if detail == nil {
		return &runtimepkg.ProviderError{
			Code:       "provider_unavailable",
			Message:    "OpenAI Realtime reported an error without detail",
			Retryable:  true,
			Extensions: sttExtension(raw),
		}
	}
	code := sttErrorCode(detail.Type, detail.Code)
	message := "OpenAI Realtime reported an error"
	if strings.TrimSpace(detail.Message) != "" {
		message += ": " + detail.Message
	}
	return &runtimepkg.ProviderError{
		Code:       code,
		Message:    message,
		Retryable:  code == "provider_rate_limited" || code == "provider_unavailable",
		Extensions: sttExtension(raw),
	}
}

func sttItemFailureError(message sttInboundMessage, raw json.RawMessage) *runtimepkg.ProviderError {
	providerErr := sttEventError(message.Error, raw)
	providerErr.Message = "OpenAI Realtime failed to transcribe an input item"
	if message.Error != nil && strings.TrimSpace(message.Error.Message) != "" {
		providerErr.Message += ": " + message.Error.Message
	}
	return providerErr
}

// sttIsEmptyBufferError recognizes the commit-with-nothing-buffered rejection.
// The documented code is `input_audio_buffer_commit_empty`; the message text is
// also matched because the same condition has been observed carrying only a
// human-readable message.
func sttIsEmptyBufferError(detail *sttErrorDetail) bool {
	if detail == nil {
		return false
	}
	if detail.Code == "input_audio_buffer_commit_empty" {
		return true
	}
	lowered := strings.ToLower(detail.Message)
	return strings.Contains(lowered, "buffer") && (strings.Contains(lowered, "empty") || strings.Contains(lowered, "too small") || strings.Contains(lowered, "no audio"))
}

func sttExtension(raw json.RawMessage) map[string]json.RawMessage {
	return map[string]json.RawMessage{sttExtensionID: append(json.RawMessage(nil), raw...)}
}

func sttTranscriptData(text string, final bool, message sttInboundMessage) json.RawMessage {
	data := map[string]any{
		"text":         text,
		"is_final":     final,
		"speech_final": final,
		"item_id":      message.ItemID,
	}
	if len(message.Languages) > 0 {
		// Only `gpt-transcribe` returns detected languages; an empty array means
		// no reliable prediction, which is why the field is emitted only when
		// populated rather than always.
		codes := make([]string, 0, len(message.Languages))
		for _, language := range message.Languages {
			if language.Code != "" {
				codes = append(codes, language.Code)
			}
		}
		if len(codes) > 0 {
			data["languages"] = codes
		}
	}
	if len(message.Usage) > 0 {
		data["usage"] = message.Usage
	}
	return marshalData(data)
}

func marshalData(value any) json.RawMessage {
	payload, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{"code":"internal"}`)
	}
	return payload
}

// The outbound event structs below mirror OpenAI's Realtime client events. The
// JSON tags are the wire names transcribed from the OpenAPI document; nothing
// here is derived from a local constant, so a rename breaks the tests.

type sttSessionUpdate struct {
	Type    string           `json:"type"`
	Session sttSessionConfig `json:"session"`
}

type sttSessionConfig struct {
	Type  string         `json:"type"`
	Audio sttAudioConfig `json:"audio"`
}

type sttAudioConfig struct {
	Input sttAudioInputConfig `json:"input"`
}

type sttAudioInputConfig struct {
	Format        sttAudioFormatConfig   `json:"format"`
	Transcription sttTranscriptionConfig `json:"transcription"`
	// TurnDetection has no omitempty on purpose: `null` is a meaningful value
	// that turns vendor turn detection off, and omitting the key leaves the
	// session default in place instead.
	TurnDetection *sttTurnDetectionConfig `json:"turn_detection"`
}

type sttAudioFormatConfig struct {
	Type string `json:"type"`
	Rate int    `json:"rate"`
}

type sttTurnDetectionConfig struct {
	Type string `json:"type"`
}

type sttTranscriptionConfig struct {
	Model     string   `json:"model"`
	Language  string   `json:"language,omitempty"`
	Languages []string `json:"languages,omitempty"`
	Prompt    string   `json:"prompt,omitempty"`
}

type sttAppendEvent struct {
	Type  string `json:"type"`
	Audio string `json:"audio"`
}

type sttControlEvent struct {
	Type string `json:"type"`
}

// sttInboundMessage is the union of the Realtime server events this adapter
// reads. Field names come from the OpenAPI document's server-event schemas.
type sttInboundMessage struct {
	Type         string          `json:"type"`
	EventID      string          `json:"event_id"`
	ItemID       string          `json:"item_id"`
	ContentIndex int             `json:"content_index"`
	Delta        string          `json:"delta"`
	Transcript   string          `json:"transcript"`
	AudioStartMS int64           `json:"audio_start_ms"`
	AudioEndMS   int64           `json:"audio_end_ms"`
	Languages    []sttLanguage   `json:"languages"`
	Usage        json.RawMessage `json:"usage"`
	Session      *sttSessionRef  `json:"session"`
	Error        *sttErrorDetail `json:"error"`
}

type sttSessionRef struct {
	ID string `json:"id"`
}

type sttLanguage struct {
	Code string `json:"code"`
}

type sttErrorDetail struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
	Param   string `json:"param"`
	EventID string `json:"event_id"`
}
