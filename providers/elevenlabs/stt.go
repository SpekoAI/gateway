package elevenlabs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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
	// STTAdapterID is the identifier returned by an ElevenLabs STT session plan.
	STTAdapterID = "elevenlabs.stt.v1"
	// sttEndpointPath is the realtime Scribe socket. The batch /v1/speech-to-text
	// endpoint is a DIFFERENT model family: it takes `scribe_v1`/`scribe_v2`, and
	// only `scribe_v2_realtime` streams over this path.
	sttEndpointPath = "/v1/speech-to-text/realtime"
	// sttPCMFrameBytes caps one input_audio_chunk. 16000 B is 0.5 s of 16 kHz
	// s16le mono and is sample-aligned, so a caller handing a whole utterance as
	// one write does not produce an oversized WebSocket message.
	sttPCMFrameBytes = 16_000
)

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

// STTAdapter implements ElevenLabs' realtime Scribe WebSocket API.
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
		return nil, errors.New("elevenlabs event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("elevenlabs maximum message bytes must be positive")
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

// Open opens a provider-direct STT stream against realtime Scribe.
func (a *STTAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindSTT {
		return nil, fmt.Errorf("elevenlabs stt supports stt sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "elevenlabs" {
		return nil, fmt.Errorf("elevenlabs adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("elevenlabs requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("elevenlabs stt requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("elevenlabs stt media: %w", err)
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("elevenlabs stt requires a bearer credential")
	}
	endpoint, err := realtimeEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint, request.Plan.Route.Model, request.Options, *request.Media)
	if err != nil {
		return nil, err
	}
	// Two different credentials, two different channels. A BYOK session carries the
	// customer's permanent API key, which belongs in `xi-api-key`. A managed session
	// carries a single-use token minted by the control plane, and the vendor accepts
	// a token ONLY as the `token` query parameter — sending it as the header fails
	// authentication. Same split Cartesia already makes with its access token.
	// A relay plan is the exception inside managed: it is managed for billing
	// purposes but carries the connector's permanent provider key, so it uses the
	// header channel like BYOK rather than putting a permanent key in the URL.
	headers := make(http.Header)
	if request.Plan.Execution.ProviderRoute == protocol.RouteSpekoRelay {
		headers.Set("xi-api-key", credential.Value)
	} else if request.Plan.Execution.CredentialSource == protocol.CredentialsManaged {
		endpoint, err = sttEndpointWithToken(endpoint, credential.Value)
		if err != nil {
			return nil, err
		}
	} else {
		headers.Set("xi-api-key", credential.Value)
	}
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
			Message:        "ElevenLabs streaming connection could not be established",
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
		rate:   request.Media.SampleRateHz,
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
// Engine, no SessionPlan.Validate — labels the same permanent ElevenLabs key
// bearer. Both spellings carry a permanent key; the header-versus-query
// channel split keys off the route, not the kind, so nothing else on the
// relay arm changes.
func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

func realtimeEndpoint(policy upstream.WebSocketPolicy, rawEndpoint, model string, options protocol.RequestOptions, media protocol.MediaFormat) (string, error) {
	endpoint, err := policy.Parse(rawEndpoint)
	if err != nil {
		return "", fmt.Errorf("elevenlabs stt endpoint: %w", err)
	}
	if endpoint.Path != sttEndpointPath {
		return "", fmt.Errorf("elevenlabs stt endpoint path must be %s, got %q", sttEndpointPath, endpoint.Path)
	}
	if strings.TrimSpace(model) == "" || model == "auto" {
		return "", errors.New("elevenlabs stt requires a concrete model in the session plan")
	}
	if media.Encoding != "pcm_s16le" {
		return "", &runtimepkg.ProviderError{Code: "unsupported_media", Message: fmt.Sprintf("ElevenLabs transcription cannot accept %s audio", media.Encoding), Hint: "Convert the input to mono pcm_s16le at 8000, 16000, 22050, 24000, 44100, or 48000 Hz and try again."}
	}
	if media.Channels != 1 {
		return "", &runtimepkg.ProviderError{Code: "unsupported_media", Message: fmt.Sprintf("ElevenLabs transcription cannot accept %d-channel audio", media.Channels), Hint: "Downmix the input to mono pcm_s16le and try again."}
	}
	query := endpoint.Query()
	query.Set("model_id", model)
	// `audio_format` accepts a FIXED set of tokens. Declaring pcm_16000 for 32 kHz
	// audio — the platform's fallback — does not resample anything: Scribe reads the
	// bytes at the wrong rate and the transcript degrades badly while the session
	// looks healthy. An unsupported rate is refused here instead.
	audioFormat, err := sttAudioFormat(media.SampleRateHz)
	if err != nil {
		return "", err
	}
	query.Set("audio_format", audioFormat)
	// Word timestamps. Without this, a committed segment arrives as
	// `committed_transcript` (text only) and no word timings are available at all.
	// With it, Scribe emits `committed_transcript_with_timestamps` carrying a
	// words array — see handleMessage for why only the timestamped twin is used.
	query.Set("include_timestamps", "true")
	// Vendor-default VAD commit strategy. The framework still owns turn detection;
	// this only decides when Scribe finalizes a segment on its own side.
	query.Set("commit_strategy", "vad")
	if language := strings.TrimSpace(options.Language); language != "" {
		// `language_code`, NOT `language`: the vendor ignores an unknown parameter,
		// so the wrong name silently leaves Scribe auto-detecting instead of pinned.
		// Only the primary subtag is accepted — `es`, not `es-MX`.
		query.Set("language_code", baseLanguageTag(language))
	}
	// Vocabulary biasing: repeated `keyterms`, one per term, matching how the
	// platform's own Scribe adapter spells it on this same realtime socket.
	for _, keyword := range options.STT.GetKeywords() {
		query.Add("keyterms", keyword)
	}
	// The caller's own Scribe settings, already allow-listed by the gateway —
	// today that is vad_silence_threshold_secs, the snappy-vs-patient commit
	// knob whose right value is genuinely per-caller.
	for _, key := range options.STT.ProviderKeys("elevenlabs") {
		query.Set(key, protocol.SttOptionString(options.STT.Provider("elevenlabs")[key]))
	}
	endpoint.RawQuery = query.Encode()
	return endpoint.String(), nil
}

// sttAudioFormat maps a portable sample rate onto one of the vendor's accepted
// `audio_format` tokens. 22050 and 44100 are accepted by ElevenLabs but are not
// portable media rates here, so they are simply unreachable rather than special.
func sttAudioFormat(sampleRateHz int) (string, error) {
	switch sampleRateHz {
	case 8000, 16000, 22050, 24000, 44100, 48000:
		return "pcm_" + strconv.Itoa(sampleRateHz), nil
	default:
		return "", &runtimepkg.ProviderError{Code: "unsupported_media", Message: fmt.Sprintf("ElevenLabs transcription cannot accept a %d Hz sample rate", sampleRateHz), Hint: "Resample the mono pcm_s16le input to 8000, 16000, 22050, 24000, 44100, or 48000 Hz and try again."}
	}
}

// baseLanguageTag reduces a tag to its primary subtag.
func baseLanguageTag(language string) string {
	lowered := strings.ToLower(strings.TrimSpace(language))
	if index := strings.IndexAny(lowered, "-_"); index > 0 {
		return lowered[:index]
	}
	return lowered
}

// sttEndpointWithToken places a minted token in the query string. The credential
// reaches the URL, so this value must never be logged; the runtime already treats
// route endpoints as sensitive for exactly this reason.
func sttEndpointWithToken(rawEndpoint, token string) (string, error) {
	endpoint, err := url.Parse(rawEndpoint)
	if err != nil {
		return "", errors.New("elevenlabs stt endpoint could not be prepared")
	}
	query := endpoint.Query()
	query.Set("token", token)
	endpoint.RawQuery = query.Encode()
	return endpoint.String(), nil
}

type sttStream struct {
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent
	rate   int

	writeMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	inputPending atomic.Bool
	closeErr     error
	stateMu      sync.Mutex
	baseFinals   []string
	timedFinals  []string
}

func (s *sttStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

// WriteAudio forwards PCM as base64 inside a JSON frame. Realtime Scribe has no
// binary input frame at all — audio arrives as `input_audio_chunk` messages — so
// this is a text write even though the payload is audio.
func (s *sttStream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("elevenlabs stt audio is empty")
	}
	for offset := 0; offset < len(audio); offset += sttPCMFrameBytes {
		end := offset + sttPCMFrameBytes
		if end > len(audio) {
			end = len(audio)
		}
		chunk := map[string]any{
			"message_type":  "input_audio_chunk",
			"audio_base_64": base64.StdEncoding.EncodeToString(audio[offset:end]),
			"commit":        false,
			"sample_rate":   s.rate,
		}
		if err := s.writeJSON(ctx, chunk); err != nil {
			return err
		}
	}
	s.inputPending.Store(true)
	return nil
}

// CommitAudio flushes buffered audio. An empty chunk with commit:true is the
// documented end-of-input signal and forces a final committed transcript for the
// last segment; there is no separate finalize message.
func (s *sttStream) CommitAudio(ctx context.Context) error {
	if !s.inputPending.CompareAndSwap(true, false) {
		return nil
	}
	if err := s.writeJSON(ctx, map[string]any{
		"message_type":  "input_audio_chunk",
		"audio_base_64": "",
		"commit":        true,
	}); err != nil {
		s.inputPending.Store(true)
		return err
	}
	return nil
}

func (s *sttStream) AppendText(context.Context, string) error {
	return runtimepkg.ErrUnsupportedOperation
}

func (s *sttStream) CommitText(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// Cancel closes the stream because the realtime protocol has no distinct cancel
// command. It aborts rather than waiting for a final result.
func (s *sttStream) Cancel(ctx context.Context) error {
	if err := s.Close(ctx); err != nil {
		return err
	}
	return s.abort()
}

// Abort immediately tears down the socket after a runtime terminal failure.
func (s *sttStream) Abort(context.Context) error { return s.abort() }

// Close commits any buffered audio and then shuts the socket down. Committing
// first is what makes the last segment arrive: dropping the socket without it
// discards whatever Scribe had buffered.
func (s *sttStream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
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
			Message:   "ElevenLabs streaming write failed",
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
					Message:   "ElevenLabs streaming read failed",
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
			Message:   "ElevenLabs sent malformed streaming JSON",
			Retryable: true,
			Cause:     err,
		}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))
	switch message.MessageType {
	case "session_started":
		// The protocol-level ack. auth_error and quota_exceeded arrive as frames on
		// an ALREADY-OPEN socket, so readiness has to wait for this rather than fire
		// at dial — otherwise those failures look like mid-stream errors and the
		// runtime loses its open-failure failover.
		return s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventUsageObserved,
			Data:       sttMarshalData(map[string]any{"provider_request_id": message.SessionID}),
			Extensions: sttExtension(raw),
		})
	case "partial_transcript":
		if strings.TrimSpace(message.Text) == "" {
			return nil
		}
		return s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventTranscriptDelta,
			Data:       sttTranscriptData(message.Text, false, nil),
			Extensions: sttExtension(raw),
		})
	case "final_transcript", "committed_transcript":
		if strings.TrimSpace(message.Text) == "" {
			return nil
		}
		if s.consumeTimedFinal(message.Text) {
			// The timestamped twin already carried both text and alignment.
			return nil
		}
		s.rememberBaseFinal(message.Text)
		return s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventTranscriptFinal,
			Data:       sttTranscriptData(message.Text, true, nil),
			Extensions: sttExtension(raw),
		})
	case "final_transcript_with_timestamps", "committed_transcript_with_timestamps":
		if strings.TrimSpace(message.Text) == "" {
			return nil
		}
		if s.consumeBaseFinal(message.Text) {
			return s.emit(runtimepkg.ProviderEvent{
				Type:       protocol.EventAlignment,
				Data:       sttAlignmentData(message.Text, message.Words),
				Extensions: sttExtension(raw),
			})
		}
		s.rememberTimedFinal(message.Text)
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventTranscriptFinal, Data: sttTranscriptData(message.Text, true, message.Words), Extensions: sttExtension(raw)})
	case "insufficient_audio_activity":
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: sttMarshalData(map[string]any{"message": "ElevenLabs detected no transcribable speech", "provider_type": message.MessageType}), Extensions: sttExtension(raw)})
	case "error", "scribe_error", "auth_error", "quota_exceeded", "rate_limited", "input_error", "transcriber_error", "commit_throttled", "unaccepted_terms", "queue_overflow", "resource_exhausted", "session_time_limit_exceeded", "chunk_size_exceeded":
		return &runtimepkg.ProviderError{
			Code:      sttErrorCode(message.MessageType),
			Message:   sttErrorMessage(message.MessageType, message.Error),
			Hint:      sttErrorHint(message.MessageType),
			Retryable: sttErrorRetryable(message.MessageType),
		}
	default:
		return s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventWarning,
			Data:       sttMarshalData(map[string]any{"message": "ignored ElevenLabs message type", "provider_type": message.MessageType}),
			Extensions: sttExtension(raw),
		})
	}
}

func (s *sttStream) rememberBaseFinal(text string) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.baseFinals = rememberFinal(s.baseFinals, text)
}

func (s *sttStream) consumeBaseFinal(text string) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return consumeFinal(&s.baseFinals, text)
}

func (s *sttStream) rememberTimedFinal(text string) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.timedFinals = rememberFinal(s.timedFinals, text)
}

func (s *sttStream) consumeTimedFinal(text string) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return consumeFinal(&s.timedFinals, text)
}

func rememberFinal(finals []string, text string) []string {
	finals = append(finals, text)
	if len(finals) > 32 {
		finals = append([]string(nil), finals[len(finals)-32:]...)
	}
	return finals
}

func consumeFinal(finals *[]string, text string) bool {
	for i, candidate := range *finals {
		if candidate == text {
			*finals = append((*finals)[:i], (*finals)[i+1:]...)
			return true
		}
	}
	return false
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
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return "authentication_failed"
	case http.StatusTooManyRequests:
		return "provider_rate_limited"
	default:
		return "provider_unavailable"
	}
}

// sttErrorCode keeps the vendor's own distinction. Collapsing auth and quota into
// one code would make a dead key and an empty balance indistinguishable, and only
// one of the two is worth retrying elsewhere.
func sttErrorCode(messageType string) string {
	switch messageType {
	case "auth_error", "unaccepted_terms":
		return "authentication_failed"
	case "quota_exceeded":
		return "provider_quota_exceeded"
	case "rate_limited", "commit_throttled":
		return "provider_rate_limited"
	case "input_error", "chunk_size_exceeded":
		return "invalid_request"
	default:
		return "provider_unavailable"
	}
}

func sttErrorRetryable(messageType string) bool {
	switch messageType {
	case "rate_limited", "commit_throttled", "queue_overflow", "resource_exhausted", "session_time_limit_exceeded", "transcriber_error", "error", "scribe_error":
		return true
	default:
		return false
	}
}

func sttErrorHint(messageType string) string {
	switch messageType {
	case "auth_error":
		return "Check that the ElevenLabs credential is active and authorized for realtime transcription."
	case "unaccepted_terms":
		return "Accept the ElevenLabs Scribe terms for the provider account, then retry."
	case "quota_exceeded":
		return "Add ElevenLabs quota or select another provider."
	case "rate_limited", "commit_throttled":
		return "Retry with backoff and reduce request or commit frequency."
	case "queue_overflow", "resource_exhausted", "session_time_limit_exceeded", "transcriber_error", "error", "scribe_error":
		return "Retry after a brief delay or use auto routing to select another provider."
	case "chunk_size_exceeded":
		return "Retry the request; the relay will split audio into smaller provider frames."
	default:
		return "Check the advertised media and request options, correct them, and retry."
	}
}

func sttErrorMessage(messageType, detail string) string {
	if strings.TrimSpace(detail) == "" {
		return "ElevenLabs reported a streaming " + messageType
	}
	return "ElevenLabs reported a streaming " + messageType + ": " + detail
}

func sttExtension(raw json.RawMessage) map[string]json.RawMessage {
	return map[string]json.RawMessage{extensionID: raw}
}

func sttTranscriptData(text string, final bool, words []sttWord) json.RawMessage {
	data := map[string]any{"text": text, "is_final": final, "speech_final": final}
	// Scribe tags each token with a type and only `word` entries carry meaningful
	// timings; `spacing` tokens would otherwise contribute zero-width ranges.
	if timings := sttWordTimings(words); len(timings) > 0 {
		data["words"] = timings
		data["audio_start_ms"] = timings[0]["start_ms"]
		data["audio_end_ms"] = timings[len(timings)-1]["end_ms"]
	}
	return sttMarshalData(data)
}

func sttAlignmentData(text string, words []sttWord) json.RawMessage {
	data := map[string]any{"text": text}
	if timings := sttWordTimings(words); len(timings) > 0 {
		data["words"] = timings
		data["audio_start_ms"] = timings[0]["start_ms"]
		data["audio_end_ms"] = timings[len(timings)-1]["end_ms"]
	}
	return sttMarshalData(data)
}

func sttWordTimings(words []sttWord) []map[string]any {
	var timings []map[string]any
	for _, word := range words {
		if word.Type != "" && word.Type != "word" {
			continue
		}
		if strings.TrimSpace(word.Text) == "" {
			continue
		}
		if math.IsNaN(word.Start) || math.IsNaN(word.End) || math.IsInf(word.Start, 0) || math.IsInf(word.End, 0) {
			continue
		}
		timings = append(timings, map[string]any{
			"text":     word.Text,
			"start_ms": sttMilliseconds(word.Start),
			"end_ms":   sttMilliseconds(word.End),
		})
	}
	return timings
}

func sttMarshalData(value any) json.RawMessage {
	payload, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{"code":"internal"}`)
	}
	return payload
}

func sttMilliseconds(seconds float64) int64 {
	return int64(math.Round(seconds * 1_000))
}

// sttWord is one token of a committed_transcript_with_timestamps frame. Start and
// End are SECONDS, and Type distinguishes `word` from `spacing`.
type sttWord struct {
	Text  string  `json:"text"`
	Start float64 `json:"start"`
	End   float64 `json:"end"`
	Type  string  `json:"type"`
}

type sttInboundMessage struct {
	MessageType string    `json:"message_type"`
	SessionID   string    `json:"session_id"`
	Text        string    `json:"text"`
	Error       string    `json:"error"`
	Words       []sttWord `json:"words"`
}
