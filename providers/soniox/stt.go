package soniox

import (
	"context"
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
	// STTAdapterID is the identifier returned by a Soniox STT session plan.
	STTAdapterID = "soniox.stt.v1"

	sttExtensionID  = "soniox.com/stt/v1"
	sttOfficialHost = "stt-rt.soniox.com"
	sttEndpointPath = "/transcribe-websocket"

	// endToken is emitted once, always final, when Soniox's semantic endpointer
	// decides the speaker finished an utterance. finToken is the reply to a
	// client-issued finalize. They are the only two segment boundaries in the
	// protocol: every other token is a word or word piece.
	endToken = "<end>"
	finToken = "<fin>"

	// Soniox records client_reference_id in usage logs and rejects anything
	// longer with HTTP 400.
	sttMaxClientReferenceCharacters = 256
)

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

// STTAdapter implements Soniox's /transcribe-websocket realtime API.
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
		return nil, errors.New("soniox event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("soniox maximum message bytes must be positive")
	}
	endpointPolicy, err := upstream.NewWebSocketPolicy(sttOfficialHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
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

// Open dials the realtime socket and completes Soniox's start handshake before
// returning. The handshake is a JSON message rather than an HTTP header, so a
// rejected credential or an unsupported model surfaces asynchronously as an
// error frame; sending the start message here at least guarantees the socket is
// configured before the runtime is allowed to write the first audio frame, and
// beats the roughly ten-second deadline Soniox gives an unconfigured socket.
func (a *STTAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindSTT {
		return nil, fmt.Errorf("soniox stt supports stt sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "soniox" {
		return nil, fmt.Errorf("soniox adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("soniox stt requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("soniox stt requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("soniox stt media: %w", err)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model == "" || model == "auto" {
		return nil, errors.New("soniox stt requires a concrete model in the session plan")
	}
	audioFormat, err := sttAudioFormat(request.Media.Encoding)
	if err != nil {
		return nil, err
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("soniox stt requires a bearer credential")
	}
	endpoint, err := sttEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}

	// No Authorization header: Soniox authenticates the start message, not the
	// handshake, for managed, BYOK, and relay credentials alike. See doc.go.
	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: sttHTTPClient(a.httpClient)})
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

	start := sttStartRequest{
		APIKey:      credential.Value,
		Model:       model,
		AudioFormat: audioFormat,
		SampleRate:  request.Media.SampleRateHz,
		NumChannels: request.Media.Channels,
		// Soniox is the endpointer for this route. Its segment boundaries are
		// the only way a caller that never issues CommitAudio can ever receive
		// a final transcript, because Soniox commits tokens at word-piece
		// granularity and never marks an utterance complete on its own.
		EnableEndpointDetection: true,
		LanguageHints:           sttLanguageHints(request.Options.Language),
		ClientReferenceID:       sttClientReferenceID(request.Plan),
	}
	payload, err := json.Marshal(start)
	if err != nil {
		_ = conn.CloseNow()
		return nil, err
	}
	if err := conn.Write(ctx, websocket.MessageText, payload); err != nil {
		_ = conn.CloseNow()
		return nil, &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "Soniox streaming start request failed",
			Retryable: true,
			Cause:     err,
		}
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
// connector that synthesizes the plan and drives these adapters directly — no
// Engine, no SessionPlan.Validate — labels the same permanent Soniox key
// bearer. Both spellings ride the same api_key field of the first JSON
// message: Soniox has no header-versus-query channel to split on, so nothing
// else on the relay arm changes.
func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

func sttEndpoint(policy upstream.WebSocketPolicy, rawEndpoint string) (string, error) {
	endpoint, err := policy.Parse(rawEndpoint)
	if err != nil {
		return "", fmt.Errorf("soniox stt endpoint: %w", err)
	}
	if endpoint.Path != sttEndpointPath {
		return "", fmt.Errorf("soniox stt endpoint path must be %s, got %q", sttEndpointPath, endpoint.Path)
	}
	return endpoint.String(), nil
}

// sttAudioFormat maps the portable media encoding onto a Soniox raw-audio
// format name. Soniox's raw list covers signed, unsigned, float, mulaw and alaw
// PCM but no bare Opus; Opus is only readable through its "auto" container
// detection, which needs an Ogg or WebM header the runtime does not produce.
func sttAudioFormat(encoding string) (string, error) {
	if encoding == "pcm_s16le" {
		return "pcm_s16le", nil
	}
	return "", fmt.Errorf("soniox stt does not support media encoding %q", encoding)
}

// sonioxLanguageAliases maps platform language subtags onto the spellings
// Soniox's supported-language table actually lists. Soniox answers an unlisted
// hint with HTTP 400 "Invalid language hint." and closes the socket, so an
// unaliased `nb` would take down the whole session rather than degrade.
var sonioxLanguageAliases = map[string]string{
	"fil": "tl",
	"nb":  "no",
	"nn":  "no",
}

// sttLanguageHints biases recognition without restricting it. Soniox
// auto-detects across 60+ languages when the field is absent, which is the
// right behaviour for an unset or "auto" request language.
func sttLanguageHints(language string) []string {
	primary, ok := sonioxPrimaryLanguage(language)
	if !ok {
		return nil
	}
	return []string{primary}
}

func sonioxPrimaryLanguage(language string) (string, bool) {
	primary := strings.ToLower(strings.TrimSpace(language))
	if index := strings.IndexByte(primary, '-'); index >= 0 {
		primary = primary[:index]
	}
	if primary == "" || primary == "auto" {
		return "", false
	}
	if alias, ok := sonioxLanguageAliases[primary]; ok {
		return alias, true
	}
	return primary, true
}

// sttClientReferenceID correlates the provider's usage log with the plan's
// reservation, the same intent as the Deepgram adapter's `extra` parameter.
// Soniox ignores the field when the credential is a temporary API key, so this
// is best-effort correlation for managed provider-direct routes rather than a
// guarantee. Relay plans are managed too and take the same tag — and because
// they carry the connector's permanent key, Soniox does record it for them.
func sttClientReferenceID(plan protocol.SessionPlan) string {
	if plan.Execution.CredentialSource != protocol.CredentialsManaged {
		return ""
	}
	reservationID := strings.TrimSpace(plan.Reservation.ID)
	if len(reservationID) > sttMaxClientReferenceCharacters {
		return ""
	}
	return reservationID
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

	// Read-loop owned; never touched by the write side.
	segment   sttSegment
	requestID string
}

func (s *sttStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

// WriteAudio forwards a raw PCM frame. An empty frame is refused rather than
// forwarded because Soniox reads a zero-length WebSocket frame as end-of-stream
// and would close the socket mid-call.
func (s *sttStream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("soniox stt audio is empty")
	}
	return s.write(ctx, websocket.MessageBinary, audio)
}

// CommitAudio issues Soniox's manual finalization control. Soniox finalizes
// everything received so far and answers with a <fin> token, which the read
// loop turns into a final transcript. The socket stays open for the next turn.
func (s *sttStream) CommitAudio(ctx context.Context) error {
	return s.writeJSON(ctx, map[string]string{"type": "finalize"})
}

func (s *sttStream) AppendText(context.Context, string) error {
	return runtimepkg.ErrUnsupportedOperation
}

func (s *sttStream) CommitText(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// Cancel closes the stream because the transcribe protocol has no cancel
// command, then aborts rather than waiting for a trailing final result.
func (s *sttStream) Cancel(ctx context.Context) error {
	if err := s.Close(ctx); err != nil {
		return err
	}
	return s.abort()
}

// Abort immediately tears down the socket after a terminal runtime failure.
func (s *sttStream) Abort(context.Context) error { return s.abort() }

// Close performs Soniox's documented shutdown: an empty frame, after which the
// server replies with a finished response and closes. A finalize is sent first
// because the empty frame has been observed in production not to flush an
// in-flight segment, and finalize is documented as safe to repeat.
func (s *sttStream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		s.writeMu.Lock()
		if err := sttWriteControl(ctx, s.conn, map[string]string{"type": "finalize"}); err != nil {
			s.closeErr = err
		}
		if s.closeErr == nil {
			if err := s.conn.Write(ctx, websocket.MessageBinary, []byte{}); err != nil {
				s.closeErr = err
			}
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
	return s.write(ctx, websocket.MessageText, payload)
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
		return &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "Soniox streaming write failed",
			Retryable: true,
			Cause:     err,
		}
	}
	return nil
}

func sttWriteControl(ctx context.Context, conn *websocket.Conn, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, payload)
}

func (s *sttStream) readLoop() {
	defer close(s.events)
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && !sonioxIsNormalClose(err) && s.ctx.Err() == nil {
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

func (s *sttStream) handleMessage(payload []byte) error {
	var message sttInboundMessage
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
		// An error invalidates the turn: Soniox closes the socket immediately
		// after this frame and the runtime fails the attempt over. Committing
		// the half-finished segment first would land a truncated user turn, so
		// it is dropped along with the stream.
		code, retryable := sonioxErrorCode(message.ErrorType, message.ErrorCode)
		return &runtimepkg.ProviderError{
			Code:           code,
			Message:        sonioxErrorMessage("Soniox reported a streaming error", message.ErrorMessage),
			Retryable:      retryable,
			ProviderStatus: message.ErrorCode,
			Extensions:     sttExtension(raw),
		}
	}
	if message.RequestID != "" && s.requestID != message.RequestID {
		s.requestID = message.RequestID
		if err := s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventUsageObserved,
			Data:       sonioxUsageData(message.RequestID, nil),
			Extensions: sttExtension(raw),
		}); err != nil {
			return err
		}
	}

	if err := s.handleTokens(message, raw); err != nil {
		return err
	}

	if message.Finished {
		// The socket is about to close. Flush whatever the endpointer never
		// got round to marking, then report the provider's own measure of
		// processed audio, which is the unit an STT reservation is priced in.
		if err := s.flushSegment(raw); err != nil {
			return err
		}
		processed := message.TotalAudioProcMS
		return s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventUsageObserved,
			Data:       sonioxUsageData(s.requestID, &processed),
			Extensions: sttExtension(raw),
		})
	}
	return nil
}

// handleTokens walks one frame in wire order. Soniox re-sends the entire
// non-final tail on every frame but sends each final token exactly once, so
// finals accumulate into the segment while the tail is rebuilt per frame.
func (s *sttStream) handleTokens(message sttInboundMessage, raw json.RawMessage) error {
	tail := strings.Builder{}
	sawContent := false
	var tailEndMS *int64

	for _, token := range message.Tokens {
		if token.Text == endToken || token.Text == finToken {
			if err := s.flushSegment(raw); err != nil {
				return err
			}
			// A provisional tail cannot outlive the boundary that closed its
			// segment; Soniox restarts the tail from scratch afterwards.
			tail.Reset()
			tailEndMS = nil
			sawContent = false
			if token.Text == endToken {
				// <end> is the semantic endpointer's verdict that the speaker
				// finished. <fin> is only this adapter's own finalize coming
				// back, and the runtime already knows it asked for that.
				if err := s.emit(runtimepkg.ProviderEvent{
					Type:       protocol.EventSpeechEnded,
					Data:       sttSpeechEndedData(token.EndMS),
					Extensions: sttExtension(raw),
				}); err != nil {
					return err
				}
			}
			continue
		}
		sawContent = true
		if token.IsFinal {
			s.segment.appendFinal(token)
			continue
		}
		tail.WriteString(token.Text)
		if token.EndMS != nil {
			tailEndMS = token.EndMS
		}
	}

	if !sawContent {
		return nil
	}
	interim := strings.TrimSpace(s.segment.text.String() + tail.String())
	if interim == "" {
		return nil
	}
	endMS := tailEndMS
	if endMS == nil {
		endMS = s.segment.endMS
	}
	return s.emit(runtimepkg.ProviderEvent{
		Type:       protocol.EventTranscriptDelta,
		Data:       sttTranscriptData(interim, false, s.segment.startMS, endMS, nil, s.requestID),
		Extensions: sttExtension(raw),
	})
}

func (s *sttStream) flushSegment(raw json.RawMessage) error {
	text, confidence, startMS, endMS := s.segment.flush()
	if text == "" {
		return nil
	}
	return s.emit(runtimepkg.ProviderEvent{
		Type:       protocol.EventTranscriptFinal,
		Data:       sttTranscriptData(text, true, startMS, endMS, confidence, s.requestID),
		Extensions: sttExtension(raw),
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

// sttSegment accumulates the committed tokens of one Soniox segment until an
// <end> or <fin> closes it. Confidence is averaged over the tokens that carried
// one; timestamps are absent on translated tokens, hence the pointers.
type sttSegment struct {
	text          strings.Builder
	confidenceSum float64
	confidenceN   int
	startMS       *int64
	endMS         *int64
}

func (a *sttSegment) appendFinal(token sttToken) {
	a.text.WriteString(token.Text)
	if token.Confidence != nil {
		a.confidenceSum += *token.Confidence
		a.confidenceN++
	}
	if token.StartMS != nil && a.startMS == nil {
		a.startMS = token.StartMS
	}
	if token.EndMS != nil {
		a.endMS = token.EndMS
	}
}

func (a *sttSegment) flush() (string, *float64, *int64, *int64) {
	text := strings.TrimSpace(a.text.String())
	var confidence *float64
	if a.confidenceN > 0 {
		average := a.confidenceSum / float64(a.confidenceN)
		confidence = &average
	}
	startMS, endMS := a.startMS, a.endMS
	*a = sttSegment{}
	return text, confidence, startMS, endMS
}

func sttTranscriptData(text string, isFinal bool, startMS, endMS *int64, confidence *float64, requestID string) json.RawMessage {
	data := map[string]any{"text": text, "is_final": isFinal, "provider_request_id": requestID}
	if startMS != nil {
		data["audio_start_ms"] = *startMS
	}
	if endMS != nil {
		data["audio_end_ms"] = *endMS
	}
	if confidence != nil {
		data["confidence"] = *confidence
	}
	return sonioxMarshalData(data)
}

func sttSpeechEndedData(endMS *int64) json.RawMessage {
	data := map[string]any{"reason": "endpoint_detected"}
	if endMS != nil {
		data["audio_end_ms"] = *endMS
	}
	return sonioxMarshalData(data)
}

func sttExtension(raw json.RawMessage) map[string]json.RawMessage {
	return map[string]json.RawMessage{sttExtensionID: raw}
}

type sttStartRequest struct {
	APIKey                  string   `json:"api_key"`
	Model                   string   `json:"model"`
	AudioFormat             string   `json:"audio_format"`
	SampleRate              int      `json:"sample_rate"`
	NumChannels             int      `json:"num_channels"`
	EnableEndpointDetection bool     `json:"enable_endpoint_detection"`
	LanguageHints           []string `json:"language_hints,omitempty"`
	ClientReferenceID       string   `json:"client_reference_id,omitempty"`
}

type sttToken struct {
	Text       string   `json:"text"`
	StartMS    *int64   `json:"start_ms"`
	EndMS      *int64   `json:"end_ms"`
	Confidence *float64 `json:"confidence"`
	IsFinal    bool     `json:"is_final"`
}

type sttInboundMessage struct {
	Tokens           []sttToken `json:"tokens"`
	FinalAudioProcMS int64      `json:"final_audio_proc_ms"`
	TotalAudioProcMS int64      `json:"total_audio_proc_ms"`
	Finished         bool       `json:"finished"`
	ErrorCode        int        `json:"error_code"`
	ErrorType        string     `json:"error_type"`
	ErrorMessage     string     `json:"error_message"`
	RequestID        string     `json:"request_id"`
}

// sonioxErrorCode maps a Soniox error frame onto the gateway's stable
// classification. Soniox states that error_type is stable across releases and
// error_message is not, so the type is the discriminator and the HTTP code is
// only a fallback for a type this adapter has not seen.
func sonioxErrorCode(errorType string, status int) (string, bool) {
	switch errorType {
	case "unauthenticated", "temp_api_key_session_expired":
		// A temporary key whose max_session_duration_seconds elapsed is not a
		// transient failure: only minting a new key clears it.
		return "authentication_failed", false
	case "organization_balance_exhausted", "organization_monthly_budget_exhausted", "project_monthly_budget_exhausted":
		return "provider_quota_exceeded", false
	case "limit_exceeded":
		return "provider_rate_limited", true
	case "invalid_request", "invalid_stream_state", "model_not_available", "max_concurrent_streams_reached", "invalid_audio_file", "invalid_cursor":
		return "invalid_request", false
	case "request_timeout", "internal_error", "service_unavailable", "max_duration_reached":
		// max_duration_reached is retryable in the sense Soniox documents:
		// open a new connection and keep going.
		return "provider_unavailable", true
	}
	return sonioxStatusCode(status)
}

func sonioxStatusCode(status int) (string, bool) {
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return "authentication_failed", false
	case status == http.StatusPaymentRequired:
		return "provider_quota_exceeded", false
	case status == http.StatusTooManyRequests:
		return "provider_rate_limited", true
	case status == http.StatusRequestTimeout:
		return "provider_unavailable", true
	case status >= 400 && status < 500:
		return "invalid_request", false
	}
	return "provider_unavailable", true
}

func sonioxErrorMessage(prefix, message string) string {
	if strings.TrimSpace(message) == "" {
		return prefix
	}
	return prefix + ": " + message
}

func sonioxIsNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}

func sonioxUsageData(requestID string, processedMS *int64) json.RawMessage {
	data := map[string]any{"provider_request_id": requestID}
	if processedMS != nil {
		data["audio_processed_ms"] = *processedMS
	}
	return sonioxMarshalData(data)
}

func sonioxMarshalData(value any) json.RawMessage {
	payload, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{"code":"internal"}`)
	}
	return payload
}
