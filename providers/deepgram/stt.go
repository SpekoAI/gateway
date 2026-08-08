package deepgram

import (
	"context"
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
	// AdapterID is the identifier returned by a Deepgram STT session plan.
	AdapterID       = "deepgram.stt.v1"
	extensionID     = "deepgram.com/v1"
	officialAPIHost = "api.deepgram.com"
)

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

// Adapter implements Deepgram's /v1/listen WebSocket API.
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
		return nil, errors.New("deepgram event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("deepgram maximum message bytes must be positive")
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

// Open opens a provider-direct STT stream. It maps the portable media format
// and plan selection into Deepgram's documented listen query parameters.
func (a *Adapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindSTT {
		return nil, fmt.Errorf("deepgram supports stt sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "deepgram" {
		return nil, fmt.Errorf("deepgram adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("deepgram requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("deepgram requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("deepgram media: %w", err)
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("deepgram requires a bearer credential")
	}
	reservationID := ""
	// Reservation tagging follows the plan's billing authority, not its credential
	// placement: a relay plan settles against a Speko reservation exactly like a
	// managed provider-direct plan, so both must bind provider-side usage to it.
	if request.Plan.Execution.ProviderRoute == protocol.RouteSpekoRelay || request.Plan.Execution.CredentialSource == protocol.CredentialsManaged {
		reservationID = request.Plan.Reservation.ID
	}
	endpoint, err := listenEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint, request.Plan.Route.Model, request.Options, *request.Media, reservationID)
	if err != nil {
		return nil, err
	}
	headers := make(http.Header)
	authorizationScheme := "Bearer"
	if request.Plan.Execution.CredentialSource == protocol.CredentialsBYOK {
		authorizationScheme = "Token"
	}
	// A relay plan is managed for billing purposes but carries the connector's
	// permanent Deepgram key, which authenticates with the Token scheme exactly
	// like a customer-owned key. Bearer stays reserved for the short-lived tokens
	// the control plane mints on managed provider-direct routes.
	if request.Plan.Execution.ProviderRoute == protocol.RouteSpekoRelay {
		authorizationScheme = "Token"
	}
	headers.Set("Authorization", authorizationScheme+" "+credential.Value)
	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{
		HTTPClient: configHTTPClient(a.httpClient),
		HTTPHeader: headers,
	})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		return nil, &runtimepkg.ProviderError{
			Code:           dialErrorCode(status),
			Message:        "Deepgram streaming connection could not be established",
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

func configHTTPClient(client *http.Client) *http.Client {
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
// Engine, no SessionPlan.Validate — labels the same permanent Deepgram key
// bearer. Both spellings carry a permanent key; scheme selection and
// reservation tagging key off the route, not the kind, so nothing else on
// the relay arm changes.
func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

func listenEndpoint(policy upstream.WebSocketPolicy, rawEndpoint, model string, options protocol.RequestOptions, media protocol.MediaFormat, reservationID string) (string, error) {
	endpoint, err := policy.Parse(rawEndpoint)
	if err != nil {
		return "", fmt.Errorf("deepgram endpoint: %w", err)
	}
	if endpoint.Path != "/v1/listen" {
		return "", fmt.Errorf("deepgram endpoint path must be /v1/listen, got %q", endpoint.Path)
	}
	if strings.TrimSpace(model) == "" || model == "auto" {
		return "", errors.New("deepgram requires a concrete model in the session plan")
	}
	encoding, err := deepgramEncoding(media.Encoding)
	if err != nil {
		return "", err
	}
	query := endpoint.Query()
	query.Set("model", model)
	query.Set("encoding", encoding)
	query.Set("sample_rate", strconv.Itoa(media.SampleRateHz))
	query.Set("channels", strconv.Itoa(media.Channels))
	query.Set("interim_results", "true")
	// The framework owns VAD/turn detection by default. Explicit Deepgram
	// endpointing support can be introduced through a validated extension later.
	query.Set("endpointing", "false")
	if strings.TrimSpace(options.Language) != "" {
		query.Set("language", options.Language)
	}
	if strings.TrimSpace(reservationID) != "" {
		query.Set("extra", "speko_reservation:"+reservationID)
	}
	endpoint.RawQuery = query.Encode()
	return endpoint.String(), nil
}

func deepgramEncoding(encoding string) (string, error) {
	switch encoding {
	case "pcm_s16le":
		return "linear16", nil
	case "opus":
		return "opus", nil
	default:
		return "", fmt.Errorf("deepgram does not support media encoding %q", encoding)
	}
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
	closeErr     error

	requestID string
}

func (s *stream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

func (s *stream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("deepgram audio is empty")
	}
	return s.write(ctx, websocket.MessageBinary, audio)
}

func (s *stream) CommitAudio(ctx context.Context) error {
	return s.writeJSON(ctx, map[string]string{"type": "Finalize"})
}

func (s *stream) AppendText(context.Context, string) error { return runtimepkg.ErrUnsupportedOperation }

func (s *stream) CommitText(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// Cancel closes the STT stream because the documented listen protocol has no
// distinct cancel command. It aborts rather than waiting for a final result.
func (s *stream) Cancel(ctx context.Context) error {
	if err := s.Close(ctx); err != nil {
		return err
	}
	return s.abort()
}

// Abort immediately tears down the socket after a runtime terminal failure.
func (s *stream) Abort(context.Context) error { return s.abort() }

func (s *stream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		s.closed.Store(true)
		s.writeMu.Lock()
		if err := writeControl(ctx, s.conn, map[string]string{"type": "CloseStream"}); err != nil {
			s.closeErr = err
		}
		s.writeMu.Unlock()
		if s.closeErr != nil {
			_ = s.abort()
		}
	})
	return s.closeErr
}

func (s *stream) abort() error {
	s.abortOnce.Do(func() {
		s.cancel()
		if err := s.conn.CloseNow(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
	})
	return s.closeErr
}

func (s *stream) writeJSON(ctx context.Context, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return s.write(ctx, websocket.MessageText, payload)
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
			Message:   "Deepgram streaming write failed",
			Retryable: true,
			Cause:     err,
		}
	}
	return nil
}

func writeControl(ctx context.Context, conn *websocket.Conn, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, payload)
}

func (s *stream) readLoop() {
	defer close(s.events)
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && !isNormalClose(err) && s.ctx.Err() == nil {
				s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{
					Code:      "provider_unavailable",
					Message:   "Deepgram streaming read failed",
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

func (s *stream) handleMessage(payload []byte) error {
	var message inboundMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "Deepgram sent malformed streaming JSON",
			Retryable: true,
			Cause:     err,
		}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))
	switch message.Type {
	case "Metadata":
		s.setRequestID(message.Metadata.RequestID)
		return s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventUsageObserved,
			Data:       usageData(message.Metadata.RequestID),
			Extensions: extension(raw),
		})
	case "SpeechStarted":
		return s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventSpeechStarted,
			Data:       speechStartedData(message.Timestamp),
			Extensions: extension(raw),
		})
	case "UtteranceEnd":
		return s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventSpeechEnded,
			Data:       utteranceEndData(message.LastWordEnd),
			Extensions: extension(raw),
		})
	case "Results":
		return s.emitResults(message, raw)
	case "Error":
		return &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   deepgramErrorMessage(message.Description),
			Retryable: false,
		}
	default:
		return s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventWarning,
			Data:       warningData(message.Type),
			Extensions: extension(raw),
		})
	}
}

func (s *stream) emitResults(message inboundMessage, raw json.RawMessage) error {
	if message.Metadata.RequestID != "" && s.setRequestID(message.Metadata.RequestID) {
		if err := s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventUsageObserved,
			Data:       usageData(message.Metadata.RequestID),
			Extensions: extension(raw),
		}); err != nil {
			return err
		}
	}
	transcript := ""
	if len(message.Channel.Alternatives) > 0 {
		transcript = message.Channel.Alternatives[0].Transcript
	}
	if transcript == "" {
		return nil
	}
	kind := protocol.EventTranscriptDelta
	if message.IsFinal {
		kind = protocol.EventTranscriptFinal
	}
	if err := s.emit(runtimepkg.ProviderEvent{
		Type:       kind,
		Data:       transcriptData(transcript, message, s.currentRequestID()),
		Extensions: extension(raw),
	}); err != nil {
		return err
	}
	if message.IsFinal && message.SpeechFinal {
		return s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventSpeechEnded,
			Data:       speechFinalData(message.Start + message.Duration),
			Extensions: extension(raw),
		})
	}
	return nil
}

func (s *stream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *stream) setRequestID(value string) bool {
	if value == "" || s.requestID == value {
		return false
	}
	s.requestID = value
	return true
}

func (s *stream) currentRequestID() string { return s.requestID }

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

func extension(raw json.RawMessage) map[string]json.RawMessage {
	return map[string]json.RawMessage{extensionID: raw}
}

func usageData(requestID string) json.RawMessage {
	return marshalData(map[string]any{"provider_request_id": requestID})
}

func speechStartedData(timestamp float64) json.RawMessage {
	return marshalData(map[string]any{"audio_start_ms": milliseconds(timestamp)})
}

func utteranceEndData(lastWordEnd float64) json.RawMessage {
	return marshalData(map[string]any{"audio_end_ms": milliseconds(lastWordEnd)})
}

func speechFinalData(end float64) json.RawMessage {
	return marshalData(map[string]any{"audio_end_ms": milliseconds(end), "reason": "speech_final"})
}

func transcriptData(text string, message inboundMessage, requestID string) json.RawMessage {
	return marshalData(map[string]any{
		"text":                text,
		"is_final":            message.IsFinal,
		"speech_final":        message.SpeechFinal,
		"audio_start_ms":      milliseconds(message.Start),
		"audio_end_ms":        milliseconds(message.Start + message.Duration),
		"provider_request_id": requestID,
	})
}

func warningData(messageType string) json.RawMessage {
	return marshalData(map[string]any{"message": "ignored Deepgram message type", "provider_type": messageType})
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

func deepgramErrorMessage(description string) string {
	if strings.TrimSpace(description) == "" {
		return "Deepgram reported a streaming error"
	}
	return "Deepgram reported a streaming error: " + description
}

type inboundMessage struct {
	Type        string  `json:"type"`
	Description string  `json:"description"`
	IsFinal     bool    `json:"is_final"`
	SpeechFinal bool    `json:"speech_final"`
	Start       float64 `json:"start"`
	Duration    float64 `json:"duration"`
	Timestamp   float64 `json:"timestamp"`
	LastWordEnd float64 `json:"last_word_end"`
	Channel     struct {
		Alternatives []struct {
			Transcript string `json:"transcript"`
		} `json:"alternatives"`
	} `json:"channel"`
	Metadata struct {
		RequestID string `json:"request_id"`
	} `json:"metadata"`
}
