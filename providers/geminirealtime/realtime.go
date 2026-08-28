// Package geminirealtime implements provider-direct Gemini Live sessions.
// The adapter receives only a one-use, model-constrained Google auth token
// from a verified plan and never holds an AI Studio API key.
package geminirealtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

const (
	AdapterID    = "google.live.v1"
	officialHost = "generativelanguage.googleapis.com"
	socketPath   = "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContentConstrained"
	extensionID  = "generativelanguage.googleapis.com/v1beta/live"

	defaultEventBuffer     = 64
	defaultMaxMessageBytes = 4 << 20
	defaultSetupTimeout    = 15 * time.Second
)

type Config struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
	SetupTimeout          time.Duration
}

type Adapter struct {
	id              string
	httpClient      *http.Client
	eventBuffer     int
	maxMessageBytes int64
	setupTimeout    time.Duration
	hosts           map[string]struct{}
	allowInsecure   bool
}

func New(config Config) (*Adapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = AdapterID
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = defaultEventBuffer
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = defaultMaxMessageBytes
	}
	if config.SetupTimeout == 0 {
		config.SetupTimeout = defaultSetupTimeout
	}
	if config.EventBuffer < 1 || config.MaxMessageBytes < 1 || config.SetupTimeout <= 0 {
		return nil, errors.New("gemini realtime: event buffer, message bound and setup timeout must be positive")
	}
	hosts := map[string]struct{}{officialHost: {}}
	for _, host := range config.AllowedEndpointHosts {
		host = strings.ToLower(strings.TrimSpace(host))
		if host == "" || strings.ContainsAny(host, "/:@?#") {
			return nil, errors.New("gemini realtime: allowed endpoint host is invalid")
		}
		hosts[host] = struct{}{}
	}
	return &Adapter{
		id: config.AdapterID, httpClient: config.HTTPClient, eventBuffer: config.EventBuffer,
		maxMessageBytes: config.MaxMessageBytes, setupTimeout: config.SetupTimeout,
		hosts: hosts, allowInsecure: config.AllowInsecureEndpoint,
	}, nil
}

func (a *Adapter) ID() string { return a.id }

// Open establishes the constrained Google socket and waits for setupComplete.
// The Engine emits the canonical session.ready event after this returns.
func (a *Adapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindRealtime {
		return nil, fmt.Errorf("gemini realtime supports realtime sessions, got %q", request.Kind)
	}
	if request.Plan.Execution.ProviderRoute != protocol.RouteProviderDirect {
		return nil, errors.New("gemini realtime requires a provider-direct route")
	}
	if request.Plan.Route.Provider != "google" {
		return nil, fmt.Errorf("gemini realtime adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("gemini realtime requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	model := strings.TrimSpace(strings.TrimPrefix(request.Plan.Route.Model, "models/"))
	if model == "" {
		return nil, errors.New("gemini realtime requires a model")
	}
	if request.Media == nil {
		return nil, errors.New("gemini realtime requires input media configuration")
	}
	if err := validatePCM("input", *request.Media, 16_000); err != nil {
		return nil, err
	}
	if request.Options.S2S == nil || request.Options.S2S.OutputMedia == nil {
		return nil, errors.New("gemini realtime requires output media configuration")
	}
	if err := validatePCM("output", *request.Options.S2S.OutputMedia, 24_000); err != nil {
		return nil, err
	}
	credential := request.Plan.Route.Credential
	if credential == nil || credential.Kind != protocol.CredentialBearer || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("gemini realtime requires a delegated bearer credential")
	}
	endpoint, err := a.parseEndpoint(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("gemini realtime endpoint: %w", err)
	}
	query := endpoint.Query()
	query.Set("access_token", credential.Value)
	endpoint.RawQuery = query.Encode()

	conn, response, err := websocket.Dial(ctx, endpoint.String(), &websocket.DialOptions{HTTPClient: a.httpClient})
	if err != nil {
		return nil, dialError(response, err)
	}
	conn.SetReadLimit(a.maxMessageBytes)
	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &liveStream{
		conn: conn, ctx: streamCtx, cancel: cancel,
		events: make(chan runtimepkg.ProviderEvent, a.eventBuffer), setupDone: make(chan error, 1),
		inputMIME: fmt.Sprintf("audio/pcm;rate=%d", request.Media.SampleRateHz),
	}
	if err := stream.writeJSON(ctx, buildSetup("models/"+model, request.Options)); err != nil {
		_ = stream.abort()
		return nil, err
	}
	go stream.readLoop()
	setupCtx, cancelSetup := context.WithTimeout(ctx, a.setupTimeout)
	defer cancelSetup()
	select {
	case err := <-stream.setupDone:
		if err != nil {
			_ = stream.abort()
			return nil, err
		}
	case <-setupCtx.Done():
		_ = stream.abort()
		return nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Gemini Live did not acknowledge setup", Retryable: true, Cause: setupCtx.Err()}
	}
	return stream, nil
}

func validatePCM(direction string, media protocol.MediaFormat, sampleRate int) error {
	if err := media.Validate(); err != nil {
		return fmt.Errorf("gemini realtime %s media: %w", direction, err)
	}
	if media.Encoding != "pcm_s16le" || media.Channels != 1 || media.SampleRateHz != sampleRate {
		return &runtimepkg.ProviderError{
			Code:    "unsupported_media",
			Message: fmt.Sprintf("gemini realtime requires %d Hz mono pcm_s16le %s, got %s/%d Hz/%d channels", sampleRate, direction, media.Encoding, media.SampleRateHz, media.Channels),
			Hint:    fmt.Sprintf("Convert %s audio to %d Hz mono pcm_s16le.", direction, sampleRate),
		}
	}
	return nil
}

func (a *Adapter) parseEndpoint(raw string) (*url.URL, error) {
	endpoint, err := url.Parse(raw)
	if err != nil || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.Fragment != "" || endpoint.RawQuery != "" {
		return nil, errors.New("endpoint must be a clean absolute WebSocket URL")
	}
	if endpoint.Scheme != "wss" && !(a.allowInsecure && endpoint.Scheme == "ws") {
		return nil, errors.New("endpoint must use wss")
	}
	if !a.allowInsecure && endpoint.Port() != "" && endpoint.Port() != "443" {
		return nil, errors.New("endpoint uses a non-standard port")
	}
	if _, ok := a.hosts[strings.ToLower(endpoint.Hostname())]; !ok {
		return nil, errors.New("endpoint host is not allowed")
	}
	if endpoint.Path != socketPath {
		return nil, fmt.Errorf("endpoint path must be %s, got %q", socketPath, endpoint.Path)
	}
	return endpoint, nil
}

func buildSetup(model string, options protocol.RequestOptions) map[string]any {
	generation := map[string]any{"responseModalities": []string{"AUDIO"}}
	if voice := strings.TrimSpace(options.Voice); voice != "" {
		generation["speechConfig"] = map[string]any{"voiceConfig": map[string]any{"prebuiltVoiceConfig": map[string]any{"voiceName": voice}}}
	}
	setup := map[string]any{
		"model": model, "generationConfig": generation,
		"inputAudioTranscription": map[string]any{}, "outputAudioTranscription": map[string]any{},
		"sessionResumption": map[string]any{},
	}
	if options.S2S != nil {
		if options.S2S.Temperature != nil {
			generation["temperature"] = *options.S2S.Temperature
		}
		if instructions := strings.TrimSpace(options.S2S.Instructions); instructions != "" {
			setup["systemInstruction"] = map[string]any{"parts": []map[string]string{{"text": instructions}}}
		}
	}
	return map[string]any{"setup": setup}
}

type liveStream struct {
	conn      *websocket.Conn
	ctx       context.Context
	cancel    context.CancelFunc
	events    chan runtimepkg.ProviderEvent
	inputMIME string
	setupDone chan error

	writeMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	setupOnce    sync.Once
	closed       atomic.Bool
	closeErr     error
	responding   atomic.Bool
	terminalMu   sync.Mutex
	terminalErr  error
}

func (s *liveStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

func (s *liveStream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("gemini realtime audio is empty")
	}
	return s.writeJSON(ctx, map[string]any{"realtimeInput": map[string]any{"audio": map[string]string{
		"data": base64.StdEncoding.EncodeToString(audio), "mimeType": s.inputMIME,
	}}})
}

func (s *liveStream) CommitAudio(ctx context.Context) error {
	return s.writeJSON(ctx, map[string]any{"realtimeInput": map[string]any{"audioStreamEnd": true}})
}
func (s *liveStream) AppendText(context.Context, string) error {
	return runtimepkg.ErrUnsupportedOperation
}
func (s *liveStream) CommitText(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// Gemini has no response.cancel message. It reports interruption when new
// speech wins the turn, so Cancel is intentionally local and idempotent.
func (s *liveStream) Cancel(context.Context) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if s.responding.Swap(false) {
		s.emit(runtimepkg.ProviderEvent{Type: protocol.EventResponseCanceled})
	}
	return nil
}

func (s *liveStream) Close(context.Context) error {
	s.gracefulOnce.Do(func() {
		s.closed.Store(true)
		s.writeMu.Lock()
		s.closeErr = s.conn.Close(websocket.StatusNormalClosure, "")
		s.writeMu.Unlock()
		s.cancel()
	})
	return s.closeErr
}
func (s *liveStream) Abort(context.Context) error { return s.abort() }
func (s *liveStream) abort() error {
	s.abortOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		if err := s.conn.CloseNow(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
	})
	return s.closeErr
}
func (s *liveStream) TerminalError() error {
	s.terminalMu.Lock()
	defer s.terminalMu.Unlock()
	return s.terminalErr
}
func (s *liveStream) setTerminal(err error) {
	s.terminalMu.Lock()
	defer s.terminalMu.Unlock()
	if s.terminalErr == nil {
		s.terminalErr = err
	}
}
func (s *liveStream) writeJSON(ctx context.Context, value any) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.conn.Write(ctx, websocket.MessageText, payload); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Gemini Live socket write failed", Retryable: true, Cause: err}
	}
	return nil
}
func (s *liveStream) emit(event runtimepkg.ProviderEvent) {
	if event.Err != nil {
		s.setTerminal(event.Err)
	}
	select {
	case s.events <- event:
	case <-s.ctx.Done():
	}
}
func (s *liveStream) settleSetup(err error) { s.setupOnce.Do(func() { s.setupDone <- err }) }

type serverMessage struct {
	SetupComplete *json.RawMessage `json:"setupComplete"`
	ServerContent *struct {
		ModelTurn *struct {
			Parts []struct {
				Text       string `json:"text"`
				InlineData *struct {
					MIMEType string `json:"mimeType"`
					Data     string `json:"data"`
				} `json:"inlineData"`
			} `json:"parts"`
		} `json:"modelTurn"`
		TurnComplete        bool                   `json:"turnComplete"`
		Interrupted         bool                   `json:"interrupted"`
		GenerationComplete  bool                   `json:"generationComplete"`
		InputTranscription  *struct{ Text string } `json:"inputTranscription"`
		OutputTranscription *struct{ Text string } `json:"outputTranscription"`
	} `json:"serverContent"`
	UsageMetadata *json.RawMessage `json:"usageMetadata"`
	GoAway        *json.RawMessage `json:"goAway"`
	ToolCall      *json.RawMessage `json:"toolCall"`
	Error         *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Status  string `json:"status"`
	} `json:"error"`
}

func (s *liveStream) readLoop() {
	defer close(s.events)
	for {
		_, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			s.finish(err)
			return
		}
		var message serverMessage
		if json.Unmarshal(payload, &message) == nil {
			s.handle(message, payload)
		}
	}
}

func (s *liveStream) handle(message serverMessage, raw []byte) {
	if message.SetupComplete != nil {
		s.settleSetup(nil)
	}
	if message.Error != nil {
		code := "provider_rejected_request"
		if message.Error.Code == 401 || message.Error.Code == 403 {
			code = "provider_authentication_failed"
		} else if message.Error.Code == 429 {
			code = "provider_rate_limited"
		} else if message.Error.Code >= 500 {
			code = "provider_unavailable"
		}
		err := &runtimepkg.ProviderError{Code: code, Message: "Gemini Live reported an error", Retryable: message.Error.Code == 429 || message.Error.Code >= 500, ProviderStatus: message.Error.Code, Extensions: extension(raw)}
		s.settleSetup(err)
		s.emit(runtimepkg.ProviderEvent{Err: err})
		return
	}
	if message.GoAway != nil {
		s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Extensions: extension(raw)})
	}
	if message.ToolCall != nil {
		s.emit(runtimepkg.ProviderEvent{Type: protocol.EventToolCall, Extensions: extension(raw)})
	}
	if content := message.ServerContent; content != nil {
		if content.InputTranscription != nil && content.InputTranscription.Text != "" {
			s.emit(runtimepkg.ProviderEvent{Type: protocol.EventTranscriptFinal, Data: marshalData(map[string]string{"text": content.InputTranscription.Text})})
		}
		if content.Interrupted && s.responding.Swap(false) {
			s.emit(runtimepkg.ProviderEvent{Type: protocol.EventResponseCanceled})
		}
		if content.ModelTurn != nil {
			for _, part := range content.ModelTurn.Parts {
				if part.InlineData == nil || part.InlineData.Data == "" {
					continue
				}
				if !s.responding.Swap(true) {
					s.emit(runtimepkg.ProviderEvent{Type: protocol.EventResponseStarted})
				}
				audio, err := base64.StdEncoding.DecodeString(part.InlineData.Data)
				if err == nil && len(audio) > 0 {
					s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Audio: audio})
				}
			}
		}
		if content.OutputTranscription != nil && content.OutputTranscription.Text != "" {
			if !s.responding.Swap(true) {
				s.emit(runtimepkg.ProviderEvent{Type: protocol.EventResponseStarted})
			}
			s.emit(runtimepkg.ProviderEvent{Type: protocol.EventTextDelta, Data: marshalData(map[string]string{"text": content.OutputTranscription.Text})})
		}
		if (content.TurnComplete || content.GenerationComplete) && s.responding.Swap(false) {
			s.emit(runtimepkg.ProviderEvent{Type: protocol.EventResponseDone})
		}
	}
	if message.UsageMetadata != nil {
		s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Extensions: extension(raw)})
	}
}

func (s *liveStream) finish(err error) {
	if s.closed.Load() && (isNormalClose(err) || s.ctx.Err() != nil) || isNormalClose(err) {
		return
	}
	failure := &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Gemini Live closed the session", Retryable: true, Cause: err}
	if status := websocket.CloseStatus(err); status != -1 {
		failure.Extensions = map[string]json.RawMessage{extensionID: marshalData(map[string]any{"close_status": int(status)})}
	}
	s.settleSetup(failure)
	s.emit(runtimepkg.ProviderEvent{Err: failure})
}

func dialError(response *http.Response, err error) error {
	status := 0
	if response != nil {
		status = response.StatusCode
	}
	code := "provider_unavailable"
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		code = "provider_authentication_failed"
	} else if status == http.StatusBadRequest || status == http.StatusNotFound {
		code = "provider_rejected_request"
	} else if status == http.StatusTooManyRequests {
		code = "provider_rate_limited"
	}
	return &runtimepkg.ProviderError{Code: code, Message: "Gemini Live connection could not be established", Retryable: status == 0 || status == http.StatusTooManyRequests || status >= 500, ProviderStatus: status, Cause: err}
}
func extension(raw []byte) map[string]json.RawMessage {
	return map[string]json.RawMessage{extensionID: append(json.RawMessage(nil), raw...)}
}
func marshalData(value any) json.RawMessage { payload, _ := json.Marshal(value); return payload }
func isNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}
