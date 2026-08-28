// Package openairealtime implements provider-direct speech-to-speech sessions
// for OpenAI Realtime and xAI Grok Voice. Credentials and endpoints come only
// from verified SessionPlans; the adapter never holds a permanent provider key.
package openairealtime

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
	AdapterIDOpenAI = "openai.realtime.v1"
	AdapterIDXAI    = "xai.realtime.v1"

	appendChunkBytes       = 24_000
	defaultEventBuffer     = 64
	defaultMaxMessageBytes = 4 << 20
	defaultSetupTimeout    = 15 * time.Second
)

type providerProfile struct {
	adapterID     string
	host          string
	path          string
	extensionID   string
	hybridSession bool
}

var profiles = map[string]providerProfile{
	"openai": {adapterID: AdapterIDOpenAI, host: "api.openai.com", path: "/v1/realtime", extensionID: "openai.com/realtime/v1"},
	"xai":    {adapterID: AdapterIDXAI, host: "api.x.ai", path: "/v1/realtime", extensionID: "x.ai/realtime/v1", hybridSession: true},
}

type Config struct {
	Provider              string
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
	SetupTimeout          time.Duration
}

type Adapter struct {
	provider        string
	profile         providerProfile
	id              string
	httpClient      *http.Client
	eventBuffer     int
	maxMessageBytes int64
	setupTimeout    time.Duration
	hosts           map[string]struct{}
	allowInsecure   bool
}

func New(config Config) (*Adapter, error) {
	profile, ok := profiles[config.Provider]
	if !ok {
		return nil, fmt.Errorf("openai realtime: unsupported provider %q", config.Provider)
	}
	if config.AdapterID == "" {
		config.AdapterID = profile.adapterID
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
		return nil, errors.New("openai realtime: event buffer, message bound and setup timeout must be positive")
	}
	hosts := map[string]struct{}{profile.host: {}}
	for _, host := range config.AllowedEndpointHosts {
		host = strings.ToLower(strings.TrimSpace(host))
		if host == "" || strings.ContainsAny(host, "/:@?#") {
			return nil, errors.New("openai realtime: allowed endpoint host is invalid")
		}
		hosts[host] = struct{}{}
	}
	return &Adapter{
		provider: config.Provider, profile: profile, id: config.AdapterID,
		httpClient: config.HTTPClient, eventBuffer: config.EventBuffer,
		maxMessageBytes: config.MaxMessageBytes, setupTimeout: config.SetupTimeout,
		hosts: hosts, allowInsecure: config.AllowInsecureEndpoint,
	}, nil
}

func (a *Adapter) ID() string { return a.id }

// Open establishes the provider socket from the signed direct route and waits
// for session.updated. The Engine emits the canonical session.ready event after
// this method returns; the adapter does not create a second ready event.
func (a *Adapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindRealtime {
		return nil, fmt.Errorf("%s realtime supports realtime sessions, got %q", a.provider, request.Kind)
	}
	if request.Plan.Execution.ProviderRoute != protocol.RouteProviderDirect {
		return nil, fmt.Errorf("%s realtime requires a provider-direct route", a.provider)
	}
	if request.Plan.Route.Provider != a.provider {
		return nil, fmt.Errorf("%s realtime adapter cannot open provider %q", a.provider, request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("%s realtime requires websocket transport, got %q", a.provider, request.Plan.Route.Transport)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model == "" {
		return nil, fmt.Errorf("%s realtime requires a model", a.provider)
	}
	if request.Media == nil {
		return nil, fmt.Errorf("%s realtime requires input media configuration", a.provider)
	}
	if err := validatePCM(a.provider, "input", *request.Media); err != nil {
		return nil, err
	}
	if request.Options.S2S == nil || request.Options.S2S.OutputMedia == nil {
		return nil, fmt.Errorf("%s realtime requires output media configuration", a.provider)
	}
	output := request.Options.S2S.OutputMedia
	if err := validatePCM(a.provider, "output", *output); err != nil {
		return nil, err
	}
	credential := request.Plan.Route.Credential
	if credential == nil || credential.Kind != protocol.CredentialBearer || strings.TrimSpace(credential.Value) == "" {
		return nil, fmt.Errorf("%s realtime requires a delegated bearer credential", a.provider)
	}
	endpoint, err := a.parseEndpoint(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("%s realtime endpoint: %w", a.provider, err)
	}
	query := endpoint.Query()
	query.Set("model", model)
	endpoint.RawQuery = query.Encode()

	headers := make(http.Header)
	headers.Set("Authorization", "Bearer "+credential.Value)
	conn, response, err := websocket.Dial(ctx, endpoint.String(), &websocket.DialOptions{HTTPClient: a.httpClient, HTTPHeader: headers})
	if err != nil {
		return nil, dialError(a.provider, response, err)
	}
	conn.SetReadLimit(a.maxMessageBytes)

	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &realtimeStream{
		provider: a.provider, profile: a.profile, conn: conn, ctx: streamCtx, cancel: cancel,
		events: make(chan runtimepkg.ProviderEvent, a.eventBuffer), setupDone: make(chan error, 1),
	}
	if err := stream.writeJSON(ctx, buildSessionUpdate(a.profile, model, request.Media.SampleRateHz, output.SampleRateHz, request.Options)); err != nil {
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
		return nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: a.provider + " realtime did not acknowledge session.update", Retryable: true, Cause: setupCtx.Err()}
	}
	return stream, nil
}

func validatePCM(provider, direction string, media protocol.MediaFormat) error {
	if err := media.Validate(); err != nil {
		return fmt.Errorf("%s realtime %s media: %w", provider, direction, err)
	}
	if media.Encoding != "pcm_s16le" || media.Channels != 1 || media.SampleRateHz != 24_000 {
		return &runtimepkg.ProviderError{
			Code: "unsupported_media", Message: fmt.Sprintf("%s realtime requires 24 kHz mono pcm_s16le %s, got %s/%d Hz/%d channels", provider, direction, media.Encoding, media.SampleRateHz, media.Channels),
			Hint: "Convert audio to 24 kHz mono pcm_s16le before opening the realtime session.",
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
	if endpoint.Path != a.profile.path {
		return nil, fmt.Errorf("endpoint path must be %s, got %q", a.profile.path, endpoint.Path)
	}
	return endpoint, nil
}

func buildSessionUpdate(profile providerProfile, model string, inputHz, outputHz int, options protocol.RequestOptions) map[string]any {
	instructions := ""
	if options.S2S != nil {
		instructions = options.S2S.Instructions
	}
	voice := strings.TrimSpace(options.Voice)
	inputFormat := map[string]any{"type": "audio/pcm", "rate": inputHz}
	outputFormat := map[string]any{"type": "audio/pcm", "rate": outputHz}
	session := map[string]any{
		"type": "realtime", "model": model, "instructions": instructions,
		"output_modalities": []string{"audio"},
	}
	if profile.hybridSession {
		session["turn_detection"] = map[string]any{"type": "server_vad"}
		if voice != "" {
			session["voice"] = voice
		}
		session["audio"] = map[string]any{
			"input": map[string]any{
				"format": inputFormat, "transcription": map[string]any{"model": "grok-transcribe"},
			},
			"output": map[string]any{"format": outputFormat},
		}
	} else {
		outputAudio := map[string]any{"format": outputFormat}
		if voice != "" {
			outputAudio["voice"] = voice
		}
		session["audio"] = map[string]any{
			"input": map[string]any{
				"format": inputFormat, "transcription": map[string]any{"model": "whisper-1"},
				"turn_detection": map[string]any{"type": "server_vad"},
			},
			"output": outputAudio,
		}
	}
	return map[string]any{"type": "session.update", "session": session}
}

type realtimeStream struct {
	provider  string
	profile   providerProfile
	conn      *websocket.Conn
	ctx       context.Context
	cancel    context.CancelFunc
	events    chan runtimepkg.ProviderEvent
	setupDone chan error

	writeMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	closeErr     error
	setupOnce    sync.Once
	ready        atomic.Bool
	responding   atomic.Bool

	terminalMu  sync.Mutex
	terminalErr error
}

func (s *realtimeStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

func (s *realtimeStream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return fmt.Errorf("%s realtime audio is empty", s.provider)
	}
	for offset := 0; offset < len(audio); offset += appendChunkBytes {
		end := min(offset+appendChunkBytes, len(audio))
		if err := s.writeJSON(ctx, map[string]any{"type": "input_audio_buffer.append", "audio": base64.StdEncoding.EncodeToString(audio[offset:end])}); err != nil {
			return err
		}
	}
	return nil
}

func (s *realtimeStream) CommitAudio(ctx context.Context) error {
	if err := s.writeJSON(ctx, map[string]any{"type": "input_audio_buffer.commit"}); err != nil {
		return err
	}
	if s.responding.Load() {
		return nil
	}
	return s.writeJSON(ctx, map[string]any{"type": "response.create"})
}

func (s *realtimeStream) AppendText(context.Context, string) error {
	return runtimepkg.ErrUnsupportedOperation
}

func (s *realtimeStream) CommitText(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

func (s *realtimeStream) Cancel(ctx context.Context) error {
	return s.writeJSON(ctx, map[string]any{"type": "response.cancel"})
}

func (s *realtimeStream) Close(context.Context) error {
	s.gracefulOnce.Do(func() {
		s.closed.Store(true)
		s.writeMu.Lock()
		s.closeErr = s.conn.Close(websocket.StatusNormalClosure, "")
		s.writeMu.Unlock()
		s.cancel()
	})
	return s.closeErr
}

func (s *realtimeStream) Abort(context.Context) error { return s.abort() }

func (s *realtimeStream) abort() error {
	s.abortOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		if err := s.conn.CloseNow(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
	})
	return s.closeErr
}

func (s *realtimeStream) TerminalError() error {
	s.terminalMu.Lock()
	defer s.terminalMu.Unlock()
	return s.terminalErr
}

func (s *realtimeStream) setTerminal(err error) {
	s.terminalMu.Lock()
	defer s.terminalMu.Unlock()
	if s.terminalErr == nil {
		s.terminalErr = err
	}
}

func (s *realtimeStream) writeJSON(ctx context.Context, value any) error {
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
		if s.closed.Load() {
			return runtimepkg.ErrSessionClosed
		}
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: s.provider + " realtime socket write failed", Retryable: true, Cause: err}
	}
	return nil
}

func (s *realtimeStream) emit(event runtimepkg.ProviderEvent) {
	if event.Err != nil {
		s.setTerminal(event.Err)
	}
	select {
	case s.events <- event:
	case <-s.ctx.Done():
	}
}

func (s *realtimeStream) settleSetup(err error) {
	s.setupOnce.Do(func() {
		if err == nil {
			s.ready.Store(true)
		}
		s.setupDone <- err
	})
}

type serverEvent struct {
	Type       string `json:"type"`
	Delta      string `json:"delta"`
	Transcript string `json:"transcript"`
	Response   *struct {
		ID     string          `json:"id"`
		Status string          `json:"status"`
		Usage  json.RawMessage `json:"usage"`
	} `json:"response"`
	Error *struct {
		Type    string `json:"type"`
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (s *realtimeStream) readLoop() {
	defer close(s.events)
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			s.finish(err)
			return
		}
		if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
			continue
		}
		var event serverEvent
		if json.Unmarshal(payload, &event) != nil {
			continue
		}
		s.handle(event, payload)
	}
}

func (s *realtimeStream) handle(event serverEvent, raw []byte) {
	switch event.Type {
	case "session.updated":
		s.settleSetup(nil)
	case "session.created":
	case "error":
		s.handleError(event, raw)
	case "input_audio_buffer.speech_started":
		s.emit(runtimepkg.ProviderEvent{Type: protocol.EventSpeechStarted})
	case "input_audio_buffer.speech_stopped":
		s.emit(runtimepkg.ProviderEvent{Type: protocol.EventSpeechEnded})
	case "conversation.item.input_audio_transcription.delta":
		if event.Delta != "" {
			s.emit(runtimepkg.ProviderEvent{Type: protocol.EventTranscriptDelta, Data: marshalData(map[string]string{"text": event.Delta})})
		}
	case "conversation.item.input_audio_transcription.updated":
		if event.Transcript != "" {
			s.emit(runtimepkg.ProviderEvent{Type: protocol.EventTranscriptDelta, Data: marshalData(map[string]string{"text": event.Transcript})})
		}
	case "conversation.item.input_audio_transcription.completed":
		s.emit(runtimepkg.ProviderEvent{Type: protocol.EventTranscriptFinal, Data: marshalData(map[string]string{"text": event.Transcript})})
	case "response.created":
		s.responding.Store(true)
		s.emit(runtimepkg.ProviderEvent{Type: protocol.EventResponseStarted})
	case "response.output_audio.delta", "response.audio.delta":
		audio, err := base64.StdEncoding.DecodeString(event.Delta)
		if err == nil && len(audio) > 0 {
			s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Audio: audio})
		}
	case "response.output_audio_transcript.delta", "response.audio_transcript.delta":
		if event.Delta != "" {
			s.emit(runtimepkg.ProviderEvent{Type: protocol.EventTextDelta, Data: marshalData(map[string]string{"text": event.Delta})})
		}
	case "response.done":
		s.responding.Store(false)
		if event.Response != nil && len(event.Response.Usage) > 0 {
			s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: marshalData(map[string]string{"provider_request_id": event.Response.ID}), Extensions: s.extension(event.Response.Usage)})
		}
		if event.Response != nil && event.Response.Status == "cancelled" {
			s.emit(runtimepkg.ProviderEvent{Type: protocol.EventResponseCanceled})
			return
		}
		s.emit(runtimepkg.ProviderEvent{Type: protocol.EventResponseDone})
	}
}

func (s *realtimeStream) handleError(event serverEvent, raw []byte) {
	code, kind := "", ""
	if event.Error != nil {
		code, kind = event.Error.Code, event.Error.Type
	}
	fatal := !s.ready.Load() || kind == "server_error" || code == "session_expired" || code == "invalid_api_key" || code == "insufficient_quota" || code == "rate_limit_exceeded"
	if !fatal {
		s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Extensions: s.extension(raw)})
		return
	}
	stable := "provider_rejected_request"
	retryable := false
	switch {
	case code == "invalid_api_key" || kind == "authentication_error":
		stable = "provider_authentication_failed"
	case code == "rate_limit_exceeded" || code == "insufficient_quota":
		stable, retryable = "provider_rate_limited", true
	case kind == "server_error" || code == "session_expired":
		stable, retryable = "provider_unavailable", true
	}
	err := &runtimepkg.ProviderError{Code: stable, Message: s.provider + " realtime reported an error", Retryable: retryable, Extensions: s.extension(raw)}
	s.settleSetup(err)
	s.emit(runtimepkg.ProviderEvent{Err: err})
}

func (s *realtimeStream) finish(err error) {
	if s.closed.Load() && (isNormalClose(err) || s.ctx.Err() != nil) {
		return
	}
	if isNormalClose(err) {
		return
	}
	failure := &runtimepkg.ProviderError{Code: "provider_unavailable", Message: s.provider + " realtime closed the session", Retryable: true, Cause: err}
	if status := websocket.CloseStatus(err); status != -1 {
		failure.Extensions = map[string]json.RawMessage{s.profile.extensionID: marshalData(map[string]any{"close_status": int(status)})}
	}
	s.settleSetup(failure)
	s.emit(runtimepkg.ProviderEvent{Err: failure})
}

func (s *realtimeStream) extension(raw []byte) map[string]json.RawMessage {
	return map[string]json.RawMessage{s.profile.extensionID: append(json.RawMessage(nil), raw...)}
}

func dialError(provider string, response *http.Response, err error) error {
	status := 0
	if response != nil {
		status = response.StatusCode
	}
	code := "provider_unavailable"
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		code = "provider_authentication_failed"
	case status == http.StatusBadRequest || status == http.StatusNotFound:
		code = "provider_rejected_request"
	case status == http.StatusTooManyRequests:
		code = "provider_rate_limited"
	}
	return &runtimepkg.ProviderError{
		Code: code, Message: provider + " realtime connection could not be established",
		Retryable:      status == 0 || status == http.StatusTooManyRequests || status >= 500,
		ProviderStatus: status, Cause: err,
	}
}

func marshalData(value any) json.RawMessage {
	payload, _ := json.Marshal(value)
	return payload
}

func isNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}
