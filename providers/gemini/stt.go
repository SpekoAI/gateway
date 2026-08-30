package gemini

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

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

const (
	// STTAdapterID is the identifier the connector registers this adapter
	// under. It is distinct from google.stt.v1: same modality, different API,
	// different credential.
	STTAdapterID = "gemini.stt.v1"
	// ProviderName is the only provider this package serves.
	ProviderName = "gemini"

	// extensionID namespaces raw vendor payloads attached to canonical events.
	extensionID = "generativelanguage.googleapis.com/v1beta"

	officialHost = "generativelanguage.googleapis.com"
	socketPath   = "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent"

	// APIKeyHeader is the credential channel for the AI Studio surface. Unlike
	// the s2s adapter — which serves provider "google" and therefore defaults
	// to the ADC bearer — every route in this package is AI Studio, so the key
	// header is the default rather than an opt-in.
	APIKeyHeader = "x-goog-api-key"

	// inputSampleRateHz is the only rate the Live API listens at.
	inputSampleRateHz = 16_000

	defaultEventBuffer     = 64
	defaultMaxMessageBytes = 4 << 20
	defaultSetupTimeout    = 15 * time.Second
)

// STTConfig controls local transport bounds. Provider identity, model,
// credential and endpoint always come from the verified plan, never from here.
type STTConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
	// SetupTimeout bounds the wait for setupComplete after the socket opens.
	SetupTimeout time.Duration
	// CredentialHeader selects where the plan's credential is placed. Empty
	// means APIKeyHeader, which is what this surface takes. It is configured
	// rather than inferred from the credential's shape for the reason the s2s
	// adapter documents: key prefixes are not a reliable discriminator, and a
	// key sent as a bearer is closed with 1008 rather than a usable error.
	CredentialHeader string
}

// STTAdapter opens Gemini live-transcription sessions.
type STTAdapter struct {
	id               string
	httpClient       *http.Client
	eventBuffer      int
	maxMessageBytes  int64
	setupTimeout     time.Duration
	credentialHeader string
	endpointPolicy   upstream.WebSocketPolicy
}

// NewSTT validates the configuration and builds the adapter.
func NewSTT(config STTConfig) (*STTAdapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = STTAdapterID
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
	if config.CredentialHeader == "" {
		config.CredentialHeader = APIKeyHeader
	}
	if config.EventBuffer < 1 || config.MaxMessageBytes < 1 || config.SetupTimeout <= 0 {
		return nil, errors.New("gemini transcribe: event buffer, message bound and setup timeout must be positive")
	}
	if config.CredentialHeader != APIKeyHeader && config.CredentialHeader != "authorization" {
		return nil, fmt.Errorf("gemini transcribe: credential header must be %s or authorization, got %q", APIKeyHeader, config.CredentialHeader)
	}
	policy, err := upstream.NewWebSocketPolicy(officialHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &STTAdapter{
		id: config.AdapterID, httpClient: config.HTTPClient, eventBuffer: config.EventBuffer,
		maxMessageBytes: config.MaxMessageBytes, setupTimeout: config.SetupTimeout,
		credentialHeader: config.CredentialHeader,
		endpointPolicy:   policy,
	}, nil
}

// ID returns the adapter identifier.
func (a *STTAdapter) ID() string { return a.id }

// Open dials the socket, sends setup, and returns once the service has
// acknowledged it — so the first WriteAudio after Open is always accepted.
// The returned stream's first event is session.ready.
func (a *STTAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindSTT {
		return nil, fmt.Errorf("gemini transcribe supports stt sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != ProviderName {
		return nil, fmt.Errorf("gemini transcribe adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("gemini transcribe requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model == "" {
		return nil, errors.New("gemini transcribe requires a model")
	}
	if request.Media == nil {
		return nil, errors.New("gemini transcribe requires input media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("gemini transcribe input media: %w", err)
	}
	if request.Media.Encoding != "pcm_s16le" || request.Media.Channels != 1 || request.Media.SampleRateHz != inputSampleRateHz {
		return nil, fmt.Errorf("gemini transcribe listens to mono pcm_s16le at %d Hz only, got %s/%d Hz/%d channels",
			inputSampleRateHz, request.Media.Encoding, request.Media.SampleRateHz, request.Media.Channels)
	}
	credential := request.Plan.Route.Credential
	if credential == nil || strings.TrimSpace(credential.Value) == "" ||
		(credential.Kind != protocol.CredentialBearer && !(request.Plan.Execution.ProviderRoute == protocol.RouteSpekoRelay && credential.Kind == protocol.CredentialRelayAccess)) {
		return nil, errors.New("gemini transcribe requires a bearer credential")
	}
	endpoint, err := a.parseEndpoint(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("gemini transcribe endpoint: %w", err)
	}

	headers := make(http.Header)
	if a.credentialHeader == APIKeyHeader {
		headers.Set(APIKeyHeader, credential.Value)
	} else {
		headers.Set("Authorization", "Bearer "+credential.Value)
	}
	conn, response, err := websocket.Dial(ctx, endpoint.String(), &websocket.DialOptions{HTTPClient: a.httpClient, HTTPHeader: headers})
	if err != nil {
		return nil, dialError(response, err)
	}
	conn.SetReadLimit(a.maxMessageBytes)

	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &sttStream{
		conn: conn, ctx: streamCtx, cancel: cancel,
		events:      make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		inputMIME:   fmt.Sprintf("audio/pcm;rate=%d", request.Media.SampleRateHz),
		setupDone:   make(chan error, 1),
		vendorModel: "models/" + model,
	}
	if err := stream.writeJSON(ctx, buildSTTSetup(stream.vendorModel, request.Options)); err != nil {
		stream.abort()
		return nil, err
	}
	go stream.readLoop()

	setupCtx, cancelSetup := context.WithTimeout(ctx, a.setupTimeout)
	defer cancelSetup()
	select {
	case err := <-stream.setupDone:
		if err != nil {
			stream.abort()
			return nil, err
		}
	case <-setupCtx.Done():
		stream.abort()
		return nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Gemini transcribe did not acknowledge setup", Retryable: true, Cause: setupCtx.Err()}
	}
	stream.emit(runtimepkg.ProviderEvent{Type: protocol.EventSessionReady, Data: marshalData(map[string]string{"provider_request_id": ""})})
	return stream, nil
}

// parseEndpoint applies the shared upstream allowlist and then pins the path.
// The path check is this adapter's own: the policy validates scheme, host,
// port and cleanliness, but only this package knows which RPC on that host it
// is allowed to dial.
func (a *STTAdapter) parseEndpoint(raw string) (*url.URL, error) {
	endpoint, err := a.endpointPolicy.Parse(raw)
	if err != nil {
		return nil, err
	}
	if endpoint.Path != socketPath {
		return nil, fmt.Errorf("endpoint path must be %s, got %q", socketPath, endpoint.Path)
	}
	return endpoint, nil
}

// buildSTTSetup is the first client message.
//
// responseModalities is TEXT: this is a transcription session, and leaving the
// default (AUDIO) would make the model speak an answer the caller is neither
// asking for nor being billed for. The transcript itself is requested through
// inputAudioTranscription, which is the Live API's documented transcription
// channel; outputAudioTranscription is deliberately absent, because there is
// no model audio to transcribe.
//
// Only the AudioTranscriptionConfig fields the v1beta discovery document
// publishes as current are sent. languageHints, languageAuto and
// adaptationPhrases are marked deprecated there and are omitted rather than
// mirrored, so a caller's language ask rides languageCodes alone.
func buildSTTSetup(vendorModel string, options protocol.RequestOptions) map[string]any {
	transcription := map[string]any{}
	if language := strings.TrimSpace(options.Language); language != "" {
		transcription["languageCodes"] = []string{language}
	}
	if options.STT != nil {
		if options.STT.Diarization != nil {
			transcription["diarization"] = *options.STT.Diarization
		}
		if keywords := trimmedKeywords(options.STT.Keywords); len(keywords) > 0 {
			transcription["customVocabulary"] = keywords
		}
	}
	return map[string]any{"setup": map[string]any{
		"model":                   vendorModel,
		"generationConfig":        map[string]any{"responseModalities": []string{"TEXT"}},
		"inputAudioTranscription": transcription,
	}}
}

func trimmedKeywords(keywords []string) []string {
	trimmed := make([]string, 0, len(keywords))
	for _, keyword := range keywords {
		if keyword = strings.TrimSpace(keyword); keyword != "" {
			trimmed = append(trimmed, keyword)
		}
	}
	return trimmed
}

// sttStream is one open Gemini live-transcription socket.
type sttStream struct {
	conn        *websocket.Conn
	ctx         context.Context
	cancel      context.CancelFunc
	events      chan runtimepkg.ProviderEvent
	inputMIME   string
	vendorModel string
	setupDone   chan error

	writeMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	closeErr     error

	setupOnce sync.Once

	// turn state. Gemini streams transcription in fragments and marks the end
	// of a turn; the adapter publishes each fragment as a delta and the joined
	// text as the turn's final, which is the shape every other relay STT
	// adapter presents.
	turnMu       sync.Mutex
	pending      strings.Builder
	sawInputText bool

	terminalMu  sync.Mutex
	terminalErr error
}

func (s *sttStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

func (s *sttStream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("gemini transcribe audio is empty")
	}
	return s.writeJSON(ctx, map[string]any{"realtimeInput": map[string]any{"audio": map[string]string{
		"data": base64.StdEncoding.EncodeToString(audio), "mimeType": s.inputMIME,
	}}})
}

// CommitAudio tells the service the caller's audio stream paused. Gemini's own
// activity detection still decides the turn; this only flushes the buffer.
func (s *sttStream) CommitAudio(ctx context.Context) error {
	return s.writeJSON(ctx, map[string]any{"realtimeInput": map[string]any{"audioStreamEnd": true}})
}

func (s *sttStream) AppendText(context.Context, string) error {
	return runtimepkg.ErrUnsupportedOperation
}

func (s *sttStream) CommitText(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// Cancel has no wire message on this socket and nothing local to suppress: a
// transcription session produces no model answer to interrupt. Flushing what
// has accumulated would invent a final the service never marked, so the call
// is accepted and does nothing.
func (s *sttStream) Cancel(context.Context) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	return nil
}

func (s *sttStream) Close(context.Context) error {
	s.gracefulOnce.Do(func() {
		s.closed.Store(true)
		s.writeMu.Lock()
		s.closeErr = s.conn.Close(websocket.StatusNormalClosure, "")
		s.writeMu.Unlock()
		s.cancel()
	})
	return s.closeErr
}

func (s *sttStream) Abort(context.Context) error { return s.abort() }

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

// TerminalError preserves the failure that ended the socket, independently of
// the bounded event queue.
func (s *sttStream) TerminalError() error {
	s.terminalMu.Lock()
	defer s.terminalMu.Unlock()
	return s.terminalErr
}

func (s *sttStream) setTerminal(err error) {
	s.terminalMu.Lock()
	defer s.terminalMu.Unlock()
	if s.terminalErr == nil {
		s.terminalErr = err
	}
}

func (s *sttStream) writeJSON(ctx context.Context, value any) error {
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
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Gemini transcribe socket write failed", Retryable: true, Cause: err}
	}
	return nil
}

func (s *sttStream) emit(event runtimepkg.ProviderEvent) {
	if event.Err != nil {
		s.setTerminal(event.Err)
	}
	select {
	case s.events <- event:
	case <-s.ctx.Done():
	}
}

func (s *sttStream) settleSetup(err error) {
	s.setupOnce.Do(func() { s.setupDone <- err })
}

// sttServerMessage is the subset of BidiGenerateContentServerMessage this
// adapter reads.
type sttServerMessage struct {
	SetupComplete *json.RawMessage `json:"setupComplete"`
	ServerContent *struct {
		ModelTurn *struct {
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"modelTurn"`
		TurnComplete       bool `json:"turnComplete"`
		GenerationComplete bool `json:"generationComplete"`
		InputTranscription *struct {
			Text string `json:"text"`
		} `json:"inputTranscription"`
	} `json:"serverContent"`
	UsageMetadata *json.RawMessage `json:"usageMetadata"`
	GoAway        *json.RawMessage `json:"goAway"`
	Error         *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Status  string `json:"status"`
	} `json:"error"`
}

func (s *sttStream) readLoop() {
	defer close(s.events)
	for {
		// The service answers with BINARY frames whose payload is JSON while
		// the documented examples show text; both are accepted, for the reason
		// the s2s adapter records — skipping binary frames made it wait out
		// its setup timeout against the real service.
		_, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			s.finish(err)
			return
		}
		var message sttServerMessage
		if err := json.Unmarshal(payload, &message); err != nil {
			continue
		}
		s.handle(message, payload)
	}
}

// handle dispatches one server message. Fields are checked INDEPENDENTLY
// rather than as alternatives: the service packs usageMetadata into the same
// message as the turn's serverContent, and treating them as a switch swallows
// the turn's end.
func (s *sttStream) handle(message sttServerMessage, raw []byte) {
	if message.SetupComplete != nil {
		s.settleSetup(nil)
	}
	if message.Error != nil {
		err := &runtimepkg.ProviderError{
			Code: "provider_rejected_request", Message: "Gemini transcribe reported an error",
			Retryable: message.Error.Code == 429 || message.Error.Code >= 500, ProviderStatus: message.Error.Code,
			Extensions: extension(raw),
		}
		s.settleSetup(err)
		s.emit(runtimepkg.ProviderEvent{Err: err})
		return
	}
	if message.GoAway != nil {
		s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: marshalData(map[string]string{"text": "the provider announced the session will end soon"}), Extensions: extension(raw)})
	}
	if message.ServerContent != nil {
		s.handleContent(message, raw)
	}
	if message.UsageMetadata != nil {
		s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: marshalData(map[string]string{"provider_request_id": ""}), Extensions: extension(raw)})
	}
}

func (s *sttStream) handleContent(message sttServerMessage, raw []byte) {
	content := message.ServerContent
	if content.InputTranscription != nil && content.InputTranscription.Text != "" {
		s.appendFragment(content.InputTranscription.Text, raw, true)
	}
	// A transcription model asked for TEXT may answer with the transcript as
	// its model turn rather than as an input transcription. Those parts are
	// read only while no inputTranscription has ever arrived on this stream:
	// once the service has shown it uses the dedicated channel, treating model
	// text as transcript too would publish every fragment twice.
	if content.ModelTurn != nil {
		for _, part := range content.ModelTurn.Parts {
			if part.Text == "" || s.inputChannelActive() {
				continue
			}
			s.appendFragment(part.Text, raw, false)
		}
	}
	// generationComplete and turnComplete both end a transcription turn: the
	// first says the model stopped generating, the second that the turn is
	// over, and a session that only ever saw one of them must still publish
	// its final. flushTurn is idempotent, so the pair is safe.
	if content.TurnComplete || content.GenerationComplete {
		s.flushTurn(raw)
	}
}

func (s *sttStream) inputChannelActive() bool {
	s.turnMu.Lock()
	defer s.turnMu.Unlock()
	return s.sawInputText
}

// appendFragment publishes one incremental transcript fragment and retains it
// for the turn's final.
func (s *sttStream) appendFragment(text string, raw []byte, fromInputChannel bool) {
	s.turnMu.Lock()
	if fromInputChannel {
		s.sawInputText = true
	}
	s.pending.WriteString(text)
	s.turnMu.Unlock()
	s.emit(runtimepkg.ProviderEvent{
		Type:       protocol.EventTranscriptDelta,
		Data:       transcriptData(text, false),
		Extensions: extension(raw),
	})
}

// flushTurn publishes the joined text of the turn as its final. A turn that
// carried no text publishes nothing: an empty final is indistinguishable from
// silence to a caller.
func (s *sttStream) flushTurn(raw []byte) {
	s.turnMu.Lock()
	text := strings.TrimSpace(s.pending.String())
	s.pending.Reset()
	s.turnMu.Unlock()
	if text == "" {
		return
	}
	s.emit(runtimepkg.ProviderEvent{
		Type:       protocol.EventTranscriptFinal,
		Data:       transcriptData(text, true),
		Extensions: extension(raw),
	})
}

func transcriptData(text string, isFinal bool) json.RawMessage {
	return marshalData(map[string]any{"text": text, "is_final": isFinal, "provider_request_id": ""})
}

// finish classifies how the socket ended. A clean close after the caller
// closed is normal; anything else is a provider failure whose close code is
// the only diagnostic the service offers.
func (s *sttStream) finish(err error) {
	if isNormalClose(err) || (s.closed.Load() && s.ctx.Err() != nil) {
		return
	}
	status := websocket.CloseStatus(err)
	failure := &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Gemini transcribe closed the session", Retryable: true, Cause: err}
	switch status {
	case websocket.StatusPolicyViolation, websocket.StatusUnsupportedData, websocket.StatusInvalidFramePayloadData:
		failure = &runtimepkg.ProviderError{Code: "provider_rejected_request", Message: "Gemini transcribe rejected the session configuration", Retryable: false, Cause: err}
	case websocket.StatusInternalError:
		failure = &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Gemini transcribe reported an internal error", Retryable: true, Cause: err}
	}
	if status != -1 {
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
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		code = "provider_authentication_failed"
	case status == http.StatusBadRequest || status == http.StatusNotFound:
		code = "provider_rejected_request"
	case status == http.StatusTooManyRequests:
		code = "provider_rate_limited"
	}
	return &runtimepkg.ProviderError{
		Code: code, Message: "Gemini transcribe connection could not be established",
		Retryable: status == 0 || status == http.StatusTooManyRequests || status >= 500, ProviderStatus: status, Cause: err,
	}
}

func extension(raw []byte) map[string]json.RawMessage {
	return map[string]json.RawMessage{extensionID: append(json.RawMessage(nil), raw...)}
}

func marshalData(value any) json.RawMessage {
	payload, _ := json.Marshal(value)
	return payload
}

func isNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}
