package xai

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

const (
	// AdapterID is the identifier returned by an xAI TTS session plan.
	AdapterID = "xai.tts.v1"
	// extensionID namespaces raw xAI payloads carried on canonical events.
	extensionID = "x.ai/v1"

	officialAPIHost = "api.x.ai"
	// ttsPath is shared by both surfaces: POST https://api.x.ai/v1/tts and
	// GET (upgrade) wss://api.x.ai/v1/tts. CONFIRMED in xAI's TTS guide.
	ttsPath = "/v1/tts"

	// DefaultModel is a Speko catalog key, NOT an xAI model id. xAI's TTS wire
	// protocol has no model field at all — the voice is selected by voice_id and
	// the engine is implied by the endpoint — and xAI publishes no TTS model
	// slug anywhere (the models page lists only a price, "$15.00 / 1M chars").
	// The gateway still needs a concrete, non-"auto" model in a signed plan, so
	// this string exists purely to satisfy that contract and to key telemetry.
	// It is deliberately never sent upstream.
	DefaultModel = "tts"
	// DefaultVoice matches xAI's documented default for an omitted voice_id.
	// The adapter always sends a voice explicitly so the wire stays deterministic.
	DefaultVoice = "eve"

	// maxTextCharacters is xAI's documented limit. On the unary endpoint it caps
	// the whole request; on the WebSocket it caps each individual text.delta.
	maxTextCharacters = 15_000

	// pcmCodec is the only codec this adapter can emit: the canonical protocol
	// admits pcm_s16le or opus, and xAI's TTS codec enum
	// (mp3|wav|pcm|mulaw|alaw) has no opus member. xAI documents pcm as 16-bit
	// little-endian, which is exactly pcm_s16le.
	pcmCodec = "pcm"
)

// documentedSampleRates is xAI's published sample_rate enum for TTS.
var documentedSampleRates = map[int]struct{}{
	8_000: {}, 16_000: {}, 22_050: {}, 24_000: {}, 44_100: {}, 48_000: {},
}

// Config controls bounded transport state and the latency/quality tradeoff that
// applies to both surfaces. Provider identity, model, voice, language, and the
// access token all come from a session plan and its provider-neutral options.
type Config struct {
	AdapterID  string
	HTTPClient *http.Client
	// EventBuffer bounds the adapter-owned event channel.
	EventBuffer int
	// MaxMessageBytes bounds one inbound WebSocket message.
	MaxMessageBytes int64
	// AudioChunkBytes is the read size used to slice the unary HTTP response
	// body into audio frames. It has no wire meaning; it only bounds how much
	// audio the adapter holds before emitting a frame.
	AudioChunkBytes int
	// MaxErrorBytes bounds how much of a failed HTTP response body is retained
	// for the canonical error event.
	MaxErrorBytes int64
	// OptimizeStreamingLatency maps to xAI's documented integer knob:
	// 0 = best quality, 1 = smaller first chunk, 2 = smallest first chunk.
	// nil selects 1, because this gateway exists to carry realtime voice and
	// time-to-first-audio is the metric that matters there.
	OptimizeStreamingLatency *int
	AllowedEndpointHosts     []string
	AllowInsecureEndpoint    bool
}

// Adapter implements both of xAI's documented TTS surfaces behind one adapter
// id, selected by the transport in the signed route:
//
//   - protocol.TransportWebSocket -> wss://api.x.ai/v1/tts, the bidirectional
//     streaming surface (text.delta/text.done in, audio.delta/audio.done out).
//     This is the preferred route: audio starts flowing before the utterance is
//     complete, one socket serves many turns, and cancellation is a real wire
//     message rather than a dropped connection.
//   - protocol.TransportHTTP -> POST https://api.x.ai/v1/tts, the unary surface.
//     Retained because it has no concurrent-session cap (xAI limits the
//     WebSocket to 50 concurrent sessions per team) and because a signed plan
//     may legitimately choose it for long-form, non-interactive synthesis.
type Adapter struct {
	id              string
	httpClient      *http.Client
	eventBuffer     int
	maxMessageBytes int64
	audioChunkBytes int
	maxErrorBytes   int64
	latency         int
	socketPolicy    upstream.WebSocketPolicy
	httpPolicy      httpPolicy
}

// New creates a bounded xAI TTS adapter.
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
	if config.AudioChunkBytes == 0 {
		config.AudioChunkBytes = 8 << 10
	}
	if config.MaxErrorBytes == 0 {
		config.MaxErrorBytes = 8 << 10
	}
	if config.EventBuffer < 1 {
		return nil, errors.New("xai event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("xai maximum message bytes must be positive")
	}
	if config.AudioChunkBytes < 1 {
		return nil, errors.New("xai audio chunk bytes must be positive")
	}
	if config.MaxErrorBytes < 1 {
		return nil, errors.New("xai maximum error bytes must be positive")
	}
	latency := 1
	if config.OptimizeStreamingLatency != nil {
		latency = *config.OptimizeStreamingLatency
	}
	if latency < 0 || latency > 2 {
		return nil, errors.New("xai optimize_streaming_latency must be 0, 1, or 2")
	}
	socketPolicy, err := upstream.NewWebSocketPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	httpEndpointPolicy, err := newHTTPPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &Adapter{
		id:              config.AdapterID,
		httpClient:      config.HTTPClient,
		eventBuffer:     config.EventBuffer,
		maxMessageBytes: config.MaxMessageBytes,
		audioChunkBytes: config.AudioChunkBytes,
		maxErrorBytes:   config.MaxErrorBytes,
		latency:         latency,
		socketPolicy:    socketPolicy,
		httpPolicy:      httpEndpointPolicy,
	}, nil
}

func (a *Adapter) ID() string { return a.id }

// Open validates the plan and opens whichever xAI surface the route selected.
func (a *Adapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindTTS {
		return nil, fmt.Errorf("xai tts supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "xai" {
		return nil, fmt.Errorf("xai adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Media == nil {
		return nil, errors.New("xai tts requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("xai tts media: %w", err)
	}
	options, err := validateGenerationOptions(request.Plan.Route.Model, request.Options, *request.Media, a.latency)
	if err != nil {
		return nil, err
	}
	credential, err := bearerCredential(request.Plan)
	if err != nil {
		return nil, err
	}
	switch request.Plan.Route.Transport {
	case protocol.TransportWebSocket:
		return a.openSocket(ctx, request, options, credential)
	case protocol.TransportHTTP:
		return a.openUnary(request, options, credential)
	default:
		return nil, fmt.Errorf("xai tts requires websocket or http transport, got %q", request.Plan.Route.Transport)
	}
}

// bearerCredential resolves the single access token xAI accepts.
//
// This switch looks redundant on purpose. xAI documents exactly ONE
// server-side authentication channel for both credential sources: an
// `Authorization: Bearer <token>` header. Its ephemeral client secret
// (POST /v1/realtime/client_secrets) is documented as usable "in the same
// fashion as an API key", so a managed plan differs from a BYOK plan only in
// the lifetime of the string, never in where the string is placed. A relay
// plan differs only in who owns the string — the relay connector's permanent
// xAI key rides the same header, never a URL. The one documented alternative
// channel — the `xai-client-secret.<token>` sec-websocket-protocol prefix —
// exists solely because browsers cannot set request headers, and must not be
// used from a server-side gateway.
//
// Contrast providers/elevenlabs/stt.go, where BYOK and managed genuinely take
// different channels. Copying that split to xAI would invent a wire detail.
func bearerCredential(plan protocol.SessionPlan) (string, error) {
	credential := plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return "", errors.New("xai tts requires a bearer credential")
	}
	switch plan.Execution.CredentialSource {
	case protocol.CredentialsBYOK, protocol.CredentialsManaged:
		return credential.Value, nil
	default:
		return "", fmt.Errorf("xai tts cannot use credential source %q", plan.Execution.CredentialSource)
	}
}

// acceptableCredentialKind reports whether a delegated credential's kind may
// authenticate the plan's route. Bearer is the norm everywhere; the relay arm
// additionally accepts relay_access, because protocol.SessionPlan validation
// requires relay plans to label their credential relay_access while a relay
// connector that synthesizes the plan and drives this adapter directly — no
// Engine, no SessionPlan.Validate — labels the same permanent xAI key bearer.
// Both spellings carry a permanent key, and both travel in the one
// Authorization: Bearer header this package sends everywhere, so nothing else
// on the relay arm changes. Shared by every xAI surface via bearerCredential.
func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

// generation holds the already-validated synthesis choices shared by both
// surfaces. The two surfaces spell them differently on the wire — the unary
// endpoint takes a JSON body keyed `voice_id` with a nested `output_format`
// object, the WebSocket takes handshake query parameters keyed `voice` with
// flat `codec`/`sample_rate` — so the spelling lives with each transport.
type generation struct {
	voice      string
	language   string
	sampleRate int
	latency    int
}

func validateGenerationOptions(model string, options protocol.RequestOptions, media protocol.MediaFormat, latency int) (generation, error) {
	if strings.TrimSpace(model) == "" || model == "auto" {
		return generation{}, errors.New("xai tts requires a concrete model in the session plan")
	}
	// xAI makes `language` REQUIRED on both surfaces, and it accepts full BCP-47
	// tags. The region subtag is load-bearing here: pt-BR and pt-PT, es-MX and
	// es-ES, ar-EG and ar-SA are all separately supported voices of the same
	// base language, so the tag is forwarded verbatim rather than truncated.
	language := strings.TrimSpace(options.Language)
	if language == "" {
		return generation{}, errors.New("xai tts requires a language in request options")
	}
	if media.Encoding != pcmEncoding {
		return generation{}, fmt.Errorf("xai tts streaming output requires pcm_s16le, got %q", media.Encoding)
	}
	if media.Channels != 1 {
		return generation{}, fmt.Errorf("xai tts produces mono audio, got %d channels", media.Channels)
	}
	if _, ok := documentedSampleRates[media.SampleRateHz]; !ok {
		return generation{}, fmt.Errorf("xai tts does not support sample rate %d", media.SampleRateHz)
	}
	// voice_id is optional upstream and defaults to eve; the adapter pins it so
	// two identical plans always produce the same wire request.
	voice := strings.TrimSpace(options.Voice)
	if voice == "" {
		voice = DefaultVoice
	}
	return generation{voice: voice, language: language, sampleRate: media.SampleRateHz, latency: latency}, nil
}

const pcmEncoding = "pcm_s16le"

func httpClientOrDefault(client *http.Client) *http.Client {
	if client != nil {
		return client
	}
	return http.DefaultClient
}

// ---------------------------------------------------------------------------
// Endpoint policy
// ---------------------------------------------------------------------------

// httpPolicy is the https twin of upstream.WebSocketPolicy.
//
// internal/upstream currently ships only a WebSocket variant, whose Parse
// rejects anything that is not a wss:// URL, and internal/upstream is a shared
// package this adapter must not modify. The rules below are deliberately
// identical to that policy so the two cannot drift in intent: allowlisted host,
// TLS unless explicitly overridden for tests, no userinfo, no port surprises,
// and no attacker-supplied query string riding along with the credential.
type httpPolicy struct {
	hosts         map[string]struct{}
	allowInsecure bool
}

func newHTTPPolicy(officialHost string, additionalHosts []string, allowInsecure bool) (httpPolicy, error) {
	hosts := make(map[string]struct{}, 1+len(additionalHosts))
	for _, host := range append([]string{officialHost}, additionalHosts...) {
		host = strings.ToLower(strings.TrimSpace(host))
		if host == "" || strings.ContainsAny(host, "/:@?#") {
			return httpPolicy{}, errors.New("xai: allowed endpoint host is invalid")
		}
		hosts[host] = struct{}{}
	}
	return httpPolicy{hosts: hosts, allowInsecure: allowInsecure}, nil
}

func (p httpPolicy) parse(raw string) (*url.URL, error) {
	endpoint, err := url.Parse(raw)
	if err != nil || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.Fragment != "" || endpoint.RawQuery != "" {
		return nil, errors.New("xai: endpoint must be a clean absolute HTTP URL")
	}
	if endpoint.Scheme != "https" && !(p.allowInsecure && endpoint.Scheme == "http") {
		return nil, errors.New("xai: endpoint must use https")
	}
	if !p.allowInsecure && endpoint.Port() != "" && endpoint.Port() != "443" {
		return nil, errors.New("xai: endpoint uses a non-standard port")
	}
	if _, ok := p.hosts[strings.ToLower(endpoint.Hostname())]; !ok {
		return nil, errors.New("xai: endpoint host is not allowed")
	}
	return endpoint, nil
}

func requireTTSPath(endpoint *url.URL) error {
	if endpoint.Path != ttsPath {
		return fmt.Errorf("xai tts endpoint path must be %s, got %q", ttsPath, endpoint.Path)
	}
	return nil
}

// ---------------------------------------------------------------------------
// WebSocket surface: wss://api.x.ai/v1/tts
// ---------------------------------------------------------------------------

func (a *Adapter) openSocket(ctx context.Context, request runtimepkg.AdapterRequest, options generation, credential string) (runtimepkg.ProviderStream, error) {
	endpoint, err := a.socketPolicy.Parse(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("xai tts endpoint: %w", err)
	}
	if err := requireTTSPath(endpoint); err != nil {
		return nil, err
	}
	// Documented handshake parameters. Note `voice`, not `voice_id`: the two
	// surfaces really do spell the same choice differently. bit_rate is omitted
	// because xAI documents it as MP3-only and this adapter always asks for pcm.
	query := endpoint.Query()
	query.Set("language", options.language)
	query.Set("voice", options.voice)
	query.Set("codec", pcmCodec)
	query.Set("sample_rate", strconv.Itoa(options.sampleRate))
	query.Set("optimize_streaming_latency", strconv.Itoa(options.latency))
	endpoint.RawQuery = query.Encode()

	headers := make(http.Header)
	headers.Set("Authorization", "Bearer "+credential)
	conn, response, err := websocket.Dial(ctx, endpoint.String(), &websocket.DialOptions{HTTPClient: httpClientOrDefault(a.httpClient), HTTPHeader: headers})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		return nil, &runtimepkg.ProviderError{
			Code: statusErrorCode(status), Message: "xAI TTS streaming connection could not be established",
			Retryable: status == 0 || statusRetryable(status), ProviderStatus: status, Cause: err,
		}
	}
	conn.SetReadLimit(a.maxMessageBytes)
	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &socketStream{conn: conn, ctx: streamCtx, cancel: cancel, events: make(chan runtimepkg.ProviderEvent, a.eventBuffer)}
	go stream.readLoop()
	return stream, nil
}

type socketStream struct {
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent

	writeMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	closing      atomic.Bool
	closeErr     error

	stateMu     sync.Mutex
	utteranceID string
	done        chan struct{}
	committed   bool
	started     bool
	traceID     string
}

func (s *socketStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

func (s *socketStream) WriteAudio(context.Context, []byte) error {
	return runtimepkg.ErrUnsupportedOperation
}

func (s *socketStream) CommitAudio(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// AppendText sends one text.delta. xAI synthesizes from the accumulated deltas
// only once text.done arrives, so audio for a long turn starts flowing while
// the caller is still producing it.
func (s *socketStream) AppendText(ctx context.Context, text string) error {
	if strings.TrimSpace(text) == "" {
		return errors.New("xai tts text is empty")
	}
	// The 15,000-character cap is documented per text.delta on this surface,
	// not per utterance: a longer turn is legal if it is split across deltas.
	if utf8.RuneCountInString(text) > maxTextCharacters {
		return &runtimepkg.ProviderError{
			Code: "input_too_large", Message: "xAI TTS text delta exceeds 15000 characters",
			Retryable: false, ProviderStatus: http.StatusRequestEntityTooLarge,
		}
	}
	if _, err := s.startOrCurrentUtterance(); err != nil {
		return err
	}
	return s.writeJSON(ctx, map[string]string{"type": "text.delta", "delta": text})
}

// CommitText flushes the accumulated deltas. The socket stays open afterwards:
// xAI documents multi-utterance sessions, so the next turn reuses it.
func (s *socketStream) CommitText(ctx context.Context) error {
	if err := s.commitUtterance(); err != nil {
		return err
	}
	if err := s.writeJSON(ctx, map[string]string{"type": "text.done"}); err != nil {
		s.uncommitUtterance()
		return err
	}
	return nil
}

// Cancel asks xAI to drop the in-flight utterance. The provider acknowledges
// with audio.clear, which is where the utterance is actually retired.
func (s *socketStream) Cancel(ctx context.Context) error {
	if !s.hasUtterance() {
		return runtimepkg.ErrSessionClosed
	}
	return s.writeJSON(ctx, map[string]string{"type": "text.clear"})
}

// Close waits for the active utterance's audio.done so a caller that closes
// straight after CommitText still receives its final audio.
func (s *socketStream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		s.closing.Store(true)
		if done := s.activeDone(); done != nil {
			select {
			case <-done:
			case <-s.ctx.Done():
			case <-ctx.Done():
				s.closeErr = ctx.Err()
			}
		}
		if s.closeErr == nil {
			s.closed.Store(true)
			if err := s.conn.Close(websocket.StatusNormalClosure, ""); err != nil {
				s.closeErr = err
			}
		}
		if s.closeErr != nil {
			_ = s.abort()
		}
	})
	return s.closeErr
}

func (s *socketStream) Abort(context.Context) error { return s.abort() }

func (s *socketStream) abort() error {
	s.abortOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		if err := s.conn.CloseNow(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
		s.finishUtterance()
	})
	return s.closeErr
}

func (s *socketStream) startOrCurrentUtterance() (string, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() || s.closing.Load() {
		return "", runtimepkg.ErrSessionClosed
	}
	if s.utteranceID != "" {
		if s.committed {
			return "", errors.New("xai tts previous utterance has not completed")
		}
		return s.utteranceID, nil
	}
	id, err := newUtteranceID()
	if err != nil {
		return "", err
	}
	s.utteranceID, s.done, s.committed, s.started = id, make(chan struct{}), false, false
	return id, nil
}

func (s *socketStream) commitUtterance() error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() || s.closing.Load() || s.utteranceID == "" || s.committed {
		return runtimepkg.ErrSessionClosed
	}
	s.committed = true
	return nil
}

func (s *socketStream) uncommitUtterance() {
	s.stateMu.Lock()
	if s.utteranceID != "" {
		s.committed = false
	}
	s.stateMu.Unlock()
}

func (s *socketStream) hasUtterance() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.utteranceID != ""
}

func (s *socketStream) activeDone() <-chan struct{} {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.done
}

func (s *socketStream) finishUtterance() string {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	id := s.utteranceID
	if s.done != nil {
		close(s.done)
	}
	s.utteranceID, s.done, s.committed, s.started = "", nil, false, false
	return id
}

// markStarted reports whether this is the first audio of the current utterance,
// which is the only place audio.started may be emitted.
func (s *socketStream) markStarted() (string, bool) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.utteranceID == "" || s.started {
		return s.utteranceID, false
	}
	s.started = true
	return s.utteranceID, true
}

func (s *socketStream) currentUtterance() string {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.utteranceID
}

func (s *socketStream) setTraceID(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.traceID == value {
		return false
	}
	s.traceID = value
	return true
}

func (s *socketStream) currentTraceID() string {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.traceID
}

func (s *socketStream) writeJSON(ctx context.Context, value any) error {
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
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "xAI TTS streaming write failed", Retryable: true, Cause: err}
	}
	return nil
}

func (s *socketStream) readLoop() {
	defer func() {
		s.cancel()
		close(s.events)
	}()
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && s.ctx.Err() == nil && !isNormalClose(err) {
				_ = s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "xAI TTS streaming read failed", Retryable: true, Cause: err}})
			}
			return
		}
		// xAI frames every server message as JSON text; audio arrives base64
		// encoded inside audio.delta rather than as a binary frame.
		if messageType != websocket.MessageText {
			continue
		}
		if err := s.handleMessage(payload); err != nil {
			_ = s.emit(runtimepkg.ProviderEvent{Err: err})
			return
		}
	}
}

type socketMessage struct {
	Type    string `json:"type"`
	Delta   string `json:"delta"`
	TraceID string `json:"trace_id"`
	Message string `json:"message"`
}

func (s *socketStream) handleMessage(payload []byte) error {
	var message socketMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "xAI TTS sent malformed streaming JSON", Retryable: true, Cause: err}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))
	if s.setTraceID(message.TraceID) {
		if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: usageData(message.TraceID)}); err != nil {
			return err
		}
	}
	switch message.Type {
	case "audio.delta":
		audio, err := base64.StdEncoding.DecodeString(message.Delta)
		if err != nil {
			return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "xAI TTS sent invalid audio data", Retryable: true, Cause: err}
		}
		id, started := s.markStarted()
		if started {
			if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: s.contextData(id)}); err != nil {
				return err
			}
		}
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Data: s.contextData(id), Extensions: extension(raw), Audio: audio})
	case "audio.done":
		id := s.finishUtterance()
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: s.contextData(id), Extensions: extension(raw)})
	case "audio.clear":
		// The provider confirmed the cancellation. The utterance is retired
		// without audio.done, which is exactly what Cancel promises.
		id := s.finishUtterance()
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: warningData(id, "provider_buffer_cleared", s.currentTraceID()), Extensions: extension(raw)})
	case "error":
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: socketErrorMessage(message.Message), Retryable: false}
	default:
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: warningData(s.currentUtterance(), message.Type, s.currentTraceID()), Extensions: extension(raw)})
	}
}

func (s *socketStream) contextData(utteranceID string) json.RawMessage {
	return marshalData(map[string]any{"utterance_id": utteranceID, "provider_request_id": s.currentTraceID()})
}

func (s *socketStream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func socketErrorMessage(message string) string {
	if strings.TrimSpace(message) == "" {
		return "xAI reported a TTS streaming error"
	}
	return "xAI reported a TTS streaming error: " + message
}

// ---------------------------------------------------------------------------
// Unary surface: POST https://api.x.ai/v1/tts
// ---------------------------------------------------------------------------

func (a *Adapter) openUnary(request runtimepkg.AdapterRequest, options generation, credential string) (runtimepkg.ProviderStream, error) {
	endpoint, err := a.httpPolicy.parse(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("xai tts endpoint: %w", err)
	}
	if err := requireTTSPath(endpoint); err != nil {
		return nil, err
	}
	streamCtx, cancel := context.WithCancel(context.Background())
	return &unaryStream{
		httpClient:      httpClientOrDefault(a.httpClient),
		endpoint:        endpoint.String(),
		credential:      credential,
		options:         options,
		audioChunkBytes: a.audioChunkBytes,
		maxErrorBytes:   a.maxErrorBytes,
		ctx:             streamCtx,
		cancel:          cancel,
		events:          make(chan runtimepkg.ProviderEvent, a.eventBuffer),
	}, nil
}

// synthesis is the documented unary request body. Fields xAI documents but this
// adapter never sets are deliberately absent rather than zero-valued:
//   - `speed` would change the voice, which belongs to the plan, not the adapter.
//   - `bit_rate` is documented as MP3-only and the codec here is always pcm.
//   - `text_normalization` defaults to false upstream and changes what is said.
//   - `with_timestamps` MUST stay unset: it switches the 200 response from raw
//     audio bytes to a base64 JSON envelope, which would break frame streaming.
//
// There is no `model` field. That is not an omission — xAI's TTS body has none.
type synthesis struct {
	Text                     string       `json:"text"`
	VoiceID                  string       `json:"voice_id"`
	Language                 string       `json:"language"`
	OutputFormat             outputFormat `json:"output_format"`
	OptimizeStreamingLatency int          `json:"optimize_streaming_latency"`
}

type outputFormat struct {
	Codec      string `json:"codec"`
	SampleRate int    `json:"sample_rate"`
}

type unaryStream struct {
	httpClient      *http.Client
	endpoint        string
	credential      string
	options         generation
	audioChunkBytes int
	maxErrorBytes   int64

	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent

	readers      sync.WaitGroup
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closeOnce    sync.Once
	closed       atomic.Bool
	closing      atomic.Bool
	aborted      atomic.Bool
	closeErr     error

	stateMu      sync.Mutex
	pending      strings.Builder
	pendingChars int
	active       *utterance
}

// utterance owns one in-flight POST. Cancel and Abort both work by cancelling
// its context, which unblocks the response body read wherever it is parked.
type utterance struct {
	id       string
	cancel   context.CancelFunc
	done     chan struct{}
	canceled atomic.Bool
	finish   sync.Once
}

func (s *unaryStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

func (s *unaryStream) WriteAudio(context.Context, []byte) error {
	return runtimepkg.ErrUnsupportedOperation
}

func (s *unaryStream) CommitAudio(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// AppendText buffers locally. The unary endpoint has no incremental input, so
// nothing can reach xAI before CommitText supplies the utterance boundary.
func (s *unaryStream) AppendText(_ context.Context, text string) error {
	if strings.TrimSpace(text) == "" {
		return errors.New("xai tts text is empty")
	}
	characters := utf8.RuneCountInString(text)
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() || s.closing.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if s.active != nil {
		return errors.New("xai tts previous utterance has not completed")
	}
	// Here the 15,000-character cap applies to the whole request, so it is
	// enforced cumulatively rather than per call.
	if s.pendingChars+characters > maxTextCharacters {
		return &runtimepkg.ProviderError{
			Code: "input_too_large", Message: "xAI TTS input exceeds 15000 characters",
			Retryable: false, ProviderStatus: http.StatusRequestEntityTooLarge,
		}
	}
	s.pending.WriteString(text)
	s.pendingChars += characters
	return nil
}

// CommitText performs the POST. http.Client.Do returns once the response
// headers are in, so status mapping happens synchronously and is reported to
// the caller, while the body is drained by a goroutine that emits audio frames
// as bytes arrive. A failed commit discards the buffered text: the caller must
// re-append to retry, rather than have a stale fragment prepended to the next
// utterance.
func (s *unaryStream) CommitText(_ context.Context) error {
	text, active, requestCtx, err := s.beginUtterance()
	if err != nil {
		return err
	}
	payload, err := json.Marshal(synthesis{
		Text:                     text,
		VoiceID:                  s.options.voice,
		Language:                 s.options.language,
		OutputFormat:             outputFormat{Codec: pcmCodec, SampleRate: s.options.sampleRate},
		OptimizeStreamingLatency: s.options.latency,
	})
	if err != nil {
		s.finishUtterance(active)
		return err
	}
	httpRequest, err := http.NewRequestWithContext(requestCtx, http.MethodPost, s.endpoint, bytes.NewReader(payload))
	if err != nil {
		s.finishUtterance(active)
		return err
	}
	httpRequest.Header.Set("Authorization", "Bearer "+s.credential)
	httpRequest.Header.Set("Content-Type", "application/json")

	response, err := s.httpClient.Do(httpRequest)
	if err != nil {
		s.finishUtterance(active)
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "xAI TTS request could not be sent", Retryable: true, Cause: err}
	}
	if response.StatusCode != http.StatusOK {
		raw := readErrorBody(response.Body, s.maxErrorBytes)
		_ = response.Body.Close()
		s.finishUtterance(active)
		return &runtimepkg.ProviderError{
			Code:           statusErrorCode(response.StatusCode),
			Message:        fmt.Sprintf("xAI TTS request failed with status %d", response.StatusCode),
			Retryable:      statusRetryable(response.StatusCode),
			ProviderStatus: response.StatusCode,
			Extensions:     extension(raw),
		}
	}
	// A 200 whose body is JSON means xAI answered with the timestamp envelope
	// (base64 audio inside JSON) instead of raw bytes. This adapter never asks
	// for that, so treat it as a contract break rather than emitting JSON text
	// into the audio path.
	if isJSONContent(response.Header.Get("Content-Type")) {
		_ = response.Body.Close()
		s.finishUtterance(active)
		return &runtimepkg.ProviderError{
			Code: "provider_unavailable", Message: "xAI TTS returned a JSON envelope where raw audio bytes were expected",
			Retryable: false, ProviderStatus: response.StatusCode,
		}
	}
	s.readers.Add(1)
	go s.streamBody(active, response)
	return nil
}

// Cancel aborts the in-flight request. There is no cancellation message on this
// surface — dropping the connection is the only lever the unary endpoint gives.
func (s *unaryStream) Cancel(context.Context) error {
	s.stateMu.Lock()
	active := s.active
	s.pending.Reset()
	s.pendingChars = 0
	s.stateMu.Unlock()
	if active == nil {
		return runtimepkg.ErrSessionClosed
	}
	active.canceled.Store(true)
	s.finishUtterance(active)
	return nil
}

// Close waits for the active utterance so a caller that closes immediately
// after CommitText still receives its audio, then releases the event channel.
func (s *unaryStream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		s.closing.Store(true)
		if done := s.activeDone(); done != nil {
			select {
			case <-done:
			case <-s.ctx.Done():
			case <-ctx.Done():
				s.closeErr = ctx.Err()
			}
		}
		if s.closeErr != nil {
			_ = s.abort()
			return
		}
		s.closed.Store(true)
		s.shutdown()
	})
	return s.closeErr
}

func (s *unaryStream) Abort(context.Context) error { return s.abort() }

func (s *unaryStream) abort() error {
	s.abortOnce.Do(func() {
		s.closed.Store(true)
		s.aborted.Store(true)
		s.shutdown()
	})
	return s.closeErr
}

// shutdown cancels every in-flight request, waits for the body readers to stop
// touching the channel, and only then closes it. The order matters: emit()
// selects on s.ctx, so cancelling first guarantees a parked reader wakes up
// instead of deadlocking the wait.
func (s *unaryStream) shutdown() {
	s.cancel()
	s.readers.Wait()
	s.closeOnce.Do(func() { close(s.events) })
}

func (s *unaryStream) beginUtterance() (string, *utterance, context.Context, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() || s.closing.Load() {
		return "", nil, nil, runtimepkg.ErrSessionClosed
	}
	if s.active != nil {
		return "", nil, nil, errors.New("xai tts previous utterance has not completed")
	}
	if s.pendingChars == 0 {
		return "", nil, nil, errors.New("xai tts has no buffered text to synthesize")
	}
	id, err := newUtteranceID()
	if err != nil {
		return "", nil, nil, err
	}
	text := s.pending.String()
	s.pending.Reset()
	s.pendingChars = 0
	requestCtx, cancel := context.WithCancel(s.ctx)
	active := &utterance{id: id, cancel: cancel, done: make(chan struct{})}
	s.active = active
	return text, active, requestCtx, nil
}

func (s *unaryStream) finishUtterance(active *utterance) {
	active.finish.Do(func() {
		active.cancel()
		close(active.done)
	})
	s.stateMu.Lock()
	if s.active == active {
		s.active = nil
	}
	s.stateMu.Unlock()
}

func (s *unaryStream) activeDone() <-chan struct{} {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.active == nil {
		return nil
	}
	return s.active.done
}

// streamBody slices the response body into audio frames as it arrives. It
// deliberately never accumulates the full utterance: the point of reading
// incrementally is that the first frame reaches the caller while xAI is still
// writing the last one.
func (s *unaryStream) streamBody(active *utterance, response *http.Response) {
	defer s.readers.Done()
	defer response.Body.Close()
	defer s.finishUtterance(active)

	// xAI documents no correlation header for TTS, so this is best-effort: the
	// usage event is emitted only when a request id actually turns up.
	requestID := strings.TrimSpace(response.Header.Get("X-Request-Id"))
	contentType := response.Header.Get("Content-Type")
	if requestID != "" {
		if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: usageData(requestID)}); err != nil {
			return
		}
	}

	buffer := make([]byte, s.audioChunkBytes)
	started := false
	total := 0
	for {
		read, err := response.Body.Read(buffer)
		if read > 0 {
			if !started {
				started = true
				if emitErr := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: unaryContextData(active.id, requestID, contentType)}); emitErr != nil {
					return
				}
			}
			total += read
			audio := make([]byte, read)
			copy(audio, buffer[:read])
			if emitErr := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Data: unaryContextData(active.id, requestID, contentType), Audio: audio}); emitErr != nil {
				return
			}
		}
		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) {
			// Always pair audio.done with an audio.started, even for an empty
			// body, so consumers never see a terminal event for an utterance
			// they were never told had begun.
			if !started {
				if emitErr := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: unaryContextData(active.id, requestID, contentType)}); emitErr != nil {
					return
				}
			}
			_ = s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: unaryDoneData(active.id, requestID, total)})
			return
		}
		// A deliberate teardown is not a provider failure. Cancel and Abort both
		// cancel the request context, which surfaces here as a read error.
		if active.canceled.Load() || s.aborted.Load() || s.ctx.Err() != nil {
			return
		}
		_ = s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "xAI TTS audio stream failed", Retryable: true, Cause: err}})
		return
	}
}

func (s *unaryStream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func unaryContextData(utteranceID, requestID, contentType string) json.RawMessage {
	return marshalData(map[string]any{"utterance_id": utteranceID, "provider_request_id": requestID, "content_type": contentType})
}

func unaryDoneData(utteranceID, requestID string, audioBytes int) json.RawMessage {
	return marshalData(map[string]any{"utterance_id": utteranceID, "provider_request_id": requestID, "audio_bytes": audioBytes})
}

// readErrorBody keeps a bounded copy of a failed response for the canonical
// error event. xAI publishes no error-body schema, so the payload is preserved
// verbatim when it is JSON and quoted as a JSON string when it is not.
func readErrorBody(body io.Reader, limit int64) json.RawMessage {
	payload, err := io.ReadAll(io.LimitReader(body, limit))
	if err != nil || len(payload) == 0 {
		return json.RawMessage(`{}`)
	}
	if json.Valid(payload) {
		return json.RawMessage(payload)
	}
	quoted, err := json.Marshal(string(payload))
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return json.RawMessage(quoted)
}

func isJSONContent(contentType string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(contentType)), "application/json")
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

// statusErrorCode maps upstream HTTP status to the stable protocol taxonomy.
// xAI documents 400, 401, 404, 429, 500, and 503 for TTS, plus 403 on its
// general error page.
func statusErrorCode(status int) string {
	switch {
	case status == http.StatusUnauthorized, status == http.StatusForbidden:
		return "authentication_failed"
	case status == http.StatusTooManyRequests:
		return "provider_rate_limited"
	case status >= 500:
		return "provider_unavailable"
	case status >= 400:
		return "invalid_request"
	default:
		// A zero status means the transport failed before any response.
		return "provider_unavailable"
	}
}

func statusRetryable(status int) bool {
	return status == http.StatusTooManyRequests || status >= 500
}

func isNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}

func extension(raw json.RawMessage) map[string]json.RawMessage {
	return map[string]json.RawMessage{extensionID: raw}
}

func usageData(requestID string) json.RawMessage {
	return marshalData(map[string]any{"provider_request_id": requestID})
}

func warningData(utteranceID, code, requestID string) json.RawMessage {
	return marshalData(map[string]any{"utterance_id": utteranceID, "code": code, "provider_request_id": requestID})
}

func marshalData(value any) json.RawMessage {
	payload, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{"code":"internal"}`)
	}
	return payload
}

func newUtteranceID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate xAI TTS utterance id: %w", err)
	}
	return hex.EncodeToString(value), nil
}
