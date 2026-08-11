package inworld

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
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// AdapterID is the identifier returned by an Inworld TTS session plan.
	AdapterID   = "inworld.tts.v1"
	extensionID = "inworld.ai/tts/v1"

	officialAPIHost = "api.inworld.ai"

	// streamPath is the documented server-streaming synthesis resource. The
	// colon is part of the path (a Google-style custom method), not a port.
	// Confirmed against the TTS OpenAPI document: `post /tts/v1/voice:stream`
	// with `servers: https://api.inworld.ai`.
	streamPath = "/tts/v1/voice:stream"

	// DefaultModel is Inworld's current flagship. `inworld-tts-1` and
	// `inworld-tts-1-max` were discontinued on 2026-06-15 and are silently
	// rerouted to their 1.5 successors upstream, so this adapter refuses them
	// rather than let a plan claim a model the provider will not run.
	DefaultModel = "inworld-tts-2"

	// maxInputCharacters is the documented per-request ceiling on `text`.
	maxInputCharacters = 2_000

	// defaultMaxResponseBytes bounds one synthesis response. Input is capped at
	// 2,000 characters, so even 48 kHz mono PCM plus base64 inflation stays far
	// below this; the limit exists only so a broken upstream cannot grow the
	// decoder's buffer without bound.
	defaultMaxResponseBytes = 64 << 20
)

// supportedModels is the current Inworld TTS lineup. Kept as an explicit set so
// a typo in a plan fails at Open rather than as an upstream 400 mid-call.
var supportedModels = map[string]struct{}{
	"inworld-tts-2":        {},
	"inworld-tts-1.5-max":  {},
	"inworld-tts-1.5-mini": {},
}

// discontinuedModels maps a retired id to the model Inworld silently routes it
// to. Naming the successor makes the rejection actionable.
var discontinuedModels = map[string]string{
	"inworld-tts-1":     "inworld-tts-1.5-mini",
	"inworld-tts-1-max": "inworld-tts-1.5-max",
}

// Config controls local transport limits. Provider identity, model, voice, and
// credential come from a verified session plan and its request options.
type Config struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxResponseBytes      int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// Adapter implements Inworld's POST /tts/v1/voice:stream synthesis API.
type Adapter struct {
	id               string
	httpClient       *http.Client
	eventBuffer      int
	maxResponseBytes int64
	endpointPolicy   endpointPolicy
}

// New creates a bounded Inworld TTS adapter.
func New(config Config) (*Adapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = AdapterID
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = 32
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = defaultMaxResponseBytes
	}
	if config.EventBuffer < 1 {
		return nil, errors.New("inworld event buffer must be positive")
	}
	if config.MaxResponseBytes < 1 {
		return nil, errors.New("inworld maximum response bytes must be positive")
	}
	policy, err := newEndpointPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &Adapter{
		id:               config.AdapterID,
		httpClient:       config.HTTPClient,
		eventBuffer:      config.EventBuffer,
		maxResponseBytes: config.MaxResponseBytes,
		endpointPolicy:   policy,
	}, nil
}

func (a *Adapter) ID() string { return a.id }

// Open validates the plan and prepares a synthesis stream. Unlike the
// WebSocket adapters it performs no network I/O: HTTP has no handshake, so the
// first request happens at CommitText.
func (a *Adapter) Open(_ context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindTTS {
		return nil, fmt.Errorf("inworld supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "inworld" {
		return nil, fmt.Errorf("inworld adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	// Every other adapter here demands websocket. Inworld TTS is a POST, so the
	// plan must say so: a websocket route would mean the control plane selected
	// a transport this adapter cannot speak.
	if request.Plan.Route.Transport != protocol.TransportHTTP {
		return nil, fmt.Errorf("inworld tts requires http transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("inworld tts requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("inworld media: %w", err)
	}
	if err := validateMedia(*request.Media); err != nil {
		return nil, err
	}
	model, err := validateModel(request.Plan.Route.Model)
	if err != nil {
		return nil, err
	}
	voice := strings.TrimSpace(request.Options.Voice)
	if voice == "" {
		return nil, errors.New("inworld tts requires a voice id in request options")
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("inworld tts requires a bearer credential")
	}
	endpoint, err := synthesisEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}

	streamCtx, cancel := context.WithCancel(context.Background())
	return &stream{
		ctx:              streamCtx,
		cancel:           cancel,
		events:           make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		httpClient:       httpClient(a.httpClient),
		endpoint:         endpoint,
		authorization:    authorizationHeader(request.Plan.Execution.ProviderRoute, request.Plan.Execution.CredentialSource, credential.Value),
		maxResponseBytes: a.maxResponseBytes,
		model:            model,
		voice:            voice,
		language:         strings.TrimSpace(request.Options.Language),
		media:            *request.Media,
	}, nil
}

// authorizationHeader picks the Inworld auth channel for the credential the
// plan carries. Inworld accepts two schemes and they are NOT interchangeable:
//
//   - BYOK: the customer's permanent portal key is already the Base64 of
//     "<key>:<secret>" — the value Inworld's docs call `$INWORLD_API_KEY`. It is
//     a Basic credential, and the TTS OpenAPI declares `inworld_basic` as this
//     resource's only security scheme. Sending it as a bearer token fails.
//   - Managed: the control plane mints a short-lived JWT at
//     POST /auth/v1/tokens/token:generate; that response is explicitly typed
//     `"type": "Bearer"` and Inworld's own sample app uses it as
//     `Authorization: Bearer <token>` against api.inworld.ai REST endpoints.
//
// A relay plan is managed for billing purposes but carries the relay
// connector's permanent portal key — the same Base64 "<key>:<secret>" value a
// customer holds — so it takes the Basic channel exactly like BYOK. Bearer
// stays reserved for the short-lived JWTs the control plane mints on managed
// provider-direct routes.
//
// Both travel in the Authorization header. Unlike the ElevenLabs and Cartesia
// WebSocket adapters there is no query-parameter channel to fall back to here:
// Inworld's query-parameter credential (`?authorization=`) is declared only on
// the WebSocket resource /tts/v1/voice:streamBidirectional, where a browser
// cannot set headers. Putting a token in this endpoint's query string would
// leave it unauthenticated and log the secret in the URL.
func authorizationHeader(route protocol.ProviderRoute, source protocol.CredentialSource, value string) string {
	if route == protocol.RouteSpekoRelay || source == protocol.CredentialsBYOK {
		return "Basic " + value
	}
	return "Bearer " + value
}

// acceptableCredentialKind reports whether a delegated credential's kind may
// authenticate the plan's route. Bearer is the norm everywhere; the relay arm
// additionally accepts relay_access, because protocol.SessionPlan validation
// requires relay plans to label their credential relay_access while a relay
// connector that synthesizes the plan and drives these adapters directly — no
// Engine, no SessionPlan.Validate — labels the same permanent Inworld key
// bearer. Both spellings carry a permanent portal key; the Basic-versus-Bearer
// prefix keys off the route and the source, never the kind, so nothing else on
// the relay arm changes.
func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

func httpClient(client *http.Client) *http.Client {
	if client != nil {
		return client
	}
	return http.DefaultClient
}

// validateMedia enforces the intersection of the canonical protocol media
// format and Inworld's audioConfig. LINEAR16 is documented as "uncompressed
// 16-bit signed little-endian samples", which is exactly pcm_s16le.
func validateMedia(media protocol.MediaFormat) error {
	if media.Encoding != "pcm_s16le" || media.Channels != 1 {
		return fmt.Errorf("inworld tts requires mono pcm_s16le output, got %s/%d channels", media.Encoding, media.Channels)
	}
	// Documented sampleRateHertz values. Anything else fails upstream with a
	// 400 once the encoding cannot honour it, so reject it here instead.
	switch media.SampleRateHz {
	case 8_000, 16_000, 22_050, 24_000, 32_000, 44_100, 48_000:
		return nil
	default:
		return fmt.Errorf("inworld tts does not support sample rate %d", media.SampleRateHz)
	}
}

func validateModel(model string) (string, error) {
	model = strings.TrimSpace(model)
	if model == "" || model == "auto" {
		return "", errors.New("inworld tts requires a concrete model in the session plan")
	}
	if successor, discontinued := discontinuedModels[model]; discontinued {
		return "", fmt.Errorf("inworld tts model %q was discontinued; use %q", model, successor)
	}
	if _, ok := supportedModels[model]; !ok {
		return "", fmt.Errorf("inworld tts does not support model %q", model)
	}
	return model, nil
}

func synthesisEndpoint(policy endpointPolicy, rawEndpoint string) (string, error) {
	endpoint, err := policy.parse(rawEndpoint)
	if err != nil {
		return "", fmt.Errorf("inworld endpoint: %w", err)
	}
	if endpoint.Path != streamPath {
		return "", fmt.Errorf("inworld tts endpoint path must be %s, got %q", streamPath, endpoint.Path)
	}
	return endpoint.String(), nil
}

// endpointPolicy is the HTTPS counterpart of upstream.WebSocketPolicy. That
// package currently ships only a WebSocket variant and this adapter must not
// modify shared code, so the same rules — no userinfo, no fragment, no
// preexisting query, no off-standard port, host allowlist — are restated for
// https here. Folding both into one helper in internal/upstream is the right
// long-term home; see the report.
type endpointPolicy struct {
	hosts         map[string]struct{}
	allowInsecure bool
}

func newEndpointPolicy(officialHost string, additionalHosts []string, allowInsecure bool) (endpointPolicy, error) {
	hosts := make(map[string]struct{}, 1+len(additionalHosts))
	for _, host := range append([]string{officialHost}, additionalHosts...) {
		host = strings.ToLower(strings.TrimSpace(host))
		if host == "" || strings.ContainsAny(host, "/:@?#") {
			return endpointPolicy{}, errors.New("inworld: allowed endpoint host is invalid")
		}
		hosts[host] = struct{}{}
	}
	return endpointPolicy{hosts: hosts, allowInsecure: allowInsecure}, nil
}

func (p endpointPolicy) parse(raw string) (*url.URL, error) {
	endpoint, err := url.Parse(raw)
	if err != nil || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.Fragment != "" || endpoint.RawQuery != "" {
		return nil, errors.New("endpoint must be a clean absolute HTTPS URL")
	}
	if endpoint.Scheme != "https" && !(p.allowInsecure && endpoint.Scheme == "http") {
		return nil, errors.New("endpoint must use https")
	}
	if !p.allowInsecure && endpoint.Port() != "" && endpoint.Port() != "443" {
		return nil, errors.New("endpoint uses a non-standard port")
	}
	if _, ok := p.hosts[strings.ToLower(endpoint.Hostname())]; !ok {
		return nil, errors.New("endpoint host is not allowed")
	}
	return endpoint, nil
}

type stream struct {
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent

	httpClient       *http.Client
	endpoint         string
	authorization    string
	maxResponseBytes int64

	model    string
	voice    string
	language string
	media    protocol.MediaFormat

	// readers tracks the response goroutine so the event channel is closed only
	// after every possible sender has returned.
	readers   sync.WaitGroup
	closeOnce sync.Once
	closeErr  error

	stateMu sync.Mutex
	closed  bool
	pending strings.Builder
	// utteranceID is non-empty exactly while one HTTP request is in flight.
	utteranceID   string
	requestCancel context.CancelFunc
	requestDone   chan struct{}
	canceled      bool
}

func (s *stream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

// WriteAudio and CommitAudio belong to transcription. A synthesizer rejects
// them so the runtime can report the mismatch instead of silently ignoring
// caller audio.
func (s *stream) WriteAudio(context.Context, []byte) error { return runtimepkg.ErrUnsupportedOperation }

func (s *stream) CommitAudio(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// AppendText buffers a fragment. Inworld synthesis is a single POST, so there
// is nothing to send until the caller marks the utterance complete.
func (s *stream) AppendText(_ context.Context, text string) error {
	if text == "" {
		return errors.New("inworld tts text is empty")
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return runtimepkg.ErrSessionClosed
	}
	if s.utteranceID != "" {
		return errors.New("inworld tts previous utterance has not completed")
	}
	if utf8.RuneCountInString(s.pending.String())+utf8.RuneCountInString(text) > maxInputCharacters {
		return &runtimepkg.ProviderError{
			Code:           "input_too_large",
			Message:        "Inworld TTS input exceeds 2000 characters",
			Retryable:      false,
			ProviderStatus: http.StatusRequestEntityTooLarge,
		}
	}
	s.pending.WriteString(text)
	return nil
}

// CommitText performs the synthesis request and starts streaming its response.
// It returns once the upstream status line is known, so a rejected request
// surfaces as a synchronous error rather than an event the caller has to
// correlate. Audio then arrives on Events until audio.done.
func (s *stream) CommitText(ctx context.Context) error {
	utteranceID, text, requestCtx, requestCancel, done, err := s.beginUtterance()
	if err != nil {
		return err
	}

	payload, err := json.Marshal(synthesizeRequest{
		Text:    text,
		VoiceID: s.voice,
		ModelID: s.model,
		AudioConfig: audioConfig{
			// LINEAR16 is the documented enum value for raw 16-bit PCM. The
			// wire names are lowerCamelCase because that is what the TTS
			// OpenAPI schema declares.
			AudioEncoding:   "LINEAR16",
			SampleRateHertz: s.media.SampleRateHz,
		},
		Language: s.language,
	})
	if err != nil {
		s.abandonUtterance(utteranceID, requestCancel, done)
		return err
	}

	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, s.endpoint, bytes.NewReader(payload))
	if err != nil {
		s.abandonUtterance(utteranceID, requestCancel, done)
		return err
	}
	request.Header.Set("Authorization", s.authorization)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")

	response, err := s.httpClient.Do(request)
	if err != nil {
		s.abandonUtterance(utteranceID, requestCancel, done)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "Inworld TTS request could not be sent",
			Retryable: true,
			Cause:     err,
		}
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		providerErr := statusError(response)
		_ = response.Body.Close()
		s.abandonUtterance(utteranceID, requestCancel, done)
		return providerErr
	}

	s.readers.Add(1)
	go s.readResponse(utteranceID, response, requestCancel, done)
	return nil
}

// Cancel discards buffered text and aborts an in-flight synthesis by cancelling
// its request context. HTTP has no cancel message; dropping the connection is
// the only way to stop the provider generating.
func (s *stream) Cancel(ctx context.Context) error {
	s.stateMu.Lock()
	hadPending := s.pending.Len() > 0
	s.pending.Reset()
	requestCancel, done := s.requestCancel, s.requestDone
	if requestCancel != nil {
		s.canceled = true
	}
	s.stateMu.Unlock()

	if requestCancel == nil {
		if hadPending {
			return nil
		}
		return runtimepkg.ErrSessionClosed
	}
	requestCancel()
	// Wait for the reader to unwind so a following CommitText sees a clean
	// stream rather than racing the aborted response.
	select {
	case <-done:
		return nil
	case <-s.ctx.Done():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close waits for an in-flight response to finish so a caller that closes right
// after CommitText still receives its final audio, then releases the stream.
func (s *stream) Close(ctx context.Context) error {
	return s.shutdown(ctx, true)
}

// Abort tears the stream down immediately after a terminal runtime failure.
func (s *stream) Abort(context.Context) error { return s.shutdown(context.Background(), false) }

func (s *stream) shutdown(ctx context.Context, graceful bool) error {
	s.closeOnce.Do(func() {
		s.stateMu.Lock()
		s.closed = true
		s.pending.Reset()
		done := s.requestDone
		s.stateMu.Unlock()

		if graceful && done != nil {
			select {
			case <-done:
			case <-ctx.Done():
				s.closeErr = ctx.Err()
			}
		}
		// Cancelling the stream context aborts any request still running and
		// releases a reader blocked emitting into a full channel.
		s.cancel()
		s.readers.Wait()
		close(s.events)
	})
	return s.closeErr
}

func (s *stream) beginUtterance() (string, string, context.Context, context.CancelFunc, chan struct{}, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return "", "", nil, nil, nil, runtimepkg.ErrSessionClosed
	}
	if s.utteranceID != "" {
		return "", "", nil, nil, nil, errors.New("inworld tts previous utterance has not completed")
	}
	text := s.pending.String()
	if text == "" {
		return "", "", nil, nil, nil, errors.New("inworld tts has no buffered text to synthesize")
	}
	utteranceID, err := newUtteranceID()
	if err != nil {
		return "", "", nil, nil, nil, err
	}
	requestCtx, requestCancel := context.WithCancel(s.ctx)
	done := make(chan struct{})
	s.pending.Reset()
	s.utteranceID = utteranceID
	s.requestCancel = requestCancel
	s.requestDone = done
	s.canceled = false
	return utteranceID, text, requestCtx, requestCancel, done, nil
}

// abandonUtterance releases utterance state when the request never reached a
// reader goroutine, so the caller can retry on the same stream.
func (s *stream) abandonUtterance(utteranceID string, requestCancel context.CancelFunc, done chan struct{}) {
	requestCancel()
	s.finishUtterance(utteranceID)
	close(done)
}

func (s *stream) finishUtterance(utteranceID string) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.utteranceID != utteranceID {
		return
	}
	s.utteranceID = ""
	s.requestCancel = nil
	s.requestDone = nil
}

func (s *stream) wasCanceled() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.canceled
}

// readResponse decodes the server-streaming JSON body and emits canonical audio
// events. The body is consumed incrementally: json.Decoder pulls only as many
// bytes as the next value needs, so each provider chunk becomes an audio frame
// as it lands instead of after the response completes.
func (s *stream) readResponse(utteranceID string, response *http.Response, requestCancel context.CancelFunc, done chan struct{}) {
	defer func() {
		requestCancel()
		_ = response.Body.Close()
		s.finishUtterance(utteranceID)
		close(done)
		s.readers.Done()
	}()

	decoder := json.NewDecoder(io.LimitReader(response.Body, s.maxResponseBytes))
	audioStarted := false
	audioBytes := 0
	var usage json.RawMessage

	for {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			// A cancelled request tears the body mid-value. That is the caller's
			// own Cancel/Abort, not a provider fault, so it stays silent.
			if s.wasCanceled() || s.ctx.Err() != nil {
				return
			}
			s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{
				Code:      "provider_unavailable",
				Message:   "Inworld TTS sent a malformed synthesis stream",
				Retryable: true,
				Cause:     err,
			}})
			return
		}

		var message streamMessage
		if err := json.Unmarshal(raw, &message); err != nil {
			s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{
				Code:      "provider_unavailable",
				Message:   "Inworld TTS sent a malformed synthesis message",
				Retryable: true,
				Cause:     err,
			}})
			return
		}
		if message.Error != nil {
			s.emit(runtimepkg.ProviderEvent{Err: streamError(message.Error, raw)})
			return
		}
		if message.Result == nil {
			continue
		}
		if len(message.Result.Usage) > 0 {
			usage = append(json.RawMessage(nil), message.Result.Usage...)
		}
		if len(message.Result.TimestampInfo) > 0 {
			if !s.emit(runtimepkg.ProviderEvent{
				Type:       protocol.EventAlignment,
				Data:       alignmentData(utteranceID, message.Result.TimestampInfo),
				Extensions: extension(raw),
			}) {
				return
			}
		}
		// Trailing ASYNC timestamp messages carry an empty audioContent; only a
		// message with audio is an audio frame.
		if message.Result.AudioContent == "" {
			continue
		}
		chunk, err := decodeAudio(message.Result.AudioContent)
		if err != nil {
			s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{
				Code:      "provider_unavailable",
				Message:   "Inworld TTS sent invalid audio data",
				Retryable: true,
				Cause:     err,
			}})
			return
		}
		// Documented quirk: with LINEAR16 the streaming endpoint prefixes a
		// complete WAV header to EVERY chunk so each can be played standalone.
		// Passing that through would splice `RIFF....RIFF....` into the PCM a
		// consumer is told is raw pcm_s16le, which is audible as a click per
		// chunk. Strip it; on a chunk without a header this is a no-op.
		chunk = stripWAVHeader(chunk)
		if len(chunk) == 0 {
			continue
		}
		if !audioStarted {
			audioStarted = true
			if !s.emit(runtimepkg.ProviderEvent{
				Type:       protocol.EventAudioStarted,
				Data:       utteranceData(utteranceID),
				Extensions: extension(raw),
			}) {
				return
			}
		}
		audioBytes += len(chunk)
		if !s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventAudioFrame,
			Data:       utteranceData(utteranceID),
			Extensions: extension(raw),
			Audio:      chunk,
		}) {
			return
		}
	}

	if s.wasCanceled() || s.ctx.Err() != nil {
		return
	}
	// A 200 that produced no audio is a failed synthesis wearing a success
	// status. Reporting it keeps failure distinguishable from a short utterance.
	if audioBytes == 0 {
		s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "Inworld TTS completed without returning audio",
			Retryable: true,
		}})
		return
	}
	if len(usage) > 0 {
		if !s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: usageData(utteranceID, usage)}) {
			return
		}
	}
	s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: utteranceData(utteranceID)})
}

// emit reports whether the event was delivered. A false result means the stream
// is shutting down and the caller must stop producing.
func (s *stream) emit(event runtimepkg.ProviderEvent) bool {
	select {
	case s.events <- event:
		return true
	case <-s.ctx.Done():
		return false
	}
}

// statusError maps an HTTP status onto the stable protocol classification. The
// body is deliberately not copied into Message: it is provider text that may
// echo request content, and Extensions is the place for raw payloads.
func statusError(response *http.Response) *runtimepkg.ProviderError {
	status := response.StatusCode
	// Read a bounded prefix so the connection can be reused and the raw payload
	// is available to a local error event.
	body, _ := io.ReadAll(io.LimitReader(response.Body, 8<<10))
	providerErr := &runtimepkg.ProviderError{
		Code:           statusErrorCode(status),
		Message:        fmt.Sprintf("Inworld TTS rejected the synthesis request with status %d", status),
		Retryable:      status == http.StatusTooManyRequests || status >= 500,
		ProviderStatus: status,
	}
	if json.Valid(body) {
		providerErr.Extensions = extension(json.RawMessage(body))
	}
	return providerErr
}

func statusErrorCode(status int) string {
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return "authentication_failed"
	case status == http.StatusTooManyRequests:
		return "provider_rate_limited"
	case status >= 500:
		return "provider_unavailable"
	default:
		return "invalid_request"
	}
}

// streamError maps an in-band error object. Its `code` is a gRPC status code,
// not an HTTP status — Inworld transcodes a gRPC service — so it must not be
// reported as ProviderStatus. The raw object travels in Extensions instead.
func streamError(status *rpcStatus, raw json.RawMessage) *runtimepkg.ProviderError {
	code := "invalid_request"
	retryable := false
	switch status.Code {
	case 7, 16: // PERMISSION_DENIED, UNAUTHENTICATED
		code = "authentication_failed"
	case 8: // RESOURCE_EXHAUSTED
		code, retryable = "provider_rate_limited", true
	case 2, 4, 13, 14: // UNKNOWN, DEADLINE_EXCEEDED, INTERNAL, UNAVAILABLE
		code, retryable = "provider_unavailable", true
	}
	message := "Inworld reported a synthesis error"
	if strings.TrimSpace(status.Message) != "" {
		message += ": " + status.Message
	}
	return &runtimepkg.ProviderError{Code: code, Message: message, Retryable: retryable, Extensions: extension(raw)}
}

// decodeAudio accepts padded standard base64, which is what protobuf JSON
// emits for a `bytes` field, and tolerates an unpadded encoder.
func decodeAudio(value string) ([]byte, error) {
	if decoded, err := base64.StdEncoding.DecodeString(value); err == nil {
		return decoded, nil
	}
	return base64.RawStdEncoding.DecodeString(value)
}

// stripWAVHeader drops a leading RIFF/WAVE container. The canonical PCM header
// is 44 bytes, but the format permits extra fmt/LIST chunks, so the `data`
// subchunk id is located rather than assumed at a fixed offset.
func stripWAVHeader(audio []byte) []byte {
	if len(audio) < 12 || string(audio[0:4]) != "RIFF" || string(audio[8:12]) != "WAVE" {
		return audio
	}
	limit := len(audio) - 8
	if limit > 1024 {
		limit = 1024
	}
	for index := 12; index < limit; index++ {
		if string(audio[index:index+4]) == "data" {
			// `data` magic plus its 4-byte length; samples start after both.
			return audio[index+8:]
		}
	}
	return audio
}

func newUtteranceID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate Inworld TTS utterance id: %w", err)
	}
	return hex.EncodeToString(value), nil
}

func extension(raw json.RawMessage) map[string]json.RawMessage {
	return map[string]json.RawMessage{extensionID: append(json.RawMessage(nil), raw...)}
}

func utteranceData(utteranceID string) json.RawMessage {
	return marshalData(map[string]any{"utterance_id": utteranceID})
}

func alignmentData(utteranceID string, timestamps json.RawMessage) json.RawMessage {
	return marshalData(map[string]any{"utterance_id": utteranceID, "timestamp_info": timestamps})
}

func usageData(utteranceID string, usage json.RawMessage) json.RawMessage {
	return marshalData(map[string]any{"utterance_id": utteranceID, "usage": usage})
}

func marshalData(value any) json.RawMessage {
	payload, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{"code":"internal"}`)
	}
	return payload
}

// audioConfig and synthesizeRequest mirror the TTS OpenAPI request schema. The
// field names are the lowerCamelCase forms the schema declares.
type audioConfig struct {
	AudioEncoding   string `json:"audioEncoding"`
	SampleRateHertz int    `json:"sampleRateHertz"`
}

type synthesizeRequest struct {
	Text        string      `json:"text"`
	VoiceID     string      `json:"voiceId"`
	ModelID     string      `json:"modelId"`
	AudioConfig audioConfig `json:"audioConfig"`
	// Language is a BCP-47 tag. Omitted when the caller named none, in which
	// case Inworld keeps the voice's own prompt and detects the language.
	Language string `json:"language,omitempty"`
}

type rpcStatus struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type synthesizeResult struct {
	AudioContent  string          `json:"audioContent"`
	Usage         json.RawMessage `json:"usage"`
	TimestampInfo json.RawMessage `json:"timestampInfo"`
}

type streamMessage struct {
	Result *synthesizeResult `json:"result"`
	Error  *rpcStatus        `json:"error"`
}
