package openai

import (
	"bytes"
	"context"
	"crypto/rand"
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
	// TTSAdapterID is the identifier returned by an OpenAI TTS session plan.
	TTSAdapterID = "openai.tts.v1"
	// ttsExtensionID namespaces raw OpenAI payloads carried on canonical events.
	ttsExtensionID = "openai.com/audio/speech/v1"

	// ttsSpeechPath is the synthesis resource. CONFIRMED raw: the OpenAPI
	// document declares `post /audio/speech` against server
	// `https://api.openai.com/v1`.
	ttsSpeechPath = "/v1/audio/speech"

	// DefaultTTSModel matches the platform's TypeScript provider and is in the
	// vendor's documented model enum for this endpoint.
	DefaultTTSModel = "gpt-4o-mini-tts"

	// DefaultVoice is the platform's existing choice. `voice` is a REQUIRED
	// field on this endpoint, so a fallback is needed when neither the caller
	// nor the control plane named one.
	DefaultVoice = "coral"

	// ttsResponseFormat is the only format this adapter can emit. CONFIRMED raw,
	// text-to-speech guide: "PCM: Similar to WAV but contains the raw samples in
	// 24kHz (16-bit signed, low-endian), without the header." That is exactly
	// pcm_s16le mono at 24 kHz, with no container to strip.
	//
	// The vendor's `opus` is Ogg-encapsulated rather than raw Opus packets, so
	// it is NOT the portable `opus` encoding this protocol means; mp3/aac/flac
	// are not portable encodings here at all, and `wav` would prepend a RIFF
	// header to raw samples the consumer is told are PCM.
	ttsResponseFormat = "pcm"

	// ttsSampleRateHz is fixed by `response_format: "pcm"`. The endpoint exposes
	// no sample-rate parameter, so a plan asking for any other rate would get
	// 24 kHz audio mislabelled as its requested rate — audible as a pitch shift.
	ttsSampleRateHz = 24_000

	// ttsStreamFormat selects the raw chunked audio body rather than SSE.
	// CONFIRMED raw: `stream_format` is `sse` | `audio`, defaults to `audio`,
	// and "`sse` is not supported for `tts-1` or `tts-1-hd`". The `audio` form
	// streams the samples themselves under `Transfer-Encoding: chunked`, so
	// reading the body incrementally yields audio frames with no base64
	// inflation and works for every model. It is sent explicitly so the wire
	// stays deterministic if the vendor default ever changes.
	ttsStreamFormat = "audio"

	// ttsMaxInputCharacters is the documented ceiling on `input`.
	ttsMaxInputCharacters = 4_096

	// ttsDefaultAudioChunkBytes is the read size used to slice the response body
	// into audio frames. It has no wire meaning; it only bounds how much audio
	// the adapter holds before emitting a frame. 8 KiB is ~85 ms at 24 kHz s16le.
	ttsDefaultAudioChunkBytes = 8 << 10

	// ttsDefaultMaxResponseBytes bounds one synthesis response. Input is capped
	// at 4,096 characters, so 24 kHz mono PCM stays far below this; the limit
	// exists only so a broken upstream cannot grow the reader without bound.
	ttsDefaultMaxResponseBytes = 64 << 20

	// ttsDefaultMaxErrorBytes bounds how much of a failed response body is kept
	// for the canonical error event.
	ttsDefaultMaxErrorBytes = 8 << 10
)

// ttsSupportedModels is the `CreateSpeechRequest.model` enum, transcribed
// verbatim from OpenAI's OpenAPI document. An explicit set makes a typo in a
// plan fail at Open rather than as an upstream 400 mid-call.
var ttsSupportedModels = map[string]struct{}{
	"tts-1":                      {},
	"tts-1-hd":                   {},
	"gpt-4o-mini-tts":            {},
	"gpt-4o-mini-tts-2025-12-15": {},
}

// TTSConfig controls local transport limits. Provider identity, model, voice,
// and credential come from a verified session plan and its request options.
type TTSConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	AudioChunkBytes       int
	MaxResponseBytes      int64
	MaxErrorBytes         int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// TTSAdapter implements OpenAI's POST /v1/audio/speech synthesis API.
//
// OpenAI has no WebSocket TTS surface: the Realtime API synthesizes speech only
// as part of a conversational response, not as a text-in/audio-out service, and
// there is no bidirectional speech endpoint. /v1/audio/speech is nonetheless a
// genuine streaming surface — the response is chunked and the first samples
// arrive long before synthesis completes — so this adapter streams the body
// incrementally rather than buffering a whole utterance.
type TTSAdapter struct {
	id               string
	httpClient       *http.Client
	eventBuffer      int
	audioChunkBytes  int
	maxResponseBytes int64
	maxErrorBytes    int64
	endpointPolicy   ttsEndpointPolicy
}

// NewTTS creates a bounded OpenAI TTS adapter.
func NewTTS(config TTSConfig) (*TTSAdapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = TTSAdapterID
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = 32
	}
	if config.AudioChunkBytes == 0 {
		config.AudioChunkBytes = ttsDefaultAudioChunkBytes
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = ttsDefaultMaxResponseBytes
	}
	if config.MaxErrorBytes == 0 {
		config.MaxErrorBytes = ttsDefaultMaxErrorBytes
	}
	if config.EventBuffer < 1 {
		return nil, errors.New("openai event buffer must be positive")
	}
	if config.AudioChunkBytes < 1 {
		return nil, errors.New("openai audio chunk bytes must be positive")
	}
	if config.MaxResponseBytes < 1 {
		return nil, errors.New("openai maximum response bytes must be positive")
	}
	if config.MaxErrorBytes < 1 {
		return nil, errors.New("openai maximum error bytes must be positive")
	}
	policy, err := newTTSEndpointPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &TTSAdapter{
		id:               config.AdapterID,
		httpClient:       config.HTTPClient,
		eventBuffer:      config.EventBuffer,
		audioChunkBytes:  config.AudioChunkBytes,
		maxResponseBytes: config.MaxResponseBytes,
		maxErrorBytes:    config.MaxErrorBytes,
		endpointPolicy:   policy,
	}, nil
}

func (a *TTSAdapter) ID() string { return a.id }

// Open validates the plan and prepares a synthesis stream. Unlike the Realtime
// socket it performs no network I/O: HTTP has no handshake, so the first
// request happens at CommitText.
func (a *TTSAdapter) Open(_ context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindTTS {
		return nil, fmt.Errorf("openai tts supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "openai" {
		return nil, fmt.Errorf("openai adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	// A websocket route would mean the control plane selected a transport this
	// adapter cannot speak: OpenAI publishes no streaming-TTS socket at all.
	if request.Plan.Route.Transport != protocol.TransportHTTP {
		return nil, fmt.Errorf("openai tts requires http transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("openai tts requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("openai tts media: %w", err)
	}
	if err := ttsValidateMedia(*request.Media); err != nil {
		return nil, err
	}
	model, err := ttsValidateModel(request.Plan.Route.Model)
	if err != nil {
		return nil, err
	}
	credential := request.Plan.Route.Credential
	if credential == nil || credential.Kind != protocol.CredentialBearer || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("openai tts requires a bearer credential")
	}
	endpoint, err := ttsSpeechEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}

	streamCtx, cancel := context.WithCancel(context.Background())
	return &ttsStream{
		ctx:    streamCtx,
		cancel: cancel,
		events: make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		// ONE credential channel. /v1/audio/speech documents exactly one
		// authentication mechanism — `Authorization: Bearer $OPENAI_API_KEY` —
		// and it is identical for a managed and a customer-owned key. OpenAI's
		// short-lived `ek_` client secret is minted by POST
		// /v1/realtime/client_secrets and its session body is oneOf a realtime or
		// a transcription session; nothing documents it against /v1/audio/*.
		// Branching on CredentialSource here would invent a split the vendor does
		// not publish. See the report for the consequence for managed routing.
		httpClient:       ttsHTTPClient(a.httpClient),
		endpoint:         endpoint,
		authorization:    "Bearer " + credential.Value,
		audioChunkBytes:  a.audioChunkBytes,
		maxResponseBytes: a.maxResponseBytes,
		maxErrorBytes:    a.maxErrorBytes,
		model:            model,
		voice:            ttsVoice(request.Options.Voice, request.Plan.Route.Voice),
	}, nil
}

func ttsHTTPClient(client *http.Client) *http.Client {
	if client != nil {
		return client
	}
	return http.DefaultClient
}

// ttsVoice prefers the caller's choice, then the control plane's. A caller that
// sent `provider: "auto"` cannot know which vendor it will get, so the party
// that picked the vendor is the only one that can pick a voice for it.
func ttsVoice(requested, planned string) string {
	if voice := strings.TrimSpace(requested); voice != "" {
		return voice
	}
	if voice := strings.TrimSpace(planned); voice != "" {
		return voice
	}
	return DefaultVoice
}

func ttsValidateModel(model string) (string, error) {
	model = strings.TrimSpace(model)
	if model == "" || model == "auto" {
		return "", errors.New("openai tts requires a concrete model in the session plan")
	}
	if _, ok := ttsSupportedModels[model]; !ok {
		return "", fmt.Errorf("openai tts does not support model %q", model)
	}
	return model, nil
}

func ttsValidateMedia(media protocol.MediaFormat) error {
	if media.Encoding != "pcm_s16le" || media.Channels != 1 {
		return fmt.Errorf("openai tts requires mono pcm_s16le output, got %s/%d channels", media.Encoding, media.Channels)
	}
	if media.SampleRateHz != ttsSampleRateHz {
		return fmt.Errorf("openai tts emits %d Hz audio, got a request for %d", ttsSampleRateHz, media.SampleRateHz)
	}
	return nil
}

func ttsSpeechEndpoint(policy ttsEndpointPolicy, rawEndpoint string) (string, error) {
	endpoint, err := policy.parse(rawEndpoint)
	if err != nil {
		return "", fmt.Errorf("openai tts endpoint: %w", err)
	}
	if endpoint.Path != ttsSpeechPath {
		return "", fmt.Errorf("openai tts endpoint path must be %s, got %q", ttsSpeechPath, endpoint.Path)
	}
	return endpoint.String(), nil
}

// ttsEndpointPolicy is the HTTPS counterpart of upstream.WebSocketPolicy. That
// package currently ships only a WebSocket variant and this adapter must not
// modify shared code, so the same rules — no userinfo, no fragment, no
// preexisting query, no off-standard port, host allowlist — are restated for
// https here. The Inworld TTS adapter carries an identical copy; folding both
// into internal/upstream is the right long-term home. See the report.
type ttsEndpointPolicy struct {
	hosts         map[string]struct{}
	allowInsecure bool
}

func newTTSEndpointPolicy(officialHost string, additionalHosts []string, allowInsecure bool) (ttsEndpointPolicy, error) {
	hosts := make(map[string]struct{}, 1+len(additionalHosts))
	for _, host := range append([]string{officialHost}, additionalHosts...) {
		host = strings.ToLower(strings.TrimSpace(host))
		if host == "" || strings.ContainsAny(host, "/:@?#") {
			return ttsEndpointPolicy{}, errors.New("openai: allowed endpoint host is invalid")
		}
		hosts[host] = struct{}{}
	}
	return ttsEndpointPolicy{hosts: hosts, allowInsecure: allowInsecure}, nil
}

func (p ttsEndpointPolicy) parse(raw string) (*url.URL, error) {
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

type ttsStream struct {
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent

	httpClient       *http.Client
	endpoint         string
	authorization    string
	audioChunkBytes  int
	maxResponseBytes int64
	maxErrorBytes    int64

	model string
	voice string

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

func (s *ttsStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

// WriteAudio and CommitAudio belong to transcription. A synthesizer rejects
// them so the runtime can report the mismatch instead of silently ignoring
// caller audio.
func (s *ttsStream) WriteAudio(context.Context, []byte) error {
	return runtimepkg.ErrUnsupportedOperation
}

func (s *ttsStream) CommitAudio(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// AppendText buffers a fragment. /v1/audio/speech is a single POST carrying the
// whole `input`, so there is nothing to send until the utterance is complete.
func (s *ttsStream) AppendText(_ context.Context, text string) error {
	if text == "" {
		return errors.New("openai tts text is empty")
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return runtimepkg.ErrSessionClosed
	}
	if s.utteranceID != "" {
		return errors.New("openai tts previous utterance has not completed")
	}
	// The documented limit is on characters, so it is counted in runes rather
	// than bytes: a byte count would reject valid non-Latin input early.
	if utf8.RuneCountInString(s.pending.String())+utf8.RuneCountInString(text) > ttsMaxInputCharacters {
		return &runtimepkg.ProviderError{
			Code:           "input_too_large",
			Message:        fmt.Sprintf("OpenAI TTS input exceeds %d characters", ttsMaxInputCharacters),
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
func (s *ttsStream) CommitText(ctx context.Context) error {
	utteranceID, text, requestCtx, requestCancel, done, err := s.beginUtterance()
	if err != nil {
		return err
	}

	payload, err := json.Marshal(ttsSpeechRequest{
		Model:          s.model,
		Input:          text,
		Voice:          s.voice,
		ResponseFormat: ttsResponseFormat,
		StreamFormat:   ttsStreamFormat,
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
	// The success body is raw samples; a failure body is JSON. Accepting both
	// keeps an error response parseable instead of arriving as opaque bytes.
	request.Header.Set("Accept", "application/octet-stream, application/json")

	response, err := s.httpClient.Do(request)
	if err != nil {
		s.abandonUtterance(utteranceID, requestCancel, done)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "OpenAI TTS request could not be sent",
			Retryable: true,
			Cause:     err,
		}
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		providerErr := ttsStatusError(response, s.maxErrorBytes)
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
func (s *ttsStream) Cancel(ctx context.Context) error {
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
func (s *ttsStream) Close(ctx context.Context) error { return s.shutdown(ctx, true) }

// Abort tears the stream down immediately after a terminal runtime failure.
func (s *ttsStream) Abort(context.Context) error { return s.shutdown(context.Background(), false) }

func (s *ttsStream) shutdown(ctx context.Context, graceful bool) error {
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

func (s *ttsStream) beginUtterance() (string, string, context.Context, context.CancelFunc, chan struct{}, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return "", "", nil, nil, nil, runtimepkg.ErrSessionClosed
	}
	if s.utteranceID != "" {
		return "", "", nil, nil, nil, errors.New("openai tts previous utterance has not completed")
	}
	text := s.pending.String()
	if text == "" {
		return "", "", nil, nil, nil, errors.New("openai tts has no buffered text to synthesize")
	}
	utteranceID, err := ttsNewUtteranceID()
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
func (s *ttsStream) abandonUtterance(utteranceID string, requestCancel context.CancelFunc, done chan struct{}) {
	requestCancel()
	s.finishUtterance(utteranceID)
	close(done)
}

func (s *ttsStream) finishUtterance(utteranceID string) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.utteranceID != utteranceID {
		return
	}
	s.utteranceID = ""
	s.requestCancel = nil
	s.requestDone = nil
}

func (s *ttsStream) wasCanceled() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.canceled
}

// readResponse slices the chunked response body into audio frames. The body is
// consumed incrementally — one Read per frame, never io.ReadAll — so samples
// reach the consumer while OpenAI is still synthesizing the rest.
func (s *ttsStream) readResponse(utteranceID string, response *http.Response, requestCancel context.CancelFunc, done chan struct{}) {
	defer func() {
		requestCancel()
		_ = response.Body.Close()
		s.finishUtterance(utteranceID)
		close(done)
		s.readers.Done()
	}()

	body := io.LimitReader(response.Body, s.maxResponseBytes)
	buffer := make([]byte, s.audioChunkBytes)
	audioStarted := false
	audioBytes := 0

	for {
		read, err := body.Read(buffer)
		if read > 0 {
			// The buffer is reused across iterations, so each frame gets its own
			// copy: ProviderEvent.Audio is owned by the runtime once delivered.
			chunk := make([]byte, read)
			copy(chunk, buffer[:read])
			if !audioStarted {
				audioStarted = true
				if !s.emit(runtimepkg.ProviderEvent{
					Type: protocol.EventAudioStarted,
					Data: ttsUtteranceData(utteranceID),
				}) {
					return
				}
			}
			audioBytes += read
			if !s.emit(runtimepkg.ProviderEvent{
				Type:  protocol.EventAudioFrame,
				Data:  ttsUtteranceData(utteranceID),
				Audio: chunk,
			}) {
				return
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			// A cancelled request tears the body mid-read. That is the caller's
			// own Cancel/Abort, not a provider fault, so it stays silent.
			if s.wasCanceled() || s.ctx.Err() != nil {
				return
			}
			s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{
				Code:      "provider_unavailable",
				Message:   "OpenAI TTS audio stream ended unexpectedly",
				Retryable: true,
				Cause:     err,
			}})
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
			Message:   "OpenAI TTS completed without returning audio",
			Retryable: true,
		}})
		return
	}
	s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: ttsUtteranceData(utteranceID)})
}

// emit reports whether the event was delivered. A false result means the stream
// is shutting down and the caller must stop producing.
func (s *ttsStream) emit(event runtimepkg.ProviderEvent) bool {
	select {
	case s.events <- event:
		return true
	case <-s.ctx.Done():
		return false
	}
}

// ttsStatusError maps an HTTP status onto the stable protocol classification.
// OpenAI's error body is `{"error":{"message","type","code","param"}}`, and its
// `code` separates an exhausted balance from a throttle even though both arrive
// as 429 — so the body is consulted before falling back to the status alone.
// The message text is deliberately not copied into Message: it is provider text
// that may echo the synthesized input, and Extensions is the place for raw
// payloads.
func ttsStatusError(response *http.Response, maxErrorBytes int64) *runtimepkg.ProviderError {
	status := response.StatusCode
	body, _ := io.ReadAll(io.LimitReader(response.Body, maxErrorBytes))

	code := ttsStatusErrorCode(status)
	var envelope struct {
		Error *sttErrorDetail `json:"error"`
	}
	if json.Unmarshal(body, &envelope) == nil && envelope.Error != nil {
		if refined := sttErrorCode(envelope.Error.Type, envelope.Error.Code); refined != "provider_unavailable" || status >= 500 {
			code = refined
		}
	}
	providerErr := &runtimepkg.ProviderError{
		Code:    code,
		Message: fmt.Sprintf("OpenAI TTS rejected the synthesis request with status %d", status),
		// Retryability follows the refined code, not the status. An exhausted
		// balance arrives as a 429 exactly like a throttle does, and retrying it
		// against the same account can only fail again.
		Retryable:      code == "provider_rate_limited" || code == "provider_unavailable",
		ProviderStatus: status,
	}
	if json.Valid(body) {
		providerErr.Extensions = ttsExtension(json.RawMessage(body))
	}
	return providerErr
}

func ttsStatusErrorCode(status int) string {
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

func ttsNewUtteranceID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate OpenAI TTS utterance id: %w", err)
	}
	return hex.EncodeToString(value), nil
}

func ttsExtension(raw json.RawMessage) map[string]json.RawMessage {
	return map[string]json.RawMessage{ttsExtensionID: append(json.RawMessage(nil), raw...)}
}

func ttsUtteranceData(utteranceID string) json.RawMessage {
	return marshalData(map[string]any{"utterance_id": utteranceID})
}

// ttsSpeechRequest mirrors the CreateSpeechRequest schema. `model`, `input`, and
// `voice` are the schema's required fields; the snake_case names are what the
// schema declares.
type ttsSpeechRequest struct {
	Model          string `json:"model"`
	Input          string `json:"input"`
	Voice          string `json:"voice"`
	ResponseFormat string `json:"response_format"`
	StreamFormat   string `json:"stream_format"`
}
