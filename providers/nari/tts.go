package nari

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// TTSAdapterID is the identifier the connector registers this adapter under.
	TTSAdapterID = "nari.tts.v1"
	// DefaultTTSModel is the latency-optimized serving class.
	DefaultTTSModel = "qwen3-tts-fast"
	// DefaultVoice is one of the two voices Nari recommends; `voice` is
	// required on every request.
	DefaultVoice = "claire"

	speechPath         = "/v1/audio/speech"
	maxInputCodePoints = 2_048
	outputSampleRateHz = 24_000

	defaultTTSEventBuffer   = 32
	defaultMaxResponseBytes = 64 << 20
	defaultMaxErrorBytes    = 8 << 10
	defaultAudioChunkBytes  = 8 << 10
	defaultCloseIdleTimeout = 30 * time.Second
	defaultHeaderTimeout    = 10 * time.Second
)

// ttsModels are the two serving classes of Qwen3-TTS 1.7B. They share one
// voice catalog and one wire shape.
var ttsModels = map[string]struct{}{"qwen3-tts": {}, "qwen3-tts-fast": {}}

// TTSConfig controls local transport bounds. Provider identity, model, voice,
// and credential always come from the verified plan.
type TTSConfig struct {
	AdapterID        string
	HTTPClient       *http.Client
	EventBuffer      int
	AudioChunkBytes  int
	MaxResponseBytes int64
	MaxErrorBytes    int64
	// GracefulCloseIdleTimeout bounds inactivity after Close begins. It resets
	// whenever response bytes arrive, so progressing synthesis is not capped.
	GracefulCloseIdleTimeout time.Duration
	// HeaderTimeout bounds the wait for the response status line. It does not
	// bound the streamed body, which lasts as long as the utterance.
	HeaderTimeout         time.Duration
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// TTSAdapter implements POST /v1/audio/speech with a streamed PCM body.
type TTSAdapter struct {
	id                       string
	httpClient               *http.Client
	eventBuffer              int
	audioChunkBytes          int
	maxResponseBytes         int64
	maxErrorBytes            int64
	gracefulCloseIdleTimeout time.Duration
	headerTimeout            time.Duration
	endpointPolicy           upstream.HTTPPolicy
}

// NewTTS validates the configuration and builds the adapter.
func NewTTS(config TTSConfig) (*TTSAdapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = TTSAdapterID
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = defaultTTSEventBuffer
	}
	if config.AudioChunkBytes == 0 {
		config.AudioChunkBytes = defaultAudioChunkBytes
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = defaultMaxResponseBytes
	}
	if config.MaxErrorBytes == 0 {
		config.MaxErrorBytes = defaultMaxErrorBytes
	}
	if config.GracefulCloseIdleTimeout == 0 {
		config.GracefulCloseIdleTimeout = defaultCloseIdleTimeout
	}
	if config.HeaderTimeout == 0 {
		config.HeaderTimeout = defaultHeaderTimeout
	}
	if config.EventBuffer < 1 || config.AudioChunkBytes < 1 || config.MaxResponseBytes < 1 || config.MaxErrorBytes < 1 || config.GracefulCloseIdleTimeout < 0 || config.HeaderTimeout < 0 {
		return nil, errors.New("nari tts: event buffer, chunk size, response bounds and timeouts must be positive")
	}
	policy, err := upstream.NewHTTPPolicy(officialHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &TTSAdapter{
		id: config.AdapterID, httpClient: config.HTTPClient, eventBuffer: config.EventBuffer,
		audioChunkBytes: config.AudioChunkBytes, maxResponseBytes: config.MaxResponseBytes, maxErrorBytes: config.MaxErrorBytes,
		gracefulCloseIdleTimeout: config.GracefulCloseIdleTimeout, headerTimeout: config.HeaderTimeout, endpointPolicy: policy,
	}, nil
}

// ID returns the adapter identifier.
func (a *TTSAdapter) ID() string { return a.id }

// Open validates the plan. HTTP has no handshake, so the first request happens
// at CommitText.
func (a *TTSAdapter) Open(_ context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindTTS {
		return nil, fmt.Errorf("nari tts supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != ProviderName {
		return nil, fmt.Errorf("nari tts adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportHTTP {
		return nil, fmt.Errorf("nari tts requires http transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("nari tts requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("nari tts media: %w", err)
	}
	if request.Media.Encoding != "pcm_s16le" || request.Media.Channels != 1 || request.Media.SampleRateHz != outputSampleRateHz {
		return nil, &runtimepkg.ProviderError{
			Code:    "unsupported_media",
			Message: fmt.Sprintf("Nari TTS emits mono pcm_s16le at %d Hz, got %s/%d Hz/%d channels", outputSampleRateHz, request.Media.Encoding, request.Media.SampleRateHz, request.Media.Channels),
			Hint:    "Request mono pcm_s16le output at 24000 Hz.",
		}
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if _, ok := ttsModels[model]; !ok {
		return nil, fmt.Errorf("nari tts does not support model %q", model)
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("nari tts requires a bearer credential")
	}
	endpoint, err := a.endpointPolicy.Parse(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("nari tts endpoint: %w", err)
	}
	if endpoint.Path != speechPath {
		return nil, fmt.Errorf("nari tts endpoint path must be %s, got %q", speechPath, endpoint.Path)
	}
	client := a.httpClient
	if client == nil {
		client = http.DefaultClient
	}
	streamCtx, cancel := context.WithCancel(context.Background())
	return &ttsStream{
		ctx: streamCtx, cancel: cancel, events: make(chan runtimepkg.ProviderEvent, a.eventBuffer), responseProgress: make(chan struct{}, 1),
		httpClient: client, endpoint: endpoint.String(), credential: credential.Value,
		audioChunkBytes: a.audioChunkBytes, maxResponseBytes: a.maxResponseBytes, maxErrorBytes: a.maxErrorBytes,
		gracefulCloseIdleTimeout: a.gracefulCloseIdleTimeout, headerTimeout: a.headerTimeout, model: model, voice: ttsVoice(request.Options.Voice, request.Plan.Route.Voice),
	}, nil
}

// ttsVoice prefers the caller's choice, then the control plane's, then the
// package default, because the endpoint refuses a request without one.
func ttsVoice(requested, planned string) string {
	if voice := strings.TrimSpace(requested); voice != "" {
		return voice
	}
	if voice := strings.TrimSpace(planned); voice != "" {
		return voice
	}
	return DefaultVoice
}

type ttsStream struct {
	ctx              context.Context
	cancel           context.CancelFunc
	events           chan runtimepkg.ProviderEvent
	responseProgress chan struct{}

	httpClient               *http.Client
	endpoint                 string
	credential               string
	audioChunkBytes          int
	maxResponseBytes         int64
	maxErrorBytes            int64
	gracefulCloseIdleTimeout time.Duration
	headerTimeout            time.Duration
	model                    string
	voice                    string

	readers       sync.WaitGroup
	closeOnce     sync.Once
	closeErr      error
	stateMu       sync.Mutex
	closed        bool
	pending       strings.Builder
	inFlight      bool
	requestCancel context.CancelFunc
	requestDone   chan struct{}
	canceled      bool
}

func (s *ttsStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }
func (s *ttsStream) WriteAudio(context.Context, []byte) error {
	return runtimepkg.ErrUnsupportedOperation
}
func (s *ttsStream) CommitAudio(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// AppendText buffers a fragment: the endpoint takes the whole utterance in
// one request. The vendor counts code points after trimming surrounding
// whitespace; the raw count is checked instead, so whitespace-only appends
// cannot grow the buffer past the cap.
func (s *ttsStream) AppendText(_ context.Context, text string) error {
	if text == "" {
		return errors.New("nari tts text is empty")
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return runtimepkg.ErrSessionClosed
	}
	if s.inFlight {
		return errors.New("nari tts previous utterance has not completed")
	}
	if utf8.RuneCountInString(s.pending.String())+utf8.RuneCountInString(text) > maxInputCodePoints {
		return &runtimepkg.ProviderError{Code: "input_too_large", Message: "Nari TTS input exceeds 2048 characters", Retryable: false, ProviderStatus: http.StatusRequestEntityTooLarge}
	}
	s.pending.WriteString(text)
	return nil
}

// speechRequest is the generate-speech body. `language` is deliberately
// absent: the voice fixes the language, and a disagreeing one is a 400.
type speechRequest struct {
	Model          string `json:"model"`
	Input          string `json:"input"`
	Voice          string `json:"voice"`
	Stream         bool   `json:"stream"`
	ResponseFormat string `json:"response_format"`
}

// CommitText performs the synthesis request. It returns once the status line
// is known, so a rejection surfaces synchronously; audio then streams on
// Events until audio.done.
func (s *ttsStream) CommitText(ctx context.Context) error {
	text, requestCtx, requestCancel, done, err := s.beginRequest()
	if err != nil {
		return err
	}
	payload, err := json.Marshal(speechRequest{Model: s.model, Input: text, Voice: s.voice, Stream: true, ResponseFormat: "pcm"})
	if err != nil {
		s.abandonRequest(requestCancel, done)
		return err
	}
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, s.endpoint, bytes.NewReader(payload))
	if err != nil {
		s.abandonRequest(requestCancel, done)
		return err
	}
	request.Header.Set("Authorization", "Bearer "+s.credential)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "audio/pcm, application/json")
	// The runtime holds its provider lock across CommitText, so a vendor that
	// accepts the connection but never answers would wedge the session.
	var headerTimedOut atomic.Bool
	headerTimer := time.AfterFunc(s.headerTimeout, func() {
		headerTimedOut.Store(true)
		requestCancel()
	})
	response, err := s.httpClient.Do(request)
	headerTimer.Stop()
	if err != nil {
		s.abandonRequest(requestCancel, done)
		if headerTimedOut.Load() {
			return &runtimepkg.ProviderError{Code: "request_timeout", Message: fmt.Sprintf("Nari TTS sent no response headers within %s", s.headerTimeout), Retryable: true, Cause: err}
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Nari TTS request could not be sent", Retryable: true, Cause: err}
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		providerErr := s.statusError(response)
		_ = response.Body.Close()
		s.abandonRequest(requestCancel, done)
		return providerErr
	}
	if mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type")); err != nil || !strings.EqualFold(mediaType, "audio/pcm") {
		_ = response.Body.Close()
		s.abandonRequest(requestCancel, done)
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Nari TTS returned an unexpected success content type", Retryable: true, ProviderStatus: response.StatusCode}
	}
	s.readers.Add(1)
	go s.readResponse(requestCtx, response, requestCancel, done)
	return nil
}

func (s *ttsStream) statusError(response *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(response.Body, s.maxErrorBytes))
	var envelope struct {
		Error *errorDetail `json:"error"`
	}
	_ = json.Unmarshal(body, &envelope)
	return providerError(fmt.Sprintf("Nari TTS rejected the synthesis request with status %d", response.StatusCode), envelope.Error, response.StatusCode, body)
}

func (s *ttsStream) beginRequest() (string, context.Context, context.CancelFunc, chan struct{}, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return "", nil, nil, nil, runtimepkg.ErrSessionClosed
	}
	if s.inFlight {
		return "", nil, nil, nil, errors.New("nari tts previous utterance has not completed")
	}
	text := s.pending.String()
	if text == "" {
		return "", nil, nil, nil, errors.New("nari tts has no buffered text to synthesize")
	}
	requestCtx, requestCancel := context.WithCancel(s.ctx)
	done := make(chan struct{})
	s.pending.Reset()
	s.inFlight = true
	s.requestCancel = requestCancel
	s.requestDone = done
	s.canceled = false
	return text, requestCtx, requestCancel, done, nil
}

func (s *ttsStream) abandonRequest(cancel context.CancelFunc, done chan struct{}) {
	cancel()
	s.finishRequest()
	close(done)
}

func (s *ttsStream) finishRequest() {
	s.stateMu.Lock()
	s.inFlight = false
	s.requestCancel = nil
	s.requestDone = nil
	s.stateMu.Unlock()
}

func (s *ttsStream) wasCanceled() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.canceled
}

// readResponse slices the streamed body into audio frames as it arrives.
func (s *ttsStream) readResponse(requestCtx context.Context, response *http.Response, requestCancel context.CancelFunc, done chan struct{}) {
	defer func() {
		requestCancel()
		_ = response.Body.Close()
		s.finishRequest()
		close(done)
		s.readers.Done()
	}()
	data := marshalData(map[string]any{"provider_request_id": strings.TrimSpace(response.Header.Get(requestIDHeader))})
	if response.Header.Get(requestIDHeader) != "" {
		if !s.emit(requestCtx, runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: data}) {
			return
		}
	}
	reader := &io.LimitedReader{R: response.Body, N: s.maxResponseBytes + 1}
	buffer := make([]byte, s.audioChunkBytes)
	started := false
	var total int64
	for {
		count, err := reader.Read(buffer)
		if count > 0 {
			s.reportResponseProgress()
			total += int64(count)
			if total > s.maxResponseBytes {
				s.emit(requestCtx, runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Nari TTS response exceeded the configured limit", Retryable: true}})
				return
			}
			if !started {
				started = true
				if !s.emit(requestCtx, runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: data}) {
					return
				}
			}
			audio := append([]byte(nil), buffer[:count]...)
			if !s.emit(requestCtx, runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Data: data, Audio: audio}) {
				return
			}
		}
		if err == nil {
			continue
		}
		if s.wasCanceled() || s.ctx.Err() != nil {
			return
		}
		if !errors.Is(err, io.EOF) {
			// The vendor documents no trailer: a failure after the status line
			// just cuts the body, so a torn read is a failed synthesis, never a
			// short utterance.
			s.emit(requestCtx, runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Nari TTS audio stream ended unexpectedly", Retryable: true, Cause: err}})
			return
		}
		if !started {
			s.emit(requestCtx, runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Nari TTS completed without returning audio", Retryable: true}})
			return
		}
		s.emit(requestCtx, runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: data})
		return
	}
}

func (s *ttsStream) reportResponseProgress() {
	select {
	case s.responseProgress <- struct{}{}:
	default:
	}
}

// emit does not let Close interrupt a blocked send: a runtime still draining
// events must receive every frame and audio.done. The graceful-close idle
// timer cancels s.ctx if the consumer is abandoned; Cancel interrupts just
// this request through requestCtx.
func (s *ttsStream) emit(requestCtx context.Context, event runtimepkg.ProviderEvent) bool {
	select {
	case s.events <- event:
		s.reportResponseProgress()
		return true
	case <-requestCtx.Done():
		return false
	case <-s.ctx.Done():
		return false
	}
}

// Cancel discards buffered text and aborts an in-flight synthesis. HTTP has no
// cancel message; dropping the request is the only way to stop generation.
func (s *ttsStream) Cancel(ctx context.Context) error {
	s.stateMu.Lock()
	hadPending := s.pending.Len() > 0
	s.pending.Reset()
	cancel, done := s.requestCancel, s.requestDone
	if cancel != nil {
		s.canceled = true
	}
	s.stateMu.Unlock()
	if cancel == nil {
		if hadPending {
			return nil
		}
		return runtimepkg.ErrSessionClosed
	}
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ctx.Done():
		return nil
	}
}

// Close lets an in-flight synthesis finish so its audio is delivered, then
// releases the stream.
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
			s.closeErr = s.waitForRequest(ctx, done)
		}
		s.cancel()
		s.readers.Wait()
		close(s.events)
	})
	return s.closeErr
}

func (s *ttsStream) waitForRequest(ctx context.Context, done <-chan struct{}) error {
	timer := time.NewTimer(s.gracefulCloseIdleTimeout)
	defer timer.Stop()
	for {
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-s.responseProgress:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(s.gracefulCloseIdleTimeout)
		case <-timer.C:
			select {
			case <-done:
				return nil
			default:
			}
			return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Nari TTS response stalled during graceful close", Retryable: true, Cause: context.DeadlineExceeded}
		}
	}
}
