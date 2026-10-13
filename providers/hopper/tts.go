package hopper

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
	TTSAdapterID = "hopper.tts.v1"
	// DefaultTTSModel is the only synthesis model Hopper serves.
	DefaultTTSModel = "qwen3-tts"
	// DefaultVoice is a featured English voice, used when neither the caller
	// nor the plan names one; `voice` is required on every request.
	DefaultVoice = "elena"

	speechPath = "/v1/audio/speech"
	// maxInputRunes is Hopper's documented cap on `input`; longer text is a
	// 400 transcript_too_long.
	maxInputRunes      = 1_000
	outputSampleRateHz = 24_000
	// outputBytesPerSecond is 24 kHz mono PCM16, the unit Hopper bills
	// synthesis in: generated audio duration.
	outputBytesPerSecond = outputSampleRateHz * 2

	defaultTTSEventBuffer   = 32
	defaultMaxResponseBytes = 64 << 20
	defaultMaxErrorBytes    = 8 << 10
	defaultAudioChunkBytes  = 8 << 10
	defaultCloseIdleTimeout = 30 * time.Second
	defaultHeaderTimeout    = 10 * time.Second
)

var ttsModels = map[string]struct{}{DefaultTTSModel: {}}

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

// TTSAdapter implements POST /v1/audio/speech with a streamed headerless PCM
// body.
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
		return nil, errors.New("hopper tts: event buffer, chunk size, response bounds and timeouts must be positive")
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
		return nil, fmt.Errorf("hopper tts supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != ProviderName {
		return nil, fmt.Errorf("hopper tts adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportHTTP {
		return nil, fmt.Errorf("hopper tts requires http transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("hopper tts requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("hopper tts media: %w", err)
	}
	if request.Media.Encoding != "pcm_s16le" || request.Media.Channels != 1 || request.Media.SampleRateHz != outputSampleRateHz {
		return nil, &runtimepkg.ProviderError{
			Code:    "unsupported_media",
			Message: fmt.Sprintf("Hopper TTS emits mono pcm_s16le at %d Hz, got %s/%d Hz/%d channels", outputSampleRateHz, request.Media.Encoding, request.Media.SampleRateHz, request.Media.Channels),
			Hint:    "Request mono pcm_s16le output at 24000 Hz.",
		}
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if _, ok := ttsModels[model]; !ok {
		return nil, fmt.Errorf("hopper tts does not support model %q", model)
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("hopper tts requires an api key credential")
	}
	endpoint, err := a.endpointPolicy.Parse(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("hopper tts endpoint: %w", err)
	}
	if endpoint.Path != speechPath {
		return nil, fmt.Errorf("hopper tts endpoint path must be %s, got %q", speechPath, endpoint.Path)
	}
	client := noRedirectClient(a.httpClient)
	streamCtx, cancel := context.WithCancel(context.Background())
	return &ttsStream{
		ctx: streamCtx, cancel: cancel, events: make(chan runtimepkg.ProviderEvent, a.eventBuffer), responseProgress: make(chan struct{}, 1),
		httpClient: client, endpoint: endpoint.String(), credential: strings.TrimSpace(credential.Value),
		audioChunkBytes: a.audioChunkBytes, maxResponseBytes: a.maxResponseBytes, maxErrorBytes: a.maxErrorBytes,
		gracefulCloseIdleTimeout: a.gracefulCloseIdleTimeout, headerTimeout: a.headerTimeout, model: model,
		voice: ttsVoice(request.Options.Voice, request.Plan.Route.Voice),
	}, nil
}

// ttsVoice prefers the caller's choice, then the control plane's, then the
// package default, because the endpoint refuses a request without one. The
// voice fixes the language, so no language is sent.
func ttsVoice(requested, planned string) string {
	if voice := strings.TrimSpace(requested); voice != "" {
		return voice
	}
	if voice := strings.TrimSpace(planned); voice != "" {
		return voice
	}
	return DefaultVoice
}

// noRedirectClient never follows a redirect: a 3xx would replay the API key
// to a URL the endpoint policy never checked.
func noRedirectClient(client *http.Client) *http.Client {
	if client == nil {
		client = http.DefaultClient
	}
	copied := *client
	copied.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &copied
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

	readers         sync.WaitGroup
	closeOnce       sync.Once
	closeErr        error
	stateMu         sync.Mutex
	closed          bool
	pending         strings.Builder
	inFlight        bool
	requestCancel   context.CancelFunc
	requestDone     chan struct{}
	canceled        bool
	billingSequence atomic.Uint64
}

func (s *ttsStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }
func (s *ttsStream) WriteAudio(context.Context, []byte) error {
	return runtimepkg.ErrUnsupportedOperation
}
func (s *ttsStream) CommitAudio(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// AppendText buffers a fragment: the endpoint takes the whole utterance in
// one request, of at most 1000 characters. Text past that is refused here
// rather than split: a cut mid-sentence would be voiced as two utterances
// with a prosody reset between them, so where to break belongs to the caller.
func (s *ttsStream) AppendText(_ context.Context, text string) error {
	if text == "" {
		return errors.New("hopper tts text is empty")
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return runtimepkg.ErrSessionClosed
	}
	if s.inFlight {
		return errors.New("hopper tts previous utterance has not completed")
	}
	if utf8.RuneCountInString(s.pending.String())+utf8.RuneCountInString(text) > maxInputRunes {
		return &runtimepkg.ProviderError{
			Code: "input_too_large", Message: fmt.Sprintf("Hopper TTS input exceeds %d characters", maxInputRunes),
			Hint:      "Commit the text in utterances of at most 1000 characters, for example one sentence or paragraph at a time.",
			Retryable: false, ProviderStatus: http.StatusRequestEntityTooLarge,
		}
	}
	s.pending.WriteString(text)
	return nil
}

// speechRequest is the synthesis body. It carries no speed or other
// generation parameter: qwen3-tts refuses every one of them with 400
// unsupported_parameter, and the session options have none to forward.
// `language` is absent too, because the voice fixes it.
type speechRequest struct {
	Model          string `json:"model"`
	Voice          string `json:"voice"`
	Input          string `json:"input"`
	ResponseFormat string `json:"response_format"`
	Stream         bool   `json:"stream"`
}

// CommitText performs the synthesis request. It returns once the status line
// is known, so a rejection surfaces synchronously; audio then streams on
// Events until audio.done.
func (s *ttsStream) CommitText(ctx context.Context) error {
	text, requestCtx, requestCancel, done, err := s.beginRequest()
	if err != nil {
		return err
	}
	// Cancel a stalled header request when CommitText's caller cancels. Stop
	// the hook when this call returns so its scope does not own the audio body.
	stopCallerCancel := context.AfterFunc(ctx, requestCancel)
	defer stopCallerCancel()
	operationID := fmt.Sprintf("synthesis-%d", s.billingSequence.Add(1))
	payload, err := json.Marshal(speechRequest{Model: s.model, Voice: s.voice, Input: text, ResponseFormat: "pcm", Stream: true})
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
	// Stop reporting false means the callback ran or is running, so the
	// request context is cancelled even if headers arrived: that context also
	// carries the audio body, so the response cannot be used.
	if !headerTimer.Stop() {
		headerTimedOut.Store(true)
		if err == nil {
			_ = response.Body.Close()
			err = context.Canceled
		}
	}
	if err != nil {
		s.abandonRequest(requestCancel, done)
		if headerTimedOut.Load() {
			return &runtimepkg.ProviderError{Code: "request_timeout", Message: fmt.Sprintf("Hopper TTS sent no response headers within %s", s.headerTimeout), Retryable: true, Cause: err}
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Hopper TTS request could not be sent", Retryable: true, Cause: err}
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		// The header timer has stopped, and the runtime still holds its
		// provider lock here, so the error body gets its own bound. A body
		// cut off by it still classifies from the status.
		bodyTimer := time.AfterFunc(s.headerTimeout, requestCancel)
		providerErr := s.statusError(response)
		bodyTimer.Stop()
		_ = response.Body.Close()
		s.abandonRequest(requestCancel, done)
		return providerErr
	}
	if mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type")); err != nil || !strings.EqualFold(mediaType, "audio/pcm") {
		_ = response.Body.Close()
		s.abandonRequest(requestCancel, done)
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Hopper TTS returned an unexpected success content type", Retryable: true, ProviderStatus: response.StatusCode}
	}
	s.readers.Add(1)
	go s.readResponse(requestCtx, response, requestCancel, done, operationID)
	return nil
}

func (s *ttsStream) statusError(response *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(response.Body, s.maxErrorBytes))
	return providerError(fmt.Sprintf("Hopper TTS rejected the synthesis request with status %d", response.StatusCode), parseError(body), response.StatusCode, body)
}

func (s *ttsStream) beginRequest() (string, context.Context, context.CancelFunc, chan struct{}, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return "", nil, nil, nil, runtimepkg.ErrSessionClosed
	}
	if s.inFlight {
		return "", nil, nil, nil, errors.New("hopper tts previous utterance has not completed")
	}
	text := s.pending.String()
	if text == "" {
		return "", nil, nil, nil, errors.New("hopper tts has no buffered text to synthesize")
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
	s.releaseRequest(done)
	close(done)
}

// releaseRequest frees the in-flight slot of the request that owns done. It
// is idempotent and leaves a newer request alone, so the reader can release
// its slot before publishing audio.done and still run its deferred cleanup
// after the caller has started the next utterance.
func (s *ttsStream) releaseRequest(done chan struct{}) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.requestDone != done {
		return
	}
	s.inFlight = false
	s.requestCancel = nil
	s.requestDone = nil
}

func (s *ttsStream) wasCanceled() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.canceled
}

// billing is one synthesis's observation. Hopper bills generated audio
// duration, so the quantity is the PCM received, as an exact fraction of a
// second, and it is complete only once the body ends cleanly.
func (s *ttsStream) billing(operationID string, audioBytes int64, complete bool) *protocol.BillingObservation {
	return &protocol.BillingObservation{
		OperationID: operationID, Model: s.model, Mode: "streaming", Complete: complete,
		Quantities:           map[string]int64{"duration_seconds": audioBytes},
		QuantityDenominators: map[string]int64{"duration_seconds": outputBytesPerSecond},
	}
}

// readResponse slices the streamed PCM into audio frames as it arrives. Frames
// carry whole samples: an odd byte left by a chunk boundary waits for the next
// read.
func (s *ttsStream) readResponse(requestCtx context.Context, response *http.Response, requestCancel context.CancelFunc, done chan struct{}, operationID string) {
	defer func() {
		requestCancel()
		_ = response.Body.Close()
		s.releaseRequest(done)
		close(done)
		s.readers.Done()
	}()
	// finish publishes the request's last event. The slot is released first:
	// a caller that reacts to audio.done by starting the next utterance must
	// not find this one still in flight.
	finish := func(event runtimepkg.ProviderEvent) {
		s.releaseRequest(done)
		s.emit(requestCtx, event)
	}
	data := marshalData(map[string]any{"sample_rate_hz": outputSampleRateHz})
	reader := &io.LimitedReader{R: response.Body, N: s.maxResponseBytes + 1}
	buffer := make([]byte, s.audioChunkBytes)
	var carry []byte
	started := false
	var total, emitted int64
	for {
		count, err := reader.Read(buffer)
		if count > 0 {
			s.reportResponseProgress()
			total += int64(count)
			if total > s.maxResponseBytes {
				finish(runtimepkg.ProviderEvent{Billing: s.billing(operationID, emitted, false), Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Hopper TTS response exceeded the configured limit", Retryable: true}})
				return
			}
			audio := append(carry, buffer[:count]...)
			carry = nil
			if len(audio)%2 == 1 {
				carry = []byte{audio[len(audio)-1]}
				audio = audio[:len(audio)-1]
			}
			if len(audio) > 0 {
				if !started {
					started = true
					if !s.emit(requestCtx, runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: data, Billing: s.billing(operationID, 0, false)}) {
						return
					}
				}
				if !s.emit(requestCtx, runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Data: data, Audio: audio}) {
					return
				}
				emitted += int64(len(audio))
			}
		}
		if err == nil {
			continue
		}
		if s.wasCanceled() || s.ctx.Err() != nil {
			return
		}
		if !errors.Is(err, io.EOF) {
			// A failure after the status line cuts the chunked body with no
			// trailer, so a torn read is a failed synthesis, never a short
			// utterance.
			finish(runtimepkg.ProviderEvent{Billing: s.billing(operationID, emitted, false), Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Hopper TTS audio stream ended unexpectedly", Retryable: true, Cause: err}})
			return
		}
		if !started {
			finish(runtimepkg.ProviderEvent{Billing: s.billing(operationID, 0, false), Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Hopper TTS completed without returning audio", Retryable: true}})
			return
		}
		finish(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: data, Billing: s.billing(operationID, emitted, true)})
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
	s.pending.Reset()
	closed := s.closed
	cancel, done := s.requestCancel, s.requestDone
	if cancel != nil {
		s.canceled = true
	}
	s.stateMu.Unlock()
	if cancel == nil {
		if closed {
			return runtimepkg.ErrSessionClosed
		}
		return nil
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

// Close lets every reader finish so its audio and audio.done are delivered,
// then releases the stream. It waits on the readers rather than the in-flight
// slot, which a reader frees just before its last event.
func (s *ttsStream) Close(ctx context.Context) error { return s.shutdown(ctx, true) }

// Abort tears the stream down immediately after a terminal runtime failure.
func (s *ttsStream) Abort(context.Context) error { return s.shutdown(context.Background(), false) }

func (s *ttsStream) shutdown(ctx context.Context, graceful bool) error {
	s.closeOnce.Do(func() {
		s.stateMu.Lock()
		s.closed = true
		s.pending.Reset()
		s.stateMu.Unlock()
		// CommitText refuses once closed is set, and the runtime never runs it
		// beside Close, so no reader is added while this waits.
		idle := make(chan struct{})
		go func() {
			s.readers.Wait()
			close(idle)
		}()
		if graceful {
			s.closeErr = s.waitForRequest(ctx, idle)
		}
		s.cancel()
		<-idle
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
			return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Hopper TTS response stalled during graceful close", Retryable: true, Cause: context.DeadlineExceeded}
		}
	}
}
