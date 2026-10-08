package munsit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
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
	TTSAdapterID = "munsit.tts.v1"
	// DefaultTTSModel is the only synthesis model GET /models lists. The model
	// id is the last segment of the synthesis path.
	DefaultTTSModel = "faseeh-v1-preview"
	// DefaultVoice is the Arabic (Najdi) voice; `voice_id` is required on
	// every request.
	DefaultVoice = "ar-najdi-male-2"
	// DefaultEnglishVoice is "Eric (V2)", an American English voice.
	DefaultEnglishVoice = "Ly3XBDAK8rmxpRAzDZ3zcwYY"

	speechPathPrefix = "/api/v1/text-to-speech/"
	// maxInputRunes is a local bound on one utterance. Munsit documents no
	// ceiling; this keeps a runaway buffer from becoming one very expensive
	// request at 2 credits a character.
	maxInputRunes = 5_000
	// creditsPerCharacter is what Munsit bills a synthesis, counted on the
	// text exactly as sent.
	creditsPerCharacter = 2
	// stability is the documented default, sent explicitly.
	stability = 0.5

	defaultTTSEventBuffer   = 32
	defaultMaxResponseBytes = 64 << 20
	defaultMaxErrorBytes    = 8 << 10
	defaultAudioChunkBytes  = 8 << 10
	defaultCloseIdleTimeout = 30 * time.Second
	defaultHeaderTimeout    = 10 * time.Second
)

var ttsModels = map[string]struct{}{DefaultTTSModel: {}}

// ttsSampleRates are the standard output rates routed here. Munsit renders
// any rate from 8000 to 48000.
var ttsSampleRates = map[int]struct{}{8_000: {}, 16_000: {}, 22_050: {}, 24_000: {}, 44_100: {}, 48_000: {}}

// ttsLanguages are the session languages routed to Munsit. Language is not
// sent: every voice reads both, and the language only picks the default voice.
var ttsLanguages = map[string]struct{}{"": {}, "ar": {}, "en": {}}

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

// TTSAdapter implements POST /api/v1/text-to-speech/{model} with a streamed
// headerless PCM body.
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
		return nil, errors.New("munsit tts: event buffer, chunk size, response bounds and timeouts must be positive")
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
		return nil, fmt.Errorf("munsit tts supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != ProviderName {
		return nil, fmt.Errorf("munsit tts adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportHTTP {
		return nil, fmt.Errorf("munsit tts requires http transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("munsit tts requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("munsit tts media: %w", err)
	}
	if _, ok := ttsSampleRates[request.Media.SampleRateHz]; !ok || request.Media.Encoding != "pcm_s16le" || request.Media.Channels != 1 {
		return nil, &runtimepkg.ProviderError{
			Code:    "unsupported_media",
			Message: fmt.Sprintf("Munsit TTS emits mono pcm_s16le at 8000, 16000, 22050, 24000, 44100 or 48000 Hz, got %s/%d Hz/%d channels", request.Media.Encoding, request.Media.SampleRateHz, request.Media.Channels),
			Hint:    "Request mono pcm_s16le output at 24000 Hz.",
		}
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if _, ok := ttsModels[model]; !ok {
		return nil, fmt.Errorf("munsit tts does not support model %q", model)
	}
	language := baseLanguage(request.Options.Language)
	if _, ok := ttsLanguages[language]; !ok {
		return nil, &runtimepkg.ProviderError{Code: "unsupported_language", Message: fmt.Sprintf("Munsit TTS is routed for ar and en only, got %q", request.Options.Language)}
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("munsit tts requires an api key credential")
	}
	endpoint, err := a.endpointPolicy.Parse(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("munsit tts endpoint: %w", err)
	}
	if endpoint.Path != speechPathPrefix+model {
		return nil, fmt.Errorf("munsit tts endpoint path must be %s%s, got %q", speechPathPrefix, model, endpoint.Path)
	}
	client := noRedirectClient(a.httpClient)
	streamCtx, cancel := context.WithCancel(context.Background())
	return &ttsStream{
		ctx: streamCtx, cancel: cancel, events: make(chan runtimepkg.ProviderEvent, a.eventBuffer), responseProgress: make(chan struct{}, 1),
		httpClient: client, endpoint: endpoint.String(), credential: strings.TrimSpace(credential.Value),
		audioChunkBytes: a.audioChunkBytes, maxResponseBytes: a.maxResponseBytes, maxErrorBytes: a.maxErrorBytes,
		gracefulCloseIdleTimeout: a.gracefulCloseIdleTimeout, headerTimeout: a.headerTimeout, model: model,
		sampleRate: request.Media.SampleRateHz,
		voice:      ttsVoice(request.Options.Voice, request.Plan.Route.Voice, language),
	}, nil
}

// ttsVoice prefers the caller's choice, then the control plane's, then the
// language's default, because the endpoint refuses a request without one.
// A voice in the request is always honored. The catalog default only reaches
// a plan the board had no cell for; on an English session with no request
// voice that fill becomes the English default.
func ttsVoice(requested, planned, language string) string {
	if voice := strings.TrimSpace(requested); voice != "" {
		return voice
	}
	planned = strings.TrimSpace(planned)
	if language == "en" && (planned == "" || planned == DefaultVoice) {
		return DefaultEnglishVoice
	}
	if planned != "" {
		return planned
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
	sampleRate               int
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
// one request.
func (s *ttsStream) AppendText(_ context.Context, text string) error {
	if text == "" {
		return errors.New("munsit tts text is empty")
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return runtimepkg.ErrSessionClosed
	}
	if s.inFlight {
		return errors.New("munsit tts previous utterance has not completed")
	}
	if utf8.RuneCountInString(s.pending.String())+utf8.RuneCountInString(text) > maxInputRunes {
		return &runtimepkg.ProviderError{Code: "input_too_large", Message: fmt.Sprintf("Munsit TTS input exceeds %d characters", maxInputRunes), Retryable: false, ProviderStatus: http.StatusRequestEntityTooLarge}
	}
	s.pending.WriteString(text)
	return nil
}

// speechRequest is the synthesis body. `dialect` and `speed` are left to the
// vendor defaults (auto and 1.0).
type speechRequest struct {
	VoiceID    string  `json:"voice_id"`
	Text       string  `json:"text"`
	Stability  float64 `json:"stability"`
	Streaming  bool    `json:"streaming"`
	SampleRate int     `json:"sample_rate"`
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
	billing := &protocol.BillingObservation{
		OperationID: fmt.Sprintf("synthesis-%d", s.billingSequence.Add(1)),
		Model:       s.model, Mode: "streaming",
		// 2 credits per character, in thousandths of a credit.
		Quantities: map[string]int64{"credits": int64(utf8.RuneCountInString(text)) * creditsPerCharacter * 1000},
	}
	payload, err := json.Marshal(speechRequest{VoiceID: s.voice, Text: text, Stability: stability, Streaming: true, SampleRate: s.sampleRate})
	if err != nil {
		s.abandonRequest(requestCancel, done)
		return err
	}
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, s.endpoint, bytes.NewReader(payload))
	if err != nil {
		s.abandonRequest(requestCancel, done)
		return err
	}
	request.Header.Set(apiKeyHeader, s.credential)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "audio/raw, application/json")
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
			return &runtimepkg.ProviderError{Code: "request_timeout", Message: fmt.Sprintf("Munsit TTS sent no response headers within %s", s.headerTimeout), Retryable: true, Cause: err}
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Munsit TTS request could not be sent", Retryable: true, Cause: err}
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
	if err := s.checkFormat(response.Header.Get("Content-Type")); err != nil {
		err.ProviderStatus = response.StatusCode
		_ = response.Body.Close()
		s.abandonRequest(requestCancel, done)
		return err
	}
	s.readers.Add(1)
	go s.readResponse(requestCtx, response, requestCancel, done, billing)
	return nil
}

// checkFormat reads audio/raw;codec=pcm16;rate=N;channels=1. The content
// type is the one place Munsit says what it rendered, and a rate other than
// the session's would play at the wrong pitch with no error.
func (s *ttsStream) checkFormat(contentType string) *runtimepkg.ProviderError {
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil || !strings.EqualFold(mediaType, "audio/raw") {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Munsit TTS returned an unexpected success content type", Retryable: true}
	}
	rate, rateErr := strconv.Atoi(params["rate"])
	codec, channels := params["codec"], params["channels"]
	if rateErr != nil || rate != s.sampleRate || (codec != "" && !strings.EqualFold(codec, "pcm16")) || (channels != "" && channels != "1") {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: fmt.Sprintf("Munsit TTS returned %q; want mono pcm16 at %d Hz", contentType, s.sampleRate), Retryable: true}
	}
	return nil
}

func (s *ttsStream) statusError(response *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(response.Body, s.maxErrorBytes))
	var detail restError
	_ = json.Unmarshal(body, &detail)
	return providerError(fmt.Sprintf("Munsit TTS rejected the synthesis request with status %d", response.StatusCode), detail, response.StatusCode, body)
}

func (s *ttsStream) beginRequest() (string, context.Context, context.CancelFunc, chan struct{}, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return "", nil, nil, nil, runtimepkg.ErrSessionClosed
	}
	if s.inFlight {
		return "", nil, nil, nil, errors.New("munsit tts previous utterance has not completed")
	}
	text := s.pending.String()
	if text == "" {
		return "", nil, nil, nil, errors.New("munsit tts has no buffered text to synthesize")
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

// readResponse slices the streamed PCM into audio frames as it arrives. Frames
// carry whole samples: an odd byte left by a chunk boundary waits for the next
// read.
func (s *ttsStream) readResponse(requestCtx context.Context, response *http.Response, requestCancel context.CancelFunc, done chan struct{}, billing *protocol.BillingObservation) {
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
	data := marshalData(map[string]any{"sample_rate_hz": s.sampleRate})
	reader := &io.LimitedReader{R: response.Body, N: s.maxResponseBytes + 1}
	buffer := make([]byte, s.audioChunkBytes)
	var carry []byte
	started := false
	var total int64
	for {
		count, err := reader.Read(buffer)
		if count > 0 {
			s.reportResponseProgress()
			total += int64(count)
			if total > s.maxResponseBytes {
				finish(runtimepkg.ProviderEvent{Billing: billing, Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Munsit TTS response exceeded the configured limit", Retryable: true}})
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
					if !s.emit(requestCtx, runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: data, Billing: billing}) {
						return
					}
				}
				if !s.emit(requestCtx, runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Data: data, Audio: audio}) {
					return
				}
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
			finish(runtimepkg.ProviderEvent{Billing: billing, Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Munsit TTS audio stream ended unexpectedly", Retryable: true, Cause: err}})
			return
		}
		if !started {
			finish(runtimepkg.ProviderEvent{Billing: billing, Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Munsit TTS completed without returning audio", Retryable: true}})
			return
		}
		completeBilling := *billing
		completeBilling.Complete = true
		finish(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: data, Billing: &completeBilling})
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
			return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Munsit TTS response stalled during graceful close", Retryable: true, Cause: context.DeadlineExceeded}
		}
	}
}
