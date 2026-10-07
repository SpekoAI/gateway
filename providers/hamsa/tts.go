package hamsa

import (
	"bytes"
	"context"
	"encoding/binary"
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
	TTSAdapterID = "hamsa.tts.v1"
	// DefaultTTSModel names Hamsa's one realtime synthesis engine. The API
	// takes no model field, so the id is a catalog label, not a vendor value.
	DefaultTTSModel = "default"
	// DefaultVoice is the Modern Standard Arabic voice the API reference pairs
	// with dialect `msa`. Hamsa refuses a request without a speaker.
	DefaultVoice = "Salem"

	ttsEndpointPath = "/v1/realtime/tts-stream"
	// maxInputCharacters is the documented per-request text ceiling.
	maxInputCharacters = 2_000
	// maxWAVHeaderBytes bounds how much body is buffered while looking for
	// a data chunk, if Hamsa ever prefixes one. Measured 2026-10-07 the body
	// is bare PCM.
	maxWAVHeaderBytes = 4 << 10

	defaultTTSEventBuffer   = 32
	defaultMaxResponseBytes = 64 << 20
	defaultMaxErrorBytes    = 8 << 10
	defaultAudioChunkBytes  = 8 << 10
	defaultCloseIdleTimeout = 30 * time.Second
	defaultHeaderTimeout    = 10 * time.Second
)

var ttsModels = map[string]struct{}{DefaultTTSModel: {}}

// ttsSampleRates maps the PCM rates Hamsa emits to its `sampleRate` values.
// mu-law is not offered: the relay carries PCM only.
var ttsSampleRates = map[int]string{16_000: "16k", 8_000: "8k"}

// ttsDialects maps a session language onto Hamsa's `dialect` code, using the
// BCP-47 table of Hamsa's Voices Catalog API. Bare `ar` is Modern Standard
// Arabic; `ar-SA` is Hamsa's Saudi dialect, not its separate `ar-sa` Gulf
// code, because that is what the catalog tags Saudi voices with.
var ttsDialects = map[string]string{
	"ar": "msa", "ar-sa": "ksa", "ar-eg": "egy", "ar-ps": "pls", "ar-sy": "syr",
	"ar-iq": "irq", "ar-jo": "jor", "ar-lb": "leb", "ar-ae": "uae", "ar-bh": "bah",
	"ar-qa": "qat", "ar-kw": "kuw", "ar-om": "oma", "en": "en",
}

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

// TTSAdapter implements POST /v1/realtime/tts-stream, a chunked PCM body.
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
	if config.EventBuffer < 1 || config.AudioChunkBytes < 2 || config.MaxResponseBytes < 1 || config.MaxErrorBytes < 1 || config.GracefulCloseIdleTimeout < 0 || config.HeaderTimeout < 0 {
		return nil, errors.New("hamsa tts: event buffer, chunk size, response bounds and timeouts must be positive")
	}
	policy, err := upstream.NewHTTPPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
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
		return nil, fmt.Errorf("hamsa tts supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "hamsa" {
		return nil, fmt.Errorf("hamsa tts adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportHTTP {
		return nil, fmt.Errorf("hamsa tts requires http transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("hamsa tts requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("hamsa tts media: %w", err)
	}
	sampleRate, ok := ttsSampleRates[request.Media.SampleRateHz]
	if request.Media.Encoding != "pcm_s16le" || request.Media.Channels != 1 || !ok {
		return nil, &runtimepkg.ProviderError{
			Code:    "unsupported_media",
			Message: fmt.Sprintf("Hamsa TTS emits mono pcm_s16le at 16000 or 8000 Hz, got %s/%d Hz/%d channels", request.Media.Encoding, request.Media.SampleRateHz, request.Media.Channels),
			Hint:    "Request mono pcm_s16le output at 16000 or 8000 Hz.",
		}
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if _, ok := ttsModels[model]; !ok {
		return nil, fmt.Errorf("hamsa tts does not support model %q", model)
	}
	dialect, err := ttsDialect(request.Options.Language)
	if err != nil {
		return nil, err
	}
	credential, err := requireAccountKey(request, "hamsa tts")
	if err != nil {
		return nil, err
	}
	endpoint, err := a.endpointPolicy.Parse(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("hamsa tts endpoint: %w", err)
	}
	if endpoint.Path != ttsEndpointPath {
		return nil, fmt.Errorf("hamsa tts endpoint path must be %s, got %q", ttsEndpointPath, endpoint.Path)
	}
	streamCtx, cancel := context.WithCancel(context.Background())
	return &ttsStream{
		ctx: streamCtx, cancel: cancel, events: make(chan runtimepkg.ProviderEvent, a.eventBuffer), responseProgress: make(chan struct{}, 1),
		httpClient: noRedirectClient(a.httpClient), endpoint: endpoint.String(), credential: credential,
		audioChunkBytes: a.audioChunkBytes, maxResponseBytes: a.maxResponseBytes, maxErrorBytes: a.maxErrorBytes,
		gracefulCloseIdleTimeout: a.gracefulCloseIdleTimeout, headerTimeout: a.headerTimeout,
		model: model, dialect: dialect, sampleRate: sampleRate, bytesPerSecond: int64(request.Media.SampleRateHz) * 2,
		voice: ttsVoice(request.Options.Voice, request.Plan.Route.Voice),
	}, nil
}

// ttsDialect maps a session language onto Hamsa's dialect code. An absent
// language is Modern Standard Arabic, the dialect of the default voice.
func ttsDialect(language string) (string, error) {
	tag := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(language), "_", "-"))
	if tag == "" {
		return "msa", nil
	}
	if dialect, ok := ttsDialects[tag]; ok {
		return dialect, nil
	}
	if base, _, _ := strings.Cut(tag, "-"); base == "en" {
		return "en", nil
	}
	return "", &runtimepkg.ProviderError{
		Code:    "unsupported_language",
		Message: fmt.Sprintf("Hamsa TTS speaks Arabic dialects and English, got %q", language),
		Hint:    "Use ar for Modern Standard Arabic, a regional tag such as ar-EG or ar-SA for a dialect, or en.",
	}
}

// ttsVoice prefers the caller's speaker, then the control plane's. A speaker
// is a Hamsa voice name or the UUID of a cloned voice; either is sent as is.
func ttsVoice(requested, planned string) string {
	if voice := strings.TrimSpace(requested); voice != "" {
		return voice
	}
	if voice := strings.TrimSpace(planned); voice != "" {
		return voice
	}
	return DefaultVoice
}

// noRedirectClient never follows a redirect: a 3xx would replay the key to a
// URL the endpoint policy never checked.
func noRedirectClient(client *http.Client) *http.Client {
	copied := *httpClient(client)
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
	dialect                  string
	sampleRate               string
	bytesPerSecond           int64
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
// one request, so the buffer is held to Hamsa's per-request ceiling.
func (s *ttsStream) AppendText(_ context.Context, text string) error {
	if text == "" {
		return errors.New("hamsa tts text is empty")
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return runtimepkg.ErrSessionClosed
	}
	if s.inFlight {
		return errors.New("hamsa tts previous utterance has not completed")
	}
	if utf8.RuneCountInString(s.pending.String())+utf8.RuneCountInString(text) > maxInputCharacters {
		return &runtimepkg.ProviderError{Code: "input_too_large", Message: "Hamsa TTS input exceeds 2000 characters", ProviderStatus: http.StatusRequestEntityTooLarge}
	}
	s.pending.WriteString(text)
	return nil
}

// speechRequest is the POST /v1/realtime/tts-stream body. mulaw is sent
// explicitly false because the PCM sampleRate cannot be combined with it.
type speechRequest struct {
	Text       string `json:"text"`
	Speaker    string `json:"speaker"`
	Dialect    string `json:"dialect"`
	Mulaw      bool   `json:"mulaw"`
	SampleRate string `json:"sampleRate"`
}

// hamsaError is the ErrorSchema every REST failure carries.
type hamsaError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
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
	payload, err := json.Marshal(speechRequest{Text: text, Speaker: s.voice, Dialect: s.dialect, SampleRate: s.sampleRate})
	if err != nil {
		s.abandonRequest(requestCancel, done)
		return err
	}
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, s.endpoint, bytes.NewReader(payload))
	if err != nil {
		s.abandonRequest(requestCancel, done)
		return err
	}
	request.Header.Set("Authorization", "Token "+s.credential)
	request.Header.Set("Content-Type", "application/json")
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
			return &runtimepkg.ProviderError{Code: "request_timeout", Message: fmt.Sprintf("Hamsa TTS sent no response headers within %s", s.headerTimeout), Retryable: true, Cause: err}
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Hamsa TTS request could not be sent", Retryable: true, Cause: err}
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		providerErr := s.statusError(response)
		_ = response.Body.Close()
		s.abandonRequest(requestCancel, done)
		return providerErr
	}
	if mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type")); err != nil || !strings.HasPrefix(strings.ToLower(mediaType), "audio/") {
		_ = response.Body.Close()
		s.abandonRequest(requestCancel, done)
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Hamsa TTS returned an unexpected success content type", Retryable: true, ProviderStatus: response.StatusCode}
	}
	billing := &protocol.BillingObservation{
		OperationID:       fmt.Sprintf("synthesis-%d", s.billingSequence.Add(1)),
		ProviderRequestID: strings.TrimSpace(response.Header.Get("X-Request-Id")),
		Model:             s.model, Mode: "streaming",
		Quantities: map[string]int64{"duration_seconds": 0},
	}
	s.readers.Add(1)
	go s.readResponse(requestCtx, response, requestCancel, done, billing)
	return nil
}

func (s *ttsStream) statusError(response *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(response.Body, s.maxErrorBytes))
	var detail hamsaError
	_ = json.Unmarshal(body, &detail)
	code, retryable := classifyStatus(response.StatusCode, detail.Message)
	message := fmt.Sprintf("Hamsa TTS rejected the synthesis request with status %d", response.StatusCode)
	if detail.Message != "" {
		message += ": " + detail.Message
	}
	providerErr := &runtimepkg.ProviderError{Code: code, Message: message, Retryable: retryable, ProviderStatus: response.StatusCode}
	if json.Valid(body) {
		providerErr.Extensions = extension(json.RawMessage(body))
	}
	return providerErr
}

// classifyStatus reads the message before the status: Hamsa answers an
// exhausted plan allowance with 429 and "usage limit", which is a quota no
// retry will clear, not a rate limit.
func classifyStatus(status int, message string) (string, bool) {
	if code, retryable := classifyErrorMessage(message); code != "provider_unavailable" {
		return code, retryable
	}
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return "authentication_failed", false
	case status == http.StatusPaymentRequired:
		return "provider_quota_exceeded", false
	case status == http.StatusTooManyRequests:
		return "provider_rate_limited", true
	case status >= 400 && status < 500:
		return "invalid_request", false
	default:
		return "provider_unavailable", true
	}
}

func (s *ttsStream) beginRequest() (string, context.Context, context.CancelFunc, chan struct{}, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return "", nil, nil, nil, runtimepkg.ErrSessionClosed
	}
	if s.inFlight {
		return "", nil, nil, nil, errors.New("hamsa tts previous utterance has not completed")
	}
	text := s.pending.String()
	if strings.TrimSpace(text) == "" {
		return "", nil, nil, nil, errors.New("hamsa tts has no buffered text to synthesize")
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

// billedMilliseconds is the delivered audio at Hamsa's grain: whole seconds,
// rounded up per request.
func (s *ttsStream) billedMilliseconds(pcmBytes int64) int64 {
	milliseconds := (pcmBytes*1000 + s.bytesPerSecond - 1) / s.bytesPerSecond
	return (milliseconds + billingIncrementMS - 1) / billingIncrementMS * billingIncrementMS
}

// readResponse slices the chunked PCM body into sample-aligned audio frames as
// it arrives. Hamsa's chunk boundaries do not respect samples (an 8 kHz body
// measured 185,850 bytes), so an odd trailing byte is carried to the next read.
func (s *ttsStream) readResponse(requestCtx context.Context, response *http.Response, requestCancel context.CancelFunc, done chan struct{}, billing *protocol.BillingObservation) {
	defer func() {
		requestCancel()
		_ = response.Body.Close()
		s.finishRequest()
		close(done)
		s.readers.Done()
	}()
	snapshot := func(pcmBytes int64, complete bool) *protocol.BillingObservation {
		next := billing.Clone()
		next.Quantities["duration_seconds"] = s.billedMilliseconds(pcmBytes)
		next.Complete = complete
		return &next
	}
	reader := &io.LimitedReader{R: response.Body, N: s.maxResponseBytes + 1}
	buffer := make([]byte, s.audioChunkBytes)
	var header wavStripper
	var carry []byte
	started := false
	var total, delivered int64
	// deliver emits the sample-aligned part of audio and carries an odd
	// trailing byte to the next read.
	deliver := func(audio []byte) bool {
		audio = append(carry, audio...)
		aligned := len(audio) &^ 1
		carry = append([]byte(nil), audio[aligned:]...)
		if aligned == 0 {
			return true
		}
		if !started {
			started = true
			if !s.emit(requestCtx, runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Billing: snapshot(0, false)}) {
				return false
			}
		}
		delivered += int64(aligned)
		return s.emit(requestCtx, runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Audio: audio[:aligned]})
	}
	for {
		count, err := reader.Read(buffer)
		if count > 0 {
			s.reportResponseProgress()
			total += int64(count)
			if total > s.maxResponseBytes {
				s.emit(requestCtx, runtimepkg.ProviderEvent{Billing: snapshot(delivered, false), Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Hamsa TTS response exceeded the configured limit", Retryable: true}})
				return
			}
			audio, headerErr := header.write(buffer[:count])
			if headerErr != nil {
				s.emit(requestCtx, runtimepkg.ProviderEvent{Billing: snapshot(delivered, false), Err: headerErr})
				return
			}
			if !deliver(audio) {
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
			// A failure after the status line just cuts the chunked body, so a
			// torn read is a failed synthesis, never a short utterance.
			s.emit(requestCtx, runtimepkg.ProviderEvent{Billing: snapshot(delivered, false), Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Hamsa TTS audio stream ended unexpectedly", Retryable: true, Cause: err}})
			return
		}
		// A body shorter than a RIFF preamble is bare PCM still held by the
		// stripper.
		if !deliver(header.flush()) {
			return
		}
		if !started {
			s.emit(requestCtx, runtimepkg.ProviderEvent{Billing: snapshot(0, false), Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Hamsa TTS completed without returning audio", Retryable: true}})
			return
		}
		// A lone trailing byte is half a sample: unplayable, and dropped.
		s.emit(requestCtx, runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Billing: snapshot(delivered, true)})
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
			return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Hamsa TTS response stalled during graceful close", Retryable: true, Cause: context.DeadlineExceeded}
		}
	}
}

// wavStripper passes bare PCM through and removes a RIFF/WAVE container if
// one leads the body. The API reference contradicts itself (its prose says no
// header, its sample shows `RIFF` with 0xFFFFFFFF sizes); live responses on
// 2026-10-07 were bare PCM, so the header is tolerated, never required.
type wavStripper struct {
	header  []byte
	decided bool
}

func (w *wavStripper) write(chunk []byte) ([]byte, *runtimepkg.ProviderError) {
	if w.decided {
		return chunk, nil
	}
	w.header = append(w.header, chunk...)
	if len(w.header) < 12 {
		return nil, nil
	}
	if string(w.header[0:4]) != "RIFF" || string(w.header[8:12]) != "WAVE" {
		w.decided = true
		payload := w.header
		w.header = nil
		return payload, nil
	}
	cursor := 12
	for {
		if cursor+8 > len(w.header) {
			if cursor+8 > maxWAVHeaderBytes {
				return nil, malformedWAV("Hamsa TTS returned a WAV header with no data chunk")
			}
			return nil, nil
		}
		id := string(w.header[cursor : cursor+4])
		size := int64(binary.LittleEndian.Uint32(w.header[cursor+4 : cursor+8]))
		body := cursor + 8
		if id == "data" {
			// A streamed header's data size is a placeholder running to the
			// end of the body.
			w.decided = true
			payload := append([]byte(nil), w.header[body:]...)
			w.header = nil
			return payload, nil
		}
		// RIFF chunks are word aligned: an odd size is followed by a pad byte.
		next := int64(body) + size + (size & 1)
		if next > maxWAVHeaderBytes {
			return nil, malformedWAV("Hamsa TTS returned a malformed WAV header")
		}
		cursor = int(next)
	}
}

// flush returns bytes still held while undecided. Only a body that ended
// before 12 bytes can leave any, and that body was never a RIFF container.
func (w *wavStripper) flush() []byte {
	if w.decided {
		return nil
	}
	w.decided = true
	payload := w.header
	w.header = nil
	return payload
}

func malformedWAV(message string) *runtimepkg.ProviderError {
	return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: message, Retryable: true}
}
