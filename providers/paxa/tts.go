package paxa

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
	"unicode/utf16"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// TTSAdapterID is the identifier the connector registers this adapter under.
	TTSAdapterID = "paxa.tts.v1"
	// DefaultTTSModel is the only TTS model Paxa serves.
	DefaultTTSModel = "paxa-tts-flash-v1"
	// DefaultVoice is the Thai voice the listening study measured; `voice` is
	// required on every request.
	DefaultVoice = "nomyen"
	// DefaultEnglishVoice is the voice the English board row measured.
	DefaultEnglishVoice = "cookie"

	speechPath = "/v1/tts"
	// maxInputUnits is Paxa's per-request ceiling, counted in UTF-16 code
	// units, the unit Paxa bills.
	maxInputUnits      = 5_000
	outputSampleRateHz = 24_000
	// maxWAVHeaderBytes bounds how much body is buffered while looking for the
	// data chunk. Paxa's header is the canonical 44 bytes.
	maxWAVHeaderBytes = 4 << 10

	defaultTTSEventBuffer   = 32
	defaultMaxResponseBytes = 64 << 20
	defaultMaxErrorBytes    = 8 << 10
	defaultAudioChunkBytes  = 8 << 10
	defaultCloseIdleTimeout = 30 * time.Second
	defaultHeaderTimeout    = 10 * time.Second
)

var ttsModels = map[string]struct{}{DefaultTTSModel: {}}

// ttsLanguages are the session languages routed to Paxa, mapped to the
// `language` value sent. Paxa also reads Mandarin, which is not routed.
var ttsLanguages = map[string]string{"": "auto", "en": "en", "th": "th"}

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

// TTSAdapter implements POST /v1/tts with a streamed WAV body.
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
		return nil, errors.New("paxa tts: event buffer, chunk size, response bounds and timeouts must be positive")
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
		return nil, fmt.Errorf("paxa tts supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != ProviderName {
		return nil, fmt.Errorf("paxa tts adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportHTTP {
		return nil, fmt.Errorf("paxa tts requires http transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("paxa tts requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("paxa tts media: %w", err)
	}
	if request.Media.Encoding != "pcm_s16le" || request.Media.Channels != 1 || request.Media.SampleRateHz != outputSampleRateHz {
		return nil, &runtimepkg.ProviderError{
			Code:    "unsupported_media",
			Message: fmt.Sprintf("Paxa TTS emits mono pcm_s16le at %d Hz, got %s/%d Hz/%d channels", outputSampleRateHz, request.Media.Encoding, request.Media.SampleRateHz, request.Media.Channels),
			Hint:    "Request mono pcm_s16le output at 24000 Hz.",
		}
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if _, ok := ttsModels[model]; !ok {
		return nil, fmt.Errorf("paxa tts does not support model %q", model)
	}
	language, err := ttsLanguage(request.Options.Language)
	if err != nil {
		return nil, err
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("paxa tts requires a bearer credential")
	}
	endpoint, err := a.endpointPolicy.Parse(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("paxa tts endpoint: %w", err)
	}
	if endpoint.Path != speechPath {
		return nil, fmt.Errorf("paxa tts endpoint path must be %s, got %q", speechPath, endpoint.Path)
	}
	client := noRedirectClient(a.httpClient)
	streamCtx, cancel := context.WithCancel(context.Background())
	return &ttsStream{
		ctx: streamCtx, cancel: cancel, events: make(chan runtimepkg.ProviderEvent, a.eventBuffer), responseProgress: make(chan struct{}, 1),
		httpClient: client, endpoint: endpoint.String(), credential: credential.Value,
		audioChunkBytes: a.audioChunkBytes, maxResponseBytes: a.maxResponseBytes, maxErrorBytes: a.maxErrorBytes,
		gracefulCloseIdleTimeout: a.gracefulCloseIdleTimeout, headerTimeout: a.headerTimeout, model: model, language: language,
		voice: ttsVoice(request.Options.Voice, request.Plan.Route.Voice, language),
	}, nil
}

// ttsLanguage narrows a BCP-47 tag to the bare subtag and refuses any language
// Paxa is not routed for. An empty language is sent as "auto".
func ttsLanguage(language string) (string, error) {
	base := strings.ToLower(strings.TrimSpace(language))
	if index := strings.IndexAny(base, "-_"); index > 0 {
		base = base[:index]
	}
	value, ok := ttsLanguages[base]
	if !ok {
		return "", &runtimepkg.ProviderError{Code: "unsupported_language", Message: fmt.Sprintf("Paxa TTS is routed for en and th only, got %q", language)}
	}
	return value, nil
}

// ttsVoice prefers the caller's choice, then the control plane's, then the
// language's default, because the endpoint refuses a request without one.
// A voice in the request is always honored, even the Thai one on an English
// session. Managed plans carry the board's per-language voice (cookie for
// en), so the catalog default only reaches a plan the board had no cell for;
// on an English session with no request voice that fill becomes cookie.
func ttsVoice(requested, planned, language string) string {
	if voice := strings.TrimSpace(requested); voice != "" {
		return voice
	}
	planned = strings.TrimSpace(planned)
	if language == "en" && (planned == "" || strings.EqualFold(planned, DefaultVoice)) {
		return DefaultEnglishVoice
	}
	if planned != "" {
		return planned
	}
	return DefaultVoice
}

// noRedirectClient never follows a redirect: a 3xx would replay the bearer
// key to a URL the endpoint policy never checked.
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
	language                 string
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
// one request, so the buffer is held to Paxa's per-request ceiling.
func (s *ttsStream) AppendText(_ context.Context, text string) error {
	if text == "" {
		return errors.New("paxa tts text is empty")
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return runtimepkg.ErrSessionClosed
	}
	if s.inFlight {
		return errors.New("paxa tts previous utterance has not completed")
	}
	if utf16Units(s.pending.String())+utf16Units(text) > maxInputUnits {
		return &runtimepkg.ProviderError{Code: "input_too_large", Message: "Paxa TTS input exceeds 5000 characters", Retryable: false, ProviderStatus: http.StatusRequestEntityTooLarge}
	}
	s.pending.WriteString(text)
	return nil
}

// utf16Units counts text the way Paxa does: one unit per BMP code point, two
// for a supplementary one.
func utf16Units(text string) int {
	units := 0
	for _, r := range text {
		units += utf16.RuneLen(r)
	}
	return units
}

// speechRequest is the POST /v1/tts body. Timestamps are never asked for:
// they bill 1.25x and the relay does not carry them.
type speechRequest struct {
	Text     string `json:"text"`
	Voice    string `json:"voice"`
	Model    string `json:"model"`
	Format   string `json:"format"`
	Stream   bool   `json:"stream"`
	Language string `json:"language"`
}

// CommitText performs the synthesis request. It returns once the status line
// is known, so a rejection surfaces synchronously; audio then streams on
// Events until audio.done.
func (s *ttsStream) CommitText(ctx context.Context) error {
	text, requestCtx, requestCancel, done, err := s.beginRequest()
	if err != nil {
		return err
	}
	payload, err := json.Marshal(speechRequest{Text: text, Voice: s.voice, Model: s.model, Format: "wav", Stream: true, Language: s.language})
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
	request.Header.Set("Accept", "audio/wav, application/problem+json")
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
			return &runtimepkg.ProviderError{Code: "request_timeout", Message: fmt.Sprintf("Paxa TTS sent no response headers within %s", s.headerTimeout), Retryable: true, Cause: err}
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Paxa TTS request could not be sent", Retryable: true, Cause: err}
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		providerErr := s.statusError(response)
		_ = response.Body.Close()
		s.abandonRequest(requestCancel, done)
		return providerErr
	}
	if mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type")); err != nil || !strings.EqualFold(mediaType, "audio/wav") {
		_ = response.Body.Close()
		s.abandonRequest(requestCancel, done)
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Paxa TTS returned an unexpected success content type", Retryable: true, ProviderStatus: response.StatusCode}
	}
	s.readers.Add(1)
	go s.readResponse(requestCtx, response, requestCancel, done)
	return nil
}

func (s *ttsStream) statusError(response *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(response.Body, s.maxErrorBytes))
	var detail problem
	_ = json.Unmarshal(body, &detail)
	return providerError(fmt.Sprintf("Paxa TTS rejected the synthesis request with status %d", response.StatusCode), detail, response.StatusCode, body)
}

func (s *ttsStream) beginRequest() (string, context.Context, context.CancelFunc, chan struct{}, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return "", nil, nil, nil, runtimepkg.ErrSessionClosed
	}
	if s.inFlight {
		return "", nil, nil, nil, errors.New("paxa tts previous utterance has not completed")
	}
	text := s.pending.String()
	if text == "" {
		return "", nil, nil, nil, errors.New("paxa tts has no buffered text to synthesize")
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

// readResponse strips the WAV header and slices the PCM that follows into
// audio frames as it arrives.
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
	var header wavStripper
	started := false
	var total int64
	for {
		count, err := reader.Read(buffer)
		if count > 0 {
			s.reportResponseProgress()
			total += int64(count)
			if total > s.maxResponseBytes {
				s.emit(requestCtx, runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Paxa TTS response exceeded the configured limit", Retryable: true}})
				return
			}
			audio, headerErr := header.write(buffer[:count])
			if headerErr != nil {
				s.emit(requestCtx, runtimepkg.ProviderEvent{Err: headerErr})
				return
			}
			if len(audio) > 0 {
				if !started {
					started = true
					if !s.emit(requestCtx, runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: data}) {
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
			// The vendor documents no trailer: a failure after the status line
			// just cuts the body, so a torn read is a failed synthesis, never a
			// short utterance.
			s.emit(requestCtx, runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Paxa TTS audio stream ended unexpectedly", Retryable: true, Cause: err}})
			return
		}
		if !started {
			s.emit(requestCtx, runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Paxa TTS completed without returning audio", Retryable: true}})
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
			return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Paxa TTS response stalled during graceful close", Retryable: true, Cause: context.DeadlineExceeded}
		}
	}
}

// wavStripper removes the RIFF/WAVE container incrementally, so frames are
// still emitted while the body arrives. It walks the chunk list rather than
// assuming the canonical 44-byte header, and checks the fmt chunk because the
// container is the one place Paxa can say it produced something other than
// the 24 kHz the plan pinned; a wrong rate would play at the wrong pitch.
type wavStripper struct {
	header    []byte
	formatOK  bool
	streaming bool
}

func (w *wavStripper) write(chunk []byte) ([]byte, *runtimepkg.ProviderError) {
	if w.streaming {
		return append([]byte(nil), chunk...), nil
	}
	w.header = append(w.header, chunk...)
	if len(w.header) < 12 {
		return nil, nil
	}
	if string(w.header[0:4]) != "RIFF" || string(w.header[8:12]) != "WAVE" {
		return nil, malformedWAV("Paxa TTS returned audio that is not a WAV container")
	}
	cursor := 12
	for {
		if cursor+8 > len(w.header) {
			if cursor+8 > maxWAVHeaderBytes {
				return nil, malformedWAV("Paxa TTS returned a WAV header with no data chunk")
			}
			return nil, nil
		}
		id := string(w.header[cursor : cursor+4])
		size := int64(binary.LittleEndian.Uint32(w.header[cursor+4 : cursor+8]))
		body := cursor + 8
		if id == "data" {
			// The streaming placeholder size runs to the end of the body.
			if !w.formatOK {
				return nil, malformedWAV("Paxa TTS returned a WAV data chunk before its fmt chunk")
			}
			w.streaming = true
			payload := append([]byte(nil), w.header[body:]...)
			w.header = nil
			return payload, nil
		}
		if id == "fmt " {
			if size < 16 {
				return nil, malformedWAV("Paxa TTS returned a truncated WAV fmt chunk")
			}
			if body+16 > len(w.header) {
				return nil, nil
			}
			format := binary.LittleEndian.Uint16(w.header[body:])
			channels := binary.LittleEndian.Uint16(w.header[body+2:])
			rate := binary.LittleEndian.Uint32(w.header[body+4:])
			bits := binary.LittleEndian.Uint16(w.header[body+14:])
			if format != 1 || channels != 1 || rate != outputSampleRateHz || bits != 16 {
				return nil, malformedWAV(fmt.Sprintf("Paxa TTS returned WAV format %d, %d channel(s), %d Hz, %d-bit; want mono 16-bit PCM at %d Hz", format, channels, rate, bits, outputSampleRateHz))
			}
			w.formatOK = true
		}
		// RIFF chunks are word aligned: an odd size is followed by a pad byte.
		next := int64(body) + size + (size & 1)
		if next > maxWAVHeaderBytes {
			return nil, malformedWAV("Paxa TTS returned a malformed WAV header")
		}
		cursor = int(next)
	}
}

func malformedWAV(message string) *runtimepkg.ProviderError {
	return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: message, Retryable: true}
}
