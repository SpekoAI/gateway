package azure

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/SpekoAI/gateway/internal/batchhttp"
	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// TTSAdapterID identifies the MAI-Voice text-to-speech implementation.
	TTSAdapterID = "azure.tts.v1"
	// TTSModelMAIVoice21 is the long-form, highest-fidelity MAI voice model.
	TTSModelMAIVoice21 = "MAI-Voice-2.1"
	// TTSModelMAIVoice21Flash is the low-latency MAI voice model Microsoft
	// positions for realtime agents and IVR.
	TTSModelMAIVoice21Flash = "MAI-Voice-2.1-Flash"
	// DefaultTTSPersona is the voice the adapter speaks with when the route
	// names none. Every MAI-Voice-2.1 persona serves both models, and Harper
	// is listed in more locales than any other.
	DefaultTTSPersona = "en-US-Harper"
	// TTSEndpoint is the SSML synthesis action on the regional Speech host of
	// the connector's eastus resource, which serves MAI voices.
	TTSEndpoint = "https://eastus.tts.speech.microsoft.com/cognitiveservices/v1"

	ttsOfficialHost = "*.tts.speech.microsoft.com"
	ttsPath         = "/cognitiveservices/v1"
	ttsExtensionID  = "speech.microsoft.com/cognitiveservices/v1"
	// ttsUserAgent is sent because the REST reference lists User-Agent as a
	// required header for this action.
	ttsUserAgent = "speko-gateway"
	// ttsMaxSSMLBytes bounds one request body. The Speech service documents
	// 64 KB of SSML per turn for its WebSocket and, for this REST action, at
	// most ten minutes of audio per request; 64 KB of text already speaks for
	// longer than that, so the bound refuses nothing servable and fails an
	// oversized utterance here with a clear code instead of mid-synthesis.
	ttsMaxSSMLBytes                = 64 << 10
	ttsDefaultMaxResponseBytes     = 128 << 20
	ttsDefaultCloseIdleTimeout     = 30 * time.Second
	ttsMAIVoiceSuffixPrefix        = "mai-voice-"
	ttsDefaultEventBuffer          = 32
	ttsResponseReadBufferBytes     = 32 << 10
	ttsResponseRequestIDHeaderName = "X-RequestId"
)

// ttsModels are the MAI voice models this adapter dials. The model is not a
// request field on this endpoint: it is the suffix of the SSML voice name
// (`en-US-Harper:MAI-Voice-2.1`), so the set is what keeps a route from
// naming a tier the catalog never priced.
var ttsModels = map[string]struct{}{
	TTSModelMAIVoice21:      {},
	TTSModelMAIVoice21Flash: {},
}

// ttsOutputFormats maps a mono s16le output rate onto the headerless raw PCM
// X-Microsoft-OutputFormat Azure documents for it. The relay catalog decides
// which of these a route advertises; the adapter only refuses a rate Azure
// has no raw format for.
var ttsOutputFormats = map[int]string{
	8_000:  "raw-8khz-16bit-mono-pcm",
	16_000: "raw-16khz-16bit-mono-pcm",
	22_050: "raw-22050hz-16bit-mono-pcm",
	24_000: "raw-24khz-16bit-mono-pcm",
	44_100: "raw-44100hz-16bit-mono-pcm",
	48_000: "raw-48khz-16bit-mono-pcm",
}

// ttsPersonaPattern is an Azure prebuilt voice name without its model
// suffix: a locale (`en-US`, `fil-PH`) followed by the persona.
var ttsPersonaPattern = regexp.MustCompile(`^[a-z]{2,3}-[A-Za-z]{2,4}-[A-Za-z][A-Za-z0-9]*$`)

// TTSConfig controls local transport limits for the MAI-Voice adapter.
type TTSConfig struct {
	AdapterID        string
	HTTPClient       *http.Client
	EventBuffer      int
	MaxResponseBytes int64
	// GracefulCloseIdleTimeout bounds inactivity after Close begins. It resets
	// whenever response bytes arrive, so progressing synthesis is not capped.
	GracefulCloseIdleTimeout time.Duration
	AllowedEndpointHosts     []string
	AllowInsecureEndpoint    bool
}

// TTSAdapter implements MAI-Voice synthesis over the Speech service's SSML
// REST action. One committed utterance is one POST whose chunked response
// body is forwarded as it arrives.
type TTSAdapter struct {
	id                       string
	httpClient               *http.Client
	eventBuffer              int
	maxResponseBytes         int64
	gracefulCloseIdleTimeout time.Duration
	endpointPolicy           upstream.HTTPPolicy
}

// NewTTS creates the MAI-Voice adapter.
func NewTTS(config TTSConfig) (*TTSAdapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = TTSAdapterID
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = ttsDefaultEventBuffer
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = ttsDefaultMaxResponseBytes
	}
	if config.GracefulCloseIdleTimeout == 0 {
		config.GracefulCloseIdleTimeout = ttsDefaultCloseIdleTimeout
	}
	if config.EventBuffer < 1 {
		return nil, errors.New("azure tts event buffer must be positive")
	}
	if config.MaxResponseBytes < 1 {
		return nil, errors.New("azure tts maximum response bytes must be positive")
	}
	if config.GracefulCloseIdleTimeout < 0 {
		return nil, errors.New("azure tts graceful close idle timeout must be positive")
	}
	policy, err := upstream.NewHTTPPolicy(ttsOfficialHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &TTSAdapter{
		id: config.AdapterID, httpClient: config.HTTPClient, eventBuffer: config.EventBuffer,
		maxResponseBytes: config.MaxResponseBytes, gracefulCloseIdleTimeout: config.GracefulCloseIdleTimeout,
		endpointPolicy: policy,
	}, nil
}

func (a *TTSAdapter) ID() string { return a.id }

// Open validates the plan and prepares a stream; nothing is dialed until the
// first CommitText.
func (a *TTSAdapter) Open(_ context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindTTS {
		return nil, fmt.Errorf("azure tts supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != ProviderName {
		return nil, fmt.Errorf("azure tts adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportHTTP {
		return nil, fmt.Errorf("azure tts requires http transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("azure tts requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("azure tts media: %w", err)
	}
	outputFormat, ok := ttsOutputFormats[request.Media.SampleRateHz]
	if request.Media.Encoding != "pcm_s16le" || request.Media.Channels != 1 || !ok {
		return nil, &runtimepkg.ProviderError{
			Code:    "unsupported_media",
			Message: fmt.Sprintf("Azure TTS serves mono pcm_s16le at 8000, 16000, 22050, 24000, 44100 or 48000 Hz, got %s/%d channels at %d Hz", request.Media.Encoding, request.Media.Channels, request.Media.SampleRateHz),
		}
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if _, ok := ttsModels[model]; !ok {
		return nil, fmt.Errorf("azure tts does not support model %q", model)
	}
	requested := strings.TrimSpace(request.Options.Voice)
	if requested == "" {
		requested = strings.TrimSpace(request.Plan.Route.Voice)
	}
	voice, err := SSMLVoiceName(requested, model)
	if err != nil {
		return nil, err
	}
	credential, err := batchhttp.Credential(request.Plan)
	if err != nil {
		return nil, fmt.Errorf("azure tts: %w", err)
	}
	endpoint, err := a.endpointPolicy.Parse(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("azure tts endpoint: %w", err)
	}
	if endpoint.Path != ttsPath {
		return nil, fmt.Errorf("azure tts endpoint path must be %s, got %q", ttsPath, endpoint.Path)
	}
	streamCtx, cancel := context.WithCancel(context.Background())
	return &ttsStream{
		ctx: streamCtx, cancel: cancel,
		events:           make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		responseProgress: make(chan struct{}, 1),
		httpClient:       noRedirectClient(a.httpClient),
		endpoint:         endpoint.String(),
		credential:       credential,
		outputFormat:     outputFormat,
		voice:            voice,
		language:         voiceLocale(voice),
		maxResponseBytes: a.maxResponseBytes, gracefulCloseIdleTimeout: a.gracefulCloseIdleTimeout,
	}, nil
}

// SSMLVoiceName renders the SSML voice name for a MAI-Voice route. The route
// model is authoritative: Azure picks the model from the voice-name suffix,
// so a caller's `en-US-Harper:MAI-Voice-2` on a MAI-Voice-2.1-Flash route
// would otherwise synthesize — and be billed — as a different model than the
// one routed. A bare persona gets the model appended, a MAI-Voice suffix is
// rewritten to the model, and any other suffix (a Dragon HD voice) is
// refused because it names a different product.
func SSMLVoiceName(voice, model string) (string, error) {
	voice = strings.TrimSpace(voice)
	if voice == "" {
		voice = DefaultTTSPersona
	}
	persona, suffix, hasSuffix := strings.Cut(voice, ":")
	if hasSuffix && !strings.HasPrefix(strings.ToLower(suffix), ttsMAIVoiceSuffixPrefix) {
		return "", &runtimepkg.ProviderError{
			Code:    "invalid_request",
			Message: fmt.Sprintf("Azure voice %q names model %q, which is not the routed %s", voice, suffix, model),
			Hint:    "Pass the voice persona alone (for example en-US-Harper); the route's model is appended.",
		}
	}
	if !ttsPersonaPattern.MatchString(persona) {
		return "", &runtimepkg.ProviderError{
			Code:    "invalid_request",
			Message: fmt.Sprintf("Azure voice %q is not a prebuilt voice name", voice),
			Hint:    "Use a MAI-Voice persona such as en-US-Harper.",
		}
	}
	return persona + ":" + model, nil
}

// voiceLocale is the SSML xml:lang, taken from the persona's locale prefix.
func voiceLocale(voice string) string {
	parts := strings.SplitN(voice, "-", 3)
	if len(parts) < 3 {
		return "en-US"
	}
	return parts[0] + "-" + parts[1]
}

// renderSSML wraps the utterance in the minimal SSML document the action
// takes. EscapeText also replaces characters XML cannot carry, so customer
// text can never break the document or inject markup.
func renderSSML(language, voice, text string) ([]byte, error) {
	var body strings.Builder
	body.WriteString(`<speak version="1.0" xmlns="http://www.w3.org/2001/10/synthesis" xml:lang="`)
	if err := xml.EscapeText(&body, []byte(language)); err != nil {
		return nil, err
	}
	body.WriteString(`"><voice name="`)
	if err := xml.EscapeText(&body, []byte(voice)); err != nil {
		return nil, err
	}
	body.WriteString(`">`)
	if err := xml.EscapeText(&body, []byte(text)); err != nil {
		return nil, err
	}
	body.WriteString(`</voice></speak>`)
	return []byte(body.String()), nil
}

// noRedirectClient never follows a redirect: a 3xx would replay the
// subscription key to a host the endpoint policy never checked.
func noRedirectClient(client *http.Client) *http.Client {
	copied := *batchhttp.Client(client)
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
	outputFormat             string
	voice                    string
	language                 string
	maxResponseBytes         int64
	gracefulCloseIdleTimeout time.Duration

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

func (s *ttsStream) AppendText(_ context.Context, text string) error {
	if text == "" {
		return errors.New("azure tts text is empty")
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return runtimepkg.ErrSessionClosed
	}
	if s.inFlight {
		return errors.New("azure tts previous utterance has not completed")
	}
	s.pending.WriteString(text)
	return nil
}

func (s *ttsStream) CommitText(ctx context.Context) error {
	text, requestCtx, requestCancel, done, err := s.beginRequest()
	if err != nil {
		return err
	}
	ssml, err := renderSSML(s.language, s.voice, text)
	if err != nil {
		s.abandonRequest(requestCancel, done)
		return err
	}
	if len(ssml) > ttsMaxSSMLBytes {
		s.abandonRequest(requestCancel, done)
		return &runtimepkg.ProviderError{Code: batchhttp.CodeInputTooLarge, Message: "Azure TTS input exceeds the 64 KB per-request SSML bound (about ten minutes of speech)", ProviderStatus: http.StatusRequestEntityTooLarge}
	}
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, s.endpoint, strings.NewReader(string(ssml)))
	if err != nil {
		s.abandonRequest(requestCancel, done)
		return err
	}
	request.Header.Set(subscriptionHeader, s.credential)
	request.Header.Set("Content-Type", "application/ssml+xml")
	request.Header.Set("X-Microsoft-OutputFormat", s.outputFormat)
	request.Header.Set("User-Agent", ttsUserAgent)
	// The request runs on the stream's context so a response body outlives
	// this call, but the caller's cancellation must still stop a request
	// whose headers have not arrived: the commit's ctx is bound until then.
	stopBinding := context.AfterFunc(ctx, requestCancel)
	response, err := s.httpClient.Do(request)
	stopBinding()
	if err != nil {
		s.abandonRequest(requestCancel, done)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return &runtimepkg.ProviderError{Code: batchhttp.CodeUnavailable, Message: "Azure TTS request could not be sent", Retryable: true, Cause: err}
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		_ = response.Body.Close()
		s.abandonRequest(requestCancel, done)
		failure := batchhttp.StatusError(ttsExtensionID, response.StatusCode, body)
		failure.Message = fmt.Sprintf("Azure TTS rejected the synthesis request with status %d", response.StatusCode)
		return failure
	}
	if err := validateTTSResponse(response); err != nil {
		_ = response.Body.Close()
		s.abandonRequest(requestCancel, done)
		return err
	}
	s.readers.Add(1)
	go s.readResponse(requestCtx, response, requestCancel, done)
	return nil
}

func (s *ttsStream) beginRequest() (string, context.Context, context.CancelFunc, chan struct{}, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return "", nil, nil, nil, runtimepkg.ErrSessionClosed
	}
	if s.inFlight {
		return "", nil, nil, nil, errors.New("azure tts previous utterance has not completed")
	}
	text := s.pending.String()
	if text == "" {
		return "", nil, nil, nil, errors.New("azure tts has no buffered text to synthesize")
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

// readResponse forwards the chunked PCM body. Chunk boundaries are not
// sample boundaries, so an odd trailing byte is carried into the next frame
// to keep every emitted frame whole 16-bit samples.
func (s *ttsStream) readResponse(requestCtx context.Context, response *http.Response, requestCancel context.CancelFunc, done chan struct{}) {
	defer func() {
		requestCancel()
		_ = response.Body.Close()
		s.finishRequest()
		close(done)
		s.readers.Done()
	}()
	requestID := strings.TrimSpace(response.Header.Get(ttsResponseRequestIDHeaderName))
	if requestID != "" {
		if !s.emit(requestCtx, runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: marshalTTSData(map[string]any{"provider_request_id": requestID})}) {
			return
		}
	}
	reader := &io.LimitedReader{R: response.Body, N: s.maxResponseBytes + 1}
	buffer := make([]byte, ttsResponseReadBufferBytes)
	var carry []byte
	started := false
	var total int64
	for {
		count, err := reader.Read(buffer)
		if count > 0 {
			s.reportResponseProgress()
			total += int64(count)
			if total > s.maxResponseBytes {
				s.emit(requestCtx, runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: batchhttp.CodeUnavailable, Message: "Azure TTS response exceeded the configured limit", Retryable: true}})
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
					if !s.emit(requestCtx, runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: marshalTTSData(map[string]any{"provider_request_id": requestID})}) {
						return
					}
				}
				if !s.emit(requestCtx, runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Data: marshalTTSData(map[string]any{"provider_request_id": requestID}), Audio: audio}) {
					return
				}
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				if s.wasCanceled() || s.ctx.Err() != nil {
					return
				}
				if !started {
					s.emit(requestCtx, runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: batchhttp.CodeUnavailable, Message: "Azure TTS completed without returning audio", Retryable: true}})
					return
				}
				// 16-bit PCM is whole samples; a dangling byte means the body
				// was cut mid-sample, and audio.done would present damaged
				// audio as complete.
				if len(carry) != 0 {
					s.emit(requestCtx, runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: batchhttp.CodeUnavailable, Message: "Azure TTS response ended mid-sample", Retryable: true}})
					return
				}
				s.emit(requestCtx, runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: marshalTTSData(map[string]any{"provider_request_id": requestID})})
				return
			}
			if !s.wasCanceled() && s.ctx.Err() == nil {
				s.emit(requestCtx, runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: batchhttp.CodeUnavailable, Message: "Azure TTS response stream failed", Retryable: true, Cause: err}})
			}
			return
		}
	}
}

func (s *ttsStream) reportResponseProgress() {
	select {
	case s.responseProgress <- struct{}{}:
	default:
	}
}

func (s *ttsStream) emit(requestCtx context.Context, event runtimepkg.ProviderEvent) bool {
	// Close deliberately does not interrupt a blocked send: a runtime that is
	// still draining events must receive every audio frame and AudioDone. The
	// graceful-close idle timer cancels s.ctx if the consumer is abandoned;
	// Cancel interrupts just this request through requestCtx.
	select {
	case s.events <- event:
		s.reportResponseProgress()
		return true
	default:
	}
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

func (s *ttsStream) Close(ctx context.Context) error { return s.shutdown(ctx, true) }
func (s *ttsStream) Abort(context.Context) error     { return s.shutdown(context.Background(), false) }

func (s *ttsStream) shutdown(ctx context.Context, graceful bool) error {
	s.closeOnce.Do(func() {
		s.stateMu.Lock()
		s.closed = true
		s.pending.Reset()
		done := s.requestDone
		s.stateMu.Unlock()
		if !graceful {
			s.cancel()
		}
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
			return &runtimepkg.ProviderError{
				Code: batchhttp.CodeUnavailable, Message: "Azure TTS response stalled during graceful close",
				Retryable: true, Cause: context.DeadlineExceeded,
			}
		}
	}
}

// validateTTSResponse refuses a 2xx that is not audio. Azure does not label
// headerless PCM as PCM: every raw-*-16bit-mono-pcm format came back
// `audio/basic` on MAI-Voice-2.1 (eastus, measured 2026-10-02) and the riff
// formats `audio/x-wav`, so any audio/* or octet-stream type passes and a JSON
// or HTML body — an error page behind a 200 — does not.
func validateTTSResponse(response *http.Response) error {
	contentType := strings.TrimSpace(response.Header.Get("Content-Type"))
	if contentType == "" {
		return nil
	}
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err == nil && (strings.HasPrefix(mediaType, "audio/") || mediaType == "application/octet-stream") {
		return nil
	}
	return &runtimepkg.ProviderError{Code: batchhttp.CodeUnavailable, Message: "Azure TTS returned an unexpected success content type", Retryable: true, ProviderStatus: response.StatusCode}
}

func marshalTTSData(value any) json.RawMessage {
	payload, _ := json.Marshal(value)
	return payload
}
