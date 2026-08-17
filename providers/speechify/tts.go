package speechify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	AdapterID               = "speechify.tts.v1"
	DefaultModel            = "simba-3.0"
	DefaultVoice            = "geffen_32"
	officialAPIHost         = "api.speechify.ai"
	streamPath              = "/v1/audio/stream"
	extensionID             = "speechify.ai/tts/v1"
	maxInputCharacters      = 20_000
	outputSampleRateHz      = 24_000
	defaultMaxResponseBytes = 128 << 20
	defaultCloseIdleTimeout = 30 * time.Second
	// Raw HTTP callers are expected to pin a date version. This is the version
	// used by Speechify's current official SDK examples and includes Simba 3.2.
	apiVersion = "2026-07-07"
)

var simbaModels = map[string]struct{}{
	"simba-3.2": {}, "simba-3.0": {}, "simba-multilingual": {}, "simba-english": {},
}

type Config struct {
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

type Adapter struct {
	id                       string
	httpClient               *http.Client
	eventBuffer              int
	maxResponseBytes         int64
	gracefulCloseIdleTimeout time.Duration
	endpointPolicy           endpointPolicy
}

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
	if config.GracefulCloseIdleTimeout == 0 {
		config.GracefulCloseIdleTimeout = defaultCloseIdleTimeout
	}
	if config.EventBuffer < 1 {
		return nil, errors.New("speechify event buffer must be positive")
	}
	if config.MaxResponseBytes < 1 {
		return nil, errors.New("speechify maximum response bytes must be positive")
	}
	if config.GracefulCloseIdleTimeout < 0 {
		return nil, errors.New("speechify graceful close idle timeout must be positive")
	}
	policy, err := newEndpointPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &Adapter{id: config.AdapterID, httpClient: config.HTTPClient, eventBuffer: config.EventBuffer, maxResponseBytes: config.MaxResponseBytes, gracefulCloseIdleTimeout: config.GracefulCloseIdleTimeout, endpointPolicy: policy}, nil
}

func (a *Adapter) ID() string { return a.id }

func (a *Adapter) Open(_ context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindTTS {
		return nil, fmt.Errorf("speechify supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "speechify" {
		return nil, fmt.Errorf("speechify adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportHTTP {
		return nil, fmt.Errorf("speechify tts requires http transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("speechify tts requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("speechify tts media: %w", err)
	}
	if request.Media.Encoding != "pcm_s16le" || request.Media.Channels != 1 || request.Media.SampleRateHz != outputSampleRateHz {
		return nil, fmt.Errorf("speechify tts requires mono pcm_s16le output at %d Hz", outputSampleRateHz)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if _, ok := simbaModels[model]; !ok {
		return nil, fmt.Errorf("speechify tts does not support model %q", model)
	}
	voice := strings.TrimSpace(request.Options.Voice)
	if voice == "" {
		voice = strings.TrimSpace(request.Plan.Route.Voice)
	}
	if voice == "" {
		voice = DefaultVoice
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("speechify tts requires a bearer credential")
	}
	endpoint, err := synthesisEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}
	client := a.httpClient
	if client == nil {
		client = http.DefaultClient
	}
	streamCtx, cancel := context.WithCancel(context.Background())
	return &stream{
		ctx: streamCtx, cancel: cancel, events: make(chan runtimepkg.ProviderEvent, a.eventBuffer), closing: make(chan struct{}), responseProgress: make(chan struct{}, 1),
		httpClient: client, endpoint: endpoint, credential: credential.Value,
		maxResponseBytes: a.maxResponseBytes, gracefulCloseIdleTimeout: a.gracefulCloseIdleTimeout, model: model, voice: voice,
		language: strings.TrimSpace(request.Options.Language),
	}, nil
}

func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

type endpointPolicy struct {
	hosts         map[string]struct{}
	allowInsecure bool
}

func newEndpointPolicy(officialHost string, additionalHosts []string, allowInsecure bool) (endpointPolicy, error) {
	hosts := make(map[string]struct{}, 1+len(additionalHosts))
	for _, host := range append([]string{officialHost}, additionalHosts...) {
		host = strings.ToLower(strings.TrimSpace(host))
		if host == "" || strings.ContainsAny(host, "/:@?#") {
			return endpointPolicy{}, errors.New("speechify: allowed endpoint host is invalid")
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

func synthesisEndpoint(policy endpointPolicy, raw string) (string, error) {
	endpoint, err := policy.parse(raw)
	if err != nil {
		return "", fmt.Errorf("speechify endpoint: %w", err)
	}
	if endpoint.Path != streamPath {
		return "", fmt.Errorf("speechify endpoint path must be %s, got %q", streamPath, endpoint.Path)
	}
	return endpoint.String(), nil
}

type stream struct {
	ctx              context.Context
	cancel           context.CancelFunc
	events           chan runtimepkg.ProviderEvent
	closing          chan struct{}
	responseProgress chan struct{}

	httpClient               *http.Client
	endpoint                 string
	credential               string
	maxResponseBytes         int64
	gracefulCloseIdleTimeout time.Duration
	model                    string
	voice                    string
	language                 string

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

func (s *stream) Events() <-chan runtimepkg.ProviderEvent  { return s.events }
func (s *stream) WriteAudio(context.Context, []byte) error { return runtimepkg.ErrUnsupportedOperation }
func (s *stream) CommitAudio(context.Context) error        { return runtimepkg.ErrUnsupportedOperation }

func (s *stream) AppendText(_ context.Context, text string) error {
	if text == "" {
		return errors.New("speechify tts text is empty")
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return runtimepkg.ErrSessionClosed
	}
	if s.inFlight {
		return errors.New("speechify tts previous utterance has not completed")
	}
	if utf8.RuneCountInString(s.pending.String())+utf8.RuneCountInString(text) > maxInputCharacters {
		return &runtimepkg.ProviderError{Code: "input_too_large", Message: "Speechify TTS input exceeds 20000 characters", Retryable: false, ProviderStatus: http.StatusRequestEntityTooLarge}
	}
	s.pending.WriteString(text)
	return nil
}

func (s *stream) CommitText(ctx context.Context) error {
	text, requestCtx, requestCancel, done, err := s.beginRequest()
	if err != nil {
		return err
	}
	body := map[string]any{"input": text, "voice_id": s.voice, "model": s.model}
	if s.language != "" && s.language != "auto" {
		body["language"] = s.language
	}
	payload, err := json.Marshal(body)
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
	request.Header.Set("Accept", "audio/pcm")
	request.Header.Set("Speechify-Version", apiVersion)
	response, err := s.httpClient.Do(request)
	if err != nil {
		s.abandonRequest(requestCancel, done)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Speechify TTS request could not be sent", Retryable: true, Cause: err}
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		providerErr := statusError(response)
		_ = response.Body.Close()
		s.abandonRequest(requestCancel, done)
		return providerErr
	}
	if err := validateAudioResponse(response); err != nil {
		_ = response.Body.Close()
		s.abandonRequest(requestCancel, done)
		return err
	}
	s.readers.Add(1)
	go s.readResponse(response, requestCancel, done)
	return nil
}

func (s *stream) beginRequest() (string, context.Context, context.CancelFunc, chan struct{}, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return "", nil, nil, nil, runtimepkg.ErrSessionClosed
	}
	if s.inFlight {
		return "", nil, nil, nil, errors.New("speechify tts previous utterance has not completed")
	}
	text := s.pending.String()
	if text == "" {
		return "", nil, nil, nil, errors.New("speechify tts has no buffered text to synthesize")
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

func (s *stream) abandonRequest(cancel context.CancelFunc, done chan struct{}) {
	cancel()
	s.finishRequest()
	close(done)
}

func (s *stream) finishRequest() {
	s.stateMu.Lock()
	s.inFlight = false
	s.requestCancel = nil
	s.requestDone = nil
	s.stateMu.Unlock()
}

func (s *stream) wasCanceled() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.canceled
}

func (s *stream) readResponse(response *http.Response, requestCancel context.CancelFunc, done chan struct{}) {
	defer func() {
		requestCancel()
		_ = response.Body.Close()
		s.finishRequest()
		close(done)
		s.readers.Done()
	}()
	requestID := strings.TrimSpace(response.Header.Get("Speechify-Request-Id"))
	if requestID == "" {
		requestID = strings.TrimSpace(response.Header.Get("X-Request-ID"))
	}
	if requestID != "" {
		if !s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: marshalData(map[string]any{"provider_request_id": requestID})}) {
			return
		}
	}
	reader := &io.LimitedReader{R: response.Body, N: s.maxResponseBytes + 1}
	buffer := make([]byte, 32<<10)
	started := false
	var total int64
	for {
		count, err := reader.Read(buffer)
		if count > 0 {
			s.reportResponseProgress()
			total += int64(count)
			if total > s.maxResponseBytes {
				s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Speechify TTS response exceeded the configured limit", Retryable: true}})
				return
			}
			if !started {
				started = true
				if !s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: marshalData(map[string]any{"provider_request_id": requestID})}) {
					return
				}
			}
			audio := append([]byte(nil), buffer[:count]...)
			if !s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Data: marshalData(map[string]any{"provider_request_id": requestID}), Audio: audio}) {
				return
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				if s.wasCanceled() || s.ctx.Err() != nil {
					return
				}
				if !started {
					s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Speechify TTS completed without returning audio", Retryable: true}})
					return
				}
				s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: marshalData(map[string]any{"provider_request_id": requestID})})
				return
			}
			if !s.wasCanceled() && s.ctx.Err() == nil {
				s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Speechify TTS response stream failed", Retryable: true, Cause: err}})
			}
			return
		}
	}
}

func (s *stream) reportResponseProgress() {
	select {
	case s.responseProgress <- struct{}{}:
	default:
	}
}

func (s *stream) emit(event runtimepkg.ProviderEvent) bool {
	// Preserve graceful delivery after Close while the runtime is still
	// consuming events. If its event buffer is already full, however, closing
	// releases the blocked reader so shutdown cannot deadlock on an abandoned
	// consumer.
	select {
	case s.events <- event:
		return true
	default:
	}
	select {
	case s.events <- event:
		return true
	case <-s.ctx.Done():
		return false
	case <-s.closing:
		return false
	}
}

func (s *stream) Cancel(ctx context.Context) error {
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

func (s *stream) Close(ctx context.Context) error { return s.shutdown(ctx, true) }
func (s *stream) Abort(context.Context) error     { return s.shutdown(context.Background(), false) }

func (s *stream) shutdown(ctx context.Context, graceful bool) error {
	s.closeOnce.Do(func() {
		s.stateMu.Lock()
		s.closed = true
		s.pending.Reset()
		done := s.requestDone
		s.stateMu.Unlock()
		close(s.closing)
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

func (s *stream) waitForRequest(ctx context.Context, done <-chan struct{}) error {
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
				Code: "provider_unavailable", Message: "Speechify TTS response stalled during graceful close",
				Retryable: true, Cause: context.DeadlineExceeded,
			}
		}
	}
}

func statusError(response *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	code := "provider_rejected_request"
	retryable := false
	switch {
	case response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden:
		code = "provider_authentication_failed"
	case response.StatusCode == http.StatusTooManyRequests:
		code, retryable = "provider_rate_limited", true
	case response.StatusCode >= 500:
		code, retryable = "provider_unavailable", true
	}
	providerErr := &runtimepkg.ProviderError{
		Code: code, Message: fmt.Sprintf("Speechify TTS rejected the synthesis request with status %d", response.StatusCode),
		Retryable: retryable, ProviderStatus: response.StatusCode,
	}
	if json.Valid(body) {
		providerErr.Extensions = map[string]json.RawMessage{extensionID: append(json.RawMessage(nil), body...)}
	}
	return providerErr
}

func validateAudioResponse(response *http.Response) error {
	mediaType, parameters, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || (mediaType != "audio/L16" && !strings.EqualFold(mediaType, "audio/l16") && !strings.EqualFold(mediaType, "audio/pcm")) {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Speechify TTS returned an unexpected success content type", Retryable: true, ProviderStatus: response.StatusCode}
	}
	if rate := parameters["rate"]; rate != "" && rate != "24000" {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Speechify TTS returned an unexpected PCM sample rate", Retryable: true, ProviderStatus: response.StatusCode}
	}
	if channels := parameters["channels"]; channels != "" && channels != "1" {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Speechify TTS returned an unexpected PCM channel count", Retryable: true, ProviderStatus: response.StatusCode}
	}
	return nil
}

func marshalData(value any) json.RawMessage {
	payload, _ := json.Marshal(value)
	return payload
}
