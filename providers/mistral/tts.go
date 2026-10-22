package mistral

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// TTSAdapterID is the identifier the connector registers this adapter under.
	TTSAdapterID = "mistral.tts.v1"
	// DefaultTTSModel is the Voxtral 3 synthesis model.
	DefaultTTSModel = "voxtral-mini-tts-3-0"
	// DefaultVoice is Paul - Neutral, a preset English voice, used when
	// neither the caller nor the plan names one; a request without a voice
	// is a 400.
	DefaultVoice = "en_paul_neutral"

	speechPath         = "/v1/audio/speech"
	outputSampleRateHz = 24_000

	defaultTTSEventBuffer   = 32
	defaultMaxResponseBytes = 64 << 20
	defaultMaxErrorBytes    = 8 << 10
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

// TTSAdapter implements POST /v1/audio/speech with a server-sent event body.
type TTSAdapter struct {
	id                       string
	httpClient               *http.Client
	eventBuffer              int
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
	if config.EventBuffer < 1 || config.MaxResponseBytes < 1 || config.MaxErrorBytes < 1 || config.GracefulCloseIdleTimeout < 0 || config.HeaderTimeout < 0 {
		return nil, errors.New("mistral tts: event buffer, response bounds and timeouts must be positive")
	}
	policy, err := upstream.NewHTTPPolicy(officialHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &TTSAdapter{
		id: config.AdapterID, httpClient: config.HTTPClient, eventBuffer: config.EventBuffer,
		maxResponseBytes: config.MaxResponseBytes, maxErrorBytes: config.MaxErrorBytes,
		gracefulCloseIdleTimeout: config.GracefulCloseIdleTimeout, headerTimeout: config.HeaderTimeout, endpointPolicy: policy,
	}, nil
}

// ID returns the adapter identifier.
func (a *TTSAdapter) ID() string { return a.id }

// Open validates the plan. HTTP has no handshake, so the first request happens
// at CommitText.
func (a *TTSAdapter) Open(_ context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindTTS {
		return nil, fmt.Errorf("mistral tts supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != ProviderName {
		return nil, fmt.Errorf("mistral tts adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportHTTP {
		return nil, fmt.Errorf("mistral tts requires http transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("mistral tts requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("mistral tts media: %w", err)
	}
	if request.Media.Encoding != "pcm_s16le" || request.Media.Channels != 1 || request.Media.SampleRateHz != outputSampleRateHz {
		return nil, &runtimepkg.ProviderError{
			Code:    "unsupported_media",
			Message: fmt.Sprintf("Mistral TTS emits mono pcm_s16le at %d Hz, got %s/%d Hz/%d channels", outputSampleRateHz, request.Media.Encoding, request.Media.SampleRateHz, request.Media.Channels),
			Hint:    "Request mono pcm_s16le output at 24000 Hz.",
		}
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if _, ok := ttsModels[model]; !ok {
		return nil, fmt.Errorf("mistral tts does not support model %q", model)
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("mistral tts requires an api key credential")
	}
	endpoint, err := a.endpointPolicy.Parse(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("mistral tts endpoint: %w", err)
	}
	if endpoint.Path != speechPath {
		return nil, fmt.Errorf("mistral tts endpoint path must be %s, got %q", speechPath, endpoint.Path)
	}
	client := noRedirectClient(a.httpClient)
	streamCtx, cancel := context.WithCancel(context.Background())
	return &ttsStream{
		ctx: streamCtx, cancel: cancel, events: make(chan runtimepkg.ProviderEvent, a.eventBuffer), responseProgress: make(chan struct{}, 1),
		httpClient: client, endpoint: endpoint.String(), credential: strings.TrimSpace(credential.Value),
		maxResponseBytes: a.maxResponseBytes, maxErrorBytes: a.maxErrorBytes,
		gracefulCloseIdleTimeout: a.gracefulCloseIdleTimeout, headerTimeout: a.headerTimeout, model: model,
		voice: ttsVoice(request.Options.Voice, request.Plan.Route.Voice),
	}, nil
}

// ttsVoice prefers the caller's choice, then the control plane's, then the
// package default, because the endpoint refuses a request without one. A
// preset slug and a voice UUID are both accepted as voice_id. The voice
// fixes the language, so no language is sent.
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
	maxResponseBytes         int64
	maxErrorBytes            int64
	gracefulCloseIdleTimeout time.Duration
	headerTimeout            time.Duration
	model                    string
	voice                    string

	readers   sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
	stateMu   sync.Mutex
	closed    bool
	pending   strings.Builder
	// inFlight is the newest request's hold on the stream: AppendText and
	// CommitText refuse while it is set. A request gives it up just before
	// publishing its last event, so a caller can start the next utterance as
	// soon as it receives audio.done.
	inFlight bool
	// requests are the requests whose events may still be published, oldest
	// first. A request stays here until its last event is delivered or
	// suppressed, so Cancel can still suppress an audio.done or failure that
	// a full event buffer holds back.
	requests []*synthesis
}

// synthesis is one synthesis request.
type synthesis struct {
	ctx    context.Context
	cancel context.CancelFunc
	// done closes once the request will publish nothing more.
	done chan struct{}
	// after is the previous request's done: this request publishes nothing
	// until the previous one has, so utterances never interleave.
	after    chan struct{}
	canceled atomic.Bool
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
		return errors.New("mistral tts text is empty")
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return runtimepkg.ErrSessionClosed
	}
	if s.inFlight {
		return errors.New("mistral tts previous utterance has not completed")
	}
	s.pending.WriteString(text)
	return nil
}

// speechRequest is the synthesis body. It carries no speed: the endpoint
// answers 422 extra_forbidden to it, and the session options have none to
// forward. `language` is absent too, because the voice fixes it.
type speechRequest struct {
	Model          string `json:"model"`
	Input          string `json:"input"`
	VoiceID        string `json:"voice_id"`
	ResponseFormat string `json:"response_format"`
	Stream         bool   `json:"stream"`
}

// CommitText performs the synthesis request. It returns once the status line
// is known, so a rejection surfaces synchronously; audio then streams on
// Events until audio.done.
func (s *ttsStream) CommitText(ctx context.Context) error {
	text, request, err := s.beginRequest()
	if err != nil {
		return err
	}
	requestCancel := request.cancel
	// Cancel a stalled header request when CommitText's caller cancels. Stop
	// the hook when this call returns so its scope does not own the audio body.
	stopCallerCancel := context.AfterFunc(ctx, requestCancel)
	defer stopCallerCancel()
	payload, err := json.Marshal(speechRequest{Model: s.model, Input: text, VoiceID: s.voice, ResponseFormat: "pcm", Stream: true})
	if err != nil {
		s.abandonRequest(request)
		return err
	}
	httpRequest, err := http.NewRequestWithContext(request.ctx, http.MethodPost, s.endpoint, bytes.NewReader(payload))
	if err != nil {
		s.abandonRequest(request)
		return err
	}
	httpRequest.Header.Set("Authorization", "Bearer "+s.credential)
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "text/event-stream")
	// The runtime holds its provider lock across CommitText, so a vendor that
	// accepts the connection but never answers would wedge the session.
	var headerTimedOut atomic.Bool
	headerTimer := time.AfterFunc(s.headerTimeout, func() {
		headerTimedOut.Store(true)
		requestCancel()
	})
	response, err := s.httpClient.Do(httpRequest)
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
		s.abandonRequest(request)
		if headerTimedOut.Load() {
			return &runtimepkg.ProviderError{Code: "request_timeout", Message: fmt.Sprintf("Mistral TTS sent no response headers within %s", s.headerTimeout), Retryable: true, Cause: err}
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Mistral TTS request could not be sent", Retryable: true, Cause: err}
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		// The header timer has stopped, and the runtime still holds its
		// provider lock here, so the error body gets its own bound. A body
		// cut off by it still classifies from the status.
		bodyTimer := time.AfterFunc(s.headerTimeout, requestCancel)
		providerErr := s.statusError(response)
		bodyTimer.Stop()
		_ = response.Body.Close()
		s.abandonRequest(request)
		return providerErr
	}
	if mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type")); err != nil || !strings.EqualFold(mediaType, "text/event-stream") {
		_ = response.Body.Close()
		s.abandonRequest(request)
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Mistral TTS returned an unexpected success content type", Retryable: true, ProviderStatus: response.StatusCode}
	}
	s.readers.Add(1)
	go s.readResponse(request, response)
	return nil
}

// statusError reads a bounded error body. An unknown voice is 404
// invalid_voice, which classify maps to invalid_request rather than to a
// missing endpoint.
func (s *ttsStream) statusError(response *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(response.Body, s.maxErrorBytes))
	providerErr := providerError(fmt.Sprintf("Mistral TTS rejected the synthesis request with status %d", response.StatusCode), parseError(body), response.StatusCode, body)
	if parseError(body).kind() == "invalid_voice" {
		providerErr.Message = fmt.Sprintf("Mistral TTS does not know voice %q", s.voice)
		providerErr.Hint = "Use a preset slug such as en_paul_neutral or a voice id from GET /v1/audio/voices."
	}
	return providerErr
}

func (s *ttsStream) beginRequest() (string, *synthesis, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return "", nil, runtimepkg.ErrSessionClosed
	}
	if s.inFlight {
		return "", nil, errors.New("mistral tts previous utterance has not completed")
	}
	text := s.pending.String()
	if text == "" {
		return "", nil, errors.New("mistral tts has no buffered text to synthesize")
	}
	request := &synthesis{done: make(chan struct{})}
	request.ctx, request.cancel = context.WithCancel(s.ctx)
	if last := len(s.requests) - 1; last >= 0 {
		request.after = s.requests[last].done
	}
	s.pending.Reset()
	s.inFlight = true
	s.requests = append(s.requests, request)
	return text, request, nil
}

func (s *ttsStream) abandonRequest(request *synthesis) {
	request.cancel()
	s.releaseRequest(request)
	close(request.done)
}

// finishing gives up the stream's in-flight hold just before the request's
// last event, so a caller that reacts to audio.done by starting the next
// utterance does not find this one still in flight. The request stays
// cancellable until releaseRequest.
func (s *ttsStream) finishing(request *synthesis) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if last := len(s.requests) - 1; last >= 0 && s.requests[last] == request {
		s.inFlight = false
	}
}

// releaseRequest drops a request that will publish nothing more. It is
// idempotent and leaves newer requests alone.
func (s *ttsStream) releaseRequest(request *synthesis) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	for index, candidate := range s.requests {
		if candidate != request {
			continue
		}
		if index == len(s.requests)-1 {
			s.inFlight = false
		}
		s.requests = append(s.requests[:index:index], s.requests[index+1:]...)
		return
	}
}

// speechEvent is one SSE payload: speech.audio.delta carries base64 float32
// samples, speech.audio.done the usage and the end of the utterance. An error
// event's shape is not documented, so its detail is read either nested under
// `error`, as the realtime socket sends it, or at the top level, as the REST
// error body has it.
type speechEvent struct {
	Type      string          `json:"type"`
	AudioData string          `json:"audio_data"`
	Error     json.RawMessage `json:"error"`
}

// readResponse turns the event stream into audio frames as it arrives. Each
// delta's float32 samples become s16le; a delta that ends mid-sample leaves
// its trailing bytes for the next one, so every frame carries whole samples.
func (s *ttsStream) readResponse(request *synthesis, response *http.Response) {
	defer func() {
		request.cancel()
		_ = response.Body.Close()
		s.releaseRequest(request)
		close(request.done)
		s.readers.Done()
	}()
	// finish publishes the request's last event, after giving up the
	// in-flight hold; Cancel can still suppress it until it is delivered.
	finish := func(event runtimepkg.ProviderEvent) {
		s.finishing(request)
		s.emit(request, event)
	}
	failed := func(message string, cause error) {
		finish(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: message, Retryable: true, Cause: cause}})
	}
	data := marshalData(map[string]any{"sample_rate_hz": outputSampleRateHz})
	limited := &io.LimitedReader{R: response.Body, N: s.maxResponseBytes + 1}
	reader := bufio.NewReader(limited)
	var frame, carry []byte
	started := false
	// handle processes one event's payload and reports whether the request
	// is over, its last event published.
	handle := func(payload []byte) bool {
		if len(bytes.TrimSpace(payload)) == 0 {
			return false
		}
		var event speechEvent
		if json.Unmarshal(payload, &event) != nil {
			failed("Mistral TTS sent a malformed audio event", nil)
			return true
		}
		switch event.Type {
		case "speech.audio.delta":
			samples, err := base64.StdEncoding.DecodeString(event.AudioData)
			if err != nil {
				failed("Mistral TTS sent undecodable audio", err)
				return true
			}
			var audio []byte
			audio, carry = float32ToPCM16(append(carry, samples...))
			if len(audio) == 0 {
				return false
			}
			if !started {
				started = true
				if !s.emit(request, runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: data}) {
					return true
				}
			}
			return !s.emit(request, runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Data: data, Audio: audio})
		case "speech.audio.done":
			switch {
			case !started:
				failed("Mistral TTS completed without returning audio", nil)
			case len(carry) > 0:
				// A stream that ends on a partial float has lost a sample: the
				// audio is broken, not complete.
				failed("Mistral TTS audio ended mid-sample", nil)
			default:
				finish(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: data})
			}
			return true
		case "error":
			detail := parseError(event.Error)
			if detail == nil {
				detail = parseError(payload)
				if detail == nil {
					detail = &errorDetail{}
				}
				// The event's own type is "error", which says nothing.
				detail.Type = nil
			}
			finish(runtimepkg.ProviderEvent{Err: providerError("Mistral TTS failed mid-synthesis", detail, 0, payload)})
			return true
		}
		return false
	}
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			s.reportResponseProgress()
		}
		if limited.N <= 0 {
			failed("Mistral TTS response exceeded the configured limit", nil)
			return
		}
		// An event ends at a blank line; `data:` lines carry its payload and
		// `event:` repeats the payload's own type.
		switch text := bytes.TrimRight(line, "\r\n"); {
		case len(text) == 0 && len(line) > 0:
			if handle(frame) {
				return
			}
			frame = frame[:0]
		case bytes.HasPrefix(text, []byte("data:")):
			frame = append(frame, bytes.TrimPrefix(bytes.TrimPrefix(text, []byte("data:")), []byte(" "))...)
		}
		if err == nil {
			continue
		}
		if request.canceled.Load() || s.ctx.Err() != nil {
			return
		}
		if !errors.Is(err, io.EOF) {
			failed("Mistral TTS audio stream ended unexpectedly", err)
			return
		}
		// A last event with no trailing blank line still counts.
		if handle(frame) {
			return
		}
		// The body ended without speech.audio.done: whatever audio arrived is
		// a cut-off utterance, never a complete one.
		if !started {
			failed("Mistral TTS completed without returning audio", nil)
			return
		}
		failed("Mistral TTS audio stream ended before speech.audio.done", nil)
		return
	}
}

// float32ToPCM16 converts whole little-endian float32 samples to s16le,
// clamping to [-1, 1], and returns the bytes of a trailing partial sample.
func float32ToPCM16(raw []byte) ([]byte, []byte) {
	whole := len(raw) / 4 * 4
	pcm := make([]byte, whole/2)
	for index := 0; index < whole; index += 4 {
		sample := math.Float32frombits(binary.LittleEndian.Uint32(raw[index:]))
		if sample != sample {
			sample = 0
		}
		sample = max(-1, min(1, sample))
		binary.LittleEndian.PutUint16(pcm[index/2:], uint16(int16(math.Round(float64(sample)*32767))))
	}
	var rest []byte
	if whole < len(raw) {
		rest = append(rest, raw[whole:]...)
	}
	return pcm, rest
}

func (s *ttsStream) reportResponseProgress() {
	select {
	case s.responseProgress <- struct{}{}:
	default:
	}
}

// emit does not let Close interrupt a blocked send: a runtime still draining
// events must receive every frame and audio.done. The graceful-close idle
// timer cancels s.ctx if the consumer is abandoned; Cancel interrupts the
// request through its own context. Nothing is published before the previous
// request has published its last event, and nothing after a Cancel.
func (s *ttsStream) emit(request *synthesis, event runtimepkg.ProviderEvent) bool {
	if request.after != nil {
		select {
		case <-request.after:
		case <-request.ctx.Done():
			return false
		case <-s.ctx.Done():
			return false
		}
	}
	if request.ctx.Err() != nil || s.ctx.Err() != nil {
		return false
	}
	select {
	case s.events <- event:
		s.reportResponseProgress()
		return true
	case <-request.ctx.Done():
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
	requests := append([]*synthesis(nil), s.requests...)
	for _, request := range requests {
		request.canceled.Store(true)
	}
	s.stateMu.Unlock()
	if len(requests) == 0 {
		if closed {
			return runtimepkg.ErrSessionClosed
		}
		return nil
	}
	// A request whose audio has all been delivered may still hold its
	// audio.done or failure behind a full event buffer; cancelling it
	// suppresses that event rather than let it reach the next utterance.
	for _, request := range requests {
		request.cancel()
	}
	for _, request := range requests {
		select {
		case <-request.done:
		case <-ctx.Done():
			return ctx.Err()
		case <-s.ctx.Done():
			return nil
		}
	}
	return nil
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
			return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Mistral TTS response stalled during graceful close", Retryable: true, Cause: context.DeadlineExceeded}
		}
	}
}
