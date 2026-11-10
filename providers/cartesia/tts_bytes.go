package cartesia

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
	// BytesTTSAdapterID is the whole-input HTTP adapter, separate from the
	// existing WebSocket adapter. Registration is a consumer decision.
	BytesTTSAdapterID = "cartesia.tts.bytes.v1"
	BytesTTSEndpoint  = "https://api.cartesia.ai/tts/bytes"
	// The version fixes the string voice field and Authorization bearer form.
	BytesTTSAPIVersion = "2026-08-14"
	bytesTTSPath       = "/tts/bytes"
)

// BytesTTSConfig controls local resource bounds; all generation identity comes
// from the verified plan and request options. No provider fallback is performed.
type BytesTTSConfig struct {
	HTTPClient               *http.Client
	EventBuffer              int
	AudioChunkBytes          int
	MaxInputBytes            int
	MaxResponseBytes         int64
	MaxErrorBytes            int64
	GracefulCloseIdleTimeout time.Duration
	HeaderTimeout            time.Duration
	AllowedEndpointHosts     []string
	AllowInsecureEndpoint    bool
}

type BytesTTSAdapter struct {
	config         BytesTTSConfig
	endpointPolicy upstream.HTTPPolicy
}

// NewBytesTTS builds the bounded raw PCM adapter for POST /tts/bytes.
// API source: https://docs.cartesia.ai/api-reference/tts/bytes .
func NewBytesTTS(config BytesTTSConfig) (*BytesTTSAdapter, error) {
	if config.EventBuffer == 0 {
		config.EventBuffer = 32
	}
	if config.AudioChunkBytes == 0 {
		config.AudioChunkBytes = 8 << 10
	}
	if config.MaxInputBytes == 0 {
		config.MaxInputBytes = 64 << 10
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = 64 << 20
	}
	if config.MaxErrorBytes == 0 {
		config.MaxErrorBytes = 8 << 10
	}
	if config.GracefulCloseIdleTimeout == 0 {
		config.GracefulCloseIdleTimeout = 30 * time.Second
	}
	if config.HeaderTimeout == 0 {
		config.HeaderTimeout = 10 * time.Second
	}
	if config.EventBuffer < 1 || config.EventBuffer > 256 || config.AudioChunkBytes < 1 || config.AudioChunkBytes > 64<<10 || config.MaxInputBytes < 1 || config.MaxInputBytes > 1<<20 || config.MaxResponseBytes < 1 || config.MaxResponseBytes > 64<<20 || config.MaxErrorBytes < 1 || config.MaxErrorBytes > 1<<20 || config.GracefulCloseIdleTimeout < 0 || config.GracefulCloseIdleTimeout > time.Minute || config.HeaderTimeout < 0 || config.HeaderTimeout > time.Minute {
		return nil, errors.New("cartesia bytes tts: invalid transport bounds")
	}
	policy, err := upstream.NewHTTPPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	config.AllowedEndpointHosts = nil
	return &BytesTTSAdapter{config: config, endpointPolicy: policy}, nil
}

func (a *BytesTTSAdapter) ID() string { return BytesTTSAdapterID }

func (a *BytesTTSAdapter) Open(_ context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindTTS || request.Plan.Route.Provider != "cartesia" || request.Plan.Route.Transport != protocol.TransportHTTP {
		return nil, errors.New("cartesia bytes tts: incompatible route")
	}
	model := request.Plan.Route.Model
	if model != "sonic-3" && model != "sonic-3.5" && model != "sonic-3.6" {
		return nil, errors.New("cartesia bytes tts: unsupported concrete model")
	}
	if request.Media == nil || request.Media.Validate() != nil || request.Media.Channels != 1 || request.Media.Encoding != "pcm_s16le" {
		return nil, &runtimepkg.ProviderError{Code: "unsupported_media", Message: "Cartesia bytes TTS requires mono pcm_s16le"}
	}
	rate := request.Media.SampleRateHz
	if rate != 8000 && rate != 16000 && rate != 22050 && rate != 24000 && rate != 44100 && rate != 48000 {
		return nil, &runtimepkg.ProviderError{Code: "unsupported_media", Message: "Cartesia bytes TTS sample rate is unsupported"}
	}
	if err := validateGenerationOptions(model, request.Options, *request.Media); err != nil {
		return nil, err
	}
	normalization, err := ttsNormalization(model, request.Options.TTS.Provider("cartesia"))
	if err != nil {
		return nil, err
	}
	if request.Options.MaxInputCharacters < 0 || !utf8.ValidString(request.Options.Voice) || !utf8.ValidString(request.Options.Language) || len(request.Options.Voice) > 512 || len(request.Options.Language) > 1024 {
		return nil, errors.New("cartesia bytes tts: invalid request options")
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" || !credential.ExpiresAt.IsZero() && !time.Now().Before(credential.ExpiresAt) {
		return nil, errors.New("cartesia bytes tts: missing or expired credential")
	}
	endpoint, err := a.endpointPolicy.Parse(request.Plan.Route.Endpoint)
	if err != nil || endpoint.Path != bytesTTSPath || endpoint.RawPath != "" {
		return nil, errors.New("cartesia bytes tts: invalid endpoint")
	}
	client := a.config.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	copied := *client
	copied.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	streamCtx, cancel := context.WithCancel(context.Background())
	return &bytesTTSStream{
		ctx: streamCtx, cancel: cancel, events: make(chan runtimepkg.ProviderEvent, a.config.EventBuffer), responseProgress: make(chan struct{}, 1),
		httpClient: &copied, endpoint: endpoint.String(), credential: credential.Value, credentialExpiresAt: credential.ExpiresAt,
		audioChunkBytes: a.config.AudioChunkBytes, maxResponseBytes: a.config.MaxResponseBytes, maxErrorBytes: a.config.MaxErrorBytes,
		gracefulCloseIdleTimeout: a.config.GracefulCloseIdleTimeout, headerTimeout: a.config.HeaderTimeout,
		model: model, voice: request.Options.Voice, language: request.Options.Language, normalization: normalization,
		sampleRateHz: rate, maximumInputBytes: a.config.MaxInputBytes, maximumCharacters: request.Options.MaxInputCharacters,
	}, nil
}

type bytesTTSStream struct {
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
	language                 string
	normalization            string
	sampleRateHz             int
	maximumInputBytes        int
	maximumCharacters        int64
	credentialExpiresAt      time.Time

	readers           sync.WaitGroup
	closeOnce         sync.Once
	closeErr          error
	stateMu           sync.Mutex
	closed            bool
	pending           strings.Builder
	pendingCharacters int64
	inFlight          bool
	requestCancel     context.CancelFunc
	requestDone       chan struct{}
	canceled          bool
	billingSequence   atomic.Uint64
}

func (s *bytesTTSStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }
func (s *bytesTTSStream) WriteAudio(context.Context, []byte) error {
	return runtimepkg.ErrUnsupportedOperation
}
func (s *bytesTTSStream) CommitAudio(context.Context) error {
	return runtimepkg.ErrUnsupportedOperation
}

// AppendText preserves the exact bounded UTF-8 input. One commit is one POST;
// fragments are never independently synthesized or silently truncated.
func (s *bytesTTSStream) AppendText(_ context.Context, text string) error {
	if text == "" {
		return errors.New("cartesia bytes tts text must be nonempty UTF-8")
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return runtimepkg.ErrSessionClosed
	}
	if s.inFlight {
		return errors.New("cartesia bytes tts previous utterance has not completed")
	}
	if len(text) > s.maximumInputBytes-s.pending.Len() {
		return &runtimepkg.ProviderError{Code: "input_too_large", Message: "Cartesia bytes TTS input exceeds the configured bound", ProviderStatus: http.StatusRequestEntityTooLarge}
	}
	if !utf8.ValidString(text) {
		return errors.New("cartesia bytes tts text must be nonempty UTF-8")
	}
	characters := s.pendingCharacters + int64(utf8.RuneCountInString(text))
	if s.maximumCharacters > 0 && characters > s.maximumCharacters {
		return &runtimepkg.ProviderError{
			Code: "input_too_large", Message: "Cartesia bytes TTS input exceeds the configured bound",
			Retryable: false, ProviderStatus: http.StatusRequestEntityTooLarge,
		}
	}
	s.pending.WriteString(text)
	s.pendingCharacters = characters
	return nil
}

// Only the typed provider request reaches Cartesia. Native work credentials,
// cell-control tokens and private input-binding markers are not generation fields.
type bytesSpeechRequest struct {
	Model         string       `json:"model_id"`
	Voice         string       `json:"voice"`
	Input         string       `json:"transcript"`
	Language      string       `json:"language"`
	Normalization string       `json:"normalization,omitempty"`
	OutputFormat  outputFormat `json:"output_format"`
}

// CommitText performs the synthesis request. It returns once the status line
// is known, so a rejection surfaces synchronously; audio then streams on
// Events until audio.done.
func (s *bytesTTSStream) CommitText(ctx context.Context) error {
	text, requestCtx, requestCancel, done, err := s.beginRequest()
	if err != nil {
		return err
	}
	// Cancel a stalled header request when CommitText's caller cancels. Stop
	// the hook when this call returns so its scope does not own the audio body.
	stopCallerCancel := context.AfterFunc(ctx, requestCancel)
	defer stopCallerCancel()
	operationID := fmt.Sprintf("synthesis-%d", s.billingSequence.Add(1))
	payload, err := json.Marshal(bytesSpeechRequest{Model: s.model, Voice: s.voice, Input: text, Language: s.language, Normalization: s.normalization, OutputFormat: outputFormat{Container: "raw", Encoding: "pcm_s16le", SampleRate: s.sampleRateHz}})
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
	request.Header.Set("Cartesia-Version", BytesTTSAPIVersion)
	request.Header.Set("Accept", "audio/pcm, application/octet-stream")
	request.Header.Set("Accept-Encoding", "identity")
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
			return &runtimepkg.ProviderError{Code: "request_timeout", Message: fmt.Sprintf("Cartesia bytes TTS sent no response headers within %s", s.headerTimeout), Retryable: true, Cause: err}
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Cartesia bytes TTS request could not be sent", Retryable: true, Cause: err}
	}
	if response.StatusCode != http.StatusOK {
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
	mediaType, _, typeErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	encoding := strings.TrimSpace(response.Header.Get("Content-Encoding"))
	if len(response.Header.Values("Content-Type")) != 1 || len(response.Header.Values("Content-Encoding")) > 1 || typeErr != nil || mediaType != "audio/pcm" && mediaType != "application/octet-stream" || response.Uncompressed || encoding != "" && !strings.EqualFold(encoding, "identity") {
		_ = response.Body.Close()
		s.abandonRequest(requestCancel, done)
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Cartesia bytes TTS returned an unexpected success content type", Retryable: true, ProviderStatus: response.StatusCode}
	}
	go s.readResponse(requestCtx, response, requestCancel, done, operationID, int64(utf8.RuneCountInString(text)))
	return nil
}

func (s *bytesTTSStream) statusError(response *http.Response) error {
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, s.maxErrorBytes))
	code := "provider_unavailable"
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		code = "authentication_failed"
	}
	if response.StatusCode == http.StatusTooManyRequests {
		code = "rate_limited"
	}
	return &runtimepkg.ProviderError{Code: code, Message: "Cartesia bytes TTS rejected the synthesis request", ProviderStatus: response.StatusCode, Retryable: response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500}
}

func (s *bytesTTSStream) beginRequest() (string, context.Context, context.CancelFunc, chan struct{}, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return "", nil, nil, nil, runtimepkg.ErrSessionClosed
	}
	if !s.credentialExpiresAt.IsZero() && !time.Now().Before(s.credentialExpiresAt) {
		return "", nil, nil, nil, errors.New("cartesia bytes tts credential expired")
	}
	if s.inFlight {
		return "", nil, nil, nil, errors.New("cartesia bytes tts previous utterance has not completed")
	}
	text := s.pending.String()
	if strings.TrimSpace(text) == "" {
		return "", nil, nil, nil, errors.New("cartesia bytes tts has no buffered text to synthesize")
	}
	requestCtx, requestCancel := context.WithCancel(s.ctx)
	done := make(chan struct{})
	s.pending.Reset()
	s.pendingCharacters = 0
	s.inFlight = true
	s.requestCancel = requestCancel
	s.requestDone = done
	s.canceled = false
	// Count the header request as well as its reader. Close may race CommitText;
	// the state mutex prevents a new Add after shutdown starts waiting.
	s.readers.Add(1)
	return text, requestCtx, requestCancel, done, nil
}

func (s *bytesTTSStream) abandonRequest(cancel context.CancelFunc, done chan struct{}) {
	cancel()
	s.releaseRequest(done)
	close(done)
	s.readers.Done()
}

// releaseRequest frees the in-flight slot of the request that owns done. It
// is idempotent and leaves a newer request alone, so the reader can release
// its slot before publishing audio.done and still run its deferred cleanup
// after the caller has started the next utterance.
func (s *bytesTTSStream) releaseRequest(done chan struct{}) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.requestDone != done {
		return
	}
	s.inFlight = false
	s.requestCancel = nil
	s.requestDone = nil
}

func (s *bytesTTSStream) wasCanceled() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.canceled
}

// This is the exact original input character count, not inferred provider
// credits: Cartesia's preprocessing can change its credit total. Consumers must
// use an explicitly reviewed character-priced profile or retain unknown cost.
// Neither audio bytes nor an SDK-local operation ID is a provider request ID.
func (s *bytesTTSStream) billing(operationID string, characters int64, complete bool) *protocol.BillingObservation {
	return &protocol.BillingObservation{
		OperationID: operationID, Model: s.model, Mode: "streaming", Complete: complete,
		Quantities: map[string]int64{"characters": characters * 1000},
	}
}

// readResponse slices the streamed PCM into audio frames as it arrives. Frames
// carry whole samples: an odd byte left by a chunk boundary waits for the next
// read.
func (s *bytesTTSStream) readResponse(requestCtx context.Context, response *http.Response, requestCancel context.CancelFunc, done chan struct{}, operationID string, inputCharacters int64) {
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
	data := marshalData(map[string]any{"sample_rate_hz": s.sampleRateHz})
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
				finish(runtimepkg.ProviderEvent{Billing: s.billing(operationID, inputCharacters, false), Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Cartesia bytes TTS response exceeded the configured limit", Retryable: true}})
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
					if !s.emit(requestCtx, runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: data, Billing: s.billing(operationID, inputCharacters, false)}) {
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
			finish(runtimepkg.ProviderEvent{Billing: s.billing(operationID, inputCharacters, false), Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Cartesia bytes TTS audio stream ended unexpectedly", Retryable: true}})
			return
		}
		if !started {
			finish(runtimepkg.ProviderEvent{Billing: s.billing(operationID, inputCharacters, false), Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Cartesia bytes TTS completed without returning audio", Retryable: true}})
			return
		}
		// A body that ends on an odd byte has lost half a sample: the audio
		// is broken, not complete, whatever came before it.
		if len(carry) > 0 {
			finish(runtimepkg.ProviderEvent{Billing: s.billing(operationID, inputCharacters, false), Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Cartesia bytes TTS audio ended mid-sample", Retryable: true}})
			return
		}
		finish(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: data, Billing: s.billing(operationID, inputCharacters, true)})
		return
	}
}

func (s *bytesTTSStream) reportResponseProgress() {
	select {
	case s.responseProgress <- struct{}{}:
	default:
	}
}

// emit does not let Close interrupt a blocked send: a runtime still draining
// events must receive every frame and audio.done. The graceful-close idle
// timer cancels s.ctx if the consumer is abandoned; Cancel interrupts just
// this request through requestCtx.
func (s *bytesTTSStream) emit(requestCtx context.Context, event runtimepkg.ProviderEvent) bool {
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
func (s *bytesTTSStream) Cancel(ctx context.Context) error {
	s.stateMu.Lock()
	s.pending.Reset()
	s.pendingCharacters = 0
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
func (s *bytesTTSStream) Close(ctx context.Context) error { return s.shutdown(ctx, true) }

// Abort tears the stream down immediately after a terminal runtime failure.
func (s *bytesTTSStream) Abort(context.Context) error { return s.shutdown(context.Background(), false) }

func (s *bytesTTSStream) shutdown(ctx context.Context, graceful bool) error {
	s.closeOnce.Do(func() {
		s.stateMu.Lock()
		s.closed = true
		s.pending.Reset()
		s.pendingCharacters = 0
		s.stateMu.Unlock()
		// beginRequest counts header I/O under stateMu before releasing it.
		// Once closed is set, no new request can join this wait group.
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

func (s *bytesTTSStream) waitForRequest(ctx context.Context, done <-chan struct{}) error {
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
			return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Cartesia bytes TTS response stalled during graceful close", Retryable: true, Cause: context.DeadlineExceeded}
		}
	}
}
