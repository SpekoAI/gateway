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
	ResponseIdleTimeout      time.Duration
	AllowedEndpointHosts     []string
	AllowInsecureEndpoint    bool
}

type BytesTTSAdapter struct {
	config         BytesTTSConfig
	endpointPolicy upstream.HTTPPolicy
	now            func() time.Time
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
	if config.ResponseIdleTimeout == 0 {
		config.ResponseIdleTimeout = 30 * time.Second
	}
	if config.EventBuffer < 1 || config.EventBuffer > 256 || config.AudioChunkBytes < 1 || config.AudioChunkBytes > 64<<10 || config.MaxInputBytes < 1 || config.MaxInputBytes > 1<<20 || config.MaxResponseBytes < 1 || config.MaxResponseBytes > 64<<20 || config.MaxErrorBytes < 1 || config.MaxErrorBytes > 1<<20 || config.GracefulCloseIdleTimeout < 0 || config.GracefulCloseIdleTimeout > time.Minute || config.HeaderTimeout < 0 || config.HeaderTimeout > time.Minute || config.ResponseIdleTimeout < 0 || config.ResponseIdleTimeout > time.Minute {
		return nil, errors.New("cartesia bytes tts: invalid transport bounds")
	}
	policy, err := upstream.NewHTTPPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	config.AllowedEndpointHosts = nil
	return &BytesTTSAdapter{config: config, endpointPolicy: policy, now: time.Now}, nil
}

func (a *BytesTTSAdapter) ID() string { return BytesTTSAdapterID }

func (a *BytesTTSAdapter) Open(_ context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	stream, err := a.open(request)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

func (a *BytesTTSAdapter) open(request runtimepkg.AdapterRequest) (*bytesTTSStream, error) {
	input, err := bytesTTSInputTemplate(request)
	if err != nil {
		return nil, err
	}
	credential := request.Plan.Route.Credential
	permanent := request.Plan.Execution.CredentialSource == protocol.CredentialsBYOK || request.Plan.Execution.ProviderRoute == protocol.RouteSpekoRelay
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" || !permanent && !credential.ExpiresAt.IsZero() && !a.now().Before(credential.ExpiresAt) && request.RefreshCredential == nil {
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
		responseIdleTimeout: a.config.ResponseIdleTimeout, now: a.now, permanentCredential: permanent,
		refreshCredential: request.RefreshCredential, providerRoute: request.Plan.Execution.ProviderRoute,
		model: input.Model, voice: input.Voice, language: input.Language, normalization: input.Normalization,
		sampleRateHz: input.OutputFormat.SampleRate, maximumInputBytes: a.config.MaxInputBytes, maximumCharacters: request.Options.MaxInputCharacters,
	}, nil
}

// Each operation retains its cancellation handle until its terminal event is
// published. Cancel signals it before taking stateMu, so a backpressured final
// send can keep publication and idle-state changes atomic without blocking cancel.
type bytesTTSOperation struct {
	ctx        context.Context
	cancel     context.CancelFunc
	done       chan struct{}
	canceled   atomic.Bool
	userCancel chan struct{}
	known      atomic.Bool
	observed   bool // owned by perform; true only after pre-dispatch evidence was published
	id         string
	characters int64
}

type bytesTTSStream struct {
	ctx                                                          context.Context
	cancel                                                       context.CancelFunc
	events                                                       chan runtimepkg.ProviderEvent
	responseProgress                                             chan struct{}
	httpClient                                                   *http.Client
	endpoint, credential, model, voice, language, normalization  string
	credentialExpiresAt                                          time.Time
	permanentCredential                                          bool
	refreshCredential                                            runtimepkg.CredentialRefresher
	providerRoute                                                protocol.ProviderRoute
	now                                                          func() time.Time
	audioChunkBytes, maximumInputBytes, sampleRateHz             int
	maximumCharacters                                            int64
	maxResponseBytes, maxErrorBytes                              int64
	gracefulCloseIdleTimeout, headerTimeout, responseIdleTimeout time.Duration
	stateMu                                                      sync.Mutex
	pending                                                      strings.Builder
	pendingCharacters                                            int64
	preparedOnly                                                 bool
	active                                                       atomic.Pointer[bytesTTSOperation]
	closing                                                      atomic.Bool
	readers                                                      sync.WaitGroup
	closeOnce                                                    sync.Once
	closeErr                                                     error
	errorMu                                                      sync.Mutex
	terminalErr                                                  error
	billingSequence                                              atomic.Uint64
}

func (s *bytesTTSStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }
func (s *bytesTTSStream) WriteAudio(context.Context, []byte) error {
	return runtimepkg.ErrUnsupportedOperation
}
func (s *bytesTTSStream) CommitAudio(context.Context) error {
	return runtimepkg.ErrUnsupportedOperation
}
func (s *bytesTTSStream) TerminalError() error {
	s.errorMu.Lock()
	defer s.errorMu.Unlock()
	return s.terminalErr
}
func (s *bytesTTSStream) terminal(err error) {
	s.errorMu.Lock()
	if s.terminalErr == nil {
		s.terminalErr = err
	}
	s.errorMu.Unlock()
}

func (s *bytesTTSStream) AppendText(_ context.Context, text string) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.preparedOnly {
		return runtimepkg.ErrUnsupportedOperation
	}
	if s.closing.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if s.active.Load() != nil {
		return errors.New("cartesia bytes tts previous utterance has not completed")
	}
	if len(text) > s.maximumInputBytes-s.pending.Len() {
		return bytesInputTooLarge()
	}
	if text == "" || !utf8.ValidString(text) {
		return errors.New("cartesia bytes tts text must be nonempty UTF-8")
	}
	characters := s.pendingCharacters + int64(utf8.RuneCountInString(text))
	if s.maximumCharacters > 0 && characters > s.maximumCharacters {
		return bytesInputTooLarge()
	}
	s.pending.WriteString(text)
	s.pendingCharacters = characters
	return nil
}
func bytesInputTooLarge() error {
	return &runtimepkg.ProviderError{Code: "input_too_large", Message: "Cartesia bytes TTS input exceeds the configured bound", ProviderStatus: http.StatusRequestEntityTooLarge}
}

type bytesSpeechRequest struct {
	Model         string       `json:"model_id"`
	Voice         string       `json:"voice"`
	Input         string       `json:"transcript"`
	Language      string       `json:"language"`
	Normalization string       `json:"normalization,omitempty"`
	OutputFormat  outputFormat `json:"output_format"`
}

// CommitText only schedules the owned operation. The runtime's serialized input
// dispatcher must remain free to process response.cancel while HTTP waits.
func (s *bytesTTSStream) CommitText(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.preparedOnly {
		return runtimepkg.ErrUnsupportedOperation
	}
	if s.closing.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if s.active.Load() != nil {
		return errors.New("cartesia bytes tts previous utterance has not completed")
	}
	text := s.pending.String()
	if strings.TrimSpace(text) == "" {
		return errors.New("cartesia bytes tts has no buffered text to synthesize")
	}
	payload, err := json.Marshal(bytesSpeechRequest{Model: s.model, Voice: s.voice, Input: text, Language: s.language, Normalization: s.normalization, OutputFormat: outputFormat{Container: "raw", Encoding: "pcm_s16le", SampleRate: s.sampleRateHz}})
	if err != nil {
		return errors.New("cartesia bytes tts could not prepare input")
	}
	if err := s.startLocked(ctx, payload, s.pendingCharacters); err != nil {
		return err
	}
	s.pending.Reset()
	s.pendingCharacters = 0
	return nil
}

func (s *bytesTTSStream) startLocked(ctx context.Context, payload []byte, characters int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	work, cancel := context.WithCancel(s.ctx)
	op := &bytesTTSOperation{ctx: work, cancel: cancel, done: make(chan struct{}), userCancel: make(chan struct{}), id: fmt.Sprintf("synthesis-%d", s.billingSequence.Add(1)), characters: characters}
	s.active.Store(op)
	s.readers.Add(1)
	go s.perform(op, payload)
	return nil
}

func (s *bytesTTSStream) release(op *bytesTTSOperation) {
	s.stateMu.Lock()
	s.active.CompareAndSwap(op, nil)
	s.stateMu.Unlock()
}
func (s *bytesTTSStream) progress() {
	select {
	case s.responseProgress <- struct{}{}:
	default:
	}
}
func (s *bytesTTSStream) emit(ctx context.Context, event runtimepkg.ProviderEvent) bool {
	select {
	case s.events <- event:
		s.progress()
		return true
	case <-ctx.Done():
		return false
	case <-s.ctx.Done():
		return false
	}
}
func (s *bytesTTSStream) finish(op *bytesTTSOperation, event runtimepkg.ProviderEvent) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.active.Load() != op {
		return
	}
	// The receiver may immediately call AppendText after audio.done. It waits
	// for stateMu until the send and release have both committed. Cancel can
	// still interrupt this send through the atomically visible operation.
	select {
	case s.events <- event:
		s.progress()
	case <-op.ctx.Done():
	case <-s.ctx.Done():
	}
	s.active.CompareAndSwap(op, nil)
}
func (s *bytesTTSStream) failed(op *bytesTTSOperation, err error) {
	if op.canceled.Load() || s.ctx.Err() != nil {
		return
	}
	s.closing.Store(true)
	s.terminal(err)
	// An internal timeout canceled op.ctx to interrupt I/O. Its failure still
	// reaches the event consumer; user cancellation remains silent here.
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.active.Load() != op || op.canceled.Load() {
		return
	}
	event := runtimepkg.ProviderEvent{Err: err}
	if op.observed {
		event.Billing = s.observation(op, op.known.Load(), false)
	}
	select {
	case s.events <- event:
		s.progress()
	case <-s.ctx.Done():
	case <-op.userCancel:
		// TerminalError independently survives a saturated or canceled queue.
	}
	s.active.CompareAndSwap(op, nil)
}

func (s *bytesTTSStream) credentialFor(ctx context.Context) (string, error) {
	if s.permanentCredential || s.credentialExpiresAt.IsZero() || s.now().Before(s.credentialExpiresAt) {
		return s.credential, nil
	}
	if s.refreshCredential == nil {
		return "", &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Cartesia bytes TTS credential cannot be renewed", Retryable: true}
	}
	credential, err := s.refreshCredential(ctx)
	if err != nil || ctx.Err() != nil || !acceptableCredentialKind(s.providerRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" || credential.ExpiresAt.IsZero() || !s.now().Before(credential.ExpiresAt) {
		return "", &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Cartesia bytes TTS credential renewal failed", Retryable: true}
	}
	s.credential, s.credentialExpiresAt = credential.Value, credential.ExpiresAt
	return s.credential, nil
}
func bytesFailure(message string) *runtimepkg.ProviderError {
	return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: message, Retryable: true}
}

// Initial evidence has no measured quantity: preparing an input is not proof
// that its bytes reached the provider. It nevertheless represents every POST
// that may have been dispatched, even if headers, audio or cancellation follow.
func (s *bytesTTSStream) observation(op *bytesTTSOperation, known, complete bool) *protocol.BillingObservation {
	quantities := map[string]int64{}
	if known {
		quantities["characters"] = op.characters * 1000
	}
	return &protocol.BillingObservation{OperationID: op.id, Model: s.model, Mode: "streaming", Quantities: quantities, Complete: complete}
}
func (s *bytesTTSStream) perform(op *bytesTTSOperation, payload []byte) {
	defer func() { op.cancel(); s.release(op); close(op.done); s.readers.Done() }()
	// Credential renewal is bounded and cancelable, but creates no provider
	// operation when it fails before dispatch.
	credentialCtx, cancelCredential := context.WithTimeout(op.ctx, s.headerTimeout)
	credential, err := s.credentialFor(credentialCtx)
	cancelCredential()
	if err != nil {
		s.failed(op, err)
		return
	}
	request, err := http.NewRequestWithContext(op.ctx, http.MethodPost, s.endpoint, bytes.NewReader(payload))
	if err != nil {
		s.failed(op, bytesFailure("Cartesia bytes TTS request could not be prepared"))
		return
	}
	request.Header.Set("Authorization", "Bearer "+credential)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Cartesia-Version", BytesTTSAPIVersion)
	request.Header.Set("Accept", "audio/pcm, application/octet-stream")
	request.Header.Set("Accept-Encoding", "identity")
	if !s.emit(op.ctx, runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Billing: s.observation(op, false, false)}) {
		return
	}
	op.observed = true
	var timedOut atomic.Bool
	headerTimer := time.AfterFunc(s.headerTimeout, func() { timedOut.Store(true); op.cancel() })
	response, err := s.httpClient.Do(request)
	if !headerTimer.Stop() {
		timedOut.Store(true)
		op.cancel()
	}
	if response != nil {
		defer response.Body.Close()
	}
	if err != nil || timedOut.Load() {
		failure := bytesFailure("Cartesia bytes TTS request could not be sent")
		if timedOut.Load() {
			failure.Code = "request_timeout"
			failure.Message = "Cartesia bytes TTS response headers timed out"
		}
		s.failed(op, failure)
		return
	}
	if response.StatusCode != http.StatusOK {
		errorTimer := time.AfterFunc(s.headerTimeout, op.cancel)
		body, _ := io.ReadAll(io.LimitReader(response.Body, s.maxErrorBytes))
		errorTimer.Stop()
		code := "provider_unavailable"
		if response.StatusCode == 401 || response.StatusCode == 403 {
			code = "authentication_failed"
		}
		if response.StatusCode == 429 {
			code = "rate_limited"
		}
		failure := &runtimepkg.ProviderError{Code: code, Message: "Cartesia bytes TTS rejected the synthesis request", ProviderStatus: response.StatusCode, Retryable: response.StatusCode == 429 || response.StatusCode >= 500}
		// A JSON body ({"error_code":"voice_not_found","title":...}) is kept
		// for SafeProviderCode; a truncated or plain-text one is not.
		if raw := bytes.TrimSpace(body); json.Valid(raw) {
			failure.Extensions = extension(json.RawMessage(raw))
		}
		s.failed(op, failure)
		return
	}
	mediaType, _, typeErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	encoding := strings.TrimSpace(response.Header.Get("Content-Encoding"))
	if len(response.Header.Values("Content-Type")) != 1 || len(response.Header.Values("Content-Encoding")) > 1 || typeErr != nil || mediaType != "audio/pcm" && mediaType != "application/octet-stream" || response.Uncompressed || encoding != "" && !strings.EqualFold(encoding, "identity") {
		s.failed(op, bytesFailure("Cartesia bytes TTS returned unsupported audio metadata"))
		return
	}
	s.readResponse(op, response)
}
func (s *bytesTTSStream) readResponse(op *bytesTTSOperation, response *http.Response) {
	data := marshalData(map[string]any{"sample_rate_hz": s.sampleRateHz})
	reader := &io.LimitedReader{R: response.Body, N: s.maxResponseBytes + 1}
	buffer := make([]byte, s.audioChunkBytes)
	var carry []byte
	var total int64
	started := false
	for {
		var timedOut atomic.Bool
		timer := time.AfterFunc(s.responseIdleTimeout, func() { timedOut.Store(true); op.cancel() })
		count, err := reader.Read(buffer)
		if !timer.Stop() {
			timedOut.Store(true)
			op.cancel()
		}
		if op.canceled.Load() || s.ctx.Err() != nil {
			return
		}
		if timedOut.Load() {
			s.failed(op, bytesFailure("Cartesia bytes TTS audio response stalled"))
			return
		}
		if count > 0 {
			s.progress()
			total += int64(count)
			if total > s.maxResponseBytes {
				s.failed(op, bytesFailure("Cartesia bytes TTS response exceeded the configured limit"))
				return
			}
			audio := append(carry, buffer[:count]...)
			carry = nil
			if len(audio)%2 != 0 {
				carry = []byte{audio[len(audio)-1]}
				audio = audio[:len(audio)-1]
			}
			if len(audio) > 0 {
				if !started {
					started = true
					op.known.Store(true)
					if !s.emit(op.ctx, runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: data, Billing: s.observation(op, true, false)}) {
						return
					}
				}
				if !s.emit(op.ctx, runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Data: data, Audio: audio}) {
					return
				}
			}
		}
		if err == nil {
			continue
		}
		if !errors.Is(err, io.EOF) {
			s.failed(op, bytesFailure("Cartesia bytes TTS audio response ended unexpectedly"))
			return
		}
		if !started || len(carry) > 0 {
			s.failed(op, bytesFailure("Cartesia bytes TTS returned incomplete PCM audio"))
			return
		}
		s.finish(op, runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: data, Billing: s.observation(op, true, true)})
		return
	}
}

func (op *bytesTTSOperation) stop() {
	if op.canceled.CompareAndSwap(false, true) {
		close(op.userCancel)
	}
	op.cancel()
}

func (s *bytesTTSStream) Cancel(ctx context.Context) error {
	// Signal before taking stateMu: a blocked terminal publication holds it.
	op := s.active.Load()
	if op != nil {
		op.stop()
	}
	s.stateMu.Lock()
	current := s.active.Load()
	if current != nil && current != op {
		current.stop()
		op = current
	}
	s.pending.Reset()
	s.pendingCharacters = 0
	closed := s.closing.Load()
	s.stateMu.Unlock()
	if op != nil {
		select {
		case <-op.done:
		case <-ctx.Done():
			return ctx.Err()
		case <-s.ctx.Done():
			return nil
		}
	}
	if closed {
		return runtimepkg.ErrSessionClosed
	}
	if !s.emit(ctx, runtimepkg.ProviderEvent{Type: protocol.EventResponseCanceled}) {
		return ctx.Err()
	}
	return nil
}
func (s *bytesTTSStream) Close(ctx context.Context) error { return s.shutdown(ctx, true) }
func (s *bytesTTSStream) Abort(context.Context) error     { return s.shutdown(context.Background(), false) }
func (s *bytesTTSStream) shutdown(ctx context.Context, graceful bool) error {
	s.closeOnce.Do(func() {
		s.closing.Store(true)
		stopped := make(chan struct{})
		go func() {
			// This barrier waits for a concurrent CommitText to finish Add.
			// The close watchdog already runs if terminal publication holds it.
			s.stateMu.Lock()
			s.pending.Reset()
			s.pendingCharacters = 0
			s.stateMu.Unlock()
			s.readers.Wait()
			close(stopped)
		}()
		if graceful {
			s.closeErr = s.waitForRequest(ctx, stopped)
			if s.closeErr != nil {
				s.terminal(s.closeErr)
			}
		}
		s.cancel()
		<-stopped
		if s.closeErr == nil {
			s.closeErr = s.TerminalError()
		}
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
			return bytesFailure("Cartesia bytes TTS response stalled during graceful close")
		}
	}
}
