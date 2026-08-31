package gemini

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// TTSAdapterID identifies the AI Studio speech-generation implementation.
	// It is distinct from google.tts.v1: same modality, different API,
	// different credential — the same split the STT adapters draw.
	TTSAdapterID = "gemini.tts.v1"

	// TTSEndpoint is the models collection both synthesis arms hang off. The
	// model id and the arm (":streamGenerateContent" or ":generateContent")
	// ride the path per request, so the catalog pins only the collection.
	TTSEndpoint = "https://generativelanguage.googleapis.com/v1beta/models"

	// TTSModel is the one model this adapter serves.
	TTSModel = "gemini-3.1-flash-tts-preview"

	// TTSDefaultVoice matches the platform adapter's default so a bare relay
	// request and a bare provider-direct request speak with the same voice.
	TTSDefaultVoice = "Aoede"

	// ttsOutputSampleRateHz is the only rate the service generates: raw 16-bit
	// mono PCM at 24 kHz, base64-inlined in the response JSON.
	ttsOutputSampleRateHz = 24_000

	// ttsStreamMaxChars selects the arm. The SSE ":streamGenerateContent"
	// endpoint intermittently truncates audio and finishes with a non-STOP
	// finishReason on HTTP 200 once a single generation exceeds ~60s of
	// speech, while ":generateContent" returns the full clip. ~14 chars/s of
	// speech puts 60s near 840 characters; 700 keeps a margin. Voice-agent
	// turns are almost always under it, so they stream (lower time-to-first-
	// audio) and only long text pays the blob's latency.
	ttsStreamMaxChars = 700

	// ttsMaxInputCharacters bounds one utterance. It is a transport bound of
	// this adapter (the vendor documents no character ceiling for the
	// preview), sized to the same 20k the other HTTP TTS adapters carry.
	ttsMaxInputCharacters = 20_000

	ttsExtensionID = "generativelanguage.googleapis.com/v1beta/models"

	defaultTTSMaxResponseBytes  = 128 << 20
	defaultTTSCloseIdleTimeout  = 30 * time.Second
	defaultTTSEventBuffer       = 32
	ttsMaxErrorBodyBytes        = 64 << 10
	sseDataPrefix               = "data:"
	finishReasonStop            = "STOP"
	responseModalityAudio       = "AUDIO"
	streamGenerateContentSuffix = ":streamGenerateContent"
	generateContentSuffix       = ":generateContent"
)

// ttsModels are the model ids this adapter serves.
var ttsModels = map[string]struct{}{
	TTSModel: {},
}

// TTSConfig controls local transport bounds. Provider identity, model,
// credential and endpoint always come from the verified plan, never from here.
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
	// CredentialHeader selects where the plan's credential is placed. Empty
	// means APIKeyHeader, which is what the AI Studio surface takes — the
	// same choice, for the same reason, as the STT adapter above.
	CredentialHeader string
}

// TTSAdapter opens Gemini speech-generation sessions.
type TTSAdapter struct {
	id                       string
	httpClient               *http.Client
	eventBuffer              int
	maxResponseBytes         int64
	gracefulCloseIdleTimeout time.Duration
	credentialHeader         string
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
		config.MaxResponseBytes = defaultTTSMaxResponseBytes
	}
	if config.GracefulCloseIdleTimeout == 0 {
		config.GracefulCloseIdleTimeout = defaultTTSCloseIdleTimeout
	}
	if config.CredentialHeader == "" {
		config.CredentialHeader = APIKeyHeader
	}
	if config.EventBuffer < 1 || config.MaxResponseBytes < 1 || config.GracefulCloseIdleTimeout <= 0 {
		return nil, errors.New("gemini tts: event buffer, response bound and close timeout must be positive")
	}
	if config.CredentialHeader != APIKeyHeader && config.CredentialHeader != "authorization" {
		return nil, fmt.Errorf("gemini tts: credential header must be %s or authorization, got %q", APIKeyHeader, config.CredentialHeader)
	}
	policy, err := upstream.NewHTTPPolicy(officialHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &TTSAdapter{
		id: config.AdapterID, httpClient: config.HTTPClient, eventBuffer: config.EventBuffer,
		maxResponseBytes: config.MaxResponseBytes, gracefulCloseIdleTimeout: config.GracefulCloseIdleTimeout,
		credentialHeader: config.CredentialHeader,
		endpointPolicy:   policy,
	}, nil
}

// ID returns the adapter identifier.
func (a *TTSAdapter) ID() string { return a.id }

// Open validates the plan and returns a stream ready for AppendText/CommitText.
func (a *TTSAdapter) Open(_ context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindTTS {
		return nil, fmt.Errorf("gemini tts supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != ProviderName {
		return nil, fmt.Errorf("gemini tts adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportHTTP {
		return nil, fmt.Errorf("gemini tts requires http transport, got %q", request.Plan.Route.Transport)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if _, ok := ttsModels[model]; !ok {
		return nil, fmt.Errorf("gemini tts does not support model %q", model)
	}
	if request.Media == nil {
		return nil, errors.New("gemini tts requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("gemini tts media: %w", err)
	}
	if request.Media.Encoding != "pcm_s16le" || request.Media.Channels != 1 || request.Media.SampleRateHz != ttsOutputSampleRateHz {
		return nil, fmt.Errorf("gemini tts generates mono pcm_s16le at %d Hz only", ttsOutputSampleRateHz)
	}
	voice := strings.TrimSpace(request.Options.Voice)
	if voice == "" {
		voice = strings.TrimSpace(request.Plan.Route.Voice)
	}
	if voice == "" {
		voice = TTSDefaultVoice
	}
	credential := request.Plan.Route.Credential
	if credential == nil || strings.TrimSpace(credential.Value) == "" ||
		(credential.Kind != protocol.CredentialBearer && !(request.Plan.Execution.ProviderRoute == protocol.RouteSpekoRelay && credential.Kind == protocol.CredentialRelayAccess)) {
		return nil, errors.New("gemini tts requires a bearer credential")
	}
	endpoint, err := a.parseEndpoint(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}
	client := a.httpClient
	if client == nil {
		client = http.DefaultClient
	}
	streamCtx, cancel := context.WithCancel(context.Background())
	return &ttsStream{
		ctx: streamCtx, cancel: cancel,
		events: make(chan runtimepkg.ProviderEvent, a.eventBuffer), responseProgress: make(chan struct{}, 1),
		httpClient: client, endpoint: endpoint, credential: credential.Value, credentialHeader: a.credentialHeader,
		maxResponseBytes: a.maxResponseBytes, gracefulCloseIdleTimeout: a.gracefulCloseIdleTimeout,
		model: model, voice: voice,
	}, nil
}

// parseEndpoint validates the plan's endpoint as the bare models collection.
// The per-request model id and arm are appended locally, so the digest-pinned
// catalog value never varies per model and the policy has already vetted the
// host before anything is added.
func (a *TTSAdapter) parseEndpoint(raw string) (*url.URL, error) {
	endpoint, err := a.endpointPolicy.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("gemini tts endpoint: %w", err)
	}
	if endpoint.Path != "/v1beta/models" {
		return nil, fmt.Errorf("gemini tts endpoint path must be /v1beta/models, got %q", endpoint.Path)
	}
	return endpoint, nil
}

type ttsStream struct {
	ctx              context.Context
	cancel           context.CancelFunc
	events           chan runtimepkg.ProviderEvent
	responseProgress chan struct{}

	httpClient               *http.Client
	endpoint                 *url.URL
	credential               string
	credentialHeader         string
	maxResponseBytes         int64
	gracefulCloseIdleTimeout time.Duration
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

func (s *ttsStream) AppendText(_ context.Context, text string) error {
	if text == "" {
		return errors.New("gemini tts text is empty")
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return runtimepkg.ErrSessionClosed
	}
	if s.inFlight {
		return errors.New("gemini tts previous utterance has not completed")
	}
	if utf8.RuneCountInString(s.pending.String())+utf8.RuneCountInString(text) > ttsMaxInputCharacters {
		return &runtimepkg.ProviderError{Code: "input_too_large", Message: fmt.Sprintf("Gemini TTS input exceeds %d characters", ttsMaxInputCharacters), Retryable: false, ProviderStatus: http.StatusRequestEntityTooLarge}
	}
	s.pending.WriteString(text)
	return nil
}

// CommitText synthesizes the buffered utterance. Short text streams over SSE
// for lower time-to-first-audio; text long enough to risk the streaming arm's
// >60s truncation takes the non-streaming arm and arrives as one clip (see
// ttsStreamMaxChars).
func (s *ttsStream) CommitText(ctx context.Context) error {
	text, requestCtx, requestCancel, done, err := s.beginRequest()
	if err != nil {
		return err
	}
	streaming := utf8.RuneCountInString(text) <= ttsStreamMaxChars
	body, err := json.Marshal(map[string]any{
		"contents": []map[string]any{{"parts": []map[string]any{{"text": text}}}},
		"generationConfig": map[string]any{
			"responseModalities": []string{responseModalityAudio},
			"speechConfig": map[string]any{
				"voiceConfig": map[string]any{
					"prebuiltVoiceConfig": map[string]any{"voiceName": s.voice},
				},
			},
		},
	})
	if err != nil {
		s.abandonRequest(requestCancel, done)
		return err
	}
	requestURL := *s.endpoint
	if streaming {
		requestURL.Path += "/" + s.model + streamGenerateContentSuffix
		requestURL.RawQuery = "alt=sse"
	} else {
		requestURL.Path += "/" + s.model + generateContentSuffix
	}
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, requestURL.String(), bytes.NewReader(body))
	if err != nil {
		s.abandonRequest(requestCancel, done)
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	if s.credentialHeader == "authorization" {
		request.Header.Set("Authorization", "Bearer "+s.credential)
	} else {
		request.Header.Set(APIKeyHeader, s.credential)
	}
	response, err := s.httpClient.Do(request)
	if err != nil {
		s.abandonRequest(requestCancel, done)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Gemini TTS request could not be sent", Retryable: true, Cause: err}
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		providerErr := ttsStatusError(response)
		_ = response.Body.Close()
		s.abandonRequest(requestCancel, done)
		return providerErr
	}
	s.readers.Add(1)
	go s.readResponse(requestCtx, response, requestCancel, done, streaming)
	return nil
}

func (s *ttsStream) beginRequest() (string, context.Context, context.CancelFunc, chan struct{}, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return "", nil, nil, nil, runtimepkg.ErrSessionClosed
	}
	if s.inFlight {
		return "", nil, nil, nil, errors.New("gemini tts previous utterance has not completed")
	}
	text := s.pending.String()
	if text == "" {
		return "", nil, nil, nil, errors.New("gemini tts has no buffered text to synthesize")
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

// ttsPayload is the subset of a generateContent response either arm reads.
type ttsPayload struct {
	Candidates []struct {
		Content struct {
			Parts []struct {
				InlineData struct {
					Data string `json:"data"`
				} `json:"inlineData"`
			} `json:"parts"`
		} `json:"content"`
		FinishReason string `json:"finishReason"`
	} `json:"candidates"`
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Status  string `json:"status"`
	} `json:"error"`
}

func (s *ttsStream) readResponse(requestCtx context.Context, response *http.Response, requestCancel context.CancelFunc, done chan struct{}, streaming bool) {
	defer func() {
		requestCancel()
		_ = response.Body.Close()
		s.finishRequest()
		close(done)
		s.readers.Done()
	}()
	if streaming {
		s.readSSE(requestCtx, response.Body)
		return
	}
	s.readBlob(requestCtx, response.Body)
}

// readSSE decodes each `data:` frame's inline PCM and emits it as its own
// audio frame. The known >60s truncation surfaces as a non-STOP finishReason
// on a 200 stream; whatever audio arrived is kept and the utterance completes,
// with the reason recorded on the AudioDone event so the runtime can see the
// clip may be cut short.
func (s *ttsStream) readSSE(requestCtx context.Context, body io.Reader) {
	limited := &io.LimitedReader{R: body, N: s.maxResponseBytes + 1}
	reader := bufio.NewReaderSize(limited, 64<<10)
	started := false
	finishReason := ""
	for {
		line, err := reader.ReadString('\n')
		if line != "" {
			s.reportResponseProgress()
			if limited.N <= 0 {
				s.emit(requestCtx, runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Gemini TTS response exceeded the configured limit", Retryable: true}})
				return
			}
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, sseDataPrefix) {
				frame := strings.TrimSpace(strings.TrimPrefix(trimmed, sseDataPrefix))
				if frame != "" && frame != "[DONE]" {
					var payload ttsPayload
					if decodeErr := json.Unmarshal([]byte(frame), &payload); decodeErr != nil {
						s.emit(requestCtx, runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Gemini TTS returned an undecodable stream frame", Retryable: true, Cause: decodeErr}})
						return
					}
					if payload.Error != nil {
						s.emit(requestCtx, runtimepkg.ProviderEvent{Err: payloadError(payload)})
						return
					}
					audio, finish, decodeErr := payloadAudio(payload)
					if decodeErr != nil {
						s.emit(requestCtx, runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Gemini TTS returned undecodable audio", Retryable: true, Cause: decodeErr}})
						return
					}
					if finish != "" {
						finishReason = finish
					}
					if len(audio) > 0 {
						if !started {
							started = true
							if !s.emit(requestCtx, runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted}) {
								return
							}
						}
						if !s.emit(requestCtx, runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Audio: audio}) {
							return
						}
					}
				}
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				if s.wasCanceled() || s.ctx.Err() != nil {
					return
				}
				if !started {
					s.emit(requestCtx, runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Gemini TTS returned no audio", Retryable: true}})
					return
				}
				s.emit(requestCtx, runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: doneData(finishReason)})
				return
			}
			if !s.wasCanceled() && s.ctx.Err() == nil {
				s.emit(requestCtx, runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Gemini TTS response stream failed", Retryable: true, Cause: err}})
			}
			return
		}
	}
}

// readBlob decodes the non-streaming arm's single JSON body and emits its
// whole clip as one audio frame.
func (s *ttsStream) readBlob(requestCtx context.Context, body io.Reader) {
	raw, err := io.ReadAll(io.LimitReader(body, s.maxResponseBytes+1))
	if err != nil {
		if !s.wasCanceled() && s.ctx.Err() == nil {
			s.emit(requestCtx, runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Gemini TTS response could not be read", Retryable: true, Cause: err}})
		}
		return
	}
	s.reportResponseProgress()
	if int64(len(raw)) > s.maxResponseBytes {
		s.emit(requestCtx, runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Gemini TTS response exceeded the configured limit", Retryable: true}})
		return
	}
	var payload ttsPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		s.emit(requestCtx, runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Gemini TTS returned an undecodable response", Retryable: true, Cause: err}})
		return
	}
	if payload.Error != nil {
		s.emit(requestCtx, runtimepkg.ProviderEvent{Err: payloadError(payload)})
		return
	}
	audio, finishReason, decodeErr := payloadAudio(payload)
	if decodeErr != nil {
		s.emit(requestCtx, runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Gemini TTS returned undecodable audio", Retryable: true, Cause: decodeErr}})
		return
	}
	if len(audio) == 0 {
		s.emit(requestCtx, runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Gemini TTS returned no audio", Retryable: true}})
		return
	}
	if !s.emit(requestCtx, runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted}) {
		return
	}
	if !s.emit(requestCtx, runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Audio: audio}) {
		return
	}
	s.emit(requestCtx, runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: doneData(finishReason)})
}

// payloadAudio concatenates every inline part's PCM and reports the last
// non-empty finishReason. The SSE arm delivers one part per frame in
// practice; tolerating several keeps a multi-part blob response whole. An
// undecodable part fails the whole payload: dropping it would splice the
// surrounding audio together and report the gapped clip as a success.
func payloadAudio(payload ttsPayload) ([]byte, string, error) {
	var audio []byte
	finish := ""
	for _, candidate := range payload.Candidates {
		for _, part := range candidate.Content.Parts {
			if part.InlineData.Data == "" {
				continue
			}
			decoded, err := base64.StdEncoding.DecodeString(part.InlineData.Data)
			if err != nil {
				return nil, "", fmt.Errorf("undecodable inline audio: %w", err)
			}
			audio = append(audio, decoded...)
		}
		if candidate.FinishReason != "" {
			finish = candidate.FinishReason
		}
	}
	return audio, finish, nil
}

// doneData records a truncated generation on the AudioDone event. A STOP (or
// absent) finish is the normal end and carries no payload.
func doneData(finishReason string) json.RawMessage {
	if finishReason == "" || finishReason == finishReasonStop {
		return nil
	}
	data, _ := json.Marshal(map[string]any{"finish_reason": finishReason})
	return data
}

func payloadError(payload ttsPayload) *runtimepkg.ProviderError {
	message := "Gemini TTS reported an error"
	status := 0
	if payload.Error != nil {
		if payload.Error.Message != "" {
			message = "Gemini TTS: " + payload.Error.Message
		}
		status = payload.Error.Code
	}
	code := "provider_rejected_request"
	retryable := false
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		code = "provider_authentication_failed"
	case status == http.StatusTooManyRequests:
		code, retryable = "provider_rate_limited", true
	case status >= 500:
		code, retryable = "provider_unavailable", true
	}
	return &runtimepkg.ProviderError{Code: code, Message: message, Retryable: retryable, ProviderStatus: status}
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
				Code: "provider_unavailable", Message: "Gemini TTS response stalled during graceful close",
				Retryable: true, Cause: context.DeadlineExceeded,
			}
		}
	}
}

// ttsStatusError maps a non-2xx HTTP status onto the canonical provider error
// vocabulary and preserves a JSON body as a raw extension for diagnosis.
func ttsStatusError(response *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(response.Body, ttsMaxErrorBodyBytes))
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
		Code: code, Message: fmt.Sprintf("Gemini TTS rejected the synthesis request with status %d", response.StatusCode),
		Retryable: retryable, ProviderStatus: response.StatusCode,
	}
	if json.Valid(body) {
		providerErr.Extensions = map[string]json.RawMessage{ttsExtensionID: append(json.RawMessage(nil), body...)}
	}
	return providerErr
}
