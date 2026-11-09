package palabra

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

const (
	STTAdapterID   = "palabra.stt.v1"
	sttPath        = "/asr/v1/speech-to-text/stream"
	STTModel       = "default"
	finalizeMarker = "<fin>"
)

var sttLanguages = map[string]struct{}{
	"ar": {}, "de": {}, "en": {}, "es": {}, "fr": {}, "hi": {}, "it": {},
	"ja": {}, "ko": {}, "nl": {}, "pt": {}, "ru": {}, "zh": {},
}

type STTConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	GracefulCloseTimeout  time.Duration
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

type STTAdapter struct {
	id              string
	httpClient      *http.Client
	eventBuffer     int
	maxMessageBytes int64
	closeTimeout    time.Duration
	endpointPolicy  endpointPolicy
}

func NewSTT(config STTConfig) (*STTAdapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = STTAdapterID
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = 32
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = 1 << 20
	}
	if config.GracefulCloseTimeout == 0 {
		config.GracefulCloseTimeout = 10 * time.Second
	}
	if config.GracefulCloseTimeout < 0 {
		return nil, errors.New("palabra stt graceful close timeout must be positive")
	}
	if config.EventBuffer < 1 {
		return nil, errors.New("palabra stt event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("palabra stt maximum message bytes must be positive")
	}
	policy, err := newEndpointPolicy([]string{"stream.palabra.ai", "api.palabra.ai"}, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &STTAdapter{id: config.AdapterID, httpClient: config.HTTPClient, eventBuffer: config.EventBuffer, maxMessageBytes: config.MaxMessageBytes, closeTimeout: config.GracefulCloseTimeout, endpointPolicy: policy}, nil
}

func (a *STTAdapter) ID() string { return a.id }

func (a *STTAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindSTT {
		return nil, fmt.Errorf("palabra stt supports stt sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "palabra" {
		return nil, fmt.Errorf("palabra stt adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("palabra stt requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model != STTModel && model != "auto" {
		return nil, fmt.Errorf("palabra stt does not expose selectable models; got %q", model)
	}
	if request.Media == nil {
		return nil, errors.New("palabra stt requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("palabra stt media: %w", err)
	}
	if request.Media.Encoding != "pcm_s16le" || request.Media.Channels != 1 {
		return nil, fmt.Errorf("palabra stt requires mono pcm_s16le input, got %s/%d channels", request.Media.Encoding, request.Media.Channels)
	}
	language, err := sttLanguage(request.Options.Language)
	if err != nil {
		return nil, err
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("palabra stt requires a bearer credential")
	}
	endpoint, err := a.endpointPolicy.parse(request.Plan.Route.Endpoint, sttPath)
	if err != nil {
		return nil, fmt.Errorf("palabra stt endpoint: %w", err)
	}
	query := endpoint.Query()
	// The dedicated ASR service rejected the account key in both bearer and
	// raw Authorization headers with HTTP 401 during the 2026-09-11 staging
	// canary, while the same key succeeded in the documented token parameter.
	// TTS still uses the Authorization header.
	query.Set("token", credential.Value)
	query.Set("format", "pcm_s16le")
	query.Set("sample_rate", fmt.Sprint(request.Media.SampleRateHz))
	if language != "auto" {
		query.Set("language", language)
	}
	endpoint.RawQuery = query.Encode()

	conn, response, err := websocket.Dial(ctx, endpoint.String(), &websocket.DialOptions{
		HTTPClient: a.httpClient,
	})
	if err != nil {
		// net/http errors can contain the full URL. Never retain the token in
		// the error chain, including its query-escaped spelling.
		redacted := strings.NewReplacer(url.QueryEscape(credential.Value), "[redacted]", credential.Value, "[redacted]").Replace(err.Error())
		return nil, dialError("Palabra STT", response, errors.New(redacted))
	}
	conn.SetReadLimit(a.maxMessageBytes)
	streamCtx, cancel := context.WithCancelCause(context.Background())
	stream := &sttStream{conn: conn, ctx: streamCtx, cancel: cancel, events: make(chan runtimepkg.ProviderEvent, a.eventBuffer), closeTimeout: a.closeTimeout}
	go stream.readLoop()
	return stream, nil
}

func sttLanguage(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" || value == "auto" {
		return "auto", nil
	}
	if index := strings.IndexByte(value, '-'); index >= 0 {
		value = value[:index]
	}
	if _, ok := sttLanguages[value]; !ok {
		return "", fmt.Errorf("palabra stt does not support language %q", value)
	}
	return value, nil
}

type sttStream struct {
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelCauseFunc
	events chan runtimepkg.ProviderEvent

	writeMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	closeErr     error
	closeTimeout time.Duration
	finalizeSent bool // protected by writeMu; reset by subsequent audio
	pending      atomic.Int64
}

func (s *sttStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

func (s *sttStream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("palabra stt audio is empty")
	}
	return s.write(ctx, websocket.MessageBinary, audio)
}

// CommitAudio sends Palabra's documented manual end-of-segment command,
// {"message_type":"finalize"}. Palabra answers every finalize with a final
// transcript whose text carries a "<fin>" suffix, even when there was no new
// speech. File uploads need this because they may end without trailing
// silence for Palabra's own endpointer to mark.
func (s *sttStream) CommitAudio(ctx context.Context) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	return s.finalizeLocked(ctx)
}

func (s *sttStream) finalizeLocked(ctx context.Context) error {
	s.pending.Add(1)
	if err := s.conn.Write(ctx, websocket.MessageText, []byte(`{"message_type":"finalize"}`)); err != nil {
		s.pending.Add(-1)
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Palabra STT finalize failed", Hint: "The connection to Palabra failed while finalizing the transcript; " + retryRemedy, Retryable: true, Cause: err}
	}
	s.finalizeSent = true
	return nil
}

func (s *sttStream) AppendText(context.Context, string) error {
	return runtimepkg.ErrUnsupportedOperation
}
func (s *sttStream) CommitText(context.Context) error { return runtimepkg.ErrUnsupportedOperation }
func (s *sttStream) Cancel(context.Context) error     { return s.abort() }
func (s *sttStream) Abort(context.Context) error      { return s.abort() }

// Close stops input and drains every outstanding finalize acknowledgement,
// bounded by GracefulCloseTimeout. Palabra drops audio that has not been
// finalized when the socket closes, so Close finalizes first unless no audio
// arrived since the last finalize. It returns immediately so the caller can
// consume Events while the reader drains; a missing acknowledgement surfaces
// through TerminalError.
func (s *sttStream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		s.writeMu.Lock()
		if !s.closed.Load() && !s.finalizeSent {
			s.closeErr = s.finalizeLocked(ctx)
		}
		s.closed.Store(true)
		s.writeMu.Unlock()
		if s.closeErr != nil {
			s.cancel(s.closeErr)
		} else if s.pending.Load() == 0 {
			s.cancel(nil)
		} else {
			go func() {
				timer := time.NewTimer(s.closeTimeout)
				defer timer.Stop()
				select {
				case <-s.ctx.Done():
				case <-timer.C:
					s.cancel(&runtimepkg.ProviderError{Code: "request_timeout", Message: "Palabra STT did not acknowledge finalization before shutdown", Hint: fmt.Sprintf("Palabra did not finish the transcript within %s of the end of audio; %s", s.closeTimeout, retryRemedy), Retryable: true})
				}
			}()
		}
	})
	return s.closeErr
}

// TerminalError retains shutdown failures even when the event buffer is full.
func (s *sttStream) TerminalError() error {
	if err := context.Cause(s.ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func (s *sttStream) abort() error {
	s.abortOnce.Do(func() {
		s.closed.Store(true)
		s.cancel(nil)
		_ = s.conn.CloseNow()
	})
	return nil
}

func (s *sttStream) write(ctx context.Context, messageType websocket.MessageType, payload []byte) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if err := s.conn.Write(ctx, messageType, payload); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Palabra STT streaming write failed", Hint: "The connection to Palabra failed while sending audio; " + retryRemedy, Retryable: true, Cause: err}
	}
	s.finalizeSent = false
	return nil
}

func (s *sttStream) readLoop() {
	defer close(s.events)
	defer func() {
		if err := s.TerminalError(); err != nil {
			select {
			case s.events <- runtimepkg.ProviderEvent{Err: err}:
			default:
			}
		}
	}()
	defer s.cancel(nil)
	defer s.conn.CloseNow()
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if s.ctx.Err() == nil && (!isNormalClose(err) || s.pending.Load() > 0) {
				s.cancel(streamFailure("Palabra STT closed before transcription completed", "the transcript was complete", err))
			}
			return
		}
		if messageType != websocket.MessageText {
			continue
		}
		if err := s.handleMessage(payload); err != nil {
			s.cancel(err)
			return
		}
		if s.closed.Load() && s.pending.Load() == 0 {
			return
		}
	}
}

func (s *sttStream) handleMessage(payload []byte) error {
	var message sttInbound
	if err := json.Unmarshal(payload, &message); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Palabra STT sent malformed streaming JSON", Hint: "Palabra sent a malformed streaming message; " + retryRemedy, Retryable: true, Cause: err}
	}
	raw := append([]byte(nil), payload...)
	switch message.MessageType {
	case "transcription":
		// A finalize acknowledgement appends this protocol marker, including
		// when there was no speech. It is never transcript content.
		finalized := strings.HasSuffix(message.Segment.Text, finalizeMarker)
		message.Segment.Text = strings.TrimSuffix(message.Segment.Text, finalizeMarker)
		kind := protocol.EventTranscriptDelta
		if message.IsEOS || finalized {
			kind = protocol.EventTranscriptFinal
		}
		err := s.emit(runtimepkg.ProviderEvent{
			Type: kind,
			Data: marshalData(map[string]any{
				"text": message.Segment.Text, "is_final": message.IsEOS || finalized,
				"language": message.Language, "audio_start_ms": milliseconds(message.Segment.StartTime),
				"audio_end_ms": milliseconds(message.Segment.EndTime), "provider_request_id": message.TranscriptionID,
			}),
			Extensions: extension(raw),
		})
		if finalized && s.pending.Load() > 0 {
			s.pending.Add(-1)
		}
		return err
	case "error":
		return message.Data.providerError(raw)
	default:
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: marshalData(map[string]any{"message": "ignored Palabra STT message type", "provider_type": message.MessageType}), Extensions: extension(raw)})
	}
}

func (s *sttStream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func milliseconds(seconds float64) int64 { return int64(math.Round(seconds * 1000)) }

type sttInbound struct {
	MessageType     string `json:"message_type"`
	TranscriptionID string `json:"transcription_id"`
	Language        string `json:"language"`
	IsEOS           bool   `json:"is_eos"`
	Segment         struct {
		Text      string  `json:"text"`
		StartTime float64 `json:"start_time"`
		EndTime   float64 `json:"end_time"`
	} `json:"segment"`
	Data frameError `json:"data"`
}
