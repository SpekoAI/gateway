package azure

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

const (
	// RealtimeAdapterID identifies the MAI-Transcribe streaming implementation.
	RealtimeAdapterID = "azure.stt.realtime.v1"
	// RealtimeModel is the streaming MAI transcription model. On this API the
	// session's transcription.model is the Foundry DEPLOYMENT name, so the
	// relay's resource deploys the model under exactly this name and the
	// route model doubles as the deployment.
	RealtimeModel = "MAI-Transcribe-2-Streaming"

	// realtimeHost is the Foundry resource host family: every resource is
	// served on its own subdomain.
	realtimeHost      = "*.services.ai.azure.com"
	realtimePath      = "/mai/v1/realtime"
	realtimeExtension = "services.ai.azure.com/mai/v1/realtime"
	// realtimeKeyHeader is the API-key handshake header. The query-string
	// form is documented too; a header keeps the key out of URLs and logs.
	realtimeKeyHeader = "api-key"

	// realtimePCMFrameBytes caps one input_audio_buffer.append at 0.5 s of
	// 24 kHz s16le mono (sample-aligned), so a whole-utterance write does not
	// become one oversized WebSocket message.
	realtimePCMFrameBytes = 24_000
	// realtimeHandshakeTimeout bounds the wait for session.updated when the
	// caller's context carries no deadline of its own.
	realtimeHandshakeTimeout = 10 * time.Second
	// realtimeDefaultCloseTimeout bounds how long Close keeps the stream open
	// for finals still owed to commits. Live, `completed` landed within a
	// second of its commit; ten leaves room without parking a caller until
	// the session lease runs out.
	realtimeDefaultCloseTimeout = 10 * time.Second
)

// realtimeModels are the deployments this adapter opens.
var realtimeModels = map[string]struct{}{RealtimeModel: {}}

// realtimeSampleRates are the only PCM rates the session format accepts:
// "Raw signed little-endian PCM16, mono, 16000 or 24000 samples/second."
var realtimeSampleRates = map[int]struct{}{16_000: {}, 24_000: {}}

// RealtimeConfig controls local transport limits. Credentials and provider
// selection always come from the signed session plan.
type RealtimeConfig struct {
	AdapterID       string
	HTTPClient      *http.Client
	EventBuffer     int
	MaxMessageBytes int64
	// CloseTimeout bounds the wait, after Close, for finals owed to commits.
	// When it passes the stream ends with a provider error naming the
	// missing final instead of waiting for the session lease.
	CloseTimeout          time.Duration
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// RealtimeAdapter implements MAI-Transcribe-2-Streaming over the Foundry
// Realtime API, an OpenAI-Realtime-like WebSocket.
type RealtimeAdapter struct {
	id              string
	httpClient      *http.Client
	eventBuffer     int
	maxMessageBytes int64
	closeTimeout    time.Duration
	endpointPolicy  upstream.WebSocketPolicy
}

// NewRealtime creates the streaming STT adapter.
func NewRealtime(config RealtimeConfig) (*RealtimeAdapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = RealtimeAdapterID
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = 32
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = 1 << 20
	}
	if config.CloseTimeout == 0 {
		config.CloseTimeout = realtimeDefaultCloseTimeout
	}
	if config.EventBuffer < 1 {
		return nil, errors.New("azure realtime event buffer must be positive")
	}
	if config.CloseTimeout < 0 {
		return nil, errors.New("azure realtime close timeout must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("azure realtime maximum message bytes must be positive")
	}
	policy, err := upstream.NewWebSocketPolicy(realtimeHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &RealtimeAdapter{id: config.AdapterID, httpClient: config.HTTPClient, eventBuffer: config.EventBuffer, maxMessageBytes: config.MaxMessageBytes, closeTimeout: config.CloseTimeout, endpointPolicy: policy}, nil
}

func (a *RealtimeAdapter) ID() string { return a.id }

// Open dials the socket, configures the transcription session and waits for
// session.updated, so a missing deployment, a wrong key or a refused setting
// surfaces as an open failure the runtime can fail over rather than as an
// error frame mid-stream.
func (a *RealtimeAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindSTT {
		return nil, fmt.Errorf("azure realtime supports stt sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != ProviderName {
		return nil, fmt.Errorf("azure realtime adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("azure realtime stt requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("azure realtime stt requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("azure realtime stt media: %w", err)
	}
	if _, ok := realtimeSampleRates[request.Media.SampleRateHz]; request.Media.Encoding != "pcm_s16le" || request.Media.Channels != 1 || !ok {
		return nil, &runtimepkg.ProviderError{
			Code:    "unsupported_media",
			Message: fmt.Sprintf("MAI-Transcribe-2-Streaming requires mono pcm_s16le at 16000 or 24000 Hz, got %s/%d channels at %d Hz", request.Media.Encoding, request.Media.Channels, request.Media.SampleRateHz),
			Hint:    "Resample the input to mono pcm_s16le at 16000 or 24000 Hz.",
		}
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if _, ok := realtimeModels[model]; !ok {
		return nil, fmt.Errorf("azure realtime stt does not support model %q", model)
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableRealtimeCredential(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("azure realtime stt requires an api-key credential")
	}
	endpoint, err := a.endpointPolicy.Parse(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("azure realtime endpoint: %w", err)
	}
	if endpoint.Path != realtimePath {
		return nil, fmt.Errorf("azure realtime endpoint path must be %s, got %q", realtimePath, endpoint.Path)
	}
	query := endpoint.Query()
	query.Set("intent", "transcription")
	endpoint.RawQuery = query.Encode()

	headers := make(http.Header)
	headers.Set(realtimeKeyHeader, strings.TrimSpace(credential.Value))
	client := a.httpClient
	if client == nil {
		client = http.DefaultClient
	}
	conn, response, err := websocket.Dial(ctx, endpoint.String(), &websocket.DialOptions{HTTPClient: client, HTTPHeader: headers})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		return nil, &runtimepkg.ProviderError{
			Code:           realtimeDialErrorCode(status),
			Message:        "Azure MAI Realtime connection could not be established",
			Retryable:      status == 0 || status == http.StatusTooManyRequests || status >= 500,
			ProviderStatus: status,
			Cause:          err,
		}
	}
	conn.SetReadLimit(a.maxMessageBytes)

	update := realtimeSessionUpdate{Type: "session.update", Session: realtimeSessionConfig{
		Type: "transcription",
		Audio: realtimeAudioConfig{Input: realtimeAudioInputConfig{
			Format:        realtimeAudioFormat{Type: "audio/pcm", Rate: request.Media.SampleRateHz},
			Transcription: realtimeTranscription{Model: model, Language: realtimeLanguage(request.Options.Language)},
		}},
	}}
	sessionID, err := realtimeHandshake(ctx, conn, update)
	if err != nil {
		_ = conn.CloseNow()
		return nil, err
	}
	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &realtimeStream{conn: conn, ctx: streamCtx, cancel: cancel, events: make(chan runtimepkg.ProviderEvent, a.eventBuffer), sessionID: sessionID, closeTimeout: a.closeTimeout}
	go stream.readLoop()
	return stream, nil
}

// acceptableRealtimeCredential mirrors the other adapters: a bearer-labelled
// permanent key, or relay_access on the relay route.
func acceptableRealtimeCredential(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

// realtimeLanguages is the language enum the Realtime session actually
// validates, transcribed from the service's own refusal (2026-10-02, live):
// "Invalid type for 'session.audio.input.transcription.language': expected
// one of 'af', 'ar', ... 'zh'". It is NOT the sixty-code table the docs list
// for the model: it spells Filipino `tl` and Norwegian `no` (the batch table
// says fil and nb), adds be/cy/hr/iw/mi/sr, and lacks as, bn, gu, ml, or, pa,
// te and yue. A value outside it fails the whole session with a 400, so the
// hint is mapped onto it or dropped.
var realtimeLanguages = map[string]struct{}{
	"af": {}, "ar": {}, "az": {}, "be": {}, "bg": {}, "bs": {}, "ca": {}, "cs": {}, "cy": {}, "da": {},
	"de": {}, "el": {}, "en": {}, "es": {}, "et": {}, "fa": {}, "fi": {}, "fr": {}, "gl": {}, "he": {},
	"hi": {}, "hr": {}, "hu": {}, "hy": {}, "id": {}, "is": {}, "it": {}, "iw": {}, "ja": {}, "kk": {},
	"kn": {}, "ko": {}, "lt": {}, "lv": {}, "mi": {}, "mk": {}, "mr": {}, "ms": {}, "ne": {}, "nl": {},
	"no": {}, "pl": {}, "pt": {}, "ro": {}, "ru": {}, "sk": {}, "sl": {}, "sr": {}, "sv": {}, "sw": {},
	"ta": {}, "th": {}, "tl": {}, "tr": {}, "uk": {}, "ur": {}, "vi": {}, "zh": {},
}

// realtimeLanguageAliases folds the spellings callers send onto the enum's.
var realtimeLanguageAliases = map[string]string{
	"fil": "tl",
	"nb":  "no",
	"nn":  "no",
	"in":  "id", // Indonesian, pre-1989 code
	"cmn": "zh",
}

// realtimeLanguage is the optional language hint: one code from the enum the
// session accepts, or nil to OMIT the field and let the model detect the
// language. Omission is the only auto-detect spelling: despite the docs, the
// service refuses an explicit `"language": null` with the same 400 as an
// unknown code, so nil must never reach the wire as null.
func realtimeLanguage(language string) *string {
	primary := strings.ToLower(strings.TrimSpace(language))
	if primary == "" || primary == "auto" {
		return nil
	}
	if index := strings.IndexAny(primary, "-_"); index > 0 {
		primary = primary[:index]
	}
	if folded, ok := realtimeLanguageAliases[primary]; ok {
		primary = folded
	}
	if _, ok := realtimeLanguages[primary]; !ok {
		return nil
	}
	return &primary
}

// realtimeHandshake sends session.update and reads until session.updated.
// session.created may precede it; settings lock at the first append, so
// nothing is appended before the update is acknowledged.
func realtimeHandshake(ctx context.Context, conn *websocket.Conn, update realtimeSessionUpdate) (string, error) {
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, realtimeHandshakeTimeout)
		defer cancel()
	}
	payload, err := json.Marshal(update)
	if err != nil {
		return "", err
	}
	if err := conn.Write(ctx, websocket.MessageText, payload); err != nil {
		return "", &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Azure MAI Realtime session could not be configured", Retryable: true, Cause: err}
	}
	sessionID := ""
	for {
		messageType, frame, err := conn.Read(ctx)
		if err != nil {
			return "", &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Azure MAI Realtime closed before acknowledging the session", Retryable: true, Cause: err}
		}
		if messageType != websocket.MessageText {
			continue
		}
		var message realtimeInbound
		if err := json.Unmarshal(frame, &message); err != nil {
			return "", &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Azure MAI Realtime sent malformed JSON", Retryable: true, Cause: err}
		}
		if message.Session != nil && message.Session.ID != "" {
			sessionID = message.Session.ID
		}
		switch message.Type {
		case "session.updated":
			return sessionID, nil
		case "error", "conversation.item.input_audio_transcription.failed":
			return "", realtimeEventError(message.Error, frame)
		}
	}
}

type realtimeStream struct {
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent

	writeMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	closing      atomic.Bool
	closeErr     error
	// pendingAudio is set by every append and cleared by a commit, so Close
	// commits only audio the model has not been asked to finalize yet.
	pendingAudio atomic.Bool
	// outstandingCommits counts commits whose `completed` (or empty-buffer
	// refusal) has not been delivered yet. It is a count, not a flag, because
	// Close can commit trailing audio while the runtime's own last commit is
	// still unanswered; Close keeps the read loop alive while it is positive.
	outstandingCommits atomic.Int32
	// closeTimeout bounds Close's wait for owed finals; finalTimedOut records
	// that it passed, so the read loop — the only goroutine that emits —
	// reports the missing final when the socket it was blocked on is closed
	// under it.
	closeTimeout  time.Duration
	finalTimedOut atomic.Bool
	// committed is the finalized text of the open commit window (the
	// concatenated delta events) and provisional the latest intermediate
	// suffix after it. Both are owned solely by readLoop.
	committed   string
	provisional string
	sessionID   string
}

func (s *realtimeStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

// WriteAudio forwards PCM as base64 inside input_audio_buffer.append.
func (s *realtimeStream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("azure realtime stt audio is empty")
	}
	for offset := 0; offset < len(audio); offset += realtimePCMFrameBytes {
		end := min(offset+realtimePCMFrameBytes, len(audio))
		if err := s.writeJSON(ctx, realtimeAppend{Type: "input_audio_buffer.append", Audio: base64.StdEncoding.EncodeToString(audio[offset:end])}); err != nil {
			return err
		}
	}
	s.pendingAudio.Store(true)
	return nil
}

// CommitAudio closes the current turn. The service has no server-side speech
// detection, so this is the only thing that produces a `completed` final;
// the runtime's end-of-speech signal reaches the model through it.
func (s *realtimeStream) CommitAudio(ctx context.Context) error {
	s.outstandingCommits.Add(1)
	if err := s.writeJSON(ctx, realtimeControl{Type: "input_audio_buffer.commit"}); err != nil {
		s.outstandingCommits.Add(-1)
		return err
	}
	s.pendingAudio.Store(false)
	return nil
}

func (s *realtimeStream) AppendText(context.Context, string) error {
	return runtimepkg.ErrUnsupportedOperation
}
func (s *realtimeStream) CommitText(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// Cancel tears the session down; the protocol has no cancel event for a
// transcription session.
func (s *realtimeStream) Cancel(ctx context.Context) error {
	if err := s.Close(ctx); err != nil {
		return err
	}
	return s.abort()
}

// Abort immediately tears down the socket after a terminal runtime failure.
func (s *realtimeStream) Abort(context.Context) error { return s.abort() }

// Close commits any buffered audio and stops accepting writes. The socket
// stays open so the last `completed` can land; the read loop releases
// Events once it has.
func (s *realtimeStream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		s.closing.Store(true)
		if s.pendingAudio.Load() {
			if err := s.CommitAudio(ctx); err != nil && !errors.Is(err, runtimepkg.ErrSessionClosed) {
				s.closeErr = err
			}
		}
		s.closed.Store(true)
		// No final is outstanding (nothing was buffered, its final already
		// landed, or the commit failed): no future frame will wake the read
		// loop, so release it here. Otherwise the loop releases itself after
		// delivering the final.
		if s.closeErr != nil || s.outstandingCommits.Load() <= 0 {
			s.cancel()
		} else if s.closeTimeout > 0 {
			// Bound the wait: Azure owes a final it may never send. The timer
			// does not emit (the read loop owns the channel and may already
			// have closed it); it closes the socket, and the read loop turns
			// the failed read into the error.
			time.AfterFunc(s.closeTimeout, func() {
				if s.outstandingCommits.Load() > 0 && s.ctx.Err() == nil {
					s.finalTimedOut.Store(true)
					_ = s.conn.CloseNow()
				}
			})
		}
		if s.closeErr != nil {
			_ = s.abort()
		}
	})
	return s.closeErr
}

func (s *realtimeStream) abort() error {
	s.abortOnce.Do(func() {
		s.cancel()
		if err := s.conn.CloseNow(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
	})
	return s.closeErr
}

func (s *realtimeStream) writeJSON(ctx context.Context, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if err := s.conn.Write(ctx, websocket.MessageText, payload); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Azure MAI Realtime write failed", Retryable: true, Cause: err}
	}
	return nil
}

func (s *realtimeStream) readLoop() {
	defer close(s.events)
	if s.sessionID != "" {
		if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: marshalTTSData(map[string]any{"provider_request_id": s.sessionID})}); err != nil {
			return
		}
	}
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if s.ctx.Err() == nil {
				switch {
				case s.finalTimedOut.Load():
					s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: fmt.Sprintf("Azure MAI Realtime sent no final for committed audio within %s of close", s.closeTimeout), Retryable: true, Cause: context.DeadlineExceeded}})
				case s.outstandingCommits.Load() > 0:
					// Even after Close: a socket that ends while a final is still
					// owed lost the caller's last words, and ending Events
					// cleanly would report that as success.
					s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Azure MAI Realtime closed before the final for committed audio arrived", Retryable: true, Cause: err}})
				case !s.closed.Load() && !realtimeIsNormalClose(err):
					s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Azure MAI Realtime read failed", Retryable: true, Cause: err}})
				}
			}
			return
		}
		if messageType != websocket.MessageText {
			continue
		}
		if err := s.handleMessage(payload); err != nil {
			s.emit(runtimepkg.ProviderEvent{Err: err})
			return
		}
	}
}

func (s *realtimeStream) handleMessage(payload []byte) error {
	var message realtimeInbound
	if err := json.Unmarshal(payload, &message); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Azure MAI Realtime sent malformed JSON", Retryable: true, Cause: err}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))
	switch message.Type {
	case "conversation.item.input_audio_transcription.delta":
		// A delta is newly FINALIZED text, appended verbatim (it carries its
		// own spacing). It supersedes the part of the provisional suffix it
		// finalized. Live (2026-10-02) the service finalizes in fragments that
		// can split a word (" Spe", then "ako.") and can respell what the
		// intermediate guessed ("Speaco" became "Speako."), so the unfinalized
		// remainder is kept only while the delta is a prefix of the suffix;
		// otherwise it is dropped until the next intermediate, which the
		// service sends right behind each delta. Keeping it is what stops the
		// cumulative partial from shrinking to the finalized text alone.
		if message.Delta == "" {
			return nil
		}
		s.committed += message.Delta
		if rest, ok := strings.CutPrefix(s.provisional, message.Delta); ok {
			s.provisional = rest
		} else {
			s.provisional = ""
		}
		return s.emitPartial(message, raw)
	case "conversation.item.input_audio_transcription.intermediate":
		// The MAI-specific partial: the whole provisional suffix after the
		// last delta, replacing the previous one. The canonical
		// transcript.delta is cumulative, so it carries committed+suffix.
		s.provisional = message.Intermediate
		return s.emitPartial(message, raw)
	case "conversation.item.input_audio_transcription.completed":
		// The commit window is closed; the next delta starts a fresh one. An
		// empty transcript is still a completed turn (silence), and
		// suppressing it would leave a batch caller waiting forever.
		text := strings.TrimSpace(message.Transcript)
		if text == "" {
			text = strings.TrimSpace(s.committed)
		}
		s.committed, s.provisional = "", ""
		err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventTranscriptFinal, Data: realtimeTranscriptData(text, true, message), Extensions: realtimeExtensions(raw)})
		// The service never ends a transcription session on its own, so once
		// Close was requested the completed item is the graceful terminal
		// acknowledgement. The count drops only after the final is queued.
		if err == nil {
			s.releaseFinal()
		}
		return err
	case "conversation.item.input_audio_transcription.failed":
		failure := realtimeEventError(message.Error, raw)
		failure.Message = "Azure MAI Realtime failed to transcribe the committed audio"
		if message.Error != nil && strings.TrimSpace(message.Error.Message) != "" {
			failure.Message += ": " + message.Error.Message
		}
		return failure
	case "error":
		// A commit with nothing (or too little) buffered is a skipped turn,
		// not a session failure; see the OpenAI adapter for the same rule.
		if realtimeIsEmptyCommit(message.Error) {
			err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: marshalTTSData(map[string]any{"message": "Azure MAI Realtime rejected a commit with no buffered audio; turn skipped"}), Extensions: realtimeExtensions(raw)})
			if err == nil {
				s.releaseFinal()
			}
			return err
		}
		return realtimeEventError(message.Error, raw)
	case "session.created", "session.updated", "input_audio_buffer.committed", "input_audio_buffer.cleared",
		"conversation.item.created", "conversation.item.added", "conversation.item.done":
		return nil
	default:
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: marshalTTSData(map[string]any{"message": "ignored Azure MAI Realtime event type", "provider_type": message.Type}), Extensions: realtimeExtensions(raw)})
	}
}

// releaseFinal records that one outstanding commit has been answered and,
// when Close is waiting and none remain, ends the read loop.
func (s *realtimeStream) releaseFinal() {
	remaining := s.outstandingCommits.Add(-1)
	if remaining < 0 {
		// A completed the adapter never asked for (a server-side commit):
		// nothing was outstanding.
		s.outstandingCommits.CompareAndSwap(remaining, 0)
		remaining = 0
	}
	if remaining == 0 && s.closing.Load() {
		s.cancel()
	}
}

func (s *realtimeStream) emitPartial(message realtimeInbound, raw json.RawMessage) error {
	text := strings.TrimSpace(s.committed + s.provisional)
	if text == "" {
		return nil
	}
	return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventTranscriptDelta, Data: realtimeTranscriptData(text, false, message), Extensions: realtimeExtensions(raw)})
}

func (s *realtimeStream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func realtimeIsNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}

func realtimeDialErrorCode(status int) string {
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return "authentication_failed"
	case status == http.StatusTooManyRequests:
		return "provider_rate_limited"
	case status >= 500 || status == 0:
		return "provider_unavailable"
	case status >= 400:
		return "invalid_request"
	default:
		return "provider_unavailable"
	}
}

// realtimeEventError maps an error frame. The documented surface names the
// frame but not its codes, so the OpenAI-shaped `type`/`code` vocabulary the
// protocol borrows is recognized and anything else stays retryable.
func realtimeEventError(detail *realtimeErrorDetail, raw json.RawMessage) *runtimepkg.ProviderError {
	if detail == nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Azure MAI Realtime reported an error without detail", Retryable: true, Extensions: realtimeExtensions(raw)}
	}
	code := "provider_unavailable"
	switch strings.ToLower(detail.Code) {
	case "invalid_api_key", "invalid_authentication", "unauthorized", "401", "403":
		code = "authentication_failed"
	case "rate_limit_exceeded", "too_many_requests", "429":
		code = "provider_rate_limited"
	case "deploymentnotfound", "model_not_found", "invalid_value", "invalid_request_error", "404":
		code = "invalid_request"
	default:
		switch strings.ToLower(detail.Type) {
		case "invalid_request_error":
			code = "invalid_request"
		case "authentication_error":
			code = "authentication_failed"
		case "rate_limit_error":
			code = "provider_rate_limited"
		}
	}
	message := "Azure MAI Realtime reported an error"
	if strings.TrimSpace(detail.Message) != "" {
		message += ": " + detail.Message
	}
	return &runtimepkg.ProviderError{Code: code, Message: message, Retryable: code == "provider_rate_limited" || code == "provider_unavailable", Extensions: realtimeExtensions(raw)}
}

func realtimeIsEmptyCommit(detail *realtimeErrorDetail) bool {
	if detail == nil {
		return false
	}
	if detail.Code == "input_audio_buffer_commit_empty" {
		return true
	}
	lowered := strings.ToLower(detail.Message)
	return strings.Contains(lowered, "buffer") && (strings.Contains(lowered, "empty") || strings.Contains(lowered, "too small") || strings.Contains(lowered, "no audio"))
}

func realtimeExtensions(raw json.RawMessage) map[string]json.RawMessage {
	return map[string]json.RawMessage{realtimeExtension: append(json.RawMessage(nil), raw...)}
}

func realtimeTranscriptData(text string, final bool, message realtimeInbound) json.RawMessage {
	return marshalTTSData(map[string]any{"text": text, "is_final": final, "speech_final": final, "item_id": message.ItemID})
}

// Outbound client events. `turn_detection` and `noise_reduction` carry no
// omitempty: the service supports only null for both, and null is what turns
// server-side detection off rather than leaving a session default in place.

type realtimeSessionUpdate struct {
	Type    string                `json:"type"`
	Session realtimeSessionConfig `json:"session"`
}

type realtimeSessionConfig struct {
	Type  string              `json:"type"`
	Audio realtimeAudioConfig `json:"audio"`
}

type realtimeAudioConfig struct {
	Input realtimeAudioInputConfig `json:"input"`
}

type realtimeAudioInputConfig struct {
	Format         realtimeAudioFormat   `json:"format"`
	Transcription  realtimeTranscription `json:"transcription"`
	TurnDetection  *struct{}             `json:"turn_detection"`
	NoiseReduction *struct{}             `json:"noise_reduction"`
}

type realtimeAudioFormat struct {
	Type string `json:"type"`
	Rate int    `json:"rate"`
}

type realtimeTranscription struct {
	Model string `json:"model"`
	// Language is omitted, never null, when there is no hint: the service
	// refuses `"language": null` (see realtimeLanguage).
	Language *string `json:"language,omitempty"`
}

type realtimeAppend struct {
	Type  string `json:"type"`
	Audio string `json:"audio"`
}

type realtimeControl struct {
	Type string `json:"type"`
}

// realtimeInbound is the union of the server events this adapter reads.
type realtimeInbound struct {
	Type         string               `json:"type"`
	ItemID       string               `json:"item_id"`
	Delta        string               `json:"delta"`
	Intermediate string               `json:"intermediate"`
	Transcript   string               `json:"transcript"`
	Session      *realtimeSessionRef  `json:"session"`
	Error        *realtimeErrorDetail `json:"error"`
}

type realtimeSessionRef struct {
	ID string `json:"id"`
}

type realtimeErrorDetail struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
}
