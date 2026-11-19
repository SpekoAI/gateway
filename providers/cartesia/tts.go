package cartesia

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
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
	// AdapterID is the identifier returned by a Cartesia TTS session plan.
	AdapterID   = "cartesia.tts.v1"
	extensionID = "cartesia.ai/v1"

	defaultTTSVersion = "2026-03-01"
	officialAPIHost   = "api.cartesia.ai"
)

// Config controls local transport limits and the explicitly selected Cartesia
// API version. Provider identity, model, voice, and access token come from a
// session plan and its provider-neutral request options.
type Config struct {
	AdapterID             string
	Version               string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// Adapter implements Cartesia's /tts/websocket streaming API.
//
// Provider note: on this route a text commit with nothing to commit (nothing
// appended since the last commit) is ignored and the session stays open;
// other providers end the session.
//
// Cartesia ends a context nobody committed on its own; that expiry ends the
// vendor's context, not the caller's utterance. A fragment opens a new context
// of the same utterance once the current one has had no fragment for
// max(2 s, the configured max_buffer_delay_ms, 3 s by default) and no audio
// or flush_done for 2 s, unless text was appended to it after its last audio
// and no audio has answered it yet; until then it continues the current
// context.
// A socket Cartesia closes in the middle of an utterance is a retryable error
// by design. An utterance Cartesia ends, with or without audio, ends with
// audio.done.
//
// Limit: the adapter does not detect partial speech, neither inside a context
// nor a continuation context that ends without audio: Cartesia gives no
// reliable per-fragment marker, and dropped speech was not observed in the
// live probe. The adapter does not inspect text.
type Adapter struct {
	id              string
	version         string
	httpClient      *http.Client
	eventBuffer     int
	maxMessageBytes int64
	endpointPolicy  upstream.WebSocketPolicy
	now             func() time.Time
}

// New creates a bounded Cartesia TTS adapter.
func New(config Config) (*Adapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = AdapterID
	}
	if config.Version == "" {
		config.Version = defaultTTSVersion
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = 32
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = 1 << 20
	}
	if config.EventBuffer < 1 {
		return nil, errors.New("cartesia event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("cartesia maximum message bytes must be positive")
	}
	endpointPolicy, err := upstream.NewWebSocketPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &Adapter{
		id:              config.AdapterID,
		version:         config.Version,
		httpClient:      config.HTTPClient,
		eventBuffer:     config.EventBuffer,
		maxMessageBytes: config.MaxMessageBytes,
		endpointPolicy:  endpointPolicy,
		now:             time.Now,
	}, nil
}

func (a *Adapter) ID() string { return a.id }

// Open opens a Cartesia TTS WebSocket. Customer API keys use the X-API-Key
// header; short-lived managed access tokens use the documented handshake query
// parameter. Relay plans carry the connector's permanent key and therefore use
// the header form as well.
func (a *Adapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindTTS {
		return nil, fmt.Errorf("cartesia supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "cartesia" {
		return nil, fmt.Errorf("cartesia adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("cartesia requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("cartesia requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("cartesia media: %w", err)
	}
	if err := validateGenerationOptions(request.Plan.Route.Model, request.Options, *request.Media); err != nil {
		return nil, err
	}
	settings, err := ttsOptions(request.Plan.Route.Model, request.Options.TTS.Provider("cartesia"))
	if err != nil {
		return nil, err
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("cartesia requires a bearer credential")
	}

	endpoint, err := websocketEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}
	headers := make(http.Header)
	headers.Set("Cartesia-Version", a.version)
	// A relay plan is managed for billing purposes but carries the connector's
	// permanent Cartesia key, which belongs in the X-API-Key header exactly like
	// a BYOK key. The access_token query channel stays reserved for the
	// short-lived tokens of managed provider-direct routes.
	route := request.Plan.Execution.ProviderRoute
	shortLived := route != protocol.RouteSpekoRelay && request.Plan.Execution.CredentialSource != protocol.CredentialsBYOK
	if !shortLived {
		headers.Set("X-API-Key", credential.Value)
	}
	handshake := func(ctx context.Context, token string) (*websocket.Conn, string, error) {
		target := endpoint
		if shortLived {
			var err error
			if target, err = addAccessToken(endpoint, token); err != nil {
				return nil, "", err
			}
		}
		conn, response, err := websocket.Dial(ctx, target, &websocket.DialOptions{HTTPClient: httpClient(a.httpClient), HTTPHeader: headers})
		if err != nil {
			status := 0
			if response != nil {
				status = response.StatusCode
			}
			return nil, "", &runtimepkg.ProviderError{
				Code:           dialErrorCode(status),
				Message:        "Cartesia streaming connection could not be established",
				Retryable:      status == 0 || status == http.StatusTooManyRequests || status >= 500,
				ProviderStatus: status,
				Cause:          err,
			}
		}
		conn.SetReadLimit(a.maxMessageBytes)
		requestID := ""
		if response != nil {
			requestID = response.Header.Get("X-Request-ID")
		}
		return conn, requestID, nil
	}
	// A new socket on a managed provider-direct route needs a current
	// short-lived token: the plan's may have expired while the first socket
	// was open, so every redial asks the refresher and builds the URL again.
	refresh := request.RefreshCredential
	redial := func(ctx context.Context) (*websocket.Conn, string, error) {
		token := credential.Value
		if shortLived && refresh != nil {
			current, err := refresh(ctx)
			if err != nil || !acceptableCredentialKind(route, current.Kind) || strings.TrimSpace(current.Value) == "" || (!current.ExpiresAt.IsZero() && !a.now().Before(current.ExpiresAt)) {
				return nil, "", &runtimepkg.ProviderError{
					Code:      "provider_unavailable",
					Message:   "Cartesia credential could not be renewed for a new connection",
					Hint:      "The provider credential could not be renewed to reconnect; retry the utterance on a new session.",
					Retryable: true,
					Cause:     err,
				}
			}
			token = current.Value
		}
		return handshake(ctx, token)
	}
	conn, requestID, err := handshake(ctx, credential.Value)
	if err != nil {
		return nil, err
	}
	bufferDelay := defaultBufferDelay
	if settings.maxBufferDelay != nil {
		bufferDelay = time.Duration(*settings.maxBufferDelay) * time.Millisecond
	}
	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &stream{
		dial:        redial,
		now:         a.now,
		bufferDelay: bufferDelay,
		conn:        conn,
		ctx:         streamCtx,
		cancel:      cancel,
		events:      make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		modelID:     request.Plan.Route.Model,
		voiceID:     request.Options.Voice,
		language:    request.Options.Language,
		media:       *request.Media,

		settings: settings,
	}
	if stream.setProviderRequestID(requestID) {
		if err := stream.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: usageData(requestID)}); err != nil {
			_ = stream.abort()
			return nil, err
		}
	}
	go stream.readLoop(conn)
	return stream, nil
}

func httpClient(client *http.Client) *http.Client {
	if client != nil {
		return client
	}
	return http.DefaultClient
}

// acceptableCredentialKind reports whether a delegated credential's kind may
// authenticate the plan's route. Bearer is the norm everywhere; the relay arm
// additionally accepts relay_access, because protocol.SessionPlan validation
// requires relay plans to label their credential relay_access while a relay
// connector that synthesizes the plan and drives this adapter directly — no
// Engine, no SessionPlan.Validate — labels the same permanent Cartesia key
// bearer. Both spellings carry a permanent key; the header-versus-query
// channel split keys off the route, not the kind, so nothing else on the
// relay arm changes.
func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

func websocketEndpoint(policy upstream.WebSocketPolicy, rawEndpoint string) (string, error) {
	endpoint, err := policy.Parse(rawEndpoint)
	if err != nil {
		return "", fmt.Errorf("cartesia endpoint: %w", err)
	}
	if endpoint.Path != "/tts/websocket" {
		return "", fmt.Errorf("cartesia endpoint path must be /tts/websocket, got %q", endpoint.Path)
	}
	return endpoint.String(), nil
}

func addAccessToken(rawEndpoint, token string) (string, error) {
	endpoint, err := url.Parse(rawEndpoint)
	if err != nil {
		return "", errors.New("cartesia endpoint could not be prepared")
	}
	query := endpoint.Query()
	query.Set("access_token", token)
	endpoint.RawQuery = query.Encode()
	return endpoint.String(), nil
}

func validateGenerationOptions(model string, options protocol.RequestOptions, media protocol.MediaFormat) error {
	if strings.TrimSpace(model) == "" || model == "auto" {
		return errors.New("cartesia requires a concrete model in the session plan")
	}
	if strings.TrimSpace(options.Voice) == "" {
		return errors.New("cartesia requires a voice id in request options")
	}
	if strings.TrimSpace(options.Language) == "" {
		return errors.New("cartesia requires a language in request options")
	}
	if media.Encoding != "pcm_s16le" {
		return fmt.Errorf("cartesia streaming output requires pcm_s16le, got %q", media.Encoding)
	}
	return nil
}

// stream keeps one client session on one Cartesia socket at a time.
//
// The caller speaks in utterances: fragments, then a commit. Cartesia speaks
// in contexts and ends a context nobody committed on its own. Its docs count
// that expiry from the context's last audio output: one second
// (https://docs.cartesia.ai/use-the-api/tts-websocket/contexts.md). Measured
// live on 2026-11-20 (sonic-3.6, max_buffer_delay_ms 0), the expiry done came
// 3.9 to 5.8 s after the last audio chunk arrived, which was 4.7 to 6.8 s
// after the last fragment was sent. A fragment that reaches a context after
// its expiry is spoken on a re-created context after that done, so it must
// never be written to a context that may be expiring:
//
//   - An utterance owns an ordered list of contexts and speaks through the
//     newest. A fragment that finds that context quiet (no text appended
//     after its last audio still awaiting audio, nothing for quietGap since
//     its last audio, flush_done or fragment, and nothing for the buffer delay
//     since its last fragment) abandons it and opens a new context in the same
//     utterance, whose word timings continue after the audio already sent. An
//     abandoned context is never written again. The fragment is judged by the
//     context's state before it is written.
//   - Frames are accepted only for the open utterance's newest context while
//     it can still speak, by context id alone. Frames for any other context —
//     the expiry done of an abandoned one, stale frames of an earlier
//     utterance — are dropped and reported as warnings, never emitted as
//     audio, timings or done.
//   - The utterance ends once, with audio.done whether or not it produced
//     audio: at the done of its committed context, or at the commit itself
//     when no context can speak any more.
//   - A socket Cartesia closes while no context can speak is replaced by the
//     next fragment, which was certainly not sent. A socket that closes while
//     a context can speak, or while a frame is being written, fails the
//     session with a retryable error.
type stream struct {
	dial   func(context.Context) (*websocket.Conn, string, error)
	now    func() time.Time
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent
	// eventsMu lets the writer side emit (a commit that ends an utterance
	// locally) while a reader may close events: emitters hold it shared,
	// finish holds it exclusively after cancelling ctx.
	eventsMu     sync.RWMutex
	eventsClosed bool
	finishOnce   sync.Once

	modelID  string
	voiceID  string
	language string
	media    protocol.MediaFormat
	// settings are the caller's provider_options.cartesia, sent on every
	// generation of every context; the zero value leaves vendor defaults.
	settings ttsSettings
	// bufferDelay is how long Cartesia may hold a fragment before speaking it.
	bufferDelay time.Duration

	writeMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	closing      atomic.Bool
	closeErr     error

	stateMu sync.Mutex
	// conn is nil after Cartesia closed an idle socket, until the next
	// fragment dials again.
	conn *websocket.Conn
	// writing is set, before the I/O, while a frame is being written;
	// lostWhileWriting records that the socket ended meanwhile, so the writer
	// reports an unknown outcome instead of the reader calling it idle.
	writing          bool
	lostWhileWriting bool
	utterance        *utterance
	droppedFrames    int

	providerRequestID string
}

// utterance is the caller's utterance, from its first fragment until it ends.
type utterance struct {
	// current is the context it speaks through; nil once that context ended
	// or was abandoned, until the next fragment opens another.
	current       *vendorContext
	lastContextID string
	committed     bool
	audioStarted  bool
	audioBytes    int
}

type vendorContext struct {
	id string
	// done closes when the context can no longer speak.
	done chan struct{}
	// offset is the utterance audio before this context, in seconds.
	offset    float64
	committed bool
	cancelled bool
	// awaitingAudio is set when text other than whitespace is appended and
	// cleared by the next audio chunk; until then the context is never quiet,
	// so audio still on its way (Cartesia may hold a buffered fragment for the
	// whole max_buffer_delay_ms) is spoken, not dropped, and the commit goes
	// to Cartesia. Its done still ends the context. It never fails anything.
	awaitingAudio bool
	lastFragment  time.Time
	lastAnswer    time.Time
}

const (
	// quietGap is how long a context may go without audio, flush_done or a
	// fragment before it is no longer written. The shortest expiry measured
	// was 3.9 s after the last audio chunk. If Cartesia ever expires a context
	// sooner (its docs say 1 s after the last audio output), a fragment written
	// to it is never emitted in the wrong utterance: its frames are dropped,
	// and the utterance goes on without it (see the limit on Adapter).
	quietGap = 2 * time.Second
	// defaultBufferDelay is Cartesia's documented max_buffer_delay_ms default.
	defaultBufferDelay = 3 * time.Second
)

func (s *stream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

func (s *stream) WriteAudio(context.Context, []byte) error { return runtimepkg.ErrUnsupportedOperation }

func (s *stream) CommitAudio(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// AppendText speaks a fragment of the open utterance, opening one if needed.
// It sets continue=true until CommitText supplies the utterance boundary.
func (s *stream) AppendText(ctx context.Context, text string) error {
	// A streaming LLM may emit word separators as their own chunks. Preserve
	// them exactly: trimming or rejecting them breaks valid streamed speech.
	if text == "" {
		return errors.New("cartesia transcript is empty")
	}
	for attempt := 0; ; attempt++ {
		contextID, conn, err := s.beginFragment(text)
		if err != nil {
			return err
		}
		if conn != nil {
			return s.send(ctx, conn, s.generationRequest(contextID, text, true))
		}
		// Cartesia closed an idle socket: this fragment was certainly not
		// sent, so it goes out on a new one.
		if attempt > 0 {
			return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Cartesia streaming connection is closed", Retryable: true}
		}
		if err := s.redial(ctx); err != nil {
			return err
		}
	}
}

// CommitText ends the open utterance. It finalizes the utterance's context,
// or ends the utterance here when no context of it can speak any more.
// With no utterance to end it returns runtime.ErrNothingToCommit and the
// session goes on.
func (s *stream) CommitText(ctx context.Context) error {
	contextID, conn, ended, err := s.beginCommit()
	switch {
	case err != nil:
		return err
	case !ended:
		return s.send(ctx, conn, s.generationRequest(contextID, "", false))
	default:
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: s.contextData(contextID)})
	}
}

// Cancel requests Cartesia to cancel queued work for the utterance's context.
// The provider documents that currently generating audio can continue, so
// callers must still await its terminal event or close the session. An
// utterance with no context left ends here.
func (s *stream) Cancel(ctx context.Context) error {
	contextID, conn, ended, err := s.beginCancel()
	switch {
	case err != nil:
		return err
	case ended:
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: s.contextData(contextID)})
	default:
		return s.send(ctx, conn, map[string]any{"context_id": contextID, "cancel": true})
	}
}

// Close waits for the utterance's context, while it can still speak, before
// closing the socket, preserving final audio when an application gracefully
// closes right after text.commit.
func (s *stream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		s.closing.Store(true)
		if done := s.activeDone(); done != nil {
			select {
			case <-done:
			case <-ctx.Done():
				s.closeErr = ctx.Err()
			}
		}
		if s.closeErr == nil {
			s.closed.Store(true)
			if conn := s.currentConn(); conn == nil {
				s.finish()
			} else if err := conn.Close(websocket.StatusNormalClosure, ""); err != nil {
				s.closeErr = err
			}
		}
		if s.closeErr != nil {
			_ = s.abort()
		}
	})
	return s.closeErr
}

// Abort immediately tears down the socket after a terminal runtime failure.
func (s *stream) Abort(context.Context) error { return s.abort() }

func (s *stream) abort() error {
	s.abortOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		conn := s.currentConn()
		if conn == nil {
			// No reader is left to close events.
			s.finish()
			return
		}
		if err := conn.CloseNow(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
		s.stateMu.Lock()
		s.endUtteranceLocked()
		s.stateMu.Unlock()
	})
	return s.closeErr
}

// finish ends the stream once: whoever sees it end last — the reader of the
// current socket, or Close/Abort when Cartesia already closed an idle one —
// closes events.
func (s *stream) finish() {
	s.finishOnce.Do(func() {
		s.cancel()
		s.stateMu.Lock()
		s.endUtteranceLocked()
		s.stateMu.Unlock()
		s.eventsMu.Lock()
		s.eventsClosed = true
		close(s.events)
		s.eventsMu.Unlock()
	})
}

func (s *stream) currentConn() *websocket.Conn {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.conn
}

// redial replaces a socket Cartesia closed while nothing was in flight; the
// old socket's reader sees it was replaced and exits quietly.
func (s *stream) redial(ctx context.Context) error {
	s.stateMu.Lock()
	if s.closed.Load() || s.closing.Load() {
		s.stateMu.Unlock()
		return runtimepkg.ErrSessionClosed
	}
	old := s.conn
	s.conn = nil
	s.stateMu.Unlock()
	if old != nil {
		_ = old.CloseNow()
	}
	conn, requestID, err := s.dial(ctx)
	if err != nil {
		return err
	}
	s.stateMu.Lock()
	if s.closed.Load() || s.closing.Load() {
		s.stateMu.Unlock()
		_ = conn.CloseNow()
		return runtimepkg.ErrSessionClosed
	}
	s.conn = conn
	s.stateMu.Unlock()
	go s.readLoop(conn)
	if s.setProviderRequestID(requestID) {
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: usageData(requestID)})
	}
	return nil
}

// beginFragment picks the context a fragment goes to and marks the write in
// progress before any I/O. A nil conn without an error means Cartesia closed
// the idle socket and nothing was marked.
func (s *stream) beginFragment(text string) (contextID string, conn *websocket.Conn, err error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() || s.closing.Load() {
		return "", nil, runtimepkg.ErrSessionClosed
	}
	if s.conn == nil {
		return "", nil, nil
	}
	u := s.utterance
	if u == nil {
		u = &utterance{}
		s.utterance = u
	} else if u.committed {
		return "", nil, errors.New("cartesia previous utterance has not completed")
	}
	now := s.now()
	if u.current != nil && s.quietLocked(u.current, now) {
		s.abandonLocked(u)
	}
	if u.current == nil {
		if err := s.openContextLocked(u); err != nil {
			return "", nil, err
		}
	}
	c := u.current
	c.lastFragment = now
	if strings.TrimSpace(text) != "" {
		c.awaitingAudio = true
	}
	s.writing, s.lostWhileWriting = true, false
	return c.id, s.conn, nil
}

// beginCommit marks the utterance committed. ended reports that it ended
// here, with no context left to finalize; otherwise the write of the final
// frame is marked in progress.
func (s *stream) beginCommit() (contextID string, conn *websocket.Conn, ended bool, err error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() || s.closing.Load() {
		return "", nil, false, runtimepkg.ErrSessionClosed
	}
	u := s.utterance
	if u == nil || u.committed {
		return "", nil, false, runtimepkg.ErrNothingToCommit
	}
	u.committed = true
	if u.current != nil && (s.conn == nil || s.quietLocked(u.current, s.now())) {
		s.abandonLocked(u)
	}
	if u.current == nil {
		contextID = u.lastContextID
		s.endUtteranceLocked()
		return contextID, nil, true, nil
	}
	u.current.committed = true
	s.writing, s.lostWhileWriting = true, false
	return u.current.id, s.conn, false, nil
}

func (s *stream) beginCancel() (contextID string, conn *websocket.Conn, ended bool, err error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() || s.closing.Load() {
		return "", nil, false, runtimepkg.ErrSessionClosed
	}
	u := s.utterance
	if u == nil {
		return "", nil, false, runtimepkg.ErrSessionClosed
	}
	if u.current != nil && (s.conn == nil || s.quietLocked(u.current, s.now())) {
		s.abandonLocked(u)
	}
	if u.current == nil {
		contextID = u.lastContextID
		s.endUtteranceLocked()
		return contextID, nil, true, nil
	}
	u.current.cancelled = true
	s.writing, s.lostWhileWriting = true, false
	return u.current.id, s.conn, false, nil
}

// send writes one frame whose write was marked in progress. When the socket
// fails around the write, Cartesia may or may not have the frame, so the
// outcome is reported as unknown and nothing is re-sent.
func (s *stream) send(ctx context.Context, conn *websocket.Conn, value any) error {
	payload, err := json.Marshal(value)
	if err == nil {
		s.writeMu.Lock()
		err = conn.Write(ctx, websocket.MessageText, payload)
		s.writeMu.Unlock()
		if err != nil {
			err = unknownSendError(err)
		}
	}
	s.stateMu.Lock()
	lost := s.lostWhileWriting
	s.writing, s.lostWhileWriting = false, false
	s.stateMu.Unlock()
	if err == nil && lost {
		err = unknownSendError(nil)
	}
	return err
}

// quietLocked reports whether a context may be expiring: no appended text
// awaits audio, and it has heard nothing for quietGap, nor a fragment for the
// buffer delay.
func (s *stream) quietLocked(c *vendorContext, now time.Time) bool {
	if c.committed || c.cancelled || c.awaitingAudio {
		return false
	}
	last := c.lastFragment
	if c.lastAnswer.After(last) {
		last = c.lastAnswer
	}
	return now.Sub(last) > quietGap && now.Sub(c.lastFragment) > s.bufferDelay
}

func (s *stream) openContextLocked(u *utterance) error {
	id, err := newContextID()
	if err != nil {
		return err
	}
	c := &vendorContext{id: id, done: make(chan struct{})}
	// A context's timestamps start at zero; within an utterance they start
	// after the audio already sent (pcm_s16le mono).
	if s.media.SampleRateHz > 0 {
		c.offset = float64(u.audioBytes) / float64(2*s.media.SampleRateHz)
	}
	u.current, u.lastContextID = c, id
	return nil
}

// abandonLocked stops writing to the utterance's context and stops accepting
// its frames; the utterance stays open.
func (s *stream) abandonLocked(u *utterance) {
	close(u.current.done)
	u.current = nil
}

func (s *stream) endUtteranceLocked() {
	if s.utterance != nil && s.utterance.current != nil {
		close(s.utterance.current.done)
	}
	s.utterance = nil
}

// activeDone returns the done channel of a context that can still speak.
func (s *stream) activeDone() <-chan struct{} {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.utterance == nil || s.utterance.current == nil || s.quietLocked(s.utterance.current, s.now()) {
		return nil
	}
	return s.utterance.current.done
}

// acceptLocked reports whether a frame belongs to the open utterance's
// current context, and returns both.
func (s *stream) acceptLocked(contextID string) (*utterance, *vendorContext, bool) {
	u := s.utterance
	if u == nil || u.current == nil || contextID == "" || u.current.id != contextID {
		return nil, nil, false
	}
	return u, u.current, true
}

// acceptAudio takes an audio chunk for the current context; started reports
// the utterance's first audio.
func (s *stream) acceptAudio(contextID string, bytes int) (accepted, started bool) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	u, c, ok := s.acceptLocked(contextID)
	if !ok {
		return false, false
	}
	c.awaitingAudio = false
	c.lastAnswer = s.now()
	u.audioBytes += bytes
	started = !u.audioStarted
	u.audioStarted = true
	return true, started
}

func (s *stream) acceptFlush(contextID string) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	_, c, ok := s.acceptLocked(contextID)
	if ok {
		c.lastAnswer = s.now()
	}
	return ok
}

// acceptTimestamps takes a timestamps frame of the current context and
// returns the offset its timings continue from.
func (s *stream) acceptTimestamps(contextID string) (offset float64, ok bool) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	_, c, ok := s.acceptLocked(contextID)
	if !ok {
		return 0, false
	}
	return c.offset, true
}

type doneOutcome int

const (
	doneDropped doneOutcome = iota
	// doneExpired: Cartesia expired an idle context; the utterance goes on.
	doneExpired
	doneFinished
)

// acceptDone decides what a done frame means.
func (s *stream) acceptDone(contextID string) doneOutcome {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	u, c, ok := s.acceptLocked(contextID)
	switch {
	case !ok:
		return doneDropped
	case c.cancelled:
		s.endUtteranceLocked()
		return doneFinished
	case c.committed || s.closing.Load():
		s.endUtteranceLocked()
		return doneFinished
	default:
		s.abandonLocked(u)
		return doneExpired
	}
}

// ownsCurrent reports whether a frame names the open utterance's current
// context.
func (s *stream) ownsCurrent(contextID string) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	_, _, ok := s.acceptLocked(contextID)
	return ok
}

func (s *stream) generationRequest(contextID, transcript string, continuation bool) generation {
	return generation{
		ModelID:          s.modelID,
		Transcript:       transcript,
		Voice:            voice{Mode: "id", ID: s.voiceID},
		Language:         s.language,
		Normalization:    s.settings.normalization,
		MaxBufferDelayMs: s.settings.maxBufferDelay,
		GenerationConfig: s.settings.generationConfig(),
		ContextID:        contextID,
		OutputFormat: outputFormat{
			Container:  "raw",
			Encoding:   "pcm_s16le",
			SampleRate: s.media.SampleRateHz,
		},
		AddTimestamps: true,
		Continue:      continuation,
	}
}

// readLoop reads one socket until it ends.
func (s *stream) readLoop(conn *websocket.Conn) {
	for {
		messageType, payload, err := conn.Read(s.ctx)
		if err != nil {
			s.socketEnded(conn, err)
			return
		}
		if messageType != websocket.MessageText {
			continue
		}
		if err := s.handleMessage(payload); err != nil {
			_ = s.emit(runtimepkg.ProviderEvent{Err: err})
			s.finish()
			return
		}
	}
}

// socketEnded decides what a closed socket means. Our own close ends the
// stream quietly; a socket already replaced by redial means nothing; a socket
// lost while a frame was being written is the writer's to report; one lost
// while the utterance's context can still speak fails the stream with a
// retryable error; and otherwise the next fragment dials again.
func (s *stream) socketEnded(conn *websocket.Conn, err error) {
	s.stateMu.Lock()
	if s.conn != conn {
		s.stateMu.Unlock()
		return
	}
	if s.closed.Load() || s.ctx.Err() != nil {
		s.stateMu.Unlock()
		s.finish()
		return
	}
	if s.writing {
		s.conn, s.lostWhileWriting = nil, true
		s.stateMu.Unlock()
		return
	}
	u := s.utterance
	canSpeak := u != nil && u.current != nil && !s.quietLocked(u.current, s.now())
	if !canSpeak && !s.closing.Load() {
		if u != nil && u.current != nil {
			s.abandonLocked(u)
		}
		s.conn = nil
		s.stateMu.Unlock()
		return
	}
	s.stateMu.Unlock()
	if canSpeak {
		_ = s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "Cartesia closed the stream mid-utterance",
			Hint:      "The provider connection closed while this utterance was being synthesized; retry the utterance on a new session.",
			Retryable: true,
			Cause:     err,
		}})
	}
	s.finish()
}

func (s *stream) handleMessage(payload []byte) error {
	var message inbound
	if err := json.Unmarshal(payload, &message); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Cartesia sent malformed streaming JSON", Retryable: true, Cause: err}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))
	if message.RequestID != "" && s.setProviderRequestID(message.RequestID) {
		if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: usageData(message.RequestID)}); err != nil {
			return err
		}
	}
	switch message.Type {
	case "chunk":
		audio, err := base64.StdEncoding.DecodeString(message.Data)
		if err != nil {
			return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Cartesia sent invalid audio data", Retryable: true, Cause: err}
		}
		accepted, started := s.acceptAudio(message.ContextID, len(audio))
		if !accepted {
			return s.emitDropped(message, raw)
		}
		if started {
			if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: s.contextData(message.ContextID), Extensions: extension(raw)}); err != nil {
				return err
			}
		}
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Data: s.contextData(message.ContextID), Extensions: extension(raw), Audio: audio})
	case "timestamps":
		offset, ok := s.acceptTimestamps(message.ContextID)
		if !ok {
			return s.emitDropped(message, raw)
		}
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAlignment, Data: s.alignmentData(message.ContextID, message.WordTimestamps, offset), Extensions: extension(raw)})
	case "flush_done":
		if !s.acceptFlush(message.ContextID) {
			return s.emitDropped(message, raw)
		}
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: s.warningData(message.ContextID, "flush_done"), Extensions: extension(raw)})
	case "done":
		switch s.acceptDone(message.ContextID) {
		case doneDropped:
			return s.emitDropped(message, raw)
		case doneExpired:
			return nil
		default:
			return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: s.contextData(message.ContextID), Extensions: extension(raw)})
		}
	case "error":
		if message.ContextID != "" && !s.ownsCurrent(message.ContextID) {
			// Cartesia answers input that reached a context after it ended —
			// a commit crossing the expiry done gets 400 "Invalid transcript".
			return s.emitDropped(message, raw)
		}
		if message.StatusCode == http.StatusBadRequest && (isUnsupportedLanguage(message.Message) || isUnsupportedLanguage(errorText(message.Error))) {
			return &runtimepkg.ProviderError{
				Code:           "invalid_request",
				Message:        fmt.Sprintf("Cartesia model %s does not support language %q", s.modelID, s.language),
				Hint:           "Choose a Cartesia model that supports this language, or use another TTS provider for it.",
				Retryable:      false,
				ProviderStatus: message.StatusCode,
				Extensions:     extension(raw),
			}
		}
		// The frame is kept for its error_code (Cartesia-Version 2026-03-01 and
		// later), which SafeProviderCode reads.
		return &runtimepkg.ProviderError{Code: cartesiaErrorCode(message.StatusCode), Message: cartesiaErrorMessage(message.Message), Retryable: message.StatusCode >= 500, ProviderStatus: message.StatusCode, Extensions: extension(raw)}
	default:
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: s.warningData(message.ContextID, message.Type), Extensions: extension(raw)})
	}
}

// emitDropped reports a frame for a context that is not the open utterance's
// current one. It carries the running count of dropped frames.
func (s *stream) emitDropped(message inbound, raw json.RawMessage) error {
	s.stateMu.Lock()
	s.droppedFrames++
	count := s.droppedFrames
	s.stateMu.Unlock()
	data := marshalData(map[string]any{"context_id": message.ContextID, "provider_type": message.Type, "provider_request_id": s.currentProviderRequestID(), "dropped": true, "dropped_frames": count})
	return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: data, Extensions: extension(raw)})
}

func unknownSendError(cause error) *runtimepkg.ProviderError {
	return &runtimepkg.ProviderError{
		Code:      "provider_unavailable",
		Message:   "Cartesia connection failed while a frame was being sent; whether Cartesia received it is unknown",
		Hint:      "The provider connection failed while sending this utterance; retry the utterance on a new session.",
		Retryable: true,
		Cause:     cause,
	}
}

func (s *stream) emit(event runtimepkg.ProviderEvent) error {
	s.eventsMu.RLock()
	defer s.eventsMu.RUnlock()
	if s.eventsClosed {
		return runtimepkg.ErrSessionClosed
	}
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func newContextID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("generate Cartesia context id: %w", err)
	}
	return hex.EncodeToString(bytes), nil
}

func isNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}

func dialErrorCode(status int) string {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return "authentication_failed"
	case http.StatusTooManyRequests:
		return "provider_rate_limited"
	default:
		return "provider_unavailable"
	}
}

func cartesiaErrorCode(status int) string {
	if status == http.StatusTooManyRequests {
		return "provider_rate_limited"
	}
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		return "authentication_failed"
	}
	return "provider_unavailable"
}

func extension(raw json.RawMessage) map[string]json.RawMessage {
	return map[string]json.RawMessage{extensionID: raw}
}

func (s *stream) setProviderRequestID(value string) bool {
	if value == "" {
		return false
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.providerRequestID == value {
		return false
	}
	s.providerRequestID = value
	return true
}

func (s *stream) currentProviderRequestID() string {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.providerRequestID
}

func (s *stream) contextData(contextID string) json.RawMessage {
	return marshalData(map[string]any{"context_id": contextID, "provider_request_id": s.currentProviderRequestID()})
}

// alignmentData carries Cartesia's own word_timestamps block verbatim and
// adds the normalized span reading beside it. Cartesia measures per WORD and
// reports start and END times in fractional seconds.
//
// A block that does not parse, or whose three parallel arrays disagree, is
// carried raw with no spans: the consumer's contract is that absent spans
// mean "nothing measured", so a garbled frame goes out silent rather than
// wrong.
//
// Each Cartesia context starts its clock at zero, so the spans of a context
// that continues an utterance after an expired one are shifted by the audio
// already sent; the raw block keeps Cartesia's own clock.
func (s *stream) alignmentData(contextID string, timestamps json.RawMessage, offset float64) json.RawMessage {
	payload := map[string]any{"context_id": contextID, "word_timestamps": timestamps, "provider_request_id": s.currentProviderRequestID()}
	var block struct {
		Words []string  `json:"words"`
		Start []float64 `json:"start"`
		End   []float64 `json:"end"`
	}
	if err := json.Unmarshal(timestamps, &block); err == nil {
		if offset > 0 {
			for index := range block.Start {
				block.Start[index] += offset
			}
			for index := range block.End {
				block.End[index] += offset
			}
		}
		timings := protocol.TimingSpansFromSeconds(block.Words, block.Start, block.End, protocol.TimingGranularityWord)
		if len(timings.Spans) > 0 {
			payload["granularity"], payload["spans"] = timings.Granularity, timings.Spans
		}
	}
	return marshalData(payload)
}

func (s *stream) warningData(contextID, messageType string) json.RawMessage {
	return marshalData(map[string]any{"context_id": contextID, "provider_type": messageType, "provider_request_id": s.currentProviderRequestID()})
}

func usageData(requestID string) json.RawMessage {
	return marshalData(map[string]any{"provider_request_id": requestID})
}

func marshalData(value any) json.RawMessage {
	payload, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{"code":"internal"}`)
	}
	return payload
}

// isUnsupportedLanguage recognizes Cartesia's explicit language refusal, a 400
// error frame reading "Your request was invalid: unsupported language 'sw'. See
// …/supported-locales …" (verified live 2026-10-09). The adapter's API version
// carries it in `message`; older versions put it in `error`, so both are read.
func isUnsupportedLanguage(text string) bool {
	return strings.Contains(strings.ToLower(text), "unsupported language")
}

// errorText reads the error frame's `error` field when it is a plain string.
func errorText(raw json.RawMessage) string {
	var text string
	if json.Unmarshal(raw, &text) != nil {
		return ""
	}
	return text
}

func cartesiaErrorMessage(message string) string {
	if strings.TrimSpace(message) == "" {
		return "Cartesia reported a streaming error"
	}
	return "Cartesia reported a streaming error: " + message
}

type voice struct {
	Mode string `json:"mode"`
	ID   string `json:"id"`
}

type outputFormat struct {
	Container  string `json:"container"`
	Encoding   string `json:"encoding"`
	SampleRate int    `json:"sample_rate"`
}

type generation struct {
	ModelID          string            `json:"model_id"`
	Transcript       string            `json:"transcript"`
	Voice            voice             `json:"voice"`
	Language         string            `json:"language"`
	Normalization    string            `json:"normalization,omitempty"`
	MaxBufferDelayMs *int              `json:"max_buffer_delay_ms,omitempty"`
	GenerationConfig *generationConfig `json:"generation_config,omitempty"`
	ContextID        string            `json:"context_id"`
	OutputFormat     outputFormat      `json:"output_format"`
	AddTimestamps    bool              `json:"add_timestamps"`
	Continue         bool              `json:"continue"`
}

// generationConfig is the request's generation_config object.
type generationConfig struct {
	Speed float64 `json:"speed"`
}

type inbound struct {
	Type           string          `json:"type"`
	Data           string          `json:"data"`
	Done           bool            `json:"done"`
	StatusCode     int             `json:"status_code"`
	ContextID      string          `json:"context_id"`
	Message        string          `json:"message"`
	Error          json.RawMessage `json:"error"`
	RequestID      string          `json:"request_id"`
	WordTimestamps json.RawMessage `json:"word_timestamps"`
}
