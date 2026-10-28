package elevenlabs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

// The Text-to-Dialogue socket, which is the ONLY streaming surface that serves
// ElevenLabs v3 and v4.
//
// `multiContextEndpoint` refuses both families because the multi-context
// text-to-speech socket genuinely cannot serve them — the vendor's own
// exclusion: it answers the handshake with HTTP 400 `unsupported_model`, "Use
// the text-to-dialogue websocket endpoint instead" (verified live 2026-09-28
// for eleven_v3_conversational, eleven_v4 and eleven_v4_turbo). They stream
// here instead, at a different path, with a different frame vocabulary, so this
// is a second adapter rather than a branch inside the first:
//
//   - the voice is REGISTERED IN A HANDSHAKE FRAME, not carried in the path.
//     `eleven_v3_conversational` and `eleven_v4_turbo` accept exactly one
//     registered voice (the broader `eleven_v3` accepts up to ten); a
//     single-voice TTS session is one either way, so this adapter registers
//     exactly one and rejects a request that names none.
//   - text arrives as `inputs`, a list of turn objects, not as a bare `text`
//     field, and turns are marked with `new_turn` rather than opened and closed
//     as numbered contexts. A gateway TTS session is one speaker saying one
//     thing, so every frame this adapter writes carries `new_turn: false` and
//     the whole session is one turn.
//   - there are no context ids at all, so the stream is single-context and the
//     `context_id` this package reports alongside every event is empty here.
//     That is deliberate: the field stays in the payload shape so a consumer
//     reading either ElevenLabs adapter parses one event schema.
//   - the socket drops after 20 SECONDS of inactivity, which a voice agent
//     reaches between turns routinely, so this adapter keeps it alive itself.
//     The text-to-speech socket needs nothing equivalent.
//
// Two vendor parameters the text-to-speech path sets are deliberately NOT set
// here, because the dialogue endpoint does not document them and an undocumented
// query parameter is a 400 risk on a route that has already reserved credit:
// `sync_alignment` (so alignment arrives only if the vendor volunteers it, and
// this adapter forwards it when it does) and `language_code` (a request-body
// field on the dialogue API, not a socket parameter — v3 infers the language).
const (
	// DialogueAdapterID identifies the ElevenLabs text-to-dialogue socket.
	DialogueAdapterID = "elevenlabs.dialogue.v1"
	dialogueEndpoint  = "/v1/text-to-dialogue/stream-input"
	// dialogueIdleTimeout is the vendor's documented inactivity window. The
	// keep-alive fires comfortably inside it rather than at the edge, because a
	// frame that races the server's own timer buys nothing.
	dialogueIdleTimeout   = 20 * time.Second
	dialogueKeepAliveTick = 12 * time.Second
	// dialogueKeepAlivePoll is how often the loop checks for idleness. Ticking
	// at dialogueKeepAliveTick itself and skipping recent writes let the gap
	// reach almost twice the tick (a write just after one tick skips the
	// next), past the vendor's window; polling bounds it at tick + poll.
	dialogueKeepAlivePoll = 2 * time.Second
)

// dialogueModelPrefixes are the model families the dialogue socket serves and
// the text-to-speech socket refuses. The dialogue socket answers any other
// model (eleven_flash_v2_5, eleven_multilingual_v2, ...) with HTTP 403, so the
// split is exhaustive in both directions.
var dialogueModelPrefixes = []string{"eleven_v3", "eleven_v4"}

// DialogueAdapter serves eleven_v3* and eleven_v4* over the text-to-dialogue
// socket.
type DialogueAdapter struct {
	id              string
	httpClient      *http.Client
	eventBuffer     int
	maxMessageBytes int64
	endpointPolicy  upstream.WebSocketPolicy
}

// NewDialogue builds the v3 adapter. It takes the same Config as New so one
// connector can construct both arms from one credential block.
func NewDialogue(config Config) (*DialogueAdapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = DialogueAdapterID
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = 32
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = 1 << 20
	}
	if config.EventBuffer < 1 || config.MaxMessageBytes < 1 {
		return nil, errors.New("elevenlabs dialogue buffers must be positive")
	}
	endpointPolicy, err := upstream.NewWebSocketPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &DialogueAdapter{id: config.AdapterID, httpClient: config.HTTPClient, eventBuffer: config.EventBuffer, maxMessageBytes: config.MaxMessageBytes, endpointPolicy: endpointPolicy}, nil
}

func (a *DialogueAdapter) ID() string { return a.id }

// ServesModel reports whether a model belongs on this socket. The connector
// that carries both ElevenLabs TTS arms dispatches on it, so the split lives in
// one place instead of being restated at every call site.
func ServesModel(model string) bool {
	model = strings.TrimSpace(model)
	for _, prefix := range dialogueModelPrefixes {
		if strings.HasPrefix(model, prefix) {
			return true
		}
	}
	return false
}

func (a *DialogueAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindTTS {
		return nil, fmt.Errorf("elevenlabs dialogue supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "elevenlabs" || request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, errors.New("elevenlabs dialogue requires an ElevenLabs websocket route")
	}
	if request.Media == nil {
		return nil, errors.New("elevenlabs dialogue requires media configuration")
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("elevenlabs dialogue requires a bearer credential")
	}
	voiceID := strings.TrimSpace(request.Options.Voice)
	if voiceID == "" {
		return nil, errors.New("elevenlabs dialogue requires a voice id")
	}
	voiceSettings, err := ttsVoiceSettings(request.Plan.Route.Model, request.Options.TTS.Provider("elevenlabs"))
	if err != nil {
		return nil, err
	}
	// Same rule as the text-to-speech arm: a BYOK key and the relay's permanent
	// connector key both ride the header, and a control-plane-minted
	// provider-direct token rides the `single_use_token` query parameter. The
	// vendor refuses a token in the header (1008 "Invalid API key", verified
	// live 2026-10-28), and only a `ttd_websocket` token opens this socket: a
	// `tts_websocket` one is refused as "Token type mismatch".
	singleUseToken := ""
	if request.Plan.Execution.CredentialSource == protocol.CredentialsManaged && request.Plan.Execution.ProviderRoute != protocol.RouteSpekoRelay {
		singleUseToken = credential.Value
	}
	endpoint, err := dialogueSocket(a.endpointPolicy, request.Plan.Route.Endpoint, request.Plan.Route.Model, *request.Media, singleUseToken)
	if err != nil {
		return nil, err
	}
	client := a.httpClient
	if client == nil {
		client = http.DefaultClient
	}
	dial := func(ctx context.Context, credential string) (*websocket.Conn, error) {
		return a.dialDialogue(ctx, client, endpoint, credential, singleUseToken != "", voiceID, voiceSettings)
	}
	conn, err := dial(ctx, credential.Value)
	if err != nil {
		return nil, err
	}
	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &dialogueStream{ctx: streamCtx, cancel: cancel, voiceID: voiceID, events: make(chan runtimepkg.ProviderEvent, a.eventBuffer), inbox: make(chan dialogueItem, a.eventBuffer), conn: conn, superseded: make(chan struct{})}
	// There is no frame that stops a dialogue turn, so Cancel has to drop the
	// socket and the session continues on a new one. A permanent key (BYOK,
	// relay) simply dials again. A managed single-use token is spent by the
	// first connection (a second one is refused "Token not found or has
	// expired", verified live 2026-10-28), so the new socket needs a fresh
	// token from the control plane; without a refresher Cancel ends the
	// session, as it always did.
	switch {
	case singleUseToken == "":
		permanent := credential.Value
		stream.redial = func(ctx context.Context) (*websocket.Conn, error) { return dial(ctx, permanent) }
	case request.RefreshCredential != nil:
		refresh := request.RefreshCredential
		stream.redial = func(ctx context.Context) (*websocket.Conn, error) {
			fresh, err := refresh(ctx)
			if err != nil {
				return nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "ElevenLabs dialogue could not obtain a credential to reconnect", Retryable: true, Cause: err}
			}
			if !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, fresh.Kind) || strings.TrimSpace(fresh.Value) == "" {
				return nil, errors.New("elevenlabs dialogue reconnect requires a bearer credential")
			}
			freshEndpoint, err := dialogueSocket(a.endpointPolicy, request.Plan.Route.Endpoint, request.Plan.Route.Model, *request.Media, fresh.Value)
			if err != nil {
				return nil, err
			}
			return a.dialDialogue(ctx, client, freshEndpoint, fresh.Value, true, voiceID, voiceSettings)
		}
	}
	stream.lastWrite.Store(time.Now().UnixNano())
	go stream.pump()
	go stream.readLoop(conn, 0)
	go stream.keepAliveLoop()
	return stream, nil
}

// dialDialogue opens one socket and registers the voice on it. The handshake
// must land before any input frame, so it is written synchronously on the
// caller's context: a connection that cannot register a voice has not started,
// and reporting that as a dial failure is more useful than surfacing it later
// as a read error.
func (a *DialogueAdapter) dialDialogue(ctx context.Context, client *http.Client, endpoint, credential string, singleUseToken bool, voiceID string, voiceSettings map[string]any) (*websocket.Conn, error) {
	headers := make(http.Header)
	if !singleUseToken {
		headers.Set("xi-api-key", credential)
	}
	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: client, HTTPHeader: headers})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		return nil, &runtimepkg.ProviderError{Code: dialErrorCode(status), Message: "ElevenLabs dialogue connection could not be established", Retryable: status == 0 || status == http.StatusTooManyRequests || status >= 500, ProviderStatus: status, Cause: err}
	}
	conn.SetReadLimit(a.maxMessageBytes)
	// The handshake frame carries a permanent key too, because the dialogue
	// socket documents `xi_api_key` there and the vendor's own sample sends
	// both. A single-use token stays in the query alone.
	handshake := map[string]any{"voices": []string{voiceID}}
	if !singleUseToken {
		handshake["xi_api_key"] = credential
	}
	// The dialogue socket takes voice_settings once, in the handshake, for
	// the whole connection; ttsVoiceSettings has already narrowed it to
	// stability.
	if voiceSettings != nil {
		handshake["voice_settings"] = voiceSettings
	}
	payload, err := json.Marshal(handshake)
	if err == nil {
		err = conn.Write(ctx, websocket.MessageText, payload)
	}
	if err != nil {
		_ = conn.CloseNow()
		return nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "ElevenLabs dialogue write failed", Retryable: true, Cause: err}
	}
	return conn, nil
}

// dialogueSocket validates the route and builds the socket URL. The path is
// fixed — unlike the text-to-speech socket there is no voice segment to append,
// because the voice is registered in the handshake instead.
func dialogueSocket(policy upstream.WebSocketPolicy, rawEndpoint, model string, media protocol.MediaFormat, singleUseToken string) (string, error) {
	endpoint, err := policy.Parse(rawEndpoint)
	if err != nil {
		return "", fmt.Errorf("elevenlabs dialogue endpoint: %w", err)
	}
	if strings.TrimSpace(model) == "" || model == "auto" {
		return "", errors.New("elevenlabs dialogue requires a concrete model")
	}
	if !ServesModel(model) {
		return "", fmt.Errorf("elevenlabs dialogue serves only %s* models, got %q", strings.Join(dialogueModelPrefixes, "*, "), model)
	}
	if path := strings.TrimRight(endpoint.Path, "/"); path != dialogueEndpoint {
		return "", fmt.Errorf("elevenlabs dialogue endpoint path must be %s, got %q", dialogueEndpoint, path)
	}
	if media.Encoding != "pcm_s16le" || media.Channels != 1 {
		return "", errors.New("elevenlabs dialogue output requires mono pcm_s16le")
	}
	// The dialogue endpoint's pcm set is the text-to-speech set plus 32 kHz.
	// 44.1 kHz is listed as Pro-tier-only upstream; it stays accepted here so a
	// subscribed key is not refused locally, and an unsubscribed one gets the
	// vendor's own error rather than a guess about the account.
	switch media.SampleRateHz {
	case 8_000, 16_000, 22_050, 24_000, 32_000, 44_100, 48_000:
	default:
		return "", fmt.Errorf("elevenlabs dialogue does not support pcm output at %d Hz", media.SampleRateHz)
	}
	query := endpoint.Query()
	query.Set("model_id", model)
	query.Set("output_format", "pcm_"+strconv.Itoa(media.SampleRateHz))
	if singleUseToken != "" {
		query.Set("single_use_token", singleUseToken)
	}
	endpoint.RawQuery = query.Encode()
	return endpoint.String(), nil
}

// dialogueReconnectTimeout bounds the redial after a Cancel, credential mint
// included. The caller's next utterance waits on it, so it is kept short.
const dialogueReconnectTimeout = 10 * time.Second

// errSuperseded reports a write on a connection a Cancel has replaced.
var errSuperseded = errors.New("elevenlabs dialogue connection superseded")

// dialogueItem is what a connection's reader hands the pump: one vendor frame,
// or how that connection ended.
type dialogueItem struct {
	generation uint64
	payload    []byte
	err        error // the connection failed
	ended      bool  // the connection closed normally
}

// dialogueStream serves one TTS session across successive sockets. Readers
// only read; every turn-state change and every event goes through ONE pump
// goroutine, which applies an item only if it comes from the current
// connection generation. That single path is what keeps a Cancel clean: a
// reader of a replaced socket cannot touch the replacement's turn state, end
// its session, or interleave a frame into its audio, because the pump checks
// the generation and mutates state under the same lock, and it alone sends.
type dialogueStream struct {
	ctx     context.Context
	cancel  context.CancelFunc
	voiceID string
	events  chan runtimepkg.ProviderEvent
	inbox   chan dialogueItem
	// redial opens a replacement connection, handshake written. Nil when the
	// credential is single-use and nothing can mint another.
	redial func(context.Context) (*websocket.Conn, error)

	// mu guards the connection and turn state. It is never held across a send
	// on events or inbox, so a consumer that stops reading cannot wedge
	// Cancel or Close.
	mu           sync.RWMutex
	conn         *websocket.Conn // nil while a reconnect is in flight
	generation   uint64
	superseded   chan struct{} // closed when Cancel replaces this generation
	reconnected  chan struct{} // closed when the in-flight reconnect settles
	reconnectErr error
	audioStarted bool
	// speaking is set by the first append of a turn and cleared when the turn
	// finishes or is cancelled: a Cancel with nothing in flight has nothing to
	// stop and keeps the connection.
	speaking bool

	writeMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	closing      atomic.Bool
	closeErr     error

	lastWrite atomic.Int64
	committed atomic.Bool
}

func (s *dialogueStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }
func (s *dialogueStream) WriteAudio(context.Context, []byte) error {
	return runtimepkg.ErrUnsupportedOperation
}
func (s *dialogueStream) CommitAudio(context.Context) error {
	return runtimepkg.ErrUnsupportedOperation
}

// AppendText writes one dialogue input. Every frame is the same speaker
// continuing the same turn: a gateway TTS session is one utterance, and
// `new_turn: true` would make the vendor treat the next sentence as a second
// speaker's line and re-plan its prosody mid-sentence.
func (s *dialogueStream) AppendText(ctx context.Context, text string) error {
	if text == "" {
		return errors.New("elevenlabs dialogue text is empty")
	}
	if s.closed.Load() || s.closing.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if s.committed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if err := s.writeJSON(ctx, map[string]any{
		"inputs": []map[string]any{{"text": text, "voice_id": s.voiceID, "new_turn": false}},
	}); err != nil {
		return err
	}
	s.mu.Lock()
	s.speaking = true
	s.mu.Unlock()
	return nil
}

// CommitText flushes. The dialogue socket buffers to a fixed server-side
// character-and-word threshold before it emits the first partial, so a short
// line — most of what an agent says — would otherwise sit unspoken until the
// socket closed. `flush` is the vendor's documented way to force synthesis
// without ending the connection.
func (s *dialogueStream) CommitText(ctx context.Context) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if !s.committed.CompareAndSwap(false, true) {
		return runtimepkg.ErrSessionClosed
	}
	if err := s.writeJSON(ctx, map[string]any{"flush": true}); err != nil {
		s.committed.Store(false)
		return err
	}
	return nil
}

// Cancel stops the current utterance and keeps the session. The socket has no
// frame that stops a turn (the vendor documents only flush, keep_alive and
// close_socket), so the connection carrying it is dropped — which stops the
// audio and the vendor's generation at once — and a replacement is dialled in
// the background so the next utterance does not pay for the handshake. Draining
// the turn instead would hold the next utterance until the vendor finished
// generating the cancelled one (measured ~3.7s for 450 characters) and bill it.
//
// One frame the pump was already sending when Cancel landed can still reach
// the session after Cancel returns (the text-to-speech arm has the same window
// between reading a frame and closing its context). It always precedes the
// replacement's first event: the pump sends in order.
func (s *dialogueStream) Cancel(context.Context) error {
	if s.closed.Load() || s.closing.Load() {
		return runtimepkg.ErrSessionClosed
	}
	s.mu.Lock()
	if !s.speaking {
		s.mu.Unlock()
		return nil
	}
	if s.redial == nil {
		s.mu.Unlock()
		return s.abort()
	}
	old := s.conn
	s.conn = nil
	close(s.superseded)
	s.superseded = make(chan struct{})
	s.generation++
	generation := s.generation
	done := make(chan struct{})
	s.reconnected, s.reconnectErr = done, nil
	s.speaking, s.audioStarted = false, false
	s.committed.Store(false)
	s.mu.Unlock()
	if old != nil {
		_ = old.CloseNow()
	}
	go s.reconnect(generation, done)
	return nil
}

func (s *dialogueStream) reconnect(generation uint64, done chan struct{}) {
	ctx, cancel := context.WithTimeout(s.ctx, dialogueReconnectTimeout)
	defer cancel()
	conn, err := s.redial(ctx)
	s.mu.Lock()
	if err == nil && (s.generation != generation || s.ctx.Err() != nil) {
		_ = conn.CloseNow()
		conn, err = nil, runtimepkg.ErrSessionClosed
	}
	if err == nil {
		s.conn = conn
		s.lastWrite.Store(time.Now().UnixNano())
	}
	s.reconnectErr = err
	close(done)
	s.mu.Unlock()
	if err != nil {
		if !s.closed.Load() {
			s.deliver(dialogueItem{generation: generation, err: err})
		}
		return
	}
	go s.readLoop(conn, generation)
}

// connection returns the live socket, waiting out an in-flight reconnect.
func (s *dialogueStream) connection(ctx context.Context) (*websocket.Conn, error) {
	s.mu.RLock()
	conn, pending := s.conn, s.reconnected
	s.mu.RUnlock()
	if conn != nil {
		return conn, nil
	}
	if pending == nil {
		return nil, runtimepkg.ErrSessionClosed
	}
	select {
	case <-pending:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	s.mu.RLock()
	conn, err := s.conn, s.reconnectErr
	s.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return nil, runtimepkg.ErrSessionClosed
	}
	return conn, nil
}

func (s *dialogueStream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		s.closing.Store(true)
		if err := s.writeJSON(ctx, map[string]any{"close_socket": true}); err != nil {
			s.closeErr = err
			_ = s.abort()
			return
		}
		s.closed.Store(true)
	})
	return s.closeErr
}

func (s *dialogueStream) Abort(context.Context) error { return s.abort() }

func (s *dialogueStream) abort() error {
	s.abortOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		s.mu.RLock()
		conn := s.conn
		s.mu.RUnlock()
		if conn != nil {
			if err := conn.CloseNow(); err != nil && s.closeErr == nil {
				s.closeErr = err
			}
		}
	})
	return s.closeErr
}

func (s *dialogueStream) writeJSON(ctx context.Context, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	conn, err := s.connection(ctx)
	if err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if err := conn.Write(ctx, websocket.MessageText, payload); err != nil {
		s.mu.RLock()
		replaced := s.conn != conn
		s.mu.RUnlock()
		if replaced {
			return errSuperseded
		}
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "ElevenLabs dialogue write failed", Retryable: true, Cause: err}
	}
	s.lastWrite.Store(time.Now().UnixNano())
	return nil
}

// keepAliveLoop holds the socket open between turns. The vendor closes after
// dialogueIdleTimeout of inactivity and a voice agent idles longer than that
// waiting on a caller, so without this the SECOND turn of a call dials a dead
// connection. The frame synthesizes nothing. It skips while a reconnect is in
// flight, and a write that lost a race with Cancel keeps the loop running for
// the replacement.
func (s *dialogueStream) keepAliveLoop() {
	ticker := time.NewTicker(dialogueKeepAlivePoll)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if s.closed.Load() || s.closing.Load() {
				return
			}
			s.mu.RLock()
			live := s.conn != nil
			s.mu.RUnlock()
			if !live || !keepAliveDue(s.lastWrite.Load(), time.Now()) {
				continue
			}
			if err := s.writeJSON(s.ctx, map[string]any{"keep_alive": true}); err != nil && !errors.Is(err, errSuperseded) {
				return
			}
		}
	}
}

// keepAliveDue reports whether the socket has been quiet for a full tick.
func keepAliveDue(lastWriteUnixNano int64, now time.Time) bool {
	return lastWriteUnixNano == 0 || now.Sub(time.Unix(0, lastWriteUnixNano)) >= dialogueKeepAliveTick
}

// readLoop reads one connection and hands everything to the pump. It decides
// nothing itself: whether a frame or a failure still matters depends on the
// generation, which only the pump checks.
func (s *dialogueStream) readLoop(conn *websocket.Conn, generation uint64) {
	for {
		messageType, payload, err := conn.Read(s.ctx)
		if err != nil {
			item := dialogueItem{generation: generation, ended: true}
			if !isNormalClose(err) && s.ctx.Err() == nil {
				item = dialogueItem{generation: generation, err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "ElevenLabs dialogue read failed", Retryable: true, Cause: err}}
			}
			s.deliver(item)
			return
		}
		if messageType != websocket.MessageText {
			continue
		}
		if !s.deliver(dialogueItem{generation: generation, payload: payload}) {
			return
		}
	}
}

// deliver queues an item for the pump. It gives up once the item's generation
// is replaced or the stream ends, so a reader never blocks on a dead pump.
func (s *dialogueStream) deliver(item dialogueItem) bool {
	s.mu.RLock()
	current, superseded := s.generation == item.generation, s.superseded
	s.mu.RUnlock()
	if !current {
		return false
	}
	select {
	case s.inbox <- item:
		return true
	case <-superseded:
		return false
	case <-s.ctx.Done():
		return false
	}
}

// pump is the only goroutine that changes turn state from vendor frames, sends
// on events, or closes it.
func (s *dialogueStream) pump() {
	defer close(s.events)
	defer s.cancel()
	for {
		var item dialogueItem
		select {
		case <-s.ctx.Done():
			return
		case item = <-s.inbox:
		}
		events, superseded, terminal := s.apply(item)
		for _, event := range events {
			select {
			case s.events <- event:
			case <-superseded:
				// Cancel replaced this generation mid-item: drop the rest.
			case <-s.ctx.Done():
				return
			}
		}
		if terminal {
			return
		}
	}
}

// apply turns one item into events, under the lock, if and only if it comes
// from the current generation. A stale item changes nothing.
func (s *dialogueStream) apply(item dialogueItem) ([]runtimepkg.ProviderEvent, chan struct{}, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if item.generation != s.generation {
		return nil, nil, false
	}
	switch {
	case item.ended:
		return nil, s.superseded, true
	case item.err != nil:
		if s.closed.Load() {
			return nil, s.superseded, true
		}
		return []runtimepkg.ProviderEvent{{Err: item.err}}, s.superseded, true
	}
	events, err := s.handleMessage(item.payload)
	if err != nil {
		return append(events, runtimepkg.ProviderEvent{Err: err}), s.superseded, true
	}
	return events, s.superseded, false
}

// dialogueInbound is the dialogue socket's own frame set. `alignment` is typed
// as raw JSON rather than as the text-to-speech `alignment` struct because the
// dialogue guide shows it as an ARRAY where text-to-speech sends an object —
// decoding it into the struct would fail the whole frame, taking the audio in
// it down with it, so it is forwarded verbatim in the extension instead.
type dialogueInbound struct {
	Audio string `json:"audio"`
	// The dialogue socket ends a turn with `is_final_audio_for_turn`, NOT the
	// text-to-speech socket's `is_final` — measured against the live endpoint
	// on 2026-09-08, where the frame arrived and the adapter did not know it.
	// Without this field a session emits audio and then never emits
	// audio.done, so a consumer waits out its own timeout on every turn. The
	// other two spellings are kept as fallbacks: `is_final` is what the
	// vendor's own websocket guide documents, so it may appear on some
	// deployments, and camelCase is what the text-to-speech socket sends.
	IsFinalForTurn bool            `json:"is_final_audio_for_turn"`
	IsFinal        bool            `json:"is_final"`
	IsFinalCamel   bool            `json:"isFinal"`
	Alignment      json.RawMessage `json:"alignment"`
	Error          json.RawMessage `json:"error"`
}

// finishesTurn reports whether the frame ends the utterance under any of the
// spellings the endpoint has been observed or documented to use.
func (m dialogueInbound) finishesTurn() bool {
	return m.IsFinalForTurn || m.IsFinal || m.IsFinalCamel
}

// handleMessage translates one vendor frame. It runs under s.mu, inside apply.
func (s *dialogueStream) handleMessage(payload []byte) ([]runtimepkg.ProviderEvent, error) {
	var message dialogueInbound
	if err := json.Unmarshal(payload, &message); err != nil {
		return nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "ElevenLabs dialogue sent malformed streaming JSON", Retryable: true, Cause: err}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))
	if len(message.Error) > 0 && string(message.Error) != "null" {
		// Carry the vendor's own text. The first version of this adapter
		// discarded it, and a live probe then reported only "reported a
		// streaming error" with no way to tell a bad voice from a bad model
		// from an exhausted quota. The error field is the vendor's own
		// message, never the credential, which rides the handshake and the
		// request header.
		return nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: fmt.Sprintf("ElevenLabs dialogue reported a streaming error: %s", strings.TrimSpace(string(message.Error))), Retryable: false}
	}
	var events []runtimepkg.ProviderEvent
	if message.Audio != "" {
		audio, err := base64.StdEncoding.DecodeString(message.Audio)
		if err != nil {
			return nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "ElevenLabs dialogue sent invalid base64 audio", Retryable: true, Cause: err}
		}
		if !s.audioStarted {
			s.audioStarted = true
			events = append(events, runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: contextData(""), Extensions: extension(raw)})
		}
		events = append(events, runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Data: contextData(""), Extensions: extension(raw), Audio: audio})
	}
	if len(message.Alignment) > 0 && string(message.Alignment) != "null" {
		// No spans: the dialogue socket's alignment shape is not the character
		// arrays TimingSpansFromMillisecondDurations reads, and inventing spans
		// from a shape we have not measured is the desync this package's
		// text-to-speech alignment comment warns about.
		events = append(events, runtimepkg.ProviderEvent{Type: protocol.EventAlignment, Data: contextData(""), Extensions: extension(raw)})
	}
	if message.finishesTurn() {
		// The turn is over, so the session takes the next one: the socket
		// accepts another inputs+flush round on the same connection (verified
		// live 2026-10-28 for eleven_v4_turbo and eleven_v3_conversational).
		// Reset BEFORE emitting, so a caller that appends as soon as it sees
		// audio.done is not refused. This mirrors finishContext on the
		// text-to-speech arm, where Pipecat commits sentence by sentence.
		s.audioStarted, s.speaking = false, false
		s.committed.Store(false)
		return append(events, runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: contextData(""), Extensions: extension(raw)}), nil
	}
	if message.Audio == "" && len(message.Alignment) == 0 {
		events = append(events, runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: marshalData(map[string]any{"message": "ignored ElevenLabs dialogue message"}), Extensions: extension(raw)})
	}
	return events, nil
}
