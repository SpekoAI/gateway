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
// ElevenLabs v3.
//
// `multiContextEndpoint` refuses eleven_v3 because the multi-context
// text-to-speech socket genuinely cannot serve it — the vendor's own exclusion.
// v3 streams here instead, at a different path, with a different frame
// vocabulary, so this is a second adapter rather than a branch inside the first:
//
//   - the voice is REGISTERED IN A HANDSHAKE FRAME, not carried in the path.
//     `eleven_v3_conversational` accepts exactly one registered voice (the
//     broader `eleven_v3` accepts up to ten); a single-voice TTS session is one
//     either way, so this adapter registers exactly one and rejects a request
//     that names none.
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
	// DialogueAdapterID identifies the ElevenLabs v3 dialogue socket.
	DialogueAdapterID = "elevenlabs.dialogue.v1"
	dialogueEndpoint  = "/v1/text-to-dialogue/stream-input"
	// dialogueModelPrefix is the vendor's own gate: "model_id must start with
	// eleven_v3". Anything else belongs on the text-to-speech socket.
	dialogueModelPrefix = "eleven_v3"
	// dialogueIdleTimeout is the vendor's documented inactivity window. The
	// keep-alive fires comfortably inside it rather than at the edge, because a
	// frame that races the server's own timer buys nothing.
	dialogueIdleTimeout   = 20 * time.Second
	dialogueKeepAliveTick = 12 * time.Second
)

// DialogueAdapter serves eleven_v3* over the text-to-dialogue socket.
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
	return strings.HasPrefix(strings.TrimSpace(model), dialogueModelPrefix)
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
	endpoint, err := dialogueSocket(a.endpointPolicy, request.Plan.Route.Endpoint, request.Plan.Route.Model, *request.Media)
	if err != nil {
		return nil, err
	}
	headers := make(http.Header)
	// Same rule as the text-to-speech arm: a BYOK key and the relay's permanent
	// connector key both ride the header, and only a control-plane-minted
	// provider-direct token would ever ride the query string. The handshake
	// frame below carries the key too, because the dialogue socket documents
	// `xi_api_key` there; sending both is what the vendor's own sample does.
	headers.Set("xi-api-key", credential.Value)
	client := a.httpClient
	if client == nil {
		client = http.DefaultClient
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
	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &dialogueStream{conn: conn, ctx: streamCtx, cancel: cancel, voiceID: voiceID, events: make(chan runtimepkg.ProviderEvent, a.eventBuffer)}
	// The handshake registers the voice and must land before any input frame,
	// so it is written synchronously on the caller's context: a session that
	// cannot register a voice has not started, and reporting that as a dial
	// failure is more useful than surfacing it later as a read error.
	if err := stream.writeJSON(ctx, map[string]any{"voices": []string{voiceID}, "xi_api_key": credential.Value}); err != nil {
		_ = stream.abort()
		return nil, err
	}
	go stream.readLoop()
	go stream.keepAliveLoop()
	return stream, nil
}

// dialogueSocket validates the route and builds the socket URL. The path is
// fixed — unlike the text-to-speech socket there is no voice segment to append,
// because the voice is registered in the handshake instead.
func dialogueSocket(policy upstream.WebSocketPolicy, rawEndpoint, model string, media protocol.MediaFormat) (string, error) {
	endpoint, err := policy.Parse(rawEndpoint)
	if err != nil {
		return "", fmt.Errorf("elevenlabs dialogue endpoint: %w", err)
	}
	if strings.TrimSpace(model) == "" || model == "auto" {
		return "", errors.New("elevenlabs dialogue requires a concrete model")
	}
	if !ServesModel(model) {
		return "", fmt.Errorf("elevenlabs dialogue serves only %s* models, got %q", dialogueModelPrefix, model)
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
	endpoint.RawQuery = query.Encode()
	return endpoint.String(), nil
}

type dialogueStream struct {
	conn    *websocket.Conn
	ctx     context.Context
	cancel  context.CancelFunc
	voiceID string
	events  chan runtimepkg.ProviderEvent

	writeMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	closing      atomic.Bool
	closeErr     error

	lastWrite    atomic.Int64
	audioStarted atomic.Bool
	committed    atomic.Bool
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
	return s.writeJSON(ctx, map[string]any{
		"inputs": []map[string]any{{"text": text, "voice_id": s.voiceID, "new_turn": false}},
	})
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

// Cancel has no frame-level equivalent on this socket: there is no context to
// close, so stopping the current utterance means dropping the connection. That
// is the honest mapping — a caller cancelling a TTS turn wants the audio to
// stop, and a half-flushed dialogue socket cannot be rewound.
func (s *dialogueStream) Cancel(context.Context) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	return s.abort()
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
		if err := s.conn.CloseNow(); err != nil && s.closeErr == nil {
			s.closeErr = err
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
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if err := s.conn.Write(ctx, websocket.MessageText, payload); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "ElevenLabs dialogue write failed", Retryable: true, Cause: err}
	}
	s.lastWrite.Store(time.Now().UnixNano())
	return nil
}

// keepAliveLoop holds the socket open between turns. The vendor closes after
// dialogueIdleTimeout of inactivity and a voice agent idles longer than that
// waiting on a caller, so without this the SECOND turn of a call dials a dead
// connection. The frame synthesizes nothing.
func (s *dialogueStream) keepAliveLoop() {
	ticker := time.NewTicker(dialogueKeepAliveTick)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if s.closed.Load() || s.closing.Load() {
				return
			}
			last := s.lastWrite.Load()
			if last != 0 && time.Since(time.Unix(0, last)) < dialogueKeepAliveTick {
				continue
			}
			if err := s.writeJSON(s.ctx, map[string]any{"keep_alive": true}); err != nil {
				return
			}
		}
	}
}

func (s *dialogueStream) readLoop() {
	defer func() { s.cancel(); close(s.events) }()
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && s.ctx.Err() == nil && !isNormalClose(err) {
				_ = s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "ElevenLabs dialogue read failed", Retryable: true, Cause: err}})
			}
			return
		}
		if messageType != websocket.MessageText {
			continue
		}
		if err := s.handleMessage(payload); err != nil {
			_ = s.emit(runtimepkg.ProviderEvent{Err: err})
			return
		}
	}
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

func (s *dialogueStream) handleMessage(payload []byte) error {
	var message dialogueInbound
	if err := json.Unmarshal(payload, &message); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "ElevenLabs dialogue sent malformed streaming JSON", Retryable: true, Cause: err}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))
	if len(message.Error) > 0 && string(message.Error) != "null" {
		// Carry the vendor's own text. The first version of this adapter
		// discarded it, and a live probe then reported only "reported a
		// streaming error" with no way to tell a bad voice from a bad model
		// from an exhausted quota. The error field is the vendor's own
		// message, never the credential, which rides the handshake and the
		// request header.
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: fmt.Sprintf("ElevenLabs dialogue reported a streaming error: %s", strings.TrimSpace(string(message.Error))), Retryable: false}
	}
	if message.Audio != "" {
		audio, err := base64.StdEncoding.DecodeString(message.Audio)
		if err != nil {
			return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "ElevenLabs dialogue sent invalid base64 audio", Retryable: true, Cause: err}
		}
		if s.audioStarted.CompareAndSwap(false, true) {
			if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: contextData(""), Extensions: extension(raw)}); err != nil {
				return err
			}
		}
		if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Data: contextData(""), Extensions: extension(raw), Audio: audio}); err != nil {
			return err
		}
	}
	if len(message.Alignment) > 0 && string(message.Alignment) != "null" {
		// No spans: the dialogue socket's alignment shape is not the character
		// arrays TimingSpansFromMillisecondDurations reads, and inventing spans
		// from a shape we have not measured is the desync this package's
		// text-to-speech alignment comment warns about.
		if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAlignment, Data: contextData(""), Extensions: extension(raw)}); err != nil {
			return err
		}
	}
	if message.finishesTurn() {
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: contextData(""), Extensions: extension(raw)})
	}
	if message.Audio == "" && len(message.Alignment) == 0 {
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: marshalData(map[string]any{"message": "ignored ElevenLabs dialogue message"}), Extensions: extension(raw)})
	}
	return nil
}

func (s *dialogueStream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}
