package xai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

// ---------------------------------------------------------------------------
// xAI streaming speech-to-text: wss://api.x.ai/v1/stt
// ---------------------------------------------------------------------------
//
// Every wire detail in this file was read from xAI's RAW documentation sources
// on 2026-08-07, not from a rendered or summarised page:
//
//   - https://docs.x.ai/developers/model-capabilities/audio/speech-to-text.md
//     (21,894 bytes) — the guide: query-parameter table, the
//     is_final/speech_final state matrix, client/server message list, and the
//     HTTP status table.
//   - https://docs.x.ai/stt-streaming.ws.json (15,041 bytes) — the machine
//     readable WebSocket contract: the authentication header, every query
//     parameter with its default, and a JSON schema plus a worked example for
//     each client and server message.
//
// Comments marked CONFIRMED restate one of those two files. Comments marked
// MEASURED come from Speko's own traffic and are labelled because the vendor
// does not document them. Where the documentation is simply silent the code
// says so rather than guessing.
//
// This is NOT xAI's Speech-to-Speech product. That is a separate API at
// wss://api.x.ai/v1/realtime with its own models and its own ephemeral
// client-secret flow; transcription shares neither. Conflating the two is what
// made the scope of that credential ambiguous — see sttBearerCredential.

const (
	// STTAdapterID is the identifier returned by an xAI STT session plan.
	STTAdapterID = "xai.stt.v1"

	// sttPath is shared by both transcription surfaces: the streaming upgrade
	// at wss://api.x.ai/v1/stt and the batch POST https://api.x.ai/v1/stt.
	// CONFIRMED in both raw sources.
	sttPath = "/v1/stt"

	// STTDefaultModel is a Speko catalog key, NOT an xAI model id.
	//
	// CONFIRMED (by absence): xAI's transcription API has no model parameter on
	// either surface — the batch multipart field list and the streaming query
	// parameter list both omit one — and the STT models page publishes no slug,
	// only per-hour prices. A signed plan still has to name a concrete,
	// non-"auto" model, so this string exists to satisfy that contract and to
	// key telemetry. It is deliberately never sent upstream, exactly like
	// DefaultModel on the TTS side.
	STTDefaultModel = "stt"

	// sttInterimResults pins xAI's `interim_results` query parameter on.
	//
	// CONFIRMED: it defaults to false, in which case the socket emits only
	// finalized results. The canonical protocol has a transcript.delta event
	// and realtime callers depend on it, so the adapter always opts in rather
	// than leaving the behaviour to an endpoint's default.
	sttInterimResults = "true"
)

// documentedSTTSampleRates is xAI's published sample_rate enum for the
// streaming transcription socket. CONFIRMED: identical list on both surfaces.
var documentedSTTSampleRates = map[int]struct{}{
	8_000: {}, 16_000: {}, 22_050: {}, 24_000: {}, 44_100: {}, 48_000: {},
}

// STTConfig controls bounded transport state for the transcription socket.
// Provider identity, model, language, and the access token all arrive in a
// signed session plan and are never configured here.
type STTConfig struct {
	AdapterID  string
	HTTPClient *http.Client
	// EventBuffer bounds the adapter-owned event channel.
	EventBuffer int
	// MaxMessageBytes bounds one inbound WebSocket message.
	MaxMessageBytes int64
	// MaxPendingAudioBytes bounds the audio held while waiting for xAI's
	// readiness ack. See sttStream.WriteAudio for why any is held at all.
	MaxPendingAudioBytes int
	AllowedEndpointHosts []string
	// AllowInsecureEndpoint permits ws:// for tests and local development.
	AllowInsecureEndpoint bool
}

// STTAdapter implements xAI's streaming transcription socket.
//
// Only the streaming surface is wired. xAI does publish a batch
// POST /v1/stt endpoint, but it is a multipart file upload capped at 500 MB —
// it takes a complete recording, not an incremental audio stream — so it
// cannot honour the runtime.ProviderStream contract the way the TTS unary
// surface can. A plan that asks for HTTP transport is rejected rather than
// silently buffering an entire call in memory.
type STTAdapter struct {
	id                   string
	httpClient           *http.Client
	eventBuffer          int
	maxMessageBytes      int64
	maxPendingAudioBytes int
	endpointPolicy       upstream.WebSocketPolicy
}

// NewSTT creates a bounded xAI transcription adapter.
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
	if config.MaxPendingAudioBytes == 0 {
		config.MaxPendingAudioBytes = 1 << 20
	}
	if config.EventBuffer < 1 {
		return nil, errors.New("xai stt event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("xai stt maximum message bytes must be positive")
	}
	if config.MaxPendingAudioBytes < 1 {
		return nil, errors.New("xai stt maximum pending audio bytes must be positive")
	}
	endpointPolicy, err := upstream.NewWebSocketPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &STTAdapter{
		id:                   config.AdapterID,
		httpClient:           config.HTTPClient,
		eventBuffer:          config.EventBuffer,
		maxMessageBytes:      config.MaxMessageBytes,
		maxPendingAudioBytes: config.MaxPendingAudioBytes,
		endpointPolicy:       endpointPolicy,
	}, nil
}

func (a *STTAdapter) ID() string { return a.id }

// Open validates the plan and dials xAI's transcription socket.
func (a *STTAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindSTT {
		return nil, fmt.Errorf("xai stt supports stt sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "xai" {
		return nil, fmt.Errorf("xai stt adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("xai stt requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("xai stt requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("xai stt media: %w", err)
	}
	credential, err := sttBearerCredential(request.Plan)
	if err != nil {
		return nil, err
	}
	endpoint, err := sttEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint, request.Plan.Route.Model, request.Options, *request.Media)
	if err != nil {
		return nil, err
	}

	headers := make(http.Header)
	headers.Set("Authorization", "Bearer "+credential)
	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{
		HTTPClient: httpClientOrDefault(a.httpClient),
		HTTPHeader: headers,
	})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		return nil, &runtimepkg.ProviderError{
			Code:           statusErrorCode(status),
			Message:        "xAI transcription connection could not be established",
			Retryable:      status == 0 || statusRetryable(status),
			ProviderStatus: status,
			Cause:          err,
		}
	}
	conn.SetReadLimit(a.maxMessageBytes)
	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &sttStream{
		conn:                 conn,
		ctx:                  streamCtx,
		cancel:               cancel,
		events:               make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		maxPendingAudioBytes: a.maxPendingAudioBytes,
	}
	go stream.readLoop()
	return stream, nil
}

// sttBearerCredential resolves the one access token xAI accepts for STT.
//
// CONFIRMED: stt-streaming.ws.json declares exactly one authentication
// mechanism for the socket — a required `Authorization` header of the form
// `Bearer <your xAI API key>`. The batch surface uses the identical header.
// There is no query token and no sec-websocket-protocol variant documented for
// transcription.
//
// CONFIRMED BY ABSENCE: xAI's ephemeral client secret
// (POST /v1/realtime/client_secrets) is NOT documented for STT. Its own page
// scopes it explicitly — "Use them when connecting to the Speech to Speech API
// from browsers or mobile apps" — and its request body binds a `session.model`
// drawn from the grok-voice-* family. Neither STT source mentions client
// secrets, ephemeral tokens, or /v1/realtime at all. So there is no
// STT-specific credential shape to model: a managed plan differs from a BYOK
// plan only in who owns the string, never in where it is placed.
//
// tts.go's resolver already encodes exactly that rule, so it is reused rather
// than restated — a second copy is a second place for the two surfaces to
// drift and invent a difference that xAI does not document. The wrap only
// re-labels which session failed. Contrast providers/cartesia/stt.go, where
// BYOK and managed genuinely take different channels.
func sttBearerCredential(plan protocol.SessionPlan) (string, error) {
	credential, err := bearerCredential(plan)
	if err != nil {
		return "", fmt.Errorf("xai stt: %w", err)
	}
	return credential, nil
}

// sttEndpoint builds the documented handshake URL.
//
// Only parameters this gateway can source from a signed plan are set. xAI also
// documents diarize, filler_words, multichannel, keyterm, smart_turn,
// smart_turn_timeout, and vad_threshold; each is left unset so the endpoint's
// documented default applies. Two of those omissions are load-bearing:
//
//   - `endpointing` (silence ms before speech_final, default 10, range 0-5000)
//     is NOT sent. Compare providers/deepgram/stt.go, which pins
//     endpointing=false because the framework owns turn detection. xAI's knob
//     is an integer with no documented "off" value, and 0 means "fire on any
//     VAD silence boundary" rather than "never fire", so there is nothing to
//     send that disables it. Inventing a value would be a guess.
//   - `keyterm` (transcription biasing, max 100 terms of 50 characters each)
//     is NOT sent because protocol.RequestOptions carries no key-term field.
//     The platform's TypeScript adapter forwards key terms; porting that needs
//     a protocol change, which this adapter must not make.
func sttEndpoint(policy upstream.WebSocketPolicy, rawEndpoint, model string, options protocol.RequestOptions, media protocol.MediaFormat) (string, error) {
	endpoint, err := policy.Parse(rawEndpoint)
	if err != nil {
		return "", fmt.Errorf("xai stt endpoint: %w", err)
	}
	if endpoint.Path != sttPath {
		return "", fmt.Errorf("xai stt endpoint path must be %s, got %q", sttPath, endpoint.Path)
	}
	// The model never reaches the wire (see STTDefaultModel), but a signed plan
	// must still commit to one so telemetry and billing have a stable key.
	if strings.TrimSpace(model) == "" || model == "auto" {
		return "", errors.New("xai stt requires a concrete model in the session plan")
	}
	encoding, err := sttEncoding(media.Encoding)
	if err != nil {
		return "", err
	}
	// CONFIRMED: multichannel transcription is supported, but it changes the
	// terminal contract — transcript.done is sent once PER CHANNEL, each with
	// its own channel_index, and transcript.partial gains a channel_index too.
	// The de-duplication in this adapter models a single utterance stream, so
	// multi-channel audio is refused rather than silently mis-stitched.
	if media.Channels != 1 {
		return "", fmt.Errorf("xai stt adapter transcribes mono audio, got %d channels", media.Channels)
	}
	if _, ok := documentedSTTSampleRates[media.SampleRateHz]; !ok {
		return "", fmt.Errorf("xai stt does not support sample rate %d", media.SampleRateHz)
	}

	query := endpoint.Query()
	query.Set("sample_rate", strconv.Itoa(media.SampleRateHz))
	query.Set("encoding", encoding)
	query.Set("interim_results", sttInterimResults)
	// Documented default is 1; pinned so two identical plans produce a
	// byte-identical handshake.
	query.Set("channels", strconv.Itoa(media.Channels))
	// language is forwarded VERBATIM, region subtag included.
	//
	// CONFIRMED: the parameter is optional and only gates Inverse Text
	// Normalization — rendering spoken numbers, currencies and units in written
	// form. The guide is explicit that recognition itself does not depend on it:
	// "The model transcribes speech in any of these languages regardless of the
	// `language` parameter."
	//
	// CONFIRMED: unlike xAI's TTS surface, which publishes region-qualified
	// locales (pt-BR/pt-PT, es-MX/es-ES, three Arabic locales), the STT
	// supported-language table lists 25 BASE codes only. The tag is still passed
	// through unmodified: truncating "pt-BR" to "pt" silently rewrites what the
	// caller asked for, buys nothing xAI documents, and would hide the day xAI
	// starts honouring regions here. The platform's TypeScript adapter does
	// truncate (`language.toLowerCase().split('-')[0]`); that is a defect, not a
	// convention to copy.
	if language := strings.TrimSpace(options.Language); language != "" {
		query.Set("language", language)
	}
	endpoint.RawQuery = query.Encode()
	return endpoint.String(), nil
}

// sttEncoding maps the portable media encoding onto xAI's enum.
func sttEncoding(encoding string) (string, error) {
	if encoding == pcmEncoding {
		// xAI documents `pcm` as "signed 16-bit little-endian (2 bytes/sample)",
		// which is exactly pcm_s16le. The wire value is spelled identically on
		// both audio surfaces — TTS `codec=pcm`, STT `encoding=pcm` — so tts.go's
		// constant is reused instead of duplicated.
		return pcmCodec, nil
	}
	// The canonical protocol also admits opus. xAI's STT encoding enum is
	// pcm|mulaw|alaw and has no opus member, and mulaw/alaw are not
	// representable in protocol.MediaFormat, so pcm is the only crossing.
	return "", fmt.Errorf("xai stt does not support media encoding %q", encoding)
}

// ---------------------------------------------------------------------------
// Stream
// ---------------------------------------------------------------------------

type sttStream struct {
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent

	maxPendingAudioBytes int

	writeMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	closing      atomic.Bool
	closeErr     error

	// sendMu serialises the "buffer or send" decision with the send itself, so
	// audio held before the readiness ack can never be overtaken by audio
	// written after it. It is always the outermost lock.
	sendMu       sync.Mutex
	ready        bool
	pendingAudio [][]byte
	pendingBytes int

	// stateMu guards the per-turn transcript state described on handlePartial.
	stateMu       sync.Mutex
	sessionID     string
	latestText    string
	turnStarted   bool
	finalizedTurn bool
}

func (s *sttStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

// WriteAudio sends one raw binary frame.
//
// CONFIRMED: audio is raw bytes in the negotiated encoding, never base64, and
// the guide is emphatic that the client must "Wait for `transcript.created`
// before sending audio — the server needs to initialize its ASR backend."
// Nothing in the runtime provides that gate: runtime.Engine emits session.ready
// as soon as Open returns, so a caller can legitimately start pushing audio
// before xAI has acknowledged the session. Frames that arrive early are
// therefore held here and flushed in order once the ack lands, rather than
// being handed to a backend that is documented as not yet listening — losing
// the front of an utterance is the most expensive thing an STT adapter can do.
func (s *sttStream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("xai stt audio is empty")
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if !s.ready {
		if s.closed.Load() || s.closing.Load() {
			return runtimepkg.ErrSessionClosed
		}
		if s.pendingBytes+len(audio) > s.maxPendingAudioBytes {
			return &runtimepkg.ProviderError{
				Code:      "provider_unavailable",
				Message:   "xAI did not acknowledge the transcription session before the buffered audio limit",
				Retryable: true,
			}
		}
		frame := make([]byte, len(audio))
		copy(frame, audio)
		s.pendingAudio = append(s.pendingAudio, frame)
		s.pendingBytes += len(frame)
		return nil
	}
	return s.write(ctx, websocket.MessageBinary, audio)
}

// releaseAudio marks the session ready and flushes anything held, in order.
func (s *sttStream) releaseAudio(ctx context.Context) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.ready {
		return nil
	}
	s.ready = true
	pending := s.pendingAudio
	s.pendingAudio, s.pendingBytes = nil, 0
	for _, frame := range pending {
		if err := s.write(ctx, websocket.MessageBinary, frame); err != nil {
			return err
		}
	}
	return nil
}

// CommitAudio forces the current utterance to finalize immediately.
//
// CONFIRMED: this is xAI's push-to-talk lever — "Force the current utterance to
// finalize as speech_final immediately, without waiting for VAD endpointing or
// Smart Turn. The session stays open so you can continue streaming audio." The
// schema's type enum accepts both "finalize" and "Finalize"; the capitalised
// spelling is the one both worked examples and the schema example use.
func (s *sttStream) CommitAudio(ctx context.Context) error {
	return s.writeJSON(ctx, map[string]string{"type": "Finalize"})
}

func (s *sttStream) AppendText(context.Context, string) error {
	return runtimepkg.ErrUnsupportedOperation
}

func (s *sttStream) CommitText(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// Cancel drops the session. xAI documents no cancel message for transcription
// and the caller has said it no longer wants the results, so waiting for a
// flush would only bill for a transcript nobody reads.
func (s *sttStream) Cancel(context.Context) error { return s.abort() }

func (s *sttStream) Abort(context.Context) error { return s.abort() }

// Close signals end of audio and lets xAI flush.
//
// CONFIRMED: audio.done makes the server "flush any remaining buffered audio,
// emit final transcript events, and send a transcript.done event. The
// connection closes after transcript.done." So Close does not tear the socket
// down; the read loop retires it when that terminal frame arrives.
func (s *sttStream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		s.closing.Store(true)
		// Audio still held for the readiness ack is flushed first: it is real
		// customer audio, and discarding it would silently truncate the turn.
		// Best effort — a failure here is reported by the audio.done write.
		_ = s.releaseAudio(ctx)
		if err := s.writeJSON(ctx, map[string]string{"type": "audio.done"}); err != nil {
			s.closeErr = err
			_ = s.abort()
		}
	})
	return s.closeErr
}

func (s *sttStream) abort() error {
	s.abortOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		if err := s.conn.CloseNow(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
	})
	return s.closeErr
}

func (s *sttStream) writeJSON(ctx context.Context, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return s.write(ctx, websocket.MessageText, payload)
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
		return &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "xAI transcription write failed",
			Retryable: true,
			Cause:     err,
		}
	}
	return nil
}

func (s *sttStream) readLoop() {
	salvage := true
	defer func() {
		// A socket that ends without transcript.done still holds the newest
		// cumulative text of an unfinished turn, and that text IS the utterance
		// (see handlePartial). Emitting it here is what keeps a dropped
		// connection from turning a good turn into an empty result.
		if salvage {
			s.flushPendingFinal()
		}
		s.closed.Store(true)
		s.cancel()
		close(s.events)
	}()
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && !s.closing.Load() && s.ctx.Err() == nil && !isNormalClose(err) {
				// Salvage before failing. An event carrying Err is terminal for
				// the attempt, so a transcript emitted after it would never be
				// seen — and the turn xAI already transcribed is not the
				// connection's fault.
				s.flushPendingFinal()
				salvage = false
				_ = s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{
					Code:      "provider_unavailable",
					Message:   "xAI transcription read failed",
					Retryable: true,
					Cause:     err,
				}})
			}
			return
		}
		// CONFIRMED: every server message is JSON text. Transcription is
		// input-only, so xAI never sends a binary frame on this socket.
		if messageType != websocket.MessageText {
			continue
		}
		done, handleErr := s.handleMessage(payload)
		if handleErr != nil {
			// xAI reported the failure itself. A half-finished transcript
			// emitted alongside it would be presented to the caller as a result.
			salvage = false
			_ = s.emit(runtimepkg.ProviderEvent{Err: handleErr})
			return
		}
		if done {
			_ = s.conn.Close(websocket.StatusNormalClosure, "")
			return
		}
	}
}

// sttInbound covers every documented server message. The four types share one
// struct because xAI's frames are flat and disjoint on `type`.
type sttInbound struct {
	Type string `json:"type"`
	// ID is present only on transcript.created: "Unique session identifier
	// (UUID)". It is the only correlation handle xAI gives a transcription
	// session, so every later event carries it as provider_request_id.
	ID          string  `json:"id"`
	Text        string  `json:"text"`
	IsFinal     bool    `json:"is_final"`
	SpeechFinal bool    `json:"speech_final"`
	Start       float64 `json:"start"`
	Duration    float64 `json:"duration"`
	Message     string  `json:"message"`
}

func (s *sttStream) handleMessage(payload []byte) (bool, error) {
	var message sttInbound
	if err := json.Unmarshal(payload, &message); err != nil {
		return false, &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "xAI sent malformed transcription JSON",
			Retryable: true,
			Cause:     err,
		}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))
	switch message.Type {
	case "transcript.created":
		// The readiness ack. Releasing held audio comes first so the backend
		// receives the front of the utterance before anything else happens.
		s.setSessionID(message.ID)
		if err := s.releaseAudio(s.ctx); err != nil {
			return false, err
		}
		return false, s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventUsageObserved,
			Data:       usageData(message.ID),
			Extensions: extension(raw),
		})
	case "transcript.partial":
		return false, s.handlePartial(message, raw)
	case "transcript.done":
		return true, s.handleDone(message, raw)
	case "error":
		// CONFIRMED: the error frame carries only `message` — no status, no
		// code — so there is nothing to classify on. The docs say most errors
		// (pipeline failures, stream timeouts) close the connection and only
		// client-message parse errors leave it open; since the adapter cannot
		// tell which it received, every in-band error terminates the attempt.
		// Handshake failures ARE classified, by HTTP status, in Open.
		return false, &runtimepkg.ProviderError{
			Code:       "provider_unavailable",
			Message:    sttErrorMessage(message.Message),
			Retryable:  false,
			Extensions: extension(raw),
		}
	default:
		// Surfaced rather than dropped so a frame type xAI adds later is visible.
		return false, s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventWarning,
			Data:       marshalData(map[string]any{"message": "ignored xAI transcription message type", "provider_type": message.Type}),
			Extensions: extension(raw),
		})
	}
}

// handlePartial maps xAI's rolling transcript.partial frames.
//
// CONFIRMED — two booleans encode three states:
//
//	is_final=false, speech_final=false   interim, "text may change"
//	is_final=true,  speech_final=false   chunk final, "text locked, ~3s of
//	                                     speech finalized"
//	is_final=true,  speech_final=true    utterance final, "speaker stopped,
//	                                     complete stitched utterance"
//
// The third row is the one that decides this function. xAI describes the
// utterance-final frame as the complete STITCHED utterance — it restates the
// chunk finals that preceded it — and transcript.done then restates that again.
//
// MEASURED — on Speko's own traffic the earlier frames are cumulative too: each
// transcript.partial.text is the whole transcript-so-far with corrections
// applied, not a delta. (The WebSocket schema calls partial text "Transcript
// text for this chunk", which does not match what the socket sends. Believe the
// socket.)
//
// So the rule is: never concatenate. The newest text replaces the previous one.
// A consumer that appends every is_final frame receives the utterance two or
// three times over — that is exactly the ~100% word error rate this provider
// produced on the platform's transcription path before the equivalent fix
// landed there. Only the utterance-final frame becomes transcript.final; chunk
// finals stay deltas, so there is exactly one canonical final per turn.
//
// Compare providers/assemblyai/stt.go handleTurn, which swallows the same class
// of duplicate final for a different vendor-specific reason.
func (s *sttStream) handlePartial(message sttInbound, raw json.RawMessage) error {
	text := strings.TrimSpace(message.Text)
	if text == "" {
		return nil
	}
	// xAI publishes no speech-start event, so the turn boundary is derived from
	// the first transcript of the turn, timestamped with the documented `start`
	// (seconds from stream start).
	if s.markTurnStarted() {
		if err := s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventSpeechStarted,
			Data:       marshalData(map[string]any{"audio_start_ms": sttMilliseconds(message.Start), "provider_request_id": s.currentSessionID()}),
			Extensions: extension(raw),
		}); err != nil {
			return err
		}
	}
	if !(message.IsFinal && message.SpeechFinal) {
		// Interim or chunk final. Both carry the transcript-so-far, so the
		// stored copy is REPLACED, never appended, and both leave the canonical
		// stream as deltas.
		s.setLatestText(text)
		return s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventTranscriptDelta,
			Data:       sttTranscriptData(text, message, s.currentSessionID()),
			Extensions: extension(raw),
		})
	}
	if err := s.emit(runtimepkg.ProviderEvent{
		Type:       protocol.EventTranscriptFinal,
		Data:       sttTranscriptData(text, message, s.currentSessionID()),
		Extensions: extension(raw),
	}); err != nil {
		return err
	}
	s.finalizeTurn()
	return s.emit(runtimepkg.ProviderEvent{
		Type:       protocol.EventSpeechEnded,
		Data:       marshalData(map[string]any{"audio_end_ms": sttMilliseconds(message.Start + message.Duration), "reason": "speech_final", "provider_request_id": s.currentSessionID()}),
		Extensions: extension(raw),
	})
}

// handleDone maps transcript.done, the last frame on the socket.
//
// CONFIRMED: it arrives after the client's audio.done, `duration` is always
// present, and the connection closes after it. Its own documented example
// carries text:"" with duration 6.43 — an empty final transcript is normal, and
// it is precisely what a session whose every turn already closed on
// speech_final looks like.
//
// Three cases:
//
//   - empty text, nothing pending  -> every utterance already finalized. Emit
//     nothing; re-emitting would double the last turn.
//   - empty text, text pending     -> chunk finals that never reached an
//     endpoint boundary. The pending cumulative text IS the utterance.
//   - non-empty text               -> xAI flushed something. Trust it.
//
// NOT VERIFIABLE from the documentation: whether a non-empty done covers the
// whole session or only the un-finalized remainder. This adapter suppresses it
// only when a speech_final already closed the turn and nothing new followed —
// the same rule the platform's TypeScript adapter shipped after live
// verification against the gateway (0/50 clips doubled), rather than the
// offline re-score that let an earlier attempt pass while still doubling.
func (s *sttStream) handleDone(message sttInbound, raw json.RawMessage) error {
	text := strings.TrimSpace(message.Text)
	pending, finalized := s.consumeTurn()
	switch {
	case text == "":
		text = pending
	case finalized && pending == "":
		text = ""
	}
	if text != "" {
		if err := s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventTranscriptFinal,
			Data:       sttTranscriptData(text, message, s.currentSessionID()),
			Extensions: extension(raw),
		}); err != nil {
			return err
		}
		if err := s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventSpeechEnded,
			Data:       marshalData(map[string]any{"audio_end_ms": sttMilliseconds(message.Duration), "reason": "transcript_done", "provider_request_id": s.currentSessionID()}),
			Extensions: extension(raw),
		}); err != nil {
			return err
		}
	}
	// `duration` is the total audio the session processed, which is the only
	// metering quantity xAI reports for transcription.
	return s.emit(runtimepkg.ProviderEvent{
		Type:       protocol.EventUsageObserved,
		Data:       marshalData(map[string]any{"provider_request_id": s.currentSessionID(), "audio_duration_ms": sttMilliseconds(message.Duration)}),
		Extensions: extension(raw),
	})
}

// flushPendingFinal emits an unfinished turn's newest cumulative text.
func (s *sttStream) flushPendingFinal() {
	pending, _ := s.consumeTurn()
	if pending == "" {
		return
	}
	_ = s.emit(runtimepkg.ProviderEvent{
		Type: protocol.EventTranscriptFinal,
		Data: marshalData(map[string]any{
			"text": pending, "is_final": true, "speech_final": false,
			"reason": "stream_ended", "provider_request_id": s.currentSessionID(),
		}),
	})
}

func (s *sttStream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// ---------------------------------------------------------------------------
// Per-turn state
// ---------------------------------------------------------------------------

func (s *sttStream) setSessionID(value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	s.stateMu.Lock()
	s.sessionID = value
	s.stateMu.Unlock()
}

func (s *sttStream) currentSessionID() string {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.sessionID
}

func (s *sttStream) setLatestText(text string) {
	s.stateMu.Lock()
	s.latestText = text
	s.stateMu.Unlock()
}

func (s *sttStream) markTurnStarted() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.turnStarted {
		return false
	}
	s.turnStarted = true
	return true
}

// finalizeTurn records that a speech_final closed the turn and clears the
// cumulative text, so a transcript.done that merely restates it is suppressed.
func (s *sttStream) finalizeTurn() {
	s.stateMu.Lock()
	s.latestText, s.turnStarted, s.finalizedTurn = "", false, true
	s.stateMu.Unlock()
}

// consumeTurn reports the text that would be lost if the session ended now,
// plus whether a speech_final already closed the turn, and resets both.
func (s *sttStream) consumeTurn() (string, bool) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	pending, finalized := s.latestText, s.finalizedTurn
	s.latestText, s.turnStarted, s.finalizedTurn = "", false, false
	return pending, finalized
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func sttTranscriptData(text string, message sttInbound, sessionID string) json.RawMessage {
	return marshalData(map[string]any{
		"text":                text,
		"is_final":            message.IsFinal,
		"speech_final":        message.SpeechFinal,
		"audio_start_ms":      sttMilliseconds(message.Start),
		"audio_end_ms":        sttMilliseconds(message.Start + message.Duration),
		"provider_request_id": sessionID,
	})
}

func sttMilliseconds(seconds float64) int64 { return int64(math.Round(seconds * 1_000)) }

func sttErrorMessage(message string) string {
	if strings.TrimSpace(message) == "" {
		return "xAI reported a transcription error"
	}
	return "xAI reported a transcription error: " + message
}
