package assemblyai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

// AssemblyAI Universal-Streaming v3, spoken over the raw WebSocket protocol.
// Every wire detail below was checked against AssemblyAI's own documentation
// (assemblyai.com/docs, August 2026) rather than inherited from a working
// client, because this vendor IGNORES unrecognized query parameters instead of
// rejecting them: a misspelled parameter is a dead feature that no test can
// see. AssemblyAI documents that trap itself — "unrecognized or misspelled
// query parameters are ignored rather than rejected".
//
//	URL   : wss://streaming.assemblyai.com/v3/ws (plus the us/eu residency
//	        hosts). This is the REALTIME streaming socket, not the batch
//	        /v2/transcript upload-and-poll API: partial transcripts arrive
//	        while audio is still being written.
//	auth  : a permanent API key goes in an `Authorization` request header with
//	        NO "Bearer" prefix; a temporary streaming token goes in the `token`
//	        QUERY PARAMETER. The two are not interchangeable — see Open.
//	send  : raw binary PCM frames. AssemblyAI REJECTS any audio message outside
//	        50-1000 ms and closes the session (code 3007), so this adapter
//	        buffers to ~100 ms messages. See WriteAudio.
//	ctrl  : {"type":"ForceEndpoint"} finalizes the pending turn,
//	        {"type":"Terminate"} ends the session.
//	recv  : Begin, SpeechStarted, Turn, Termination, Heartbeat,
//	        SpeakerRevision, plus an Error frame carrying `error`/`error_code`.
const (
	// AdapterID is the identifier returned by an AssemblyAI STT session plan.
	AdapterID = "assemblyai.stt.v1"
	// DefaultModel is AssemblyAI's own documented default `speech_model`. The
	// adapter never applies it: a session plan must name a concrete model, and
	// this constant exists so the catalog and the vendor agree on one value.
	DefaultModel = "universal-3-5-pro"

	extensionID = "assemblyai.com/v3"

	// officialAPIHost is the global streaming host. The two residency hosts are
	// allowed alongside it because AssemblyAI documents them as first-class
	// endpoints for EU/US data-residency accounts, and a customer pinned to one
	// of them is not using a "custom" deployment.
	officialAPIHost = "streaming.assemblyai.com"
	usResidencyHost = "streaming.us.assemblyai.com"
	euResidencyHost = "streaming.eu.assemblyai.com"

	// endpointPath is the only streaming path. v2 (/v2/realtime/ws) is retired
	// and answers with close code 410.
	endpointPath = "/v3/ws"

	// AssemblyAI's documented audio-message bounds. A message shorter than
	// minAudioMS or longer than maxAudioMS closes the session with 3007
	// ("Input duration violation: <n> ms. Expected between 50 and 1000 ms").
	// The runtime hands this adapter whatever frame size its transport uses —
	// 10-20 ms for a live telephony leg — so batching is mandatory, not an
	// optimization. Sending 10 ms frames straight through kills every session
	// on the first write and the agent goes deaf while the socket looks fine.
	minAudioMS   = 50
	maxAudioMS   = 1000
	batchAudioMS = 100

	// AssemblyAI streaming accepts 8000-96000 Hz. The portable MediaFormat
	// allows up to 192000, so the upper bound has to be re-checked here.
	minSampleRateHz = 8_000
	maxSampleRateHz = 96_000

	bytesPerSample = 2
)

// AssemblyAI's documented WebSocket close codes, reused as the `error_code` of
// the Error frame the server sends before closing. They are NOT HTTP statuses.
const (
	closeUnauthorized   = 1008 // missing/invalid key, or an account-level block
	closeInternalError  = 1011
	closeDeprecatedV2   = 410
	closeServerError    = 3005 // "Session Cancelled: An error occurred"
	closeInvalidMessage = 3006 // bad JSON/type, or inactivity termination
	closeInputDuration  = 3007 // audio message outside 50-1000 ms, or too fast
	closeSessionExpired = 3008
	closeTooManySession = 3009 // concurrency limit
)

// streamingLanguages is AssemblyAI's documented `language_codes` vocabulary.
// An unlisted tag is refused locally instead of being sent: an unusable value
// on a recognized parameter is the same silent failure as a misspelled
// parameter name, except it degrades the transcript instead of doing nothing.
var streamingLanguages = map[string]struct{}{
	"en": {}, "es": {}, "fr": {}, "de": {}, "it": {}, "pt": {},
	"tr": {}, "nl": {}, "sv": {}, "no": {}, "da": {}, "fi": {},
	"hi": {}, "vi": {}, "ar": {}, "he": {}, "ja": {}, "zh": {},
}

// Config controls local transport limits. Credentials and provider selection
// always come from the signed session plan, never from this configuration.
type Config struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// Adapter implements AssemblyAI's Universal-Streaming v3 WebSocket API.
type Adapter struct {
	id              string
	httpClient      *http.Client
	eventBuffer     int
	maxMessageBytes int64
	endpointPolicy  upstream.WebSocketPolicy
}

// New creates an STT adapter with bounded provider-event buffering.
func New(config Config) (*Adapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = AdapterID
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = 32
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = 1 << 20
	}
	if config.EventBuffer < 1 {
		return nil, errors.New("assemblyai event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("assemblyai maximum message bytes must be positive")
	}
	allowed := append([]string{usResidencyHost, euResidencyHost}, config.AllowedEndpointHosts...)
	endpointPolicy, err := upstream.NewWebSocketPolicy(officialAPIHost, allowed, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &Adapter{
		id:              config.AdapterID,
		httpClient:      config.HTTPClient,
		eventBuffer:     config.EventBuffer,
		maxMessageBytes: config.MaxMessageBytes,
		endpointPolicy:  endpointPolicy,
	}, nil
}

func (a *Adapter) ID() string { return a.id }

// Open opens a provider-direct STT stream against Universal-Streaming v3.
func (a *Adapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindSTT {
		return nil, fmt.Errorf("assemblyai supports stt sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "assemblyai" {
		return nil, fmt.Errorf("assemblyai adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("assemblyai requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("assemblyai requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("assemblyai media: %w", err)
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("assemblyai requires a bearer credential")
	}
	endpoint, err := streamEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint, request.Plan.Route.Model, request.Options, *request.Media)
	if err != nil {
		return nil, err
	}
	// Two credentials, two channels, and they are NOT interchangeable.
	//
	// A BYOK session carries the customer's permanent API key. AssemblyAI takes
	// that in an `Authorization` header whose value is the bare key — no
	// "Bearer" prefix, which is unusual enough that it is worth stating twice.
	// It must never reach the query string, because a URL reaches access logs.
	//
	// A managed session carries a single-use temporary streaming token minted
	// by the control plane from GET https://streaming.assemblyai.com/v3/token.
	// AssemblyAI accepts that ONLY as the `token` query parameter; it documents
	// the header form for the API key alone. Putting the token in the header
	// fails authentication at the handshake, and no unit test that asserts what
	// our own code emitted would ever notice. Same split as ElevenLabs STT.
	//
	// A relay plan is the exception inside managed: it is managed for billing
	// purposes but carries the connector's permanent AssemblyAI key, which
	// belongs in the bare Authorization header exactly like a BYOK key. The
	// `token` query channel stays reserved for the short-lived tokens of
	// managed provider-direct routes.
	headers := make(http.Header)
	if request.Plan.Execution.ProviderRoute == protocol.RouteSpekoRelay {
		headers.Set("Authorization", credential.Value)
	} else if request.Plan.Execution.CredentialSource == protocol.CredentialsManaged {
		endpoint, err = endpointWithToken(endpoint, credential.Value)
		if err != nil {
			return nil, err
		}
	} else {
		headers.Set("Authorization", credential.Value)
	}
	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{
		HTTPClient: configHTTPClient(a.httpClient),
		HTTPHeader: headers,
	})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		return nil, &runtimepkg.ProviderError{
			Code:           dialErrorCode(status),
			Message:        "AssemblyAI streaming connection could not be established",
			Retryable:      status == 0 || status == http.StatusTooManyRequests || status >= 500,
			ProviderStatus: status,
			Cause:          err,
		}
	}
	conn.SetReadLimit(a.maxMessageBytes)
	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &stream{
		conn:            conn,
		ctx:             streamCtx,
		cancel:          cancel,
		events:          make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		minAudioBytes:   audioBytes(request.Media.SampleRateHz, minAudioMS),
		batchAudioBytes: audioBytes(request.Media.SampleRateHz, batchAudioMS),
	}
	go stream.readLoop()
	return stream, nil
}

func configHTTPClient(client *http.Client) *http.Client {
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
// Engine, no SessionPlan.Validate — labels the same permanent AssemblyAI key
// bearer. Both spellings carry a permanent key; the header-versus-query
// channel split keys off the route, not the kind, so nothing else on the
// relay arm changes.
func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

// streamEndpoint builds the v3 connect URL. Every parameter name here is
// spelled the way AssemblyAI's connection-parameter table spells it; see the
// per-parameter notes for the ones that are easy to get wrong.
func streamEndpoint(policy upstream.WebSocketPolicy, rawEndpoint, model string, options protocol.RequestOptions, media protocol.MediaFormat) (string, error) {
	endpoint, err := policy.Parse(rawEndpoint)
	if err != nil {
		return "", fmt.Errorf("assemblyai endpoint: %w", err)
	}
	if endpoint.Path != endpointPath {
		return "", fmt.Errorf("assemblyai endpoint path must be %s, got %q", endpointPath, endpoint.Path)
	}
	if strings.TrimSpace(model) == "" || model == "auto" {
		return "", errors.New("assemblyai requires a concrete model in the session plan")
	}
	if err := validateMedia(media); err != nil {
		return "", err
	}
	query := endpoint.Query()
	// `speech_model`, not `model`. AssemblyAI echoes the resolved model back on
	// the Begin frame precisely because a misspelling here silently downgrades
	// the session to the account default instead of failing.
	query.Set("speech_model", model)
	query.Set("encoding", media.Encoding)
	query.Set("sample_rate", strconv.Itoa(media.SampleRateHz))
	// Formatted finals: punctuation, casing, and number formatting. AssemblyAI
	// defaults this to FALSE, and the difference is not cosmetic — the platform
	// measured word error rate regress from 2.0% to 5.7% when it consumed the
	// unformatted final instead of the formatted one. Sent unconditionally: the
	// parameter table scopes it to the Universal-Streaming tiers, but an
	// inapplicable parameter is ignored, whereas omitting it where it IS honored
	// costs real accuracy. readLoop depends on the two-final sequence this turns
	// on; see handleTurn.
	query.Set("format_turns", "true")
	// Partial (non-final) turns are how a realtime consumer sees speech before
	// the turn commits. The documented default is already true; it is pinned
	// here so a vendor default flip cannot quietly turn this adapter into a
	// commit-only transcriber.
	query.Set("include_partial_turns", "true")
	if language := strings.TrimSpace(options.Language); language != "" {
		code, err := streamingLanguage(language)
		if err != nil {
			return "", err
		}
		// `language_codes` (plural, a JSON array) — there is no `language`
		// parameter on this API at all, so the singular name every other vendor
		// uses would be accepted-and-ignored here.
		encoded, err := json.Marshal([]string{code})
		if err != nil {
			return "", fmt.Errorf("assemblyai language: %w", err)
		}
		query.Set("language_codes", string(encoded))
	}
	// Deliberately NOT sent: a Speko reservation marker. Deepgram has an `extra`
	// passthrough parameter for that; AssemblyAI has none, so any invented
	// parameter would be silently dropped and the metering hint would be a lie.
	endpoint.RawQuery = query.Encode()
	return endpoint.String(), nil
}

func validateMedia(media protocol.MediaFormat) error {
	// AssemblyAI's own streaming audio requirements say PCM16 or mu-law, mono,
	// at the declared sample rate. The api-reference table additionally lists
	// compressed containers, but the two sources disagree and the portable
	// MediaFormat only ever offers pcm_s16le or opus — so the intersection that
	// is documented consistently is pcm_s16le, and opus is refused rather than
	// gambled on. Declaring an encoding the vendor does not decode does not
	// fail loudly; it produces a healthy-looking session full of garbage.
	if media.Encoding != "pcm_s16le" {
		return fmt.Errorf("assemblyai requires pcm_s16le, got %q", media.Encoding)
	}
	if media.Channels != 1 {
		return fmt.Errorf("assemblyai requires mono audio, got %d channels", media.Channels)
	}
	if media.SampleRateHz < minSampleRateHz || media.SampleRateHz > maxSampleRateHz {
		return fmt.Errorf("assemblyai sample rate must be between %d and %d Hz, got %d", minSampleRateHz, maxSampleRateHz, media.SampleRateHz)
	}
	return nil
}

// streamingLanguage reduces a portable tag to the primary subtag AssemblyAI
// accepts (`es`, never `es-419`) and refuses anything outside the documented
// set.
func streamingLanguage(language string) (string, error) {
	lowered := strings.ToLower(strings.TrimSpace(language))
	if index := strings.IndexAny(lowered, "-_"); index > 0 {
		lowered = lowered[:index]
	}
	if _, ok := streamingLanguages[lowered]; !ok {
		return "", fmt.Errorf("assemblyai streaming does not support language %q", language)
	}
	return lowered, nil
}

// endpointWithToken places a minted temporary streaming token in the query
// string. The credential reaches the URL, so this value must never be logged;
// the runtime already treats route endpoints as sensitive for exactly this
// reason. Only a temporary token may travel this way — never an API key.
func endpointWithToken(rawEndpoint, token string) (string, error) {
	endpoint, err := url.Parse(rawEndpoint)
	if err != nil {
		return "", errors.New("assemblyai endpoint could not be prepared")
	}
	query := endpoint.Query()
	query.Set("token", token)
	endpoint.RawQuery = query.Encode()
	return endpoint.String(), nil
}

// audioBytes converts a duration into a sample-aligned byte count, rounding UP
// so a computed "50 ms" buffer is never 49.9 ms at an awkward sample rate —
// AssemblyAI enforces the floor exactly and closes the session below it.
func audioBytes(sampleRateHz, milliseconds int) int {
	total := sampleRateHz*bytesPerSample*milliseconds + 999
	total /= 1_000
	if total%bytesPerSample != 0 {
		total += bytesPerSample - total%bytesPerSample
	}
	return total
}

type stream struct {
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent

	minAudioBytes   int
	batchAudioBytes int

	writeMu      sync.Mutex
	audioMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	closing      atomic.Bool
	closeErr     error

	// pending holds audio that has not yet reached AssemblyAI's 50 ms floor.
	pending []byte
	// turnPending is true while a turn has been seen whose FORMATTED final has
	// not arrived. Only then does teardown force the endpoint: forcing one with
	// nothing in flight just produces an empty turn.
	turnPending atomic.Bool

	// sessionID is written and read only by readLoop.
	sessionID string
}

func (s *stream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

// WriteAudio buffers PCM and forwards it in ~100 ms binary messages.
//
// This is the one place this adapter deliberately does not write through.
// AssemblyAI rejects any audio message outside 50-1000 ms and terminates the
// session, so a caller feeding 10-20 ms transport frames would kill the stream
// on its first write. Buffering to a batch keeps every message comfortably
// inside the window in both directions and costs at most ~100 ms of added
// latency. The runtime serializes writes for a session, and audioMu additionally
// guards the buffer against a concurrent teardown flush.
func (s *stream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("assemblyai audio is empty")
	}
	s.audioMu.Lock()
	s.pending = append(s.pending, audio...)
	batchCount := len(s.pending) / s.batchAudioBytes
	batches := make([][]byte, 0, batchCount)
	for index := 0; index < batchCount; index++ {
		batch := make([]byte, s.batchAudioBytes)
		copy(batch, s.pending[index*s.batchAudioBytes:])
		batches = append(batches, batch)
	}
	if batchCount > 0 {
		s.pending = append([]byte(nil), s.pending[batchCount*s.batchAudioBytes:]...)
	}
	s.audioMu.Unlock()
	for _, batch := range batches {
		if err := s.write(ctx, websocket.MessageBinary, batch); err != nil {
			return err
		}
	}
	return nil
}

// CommitAudio flushes the buffered tail and forces the pending turn to
// finalize. AssemblyAI has no "finalize" message: {"type":"ForceEndpoint"} is
// the documented way to make it commit a turn that its own endpointer has not
// closed yet, which is what a push-to-talk or benchmark caller is asking for.
func (s *stream) CommitAudio(ctx context.Context) error {
	if err := s.flushAudio(ctx); err != nil {
		return err
	}
	return s.writeJSON(ctx, map[string]string{"type": "ForceEndpoint"})
}

func (s *stream) AppendText(context.Context, string) error { return runtimepkg.ErrUnsupportedOperation }

func (s *stream) CommitText(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// Cancel discards the session. There is no cancel command in the protocol, and
// unlike Close this must NOT flush or force a final — the caller no longer
// wants the transcript.
func (s *stream) Cancel(context.Context) error { return s.abort() }

// Abort immediately tears down the socket after a terminal runtime failure.
func (s *stream) Abort(context.Context) error { return s.abort() }

// Close ends the session gracefully: flush the buffered tail, force a still-
// pending turn to commit, then Terminate.
//
// Order matters. Dropping the socket with a turn in flight loses the caller's
// last utterance, which is the single most visible failure a transcriber can
// have. The read loop stays live afterwards so the finals AssemblyAI emits in
// response still reach the consumer; it ends on the Termination frame. Nothing
// here blocks waiting for that reply — AssemblyAI does not reliably send one,
// and a teardown that waits is a teardown that hangs.
func (s *stream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		s.closing.Store(true)
		if err := s.flushAudio(ctx); err != nil {
			s.closeErr = err
		}
		if s.closeErr == nil && s.turnPending.Load() {
			if err := s.writeJSON(ctx, map[string]string{"type": "ForceEndpoint"}); err != nil {
				s.closeErr = err
			}
		}
		if s.closeErr == nil {
			if err := s.writeJSON(ctx, map[string]string{"type": "Terminate"}); err != nil {
				s.closeErr = err
			}
		}
		if s.closeErr != nil {
			_ = s.abort()
		}
	})
	return s.closeErr
}

func (s *stream) abort() error {
	s.abortOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		if err := s.conn.CloseNow(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
	})
	return s.closeErr
}

// flushAudio sends whatever is buffered, but only if it clears the 50 ms floor.
// A shorter remainder is dropped: sending it would close the whole session with
// an input-duration violation, which costs the caller every transcript still in
// flight in exchange for under 50 ms of trailing audio.
func (s *stream) flushAudio(ctx context.Context) error {
	s.audioMu.Lock()
	pending := s.pending
	s.pending = nil
	s.audioMu.Unlock()
	if len(pending) < s.minAudioBytes {
		return nil
	}
	return s.write(ctx, websocket.MessageBinary, pending)
}

func (s *stream) writeJSON(ctx context.Context, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return s.write(ctx, websocket.MessageText, payload)
}

func (s *stream) write(ctx context.Context, messageType websocket.MessageType, payload []byte) error {
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
			Message:   "AssemblyAI streaming write failed",
			Retryable: true,
			Cause:     err,
		}
	}
	return nil
}

func (s *stream) readLoop() {
	defer func() {
		s.closed.Store(true)
		s.cancel()
		close(s.events)
	}()
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && !s.closing.Load() && s.ctx.Err() == nil && !isNormalClose(err) {
				s.emit(runtimepkg.ProviderEvent{Err: readErrorFor(err)})
			}
			return
		}
		if messageType != websocket.MessageText {
			continue
		}
		done, err := s.handleMessage(payload)
		if err != nil {
			s.emit(runtimepkg.ProviderEvent{Err: err})
			return
		}
		if done {
			_ = s.conn.Close(websocket.StatusNormalClosure, "")
			return
		}
	}
}

func (s *stream) handleMessage(payload []byte) (bool, error) {
	var message inboundMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return false, &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "AssemblyAI sent malformed streaming JSON",
			Retryable: true,
			Cause:     err,
		}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))
	// The Error frame is matched on either signal. AssemblyAI documents the
	// frame as {"type":"Error","error_code":n,"error":"..."} but omits Error
	// from the server-message list in the API reference, and observed frames
	// have carried `error` without the type — so neither alone is trustworthy.
	if message.Type == "Error" || strings.TrimSpace(message.Error) != "" {
		code, retryable := errorClass(message.ErrorCode, message.Error)
		return false, &runtimepkg.ProviderError{
			Code:      code,
			Message:   errorMessage(message.Error),
			Retryable: retryable,
			// AssemblyAI's error_code is a WebSocket close code, not an HTTP
			// status. It is surfaced here because it is the only machine-readable
			// classification the vendor gives.
			ProviderStatus: message.ErrorCode,
			Extensions:     extension(raw),
		}
	}
	switch message.Type {
	case "Begin":
		// Server-confirmed session start. `id` is the only identifier the
		// session ever gets, so every later event is tagged with it.
		s.sessionID = message.ID
		return false, s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventUsageObserved,
			Data:       marshalData(map[string]any{"provider_request_id": message.ID, "expires_at_unix": message.ExpiresAt}),
			Extensions: extension(raw),
		})
	case "SpeechStarted":
		return false, s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventSpeechStarted,
			Data:       marshalData(map[string]any{"audio_start_ms": int64(math.Round(message.Timestamp)), "provider_request_id": s.sessionID}),
			Extensions: extension(raw),
		})
	case "Turn":
		return false, s.handleTurn(message, raw)
	case "Termination":
		// The session's own accounting, and the last frame on the socket.
		if err := s.emit(runtimepkg.ProviderEvent{
			Type: protocol.EventUsageObserved,
			Data: marshalData(map[string]any{
				"provider_request_id": s.sessionID,
				"audio_duration_ms":   milliseconds(message.AudioDurationSeconds),
				"session_duration_ms": milliseconds(message.SessionDurationSeconds),
			}),
			Extensions: extension(raw),
		}); err != nil {
			return false, err
		}
		return true, nil
	default:
		// Heartbeat, SpeakerRevision, LLMGatewayResponse and anything added
		// later. Surfaced rather than dropped so a new frame type is visible.
		return false, s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventWarning,
			Data:       marshalData(map[string]any{"message": "ignored AssemblyAI message type", "provider_type": message.Type}),
			Extensions: extension(raw),
		})
	}
}

// handleTurn maps AssemblyAI's rolling Turn frames onto transcript events.
//
// The de-duplication here is the whole reason format_turns is on. With it,
// AssemblyAI commits a turn TWICE: first a rough `end_of_turn` frame with
// turn_is_formatted=false, then a corrected frame with turn_is_formatted=true
// carrying punctuation, casing and formatted numbers. Emitting both
// double-fires every final; emitting only the first is what regressed the
// platform's word error rate from 2.0% to 5.7%. So the explicitly-unformatted
// final is swallowed and the formatted twin is the one that reaches the runtime.
//
// turn_is_formatted is a *bool on purpose. Some frames omit the flag entirely,
// and a missing flag is NOT the same as false: an absent flag means no
// corrected twin is coming, so that final must be emitted or the turn is lost.
func (s *stream) handleTurn(message inboundMessage, raw json.RawMessage) error {
	text := message.Transcript
	if strings.TrimSpace(text) == "" {
		return nil
	}
	final := message.EndOfTurn
	if final && message.TurnIsFormatted != nil && !*message.TurnIsFormatted {
		s.turnPending.Store(true)
		return nil
	}
	if final && message.TurnIsFormatted != nil && *message.TurnIsFormatted {
		s.turnPending.Store(false)
	} else {
		s.turnPending.Store(true)
	}
	kind := protocol.EventTranscriptDelta
	if final {
		kind = protocol.EventTranscriptFinal
	}
	if err := s.emit(runtimepkg.ProviderEvent{
		Type:       kind,
		Data:       s.transcriptData(message),
		Extensions: extension(raw),
	}); err != nil {
		return err
	}
	if !final {
		return nil
	}
	return s.emit(runtimepkg.ProviderEvent{
		Type: protocol.EventSpeechEnded,
		Data: marshalData(map[string]any{
			"reason":              "end_of_turn",
			"turn_order":          message.TurnOrder,
			"provider_request_id": s.sessionID,
		}),
		Extensions: extension(raw),
	})
}

func (s *stream) transcriptData(message inboundMessage) json.RawMessage {
	data := map[string]any{
		"text":                   message.Transcript,
		"is_final":               message.EndOfTurn,
		"turn_order":             message.TurnOrder,
		"end_of_turn_confidence": message.EndOfTurnConfidence,
		"provider_request_id":    s.sessionID,
	}
	if strings.TrimSpace(message.Utterance) != "" {
		data["utterance"] = message.Utterance
	}
	if strings.TrimSpace(message.LanguageCode) != "" {
		data["language"] = message.LanguageCode
	}
	// Word timings pass through verbatim. AssemblyAI documents the fields of a
	// word but not the unit of `start`/`end`, so converting them would mean
	// inventing one; the raw array is honest and the consumer can read the same
	// frame from Extensions.
	if len(message.Words) > 0 {
		data["words"] = message.Words
	}
	return marshalData(data)
}

func (s *stream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func isNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}

// readErrorFor classifies a read failure. AssemblyAI is documented to send the
// Error frame before closing, so handleMessage usually classifies first — but
// a handshake-time rejection or a dropped frame can leave the close code as the
// only evidence, and collapsing that into "provider unavailable" would hide a
// dead key behind a transient-looking error the runtime would then retry.
func readErrorFor(err error) *runtimepkg.ProviderError {
	status := int(websocket.CloseStatus(err))
	if status < 0 {
		return &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "AssemblyAI streaming read failed",
			Retryable: true,
			Cause:     err,
		}
	}
	code, retryable := errorClass(status, "")
	return &runtimepkg.ProviderError{
		Code:           code,
		Message:        "AssemblyAI closed the streaming session",
		Retryable:      retryable,
		ProviderStatus: status,
		Cause:          err,
	}
}

// errorClass maps an AssemblyAI failure onto the gateway's stable code
// vocabulary, keeping classes the runtime treats differently apart.
//
// The awkward case is 1008. AssemblyAI collapses a rejected key AND an
// exhausted balance onto that single code ("Unauthorized Connection: <reason>"),
// so the reason text is the only thing separating a credential the customer
// must replace from a wallet they must top up. Those are different problems
// with different fixes, and failing over to another key does nothing for one of
// them, so the text is inspected rather than merging the two.
func errorClass(code int, detail string) (string, bool) {
	switch code {
	case closeUnauthorized:
		if mentionsBilling(detail) {
			return "provider_quota_exceeded", false
		}
		return "authentication_failed", false
	case closeTooManySession:
		// Concurrency limit: the same session succeeds once a slot frees up.
		return "provider_rate_limited", true
	case closeInvalidMessage, closeInputDuration, closeDeprecatedV2:
		// Client-side protocol faults (bad JSON, an audio message outside
		// 50-1000 ms, a retired endpoint). Retrying reproduces them exactly.
		return "invalid_request", false
	case closeSessionExpired:
		// The session hit its maximum duration. A fresh session works.
		return "provider_unavailable", true
	case closeServerError, closeInternalError:
		return "provider_unavailable", true
	case 0:
		// An Error frame with no code. Fall back to the reason text.
		switch {
		case mentionsBilling(detail):
			return "provider_quota_exceeded", false
		case containsFold(detail, "unauthorized"), containsFold(detail, "api key"):
			return "authentication_failed", false
		case containsFold(detail, "violation"), containsFold(detail, "invalid"):
			return "invalid_request", false
		}
		return "provider_unavailable", false
	default:
		return "provider_unavailable", false
	}
}

func mentionsBilling(detail string) bool {
	for _, needle := range []string{"balance", "insufficient", "funds", "credit", "quota", "payment", "billing"} {
		if containsFold(detail, needle) {
			return true
		}
	}
	return false
}

func containsFold(haystack, needle string) bool {
	return strings.Contains(strings.ToLower(haystack), needle)
}

func errorMessage(detail string) string {
	if strings.TrimSpace(detail) == "" {
		return "AssemblyAI reported a streaming error"
	}
	return "AssemblyAI reported a streaming error: " + detail
}

func dialErrorCode(status int) string {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return "authentication_failed"
	case http.StatusPaymentRequired:
		return "provider_quota_exceeded"
	case http.StatusTooManyRequests:
		return "provider_rate_limited"
	case http.StatusBadRequest:
		return "invalid_request"
	default:
		return "provider_unavailable"
	}
}

func extension(raw json.RawMessage) map[string]json.RawMessage {
	return map[string]json.RawMessage{extensionID: raw}
}

func marshalData(value any) json.RawMessage {
	payload, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{"code":"internal"}`)
	}
	return payload
}

func milliseconds(seconds float64) int64 { return int64(math.Round(seconds * 1_000)) }

// inboundMessage is the union of every v3 server frame this adapter reads.
// Field names are snake_case exactly as AssemblyAI sends them.
type inboundMessage struct {
	Type string `json:"type"`
	// Begin
	ID        string `json:"id"`
	ExpiresAt int64  `json:"expires_at"`
	// SpeechStarted — documented in milliseconds, unlike most vendors' seconds.
	Timestamp float64 `json:"timestamp"`
	// Turn
	TurnOrder           int             `json:"turn_order"`
	Transcript          string          `json:"transcript"`
	Utterance           string          `json:"utterance"`
	EndOfTurn           bool            `json:"end_of_turn"`
	TurnIsFormatted     *bool           `json:"turn_is_formatted"`
	EndOfTurnConfidence float64         `json:"end_of_turn_confidence"`
	LanguageCode        string          `json:"language_code"`
	Words               json.RawMessage `json:"words"`
	// Termination
	AudioDurationSeconds   float64 `json:"audio_duration_seconds"`
	SessionDurationSeconds float64 `json:"session_duration_seconds"`
	// Error
	Error     string `json:"error"`
	ErrorCode int    `json:"error_code"`
}

// Compile-time proof that the stream satisfies both runtime contracts.
var (
	_ runtimepkg.ProviderStream         = (*stream)(nil)
	_ runtimepkg.AbortingProviderStream = (*stream)(nil)
	_ runtimepkg.Adapter                = (*Adapter)(nil)
)

// AssemblyAI's upper bound is enforced structurally rather than checked at
// runtime: WriteAudio never emits a message larger than batchAudioMS. These two
// constants fail to compile (unsigned conversion of a negative constant) if the
// batch size is ever tuned outside the vendor's 50-1000 ms window.
const (
	_ = uint(maxAudioMS - batchAudioMS)
	_ = uint(batchAudioMS - minAudioMS)
)
