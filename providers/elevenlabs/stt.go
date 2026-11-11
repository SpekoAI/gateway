package elevenlabs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"slices"
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

const (
	// STTAdapterID is the identifier returned by an ElevenLabs STT session plan.
	STTAdapterID = "elevenlabs.stt.v1"
	// sttEndpointPath is the realtime Scribe socket. The batch /v1/speech-to-text
	// endpoint serves DIFFERENT model ids; scribeModels says which id belongs
	// to which.
	sttEndpointPath = "/v1/speech-to-text/realtime"
	// sttPCMFrameBytes caps one input_audio_chunk. 16000 B is 0.5 s of 16 kHz
	// s16le mono and is sample-aligned, so a caller handing a whole utterance as
	// one write does not produce an oversized WebSocket message.
	sttPCMFrameBytes = 16_000
	// sttCloseBillingTimeout bounds Close's attempt to hand over the complete
	// billing observation. If the consumer is stalled, the read loop retries on
	// exit with sttFinalBillingTimeout, which bounds teardown for a consumer
	// that stopped for good. sttWriteSettleTimeout bounds the wait for a write
	// in flight when the stream ends.
	sttCloseBillingTimeout = 2 * time.Second
	sttFinalBillingTimeout = 30 * time.Second
	sttWriteSettleTimeout  = 2 * time.Second
	// sttSpeakersTimeout bounds how long a commit on a diarized session waits
	// for its diarized_transcript, from its arrival while the stream runs and
	// from Close at the end, before it goes out without speakers. Scribe sends
	// the frame within tens of milliseconds of the commit, and the platform's
	// Scribe worker holds a commit for its speakers just as long.
	sttSpeakersTimeout = 800 * time.Millisecond
)

// STTConfig controls local transport limits. Credentials and provider selection
// always come from the signed session plan, never from this configuration.
type STTConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// STTAdapter implements ElevenLabs' realtime Scribe WebSocket API.
type STTAdapter struct {
	id              string
	httpClient      *http.Client
	eventBuffer     int
	maxMessageBytes int64
	endpointPolicy  upstream.WebSocketPolicy
}

// NewSTT creates an STT adapter with bounded provider-event buffering.
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
	if config.EventBuffer < 1 {
		return nil, errors.New("elevenlabs event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("elevenlabs maximum message bytes must be positive")
	}
	endpointPolicy, err := upstream.NewWebSocketPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &STTAdapter{
		id:              config.AdapterID,
		httpClient:      config.HTTPClient,
		eventBuffer:     config.EventBuffer,
		maxMessageBytes: config.MaxMessageBytes,
		endpointPolicy:  endpointPolicy,
	}, nil
}

func (a *STTAdapter) ID() string { return a.id }

// Open opens a provider-direct STT stream against realtime Scribe.
func (a *STTAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindSTT {
		return nil, fmt.Errorf("elevenlabs stt supports stt sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "elevenlabs" {
		return nil, fmt.Errorf("elevenlabs adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("elevenlabs requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("elevenlabs stt requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("elevenlabs stt media: %w", err)
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("elevenlabs stt requires a bearer credential")
	}
	session, err := planRealtime(a.endpointPolicy, request.Plan.Route.Endpoint, request.Plan.Route.Model, request.Options, *request.Media)
	if err != nil {
		return nil, err
	}
	endpoint := session.endpoint
	// Two different credentials, two different channels. A BYOK session carries the
	// customer's permanent API key, which belongs in `xi-api-key`. A managed session
	// carries a single-use token minted by the control plane, and the vendor accepts
	// a token ONLY as the `token` query parameter — sending it as the header fails
	// authentication. Same split Cartesia already makes with its access token.
	// A relay plan is the exception inside managed: it is managed for billing
	// purposes but carries the connector's permanent provider key, so it uses the
	// header channel like BYOK rather than putting a permanent key in the URL.
	headers := make(http.Header)
	if request.Plan.Execution.ProviderRoute == protocol.RouteSpekoRelay {
		headers.Set("xi-api-key", credential.Value)
	} else if request.Plan.Execution.CredentialSource == protocol.CredentialsManaged {
		endpoint, err = sttEndpointWithToken(endpoint, credential.Value)
		if err != nil {
			return nil, err
		}
	} else {
		headers.Set("xi-api-key", credential.Value)
	}
	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{
		HTTPClient: sttHTTPClient(a.httpClient),
		HTTPHeader: headers,
	})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		return nil, &runtimepkg.ProviderError{
			Code:           sttDialErrorCode(status),
			Message:        "ElevenLabs streaming connection could not be established",
			Retryable:      status == 0 || status == http.StatusTooManyRequests || status >= 500,
			ProviderStatus: status,
			Cause:          err,
		}
	}
	conn.SetReadLimit(a.maxMessageBytes)
	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &sttStream{
		conn:     conn,
		ctx:      streamCtx,
		cancel:   cancel,
		events:   make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		sendTurn: make(chan struct{}, 1),
		rate:     request.Media.SampleRateHz,
		model:    request.Plan.Route.Model,

		keytermLibrary: session.keytermLibrary,
		diarize:        session.diarize,
		speakerMerges:  map[string]string{},
	}
	go stream.readLoop()
	return stream, nil
}

func sttHTTPClient(client *http.Client) *http.Client {
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
// Engine, no SessionPlan.Validate — labels the same permanent ElevenLabs key
// bearer. Both spellings carry a permanent key; the header-versus-query
// channel split keys off the route, not the kind, so nothing else on the
// relay arm changes.
func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

// realtimeSession is what Open derives from a request before it dials: the
// socket URL, and the two things the model's capabilities decide beyond it.
type realtimeSession struct {
	endpoint string
	// keytermLibrary rides the first input_audio_chunk.
	keytermLibrary []string
	// diarize means finals come from diarized_transcript, not the commit.
	diarize bool
}

func planRealtime(policy upstream.WebSocketPolicy, rawEndpoint, model string, options protocol.RequestOptions, media protocol.MediaFormat) (realtimeSession, error) {
	endpoint, err := policy.Parse(rawEndpoint)
	if err != nil {
		return realtimeSession{}, fmt.Errorf("elevenlabs stt endpoint: %w", err)
	}
	if endpoint.Path != sttEndpointPath {
		return realtimeSession{}, fmt.Errorf("elevenlabs stt endpoint path must be %s, got %q", sttEndpointPath, endpoint.Path)
	}
	if strings.TrimSpace(model) == "" || model == "auto" {
		return realtimeSession{}, errors.New("elevenlabs stt requires a concrete model in the session plan")
	}
	capabilities, err := scribeModelFor(model, scribeRealtime)
	if err != nil {
		return realtimeSession{}, err
	}
	if media.Encoding != "pcm_s16le" {
		return realtimeSession{}, &runtimepkg.ProviderError{Code: "unsupported_media", Message: fmt.Sprintf("ElevenLabs transcription cannot accept %s audio", media.Encoding), Hint: "Convert the input to mono pcm_s16le at 8000, 16000, 22050, 24000, 44100, or 48000 Hz and try again."}
	}
	if media.Channels != 1 {
		return realtimeSession{}, &runtimepkg.ProviderError{Code: "unsupported_media", Message: fmt.Sprintf("ElevenLabs transcription cannot accept %d-channel audio", media.Channels), Hint: "Downmix the input to mono pcm_s16le and try again."}
	}
	query := endpoint.Query()
	query.Set("model_id", model)
	// `audio_format` accepts a FIXED set of tokens. Declaring pcm_16000 for 32 kHz
	// audio — the platform's fallback — does not resample anything: Scribe reads the
	// bytes at the wrong rate and the transcript degrades badly while the session
	// looks healthy. An unsupported rate is refused here instead.
	audioFormat, err := sttAudioFormat(media.SampleRateHz)
	if err != nil {
		return realtimeSession{}, err
	}
	query.Set("audio_format", audioFormat)
	// Word timestamps. Without this, a committed segment arrives as
	// `committed_transcript` (text only) and no word timings are available at all.
	// With it, Scribe emits `committed_transcript_with_timestamps` carrying a
	// words array — see handleMessage for why only the timestamped twin is used.
	query.Set("include_timestamps", "true")
	// The framework still owns turn detection; this only decides when Scribe
	// finalizes a segment on its own side. Always sent, because the vendor's
	// default is manual commits. A caller of a model with turn prediction can
	// replace it below.
	query.Set("commit_strategy", "vad")
	if language := strings.TrimSpace(options.Language); language != "" {
		// `language_code`, NOT `language`: the vendor ignores an unknown parameter,
		// so the wrong name silently leaves Scribe auto-detecting instead of pinned.
		// Only the primary subtag is accepted — `es`, not `es-MX`.
		query.Set("language_code", sttLanguageCode(language))
	}
	// Vocabulary biasing: repeated `keyterms`, one per term, matching how the
	// platform's own Scribe adapter spells it on this same realtime socket.
	keyterms, library, err := capabilities.realtimeKeyterms(options.STT.GetKeywords())
	if err != nil {
		return realtimeSession{}, err
	}
	for _, keyword := range keyterms {
		query.Add("keyterms", keyword)
	}
	diarize := options.STT.Diarize()
	if diarize {
		if !capabilities.diarization {
			return realtimeSession{}, scribeUnsupported(model, "diarization", func(candidate scribeModel) bool { return candidate.diarization })
		}
		query.Set("diarize", "true")
	}
	// The caller's own Scribe settings. The gateway allow-lists them per
	// model, and the model's capabilities refuse a gated one again here for
	// callers that drive this adapter without the gateway.
	params, err := capabilities.params(model, options.STT)
	if err != nil {
		return realtimeSession{}, err
	}
	replaced := map[string]bool{}
	for _, param := range params {
		if !replaced[param.name] {
			query.Del(param.name)
			replaced[param.name] = true
		}
		query.Add(param.name, param.value)
	}
	endpoint.RawQuery = query.Encode()
	return realtimeSession{endpoint: endpoint.String(), keytermLibrary: library, diarize: diarize}, nil
}

// sttAudioFormat maps a portable sample rate onto one of the vendor's accepted
// `audio_format` tokens. 22050 and 44100 are accepted by ElevenLabs but are not
// portable media rates here, so they are simply unreachable rather than special.
func sttAudioFormat(sampleRateHz int) (string, error) {
	switch sampleRateHz {
	case 8000, 16000, 22050, 24000, 44100, 48000:
		return "pcm_" + strconv.Itoa(sampleRateHz), nil
	default:
		return "", &runtimepkg.ProviderError{Code: "unsupported_media", Message: fmt.Sprintf("ElevenLabs transcription cannot accept a %d Hz sample rate", sampleRateHz), Hint: "Resample the mono pcm_s16le input to 8000, 16000, 22050, 24000, 44100, or 48000 Hz and try again."}
	}
}

// sttLanguageCode is the Scribe `language_code` (realtime and batch). Scribe
// accepts ISO 639-1 or 639-3 codes, and lists Norwegian only as `nor`: a
// platform `nb` is answered with invalid_request and close 1008, which ends
// the session. The aliases match the platform's elevenLabsSttLanguage
// (packages/providers lang.ts) for Norwegian only. TTS uses ttsLanguageCode
// instead, because its 639-1 language_code would reject `nor`.
func sttLanguageCode(language string) string {
	base := baseLanguageTag(language)
	if alias, ok := sttLanguageAliases[base]; ok {
		return alias
	}
	return base
}

var sttLanguageAliases = map[string]string{"nb": "nor", "nn": "nor"}

// baseLanguageTag reduces a tag to its primary subtag.
func baseLanguageTag(language string) string {
	lowered := strings.ToLower(strings.TrimSpace(language))
	if index := strings.IndexAny(lowered, "-_"); index > 0 {
		return lowered[:index]
	}
	return lowered
}

// sttEndpointWithToken places a minted token in the query string. The credential
// reaches the URL, so this value must never be logged; the runtime already treats
// route endpoints as sensitive for exactly this reason.
func sttEndpointWithToken(rawEndpoint, token string) (string, error) {
	endpoint, err := url.Parse(rawEndpoint)
	if err != nil {
		return "", errors.New("elevenlabs stt endpoint could not be prepared")
	}
	query := endpoint.Query()
	query.Set("token", token)
	endpoint.RawQuery = query.Encode()
	return endpoint.String(), nil
}

type sttStream struct {
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent
	rate   int
	// model is the realtime Scribe model on the socket, named in billing
	// because every Scribe model bills at its own rate.
	model string

	writeMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	inputPending atomic.Bool
	closeErr     error
	stateMu      sync.Mutex
	baseFinals   []string
	timedFinals  []string
	// sentBytes is the PCM written to the socket. Scribe reports no audio
	// count on the realtime socket, so this is the billed duration.
	sentBytes atomic.Int64
	// sendTurn admits one sender at a time for the billing observations,
	// held events and, on a diarized session, everything the read loop
	// delivers, so they reach the consumer in order: a late session_started
	// cannot follow the complete observation with an incomplete one, and a
	// commit received before the complete observation goes out before it.
	// readLoop closes the channel while holding it, so no holder sends after
	// that. It is a channel rather than a mutex so Close can stop waiting for
	// a holder that a stalled consumer blocks.
	sendTurn chan struct{}
	// billingMu guards sessionID, billed, eventsClosed, held and
	// speakersTimer. It is never held across a send.
	billingMu sync.Mutex
	// endedErr is why the provider side ended input, set before closed when
	// the read loop exits with input still open. A refused write returns it,
	// so a concurrent writer reports the vendor's failure (quota, rate limit)
	// rather than a bare ErrSessionClosed the runtime would treat as internal.
	endedErr     atomic.Pointer[error]
	sessionID    string
	billed       bool
	eventsClosed bool

	// keytermLibrary is sent on the first audio chunk and then cleared: the
	// vendor closes the socket on a library in any later chunk. Guarded by
	// writeMu, so two concurrent writers cannot both be first.
	keytermLibrary []string
	diarize        bool
	// held is what a diarized session has not yet given the consumer, in the
	// vendor's order: a commit waiting for its diarized_transcript first, then
	// everything received after it. Only the send turn's holder changes it.
	held []sttHeld
	// speakersTimer releases the commit at the head of held once it is due.
	speakersTimer *time.Timer
	// speakerMerges maps a speaker id the vendor merged away to the id it
	// merged into. Touched only by the read loop.
	speakerMerges map[string]string
}

type sttHeld struct {
	event   runtimepkg.ProviderEvent
	segment int
	// due is when a commit whose diarized_transcript has not arrived goes out
	// without it. Zero for every other event.
	due time.Time
}

func (h sttHeld) waiting() bool { return !h.due.IsZero() }

func (s *sttStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

// WriteAudio forwards PCM as base64 inside a JSON frame. Realtime Scribe has no
// binary input frame at all — audio arrives as `input_audio_chunk` messages — so
// this is a text write even though the payload is audio.
func (s *sttStream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("elevenlabs stt audio is empty")
	}
	for offset := 0; offset < len(audio); offset += sttPCMFrameBytes {
		end := offset + sttPCMFrameBytes
		if end > len(audio) {
			end = len(audio)
		}
		chunk := map[string]any{
			"message_type":  "input_audio_chunk",
			"audio_base_64": base64.StdEncoding.EncodeToString(audio[offset:end]),
			"commit":        false,
			"sample_rate":   s.rate,
		}
		if err := s.writeFrame(ctx, chunk, int64(end-offset)); err != nil {
			return err
		}
	}
	s.inputPending.Store(true)
	return nil
}

// CommitAudio flushes buffered audio. An empty chunk with commit:true is the
// documented end-of-input signal and forces a final committed transcript for the
// last segment; there is no separate finalize message.
func (s *sttStream) CommitAudio(ctx context.Context) error {
	if !s.inputPending.CompareAndSwap(true, false) {
		return nil
	}
	if err := s.writeFrame(ctx, map[string]any{
		"message_type":  "input_audio_chunk",
		"audio_base_64": "",
		"commit":        true,
	}, 0); err != nil {
		s.inputPending.Store(true)
		return err
	}
	return nil
}

func (s *sttStream) AppendText(context.Context, string) error {
	return runtimepkg.ErrUnsupportedOperation
}

func (s *sttStream) CommitText(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// Cancel closes the stream because the realtime protocol has no distinct cancel
// command. It aborts rather than waiting for a final result, or for the
// speakers of a commit already received.
func (s *sttStream) Cancel(ctx context.Context) error {
	closeErr := s.closeInput(ctx, 0)
	if err := s.abort(); closeErr == nil {
		return err
	}
	return closeErr
}

// Abort immediately tears down the socket after a runtime terminal failure.
func (s *sttStream) Abort(context.Context) error { return s.abort() }

// Close commits any buffered audio and then shuts the socket down. Committing
// first is what makes the last segment arrive: dropping the socket without it
// discards whatever Scribe had buffered.
func (s *sttStream) Close(ctx context.Context) error {
	return s.closeInput(ctx, sttSpeakersTimeout)
}

// closeInput closes input once. A commit still held for its speakers gets up
// to speakersTimeout for them before the complete billing observation.
func (s *sttStream) closeInput(ctx context.Context, speakersTimeout time.Duration) error {
	s.gracefulOnce.Do(func() {
		// A commit refused because Scribe closed the stream normally is not a
		// Close failure: a clean server close stays a clean shutdown. A real
		// provider error is returned, so Cancel cannot abort past it as if
		// the stream had ended cleanly.
		if err := s.CommitAudio(ctx); err != nil && !errors.Is(err, runtimepkg.ErrSessionClosed) && !errors.Is(err, errSTTProviderClosed) {
			s.closeErr = err
		}
		s.closed.Store(true)
		if s.closeErr != nil {
			_ = s.abort()
			return
		}
		// Input is closed, so the PCM count is final. Taking the write lock
		// waits out a frame still in flight before it is read.
		s.writeMu.Lock()
		s.writeMu.Unlock()
		s.awaitSpeakers(speakersTimeout)
		s.emitCompleteBilling(sttCloseBillingTimeout, false)
	})
	return s.closeErr
}

// awaitSpeakers waits, up to timeout, until no commit is held for its
// speakers, so a diarized_transcript the read loop is still receiving labels
// its final before emitCompleteBilling sends the commit without them. It
// returns at once when no commit waits. It polls rather than holding
// billingMu, which the read loop needs to deliver the final.
func (s *sttStream) awaitSpeakers(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for s.awaitingSpeakers() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
}

func (s *sttStream) awaitingSpeakers() bool {
	s.billingMu.Lock()
	defer s.billingMu.Unlock()
	return !s.eventsClosed && slices.ContainsFunc(s.held, sttHeld.waiting)
}

// emitCompleteBilling hands the complete observation to the consumer exactly
// once, while the events channel is open. ElevenLabs bills the audio it
// received however the stream ended, so Close sends it once input is final
// and the read loop sends it on exit for every other ending (Abort, a
// server close, a provider error). It counts as sent only once delivered, so a
// stalled consumer that resumes still gets it from the read loop's retry. The
// send is bounded by timeout rather than s.ctx, which Abort has cancelled.
// With requireAudio, a stream that never started and sent no PCM reports
// nothing, so a failure before startup stays its consumer's first event. A
// started stream always completes its operation, at zero if no audio went out.
//
// Held events go first, with commits still waiting for their speakers sent
// without them: their words were received before the observation, and losing
// the words would be worse than losing their labels. A late
// diarized_transcript for one of them then answers nothing. If the consumer
// stalls before taking them all, the observation waits for the read loop's
// retry rather than overtake them.
//
// timeout bounds the whole attempt, including the wait for a sender the
// stalled consumer is blocking, so a consumer that never reads again holds
// Close for timeout at most.
func (s *sttStream) emitCompleteBilling(timeout time.Duration, requireAudio bool) {
	attempt, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if !s.takeTurn(attempt.Done()) {
		return
	}
	defer s.releaseTurn()
	s.completeBilling(attempt.Done(), requireAudio)
}

// completeBilling is emitCompleteBilling for a caller that holds the send
// turn, giving up once stop closes.
func (s *sttStream) completeBilling(stop <-chan struct{}, requireAudio bool) {
	if !s.sendHeld(true, stop) {
		return
	}
	s.billingMu.Lock()
	if s.billed || (requireAudio && s.sentBytes.Load() == 0 && s.sessionID == "") {
		s.billingMu.Unlock()
		return
	}
	observation := s.billing(true)
	s.billingMu.Unlock()
	if s.sendUntil(stop, runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Billing: observation}) {
		s.billingMu.Lock()
		s.billed = true
		s.billingMu.Unlock()
	}
}

// awaitWrites waits, up to timeout, for a write in flight to finish, so the
// complete observation counts audio that reached Scribe. A write that is
// still stuck after the timeout did not complete and is not billed.
func (s *sttStream) awaitWrites(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for !s.writeMu.TryLock() {
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	s.writeMu.Unlock()
}

// billing is the session's one duration observation. Until the stream ends it
// is incomplete and carries no quantity; the complete observation is the PCM
// this stream sent, as an exact fraction of a second. Only a consumer that
// stops reading for good before the read loop's exit leaves it unresolved.
// The caller holds billingMu.
func (s *sttStream) billing(complete bool) *protocol.BillingObservation {
	observation := &protocol.BillingObservation{OperationID: "stream", Model: s.model, Mode: "streaming", ProviderRequestID: s.sessionID, Quantities: map[string]int64{}}
	if complete {
		observation.Quantities["duration_seconds"] = s.sentBytes.Load()
		observation.QuantityDenominators = map[string]int64{"duration_seconds": int64(s.rate) * 2}
		observation.Complete = true
	}
	return observation
}

func (s *sttStream) abort() error {
	s.abortOnce.Do(func() {
		// Cancelling fails a write in flight, so the read loop's exit counts
		// only audio that reached Scribe before it sends the complete
		// observation.
		s.cancel()
		if err := s.conn.CloseNow(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
	})
	return s.closeErr
}

// writeFrame writes one JSON frame and, for an audio chunk, counts its PCM
// bytes under the same lock. Counting inside the lock is what lets the read
// loop's exit trust sentBytes once it has held the lock after closing input.
func (s *sttStream) writeFrame(ctx context.Context, chunk map[string]any, audioBytes int64) error {
	if s.closed.Load() {
		return s.inputClosedErr()
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.closed.Load() {
		return s.inputClosedErr()
	}
	if len(s.keytermLibrary) > 0 {
		chunk["keyterm_library"] = s.keytermLibrary
		s.keytermLibrary = nil
	}
	payload, err := json.Marshal(chunk)
	if err != nil {
		return err
	}
	if err := s.conn.Write(ctx, websocket.MessageText, payload); err != nil {
		return &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "ElevenLabs streaming write failed",
			Retryable: true,
			Cause:     err,
		}
	}
	s.sentBytes.Add(audioBytes)
	return nil
}

// errSTTProviderClosed is the reason recorded when Scribe ended the stream
// without an error frame. Close treats only this one as clean; a real
// provider error stays a failure.
var errSTTProviderClosed error = &runtimepkg.ProviderError{
	Code:      "provider_unavailable",
	Message:   "ElevenLabs ended the streaming connection",
	Retryable: true,
}

// inputClosedErr is what a write returns once input is closed: the reason the
// provider side ended the stream, or ErrSessionClosed after the caller's Close.
func (s *sttStream) inputClosedErr() error {
	if err := s.endedErr.Load(); err != nil {
		return *err
	}
	return runtimepkg.ErrSessionClosed
}

func (s *sttStream) readLoop() {
	// terminal is delivered after the complete observation: consumers stop at
	// an error event, so billing has to reach them first.
	var terminal *runtimepkg.ProviderEvent
	defer func() {
		// Close input first: writeFrame checks closed under the write lock,
		// so once awaitWrites has held that lock no later audio can be sent,
		// and every write that succeeded is already in sentBytes. If input was
		// still open, the provider or a teardown ended the stream; record why
		// before closing, so a refused write carries the same failure.
		if !s.closed.Load() {
			ended := errSTTProviderClosed
			if terminal != nil && terminal.Err != nil {
				ended = terminal.Err
			}
			s.endedErr.Store(&ended)
		}
		s.closed.Store(true)
		s.awaitWrites(sttWriteSettleTimeout)
		// Another holder lets the turn go once its event is taken, Abort
		// cancels s.ctx, or Close's bound passes.
		s.sendTurn <- struct{}{}
		defer s.releaseTurn()
		final, cancel := context.WithTimeout(context.Background(), sttFinalBillingTimeout)
		s.completeBilling(final.Done(), true)
		cancel()
		if terminal != nil {
			// Bounded like the billing send rather than tied to s.ctx, so a
			// Cancel that aborts meanwhile cannot drop the failure.
			s.emitBounded(*terminal, sttFinalBillingTimeout)
		}
		s.billingMu.Lock()
		s.eventsClosed = true
		close(s.events)
		s.billingMu.Unlock()
	}()
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && !sttIsNormalClose(err) && s.ctx.Err() == nil {
				terminal = &runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{
					Code:      "provider_unavailable",
					Message:   "ElevenLabs streaming read failed",
					Retryable: true,
					Cause:     err,
				}}
			}
			return
		}
		if messageType != websocket.MessageText {
			continue
		}
		if err := s.handleMessage(payload); err != nil {
			// An event send interrupted by our own Cancel or Abort is teardown,
			// not a provider failure; only a real error becomes terminal.
			if ctxErr := s.ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
				return
			}
			terminal = &runtimepkg.ProviderEvent{Err: err}
			return
		}
	}
}

func (s *sttStream) handleMessage(payload []byte) error {
	var message sttInboundMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "ElevenLabs sent malformed streaming JSON",
			Retryable: true,
			Cause:     err,
		}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))
	switch message.MessageType {
	case "session_started":
		// The protocol-level ack. auth_error and quota_exceeded arrive as frames on
		// an ALREADY-OPEN socket, so readiness has to wait for this rather than fire
		// at dial — otherwise those failures look like mid-stream errors and the
		// runtime loses its open-failure failover.
		if !s.takeTurn(s.ctx.Done()) {
			return s.ctx.Err()
		}
		defer s.releaseTurn()
		s.billingMu.Lock()
		var observation *protocol.BillingObservation
		if !s.billed {
			s.sessionID = message.SessionID
			observation = s.billing(false)
		}
		s.billingMu.Unlock()
		return s.emit(runtimepkg.ProviderEvent{
			Type:       protocol.EventUsageObserved,
			Data:       sttMarshalData(map[string]any{"provider_request_id": message.SessionID}),
			Extensions: sttExtension(raw),
			Billing:    observation,
		})
	case "partial_transcript":
		if strings.TrimSpace(message.Text) == "" {
			return nil
		}
		data := map[string]any{"text": message.Text, "is_final": false, "speech_final": false}
		// A model with turn prediction scores every partial with the chance
		// that the speaker has finished. Carried under the vendor's name, the
		// way AssemblyAI's end_of_turn_confidence is.
		if message.TurnProbability != nil {
			data["turn_probability"] = *message.TurnProbability
		}
		return s.deliver(runtimepkg.ProviderEvent{
			Type:       protocol.EventTranscriptDelta,
			Data:       sttMarshalData(data),
			Extensions: sttExtension(raw),
		})
	case "final_transcript", "committed_transcript":
		if strings.TrimSpace(message.Text) == "" {
			return nil
		}
		if s.diarize {
			// The speakers arrive on the diarized_transcript that follows
			// every commit, and the final waits for it. resolveCommit,
			// releaseDueCommit and emitCompleteBilling say when a commit goes
			// out without them.
			return s.updateHeld(func() {
				s.held = append(s.held, sttHeld{event: heldFinal(message.Text), segment: message.SegmentIndex, due: time.Now().Add(sttSpeakersTimeout)})
			})
		}
		if s.consumeTimedFinal(message.Text) {
			// The timestamped twin already carried both text and alignment.
			return nil
		}
		s.rememberBaseFinal(message.Text)
		return s.deliver(runtimepkg.ProviderEvent{
			Type:       protocol.EventTranscriptFinal,
			Data:       sttTranscriptData(message.Text, true, nil),
			Extensions: sttExtension(raw),
		})
	case "final_transcript_with_timestamps", "committed_transcript_with_timestamps":
		// On a diarized session the diarized_transcript carries the same word
		// timings beside the speakers, so this twin adds nothing.
		if strings.TrimSpace(message.Text) == "" || s.diarize {
			return nil
		}
		if s.consumeBaseFinal(message.Text) {
			return s.deliver(runtimepkg.ProviderEvent{
				Type:       protocol.EventAlignment,
				Data:       sttAlignmentData(message.Text, message.Words),
				Extensions: sttExtension(raw),
			})
		}
		s.rememberTimedFinal(message.Text)
		return s.deliver(runtimepkg.ProviderEvent{Type: protocol.EventTranscriptFinal, Data: sttTranscriptData(message.Text, true, message.Words), Extensions: sttExtension(raw)})
	// Every error type in the vendor's realtime event schema. ElevenLabs sends
	// the error frame and then closes the socket, so each one is terminal.
	case "error", "scribe_error", "auth_error", "quota_exceeded", "rate_limited", "input_error", "invalid_request", "insufficient_audio_activity", "transcriber_error", "commit_throttled", "unaccepted_terms", "queue_overflow", "resource_exhausted", "session_time_limit_exceeded", "chunk_size_exceeded":
		return &runtimepkg.ProviderError{
			Code:      sttErrorCode(message.MessageType),
			Message:   sttErrorMessage(message.MessageType, message.Error),
			Hint:      sttErrorHint(message.MessageType),
			Retryable: sttErrorRetryable(message.MessageType),
		}
	case "diarized_transcript":
		if !s.diarize {
			return s.emitIgnored(message.MessageType, raw)
		}
		return s.resolveCommit(message, raw)
	case "speakers_merged":
		if !s.diarize {
			return s.emitIgnored(message.MessageType, raw)
		}
		s.speakerMerges[message.FromSpeakerID] = message.ToSpeakerID
		// Finals already delivered keep the old label: the protocol has no
		// event that relabels a past transcript. The merge is surfaced so a
		// consumer that stores speakers can apply it; later finals already
		// use the surviving id.
		return s.deliver(runtimepkg.ProviderEvent{
			Type: protocol.EventWarning,
			Data: sttMarshalData(map[string]any{
				"message":       "ElevenLabs merged two speaker labels",
				"provider_type": message.MessageType,
				"from_speaker":  message.FromSpeakerID,
				"to_speaker":    s.mergedSpeaker(message.ToSpeakerID),
			}),
			Extensions: sttExtension(raw),
		})
	default:
		return s.emitIgnored(message.MessageType, raw)
	}
}

func (s *sttStream) emitIgnored(messageType string, raw json.RawMessage) error {
	return s.deliver(runtimepkg.ProviderEvent{
		Type:       protocol.EventWarning,
		Data:       sttMarshalData(map[string]any{"message": "ignored ElevenLabs message type", "provider_type": messageType}),
		Extensions: sttExtension(raw),
	})
}

// deliver gives the consumer an event the read loop produced. On a diarized
// session an event behind a commit still waiting for its speakers is held
// until that commit's final goes out, so a later segment's partial cannot
// overtake an earlier segment's final.
func (s *sttStream) deliver(event runtimepkg.ProviderEvent) error {
	if !s.diarize {
		return s.emit(event)
	}
	return s.updateHeld(func() { s.held = append(s.held, sttHeld{event: event}) })
}

// resolveCommit gives the commit a diarized_transcript answers its speakers
// and releases what is held up to the next commit still waiting. The vendor
// sends them in commit order, so earlier commits still waiting will not get
// theirs in order; they go out without speakers. A frame that answers no
// waiting commit, such as a duplicate or one for a commit already sent
// without speakers, yields no final, and one without text delivers the
// commit's text without speakers.
func (s *sttStream) resolveCommit(message sttInboundMessage, raw json.RawMessage) error {
	return s.updateHeld(func() {
		match := slices.IndexFunc(s.held, func(held sttHeld) bool { return held.waiting() && held.segment == message.SegmentIndex })
		if match < 0 {
			return
		}
		for index := range s.held[:match] {
			s.held[index].due = time.Time{}
		}
		resolved := &s.held[match]
		if strings.TrimSpace(message.Text) != "" {
			resolved.event.Data = s.diarizedData(message.Text, message.Words)
		}
		resolved.event.Extensions = sttExtension(raw)
		resolved.due = time.Time{}
	})
}

// releaseDueCommit runs on the speakers timer. It sends the commit at the head
// of held without speakers once it is due, and what is held behind it up to
// the next commit still waiting. A timer that fired for a head released
// meanwhile finds the new head not yet due and only re-arms.
func (s *sttStream) releaseDueCommit() {
	// A send fails only once Abort has cancelled the stream, and the read
	// loop's exit then delivers what is still held.
	_ = s.updateHeld(func() {
		if len(s.held) > 0 && s.held[0].waiting() && !time.Now().Before(s.held[0].due) {
			s.held[0].due = time.Time{}
		}
	})
}

// updateHeld applies change to held and then sends held events up to the
// next commit still waiting, holding the send turn throughout so the read
// loop, the timer and Close's complete observation cannot interleave their
// sends. A consumer that stops reading blocks it until it reads again or
// Abort cancels the stream.
func (s *sttStream) updateHeld(change func()) error {
	if !s.takeTurn(s.ctx.Done()) {
		return s.ctx.Err()
	}
	defer s.releaseTurn()
	s.billingMu.Lock()
	change()
	s.billingMu.Unlock()
	if !s.sendHeld(false, s.ctx.Done()) {
		return s.ctx.Err()
	}
	return nil
}

// sendHeld sends held events in order up to the next commit still waiting
// for its speakers, or every one with all, and then points the speakers timer
// at what is left. It reports false once stop closes, leaving the event it
// was sending held. The caller holds the send turn.
func (s *sttStream) sendHeld(all bool, stop <-chan struct{}) bool {
	s.billingMu.Lock()
	defer s.billingMu.Unlock()
	for len(s.held) > 0 && (all || !s.held[0].waiting()) {
		event := s.held[0].event
		s.billingMu.Unlock()
		sent := s.sendUntil(stop, event)
		s.billingMu.Lock()
		if !sent {
			return false
		}
		s.held = s.held[1:]
	}
	s.resetSpeakersTimer()
	return true
}

// takeTurn waits for the send turn until stop closes, and reports whether it
// holds the turn with the event channel still open.
func (s *sttStream) takeTurn(stop <-chan struct{}) bool {
	select {
	case s.sendTurn <- struct{}{}:
	case <-stop:
		return false
	}
	s.billingMu.Lock()
	open := !s.eventsClosed
	s.billingMu.Unlock()
	if !open {
		s.releaseTurn()
	}
	return open
}

func (s *sttStream) releaseTurn() { <-s.sendTurn }

// resetSpeakersTimer points the speakers timer at the commit at the head of
// held, or stops it when no commit waits there. The caller holds billingMu.
func (s *sttStream) resetSpeakersTimer() {
	if len(s.held) == 0 || !s.held[0].waiting() {
		if s.speakersTimer != nil {
			s.speakersTimer.Stop()
		}
		return
	}
	wait := time.Until(s.held[0].due)
	if s.speakersTimer == nil {
		s.speakersTimer = time.AfterFunc(wait, s.releaseDueCommit)
		return
	}
	s.speakersTimer.Reset(wait)
}

// heldFinal is a committed segment delivered without its speakers.
func heldFinal(text string) runtimepkg.ProviderEvent {
	return runtimepkg.ProviderEvent{Type: protocol.EventTranscriptFinal, Data: sttTranscriptData(text, true, nil)}
}

// mergedSpeaker follows the vendor's merges to the id a speaker ended up as.
// Merges can chain; ids are never reused, so the walk ends.
func (s *sttStream) mergedSpeaker(id string) string {
	for range len(s.speakerMerges) {
		next, merged := s.speakerMerges[id]
		if !merged {
			break
		}
		id = next
	}
	return id
}

// diarizedData is a final with the vendor's speaker label on each word, and on
// the final itself when one speaker said all of it. A word whose speaker the
// vendor has not resolved could be anyone's, so it keeps the final unlabelled.
func (s *sttStream) diarizedData(text string, words []sttWord) json.RawMessage {
	data := map[string]any{"text": text, "is_final": true, "speech_final": true}
	timings := sttWordTimings(words)
	// An unresolved word counts as the speaker "".
	speakers := map[string]struct{}{}
	for index, word := range sttSpokenWords(words) {
		speaker := ""
		if word.SpeakerID != nil && *word.SpeakerID != "" {
			speaker = s.mergedSpeaker(*word.SpeakerID)
			timings[index]["speaker"] = speaker
		}
		speakers[speaker] = struct{}{}
	}
	sttSetWords(data, timings)
	if len(speakers) == 1 {
		for speaker := range speakers {
			if speaker != "" {
				data["speaker"] = speaker
			}
		}
	}
	return sttMarshalData(data)
}

func (s *sttStream) rememberBaseFinal(text string) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.baseFinals = rememberFinal(s.baseFinals, text)
}

func (s *sttStream) consumeBaseFinal(text string) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return consumeFinal(&s.baseFinals, text)
}

func (s *sttStream) rememberTimedFinal(text string) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.timedFinals = rememberFinal(s.timedFinals, text)
}

func (s *sttStream) consumeTimedFinal(text string) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return consumeFinal(&s.timedFinals, text)
}

func rememberFinal(finals []string, text string) []string {
	finals = append(finals, text)
	if len(finals) > 32 {
		finals = append([]string(nil), finals[len(finals)-32:]...)
	}
	return finals
}

func consumeFinal(finals *[]string, text string) bool {
	for i, candidate := range *finals {
		if candidate == text {
			*finals = append((*finals)[:i], (*finals)[i+1:]...)
			return true
		}
	}
	return false
}

// sendUntil hands event to the consumer unless stop closes first, and reports
// whether the consumer took it. The caller holds the send turn.
func (s *sttStream) sendUntil(stop <-chan struct{}, event runtimepkg.ProviderEvent) bool {
	select {
	case s.events <- event:
		return true
	case <-stop:
		return false
	}
}

// emitBounded delivers a teardown event, waiting at most timeout for a
// consumer, and reports whether one took it. The caller is the read loop's
// exit, before it closes the channel.
func (s *sttStream) emitBounded(event runtimepkg.ProviderEvent, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case s.events <- event:
		return true
	case <-timer.C:
		return false
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

func sttIsNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}

func sttDialErrorCode(status int) string {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return "authentication_failed"
	case http.StatusTooManyRequests:
		return "provider_rate_limited"
	default:
		return "provider_unavailable"
	}
}

// sttErrorCode keeps the vendor's own distinction. Collapsing auth and quota into
// one code would make a dead key and an empty balance indistinguishable, and only
// one of the two is worth retrying elsewhere.
func sttErrorCode(messageType string) string {
	switch messageType {
	case "auth_error", "unaccepted_terms":
		return "authentication_failed"
	case "quota_exceeded":
		return "provider_quota_exceeded"
	case "rate_limited", "commit_throttled":
		return "provider_rate_limited"
	// invalid_request: the connection parameters were rejected.
	// insufficient_audio_activity: the caller stopped sending audio long enough
	// for Scribe to drop the connection. Retrying the same request repeats both.
	case "input_error", "chunk_size_exceeded", "invalid_request", "insufficient_audio_activity":
		return "invalid_request"
	default:
		return "provider_unavailable"
	}
}

func sttErrorRetryable(messageType string) bool {
	switch messageType {
	case "rate_limited", "commit_throttled", "queue_overflow", "resource_exhausted", "session_time_limit_exceeded", "transcriber_error", "error", "scribe_error":
		return true
	default:
		return false
	}
}

func sttErrorHint(messageType string) string {
	switch messageType {
	case "auth_error":
		return "Check that the ElevenLabs credential is active and authorized for realtime transcription."
	case "unaccepted_terms":
		return "Accept the ElevenLabs Scribe terms for the provider account, then retry."
	case "quota_exceeded":
		return "Add ElevenLabs quota or select another provider."
	case "rate_limited", "commit_throttled":
		return "Retry with backoff and reduce request or commit frequency."
	case "queue_overflow", "resource_exhausted", "session_time_limit_exceeded", "transcriber_error", "error", "scribe_error":
		return "Retry after a brief delay or use auto routing to select another provider."
	case "chunk_size_exceeded":
		return "Retry the request; the relay will split audio into smaller provider frames."
	case "insufficient_audio_activity":
		return "Keep streaming audio, including silence, for as long as the transcription session is open."
	default:
		return "Check the advertised media and request options, correct them, and retry."
	}
}

func sttErrorMessage(messageType, detail string) string {
	if strings.TrimSpace(detail) == "" {
		return "ElevenLabs reported a streaming " + messageType
	}
	return "ElevenLabs reported a streaming " + messageType + ": " + detail
}

func sttExtension(raw json.RawMessage) map[string]json.RawMessage {
	return map[string]json.RawMessage{extensionID: raw}
}

func sttTranscriptData(text string, final bool, words []sttWord) json.RawMessage {
	data := map[string]any{"text": text, "is_final": final, "speech_final": final}
	// Scribe tags each token with a type and only `word` entries carry meaningful
	// timings; `spacing` tokens would otherwise contribute zero-width ranges.
	sttSetWords(data, sttWordTimings(words))
	return sttMarshalData(data)
}

func sttAlignmentData(text string, words []sttWord) json.RawMessage {
	data := map[string]any{"text": text}
	sttSetWords(data, sttWordTimings(words))
	return sttMarshalData(data)
}

// sttSetWords adds the word timings and the span they cover: the first word's
// start and the last word's end, each left out when that word has no time.
func sttSetWords(data map[string]any, timings []map[string]any) {
	if len(timings) == 0 {
		return
	}
	data["words"] = timings
	if start, timed := timings[0]["start_ms"]; timed {
		data["audio_start_ms"] = start
	}
	if end, timed := timings[len(timings)-1]["end_ms"]; timed {
		data["audio_end_ms"] = end
	}
}

// sttSpokenWords keeps the tokens that are words.
func sttSpokenWords(words []sttWord) []sttWord {
	var spoken []sttWord
	for _, word := range words {
		if word.Type != "" && word.Type != "word" {
			continue
		}
		if strings.TrimSpace(word.Text) == "" {
			continue
		}
		spoken = append(spoken, word)
	}
	return spoken
}

// sttWordTimings is each word with its time in milliseconds. A word the vendor
// has not aligned has no time, which is not the same as time zero.
func sttWordTimings(words []sttWord) []map[string]any {
	var timings []map[string]any
	for _, word := range sttSpokenWords(words) {
		timing := map[string]any{"text": word.Text}
		if word.Start != nil {
			timing["start_ms"] = sttMilliseconds(*word.Start)
		}
		if word.End != nil {
			timing["end_ms"] = sttMilliseconds(*word.End)
		}
		timings = append(timings, timing)
	}
	return timings
}

func sttMarshalData(value any) json.RawMessage {
	payload, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{"code":"internal"}`)
	}
	return payload
}

func sttMilliseconds(seconds float64) int64 {
	return int64(math.Round(seconds * 1_000))
}

// sttWord is one token of a committed_transcript_with_timestamps or
// diarized_transcript frame. Start and End are SECONDS, and Type distinguishes
// `word` from `spacing`. The vendor's word schema makes Start and End
// optional and nullable. SpeakerID is set only on a diarized session, and is
// null while the vendor has not resolved the speaker.
type sttWord struct {
	Text      string   `json:"text"`
	Start     *float64 `json:"start"`
	End       *float64 `json:"end"`
	Type      string   `json:"type"`
	SpeakerID *string  `json:"speaker_id"`
}

type sttInboundMessage struct {
	MessageType string    `json:"message_type"`
	SessionID   string    `json:"session_id"`
	Text        string    `json:"text"`
	Error       string    `json:"error"`
	Words       []sttWord `json:"words"`
	// The fields below are sent only by a model with turn prediction
	// (TurnProbability) or on a diarized session (the rest).
	TurnProbability *float64 `json:"turn_probability"`
	SegmentIndex    int      `json:"segment_index"`
	FromSpeakerID   string   `json:"from_speaker_id"`
	ToSpeakerID     string   `json:"to_speaker_id"`
}
