package inworld

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

// Inworld Realtime STT, spoken over the bidirectional WebSocket resource. This
// is the STREAMING surface: interim transcripts arrive while audio is still
// being written. Inworld also publishes a synchronous twin,
// POST /stt/v1/transcribe, which takes one base64 audio blob and answers with a
// finished transcript. That one is deliberately not used here — a realtime
// pipeline cannot wait for a whole utterance.
//
// Every wire string below was transcribed from Inworld's own AsyncAPI document,
// fetched as raw source at https://docs.inworld.ai/api-reference/sttAPI/
// speechtotext/transcribe-stream-websocket.md (August 2026), and cross-checked
// against Inworld's published sample, stt/js/example_stt_websocket.js in
// github.com/inworld-ai/inworld-api-examples. The rendered API-reference page
// was not trusted for field names; the embedded document was.
//
//	URL   : wss://api.inworld.ai/stt/v1/transcribe:streamBidirectional. The
//	        colon is part of the path (a Google-style custom method), not a port.
//	auth  : `Authorization` REQUEST HEADER, both ways — see sttAuthorization.
//	send  : {"transcribeConfig":{...}} first, then {"audioChunk":{"content":
//	        "<base64>"}} per frame, {"endTurn":{}} to force a final, and
//	        {"closeStream":{}} to finish. Audio is base64 INSIDE a JSON text
//	        frame; this API never reads binary WebSocket frames.
//	recv  : every server message is wrapped in a top-level `result` object:
//	        result.transcription, result.usage, result.speechStarted,
//	        result.speechStopped.
const (
	// STTAdapterID is the identifier returned by an Inworld STT session plan.
	STTAdapterID = "inworld.stt.v1"

	// STTDefaultModel is Inworld's first-party streaming model, spelled the way
	// the platform and the benchmark board spell it. sttModelID qualifies it to
	// the `inworld/inworld-stt-1` the wire wants; see that function for why the
	// bare form is the one carried in a session plan.
	STTDefaultModel = "inworld-stt-1"

	// sttExtensionID namespaces raw STT frames on canonical events. It is
	// deliberately NOT the TTS package's extensionID ("inworld.ai/tts/v1"):
	// tagging a transcript with a synthesis namespace would make the raw payload
	// unparseable by anything keying off it.
	sttExtensionID = "inworld.ai/stt/v1"

	// sttStreamPath is the only streaming transcription resource.
	sttStreamPath = "/stt/v1/transcribe:streamBidirectional"

	// sttAudioEncoding is the one encoding the STREAMING endpoint accepts.
	// AUTO_DETECT, MP3, OGG_OPUS and FLAC are declared on the shared
	// AudioEncoding enum but the quickstart scopes streaming to LINEAR16 alone,
	// and the enum's own notes mark MP3/OGG_OPUS/FLAC "Not supported for
	// streaming transcription".
	sttAudioEncoding = "LINEAR16"

	// sttInworldModelPrefix is the provider segment of Inworld's
	// "{provider}/{model-name}" modelId format for its first-party model.
	sttInworldModelPrefix = "inworld/"

	// sttMaxHeaderScanBytes bounds the RIFF-header search in sttPCMGuard so a
	// stream that never contains a `data` tag cannot buffer without limit.
	sttMaxHeaderScanBytes = 4 << 10
)

// sttStreamingModels is the WebSocket-capable catalogue, spelled exactly as the
// `modelId` field expects. Held as an explicit set, like the TTS lineup, so a
// typo fails at Open instead of arriving as an upstream error mid-call.
//
// Inworld's STT API is a multi-vendor gateway: a plan routed to provider
// "inworld" may name a third-party model, and that is a real capability of this
// account rather than a mistake. The dedicated assemblyai/deepgram/soniox
// adapters in this repository reach those vendors directly with the customer's
// own key; these ids reach them through Inworld's key and billing.
var sttStreamingModels = map[string]struct{}{
	"inworld/inworld-stt-1":                       {},
	"assemblyai/universal-streaming-multilingual": {},
	"assemblyai/universal-streaming-english":      {},
	"assemblyai/u3-rt-pro":                        {},
	"assemblyai/whisper-rt":                       {},
	"soniox/stt-rt-v4":                            {},
	"soniox/stt-rt-v5":                            {},
	"deepgram/flux-general-en":                    {},
	"deepgram/flux-general-multi":                 {},
}

// sttSyncOnlyModels are documented on the STT API but NOT on this resource.
// Naming them separately turns "Sync API only" into an actionable rejection
// rather than a generic unsupported-model error.
var sttSyncOnlyModels = map[string]struct{}{
	"groq/whisper-large-v3": {},
}

// sttInworldLanguages is the documented 30-language hint vocabulary of the
// first-party model. Third-party models keep their own coverage — Inworld
// publishes no unified list for them — so this set is applied only to
// `inworld/inworld-stt-1`; see sttLanguageHint.
var sttInworldLanguages = map[string]struct{}{
	"ar": {}, "yue": {}, "zh": {}, "cs": {}, "da": {}, "nl": {},
	"en": {}, "fil": {}, "fi": {}, "fr": {}, "de": {}, "el": {},
	"hi": {}, "hu": {}, "id": {}, "it": {}, "ja": {}, "ko": {},
	"mk": {}, "ms": {}, "fa": {}, "pl": {}, "pt": {}, "ro": {},
	"ru": {}, "es": {}, "sv": {}, "th": {}, "tr": {}, "vi": {},
}

// Exact control frames. Written as literal bytes rather than marshalled from a
// map so the wire spelling is readable in one place and cannot drift with a
// struct rename.
var (
	sttEndTurnFrame     = []byte(`{"endTurn":{}}`)
	sttCloseStreamFrame = []byte(`{"closeStream":{}}`)
	sttWAVDataTag       = []byte("data")
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

// STTAdapter implements Inworld's /stt/v1/transcribe:streamBidirectional API.
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
		return nil, errors.New("inworld stt event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("inworld stt maximum message bytes must be positive")
	}
	// officialAPIHost is shared with the TTS adapter: both resources live on
	// api.inworld.ai. The policy itself is the WebSocket one from
	// internal/upstream, not the HTTPS variant tts.go had to restate locally.
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

// Open dials the bidirectional socket and sends the mandatory configuration
// frame. Inworld requires transcribeConfig to be the FIRST message: an
// unwrapped or missing config is rejected by closing the socket without an
// error frame, so sending it here — synchronously, before Open returns — is
// what turns that silent close into a reportable Open failure.
func (a *STTAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindSTT {
		return nil, fmt.Errorf("inworld stt supports stt sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "inworld" {
		return nil, fmt.Errorf("inworld stt adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	// The sibling TTS adapter is HTTP; this one is a socket. A plan that named
	// the other transport selected an adapter that cannot speak it.
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("inworld stt requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("inworld stt requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("inworld stt media: %w", err)
	}
	if err := validateSTTMedia(*request.Media); err != nil {
		return nil, err
	}
	modelID, err := sttModelID(request.Plan.Route.Model)
	if err != nil {
		return nil, err
	}
	language, err := sttLanguageHint(request.Options.Language, modelID)
	if err != nil {
		return nil, err
	}
	credential := request.Plan.Route.Credential
	if credential == nil || credential.Kind != protocol.CredentialBearer || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("inworld stt requires a bearer credential")
	}
	endpoint, err := sttEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}

	headers := make(http.Header)
	headers.Set("Authorization", sttAuthorization(request.Plan.Execution.CredentialSource, credential.Value))
	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{
		HTTPClient: httpClient(a.httpClient),
		HTTPHeader: headers,
	})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		return nil, &runtimepkg.ProviderError{
			Code:           sttDialErrorCode(status),
			Message:        "Inworld STT streaming connection could not be established",
			Retryable:      status == 0 || status == http.StatusTooManyRequests || status >= 500,
			ProviderStatus: status,
			Cause:          err,
		}
	}
	conn.SetReadLimit(a.maxMessageBytes)

	payload, err := json.Marshal(sttConfigFrame{TranscribeConfig: sttTranscribeConfig{
		ModelID:       modelID,
		AudioEncoding: sttAudioEncoding,
		// Both dimensions are sent unconditionally. Inworld can infer them from
		// a container header, and raw LINEAR16 has none — omitting them would
		// silently fall back to the documented 16 kHz mono defaults and
		// transcribe an 8 kHz telephony leg at the wrong rate.
		SampleRateHertz:  request.Media.SampleRateHz,
		NumberOfChannels: request.Media.Channels,
		Language:         language,
	}})
	if err != nil {
		_ = conn.CloseNow()
		return nil, err
	}
	if err := conn.Write(ctx, websocket.MessageText, payload); err != nil {
		_ = conn.CloseNow()
		return nil, &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "Inworld STT transcribe configuration could not be sent",
			Retryable: true,
			Cause:     err,
		}
	}

	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &sttStream{
		conn:   conn,
		ctx:    streamCtx,
		cancel: cancel,
		events: make(chan runtimepkg.ProviderEvent, a.eventBuffer),
	}
	go stream.readLoop()
	return stream, nil
}

// sttAuthorization picks the Inworld auth channel for the credential the plan
// carries. The two schemes are not interchangeable:
//
//   - BYOK: the customer's portal key is already the Base64 of "<key>:<secret>"
//     — the value Inworld's docs call $INWORLD_API_KEY. It is a Basic
//     credential. Inworld's own STT WebSocket sample sends exactly
//     `Authorization: Basic <key>`.
//   - Managed: the control plane mints a short-lived JWT at
//     POST /auth/v1/tokens/token:generate, whose response is typed
//     `"type": "Bearer"`. Inworld documents `Authorization: Bearer $JWT` as the
//     way to open a WebSocket with a minted token.
//
// Both travel in the request HEADER, and that is the deliberate choice here.
// This resource's AsyncAPI additionally declares a security scheme named
// `authorization` with `in: query` — the browser fallback, since a browser
// WebSocket cannot set headers. A server-side dial can, so the query channel
// buys nothing and costs the secret being written into every URL that reaches
// an access log. Unlike AssemblyAI and ElevenLabs STT, where the managed token
// is ONLY accepted in the query string, Inworld accepts both credentials in the
// header, so there is no correctness reason to split the two channels.
func sttAuthorization(source protocol.CredentialSource, value string) string {
	if source == protocol.CredentialsBYOK {
		return "Basic " + value
	}
	return "Bearer " + value
}

func sttEndpoint(policy upstream.WebSocketPolicy, rawEndpoint string) (string, error) {
	endpoint, err := policy.Parse(rawEndpoint)
	if err != nil {
		return "", fmt.Errorf("inworld stt endpoint: %w", err)
	}
	if endpoint.Path != sttStreamPath {
		return "", fmt.Errorf("inworld stt endpoint path must be %s, got %q", sttStreamPath, endpoint.Path)
	}
	// Deliberately NOT added: a Speko reservation marker. Deepgram has an
	// `extra` passthrough parameter for that; Inworld's transcribeConfig has no
	// equivalent, and an invented field would be dropped, making the metering
	// hint a lie.
	return endpoint.String(), nil
}

// sttModelID resolves a session plan's model onto Inworld's wire `modelId`,
// which is always "{provider}/{model-name}".
//
// A bare name is qualified with `inworld/`, because that is the form the
// platform's own client and the STT benchmark board carry — they name the model
// and let the adapter own the vendor prefix. An already-qualified id is used
// verbatim, which is how a plan reaches one of the third-party models Inworld
// resells; qualifying it again would produce `inworld/deepgram/...`.
func sttModelID(model string) (string, error) {
	model = strings.TrimSpace(model)
	if model == "" || model == "auto" {
		return "", errors.New("inworld stt requires a concrete model in the session plan")
	}
	qualified := model
	if !strings.Contains(qualified, "/") {
		qualified = sttInworldModelPrefix + qualified
	}
	if _, syncOnly := sttSyncOnlyModels[qualified]; syncOnly {
		return "", fmt.Errorf("inworld stt model %q is available on the synchronous transcribe endpoint only", qualified)
	}
	if _, ok := sttStreamingModels[qualified]; !ok {
		return "", fmt.Errorf("inworld stt does not support model %q", qualified)
	}
	return qualified, nil
}

// validateSTTMedia enforces the intersection of the canonical protocol media
// format and what the streaming resource decodes. LINEAR16 is documented as
// "uncompressed 16-bit signed little-endian samples", which is exactly
// pcm_s16le; opus is refused rather than gambled on, because declaring an
// encoding the vendor does not decode produces a healthy-looking session full
// of garbage instead of an error.
//
// No sample-rate whitelist is applied. The TTS resource enumerates its accepted
// sampleRateHertz values, but the STT schema documents only a default of 16000
// and a recommendation, so inventing a list here would refuse configurations
// that work. protocol.MediaFormat.Validate already bounds the range.
func validateSTTMedia(media protocol.MediaFormat) error {
	if media.Encoding != "pcm_s16le" {
		return fmt.Errorf("inworld stt requires pcm_s16le, got %q", media.Encoding)
	}
	return nil
}

// sttLanguageHint normalizes the caller's tag to the base subtag Inworld stores.
//
// `language` is a hint, not a constraint: the model auto-detects and may switch
// mid-stream. It is still worth getting right, because on the first-party model
// the hint also pins the OUTPUT SCRIPT for en/zh/yue/ja/ko/ru/hi — `en` keeps a
// foreign name romanized rather than in its native script — so a hint that is
// dropped changes the transcript, not just the ranking.
//
// Inworld says BCP-47 is accepted and converted to the base ISO 639 code. The
// conversion is done locally instead so the value on the wire is the value that
// was validated, and validation runs against the documented 30-language set
// only for `inworld/inworld-stt-1`. Third-party models keep their own coverage
// and Inworld publishes no unified list, so their hints pass through.
func sttLanguageHint(language, modelID string) (string, error) {
	lowered := strings.ToLower(strings.TrimSpace(language))
	if lowered == "" {
		return "", nil
	}
	if index := strings.IndexAny(lowered, "-_"); index > 0 {
		lowered = lowered[:index]
	}
	if modelID != sttInworldModelPrefix+STTDefaultModel {
		return lowered, nil
	}
	if _, ok := sttInworldLanguages[lowered]; !ok {
		return "", fmt.Errorf("inworld stt does not support language %q", language)
	}
	return lowered, nil
}

type sttStream struct {
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

	// turnPending is true once speech has been seen whose final has not landed.
	// Only then does Close force a turn: an endTurn with nothing in flight just
	// produces another empty marker.
	turnPending atomic.Bool

	// guard holds RIFF-stripping state. The runtime serializes WriteAudio,
	// control methods and Close for one session, so it needs no lock of its own.
	guard sttPCMGuard
}

func (s *sttStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

// WriteAudio forwards one PCM frame as {"audioChunk":{"content":"<base64>"}}.
//
// This API never reads binary WebSocket frames: the AsyncAPI types `content` as
// a protobuf `bytes` field, whose JSON encoding is standard padded base64, and
// Inworld's own sample sends Buffer.toString('base64'). Sending the PCM as a
// binary frame instead would be ignored, and the session would look healthy
// while transcribing silence.
func (s *sttStream) WriteAudio(ctx context.Context, audio []byte) error {
	if len(audio) == 0 {
		return errors.New("inworld stt audio is empty")
	}
	pcm := s.guard.trim(audio)
	if len(pcm) == 0 {
		// The whole frame was container header, or the guard is still waiting
		// for enough bytes to decide. Nothing to send yet.
		return nil
	}
	payload, err := json.Marshal(sttAudioChunkFrame{AudioChunk: sttAudioChunk{
		Content: base64.StdEncoding.EncodeToString(pcm),
	}})
	if err != nil {
		return err
	}
	return s.write(ctx, payload)
}

// CommitAudio forces the pending turn to finalize with {"endTurn":{}}.
//
// Inworld does NOT commit a turn on socket close, and its automatic end-of-turn
// detector waits for a sustained pause — so a push-to-talk or benchmark caller
// asking for the transcript now has no other way to get it. endTurn is the
// documented manual turn boundary and is what the platform's TypeScript client
// sends at end-of-input.
func (s *sttStream) CommitAudio(ctx context.Context) error {
	return s.write(ctx, sttEndTurnFrame)
}

// AppendText and CommitText belong to synthesis. A transcriber rejects them so
// the runtime reports the mismatch instead of silently dropping caller text.
func (s *sttStream) AppendText(context.Context, string) error {
	return runtimepkg.ErrUnsupportedOperation
}

func (s *sttStream) CommitText(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// Cancel discards the session. Unlike Close it must NOT force a final: the
// caller no longer wants the transcript.
func (s *sttStream) Cancel(context.Context) error { return s.abort() }

// Abort immediately tears down the socket after a terminal runtime failure.
func (s *sttStream) Abort(context.Context) error { return s.abort() }

// Close ends the session gracefully: force a still-pending turn to commit, then
// send closeStream.
//
// Order matters. Dropping the socket with a turn in flight loses the caller's
// last utterance, which is the most visible failure a transcriber can have —
// and Inworld will not commit it on its own. closeStream is the documented
// end-of-input signal for HTTP/WebSocket clients, which have no equivalent of a
// gRPC half-close. The read loop deliberately stays live afterwards so the
// trailing final and the usage frame still reach the consumer; it ends when
// Inworld closes the socket. Nothing here blocks waiting for that, because a
// teardown that waits on a provider is a teardown that hangs.
func (s *sttStream) Close(ctx context.Context) error {
	s.gracefulOnce.Do(func() {
		s.closing.Store(true)
		if s.turnPending.Load() {
			if err := s.write(ctx, sttEndTurnFrame); err != nil {
				s.closeErr = err
			}
		}
		if s.closeErr == nil {
			if err := s.write(ctx, sttCloseStreamFrame); err != nil {
				s.closeErr = err
			}
		}
		if s.closeErr != nil {
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

func (s *sttStream) write(ctx context.Context, payload []byte) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if err := s.conn.Write(ctx, websocket.MessageText, payload); err != nil {
		return &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "Inworld STT streaming write failed",
			Retryable: true,
			Cause:     err,
		}
	}
	return nil
}

func (s *sttStream) readLoop() {
	defer func() {
		s.closed.Store(true)
		s.cancel()
		close(s.events)
	}()
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && !s.closing.Load() && s.ctx.Err() == nil && !sttIsNormalClose(err) {
				s.emit(runtimepkg.ProviderEvent{Err: sttReadError(err)})
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

func (s *sttStream) handleMessage(payload []byte) error {
	var message sttInbound
	if err := json.Unmarshal(payload, &message); err != nil {
		return &runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "Inworld STT sent malformed streaming JSON",
			Retryable: true,
			Cause:     err,
		}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))
	// The AsyncAPI declares no error frame for this channel, but the service is
	// a gRPC transcoding gateway — its REST twin returns `{"error":{code,
	// message}}` with a gRPC status code, and the schema notes that a rejected
	// `prompts` value comes back as INVALID_ARGUMENT (code 3). Handling that
	// object is what keeps a rejected configuration from looking like an
	// unexplained hang.
	if message.Error != nil {
		return sttStreamError(message.Error, raw)
	}
	if message.Result == nil {
		return s.emitWarning("ignored Inworld STT message without a result", raw)
	}
	switch {
	case message.Result.Transcription != nil:
		return s.handleTranscription(*message.Result.Transcription, raw)
	case message.Result.SpeechStarted != nil:
		s.turnPending.Store(true)
		return s.emit(runtimepkg.ProviderEvent{
			Type: protocol.EventSpeechStarted,
			Data: marshalData(map[string]any{
				"audio_start_ms": message.Result.SpeechStarted.StartTimeMs,
				"confidence":     message.Result.SpeechStarted.Confidence,
			}),
			Extensions: sttExtension(raw),
		})
	case message.Result.SpeechStopped != nil:
		// Voice activity stopped. This is NOT the turn boundary: Inworld emits
		// it on any detected silence, and the transcript for the turn may still
		// be revised, so turnPending is left alone.
		return s.emit(runtimepkg.ProviderEvent{
			Type: protocol.EventSpeechEnded,
			Data: marshalData(map[string]any{
				"reason":              "speech_stopped",
				"silence_duration_ms": message.Result.SpeechStopped.SilenceDurationMs,
			}),
			Extensions: sttExtension(raw),
		})
	case message.Result.Usage != nil:
		return s.emit(runtimepkg.ProviderEvent{
			Type: protocol.EventUsageObserved,
			Data: marshalData(map[string]any{
				"audio_duration_ms": message.Result.Usage.TranscribedAudioMs,
				"model_id":          message.Result.Usage.ModelID,
			}),
			Extensions: sttExtension(raw),
		})
	default:
		return s.emitWarning("ignored unrecognized Inworld STT result", raw)
	}
}

// handleTranscription maps one transcription frame onto transcript events.
//
// The empty-text guard is load-bearing, not defensive tidiness. Inworld emits an
// empty final marker at the START of a turn — isFinal true, transcript "" —
// before any speech has been recognized. Forwarding it fires a transcript.final
// with no text, which a consumer reads as a completed empty utterance and, in a
// voice agent, answers. The platform's TypeScript client drops the same frame.
func (s *sttStream) handleTranscription(transcription sttTranscription, raw json.RawMessage) error {
	text := strings.TrimSpace(transcription.Transcript)
	if text == "" {
		return nil
	}
	kind := protocol.EventTranscriptDelta
	if transcription.IsFinal {
		kind = protocol.EventTranscriptFinal
	}
	s.turnPending.Store(!transcription.IsFinal)
	data := map[string]any{
		"text":                text,
		"is_final":            transcription.IsFinal,
		"silence_duration_ms": transcription.SilenceDurationMs,
	}
	// Word timings and voice profile pass through verbatim rather than being
	// reshaped: neither is populated for the first-party model today, and
	// inventing a canonical shape from an unexercised payload would be a guess.
	if len(transcription.WordTimestamps) > 0 {
		data["words"] = transcription.WordTimestamps
	}
	if len(transcription.VoiceProfile) > 0 && !bytes.Equal(transcription.VoiceProfile, []byte("null")) {
		data["voice_profile"] = transcription.VoiceProfile
	}
	if err := s.emit(runtimepkg.ProviderEvent{
		Type:       kind,
		Data:       marshalData(data),
		Extensions: sttExtension(raw),
	}); err != nil {
		return err
	}
	if !transcription.IsFinal {
		return nil
	}
	return s.emit(runtimepkg.ProviderEvent{
		Type:       protocol.EventSpeechEnded,
		Data:       marshalData(map[string]any{"reason": "end_of_turn"}),
		Extensions: sttExtension(raw),
	})
}

// emitWarning surfaces a frame this adapter does not model instead of dropping
// it, so a message type Inworld adds later is visible rather than invisible.
func (s *sttStream) emitWarning(message string, raw json.RawMessage) error {
	return s.emit(runtimepkg.ProviderEvent{
		Type:       protocol.EventWarning,
		Data:       marshalData(map[string]any{"message": message}),
		Extensions: sttExtension(raw),
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

// sttPCMGuard strips a leading RIFF/WAVE container from the head of an input
// stream.
//
// Inworld's LINEAR16 decoder treats a container header delivered as audio as
// fatal: the stream goes quiet — zero partials, no final — rather than
// erroring. The platform hit this because WebSocket preserves frame boundaries,
// so a 44-byte RIFF header can arrive as its own chunk (measured: 0 usable
// transcripts in 20 attempts). The canonical MediaFormat here promises raw
// pcm_s16le, so this should never fire; it exists because the failure it
// prevents is silent, and a silent transcriber is indistinguishable from a
// working one until someone reads the output.
type sttPCMGuard struct {
	done bool
	head []byte
}

// trim returns the PCM to forward. An empty result means "nothing yet" — either
// the bytes so far were all container header, or the guard needs more of them
// before it can tell.
func (g *sttPCMGuard) trim(audio []byte) []byte {
	if g.done {
		return audio
	}
	g.head = append(g.head, audio...)
	if len(g.head) < 4 {
		return nil // not enough bytes to read the magic
	}
	if !bytes.Equal(g.head[0:4], []byte("RIFF")) {
		return g.release(g.head) // raw PCM: forward untouched
	}
	// PCM starts 8 bytes past the `data` subchunk tag (tag + length). The tag is
	// located rather than assumed at offset 36, because the format permits extra
	// fmt/LIST chunks ahead of it.
	index := -1
	if len(g.head) > 12 {
		if at := bytes.Index(g.head[12:], sttWAVDataTag); at >= 0 {
			index = 12 + at
		}
	}
	if index < 0 {
		if len(g.head) < sttMaxHeaderScanBytes {
			return nil // header still arriving
		}
		return g.release(g.head) // pathological: no `data` in 4 KiB, send as-is
	}
	if len(g.head) < index+8 {
		return nil // wait for the length field
	}
	return g.release(g.head[index+8:])
}

func (g *sttPCMGuard) release(pcm []byte) []byte {
	g.done = true
	// pcm aliases head, so it is copied before head is dropped.
	out := append([]byte(nil), pcm...)
	g.head = nil
	return out
}

func sttIsNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}

// sttReadError classifies a read failure. Inworld documents no close-code
// vocabulary for this channel and declares no error frame, so a close is not
// over-interpreted here: the code is left to the wrapped websocket error, which
// stringifies it, and the classification stays provider_unavailable/retryable.
// ProviderStatus is reserved for real HTTP statuses — the handshake — so that a
// consumer reading it never has to guess whether 1011 is an HTTP status.
func sttReadError(err error) *runtimepkg.ProviderError {
	return &runtimepkg.ProviderError{
		Code:      "provider_unavailable",
		Message:   "Inworld STT streaming read failed",
		Retryable: true,
		Cause:     err,
	}
}

// sttStreamError maps an in-band error object. rpcStatus is shared with the TTS
// adapter because it is Inworld's error envelope for the whole API, not a
// synthesis detail. Its `code` is a gRPC status, NOT an HTTP status, so it is
// carried in Extensions rather than ProviderStatus.
func sttStreamError(status *rpcStatus, raw json.RawMessage) *runtimepkg.ProviderError {
	code := "invalid_request"
	retryable := false
	switch status.Code {
	case 7, 16: // PERMISSION_DENIED, UNAUTHENTICATED
		code = "authentication_failed"
	case 8: // RESOURCE_EXHAUSTED
		code, retryable = "provider_rate_limited", true
	case 2, 4, 13, 14: // UNKNOWN, DEADLINE_EXCEEDED, INTERNAL, UNAVAILABLE
		code, retryable = "provider_unavailable", true
	}
	message := "Inworld reported a transcription error"
	if strings.TrimSpace(status.Message) != "" {
		message += ": " + status.Message
	}
	return &runtimepkg.ProviderError{Code: code, Message: message, Retryable: retryable, Extensions: sttExtension(raw)}
}

func sttDialErrorCode(status int) string {
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

func sttExtension(raw json.RawMessage) map[string]json.RawMessage {
	return map[string]json.RawMessage{sttExtensionID: raw}
}

// sttTranscribeConfig mirrors the transcribeConfig schema. Names are the
// lowerCamelCase forms the AsyncAPI declares; the platform's TypeScript client
// sends snake_case for some Inworld payloads, which this resource does not read.
type sttTranscribeConfig struct {
	ModelID          string `json:"modelId"`
	AudioEncoding    string `json:"audioEncoding"`
	SampleRateHertz  int    `json:"sampleRateHertz"`
	NumberOfChannels int    `json:"numberOfChannels"`
	// Language is optional: with no hint Inworld auto-detects and may follow a
	// speaker who switches language mid-stream.
	Language string `json:"language,omitempty"`
}

// sttConfigFrame and sttAudioChunkFrame carry the mandatory top-level wrapper.
// An unwrapped config is rejected by closing the socket without an error frame.
type sttConfigFrame struct {
	TranscribeConfig sttTranscribeConfig `json:"transcribeConfig"`
}

type sttAudioChunk struct {
	Content string `json:"content"`
}

type sttAudioChunkFrame struct {
	AudioChunk sttAudioChunk `json:"audioChunk"`
}

// sttInbound is the union of every server frame. All four documented messages
// arrive wrapped in a top-level `result`; `error` is the transcoding gateway's
// out-of-band envelope and sits beside it.
type sttInbound struct {
	Result *sttResult `json:"result"`
	Error  *rpcStatus `json:"error"`
}

type sttResult struct {
	Transcription *sttTranscription `json:"transcription"`
	Usage         *sttUsage         `json:"usage"`
	SpeechStarted *sttSpeechStarted `json:"speechStarted"`
	SpeechStopped *sttSpeechStopped `json:"speechStopped"`
}

type sttTranscription struct {
	Transcript        string          `json:"transcript"`
	IsFinal           bool            `json:"isFinal"`
	WordTimestamps    json.RawMessage `json:"wordTimestamps"`
	VoiceProfile      json.RawMessage `json:"voiceProfile"`
	SilenceDurationMs int64           `json:"silenceDurationMs"`
}

type sttUsage struct {
	TranscribedAudioMs int64  `json:"transcribedAudioMs"`
	ModelID            string `json:"modelId"`
}

type sttSpeechStarted struct {
	StartTimeMs int64   `json:"startTimeMs"`
	Confidence  float64 `json:"confidence"`
}

type sttSpeechStopped struct {
	SilenceDurationMs int64 `json:"silenceDurationMs"`
}

// Compile-time proof that the stream satisfies both runtime contracts.
var (
	_ runtimepkg.ProviderStream         = (*sttStream)(nil)
	_ runtimepkg.AbortingProviderStream = (*sttStream)(nil)
	_ runtimepkg.Adapter                = (*STTAdapter)(nil)
)
