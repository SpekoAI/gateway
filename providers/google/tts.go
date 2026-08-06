package google

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// AdapterID is the identifier a Google Cloud Text-to-Speech session plan
	// must name in PlanRoute.Adapter.
	AdapterID = "google.tts.v1"

	// extensionID namespaces the raw vendor payload attached to canonical
	// events. Google's v1 SynthesizeSpeechResponse has exactly one documented
	// member (audioContent), so on success this carries a compact synthesis
	// descriptor plus any *undocumented* trailing members the service returned;
	// on failure it carries Google's google.rpc.Status error object verbatim,
	// which is the payload that actually matters when debugging a 400.
	extensionID = "texttospeech.googleapis.com/v1"

	// DefaultModel is the Speko-side label for the Chirp 3: HD family.
	//
	// It is deliberately NOT serialized. The voice name is what selects the
	// model on this API ("<locale>-Chirp3-HD-<Voice>"), and every published
	// request example carries only languageCode and name. The v1 discovery
	// document does expose an optional voice.modelName ("The name of the model.
	// If set, the service will choose the model matching the specified
	// configuration"), but its accepted values are documented only for the
	// Gemini-TTS models (gemini-2.5-flash-tts, gemini-2.5-pro-tts) and never
	// for Chirp. Sending "chirp-3-hd" there would be a guess that 400s.
	DefaultModel = "chirp-3-hd"

	// officialAPIHost is the global Cloud Text-to-Speech service endpoint from
	// the published discovery document (rootUrl).
	officialAPIHost = "texttospeech.googleapis.com"

	// synthesizePath is the only synthesis path the REST surface exposes. Both
	// the v1 and v1beta1 discovery documents list exactly three POST methods and
	// none of them stream; see the streaming note on Adapter.
	synthesizePath = "/v1/text:synthesize"

	// audioEncodingLinear16 is the exact enum string from the v1 discovery
	// document, whose AudioConfig.audioEncoding enum is
	// [AUDIO_ENCODING_UNSPECIFIED LINEAR16 MP3 OGG_OPUS MULAW ALAW PCM M4A].
	//
	// LINEAR16 is chosen because it is the documented default for Chirp 3: HD
	// and is accepted by every voice family on the unary method. PCM is the
	// headerless twin ("as opposed to LINEAR16, audio won't be wrapped in a WAV
	// (or any other) header") but the Chirp 3: HD page lists it only for that
	// one family, and an encoding that 400s on a Neural2 voice is exactly the
	// silent bug this adapter must not ship.
	//
	// The cost of LINEAR16 is the container: the response schema says "For
	// LINEAR16 audio, we include the WAV header", so the RIFF wrapper is
	// stripped back off to yield the raw pcm_s16le this gateway's MediaFormat
	// promises. Concatenating unstripped chunks would splice header bytes into
	// the PCM stream and produce a click at every join.
	audioEncodingLinear16 = "LINEAR16"

	// maxInputBytes mirrors the published quota "Total bytes per request:
	// 5,000". It is BYTES, not characters: Google's own note is that one
	// character in ja-JP (or hi-IN) is several bytes, so counting runes would
	// let a Devanagari utterance sail past the real limit and 400 upstream.
	maxInputBytes = 5_000

	// maxWAVHeaderBytes bounds the prefix buffered while locating the RIFF
	// "data" chunk. Real Cloud TTS headers are 44 bytes; anything past this is
	// a malformed container, not a header.
	maxWAVHeaderBytes = 4 << 10

	// maxJSONEnvelopeBytes bounds the non-audio parts of a streamed success
	// response (everything before `"audioContent":"` and everything after the
	// closing quote).
	maxJSONEnvelopeBytes = 8 << 10

	defaultEventBuffer      = 32
	defaultMaxFrameBytes    = 32 << 10
	defaultMaxResponseBytes = 32 << 20
	defaultRequestTimeout   = 60 * time.Second

	// googleAPIKeyPrefix is the observed prefix of a Google Cloud API key. An
	// API key sent as "Authorization: Bearer" is rejected by Google with a bare
	// 401, which reads identically to an expired OAuth token. Catching it here
	// turns a confusing auth failure into a precise one.
	googleAPIKeyPrefix = "AIza"
)

// regionalAPIHosts are the documented data-residency endpoints. Chirp 3: HD is
// served from the us and eu multi-region endpoints; us-central1 is a
// single-region endpoint that carries Neural2 voices only.
var regionalAPIHosts = []string{
	"us-texttospeech.googleapis.com",
	"eu-texttospeech.googleapis.com",
	"us-central1-texttospeech.googleapis.com",
}

// audioContentMember is the JSON member holding the base64 audio. It is scanned
// for incrementally so frames can be emitted while the body is still arriving.
var audioContentMember = []byte(`"audioContent"`)

// voiceLocalePattern extracts the BCP-47 locale that prefixes every Google
// voice name ("hi-IN-Chirp3-HD-Charon" -> hi-IN). Region is either two letters
// or a three-digit UN M.49 code ("es-419").
var voiceLocalePattern = regexp.MustCompile(`^([A-Za-z]{2,3})-([A-Za-z]{2}|[0-9]{3})(?:-|$)`)

// languageTagPattern accepts a bare language ("hi") or a full tag ("hi-IN").
// The bare form is accepted only as a cross-check against a voice that already
// names its region; it is never sent on the wire.
var languageTagPattern = regexp.MustCompile(`^([A-Za-z]{2,3})(?:[-_]([A-Za-z]{2}|[0-9]{3}))?$`)

// Config controls local transport bounds. Provider identity, model, voice,
// language, and the access token all come from a verified session plan.
type Config struct {
	AdapterID  string
	HTTPClient *http.Client
	// EventBuffer bounds the canonical event channel.
	EventBuffer int
	// MaxFrameBytes bounds one emitted audio.frame. It is rounded down to an
	// even number so an s16le sample is never split across two frames.
	MaxFrameBytes int
	// MaxResponseBytes bounds a single synthesis response body.
	MaxResponseBytes int64
	// RequestTimeout bounds one synthesis POST.
	RequestTimeout time.Duration
	// QuotaProject populates the x-goog-user-project header. Google's own curl
	// examples send it alongside the bearer token so usage bills to the
	// caller's project rather than the token's home project. There is no plan
	// field carrying a GCP project id, so it is deployment configuration.
	QuotaProject          string
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// Adapter implements Google Cloud Text-to-Speech over its REST surface.
//
// # Why HTTP and not streaming
//
// Cloud Text-to-Speech does expose a streaming synthesis method,
// TextToSpeech.StreamingSynthesize, documented as "Performs bidirectional
// streaming speech synthesis: receives audio while sending text". It is
// bidirectional-streaming gRPC and has no HTTP/JSON binding: the live v1 and
// v1beta1 discovery documents list only voices.list, text:synthesize,
// :synthesizeLongAudio, and the operations methods. Bidirectional streams
// cannot be transcoded to REST at all, so no `alt=sse` escape hatch exists
// either. Reaching it would require gRPC framing plus generated protobuf
// message types — new Go module dependencies this build does not take.
//
// The adapter therefore performs one POST and re-streams its response. That is
// still incremental: the body is parsed and base64-decoded as it arrives rather
// than buffered whole, so audio.frame events begin flowing before the upstream
// response is complete. Framing is local, not wire-level; time-to-first-frame
// is bounded by Google's own time-to-first-byte, not by full synthesis.
type Adapter struct {
	id               string
	httpClient       *http.Client
	eventBuffer      int
	maxFrameBytes    int
	maxResponseBytes int64
	requestTimeout   time.Duration
	quotaProject     string
	endpointPolicy   endpointPolicy
}

// New creates a bounded Google Cloud Text-to-Speech adapter.
func New(config Config) (*Adapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = AdapterID
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = defaultEventBuffer
	}
	if config.MaxFrameBytes == 0 {
		config.MaxFrameBytes = defaultMaxFrameBytes
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = defaultMaxResponseBytes
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = defaultRequestTimeout
	}
	if config.EventBuffer < 1 {
		return nil, errors.New("google tts event buffer must be positive")
	}
	if config.MaxFrameBytes < 2 {
		return nil, errors.New("google tts maximum frame bytes must hold one s16le sample")
	}
	if config.MaxResponseBytes < 1 {
		return nil, errors.New("google tts maximum response bytes must be positive")
	}
	if config.RequestTimeout < 0 {
		return nil, errors.New("google tts request timeout must not be negative")
	}
	policy, err := newEndpointPolicy(append([]string{officialAPIHost}, regionalAPIHosts...), config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &Adapter{
		id:               config.AdapterID,
		httpClient:       config.HTTPClient,
		eventBuffer:      config.EventBuffer,
		maxFrameBytes:    config.MaxFrameBytes &^ 1,
		maxResponseBytes: config.MaxResponseBytes,
		requestTimeout:   config.RequestTimeout,
		quotaProject:     strings.TrimSpace(config.QuotaProject),
		endpointPolicy:   policy,
	}, nil
}

func (a *Adapter) ID() string { return a.id }

// Open validates the plan and prepares a synthesis stream. No network call is
// made here: the unary REST method has no session to establish, so the first
// request happens on CommitText.
func (a *Adapter) Open(_ context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindTTS {
		return nil, fmt.Errorf("google tts supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "google" {
		return nil, fmt.Errorf("google tts adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportHTTP {
		return nil, fmt.Errorf("google tts requires http transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("google tts requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("google tts media: %w", err)
	}
	if request.Media.Encoding != "pcm_s16le" || request.Media.Channels != 1 {
		return nil, fmt.Errorf("google tts produces mono pcm_s16le, got %s/%d channels", request.Media.Encoding, request.Media.Channels)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model == "" || model == "auto" {
		return nil, errors.New("google tts requires a concrete model in the session plan")
	}
	voice, err := resolveVoice(request)
	if err != nil {
		return nil, err
	}
	languageCode, err := resolveLanguageCode(voice, request.Options.Language)
	if err != nil {
		return nil, err
	}
	endpoint, err := a.endpointPolicy.parse(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}
	headers, err := authorizationHeaders(request, a.quotaProject)
	if err != nil {
		return nil, err
	}

	streamCtx, cancel := context.WithCancel(context.Background())
	return &stream{
		ctx:              streamCtx,
		cancel:           cancel,
		events:           make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		httpClient:       requestHTTPClient(a.httpClient),
		endpoint:         endpoint,
		headers:          headers,
		model:            model,
		voice:            voice,
		languageCode:     languageCode,
		sampleRateHertz:  request.Media.SampleRateHz,
		maxFrameBytes:    a.maxFrameBytes,
		maxResponseBytes: a.maxResponseBytes,
		requestTimeout:   a.requestTimeout,
	}, nil
}

func requestHTTPClient(client *http.Client) *http.Client {
	if client != nil {
		return client
	}
	return http.DefaultClient
}

// authorizationHeaders builds the request headers for both credential sources.
//
// Managed and BYOK deliberately share one code path because Google offers one
// documented header mechanism for this API: "Authorization: Bearer
// <access-token>". The two sources differ only in who minted the token (a
// control-plane service-account impersonation versus the customer's own
// gcloud/ADC credential), never in how it travels. Manufacturing a second
// channel would be fiction.
//
// The one thing this adapter will not do is put the secret in the URL. Google
// also accepts an API key via the `key` query parameter or the X-Goog-Api-Key
// header, but a query-string credential lands in proxy and server access logs,
// and the protocol has no api_key CredentialKind to distinguish one anyway.
func authorizationHeaders(request runtimepkg.AdapterRequest, quotaProject string) (http.Header, error) {
	credential := request.Plan.Route.Credential
	if credential == nil || credential.Kind != protocol.CredentialBearer {
		return nil, errors.New("google tts requires a bearer credential")
	}
	token := strings.TrimSpace(credential.Value)
	if token == "" {
		return nil, errors.New("google tts requires a non-empty bearer credential")
	}
	if strings.HasPrefix(token, googleAPIKeyPrefix) {
		return nil, errors.New("google tts credential looks like an API key; this adapter sends OAuth access tokens in the Authorization header")
	}
	switch request.Plan.Execution.CredentialSource {
	case protocol.CredentialsManaged, protocol.CredentialsBYOK:
	default:
		return nil, fmt.Errorf("google tts cannot use credential source %q", request.Plan.Execution.CredentialSource)
	}
	headers := make(http.Header, 4)
	headers.Set("Authorization", "Bearer "+token)
	// Google's own examples send the charset; the API is strict about the type.
	headers.Set("Content-Type", "application/json; charset=utf-8")
	headers.Set("Accept", "application/json")
	if quotaProject != "" {
		headers.Set("X-Goog-User-Project", quotaProject)
	}
	return headers, nil
}

// resolveVoice prefers the caller's voice and falls back to the control
// plane's choice. Cloud TTS will pick a voice for a bare languageCode, but that
// choice is not stable across releases, so this adapter insists on a concrete
// name — a benchmark board that cannot name its voice cannot reproduce a score.
func resolveVoice(request runtimepkg.AdapterRequest) (string, error) {
	if voice := strings.TrimSpace(request.Options.Voice); voice != "" {
		return voice, nil
	}
	if voice := strings.TrimSpace(request.Plan.Route.Voice); voice != "" {
		return voice, nil
	}
	return "", errors.New("google tts requires a voice name, for example hi-IN-Chirp3-HD-Charon")
}

// resolveLanguageCode produces the BCP-47 tag sent as voice.languageCode.
//
// Google requires a language tag that may include a region ("en-US"); for the
// curated Chirp voices the region is not optional in practice — Hindi is
// hi-IN, Tamil ta-IN, Telugu te-IN, and a bare "hi" does not select them. The
// voice name already carries its locale as a prefix, so the voice is treated as
// authoritative and RequestOptions.Language is a cross-check: a caller that
// says "hi" against a hi-IN voice is agreeing, a caller that says "en-US" is
// contradicting and gets an error rather than a silently wrong voice.
func resolveLanguageCode(voice, requested string) (string, error) {
	requested = strings.TrimSpace(requested)
	voiceLanguage, voiceRegion, voiceOK := localeFromVoice(voice)
	if voiceOK {
		if requested != "" {
			language, region, ok := parseLanguageTag(requested)
			if !ok {
				return "", fmt.Errorf("google tts language %q is not a BCP-47 language tag", requested)
			}
			if language != voiceLanguage || (region != "" && region != voiceRegion) {
				return "", fmt.Errorf("google tts language %q contradicts voice %q", requested, voice)
			}
		}
		return voiceLanguage + "-" + voiceRegion, nil
	}
	if requested == "" {
		return "", fmt.Errorf("google tts voice %q carries no locale prefix, so request options must supply a language code", voice)
	}
	language, region, ok := parseLanguageTag(requested)
	if !ok {
		return "", fmt.Errorf("google tts language %q is not a BCP-47 language tag", requested)
	}
	if region == "" {
		return "", fmt.Errorf("google tts language %q needs a region subtag, for example hi-IN rather than hi", requested)
	}
	return language + "-" + region, nil
}

func localeFromVoice(voice string) (language, region string, ok bool) {
	match := voiceLocalePattern.FindStringSubmatch(voice)
	if match == nil {
		return "", "", false
	}
	return strings.ToLower(match[1]), canonicalRegion(match[2]), true
}

func parseLanguageTag(tag string) (language, region string, ok bool) {
	match := languageTagPattern.FindStringSubmatch(tag)
	if match == nil {
		return "", "", false
	}
	return strings.ToLower(match[1]), canonicalRegion(match[2]), true
}

// canonicalRegion uppercases an alphabetic region and leaves a numeric UN M.49
// region ("419") alone. Google's own docs show both "en-US" and the lowercase
// "en-us" form; the service is case-insensitive but the canonical tag is what
// gets logged and compared, so it is normalized here.
func canonicalRegion(region string) string {
	if region == "" {
		return ""
	}
	if region[0] >= '0' && region[0] <= '9' {
		return region
	}
	return strings.ToUpper(region)
}

// endpointPolicy is the HTTP counterpart of upstream.WebSocketPolicy. The
// shared helper in internal/upstream only validates wss endpoints, and this
// package must not edit shared code, so the same rules are restated here for
// https. Consolidating the two belongs in internal/upstream.
type endpointPolicy struct {
	hosts         map[string]struct{}
	allowInsecure bool
}

func newEndpointPolicy(officialHosts, additionalHosts []string, allowInsecure bool) (endpointPolicy, error) {
	hosts := make(map[string]struct{}, len(officialHosts)+len(additionalHosts))
	for _, host := range append(append([]string{}, officialHosts...), additionalHosts...) {
		host = strings.ToLower(strings.TrimSpace(host))
		if host == "" || strings.ContainsAny(host, "/:@?#") {
			return endpointPolicy{}, errors.New("google tts allowed endpoint host is invalid")
		}
		hosts[host] = struct{}{}
	}
	return endpointPolicy{hosts: hosts, allowInsecure: allowInsecure}, nil
}

// parse validates scheme, host, port, userinfo, query, and path before a
// customer-owned access token is attached to the request.
func (p endpointPolicy) parse(raw string) (string, error) {
	endpoint, err := url.Parse(raw)
	if err != nil || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.Fragment != "" || endpoint.RawQuery != "" {
		return "", errors.New("google tts endpoint must be a clean absolute https URL")
	}
	if endpoint.Scheme != "https" && !(p.allowInsecure && endpoint.Scheme == "http") {
		return "", errors.New("google tts endpoint must use https")
	}
	if !p.allowInsecure && endpoint.Port() != "" && endpoint.Port() != "443" {
		return "", errors.New("google tts endpoint uses a non-standard port")
	}
	if _, ok := p.hosts[strings.ToLower(endpoint.Hostname())]; !ok {
		return "", errors.New("google tts endpoint host is not allowed")
	}
	if endpoint.Path != synthesizePath {
		return "", fmt.Errorf("google tts endpoint path must be %s, got %q", synthesizePath, endpoint.Path)
	}
	return endpoint.String(), nil
}

// synthesizeRequest is the documented SynthesizeSpeechRequest body. The three
// members are siblings at the top level; nesting audioConfig inside voice (or
// using the proto snake_case names, which the JSON surface also tolerates but
// which no other adapter here uses) is the shape to avoid.
type synthesizeRequest struct {
	Input       synthesisInput `json:"input"`
	Voice       voiceSelection `json:"voice"`
	AudioConfig audioConfig    `json:"audioConfig"`
}

type synthesisInput struct {
	Text string `json:"text"`
}

type voiceSelection struct {
	LanguageCode string `json:"languageCode"`
	Name         string `json:"name"`
}

type audioConfig struct {
	AudioEncoding   string `json:"audioEncoding"`
	SampleRateHertz int    `json:"sampleRateHertz"`
}

// apiError is google.rpc.Status as rendered by the JSON surface.
type apiError struct {
	Error struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Status  string `json:"status"`
	} `json:"error"`
}

type stream struct {
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent

	httpClient       *http.Client
	endpoint         string
	headers          http.Header
	model            string
	voice            string
	languageCode     string
	sampleRateHertz  int
	maxFrameBytes    int
	maxResponseBytes int64
	requestTimeout   time.Duration

	inflight     sync.WaitGroup
	shutdownOnce sync.Once
	closeErr     error

	stateMu          sync.Mutex
	buffer           strings.Builder
	closed           bool
	requestCancel    context.CancelFunc
	requestCancelled bool
}

func (s *stream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

// WriteAudio and CommitAudio are input-audio operations; a TTS session has none.
func (s *stream) WriteAudio(context.Context, []byte) error {
	return runtimepkg.ErrUnsupportedOperation
}

func (s *stream) CommitAudio(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// AppendText buffers a fragment. The unary method takes one whole utterance, so
// nothing reaches Google until CommitText.
func (s *stream) AppendText(_ context.Context, text string) error {
	if strings.TrimSpace(text) == "" {
		return errors.New("google tts text is empty")
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return runtimepkg.ErrSessionClosed
	}
	if s.requestCancel != nil {
		return errors.New("google tts previous utterance has not completed")
	}
	if s.buffer.Len()+len(text) > maxInputBytes {
		return &runtimepkg.ProviderError{
			Code:           "input_too_large",
			Message:        "Google Cloud Text-to-Speech accepts at most 5000 bytes of input per request",
			Retryable:      false,
			ProviderStatus: http.StatusRequestEntityTooLarge,
		}
	}
	s.buffer.WriteString(text)
	return nil
}

// CommitText posts the buffered utterance and streams the response back as
// canonical events. It returns as soon as the request is accepted locally;
// audio.started, audio.frame, and audio.done arrive on Events, and a synthesis
// failure arrives as a terminal ProviderEvent.Err, matching how the WebSocket
// adapters surface upstream faults.
func (s *stream) CommitText(context.Context) error {
	text, utteranceID, requestCtx, err := s.startRequest()
	if err != nil {
		return err
	}
	go func() {
		defer s.inflight.Done()
		terminal := s.synthesize(requestCtx, text, utteranceID)
		// Release the in-flight slot BEFORE the terminal event becomes visible.
		// A consumer that reacts to audio.done (or to a cancellation warning) by
		// starting the next utterance must not lose the race against this
		// goroutine's own cleanup.
		s.finishRequest()
		if terminal != nil {
			_ = s.emit(*terminal)
		}
	}()
	return nil
}

// startRequest claims the single in-flight slot. The WaitGroup counter is
// incremented under stateMu, and shutdown sets closed under the same lock
// before waiting, so a request can never be registered after the event channel
// has been closed.
func (s *stream) startRequest() (string, string, context.Context, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return "", "", nil, runtimepkg.ErrSessionClosed
	}
	if s.requestCancel != nil {
		return "", "", nil, errors.New("google tts previous utterance has not completed")
	}
	if s.buffer.Len() == 0 {
		return "", "", nil, errors.New("google tts has no buffered text to synthesize")
	}
	utteranceID, err := newUtteranceID()
	if err != nil {
		return "", "", nil, err
	}
	text := s.buffer.String()
	s.buffer.Reset()

	requestCtx, cancel := context.WithCancel(s.ctx)
	if s.requestTimeout > 0 {
		requestCtx, cancel = context.WithTimeout(s.ctx, s.requestTimeout)
	}
	s.requestCancel = cancel
	s.requestCancelled = false
	s.inflight.Add(1)
	return text, utteranceID, requestCtx, nil
}

func (s *stream) finishRequest() {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.requestCancel != nil {
		s.requestCancel()
		s.requestCancel = nil
	}
	s.requestCancelled = false
}

func (s *stream) wasCancelled() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.requestCancelled
}

// Cancel abandons the in-flight synthesis and drops anything still buffered.
// It is not terminal: the session stays usable for another utterance, which is
// how Deepgram's Clear behaves and what a barge-in needs.
func (s *stream) Cancel(context.Context) error {
	s.stateMu.Lock()
	cancel := s.requestCancel
	buffered := s.buffer.Len() > 0
	s.buffer.Reset()
	if cancel != nil {
		s.requestCancelled = true
	}
	s.stateMu.Unlock()

	if cancel == nil && !buffered {
		return runtimepkg.ErrSessionClosed
	}
	if cancel != nil {
		cancel()
	}
	return nil
}

// Close waits for an in-flight synthesis so a caller that closes immediately
// after CommitText still receives its final audio, then closes the event
// channel. A ctx deadline bounds that wait.
func (s *stream) Close(ctx context.Context) error {
	s.shutdownOnce.Do(func() {
		s.markClosed()
		drained := make(chan struct{})
		go func() { s.inflight.Wait(); close(drained) }()
		select {
		case <-drained:
		case <-ctx.Done():
			s.closeErr = ctx.Err()
		}
		s.cancel()
		s.inflight.Wait()
		close(s.events)
	})
	return s.closeErr
}

// Abort tears the session down immediately after a terminal runtime failure:
// the in-flight HTTP request is cancelled through its context rather than
// drained.
func (s *stream) Abort(context.Context) error { return s.abort() }

func (s *stream) abort() error {
	s.shutdownOnce.Do(func() {
		s.markClosed()
		s.cancel()
		s.inflight.Wait()
		close(s.events)
	})
	return s.closeErr
}

func (s *stream) markClosed() {
	s.stateMu.Lock()
	s.closed = true
	s.stateMu.Unlock()
}

// synthesize performs the single POST and returns the utterance's TERMINAL
// event (audio.done, a cancellation warning, or a ProviderEvent carrying Err).
// Intermediate events are emitted as they are produced; the caller emits the
// terminal one after releasing the in-flight slot. A nil return means the
// stream context ended and nothing more should be published.
func (s *stream) synthesize(ctx context.Context, text, utteranceID string) *runtimepkg.ProviderEvent {
	body, err := json.Marshal(synthesizeRequest{
		Input: synthesisInput{Text: text},
		Voice: voiceSelection{LanguageCode: s.languageCode, Name: s.voice},
		AudioConfig: audioConfig{
			AudioEncoding:   audioEncodingLinear16,
			SampleRateHertz: s.sampleRateHertz,
		},
	})
	if err != nil {
		return errorEvent(&runtimepkg.ProviderError{Code: "invalid_request", Message: "Google TTS request could not be encoded", Cause: err})
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(body))
	if err != nil {
		return errorEvent(&runtimepkg.ProviderError{Code: "invalid_request", Message: "Google TTS request could not be built", Cause: err})
	}
	httpRequest.Header = s.headers.Clone()

	response, err := s.httpClient.Do(httpRequest)
	if err != nil {
		if s.wasCancelled() {
			return cancelledEvent(utteranceID)
		}
		return errorEvent(&runtimepkg.ProviderError{
			Code:      "provider_unavailable",
			Message:   "Google TTS request could not be delivered",
			Retryable: true,
			Cause:     err,
		})
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<16))
		_ = response.Body.Close()
	}()

	if requestID := providerRequestID(response.Header); requestID != "" {
		if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: usageData(requestID)}); err != nil {
			return nil
		}
	}
	if response.StatusCode != http.StatusOK {
		return errorEvent(s.statusError(response))
	}
	return s.streamAudio(ctx, response, utteranceID)
}

// streamAudio walks the response body once, decoding base64 audio into frames
// as bytes arrive rather than buffering the whole utterance first. This is what
// keeps the adapter incremental on a request/response transport: the first
// audio.frame is published as soon as enough of Google's body has landed, not
// after the whole utterance has been received.
func (s *stream) streamAudio(ctx context.Context, response *http.Response, utteranceID string) *runtimepkg.ProviderEvent {
	reader := io.LimitReader(response.Body, s.maxResponseBytes+1)
	scanner := &audioContentScanner{}
	decoder := &base64StreamDecoder{}
	stripper := &wavStripper{}

	var (
		pending      []byte
		total        int64
		audioStarted bool
		read         int64
		chunk        = make([]byte, 32<<10)
	)

	// emitFrames drains whole frames out of pending. Frames are capped at
	// maxFrameBytes and that cap is even, so an s16le sample never straddles
	// two events.
	emitFrames := func(final bool) error {
		for len(pending) >= s.maxFrameBytes || (final && len(pending) > 0) {
			size := min(len(pending), s.maxFrameBytes)
			if !audioStarted {
				if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: s.utteranceData(utteranceID)}); err != nil {
					return err
				}
				audioStarted = true
			}
			frame := make([]byte, size)
			copy(frame, pending[:size])
			pending = pending[size:]
			if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Data: s.utteranceData(utteranceID), Audio: frame}); err != nil {
				return err
			}
		}
		return nil
	}

	for {
		n, readErr := reader.Read(chunk)
		if n > 0 {
			read += int64(n)
			if read > s.maxResponseBytes {
				return errorEvent(&runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Google TTS response exceeded the configured size limit", Retryable: false})
			}
			encoded, scanErr := scanner.write(chunk[:n])
			if scanErr != nil {
				return errorEvent(scanErr)
			}
			if len(encoded) > 0 {
				decoded, decodeErr := decoder.write(encoded)
				if decodeErr != nil {
					return errorEvent(decodeErr)
				}
				payload, stripErr := stripper.write(decoded)
				if stripErr != nil {
					return errorEvent(stripErr)
				}
				if len(payload) > 0 {
					pending = append(pending, payload...)
					total += int64(len(payload))
					if err := emitFrames(false); err != nil {
						return nil
					}
				}
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			if ctx.Err() != nil && s.wasCancelled() {
				return cancelledEvent(utteranceID)
			}
			return errorEvent(&runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Google TTS response could not be read", Retryable: true, Cause: readErr})
		}
	}

	remainder, flushErr := decoder.flush()
	if flushErr != nil {
		return errorEvent(flushErr)
	}
	if len(remainder) > 0 {
		payload, stripErr := stripper.write(remainder)
		if stripErr != nil {
			return errorEvent(stripErr)
		}
		pending = append(pending, payload...)
		total += int64(len(payload))
	}
	if !scanner.complete() {
		return errorEvent(&runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Google TTS response contained no audioContent member", Retryable: true})
	}
	if err := emitFrames(true); err != nil {
		return nil
	}
	if total == 0 {
		// Ported from the platform adapter: a 200 carrying no audio is a real
		// Cloud TTS failure mode and must not look like a silent success.
		return errorEvent(&runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Google TTS returned no audio", Retryable: true})
	}
	return &runtimepkg.ProviderEvent{
		Type:       protocol.EventAudioDone,
		Data:       s.utteranceData(utteranceID),
		Extensions: successExtension(scanner.trailer(), total),
	}
}

func (s *stream) statusError(response *http.Response) *runtimepkg.ProviderError {
	raw, _ := io.ReadAll(io.LimitReader(response.Body, maxJSONEnvelopeBytes))
	message := "Google TTS rejected the synthesis request"
	var payload apiError
	if err := json.Unmarshal(raw, &payload); err == nil && strings.TrimSpace(payload.Error.Message) != "" {
		message = message + ": " + payload.Error.Message
	}
	providerErr := &runtimepkg.ProviderError{
		Code:           statusErrorCode(response.StatusCode),
		Message:        message,
		Retryable:      retryableStatus(response.StatusCode),
		ProviderStatus: response.StatusCode,
	}
	if json.Valid(raw) {
		providerErr.Extensions = map[string]json.RawMessage{extensionID: json.RawMessage(append([]byte(nil), raw...))}
	}
	return providerErr
}

func statusErrorCode(status int) string {
	switch {
	case status == http.StatusUnauthorized, status == http.StatusForbidden:
		return "authentication_failed"
	case status == http.StatusTooManyRequests:
		return "provider_rate_limited"
	case status >= 500:
		return "provider_unavailable"
	case status >= 400:
		return "invalid_request"
	default:
		return "provider_unavailable"
	}
}

func retryableStatus(status int) bool {
	return status == http.StatusTooManyRequests || status >= 500
}

// providerRequestID reads whichever correlation header the Google frontend
// attached. Neither is contractual for this API, so a missing header simply
// means no usage.observed event rather than an error.
func providerRequestID(header http.Header) string {
	for _, name := range []string{"X-Goog-Request-Id", "X-Request-Id"} {
		if value := strings.TrimSpace(header.Get(name)); value != "" {
			return value
		}
	}
	return ""
}

func (s *stream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func errorEvent(err *runtimepkg.ProviderError) *runtimepkg.ProviderEvent {
	return &runtimepkg.ProviderEvent{Err: err}
}

// cancelledEvent reports a caller-initiated abandon as a warning rather than a
// terminal Err: Cancel exists so a barge-in can be followed by another
// utterance, and an Err would tear the session down instead.
func cancelledEvent(utteranceID string) *runtimepkg.ProviderEvent {
	return &runtimepkg.ProviderEvent{
		Type: protocol.EventWarning,
		Data: marshalData(map[string]any{"code": "provider_request_cancelled", "utterance_id": utteranceID}),
	}
}

func (s *stream) utteranceData(utteranceID string) json.RawMessage {
	return marshalData(map[string]any{"utterance_id": utteranceID, "voice": s.voice, "language_code": s.languageCode, "model": s.model})
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

// successExtension records what the vendor actually returned. v1's response
// object has one documented member and it is the audio itself, which is already
// delivered as real bytes, so echoing it here would duplicate the utterance in
// memory. Any *other* member the service returns is preserved verbatim.
func successExtension(trailer []byte, audioBytes int64) map[string]json.RawMessage {
	fields := map[string]json.RawMessage{
		"audio_encoding": json.RawMessage(`"` + audioEncodingLinear16 + `"`),
		"audio_bytes":    json.RawMessage(fmt.Sprintf("%d", audioBytes)),
	}
	var extra map[string]json.RawMessage
	if err := json.Unmarshal(trailerObject(trailer), &extra); err == nil {
		for key, value := range extra {
			fields[key] = value
		}
	}
	payload, err := json.Marshal(fields)
	if err != nil {
		return nil
	}
	return map[string]json.RawMessage{extensionID: payload}
}

// trailerObject rebuilds the response members that followed audioContent into a
// standalone object. Best effort: Cloud TTS v1 emits audioContent first and
// alone, so the common result is "{}".
func trailerObject(trailer []byte) []byte {
	rest := bytes.TrimSpace(trailer)
	rest = bytes.TrimPrefix(rest, []byte(","))
	return append([]byte("{"), rest...)
}

func newUtteranceID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate Google TTS utterance id: %w", err)
	}
	return hex.EncodeToString(value), nil
}

// audioContentScanner pulls the base64 audio out of the JSON envelope without
// materializing the envelope. Base64 uses none of the characters JSON escapes,
// so the first quote after the value starts is unambiguously its terminator.
type audioContentScanner struct {
	head     []byte
	trail    []byte
	inValue  bool
	finished bool
}

func (s *audioContentScanner) write(chunk []byte) ([]byte, *runtimepkg.ProviderError) {
	if s.finished {
		if len(s.trail)+len(chunk) > maxJSONEnvelopeBytes {
			return nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Google TTS response envelope is implausibly large", Retryable: false}
		}
		s.trail = append(s.trail, chunk...)
		return nil, nil
	}
	if s.inValue {
		if index := bytes.IndexByte(chunk, '"'); index >= 0 {
			s.finished = true
			s.trail = append(s.trail, chunk[index+1:]...)
			return chunk[:index], nil
		}
		return chunk, nil
	}

	s.head = append(s.head, chunk...)
	index := bytes.Index(s.head, audioContentMember)
	if index < 0 || index > maxJSONEnvelopeBytes {
		// Only the bytes BEFORE the audio member count as envelope. Checking
		// len(head) instead would reject a perfectly normal response whose
		// first read happened to carry the member plus a large slab of base64.
		if len(s.head) > maxJSONEnvelopeBytes {
			return nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Google TTS response envelope is implausibly large", Retryable: false}
		}
		return nil, nil
	}
	cursor := index + len(audioContentMember)
	for cursor < len(s.head) && isJSONSpace(s.head[cursor]) {
		cursor++
	}
	if cursor >= len(s.head) {
		return nil, nil
	}
	if s.head[cursor] != ':' {
		return nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Google TTS response is not a JSON object", Retryable: true}
	}
	cursor++
	for cursor < len(s.head) && isJSONSpace(s.head[cursor]) {
		cursor++
	}
	if cursor >= len(s.head) {
		return nil, nil
	}
	if s.head[cursor] != '"' {
		return nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Google TTS audioContent is not a JSON string", Retryable: true}
	}
	s.inValue = true
	remainder := s.head[cursor+1:]
	s.head = s.head[:index]
	return s.write(remainder)
}

func (s *audioContentScanner) complete() bool { return s.finished }

func (s *audioContentScanner) trailer() []byte { return s.trail }

func isJSONSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

// base64StreamDecoder decodes a base64 string in arbitrary slices, carrying at
// most three characters between writes. Proto3 JSON mandates standard base64,
// but the base64url alphabet is accepted defensively because it costs two
// substitutions and a mis-decode would be silent garbage audio.
type base64StreamDecoder struct {
	carry []byte
}

func (d *base64StreamDecoder) write(chunk []byte) ([]byte, *runtimepkg.ProviderError) {
	buffer := append(d.carry, chunk...)
	usable := len(buffer) - len(buffer)%4
	d.carry = append([]byte(nil), buffer[usable:]...)
	if usable == 0 {
		return nil, nil
	}
	return decodeBase64(buffer[:usable])
}

func (d *base64StreamDecoder) flush() ([]byte, *runtimepkg.ProviderError) {
	if len(d.carry) == 0 {
		return nil, nil
	}
	remainder := d.carry
	d.carry = nil
	return decodeBase64(remainder)
}

func decodeBase64(encoded []byte) ([]byte, *runtimepkg.ProviderError) {
	normalized := bytes.ReplaceAll(bytes.ReplaceAll(encoded, []byte("-"), []byte("+")), []byte("_"), []byte("/"))
	decoded := make([]byte, base64.StdEncoding.DecodedLen(len(normalized)))
	n, err := base64.StdEncoding.Decode(decoded, normalized)
	if err != nil {
		return nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Google TTS returned audio that is not valid base64", Retryable: true, Cause: err}
	}
	return decoded[:n], nil
}

// wavStripper removes the RIFF/WAVE container LINEAR16 responses are wrapped
// in, incrementally, so frames can still be emitted while the body arrives. It
// walks the chunk list rather than assuming the canonical 44-byte header,
// because a writer is free to insert LIST or fact chunks before "data".
type wavStripper struct {
	header    []byte
	streaming bool
}

func (w *wavStripper) write(chunk []byte) ([]byte, *runtimepkg.ProviderError) {
	if w.streaming {
		return chunk, nil
	}
	w.header = append(w.header, chunk...)
	if len(w.header) < 12 {
		return nil, nil
	}
	if !bytes.Equal(w.header[0:4], []byte("RIFF")) || !bytes.Equal(w.header[8:12], []byte("WAVE")) {
		// Already headerless (a PCM-encoded response, or a fixture). Pass it
		// straight through rather than corrupting the first samples.
		w.streaming = true
		payload := w.header
		w.header = nil
		return payload, nil
	}
	cursor := 12
	for {
		if cursor+8 > len(w.header) {
			if cursor+8 > maxWAVHeaderBytes {
				return nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Google TTS returned a WAV header with no data chunk", Retryable: true}
			}
			return nil, nil
		}
		id := w.header[cursor : cursor+4]
		size := int64(binary.LittleEndian.Uint32(w.header[cursor+4 : cursor+8]))
		if bytes.Equal(id, []byte("data")) {
			w.streaming = true
			payload := append([]byte(nil), w.header[cursor+8:]...)
			w.header = nil
			return payload, nil
		}
		// RIFF chunks are word aligned: an odd size is followed by a pad byte.
		next := int64(cursor) + 8 + size + (size & 1)
		if next <= int64(cursor) || next > maxWAVHeaderBytes {
			return nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Google TTS returned a malformed WAV header", Retryable: true}
		}
		cursor = int(next)
	}
}
