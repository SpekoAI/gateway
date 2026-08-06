package playht

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

const (
	// AdapterID is the identifier returned by a PlayHT TTS session plan.
	AdapterID   = "playht.tts.v1"
	extensionID = "play.ht/v1"

	// defaultAuthURL is PlayHT's documented WebSocket auth endpoint. It is an
	// HTTPS endpoint, not a WebSocket one: it exchanges an account credential
	// for expiring per-model WebSocket URLs.
	defaultAuthURL = "https://api.play.ht/api/v4/websocket-auth"

	// officialWebSocketHost is where PlayHT's returned synthesis URLs live.
	// It is deliberately NOT the auth host (api.play.ht): PlayHT fronts realtime
	// inference on a separate domain, so the endpoint allowlist must name it.
	officialWebSocketHost = "ws.fal.run"

	// defaultVoiceEngine is the lowest-latency engine PlayHT supports on the
	// WebSocket API, which is the only reason a caller would choose streaming.
	defaultVoiceEngine = "Play3.0-mini"

	// defaultOutputFormat asks for headerless audio. PlayHT prefixes every
	// non-raw, non-mulaw stream with a container header chunk; raw is the only
	// documented format that starts on a sample boundary.
	defaultOutputFormat = "raw"

	// maxAuthResponseBytes bounds the auth response so a hostile or broken
	// endpoint cannot force unbounded allocation before the socket is dialled.
	maxAuthResponseBytes = 1 << 20
)

// supportedVoiceEngines is the exact set PlayHT's own SDK permits on the
// WebSocket API. Engines outside it (for example PlayDialog-turbo) are HTTP
// only, so accepting one here would produce a confusing upstream failure
// instead of a clear local rejection.
var supportedVoiceEngines = map[string]struct{}{
	"Play3.0-mini":           {},
	"PlayDialog":             {},
	"PlayDialogMultilingual": {},
	"PlayDialogArabic":       {},
}

// playHTLanguages maps BCP-47 primary subtags onto PlayHT's language names.
// PlayHT does not accept BCP-47: its language parameter takes full lowercase
// English names ("english", "mandarin"). Every value below is one of the names
// PlayHT's SDK enumerates; an unmapped tag is omitted rather than guessed, so
// the voice and engine pick the language instead of a rejected request.
var playHTLanguages = map[string]string{
	"af": "afrikaans", "sq": "albanian", "am": "amharic", "ar": "arabic",
	"bn": "bengali", "bg": "bulgarian", "ca": "catalan", "hr": "croatian",
	"cs": "czech", "da": "danish", "nl": "dutch", "en": "english",
	"fr": "french", "gl": "galician", "de": "german", "el": "greek",
	"he": "hebrew", "iw": "hebrew", "hi": "hindi", "hu": "hungarian",
	"id": "indonesian", "in": "indonesian", "it": "italian", "ja": "japanese",
	"ko": "korean", "ms": "malay", "zh": "mandarin", "pl": "polish",
	"pt": "portuguese", "ru": "russian", "sr": "serbian", "es": "spanish",
	"sv": "swedish", "tl": "tagalog", "fil": "tagalog", "th": "thai",
	"tr": "turkish", "uk": "ukrainian", "ur": "urdu", "xh": "xhosa",
}

// Config controls local transport limits and the PlayHT auth endpoint.
// Provider identity, voice engine, voice, and credentials come from a session
// plan and its provider-neutral request options.
type Config struct {
	AdapterID string
	// AuthURL overrides PlayHT's WebSocket auth endpoint. It must be absolute
	// and HTTPS unless AllowInsecureEndpoint is set for tests.
	AuthURL               string
	OutputFormat          string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxMessageBytes       int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// Adapter implements PlayHT's two-step realtime TTS flow.
type Adapter struct {
	id              string
	authURL         string
	outputFormat    string
	httpClient      *http.Client
	eventBuffer     int
	maxMessageBytes int64
	allowInsecure   bool
	endpointPolicy  upstream.WebSocketPolicy
}

// New creates a bounded PlayHT TTS adapter.
func New(config Config) (*Adapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = AdapterID
	}
	if config.AuthURL == "" {
		config.AuthURL = defaultAuthURL
	}
	if config.OutputFormat == "" {
		config.OutputFormat = defaultOutputFormat
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = 32
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = 1 << 20
	}
	if config.EventBuffer < 1 {
		return nil, errors.New("playht event buffer must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("playht maximum message bytes must be positive")
	}
	authURL, err := url.Parse(strings.TrimSpace(config.AuthURL))
	if err != nil || authURL.Host == "" || authURL.User != nil {
		return nil, errors.New("playht auth url must be a clean absolute URL")
	}
	if authURL.Scheme != "https" && !(config.AllowInsecureEndpoint && authURL.Scheme == "http") {
		return nil, errors.New("playht auth url must use https")
	}
	endpointPolicy, err := upstream.NewWebSocketPolicy(officialWebSocketHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &Adapter{
		id:              config.AdapterID,
		authURL:         authURL.String(),
		outputFormat:    config.OutputFormat,
		httpClient:      config.HTTPClient,
		eventBuffer:     config.EventBuffer,
		maxMessageBytes: config.MaxMessageBytes,
		allowInsecure:   config.AllowInsecureEndpoint,
		endpointPolicy:  endpointPolicy,
	}, nil
}

func (a *Adapter) ID() string { return a.id }

// Open resolves a PlayHT synthesis WebSocket and connects to it. BYOK plans
// spend the customer's account credential on the auth endpoint here; managed
// plans receive a URL the control plane already minted and dial it directly.
func (a *Adapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindTTS {
		return nil, fmt.Errorf("playht supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "playht" {
		return nil, fmt.Errorf("playht adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("playht requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("playht requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("playht media: %w", err)
	}
	if err := validateGenerationOptions(request.Plan.Route.Model, request.Options, *request.Media); err != nil {
		return nil, err
	}
	credential := request.Plan.Route.Credential
	if credential == nil || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("playht requires a credential")
	}

	endpoint, expiresAt, err := a.resolveSessionURL(ctx, request, *credential)
	if err != nil {
		return nil, err
	}

	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: httpClient(a.httpClient)})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		return nil, &runtimepkg.ProviderError{
			Code: dialErrorCode(status),
			// The URL embeds a session JWT, so it must never reach this message.
			Message:        "PlayHT streaming connection could not be established",
			Retryable:      status == 0 || status == http.StatusTooManyRequests || status >= 500,
			ProviderStatus: status,
			Cause:          err,
		}
	}
	conn.SetReadLimit(a.maxMessageBytes)
	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &stream{
		conn:               conn,
		ctx:                streamCtx,
		cancel:             cancel,
		events:             make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		voiceEngine:        request.Plan.Route.Model,
		voiceID:            request.Options.Voice,
		language:           playHTLanguage(request.Options.Language),
		outputFormat:       a.outputFormat,
		media:              *request.Media,
		maxInputCharacters: request.Options.MaxInputCharacters,
		credentialExpiry:   expiresAt,
	}
	go stream.readLoop()
	return stream, nil
}

// resolveSessionURL produces the exact wss URL to dial for this plan.
func (a *Adapter) resolveSessionURL(ctx context.Context, request runtimepkg.AdapterRequest, credential protocol.DelegatedCredential) (string, time.Time, error) {
	if request.Plan.Execution.CredentialSource == protocol.CredentialsManaged {
		// Managed plans never hand the adapter an account key. The control
		// plane already spent one on PlayHT's auth endpoint and delegated the
		// resulting expiring URL, which is precisely what session_url means.
		if credential.Kind != protocol.CredentialSessionURL {
			return "", time.Time{}, fmt.Errorf("playht managed plans require a %q credential holding a pre-minted websocket url, got %q", protocol.CredentialSessionURL, credential.Kind)
		}
		endpoint, err := a.validateSessionURL(credential.Value)
		return endpoint, credential.ExpiresAt, err
	}
	if credential.Kind != protocol.CredentialBearer {
		return "", time.Time{}, fmt.Errorf("playht byok plans require a %q credential, got %q", protocol.CredentialBearer, credential.Kind)
	}
	userID, apiKey, err := splitAccountCredential(credential.Value)
	if err != nil {
		return "", time.Time{}, err
	}
	return a.mintSessionURL(ctx, userID, apiKey, request.Plan.Route.Model)
}

// mintSessionURL performs PlayHT's auth step: POST the account credential and
// receive expiring per-engine WebSocket URLs.
func (a *Adapter) mintSessionURL(ctx context.Context, userID, apiKey, voiceEngine string) (string, time.Time, error) {
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, a.authURL, nil)
	if err != nil {
		return "", time.Time{}, &runtimepkg.ProviderError{Code: "invalid_request", Message: "PlayHT auth request could not be built", Cause: err}
	}
	httpRequest.Header.Set("Authorization", "Bearer "+apiKey)
	httpRequest.Header.Set("X-User-Id", userID)
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "application/json")

	response, err := httpClient(a.httpClient).Do(httpRequest)
	if err != nil {
		return "", time.Time{}, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "PlayHT auth endpoint could not be reached", Retryable: true, Cause: err}
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxAuthResponseBytes))
		_ = response.Body.Close()
	}()

	payload, err := io.ReadAll(io.LimitReader(response.Body, maxAuthResponseBytes))
	if err != nil {
		return "", time.Time{}, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "PlayHT auth response could not be read", Retryable: true, ProviderStatus: response.StatusCode, Cause: err}
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
		return "", time.Time{}, &runtimepkg.ProviderError{
			Code:           authErrorCode(response.StatusCode),
			Message:        "PlayHT rejected the websocket auth request",
			Retryable:      response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500,
			ProviderStatus: response.StatusCode,
			Extensions:     extension(json.RawMessage(append([]byte(nil), payload...))),
		}
	}

	rawURL, expiresAt, err := parseAuthResponse(payload, voiceEngine)
	if err != nil {
		return "", time.Time{}, err
	}
	endpoint, err := a.validateSessionURL(rawURL)
	return endpoint, expiresAt, err
}

// parseAuthResponse tolerates every response shape PlayHT is known to serve.
// The vendor's docs and its own SDK disagree on the layout, so keying off one
// of them would break the moment an account is served the other.
func parseAuthResponse(payload []byte, voiceEngine string) (string, time.Time, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return "", time.Time{}, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "PlayHT sent a malformed auth response", Retryable: true, Cause: err}
	}

	var expiresAt time.Time
	if raw, ok := envelope["expires_at"]; ok {
		var value string
		if json.Unmarshal(raw, &value) == nil {
			// A cosmetic parse failure must not cost a working session, so the
			// zero time simply means "PlayHT did not tell us in a form we read".
			if parsed, err := time.Parse(time.RFC3339, value); err == nil {
				expiresAt = parsed
			}
		}
	}

	// Documented shape: {"websocket_urls": {"<engine>": "wss://..."}}.
	if raw, ok := envelope["websocket_urls"]; ok {
		var byEngine map[string]string
		if err := json.Unmarshal(raw, &byEngine); err == nil {
			if endpoint := strings.TrimSpace(byEngine[voiceEngine]); endpoint != "" {
				return endpoint, expiresAt, nil
			}
			return "", time.Time{}, &runtimepkg.ProviderError{
				Code:    "invalid_request",
				Message: fmt.Sprintf("PlayHT returned no websocket url for voice engine %q", voiceEngine),
			}
		}
	}

	// SDK shape: {"<engine>": {"websocket_url": "wss://...", ...}}.
	if raw, ok := envelope[voiceEngine]; ok {
		var coordinates struct {
			WebSocketURL string `json:"websocket_url"`
		}
		if err := json.Unmarshal(raw, &coordinates); err == nil && strings.TrimSpace(coordinates.WebSocketURL) != "" {
			return strings.TrimSpace(coordinates.WebSocketURL), expiresAt, nil
		}
	}

	// Legacy shape: a single flat {"websocket_url": "wss://..."}.
	if raw, ok := envelope["websocket_url"]; ok {
		var endpoint string
		if err := json.Unmarshal(raw, &endpoint); err == nil && strings.TrimSpace(endpoint) != "" {
			return strings.TrimSpace(endpoint), expiresAt, nil
		}
	}

	return "", time.Time{}, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "PlayHT auth response contained no websocket url", Retryable: true}
}

// validateSessionURL host-checks a PlayHT URL without discarding its token.
//
// upstream.WebSocketPolicy.Parse refuses any endpoint carrying a query string,
// but PlayHT delivers its per-session JWT *in* the query (?fal_jwt_token=...).
// Validating the query-free base keeps every security property the policy
// exists for -- scheme, host allowlist, port, userinfo, fragment -- and then
// the vendor's own query is restored for the dial.
func (a *Adapter) validateSessionURL(raw string) (string, error) {
	endpoint, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || endpoint.Host == "" {
		return "", &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "PlayHT returned an unusable websocket url"}
	}
	base := *endpoint
	base.RawQuery = ""
	if _, err := a.endpointPolicy.Parse(base.String()); err != nil {
		return "", fmt.Errorf("playht websocket url: %w", err)
	}
	return endpoint.String(), nil
}

// splitAccountCredential unpacks PlayHT's two-part account credential.
//
// PlayHT authenticates with an API key AND a user id, but a BYOK
// LocalCredential is a single opaque string, so the pair travels as
// "<user_id>:<api_key>". Neither component contains a colon.
func splitAccountCredential(value string) (string, string, error) {
	userID, apiKey, found := strings.Cut(strings.TrimSpace(value), ":")
	userID, apiKey = strings.TrimSpace(userID), strings.TrimSpace(apiKey)
	if !found || userID == "" || apiKey == "" {
		return "", "", errors.New(`playht byok credential must be "<user_id>:<api_key>"`)
	}
	return userID, apiKey, nil
}

func httpClient(client *http.Client) *http.Client {
	if client != nil {
		return client
	}
	return http.DefaultClient
}

func validateGenerationOptions(voiceEngine string, options protocol.RequestOptions, media protocol.MediaFormat) error {
	voiceEngine = strings.TrimSpace(voiceEngine)
	if voiceEngine == "" || voiceEngine == "auto" {
		return errors.New("playht requires a concrete voice engine in the session plan")
	}
	if _, ok := supportedVoiceEngines[voiceEngine]; !ok {
		return fmt.Errorf("playht voice engine %q does not support the websocket api", voiceEngine)
	}
	if strings.TrimSpace(options.Voice) == "" {
		return errors.New("playht requires a voice id in request options")
	}
	if media.Encoding != "pcm_s16le" {
		return fmt.Errorf("playht streaming output requires pcm_s16le, got %q", media.Encoding)
	}
	if media.Channels != 1 {
		return fmt.Errorf("playht streaming output is mono, got %d channels", media.Channels)
	}
	return nil
}

// playHTLanguage converts a BCP-47 tag into a PlayHT language name, returning
// empty when the tag has no known equivalent so the field can be omitted.
func playHTLanguage(tag string) string {
	tag = strings.TrimSpace(strings.ToLower(tag))
	if tag == "" {
		return ""
	}
	if name, ok := playHTLanguages[tag]; ok {
		return name
	}
	primary, _, _ := strings.Cut(tag, "-")
	return playHTLanguages[primary]
}

type stream struct {
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent

	voiceEngine        string
	voiceID            string
	language           string
	outputFormat       string
	media              protocol.MediaFormat
	maxInputCharacters int64
	credentialExpiry   time.Time

	writeMu      sync.Mutex
	gracefulOnce sync.Once
	abortOnce    sync.Once
	closed       atomic.Bool
	closing      atomic.Bool
	closeErr     error

	stateMu      sync.Mutex
	pending      []string
	pendingChars int64
	requestID    string
	requestDone  chan struct{}
	audioStarted bool
}

func (s *stream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

func (s *stream) WriteAudio(context.Context, []byte) error { return runtimepkg.ErrUnsupportedOperation }

func (s *stream) CommitAudio(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// AppendText buffers text locally.
//
// PlayHT's WebSocket protocol has no incremental-append command: every message
// is a complete synthesis request carrying the full text. Buffering here and
// sending once at CommitText is therefore the only mapping that produces one
// utterance instead of one clipped utterance per fragment.
func (s *stream) AppendText(_ context.Context, text string) error {
	if strings.TrimSpace(text) == "" {
		return errors.New("playht transcript is empty")
	}
	characters := int64(utf8.RuneCountInString(text))
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() || s.closing.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if s.requestID != "" {
		return errors.New("playht previous utterance has not completed")
	}
	if s.maxInputCharacters > 0 && s.pendingChars+characters > s.maxInputCharacters {
		return &runtimepkg.ProviderError{
			Code:      "input_too_large",
			Message:   "PlayHT input exceeds the authorized character allowance",
			Retryable: false,
		}
	}
	s.pending = append(s.pending, text)
	s.pendingChars += characters
	return nil
}

// CommitText sends the buffered text as a single PlayHT synthesis request.
func (s *stream) CommitText(ctx context.Context) error {
	request, err := s.startRequest()
	if err != nil {
		return err
	}
	if err := s.writeJSON(ctx, request); err != nil {
		s.finishRequest(request.RequestID)
		return err
	}
	return nil
}

// Cancel discards text that has not been sent yet.
//
// PlayHT documents no cancel, clear, or flush command on the WebSocket API:
// once a synthesis request is on the wire the only way to stop it is to drop
// the socket. Reporting that honestly beats pretending a barge-in succeeded.
func (s *stream) Cancel(context.Context) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if s.requestID != "" {
		return runtimepkg.ErrUnsupportedOperation
	}
	if len(s.pending) == 0 {
		return runtimepkg.ErrSessionClosed
	}
	s.pending, s.pendingChars = nil, 0
	return nil
}

// Close waits for an in-flight utterance to finish so a caller that closes
// immediately after CommitText still receives its final audio.
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
			if err := s.conn.Close(websocket.StatusNormalClosure, ""); err != nil {
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
		if err := s.conn.CloseNow(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
		s.finishRequest("")
	})
	return s.closeErr
}

func (s *stream) startRequest() (synthesisRequest, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed.Load() || s.closing.Load() {
		return synthesisRequest{}, runtimepkg.ErrSessionClosed
	}
	if s.requestID != "" {
		return synthesisRequest{}, errors.New("playht previous utterance has not completed")
	}
	if len(s.pending) == 0 {
		return synthesisRequest{}, errors.New("playht has no buffered text to synthesize")
	}
	requestID, err := newRequestID()
	if err != nil {
		return synthesisRequest{}, err
	}
	text := strings.Join(s.pending, "")
	s.pending, s.pendingChars = nil, 0
	s.requestID = requestID
	s.requestDone = make(chan struct{})
	s.audioStarted = false
	return synthesisRequest{
		Text:         text,
		Voice:        s.voiceID,
		VoiceEngine:  s.voiceEngine,
		OutputFormat: s.outputFormat,
		SampleRate:   s.media.SampleRateHz,
		Language:     s.language,
		RequestID:    requestID,
	}, nil
}

func (s *stream) activeDone() <-chan struct{} {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.requestDone
}

func (s *stream) finishRequest(requestID string) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.requestID == "" || (requestID != "" && s.requestID != requestID) {
		return
	}
	close(s.requestDone)
	s.requestID, s.requestDone, s.audioStarted = "", nil, false
}

// markAudioStarted reports whether this is the first audio of the utterance,
// guaranteeing exactly one EventAudioStarted before any EventAudioFrame.
func (s *stream) markAudioStarted() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.audioStarted {
		return false
	}
	s.audioStarted = true
	return true
}

func (s *stream) currentRequestID() string {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.requestID
}

func (s *stream) writeJSON(ctx context.Context, value any) error {
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
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "PlayHT streaming write failed", Retryable: true, Cause: err}
	}
	return nil
}

func (s *stream) readLoop() {
	defer func() {
		s.cancel()
		s.finishRequest("")
		close(s.events)
	}()
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && s.ctx.Err() == nil && !isNormalClose(err) {
				_ = s.emit(runtimepkg.ProviderEvent{Err: &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "PlayHT streaming read failed", Retryable: true, Cause: err}})
			}
			return
		}
		if messageType == websocket.MessageBinary {
			if err := s.handleAudio(payload); err != nil {
				_ = s.emit(runtimepkg.ProviderEvent{Err: err})
				return
			}
			continue
		}
		if err := s.handleControl(payload); err != nil {
			_ = s.emit(runtimepkg.ProviderEvent{Err: err})
			return
		}
	}
}

// handleAudio forwards one PlayHT binary frame.
//
// PlayHT sends audio as real WebSocket binary frames, never base64 in JSON, so
// the payload is emitted byte-for-byte with no decoding step.
func (s *stream) handleAudio(payload []byte) error {
	// Container formats open with a header chunk that is metadata, not samples.
	// Emitting it would inject a click at the head of every utterance.
	if len(payload) >= 4 && string(payload[:4]) == "RIFF" {
		return nil
	}
	if len(payload) == 0 {
		return nil
	}
	if s.markAudioStarted() {
		if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: s.requestData()}); err != nil {
			return err
		}
	}
	return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Data: s.requestData(), Audio: payload})
}

func (s *stream) handleControl(payload []byte) error {
	var message inbound
	if err := json.Unmarshal(payload, &message); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "PlayHT sent malformed streaming JSON", Retryable: true, Cause: err}
	}
	raw := json.RawMessage(append([]byte(nil), payload...))

	// A failure surfaces three ways: an explicit error type, a bare
	// {"error": ...} body, or a normal start/end message whose documented
	// "status" field carries a non-2xx code. Treating only the first two as
	// errors would let an authentication failure look like a silent utterance.
	status := message.status()
	if message.Type == "error" || message.hasError() || status >= 400 {
		return &runtimepkg.ProviderError{
			Code:           streamErrorCode(message.Type, status, message.Message),
			Message:        streamErrorMessage(message.Message),
			Retryable:      status >= 500 || status == http.StatusTooManyRequests,
			ProviderStatus: status,
			Extensions:     extension(raw),
		}
	}

	switch message.Type {
	case "start":
		if s.markAudioStarted() {
			return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: s.requestData(), Extensions: extension(raw)})
		}
		return nil
	case "end":
		// A late "end" for an utterance that already finished must not close a
		// nil channel or emit a second terminal event.
		if s.currentRequestID() == "" {
			return nil
		}
		// Keep the emitted sequence well-formed even when PlayHT ends an
		// utterance that produced no audio at all.
		if s.markAudioStarted() {
			if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: s.requestData()}); err != nil {
				return err
			}
		}
		if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: s.requestData(), Extensions: extension(raw)}); err != nil {
			return err
		}
		s.finishRequest("")
		return nil
	default:
		return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventWarning, Data: s.warningData(message.Type), Extensions: extension(raw)})
	}
}

func (s *stream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *stream) requestData() json.RawMessage {
	return marshalData(map[string]any{"request_id": s.currentRequestID(), "voice_engine": s.voiceEngine})
}

func (s *stream) warningData(messageType string) json.RawMessage {
	return marshalData(map[string]any{"request_id": s.currentRequestID(), "provider_type": messageType})
}

func marshalData(value any) json.RawMessage {
	payload, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{"code":"internal"}`)
	}
	return payload
}

func extension(raw json.RawMessage) map[string]json.RawMessage {
	return map[string]json.RawMessage{extensionID: raw}
}

func newRequestID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("generate PlayHT request id: %w", err)
	}
	return hex.EncodeToString(bytes), nil
}

func isNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}

func authErrorCode(status int) string {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return "authentication_failed"
	case http.StatusPaymentRequired:
		return "provider_quota_exceeded"
	case http.StatusTooManyRequests:
		return "provider_rate_limited"
	case http.StatusBadRequest, http.StatusNotFound, http.StatusUnprocessableEntity:
		return "invalid_request"
	default:
		return "provider_unavailable"
	}
}

func dialErrorCode(status int) string {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return "authentication_failed"
	case http.StatusPaymentRequired:
		return "provider_quota_exceeded"
	case http.StatusTooManyRequests:
		return "provider_rate_limited"
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return "invalid_request"
	default:
		return "provider_unavailable"
	}
}

// streamErrorCode classifies an in-stream error. PlayHT does not document a
// status code on every WebSocket error body, so the message is the fallback
// signal when no numeric status arrives.
func streamErrorCode(messageType string, status int, message string) string {
	if status != 0 {
		return authErrorCode(status)
	}
	lowered := strings.ToLower(message + " " + messageType)
	switch {
	case strings.Contains(lowered, "unauthor"), strings.Contains(lowered, "forbidden"), strings.Contains(lowered, "expired"), strings.Contains(lowered, "invalid token"):
		return "authentication_failed"
	case strings.Contains(lowered, "quota"), strings.Contains(lowered, "credit"), strings.Contains(lowered, "insufficient"):
		return "provider_quota_exceeded"
	case strings.Contains(lowered, "rate limit"), strings.Contains(lowered, "too many"):
		return "provider_rate_limited"
	case strings.Contains(lowered, "invalid"), strings.Contains(lowered, "unsupported"):
		return "invalid_request"
	default:
		return "provider_unavailable"
	}
}

func streamErrorMessage(message string) string {
	if strings.TrimSpace(message) == "" {
		return "PlayHT reported a streaming error"
	}
	return "PlayHT reported a streaming error: " + message
}

// synthesisRequest is one complete PlayHT WebSocket synthesis command.
type synthesisRequest struct {
	Text         string `json:"text"`
	Voice        string `json:"voice"`
	VoiceEngine  string `json:"voice_engine"`
	OutputFormat string `json:"output_format"`
	SampleRate   int    `json:"sample_rate"`
	Language     string `json:"language,omitempty"`
	RequestID    string `json:"request_id"`
}

type inbound struct {
	Type string `json:"type"`
	// RequestID is deliberately raw: PlayHT echoes the client's string id in
	// some deployments and a numeric id in others, and decoding into a string
	// would turn a routine control message into a fatal parse error.
	RequestID json.RawMessage `json:"request_id"`
	Message   string          `json:"message"`
	// Status is the documented field on start/end messages, which PlayHT
	// describes as "200 or whatever is the response" -- so a failed request is
	// reported through an otherwise ordinary start message, not a distinct
	// error type. StatusCode is accepted as a defensive alias.
	Status     int             `json:"status"`
	StatusCode int             `json:"status_code"`
	Error      json.RawMessage `json:"error"`
}

// status normalizes the two spellings PlayHT is known to use.
func (m inbound) status() int {
	if m.Status != 0 {
		return m.Status
	}
	return m.StatusCode
}

// hasError distinguishes a real error body from an explicit JSON null.
func (m inbound) hasError() bool {
	return len(m.Error) > 0 && string(m.Error) != "null"
}
