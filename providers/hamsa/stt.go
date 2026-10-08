package hamsa

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
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
	// STTAdapterID is the identifier returned by a Hamsa STT session plan.
	STTAdapterID = "hamsa.stt.v1"

	// officialAPIHost serves both the REST and realtime surfaces.
	officialAPIHost = "api.tryhamsa.com"

	// sttEndpointPath is the realtime socket. It is NOT a frame-streaming
	// recognizer: each `stt` message must carry one COMPLETE WAV utterance
	// (raw or partial PCM is refused with "Only WAV files are supported"),
	// and the transcript for it comes back as a single plain-string frame.
	sttEndpointPath = "/v1/realtime/ws"

	// apiKeyHeader authenticates the realtime socket. Hamsa documents it as
	// the alternative to the api_key query parameter; a header keeps the key
	// out of every URL a proxy or access log records.
	apiKeyHeader = "X-Api-Key"

	// DefaultSTTModel is Hamsa's current generation. The API reference
	// enumerates `s2` and `s3` for the socket's `model` field, and `s2` stays
	// selectable through it.
	DefaultSTTModel = "s3"

	extensionID = "tryhamsa.com/realtime_v1"

	// sttSampleRateHz is the one rate this adapter sends. Hamsa documents WAV
	// input without a rate table, and every verified integration submits
	// 16 kHz mono; accepting other rates here would forward audio the vendor
	// has never been observed to accept.
	sttSampleRateHz = 16_000

	// utteranceTimeout bounds one transcribe round trip. The socket answers a
	// whole utterance at once, so a reply that has not arrived by now is a
	// stuck session, not a slow interim.
	utteranceTimeout = 30 * time.Second

	// billingIncrementMS is Hamsa's metering grain. Measured 2026-10-07 against
	// the project's credit counter: every realtime request is charged 3 credits
	// per minute of audio, rounded UP to a whole second per request (3 s cost
	// 0.15 credits, 65 s cost 3.25, a 0.28 s synthesis cost 0.05).
	billingIncrementMS = 1000

	// sttBytesPerSecond is 16 kHz mono PCM16.
	sttBytesPerSecond = sttSampleRateHz * 2
)

// Close codes the realtime socket documents.
const (
	closeAuthenticationFailed websocket.StatusCode = 4001
	closeInsufficientFunds    websocket.StatusCode = 4003
	closeInternalAuthError    websocket.StatusCode = 4500
)

// STTConfig controls local transport limits and endpoint policy. Provider
// identity, model, and credential all come from the signed session plan.
type STTConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	EventBuffer           int
	MaxUtteranceBytes     int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// STTAdapter implements Hamsa's whole-utterance realtime WebSocket. The
// canonical streaming contract is mapped onto it by buffering: WriteAudio
// accumulates one turn, CommitAudio (and a Close with a non-empty tail)
// performs one socket round trip for the buffered utterance and emits its
// final transcript. There are no interim transcripts by construction.
type STTAdapter struct {
	id                string
	httpClient        *http.Client
	eventBuffer       int
	maxUtteranceBytes int64
	endpointPolicy    upstream.WebSocketPolicy
}

// NewSTT creates a bounded Hamsa STT adapter.
func NewSTT(config STTConfig) (*STTAdapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = STTAdapterID
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = 32
	}
	if config.MaxUtteranceBytes == 0 {
		// 15 MiB of 16 kHz mono PCM16 is ~8 minutes of audio — far beyond any
		// conversational turn, small enough to keep a hostile writer bounded.
		config.MaxUtteranceBytes = 15 << 20
	}
	if config.EventBuffer < 1 {
		return nil, errors.New("hamsa stt event buffer must be positive")
	}
	if config.MaxUtteranceBytes < 1 {
		return nil, errors.New("hamsa stt maximum utterance bytes must be positive")
	}
	endpointPolicy, err := upstream.NewWebSocketPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &STTAdapter{
		id:                config.AdapterID,
		httpClient:        config.HTTPClient,
		eventBuffer:       config.EventBuffer,
		maxUtteranceBytes: config.MaxUtteranceBytes,
		endpointPolicy:    endpointPolicy,
	}, nil
}

func (a *STTAdapter) ID() string { return a.id }

// Open validates the plan and media and returns a buffering stream. No socket
// is dialled here: the vendor charges per submitted utterance, and a session
// that never commits audio should never touch the provider.
func (a *STTAdapter) Open(_ context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindSTT {
		return nil, fmt.Errorf("hamsa stt supports stt sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != "hamsa" {
		return nil, fmt.Errorf("hamsa stt adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("hamsa stt requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Media == nil {
		return nil, errors.New("hamsa stt requires media configuration")
	}
	if err := request.Media.Validate(); err != nil {
		return nil, fmt.Errorf("hamsa stt media: %w", err)
	}
	if err := validateSTTOptions(request.Plan.Route.Model, *request.Media); err != nil {
		return nil, err
	}
	credential, err := requireAccountKey(request, "hamsa stt")
	if err != nil {
		return nil, err
	}
	endpoint, err := a.endpointPolicy.Parse(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("hamsa stt endpoint: %w", err)
	}
	if endpoint.Path != sttEndpointPath {
		return nil, fmt.Errorf("hamsa stt endpoint path must be %s, got %q", sttEndpointPath, endpoint.Path)
	}
	return &sttStream{
		adapter:    a,
		endpoint:   endpoint.String(),
		credential: credential,
		model:      request.Plan.Route.Model,
		language:   normalizeLanguage(request.Options.Language),
		events:     make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		billing: protocol.BillingObservation{
			OperationID: "session", Model: request.Plan.Route.Model, Mode: "streaming",
			Quantities: map[string]int64{"duration_seconds": 0},
		},
	}, nil
}

func validateSTTOptions(model string, media protocol.MediaFormat) error {
	if strings.TrimSpace(model) == "" || model == "auto" {
		return errors.New("hamsa stt requires a concrete model in the session plan")
	}
	if media.Encoding != "pcm_s16le" {
		return fmt.Errorf("hamsa stt cannot accept encoding %q", media.Encoding)
	}
	if media.Channels != 1 {
		return fmt.Errorf("hamsa stt requires mono audio, got %d channels", media.Channels)
	}
	if media.SampleRateHz != sttSampleRateHz {
		return fmt.Errorf("hamsa stt requires %d Hz audio, got %d Hz", sttSampleRateHz, media.SampleRateHz)
	}
	return nil
}

// sttOutbound is the one request shape the realtime socket accepts.
type sttOutbound struct {
	Type    string             `json:"type"`
	Payload sttOutboundPayload `json:"payload"`
}

type sttOutboundPayload struct {
	AudioBase64 string `json:"audioBase64"`
	Language    string `json:"language"`
	Model       string `json:"model"`
	// IsEosEnabled asks Hamsa's own end-of-speech model whether the speaker
	// has finished. It is always sent false: a commit already IS the end of
	// the turn, decided by the caller's VAD. With it on, Hamsa answers an
	// utterance it judges unfinished with "[THINKING]" and then nothing at all
	// (observed 2026-10-07 on a 3 s clip that stopped mid-sentence: 45 s of
	// silence after the marker), so the turn hung until utteranceTimeout.
	IsEosEnabled bool `json:"isEosEnabled"`
}

// sttControl is a JSON control frame. Anything on the socket that fails to
// parse as JSON is the transcript itself.
type sttControl struct {
	Type    string `json:"type"`
	Payload struct {
		Message string `json:"message"`
	} `json:"payload"`
}

type sttStream struct {
	adapter    *STTAdapter
	endpoint   string
	credential string
	model      string
	language   string
	events     chan runtimepkg.ProviderEvent

	// The runtime serializes WriteAudio, CommitAudio, Close, and Cancel, so
	// the utterance buffer and billing snapshot need no lock of their own;
	// closed guards the events channel against the emit-after-close race.
	buffer    []byte
	billing   protocol.BillingObservation
	closed    atomic.Bool
	closeOnce sync.Once
}

func (s *sttStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }

// WriteAudio buffers one frame of raw PCM. Nothing is sent upstream until the
// turn commits: the socket only accepts whole utterances.
func (s *sttStream) WriteAudio(_ context.Context, audio []byte) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if len(audio) == 0 {
		return errors.New("hamsa stt audio is empty")
	}
	if int64(len(s.buffer)+len(audio)) > s.adapter.maxUtteranceBytes {
		return &runtimepkg.ProviderError{
			Code:    "provider_unavailable",
			Message: "Hamsa transcription utterance exceeds the adapter's buffer limit",
		}
	}
	s.buffer = append(s.buffer, audio...)
	return nil
}

// CommitAudio transcribes the buffered turn. An empty buffer commits to an
// empty final transcript without a provider round trip — silence is a valid
// finalized turn, and the vendor bills per submitted utterance.
func (s *sttStream) CommitAudio(ctx context.Context) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	return s.transcribeBuffered(ctx)
}

func (s *sttStream) AppendText(context.Context, string) error {
	return runtimepkg.ErrUnsupportedOperation
}

func (s *sttStream) CommitText(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// Cancel drops the uncommitted tail. Every utterance already sent finished
// synchronously inside its CommitAudio, so the billing snapshot is final.
func (s *sttStream) Cancel(ctx context.Context) error {
	s.emitBilling(ctx, true)
	s.shutdown()
	return nil
}

func (s *sttStream) Abort(context.Context) error {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.emitBilling(ctx, true)
	s.shutdown()
	return nil
}

// Close flushes an uncommitted tail exactly like the end of a committed turn,
// so a caller that streams audio and closes — the batch transcription shape —
// still gets its final transcript before the event channel ends.
func (s *sttStream) Close(ctx context.Context) error {
	if s.closed.Load() {
		return nil
	}
	var err error
	if len(s.buffer) > 0 {
		err = s.transcribeBuffered(ctx)
	}
	s.emitBilling(ctx, true)
	s.shutdown()
	return err
}

// recordUtterance adds one dispatched utterance to the session snapshot at the
// grain Hamsa charges: whole seconds, rounded up per request.
func (s *sttStream) recordUtterance(pcmBytes int) {
	milliseconds := (int64(pcmBytes)*1000 + sttBytesPerSecond - 1) / sttBytesPerSecond
	s.billing.Quantities["duration_seconds"] += (milliseconds + billingIncrementMS - 1) / billingIncrementMS * billingIncrementMS
}

// emitBilling publishes the cumulative snapshot. A final snapshot is offered
// without blocking past ctx: Abort passes a cancelled context because a failed
// runtime may no longer drain events, and the event buffer usually has room.
func (s *sttStream) emitBilling(ctx context.Context, complete bool) {
	observation := s.billing.Clone()
	observation.Complete = complete
	event := runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Billing: &observation}
	if s.closed.Load() {
		return
	}
	select {
	case s.events <- event:
	default:
		select {
		case s.events <- event:
		case <-ctx.Done():
		}
	}
}

func (s *sttStream) shutdown() {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		s.buffer = nil
		close(s.events)
	})
}

func (s *sttStream) transcribeBuffered(ctx context.Context) error {
	utterance := s.buffer
	s.buffer = nil
	if len(utterance) == 0 {
		return s.emit(ctx, runtimepkg.ProviderEvent{
			Type: protocol.EventTranscriptFinal,
			Data: transcriptData("", s.language),
		})
	}
	text, raw, err := s.transcribeUtterance(ctx, utterance)
	if err != nil {
		// Provider failures terminate the attempt through the event channel,
		// matching the streaming adapters' read loops; the commit call itself
		// only fails on local session errors.
		return s.emit(ctx, runtimepkg.ProviderEvent{Err: err})
	}
	// Only an answered utterance is metered: an error frame or a dropped
	// socket returns before Hamsa reports a transcript to charge for.
	s.recordUtterance(len(utterance))
	s.emitBilling(ctx, false)
	if text != "" {
		if err := s.emit(ctx, runtimepkg.ProviderEvent{Type: protocol.EventSpeechStarted, Data: transcriptMetadata()}); err != nil {
			return err
		}
	}
	if err := s.emit(ctx, runtimepkg.ProviderEvent{Type: protocol.EventTranscriptFinal, Data: transcriptData(text, s.language), Extensions: extension(raw)}); err != nil {
		return err
	}
	if text != "" {
		return s.emit(ctx, runtimepkg.ProviderEvent{Type: protocol.EventSpeechEnded, Data: transcriptMetadata()})
	}
	return nil
}

// transcribeUtterance performs one whole-utterance round trip: dial, send the
// WAV-wrapped turn, wait for the plain-string transcript. The socket is
// per-utterance by design — Hamsa keeps no session state between turns — so a
// fresh dial per commit is the vendor's own usage model, not an inefficiency.
func (s *sttStream) transcribeUtterance(ctx context.Context, utterance []byte) (string, json.RawMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, utteranceTimeout)
	defer cancel()

	conn, response, err := websocket.Dial(ctx, s.endpoint, &websocket.DialOptions{
		HTTPClient: httpClient(s.adapter.httpClient),
		HTTPHeader: http.Header{apiKeyHeader: []string{s.credential}},
	})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		return "", nil, &runtimepkg.ProviderError{
			Code:           dialErrorCode(status),
			Message:        "Hamsa transcription connection could not be established",
			Retryable:      status == 0 || status == http.StatusTooManyRequests || status >= 500,
			ProviderStatus: status,
			Cause:          err,
		}
	}
	defer conn.CloseNow()
	conn.SetReadLimit(1 << 20)

	request, err := json.Marshal(sttOutbound{
		Type: "stt",
		Payload: sttOutboundPayload{
			AudioBase64:  base64.StdEncoding.EncodeToString(pcm16ToWAV(utterance, sttSampleRateHz)),
			Language:     s.language,
			Model:        s.model,
			IsEosEnabled: false,
		},
	})
	if err != nil {
		return "", nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Hamsa transcription request could not be encoded", Cause: err}
	}
	if err := conn.Write(ctx, websocket.MessageText, request); err != nil {
		return "", nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Hamsa transcription write failed", Retryable: true, Cause: err}
	}

	for {
		_, payload, err := conn.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return "", nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Hamsa transcription timed out", Retryable: true, Cause: ctx.Err()}
			}
			if isNormalClose(err) {
				// The socket may answer an unrecognized utterance by closing
				// without a transcript frame; surface that as a valid empty
				// final rather than a mystery failure.
				return "", nil, nil
			}
			if closeErr := closeStatusError(websocket.CloseStatus(err), err); closeErr != nil {
				return "", nil, closeErr
			}
			return "", nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Hamsa transcription read failed", Retryable: true, Cause: err}
		}
		var control sttControl
		if json.Unmarshal(payload, &control) == nil && control.Type != "" {
			switch control.Type {
			case "error":
				code, retryable := classifyErrorMessage(control.Payload.Message)
				return "", nil, &runtimepkg.ProviderError{
					Code:       code,
					Message:    sttErrorMessage(control.Payload.Message),
					Retryable:  retryable,
					Extensions: extension(json.RawMessage(append([]byte(nil), payload...))),
				}
			default:
				// {"type":"info"} on connect, and any future control frame:
				// not the transcript, keep waiting.
				continue
			}
		}
		text := strings.TrimSpace(string(payload))
		// Bracketed status markers ("[THINKING]") are Hamsa's end-of-speech
		// verdict that the speaker has not finished. isEosEnabled is sent
		// false so they should not arrive; if one does, it is never the
		// transcript: skip it and keep waiting, so a stuck session hits the
		// timeout instead of returning the marker as a billed result.
		if statusMarker(text) {
			continue
		}
		// Any other frame that is not a JSON control frame IS the transcript.
		return text, json.RawMessage(append([]byte(nil), payload...)), nil
	}
}

func (s *sttStream) emit(ctx context.Context, event runtimepkg.ProviderEvent) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	select {
	case s.events <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// requireAccountKey enforces the same credential policy as the other
// single-key vendors: Hamsa publishes no delegated, session-scoped credential,
// so a managed provider-direct plan could only be forwarding the customer's
// root account key and must be refused. The relay arm carries the relay
// connector's OWN permanent key and is the one managed construction honoured.
func requireAccountKey(request runtimepkg.AdapterRequest, adapter string) (string, error) {
	if request.Plan.Execution.ProviderRoute != protocol.RouteSpekoRelay && request.Plan.Execution.CredentialSource != protocol.CredentialsBYOK {
		return "", fmt.Errorf("%s is BYOK-only on provider-direct routes: Hamsa publishes no delegated credential, got credential source %q", adapter, request.Plan.Execution.CredentialSource)
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return "", fmt.Errorf("%s requires a bearer credential", adapter)
	}
	return strings.TrimSpace(credential.Value), nil
}

func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

// normalizeLanguage lowercases and strips a region subtag; the socket wants a
// bare ISO-639 code and today recognizes Arabic (plus mixed Arabic-English
// within it). An absent language defaults to Arabic — the vendor's own home —
// rather than leaving the field empty, which the socket does not document.
func normalizeLanguage(language string) string {
	language = strings.ToLower(strings.TrimSpace(language))
	if language == "" {
		return "ar"
	}
	base, _, _ := strings.Cut(language, "-")
	return base
}

// pcm16ToWAV wraps raw little-endian mono PCM16 in the minimal 44-byte RIFF
// header. The socket refuses raw PCM outright, so the container is load-
// bearing, not a formality.
func pcm16ToWAV(pcm []byte, sampleRateHz int) []byte {
	out := make([]byte, 44+len(pcm))
	copy(out[0:4], "RIFF")
	binary.LittleEndian.PutUint32(out[4:8], uint32(36+len(pcm)))
	copy(out[8:12], "WAVE")
	copy(out[12:16], "fmt ")
	binary.LittleEndian.PutUint32(out[16:20], 16)
	binary.LittleEndian.PutUint16(out[20:22], 1) // PCM
	binary.LittleEndian.PutUint16(out[22:24], 1) // mono
	binary.LittleEndian.PutUint32(out[24:28], uint32(sampleRateHz))
	binary.LittleEndian.PutUint32(out[28:32], uint32(sampleRateHz*2))
	binary.LittleEndian.PutUint16(out[32:34], 2)  // block align
	binary.LittleEndian.PutUint16(out[34:36], 16) // bits per sample
	copy(out[36:40], "data")
	binary.LittleEndian.PutUint32(out[40:44], uint32(len(pcm)))
	copy(out[44:], pcm)
	return out
}

func httpClient(client *http.Client) *http.Client {
	if client != nil {
		return client
	}
	return http.DefaultClient
}

func isNormalClose(err error) bool {
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}

// closeStatusError maps the socket's documented application close codes. The
// wallet code is the one that matters operationally: retrying an empty
// wallet only burns the fallback budget.
func closeStatusError(status websocket.StatusCode, cause error) *runtimepkg.ProviderError {
	switch status {
	case closeAuthenticationFailed, closeInternalAuthError:
		return &runtimepkg.ProviderError{Code: "authentication_failed", Message: "Hamsa refused the API key", Cause: cause}
	case closeInsufficientFunds:
		return &runtimepkg.ProviderError{Code: "provider_quota_exceeded", Message: "Hamsa project wallet balance is depleted", Cause: cause}
	}
	return nil
}

// classifyErrorMessage reads the documented error prose, the only signal an
// error frame carries.
func classifyErrorMessage(message string) (string, bool) {
	lower := strings.ToLower(message)
	switch {
	case strings.Contains(lower, "rate limit"):
		return "provider_rate_limited", true
	case strings.Contains(lower, "insufficient funds"), strings.Contains(lower, "usage limit"):
		return "provider_quota_exceeded", false
	case strings.Contains(lower, "api key"), strings.Contains(lower, "inactive"), strings.Contains(lower, "not owned"):
		return "authentication_failed", false
	case strings.Contains(lower, "invalid payload"), strings.Contains(lower, "supported"), strings.Contains(lower, "invalid message format"):
		return "invalid_request", false
	}
	return "provider_unavailable", true
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

func extension(raw json.RawMessage) map[string]json.RawMessage {
	if raw == nil {
		return nil
	}
	return map[string]json.RawMessage{extensionID: raw}
}

func marshalData(value any) json.RawMessage {
	payload, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{"code":"internal"}`)
	}
	return payload
}

func transcriptData(text, language string) json.RawMessage {
	return marshalData(map[string]any{
		"text":     text,
		"is_final": true,
		"language": language,
	})
}

func transcriptMetadata() json.RawMessage {
	return marshalData(map[string]any{})
}

var statusMarkerPattern = regexp.MustCompile(`^\[[A-Z_ ]+\]$`)

func statusMarker(text string) bool {
	return statusMarkerPattern.MatchString(text)
}

func sttErrorMessage(message string) string {
	if strings.TrimSpace(message) == "" {
		return "Hamsa reported a transcription error"
	}
	return "Hamsa reported a transcription error: " + message
}
