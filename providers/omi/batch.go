package omi

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/SpekoAI/gateway/internal/batchhttp"
	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/metering"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// BatchAdapterID identifies the prerecorded upload implementation.
	BatchAdapterID = "omi.stt.batch.v1"
	// BatchEndpoint is the prerecorded twin of the realtime socket, on the
	// same US host. It serves the same model.
	BatchEndpoint = "https://api.us.omi.health/v1/audio/transcriptions"
	// BatchModel is the model an upload asks for.
	BatchModel = Model
	// BatchMaxAudioBytes is the largest PCM payload an upload carries: the
	// relay catalog's MaxPCMBytes for this row. The WAV header and the
	// multipart envelope fit in the 16 KiB left under Omi's 100 MB upload
	// limit, read as decimal megabytes. A larger payload is refused before
	// any of it is sent.
	BatchMaxAudioBytes int64 = 100_000_000 - 16<<10
	// BatchMaxDurationSeconds is Omi's two-hour cap (422 audio_too_long).
	// At the rates the relay carries, BatchMaxAudioBytes binds first.
	BatchMaxDurationSeconds int64 = 2 * 60 * 60

	batchPath        = "/v1/audio/transcriptions"
	batchExtensionID = "api.us.omi.health/v1/audio/transcriptions"
	// requestIDHeader names the request on every REST answer.
	requestIDHeader = "X-Request-Id"
	// idempotencyHeader makes a resubmitted upload the same job.
	idempotencyHeader = "Idempotency-Key"
	// pollWaitSeconds is the job long-poll Omi holds a GET open for.
	pollWaitSeconds = "20"

	defaultPollInterval = time.Second
	defaultMaxBackoff   = 10 * time.Second
	defaultPollDeadline = 10 * time.Minute
	// wavHeaderBytes is the canonical RIFF/WAVE header the relay writes.
	wavHeaderBytes = 44
)

// BatchConfig controls local transport limits for the prerecorded adapter.
type BatchConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	MaxResponseBytes      int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
	// PollInterval is the pause between job polls that return without a
	// result; the poll itself long-polls for up to 20 s.
	PollInterval time.Duration
	// MaxBackoff caps the backoff after a 429 or 5xx poll.
	MaxBackoff time.Duration
	// PollDeadline bounds a 202 job from acceptance to result.
	PollDeadline time.Duration
}

// BatchAdapter implements runtime.BatchTranscriber over POST
// /v1/audio/transcriptions.
type BatchAdapter struct {
	id                    string
	httpClient            *http.Client
	maxResponseBytes      int64
	pollInterval          time.Duration
	maxBackoff            time.Duration
	pollDeadline          time.Duration
	allowInsecureEndpoint bool
	endpointPolicy        upstream.HTTPPolicy
}

// NewBatch creates the prerecorded adapter.
func NewBatch(config BatchConfig) (*BatchAdapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = BatchAdapterID
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = batchhttp.DefaultMaxResponseBytes
	}
	if config.PollInterval == 0 {
		config.PollInterval = defaultPollInterval
	}
	if config.MaxBackoff == 0 {
		config.MaxBackoff = defaultMaxBackoff
	}
	if config.PollDeadline == 0 {
		config.PollDeadline = defaultPollDeadline
	}
	if config.MaxResponseBytes < 1 || config.PollInterval <= 0 || config.MaxBackoff <= 0 || config.PollDeadline <= 0 {
		return nil, errors.New("omi batch limits, poll interval, backoff and deadline must be positive")
	}
	policy, err := upstream.NewHTTPPolicy(officialHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &BatchAdapter{
		id: config.AdapterID, httpClient: config.HTTPClient, maxResponseBytes: config.MaxResponseBytes,
		pollInterval: config.PollInterval, maxBackoff: config.MaxBackoff, pollDeadline: config.PollDeadline,
		allowInsecureEndpoint: config.AllowInsecureEndpoint, endpointPolicy: policy,
	}, nil
}

func (a *BatchAdapter) ID() string { return a.id }

// Transcribe uploads the WAV container as the multipart `file` part beside
// model, language and response_format verbose_json, which alone carries the
// billable duration. A request for speaker labels or word timings sends
// diarize=true, the only switch that returns either. A 202 job is long-polled
// to its result.
func (a *BatchAdapter) Transcribe(ctx context.Context, request runtimepkg.BatchTranscribeRequest) (*runtimepkg.BatchTranscription, error) {
	if request.Plan.Route.Provider != ProviderName {
		return nil, fmt.Errorf("omi batch adapter cannot serve provider %q", request.Plan.Route.Provider)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model != BatchModel {
		return nil, fmt.Errorf("omi batch does not support model %q", model)
	}
	language, err := batchLanguage(request.Options.Language)
	if err != nil {
		return nil, err
	}
	if err := batchOptions(request.Options.STT); err != nil {
		return nil, err
	}
	if request.Media.Encoding != "pcm_s16le" || request.Media.Channels != 1 || request.Media.SampleRateHz < 8_000 || request.Media.SampleRateHz > 48_000 {
		return nil, &runtimepkg.ProviderError{
			Code:    batchhttp.CodeUnsupportedMedia,
			Message: fmt.Sprintf("Omi uploads take mono pcm_s16le WAV from 8000 to 48000 Hz, got %s/%d Hz/%d channels", request.Media.Encoding, request.Media.SampleRateHz, request.Media.Channels),
			Hint:    "Resample the audio to mono pcm_s16le at 16000 Hz.",
		}
	}
	// Refuse before reading the file rather than after streaming a body the
	// service would refuse.
	// The limit is on the PCM, so the relay's MaxPCMBytes check and this one
	// agree at the boundary; the 44-byte header fits in the envelope reserve.
	if request.AudioBytes-wavHeaderBytes > BatchMaxAudioBytes || audioSeconds(request.Media, request.AudioBytes) > BatchMaxDurationSeconds {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInputTooLarge, Message: "the upload exceeds Omi's 100 MB transcription limit", Hint: "Split the audio into files under 100 MB (about 52 minutes at 16 kHz)."}
	}
	credential, err := batchhttp.Credential(request.Plan)
	if err != nil {
		return nil, err
	}
	endpoint, err := a.endpointPolicy.Parse(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}
	if endpoint.Path != batchPath {
		return nil, fmt.Errorf("omi batch endpoint path must be %s, got %q", batchPath, endpoint.Path)
	}
	if err := batchhttp.Rewind(request.Audio); err != nil {
		return nil, err
	}
	audio, err := exactWAV(request.Audio, request.AudioBytes)
	if err != nil {
		return nil, err
	}
	wantSpeakers := request.Options.STT.Diarize()
	wantWords := request.Options.STT.WantsWordTimestamps()
	fields := []batchhttp.MultipartField{
		{Name: "model", Value: model},
		{Name: "language", Value: language},
		{Name: "response_format", Value: "verbose_json"},
	}
	if wantSpeakers || wantWords {
		fields = append(fields, batchhttp.MultipartField{Name: "diarize", Value: "true"})
	}
	body, contentType := batchhttp.Multipart(fields, "file", "audio.wav", "audio/wav", audio)
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), body)
	if err != nil {
		body.Close()
		return nil, err
	}
	httpRequest.Header.Set("Content-Type", contentType)
	httpRequest.Header.Set("Accept", "application/json")
	httpRequest.Header.Set("Authorization", "Bearer "+credential)
	httpRequest.Header.Set(idempotencyHeader, idempotencyKey(request.Plan))

	response, err := batchhttp.Do(a.httpClient, httpRequest, a.maxResponseBytes)
	if err != nil {
		return nil, err
	}
	requested := batchRequest{model: model, language: language, wantSpeakers: wantSpeakers, wantWords: wantWords}
	switch response.Status {
	case http.StatusOK:
		return parseBatchResponse(response.Body, response.Header.Get(requestIDHeader), requested)
	case http.StatusAccepted:
		return a.awaitJob(ctx, endpoint, credential, response, requested)
	default:
		return nil, providerError(fmt.Sprintf("Omi rejected the transcription with status %d", response.Status), parseError(response.Body), response.Status, response.Body)
	}
}

// batchOptions refuses asks an upload cannot honor. Omi accepts an inline
// vocabulary and then drops it as not_selected, so keywords are refused
// rather than silently ignored.
func batchOptions(options *protocol.SttOptions) error {
	switch {
	case len(options.GetKeywords()) > 0:
		return &runtimepkg.ProviderError{Code: batchhttp.CodeInvalidRequest, Message: "Omi uploads do not apply a keyword list", Hint: "Drop the keywords option or choose a provider that supports vocabulary biasing."}
	case options.ReduceNoise():
		return &runtimepkg.ProviderError{Code: batchhttp.CodeInvalidRequest, Message: "Omi uploads have no noise-reduction option", Hint: "Drop the noise-reduction option."}
	case options.TranslationTarget() != "":
		return &runtimepkg.ProviderError{Code: batchhttp.CodeInvalidRequest, Message: "Omi uploads do not translate", Hint: "Drop the translation option."}
	case len(options.ProviderKeys(ProviderName)) > 0:
		return &runtimepkg.ProviderError{Code: batchhttp.CodeInvalidRequest, Message: "Omi uploads take no provider-specific settings", Hint: "silence_ms applies to realtime sessions only."}
	}
	return nil
}

// audioSeconds is the duration the WAV declares through its byte count, or
// zero when the format is unknown.
func audioSeconds(media protocol.MediaFormat, audioBytes int64) int64 {
	if media.SampleRateHz <= 0 || media.Channels <= 0 || audioBytes <= wavHeaderBytes {
		return 0
	}
	return (audioBytes - wavHeaderBytes) / int64(media.SampleRateHz*media.Channels*2)
}

// exactWAV makes the canonical 44-byte header state the real sizes. A WAV
// written to a pipe declares an unknown (maximal) data size, which Omi reads
// as more than two hours and refuses with audio_too_long. A container that is
// not the canonical header is sent unchanged.
func exactWAV(audio io.ReadSeeker, total int64) (io.Reader, error) {
	header := make([]byte, wavHeaderBytes)
	read, err := io.ReadFull(audio, header)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeProviderError, Message: "the audio could not be read", Cause: err}
	}
	header = header[:read]
	if read == wavHeaderBytes && total > wavHeaderBytes && total-8 <= math.MaxUint32 &&
		string(header[0:4]) == "RIFF" && string(header[8:12]) == "WAVE" && string(header[12:16]) == "fmt " &&
		binary.LittleEndian.Uint32(header[16:20]) == 16 && string(header[36:40]) == "data" {
		binary.LittleEndian.PutUint32(header[4:8], uint32(total-8))
		binary.LittleEndian.PutUint32(header[40:44], uint32(total-wavHeaderBytes))
	}
	return io.MultiReader(bytes.NewReader(header), audio), nil
}

// idempotencyKey derives the upload's key from the plan's opaque ids, so a
// resubmission of the same attempt is the same Omi job and a failover attempt
// is a new one. The ids are hashed rather than sent. A plan without ids gets
// a random key.
func idempotencyKey(plan protocol.SessionPlan) string {
	parts := []string{plan.PlanID, plan.AttemptID, plan.Reservation.ID, plan.SessionID}
	if strings.TrimSpace(strings.Join(parts, "")) == "" {
		var random [16]byte
		_, _ = rand.Read(random[:])
		return "speko-" + hex.EncodeToString(random[:])
	}
	sum := sha256.Sum256([]byte(BatchAdapterID + "\x00" + strings.Join(parts, "\x00")))
	return "speko-" + hex.EncodeToString(sum[:16])
}

// batchRequest is what the upload asked for, which shapes the answer.
type batchRequest struct {
	model        string
	language     string
	wantSpeakers bool
	wantWords    bool
}

// batchJob is a 202 answer and each poll of it.
type batchJob struct {
	ID      string       `json:"id"`
	Status  string       `json:"status"`
	PollURL string       `json:"poll_url"`
	Error   *errorDetail `json:"error"`
	Result  *struct {
		Content     json.RawMessage `json:"content"`
		DownloadURL string          `json:"download_url"`
		Expired     bool            `json:"expired"`
	} `json:"result"`
}

// awaitJob long-polls a 202 job to its result within the poll deadline. The
// bearer goes only to a poll URL on the submit endpoint's own origin; a
// signed download URL on another host is fetched without it. A 429 or 5xx
// poll backs off and retries; any other failure ends the job.
func (a *BatchAdapter) awaitJob(ctx context.Context, endpoint *url.URL, credential string, accepted *batchhttp.Response, requested batchRequest) (*runtimepkg.BatchTranscription, error) {
	var job batchJob
	if err := batchhttp.DecodeJSON(accepted.Body, &job); err != nil {
		return nil, err
	}
	requestID := strings.TrimSpace(accepted.Header.Get(requestIDHeader))
	rawPoll := strings.TrimSpace(job.PollURL)
	if rawPoll == "" {
		rawPoll = strings.TrimSpace(accepted.Header.Get("Location"))
	}
	if rawPoll == "" {
		return nil, batchhttp.Malformed(errors.New("omi 202 carries neither poll_url nor Location"))
	}
	pollURL, err := a.sameOrigin(endpoint, rawPoll)
	if err != nil {
		return nil, err
	}
	query := pollURL.Query()
	query.Set("wait", pollWaitSeconds)
	query.Set("include_result", "true")
	pollURL.RawQuery = query.Encode()

	pollCtx, cancel := context.WithTimeout(ctx, a.pollDeadline)
	defer cancel()
	backoff := a.pollInterval
	// text holds a finished transcript whose speaker labels or word timings
	// were still being attached. If the deadline passes first, the caller
	// gets the final text without them, as for a language Omi cannot align.
	var text *runtimepkg.BatchTranscription
	// Only the adapter's own poll deadline earns the text fallback; a caller
	// that cancelled, or whose own deadline passed, gets its own error.
	ownDeadline := func() bool { return text != nil && ctx.Err() == nil && pollCtx.Err() != nil }
	for {
		pollRequest, err := http.NewRequestWithContext(pollCtx, http.MethodGet, pollURL.String(), nil)
		if err != nil {
			return nil, err
		}
		pollRequest.Header.Set("Accept", "application/json")
		pollRequest.Header.Set("Authorization", "Bearer "+credential)
		polled, err := batchhttp.Do(a.httpClient, pollRequest, a.maxResponseBytes)
		if err != nil {
			if ownDeadline() {
				return text, nil
			}
			return nil, a.pollError(pollCtx, err)
		}
		wait := a.pollInterval
		switch {
		case polled.Status == http.StatusTooManyRequests || polled.Status >= 500:
			wait = backoff
			backoff = min(backoff*2, a.maxBackoff)
		case polled.Status < 200 || polled.Status >= 300:
			return nil, providerError(fmt.Sprintf("Omi rejected the job poll with status %d", polled.Status), parseError(polled.Body), polled.Status, polled.Body)
		default:
			backoff = a.pollInterval
			var state batchJob
			if err := batchhttp.DecodeJSON(polled.Body, &state); err != nil {
				return nil, err
			}
			done, result, err := a.jobState(pollCtx, endpoint, credential, state, requestID, requested)
			if err != nil && ownDeadline() {
				return text, nil
			}
			if err != nil || done {
				return result, err
			}
			if result != nil {
				text = result
			}
		}
		timer := time.NewTimer(wait)
		select {
		case <-pollCtx.Done():
			timer.Stop()
			if ownDeadline() {
				return text, nil
			}
			return nil, a.pollError(pollCtx, pollCtx.Err())
		case <-timer.C:
		}
	}
}

// pollError classifies a poll that could not finish. The poll deadline is a
// request_timeout like any other stall; a caller's cancellation keeps its own
// classification.
func (a *BatchAdapter) pollError(ctx context.Context, err error) error {
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) {
		classified := batchhttp.TransportError(ctx, err)
		errors.As(classified, &providerErr)
	}
	if providerErr.Code == batchhttp.CodeRequestTimeout && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		providerErr.Message = fmt.Sprintf("the Omi job did not finish within %s", a.pollDeadline)
	}
	return providerErr
}

// jobState reads one poll. A succeeded job's result is inline content (an
// object or a JSON string) or a download_url; a failed, expired or canceled
// job ends with its error code classified.
func (a *BatchAdapter) jobState(ctx context.Context, endpoint *url.URL, credential string, state batchJob, requestID string, requested batchRequest) (bool, *runtimepkg.BatchTranscription, error) {
	switch strings.ToLower(strings.TrimSpace(state.Status)) {
	case "succeeded", "completed", "complete", "done":
	case "failed", "error":
		// A documented code (queue_full, audio_too_long, …) keeps its class;
		// any other failure is the provider's, as with every job API.
		if state.Error != nil && strings.TrimSpace(state.Error.Code) != "" {
			if canonical, _ := classify(state.Error, 0); canonical != batchhttp.CodeInvalidRequest {
				return true, nil, providerError("the Omi job failed", state.Error, 0, marshalData(map[string]string{"code": state.Error.Code}))
			}
			failure := batchhttp.Failed(batchExtensionID, state.Error.Code)
			// Failed keeps the code as a bare string, which SafeProviderCode
			// does not read.
			failure.ProviderCode = state.Error.Code
			return true, nil, failure
		}
		return true, nil, batchhttp.Failed(batchExtensionID, "")
	case "expired", "canceled", "cancelled":
		return true, nil, batchhttp.Failed(batchExtensionID, state.Status)
	// Omi documents awaiting_upload, accepted, running, succeeded and failed
	// (docs.omi.health/async); the rest are tolerated spellings.
	case "awaiting_upload", "accepted", "queued", "pending", "running", "processing", "in_progress", "":
		return false, nil, nil
	default:
		return true, nil, batchhttp.Malformed(fmt.Errorf("omi job status %q", state.Status))
	}
	if state.Result == nil || state.Result.Expired {
		return true, nil, batchhttp.Failed(batchExtensionID, "result expired")
	}
	if content := bytes.TrimSpace(state.Result.Content); len(content) > 0 && string(content) != "null" {
		if content[0] == '"' {
			var inner string
			if err := json.Unmarshal(content, &inner); err != nil {
				return true, nil, batchhttp.Malformed(err)
			}
			content = []byte(inner)
		}
		result, err := parseBatchResponse(content, requestID, requested)
		if err == nil && decorating(content, requested) {
			// The text is final; poll again for the labels the caller asked for.
			return false, result, nil
		}
		return true, result, err
	}
	download := strings.TrimSpace(state.Result.DownloadURL)
	if download == "" {
		return true, nil, batchhttp.Malformed(errors.New("omi job result carries neither content nor download_url"))
	}
	result, still, err := a.download(ctx, endpoint, credential, download, requestID, requested)
	return !still, result, err
}

// download fetches a job's stored result. The bearer goes only to the API's
// own origin; a signed storage URL authenticates itself and must not see the
// key. The URL is never quoted in an error: its signature is a credential.
// The second result reports whether Omi is still attaching the speaker labels
// or word timings the caller asked for, so the poll should fetch it again
// (each poll may carry a fresh download_url).
func (a *BatchAdapter) download(ctx context.Context, endpoint *url.URL, credential, raw, requestID string, requested batchRequest) (*runtimepkg.BatchTranscription, bool, error) {
	target, err := url.Parse(raw)
	if err != nil || target.Hostname() == "" || target.User != nil || target.Fragment != "" {
		return nil, false, batchhttp.Malformed(errors.New("omi job download_url is not a clean absolute URL"))
	}
	if target.Scheme != "https" && !(a.allowInsecureEndpoint && target.Scheme == "http") {
		return nil, false, batchhttp.Malformed(errors.New("omi job download_url must use https"))
	}
	authorize := sameOrigin(endpoint, target)
	downloadRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, false, batchhttp.Malformed(errors.New("omi job download_url could not be requested"))
	}
	downloadRequest.Header.Set("Accept", "application/json")
	if authorize {
		downloadRequest.Header.Set("Authorization", "Bearer "+credential)
	}
	fetched, err := batchhttp.Do(a.httpClient, downloadRequest, a.maxResponseBytes)
	if err != nil {
		return nil, false, withoutURL(err)
	}
	if fetched.Status < 200 || fetched.Status >= 300 {
		// A storage error body may restate the signed URL, so it is not kept.
		return nil, false, providerError(fmt.Sprintf("Omi job result download failed with status %d", fetched.Status), nil, fetched.Status, nil)
	}
	result, err := parseBatchResponse(fetched.Body, requestID, requested)
	return result, err == nil && decorating(fetched.Body, requested), err
}

// sameOrigin resolves a poll URL against the submit endpoint and accepts it
// only on the endpoint's own scheme, host and port, so the bearer never
// leaves the Omi API.
func (a *BatchAdapter) sameOrigin(endpoint *url.URL, raw string) (*url.URL, error) {
	reference, err := url.Parse(raw)
	if err != nil {
		return nil, batchhttp.Malformed(errors.New("omi poll_url is not a URL"))
	}
	resolved := endpoint.ResolveReference(reference)
	if resolved.User != nil || resolved.Fragment != "" || !sameOrigin(endpoint, resolved) {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeProviderError, Message: "Omi named a poll URL off its API origin, which the relay does not call"}
	}
	return resolved, nil
}

func sameOrigin(a, b *url.URL) bool {
	return strings.EqualFold(a.Scheme, b.Scheme) && strings.EqualFold(a.Hostname(), b.Hostname()) && effectivePort(a) == effectivePort(b)
}

func effectivePort(u *url.URL) string {
	if port := u.Port(); port != "" {
		return port
	}
	if strings.EqualFold(u.Scheme, "http") {
		return "80"
	}
	return "443"
}

// withoutURL drops the *url.Error wrapper a transport failure carries, whose
// text quotes the URL, keeping the classification.
func withoutURL(err error) error {
	var providerErr *runtimepkg.ProviderError
	if errors.As(err, &providerErr) {
		var urlErr *url.Error
		if errors.As(providerErr.Cause, &urlErr) {
			providerErr.Cause = urlErr.Err
		}
	}
	return err
}

// batchResponse is the verbose_json answer.
type batchResponse struct {
	Text       *string        `json:"text"`
	Language   string         `json:"language"`
	Model      string         `json:"model"`
	ModelBuild string         `json:"model_build"`
	RequestID  string         `json:"request_id"`
	Duration   float64        `json:"duration"`
	Segments   []batchSegment `json:"segments"`
	Words      []batchWord    `json:"words"`
	// Decoration reports speaker labelling on a diarized job. Omi delivers
	// the final text first; while Status is "processing" the speaker labels
	// and word timings are still being attached (docs.omi.health/async).
	Decoration *struct {
		Status string `json:"status"`
		// StaleAfter is when Omi says to stop waiting for the labels even if
		// Status still reads processing.
		StaleAfter string `json:"stale_after"`
	} `json:"decoration"`
}

// decorating reports whether a result still lacks the speaker labels or word
// timings the caller asked for because Omi is attaching them.
func decorating(body []byte, requested batchRequest) bool {
	if !requested.wantSpeakers && !requested.wantWords {
		return false
	}
	var payload batchResponse
	if json.Unmarshal(body, &payload) != nil || payload.Decoration == nil {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(payload.Decoration.Status), "processing") {
		return false
	}
	if stale, err := time.Parse(time.RFC3339, strings.TrimSpace(payload.Decoration.StaleAfter)); err == nil && !time.Now().Before(stale) {
		// Enrichment stalled: Omi says to stop polling and keep the text.
		return false
	}
	return true
}

type batchSegment struct {
	Text    string          `json:"text"`
	Start   *float64        `json:"start"`
	End     *float64        `json:"end"`
	Speaker json.RawMessage `json:"speaker"`
}

type batchWord struct {
	Word    string          `json:"word"`
	Start   *float64        `json:"start"`
	End     *float64        `json:"end"`
	Speaker json.RawMessage `json:"speaker"`
}

// speakerLabel carries Omi's speaker verbatim, a string or a number.
func speakerLabel(raw json.RawMessage) string {
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return strings.TrimSpace(text)
	}
	var number json.Number
	if json.Unmarshal(raw, &number) == nil {
		return number.String()
	}
	return ""
}

func parseBatchResponse(body []byte, headerRequestID string, requested batchRequest) (*runtimepkg.BatchTranscription, error) {
	var payload batchResponse
	if err := batchhttp.DecodeJSON(body, &payload); err != nil {
		return nil, err
	}
	// Silence answers with an empty text; a body without the field is a
	// response shape this adapter does not understand.
	if payload.Text == nil {
		return nil, batchhttp.Malformed(errors.New("omi transcription carries no text"))
	}
	// A silent model substitution must fail, not bill another model. The US
	// host may omit model, so the build id (<model>@<hash>) is checked too.
	served := strings.TrimSpace(payload.Model)
	built, _, _ := strings.Cut(strings.TrimSpace(payload.ModelBuild), "@")
	if (served != "" && served != requested.model) || (built != "" && built != requested.model) {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeUnavailable, Message: fmt.Sprintf("Omi served model %q, requested %q", firstNonEmpty(served, built), requested.model)}
	}
	requestID := firstNonEmpty(strings.TrimSpace(payload.RequestID), strings.TrimSpace(headerRequestID))
	if len(requestID) > 512 {
		requestID = ""
	}
	// duration is the billable audio, digital silence included. A response
	// without it stays incomplete rather than billing zero or the bytes.
	observation := metering.Duration("request", requested.model, "batch", body, 1000, "duration")
	observation.ProviderRequestID = requestID
	result := &runtimepkg.BatchTranscription{
		Billing:           metering.Report(observation),
		Text:              strings.TrimSpace(*payload.Text),
		DurationMS:        batchhttp.SecondsToMS(payload.Duration),
		ProviderRequestID: requestID,
		Extensions:        batchhttp.RawExtension(batchExtensionID, body),
	}
	result.Language = baseLanguage(payload.Language)
	if result.Language == "" || result.Language == batchAutoLanguage {
		result.Language = ""
		if requested.language != batchAutoLanguage {
			result.Language = requested.language
		}
	}
	for _, segment := range payload.Segments {
		text := strings.TrimSpace(segment.Text)
		if text == "" || segment.Start == nil || segment.End == nil {
			continue
		}
		converted := runtimepkg.BatchSegment{Text: text, StartMS: batchhttp.SecondsToMS(*segment.Start), EndMS: batchhttp.SecondsToMS(*segment.End)}
		if requested.wantSpeakers {
			converted.Speaker = speakerLabel(segment.Speaker)
		}
		result.Segments = append(result.Segments, converted)
	}
	// Words come back only with diarize=true, and some languages answer
	// without them even then; that is not an error.
	words := make([]batchhttp.Word, 0, len(payload.Words))
	for _, word := range payload.Words {
		text := strings.TrimSpace(word.Word)
		if text == "" || word.Start == nil || word.End == nil {
			continue
		}
		timed := batchhttp.Word{Text: text, StartMS: batchhttp.SecondsToMS(*word.Start), EndMS: batchhttp.SecondsToMS(*word.End)}
		if requested.wantSpeakers {
			timed.Speaker = speakerLabel(word.Speaker)
		}
		words = append(words, timed)
	}
	if len(result.Segments) == 0 {
		result.Segments = batchhttp.GroupWords(words, 0)
	}
	if result.Text == "" {
		result.Text = batchhttp.JoinSegments(result.Segments)
	}
	if requested.wantWords {
		for _, word := range words {
			result.Words = append(result.Words, runtimepkg.BatchWord{Text: word.Text, StartMS: word.StartMS, EndMS: word.EndMS, Speaker: word.Speaker})
		}
	}
	return result, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
