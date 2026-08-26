package hamsa

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/SpekoAI/gateway/internal/batchhttp"
	"github.com/SpekoAI/gateway/internal/upstream"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// BatchAdapterID identifies the Hamsa batch jobs implementation.
	BatchAdapterID = "hamsa.stt.batch.v1"
	// BatchEndpoint submits a transcription job. Hamsa's batch API takes a
	// media URL only — no upload step and no request body audio — so this
	// adapter requires BatchTranscribeRequest.SourceURL.
	BatchEndpoint = "https://api.tryhamsa.com/v1/jobs/transcribe"
	// BatchModelGeneral and BatchModelConversational are the batch model
	// family; the realtime s2/s3 models have no documented batch twin.
	BatchModelGeneral        = "Hamsa-General-V2.0"
	BatchModelConversational = "Hamsa-Conversational-V1.0"
	// BatchMaxDurationSeconds is the documented per-job ceiling ("60 minutes").
	BatchMaxDurationSeconds int64 = 60 * 60
	// BatchMaxAudioBytes is the documented per-file ceiling ("500 MB").
	BatchMaxAudioBytes int64 = 500_000_000

	batchExtensionID = "tryhamsa.com/v1/jobs"
	batchJobsPath    = "/v1/jobs"
	defaultBatchPoll = 5 * time.Second
)

// BatchConfig controls local transport limits for the jobs adapter.
type BatchConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	MaxResponseBytes      int64
	PollInterval          time.Duration
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// BatchAdapter implements runtime.BatchTranscriber over Hamsa's async jobs:
// submit the media URL, poll the job until it completes.
//
// Hamsa documents the submit and status envelopes but not the completed
// result document; the parser below accepts the shapes the STT product
// advertises (text, speaker-labelled segments, word timings) and treats a
// completed job with none of them as malformed rather than as silence.
type BatchAdapter struct {
	id               string
	httpClient       *http.Client
	maxResponseBytes int64
	pollInterval     time.Duration
	endpointPolicy   upstream.HTTPPolicy
}

// NewBatch creates the jobs adapter.
func NewBatch(config BatchConfig) (*BatchAdapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = BatchAdapterID
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = batchhttp.DefaultMaxResponseBytes
	}
	if config.PollInterval == 0 {
		config.PollInterval = defaultBatchPoll
	}
	if config.MaxResponseBytes < 1 || config.PollInterval < 0 {
		return nil, errors.New("hamsa batch limits must be positive")
	}
	policy, err := upstream.NewHTTPPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &BatchAdapter{id: config.AdapterID, httpClient: config.HTTPClient, maxResponseBytes: config.MaxResponseBytes, pollInterval: config.PollInterval, endpointPolicy: policy}, nil
}

func (a *BatchAdapter) ID() string { return a.id }

// Transcribe runs submit → poll.
func (a *BatchAdapter) Transcribe(ctx context.Context, request runtimepkg.BatchTranscribeRequest) (*runtimepkg.BatchTranscription, error) {
	if request.Plan.Route.Provider != "hamsa" {
		return nil, fmt.Errorf("hamsa batch adapter cannot serve provider %q", request.Plan.Route.Provider)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model == "" || model == "s2" || model == "s3" {
		return nil, fmt.Errorf("hamsa batch adapter requires a batch model, got %q", model)
	}
	if request.AudioBytes > BatchMaxAudioBytes {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInputTooLarge, Message: "the upload exceeds Hamsa's 500 MB limit"}
	}
	sourceURL := strings.TrimSpace(request.SourceURL)
	if sourceURL == "" || !strings.HasPrefix(strings.ToLower(sourceURL), "https://") {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInvalidRequest, Message: "Hamsa batch transcription needs the audio at an HTTPS URL", Hint: "This route requires hosted audio; the relay supplies it for job submissions."}
	}
	credential, err := batchhttp.Credential(request.Plan)
	if err != nil {
		return nil, err
	}
	submitURL, err := a.endpointPolicy.Parse(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}
	authorize := func(r *http.Request) {
		r.Header.Set("Authorization", "Token "+credential)
		r.Header.Set("Accept", "application/json")
	}
	exchange := func(r *http.Request) (*batchhttp.Response, error) {
		authorize(r)
		response, err := batchhttp.Do(a.httpClient, r, a.maxResponseBytes)
		if err != nil {
			return nil, err
		}
		if response.Status < 200 || response.Status >= 300 {
			return nil, batchhttp.StatusError(batchExtensionID, response.Status, response.Body)
		}
		return response, nil
	}

	submission := map[string]any{
		"mediaUrl":       sourceURL,
		"model":          model,
		"processingType": "async",
	}
	if language := normalizeLanguage(request.Options.Language); language != "" {
		submission["language"] = language
	}
	if request.Options.STT.Diarize() {
		submission["diarization"] = true
	}
	for _, key := range request.Options.STT.ProviderKeys("hamsa") {
		submission[key] = request.Options.STT.Provider("hamsa")[key]
	}
	encoded, err := json.Marshal(submission)
	if err != nil {
		return nil, err
	}
	submit, err := http.NewRequestWithContext(ctx, http.MethodPost, submitURL.String(), bytes.NewReader(encoded))
	if err != nil {
		return nil, err
	}
	submit.Header.Set("Content-Type", "application/json")
	submitted, err := exchange(submit)
	if err != nil {
		return nil, err
	}
	var accepted struct {
		Data struct {
			JobID string `json:"jobId"`
		} `json:"data"`
	}
	if err := batchhttp.DecodeJSON(submitted.Body, &accepted); err != nil {
		return nil, err
	}
	jobID := strings.TrimSpace(accepted.Data.JobID)
	if jobID == "" {
		return nil, batchhttp.Malformed(errors.New("hamsa submit returned no jobId"))
	}

	statusURL := *submitURL
	statusURL.Path = batchJobsPath
	statusURL.RawQuery = "jobId=" + jobID
	var result *runtimepkg.BatchTranscription
	err = batchhttp.Poll(ctx, a.pollInterval, func(ctx context.Context) (bool, error) {
		poll, err := http.NewRequestWithContext(ctx, http.MethodGet, statusURL.String(), nil)
		if err != nil {
			return false, err
		}
		polled, err := exchange(poll)
		if err != nil {
			return false, err
		}
		var envelope struct {
			Data batchJob `json:"data"`
		}
		if err := batchhttp.DecodeJSON(polled.Body, &envelope); err != nil {
			return false, err
		}
		switch strings.ToUpper(envelope.Data.Status) {
		case "COMPLETED", "SUCCESS", "SUCCEEDED":
			parsed, err := envelope.Data.result(jobID, polled.Body)
			if err != nil {
				return false, err
			}
			result = parsed
			return true, nil
		case "FAILED", "ERROR", "CANCELLED":
			return false, batchhttp.Failed(batchExtensionID, envelope.Data.Error)
		case "PENDING", "PROCESSING", "QUEUED", "RUNNING", "":
			return false, nil
		default:
			return false, batchhttp.Malformed(fmt.Errorf("hamsa job status %q", envelope.Data.Status))
		}
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

type batchJob struct {
	Status      string          `json:"status"`
	Error       string          `json:"error"`
	UsageTime   json.RawMessage `json:"usageTime"`
	JobResponse json.RawMessage `json:"jobResponse"`
}

// batchDocument is the union of the result shapes Hamsa's STT product
// advertises. Unknown fields are ignored; a document with no text and no
// timed units is refused.
type batchDocument struct {
	Text       string          `json:"text"`
	Transcript string          `json:"transcript"`
	Language   string          `json:"language"`
	Duration   json.RawMessage `json:"duration"`
	Segments   []batchSpan     `json:"segments"`
	Utterances []batchSpan     `json:"utterances"`
	Words      []batchSpan     `json:"words"`
}

type batchSpan struct {
	Text    string          `json:"text"`
	Word    string          `json:"word"`
	Start   json.RawMessage `json:"start"`
	End     json.RawMessage `json:"end"`
	Speaker json.RawMessage `json:"speaker"`
}

func (j batchJob) result(jobID string, body []byte) (*runtimepkg.BatchTranscription, error) {
	var document batchDocument
	raw := bytes.TrimSpace(j.JobResponse)
	if len(raw) == 0 || string(raw) == "null" {
		return nil, batchhttp.Malformed(errors.New("hamsa completed job carries no jobResponse"))
	}
	// jobResponse is documented loosely; accept it as an object or as a JSON
	// string holding an object.
	if raw[0] == '"' {
		var inner string
		if err := json.Unmarshal(raw, &inner); err != nil {
			return nil, batchhttp.Malformed(err)
		}
		raw = []byte(inner)
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		return nil, batchhttp.Malformed(err)
	}
	result := &runtimepkg.BatchTranscription{
		Text:              strings.TrimSpace(document.Text),
		Language:          document.Language,
		DurationMS:        flexibleSecondsToMS(document.Duration),
		ProviderRequestID: jobID,
		Extensions:        batchhttp.RawExtension(batchExtensionID, body),
	}
	if result.Text == "" {
		result.Text = strings.TrimSpace(document.Transcript)
	}
	if result.DurationMS == 0 {
		result.DurationMS = flexibleSecondsToMS(j.UsageTime)
	}
	spans := document.Segments
	if len(spans) == 0 {
		spans = document.Utterances
	}
	if len(spans) > 0 {
		for _, span := range spans {
			text := strings.TrimSpace(span.Text)
			if text == "" {
				text = strings.TrimSpace(span.Word)
			}
			if text == "" {
				continue
			}
			result.Segments = append(result.Segments, runtimepkg.BatchSegment{Text: text, StartMS: flexibleSecondsToMS(span.Start), EndMS: flexibleSecondsToMS(span.End), Speaker: flexibleString(span.Speaker)})
		}
	} else if len(document.Words) > 0 {
		words := make([]batchhttp.Word, 0, len(document.Words))
		for _, word := range document.Words {
			text := word.Word
			if text == "" {
				text = word.Text
			}
			words = append(words, batchhttp.Word{Text: text, StartMS: flexibleSecondsToMS(word.Start), EndMS: flexibleSecondsToMS(word.End), Speaker: flexibleString(word.Speaker)})
		}
		result.Segments = batchhttp.GroupWords(words, 0)
	}
	if result.Text == "" {
		result.Text = batchhttp.JoinSegments(result.Segments)
	}
	if result.Text == "" && len(result.Segments) == 0 {
		return nil, batchhttp.Malformed(errors.New("hamsa completed job carries no transcript"))
	}
	return result, nil
}

// flexibleSecondsToMS reads a timing that Hamsa may serialize as a number of
// seconds or as a numeric string.
func flexibleSecondsToMS(raw json.RawMessage) int64 {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return 0
	}
	var seconds float64
	if err := json.Unmarshal(raw, &seconds); err == nil {
		return batchhttp.SecondsToMS(seconds)
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		if _, err := fmt.Sscanf(strings.TrimSpace(text), "%g", &seconds); err == nil {
			return batchhttp.SecondsToMS(seconds)
		}
	}
	return 0
}

func flexibleString(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text
	}
	return string(raw)
}
