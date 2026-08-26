package alibaba

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/SpekoAI/gateway/internal/batchhttp"
	"github.com/SpekoAI/gateway/internal/upstream"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// BatchAdapterID identifies the DashScope file transcription implementation.
	BatchAdapterID = "alibaba.stt.batch.v1"
	// BatchEndpoint is DashScope's asynchronous file-transcription task
	// endpoint on the international host. It takes a public URL only — no
	// upload step and no body audio — so this adapter requires
	// BatchTranscribeRequest.SourceURL.
	BatchEndpoint = "https://dashscope-intl.aliyuncs.com/api/v1/services/audio/asr/transcription"
	// BatchModel is the file-transcription twin of qwen3-asr-flash-realtime.
	BatchModel = "qwen3-asr-flash-filetrans"
	// BatchMaxDurationSeconds is the documented per-file ceiling ("12 hours").
	BatchMaxDurationSeconds int64 = 12 * 60 * 60
	// BatchMaxAudioBytes is the documented per-file ceiling ("2 GB").
	BatchMaxAudioBytes int64 = 2 << 30

	batchExtensionID = "dashscope.aliyuncs.com/api/v1/services/audio/asr/transcription"
	batchTasksPath   = "/api/v1/tasks/"
	defaultBatchPoll = 3 * time.Second
	// batchResultMaxBytes bounds the transcription document fetched from the
	// signed result URL DashScope hands back.
	batchResultMaxBytes int64 = 64 << 20
)

// BatchConfig controls local transport limits for the file adapter.
type BatchConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	MaxResponseBytes      int64
	PollInterval          time.Duration
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// BatchAdapter implements runtime.BatchTranscriber over DashScope's async
// tasks: submit the URL, poll the task, fetch the result document the task
// points at.
type BatchAdapter struct {
	id               string
	httpClient       *http.Client
	maxResponseBytes int64
	pollInterval     time.Duration
	endpointPolicy   upstream.HTTPPolicy
}

// NewBatch creates the file adapter.
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
		return nil, errors.New("alibaba batch limits must be positive")
	}
	policy, err := upstream.NewHTTPPolicy(InternationalAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &BatchAdapter{id: config.AdapterID, httpClient: config.HTTPClient, maxResponseBytes: config.MaxResponseBytes, pollInterval: config.PollInterval, endpointPolicy: policy}, nil
}

func (a *BatchAdapter) ID() string { return a.id }

// Transcribe runs submit → poll → fetch result.
func (a *BatchAdapter) Transcribe(ctx context.Context, request runtimepkg.BatchTranscribeRequest) (*runtimepkg.BatchTranscription, error) {
	if request.Plan.Route.Provider != "alibaba" {
		return nil, fmt.Errorf("alibaba batch adapter cannot serve provider %q", request.Plan.Route.Provider)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model == "" || strings.Contains(model, "realtime") {
		return nil, fmt.Errorf("alibaba batch adapter requires a file-transcription model, got %q", model)
	}
	if request.AudioBytes > BatchMaxAudioBytes {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInputTooLarge, Message: "the upload exceeds DashScope's 2 GB file limit"}
	}
	if request.Options.STT.Diarize() {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInvalidRequest, Message: "Qwen file transcription does not label speakers", Hint: "Drop the diarization option or choose a provider that diarizes."}
	}
	sourceURL := strings.TrimSpace(request.SourceURL)
	if sourceURL == "" || !strings.HasPrefix(strings.ToLower(sourceURL), "https://") {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInvalidRequest, Message: "DashScope file transcription needs the audio at an HTTPS URL", Hint: "This route requires hosted audio; the relay supplies it for job submissions."}
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
		r.Header.Set("Authorization", "Bearer "+credential)
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

	parameters := map[string]any{}
	if language := strings.TrimSpace(request.Options.Language); language != "" {
		parameters["language_hints"] = []string{baseLanguageTag(language)}
	}
	for _, key := range request.Options.STT.ProviderKeys("alibaba") {
		parameters[key] = request.Options.STT.Provider("alibaba")[key]
	}
	encoded, err := json.Marshal(map[string]any{
		"model":      model,
		"input":      map[string]any{"file_url": sourceURL},
		"parameters": parameters,
	})
	if err != nil {
		return nil, err
	}
	submit, err := http.NewRequestWithContext(ctx, http.MethodPost, submitURL.String(), bytes.NewReader(encoded))
	if err != nil {
		return nil, err
	}
	submit.Header.Set("Content-Type", "application/json")
	// The async header is what makes this endpoint return a task rather than
	// refusing a long file.
	submit.Header.Set("X-DashScope-Async", "enable")
	submitted, err := exchange(submit)
	if err != nil {
		return nil, err
	}
	var task batchTask
	if err := batchhttp.DecodeJSON(submitted.Body, &task); err != nil {
		return nil, err
	}
	if strings.TrimSpace(task.Output.TaskID) == "" {
		return nil, batchhttp.Malformed(errors.New("dashscope submit returned no task_id"))
	}

	taskURL := *submitURL
	taskURL.Path = batchTasksPath + url.PathEscape(task.Output.TaskID)
	var final batchTask
	err = batchhttp.Poll(ctx, a.pollInterval, func(ctx context.Context) (bool, error) {
		poll, err := http.NewRequestWithContext(ctx, http.MethodGet, taskURL.String(), nil)
		if err != nil {
			return false, err
		}
		polled, err := exchange(poll)
		if err != nil {
			return false, err
		}
		var current batchTask
		if err := batchhttp.DecodeJSON(polled.Body, &current); err != nil {
			return false, err
		}
		switch current.Output.TaskStatus {
		case "SUCCEEDED":
			final = current
			return true, nil
		case "FAILED", "CANCELED", "UNKNOWN":
			detail := current.Output.Message
			if detail == "" {
				detail = current.Output.Code
			}
			return false, batchhttp.Failed(batchExtensionID, detail)
		case "PENDING", "RUNNING", "":
			return false, nil
		default:
			return false, batchhttp.Malformed(fmt.Errorf("dashscope task status %q", current.Output.TaskStatus))
		}
	})
	if err != nil {
		return nil, err
	}

	resultURL := final.Output.Result.TranscriptionURL
	if resultURL == "" && len(final.Output.Results) > 0 {
		if final.Output.Results[0].SubtaskStatus != "" && final.Output.Results[0].SubtaskStatus != "SUCCEEDED" {
			return nil, batchhttp.Failed(batchExtensionID, final.Output.Results[0].Message)
		}
		resultURL = final.Output.Results[0].TranscriptionURL
	}
	if !strings.HasPrefix(strings.ToLower(resultURL), "https://") {
		return nil, batchhttp.Malformed(errors.New("dashscope task carries no https transcription_url"))
	}
	// The result lives at a signed object-storage URL the provider chose;
	// it is fetched without credentials and bounded like any other body.
	fetch, err := http.NewRequestWithContext(ctx, http.MethodGet, resultURL, nil)
	if err != nil {
		return nil, err
	}
	fetched, err := batchhttp.Do(a.httpClient, fetch, batchResultMaxBytes)
	if err != nil {
		return nil, err
	}
	if fetched.Status < 200 || fetched.Status >= 300 {
		return nil, batchhttp.StatusError(batchExtensionID, fetched.Status, nil)
	}
	var document struct {
		Transcripts []struct {
			ChannelID int    `json:"channel_id"`
			Text      string `json:"text"`
			Sentences []struct {
				BeginTime int64  `json:"begin_time"`
				EndTime   int64  `json:"end_time"`
				Text      string `json:"text"`
				Language  string `json:"language"`
			} `json:"sentences"`
		} `json:"transcripts"`
	}
	if err := batchhttp.DecodeJSON(fetched.Body, &document); err != nil {
		return nil, err
	}
	if len(document.Transcripts) == 0 {
		return nil, batchhttp.Malformed(errors.New("dashscope transcription document carries no transcripts"))
	}
	first := document.Transcripts[0]
	result := &runtimepkg.BatchTranscription{
		Text:              strings.TrimSpace(first.Text),
		DurationMS:        final.Usage.Seconds * 1000,
		ProviderRequestID: task.Output.TaskID,
		Extensions:        batchhttp.RawExtension(batchExtensionID, fetched.Body),
	}
	for _, sentence := range first.Sentences {
		text := strings.TrimSpace(sentence.Text)
		if text == "" {
			continue
		}
		if result.Language == "" && sentence.Language != "" {
			result.Language = sentence.Language
		}
		result.Segments = append(result.Segments, runtimepkg.BatchSegment{Text: text, StartMS: sentence.BeginTime, EndMS: sentence.EndTime})
	}
	if result.Text == "" {
		result.Text = batchhttp.JoinSegments(result.Segments)
	}
	return result, nil
}

type batchTask struct {
	RequestID string `json:"request_id"`
	Output    struct {
		TaskID     string `json:"task_id"`
		TaskStatus string `json:"task_status"`
		Code       string `json:"code"`
		Message    string `json:"message"`
		Result     struct {
			TranscriptionURL string `json:"transcription_url"`
		} `json:"result"`
		Results []struct {
			SubtaskStatus    string `json:"subtask_status"`
			TranscriptionURL string `json:"transcription_url"`
			Message          string `json:"message"`
		} `json:"results"`
	} `json:"output"`
	Usage struct {
		Seconds int64 `json:"seconds"`
	} `json:"usage"`
}
