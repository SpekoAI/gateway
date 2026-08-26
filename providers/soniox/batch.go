package soniox

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
	// BatchAdapterID identifies the async transcription implementation.
	BatchAdapterID = "soniox.stt.batch.v1"
	// BatchEndpoint creates an async transcription; the files, status and
	// transcript resources hang off the same origin.
	BatchEndpoint = "https://api.soniox.com/v1/transcriptions"
	// BatchModel is the async counterpart of stt-rt-v5: the same v5
	// generation exposed through the file API.
	BatchModel = "stt-async-v5"
	// BatchMaxDurationSeconds is the documented hard per-file cap (300 min).
	BatchMaxDurationSeconds int64 = 300 * 60

	batchAPIHost     = "api.soniox.com"
	batchFilesPath   = "/v1/files"
	batchExtensionID = "soniox.com/v1/transcriptions"
	defaultBatchPoll = 2 * time.Second
	// batchTokenGapMS splits segments on a pause longer than this between
	// tokens of one speaker.
	batchTokenGapMS = 800
)

// BatchConfig controls local transport limits for the async adapter.
type BatchConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	MaxResponseBytes      int64
	PollInterval          time.Duration
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// BatchAdapter implements runtime.BatchTranscriber over Soniox's async API:
// upload the file, create a transcription against it, poll, fetch the
// transcript, then delete both resources — Soniox caps stored transcriptions
// per account, so leaving them behind eventually refuses new jobs.
type BatchAdapter struct {
	id               string
	httpClient       *http.Client
	maxResponseBytes int64
	pollInterval     time.Duration
	endpointPolicy   upstream.HTTPPolicy
}

// NewBatch creates the async adapter.
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
		return nil, errors.New("soniox batch limits must be positive")
	}
	policy, err := upstream.NewHTTPPolicy(batchAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &BatchAdapter{id: config.AdapterID, httpClient: config.HTTPClient, maxResponseBytes: config.MaxResponseBytes, pollInterval: config.PollInterval, endpointPolicy: policy}, nil
}

func (a *BatchAdapter) ID() string { return a.id }

// Transcribe runs upload → create → poll → fetch → delete.
func (a *BatchAdapter) Transcribe(ctx context.Context, request runtimepkg.BatchTranscribeRequest) (*runtimepkg.BatchTranscription, error) {
	if request.Plan.Route.Provider != "soniox" {
		return nil, fmt.Errorf("soniox batch adapter cannot serve provider %q", request.Plan.Route.Provider)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model == "" || strings.Contains(model, "-rt-") {
		return nil, fmt.Errorf("soniox batch adapter requires an async model, got %q", model)
	}
	credential, err := batchhttp.Credential(request.Plan)
	if err != nil {
		return nil, err
	}
	createURL, err := a.endpointPolicy.Parse(request.Plan.Route.Endpoint)
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

	// Step 1: upload the container.
	filesURL := *createURL
	filesURL.Path = batchFilesPath
	body, contentType := batchhttp.Multipart(nil, "file", "audio.wav", "audio/wav", request.Audio)
	upload, err := http.NewRequestWithContext(ctx, http.MethodPost, filesURL.String(), body)
	if err != nil {
		body.Close()
		return nil, err
	}
	upload.Header.Set("Content-Type", contentType)
	uploaded, err := exchange(upload)
	if err != nil {
		return nil, err
	}
	var file struct {
		ID string `json:"id"`
	}
	if err := batchhttp.DecodeJSON(uploaded.Body, &file); err != nil {
		return nil, err
	}
	if strings.TrimSpace(file.ID) == "" {
		return nil, batchhttp.Malformed(errors.New("soniox upload returned no file id"))
	}
	fileURL := filesURL
	fileURL.Path = batchFilesPath + "/" + url.PathEscape(file.ID)
	defer a.deleteQuietly(ctx, fileURL.String(), authorize)

	// Step 2: create the transcription.
	creation := map[string]any{
		"file_id":                    file.ID,
		"model":                      model,
		"enable_speaker_diarization": request.Options.STT.Diarize(),
	}
	if language := strings.TrimSpace(request.Options.Language); language != "" {
		creation["language_hints"] = []string{baseLanguage(language)}
	} else {
		creation["enable_language_identification"] = true
	}
	if keywords := request.Options.STT.GetKeywords(); len(keywords) > 0 {
		creation["context"] = map[string]any{"terms": keywords}
	}
	if reservation := strings.TrimSpace(request.Plan.Reservation.ID); reservation != "" {
		creation["client_reference_id"] = "speko_reservation:" + reservation
	}
	for _, key := range request.Options.STT.ProviderKeys("soniox") {
		creation[key] = request.Options.STT.Provider("soniox")[key]
	}
	encoded, err := json.Marshal(creation)
	if err != nil {
		return nil, err
	}
	create, err := http.NewRequestWithContext(ctx, http.MethodPost, createURL.String(), bytes.NewReader(encoded))
	if err != nil {
		return nil, err
	}
	create.Header.Set("Content-Type", "application/json")
	created, err := exchange(create)
	if err != nil {
		return nil, err
	}
	var job batchJob
	if err := batchhttp.DecodeJSON(created.Body, &job); err != nil {
		return nil, err
	}
	if strings.TrimSpace(job.ID) == "" {
		return nil, batchhttp.Malformed(errors.New("soniox create returned no transcription id"))
	}
	statusURL := *createURL
	statusURL.Path = strings.TrimRight(createURL.Path, "/") + "/" + url.PathEscape(job.ID)
	defer a.deleteQuietly(ctx, statusURL.String(), authorize)

	// Step 3: poll.
	var final batchJob
	err = batchhttp.Poll(ctx, a.pollInterval, func(ctx context.Context) (bool, error) {
		poll, err := http.NewRequestWithContext(ctx, http.MethodGet, statusURL.String(), nil)
		if err != nil {
			return false, err
		}
		polled, err := exchange(poll)
		if err != nil {
			return false, err
		}
		var current batchJob
		if err := batchhttp.DecodeJSON(polled.Body, &current); err != nil {
			return false, err
		}
		switch current.Status {
		case "completed":
			final = current
			return true, nil
		case "error":
			return false, batchhttp.Failed(batchExtensionID, current.ErrorMessage)
		case "queued", "processing", "":
			return false, nil
		default:
			return false, batchhttp.Malformed(fmt.Errorf("soniox transcription status %q", current.Status))
		}
	})
	if err != nil {
		return nil, err
	}

	// Step 4: fetch the transcript.
	transcriptURL := statusURL
	transcriptURL.Path += "/transcript"
	fetch, err := http.NewRequestWithContext(ctx, http.MethodGet, transcriptURL.String(), nil)
	if err != nil {
		return nil, err
	}
	fetched, err := exchange(fetch)
	if err != nil {
		return nil, err
	}
	var transcript struct {
		Text   string `json:"text"`
		Tokens []struct {
			Text     string  `json:"text"`
			StartMS  int64   `json:"start_ms"`
			EndMS    int64   `json:"end_ms"`
			Speaker  *string `json:"speaker"`
			Language string  `json:"language"`
		} `json:"tokens"`
	}
	if err := batchhttp.DecodeJSON(fetched.Body, &transcript); err != nil {
		return nil, err
	}
	result := &runtimepkg.BatchTranscription{
		Text:              strings.TrimSpace(transcript.Text),
		DurationMS:        final.AudioDurationMS,
		ProviderRequestID: job.ID,
		Extensions:        batchhttp.RawExtension(batchExtensionID, fetched.Body),
	}
	// Soniox tokens are sub-word pieces that carry their own leading spaces,
	// so segments are built by concatenation rather than space-joining.
	var current *runtimepkg.BatchSegment
	var buffer strings.Builder
	flush := func() {
		if current != nil {
			current.Text = strings.TrimSpace(buffer.String())
			if current.Text != "" {
				result.Segments = append(result.Segments, *current)
			}
		}
		current = nil
		buffer.Reset()
	}
	for _, token := range transcript.Tokens {
		if strings.TrimSpace(token.Text) == "" && current == nil {
			continue
		}
		speaker := ""
		if token.Speaker != nil {
			speaker = *token.Speaker
		}
		if result.Language == "" && token.Language != "" {
			result.Language = token.Language
		}
		if current != nil && (speaker != current.Speaker || token.StartMS-current.EndMS > batchTokenGapMS) {
			flush()
		}
		if current == nil {
			current = &runtimepkg.BatchSegment{StartMS: token.StartMS, EndMS: token.EndMS, Speaker: speaker}
		}
		if token.EndMS > current.EndMS {
			current.EndMS = token.EndMS
		}
		buffer.WriteString(token.Text)
	}
	flush()
	if result.Text == "" {
		result.Text = batchhttp.JoinSegments(result.Segments)
	}
	return result, nil
}

type batchJob struct {
	ID              string `json:"id"`
	Status          string `json:"status"`
	ErrorMessage    string `json:"error_message"`
	AudioDurationMS int64  `json:"audio_duration_ms"`
}

// deleteQuietly releases a stored file or transcription. Cleanup failures are
// swallowed: the transcript is already in hand and Soniox expires stored
// resources on its own schedule; a cleanup error must never fail the job.
func (a *BatchAdapter) deleteQuietly(ctx context.Context, target string, authorize func(*http.Request)) {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(cleanup, http.MethodDelete, target, nil)
	if err != nil {
		return
	}
	authorize(request)
	// Through batchhttp.Do, never the raw client: the bearer is on this
	// request too, and only Do refuses the redirect that would replay it.
	_, _ = batchhttp.Do(a.httpClient, request, 1<<10)
}

func baseLanguage(tag string) string {
	tag = strings.ToLower(strings.ReplaceAll(strings.TrimSpace(tag), "_", "-"))
	if i := strings.IndexByte(tag, '-'); i > 0 {
		return tag[:i]
	}
	return tag
}
