package assemblyai

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
	// BatchAdapterID identifies the pre-recorded Transcripts API implementation.
	BatchAdapterID = "assemblyai.stt.batch.v1"
	// BatchEndpoint is the transcript submission endpoint. The adapter derives
	// the upload endpoint from the same origin, so a residency host
	// (api.eu.assemblyai.com) moves both steps together.
	BatchEndpoint = "https://api.assemblyai.com/v2/transcript"
	// BatchMaxDurationSeconds is the documented per-file ceiling ("10 hours").
	BatchMaxDurationSeconds int64 = 10 * 60 * 60
	// BatchMaxAudioBytes is the documented ceiling for POST /v2/upload
	// ("2.2 GB"); URL submissions allow 5 GB but this adapter always uploads.
	BatchMaxAudioBytes int64 = 2_200_000_000

	batchAPIHost       = "api.assemblyai.com"
	batchEURegionHost  = "api.eu.assemblyai.com"
	batchExtensionID   = "assemblyai.com/v2/transcript"
	batchUploadPath    = "/v2/upload"
	defaultBatchPoll   = 3 * time.Second
	batchStatusDone    = "completed"
	batchStatusError   = "error"
	batchStatusQueued  = "queued"
	batchStatusWorking = "processing"
)

// BatchConfig controls local transport limits for the pre-recorded adapter.
type BatchConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	MaxResponseBytes      int64
	PollInterval          time.Duration
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// BatchAdapter implements runtime.BatchTranscriber over the async Transcripts
// API: upload the bytes, submit a transcript job against the upload URL, poll
// until it completes.
type BatchAdapter struct {
	id               string
	httpClient       *http.Client
	maxResponseBytes int64
	pollInterval     time.Duration
	endpointPolicy   upstream.HTTPPolicy
}

// NewBatch creates the pre-recorded adapter.
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
		return nil, errors.New("assemblyai batch limits must be positive")
	}
	policy, err := upstream.NewHTTPPolicy(batchAPIHost, append([]string{batchEURegionHost}, config.AllowedEndpointHosts...), config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &BatchAdapter{id: config.AdapterID, httpClient: config.HTTPClient, maxResponseBytes: config.MaxResponseBytes, pollInterval: config.PollInterval, endpointPolicy: policy}, nil
}

func (a *BatchAdapter) ID() string { return a.id }

// Transcribe runs upload → submit → poll and maps the completed transcript.
func (a *BatchAdapter) Transcribe(ctx context.Context, request runtimepkg.BatchTranscribeRequest) (*runtimepkg.BatchTranscription, error) {
	if request.Plan.Route.Provider != "assemblyai" {
		return nil, fmt.Errorf("assemblyai batch adapter cannot serve provider %q", request.Plan.Route.Provider)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model == "" {
		return nil, errors.New("assemblyai batch adapter requires a speech model")
	}
	if request.AudioBytes > BatchMaxAudioBytes {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInputTooLarge, Message: "the upload exceeds AssemblyAI's 2.2 GB upload limit"}
	}
	credential, err := batchhttp.Credential(request.Plan)
	if err != nil {
		return nil, err
	}
	submitURL, err := a.endpointPolicy.Parse(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}
	headers := func(r *http.Request, contentType string) {
		// AssemblyAI takes the raw key with no scheme, on both hosts.
		r.Header.Set("Authorization", credential)
		r.Header.Set("Content-Type", contentType)
		r.Header.Set("Accept", "application/json")
	}

	// Step 1: upload. The bytes go to the provider's own storage; the returned
	// URL is only resolvable with a key from the same project.
	uploadURL := *submitURL
	uploadURL.Path = batchUploadPath
	upload, err := http.NewRequestWithContext(ctx, http.MethodPost, uploadURL.String(), request.Audio)
	if err != nil {
		return nil, err
	}
	upload.ContentLength = request.AudioBytes
	headers(upload, "application/octet-stream")
	uploaded, err := batchhttp.Do(a.httpClient, upload, a.maxResponseBytes)
	if err != nil {
		return nil, err
	}
	if uploaded.Status < 200 || uploaded.Status >= 300 {
		return nil, batchhttp.StatusError(batchExtensionID, uploaded.Status, uploaded.Body)
	}
	var uploadedBody struct {
		UploadURL string `json:"upload_url"`
	}
	if err := batchhttp.DecodeJSON(uploaded.Body, &uploadedBody); err != nil {
		return nil, err
	}
	if strings.TrimSpace(uploadedBody.UploadURL) == "" {
		return nil, batchhttp.Malformed(errors.New("assemblyai upload returned no upload_url"))
	}

	// Step 2: submit.
	submission := map[string]any{
		"audio_url":    uploadedBody.UploadURL,
		"speech_model": model,
		"punctuate":    true,
		"format_text":  true,
	}
	if language := strings.TrimSpace(request.Options.Language); language != "" {
		submission["language_code"] = baseBatchLanguage(language)
	} else {
		submission["language_detection"] = true
	}
	if request.Options.STT.Diarize() {
		submission["speaker_labels"] = true
	}
	if keywords := request.Options.STT.GetKeywords(); len(keywords) > 0 {
		submission["keyterms_prompt"] = keywords
	}
	for _, key := range request.Options.STT.ProviderKeys("assemblyai") {
		submission[key] = request.Options.STT.Provider("assemblyai")[key]
	}
	encoded, err := json.Marshal(submission)
	if err != nil {
		return nil, err
	}
	submit, err := http.NewRequestWithContext(ctx, http.MethodPost, submitURL.String(), bytes.NewReader(encoded))
	if err != nil {
		return nil, err
	}
	headers(submit, "application/json")
	submitted, err := batchhttp.Do(a.httpClient, submit, a.maxResponseBytes)
	if err != nil {
		return nil, err
	}
	if submitted.Status < 200 || submitted.Status >= 300 {
		return nil, batchhttp.StatusError(batchExtensionID, submitted.Status, submitted.Body)
	}
	var job batchTranscript
	if err := batchhttp.DecodeJSON(submitted.Body, &job); err != nil {
		return nil, err
	}
	if strings.TrimSpace(job.ID) == "" {
		return nil, batchhttp.Malformed(errors.New("assemblyai submit returned no transcript id"))
	}

	// Step 3: poll the transcript resource until it is terminal.
	statusURL := *submitURL
	statusURL.Path = strings.TrimRight(submitURL.Path, "/") + "/" + url.PathEscape(job.ID)
	var final batchTranscript
	var finalBody []byte
	err = batchhttp.Poll(ctx, a.pollInterval, func(ctx context.Context) (bool, error) {
		poll, err := http.NewRequestWithContext(ctx, http.MethodGet, statusURL.String(), nil)
		if err != nil {
			return false, err
		}
		headers(poll, "application/json")
		polled, err := batchhttp.Do(a.httpClient, poll, a.maxResponseBytes)
		if err != nil {
			return false, err
		}
		if polled.Status < 200 || polled.Status >= 300 {
			return false, batchhttp.StatusError(batchExtensionID, polled.Status, polled.Body)
		}
		var current batchTranscript
		if err := batchhttp.DecodeJSON(polled.Body, &current); err != nil {
			return false, err
		}
		switch current.Status {
		case batchStatusDone:
			final, finalBody = current, polled.Body
			return true, nil
		case batchStatusError:
			return false, batchhttp.Failed(batchExtensionID, current.Error)
		case batchStatusQueued, batchStatusWorking, "":
			return false, nil
		default:
			return false, batchhttp.Malformed(fmt.Errorf("assemblyai transcript status %q", current.Status))
		}
	})
	if err != nil {
		return nil, err
	}
	return final.result(finalBody), nil
}

type batchTranscript struct {
	ID            string  `json:"id"`
	Status        string  `json:"status"`
	Error         string  `json:"error"`
	Text          *string `json:"text"`
	LanguageCode  string  `json:"language_code"`
	AudioDuration *int64  `json:"audio_duration"`
	Words         []struct {
		Text    string  `json:"text"`
		Start   int64   `json:"start"`
		End     int64   `json:"end"`
		Speaker *string `json:"speaker"`
	} `json:"words"`
	Utterances []struct {
		Text    string  `json:"text"`
		Start   int64   `json:"start"`
		End     int64   `json:"end"`
		Speaker *string `json:"speaker"`
	} `json:"utterances"`
}

func (t batchTranscript) result(body []byte) *runtimepkg.BatchTranscription {
	result := &runtimepkg.BatchTranscription{
		Language:          t.LanguageCode,
		ProviderRequestID: t.ID,
		Extensions:        batchhttp.RawExtension(batchExtensionID, body),
	}
	if t.Text != nil {
		result.Text = strings.TrimSpace(*t.Text)
	}
	if t.AudioDuration != nil {
		result.DurationMS = *t.AudioDuration * 1000
	}
	if len(t.Utterances) > 0 {
		for _, utterance := range t.Utterances {
			text := strings.TrimSpace(utterance.Text)
			if text == "" {
				continue
			}
			result.Segments = append(result.Segments, runtimepkg.BatchSegment{Text: text, StartMS: utterance.Start, EndMS: utterance.End, Speaker: derefString(utterance.Speaker)})
		}
		return result
	}
	words := make([]batchhttp.Word, 0, len(t.Words))
	for _, word := range t.Words {
		words = append(words, batchhttp.Word{Text: word.Text, StartMS: word.Start, EndMS: word.End, Speaker: derefString(word.Speaker)})
	}
	result.Segments = batchhttp.GroupWords(words, 0)
	return result
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// baseBatchLanguage lowers a BCP-47 tag to the two-letter code the
// pre-recorded `language_code` parameter documents (en, es, de…), keeping the
// few region-qualified codes AssemblyAI lists explicitly (en_us, en_uk, en_au,
// pt_br, zh_cn…) in its own underscore spelling.
func baseBatchLanguage(tag string) string {
	tag = strings.ToLower(strings.ReplaceAll(strings.TrimSpace(tag), "_", "-"))
	switch tag {
	case "en-us", "en-gb", "en-au", "pt-br", "pt-pt", "zh-cn", "zh-tw", "es-es", "es-mx":
		return strings.ReplaceAll(tag, "-", "_")
	}
	if i := strings.IndexByte(tag, '-'); i > 0 {
		return tag[:i]
	}
	return tag
}
