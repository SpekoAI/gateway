package speechmatics

import (
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
	// BatchAdapterID identifies the Speechmatics batch jobs implementation.
	BatchAdapterID = "speechmatics.stt.batch.v1"
	// BatchEndpoint is the EU jobs endpoint; a plan may name any documented
	// regional host (eu1, eu2, us1, au1 under asr.api.speechmatics.com) and the
	// job is addressed to the host that created it.
	BatchEndpoint = "https://eu2.asr.api.speechmatics.com/v2/jobs"
	// BatchMaxAudioBytes is the documented ceiling for media sent in the POST
	// body ("< 1 GB").
	BatchMaxAudioBytes int64 = 1 << 30
	// BatchMaxDurationSeconds is not documented per file; Speechmatics bounds
	// accounts by monthly hours. Four hours keeps one job inside the 1 GB
	// body cap at the recommended 16 kHz mono and well inside the vendor's
	// own processing envelope.
	BatchMaxDurationSeconds int64 = 4 * 60 * 60

	batchHostSuffix  = "*.asr.api.speechmatics.com"
	batchExtensionID = "speechmatics.com/v2/jobs"
	defaultBatchPoll = 3 * time.Second
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

// BatchAdapter implements runtime.BatchTranscriber over the batch jobs API:
// create a job with the media in the body, poll its status, fetch the json-v2
// transcript, delete the job.
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
		return nil, errors.New("speechmatics batch limits must be positive")
	}
	policy, err := upstream.NewHTTPPolicy(batchHostSuffix, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &BatchAdapter{id: config.AdapterID, httpClient: config.HTTPClient, maxResponseBytes: config.MaxResponseBytes, pollInterval: config.PollInterval, endpointPolicy: policy}, nil
}

func (a *BatchAdapter) ID() string { return a.id }

// Transcribe runs create → poll → fetch → delete.
func (a *BatchAdapter) Transcribe(ctx context.Context, request runtimepkg.BatchTranscribeRequest) (*runtimepkg.BatchTranscription, error) {
	if request.Plan.Route.Provider != "speechmatics" {
		return nil, fmt.Errorf("speechmatics batch adapter cannot serve provider %q", request.Plan.Route.Provider)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model == "" {
		return nil, errors.New("speechmatics batch adapter requires a model")
	}
	if request.AudioBytes > BatchMaxAudioBytes {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInputTooLarge, Message: "the upload exceeds Speechmatics' 1 GB job body limit"}
	}
	credential, err := batchhttp.Credential(request.Plan)
	if err != nil {
		return nil, err
	}
	jobsURL, err := a.endpointPolicy.Parse(request.Plan.Route.Endpoint)
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

	// The job config is the same transcription_config the realtime frame
	// carries, minus the streaming-only knobs. Language "auto" asks for
	// language identification when the caller pinned nothing.
	transcription := map[string]any{"model": model}
	if language := strings.TrimSpace(request.Options.Language); language != "" {
		transcription["language"] = speechmaticsLanguage(language)
		if locale := outputLocale(language); locale != "" {
			transcription["output_locale"] = locale
		}
	} else {
		transcription["language"] = "auto"
	}
	if request.Options.STT.Diarize() {
		transcription["diarization"] = "speaker"
	}
	if keywords := request.Options.STT.GetKeywords(); len(keywords) > 0 {
		vocab := make([]map[string]string, 0, len(keywords))
		for _, keyword := range keywords {
			vocab = append(vocab, map[string]string{"content": keyword})
		}
		transcription["additional_vocab"] = vocab
	}
	for _, key := range request.Options.STT.ProviderKeys("speechmatics") {
		transcription[key] = request.Options.STT.Provider("speechmatics")[key]
	}
	config, err := json.Marshal(map[string]any{"type": "transcription", "transcription_config": transcription})
	if err != nil {
		return nil, err
	}
	body, contentType := batchhttp.Multipart([]batchhttp.MultipartField{{Name: "config", Value: string(config)}}, "data_file", "audio.wav", "audio/wav", request.Audio)
	create, err := http.NewRequestWithContext(ctx, http.MethodPost, jobsURL.String(), body)
	if err != nil {
		body.Close()
		return nil, err
	}
	create.Header.Set("Content-Type", contentType)
	created, err := exchange(create)
	if err != nil {
		return nil, err
	}
	var job struct {
		ID string `json:"id"`
	}
	if err := batchhttp.DecodeJSON(created.Body, &job); err != nil {
		return nil, err
	}
	if strings.TrimSpace(job.ID) == "" {
		return nil, batchhttp.Malformed(errors.New("speechmatics job creation returned no id"))
	}
	jobURL := *jobsURL
	jobURL.Path = strings.TrimRight(jobsURL.Path, "/") + "/" + url.PathEscape(job.ID)
	defer a.deleteQuietly(ctx, jobURL.String(), authorize)

	var durationMS int64
	err = batchhttp.Poll(ctx, a.pollInterval, func(ctx context.Context) (bool, error) {
		poll, err := http.NewRequestWithContext(ctx, http.MethodGet, jobURL.String(), nil)
		if err != nil {
			return false, err
		}
		polled, err := exchange(poll)
		if err != nil {
			return false, err
		}
		var status struct {
			Job struct {
				Status   string   `json:"status"`
				Duration *float64 `json:"duration"`
				Errors   []struct {
					Message string `json:"message"`
				} `json:"errors"`
			} `json:"job"`
		}
		if err := batchhttp.DecodeJSON(polled.Body, &status); err != nil {
			return false, err
		}
		switch status.Job.Status {
		case "done":
			if status.Job.Duration != nil {
				durationMS = batchhttp.SecondsToMS(*status.Job.Duration)
			}
			return true, nil
		case "rejected", "deleted", "expired":
			detail := status.Job.Status
			if len(status.Job.Errors) > 0 {
				detail = status.Job.Errors[len(status.Job.Errors)-1].Message
			}
			return false, batchhttp.Failed(batchExtensionID, detail)
		case "running", "":
			return false, nil
		default:
			return false, batchhttp.Malformed(fmt.Errorf("speechmatics job status %q", status.Job.Status))
		}
	})
	if err != nil {
		return nil, err
	}

	transcriptURL := jobURL
	transcriptURL.Path += "/transcript"
	transcriptURL.RawQuery = "format=json-v2"
	fetch, err := http.NewRequestWithContext(ctx, http.MethodGet, transcriptURL.String(), nil)
	if err != nil {
		return nil, err
	}
	fetched, err := exchange(fetch)
	if err != nil {
		return nil, err
	}
	var transcript struct {
		Job struct {
			Duration float64 `json:"duration"`
		} `json:"job"`
		Metadata struct {
			TranscriptionConfig struct {
				Language string `json:"language"`
			} `json:"transcription_config"`
		} `json:"metadata"`
		Results []struct {
			Type         string  `json:"type"`
			StartTime    float64 `json:"start_time"`
			EndTime      float64 `json:"end_time"`
			Speaker      string  `json:"speaker"`
			AttachesTo   string  `json:"attaches_to"`
			Alternatives []struct {
				Content  string `json:"content"`
				Language string `json:"language"`
			} `json:"alternatives"`
		} `json:"results"`
	}
	if err := batchhttp.DecodeJSON(fetched.Body, &transcript); err != nil {
		return nil, err
	}
	if durationMS == 0 {
		durationMS = batchhttp.SecondsToMS(transcript.Job.Duration)
	}
	// Punctuation arrives as its own result attached to the preceding word;
	// folding it into that word keeps "there." a single token for grouping.
	var words []batchhttp.Word
	language := ""
	for _, item := range transcript.Results {
		if len(item.Alternatives) == 0 {
			continue
		}
		content := item.Alternatives[0].Content
		if language == "" && item.Alternatives[0].Language != "" {
			language = item.Alternatives[0].Language
		}
		if item.Type == "punctuation" && len(words) > 0 && item.AttachesTo != "next" {
			words[len(words)-1].Text += content
			if end := batchhttp.SecondsToMS(item.EndTime); end > words[len(words)-1].EndMS {
				words[len(words)-1].EndMS = end
			}
			continue
		}
		if item.Type != "word" && item.Type != "entity" {
			continue
		}
		words = append(words, batchhttp.Word{Text: content, StartMS: batchhttp.SecondsToMS(item.StartTime), EndMS: batchhttp.SecondsToMS(item.EndTime), Speaker: speakerLabel(item.Speaker)})
	}
	if language == "" && transcript.Metadata.TranscriptionConfig.Language != "auto" {
		language = transcript.Metadata.TranscriptionConfig.Language
	}
	segments := batchhttp.GroupWords(words, 0)
	return &runtimepkg.BatchTranscription{
		Text:              batchhttp.JoinSegments(segments),
		Segments:          segments,
		Language:          language,
		DurationMS:        durationMS,
		ProviderRequestID: job.ID,
		Extensions:        batchhttp.RawExtension(batchExtensionID, fetched.Body),
	}, nil
}

// speakerLabel drops Speechmatics' "UU" placeholder for unknown speakers so
// an undiarized job yields unlabeled segments rather than one speaker "UU".
func speakerLabel(speaker string) string {
	if speaker == "UU" {
		return ""
	}
	return speaker
}

func (a *BatchAdapter) deleteQuietly(ctx context.Context, target string, authorize func(*http.Request)) {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(cleanup, http.MethodDelete, target, nil)
	if err != nil {
		return
	}
	authorize(request)
	response, err := batchhttp.Client(a.httpClient).Do(request)
	if err != nil {
		return
	}
	response.Body.Close()
}
