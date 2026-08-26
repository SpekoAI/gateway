package smallest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/SpekoAI/gateway/internal/batchhttp"
	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// BatchAdapterID identifies the Pulse pre-recorded implementation.
	BatchAdapterID = "smallest.stt.batch.v1"
	// BatchEndpoint is the pre-recorded Pulse endpoint; the model rides the
	// query string, which the adapter sets.
	BatchEndpoint = "https://api.smallest.ai/waves/v1/stt/"
	// BatchMaxAudioBytes is the documented per-request ceiling ("250 MB").
	BatchMaxAudioBytes int64 = 250_000_000
	// BatchMaxDurationSeconds follows the documented "10 minutes per Session"
	// timeout and the troubleshooting advice to split longer recordings. It is
	// the smallest whole-file bound in the catalog, so long audio arrives here
	// already chunked.
	BatchMaxDurationSeconds int64 = 10 * 60

	batchExtensionID = "smallest.ai/waves/v1/stt"
)

// BatchConfig controls local transport limits for the pre-recorded adapter.
type BatchConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	MaxResponseBytes      int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// BatchAdapter implements runtime.BatchTranscriber over the Pulse
// pre-recorded endpoint: the WAV is the raw request body.
type BatchAdapter struct {
	id               string
	httpClient       *http.Client
	maxResponseBytes int64
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
	if config.MaxResponseBytes < 1 {
		return nil, errors.New("smallest batch maximum response bytes must be positive")
	}
	policy, err := upstream.NewHTTPPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &BatchAdapter{id: config.AdapterID, httpClient: config.HTTPClient, maxResponseBytes: config.MaxResponseBytes, endpointPolicy: policy}, nil
}

func (a *BatchAdapter) ID() string { return a.id }

// Transcribe POSTs the WAV as an octet-stream body.
func (a *BatchAdapter) Transcribe(ctx context.Context, request runtimepkg.BatchTranscribeRequest) (*runtimepkg.BatchTranscription, error) {
	if request.Plan.Route.Provider != "smallest" {
		return nil, fmt.Errorf("smallest batch adapter cannot serve provider %q", request.Plan.Route.Provider)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model == "" {
		return nil, errors.New("smallest batch adapter requires a model")
	}
	if request.AudioBytes > BatchMaxAudioBytes {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInputTooLarge, Message: "the upload exceeds Smallest's 250 MB limit"}
	}
	if request.Media.Channels > 1 {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeUnsupportedMedia, Message: "Smallest pre-recorded transcription accepts mono audio only", Hint: "Downmix the input to mono and try again."}
	}
	credential, err := batchhttp.Credential(request.Plan)
	if err != nil {
		return nil, err
	}
	endpoint, err := a.endpointPolicy.Parse(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}
	query := url.Values{}
	query.Set("model", model)
	query.Set("word_timestamps", "true")
	if language := strings.TrimSpace(request.Options.Language); language != "" {
		query.Set("language", language)
	}
	if request.Options.STT.Diarize() {
		query.Set("diarize", "true")
	}
	for _, key := range request.Options.STT.ProviderKeys("smallest") {
		query.Set(key, protocol.SttOptionString(request.Options.STT.Provider("smallest")[key]))
	}
	endpoint.RawQuery = query.Encode()

	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), request.Audio)
	if err != nil {
		return nil, err
	}
	httpRequest.ContentLength = request.AudioBytes
	httpRequest.Header.Set("Content-Type", "application/octet-stream")
	httpRequest.Header.Set("Accept", "application/json")
	httpRequest.Header.Set("Authorization", "Bearer "+credential)

	response, err := batchhttp.Do(a.httpClient, httpRequest, a.maxResponseBytes)
	if err != nil {
		return nil, err
	}
	if response.Status < 200 || response.Status >= 300 {
		return nil, batchhttp.StatusError(batchExtensionID, response.Status, response.Body)
	}
	var payload struct {
		RequestID     string `json:"request_id"`
		Status        string `json:"status"`
		Text          string `json:"text"`
		Transcription string `json:"transcription"`
		Language      string `json:"language"`
		Words         []struct {
			Word    string  `json:"word"`
			Start   float64 `json:"start"`
			End     float64 `json:"end"`
			Speaker *string `json:"speaker"`
		} `json:"words"`
		Utterances []struct {
			Text    string  `json:"text"`
			Start   float64 `json:"start"`
			End     float64 `json:"end"`
			Speaker *string `json:"speaker"`
		} `json:"utterances"`
		Metadata struct {
			Duration float64 `json:"duration"`
		} `json:"metadata"`
	}
	if err := batchhttp.DecodeJSON(response.Body, &payload); err != nil {
		return nil, err
	}
	if payload.Status != "" && payload.Status != "success" && payload.Status != "completed" {
		return nil, batchhttp.Failed(batchExtensionID, payload.Status)
	}
	result := &runtimepkg.BatchTranscription{
		Text:              strings.TrimSpace(payload.Text),
		Language:          payload.Language,
		DurationMS:        batchhttp.SecondsToMS(payload.Metadata.Duration),
		ProviderRequestID: payload.RequestID,
		Extensions:        batchhttp.RawExtension(batchExtensionID, response.Body),
	}
	if result.Text == "" {
		result.Text = strings.TrimSpace(payload.Transcription)
	}
	if len(payload.Utterances) > 0 {
		for _, utterance := range payload.Utterances {
			text := strings.TrimSpace(utterance.Text)
			if text == "" {
				continue
			}
			speaker := ""
			if utterance.Speaker != nil {
				speaker = *utterance.Speaker
			}
			result.Segments = append(result.Segments, runtimepkg.BatchSegment{Text: text, StartMS: batchhttp.SecondsToMS(utterance.Start), EndMS: batchhttp.SecondsToMS(utterance.End), Speaker: speaker})
		}
	} else {
		words := make([]batchhttp.Word, 0, len(payload.Words))
		for _, word := range payload.Words {
			speaker := ""
			if word.Speaker != nil {
				speaker = *word.Speaker
			}
			words = append(words, batchhttp.Word{Text: word.Word, StartMS: batchhttp.SecondsToMS(word.Start), EndMS: batchhttp.SecondsToMS(word.End), Speaker: speaker})
		}
		result.Segments = batchhttp.GroupWords(words, 0)
	}
	if result.Text == "" {
		result.Text = batchhttp.JoinSegments(result.Segments)
	}
	return result, nil
}
