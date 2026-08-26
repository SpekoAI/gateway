package modulate

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/SpekoAI/gateway/internal/batchhttp"
	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// BatchAdapterID identifies the Velma batch implementation.
	BatchAdapterID = "modulate.stt.batch.v1"
	// BatchEndpointMultilingual is the batch twin of the multilingual
	// streaming model; BatchEndpointEnglishFast pairs with English Fast. The
	// model is selected by PATH on this API, so each catalog row carries its
	// own endpoint and the model id names the path's last segment.
	BatchEndpointMultilingual = "https://platform.modulate.ai/api/velma-2-stt-batch"
	BatchEndpointEnglishFast  = "https://platform.modulate.ai/api/velma-2-stt-batch-english-vfast"
	// BatchModelMultilingual and BatchModelEnglishFast are the batch model ids.
	BatchModelMultilingual = "velma-2-stt-batch"
	BatchModelEnglishFast  = "velma-2-stt-batch-english-vfast"
	// BatchMaxAudioBytes is the documented upload ceiling ("100 MB", 413
	// above). It binds long before any duration: 16 kHz mono WAV crosses it at
	// about 52 minutes.
	BatchMaxAudioBytes int64 = 100_000_000
	// BatchMaxDurationSeconds is a local bound consistent with the byte cap at
	// the lowest rate this gateway carries (8 kHz mono, ~104 min).
	BatchMaxDurationSeconds int64 = 100 * 60

	batchExtensionID = "modulate.ai/api/velma-2-stt-batch"
)

// BatchConfig controls local transport limits for the batch adapter.
type BatchConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	MaxResponseBytes      int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// BatchAdapter implements runtime.BatchTranscriber over the Velma batch
// endpoints.
type BatchAdapter struct {
	id               string
	httpClient       *http.Client
	maxResponseBytes int64
	endpointPolicy   upstream.HTTPPolicy
}

// NewBatch creates the batch adapter.
func NewBatch(config BatchConfig) (*BatchAdapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = BatchAdapterID
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = batchhttp.DefaultMaxResponseBytes
	}
	if config.MaxResponseBytes < 1 {
		return nil, errors.New("modulate batch maximum response bytes must be positive")
	}
	policy, err := upstream.NewHTTPPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &BatchAdapter{id: config.AdapterID, httpClient: config.HTTPClient, maxResponseBytes: config.MaxResponseBytes, endpointPolicy: policy}, nil
}

func (a *BatchAdapter) ID() string { return a.id }

// Transcribe POSTs the WAV as the multipart `upload_file`.
func (a *BatchAdapter) Transcribe(ctx context.Context, request runtimepkg.BatchTranscribeRequest) (*runtimepkg.BatchTranscription, error) {
	if request.Plan.Route.Provider != "modulate" {
		return nil, fmt.Errorf("modulate batch adapter cannot serve provider %q", request.Plan.Route.Provider)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if request.AudioBytes > BatchMaxAudioBytes {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInputTooLarge, Message: "the upload exceeds Modulate's 100 MB batch limit"}
	}
	credential, err := batchhttp.Credential(request.Plan)
	if err != nil {
		return nil, err
	}
	endpoint, err := a.endpointPolicy.Parse(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}
	// The endpoint path IS the model; a plan whose model and path disagree
	// would bill one model and run another.
	if !strings.HasSuffix(endpoint.Path, "/"+model) || !strings.Contains(model, "-batch") {
		return nil, fmt.Errorf("modulate batch endpoint %s does not serve model %q", endpoint.Path, model)
	}
	language := strings.TrimSpace(request.Options.Language)
	if language != "" && !languageTagPattern.MatchString(language) {
		return nil, fmt.Errorf("modulate stt language %q is not a valid language tag", request.Options.Language)
	}
	if model == BatchModelEnglishFast && language != "" && primaryLanguage(language) != "en" {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInvalidRequest, Message: "Modulate English Fast transcribes English only", Hint: "Choose the multilingual Modulate model for this language."}
	}
	var fields []batchhttp.MultipartField
	if language != "" && model != BatchModelEnglishFast {
		fields = append(fields, batchhttp.MultipartField{Name: "language", Value: primaryLanguage(language)})
	}
	if request.Options.STT.Diarize() {
		fields = append(fields, batchhttp.MultipartField{Name: "diarize", Value: "true"})
	}
	for _, key := range request.Options.STT.ProviderKeys("modulate") {
		fields = append(fields, batchhttp.MultipartField{Name: key, Value: protocol.SttOptionString(request.Options.STT.Provider("modulate")[key])})
	}
	body, contentType := batchhttp.Multipart(fields, "upload_file", "audio.wav", "audio/wav", request.Audio)
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), body)
	if err != nil {
		body.Close()
		return nil, err
	}
	httpRequest.Header.Set("Content-Type", contentType)
	httpRequest.Header.Set("Accept", "application/json")
	httpRequest.Header.Set("X-API-Key", credential)

	response, err := batchhttp.Do(a.httpClient, httpRequest, a.maxResponseBytes)
	if err != nil {
		return nil, err
	}
	if response.Status < 200 || response.Status >= 300 {
		return nil, batchhttp.StatusError(batchExtensionID, response.Status, response.Body)
	}
	var payload struct {
		Text       string `json:"text"`
		DurationMS int64  `json:"duration_ms"`
		Utterances []struct {
			Text       string `json:"text"`
			StartMS    int64  `json:"start_ms"`
			DurationMS int64  `json:"duration_ms"`
			Speaker    *int   `json:"speaker"`
			Language   string `json:"language"`
		} `json:"utterances"`
	}
	if err := batchhttp.DecodeJSON(response.Body, &payload); err != nil {
		return nil, err
	}
	result := &runtimepkg.BatchTranscription{
		Text:              strings.TrimSpace(payload.Text),
		DurationMS:        payload.DurationMS,
		ProviderRequestID: response.Header.Get("x-request-id"),
		Extensions:        batchhttp.RawExtension(batchExtensionID, response.Body),
	}
	for _, utterance := range payload.Utterances {
		text := strings.TrimSpace(utterance.Text)
		if text == "" {
			continue
		}
		speaker := ""
		if utterance.Speaker != nil {
			speaker = fmt.Sprintf("%d", *utterance.Speaker)
		}
		if result.Language == "" && utterance.Language != "" {
			result.Language = utterance.Language
		}
		result.Segments = append(result.Segments, runtimepkg.BatchSegment{Text: text, StartMS: utterance.StartMS, EndMS: utterance.StartMS + utterance.DurationMS, Speaker: speaker})
	}
	if result.Text == "" {
		result.Text = batchhttp.JoinSegments(result.Segments)
	}
	return result, nil
}
