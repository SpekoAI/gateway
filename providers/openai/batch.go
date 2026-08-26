package openai

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
	// BatchAdapterID identifies the /v1/audio/transcriptions implementation.
	BatchAdapterID = "openai.stt.batch.v1"
	// BatchEndpoint is OpenAI's synchronous file transcription endpoint.
	BatchEndpoint = "https://api.openai.com/v1/audio/transcriptions"
	// BatchMaxAudioBytes is the documented upload ceiling ("25 MB"). It is the
	// binding constraint for this route: a 16 kHz mono WAV crosses it at about
	// thirteen minutes, so long audio arrives here already chunked.
	BatchMaxAudioBytes int64 = 25_000_000
	// BatchMaxDurationSeconds is not documented separately; the byte cap binds
	// first at every rate this gateway carries. Thirty minutes is a generous
	// upper bound for the lowest rate (8 kHz mono).
	BatchMaxDurationSeconds int64 = 30 * 60

	batchExtensionID = "openai.com/v1/audio/transcriptions"
)

// batchFileModels are the model ids /v1/audio/transcriptions accepts. The
// realtime-only ids (gpt-live-transcribe, gpt-realtime-whisper) are absent on
// purpose: their file counterparts are gpt-transcribe and whisper-1.
var batchFileModels = map[string]struct{}{
	"gpt-transcribe":                    {},
	"gpt-4o-transcribe":                 {},
	"gpt-4o-mini-transcribe":            {},
	"gpt-4o-mini-transcribe-2025-12-15": {},
	"gpt-4o-transcribe-diarize":         {},
	"whisper-1":                         {},
}

// BatchConfig controls local transport limits for the file adapter.
type BatchConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	MaxResponseBytes      int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// BatchAdapter implements runtime.BatchTranscriber over the synchronous file
// endpoint. Response shape is chosen per model: whisper-1 is the only model
// that returns segment timings (verbose_json); the diarize model returns
// speaker-labelled segments (diarized_json); every other model returns text
// and usage only.
type BatchAdapter struct {
	id               string
	httpClient       *http.Client
	maxResponseBytes int64
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
	if config.MaxResponseBytes < 1 {
		return nil, errors.New("openai batch maximum response bytes must be positive")
	}
	policy, err := upstream.NewHTTPPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &BatchAdapter{id: config.AdapterID, httpClient: config.HTTPClient, maxResponseBytes: config.MaxResponseBytes, endpointPolicy: policy}, nil
}

func (a *BatchAdapter) ID() string { return a.id }

// Transcribe POSTs the WAV as the multipart `file`.
func (a *BatchAdapter) Transcribe(ctx context.Context, request runtimepkg.BatchTranscribeRequest) (*runtimepkg.BatchTranscription, error) {
	if request.Plan.Route.Provider != "openai" {
		return nil, fmt.Errorf("openai batch adapter cannot serve provider %q", request.Plan.Route.Provider)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if _, ok := batchFileModels[model]; !ok {
		return nil, fmt.Errorf("openai batch adapter cannot serve model %q on /v1/audio/transcriptions", model)
	}
	if request.AudioBytes > BatchMaxAudioBytes {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInputTooLarge, Message: "the upload exceeds OpenAI's 25 MB file limit"}
	}
	diarize := request.Options.STT.Diarize()
	if diarize && model != "gpt-4o-transcribe-diarize" {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInvalidRequest, Message: "speaker diarization on OpenAI requires gpt-4o-transcribe-diarize", Hint: "Choose gpt-4o-transcribe-diarize or drop the diarization option."}
	}
	credential, err := batchhttp.Credential(request.Plan)
	if err != nil {
		return nil, err
	}
	endpoint, err := a.endpointPolicy.Parse(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}
	fields := []batchhttp.MultipartField{{Name: "model", Value: model}}
	responseFormat := "json"
	switch model {
	case "whisper-1":
		responseFormat = "verbose_json"
		fields = append(fields, batchhttp.MultipartField{Name: "timestamp_granularities[]", Value: "segment"})
	case "gpt-4o-transcribe-diarize":
		responseFormat = "diarized_json"
		fields = append(fields, batchhttp.MultipartField{Name: "chunking_strategy", Value: "auto"})
	}
	fields = append(fields, batchhttp.MultipartField{Name: "response_format", Value: responseFormat})
	if language := strings.TrimSpace(request.Options.Language); language != "" {
		fields = append(fields, batchhttp.MultipartField{Name: "language", Value: baseBatchLanguage(language)})
	}
	if prompt := sttPromptFor(request.Options.STT); prompt != "" {
		fields = append(fields, batchhttp.MultipartField{Name: "prompt", Value: prompt})
	}
	for _, key := range request.Options.STT.ProviderKeys("openai") {
		if key == "prompt" {
			continue
		}
		fields = append(fields, batchhttp.MultipartField{Name: key, Value: protocol.SttOptionString(request.Options.STT.Provider("openai")[key])})
	}
	body, contentType := batchhttp.Multipart(fields, "file", "audio.wav", "audio/wav", request.Audio)
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), body)
	if err != nil {
		body.Close()
		return nil, err
	}
	httpRequest.Header.Set("Content-Type", contentType)
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
		Text     string  `json:"text"`
		Language string  `json:"language"`
		Duration float64 `json:"duration"`
		Segments []struct {
			Text    string  `json:"text"`
			Start   float64 `json:"start"`
			End     float64 `json:"end"`
			Speaker string  `json:"speaker"`
		} `json:"segments"`
		Usage struct {
			Type    string  `json:"type"`
			Seconds float64 `json:"seconds"`
		} `json:"usage"`
	}
	if err := batchhttp.DecodeJSON(response.Body, &payload); err != nil {
		return nil, err
	}
	result := &runtimepkg.BatchTranscription{
		Text:              strings.TrimSpace(payload.Text),
		Language:          payload.Language,
		DurationMS:        batchhttp.SecondsToMS(payload.Duration),
		ProviderRequestID: response.Header.Get("x-request-id"),
		Extensions:        batchhttp.RawExtension(batchExtensionID, response.Body),
	}
	if result.DurationMS == 0 && payload.Usage.Type == "duration" {
		result.DurationMS = batchhttp.SecondsToMS(payload.Usage.Seconds)
	}
	for _, segment := range payload.Segments {
		text := strings.TrimSpace(segment.Text)
		if text == "" {
			continue
		}
		result.Segments = append(result.Segments, runtimepkg.BatchSegment{Text: text, StartMS: batchhttp.SecondsToMS(segment.Start), EndMS: batchhttp.SecondsToMS(segment.End), Speaker: segment.Speaker})
	}
	return result, nil
}

// baseBatchLanguage lowers a BCP-47 tag to the ISO-639-1 code the `language`
// form field documents.
func baseBatchLanguage(tag string) string {
	tag = strings.ToLower(strings.ReplaceAll(strings.TrimSpace(tag), "_", "-"))
	if i := strings.IndexByte(tag, '-'); i > 0 {
		return tag[:i]
	}
	return tag
}
