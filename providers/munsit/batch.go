package munsit

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/SpekoAI/gateway/internal/batchhttp"
	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/metering"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// BatchAdapterID identifies the prerecorded upload implementation.
	BatchAdapterID = "munsit.stt.batch.v1"
	// BatchEndpoint is the prerecorded twin of the listen socket. It serves
	// the same two model ids.
	BatchEndpoint = "https://api.munsit.com/api/v1/audio/transcribe"
	// BatchMaxDurationSeconds is the documented cap: files must be under 60
	// minutes, so one request is held to just below it and longer audio is
	// the caller's to chunk.
	BatchMaxDurationSeconds int64 = 60*60 - 1
	// BatchMaxAudioBytes is that duration at the highest rate the relay
	// carries (48 kHz mono PCM16) plus the WAV header. Munsit documents no
	// byte cap; this one refuses an upload the duration cap would refuse
	// anyway before any of it is sent.
	BatchMaxAudioBytes int64 = BatchMaxDurationSeconds*48_000*2 + 44

	batchPath        = "/api/v1/audio/transcribe"
	batchExtensionID = "api.munsit.com/api/v1/audio/transcribe"
)

// BatchConfig controls local transport limits for the prerecorded adapter.
type BatchConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	MaxResponseBytes      int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// BatchAdapter implements runtime.BatchTranscriber over POST
// /api/v1/audio/transcribe.
type BatchAdapter struct {
	id               string
	httpClient       *http.Client
	maxResponseBytes int64
	endpointPolicy   upstream.HTTPPolicy
}

// NewBatch creates the prerecorded adapter.
func NewBatch(config BatchConfig) (*BatchAdapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = BatchAdapterID
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = batchhttp.DefaultMaxResponseBytes
	}
	if config.MaxResponseBytes < 1 {
		return nil, errors.New("munsit batch maximum response bytes must be positive")
	}
	policy, err := upstream.NewHTTPPolicy(officialHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &BatchAdapter{id: config.AdapterID, httpClient: config.HTTPClient, maxResponseBytes: config.MaxResponseBytes, endpointPolicy: policy}, nil
}

func (a *BatchAdapter) ID() string { return a.id }

// Transcribe uploads the WAV container as the multipart `file` part beside
// `model` and, for munsit-en-ar, `language`. munsit reads Arabic only and
// takes no language field.
func (a *BatchAdapter) Transcribe(ctx context.Context, request runtimepkg.BatchTranscribeRequest) (*runtimepkg.BatchTranscription, error) {
	if request.Plan.Route.Provider != ProviderName {
		return nil, fmt.Errorf("munsit batch adapter cannot serve provider %q", request.Plan.Route.Provider)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	language, err := sttLanguage(model, request.Options.Language)
	if err != nil {
		return nil, err
	}
	// Refuse before reading the file rather than after streaming a body the
	// service would refuse.
	if request.AudioBytes > BatchMaxAudioBytes || audioSeconds(request.Media, request.AudioBytes) > BatchMaxDurationSeconds {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInputTooLarge, Message: "the upload exceeds Munsit's 60-minute transcription limit"}
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
		return nil, fmt.Errorf("munsit batch endpoint path must be %s, got %q", batchPath, endpoint.Path)
	}
	if err := batchhttp.Rewind(request.Audio); err != nil {
		return nil, err
	}
	fields := []batchhttp.MultipartField{{Name: "model", Value: model}}
	if model == CodeSwitchSTTModel && language != "auto" {
		fields = append(fields, batchhttp.MultipartField{Name: "language", Value: language})
	}
	body, contentType := batchhttp.Multipart(fields, "file", "audio.wav", "audio/wav", request.Audio)
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), body)
	if err != nil {
		body.Close()
		return nil, err
	}
	httpRequest.Header.Set("Content-Type", contentType)
	httpRequest.Header.Set("Accept", "application/json")
	httpRequest.Header.Set(apiKeyHeader, credential)

	response, err := batchhttp.Do(a.httpClient, httpRequest, a.maxResponseBytes)
	if err != nil {
		return nil, err
	}
	if response.Status < 200 || response.Status >= 300 {
		return nil, batchhttp.StatusError(batchExtensionID, response.Status, response.Body)
	}
	return parseBatchResponse(response.Body, model, language, request.Options.STT.WantsWordTimestamps())
}

// audioSeconds is the duration the WAV declares through its byte count, or
// zero when the format is unknown.
func audioSeconds(media protocol.MediaFormat, audioBytes int64) int64 {
	if media.SampleRateHz <= 0 || media.Channels <= 0 || audioBytes <= 44 {
		return 0
	}
	return (audioBytes - 44) / int64(media.SampleRateHz*media.Channels*2)
}

// batchResponse is the JSON answer of the transcribe endpoint.
type batchResponse struct {
	Data *struct {
		TranscriptionID string  `json:"transcriptionId"`
		Transcription   string  `json:"transcription"`
		Duration        float64 `json:"duration"`
		Timestamps      []struct {
			Word  string  `json:"word"`
			Start float64 `json:"start"`
			End   float64 `json:"end"`
		} `json:"timestamps"`
	} `json:"data"`
}

func parseBatchResponse(body []byte, model, language string, wantWords bool) (*runtimepkg.BatchTranscription, error) {
	var payload batchResponse
	if err := batchhttp.DecodeJSON(body, &payload); err != nil {
		return nil, err
	}
	// A completed transcription always carries its id; a 200 without one is
	// a response shape this adapter does not understand, not silence.
	if payload.Data == nil || payload.Data.TranscriptionID == "" {
		return nil, batchhttp.Malformed(errors.New("munsit transcription carries no transcriptionId"))
	}
	data := payload.Data
	words := make([]batchhttp.Word, 0, len(data.Timestamps))
	for _, word := range data.Timestamps {
		words = append(words, batchhttp.Word{Text: word.Word, StartMS: batchhttp.SecondsToMS(word.Start), EndMS: batchhttp.SecondsToMS(word.End)})
	}
	// creditsConsumed is what the upload cost. A response without it stays
	// incomplete rather than billing zero.
	observation := &protocol.BillingObservation{OperationID: "request", Model: model, Mode: "batch", ProviderRequestID: data.TranscriptionID, Quantities: map[string]int64{}}
	if credits, ok := metering.Quantity(body, 1000, "data", "stats", "creditsConsumed"); ok {
		observation.Quantities["credits"] = credits
		observation.Complete = true
	}
	result := &runtimepkg.BatchTranscription{
		Billing: metering.Report(observation),
		// munsit-en-ar answers with a leading space.
		Text:              strings.TrimSpace(data.Transcription),
		Segments:          batchhttp.GroupWords(words, 0),
		DurationMS:        batchhttp.SecondsToMS(data.Duration),
		ProviderRequestID: data.TranscriptionID,
		Extensions:        batchhttp.RawExtension(batchExtensionID, body),
	}
	if language != "auto" {
		result.Language = language
	}
	if result.Text == "" {
		result.Text = batchhttp.JoinSegments(result.Segments)
	}
	if wantWords {
		for _, word := range words {
			if text := strings.TrimSpace(word.Text); text != "" {
				result.Words = append(result.Words, runtimepkg.BatchWord{Text: text, StartMS: word.StartMS, EndMS: word.EndMS})
			}
		}
	}
	return result, nil
}
