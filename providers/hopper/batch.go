package hopper

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/SpekoAI/gateway/internal/batchhttp"
	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/metering"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// BatchAdapterID identifies the prerecorded upload implementation.
	BatchAdapterID = "hopper.stt.batch.v1"
	// BatchEndpoint is the prerecorded twin of the listen socket. It serves
	// the same model.
	BatchEndpoint = "https://api.withhopper.com/v1/audio/transcriptions"
	// BatchMaxAudioBytes holds the file under Hopper's 25 MB transcription
	// body limit, read as decimal megabytes, with room left for the
	// multipart envelope around it. A larger upload would be refused with
	// 413 body_too_large, so it is refused before any of it is sent.
	BatchMaxAudioBytes int64 = 25_000_000 - 16<<10

	batchPath = "/v1/audio/transcriptions"
	// costHeader is the request's cost in USD, e.g. 0.000084 for 6.08 s at
	// $0.05 an hour.
	costHeader       = "X-Hopper-Request-Cost-Usd"
	batchExtensionID = "api.withhopper.com/v1/audio/transcriptions"
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
// /v1/audio/transcriptions.
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
		return nil, errors.New("hopper batch maximum response bytes must be positive")
	}
	policy, err := upstream.NewHTTPPolicy(officialHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &BatchAdapter{id: config.AdapterID, httpClient: config.HTTPClient, maxResponseBytes: config.MaxResponseBytes, endpointPolicy: policy}, nil
}

func (a *BatchAdapter) ID() string { return a.id }

// Transcribe uploads the WAV container as the multipart `file` part beside
// `model` and `sample_rate`. nemotron-asr reads English only and takes no
// language field.
func (a *BatchAdapter) Transcribe(ctx context.Context, request runtimepkg.BatchTranscribeRequest) (*runtimepkg.BatchTranscription, error) {
	if request.Plan.Route.Provider != ProviderName {
		return nil, fmt.Errorf("hopper batch adapter cannot serve provider %q", request.Plan.Route.Provider)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model != DefaultSTTModel {
		return nil, fmt.Errorf("hopper batch does not support model %q", model)
	}
	language, err := sttLanguage(request.Options.Language)
	if err != nil {
		return nil, err
	}
	if _, ok := sampleRates[request.Media.SampleRateHz]; !ok || request.Media.Encoding != "pcm_s16le" || request.Media.Channels != 1 {
		return nil, &runtimepkg.ProviderError{
			Code:    batchhttp.CodeUnsupportedMedia,
			Message: fmt.Sprintf("Hopper transcribes mono pcm_s16le at 8000, 16000, 22050, 24000, 44100 or 48000 Hz, got %s/%d Hz/%d channels", request.Media.Encoding, request.Media.SampleRateHz, request.Media.Channels),
			Hint:    "Resample the audio to mono pcm_s16le at 16000 Hz.",
		}
	}
	// Refuse before reading the file rather than after streaming a body the
	// service would refuse.
	if request.AudioBytes > BatchMaxAudioBytes {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInputTooLarge, Message: "the upload exceeds Hopper's 25 MB transcription limit", Hint: "Split the audio into files under 25 MB (about 13 minutes at 16 kHz)."}
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
		return nil, fmt.Errorf("hopper batch endpoint path must be %s, got %q", batchPath, endpoint.Path)
	}
	if err := batchhttp.Rewind(request.Audio); err != nil {
		return nil, err
	}
	fields := []batchhttp.MultipartField{{Name: "model", Value: model}, {Name: "sample_rate", Value: strconv.Itoa(request.Media.SampleRateHz)}}
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
		// The error code tells a spent balance (402 insufficient_quota) apart
		// from a malformed request, which the status alone does not.
		return nil, providerError(fmt.Sprintf("Hopper rejected the transcription with status %d", response.Status), parseError(response.Body), response.Status, response.Body)
	}
	return parseBatchResponse(response.Body, response.Header, model, language, request.Options.STT.WantsWordTimestamps())
}

// batchResponse is the JSON answer of the transcriptions endpoint.
type batchResponse struct {
	Text            *string `json:"text"`
	Words           []word  `json:"words"`
	DurationSeconds float64 `json:"duration_seconds"`
}

func parseBatchResponse(body []byte, header http.Header, model, language string, wantWords bool) (*runtimepkg.BatchTranscription, error) {
	var payload batchResponse
	if err := batchhttp.DecodeJSON(body, &payload); err != nil {
		return nil, err
	}
	// Silence still answers with an empty text; a 200 without the field is a
	// response shape this adapter does not understand.
	if payload.Text == nil {
		return nil, batchhttp.Malformed(errors.New("hopper transcription carries no text"))
	}
	words := make([]batchhttp.Word, 0, len(payload.Words))
	for _, candidate := range payload.Words {
		if text, start, end, ok := candidate.timed(); ok {
			words = append(words, batchhttp.Word{Text: text, StartMS: batchhttp.SecondsToMS(start), EndMS: batchhttp.SecondsToMS(end)})
		}
	}
	// Hopper times only the first turn's words: on 2026-10-13 a two-turn clip
	// came back with 5 timed words against a 20-word text. Segments and words
	// built from that would silently drop the rest, so a word list that does
	// not cover the text is withheld and the text stands alone.
	if len(words) != len(strings.Fields(*payload.Text)) {
		words = nil
	}
	// duration_seconds is the audio Hopper billed. A response without it
	// stays incomplete rather than billing zero.
	observation := metering.Duration("request", model, "batch", body, 1000, "duration_seconds")
	observation.Language = language
	// The request's exact USD cost rides a response header, in nano-dollars
	// so a six-decimal figure stays exact. A missing or unreadable header
	// leaves the duration as the only evidence.
	if cost, ok := metering.Quantity([]byte(strings.TrimSpace(header.Get(costHeader))), 1_000_000_000); ok {
		observation.Quantities["provider_cost_usd"] = cost
		observation.QuantityDenominators = map[string]int64{"provider_cost_usd": 1_000_000_000}
	}
	result := &runtimepkg.BatchTranscription{
		Billing:    metering.Report(observation),
		Text:       strings.TrimSpace(*payload.Text),
		Segments:   batchhttp.GroupWords(words, 0),
		Language:   language,
		DurationMS: batchhttp.SecondsToMS(payload.DurationSeconds),
		Extensions: batchhttp.RawExtension(batchExtensionID, body),
	}
	if result.Text == "" {
		result.Text = batchhttp.JoinSegments(result.Segments)
	}
	if wantWords {
		for _, timed := range words {
			result.Words = append(result.Words, runtimepkg.BatchWord{Text: timed.Text, StartMS: timed.StartMS, EndMS: timed.EndMS})
		}
	}
	return result, nil
}
