package elevenlabs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/SpekoAI/gateway/internal/batchhttp"
	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// BatchAdapterID identifies the Scribe file-transcription implementation.
	BatchAdapterID = "elevenlabs.stt.batch.v1"
	// BatchEndpoint is Scribe's synchronous file endpoint.
	BatchEndpoint = "https://api.elevenlabs.io/v1/speech-to-text"
	// BatchModel is the batch Scribe generation paired with scribe_v2_realtime.
	BatchModel = "scribe_v2"
	// BatchMaxDurationSeconds is the documented per-file ceiling ("up to 10
	// hours").
	BatchMaxDurationSeconds int64 = 10 * 60 * 60
	// BatchMaxAudioBytes is the tighter of the two documented size limits
	// (2 GB for URL input; the multipart `file` allows more). One bound keeps
	// the catalog honest for either input mode.
	BatchMaxAudioBytes int64 = 2 << 30

	batchExtensionID = "elevenlabs.io/v1/speech-to-text"
)

// BatchConfig controls local transport limits for the Scribe file adapter.
type BatchConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	MaxResponseBytes      int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// BatchAdapter implements runtime.BatchTranscriber over the synchronous
// Scribe endpoint: one multipart POST, one JSON response.
type BatchAdapter struct {
	id               string
	httpClient       *http.Client
	maxResponseBytes int64
	endpointPolicy   upstream.HTTPPolicy
}

// NewBatch creates the Scribe file adapter.
func NewBatch(config BatchConfig) (*BatchAdapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = BatchAdapterID
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = batchhttp.DefaultMaxResponseBytes
	}
	if config.MaxResponseBytes < 1 {
		return nil, errors.New("elevenlabs batch maximum response bytes must be positive")
	}
	policy, err := upstream.NewHTTPPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &BatchAdapter{id: config.AdapterID, httpClient: config.HTTPClient, maxResponseBytes: config.MaxResponseBytes, endpointPolicy: policy}, nil
}

func (a *BatchAdapter) ID() string { return a.id }

// Transcribe POSTs the WAV as the multipart `file` and maps the synchronous
// response. Scribe returns words with speaker ids and no coarser unit, so
// segments are grouped locally.
func (a *BatchAdapter) Transcribe(ctx context.Context, request runtimepkg.BatchTranscribeRequest) (*runtimepkg.BatchTranscription, error) {
	if request.Plan.Route.Provider != "elevenlabs" {
		return nil, fmt.Errorf("elevenlabs batch adapter cannot serve provider %q", request.Plan.Route.Provider)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model == "" || strings.Contains(model, "realtime") {
		return nil, fmt.Errorf("elevenlabs batch adapter requires a file Scribe model, got %q", model)
	}
	if request.AudioBytes > BatchMaxAudioBytes {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInputTooLarge, Message: "the upload exceeds Scribe's 2 GB file limit"}
	}
	credential, err := batchhttp.Credential(request.Plan)
	if err != nil {
		return nil, err
	}
	endpoint, err := a.endpointPolicy.Parse(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}
	fields := []batchhttp.MultipartField{
		{Name: "model_id", Value: model},
		{Name: "diarize", Value: strconv.FormatBool(request.Options.STT.Diarize())},
		{Name: "timestamps_granularity", Value: "word"},
		{Name: "tag_audio_events", Value: "false"},
	}
	if language := strings.TrimSpace(request.Options.Language); language != "" {
		fields = append(fields, batchhttp.MultipartField{Name: "language_code", Value: baseLanguageTag(language)})
	}
	for _, keyword := range request.Options.STT.GetKeywords() {
		fields = append(fields, batchhttp.MultipartField{Name: "keyterms", Value: keyword})
	}
	for _, key := range request.Options.STT.ProviderKeys("elevenlabs") {
		fields = append(fields, batchhttp.MultipartField{Name: key, Value: protocol.SttOptionString(request.Options.STT.Provider("elevenlabs")[key])})
	}
	body, contentType := batchhttp.Multipart(fields, "file", "audio.wav", "audio/wav", request.Audio)
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), body)
	if err != nil {
		body.Close()
		return nil, err
	}
	httpRequest.Header.Set("Content-Type", contentType)
	httpRequest.Header.Set("Accept", "application/json")
	httpRequest.Header.Set("xi-api-key", credential)

	response, err := batchhttp.Do(a.httpClient, httpRequest, a.maxResponseBytes)
	if err != nil {
		return nil, err
	}
	if response.Status < 200 || response.Status >= 300 {
		return nil, batchhttp.StatusError(batchExtensionID, response.Status, response.Body)
	}
	var payload struct {
		LanguageCode      string  `json:"language_code"`
		Text              string  `json:"text"`
		TranscriptionID   string  `json:"transcription_id"`
		AudioDurationSecs float64 `json:"audio_duration_secs"`
		Words             []struct {
			Text      string  `json:"text"`
			Type      string  `json:"type"`
			Start     float64 `json:"start"`
			End       float64 `json:"end"`
			SpeakerID string  `json:"speaker_id"`
		} `json:"words"`
	}
	if err := batchhttp.DecodeJSON(response.Body, &payload); err != nil {
		return nil, err
	}
	words := make([]batchhttp.Word, 0, len(payload.Words))
	for _, word := range payload.Words {
		// Scribe interleaves `spacing` and `audio_event` entries with words;
		// only words carry transcript text.
		if word.Type != "" && word.Type != "word" {
			continue
		}
		words = append(words, batchhttp.Word{Text: word.Text, StartMS: batchhttp.SecondsToMS(word.Start), EndMS: batchhttp.SecondsToMS(word.End), Speaker: word.SpeakerID})
	}
	result := &runtimepkg.BatchTranscription{
		Text:              strings.TrimSpace(payload.Text),
		Segments:          batchhttp.GroupWords(words, 0),
		Language:          payload.LanguageCode,
		DurationMS:        batchhttp.SecondsToMS(payload.AudioDurationSecs),
		ProviderRequestID: payload.TranscriptionID,
		Extensions:        batchhttp.RawExtension(batchExtensionID, response.Body),
	}
	if result.ProviderRequestID == "" {
		result.ProviderRequestID = response.Header.Get("request-id")
	}
	return result, nil
}
