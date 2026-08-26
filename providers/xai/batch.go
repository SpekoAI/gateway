package xai

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
	// BatchAdapterID identifies the xAI REST /v1/stt implementation.
	BatchAdapterID = "xai.stt.batch.v1"
	// BatchEndpoint is the REST twin of the wss://api.x.ai/v1/stt socket. xAI
	// exposes one Grok STT service over both transports and takes no model
	// parameter; the catalog's model id is a relay-side label.
	BatchEndpoint = "https://api.x.ai/v1/stt"
	// BatchMaxAudioBytes is the documented ceiling ("500 MB", HTTP 413 above).
	BatchMaxAudioBytes int64 = 500_000_000
	// BatchMaxDurationSeconds is a local bound for the synchronous call: no
	// duration cap is documented and the byte cap admits ~4.5 h of 16 kHz mono,
	// so two hours keeps the held connection reasonable.
	BatchMaxDurationSeconds int64 = 2 * 60 * 60

	batchExtensionID = "x.ai/v1/stt"
)

// BatchConfig controls local transport limits for the REST adapter.
type BatchConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	MaxResponseBytes      int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// BatchAdapter implements runtime.BatchTranscriber over POST /v1/stt.
type BatchAdapter struct {
	id               string
	httpClient       *http.Client
	maxResponseBytes int64
	endpointPolicy   upstream.HTTPPolicy
}

// NewBatch creates the REST adapter.
func NewBatch(config BatchConfig) (*BatchAdapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = BatchAdapterID
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = batchhttp.DefaultMaxResponseBytes
	}
	if config.MaxResponseBytes < 1 {
		return nil, errors.New("xai batch maximum response bytes must be positive")
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
	if request.Plan.Route.Provider != "xai" {
		return nil, fmt.Errorf("xai batch adapter cannot serve provider %q", request.Plan.Route.Provider)
	}
	if request.AudioBytes > BatchMaxAudioBytes {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInputTooLarge, Message: "the upload exceeds xAI's 500 MB limit"}
	}
	credential, err := batchhttp.Credential(request.Plan)
	if err != nil {
		return nil, err
	}
	endpoint, err := a.endpointPolicy.Parse(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}
	var fields []batchhttp.MultipartField
	if language := strings.TrimSpace(request.Options.Language); language != "" {
		fields = append(fields, batchhttp.MultipartField{Name: "language", Value: language})
	}
	if request.Options.STT.Diarize() {
		fields = append(fields, batchhttp.MultipartField{Name: "diarize", Value: "true"})
	}
	for _, keyword := range request.Options.STT.GetKeywords() {
		fields = append(fields, batchhttp.MultipartField{Name: "keyterm", Value: keyword})
	}
	for _, key := range request.Options.STT.ProviderKeys("xai") {
		fields = append(fields, batchhttp.MultipartField{Name: key, Value: protocol.SttOptionString(request.Options.STT.Provider("xai")[key])})
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
		Words    []struct {
			Text    string  `json:"text"`
			Start   float64 `json:"start"`
			End     float64 `json:"end"`
			Speaker *int    `json:"speaker"`
		} `json:"words"`
	}
	if err := batchhttp.DecodeJSON(response.Body, &payload); err != nil {
		return nil, err
	}
	words := make([]batchhttp.Word, 0, len(payload.Words))
	for _, word := range payload.Words {
		speaker := ""
		if word.Speaker != nil {
			speaker = fmt.Sprintf("%d", *word.Speaker)
		}
		words = append(words, batchhttp.Word{Text: word.Text, StartMS: batchhttp.SecondsToMS(word.Start), EndMS: batchhttp.SecondsToMS(word.End), Speaker: speaker})
	}
	return &runtimepkg.BatchTranscription{
		Text:              strings.TrimSpace(payload.Text),
		Segments:          batchhttp.GroupWords(words, 0),
		Language:          payload.Language,
		DurationMS:        batchhttp.SecondsToMS(payload.Duration),
		ProviderRequestID: response.Header.Get("x-request-id"),
		Extensions:        batchhttp.RawExtension(batchExtensionID, response.Body),
	}, nil
}
