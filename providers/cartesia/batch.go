package cartesia

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
	// BatchAdapterID identifies the Cartesia batch /stt implementation.
	BatchAdapterID = "cartesia.stt.batch.v1"
	// BatchEndpoint is Cartesia's synchronous file transcription endpoint.
	BatchEndpoint = "https://api.cartesia.ai/stt"
	// BatchModel is the only model the batch endpoint serves ("ink-whisper
	// only"; ink-2 batch is "not available yet").
	BatchModel = "ink-whisper"
	// BatchMaxDurationSeconds is a local bound: Cartesia documents no file
	// limit and chunks server-side, but the call is synchronous, so one request
	// is held to two hours to keep the connection inside any sane proxy or
	// client timeout.
	BatchMaxDurationSeconds int64 = 2 * 60 * 60
	// BatchMaxAudioBytes bounds the multipart upload for the same reason.
	BatchMaxAudioBytes int64 = 1 << 30

	batchExtensionID = "cartesia.ai/stt"

	// Cartesia meters every endpoint in credits, and its credit table prices
	// POST /stt at "1 credit per 2 seconds of audio" — half the 1 credit per
	// second the ink-whisper WebSocket costs. Credits, not seconds, are the
	// quantity Cartesia bills, so that is what this adapter observes; the
	// credit-to-USD rate stays in the frozen price schedule.
	//
	// The denominator keeps the conversion exact: two seconds of audio is one
	// whole credit, so an odd number of milliseconds is a half-thousandth of a
	// credit and would be lost at the default denominator of 1000. Protocol
	// validation caps a denominator at 1e9, which is also the finest credit
	// fraction this can express.
	batchCreditDenominator int64 = 1_000_000_000
	batchSecondsPerCredit  int64 = 2
)

// BatchConfig controls local transport limits for the batch adapter.
type BatchConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	Version               string
	MaxResponseBytes      int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// BatchAdapter implements runtime.BatchTranscriber over POST /stt.
type BatchAdapter struct {
	id               string
	httpClient       *http.Client
	version          string
	maxResponseBytes int64
	endpointPolicy   upstream.HTTPPolicy
}

// NewBatch creates the batch adapter.
func NewBatch(config BatchConfig) (*BatchAdapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = BatchAdapterID
	}
	if config.Version == "" {
		config.Version = defaultSTTVersion
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = batchhttp.DefaultMaxResponseBytes
	}
	if config.MaxResponseBytes < 1 {
		return nil, errors.New("cartesia batch maximum response bytes must be positive")
	}
	policy, err := upstream.NewHTTPPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &BatchAdapter{id: config.AdapterID, httpClient: config.HTTPClient, version: config.Version, maxResponseBytes: config.MaxResponseBytes, endpointPolicy: policy}, nil
}

func (a *BatchAdapter) ID() string { return a.id }

// Transcribe POSTs the WAV as the multipart `file`. Cartesia's batch endpoint
// does not diarize, so a diarization ask is refused rather than answered with
// unlabeled segments.
func (a *BatchAdapter) Transcribe(ctx context.Context, request runtimepkg.BatchTranscribeRequest) (*runtimepkg.BatchTranscription, error) {
	if request.Plan.Route.Provider != "cartesia" {
		return nil, fmt.Errorf("cartesia batch adapter cannot serve provider %q", request.Plan.Route.Provider)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model != BatchModel {
		return nil, fmt.Errorf("cartesia batch endpoint serves %s only, got %q", BatchModel, model)
	}
	if request.AudioBytes > BatchMaxAudioBytes {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInputTooLarge, Message: "the upload exceeds the Cartesia batch size bound"}
	}
	if request.Options.STT.Diarize() {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInvalidRequest, Message: "Cartesia batch transcription does not label speakers", Hint: "Drop the diarization option or choose a provider that diarizes."}
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
		{Name: "model", Value: model},
		{Name: "timestamp_granularities[]", Value: "word"},
	}
	if language := strings.TrimSpace(request.Options.Language); language != "" {
		fields = append(fields, batchhttp.MultipartField{Name: "language", Value: primaryLanguageTag(language)})
	}
	for _, key := range request.Options.STT.ProviderKeys("cartesia") {
		fields = append(fields, batchhttp.MultipartField{Name: key, Value: protocol.SttOptionString(request.Options.STT.Provider("cartesia")[key])})
	}
	body, contentType := batchhttp.Multipart(fields, "file", "audio.wav", "audio/wav", request.Audio)
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), body)
	if err != nil {
		body.Close()
		return nil, err
	}
	httpRequest.Header.Set("Content-Type", contentType)
	httpRequest.Header.Set("Accept", "application/json")
	httpRequest.Header.Set("Cartesia-Version", a.version)
	// Permanent Cartesia keys travel in X-API-Key, as on the realtime socket.
	httpRequest.Header.Set("X-API-Key", credential)

	response, err := batchhttp.Do(a.httpClient, httpRequest, a.maxResponseBytes)
	if err != nil {
		return nil, err
	}
	if response.Status < 200 || response.Status >= 300 {
		return nil, batchhttp.StatusError(batchExtensionID, response.Status, response.Body)
	}
	var payload struct {
		RequestID string  `json:"request_id"`
		Text      string  `json:"text"`
		Language  string  `json:"language"`
		Duration  float64 `json:"duration"`
		Words     []struct {
			Word  string  `json:"word"`
			Start float64 `json:"start"`
			End   float64 `json:"end"`
		} `json:"words"`
	}
	if err := batchhttp.DecodeJSON(response.Body, &payload); err != nil {
		return nil, err
	}
	words := make([]batchhttp.Word, 0, len(payload.Words))
	for _, word := range payload.Words {
		words = append(words, batchhttp.Word{Text: word.Word, StartMS: batchhttp.SecondsToMS(word.Start), EndMS: batchhttp.SecondsToMS(word.End)})
	}
	result := &runtimepkg.BatchTranscription{
		Text:              strings.TrimSpace(payload.Text),
		Segments:          batchhttp.GroupWords(words, 0),
		Language:          payload.Language,
		DurationMS:        batchhttp.SecondsToMS(payload.Duration),
		ProviderRequestID: payload.RequestID,
		Extensions:        batchhttp.RawExtension(batchExtensionID, response.Body),
	}
	if result.ProviderRequestID == "" {
		result.ProviderRequestID = response.Header.Get("X-Request-ID")
	}
	result.Billing = metering.Report(batchCredits(model, response.Body, result.ProviderRequestID))
	return result, nil
}

// batchCredits converts the response's own `duration` — documented as "The
// duration of the input audio in seconds", which is the quantity Cartesia
// charges against, silence included — into billable credits. A response
// carrying no usable duration stays incomplete rather than billing zero: the
// request consumed credits whether or not this adapter could read how many.
func batchCredits(model string, body []byte, requestID string) *protocol.BillingObservation {
	o := &protocol.BillingObservation{
		OperationID: "request", Model: model, Mode: "batch",
		ProviderRequestID: requestID, Quantities: map[string]int64{},
	}
	if n, ok := metering.Quantity(body, batchCreditDenominator/batchSecondsPerCredit, "duration"); ok {
		o.Quantities["credits"] = n
		o.QuantityDenominators = map[string]int64{"credits": batchCreditDenominator}
		o.Complete = true
	}
	return o
}

func primaryLanguageTag(tag string) string {
	tag = strings.ToLower(strings.ReplaceAll(strings.TrimSpace(tag), "_", "-"))
	if i := strings.IndexByte(tag, '-'); i > 0 {
		return tag[:i]
	}
	return tag
}
