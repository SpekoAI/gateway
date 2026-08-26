package deepgram

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
	// BatchAdapterID identifies the pre-recorded /v1/listen implementation.
	BatchAdapterID = "deepgram.stt.batch.v1"
	// BatchEndpoint is Deepgram's documented pre-recorded audio endpoint. The
	// same path serves streaming over wss; over https it takes the whole file
	// in the request body and answers with the whole transcript.
	BatchEndpoint = "https://api.deepgram.com/v1/listen"
	// BatchMaxAudioBytes is the documented per-request ceiling ("2 GB").
	BatchMaxAudioBytes int64 = 2 << 30
	// BatchMaxDurationSeconds is not a documented audio cap — Deepgram bounds
	// PROCESSING at 10 minutes for Nova/Base/Enhanced and answers 504 past it.
	// Two hours of audio processes in a small fraction of that; the value here
	// keeps one request comfortably inside the timeout with room for a slow
	// day at the vendor.
	BatchMaxDurationSeconds int64 = 2 * 60 * 60

	batchExtensionID = "deepgram.com/v1/listen"
)

// BatchConfig controls local transport limits for the pre-recorded adapter.
type BatchConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	MaxResponseBytes      int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// BatchAdapter implements runtime.BatchTranscriber over POST /v1/listen.
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
		return nil, errors.New("deepgram batch maximum response bytes must be positive")
	}
	policy, err := upstream.NewHTTPPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &BatchAdapter{id: config.AdapterID, httpClient: config.HTTPClient, maxResponseBytes: config.MaxResponseBytes, endpointPolicy: policy}, nil
}

func (a *BatchAdapter) ID() string { return a.id }

// Transcribe POSTs the WAV container as the request body and maps the
// synchronous response. Utterances are preferred as segments when the vendor
// returns them; otherwise words are grouped locally.
func (a *BatchAdapter) Transcribe(ctx context.Context, request runtimepkg.BatchTranscribeRequest) (*runtimepkg.BatchTranscription, error) {
	if request.Plan.Route.Provider != "deepgram" {
		return nil, fmt.Errorf("deepgram batch adapter cannot serve provider %q", request.Plan.Route.Provider)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model == "" || isFluxModel(model) {
		return nil, fmt.Errorf("deepgram batch adapter requires a pre-recorded model, got %q", model)
	}
	if request.AudioBytes > BatchMaxAudioBytes {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInputTooLarge, Message: "the upload exceeds Deepgram's 2 GB pre-recorded limit"}
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
	query.Set("punctuate", "true")
	query.Set("smart_format", "true")
	query.Set("utterances", "true")
	if language := strings.TrimSpace(request.Options.Language); language != "" {
		query.Set("language", language)
	} else {
		query.Set("detect_language", "true")
	}
	if request.Options.STT.Diarize() {
		query.Set("diarize", "true")
	}
	if keywords := request.Options.STT.GetKeywords(); len(keywords) > 0 {
		name := "keywords"
		if strings.HasPrefix(model, "nova-3") {
			name = "keyterm"
		}
		for _, keyword := range keywords {
			query.Add(name, keyword)
		}
	}
	if request.Plan.Execution.ProviderRoute == protocol.RouteSpekoRelay || request.Plan.Execution.CredentialSource == protocol.CredentialsManaged {
		if reservation := strings.TrimSpace(request.Plan.Reservation.ID); reservation != "" {
			query.Set("extra", "speko_reservation:"+reservation)
		}
	}
	for _, key := range request.Options.STT.ProviderKeys("deepgram") {
		query.Set(key, protocol.SttOptionString(request.Options.STT.Provider("deepgram")[key]))
	}
	endpoint.RawQuery = query.Encode()

	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), request.Audio)
	if err != nil {
		return nil, err
	}
	httpRequest.ContentLength = request.AudioBytes
	httpRequest.Header.Set("Content-Type", "audio/wav")
	httpRequest.Header.Set("Accept", "application/json")
	// Token for permanent keys (BYOK and the relay connector's own key);
	// Bearer only for the short-lived tokens managed provider-direct plans
	// carry — the same rule as the realtime adapter.
	scheme := "Bearer"
	if request.Plan.Execution.CredentialSource == protocol.CredentialsBYOK || request.Plan.Execution.ProviderRoute == protocol.RouteSpekoRelay {
		scheme = "Token"
	}
	httpRequest.Header.Set("Authorization", scheme+" "+credential)

	response, err := batchhttp.Do(a.httpClient, httpRequest, a.maxResponseBytes)
	if err != nil {
		return nil, err
	}
	if response.Status < 200 || response.Status >= 300 {
		return nil, batchhttp.StatusError(batchExtensionID, response.Status, response.Body)
	}
	return parseBatchResponse(response.Body)
}

type batchResponse struct {
	Metadata struct {
		RequestID string  `json:"request_id"`
		Duration  float64 `json:"duration"`
	} `json:"metadata"`
	Results struct {
		Channels []struct {
			DetectedLanguage string `json:"detected_language"`
			Alternatives     []struct {
				Transcript string `json:"transcript"`
				Words      []struct {
					Word           string  `json:"word"`
					PunctuatedWord string  `json:"punctuated_word"`
					Start          float64 `json:"start"`
					End            float64 `json:"end"`
					Speaker        *int    `json:"speaker"`
				} `json:"words"`
			} `json:"alternatives"`
		} `json:"channels"`
		Utterances []struct {
			Start      float64 `json:"start"`
			End        float64 `json:"end"`
			Speaker    *int    `json:"speaker"`
			Transcript string  `json:"transcript"`
		} `json:"utterances"`
	} `json:"results"`
}

func parseBatchResponse(body []byte) (*runtimepkg.BatchTranscription, error) {
	var payload batchResponse
	if err := batchhttp.DecodeJSON(body, &payload); err != nil {
		return nil, err
	}
	if len(payload.Results.Channels) == 0 || len(payload.Results.Channels[0].Alternatives) == 0 {
		return nil, batchhttp.Malformed(errors.New("deepgram response carries no channel alternative"))
	}
	alternative := payload.Results.Channels[0].Alternatives[0]
	result := &runtimepkg.BatchTranscription{
		Text:              strings.TrimSpace(alternative.Transcript),
		Language:          payload.Results.Channels[0].DetectedLanguage,
		DurationMS:        batchhttp.SecondsToMS(payload.Metadata.Duration),
		ProviderRequestID: payload.Metadata.RequestID,
		Extensions:        batchhttp.RawExtension(batchExtensionID, body),
	}
	if len(payload.Results.Utterances) > 0 {
		for _, utterance := range payload.Results.Utterances {
			text := strings.TrimSpace(utterance.Transcript)
			if text == "" {
				continue
			}
			result.Segments = append(result.Segments, runtimepkg.BatchSegment{
				Text: text, StartMS: batchhttp.SecondsToMS(utterance.Start), EndMS: batchhttp.SecondsToMS(utterance.End), Speaker: speakerLabel(utterance.Speaker),
			})
		}
		return result, nil
	}
	words := make([]batchhttp.Word, 0, len(alternative.Words))
	for _, word := range alternative.Words {
		text := word.PunctuatedWord
		if text == "" {
			text = word.Word
		}
		words = append(words, batchhttp.Word{Text: text, StartMS: batchhttp.SecondsToMS(word.Start), EndMS: batchhttp.SecondsToMS(word.End), Speaker: speakerLabel(word.Speaker)})
	}
	result.Segments = batchhttp.GroupWords(words, 0)
	return result, nil
}

func speakerLabel(speaker *int) string {
	if speaker == nil {
		return ""
	}
	return fmt.Sprintf("%d", *speaker)
}
