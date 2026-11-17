package fonada

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
	// ProviderName is the relay provider id.
	ProviderName = "fonada"
	// BatchAdapterID identifies the Kapture v3 file-transcription implementation.
	BatchAdapterID = "fonada.stt.batch.v1"
	// BatchEndpoint is the Kapture v3 transcription endpoint. The path names
	// the model; the form has no model field.
	BatchEndpoint = "https://api.fonada.ai/v3/asr/transcribe"
	// BatchModel is the only model the endpoint serves.
	BatchModel = "kapture-v3"
	// BatchMaxAudioBytes holds the file under the documented 25 MB limit, read
	// as decimal megabytes, with room for the multipart envelope. A larger
	// upload is refused before any of it is sent.
	BatchMaxAudioBytes int64 = 25_000_000 - 16<<10

	officialAPIHost  = "api.fonada.ai"
	batchPath        = "/v3/asr/transcribe"
	batchExtensionID = "api.fonada.ai/v3/asr/transcribe"
)

// languages are the documented language_id values (ten, read 2026-11-17;
// an earlier revision of the page also listed or). FonadaLabs refuses any
// other code with 400 unsupported_language before it charges.
var languages = map[string]struct{}{
	"en": {}, "bn": {}, "gu": {}, "hi": {}, "kn": {},
	"ml": {}, "mr": {}, "pa": {}, "ta": {}, "te": {},
}

// BatchConfig controls local transport limits for the transcription adapter.
type BatchConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	MaxResponseBytes      int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// BatchAdapter implements runtime.BatchTranscriber over POST
// /v3/asr/transcribe: one multipart request, one JSON response.
type BatchAdapter struct {
	id               string
	httpClient       *http.Client
	maxResponseBytes int64
	endpointPolicy   upstream.HTTPPolicy
}

// NewBatch creates the Kapture v3 transcription adapter.
func NewBatch(config BatchConfig) (*BatchAdapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = BatchAdapterID
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = batchhttp.DefaultMaxResponseBytes
	}
	if config.MaxResponseBytes < 1 {
		return nil, errors.New("fonada batch maximum response bytes must be positive")
	}
	policy, err := upstream.NewHTTPPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &BatchAdapter{id: config.AdapterID, httpClient: config.HTTPClient, maxResponseBytes: config.MaxResponseBytes, endpointPolicy: policy}, nil
}

func (a *BatchAdapter) ID() string { return a.id }

// Transcribe uploads the WAV container as the multipart `file` part beside
// `language_id`.
func (a *BatchAdapter) Transcribe(ctx context.Context, request runtimepkg.BatchTranscribeRequest) (*runtimepkg.BatchTranscription, error) {
	if request.Plan.Route.Provider != ProviderName {
		return nil, fmt.Errorf("fonada batch adapter cannot serve provider %q", request.Plan.Route.Provider)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model != BatchModel {
		return nil, fmt.Errorf("fonada batch adapter requires %s, got %q", BatchModel, model)
	}
	language, err := sttLanguage(request.Options.Language)
	if err != nil {
		return nil, err
	}
	if request.Media.Encoding != "pcm_s16le" || request.Media.SampleRateHz < 8000 || request.Media.SampleRateHz > 48000 || request.Media.Channels < 1 || request.Media.Channels > 2 {
		return nil, &runtimepkg.ProviderError{
			Code:    batchhttp.CodeUnsupportedMedia,
			Message: fmt.Sprintf("FonadaLabs transcribes mono or stereo 16-bit PCM WAV at 8000 to 48000 Hz, got %s/%d Hz/%d channels", request.Media.Encoding, request.Media.SampleRateHz, request.Media.Channels),
			Hint:    "Resample the audio to mono pcm_s16le at 16000 Hz.",
		}
	}
	if request.AudioBytes > BatchMaxAudioBytes {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInputTooLarge, Message: "the upload exceeds FonadaLabs' 25 MB transcription limit", Hint: "Split the audio into files under 25 MB (about 13 minutes at 16 kHz mono)."}
	}
	if err := checkOptions(request.Options); err != nil {
		return nil, err
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
		return nil, fmt.Errorf("fonada batch endpoint path must be %s, got %q", batchPath, endpoint.Path)
	}
	if err := batchhttp.Rewind(request.Audio); err != nil {
		return nil, err
	}
	fields := []batchhttp.MultipartField{{Name: "language_id", Value: language}}
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
	return parseBatchResponse(response.Body, model, language)
}

// batchResponse is the documented success body. chunk_texts, bucket,
// storage_prefix, processing_time_ms and credits_used stay in the raw
// extension only.
type batchResponse struct {
	JobID          string   `json:"job_id"`
	Text           *string  `json:"text"`
	Language       string   `json:"language"`
	AudioDurationS *float64 `json:"audio_duration_s"`
	Chunks         []struct {
		Text   string   `json:"text"`
		StartS *float64 `json:"start_s"`
		EndS   *float64 `json:"end_s"`
	} `json:"transcript_chunks"`
}

func parseBatchResponse(body []byte, model, requested string) (*runtimepkg.BatchTranscription, error) {
	var payload batchResponse
	if err := batchhttp.DecodeJSON(body, &payload); err != nil {
		return nil, err
	}
	// Without the processed duration the request cannot be billed or checked
	// for coverage, so a body that lacks it is a shape this adapter does not
	// understand.
	if payload.AudioDurationS == nil || *payload.AudioDurationS < 0 {
		return nil, batchhttp.Malformed(errors.New("fonada transcription carries no audio_duration_s"))
	}
	// The documented body always carries text, empty for silence. A body
	// with neither text nor chunks has no transcript to report.
	if payload.Text == nil && len(payload.Chunks) == 0 {
		return nil, batchhttp.Malformed(errors.New("fonada transcription carries neither text nor transcript_chunks"))
	}
	var text string
	if payload.Text != nil {
		text = strings.Join(strings.Fields(*payload.Text), " ")
	}
	segments := make([]runtimepkg.BatchSegment, 0, len(payload.Chunks))
	for _, chunk := range payload.Chunks {
		chunkText := strings.Join(strings.Fields(chunk.Text), " ")
		if chunkText == "" {
			continue
		}
		if chunk.StartS == nil || chunk.EndS == nil || *chunk.StartS < 0 || *chunk.EndS < *chunk.StartS {
			return nil, batchhttp.Malformed(errors.New("fonada transcript chunk carries no valid start_s/end_s"))
		}
		segments = append(segments, runtimepkg.BatchSegment{Text: chunkText, StartMS: batchhttp.SecondsToMS(*chunk.StartS), EndMS: batchhttp.SecondsToMS(*chunk.EndS)})
	}
	if text != "" && len(segments) == 0 {
		// The chunks are the only evidence that the text covers the upload.
		return nil, batchhttp.Malformed(errors.New("fonada transcript carries no timed chunks"))
	}
	if text == "" {
		// The chunk texts are the transcript too; an empty top-level field
		// beside timed speech is not silence.
		text = batchhttp.JoinSegments(segments)
	}
	// Report the language as a documented code, the form the request sent.
	// An echo in any other form falls back to the requested code.
	language := requested
	if echoed, err := sttLanguage(payload.Language); err == nil {
		language = echoed
	}
	observation := metering.Duration("request", model, "batch", body, 1000, "audio_duration_s")
	observation.ProviderRequestID = payload.JobID
	return &runtimepkg.BatchTranscription{
		Billing:           metering.Report(observation),
		Text:              text,
		Segments:          segments,
		Language:          language,
		DurationMS:        batchhttp.SecondsToMS(*payload.AudioDurationS),
		ProviderRequestID: payload.JobID,
		Extensions:        batchhttp.RawExtension(batchExtensionID, body),
	}, nil
}

// sttLanguage maps a BCP-47 tag to a documented language_id. FonadaLabs has
// no language detection: the field is required, so a request that names no
// language is refused rather than guessed.
func sttLanguage(language string) (string, error) {
	lowered := strings.ToLower(strings.TrimSpace(language))
	base := lowered
	if index := strings.IndexAny(lowered, "-_"); index > 0 {
		base = lowered[:index]
	}
	if _, ok := languages[base]; ok {
		return base, nil
	}
	message := "FonadaLabs Kapture v3 needs a language: en, bn, gu, hi, kn, ml, mr, pa, ta or te"
	if lowered != "" && lowered != "auto" {
		message = fmt.Sprintf("FonadaLabs Kapture v3 does not transcribe %q", strings.TrimSpace(language))
	}
	return "", &runtimepkg.ProviderError{
		Code:    "unsupported_language",
		Message: message,
		Hint:    "Send one of en, bn, gu, hi, kn, ml, mr, pa, ta or te, or route the audio to another provider.",
	}
}

// checkOptions refuses asks Kapture v3 cannot honour, so a caller never gets a
// successful transcript without the behaviour it requested. The endpoint
// takes only the file and the language.
func checkOptions(options protocol.RequestOptions) error {
	stt := options.STT
	switch {
	case stt.Diarize():
		return &runtimepkg.ProviderError{Code: batchhttp.CodeInvalidRequest, Message: "FonadaLabs Kapture v3 does not label speakers", Hint: "Drop the diarization option or choose a provider that diarizes."}
	case stt.WantsWordTimestamps():
		return &runtimepkg.ProviderError{Code: batchhttp.CodeInvalidRequest, Message: "FonadaLabs Kapture v3 returns no word timings", Hint: "Drop the word_timestamps option or choose a provider that times words."}
	case len(stt.GetKeywords()) > 0:
		return &runtimepkg.ProviderError{Code: batchhttp.CodeInvalidRequest, Message: "FonadaLabs Kapture v3 takes no keyword list", Hint: "Drop the keywords option or choose a provider that supports vocabulary biasing."}
	case stt.ReduceNoise():
		return &runtimepkg.ProviderError{Code: batchhttp.CodeInvalidRequest, Message: "FonadaLabs Kapture v3 has no noise-reduction option", Hint: "Drop the noise-reduction option."}
	case stt.TranslationTarget() != "":
		return &runtimepkg.ProviderError{Code: batchhttp.CodeInvalidRequest, Message: "FonadaLabs Kapture v3 does not translate", Hint: "Drop the translation option or choose a provider that translates."}
	case len(stt.ProviderKeys(ProviderName)) > 0:
		return &runtimepkg.ProviderError{Code: batchhttp.CodeInvalidRequest, Message: "FonadaLabs Kapture v3 takes no provider-specific settings"}
	}
	return nil
}
