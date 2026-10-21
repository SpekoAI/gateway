package mistral

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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
	BatchAdapterID = "mistral.stt.batch.v1"
	// BatchEndpoint serves both offline Voxtral 3 models.
	BatchEndpoint = "https://api.mistral.ai/v1/audio/transcriptions"
	// BatchMiniModel is the cheaper offline model; BatchSmallModel the more
	// accurate one. Both diarize and take context_bias.
	BatchMiniModel  = "voxtral-mini-transcribe-3-0"
	BatchSmallModel = "voxtral-small-transcribe-3-0"
	// BatchMaxAudioBytes holds the file under the 500 MB upload limit
	// Mistral lists among its known limitations, with room left for the
	// multipart envelope around it.
	BatchMaxAudioBytes int64 = 500_000_000 - 16<<10
	// BatchMaxDurationSeconds is a local bound: Mistral documents no
	// duration limit for the 3.0 models, and both share a 65,536-token
	// context. Thirty minutes is what Voxtral Mini Transcribe has handled
	// per request since its first release; longer audio is chunked by
	// transcription jobs.
	BatchMaxDurationSeconds int64 = 30 * 60

	batchPath        = "/v1/audio/transcriptions"
	batchExtensionID = "api.mistral.ai/v1/audio/transcriptions"
	// correlationHeader is the request id on every response.
	correlationHeader = "Mistral-Correlation-Id"
	// wavHeaderSearchBytes bounds how far into the container the data chunk
	// is looked for.
	wavHeaderSearchBytes = 4 << 10
)

var batchModels = map[string]struct{}{BatchMiniModel: {}, BatchSmallModel: {}}

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
		return nil, errors.New("mistral batch maximum response bytes must be positive")
	}
	policy, err := upstream.NewHTTPPolicy(officialHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &BatchAdapter{id: config.AdapterID, httpClient: config.HTTPClient, maxResponseBytes: config.MaxResponseBytes, endpointPolicy: policy}, nil
}

func (a *BatchAdapter) ID() string { return a.id }

// Transcribe uploads the WAV container as the multipart `file` part beside
// `model`, the optional `language`, `diarize` when speakers were asked for,
// `timestamp_granularities=segment` (the only granularity 3.0 serves) and
// one `context_bias` field per keyword.
func (a *BatchAdapter) Transcribe(ctx context.Context, request runtimepkg.BatchTranscribeRequest) (*runtimepkg.BatchTranscription, error) {
	if request.Plan.Route.Provider != ProviderName {
		return nil, fmt.Errorf("mistral batch adapter cannot serve provider %q", request.Plan.Route.Provider)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if _, ok := batchModels[model]; !ok {
		return nil, fmt.Errorf("mistral batch does not support model %q", model)
	}
	if request.Options.STT.WantsWordTimestamps() {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInvalidRequest, Message: "Mistral Voxtral 3 returns segment timestamps only", Hint: "Drop word_timestamps, or route to a provider that times words."}
	}
	language, err := sttLanguage(request.Options.Language)
	if err != nil {
		return nil, err
	}
	if request.Media.Encoding != "pcm_s16le" || request.Media.SampleRateHz < 8_000 || request.Media.Channels < 1 {
		return nil, &runtimepkg.ProviderError{
			Code:    batchhttp.CodeUnsupportedMedia,
			Message: fmt.Sprintf("Mistral transcribes pcm_s16le WAV, got %s/%d Hz/%d channels", request.Media.Encoding, request.Media.SampleRateHz, request.Media.Channels),
			Hint:    "Send the audio as pcm_s16le at 16000 Hz.",
		}
	}
	// Refuse before reading the file rather than after streaming a body the
	// service would refuse.
	if request.AudioBytes > BatchMaxAudioBytes {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInputTooLarge, Message: "the upload exceeds Mistral's 500 MB transcription limit", Hint: "Split the audio or submit it as a transcription job."}
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
		return nil, fmt.Errorf("mistral batch endpoint path must be %s, got %q", batchPath, endpoint.Path)
	}
	pcm, err := readWAV(request.Audio, request.AudioBytes)
	if err != nil {
		return nil, err
	}
	if pcm.sampleRate != request.Media.SampleRateHz || pcm.channels != request.Media.Channels {
		return nil, &runtimepkg.ProviderError{
			Code:    batchhttp.CodeUnsupportedMedia,
			Message: fmt.Sprintf("the WAV header says %d Hz/%d channels, the request %d Hz/%d channels", pcm.sampleRate, pcm.channels, request.Media.SampleRateHz, request.Media.Channels),
		}
	}
	if err := batchhttp.Rewind(request.Audio); err != nil {
		return nil, err
	}
	fields := []batchhttp.MultipartField{{Name: "model", Value: model}, {Name: "timestamp_granularities", Value: "segment"}}
	if language != "" {
		fields = append(fields, batchhttp.MultipartField{Name: "language", Value: language})
	}
	if request.Options.STT.Diarize() {
		fields = append(fields, batchhttp.MultipartField{Name: "diarize", Value: "true"})
	}
	for _, keyword := range request.Options.STT.GetKeywords() {
		fields = append(fields, batchhttp.MultipartField{Name: "context_bias", Value: keyword})
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
		return nil, providerError(fmt.Sprintf("Mistral rejected the transcription with status %d", response.Status), parseError(response.Body), response.Status, response.Body)
	}
	return parseBatchResponse(response.Body, response.Header, model, language, request.Options.STT.Diarize(), pcm.dataBytes, int64(pcm.sampleRate)*int64(pcm.channels)*2)
}

// batchResponse is the JSON answer of the transcriptions endpoint. language
// comes back null even when one was sent, and speaker_id only when diarize
// was. usage.prompt_audio_seconds is an integer, floored, so it is not read.
type batchResponse struct {
	Text     *string `json:"text"`
	Segments []struct {
		Text      string   `json:"text"`
		Start     *float64 `json:"start"`
		End       *float64 `json:"end"`
		SpeakerID string   `json:"speaker_id"`
	} `json:"segments"`
}

func parseBatchResponse(body []byte, header http.Header, model, language string, diarized bool, pcmBytes, bytesPerSecond int64) (*runtimepkg.BatchTranscription, error) {
	var payload batchResponse
	if err := batchhttp.DecodeJSON(body, &payload); err != nil {
		return nil, err
	}
	// Silence still answers with an empty text; a 200 without the field is a
	// response shape this adapter does not understand.
	if payload.Text == nil {
		return nil, batchhttp.Malformed(errors.New("mistral transcription carries no text"))
	}
	segments := make([]runtimepkg.BatchSegment, 0, len(payload.Segments))
	for _, segment := range payload.Segments {
		text := strings.TrimSpace(segment.Text)
		if text == "" || segment.Start == nil || segment.End == nil {
			continue
		}
		converted := runtimepkg.BatchSegment{Text: text, StartMS: batchhttp.SecondsToMS(*segment.Start), EndMS: batchhttp.SecondsToMS(*segment.End)}
		// Speakers are reported only when the caller asked for them.
		if diarized {
			converted.Speaker = strings.TrimSpace(segment.SpeakerID)
		}
		segments = append(segments, converted)
	}
	requestID := requestIdentifier(header.Get(correlationHeader))
	// Mistral bills the audio it was sent, and the integer it reports is
	// floored, so the observation is the PCM in the uploaded container, an
	// exact fraction of a second. DurationMS stays zero for the same
	// reason: the relay then meters the bytes it sent.
	observation := &protocol.BillingObservation{
		OperationID: "request", Model: model, Mode: "batch", Language: language, ProviderRequestID: requestID, Complete: true,
		Quantities:           map[string]int64{"duration_seconds": pcmBytes},
		QuantityDenominators: map[string]int64{"duration_seconds": bytesPerSecond},
	}
	result := &runtimepkg.BatchTranscription{
		Billing:           metering.Report(observation),
		Text:              strings.TrimSpace(*payload.Text),
		Segments:          segments,
		Language:          language,
		ProviderRequestID: requestID,
		Extensions:        batchhttp.RawExtension(batchExtensionID, body),
	}
	if result.Text == "" {
		result.Text = batchhttp.JoinSegments(result.Segments)
	}
	return result, nil
}

// wavPCM is what the container's header says about its audio.
type wavPCM struct {
	sampleRate int
	channels   int
	dataBytes  int64
}

// wavFormatExtensible is WAVE_FORMAT_EXTENSIBLE, whose sub-format GUID opens
// with the real format tag.
const (
	wavFormatPCM        = 1
	wavFormatExtensible = 0xFFFE
)

// readWAV reads the container's fmt and data chunks. Only 16-bit integer
// PCM is accepted: the billed duration is the data length over the header's
// own rate and channels, which holds only for that encoding. The data length
// is the declared size, clamped to what the container actually holds.
func readWAV(audio io.ReadSeeker, total int64) (wavPCM, error) {
	unsupported := func(message string) (wavPCM, error) {
		return wavPCM{}, &runtimepkg.ProviderError{Code: batchhttp.CodeUnsupportedMedia, Message: message, Hint: "Send 16-bit PCM WAV."}
	}
	if err := batchhttp.Rewind(audio); err != nil {
		return wavPCM{}, err
	}
	header := make([]byte, wavHeaderSearchBytes)
	read, err := io.ReadFull(audio, header)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return wavPCM{}, &runtimepkg.ProviderError{Code: batchhttp.CodeProviderError, Message: "the audio could not be read", Cause: err}
	}
	header = header[:read]
	if len(header) < 12 || string(header[0:4]) != "RIFF" || string(header[8:12]) != "WAVE" {
		return unsupported("the upload is not a WAV container")
	}
	var format *wavPCM
	for offset := 12; offset+8 <= len(header); {
		id, size := string(header[offset:offset+4]), int64(binary.LittleEndian.Uint32(header[offset+4:offset+8]))
		body := header[offset+8:]
		switch {
		case id == "fmt " && size >= 16 && len(body) >= 16:
			tag := binary.LittleEndian.Uint16(body[0:2])
			if tag == wavFormatExtensible && size >= 26 && len(body) >= 26 {
				tag = binary.LittleEndian.Uint16(body[24:26])
			}
			if tag != wavFormatPCM || binary.LittleEndian.Uint16(body[14:16]) != 16 {
				return unsupported("the WAV is not 16-bit PCM")
			}
			format = &wavPCM{sampleRate: int(binary.LittleEndian.Uint32(body[4:8])), channels: int(binary.LittleEndian.Uint16(body[2:4]))}
		case id == "data":
			if format == nil {
				return unsupported("the WAV container has no format chunk before its data")
			}
			format.dataBytes = min(size, max(total-int64(offset+8), 0))
			return *format, nil
		}
		offset += 8 + int(size) + int(size&1)
	}
	return unsupported("the WAV container has no data chunk")
}
