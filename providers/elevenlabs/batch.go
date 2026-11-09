package elevenlabs

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/SpekoAI/gateway/internal/batchhttp"
	"github.com/SpekoAI/gateway/internal/upstream"
	billing "github.com/SpekoAI/gateway/metering"
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
	// webhook=true makes Scribe accept the job and transcribe later. This
	// adapter returns the transcript from the response it waits for, so it
	// would return nothing and could not know what Scribe ends up billing.
	if batchProviderFlag(request, "webhook") {
		return nil, &runtimepkg.ProviderError{Code: "invalid_request", Message: "ElevenLabs webhook mode is asynchronous; this batch API returns the transcript in the response, so webhook is not supported"}
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
		fields = append(fields, batchhttp.MultipartField{Name: "language_code", Value: sttLanguageCode(language)})
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
	// Scribe's batch and realtime models bill at different rates, so the
	// observation names the batch model that ran. audio_duration_secs is the
	// length Scribe processed, but the API documents it as optional. When it
	// is missing, the duration comes from the upload itself: the request is
	// always a WAV container of PCM in request.Media, so its data chunk gives
	// the exact length, as a fraction of a second like the realtime path. Any
	// other encoding leaves the observation incomplete.
	observation := billing.Duration("request", model, "batch", response.Body, 1000, "audio_duration_secs")
	// Only a final 200 carries a finished transcription to measure.
	if !observation.Complete && response.Status == http.StatusOK {
		if pcm, ok := wavPCMBytes(request.Audio, request.AudioBytes); ok && request.Media.Encoding == "pcm_s16le" && request.Media.SampleRateHz > 0 {
			// Scribe bills each channel independently with
			// use_multi_channel, so the quantity is channel-seconds: every
			// channel's PCM counts. Otherwise it is the wall-clock length.
			denominator := int64(request.Media.SampleRateHz) * 2
			if !batchProviderFlag(request, "use_multi_channel") {
				denominator *= int64(max(request.Media.Channels, 1))
			}
			observation.Quantities["duration_seconds"] = pcm
			observation.QuantityDenominators = map[string]int64{"duration_seconds": denominator}
			observation.Complete = true
		}
	}
	observation.ProviderRequestID = result.ProviderRequestID
	result.Billing = billing.Report(observation)
	return result, nil
}

// wavPCMBytes returns the size of a WAV container's data chunk, walking the
// RIFF chunks from the start. A data size the writer left open (0 or
// 0xFFFFFFFF) or one past the end of the upload is bounded by the bytes
// actually uploaded. The reader is rewound afterwards.
func wavPCMBytes(audio io.ReadSeeker, total int64) (int64, bool) {
	if audio == nil {
		return 0, false
	}
	if _, err := audio.Seek(0, io.SeekStart); err != nil {
		return 0, false
	}
	defer func() { _, _ = audio.Seek(0, io.SeekStart) }()
	header := make([]byte, 12)
	if _, err := io.ReadFull(audio, header); err != nil || string(header[0:4]) != "RIFF" || string(header[8:12]) != "WAVE" {
		return 0, false
	}
	position := int64(12)
	chunk := make([]byte, 8)
	for {
		if _, err := io.ReadFull(audio, chunk); err != nil {
			return 0, false
		}
		position += 8
		size := int64(binary.LittleEndian.Uint32(chunk[4:8]))
		if string(chunk[0:4]) == "data" {
			if remaining := total - position; total > 0 && (size == 0 || size == 0xFFFFFFFF || size > remaining) {
				size = remaining
			}
			return size, size > 0
		}
		skip := size + size%2
		if _, err := audio.Seek(skip, io.SeekCurrent); err != nil {
			return 0, false
		}
		position += skip
	}
}

// batchProviderFlag reports whether the caller set an ElevenLabs provider
// option to true.
func batchProviderFlag(request runtimepkg.BatchTranscribeRequest, key string) bool {
	value, ok := request.Options.STT.Provider("elevenlabs")[key]
	return ok && strings.EqualFold(strings.TrimSpace(protocol.SttOptionString(value)), "true")
}
