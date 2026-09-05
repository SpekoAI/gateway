package gemini

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/SpekoAI/gateway/internal/batchhttp"
	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// BatchAdapterID identifies the Interactions API implementation.
	BatchAdapterID = "gemini.stt.batch.v1"
	// BatchEndpoint is the Interactions API collection. It takes the model id
	// in the body rather than the path, so one endpoint serves every batch
	// transcription model.
	BatchEndpoint = "https://generativelanguage.googleapis.com/v1beta/interactions"

	// BatchModel is the model id the interactions endpoint serves. It differs
	// from the catalog's customer-facing row id, which names the live socket.
	BatchModel = "gemini-3.5-transcribe"

	// batchRequestCeilingBytes is the documented ceiling on a whole inline
	// request ("under 20MB total request size"). It bounds the BASE64 payload,
	// not the audio, which is why the usable audio limit below is three
	// quarters of it less slack for the surrounding JSON.
	batchRequestCeilingBytes int64 = 20 << 20
	// BatchMaxAudioBytes is the whole-file ceiling that follows: base64 expands
	// by 4/3, and a 16 KiB reserve covers the JSON envelope and the WAV header.
	// The catalog restates this expression rather than importing it — the
	// control plane consumes the catalog too — and a connector test pins the
	// two together.
	BatchMaxAudioBytes int64 = (20<<20)/4*3 - (16 << 10)
	// BatchMaxDurationSeconds is not documented separately; the byte cap binds
	// first at every rate the relay carries (eight minutes of 16 kHz mono is
	// already inside it). It is what transcription jobs chunk to.
	BatchMaxDurationSeconds int64 = 480

	batchExtensionID = "generativelanguage.googleapis.com/v1beta/interactions"

	// Wire literals from the generated Interactions types.
	stepModelOutput    = "model_output"
	contentText        = "text"
	annotationWordInfo = "word_info"
)

// batchModels are the model ids this endpoint serves. The live-only id is
// absent on purpose: gemini-3.5-transcribe-live is a Live API socket and has
// no Interactions counterpart, so routing it here would bill a request the
// service refuses.
var batchModels = map[string]struct{}{
	BatchModel: {},
}

// BatchConfig controls local transport limits for the Interactions adapter.
type BatchConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	MaxResponseBytes      int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// BatchAdapter implements runtime.BatchTranscriber over the Interactions API.
//
// Audio rides the request INLINE as base64 rather than through the Files API.
// The two-step upload buys longer audio, but it also introduces a second
// endpoint, a file lifecycle the relay would have to clean up, and a window in
// which customer audio sits addressable in Google's file store. Inline keeps
// one request per transcription and no residue; the catalog's batch limits
// chunk anything longer, which is the same trade the Cloud Speech row already
// makes.
type BatchAdapter struct {
	id               string
	httpClient       *http.Client
	maxResponseBytes int64
	endpointPolicy   upstream.HTTPPolicy
}

// NewBatch creates the Interactions adapter.
func NewBatch(config BatchConfig) (*BatchAdapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = BatchAdapterID
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = batchhttp.DefaultMaxResponseBytes
	}
	if config.MaxResponseBytes < 1 {
		return nil, errors.New("gemini batch maximum response bytes must be positive")
	}
	policy, err := upstream.NewHTTPPolicy(officialHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &BatchAdapter{id: config.AdapterID, httpClient: config.HTTPClient, maxResponseBytes: config.MaxResponseBytes, endpointPolicy: policy}, nil
}

func (a *BatchAdapter) ID() string { return a.id }

// Transcribe POSTs the WAV inline as one interaction.
//
// Request and response field names come from the generated Interactions types
// in Google's own genai SDK (google-genai 2.20.0,
// google/genai/_gaos/types/interactions), which is the machine-readable schema
// the REST discovery documents do not publish for this surface.
//
// Transcription settings ride generation_config.transcription_config, NOT the
// request root. Of its fields, language_codes and custom_vocabulary are
// current, while adaptation_phrases and the ROOT-level diarization_mode and
// timestamp_granularities are all marked deprecated in favour of the
// discriminated `mode` object — so this adapter sends the mode object and none
// of the deprecated spellings.
func (a *BatchAdapter) Transcribe(ctx context.Context, request runtimepkg.BatchTranscribeRequest) (*runtimepkg.BatchTranscription, error) {
	if request.Plan.Route.Provider != ProviderName {
		return nil, fmt.Errorf("gemini batch adapter cannot serve provider %q", request.Plan.Route.Provider)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if _, ok := batchModels[model]; !ok {
		return nil, fmt.Errorf("gemini batch adapter cannot serve model %q on the interactions endpoint", model)
	}
	// Refuse before reading the file rather than after building a payload the
	// service will reject.
	if request.AudioBytes > BatchMaxAudioBytes {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInputTooLarge, Message: "the upload exceeds the Gemini inline request limit"}
	}
	// Google refuses custom_vocabulary alongside the verbatim mode's speaker
	// labels or word timings ("custom_vocabulary is incompatible with
	// timestamps", HTTP 400, observed live 2026-09-05). Refuse here with the
	// conflict named rather than sending a request the service rejects, and
	// never drop one ask to satisfy the other.
	if verbatimModeRequested(request.Options) && len(trimmedKeywords(request.Options.STT.GetKeywords())) > 0 {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInvalidRequest, Message: "Gemini cannot combine keywords with diarization or word timestamps; drop one of the asks"}
	}
	credential, err := batchhttp.Credential(request.Plan)
	if err != nil {
		return nil, err
	}
	endpoint, err := a.endpointPolicy.Parse(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}
	if err := batchhttp.Rewind(request.Audio); err != nil {
		return nil, err
	}
	audio, err := io.ReadAll(io.LimitReader(request.Audio, BatchMaxAudioBytes+1))
	if err != nil {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeProviderError, Message: "the audio could not be read", Cause: err}
	}
	// AudioBytes is a declaration; this is the file. A stream longer than the
	// declaration must not slip past the check above.
	if int64(len(audio)) > BatchMaxAudioBytes {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInputTooLarge, Message: "the upload exceeds the Gemini inline request limit"}
	}
	payload := map[string]any{
		"model": model,
		"input": []map[string]any{{
			"type":      "audio",
			"data":      base64.StdEncoding.EncodeToString(audio),
			"mime_type": "audio/wav",
		}},
	}
	if transcription := transcriptionConfig(request.Options); len(transcription) > 0 {
		payload["generation_config"] = map[string]any{"transcription_config": transcription}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "application/json")
	// The AI Studio surface authenticates with a raw API key, not a bearer.
	httpRequest.Header.Set(APIKeyHeader, credential)

	response, err := batchhttp.Do(a.httpClient, httpRequest, a.maxResponseBytes)
	if err != nil {
		return nil, err
	}
	if response.Status < 200 || response.Status >= 300 {
		return nil, batchhttp.StatusError(batchExtensionID, response.Status, response.Body)
	}
	var decoded interaction
	if err := batchhttp.DecodeJSON(response.Body, &decoded); err != nil {
		return nil, err
	}
	text := decoded.transcript()
	if text == "" {
		return nil, batchhttp.Failed(batchExtensionID, "the response carried no transcript")
	}
	words := decoded.words()
	return &runtimepkg.BatchTranscription{
		Text: text,
		// Words are annotations on the transcript text, present only when the
		// request asked for verbatim mode above. Absent, Segments stays empty,
		// which BatchTranscription documents as the honest shape for untimed
		// text.
		Segments: batchhttp.GroupWords(words, 0),
		// Per-word timings surface only when the caller asked for them; a
		// diarization-only request keeps its segments and nothing more.
		Words: wordTimings(words, request.Options),
		// DurationMS stays zero: the interaction's usage block reports tokens
		// by modality and no audio duration at all, so the caller meters from
		// the audio it sent rather than from a number this response invents.
		ProviderRequestID: decoded.ID,
		Extensions:        batchhttp.RawExtension(batchExtensionID, response.Body),
	}, nil
}

// transcriptionConfig renders the caller's asks onto
// generation_config.transcription_config, or nil when they asked for nothing.
//
// `mode` is left unset unless diarization was requested. The two modes are
// verbatim and smart, and only verbatim carries speaker labels and word
// timings — the Live API's twin of this field states the incompatibility
// outright ("Timestamps and diarization are incompatible with mode SMART"),
// and the smart mode object here carries neither field to set.
//
// Which mode this endpoint defaults to is NOT documented in the generated
// types, so the adapter does not assume: naming a mode when the caller has no
// need of one could silently switch every transcript between verbatim output
// and smart's disfluency removal and auto-formatting. Leaving it unset keeps
// whichever default the service has, and the one case that genuinely requires
// verbatim asks for it explicitly.
func transcriptionConfig(options protocol.RequestOptions) map[string]any {
	config := map[string]any{}
	if language := strings.TrimSpace(options.Language); language != "" {
		config["language_codes"] = []string{language}
	}
	if options.STT != nil {
		if keywords := trimmedKeywords(options.STT.Keywords); len(keywords) > 0 {
			config["custom_vocabulary"] = keywords
		}
	}
	// Both asks ride the same verbatim mode object. Word timings are always
	// requested inside it: diarization needs them to build speaker segments,
	// and word_timestamps is nothing else. Which of the two the caller asked
	// for decides what the RESULT exposes (see wordTimings), not the wire.
	if verbatimModeRequested(options) {
		mode := map[string]any{
			"type":                    "verbatim",
			"timestamp_granularities": []string{"word"},
		}
		if options.STT.Diarize() {
			mode["diarization_mode"] = "speaker"
		}
		config["mode"] = mode
	}
	if len(config) == 0 {
		return nil
	}
	return config
}

// verbatimModeRequested reports whether the caller asked for something only
// the verbatim mode carries: speaker labels or per-word timings.
func verbatimModeRequested(options protocol.RequestOptions) bool {
	return options.STT.Diarize() || options.STT.WantsWordTimestamps()
}

// wordTimings maps the annotations onto the result's Words when, and only
// when, the caller asked for word_timestamps. Every word_info annotation
// carries both offsets, so a word with none is a degraded reading (offsetMS
// yields zero), kept rather than dropped so the word list stays complete.
func wordTimings(words []batchhttp.Word, options protocol.RequestOptions) []runtimepkg.BatchWord {
	if !options.STT.WantsWordTimestamps() || len(words) == 0 {
		return nil
	}
	timed := make([]runtimepkg.BatchWord, 0, len(words))
	for _, word := range words {
		timed = append(timed, runtimepkg.BatchWord{Text: word.Text, StartMS: word.StartMS, EndMS: word.EndMS, Speaker: word.Speaker})
	}
	return timed
}

// interaction is the subset of the Interactions response this adapter reads.
//
// output_text is documented as "concatenated text from the last model output",
// but Google's own SDK does not trust it: it recomputes the value from steps
// on every parse. This decoder does the same in reverse — steps first, the
// flat field only as a fallback — so a response that omits the convenience
// field still yields a transcript.
type interaction struct {
	ID         string `json:"id"`
	OutputText string `json:"output_text"`
	Steps      []struct {
		Type    string `json:"type"`
		Content []struct {
			Type        string `json:"type"`
			Text        string `json:"text"`
			Annotations []struct {
				Type        string `json:"type"`
				Text        string `json:"text"`
				StartOffset string `json:"start_offset"`
				EndOffset   string `json:"end_offset"`
				Speaker     string `json:"speaker"`
			} `json:"annotations"`
		} `json:"content"`
	} `json:"steps"`
}

func (i interaction) transcript() string {
	var parts []string
	for _, step := range i.Steps {
		if step.Type != stepModelOutput {
			continue
		}
		for _, content := range step.Content {
			if content.Type == contentText && content.Text != "" {
				parts = append(parts, content.Text)
			}
		}
	}
	if joined := strings.TrimSpace(strings.Join(parts, "")); joined != "" {
		return joined
	}
	return strings.TrimSpace(i.OutputText)
}

// words flattens the word_info annotations across the model output. Other
// annotation kinds (url_citation, file_citation, place_citation) share the
// list and are skipped by type.
func (i interaction) words() []batchhttp.Word {
	var words []batchhttp.Word
	for _, step := range i.Steps {
		if step.Type != stepModelOutput {
			continue
		}
		for _, content := range step.Content {
			for _, annotation := range content.Annotations {
				if annotation.Type != annotationWordInfo || strings.TrimSpace(annotation.Text) == "" {
					continue
				}
				words = append(words, batchhttp.Word{
					Text:    annotation.Text,
					StartMS: offsetMS(annotation.StartOffset),
					EndMS:   offsetMS(annotation.EndOffset),
					Speaker: annotation.Speaker,
				})
			}
		}
	}
	return words
}

// offsetMS reads a protobuf Duration in its JSON form — a decimal number of
// seconds with a trailing "s", such as "1.500s". An unparseable or absent
// offset yields zero rather than an error: a missing timestamp is a degraded
// segment, not a failed transcription.
func offsetMS(offset string) int64 {
	offset = strings.TrimSuffix(strings.TrimSpace(offset), "s")
	if offset == "" {
		return 0
	}
	seconds, err := strconv.ParseFloat(offset, 64)
	if err != nil {
		return 0
	}
	return batchhttp.SecondsToMS(seconds)
}
