package azure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/SpekoAI/gateway/internal/batchhttp"
	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// ProviderName is the relay catalog provider this package serves. It is
	// `azure`, not `microsoft`, because the credential is an Azure Speech
	// resource key and the platform already files Azure Speech STT and TTS
	// under that vendor slot.
	ProviderName = "azure"
	// BatchAdapterID identifies the prerecorded implementation.
	BatchAdapterID = "azure.stt.batch.v1"
	// DefaultModel is the MAI model the adapter asks for when the route
	// names none; it is also the only model the catalog row publishes.
	DefaultModel = "MAI-Transcribe-2"
	// BatchModel is the model id the catalog row's batch route names.
	BatchModel = DefaultModel
	// BatchEndpoint is the fast-transcription action on the regional Speech
	// host of a region that serves MAI-Transcribe. The api-version query is
	// NOT part of the catalog endpoint: the upstream policy refuses a
	// preexisting query so the adapter always owns the parameters it sends.
	BatchEndpoint = "https://eastus.api.cognitive.microsoft.com/speechtotext/transcriptions:transcribe"
	// APIVersion is the fast-transcription API version that carries
	// enhancedMode.
	APIVersion = "2025-10-15"

	// batchRequestCeilingBytes is the documented ceiling on the audio file
	// ("less than 300 MB"). It bounds the multipart body, which is why the
	// usable audio limit below reserves room for the envelope.
	batchRequestCeilingBytes int64 = 300 << 20
	// BatchMaxAudioBytes is the whole-file ceiling that follows: the
	// multipart framing and the definition part fit comfortably in a 16 KiB
	// reserve.
	BatchMaxAudioBytes int64 = batchRequestCeilingBytes - (16 << 10)
	// BatchMaxDurationSeconds is the documented five-hour cap of the
	// fast-transcription endpoint. At 16 kHz mono the byte cap admits ~2.7
	// hours, so the byte cap binds first at every rate the relay carries.
	BatchMaxDurationSeconds int64 = 5 * 3600

	// officialHost is the regional Speech host family; resourceHost is the
	// custom-subdomain family a Foundry resource answers on. Both are
	// documented for the same action and the same key.
	officialHost = "*.api.cognitive.microsoft.com"
	resourceHost = "*.cognitiveservices.azure.com"

	batchExtensionID = "cognitive.microsoft.com/speechtotext/transcriptions:transcribe"

	// Wire literals of the multipart request.
	definitionPartName = "definition"
	audioPartName      = "audio"
	subscriptionHeader = "Ocp-Apim-Subscription-Key"
	modelPrefix        = "mai-transcribe"
)

// BatchConfig controls local transport limits for the prerecorded adapter.
type BatchConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	MaxResponseBytes      int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// BatchAdapter implements runtime.BatchTranscriber over the fast-transcription
// action in enhanced mode.
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
		return nil, errors.New("azure batch maximum response bytes must be positive")
	}
	hosts := append([]string{resourceHost}, config.AllowedEndpointHosts...)
	policy, err := upstream.NewHTTPPolicy(officialHost, hosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &BatchAdapter{id: config.AdapterID, httpClient: config.HTTPClient, maxResponseBytes: config.MaxResponseBytes, endpointPolicy: policy}, nil
}

func (a *BatchAdapter) ID() string { return a.id }

// Transcribe POSTs the WAV as the multipart `audio` part beside the JSON
// `definition` text part, with api-version appended to the catalog endpoint.
func (a *BatchAdapter) Transcribe(ctx context.Context, request runtimepkg.BatchTranscribeRequest) (*runtimepkg.BatchTranscription, error) {
	if request.Plan.Route.Provider != ProviderName {
		return nil, fmt.Errorf("azure batch adapter cannot serve provider %q", request.Plan.Route.Provider)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model == "" {
		model = DefaultModel
	}
	// enhancedMode is what selects a MAI model; a route naming anything else
	// would silently run Azure's classic recognizer under the MAI rate card.
	if !strings.HasPrefix(strings.ToLower(model), modelPrefix) {
		return nil, fmt.Errorf("azure batch adapter cannot serve model %q on the enhanced-mode endpoint", model)
	}
	// Refuse before reading the file rather than after streaming a body the
	// service will answer 413 to.
	if request.AudioBytes > BatchMaxAudioBytes {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInputTooLarge, Message: "the upload exceeds the Azure fast-transcription file limit"}
	}
	credential, err := batchhttp.Credential(request.Plan)
	if err != nil {
		return nil, err
	}
	endpoint, err := a.endpointPolicy.Parse(request.Plan.Route.Endpoint)
	if err != nil {
		return nil, err
	}
	endpoint.RawQuery = url.Values{"api-version": {APIVersion}}.Encode()
	if err := batchhttp.Rewind(request.Audio); err != nil {
		return nil, err
	}
	definition, err := json.Marshal(batchDefinition(model, request.Options))
	if err != nil {
		return nil, err
	}
	body, contentType := batchhttp.Multipart(
		[]batchhttp.MultipartField{{Name: definitionPartName, Value: string(definition)}},
		audioPartName, "audio.wav", "audio/wav",
		&boundedReader{reader: request.Audio, remaining: BatchMaxAudioBytes},
	)
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), body)
	if err != nil {
		body.Close()
		return nil, err
	}
	httpRequest.Header.Set("Content-Type", contentType)
	httpRequest.Header.Set("Accept", "application/json")
	httpRequest.Header.Set(subscriptionHeader, credential)

	response, err := batchhttp.Do(a.httpClient, httpRequest, a.maxResponseBytes)
	if err != nil {
		return nil, err
	}
	if response.Status < 200 || response.Status >= 300 {
		return nil, batchhttp.StatusError(batchExtensionID, response.Status, response.Body)
	}
	var decoded transcription
	if err := batchhttp.DecodeJSON(response.Body, &decoded); err != nil {
		return nil, err
	}
	segments := decoded.segments()
	text := decoded.combinedText()
	if text == "" {
		text = batchhttp.JoinSegments(segments)
	}
	if text == "" {
		return nil, batchhttp.Failed(batchExtensionID, "the response carried no transcript")
	}
	return &runtimepkg.BatchTranscription{
		Text:              text,
		Segments:          segments,
		Language:          decoded.language(),
		DurationMS:        decoded.DurationMilliseconds,
		ProviderRequestID: requestID(response.Header),
		Extensions:        batchhttp.RawExtension(batchExtensionID, response.Body),
	}, nil
}

// batchDefinition is the JSON `definition` part. Enhanced mode with the
// route's model is what makes this a MAI request; word timestamps are always
// asked for so the phrases carry offsets the relay can publish as segments
// (the default, "none", omits timing altogether). Style is left at the
// vendor default (verbatim) because nothing on the relay contract asks for
// one.
func batchDefinition(model string, options protocol.RequestOptions) map[string]any {
	definition := map[string]any{
		"enhancedMode": map[string]any{
			"enabled":      true,
			"model":        model,
			"modelOptions": map[string]any{"timestamps": "word"},
		},
	}
	if code := locale(options.Language); code != "" {
		definition["locales"] = []string{code}
	}
	if options.STT.Diarize() {
		definition["diarization"] = map[string]any{"enabled": true}
	}
	if keywords := options.STT.GetKeywords(); len(keywords) > 0 {
		definition["phraseList"] = map[string]any{"phrases": keywords}
	}
	return definition
}

// requestID is Azure's per-call identifier, carried on the response headers
// rather than in the body.
func requestID(header http.Header) string {
	for _, name := range []string{"apim-request-id", "x-ms-request-id", "x-requestid"} {
		if value := strings.TrimSpace(header.Get(name)); value != "" {
			return value
		}
	}
	return ""
}

// boundedReader fails the upload once more than the ceiling has been read.
type boundedReader struct {
	reader    io.Reader
	remaining int64
}

// errUploadTooLarge ends the multipart pipe when the audio outruns its
// declaration; the HTTP client surfaces it as the request's transport error.
var errUploadTooLarge = errors.New("azure fast-transcription upload exceeds the file limit")

func (b *boundedReader) Read(p []byte) (int, error) {
	if b.remaining <= 0 {
		// The ceiling was reached on an earlier call and the probe below
		// found nothing past it, so the file is complete.
		return 0, io.EOF
	}
	if int64(len(p)) > b.remaining {
		p = p[:b.remaining]
	}
	n, err := b.reader.Read(p)
	b.remaining -= int64(n)
	if b.remaining == 0 && err == nil {
		// Peek one more byte: a file exactly at the ceiling is fine, one past
		// it is not.
		var probe [1]byte
		if extra, _ := b.reader.Read(probe[:]); extra > 0 {
			return n, errUploadTooLarge
		}
		return n, io.EOF
	}
	return n, err
}

// transcription is the JSON answer of the fast-transcription action.
type transcription struct {
	DurationMilliseconds int64 `json:"durationMilliseconds"`
	CombinedPhrases      []struct {
		Channel *int   `json:"channel"`
		Text    string `json:"text"`
	} `json:"combinedPhrases"`
	Phrases []struct {
		Channel              *int    `json:"channel"`
		Speaker              *int    `json:"speaker"`
		OffsetMilliseconds   int64   `json:"offsetMilliseconds"`
		DurationMilliseconds int64   `json:"durationMilliseconds"`
		Text                 string  `json:"text"`
		Locale               string  `json:"locale"`
		Confidence           float64 `json:"confidence"`
	} `json:"phrases"`
}

// combinedText joins the per-channel combined transcripts. The relay sends
// mono, so there is normally exactly one.
func (t transcription) combinedText() string {
	parts := make([]string, 0, len(t.CombinedPhrases))
	for _, combined := range t.CombinedPhrases {
		if text := strings.TrimSpace(combined.Text); text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, " ")
}

// segments renders the phrases as relay segments. A phrase is the finest
// span the service times consistently (words are per-phrase children); an
// empty phrase is dropped rather than published as a silent span. The
// speaker is Azure's integer label rendered as text, and only when the
// service assigned one.
func (t transcription) segments() []runtimepkg.BatchSegment {
	segments := make([]runtimepkg.BatchSegment, 0, len(t.Phrases))
	for _, phrase := range t.Phrases {
		text := strings.TrimSpace(phrase.Text)
		if text == "" {
			continue
		}
		segment := runtimepkg.BatchSegment{Text: text, StartMS: phrase.OffsetMilliseconds, EndMS: phrase.OffsetMilliseconds + phrase.DurationMilliseconds}
		if phrase.Speaker != nil {
			segment.Speaker = strconv.Itoa(*phrase.Speaker)
		}
		segments = append(segments, segment)
	}
	return segments
}

// language is the locale the service reported, when every timed phrase
// agrees on one; a code-switched answer reports none rather than the first
// phrase's language.
func (t transcription) language() string {
	reported := ""
	for _, phrase := range t.Phrases {
		code := strings.TrimSpace(phrase.Locale)
		if code == "" || strings.TrimSpace(phrase.Text) == "" {
			continue
		}
		if reported == "" {
			reported = code
			continue
		}
		if !strings.EqualFold(reported, code) {
			return ""
		}
	}
	return reported
}
