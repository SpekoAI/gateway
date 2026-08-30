package gemini

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/SpekoAI/gateway/internal/batchhttp"
	"github.com/SpekoAI/gateway/internal/upstream"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// BatchAdapterID identifies the Interactions API implementation.
	BatchAdapterID = "gemini.stt.batch.v1"
	// BatchEndpoint is the Interactions API collection. It takes the model id
	// in the body rather than the path, so one endpoint serves every batch
	// transcription model.
	BatchEndpoint = "https://generativelanguage.googleapis.com/v1beta/interactions"

	// batchRequestCeilingBytes is the documented ceiling on a whole inline
	// request ("under 20MB total request size"). It bounds the BASE64 payload,
	// not the audio, which is why the catalog's byte limit is three quarters
	// of it less slack for the surrounding JSON.
	batchRequestCeilingBytes int64 = 20 << 20

	batchExtensionID = "generativelanguage.googleapis.com/v1beta/interactions"
)

// batchModels are the model ids this endpoint serves. The live-only id is
// absent on purpose: gemini-3.5-transcribe-live is a Live API socket and has
// no Interactions counterpart, so routing it here would bill a request the
// service refuses.
var batchModels = map[string]struct{}{
	"gemini-3.5-transcribe": {},
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
// The request is deliberately MINIMAL: model and audio, nothing else. The
// Interactions API is absent from every published discovery document, so its
// TranscriptionConfig field spellings are documented prose rather than a
// machine-readable schema, and Google's JSON surfaces reject unknown fields
// with 400 INVALID_ARGUMENT. Sending a guessed field name would therefore
// fail every request rather than degrade. Feature asks the config would carry
// are refused below instead of being silently dropped; the live route, whose
// config IS schema-published, serves them.
func (a *BatchAdapter) Transcribe(ctx context.Context, request runtimepkg.BatchTranscribeRequest) (*runtimepkg.BatchTranscription, error) {
	if request.Plan.Route.Provider != ProviderName {
		return nil, fmt.Errorf("gemini batch adapter cannot serve provider %q", request.Plan.Route.Provider)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if _, ok := batchModels[model]; !ok {
		return nil, fmt.Errorf("gemini batch adapter cannot serve model %q on the interactions endpoint", model)
	}
	if request.Options.STT.Diarize() {
		return nil, &runtimepkg.ProviderError{
			Code:    batchhttp.CodeInvalidRequest,
			Message: "speaker diarization is not available on Gemini prerecorded transcription",
			Hint:    "Use the gemini-3.5-transcribe-live route for diarized transcription, or drop the diarization option.",
		}
	}
	// base64 expands by 4/3, and the ceiling is on the whole request. Refuse
	// before reading the file rather than after building a payload the service
	// will reject.
	if encodedLen(request.AudioBytes) > batchRequestCeilingBytes {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInputTooLarge, Message: "the upload exceeds the Gemini inline request limit"}
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
	audio, err := io.ReadAll(io.LimitReader(request.Audio, batchRequestCeilingBytes+1))
	if err != nil {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeProviderError, Message: "the audio could not be read", Cause: err}
	}
	if encodedLen(int64(len(audio))) > batchRequestCeilingBytes {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInputTooLarge, Message: "the upload exceeds the Gemini inline request limit"}
	}
	body, err := json.Marshal(map[string]any{
		"model": model,
		"input": []map[string]string{{
			"type":      "audio",
			"data":      base64.StdEncoding.EncodeToString(audio),
			"mime_type": "audio/wav",
		}},
	})
	if err != nil {
		return nil, err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), strings.NewReader(string(body)))
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
	var payload batchResponse
	if err := batchhttp.DecodeJSON(response.Body, &payload); err != nil {
		return nil, err
	}
	text := payload.transcript()
	if text == "" {
		return nil, batchhttp.Failed(batchExtensionID, "the response carried no transcript")
	}
	return &runtimepkg.BatchTranscription{
		Text: text,
		// Segments stay empty. Word-level annotations are returned only when
		// the request enables them, and this request cannot: see Transcribe's
		// comment on unpinned config spellings. BatchTranscription documents
		// empty segments as the honest shape for untimed text, and the caller
		// meters from the audio it sent when DurationMS is likewise absent.
		ProviderRequestID: response.Header.Get("x-request-id"),
		Extensions:        batchhttp.RawExtension(batchExtensionID, response.Body),
	}, nil
}

// batchResponse reads the transcript from an interaction. Both spellings are
// accepted because the REST documentation shows the snake_case field while
// Google's JSON surfaces normally emit camelCase, and no discovery document
// settles which this endpoint returns.
type batchResponse struct {
	OutputTextSnake string `json:"output_text"`
	OutputTextCamel string `json:"outputText"`
}

func (r batchResponse) transcript() string {
	if text := strings.TrimSpace(r.OutputTextSnake); text != "" {
		return text
	}
	return strings.TrimSpace(r.OutputTextCamel)
}

// encodedLen is the base64 size of n bytes, saturating rather than overflowing
// on a nonsense length.
func encodedLen(n int64) int64 {
	if n < 0 || n > (1<<62)/2 {
		return 1 << 62
	}
	return (n + 2) / 3 * 4
}
