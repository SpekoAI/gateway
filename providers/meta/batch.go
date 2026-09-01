package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"

	"github.com/SpekoAI/gateway/internal/batchhttp"
	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// BatchAdapterID identifies the prerecorded multipart implementation.
	BatchAdapterID = "meta.stt.batch.v1"
	// BatchEndpoint is the prerecorded twin of the realtime socket.
	BatchEndpoint = "https://api.meta.ai/v1/asr/transcribe"
	// BatchModel is the model id the prerecorded endpoint serves. Muse Voice
	// Transcribe publishes one id for both arms, so it equals the catalog
	// row's model rather than naming a sibling.
	BatchModel = DefaultModel

	// batchRequestCeilingBytes is the documented ceiling on the whole request
	// body ("32 MB", HTTP 413 above). It bounds the multipart body, which is
	// why the usable audio limit below reserves room for the envelope.
	batchRequestCeilingBytes int64 = 32 << 20
	// BatchMaxAudioBytes is the whole-file ceiling that follows: the multipart
	// framing and the request JSON part fit comfortably in a 16 KiB reserve.
	BatchMaxAudioBytes int64 = batchRequestCeilingBytes - (16 << 10)
	// BatchMaxDurationSeconds is the documented ten-minute audio cap (HTTP 400
	// above). At 24 kHz mono the byte cap admits ~11.6 minutes, so this binds
	// first at every rate the relay carries, and it is what transcription
	// jobs chunk to.
	BatchMaxDurationSeconds int64 = 600

	batchExtensionID = "api.meta.ai/v1/asr/transcribe"

	// Wire literals of the multipart request.
	requestPartName = "request"
	audioPartName   = "audio"
	encodingWAV     = "WAV"
)

// BatchConfig controls local transport limits for the prerecorded adapter.
type BatchConfig struct {
	AdapterID             string
	HTTPClient            *http.Client
	MaxResponseBytes      int64
	AllowedEndpointHosts  []string
	AllowInsecureEndpoint bool
}

// BatchAdapter implements runtime.BatchTranscriber over POST /v1/asr/transcribe.
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
		return nil, errors.New("meta batch maximum response bytes must be positive")
	}
	policy, err := upstream.NewHTTPPolicy(officialHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &BatchAdapter{id: config.AdapterID, httpClient: config.HTTPClient, maxResponseBytes: config.MaxResponseBytes, endpointPolicy: policy}, nil
}

func (a *BatchAdapter) ID() string { return a.id }

// Transcribe POSTs the WAV as the multipart `audio` part beside a JSON
// `request` part.
//
// The request part is typed application/json explicitly — the documented
// curl spells it `;type=application/json` — which the shared multipart helper
// cannot express for a non-file field, so the body is framed here.
func (a *BatchAdapter) Transcribe(ctx context.Context, request runtimepkg.BatchTranscribeRequest) (*runtimepkg.BatchTranscription, error) {
	if request.Plan.Route.Provider != ProviderName {
		return nil, fmt.Errorf("meta batch adapter cannot serve provider %q", request.Plan.Route.Provider)
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model != BatchModel {
		return nil, fmt.Errorf("meta batch adapter cannot serve model %q on the transcribe endpoint", model)
	}
	// Refuse before reading the file rather than after streaming a body the
	// service will answer 413 to.
	if request.AudioBytes > BatchMaxAudioBytes {
		return nil, &runtimepkg.ProviderError{Code: batchhttp.CodeInputTooLarge, Message: "the upload exceeds the Meta transcribe request limit"}
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
	settings, err := json.Marshal(batchSettings(model, request.Options))
	if err != nil {
		return nil, err
	}
	// AudioBytes is a declaration; the bounded reader makes the file itself
	// fail at the ceiling so a stream longer than the declaration cannot
	// slip past the check above, and is never silently truncated either.
	body, contentType := multipartBody(settings, &boundedReader{reader: request.Audio, remaining: BatchMaxAudioBytes})
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
	var decoded transcription
	if err := batchhttp.DecodeJSON(response.Body, &decoded); err != nil {
		return nil, err
	}
	segments := decoded.segments()
	text := strings.TrimSpace(decoded.Transcript)
	if text == "" {
		text = batchhttp.JoinSegments(segments)
	}
	if text == "" {
		return nil, batchhttp.Failed(batchExtensionID, "the response carried no transcript")
	}
	return &runtimepkg.BatchTranscription{
		Text:              text,
		Segments:          segments,
		DurationMS:        decoded.AudioDurationMs,
		ProviderRequestID: decoded.SessionID,
		Extensions:        batchhttp.RawExtension(batchExtensionID, response.Body),
	}, nil
}

// batchSettings is the JSON `request` part. The mode follows the caller's
// asks the same way the realtime handshake does: DIARIZATION for speaker
// labels, ENDPOINTING otherwise, so the response is split into turns with
// timestamps in either case.
func batchSettings(model string, options protocol.RequestOptions) map[string]any {
	settings := map[string]any{
		"model":         model,
		"mode":          modeFor(options),
		"audioEncoding": encodingWAV,
	}
	if bias := languageBias(options.Language); bias != "" {
		settings["languageBias"] = []string{bias}
	}
	if options.STT != nil {
		if keywords := trimmedKeywords(options.STT.Keywords); len(keywords) > 0 {
			settings["keywords"] = keywords
		}
	}
	return settings
}

// multipartBody streams the two-part form: the JSON settings first, then the
// WAV read from audio as the HTTP client consumes the request.
func multipartBody(settings []byte, audio io.Reader) (io.ReadCloser, string) {
	reader, writer := io.Pipe()
	form := multipart.NewWriter(writer)
	go func() {
		err := func() error {
			header := make(textproto.MIMEHeader, 2)
			header.Set("Content-Disposition", fmt.Sprintf(`form-data; name="%s"`, requestPartName))
			header.Set("Content-Type", "application/json")
			part, err := form.CreatePart(header)
			if err != nil {
				return err
			}
			if _, err := part.Write(settings); err != nil {
				return err
			}
			header = make(textproto.MIMEHeader, 2)
			header.Set("Content-Disposition", fmt.Sprintf(`form-data; name="%s"; filename="audio.wav"`, audioPartName))
			header.Set("Content-Type", "audio/wav")
			part, err = form.CreatePart(header)
			if err != nil {
				return err
			}
			if _, err := io.Copy(part, audio); err != nil {
				return err
			}
			return form.Close()
		}()
		writer.CloseWithError(err)
	}()
	return reader, form.FormDataContentType()
}

// boundedReader fails the upload once more than the ceiling has been read.
type boundedReader struct {
	reader    io.Reader
	remaining int64
}

// errUploadTooLarge ends the multipart pipe when the audio outruns its
// declaration; the HTTP client surfaces it as the request's transport error.
var errUploadTooLarge = errors.New("meta transcribe upload exceeds the request limit")

func (b *boundedReader) Read(p []byte) (int, error) {
	if b.remaining <= 0 {
		return 0, errUploadTooLarge
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
	}
	return n, err
}

// transcription is the JSON answer of the prerecorded endpoint.
type transcription struct {
	SessionID       string `json:"sessionId"`
	Transcript      string `json:"transcript"`
	AudioDurationMs int64  `json:"audioDurationMs"`
	Turns           []struct {
		TurnID     int64  `json:"turnId"`
		StartMs    int64  `json:"startMs"`
		EndMs      int64  `json:"endMs"`
		Transcript string `json:"transcript"`
		Speaker    string `json:"speaker"`
	} `json:"turns"`
}

// segments renders the turns as relay segments. Turn-level timing is the
// finest the service offers, so each turn is one segment; an empty turn is
// dropped rather than published as a silent span.
func (t transcription) segments() []runtimepkg.BatchSegment {
	segments := make([]runtimepkg.BatchSegment, 0, len(t.Turns))
	for _, turn := range t.Turns {
		text := strings.TrimSpace(turn.Transcript)
		if text == "" {
			continue
		}
		segments = append(segments, runtimepkg.BatchSegment{Text: text, StartMS: turn.StartMs, EndMS: turn.EndMs, Speaker: turn.Speaker})
	}
	return segments
}
