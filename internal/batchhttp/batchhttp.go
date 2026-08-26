// Package batchhttp is the shared plumbing under every provider's
// BatchTranscriber: credential extraction, bounded HTTP exchanges, the closed
// status-to-ProviderError mapping, streaming multipart bodies, the poll loop
// for job-style APIs, and word-to-segment grouping. It holds no provider
// knowledge; each adapter owns its own request and response shapes.
package batchhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

// Provider-error codes on the closed set the relay's classifier understands.
// Any other string collapses to provider_error downstream, so adapters use
// only these.
const (
	CodeAuthenticationFailed = "authentication_failed"
	CodeRateLimited          = "provider_rate_limited"
	CodeUnavailable          = "provider_unavailable"
	CodeInvalidRequest       = "invalid_request"
	CodeInputTooLarge        = "input_too_large"
	CodeUnsupportedMedia     = "unsupported_media"
	CodeRequestTimeout       = "request_timeout"
	CodeProviderError        = "provider_error"
)

// DefaultMaxResponseBytes bounds a transcript body. Word-level output for
// several hours of audio runs to tens of megabytes; 64 MiB leaves room while
// still refusing an unbounded stream.
const DefaultMaxResponseBytes int64 = 64 << 20

// Credential returns the plan's provider key. Bearer is the norm; relay_access
// is accepted because a relay connector that synthesizes the plan itself
// labels the same permanent key either way (see the realtime adapters'
// acceptableCredentialKind).
func Credential(plan protocol.SessionPlan) (string, error) {
	credential := plan.Route.Credential
	if credential == nil {
		return "", errors.New("batch transcription requires a credential")
	}
	if credential.Kind != protocol.CredentialBearer && !(plan.Execution.ProviderRoute == protocol.RouteSpekoRelay && credential.Kind == protocol.CredentialRelayAccess) {
		return "", fmt.Errorf("batch transcription cannot use credential kind %q", credential.Kind)
	}
	value := strings.TrimSpace(credential.Value)
	if value == "" {
		return "", errors.New("batch transcription requires a non-empty credential")
	}
	return value, nil
}

// Client defaults a nil client.
func Client(client *http.Client) *http.Client {
	if client != nil {
		return client
	}
	return http.DefaultClient
}

// Response is one bounded HTTP exchange.
type Response struct {
	Status int
	Header http.Header
	Body   []byte
}

// Do performs the request and reads at most maxBody bytes of the response. A
// body that exceeds the bound is a provider_error rather than a truncated
// document parsed as if complete. Transport failures are classified: a context
// deadline is request_timeout, anything else provider_unavailable, both
// retryable.
func Do(client *http.Client, request *http.Request, maxBody int64) (*Response, error) {
	if maxBody <= 0 {
		maxBody = DefaultMaxResponseBytes
	}
	response, err := Client(client).Do(request)
	if err != nil {
		return nil, TransportError(request.Context(), err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxBody+1))
	if err != nil {
		return nil, TransportError(request.Context(), err)
	}
	if int64(len(body)) > maxBody {
		return nil, &runtimepkg.ProviderError{Code: CodeProviderError, Message: "provider response exceeded the size bound", Retryable: false, ProviderStatus: response.StatusCode}
	}
	return &Response{Status: response.StatusCode, Header: response.Header, Body: body}, nil
}

// TransportError classifies a failure to complete an HTTP exchange.
func TransportError(ctx context.Context, err error) error {
	if errors.Is(err, context.DeadlineExceeded) || (ctx != nil && errors.Is(ctx.Err(), context.DeadlineExceeded)) {
		return &runtimepkg.ProviderError{Code: CodeRequestTimeout, Message: "the provider did not answer before the deadline", Retryable: true, Cause: err}
	}
	if errors.Is(err, context.Canceled) {
		return &runtimepkg.ProviderError{Code: CodeRequestTimeout, Message: "the request was canceled", Retryable: true, Cause: err}
	}
	return &runtimepkg.ProviderError{Code: CodeUnavailable, Message: "the provider could not be reached", Retryable: true, Cause: err}
}

// StatusError maps a non-2xx status onto the closed code set. The body is
// retained under the extension key for local diagnostics only; the message
// never quotes it, because a vendor error string is not caller-safe.
func StatusError(extension string, status int, body []byte) *runtimepkg.ProviderError {
	failure := &runtimepkg.ProviderError{ProviderStatus: status, Message: fmt.Sprintf("provider returned HTTP %d", status)}
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		failure.Code = CodeAuthenticationFailed
	case status == http.StatusTooManyRequests:
		failure.Code, failure.Retryable = CodeRateLimited, true
	case status == http.StatusRequestEntityTooLarge:
		failure.Code = CodeInputTooLarge
	case status == http.StatusUnsupportedMediaType:
		failure.Code = CodeUnsupportedMedia
	case status == http.StatusRequestTimeout || status == http.StatusGatewayTimeout:
		failure.Code, failure.Retryable = CodeRequestTimeout, true
	case status >= 500:
		failure.Code, failure.Retryable = CodeUnavailable, true
	case status == http.StatusBadRequest || status == http.StatusUnprocessableEntity || status == http.StatusNotFound || status == http.StatusConflict:
		failure.Code = CodeInvalidRequest
	default:
		failure.Code = CodeProviderError
	}
	if len(body) > 0 && extension != "" {
		raw := bytes.TrimSpace(body)
		if !json.Valid(raw) {
			quoted, _ := json.Marshal(string(raw))
			raw = quoted
		}
		failure.Extensions = map[string]json.RawMessage{extension: json.RawMessage(raw)}
	}
	return failure
}

// Malformed is the error for a 2xx body the adapter could not interpret.
func Malformed(cause error) *runtimepkg.ProviderError {
	return &runtimepkg.ProviderError{Code: CodeProviderError, Message: "provider returned a malformed transcription", Retryable: true, Cause: cause}
}

// Failed is the error for a job the provider itself reports as failed. It is
// not retryable by default: the same audio fails the same way.
func Failed(extension, detail string) *runtimepkg.ProviderError {
	failure := &runtimepkg.ProviderError{Code: CodeProviderError, Message: "the provider reported the transcription failed"}
	if detail != "" && extension != "" {
		quoted, _ := json.Marshal(detail)
		failure.Extensions = map[string]json.RawMessage{extension: json.RawMessage(quoted)}
	}
	return failure
}

// DecodeJSON unmarshals a body, mapping decode errors onto Malformed.
func DecodeJSON(body []byte, target any) error {
	if err := json.Unmarshal(body, target); err != nil {
		return Malformed(err)
	}
	return nil
}

// MultipartField is one non-file part.
type MultipartField struct{ Name, Value string }

// Multipart streams a multipart/form-data body: the fields first, then one
// file part read from audio as the HTTP client consumes the request, so a
// multi-gigabyte upload is never held in memory. The returned reader must be
// drained or closed by the HTTP client; the content type carries the boundary.
func Multipart(fields []MultipartField, fileField, fileName, fileContentType string, audio io.Reader) (io.ReadCloser, string) {
	reader, writer := io.Pipe()
	form := multipart.NewWriter(writer)
	go func() {
		err := func() error {
			for _, field := range fields {
				if err := form.WriteField(field.Name, field.Value); err != nil {
					return err
				}
			}
			header := make(textproto.MIMEHeader, 2)
			header.Set("Content-Disposition", fmt.Sprintf(`form-data; name="%s"; filename="%s"`, escapeQuotes(fileField), escapeQuotes(fileName)))
			header.Set("Content-Type", fileContentType)
			part, err := form.CreatePart(header)
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

func escapeQuotes(s string) string {
	return strings.NewReplacer("\\", "\\\\", `"`, "\\\"").Replace(s)
}

// Rewind seeks the audio back to its start for a second pass.
func Rewind(audio io.Seeker) error {
	if _, err := audio.Seek(0, io.SeekStart); err != nil {
		return &runtimepkg.ProviderError{Code: CodeProviderError, Message: "the audio could not be re-read", Cause: err}
	}
	return nil
}

// Poll calls check at the interval until it reports done, fails, or the
// context ends. A context deadline becomes request_timeout so a job the
// provider never finishes is classified like any other stall.
func Poll(ctx context.Context, interval time.Duration, check func(context.Context) (bool, error)) error {
	if interval <= 0 {
		interval = time.Second
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return TransportError(ctx, ctx.Err())
		case <-timer.C:
		}
		done, err := check(ctx)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		timer.Reset(interval)
	}
}

// Word is a provider word with timing, the unit most batch APIs return.
type Word struct {
	Text    string
	StartMS int64
	EndMS   int64
	Speaker string
}

// GroupWords folds word timings into segments for providers that return no
// coarser unit. A new segment starts on a speaker change or a silence longer
// than gapMS. Words are joined with single spaces; providers that carry their
// own spacing tokens should strip them before calling.
func GroupWords(words []Word, gapMS int64) []runtimepkg.BatchSegment {
	if gapMS <= 0 {
		gapMS = 800
	}
	var segments []runtimepkg.BatchSegment
	var current *runtimepkg.BatchSegment
	var parts []string
	flush := func() {
		if current != nil {
			current.Text = strings.Join(parts, " ")
			segments = append(segments, *current)
		}
		current, parts = nil, nil
	}
	for _, word := range words {
		text := strings.TrimSpace(word.Text)
		if text == "" {
			continue
		}
		if current != nil && (word.Speaker != current.Speaker || word.StartMS-current.EndMS > gapMS) {
			flush()
		}
		if current == nil {
			current = &runtimepkg.BatchSegment{StartMS: word.StartMS, EndMS: word.EndMS, Speaker: word.Speaker}
		}
		if word.EndMS > current.EndMS {
			current.EndMS = word.EndMS
		}
		parts = append(parts, text)
	}
	flush()
	return segments
}

// JoinSegments renders the full transcript from segments when the provider
// returns no whole-text field.
func JoinSegments(segments []runtimepkg.BatchSegment) string {
	parts := make([]string, 0, len(segments))
	for _, segment := range segments {
		if text := strings.TrimSpace(segment.Text); text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, " ")
}

// SecondsToMS converts a floating-point seconds value as vendors report it.
func SecondsToMS(seconds float64) int64 {
	if seconds <= 0 {
		return 0
	}
	return int64(seconds*1000 + 0.5)
}

// RawExtension wraps a body for BatchTranscription.Extensions when it is
// valid JSON; invalid bodies are dropped rather than quoted.
func RawExtension(key string, body []byte) map[string]json.RawMessage {
	if key == "" || !json.Valid(body) {
		return nil
	}
	return map[string]json.RawMessage{key: json.RawMessage(bytes.Clone(body))}
}
