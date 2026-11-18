package deepgram

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

// STTCapacityRefusedAt recognizes only a refusal observed by this adapter while
// opening a Deepgram STT socket: HTTP 429 and a complete, unambiguous native
// TOO_MANY_REQUESTS response. The timestamp is sampled when response headers
// arrive, before reading or closing the body. Generic ProviderErrors and caller
// hints cannot create this observation. Wrapped errors retain it.
//
// This proves provider I/O occurred. It supplies no usage, monetary zero,
// account/credential binding, retry authority or fallback authority.
func STTCapacityRefusedAt(err error) (time.Time, bool) {
	var refusal *sttCapacityRefusal
	if !errors.As(err, &refusal) || refusal == nil {
		return time.Time{}, false
	}
	return refusal.refusedAt, true
}

type sttCapacityRefusal struct {
	failure   *runtimepkg.ProviderError
	refusedAt time.Time
}

func (e *sttCapacityRefusal) Error() string { return e.failure.Error() }
func (e *sttCapacityRefusal) Unwrap() error { return e.failure }

// The WebSocket library reads at most 1024 error-body bytes and discards read
// errors. Observe EOF independently so neither a valid truncated prefix nor a
// read failure can be mistaken for a complete native refusal. Successful 101
// bodies are left untouched because they also implement io.ReadWriteCloser.
type sttHandshakeObserver struct {
	client     *http.Client
	transport  http.RoundTripper
	mu         sync.Mutex
	roundTrips int
	response   *http.Response
	body       *sttRefusalBody
	refusedAt  time.Time
}

func newSTTHandshakeObserver(client *http.Client) *sttHandshakeObserver {
	copy := *client
	observer := &sttHandshakeObserver{client: &copy, transport: client.Transport}
	if observer.transport == nil {
		observer.transport = http.DefaultTransport
	}
	copy.Transport = observer
	return observer
}

func (o *sttHandshakeObserver) RoundTrip(request *http.Request) (*http.Response, error) {
	o.mu.Lock()
	o.roundTrips++
	o.mu.Unlock()
	response, err := o.transport.RoundTrip(request)
	observedAt := time.Now().UTC()
	if err == nil && response != nil && response.StatusCode == http.StatusTooManyRequests && response.Body != nil {
		body := &sttRefusalBody{ReadCloser: response.Body}
		response.Body = body
		o.mu.Lock()
		o.response, o.body, o.refusedAt = response, body, observedAt
		o.mu.Unlock()
	}
	return response, err
}

func (o *sttHandshakeObserver) capacityRefusedAt(response *http.Response) (time.Time, bool) {
	o.mu.Lock()
	owned, body, at, trips := o.response, o.body, o.refusedAt, o.roundTrips
	o.mu.Unlock()
	// Redirects do not attest a refusal by the originally selected endpoint.
	if response == nil || owned != response || trips != 1 || body == nil || !body.complete() {
		return time.Time{}, false
	}
	// Dial replaced this with its bounded in-memory copy after closing the
	// original body. Do not retain provider messages or request identifiers.
	payload, err := io.ReadAll(io.LimitReader(response.Body, 1025))
	if err != nil || len(payload) > 1024 || !nativeSTTCapacityRefusal(payload) {
		return time.Time{}, false
	}
	return at, true
}

type sttRefusalBody struct {
	io.ReadCloser
	mu      sync.Mutex
	eof     bool
	readErr bool
}

func (b *sttRefusalBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.mu.Lock()
	if err == io.EOF {
		b.eof = true
	} else if err != nil {
		b.readErr = true
	}
	b.mu.Unlock()
	return n, err
}

func (b *sttRefusalBody) complete() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.eof && !b.readErr
}

func nativeSTTCapacityRefusal(payload []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return false
	}
	seen := make(map[string]bool)
	matched := false
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok || seen[key] {
			return false
		}
		seen[key] = true
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			return false
		}
		if key == "err_code" {
			var code string
			matched = json.Unmarshal(value, &code) == nil && code == "TOO_MANY_REQUESTS"
		}
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return false
	}
	var trailing json.RawMessage
	return matched && decoder.Decode(&trailing) == io.EOF
}
