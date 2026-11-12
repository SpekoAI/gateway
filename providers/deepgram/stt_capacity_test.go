package deepgram

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

func TestSTTCapacityRefusalRequiresCompleteNativeHandshakeResponse(t *testing.T) {
	t.Parallel()
	const native = `{"err_code":"TOO_MANY_REQUESTS"}`
	for _, test := range []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{"native", 429, native, true},
		{"documented fields", 429, `{"err_code":"TOO_MANY_REQUESTS","err_msg":"secret-message","request_id":"secret-id"}`, true},
		{"bounded whitespace", 429, native + strings.Repeat(" ", 900), true},
		{"generic status", 429, `{}`, false},
		{"message hint", 429, `{"err_msg":"TOO_MANY_REQUESTS"}`, false},
		{"tts native family", 429, `{"err_code":"Too Many Requests"}`, false},
		{"quota", 429, `{"err_code":"QUOTA_EXCEEDED"}`, false},
		{"payment", 402, native, false},
		{"authentication", 401, native, false},
		{"unavailable", 503, native, false},
		{"non-upgrade success", 200, native, false},
		{"code case", 429, `{"err_code":"too_many_requests"}`, false},
		{"field case", 429, `{"ERR_CODE":"TOO_MANY_REQUESTS"}`, false},
		{"code type", 429, `{"err_code":["TOO_MANY_REQUESTS"]}`, false},
		{"null code", 429, `{"err_code":null}`, false},
		{"duplicate code", 429, `{"err_code":"QUOTA_EXCEEDED","err_code":"TOO_MANY_REQUESTS"}`, false},
		{"identical duplicate code", 429, `{"err_code":"TOO_MANY_REQUESTS","err_code":"TOO_MANY_REQUESTS"}`, false},
		{"escaped duplicate code", 429, `{"err_code":"TOO_MANY_REQUESTS","err_\u0063ode":"TOO_MANY_REQUESTS"}`, false},
		{"duplicate other field", 429, `{"err_code":"TOO_MANY_REQUESTS","x":1,"x":2}`, false},
		{"array envelope", 429, `[` + native + `]`, false},
		{"malformed", 429, `{"err_code":"TOO_MANY_REQUESTS"`, false},
		{"trailing json", 429, native + `{}`, false},
		{"trailing garbage", 429, native + `x`, false},
		{"valid truncated prefix", 429, native + strings.Repeat(" ", 2048) + `{}`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/listen" {
					t.Errorf("path = %q", r.URL.Path)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(test.status)
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			config := testConfig(server.URL)
			config.HTTPClient = server.Client()
			adapter, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			before := time.Now()
			stream, err := adapter.Open(t.Context(), sttCapacityRequest(server.URL, protocol.RequestOptions{}))
			if stream != nil || err == nil {
				t.Fatalf("open = (%v, %v)", stream, err)
			}
			at, ok := STTCapacityRefusedAt(fmt.Errorf("caller wrapper: %w", err))
			if ok != test.want || (!ok && !at.IsZero()) || (ok && (at.Before(before) || at.After(time.Now()) || at.Location() != time.UTC)) {
				t.Fatalf("observation = (%v, %t), want recognized=%t", at, ok, test.want)
			}
			var failure *runtimepkg.ProviderError
			if !errors.As(err, &failure) || failure.ProviderStatus != test.status || failure.Code != dialErrorCode(test.status) || failure.Retryable != (test.status == 429 || test.status >= 500) {
				t.Fatalf("provider failure = %#v, err=%v", failure, err)
			}
			if len(failure.Extensions) != 0 || strings.Contains(fmt.Sprintf("%+v", failure), "secret-") {
				t.Fatal("native provider content escaped into the observation/error")
			}
		})
	}
}

func TestSTTCapacityRefusalTimestampPrecedesBodyCleanup(t *testing.T) {
	t.Parallel()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"err_code":"TOO_MANY_REQUESTS"}`)
	}))
	defer server.Close()
	client := server.Client()
	base := client.Transport
	closing, release := make(chan time.Time, 1), make(chan struct{})
	var released sync.Once
	defer released.Do(func() { close(release) })
	client.Transport = sttCapacityRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		response, err := base.RoundTrip(request)
		if err == nil {
			response.Body = &sttCapacityCloseGate{ReadCloser: response.Body, closing: closing, release: release}
		}
		return response, err
	})
	config := testConfig(server.URL)
	config.HTTPClient = client
	adapter, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	before := time.Now()
	go func() {
		_, err := adapter.Open(ctx, sttCapacityRequest(server.URL, protocol.RequestOptions{}))
		result <- err
	}()
	var closeStarted time.Time
	select {
	case closeStarted = <-closing:
	case <-ctx.Done():
		t.Fatal("body cleanup did not start")
	}
	released.Do(func() { close(release) })
	select {
	case err := <-result:
		at, ok := STTCapacityRefusedAt(err)
		if !ok || at.Before(before) || !at.Before(closeStarted) {
			t.Fatalf("timestamp %v is not between open %v and cleanup %v: %v", at, before, closeStarted, err)
		}
		if again, ok := STTCapacityRefusedAt(fmt.Errorf("cleanup wrapper: %w", err)); !ok || !again.Equal(at) {
			t.Fatal("error wrapping changed the original refusal timestamp")
		}
	case <-ctx.Done():
		t.Fatal("open did not return after cleanup")
	}
}

func TestSTTCapacityRefusalRejectsTruncatedHTTPBodyAndRedirect(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"truncated", "redirect"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mode == "redirect" && r.URL.Path == "/v1/listen" {
					http.Redirect(w, r, "/different-origin-of-evidence", http.StatusFound)
					return
				}
				if mode == "truncated" {
					w.Header().Set("Content-Length", "1000")
				}
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = io.WriteString(w, `{"err_code":"TOO_MANY_REQUESTS"}`)
			}))
			defer server.Close()
			config := testConfig(server.URL)
			config.HTTPClient = server.Client()
			adapter, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			_, err = adapter.Open(t.Context(), sttCapacityRequest(server.URL, protocol.RequestOptions{}))
			if err == nil {
				t.Fatal("expected failed handshake")
			}
			if _, ok := STTCapacityRefusedAt(err); ok {
				t.Fatal("incomplete/redirected response manufactured capacity evidence")
			}
		})
	}
}

func TestSTTCapacityRefusalDoesNotMutateOrShareHTTPClientObservations(t *testing.T) {
	t.Parallel()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		if r.URL.Query().Get("language") == "en" {
			_, _ = io.WriteString(w, `{"err_code":"TOO_MANY_REQUESTS"}`)
		} else {
			_, _ = io.WriteString(w, `{"err_code":"QUOTA_EXCEEDED"}`)
		}
	}))
	defer server.Close()
	client := server.Client()
	transport := client.Transport
	config := testConfig(server.URL)
	config.HTTPClient = client
	adapter, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for i := range 16 {
		group.Go(func() {
			language := "de"
			if i%2 == 0 {
				language = "en"
			}
			_, err := adapter.Open(t.Context(), sttCapacityRequest(server.URL, protocol.RequestOptions{Language: language}))
			if _, ok := STTCapacityRefusedAt(err); ok != (language == "en") {
				t.Errorf("language %s received another open's observation: %v", language, err)
			}
		})
	}
	group.Wait()
	if client.Transport != transport {
		t.Fatal("adapter mutated the shared HTTP client")
	}
}

func TestSTTCapacityRefusalRejectsGenericAndLocalFailures(t *testing.T) {
	t.Parallel()
	for _, err := range []error{nil, errors.New("TOO_MANY_REQUESTS"), &runtimepkg.ProviderError{Code: "provider_rate_limited", ProviderStatus: 429, Retryable: true}} {
		if _, ok := STTCapacityRefusedAt(err); ok {
			t.Fatal("unowned error manufactured capacity evidence")
		}
	}
	config := testConfig("https://capacity.test")
	calls := 0
	config.HTTPClient = &http.Client{Transport: sttCapacityRoundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return nil, errors.New("transport unavailable")
	})}
	adapter, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	request := sttCapacityRequest("https://capacity.test", protocol.RequestOptions{})
	_, err = adapter.Open(t.Context(), request)
	if _, ok := STTCapacityRefusedAt(err); ok || calls != 1 {
		t.Fatalf("transport failure = %v, calls=%d", err, calls)
	}
	request.Plan.Route.Credential.Value = ""
	_, err = adapter.Open(t.Context(), request)
	if _, ok := STTCapacityRefusedAt(err); ok || calls != 1 {
		t.Fatalf("local validation = %v, calls=%d", err, calls)
	}
}

type sttCapacityRoundTripFunc func(*http.Request) (*http.Response, error)

func (f sttCapacityRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type sttCapacityCloseGate struct {
	io.ReadCloser
	closing chan<- time.Time
	release <-chan struct{}
	once    sync.Once
}

func (b *sttCapacityCloseGate) Close() error {
	b.once.Do(func() { b.closing <- time.Now() })
	<-b.release
	return b.ReadCloser.Close()
}

func sttCapacityRequest(serverURL string, options protocol.RequestOptions) runtimepkg.AdapterRequest {
	request := adapterRequest(serverURL, options)
	if strings.HasPrefix(serverURL, "https://") {
		request.Plan.Route.Endpoint = strings.Replace(request.Plan.Route.Endpoint, "ws://", "wss://", 1)
	}
	return request
}
