package batchhttp

import (
	"context"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

func TestCredential(t *testing.T) {
	t.Parallel()
	plan := protocol.SessionPlan{Route: protocol.PlanRoute{Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: " key "}}}
	if got, err := Credential(plan); err != nil || got != "key" {
		t.Fatalf("bearer: %q %v", got, err)
	}
	plan.Route.Credential.Kind = protocol.CredentialRelayAccess
	if _, err := Credential(plan); err == nil {
		t.Fatal("relay_access outside the relay route accepted")
	}
	plan.Execution.ProviderRoute = protocol.RouteSpekoRelay
	if got, err := Credential(plan); err != nil || got != "key" {
		t.Fatalf("relay_access on relay route: %q %v", got, err)
	}
	plan.Route.Credential.Value = "  "
	if _, err := Credential(plan); err == nil {
		t.Fatal("empty credential accepted")
	}
	plan.Route.Credential = nil
	if _, err := Credential(plan); err == nil {
		t.Fatal("missing credential accepted")
	}
}

func TestStatusErrorMapping(t *testing.T) {
	t.Parallel()
	cases := map[int]struct {
		code      string
		retryable bool
	}{
		401: {CodeAuthenticationFailed, false},
		403: {CodeAuthenticationFailed, false},
		429: {CodeRateLimited, true},
		413: {CodeInputTooLarge, false},
		415: {CodeUnsupportedMedia, false},
		408: {CodeRequestTimeout, true},
		504: {CodeRequestTimeout, true},
		500: {CodeUnavailable, true},
		503: {CodeUnavailable, true},
		400: {CodeInvalidRequest, false},
		422: {CodeInvalidRequest, false},
		404: {CodeInvalidRequest, false},
		418: {CodeProviderError, false},
	}
	for status, want := range cases {
		got := StatusError("x", status, []byte(`{"error":"boom"}`))
		if got.Code != want.code || got.Retryable != want.retryable || got.ProviderStatus != status {
			t.Fatalf("%d: got %s/%v, want %s/%v", status, got.Code, got.Retryable, want.code, want.retryable)
		}
		if strings.Contains(got.Message, "boom") {
			t.Fatalf("%d: message quotes the vendor body", status)
		}
		if string(got.Extensions["x"]) != `{"error":"boom"}` {
			t.Fatalf("%d: extension = %s", status, got.Extensions["x"])
		}
	}
	plain := StatusError("x", 500, []byte("not json"))
	if string(plain.Extensions["x"]) != `"not json"` {
		t.Fatalf("non-JSON body should be quoted, got %s", plain.Extensions["x"])
	}
}

func TestDoBoundsBodyAndClassifiesTransport(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/big" {
			_, _ = w.Write(make([]byte, 2048))
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	request, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL+"/small", nil)
	response, err := Do(server.Client(), request, 1024)
	if err != nil || response.Status != http.StatusAccepted || string(response.Body) != `{"ok":true}` {
		t.Fatalf("small: %+v %v", response, err)
	}

	request, _ = http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL+"/big", nil)
	_, err = Do(server.Client(), request, 1024)
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != CodeProviderError {
		t.Fatalf("oversized body: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	request, _ = http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/small", nil)
	_, err = Do(server.Client(), request, 1024)
	if !errors.As(err, &providerErr) || providerErr.Code != CodeRequestTimeout || !providerErr.Retryable {
		t.Fatalf("deadline: %v", err)
	}

	server.Close()
	request, _ = http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL+"/small", nil)
	_, err = Do(server.Client(), request, 1024)
	if !errors.As(err, &providerErr) || providerErr.Code != CodeUnavailable || !providerErr.Retryable {
		t.Fatalf("refused dial: %v", err)
	}
}

func TestMultipartStreamsFieldsThenFile(t *testing.T) {
	t.Parallel()
	audio := strings.NewReader("RIFF....WAVEdata")
	body, contentType := Multipart([]MultipartField{{"model", "nova-3"}, {"language", "en"}}, "file", "audio.wav", "audio/wav", audio)
	defer body.Close()
	_, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		t.Fatalf("content type: %v", err)
	}
	reader := multipart.NewReader(body, params["boundary"])
	var names []string
	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("part: %v", err)
		}
		content, _ := io.ReadAll(part)
		names = append(names, part.FormName()+"="+string(content))
		if part.FormName() == "file" && (part.FileName() != "audio.wav" || part.Header.Get("Content-Type") != "audio/wav") {
			t.Fatalf("file part header: %v", part.Header)
		}
	}
	if strings.Join(names, "|") != "model=nova-3|language=en|file=RIFF....WAVEdata" {
		t.Fatalf("parts = %v", names)
	}
}

func TestPollStopsOnDoneErrorAndDeadline(t *testing.T) {
	t.Parallel()
	calls := 0
	err := Poll(context.Background(), time.Millisecond, func(context.Context) (bool, error) {
		calls++
		return calls == 3, nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("done: calls=%d err=%v", calls, err)
	}
	boom := errors.New("boom")
	if err := Poll(context.Background(), time.Millisecond, func(context.Context) (bool, error) { return false, boom }); !errors.Is(err, boom) {
		t.Fatalf("error: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = Poll(ctx, time.Millisecond, func(context.Context) (bool, error) { return false, nil })
	var providerErr *runtimepkg.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Code != CodeRequestTimeout {
		t.Fatalf("deadline: %v", err)
	}
}

func TestGroupWords(t *testing.T) {
	t.Parallel()
	words := []Word{
		{"Hello", 0, 400, "A"}, {"there", 450, 800, "A"},
		{"Hi", 900, 1100, "B"},
		{"", 1100, 1100, "B"},
		{"Later", 5000, 5400, "B"},
	}
	got := GroupWords(words, 800)
	want := []runtimepkg.BatchSegment{
		{Text: "Hello there", StartMS: 0, EndMS: 800, Speaker: "A"},
		{Text: "Hi", StartMS: 900, EndMS: 1100, Speaker: "B"},
		{Text: "Later", StartMS: 5000, EndMS: 5400, Speaker: "B"},
	}
	if len(got) != len(want) {
		t.Fatalf("segments = %+v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("segment %d = %+v, want %+v", i, got[i], want[i])
		}
	}
	if JoinSegments(got) != "Hello there Hi Later" {
		t.Fatalf("join = %q", JoinSegments(got))
	}
	if (&runtimepkg.BatchTranscription{Segments: got}).LastTimedMS() != 5400 {
		t.Fatal("last timed")
	}
	if SecondsToMS(1.5004) != 1500 || SecondsToMS(-1) != 0 {
		t.Fatal("seconds to ms")
	}
}
