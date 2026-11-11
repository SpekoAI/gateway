package cartesia

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

func TestPreparedBytesTTSFreezesFinalProviderBodyBeforeCredentialsOrIO(t *testing.T) {
	t.Parallel()
	const text = " A漢🙂\x00e\u0301\n<>& "
	var calls atomic.Int64
	bodies := make(chan bytesSpeechRequest, 1)
	rawBodies := make(chan string, 1)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		raw, err := io.ReadAll(r.Body)
		var body bytesSpeechRequest
		if err != nil || json.Unmarshal(raw, &body) != nil {
			t.Error("invalid prepared provider body")
		}
		bodies <- body
		rawBodies <- string(raw)
		if r.Header.Get("Authorization") != "Bearer synthetic-funded-credential" {
			t.Error("prepared input supplied a credential")
		}
		w.Header().Set("Content-Type", "audio/pcm")
		_, _ = w.Write([]byte{1, 2, 3, 4})
	}))
	defer server.Close()
	adapter, err := NewBytesTTS(BytesTTSConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatal(err)
	}
	request := bytesRequest()
	request.Plan.Route.Credential = nil
	request.Plan.Route.Endpoint = ""
	input, err := adapter.PrepareInput(request, text)
	if err != nil || input.Text() != text || calls.Load() != 0 {
		t.Fatal("preparation did not retain exact final text without credentials or I/O")
	}
	// Admission supplies endpoint/credentials, but cannot change prepared input.
	request.Plan.Route.Endpoint = server.URL + bytesTTSPath
	request.Plan.Route.Credential = &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "synthetic-funded-credential"}
	stream, err := adapter.OpenPrepared(context.Background(), request, input)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.(runtimepkg.AbortingProviderStream).Abort(context.Background())
	request.Options.Voice = "CHANGED_AFTER_OPEN"
	request.Options.TTS.ProviderOptions["cartesia"]["normalization"] = "auto"
	request.Media.SampleRateHz = 48000
	if err := stream.AppendText(context.Background(), "substituted"); !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
		t.Fatal("prepared operation accepted replacement text")
	}
	audio, done := bytesDrain(t, stream)
	if done.Err != nil || !bytes.Equal(audio, []byte{1, 2, 3, 4}) || done.Billing == nil || !done.Billing.Complete || done.Billing.Quantities["characters"] != int64(utf8.RuneCountInString(text))*1000 {
		t.Fatal("prepared operation lost its actual output/billing lifecycle")
	}
	actual := <-bodies
	if <-rawBodies != input.(preparedBytesTTSInput).payload || actual.Input != input.Text() || actual.Model != "sonic-3.6" || actual.Voice != "voice-id" || actual.Language != "en" || actual.Normalization != "off" || actual.OutputFormat.SampleRate != 24000 {
		t.Fatal("the actual provider body differs from the immutable prepared input")
	}
	for _, err := range []error{stream.AppendText(context.Background(), "another"), stream.CommitText(context.Background())} {
		if !errors.Is(err, runtimepkg.ErrUnsupportedOperation) {
			t.Fatal("completed prepared operation accepted another synthesis")
		}
	}
	if err := stream.Close(context.Background()); err != nil || calls.Load() != 1 {
		t.Fatal("prepared stream did not close after its one operation")
	}
}

func TestPreparedBytesTTSRejectsChangedFundedSettingsBeforeIO(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer server.Close()
	adapter, err := NewBytesTTS(BytesTTSConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*runtimepkg.AdapterRequest){
		"model":    func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "sonic-3" },
		"voice":    func(r *runtimepkg.AdapterRequest) { r.Options.Voice = "other-voice" },
		"language": func(r *runtimepkg.AdapterRequest) { r.Options.Language = "fr" },
		"media":    func(r *runtimepkg.AdapterRequest) { r.Media.SampleRateHz = 48000 },
		"normalization": func(r *runtimepkg.AdapterRequest) {
			r.Options.TTS = &protocol.TtsOptions{ProviderOptions: map[string]map[string]any{"cartesia": {"normalization": "auto"}}}
		},
		"lower bound": func(r *runtimepkg.AdapterRequest) { r.Options.MaxInputCharacters = 2 },
		"credential":  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential = nil },
		"endpoint":    func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = "https://untrusted.invalid/tts/bytes" },
	} {
		t.Run(name, func(t *testing.T) {
			request := bytesRequest()
			request.Options.TTS = nil
			request.Plan.Route.Endpoint = server.URL + bytesTTSPath
			input, err := adapter.PrepareInput(request, "original")
			if err != nil {
				t.Fatal(err)
			}
			mutate(&request)
			stream, err := adapter.OpenPrepared(context.Background(), request, input)
			if err == nil || stream != nil || calls.Load() != 0 {
				t.Fatal("changed funded request reached provider I/O")
			}
		})
	}
}

func TestPreparedBytesTTSWorkCancellationStopsActualHTTP(t *testing.T) {
	t.Parallel()
	entered, stopped := make(chan struct{}), make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(entered)
		<-r.Context().Done()
		close(stopped)
	}))
	defer server.Close()
	defer server.CloseClientConnections()
	adapter, err := NewBytesTTS(BytesTTSConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true, HeaderTimeout: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	request := bytesRequest()
	request.Plan.Route.Endpoint = server.URL + bytesTTSPath
	input, err := adapter.PrepareInput(request, "original")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := adapter.OpenPrepared(ctx, request, input)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.(runtimepkg.AbortingProviderStream).Abort(context.Background())
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("prepared request did not start")
	}
	initial := bytesEvent(t, stream)
	if initial.Billing == nil || initial.Billing.Complete || len(initial.Billing.Quantities) != 0 {
		t.Fatal("dispatched prepared request did not retain unknown exposure")
	}
	cancel()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("work cancellation did not stop the actual provider request")
	}
}

func TestPreparedBytesTTSRejectsInvalidInputAndUnavailablePreparation(t *testing.T) {
	t.Parallel()
	adapter, err := NewBytesTTS(BytesTTSConfig{MaxInputBytes: 16})
	if err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"", " \t\n", "\xff", strings.Repeat("a", 17)} {
		if input, err := adapter.PrepareInput(bytesRequest(), text); err == nil || input != nil {
			t.Fatal("invalid input was prepared")
		}
	}
	request := bytesRequest()
	request.Options.MaxInputCharacters = 1
	if input, err := adapter.PrepareInput(request, "漢🙂"); err == nil || input != nil {
		t.Fatal("the character bound was bypassed")
	}
	request = bytesRequest()
	input, err := adapter.PrepareInput(request, "original")
	if err != nil {
		t.Fatal(err)
	}
	for _, unavailable := range []PreparedBytesTTSInput{nil, preparedBytesTTSInput{}} {
		if stream, err := adapter.OpenPrepared(context.Background(), request, unavailable); err == nil || stream != nil {
			t.Fatal("missing preparation fell back to ordinary input")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if stream, err := adapter.OpenPrepared(ctx, request, input); !errors.Is(err, context.Canceled) || stream != nil {
		t.Fatal("cancelled prepared request was opened")
	}
}
