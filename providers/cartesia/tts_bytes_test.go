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

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

func bytesRequest() runtimepkg.AdapterRequest {
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindTTS,
		Plan: protocol.SessionPlan{Execution: protocol.Execution{ProviderRoute: protocol.RouteSpekoRelay, CredentialSource: protocol.CredentialsManaged},
			Route: protocol.PlanRoute{Provider: "cartesia", Model: "sonic-3.6", Adapter: BytesTTSAdapterID, Transport: protocol.TransportHTTP, Endpoint: BytesTTSEndpoint, Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "sk_car_synthetic"}}},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24000, Channels: 1},
		Options: protocol.RequestOptions{Voice: "voice-id", Language: "en", TTS: &protocol.TtsOptions{ProviderOptions: map[string]map[string]any{"cartesia": {"normalization": "off"}}}},
	}
}

func openBytes(t *testing.T, handler http.HandlerFunc, config BytesTTSConfig, request runtimepkg.AdapterRequest) runtimepkg.ProviderStream {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	config.HTTPClient = server.Client()
	config.AllowedEndpointHosts = []string{"127.0.0.1"}
	config.AllowInsecureEndpoint = true
	if config.AudioChunkBytes == 0 {
		config.AudioChunkBytes = 3
	}
	adapter, err := NewBytesTTS(config)
	if err != nil {
		t.Fatal(err)
	}
	request.Plan.Route.Endpoint = server.URL + bytesTTSPath
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) })
	return stream
}

func bytesEvent(t *testing.T, stream runtimepkg.ProviderStream) runtimepkg.ProviderEvent {
	t.Helper()
	select {
	case event, ok := <-stream.Events():
		if !ok {
			t.Fatal("events closed before terminal evidence")
		}
		return event
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for bytes TTS event")
		return runtimepkg.ProviderEvent{}
	}
}

func bytesDrain(t *testing.T, stream runtimepkg.ProviderStream) ([]byte, runtimepkg.ProviderEvent) {
	t.Helper()
	var audio []byte
	for {
		event := bytesEvent(t, stream)
		audio = append(audio, event.Audio...)
		if len(event.Audio)%2 != 0 {
			t.Fatal("audio frame splits a PCM sample")
		}
		if event.Err != nil || event.Type == protocol.EventAudioDone {
			return audio, event
		}
	}
}

func TestBytesTTSUsesPinnedHTTPShapeAndExactUnicodeInputFactsAcrossUtterances(t *testing.T) {
	t.Parallel()
	pcm := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	bodies := make(chan map[string]any, 2)
	stream := openBytes(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != bytesTTSPath || r.URL.RawQuery != "" || r.Header.Get("Authorization") != "Bearer sk_car_synthetic" || r.Header.Get("Cartesia-Version") != BytesTTSAPIVersion || r.Header.Get("Accept-Encoding") != "identity" {
			t.Error("incorrect pinned HTTP request")
		}
		var body map[string]any
		if json.NewDecoder(r.Body).Decode(&body) != nil {
			t.Error("invalid provider JSON")
		}
		bodies <- body
		w.Header().Set("Content-Type", "audio/pcm")
		_, _ = w.Write(pcm)
	}, BytesTTSConfig{}, bytesRequest())
	operation := ""
	for range 2 {
		if err := stream.AppendText(context.Background(), " A漢"); err != nil {
			t.Fatal(err)
		}
		if err := stream.AppendText(context.Background(), "🙂\n"); err != nil {
			t.Fatal(err)
		}
		if err := stream.CommitText(context.Background()); err != nil {
			t.Fatal(err)
		}
		body := <-bodies
		if len(body) != 6 || body["transcript"] != " A漢🙂\n" || body["model_id"] != "sonic-3.6" || body["voice"] != "voice-id" || body["language"] != "en" || body["normalization"] != "off" {
			t.Fatal("generation fields or exact input changed")
		}
		format := body["output_format"].(map[string]any)
		if len(format) != 3 || format["container"] != "raw" || format["encoding"] != "pcm_s16le" || format["sample_rate"] != float64(24000) {
			t.Fatal("raw PCM configuration changed")
		}
		pending := bytesEvent(t, stream)
		if pending.Type != protocol.EventUsageObserved || pending.Billing == nil || pending.Billing.Complete || len(pending.Billing.Quantities) != 0 {
			t.Fatal("dispatched operation lacks initial unknown-quantity evidence")
		}
		started := bytesEvent(t, stream)
		if started.Type != protocol.EventAudioStarted || started.Billing == nil || started.Billing.Complete || started.Billing.Quantities["characters"] != 5000 {
			t.Fatal("accepted input facts missing before audio")
		}
		audio, done := bytesDrain(t, stream)
		if done.Err != nil || !bytes.Equal(audio, pcm) || done.Billing == nil || !done.Billing.Complete || done.Billing.Model != "sonic-3.6" || done.Billing.Mode != "streaming" || len(done.Billing.Quantities) != 1 || done.Billing.Quantities["characters"] != 5000 || done.Billing.ProviderRequestID != "" {
			t.Fatal("actual complete input/audio facts were misclassified")
		}
		if err := done.Billing.Validate(); err != nil {
			t.Fatal(err)
		}
		if merged, err := protocol.MergeBillingObservation(*started.Billing, *done.Billing); err != nil || !merged.Complete {
			t.Fatal("request observations did not merge")
		}
		if operation == done.Billing.OperationID {
			t.Fatal("separate POSTs reused an operation ID")
		}
		operation = done.Billing.OperationID
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestBytesTTSRejectsUnpinnedOrIncompatibleInputsBeforeIO(t *testing.T) {
	t.Parallel()
	adapter, err := NewBytesTTS(BytesTTSConfig{})
	if err != nil {
		t.Fatal(err)
	}
	for _, model := range []string{"sonic-3", "sonic-3.5", "sonic-3.6"} {
		request := bytesRequest()
		request.Plan.Route.Model = model
		request.Options.TTS = nil
		stream, err := adapter.Open(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		_ = stream.Close(context.Background())
	}
	for name, mutate := range map[string]func(*runtimepkg.AdapterRequest){
		"model":     func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "sonic-latest" },
		"provider":  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Provider = "other" },
		"path":      func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = "https://api.cartesia.ai/tts/sse" },
		"host":      func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = "https://other.invalid/tts/bytes" },
		"query":     func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint += "?access_token=private" },
		"transport": func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Transport = protocol.TransportWebSocket },
		"stereo":    func(r *runtimepkg.AdapterRequest) { r.Media.Channels = 2 },
		"format":    func(r *runtimepkg.AdapterRequest) { r.Media.Encoding = "pcm_f32le" },
		"rate":      func(r *runtimepkg.AdapterRequest) { r.Media.SampleRateHz = 12345 },
		"voice":     func(r *runtimepkg.AdapterRequest) { r.Options.Voice = "" },
		"expired": func(r *runtimepkg.AdapterRequest) {
			r.Plan.Execution.ProviderRoute = protocol.RouteProviderDirect
			r.Plan.Route.Credential.ExpiresAt = time.Now().Add(-time.Second)
		},
		"normalization": func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "sonic-3.5" },
	} {
		t.Run(name, func(t *testing.T) {
			request := bytesRequest()
			mutate(&request)
			if _, err := adapter.Open(context.Background(), request); err == nil {
				t.Fatal("incompatible bytes request accepted")
			}
		})
	}
}

func TestBytesTTSBoundsWholeInputWithoutMutatingAcceptedFragments(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	request := bytesRequest()
	request.Options.MaxInputCharacters = 5
	stream := openBytes(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var body bytesSpeechRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.Input != " A漢🙂\n" {
			t.Error("refused append changed the input")
		}
		w.Header().Set("Content-Type", "audio/pcm")
		_, _ = w.Write([]byte{1, 2})
	}, BytesTTSConfig{MaxInputBytes: 10}, request)
	for _, invalid := range []string{"", string([]byte{0xff}), strings.Repeat("界", 4)} {
		if stream.AppendText(context.Background(), invalid) == nil {
			t.Fatal("unbounded or malformed input accepted")
		}
	}
	if err := stream.AppendText(context.Background(), " A漢🙂\n"); err != nil {
		t.Fatal(err)
	}
	if stream.AppendText(context.Background(), "x") == nil {
		t.Fatal("character/input bound bypassed")
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, done := bytesDrain(t, stream); done.Err != nil {
		t.Fatal(done.Err)
	}
	if calls.Load() != 1 {
		t.Fatal("refused input made provider I/O")
	}
}

func TestBytesTTSBrokenOrUnboundedSuccessNeverPublishesCompleteEvidence(t *testing.T) {
	t.Parallel()
	for name, handler := range map[string]http.HandlerFunc{
		"empty": func(w http.ResponseWriter, _ *http.Request) { w.Header().Set("Content-Type", "audio/pcm") },
		"torn": func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "audio/pcm")
			w.Header().Set("Content-Length", "12")
			_, _ = w.Write([]byte{1, 2, 3, 4})
		},
		"mid-sample": func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "audio/pcm")
			_, _ = w.Write([]byte{1, 2, 3})
		},
		"limit": func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "audio/pcm")
			_, _ = w.Write(make([]byte, 10))
		},
	} {
		t.Run(name, func(t *testing.T) {
			stream := openBytes(t, handler, BytesTTSConfig{MaxResponseBytes: 8}, bytesRequest())
			_ = stream.AppendText(context.Background(), "private synthetic input")
			if err := stream.CommitText(context.Background()); err != nil {
				t.Fatal(err)
			}
			audio, terminal := bytesDrain(t, stream)
			if len(audio) > 8 || terminal.Err == nil || terminal.Billing == nil || terminal.Billing.Complete || terminal.Type == protocol.EventAudioDone || strings.Contains(terminal.Err.Error(), "private synthetic input") {
				t.Fatal("failed response became complete or echoed input")
			}
		})
	}
}

func TestBytesTTSErrorsAndRedirectsAreBoundedAndContentFree(t *testing.T) {
	t.Parallel()
	var leaked atomic.Bool
	foreign := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { leaked.Store(true) }))
	defer foreign.Close()
	for _, status := range []int{206, 400, 401, 403, 429, 500, 307} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			stream := openBytes(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Location", foreign.URL)
				w.WriteHeader(status)
				_, _ = io.WriteString(w, strings.Repeat("private provider echo", 10000))
			}, BytesTTSConfig{MaxErrorBytes: 64}, bytesRequest())
			_ = stream.AppendText(context.Background(), "private input")
			if err := stream.CommitText(context.Background()); err != nil {
				t.Fatal(err)
			}
			_, terminal := bytesDrain(t, stream)
			err := terminal.Err
			var failure *runtimepkg.ProviderError
			if !errors.As(err, &failure) || failure.ProviderStatus != status || strings.Contains(err.Error(), "private") || failure.Retryable != (status == 429 || status >= 500) {
				t.Fatal("provider failure exposed content or changed classification")
			}
		})
	}
	if leaked.Load() {
		t.Fatal("credential followed a redirect")
	}
	for _, headers := range []map[string]string{{"Content-Type": "application/json"}, {"Content-Type": "audio/pcm", "Content-Encoding": "gzip"}} {
		stream := openBytes(t, func(w http.ResponseWriter, _ *http.Request) {
			for k, v := range headers {
				w.Header().Set(k, v)
			}
			_, _ = io.WriteString(w, "private provider echo")
		}, BytesTTSConfig{}, bytesRequest())
		_ = stream.AppendText(context.Background(), "private input")
		if err := stream.CommitText(context.Background()); err != nil {
			t.Fatal(err)
		}
		_, terminal := bytesDrain(t, stream)
		if err := terminal.Err; err == nil || strings.Contains(err.Error(), "private") {
			t.Fatal("unsupported response accepted or echoed")
		}
	}
}

func TestBytesTTSAbortOwnsHeaderRequestAndCloseCannotRaceAReaderAdd(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{})
	stream := openBytes(t, func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(entered)
		<-r.Context().Done()
	}, BytesTTSConfig{}, bytesRequest())
	_ = stream.AppendText(context.Background(), "synthetic request")
	committed := make(chan error, 1)
	go func() { committed <- stream.CommitText(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("header request did not start")
	}
	stopped := make(chan struct{})
	go func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("abort deadlocked with active header request")
	}
	if err := <-committed; err != nil {
		t.Fatal("commit blocked or failed before scheduling the cancelable request")
	}
	for event := range stream.Events() {
		if event.Type != protocol.EventUsageObserved || event.Billing == nil || event.Billing.Complete || len(event.Billing.Quantities) != 0 {
			t.Fatal("aborted header request lost its pending operation or claimed completion")
		}
	}
}

func TestBytesTTSHeaderDeadlineAndCancelBoundIO(t *testing.T) {
	t.Parallel()
	t.Run("header-timeout", func(t *testing.T) {
		stream := openBytes(t, func(_ http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			<-r.Context().Done()
		}, BytesTTSConfig{HeaderTimeout: 20 * time.Millisecond}, bytesRequest())
		_ = stream.AppendText(context.Background(), "synthetic request")
		if err := stream.CommitText(context.Background()); err != nil {
			t.Fatal(err)
		}
		_, terminal := bytesDrain(t, stream)
		err := terminal.Err
		var failure *runtimepkg.ProviderError
		if !errors.As(err, &failure) || failure.Code != "request_timeout" {
			t.Fatal("stalled response headers had no bounded failure")
		}
	})
	t.Run("cancel-body", func(t *testing.T) {
		stream := openBytes(t, func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("Content-Type", "audio/pcm")
			_, _ = w.Write([]byte{1, 2})
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		}, BytesTTSConfig{}, bytesRequest())
		_ = stream.AppendText(context.Background(), "synthetic request")
		if err := stream.CommitText(context.Background()); err != nil {
			t.Fatal(err)
		}
		if pending := bytesEvent(t, stream); pending.Type != protocol.EventUsageObserved {
			t.Fatal("missing dispatched-operation snapshot")
		}
		if first := bytesEvent(t, stream); first.Type != protocol.EventAudioStarted {
			t.Fatal("audio did not start")
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := stream.Cancel(ctx); err != nil {
			t.Fatal(err)
		}
		if err := stream.Close(ctx); err != nil {
			t.Fatal(err)
		}
		for event := range stream.Events() {
			if event.Type == protocol.EventAudioDone || event.Billing != nil && event.Billing.Complete {
				t.Fatal("cancelled request became complete")
			}
		}
	})
}

func TestBytesTTSGracefulCloseCannotHangOnAnAbandonedEventConsumer(t *testing.T) {
	t.Parallel()
	stream := openBytes(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "audio/pcm")
		_, _ = w.Write(make([]byte, 32))
	}, BytesTTSConfig{EventBuffer: 1, GracefulCloseIdleTimeout: 20 * time.Millisecond}, bytesRequest())
	_ = stream.AppendText(context.Background(), "synthetic request")
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var failure *runtimepkg.ProviderError
	if err := stream.Close(ctx); !errors.As(err, &failure) || failure.Code != "provider_unavailable" {
		t.Fatal("abandoned consumer did not end at the configured idle bound")
	}
	for event := range stream.Events() {
		if event.Type == protocol.EventAudioDone || event.Billing != nil && event.Billing.Complete {
			t.Fatal("abandoned output became complete")
		}
	}
}
