package hamsa

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

// Every vendor string in this file is transcribed as a literal rather than
// referenced from the adapter's constants, so a misspelled constant cannot
// vouch for itself.

func TestSTTAdapterWholeUtteranceRoundTrip(t *testing.T) {
	t.Parallel()
	requests := make(chan *http.Request, 1)
	server := newHamsaServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		requests <- request.Clone(request.Context())

		// The vendor greets with an info control frame before any request is
		// answered; the adapter must not mistake it for a transcript.
		if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"info"}`)); err != nil {
			t.Errorf("write info: %v", err)
			return
		}

		messageType, payload, err := conn.Read(ctx)
		if err != nil || messageType != websocket.MessageText {
			t.Errorf("request = (%v, %v), want one text frame", messageType, err)
			return
		}
		var request2 struct {
			Type    string `json:"type"`
			Payload struct {
				AudioBase64  string `json:"audioBase64"`
				Language     string `json:"language"`
				Model        string `json:"model"`
				IsEosEnabled *bool  `json:"isEosEnabled"`
			} `json:"payload"`
		}
		if err := json.Unmarshal(payload, &request2); err != nil {
			t.Errorf("request json: %v", err)
			return
		}
		// isEosEnabled must be an explicit false: the socket's own default is
		// true, and with it on an utterance Hamsa judges unfinished is answered
		// with "[THINKING]" and nothing else.
		if request2.Type != "stt" || request2.Payload.Model != "s3" || request2.Payload.Language != "ar" || request2.Payload.IsEosEnabled == nil || *request2.Payload.IsEosEnabled {
			t.Errorf("request = %+v, want type stt, model s3, language ar, isEosEnabled false", request2)
			return
		}
		wav, err := base64.StdEncoding.DecodeString(request2.Payload.AudioBase64)
		if err != nil {
			t.Errorf("audioBase64: %v", err)
			return
		}
		// The socket refuses raw PCM: the payload must be a COMPLETE 16 kHz
		// mono PCM16 WAV whose data section is exactly the buffered turn.
		if len(wav) != 44+4 || string(wav[0:4]) != "RIFF" || string(wav[8:12]) != "WAVE" {
			t.Errorf("wav header = %q (%d bytes), want RIFF/WAVE with 44-byte header", wav[:12], len(wav))
			return
		}
		if binary.LittleEndian.Uint32(wav[24:28]) != 16000 || binary.LittleEndian.Uint16(wav[22:24]) != 1 {
			t.Error("wav must declare 16000 Hz mono")
			return
		}
		if string(wav[44:]) != "\x01\x02\x03\x04" {
			t.Errorf("wav data = %q, want the buffered PCM verbatim", wav[44:])
			return
		}

		// A bracketed status marker precedes the transcript while the service
		// works; it must never surface as the result.
		if err := conn.Write(ctx, websocket.MessageText, []byte("[THINKING]")); err != nil {
			t.Errorf("write status marker: %v", err)
			return
		}
		// The transcript is a PLAIN string frame, not JSON.
		if err := conn.Write(ctx, websocket.MessageText, []byte("مرحبا بكم")); err != nil {
			t.Errorf("write transcript: %v", err)
		}
	})
	defer server.Close()

	adapter, err := NewSTT(hamsaSTTConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), hamsaSTTRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.WriteAudio(context.Background(), []byte{1, 2}); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	if err := stream.WriteAudio(context.Background(), []byte{3, 4}); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	if err := stream.CommitAudio(context.Background()); err != nil {
		t.Fatalf("commit audio: %v", err)
	}

	events := collectHamsaEvents(t, stream.Events(), 4)
	want := []protocol.EventType{protocol.EventUsageObserved, protocol.EventSpeechStarted, protocol.EventTranscriptFinal, protocol.EventSpeechEnded}
	for index := range want {
		if events[index].Type != want[index] {
			t.Fatalf("event %d = %q, want %q", index, events[index].Type, want[index])
		}
	}
	// Four bytes is 0.125 ms of audio; Hamsa charges a whole second for it.
	assertHamsaBilling(t, events[0].Billing, 1000, false)
	events = events[1:]
	var final struct {
		Text     string `json:"text"`
		IsFinal  bool   `json:"is_final"`
		Language string `json:"language"`
	}
	if err := json.Unmarshal(events[1].Data, &final); err != nil {
		t.Fatalf("final data: %v", err)
	}
	if final.Text != "مرحبا بكم" || !final.IsFinal || final.Language != "ar" {
		t.Fatalf("final = %+v", final)
	}
	if events[1].Extensions[extensionID] == nil {
		t.Fatal("final transcript dropped the raw Hamsa payload extension")
	}

	// The key rides the X-Api-Key header and never the URL, where proxies
	// and access logs would record it.
	request := <-requests
	if request.Header.Get("X-Api-Key") != "customer-hamsa-key" {
		t.Fatalf("X-Api-Key = %q, want customer-hamsa-key", request.Header.Get("X-Api-Key"))
	}
	if request.URL.RawQuery != "" {
		t.Fatalf("dial query = %q, want none", request.URL.RawQuery)
	}

	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}
	closing := collectHamsaEvents(t, stream.Events(), 1)
	assertHamsaBilling(t, closing[0].Billing, 1000, true)
	if _, ok := <-stream.Events(); ok {
		t.Fatal("events must end after close")
	}
}

func TestSTTAdapterCloseFlushesTheUncommittedTail(t *testing.T) {
	t.Parallel()
	server := newHamsaServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		if _, _, err := conn.Read(ctx); err != nil {
			t.Errorf("read request: %v", err)
			return
		}
		if err := conn.Write(ctx, websocket.MessageText, []byte("goodbye")); err != nil {
			t.Errorf("write transcript: %v", err)
		}
	})
	defer server.Close()

	adapter, err := NewSTT(hamsaSTTConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), hamsaSTTRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.WriteAudio(context.Background(), []byte{9, 9}); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	// The batch shape: audio, then Close, no explicit commit. The tail must
	// still be transcribed before the event channel ends.
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}
	events := collectHamsaEvents(t, stream.Events(), 5)
	var final struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(events[2].Data, &final); err != nil || final.Text != "goodbye" {
		t.Fatalf("final = %+v, err=%v", final, err)
	}
	assertHamsaBilling(t, events[4].Billing, 1000, true)
}

func TestSTTAdapterBillsEachUtteranceInWholeSeconds(t *testing.T) {
	t.Parallel()
	server := newHamsaServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		conn.SetReadLimit(1 << 20)
		if _, _, err := conn.Read(ctx); err != nil {
			t.Errorf("read request: %v", err)
			return
		}
		if err := conn.Write(ctx, websocket.MessageText, []byte("نعم")); err != nil {
			t.Errorf("write transcript: %v", err)
		}
	})
	defer server.Close()

	adapter, err := NewSTT(hamsaSTTConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), hamsaSTTRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	// 1.5 s and then exactly 2 s of 16 kHz PCM16: Hamsa rounds each request
	// up on its own, so the session owes 2 s + 2 s, not 3.5 s rounded once.
	for _, pcmBytes := range []int{48_000, 64_000} {
		if err := stream.WriteAudio(context.Background(), make([]byte, pcmBytes)); err != nil {
			t.Fatalf("write audio: %v", err)
		}
		if err := stream.CommitAudio(context.Background()); err != nil {
			t.Fatalf("commit audio: %v", err)
		}
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close stream: %v", err)
	}
	var last *protocol.BillingObservation
	for event := range stream.Events() {
		if event.Err != nil {
			t.Fatalf("provider error: %v", event.Err)
		}
		if event.Billing != nil {
			last = event.Billing
		}
	}
	assertHamsaBilling(t, last, 4000, true)
}

func TestSTTAdapterClassifiesTheDocumentedCloseCodes(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		status websocket.StatusCode
		code   string
	}{
		{4001, "authentication_failed"},
		{4003, "provider_quota_exceeded"},
		{4500, "authentication_failed"},
	} {
		server := newHamsaServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
			if _, _, err := conn.Read(ctx); err != nil {
				t.Errorf("read request: %v", err)
				return
			}
			_ = conn.Close(test.status, "refused")
		})
		adapter, err := NewSTT(hamsaSTTConfig(server.URL))
		if err != nil {
			t.Fatalf("new STT adapter: %v", err)
		}
		stream, err := adapter.Open(context.Background(), hamsaSTTRequest(server.URL))
		if err != nil {
			t.Fatalf("open stream: %v", err)
		}
		if err := stream.WriteAudio(context.Background(), []byte{1, 2}); err != nil {
			t.Fatalf("write audio: %v", err)
		}
		if err := stream.CommitAudio(context.Background()); err != nil {
			t.Fatalf("commit audio: %v", err)
		}
		event := <-stream.Events()
		providerErr, ok := event.Err.(*runtimepkg.ProviderError)
		if !ok || providerErr.Code != test.code || providerErr.Retryable {
			t.Errorf("close %d: error = %#v, want non-retryable %s", test.status, event.Err, test.code)
		}
		server.Close()
	}
}

func TestSTTAdapterEmptyCommitFinalizesSilenceWithoutDialling(t *testing.T) {
	t.Parallel()
	server := newHamsaServer(t, func(_ context.Context, _ *http.Request, _ *websocket.Conn) {
		t.Error("an empty commit must not reach the provider")
	})
	defer server.Close()

	adapter, err := NewSTT(hamsaSTTConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), hamsaSTTRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.CommitAudio(context.Background()); err != nil {
		t.Fatalf("commit audio: %v", err)
	}
	events := collectHamsaEvents(t, stream.Events(), 1)
	if events[0].Type != protocol.EventTranscriptFinal {
		t.Fatalf("event = %q, want transcript final", events[0].Type)
	}
	var final struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(events[0].Data, &final); err != nil || final.Text != "" {
		t.Fatalf("final = %+v, err=%v, want an empty finalized turn", final, err)
	}
}

func TestSTTAdapterSurfacesTheVendorErrorFrame(t *testing.T) {
	t.Parallel()
	server := newHamsaServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		if _, _, err := conn.Read(ctx); err != nil {
			t.Errorf("read request: %v", err)
			return
		}
		if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"error","payload":{"message":"Only WAV files are supported"}}`)); err != nil {
			t.Errorf("write error frame: %v", err)
		}
	})
	defer server.Close()

	adapter, err := NewSTT(hamsaSTTConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), hamsaSTTRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.WriteAudio(context.Background(), []byte{1, 2}); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	// Provider failures terminate the attempt via the event channel, matching
	// the streaming adapters; the commit call itself still succeeds locally.
	if err := stream.CommitAudio(context.Background()); err != nil {
		t.Fatalf("commit audio: %v", err)
	}
	event := <-stream.Events()
	providerErr, ok := event.Err.(*runtimepkg.ProviderError)
	if !ok {
		t.Fatalf("event error = %T (%v), want *runtimepkg.ProviderError", event.Err, event.Err)
	}
	if !strings.Contains(providerErr.Message, "Only WAV files are supported") {
		t.Fatalf("message = %q, want the vendor detail preserved", providerErr.Message)
	}
	if providerErr.Extensions[extensionID] == nil {
		t.Fatal("provider error dropped the raw Hamsa payload extension")
	}
	if providerErr.Code != "invalid_request" || providerErr.Retryable {
		t.Fatalf("error = %s retryable=%v, want a non-retryable invalid_request", providerErr.Code, providerErr.Retryable)
	}
}

func TestClassifyErrorMessageReadsTheDocumentedErrors(t *testing.T) {
	t.Parallel()
	for message, want := range map[string]string{
		"API key is invalid or expired":                              "authentication_failed",
		"Insufficient funds in wallet":                               "provider_quota_exceeded",
		"You have reached your speech-to-text usage limit!":          "provider_quota_exceeded",
		"Rate limit exceeded for this API key":                       "provider_rate_limited",
		"Invalid payload for message type: stt":                      "invalid_request",
		"Error generating transcription: Audio format not supported": "invalid_request",
		"something new": "provider_unavailable",
	} {
		if got, _ := classifyErrorMessage(message); got != want {
			t.Errorf("classifyErrorMessage(%q) = %s, want %s", message, got, want)
		}
	}
}

func TestSTTAdapterRejectsMediaTheVendorCannotTake(t *testing.T) {
	t.Parallel()
	adapter, err := NewSTT(STTConfig{})
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	for name, mutate := range map[string]func(*runtimepkg.AdapterRequest){
		"wrong encoding":    func(r *runtimepkg.AdapterRequest) { r.Media.Encoding = "opus" },
		"stereo":            func(r *runtimepkg.AdapterRequest) { r.Media.Channels = 2 },
		"wrong sample rate": func(r *runtimepkg.AdapterRequest) { r.Media.SampleRateHz = 24_000 },
		"unpinned model":    func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "auto" },
	} {
		request := hamsaSTTRequest("http://api.tryhamsa.com")
		mutate(&request)
		if _, err := adapter.Open(context.Background(), request); err == nil {
			t.Errorf("%s: open must fail", name)
		}
	}
}

func newHamsaServer(t *testing.T, callback func(context.Context, *http.Request, *websocket.Conn)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/realtime/ws" {
			http.NotFound(w, r)
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		go func() {
			defer conn.CloseNow()
			callback(context.Background(), r, conn)
		}()
	}))
}

func hamsaSTTRequest(serverURL string) runtimepkg.AdapterRequest {
	now := time.Date(2026, time.August, 16, 12, 0, 0, 0, time.UTC)
	endpoint, _ := url.Parse(serverURL)
	endpoint.Scheme = "ws"
	endpoint.Path = "/v1/realtime/ws"
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindSTT,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{
				Placement:        protocol.PlacementEmbedded,
				ProviderRoute:    protocol.RouteProviderDirect,
				CredentialSource: protocol.CredentialsBYOK,
			},
			Route: protocol.PlanRoute{
				Provider: "hamsa", Model: "s3", Adapter: STTAdapterID,
				Transport: protocol.TransportWebSocket, Endpoint: endpoint.String(),
				Credential: &protocol.DelegatedCredential{
					Kind: protocol.CredentialBearer, Value: "customer-hamsa-key", ExpiresAt: now.Add(time.Hour),
				},
			},
		},
		Options: protocol.RequestOptions{Language: "ar"},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
	}
}

func hamsaSTTConfig(serverURL string) STTConfig {
	endpoint, _ := url.Parse(serverURL)
	return STTConfig{AllowedEndpointHosts: []string{endpoint.Hostname()}, AllowInsecureEndpoint: true}
}

func assertHamsaBilling(t *testing.T, billing *protocol.BillingObservation, milliseconds int64, complete bool) {
	t.Helper()
	if billing == nil {
		t.Fatal("no billing observation")
	}
	if err := billing.Validate(); err != nil {
		t.Fatalf("billing observation invalid: %v", err)
	}
	if billing.OperationID != "session" || billing.Model != "s3" || billing.Mode != "streaming" || billing.Quantities["duration_seconds"] != milliseconds || billing.Complete != complete {
		t.Fatalf("billing = %+v, want session s3 streaming duration_seconds=%d complete=%v", billing, milliseconds, complete)
	}
}

func collectHamsaEvents(t *testing.T, events <-chan runtimepkg.ProviderEvent, count int) []runtimepkg.ProviderEvent {
	t.Helper()
	result := make([]runtimepkg.ProviderEvent, 0, count)
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for len(result) < count {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatalf("events closed after %d of %d events", len(result), count)
			}
			if event.Err != nil {
				t.Fatalf("unexpected provider error after %d events: %v", len(result), event.Err)
			}
			result = append(result, event)
		case <-timer.C:
			t.Fatalf("timed out after %d of %d events", len(result), count)
		}
	}
	return result
}
