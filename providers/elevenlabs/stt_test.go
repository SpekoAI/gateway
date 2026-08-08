package elevenlabs

import (
	"context"
	"encoding/base64"
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

func TestSTTAdapterSendsBase64AudioAndMapsTranscripts(t *testing.T) {
	t.Parallel()
	requests := make(chan *http.Request, 1)
	frames := make(chan map[string]any, 4)
	server := newSTTServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		requests <- request.Clone(request.Context())
		if err := writeSTTJSON(ctx, conn, map[string]any{"message_type": "session_started", "session_id": "el-stt-1"}); err != nil {
			t.Errorf("session_started: %v", err)
			return
		}
		for range 2 {
			frame, err := readSTTJSON(ctx, conn)
			if err != nil {
				t.Errorf("read frame: %v", err)
				return
			}
			frames <- frame
		}
		for _, message := range []map[string]any{
			{"message_type": "partial_transcript", "text": "hello"},
			// Both arrive for the SAME segment when include_timestamps is on. Only
			// the timestamped twin may produce a final, or every turn double-fires.
			{"message_type": "committed_transcript", "text": "hello world"},
			{"message_type": "committed_transcript_with_timestamps", "text": "hello world", "words": []map[string]any{
				{"text": "hello", "start": 0.1, "end": 0.4, "type": "word"},
				{"text": " ", "start": 0.4, "end": 0.4, "type": "spacing"},
				{"text": "world", "start": 0.45, "end": 0.9, "type": "word"},
			}},
		} {
			if err := writeSTTJSON(ctx, conn, message); err != nil {
				t.Errorf("write transcript: %v", err)
				return
			}
		}
	})
	defer server.Close()

	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	providerStream, err := adapter.Open(context.Background(), sttRequest(server.URL, protocol.CredentialsBYOK))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := providerStream.WriteAudio(context.Background(), []byte{1, 2, 3, 4}); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	if err := providerStream.CommitAudio(context.Background()); err != nil {
		t.Fatalf("commit audio: %v", err)
	}

	// Realtime Scribe has NO binary input frame. Audio must arrive base64-encoded
	// inside an input_audio_chunk, so a binary write would be silently ignored.
	audioFrame := <-frames
	if audioFrame["message_type"] != "input_audio_chunk" {
		t.Fatalf("audio frame message_type = %v, want input_audio_chunk", audioFrame["message_type"])
	}
	decoded, err := base64.StdEncoding.DecodeString(audioFrame["audio_base_64"].(string))
	if err != nil || string(decoded) != "\x01\x02\x03\x04" {
		t.Fatalf("audio_base_64 decoded to %q (%v), want the raw PCM", decoded, err)
	}
	if audioFrame["commit"] != false {
		t.Fatalf("audio frame commit = %v, want false", audioFrame["commit"])
	}
	commitFrame := <-frames
	if commitFrame["commit"] != true || commitFrame["audio_base_64"] != "" {
		t.Fatalf("commit frame = %v, want an empty chunk with commit true", commitFrame)
	}

	// session_started, the partial, and exactly ONE final — not two.
	events := collectSTTEvents(t, providerStream.Events(), 3)
	want := []protocol.EventType{protocol.EventUsageObserved, protocol.EventTranscriptDelta, protocol.EventTranscriptFinal}
	for index := range want {
		if events[index].Type != want[index] {
			t.Fatalf("event %d = %q, want %q", index, events[index].Type, want[index])
		}
	}
	if events[2].Extensions[extensionID] == nil {
		t.Fatal("final transcript did not retain the ElevenLabs extension")
	}
	var final struct {
		Text    string `json:"text"`
		IsFinal bool   `json:"is_final"`
		StartMs int64  `json:"audio_start_ms"`
		EndMs   int64  `json:"audio_end_ms"`
		Words   []struct {
			Text    string `json:"text"`
			StartMs int64  `json:"start_ms"`
			EndMs   int64  `json:"end_ms"`
		} `json:"words"`
	}
	if err := json.Unmarshal(events[2].Data, &final); err != nil {
		t.Fatalf("decode final: %v", err)
	}
	if final.Text != "hello world" || !final.IsFinal {
		t.Fatalf("final = %+v, want the committed text", final)
	}
	// The `spacing` token must be dropped: it carries a zero-width range and would
	// corrupt any consumer that trusts word timings.
	if len(final.Words) != 2 || final.Words[0].Text != "hello" || final.Words[1].Text != "world" {
		t.Fatalf("words = %+v, want the two word-typed tokens only", final.Words)
	}
	if final.Words[0].StartMs != 100 || final.Words[1].EndMs != 900 {
		t.Fatalf("word timings = %+v, want seconds converted to ms", final.Words)
	}
	if final.StartMs != 100 || final.EndMs != 900 {
		t.Fatalf("segment span = %d..%d ms, want 100..900 from the word timings", final.StartMs, final.EndMs)
	}

	query := (<-requests).URL.Query()
	for field, wanted := range map[string]string{
		"model_id":           "scribe_v2_realtime",
		"audio_format":       "pcm_16000",
		"include_timestamps": "true",
		"commit_strategy":    "vad",
		// `language_code`, NOT `language`. The vendor ignores an unknown parameter, so
		// the wrong name leaves Scribe auto-detecting and the caller never finds out.
		"language_code": "es",
	} {
		if got := query.Get(field); got != wanted {
			t.Fatalf("query %s = %q, want %q", field, got, wanted)
		}
	}
	_ = providerStream.(runtimepkg.AbortingProviderStream).Abort(context.Background())
}

// Each of these arrives as a frame on an ALREADY-OPEN socket, so the code has to
// come from the frame rather than a dial status. Collapsing them would make a dead
// key and an empty balance indistinguishable, and only one is worth retrying.
func TestSTTAdapterKeepsTheVendorsErrorDistinction(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		frame     string
		code      string
		retryable bool
	}{
		{frame: "auth_error", code: "authentication_failed"},
		{frame: "quota_exceeded", code: "provider_quota_exceeded"},
		{frame: "rate_limited", code: "provider_rate_limited", retryable: true},
		{frame: "input_error", code: "invalid_request"},
		{frame: "error", code: "provider_unavailable"},
	} {
		t.Run(tc.frame, func(t *testing.T) {
			t.Parallel()
			server := newSTTServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
				_ = writeSTTJSON(ctx, conn, map[string]any{"message_type": tc.frame, "error": "detail here"})
			})
			defer server.Close()
			adapter, err := NewSTT(sttTestConfig(server.URL))
			if err != nil {
				t.Fatalf("new STT adapter: %v", err)
			}
			providerStream, err := adapter.Open(context.Background(), sttRequest(server.URL, protocol.CredentialsBYOK))
			if err != nil {
				t.Fatalf("open stream: %v", err)
			}
			defer func() { _ = providerStream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()
			timer := time.NewTimer(2 * time.Second)
			defer timer.Stop()
			select {
			case event := <-providerStream.Events():
				providerErr := &runtimepkg.ProviderError{}
				if event.Err == nil {
					t.Fatalf("%s produced no error", tc.frame)
				}
				if !asProviderError(event.Err, &providerErr) {
					t.Fatalf("%s error is not a ProviderError: %v", tc.frame, event.Err)
				}
				if providerErr.Code != tc.code {
					t.Fatalf("%s code = %q, want %q", tc.frame, providerErr.Code, tc.code)
				}
				if providerErr.Retryable != tc.retryable {
					t.Fatalf("%s retryable = %v, want %v", tc.frame, providerErr.Retryable, tc.retryable)
				}
				if !strings.Contains(providerErr.Message, "detail here") {
					t.Fatalf("%s message dropped the vendor detail: %q", tc.frame, providerErr.Message)
				}
			case <-timer.C:
				t.Fatalf("%s timed out", tc.frame)
			}
		})
	}
}

// A write larger than one frame budget must be split. The route can hand a whole
// utterance as a single write, and an oversized WebSocket message would be
// rejected by the server rather than transcribed.
func TestSTTAdapterSplitsAnOversizedWrite(t *testing.T) {
	t.Parallel()
	frames := make(chan int, 8)
	server := newSTTServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		conn.SetReadLimit(1 << 20)
		for {
			frame, err := readSTTJSON(ctx, conn)
			if err != nil {
				return
			}
			encoded, _ := frame["audio_base_64"].(string)
			decoded, _ := base64.StdEncoding.DecodeString(encoded)
			frames <- len(decoded)
		}
	})
	defer server.Close()
	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	providerStream, err := adapter.Open(context.Background(), sttRequest(server.URL, protocol.CredentialsBYOK))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer func() { _ = providerStream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()
	if err := providerStream.WriteAudio(context.Background(), make([]byte, sttPCMFrameBytes+500)); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	if first := <-frames; first != sttPCMFrameBytes {
		t.Fatalf("first frame = %d bytes, want the %d cap", first, sttPCMFrameBytes)
	}
	if second := <-frames; second != 500 {
		t.Fatalf("second frame = %d bytes, want the 500-byte remainder", second)
	}
}

// Close must commit first. Dropping the socket without a commit discards whatever
// Scribe had buffered, which silently loses the last thing the caller said.
func TestSTTAdapterCommitsBeforeClosing(t *testing.T) {
	t.Parallel()
	frames := make(chan map[string]any, 2)
	server := newSTTServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		for {
			frame, err := readSTTJSON(ctx, conn)
			if err != nil {
				return
			}
			frames <- frame
		}
	})
	defer server.Close()
	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	providerStream, err := adapter.Open(context.Background(), sttRequest(server.URL, protocol.CredentialsBYOK))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := providerStream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	frame := <-frames
	if frame["commit"] != true {
		t.Fatalf("close sent %v, want a commit frame", frame)
	}
	// After Close the stream is closed for writes; a further write must be refused
	// rather than racing the teardown.
	if err := providerStream.WriteAudio(context.Background(), []byte{1, 2}); err == nil {
		t.Fatal("write after close succeeded")
	}
	_ = providerStream.(runtimepkg.AbortingProviderStream).Abort(context.Background())
}

func TestSTTAdapterRejectsMismatchedRequests(t *testing.T) {
	t.Parallel()
	adapter, err := NewSTT(STTConfig{AllowedEndpointHosts: []string{"example.test"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	for name, mutate := range map[string]func(*runtimepkg.AdapterRequest){
		"tts kind":         func(r *runtimepkg.AdapterRequest) { r.Kind = protocol.SessionKindTTS },
		"other provider":   func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Provider = "deepgram" },
		"no credential":    func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential = nil },
		"auto model":       func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "auto" },
		"no media":         func(r *runtimepkg.AdapterRequest) { r.Media = nil },
		"opus encoding":    func(r *runtimepkg.AdapterRequest) { r.Media.Encoding = "opus" },
		"batch model path": func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = "ws://example.test/v1/speech-to-text" },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			request := sttRequest("http://example.test", protocol.CredentialsBYOK)
			mutate(&request)
			if _, err := adapter.Open(context.Background(), request); err == nil {
				t.Fatalf("%s was accepted", name)
			}
		})
	}
}

func newSTTServer(t *testing.T, callback func(context.Context, *http.Request, *websocket.Conn)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != sttEndpointPath {
			http.NotFound(w, r)
			return
		}
		// The vendor accepts EITHER a permanent key in the header OR a single-use
		// token in the query string. The harness enforces the same either/or so a
		// regression to header-only managed auth fails here rather than in staging.
		if r.Header.Get("xi-api-key") == "" && r.URL.Query().Get("token") == "" {
			http.Error(w, "missing credential", http.StatusUnauthorized)
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

func sttRequest(serverURL string, source protocol.CredentialSource) runtimepkg.AdapterRequest {
	now := time.Date(2026, time.August, 6, 12, 0, 0, 0, time.UTC)
	endpoint, _ := url.Parse(serverURL)
	endpoint.Scheme = "ws"
	endpoint.Path = sttEndpointPath
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindSTT,
		Plan: protocol.SessionPlan{
			Execution: protocol.Execution{Placement: protocol.PlacementEmbedded, ProviderRoute: protocol.RouteProviderDirect, CredentialSource: source},
			Route: protocol.PlanRoute{
				Provider: "elevenlabs", Model: "scribe_v2_realtime", Adapter: STTAdapterID,
				Transport: protocol.TransportWebSocket, Endpoint: endpoint.String(),
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "customer-elevenlabs-key", ExpiresAt: now.Add(time.Minute)},
			},
		},
		// Regional tag on purpose: Scribe only accepts the primary subtag.
		Options: protocol.RequestOptions{Language: "es-MX"},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
	}
}

func sttTestConfig(serverURL string) STTConfig {
	endpoint, _ := url.Parse(serverURL)
	return STTConfig{AllowedEndpointHosts: []string{endpoint.Hostname()}, AllowInsecureEndpoint: true}
}

func writeSTTJSON(ctx context.Context, conn *websocket.Conn, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, payload)
}

func readSTTJSON(ctx context.Context, conn *websocket.Conn) (map[string]any, error) {
	_, payload, err := conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

func collectSTTEvents(t *testing.T, events <-chan runtimepkg.ProviderEvent, count int) []runtimepkg.ProviderEvent {
	t.Helper()
	result := make([]runtimepkg.ProviderEvent, 0, count)
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for len(result) < count {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatalf("events closed after %d events", len(result))
			}
			if event.Err != nil {
				t.Fatalf("provider event error: %v", event.Err)
			}
			result = append(result, event)
		case <-timer.C:
			t.Fatalf("timed out after %d events", len(result))
		}
	}
	return result
}

func asProviderError(err error, target **runtimepkg.ProviderError) bool {
	providerErr, ok := err.(*runtimepkg.ProviderError)
	if !ok {
		return false
	}
	*target = providerErr
	return true
}

// A managed session carries a minted single-use token, and the vendor accepts a
// token ONLY as the `token` query parameter. Sending it as `xi-api-key` fails
// authentication, so managed sessions would have died at the handshake.
func TestSTTAdapterSendsAManagedTokenAsAQueryParameter(t *testing.T) {
	t.Parallel()
	requests := make(chan *http.Request, 1)
	server := newSTTServer(t, func(_ context.Context, request *http.Request, _ *websocket.Conn) {
		requests <- request.Clone(request.Context())
	})
	defer server.Close()
	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	providerStream, err := adapter.Open(context.Background(), sttRequest(server.URL, protocol.CredentialsManaged))
	if err != nil {
		t.Fatalf("open managed stream: %v", err)
	}
	defer func() { _ = providerStream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()
	request := <-requests
	if got := request.URL.Query().Get("token"); got != "customer-elevenlabs-key" {
		t.Fatalf("managed token query = %q, want the minted credential", got)
	}
	// And it must NOT also go in the header: the header is for a permanent key.
	if header := request.Header.Get("xi-api-key"); header != "" {
		t.Fatalf("managed session also sent xi-api-key = %q", header)
	}
}

func TestSTTAdapterSendsABYOKKeyAsAHeader(t *testing.T) {
	t.Parallel()
	requests := make(chan *http.Request, 1)
	server := newSTTServer(t, func(_ context.Context, request *http.Request, _ *websocket.Conn) {
		requests <- request.Clone(request.Context())
	})
	defer server.Close()
	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	providerStream, err := adapter.Open(context.Background(), sttRequest(server.URL, protocol.CredentialsBYOK))
	if err != nil {
		t.Fatalf("open byok stream: %v", err)
	}
	defer func() { _ = providerStream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()
	request := <-requests
	if got := request.Header.Get("xi-api-key"); got != "customer-elevenlabs-key" {
		t.Fatalf("byok xi-api-key = %q, want the customer key", got)
	}
	// A permanent key must never be put in a URL, where it can reach logs.
	if token := request.URL.Query().Get("token"); token != "" {
		t.Fatalf("byok session leaked the key into the query string: %q", token)
	}
}

// A relay plan is managed for billing purposes but carries the connector's
// permanent provider key, so it must use the header channel like BYOK — never
// the `token` query parameter, where a permanent key could reach logs.
func TestSTTAdapterSendsARelayKeyAsAHeader(t *testing.T) {
	t.Parallel()
	requests := make(chan *http.Request, 1)
	server := newSTTServer(t, func(_ context.Context, request *http.Request, _ *websocket.Conn) {
		requests <- request.Clone(request.Context())
	})
	defer server.Close()
	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	request := sttRequest(server.URL, protocol.CredentialsManaged)
	request.Plan.Execution.ProviderRoute = protocol.RouteSpekoRelay
	request.Plan.Route.Credential.Value = "connector-elevenlabs-key"
	providerStream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open relay stream: %v", err)
	}
	defer func() { _ = providerStream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) }()
	received := <-requests
	if got := received.Header.Get("xi-api-key"); got != "connector-elevenlabs-key" {
		t.Fatalf("relay xi-api-key = %q, want the connector key", got)
	}
	if token := received.URL.Query().Get("token"); token != "" {
		t.Fatalf("relay session leaked the key into the query string: %q", token)
	}
}

// protocol.SessionPlan validation requires a relay plan to label its
// credential relay_access, while a connector that synthesizes the plan and
// drives the adapter directly labels the same permanent key bearer. The relay
// arm must accept both spellings, or one of the two constructions becomes
// quietly unreachable.
func TestSTTAdapterAcceptsRelayAccessCredentialKindOnRelayRoute(t *testing.T) {
	t.Parallel()
	server := newSTTServer(t, func(context.Context, *http.Request, *websocket.Conn) {})
	defer server.Close()
	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	request := sttRequest(server.URL, protocol.CredentialsManaged)
	request.Plan.Execution.ProviderRoute = protocol.RouteSpekoRelay
	request.Plan.Route.Credential.Kind = protocol.CredentialRelayAccess
	request.Plan.Route.Credential.Value = "connector-elevenlabs-key"
	providerStream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open relay stream with relay_access credential: %v", err)
	}
	_ = providerStream.(runtimepkg.AbortingProviderStream).Abort(context.Background())
}

// `audio_format` accepts a fixed set of tokens. The platform's TS adapter silently
// substitutes pcm_16000 for anything else, which does not resample: Scribe reads
// the bytes at the wrong rate and transcription degrades while the session looks
// healthy. An unsupported rate must be refused instead.
func TestSTTAdapterRefusesAnUndeclarableSampleRate(t *testing.T) {
	t.Parallel()
	adapter, err := NewSTT(STTConfig{AllowedEndpointHosts: []string{"example.test"}, AllowInsecureEndpoint: true})
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	for _, rate := range []int{8000, 16000, 24000, 48000} {
		request := sttRequest("http://example.test", protocol.CredentialsBYOK)
		request.Media.SampleRateHz = rate
		if _, err := adapter.Open(context.Background(), request); err != nil && strings.Contains(err.Error(), "sample rate") {
			t.Fatalf("%d Hz was refused but is an accepted audio_format", rate)
		}
	}
	request := sttRequest("http://example.test", protocol.CredentialsBYOK)
	request.Media.SampleRateHz = 32_000
	if _, err := adapter.Open(context.Background(), request); err == nil || !strings.Contains(err.Error(), "sample rate") {
		t.Fatalf("32000 Hz = %v, want a sample-rate refusal rather than a silent pcm_16000", err)
	}
}
