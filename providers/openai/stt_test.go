package openai

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

// TestSTTOpenSendsDocumentedSessionUpdate pins the whole handshake: the path,
// the auth header, the ABSENCE of a query string, and every field name in the
// session.update body. Each literal below is transcribed from OpenAI's raw
// sources rather than referenced from a package constant, so a typo in the
// adapter fails here instead of shipping.
func TestSTTOpenSendsDocumentedSessionUpdate(t *testing.T) {
	t.Parallel()

	handshakes := make(chan *http.Request, 1)
	frames := make(chan []byte, 4)
	server := newRealtimeServer(t, handshakes, frames, nil)
	defer server.Close()

	stream := openSTT(t, server.URL, nil)
	defer func() { _ = stream.Abort(context.Background()) }()

	handshake := receiveRequest(t, handshakes)
	if handshake.URL.Path != "/v1/realtime" {
		t.Errorf("path = %q, want /v1/realtime", handshake.URL.Path)
	}
	// The TypeScript provider dials `?intent=transcription`. That parameter is in
	// no OpenAI source; a transcription session is selected by the body. If it
	// ever creeps back in, this fails.
	if handshake.URL.RawQuery != "" {
		t.Errorf("query = %q, want no query parameters", handshake.URL.RawQuery)
	}
	if got := handshake.Header.Get("Authorization"); got != "Bearer customer-openai-key" {
		t.Errorf("Authorization = %q", got)
	}

	// Compared as decoded JSON so the assertion is about field NAMES, not Go
	// struct ordering.
	var got map[string]any
	if err := json.Unmarshal(receiveFrame(t, frames), &got); err != nil {
		t.Fatalf("session.update is not JSON: %v", err)
	}
	want := map[string]any{
		"type": "session.update",
		"session": map[string]any{
			"type": "transcription",
			"audio": map[string]any{
				"input": map[string]any{
					"format": map[string]any{"type": "audio/pcm", "rate": float64(24000)},
					"transcription": map[string]any{
						"model": "gpt-live-transcribe",
						// gpt-live-transcribe takes the PLURAL field; sending the
						// singular `language` to it pins nothing at all.
						"languages": []any{"en"},
					},
					// null, not omitted: omitting the key leaves the vendor's own
					// turn detection in place, and this framework owns turns.
					"turn_detection": nil,
				},
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("session.update =\n%#v\nwant\n%#v", got, want)
	}
}

// TestSTTRelayRouteSendsTheConnectorKeyAsBearer: a relay plan is managed for
// billing purposes but carries the relay connector's permanent OpenAI key.
// OpenAI has exactly one credential channel, so the key travels in the same
// Authorization: Bearer header as every other source and never in the URL.
// Both credential-kind spellings must open, because protocol.SessionPlan
// validation labels a relay credential relay_access while the relay connector
// that synthesizes plans and drives this adapter directly labels the same
// permanent key bearer.
func TestSTTRelayRouteSendsTheConnectorKeyAsBearer(t *testing.T) {
	t.Parallel()

	for _, kind := range []protocol.CredentialKind{protocol.CredentialBearer, protocol.CredentialRelayAccess} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			handshakes := make(chan *http.Request, 1)
			frames := make(chan []byte, 4)
			server := newRealtimeServer(t, handshakes, frames, nil)
			defer server.Close()

			stream := openSTT(t, server.URL, func(request *runtimepkg.AdapterRequest) {
				request.Plan.Execution.ProviderRoute = protocol.RouteSpekoRelay
				request.Plan.Execution.CredentialSource = protocol.CredentialsManaged
				request.Plan.Route.Credential.Kind = kind
				request.Plan.Route.Credential.Value = "connector-openai-key"
			})
			defer func() { _ = stream.Abort(context.Background()) }()

			handshake := receiveRequest(t, handshakes)
			if got := handshake.Header.Get("Authorization"); got != "Bearer connector-openai-key" {
				t.Errorf("Authorization = %q", got)
			}
			if handshake.URL.RawQuery != "" {
				t.Errorf("relay handshake query = %q, want none", handshake.URL.RawQuery)
			}
		})
	}
}

// TestSTTUsesSingularLanguageForNonLiveModels guards the other half of the
// vendor's language-field split, and that a regional tag is reduced to the
// ISO-639-1 primary subtag the singular field documents.
func TestSTTUsesSingularLanguageForNonLiveModels(t *testing.T) {
	t.Parallel()

	handshakes := make(chan *http.Request, 1)
	frames := make(chan []byte, 4)
	server := newRealtimeServer(t, handshakes, frames, nil)
	defer server.Close()

	stream := openSTT(t, server.URL, func(request *runtimepkg.AdapterRequest) {
		request.Plan.Route.Model = "gpt-4o-transcribe"
		request.Options.Language = "es-MX"
	})
	defer func() { _ = stream.Abort(context.Background()) }()

	transcription := sessionTranscription(t, receiveFrame(t, frames))
	want := map[string]any{"model": "gpt-4o-transcribe", "language": "es"}
	if !reflect.DeepEqual(transcription, want) {
		t.Fatalf("transcription = %#v, want %#v", transcription, want)
	}
}

// TestSTTPreservesRegionalChineseTag exists because the obvious implementation —
// reduce every tag to its primary subtag — silently changes which Chinese
// variant is transcribed. OpenAI documents zh-cn/zh-tw/zh-hk as accepted whole.
func TestSTTPreservesRegionalChineseTag(t *testing.T) {
	t.Parallel()

	handshakes := make(chan *http.Request, 1)
	frames := make(chan []byte, 4)
	server := newRealtimeServer(t, handshakes, frames, nil)
	defer server.Close()

	stream := openSTT(t, server.URL, func(request *runtimepkg.AdapterRequest) {
		request.Options.Language = "zh-TW"
	})
	defer func() { _ = stream.Abort(context.Background()) }()

	transcription := sessionTranscription(t, receiveFrame(t, frames))
	want := map[string]any{"model": "gpt-live-transcribe", "languages": []any{"zh-tw"}}
	if !reflect.DeepEqual(transcription, want) {
		t.Fatalf("transcription = %#v, want %#v", transcription, want)
	}
}

// TestSTTForwardsAudioAsBase64AppendAndCommit checks the two client events that
// carry a live turn. Audio is a base64 STRING field on a JSON event: the socket
// has no binary input frame, so sending a binary message would be silently
// dropped by the server.
func TestSTTForwardsAudioAsBase64AppendAndCommit(t *testing.T) {
	t.Parallel()

	handshakes := make(chan *http.Request, 1)
	frames := make(chan []byte, 8)
	server := newRealtimeServer(t, handshakes, frames, nil)
	defer server.Close()

	stream := openSTT(t, server.URL, nil)
	defer func() { _ = stream.Abort(context.Background()) }()
	receiveFrame(t, frames) // session.update

	if err := stream.WriteAudio(context.Background(), []byte{1, 2, 3, 4}); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	if err := stream.CommitAudio(context.Background()); err != nil {
		t.Fatalf("commit audio: %v", err)
	}

	var append map[string]any
	if err := json.Unmarshal(receiveFrame(t, frames), &append); err != nil {
		t.Fatalf("append frame is not JSON: %v", err)
	}
	wantAppend := map[string]any{
		"type":  "input_audio_buffer.append",
		"audio": base64.StdEncoding.EncodeToString([]byte{1, 2, 3, 4}),
	}
	if !reflect.DeepEqual(append, wantAppend) {
		t.Fatalf("append = %#v, want %#v", append, wantAppend)
	}

	var commit map[string]any
	if err := json.Unmarshal(receiveFrame(t, frames), &commit); err != nil {
		t.Fatalf("commit frame is not JSON: %v", err)
	}
	if !reflect.DeepEqual(commit, map[string]any{"type": "input_audio_buffer.commit"}) {
		t.Fatalf("commit = %#v", commit)
	}
}

// TestSTTSplitsOversizedAudioIntoMultipleAppends keeps one large caller write
// from becoming one oversized WebSocket message.
func TestSTTSplitsOversizedAudioIntoMultipleAppends(t *testing.T) {
	t.Parallel()

	handshakes := make(chan *http.Request, 1)
	frames := make(chan []byte, 8)
	server := newRealtimeServer(t, handshakes, frames, nil)
	defer server.Close()

	stream := openSTT(t, server.URL, nil)
	defer func() { _ = stream.Abort(context.Background()) }()
	receiveFrame(t, frames) // session.update

	// 1.5 frames' worth at the adapter's 0.5 s slice.
	if err := stream.WriteAudio(context.Background(), make([]byte, 36_000)); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	total := 0
	for index := 0; index < 2; index++ {
		var frame struct {
			Type  string `json:"type"`
			Audio string `json:"audio"`
		}
		if err := json.Unmarshal(receiveFrame(t, frames), &frame); err != nil {
			t.Fatalf("append %d is not JSON: %v", index, err)
		}
		if frame.Type != "input_audio_buffer.append" {
			t.Fatalf("append %d type = %q", index, frame.Type)
		}
		decoded, err := base64.StdEncoding.DecodeString(frame.Audio)
		if err != nil {
			t.Fatalf("append %d audio is not base64: %v", index, err)
		}
		total += len(decoded)
	}
	if total != 36_000 {
		t.Fatalf("forwarded %d bytes across two appends, want 36000", total)
	}
}

// TestSTTEmitsCumulativePartialsThenFinal is the streaming contract: several
// interim frames must reach the consumer BEFORE completion, and each one must
// carry the utterance so far. OpenAI's deltas are incremental ("Hello," / " how"
// / " are you?"); every other transcript.delta in this protocol is cumulative
// and consumers REPLACE the displayed partial on each one, so forwarding raw
// deltas would render one word at a time.
func TestSTTEmitsCumulativePartialsThenFinal(t *testing.T) {
	t.Parallel()

	handshakes := make(chan *http.Request, 1)
	frames := make(chan []byte, 4)
	server := newRealtimeServer(t, handshakes, frames, func(ctx context.Context, conn *websocket.Conn) {
		writeServerJSON(t, ctx, conn, `{"type":"session.created","event_id":"event_1","session":{"id":"sess_openai_1"}}`)
		writeServerJSON(t, ctx, conn, `{"type":"input_audio_buffer.speech_started","event_id":"event_2","audio_start_ms":1000,"item_id":"item_003"}`)
		writeServerJSON(t, ctx, conn, `{"type":"conversation.item.input_audio_transcription.delta","event_id":"event_3","item_id":"item_003","content_index":0,"delta":"Hello,"}`)
		writeServerJSON(t, ctx, conn, `{"type":"conversation.item.input_audio_transcription.delta","event_id":"event_4","item_id":"item_003","content_index":0,"delta":" how"}`)
		writeServerJSON(t, ctx, conn, `{"type":"conversation.item.input_audio_transcription.delta","event_id":"event_5","item_id":"item_003","content_index":0,"delta":" are you?"}`)
		writeServerJSON(t, ctx, conn, `{"type":"input_audio_buffer.committed","event_id":"event_6","item_id":"item_003"}`)
		writeServerJSON(t, ctx, conn, `{"type":"conversation.item.input_audio_transcription.completed","event_id":"event_7","item_id":"item_003","content_index":0,"transcript":"Hello, how are you?","usage":{"type":"tokens","total_tokens":48}}`)
		writeServerJSON(t, ctx, conn, `{"type":"input_audio_buffer.speech_stopped","event_id":"event_8","audio_end_ms":2000,"item_id":"item_003"}`)
	})
	defer server.Close()

	stream := openSTT(t, server.URL, nil)
	defer func() { _ = stream.Abort(context.Background()) }()

	events := collectSTT(t, stream.Events(), 7)
	wantTypes := []string{
		string(protocol.EventUsageObserved),
		string(protocol.EventSpeechStarted),
		string(protocol.EventTranscriptDelta),
		string(protocol.EventTranscriptDelta),
		string(protocol.EventTranscriptDelta),
		string(protocol.EventTranscriptFinal),
		string(protocol.EventSpeechEnded),
	}
	if got := sttEventTypes(events); !reflect.DeepEqual(got, wantTypes) {
		t.Fatalf("event types = %v, want %v", got, wantTypes)
	}
	// Three interim frames landed before the final: this is what makes the
	// adapter streaming rather than batch.
	wantPartials := []string{"Hello,", "Hello, how", "Hello, how are you?"}
	for index, want := range wantPartials {
		if got := transcriptText(t, events[2+index]); got != want {
			t.Fatalf("partial %d = %q, want %q", index, got, want)
		}
	}
	if got := transcriptText(t, events[5]); got != "Hello, how are you?" {
		t.Fatalf("final text = %q", got)
	}
	// The raw provider payload must survive on the canonical event under the
	// literal extension namespace, so a consumer can recover vendor detail.
	if events[5].Extensions["openai.com/realtime/v1"] == nil {
		t.Fatal("final transcript must retain its OpenAI extension")
	}
	if got := usageRequestID(t, events[0]); got != "sess_openai_1" {
		t.Fatalf("usage provider_request_id = %q", got)
	}
}

// TestSTTRestartsPartialAfterEachFinal: the socket outlives mid-session finals,
// so without a reset the second turn would be prefixed with the first.
func TestSTTRestartsPartialAfterEachFinal(t *testing.T) {
	t.Parallel()

	handshakes := make(chan *http.Request, 1)
	frames := make(chan []byte, 4)
	server := newRealtimeServer(t, handshakes, frames, func(ctx context.Context, conn *websocket.Conn) {
		writeServerJSON(t, ctx, conn, `{"type":"conversation.item.input_audio_transcription.delta","event_id":"e1","item_id":"item_1","delta":"first"}`)
		writeServerJSON(t, ctx, conn, `{"type":"conversation.item.input_audio_transcription.completed","event_id":"e2","item_id":"item_1","transcript":"first turn"}`)
		writeServerJSON(t, ctx, conn, `{"type":"conversation.item.input_audio_transcription.delta","event_id":"e3","item_id":"item_2","delta":"second"}`)
	})
	defer server.Close()

	stream := openSTT(t, server.URL, nil)
	defer func() { _ = stream.Abort(context.Background()) }()

	events := collectSTT(t, stream.Events(), 3)
	if got := transcriptText(t, events[2]); got != "second" {
		t.Fatalf("second-turn partial = %q, want %q (the first turn leaked in)", got, "second")
	}
}

// TestSTTMapsErrorFramesToDistinctCodes drives whole raw frames through the
// message handler so both the JSON field names (`error.type`, `error.code`) and
// the classification are covered. Collapsing these into one code would make a
// dead key, an empty balance, and a throttle indistinguishable.
func TestSTTMapsErrorFramesToDistinctCodes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		frame   string
		code    string
		retries bool
	}{
		{
			name:  "revoked key",
			frame: `{"type":"error","event_id":"e1","error":{"type":"invalid_request_error","code":"invalid_api_key","message":"Incorrect API key provided."}}`,
			code:  "authentication_failed",
		},
		{
			name:  "exhausted balance",
			frame: `{"type":"error","event_id":"e2","error":{"type":"insufficient_quota","code":"insufficient_quota","message":"You exceeded your current quota."}}`,
			code:  "provider_quota_exceeded",
		},
		{
			name:    "throttle",
			frame:   `{"type":"error","event_id":"e3","error":{"type":"rate_limit_error","code":"rate_limit_exceeded","message":"Rate limit reached."}}`,
			code:    "provider_rate_limited",
			retries: true,
		},
		{
			name:  "caller sent something invalid",
			frame: `{"type":"error","event_id":"e4","error":{"type":"invalid_request_error","code":"invalid_value","message":"Turn detection is not supported for this transcription model"}}`,
			code:  "invalid_request",
		},
		{
			name:    "vendor fault",
			frame:   `{"type":"error","event_id":"e5","error":{"type":"server_error","code":null,"message":"The server had an error."}}`,
			code:    "provider_unavailable",
			retries: true,
		},
		{
			name:  "per-item transcription failure",
			frame: `{"type":"conversation.item.input_audio_transcription.failed","event_id":"e6","item_id":"item_1","error":{"type":"invalid_request_error","code":"audio_unintelligible","message":"Audio could not be transcribed."}}`,
			code:  "invalid_request",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			stream, cancel := newHandlerOnlySTTStream()
			defer cancel()

			err := stream.handleMessage([]byte(testCase.frame))
			providerErr, ok := err.(*runtimepkg.ProviderError)
			if !ok {
				t.Fatalf("error = %T (%v), want *runtime.ProviderError", err, err)
			}
			if providerErr.Code != testCase.code {
				t.Fatalf("code = %q, want %q", providerErr.Code, testCase.code)
			}
			if providerErr.Retryable != testCase.retries {
				t.Fatalf("retryable = %v, want %v", providerErr.Retryable, testCase.retries)
			}
			if providerErr.Extensions["openai.com/realtime/v1"] == nil {
				t.Fatal("error must retain the raw provider payload")
			}
		})
	}
}

// TestSTTSwallowsEmptyBufferErrorOnlyAfterStreamEndCommit: our own end-of-stream
// commit can hit an already-empty buffer, which errors benignly. Swallowing it
// unconditionally would hide a real mid-stream failure, so the guard is scoped
// to after the commit.
func TestSTTSwallowsEmptyBufferErrorOnlyAfterStreamEndCommit(t *testing.T) {
	t.Parallel()

	const frame = `{"type":"error","event_id":"e1","error":{"type":"invalid_request_error","code":"input_audio_buffer_commit_empty","message":"Error committing input audio buffer: the buffer is empty."}}`

	beforeCommit, cancelBefore := newHandlerOnlySTTStream()
	defer cancelBefore()
	if err := beforeCommit.handleMessage([]byte(frame)); err == nil {
		t.Fatal("an empty-buffer error before our commit is a real failure and must surface")
	}

	afterCommit, cancelAfter := newHandlerOnlySTTStream()
	defer cancelAfter()
	afterCommit.committed.Store(true)
	if err := afterCommit.handleMessage([]byte(frame)); err != nil {
		t.Fatalf("empty-buffer error after our own commit must be swallowed, got %v", err)
	}
}

// TestSTTCloseCommitsTrailingTurn: with turn detection off, a commit is the only
// thing that finalizes buffered audio. Closing without one drops the last turn.
func TestSTTCloseCommitsTrailingTurn(t *testing.T) {
	t.Parallel()

	handshakes := make(chan *http.Request, 1)
	frames := make(chan []byte, 4)
	server := newRealtimeServer(t, handshakes, frames, nil)
	defer server.Close()

	stream := openSTT(t, server.URL, nil)
	receiveFrame(t, frames) // session.update
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	var commit map[string]any
	if err := json.Unmarshal(receiveFrame(t, frames), &commit); err != nil {
		t.Fatalf("close frame is not JSON: %v", err)
	}
	if !reflect.DeepEqual(commit, map[string]any{"type": "input_audio_buffer.commit"}) {
		t.Fatalf("close sent %#v, want an input_audio_buffer.commit", commit)
	}
	if err := stream.WriteAudio(context.Background(), []byte{1}); err != runtimepkg.ErrSessionClosed {
		t.Fatalf("write after close = %v, want ErrSessionClosed", err)
	}
	_ = stream.Abort(context.Background())
}

// TestSTTRejectsMismatchedPlans covers every way a plan can be wrong for this
// adapter. Each rejection exists so a misrouted session fails at Open, where the
// runtime can fail over, rather than as an upstream error mid-call.
func TestSTTRejectsMismatchedPlans(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		mutate  func(*runtimepkg.AdapterRequest)
		wantSub string
	}{
		{
			name:    "wrong kind",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Kind = protocol.SessionKindTTS },
			wantSub: "stt sessions",
		},
		{
			name:    "another vendor's plan",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Provider = "deepgram" },
			wantSub: "cannot open provider",
		},
		{
			name:    "http transport",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Transport = protocol.TransportHTTP },
			wantSub: "websocket transport",
		},
		{
			name:    "unresolved model",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "auto" },
			wantSub: "concrete model",
		},
		{
			name:    "model the vendor does not list",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "gpt-4o-transcribe-turbo" },
			wantSub: "does not support model",
		},
		{
			name: "non-bearer credential",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Plan.Route.Credential = &protocol.DelegatedCredential{Kind: protocol.CredentialSignedURL, Value: "secret-that-must-not-leak"}
			},
			wantSub: "bearer credential",
		},
		{
			// relay_access is protocol.SessionPlan.Validate's label for a relay
			// credential; the same validation forbids it on provider_direct, so
			// the adapter refuses it there rather than treating it as a bearer
			// synonym.
			name: "relay_access credential off the relay route",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Plan.Route.Credential = &protocol.DelegatedCredential{Kind: protocol.CredentialRelayAccess, Value: "secret-that-must-not-leak"}
			},
			wantSub: "bearer credential",
		},
		{
			name:    "missing credential",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential = nil },
			wantSub: "bearer credential",
		},
		{
			name:    "no media",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Media = nil },
			wantSub: "media configuration",
		},
		{
			name: "stereo input",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Media = &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 2}
			},
			wantSub: "mono pcm_s16le",
		},
		{
			name: "opus input",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Media = &protocol.MediaFormat{Encoding: "opus", SampleRateHz: 24_000, Channels: 1}
			},
			wantSub: "mono pcm_s16le",
		},
		{
			// The Realtime PCM format enum has exactly one member, 24000. The
			// TypeScript provider declares max(rate, 24000), which is invalid above
			// 24 kHz; refusing is the honest behaviour.
			name: "48 kHz input",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Media = &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 48_000, Channels: 1}
			},
			wantSub: "24000 Hz sample rate",
		},
		{
			name: "16 kHz input",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Media = &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1}
			},
			wantSub: "24000 Hz sample rate",
		},
		{
			name:    "endpoint on another path",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = "ws://127.0.0.1:1/v1/audio/speech" },
			wantSub: "endpoint path must be /v1/realtime",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			adapter, err := NewSTT(STTConfig{AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			request := sttAdapterRequest("ws://127.0.0.1:1/v1/realtime")
			testCase.mutate(&request)
			_, err = adapter.Open(context.Background(), request)
			if err == nil {
				t.Fatal("expected the plan to be rejected")
			}
			if !strings.Contains(err.Error(), testCase.wantSub) {
				t.Fatalf("error = %q, want it to mention %q", err, testCase.wantSub)
			}
			// A rejection must never echo the credential it refused.
			if strings.Contains(err.Error(), "secret-that-must-not-leak") {
				t.Fatalf("rejection leaked the credential: %v", err)
			}
		})
	}
}

// TestSTTClassifiesHandshakeFailures: an upgrade that never completes has only a
// status to classify by, and the runtime routes on that code.
func TestSTTClassifiesHandshakeFailures(t *testing.T) {
	t.Parallel()

	cases := []struct {
		status  int
		code    string
		retries bool
	}{
		{status: http.StatusUnauthorized, code: "authentication_failed"},
		{status: http.StatusForbidden, code: "authentication_failed"},
		{status: http.StatusTooManyRequests, code: "provider_rate_limited", retries: true},
		{status: http.StatusBadRequest, code: "invalid_request"},
		{status: http.StatusServiceUnavailable, code: "provider_unavailable", retries: true},
	}

	for _, testCase := range cases {
		t.Run(http.StatusText(testCase.status), func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(testCase.status)
			}))
			defer server.Close()

			adapter, err := NewSTT(sttTestConfig(server.URL))
			if err != nil {
				t.Fatalf("new adapter: %v", err)
			}
			_, err = adapter.Open(context.Background(), sttAdapterRequest(sttEndpointFor(server.URL)))
			providerErr, ok := err.(*runtimepkg.ProviderError)
			if !ok {
				t.Fatalf("error = %T (%v), want *runtime.ProviderError", err, err)
			}
			if providerErr.Code != testCase.code {
				t.Fatalf("code = %q, want %q", providerErr.Code, testCase.code)
			}
			if providerErr.ProviderStatus != testCase.status {
				t.Fatalf("provider status = %d, want %d", providerErr.ProviderStatus, testCase.status)
			}
			if providerErr.Retryable != testCase.retries {
				t.Fatalf("retryable = %v, want %v", providerErr.Retryable, testCase.retries)
			}
		})
	}
}

// TestSTTRejectsSynthesisOperations: a transcriber that silently ignored text
// input would leave the runtime unable to report the mismatch.
func TestSTTRejectsSynthesisOperations(t *testing.T) {
	t.Parallel()

	stream, cancel := newHandlerOnlySTTStream()
	defer cancel()

	if err := stream.AppendText(context.Background(), "hello"); err != runtimepkg.ErrUnsupportedOperation {
		t.Fatalf("AppendText = %v", err)
	}
	if err := stream.CommitText(context.Background()); err != runtimepkg.ErrUnsupportedOperation {
		t.Fatalf("CommitText = %v", err)
	}
}

// TestSTTWarnsOnUnknownEventsButNotOnAcks keeps the ack frames every turn
// produces from generating a warning apiece, while still surfacing a genuinely
// unrecognized event so a protocol change is visible.
func TestSTTWarnsOnUnknownEventsButNotOnAcks(t *testing.T) {
	t.Parallel()

	stream, cancel := newHandlerOnlySTTStream()
	defer cancel()

	for _, ack := range []string{
		`{"type":"input_audio_buffer.committed","event_id":"e1","item_id":"i1"}`,
		`{"type":"input_audio_buffer.cleared","event_id":"e2"}`,
		`{"type":"conversation.item.created","event_id":"e3","item_id":"i1"}`,
	} {
		if err := stream.handleMessage([]byte(ack)); err != nil {
			t.Fatalf("ack %s: %v", ack, err)
		}
		select {
		case event := <-stream.events:
			t.Fatalf("ack produced an event: %+v", event)
		default:
		}
	}

	if err := stream.handleMessage([]byte(`{"type":"conversation.item.retention_policy.updated","event_id":"e4"}`)); err != nil {
		t.Fatalf("unknown event: %v", err)
	}
	select {
	case event := <-stream.events:
		if event.Type != protocol.EventWarning {
			t.Fatalf("unknown event produced %q, want a warning", event.Type)
		}
	default:
		t.Fatal("an unrecognized event must surface as a warning")
	}
}

// --- helpers -------------------------------------------------------------

func newRealtimeServer(
	t *testing.T,
	handshakes chan<- *http.Request,
	frames chan<- []byte,
	produce func(context.Context, *websocket.Conn),
) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/realtime" {
			http.NotFound(w, r)
			return
		}
		select {
		case handshakes <- r.Clone(r.Context()):
		default:
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		ctx := context.Background()
		go func() {
			defer conn.CloseNow()
			if produce != nil {
				produce(ctx, conn)
			}
			for {
				messageType, payload, err := conn.Read(ctx)
				if err != nil {
					return
				}
				if messageType != websocket.MessageText {
					continue
				}
				select {
				case frames <- append([]byte(nil), payload...):
				default:
				}
			}
		}()
	}))
}

func writeServerJSON(t *testing.T, ctx context.Context, conn *websocket.Conn, payload string) {
	t.Helper()
	if !json.Valid([]byte(payload)) {
		t.Fatalf("test fixture is not valid JSON: %s", payload)
	}
	if err := conn.Write(ctx, websocket.MessageText, []byte(payload)); err != nil {
		t.Errorf("write server frame: %v", err)
	}
}

type sttTestStream interface {
	runtimepkg.ProviderStream
	runtimepkg.AbortingProviderStream
}

func openSTT(t *testing.T, serverURL string, mutate func(*runtimepkg.AdapterRequest)) sttTestStream {
	t.Helper()
	adapter, err := NewSTT(sttTestConfig(serverURL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	request := sttAdapterRequest(sttEndpointFor(serverURL))
	if mutate != nil {
		mutate(&request)
	}
	opened, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	stream, ok := opened.(sttTestStream)
	if !ok {
		t.Fatalf("adapter stream %T does not implement AbortingProviderStream", opened)
	}
	return stream
}

// newHandlerOnlySTTStream builds a stream with no socket, for the message-
// mapping tests. handleMessage never touches the connection.
func newHandlerOnlySTTStream() (*sttStream, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	return &sttStream{ctx: ctx, cancel: cancel, events: make(chan runtimepkg.ProviderEvent, 8)}, cancel
}

func sttTestConfig(serverURL string) STTConfig {
	endpoint, _ := url.Parse(serverURL)
	return STTConfig{AllowedEndpointHosts: []string{endpoint.Hostname()}, AllowInsecureEndpoint: true}
}

func sttEndpointFor(serverURL string) string {
	endpoint, _ := url.Parse(serverURL)
	endpoint.Scheme = "ws"
	endpoint.Path = "/v1/realtime"
	return endpoint.String()
}

func sttAdapterRequest(endpoint string) runtimepkg.AdapterRequest {
	now := time.Date(2026, time.August, 1, 11, 59, 0, 0, time.UTC)
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindSTT,
		Plan: protocol.SessionPlan{
			PlanID: "plan_openai", SessionID: "sess_openai", AttemptID: "att_1",
			Execution: protocol.Execution{Placement: protocol.PlacementEmbedded, ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK},
			ExpiresAt: now.Add(time.Hour),
			Route: protocol.PlanRoute{
				Provider: "openai", Model: "gpt-live-transcribe", Adapter: STTAdapterID,
				Transport: protocol.TransportWebSocket, Endpoint: endpoint,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "customer-openai-key", ExpiresAt: now.Add(30 * time.Minute)},
			},
			Reservation: protocol.Reservation{
				ID: "res_openai", LeaseDurationSeconds: 60, LeaseExpiresAt: now.Add(time.Minute),
				Concurrency: protocol.ConcurrencyReservation{LeaseID: "conc_openai", Slots: 1},
				Usage:       protocol.UsageReservation{Unit: protocol.UsageUnitDurationSeconds, AuthorizedUnits: 60},
			},
			Requirements: protocol.Requirements{Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision, RuntimeVersion: "0.1.0"},
			Signature:    "test-signature",
		},
		Options: protocol.RequestOptions{Language: "en"},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1},
	}
}

func receiveRequest(t *testing.T, requests <-chan *http.Request) *http.Request {
	t.Helper()
	select {
	case request := <-requests:
		return request
	case <-time.After(3 * time.Second):
		t.Fatal("server never observed the websocket handshake")
		return nil
	}
}

func receiveFrame(t *testing.T, frames <-chan []byte) []byte {
	t.Helper()
	select {
	case frame := <-frames:
		return frame
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for a client frame")
		return nil
	}
}

func sessionTranscription(t *testing.T, frame []byte) map[string]any {
	t.Helper()
	var update struct {
		Session struct {
			Audio struct {
				Input struct {
					Transcription map[string]any `json:"transcription"`
				} `json:"input"`
			} `json:"audio"`
		} `json:"session"`
	}
	if err := json.Unmarshal(frame, &update); err != nil {
		t.Fatalf("session.update is not JSON: %v", err)
	}
	return update.Session.Audio.Input.Transcription
}

func collectSTT(t *testing.T, events <-chan runtimepkg.ProviderEvent, want int) []runtimepkg.ProviderEvent {
	t.Helper()
	collected := make([]runtimepkg.ProviderEvent, 0, want)
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for len(collected) < want {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatalf("provider events closed after %d events", len(collected))
			}
			if event.Err != nil {
				t.Fatalf("provider event error: %v", event.Err)
			}
			collected = append(collected, event)
		case <-timer.C:
			t.Fatalf("timed out after %d events", len(collected))
		}
	}
	return collected
}

func sttEventTypes(events []runtimepkg.ProviderEvent) []string {
	types := make([]string, len(events))
	for index, event := range events {
		types[index] = string(event.Type)
	}
	return types
}

func transcriptText(t *testing.T, event runtimepkg.ProviderEvent) string {
	t.Helper()
	var data struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(event.Data, &data); err != nil {
		t.Fatalf("decode transcript data: %v", err)
	}
	return data.Text
}

func usageRequestID(t *testing.T, event runtimepkg.ProviderEvent) string {
	t.Helper()
	var data struct {
		ProviderRequestID string `json:"provider_request_id"`
	}
	if err := json.Unmarshal(event.Data, &data); err != nil {
		t.Fatalf("decode usage data: %v", err)
	}
	return data.ProviderRequestID
}
