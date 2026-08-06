package xai

import (
	"context"
	"encoding/json"
	"fmt"
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

// Every xAI wire string in this file is typed as a literal, transcribed from
// https://docs.x.ai/stt-streaming.ws.json and the raw speech-to-text guide.
// Comparing against the adapter's own constants would keep a misspelled
// parameter or event name green — the assertion and the bug would agree.

// ---------------------------------------------------------------------------
// Handshake
// ---------------------------------------------------------------------------

// Pins the exact documented handshake: parameter names, their values, the
// Bearer header, and the path. This is the check that catches a renamed query
// parameter or a value xAI does not accept, because it reads the bytes the
// server actually received rather than the adapter's view of them.
func TestSTTHandshakeSendsDocumentedQueryParameters(t *testing.T) {
	t.Parallel()

	requests := make(chan *http.Request, 1)
	server := newSTTServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		requests <- request.Clone(request.Context())
		_, _, _ = conn.Read(ctx)
	})
	defer server.Close()

	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	if adapter.ID() != "xai.stt.v1" {
		t.Fatalf("adapter id = %q, want xai.stt.v1", adapter.ID())
	}
	stream, err := adapter.Open(context.Background(), sttAdapterRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer abortStream(t, stream)

	select {
	case received := <-requests:
		if received.URL.Path != "/v1/stt" {
			t.Fatalf("path = %q, want /v1/stt", received.URL.Path)
		}
		query := received.URL.Query()
		for parameter, want := range map[string]string{
			"sample_rate": "16000",
			"encoding":    "pcm",
			// interim_results defaults to false upstream, which would suppress
			// every transcript.delta the canonical protocol promises.
			"interim_results": "true",
			"channels":        "1",
			// The region subtag must survive. The platform's TypeScript adapter
			// truncates it, and a tag the caller chose is not the adapter's to
			// rewrite.
			"language": "pt-BR",
		} {
			if got := query.Get(parameter); got != want {
				t.Errorf("query %s = %q, want %q", parameter, got, want)
			}
		}
		// xAI's transcription API has no model field on either surface. Sending
		// the plan's Speko catalog key would be inventing a parameter.
		if got := query.Get("model"); got != "" {
			t.Errorf("query model = %q, want it absent", got)
		}
		// endpointing has no documented "off" value (0 means "fire on any VAD
		// boundary", not "never"), so the adapter must not guess one.
		if got := query.Get("endpointing"); got != "" {
			t.Errorf("query endpointing = %q, want it absent", got)
		}
		if got := received.Header.Get("Authorization"); got != "Bearer customer-xai-key" {
			t.Errorf("Authorization = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("server never observed the websocket handshake")
	}
}

// xAI documents ONE authentication channel for transcription, and its ephemeral
// client secret is scoped to the Speech-to-Speech API, never to STT. So a
// managed plan and a BYOK plan must be byte-identical on the wire apart from
// the token itself. A split here would be an invented wire detail.
func TestSTTCredentialSourcesShareTheBearerHeader(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name   string
		source protocol.CredentialSource
		token  string
	}{
		{name: "byok permanent key", source: protocol.CredentialsBYOK, token: "customer-xai-key"},
		{name: "managed short lived token", source: protocol.CredentialsManaged, token: "managed-xai-token"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			requests := make(chan *http.Request, 1)
			server := newSTTServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
				requests <- request.Clone(request.Context())
				_, _, _ = conn.Read(ctx)
			})
			defer server.Close()

			adapter, err := NewSTT(sttTestConfig(server.URL))
			if err != nil {
				t.Fatalf("new STT adapter: %v", err)
			}
			request := sttAdapterRequest(server.URL)
			request.Plan.Execution.CredentialSource = testCase.source
			request.Plan.Route.Credential.Value = testCase.token
			stream, err := adapter.Open(context.Background(), request)
			if err != nil {
				t.Fatalf("open stream: %v", err)
			}
			defer abortStream(t, stream)

			select {
			case received := <-requests:
				if got := received.Header.Get("Authorization"); got != "Bearer "+testCase.token {
					t.Fatalf("Authorization = %q", got)
				}
				if strings.Contains(received.URL.RawQuery, testCase.token) {
					t.Fatal("the access token must never appear in the request URL")
				}
				// The xai-client-secret. subprotocol channel exists only because
				// browsers cannot set headers; a server-side gateway must not use it.
				if got := received.Header.Get("Sec-Websocket-Protocol"); strings.Contains(got, "xai-client-secret") {
					t.Fatalf("Sec-Websocket-Protocol = %q", got)
				}
			case <-time.After(time.Second):
				t.Fatal("server never observed the websocket handshake")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Cumulative partials — the reason this adapter exists in the shape it does
// ---------------------------------------------------------------------------

// The core regression test. xAI restates the whole utterance on every finality
// tier: chunk finals carry the transcript-so-far, the speech_final frame is the
// "complete stitched utterance", and transcript.done restates it once more,
// ITN-formatted. A consumer that appends every is_final frame gets the sentence
// three times over — that is the ~100% word error rate this provider produced
// on the platform before the equivalent fix. Exactly one transcript.final may
// leave this adapter per turn, and it must be the speech_final text, not a
// concatenation and not the done restatement.
func TestSTTCumulativePartialsProduceExactlyOneFinal(t *testing.T) {
	t.Parallel()

	server := newSTTServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		for _, frame := range []map[string]any{
			{"type": "transcript.created", "id": "xai-stt-session"},
			{"type": "transcript.partial", "text": "después del", "is_final": false, "speech_final": false, "start": 0.0, "duration": 0.6},
			{"type": "transcript.partial", "text": "después del accidente falleció", "is_final": true, "speech_final": false, "start": 0.0, "duration": 1.8},
			{"type": "transcript.partial", "text": "después del accidente falleció al poco tiempo", "is_final": true, "speech_final": true, "start": 0.0, "duration": 3.2},
			{"type": "transcript.done", "text": "Después del accidente falleció al poco tiempo.", "duration": 3.4},
		} {
			if err := writeSTTFrame(ctx, conn, frame); err != nil {
				t.Errorf("write %v: %v", frame["type"], err)
				return
			}
		}
		_, _, _ = conn.Read(ctx)
	})
	defer server.Close()

	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), sttAdapterRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	events := drainSTTEvents(t, stream.Events())

	finals := sttTextsOfType(events, protocol.EventTranscriptFinal)
	if len(finals) != 1 {
		t.Fatalf("transcript.final count = %d (%q), want exactly one per turn", len(finals), finals)
	}
	if finals[0] != "después del accidente falleció al poco tiempo" {
		t.Fatalf("final text = %q, want the speech_final utterance", finals[0])
	}
	// The two earlier cumulative frames must reach the caller as interims so a
	// live UI can render them, but they must not finalize.
	deltas := sttTextsOfType(events, protocol.EventTranscriptDelta)
	if len(deltas) != 2 || deltas[0] != "después del" || deltas[1] != "después del accidente falleció" {
		t.Fatalf("deltas = %q, want the two cumulative pre-final frames", deltas)
	}
	// The failure mode this guards is textual, so assert on the text: the
	// doubling looked like "...poco tiempo Después del accidente...".
	for _, text := range append(deltas, finals...) {
		if strings.Count(strings.ToLower(text), "después del") > 1 {
			t.Fatalf("text %q repeats the utterance — partials were concatenated, not replaced", text)
		}
	}
	// transcript.done merely restated a turn speech_final already closed, so it
	// must not appear at all.
	for _, text := range finals {
		if text == "Después del accidente falleció al poco tiempo." {
			t.Fatal("the transcript.done restatement was emitted as a second final")
		}
	}
	sttAssertOrder(t, events, []protocol.EventType{
		protocol.EventUsageObserved,   // transcript.created
		protocol.EventSpeechStarted,   // derived: xAI publishes no speech-start event
		protocol.EventTranscriptDelta, // interim
		protocol.EventTranscriptDelta, // chunk final, still cumulative
		protocol.EventTranscriptFinal, // speech_final: the one canonical final
		protocol.EventSpeechEnded,
		protocol.EventUsageObserved, // transcript.done duration
	})
}

// xAI's own documented transcript.done example carries text:"" with a non-zero
// duration. When a turn never reached an endpoint boundary that empty done is
// the only signal left, and the newest cumulative partial IS the utterance —
// dropping it turns a good turn into an empty result.
func TestSTTEmptyDoneFallsBackToNewestCumulativePartial(t *testing.T) {
	t.Parallel()

	events := runSTTFrames(t, []map[string]any{
		{"type": "transcript.created", "id": "xai-stt-session"},
		{"type": "transcript.partial", "text": "However", "is_final": true, "speech_final": false, "start": 0.0, "duration": 0.4},
		{"type": "transcript.partial", "text": "However, due to the slow channels", "is_final": true, "speech_final": false, "start": 0.0, "duration": 1.5},
		{"type": "transcript.partial", "text": "However, due to the slow channels, styles could lag.", "is_final": true, "speech_final": false, "start": 0.0, "duration": 2.9},
		{"type": "transcript.done", "text": "", "duration": 6.43},
	})

	finals := sttTextsOfType(events, protocol.EventTranscriptFinal)
	if len(finals) != 1 {
		t.Fatalf("transcript.final count = %d (%q), want one", len(finals), finals)
	}
	// The LAST cumulative partial wins. Concatenating would produce
	// "However However, due to the slow channels However, due to ...".
	if finals[0] != "However, due to the slow channels, styles could lag." {
		t.Fatalf("final text = %q, want the newest cumulative partial", finals[0])
	}
}

// When no speech_final ever fired, transcript.done is the authoritative flush
// and must be emitted — this is the case the de-duplication must NOT swallow.
func TestSTTDoneIsTheFinalWhenNoSpeechFinalFired(t *testing.T) {
	t.Parallel()

	events := runSTTFrames(t, []map[string]any{
		{"type": "transcript.created", "id": "xai-stt-session"},
		{"type": "transcript.partial", "text": "hola", "is_final": false, "speech_final": false, "start": 0.0, "duration": 0.3},
		{"type": "transcript.partial", "text": "hola mundo", "is_final": true, "speech_final": false, "start": 0.0, "duration": 0.9},
		{"type": "transcript.done", "text": "Hola mundo.", "duration": 1.2},
	})

	finals := sttTextsOfType(events, protocol.EventTranscriptFinal)
	if len(finals) != 1 || finals[0] != "Hola mundo." {
		t.Fatalf("finals = %q, want exactly the transcript.done text", finals)
	}
}

// A socket that closes before transcript.done still holds the newest cumulative
// text of the open turn. Losing it is an empty transcript for a turn the model
// actually heard, so the adapter emits it on the way out.
func TestSTTSocketCloseBeforeDoneStillEmitsTheTurn(t *testing.T) {
	t.Parallel()

	// No transcript.done — the server closes the session normally instead.
	events := runSTTFrames(t, []map[string]any{
		{"type": "transcript.created", "id": "xai-stt-session"},
		{"type": "transcript.partial", "text": "hola mundo", "is_final": true, "speech_final": false, "start": 0.0, "duration": 0.9},
	})

	finals := sttTextsOfType(events, protocol.EventTranscriptFinal)
	if len(finals) != 1 || finals[0] != "hola mundo" {
		t.Fatalf("finals = %q, want the pending turn recovered on close", finals)
	}
}

// An abruptly dropped connection is a genuine provider failure AND still holds
// a transcript xAI already produced. The event carrying Err terminates the
// attempt, so the salvaged transcript has to be emitted before it or the caller
// never sees the words that were actually recognised.
func TestSTTAbruptDropSalvagesTheTurnBeforeFailing(t *testing.T) {
	t.Parallel()

	server := newSTTServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		for _, frame := range []map[string]any{
			{"type": "transcript.created", "id": "xai-stt-session"},
			{"type": "transcript.partial", "text": "hola mundo", "is_final": true, "speech_final": false, "start": 0.0, "duration": 0.9},
		} {
			if err := writeSTTFrame(ctx, conn, frame); err != nil {
				t.Errorf("write %v: %v", frame["type"], err)
				return
			}
		}
		// Returning runs the server's CloseNow: no close frame, no audio.done.
	})
	defer server.Close()

	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), sttAdapterRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer abortStream(t, stream)

	collected := make([]runtimepkg.ProviderEvent, 0, 8)
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-stream.Events():
			if !ok {
				t.Fatalf("events closed without a provider error: %v", eventTypes(collected))
			}
			if event.Err == nil {
				collected = append(collected, event)
				continue
			}
			assertProviderError(t, event.Err, "provider_unavailable", true, 0)
			finals := sttTextsOfType(collected, protocol.EventTranscriptFinal)
			if len(finals) != 1 || finals[0] != "hola mundo" {
				t.Fatalf("finals before the failure = %q, want the salvaged turn", finals)
			}
			return
		case <-timer.C:
			t.Fatalf("timed out after %v", eventTypes(collected))
		}
	}
}

// ---------------------------------------------------------------------------
// Client messages
// ---------------------------------------------------------------------------

// CommitAudio and Close are the only two control messages xAI documents for
// transcription, and they are not interchangeable: Finalize closes the current
// utterance and keeps the session, audio.done ends the session. Getting either
// name wrong is silent — the server answers with an error frame at best.
func TestSTTCommitSendsFinalizeAndCloseSendsAudioDone(t *testing.T) {
	t.Parallel()

	messages := make(chan map[string]any, 4)
	audio := make(chan []byte, 4)
	server := newSTTServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		if err := writeSTTFrame(ctx, conn, map[string]any{"type": "transcript.created", "id": "xai-stt-session"}); err != nil {
			t.Errorf("write created: %v", err)
			return
		}
		for {
			messageType, payload, err := conn.Read(ctx)
			if err != nil {
				return
			}
			if messageType == websocket.MessageBinary {
				audio <- append([]byte(nil), payload...)
				continue
			}
			var message map[string]any
			if err := json.Unmarshal(payload, &message); err != nil {
				t.Errorf("client json: %v", err)
				return
			}
			messages <- message
		}
	})
	defer server.Close()

	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), sttAdapterRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.WriteAudio(context.Background(), []byte{0x01, 0x02, 0x03, 0x04}); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	if err := stream.CommitAudio(context.Background()); err != nil {
		t.Fatalf("commit audio: %v", err)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Raw binary, never base64: xAI's schema types the audio frame as
	// {"type":"string","format":"binary"} and the guide says "send raw bytes
	// directly".
	if frame := sttNextAudio(t, audio); string(frame) != "\x01\x02\x03\x04" {
		t.Fatalf("audio frame = %q, want the raw bytes", frame)
	}
	if message := sttNextMessage(t, messages); message["type"] != "Finalize" {
		t.Fatalf("commit message = %v, want type Finalize", message)
	}
	if message := sttNextMessage(t, messages); message["type"] != "audio.done" {
		t.Fatalf("close message = %v, want type audio.done", message)
	}
	abortStream(t, stream)
}

// The guide is emphatic in bold: "Wait for transcript.created before sending
// audio — the server needs to initialize its ASR backend." Nothing upstream of
// the adapter provides that gate (runtime.Engine emits session.ready as soon as
// Open returns), so audio produced early must be held and then replayed IN
// ORDER. Sending it early loses the front of the utterance; reordering it
// scrambles the whole turn.
func TestSTTHoldsAudioUntilTranscriptCreated(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	received := make(chan []byte, 8)
	server := newSTTServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		go func() {
			for {
				messageType, payload, err := conn.Read(ctx)
				if err != nil {
					return
				}
				if messageType == websocket.MessageBinary {
					received <- append([]byte(nil), payload...)
				}
			}
		}()
		<-release
		if err := writeSTTFrame(ctx, conn, map[string]any{"type": "transcript.created", "id": "xai-stt-session"}); err != nil {
			t.Errorf("write created: %v", err)
			return
		}
		<-ctx.Done()
	})
	defer server.Close()

	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), sttAdapterRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer abortStream(t, stream)

	for _, frame := range [][]byte{{0x11, 0x22}, {0x33, 0x44}, {0x55, 0x66}} {
		if err := stream.WriteAudio(context.Background(), frame); err != nil {
			t.Fatalf("write audio %x: %v", frame, err)
		}
	}
	// An adapter that writes straight through does so synchronously inside
	// WriteAudio, so anything on the wire now is a real violation.
	select {
	case frame := <-received:
		t.Fatalf("audio %x reached xAI before transcript.created", frame)
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	for _, want := range []string{"\x11\x22", "\x33\x44", "\x55\x66"} {
		if got := sttNextAudio(t, received); string(got) != want {
			t.Fatalf("flushed frame = %q, want %q in the order it was written", got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

// A rejected upgrade is where an expired managed token, an exhausted quota and
// a dead backend all surface, and the runtime's retry decision hangs on telling
// them apart. xAI documents 400, 401, 413, 429, 502 and 503 for transcription.
func TestSTTHandshakeStatusMapsToDistinctProviderErrorCodes(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		status    int
		code      string
		retryable bool
	}{
		{status: http.StatusBadRequest, code: "invalid_request", retryable: false},
		{status: http.StatusUnauthorized, code: "authentication_failed", retryable: false},
		{status: http.StatusForbidden, code: "authentication_failed", retryable: false},
		{status: http.StatusTooManyRequests, code: "provider_rate_limited", retryable: true},
		{status: http.StatusBadGateway, code: "provider_unavailable", retryable: true},
		{status: http.StatusServiceUnavailable, code: "provider_unavailable", retryable: true},
	} {
		t.Run(fmt.Sprintf("status %d", testCase.status), func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(testCase.status)
			}))
			defer server.Close()

			adapter, err := NewSTT(sttTestConfig(server.URL))
			if err != nil {
				t.Fatalf("new STT adapter: %v", err)
			}
			_, err = adapter.Open(context.Background(), sttAdapterRequest(server.URL))
			assertProviderError(t, err, testCase.code, testCase.retryable, testCase.status)
		})
	}
}

// The in-band error frame carries only `message` — no status, no code — so it
// cannot be classified further, and the docs say most errors close the socket
// anyway. It must terminate the attempt with the vendor text preserved, not be
// swallowed as a warning.
func TestSTTInBandErrorFrameTerminatesTheAttempt(t *testing.T) {
	t.Parallel()

	server := newSTTServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		if err := writeSTTFrame(ctx, conn, map[string]any{
			"type": "error", "message": "Invalid message: expected {\"type\": \"audio.done\"}",
		}); err != nil {
			t.Errorf("write error frame: %v", err)
			return
		}
		_, _, _ = conn.Read(ctx)
	})
	defer server.Close()

	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), sttAdapterRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer abortStream(t, stream)

	select {
	case event := <-stream.Events():
		providerError := assertProviderError(t, event.Err, "provider_unavailable", false, 0)
		if !strings.Contains(providerError.Message, "Invalid message") {
			t.Fatalf("message = %q, want xAI's own text preserved", providerError.Message)
		}
		if providerError.Extensions[extensionID] == nil {
			t.Fatal("the raw xAI error frame was not retained on the extension")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the provider error")
	}
}

// Open is the last place a malformed or mis-routed plan can be stopped before a
// customer credential is put on the wire, so every documented constraint is
// checked there rather than trusted.
func TestSTTOpenRejectsInvalidRequests(t *testing.T) {
	t.Parallel()

	server := newSTTServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		_, _, _ = conn.Read(ctx)
	})
	defer server.Close()

	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}

	for _, testCase := range []struct {
		name    string
		mutate  func(*runtimepkg.AdapterRequest)
		wantSub string
	}{
		{
			name:    "a tts plan routed to the stt adapter",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Kind = protocol.SessionKindTTS },
			wantSub: "stt sessions",
		},
		{
			name:    "another vendor's plan",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Provider = "deepgram" },
			wantSub: "cannot open provider",
		},
		{
			// The batch POST /v1/stt surface is a multipart file upload, not an
			// incremental stream, so it is deliberately not wired.
			name:    "http transport",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Transport = protocol.TransportHTTP },
			wantSub: "websocket transport",
		},
		{
			name:    "an unresolved auto model",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "auto" },
			wantSub: "concrete model",
		},
		{
			name:    "no model at all",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "  " },
			wantSub: "concrete model",
		},
		{
			name:    "no media",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Media = nil },
			wantSub: "media configuration",
		},
		{
			// The canonical protocol admits opus; xAI's encoding enum is
			// pcm|mulaw|alaw and has no opus member.
			name: "opus audio",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Media = &protocol.MediaFormat{Encoding: "opus", SampleRateHz: 16_000, Channels: 1}
			},
			wantSub: "media encoding",
		},
		{
			// Multichannel is documented but changes the terminal contract to one
			// transcript.done PER CHANNEL, which this adapter does not model.
			name: "stereo audio",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Media = &protocol.MediaFormat{Encoding: pcmEncoding, SampleRateHz: 16_000, Channels: 2}
			},
			wantSub: "mono audio",
		},
		{
			// 11025 Hz is inside the protocol's own bounds but outside xAI's
			// published enum, so only an adapter-side check catches it.
			name: "a sample rate xAI does not publish",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Media = &protocol.MediaFormat{Encoding: pcmEncoding, SampleRateHz: 11_025, Channels: 1}
			},
			wantSub: "sample rate",
		},
		{
			name:    "no credential",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential = nil },
			wantSub: "bearer credential",
		},
		{
			name: "a signed-url credential",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Plan.Route.Credential.Kind = protocol.CredentialSignedURL
			},
			wantSub: "bearer credential",
		},
		{
			name:    "an empty credential",
			mutate:  func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential.Value = "   " },
			wantSub: "bearer credential",
		},
		{
			name: "an unknown credential source",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Plan.Execution.CredentialSource = protocol.CredentialSource("delegated")
			},
			wantSub: "credential source",
		},
		{
			name: "an endpoint on another host",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Plan.Route.Endpoint = "ws://transcribe.evil.test/v1/stt"
			},
			wantSub: "endpoint",
		},
		{
			name: "an endpoint on the wrong path",
			mutate: func(r *runtimepkg.AdapterRequest) {
				r.Plan.Route.Endpoint = strings.Replace(r.Plan.Route.Endpoint, "/v1/stt", "/v1/tts", 1)
			},
			wantSub: "endpoint path must be /v1/stt",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			request := sttAdapterRequest(server.URL)
			testCase.mutate(&request)
			stream, err := adapter.Open(context.Background(), request)
			if err == nil {
				_ = stream.Close(context.Background())
				t.Fatalf("open succeeded, want a rejection mentioning %q", testCase.wantSub)
			}
			if !strings.Contains(err.Error(), testCase.wantSub) {
				t.Fatalf("error = %q, want it to mention %q", err, testCase.wantSub)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Contract edges
// ---------------------------------------------------------------------------

// Transcription is audio-in only. The text half of ProviderStream must report
// the shared sentinel so the runtime can distinguish "this adapter cannot" from
// "this call failed", and an empty audio frame is a caller bug, not a wire
// message xAI has any use for.
func TestSTTTextInputIsUnsupported(t *testing.T) {
	t.Parallel()

	server := newSTTServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		_, _, _ = conn.Read(ctx)
	})
	defer server.Close()

	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), sttAdapterRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer abortStream(t, stream)

	if err := stream.AppendText(context.Background(), "not a transcription input"); err != runtimepkg.ErrUnsupportedOperation {
		t.Errorf("AppendText = %v, want ErrUnsupportedOperation", err)
	}
	if err := stream.CommitText(context.Background()); err != runtimepkg.ErrUnsupportedOperation {
		t.Errorf("CommitText = %v, want ErrUnsupportedOperation", err)
	}
	if err := stream.WriteAudio(context.Background(), nil); err == nil {
		t.Error("WriteAudio accepted an empty frame")
	}
}

// transcript.created is the only correlation handle a transcription session
// gets, so its id has to reach telemetry; and a frame type xAI adds later must
// surface as a warning rather than vanish.
func TestSTTSessionIdIsReportedAndUnknownFramesWarn(t *testing.T) {
	t.Parallel()

	events := runSTTFrames(t, []map[string]any{
		{"type": "transcript.created", "id": "83f2f6fd-1cd1-4747-bc52-cebddc961c32"},
		{"type": "transcript.heartbeat"},
		{"type": "transcript.partial", "text": "one", "is_final": true, "speech_final": true, "start": 0.0, "duration": 0.5},
		{"type": "transcript.done", "text": "", "duration": 0.5},
	})

	sttAssertOrder(t, events, []protocol.EventType{
		protocol.EventUsageObserved,
		protocol.EventWarning,
		protocol.EventSpeechStarted,
		protocol.EventTranscriptFinal,
		protocol.EventSpeechEnded,
		protocol.EventUsageObserved,
	})
	var usage struct {
		ProviderRequestID string `json:"provider_request_id"`
	}
	if err := json.Unmarshal(events[0].Data, &usage); err != nil {
		t.Fatalf("usage data: %v", err)
	}
	if usage.ProviderRequestID != "83f2f6fd-1cd1-4747-bc52-cebddc961c32" {
		t.Fatalf("provider_request_id = %q, want the transcript.created id", usage.ProviderRequestID)
	}
	// Every later event is tagged with the same id, and the final carries the
	// documented timing fields.
	var final struct {
		Text              string `json:"text"`
		IsFinal           bool   `json:"is_final"`
		SpeechFinal       bool   `json:"speech_final"`
		AudioStartMS      int64  `json:"audio_start_ms"`
		AudioEndMS        int64  `json:"audio_end_ms"`
		ProviderRequestID string `json:"provider_request_id"`
	}
	if err := json.Unmarshal(events[3].Data, &final); err != nil {
		t.Fatalf("final data: %v", err)
	}
	if final.Text != "one" || !final.IsFinal || !final.SpeechFinal || final.AudioEndMS != 500 || final.ProviderRequestID != "83f2f6fd-1cd1-4747-bc52-cebddc961c32" {
		t.Fatalf("final transcript data = %+v", final)
	}
	if events[3].Extensions[extensionID] == nil {
		t.Fatal("the raw xAI frame was not retained on the final transcript")
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newSTTServer(t *testing.T, callback func(context.Context, *http.Request, *websocket.Conn)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/stt" {
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

func writeSTTFrame(ctx context.Context, conn *websocket.Conn, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, payload)
}

func sttTestConfig(serverURL string) STTConfig {
	endpoint, _ := url.Parse(serverURL)
	return STTConfig{AllowedEndpointHosts: []string{endpoint.Hostname()}, AllowInsecureEndpoint: true}
}

// sttAdapterRequest builds a valid streaming transcription plan. The language
// is deliberately region-qualified so any truncation shows up on the wire.
func sttAdapterRequest(serverURL string) runtimepkg.AdapterRequest {
	now := time.Date(2026, time.August, 1, 11, 59, 0, 0, time.UTC)
	endpoint, err := url.Parse(serverURL)
	if err == nil && endpoint.Host != "" {
		endpoint.Scheme = "ws"
		endpoint.Path = "/v1/stt"
		endpoint.RawQuery = ""
		serverURL = endpoint.String()
	}
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindSTT,
		Plan: protocol.SessionPlan{
			PlanID: "plan_xai_stt", SessionID: "sess_xai_stt", AttemptID: "att_1",
			Execution: protocol.Execution{Placement: protocol.PlacementEmbedded, ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK},
			ExpiresAt: now.Add(time.Hour),
			Route: protocol.PlanRoute{
				Provider: "xai", Model: STTDefaultModel, Adapter: STTAdapterID, Transport: protocol.TransportWebSocket, Endpoint: serverURL,
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "customer-xai-key", ExpiresAt: now.Add(30 * time.Minute)},
			},
			Reservation: protocol.Reservation{
				ID: "res_xai_stt", LeaseDurationSeconds: 60, LeaseExpiresAt: now.Add(time.Minute),
				RenewalURL:  "https://control.speko.test/v1/sessions/sess_xai_stt/lease-renewals",
				Concurrency: protocol.ConcurrencyReservation{LeaseID: "conc_xai_stt", Slots: 1},
				Usage:       protocol.UsageReservation{Unit: protocol.UsageUnitDurationSeconds, AuthorizedUnits: 600},
			},
			Telemetry:    protocol.Telemetry{Endpoint: "https://control.speko.test/v1/runtime-events", Token: "telemetry-token", FlushIntervalMS: 5_000},
			Requirements: protocol.Requirements{Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision, RuntimeVersion: "0.1.0"},
			Signature:    "test-signature",
		},
		Options: protocol.RequestOptions{Language: "pt-BR"},
		Media:   &protocol.MediaFormat{Encoding: pcmEncoding, SampleRateHz: 16_000, Channels: 1},
	}
}

// runSTTFrames replays a documented server-message sequence and returns every
// event the adapter produced, up to and including the channel closing.
func runSTTFrames(t *testing.T, frames []map[string]any) []runtimepkg.ProviderEvent {
	t.Helper()
	server := newSTTServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		for _, frame := range frames {
			if err := writeSTTFrame(ctx, conn, frame); err != nil {
				t.Errorf("write %v: %v", frame["type"], err)
				return
			}
		}
		// xAI closes the connection after transcript.done, so the fake does the
		// same: a graceful close is the documented end of a session, not a fault.
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	defer server.Close()

	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new STT adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), sttAdapterRequest(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	return drainSTTEvents(t, stream.Events())
}

func drainSTTEvents(t *testing.T, events <-chan runtimepkg.ProviderEvent) []runtimepkg.ProviderEvent {
	t.Helper()
	collected := make([]runtimepkg.ProviderEvent, 0, 8)
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-events:
			if !ok {
				return collected
			}
			if event.Err != nil {
				t.Fatalf("provider event error: %v", event.Err)
			}
			collected = append(collected, event)
		case <-timer.C:
			t.Fatalf("timed out after %d events: %v", len(collected), eventTypes(collected))
			return collected
		}
	}
}

func sttTextsOfType(events []runtimepkg.ProviderEvent, kind protocol.EventType) []string {
	texts := make([]string, 0, len(events))
	for _, event := range events {
		if event.Type != kind {
			continue
		}
		var payload struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(event.Data, &payload); err != nil {
			continue
		}
		texts = append(texts, payload.Text)
	}
	return texts
}

func sttAssertOrder(t *testing.T, events []runtimepkg.ProviderEvent, want []protocol.EventType) {
	t.Helper()
	got := eventTypes(events)
	if len(got) != len(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for index := range want {
		if events[index].Type != want[index] {
			t.Fatalf("event %d = %q, want %q (full sequence %v)", index, events[index].Type, want[index], got)
		}
	}
}

func sttNextMessage(t *testing.T, messages <-chan map[string]any) map[string]any {
	t.Helper()
	select {
	case message := <-messages:
		return message
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a client message")
		return nil
	}
}

func sttNextAudio(t *testing.T, frames <-chan []byte) []byte {
	t.Helper()
	select {
	case frame := <-frames:
		return frame
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for an audio frame")
		return nil
	}
}
