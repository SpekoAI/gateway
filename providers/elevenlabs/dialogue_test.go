package elevenlabs

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

// The wire contract, asserted end to end: the handshake registers exactly one
// voice and carries the key, text rides an `inputs` list with `new_turn` false,
// CommitText flushes without closing, and Close sends `close_socket`. The v4
// pair runs the same flow: it streams only on this socket, so a regression in
// its session (not just its URL) must fail here.
func TestDialogueAdapterRegistersOneVoiceAndFlushesWithoutClosing(t *testing.T) {
	t.Parallel()
	for _, model := range []string{"eleven_v3_conversational", "eleven_v4", "eleven_v4_turbo"} {
		t.Run(model, func(t *testing.T) {
			t.Parallel()
			requests := make(chan *http.Request, 1)
			server := newDialogueServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
				requests <- request.Clone(request.Context())
				handshake, err := readDialogueMessage(ctx, conn)
				if err != nil || len(handshake.Voices) != 1 || handshake.Voices[0] != "voice_123" || handshake.APIKey == "" {
					t.Errorf("handshake = %+v, err=%v", handshake, err)
					return
				}
				first, err := readDialogueMessage(ctx, conn)
				if err != nil || len(first.Inputs) != 1 || first.Inputs[0].Text != "Hello, " || first.Inputs[0].VoiceID != "voice_123" || first.Inputs[0].NewTurn {
					t.Errorf("first input = %+v, err=%v", first, err)
					return
				}
				second, err := readDialogueMessage(ctx, conn)
				if err != nil || len(second.Inputs) != 1 || second.Inputs[0].Text != "world." {
					t.Errorf("second input = %+v, err=%v", second, err)
					return
				}
				flush, err := readDialogueMessage(ctx, conn)
				if err != nil || !flush.Flush || len(flush.Inputs) != 0 || flush.CloseSocket {
					t.Errorf("flush = %+v, err=%v", flush, err)
					return
				}
				if err := writeServerJSON(ctx, conn, map[string]any{"audio": base64.StdEncoding.EncodeToString([]byte{1, 2, 3})}); err != nil {
					t.Errorf("write audio: %v", err)
					return
				}
				if err := writeServerJSON(ctx, conn, map[string]any{"is_final": true}); err != nil {
					t.Errorf("write final: %v", err)
					return
				}
				closeSocket, err := readDialogueMessage(ctx, conn)
				if err != nil || !closeSocket.CloseSocket {
					t.Errorf("close socket = %+v, err=%v", closeSocket, err)
					return
				}
				_ = conn.Close(websocket.StatusNormalClosure, "")
			})
			defer server.Close()

			adapter, err := NewDialogue(testConfig(server.URL))
			if err != nil {
				t.Fatalf("new dialogue adapter: %v", err)
			}
			stream, err := adapter.Open(context.Background(), dialogueRequest(server.URL, model))
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			if err := stream.AppendText(context.Background(), "Hello, "); err != nil {
				t.Fatalf("append: %v", err)
			}
			if err := stream.AppendText(context.Background(), "world."); err != nil {
				t.Fatalf("second append: %v", err)
			}
			if err := stream.CommitText(context.Background()); err != nil {
				t.Fatalf("commit: %v", err)
			}
			events := collectEvents(t, stream.Events(), 3)
			if got := eventTypes(events); strings.Join(got, ",") != "audio.started,audio.frame,audio.done" {
				t.Fatalf("event types = %v", got)
			}
			if err := stream.Close(context.Background()); err != nil {
				t.Fatalf("close: %v", err)
			}

			request := <-requests
			query := request.URL.Query()
			if query.Get("model_id") != model {
				t.Fatalf("model_id = %q", query.Get("model_id"))
			}
			if query.Get("output_format") != "pcm_16000" {
				t.Fatalf("output_format = %q", query.Get("output_format"))
			}
			// Neither parameter is documented on this endpoint; sending one is a 400
			// risk after admission has already reserved credit.
			if query.Has("sync_alignment") || query.Has("language_code") {
				t.Fatalf("undocumented dialogue query parameters sent: %v", query)
			}
			if request.Header.Get("xi-api-key") == "" {
				t.Fatal("xi-api-key header missing")
			}
		})
	}
}

// A model outside the v3/v4 families must never reach this socket: it is the
// text-to-speech adapter's job, and the vendor answers it here with HTTP 403,
// so the split has to fail loudly rather than dial a path that will refuse it.
func TestDialogueAdapterServesOnlyDialogueFamilies(t *testing.T) {
	t.Parallel()
	adapter, err := NewDialogue(testConfig("http://127.0.0.1:1"))
	if err != nil {
		t.Fatalf("new dialogue adapter: %v", err)
	}
	for _, model := range []string{"eleven_flash_v2_5", "eleven_multilingual_v2", "eleven_turbo_v2_5", "auto", ""} {
		if ServesModel(model) {
			t.Fatalf("ServesModel(%q) = true, want false", model)
		}
		_, err := adapter.Open(context.Background(), dialogueRequest("http://127.0.0.1:1", model))
		if err == nil {
			t.Fatalf("open accepted %q", model)
		}
		if strings.Contains(err.Error(), "customer-elevenlabs-key") {
			t.Fatalf("error leaked the credential: %v", err)
		}
	}
	for _, model := range []string{"eleven_v3", "eleven_v3_conversational", "eleven_v4", "eleven_v4_turbo"} {
		if !ServesModel(model) {
			t.Fatalf("ServesModel(%q) = false, want true", model)
		}
	}
}

// v4 streams ONLY on this socket: the text-to-speech socket answers
// eleven_v4 and eleven_v4_turbo with HTTP 400 unsupported_model, "Use the
// text-to-dialogue websocket endpoint instead" (verified live 2026-09-28).
func TestDialogueSocketServesV4Family(t *testing.T) {
	t.Parallel()
	adapter, err := NewDialogue(Config{})
	if err != nil {
		t.Fatalf("new dialogue adapter: %v", err)
	}
	media := protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1}
	for _, model := range []string{"eleven_v4", "eleven_v4_turbo"} {
		raw, err := dialogueSocket(adapter.endpointPolicy, "wss://api.elevenlabs.io"+dialogueEndpoint, model, media, "")
		if err != nil {
			t.Fatalf("%s: %v", model, err)
		}
		endpoint, _ := url.Parse(raw)
		if endpoint.Path != dialogueEndpoint || endpoint.Query().Get("model_id") != model || endpoint.Query().Get("output_format") != "pcm_24000" {
			t.Fatalf("%s endpoint = %s", model, raw)
		}
	}
}

// The keep-alive is the whole reason a second turn works: the vendor closes the
// socket after 20s of silence, and a voice agent idles longer than that between
// turns. The frame must synthesize nothing.
func TestDialogueAdapterKeepsTheSocketAliveWhileIdle(t *testing.T) {
	t.Parallel()
	keepAlives := make(chan struct{}, 4)
	server := newDialogueServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		if _, err := readDialogueMessage(ctx, conn); err != nil {
			return
		}
		for {
			message, err := readDialogueMessage(ctx, conn)
			if err != nil {
				return
			}
			switch {
			case message.KeepAlive:
				if len(message.Inputs) != 0 || message.Flush {
					t.Errorf("keep-alive carried work: %+v", message)
					return
				}
				select {
				case keepAlives <- struct{}{}:
				default:
				}
			case message.CloseSocket:
				_ = conn.Close(websocket.StatusNormalClosure, "")
				return
			}
		}
	})
	defer server.Close()

	adapter, err := NewDialogue(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new dialogue adapter: %v", err)
	}
	// The production tick is 12s, far longer than a unit test should sleep, so
	// the stream is driven directly: this asserts the FRAME the loop writes and
	// that it carries no work, which is what a caller could get wrong.
	stream, err := adapter.Open(context.Background(), dialogueRequest(server.URL, "eleven_v3_conversational"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	dialogue, ok := stream.(*dialogueStream)
	if !ok {
		t.Fatalf("stream type = %T", stream)
	}
	if err := dialogue.writeJSON(context.Background(), map[string]any{"keep_alive": true}); err != nil {
		t.Fatalf("keep-alive write: %v", err)
	}
	select {
	case <-keepAlives:
	case <-time.After(time.Second):
		t.Fatal("no keep-alive reached the server")
	}
	// The longest silence the loop allows is a full tick plus one poll: a write
	// just after a poll is not idle for a whole tick until up to one poll later.
	if dialogueKeepAliveTick+dialogueKeepAlivePoll >= dialogueIdleTimeout {
		t.Fatalf("keep-alive tick %v + poll %v does not fit inside the vendor's %v idle window", dialogueKeepAliveTick, dialogueKeepAlivePoll, dialogueIdleTimeout)
	}
	now := time.Now()
	if keepAliveDue(now.Add(-dialogueKeepAliveTick+time.Millisecond).UnixNano(), now) || !keepAliveDue(now.Add(-dialogueKeepAliveTick).UnixNano(), now) {
		t.Fatal("keep-alive is not due exactly once the socket has been quiet for a full tick")
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// Alignment on this socket is an ARRAY where the text-to-speech socket sends an
// object. Decoding it into the shared struct would fail the whole frame and take
// the audio in it down too, so it is forwarded raw and produces no spans.
func TestDialogueAdapterForwardsArrayAlignmentWithoutSpans(t *testing.T) {
	t.Parallel()
	server := newDialogueServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		if _, err := readDialogueMessage(ctx, conn); err != nil {
			return
		}
		if _, err := readDialogueMessage(ctx, conn); err != nil {
			return
		}
		if err := writeServerJSON(ctx, conn, map[string]any{
			"audio":     base64.StdEncoding.EncodeToString([]byte{4}),
			"alignment": []map[string]any{{"text": "Hi", "start": 0, "end": 120}},
		}); err != nil {
			t.Errorf("write alignment: %v", err)
			return
		}
		<-ctx.Done()
	})
	defer server.Close()

	adapter, err := NewDialogue(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new dialogue adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), dialogueRequest(server.URL, "eleven_v3_conversational"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := stream.AppendText(context.Background(), "Hi"); err != nil {
		t.Fatalf("append: %v", err)
	}
	events := collectEvents(t, stream.Events(), 3)
	if got := eventTypes(events); strings.Join(got, ",") != "audio.started,audio.frame,alignment" {
		t.Fatalf("event types = %v", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(events[2].Data, &payload); err != nil {
		t.Fatalf("alignment data: %v", err)
	}
	if _, present := payload["spans"]; present {
		t.Fatalf("dialogue alignment invented spans: %v", payload)
	}
	if len(events[2].Extensions[extensionID]) == 0 {
		t.Fatal("raw dialogue alignment was not forwarded in the extension")
	}
	if aborting, ok := stream.(runtimepkg.AbortingProviderStream); ok {
		_ = aborting.Abort(context.Background())
	}
}

// The turn-final spelling, measured against the live endpoint on 2026-09-08:
// the dialogue socket ends a turn with `is_final_audio_for_turn`, NOT the
// text-to-speech socket's `is_final`. The first version of this adapter knew
// only the latter, so a live session emitted audio and then never emitted
// audio.done — the consumer waited out its own timeout on every turn. All
// three spellings must finish the turn, and the vendor's own error text must
// reach the caller rather than being flattened to "an error occurred".
func TestDialogueAdapterFinishesTheTurnOnEverySpelling(t *testing.T) {
	t.Parallel()
	for _, field := range []string{"is_final_audio_for_turn", "is_final", "isFinal"} {
		final := field
		t.Run(final, func(t *testing.T) {
			t.Parallel()
			server := newDialogueServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
				if _, err := readDialogueMessage(ctx, conn); err != nil {
					return
				}
				if _, err := readDialogueMessage(ctx, conn); err != nil {
					return
				}
				if err := writeServerJSON(ctx, conn, map[string]any{"audio": base64.StdEncoding.EncodeToString([]byte{7, 7})}); err != nil {
					return
				}
				if err := writeServerJSON(ctx, conn, map[string]any{final: true}); err != nil {
					return
				}
				<-ctx.Done()
			})
			defer server.Close()

			adapter, err := NewDialogue(testConfig(server.URL))
			if err != nil {
				t.Fatalf("new dialogue adapter: %v", err)
			}
			stream, err := adapter.Open(context.Background(), dialogueRequest(server.URL, "eleven_v3_conversational"))
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			if err := stream.AppendText(context.Background(), "Hi"); err != nil {
				t.Fatalf("append: %v", err)
			}
			events := collectEvents(t, stream.Events(), 3)
			if got := eventTypes(events); strings.Join(got, ",") != "audio.started,audio.frame,audio.done" {
				t.Fatalf("%s produced %v, want the turn to finish", final, got)
			}
			if aborting, ok := stream.(runtimepkg.AbortingProviderStream); ok {
				_ = aborting.Abort(context.Background())
			}
		})
	}
}

// The vendor's error text is the only thing that separates a bad voice from an
// exhausted quota, so it must survive into the ProviderError.
func TestDialogueAdapterCarriesTheVendorErrorText(t *testing.T) {
	t.Parallel()
	server := newDialogueServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		if _, err := readDialogueMessage(ctx, conn); err != nil {
			return
		}
		if _, err := readDialogueMessage(ctx, conn); err != nil {
			return
		}
		if err := writeServerJSON(ctx, conn, map[string]any{"error": "voice_not_found"}); err != nil {
			return
		}
		<-ctx.Done()
	})
	defer server.Close()

	adapter, err := NewDialogue(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new dialogue adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), dialogueRequest(server.URL, "eleven_v3_conversational"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := stream.AppendText(context.Background(), "Hi"); err != nil {
		t.Fatalf("append: %v", err)
	}
	select {
	case event, ok := <-stream.Events():
		if !ok || event.Err == nil {
			t.Fatalf("event = %+v, ok=%v, want a provider error", event, ok)
		}
		if !strings.Contains(event.Err.Error(), "voice_not_found") {
			t.Fatalf("error lost the vendor text: %v", event.Err)
		}
		if strings.Contains(event.Err.Error(), "customer-elevenlabs-key") {
			t.Fatalf("error leaked the credential: %v", event.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no error event")
	}
}

// Media guards: the relay only ever asks for mono pcm_s16le, and the dialogue
// endpoint's pcm set is the text-to-speech set plus 32 kHz.
func TestDialogueAdapterMediaGuards(t *testing.T) {
	t.Parallel()
	adapter, err := NewDialogue(testConfig("http://127.0.0.1:1"))
	if err != nil {
		t.Fatalf("new dialogue adapter: %v", err)
	}
	for _, test := range []struct {
		name  string
		media protocol.MediaFormat
		want  bool
	}{
		{"32k is accepted here and not on text-to-speech", protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 32_000, Channels: 1}, true},
		{"mp3 is refused", protocol.MediaFormat{Encoding: "mp3", SampleRateHz: 44_100, Channels: 1}, false},
		{"stereo is refused", protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 2}, false},
		{"unsupported rate is refused", protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 12_345, Channels: 1}, false},
	} {
		request := dialogueRequest("http://127.0.0.1:1", "eleven_v3_conversational")
		media := test.media
		request.Media = &media
		_, err := adapter.Open(context.Background(), request)
		// Every accepted case still fails to DIAL 127.0.0.1:1; what separates
		// the two is whether the failure is the media guard or the connection.
		mediaRejected := err != nil && strings.Contains(err.Error(), "pcm_s16le") || err != nil && strings.Contains(err.Error(), "pcm output at")
		if test.want && mediaRejected {
			t.Fatalf("%s: media rejected: %v", test.name, err)
		}
		if !test.want && !mediaRejected {
			t.Fatalf("%s: media accepted, err=%v", test.name, err)
		}
	}
}

// A missing voice is a hard error, not a silently unregistered handshake: the
// dialogue socket has no default speaker.
func TestDialogueAdapterRequiresAVoice(t *testing.T) {
	t.Parallel()
	adapter, err := NewDialogue(testConfig("http://127.0.0.1:1"))
	if err != nil {
		t.Fatalf("new dialogue adapter: %v", err)
	}
	request := dialogueRequest("http://127.0.0.1:1", "eleven_v3_conversational")
	request.Options.Voice = "   "
	if _, err := adapter.Open(context.Background(), request); err == nil || !strings.Contains(err.Error(), "voice id") {
		t.Fatalf("open without a voice: %v", err)
	}
}

// Pipecat keeps one Gateway session for a bot turn and commits sentence by
// sentence, waiting for audio.done between them. The dialogue socket takes
// another inputs+flush round on the same connection, so the session must too:
// it used to refuse every append after the first commit.
func TestDialogueAdapterSpeaksSuccessiveCommitsOnOneSession(t *testing.T) {
	t.Parallel()
	server := newDialogueServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		if _, err := readDialogueMessage(ctx, conn); err != nil {
			t.Errorf("read handshake: %v", err)
			return
		}
		for turn := 0; turn < 2; turn++ {
			input, err := readDialogueMessage(ctx, conn)
			if err != nil || len(input.Inputs) != 1 {
				t.Errorf("turn %d input = %+v, err=%v", turn, input, err)
				return
			}
			flush, err := readDialogueMessage(ctx, conn)
			if err != nil || !flush.Flush {
				t.Errorf("turn %d flush = %+v, err=%v", turn, flush, err)
				return
			}
			if err := writeServerJSON(ctx, conn, map[string]any{"audio": base64.StdEncoding.EncodeToString([]byte{byte(turn)})}); err != nil {
				t.Errorf("write audio: %v", err)
				return
			}
			if err := writeServerJSON(ctx, conn, map[string]any{"is_final_audio_for_turn": true}); err != nil {
				t.Errorf("write final: %v", err)
				return
			}
		}
		if message, err := readDialogueMessage(ctx, conn); err != nil || !message.CloseSocket {
			t.Errorf("close socket = %+v, err=%v", message, err)
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	defer server.Close()

	adapter, err := NewDialogue(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new dialogue adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), dialogueRequest(server.URL, "eleven_v4_turbo"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for turn, sentence := range []string{"First sentence.", "Second sentence."} {
		if err := stream.AppendText(context.Background(), sentence); err != nil {
			t.Fatalf("turn %d append: %v", turn, err)
		}
		if err := stream.CommitText(context.Background()); err != nil {
			t.Fatalf("turn %d commit: %v", turn, err)
		}
		events := collectEvents(t, stream.Events(), 3)
		if got := eventTypes(events); strings.Join(got, ",") != "audio.started,audio.frame,audio.done" {
			t.Fatalf("turn %d event types = %v", turn, got)
		}
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// cancelServer answers each connection in turn. The first keeps streaming
// audio until it is dropped, standing in for a long utterance the caller
// interrupts; every later connection speaks one flushed turn and then waits
// for close_socket.
func cancelServer(t *testing.T, connections chan<- *http.Request, handshakes chan<- dialogueClientMessage) *httptest.Server {
	t.Helper()
	var count atomic.Int32
	return newDialogueServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
		index := count.Add(1)
		connections <- request.Clone(request.Context())
		handshake, err := readDialogueMessage(ctx, conn)
		if err != nil {
			return
		}
		handshakes <- handshake
		if index == 1 {
			if _, err := readDialogueMessage(ctx, conn); err != nil {
				return
			}
			for {
				if err := writeServerJSON(ctx, conn, map[string]any{"audio": base64.StdEncoding.EncodeToString([]byte{0xAA})}); err != nil {
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
		for {
			message, err := readDialogueMessage(ctx, conn)
			if err != nil {
				return
			}
			switch {
			case message.Flush:
				_ = writeServerJSON(ctx, conn, map[string]any{"audio": base64.StdEncoding.EncodeToString([]byte{0xBB})})
				_ = writeServerJSON(ctx, conn, map[string]any{"is_final_audio_for_turn": true})
			case message.CloseSocket:
				_ = conn.Close(websocket.StatusNormalClosure, "")
				return
			}
		}
	})
}

// waitForCancelledAudio drains events until the first audio frame of the
// utterance being cancelled, so the Cancel lands mid-speech.
func waitForCancelledAudio(t *testing.T, events <-chan runtimepkg.ProviderEvent) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatal("events closed before the first utterance spoke")
			}
			if event.Err != nil {
				t.Fatalf("first utterance error: %v", event.Err)
			}
			if event.Type == protocol.EventAudioFrame {
				return
			}
		case <-deadline:
			t.Fatal("first utterance never spoke")
		}
	}
}

// After a Cancel the session must still speak: the caller interrupted one
// utterance, not the call. The cancelled socket is dropped (the vendor has no
// stop-turn frame) and the next utterance runs on a replacement, so no audio
// of the cancelled turn may reach the session after Cancel returns.
func TestDialogueAdapterSpeaksAgainAfterCancel(t *testing.T) {
	t.Parallel()
	for _, managed := range []bool{false, true} {
		name := "permanent key"
		if managed {
			name = "managed token"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			connections := make(chan *http.Request, 4)
			handshakes := make(chan dialogueClientMessage, 4)
			server := cancelServer(t, connections, handshakes)
			defer server.Close()

			adapter, err := NewDialogue(testConfig(server.URL))
			if err != nil {
				t.Fatalf("new dialogue adapter: %v", err)
			}
			request := dialogueRequest(server.URL, "eleven_v4_turbo")
			var refreshes atomic.Int32
			if managed {
				request.Plan.Execution.CredentialSource = protocol.CredentialsManaged
				request.Plan.Route.Credential.Value = "token-1"
				request.RefreshCredential = func(context.Context) (protocol.DelegatedCredential, error) {
					n := refreshes.Add(1)
					return protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: fmt.Sprintf("token-%d", n+1), ExpiresAt: time.Now().Add(time.Minute)}, nil
				}
			}
			stream, err := adapter.Open(context.Background(), request)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			if err := stream.AppendText(context.Background(), "A long first answer the caller talks over."); err != nil {
				t.Fatalf("first append: %v", err)
			}
			waitForCancelledAudio(t, stream.Events())
			if err := stream.Cancel(context.Background()); err != nil {
				t.Fatalf("cancel: %v", err)
			}
			if err := stream.AppendText(context.Background(), "Sorry, go ahead."); err != nil {
				t.Fatalf("append after cancel: %v", err)
			}
			if err := stream.CommitText(context.Background()); err != nil {
				t.Fatalf("commit after cancel: %v", err)
			}
			var audio []byte
			deadline := time.After(5 * time.Second)
		collect:
			for {
				select {
				case event, ok := <-stream.Events():
					if !ok {
						t.Fatal("session ended after cancel")
					}
					if event.Err != nil {
						t.Fatalf("event error after cancel: %v", event.Err)
					}
					audio = append(audio, event.Audio...)
					if event.Type == protocol.EventAudioDone {
						break collect
					}
				case <-deadline:
					t.Fatal("no audio.done after cancel")
				}
			}
			// Frames already queued before Cancel returned may still drain; none
			// may arrive after the replacement's audio starts.
			if len(audio) == 0 || audio[len(audio)-1] != 0xBB || bytes.Contains(audio[bytes.IndexByte(audio, 0xBB):], []byte{0xAA}) {
				t.Fatalf("post-cancel audio = %x, want the replacement's turn only", audio)
			}
			if err := stream.Close(context.Background()); err != nil {
				t.Fatalf("close: %v", err)
			}

			first, second := <-connections, <-connections
			firstHandshake, secondHandshake := <-handshakes, <-handshakes
			if managed {
				if first.URL.Query().Get("single_use_token") != "token-1" || second.URL.Query().Get("single_use_token") != "token-2" || refreshes.Load() != 1 {
					t.Fatalf("tokens = %q then %q after %d refreshes; a spent token must never be replayed", first.URL.Query().Get("single_use_token"), second.URL.Query().Get("single_use_token"), refreshes.Load())
				}
				if second.Header.Get("xi-api-key") != "" || secondHandshake.APIKey != "" {
					t.Fatal("refreshed token leaked outside the query")
				}
			} else if second.Header.Get("xi-api-key") != "customer-elevenlabs-key" || secondHandshake.APIKey != "customer-elevenlabs-key" {
				t.Fatalf("permanent key not reused: header=%q frame=%q", second.Header.Get("xi-api-key"), secondHandshake.APIKey)
			}
			if len(firstHandshake.Voices) != 1 || len(secondHandshake.Voices) != 1 || secondHandshake.Voices[0] != firstHandshake.Voices[0] {
				t.Fatalf("voice not re-registered: %v then %v", firstHandshake.Voices, secondHandshake.Voices)
			}
		})
	}
}

// A consumer that stops reading fills the events buffer and blocks the
// reader mid-send. Cancel must still return: the first version held the
// stream lock across that send, so Cancel waited on it forever (found by a
// live run against ElevenLabs, where audio outpaces a paused reader).
func TestDialogueAdapterCancelDoesNotWaitOnAStalledConsumer(t *testing.T) {
	t.Parallel()
	connections := make(chan *http.Request, 4)
	handshakes := make(chan dialogueClientMessage, 4)
	server := cancelServer(t, connections, handshakes)
	defer server.Close()
	adapter, err := NewDialogue(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new dialogue adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), dialogueRequest(server.URL, "eleven_v4_turbo"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := stream.AppendText(context.Background(), "Fill the buffer."); err != nil {
		t.Fatalf("append: %v", err)
	}
	// 32 buffered events at one frame per 5ms: well past full by now.
	time.Sleep(400 * time.Millisecond)
	cancelled := make(chan error, 1)
	go func() { cancelled <- stream.Cancel(context.Background()) }()
	select {
	case err := <-cancelled:
		if err != nil {
			t.Fatalf("cancel: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Cancel blocked behind a reader stuck on a full events channel")
	}
	_ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background())
}

// A Cancel with nothing in flight has nothing to stop, so it keeps the socket
// rather than spending a reconnect (and, managed, a minted token).
func TestDialogueAdapterIdleCancelKeepsTheConnection(t *testing.T) {
	t.Parallel()
	connections := make(chan *http.Request, 4)
	handshakes := make(chan dialogueClientMessage, 4)
	server := cancelServer(t, connections, handshakes)
	defer server.Close()
	adapter, err := NewDialogue(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new dialogue adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), dialogueRequest(server.URL, "eleven_v4_turbo"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatalf("idle cancel: %v", err)
	}
	_ = stream.Close(context.Background())
	<-connections
	select {
	case <-connections:
		t.Fatal("idle cancel reconnected")
	case <-time.After(200 * time.Millisecond):
	}
}

// Without a refresher a managed single-use token cannot authenticate a second
// socket, so Cancel ends the session as it always did instead of replaying a
// spent token.
func TestDialogueAdapterManagedCancelWithoutRefresherEndsTheSession(t *testing.T) {
	t.Parallel()
	connections := make(chan *http.Request, 4)
	handshakes := make(chan dialogueClientMessage, 4)
	server := cancelServer(t, connections, handshakes)
	defer server.Close()
	adapter, err := NewDialogue(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new dialogue adapter: %v", err)
	}
	request := dialogueRequest(server.URL, "eleven_v4_turbo")
	request.Plan.Execution.CredentialSource = protocol.CredentialsManaged
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := stream.AppendText(context.Background(), "Something to cancel."); err != nil {
		t.Fatalf("append: %v", err)
	}
	waitForCancelledAudio(t, stream.Events())
	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if err := stream.AppendText(context.Background(), "More."); !errors.Is(err, runtimepkg.ErrSessionClosed) {
		t.Fatalf("append after cancel = %v, want ErrSessionClosed", err)
	}
	<-connections
	select {
	case <-connections:
		t.Fatal("reconnected without a credential source")
	case <-time.After(200 * time.Millisecond):
	}
}

// A refresh the control plane refuses ends the session with the provider error
// rather than leaving the next utterance hanging.
func TestDialogueAdapterFailedRefreshEndsTheSession(t *testing.T) {
	t.Parallel()
	connections := make(chan *http.Request, 4)
	handshakes := make(chan dialogueClientMessage, 4)
	server := cancelServer(t, connections, handshakes)
	defer server.Close()
	adapter, err := NewDialogue(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new dialogue adapter: %v", err)
	}
	request := dialogueRequest(server.URL, "eleven_v4_turbo")
	request.Plan.Execution.CredentialSource = protocol.CredentialsManaged
	request.RefreshCredential = func(context.Context) (protocol.DelegatedCredential, error) {
		return protocol.DelegatedCredential{}, errors.New("reissue cap reached")
	}
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := stream.AppendText(context.Background(), "Something to cancel."); err != nil {
		t.Fatalf("append: %v", err)
	}
	waitForCancelledAudio(t, stream.Events())
	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if err := stream.AppendText(context.Background(), "More."); err == nil {
		t.Fatal("append after a failed refresh succeeded")
	}
	var providerErr *runtimepkg.ProviderError
	for event := range stream.Events() {
		if event.Err != nil && errors.As(event.Err, &providerErr) {
			break
		}
	}
	if providerErr == nil || !strings.Contains(providerErr.Message, "reconnect") {
		t.Fatalf("terminal error = %v, want the reconnect failure", providerErr)
	}
}

// A managed provider-direct plan carries a control-plane-minted single-use
// token. The vendor accepts it only as the `single_use_token` query parameter
// (the header answers 1008 "Invalid API key"), so it must never reach the header
// or the handshake frame, while a relay plan's permanent connector key keeps the
// header channel.
func TestDialogueAdapterSendsManagedTokensOnTheQueryOnly(t *testing.T) {
	t.Parallel()
	for _, route := range []protocol.ProviderRoute{protocol.RouteProviderDirect, protocol.RouteSpekoRelay} {
		t.Run(string(route), func(t *testing.T) {
			t.Parallel()
			type observed struct {
				request   *http.Request
				handshake dialogueClientMessage
			}
			seen := make(chan observed, 1)
			server := newDialogueServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
				handshake, err := readDialogueMessage(ctx, conn)
				if err != nil {
					t.Errorf("read handshake: %v", err)
					return
				}
				seen <- observed{request: request.Clone(request.Context()), handshake: handshake}
				_ = conn.Close(websocket.StatusNormalClosure, "")
			})
			defer server.Close()

			adapter, err := NewDialogue(testConfig(server.URL))
			if err != nil {
				t.Fatalf("new dialogue adapter: %v", err)
			}
			request := dialogueRequest(server.URL, "eleven_v4_turbo")
			request.Plan.Execution.CredentialSource = protocol.CredentialsManaged
			request.Plan.Execution.ProviderRoute = route
			request.Plan.Route.Credential.Value = "temporary-elevenlabs-token"
			stream, err := adapter.Open(context.Background(), request)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer stream.Close(context.Background())

			got := <-seen
			token := got.request.URL.Query().Get("single_use_token")
			header := got.request.Header.Get("xi-api-key")
			if route == protocol.RouteProviderDirect {
				if token != "temporary-elevenlabs-token" || header != "" || got.handshake.APIKey != "" {
					t.Fatalf("provider-direct token=%q header=%q frame=%q", token, header, got.handshake.APIKey)
				}
				return
			}
			if token != "" || header != "temporary-elevenlabs-token" || got.handshake.APIKey != "temporary-elevenlabs-token" {
				t.Fatalf("relay token=%q header=%q frame=%q", token, header, got.handshake.APIKey)
			}
		})
	}
}

type dialogueClientMessage struct {
	Voices []string `json:"voices"`
	APIKey string   `json:"xi_api_key"`
	Inputs []struct {
		Text    string `json:"text"`
		VoiceID string `json:"voice_id"`
		NewTurn bool   `json:"new_turn"`
	} `json:"inputs"`
	Flush       bool `json:"flush"`
	KeepAlive   bool `json:"keep_alive"`
	CloseSocket bool `json:"close_socket"`

	VoiceSettings map[string]any `json:"voice_settings"`
}

func readDialogueMessage(ctx context.Context, conn *websocket.Conn) (dialogueClientMessage, error) {
	var message dialogueClientMessage
	_, payload, err := conn.Read(ctx)
	if err != nil {
		return message, err
	}
	return message, json.Unmarshal(payload, &message)
}

func newDialogueServer(t *testing.T, callback func(context.Context, *http.Request, *websocket.Conn)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != dialogueEndpoint {
			http.NotFound(w, request)
			return
		}
		conn, err := websocket.Accept(w, request, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			defer cancel()
			defer conn.CloseNow()
			callback(ctx, request, conn)
		}()
	}))
}

func dialogueRequest(serverURL, model string) runtimepkg.AdapterRequest {
	request := elevenLabsRequest(serverURL, protocol.CredentialsBYOK)
	endpoint, _ := url.Parse(serverURL)
	endpoint.Scheme = "ws"
	endpoint.Path = dialogueEndpoint
	request.Plan.Route.Endpoint = endpoint.String()
	request.Plan.Route.Model = model
	request.Plan.Route.Adapter = DialogueAdapterID
	return request
}
