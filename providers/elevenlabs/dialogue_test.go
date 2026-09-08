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

// The wire contract, asserted end to end: the handshake registers exactly one
// voice and carries the key, text rides an `inputs` list with `new_turn` false,
// CommitText flushes without closing, and Close sends `close_socket`.
func TestDialogueAdapterRegistersOneVoiceAndFlushesWithoutClosing(t *testing.T) {
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
	stream, err := adapter.Open(context.Background(), dialogueRequest(server.URL, "eleven_v3_conversational"))
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
	if query.Get("model_id") != "eleven_v3_conversational" {
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
}

// A non-v3 model must never reach this socket: it is the text-to-speech
// adapter's job, and the split has to fail loudly rather than dial a path the
// vendor will reject.
func TestDialogueAdapterServesOnlyV3Models(t *testing.T) {
	t.Parallel()
	adapter, err := NewDialogue(testConfig("http://127.0.0.1:1"))
	if err != nil {
		t.Fatalf("new dialogue adapter: %v", err)
	}
	for _, model := range []string{"eleven_flash_v2_5", "eleven_multilingual_v2", "auto", ""} {
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
	for _, model := range []string{"eleven_v3", "eleven_v3_conversational"} {
		if !ServesModel(model) {
			t.Fatalf("ServesModel(%q) = false, want true", model)
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
	if dialogueKeepAliveTick >= dialogueIdleTimeout {
		t.Fatalf("keep-alive tick %v does not fit inside the vendor's %v idle window", dialogueKeepAliveTick, dialogueIdleTimeout)
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
