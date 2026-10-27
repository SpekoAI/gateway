package elevenlabs

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"testing"

	"github.com/SpekoAI/gateway/protocol"
	"github.com/coder/websocket"
)

func TestValidateTTSOptions(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		model    string
		settings map[string]any
		wantKey  string
	}{
		{"none", "eleven_flash_v2_5", nil, ""},
		{"every voice setting on multi-context", "eleven_flash_v2_5", map[string]any{"stability": 0.4, "similarity_boost": 0.8, "style": 0.2, "use_speaker_boost": true, "speed": 1.1}, ""},
		{"integer spelling", "eleven_multilingual_v2", map[string]any{"stability": 1}, ""},
		{"stability on dialogue", "eleven_v4_turbo", map[string]any{"stability": 0.3}, ""},
		{"style on dialogue", "eleven_v3_conversational", map[string]any{"stability": 0.5, "style": 0.2}, "style"},
		{"stability above one", "eleven_flash_v2_5", map[string]any{"stability": 1.7}, "stability"},
		{"speed outside the vendor range", "eleven_flash_v2_5", map[string]any{"speed": 1.5}, "speed"},
		{"number as a string", "eleven_flash_v2_5", map[string]any{"stability": "0.4"}, "stability"},
		{"boost as a number", "eleven_flash_v2_5", map[string]any{"use_speaker_boost": 1}, "use_speaker_boost"},
		{"unknown setting", "eleven_flash_v2_5", map[string]any{"optimize_streaming_latency": 3}, "optimize_streaming_latency"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateTTSOptions(test.model, test.settings)
			if test.wantKey == "" {
				if err != nil {
					t.Fatalf("ValidateTTSOptions = %v, want nil", err)
				}
				return
			}
			var refusal *protocol.TtsOptionError
			if !errors.As(err, &refusal) || refusal.Provider != "elevenlabs" || refusal.Key != test.wantKey {
				t.Fatalf("ValidateTTSOptions = %v, want a refusal naming %q", err, test.wantKey)
			}
		})
	}
}

// Voice settings are per context on the multi-context socket, so every
// utterance's context must open with them, not just the session's first.
func TestAdapterSendsVoiceSettingsAtEveryContextStart(t *testing.T) {
	t.Parallel()
	want := map[string]any{"stability": 0.4, "similarity_boost": 0.8, "use_speaker_boost": true}
	server := newMultiContextServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		for _, text := range []string{"first", "second"} {
			init, err := readClientMessage(ctx, conn)
			if err != nil || init.ContextID == "" || init.Text != " " || !reflect.DeepEqual(init.VoiceSettings, want) {
				t.Errorf("init = %+v, err=%v", init, err)
				return
			}
			body, err := readClientMessage(ctx, conn)
			if err != nil || body.ContextID != init.ContextID || body.Text != text || body.VoiceSettings != nil {
				t.Errorf("body = %+v, err=%v", body, err)
				return
			}
			cancel, err := readClientMessage(ctx, conn)
			if err != nil || !cancel.CloseContext {
				t.Errorf("cancel = %+v, err=%v", cancel, err)
				return
			}
		}
		_, _ = readClientMessage(ctx, conn) // close_socket
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	defer server.Close()

	adapter, _ := New(testConfig(server.URL))
	request := elevenLabsRequest(server.URL, protocol.CredentialsBYOK)
	request.Options.TTS = &protocol.TtsOptions{ProviderOptions: map[string]map[string]any{
		"elevenlabs": {"stability": 0.4, "similarity_boost": 0.8, "use_speaker_boost": true},
		// Another vendor's settings are ignored, never refused.
		"cartesia": {"normalization": "en-IN"},
	}}
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, text := range []string{"first", "second"} {
		if err := stream.AppendText(context.Background(), text); err != nil {
			t.Fatalf("append %q: %v", text, err)
		}
		if err := stream.Cancel(context.Background()); err != nil {
			t.Fatalf("cancel: %v", err)
		}
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestAdapterRefusesOutOfRangeVoiceSettingsBeforeDialing(t *testing.T) {
	t.Parallel()
	dialed := make(chan struct{}, 1)
	server := newMultiContextServer(t, func(context.Context, *http.Request, *websocket.Conn) { dialed <- struct{}{} })
	defer server.Close()

	adapter, _ := New(testConfig(server.URL))
	request := elevenLabsRequest(server.URL, protocol.CredentialsBYOK)
	request.Options.TTS = &protocol.TtsOptions{ProviderOptions: map[string]map[string]any{"elevenlabs": {"stability": 1.7}}}
	_, err := adapter.Open(context.Background(), request)
	var refusal *protocol.TtsOptionError
	if !errors.As(err, &refusal) || refusal.Key != "stability" {
		t.Fatalf("open = %v, want a stability refusal", err)
	}
	select {
	case <-dialed:
		t.Fatal("the adapter dialed the vendor before refusing")
	default:
	}
}

func TestDialogueAdapterSendsStabilityInTheHandshake(t *testing.T) {
	t.Parallel()
	handshakes := make(chan dialogueClientMessage, 1)
	server := newDialogueServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		handshake, err := readDialogueMessage(ctx, conn)
		if err != nil {
			t.Errorf("handshake: %v", err)
			return
		}
		handshakes <- handshake
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	defer server.Close()

	adapter, _ := NewDialogue(testConfig(server.URL))
	request := dialogueRequest(server.URL, "eleven_v4_turbo")
	request.Options.TTS = &protocol.TtsOptions{ProviderOptions: map[string]map[string]any{"elevenlabs": {"stability": 0.3}}}
	stream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = stream.Close(context.Background()) }()
	handshake := <-handshakes
	if !reflect.DeepEqual(handshake.VoiceSettings, map[string]any{"stability": 0.3}) || len(handshake.Voices) != 1 {
		t.Fatalf("handshake = %+v, want stability 0.3 beside the one voice", handshake)
	}

	request.Options.TTS = &protocol.TtsOptions{ProviderOptions: map[string]map[string]any{"elevenlabs": {"similarity_boost": 0.8}}}
	if _, err := adapter.Open(context.Background(), request); err == nil {
		t.Fatal("open with similarity_boost on the dialogue socket succeeded, want a refusal")
	}
}
