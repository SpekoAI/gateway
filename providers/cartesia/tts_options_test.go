package cartesia

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
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
		{"none", "sonic-3", nil, ""},
		{"locale", "sonic-3.6", map[string]any{"normalization": "en-IN"}, ""},
		{"language", "sonic-3.6", map[string]any{"normalization": "en"}, ""},
		{"off", "sonic-3.6", map[string]any{"normalization": "off"}, ""},
		{"auto", "sonic-3.6", map[string]any{"normalization": "auto"}, ""},
		{"undocumented model", "sonic-3.5", map[string]any{"normalization": "en-IN"}, "normalization"},
		{"not a tag", "sonic-3.6", map[string]any{"normalization": "english please"}, "normalization"},
		{"not a string", "sonic-3.6", map[string]any{"normalization": true}, "normalization"},
		{"unknown setting", "sonic-3.6", map[string]any{"volume": 1.2}, "volume"},
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
			if !errors.As(err, &refusal) || refusal.Provider != "cartesia" || refusal.Key != test.wantKey {
				t.Fatalf("ValidateTTSOptions = %v, want a refusal naming %q", err, test.wantKey)
			}
		})
	}
}

// normalization rides every generation of the session, and a session that
// set none sends no field at all, so pre-existing payloads stay byte-identical.
func TestAdapterSendsNormalizationOnEveryGeneration(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		options *protocol.TtsOptions
		want    any
	}{
		{"set", &protocol.TtsOptions{ProviderOptions: map[string]map[string]any{"cartesia": {"normalization": "en-IN"}}}, "en-IN"},
		{"absent", nil, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			frames := make(chan map[string]any, 2)
			server := newTTSServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
				var contextID any
				for range 2 {
					_, payload, err := conn.Read(ctx)
					if err != nil {
						t.Errorf("read generation: %v", err)
						return
					}
					var frame map[string]any
					if err := json.Unmarshal(payload, &frame); err != nil {
						t.Errorf("decode generation: %v", err)
						return
					}
					contextID = frame["context_id"]
					frames <- frame
				}
				// Close waits for the context to finish.
				if err := writeJSON(ctx, conn, map[string]any{"type": "done", "context_id": contextID, "done": true}); err != nil {
					t.Errorf("done: %v", err)
					return
				}
				waitForClientClose(ctx, conn)
			})
			defer server.Close()

			adapter, _ := New(testConfig(server.URL))
			request := adapterRequest(server.URL)
			request.Plan.Route.Model = "sonic-3.6"
			request.Options.Language = "hi"
			request.Options.TTS = test.options
			stream, err := adapter.Open(context.Background(), request)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer func() { _ = stream.Close(context.Background()) }()
			if err := stream.AppendText(context.Background(), "आपका प्लान 19,497 रुपये का है"); err != nil {
				t.Fatalf("append: %v", err)
			}
			if err := stream.CommitText(context.Background()); err != nil {
				t.Fatalf("commit: %v", err)
			}
			for range 2 {
				frame := <-frames
				got, present := frame["normalization"]
				if (test.want == nil && present) || (test.want != nil && got != test.want) || frame["language"] != "hi" {
					t.Fatalf("generation = %v, want normalization %v beside language hi", frame, test.want)
				}
			}
		})
	}
}

func TestAdapterRefusesNormalizationOnAnUndocumentedModel(t *testing.T) {
	t.Parallel()
	adapter, _ := New(testConfig("http://127.0.0.1:1"))
	request := adapterRequest("http://127.0.0.1:1")
	request.Plan.Route.Model = "sonic-3.5"
	request.Options.TTS = &protocol.TtsOptions{ProviderOptions: map[string]map[string]any{"cartesia": {"normalization": "en-IN"}}}
	var refusal *protocol.TtsOptionError
	if _, err := adapter.Open(context.Background(), request); !errors.As(err, &refusal) {
		t.Fatalf("open = %v, want a normalization refusal before dialing", err)
	}
}
