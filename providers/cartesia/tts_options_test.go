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
		{"buffer delay", "sonic-3.6", map[string]any{"max_buffer_delay_ms": 300.0}, ""},
		{"buffer delay go int", "sonic-3", map[string]any{"max_buffer_delay_ms": 300}, ""},
		{"buffer delay off", "sonic-3.5", map[string]any{"max_buffer_delay_ms": 0.0}, ""},
		{"buffer delay max", "sonic-3.6", map[string]any{"max_buffer_delay_ms": 5000.0}, ""},
		{"buffer delay fraction", "sonic-3.6", map[string]any{"max_buffer_delay_ms": 300.5}, "max_buffer_delay_ms"},
		{"buffer delay negative", "sonic-3.6", map[string]any{"max_buffer_delay_ms": -1.0}, "max_buffer_delay_ms"},
		{"buffer delay too long", "sonic-3.6", map[string]any{"max_buffer_delay_ms": 5001.0}, "max_buffer_delay_ms"},
		{"buffer delay string", "sonic-3.6", map[string]any{"max_buffer_delay_ms": "300"}, "max_buffer_delay_ms"},
		{"buffer delay bool", "sonic-3.6", map[string]any{"max_buffer_delay_ms": true}, "max_buffer_delay_ms"},
		{"buffer delay undocumented model", "sonic-2", map[string]any{"max_buffer_delay_ms": 300.0}, "max_buffer_delay_ms"},
		{"speed", "sonic-3.6", map[string]any{"speed": 1.2}, ""},
		{"speed sonic-3.5", "sonic-3.5", map[string]any{"speed": 0.6}, ""},
		{"speed sonic-3", "sonic-3", map[string]any{"speed": 1.5}, ""},
		{"speed go int", "sonic-3.6", map[string]any{"speed": 1}, ""},
		{"speed too slow", "sonic-3.6", map[string]any{"speed": 0.59}, "speed"},
		{"speed too fast", "sonic-3.6", map[string]any{"speed": 1.51}, "speed"},
		{"speed string", "sonic-3.6", map[string]any{"speed": "fast"}, "speed"},
		{"speed undocumented model", "sonic-2", map[string]any{"speed": 1.2}, "speed"},
		{"all three", "sonic-3.6", map[string]any{"normalization": "en-IN", "max_buffer_delay_ms": 250.0, "speed": 1.1}, ""},
		{"one bad of several", "sonic-3.6", map[string]any{"max_buffer_delay_ms": 250.0, "speed": 2.0}, "speed"},
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

func TestRefusalNamesEveryAcceptedSetting(t *testing.T) {
	t.Parallel()
	err := ValidateTTSOptions("sonic-3.6", map[string]any{"volume": 1.2})
	var refusal *protocol.TtsOptionError
	if !errors.As(err, &refusal) || refusal.Detail != "Cartesia TTS accepts only normalization, max_buffer_delay_ms and speed" {
		t.Fatalf("unknown setting refusal = %v", err)
	}
}

// max_buffer_delay_ms and generation_config.speed ride every generation of the
// context, the closing one included: Cartesia requires every request of a
// context to repeat the same fields.
func TestAdapterSendsStreamOptionsOnEveryGeneration(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		settings  map[string]any
		wantDelay any
		wantSpeed any
	}{
		{"buffer delay", map[string]any{"max_buffer_delay_ms": 300.0}, float64(300), nil},
		{"speed", map[string]any{"speed": 1.2}, nil, 1.2},
		{"both", map[string]any{"max_buffer_delay_ms": 0.0, "speed": 0.8}, float64(0), 0.8},
		{"absent", nil, nil, nil},
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
				if err := writeJSON(ctx, conn, map[string]any{"type": "done", "context_id": contextID, "done": true}); err != nil {
					t.Errorf("done: %v", err)
					return
				}
				waitForClientClose(ctx, conn)
			})
			defer server.Close()

			adapter, _ := New(testConfig(server.URL))
			request := adapterRequest(server.URL)
			request.Plan.Route.Model = "sonic-3.5"
			if test.settings != nil {
				request.Options.TTS = &protocol.TtsOptions{ProviderOptions: map[string]map[string]any{"cartesia": test.settings}}
			}
			stream, err := adapter.Open(context.Background(), request)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer func() { _ = stream.Close(context.Background()) }()
			if err := stream.AppendText(context.Background(), "First phrase. "); err != nil {
				t.Fatalf("append: %v", err)
			}
			if err := stream.CommitText(context.Background()); err != nil {
				t.Fatalf("commit: %v", err)
			}
			for range 2 {
				frame := <-frames
				delay, hasDelay := frame["max_buffer_delay_ms"]
				if (test.wantDelay == nil) == hasDelay || (hasDelay && delay != test.wantDelay) {
					t.Fatalf("generation = %v, want max_buffer_delay_ms %v", frame, test.wantDelay)
				}
				config, hasConfig := frame["generation_config"]
				if test.wantSpeed == nil {
					if hasConfig {
						t.Fatalf("generation = %v, want no generation_config", frame)
					}
					continue
				}
				object, ok := config.(map[string]any)
				if !ok || len(object) != 1 || object["speed"] != test.wantSpeed {
					t.Fatalf("generation = %v, want generation_config {speed: %v}", frame, test.wantSpeed)
				}
			}
		})
	}
}

// A caller that sets nothing must send exactly the bytes it sent before
// provider options grew stream settings.
func TestGenerationWithoutOptionsIsUnchanged(t *testing.T) {
	t.Parallel()
	s := &stream{modelID: "sonic-3.6", voiceID: "voice-id", language: "en", media: protocol.MediaFormat{SampleRateHz: 24000}}
	for _, test := range []struct {
		transcript   string
		continuation bool
		want         string
	}{
		{"Hello. ", true, `{"model_id":"sonic-3.6","transcript":"Hello. ","voice":{"mode":"id","id":"voice-id"},"language":"en","context_id":"ctx","output_format":{"container":"raw","encoding":"pcm_s16le","sample_rate":24000},"add_timestamps":true,"continue":true}`},
		{"", false, `{"model_id":"sonic-3.6","transcript":"","voice":{"mode":"id","id":"voice-id"},"language":"en","context_id":"ctx","output_format":{"container":"raw","encoding":"pcm_s16le","sample_rate":24000},"add_timestamps":true,"continue":false}`},
	} {
		payload, err := json.Marshal(s.generationRequest("ctx", test.transcript, test.continuation))
		if err != nil || string(payload) != test.want {
			t.Fatalf("generation = %s, %v; want %s", payload, err, test.want)
		}
	}
}

func TestAdapterRefusesStreamOptionsBeforeDialing(t *testing.T) {
	t.Parallel()
	for _, settings := range []map[string]any{
		{"max_buffer_delay_ms": 300.5},
		{"max_buffer_delay_ms": 6000.0},
		{"speed": 2.0},
		{"speed": "fast"},
	} {
		adapter, _ := New(testConfig("http://127.0.0.1:1"))
		request := adapterRequest("http://127.0.0.1:1")
		request.Plan.Route.Model = "sonic-3.6"
		request.Options.TTS = &protocol.TtsOptions{ProviderOptions: map[string]map[string]any{"cartesia": settings}}
		var refusal *protocol.TtsOptionError
		if _, err := adapter.Open(context.Background(), request); !errors.As(err, &refusal) {
			t.Fatalf("open with %v = %v, want a refusal before dialing", settings, err)
		}
	}
}
