package protocol_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/SpekoAI/gateway/protocol"
)

func TestTranslationControlsAreSessionUpdateOnly(t *testing.T) {
	t.Parallel()
	translation := protocol.SpeechProtocolOpenAIRealtimeTranslationV1
	if got := protocol.ProviderControlTypes(translation); len(got) != 1 || got[0] != "session.update" {
		t.Fatalf("translation controls = %v, want [session.update]", got)
	}
	// Conversational Realtime and GPT-Live commands have no meaning on a
	// translation socket. The audio append and session.close are the media
	// path and lifecycle, never controls.
	for _, controlType := range []string{"response.create", "response.cancel", "input_audio_buffer.commit", "conversation.item.create",
		"session.input_audio_buffer.append", "session.close", "session.instructions.append"} {
		if protocol.ProviderControlAllowed(translation, controlType) {
			t.Fatalf("%q must not be forwardable on %s", controlType, translation)
		}
	}
	// And the translation command set does not leak onto the Realtime route.
	if !protocol.ProviderControlAllowed(protocol.SpeechProtocolOpenAIRealtimeV1, "session.update") {
		t.Fatal("realtime keeps session.update")
	}
}

func TestTranslationSessionUpdateShape(t *testing.T) {
	t.Parallel()
	vendorExample := `{"type":"session.update","session":{"audio":{"input":{"transcription":{"model":"gpt-realtime-whisper"},"noise_reduction":{"type":"near_field"}},"output":{"language":"es"}}}}`
	update, err := protocol.ValidateTranslationSessionUpdate([]byte(vendorExample))
	if err != nil {
		t.Fatalf("vendor example refused: %v", err)
	}
	if update.Language != "es" || update.TranscriptionModel != "gpt-realtime-whisper" || update.NoiseReduction != "near_field" {
		t.Fatalf("decoded = %+v", update)
	}
	minimal, err := protocol.ValidateTranslationSessionUpdate([]byte(`{"type":"session.update","event_id":"evt_1","session":{"audio":{"output":{"language":"ja"}}}}`))
	if err != nil || minimal.Language != "ja" || minimal.EventID != "evt_1" {
		t.Fatalf("minimal update = %+v, %v", minimal, err)
	}
	disabled, err := protocol.ValidateTranslationSessionUpdate([]byte(`{"type":"session.update","session":{"audio":{"input":{"transcription":null,"noise_reduction":null}}}}`))
	if err != nil || !disabled.TranscriptionDisabled || !disabled.NoiseReductionDisabled || disabled.Language != "" {
		t.Fatalf("null input settings = %+v, %v", disabled, err)
	}
	for _, language := range protocol.TranslationOutputLanguages {
		if _, err := protocol.ValidateTranslationSessionUpdate([]byte(`{"type":"session.update","session":{"audio":{"output":{"language":"` + language + `"}}}}`)); err != nil {
			t.Fatalf("language %q refused: %v", language, err)
		}
	}
	if len(protocol.TranslationOutputLanguages) != 13 {
		t.Fatalf("the vendor publishes 13 output languages, got %d", len(protocol.TranslationOutputLanguages))
	}

	for name, payload := range map[string]string{
		"conversational body":   `{"type":"session.update","session":{"type":"realtime","instructions":"hi","audio":{"output":{"language":"es"}}}}`,
		"voice":                 `{"type":"session.update","session":{"audio":{"output":{"language":"es","voice":"marin"}}}}`,
		"output format":         `{"type":"session.update","session":{"audio":{"output":{"language":"es","format":{"type":"audio/pcm","rate":16000}}}}}`,
		"input format":          `{"type":"session.update","session":{"audio":{"input":{"format":{"type":"audio/pcm","rate":16000}}}}}`,
		"turn detection":        `{"type":"session.update","session":{"audio":{"input":{"turn_detection":{"type":"server_vad"}}}}}`,
		"model":                 `{"type":"session.update","session":{"model":"gpt-realtime","audio":{"output":{"language":"es"}}}}`,
		"tools":                 `{"type":"session.update","session":{"tools":[],"audio":{"output":{"language":"es"}}}}`,
		"unsupported language":  `{"type":"session.update","session":{"audio":{"output":{"language":"uz"}}}}`,
		"missing language":      `{"type":"session.update","session":{"audio":{"output":{}}}}`,
		"empty audio":           `{"type":"session.update","session":{"audio":{}}}`,
		"no audio":              `{"type":"session.update","session":{}}`,
		"null session":          `{"type":"session.update","session":null}`,
		"unpriced transcriber":  `{"type":"session.update","session":{"audio":{"input":{"transcription":{"model":"gpt-4o-transcribe"}}}}}`,
		"bad noise reduction":   `{"type":"session.update","session":{"audio":{"input":{"noise_reduction":{"type":"studio"}}}}}`,
		"unknown envelope":      `{"type":"session.update","response":{},"session":{"audio":{"output":{"language":"es"}}}}`,
		"wrong type":            `{"type":"response.create","session":{"audio":{"output":{"language":"es"}}}}`,
		"language not a string": `{"type":"session.update","session":{"audio":{"output":{"language":7}}}}`,
	} {
		if _, err := protocol.ValidateTranslationSessionUpdate([]byte(payload)); err == nil {
			t.Fatalf("%s: accepted %s", name, payload)
		}
	}
}

func TestTranslationProviderControlValidatesTheSessionShape(t *testing.T) {
	t.Parallel()
	translation := protocol.SpeechProtocolOpenAIRealtimeTranslationV1
	good := protocol.ProviderControl{Type: "session.update", Payload: json.RawMessage(`{"type":"session.update","session":{"audio":{"output":{"language":"de"}}}}`)}
	if err := good.Validate(translation); err != nil {
		t.Fatalf("valid translation update refused: %v", err)
	}
	conversational := protocol.ProviderControl{Type: "session.update", Payload: json.RawMessage(`{"type":"session.update","session":{"instructions":"be nice"}}`)}
	if err := conversational.Validate(translation); err == nil || !strings.Contains(err.Error(), "payload") {
		t.Fatalf("conversational session body on translation = %v, want payload refusal", err)
	}
	// The same body stays legal on the conversational protocol.
	if err := conversational.Validate(protocol.SpeechProtocolOpenAIRealtimeV1); err != nil {
		t.Fatalf("realtime session.update refused: %v", err)
	}
}
