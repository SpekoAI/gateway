package gladia

import (
	"encoding/json"
	"testing"

	"github.com/SpekoAI/gateway/protocol"
)

func sttBoolPointer(value bool) *bool { return &value }

// The portable noise_reduction ask becomes pre_processing.audio_enhancer, and
// keywords become realtime_processing.custom_vocabulary(+config), in the
// spellings POST /v2/live documents. Asserted through JSON so a struct-tag
// rename cannot keep this green.
func TestInitRequestCarriesEnhancerAndVocabulary(t *testing.T) {
	t.Parallel()
	media := protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1}
	options := protocol.RequestOptions{Language: "en", STT: &protocol.SttOptions{
		NoiseReduction: sttBoolPointer(true),
		Keywords:       []string{"Speko", "São Paulo"},
	}}
	encoded, err := json.Marshal(newInitRequest("solaria-1", options, media))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var body map[string]any
	if err := json.Unmarshal(encoded, &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	pre, ok := body["pre_processing"].(map[string]any)
	if !ok || pre["audio_enhancer"] != true {
		t.Fatalf("pre_processing.audio_enhancer must be true: %s", encoded)
	}
	processing, ok := body["realtime_processing"].(map[string]any)
	if !ok || processing["custom_vocabulary"] != true {
		t.Fatalf("realtime_processing.custom_vocabulary must be true: %s", encoded)
	}
	config, ok := processing["custom_vocabulary_config"].(map[string]any)
	if !ok {
		t.Fatalf("custom_vocabulary_config must ride with the flag: %s", encoded)
	}
	vocabulary, ok := config["vocabulary"].([]any)
	if !ok || len(vocabulary) != 2 || vocabulary[1] != "São Paulo" {
		t.Fatalf("vocabulary: %v", config["vocabulary"])
	}
}

// A request that asked for nothing produces the init body this adapter always
// sent: neither optional object appears at all.
func TestInitRequestWithoutOptionsIsUnchanged(t *testing.T) {
	t.Parallel()
	media := protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1}
	encoded, err := json.Marshal(newInitRequest("solaria-1", protocol.RequestOptions{Language: "en"}, media))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var body map[string]any
	if err := json.Unmarshal(encoded, &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, exists := body["pre_processing"]; exists {
		t.Fatalf("no ask, no pre_processing: %s", encoded)
	}
	if _, exists := body["realtime_processing"]; exists {
		t.Fatalf("no ask, no realtime_processing: %s", encoded)
	}
}
