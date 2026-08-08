package relayapi_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SpekoAI/gateway/relayapi"
)

// The golden contract: every fixture under testdata/ must decode, validate,
// and re-marshal to exactly the compacted fixture bytes. Fixture key order
// therefore pins wire field order, and a struct change that silently
// reshapes the wire fails here before it can reach a spec or a client.

func TestGoldenFixturesRoundTrip(t *testing.T) {
	t.Parallel()

	assertGolden[relayapi.Routing](t, "routing-auto.json")
	assertGolden[relayapi.Routing](t, "routing-explicit.json")
	assertGolden[relayapi.ErrorEnvelope](t, "error-envelope.json")
	assertGolden[relayapi.ModelsResponse](t, "models-response.json")
	assertGolden[relayapi.TranscriptionRequest](t, "stt-transcription-request.json")
	assertGolden[relayapi.TranscriptionResponse](t, "stt-transcription-response.json")
	assertGolden[relayapi.SpeechRequest](t, "tts-speech-request.json")
	assertGolden[relayapi.LLMRequest](t, "llm-request-tools.json")
	assertGolden[relayapi.LLMRequest](t, "llm-request-structured.json")
	assertGolden[relayapi.LLMResponse](t, "llm-response.json")
}

func assertGolden[T interface{ Validate() error }](t *testing.T, name string) {
	t.Helper()
	assertGoldenValue[T](t, name, readFixture(t, name))
}

// assertGoldenValue decodes raw into T, validates it, and requires the
// re-marshaled bytes to equal the compacted raw bytes.
func assertGoldenValue[T interface{ Validate() error }](t *testing.T, name string, raw []byte) {
	t.Helper()
	var value T
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("%s: decode: %v", name, err)
	}
	if err := value.Validate(); err != nil {
		t.Fatalf("%s: must validate: %v", name, err)
	}
	assertStableBytes(t, name, raw, value)
}

func assertStableBytes(t *testing.T, name string, raw []byte, value any) {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("%s: marshal: %v", name, err)
	}
	var compacted bytes.Buffer
	if err := json.Compact(&compacted, raw); err != nil {
		t.Fatalf("%s: compact fixture: %v", name, err)
	}
	if !bytes.Equal(encoded, compacted.Bytes()) {
		t.Fatalf("%s: marshal is not byte-stable:\n got: %s\nwant: %s", name, encoded, compacted.Bytes())
	}
}

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return contents
}

func decodeFixture(t *testing.T, name string, target any) {
	t.Helper()
	if err := json.Unmarshal(readFixture(t, name), target); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
}

func assertInvalid(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("expected validation error containing %q, got %v", want, err)
	}
}
