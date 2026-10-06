package relayapi_test

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/SpekoAI/gateway/relayapi"
)

func TestEmbeddedOpenAPISpecMatchesNormativeMirror(t *testing.T) {
	t.Parallel()

	want, err := os.ReadFile("openapi.json")
	if err != nil {
		t.Fatalf("read normative mirror: %v", err)
	}
	got := relayapi.OpenAPISpecJSON()
	if !bytes.Equal(got, want) {
		t.Fatal("embedded OpenAPI document differs from openapi.json")
	}
	var doc map[string]any
	if err := json.Unmarshal(got, &doc); err != nil {
		t.Fatalf("embedded OpenAPI document is invalid JSON: %v", err)
	}

	got[0] ^= 0xff
	if bytes.Equal(relayapi.OpenAPISpecJSON(), got) {
		t.Fatal("OpenAPISpecJSON returned mutable shared storage")
	}
}

func TestEmbeddedAsyncAPISpecMatchesNormativeMirror(t *testing.T) {
	t.Parallel()

	want, err := os.ReadFile("asyncapi.json")
	if err != nil {
		t.Fatalf("read normative mirror: %v", err)
	}
	got := relayapi.AsyncAPISpecJSON()
	if !bytes.Equal(got, want) {
		t.Fatal("embedded AsyncAPI document differs from asyncapi.json")
	}
	var doc map[string]any
	if err := json.Unmarshal(got, &doc); err != nil {
		t.Fatalf("embedded AsyncAPI document is invalid JSON: %v", err)
	}

	got[0] ^= 0xff
	if bytes.Equal(relayapi.AsyncAPISpecJSON(), got) {
		t.Fatal("AsyncAPISpecJSON returned mutable shared storage")
	}
}
