package relayapi_test

import (
	"strings"
	"testing"

	"github.com/SpekoAI/gateway/relayapi"
)

func TestHashContentMatchesKnownVector(t *testing.T) {
	t.Parallel()

	// SHA-256("hello"), fixed so the canonical format can never drift
	// silently: prefix, lowercase hex, 64 digits.
	const want = "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	if got := relayapi.HashContent([]byte("hello")); got != want {
		t.Fatalf("HashContent = %q, want %q", got, want)
	}
	if err := relayapi.ValidateContentHash(relayapi.HashContent(nil)); err != nil {
		t.Fatalf("canonical hash must validate: %v", err)
	}
}

func TestContentHasherMatchesOneShotHash(t *testing.T) {
	t.Parallel()

	// Multipart bodies stream each part into the hash in order, so the
	// incremental hasher must agree with hashing the concatenation.
	request := readFixture(t, "stt-transcription-request.json")
	audio := []byte("fake-pcm-bytes")

	hasher := relayapi.NewContentHasher()
	for _, part := range [][]byte{request, audio} {
		if _, err := hasher.Write(part); err != nil {
			t.Fatalf("write part: %v", err)
		}
	}
	if got, want := hasher.Sum(), relayapi.HashContent(append(append([]byte{}, request...), audio...)); got != want {
		t.Fatalf("streamed hash = %q, want %q", got, want)
	}

	// A WebSocket session hashes exactly the configure text frame: the
	// helper is the same, the input is the raw frame bytes as sent.
	frame := []byte(`{"type":"session.configure","routing":{"mode":"auto","objective":"balanced"}}`)
	if got, want := relayapi.HashContent(frame), relayapi.HashContent(frame); got != want {
		t.Fatalf("configure-frame hash is not deterministic: %q vs %q", got, want)
	}
}

func TestValidateContentHash(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		value string
	}{
		{"missing prefix", strings.Repeat("a", 64)},
		{"wrong prefix", "sha512:" + strings.Repeat("a", 64)},
		{"short digest", "sha256:" + strings.Repeat("a", 63)},
		{"long digest", "sha256:" + strings.Repeat("a", 65)},
		{"uppercase hex", "sha256:" + strings.Repeat("A", 64)},
		{"non-hex digest", "sha256:" + strings.Repeat("g", 64)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertInvalid(t, relayapi.ValidateContentHash(tc.value), "content hash")
		})
	}
}

func TestValidateIdempotencyKey(t *testing.T) {
	t.Parallel()

	if err := relayapi.ValidateIdempotencyKey(strings.Repeat("k", relayapi.MaxIdempotencyKeyLength)); err != nil {
		t.Fatalf("maximum-length key must validate: %v", err)
	}
	assertInvalid(t, relayapi.ValidateIdempotencyKey(""), "required")
	assertInvalid(t, relayapi.ValidateIdempotencyKey("   "), "required")
	assertInvalid(t, relayapi.ValidateIdempotencyKey(strings.Repeat("k", relayapi.MaxIdempotencyKeyLength+1)), "must not exceed")
}
