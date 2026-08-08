package relayapi

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"strings"
)

// Idempotency-Key is required on every POST and on every WebSocket upgrade
// request. The relay persists only the key, a content hash, and the
// request's execution status — never the content itself — so replay
// detection stays content-free.
//
// The content hash pins what the key was used FOR:
//
//   - HTTP requests with a single-part body hash the raw body bytes exactly
//     as sent.
//   - Multipart bodies (POST /v1/stt/transcriptions) hash the decoded part
//     payload bytes ONLY — no part headers, no boundary or delimiter bytes —
//     concatenated in part order (request, then audio). Hashing the raw
//     multipart bytes would fold in the boundary string, which changes on
//     every send and would turn each identical retry into an
//     idempotency_conflict. The concatenation carries no separator, so the
//     hash deliberately pins the combined payload byte sequence rather than
//     the exact part split: two requests that shift bytes across the
//     request/audio border collide, an accepted looseness because both
//     parts come from the same caller under the same key.
//   - WebSocket sessions hash the exact bytes of the session.configure text
//     frame. Later frames are intentionally outside the hash: they are the
//     stream's continuous content, guarded by budgets and leases, while the
//     configure frame is the session's intent — the only part a retry must
//     reproduce byte-for-byte.
//
// Reuse semantics, in terms of the error codes in errors.go: the same key
// with the same hash while the original admission is still live returns
// request_in_progress (retryable — no second plan is minted while the first
// could still be consumed); after dispatch it returns
// request_already_started with the original request id, because stateless
// mode cannot replay output; the same key with a different hash returns
// idempotency_conflict.

// ContentHashPrefix starts every canonical content hash, matching the
// catalog digest convention so every digest in the system reads the same
// way.
const ContentHashPrefix = "sha256:"

// MaxIdempotencyKeyLength bounds the key so it can be persisted and indexed
// without truncation. The bound counts BYTES, not characters: persistence
// cares about storage width, so a multi-byte character spends as many of the
// 256 as it occupies. The specs' maxLength: 256 counts code points (JSON
// Schema cannot express bytes) and is therefore only an approximation; this
// byte bound is the normative one, stated in both spec descriptions.
const MaxIdempotencyKeyLength = 256

// HashContent returns the canonical content hash of a complete payload:
// "sha256:" followed by 64 lowercase hex digits.
func HashContent(content []byte) string {
	digest := sha256.Sum256(content)
	return ContentHashPrefix + hex.EncodeToString(digest[:])
}

// ContentHasher computes the canonical content hash incrementally. It exists
// for multipart bodies, whose parts are streamed into the hash in order
// without buffering the audio; hashing one complete buffer through it
// matches HashContent exactly.
type ContentHasher struct {
	digest hash.Hash
}

// NewContentHasher returns a hasher ready for the first byte.
func NewContentHasher() *ContentHasher {
	return &ContentHasher{digest: sha256.New()}
}

// Write feeds payload bytes into the hash. It implements io.Writer and never
// returns an error.
func (c *ContentHasher) Write(p []byte) (int, error) {
	return c.digest.Write(p)
}

// Sum returns the canonical content hash of everything written so far.
func (c *ContentHasher) Sum() string {
	return ContentHashPrefix + hex.EncodeToString(c.digest.Sum(nil))
}

// ValidateIdempotencyKey checks a caller-supplied key: non-blank and within
// the persistence bound. Keys are opaque; the relay imposes no format beyond
// that.
func ValidateIdempotencyKey(key string) error {
	if strings.TrimSpace(key) == "" {
		return fmt.Errorf("idempotency key: required")
	}
	if len(key) > MaxIdempotencyKeyLength {
		return fmt.Errorf("idempotency key: must not exceed %d bytes", MaxIdempotencyKeyLength)
	}
	return nil
}

// ValidateContentHash checks the canonical content hash format produced by
// HashContent.
func ValidateContentHash(value string) error {
	if !validSHA256Digest(value) {
		return fmt.Errorf("content hash: must be sha256:<64 lowercase hex>")
	}
	return nil
}

func validSHA256Digest(value string) bool {
	if !strings.HasPrefix(value, ContentHashPrefix) {
		return false
	}
	digest := value[len(ContentHashPrefix):]
	if len(digest) != 64 {
		return false
	}
	for _, r := range digest {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}
