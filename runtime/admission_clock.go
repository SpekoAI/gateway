package runtime

import (
	"sync"
	"time"
)

// A wall clock may move backwards after expired replay entries were removed.
// Keeping its observed high-water mark makes those expired plans stay expired.
// A frozen clock may retain entries longer; the admission caches have fixed
// entry limits and refuse new work instead of dropping still-valid entries.
type admissionClock struct {
	mu     sync.Mutex
	wall   func() time.Time
	latest time.Time
}

func newAdmissionClock(wall func() time.Time) *admissionClock { return &admissionClock{wall: wall} }
func (c *admissionClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.wall().UTC()
	if now.After(c.latest) {
		c.latest = now
	}
	return c.latest
}

// DefaultMaxReplayEntries bounds retained keys in each Engine/default verifier.
const DefaultMaxReplayEntries = 65_536
