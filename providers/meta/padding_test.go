package meta

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

// padFrameBytes24k is one paddingFrame of 24 kHz mono pcm_s16le.
const padFrameBytes24k = bytesPerSecond24k / 10

// padClock is a settable clock for the padding loop.
type padClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *padClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *padClock) set(now time.Time) {
	c.mu.Lock()
	c.now = now
	c.mu.Unlock()
}

// padHarness drives one stream's padding loop in lockstep: every tick or
// commit waits for the loop to report what that iteration did.
type padHarness struct {
	t          *testing.T
	fake       *fakeRealtime
	stream     *sttStream
	ctx        context.Context
	clock      *padClock
	start      time.Time
	ticks      chan time.Time
	iterations chan bool
}

func openPadded(t *testing.T, fake *fakeRealtime) *padHarness {
	t.Helper()
	adapter, err := NewSTT(STTConfig{AllowedEndpointHosts: []string{fake.host()}, AllowInsecureEndpoint: true, SetupTimeout: 5 * time.Second, CloseDrainTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	h := &padHarness{
		t: t, fake: fake,
		start:      time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC),
		ticks:      make(chan time.Time),
		iterations: make(chan bool, 64),
	}
	h.clock = &padClock{now: h.start}
	adapter.now = h.clock.Now
	adapter.newTicker = func(period time.Duration) (<-chan time.Time, func()) {
		if period != paddingFrame {
			t.Errorf("padding ticker period = %v, want %v", period, paddingFrame)
		}
		return h.ticks, func() {}
	}
	adapter.afterPad = func(wrote bool) { h.iterations <- wrote }
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	stream, err := adapter.Open(ctx, realtimeRequest(fake.endpoint()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) })
	h.stream, h.ctx = stream.(*sttStream), ctx
	return h
}

// at moves the clock to start+offset.
func (h *padHarness) at(offset time.Duration) { h.clock.set(h.start.Add(offset)) }

// tick fires the padding ticker at start+offset and reports whether that
// iteration wrote silence.
func (h *padHarness) tick(offset time.Duration) bool {
	h.t.Helper()
	h.at(offset)
	select {
	case h.ticks <- h.clock.Now():
	case <-time.After(5 * time.Second):
		h.t.Fatalf("padding loop did not take the tick at %v", offset)
	}
	return h.iteration()
}

func (h *padHarness) iteration() bool {
	h.t.Helper()
	select {
	case wrote := <-h.iterations:
		return wrote
	case <-time.After(5 * time.Second):
		h.t.Fatal("padding loop did not finish an iteration")
		return false
	}
}

// caller writes n bytes of non-silent caller audio at start+offset.
func (h *padHarness) caller(offset time.Duration, n int) {
	h.t.Helper()
	h.at(offset)
	if err := h.stream.WriteAudio(h.ctx, bytes.Repeat([]byte{1}, n)); err != nil {
		h.t.Fatal(err)
	}
}

func (h *padHarness) expectTicks(want ...tickWant) {
	h.t.Helper()
	for _, step := range want {
		if got := h.tick(step.at); got != step.pads {
			h.t.Fatalf("tick at %v padded = %v, want %v", step.at, got, step.pads)
		}
	}
}

type tickWant struct {
	at   time.Duration
	pads bool
}

func ms(n int) time.Duration { return time.Duration(n) * time.Millisecond }

// framesReceived waits until the fake service has received n audio frames
// and returns them.
func (h *padHarness) framesReceived(n int) [][]byte {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		h.fake.mu.Lock()
		frames := append([][]byte(nil), h.fake.audio...)
		h.fake.mu.Unlock()
		if len(frames) >= n {
			if len(frames) > n {
				h.t.Fatalf("service received %d audio frames, want %d", len(frames), n)
			}
			return frames
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("service received %d audio frames, want %d", len(frames), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func isPadFrame(frame []byte, bytesPerSecond int) bool {
	return len(frame) == bytesPerSecond/10 && bytes.Count(frame, []byte{0}) == len(frame)
}

func firstFrame(frames [][]byte) string {
	if len(frames) == 0 {
		return ""
	}
	return string(frames[0])
}

// A gap in the caller's audio is filled with silence once it passes
// paddingGap, one 100 ms frame per 100 ms: exactly real time, never a frame
// ahead of the clock.
func TestPaddingFillsACallerGapAtRealTime(t *testing.T) {
	t.Parallel()
	h := openPadded(t, newFakeRealtime(t))
	h.caller(0, padFrameBytes24k) // plays out until 100 ms
	h.expectTicks(
		tickWant{ms(300), false},
		tickWant{ms(399), false},
		tickWant{ms(400), true},
		tickWant{ms(450), false}, // the frame written at 400 ms still plays
		tickWant{ms(500), true},
		tickWant{ms(600), true},
		tickWant{ms(600), false}, // a second tick at the same instant never runs ahead
	)
	if got := h.stream.paddedBytes.Load(); got != 3*padFrameBytes24k {
		t.Fatalf("padded bytes = %d, want three 100 ms frames", got)
	}
	frames := h.framesReceived(4)
	if frames[0][0] != 1 {
		t.Fatal("the caller's frame was not first")
	}
	for index, frame := range frames[1:] {
		if !isPadFrame(frame, bytesPerSecond24k) {
			t.Fatalf("frame %d is %d bytes, want 100 ms of silence", index+1, len(frame))
		}
	}
}

// Caller audio stops padding at once, and a new gap must pass paddingGap
// again before padding resumes.
func TestPaddingStopsWhenCallerAudioResumes(t *testing.T) {
	t.Parallel()
	h := openPadded(t, newFakeRealtime(t))
	h.caller(0, padFrameBytes24k)
	h.expectTicks(tickWant{ms(400), true})
	h.caller(ms(450), padFrameBytes24k)
	h.expectTicks(
		tickWant{ms(450), false},
		tickWant{ms(500), false},
		tickWant{ms(849), false},
		tickWant{ms(850), true},
	)
	frames := h.framesReceived(4)
	for index, wantCaller := range []bool{true, false, true, false} {
		if isCaller := frames[index][0] == 1; isCaller != wantCaller {
			t.Fatalf("frame %d caller = %v, want %v (caller, pad, caller, pad)", index, isCaller, wantCaller)
		}
	}
}

// A commit pads at once, without waiting out paddingGap, so the service's
// endpointing sees the turn end; it still never pads over caller audio that
// has not played out yet.
func TestCommitStartsPaddingImmediately(t *testing.T) {
	t.Parallel()
	t.Run("after the caller's audio played out", func(t *testing.T) {
		t.Parallel()
		h := openPadded(t, newFakeRealtime(t))
		h.caller(0, padFrameBytes24k)
		h.at(ms(100))
		if err := h.stream.CommitAudio(h.ctx); err != nil {
			t.Fatal(err)
		}
		if !h.iteration() {
			t.Fatal("commit did not pad at once")
		}
		h.expectTicks(tickWant{ms(150), false}, tickWant{ms(200), true})
	})
	t.Run("while the caller's audio still plays", func(t *testing.T) {
		t.Parallel()
		h := openPadded(t, newFakeRealtime(t))
		h.caller(0, 5*padFrameBytes24k) // plays out until 500 ms
		h.at(ms(100))
		if err := h.stream.CommitAudio(h.ctx); err != nil {
			t.Fatal(err)
		}
		if h.iteration() {
			t.Fatal("commit padded ahead of the caller's audio")
		}
		h.expectTicks(tickWant{ms(400), false}, tickWant{ms(500), true})
	})
}

// Once Close has sent endStream no silence follows it, and the loop ends.
func TestNoPaddingAfterEndStream(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.holdOpenAfterEndStream = true
	h := openPadded(t, fake)
	h.caller(0, padFrameBytes24k)
	h.expectTicks(tickWant{ms(400), true})
	if err := h.stream.Close(h.ctx); err != nil {
		t.Fatal(err)
	}
	h.expectTicks(tickWant{ms(500), false})
	select {
	case h.ticks <- h.start.Add(ms(600)):
		t.Fatal("padding loop still running after endStream")
	case <-time.After(100 * time.Millisecond):
	}
	if got := h.stream.paddedBytes.Load(); got != padFrameBytes24k {
		t.Fatalf("padded bytes = %d after Close, want only the frame before it", got)
	}
	h.fake.mu.Lock()
	defer h.fake.mu.Unlock()
	if !h.fake.endStream || h.fake.audioAfterEndStream != 0 {
		t.Fatalf("endStream = %v, audio after it = %d frames, want endStream and none after", h.fake.endStream, h.fake.audioAfterEndStream)
	}
}

// The service processes, and charges for, the padding, but the customer is
// billed only the audio they sent: 1 s of caller audio plus 2 s of padding
// that the service reports as 3 s processed bills 1 s.
func TestPaddingIsNeverBilled(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.closingReplies = []string{`{"type":"audioProgress","audioProcessedMs":3000}`}
	h := openPadded(t, fake)
	for range 10 {
		h.caller(0, padFrameBytes24k) // a 1 s burst, playing out until 1000 ms
	}
	for offset := 1000; offset < 3000; offset += 100 {
		h.expectTicks(tickWant{ms(offset), true})
	}
	if got := h.stream.paddedBytes.Load(); got != 2*bytesPerSecond24k {
		t.Fatalf("padded bytes = %d, want 2 s", got)
	}
	if err := h.stream.Close(h.ctx); err != nil {
		t.Fatal(err)
	}
	observations := drainBilling(t, h.stream, false)
	final := mergeLikeConnector(t, observations)
	if final.Quantities["duration_seconds"] != 1000 || !final.Complete {
		t.Fatalf("final = %+v, want a complete 1000 ms: the caller's audio only", final)
	}
}

// The production clock and ticker pad a stalled caller on their own.
func TestPaddingRunsOnTheRealClock(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	stream, ctx := openRealtime(t, fake, realtimeRequest(fake.endpoint()))
	if err := stream.WriteAudio(ctx, []byte("caller")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		fake.mu.Lock()
		frames := append([][]byte(nil), fake.audio...)
		fake.mu.Unlock()
		if len(frames) >= 3 {
			for index, frame := range frames[1:] {
				if !isPadFrame(frame, bytesPerSecond24k) {
					t.Fatalf("frame %d is %d bytes, want 100 ms of silence", index+1, len(frame))
				}
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("service received %d frames, want the caller's and padding after it", len(frames))
		}
		time.Sleep(20 * time.Millisecond)
	}
}
