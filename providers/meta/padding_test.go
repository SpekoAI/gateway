package meta

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
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
	iterations chan int
	// callerBytes is the caller audio written so far.
	callerBytes int
}

// openPadded opens a stream on a fake clock and ticker; tune adjusts the
// adapter before Open.
func openPadded(t *testing.T, fake *fakeRealtime, tune ...func(*STTAdapter)) *padHarness {
	t.Helper()
	adapter, err := NewSTT(STTConfig{AllowedEndpointHosts: []string{fake.host()}, AllowInsecureEndpoint: true, SetupTimeout: 5 * time.Second, CloseDrainTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	h := &padHarness{
		t: t, fake: fake,
		start:      time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC),
		ticks:      make(chan time.Time),
		iterations: make(chan int, 64),
	}
	h.clock = &padClock{now: h.start}
	adapter.now = h.clock.Now
	adapter.newTicker = func(period time.Duration) (<-chan time.Time, func()) {
		if period != paddingFrame {
			t.Errorf("padding ticker period = %v, want %v", period, paddingFrame)
		}
		return h.ticks, func() {}
	}
	adapter.afterPad = func(frames int) { h.iterations <- frames }
	for _, apply := range tune {
		apply(adapter)
	}
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

// tick fires the padding ticker at start+offset and reports how many frames
// of silence that iteration wrote.
func (h *padHarness) tick(offset time.Duration) int {
	h.t.Helper()
	h.at(offset)
	select {
	case h.ticks <- h.clock.Now():
	case <-time.After(5 * time.Second):
		h.t.Fatalf("padding loop did not take the tick at %v", offset)
	}
	return h.iteration()
}

func (h *padHarness) iteration() int {
	h.t.Helper()
	select {
	case frames := <-h.iterations:
		return frames
	case <-time.After(5 * time.Second):
		h.t.Fatal("padding loop did not finish an iteration")
		return 0
	}
}

// commit calls CommitAudio at start+offset and reports how many frames the
// iteration it starts wrote.
func (h *padHarness) commit(offset time.Duration) int {
	h.t.Helper()
	h.at(offset)
	if err := h.stream.CommitAudio(h.ctx); err != nil {
		h.t.Fatal(err)
	}
	return h.iteration()
}

// caller writes n bytes of non-silent caller audio at start+offset.
func (h *padHarness) caller(offset time.Duration, n int) {
	h.t.Helper()
	h.at(offset)
	if err := h.stream.WriteAudio(h.ctx, bytes.Repeat([]byte{1}, n)); err != nil {
		h.t.Fatal(err)
	}
	h.callerBytes += n
}

// lag is how far everything written, caller audio and padding, trails
// start+offset.
func (h *padHarness) lag(offset time.Duration) time.Duration {
	written := int64(h.callerBytes) + h.stream.paddedBytes.Load()
	return offset - time.Duration(written*int64(time.Second)/bytesPerSecond24k)
}

func (h *padHarness) expectTicks(want ...tickWant) {
	h.t.Helper()
	for _, step := range want {
		if got := h.tick(step.at); got != step.frames {
			h.t.Fatalf("tick at %v padded %d frames, want %d", step.at, got, step.frames)
		}
	}
}

type tickWant struct {
	at     time.Duration
	frames int
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

// Once the audio written trails the clock by paddingGap, padding fills the
// whole gap up to the clock, at most one frame ahead, and then keeps the
// stream at real time, one 100 ms frame per 100 ms.
func TestPaddingFillsACallerGapAtRealTime(t *testing.T) {
	t.Parallel()
	h := openPadded(t, newFakeRealtime(t))
	h.caller(0, padFrameBytes24k) // plays out until 100 ms
	h.expectTicks(
		tickWant{ms(300), 0},
		tickWant{ms(499), 0},
		tickWant{ms(500), 5}, // 100..600 ms: the 400 ms gap and one frame ahead
		tickWant{ms(550), 0}, // the frame ahead still plays
		tickWant{ms(600), 1},
		tickWant{ms(600), 0}, // a second tick at the same instant never runs ahead
		tickWant{ms(700), 1},
	)
	if got := h.stream.paddedBytes.Load(); got != 7*padFrameBytes24k {
		t.Fatalf("padded bytes = %d, want seven 100 ms frames", got)
	}
	frames := h.framesReceived(8)
	if frames[0][0] != 1 {
		t.Fatal("the caller's frame was not first")
	}
	for index, frame := range frames[1:] {
		if !isPadFrame(frame, bytesPerSecond24k) {
			t.Fatalf("frame %d is %d bytes, want 100 ms of silence", index+1, len(frame))
		}
	}
}

// Caller audio that arrives in short bursts with gaps between them (100 ms
// every 500 ms) never leaves the stream more than paddingGap behind the
// clock: each gap is filled in full, not just from when padding started.
func TestPaddingKeepsIntermittentCallerAudioAtRealTime(t *testing.T) {
	t.Parallel()
	h := openPadded(t, newFakeRealtime(t))
	for burst := range 10 {
		at := time.Duration(burst) * 500 * time.Millisecond
		h.caller(at, padFrameBytes24k)
		for step := 1; step < 5; step++ {
			offset := at + time.Duration(step)*paddingFrame
			h.tick(offset)
			if lag := h.lag(offset); lag > paddingGap {
				t.Fatalf("at %v the stream trails the clock by %v, want at most %v", offset, lag, paddingGap)
			}
		}
	}
	h.commit(ms(4950))
	if lag := h.lag(ms(4950)); lag > 0 {
		t.Fatalf("after the commit the stream trails the clock by %v, want none", lag)
	}
}

// Caller audio stops padding at once, and the stream must trail the clock by
// paddingGap again before padding resumes.
func TestPaddingStopsWhenCallerAudioResumes(t *testing.T) {
	t.Parallel()
	h := openPadded(t, newFakeRealtime(t))
	h.caller(0, padFrameBytes24k)
	h.expectTicks(tickWant{ms(500), 5}) // written until 600 ms
	h.caller(ms(550), padFrameBytes24k) // written until 700 ms
	h.expectTicks(
		tickWant{ms(600), 0},
		tickWant{ms(700), 0},
		tickWant{ms(1099), 0},
		tickWant{ms(1100), 5},
	)
	frames := h.framesReceived(12)
	for index, frame := range frames {
		wantCaller := index == 0 || index == 6
		if isCaller := frame[0] == 1; isCaller != wantCaller {
			t.Fatalf("frame %d caller = %v, want %v (caller, 5 pad, caller, 5 pad)", index, isCaller, wantCaller)
		}
	}
}

// A commit pads at once, without waiting for paddingGap, so the service's
// endpointing sees the turn end; it fills whatever gap there is, and still
// never pads over caller audio that has not played out yet.
func TestCommitStartsPaddingImmediately(t *testing.T) {
	t.Parallel()
	t.Run("after the caller's audio played out", func(t *testing.T) {
		t.Parallel()
		h := openPadded(t, newFakeRealtime(t))
		h.caller(0, padFrameBytes24k)
		if got := h.commit(ms(100)); got != 1 {
			t.Fatalf("commit padded %d frames, want 1", got)
		}
		h.expectTicks(tickWant{ms(150), 0}, tickWant{ms(200), 1})
	})
	t.Run("after a gap shorter than paddingGap", func(t *testing.T) {
		t.Parallel()
		h := openPadded(t, newFakeRealtime(t))
		h.caller(0, padFrameBytes24k)
		if got := h.commit(ms(300)); got != 3 {
			t.Fatalf("commit padded %d frames, want 3 (100..400 ms)", got)
		}
	})
	t.Run("while the caller's audio still plays", func(t *testing.T) {
		t.Parallel()
		h := openPadded(t, newFakeRealtime(t))
		h.caller(0, 5*padFrameBytes24k) // plays out until 500 ms
		if got := h.commit(ms(100)); got != 0 {
			t.Fatalf("commit padded %d frames ahead of the caller's audio", got)
		}
		h.expectTicks(tickWant{ms(400), 0}, tickWant{ms(500), 1})
	})
}

// One stretch of padding stops at paddingStretchCap, so an idle client
// cannot keep the service running; a commit does not restart it, and only
// caller audio starts a new stretch, without making up the time padding
// gave up on.
func TestPaddingStopsAtTheStretchCap(t *testing.T) {
	t.Parallel()
	h := openPadded(t, newFakeRealtime(t))
	frames := int(paddingStretchCap / paddingFrame)
	h.caller(0, padFrameBytes24k)
	h.expectTicks(
		tickWant{ms(500), 5},
		tickWant{paddingStretchCap + 10*time.Second, frames - 5},
		tickWant{200 * time.Second, 0},
	)
	if got := h.commit(200 * time.Second); got != 0 {
		t.Fatalf("a commit restarted padding past the cap: %d frames", got)
	}

	h.caller(200*time.Second, padFrameBytes24k)
	h.expectTicks(
		tickWant{200*time.Second + ms(400), 0},
		tickWant{200*time.Second + ms(500), 5},
	)
	if got := h.stream.paddedBytes.Load(); got != int64((frames+5)*padFrameBytes24k) {
		t.Fatalf("padded bytes = %d, want the capped stretch and a new 500 ms one", got)
	}
}

// Once Close has sent endStream no silence follows it, and the loop ends.
func TestNoPaddingAfterEndStream(t *testing.T) {
	t.Parallel()
	fake := newFakeRealtime(t)
	fake.holdOpenAfterEndStream = true
	h := openPadded(t, fake)
	h.caller(0, padFrameBytes24k)
	h.expectTicks(tickWant{ms(500), 5})
	if err := h.stream.Close(h.ctx); err != nil {
		t.Fatal(err)
	}
	h.expectTicks(tickWant{ms(600), 0})
	select {
	case h.ticks <- h.start.Add(ms(700)):
		t.Fatal("padding loop still running after endStream")
	case <-time.After(100 * time.Millisecond):
	}
	if got := h.stream.paddedBytes.Load(); got != 5*padFrameBytes24k {
		t.Fatalf("padded bytes = %d after Close, want only the frames before it", got)
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
		h.caller(0, padFrameBytes24k) // a 1 s burst, written until 1000 ms
	}
	h.expectTicks(tickWant{ms(1400), 5})
	for offset := 1500; offset < 3000; offset += 100 {
		h.expectTicks(tickWant{ms(offset), 1})
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

// The service's processed position counts padding wherever it sits, so the
// billed caller audio is that position less the padding inside it, and the
// evidence is complete only once the caller's own audio is covered. A
// position that has reached only padding ahead of later caller audio bills
// what precedes it and stays incomplete through a drain timeout.
func TestBilledCallerAudioExcludesPaddingWhereverItSits(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		layout   func(h *padHarness) // ends with the stream at 4 s (padding first) or 7 s
		reported int
		hangUp   bool
		billed   int64
		complete bool
	}{
		{
			name: "padding between turns, progress stalled inside it", layout: callerPadCaller, reported: 4000,
			billed: 1000,
		},
		{
			name: "padding between turns, everything processed", layout: callerPadCaller, reported: 7000, hangUp: true,
			billed: 4000, complete: true,
		},
		{
			name: "padding before the caller's audio, progress stalled inside it", layout: padThenCaller, reported: 1000,
			billed: 0,
		},
		{
			name: "padding before the caller's audio, everything processed", layout: padThenCaller, reported: 2000, hangUp: true,
			billed: 1000, complete: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fake := newFakeRealtime(t)
			fake.closingReplies = []string{fmt.Sprintf(`{"type":"audioProgress","audioProcessedMs":%d}`, test.reported)}
			fake.holdOpenAfterEndStream = !test.hangUp
			h := openPadded(t, fake, func(a *STTAdapter) { a.closeDrainTimeout = 200 * time.Millisecond })
			test.layout(h)
			if err := h.stream.Close(h.ctx); err != nil {
				t.Fatal(err)
			}
			final := mergeLikeConnector(t, drainBilling(t, h.stream, false))
			if final.Quantities["duration_seconds"] != test.billed || final.Complete != test.complete {
				t.Fatalf("final = %+v, want %d ms, complete %v", final, test.billed, test.complete)
			}
		})
	}
}

// callerPadCaller writes 1 s of caller audio, 3 s of padding and 3 s more
// caller audio.
func callerPadCaller(h *padHarness) {
	for range 10 {
		h.caller(0, padFrameBytes24k)
	}
	h.expectTicks(tickWant{ms(1400), 5})
	for offset := 1500; offset < 4000; offset += 100 {
		h.expectTicks(tickWant{ms(offset), 1})
	}
	for range 30 {
		h.caller(ms(4000), padFrameBytes24k)
	}
}

// padThenCaller writes 1 s of padding before any caller audio, then 1 s of
// caller audio.
func padThenCaller(h *padHarness) {
	h.expectTicks(tickWant{ms(400), 5})
	for offset := 500; offset < 1000; offset += 100 {
		h.expectTicks(tickWant{ms(offset), 1})
	}
	for range 10 {
		h.caller(ms(1000), padFrameBytes24k)
	}
}

// A position the service reports counts padding too; published as
// audio_end_ms it is mapped onto the caller's audio, so 1 s of padding
// before a 100 ms caller frame does not make the frame end at 1100 ms.
func TestReportedPositionsMapOntoCallerAudio(t *testing.T) {
	t.Parallel()
	h := openPadded(t, newFakeRealtime(t))
	h.expectTicks(tickWant{ms(400), 5})
	for offset := 500; offset < 1000; offset += 100 {
		h.expectTicks(tickWant{ms(offset), 1})
	}
	h.caller(ms(1000), padFrameBytes24k) // written 1000..1100 ms
	h.expectTicks(tickWant{ms(1500), 5}) // padding 1100..1600 ms
	for _, step := range []struct{ processed, caller int64 }{
		{500, 0}, {1000, 0}, {1050, 50}, {1100, 100}, {1300, 100}, {1600, 100},
	} {
		if got := h.stream.callerMs(step.processed); got != step.caller {
			t.Fatalf("processed %d ms maps to %d ms of caller audio, want %d", step.processed, got, step.caller)
		}
	}
	var data map[string]any
	if err := json.Unmarshal(h.stream.turnData(serverMessage{AudioProcessedMs: 1100}, nil), &data); err != nil || data["audio_end_ms"] != float64(100) {
		t.Fatalf("turn data = %v (%v), want audio_end_ms 100", data, err)
	}
}

// A padding write the service never takes is bounded: it fails after
// padWriteTimeout, padding stops, and writeMu is free again for caller
// audio and teardown.
func TestStalledPaddingWriteEndsPadding(t *testing.T) {
	t.Parallel()
	const timeout = 50 * time.Millisecond
	deadlines := make(chan time.Duration, 1)
	h := openPadded(t, newFakeRealtime(t), func(a *STTAdapter) {
		a.padWriteTimeout = timeout
		a.padWriter = func(ctx context.Context, _ []byte) error {
			deadline, _ := ctx.Deadline()
			deadlines <- time.Until(deadline)
			<-ctx.Done()
			return ctx.Err()
		}
	})
	h.caller(0, padFrameBytes24k)
	if got := h.tick(ms(500)); got != 0 {
		t.Fatalf("a stalled write reported %d frames", got)
	}
	if left := <-deadlines; left <= 0 || left > timeout {
		t.Fatalf("padding write deadline %v away, want within %v", left, timeout)
	}
	select {
	case h.ticks <- h.start.Add(ms(600)):
		t.Fatal("padding loop still running after a failed write")
	case <-time.After(100 * time.Millisecond):
	}
	written := make(chan error, 1)
	go func() { written <- h.stream.WriteAudio(h.ctx, []byte{1, 1}) }()
	select {
	case <-written:
	case <-time.After(time.Second):
		t.Fatal("caller audio still waits behind the failed padding write")
	}
}

// Caller audio that arrives during a catch-up waits for it and follows the
// silence: the whole gap is filled before the resumed speech, nothing is
// spliced after it, and nothing is dropped, so the bytes written keep up
// with the clock.
func TestCallerAudioDuringACatchUpFollowsTheWholeGap(t *testing.T) {
	t.Parallel()
	const catchUp = 11 // a 1 s gap and one frame ahead
	started := make(chan struct{}, catchUp)
	proceed := make(chan struct{})
	var h *padHarness
	h = openPadded(t, newFakeRealtime(t), func(a *STTAdapter) {
		a.padWriter = func(ctx context.Context, frame []byte) error {
			started <- struct{}{}
			select {
			case <-proceed:
				return h.stream.conn.Write(ctx, websocket.MessageBinary, frame)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
	h.caller(0, padFrameBytes24k) // written until 100 ms
	h.at(ms(1100))
	padded := make(chan int, 1)
	go func() {
		h.ticks <- h.clock.Now()
		padded <- <-h.iterations
	}()
	<-started
	callerDone := make(chan error, 1)
	go func() { callerDone <- h.stream.WriteAudio(h.ctx, bytes.Repeat([]byte{1}, padFrameBytes24k)) }()
	for written := 1; written <= catchUp; written++ {
		time.Sleep(5 * time.Millisecond) // the caller waits on writeMu
		select {
		case <-callerDone:
			t.Fatalf("caller audio got in after %d of %d catch-up frames", written-1, catchUp)
		default:
		}
		proceed <- struct{}{}
		if written < catchUp {
			<-started
		}
	}
	if err := <-callerDone; err != nil {
		t.Fatal(err)
	}
	if got := <-padded; got != catchUp {
		t.Fatalf("catch-up wrote %d frames, want %d", got, catchUp)
	}
	h.callerBytes += padFrameBytes24k
	if lag := h.lag(ms(1100)); lag > 0 {
		t.Fatalf("after the catch-up and the caller's frame the stream trails the clock by %v", lag)
	}
	frames := h.framesReceived(catchUp + 2)
	for index, frame := range frames {
		wantCaller := index == 0 || index == catchUp+1
		if isCaller := frame[0] == 1; isCaller != wantCaller {
			t.Fatalf("frame %d caller = %v, want %v (caller, %d pad, caller)", index, isCaller, wantCaller, catchUp)
		}
	}
}

// A catch-up whose writes take time catches up to the clock as it moves,
// so the caller audio queued behind it lands on a stream that is current
// and no silence is due after it.
func TestSlowCatchUpEndsCurrentWithTheClock(t *testing.T) {
	t.Parallel()
	var h *padHarness
	h = openPadded(t, newFakeRealtime(t), func(a *STTAdapter) {
		a.padWriter = func(ctx context.Context, frame []byte) error {
			h.clock.set(h.clock.Now().Add(60 * time.Millisecond)) // each write takes 60 ms
			return h.stream.conn.Write(ctx, websocket.MessageBinary, frame)
		}
	})
	h.caller(0, padFrameBytes24k)
	frames := h.tick(ms(500))
	end := h.clock.Now().Sub(h.start)
	if lag := h.lag(end); lag > 0 {
		t.Fatalf("after a %d-frame catch-up ending at %v the stream trails the clock by %v, want none", frames, end, lag)
	}
	h.caller(end, padFrameBytes24k)
	if got := h.tick(end + paddingFrame); got != 0 {
		t.Fatalf("%d frames of silence followed the caller audio queued behind the catch-up", got)
	}
}

// One deadline bounds a whole catch-up, so writes that each succeed just in
// time cannot add up to holding writeMu for longer than padWriteTimeout.
func TestSlowPaddingWritesShareOneDeadline(t *testing.T) {
	t.Parallel()
	const timeout = 100 * time.Millisecond
	h := openPadded(t, newFakeRealtime(t), func(a *STTAdapter) {
		a.padWriteTimeout = timeout
		a.padWriter = func(ctx context.Context, _ []byte) error {
			select {
			case <-time.After(timeout * 2 / 3):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
	h.caller(0, padFrameBytes24k)
	began := time.Now()
	if got := h.tick(ms(500)); got != 1 {
		t.Fatalf("catch-up wrote %d frames, want 1 before its deadline", got)
	}
	if held := time.Since(began); held > 3*timeout {
		t.Fatalf("catch-up held writeMu for %v, want about %v", held, timeout)
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
