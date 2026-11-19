package cartesia

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

// Frames as Cartesia sends them (measured live 2026-11-20).
func chunkFrame(contextID string, audio []byte) map[string]any {
	return map[string]any{"type": "chunk", "context_id": contextID, "status_code": 206, "done": false, "data": base64.StdEncoding.EncodeToString(audio)}
}

func timestampsFrame(contextID, word string, start, end float64) map[string]any {
	return map[string]any{"type": "timestamps", "context_id": contextID, "status_code": 206, "done": false, "word_timestamps": map[string]any{"words": []string{word}, "start": []float64{start}, "end": []float64{end}}}
}

func flushDoneFrame(contextID string) map[string]any {
	return map[string]any{"type": "flush_done", "context_id": contextID, "status_code": 206, "done": false, "flush_done": true}
}

func doneFrame(contextID string) map[string]any {
	return map[string]any{"type": "done", "context_id": contextID, "status_code": 200, "done": true}
}

func writeFrames(ctx context.Context, t *testing.T, conn *websocket.Conn, frames ...map[string]any) bool {
	for _, frame := range frames {
		if err := writeJSON(ctx, conn, frame); err != nil {
			t.Errorf("write %v: %v", frame["type"], err)
			return false
		}
	}
	return true
}

// nextEvents reads n events, errors included.
func nextEvents(t *testing.T, events <-chan runtimepkg.ProviderEvent, n int) []runtimepkg.ProviderEvent {
	t.Helper()
	out := make([]runtimepkg.ProviderEvent, 0, n)
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for len(out) < n {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatalf("events closed after %d of %d", len(out), n)
			}
			out = append(out, event)
		case <-timer.C:
			t.Fatalf("timed out after %d of %d events", len(out), n)
		}
	}
	return out
}

func describe(events []runtimepkg.ProviderEvent) string {
	parts := make([]string, len(events))
	for i, event := range events {
		switch {
		case event.Err != nil:
			parts[i] = "error"
		case event.Type == protocol.EventAudioFrame:
			parts[i] = "audio.frame" + string(rune('0'+event.Audio[0]))
		case event.Type == protocol.EventWarning && strings.Contains(string(event.Data), `"dropped":true`):
			parts[i] = "dropped"
		default:
			parts[i] = string(event.Type)
		}
	}
	return strings.Join(parts, ",")
}

// The review's sequence with the real adapter: the start of A is spoken; a
// continuation of A and its commit go out before the adapter reads the
// expiry done Cartesia already sent for A's context; Cartesia then speaks the
// continuation on a re-created context. Its audio must never be emitted, least
// of all inside the next utterance: the utterance fails with a retryable error
// before any done, and nothing is re-sent.
func TestAdapterFailsAContinuationThatCrossedTheExpiryInsteadOfMisplacingIt(t *testing.T) {
	t.Parallel()

	server := newTTSServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		start, err := readGeneration(ctx, conn)
		if err != nil {
			t.Errorf("start: %v", err)
			return
		}
		if !writeFrames(ctx, t, conn, chunkFrame(start.ContextID, []byte{1}), timestampsFrame(start.ContextID, "Start", 0, 0.1), flushDoneFrame(start.ContextID)) {
			return
		}
		for range 2 { // the continuation and the commit
			if _, err := readGeneration(ctx, conn); err != nil {
				t.Errorf("continuation: %v", err)
				return
			}
		}
		writeFrames(ctx, t, conn, doneFrame(start.ContextID), chunkFrame(start.ContextID, []byte{2}), timestampsFrame(start.ContextID, "continuation", 0, 0.1), doneFrame(start.ContextID))
		waitForClientClose(ctx, conn)
	})
	defer server.Close()

	providerStream := openTestStream(t, server.URL)
	defer func() { _ = providerStream.Close(context.Background()) }()
	if err := providerStream.AppendText(context.Background(), "Start of A, "); err != nil {
		t.Fatalf("append: %v", err)
	}
	if got := describe(nextEvents(t, providerStream.Events(), 4)); got != "audio.started,audio.frame1,alignment,warning" {
		t.Fatalf("start of A = %s", got)
	}
	if err := providerStream.AppendText(context.Background(), "continuation of A."); err != nil {
		t.Fatalf("continuation: %v", err)
	}
	if err := providerStream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	events := nextEvents(t, providerStream.Events(), 1)
	var failure *runtimepkg.ProviderError
	if !errors.As(events[0].Err, &failure) || !failure.Retryable || failure.Code != "provider_unavailable" || failure.Hint == "" {
		t.Fatalf("after the crossing = %s %+v; want a retryable error before any done or audio", describe(events), events[0].Err)
	}
	for event := range providerStream.Events() {
		if event.Type == protocol.EventAudioFrame || event.Type == protocol.EventAudioDone || event.Type == protocol.EventAlignment {
			t.Fatalf("emitted %s after the failure", event.Type)
		}
	}
}

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func openClockedStream(t *testing.T, serverURL string, clock *testClock, settings map[string]any) runtimepkg.ProviderStream {
	t.Helper()
	adapter, err := New(testConfig(serverURL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	adapter.now = clock.Now
	request := adapterRequest(serverURL)
	request.Plan.Route.Model = "sonic-3.6"
	if settings != nil {
		request.Options.TTS = &protocol.TtsOptions{ProviderOptions: map[string]map[string]any{"cartesia": settings}}
	}
	providerStream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return providerStream
}

// A fragment after a pause continues the context while it is fresh, and opens
// a new context of the same utterance once the context has been quiet for 2 s
// (and past the buffer delay). The abandoned context is never written again;
// its expiry done arrives late and is dropped; word timings of the new
// context continue after the audio already sent.
func TestAdapterOpensANewContextOnlyAfterAQuietGap(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		gap        time.Duration
		settings   map[string]any
		newContext bool
	}{
		{"1.9 s, no buffering", 1900 * time.Millisecond, map[string]any{"max_buffer_delay_ms": 0.0}, false},
		{"2.1 s, no buffering", 2100 * time.Millisecond, map[string]any{"max_buffer_delay_ms": 0.0}, true},
		{"2.1 s, default 3 s buffering", 2100 * time.Millisecond, nil, false},
		{"3.1 s, default 3 s buffering", 3100 * time.Millisecond, nil, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			changed := make(chan bool, 1)
			server := newTTSServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
				first, err := readGeneration(ctx, conn)
				if err != nil {
					t.Errorf("first: %v", err)
					return
				}
				// Half a second of 16 kHz mono pcm_s16le.
				halfSecond := make([]byte, 16_000)
				halfSecond[0] = 1
				if !writeFrames(ctx, t, conn, chunkFrame(first.ContextID, halfSecond), timestampsFrame(first.ContextID, "Hello", 0, 0.4), flushDoneFrame(first.ContextID)) {
					return
				}
				next, err := readGeneration(ctx, conn)
				if err != nil {
					t.Errorf("continuation: %v", err)
					return
				}
				changed <- next.ContextID != first.ContextID
				commit, err := readGeneration(ctx, conn)
				if err != nil || commit.Continue || commit.ContextID != next.ContextID {
					t.Errorf("commit = %+v, %v; want continue:false on %s", commit, err, next.ContextID)
					return
				}
				if next.ContextID == first.ContextID {
					writeFrames(ctx, t, conn, chunkFrame(next.ContextID, []byte{2}), timestampsFrame(next.ContextID, "world", 0.5, 0.8), doneFrame(next.ContextID))
				} else {
					// The abandoned context's expiry done arrives late.
					writeFrames(ctx, t, conn, doneFrame(first.ContextID), chunkFrame(next.ContextID, []byte{2}), timestampsFrame(next.ContextID, "world", 0, 0.3), doneFrame(next.ContextID))
				}
				waitForClientClose(ctx, conn)
			})
			defer server.Close()

			clock := &testClock{now: time.Date(2026, time.November, 20, 12, 0, 0, 0, time.UTC)}
			providerStream := openClockedStream(t, server.URL, clock, test.settings)
			defer func() { _ = providerStream.Close(context.Background()) }()
			if err := providerStream.AppendText(context.Background(), "Hello, "); err != nil {
				t.Fatalf("append: %v", err)
			}
			if got := describe(nextEvents(t, providerStream.Events(), 4)); got != "audio.started,audio.frame1,alignment,warning" {
				t.Fatalf("first fragment = %s", got)
			}
			clock.Advance(test.gap)
			if err := providerStream.AppendText(context.Background(), "world."); err != nil {
				t.Fatalf("append after %v: %v", test.gap, err)
			}
			if err := providerStream.CommitText(context.Background()); err != nil {
				t.Fatalf("commit: %v", err)
			}
			want := "audio.frame2,alignment,audio.done"
			if test.newContext {
				want = "dropped," + want
			}
			events := nextEvents(t, providerStream.Events(), strings.Count(want, ",")+1)
			if got := describe(events); got != want {
				t.Fatalf("after %v = %s; want %s", test.gap, got, want)
			}
			if span := spansOf(t, events[len(events)-2])[0]; span != (protocol.TimingSpan{Text: "world", StartMS: 500, EndMS: 800}) {
				t.Errorf("continued timing = %+v; want 500-800 ms", span)
			}
			if got := <-changed; got != test.newContext {
				t.Errorf("continuation on a new context = %t; want %t", got, test.newContext)
			}
		})
	}
}

// A stale frame of the previous utterance's context that arrives inside the
// next utterance is dropped, never emitted as audio or timing.
func TestAdapterDropsAStaleFrameOfThePreviousUtterance(t *testing.T) {
	t.Parallel()
	server := newTTSServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		a, err := readGeneration(ctx, conn)
		if err != nil {
			t.Errorf("a: %v", err)
			return
		}
		if _, err := readGeneration(ctx, conn); err != nil {
			t.Errorf("a commit: %v", err)
			return
		}
		if !writeFrames(ctx, t, conn, chunkFrame(a.ContextID, []byte{1}), doneFrame(a.ContextID)) {
			return
		}
		b, err := readGeneration(ctx, conn)
		if err != nil || b.ContextID == a.ContextID {
			t.Errorf("b = %+v, %v", b, err)
			return
		}
		if !writeFrames(ctx, t, conn, chunkFrame(b.ContextID, []byte{2}), chunkFrame(a.ContextID, []byte{9}), timestampsFrame(a.ContextID, "stale", 0, 0.1), chunkFrame(b.ContextID, []byte{3})) {
			return
		}
		if _, err := readGeneration(ctx, conn); err != nil {
			t.Errorf("b commit: %v", err)
			return
		}
		writeFrames(ctx, t, conn, doneFrame(b.ContextID))
		waitForClientClose(ctx, conn)
	})
	defer server.Close()

	providerStream := openTestStream(t, server.URL)
	defer func() { _ = providerStream.Close(context.Background()) }()
	for _, step := range []struct {
		text string
		want string
	}{
		{"First.", "audio.started,audio.frame1,audio.done"},
		{"Second.", "audio.started,audio.frame2,dropped,dropped,audio.frame3,audio.done"},
	} {
		if err := providerStream.AppendText(context.Background(), step.text); err != nil {
			t.Fatalf("append %q: %v", step.text, err)
		}
		if err := providerStream.CommitText(context.Background()); err != nil {
			t.Fatalf("commit %q: %v", step.text, err)
		}
		if got := describe(nextEvents(t, providerStream.Events(), strings.Count(step.want, ",")+1)); got != step.want {
			t.Fatalf("%q = %s; want %s", step.text, got, step.want)
		}
	}
}

// Text that ends with no audio is a retryable error, not a bare done: at the
// done of the committed context, and at a commit after Cartesia expired the
// context.
func TestAdapterReportsAnUtteranceThatProducedNoAudio(t *testing.T) {
	t.Parallel()
	for _, expired := range []bool{false, true} {
		server := newTTSServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
			fragment, err := readGeneration(ctx, conn)
			if err != nil {
				t.Errorf("fragment: %v", err)
				return
			}
			if expired {
				writeFrames(ctx, t, conn, flushDoneFrame(fragment.ContextID), doneFrame(fragment.ContextID), map[string]any{"type": "test_marker"})
			} else if _, err := readGeneration(ctx, conn); err == nil {
				writeFrames(ctx, t, conn, flushDoneFrame(fragment.ContextID), doneFrame(fragment.ContextID))
			}
			waitForClientClose(ctx, conn)
		})
		providerStream := openTestStream(t, server.URL)
		if err := providerStream.AppendText(context.Background(), "Hello."); err != nil {
			t.Fatalf("append: %v", err)
		}
		var failure *runtimepkg.ProviderError
		if expired {
			_ = nextEvents(t, providerStream.Events(), 2) // flush_done, marker
			err := providerStream.CommitText(context.Background())
			if !errors.As(err, &failure) {
				t.Fatalf("commit after expiry without audio = %v; want a provider error", err)
			}
		} else {
			if err := providerStream.CommitText(context.Background()); err != nil {
				t.Fatalf("commit: %v", err)
			}
			events := nextEvents(t, providerStream.Events(), 2)
			if !errors.As(events[1].Err, &failure) {
				t.Fatalf("events = %s; want flush_done then an error", describe(events))
			}
		}
		if !failure.Retryable || !strings.Contains(failure.Message, "no audio") {
			t.Errorf("expired=%t: error = %+v", expired, failure)
		}
		_ = providerStream.Close(context.Background())
		server.Close()
	}
}

// A commit with nothing appended, a second commit while the utterance is
// finishing, and one after it finished, return runtime.ErrNothingToCommit and
// leave the session usable.
func TestAdapterCommitWithNothingToCommitKeepsTheSession(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	server := newTTSServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		fragment, err := readGeneration(ctx, conn)
		if err != nil {
			t.Errorf("fragment: %v", err)
			return
		}
		if _, err := readGeneration(ctx, conn); err != nil {
			t.Errorf("commit: %v", err)
			return
		}
		<-release
		writeFrames(ctx, t, conn, chunkFrame(fragment.ContextID, []byte{1}), doneFrame(fragment.ContextID))
		waitForClientClose(ctx, conn)
	})
	defer server.Close()

	providerStream := openTestStream(t, server.URL)
	defer func() { _ = providerStream.Close(context.Background()) }()
	nothing := func(when string) {
		t.Helper()
		err := providerStream.CommitText(context.Background())
		if !errors.Is(err, runtimepkg.ErrNothingToCommit) || errors.Is(err, runtimepkg.ErrSessionClosed) {
			t.Fatalf("commit %s = %v; want ErrNothingToCommit", when, err)
		}
	}
	nothing("before any append")
	if err := providerStream.AppendText(context.Background(), "Hello."); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := providerStream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	nothing("while the utterance finishes")
	close(release)
	if got := describe(nextEvents(t, providerStream.Events(), 3)); got != "audio.started,audio.frame1,audio.done" {
		t.Fatalf("utterance = %s", got)
	}
	nothing("after the utterance finished")
}

// The two orders in which a socket can end around the first write of a
// fragment. Either way the client hears about it: an EOF seen while the write
// is in progress makes the write's outcome unknown, and an EOF right after a
// completed write finds speech in flight.
func TestAdapterReportsASocketLostAroundTheFirstWrite(t *testing.T) {
	t.Parallel()
	for _, eofDuringWrite := range []bool{true, false} {
		server := newTTSServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
			waitForClientClose(ctx, conn)
		})
		s := openTestStream(t, server.URL).(*stream)
		contextID, conn, err := s.beginFragment("Hello.")
		if err != nil || conn == nil {
			t.Fatalf("begin = %v, %v", conn, err)
		}
		if eofDuringWrite {
			s.socketEnded(conn, io.EOF)
		}
		sendErr := s.send(context.Background(), conn, s.generationRequest(contextID, "Hello.", true))
		var failure *runtimepkg.ProviderError
		if eofDuringWrite {
			if !errors.As(sendErr, &failure) || !failure.Retryable || !strings.Contains(failure.Message, "unknown") {
				t.Errorf("EOF during the write: send = %v; want an unknown-outcome retryable error", sendErr)
			}
		} else {
			if sendErr != nil {
				t.Fatalf("send: %v", sendErr)
			}
			s.socketEnded(conn, io.EOF)
			events := nextEvents(t, s.Events(), 1)
			if !errors.As(events[0].Err, &failure) || !failure.Retryable {
				t.Errorf("EOF after the write: event = %+v; want a retryable error", events[0])
			}
		}
		_ = s.Abort(context.Background())
		server.Close()
	}
}

// A managed provider-direct session authenticates with a short-lived token in
// the URL. A new socket after Cartesia closed the idle one must carry a
// current token from the refresher, not the first one.
func TestAdapterRedialsAManagedRouteWithACurrentToken(t *testing.T) {
	t.Parallel()
	var connections atomic.Int32
	tokens := make(chan string, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := r.URL.Query().Get("access_token")
		tokens <- token
		number := connections.Add(1)
		if (number == 1 && token != "token-1") || (number > 1 && token != "token-2") {
			http.Error(w, "expired token", http.StatusUnauthorized)
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		go func() {
			defer conn.CloseNow()
			ctx := context.Background()
			if number == 1 {
				_ = conn.Close(websocket.StatusNormalClosure, "idle")
				return
			}
			fragment, err := readGeneration(ctx, conn)
			if err != nil {
				return
			}
			if _, err := readGeneration(ctx, conn); err != nil {
				return
			}
			writeFrames(ctx, t, conn, chunkFrame(fragment.ContextID, []byte{1}), doneFrame(fragment.ContextID))
			waitForClientClose(ctx, conn)
		}()
	}))
	defer server.Close()

	adapter, err := New(testConfig(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	var refreshes atomic.Int32
	request := adapterRequest(server.URL)
	request.Plan.Execution.CredentialSource = protocol.CredentialsManaged
	request.Plan.Route.Credential.Value = "token-1"
	request.RefreshCredential = func(context.Context) (protocol.DelegatedCredential, error) {
		refreshes.Add(1)
		return protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: "token-2", ExpiresAt: time.Now().Add(time.Hour)}, nil
	}
	providerStream, err := adapter.Open(context.Background(), request)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = providerStream.Close(context.Background()) }()
	waitForDisconnect(t, providerStream)
	if err := providerStream.AppendText(context.Background(), "Hello."); err != nil {
		t.Fatalf("append after the idle socket closed: %v", err)
	}
	if err := providerStream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if got := describe(nextEvents(t, providerStream.Events(), 3)); got != "audio.started,audio.frame1,audio.done" {
		t.Fatalf("events = %s", got)
	}
	if got := refreshes.Load(); got != 1 {
		t.Errorf("RefreshCredential called %d times; want once for the new socket", got)
	}
	if first, second := <-tokens, <-tokens; first != "token-1" || second != "token-2" {
		t.Errorf("handshake tokens = %q, %q; want token-1 then token-2", first, second)
	}

	// A refresher that fails makes the fragment fail retryably, before any
	// handshake with a stale token.
	var handshakes atomic.Int32
	idle := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handshakes.Add(1)
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		_ = conn.Close(websocket.StatusNormalClosure, "idle")
	}))
	defer idle.Close()
	idleAdapter, err := New(testConfig(idle.URL))
	if err != nil {
		t.Fatal(err)
	}
	failingRequest := adapterRequest(idle.URL)
	failingRequest.Plan.Execution.CredentialSource = protocol.CredentialsManaged
	failingRequest.Plan.Route.Credential.Value = "token-1"
	failingRequest.RefreshCredential = func(context.Context) (protocol.DelegatedCredential, error) {
		return protocol.DelegatedCredential{}, errors.New("control plane unavailable")
	}
	failing, err := idleAdapter.Open(context.Background(), failingRequest)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = failing.Close(context.Background()) }()
	waitForDisconnect(t, failing)
	var failure *runtimepkg.ProviderError
	if err := failing.AppendText(context.Background(), "Hello."); !errors.As(err, &failure) || !failure.Retryable || failure.Hint == "" {
		t.Errorf("append with a failing refresher = %v; want a retryable provider error with a hint", err)
	}
	if got := handshakes.Load(); got != 1 {
		t.Errorf("handshakes = %d; want only the first, none with a stale token", got)
	}
}
