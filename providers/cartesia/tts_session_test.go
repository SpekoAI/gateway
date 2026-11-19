package cartesia

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

// Cartesia expires a context nobody committed and says so with an ordinary
// done frame. Its docs count one second from the context's last audio output;
// measured live (sonic-3.6, max_buffer_delay_ms 0) the done came 3.9 to 5.8 s
// after the last audio chunk arrived, 4.7 to 6.8 s after the last fragment
// was sent. A caller that pauses before input.commit (the
// production failure) must get its utterance finished, not a session-closed
// error, and must not see utterance.done before it commits. Cartesia answers
// a commit sent after expiry with 400 "Invalid transcript", so none is sent.
func TestAdapterCommitAfterCartesiaExpiredTheContextFinishesTheUtterance(t *testing.T) {
	t.Parallel()

	frames := make(chan generation, 4)
	server := newTTSServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		first, err := readGeneration(ctx, conn)
		if err != nil {
			t.Errorf("first generation: %v", err)
			return
		}
		frames <- first
		// Audio and words for the fragment, the flush_done Cartesia sends after
		// each fragment, its own expiry of the still-open context, then a
		// marker proving all were read.
		for _, message := range []map[string]any{
			{"type": "chunk", "context_id": first.ContextID, "data": base64.StdEncoding.EncodeToString([]byte{1, 2})},
			wordsFrame(first.ContextID, "Hello,"),
			{"type": "flush_done", "context_id": first.ContextID, "status_code": 206, "done": false, "flush_done": true},
			{"type": "done", "context_id": first.ContextID, "status_code": 200, "done": true},
			{"type": "test_marker", "context_id": first.ContextID},
		} {
			if err := writeJSON(ctx, conn, message); err != nil {
				t.Errorf("write %v: %v", message["type"], err)
				return
			}
		}
		// The commit for an expired context sends nothing; the next frame is
		// the next utterance on a fresh context.
		next, err := readGeneration(ctx, conn)
		if err != nil {
			t.Errorf("next generation: %v", err)
			return
		}
		frames <- next
		final, err := readGeneration(ctx, conn)
		if err != nil {
			t.Errorf("final generation: %v", err)
			return
		}
		frames <- final
		for _, message := range []map[string]any{
			{"type": "chunk", "context_id": next.ContextID, "data": base64.StdEncoding.EncodeToString([]byte{3})},
			wordsFrame(next.ContextID, "Again."),
			{"type": "done", "context_id": next.ContextID, "done": true},
		} {
			if err := writeJSON(ctx, conn, message); err != nil {
				t.Errorf("write %v: %v", message["type"], err)
				return
			}
		}
		waitForClientClose(ctx, conn)
	})
	defer server.Close()

	providerStream := openTestStream(t, server.URL)
	if err := providerStream.AppendText(context.Background(), "Hello, "); err != nil {
		t.Fatalf("append: %v", err)
	}
	if got := strings.Join(eventTypes(collectProviderEvents(t, providerStream.Events(), 5)), ","); got != "audio.started,audio.frame,alignment,warning,warning" {
		t.Fatalf("events before commit = %s; utterance.done must wait for the commit", got)
	}
	if err := providerStream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit after Cartesia expired the context: %v", err)
	}
	done := collectProviderEvents(t, providerStream.Events(), 1)
	if done[0].Type != protocol.EventAudioDone {
		t.Fatalf("event after commit = %s, want audio.done", done[0].Type)
	}
	first := <-frames
	if contextOf(t, done[0]) != first.ContextID {
		t.Fatalf("audio.done context = %s, want %s", contextOf(t, done[0]), first.ContextID)
	}

	if err := providerStream.AppendText(context.Background(), "Again."); err != nil {
		t.Fatalf("append next utterance: %v", err)
	}
	if err := providerStream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit next utterance: %v", err)
	}
	if got := strings.Join(eventTypes(collectProviderEvents(t, providerStream.Events(), 4)), ","); got != "audio.started,audio.frame,alignment,audio.done" {
		t.Fatalf("next utterance events = %s", got)
	}
	next, final := <-frames, <-frames
	if next.ContextID == first.ContextID || next.Transcript != "Again." || !next.Continue || final.ContextID != next.ContextID || final.Continue {
		t.Fatalf("next utterance frames = %+v then %+v", next, final)
	}
	if err := providerStream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// A pause between two fragments of one utterance can outlast the Cartesia
// context. Cartesia would accept the old id again (measured live), but its
// docs call that unsupported, so the next fragment continues the same
// utterance on a fresh context:
// no second utterance start, one done after the commit, and word timings that
// continue after the audio already sent instead of restarting at zero.
func TestAdapterAppendAfterCartesiaExpiredTheContextContinuesTheUtterance(t *testing.T) {
	t.Parallel()

	frames := make(chan generation, 3)
	server := newTTSServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		first, err := readGeneration(ctx, conn)
		if err != nil {
			t.Errorf("first generation: %v", err)
			return
		}
		frames <- first
		// Half a second of 16 kHz mono pcm_s16le.
		halfSecond := make([]byte, 16_000)
		for _, message := range []map[string]any{
			{"type": "chunk", "context_id": first.ContextID, "data": base64.StdEncoding.EncodeToString(halfSecond)},
			{"type": "timestamps", "context_id": first.ContextID, "word_timestamps": map[string]any{"words": []string{"Hello"}, "start": []float64{0}, "end": []float64{0.4}}},
			{"type": "done", "context_id": first.ContextID, "status_code": 200, "done": true},
			{"type": "test_marker", "context_id": first.ContextID},
		} {
			if err := writeJSON(ctx, conn, message); err != nil {
				t.Errorf("write %v: %v", message["type"], err)
				return
			}
		}
		second, err := readGeneration(ctx, conn)
		if err != nil {
			t.Errorf("second generation: %v", err)
			return
		}
		frames <- second
		final, err := readGeneration(ctx, conn)
		if err != nil {
			t.Errorf("final generation: %v", err)
			return
		}
		frames <- final
		for _, message := range []map[string]any{
			{"type": "chunk", "context_id": second.ContextID, "data": base64.StdEncoding.EncodeToString([]byte{1, 2})},
			{"type": "timestamps", "context_id": second.ContextID, "word_timestamps": map[string]any{"words": []string{"world"}, "start": []float64{0}, "end": []float64{0.3}}},
			{"type": "done", "context_id": second.ContextID, "done": true},
		} {
			if err := writeJSON(ctx, conn, message); err != nil {
				t.Errorf("write %v: %v", message["type"], err)
				return
			}
		}
		waitForClientClose(ctx, conn)
	})
	defer server.Close()

	providerStream := openTestStream(t, server.URL)
	if err := providerStream.AppendText(context.Background(), "Hello, "); err != nil {
		t.Fatalf("append: %v", err)
	}
	firstEvents := collectProviderEvents(t, providerStream.Events(), 4)
	if got := strings.Join(eventTypes(firstEvents), ","); got != "audio.started,audio.frame,alignment,warning" {
		t.Fatalf("events before the pause = %s", got)
	}
	if err := providerStream.AppendText(context.Background(), "world."); err != nil {
		t.Fatalf("append after Cartesia expired the context: %v", err)
	}
	if err := providerStream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	secondEvents := collectProviderEvents(t, providerStream.Events(), 3)
	if got := strings.Join(eventTypes(secondEvents), ","); got != "audio.frame,alignment,audio.done" {
		t.Fatalf("events after the pause = %s; the utterance must not restart", got)
	}
	if want := (protocol.TimingSpan{Text: "Hello", StartMS: 0, EndMS: 400}); spansOf(t, firstEvents[2])[0] != want {
		t.Fatalf("first spans = %+v, want %+v", spansOf(t, firstEvents[2]), want)
	}
	if want := (protocol.TimingSpan{Text: "world", StartMS: 500, EndMS: 800}); spansOf(t, secondEvents[1])[0] != want {
		t.Fatalf("continued spans = %+v, want %+v (offset by the 500 ms already sent)", spansOf(t, secondEvents[1]), want)
	}
	first, second, final := <-frames, <-frames, <-frames
	if second.ContextID == first.ContextID || second.Transcript != "world." || !second.Continue || final.ContextID != second.ContextID || final.Continue {
		t.Fatalf("frames = %+v, %+v, %+v", first, second, final)
	}
	if err := providerStream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// A commit can cross Cartesia's expiry on the wire: Cartesia sends done for
// the context, then answers the commit that arrived after it with 400
// "Invalid transcript" (both measured live). The utterance's audio was all
// delivered, so that late error, like any later frame about a context that
// already ended, is a warning and the session goes on.
func TestAdapterCommitCrossingCartesiaExpiryKeepsTheSession(t *testing.T) {
	t.Parallel()

	server := newTTSServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		fragment, err := readGeneration(ctx, conn)
		if err != nil {
			t.Errorf("fragment: %v", err)
			return
		}
		if !writeFrames(ctx, t, conn, map[string]any{"type": "chunk", "context_id": fragment.ContextID, "data": base64.StdEncoding.EncodeToString([]byte{1, 2})}, wordsFrame(fragment.ContextID, "Hello.")) {
			return
		}
		if commit, err := readGeneration(ctx, conn); err != nil || commit.Continue || commit.ContextID != fragment.ContextID {
			t.Errorf("commit = %+v, %v", commit, err)
			return
		}
		for _, message := range []map[string]any{
			{"type": "done", "context_id": fragment.ContextID, "status_code": 200, "done": true},
			{"type": "error", "context_id": fragment.ContextID, "status_code": 400, "done": true, "title": "Invalid transcript", "message": "Invalid transcript"},
			// A context Cartesia re-created for a fragment that crossed its
			// expiry ends again later, about a context the adapter closed.
			{"type": "done", "context_id": fragment.ContextID, "status_code": 200, "done": true},
		} {
			if err := writeJSON(ctx, conn, message); err != nil {
				t.Errorf("write %v: %v", message["type"], err)
				return
			}
		}
		next, err := readGeneration(ctx, conn)
		if err != nil || next.ContextID == fragment.ContextID {
			t.Errorf("next utterance = %+v, %v", next, err)
			return
		}
		if _, err := readGeneration(ctx, conn); err != nil {
			t.Errorf("next commit: %v", err)
			return
		}
		for _, message := range []map[string]any{
			{"type": "chunk", "context_id": next.ContextID, "data": base64.StdEncoding.EncodeToString([]byte{3})},
			wordsFrame(next.ContextID, "Again."),
			{"type": "done", "context_id": next.ContextID, "status_code": 200, "done": true},
		} {
			if err := writeJSON(ctx, conn, message); err != nil {
				t.Errorf("write %v: %v", message["type"], err)
				return
			}
		}
		waitForClientClose(ctx, conn)
	})
	defer server.Close()

	providerStream := openTestStream(t, server.URL)
	if err := providerStream.AppendText(context.Background(), "Hello."); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := providerStream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if got := strings.Join(eventTypes(collectProviderEvents(t, providerStream.Events(), 6)), ","); got != "audio.started,audio.frame,alignment,audio.done,warning,warning" {
		t.Fatalf("events = %s; the late error and done must be warnings", got)
	}
	if err := providerStream.AppendText(context.Background(), "Again."); err != nil {
		t.Fatalf("append next utterance: %v", err)
	}
	if err := providerStream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit next utterance: %v", err)
	}
	if got := strings.Join(eventTypes(collectProviderEvents(t, providerStream.Events(), 4)), ","); got != "audio.started,audio.frame,alignment,audio.done" {
		t.Fatalf("next utterance events = %s", got)
	}
	if err := providerStream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// Cartesia closing the socket while no utterance is in flight must not end
// the session: the next append dials a new socket and produces audio, and a
// later Close on a disconnected stream is clean.
func TestAdapterRedialsWhenCartesiaClosesTheIdleSocket(t *testing.T) {
	t.Parallel()
	for name, closeSocket := range map[string]func(*websocket.Conn){
		"normal closure": func(conn *websocket.Conn) { _ = conn.Close(websocket.StatusNormalClosure, "idle") },
		"dropped":        func(conn *websocket.Conn) { _ = conn.CloseNow() },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var connections atomic.Int32
			server := newTTSServer(t, func(ctx context.Context, request *http.Request, conn *websocket.Conn) {
				number := connections.Add(1)
				if number > 2 {
					t.Errorf("unexpected connection %d", number)
					return
				}
				if got := request.Header.Get("X-API-Key"); got != "customer-cartesia-key" {
					t.Errorf("connection %d X-API-Key = %q", number, got)
				}
				for {
					frame, err := readGeneration(ctx, conn)
					if err != nil {
						t.Errorf("connection %d: %v", number, err)
						return
					}
					if frame.Continue {
						continue
					}
					for _, message := range []map[string]any{
						{"type": "chunk", "context_id": frame.ContextID, "data": base64.StdEncoding.EncodeToString([]byte{byte(number)})},
						wordsFrame(frame.ContextID, "Hello."),
						{"type": "done", "context_id": frame.ContextID, "done": true},
					} {
						if err := writeJSON(ctx, conn, message); err != nil {
							t.Errorf("write %v: %v", message["type"], err)
							return
						}
					}
					break
				}
				closeSocket(conn)
			})
			defer server.Close()

			providerStream := openTestStream(t, server.URL)
			for want := byte(1); want <= 2; want++ {
				if err := providerStream.AppendText(context.Background(), "Hello."); err != nil {
					t.Fatalf("append on connection %d: %v", want, err)
				}
				if err := providerStream.CommitText(context.Background()); err != nil {
					t.Fatalf("commit on connection %d: %v", want, err)
				}
				events := collectProviderEvents(t, providerStream.Events(), 4)
				if got := strings.Join(eventTypes(events), ","); got != "audio.started,audio.frame,alignment,audio.done" || string(events[1].Audio) != string([]byte{want}) {
					t.Fatalf("connection %d events = %s audio=%v", want, got, events[1].Audio)
				}
				waitForDisconnect(t, providerStream)
			}
			if err := providerStream.Close(context.Background()); err != nil {
				t.Fatalf("close while disconnected: %v", err)
			}
			select {
			case event, ok := <-providerStream.Events():
				if ok {
					t.Fatalf("event after close = %+v", event)
				}
			case <-time.After(time.Second):
				t.Fatal("events did not close")
			}
		})
	}
}

// Cartesia closing the socket while an utterance is in flight ends the
// session with a typed, retryable provider error that says what happened.
func TestAdapterReportsRetryableErrorWhenCartesiaClosesMidUtterance(t *testing.T) {
	t.Parallel()
	for name, closeSocket := range map[string]func(*websocket.Conn){
		"normal closure": func(conn *websocket.Conn) { _ = conn.Close(websocket.StatusNormalClosure, "bye") },
		"dropped":        func(conn *websocket.Conn) { _ = conn.CloseNow() },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			server := newTTSServer(t, func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
				first, err := readGeneration(ctx, conn)
				if err != nil {
					t.Errorf("first generation: %v", err)
					return
				}
				if err := writeJSON(ctx, conn, map[string]any{"type": "chunk", "context_id": first.ContextID, "data": base64.StdEncoding.EncodeToString([]byte{1})}); err != nil {
					t.Errorf("chunk: %v", err)
					return
				}
				closeSocket(conn)
			})
			defer server.Close()

			providerStream := openTestStream(t, server.URL)
			if err := providerStream.AppendText(context.Background(), "Hello, "); err != nil {
				t.Fatalf("append: %v", err)
			}
			if got := strings.Join(eventTypes(collectProviderEvents(t, providerStream.Events(), 2)), ","); got != "audio.started,audio.frame" {
				t.Fatalf("events = %s", got)
			}
			var failure *runtimepkg.ProviderError
			select {
			case event, ok := <-providerStream.Events():
				if !ok || !errors.As(event.Err, &failure) {
					t.Fatalf("event = %+v (open=%t), want a provider error", event, ok)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("no error after Cartesia closed mid-utterance")
			}
			if failure.Code != "provider_unavailable" || !failure.Retryable || !strings.Contains(failure.Message, "mid-utterance") || failure.Hint == "" {
				t.Fatalf("error = %+v", failure)
			}
			if _, ok := <-providerStream.Events(); ok {
				t.Fatal("events must close after the mid-utterance failure")
			}
			if err := providerStream.Close(context.Background()); err != nil && !errors.Is(err, runtimepkg.ErrSessionClosed) {
				t.Logf("close after failure: %v", err)
			}
		})
	}
}

func openTestStream(t *testing.T, serverURL string) runtimepkg.ProviderStream {
	t.Helper()
	adapter, err := New(testConfig(serverURL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	providerStream, err := adapter.Open(context.Background(), adapterRequest(serverURL))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return providerStream
}

// waitForDisconnect waits until the adapter has noticed that Cartesia closed
// an idle socket. Nothing observable leaves the stream when that happens, so
// the internal connection slot is polled.
func waitForDisconnect(t *testing.T, providerStream runtimepkg.ProviderStream) {
	t.Helper()
	s := providerStream.(*stream)
	deadline := time.Now().Add(2 * time.Second)
	for s.currentConn() != nil {
		if time.Now().After(deadline) {
			t.Fatal("adapter did not notice the closed socket")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func contextOf(t *testing.T, event runtimepkg.ProviderEvent) string {
	t.Helper()
	var data struct {
		ContextID string `json:"context_id"`
	}
	if err := json.Unmarshal(event.Data, &data); err != nil {
		t.Fatalf("decode event data: %v", err)
	}
	return data.ContextID
}

func spansOf(t *testing.T, event runtimepkg.ProviderEvent) []protocol.TimingSpan {
	t.Helper()
	var data struct {
		Spans []protocol.TimingSpan `json:"spans"`
	}
	if err := json.Unmarshal(event.Data, &data); err != nil || len(data.Spans) == 0 {
		t.Fatalf("decode spans from %s: %v", event.Data, err)
	}
	return data.Spans
}
