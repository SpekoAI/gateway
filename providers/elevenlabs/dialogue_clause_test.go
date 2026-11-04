package elevenlabs

import (
	"context"
	"encoding/base64"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestEndsAtClause(t *testing.T) {
	t.Parallel()
	cases := []struct {
		text         string
		requireSpace bool
		want         bool
	}{
		{"Hello, ", true, true},
		{"Hello,", true, false},
		{"Hello,", false, true},
		{"It costs 3.", true, false}, // "3." may be "3.5": wait for the next token
		{"Done.\" ", true, true},
		{"Done.) ", true, true},
		{"नमस्ते, ", true, true},
		{"पहुँच जाएगा।", true, false},
		{"पहुँच जाएगा।", false, true},
		{"你好，", true, true}, // full width needs no space
		{"مرحبا، ", true, true},
		{"Hello ", true, false},
		{"Hello", false, false},
		{"", false, false},
		{"   ", false, false},
	}
	for _, c := range cases {
		if got := endsAtClause(c.text, c.requireSpace); got != c.want {
			t.Errorf("endsAtClause(%q, %v) = %v, want %v", c.text, c.requireSpace, got, c.want)
		}
	}
}

// tokenServer reads client frames and answers each flush that followed text
// with audio and one is_final_audio_for_turn, as the live socket does
// (measured 2026-11-04: a flush with no new text gets no final). It records
// the frames in order as "text:<t>" or "flush" for the test to assert.
func tokenServer(t *testing.T, frames chan<- string, finalDelay time.Duration) func(context.Context, *http.Request, *websocket.Conn) {
	return func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
		if _, err := readDialogueMessage(ctx, conn); err != nil {
			t.Errorf("read handshake: %v", err)
			return
		}
		pendingText := false
		for {
			message, err := readDialogueMessage(ctx, conn)
			if err != nil {
				return
			}
			switch {
			case message.CloseSocket:
				frames <- "close"
				_ = conn.Close(websocket.StatusNormalClosure, "")
				return
			case len(message.Inputs) == 1:
				frames <- "text:" + message.Inputs[0].Text
				pendingText = true
			case message.Flush:
				frames <- "flush"
				if !pendingText {
					continue
				}
				pendingText = false
				time.Sleep(finalDelay)
				if err := writeServerJSON(ctx, conn, map[string]any{"audio": base64.StdEncoding.EncodeToString([]byte{1, 2})}); err != nil {
					return
				}
				if err := writeServerJSON(ctx, conn, map[string]any{"is_final_audio_for_turn": true}); err != nil {
					return
				}
			}
		}
	}
}

func drainFrames(frames <-chan string, until string) []string {
	var got []string
	timeout := time.After(2 * time.Second)
	for {
		select {
		case frame := <-frames:
			got = append(got, frame)
			if frame == until {
				return got
			}
		case <-timeout:
			return got
		}
	}
}

// An LLM reply streams token by token and commits once at its end. Each
// clause is flushed as soon as the next token shows it ended, the clause
// finals end nothing, and the session emits exactly one audio.done, after the
// commit's own final. A second turn on the same session must work too: a
// stale flush counter would swallow its audio.done.
func TestDialogueAdapterFlushesEachClauseOfAStreamedReply(t *testing.T) {
	t.Parallel()
	frames := make(chan string, 64)
	server := newDialogueServer(t, tokenServer(t, frames, 0))
	defer server.Close()

	adapter, err := NewDialogue(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new dialogue adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), dialogueRequest(server.URL, "eleven_v4_turbo"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for turn := 0; turn < 2; turn++ {
		for _, token := range []string{"नमस्ते", ",", " मैं", " मदद", " करूँगी", "।"} {
			if err := stream.AppendText(context.Background(), token); err != nil {
				t.Fatalf("turn %d append %q: %v", turn, token, err)
			}
		}
		if err := stream.CommitText(context.Background()); err != nil {
			t.Fatalf("turn %d commit: %v", turn, err)
		}
		events := collectEvents(t, stream.Events(), 4)
		if got := strings.Join(eventTypes(events), ","); got != "audio.started,audio.frame,audio.frame,audio.done" {
			t.Fatalf("turn %d event types = %s", turn, got)
		}
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	got := strings.Join(drainFrames(frames, "close"), "|")
	oneTurn := "text:नमस्ते|text:,|flush|text: मैं|text: मदद|text: करूँगी|text:।|flush"
	if want := oneTurn + "|" + oneTurn + "|close"; got != want {
		t.Fatalf("frames =\n%s\nwant\n%s", got, want)
	}
}

// When the reply's last token already closed a clause and was flushed, the
// commit has nothing to flush. The turn still ends exactly once, whether the
// clause's final arrives before or after the commit.
func TestDialogueAdapterEndsAFullyFlushedTurnOnce(t *testing.T) {
	t.Parallel()
	for name, finalDelay := range map[string]time.Duration{"final after commit": 100 * time.Millisecond, "final before commit": 0} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			frames := make(chan string, 64)
			server := newDialogueServer(t, tokenServer(t, frames, finalDelay))
			defer server.Close()

			adapter, err := NewDialogue(testConfig(server.URL))
			if err != nil {
				t.Fatalf("new dialogue adapter: %v", err)
			}
			stream, err := adapter.Open(context.Background(), dialogueRequest(server.URL, "eleven_v4_turbo"))
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			for turn := 0; turn < 2; turn++ {
				if err := stream.AppendText(context.Background(), "All done. "); err != nil {
					t.Fatalf("append: %v", err)
				}
				if finalDelay == 0 {
					// Let the clause's final land before the commit.
					first := collectEvents(t, stream.Events(), 2)
					if got := strings.Join(eventTypes(first), ","); got != "audio.started,audio.frame" {
						t.Fatalf("turn %d events before commit = %s", turn, got)
					}
				}
				if err := stream.CommitText(context.Background()); err != nil {
					t.Fatalf("commit: %v", err)
				}
				want := "audio.started,audio.frame,audio.done"
				count := 3
				if finalDelay == 0 {
					want, count = "audio.done", 1
				}
				if got := strings.Join(eventTypes(collectEvents(t, stream.Events(), count)), ","); got != want {
					t.Fatalf("turn %d event types = %s, want %s", turn, got, want)
				}
			}
			select {
			case event := <-stream.Events():
				t.Fatalf("unexpected extra event %s", event.Type)
			case <-time.After(150 * time.Millisecond):
			}
			if err := stream.Close(context.Background()); err != nil {
				t.Fatalf("close: %v", err)
			}
			if got, want := strings.Join(drainFrames(frames, "close"), "|"), "text:All done. |flush|text:All done. |flush|close"; got != want {
				t.Fatalf("frames = %s, want %s (the commit must not send an empty flush)", got, want)
			}
		})
	}
}

func TestLastClauseSplit(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"Hello, how":         "Hello, ",
		"Done. Next, then":   "Done. Next, ",
		"Hi. ":               "", // boundary at the end: endsAtClause's case
		"It costs 3.5 today": "",
		"नमस्ते, मैं":        "नमस्ते, ",
		"你好，我":               "你好，",
		"\"Yes,\" she said":  "\"Yes,\" ",
		"no punctuation":     "",
	}
	for text, head := range cases {
		if got := text[:lastClauseSplit(text)]; got != head {
			t.Errorf("lastClauseSplit(%q) head = %q, want %q", text, got, head)
		}
	}
}

// A chunk that carries a boundary inside it is split there, so its first
// clause is flushed without waiting for the next chunk.
func TestDialogueAdapterFlushesABoundaryInsideAChunk(t *testing.T) {
	t.Parallel()
	frames := make(chan string, 64)
	server := newDialogueServer(t, tokenServer(t, frames, 0))
	defer server.Close()

	adapter, err := NewDialogue(testConfig(server.URL))
	if err != nil {
		t.Fatalf("new dialogue adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), dialogueRequest(server.URL, "eleven_v4_turbo"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, chunk := range []string{"Hello, how", " can I help?"} {
		if err := stream.AppendText(context.Background(), chunk); err != nil {
			t.Fatalf("append %q: %v", chunk, err)
		}
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if got := strings.Join(eventTypes(collectEvents(t, stream.Events(), 4)), ","); got != "audio.started,audio.frame,audio.frame,audio.done" {
		t.Fatalf("event types = %s", got)
	}
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got, want := strings.Join(drainFrames(frames, "close"), "|"), "text:Hello, |flush|text:how|text: can I help?|flush|close"; got != want {
		t.Fatalf("frames = %s, want %s", got, want)
	}
}

// LiveKit closes the session right after committing. When the socket's end
// reaches the pump before the commitDone item of a fully flushed turn, the
// turn must still get its audio.done (Pullfrog review of gateway#143).
func TestDialogueSocketEndFinishesAFullyFlushedTurn(t *testing.T) {
	t.Parallel()
	s := &dialogueStream{speaking: true, audioStarted: true}
	s.committed.Store(true)
	events, _, terminal := s.apply(dialogueItem{ended: true})
	if !terminal {
		t.Fatal("socket end did not end the stream")
	}
	if got := strings.Join(eventTypes(events), ","); got != "audio.done" {
		t.Fatalf("events at socket end = %q, want audio.done", got)
	}
	// A late commitDone after the end changes nothing.
	if late, _, _ := s.apply(dialogueItem{commitDone: true}); len(late) != 0 {
		t.Fatalf("commitDone after the end emitted %v", eventTypes(late))
	}

	// A turn still waiting on a final is not complete: no audio.done.
	pending := &dialogueStream{speaking: true, pendingFlushes: 1}
	pending.committed.Store(true)
	if events, _, _ := pending.apply(dialogueItem{ended: true}); len(events) != 0 {
		t.Fatalf("socket end with a final pending emitted %v", eventTypes(events))
	}
}
