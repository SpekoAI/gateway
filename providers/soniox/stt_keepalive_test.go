package soniox

import (
	"context"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// Soniox ends a stream that receives neither audio nor a keepalive for 20
// seconds, so the adapter keeps a paused stream alive on its own and stays
// quiet while audio is flowing.
func TestSTTSendsKeepaliveOnlyWhileAudioIsIdle(t *testing.T) {
	frames := make(chan wireFrame, 64)
	server := newSTTServer(t, func(ctx context.Context, conn *websocket.Conn) {
		if _, err := readJSONObject(ctx, conn); err != nil { // start request
			t.Errorf("read start: %v", err)
			return
		}
		for {
			kind, payload, err := conn.Read(ctx)
			if err != nil {
				return
			}
			frames <- wireFrame{kind: kind, payload: payload}
		}
	})
	defer server.Close()

	const interval = 200 * time.Millisecond
	config := sttTestConfig(server.URL)
	config.KeepAliveInterval = interval
	adapter, err := NewSTT(config)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := adapter.Open(context.Background(), sttAdapterRequest(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer abortStream(stream)

	// Audio written more often than the interval suppresses keepalives.
	deadline := time.Now().Add(3 * interval)
	for time.Now().Before(deadline) {
		if err := stream.WriteAudio(context.Background(), []byte{1, 2}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	for drained := false; !drained; {
		select {
		case frame := <-frames:
			if frame.kind != websocket.MessageBinary {
				t.Fatalf("frame during audio = %v %q, want only audio", frame.kind, frame.payload)
			}
		default:
			drained = true
		}
	}

	// Once audio pauses, keepalives arrive on the interval.
	for range 2 {
		frame := mustReceiveFrame(t, frames)
		if frame.kind != websocket.MessageText || string(frame.payload) != `{"type":"keepalive"}` {
			t.Fatalf("idle frame = %v %q", frame.kind, frame.payload)
		}
	}
}

func TestSTTStopsKeepaliveAfterClose(t *testing.T) {
	frames := make(chan wireFrame, 64)
	server := newSTTServer(t, func(ctx context.Context, conn *websocket.Conn) {
		if _, err := readJSONObject(ctx, conn); err != nil {
			return
		}
		for {
			kind, payload, err := conn.Read(ctx)
			if err != nil {
				return
			}
			frames <- wireFrame{kind: kind, payload: payload}
		}
	})
	defer server.Close()

	config := sttTestConfig(server.URL)
	config.KeepAliveInterval = 30 * time.Millisecond
	adapter, err := NewSTT(config)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := adapter.Open(context.Background(), sttAdapterRequest(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer abortStream(stream)
	if err := stream.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	// finalize, then the empty end-of-stream text frame.
	for range 2 {
		mustReceiveFrame(t, frames)
	}
	select {
	case frame := <-frames:
		t.Fatalf("frame after close = %v %q", frame.kind, frame.payload)
	case <-time.After(150 * time.Millisecond):
	}
}

func TestSTTUnrequestedGoingAwayIsAnError(t *testing.T) {
	server := newSTTServer(t, func(ctx context.Context, conn *websocket.Conn) {
		if _, err := readJSONObject(ctx, conn); err != nil {
			return
		}
		_ = conn.Close(websocket.StatusGoingAway, "idle")
	})
	defer server.Close()

	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	stream, err := adapter.Open(context.Background(), sttAdapterRequest(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer abortStream(stream)
	providerError := awaitProviderError(t, stream.Events())
	if providerError.Code != "provider_unavailable" || !providerError.Retryable || providerError.Hint == "" {
		t.Fatalf("error = %#v", providerError)
	}
}

func TestSTTRejectsNegativeKeepAliveInterval(t *testing.T) {
	if _, err := NewSTT(STTConfig{KeepAliveInterval: -time.Second}); err == nil {
		t.Fatal("negative keepalive interval was accepted")
	}
}
