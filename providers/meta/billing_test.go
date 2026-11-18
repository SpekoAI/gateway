package meta

import (
	"context"
	"encoding/json"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
	"testing"
	"time"
)

func TestProcessedAudioBilling(t *testing.T) {
	for _, tc := range []struct {
		name, duration string
		want           int64
		complete       bool
	}{
		{"fractional", "1999", 1999, true}, {"silence", "0", 0, true},
		{"missing", "", 0, false}, {"null", "null", 0, false}, {"negative", "-1", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeRealtime(t)
			final := `{"type":"audioProgress"`
			if tc.duration != "" {
				final += `,"audioProcessedMs":` + tc.duration
			}
			final += `}`
			fake.closingReplies = []string{final, final}
			stream, ctx := openRealtime(t, fake, realtimeRequest(fake.endpoint()))
			// Two seconds of 24 kHz mono PCM: the billed figure may never
			// exceed the audio actually forwarded.
			writePCM(t, stream, ctx, 2*bytesPerSecond24k)
			if err := stream.Close(ctx); err != nil {
				t.Fatal(err)
			}
			var last *protocol.BillingObservation
			for event := range stream.Events() {
				if event.Err != nil {
					t.Fatal(event.Err)
				}
				if event.Billing != nil {
					last = event.Billing
				}
			}
			if last == nil || last.Complete != tc.complete || last.Quantities["duration_seconds"] != tc.want || last.ProviderRequestID != "sess-1" {
				t.Fatalf("%+v", last)
			}
		})
	}
}

func TestInterruptedAudioKeepsPartialBilling(t *testing.T) {
	s := &sttStream{ctx: context.Background(), events: make(chan runtimepkg.ProviderEvent, 8), setupDone: make(chan error, 1), bytesPerSecond: bytesPerSecond24k, billing: protocol.BillingObservation{OperationID: "session", Model: DefaultModel, Mode: "streaming", Quantities: map[string]int64{}}}
	s.forwardedBytes.Store(31 * bytesPerSecond24k)
	raw := []byte(`{"type":"audioProgress","audioProcessedMs":30400}`)
	var message serverMessage
	_ = json.Unmarshal(raw, &message)
	s.handle(message, raw)
	s.finish(websocket.CloseError{Code: websocket.StatusInternalError})
	first := <-s.events
	if first.Billing == nil || first.Billing.Complete || first.Billing.Quantities["duration_seconds"] != 30400 {
		t.Fatalf("%+v", first)
	}
	final := <-s.events
	if final.Billing == nil || final.Billing.Complete || final.Billing.Quantities["duration_seconds"] != 30400 {
		t.Fatalf("%+v", final)
	}
}

const bytesPerSecond24k = 24_000 * 2

// openRealtimeDrain opens a session whose Close drain window is short, so a
// service that never hangs up is closed locally within the test.
func openRealtimeDrain(t *testing.T, fake *fakeRealtime, drain time.Duration) (runtimepkg.ProviderStream, context.Context) {
	t.Helper()
	adapter, err := NewSTT(STTConfig{AllowedEndpointHosts: []string{fake.host()}, AllowInsecureEndpoint: true, SetupTimeout: 5 * time.Second, CloseDrainTimeout: drain})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	stream, err := adapter.Open(ctx, realtimeRequest(fake.endpoint()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stream.(runtimepkg.AbortingProviderStream).Abort(context.Background()) })
	return stream, ctx
}

// drainBilling reads the stream to its end and returns every billing
// observation in order, failing on a provider error unless allowed.
func drainBilling(t *testing.T, stream runtimepkg.ProviderStream, allowErr bool) []protocol.BillingObservation {
	t.Helper()
	var observations []protocol.BillingObservation
	for event := range stream.Events() {
		if event.Err != nil && !allowErr {
			t.Fatal(event.Err)
		}
		if event.Billing != nil {
			observations = append(observations, *event.Billing)
		}
	}
	if len(observations) == 0 {
		t.Fatal("no billing observation")
	}
	return observations
}

// mergeLikeConnector folds observations exactly as the relay connector does
// (cumulative merge, never a sum) and fails if any step would be rejected.
func mergeLikeConnector(t *testing.T, observations []protocol.BillingObservation) protocol.BillingObservation {
	t.Helper()
	var merged protocol.BillingObservation
	for index, o := range observations {
		next, err := protocol.MergeBillingObservation(merged, o)
		if err != nil {
			t.Fatalf("observation %d %+v rejected by cumulative merge: %v", index, o, err)
		}
		merged = next
	}
	return merged
}

// The production case (muse-voice-transcribe-1.0, every session with speech
// left complete:false): turn events carry a position that trails the latest
// audioProgress. That used to fail the cumulative merge as a regression and
// mark the whole session unbillable. The session total is the largest
// processed position, reported once (never summed), and the client's close
// makes it complete.
func TestClientCloseWithTrailingTurnPositionsBillsCompleteProcessedAudio(t *testing.T) {
	fake := newFakeRealtime(t,
		`{"type":"audioProgress","audioProcessedMs":1600}`,
		`{"type":"speechStart","turnId":1,"audioProcessedMs":1200}`,
		`{"type":"transcript","transcript":"how is","final":false,"audioProcessedMs":2000}`,
		`{"type":"audioProgress","audioProcessedMs":4000}`,
		`{"type":"speechEnd","turnId":1,"audioProcessedMs":3600}`,
		`{"type":"speechComplete","turnId":1,"transcript":"How is it?","audioProcessedMs":3600}`,
	)
	fake.closingReplies = []string{`{"type":"audioProgress","audioProcessedMs":5000}`}
	stream, ctx := openRealtime(t, fake, realtimeRequest(fake.endpoint()))
	writePCM(t, stream, ctx, 5*bytesPerSecond24k)
	if err := stream.Close(ctx); err != nil {
		t.Fatal(err)
	}
	observations := drainBilling(t, stream, false)
	final := observations[len(observations)-1]
	if !final.Complete || final.Quantities["duration_seconds"] != 5000 {
		t.Fatalf("final = %+v, want complete 5000 ms", final)
	}
	for _, o := range observations[:len(observations)-1] {
		if o.Complete {
			t.Fatalf("interim observation marked complete: %+v", o)
		}
	}
	if merged := mergeLikeConnector(t, observations); !merged.Complete || merged.Quantities["duration_seconds"] != 5000 {
		t.Fatalf("connector view = %+v, want complete 5000 ms (replaced, not summed)", merged)
	}
}

// A client close where the service never hangs up: the drain window expires
// and the socket is closed locally. The processed position had reached all
// forwarded audio, so the figure is final and complete — and the terminal
// observation is delivered even though the drain cancelled the stream
// context first (it used to race the cancellation and could be dropped).
func TestClientCloseCompletesAfterDrainWhenProcessedCoversForwarded(t *testing.T) {
	for run := 0; run < 10; run++ {
		fake := newFakeRealtime(t, `{"type":"audioProgress","audioProcessedMs":2960}`)
		fake.holdOpenAfterEndStream = true
		stream, ctx := openRealtimeDrain(t, fake, 50*time.Millisecond)
		writePCM(t, stream, ctx, 3*bytesPerSecond24k)
		// Let the progress frame land before the input ends.
		waitForBilling(t, stream, ctx)
		if err := stream.Close(ctx); err != nil {
			t.Fatal(err)
		}
		observations := drainBilling(t, stream, false)
		final := observations[len(observations)-1]
		if !final.Complete || final.Quantities["duration_seconds"] != 2960 {
			t.Fatalf("run %d: final = %+v, want complete 2960 ms", run, final)
		}
	}
}

// When the processed position is still short of the forwarded audio at the
// end of the drain, part of the measurement may be missing: the evidence
// stays partial (billed later by the deadline path as a lower bound).
func TestClientCloseStaysPartialWhenProcessedTrailsForwarded(t *testing.T) {
	fake := newFakeRealtime(t, `{"type":"audioProgress","audioProcessedMs":3000}`)
	fake.holdOpenAfterEndStream = true
	stream, ctx := openRealtimeDrain(t, fake, 50*time.Millisecond)
	writePCM(t, stream, ctx, 5*bytesPerSecond24k)
	waitForBilling(t, stream, ctx)
	if err := stream.Close(ctx); err != nil {
		t.Fatal(err)
	}
	observations := drainBilling(t, stream, false)
	final := observations[len(observations)-1]
	if final.Complete || final.Quantities["duration_seconds"] != 3000 {
		t.Fatalf("final = %+v, want partial 3000 ms", final)
	}
}

// Never bill more than was sent: a processed counter that runs ahead of the
// forwarded audio is capped at the forwarded duration.
func TestBilledDurationNeverExceedsForwardedAudio(t *testing.T) {
	fake := newFakeRealtime(t)
	fake.closingReplies = []string{`{"type":"audioProgress","audioProcessedMs":9000}`}
	stream, ctx := openRealtime(t, fake, realtimeRequest(fake.endpoint()))
	writePCM(t, stream, ctx, 2*bytesPerSecond24k)
	if err := stream.Close(ctx); err != nil {
		t.Fatal(err)
	}
	observations := drainBilling(t, stream, false)
	for _, o := range observations {
		if o.Quantities["duration_seconds"] > 2000 {
			t.Fatalf("observation %+v bills more than the 2000 ms forwarded", o)
		}
	}
	if final := observations[len(observations)-1]; final.Quantities["duration_seconds"] != 2000 {
		t.Fatalf("final = %+v, want 2000 ms", final)
	}
	mergeLikeConnector(t, observations)
}

// A service failure after endStream is not a clean end: even with the
// position covering the forwarded audio the evidence stays partial.
func TestProviderFailureAfterEndStreamStaysPartial(t *testing.T) {
	fake := newFakeRealtime(t)
	fake.closingReplies = []string{`{"type":"audioProgress","audioProcessedMs":2000}`}
	fake.closeStatus = websocket.StatusInternalError
	stream, ctx := openRealtime(t, fake, realtimeRequest(fake.endpoint()))
	writePCM(t, stream, ctx, 2*bytesPerSecond24k)
	if err := stream.Close(ctx); err != nil {
		t.Fatal(err)
	}
	observations := drainBilling(t, stream, true)
	if final := observations[len(observations)-1]; final.Complete {
		t.Fatalf("final = %+v, want partial after a provider failure", final)
	}
}

// An aborted session (no endStream, input torn down locally) never claims a
// final total, however far the position got.
func TestAbortedSessionStaysPartial(t *testing.T) {
	fake := newFakeRealtime(t, `{"type":"audioProgress","audioProcessedMs":1000}`)
	stream, ctx := openRealtime(t, fake, realtimeRequest(fake.endpoint()))
	writePCM(t, stream, ctx, bytesPerSecond24k)
	waitForBilling(t, stream, ctx)
	if err := stream.(runtimepkg.AbortingProviderStream).Abort(ctx); err != nil {
		t.Fatal(err)
	}
	for event := range stream.Events() {
		if event.Billing != nil && event.Billing.Complete {
			t.Fatalf("aborted session reported complete: %+v", event.Billing)
		}
	}
}

// A session that served nothing (no audio forwarded, no progress) never
// reports a billable quantity.
func TestSessionWithoutAudioBillsNothing(t *testing.T) {
	fake := newFakeRealtime(t)
	stream, ctx := openRealtime(t, fake, realtimeRequest(fake.endpoint()))
	if err := stream.Close(ctx); err != nil {
		t.Fatal(err)
	}
	for _, o := range drainBilling(t, stream, false) {
		if o.Complete || o.Quantities["duration_seconds"] != 0 {
			t.Fatalf("observation %+v, want nothing billable", o)
		}
	}
}

// waitForBilling consumes events until an observation reports processed
// audio. The published figure is capped at the audio forwarded so far, so
// only "some progress" is awaited, not the service's raw number.
func waitForBilling(t *testing.T, stream runtimepkg.ProviderStream, ctx context.Context) {
	t.Helper()
	for {
		select {
		case event, ok := <-stream.Events():
			if !ok {
				t.Fatal("stream ended before any processed audio was billed")
			}
			if event.Err != nil {
				t.Fatal(event.Err)
			}
			if event.Billing != nil && event.Billing.Quantities["duration_seconds"] > 0 {
				return
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for processed audio")
		}
	}
}

// writePCM forwards n bytes of silence in 100 ms frames, the way a caller
// streams audio (and within the fake server's frame limit).
func writePCM(t *testing.T, stream runtimepkg.ProviderStream, ctx context.Context, n int) {
	t.Helper()
	const frame = bytesPerSecond24k / 10
	for n > 0 {
		size := min(n, frame)
		if err := stream.WriteAudio(ctx, make([]byte, size)); err != nil {
			t.Fatal(err)
		}
		n -= size
	}
}
