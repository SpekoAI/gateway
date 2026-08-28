package protocol_test

import (
	"testing"

	"github.com/SpekoAI/gateway/protocol"
)

func TestTimingMSFromSecondsRoundsRatherThanTruncates(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		seconds float64
		want    int64
	}{
		{"cartesia first word start", 0.11937626, 119},
		{"rounds up past the half", 0.0295, 30},
		{"rounds down below the half", 0.0294, 29},
		{"exact", 0.282, 282},
		{"zero", 0, 0},
	} {
		if got := protocol.TimingMSFromSeconds(tc.seconds); got != tc.want {
			t.Errorf("%s: TimingMSFromSeconds(%v) = %d, want %d", tc.name, tc.seconds, got, tc.want)
		}
	}
}

// Truncation would bias every span low; rounding keeps the error bounded at
// half a millisecond and unbiased, so it cannot accumulate across a render.
func TestTimingMSFromSecondsDoesNotAccumulateBias(t *testing.T) {
	t.Parallel()

	var worst int64
	for i := range 20_000 {
		seconds := float64(i) * 0.0271
		want := int64(seconds*1000 + 0.5)
		if diff := protocol.TimingMSFromSeconds(seconds) - want; diff > worst || -diff > worst {
			worst = max(diff, -diff)
		}
	}
	if worst > 1 {
		t.Fatalf("worst per-span error = %d ms, want at most 1", worst)
	}
}

// The real Cartesia and Soniox shape: seconds, with an END time.
func TestTimingSpansFromSeconds(t *testing.T) {
	t.Parallel()

	got := protocol.TimingSpansFromSeconds(
		[]string{" ", "l", "i"},
		[]float64{0.282, 0.297, 0.312},
		[]float64{0.297, 0.312, 0.327},
		protocol.TimingGranularityCharacter,
	)
	if got.Granularity != protocol.TimingGranularityCharacter {
		t.Fatalf("granularity = %q", got.Granularity)
	}
	want := []protocol.TimingSpan{
		{Text: " ", StartMS: 282, EndMS: 297},
		{Text: "l", StartMS: 297, EndMS: 312},
		{Text: "i", StartMS: 312, EndMS: 327},
	}
	if len(got.Spans) != len(want) {
		t.Fatalf("spans = %+v", got.Spans)
	}
	for i := range want {
		if got.Spans[i] != want[i] {
			t.Errorf("spans[%d] = %+v, want %+v", i, got.Spans[i], want[i])
		}
	}
}

// The real ElevenLabs streaming shape: milliseconds, with a DURATION. The
// end time must come out as start+duration, never as the duration itself.
func TestTimingSpansFromMillisecondDurationsSumsToEndTimes(t *testing.T) {
	t.Parallel()

	got := protocol.TimingSpansFromMillisecondDurations(
		[]string{"T", "h", "e", " "},
		[]int{0, 93, 139, 186},
		[]int{93, 46, 47, 93},
		protocol.TimingGranularityCharacter,
	)
	want := []protocol.TimingSpan{
		{Text: "T", StartMS: 0, EndMS: 93},
		{Text: "h", StartMS: 93, EndMS: 139},
		{Text: "e", StartMS: 139, EndMS: 186},
		{Text: " ", StartMS: 186, EndMS: 279},
	}
	if len(got.Spans) != len(want) {
		t.Fatalf("spans = %+v", got.Spans)
	}
	for i := range want {
		if got.Spans[i] != want[i] {
			t.Errorf("spans[%d] = %+v, want %+v", i, got.Spans[i], want[i])
		}
	}
}

// A length disagreement means the frame was not understood. Half-decoding it
// would pair text with times from a different position, which is worse than
// staying silent.
func TestTimingSpansRejectDisagreeingArrayLengths(t *testing.T) {
	t.Parallel()

	if got := protocol.TimingSpansFromSeconds([]string{"a", "b"}, []float64{0}, []float64{1}, protocol.TimingGranularityWord); len(got.Spans) != 0 {
		t.Errorf("short starts: spans = %+v, want none", got.Spans)
	}
	if got := protocol.TimingSpansFromSeconds([]string{"a"}, []float64{0}, []float64{1, 2}, protocol.TimingGranularityWord); len(got.Spans) != 0 {
		t.Errorf("long ends: spans = %+v, want none", got.Spans)
	}
	if got := protocol.TimingSpansFromMillisecondDurations([]string{"a", "b"}, []int{0, 1}, []int{5}, protocol.TimingGranularityCharacter); len(got.Spans) != 0 {
		t.Errorf("short durations: spans = %+v, want none", got.Spans)
	}
	if got := protocol.TimingSpansFromSeconds(nil, nil, nil, protocol.TimingGranularityWord); len(got.Spans) != 0 {
		t.Errorf("empty: spans = %+v, want none", got.Spans)
	}
}

// Overlapping and zero-width spans are what real engines return; the
// normalizer must carry them through rather than repair them.
func TestTimingSpansCarryOverlappingAndZeroWidthSpans(t *testing.T) {
	t.Parallel()

	got := protocol.TimingSpansFromSeconds(
		[]string{"e", "e", "の"},
		[]float64{0.834, 0.834, 1.720},
		[]float64{0.854, 0.854, 1.720},
		protocol.TimingGranularityCharacter,
	)
	if len(got.Spans) != 3 {
		t.Fatalf("spans = %+v", got.Spans)
	}
	if got.Spans[0] != got.Spans[1] {
		t.Errorf("identical overlapping spans must survive: %+v vs %+v", got.Spans[0], got.Spans[1])
	}
	if got.Spans[2].StartMS != got.Spans[2].EndMS {
		t.Errorf("zero-width span must survive: %+v", got.Spans[2])
	}
}
