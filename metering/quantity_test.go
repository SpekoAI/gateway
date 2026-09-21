package metering

import "testing"

func TestExactQuantityPresence(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want int64
		ok   bool
	}{
		{`{"usage":{"seconds":0}}`, 0, true}, {`{"usage":{"seconds":1.001}}`, 1001, true},
		{`{"usage":{"seconds":1e-3}}`, 1, true}, {`{"usage":{"seconds":null}}`, 0, false},
		{`{"usage":{}}`, 0, false}, {`{"usage":{"seconds":-1}}`, 0, false},
		{`{"usage":{"seconds":0.0001}}`, 0, false}, {`{"usage":{"seconds":"3"}}`, 0, false},
		{`{"usage":{"seconds":9223372036854776}}`, 0, false},
	} {
		got, ok := Quantity([]byte(tc.raw), 1000, "usage", "seconds")
		if got != tc.want || ok != tc.ok {
			t.Fatalf("%s: %d,%v", tc.raw, got, ok)
		}
	}
}

func TestDurationPreservesSubMillisecondEvidence(t *testing.T) {
	got := Duration("request", "asr", "batch", []byte(`{"duration":1.000125}`), 1000, "duration")
	if !got.Complete || got.Quantities["duration_seconds"] != 1000125000 || got.QuantityDenominator("duration_seconds") != 1000000000 {
		t.Fatalf("%+v", got)
	}
	if got := Duration("request", "asr", "batch", []byte(`{"duration":0.0000000001}`), 1000, "duration"); got.Complete {
		t.Fatal("silently rounded provider duration")
	}
}
