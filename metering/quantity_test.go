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
