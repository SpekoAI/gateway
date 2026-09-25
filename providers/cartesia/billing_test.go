package cartesia

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

// TestBatchCreditObservation pins the quantity Cartesia actually bills for
// POST /stt: credits, at one credit per two seconds of the response's own
// reported audio duration. The denominator has to carry the halving, because
// an odd millisecond is half a thousandth of a credit.
func TestBatchCreditObservation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, duration string
		wantCredits    int64
		wantComplete   bool
	}{
		// 1823.17s of audio is 911.585 credits, not 1823.17.
		{"fractional", `1823.17`, 911_585_000_000, true},
		// An odd millisecond survives: 2.999s is 1.4995 credits.
		{"odd millisecond", `2.999`, 1_499_500_000, true},
		// Cartesia bills silence, so a silent file is a measured zero, not a
		// missing quantity.
		{"silence", `0`, 0, true},
		{"missing", ``, 0, false},
		{"null", `null`, 0, false},
		{"negative", `-1`, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			body := `{"request_id":"req-c","text":"hi","language":"en"`
			if tc.duration != "" {
				body += `,"duration":` + tc.duration
			}
			body += `}`
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(body))
			}))
			defer server.Close()
			adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
			if err != nil {
				t.Fatalf("adapter: %v", err)
			}
			result, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{
				Plan: batchPlan(server.URL+"/stt", BatchModel), Audio: strings.NewReader("RIFF....WAVE"), AudioBytes: 12,
			})
			if err != nil {
				t.Fatalf("transcribe: %v", err)
			}
			report := result.Billing
			if report == nil || len(report.Operations) != 1 {
				t.Fatalf("billing = %+v", report)
			}
			if err := report.Validate(); err != nil {
				t.Fatalf("invalid report: %v", err)
			}
			o := report.Operations[0]
			if o.Complete != tc.wantComplete || report.Complete != tc.wantComplete {
				t.Fatalf("complete = %v/%v want %v", o.Complete, report.Complete, tc.wantComplete)
			}
			if o.Model != BatchModel || o.Mode != "batch" || o.ProviderRequestID != "req-c" {
				t.Fatalf("identity = %+v", o)
			}
			if !tc.wantComplete {
				if len(o.Quantities) != 0 {
					t.Fatalf("unmeasured duration became a quantity: %+v", o.Quantities)
				}
				return
			}
			if got := o.Quantities["credits"]; got != tc.wantCredits {
				t.Fatalf("credits = %d want %d", got, tc.wantCredits)
			}
			if len(o.Quantities) != 1 {
				t.Fatalf("extra dimensions: %+v", o.Quantities)
			}
			if got := o.QuantityDenominator("credits"); got != batchCreditDenominator {
				t.Fatalf("denominator = %d want %d", got, batchCreditDenominator)
			}
		})
	}
}

// TestBatchCreditsNeverExceedTheWebSocketRate is the arithmetic the vendor's
// own credit table states: the file endpoint costs half what the ink-whisper
// socket costs for the same audio. A regression that dropped the halving would
// double every prerecorded charge, and would still look plausible in isolation.
func TestBatchCreditsNeverExceedTheWebSocketRate(t *testing.T) {
	t.Parallel()
	o := batchCredits(BatchModel, []byte(`{"duration":60}`), "req-c")
	if err := o.Validate(); err != nil {
		t.Fatalf("invalid observation: %v", err)
	}
	seconds := int64(60)
	// One credit per second is the socket rate; the file endpoint is half.
	socketCredits := seconds * batchCreditDenominator
	if got := o.Quantities["credits"]; got*2 != socketCredits {
		t.Fatalf("60s billed %d credit-thousandths; the file endpoint must be half the socket's %d", got, socketCredits)
	}
	var zero protocol.BillingObservation
	if _, err := protocol.MergeBillingObservation(zero, *o); err != nil {
		t.Fatalf("observation does not merge: %v", err)
	}
	// A duration Cartesia sent as a string, or finer than the credit
	// denominator can hold, is unmeasured rather than silently zero.
	for _, body := range []string{`{"duration":"60"}`, `{"duration":0.000000001}`, `{}`} {
		if o := batchCredits(BatchModel, []byte(body), "req-c"); o.Complete || len(o.Quantities) != 0 {
			t.Fatalf("%s became %+v", body, o)
		}
	}
}
