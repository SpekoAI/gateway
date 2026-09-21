package protocol

import "testing"

func TestBillingCumulativeEvidence(t *testing.T) {
	first := BillingObservation{OperationID: "response-1", Model: "model", Mode: "streaming", Quantities: map[string]int64{"input_audio_tokens": 1000}}
	next := first.Clone()
	next.Quantities["input_audio_tokens"] = 3000
	next.Quantities["output_tokens"] = 0
	next.Complete = true
	merged, err := MergeBillingObservation(first, next)
	if err != nil || merged.Quantities["input_audio_tokens"] != 3000 {
		t.Fatalf("%+v %v", merged, err)
	}
	duplicate, err := MergeBillingObservation(merged, next)
	if err != nil || duplicate.Quantities["input_audio_tokens"] != 3000 {
		t.Fatal("duplicate was not idempotent")
	}
	next.Quantities["input_audio_tokens"] = 4000
	if _, err := MergeBillingObservation(merged, next); err == nil {
		t.Fatal("accepted change to final evidence")
	}
	if _, err := MergeBillingObservation(merged, first); err == nil {
		t.Fatal("accepted regression")
	}
	if merged.Quantities["input_audio_tokens"] != 3000 {
		t.Fatal("caller mutation changed frozen evidence")
	}
}

func TestBillingEvidenceValidation(t *testing.T) {
	op := BillingObservation{OperationID: "op", Model: "model", Mode: "sync", Complete: true, Quantities: map[string]int64{"characters": 0}}
	if err := (BillingReport{Revision: BillingRevision, Complete: true, Operations: []BillingObservation{op}}).Validate(); err != nil {
		t.Fatal(err)
	}
	for _, report := range []BillingReport{
		{Revision: 2, Complete: true, Operations: []BillingObservation{op}},
		{Revision: 1, Complete: true},
		{Revision: 1, Complete: true, Operations: []BillingObservation{op, op}},
	} {
		if report.Validate() == nil {
			t.Fatalf("accepted invalid report %+v", report)
		}
	}
	for _, q := range []map[string]int64{{}, {"characters": -1}, {"customer_charge": 1000}} {
		bad := op
		bad.Quantities = q
		if bad.Validate() == nil {
			t.Fatalf("accepted invalid quantities %v", q)
		}
	}
}
