// Package metering normalizes provider evidence without falling back to submitted
// audio or text. Missing, null, fractional sub-milli-unit and malformed values
// remain incomplete. Provider rounding belongs to the frozen rate schedule.
package metering

import (
	"bytes"
	"encoding/json"
	"github.com/SpekoAI/gateway/protocol"
	"math/big"
)

func Quantity(raw []byte, scale int64, path ...string) (int64, bool) {
	for _, field := range path {
		var object map[string]json.RawMessage
		if json.Unmarshal(raw, &object) != nil {
			return 0, false
		}
		raw = object[field]
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] == '"' || string(raw) == "null" || scale <= 0 {
		return 0, false
	}
	var n json.Number
	if json.Unmarshal(raw, &n) != nil {
		return 0, false
	}
	r, ok := new(big.Rat).SetString(string(n))
	if !ok || r.Sign() < 0 {
		return 0, false
	}
	r.Mul(r, new(big.Rat).SetInt64(scale))
	if !r.IsInt() || !r.Num().IsInt64() {
		return 0, false
	}
	return r.Num().Int64(), true
}

func Duration(id, model, mode string, raw []byte, scale int64, path ...string) *protocol.BillingObservation {
	o := &protocol.BillingObservation{OperationID: id, Model: model, Mode: mode, Quantities: map[string]int64{}}
	if n, ok := Quantity(raw, scale, path...); ok {
		o.Quantities["duration_seconds"] = n
		o.Complete = true
	} else if n, ok := Quantity(raw, scale*1_000_000, path...); ok {
		o.Quantities["duration_seconds"] = n
		o.QuantityDenominators = map[string]int64{"duration_seconds": 1_000_000_000}
		o.Complete = true
	}
	return o
}

func Report(o *protocol.BillingObservation) *protocol.BillingReport {
	return &protocol.BillingReport{Revision: protocol.BillingRevision, Complete: o.Complete, Operations: []protocol.BillingObservation{o.Clone()}}
}
