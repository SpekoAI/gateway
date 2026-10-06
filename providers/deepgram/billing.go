package deepgram

import (
	"net/url"
	"slices"
)

// multilingualLanguage is the only language VALUE Deepgram prices as its own
// tier. Nova-3 multilingual is not a model id — it is `language=multi` on the
// ordinary nova-3 model — so the tier has to travel on the observation's
// Language field. That field therefore carries the PRICED TIER, not the
// request's language: Deepgram publishes two nova-3 rates, and a frozen
// variant per BCP-47 tag would be a price table the vendor does not have.
const multilingualLanguage = "multi"

// streamingBaseQueryKeys and batchBaseQueryKeys are the request parameters
// that make a transcription happen at all — the ones this adapter sets
// unconditionally, plus the language and reservation tags. Deepgram's
// published per-minute rates are quoted "before add-ons", so any OTHER
// parameter on the executed request is reported as a feature.
//
// Nothing here prices an add-on. A feature that no frozen billing variant
// carries makes the operation unpriceable, which the control plane settles as
// an unresolved obligation rather than charging the base rate for a request
// that was not a base request. That is deliberate: the add-on surcharges are
// not established by any source this repository carries, and billing them at
// the base rate would be a silent undercharge.
//
// `detect_language` is NOT a base key even though the batch path sets it by
// default, for two independent reasons: Deepgram sells language detection
// separately, and with it on, the billed mono/multilingual tier is not
// determined at request time at all. A caller that names a language gets a
// priced request; one that does not gets a flagged, unpriced one.
var streamingBaseQueryKeys = map[string]bool{
	"model": true, "encoding": true, "sample_rate": true, "channels": true,
	"interim_results": true, "endpointing": true, "language": true,
	"tag": true, "extra": true,
}

// punctuate, smart_format and utterances are base keys on the prerecorded
// path because this adapter sets all three on EVERY request (batch.go). No
// caller can make a Deepgram batch request without them, so they cannot be
// what "before add-ons" excludes — the published prerecorded rate would
// describe no request this adapter is able to send.
var batchBaseQueryKeys = map[string]bool{
	"model": true, "language": true, "punctuate": true, "smart_format": true,
	"utterances": true, "extra": true,
}

// billingDimensions reads the query that actually went to Deepgram — after
// the caller's allow-listed provider keys have been merged in — and reports
// the priced language tier and the sorted add-on set. Reading the executed
// query rather than the request options is what makes a tier set through the
// provider-key passthrough (`language=multi`) count.
func billingDimensions(query url.Values, base map[string]bool) (string, []string) {
	language := ""
	if query.Get("language") == multilingualLanguage {
		language = multilingualLanguage
	}
	var features []string
	for key := range query {
		if !base[key] {
			features = append(features, key)
		}
	}
	slices.Sort(features)
	return language, features
}
