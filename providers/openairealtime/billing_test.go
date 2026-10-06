package openairealtime

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

func newBillingStream(t *testing.T, provider, model string) *realtimeStream {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &realtimeStream{
		provider: provider, profile: profiles[provider], model: model,
		ctx: ctx, cancel: cancel, events: make(chan runtimepkg.ProviderEvent, 16),
		// settleSetup publishes to setupDone; Open owns the receive, so the
		// unit test buffers it rather than standing up a socket.
		setupDone: make(chan error, 1),
	}
}

// billingFor drives one raw vendor event through handle and returns the
// observation it established, or nil.
func billingFor(t *testing.T, stream *realtimeStream, raw string) *protocol.BillingObservation {
	t.Helper()
	var event serverEvent
	if err := json.Unmarshal([]byte(raw), &event); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	stream.handle(event, []byte(raw))
	for {
		select {
		case emitted := <-stream.events:
			if emitted.Billing != nil {
				return emitted.Billing
			}
		default:
			return nil
		}
	}
}

const realtimeUsageFixture = `{"type":"response.done","response":{"id":"resp_1","status":"completed","usage":{
  "total_tokens":1000,"input_tokens":700,"output_tokens":300,
  "input_token_details":{"cached_tokens":200,"text_tokens":100,"audio_tokens":580,"image_tokens":20,
    "cached_tokens_details":{"text_tokens":40,"audio_tokens":150,"image_tokens":10}},
  "output_token_details":{"text_tokens":50,"audio_tokens":250}}}}`

// TestResponseBillingSplitsTheModalitiesTheVendorPrices is the point of the
// whole change: gpt-realtime charges $32/M for input audio and $4/M for input
// text, so a meter that reported 700 input tokens would be off by 7x on the
// dominant dimension. The observation has to carry the disjoint per-modality
// pieces, with the cached share subtracted out of each one.
func TestResponseBillingSplitsTheModalitiesTheVendorPrices(t *testing.T) {
	t.Parallel()

	observation := billingFor(t, newBillingStream(t, "openai", "gpt-realtime"), realtimeUsageFixture)
	if observation == nil {
		t.Fatal("response.done established no billing observation")
	}
	if err := observation.Validate(); err != nil {
		t.Fatalf("observation must validate: %v", err)
	}
	if !observation.Complete {
		t.Fatal("a reconciling usage document must produce COMPLETE evidence")
	}
	if observation.OperationID != "response/resp_1" || observation.ProviderResponseID != "resp_1" {
		t.Fatalf("operation identity: %+v", observation)
	}
	if observation.Model != "gpt-realtime" || observation.Mode != "streaming" {
		t.Fatalf("variant selectors: %+v", observation)
	}
	want := map[string]int64{
		// 580 audio in the prompt, 150 of them cache reads.
		unitInputAudioTokens: 430_000, unitCachedInputAudioTokens: 150_000,
		// 100 text in the prompt, 40 of them cache reads.
		unitInputTextTokens: 60_000, unitCachedInputTextTokens: 40_000,
		// 20 image in the prompt, 10 of them cache reads.
		unitInputImageTokens: 10_000, unitCachedInputImageTokens: 10_000,
		unitOutputAudioTokens: 250_000, unitOutputTextTokens: 50_000,
	}
	if len(observation.Quantities) != len(want) {
		t.Fatalf("quantities = %v, want the %d priced dimensions", observation.Quantities, len(want))
	}
	for unit, quantity := range want {
		if got := observation.Quantities[unit]; got != quantity {
			t.Errorf("%s = %d, want %d", unit, got, quantity)
		}
	}
	// The disjoint pieces have to add back up to what the vendor reported.
	var input, output int64
	for _, unit := range []string{unitInputAudioTokens, unitCachedInputAudioTokens, unitInputTextTokens, unitCachedInputTextTokens, unitInputImageTokens, unitCachedInputImageTokens} {
		input += observation.Quantities[unit]
	}
	for _, unit := range []string{unitOutputAudioTokens, unitOutputTextTokens} {
		output += observation.Quantities[unit]
	}
	if input != 700_000 || output != 300_000 {
		t.Fatalf("split sums to %d input / %d output, want 700000 / 300000", input, output)
	}
}

// TestResponseBillingRefusesEvidenceThatDoesNotReconcile pins the fail-closed
// half. Every one of these shapes could be turned into a plausible-looking
// number, and each one would be an invented bill; the observation stays
// INCOMPLETE instead, which leaves the reservation unresolved rather than
// settling it.
func TestResponseBillingRefusesEvidenceThatDoesNotReconcile(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		raw  string
	}{
		{"no usage document at all", `{"type":"response.done","response":{"id":"r","status":"completed"}}`},
		{"totals with no modality split", `{"type":"response.done","response":{"id":"r","usage":{"total_tokens":30,"input_tokens":10,"output_tokens":20}}}`},
		{"modality split disagrees with the input total", `{"type":"response.done","response":{"id":"r","usage":{"input_tokens":10,"output_tokens":0,"input_token_details":{"text_tokens":3,"audio_tokens":3,"image_tokens":0}}}}`},
		{"output split disagrees with the output total", `{"type":"response.done","response":{"id":"r","usage":{"input_tokens":0,"output_tokens":10,"output_token_details":{"text_tokens":1,"audio_tokens":1}}}}`},
		{"cached total with no cached split", `{"type":"response.done","response":{"id":"r","usage":{"input_tokens":10,"output_tokens":0,"input_token_details":{"cached_tokens":4,"text_tokens":0,"audio_tokens":10,"image_tokens":0}}}}`},
		{"cached split disagrees with the cached total", `{"type":"response.done","response":{"id":"r","usage":{"input_tokens":10,"output_tokens":0,"input_token_details":{"cached_tokens":4,"text_tokens":0,"audio_tokens":10,"image_tokens":0,"cached_tokens_details":{"text_tokens":0,"audio_tokens":2,"image_tokens":0}}}}}`},
		{"more cached audio than audio", `{"type":"response.done","response":{"id":"r","usage":{"input_tokens":10,"output_tokens":0,"input_token_details":{"cached_tokens":10,"text_tokens":8,"audio_tokens":2,"image_tokens":0,"cached_tokens_details":{"text_tokens":6,"audio_tokens":4,"image_tokens":0}}}}}`},
		{"totals disagree with their own sum", `{"type":"response.done","response":{"id":"r","usage":{"total_tokens":99,"input_tokens":10,"output_tokens":20,"input_token_details":{"text_tokens":10,"audio_tokens":0,"image_tokens":0},"output_token_details":{"text_tokens":20,"audio_tokens":0}}}}`},
		{"negative quantity", `{"type":"response.done","response":{"id":"r","usage":{"input_tokens":-1,"output_tokens":0}}}`},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			observation := billingFor(t, newBillingStream(t, "openai", "gpt-realtime"), testCase.raw)
			if observation == nil {
				t.Fatal("a completed response must always establish an observation, complete or not")
			}
			if observation.Complete {
				t.Fatalf("unreconciled evidence was reported as complete: %v", observation.Quantities)
			}
			if len(observation.Quantities) != 0 {
				t.Fatalf("an incomplete observation must claim no quantities, got %v", observation.Quantities)
			}
			if err := observation.Validate(); err != nil {
				t.Fatalf("an incomplete observation must still validate: %v", err)
			}
		})
	}
}

// TestSessionAnchorSeparatesZeroFromUnmetered is why the setup confirmation
// carries an observation. A caller who hangs up before the model answers owes
// nothing, and that is a measured zero; an adapter that never metered at all
// produces the same empty report unless something anchors it.
func TestSessionAnchorSeparatesZeroFromUnmetered(t *testing.T) {
	t.Parallel()

	observation := billingFor(t, newBillingStream(t, "openai", "gpt-realtime"), `{"type":"session.updated","session":{}}`)
	if observation == nil {
		t.Fatal("session.updated established no billing observation")
	}
	if err := observation.Validate(); err != nil {
		t.Fatalf("observation must validate: %v", err)
	}
	if observation.OperationID != sessionOperationID || !observation.Complete {
		t.Fatalf("session anchor: %+v", observation)
	}
	if len(observation.Quantities) != len(zeroBillingQuantities()) {
		t.Fatalf("the anchor must carry the full priced dimension set: %v", observation.Quantities)
	}
	for unit, quantity := range observation.Quantities {
		if quantity != 0 {
			t.Errorf("%s = %d, want a measured zero", unit, quantity)
		}
	}
}

// TestXAIRealtimeEmitsNoTokenBilling holds the two vendors on this adapter
// apart. Grok Voice is sold per connected audio minute plus a flat fee per
// text input; offering settlement a set of token dimensions its rate card
// does not price would fail the attempt outright, so the xAI profile meters
// nothing here and its own units land in a separate change.
func TestXAIRealtimeEmitsNoTokenBilling(t *testing.T) {
	t.Parallel()

	stream := newBillingStream(t, "xai", "grok-voice-think-fast-2.0")
	if got := billingFor(t, stream, realtimeUsageFixture); got != nil {
		t.Fatalf("xAI realtime emitted token billing: %+v", got)
	}
	if got := billingFor(t, newBillingStream(t, "xai", "grok-voice-think-fast-2.0"), `{"type":"session.updated","session":{}}`); got != nil {
		t.Fatalf("xAI realtime emitted a session billing anchor: %+v", got)
	}
	// The OpenAI profile on the same adapter does meter, so the gate is the
	// profile flag and not a silent failure to parse.
	if got := billingFor(t, newBillingStream(t, "openai", "gpt-realtime"), realtimeUsageFixture); got == nil {
		t.Fatal("the openai profile must still meter")
	}
}
