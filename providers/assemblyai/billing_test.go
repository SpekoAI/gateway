package assemblyai

import (
	"context"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"testing"
)

func TestStreamingBillsSessionDurationNotSubmittedAudio(t *testing.T) {
	for _, body := range []string{
		`{"type":"Termination","audio_duration_seconds":1.5,"session_duration_seconds":2}`,
		`{"type":"Termination","audio_duration_seconds":1.5,"session_duration_seconds":0}`,
		`{"type":"Termination","audio_duration_seconds":1.5}`,
	} {
		s := &stream{ctx: context.Background(), events: make(chan runtimepkg.ProviderEvent, 4), billingModel: "universal-3-5-pro", sessionID: "id"}
		done, err := s.handleMessage([]byte(body))
		if err != nil || !done {
			t.Fatalf("%v %v", done, err)
		}
		got := (<-s.events).Billing
		if got == nil || got.ProviderRequestID != "id" {
			t.Fatalf("%+v", got)
		}
		switch body {
		case `{"type":"Termination","audio_duration_seconds":1.5,"session_duration_seconds":2}`:
			if !got.Complete || got.Quantities["duration_seconds"] != 2000 {
				t.Fatalf("%+v", got)
			}
		case `{"type":"Termination","audio_duration_seconds":1.5,"session_duration_seconds":0}`:
			if !got.Complete || got.Quantities["duration_seconds"] != 0 {
				t.Fatalf("%+v", got)
			}
		default:
			if got.Complete {
				t.Fatal("missing duration became zero")
			}
		}
	}
}

func TestTurnDetectionSettingsAreNotBillingFeatures(t *testing.T) {
	for _, key := range []string{
		"end_of_turn_confidence_threshold",
		"min_end_of_turn_silence_when_confident",
		"min_turn_silence",
		"max_turn_silence",
	} {
		options := protocol.RequestOptions{STT: &protocol.SttOptions{
			ProviderOptions: map[string]map[string]any{"assemblyai": {key: 400}},
		}}
		if got := billingFeatures(options, false); len(got) != 0 {
			t.Fatalf("%s billed as a feature: %v", key, got)
		}
	}
}

func TestKeywordBillingDependsOnExecutionMode(t *testing.T) {
	options := protocol.RequestOptions{STT: &protocol.SttOptions{Keywords: []string{"Speko"}}}
	if got := billingFeatures(options, false); len(got) != 0 {
		t.Fatalf("included streaming keywords: %v", got)
	}
	if got := billingFeatures(options, true); len(got) != 1 || got[0] != "keyterms" {
		t.Fatalf("batch add-on missing: %v", got)
	}
}
