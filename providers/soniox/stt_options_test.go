package soniox

import (
	"testing"

	"github.com/SpekoAI/gateway/protocol"
)

func sttBoolPointer(value bool) *bool { return &value }

// Speaker labels and vocabulary biasing both ride the one start frame, in the
// spellings Soniox's WebSocket API documents: enable_speaker_diarization and
// context.terms.
func TestSttStartRequestCarriesDiarizationAndContext(t *testing.T) {
	t.Parallel()
	options := &protocol.SttOptions{
		Diarization: sttBoolPointer(true),
		Keywords:    []string{"Speko", "Speko", "São Paulo"},
	}
	start := sttStartRequest{
		EnableSpeakerDiariz: options.Diarize(),
		Context:             sttContextTerms(options.GetKeywords()),
	}
	if !start.EnableSpeakerDiariz {
		t.Fatal("diarization must reach the start frame")
	}
	// Duplicates collapse; order and content otherwise survive intact. No
	// silent truncation: the protocol's own bounds keep the total under the
	// vendor's context ceiling.
	if start.Context == nil || len(start.Context.Terms) != 2 || start.Context.Terms[1] != "São Paulo" {
		t.Fatalf("context terms: %+v", start.Context)
	}
}

// A request that asked for nothing keeps the start frame it always had: no
// context object, no diarization key (omitempty on a false bool).
func TestSttStartRequestWithoutOptionsIsUnchanged(t *testing.T) {
	t.Parallel()
	var options *protocol.SttOptions
	if options.Diarize() {
		t.Fatal("nil options never diarize")
	}
	if sttContextTerms(options.GetKeywords()) != nil {
		t.Fatal("nil options carry no context")
	}
}
