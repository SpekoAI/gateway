package soniox

import (
	"encoding/json"
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

// "multi" is the multilingual spelling Deepgram takes; Soniox refuses it as a
// hint (HTTP 400 "Invalid language hint."), so it must mean auto-detect.
func TestMultiLanguageSendsNoHint(t *testing.T) {
	t.Parallel()
	for _, language := range []string{"", "auto", "multi", "MULTI"} {
		if hints := sttLanguageHints(language); hints != nil {
			t.Fatalf("language %q: want no hint, got %v", language, hints)
		}
	}
	if hints := sttLanguageHints("kn-IN"); len(hints) != 1 || hints[0] != "kn" {
		t.Fatalf("kn-IN: got %v", hints)
	}
}

// Endpoint knobs ride the start frame beside the adapter's own fields, which
// a caller setting can never overwrite.
func TestStartPayloadCarriesEndpointSettings(t *testing.T) {
	t.Parallel()
	start := sttStartRequest{Model: "stt-rt-v5", EnableEndpointDetection: true, EnableLanguageIdentification: true}
	payload, err := sttStartPayload(start, map[string]any{"max_endpoint_delay_ms": float64(500), "model": "other"})
	if err != nil {
		t.Fatal(err)
	}
	var frame map[string]any
	if err := json.Unmarshal(payload, &frame); err != nil {
		t.Fatal(err)
	}
	if frame["max_endpoint_delay_ms"] != float64(500) || frame["model"] != "stt-rt-v5" || frame["enable_language_identification"] != true {
		t.Fatalf("frame: %s", payload)
	}
	unchanged, err := sttStartPayload(start, nil)
	if err != nil {
		t.Fatal(err)
	}
	direct, _ := json.Marshal(start)
	if string(unchanged) != string(direct) {
		t.Fatalf("no settings must keep the exact frame: %s vs %s", unchanged, direct)
	}
}

// A final reports the language most of its words were identified in.
func TestFinalReportsMajorityIdentifiedLanguage(t *testing.T) {
	t.Parallel()
	var segment sttSegment
	for _, token := range []sttToken{{Text: "ನಮಸ್ಕಾರ", Language: "kn"}, {Text: " ", Language: "te"}, {Text: " హలో", Language: "te"}, {Text: " ಹೇಗಿದ್ದೀರಿ", Language: "kn"}} {
		segment.appendFinal(token)
	}
	_, language, _, _, _ := segment.flush()
	if language != "kn" {
		t.Fatalf("language: got %q, want kn", language)
	}
	var data map[string]any
	if err := json.Unmarshal(sttTranscriptData("x", "", language, true, nil, nil, nil, "req"), &data); err != nil {
		t.Fatal(err)
	}
	if data["language"] != "kn" {
		t.Fatalf("data: %v", data)
	}
	var unidentified map[string]any
	if err := json.Unmarshal(sttTranscriptData("x", "", "", true, nil, nil, nil, "req"), &unidentified); err != nil {
		t.Fatal(err)
	}
	if _, present := unidentified["language"]; present {
		t.Fatalf("a session without identification must keep its exact bytes: %v", unidentified)
	}
}
