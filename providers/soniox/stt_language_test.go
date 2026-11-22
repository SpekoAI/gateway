package soniox

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

func TestSTTLanguageIdentificationLeavesDefaultBytesUnchanged(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		hint     string
		explicit *bool
	}{
		{name: "legacy automatic language"},
		{name: "legacy hinted language", hint: "en"},
		{name: "explicit false keeps automatic language", explicit: sttBoolPointer(false)},
		{name: "explicit false keeps hinted language", hint: "en", explicit: sttBoolPointer(false)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			token := sonioxToken("Hello", 120, 380, 1, true, "")
			if tc.hint == "" {
				token["language"] = "en"
			}
			start, events := runSTTScript(t, func(url string) runtimepkg.AdapterRequest {
				request := sttAdapterRequest(url)
				request.Options.Language = tc.hint
				if tc.explicit != nil {
					request.Options.STT = &protocol.SttOptions{LanguageIdentification: tc.explicit}
				}
				return request
			}, []map[string]any{sonioxFrame(token), sonioxFrame(sonioxEnd(400))}, 3)

			const delta = `{"audio_end_ms":380,"audio_start_ms":120,"is_final":false,"provider_request_id":"","speech_final":false,"text":"Hello"}`
			final := `{"audio_end_ms":380,"audio_start_ms":120,"confidence":1,"is_final":true,"provider_request_id":"","speech_final":true,"text":"Hello"}`
			if tc.hint == "" {
				final = `{"audio_end_ms":380,"audio_start_ms":120,"confidence":1,"is_final":true,"language":"en","provider_request_id":"","speech_final":true,"text":"Hello"}`
				if start["enable_language_identification"] != true {
					t.Fatalf("legacy automatic setup must ask for vendor token labels: %v", start)
				}
			} else if _, present := start["enable_language_identification"]; present {
				t.Fatalf("legacy hinted setup must omit vendor token labels: %v", start)
			}
			if events[0].Type != protocol.EventTranscriptDelta || string(events[0].Data) != delta {
				t.Errorf("delta changed: %s", events[0].Data)
			}
			if events[1].Type != protocol.EventTranscriptFinal || string(events[1].Data) != final {
				t.Errorf("final changed: %s", events[1].Data)
			}
		})
	}
}

func TestSTTLanguageRunsFollowMixedSourceAndProvisionalRevisions(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		diarize bool
	}{
		{name: "languages only"},
		{name: "languages and speakers", diarize: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			hello := sttLabeledToken("  Hel", 120, 220, true, "1", "en")
			piece := sttLabeledToken("lo", 220, 380, true, "1", "EN")
			provisional := sttLabeledToken(" мир", 410, 620, false, "1", "en")
			revised := sonioxWithoutTime(sttLabeledToken(" мир", 410, 620, false, "1", "ru"), "end_ms")
			final := sttLabeledToken(" мир", 410, 650, true, "1", "ru")
			next := sttLabeledToken("!", 650, 660, true, "2", "ru")
			unknown := sttLabeledToken(" okay", 680, 810, true, "2", "und")
			invalid := sttLabeledToken(" perhaps", 820, 910, true, "2", "not_a_tag")
			translated := sttLabeledToken(" Hola", 0, 0, true, "9", "es")
			translated["translation_status"] = "translation"
			frames := []map[string]any{
				sonioxFrame(hello, piece, provisional),
				sonioxFrame(revised),
				sonioxFrame(final, next, unknown, invalid, translated, sonioxEnd(1000)),
			}

			start, events := runSTTScriptUntil(t, func(url string) runtimepkg.AdapterRequest {
				request := sttAdapterRequest(url)
				request.Plan.Route.Model = "stt-rt-v6"
				request.Options.STT = &protocol.SttOptions{
					LanguageIdentification: sttBoolPointer(true),
					Diarization:            sttBoolPointer(tc.diarize),
					Translation:            &protocol.SttTranslation{TargetLanguage: "es"},
				}
				return request
			}, frames, untilSpeechEnded)
			if start["enable_language_identification"] != true || !reflect.DeepEqual(start["language_hints"], []any{"en"}) {
				t.Fatalf("opt-in should enable token labels alongside the existing hint: %v", start)
			}
			if len(events) != 4 || events[0].Type != protocol.EventTranscriptDelta ||
				events[1].Type != protocol.EventTranscriptDelta || events[2].Type != protocol.EventTranscriptFinal {
				t.Fatalf("unexpected event boundaries: %v", events)
			}
			one, two := "", ""
			if tc.diarize {
				one, two = "1", "2"
			}
			assertSTTLanguageRuns(t, events[0], "Hello мир", "", []sttLanguageRun{
				{0, 9, "en", one, 120, 620},
			})
			assertSTTLanguageRuns(t, events[1], "Hello мир", "", []sttLanguageRun{
				{0, 5, "en", one, 120, 380},
				{6, 9, "ru", one, 410, -1},
			})
			wantFinal := []sttLanguageRun{{0, 5, "en", one, 120, 380}}
			if tc.diarize {
				wantFinal = append(wantFinal, sttLanguageRun{6, 9, "ru", one, 410, 650}, sttLanguageRun{9, 10, "ru", two, 650, 660})
			} else {
				wantFinal = append(wantFinal, sttLanguageRun{6, 10, "ru", "", 410, 660})
			}
			wantFinal = append(wantFinal, sttLanguageRun{11, 23, "", two, 680, 910})
			assertSTTLanguageRuns(t, events[2], "Hello мир! okay perhaps", "Hola", wantFinal)
		})
	}
}

func TestSTTLanguageOnlyLongSegmentKeepsTextAndRebasesRunsAfterCut(t *testing.T) {
	t.Parallel()

	frames, words := longStretch(80, true)
	for index, frame := range frames {
		for _, raw := range frame["tokens"].([]any) {
			token := raw.(map[string]any)
			if index < 40 {
				token["language"] = "en"
			} else {
				token["language"] = "ru"
			}
		}
	}
	_, events := runSTTScriptUntil(t, func(url string) runtimepkg.AdapterRequest {
		request := sttAdapterRequest(url)
		request.Options.STT = &protocol.SttOptions{LanguageIdentification: sttBoolPointer(true)}
		return request
	}, frames, untilSpeechEnded)

	var finals []string
	var prefix int
	for _, event := range events {
		if event.Type != protocol.EventTranscriptFinal {
			continue
		}
		view := decodeSTTLanguageRuns(t, event)
		finals = append(finals, view.Text)
		if view.Interim {
			prefix++
		}
		if len(view.Runs) == 0 || view.Runs[0].From != 0 {
			t.Errorf("final runs must start at the beginning of their own text: %s", event.Data)
		}
		for _, run := range view.Runs {
			if run.Language == "" || run.Speaker != "" || run.To > len([]rune(view.Text)) {
				t.Errorf("invalid language-only run: %s", event.Data)
			}
		}
	}
	if prefix != 1 || strings.Join(finals, " ") != strings.Join(words, " ") {
		t.Errorf("expected one interim final and all words once, got %d and %q", prefix, finals)
	}
}

func sttLabeledToken(text string, start, end int64, final bool, speaker, language string) map[string]any {
	token := sonioxToken(text, start, end, 1, final, speaker)
	token["language"] = language
	return token
}

type sttLanguageRun struct {
	from, to          int
	language, speaker string
	start, end        int64
}

type sttLanguageView struct {
	Text        string `json:"text"`
	Translation string `json:"translation"`
	Interim     bool   `json:"interim"`
	Runs        []struct {
		From     int    `json:"text_start"`
		To       int    `json:"text_end"`
		Language string `json:"language"`
		Speaker  string `json:"speaker"`
		Start    *int64 `json:"start_ms"`
		End      *int64 `json:"end_ms"`
	} `json:"language_runs"`
}

func decodeSTTLanguageRuns(t *testing.T, event runtimepkg.ProviderEvent) sttLanguageView {
	t.Helper()
	var view sttLanguageView
	if err := json.Unmarshal(event.Data, &view); err != nil {
		t.Fatal(err)
	}
	return view
}

func assertSTTLanguageRuns(t *testing.T, event runtimepkg.ProviderEvent, text, translation string, want []sttLanguageRun) {
	t.Helper()
	view := decodeSTTLanguageRuns(t, event)
	if view.Text != text || view.Translation != translation {
		t.Errorf("text and translation = %q, %q, want %q, %q", view.Text, view.Translation, text, translation)
	}
	timeOrMissing := func(time *int64) int64 {
		if time == nil {
			return -1
		}
		return *time
	}
	runs := make([]sttLanguageRun, 0, len(view.Runs))
	for _, run := range view.Runs {
		runs = append(runs, sttLanguageRun{run.From, run.To, run.Language, run.Speaker, timeOrMissing(run.Start), timeOrMissing(run.End)})
	}
	if !reflect.DeepEqual(runs, want) {
		t.Errorf("runs = %+v, want %+v (%s)", runs, want, event.Data)
	}
}
