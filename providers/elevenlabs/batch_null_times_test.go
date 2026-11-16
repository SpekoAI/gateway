package elevenlabs

import (
	"context"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

// Scribe documents a word's start and end as nullable. A word sent without
// them keeps its text and sits between the timed words around it, instead of
// opening a segment at 0 ms.
func TestBatchTranscribePlacesWordsScribeSentWithoutTimes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		words string
		want  []runtimepkg.BatchSegment
	}{
		{
			name:  "leading word with no times",
			words: `{"text":"Um","type":"word","start":null,"end":null,"speaker_id":"speaker_0"},{"text":"hello.","type":"word","start":5.0,"end":5.4,"speaker_id":"speaker_0"}`,
			want:  []runtimepkg.BatchSegment{{Text: "Um hello.", StartMS: 5000, EndMS: 5400, Speaker: "speaker_0"}},
		},
		{
			name:  "untimed word opening a new speaker's turn",
			words: `{"text":"Hi.","type":"word","start":1.0,"end":1.2,"speaker_id":"speaker_0"},{"text":"So","type":"word","speaker_id":"speaker_1"},{"text":"yes.","type":"word","start":6.0,"end":6.3,"speaker_id":"speaker_1"}`,
			want: []runtimepkg.BatchSegment{
				{Text: "Hi.", StartMS: 1000, EndMS: 1200, Speaker: "speaker_0"},
				{Text: "So yes.", StartMS: 6000, EndMS: 6300, Speaker: "speaker_1"},
			},
		},
		{
			name:  "untimed word closing a speaker's turn",
			words: `{"text":"Hi","type":"word","start":1.0,"end":1.2,"speaker_id":"speaker_0"},{"text":"there.","type":"word","speaker_id":"speaker_0"},{"text":"Yes.","type":"word","start":6.0,"end":6.3,"speaker_id":"speaker_1"}`,
			want: []runtimepkg.BatchSegment{
				{Text: "Hi there.", StartMS: 1000, EndMS: 1200, Speaker: "speaker_0"},
				{Text: "Yes.", StartMS: 6000, EndMS: 6300, Speaker: "speaker_1"},
			},
		},
		{
			name:  "trailing word with no times",
			words: `{"text":"Okay","type":"word","start":3.0,"end":3.3},{"text":"bye.","type":"word"}`,
			want:  []runtimepkg.BatchSegment{{Text: "Okay bye.", StartMS: 3000, EndMS: 3300}},
		},
		{
			name:  "only one bound present",
			words: `{"text":"One","type":"word","start":2.0,"end":2.2},{"text":"two","type":"word","start":null,"end":2.6},{"text":"three.","type":"word","start":2.7,"end":3.0}`,
			want:  []runtimepkg.BatchSegment{{Text: "One two three.", StartMS: 2000, EndMS: 3000}},
		},
		{
			name:  "trailing word with only an end keeps it, without a gap",
			words: `{"text":"Okay","type":"word","start":1.0,"end":1.2},{"text":"goodbye.","type":"word","end":2.6}`,
			want:  []runtimepkg.BatchSegment{{Text: "Okay goodbye.", StartMS: 1000, EndMS: 2600}},
		},
		{
			name:  "leading word with only a start keeps it, without a gap",
			words: `{"text":"Well","type":"word","start":2.0},{"text":"yes.","type":"word","start":5.0,"end":5.3}`,
			want:  []runtimepkg.BatchSegment{{Text: "Well yes.", StartMS: 2000, EndMS: 5300}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(`{"language_code":"en","text":"x","transcription_id":"tx","audio_duration_secs":7,"words":[` + tc.words + `]}`))
			}))
			defer server.Close()
			adapter, err := NewBatch(BatchConfig{HTTPClient: server.Client(), AllowedEndpointHosts: []string{"127.0.0.1"}, AllowInsecureEndpoint: true})
			if err != nil {
				t.Fatalf("adapter: %v", err)
			}
			result, err := adapter.Transcribe(context.Background(), runtimepkg.BatchTranscribeRequest{Plan: batchPlan(server.URL + "/v1/speech-to-text"), Audio: strings.NewReader("RIFF....WAVE"), AudioBytes: 12})
			if err != nil {
				t.Fatalf("transcribe: %v", err)
			}
			if !reflect.DeepEqual(result.Segments, tc.want) {
				t.Fatalf("segments = %+v, want %+v", result.Segments, tc.want)
			}
		})
	}
}

func TestTimeScribeWordsPlacesEachUntimedWord(t *testing.T) {
	t.Parallel()
	at := func(seconds float64) *float64 { return &seconds }
	type span struct{ start, end int64 }
	cases := []struct {
		name  string
		words []scribeWord
		want  []span
	}{
		{"all untimed", []scribeWord{{text: "a"}, {text: "b"}}, []span{{0, 0}, {0, 0}}},
		{"start only reaches the next word", []scribeWord{{text: "a", start: at(1)}, {text: "b", start: at(5), end: at(5.5)}}, []span{{1000, 5000}, {5000, 5500}}},
		{"end only reaches back to the previous word", []scribeWord{{text: "a", start: at(1), end: at(1.4)}, {text: "b", end: at(2)}}, []span{{1000, 1400}, {1400, 2000}}},
		{"lone start only", []scribeWord{{text: "a", start: at(1)}}, []span{{1000, 1000}}},
		{"lone end only", []scribeWord{{text: "a", end: at(2)}}, []span{{2000, 2000}}},
		{"one-bound words side by side", []scribeWord{{text: "a", end: at(1)}, {text: "b", start: at(3)}, {text: "c", start: at(4), end: at(4.2)}}, []span{{1000, 1000}, {3000, 4000}, {4000, 4200}}},
		{"end before start", []scribeWord{{text: "a", start: at(2), end: at(1)}}, []span{{2000, 2000}}},
		{"consecutive untimed lead in", []scribeWord{{text: "a"}, {text: "b"}, {text: "c", start: at(3), end: at(3.2)}}, []span{{3000, 3000}, {3000, 3000}, {3000, 3200}}},
		{"same speaker after a gap", []scribeWord{{text: "a", start: at(1), end: at(1.2)}, {text: "b"}, {text: "c", start: at(5), end: at(5.2)}}, []span{{1000, 1200}, {5000, 5000}, {5000, 5200}}},
		{
			"turn end before another speaker",
			[]scribeWord{{text: "a", speaker: "s0", start: at(1), end: at(1.2)}, {text: "b", speaker: "s0"}, {text: "c", speaker: "s1", start: at(6), end: at(6.3)}},
			[]span{{1000, 1200}, {1200, 1200}, {6000, 6300}},
		},
		{
			"turn start after another speaker",
			[]scribeWord{{text: "a", speaker: "s0", start: at(1), end: at(1.2)}, {text: "b", speaker: "s1"}, {text: "c", speaker: "s1", start: at(6), end: at(6.3)}},
			[]span{{1000, 1200}, {6000, 6000}, {6000, 6300}},
		},
		{
			"alternating untimed speakers stay in time order",
			[]scribeWord{{text: "a", speaker: "s0", start: at(1), end: at(1.2)}, {text: "b", speaker: "s1"}, {text: "c", speaker: "s0"}, {text: "d", speaker: "s1", start: at(6), end: at(6.3)}},
			[]span{{1000, 1200}, {6000, 6000}, {6000, 6000}, {6000, 6300}},
		},
		{
			"turn end then turn start in one untimed run",
			[]scribeWord{{text: "a", speaker: "s0", start: at(1), end: at(1.2)}, {text: "b", speaker: "s0"}, {text: "c", speaker: "s1"}, {text: "d", speaker: "s1", start: at(6), end: at(6.3)}},
			[]span{{1000, 1200}, {1200, 1200}, {6000, 6000}, {6000, 6300}},
		},
		{
			"no same-speaker neighbour",
			[]scribeWord{{text: "a", speaker: "s0", start: at(1), end: at(1.2)}, {text: "b", speaker: "s2"}, {text: "c", speaker: "s1", start: at(6), end: at(6.3)}},
			[]span{{1000, 1200}, {6000, 6000}, {6000, 6300}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			words := timeScribeWords(tc.words)
			got := make([]span, len(words))
			for i, word := range words {
				got[i] = span{word.StartMS, word.EndMS}
				if word.Text != tc.words[i].text || word.Speaker != tc.words[i].speaker {
					t.Fatalf("word %d = %+v, want text %q speaker %q", i, word, tc.words[i].text, tc.words[i].speaker)
				}
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("spans = %v, want %v", got, tc.want)
			}
		})
	}
}

// Whatever the mix of speakers and missing times, placed words never go back
// in time, so segments come out in order.
func TestTimeScribeWordsKeepsTimeOrder(t *testing.T) {
	t.Parallel()
	random := rand.New(rand.NewPCG(1, 2))
	for range 2000 {
		words := make([]scribeWord, 1+random.IntN(8))
		clock := 0.0
		for i := range words {
			words[i] = scribeWord{text: "w", speaker: []string{"s0", "s1", "s2"}[random.IntN(3)]}
			if random.IntN(2) == 0 {
				clock += 0.1 + random.Float64()*3
				start, end := clock, clock+0.2
				switch random.IntN(4) {
				case 0:
					words[i].start = &start
				case 1:
					words[i].end = &end
				default:
					words[i].start, words[i].end = &start, &end
				}
				clock = end
			}
		}
		placed := timeScribeWords(words)
		for i, word := range placed {
			if word.EndMS < word.StartMS {
				t.Fatalf("word %d ends at %d before it starts at %d: %+v", i, word.EndMS, word.StartMS, placed)
			}
		}
		for i := 1; i < len(placed); i++ {
			if placed[i].StartMS < placed[i-1].StartMS {
				t.Fatalf("word %d starts at %d before word %d at %d: %+v", i, placed[i].StartMS, i-1, placed[i-1].StartMS, placed)
			}
		}
	}
}
