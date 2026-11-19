package soniox

import (
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

// checkFinalsCover walks text with the events' finals in order: each final
// must be the next stretch of text, give or take the spaces a final trims,
// and must start and end on a character boundary (sttGraphemeBreaks, pinned
// by TestSTTGraphemeBreaks), never inside one. Nothing may be left over. It
// returns how many finals there were.
func checkFinalsCover(t *testing.T, events []runtimepkg.ProviderEvent, text string) int {
	t.Helper()
	breaks := sttGraphemeBreaks(text)
	cursor, finals := 0, 0
	for _, event := range events {
		if event.Type != protocol.EventTranscriptFinal {
			continue
		}
		final := decodeTranslated(t, event.Data).Text
		for cursor < len(text) && text[cursor] == ' ' {
			cursor++
		}
		if !strings.HasPrefix(text[cursor:], final) {
			t.Fatalf("final %d is not the next stretch of the text at byte %d:\n final %+q\n  text %+q", finals, cursor, clip(final), clip(text[cursor:]))
		}
		end := cursor + len(final)
		if !breaks[cursor] || !breaks[end] {
			t.Fatalf("final %d (bytes %d-%d) starts or ends inside a character: ...%+q | %+q ... | %+q", finals, cursor, end, around(text, cursor), clip(final), around(text, end))
		}
		cursor = end
		finals++
	}
	if rest := strings.TrimLeft(text[cursor:], " "); rest != "" {
		t.Fatalf("after %d finals, text left over: %+q", finals, clip(rest))
	}
	return finals
}

func clip(text string) string {
	if utf8.RuneCountInString(text) <= 12 {
		return text
	}
	return string([]rune(text)[:12]) + "..."
}

func around(text string, offset int) string {
	from, to := max(0, offset-12), min(len(text), offset+12)
	for from > 0 && !utf8.RuneStart(text[from]) {
		from--
	}
	for to < len(text) && !utf8.RuneStart(text[to]) {
		to++
	}
	return text[from:offset] + "|" + text[offset:to]
}

// The character boundaries a cut may use are Unicode's extended grapheme
// cluster boundaries (UAX #29), including the Indic conjunct rule.
func TestSTTGraphemeBreaks(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name  string
		left  string
		right string
		want  bool
	}{
		{"two letters", "ab", "c", true},
		{"before a space", "ab", " c", true},
		{"combining acute", "e", "\u0301", false},
		{"Thai sara am", "ก", "ำ", false},
		{"Thai tone mark", "ก", "\u0e48", false},
		{"Thai consonants", "ก", "ข", true},
		{"Lao sara am", "ກ", "ຳ", false},
		{"Devanagari conjunct", "क्", "ष", false},
		{"Devanagari conjunct with joiner", "क्\u200d", "ष", false},
		{"Devanagari virama before the linker", "क", "्ष", false},
		{"Devanagari vowel sign", "क", "ा", false},
		{"Bengali conjunct", "ক্", "ষ", false},
		{"virama before Latin", "क्", "a", true},
		{"lone virama before a consonant", " ्", "क", true},
		{"Devanagari syllables", "का", "म", true},
		{"emoji before joiner", "👩", "\u200d💻", false},
		{"emoji after joiner", "👩\u200d", "💻", false},
		{"variation selector after a heart", "❤", "\ufe0f", false},
		{"variation selector after a full stop", "あ。", "\ufe00", false},
		{"skin tone", "👍", "\U0001F3FD", false},
		{"inside a flag", "🇯", "🇵", false},
		{"between two flags", "🇯🇵", "🇫🇷", true},
		{"inside the second flag", "🇯🇵🇫", "🇷", false},
		{"Hangul leading then vowel jamo", "\u1100", "\u1161", false},
		{"Hangul vowel then trailing jamo", "\u1100\u1161", "\u11a8", false},
		{"Hangul syllable then leading jamo", "\u1100\u1161\u11a8", "\u1100", true},
		{"Hangul precomposed then trailing jamo", "가", "\u11a8", false},
		{"CR LF", "a\r", "\n", false},
	} {
		breaks := sttGraphemeBreaks(testCase.left + testCase.right)
		if got := breaks[len(testCase.left)]; got != testCase.want {
			t.Errorf("%s: break between %+q and %+q = %v, want %v", testCase.name, testCase.left, testCase.right, got, testCase.want)
		}
	}
}

// The three cuts a review saw part a character, each replayed on the real
// adapter: the bound passes on the frame that finalizes the 2,001st
// character, the boundary the cut would pick lies inside one, and the cut
// steps back to the boundary before that character.
func TestSTTCutNeverPartsACharacter(t *testing.T) {
	t.Parallel()

	// repeat is count copies of one character in pieces of up to ten, so
	// the long run before the bound costs few frames.
	repeat := func(character string, count int) []string {
		var list []string
		for ; count > 0; count -= 10 {
			list = append(list, strings.Repeat(character, min(count, 10)))
		}
		return list
	}
	for _, testCase := range []struct {
		name   string
		pieces []string
		want   []string
	}{
		{
			// The 2,001st character, sara am, passes the bound: the cut
			// would fall between ก and ำ.
			name:   "Thai sara am at the bound",
			pieces: append(repeat("ข", 1999), "ก", "ำ", "ข"),
			want:   []string{strings.Repeat("ข", 1999), "กำข"},
		},
		{
			// क्ष is one character: virama between two consonants.
			name:   "Devanagari conjunct at the bound",
			pieces: append(repeat("प", 1998), "क", "्", "ष", "प"),
			want:   []string{strings.Repeat("प", 1998), "क्षप"},
		},
		{
			// A variation selector after a full stop belongs to it; the
			// sentence end is not a boundary there.
			name:   "variation selector after a Japanese full stop",
			pieces: append(repeat("あ", 1995), "。", "\ufe00", "い", "い", "い", "い", "い"),
			want:   []string{strings.Repeat("あ", 1995) + "。\ufe00いいい", "いい"},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			_, events := runSTTScriptUntil(t, diarizedRequest, pieceStretch(testCase.pieces, 1, 2), untilSpeechEnded)
			var finals []string
			for _, event := range events {
				if event.Type == protocol.EventTranscriptFinal {
					finals = append(finals, decodeTranslated(t, event.Data).Text)
				}
			}
			if strings.Join(finals, "\n") != strings.Join(testCase.want, "\n") {
				got := make([]string, len(finals))
				for index, final := range finals {
					got[index] = around(final, len(final)) + " (" + strconv.Itoa(utf8.RuneCountInString(final)) + " characters)"
				}
				t.Fatalf("finals end:\n %s", strings.Join(got, "\n "))
			}
		})
	}
}

// randomUnit is one whole character of the scripts most often parted by a
// careless cut: Thai, Devanagari conjuncts, emoji sequences, Hangul jamo and
// Latin with combining marks, with spaces and sentence ends between them.
func randomUnit(rng *rand.Rand) string {
	pick := func(from, to rune) string { return string(from + rng.Int32N(to-from+1)) }
	switch rng.IntN(12) {
	case 0, 1: // Thai consonant, maybe a vowel or tone mark, maybe sara am
		unit := pick(0x0E01, 0x0E2E)
		if rng.IntN(2) == 0 {
			unit += []string{"\u0e31", "\u0e34", "\u0e35", "\u0e36", "\u0e37", "\u0e38", "\u0e39", "\u0e48", "\u0e49", "\u0e4a", "\u0e4b"}[rng.IntN(11)]
		}
		if rng.IntN(3) == 0 {
			unit += "ำ"
		}
		return unit
	case 2, 3: // Devanagari consonant, maybe a conjunct, maybe a vowel sign
		unit := pick(0x0915, 0x0939)
		if rng.IntN(2) == 0 {
			unit += "्"
			if rng.IntN(4) == 0 {
				unit += "\u200d"
			}
			unit += pick(0x0915, 0x0939)
		}
		if rng.IntN(2) == 0 {
			unit += pick(0x093E, 0x094C)
		}
		return unit
	case 4: // emoji sequences
		return []string{"👩\u200d💻", "👨\u200d👩\u200d👧", "❤\ufe0f", "👍\U0001F3FD", "🇯🇵", "🇫🇷"}[rng.IntN(6)]
	case 5, 6: // Hangul: conjoining jamo or a precomposed syllable
		if rng.IntN(3) == 0 {
			return pick(0xAC00, 0xD7A3)
		}
		unit := pick(0x1100, 0x1112) + pick(0x1161, 0x1175)
		if rng.IntN(2) == 0 {
			unit += pick(0x11A8, 0x11C2)
		}
		return unit
	case 7, 8, 9: // Latin letter with up to two combining marks
		unit := pick('a', 'z')
		for range rng.IntN(3) {
			unit += pick(0x0300, 0x036F)
		}
		return unit
	case 10:
		return " "
	default: // a sentence end, sometimes with a variation selector
		return []string{".", "。", "！", "。\ufe00"}[rng.IntN(4)]
	}
}

// randomPieces is about characters runes of random units cut into pieces of
// one to three runes wherever the cut falls, inside characters too, as
// Soniox's tokens may.
func randomPieces(rng *rand.Rand, characters int) ([]string, string) {
	var text strings.Builder
	for utf8.RuneCountInString(text.String()) < characters {
		text.WriteString(randomUnit(rng))
	}
	runes := []rune(text.String())
	var pieces []string
	for len(runes) > 0 {
		size := min(1+rng.IntN(3), len(runes))
		pieces = append(pieces, string(runes[:size]))
		runes = runes[size:]
	}
	return pieces, text.String()
}

// Whatever the script and wherever the tokens part it, a cut at either bound
// never starts or ends a final inside a character, and the finals spell the
// text exactly once, in order.
func TestSTTCutsKeepEveryCharacterWhole(t *testing.T) {
	t.Parallel()

	for _, bound := range []struct {
		name       string
		msPerPiece int64
	}{
		{name: "characters", msPerPiece: 2},
		{name: "time", msPerPiece: 150},
	} {
		for seed := range uint64(12) {
			t.Run(bound.name+"/seed "+strconv.FormatUint(seed, 10), func(t *testing.T) {
				t.Parallel()
				rng := rand.New(rand.NewPCG(seed, 0x5011))
				pieces, text := randomPieces(rng, 2200+rng.IntN(1600))
				frames := pieceStretch(pieces, 1+rng.IntN(4), bound.msPerPiece)
				_, events := runSTTScriptUntil(t, diarizedRequest, frames, untilSpeechEnded)
				if finals := checkFinalsCover(t, events, text); finals < 2 {
					t.Fatalf("%d finals, want the text cut", finals)
				}
			})
		}
	}
}
