package soniox

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// Soniox bounds one TTS stream twice: its accumulated text at 5,000 bytes
// ("input text too long (limit: 5000 bytes)", however the text is chunked),
// and its generated audio at two minutes, past which the output is truncated
// and the stream ends with max_audio_duration_reached (HTTP 413). The audio
// cap is the binding one. Speech runs at roughly 15 characters a second, so
// two minutes is about 1,800 characters at an ordinary pace, and a slow voice,
// or text full of pauses ("...", [slowly] tags), reaches it well before that.
//
// So the adapter never lets one stream carry more than ttsStreamTextBudget
// cost units: at 15 characters a second that is 80 seconds of audio, and it
// still fits inside the cap at a 10-characters-a-second crawl. Text past the
// budget is not refused; it rolls over onto the next stream of the same
// utterance (see ttsStream.planLocked).
//
// Cost is the UTF-8 byte length, Soniox's own unit for its text cap, except
// that ideographs, kana, and hangul cost ttsWideRuneCost. Those scripts are
// spoken at about 4 to 8 characters a second, so their 3 bytes a character
// would undercount their audio. Every rune costs at least its byte length, so
// the budget also keeps a stream far below the 5,000-byte text cap.
const (
	ttsStreamTextBudget = 1_200
	ttsWideRuneCost     = 4

	// Once a stream holds ttsStreamSoftBudget, the next sentence end rolls
	// it over even though the hard budget is not reached yet. A caller
	// streaming LLM tokens appends a word at a time, and without this the
	// hard budget would land in the middle of whatever sentence was being
	// spoken, which a fresh stream renders with an audible prosody reset.
	ttsStreamSoftBudget = 900
)

// ttsRuneCost is one rune's charge against ttsStreamTextBudget.
func ttsRuneCost(r rune, size int) int {
	if unicode.In(r, unicode.Han, unicode.Hiragana, unicode.Katakana, unicode.Hangul) {
		return ttsWideRuneCost
	}
	return size
}

func ttsTextCost(text string) int {
	cost := 0
	for index := 0; index < len(text); {
		r, size := utf8.DecodeRuneInString(text[index:])
		cost += ttsRuneCost(r, size)
		index += size
	}
	return cost
}

// ttsBoundary is what the text sent so far ends with, which decides how good
// a place the next position is to end a stream.
type ttsBoundary uint8

const (
	ttsBoundaryNone ttsBoundary = iota
	// After . ! ? and their equivalents: a sentence end once whitespace
	// follows, so "3.14" and "v2.1" are not cut.
	ttsBoundarySentence
	// After 。！？: CJK writes no space between sentences, so the next rune
	// starts a new sentence whatever it is.
	ttsBoundarySentenceWide
	ttsBoundaryClause
	ttsBoundaryClauseWide
)

// ttsCutClass ranks a cut position; higher is better.
type ttsCutClass uint8

const (
	ttsCutNone ttsCutClass = iota
	ttsCutSpace
	ttsCutClause
	ttsCutSentence
)

func ttsAdvanceBoundary(state ttsBoundary, r rune) ttsBoundary {
	switch {
	case strings.ContainsRune("。！？｡．", r):
		return ttsBoundarySentenceWide
	case strings.ContainsRune(".!?…‼⁇⁈⁉؟۔।॥።፧։", r):
		return ttsBoundarySentence
	case strings.ContainsRune("、，；：", r):
		return ttsBoundaryClauseWide
	case strings.ContainsRune(",;:—–،؛፣፤", r):
		return ttsBoundaryClause
	case strings.ContainsRune("\"'”’»)]}」』）】〕》〉›", r):
		// A closing quote or bracket keeps the boundary it closes over:
		// `He said "stop."` still ends a sentence.
		return state
	}
	return ttsBoundaryNone
}

func ttsBoundaryAfter(state ttsBoundary, text string) ttsBoundary {
	for _, r := range text {
		state = ttsAdvanceBoundary(state, r)
	}
	return state
}

// ttsCutClassAt rates cutting immediately before rune next, given what the
// text before the cut ends with.
func ttsCutClassAt(state ttsBoundary, next rune) ttsCutClass {
	if unicode.IsSpace(next) {
		switch state {
		case ttsBoundarySentence, ttsBoundarySentenceWide:
			return ttsCutSentence
		case ttsBoundaryClause, ttsBoundaryClauseWide:
			return ttsCutClause
		}
		return ttsCutSpace
	}
	switch state {
	case ttsBoundarySentenceWide:
		return ttsCutSentence
	case ttsBoundaryClauseWide:
		return ttsCutClause
	}
	return ttsCutNone
}

// ttsJoinsPrevious reports a rune that renders as part of the rune before it,
// so a forced cut must not separate them: combining marks (the vowel signs of
// Indic scripts, Arabic harakat), zero-width joiners, and variation selectors.
func ttsJoinsPrevious(r rune) bool {
	return unicode.In(r, unicode.Mn, unicode.Mc, unicode.Me) || r == '‍' || unicode.Is(unicode.Variation_Selector, r)
}

// ttsSplitForStream decides how much of text goes onto a stream that already
// holds used cost units and whose text ends in state. It returns the head to
// send, the tail left for the next stream (leading whitespace removed), and
// whether the stream must be ended now so the tail can start a new one.
//
// final says no further text will follow, so text that fits is never split
// early just to land on a sentence end.
//
// Cuts fall only on rune boundaries. In order of preference a stream ends at a
// sentence end, a clause mark, then whitespace, choosing the latest one that
// still leaves the stream at least half full; failing that, at any whitespace;
// and only text with no whitespace at all is cut mid-word, never between a
// base character and a mark that combines with it.
func ttsSplitForStream(text string, used int, state ttsBoundary, final bool) (head, tail string, roll bool) {
	const unset = -1
	var (
		lastInWindow = [ttsCutSentence + 1]int{unset, unset, unset, unset}
		lastAnySpace = unset
		limit        = len(text)
		cost         = used
		fits         = true
		softSentence = unset
	)
	for index := 0; index < len(text); {
		r, size := utf8.DecodeRuneInString(text[index:])
		// A cut at index leaves text[:index] on this stream, which must hold
		// something to be worth ending.
		if class := ttsCutClassAt(state, r); class != ttsCutNone && cost > 0 {
			if cost >= ttsStreamTextBudget/2 {
				lastInWindow[class] = index
			}
			if class >= ttsCutSpace {
				lastAnySpace = index
			}
			if class == ttsCutSentence && cost >= ttsStreamSoftBudget {
				softSentence = index
			}
		}
		runeCost := ttsRuneCost(r, size)
		if cost+runeCost > ttsStreamTextBudget {
			limit, fits = index, false
			break
		}
		cost += runeCost
		state = ttsAdvanceBoundary(state, r)
		index += size
	}

	cut := unset
	switch {
	case fits:
		if final || softSentence == unset {
			return text, "", false
		}
		cut = softSentence
	case lastInWindow[ttsCutSentence] != unset:
		cut = lastInWindow[ttsCutSentence]
	case lastInWindow[ttsCutClause] != unset:
		cut = lastInWindow[ttsCutClause]
	case lastInWindow[ttsCutSpace] != unset:
		cut = lastInWindow[ttsCutSpace]
	case lastAnySpace != unset:
		cut = lastAnySpace
	default:
		cut = limit
		for cut > 0 {
			r, _ := utf8.DecodeRuneInString(text[cut:])
			if !ttsJoinsPrevious(r) {
				break
			}
			_, size := utf8.DecodeLastRuneInString(text[:cut])
			cut -= size
		}
		if cut == 0 && used == 0 {
			// One base character carrying more marks than a whole stream
			// holds: nothing sensible is left but the rune boundary.
			cut = limit
		}
		if cut == 0 {
			// The stream is full and this text opens with marks that join
			// the character an earlier append ended it with. They belong to
			// that character, so they stay on this stream, a few units over
			// the budget, which sits well under both vendor caps.
			for cut < len(text) {
				r, size := utf8.DecodeRuneInString(text[cut:])
				if !ttsJoinsPrevious(r) {
					break
				}
				cut += size
			}
		}
	}
	head = text[:cut]
	tail = strings.TrimLeftFunc(text[cut:], unicode.IsSpace)
	if tail == "" {
		return head, "", false
	}
	return head, tail, true
}
