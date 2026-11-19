package soniox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

// longStretch is a diarized talk with no endpoint, 300 ms a word, the
// speaker changing on every word. Each word comes in two pieces, " wN" and
// "x", final in consecutive frames, so every frame ends inside a word: a cut
// that split one would show "wN" and "x" as two words. With sentences, every
// tenth word is followed by a "." piece. It returns the frames and the words
// as finals should spell them.
func longStretch(words int, sentences bool) ([]map[string]any, []string) {
	var frames []map[string]any
	var spelled []string
	tail := func(index int, final bool) []map[string]any {
		start := int64(index * 300)
		speaker := strconv.Itoa(index%2 + 1)
		tokens := []map[string]any{sonioxToken("x", start+150, start+250, 1, final, speaker)}
		if sentences && index%10 == 9 && final {
			tokens = append(tokens, sonioxToken(".", start+250, start+260, 1, final, speaker))
		}
		return tokens
	}
	for index := range words {
		start := int64(index * 300)
		var tokens []map[string]any
		if index > 0 {
			tokens = append(tokens, tail(index-1, true)...)
		}
		tokens = append(tokens, sonioxToken(" w"+strconv.Itoa(index), start, start+150, 1, true, strconv.Itoa(index%2+1)))
		tokens = append(tokens, tail(index, false)...)
		frames = append(frames, sonioxFrame(tokens...))
		word := "w" + strconv.Itoa(index) + "x"
		if sentences && index%10 == 9 {
			word += "."
		}
		spelled = append(spelled, word)
	}
	last := words - 1
	frames = append(frames, sonioxFrame(append(tail(last, true), sonioxEnd(int64(last*300+280)))...))
	return frames, spelled
}

func finalWords(t *testing.T, events []runtimepkg.ProviderEvent) []string {
	t.Helper()
	var words []string
	for _, event := range events {
		if event.Type != protocol.EventTranscriptFinal {
			continue
		}
		var final struct {
			Words []struct {
				Text string `json:"text"`
			} `json:"words"`
		}
		if err := json.Unmarshal(event.Data, &final); err != nil {
			t.Fatal(err)
		}
		for _, word := range final.Words {
			words = append(words, word.Text)
		}
	}
	return words
}

func untilSpeechEnded(events []runtimepkg.ProviderEvent) bool {
	return len(events) > 0 && events[len(events)-1].Type == protocol.EventSpeechEnded
}

// Without an endpoint, a diarized segment's finalized part is cut once it
// spans more than 20 s: the prefix up to the last sentence end in its last
// quarter goes out once as a final that does not end the utterance, and later
// deltas carry only the rest.
func TestSTTDiarizedLongStretchIsCutAtASentenceEnd(t *testing.T) {
	t.Parallel()

	frames, _ := longStretch(80, true)
	_, events := runSTTScriptUntil(t, diarizedRequest, frames, untilSpeechEnded)

	var finals []map[string]any
	for _, event := range events {
		if event.Type != protocol.EventTranscriptFinal {
			continue
		}
		var data map[string]any
		if err := json.Unmarshal(event.Data, &data); err != nil {
			t.Fatal(err)
		}
		finals = append(finals, data)
	}
	if len(finals) != 2 {
		t.Fatalf("got %d finals, want the cut and the endpoint's: %v", len(finals), finals)
	}
	cut, rest := finals[0], finals[1]
	if text := cut["text"].(string); !strings.HasPrefix(text, "w0x w1x") || !strings.HasSuffix(text, "w59x.") {
		t.Errorf("cut final text = %q, want w0x ... w59x.", text)
	}
	if cut["speech_final"] != false || cut["is_final"] != true {
		t.Errorf("cut final: is_final %v, speech_final %v; want true, false", cut["is_final"], cut["speech_final"])
	}
	// The cut says so explicitly, so a consumer answering commits does not
	// take it for the segment's end; the endpoint's final does not.
	if cut["interim"] != true {
		t.Errorf("cut final: interim %v, want true", cut["interim"])
	}
	if value, present := rest["interim"]; present {
		t.Errorf("endpoint final: interim %v, want no interim key", value)
	}
	if start, end := cut["audio_start_ms"], cut["audio_end_ms"]; start != float64(0) || end != float64(59*300+260) {
		t.Errorf("cut final span = %v-%v, want 0-%d", start, end, 59*300+260)
	}
	if text := rest["text"].(string); !strings.HasPrefix(text, "w60x ") || rest["speech_final"] != true {
		t.Errorf("endpoint final = %q (speech_final %v), want it to start at w60x and end the utterance", text, rest["speech_final"])
	}
	if start := rest["audio_start_ms"]; start != float64(60*300) {
		t.Errorf("endpoint final starts at %v, want %d", start, 60*300)
	}
	// The deltas after the cut carry only the rest; speakerView checks each
	// run's offsets against the delta's own text.
	afterCut := 0
	for _, event := range events {
		if event.Type == protocol.EventTranscriptFinal {
			afterCut++
			continue
		}
		if event.Type != protocol.EventTranscriptDelta || afterCut != 1 {
			continue
		}
		view := speakerView(t, event.Data)
		if !strings.HasPrefix(view, "w60x") || strings.Contains(view, "w59") {
			t.Fatalf("a delta after the cut does not start at w60x: %s", view)
		}
		if !strings.Contains(view, "<w60x>@1[18000-18250]") {
			t.Fatalf("the rest's first run lost its text or times: %s", view)
		}
	}
	if afterCut != 2 {
		t.Fatalf("saw %d finals, want 2", afterCut)
	}
}

// A cut leaves the last finalized piece open. When that piece is only
// whitespace, the segment has nothing left to say when it ends, yet it has
// already sent an interim final: it still gets its closing final, empty and
// not interim, so a consumer that holds interim text until its segment
// closes is not left waiting. Answering a commit, that closing final is the
// one answer: no second empty final follows it.
func TestSTTCutSegmentWithNothingLeftStillCloses(t *testing.T) {
	t.Parallel()

	stretch := func() map[string]any {
		return sonioxFrame(
			sonioxToken("Long", 0, 400, 1, true, "1"),
			sonioxToken(" talk.", 400, 25_000, 1, true, "1"),
			sonioxToken(" ", 25_000, 25_100, 1, true, "1"),
		)
	}
	check := func(t *testing.T, events []runtimepkg.ProviderEvent, wantTypes string) {
		t.Helper()
		if got := strings.Join(eventTypeNames(events), ","); got != wantTypes {
			t.Fatalf("event types = %s, want %s", got, wantTypes)
		}
		var cut, closing map[string]any
		if err := json.Unmarshal(events[0].Data, &cut); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(events[1].Data, &closing); err != nil {
			t.Fatal(err)
		}
		if cut["text"] != "Long talk." || cut["interim"] != true || cut["speech_final"] != false {
			t.Errorf("cut final = %v, want interim \"Long talk.\"", cut)
		}
		if value, present := closing["interim"]; present || closing["text"] != "" || closing["is_final"] != true || closing["speech_final"] != true {
			t.Errorf("closing final = %v (interim %v), want empty text, is_final and speech_final true, no interim", closing, value)
		}
	}

	t.Run("endpoint", func(t *testing.T) {
		t.Parallel()
		_, events := runSTTScriptUntil(t, diarizedRequest, []map[string]any{stretch(), sonioxFrame(sonioxEnd(25_200))}, untilSpeechEnded)
		check(t, events, "transcript.final,transcript.final,speech.ended")
	})

	t.Run("commit", func(t *testing.T) {
		t.Parallel()
		server := newSTTServer(t, func(ctx context.Context, conn *websocket.Conn) {
			if _, err := readJSONObject(ctx, conn); err != nil {
				t.Errorf("read start request: %v", err)
				return
			}
			if err := writeJSONFrame(ctx, conn, stretch()); err != nil {
				t.Errorf("stretch frame: %v", err)
				return
			}
			if _, err := readJSONObject(ctx, conn); err != nil {
				t.Errorf("read finalize: %v", err)
				return
			}
			if err := writeJSONFrame(ctx, conn, sonioxFrame(map[string]any{"text": "<fin>", "is_final": true})); err != nil {
				t.Errorf("fin frame: %v", err)
				return
			}
			if err := writeJSONFrame(ctx, conn, map[string]any{"tokens": []any{}, "finished": true, "total_audio_proc_ms": 25_200}); err != nil {
				t.Errorf("finished frame: %v", err)
				return
			}
			waitForPeer(ctx, conn)
		})
		defer server.Close()

		adapter, err := NewSTT(sttTestConfig(server.URL))
		if err != nil {
			t.Fatalf("new adapter: %v", err)
		}
		stream, err := adapter.Open(context.Background(), diarizedRequest(server.URL))
		if err != nil {
			t.Fatalf("open stream: %v", err)
		}
		defer abortStream(stream)
		events := collectEvents(t, stream.Events(), 1)
		if err := stream.CommitAudio(context.Background()); err != nil {
			t.Fatalf("commit audio: %v", err)
		}
		events = append(events, collectEvents(t, stream.Events(), 2)...)
		check(t, events, "transcript.final,transcript.final,usage.observed")
	})
}

// However long people talk without an endpoint, a diarized session's events
// stay bounded and their total grows with the talk, not with its square:
// twice the words, about twice the bytes.
func TestSTTDiarizedLongStretchStaysLinear(t *testing.T) {
	t.Parallel()

	measure := func(words int) (total, largest int) {
		frames, _ := longStretch(words, false)
		_, events := runSTTScriptUntil(t, diarizedRequest, frames, untilSpeechEnded)
		for _, event := range events {
			total += len(event.Data)
			largest = max(largest, len(event.Data))
		}
		return total, largest
	}
	short, shortLargest := measure(800)
	long, longLargest := measure(1600)
	t.Logf("800 words: %d bytes, largest event %d; 1600 words: %d bytes, largest %d", short, shortLargest, long, longLargest)
	if ratio := float64(long) / float64(short); ratio > 2.3 {
		t.Errorf("twice the words cost %.2f times the bytes, want about 2", ratio)
	}
	if longLargest > 16_000 {
		t.Errorf("largest event %d bytes, want at most 16000 whatever the length", longLargest)
	}
}

// Cutting never splits a word, drops one or repeats one: every word of a
// long diarized stretch reaches exactly one final, in order, including words
// said in two pieces.
func TestSTTDiarizedLongStretchKeepsEveryWordOnce(t *testing.T) {
	t.Parallel()

	for _, sentences := range []bool{false, true} {
		frames, spelled := longStretch(300, sentences)
		_, events := runSTTScriptUntil(t, diarizedRequest, frames, untilSpeechEnded)
		if cuts := countEvents(events, protocol.EventTranscriptFinal); cuts < 3 {
			t.Fatalf("sentences %v: %d finals, want the stretch cut several times", sentences, cuts)
		}
		if got, want := strings.Join(finalWords(t, events), " "), strings.Join(spelled, " "); got != want {
			t.Fatalf("sentences %v: final words:\n got %s\nwant %s", sentences, got, want)
		}
	}
}

// A session without diarization keeps today's behaviour: one growing segment
// and no cut.
func TestSTTUndiarizedLongStretchIsNotCut(t *testing.T) {
	t.Parallel()

	frames, _ := longStretch(120, true)
	for _, frame := range frames {
		for _, token := range frame["tokens"].([]any) {
			delete(token.(map[string]any), "speaker")
		}
	}
	_, events := runSTTScriptUntil(t, sttAdapterRequest, frames, untilSpeechEnded)
	if finals := countEvents(events, protocol.EventTranscriptFinal); finals != 1 {
		t.Fatalf("got %d finals, want only the endpoint's", finals)
	}
}

// translatingRequest is a diarized session that also asked Soniox for its
// own one-way translation.
func translatingRequest(serverURL string) runtimepkg.AdapterRequest {
	request := diarizedRequest(serverURL)
	request.Options.STT.Translation = &protocol.SttTranslation{TargetLanguage: "es"}
	return request
}

// withTranslation turns a longStretch into a translating session's frames:
// every original piece is marked as such, and each word's first piece is
// followed by its final translated token " tN", as Soniox interleaves them.
func withTranslation(frames []map[string]any) []map[string]any {
	var out []map[string]any
	for _, frame := range frames {
		var tokens []map[string]any
		for _, item := range frame["tokens"].([]any) {
			token := item.(map[string]any)
			text := token["text"].(string)
			if text == "<end>" {
				tokens = append(tokens, token)
				continue
			}
			original := map[string]any{"translation_status": "original", "language": "en"}
			for key, value := range token {
				original[key] = value
			}
			tokens = append(tokens, original)
			if word, isWord := strings.CutPrefix(text, " w"); isWord && token["is_final"] == true {
				tokens = append(tokens, map[string]any{"text": " t" + word, "is_final": true, "language": "es", "source_language": "en", "translation_status": "translation"})
			}
		}
		out = append(out, sonioxFrame(tokens...))
	}
	return out
}

// eventDigest is the SHA-256 of every event's type and data bytes, in order.
func eventDigest(events []runtimepkg.ProviderEvent) string {
	hash := sha256.New()
	for _, event := range events {
		hash.Write([]byte(event.Type))
		hash.Write([]byte{'\n'})
		hash.Write(event.Data)
		hash.Write([]byte{'\n'})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// With Soniox's own translation on, a forced cut would hand the prefix the
// translation of words still in the tail, so a diarized session that
// translates is never cut: one final at the endpoint, and every event byte
// for byte what it was before cutting existed. The digest was recorded on
// the adapter as it stood before the cut (e8ee7d3).
func TestSTTDiarizedTranslatingLongStretchIsNotCut(t *testing.T) {
	t.Parallel()

	frames, _ := longStretch(120, true)
	_, events := runSTTScriptUntil(t, translatingRequest, withTranslation(frames), untilSpeechEnded)
	if finals := countEvents(events, protocol.EventTranscriptFinal); finals != 1 {
		t.Fatalf("got %d finals, want only the endpoint's", finals)
	}
	translated := make([]string, 120)
	for index := range translated {
		translated[index] = "t" + strconv.Itoa(index)
	}
	final := decodeTranslated(t, events[len(events)-2].Data)
	if got, want := translationValue(final), strings.Join(translated, " "); got != want {
		t.Errorf("final translation = %q, want every translated word: %q", got, want)
	}
	const before = "3f25a6d4fae9f40f3eabf97bd8a723f18a8979e0326ecb537d4f6183701f6f9d"
	if got := eventDigest(events); got != before {
		t.Errorf("event bytes changed: digest %s, want %s", got, before)
	}
}

// unspacedPieces is a script written without spaces or punctuation, the way
// Chinese and Japanese are: no word or sentence boundary anywhere. Some
// pieces start or end a character made of several code points ("e" +
// U+0301, an emoji joined by U+200D, a heart with variation selector U+FE0F)
// that no cut may part.
var unspacedPieces = []string{"今天", "我们", "去了", "市场", "e", "́", "👩", "‍", "💻", "明天", "还要", "❤", "️", "继续", "旅行"}

// longUnspacedStretch is a diarized talk in unspacedPieces with no endpoint,
// msPerPiece a piece, perFrame pieces finalized a frame (pieceStretch). It
// returns the frames and the whole text as the finals should spell it.
func longUnspacedStretch(pieces, perFrame int, msPerPiece int64) ([]map[string]any, string) {
	list := make([]string, pieces)
	for index := range list {
		list[index] = unspacedPieces[index%len(unspacedPieces)]
	}
	return pieceStretch(list, perFrame, msPerPiece), strings.Join(list, "")
}

// pieceStretch is a diarized talk of the given pieces with no endpoint,
// msPerPiece a piece, the speaker changing every 100 pieces. Each frame
// finalizes perFrame pieces and shows the next one as provisional tail; the
// last frame ends the segment.
func pieceStretch(pieces []string, perFrame int, msPerPiece int64) []map[string]any {
	piece := func(index int, final bool) map[string]any {
		start := int64(index) * msPerPiece
		return sonioxToken(pieces[index], start, start+msPerPiece, 1, final, strconv.Itoa(index/100%2+1))
	}
	var frames []map[string]any
	for first := 0; first < len(pieces); first += perFrame {
		var tokens []map[string]any
		for index := first; index < min(first+perFrame, len(pieces)); index++ {
			tokens = append(tokens, piece(index, true))
		}
		if next := first + perFrame; next < len(pieces) {
			tokens = append(tokens, piece(next, false))
		} else {
			tokens = append(tokens, sonioxEnd(int64(len(pieces))*msPerPiece+20))
		}
		frames = append(frames, sonioxFrame(tokens...))
	}
	return frames
}

// With no word or sentence boundary in sight, the stretch is still cut and
// stays linear: twice the talk, about twice the bytes, no event growing with
// it. Pieces 2 ms apart pass 2,000 characters long before 20 s.
func TestSTTDiarizedUnspacedStretchStaysLinear(t *testing.T) {
	t.Parallel()

	measure := func(pieces int) (total, largest int) {
		frames, _ := longUnspacedStretch(pieces, 3, 2)
		_, events := runSTTScriptUntil(t, diarizedRequest, frames, untilSpeechEnded)
		for _, event := range events {
			total += len(event.Data)
			largest = max(largest, len(event.Data))
		}
		return total, largest
	}
	short, shortLargest := measure(3000)
	long, longLargest := measure(6000)
	t.Logf("3000 pieces: %d bytes, largest event %d; 6000 pieces: %d bytes, largest %d", short, shortLargest, long, longLargest)
	if ratio := float64(long) / float64(short); ratio > 2.3 {
		t.Errorf("twice the talk cost %.2f times the bytes, want about 2", ratio)
	}
	if longLargest > 16_000 {
		t.Errorf("largest event %d bytes, want at most 16000 whatever the length", longLargest)
	}
}

// Cutting text with no boundary keeps every character in exactly one final,
// in order, and never parts a character, whether the 2,000
// characters or the 20 s bound passes first.
func TestSTTDiarizedUnspacedStretchKeepsEveryCharacterOnce(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name       string
		msPerPiece int64
	}{
		{name: "characters first", msPerPiece: 2},
		{name: "time first", msPerPiece: 150},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			frames, spelled := longUnspacedStretch(3000, 1, testCase.msPerPiece)
			_, events := runSTTScriptUntil(t, diarizedRequest, frames, untilSpeechEnded)
			if finals := checkFinalsCover(t, events, spelled); finals < 3 {
				t.Fatalf("%d finals, want the stretch cut several times", finals)
			}
		})
	}
}

// Where no word or sentence boundary exists, the cut falls at the last
// boundary between finalized tokens within the first 2,000 characters that
// does not part a character, and never after the last finalized
// token.
func TestSTTCutPointWithoutBoundary(t *testing.T) {
	t.Parallel()

	tokens := func(head int, tail ...string) []sttToken {
		list := make([]sttToken, 0, head+len(tail))
		for range head {
			list = append(list, sttToken{Text: "中"})
		}
		for _, text := range tail {
			list = append(list, sttToken{Text: text})
		}
		return list
	}
	for _, testCase := range []struct {
		name   string
		tokens []sttToken
		want   int
	}{
		{name: "at the bound", tokens: tokens(2001), want: 2000},
		{name: "keeps the last finalized token", tokens: tokens(30), want: 29},
		{name: "combining mark stays with its base", tokens: tokens(1999, "e", "́"), want: 1999},
		{name: "joined emoji stays whole", tokens: tokens(1998, "👩", "‍", "💻"), want: 1998},
		{name: "variation selector stays", tokens: tokens(1999, "❤", "️"), want: 1999},
		{name: "multi-character tokens", tokens: tokens(0, strings.Repeat("中", 1500), strings.Repeat("中", 400), strings.Repeat("中", 200)), want: 2},
	} {
		if got := sttCutPoint(testCase.tokens); got != testCase.want {
			t.Errorf("%s: cut at %d, want %d", testCase.name, got, testCase.want)
		}
	}
}
