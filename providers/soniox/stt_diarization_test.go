package soniox

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

// runSTTScript opens a stream for request against a server that reads the
// start frame, sends frames in order, and then waits. It returns the start
// frame and the first want provider events.
func runSTTScript(t *testing.T, request func(serverURL string) runtimepkg.AdapterRequest, frames []map[string]any, want int) (map[string]any, []runtimepkg.ProviderEvent) {
	t.Helper()
	return runSTTScriptUntil(t, request, frames, func(events []runtimepkg.ProviderEvent) bool { return len(events) >= want })
}

// runSTTScriptUntil is runSTTScript for a script whose event count is not
// fixed: it collects until done holds.
func runSTTScriptUntil(t *testing.T, request func(serverURL string) runtimepkg.AdapterRequest, frames []map[string]any, done func([]runtimepkg.ProviderEvent) bool) (map[string]any, []runtimepkg.ProviderEvent) {
	t.Helper()
	starts := make(chan map[string]any, 1)
	server := newSTTServer(t, func(ctx context.Context, conn *websocket.Conn) {
		start, err := readJSONObject(ctx, conn)
		if err != nil {
			t.Errorf("read start request: %v", err)
			return
		}
		starts <- start
		for index, frame := range frames {
			if err := writeJSONFrame(ctx, conn, frame); err != nil {
				t.Errorf("frame %d: %v", index, err)
				return
			}
		}
		waitForPeer(ctx, conn)
	})
	t.Cleanup(server.Close)

	adapter, err := NewSTT(sttTestConfig(server.URL))
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	stream, err := adapter.Open(context.Background(), request(server.URL))
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	t.Cleanup(func() { abortStream(stream) })
	start := mustReceiveObject(t, starts)
	var events []runtimepkg.ProviderEvent
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for !done(events) {
		select {
		case event, ok := <-stream.Events():
			if !ok {
				t.Fatalf("provider events closed after %d events", len(events))
			}
			if event.Err != nil {
				t.Fatalf("unexpected provider error: %v", event.Err)
			}
			events = append(events, event)
		case <-timer.C:
			t.Fatalf("timed out after %d provider events", len(events))
		}
	}
	return start, events
}

// spanView is an event's whole-text span, "-" where a bound is absent.
func spanView(t *testing.T, data json.RawMessage) string {
	t.Helper()
	var decoded struct {
		StartMS *int64 `json:"audio_start_ms"`
		EndMS   *int64 `json:"audio_end_ms"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("decode transcript: %v", err)
	}
	number := func(value *int64) string {
		if value == nil {
			return "-"
		}
		return strconv.FormatInt(*value, 10)
	}
	return "[" + number(decoded.StartMS) + "-" + number(decoded.EndMS) + "]"
}

func diarizedRequest(serverURL string) runtimepkg.AdapterRequest {
	request := sttAdapterRequest(serverURL)
	request.Options.STT = &protocol.SttOptions{Diarization: sttBoolPointer(true)}
	return request
}

// sonioxToken builds one recorded-style Soniox token. speaker "" leaves the
// field off, as Soniox does on a session without diarization.
func sonioxToken(text string, startMS, endMS int64, confidence float64, final bool, speaker string) map[string]any {
	token := map[string]any{"text": text, "start_ms": startMS, "end_ms": endMS, "confidence": confidence, "is_final": final}
	if speaker != "" {
		token["speaker"] = speaker
	}
	return token
}

func sonioxEnd(endMS int64) map[string]any {
	return map[string]any{"text": "<end>", "is_final": true, "end_ms": endMS}
}

func sonioxFrame(tokens ...map[string]any) map[string]any {
	list := make([]any, len(tokens))
	for index, token := range tokens {
		list[index] = token
	}
	return map[string]any{"tokens": list, "final_audio_proc_ms": 0, "total_audio_proc_ms": 0}
}

// A session that never asked for diarization reads exactly the bytes it read
// before speaker labels existed. The want strings are today's output,
// recorded before the adapter learned to read speakers.
func TestSTTUndiarizedSessionKeepsItsExactEventBytes(t *testing.T) {
	t.Parallel()

	_, events := runSTTScript(t, sttAdapterRequest, []map[string]any{
		sonioxFrame(
			sonioxToken("How", 120, 240, 1, false, ""),
			sonioxToken(" are", 240, 360, 0.5, false, ""),
		),
		sonioxFrame(
			sonioxToken("How", 120, 240, 1, true, ""),
			sonioxToken(" are", 240, 360, 0.5, true, ""),
			sonioxToken(" you", 360, 520, 1, false, ""),
			sonioxToken("?", 520, 600, 0.5, false, ""),
		),
		sonioxFrame(
			sonioxToken(" you", 360, 520, 1, true, ""),
			sonioxToken("?", 520, 600, 0.5, true, ""),
			sonioxEnd(640),
		),
	}, 4)

	want := []struct {
		kind protocol.EventType
		data string
	}{
		{protocol.EventTranscriptDelta, `{"audio_end_ms":360,"is_final":false,"provider_request_id":"","speech_final":false,"text":"How are"}`},
		{protocol.EventTranscriptDelta, `{"audio_end_ms":600,"audio_start_ms":120,"is_final":false,"provider_request_id":"","speech_final":false,"text":"How are you?"}`},
		{protocol.EventTranscriptFinal, `{"audio_end_ms":600,"audio_start_ms":120,"confidence":0.75,"is_final":true,"provider_request_id":"","speech_final":true,"text":"How are you?"}`},
		{protocol.EventSpeechEnded, `{"audio_end_ms":640,"reason":"endpoint_detected"}`},
	}
	for index, event := range events {
		if event.Type != want[index].kind || string(event.Data) != want[index].data {
			t.Errorf("event %d = %s %s\nwant      %s %s", index, event.Type, event.Data, want[index].kind, want[index].data)
		}
	}
}

// enable_speaker_diarization is Soniox's start-frame switch
// (soniox.com/docs/stt/concepts/speaker-diarization). It is sent only when
// the caller asked: an explicit false and no ask both leave it off the frame.
func TestSTTDiarizationOptionRidesTheStartFrameOnlyWhenAsked(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name    string
		options *protocol.SttOptions
		want    bool
	}{
		{name: "asked", options: &protocol.SttOptions{Diarization: sttBoolPointer(true)}, want: true},
		{name: "explicit false", options: &protocol.SttOptions{Diarization: sttBoolPointer(false)}},
		{name: "not asked"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			start, _ := runSTTScript(t, func(serverURL string) runtimepkg.AdapterRequest {
				request := sttAdapterRequest(serverURL)
				request.Options.STT = testCase.options
				return request
			}, nil, 0)
			value, present := start["enable_speaker_diarization"]
			if testCase.want && value != true {
				t.Fatalf("start frame enable_speaker_diarization = %v, want true: %v", value, start)
			}
			if !testCase.want && present {
				t.Fatalf("start frame carries enable_speaker_diarization = %v without an ask", value)
			}
		})
	}
}

// speakerView renders a diarized transcript event as
// "text | speaker | word@speaker[start-end] ...", "-" marking an absent field,
// so a whole attribution reads on one line. Speaker runs render the same way,
// as "<run text>@speaker[start-end]" with the text cut out of the event's text
// at the run's character offsets.
func speakerView(t *testing.T, data json.RawMessage) string {
	t.Helper()
	var decoded struct {
		Text    string  `json:"text"`
		Speaker *string `json:"speaker"`
		Words   []struct {
			Text    string  `json:"text"`
			StartMS *int64  `json:"start_ms"`
			EndMS   *int64  `json:"end_ms"`
			Speaker *string `json:"speaker"`
		} `json:"words"`
		Runs []struct {
			TextStart int     `json:"text_start"`
			TextEnd   int     `json:"text_end"`
			StartMS   *int64  `json:"start_ms"`
			EndMS     *int64  `json:"end_ms"`
			Speaker   *string `json:"speaker"`
		} `json:"speaker_runs"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("decode transcript: %v", err)
	}
	optional := func(value *string) string {
		if value == nil {
			return "-"
		}
		return *value
	}
	number := func(value *int64) string {
		if value == nil {
			return "-"
		}
		return strconv.FormatInt(*value, 10)
	}
	parts := []string{decoded.Text, optional(decoded.Speaker)}
	for _, word := range decoded.Words {
		parts = append(parts, word.Text+"@"+optional(word.Speaker)+"["+number(word.StartMS)+"-"+number(word.EndMS)+"]")
	}
	characters := []rune(decoded.Text)
	for _, run := range decoded.Runs {
		if run.TextStart < 0 || run.TextEnd > len(characters) || run.TextStart > run.TextEnd {
			t.Fatalf("run offsets [%d,%d) outside %q", run.TextStart, run.TextEnd, decoded.Text)
		}
		parts = append(parts, "<"+string(characters[run.TextStart:run.TextEnd])+">@"+optional(run.Speaker)+"["+number(run.StartMS)+"-"+number(run.EndMS)+"]")
	}
	return strings.Join(parts, " | ")
}

// Two people alternating. The first segment is one speaker's, so its final
// names that speaker; the second changes speaker without an endpoint between
// them, so each word carries its own label and the final names no one. Word
// pieces ("fan" "tastic", "I" "'m") join into the words they spell.
func TestSTTDiarizedFinalsCarryTheSpeakerOfEachWord(t *testing.T) {
	t.Parallel()

	_, events := runSTTScript(t, diarizedRequest, []map[string]any{
		sonioxFrame(
			sonioxToken("How", 120, 240, 1, true, "1"),
			sonioxToken(" are", 240, 360, 1, true, "1"),
			sonioxToken(" you", 360, 520, 1, true, "1"),
			sonioxToken("?", 520, 600, 1, true, "1"),
			sonioxEnd(640),
		),
		sonioxFrame(
			sonioxToken(" I", 900, 980, 1, true, "2"),
			sonioxToken("'m", 980, 1040, 1, true, "2"),
			sonioxToken(" fan", 1040, 1200, 1, true, "2"),
			sonioxToken("tastic", 1200, 1400, 1, true, "2"),
			sonioxToken(".", 1400, 1440, 1, true, "2"),
			sonioxToken(" And", 1700, 1820, 1, true, "1"),
			sonioxToken(" you", 1820, 1960, 1, true, "1"),
			sonioxToken("?", 1960, 2000, 1, true, "1"),
			sonioxEnd(2040),
		),
	}, 4)

	if got := strings.Join(eventTypeNames(events), ","); got != "transcript.final,speech.ended,transcript.final,speech.ended" {
		t.Fatalf("event types = %s", got)
	}
	for index, want := range map[int]string{
		0: "How are you? | 1 | How@1[120-240] | are@1[240-360] | you?@1[360-600]",
		2: "I'm fantastic. And you? | - | I'm@2[900-1040] | fantastic.@2[1040-1440] | And@1[1700-1820] | you?@1[1820-2000]",
	} {
		if got := speakerView(t, events[index].Data); got != want {
			t.Errorf("final %d:\n got %s\nwant %s", index, got, want)
		}
	}
}

// Soniox labels non-final tokens too, so a delta attributes the hypothesis
// as soon as it is heard. The speaker changes inside the non-final tail of
// the first frame; in the second, the first speaker's words are final and
// the second speaker's are still provisional, and the delta covers both. A
// delta carries one entry per speaker run, not per word.
func TestSTTDiarizedDeltaAttributesASpeakerChangeInsideTheHypothesis(t *testing.T) {
	t.Parallel()

	_, events := runSTTScript(t, diarizedRequest, []map[string]any{
		sonioxFrame(
			sonioxToken("Hello", 100, 300, 1, false, "1"),
			sonioxToken(" there", 300, 520, 1, false, "1"),
			sonioxToken(".", 520, 560, 1, false, "1"),
			sonioxToken(" Hi", 900, 1000, 1, false, "2"),
		),
		sonioxFrame(
			sonioxToken("Hello", 100, 300, 1, true, "1"),
			sonioxToken(" there", 300, 520, 1, true, "1"),
			sonioxToken(".", 520, 560, 1, true, "1"),
			sonioxToken(" Hi", 900, 1000, 1, false, "2"),
			sonioxToken("!", 1000, 1040, 1, false, "2"),
			sonioxToken(" How", 1100, 1200, 1, false, "2"),
		),
		sonioxFrame(
			sonioxToken(" Hi", 900, 1000, 1, false, "2"),
			sonioxToken("!", 1000, 1040, 1, false, "2"),
		),
	}, 3)

	for index, want := range []string{
		"Hello there. Hi | - | <Hello there.>@1[100-560] | <Hi>@2[900-1000]",
		"Hello there. Hi! How | - | <Hello there.>@1[100-560] | <Hi! How>@2[900-1200]",
		"Hello there. Hi! | - | <Hello there.>@1[100-560] | <Hi!>@2[900-1040]",
	} {
		if events[index].Type != protocol.EventTranscriptDelta {
			t.Fatalf("event %d type = %s", index, events[index].Type)
		}
		if got := speakerView(t, events[index].Data); got != want {
			t.Errorf("delta %d:\n got %s\nwant %s", index, got, want)
		}
	}
}

// A word Soniox left without a label could be anyone's: it carries no
// speaker, and the event names no one even when every labelled word agrees.
func TestSTTDiarizedUnlabelledWordKeepsTheEventUnattributed(t *testing.T) {
	t.Parallel()

	_, events := runSTTScript(t, diarizedRequest, []map[string]any{
		sonioxFrame(
			sonioxToken("Yes", 100, 300, 1, true, "1"),
			sonioxToken(" um", 300, 400, 1, true, ""),
			sonioxEnd(440),
		),
	}, 1)
	if got, want := speakerView(t, events[0].Data), "Yes um | - | Yes@1[100-300] | um@-[300-400]"; got != want {
		t.Fatalf("final:\n got %s\nwant %s", got, want)
	}
}

// A word, and a run, is bounded by its own first and last pieces. When those
// pieces carry no time, the bound is unknown and stays absent, rather than
// borrowing an inner piece's time and looking fully timed.
func TestSTTDiarizedBoundsComeOnlyFromTheOuterPieces(t *testing.T) {
	t.Parallel()

	pieces := func(final bool) []map[string]any {
		return []map[string]any{
			{"text": "这", "end_ms": 320, "is_final": final, "speaker": "1"},
			{"text": "很好", "start_ms": 320, "end_ms": 360, "is_final": final, "speaker": "1"},
			{"text": "。", "start_ms": 360, "is_final": final, "speaker": "1"},
		}
	}
	_, events := runSTTScript(t, diarizedRequest, []map[string]any{
		sonioxFrame(pieces(false)...),
		sonioxFrame(append(pieces(true), sonioxEnd(400))...),
	}, 2)
	if got, want := speakerView(t, events[0].Data), "这很好。 | 1 | <这很好。>@1[---]"; got != want {
		t.Errorf("delta:\n got %s\nwant %s", got, want)
	}
	if got, want := speakerView(t, events[1].Data), "这很好。 | 1 | 这很好。@1[---]"; got != want {
		t.Errorf("final:\n got %s\nwant %s", got, want)
	}
	// The whole text has the same outer pieces, so its span is unknown too,
	// not the inner piece's 320-360.
	for index, event := range events {
		if got := spanView(t, event.Data); got != "[---]" {
			t.Errorf("event %d (%s) span = %s, want [---]", index, event.Type, got)
		}
	}
}

// A final's span covers its whole text, so it is reported only when its
// first piece has a start and its last piece an end. Here B started at
// 300 ms and its end is unknown: "A B" was not said within 100-200 ms. Each
// speaker's words keep the bounds they have.
func TestSTTDiarizedFinalSpanNeedsBothOuterBounds(t *testing.T) {
	t.Parallel()

	pieces := func(final bool) []map[string]any {
		return []map[string]any{
			sonioxToken("A", 100, 200, 1, final, "1"),
			{"text": " B", "start_ms": 300, "confidence": 1, "is_final": final, "speaker": "2"},
		}
	}
	_, events := runSTTScript(t, diarizedRequest, []map[string]any{
		sonioxFrame(pieces(false)...),
		sonioxFrame(append(pieces(true), sonioxEnd(400))...),
	}, 3)
	if got, want := speakerView(t, events[0].Data)+" "+spanView(t, events[0].Data), "A B | - | <A>@1[100-200] | <B>@2[300--] [---]"; got != want {
		t.Errorf("delta:\n got %s\nwant %s", got, want)
	}
	if got, want := speakerView(t, events[1].Data)+" "+spanView(t, events[1].Data), "A B | - | A@1[100-200] | B@2[300--] [---]"; got != want {
		t.Errorf("final:\n got %s\nwant %s", got, want)
	}
}

// Without an endpoint the hypothesis grows for as long as people talk, and
// every delta repeats it. Labels must add a bounded share to that, not a
// per-word list: 800 words, one new word per update, speakers alternating
// every 100 words, stay within 1.5 times the bytes of the same stream
// without labels.
func TestSTTDiarizedDeltasStayProportionalWithoutAnEndpoint(t *testing.T) {
	t.Parallel()

	const words = 800
	frames := func(labelled bool) []map[string]any {
		speaker := func(index int) string {
			if !labelled {
				return ""
			}
			return strconv.Itoa(index/100%2 + 1)
		}
		script := make([]map[string]any, 0, words)
		for index := range words {
			start := int64(index * 300)
			tokens := []map[string]any{sonioxToken(" word"+strconv.Itoa(index%50), start, start+250, 1, false, speaker(index))}
			if index > 0 {
				previous := int64((index - 1) * 300)
				tokens = append([]map[string]any{sonioxToken(" word"+strconv.Itoa((index-1)%50), previous, previous+250, 1, true, speaker(index-1))}, tokens...)
			}
			script = append(script, sonioxFrame(tokens...))
		}
		return script
	}
	total := func(request func(string) runtimepkg.AdapterRequest, labelled bool) int {
		_, events := runSTTScriptUntil(t, request, frames(labelled), func(events []runtimepkg.ProviderEvent) bool {
			return countEvents(events, protocol.EventTranscriptDelta) >= words
		})
		sum := 0
		for _, event := range events {
			sum += len(event.Data)
		}
		return sum
	}
	plain := total(sttAdapterRequest, false)
	labelled := total(diarizedRequest, true)
	t.Logf("labelled events %d bytes, unlabelled %d, ratio %.2f", labelled, plain, float64(labelled)/float64(plain))
	if float64(labelled) > 1.5*float64(plain) {
		t.Fatalf("labelled events total %d bytes, %.2f times the %d unlabelled", labelled, float64(labelled)/float64(plain), plain)
	}
}

func countEvents(events []runtimepkg.ProviderEvent, kind protocol.EventType) int {
	count := 0
	for _, event := range events {
		if event.Type == kind {
			count++
		}
	}
	return count
}

// Every word reaches exactly one final, in order, whatever deltas showed it
// before: words first heard as provisional tail, segments closed by the
// endpointer, and a speaker change inside a segment.
func TestSTTDiarizedEveryWordLandsInExactlyOneFinal(t *testing.T) {
	t.Parallel()

	var script []map[string]any
	var spoken []string
	for index := range 30 {
		word := "w" + strconv.Itoa(index)
		spoken = append(spoken, word)
		start := int64(index * 300)
		speaker := strconv.Itoa(index/7%2 + 1)
		script = append(script, sonioxFrame(sonioxToken(" "+word, start, start+250, 1, false, speaker)))
		frame := sonioxFrame(sonioxToken(" "+word, start, start+250, 1, true, speaker))
		if index%10 == 9 {
			frame = sonioxFrame(sonioxToken(" "+word, start, start+250, 1, true, speaker), sonioxEnd(start+280))
		}
		script = append(script, frame)
	}
	// 30 tail deltas, 27 deltas re-sending the segment after a word is
	// final (none after the 3 frames carrying <end>), 3 finals, 3
	// speech.ended.
	_, events := runSTTScript(t, diarizedRequest, script, 63)

	var finals []string
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
			finals = append(finals, word.Text)
		}
	}
	if got, want := strings.Join(finals, " "), strings.Join(spoken, " "); got != want {
		t.Fatalf("final words:\n got %s\nwant %s", got, want)
	}
}
