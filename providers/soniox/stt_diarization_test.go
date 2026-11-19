package soniox

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

// runSTTScript opens a stream for request against a server that reads the
// start frame, sends frames in order, and then waits. It returns the start
// frame and the first want provider events.
func runSTTScript(t *testing.T, request func(serverURL string) runtimepkg.AdapterRequest, frames []map[string]any, want int) (map[string]any, []runtimepkg.ProviderEvent) {
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
	return start, collectEvents(t, stream.Events(), want)
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
// so a whole attribution reads on one line.
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
// the second speaker's are still provisional, and the delta covers both.
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
		"Hello there. Hi | - | Hello@1[100-300] | there.@1[300-560] | Hi@2[900-1000]",
		"Hello there. Hi! How | - | Hello@1[100-300] | there.@1[300-560] | Hi!@2[900-1040] | How@2[1100-1200]",
		"Hello there. Hi! | - | Hello@1[100-300] | there.@1[300-560] | Hi!@2[900-1040]",
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
