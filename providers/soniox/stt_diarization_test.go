package soniox

import (
	"context"
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
