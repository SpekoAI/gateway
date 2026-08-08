package relayapi_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/SpekoAI/gateway/relayapi"
)

func TestTTSWebSocketFixturesRoundTrip(t *testing.T) {
	t.Parallel()

	var frames []json.RawMessage
	if err := json.Unmarshal(readFixture(t, "ws-tts-messages.json"), &frames); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}

	seen := make(map[string]bool)
	for i, frame := range frames {
		name := fmt.Sprintf("ws-tts-messages.json[%d]", i)
		switch messageType(t, name, frame) {
		case string(relayapi.TTSControlSessionConfigure):
			assertGoldenValue[relayapi.TTSSessionConfigure](t, name, frame)
		case string(relayapi.TTSControlInputAppend):
			assertGoldenValue[relayapi.TTSInputAppend](t, name, frame)
		case string(relayapi.TTSControlInputCommit):
			assertGoldenValue[relayapi.TTSInputCommit](t, name, frame)
		case string(relayapi.TTSControlInputCancel):
			assertGoldenValue[relayapi.TTSInputCancel](t, name, frame)
		case string(relayapi.TTSControlSessionClose):
			assertGoldenValue[relayapi.TTSSessionClose](t, name, frame)
		case string(relayapi.TTSEventSessionReady):
			assertGoldenValue[relayapi.TTSSessionReady](t, name, frame)
		case string(relayapi.TTSEventUtteranceStarted):
			assertGoldenValue[relayapi.TTSUtteranceStarted](t, name, frame)
		case string(relayapi.TTSEventUtteranceDone):
			assertGoldenValue[relayapi.TTSUtteranceDone](t, name, frame)
		case string(relayapi.TTSEventUsageUpdated):
			assertGoldenValue[relayapi.TTSUsageUpdated](t, name, frame)
		case string(relayapi.TTSEventSessionClosed):
			assertGoldenValue[relayapi.TTSSessionClosed](t, name, frame)
		case relayapi.ErrorEventType:
			assertGoldenValue[relayapi.ErrorEvent](t, name, frame)
		default:
			t.Fatalf("%s: unknown message type", name)
		}
		seen[messageType(t, name, frame)] = true
	}

	for _, want := range []string{
		string(relayapi.TTSControlSessionConfigure),
		string(relayapi.TTSControlInputAppend),
		string(relayapi.TTSControlInputCommit),
		string(relayapi.TTSControlInputCancel),
		string(relayapi.TTSControlSessionClose),
		string(relayapi.TTSEventSessionReady),
		string(relayapi.TTSEventUtteranceStarted),
		string(relayapi.TTSEventUtteranceDone),
		string(relayapi.TTSEventUsageUpdated),
		string(relayapi.TTSEventSessionClosed),
		relayapi.ErrorEventType,
	} {
		if !seen[want] {
			t.Fatalf("fixture must cover message type %q", want)
		}
	}
}

func TestSpeechRequestValidation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		mutate func(r *relayapi.SpeechRequest)
		want   string
	}{
		{"empty input", func(r *relayapi.SpeechRequest) { r.Input = "" }, "input: required"},
		{"unknown encoding", func(r *relayapi.SpeechRequest) { r.Audio.Encoding = "mp3" }, "encoding: unsupported value"},
		{"high sample rate", func(r *relayapi.SpeechRequest) { r.Audio.SampleRateHz = 200_000 }, "sample_rate_hz"},
		{"zero channels", func(r *relayapi.SpeechRequest) { r.Audio.Channels = 0 }, "channels"},
		{"explicit routing without model", func(r *relayapi.SpeechRequest) { r.Routing.Model = "" }, "explicit mode requires provider and model"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var request relayapi.SpeechRequest
			decodeFixture(t, "tts-speech-request.json", &request)
			if err := request.Validate(); err != nil {
				t.Fatalf("fixture must validate before mutation: %v", err)
			}
			tc.mutate(&request)
			assertInvalid(t, request.Validate(), tc.want)
		})
	}
}

func TestTTSStreamMessageValidation(t *testing.T) {
	t.Parallel()

	configure := relayapi.TTSSessionConfigure{
		Type:    relayapi.TTSControlSessionConfigure,
		Routing: relayapi.Routing{}.NormalizeDefault(),
		Audio:   relayapi.AudioConfig{Encoding: "pcm_s16le", SampleRateHz: 24_000, Channels: 1},
	}
	if err := configure.Validate(); err != nil {
		t.Fatalf("valid configure must validate: %v", err)
	}

	cases := []struct {
		name    string
		message interface{ Validate() error }
		want    string
	}{
		{"configure with wrong tag", func() relayapi.TTSSessionConfigure { c := configure; c.Type = "configure"; return c }(), `got "configure"`},
		{"configure with unknown encoding", func() relayapi.TTSSessionConfigure { c := configure; c.Audio.Encoding = "flac"; return c }(), "encoding: unsupported value"},
		{"configure with unnormalized routing", func() relayapi.TTSSessionConfigure { c := configure; c.Routing = relayapi.Routing{}; return c }(), "routing: mode: unsupported value"},
		{"append without text", relayapi.TTSInputAppend{Type: relayapi.TTSControlInputAppend}, "text: required"},
		{"append with wrong tag", relayapi.TTSInputAppend{Type: "append", Text: "x"}, `got "append"`},
		{"commit with wrong tag", relayapi.TTSInputCommit{Type: "commit"}, `got "commit"`},
		{"cancel with wrong tag", relayapi.TTSInputCancel{Type: "cancel"}, `got "cancel"`},
		{"close with wrong tag", relayapi.TTSSessionClose{Type: "close"}, `got "close"`},
		{"ready without request id", relayapi.TTSSessionReady{Type: relayapi.TTSEventSessionReady, Route: relayapi.Route{Provider: "cartesia", Model: "sonic-3", Region: "ap-northeast-1", AttemptID: "att_1"}}, "request_id: required"},
		{"utterance started at zero", relayapi.TTSUtteranceStarted{Type: relayapi.TTSEventUtteranceStarted}, "sequence: must be at least 1"},
		{"utterance done at zero", relayapi.TTSUtteranceDone{Type: relayapi.TTSEventUtteranceDone}, "sequence: must be at least 1"},
		{"usage update with negative usage", relayapi.TTSUsageUpdated{Type: relayapi.TTSEventUsageUpdated, Usage: relayapi.Usage{Characters: -1}}, "must not be negative"},
		{"closed with negative usage", relayapi.TTSSessionClosed{Type: relayapi.TTSEventSessionClosed, Usage: relayapi.Usage{Characters: -1}}, "must not be negative"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertInvalid(t, tc.message.Validate(), tc.want)
		})
	}
}
