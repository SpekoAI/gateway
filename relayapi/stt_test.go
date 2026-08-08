package relayapi_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/SpekoAI/gateway/relayapi"
)

func TestSTTWebSocketFixturesRoundTrip(t *testing.T) {
	t.Parallel()

	var frames []json.RawMessage
	if err := json.Unmarshal(readFixture(t, "ws-stt-messages.json"), &frames); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}

	seen := make(map[string]bool)
	for i, frame := range frames {
		name := fmt.Sprintf("ws-stt-messages.json[%d]", i)
		switch messageType(t, name, frame) {
		case string(relayapi.STTControlSessionConfigure):
			assertGoldenValue[relayapi.STTSessionConfigure](t, name, frame)
		case string(relayapi.STTControlInputCommit):
			assertGoldenValue[relayapi.STTInputCommit](t, name, frame)
		case string(relayapi.STTControlSessionClose):
			assertGoldenValue[relayapi.STTSessionClose](t, name, frame)
		case string(relayapi.STTEventSessionReady):
			assertGoldenValue[relayapi.STTSessionReady](t, name, frame)
		case string(relayapi.STTEventTranscriptDelta):
			assertGoldenValue[relayapi.STTTranscriptDelta](t, name, frame)
		case string(relayapi.STTEventTranscriptFinal):
			assertGoldenValue[relayapi.STTTranscriptFinal](t, name, frame)
		case string(relayapi.STTEventUsageUpdated):
			assertGoldenValue[relayapi.STTUsageUpdated](t, name, frame)
		case string(relayapi.STTEventSessionClosed):
			assertGoldenValue[relayapi.STTSessionClosed](t, name, frame)
		case relayapi.ErrorEventType:
			assertGoldenValue[relayapi.ErrorEvent](t, name, frame)
		default:
			t.Fatalf("%s: unknown message type", name)
		}
		seen[messageType(t, name, frame)] = true
	}

	for _, want := range []string{
		string(relayapi.STTControlSessionConfigure),
		string(relayapi.STTControlInputCommit),
		string(relayapi.STTControlSessionClose),
		string(relayapi.STTEventSessionReady),
		string(relayapi.STTEventTranscriptDelta),
		string(relayapi.STTEventTranscriptFinal),
		string(relayapi.STTEventUsageUpdated),
		string(relayapi.STTEventSessionClosed),
		relayapi.ErrorEventType,
	} {
		if !seen[want] {
			t.Fatalf("fixture must cover message type %q", want)
		}
	}
}

func messageType(t *testing.T, name string, frame []byte) string {
	t.Helper()
	var tag struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(frame, &tag); err != nil {
		t.Fatalf("%s: decode tag: %v", name, err)
	}
	return tag.Type
}

func TestTranscriptionRequestValidation(t *testing.T) {
	t.Parallel()

	// An omitted routing object is valid once normalized.
	request := relayapi.TranscriptionRequest{Routing: relayapi.Routing{}.NormalizeDefault()}
	if err := request.Validate(); err != nil {
		t.Fatalf("normalized default routing must validate: %v", err)
	}

	// Without normalization the zero routing is rejected, not defaulted.
	assertInvalid(t, relayapi.TranscriptionRequest{}.Validate(), "routing: mode: unsupported value")
}

func TestTranscriptionResponseValidation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		mutate func(r *relayapi.TranscriptionResponse)
		want   string
	}{
		{"negative segment start", func(r *relayapi.TranscriptionResponse) { r.Segments[0].StartMS = -1 }, "segments[0]"},
		{"segment ends before start", func(r *relayapi.TranscriptionResponse) { r.Segments[0].EndMS = r.Segments[0].StartMS - 1 }, "segments[0]"},
		{"incomplete route", func(r *relayapi.TranscriptionResponse) { r.Route.AttemptID = "" }, "route:"},
		{"negative usage", func(r *relayapi.TranscriptionResponse) { r.Usage.DurationMS = -1 }, "must not be negative"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var response relayapi.TranscriptionResponse
			decodeFixture(t, "stt-transcription-response.json", &response)
			if err := response.Validate(); err != nil {
				t.Fatalf("fixture must validate before mutation: %v", err)
			}
			tc.mutate(&response)
			assertInvalid(t, response.Validate(), tc.want)
		})
	}
}

func TestSTTStreamMessageValidation(t *testing.T) {
	t.Parallel()

	configure := relayapi.STTSessionConfigure{
		Type:    relayapi.STTControlSessionConfigure,
		Routing: relayapi.Routing{}.NormalizeDefault(),
		Audio:   relayapi.AudioConfig{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
	}
	if err := configure.Validate(); err != nil {
		t.Fatalf("valid configure must validate: %v", err)
	}

	cases := []struct {
		name    string
		message interface{ Validate() error }
		want    string
	}{
		{"configure with wrong tag", func() relayapi.STTSessionConfigure { c := configure; c.Type = "configure"; return c }(), `got "configure"`},
		{"configure with unknown encoding", func() relayapi.STTSessionConfigure { c := configure; c.Audio.Encoding = "mp3"; return c }(), "encoding: unsupported value"},
		{"configure with low sample rate", func() relayapi.STTSessionConfigure { c := configure; c.Audio.SampleRateHz = 4_000; return c }(), "sample_rate_hz"},
		{"configure with too many channels", func() relayapi.STTSessionConfigure { c := configure; c.Audio.Channels = 9; return c }(), "channels"},
		{"configure with unnormalized routing", func() relayapi.STTSessionConfigure { c := configure; c.Routing = relayapi.Routing{}; return c }(), "routing: mode: unsupported value"},
		{"commit with wrong tag", relayapi.STTInputCommit{Type: "commit"}, `got "commit"`},
		{"close with wrong tag", relayapi.STTSessionClose{Type: "close"}, `got "close"`},
		{"ready without request id", relayapi.STTSessionReady{Type: relayapi.STTEventSessionReady, Route: relayapi.Route{Provider: "deepgram", Model: "nova-3", Region: "eu-west-1", AttemptID: "att_1"}}, "request_id: required"},
		{"ready with incomplete route", relayapi.STTSessionReady{Type: relayapi.STTEventSessionReady, RequestID: "req_1", Route: relayapi.Route{Provider: "deepgram"}}, "route:"},
		{"empty transcript delta", relayapi.STTTranscriptDelta{Type: relayapi.STTEventTranscriptDelta}, "text: required"},
		{"final with bad segment", relayapi.STTTranscriptFinal{Type: relayapi.STTEventTranscriptFinal, Text: "x", Segments: []relayapi.TranscriptSegment{{StartMS: 10, EndMS: 5}}}, "segments[0]"},
		{"usage update with negative usage", relayapi.STTUsageUpdated{Type: relayapi.STTEventUsageUpdated, Usage: relayapi.Usage{DurationMS: -1}}, "must not be negative"},
		{"closed with negative usage", relayapi.STTSessionClosed{Type: relayapi.STTEventSessionClosed, Usage: relayapi.Usage{DurationMS: -1}}, "must not be negative"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertInvalid(t, tc.message.Validate(), tc.want)
		})
	}
}
