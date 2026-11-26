package relayapi_test

import (
	"encoding/json"
	"fmt"
	"strings"
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

	active := true
	request.Options = &relayapi.STTOptions{LanguageIdentification: &active}
	assertInvalid(t, request.Validate(), "language_identification")
	active = false
	if err := request.Validate(); err != nil {
		t.Fatalf("batch must accept explicit false: %v", err)
	}
}

func TestSTTLanguageSegmentsKeepLegacyFrameBytesWhenUnlabelled(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		body any
		want string
	}{
		{"delta", relayapi.STTTranscriptDelta{Type: relayapi.STTEventTranscriptDelta, Text: "Hi", Segments: []relayapi.TranscriptDeltaSegment{{Text: "Hi", Speaker: "1"}}},
			`{"type":"transcript.delta","text":"Hi","segments":[{"text":"Hi","speaker":"1"}]}`},
		{"final", relayapi.STTTranscriptFinal{Type: relayapi.STTEventTranscriptFinal, Text: "Hi", Segments: []relayapi.TranscriptSegment{{Text: "Hi", StartMS: 0, EndMS: 1, Speaker: "1"}}},
			`{"type":"transcript.final","text":"Hi","segments":[{"text":"Hi","start_ms":0,"end_ms":1,"speaker":"1"}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := json.Marshal(tc.body)
			if err != nil || string(got) != tc.want {
				t.Errorf("marshal: %s, %v; want %s", got, err, tc.want)
			}
		})
	}
}

func TestSTTLanguageIdentificationIsCapabilityGated(t *testing.T) {
	t.Parallel()

	active := true
	options := &relayapi.STTOptions{LanguageIdentification: &active}
	if ask, supported := (relayapi.ModelCapabilities{}).SupportsSTTOptions(options); supported || ask != "language_identification" {
		t.Errorf("unsupported model returned %q, %v", ask, supported)
	}
	if ask, supported := (relayapi.ModelCapabilities{LanguageIdentification: true}).SupportsSTTOptions(options); !supported || ask != "" {
		t.Errorf("supporting model returned %q, %v", ask, supported)
	}
	active = false
	if ask, supported := (relayapi.ModelCapabilities{}).SupportsSTTOptions(options); !supported || ask != "" {
		t.Errorf("explicit false returned %q, %v", ask, supported)
	}
	for _, tag := range []string{"pt-BR", "zh-Hant", "en"} {
		if err := (relayapi.TranscriptSegment{Language: tag}).Validate(); err != nil {
			t.Errorf("valid tag %q: %v", tag, err)
		}
	}
	assertInvalid(t, (relayapi.TranscriptDeltaSegment{Language: "en_US"}).Validate(), "language")
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
		{"word ends before start", func(r *relayapi.TranscriptionResponse) {
			r.Words = []relayapi.TranscriptWord{{Text: "hello", StartMS: 500, EndMS: 400}}
		}, "words[0]"},
		{"blank word", func(r *relayapi.TranscriptionResponse) {
			r.Words = []relayapi.TranscriptWord{{Text: " ", StartMS: 0, EndMS: 400}}
		}, "words[0]"},
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

	// On a translating session the translation trails the transcript, so a
	// delta that advances only the translation is still something to say.
	if err := (relayapi.STTTranscriptDelta{Type: relayapi.STTEventTranscriptDelta, Translation: "hola"}).Validate(); err != nil {
		t.Fatalf("a translation-only delta must validate: %v", err)
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
		{"commit id too long", relayapi.STTInputCommit{Type: relayapi.STTControlInputCommit, CommitID: strings.Repeat("a", relayapi.MaxCommitIDLength+1)}, "commit_id"},
		{"commit id with a space", relayapi.STTInputCommit{Type: relayapi.STTControlInputCommit, CommitID: "turn 7"}, "commit_id"},
		{"commit id with a slash", relayapi.STTInputCommit{Type: relayapi.STTControlInputCommit, CommitID: "turn/7"}, "commit_id"},
		{"commit id beyond ASCII", relayapi.STTInputCommit{Type: relayapi.STTControlInputCommit, CommitID: "turné"}, "commit_id"},
		{"final with bad commit id", relayapi.STTTranscriptFinal{Type: relayapi.STTEventTranscriptFinal, CommitID: "turn 7"}, "commit_id"},
		{"interim final with a commit id", relayapi.STTTranscriptFinal{Type: relayapi.STTEventTranscriptFinal, Text: "x", Interim: true, CommitID: "review-1"}, "commit_id"},
		{"close with wrong tag", relayapi.STTSessionClose{Type: "close"}, `got "close"`},
		{"ready without request id", relayapi.STTSessionReady{Type: relayapi.STTEventSessionReady, Route: relayapi.Route{Provider: "deepgram", Model: "nova-3", Region: "eu-west-1", AttemptID: "att_1"}}, "request_id: required"},
		{"ready with incomplete route", relayapi.STTSessionReady{Type: relayapi.STTEventSessionReady, RequestID: "req_1", Route: relayapi.Route{Provider: "deepgram"}}, "route:"},
		{"empty transcript delta", relayapi.STTTranscriptDelta{Type: relayapi.STTEventTranscriptDelta}, "text: required"},
		{"delta with bad segment", relayapi.STTTranscriptDelta{Type: relayapi.STTEventTranscriptDelta, Text: "x", Segments: []relayapi.TranscriptDeltaSegment{{Text: "x", StartMS: ms(10), EndMS: ms(5)}}}, "segments[0]"},
		{"delta segment with a start and no end", relayapi.STTTranscriptDelta{Type: relayapi.STTEventTranscriptDelta, Text: "x", Segments: []relayapi.TranscriptDeltaSegment{{Text: "x", StartMS: ms(10)}}}, "segments[0]"},
		{"delta segment with an end and no start", relayapi.STTTranscriptDelta{Type: relayapi.STTEventTranscriptDelta, Text: "x", Segments: []relayapi.TranscriptDeltaSegment{{Text: "x", EndMS: ms(10)}}}, "segments[0]"},
		{"delta segment before zero", relayapi.STTTranscriptDelta{Type: relayapi.STTEventTranscriptDelta, Text: "x", Segments: []relayapi.TranscriptDeltaSegment{{Text: "x", StartMS: ms(-1), EndMS: ms(5)}}}, "segments[0]"},
		{"final with bad segment", relayapi.STTTranscriptFinal{Type: relayapi.STTEventTranscriptFinal, Text: "x", Segments: []relayapi.TranscriptSegment{{StartMS: 10, EndMS: 5}}}, "segments[0]"},
		{"continuation with of and no offset", func() relayapi.STTSessionConfigure {
			c := configure
			c.Continuation = &relayapi.STTContinuation{Of: "req_1"}
			return c
		}(), "continuation: of and audio_offset_ms"},
		{"continuation with offset and no of", func() relayapi.STTSessionConfigure {
			c := configure
			c.Continuation = &relayapi.STTContinuation{AudioOffsetMS: ms(10)}
			return c
		}(), "continuation: of and audio_offset_ms"},
		{"continuation with negative offset", func() relayapi.STTSessionConfigure {
			c := configure
			c.Continuation = &relayapi.STTContinuation{Of: "req_1", AudioOffsetMS: ms(-1)}
			return c
		}(), "continuation: audio_offset_ms"},
		{"continuation past a day", func() relayapi.STTSessionConfigure {
			c := configure
			c.Continuation = &relayapi.STTContinuation{Of: "req_1", AudioOffsetMS: ms(relayapi.MaxContinuationOffsetMS + 1)}
			return c
		}(), "continuation: audio_offset_ms"},
		{"continuation of with a space", func() relayapi.STTSessionConfigure {
			c := configure
			c.Continuation = &relayapi.STTContinuation{Of: "req 1", AudioOffsetMS: ms(0)}
			return c
		}(), "continuation: of"},
		{"continuation of too long", func() relayapi.STTSessionConfigure {
			c := configure
			c.Continuation = &relayapi.STTContinuation{Of: strings.Repeat("r", relayapi.MaxContinuationOfLength+1), AudioOffsetMS: ms(0)}
			return c
		}(), "continuation: of"},
		{"final with negative covered_ms", relayapi.STTTranscriptFinal{Type: relayapi.STTEventTranscriptFinal, CoveredMS: ms(-1)}, "covered_ms"},
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

func ms(value int64) *int64 { return &value }

// A delta segment can say who said its text before anyone knows when they
// stopped: without both times it carries neither, and still validates.
func TestSTTTranscriptDeltaSegmentWithoutTimes(t *testing.T) {
	t.Parallel()
	delta := relayapi.STTTranscriptDelta{Type: relayapi.STTEventTranscriptDelta, Text: "A B", Segments: []relayapi.TranscriptDeltaSegment{
		{Text: "A", StartMS: ms(100), EndMS: ms(200), Speaker: "1"},
		{Text: "B", Speaker: "2"},
	}}
	if err := delta.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	encoded, err := json.Marshal(delta)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"type":"transcript.delta","text":"A B","segments":[{"text":"A","start_ms":100,"end_ms":200,"speaker":"1"},{"text":"B","speaker":"2"}]}`
	if string(encoded) != want {
		t.Fatalf("delta = %s\nwant    %s", encoded, want)
	}
}

// Speaker segments on a delta are opt-in: a delta from a session that never
// asked for diarization keeps the exact bytes it always had.
func TestSTTTranscriptDeltaWithoutSegmentsKeepsItsBytes(t *testing.T) {
	t.Parallel()
	encoded, err := json.Marshal(relayapi.STTTranscriptDelta{Type: relayapi.STTEventTranscriptDelta, Text: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(encoded), `{"type":"transcript.delta","text":"hello"}`; got != want {
		t.Fatalf("delta = %s, want %s", got, want)
	}
}

// An interim final says more of the same segment follows; a final that
// does not say so keeps the bytes it always had.
func TestSTTInterimFinalOnTheWire(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		final relayapi.STTTranscriptFinal
		want  string
	}{
		{relayapi.STTTranscriptFinal{Type: relayapi.STTEventTranscriptFinal, Text: "first part", Interim: true}, `{"type":"transcript.final","text":"first part","interim":true}`},
		{relayapi.STTTranscriptFinal{Type: relayapi.STTEventTranscriptFinal, Text: "tail", CommitID: "review-1"}, `{"type":"transcript.final","text":"tail","commit_id":"review-1"}`},
	} {
		if err := test.final.Validate(); err != nil {
			t.Fatalf("%s: validate: %v", test.want, err)
		}
		encoded, err := json.Marshal(test.final)
		if err != nil {
			t.Fatal(err)
		}
		if string(encoded) != test.want {
			t.Errorf("final = %s\nwant    %s", encoded, test.want)
		}
	}
}

// Continuation is opt-in on every frame it touches: a client that never asks
// sends and reads exactly the bytes it always did, and 0 is a real value
// distinct from absent.
func TestSTTContinuationOnTheWire(t *testing.T) {
	t.Parallel()
	configure := relayapi.STTSessionConfigure{
		Type:    relayapi.STTControlSessionConfigure,
		Routing: relayapi.Routing{}.NormalizeDefault(),
		Audio:   relayapi.AudioConfig{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
	}
	asking, continuing := configure, configure
	asking.Continuation = &relayapi.STTContinuation{}
	continuing.Continuation = &relayapi.STTContinuation{Of: strings.Repeat("r", relayapi.MaxContinuationOfLength), AudioOffsetMS: ms(relayapi.MaxContinuationOffsetMS)}
	ready := relayapi.STTSessionReady{Type: relayapi.STTEventSessionReady, RequestID: "req_1", Route: relayapi.Route{Provider: "deepgram", Model: "nova-3", Region: "eu-west-1", AttemptID: "att_1"}}
	readyContinued := ready
	readyContinued.Continuation = true
	for _, test := range []struct {
		name  string
		value interface{ Validate() error }
		want  string
	}{
		{"plain configure", configure, `{"type":"session.configure","routing":{"mode":"auto","objective":"balanced"},"audio":{"encoding":"pcm_s16le","sample_rate_hz":16000,"channels":1}}`},
		{"configure asking", asking, `{"type":"session.configure","routing":{"mode":"auto","objective":"balanced"},"audio":{"encoding":"pcm_s16le","sample_rate_hz":16000,"channels":1},"continuation":{}}`},
		{"configure continuing", continuing, `{"type":"session.configure","routing":{"mode":"auto","objective":"balanced"},"audio":{"encoding":"pcm_s16le","sample_rate_hz":16000,"channels":1},"continuation":{"of":"` + strings.Repeat("r", relayapi.MaxContinuationOfLength) + `","audio_offset_ms":86400000}}`},
		{"plain ready", ready, `{"type":"session.ready","request_id":"req_1","route":{"provider":"deepgram","model":"nova-3","region":"eu-west-1","attempt_id":"att_1"}}`},
		{"ready continuing", readyContinued, `{"type":"session.ready","request_id":"req_1","route":{"provider":"deepgram","model":"nova-3","region":"eu-west-1","attempt_id":"att_1"},"continuation":true}`},
		{"plain final", relayapi.STTTranscriptFinal{Type: relayapi.STTEventTranscriptFinal, Text: "hi"}, `{"type":"transcript.final","text":"hi"}`},
		{"final covered at zero", relayapi.STTTranscriptFinal{Type: relayapi.STTEventTranscriptFinal, Text: "hi", CoveredMS: ms(0)}, `{"type":"transcript.final","text":"hi","covered_ms":0}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.value.Validate(); err != nil {
				t.Fatalf("validate: %v", err)
			}
			encoded, err := json.Marshal(test.value)
			if err != nil {
				t.Fatal(err)
			}
			if string(encoded) != test.want {
				t.Errorf("got  %s\nwant %s", encoded, test.want)
			}
		})
	}
	if !continuing.Continuation.Continuing() || asking.Continuation.Continuing() || configure.Continuation.Continuing() {
		t.Error("Continuing must be true only on a session that names an offset")
	}
}

// An offset that does not fit int64, or is not a whole number, is refused at
// decode rather than wrapped or truncated.
func TestSTTContinuationOffsetDecode(t *testing.T) {
	t.Parallel()
	// What the schema refuses Go refuses too, rather than reading it as {}:
	// an empty label, a null member, an unknown member.
	for raw, want := range map[string]string{
		`{"of":""}`:                                   "of:",
		`{"of":"","audio_offset_ms":0}`:               "of:",
		`{"of":null}`:                                 "of: must not be null",
		`{"audio_offset_ms":null}`:                    "audio_offset_ms: must not be null",
		`{"of":null,"audio_offset_ms":null}`:          "must not be null",
		`{"typo":1}`:                                  "typo: unknown member",
		`{"of":"req_1","audio_offset_ms":0,"x":true}`: "x: unknown member",
	} {
		var continuation relayapi.STTContinuation
		if err := json.Unmarshal([]byte(raw), &continuation); err != nil {
			t.Fatalf("%s: decode: %v", raw, err)
		}
		assertInvalid(t, continuation.Validate(), want)
	}
	for _, raw := range []string{`{}`, `{"of":"req_1","audio_offset_ms":0}`} {
		var continuation relayapi.STTContinuation
		if err := json.Unmarshal([]byte(raw), &continuation); err != nil || continuation.Validate() != nil {
			t.Fatalf("%s must decode and validate: %v / %v", raw, err, continuation.Validate())
		}
	}
	assertInvalid(t, (relayapi.STTTranscriptFinal{Type: relayapi.STTEventTranscriptFinal, CoveredMS: ms(relayapi.MaxCoveredMS + 1)}).Validate(), "covered_ms")
	for _, raw := range []string{`{"of":"req_1","audio_offset_ms":9223372036854775808}`, `{"of":"req_1","audio_offset_ms":1.5}`, `{"of":"req_1","audio_offset_ms":"10"}`} {
		var continuation relayapi.STTContinuation
		if err := json.Unmarshal([]byte(raw), &continuation); err == nil {
			t.Errorf("%s: decoded to %+v, want an error", raw, continuation)
		}
	}
}

// A commit id is 1 to 64 ASCII letters, digits, and . _ : - so it can be
// echoed verbatim and matched byte for byte.
func TestSTTCommitIDForm(t *testing.T) {
	t.Parallel()
	for _, id := range []string{"a", "Z9", "turn-7:a_b.c", strings.Repeat("x", relayapi.MaxCommitIDLength)} {
		if !relayapi.ValidCommitID(id) {
			t.Errorf("ValidCommitID(%q) = false, want true", id)
		}
		if err := (relayapi.STTInputCommit{Type: relayapi.STTControlInputCommit, CommitID: id}).Validate(); err != nil {
			t.Errorf("commit with id %q: %v", id, err)
		}
	}
	for _, id := range []string{"", strings.Repeat("x", relayapi.MaxCommitIDLength+1), "a b", "a/b", "a\nb", "é", "a+b", "{}"} {
		if relayapi.ValidCommitID(id) {
			t.Errorf("ValidCommitID(%q) = true, want false", id)
		}
	}
	if err := (relayapi.STTInputCommit{Type: relayapi.STTControlInputCommit}).Validate(); err != nil {
		t.Fatalf("a commit without an id must stay valid: %v", err)
	}
}

// On the wire, a commit_id that is sent must be well formed: an empty one is
// refused rather than read as no id. Omitted and null both mean no id.
func TestSTTInputCommitFrameCommitID(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		frame, wantID string
		valid         bool
	}{
		{`{"type":"input.commit"}`, "", true},
		{`{"type":"input.commit","commit_id":null}`, "", true},
		{`{"type":"input.commit","commit_id":"turn-7"}`, "turn-7", true},
		{`{"type":"input.commit","commit_id":""}`, "", false},
		{`{"type":"input.commit","commit_id":"turn 7"}`, "turn 7", false},
	} {
		var commit relayapi.STTInputCommit
		if err := json.Unmarshal([]byte(test.frame), &commit); err != nil {
			t.Fatalf("%s: decode: %v", test.frame, err)
		}
		if commit.CommitID != test.wantID {
			t.Errorf("%s: commit_id = %q, want %q", test.frame, commit.CommitID, test.wantID)
		}
		err := commit.Validate()
		if test.valid && err != nil {
			t.Errorf("%s: %v, want valid", test.frame, err)
		}
		if !test.valid && (err == nil || !strings.Contains(err.Error(), "commit_id")) {
			t.Errorf("%s: Validate() = %v, want a commit_id error", test.frame, err)
		}
	}
	var commit relayapi.STTInputCommit
	if err := json.Unmarshal([]byte(`{"type":"input.commit","commit_id":7}`), &commit); err == nil {
		t.Fatal("a numeric commit_id must not decode")
	}
}
