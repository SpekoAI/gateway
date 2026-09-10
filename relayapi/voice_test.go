package relayapi_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/SpekoAI/gateway/relayapi"
)

func liveStartFixture(t *testing.T) []byte {
	t.Helper()
	var frames []json.RawMessage
	decodeFixture(t, "ws-live-messages.json", &frames)
	return frames[0]
}

func TestLiveSessionStartDecodesAndValidatesTheFixture(t *testing.T) {
	t.Parallel()
	start, err := relayapi.DecodeLiveSessionStart(liveStartFixture(t))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if err := start.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if start.Session.Model != "gpt-live-1" || start.Session.SampleRateHz() != 24_000 || start.Session.Voice() != "marin" {
		t.Fatalf("session = %+v", start.Session)
	}
	if start.Session.DelegationType() != "responses" || start.Session.Delegation.Responses.Model != "gpt-5.6-luna" {
		t.Fatalf("delegation = %+v", start.Session.Delegation)
	}
	if got := start.Session.Delegation.Responses.ToolTypes(); len(got) != 2 || got[0] != "web_search" || got[1] != "function" {
		t.Fatalf("tool types = %v", got)
	}
}

func TestLiveSessionStartDefaults(t *testing.T) {
	t.Parallel()
	start, err := relayapi.DecodeLiveSessionStart([]byte(`{"type":"session.start","session":{"model":"gpt-live-1"}}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if err := start.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if start.Session.SampleRateHz() != 24_000 || start.Session.Voice() != "" || start.Session.DelegationType() != "client" {
		t.Fatalf("defaults = rate %d voice %q delegation %q", start.Session.SampleRateHz(), start.Session.Voice(), start.Session.DelegationType())
	}
}

func TestLiveSessionStartRejectsEachRuleViolation(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		mutate func(s *relayapi.LiveSessionStart)
		want   string
	}{
		{"wrong type", func(s *relayapi.LiveSessionStart) { s.Type = "session.update" }, "type"},
		{"no model", func(s *relayapi.LiveSessionStart) { s.Session.Model = "" }, "model"},
		{"auto model", func(s *relayapi.LiveSessionStart) { s.Session.Model = "auto" }, "model"},
		{"padded model", func(s *relayapi.LiveSessionStart) { s.Session.Model = " gpt-live-1" }, "model"},
		{"g711 audio", func(s *relayapi.LiveSessionStart) {
			s.Session.Audio.Format = &relayapi.LiveAudioFormat{Type: "audio/pcmu", Rate: 8000}
		}, "audio/pcm"},
		{"8k pcm", func(s *relayapi.LiveSessionStart) {
			s.Session.Audio.Format = &relayapi.LiveAudioFormat{Type: "audio/pcm", Rate: 8000}
		}, "rate"},
		{"store true", func(s *relayapi.LiveSessionStart) { v := true; s.Session.Store = &v }, "store"},
		{"responses without model", func(s *relayapi.LiveSessionStart) { s.Session.Delegation.Responses.Model = "" }, "backend model is required"},
		{"client with backend", func(s *relayapi.LiveSessionStart) {
			s.Session.Delegation.Type = "client"
		}, "not valid for client delegation"},
		{"unknown delegation", func(s *relayapi.LiveSessionStart) { s.Session.Delegation.Type = "server" }, "client or responses"},
		{"tiny max output", func(s *relayapi.LiveSessionStart) { v := 8; s.Session.Delegation.Responses.MaxOutputTokens = &v }, "max_output_tokens"},
		{"unsupported tool", func(s *relayapi.LiveSessionStart) {
			s.Session.Delegation.Responses.Tools = []json.RawMessage{json.RawMessage(`{"type":"code_interpreter"}`)}
		}, "not supported"},
		{"nameless function", func(s *relayapi.LiveSessionStart) {
			s.Session.Delegation.Responses.Tools = []json.RawMessage{json.RawMessage(`{"type":"function"}`)}
		}, "requires a name"},
		{"duplicate function", func(s *relayapi.LiveSessionStart) {
			s.Session.Delegation.Responses.Tools = []json.RawMessage{json.RawMessage(`{"type":"function","name":"a"}`), json.RawMessage(`{"type":"function","name":"a"}`)}
		}, "duplicate"},
		{"bad service tier", func(s *relayapi.LiveSessionStart) { s.Session.Delegation.Responses.ServiceTier = "turbo" }, "service_tier"},
		{"history bad role", func(s *relayapi.LiveSessionStart) { s.Session.Input[0].Role = "tool" }, "role"},
		{"history empty content", func(s *relayapi.LiveSessionStart) { s.Session.Input[0].Content = nil }, "content"},
		{"history bad part", func(s *relayapi.LiveSessionStart) { s.Session.Input[0].Content[0].Type = "input_audio" }, "input_text or output_text"},
		{"oversized instructions", func(s *relayapi.LiveSessionStart) {
			s.Session.Instructions = strings.Repeat("x", relayapi.MaxLiveInstructionsBytes+1)
		}, "instructions"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			start, err := relayapi.DecodeLiveSessionStart(liveStartFixture(t))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			tc.mutate(&start)
			assertInvalid(t, start.Validate(), tc.want)
		})
	}
}

func TestLiveSessionStartRefusesUnknownFieldsAndTrailingContent(t *testing.T) {
	t.Parallel()
	if _, err := relayapi.DecodeLiveSessionStart([]byte(`{"type":"session.start","session":{"model":"gpt-live-1","turn_detection":{"type":"server_vad"}}}`)); err == nil {
		t.Fatal("unknown session field must be refused")
	}
	if _, err := relayapi.DecodeLiveSessionStart([]byte(`{"type":"session.start","session":{"model":"gpt-live-1"}} {}`)); err == nil {
		t.Fatal("trailing content must be refused")
	}
}
