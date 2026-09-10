package protocol_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
)

func TestSpeechProtocolsAndPublicRoutes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		protocol protocol.SpeechProtocol
		route    string
	}{
		{protocol.SpeechProtocolOpenAIRealtimeV1, "/v1/realtime"},
		{protocol.SpeechProtocolOpenAILiveV1, "/v1/live"},
		{protocol.SpeechProtocolGoogleLiveV1, ""},
		{protocol.SpeechProtocolXAIRealtimeV1, ""},
	} {
		if !protocol.ValidSpeechProtocol(tc.protocol) {
			t.Fatalf("%s must be valid", tc.protocol)
		}
		if got := tc.protocol.PublicRoute(); got != tc.route {
			t.Fatalf("%s route = %q, want %q", tc.protocol, got, tc.route)
		}
	}
	if protocol.ValidSpeechProtocol("openai.realtime.v2") {
		t.Fatal("unknown protocol accepted")
	}
}

func TestLiveOptionsValidation(t *testing.T) {
	t.Parallel()
	valid := protocol.LiveOptions{
		History: []protocol.LiveHistoryItem{{Role: "user", Text: "hello"}},
		Delegation: &protocol.LiveDelegation{Type: protocol.LiveDelegationResponses, Responses: &protocol.LiveResponsesConfig{
			Model: "gpt-5.6-luna", MaxOutputTokens: 256,
			Tools: []protocol.LiveTool{protocol.NewLiveTool(json.RawMessage(`{"type":"web_search"}`)), protocol.NewLiveTool(json.RawMessage(`{"type":"function","name":"lookup"}`))},
		}},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid options rejected: %v", err)
	}
	if got := valid.Delegation.Responses.ToolTypes(); len(got) != 2 || got[0] != protocol.LiveToolWebSearch || got[1] != protocol.LiveToolFunction {
		t.Fatalf("tool types = %v", got)
	}
	var nilDelegation *protocol.LiveDelegation
	if nilDelegation.Mode() != protocol.LiveDelegationClient {
		t.Fatal("absent delegation must be client mode")
	}
	cases := []struct {
		name   string
		mutate func(o *protocol.LiveOptions)
		want   string
	}{
		{"responses without model", func(o *protocol.LiveOptions) { o.Delegation.Responses.Model = "" }, "backend model"},
		{"responses auto model", func(o *protocol.LiveOptions) { o.Delegation.Responses.Model = "auto" }, "backend model"},
		{"client with backend", func(o *protocol.LiveOptions) { o.Delegation.Type = protocol.LiveDelegationClient }, "not valid for client"},
		{"unknown delegation", func(o *protocol.LiveOptions) { o.Delegation.Type = "server" }, "client or responses"},
		{"tiny max output", func(o *protocol.LiveOptions) { o.Delegation.Responses.MaxOutputTokens = 4 }, "max_output_tokens"},
		{"unsupported tool", func(o *protocol.LiveOptions) {
			o.Delegation.Responses.Tools = []protocol.LiveTool{protocol.NewLiveTool(json.RawMessage(`{"type":"file_search"}`))}
		}, "not supported"},
		{"nameless function", func(o *protocol.LiveOptions) {
			o.Delegation.Responses.Tools = []protocol.LiveTool{protocol.NewLiveTool(json.RawMessage(`{"type":"function"}`))}
		}, "requires a name"},
		{"bad tier", func(o *protocol.LiveOptions) { o.Delegation.Responses.ServiceTier = "turbo" }, "service_tier"},
		{"bad history role", func(o *protocol.LiveOptions) { o.History[0].Role = "tool" }, "role"},
		{"empty history text", func(o *protocol.LiveOptions) { o.History[0].Text = " " }, "text"},
		{"oversized history", func(o *protocol.LiveOptions) {
			o.History = nil
			for i := 0; i < protocol.MaxLiveHistoryItems+1; i++ {
				o.History = append(o.History, protocol.LiveHistoryItem{Role: "user", Text: "x"})
			}
		}, "at most"},
		{"oversized backend instructions", func(o *protocol.LiveOptions) {
			o.Delegation.Responses.Instructions = strings.Repeat("x", protocol.MaxLiveBackendInstructionsBytes+1)
		}, "instructions"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			options := valid
			options.History = append([]protocol.LiveHistoryItem(nil), valid.History...)
			delegation := *valid.Delegation
			responses := *valid.Delegation.Responses
			delegation.Responses = &responses
			options.Delegation = &delegation
			tc.mutate(&options)
			err := options.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestLiveToolRoundTripsVerbatim(t *testing.T) {
	t.Parallel()
	raw := `{"type":"function","name":"lookup","description":"d","parameters":{"type":"object","properties":{"id":{"type":"string"}}},"strict":true}`
	var config protocol.LiveResponsesConfig
	if err := json.Unmarshal([]byte(`{"model":"m","tools":[`+raw+`]}`), &config); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	encoded, err := json.Marshal(config.Tools[0])
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(encoded) != raw {
		t.Fatalf("tool changed shape: %s", encoded)
	}
}

func TestProviderControlAllowlistsAreProtocolScoped(t *testing.T) {
	t.Parallel()
	live := protocol.SpeechProtocolOpenAILiveV1
	realtime := protocol.SpeechProtocolOpenAIRealtimeV1
	for _, name := range []string{"session.instructions.append", "session.thinking.append", "session.commentary.append", "response.item.create", "response.create", "session.update", "session.input_audio.mute", "session.input_audio.unmute"} {
		if !protocol.ProviderControlAllowed(live, name) {
			t.Fatalf("live must allow %s", name)
		}
	}
	for _, name := range []string{"input_audio_buffer.commit", "response.cancel", "conversation.item.create", "session.start", "session.close", "session.input_audio.append"} {
		if protocol.ProviderControlAllowed(live, name) {
			t.Fatalf("live must refuse %s", name)
		}
	}
	for _, name := range []string{"input_audio_buffer.commit", "response.cancel", "conversation.item.create", "response.create", "session.update"} {
		if !protocol.ProviderControlAllowed(realtime, name) {
			t.Fatalf("realtime must allow %s", name)
		}
	}
	for _, name := range []string{"session.commentary.append", "response.item.create", "input_audio_buffer.append", "session.close"} {
		if protocol.ProviderControlAllowed(realtime, name) {
			t.Fatalf("realtime must refuse %s", name)
		}
	}
	if protocol.ProviderControlAllowed(protocol.SpeechProtocolGoogleLiveV1, "session.update") {
		t.Fatal("protocols without a public route forward no controls")
	}
	if types := protocol.ProviderControlTypes(live); len(types) != 8 || types[0] != "response.create" {
		t.Fatalf("live control types = %v", types)
	}
}

func TestProviderControlValidation(t *testing.T) {
	t.Parallel()
	live := protocol.SpeechProtocolOpenAILiveV1
	ok := protocol.ProviderControl{Type: "response.create", Payload: json.RawMessage(`{"type":"response.create","event_id":"c1"}`)}
	if err := ok.Validate(live); err != nil {
		t.Fatalf("valid control rejected: %v", err)
	}
	for _, tc := range []struct {
		name    string
		control protocol.ProviderControl
		want    string
	}{
		{"empty type", protocol.ProviderControl{Payload: json.RawMessage(`{}`)}, "type: required"},
		{"foreign type", protocol.ProviderControl{Type: "response.cancel", Payload: json.RawMessage(`{"type":"response.cancel"}`)}, "not a forwardable"},
		{"missing payload", protocol.ProviderControl{Type: "response.create"}, "payload"},
		{"array payload", protocol.ProviderControl{Type: "response.create", Payload: json.RawMessage(`[]`)}, "JSON object"},
		{"tag mismatch", protocol.ProviderControl{Type: "response.create", Payload: json.RawMessage(`{"type":"session.update"}`)}, "does not match"},
		{"oversized", protocol.ProviderControl{Type: "response.create", Payload: json.RawMessage(`{"type":"response.create","x":"` + strings.Repeat("y", protocol.MaxProviderControlBytes) + `"}`)}, "at most"},
	} {
		if err := tc.control.Validate(live); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: err = %v, want %q", tc.name, err, tc.want)
		}
	}
	event := protocol.ProviderEvent{Type: "session.delegation.created", Payload: json.RawMessage(`{"type":"session.delegation.created","delegation":{"id":"item_1"}}`)}
	if err := event.Validate(); err != nil {
		t.Fatalf("valid event rejected: %v", err)
	}
	if err := (protocol.ProviderEvent{Type: "x", Payload: json.RawMessage(`"s"`)}).Validate(); err == nil {
		t.Fatal("non-object event payload accepted")
	}
}

func TestRelayPlanProtocolRules(t *testing.T) {
	t.Parallel()
	var plan protocol.RelayPlan
	decodeFixture(t, "relay-plan-s2s.json", &plan)
	now := plan.ExpiresAt.Add(-time.Minute)
	if err := plan.Validate(now); err != nil {
		t.Fatalf("fixture must validate: %v", err)
	}
	plan.Protocol = ""
	if err := plan.Validate(now); err == nil || !strings.Contains(err.Error(), "protocol") {
		t.Fatalf("s2s plan without protocol accepted: %v", err)
	}
	plan.Protocol = "openai.realtime.v9"
	if err := plan.Validate(now); err == nil || !strings.Contains(err.Error(), "protocol") {
		t.Fatalf("unknown protocol accepted: %v", err)
	}
	plan.Protocol = protocol.SpeechProtocolOpenAILiveV1
	plan.Budgets = append(plan.Budgets, protocol.RelayBudget{Group: protocol.RelayBudgetGroupBackendInput, CeilingUnits: 1000})
	if err := plan.Validate(now); err == nil || !strings.Contains(err.Error(), "together") {
		t.Fatalf("lone backend_input accepted: %v", err)
	}
	plan.Budgets = append(plan.Budgets, protocol.RelayBudget{Group: protocol.RelayBudgetGroupBackendOutput, CeilingUnits: 500}, protocol.RelayBudget{Group: protocol.RelayBudgetGroupBackendTools, CeilingUnits: 4})
	if err := plan.Validate(now); err != nil {
		t.Fatalf("full live budget set rejected: %v", err)
	}
	plan.Budgets = plan.Budgets[1:]
	if err := plan.Validate(now); err == nil || !strings.Contains(err.Error(), "s2s_duration") {
		t.Fatalf("s2s plan without s2s_duration accepted: %v", err)
	}

	var sttPlan protocol.RelayPlan
	decodeFixture(t, "relay-plan-stt.json", &sttPlan)
	sttPlan.Protocol = protocol.SpeechProtocolOpenAILiveV1
	if err := sttPlan.Validate(sttPlan.ExpiresAt.Add(-time.Minute)); err == nil || !strings.Contains(err.Error(), "only for s2s") {
		t.Fatalf("stt plan with protocol accepted: %v", err)
	}
}
