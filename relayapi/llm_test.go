package relayapi_test

import (
	"encoding/json"
	"testing"

	"github.com/SpekoAI/gateway/relayapi"
)

func TestLLMRequestRejectsEachRuleViolation(t *testing.T) {
	t.Parallel()

	// llm-request-tools.json carries messages at input[0..1], a
	// function_call at input[2], and a function_result at input[3], so one
	// fixture exercises every input variant.
	cases := []struct {
		name   string
		mutate func(r *relayapi.LLMRequest)
		want   string
	}{
		{"missing max_output_tokens", func(r *relayapi.LLMRequest) { r.MaxOutputTokens = 0 }, "max_output_tokens: positive value is required"},
		{"negative max_output_tokens", func(r *relayapi.LLMRequest) { r.MaxOutputTokens = -1 }, "max_output_tokens: positive value is required"},
		{"empty input", func(r *relayapi.LLMRequest) { r.Input = nil }, "input: at least one item is required"},
		{"unknown item type", func(r *relayapi.LLMRequest) { r.Input[0].Type = "blob" }, "type: unsupported value"},
		{"unknown message role", func(r *relayapi.LLMRequest) { r.Input[0].Role = "tool" }, "role: unsupported value"},
		{"message without content", func(r *relayapi.LLMRequest) { r.Input[0].Content = nil }, "content: at least one part is required"},
		{"unknown content part type", func(r *relayapi.LLMRequest) { r.Input[0].Content[0].Type = "image" }, "content[0].type: unsupported value"},
		{"message with function field bleed", func(r *relayapi.LLMRequest) { r.Input[0].CallID = "call_9" }, "carry only role and content"},
		{"function_call without call_id", func(r *relayapi.LLMRequest) { r.Input[2].CallID = "" }, "call_id and name are required"},
		{"function_call without name", func(r *relayapi.LLMRequest) { r.Input[2].Name = "" }, "call_id and name are required"},
		{"function_call with invalid arguments", func(r *relayapi.LLMRequest) { r.Input[2].Arguments = "{" }, "arguments: must be a JSON text"},
		{"function_call with empty arguments", func(r *relayapi.LLMRequest) { r.Input[2].Arguments = "" }, "arguments: must be a JSON text"},
		{"function_call with result bleed", func(r *relayapi.LLMRequest) { r.Input[2].Result = "done" }, "carry only call_id, name, and arguments"},
		{"function_result without call_id", func(r *relayapi.LLMRequest) { r.Input[3].CallID = "" }, "call_id: required"},
		{"function_result with name bleed", func(r *relayapi.LLMRequest) { r.Input[3].Name = "get_weather" }, "carry only call_id and result"},
		{"structured_json in input", func(r *relayapi.LLMRequest) {
			r.Input[1] = relayapi.Item{Type: relayapi.ItemTypeStructuredJSON, JSON: json.RawMessage(`{"total":1}`)}
		}, "structured_json items are output-only"},
		{"duplicate tool names", func(r *relayapi.LLMRequest) { r.Tools = append(r.Tools, r.Tools[0]) }, "duplicate tool name"},
		{"tool without name", func(r *relayapi.LLMRequest) { r.Tools[0].Name = " " }, "name: required"},
		{"tool with invalid parameters", func(r *relayapi.LLMRequest) { r.Tools[0].Parameters = json.RawMessage("{") }, "parameters: must be a JSON value"},
		{"temperature above bound", func(r *relayapi.LLMRequest) { temperature := 2.5; r.Temperature = &temperature }, "temperature: must be between 0 and 2"},
		{"temperature below bound", func(r *relayapi.LLMRequest) { temperature := -0.1; r.Temperature = &temperature }, "temperature: must be between 0 and 2"},
		{"top_p zero", func(r *relayapi.LLMRequest) { topP := 0.0; r.TopP = &topP }, "top_p: must be greater than 0 and at most 1"},
		{"top_p above bound", func(r *relayapi.LLMRequest) { topP := 1.5; r.TopP = &topP }, "top_p: must be greater than 0 and at most 1"},
		{"mixed routing fields", func(r *relayapi.LLMRequest) { r.Routing.Model = "gpt-5.2" }, "valid only in explicit mode"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var request relayapi.LLMRequest
			decodeFixture(t, "llm-request-tools.json", &request)
			if err := request.Validate(); err != nil {
				t.Fatalf("fixture must validate before mutation: %v", err)
			}
			tc.mutate(&request)
			assertInvalid(t, request.Validate(), tc.want)
		})
	}
}

func TestLLMRequestRejectsResponseFormatViolations(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		mutate func(f *relayapi.ResponseFormat)
		want   string
	}{
		{"loose format type", func(f *relayapi.ResponseFormat) { f.Type = "json_object" }, `want "json_schema"`},
		{"missing name", func(f *relayapi.ResponseFormat) { f.Name = "" }, "name: required"},
		{"missing schema", func(f *relayapi.ResponseFormat) { f.Schema = nil }, "schema: must be a JSON value"},
		{"invalid schema", func(f *relayapi.ResponseFormat) { f.Schema = json.RawMessage("{") }, "schema: must be a JSON value"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var request relayapi.LLMRequest
			decodeFixture(t, "llm-request-structured.json", &request)
			tc.mutate(request.ResponseFormat)
			assertInvalid(t, request.Validate(), tc.want)
		})
	}
}

func TestLLMResponseRejectsEachRuleViolation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		mutate func(r *relayapi.LLMResponse)
		want   string
	}{
		{"provider-shaped id", func(r *relayapi.LLMResponse) { r.ID = "chatcmpl-abc123" }, "Speko-minted"},
		{"bare prefix id", func(r *relayapi.LLMResponse) { r.ID = "resp_" }, "Speko-minted"},
		{"incomplete route", func(r *relayapi.LLMResponse) { r.Route.Region = "" }, "route:"},
		{"empty output", func(r *relayapi.LLMResponse) { r.Output = nil }, "output: at least one item is required"},
		{"invalid output item", func(r *relayapi.LLMResponse) { r.Output[0].Role = "tool" }, "role: unsupported value"},
		// The output-side mirror of the input-side structured_json rejection:
		// function_result items only ever travel caller-to-relay, and output
		// messages are always assistant-authored, so a connector
		// normalization bug that emits either must fail loudly.
		{"function_result in output", func(r *relayapi.LLMResponse) {
			r.Output[0] = relayapi.Item{Type: relayapi.ItemTypeFunctionResult, CallID: "call_1", Result: "done"}
		}, "function_result items are input-only"},
		{"user message in output", func(r *relayapi.LLMResponse) { r.Output[0].Role = relayapi.RoleUser }, "output messages are assistant-authored"},
		{"system message in output", func(r *relayapi.LLMResponse) { r.Output[0].Role = relayapi.RoleSystem }, "output messages are assistant-authored"},
		{"invalid structured output", func(r *relayapi.LLMResponse) { r.Output[1].JSON = json.RawMessage("{") }, "json: must be a JSON value"},
		{"unknown stop reason", func(r *relayapi.LLMResponse) { r.StopReason = "length" }, "stop_reason: unsupported value"},
		{"negative usage line", func(r *relayapi.LLMResponse) { r.Usage.OutputTokens = -1 }, "must not be negative"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var response relayapi.LLMResponse
			decodeFixture(t, "llm-response.json", &response)
			if err := response.Validate(); err != nil {
				t.Fatalf("fixture must validate before mutation: %v", err)
			}
			tc.mutate(&response)
			assertInvalid(t, response.Validate(), tc.want)
		})
	}
}

func TestResponseIDIsSpekoMinted(t *testing.T) {
	t.Parallel()

	if got := relayapi.ResponseID("req_7d81b2"); got != "resp_req_7d81b2" {
		t.Fatalf("ResponseID = %q, want resp_req_7d81b2", got)
	}

	// The golden response fixture uses exactly the minted form so nobody can
	// mistake a provider identifier for a valid response id.
	var response relayapi.LLMResponse
	decodeFixture(t, "llm-response.json", &response)
	if response.ID != relayapi.ResponseID("req_7d81b2") {
		t.Fatalf("fixture id %q is not the minted form", response.ID)
	}
}
