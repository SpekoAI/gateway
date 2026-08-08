package relayapi_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/SpekoAI/gateway/relayapi"
)

// streamPayload is what every SSE data payload must offer: validation plus
// the event name it travels under.
type streamPayload interface {
	Validate() error
	Event() string
}

func TestSSEEventFixturesRoundTrip(t *testing.T) {
	t.Parallel()

	var entries []struct {
		Event string          `json:"event"`
		Data  json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(readFixture(t, "llm-sse-events.json"), &entries); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}

	seen := make(map[string]bool)
	for i, entry := range entries {
		name := fmt.Sprintf("llm-sse-events.json[%d]", i)
		switch entry.Event {
		case relayapi.SSEResponseCreated:
			assertStreamPayload[relayapi.ResponseCreated](t, name, entry.Event, entry.Data)
		case relayapi.SSEResponseItemAdded:
			assertStreamPayload[relayapi.ResponseItemAdded](t, name, entry.Event, entry.Data)
		case relayapi.SSEResponseTextDelta:
			assertStreamPayload[relayapi.ResponseTextDelta](t, name, entry.Event, entry.Data)
		case relayapi.SSEResponseFunctionCallArgumentsDelta:
			assertStreamPayload[relayapi.ResponseFunctionCallArgumentsDelta](t, name, entry.Event, entry.Data)
		case relayapi.SSEResponseItemCompleted:
			assertStreamPayload[relayapi.ResponseItemCompleted](t, name, entry.Event, entry.Data)
		case relayapi.SSEResponseCompleted:
			assertStreamPayload[relayapi.ResponseCompleted](t, name, entry.Event, entry.Data)
		case relayapi.SSEError:
			assertStreamPayload[relayapi.ErrorEnvelope](t, name, entry.Event, entry.Data)
		default:
			t.Fatalf("%s: unknown event %q", name, entry.Event)
		}
		seen[entry.Event] = true
	}

	for _, want := range []string{
		relayapi.SSEResponseCreated,
		relayapi.SSEResponseItemAdded,
		relayapi.SSEResponseTextDelta,
		relayapi.SSEResponseFunctionCallArgumentsDelta,
		relayapi.SSEResponseItemCompleted,
		relayapi.SSEResponseCompleted,
		relayapi.SSEError,
	} {
		if !seen[want] {
			t.Fatalf("fixture must cover SSE event %q", want)
		}
	}
}

func assertStreamPayload[T streamPayload](t *testing.T, name, event string, data []byte) {
	t.Helper()
	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatalf("%s: decode: %v", name, err)
	}
	if err := value.Validate(); err != nil {
		t.Fatalf("%s: must validate: %v", name, err)
	}
	if got := value.Event(); got != event {
		t.Fatalf("%s: Event() = %q, want %q", name, got, event)
	}
	assertStableBytes(t, name, data, value)
}

func TestTerminalSSEEventClassification(t *testing.T) {
	t.Parallel()

	// Exactly one terminal event ends every stream; only these two names
	// qualify.
	terminal := map[string]bool{
		relayapi.SSEResponseCompleted: true,
		relayapi.SSEError:             true,
	}
	for _, event := range []string{
		relayapi.SSEResponseCreated,
		relayapi.SSEResponseItemAdded,
		relayapi.SSEResponseTextDelta,
		relayapi.SSEResponseFunctionCallArgumentsDelta,
		relayapi.SSEResponseItemCompleted,
		relayapi.SSEResponseCompleted,
		relayapi.SSEError,
	} {
		if got := relayapi.TerminalSSEEvent(event); got != terminal[event] {
			t.Fatalf("TerminalSSEEvent(%q) = %v, want %v", event, got, terminal[event])
		}
	}
}

func TestSSEEventValidationRejectsEachRuleViolation(t *testing.T) {
	t.Parallel()

	// The added/completed pair pins the shell contract: an argument-less
	// function_call is a legal shell at item.added time but an illegal
	// complete item at item.completed time.
	shell := relayapi.Item{Type: relayapi.ItemTypeFunctionCall, CallID: "call_1", Name: "get_weather"}
	if err := (relayapi.ResponseItemAdded{OutputIndex: 0, Item: shell}).Validate(); err != nil {
		t.Fatalf("function_call shell must be a valid item.added payload: %v", err)
	}
	assertInvalid(t, relayapi.ResponseItemCompleted{OutputIndex: 0, Item: shell}.Validate(), "arguments: must be a JSON text")

	cases := []struct {
		name  string
		event interface{ Validate() error }
		want  string
	}{
		{"created with provider-shaped id", relayapi.ResponseCreated{ResponseID: "chatcmpl-abc123"}, "Speko-minted"},
		{"created with bare prefix", relayapi.ResponseCreated{ResponseID: "resp_"}, "Speko-minted"},
		{"item.added negative index", relayapi.ResponseItemAdded{OutputIndex: -1, Item: shell}, "output_index: must not be negative"},
		{"item.added shell with invalid partial arguments", relayapi.ResponseItemAdded{Item: relayapi.Item{Type: relayapi.ItemTypeFunctionCall, CallID: "call_1", Name: "get_weather", Arguments: "{"}}, "arguments: must be a JSON text"},
		// The output-side union narrowing holds for shells and completed
		// items alike: function_result items are input-only and output
		// messages are assistant-authored, so a connector cannot announce
		// either without failing contract validation.
		{"item.added with function_result", relayapi.ResponseItemAdded{Item: relayapi.Item{Type: relayapi.ItemTypeFunctionResult, CallID: "call_1"}}, "function_result items are input-only"},
		{"item.added with user message", relayapi.ResponseItemAdded{Item: relayapi.Item{Type: relayapi.ItemTypeMessage, Role: relayapi.RoleUser, Content: []relayapi.ContentPart{{Type: relayapi.ContentPartText, Text: "hi"}}}}, "output messages are assistant-authored"},
		{"item.completed with function_result", relayapi.ResponseItemCompleted{Item: relayapi.Item{Type: relayapi.ItemTypeFunctionResult, CallID: "call_1", Result: "ok"}}, "function_result items are input-only"},
		{"item.completed with system message", relayapi.ResponseItemCompleted{Item: relayapi.Item{Type: relayapi.ItemTypeMessage, Role: relayapi.RoleSystem, Content: []relayapi.ContentPart{{Type: relayapi.ContentPartText, Text: "hi"}}}}, "output messages are assistant-authored"},
		{"text delta empty", relayapi.ResponseTextDelta{OutputIndex: 0}, "delta: required"},
		{"text delta negative index", relayapi.ResponseTextDelta{OutputIndex: -1, Delta: "x"}, "output_index: must not be negative"},
		{"arguments delta empty", relayapi.ResponseFunctionCallArgumentsDelta{OutputIndex: 0}, "delta: required"},
		{"completed with unknown stop reason", relayapi.ResponseCompleted{StopReason: "length"}, "stop_reason: unsupported value"},
		{"completed with negative usage", relayapi.ResponseCompleted{StopReason: relayapi.StopReasonStop, Usage: relayapi.Usage{OutputTokens: -1}}, "must not be negative"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertInvalid(t, tc.event.Validate(), tc.want)
		})
	}
}
