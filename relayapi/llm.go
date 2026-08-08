package relayapi

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ItemType tags every entry of an LLM request input or response output. The
// union is closed and flat: exactly one variant's fields may be set, and
// fields from another variant are rejected rather than ignored.
type ItemType string

const (
	ItemTypeMessage        ItemType = "message"
	ItemTypeFunctionCall   ItemType = "function_call"
	ItemTypeFunctionResult ItemType = "function_result"
	// ItemTypeStructuredJSON carries the model's schema-conforming JSON when
	// the request asked for structured output. It is output-only:
	// LLMRequest.Validate rejects it in input, because resent history
	// represents earlier structured output as ordinary assistant text.
	ItemTypeStructuredJSON ItemType = "structured_json"
)

// MessageRole is the closed author set for message items. There is no tool
// role: tool traffic uses the dedicated function_call and function_result
// item types.
type MessageRole string

const (
	RoleSystem    MessageRole = "system"
	RoleUser      MessageRole = "user"
	RoleAssistant MessageRole = "assistant"
)

// ContentPartText is the only content part type at launch. The part list
// exists so richer parts can be added later without reshaping messages.
const ContentPartText = "text"

// ContentPart is one piece of a message's content.
type ContentPart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// Item is the tagged union entry of LLM input and output. Callers resend
// full conversation history on every request — including function_call and
// function_result items — because the relay stores no conversation state and
// exposes no previous_response_id.
//
// Arguments is JSON carried as a string, matching how function call
// arguments stream as text deltas. Result is opaque text: the relay passes
// it through without requiring JSON, and it may legitimately be empty —
// a function can succeed and have nothing to say. JSON is a raw structured
// value.
type Item struct {
	Type ItemType `json:"type"`
	// Message fields.
	Role    MessageRole   `json:"role,omitempty"`
	Content []ContentPart `json:"content,omitempty"`
	// Function call fields (CallID is shared with function_result: it links
	// a result to the call that produced it).
	CallID    string `json:"call_id,omitempty"`
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments,omitempty"`
	// Function result field.
	Result string `json:"result,omitempty"`
	// Structured output field.
	JSON json.RawMessage `json:"json,omitempty"`
}

// Validate checks a complete item: the variant's own rules plus the union
// discipline that no other variant's fields are set. Streaming uses a
// relaxed shell check at response.item.added time — see llm_stream.go —
// because deltas have not yet delivered arguments or structured JSON.
func (i Item) Validate() error {
	return i.validate(false)
}

func (i Item) validate(shell bool) error {
	switch i.Type {
	case ItemTypeMessage:
		if !validMessageRole(i.Role) {
			return fmt.Errorf("role: unsupported value %q", i.Role)
		}
		if len(i.Content) == 0 {
			return fmt.Errorf("content: at least one part is required")
		}
		for j, part := range i.Content {
			if part.Type != ContentPartText {
				return fmt.Errorf("content[%d].type: unsupported value %q", j, part.Type)
			}
		}
		if i.CallID != "" || i.Name != "" || i.Arguments != "" || i.Result != "" || len(i.JSON) > 0 {
			return fmt.Errorf("message items carry only role and content")
		}
	case ItemTypeFunctionCall:
		if strings.TrimSpace(i.CallID) == "" || strings.TrimSpace(i.Name) == "" {
			return fmt.Errorf("call_id and name are required")
		}
		if shell {
			if i.Arguments != "" && !json.Valid([]byte(i.Arguments)) {
				return fmt.Errorf("arguments: must be a JSON text")
			}
		} else if !json.Valid([]byte(i.Arguments)) {
			return fmt.Errorf("arguments: must be a JSON text")
		}
		if i.Role != "" || len(i.Content) > 0 || i.Result != "" || len(i.JSON) > 0 {
			return fmt.Errorf("function_call items carry only call_id, name, and arguments")
		}
	case ItemTypeFunctionResult:
		if strings.TrimSpace(i.CallID) == "" {
			return fmt.Errorf("call_id: required")
		}
		if i.Role != "" || len(i.Content) > 0 || i.Name != "" || i.Arguments != "" || len(i.JSON) > 0 {
			return fmt.Errorf("function_result items carry only call_id and result")
		}
	case ItemTypeStructuredJSON:
		if shell {
			if len(i.JSON) > 0 && !json.Valid(i.JSON) {
				return fmt.Errorf("json: must be a JSON value")
			}
		} else if len(i.JSON) == 0 || !json.Valid(i.JSON) {
			return fmt.Errorf("json: must be a JSON value")
		}
		if i.Role != "" || len(i.Content) > 0 || i.CallID != "" || i.Name != "" || i.Arguments != "" || i.Result != "" {
			return fmt.Errorf("structured_json items carry only json")
		}
	default:
		return fmt.Errorf("type: unsupported value %q", i.Type)
	}
	return nil
}

// validateOutputItem checks an item on the output side of the union: the
// item's own rules plus the narrowing that mirrors the input-side
// structured_json rejection. Output is server-emitted, so a connector
// normalization bug that mislabels an item — a function_result that only
// callers may send, or a message claiming a non-assistant author — must fail
// loudly here instead of validating as a plausible input shape.
func validateOutputItem(i Item, shell bool) error {
	if i.Type == ItemTypeFunctionResult {
		return fmt.Errorf("type: function_result items are input-only")
	}
	if err := i.validate(shell); err != nil {
		return err
	}
	if i.Type == ItemTypeMessage && i.Role != RoleAssistant {
		return fmt.Errorf("role: output messages are assistant-authored, got %q", i.Role)
	}
	return nil
}

// FunctionTool declares a caller-defined function the model may call.
// Parameters is the function's JSON Schema; the relay passes it through and
// enforces only that it is well-formed JSON — schema semantics belong to the
// provider. Tools are admitted only on models advertising the capability.
type FunctionTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

// Validate checks the tool declaration.
func (t FunctionTool) Validate() error {
	if strings.TrimSpace(t.Name) == "" {
		return fmt.Errorf("name: required")
	}
	if len(t.Parameters) > 0 && !json.Valid(t.Parameters) {
		return fmt.Errorf("parameters: must be a JSON value")
	}
	return nil
}

// ResponseFormatJSONSchema is the only response format type. The field is an
// enum of one so json_object-style loose modes can be added deliberately,
// never by accident.
const ResponseFormatJSONSchema = "json_schema"

// ResponseFormat requests schema-conforming structured output. It is
// admitted only on models advertising structured output support.
type ResponseFormat struct {
	Type   string          `json:"type"`
	Name   string          `json:"name"`
	Schema json.RawMessage `json:"schema"`
	Strict bool            `json:"strict,omitempty"`
}

// Validate checks the format declaration.
func (f ResponseFormat) Validate() error {
	if f.Type != ResponseFormatJSONSchema {
		return fmt.Errorf("type: got %q, want %q", f.Type, ResponseFormatJSONSchema)
	}
	if strings.TrimSpace(f.Name) == "" {
		return fmt.Errorf("name: required")
	}
	if len(f.Schema) == 0 || !json.Valid(f.Schema) {
		return fmt.Errorf("schema: must be a JSON value")
	}
	return nil
}

// LLMRequest is the POST /v1/llm/responses body. MaxOutputTokens is
// required: the relay reserves credit for the worst-case generation before
// dispatch, and an unbounded generation cannot be priced.
type LLMRequest struct {
	Routing         Routing         `json:"routing"`
	Input           []Item          `json:"input"`
	Tools           []FunctionTool  `json:"tools,omitempty"`
	ResponseFormat  *ResponseFormat `json:"response_format,omitempty"`
	MaxOutputTokens int64           `json:"max_output_tokens"`
	Temperature     *float64        `json:"temperature,omitempty"`
	TopP            *float64        `json:"top_p,omitempty"`
	Stream          bool            `json:"stream,omitempty"`
}

// Validate checks the full request contract. Callers normalize Routing with
// NormalizeDefault first — an omitted routing object defaults to
// {mode: auto, objective: balanced}.
func (r LLMRequest) Validate() error {
	if err := r.Routing.Validate(); err != nil {
		return fmt.Errorf("routing: %w", err)
	}
	if len(r.Input) == 0 {
		return fmt.Errorf("input: at least one item is required")
	}
	for i, item := range r.Input {
		if item.Type == ItemTypeStructuredJSON {
			return fmt.Errorf("input[%d].type: structured_json items are output-only", i)
		}
		if err := item.Validate(); err != nil {
			return fmt.Errorf("input[%d]: %w", i, err)
		}
	}
	seen := make(map[string]struct{}, len(r.Tools))
	for i, tool := range r.Tools {
		if err := tool.Validate(); err != nil {
			return fmt.Errorf("tools[%d]: %w", i, err)
		}
		if _, dup := seen[tool.Name]; dup {
			return fmt.Errorf("tools[%d].name: duplicate tool name %q", i, tool.Name)
		}
		seen[tool.Name] = struct{}{}
	}
	if r.ResponseFormat != nil {
		if err := r.ResponseFormat.Validate(); err != nil {
			return fmt.Errorf("response_format: %w", err)
		}
	}
	if r.MaxOutputTokens <= 0 {
		return fmt.Errorf("max_output_tokens: positive value is required")
	}
	if r.Temperature != nil && (*r.Temperature < 0 || *r.Temperature > 2) {
		return fmt.Errorf("temperature: must be between 0 and 2")
	}
	if r.TopP != nil && (*r.TopP <= 0 || *r.TopP > 1) {
		return fmt.Errorf("top_p: must be greater than 0 and at most 1")
	}
	return nil
}

// StopReason explains why generation ended. The set is closed.
type StopReason string

const (
	StopReasonStop            StopReason = "stop"
	StopReasonMaxOutputTokens StopReason = "max_output_tokens"
	StopReasonToolCall        StopReason = "tool_call"
)

// ResponseIDPrefix starts every LLM response id. Response ids are
// Speko-minted — resp_<request-id> — so output never depends on or reveals a
// provider's response id; provider response and conversation ids are
// captured only as content-free telemetry evidence.
const ResponseIDPrefix = "resp_"

// ResponseID mints the public LLM response id for a relay request.
func ResponseID(requestID string) string {
	return ResponseIDPrefix + requestID
}

// LLMResponse is the non-streaming POST /v1/llm/responses body. There is no
// previous_response_id to feed it back into: callers resend full history.
type LLMResponse struct {
	ID         string     `json:"id"`
	Route      Route      `json:"route"`
	Output     []Item     `json:"output"`
	StopReason StopReason `json:"stop_reason"`
	Usage      Usage      `json:"usage"`
}

// Validate checks the full response contract, including that the id carries
// the Speko-minted prefix.
func (r LLMResponse) Validate() error {
	if !strings.HasPrefix(r.ID, ResponseIDPrefix) || len(r.ID) == len(ResponseIDPrefix) {
		return fmt.Errorf("id: must be a Speko-minted response id (%s<request-id>)", ResponseIDPrefix)
	}
	if err := r.Route.Validate(); err != nil {
		return fmt.Errorf("route: %w", err)
	}
	if len(r.Output) == 0 {
		return fmt.Errorf("output: at least one item is required")
	}
	for i, item := range r.Output {
		if err := validateOutputItem(item, false); err != nil {
			return fmt.Errorf("output[%d]: %w", i, err)
		}
	}
	if !validStopReason(r.StopReason) {
		return fmt.Errorf("stop_reason: unsupported value %q", r.StopReason)
	}
	if err := r.Usage.Validate(); err != nil {
		return fmt.Errorf("usage: %w", err)
	}
	return nil
}

func validMessageRole(v MessageRole) bool {
	return v == RoleSystem || v == RoleUser || v == RoleAssistant
}

func validStopReason(v StopReason) bool {
	return v == StopReasonStop || v == StopReasonMaxOutputTokens || v == StopReasonToolCall
}
