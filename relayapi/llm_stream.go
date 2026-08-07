package relayapi

import "fmt"

// SSE event names for streaming POST /v1/llm/responses. The set is closed.
//
// Every stream carries EXACTLY ONE terminal event: response.completed on
// success or error on failure — never both and never zero, so a client that
// sees the socket close without a terminal event knows the stream was
// truncated rather than finished. See TerminalSSEEvent.
const (
	SSEResponseCreated                    = "response.created"
	SSEResponseItemAdded                  = "response.item.added"
	SSEResponseTextDelta                  = "response.text.delta"
	SSEResponseFunctionCallArgumentsDelta = "response.function_call_arguments.delta"
	SSEResponseItemCompleted              = "response.item.completed"
	SSEResponseCompleted                  = "response.completed"
	SSEError                              = "error"
)

// StreamEvent is implemented by every SSE data payload. Event returns the
// SSE event name the payload travels under.
type StreamEvent interface {
	Event() string
}

// TerminalSSEEvent reports whether the named event ends a stream. Exactly
// one terminal event ends every stream.
func TerminalSSEEvent(name string) bool {
	return name == SSEResponseCompleted || name == SSEError
}

// ResponseCreated opens every stream. ResponseID is Speko-minted —
// ResponseID(request_id), never a provider identifier — and matches the ID
// a non-streaming call would have returned.
type ResponseCreated struct {
	ResponseID string `json:"response_id"`
}

// Event returns the SSE event name.
func (ResponseCreated) Event() string { return SSEResponseCreated }

// Validate checks that the id carries the Speko-minted prefix.
func (e ResponseCreated) Validate() error {
	if len(e.ResponseID) <= len(ResponseIDPrefix) || e.ResponseID[:len(ResponseIDPrefix)] != ResponseIDPrefix {
		return fmt.Errorf("response_id: must be a Speko-minted response id (%s<request-id>)", ResponseIDPrefix)
	}
	return nil
}

// ResponseItemAdded announces a new output item at OutputIndex. The item is
// a shell: its streamed content (message text, function call arguments,
// structured JSON) arrives through subsequent delta events and is complete
// only in response.item.completed.
type ResponseItemAdded struct {
	OutputIndex int  `json:"output_index"`
	Item        Item `json:"item"`
}

// Event returns the SSE event name.
func (ResponseItemAdded) Event() string { return SSEResponseItemAdded }

// Validate checks the index and the item shell. The shell check relaxes
// only the not-yet-streamed content; identity fields must be final, and the
// shell obeys the output-side union narrowing — no function_result shells,
// no non-assistant messages.
func (e ResponseItemAdded) Validate() error {
	if e.OutputIndex < 0 {
		return fmt.Errorf("output_index: must not be negative")
	}
	if err := validateOutputItem(e.Item, true); err != nil {
		return fmt.Errorf("item: %w", err)
	}
	return nil
}

// ResponseTextDelta appends text to the message or structured_json item at
// OutputIndex.
type ResponseTextDelta struct {
	OutputIndex int    `json:"output_index"`
	Delta       string `json:"delta"`
}

// Event returns the SSE event name.
func (ResponseTextDelta) Event() string { return SSEResponseTextDelta }

// Validate checks the index and that the delta says something — empty
// provider deltas are dropped at normalization, never forwarded.
func (e ResponseTextDelta) Validate() error {
	if e.OutputIndex < 0 {
		return fmt.Errorf("output_index: must not be negative")
	}
	if e.Delta == "" {
		return fmt.Errorf("delta: required")
	}
	return nil
}

// ResponseFunctionCallArgumentsDelta appends argument text to the
// function_call item at OutputIndex. The concatenated deltas form the JSON
// arguments finalized in response.item.completed.
type ResponseFunctionCallArgumentsDelta struct {
	OutputIndex int    `json:"output_index"`
	Delta       string `json:"delta"`
}

// Event returns the SSE event name.
func (ResponseFunctionCallArgumentsDelta) Event() string {
	return SSEResponseFunctionCallArgumentsDelta
}

// Validate checks the index and that the delta says something — empty
// provider deltas are dropped at normalization, never forwarded.
func (e ResponseFunctionCallArgumentsDelta) Validate() error {
	if e.OutputIndex < 0 {
		return fmt.Errorf("output_index: must not be negative")
	}
	if e.Delta == "" {
		return fmt.Errorf("delta: required")
	}
	return nil
}

// ResponseItemCompleted carries the finalized item at OutputIndex — the
// same value a non-streaming response would contain at that position.
type ResponseItemCompleted struct {
	OutputIndex int  `json:"output_index"`
	Item        Item `json:"item"`
}

// Event returns the SSE event name.
func (ResponseItemCompleted) Event() string { return SSEResponseItemCompleted }

// Validate checks the index and the complete item under the output-side
// union narrowing — the same rules a non-streaming response's output obeys.
func (e ResponseItemCompleted) Validate() error {
	if e.OutputIndex < 0 {
		return fmt.Errorf("output_index: must not be negative")
	}
	if err := validateOutputItem(e.Item, false); err != nil {
		return fmt.Errorf("item: %w", err)
	}
	return nil
}

// ResponseCompleted is the success terminal event, carrying the stop reason
// and the authoritative usage for the whole response.
type ResponseCompleted struct {
	StopReason StopReason `json:"stop_reason"`
	Usage      Usage      `json:"usage"`
}

// Event returns the SSE event name.
func (ResponseCompleted) Event() string { return SSEResponseCompleted }

// Validate checks the stop reason and usage.
func (e ResponseCompleted) Validate() error {
	if !validStopReason(e.StopReason) {
		return fmt.Errorf("stop_reason: unsupported value %q", e.StopReason)
	}
	if err := e.Usage.Validate(); err != nil {
		return fmt.Errorf("usage: %w", err)
	}
	return nil
}

// Event returns the SSE event name for the failure terminal event. The HTTP
// error envelope doubles as the SSE error payload so both transports share
// one normalized error shape.
func (ErrorEnvelope) Event() string { return SSEError }
