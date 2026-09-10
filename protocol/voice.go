package protocol

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"
)

// SpeechProtocol names the native wire protocol a speech-to-speech model
// speaks. It is the dispatch key that lets two OpenAI voice adapters coexist
// under one provider: gpt-realtime-* models speak the Realtime protocol on
// wss://api.openai.com/v1/realtime, while gpt-live-1 speaks the Live protocol
// on wss://api.openai.com/v1/live/sessions. Catalog rows carry the protocol,
// signed relay plans assert it, connectors verify it before touching a
// credential, and the Router exposes the OpenAI protocols natively on their
// own public routes.
type SpeechProtocol string

const (
	// SpeechProtocolOpenAIRealtimeV1 is the OpenAI Realtime event protocol
	// (input_audio_buffer.append, response.output_audio.delta, ...). Public
	// route: GET /v1/realtime?model=<id>.
	SpeechProtocolOpenAIRealtimeV1 SpeechProtocol = "openai.realtime.v1"
	// SpeechProtocolOpenAILiveV1 is the GPT-Live event protocol
	// (session.start, session.input_audio.append, session.output_audio.delta,
	// delegation and response.event envelopes). Public route: GET /v1/live;
	// the model rides the initial session.start frame.
	SpeechProtocolOpenAILiveV1 SpeechProtocol = "openai.live.v1"
	// SpeechProtocolGoogleLiveV1 is Gemini Live (BidiGenerateContent). It has
	// no public Router route; the protocol exists so relay plans for every
	// s2s catalog row can assert what their connector speaks.
	SpeechProtocolGoogleLiveV1 SpeechProtocol = "google.live.v1"
	// SpeechProtocolXAIRealtimeV1 is xAI Grok Voice, a Realtime-shaped
	// protocol with its own session body. No public Router route.
	SpeechProtocolXAIRealtimeV1 SpeechProtocol = "xai.realtime.v1"
)

// ValidSpeechProtocol reports whether p names a known speech protocol.
func ValidSpeechProtocol(p SpeechProtocol) bool {
	switch p {
	case SpeechProtocolOpenAIRealtimeV1, SpeechProtocolOpenAILiveV1, SpeechProtocolGoogleLiveV1, SpeechProtocolXAIRealtimeV1:
		return true
	}
	return false
}

// PublicRoute returns the Router route path that exposes this protocol
// natively, or "" for protocols the Router does not expose.
func (p SpeechProtocol) PublicRoute() string {
	switch p {
	case SpeechProtocolOpenAIRealtimeV1:
		return "/v1/realtime"
	case SpeechProtocolOpenAILiveV1:
		return "/v1/live"
	}
	return ""
}

// Live session configuration bounds. They bound what a caller can place on
// the wire, not what the vendor accepts: every one is comfortably above the
// documented vendor limit (16,384 instruction tokens, 500-token appends) and
// exists so a session request can be hashed, validated, and forwarded without
// buffering an unbounded document.
const (
	MaxLiveInstructionsBytes        = 96 << 10
	MaxLiveBackendInstructionsBytes = 96 << 10
	MaxLiveHistoryItems             = 256
	MaxLiveHistoryBytes             = 256 << 10
	MaxLiveHistoryItemBytes         = 32 << 10
	MaxLiveTools                    = 64
	MaxLiveToolBytes                = 64 << 10
	MaxLiveToolChoiceBytes          = 4 << 10
	MaxLiveSettingBytes             = 4 << 10
	MinLiveMaxOutputTokens          = 16
	MaxLiveMaxOutputTokens          = 131_072
	// MaxLiveContextAppendBytes bounds the content of one
	// session.{instructions,thinking,commentary}.append. The vendor caps an
	// append at 500 tokens; 16 KiB of UTF-8 is far above that in every
	// script, so the bound rejects only abuse, never a legitimate append.
	MaxLiveContextAppendBytes = 16 << 10
)

// Hosted accounting bounds for a GPT-Live Responses backend. The Router
// reserves backend exposure per response — in-flight work included — from
// these bounds and the session's max_output_tokens, and refuses further
// continuations or configuration changes past the count limits. They are
// shared here so the edge that sizes a reservation and the connector that
// guards it can never disagree.
const (
	// LiveBackendInputTokensPerResponse bounds the context GPT-Live supplies
	// to one backend response: the replacement-engine history cap (8,192
	// tokens) plus instructions, tools, and appended context.
	LiveBackendInputTokensPerResponse int64 = 16_384
	// LiveBackendToolCallsPerResponse bounds billable hosted tool calls one
	// backend response may perform.
	LiveBackendToolCallsPerResponse int64 = 8
	// LiveBackendResponsesPerSlice is how many responses one admission or
	// top-up slice funds.
	LiveBackendResponsesPerSlice int64 = 2
	// MaxLiveContinuations caps response.create commands per session.
	MaxLiveContinuations = 64
	// MaxLiveConfigurationChanges caps session.update commands per session.
	MaxLiveConfigurationChanges = 32
)

// LiveDelegationType selects who runs delegated work for a GPT-Live session.
type LiveDelegationType string

const (
	// LiveDelegationClient hands delegations to the application: the model
	// emits session.delegation.created and the caller answers with
	// session.commentary.append / session.thinking.append. The application
	// retains its own conversation and task state. This is the mode an
	// omitted or null delegation selects.
	LiveDelegationClient LiveDelegationType = "client"
	// LiveDelegationResponses lets GPT-Live call a Responses backend of the
	// caller's choosing. The backend model is required; function results and
	// continuations flow through response.item.create / response.create.
	LiveDelegationResponses LiveDelegationType = "responses"
)

// LiveOptions is the bounded GPT-Live session configuration beyond the voice,
// instructions, and media that every speech-to-speech session carries on
// S2SOptions. The voice instructions (S2SOptions.Instructions) steer the live
// model; backend instructions live on Delegation.Responses and are never
// merged into them.
type LiveOptions struct {
	// History seeds the session with prior text turns (the vendor's session
	// `input`). Append-only afterwards: a running session takes context
	// through the append commands, never a replacement history.
	History []LiveHistoryItem `json:"history,omitempty"`
	// Delegation selects the delegation mode. Nil selects client delegation.
	Delegation *LiveDelegation `json:"delegation,omitempty"`
}

// LiveHistoryItem is one prior text message.
type LiveHistoryItem struct {
	Role string `json:"role"`
	Text string `json:"text"`
}

// LiveDelegation names the delegation mode and, for Responses delegation, the
// backend configuration.
type LiveDelegation struct {
	Type      LiveDelegationType   `json:"type"`
	Responses *LiveResponsesConfig `json:"responses,omitempty"`
}

// LiveResponsesConfig configures the Responses backend GPT-Live delegates to.
// Model is required. Tools may be function definitions or web_search entries
// in the Responses tool schema; ToolChoice, Reasoning, and Text are forwarded
// verbatim within their size bounds because their shapes belong to the
// backend model, not to this contract.
type LiveResponsesConfig struct {
	Model             string          `json:"model"`
	Instructions      string          `json:"instructions,omitempty"`
	Tools             []LiveTool      `json:"tools,omitempty"`
	ToolChoice        json.RawMessage `json:"tool_choice,omitempty"`
	ParallelToolCalls *bool           `json:"parallel_tool_calls,omitempty"`
	MaxOutputTokens   int             `json:"max_output_tokens,omitempty"`
	ServiceTier       string          `json:"service_tier,omitempty"`
	Reasoning         json.RawMessage `json:"reasoning,omitempty"`
	Text              json.RawMessage `json:"text,omitempty"`
}

// LiveToolType is the tool kinds GPT-Live Responses delegation supports.
type LiveToolType string

const (
	LiveToolFunction  LiveToolType = "function"
	LiveToolWebSearch LiveToolType = "web_search"
)

// LiveTool is one backend tool declaration kept verbatim: the function schema
// is customer content the backend interprets, so only the type tag and the
// function name are read here.
type LiveTool struct {
	raw json.RawMessage
}

// NewLiveTool wraps a raw Responses tool object.
func NewLiveTool(raw json.RawMessage) LiveTool {
	return LiveTool{raw: append(json.RawMessage(nil), raw...)}
}

// Raw returns the tool declaration bytes.
func (t LiveTool) Raw() json.RawMessage { return append(json.RawMessage(nil), t.raw...) }

// Type returns the tool type tag ("function" or "web_search").
func (t LiveTool) Type() LiveToolType {
	var tagged struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(t.raw, &tagged)
	return LiveToolType(strings.TrimSpace(tagged.Type))
}

// Name returns the function name for function tools, "" otherwise.
func (t LiveTool) Name() string {
	var tagged struct {
		Name string `json:"name"`
	}
	_ = json.Unmarshal(t.raw, &tagged)
	return strings.TrimSpace(tagged.Name)
}

// MarshalJSON writes the tool verbatim.
func (t LiveTool) MarshalJSON() ([]byte, error) {
	if len(t.raw) == 0 {
		return []byte("null"), nil
	}
	return append([]byte(nil), t.raw...), nil
}

// UnmarshalJSON keeps the tool verbatim.
func (t *LiveTool) UnmarshalJSON(data []byte) error {
	t.raw = append(json.RawMessage(nil), data...)
	return nil
}

// Validate checks the bounded tool declaration: a JSON object with a
// supported type tag, and a function name for function tools.
func (t LiveTool) Validate() error {
	if len(t.raw) > MaxLiveToolBytes {
		return fmt.Errorf("tool exceeds %d bytes", MaxLiveToolBytes)
	}
	if !isJSONObject(t.raw) {
		return fmt.Errorf("tool must be a JSON object")
	}
	switch t.Type() {
	case LiveToolFunction:
		if t.Name() == "" {
			return fmt.Errorf("function tool requires a name")
		}
	case LiveToolWebSearch:
	default:
		return fmt.Errorf("tool type %q is not supported (function or web_search)", t.Type())
	}
	return nil
}

// Validate checks the bounded GPT-Live configuration.
func (o LiveOptions) Validate() error {
	if len(o.History) > MaxLiveHistoryItems {
		return fmt.Errorf("history: at most %d items", MaxLiveHistoryItems)
	}
	total := 0
	for i, item := range o.History {
		if err := item.Validate(); err != nil {
			return fmt.Errorf("history[%d]: %w", i, err)
		}
		total += len(item.Text)
	}
	if total > MaxLiveHistoryBytes {
		return fmt.Errorf("history: at most %d bytes in total", MaxLiveHistoryBytes)
	}
	if o.Delegation != nil {
		if err := o.Delegation.Validate(); err != nil {
			return fmt.Errorf("delegation: %w", err)
		}
	}
	return nil
}

// Validate checks one history item.
func (h LiveHistoryItem) Validate() error {
	switch h.Role {
	case "user", "assistant", "system", "developer":
	default:
		return fmt.Errorf("role: must be user, assistant, system, or developer")
	}
	if strings.TrimSpace(h.Text) == "" {
		return fmt.Errorf("text: required")
	}
	if len(h.Text) > MaxLiveHistoryItemBytes {
		return fmt.Errorf("text: at most %d bytes", MaxLiveHistoryItemBytes)
	}
	if !utf8.ValidString(h.Text) {
		return fmt.Errorf("text: must be valid UTF-8")
	}
	return nil
}

// Mode returns the effective delegation type: an absent delegation is
// client delegation.
func (d *LiveDelegation) Mode() LiveDelegationType {
	if d == nil || d.Type == "" {
		return LiveDelegationClient
	}
	return d.Type
}

// Validate checks the delegation. Responses delegation requires an explicit
// backend model; client delegation carries no backend configuration.
func (d LiveDelegation) Validate() error {
	switch d.Type {
	case LiveDelegationClient:
		if d.Responses != nil {
			return fmt.Errorf("responses: not valid for client delegation")
		}
	case LiveDelegationResponses:
		if d.Responses == nil {
			return fmt.Errorf("responses: required for responses delegation")
		}
		if err := d.Responses.Validate(); err != nil {
			return fmt.Errorf("responses: %w", err)
		}
	default:
		return fmt.Errorf("type: must be client or responses")
	}
	return nil
}

// Validate checks the Responses backend configuration.
func (c LiveResponsesConfig) Validate() error {
	model := strings.TrimSpace(c.Model)
	if model == "" || model == "auto" {
		return fmt.Errorf("model: an explicit backend model is required")
	}
	if len(model) > 128 || strings.ContainsAny(model, " \t\r\n/\\") {
		return fmt.Errorf("model: invalid model id")
	}
	if len(c.Instructions) > MaxLiveBackendInstructionsBytes {
		return fmt.Errorf("instructions: at most %d bytes", MaxLiveBackendInstructionsBytes)
	}
	if !utf8.ValidString(c.Instructions) {
		return fmt.Errorf("instructions: must be valid UTF-8")
	}
	if len(c.Tools) > MaxLiveTools {
		return fmt.Errorf("tools: at most %d tools", MaxLiveTools)
	}
	names := make(map[string]bool, len(c.Tools))
	for i, tool := range c.Tools {
		if err := tool.Validate(); err != nil {
			return fmt.Errorf("tools[%d]: %w", i, err)
		}
		if name := tool.Name(); name != "" {
			if names[name] {
				return fmt.Errorf("tools[%d]: duplicate function name %q", i, name)
			}
			names[name] = true
		}
	}
	if len(c.ToolChoice) > 0 {
		if len(c.ToolChoice) > MaxLiveToolChoiceBytes || !json.Valid(c.ToolChoice) {
			return fmt.Errorf("tool_choice: must be valid JSON within %d bytes", MaxLiveToolChoiceBytes)
		}
	}
	if c.MaxOutputTokens != 0 && (c.MaxOutputTokens < MinLiveMaxOutputTokens || c.MaxOutputTokens > MaxLiveMaxOutputTokens) {
		return fmt.Errorf("max_output_tokens: must be between %d and %d when set", MinLiveMaxOutputTokens, MaxLiveMaxOutputTokens)
	}
	switch c.ServiceTier {
	case "", "auto", "default", "flex", "priority":
	default:
		return fmt.Errorf("service_tier: must be auto, default, flex, or priority")
	}
	for name, raw := range map[string]json.RawMessage{"reasoning": c.Reasoning, "text": c.Text} {
		if len(raw) == 0 {
			continue
		}
		if len(raw) > MaxLiveSettingBytes || !isJSONObject(raw) {
			return fmt.Errorf("%s: must be a JSON object within %d bytes", name, MaxLiveSettingBytes)
		}
	}
	return nil
}

// ToolTypes returns the distinct tool types the backend declares, in
// declaration order. Managed admission prices each of them.
func (c LiveResponsesConfig) ToolTypes() []LiveToolType {
	seen := make(map[LiveToolType]bool, len(c.Tools))
	var types []LiveToolType
	for _, tool := range c.Tools {
		kind := tool.Type()
		if seen[kind] {
			continue
		}
		seen[kind] = true
		types = append(types, kind)
	}
	return types
}

func isJSONObject(raw json.RawMessage) bool {
	trimmed := strings.TrimSpace(string(raw))
	return json.Valid(raw) && strings.HasPrefix(trimmed, "{")
}

// Provider control and event envelopes.
//
// A speech-to-speech session forwards a BOUNDED subset of the vendor's native
// client events upstream (ProviderControl) and surfaces the vendor's native
// server events downstream (ProviderEvent). Payloads are the native JSON
// objects verbatim so native identifiers — event_id, delegation_id,
// response_id, call_id, item_id — and nested Responses events survive the
// hop untouched. The type tag is duplicated outside the payload so a hop can
// route without parsing customer content.

const (
	// MaxProviderControlBytes bounds one forwarded native command.
	MaxProviderControlBytes = 64 << 10
	// MaxProviderEventBytes bounds one surfaced native event, including a
	// base64 audio delta.
	MaxProviderEventBytes = 2 << 20
)

// ProviderControl is one native client command forwarded on a
// speech-to-speech session.
type ProviderControl struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// ProviderEvent is one native server event surfaced from a speech-to-speech
// session. It rides protocol.Event.Data under EventProviderEvent.
type ProviderEvent struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// Live client commands the hop forwards. Audio appends and session.start /
// session.close are NOT controls: audio rides the media path and the session
// lifecycle is owned by the hop.
var liveProviderControls = map[string]bool{
	"session.update":              true,
	"session.instructions.append": true,
	"session.thinking.append":     true,
	"session.commentary.append":   true,
	"session.input_audio.mute":    true,
	"session.input_audio.unmute":  true,
	"response.item.create":        true,
	"response.create":             true,
}

// Realtime client commands the hop forwards. input_audio_buffer.append is the
// media path; session.update is forwarded after the hop pins model and audio
// format.
var realtimeProviderControls = map[string]bool{
	"session.update":             true,
	"input_audio_buffer.commit":  true,
	"input_audio_buffer.clear":   true,
	"conversation.item.create":   true,
	"conversation.item.retrieve": true,
	"conversation.item.truncate": true,
	"conversation.item.delete":   true,
	"response.create":            true,
	"response.cancel":            true,
	"output_audio_buffer.clear":  true,
}

// ProviderControlAllowed reports whether a native command type may be
// forwarded on a session of the given protocol. A Realtime-only control on a
// Live session (input_audio_buffer.commit, response.cancel) and a Live-only
// control on a Realtime session are both refused.
func ProviderControlAllowed(protocol SpeechProtocol, controlType string) bool {
	switch protocol {
	case SpeechProtocolOpenAILiveV1:
		return liveProviderControls[controlType]
	case SpeechProtocolOpenAIRealtimeV1, SpeechProtocolXAIRealtimeV1:
		return realtimeProviderControls[controlType]
	}
	return false
}

// ProviderControlTypes lists the forwardable command types for a protocol.
func ProviderControlTypes(protocol SpeechProtocol) []string {
	var table map[string]bool
	switch protocol {
	case SpeechProtocolOpenAILiveV1:
		table = liveProviderControls
	case SpeechProtocolOpenAIRealtimeV1, SpeechProtocolXAIRealtimeV1:
		table = realtimeProviderControls
	}
	types := make([]string, 0, len(table))
	for name := range table {
		types = append(types, name)
	}
	sortStrings(types)
	return types
}

// Validate checks the envelope against a protocol: allowed type, a JSON
// object payload within bounds whose own type tag matches the envelope.
func (c ProviderControl) Validate(protocol SpeechProtocol) error {
	if strings.TrimSpace(c.Type) == "" {
		return fmt.Errorf("type: required")
	}
	if !ProviderControlAllowed(protocol, c.Type) {
		return fmt.Errorf("type: %q is not a forwardable control on %s sessions", c.Type, protocol)
	}
	if len(c.Payload) == 0 || len(c.Payload) > MaxProviderControlBytes {
		return fmt.Errorf("payload: must be present and at most %d bytes", MaxProviderControlBytes)
	}
	if !isJSONObject(c.Payload) {
		return fmt.Errorf("payload: must be a JSON object")
	}
	var tagged struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(c.Payload, &tagged); err != nil {
		return fmt.Errorf("payload: %w", err)
	}
	if tagged.Type != c.Type {
		return fmt.Errorf("payload: type tag %q does not match envelope type %q", tagged.Type, c.Type)
	}
	return nil
}

// Validate checks a surfaced event envelope.
func (e ProviderEvent) Validate() error {
	if strings.TrimSpace(e.Type) == "" {
		return fmt.Errorf("type: required")
	}
	if len(e.Payload) == 0 || len(e.Payload) > MaxProviderEventBytes {
		return fmt.Errorf("payload: must be present and at most %d bytes", MaxProviderEventBytes)
	}
	if !isJSONObject(e.Payload) {
		return fmt.Errorf("payload: must be a JSON object")
	}
	return nil
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j-1] > values[j]; j-- {
			values[j-1], values[j] = values[j], values[j-1]
		}
	}
}

// VoiceSessionConfigure rides the connector handshake of a native
// speech-to-speech relay session (the Router's /v1/realtime and /v1/live).
// The edge builds it from what it admitted — the protocol, both audio
// directions, the resolved voice, the voice instructions, and for GPT-Live
// the bounded history and delegation — and the connector opens the provider
// from it immediately after verification. The connector refuses a configure
// whose protocol differs from the verified plan's protocol claim.
type VoiceSessionConfigure struct {
	Protocol     SpeechProtocol `json:"protocol"`
	Media        MediaFormat    `json:"media"`
	OutputMedia  MediaFormat    `json:"output_media"`
	Voice        string         `json:"voice,omitempty"`
	Instructions string         `json:"instructions,omitempty"`
	Live         *LiveOptions   `json:"live,omitempty"`
}

// Validate checks the configure document.
func (c VoiceSessionConfigure) Validate() error {
	if !ValidSpeechProtocol(c.Protocol) {
		return fmt.Errorf("protocol: unsupported value %q", c.Protocol)
	}
	if err := c.Media.Validate(); err != nil {
		return fmt.Errorf("media: %w", err)
	}
	if err := c.OutputMedia.Validate(); err != nil {
		return fmt.Errorf("output_media: %w", err)
	}
	if len(c.Voice) > 128 {
		return fmt.Errorf("voice: at most 128 bytes")
	}
	if len(c.Instructions) > MaxLiveInstructionsBytes {
		return fmt.Errorf("instructions: at most %d bytes", MaxLiveInstructionsBytes)
	}
	if c.Live != nil {
		if c.Protocol != SpeechProtocolOpenAILiveV1 {
			return fmt.Errorf("live: valid only for %s", SpeechProtocolOpenAILiveV1)
		}
		if err := c.Live.Validate(); err != nil {
			return fmt.Errorf("live: %w", err)
		}
	}
	return nil
}

// Options converts the configure into the adapter request options the
// speech adapters read.
func (c VoiceSessionConfigure) Options() RequestOptions {
	output := c.OutputMedia
	return RequestOptions{
		Voice: c.Voice,
		S2S:   &S2SOptions{Instructions: c.Instructions, OutputMedia: &output, Live: c.Live},
	}
}
