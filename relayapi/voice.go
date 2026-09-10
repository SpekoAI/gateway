package relayapi

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"
)

// Public speech-to-speech routes. Both sockets speak the vendor's NATIVE JSON
// event protocol: the Router authenticates, admits, meters, and forwards, but
// it does not translate events. What the Router adds is bounded: it pins the
// model and audio format an admitted session may use, refuses commands that
// belong to the other protocol, and emits its own typed error frame (the
// ErrorEvent shape shared with every streaming route) when the relay — not
// the vendor — ends a session.
const (
	// RealtimeRoutePath serves the OpenAI Realtime protocol. The model is an
	// exact query parameter: GET /v1/realtime?model=gpt-realtime-2.1. The
	// idempotency content hash covers the model query value.
	RealtimeRoutePath = "/v1/realtime"
	// LiveRoutePath serves the GPT-Live protocol. The first text frame MUST be
	// session.start naming the model; its exact frame bytes are the
	// idempotency content hash.
	LiveRoutePath = "/v1/live"
	// RealtimeModelQueryParam is the /v1/realtime model selector.
	RealtimeModelQueryParam = "model"
	// LiveSessionStartType is the type tag of the first /v1/live frame.
	LiveSessionStartType = "session.start"
)

// Live session bounds. They mirror the protocol package bounds (which this
// standard-library-only package cannot import) and exist so a session.start
// frame is validated before anything is hashed, admitted, or forwarded.
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
)

// LiveAudioSampleRates are the PCM16 rates the Router transports for GPT-Live
// at launch. One format applies to both directions, as in the vendor
// protocol. G.711 μ-law and A-law are refused: the Router carries raw PCM.
var LiveAudioSampleRates = []int{16_000, 24_000}

// LiveSessionStart is the first text frame of a GET /v1/live session: the
// native GPT-Live session.start command. Unknown session fields are refused
// so the Router forwards exactly what it validated.
type LiveSessionStart struct {
	Type    string            `json:"type"`
	EventID string            `json:"event_id,omitempty"`
	Session LiveSessionConfig `json:"session"`
}

// LiveSessionConfig is the session object of session.start.
type LiveSessionConfig struct {
	// Model is the exact GPT-Live model id; auto selection is not offered on
	// this route.
	Model string `json:"model"`
	// Instructions steer the live voice model. Backend instructions live on
	// delegation.responses.instructions and are never merged into these.
	Instructions string `json:"instructions,omitempty"`
	// Audio declares the one PCM16 format used in both directions and the
	// output voice (default marin).
	Audio *LiveAudioConfig `json:"audio,omitempty"`
	// Input seeds prior text messages.
	Input []LiveInputItem `json:"input,omitempty"`
	// Delegation selects client or responses delegation; omitted or null is
	// client delegation.
	Delegation *LiveDelegationConfig `json:"delegation,omitempty"`
	// Store must be false or omitted: the Router never persists session
	// recordings at the vendor on a caller's behalf.
	Store *bool `json:"store,omitempty"`
}

// LiveAudioConfig is session.audio.
type LiveAudioConfig struct {
	Format *LiveAudioFormat `json:"format,omitempty"`
	Output *LiveAudioOutput `json:"output,omitempty"`
}

// LiveAudioFormat is the shared input/output audio format.
type LiveAudioFormat struct {
	Type string `json:"type"`
	Rate int    `json:"rate"`
}

// LiveAudioOutput is session.audio.output.
type LiveAudioOutput struct {
	Voice string `json:"voice,omitempty"`
}

// LiveInputItem is one seeded history message.
type LiveInputItem struct {
	Type    string            `json:"type,omitempty"`
	Role    string            `json:"role"`
	Content []LiveContentPart `json:"content"`
}

// LiveContentPart is one text part of a seeded message.
type LiveContentPart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// LiveDelegationConfig is session.delegation.
type LiveDelegationConfig struct {
	Type      string               `json:"type"`
	Responses *LiveResponsesConfig `json:"responses,omitempty"`
}

// LiveResponsesConfig is session.delegation.responses: the Responses backend
// GPT-Live delegates to. Tools, tool_choice, reasoning, and text are kept
// verbatim within their size bounds because their shapes belong to the
// backend model.
type LiveResponsesConfig struct {
	Model             string            `json:"model"`
	Instructions      string            `json:"instructions,omitempty"`
	Tools             []json.RawMessage `json:"tools,omitempty"`
	ToolChoice        json.RawMessage   `json:"tool_choice,omitempty"`
	ParallelToolCalls *bool             `json:"parallel_tool_calls,omitempty"`
	MaxOutputTokens   *int              `json:"max_output_tokens,omitempty"`
	ServiceTier       string            `json:"service_tier,omitempty"`
	Reasoning         json.RawMessage   `json:"reasoning,omitempty"`
	Text              json.RawMessage   `json:"text,omitempty"`
}

// DecodeLiveSessionStart decodes the first frame strictly: unknown fields
// anywhere in the command are refused, so a caller learns that the Router
// dropped nothing rather than discovering it at the vendor.
func DecodeLiveSessionStart(raw []byte) (LiveSessionStart, error) {
	var start LiveSessionStart
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&start); err != nil {
		return LiveSessionStart{}, fmt.Errorf("session.start is not a valid frame: %w", err)
	}
	if decoder.More() {
		return LiveSessionStart{}, fmt.Errorf("session.start frame carries trailing content")
	}
	return start, nil
}

// Validate checks the session.start command.
func (s LiveSessionStart) Validate() error {
	if s.Type != LiveSessionStartType {
		return fmt.Errorf("type: got %q, want %q", s.Type, LiveSessionStartType)
	}
	if len(s.EventID) > 256 {
		return fmt.Errorf("event_id: at most 256 bytes")
	}
	if err := s.Session.Validate(); err != nil {
		return fmt.Errorf("session: %w", err)
	}
	return nil
}

// Validate checks the session object.
func (c LiveSessionConfig) Validate() error {
	model := strings.TrimSpace(c.Model)
	if model == "" || model != c.Model || model == "auto" {
		return fmt.Errorf("model: an exact model id is required")
	}
	if len(model) > 128 || strings.ContainsAny(model, " \t\r\n/\\") {
		return fmt.Errorf("model: invalid model id")
	}
	if len(c.Instructions) > MaxLiveInstructionsBytes {
		return fmt.Errorf("instructions: at most %d bytes", MaxLiveInstructionsBytes)
	}
	if !utf8.ValidString(c.Instructions) {
		return fmt.Errorf("instructions: must be valid UTF-8")
	}
	if c.Audio != nil {
		if err := c.Audio.Validate(); err != nil {
			return fmt.Errorf("audio: %w", err)
		}
	}
	if len(c.Input) > MaxLiveHistoryItems {
		return fmt.Errorf("input: at most %d items", MaxLiveHistoryItems)
	}
	total := 0
	for i, item := range c.Input {
		if err := item.Validate(); err != nil {
			return fmt.Errorf("input[%d]: %w", i, err)
		}
		for _, part := range item.Content {
			total += len(part.Text)
		}
	}
	if total > MaxLiveHistoryBytes {
		return fmt.Errorf("input: at most %d bytes of text in total", MaxLiveHistoryBytes)
	}
	if c.Delegation != nil {
		if err := c.Delegation.Validate(); err != nil {
			return fmt.Errorf("delegation: %w", err)
		}
	}
	if c.Store != nil && *c.Store {
		return fmt.Errorf("store: session recordings are not stored through the Router")
	}
	return nil
}

// SampleRateHz returns the declared PCM rate, defaulting to 24 kHz.
func (c LiveSessionConfig) SampleRateHz() int {
	if c.Audio == nil || c.Audio.Format == nil || c.Audio.Format.Rate == 0 {
		return 24_000
	}
	return c.Audio.Format.Rate
}

// Voice returns the requested output voice, "" when the caller left it to
// the route default.
func (c LiveSessionConfig) Voice() string {
	if c.Audio == nil || c.Audio.Output == nil {
		return ""
	}
	return strings.TrimSpace(c.Audio.Output.Voice)
}

// DelegationType returns the effective delegation mode.
func (c LiveSessionConfig) DelegationType() string {
	if c.Delegation == nil || c.Delegation.Type == "" {
		return "client"
	}
	return c.Delegation.Type
}

// Validate checks the audio block: PCM16 at a transported rate.
func (a LiveAudioConfig) Validate() error {
	if a.Format != nil {
		if err := a.Format.Validate(); err != nil {
			return fmt.Errorf("format: %w", err)
		}
	}
	if a.Output != nil && len(a.Output.Voice) > 128 {
		return fmt.Errorf("output.voice: at most 128 bytes")
	}
	return nil
}

// Validate refuses every format but mono PCM16 at a transported rate.
func (f LiveAudioFormat) Validate() error {
	if f.Type != "audio/pcm" {
		return fmt.Errorf("type: %q is not transported; use audio/pcm", f.Type)
	}
	for _, rate := range LiveAudioSampleRates {
		if f.Rate == rate {
			return nil
		}
	}
	return fmt.Errorf("rate: %d Hz is not transported; use 16000 or 24000", f.Rate)
}

// Validate checks one seeded message.
func (i LiveInputItem) Validate() error {
	if i.Type != "" && i.Type != "message" {
		return fmt.Errorf("type: must be message")
	}
	switch i.Role {
	case "user", "assistant", "system", "developer":
	default:
		return fmt.Errorf("role: must be user, assistant, system, or developer")
	}
	if len(i.Content) == 0 {
		return fmt.Errorf("content: at least one text part is required")
	}
	for j, part := range i.Content {
		if part.Type != "input_text" && part.Type != "output_text" {
			return fmt.Errorf("content[%d].type: must be input_text or output_text", j)
		}
		if strings.TrimSpace(part.Text) == "" {
			return fmt.Errorf("content[%d].text: required", j)
		}
		if len(part.Text) > MaxLiveHistoryItemBytes {
			return fmt.Errorf("content[%d].text: at most %d bytes", j, MaxLiveHistoryItemBytes)
		}
		if !utf8.ValidString(part.Text) {
			return fmt.Errorf("content[%d].text: must be valid UTF-8", j)
		}
	}
	return nil
}

// Validate checks the delegation block. Responses delegation requires an
// explicit backend model.
func (d LiveDelegationConfig) Validate() error {
	switch d.Type {
	case "client":
		if d.Responses != nil {
			return fmt.Errorf("responses: not valid for client delegation")
		}
	case "responses":
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

// LiveToolType peeks the type tag of one raw tool declaration.
func LiveToolType(tool json.RawMessage) string {
	var tagged struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(tool, &tagged)
	return strings.TrimSpace(tagged.Type)
}

// Validate checks the backend configuration.
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
		if len(tool) > MaxLiveToolBytes || !isJSONObject(tool) {
			return fmt.Errorf("tools[%d]: must be a JSON object within %d bytes", i, MaxLiveToolBytes)
		}
		switch LiveToolType(tool) {
		case "function":
			var named struct {
				Name string `json:"name"`
			}
			_ = json.Unmarshal(tool, &named)
			name := strings.TrimSpace(named.Name)
			if name == "" {
				return fmt.Errorf("tools[%d]: function tool requires a name", i)
			}
			if names[name] {
				return fmt.Errorf("tools[%d]: duplicate function name %q", i, name)
			}
			names[name] = true
		case "web_search":
		default:
			return fmt.Errorf("tools[%d]: type %q is not supported (function or web_search)", i, LiveToolType(tool))
		}
	}
	if len(c.ToolChoice) > 0 && (len(c.ToolChoice) > MaxLiveToolChoiceBytes || !json.Valid(c.ToolChoice)) {
		return fmt.Errorf("tool_choice: must be valid JSON within %d bytes", MaxLiveToolChoiceBytes)
	}
	if c.MaxOutputTokens != nil && (*c.MaxOutputTokens < MinLiveMaxOutputTokens || *c.MaxOutputTokens > MaxLiveMaxOutputTokens) {
		return fmt.Errorf("max_output_tokens: must be between %d and %d", MinLiveMaxOutputTokens, MaxLiveMaxOutputTokens)
	}
	switch c.ServiceTier {
	case "", "auto", "default", "flex", "priority":
	default:
		return fmt.Errorf("service_tier: must be auto, default, flex, or priority")
	}
	if len(c.Reasoning) > 0 && (len(c.Reasoning) > MaxLiveSettingBytes || !isJSONObject(c.Reasoning)) {
		return fmt.Errorf("reasoning: must be a JSON object within %d bytes", MaxLiveSettingBytes)
	}
	if len(c.Text) > 0 && (len(c.Text) > MaxLiveSettingBytes || !isJSONObject(c.Text)) {
		return fmt.Errorf("text: must be a JSON object within %d bytes", MaxLiveSettingBytes)
	}
	return nil
}

// ToolTypes returns the distinct declared tool types in declaration order.
func (c LiveResponsesConfig) ToolTypes() []string {
	seen := make(map[string]bool, len(c.Tools))
	var types []string
	for _, tool := range c.Tools {
		kind := LiveToolType(tool)
		if seen[kind] {
			continue
		}
		seen[kind] = true
		types = append(types, kind)
	}
	return types
}

func isJSONObject(raw json.RawMessage) bool {
	return json.Valid(raw) && strings.HasPrefix(strings.TrimSpace(string(raw)), "{")
}
