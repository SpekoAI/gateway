package relayapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

// Gemini Live (BidiGenerateContent) on the Router's /v1/google/gemini-live socket.
//
// The route is the third public speech-to-speech surface and behaves like the
// other two: the Router authenticates, admits, meters and forwards, and does
// not translate events. What differs is the FRAMING. OpenAI Realtime and
// GPT-Live tag every message with a "type" field; Gemini tags by the single
// top-level KEY of the object — `{"setup":…}`, `{"realtimeInput":…}`,
// `{"clientContent":…}`. Nothing in the type-tagged path (controlType,
// ProviderControl) can classify those, so this file carries the parallel
// vocabulary and the edge switches on the protocol to pick one.
const (
	// BidiRoutePath serves the Gemini Live protocol. The first text frame MUST
	// be the vendor's setup message naming the model; its exact frame bytes
	// are the idempotency content hash, exactly as session.start is on
	// /v1/openai/gpt-live.
	BidiRoutePath = "/v1/google/gemini-live"
	// BidiSetupKey is the top-level key of that first frame.
	BidiSetupKey = "setup"
	// BidiRealtimeInputKey is the media key. A realtimeInput frame that
	// carries audio rides the metered media path; one that carries none
	// (audioStreamEnd, activityStart/End, text) is forwarded as a control, and
	// the adapter re-checks that no audio is smuggled through that channel.
	BidiRealtimeInputKey = "realtimeInput"
	// MaxBidiSetupFrameBytes bounds the whole setup frame. It mirrors
	// protocol.MaxBidiSetupBytes (relayapi cannot import protocol), so a
	// frame this decoder accepts always fits the connector handshake: the
	// per-field bounds below sum well past it, and without this check a
	// document could pass here and be refused one hop later.
	MaxBidiSetupFrameBytes = 192 << 10
)

// Gemini Live setup bounds. They mirror the /v1/openai/gpt-live bounds so one route
// cannot accept a document the other would refuse, and exist so the setup
// frame is validated before anything is hashed, admitted, or forwarded.
const (
	MaxBidiInstructionsBytes = 96 << 10
	MaxBidiHistoryItems      = 256
	MaxBidiHistoryBytes      = 256 << 10
	MaxBidiTools             = 64
	MaxBidiToolBytes         = 64 << 10
	// MaxBidiSettingBytes bounds each vendor setting sub-document, including
	// generationConfig. 4 KiB holds because every member the vendor documents
	// for Live is a scalar, an enum or a short string: responseModalities,
	// speechConfig (voiceConfig.prebuiltVoiceConfig.voiceName, languageCode),
	// thinkingConfig (thinkingLevel, thinkingBudget, includeThoughts),
	// enableAffectiveDialog, mediaResolution, translationConfig and the
	// sampling scalars together stay well under 1 KiB. The only unbounded
	// GenerationConfig members — responseSchema and responseJsonSchema — are
	// ones the vendor's Live reference lists as unsupported on this surface,
	// so a document that needs more than 4 KiB here is not a Live document.
	MaxBidiSettingBytes = 4 << 10
	// MaxBidiTranscriptionBytes bounds inputAudioTranscription and
	// outputAudioTranscription separately: unlike the other settings they
	// carry open-ended phrase lists (customVocabulary, adaptationPhrases) that
	// a caller biasing recognition toward a product catalog can legitimately
	// grow past 4 KiB.
	MaxBidiTranscriptionBytes = 16 << 10
)

// BidiAudioSampleRates are the PCM16 input rates the Router transports for
// Gemini Live. The vendor listens at 16 kHz and speaks at 24 kHz — unlike
// GPT-Live, the two directions differ and are declared separately, so the
// output rate is not a caller choice on this route.
var BidiAudioSampleRates = []int{16_000}

// BidiOutputSampleRateHz is the rate Gemini Live speaks at. Fixed by the
// vendor; the catalog row advertises it as the only output format.
const BidiOutputSampleRateHz = 24_000

// BidiSetup is the first text frame of a GET /v1/google/gemini-live session: the vendor's
// own setup message. Unknown fields are refused so the Router forwards
// exactly what it validated.
type BidiSetup struct {
	Setup BidiSetupConfig `json:"setup"`
}

// BidiSetupConfig is the setup object.
type BidiSetupConfig struct {
	// Model is the exact Gemini Live model id. The vendor spells it
	// "models/<id>"; both spellings are accepted and ModelID normalizes.
	Model string `json:"model"`
	// GenerationConfig carries everything the vendor files under it on the
	// wire: responseModalities, speechConfig, thinkingConfig (Gemini 3.8's
	// thinkingLevel / includeThoughts), enableAffectiveDialog,
	// mediaResolution, translationConfig and the sampling scalars. The SDK's
	// flat LiveConnectConfig fields (thinking_config, enable_affective_dialog,
	// media_resolution, temperature, …) all land HERE, which is why none of
	// them is a top-level setup member. Kept verbatim within its bound
	// because the shape belongs to the vendor.
	GenerationConfig json.RawMessage `json:"generationConfig,omitempty"`
	// SystemInstruction steers the live model.
	SystemInstruction json.RawMessage `json:"systemInstruction,omitempty"`
	// Tools are forwarded verbatim; function calls come back as toolCall
	// server messages and are answered with a toolResponse client message.
	Tools []json.RawMessage `json:"tools,omitempty"`
	// SessionResumption and ContextWindowCompression are vendor session
	// settings the Router passes through unchanged.
	SessionResumption        json.RawMessage `json:"sessionResumption,omitempty"`
	ContextWindowCompression json.RawMessage `json:"contextWindowCompression,omitempty"`
	// InputAudioTranscription and OutputAudioTranscription enable the
	// vendor's own transcripts; both ride through untouched.
	InputAudioTranscription  json.RawMessage `json:"inputAudioTranscription,omitempty"`
	OutputAudioTranscription json.RawMessage `json:"outputAudioTranscription,omitempty"`
	// RealtimeInputConfig tunes vendor-side VAD and turn handling.
	RealtimeInputConfig json.RawMessage `json:"realtimeInputConfig,omitempty"`
	// Proactivity (proactiveAudio) and HistoryConfig
	// (initialHistoryInClientContent) complete the setup members the vendor's
	// Live reference documents; both ride through untouched.
	Proactivity   json.RawMessage `json:"proactivity,omitempty"`
	HistoryConfig json.RawMessage `json:"historyConfig,omitempty"`
	// SafetySettings and Labels are absent from the Live reference page but
	// present on the vendor's v1beta wire schema for this setup message, and
	// the vendor's own SDK emits both to this endpoint
	// (LiveConnectConfig.safety_settings / labels). Because unknown fields are
	// refused, leaving them out would break any client that set either.
	SafetySettings json.RawMessage `json:"safetySettings,omitempty"`
	Labels         json.RawMessage `json:"labels,omitempty"`
}

// DecodeBidiSetup decodes the first frame strictly: unknown fields anywhere in
// the message are refused, so a caller learns that the Router dropped nothing
// rather than discovering it at the vendor.
func DecodeBidiSetup(raw []byte) (BidiSetup, error) {
	if len(raw) > MaxBidiSetupFrameBytes {
		return BidiSetup{}, fmt.Errorf("setup frame is larger than %d bytes", MaxBidiSetupFrameBytes)
	}
	var setup BidiSetup
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&setup); err != nil {
		return BidiSetup{}, fmt.Errorf("setup is not a valid frame: %w", err)
	}
	// Decoder.More reports false at a stray closing delimiter, so it cannot
	// prove the frame was consumed: only a second decode that hits EOF can.
	if err := decoder.Decode(new(json.RawMessage)); !errors.Is(err, io.EOF) {
		return BidiSetup{}, fmt.Errorf("setup frame carries trailing content")
	}
	return setup, nil
}

// Validate checks the setup message.
func (s BidiSetup) Validate() error {
	if err := s.Setup.Validate(); err != nil {
		return fmt.Errorf("setup: %w", err)
	}
	return nil
}

// ModelID is the bare catalog model id, with the vendor's "models/" prefix
// removed. The catalog and the signed plan both speak the bare id; accepting
// either spelling keeps a copy-pasted vendor snippet working.
func (c BidiSetupConfig) ModelID() string {
	return strings.TrimPrefix(strings.TrimSpace(c.Model), "models/")
}

// Validate checks the setup object.
func (c BidiSetupConfig) Validate() error {
	model := c.ModelID()
	if model == "" || model == "auto" {
		return fmt.Errorf("model: an exact model id is required")
	}
	if len(model) > 128 || strings.ContainsAny(model, " \t\r\n\\") {
		return fmt.Errorf("model: invalid model id")
	}
	if len(c.SystemInstruction) > MaxBidiInstructionsBytes {
		return fmt.Errorf("systemInstruction: at most %d bytes", MaxBidiInstructionsBytes)
	}
	if len(c.SystemInstruction) > 0 && !utf8.Valid(c.SystemInstruction) {
		return fmt.Errorf("systemInstruction: must be valid UTF-8")
	}
	if len(c.Tools) > MaxBidiTools {
		return fmt.Errorf("tools: at most %d entries", MaxBidiTools)
	}
	if len(c.SystemInstruction) > 0 && !isJSONObject(c.SystemInstruction) {
		return fmt.Errorf("systemInstruction: must be a JSON object")
	}
	for i, tool := range c.Tools {
		if len(tool) > MaxBidiToolBytes {
			return fmt.Errorf("tools[%d]: at most %d bytes", i, MaxBidiToolBytes)
		}
		if !isJSONObject(tool) {
			return fmt.Errorf("tools[%d]: must be a JSON object", i)
		}
	}
	// The settings stay opaque inside, but their OUTER kind is the vendor's
	// documented contract: an object (or, for safetySettings, an array). A
	// scalar here is a malformed document, and refusing it at the hop is
	// cheaper for the caller than a vendor rejection mid-handshake.
	for name, raw := range map[string]json.RawMessage{
		"generationConfig":         c.GenerationConfig,
		"sessionResumption":        c.SessionResumption,
		"contextWindowCompression": c.ContextWindowCompression,
		"realtimeInputConfig":      c.RealtimeInputConfig,
		"proactivity":              c.Proactivity,
		"historyConfig":            c.HistoryConfig,
		"labels":                   c.Labels,
	} {
		if len(raw) > MaxBidiSettingBytes {
			return fmt.Errorf("%s: at most %d bytes", name, MaxBidiSettingBytes)
		}
		if len(raw) > 0 && !isJSONObject(raw) {
			return fmt.Errorf("%s: must be a JSON object", name)
		}
	}
	if len(c.SafetySettings) > MaxBidiSettingBytes {
		return fmt.Errorf("safetySettings: at most %d bytes", MaxBidiSettingBytes)
	}
	if len(c.SafetySettings) > 0 && !isJSONArray(c.SafetySettings) {
		return fmt.Errorf("safetySettings: must be a JSON array")
	}
	for name, raw := range map[string]json.RawMessage{
		"inputAudioTranscription":  c.InputAudioTranscription,
		"outputAudioTranscription": c.OutputAudioTranscription,
	} {
		if len(raw) > MaxBidiTranscriptionBytes {
			return fmt.Errorf("%s: at most %d bytes", name, MaxBidiTranscriptionBytes)
		}
		if len(raw) > 0 && !isJSONObject(raw) {
			return fmt.Errorf("%s: must be a JSON object", name)
		}
	}
	return nil
}

// BidiMessageKey returns the single top-level key of a Gemini Live frame.
//
// The key IS the message type on this protocol, so a frame carrying none — or
// carrying several — is unclassifiable and refused rather than guessed at. The
// type-tagged routes get the same treatment from controlType.
func BidiMessageKey(payload []byte) (string, bool) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return "", false
	}
	if len(envelope) != 1 {
		return "", false
	}
	for key := range envelope {
		if strings.TrimSpace(key) == "" {
			return "", false
		}
		return key, true
	}
	return "", false
}

// BidiAudioChunk is the audio carried by one realtimeInput frame. Both the
// current `audio` field and the older `mediaChunks` array are accepted: the
// vendor's own SDKs still emit the latter, and refusing it would make working
// client code fail at the Router for no transport reason.
type BidiAudioChunk struct {
	RealtimeInput struct {
		Audio *struct {
			Data     string `json:"data"`
			MIMEType string `json:"mimeType,omitempty"`
		} `json:"audio,omitempty"`
		MediaChunks []struct {
			Data     string `json:"data"`
			MIMEType string `json:"mimeType,omitempty"`
		} `json:"mediaChunks,omitempty"`
		AudioStreamEnd *bool `json:"audioStreamEnd,omitempty"`
	} `json:"realtimeInput"`
}

// DecodeBidiAudio pulls the base64 audio payloads out of a realtimeInput
// frame, plus whether the caller signalled end of input audio. Unknown fields
// are tolerated here — unlike setup, this frame is on the hot media path and
// the vendor adds fields to it.
func DecodeBidiAudio(payload []byte) (chunks []string, streamEnd bool, ok bool) {
	var frame BidiAudioChunk
	if err := json.Unmarshal(payload, &frame); err != nil {
		return nil, false, false
	}
	if frame.RealtimeInput.Audio != nil && frame.RealtimeInput.Audio.Data != "" {
		chunks = append(chunks, frame.RealtimeInput.Audio.Data)
	}
	for _, chunk := range frame.RealtimeInput.MediaChunks {
		if chunk.Data != "" {
			chunks = append(chunks, chunk.Data)
		}
	}
	streamEnd = frame.RealtimeInput.AudioStreamEnd != nil && *frame.RealtimeInput.AudioStreamEnd
	return chunks, streamEnd, len(chunks) > 0 || streamEnd
}
