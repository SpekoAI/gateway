package relayapi_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/SpekoAI/gateway/relayapi"
)

func validBidiSetup(t *testing.T) relayapi.BidiSetup {
	t.Helper()
	setup, err := relayapi.DecodeBidiSetup([]byte(`{"setup":{"model":"gemini-3.8-live"}}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return setup
}

func TestBidiSetupAcceptsBothModelSpellings(t *testing.T) {
	t.Parallel()
	// The vendor's own snippets say "models/<id>"; the catalog and the signed
	// plan say the bare id. Both must land on the same admitted model or a
	// copy-pasted example fails at the Router for no transport reason.
	for _, raw := range []string{
		`{"setup":{"model":"gemini-3.8-live"}}`,
		`{"setup":{"model":"models/gemini-3.8-live"}}`,
	} {
		setup, err := relayapi.DecodeBidiSetup([]byte(raw))
		if err != nil {
			t.Fatalf("%s: decode: %v", raw, err)
		}
		if err := setup.Validate(); err != nil {
			t.Fatalf("%s: validate: %v", raw, err)
		}
		if got := setup.Setup.ModelID(); got != "gemini-3.8-live" {
			t.Fatalf("%s: ModelID = %q", raw, got)
		}
	}
}

func TestBidiSetupAcceptsEveryDocumentedField(t *testing.T) {
	t.Parallel()
	// DecodeBidiSetup refuses unknown fields, so every setup member the vendor
	// documents must be known here or a real client breaks at the Router.
	// The list is BidiGenerateContentSetup from https://ai.google.dev/api/live
	// (model, generationConfig, systemInstruction, tools, realtimeInputConfig,
	// sessionResumption, contextWindowCompression, inputAudioTranscription,
	// outputAudioTranscription, proactivity, historyConfig) plus the two
	// members the SDK also emits to this endpoint (safetySettings, labels).
	// The generationConfig body is the live-guide's largest example converted
	// to the wire's camelCase: thinkingConfig, enableAffectiveDialog and
	// mediaResolution live INSIDE generationConfig, not beside it.
	raw := `{"setup":{
		"model":"models/gemini-3.8-live",
		"generationConfig":{
			"responseModalities":["AUDIO"],
			"temperature":0.7,"topP":0.95,"topK":40,"maxOutputTokens":2048,"seed":7,
			"speechConfig":{"voiceConfig":{"prebuiltVoiceConfig":{"voiceName":"Kore"}},"languageCode":"en-US"},
			"thinkingConfig":{"thinkingLevel":"low","includeThoughts":true},
			"enableAffectiveDialog":true,
			"mediaResolution":"MEDIA_RESOLUTION_LOW",
			"translationConfig":{"targetLanguageCode":"es-ES","echoTargetLanguage":false}
		},
		"systemInstruction":{"parts":[{"text":"Be concise."}]},
		"tools":[{"functionDeclarations":[{"name":"lookup_order","parameters":{"type":"object","properties":{"order_id":{"type":"string"}}}}]},{"googleSearch":{}}],
		"realtimeInputConfig":{
			"automaticActivityDetection":{"disabled":false,"startOfSpeechSensitivity":"START_SENSITIVITY_LOW","endOfSpeechSensitivity":"END_SENSITIVITY_LOW","prefixPaddingMs":20,"silenceDurationMs":100},
			"activityHandling":"START_OF_ACTIVITY_INTERRUPTS",
			"turnCoverage":"TURN_INCLUDES_ONLY_ACTIVITY"
		},
		"sessionResumption":{"handle":"resume-token"},
		"contextWindowCompression":{"triggerTokens":"25600","slidingWindow":{"targetTokens":"12800"}},
		"inputAudioTranscription":{"languageCodes":["en-US"],"customVocabulary":["Speko"],"wordTimestamp":true,"diarization":false,"mode":"MODE_UNSPECIFIED"},
		"outputAudioTranscription":{},
		"proactivity":{"proactiveAudio":true},
		"historyConfig":{"initialHistoryInClientContent":false},
		"safetySettings":[{"category":"HARM_CATEGORY_HARASSMENT","threshold":"BLOCK_ONLY_HIGH"}],
		"labels":{"safety_identifier":"user_session_123"}
	}}`
	setup, err := relayapi.DecodeBidiSetup([]byte(raw))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if err := setup.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if got := setup.Setup.ModelID(); got != "gemini-3.8-live" {
		t.Fatalf("ModelID = %q", got)
	}
	// Every passthrough field must have landed, not merely been tolerated:
	// a field decoded to nothing would be dropped on forward.
	for name, raw := range map[string]json.RawMessage{
		"generationConfig":         setup.Setup.GenerationConfig,
		"systemInstruction":        setup.Setup.SystemInstruction,
		"realtimeInputConfig":      setup.Setup.RealtimeInputConfig,
		"sessionResumption":        setup.Setup.SessionResumption,
		"contextWindowCompression": setup.Setup.ContextWindowCompression,
		"inputAudioTranscription":  setup.Setup.InputAudioTranscription,
		"outputAudioTranscription": setup.Setup.OutputAudioTranscription,
		"proactivity":              setup.Setup.Proactivity,
		"historyConfig":            setup.Setup.HistoryConfig,
		"safetySettings":           setup.Setup.SafetySettings,
		"labels":                   setup.Setup.Labels,
	} {
		if len(raw) == 0 {
			t.Errorf("%s was not captured", name)
		}
	}
	if len(setup.Setup.Tools) != 2 {
		t.Errorf("tools = %d entries, want 2", len(setup.Setup.Tools))
	}
	var generation map[string]json.RawMessage
	if err := json.Unmarshal(setup.Setup.GenerationConfig, &generation); err != nil {
		t.Fatalf("generationConfig: %v", err)
	}
	if _, ok := generation["thinkingConfig"]; !ok {
		t.Error("generationConfig.thinkingConfig was not preserved")
	}
}

func TestBidiSetupRejectsUnknownFields(t *testing.T) {
	t.Parallel()
	// Strict decoding is the promise that the Router forwarded everything it
	// accepted — a silently dropped field would surface as the model ignoring
	// a setting the caller believes it sent.
	if _, err := relayapi.DecodeBidiSetup([]byte(`{"setup":{"model":"gemini-3.8-live","nope":1}}`)); err == nil {
		t.Fatal("expected an unknown setup field to be refused")
	}
	if _, err := relayapi.DecodeBidiSetup([]byte(`{"setup":{"model":"x"}} {"setup":{}}`)); err == nil {
		t.Fatal("expected trailing content to be refused")
	}
}

func TestBidiSetupRejectsAutoAndMalformedModels(t *testing.T) {
	t.Parallel()
	for _, model := range []string{"", "auto", "models/", "gemini live", strings.Repeat("m", 129)} {
		setup := relayapi.BidiSetup{}
		setup.Setup.Model = model
		if err := setup.Validate(); err == nil {
			t.Fatalf("model %q should be refused", model)
		}
	}
}

func TestBidiSetupBoundsOversizedDocuments(t *testing.T) {
	t.Parallel()
	setup := validBidiSetup(t)
	setup.Setup.SystemInstruction = json.RawMessage(`"` + strings.Repeat("a", relayapi.MaxBidiInstructionsBytes) + `"`)
	assertInvalid(t, setup.Validate(), "systemInstruction")

	setup = validBidiSetup(t)
	setup.Setup.RealtimeInputConfig = json.RawMessage(`{"pad":"` + strings.Repeat("a", relayapi.MaxBidiSettingBytes) + `"}`)
	assertInvalid(t, setup.Validate(), "realtimeInputConfig")

	setup = validBidiSetup(t)
	setup.Setup.Proactivity = json.RawMessage(`{"pad":"` + strings.Repeat("a", relayapi.MaxBidiSettingBytes) + `"}`)
	assertInvalid(t, setup.Validate(), "proactivity")

	// Transcription configs have their own, larger bound: a phrase list that
	// the setting bound would refuse must pass, and the larger bound must
	// still hold.
	setup = validBidiSetup(t)
	setup.Setup.InputAudioTranscription = json.RawMessage(`{"customVocabulary":["` + strings.Repeat("a", relayapi.MaxBidiSettingBytes) + `"]}`)
	if err := setup.Validate(); err != nil {
		t.Fatalf("a %d-byte inputAudioTranscription must pass: %v", relayapi.MaxBidiSettingBytes, err)
	}
	setup.Setup.OutputAudioTranscription = json.RawMessage(`{"customVocabulary":["` + strings.Repeat("a", relayapi.MaxBidiTranscriptionBytes) + `"]}`)
	assertInvalid(t, setup.Validate(), "outputAudioTranscription")

	setup = validBidiSetup(t)
	tools := make([]json.RawMessage, relayapi.MaxBidiTools+1)
	for i := range tools {
		tools[i] = json.RawMessage(`{}`)
	}
	setup.Setup.Tools = tools
	assertInvalid(t, setup.Validate(), "tools")
}

func TestBidiMessageKeyClassifiesByTopLevelKey(t *testing.T) {
	t.Parallel()
	// The key IS the message type here; there is no "type" field to read.
	for raw, want := range map[string]string{
		`{"setup":{}}`:         relayapi.BidiSetupKey,
		`{"realtimeInput":{}}`: relayapi.BidiRealtimeInputKey,
		`{"clientContent":{}}`: "clientContent",
		`{"toolResponse":{}}`:  "toolResponse",
	} {
		got, ok := relayapi.BidiMessageKey([]byte(raw))
		if !ok || got != want {
			t.Fatalf("%s: got (%q,%v), want %q", raw, got, ok, want)
		}
	}
	// Zero keys and several keys are both unclassifiable, and guessing which
	// one the caller meant would forward the wrong message.
	for _, raw := range []string{`{}`, `{"a":1,"b":2}`, `[]`, `not json`, `{"":1}`} {
		if _, ok := relayapi.BidiMessageKey([]byte(raw)); ok {
			t.Fatalf("%s should not classify", raw)
		}
	}
}

func TestDecodeBidiAudioAcceptsBothVendorShapes(t *testing.T) {
	t.Parallel()
	// `mediaChunks` is the older spelling and the vendor's SDKs still emit it.
	chunks, end, ok := relayapi.DecodeBidiAudio([]byte(`{"realtimeInput":{"audio":{"data":"AAA="}}}`))
	if !ok || end || len(chunks) != 1 || chunks[0] != "AAA=" {
		t.Fatalf("audio: got (%v,%v,%v)", chunks, end, ok)
	}
	chunks, _, ok = relayapi.DecodeBidiAudio([]byte(`{"realtimeInput":{"mediaChunks":[{"data":"AAA="},{"data":"BBB="}]}}`))
	if !ok || len(chunks) != 2 {
		t.Fatalf("mediaChunks: got (%v,%v)", chunks, ok)
	}
	if _, end, ok = relayapi.DecodeBidiAudio([]byte(`{"realtimeInput":{"audioStreamEnd":true}}`)); !ok || !end {
		t.Fatalf("audioStreamEnd: got (%v,%v)", end, ok)
	}
	if _, _, ok = relayapi.DecodeBidiAudio([]byte(`{"realtimeInput":{}}`)); ok {
		t.Fatal("an empty realtimeInput carries nothing and must not read as audio")
	}
}

func TestBidiRouteIsDistinctFromTheOpenAIRoutes(t *testing.T) {
	t.Parallel()
	// Five protocol families, five paths: a collision would route a session
	// to a socket that speaks a different framing.
	paths := map[string]bool{
		relayapi.RealtimeRoutePath:        true,
		relayapi.LiveRoutePath:            true,
		relayapi.BidiRoutePath:            true,
		relayapi.TranslationRoutePath:     true,
		relayapi.QwenTranslationRoutePath: true,
	}
	if len(paths) != 5 {
		t.Fatalf("voice route paths collide: %v", paths)
	}
}

func TestDecodeBidiSetupRefusesOversizedFrames(t *testing.T) {
	t.Parallel()
	// The per-field bounds sum past the connector handshake bound, so the
	// frame as a whole is checked first: a document accepted here must never
	// be refused one hop later.
	padding := strings.Repeat("x", relayapi.MaxBidiSetupFrameBytes)
	frame := []byte(`{"setup":{"model":"gemini-3.8-live","systemInstruction":{"parts":[{"text":"` + padding + `"}]}}}`)
	if _, err := relayapi.DecodeBidiSetup(frame); err == nil {
		t.Fatal("an oversized setup frame must be refused")
	}
}

func TestDecodeBidiSetupRefusesTrailingDelimiters(t *testing.T) {
	t.Parallel()
	// Decoder.More reports false at a stray closing delimiter, so the earlier
	// check let `}}}` through; only a decode that reaches EOF proves the frame
	// was consumed whole.
	for _, frame := range []string{
		`{"setup":{"model":"gemini-3.8-live"}}}`,
		`{"setup":{"model":"gemini-3.8-live"}}]`,
		`{"setup":{"model":"gemini-3.8-live"}} {}`,
	} {
		if _, err := relayapi.DecodeBidiSetup([]byte(frame)); err == nil {
			t.Fatalf("%s must be refused", frame)
		}
	}
}

func TestBidiSetupRefusesMalformedSettingShapes(t *testing.T) {
	t.Parallel()
	// The settings stay opaque inside, but a scalar where the vendor documents
	// an object (or an array, for safetySettings) is a malformed document and
	// is refused at the hop rather than passed on to fail at the vendor.
	for _, frame := range []string{
		`{"setup":{"model":"gemini-3.8-live","generationConfig":false}}`,
		`{"setup":{"model":"gemini-3.8-live","tools":[42]}}`,
		`{"setup":{"model":"gemini-3.8-live","systemInstruction":"be brief"}}`,
		`{"setup":{"model":"gemini-3.8-live","safetySettings":{}}}`,
		`{"setup":{"model":"gemini-3.8-live","inputAudioTranscription":[]}}`,
		`{"setup":{"model":"gemini-3.8-live","labels":"prod"}}`,
	} {
		setup, err := relayapi.DecodeBidiSetup([]byte(frame))
		if err != nil {
			t.Fatalf("%s: decode: %v", frame, err)
		}
		if err := setup.Validate(); err == nil {
			t.Fatalf("%s must fail validation", frame)
		}
	}
	setup, err := relayapi.DecodeBidiSetup([]byte(`{"setup":{"model":"gemini-3.8-live","generationConfig":{},"tools":[{}],"systemInstruction":{"parts":[]},"safetySettings":[],"labels":{}}}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if err := setup.Validate(); err != nil {
		t.Fatalf("well-shaped settings must validate: %v", err)
	}
}
