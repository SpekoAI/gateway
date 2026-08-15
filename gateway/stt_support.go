package gateway

import (
	"fmt"
	"strings"

	"github.com/SpekoAI/gateway/protocol"
)

// sttSupport records which caller STT options a provider's streaming adapter
// can actually honor, and which of the vendor's own settings it will forward.
//
// This table exists for the same reason the catalog does: the failure mode is
// a caller asking for diarization, being routed to a vendor that cannot
// diarize, and receiving a 200 whose missing speakers look exactly like a
// one-speaker recording. The gateway refuses that session instead, naming the
// option, so the caller learns to pin a capable provider or drop the ask.
//
// providerKeys is the allow-list for provider_options passthrough. A name
// outside it is refused rather than forwarded, because a plausible setting
// the vendor never reads is accepted-and-ignored — a request that cost money
// and did not do what it said. Every entry's wire translation lives in the
// provider's own adapter; this table only answers "may it be asked".
type sttSupport struct {
	diarization    bool
	keywords       bool
	noiseReduction bool
	providerKeys   []string
	// modelKeys scopes settings to a model family when one provider serves
	// two wire generations. The FIRST matching prefix contributes its keys —
	// the same first-match rule the catalog's ModelRoutes use — and an empty
	// prefix is the everything-else row. A setting outside the matched row is
	// refused rather than written onto a URL the model ignores: Deepgram's
	// Flux endpoint reads none of the v1 formatting knobs, so a model-blind
	// list would accept `numerals` on the DEFAULT route and silently drop it.
	modelKeys []sttModelKeys
}

type sttModelKeys struct {
	prefix string
	keys   []string
}

// keysFor returns the settings this provider accepts on this model.
func (s sttSupport) keysFor(model string) []string {
	for _, family := range s.modelKeys {
		if strings.HasPrefix(model, family.prefix) {
			return append(family.keys, s.providerKeys...)
		}
	}
	return s.providerKeys
}

var sttOptionSupport = map[string]sttSupport{
	// diarize / keyterm+keywords on v1 listen; keyterm on v2 (Flux). Flux owns
	// turn detection, so its three eot_* knobs are caller-tunable there and
	// meaningless on v1; the v1 formatting and endpointing knobs are ignored
	// by the Flux endpoint. Diarization is v1-only. The catalog default is
	// flux-general-en, so the split guards the DEFAULT route, not an edge.
	"deepgram": {diarization: true, keywords: true, modelKeys: []sttModelKeys{
		{prefix: "flux-", keys: []string{"eot_threshold", "eager_eot_threshold", "eot_timeout_ms"}},
		{prefix: "", keys: []string{
			"punctuate", "numerals", "smart_format", "filler_words", "profanity_filter",
			"dictation", "measurements",
			// The v1 adapter pins endpointing=false because the framework owns
			// turn detection by default; a caller who sets it is choosing vendor
			// endpointing deliberately, and the caller value replaces the pin.
			"endpointing", "utterance_end_ms",
		}},
	}},
	// All on the v3 socket's own connection parameters: keyterms_prompt,
	// speaker_labels (streaming diarization shipped vendor-side — the old
	// "no speakers on this transport" claim is stale), and voice_focus, which
	// is the second provider behind the canonical noise_reduction ask.
	// min_turn_silence is the current parameter table's spelling;
	// min_end_of_turn_silence_when_confident stays for callers holding the
	// earlier documented name.
	"assemblyai": {diarization: true, keywords: true, noiseReduction: true, providerKeys: []string{
		"end_of_turn_confidence_threshold", "min_end_of_turn_silence_when_confident",
		"min_turn_silence", "max_turn_silence",
		"max_speakers", "voice_focus", "voice_focus_threshold", "filter_profanity",
	}},
	// Scribe realtime takes repeated keyterms; vad_silence_threshold_secs is
	// the one commit-strategy knob worth exposing (default 1.5s, and the
	// snappy-vs-patient tradeoff is genuinely per-caller).
	"elevenlabs": {keywords: true, providerKeys: []string{"vad_silence_threshold_secs"}},
	// enable_speaker_diarization and context terms both ride the start frame.
	"soniox": {diarization: true, keywords: true},
	// custom vocabulary and the audio enhancer ride the live init call. Live
	// sessions have no diarization option at all (batch does).
	"gladia": {keywords: true, noiseReduction: true},
	// Keywords fold into the transcription prompt alongside a caller's own
	// prompt text. gpt-4o-transcribe-diarize is batch-only, so no realtime
	// diarization. The vendor documents `prompt` on the realtime session for
	// the gpt-live-transcribe / gpt-transcribe generation only, so both the
	// prompt setting and the keywords that fold into it are gated to those
	// families — sending a prompt an older model ignores is the silent no-op
	// this table refuses.
	// noise_reduction is session-level rather than model-gated: the realtime
	// session object takes it whatever transcription model is configured.
	"openai": {keywords: true, noiseReduction: true, providerKeys: []string{"noise_reduction"}, modelKeys: []sttModelKeys{
		{prefix: "gpt-live-transcribe", keys: []string{"prompt"}},
		{prefix: "gpt-transcribe", keys: []string{"prompt"}},
		{prefix: "", keys: nil},
	}},
}

// SttSupportError is a session refused because the routed provider cannot
// honor a canonical STT ask. Distinguished from provider open failures so the
// create handler can answer 422 with the option named instead of a generic
// bad-gateway, and so the fallback exchange is never spent on it — no retry
// changes what a vendor supports.
type SttSupportError struct {
	Provider string
	Option   string
	Detail   string
}

func (e *SttSupportError) Error() string {
	return fmt.Sprintf("provider %q cannot honor %s: %s", e.Provider, e.Option, e.Detail)
}

// validateSttOptionProviders refuses provider_options entries for providers
// this build has no support row for. Checked at create time against every
// named provider — not just the routed one — because a misspelled provider
// name would otherwise validate and then match nothing, which is the
// accepted-but-unfulfilled outcome the options exist to prevent.
func validateSttOptionProviders(options *protocol.SttOptions) error {
	if options == nil {
		return nil
	}
	for provider := range options.ProviderOptions {
		if _, known := sttOptionSupport[provider]; !known {
			return fmt.Errorf("provider_options.%s: no transcription provider by this name accepts settings", provider)
		}
	}
	return nil
}

// validateSttRouteSupport fails closed before a provider session is opened:
// every canonical ask must be one the routed provider can honor, and every
// provider_options key addressed to the routed provider must be on its
// allow-list. Settings for OTHER providers are ignored by design — a caller
// who tunes two providers means "whichever of you answers".
func validateSttRouteSupport(provider, model string, options *protocol.SttOptions) error {
	if options.IsZero() {
		return nil
	}
	name := strings.ToLower(provider)
	support, known := sttOptionSupport[name]
	if !known {
		// Only an ACTIVE ask fails closed. An explicit false turns a vendor
		// default off, which a provider without the feature satisfies by
		// doing nothing; settings addressed to other providers are ignored
		// by design.
		ask := firstSttAsk(options)
		if ask == "" && len(options.Provider(name)) == 0 {
			return nil
		}
		if ask == "" {
			ask = "provider_options." + name
		}
		return &SttSupportError{Provider: name, Option: ask, Detail: "this provider accepts no transcription options; pin a provider that supports the ask or drop it"}
	}
	if options.Diarize() && !support.diarization {
		return &SttSupportError{Provider: name, Option: "diarization", Detail: "no speaker labels on this provider's streaming transport"}
	}
	// Deepgram's split is model-level: Flux owns turns and takes keyterm, but
	// has no diarize parameter at all.
	if options.Diarize() && name == "deepgram" && strings.HasPrefix(model, "flux-") {
		return &SttSupportError{Provider: name, Option: "diarization", Detail: "Deepgram Flux models have no diarization; pin a nova model"}
	}
	if len(options.GetKeywords()) > 0 && !support.keywords {
		return &SttSupportError{Provider: name, Option: "keywords", Detail: "this provider has no vocabulary-biasing parameter on its streaming transport"}
	}
	// OpenAI keywords fold into the realtime prompt, and only the
	// gpt-live-transcribe / gpt-transcribe generation documents that field —
	// on an older model the folded prompt would be accepted and ignored.
	if len(options.GetKeywords()) > 0 && name == "openai" && !openaiPromptModel(model) {
		return &SttSupportError{Provider: name, Option: "keywords", Detail: "keywords ride the realtime prompt, which this model does not read; pin gpt-live-transcribe"}
	}
	if options.ReduceNoise() && !support.noiseReduction {
		return &SttSupportError{Provider: name, Option: "noise_reduction", Detail: "this provider has no audio-enhancement parameter; gladia and assemblyai support it"}
	}
	allowed := support.keysFor(model)
	for _, key := range options.ProviderKeys(name) {
		if !sttKeyAllowed(allowed, key) {
			return &SttSupportError{Provider: name, Option: "provider_options." + name + "." + key, Detail: "this provider does not accept this setting on this model"}
		}
	}
	// AssemblyAI validates voice_focus_threshold against voice_focus and
	// answers a threshold sent alone with an Error FRAME — after the
	// handshake succeeded, so the session opens and then dies. Refusing it at
	// create turns a mid-call failure into an answerable one.
	if name == "assemblyai" && options.Provider(name)["voice_focus_threshold"] != nil &&
		options.Provider(name)["voice_focus"] == nil && !options.ReduceNoise() {
		return &SttSupportError{
			Provider: name,
			Option:   "provider_options.assemblyai.voice_focus_threshold",
			Detail:   "voice_focus_threshold needs voice focus on; set noise_reduction true or providerOptions.assemblyai.voice_focus",
		}
	}
	return nil
}

// openaiPromptModel reports whether this model's realtime session documents
// the transcription prompt field.
func openaiPromptModel(model string) bool {
	return strings.HasPrefix(model, "gpt-live-transcribe") || strings.HasPrefix(model, "gpt-transcribe")
}

func sttKeyAllowed(allowed []string, key string) bool {
	for _, name := range allowed {
		if name == key {
			return true
		}
	}
	return false
}

// firstSttAsk names the first ACTIVE ask, so a refusal tells the caller which
// requirement to drop first. Empty when every canonical field is unset or an
// explicit false — turning a feature off is never an ask.
func firstSttAsk(options *protocol.SttOptions) string {
	switch {
	case options.Diarize():
		return "diarization"
	case len(options.GetKeywords()) > 0:
		return "keywords"
	case options.ReduceNoise():
		return "noise_reduction"
	default:
		return ""
	}
}
