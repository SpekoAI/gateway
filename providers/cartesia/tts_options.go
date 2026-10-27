package cartesia

import (
	"sort"
	"strings"

	"github.com/SpekoAI/gateway/protocol"
)

// normalizationKey is Cartesia's text-normalization control: how numbers,
// dates, times and currency in the transcript are read. "auto" (the vendor
// default) reads them by the request's language, "off" skips the normalizer,
// and a language or locale ("en", "en-IN") reads them by that language's
// conventions while the speech itself stays in the request's language. That
// last form is the whole point for code-switched calls: a Hindi session can
// say "19,497" and "25/10/2026" the English way with "en-IN".
// docs.cartesia.ai/build-with-cartesia/capability-guides/text-normalization
const normalizationKey = "normalization"

// normalizationModels are the model families the vendor documents the field
// for. sonic-3.5 and sonic-3 are left out until a live probe shows they read
// it: forwarding a field a model ignores is the accepted-but-unfulfilled
// outcome provider options exist to prevent.
var normalizationModels = []string{"sonic-3.6"}

// ValidateTTSOptions reports whether every setting addressed to Cartesia can
// be honored by this model. Shared by the local gateway's route check, the
// relay edge's routing check, and the adapter itself.
func ValidateTTSOptions(model string, settings map[string]any) error {
	_, err := ttsNormalization(model, settings)
	return err
}

// ttsNormalization validates settings and returns the normalization value to
// send, or "" when the caller set none.
func ttsNormalization(model string, settings map[string]any) (string, error) {
	keys := make([]string, 0, len(settings))
	for key := range settings {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	normalization := ""
	for _, key := range keys {
		if key != normalizationKey {
			return "", refuse(key, "Cartesia TTS accepts only normalization")
		}
		if !servesNormalization(model) {
			return "", refuse(key, "normalization is available on "+strings.Join(normalizationModels, ", ")+" only")
		}
		value, ok := settings[key].(string)
		value = strings.TrimSpace(value)
		if !ok || (value != "auto" && value != "off" && !protocol.ValidLanguageTag(value)) {
			return "", refuse(key, `must be "auto", "off", or a language or locale such as "en" or "en-IN"`)
		}
		normalization = value
	}
	return normalization, nil
}

func servesNormalization(model string) bool {
	for _, prefix := range normalizationModels {
		if strings.HasPrefix(strings.TrimSpace(model), prefix) {
			return true
		}
	}
	return false
}

func refuse(key, detail string) *protocol.TtsOptionError {
	return &protocol.TtsOptionError{Provider: "cartesia", Key: key, Detail: detail}
}
