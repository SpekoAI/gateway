package cartesia

import (
	"fmt"
	"math"
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

// maxBufferDelayKey is how long Cartesia holds continued text before it starts
// generating: "The maximum time in milliseconds to buffer text before starting
// generation. Values between [0, 5000]ms are supported. Defaults to 3000ms."
// 0 turns server-side buffering off. It is a top-level field of the WebSocket
// generation request; the bytes endpoint has no such field, since it takes
// the whole transcript at once. Contexts require every request to repeat the
// same fields, so it rides every generation of the stream.
// docs.cartesia.ai/api-reference/tts/websocket
// docs.cartesia.ai/use-the-api/tts-websocket/buffering
const maxBufferDelayKey = "max_buffer_delay_ms"

const maxBufferDelayLimitMs = 5000

// maxBufferDelayModels are the models the WebSocket request schema that
// documents the field enumerates.
var maxBufferDelayModels = []string{"sonic-3.6", "sonic-3.5", "sonic-3"}

// speedKey is generation_config.speed: "Adjust the speed of the generated
// speech between 0.6x and 1.5x the default speed." GenerationConfig is
// "Available on sonic-3 and newer models", and the Sonic 3.5 and 3.6 releases
// keep the speed control. It applies to instant voice clones; professional
// clones "don't support request-time speed adjustments". Both the WebSocket
// and bytes requests carry generation_config.
// docs.cartesia.ai/build-with-cartesia/capability-guides/volume-speed-emotion
// docs.cartesia.ai/api-reference/tts/bytes
// docs.cartesia.ai/changelog/2026
const speedKey = "speed"

const (
	minSpeed = 0.6
	maxSpeed = 1.5
)

var speedModels = []string{"sonic-3.6", "sonic-3.5", "sonic-3"}

// ttsSettings are the validated provider options to send. The zero value is
// "the caller set none": every field is omitted from the wire.
type ttsSettings struct {
	normalization  string
	maxBufferDelay *int
	speed          *float64
}

// generationConfig returns the generation_config object to send, or nil.
func (s ttsSettings) generationConfig() *generationConfig {
	if s.speed == nil {
		return nil
	}
	return &generationConfig{Speed: *s.speed}
}

// ValidateTTSOptions reports whether every setting addressed to Cartesia can
// be honored by this model. Shared by the local gateway's route check, the
// relay edge's routing check, and the adapter itself.
func ValidateTTSOptions(model string, settings map[string]any) error {
	_, err := ttsOptions(model, settings)
	return err
}

// ttsOptions validates settings and returns the values to send.
func ttsOptions(model string, settings map[string]any) (ttsSettings, error) {
	keys := make([]string, 0, len(settings))
	for key := range settings {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var out ttsSettings
	for _, key := range keys {
		switch key {
		case normalizationKey:
			if !servesModel(normalizationModels, model) {
				return ttsSettings{}, refuse(key, "normalization is available on "+strings.Join(normalizationModels, ", ")+" only")
			}
			value, ok := settings[key].(string)
			value = strings.TrimSpace(value)
			if !ok || (value != "auto" && value != "off" && !protocol.ValidLanguageTag(value)) {
				return ttsSettings{}, refuse(key, `must be "auto", "off", or a language or locale such as "en" or "en-IN"`)
			}
			out.normalization = value
		case maxBufferDelayKey:
			if !servesModel(maxBufferDelayModels, model) {
				return ttsSettings{}, refuse(key, "max_buffer_delay_ms is available on "+strings.Join(maxBufferDelayModels, ", ")+" only")
			}
			number, ok := protocol.TtsOptionNumber(settings[key])
			if !ok || math.IsNaN(number) || number != math.Trunc(number) || number < 0 || number > maxBufferDelayLimitMs {
				return ttsSettings{}, refuse(key, fmt.Sprintf("must be an integer from 0 to %d", maxBufferDelayLimitMs))
			}
			delay := int(number)
			out.maxBufferDelay = &delay
		case speedKey:
			if !servesModel(speedModels, model) {
				return ttsSettings{}, refuse(key, "speed is available on "+strings.Join(speedModels, ", ")+" only")
			}
			number, ok := protocol.TtsOptionNumber(settings[key])
			if !ok || math.IsNaN(number) || number < minSpeed || number > maxSpeed {
				return ttsSettings{}, refuse(key, fmt.Sprintf("must be a number from %g to %g", minSpeed, maxSpeed))
			}
			out.speed = &number
		default:
			return ttsSettings{}, refuse(key, "Cartesia TTS accepts only normalization, max_buffer_delay_ms and speed")
		}
	}
	return out, nil
}

func servesModel(families []string, model string) bool {
	for _, prefix := range families {
		if strings.HasPrefix(strings.TrimSpace(model), prefix) {
			return true
		}
	}
	return false
}

func refuse(key, detail string) *protocol.TtsOptionError {
	return &protocol.TtsOptionError{Provider: "cartesia", Key: key, Detail: detail}
}
