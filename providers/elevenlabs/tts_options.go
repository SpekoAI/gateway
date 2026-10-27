package elevenlabs

import (
	"fmt"
	"math"
	"sort"

	"github.com/SpekoAI/gateway/protocol"
)

// The caller-tunable ElevenLabs synthesis settings are the voice_settings
// object, spelled flat under provider_options.elevenlabs because provider
// option values are scalar-only:
//
//	{"elevenlabs": {"stability": 0.4, "similarity_boost": 0.8, "style": 0.2}}
//
// The two TTS sockets accept different subsets, verified live 2026-10-27:
//
//   - multi-context text-to-speech (flash, turbo, multilingual): all five,
//     sent as the context's init packet `{"text": " ", "voice_settings": …}`.
//     An out-of-range value there yields NO audio and NO error frame, which
//     is why every range below is checked before the socket is dialed.
//   - text-to-dialogue (eleven_v3*, eleven_v4*): stability only, in the
//     handshake. Any 0-1 value is accepted (not just v3's 0/0.5/1 presets);
//     the other settings have no field on that wire, so they are refused
//     rather than dropped.
type voiceSettingRange struct {
	min, max float64
}

var ttsVoiceSettingRanges = map[string]voiceSettingRange{
	"stability":        {0, 1},
	"similarity_boost": {0, 1},
	"style":            {0, 1},
	// The vendor's documented speed range; 1.5 produced silence live.
	"speed": {0.7, 1.2},
}

const ttsSpeakerBoostKey = "use_speaker_boost"

// dialogueVoiceSettings are the voice_settings fields the text-to-dialogue
// socket reads.
var dialogueVoiceSettings = map[string]struct{}{"stability": {}}

// ValidateTTSOptions reports whether every setting addressed to ElevenLabs
// can be honored by this model's socket. The local gateway's route check, the
// relay edge's routing check, and both adapters call it, so "may it be asked"
// and "what reaches the wire" cannot drift apart.
func ValidateTTSOptions(model string, settings map[string]any) error {
	_, err := ttsVoiceSettings(model, settings)
	return err
}

// ttsVoiceSettings validates settings and returns the voice_settings object
// to send, or nil when the caller set none.
func ttsVoiceSettings(model string, settings map[string]any) (map[string]any, error) {
	if len(settings) == 0 {
		return nil, nil
	}
	dialogue := ServesModel(model)
	keys := make([]string, 0, len(settings))
	for key := range settings {
		keys = append(keys, key)
	}
	// Sorted so the refusal a caller sees is the same one every time.
	sort.Strings(keys)
	wire := make(map[string]any, len(settings))
	for _, key := range keys {
		value := settings[key]
		_, numeric := ttsVoiceSettingRanges[key]
		if !numeric && key != ttsSpeakerBoostKey {
			return nil, refuse(key, "ElevenLabs TTS accepts only stability, similarity_boost, style, use_speaker_boost and speed")
		}
		if _, ok := dialogueVoiceSettings[key]; dialogue && !ok {
			return nil, refuse(key, fmt.Sprintf("%s streams on the text-to-dialogue socket, which accepts only stability", model))
		}
		if key == ttsSpeakerBoostKey {
			flag, ok := value.(bool)
			if !ok {
				return nil, refuse(key, "must be a boolean")
			}
			wire[key] = flag
			continue
		}
		number, ok := protocol.TtsOptionNumber(value)
		bounds := ttsVoiceSettingRanges[key]
		if !ok || math.IsNaN(number) || number < bounds.min || number > bounds.max {
			return nil, refuse(key, fmt.Sprintf("must be a number from %g to %g", bounds.min, bounds.max))
		}
		wire[key] = number
	}
	return wire, nil
}

func refuse(key, detail string) *protocol.TtsOptionError {
	return &protocol.TtsOptionError{Provider: "elevenlabs", Key: key, Detail: detail}
}
