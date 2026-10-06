package assemblyai

import (
	"github.com/SpekoAI/gateway/protocol"
	"sort"
)

// Only metadata is retained. Keywords are included in realtime pricing but
// billed as a separate add-on for async Universal-3.5 Pro.
func billingFeatures(options protocol.RequestOptions, batch bool) []string {
	var features []string
	if options.STT.Diarize() {
		features = append(features, "diarization")
	}
	if batch && len(options.STT.GetKeywords()) > 0 {
		features = append(features, "keyterms")
	}
	if options.STT.ReduceNoise() {
		features = append(features, "voice_focus")
	}
	// Turn-detection thresholds change when a turn commits, not what the
	// vendor bills. min_turn_silence is the current spelling of
	// min_end_of_turn_silence_when_confident; the two must price alike, or
	// the same setting under its new name becomes an unpriceable feature.
	for _, key := range options.STT.ProviderKeys("assemblyai") {
		switch key {
		case "end_of_turn_confidence_threshold", "min_end_of_turn_silence_when_confident", "min_turn_silence", "max_turn_silence":
		default:
			features = append(features, "option:"+key)
		}
	}
	sort.Strings(features)
	return features
}
