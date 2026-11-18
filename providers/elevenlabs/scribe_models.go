package elevenlabs

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	// ModelScribeV2Realtime is the realtime default; BatchModel is its batch pair.
	ModelScribeV2Realtime = "scribe_v2_realtime"
	// ModelScribeV3Live is the realtime Scribe v3 generation.
	ModelScribeV3Live = "scribe_v3_live"
	// ModelScribeV3 is the batch Scribe v3 generation.
	ModelScribeV3 = "scribe_v3"
)

type scribeSurface string

const (
	scribeRealtime scribeSurface = "realtime"
	scribeBatch    scribeSurface = "batch"
)

// scribeModel is what one Scribe model does beyond the baseline every model on
// its surface shares. The vendor answers a newer model's parameter on an older
// model by closing the socket (realtime) or failing the upload (batch), so
// both adapters consult this table and refuse first.
type scribeModel struct {
	surface scribeSurface
	// options are the scribeGatedOptions settings this model reads.
	options []string
	// diarization marks a realtime model that takes diarize=true and answers
	// with diarized_transcript and speakers_merged. Batch diarization is
	// baseline and is not modelled here.
	diarization bool
	// keytermLibrary marks a realtime model whose first input_audio_chunk
	// accepts keyterm_library.
	keytermLibrary bool
}

// scribeModels is the one place a Scribe model id is classified. An id it does
// not list runs with the baseline of the adapter it was sent to, so a model
// the vendor ships later keeps working without its newer parameters.
var scribeModels = map[string]scribeModel{
	"scribe_v1":           {surface: scribeBatch},
	BatchModel:            {surface: scribeBatch},
	ModelScribeV3:         {surface: scribeBatch, options: []string{"numbers_format", "secondary_language_codes"}},
	ModelScribeV2Realtime: {surface: scribeRealtime},
	ModelScribeV3Live:     {surface: scribeRealtime, options: []string{"multilang_mode", "turn_prediction"}, diarization: true, keytermLibrary: true},
}

// Vendor limits, from the keyterm prompting guide. The realtime `keyterms`
// query takes 50 terms of 20 characters; the keyterm library takes the rest.
const (
	realtimeKeytermsMax     = 50
	realtimeKeytermChars    = 20
	keytermLibraryMaxTerms  = 10_000
	keytermLibraryTermChars = 50
	keytermLibraryMaxBytes  = 2 << 20
)

// scribeParam is one vendor parameter: a query parameter on the realtime
// socket, a multipart field on the batch upload.
type scribeParam struct{ name, value string }

// scribeGatedOptions translates each model-gated provider_options setting into
// vendor parameters. A setting named here is written only for a model whose
// scribeModels row lists it. Every other setting passes through under its own
// name, as it did before any model was gated.
var scribeGatedOptions = map[string]func(value any) ([]scribeParam, error){
	"multilang_mode": func(value any) ([]scribeParam, error) {
		return scribeFlag(value, scribeParam{"multilang_mode", "true"})
	},
	// commit_strategy is gateway-owned, so turn prediction is a flag of its
	// own. It replaces the pinned vad strategy; the vendor keeps VAD active
	// beside it, so vad_silence_threshold_secs still applies.
	"turn_prediction": func(value any) ([]scribeParam, error) {
		return scribeFlag(value, scribeParam{"commit_strategy", "turn_prediction"})
	},
	"numbers_format": func(value any) ([]scribeParam, error) {
		format, _ := value.(string)
		if format != "digits" && format != "words" {
			return nil, errors.New("takes digits or words")
		}
		return []scribeParam{{"numbers_format", format}}, nil
	},
	// The vendor wants one form field per language and rejects a JSON array,
	// while provider options are scalars, so the caller sends "es,fr". The
	// vendor skips a code it does not recognize without an error, so each one
	// must be a language scribeLanguages lists.
	"secondary_language_codes": func(value any) ([]scribeParam, error) {
		list, _ := value.(string)
		var params []scribeParam
		for _, code := range strings.Split(list, ",") {
			language, supported := scribeLanguage(code)
			if !supported {
				return nil, fmt.Errorf("has %q, which is not a language Scribe supports; use comma-separated ISO 639-1 or 639-3 codes such as \"es,fr\"", strings.TrimSpace(code))
			}
			params = append(params, scribeParam{"secondary_language_codes", language})
		}
		return params, nil
	},
}

func scribeFlag(value any, on scribeParam) ([]scribeParam, error) {
	enabled, isBool := value.(bool)
	if !isBool {
		return nil, errors.New("takes true or false")
	}
	if !enabled {
		return nil, nil
	}
	return []scribeParam{on}, nil
}

// scribeModelFor resolves a plan's model for the surface an adapter serves.
func scribeModelFor(id string, surface scribeSurface) (scribeModel, error) {
	model, known := scribeModels[id]
	if !known {
		return scribeModel{surface: surface}, nil
	}
	if model.surface != surface {
		return scribeModel{}, fmt.Errorf("elevenlabs %s adapter cannot run %q, a %s Scribe model", surface, id, model.surface)
	}
	return model, nil
}

// params turns the caller's ElevenLabs settings into vendor parameters in
// setting-name order, refusing a gated setting this model does not read.
func (m scribeModel) params(id string, options *protocol.SttOptions) ([]scribeParam, error) {
	var params []scribeParam
	for _, key := range options.ProviderKeys("elevenlabs") {
		value := options.Provider("elevenlabs")[key]
		translate, gated := scribeGatedOptions[key]
		if !gated {
			// The gateway refuses these names before routing. A caller that
			// drives the adapter directly would otherwise override or repeat a
			// parameter the adapter writes itself, past every gate above.
			if protocol.SttOptionKeyReserved(key) || strings.EqualFold(strings.TrimSpace(key), "keyterm_library") {
				return nil, &runtimepkg.ProviderError{Code: "invalid_request", Message: "ElevenLabs " + key + " is set by the gateway, not by provider options", Hint: "Use the diarization or keywords option, or turn_prediction, instead."}
			}
			params = append(params, scribeParam{key, protocol.SttOptionString(value)})
			continue
		}
		if !slices.Contains(m.options, key) {
			return nil, scribeUnsupported(id, key, func(candidate scribeModel) bool { return slices.Contains(candidate.options, key) })
		}
		translated, err := translate(value)
		if err != nil {
			return nil, &runtimepkg.ProviderError{Code: "invalid_request", Message: "ElevenLabs " + key + " " + err.Error(), Hint: "Correct the option and try again."}
		}
		params = append(params, translated...)
	}
	return params, nil
}

// realtimeKeyterms splits the caller's keywords between the `keyterms` query
// and the keyterm library. A model without a library gets every keyword on the
// query, unchanged. With one, a keyword past the query's limits rides the
// library instead of being answered with a closed socket.
func (m scribeModel) realtimeKeyterms(keywords []string) (query, library []string, err error) {
	if !m.keytermLibrary {
		return keywords, nil, nil
	}
	libraryBytes := 0
	for _, keyword := range keywords {
		length := utf8.RuneCountInString(keyword)
		switch {
		case length <= realtimeKeytermChars && len(query) < realtimeKeytermsMax:
			query = append(query, keyword)
		case length <= keytermLibraryTermChars:
			library = append(library, keyword)
			libraryBytes += len(keyword)
		default:
			return nil, nil, &runtimepkg.ProviderError{Code: "invalid_request", Message: fmt.Sprintf("ElevenLabs keywords are limited to %d characters each", keytermLibraryTermChars), Hint: "Shorten the keyword and try again."}
		}
	}
	if len(library) > keytermLibraryMaxTerms || libraryBytes > keytermLibraryMaxBytes {
		return nil, nil, &runtimepkg.ProviderError{Code: "invalid_request", Message: fmt.Sprintf("ElevenLabs accepts at most %d library keywords and 2 MB of keyword text", keytermLibraryMaxTerms), Hint: "Send fewer keywords and try again."}
	}
	return query, library, nil
}

// scribeUnsupported is the refusal for a feature the pinned model lacks. It
// names the models that have it, so the caller knows what to pin.
func scribeUnsupported(id, feature string, has func(scribeModel) bool) error {
	var capable []string
	for candidate, model := range scribeModels {
		if has(model) {
			capable = append(capable, candidate)
		}
	}
	sort.Strings(capable)
	return &runtimepkg.ProviderError{
		Code:    "invalid_request",
		Message: fmt.Sprintf("ElevenLabs %s does not support %s", id, feature),
		Hint:    fmt.Sprintf("Pin %s or drop the option.", strings.Join(capable, " or ")),
	}
}

// ScribeRealtimeCapabilities reports the model-gated provider_options settings
// a realtime Scribe model reads and whether it diarizes, for callers that
// publish the same support table (the gateway's option allow-list).
func ScribeRealtimeCapabilities(model string) (options []string, diarization bool) {
	capabilities, err := scribeModelFor(model, scribeRealtime)
	if err != nil {
		return nil, false
	}
	return slices.Clone(capabilities.options), capabilities.diarization
}
