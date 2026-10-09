package deepgram

import "strings"

// Regional and script-bearing language codes Deepgram documents per model, from
// https://developers.deepgram.com/docs/models-languages-overview (read
// 2026-10-09). Every other region-bearing tag is refused outright — nova-3
// answers `sw-KE` with a 400 while `sw` transcribes — so a tag outside its
// model's set is reduced to the primary subtag instead. Keys are lowercase for
// matching; values are the documented spelling sent on the wire.
var (
	nova3RegionalTags = regionalTags(
		"af-ZA", "ar-AE", "ar-SA", "ar-QA", "ar-KW", "ar-SY", "ar-LB", "ar-PS", "ar-JO", "ar-EG", "ar-SD", "ar-TD",
		"ar-MA", "ar-DZ", "ar-TN", "ar-IQ", "ar-IR", "as-IN", "zh-HK", "zh-CN", "zh-Hans", "zh-TW", "zh-Hant",
		"cs-CZ", "da-DK", "en-US", "en-AU", "en-GB", "en-IN", "en-NZ", "nl-BE", "fr-CA", "ka-GE", "de-CH", "gu-IN",
		"kk-KZ", "ko-KR", "ps-AF", "pt-BR", "pt-PT", "pa-IN", "es-419", "sv-SE", "th-TH", "tr-TR",
	)
	nova3EnglishRegionalTags = regionalTags("en-US", "en-AU", "en-CA", "en-GB", "en-IE", "en-IN", "en-NZ")
	nova2RegionalTags        = regionalTags(
		"zh-CN", "zh-Hans", "zh-TW", "zh-Hant", "zh-HK", "da-DK", "en-US", "en-AU", "en-GB", "en-NZ", "en-IN",
		"nl-BE", "fr-CA", "de-CH", "ko-KR", "pt-BR", "pt-PT", "es-419", "sv-SE", "th-TH",
	)
	novaRegionalTags     = regionalTags("en-US", "en-AU", "en-GB", "en-NZ", "en-IN", "es-419", "hi-Latn")
	enhancedRegionalTags = regionalTags("en-US", "pt-BR", "pt-PT", "es-419", "es-LATAM")
	baseRegionalTags     = regionalTags("zh-CN", "zh-TW", "en-US", "fr-CA", "hi-Latn", "pt-BR", "pt-PT", "es-419", "es-LATAM")
	// Every domain variant (nova-2-phonecall, base-meeting, …) is English-only.
	englishVariantRegionalTags = regionalTags("en-US")
	// A model the table does not know yet keeps any region Deepgram documents
	// for some model, rather than silently losing en-GB on a new release.
	allRegionalTags = mergeRegionalTags(nova3RegionalTags, nova3EnglishRegionalTags, nova2RegionalTags, novaRegionalTags, enhancedRegionalTags, baseRegionalTags)
)

// deepgramLanguage is the language value sent for model: a documented regional
// code in its documented spelling, otherwise the lowercase primary subtag.
// Bare codes (`sw`, `id`, `multi`) pass through unchanged.
func deepgramLanguage(model, language string) string {
	language = strings.TrimSpace(language)
	separator := strings.IndexAny(language, "-_")
	if separator <= 0 {
		return language
	}
	if documented, ok := regionalTagsFor(model)[strings.ToLower(strings.ReplaceAll(language, "_", "-"))]; ok {
		return documented
	}
	return strings.ToLower(language[:separator])
}

func regionalTagsFor(model string) map[string]string {
	switch {
	case isFluxModel(model), strings.HasPrefix(model, "whisper"):
		// Flux language_hint and Whisper take bare ISO 639-1 codes only.
		return nil
	case model == "nova-3-medical" || model == "nova-3-pharma":
		return nova3EnglishRegionalTags
	case strings.HasPrefix(model, "nova-3"):
		return nova3RegionalTags
	case model == "nova-2" || model == "nova-2-general":
		return nova2RegionalTags
	case model == "nova" || model == "nova-general":
		return novaRegionalTags
	case model == "enhanced" || model == "enhanced-general":
		return enhancedRegionalTags
	case model == "base" || model == "base-general":
		return baseRegionalTags
	case strings.HasPrefix(model, "nova-2-"), strings.HasPrefix(model, "nova-"), strings.HasPrefix(model, "enhanced-"), strings.HasPrefix(model, "base-"):
		return englishVariantRegionalTags
	default:
		return allRegionalTags
	}
}

func regionalTags(tags ...string) map[string]string {
	set := make(map[string]string, len(tags))
	for _, tag := range tags {
		set[strings.ToLower(tag)] = tag
	}
	return set
}

func mergeRegionalTags(sets ...map[string]string) map[string]string {
	merged := make(map[string]string)
	for _, set := range sets {
		for key, tag := range set {
			merged[key] = tag
		}
	}
	return merged
}
