package azure

import "strings"

// locales is the set of ISO-639-1 (plus `fil` and `yue`) codes the
// MAI-Transcribe-2 language table lists. The `locales` definition field takes
// exactly these bare codes and at most one of them; a code outside the set
// yields "" and the field is left unsent so the model falls back to its own
// language identification rather than the request failing on a locale it
// does not serve.
var locales = map[string]struct{}{
	"af": {}, "ar": {}, "as": {}, "az": {}, "bg": {}, "bn": {}, "bs": {}, "ca": {}, "cs": {}, "da": {},
	"de": {}, "el": {}, "en": {}, "es": {}, "et": {}, "fa": {}, "fi": {}, "fil": {}, "fr": {}, "gl": {},
	"gu": {}, "he": {}, "hi": {}, "hu": {}, "hy": {}, "id": {}, "is": {}, "it": {}, "ja": {}, "kk": {},
	"kn": {}, "ko": {}, "lt": {}, "lv": {}, "mk": {}, "ml": {}, "mr": {}, "ms": {}, "nb": {}, "ne": {},
	"nl": {}, "or": {}, "pa": {}, "pl": {}, "pt": {}, "ro": {}, "ru": {}, "sk": {}, "sl": {}, "sv": {},
	"sw": {}, "ta": {}, "te": {}, "th": {}, "tr": {}, "uk": {}, "ur": {}, "vi": {}, "yue": {}, "zh": {},
}

// legacyCodes folds the retired ISO codes and common aliases callers still
// send onto the code the table lists.
var legacyCodes = map[string]string{
	"iw":  "he", // Hebrew, pre-1989 code
	"in":  "id", // Indonesian, pre-1989 code
	"tl":  "fil",
	"cmn": "zh",
	"no":  "nb", // Norwegian; the table lists Bokmål only
	"nn":  "nb",
}

// locale renders a caller's language ask as the single `locales` entry. A tag
// such as "en-US" or "pt_BR" is reduced to its primary subtag; "zh-HK" is not
// special-cased to Cantonese because the tag does not say so — a caller who
// means Cantonese sends "yue".
func locale(language string) string {
	primary := strings.ToLower(strings.TrimSpace(language))
	if primary == "" {
		return ""
	}
	if index := strings.IndexAny(primary, "-_"); index > 0 {
		primary = primary[:index]
	}
	if folded, ok := legacyCodes[primary]; ok {
		primary = folded
	}
	if _, ok := locales[primary]; ok {
		return primary
	}
	return ""
}
