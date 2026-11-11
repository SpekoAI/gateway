package elevenlabs

import "slices"

// scribeLanguages are the ISO 639-3 codes of the languages ElevenLabs
// documents for Scribe v2 and v3 (speech-to-text capabilities, "Supported
// languages", including the per-language accuracy breakdown that adds cmn and
// yor). Scribe skips a secondary language it does not recognize without an
// error, so the adapter refuses any code that does not resolve to one of these.
var scribeLanguages = []string{
	"afr", "amh", "ara", "hye", "asm", "ast", "aze", "bel", "ben", "bos",
	"bul", "mya", "yue", "cat", "ceb", "nya", "hrv", "ces", "dan", "nld",
	"eng", "est", "fil", "fin", "fra", "ful", "glg", "lug", "kat", "deu",
	"ell", "guj", "hau", "heb", "hin", "hun", "isl", "ibo", "ind", "gle",
	"ita", "jpn", "jav", "kea", "kan", "kaz", "khm", "kor", "kur", "kir",
	"lao", "lav", "lin", "lit", "luo", "ltz", "mkd", "msa", "mal", "mlt",
	"zho", "cmn", "mri", "mar", "mon", "nep", "nso", "nor", "oci", "ori",
	"pus", "fas", "pol", "por", "pan", "ron", "rus", "srp", "sna", "snd",
	"sin", "slk", "slv", "som", "spa", "swa", "swe", "tam", "tgk", "tel",
	"tha", "tur", "ukr", "umb", "urd", "uzb", "vie", "cym", "wol", "xho",
	"yor", "zul",
}

// scribeISO6391 maps the ISO 639-1 code of each language in scribeLanguages
// that has one to its 639-3 code. Asturian, Cantonese, Cebuano, Filipino,
// Kabuverdianu, Luo, Northern Sotho, Umbundu and Mandarin (cmn) have no 639-1
// code and are named by their 639-3 code.
var scribeISO6391 = map[string]string{
	"af": "afr", "am": "amh", "ar": "ara", "hy": "hye", "as": "asm", "az": "aze", "be": "bel", "bn": "ben",
	"bs": "bos", "bg": "bul", "my": "mya", "ca": "cat", "ny": "nya", "hr": "hrv", "cs": "ces", "da": "dan",
	"nl": "nld", "en": "eng", "et": "est", "fi": "fin", "fr": "fra", "ff": "ful", "gl": "glg", "lg": "lug",
	"ka": "kat", "de": "deu", "el": "ell", "gu": "guj", "ha": "hau", "he": "heb", "hi": "hin", "hu": "hun",
	"is": "isl", "ig": "ibo", "id": "ind", "ga": "gle", "it": "ita", "ja": "jpn", "jv": "jav", "kn": "kan",
	"kk": "kaz", "km": "khm", "ko": "kor", "ku": "kur", "ky": "kir", "lo": "lao", "lv": "lav", "ln": "lin",
	"lt": "lit", "lb": "ltz", "mk": "mkd", "ms": "msa", "ml": "mal", "mt": "mlt", "zh": "zho", "mi": "mri",
	"mr": "mar", "mn": "mon", "ne": "nep", "no": "nor", "oc": "oci", "or": "ori", "ps": "pus", "fa": "fas",
	"pl": "pol", "pt": "por", "pa": "pan", "ro": "ron", "ru": "rus", "sr": "srp", "sn": "sna", "sd": "snd",
	"si": "sin", "sk": "slk", "sl": "slv", "so": "som", "es": "spa", "sw": "swa", "sv": "swe", "ta": "tam",
	"tg": "tgk", "te": "tel", "th": "tha", "tr": "tur", "uk": "ukr", "ur": "urd", "uz": "uzb", "vi": "vie",
	"cy": "cym", "wo": "wol", "xh": "xho", "yo": "yor", "zu": "zul",
}

// scribeLanguage resolves a caller's language code, regional tag or
// Norwegian alias to the 639-3 code Scribe documents. The documented code is
// what goes to the vendor, so recognition never rests on a 639-1 spelling
// the docs do not list.
func scribeLanguage(code string) (string, bool) {
	code = sttLanguageCode(code)
	if slices.Contains(scribeLanguages, code) {
		return code, true
	}
	iso6393, mapped := scribeISO6391[code]
	return iso6393, mapped
}
