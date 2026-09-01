package meta

import "strings"

// languageNames maps a BCP-47 primary language subtag to the language NAME
// the service's languageBias field takes. The set is the twenty-five
// languages the documentation lists; a tag outside it yields "" and the bias
// is left unsent, because the service documents no error shape for an
// unknown name and a silently ignored ask is worse than none.
var languageNames = map[string]string{
	"ar":  "Arabic",
	"bn":  "Bengali",
	"nl":  "Dutch",
	"en":  "English",
	"fr":  "French",
	"de":  "German",
	"he":  "Hebrew",
	"iw":  "Hebrew",
	"hi":  "Hindi",
	"id":  "Indonesian",
	"in":  "Indonesian",
	"it":  "Italian",
	"ja":  "Japanese",
	"kn":  "Kannada",
	"ko":  "Korean",
	"ms":  "Malay",
	"zh":  "Mandarin Chinese",
	"cmn": "Mandarin Chinese",
	"mr":  "Marathi",
	"pl":  "Polish",
	"pt":  "Portuguese",
	"es":  "Spanish",
	"tl":  "Tagalog",
	"fil": "Tagalog",
	"ta":  "Tamil",
	"te":  "Telugu",
	"th":  "Thai",
	"tr":  "Turkish",
	"vi":  "Vietnamese",
}

// languageBias renders a caller's language ask as a languageBias entry. A
// tag such as "en-US" or "pt_BR" is reduced to its primary subtag; a caller
// who already passes one of the documented names is passed through as is.
func languageBias(language string) string {
	language = strings.TrimSpace(language)
	if language == "" {
		return ""
	}
	for _, name := range languageNames {
		if strings.EqualFold(name, language) {
			return name
		}
	}
	primary := strings.ToLower(language)
	if index := strings.IndexAny(primary, "-_"); index > 0 {
		primary = primary[:index]
	}
	return languageNames[primary]
}
