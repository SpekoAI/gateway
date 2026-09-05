package protocol

import (
	"encoding/json"
	"strings"
	"testing"
)

func boolPtr(value bool) *bool { return &value }

// A RequestOptions that asks for nothing must marshal byte-identically to
// what it produced before SttOptions existed: the field is a pointer exactly
// so that older control planes and fingerprint caches never see a new key.
func TestRequestOptionsWithoutSttOptionsIsByteIdentical(t *testing.T) {
	t.Parallel()
	options := RequestOptions{Provider: "auto", Language: "en", Model: "auto"}
	encoded, err := json.Marshal(options)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(encoded), "stt") {
		t.Fatalf("a request that asked for nothing must not mention stt: %s", encoded)
	}
}

func TestSttOptionsRoundTrip(t *testing.T) {
	t.Parallel()
	source := `{"diarization":true,"keywords":["Speko","Casey"],"noise_reduction":false,"provider_options":{"deepgram":{"numerals":true}}}`
	var options SttOptions
	if err := json.Unmarshal([]byte(source), &options); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !options.Diarize() {
		t.Fatal("diarization true must survive the trip")
	}
	// An explicit false is not the same as unset: it survives as a value.
	if options.NoiseReduction == nil || *options.NoiseReduction {
		t.Fatal("noise_reduction false must stay an explicit false")
	}
	if options.ReduceNoise() {
		t.Fatal("an explicit false is not an ask")
	}
	if len(options.GetKeywords()) != 2 {
		t.Fatalf("keywords: %v", options.Keywords)
	}
	if err := options.Normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if _, exists := options.Provider("deepgram")["numerals"]; !exists {
		t.Fatal("provider option must survive normalization")
	}
}

func TestSttOptionsZeroState(t *testing.T) {
	t.Parallel()
	var nilOptions *SttOptions
	if !nilOptions.IsZero() || nilOptions.Diarize() || nilOptions.ReduceNoise() {
		t.Fatal("a nil options pointer asks for nothing")
	}
	if nilOptions.GetKeywords() != nil || nilOptions.Provider("deepgram") != nil {
		t.Fatal("nil accessors must be safe and empty")
	}
	empty := &SttOptions{}
	if !empty.IsZero() {
		t.Fatal("an empty struct asks for nothing")
	}
	// Diarization: explicit false is NOT zero — the caller said something.
	explicit := &SttOptions{Diarization: boolPtr(false)}
	if explicit.IsZero() {
		t.Fatal("an explicit false is a statement, not silence")
	}
	words := &SttOptions{WordTimestamps: boolPtr(true)}
	if words.IsZero() || !words.WantsWordTimestamps() || nilOptions.WantsWordTimestamps() {
		t.Fatal("a word_timestamps ask must be visible and nil-safe")
	}
}

func TestSttOptionsKeywordBounds(t *testing.T) {
	t.Parallel()
	tooMany := &SttOptions{Keywords: make([]string, maxSttKeywords+1)}
	for index := range tooMany.Keywords {
		tooMany.Keywords[index] = "term"
	}
	if err := tooMany.Normalize(); err == nil {
		t.Fatal("101 keywords must be refused")
	}
	long := &SttOptions{Keywords: []string{strings.Repeat("x", maxSttKeywordLength+1)}}
	if err := long.Normalize(); err == nil {
		t.Fatal("a 65-character keyword must be refused")
	}
	control := &SttOptions{Keywords: []string{"line\r\nbreak"}}
	if err := control.Normalize(); err == nil {
		t.Fatal("a control character inside a keyword must be refused")
	}
	fine := &SttOptions{Keywords: []string{"Speko", "Bäckerstraße", "São Paulo"}}
	if err := fine.Normalize(); err != nil {
		t.Fatalf("multi-word and non-ASCII keywords are legitimate vocabulary: %v", err)
	}
	// The limit is a CHARACTER count: a 40-character Cyrillic term is 80 UTF-8
	// bytes, and a byte count would reject exactly the multilingual vocabulary
	// these options exist to carry.
	cyrillic := &SttOptions{Keywords: []string{strings.Repeat("д", maxSttKeywordLength)}}
	if err := cyrillic.Normalize(); err != nil {
		t.Fatalf("a 64-character non-ASCII keyword is within the limit: %v", err)
	}
	overRunes := &SttOptions{Keywords: []string{strings.Repeat("д", maxSttKeywordLength+1)}}
	if err := overRunes.Normalize(); err == nil {
		t.Fatal("65 characters is over the limit whatever the encoding")
	}
}

// Two spellings folding onto one name are refused, not merged: map iteration
// order would otherwise decide which survives, and two byte-identical bodies
// could normalize to different settings across idempotent replays.
func TestSttOptionsCaseFoldCollisionsAreRefused(t *testing.T) {
	t.Parallel()
	providers := &SttOptions{ProviderOptions: map[string]map[string]any{
		"DeepGram": {"numerals": true},
		"deepgram": {"punctuate": false},
	}}
	if err := providers.Normalize(); err == nil {
		t.Fatal("case-distinct provider names folding together must be refused")
	}
	settings := &SttOptions{ProviderOptions: map[string]map[string]any{
		"deepgram": {"Numerals": true, "numerals": false},
	}}
	if err := settings.Normalize(); err == nil {
		t.Fatal("case-distinct setting names folding together must be refused")
	}
}

func TestSttOptionsProviderOptionShape(t *testing.T) {
	t.Parallel()
	// Non-scalar values are refused: no evidenced vendor setting nests.
	for _, source := range []string{
		`{"provider_options":{"deepgram":{"punctuate":[true]}}}`,
		`{"provider_options":{"deepgram":{"punctuate":{"on":true}}}}`,
		`{"provider_options":{"deepgram":{"punctuate":null}}}`,
	} {
		var options SttOptions
		if err := json.Unmarshal([]byte(source), &options); err != nil {
			t.Fatalf("unmarshal %s: %v", source, err)
		}
		if err := options.Normalize(); err == nil {
			t.Fatalf("non-scalar value must be refused: %s", source)
		}
	}
	// Reserved, gateway-owned names are refused whatever their case: a second
	// model bills one model and runs another.
	for _, key := range []string{"model", "Model", "language", "api_key", "diarize", "sample_rate"} {
		options := &SttOptions{ProviderOptions: map[string]map[string]any{"deepgram": {key: "x"}}}
		if err := options.Normalize(); err == nil {
			t.Fatalf("reserved key %q must be refused", key)
		}
	}
	// A long string and a control character are refused; both are written into
	// vendor URLs and frames verbatim.
	long := &SttOptions{ProviderOptions: map[string]map[string]any{"openai": {"prompt": strings.Repeat("y", maxSttOptionStringValue+1)}}}
	if err := long.Normalize(); err == nil {
		t.Fatal("an oversized string value must be refused")
	}
	sneaky := &SttOptions{ProviderOptions: map[string]map[string]any{"openai": {"prompt": "casey\r\n--boundary"}}}
	if err := sneaky.Normalize(); err == nil {
		t.Fatal("a control character inside a value must be refused")
	}
}

// Validation folds case and map lookup cannot: a document written
// {"Deepgram":{"Numerals":true}} must reach the adapter under the lower-case
// names the adapters look up, or it validates and then silently forwards
// nothing.
func TestSttOptionsNormalizeFoldsCase(t *testing.T) {
	t.Parallel()
	options := &SttOptions{ProviderOptions: map[string]map[string]any{"DeepGram": {"Numerals": true}}}
	if err := options.Normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if _, exists := options.Provider("deepgram")["numerals"]; !exists {
		t.Fatal("mixed-case names must fold to what adapters look up")
	}
	if options.Provider("DEEPGRAM")["numerals"] != true {
		t.Fatal("Provider() folds its argument too")
	}
	keys := options.ProviderKeys("deepgram")
	if len(keys) != 1 || keys[0] != "numerals" {
		t.Fatalf("keys: %v", keys)
	}
}

func TestSttOptionStringSpellsScalarsLikeACaller(t *testing.T) {
	t.Parallel()
	if SttOptionString("nova") != "nova" {
		t.Fatal("strings pass unquoted")
	}
	if SttOptionString(true) != "true" || SttOptionString(false) != "false" {
		t.Fatal("booleans spell their JSON form")
	}
	// JSON numbers arrive as float64; integral values must not grow a decimal
	// point on the vendor URL.
	if SttOptionString(float64(1200)) != "1200" {
		t.Fatalf("integral float: %s", SttOptionString(float64(1200)))
	}
	if SttOptionString(0.7) != "0.7" {
		t.Fatalf("fraction: %s", SttOptionString(0.7))
	}
}
