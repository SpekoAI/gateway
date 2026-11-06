package omi

import (
	"errors"
	"net/url"
	"testing"

	"github.com/SpekoAI/gateway/gateway"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

func TestLanguagesMapByBaseCode(t *testing.T) {
	t.Parallel()
	for tag, want := range map[string][2]string{
		"":      {"en", "auto"},
		"auto":  {"", "auto"},
		"en-US": {"en", "en"},
		"es_MX": {"es", "es"},
		"PT-br": {"pt", "pt"},
		"fr-CA": {"fr", "fr"},
		"de":    {"de", "de"},
		"nl-BE": {"nl", "nl"},
		"ar-EG": {"ar", "ar"},
		"hi-IN": {"hi", "hi"},
		"ja":    {"", ""},
		"zh-CN": {"", ""},
	} {
		realtime, realtimeErr := realtimeLanguage(tag)
		batch, batchErr := batchLanguage(tag)
		if realtime != want[0] || batch != want[1] {
			t.Errorf("%q: realtime %q batch %q, want %q %q", tag, realtime, batch, want[0], want[1])
		}
		for surface, err := range map[string]error{"realtime": realtimeErr, "batch": batchErr} {
			index := map[string]int{"realtime": 0, "batch": 1}[surface]
			var providerErr *runtimepkg.ProviderError
			if want[index] == "" && (!errors.As(err, &providerErr) || providerErr.Code != "unsupported_language") {
				t.Errorf("%q %s: error = %v, want unsupported_language", tag, surface, err)
			}
			if want[index] != "" && err != nil {
				t.Errorf("%q %s: error = %v", tag, surface, err)
			}
		}
	}
}

func TestRealtimeNeverSendsAuto(t *testing.T) {
	t.Parallel()
	for _, tag := range []string{"", " ", "auto", "AUTO", "auto-detect"} {
		if language, _ := realtimeLanguage(tag); language == "auto" {
			t.Fatalf("%q mapped to auto", tag)
		}
	}
}

func TestClassifyBranchesOnCodeNotMessage(t *testing.T) {
	t.Parallel()
	cases := []struct {
		code, message string
		status        int
		want          string
		retryable     bool
	}{
		{"UNAUTHORIZED", "", 0, "authentication_failed", false},
		{"", "", 401, "authentication_failed", false},
		{"billing_blocked", "", 402, "provider_quota_exceeded", false},
		{"", "", 403, "invalid_request", false},
		{"TOO_MANY_STREAMS", "", 0, "provider_rate_limited", true},
		{"SPEAKER_CAPACITY_FULL", "", 0, "provider_rate_limited", true},
		{"queue_full", "", 429, "provider_rate_limited", true},
		{"tenant_upload_concurrency_exceeded", "", 429, "provider_rate_limited", true},
		{"capacity_unavailable", "", 503, "provider_unavailable", true},
		{"", "", 503, "provider_unavailable", true},
		{"audio_too_long", "", 422, "input_too_large", false},
		{"", "", 413, "input_too_large", false},
		{"UNSUPPORTED_AUDIO_FORMAT", "", 0, "unsupported_media", false},
		{"", "", 415, "unsupported_media", false},
		{"UNSUPPORTED_LANGUAGE", "", 0, "unsupported_language", false},
		{"invalid_request", "Unsupported language 'ja'", 422, "unsupported_language", false},
		{"invalid_request", "Unsupported language 'ja'", 400, "invalid_request", false},
		{"invalid_request", "bad field", 422, "invalid_request", false},
		// The message alone never decides the class.
		{"", "unauthorized key", 500, "provider_unavailable", true},
		{"", "", 307, "provider_error", false},
	}
	for _, test := range cases {
		got, retryable := classify(&errorDetail{Code: test.code, Message: test.message}, test.status)
		if got != test.want || retryable != test.retryable {
			t.Errorf("classify(%q, %q, %d) = %s/%v, want %s/%v", test.code, test.message, test.status, got, retryable, test.want, test.retryable)
		}
	}
}

func TestTheCatalogRowMatchesTheAdapter(t *testing.T) {
	t.Parallel()
	var entry gateway.CatalogEntry
	for _, candidate := range gateway.Catalog() {
		if candidate.Provider == ProviderName {
			if entry.Provider != "" || candidate.Kind != protocol.SessionKindSTT {
				t.Fatalf("unexpected omi catalog row %+v", candidate)
			}
			entry = candidate
		}
	}
	if entry.Transport != protocol.TransportWebSocket || entry.Adapter != STTAdapterID || entry.DefaultModel != Model || entry.Endpoint != RealtimeEndpoint {
		t.Fatalf("catalog row = %+v", entry)
	}
	parsed, err := url.Parse(RealtimeEndpoint)
	if err != nil || parsed.Host != officialHost || parsed.Path != streamPath {
		t.Fatalf("RealtimeEndpoint = %s", RealtimeEndpoint)
	}
}
