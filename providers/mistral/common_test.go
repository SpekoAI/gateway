package mistral

import (
	"errors"
	"net/http"
	"testing"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

func TestClassifyReadsTheStatusThenTheBody(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		body      string
		status    int
		want      string
		retryable bool
	}{
		// The bodies are the ones the live API returned on 2026-10-21.
		{name: "refused key", body: `{"detail":"Invalid API Key"}`, status: http.StatusUnauthorized, want: "authentication_failed"},
		{name: "model not open to the key", status: http.StatusForbidden, want: "authentication_failed"},
		{name: "unknown voice", body: `{"object":"error","message":"Voice 'nope_x' not found.","type":"invalid_voice","param":null,"code":"1902","raw_status_code":404}`, status: http.StatusNotFound, want: "invalid_request"},
		{name: "speed", body: `{"object":"error","message":{"detail":[{"type":"extra_forbidden","loc":["body","speed"],"msg":"Extra inputs are not permitted","input":1.1}]},"type":"invalid_request_error","param":null,"code":null,"raw_status_code":422}`, status: http.StatusUnprocessableEntity, want: "invalid_request"},
		{name: "word timestamps", body: `{"object":"error","message":"Word-level segments are not enabled for this model","type":"invalid_request_invalid_args","param":null,"code":"3051","raw_status_code":400}`, status: http.StatusBadRequest, want: "invalid_request"},
		{name: "moderation", body: `{"object":"error","message":"Input refused by moderation","type":"forbidden","code":"4000"}`, status: http.StatusForbidden, want: "invalid_request"},
		{name: "quota", body: `{"object":"error","message":"Monthly quota exceeded","type":"rate_limited"}`, status: http.StatusTooManyRequests, want: "provider_quota_exceeded"},
		{name: "rate limit", body: `{"object":"error","message":"Requests rate limit exceeded","type":"rate_limited"}`, status: http.StatusTooManyRequests, want: "provider_rate_limited", retryable: true},
		{name: "payment", status: http.StatusPaymentRequired, want: "provider_quota_exceeded"},
		{name: "too large", status: http.StatusRequestEntityTooLarge, want: "input_too_large"},
		{name: "outage", status: http.StatusServiceUnavailable, want: "provider_unavailable", retryable: true},
		{name: "validation frame", body: `{"message":"target_streaming_delay_ms must be one of the trained values: ['80-1200', '2400'] (got 5000).","code":3051}`, want: "invalid_request"},
		{name: "unknown frame", body: `{"message":"internal error","code":1000}`, want: "provider_unavailable", retryable: true},
		{name: "vendor invalid type", body: `{"message":"bad input","type":"invalid_request_error"}`, want: "invalid_request"},
		{name: "vendor auth type", body: `{"message":"no","type":"authentication_error"}`, want: "authentication_failed"},
		{name: "vendor rate type", body: `{"message":"slow down","type":"rate_limit_error"}`, want: "provider_rate_limited", retryable: true},
		{name: "string raw status", body: `{"message":"no","raw_status_code":"403"}`, want: "authentication_failed"},
		{name: "odd field types", body: `{"message":7,"type":["x"],"code":{"a":1},"raw_status_code":true}`, want: "provider_unavailable", retryable: true},
	}
	for _, tc := range cases {
		got, retryable := classify(parseError([]byte(tc.body)), tc.status)
		if got != tc.want || retryable != tc.retryable {
			t.Errorf("%s: classify = %s/%v, want %s/%v", tc.name, got, retryable, tc.want, tc.retryable)
		}
	}
}

func TestProviderErrorKeepsTheVendorTextOutOfTheMessage(t *testing.T) {
	t.Parallel()
	raw := []byte(`{"object":"error","message":"Voice 'nope_x' not found.","type":"invalid_voice","param":null,"code":"1902","raw_status_code":404}`)
	err := providerError("Mistral TTS rejected the synthesis request", parseError(raw), http.StatusNotFound, raw)
	if err.Code != "invalid_request" || err.Message != "Mistral TTS rejected the synthesis request (invalid_voice)" || string(err.Extensions[extensionID]) != string(raw) {
		t.Fatalf("error = %+v", err)
	}
	if plain := providerError("x", parseError([]byte("<html>")), http.StatusBadGateway, []byte("<html>")); plain.Extensions != nil || plain.Code != "provider_unavailable" {
		t.Fatalf("a non-JSON body was attached or misread: %+v", plain)
	}
}

func TestSTTLanguageTakesTheListedPrimarySubtag(t *testing.T) {
	t.Parallel()
	for language, want := range map[string]string{"": "", "auto": "", "en-US": "en", " PT_br ": "pt", "fil": "tl", "nb-NO": "no", "lb": "lb", "zh-Hans": "zh"} {
		if got, err := sttLanguage(language); err != nil || got != want {
			t.Errorf("sttLanguage(%q) = %q %v, want %q", language, got, err, want)
		}
	}
	for _, language := range []string{"sw", "he-IL", "yue"} {
		_, err := sttLanguage(language)
		var providerErr *runtimepkg.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != "unsupported_language" || providerErr.Hint == "" {
			t.Errorf("sttLanguage(%q) = %v, want unsupported_language with a hint", language, err)
		}
	}
	if len(languages) != 58 {
		t.Fatalf("languages lists %d codes, want the 58 the models list", len(languages))
	}
}

func TestCredentialKinds(t *testing.T) {
	t.Parallel()
	if !acceptableCredentialKind(protocol.RouteProviderDirect, protocol.CredentialBearer) || !acceptableCredentialKind(protocol.RouteSpekoRelay, protocol.CredentialRelayAccess) {
		t.Fatal("a permanent key was refused")
	}
	if acceptableCredentialKind(protocol.RouteProviderDirect, protocol.CredentialRelayAccess) {
		t.Fatal("relay_access accepted off the relay route")
	}
}
