package hopper

import (
	"errors"
	"net/http"
	"testing"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

func TestClassifyPrefersTheErrorCodeOverTheStatus(t *testing.T) {
	t.Parallel()
	cases := []struct {
		code      string
		status    int
		want      string
		retryable bool
	}{
		{code: "invalid_api_key", status: http.StatusBadRequest, want: "authentication_failed"},
		{code: "api_key_disabled", status: http.StatusUnauthorized, want: "authentication_failed"},
		{code: "insufficient_quota", status: http.StatusBadRequest, want: "provider_quota_exceeded"},
		{code: "rate_limit_exceeded", status: http.StatusBadRequest, want: "provider_rate_limited", retryable: true},
		{code: "transcript_too_long", status: http.StatusBadRequest, want: "input_too_large"},
		{code: "body_too_large", status: http.StatusRequestEntityTooLarge, want: "input_too_large"},
		{code: "unsupported_audio_format", status: http.StatusBadRequest, want: "unsupported_media"},
		{code: "invalid_sample_rate", status: http.StatusBadRequest, want: "unsupported_media"},
		{code: "unsupported_parameter", status: http.StatusBadRequest, want: "invalid_request"},
		{code: "voice_not_found", status: http.StatusNotFound, want: "invalid_request"},
		{code: "upstream_timeout", status: http.StatusGatewayTimeout, want: "request_timeout", retryable: true},
		{code: "model_offline", status: http.StatusServiceUnavailable, want: "provider_unavailable", retryable: true},
		{code: "upstream_error", status: http.StatusBadGateway, want: "provider_unavailable", retryable: true},
		{status: http.StatusForbidden, want: "authentication_failed"},
		{status: http.StatusPaymentRequired, want: "provider_quota_exceeded"},
		{status: http.StatusRequestEntityTooLarge, want: "input_too_large"},
		{status: http.StatusTooManyRequests, want: "provider_rate_limited", retryable: true},
		{status: http.StatusBadRequest, want: "invalid_request"},
		{status: http.StatusServiceUnavailable, want: "provider_unavailable", retryable: true},
		{status: 0, want: "provider_unavailable", retryable: true},
	}
	for _, tc := range cases {
		got, retryable := classify(tc.code, tc.status)
		if got != tc.want || retryable != tc.retryable {
			t.Errorf("classify(%q, %d) = %s/%v, want %s/%v", tc.code, tc.status, got, retryable, tc.want, tc.retryable)
		}
	}
}

func TestProviderErrorKeepsTheVendorTextOutOfTheMessage(t *testing.T) {
	t.Parallel()
	raw := []byte(`{"error":{"message":"Incorrect API key provided","type":"invalid_request_error","code":"invalid_api_key","param":null}}`)
	err := providerError("Hopper TTS rejected the synthesis request", parseError(raw), http.StatusUnauthorized, raw)
	if err.Code != "authentication_failed" || err.Message != "Hopper TTS rejected the synthesis request (invalid_api_key)" || string(err.Extensions[extensionID]) != string(raw) {
		t.Fatalf("error = %+v", err)
	}
	if plain := providerError("x", parseError([]byte("<html>")), http.StatusBadGateway, []byte("<html>")); plain.Extensions != nil || plain.Code != "provider_unavailable" {
		t.Fatalf("a non-JSON body was attached or misread: %+v", plain)
	}
}

func TestSTTLanguageIsEnglishOnly(t *testing.T) {
	t.Parallel()
	for _, language := range []string{"", "en", "en-US", " EN_gb "} {
		if got, err := sttLanguage(language); err != nil || got != "en" {
			t.Errorf("sttLanguage(%q) = %q %v, want en", language, got, err)
		}
	}
	for _, language := range []string{"fr", "ar-SA", "auto"} {
		_, err := sttLanguage(language)
		var providerErr *runtimepkg.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Code != "unsupported_language" || providerErr.Hint == "" {
			t.Errorf("sttLanguage(%q) = %v, want unsupported_language with a hint", language, err)
		}
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
