package munsit

import (
	"net/http"
	"testing"

	"github.com/SpekoAI/gateway/protocol"
)

func TestClassifyPrefersTheErrorCodeOverTheStatus(t *testing.T) {
	t.Parallel()
	cases := []struct {
		code, status int
		want         string
		retryable    bool
	}{
		{code: 40101, status: http.StatusBadRequest, want: "authentication_failed"},
		{code: 40201, status: http.StatusBadRequest, want: "provider_quota_exceeded"},
		{code: 42901, status: http.StatusBadRequest, want: "provider_rate_limited", retryable: true},
		{code: 40001, status: http.StatusInternalServerError, want: "invalid_request"},
		{code: 50001, status: http.StatusBadRequest, want: "provider_unavailable", retryable: true},
		{code: 50202, status: http.StatusBadGateway, want: "provider_unavailable", retryable: true},
		{status: http.StatusForbidden, want: "authentication_failed"},
		{status: http.StatusPaymentRequired, want: "provider_quota_exceeded"},
		{status: http.StatusRequestEntityTooLarge, want: "input_too_large"},
		{status: http.StatusTooManyRequests, want: "provider_rate_limited", retryable: true},
		{status: http.StatusNotFound, want: "invalid_request"},
		{status: http.StatusServiceUnavailable, want: "provider_unavailable", retryable: true},
		{status: 0, want: "provider_unavailable", retryable: true},
	}
	for _, tc := range cases {
		got, retryable := classify(tc.code, tc.status)
		if got != tc.want || retryable != tc.retryable {
			t.Errorf("classify(%d, %d) = %s/%v, want %s/%v", tc.code, tc.status, got, retryable, tc.want, tc.retryable)
		}
	}
}

func TestProviderErrorKeepsTheVendorTextOutOfTheMessage(t *testing.T) {
	t.Parallel()
	raw := []byte(`{"errorCode":40101,"errorMessage":"Invalid API key"}`)
	err := providerError("Munsit TTS rejected the synthesis request", restError{ErrorCode: 40101, ErrorMessage: "Invalid API key"}, http.StatusUnauthorized, raw)
	if err.Message != "Munsit TTS rejected the synthesis request (40101)" || string(err.Extensions[extensionID]) != string(raw) {
		t.Fatalf("error = %+v", err)
	}
	if plain := providerError("x", restError{}, http.StatusBadGateway, []byte("<html>")); plain.Extensions != nil {
		t.Fatalf("a non-JSON body was attached: %v", plain.Extensions)
	}
}

func TestBaseLanguageStripsTheRegion(t *testing.T) {
	t.Parallel()
	for input, want := range map[string]string{"ar-SA": "ar", " AR_eg ": "ar", "en": "en", "": "", "ar-Arab-EG": "ar"} {
		if got := baseLanguage(input); got != want {
			t.Errorf("baseLanguage(%q) = %q, want %q", input, got, want)
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
