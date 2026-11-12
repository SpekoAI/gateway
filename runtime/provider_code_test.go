package runtime_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/coder/websocket"

	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

func TestSafeProviderCodeReadsProviderErrorEnvelopes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		payload string
		want    string
	}{
		{"elevenlabs status is more specific than code", `{"detail":{"type":"invalid_request","code":"bad_request","status":"voice_limit_reached","message":"You have reached your maximum amount of custom voices"}}`, "voice_limit_reached"},
		{"elevenlabs voice_not_found as code and status", `{"detail":{"type":"not_found","code":"voice_not_found","status":"voice_not_found","message":"A voice with voice_id abc does not exist."}}`, "voice_not_found"},
		{"elevenlabs code without the legacy status", `{"detail":{"type":"validation_error","code":"invalid_parameters","message":"keyterms is not supported","param":"keyterms"}}`, "invalid_parameters"},
		{"elevenlabs legacy status only", `{"detail":{"status":"quota_exceeded","message":"This request exceeds your quota of 10000."}}`, "quota_exceeded"},
		{"elevenlabs websocket error frame", `{"message":"This request exceeds your quota.","error":"quota_exceeded","code":1008}`, "quota_exceeded"},
		{"openai code", `{"error":{"message":"You exceeded your current quota.","type":"insufficient_quota","param":null,"code":"insufficient_quota"}}`, "insufficient_quota"},
		{"openai null code falls to type", `{"error":{"message":"The server had an error.","type":"server_error","param":null,"code":null}}`, "server_error"},
		{"openai realtime error event", `{"type":"error","event_id":"evt_1","error":{"type":"invalid_request_error","code":"session_expired","message":"Your session hit the maximum duration."}}`, "session_expired"},
		{"anthropic type", `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`, "overloaded_error"},
		{"google numeric code yields to status", `{"error":{"code":429,"message":"Resource has been exhausted.","status":"RESOURCE_EXHAUSTED"}}`, "RESOURCE_EXHAUSTED"},
		{"top-level code", `{"code":"InvalidApiKey","message":"Invalid API-key provided.","request_id":"r1"}`, "InvalidApiKey"},
		{"top-level err_code", `{"err_code":"INVALID_AUTH","err_msg":"Invalid credentials.","request_id":"r1"}`, "INVALID_AUTH"},
		{"top-level error_code", `{"error_code":"rate_limited","message":"Slow down."}`, "rate_limited"},
		{"top-level error string", `{"error":"voice_not_found"}`, "voice_not_found"},
		{"error_code precedes code", `{"code":"generic","error_code":"specific"}`, "specific"},
		{"envelope precedes top level", `{"code":"outer","error":{"code":"inner"}}`, "inner"},
		{"free-text code falls through to the next field", `{"code":"something went wrong","error":"voice_not_found"}`, "voice_not_found"},
		{"free-text error string", `{"error":"Invalid API key provided"}`, ""},
		{"free-text envelope", `{"error":{"code":"bad request: voice abc","message":"x"}}`, ""},
		{"numeric codes only", `{"code":1008,"error_code":400}`, ""},
		{"top-level type is a frame name", `{"type":"done"}`, ""},
		{"a non-error frame", `{"audio":"AAAA","isFinal":false}`, ""},
		{"bare string body", `"Unauthorized"`, ""},
		{"array body", `{"detail":[{"loc":["body","text"],"msg":"field required","type":"value_error.missing"}]}`, ""},
		{"null error", `{"error":null}`, ""},
		{"not json", `upstream connect error`, ""},
		{"64 bytes", fmt.Sprintf(`{"code":%q}`, strings.Repeat("a", 64)), strings.Repeat("a", 64)},
		{"65 bytes", fmt.Sprintf(`{"code":%q}`, strings.Repeat("a", 65)), ""},
		{"url", `{"code":"https://api.example/v1?key=sk-1"}`, ""},
		{"newline", `{"code":"quota_exceeded\n"}`, ""},
		{"non-ascii", `{"code":"quota_épuisé"}`, ""},
		{"json in a string", `{"error":"{\"code\":\"x\"}"}`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			failure := &runtimepkg.ProviderError{Code: "provider_unavailable", Extensions: map[string]json.RawMessage{"vendor.example/v1": json.RawMessage(tc.payload)}}
			if got := failure.SafeProviderCode(); got != tc.want {
				t.Fatalf("SafeProviderCode(%s) = %q, want %q", tc.payload, got, tc.want)
			}
		})
	}
}

func TestSafeProviderCodePrecedence(t *testing.T) {
	t.Parallel()
	payload := map[string]json.RawMessage{"vendor.example/v1": json.RawMessage(`{"error":"from_payload"}`)}
	closed := fmt.Errorf("read: %w", websocket.CloseError{Code: websocket.StatusPolicyViolation, Reason: "from_close"})
	for _, tc := range []struct {
		name    string
		failure *runtimepkg.ProviderError
		want    string
	}{
		{"nil", nil, ""},
		{"empty", &runtimepkg.ProviderError{Code: "provider_unavailable"}, ""},
		{"adapter code wins", &runtimepkg.ProviderError{ProviderCode: "from_adapter", Extensions: payload, Cause: closed}, "from_adapter"},
		{"a free-text adapter code yields", &runtimepkg.ProviderError{ProviderCode: "not a code", Extensions: payload}, "from_payload"},
		{"payload before close reason", &runtimepkg.ProviderError{Extensions: payload, Cause: closed}, "from_payload"},
		{"close reason", &runtimepkg.ProviderError{Cause: closed}, "from_close"},
		{"close reason sentence", &runtimepkg.ProviderError{Cause: websocket.CloseError{Code: websocket.StatusPolicyViolation, Reason: "Model 'm' does not support language_code 'sw'."}}, ""},
		{"several extensions read in key order", &runtimepkg.ProviderError{Extensions: map[string]json.RawMessage{
			"b.example": json.RawMessage(`{"error":"second"}`),
			"a.example": json.RawMessage(`{"error":"first"}`),
		}}, "first"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.failure.SafeProviderCode(); got != tc.want {
				t.Fatalf("SafeProviderCode = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSanitizeProviderCode(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]string{
		"voice_not_found":         "voice_not_found",
		"RESOURCE_EXHAUSTED":      "RESOURCE_EXHAUSTED",
		"DATA-0000":               "DATA-0000",
		"value_error.missing":     "value_error.missing",
		"output_empty:max_tokens": "output_empty:max_tokens",
		"429":                     "429",
		"":                        "",
		"two words":               "",
		" leading":                "",
		"tab\there":               "",
		"a/b":                     "",
		"a=b":                     "",
		`"quoted"`:                "",
		"émoji":                   "",
		strings.Repeat("x", 65):   "",
	} {
		if got := runtimepkg.SanitizeProviderCode(in); got != want {
			t.Errorf("SanitizeProviderCode(%q) = %q, want %q", in, got, want)
		}
	}
}
