package gateway_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func ttsRequestBody(providerOptions map[string]map[string]any) map[string]any {
	body := gatewayRequestBody()
	body["kind"] = "tts"
	body["request"] = map[string]any{"model": "mock-model", "tts": map[string]any{"provider_options": providerOptions}}
	return body
}

func TestCreateRefusesTtsOptionsBeforeAnyPlan(t *testing.T) {
	t.Parallel()
	sttBody := gatewayRequestBody()
	sttBody["request"] = map[string]any{"model": "mock-model", "tts": map[string]any{"provider_options": map[string]any{"cartesia": map[string]any{"normalization": "en-IN"}}}}
	for _, test := range []struct {
		name string
		body map[string]any
		want string
	}{
		{"tts options on an stt session", sttBody, "valid only on tts sessions"},
		{"provider that takes no settings", ttsRequestBody(map[string]map[string]any{"cartesian": {"normalization": "en-IN"}}), "provider_options.cartesian"},
		{"reserved key", ttsRequestBody(map[string]map[string]any{"elevenlabs": {"model_id": "eleven_v3"}}), "owned by the gateway"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			gatewayServer, plans := newServer(t)
			httpServer := httptest.NewServer(gatewayServer.Handler())
			defer httpServer.Close()
			response := postJSON(t, httpServer.URL+"/v1/sessions", test.body, "local-token", "tts-options-"+test.name)
			defer response.Body.Close()
			var body struct {
				Error struct {
					Code    string `json:"code"`
					Message string `json:"message"`
				} `json:"error"`
			}
			_ = json.NewDecoder(response.Body).Decode(&body)
			if response.StatusCode != http.StatusBadRequest || body.Error.Code != "invalid_tts_options" || !strings.Contains(body.Error.Message, test.want) {
				t.Fatalf("create = %d %+v, want 400 invalid_tts_options mentioning %q", response.StatusCode, body.Error, test.want)
			}
			if calls := plans.calls(); len(calls) != 0 {
				t.Fatalf("a refused create asked the control plane for %d plans", len(calls))
			}
		})
	}
}

// The routed provider's refusal is the caller's to fix, so it answers 422 with
// the setting named, and the plan request never carried the settings.
func TestCreateRefusesATtsOptionTheRoutedModelCannotHonor(t *testing.T) {
	t.Parallel()
	gatewayServer, plans := newServer(t)
	plans.plan.Route.Provider = "cartesia"
	plans.plan.Route.Model = "sonic-3.5"
	httpServer := httptest.NewServer(gatewayServer.Handler())
	defer httpServer.Close()
	response := postJSON(t, httpServer.URL+"/v1/sessions", ttsRequestBody(map[string]map[string]any{"cartesia": {"normalization": "en-IN"}}), "local-token", "tts-route-refusal")
	defer response.Body.Close()
	var body struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	_ = json.NewDecoder(response.Body).Decode(&body)
	if response.StatusCode != http.StatusUnprocessableEntity || body.Error.Code != "tts_option_unsupported" || !strings.Contains(body.Error.Message, "provider_options.cartesia.normalization") {
		t.Fatalf("create = %d %+v, want 422 tts_option_unsupported naming the setting", response.StatusCode, body.Error)
	}
	for _, call := range plans.calls() {
		if call.Request.TTS != nil {
			t.Fatal("tts options must not ride the control-plane request")
		}
	}
}
