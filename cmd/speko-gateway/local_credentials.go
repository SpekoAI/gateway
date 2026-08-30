package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/SpekoAI/gateway/gateway"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

type localCredentialSpec struct {
	Provider string
	Env      string
}

// localCredentialSpecs is the one executable-level inventory of BYOK
// credential variables. Tests require the example environment and Compose
// deployment to expose every entry so adding an adapter cannot silently ship
// an unusable configuration surface again.
var localCredentialSpecs = []localCredentialSpec{
	{Provider: "alibaba", Env: "SPEKO_ALIBABA_BYOK_API_KEY"},
	{Provider: "assemblyai", Env: "SPEKO_ASSEMBLYAI_BYOK_API_KEY"},
	{Provider: "cartesia", Env: "SPEKO_CARTESIA_BYOK_API_KEY"},
	{Provider: "deepgram", Env: "SPEKO_DEEPGRAM_BYOK_API_KEY"},
	{Provider: "elevenlabs", Env: "SPEKO_ELEVENLABS_BYOK_API_KEY"},
	{Provider: "fish", Env: "SPEKO_FISH_BYOK_API_KEY"},
	{Provider: "gemini", Env: "SPEKO_GEMINI_BYOK_API_KEY"},
	{Provider: "gladia", Env: "SPEKO_GLADIA_BYOK_API_KEY"},
	{Provider: "google", Env: "SPEKO_GOOGLE_BYOK_ACCESS_TOKEN"},
	{Provider: "gradium", Env: "SPEKO_GRADIUM_BYOK_API_KEY"},
	{Provider: "hamsa", Env: "SPEKO_HAMSA_BYOK_API_KEY"},
	{Provider: "hume", Env: "SPEKO_HUME_BYOK_API_KEY"},
	{Provider: "inworld", Env: "SPEKO_INWORLD_BYOK_API_KEY"},
	{Provider: "maya", Env: "SPEKO_MAYA_BYOK_API_KEY"},
	{Provider: "minimax", Env: "SPEKO_MINIMAX_BYOK_API_KEY"},
	{Provider: "modulate", Env: "SPEKO_MODULATE_BYOK_API_KEY"},
	{Provider: "openai", Env: "SPEKO_OPENAI_BYOK_API_KEY"},
	{Provider: "palabra", Env: "SPEKO_PALABRA_BYOK_API_KEY"},
	{Provider: "rime", Env: "SPEKO_RIME_BYOK_API_KEY"},
	{Provider: "smallest", Env: "SPEKO_SMALLEST_BYOK_API_KEY"},
	{Provider: "soniox", Env: "SPEKO_SONIOX_BYOK_API_KEY"},
	{Provider: "speechify", Env: "SPEKO_SPEECHIFY_BYOK_API_KEY"},
	{Provider: "speechmatics", Env: "SPEKO_SPEECHMATICS_BYOK_API_KEY"},
	{Provider: "xai", Env: "SPEKO_XAI_BYOK_API_KEY"},
}

func loadLocalCredentials() (map[string]runtimepkg.LocalCredential, error) {
	credentials := make(map[string]runtimepkg.LocalCredential)
	for _, spec := range localCredentialSpecs {
		credential, configured, err := credentialFromEnv(spec.Env)
		if err != nil {
			return nil, err
		}
		if configured {
			credentials[spec.Provider] = credential
		}
	}
	return credentials, nil
}

func credentialFromEnv(name string) (runtimepkg.LocalCredential, bool, error) {
	direct := env(name)
	filePath := env(name + "_FILE")
	if direct != "" && filePath != "" {
		return runtimepkg.LocalCredential{}, false, fmt.Errorf("%s and %s_FILE are mutually exclusive", name, name)
	}
	credential := runtimepkg.LocalCredential{Kind: protocol.CredentialBearer}
	switch {
	case direct != "":
		credential.Value = direct
		return credential, true, nil
	case filePath != "":
		// Validate at startup; Engine rereads the same file for each session so
		// short-lived OAuth tokens can be refreshed in place.
		if _, err := secret(name); err != nil {
			return runtimepkg.LocalCredential{}, false, err
		}
		credential.ValueFile = filePath
		return credential, true, nil
	default:
		return runtimepkg.LocalCredential{}, false, nil
	}
}

func loadLocalRouteOverrides() (map[string]gateway.LocalRouteOverride, error) {
	overrides := make(map[string]gateway.LocalRouteOverride)
	if endpoint := env("SPEKO_GOOGLE_STT_ENDPOINT"); endpoint != "" {
		overrides["google.stt.v1"] = gateway.LocalRouteOverride{Endpoint: endpoint}
	}
	for _, entry := range gateway.Catalog() {
		if entry.Kind != protocol.SessionKindTTS {
			continue
		}
		name := "SPEKO_" + strings.ToUpper(entry.Provider) + "_BYOK_TTS_VOICE"
		voice := env(name)
		if voice == "" {
			continue
		}
		override := overrides[entry.Adapter]
		override.DefaultVoice = voice
		overrides[entry.Adapter] = override
	}
	return overrides, nil
}

func configuredProviders(credentials map[string]runtimepkg.LocalCredential) []string {
	providers := make([]string, 0, len(credentials))
	for provider := range credentials {
		providers = append(providers, provider)
	}
	sort.Strings(providers)
	return providers
}
