package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/gateway"
	"github.com/SpekoAI/gateway/protocol"
)

func TestEveryCatalogProviderHasAConfigurableBYOKCredential(t *testing.T) {
	catalogProviders := map[string]struct{}{}
	for _, entry := range gateway.Catalog() {
		catalogProviders[entry.Provider] = struct{}{}
	}
	configured := map[string]string{}
	for _, spec := range localCredentialSpecs {
		if previous := configured[spec.Provider]; previous != "" {
			t.Fatalf("provider %q has duplicate credential variables %q and %q", spec.Provider, previous, spec.Env)
		}
		configured[spec.Provider] = spec.Env
	}
	for provider := range catalogProviders {
		if configured[provider] == "" {
			t.Fatalf("catalog provider %q has no BYOK credential variable", provider)
		}
	}
	if len(configured) != len(catalogProviders) {
		t.Fatalf("credential providers = %d, catalog providers = %d", len(configured), len(catalogProviders))
	}
}

func TestBYOKCredentialVariablesAreExposedByExamplesAndCompose(t *testing.T) {
	environment, err := os.ReadFile(filepath.Join("..", "..", ".env.example"))
	if err != nil {
		t.Fatalf("read .env.example: %v", err)
	}
	compose, err := os.ReadFile(filepath.Join("..", "..", "deploy", "docker-compose.yml"))
	if err != nil {
		t.Fatalf("read docker-compose.yml: %v", err)
	}
	for _, spec := range localCredentialSpecs {
		if !strings.Contains(string(environment), spec.Env+"=") {
			t.Errorf(".env.example does not expose %s", spec.Env)
		}
		if !strings.Contains(string(compose), spec.Env+":") {
			t.Errorf("docker-compose.yml does not forward %s", spec.Env)
		}
	}
	for _, name := range []string{"SPEKO_PLAYHT_BYOK_USER_ID", "SPEKO_GOOGLE_STT_ENDPOINT"} {
		if !strings.Contains(string(environment), name+"=") || !strings.Contains(string(compose), name+":") {
			t.Errorf("configuration surfaces do not expose %s", name)
		}
	}
	for _, entry := range gateway.Catalog() {
		if entry.Kind != protocol.SessionKindTTS {
			continue
		}
		name := "SPEKO_" + strings.ToUpper(entry.Provider) + "_BYOK_TTS_VOICE"
		if !strings.Contains(string(environment), name+"=") || !strings.Contains(string(compose), name+":") {
			t.Errorf("configuration surfaces do not expose %s", name)
		}
	}
}

func TestPlayHTAcceptsVendorNativeCredentialParts(t *testing.T) {
	for _, spec := range localCredentialSpecs {
		t.Setenv(spec.Env, "")
		t.Setenv(spec.Env+"_FILE", "")
	}
	t.Setenv("SPEKO_PLAYHT_BYOK_USER_ID", "user-123")
	t.Setenv("SPEKO_PLAYHT_BYOK_API_KEY", "key-456")
	credentials, err := loadLocalCredentials()
	if err != nil {
		t.Fatalf("load local credentials: %v", err)
	}
	if got := credentials["playht"].Value; got != "user-123:key-456" {
		t.Fatalf("PlayHT credential = %q", got)
	}
}

func TestLocalRouteOverridesReadGoogleEndpointAndProviderVoice(t *testing.T) {
	t.Setenv("SPEKO_GOOGLE_STT_ENDPOINT", "https://speech.googleapis.com/v2/projects/test-project/locations/eu/recognizers/_:recognize")
	t.Setenv("SPEKO_CARTESIA_BYOK_TTS_VOICE", "voice-123")
	overrides, err := loadLocalRouteOverrides()
	if err != nil {
		t.Fatalf("load local route overrides: %v", err)
	}
	if got := overrides["google.stt.v1"].Endpoint; !strings.Contains(got, "projects/test-project/") {
		t.Fatalf("Google STT endpoint = %q", got)
	}
	if got := overrides["cartesia.tts.v1"].DefaultVoice; got != "voice-123" {
		t.Fatalf("Cartesia default voice = %q", got)
	}
}

func TestInstanceHeartbeatCarriesDrainingState(t *testing.T) {
	startedAt := time.Date(2026, time.August, 6, 12, 0, 0, 0, time.UTC)
	heartbeat := instanceHeartbeat(
		gateway.Stats{ActiveSessions: 2, PendingSessions: 1, SessionCapacity: 10, SessionsTotal: 25, Draining: true},
		3,
		protocol.RuntimeDescriptor{Name: "go-gateway", Version: "test", InstanceID: "worker-1"},
		&protocol.Workload{Type: "agent", ID: "agent-1"},
		startedAt,
	)
	if !heartbeat.Draining || heartbeat.ActiveSessions != 2 || heartbeat.TelemetryDropped != 3 || heartbeat.WorkloadID != "agent-1" {
		t.Fatalf("heartbeat = %+v", heartbeat)
	}
}

func TestTurnEventEndpointDerivesOnlyFromHTTPSControlPlaneOrigin(t *testing.T) {
	endpoint, err := turnEventEndpoint("https://control.speko.test/")
	if err != nil || endpoint != "https://control.speko.test/v1/turn-events" {
		t.Fatalf("endpoint = %q err = %v", endpoint, err)
	}
	for _, invalid := range []string{
		"http://control.speko.test",
		"https://user:secret@control.speko.test",
		"https://control.speko.test?token=1",
		"https://control.speko.test#fragment",
		"control.speko.test",
		"",
	} {
		if _, err := turnEventEndpoint(invalid); err == nil {
			t.Fatalf("turnEventEndpoint(%q) accepted a non-https origin", invalid)
		}
	}
}

func TestSecretReadsEnvironmentOrFileWithoutAmbiguity(t *testing.T) {
	t.Run("environment", func(t *testing.T) {
		t.Setenv("TEST_GATEWAY_SECRET", "  value-from-env  ")
		got, err := secret("TEST_GATEWAY_SECRET")
		if err != nil || got != "value-from-env" {
			t.Fatalf("secret = %q, err=%v", got, err)
		}
	})

	t.Run("file", func(t *testing.T) {
		secretPath := filepath.Join(t.TempDir(), "provider-key")
		if err := os.WriteFile(secretPath, []byte("value-from-file\n"), 0o600); err != nil {
			t.Fatalf("write secret fixture: %v", err)
		}
		t.Setenv("TEST_GATEWAY_SECRET_FILE", secretPath)
		got, err := secret("TEST_GATEWAY_SECRET")
		if err != nil || got != "value-from-file" {
			t.Fatalf("secret = %q, err=%v", got, err)
		}
	})

	t.Run("mutually exclusive", func(t *testing.T) {
		t.Setenv("TEST_GATEWAY_SECRET", "value")
		t.Setenv("TEST_GATEWAY_SECRET_FILE", "/not/read")
		if _, err := secret("TEST_GATEWAY_SECRET"); err == nil {
			t.Fatal("environment and file values were both accepted")
		}
	})

	t.Run("bounded", func(t *testing.T) {
		secretPath := filepath.Join(t.TempDir(), "oversized")
		if err := os.WriteFile(secretPath, []byte(strings.Repeat("x", maxSecretBytes+1)), 0o600); err != nil {
			t.Fatalf("write secret fixture: %v", err)
		}
		t.Setenv("TEST_GATEWAY_SECRET_FILE", secretPath)
		if _, err := secret("TEST_GATEWAY_SECRET"); err == nil {
			t.Fatal("oversized secret was accepted")
		}
	})
}

func TestBoolEnv(t *testing.T) {
	t.Run("fallback", func(t *testing.T) {
		if got, err := boolEnv("TEST_GATEWAY_BOOL", true); err != nil || !got {
			t.Fatalf("bool env = %t, err=%v", got, err)
		}
	})
	t.Run("false", func(t *testing.T) {
		t.Setenv("TEST_GATEWAY_BOOL", "false")
		if got, err := boolEnv("TEST_GATEWAY_BOOL", true); err != nil || got {
			t.Fatalf("bool env = %t, err=%v", got, err)
		}
	})
	t.Run("invalid", func(t *testing.T) {
		t.Setenv("TEST_GATEWAY_BOOL", "sometimes")
		if _, err := boolEnv("TEST_GATEWAY_BOOL", true); err == nil {
			t.Fatal("invalid boolean was accepted")
		}
	})
}

func TestDurationEnv(t *testing.T) {
	t.Run("fallback", func(t *testing.T) {
		if got, err := durationEnv("TEST_GATEWAY_DURATION", time.Minute); err != nil || got != time.Minute {
			t.Fatalf("duration env = %s, err=%v", got, err)
		}
	})
	t.Run("value", func(t *testing.T) {
		t.Setenv("TEST_GATEWAY_DURATION", "90s")
		if got, err := durationEnv("TEST_GATEWAY_DURATION", time.Minute); err != nil || got != 90*time.Second {
			t.Fatalf("duration env = %s, err=%v", got, err)
		}
	})
	t.Run("too small", func(t *testing.T) {
		t.Setenv("TEST_GATEWAY_DURATION", "500ms")
		if _, err := durationEnv("TEST_GATEWAY_DURATION", time.Minute); err == nil {
			t.Fatal("sub-second duration was accepted")
		}
	})
}
