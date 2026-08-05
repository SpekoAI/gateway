// speko-gateway runs the open customer-side Speko data plane behind an
// owner-only Unix socket. It never exposes a public listener or logs plans,
// provider keys, audio, transcripts, prompts, or synthesized text.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/SpekoAI/gateway/controlplane"
	"github.com/SpekoAI/gateway/gateway"
	"github.com/SpekoAI/gateway/protocol"
	"github.com/SpekoAI/gateway/providers/cartesia"
	"github.com/SpekoAI/gateway/providers/deepgram"
	"github.com/SpekoAI/gateway/providers/elevenlabs"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	maxSecretBytes          = 64 << 10
	defaultControlPlaneURL  = "https://gateway.speko.ai"
	defaultTelemetryURL     = "https://gateway.speko.ai/v1/runtime-events"
	defaultPlanAudience     = "speko-runtime"
	defaultLocalSessionTime = 24 * time.Hour
)

var (
	version   = "dev"
	commit    = "unknown"
	buildDate = "unknown"
)

func main() {
	if len(os.Args) == 2 && (os.Args[1] == "version" || os.Args[1] == "--version") {
		fmt.Printf("speko-gateway %s (commit=%s, built=%s)\n", version, commit, buildDate)
		return
	}
	if err := run(); err != nil {
		log.Printf("speko-gateway: %v", err)
		os.Exit(1)
	}
}

func run() error {
	apiKey, err := secret("SPEKO_API_KEY")
	if err != nil {
		return err
	}
	localToken, err := secret("SPEKO_LOCAL_AUTH_TOKEN")
	if err != nil {
		return err
	}
	if localToken == "" {
		return errors.New("SPEKO_LOCAL_AUTH_TOKEN (or SPEKO_LOCAL_AUTH_TOKEN_FILE) is required")
	}
	telemetryDisabled, err := boolEnv("SPEKO_TELEMETRY_DISABLED", false)
	if err != nil {
		return err
	}

	socketPath := env("SPEKO_SOCKET_PATH")
	if socketPath == "" {
		socketPath = "/run/speko/runtime.sock"
	}
	instanceID := env("SPEKO_RUNTIME_INSTANCE_ID")
	if instanceID == "" {
		instanceID, err = os.Hostname()
		if err != nil {
			return errors.New("determine runtime instance ID")
		}
	}

	deepgramAdapter, err := deepgram.New(deepgram.Config{})
	if err != nil {
		return err
	}
	deepgramTTSAdapter, err := deepgram.NewTTS(deepgram.TTSConfig{})
	if err != nil {
		return err
	}
	elevenLabsAdapter, err := elevenlabs.New(elevenlabs.Config{})
	if err != nil {
		return err
	}
	cartesiaAdapter, err := cartesia.New(cartesia.Config{})
	if err != nil {
		return err
	}
	adapters := []runtimepkg.Adapter{deepgramAdapter, deepgramTTSAdapter, elevenLabsAdapter, cartesiaAdapter}
	runtimeDescriptor := protocol.RuntimeDescriptor{
		Name:           "go-gateway",
		Version:        version,
		InstanceID:     instanceID,
		Placement:      protocol.PlacementSidecar,
		ProviderRoutes: []protocol.ProviderRoute{protocol.RouteProviderDirect},
		Adapters:       []string{deepgramAdapter.ID(), deepgramTTSAdapter.ID(), elevenLabsAdapter.ID(), cartesiaAdapter.ID()},
	}

	localCredentials := make(map[string]runtimepkg.LocalCredential)
	for provider, name := range map[string]string{
		"deepgram":   "SPEKO_DEEPGRAM_BYOK_API_KEY",
		"cartesia":   "SPEKO_CARTESIA_BYOK_API_KEY",
		"elevenlabs": "SPEKO_ELEVENLABS_BYOK_API_KEY",
	} {
		key, err := secret(name)
		if err != nil {
			return err
		}
		if key != "" {
			localCredentials[provider] = runtimepkg.LocalCredential{Kind: protocol.CredentialBearer, Value: key}
		}
	}

	var plans gateway.PlanClient
	var verifier runtimepkg.PlanVerifier
	telemetryExporter, err := runtimepkg.NewTelemetryExporter(runtimepkg.TelemetryExporterConfig{
		UserAgent:                "speko-gateway/" + version,
		AnonymousEndpoint:        defaultTelemetryURL,
		DisableOptionalTelemetry: telemetryDisabled,
	})
	if err != nil {
		return err
	}
	defer func() {
		flushCtx, cancelFlush := context.WithTimeout(context.Background(), 10*time.Second)
		_ = telemetryExporter.Close(flushCtx)
		cancelFlush()
		stats := telemetryExporter.Stats()
		log.Printf("telemetry exporter: exported=%d suppressed=%d dropped=%d retried=%d failed_batches=%d", stats.Exported, stats.Suppressed, stats.Dropped, stats.Retried, stats.FailedBatches)
	}()
	if apiKey == "" {
		if len(localCredentials) == 0 {
			return errors.New("local routing requires at least one SPEKO_<PROVIDER>_BYOK_API_KEY (or _FILE)")
		}
		maxSessionDuration, err := durationEnv("SPEKO_LOCAL_MAX_SESSION_DURATION", defaultLocalSessionTime)
		if err != nil {
			return err
		}
		providers := make([]string, 0, len(localCredentials))
		for provider := range localCredentials {
			providers = append(providers, provider)
		}
		localPlanner, err := gateway.NewLocalPlanner(gateway.LocalPlannerConfig{Providers: providers, MaxSessionDuration: maxSessionDuration})
		if err != nil {
			return err
		}
		plans = localPlanner
		verifier = localPlanner
		log.Printf("routing=local-byok anonymous_telemetry=%t", !telemetryDisabled)
	} else {
		controlURL := env("SPEKO_CONTROL_PLANE_URL")
		if controlURL == "" {
			controlURL = defaultControlPlaneURL
		}
		issuer := env("SPEKO_PLAN_ISSUER")
		if issuer == "" {
			issuer = strings.TrimRight(controlURL, "/")
		}
		audience := env("SPEKO_PLAN_AUDIENCE")
		if audience == "" {
			audience = defaultPlanAudience
		}
		jwksURL := env("SPEKO_JWKS_URL")
		if jwksURL == "" {
			jwksURL = strings.TrimRight(controlURL, "/") + "/.well-known/jwks.json"
		}
		plans, err = controlplane.New(controlplane.Config{BaseURL: controlURL, APIKey: apiKey, UserAgent: "speko-gateway/" + version})
		if err != nil {
			return err
		}
		verifier, err = runtimepkg.NewPlanVerifier(runtimepkg.PlanVerifierConfig{JWKSURL: jwksURL, Issuer: issuer, Audience: audience})
		if err != nil {
			return err
		}
		log.Printf("routing=speko optional_telemetry=%t", !telemetryDisabled)
	}

	engine, err := runtimepkg.New(runtimepkg.Config{
		Adapters: adapters, Verifier: verifier, LocalCredentials: localCredentials, Telemetry: telemetryExporter,
	})
	if err != nil {
		return err
	}
	server, err := gateway.New(gateway.Config{
		Engine: engine, Plans: plans, LocalAuthToken: localToken, Runtime: runtimeDescriptor,
	})
	if err != nil {
		return err
	}
	listener, err := gateway.ListenUnix(socketPath)
	if err != nil {
		return err
	}
	defer func() {
		_ = listener.Close()
		_ = os.Remove(socketPath)
	}()
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.Serve(listener) }()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	select {
	case signal := <-signals:
		log.Printf("received %s; draining", signal)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := server.Drain(ctx)
		cancel()
		return err
	case err := <-serveErrors:
		return err
	}
}

func env(name string) string { return strings.TrimSpace(os.Getenv(name)) }

func boolEnv(name string, fallback bool) (bool, error) {
	value := env(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false", name)
	}
	return parsed, nil
}

func durationEnv(name string, fallback time.Duration) (time.Duration, error) {
	value := env(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed < time.Second {
		return 0, fmt.Errorf("%s must be a duration of at least one second", name)
	}
	return parsed, nil
}

// secret accepts either NAME or NAME_FILE. File variants work with Docker and
// Kubernetes secrets without placing raw provider keys in container metadata.
func secret(name string) (string, error) {
	direct := env(name)
	filePath := env(name + "_FILE")
	if direct != "" && filePath != "" {
		return "", fmt.Errorf("%s and %s_FILE are mutually exclusive", name, name)
	}
	if filePath == "" {
		return direct, nil
	}
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("read %s_FILE: %w", name, err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxSecretBytes+1))
	if err != nil {
		return "", fmt.Errorf("read %s_FILE: %w", name, err)
	}
	if len(data) > maxSecretBytes {
		return "", fmt.Errorf("%s_FILE exceeds %d bytes", name, maxSecretBytes)
	}
	value := strings.TrimSpace(string(data))
	if value == "" {
		return "", fmt.Errorf("%s_FILE is empty", name)
	}
	return value, nil
}
