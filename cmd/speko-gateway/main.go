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
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/SpekoAI/gateway/controlplane"
	"github.com/SpekoAI/gateway/gateway"
	"github.com/SpekoAI/gateway/protocol"
	"github.com/SpekoAI/gateway/providers/alibaba"
	"github.com/SpekoAI/gateway/providers/assemblyai"
	"github.com/SpekoAI/gateway/providers/cartesia"
	"github.com/SpekoAI/gateway/providers/deepgram"
	"github.com/SpekoAI/gateway/providers/elevenlabs"
	"github.com/SpekoAI/gateway/providers/gladia"
	"github.com/SpekoAI/gateway/providers/google"
	"github.com/SpekoAI/gateway/providers/gradium"
	"github.com/SpekoAI/gateway/providers/hume"
	"github.com/SpekoAI/gateway/providers/inworld"
	"github.com/SpekoAI/gateway/providers/minimax"
	"github.com/SpekoAI/gateway/providers/openai"
	"github.com/SpekoAI/gateway/providers/playht"
	"github.com/SpekoAI/gateway/providers/rime"
	"github.com/SpekoAI/gateway/providers/smallest"
	"github.com/SpekoAI/gateway/providers/soniox"
	"github.com/SpekoAI/gateway/providers/xai"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
)

const (
	maxSecretBytes         = 64 << 10
	defaultControlPlaneURL = "https://gateway.speko.dev"
	defaultTelemetryURL    = "https://gateway.speko.dev/v1/anonymous-runtime-events"
	// defaultAnonymousTurnEventsURL shares the anonymous telemetry base above:
	// turn markers without an API key go to the same unauthenticated surface
	// as anonymous runtime events, just a different path.
	defaultAnonymousTurnEventsURL = "https://gateway.speko.dev/v1/anonymous-turn-events"
	defaultPlanAudience           = "speko-runtime"
	defaultLocalSessionTime       = 24 * time.Hour
	defaultHeartbeatTime          = 20 * time.Second
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
	var workload *protocol.Workload
	if workloadID := env("SPEKO_WORKLOAD_ID"); workloadID != "" {
		workloadType := env("SPEKO_WORKLOAD_TYPE")
		if workloadType == "" {
			workloadType = "agent"
		}
		workload = &protocol.Workload{Type: workloadType, ID: workloadID}
	}
	maxSessions, err := positiveIntEnv("SPEKO_MAX_SESSIONS", 100)
	if err != nil {
		return err
	}
	heartbeatInterval, err := durationEnv("SPEKO_INSTANCE_HEARTBEAT_INTERVAL", defaultHeartbeatTime)
	if err != nil {
		return err
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
	elevenLabsSTTAdapter, err := elevenlabs.NewSTT(elevenlabs.STTConfig{})
	if err != nil {
		return err
	}
	cartesiaAdapter, err := cartesia.New(cartesia.Config{})
	if err != nil {
		return err
	}
	cartesiaSTTAdapter, err := cartesia.NewSTT(cartesia.STTConfig{})
	if err != nil {
		return err
	}
	assemblyAIAdapter, err := assemblyai.New(assemblyai.Config{})
	if err != nil {
		return err
	}
	gladiaAdapter, err := gladia.New(gladia.Config{})
	if err != nil {
		return err
	}
	googleAdapter, err := google.New(google.Config{})
	if err != nil {
		return err
	}
	inworldAdapter, err := inworld.New(inworld.Config{})
	if err != nil {
		return err
	}
	minimaxAdapter, err := minimax.New(minimax.Config{})
	if err != nil {
		return err
	}
	playhtAdapter, err := playht.New(playht.Config{})
	if err != nil {
		return err
	}
	xaiAdapter, err := xai.New(xai.Config{})
	if err != nil {
		return err
	}
	googleSTTAdapter, err := google.NewSTT(google.STTConfig{})
	if err != nil {
		return err
	}
	gradiumSTTAdapter, err := gradium.NewSTT(gradium.STTConfig{})
	if err != nil {
		return err
	}
	gradiumTTSAdapter, err := gradium.NewTTS(gradium.TTSConfig{})
	if err != nil {
		return err
	}
	rimeAdapter, err := rime.New(rime.Config{})
	if err != nil {
		return err
	}
	humeAdapter, err := hume.New(hume.Config{})
	if err != nil {
		return err
	}
	inworldSTTAdapter, err := inworld.NewSTT(inworld.STTConfig{})
	if err != nil {
		return err
	}
	xaiSTTAdapter, err := xai.NewSTT(xai.STTConfig{})
	if err != nil {
		return err
	}
	alibabaSTTAdapter, err := alibaba.NewSTT(alibaba.STTConfig{})
	if err != nil {
		return err
	}
	alibabaTTSAdapter, err := alibaba.NewTTS(alibaba.TTSConfig{})
	if err != nil {
		return err
	}
	openaiSTTAdapter, err := openai.NewSTT(openai.STTConfig{})
	if err != nil {
		return err
	}
	openaiTTSAdapter, err := openai.NewTTS(openai.TTSConfig{})
	if err != nil {
		return err
	}
	sonioxSTTAdapter, err := soniox.NewSTT(soniox.STTConfig{})
	if err != nil {
		return err
	}
	sonioxTTSAdapter, err := soniox.NewTTS(soniox.TTSConfig{})
	if err != nil {
		return err
	}
	smallestSTTAdapter, err := smallest.NewSTT(smallest.STTConfig{})
	if err != nil {
		return err
	}
	smallestTTSAdapter, err := smallest.New(smallest.Config{})
	if err != nil {
		return err
	}
	adapters := []runtimepkg.Adapter{
		deepgramAdapter, deepgramTTSAdapter, elevenLabsAdapter, elevenLabsSTTAdapter,
		cartesiaAdapter, cartesiaSTTAdapter, assemblyAIAdapter, gladiaAdapter,
		googleAdapter, inworldAdapter, minimaxAdapter, playhtAdapter, xaiAdapter,
		sonioxSTTAdapter, sonioxTTSAdapter, smallestSTTAdapter, smallestTTSAdapter,
		openaiSTTAdapter, openaiTTSAdapter, alibabaSTTAdapter, alibabaTTSAdapter,
		gradiumSTTAdapter, gradiumTTSAdapter, rimeAdapter, humeAdapter,
		inworldSTTAdapter, xaiSTTAdapter, googleSTTAdapter,
	}
	adapterIDs := make([]string, 0, len(adapters))
	for _, adapter := range adapters {
		adapterIDs = append(adapterIDs, adapter.ID())
	}
	runtimeDescriptor := protocol.RuntimeDescriptor{
		Name:           "go-gateway",
		Version:        version,
		InstanceID:     instanceID,
		Placement:      protocol.PlacementSidecar,
		ProviderRoutes: []protocol.ProviderRoute{protocol.RouteProviderDirect},
		// Derived from the slice above rather than restated. The two lists drifting
		// apart would advertise an adapter the engine does not have, or hide one it
		// does — and nothing would fail until a session tried to open.
		Adapters: adapterIDs,
	}

	localCredentials, err := loadLocalCredentials()
	if err != nil {
		return err
	}
	var localPlanner *gateway.LocalPlanner
	if len(localCredentials) > 0 {
		maxSessionDuration, err := durationEnv("SPEKO_LOCAL_MAX_SESSION_DURATION", defaultLocalSessionTime)
		if err != nil {
			return err
		}
		routeOverrides, err := loadLocalRouteOverrides()
		if err != nil {
			return err
		}
		localPlanner, err = gateway.NewLocalPlanner(gateway.LocalPlannerConfig{
			Providers: configuredProviders(localCredentials), MaxSessionDuration: maxSessionDuration,
			RouteOverrides: routeOverrides,
		})
		if err != nil {
			return err
		}
	}

	var plans gateway.PlanClient
	var verifier runtimepkg.PlanVerifier
	var hostedClient *controlplane.Client
	turnEventDestinations := gateway.TurnEventDestinations{AnonymousEndpoint: defaultAnonymousTurnEventsURL}
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
		if localPlanner == nil {
			return errors.New("local routing requires at least one SPEKO_<PROVIDER>_BYOK_API_KEY (or _FILE)")
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
		hostedClient, err = controlplane.New(controlplane.Config{BaseURL: controlURL, APIKey: apiKey, UserAgent: "speko-gateway/" + version})
		if err != nil {
			return err
		}
		// Turn markers on a managed route go to the configured control plane
		// and nowhere else — the endpoint is derived from SPEKO_CONTROL_PLANE_URL
		// alone, under the same https origin rule the fallback path applies.
		authenticatedTurnEventsEndpoint, err := turnEventEndpoint(controlURL)
		if err != nil {
			return err
		}
		turnEventDestinations.AuthenticatedEndpoint = authenticatedTurnEventsEndpoint
		turnEventDestinations.AuthenticatedToken = apiKey
		hostedVerifier, err := runtimepkg.NewPlanVerifier(runtimepkg.PlanVerifierConfig{JWKSURL: jwksURL, Issuer: issuer, Audience: audience})
		if err != nil {
			return err
		}
		plans = hostedClient
		verifier = hostedVerifier
		if localPlanner != nil {
			plans, err = gateway.NewCredentialSourcePlanner(localPlanner, hostedClient)
			if err != nil {
				return err
			}
			verifier = runtimepkg.PlanVerifierFunc(func(ctx context.Context, plan protocol.SessionPlan) error {
				if plan.Execution.CredentialSource == protocol.CredentialsBYOK {
					return localPlanner.Verify(ctx, plan)
				}
				return hostedVerifier.Verify(ctx, plan)
			})
		}
		log.Printf("routing=speko local_byok_providers=%d optional_telemetry=%t", len(localCredentials), !telemetryDisabled)
	}

	engine, err := runtimepkg.New(runtimepkg.Config{
		Adapters: adapters, Verifier: verifier, LocalCredentials: localCredentials, Telemetry: telemetryExporter,
	})
	if err != nil {
		return err
	}
	// Prefetching only applies to Speko-managed routing. BYOK plans are signed
	// in this process by LocalPlanner and already cost nothing to produce.
	var warmPlans *gateway.PlanPool
	if hostedClient != nil {
		warmPlanTarget, err := nonNegativeIntEnv("SPEKO_WARM_PLAN_TARGET", 4)
		if err != nil {
			return err
		}
		if warmPlanTarget > 0 {
			warmPlans, err = gateway.NewPlanPool(gateway.PlanPoolConfig{
				Plans: plans, Target: warmPlanTarget,
				Runtime: runtimeDescriptor, Workload: workload,
			})
			if err != nil {
				return err
			}
		}
	}
	server, err := gateway.New(gateway.Config{
		Engine: engine, Plans: plans, WarmPlans: warmPlans, LocalAuthToken: localToken,
		Runtime: runtimeDescriptor, Workload: workload, MaxSessions: maxSessions,
		Telemetry: telemetryExporter, TurnEvents: turnEventDestinations,
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

	processCtx, cancelProcess := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancelProcess()
	startedAt := time.Now().UTC()
	if warmPlans != nil {
		// Warm the routes named in configuration before any traffic arrives, so
		// the first session of the process — the one a real caller is waiting
		// on — is as fast as the thousandth.
		for _, route := range warmRoutesFromEnv() {
			if err := warmPlans.Warm(processCtx, route); err != nil {
				log.Printf("warm route %s/%s: %v", route.Kind, route.Request.Provider, err)
			}
		}
		go warmPlans.Run(processCtx)
	}
	if hostedClient != nil {
		go reportInstanceLoop(processCtx, hostedClient, server, telemetryExporter, runtimeDescriptor, workload, startedAt, heartbeatInterval)
	}
	select {
	case <-processCtx.Done():
		log.Printf("received shutdown signal; draining")
		server.BeginDrain()
		if hostedClient != nil {
			heartbeatCtx, cancelHeartbeat := context.WithTimeout(context.Background(), 5*time.Second)
			if err := reportInstance(heartbeatCtx, hostedClient, server, telemetryExporter, runtimeDescriptor, workload, startedAt); err != nil {
				log.Printf("final draining heartbeat failed: %v", err)
			}
			cancelHeartbeat()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		drainErr := server.Drain(ctx)
		cancel()
		if hostedClient != nil {
			deregisterCtx, cancelDeregister := context.WithTimeout(context.Background(), 5*time.Second)
			if err := hostedClient.DeregisterInstance(deregisterCtx, instanceID); err != nil {
				log.Printf("runtime instance deregistration failed: %v", err)
			}
			cancelDeregister()
		}
		return drainErr
	case err := <-serveErrors:
		return err
	}
}

func reportInstanceLoop(ctx context.Context, client *controlplane.Client, server *gateway.Server, telemetry *runtimepkg.TelemetryExporter, descriptor protocol.RuntimeDescriptor, workload *protocol.Workload, startedAt time.Time, interval time.Duration) {
	report := func() {
		reportCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := reportInstance(reportCtx, client, server, telemetry, descriptor, workload, startedAt); err != nil && ctx.Err() == nil {
			log.Printf("runtime instance heartbeat failed: %v", err)
		}
	}
	report()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			report()
		}
	}
}

func reportInstance(ctx context.Context, client *controlplane.Client, server *gateway.Server, telemetry *runtimepkg.TelemetryExporter, descriptor protocol.RuntimeDescriptor, workload *protocol.Workload, startedAt time.Time) error {
	stats := server.Stats()
	telemetryStats := telemetry.Stats()
	heartbeat := instanceHeartbeat(stats, telemetryStats.Dropped, descriptor, workload, startedAt)
	return client.ReportInstance(ctx, descriptor.InstanceID, heartbeat)
}

func instanceHeartbeat(stats gateway.Stats, telemetryDropped uint64, descriptor protocol.RuntimeDescriptor, workload *protocol.Workload, startedAt time.Time) controlplane.InstanceHeartbeat {
	heartbeat := controlplane.InstanceHeartbeat{
		RuntimeName: descriptor.Name, RuntimeVersion: descriptor.Version, StartedAt: startedAt,
		ActiveSessions: stats.ActiveSessions, PendingSessions: stats.PendingSessions,
		SessionCapacity: stats.SessionCapacity, SessionsTotal: stats.SessionsTotal,
		TelemetryDropped: telemetryDropped, Draining: stats.Draining,
	}
	if workload != nil {
		heartbeat.WorkloadType = workload.Type
		heartbeat.WorkloadID = workload.ID
	}
	return heartbeat
}

// warmRoutesFromEnv parses SPEKO_WARM_ROUTES, a comma-separated list of
// `kind:provider[:model[:language]]` entries — for example
// `stt:deepgram:nova-3:en,tts:elevenlabs::en`.
//
// The pool learns route shapes from traffic on its own, so this exists for one
// case only, and it is the case that matters most: the first session after a
// deploy or a scale-up. Without it that session pays the full control-plane
// round trip, and it is the session a real person is on the other end of.
//
// A malformed entry is skipped with a log line rather than failing boot. Warm
// routes are an optimization, and refusing to start a voice worker over a typo
// in an optional hint would be a worse outcome than a cold first call.
func warmRoutesFromEnv() []protocol.SessionPlanRequest {
	raw := env("SPEKO_WARM_ROUTES")
	if raw == "" {
		return nil
	}
	maxCharacters, err := positiveIntEnv("SPEKO_WARM_TTS_MAX_CHARACTERS", 100_000)
	if err != nil {
		log.Printf("SPEKO_WARM_TTS_MAX_CHARACTERS ignored: %v", err)
		maxCharacters = 100_000
	}
	var routes []protocol.SessionPlanRequest
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		fields := strings.Split(entry, ":")
		if len(fields) < 2 || len(fields) > 4 {
			log.Printf("SPEKO_WARM_ROUTES entry %q ignored: want kind:provider[:model[:language]]", entry)
			continue
		}
		kind := protocol.SessionKind(strings.ToLower(strings.TrimSpace(fields[0])))
		if kind != protocol.SessionKindSTT && kind != protocol.SessionKindTTS {
			log.Printf("SPEKO_WARM_ROUTES entry %q ignored: only stt and tts can be prefetched", entry)
			continue
		}
		options := protocol.RequestOptions{Provider: strings.TrimSpace(fields[1])}
		if len(fields) > 2 {
			options.Model = strings.TrimSpace(fields[2])
		}
		if len(fields) > 3 {
			options.Language = strings.TrimSpace(fields[3])
		}
		if kind == protocol.SessionKindTTS {
			options.MaxInputCharacters = int64(maxCharacters)
		}
		routes = append(routes, protocol.SessionPlanRequest{
			Kind: kind, Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision,
			Execution: protocol.ExecutionRequest{
				ProviderRoute: protocol.RouteAuto, CredentialSource: protocol.CredentialsManaged,
				RelayPolicy: protocol.RelayForbidden,
			},
			Request: options,
			// The plan does not bind media, and the runtime validates the real
			// format at open, so a canonical shape is enough to make the
			// prefetch request valid. 16 kHz mono is what the LiveKit
			// integration sends.
			Media: &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: 16_000, Channels: 1},
		})
	}
	return routes
}

// turnEventEndpoint derives the authenticated turn-event ingest URL from the
// configured control-plane URL and nothing else. The same https origin shape
// the control-plane client enforces is checked again here so a future
// AllowInsecureHTTP escape hatch cannot silently widen where the API key is
// sent.
func turnEventEndpoint(controlPlaneURL string) (string, error) {
	parsed, err := url.Parse(strings.TrimRight(controlPlaneURL, "/"))
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("SPEKO_CONTROL_PLANE_URL must be an absolute https URL to derive the turn-event endpoint")
	}
	return parsed.String() + "/v1/turn-events", nil
}

func nonNegativeIntEnv(name string, fallback int) (int, error) {
	value := env(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", name)
	}
	return parsed, nil
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

func positiveIntEnv(name string, fallback int) (int, error) {
	value := env(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
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
