package gateway

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/SpekoAI/gateway/controlplane"
	"github.com/SpekoAI/gateway/protocol"
)

const (
	defaultLocalSessionLimit = 24 * time.Hour
	localSignaturePrefix     = "local.v1."
)

var (
	ErrLocalModeRequiresBYOK = errors.New("gateway: local routing requires provider-direct BYOK execution")
	ErrLocalPlanSignature    = errors.New("gateway: invalid local session plan signature")
	ErrLocalPlanRenewal      = errors.New("gateway: local session plans are not renewable")
)

// LocalPlannerConfig describes the credentials and limits available to a
// locally routed gateway. Provider key values are intentionally not accepted;
// only provider names are needed to make a local routing decision.
type LocalPlannerConfig struct {
	Providers          []string
	MaxSessionDuration time.Duration
	Now                func() time.Time
}

// LocalPlanner issues and verifies process-local plans when no Speko API key
// is configured. HMAC signing preserves the same verify-before-key-injection
// invariant used by connected plans without contacting Speko.
type LocalPlanner struct {
	providers          map[string]struct{}
	maxSessionDuration time.Duration
	now                func() time.Time
	key                [sha256.Size]byte
}

func NewLocalPlanner(config LocalPlannerConfig) (*LocalPlanner, error) {
	if config.MaxSessionDuration == 0 {
		config.MaxSessionDuration = defaultLocalSessionLimit
	}
	if config.MaxSessionDuration < time.Second {
		return nil, errors.New("gateway: local maximum session duration must be at least one second")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	providers := make(map[string]struct{}, len(config.Providers))
	for _, provider := range config.Providers {
		provider = strings.ToLower(strings.TrimSpace(provider))
		if provider != "deepgram" && provider != "elevenlabs" && provider != "cartesia" {
			return nil, fmt.Errorf("gateway: unsupported local provider %q", provider)
		}
		providers[provider] = struct{}{}
	}
	if len(providers) == 0 {
		return nil, errors.New("gateway: at least one local provider is required")
	}
	planner := &LocalPlanner{providers: providers, maxSessionDuration: config.MaxSessionDuration, now: config.Now}
	if _, err := rand.Read(planner.key[:]); err != nil {
		return nil, fmt.Errorf("gateway: generate local plan key: %w", err)
	}
	return planner, nil
}

func (p *LocalPlanner) CreateSessionPlan(_ context.Context, request protocol.SessionPlanRequest, _ controlplane.CreateOptions) (protocol.SessionPlan, string, error) {
	if err := request.Validate(); err != nil {
		return protocol.SessionPlan{}, "", fmt.Errorf("gateway: invalid local session request: %w", err)
	}
	if request.Execution.CredentialSource != protocol.CredentialsBYOK || (request.Execution.ProviderRoute != protocol.RouteAuto && request.Execution.ProviderRoute != protocol.RouteProviderDirect) || request.Execution.RelayPolicy != protocol.RelayForbidden {
		return protocol.SessionPlan{}, "", ErrLocalModeRequiresBYOK
	}
	provider, err := p.selectProvider(request.Kind, request.Request.Provider)
	if err != nil {
		return protocol.SessionPlan{}, "", err
	}
	route, err := localRoute(request.Kind, provider, request.Request.Model)
	if err != nil {
		return protocol.SessionPlan{}, "", err
	}
	now := p.now().UTC()
	planID, err := localID("plan")
	if err != nil {
		return protocol.SessionPlan{}, "", err
	}
	sessionID, err := localID("session")
	if err != nil {
		return protocol.SessionPlan{}, "", err
	}
	attemptID, err := localID("attempt")
	if err != nil {
		return protocol.SessionPlan{}, "", err
	}
	usage := protocol.UsageReservation{Unit: protocol.UsageUnitDurationSeconds, AuthorizedUnits: int64(p.maxSessionDuration / time.Second)}
	if request.Kind == protocol.SessionKindTTS {
		usage = protocol.UsageReservation{Unit: protocol.UsageUnitCharacters, AuthorizedUnits: request.Request.MaxInputCharacters}
	}
	plan := protocol.SessionPlan{
		PlanID: planID, SessionID: sessionID, AttemptID: attemptID,
		Execution: protocol.Execution{Placement: request.Runtime.Placement, ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK},
		ExpiresAt: now.Add(time.Minute),
		Route:     route,
		Reservation: protocol.Reservation{
			ID: "local-" + sessionID, LeaseDurationSeconds: int(p.maxSessionDuration / time.Second), LeaseExpiresAt: now.Add(p.maxSessionDuration),
			Concurrency: protocol.ConcurrencyReservation{LeaseID: "local-" + sessionID, Slots: 1}, Usage: usage,
		},
		Requirements: protocol.Requirements{Protocol: protocol.VoiceV0, ProtocolRevision: protocol.CurrentRevision, RuntimeVersion: request.Runtime.Version},
	}
	signature, err := p.sign(plan)
	if err != nil {
		return protocol.SessionPlan{}, "", err
	}
	plan.Signature = signature
	return plan, "", nil
}

func (p *LocalPlanner) RenewSessionLease(context.Context, protocol.SessionPlan) (protocol.SessionLease, string, error) {
	return protocol.SessionLease{}, "", ErrLocalPlanRenewal
}

func (p *LocalPlanner) ExchangeFallbackPlan(context.Context, protocol.SessionPlan, controlplane.FallbackRequest, string) (protocol.SessionPlan, string, error) {
	return protocol.SessionPlan{}, "", errors.New("gateway: local fallback is not configured")
}

func (p *LocalPlanner) Verify(_ context.Context, plan protocol.SessionPlan) error {
	if !strings.HasPrefix(plan.Signature, localSignaturePrefix) || plan.Execution.CredentialSource != protocol.CredentialsBYOK || plan.Execution.ProviderRoute != protocol.RouteProviderDirect || plan.Route.Credential != nil {
		return ErrLocalPlanSignature
	}
	encoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(plan.Signature, localSignaturePrefix))
	if err != nil {
		return ErrLocalPlanSignature
	}
	unsigned := plan.Unsigned()
	payload, err := json.Marshal(unsigned)
	if err != nil {
		return ErrLocalPlanSignature
	}
	expected := hmac.New(sha256.New, p.key[:])
	_, _ = expected.Write(payload)
	if !hmac.Equal(encoded, expected.Sum(nil)) {
		return ErrLocalPlanSignature
	}
	return nil
}

func (p *LocalPlanner) sign(plan protocol.SessionPlan) (string, error) {
	payload, err := json.Marshal(plan.Unsigned())
	if err != nil {
		return "", fmt.Errorf("gateway: encode local plan: %w", err)
	}
	signature := hmac.New(sha256.New, p.key[:])
	_, _ = signature.Write(payload)
	return localSignaturePrefix + base64.RawURLEncoding.EncodeToString(signature.Sum(nil)), nil
}

func (p *LocalPlanner) selectProvider(kind protocol.SessionKind, requested string) (string, error) {
	requested = strings.ToLower(strings.TrimSpace(requested))
	if requested != "" && requested != "auto" {
		if _, ok := p.providers[requested]; !ok {
			return "", fmt.Errorf("gateway: BYOK credential for provider %q is not configured", requested)
		}
		if supportsLocalKind(requested, kind) {
			return requested, nil
		}
		return "", fmt.Errorf("gateway: provider %q does not support local %s", requested, kind)
	}
	selected := ""
	for provider := range p.providers {
		if !supportsLocalKind(provider, kind) {
			continue
		}
		if selected != "" {
			return "", errors.New("gateway: request.provider is required when several local providers are configured")
		}
		selected = provider
	}
	if selected == "" {
		return "", fmt.Errorf("gateway: no configured local provider supports %s", kind)
	}
	return selected, nil
}

func supportsLocalKind(provider string, kind protocol.SessionKind) bool {
	return (provider == "deepgram" && (kind == protocol.SessionKindSTT || kind == protocol.SessionKindTTS)) ||
		(provider == "elevenlabs" && kind == protocol.SessionKindTTS) ||
		(provider == "cartesia" && (kind == protocol.SessionKindSTT || kind == protocol.SessionKindTTS))
}

func localRoute(kind protocol.SessionKind, provider, model string) (protocol.PlanRoute, error) {
	model = strings.TrimSpace(model)
	switch {
	case provider == "deepgram" && kind == protocol.SessionKindSTT:
		if model == "" || model == "auto" {
			model = "nova-3"
		}
		return protocol.PlanRoute{Provider: provider, Model: model, Adapter: "deepgram.stt.v1", Transport: protocol.TransportWebSocket, Endpoint: "wss://api.deepgram.com/v1/listen"}, nil
	case provider == "deepgram" && kind == protocol.SessionKindTTS:
		if model == "" || model == "auto" {
			model = "aura-2-thalia-en"
		}
		return protocol.PlanRoute{Provider: provider, Model: model, Adapter: "deepgram.tts.v1", Transport: protocol.TransportWebSocket, Endpoint: "wss://api.deepgram.com/v1/speak"}, nil
	case provider == "elevenlabs" && kind == protocol.SessionKindTTS:
		if model == "" || model == "auto" {
			model = "eleven_flash_v2_5"
		}
		return protocol.PlanRoute{Provider: provider, Model: model, Adapter: "elevenlabs.tts.v1", Transport: protocol.TransportWebSocket, Endpoint: "wss://api.elevenlabs.io/v1/text-to-speech"}, nil
	case provider == "cartesia" && kind == protocol.SessionKindTTS:
		if model == "" || model == "auto" {
			model = "sonic-3"
		}
		return protocol.PlanRoute{Provider: provider, Model: model, Adapter: "cartesia.tts.v1", Transport: protocol.TransportWebSocket, Endpoint: "wss://api.cartesia.ai/tts/websocket"}, nil
	case provider == "cartesia" && kind == protocol.SessionKindSTT:
		if model == "" || model == "auto" {
			model = "ink-2"
		}
		return protocol.PlanRoute{Provider: provider, Model: model, Adapter: "cartesia.stt.v1", Transport: protocol.TransportWebSocket, Endpoint: "wss://api.cartesia.ai/stt/websocket"}, nil
	default:
		return protocol.PlanRoute{}, fmt.Errorf("gateway: unsupported local route provider=%q kind=%q", provider, kind)
	}
}

func localID(prefix string) (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("gateway: generate local %s id: %w", prefix, err)
	}
	return prefix + "_" + hex.EncodeToString(random[:]), nil
}
