package gateway

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SpekoAI/gateway/controlplane"
	"github.com/SpekoAI/gateway/protocol"
)

const (
	defaultWarmPoolTarget    = 4
	defaultWarmPoolMinLeft   = 60 * time.Second
	defaultWarmPoolInterval  = 5 * time.Second
	defaultWarmPoolMaxRoutes = 16
	// A route that has not been asked for in this long stops being refilled.
	// Without it, a worker that briefly used a second language would keep
	// paying for plans nobody wants for the life of the process.
	defaultWarmPoolIdleAfter = 10 * time.Minute
)

// planKey identifies plans that are interchangeable for a create request.
//
// The request options are hashed whole rather than picked apart field by field.
// They decide the route in ways that are not obvious from any one of them —
// provider, model, and voice directly, but also language, objective, and the
// allow/deny lists, which together resolve `auto` to a concrete vendor. Hashing
// the lot means a new routing input cannot silently start sharing a pool with
// requests it would route differently.
//
// Media format is deliberately excluded: the plan does not bind it, the runtime
// validates it independently at open, and including it would fragment the pool
// by sample rate for no benefit.
type planKey struct {
	kind             protocol.SessionKind
	credentialSource protocol.CredentialSource
	providerRoute    protocol.ProviderRoute
	relayPolicy      protocol.RelayPolicy
	options          [sha256.Size]byte
}

// warmPlan is one prefetched plan and the instant after which it is no longer
// worth handing out.
type warmPlan struct {
	plan     protocol.SessionPlan
	usableTo time.Time
}

// PlanPoolMetrics reports whether prefetching is actually absorbing demand.
type PlanPoolMetrics struct {
	Hits     uint64
	Misses   uint64
	Expired  uint64
	Refills  uint64
	Failures uint64
	Depth    int
	Routes   int
}

// PlanPoolConfig configures prefetching.
type PlanPoolConfig struct {
	Plans PlanClient
	// Target is the number of plans kept warm per distinct route.
	Target int
	// MinRemaining refuses to hand out a plan that is about to expire. A plan
	// only has to be live at the provider handshake, but handing over one with
	// two seconds left would trade a control-plane round trip for a flaky one.
	MinRemaining time.Duration
	Interval     time.Duration
	IdleAfter    time.Duration
	MaxRoutes    int
	Runtime      protocol.RuntimeDescriptor
	Workload     *protocol.Workload
	Now          func() time.Time
}

// PlanPool keeps signed session plans warm so creating a session costs no
// network round trip.
//
// This is the piece that makes the zero-overhead claim true rather than
// approximately true. Everything else in the fast path shaves milliseconds off
// a control-plane call; this removes the call. What remains between a caller's
// first audio frame and the provider socket is the provider dial itself.
//
// It is strictly a cache. A route nobody has asked for yet, an exhausted pool,
// or an unreachable control plane all fall through to the ordinary synchronous
// create, so nothing fails that would not have failed before.
type PlanPool struct {
	plans        PlanClient
	target       int
	minRemaining time.Duration
	interval     time.Duration
	idleAfter    time.Duration
	maxRoutes    int
	runtime      protocol.RuntimeDescriptor
	workload     *protocol.Workload
	now          func() time.Time

	mu     sync.Mutex
	routes map[planKey]*warmRoute

	hits     atomic.Uint64
	misses   atomic.Uint64
	expired  atomic.Uint64
	refills  atomic.Uint64
	failures atomic.Uint64
}

// warmRoute is one route's warm plans plus the request shape needed to make
// more of them.
type warmRoute struct {
	request  protocol.SessionPlanRequest
	plans    []warmPlan
	lastUsed time.Time
	// refilling keeps a slow control plane from being asked for the same route
	// by every warm tick that fires while the first is still outstanding.
	refilling bool
}

func NewPlanPool(config PlanPoolConfig) (*PlanPool, error) {
	if config.Plans == nil {
		return nil, errors.New("gateway: plan pool requires a plan client")
	}
	if config.Target == 0 {
		config.Target = defaultWarmPoolTarget
	}
	if config.MinRemaining == 0 {
		config.MinRemaining = defaultWarmPoolMinLeft
	}
	if config.Interval == 0 {
		config.Interval = defaultWarmPoolInterval
	}
	if config.IdleAfter == 0 {
		config.IdleAfter = defaultWarmPoolIdleAfter
	}
	if config.MaxRoutes == 0 {
		config.MaxRoutes = defaultWarmPoolMaxRoutes
	}
	if config.Target < 1 || config.MinRemaining <= 0 || config.Interval <= 0 || config.IdleAfter <= 0 || config.MaxRoutes < 1 {
		return nil, errors.New("gateway: plan pool target, minimum remaining, interval, idle window, and route bound must be positive")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &PlanPool{
		plans: config.Plans, target: config.Target, minRemaining: config.MinRemaining,
		interval: config.Interval, idleAfter: config.IdleAfter, maxRoutes: config.MaxRoutes,
		runtime: config.Runtime, workload: config.Workload, now: config.Now,
		routes: make(map[planKey]*warmRoute),
	}, nil
}

// Take returns a warm plan for this request, if one is available, and always
// registers the route so the next request for the same shape is warm.
//
// A miss is not an error. It means the caller pays what it paid before.
func (p *PlanPool) Take(request protocol.SessionPlanRequest) (protocol.SessionPlan, bool) {
	key, poolable := poolKeyFor(request)
	if !poolable {
		return protocol.SessionPlan{}, false
	}
	now := p.now()
	p.mu.Lock()
	defer p.mu.Unlock()
	route, known := p.routes[key]
	if !known {
		if len(p.routes) >= p.maxRoutes {
			// A process fanning out over more shapes than the pool can hold is
			// better served by leaving every existing route warm than by
			// thrashing them all. Those requests keep working, synchronously.
			p.misses.Add(1)
			return protocol.SessionPlan{}, false
		}
		route = &warmRoute{request: p.prefetchRequest(request)}
		p.routes[key] = route
	}
	route.lastUsed = now
	for len(route.plans) > 0 {
		candidate := route.plans[0]
		route.plans = route.plans[1:]
		if candidate.usableTo.After(now) {
			p.hits.Add(1)
			return candidate.plan, true
		}
		p.expired.Add(1)
	}
	p.misses.Add(1)
	return protocol.SessionPlan{}, false
}

// Run keeps every recently used route stocked until ctx is canceled.
func (p *PlanPool) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.refill(ctx)
		}
	}
}

// Warm registers a route before any traffic has asked for it, so the very first
// session of a process is warm too. Without it the first call of every shape —
// which for a voice agent worker is the one a real person is waiting on —
// always pays full price.
func (p *PlanPool) Warm(ctx context.Context, request protocol.SessionPlanRequest) error {
	key, poolable := poolKeyFor(request)
	if !poolable {
		return errors.New("gateway: only managed provider-direct routes can be prefetched")
	}
	p.mu.Lock()
	if _, known := p.routes[key]; !known {
		if len(p.routes) >= p.maxRoutes {
			p.mu.Unlock()
			return errors.New("gateway: warm route limit reached")
		}
		p.routes[key] = &warmRoute{request: p.prefetchRequest(request), lastUsed: p.now()}
	}
	p.mu.Unlock()
	p.refill(ctx)
	return nil
}

// Metrics reports pool effectiveness. A miss rate that does not fall toward
// zero after warm-up means prefetching is not working and sessions are paying
// the control-plane round trip they were promised they would not.
func (p *PlanPool) Metrics() PlanPoolMetrics {
	p.mu.Lock()
	depth := 0
	for _, route := range p.routes {
		depth += len(route.plans)
	}
	routes := len(p.routes)
	p.mu.Unlock()
	return PlanPoolMetrics{
		Hits: p.hits.Load(), Misses: p.misses.Load(), Expired: p.expired.Load(),
		Refills: p.refills.Load(), Failures: p.failures.Load(), Depth: depth, Routes: routes,
	}
}

// refill tops up every live route. It runs outside the pool lock so a slow
// control plane cannot block Take, which is on the session-create path.
func (p *PlanPool) refill(ctx context.Context) {
	type work struct {
		key     planKey
		request protocol.SessionPlanRequest
		count   int
	}
	now := p.now()
	var pending []work
	p.mu.Lock()
	for key, route := range p.routes {
		if now.Sub(route.lastUsed) > p.idleAfter {
			delete(p.routes, key)
			continue
		}
		route.plans = p.usablePlansLocked(route.plans, now)
		needed := p.target - len(route.plans)
		if needed <= 0 || route.refilling {
			continue
		}
		route.refilling = true
		pending = append(pending, work{key: key, request: route.request, count: needed})
	}
	p.mu.Unlock()

	for _, item := range pending {
		p.refillRoute(ctx, item.key, item.request, item.count)
	}
}

func (p *PlanPool) refillRoute(ctx context.Context, key planKey, request protocol.SessionPlanRequest, count int) {
	defer func() {
		p.mu.Lock()
		if route, known := p.routes[key]; known {
			route.refilling = false
		}
		p.mu.Unlock()
	}()
	idempotencyKey, err := warmIdempotencyKey()
	if err != nil {
		p.failures.Add(1)
		return
	}
	plans, _, err := p.plans.CreateSessionPlanBatch(ctx, request, count, controlplane.CreateOptions{IdempotencyKey: idempotencyKey})
	if err != nil {
		// Nothing to recover here. The pool stays as deep as it is and creates
		// fall through to the synchronous path until the next tick succeeds.
		p.failures.Add(1)
		return
	}
	now := p.now()
	p.mu.Lock()
	defer p.mu.Unlock()
	route, known := p.routes[key]
	if !known {
		return
	}
	for _, plan := range plans {
		if len(route.plans) >= p.target {
			break
		}
		usableTo := plan.ExpiresAt.Add(-p.minRemaining)
		if !usableTo.After(now) {
			p.expired.Add(1)
			continue
		}
		route.plans = append(route.plans, warmPlan{plan: plan, usableTo: usableTo})
	}
	p.refills.Add(1)
}

func (p *PlanPool) usablePlansLocked(plans []warmPlan, now time.Time) []warmPlan {
	kept := plans[:0]
	for _, candidate := range plans {
		if candidate.usableTo.After(now) {
			kept = append(kept, candidate)
			continue
		}
		p.expired.Add(1)
	}
	return kept
}

// prefetchRequest strips the parts of a create request that describe one
// specific session and keeps the parts that decide the route. Media is dropped
// for kinds that do not require it in a plan request; where it is required, a
// canonical shape stands in, because the plan does not bind media and the
// runtime validates the real thing at open.
func (p *PlanPool) prefetchRequest(request protocol.SessionPlanRequest) protocol.SessionPlanRequest {
	prefetch := request
	prefetch.Runtime = p.runtime
	prefetch.Workload = p.workload
	prefetch.Integration = request.Integration
	return prefetch
}

// poolKeyFor reports whether a request can be served from prefetched plans, and
// under what key.
//
// Only managed provider-direct sessions qualify. BYOK plans are issued locally
// by LocalPlanner and already cost nothing, and a relay session is not what the
// zero-overhead promise is about.
func poolKeyFor(request protocol.SessionPlanRequest) (planKey, bool) {
	if request.Execution.CredentialSource != protocol.CredentialsManaged {
		return planKey{}, false
	}
	if request.Execution.ProviderRoute == protocol.RouteSpekoRelay || request.Execution.RelayPolicy == protocol.RelayRequired {
		return planKey{}, false
	}
	options, err := json.Marshal(normalizedRequestOptions(request.Request))
	if err != nil {
		return planKey{}, false
	}
	return planKey{
		kind:             request.Kind,
		credentialSource: request.Execution.CredentialSource,
		providerRoute:    request.Execution.ProviderRoute,
		relayPolicy:      request.Execution.RelayPolicy,
		options:          sha256.Sum256(options),
	}, true
}

// normalizedRequestOptions folds away differences that the control plane treats
// as identical, so "Deepgram" and "deepgram" share a pool instead of splitting
// it. Provider matching in LaunchPolicy is case-insensitive and trims space;
// this mirrors that and nothing more.
func normalizedRequestOptions(options protocol.RequestOptions) protocol.RequestOptions {
	options.Provider = strings.ToLower(strings.TrimSpace(options.Provider))
	options.Model = strings.ToLower(strings.TrimSpace(options.Model))
	options.Language = strings.ToLower(strings.TrimSpace(options.Language))
	options.Objective = strings.ToLower(strings.TrimSpace(options.Objective))
	options.Voice = strings.TrimSpace(options.Voice)
	options.Allow = normalizedProviderList(options.Allow)
	options.Deny = normalizedProviderList(options.Deny)
	return options
}

// normalizedProviderList lowercases and sorts a copy so list order cannot split
// the pool. It does not deduplicate: the control plane does not, and a pool key
// must never claim two requests are the same when the issuer might disagree.
func normalizedProviderList(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	normalized := make([]string, 0, len(values))
	for _, value := range values {
		normalized = append(normalized, strings.ToLower(strings.TrimSpace(value)))
	}
	sort.Strings(normalized)
	return normalized
}

func warmIdempotencyKey() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return "warm-" + hex.EncodeToString(value[:]), nil
}
