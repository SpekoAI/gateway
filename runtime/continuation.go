package runtime

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SpekoAI/gateway/controlplane"
	"github.com/SpekoAI/gateway/protocol"
)

const renewalLead = 15 * time.Second

// fundedClock uses SQL time plus a conservative full RTT. Wall/monotonic
// divergence pauses traffic until a shared new sample succeeds. The last
// accepted hard timer stays armed throughout that request.
type fundedClock struct {
	client                       EntitlementClient
	plan                         protocol.SessionPlan
	wall                         func() time.Time
	refreshMu                    sync.Mutex
	mu                           sync.Mutex
	server, localWall, monotonic time.Time
}

func (c *fundedClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.server.Add(max(time.Since(c.monotonic), c.wall().Sub(c.localWall), 0))
}

// A failed resynchronization must not stamp durable receipts with an
// arbitrary wall-clock jump. Until a new SQL sample arrives, retain the last
// trusted sample plus monotonic elapsed time for event timestamps.
func (c *fundedClock) timestamp() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.server.IsZero() {
		return time.Time{}
	}
	return c.server.Add(max(time.Since(c.monotonic), 0))
}

func (c *fundedClock) stale() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	delta := c.wall().Sub(c.localWall) - time.Since(c.monotonic)
	return c.server.IsZero() || delta > time.Second || delta < -time.Second
}

func (c *fundedClock) synchronize(ctx context.Context) (bool, error) {
	if !c.stale() {
		return false, nil
	}
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()
	if !c.stale() {
		return false, nil
	}
	for range 2 {
		started, wallStarted := time.Now(), c.wall()
		sampleCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		sample, err := c.client.EntitlementClock(sampleCtx, c.plan)
		cancel()
		if err != nil {
			return false, err
		}
		received, wall := time.Now(), c.wall()
		elapsed, wallElapsed := received.Sub(started), wall.Sub(wallStarted)
		if delta := wallElapsed - elapsed; delta > time.Second || delta < -time.Second {
			continue
		}
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		c.mu.Lock()
		c.server, c.localWall, c.monotonic = sample.Add(max(elapsed, wallElapsed, 0)), wall, received
		c.mu.Unlock()
		return true, nil
	}
	return false, errors.New("runtime: clock changed during entitlement synchronization")
}

// ErrFundedGoogleClockAuthority refuses a funded shape whose current signed
// plan supplies no accepted clock endpoint. It never falls into OSS admission.
var ErrFundedGoogleClockAuthority = errors.New("runtime: funded Google plan has no signed clock authority")

// ErrFundedGoogleAdapter refuses unknown adapter implementations without
// treating their funded Google plans as ordinary local-clock sessions.
var ErrFundedGoogleAdapter = errors.New("runtime: funded Google adapter is not supported")

func fundedGoogle(plan protocol.SessionPlan) bool {
	return plan.Execution.CredentialSource == protocol.CredentialsManaged &&
		plan.Execution.ProviderRoute == protocol.RouteProviderDirect && plan.Route.Provider == "google" &&
		plan.UsesFundedContinuation()
}

func (e *Engine) supportsFundedGoogle(plan protocol.SessionPlan) bool {
	// Every ID, including the default, must identify an installed implementation
	// that retains physical caps. Wrappers must forward its actual capability.
	adapter, ok := e.adapters[plan.Route.Adapter].(GoogleRealtimeAdapter)
	return ok && adapter.GoogleRealtimeAdapter()
}

func renewableGoogle(plan protocol.SessionPlan) bool {
	b := plan.Reservation.Billing
	return fundedGoogle(plan) && b != nil && b.RenewalURL != "" &&
		b.RenewableUntil.After(plan.Reservation.LeaseExpiresAt)
}

type continuation struct {
	terminalLeaseExpired atomic.Bool
	ctx                  context.Context
	cancel               context.CancelFunc
	session              *Session
	client               EntitlementClient
	clock                *fundedClock
	stream               ResumableProviderStream
	mu                   sync.Mutex
	expires              time.Time
	// hardDeadline is the monotonic cap anchored before this socket's setup.
	// SQL resampling and slow setup may tighten it, never restart its lifetime.
	hardDeadline                       time.Time
	sequence                           int64
	authorizedUnits                    int64
	pending, paused, disabled, closing bool
	changed                            chan struct{}
}

func (c *continuation) close() bool {
	c.mu.Lock()
	c.closing, c.disabled, c.pending = true, true, false
	paused := c.paused
	c.mu.Unlock()
	c.cancel()
	return paused
}

func (c *continuation) notifyLocked() { close(c.changed); c.changed = make(chan struct{}) }

func (c *continuation) synchronize(ctx context.Context) error {
	refreshed, err := c.clock.synchronize(ctx)
	if err != nil {
		return err
	}
	if refreshed {
		c.mu.Lock()
		if !c.paused && c.session.ctx.Err() == nil {
			c.session.setLeaseDeadline(c.expires)
		}
		c.mu.Unlock()
	}
	return nil
}

func (c *continuation) beforeIO() error {
	for {
		if err := c.synchronize(c.session.ctx); err != nil {
			return err
		}
		c.mu.Lock()
		paused, expires, hardDeadline, changed := c.paused, c.expires, c.hardDeadline, c.changed
		c.mu.Unlock()
		if c.session.ctx.Err() != nil {
			return ErrSessionClosed
		}
		if !paused && c.clock.now().Before(expires) && time.Now().Before(hardDeadline) {
			return nil
		}
		if !paused {
			c.expire(expires)
			continue
		}
		select {
		case <-changed:
		case <-c.session.ctx.Done():
			return ErrSessionClosed
		}
	}
}

func (c *continuation) ready() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.paused && !c.clock.stale() && c.clock.now().Before(c.expires) && time.Now().Before(c.hardDeadline) && c.session.ctx.Err() == nil
}

func (c *continuation) audioFits(accepted, frame int64) bool {
	c.mu.Lock()
	units := c.authorizedUnits
	c.mu.Unlock()
	bytesPerSecond := int64(c.session.media.SampleRateHz) * int64(c.session.media.Channels) * 2
	maximum := int64(math.MaxInt64)
	if units <= math.MaxInt64/bytesPerSecond {
		maximum = units * bytesPerSecond
	}
	return frame <= maximum-accepted
}

// expire detaches the old socket even when a committed renewal response is
// missing. Recovery may use that same pending intent, never that old socket.
func (c *continuation) expire(expected time.Time) {
	c.mu.Lock()
	if c.paused || !c.expires.Equal(expected) || (c.clock.now().Before(expected) && time.Now().Before(c.hardDeadline)) {
		c.mu.Unlock()
		return
	}
	c.paused = true
	recoverable := c.pending && c.stream.CanResume()
	if !recoverable {
		c.terminalLeaseExpired.Store(true)
	}
	c.stream.Detach()
	c.notifyLocked()
	c.mu.Unlock()
	if !recoverable {
		c.session.fail(ErrSessionLeaseExpired)
	}
}

func (c *continuation) run() {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := c.synchronize(c.ctx); err != nil {
			if c.ctx.Err() == nil {
				c.session.fail(errors.New("runtime: entitlement clock unavailable"))
			}
			return
		}
		c.mu.Lock()
		remaining := c.expires.Sub(c.clock.now())
		if !c.pending && !c.disabled && remaining <= renewalLead && c.expires.Before(c.session.plan.Reservation.Billing.RenewableUntil) {
			c.pending = true
			go c.renew(c.expires)
		}
		c.mu.Unlock()
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (c *continuation) renew(previous time.Time) {
	ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	defer cancel()
	key := "renew:" + c.session.plan.AttemptID + ":" + previous.UTC().Format(time.RFC3339Nano)
	var issued *controlplane.EntitlementRenewal
	var failure error
	attempts := 0
	for ctx.Err() == nil {
		if err := c.synchronize(ctx); err != nil {
			failure = err
			break
		}
		now := c.clock.now()
		if !now.Before(c.session.plan.Reservation.Billing.RenewableUntil) {
			failure = ErrSessionLeaseExpired
			break
		}
		if !c.stream.CanResume() {
			if !now.Before(previous) {
				failure = ErrResumptionUnavailable
				break
			}
			if !continuationWait(ctx) {
				break
			}
			continue
		}
		if attempts == 3 {
			break
		}
		attempts++
		requestCtx, requestCancel := context.WithTimeout(ctx, 3*time.Second)
		renewal, _, err := c.client.RenewEntitlement(requestCtx, c.session.plan, controlplane.EntitlementRenewalRequest{PreviousExpiresAt: previous}, key)
		requestCancel()
		if ctx.Err() != nil {
			break
		}
		if err != nil {
			failure = err
			var refusal *controlplane.HTTPError
			if errors.As(err, &refusal) && refusal.Status != http.StatusServiceUnavailable && refusal.Status < 500 {
				break
			}
			if !continuationWait(ctx) {
				break
			}
			continue
		}
		if renewal.Validate(c.session.plan, previous) != nil {
			failure = errors.New("runtime: invalid funded continuation")
			break
		}
		if issued != nil && *issued != renewal {
			failure = errors.New("runtime: funded continuation changed on replay")
			break
		}
		issued = &renewal
		c.mu.Lock()
		sequence := c.sequence
		authorized := c.authorizedUnits
		c.mu.Unlock()
		if sequence == math.MaxInt64 || renewal.Sequence != sequence+1 || renewal.AuthorizedUnits > math.MaxInt64-authorized {
			failure = errors.New("runtime: invalid funded continuation sequence")
			break
		}
		if err := c.synchronize(ctx); err != nil {
			failure = err
			break
		}
		if !c.clock.now().Before(renewal.Credential.ExpiresAt) {
			failure = ErrSessionLeaseExpired
			break
		}
		// A resumability withdrawal during HTTP requires a fresh same-key delivery
		// check once another handle arrives. Never silently start a new conversation.
		if !c.stream.CanResume() {
			continue
		}
		// Freeze the absolute credential/lease/root horizon as elapsed time
		// BEFORE any provider lock, Detach or Resume can block. Validation binds
		// the credential expiry to the funded lease and clips it to the original
		// renewable horizon; no new timing authority is introduced here.
		anchored := time.Now()
		hardDeadline := anchored.Add(renewal.Credential.ExpiresAt.Sub(c.clock.now()))
		c.session.providerMu.Lock()
		c.mu.Lock()
		if ctx.Err() != nil || !time.Now().Before(hardDeadline) {
			c.mu.Unlock()
			c.session.providerMu.Unlock()
			failure = ErrSessionLeaseExpired
			break
		}
		c.paused = true
		c.stream.Detach()
		c.session.stopLeaseTimer()
		c.notifyLocked()
		adopted, expired := false, false // protected by c.mu, including the timer
		adoptionTimer := time.AfterFunc(max(time.Until(hardDeadline), 0), func() {
			c.mu.Lock()
			if adopted {
				c.mu.Unlock()
				return // canonical timer already owns this exact deadline
			}
			expired = true
			c.terminalLeaseExpired.Store(true)
			c.mu.Unlock()
			// Detach is concurrent with Resume by contract. Keep the terminal
			// fence visible before physical closure can wake an event consumer.
			c.stream.Detach()
			c.session.fail(ErrSessionLeaseExpired)
		})
		defer func() {
			if adopted {
				adoptionTimer.Stop()
				return
			}
			// Failure/cancellation is not physical closure evidence. Preserve
			// this cap until actual serialized teardown and event draining join.
			go func() { <-c.session.physicalStopped; adoptionTimer.Stop() }()
		}()
		c.mu.Unlock()
		resumeCtx, resumeCancel := context.WithDeadlineCause(ctx, hardDeadline, ErrSessionLeaseExpired)
		// Keep the cap distinct from the shorter request/cancellation budget.
		// The provider must arm it on a replacement before setup can block.
		resumeCtx = context.WithValue(resumeCtx, providerPhysicalDeadlineKey{}, hardDeadline)
		err = c.stream.Resume(resumeCtx, renewal.Credential)
		c.session.providerMu.Unlock()
		resumeCancel()
		if err != nil {
			failure = err
			break
		}
		if err := c.synchronize(ctx); err != nil {
			failure = err
			break
		}
		c.mu.Lock()
		if expired || ctx.Err() != nil || !time.Now().Before(hardDeadline) || !c.clock.now().Before(renewal.LeaseExpiresAt) {
			c.mu.Unlock()
			failure = ErrSessionLeaseExpired
			break
		}
		c.expires, c.sequence, c.paused, c.pending = renewal.LeaseExpiresAt, renewal.Sequence, false, false
		c.hardDeadline = hardDeadline
		c.authorizedUnits += renewal.AuthorizedUnits
		c.session.setLeaseDeadline(c.expires)
		// Install the canonical owner before retiring the adoption timer. A
		// callback already waiting on c.mu observes adopted, and a late return
		// cannot reset the cap or advance allowance/sequence.
		adopted = true
		adoptionTimer.Stop()
		c.notifyLocked()
		c.mu.Unlock()
		return
	}
	c.mu.Lock()
	c.pending, c.disabled = false, true
	paused := c.paused
	closing := c.closing
	c.notifyLocked()
	c.mu.Unlock()
	if paused && !closing && c.session.ctx.Err() == nil {
		if failure == nil {
			failure = ctx.Err()
		}
		if failure == nil {
			failure = errors.New("renewal delivery retry budget exhausted")
		}
		c.session.fail(fmt.Errorf("runtime: funded continuation unavailable: %w", failure))
	}
}

func continuationWait(ctx context.Context) bool {
	timer := time.NewTimer(250 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
