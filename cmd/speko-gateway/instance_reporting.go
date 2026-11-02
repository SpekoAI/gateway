package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/SpekoAI/gateway/controlplane"
)

var errInstanceSequenceExhausted = errors.New("runtime instance sequence exhausted")
var errInstanceClosed = errors.New("runtime instance lifetime is closed")

// One reporter belongs to one hosted process, including its final draining
// report and DELETE. Failed reports retain their exact sequence and body;
// cancellation is not evidence that the server did not commit the report.
type instanceReporter struct {
	mu          sync.Mutex
	client      *controlplane.Client
	instanceID  string
	incarnation string
	startedAt   time.Time
	sequence    uint32
	pending     *controlplane.InstanceHeartbeatV2
	terminal    *controlplane.InstanceLifetimeV2
}

func newInstanceReporter(client *controlplane.Client, instanceID string, startedAt time.Time) (*instanceReporter, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, errors.New("create runtime instance lifetime")
	}
	id[6] = id[6]&0x0f | 0x40
	id[8] = id[8]&0x3f | 0x80
	incarnation := fmt.Sprintf("%x-%x-%x-%x-%x", id[:4], id[4:6], id[6:8], id[8:10], id[10:])
	return &instanceReporter{client: client, instanceID: instanceID, incarnation: incarnation, startedAt: startedAt}, nil
}

func (r *instanceReporter) report(ctx context.Context, heartbeat controlplane.InstanceHeartbeat) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.terminal != nil {
		return errInstanceClosed
	}
	// Keep the final uint32 sequence for terminal DELETE. Never wrap or create
	// a replacement incarnation inside this process to evade the high-water mark.
	if r.sequence >= math.MaxUint32-1 {
		return errInstanceSequenceExhausted
	}
	if r.pending != nil {
		if err := r.sendPending(ctx); err != nil {
			return err
		}
	}
	r.sequence++
	heartbeat.StartedAt = r.startedAt
	if heartbeat.Labels != nil {
		labels := make(map[string]string, len(heartbeat.Labels))
		for key, value := range heartbeat.Labels {
			labels[key] = value
		}
		heartbeat.Labels = labels
	}
	r.pending = &controlplane.InstanceHeartbeatV2{InstanceHeartbeat: heartbeat, IncarnationID: r.incarnation, Sequence: r.sequence}
	err := r.sendPending(ctx)
	// A lost final PUT acknowledgement must not keep the process retrying
	// forever. Always drain, retaining the final sequence for DELETE. Return
	// only the fixed error, since the command logs its terminal exit reason.
	if r.sequence == math.MaxUint32-1 {
		return errInstanceSequenceExhausted
	}
	return err
}

func (r *instanceReporter) sendPending(ctx context.Context) error {
	if err := retryInstanceRequest(ctx, func() error { return r.client.ReportInstanceV2(ctx, r.instanceID, *r.pending) }); err != nil {
		return err
	}
	r.pending = nil
	return nil
}

func (r *instanceReporter) close(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.terminal == nil {
		r.sequence++ // report always reserves this sequence, including after failure.
		r.terminal = &controlplane.InstanceLifetimeV2{IncarnationID: r.incarnation, StartedAt: r.startedAt, Sequence: r.sequence}
		r.pending = nil
	}
	// Preparing DELETE is terminal even if its acknowledgement is lost. No
	// later PUT is allowed, and another close retries this identical identity.
	return retryInstanceRequest(ctx, func() error { return r.client.DeregisterInstanceV2(ctx, r.instanceID, *r.terminal) })
}

// The caller's five-second context bounds both attempts together. The next
// periodic report may retry the retained body, but never silently downgrades.
func retryInstanceRequest(ctx context.Context, send func() error) error {
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err = send()
		if err == nil {
			return nil
		}
		var response *controlplane.HTTPError
		if errors.As(err, &response) && response.Status < 500 && response.Status != 429 {
			return err
		}
	}
	return err
}
