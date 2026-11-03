package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/SpekoAI/gateway/protocol"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// InstanceHeartbeat is the content-free process state exposed in the customer
// dashboard. Workload identity is optional because one gateway may serve
// framework-neutral sessions or several workloads.
type InstanceHeartbeat struct {
	// Local snapshot is serialized only by the explicit v3 envelope.
	Capacity         *protocol.CapacitySnapshot `json:"-"`
	RuntimeName      string                     `json:"runtime_name"`
	RuntimeVersion   string                     `json:"runtime_version"`
	WorkloadType     string                     `json:"workload_type,omitempty"`
	WorkloadID       string                     `json:"workload_id,omitempty"`
	StartedAt        time.Time                  `json:"started_at"`
	ActiveSessions   int64                      `json:"active_sessions"`
	PendingSessions  int                        `json:"pending_sessions"`
	SessionCapacity  int                        `json:"session_capacity"`
	SessionsTotal    uint64                     `json:"sessions_total"`
	TelemetryDropped uint64                     `json:"telemetry_dropped"`
	Draining         bool                       `json:"draining"`
	Labels           map[string]string          `json:"labels,omitempty"`
}

// InstanceHeartbeatV2 opts into lifetime fencing, not capacity attribution.
// Callers serialize mutations and retain the same sequence/body on retries.
type InstanceHeartbeatV2 struct {
	InstanceHeartbeat
	IncarnationID string `json:"incarnation_id"`
	Sequence      uint32 `json:"sequence"`
}

// InstanceLifetimeV2 identifies one process for a terminal DELETE.
// Reserve a sequence for shutdown and never wrap it under one incarnation.
type InstanceHeartbeatV3 struct {
	InstanceHeartbeatV2
	Capacity protocol.CapacitySnapshot `json:"capacity"`
}

func (c *Client) ReportInstanceV3(ctx context.Context, instanceID string, h InstanceHeartbeatV3) error {
	if !validHeartbeat(instanceID, h.InstanceHeartbeat) || !incarnationPattern.MatchString(h.IncarnationID) || h.Sequence == 0 || h.Capacity.Validate(h.ActiveSessions, h.PendingSessions) != nil {
		return errors.New("controlplane: invalid attributed runtime heartbeat")
	}
	return c.sendVersionedInstanceRequest(ctx, http.MethodPut, instanceID, h, "3")
}
func (c *Client) DeregisterInstanceV3(ctx context.Context, instanceID string, h InstanceLifetimeV2) error {
	if strings.TrimSpace(instanceID) == "" || !incarnationPattern.MatchString(h.IncarnationID) || h.StartedAt.IsZero() || h.Sequence == 0 {
		return errors.New("controlplane: invalid attributed runtime lifetime")
	}
	return c.sendVersionedInstanceRequest(ctx, http.MethodDelete, instanceID, h, "3")
}

type InstanceLifetimeV2 struct {
	IncarnationID string    `json:"incarnation_id"`
	StartedAt     time.Time `json:"started_at"`
	Sequence      uint32    `json:"sequence"`
}

const InstanceProtocolHeader = "Speko-Instance-Protocol"

var incarnationPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// ReportInstanceV2 requires explicit server acknowledgement. It never falls
// back to an unversioned report, which could undo a lifetime fence.
func (c *Client) ReportInstanceV2(ctx context.Context, instanceID string, heartbeat InstanceHeartbeatV2) error {
	if !validHeartbeat(instanceID, heartbeat.InstanceHeartbeat) || !incarnationPattern.MatchString(heartbeat.IncarnationID) || heartbeat.Sequence == 0 {
		return errors.New("controlplane: invalid versioned runtime instance heartbeat")
	}
	return c.sendVersionedInstanceRequest(ctx, http.MethodPut, instanceID, heartbeat, "2")
}

// DeregisterInstanceV2 closes only the specified lifetime. A timeout is retried
// with this exact body; successful DELETE is terminal for that incarnation.
func (c *Client) DeregisterInstanceV2(ctx context.Context, instanceID string, lifetime InstanceLifetimeV2) error {
	if strings.TrimSpace(instanceID) == "" || !incarnationPattern.MatchString(lifetime.IncarnationID) || lifetime.StartedAt.IsZero() || lifetime.Sequence == 0 {
		return errors.New("controlplane: invalid runtime instance lifetime")
	}
	return c.sendVersionedInstanceRequest(ctx, http.MethodDelete, instanceID, lifetime, "2")
}

func validHeartbeat(instanceID string, heartbeat InstanceHeartbeat) bool {
	return strings.TrimSpace(instanceID) != "" && strings.TrimSpace(heartbeat.RuntimeName) != "" && strings.TrimSpace(heartbeat.RuntimeVersion) != "" && !heartbeat.StartedAt.IsZero() && heartbeat.ActiveSessions >= 0 && heartbeat.PendingSessions >= 0 && heartbeat.SessionCapacity >= 1
}

// ReportInstance upserts one authenticated runtime instance heartbeat.
func (c *Client) ReportInstance(ctx context.Context, instanceID string, heartbeat InstanceHeartbeat) error {
	if !validHeartbeat(instanceID, heartbeat) {
		return errors.New("controlplane: invalid runtime instance heartbeat")
	}
	return c.sendInstanceRequest(ctx, http.MethodPut, instanceID, heartbeat)
}

// DeregisterInstance marks one authenticated runtime instance offline without
// deleting its historical identity from the customer dashboard.
func (c *Client) DeregisterInstance(ctx context.Context, instanceID string) error {
	if strings.TrimSpace(instanceID) == "" {
		return errors.New("controlplane: runtime instance id is required")
	}
	return c.sendInstanceRequest(ctx, http.MethodDelete, instanceID, nil)
}

func (c *Client) sendInstanceRequest(ctx context.Context, method, instanceID string, value any) error {
	return c.sendVersionedInstanceRequest(ctx, method, instanceID, value, "")
}

func (c *Client) sendVersionedInstanceRequest(ctx context.Context, method, instanceID string, value any, version string) error {
	endpoint := c.resolve("/v1/runtime-instances/" + instanceID)
	// URL.Path holds decoded text. Keep the ID as one escaped segment in
	// RawPath so serialization escapes it once, preserving literal percent
	// sequences as distinct identities and retaining the configured base path.
	basePath := c.baseURL.EscapedPath()
	// resolve trims decoded trailing slashes, so remove their escaped forms
	// too. Otherwise URL.String discards an inconsistent RawPath entirely.
	for strings.HasSuffix(basePath, "/") || len(basePath) >= 3 && strings.EqualFold(basePath[len(basePath)-3:], "%2f") {
		trim := 1
		if basePath[len(basePath)-1] != '/' {
			trim = 3
		}
		basePath = basePath[:len(basePath)-trim]
	}
	endpoint.RawPath = basePath + "/v1/runtime-instances/" + url.PathEscape(instanceID)
	var body io.Reader
	if value != nil {
		encoded, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("controlplane: encode runtime instance heartbeat: %w", err)
		}
		limit := 16 * 1024
		if version == "3" {
			limit = 64 * 1024
		}
		if version != "" && len(encoded) > limit {
			return errors.New("controlplane: runtime instance report exceeds 16 KiB")
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), body)
	if err != nil {
		return fmt.Errorf("controlplane: create runtime instance request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+c.bearer)
	request.Header.Set("Accept", "application/json")
	if version != "" {
		request.Header.Set(InstanceProtocolHeader, version)
	}
	if value != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if c.userAgent != "" {
		request.Header.Set("User-Agent", c.userAgent)
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("controlplane: send runtime instance request: %w", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxResponseBytes))
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return &HTTPError{Status: response.StatusCode, RequestID: response.Header.Get("X-Request-ID")}
	}
	if version != "" && (response.StatusCode != http.StatusNoContent || response.Header.Get(InstanceProtocolHeader) != version) {
		return errors.New("controlplane: runtime instance protocol was not acknowledged")
	}
	return nil
}
