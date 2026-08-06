package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// InstanceHeartbeat is the content-free process state exposed in the customer
// dashboard. Workload identity is optional because one gateway may serve
// framework-neutral sessions or several workloads.
type InstanceHeartbeat struct {
	RuntimeName      string            `json:"runtime_name"`
	RuntimeVersion   string            `json:"runtime_version"`
	WorkloadType     string            `json:"workload_type,omitempty"`
	WorkloadID       string            `json:"workload_id,omitempty"`
	StartedAt        time.Time         `json:"started_at"`
	ActiveSessions   int64             `json:"active_sessions"`
	PendingSessions  int               `json:"pending_sessions"`
	SessionCapacity  int               `json:"session_capacity"`
	SessionsTotal    uint64            `json:"sessions_total"`
	TelemetryDropped uint64            `json:"telemetry_dropped"`
	Draining         bool              `json:"draining"`
	Labels           map[string]string `json:"labels,omitempty"`
}

// ReportInstance upserts one authenticated runtime instance heartbeat.
func (c *Client) ReportInstance(ctx context.Context, instanceID string, heartbeat InstanceHeartbeat) error {
	if strings.TrimSpace(instanceID) == "" || strings.TrimSpace(heartbeat.RuntimeName) == "" || strings.TrimSpace(heartbeat.RuntimeVersion) == "" || heartbeat.StartedAt.IsZero() || heartbeat.ActiveSessions < 0 || heartbeat.PendingSessions < 0 || heartbeat.SessionCapacity < 1 {
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
	endpoint := c.resolve("/v1/runtime-instances/" + url.PathEscape(instanceID))
	var body io.Reader
	if value != nil {
		encoded, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("controlplane: encode runtime instance heartbeat: %w", err)
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), body)
	if err != nil {
		return fmt.Errorf("controlplane: create runtime instance request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+c.bearer)
	request.Header.Set("Accept", "application/json")
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
	return nil
}
