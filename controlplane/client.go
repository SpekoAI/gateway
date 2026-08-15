// Package controlplane implements the runtime-facing Speko control-plane
// contract. It only performs setup and recovery-boundary calls; it is never a
// dependency of the audio or text hot path.
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

	"github.com/SpekoAI/gateway/protocol"
)

const maxResponseBytes = 1 << 20

// Config configures an authenticated control-plane client. The Speko API key
// authenticates setup calls and is never sent to an upstream model provider.
type Config struct {
	BaseURL           string
	APIKey            string
	HTTPClient        *http.Client
	UserAgent         string
	AllowInsecureHTTP bool
}

// Client obtains signed session plans and exchanges fallback plans at explicit
// recovery boundaries.
type Client struct {
	baseURL    *url.URL
	bearer     string
	httpClient *http.Client
	userAgent  string
}

// CreateOptions provides per-request metadata. IdempotencyKey is required so
// timeout retries cannot reserve two concurrent sessions.
type CreateOptions struct {
	IdempotencyKey string
}

// FallbackRequest explains a recovery-boundary exchange without sending raw
// media, transcripts, or provider credential values to the control plane.
type FallbackRequest struct {
	AttemptID      string `json:"attempt_id"`
	Reason         string `json:"reason"`
	ProviderCode   string `json:"provider_code,omitempty"`
	ProviderStatus int    `json:"provider_status,omitempty"`
}

// HTTPError is returned for non-2xx responses. RequestID allows live canaries
// and support tooling to correlate the control-plane decision without logging
// bearer credentials or response bodies. Code carries the control plane's own
// machine-readable refusal (`no_eligible_route`, `credit_exhausted`, …): a
// caller who pinned a provider the control plane cannot serve needs to see
// THAT, not a generic plan failure that reads like an outage.
type HTTPError struct {
	Status    int
	RequestID string
	Code      string
	Message   string
}

func (e *HTTPError) Error() string {
	detail := ""
	if e.Code != "" {
		detail = " (" + e.Code + ")"
	}
	if e.RequestID != "" {
		return fmt.Sprintf("control plane request failed with HTTP %d%s (request_id=%s)", e.Status, detail, e.RequestID)
	}
	return fmt.Sprintf("control plane request failed with HTTP %d%s", e.Status, detail)
}

// New constructs a control-plane client.
func New(config Config) (*Client, error) {
	if strings.TrimSpace(config.APIKey) == "" {
		return nil, errors.New("controlplane: api key is required")
	}
	endpoint, err := url.Parse(config.BaseURL)
	if err != nil || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || (endpoint.Scheme != "https" && !(config.AllowInsecureHTTP && endpoint.Scheme == "http")) {
		return nil, errors.New("controlplane: base url must be an absolute https URL")
	}
	if config.HTTPClient == nil {
		config.HTTPClient = http.DefaultClient
	}
	return &Client{
		baseURL:    endpoint,
		bearer:     config.APIKey,
		httpClient: config.HTTPClient,
		userAgent:  config.UserAgent,
	}, nil
}

// CreateSessionPlan calls POST /v1/session-plans and returns the exact signed
// plan issued by the control plane. Verification remains a separate mandatory
// runtime step so callers cannot accidentally trust HTTP transport alone.
func (c *Client) CreateSessionPlan(ctx context.Context, request protocol.SessionPlanRequest, options CreateOptions) (protocol.SessionPlan, string, error) {
	if err := request.Validate(); err != nil {
		return protocol.SessionPlan{}, "", fmt.Errorf("controlplane: invalid session plan request: %w", err)
	}
	if strings.TrimSpace(options.IdempotencyKey) == "" {
		return protocol.SessionPlan{}, "", errors.New("controlplane: idempotency key is required")
	}
	endpoint := c.resolve("/v1/session-plans")
	var plan protocol.SessionPlan
	requestID, err := c.postPlan(ctx, endpoint, request, options.IdempotencyKey, &plan)
	if err != nil {
		return protocol.SessionPlan{}, requestID, err
	}
	return plan, requestID, nil
}

// CreateSessionPlanBatch calls POST /v1/session-plan-batches and returns
// several independently signed plans.
//
// It exists so a gateway can keep plans warm ahead of demand. The latency a
// caller actually feels is between their first audio frame and the provider
// socket opening, and a plan fetched at that moment puts a full control-plane
// round trip inside it. Fetching plans in advance moves that round trip off the
// path entirely, and fetching them one at a time would only relocate it.
func (c *Client) CreateSessionPlanBatch(ctx context.Context, request protocol.SessionPlanRequest, count int, options CreateOptions) ([]protocol.SessionPlan, string, error) {
	if err := request.Validate(); err != nil {
		return nil, "", fmt.Errorf("controlplane: invalid session plan request: %w", err)
	}
	if count < 1 {
		return nil, "", errors.New("controlplane: plan batch count must be positive")
	}
	if strings.TrimSpace(options.IdempotencyKey) == "" {
		return nil, "", errors.New("controlplane: idempotency key is required")
	}
	body := struct {
		Count int                         `json:"count"`
		Plan  protocol.SessionPlanRequest `json:"plan"`
	}{Count: count, Plan: request}
	var batch struct {
		Plans []protocol.SessionPlan `json:"plans"`
	}
	requestID, err := c.postJSON(ctx, c.resolve("/v1/session-plan-batches"), body, options.IdempotencyKey, &batch)
	if err != nil {
		return nil, requestID, err
	}
	if len(batch.Plans) == 0 {
		return nil, requestID, errors.New("controlplane: plan batch response contained no plans")
	}
	for _, plan := range batch.Plans {
		if plan.Signature == "" {
			return nil, requestID, errors.New("controlplane: plan batch response contained an unsigned plan")
		}
	}
	return batch.Plans, requestID, nil
}

// ExchangeFallbackPlan obtains a newly signed plan only at a known recovery
// boundary. The exchange URL is itself signed inside the current plan and is
// therefore used verbatim after validation by the caller.
func (c *Client) ExchangeFallbackPlan(ctx context.Context, current protocol.SessionPlan, request FallbackRequest, idempotencyKey string) (protocol.SessionPlan, string, error) {
	if current.Fallback == nil || strings.TrimSpace(current.Fallback.ExchangeURL) == "" {
		return protocol.SessionPlan{}, "", errors.New("controlplane: current plan does not permit fallback")
	}
	if request.AttemptID != current.AttemptID {
		return protocol.SessionPlan{}, "", errors.New("controlplane: fallback attempt_id must match current plan")
	}
	if strings.TrimSpace(request.Reason) == "" || strings.TrimSpace(idempotencyKey) == "" {
		return protocol.SessionPlan{}, "", errors.New("controlplane: fallback reason and idempotency key are required")
	}
	endpoint, err := url.Parse(current.Fallback.ExchangeURL)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil || !sameOrigin(endpoint, c.baseURL) {
		return protocol.SessionPlan{}, "", errors.New("controlplane: fallback exchange URL must use the configured control-plane origin")
	}
	var plan protocol.SessionPlan
	requestID, err := c.postPlan(ctx, endpoint, request, idempotencyKey, &plan)
	if err != nil {
		return protocol.SessionPlan{}, requestID, err
	}
	return plan, requestID, nil
}

func (c *Client) postPlan(ctx context.Context, endpoint *url.URL, value any, idempotencyKey string, target *protocol.SessionPlan) (string, error) {
	body, requestID, err := c.post(ctx, endpoint, value, idempotencyKey)
	if err != nil {
		return requestID, err
	}
	if err := json.Unmarshal(body, target); err == nil && target.Signature != "" {
		return requestID, nil
	}
	var wrapped struct {
		Plan protocol.SessionPlan `json:"plan"`
	}
	if err := json.Unmarshal(body, &wrapped); err != nil || wrapped.Plan.Signature == "" {
		return requestID, errors.New("controlplane: response did not contain a signed session plan")
	}
	*target = wrapped.Plan
	return requestID, nil
}

func (c *Client) postJSON(ctx context.Context, endpoint *url.URL, value any, idempotencyKey string, target any) (string, error) {
	body, requestID, err := c.post(ctx, endpoint, value, idempotencyKey)
	if err != nil {
		return requestID, err
	}
	if err := json.Unmarshal(body, target); err != nil {
		return requestID, errors.New("controlplane: response could not be decoded")
	}
	return requestID, nil
}

func (c *Client) post(ctx context.Context, endpoint *url.URL, value any, idempotencyKey string) ([]byte, string, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return nil, "", fmt.Errorf("controlplane: encode request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return nil, "", fmt.Errorf("controlplane: create request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+c.bearer)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Idempotency-Key", idempotencyKey)
	request.Header.Set("Speko-Protocol-Revision", fmt.Sprint(protocol.CurrentRevision))
	if c.userAgent != "" {
		request.Header.Set("User-Agent", c.userAgent)
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, "", fmt.Errorf("controlplane: send request: %w", err)
	}
	defer response.Body.Close()
	requestID := response.Header.Get("X-Request-ID")
	body, err = io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return nil, requestID, fmt.Errorf("controlplane: read response: %w", err)
	}
	if len(body) > maxResponseBytes {
		return nil, requestID, errors.New("controlplane: response exceeds size limit")
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		// The hosted control plane answers refusals with `error.code` and no
		// message; older shapes carry `error.message`. Both are bounded by the
		// read limit above and neither is trusted for anything but display.
		var failure struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		_ = json.Unmarshal(body, &failure)
		return nil, requestID, &HTTPError{Status: response.StatusCode, RequestID: requestID, Code: failure.Error.Code, Message: failure.Error.Message}
	}
	return body, requestID, nil
}

func (c *Client) resolve(path string) *url.URL {
	endpoint := *c.baseURL
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + path
	endpoint.RawQuery = ""
	return &endpoint
}

func sameOrigin(left, right *url.URL) bool {
	return strings.EqualFold(left.Scheme, right.Scheme) && strings.EqualFold(left.Host, right.Host)
}
