// Package relayapi defines the public wire contract of the Global Speko
// Relay: HTTP request and response bodies, WebSocket control and event
// frames, SSE stream events, the normalized error envelope, response
// headers, and the idempotency hashing rules shared by every relay endpoint.
//
// The package is the single source of truth for both sides of the wire: the
// relay edge implements these types, and the hand-authored OpenAPI/AsyncAPI
// documents are machine-checked against them. It deliberately depends only
// on the standard library so integrators can import the contract without
// pulling in relay internals.
//
// Contract decisions that shape the types:
//
//   - Routing is a closed tagged union. An entirely omitted routing object
//     means {mode: auto, objective: balanced}; handlers apply that default
//     with Routing.NormalizeDefault before validating. Mixed auto/explicit
//     fields and unknown objectives are rejected, never silently ignored.
//
//   - Errors are always {"error": {code, message, retryable, request_id}}
//     with a closed code set. Raw provider response bodies are never
//     forwarded to callers.
//
//   - LLM response IDs are Speko-minted as resp_<request-id>. Provider
//     response and conversation IDs never appear in output; they are
//     captured only as content-free telemetry evidence. There is no
//     previous_response_id anywhere in the contract: callers resend full
//     history, including function results, on every request.
//
//   - Usage split lines are mutually exclusive: cached_input_tokens are not
//     repeated in input_tokens and reasoning_tokens are not repeated in
//     output_tokens, so the splits always sum to the totals. Providers that
//     report no split report all-uncached / all-visible.
//
//   - Idempotency-Key is required on every POST and on every WebSocket
//     upgrade request. The content hash for single-part HTTP bodies covers
//     the raw body bytes as sent; for multipart bodies it covers the decoded
//     part payload bytes only — no part headers, no boundary bytes —
//     concatenated in part order; for WebSocket sessions it covers the exact
//     session.configure text-frame bytes, and later frames are intentionally
//     outside the hash. See idempotency.go for the full contract.
//
//   - Speko-Region names the Speko relay location that served a request. It
//     is a proximity fact, not a provider data-residency guarantee.
package relayapi
