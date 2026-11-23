# Funded Google session renewal

A managed Google realtime session is paid for in short slices. The signed
SessionPlan funds the first slice, and the Gateway asks the control plane for
each next one. This page is the contract for those calls and the rules the
runtime enforces around them. The client code is `controlplane/renewals.go`
and `controlplane/client.go`. The runtime side is `runtime/continuation.go`.

A plan belongs to the funded family when its telemetry token starts with
`gwt1.` or its `reservation.billing.mode` is `direct_entitlement`.

## API

All four calls send `Speko-Protocol-Revision: 3`, refuse redirects, and go
only to the configured HTTPS control-plane origin. For a funded plan they
authenticate with the plan's token (`gwt1.` plus 43 base64url characters),
never with the API key.

| Client method | Request | Purpose |
| --- | --- | --- |
| `RenewEntitlement` | `POST` the plan's `reservation.billing.renewal_url`, which is `/v1/sessions/{session_id}/entitlements/renew` | Fund and mint the next slice |
| `EntitlementClock` | `GET /v1/sessions/{session_id}/entitlements/clock` | Read the control plane's database time |
| `ExchangeFallbackPlan` | `POST /v1/sessions/{session_id}/fallback-plans` | Get a new plan after a provider open failed |
| `RefreshCredential` | `POST /v1/sessions/{session_id}/attempts/{attempt_id}/credentials` | Get a new credential for the same attempt |

### Renewal

A plan permits renewal only when all of these hold: managed credentials,
`provider_direct` route, provider `google`, adapter `google.live.v1`, usage
unit `duration_seconds`, billing mode `direct_entitlement`, currency `USD`,
and `renewable_until` after `lease_expires_at`.

Request: header `Idempotency-Key` (1 to 1024 bytes) and body
`{"previous_expires_at": "<RFC3339>"}`. `previous_expires_at` is not before
the plan's `lease_expires_at` and is before `renewable_until`.

Response (example in
[`testdata/google-entitlement-renewal.json`](testdata/google-entitlement-renewal.json)):

| Field | Rule |
| --- | --- |
| `entitlement_id` | Non-empty |
| `session_id`, `attempt_id`, `reservation_id` | Equal the plan's |
| `sequence` | Positive int64. Each renewal is the previous sequence plus one |
| `previous_expires_at` | Equals the request |
| `lease_expires_at` | Later than `previous_expires_at` by whole seconds, at most 300, and never after the plan's `renewable_until` |
| `authorized_units` | The slice length in seconds |
| `maximum_amount_micros` | Positive int64. The cap for this slice, not a running total |
| `currency` | `USD` |
| `credential` | `kind` `bearer`, non-empty `value`, `expires_at` equal to `lease_expires_at` |

Numeric int64 fields keep their exact values. Only the control plane
computes money.

### Clock

Request headers: the plan token, `Speko-Protocol-Revision: 3` and
`Cache-Control: no-store`. The response is `200` with
`{"server_time": "<RFC3339>"}`, at most 4096 bytes. It reads database time
without funding or minting anything.

The clock URL is derived from the signed renewal URL, whose path must be
exactly `/v1/sessions/{session_id}/entitlements/renew`.

### Fallback and credential reissue

- A funded fallback uses an idempotency key of at most 200 characters. The
  new plan must keep the session ID and change the attempt ID and plan ID.
- A funded fallback cannot take an unrelated prefetched plan in its place.
- One credential-reissue call keeps the same idempotency key across its
  retries. A new call is a new reconnect intent. It retries only
  `plan_not_recorded` and 503, for at most 5 seconds.
- Plans outside the funded family keep using the API key. A funded plan with
  a missing or malformed token is an error and never falls back to the API
  key.

## How the runtime renews

- The control plane decides when a renewal is allowed from its database
  clock (currently the last 30 seconds of the funded slice). The Gateway
  starts asking 15 seconds before the slice ends.
- One renewal uses one key and one body, at most three HTTP attempts and a
  ten-second budget. A lost acknowledgement keeps the previous expiry and
  key. A replayed response that differs is refused.
- A renewal alone does not extend the session. The new slice is adopted only
  after a resumable provider stream has acknowledged setup with the new
  credential.
- The runtime enforces increasing sequence and expiry, the signed
  `renewable_until`, and the cumulative funded audio allowance.
- The old socket is detached at its unchanged lease time even when the HTTP
  response is missing. Recovery cannot reuse that socket, an expired
  credential or a withdrawn resumption handle.

## Rules

Admission:

- `runtime.OpenRequest.Entitlements` is required for a renewable funded
  Google plan, also for embedded callers and custom planners. Without it the
  open fails before provider I/O.
- Such plans need a `ClockedPlanVerifier`. It authenticates the exact JWS,
  issuer and audience and fences the nonce before it reads database time.
  Plan, credential and envelope time checks then use that one sample, so a
  constant host clock offset does not matter.
- An invalid signature or a replay never causes a clock request.
- Funded classification does not depend on renewal eligibility. A funded
  Google plan without a signed renewal URL fails with
  `ErrFundedGoogleClockAuthority` after signature and nonce checks and
  before clock, minting or provider I/O. It never falls back to local-clock
  admission.
- Every Google adapter ID, including `google.live.v1`, must be an installed
  implementation that declares the capability. Wrappers forward it. Others
  fail with `ErrFundedGoogleAdapter` before clock or provider I/O.

Trusted time:

- The Engine adds the full measured round trip to a clock sample, then
  advances it by the greater of wall and monotonic elapsed time.
- When wall and monotonic time diverge, one shared new sample is taken.
  Input and provider-event forwarding pause until it succeeds. Bounded
  queues hold events, and overflow ends the session explicitly.
- The last accepted lease keeps its own hard timer during clock requests.
- A new sample can shorten a deadline. It never extends one.

Replay protection:

- Custom replay stores must implement `FundedReplayCache.TryUseFunded` with
  atomic monotonic-TTL retention and a persisted cutoff of the signed
  expiries they retired. The default memory cache does both. A TTL-only
  `ClockedReplayCache` fails closed before clock I/O.
- A shared nonce cache must update the cutoff and evict atomically across
  consumers. A separate expiring SETNX key is not enough.
- A signed horizon at or below the cutoff fails with `ErrPlanReplay` before
  provider work, so a database clock rollback cannot revive a consumed plan.
  The cutoff may also refuse an unused older plan after a rollback.
- Keeping a nonce for its whole signed lifetime never extends a lease or
  lets the original plan be reopened.
- The Engine and default nonce caches hold 65,536 entries each
  (`MaxReplayEntries` changes the bound). A full cache refuses new admission
  with `ErrReplayCacheFull`. Expired entries free space.
- Ordinary admission keeps a non-decreasing wall-time watermark. A frozen or
  rolled-back clock never drops a live guard, and may refuse new plans until
  their timestamps catch up.

Signing keys:

- JWKS `max-age` and `MaxCacheTTL` are measured from the start of the
  request on the monotonic clock, for session and relay verifiers.
- Only an HTTP 200 or an authoritative 304 starts a new lifetime. Cache
  hits, failed refreshes and clock rollback cannot extend it. An expired key
  is never used after a failed or slow refresh.

Lease and teardown:

- The hard limit of a socket is the earliest of the signed credential
  expiry, the funded lease and the plan's final horizon. It is fixed once
  and bounds the provider handshake, setup and the running session. A later
  lease never replaces an earlier credential expiry.
- Every failure after the nonce fence records one `session.failed`, with
  database-aligned timestamps when available and no invented final usage.
- Interrupting a socket does not prove teardown. Capacity stays held until
  the original close or abort has returned and the provider event source has
  ended.
- A normal close uses the WebSocket close handshake. Peer statuses 1000 and
  1001 are clean. Handshake timeouts, transport errors and other statuses
  are failures, published before the event source closes.

## Not built

- Managed Google conversation resumption. Google's managed token mask pins
  `sessionResumption` to `{}` and overwrites a supplied handle, so the
  adapter reports no resumption and refuses a managed `Resume` before network
  I/O. The Engine therefore mints no further slice for it and closes the
  session at its original funded lease.
