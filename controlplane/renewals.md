# Funded Google renewal

`Client.RenewEntitlement` calls the `renewal_url` in a verified managed Google
realtime SessionPlan. The endpoint must use the configured HTTPS cell origin.
It sends the plan's `gwt1.` token, protocol revision 3, an explicit idempotency
key, and only `previous_expires_at`. Redirects are refused.

The cell decides the renewal window from its database clock (currently the
last 30 seconds of the funded slice). A lost acknowledgment must retain the
previous expiry and key. The response binds session, attempt, reservation and
previous expiry. Its new horizon is at most 300 seconds later and never beyond
the original signed `renewable_until`. Numeric int64 values retain exact values;
only the cell calculates money. `maximum_amount_micros` is this slice's cap.

`Client.EntitlementClock` uses the plan-token-authenticated, revision-3 GET
`/v1/sessions/:session_id/entitlements/clock`. It refuses redirects and changed
URL identities. The response `{server_time}` samples SQL time without funding
or minting. The Engine adds the full measured round trip, then advances the
sample using the greater of wall and monotonic elapsed time. Constant local
clock offsets cancel. Wall/monotonic divergence triggers one shared new sample;
input and provider-event forwarding pause until it succeeds. Existing bounded
queues retain events; overflow terminates explicitly. The last accepted lease
has an independent hard timer throughout clock requests.

`runtime.OpenRequest.Entitlements` wires this client into the Engine. The
Gateway and mixed credential-source planner pass it through automatically for
renewable Google roots. The Engine retains its original verified plan,
attempt, reservation, capacity attribution and receipt destination. Its
continuation coordinator can adopt a slice only after a resumable stream has
acknowledged the new setup. It enforces increasing sequence/expiry, the signed
final horizon, and cumulative funded PCM allowance. A pending renewal uses one
key/body, at most three HTTP attempts and a ten-second recovery budget. The
old socket is detached at its unchanged lease timestamp even when an HTTP
response is missing. Recovery cannot reuse that socket, expired credentials,
or a withdrawn resumption handle. Closing cancels pending delivery; a live
socket can drain queued input, and a detached socket terminates immediately.
Provider teardown still precedes physical capacity release.

**Managed Google conversation resumption remains refused.** Google's current
managed token mask pins `sessionResumption` to `{}`; it overwrites a supplied
handle. Removing that mask would permit resumptions outside `uses:1`, without
an accepted provider-enforced concurrency/cost bound. The Google adapter reports
no usable managed resumption capability and rejects managed `Resume` before
network I/O. Consequently the Engine does not automatically mint another slice
for that adapter and closes it at its original funded lease. A handle-free HTTP
renewal remains a funded credential boundary, not conversation resumption.
No token mask, provider constraints or financial policy is changed here.

Integration tests use the real Engine, stock JWS verifier, public Go client and
owned HTTP/WebSocket transports. The continuation fixture uses customer-owned
socket semantics to exercise adoption, response loss, setup failure and
cancellation; it is not managed Google provider acceptance. Separate tests run
the actual managed adapter and prove refusal/no mint, SQL-aligned hard expiry,
clock-refresh input/output fencing and independent expiry during a held clock
request. The representative renewal fixture remains
`testdata/google-entitlement-renewal.json`. SQL-backed issuance/mint/settlement
acceptance belongs to the paired platform suite.

Funded fallback and credential reissue use the issued `gwt1.` token, revision 3,
no redirects and bounded idempotency keys. A refresh invocation keeps the same
key across retries; a new invocation starts a new reconnect intent. A funded
fallback cannot consume an unrelated warm plan to bypass the parent lifecycle.
The existing OSS setup-key family remains supported for its existing plans;
a malformed or missing funded token cannot fall into it. Hosted recovery still
requires the platform's durable predecessor/constraint lifecycle. Managed
ElevenLabs reissue remains refused because its provider token lacks a proven
work/cost bound. This Gateway change creates no native stored-BYOK registry or
reference family; process-local BYOK credentials retain their existing path.
