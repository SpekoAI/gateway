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

`runtime.OpenRequest.Entitlements` is required for renewable funded Google
roots, including embedded callers and custom planners. Missing authority fails
before provider I/O. The Gateway and mixed credential-source planner pass the
client through automatically. Such roots also require `ClockedPlanVerifier`:
the stock verifier authenticates the exact JWS, issuer and audience and fences
its nonce before sampling SQL time. Plan, credential and envelope time checks
then use the same trusted sample, so a short three-second slice works at a
constant host offset of plus or minus ten seconds. Invalid signatures and
replays cannot cause clock requests. Custom replay stores must implement
`FundedReplayCache.TryUseFunded` with atomic monotonic-TTL retention and a
persisted signed-expiry retirement cutoff; the default memory cache provides
both. TTL-only `ClockedReplayCache` implementations fail closed before clock
I/O. Retaining the nonce for its full signed issuance lifetime cannot extend
the lease or allow the original plan to be reopened.
The mixed BYOK/managed CLI verifier preserves this capability.

Funded Google classification is independent of renewal eligibility. A managed
`direct_entitlement` root (or the existing funded token family) cannot fall into
ordinary local-clock admission merely because its RenewalURL is absent. The
current clock port derives authority from that signed URL; unsupported
nonrenewable shapes receive `ErrFundedGoogleClockAuthority` after signature and
nonce authentication, before clock, minting or provider I/O. No unsigned clock
endpoint or allowance is invented. Existing ordinary OSS families stay separate.
Every Google adapter ID, including `google.live.v1`, requires the installed
implementation's explicit capability. Wrappers forward that actual capability
and refuse when it is absent or false; no default-ID exemption exists. A custom
adapter cannot gain funded acceptance by matching a string. Unrecognized funded
Google adapters receive `ErrFundedGoogleAdapter` after authentication and before
clock or provider I/O.

Ordinary local/signature-only admission and the default verifier retain a
nondecreasing wall-time watermark. Spent local attempts remain fenced while
their plans can still validate; once expiry permits eviction, a later backward
clock change cannot revive them. A frozen/rolled-back clock never authorizes
dropping a live guard. Engine and default nonce caches use fixed-size digest
keys, default to 65,536 entries each, and refuse new admission with
`ErrReplayCacheFull` when full. `MaxReplayEntries` can set each bound. Expired
entries free space; the admission watermark remains. Clock rollback can
conservatively refuse new plans until their timestamps catch up. SQL-clock
funded admission retains its separate trusted-time and nonce contract.

Funded nonce and Engine-attempt eviction retain a nondecreasing cutoff of the
signed expiries they retired. A later SQL rollback cannot revive a forgotten
consumed plan: horizons at/below that cutoff receive `ErrPlanReplay` before new
provider work. Each cache keeps its configured bounded digest map and one cutoff
timestamp; eviction reclaims space for distinct newly signed later horizons.
The cutoff can conservatively refuse an unused older horizon after rollback;
it does not clamp the authoritative SQL sample, renew an attempt, or retry a
spent root. Shared nonce caches must update/persist cutoff and eviction
atomically across consumers; a separate expiring SETNX key is insufficient.

Signing-key freshness has a separate elapsed-time deadline for both session
and relay verifiers. JWKS `max-age` and `MaxCacheTTL` are measured from request
start using the process monotonic clock, including retrieval/decoding time.
Only a successful HTTP 200 or authoritative 304 revalidation starts a new
bounded lifetime. Cache hits, failed refreshes and admission clock rollback
cannot extend it; an expired key is never used after a failed/slow refresh.

The accepted lease deadline bounds the provider's handshake and setup context,
as well as the running session timer. Partial Google opens return their stream
for teardown, and physical capacity remains held until abort/close returns and
the event source ends. Every failure after the verified nonce fence records one
`session.failed`, using trusted SQL-aligned timestamps when available, with no
fabricated usage finality. Normal close uses the WebSocket close handshake and
drains the provider event bridge; an independent lease expiration or explicit
abort can still close the upgraded physical transport while that handshake is
pending. The Engine retains its original verified plan,
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

Setup/write/cancellation errors return the acquired stream and original error
without waiting for physical Close returns. The sole reader and serialized
teardown retain that exact stream; the Engine publishes its opening failure
receipt promptly and releases capacity only after both actual joins complete.

The root cap is the earliest original signed credential expiry, funded lease
and root horizon, mapped once from trusted SQL time to monotonic elapsed time.
A still-valid shorter credential is admitted normally and may carry I/O only
until that original cap. The Engine passes the derived cap to the Google
upgraded transport, whose independent owner survives setupComplete and a held
adapter Open return. It does not depend on caller/setup-context cancellation
after handoff. A late Open return fails admission and retains capacity until
its actual abort/event draining join; it never creates a replacement deadline.
The original absolute SQL credential/lease/root minimum is retained alongside
its monotonic cap. Fresh trusted samples may tighten both the independent
physical timer and I/O/readiness fence; a backward sample cannot extend either.
The later SQL lease never replaces an earlier credential horizon. Replacement
adoption similarly tightens its original cap after any fresh SQL sample without
resetting the lifetime or reusing the root credential's horizon.

The pending replacement retains that same absolute horizon and monotonic
minimum before `Resume` can block. A fresh accepted SQL sample tightens its
temporary timer and its actual physical owner even while setup or the original
`Resume` return is held. The read-only context accessor exposes only this
original, possibly shortened cap; it cannot mint a new horizon. The provider
lock wait itself can cross a completed SQL refresh or clock divergence. After
that wait, the coordinator joins any required refresh and reconciles the latest
trusted time with the original cap under the replacement-registration lock.
Already expired replacements refuse before provider setup; backward SQL never
extends the original elapsed deadline. The provider
registers one pending socket owner before dialing and attaches the upgraded
transport before handshake/setup. Tightening before acquisition is retained
and applied to that exact transport; a later deadline cannot extend it.
Detach/interrupt reaches the pending owner as well as the predecessor, while
adoption and old expiry callbacks remain bound to their own generation.

Owned TLS tests independently hold the tightening method's return, preventing
the coordinator from completing its timer path. The pending physical timer
still closes the actual socket under the original bound. Terminal failure
metadata does not await a held transport Close return, but capacity does: both
the original Resume continuation and actual physical teardown must join.
These are owned-transport controls; managed Google resumption remains refused.

Before a resumed socket can open, the coordinator anchors its validated
credential/lease/root horizon to monotonic elapsed time. An independent owner
keeps that exact cap armed during Resume, blocked clock synchronization and
adoption. Successful canonical adoption installs its timer before retiring the
temporary owner; late setup or resampling cannot restart the socket lifetime.
A return after expiry cannot adopt sequence or allowance. Failure/cancellation
retains the deadline until physical teardown actually joins, and capacity
remains held while that evidence is unknown. This machinery is exercised with
owned resumable transports; it does not enable managed Google resumption.
The coordinator carries its already validated cap separately from the shorter
setup/request cancellation deadline. The adapter arms it on the actual pending
replacement transport before setup blocks, even before `s.current` adoption.
It never copies the old root cap or maps signed time through the host wall clock.
Every replacement binds expiry to the stable stream's exact-socket detachment
before setup. Expiry detaches that predecessor before its event source can end,
so a valid pending next slice may recover through its original same-key intent.
The callback fences an expired pending stream under the adoption lock too;
late callbacks cannot detach a newer socket or make expired setup adoptable.

Terminal abort claims its error before calling the Google stream's concurrent
`Interrupt`, which closes physical I/O without waiting for the provider mutex,
an in-progress close handshake, or public event consumption. Pending input,
receipt publication and serialized `Abort`/`Close` joins happen afterward.
Interrupting a socket never proves teardown complete: capacity remains held
until the original close/abort and the provider event source have both ended.

Interruption starts bounded physical close attempts without waiting for any
transport Close return. A stalled normal Close cannot block abort/lease terminal
metadata. At most two physical attempts run concurrently; an unused normal slot
or a completed failed slot can be used while the prior interrupt is held. This
also covers cancellation during invalid-upgrade diagnostic reads, before the
SDK has called normal Close. A failed attempt preserves its diagnostic but
does not latch future interruption.
If expiry queues another interruption while both slots are occupied, either
failed close completion can consume that retry when it frees a slot. The normal
slot need not finish before expiry, and a held interrupt return cannot strand it.
The original cap remains armed while closure is unproven. Serialized teardown
waits for actual successful/already-closed transport evidence and every active
attempt to join before releasing capacity. Detach is prompt; replacement setup
joins its exact predecessor first, and late callbacks still address only their
original socket.

A failed graceful Google close publishes its terminal provider failure before
cancelling the reader or closing its event source. The stable bridge preserves
that outcome even if the Engine's serialized `Close` return is delayed. Event
closure cannot race the failed return into `session.closed`; earlier abort or
lease failure still wins, and a successful normal handshake remains clean.
Both explicit normal peer statuses, 1000 and 1001, are clean in the terminal
outcome and the Close return. Handshake timeouts, transport errors and abnormal
protocol statuses remain failures with the same publication ordering.
The upgraded transport records its own first Close result. A normal peer1001
can mask that result inside the WebSocket SDK, so the adapter combines the
observed transport failure with the normalized handshake result before terminal
publication. The peer-initiated normal-close reader path observes that result
too. Clean1001 remains clean; an accompanying transport failure cannot
be erased by its status. No SDK patch or error-text inference is involved.

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
