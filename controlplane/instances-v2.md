# Process lifetime reports

`ReportInstanceV2` and `DeregisterInstanceV2` use the existing instance URL with
`Speko-Instance-Protocol: 2`. Success requires an empty 204 and an explicit
protocol-2 response header. Unsupported servers and missing acknowledgements
return errors; neither method retries or falls back to unversioned reporting.
HTTP failures preserve the existing status and request ID error.

PUT embeds the existing heartbeat fields and adds a canonical lowercase UUID
`incarnation_id` and positive numeric uint32 `sequence`. DELETE sends exactly
those lifetime fields plus `started_at`. Timestamps retain RFC3339 nanoseconds;
the existing one-escape URL boundary preserves slash, percent and Unicode IDs.
Versioned request bodies are bounded at 16 KiB.

The caller generates a new incarnation per process, before any reporting or
warm-up, and serializes mutations. Retry a timed-out mutation with the same
sequence and body. Reserve sequence space for the terminal DELETE; never wrap
or reset a sequence in one incarnation. A replacement needs a strictly newer
start instant. Latest identical replay does not refresh server liveness. DELETE
is terminal, old mutations cannot revive a lifetime, and an upgraded identity
refuses unversioned PUT/DELETE. Retrying with a new sequence is a new mutation.

The hosted command-line process uses these versioned methods automatically.
It creates one random incarnation and start instant before warm-up, serializes
periodic and final reports, and retains a failed report's exact sequence/body
until acknowledged. Each retained mutation makes at most two attempts per call within the shared
five-second budget. A later report first retries pending evidence, then reports
current statistics. Labels are detached when the mutation is prepared.

Signal shutdown, listener failure and sequence exhaustion stop and join the
periodic loop before a final draining report and lifetime-bound DELETE. DELETE
is locally terminal even if its acknowledgement is lost; every retry uses the
same body. The last uint32 sequence is reserved for DELETE. Exhaustion drains
the process instead of wrapping or resetting. Background warm-up is cancelled and joined
on every serving exit. Reporting failures remain content-free availability
messages; they do not grant execution authority or expose response diagnostics.

Deploy the paired platform reader before upgrading the command. Unsupported
servers or missing acknowledgements cannot cause legacy fallback. The legacy
library methods remain available for explicit existing callers. Adoption is
one-way per identity; returning to an old reporter requires a different identity.
If registration/terminal delivery cannot be acknowledged, dashboard state may
remain stale until its ordinary liveness timeout; shutdown reports are not
provider-drain evidence. Parent acceptance must pin this producer and exercise
the actual command against the real Effect handler/PostgreSQL, including
replacement and delayed old-process DELETE.

Protocol 2 establishes lifetime fencing only. It adds no SessionPlan revision,
per-plan attribution, provider-account authority or direct-capacity enforcement.
Reports and DELETE release no reservations, money or capacity. Those features
require their separate reviewed contracts and issuer/runtime integration.
