# Opt-in process lifetime reports

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

Deploy the paired platform reader before opting in. Unversioned methods and the
current command-line reporter remain unchanged. Opt-in is one-way per identity;
returning to an old reporter requires a different identity. These client methods
do not implement the command-line process manager or automatically negotiate it.

Protocol 2 establishes lifetime fencing only. It adds no SessionPlan revision,
per-plan attribution, provider-account authority or direct-capacity enforcement.
Reports and DELETE release no reservations, money or capacity. Those features
require their separate reviewed contracts and issuer/runtime integration.
