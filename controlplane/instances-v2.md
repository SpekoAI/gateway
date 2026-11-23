# Instance reports: protocol 2

A Gateway that uses a Speko API key reports its process state to the control
plane, so the dashboard can show running Gateways. Protocol 2 adds lifetime
fencing: the control plane can tell one process from its replacement and
ignore late messages from the old one.

The Go client exposes it as `Client.ReportInstanceV2` and
`Client.DeregisterInstanceV2` (`controlplane/instances.go`). The
`speko-gateway` command reports with [protocol 3](instances-v3.md), which
keeps every rule on this page and adds capacity counts.

## API

| Method and path | Body | Purpose |
| --- | --- | --- |
| `PUT /v1/runtime-instances/{instance_id}` | Heartbeat | Report the process |
| `DELETE /v1/runtime-instances/{instance_id}` | Lifetime | End this process lifetime |

Request headers: `Authorization: Bearer <SPEKO_API_KEY>` and
`Speko-Instance-Protocol: 2`.

Success is an empty `204` whose response also carries
`Speko-Instance-Protocol: 2`. Any other 2xx, or a missing header, is an
error. A non-2xx response is returned as an error with its status and
`X-Request-ID`.

Heartbeat body:

| Field | Type | Meaning |
| --- | --- | --- |
| `runtime_name`, `runtime_version` | string | Required |
| `workload_type`, `workload_id` | string | Optional |
| `started_at` | RFC3339 timestamp with nanoseconds | Process start |
| `active_sessions` | integer, 0 or more | Open sessions |
| `pending_sessions` | integer, 0 or more | Sessions being opened |
| `session_capacity` | integer, 1 or more | Session limit of the process |
| `sessions_total` | integer | Sessions opened since start |
| `telemetry_dropped` | integer | Telemetry events dropped |
| `draining` | boolean | The process refuses new sessions |
| `labels` | object of strings | Optional. Never content or secrets |
| `incarnation_id` | lowercase UUID | One per process |
| `sequence` | positive uint32, JSON number | Mutation counter |

Lifetime body (DELETE): exactly `incarnation_id`, `started_at` and
`sequence`.

Bodies are at most 16 KiB. The instance ID is sent as one escaped path
segment, so IDs with slashes, percent signs or Unicode stay distinct.

## Rules

- The client methods never retry and never fall back to an unversioned
  request. A server that does not support the protocol produces an error.
- Create a new incarnation for each process, before any report or warm-up.
- Send mutations one at a time.
- Retry a timed-out mutation with the same sequence and the same body. A
  retry with a new sequence is a new mutation.
- Never wrap or reset the sequence within one incarnation. Keep sequence
  space for the final DELETE.
- A replacement process needs a strictly newer `started_at`.
- Replaying the latest identical mutation does not refresh liveness on the
  server.
- DELETE is terminal. Old mutations cannot revive a lifetime.
- Adoption is one-way per instance ID. After a versioned report, the server
  refuses unversioned PUT and DELETE for that ID. Going back to an old
  reporter needs a different instance ID.
- The server must support the protocol before a Gateway that uses it is
  deployed.
- If a report or DELETE is never acknowledged, the dashboard may stay stale
  until its normal liveness timeout.
- Protocol 2 is lifetime fencing only. It adds no SessionPlan revision, no
  per-plan attribution, no provider-account authority and no capacity
  enforcement. Reports and DELETE release no reservations, money or capacity,
  and a shutdown report is not proof that provider connections have drained.

The unversioned `Client.ReportInstance` and `Client.DeregisterInstance`
remain for callers that have not adopted a versioned protocol.
