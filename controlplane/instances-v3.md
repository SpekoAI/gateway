# Attributed instance reporting (protocol 3)

The command sends `Speko-Instance-Protocol: 3` and requires the same header plus
204 in the response. It retains the exact original sequence, lifetime, process
counts and capacity rows after a lost acknowledgement. It never downgrades to
protocol 2 or an unversioned request. PUT is limited to 64 KiB; DELETE retains
the three existing lifetime fields. The cell counterpart must support protocol
3 before this command is used with hosted managed routing.

A v3 PUT includes the existing process metadata and a closed `capacity` object:

```json
{"complete":true,"accounts":[{"account":"account/one","capability":"stt_streams","model":"nova-3","active":1,"pending":0}],"byok_active":0,"byok_pending":0,"unknown_active":0,"unknown_pending":0}
```

Every counter is an exact nonnegative int64 JSON number. There are at most 64
unique account/capability/model rows. The active and pending dimensions each
sum to the corresponding process count. Capability is one of `stt_streams`,
`tts_streams`, or `realtime_sessions`. Account ids and model names are bounded
public registry identities, never key aliases or secret values.

The cell can add optional signed `capacity` metadata to a managed SessionPlan:
`{account,capability,model,slots}`. Both normal and credential-digest JWS families
bind it. The engine validates the model, slots and requested capability and
publishes this projection to the local metrics callback only after signature
verification and before physical provider opening. No provider credentials are
passed to that callback. Failed verification never changes occupancy.

The server captures opening and active sessions under the same mutex. The
opening-to-active handoff counts once, fallback updates the verified selected
account, and closure removes actual local occupancy. Reserved server-side plans
remain the cell's separate accounting responsibility. A plan lacking signed
account metadata stays explicitly unknown. BYOK is counted separately and never
uses a Speko account. If there are more than 64 dimensions, overflow stays in
unknown counters rather than disappearing.

`complete` is true exactly when both unknown counters are zero. An incomplete
report must not erase previously proved occupancy in the reader. Lifetime-only
v2 reports remain a separate contract and establish no per-account attribution.
Old Gateways ignore new plan fields; adding this metadata does not give them
ownership-epoch or any other new financial enforcement.

The private cell issuer commits exact account/capability/model leases and its
signed tail alongside funds and request evidence. The paired cell reader now
accepts protocol 3, proves account authority from actual issued signed plans for
that exact organization/process incarnation, and retains immutable observations
with the original signed liveness TTL. Its common capacity allocator takes
MAX(attributed physical sessions, reserved slot weight) per lifetime and model;
covering agent parents count once. Incomplete reports preserve known occupancy,
and exact lost-ACK retries never refresh it. The actual current command passes
local TLS/PostgreSQL pairing for enablement, restart, lost ACK and stale shutdown.

Normal hosted Gateway issuer/event routes, media execution and settlement of
the same work still need their coordinated runtime acceptance. Process reporting
retains the cell's durable `speko:write` authority; execute-only and managed-session
credential integration remains a separate runtime concern. Neither the command
nor the new God switches have been deployed.
