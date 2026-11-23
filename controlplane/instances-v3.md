# Instance reports: protocol 3

Protocol 3 is [protocol 2](instances-v2.md) plus a `capacity` object that
says how many sessions the process holds per provider account, capability
and model. The `speko-gateway` command uses it whenever `SPEKO_API_KEY` is
set. The Go client methods are `Client.ReportInstanceV3` and
`Client.DeregisterInstanceV3`.

## API

| Method and path | Body |
| --- | --- |
| `PUT /v1/runtime-instances/{instance_id}` | The protocol-2 heartbeat plus `capacity`. At most 64 KiB |
| `DELETE /v1/runtime-instances/{instance_id}` | The three protocol-2 lifetime fields |

Requests send `Speko-Instance-Protocol: 3`. Success is an empty `204` whose
response carries the same header. The command never downgrades to protocol 2
or to an unversioned request.

## The `capacity` object

```json
{"complete":true,"accounts":[{"account":"account/one","capability":"stt_streams","model":"nova-3","active":1,"pending":0}],"byok_active":0,"byok_pending":0,"unknown_active":0,"unknown_pending":0}
```

| Field | Rule |
| --- | --- |
| `accounts` | Required array, at most 64 rows, unique by `account`, `capability` and `model` |
| `accounts[].account` | 1 to 200 characters: a letter or digit, then letters, digits or `._:/-`. A public registry name, never a key alias or secret |
| `accounts[].capability` | `stt_streams`, `tts_streams` or `realtime_sessions` |
| `accounts[].model` | 1 to 256 bytes, no surrounding whitespace, no control characters |
| `accounts[].active`, `accounts[].pending` | Non-negative. Not both zero |
| `byok_active`, `byok_pending` | Sessions on the customer's own keys |
| `unknown_active`, `unknown_pending` | Sessions whose plan carries no signed account |
| `complete` | `true` exactly when both unknown counters are zero |

- Every counter is an exact non-negative int64 JSON number.
- The active counters sum to `active_sessions` and the pending counters sum
  to `pending_sessions`.
- When there are more than 64 rows, the overflow is added to the unknown
  counters.

## Where the account comes from

A managed SessionPlan can carry optional signed `capacity` metadata:
`{account, capability, model, slots}`, with `slots` positive. Both
session-plan JWS families bind it.

- The engine checks that `capability` matches the session kind, and counts
  the session only after the plan's signature is verified and before the
  provider connection opens.
- A failed verification never changes the counts.
- A plan without signed account metadata is counted as unknown.
- BYOK sessions are counted separately and never under a Speko account.
- No provider credential is passed to the counting code.

## How the command reports

- It creates one random incarnation and start instant before warm-up.
- It reports once at start, then every `SPEKO_INSTANCE_HEARTBEAT_INTERVAL`
  (20 s by default).
- A report that was not acknowledged keeps its exact sequence and body. The
  next report first resends it, then reports current counts.
- Each send makes at most two attempts inside a five-second budget. It
  retries only transport errors, 429 and 5xx.
- On a signal, a listener failure or sequence exhaustion, it stops the
  periodic loop, sends a final draining report, waits up to 30 seconds for
  sessions to end, and sends DELETE.
- The last uint32 sequence is kept for DELETE. When the sequence runs out,
  the process drains instead of wrapping.
- DELETE is final locally even if its acknowledgement is lost. Every retry
  uses the same body.
- Report failures are logged without response details.

## Rules

- The control plane must support protocol 3 before this command is used with
  managed routing.
- The API key that reports needs the `speko:write` scope.
- An incomplete report must not erase occupancy the reader has already
  proved. An exact retry never refreshes it.
- The counts are sessions, not plan slot weights. Plans the control plane
  has reserved but the process has not opened are the control plane's own
  accounting.
- Old Gateways ignore new plan fields. Adding `capacity` to a plan gives
  them no new enforcement.
- Reports grant no execution authority and release no reservations.
