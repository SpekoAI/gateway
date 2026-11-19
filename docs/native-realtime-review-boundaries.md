# Native realtime review boundaries

The native sidecar forwards one caller-supplied session/model attempt. A local
socket, a new client, a UUID, configuration readiness, or a successful backend
request supplies no fresh admission, provider execution identity, or financial
release proof. The production source/factory/matrix gates remain unchanged.

The accepted PR142/main lineage is merged normally, including its held-word
contract, original capture clock, failed standby refusal and empty STT cleanup.
Go180 grant/allowance/SQL-clock/replay behavior is inherited unchanged.

This continuation fixes independently reproduced conversation defects:

- Gemini initial history is completed before queued realtime audio is released.
- Extended-thinking Gemini tools declare the required `NON_BLOCKING` behavior.
- Genuine typed user messages reach Gemini before their completion request.
  Context is detached before waits, known native history cannot be rewritten,
  and captured/resampler-held input cannot be overtaken by a typed request.
  A best-effort SDK context error also fences that same physical session.
- Provider interruption reaches actual framework playback, including audio still
  owned by the framework after native `turnComplete`. Exact SDK cleanup is
  distinguished from independent caller cancellation; unresolved local work
  stops the original transport. A local close proves no remote drain or NoIO.
- Native Gemini tool cancellation latches before waits, cancels only exact
  framework tasks and separately joins original callback registration and tasks
  through the private compatibility port described below.
- GPT transport failure physically closes its original socket and closes owned
  command/delegation channels before joining children. A latched error is
  delivered independently of that join; error delivery is not a join receipt.

A short source frame can remain entirely inside the native resampler. Its
ownership is recorded before conversion. Clearing a still-local frame replaces
the resampler; it cannot mix the cleared samples into the next commit. An
already-started Gemini input cannot be remotely cleared through a fabricated
control. These behaviors are covered with owned actual SDK/sidecar/PCM controls.

## Automatic Gemini input completion and clear

Automatic input now retains a private queued/published audio sequence and the
source-sample prefix represented by converted PCM. The first provider turn
observation freezes its published prefix; native `turnComplete` retires only
that prefix. A completed observed voice exchange can then accept a genuine
typed follow-up. Later captured/sent/queued audio and any resampler remainder
stay pending, including a write queued before that first provider observation.
A response terminal does not acknowledge an entire continuous input stream.
When later input cannot be associated with that original turn, it remains
unresolved rather than being silently marked complete.

`clear_audio()` in automatic mode physically fences queued or transmitted
unresolved input, just as manual mode already does. The protocol supplies no
remote buffer-clear acknowledgment to claim. Genuinely still-local conversion
can be reset, and an already completed original prefix does not permanently
poison the session's clear path. Automatic `commit_audio()` can flush local
conversion, but cannot mark its already transmitted input complete by itself.
No new native control, acknowledgment, timer or admission authority is added.

## Exact SDK tool cancellation port

The supported LiveKit versions do not expose a public per-call cancellation
handle to a realtime model. The private port observes only the exact owning
`AgentSession` instance, captured from its genuine model factory call. It accepts
registration only in the actual SDK tool-start callback with the original
`FunctionCall` object and that activity's actual `SpeechHandle`. Completion and
retained-history notifications issue no registration. Task selection requires
both original object identities in actual SDK task metadata; matching IDs,
copied objects or an empty task scan never establish ownership or completion.

The SDK callback runs before creating its actual tool task. The port queues its
registration observation first, after which the SDK installs task metadata
synchronously. Owned SDK controls prove cancellation before the first task
side effect on locked 1.6.8, floor 1.4.5 and retained 1.8.10, including Python
3.10. This relies on those inspected private SDK implementations, not a portable
promise about future versions. It patches one instance callback and never
cancels tasks globally or by provider ID.

Cancellation and closure latch before awaits and request cancellation of known
original tasks synchronously. Registration and actual task completion join
separately within one original one-second cancellation budget. Noncooperative,
missing, copied, late or unowned tasks remain UNKNOWN/unjoined; original task
references and pending registration hooks remain retained. A timer, cancelled
future, transport EOF or session close never establishes task completion.

Queued tool results are filtered inside the actual physical write task, after
its writer lock and task preparation. Other uncancelled results in the same
batch remain. If cancellation observes an already-entered result write, the
transport closes and the result remains UNKNOWN; a local latch cannot retract a
provider result already entering I/O. Previously completed effects or responses
are not undone. Direct low-level function-stream consumers and wrapper factory
paths without the exact activity port cannot claim SDK task cancellation.

This is local SDK task control only. It supplies no execution admission,
provider non-execution, remote NoIO, worker stop, financial release or cleanup
proof. The actual healthy tool rounds, same-ID independent sessions, callback
ordering, held physical writes, copied calls, late callbacks and noncooperative
tasks remain separate controls.

## Explicit implementation and acceptance blockers

Two enabled desired-behavior gates remain on the ordinary test path:

1. **Scoped GPT task delivery.** The native commentary request has no echoed
   causal binding to a later client delegation. The actual delegated backend
   receives startup/history but not the scoped application ask. Retaining a
   global or next-delegation instruction would donate the ask to unrelated old
   work, so that remembered-instruction workaround is not installed.
2. **Repeated GPT reply ownership.** Two ordinary application asks remain
   callable. Delayed audio from a retired first ask can resolve the second
   reply handle. GPT-Live audio has no per-response/delegation identity or
   response-audio completion marker. Backend completion, a preamble, silence,
   local playback, cancellation or seeing one delegation cannot prove old
   provider work drained. The user explicitly requires repeated
   `generate_reply()` support; a one-ask restriction is not installed.

The missing boundary is provider-backed per-ask/delegation/audio identity plus
terminal ownership, or an independently accepted per-ask admission/render path.
This requirement is not a new wire field, authentication endpoint, issuer or
mounted adapter. Constructing another client with the same supplied credential
is not a fresh authority allocation.

The owned peers use synthetic content and actual Go/SDK transport. They prove
local behavior, not provider/factory activation, model funding, execution
admission, worker stop, storage cleanup, settlement or PLAYED. Current reader,
review, CI and integration acceptance remain separately gated.
