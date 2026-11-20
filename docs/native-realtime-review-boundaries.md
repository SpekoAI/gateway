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

The first provider response cannot identify the input it consumed. On the
previous `6dc1f2d` candidate an actual peer received A and then B before releasing
A's first transcript/model output and terminal. That falsely retired B and let
both a typed context and a clear proceed. Both desired security assertions
reproduced RED; a first-event snapshot is not an acknowledgment.

The adapter now accepts only a genuine native `voiceActivity` `ACTIVITY_END`
with a valid `audioOffset`. The offset is measured against the original physical
16 kHz audio stream. It can retire only a monotonic, already-published converted
prefix. Missing, malformed, negative, regressed, replayed, unsent or out-of-range
values cannot retire input. Fractional samples round down; source input retained
inside the resampler remains pending. Local clear preserves the lifetime stream
cursor and starts a new local conversion segment, so a previous offset cannot
be donated to a later source segment. Native audio offsets are not local clocks
or current-capture counters.

The [native API reference](https://aistudio.google.com/api-reference/live) defines
that stream offset separately from model-turn completion. Input transcription is
independently ordered; its text, optional end offset or a model `turnComplete`
is not used as whole-input acknowledgment. `interactionStatus` is likewise not
a sample boundary. No new wire field, native control or provider marker is
invented, and no provider/model support for optional events is assumed.

Actual SDK voice-to-typed positives pass when the owned native peer supplies the
original `ACTIVITY_END` offset. **The two original unannotated desired R1
positives remain enabled and RED.** Eventless compatibility is unresolved;
these positives were not rewritten to add the missing evidence. This partial
implementation does not close R1 or establish production availability.

`clear_audio()` physically fences unresolved queued or transmitted input. There
is no fabricated remote buffer-clear acknowledgment. Genuinely still-local
conversion can be reset. Automatic `commit_audio()` flushes local conversion but
cannot acknowledge transmitted input by itself; the adapter sends no automatic
`audioStreamEnd` or activity reset. Reopening an audio stream must not reset its
original socket lifetime offset.

LiveKit1.4.5 normalizes an AgentSession manual-mode request away when the model
advertises server turn detection. A stronger actual SDK commit diagnostic showed
that forcing native manual mode from the original requested setting left that
pipeline inconsistent, so that production experiment was restored. The new
floor manual-clear case uses the supported public
`model.session(turn_detection_disabled=True)` factory; modern lanes retain the
actual AgentSession manual path. No original uploaded fixture/window/assertion or capability
skip was changed.

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

Application asks now use separate physical native operations, while the primary
transport continues ordinary caller/tool flow. Each genuine `generate_reply()`
invocation captures its immutable context, scoped instructions, selected tools
and original setup deadline before any await. A constructor-owned one-shot
factory opens one new **unfunded** request using the same selected GPT model,
voice and backend. The relay must independently authenticate, authorize, fund
and cap that request. Reusable API credentials do not reuse the old grant;
financial-request or otherwise ineligible credentials can refuse the new open.
There is no reconnect, old-attempt replay, provider substitution or one-ask rule.

The private compatibility port binds the actual synchronous AgentSession call's
original SpeechHandle, and later accepts generation/tool observations only from
the inspected original SDK Activity code frame and exact original objects.
A dedicated stream's first generation resolves only its own Future. Its later
voice cannot resolve a newer application's Future. Native startup/history can
delegate before commentary; immutable scoped input is installed before opening,
and an already-observed startup delegation suppresses the redundant request.
Backend/tool rounds use that original owner's context and selected tools.
Identical provider delegation/call IDs on different streams establish no shared
ownership. Exact SDK tool tasks and callback registration join separately.

No local silence, first generation, Future resolution, SDK playback or later ask
proves the physical provider response ended. Streams remain retained until their
original cancellation, actual root/session close or server-enforced expiry.
Public AgentSession interruption closes and joins the current application owners
without retiring the primary conversation; a later genuine ask can independently
request new admission. This does not invent a native cancellation control for
ordinary primary audio. Actual session/model close remains the root boundary.
The original setup deadline bounds queue waits, physical open and first
generation; it is never renewed after waiting. After generation begins, provider
authority and expiry remain the server's responsibility. Original-operation
metadata is bounded by 128 retained operations and a shared 2 MiB captured
context limit; generation identity retention is bounded by 128 per stream.
These local resource limits confer no paid capacity or root lifetime.

Actual forwarded/heard SDK assistant segments are copied into the primary's
ordinary conversation history before its next microphone frame. Private scoped
instructions and unheard native transcript fragments are never copied there.
This observation does not close the dedicated physical stream or establish a
whole-answer terminal. A noncooperative tool remains retained UNKNOWN/unjoined;
root closure never turns it into a clean stop or NoIO receipt. Direct low-level
consumers without exact SDK object membership cannot claim that cancellation
port. The caller must close the actual AgentSession/models before their clients.

The two original GPT scoped/late-voice assertions and original time windows are
preserved. Only their owned-peer fixture was generalized to accept genuine
requests over separate primary/per-ask sockets. Both assertions still reproduce
semantic RED on archived exact `03f819c` production, and pass this composition.
Additional actual-SDK controls preserve queued and late PCM playback, scoped
tool/history ownership, ordinary microphone flow, pre-open cancellation and
canonical refusal without retry. The two eventless Gemini R1 positives described
above remain enabled RED and are a separate acceptance blocker.

The owned peers use synthetic content and actual Go/SDK transport. They prove
local behavior, not provider/factory activation, model funding, execution
admission, worker stop, storage cleanup, settlement or PLAYED. Current reader,
review, CI and integration acceptance remain separately gated.
