# Native realtime review boundaries

The native sidecar forwards one caller-supplied session/model attempt. A local
socket, a new client, a UUID, configuration readiness, or a successful backend
request supplies no fresh admission, provider execution identity, or financial
release proof. The production source/factory/matrix gates remain unchanged.

The accepted PR142/main lineage is merged normally, including its held-word
contract, original capture clock, failed standby refusal and empty STT cleanup.
Go180 grant/allowance/SQL-clock/replay behavior is inherited unchanged.

This continuation fixes four independently reproduced conversation defects:

- Gemini initial history is completed before queued realtime audio is released.
- Extended-thinking Gemini tools declare the required `NON_BLOCKING` behavior.
  This setup compatibility alone does not prove cancellable tool execution.
- Genuine typed user messages reach Gemini before their completion request.
  Context is detached before waits, known native history cannot be rewritten,
  and captured/resampler-held input cannot be overtaken by a typed request.
  A best-effort SDK context error also fences that same physical session.
- Provider interruption reaches actual framework playback, including audio still
  owned by the framework after native `turnComplete`. Exact SDK cleanup is
  distinguished from independent caller cancellation; unresolved local work
  stops the original transport. A local close proves no remote drain or NoIO.

A short source frame can remain entirely inside the native resampler. Its
ownership is recorded before conversion. Clearing a still-local frame replaces
the resampler; it cannot mix the cleared samples into the next commit. An
already-started Gemini input cannot be remotely cleared through a fabricated
control. These behaviors are covered with owned actual SDK/sidecar/PCM controls.

## Explicit implementation and acceptance blockers

Three enabled desired-behavior gates remain on the ordinary test path:

1. **Gemini task cancellation.** A native cancellation names an original call,
   but the supported SDK function stream exposes no portable per-call owned
   cancellation/join handle to this model. The actual AgentSession tool can
   still perform its synthetic effect after cancellation is received. The
   existing healthy Gemini tool contract is preserved; replacing it with a
   blanket refusal is not claimed as an implementation of cancellation.
2. **Scoped GPT task delivery.** The native commentary request has no echoed
   causal binding to a later client delegation. The actual delegated backend
   receives startup/history but not the scoped application ask. Retaining a
   global or next-delegation instruction would donate the ask to unrelated old
   work, so that remembered-instruction workaround is not installed.
3. **Repeated GPT reply ownership.** Two ordinary application asks remain
   callable. Delayed audio from a retired first ask can resolve the second
   reply handle. GPT-Live audio has no per-response/delegation identity or
   response-audio completion marker. Backend completion, a preamble, silence,
   local playback, cancellation or seeing one delegation cannot prove old
   provider work drained. The user explicitly requires repeated
   `generate_reply()` support; a one-ask restriction is not installed.

The minimum missing boundaries are an SDK-owned native-call cancellation and
actual task-join port, and provider-backed per-ask/delegation/audio identity plus
terminal ownership (or an independently accepted per-ask admission/render
path). They are requirements, not new wire fields, authentication endpoints,
issuers or mounted adapters. Constructing another client with the same supplied
credential is not a fresh authority allocation.

The owned peers use synthetic content and actual Go/SDK transport. They prove
local behavior, not provider/factory activation, model funding, execution
admission, worker stop, storage cleanup, settlement or PLAYED. Current reader,
review, CI and integration acceptance remain separately gated.
