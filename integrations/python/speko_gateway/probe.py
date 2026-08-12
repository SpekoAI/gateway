"""Content-free conversation profiler probe for LiveKit agents.

The probe observes one ``AgentSession`` from inside the worker process — the
only vantage point that sees the LLM leg (which never touches the local
Gateway), real playback instants, and all three provider identity namespaces
at once. It mints opaque conversation/turn identifiers, stamps every marker
with a per-probe monotonic clock, and posts small batches to the local
Gateway socket (``POST /v1/turn-events``), which forwards them through its
bounded telemetry exporter.

Privacy invariants, enforced here and re-validated by the Gateway:

- markers carry a closed vocabulary and closed field sets;
- no transcript text, prompts, tool names/args, room names, participant
  identities, or caller IDs ever leave the process through this module;
- everything is optional telemetry: ``SPEKO_TELEMETRY_DISABLED=true`` on the
  Gateway suppresses it entirely.

Operational invariants: the probe never blocks the audio path and never
raises into the agent. Posting is fire-and-forget over a bounded queue; when
the queue is full, markers are counted as dropped and discarded.

Usage::

    probe = ConversationProbe(session)
    probe.start()              # before session.start(), in the entrypoint task
    ...
    await probe.aclose()       # during shutdown

``start()`` registers the probe in a context-variable registry, so the Speko
STT/TTS/LLM streams created afterwards attach their leg identifiers and
request markers automatically.
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import logging
import secrets
import time
from collections import deque
from collections.abc import Callable
from typing import Any

from ._livekit_bridge import _INTEGRATION_VERSION
from .client import GatewayClient

_logger = logging.getLogger("speko_gateway.probe")

_MAX_QUEUED_EVENTS = 1024
_MAX_BATCH_EVENTS = 64
_FLUSH_INTERVAL_SECONDS = 0.25
# turn_<6 digits> is the wire shape; a conversation that somehow exceeds it
# stops opening turns instead of emitting identifiers the Gateway rejects.
_MAX_TURNS = 999_999

_INTEGRATION_NAME = "livekit-python"

# LiveKit CloseReason values -> the profiler's closed end-reason vocabulary.
_END_REASONS = {
    "error": "error",
    "job_shutdown": "shutdown",
    "user_initiated": "shutdown",
    "task_completed": "shutdown",
    "participant_disconnected": "hangup",
}

# Hook markers the integration streams may report, with the only data fields
# each may carry. Anything else is silently ignored: the vocabulary stays
# closed even against a buggy or hostile caller of the registry.
_HOOK_MARKER_FIELDS: dict[str, frozenset[str]] = {
    "llm.requested": frozenset(),
    "llm.first_token": frozenset(),
    "llm.completed": frozenset({"ok"}),
    "tts.requested": frozenset(),
    "tts.first_audio": frozenset(),
}

_active_probe: contextvars.ContextVar[ConversationProbe | None] = (
    contextvars.ContextVar("speko_gateway_active_probe", default=None)
)


def active_probe() -> ConversationProbe | None:
    """Return the probe registered in the current context, if any."""

    return _active_probe.get()


def report_leg(
    kind: str,
    *,
    session_id: str = "",
    attempt_id: str = "",
    request_id: str = "",
    provider: str = "",
    model: str = "",
) -> None:
    """Attach a provider leg to the active probe's open turn; no-op otherwise."""

    probe = _active_probe.get()
    if probe is not None:
        probe.report_leg(
            kind,
            session_id=session_id,
            attempt_id=attempt_id,
            request_id=request_id,
            provider=provider,
            model=model,
        )


def report_marker(marker_type: str, **fields: Any) -> None:
    """Stamp a hook marker on the active probe's open turn; no-op otherwise."""

    probe = _active_probe.get()
    if probe is not None:
        probe.report_marker(marker_type, **fields)


def _never_raises(method: Callable[..., Any]) -> Callable[..., Any]:
    """Suppress every exception: the probe must never fail the audio path."""

    @functools.wraps(method)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return method(*args, **kwargs)
        except Exception:
            _logger.debug("speko probe suppressed an internal error", exc_info=True)
            return None

    return wrapper


class ConversationProbe:
    """Observe one LiveKit ``AgentSession`` and emit content-free turn markers.

    All markers are produced on the session's event loop, so no locking is
    needed; ``mono_ms`` stamps use one per-probe monotonic epoch and are the
    only timestamps the server does arithmetic on.
    """

    def __init__(self, session: Any, *, client: GatewayClient | None = None) -> None:
        self._session = session
        self._client = client or GatewayClient.from_env()
        self._owns_client = client is None
        self._epoch_ns = time.monotonic_ns()
        # Opaque and random: never derived from room, user, or job identity.
        self.conversation_id = "conv_" + secrets.token_hex(16)
        self.dropped_events = 0

        self._seq = 0
        self._turn_index = 0
        self._turn_id: str | None = None
        self._user_speech_open = False
        self._playback_open = False
        self._turn_tool_count = 0
        self._tool_indexes: dict[str, int] = {}
        self._turn_speech_handles: set[Any] = set()

        self._pending: deque[dict[str, Any]] = deque()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._flush_timer: asyncio.TimerHandle | None = None
        self._flush_task: asyncio.Task[None] | None = None

        self._started = False
        self._ended = False
        self._closed = False
        self._registry_token: contextvars.Token[ConversationProbe | None] | None = None
        self._audio_output: Any = None
        self._session_handlers: list[tuple[str, Callable[..., Any]]] = []

    @_never_raises
    def start(self) -> None:
        """Subscribe to the session and open the conversation.

        Call it from the agent entrypoint before ``session.start()`` so tasks
        spawned by the session inherit the context-variable registration.
        """

        if self._started or self._closed:
            return
        self._started = True
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = None
        self._registry_token = _active_probe.set(self)
        self._session_handlers = [
            ("user_state_changed", self._on_user_state_changed),
            ("user_input_transcribed", self._on_user_input_transcribed),
            ("speech_created", self._on_speech_created),
            ("tool_execution_updated", self._on_tool_execution_updated),
            ("close", self._on_close),
        ]
        for event_name, handler in self._session_handlers:
            self._session.on(event_name, handler)
        self._ensure_playback_listeners()
        # The Gateway enriches this marker server-side with workload/instance
        # identity; the probe intentionally cannot supply those fields.
        self._emit(
            "conversation.started",
            {
                "integration": _INTEGRATION_NAME,
                "integration_version": _INTEGRATION_VERSION,
            },
        )

    async def aclose(self) -> None:
        """End the conversation if still open, flush, and release resources."""

        if self._closed:
            return
        self._end_conversation("shutdown")
        self._closed = True
        self._detach()
        if self._registry_token is not None:
            try:
                _active_probe.reset(self._registry_token)
            except ValueError:
                # aclose ran in a different context than start; drop the
                # registration for this context instead.
                _active_probe.set(None)
            self._registry_token = None
        if self._flush_timer is not None:
            self._flush_timer.cancel()
            self._flush_timer = None
        running = self._flush_task
        if running is not None and not running.done():
            try:
                await running
            except Exception:
                _logger.debug("speko probe flush task failed", exc_info=True)
        try:
            await self._flush()
        except Exception:
            _logger.debug("speko probe final flush failed", exc_info=True)
        if self._pending:
            # A failed final post bails with markers still queued; the probe
            # is closed, so account for them instead of leaving the loss
            # invisible.
            self.dropped_events += len(self._pending)
            self._pending.clear()
        if self._owns_client:
            try:
                await self._client.aclose()
            except Exception:
                _logger.debug("speko probe client close failed", exc_info=True)

    # ------------------------------------------------------------------
    # Registry hooks used by the Speko STT/TTS/LLM streams.

    @_never_raises
    def report_leg(
        self,
        kind: str,
        *,
        session_id: str = "",
        attempt_id: str = "",
        request_id: str = "",
        provider: str = "",
        model: str = "",
    ) -> None:
        """Bind a provider leg's identity to the open turn.

        Linkage is explicit, never inferred: STT/TTS legs carry the Gateway
        session/attempt identifiers, the LLM leg carries the relay request ID.
        """

        if self._turn_id is None:
            return
        data: dict[str, Any] = {"kind": kind}
        if kind in ("stt", "tts"):
            if not session_id or not attempt_id:
                return
            data["session_id"] = str(session_id)
            data["attempt_id"] = str(attempt_id)
        elif kind == "llm":
            if not request_id:
                return
            data["request_id"] = str(request_id)
        else:
            return
        if provider:
            data["provider"] = str(provider)
        if model:
            data["model"] = str(model)
        self._emit("leg.attached", data, turn_id=self._turn_id)

    @_never_raises
    def report_marker(self, marker_type: str, **fields: Any) -> None:
        """Stamp one hook marker (llm.*/tts.*) on the open turn, monotonic now."""

        allowed = _HOOK_MARKER_FIELDS.get(marker_type)
        if allowed is None or self._turn_id is None:
            return
        data: dict[str, Any] = {}
        if "ok" in allowed and "ok" in fields:
            data["ok"] = bool(fields["ok"])
        self._emit(marker_type, data, turn_id=self._turn_id)

    # ------------------------------------------------------------------
    # Turn state machine.

    @_never_raises
    def _on_user_state_changed(self, event: Any) -> None:
        new_state = getattr(event, "new_state", "")
        old_state = getattr(event, "old_state", "")
        if new_state == "speaking":
            if self._turn_id is None:
                self._begin_turn("user")
            elif self._playback_open:
                # The user began speaking over agent audio: barge-in. The
                # marker lands in the playing turn so the assembler can anchor
                # barge_in_stop_ms inside one turn.
                self._emit("interrupt.detected", {}, turn_id=self._turn_id)
            if self._turn_id is None:
                return
            self._user_speech_open = True
            self._emit("user.speech.started", {}, turn_id=self._turn_id)
        elif (
            old_state == "speaking"
            and self._user_speech_open
            and self._turn_id is not None
        ):
            self._user_speech_open = False
            self._emit("user.speech.ended", {}, turn_id=self._turn_id)

    @_never_raises
    def _on_user_input_transcribed(self, event: Any) -> None:
        # Only the instant matters; the transcript text is never copied.
        if getattr(event, "is_final", False) and self._turn_id is not None:
            self._emit("user.transcript.final", {}, turn_id=self._turn_id)

    @_never_raises
    def _on_speech_created(self, event: Any) -> None:
        if self._turn_id is None:
            self._begin_turn("agent")
        if self._turn_id is None:
            return
        handle = getattr(event, "speech_handle", None)
        if handle is None or handle in self._turn_speech_handles:
            return
        self._turn_speech_handles.add(handle)
        turn_id = self._turn_id

        def finished(done_handle: Any, turn_id: str = turn_id) -> None:
            self._on_speech_done(done_handle, turn_id)

        try:
            handle.add_done_callback(finished)
        except Exception:
            self._turn_speech_handles.discard(handle)
            _logger.debug("speko probe could not watch a speech handle", exc_info=True)
        # Room IO may attach the audio output after start(); retry lazily.
        self._ensure_playback_listeners()

    @_never_raises
    def _on_speech_done(self, handle: Any, turn_id: str) -> None:
        if self._closed or self._ended or turn_id != self._turn_id:
            return
        self._turn_speech_handles.discard(handle)
        if bool(getattr(handle, "interrupted", False)):
            # SpeechHandle.interrupted means LiveKit resolved the handle's
            # cancel future; the done callback is the public point where that
            # becomes observable.
            self._emit("interrupt.cancel_sent", {}, turn_id=turn_id)
        if not self._turn_speech_handles:
            # The whole assistant reply — including tool-call follow-ups and
            # queued say() segments — has finished (or failed) playing.
            self._complete_turn()

    @_never_raises
    def _on_tool_execution_updated(self, event: Any) -> None:
        if self._turn_id is None:
            return
        update = getattr(event, "update", None)
        update_type = getattr(update, "type", "")
        if update_type == "tool_call_started":
            call = getattr(update, "function_call", None)
            call_id = str(getattr(call, "call_id", ""))
            if not call_id or call_id in self._tool_indexes:
                return
            self._turn_tool_count += 1
            # The 1-based index — never the tool's name — identifies the call.
            self._tool_indexes[call_id] = self._turn_tool_count
            self._emit(
                "tool.started",
                {"tool_index": self._turn_tool_count},
                turn_id=self._turn_id,
            )
        elif update_type == "tool_call_ended":
            call_id = str(getattr(update, "call_id", ""))
            tool_index = self._tool_indexes.get(call_id)
            if tool_index is None:
                return
            self._emit(
                "tool.completed",
                {
                    "tool_index": tool_index,
                    "ok": getattr(update, "status", "") == "done",
                },
                turn_id=self._turn_id,
            )

    @_never_raises
    def _on_playback_started(self, event: Any) -> None:
        if self._turn_id is None or self._playback_open:
            return
        self._playback_open = True
        self._emit("playback.started", {}, turn_id=self._turn_id)

    @_never_raises
    def _on_playback_finished(self, event: Any) -> None:
        if self._turn_id is None:
            return
        interrupted = bool(getattr(event, "interrupted", False))
        # v1 keeps one playback pair per turn: later same-turn segments are
        # ignored unless one reports an interruption, which corrects the pair
        # (the assembler resolves the duplicate by last writer).
        if not self._playback_open and not interrupted:
            return
        self._playback_open = False
        data: dict[str, Any] = {"interrupted": interrupted}
        position = getattr(event, "playback_position", None)
        if isinstance(position, (int, float)) and not isinstance(position, bool):
            data["playback_position_ms"] = max(0, int(position * 1000))
        self._emit("playback.stopped", data, turn_id=self._turn_id)

    @_never_raises
    def _on_close(self, event: Any) -> None:
        reason = getattr(event, "reason", None)
        reason_value = str(getattr(reason, "value", reason))
        self._end_conversation(_END_REASONS.get(reason_value, "unknown"))

    def _begin_turn(self, initiator: str) -> None:
        if self._turn_index >= _MAX_TURNS:
            return
        self._turn_index += 1
        self._turn_id = f"turn_{self._turn_index:06d}"
        self._user_speech_open = False
        self._playback_open = False
        self._turn_tool_count = 0
        self._tool_indexes = {}
        self._turn_speech_handles = set()
        self._emit("turn.started", {"initiator": initiator}, turn_id=self._turn_id)

    def _complete_turn(self) -> None:
        turn_id = self._turn_id
        if turn_id is None:
            return
        self._emit("turn.completed", {}, turn_id=turn_id)
        was_user_speech_open = self._user_speech_open
        self._turn_id = None
        self._user_speech_open = False
        self._playback_open = False
        self._turn_speech_handles = set()
        self._tool_indexes = {}
        self._turn_tool_count = 0
        # A user who interrupted is usually still speaking when the cancelled
        # turn completes; there is no further state transition to observe, so
        # the next turn opens here.
        if (
            was_user_speech_open
            and getattr(self._session, "user_state", "") == "speaking"
        ):
            self._begin_turn("user")
            if self._turn_id is not None:
                self._user_speech_open = True
                self._emit("user.speech.started", {}, turn_id=self._turn_id)

    @_never_raises
    def _end_conversation(self, reason: str) -> None:
        if self._ended or self._closed:
            return
        self._ended = True
        # Open turns deliberately get no turn.completed: the assembler marks
        # them abandoned when conversation.ended arrives.
        self._append(
            "conversation.ended",
            {"reason": reason, "turn_count": self._turn_index},
            turn_id="",
        )
        self._begin_flush()

    # ------------------------------------------------------------------
    # Bounded queue, batching, and fire-and-forget delivery.

    def _mono_ms(self) -> int:
        return (time.monotonic_ns() - self._epoch_ns) // 1_000_000

    def _emit(
        self, marker_type: str, data: dict[str, Any], *, turn_id: str = ""
    ) -> None:
        if self._closed or self._ended:
            return
        self._append(marker_type, data, turn_id=turn_id)

    def _append(self, marker_type: str, data: dict[str, Any], *, turn_id: str) -> None:
        if len(self._pending) >= _MAX_QUEUED_EVENTS:
            self.dropped_events += 1
            return
        self._seq += 1
        event: dict[str, Any] = {
            "type": marker_type,
            "conversation_id": self.conversation_id,
            "seq": self._seq,
            # Wall clock for display only; all turn arithmetic uses mono_ms.
            "created_at_ms": int(time.time() * 1000),
            "data": {"mono_ms": self._mono_ms(), **data},
        }
        if turn_id:
            event["turn_id"] = turn_id
        self._pending.append(event)
        self._schedule_flush()

    def _schedule_flush(self) -> None:
        if self._closed:
            # A deferred re-arm (or a stray timer) can fire after aclose has
            # drained and accounted for everything; scheduling anything now
            # would race the completed shutdown flush.
            return
        if self._loop is None:
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                return
        if len(self._pending) >= _MAX_BATCH_EVENTS:
            self._begin_flush()
        elif self._flush_timer is None:
            self._flush_timer = self._loop.call_later(
                _FLUSH_INTERVAL_SECONDS, self._begin_flush
            )

    @_never_raises
    def _begin_flush(self) -> None:
        if self._flush_timer is not None:
            self._flush_timer.cancel()
            self._flush_timer = None
        if self._closed or self._loop is None or not self._pending:
            # After aclose only its own final flush may run; a timer or
            # deferred callback that outlived the close must not start a task
            # that would race shutdown accounting.
            return
        if self._flush_task is not None and not self._flush_task.done():
            # The running flusher drains everything queued so far.
            return
        self._flush_task = self._loop.create_task(self._flush())

    async def _flush(self) -> None:
        try:
            while self._pending:
                batch_size = min(_MAX_BATCH_EVENTS, len(self._pending))
                batch = [self._pending.popleft() for _ in range(batch_size)]
                try:
                    await self._client.post_turn_events(batch)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # No retry by design: the Gateway exporter owns retry and
                    # deduplication; a failed local post just drops the batch.
                    self.dropped_events += len(batch)
                    _logger.debug("speko probe turn event post failed", exc_info=True)
                    return
        finally:
            # Markers appended while a post was in flight can have consumed
            # the timer through a no-op _begin_flush; if this task then bails
            # on a failed post, nothing is armed and the leftovers would
            # strand until the next append. Re-arm once this task is done —
            # call_soon runs after the task is marked finished, so the next
            # _begin_flush actually starts a new one.
            if self._pending and not self._closed and self._loop is not None:
                self._loop.call_soon(self._schedule_flush)

    def _ensure_playback_listeners(self) -> None:
        if self._audio_output is not None:
            return
        output = getattr(self._session, "output", None)
        audio = getattr(output, "audio", None) if output is not None else None
        if audio is None:
            return
        self._audio_output = audio
        audio.on("playback_started", self._on_playback_started)
        audio.on("playback_finished", self._on_playback_finished)

    def _detach(self) -> None:
        for event_name, handler in self._session_handlers:
            try:
                self._session.off(event_name, handler)
            except Exception:  # noqa: BLE001, S110 - detach must not raise
                pass
        self._session_handlers = []
        if self._audio_output is not None:
            for event_name, handler in (
                ("playback_started", self._on_playback_started),
                ("playback_finished", self._on_playback_finished),
            ):
                try:
                    self._audio_output.off(event_name, handler)
                except Exception:  # noqa: BLE001, S110 - detach must not raise
                    pass
            self._audio_output = None
