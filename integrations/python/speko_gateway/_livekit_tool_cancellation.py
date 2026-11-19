"""Exact SDK task cancellation. No execution, stop, NoIO or funding authority."""

from __future__ import annotations

import asyncio
import inspect
import types
import weakref
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from livekit.agents import llm

from .client import GatewayError
from .relay_realtime import _FRAME_BYTES


def _unavailable() -> GatewayError:
    return GatewayError(
        "Original SDK tool cancellation ownership is unavailable", retryable=False
    )


def capture_activity(model: Any) -> Any:
    from livekit.agents.voice.agent_activity import AgentActivity

    code = inspect.unwrap(AgentActivity._start_session).__code__
    frame = inspect.currentframe()
    selected = None
    try:
        for _ in range(64):
            if frame is None:
                return selected
            if frame.f_code is code:
                owner = frame.f_locals.get("self")
                if not isinstance(owner, AgentActivity) or owner.llm is not model:
                    return None
                if selected is not None and selected is not owner:
                    return None
                selected = owner
            frame = frame.f_back
        return None
    finally:
        del frame


def _callback_codes() -> set[types.CodeType]:
    from livekit.agents.voice.agent_activity import AgentActivity

    found: set[types.CodeType] = set()

    def walk(code: types.CodeType) -> None:
        for value in code.co_consts:
            if isinstance(value, types.CodeType):
                if value.co_name == "_tool_execution_started_cb":
                    found.add(value)
                walk(value)

    for name in ("_realtime_generation_task", "_realtime_generation_task_impl"):
        method = getattr(AgentActivity, name, None)
        if method is not None:
            walk(inspect.unwrap(method).__code__)
    return found


@dataclass(eq=False)
class _OriginalCall:
    call: llm.FunctionCall
    registration: asyncio.Future[None]
    speech: Any = None
    cancelled: bool = False
    registered: bool = False
    unknown: bool = False
    joined: bool = False
    deadline: float | None = None
    tasks: set[asyncio.Task] = field(default_factory=set)
    requested: set[asyncio.Task] = field(default_factory=set)
    publications: set[asyncio.Task] = field(default_factory=set)
    join_task: asyncio.Task[bool] | None = None


class _SessionMux:
    def __init__(self, session: Any) -> None:
        from livekit.agents import AgentSession

        self.session = weakref.ref(session)
        self.original = session._tool_items_added
        if (
            not inspect.ismethod(self.original)
            or self.original.__func__ is not AgentSession._tool_items_added
        ):
            raise _unavailable()
        self.ports: set[SdkToolCancellation] = set()

        def added(items: Any) -> None:
            for port in list(self.ports):
                port._capture(items)
            self.original(items)

        self.observed = added
        session._tool_items_added = added
        session._speko_original_sdk_tools = self

    def remove(self, port: SdkToolCancellation) -> None:
        self.ports.discard(port)
        session = self.session()
        if not self.ports and session is not None:
            if session._tool_items_added is self.observed:
                session._tool_items_added = self.original
            if getattr(session, "_speko_original_sdk_tools", None) is self:
                del session._speko_original_sdk_tools


class SdkToolCancellation:
    """Private compatibility port issued only by the genuine SDK factory stack.

    Cancellation latches before awaits. Registration and actual tasks join
    separately. Missing/copy/noncooperative ownership stays UNKNOWN; no timer or
    empty task scan establishes completion. Unjoined records keep their hook.
    """

    def __init__(self, native: Any, activity: Any) -> None:
        self.native = weakref.ref(native)
        self.activity = weakref.ref(activity) if activity is not None else lambda: None
        self.calls: dict[str, _OriginalCall] = {}
        self.closed = False
        self.bytes = 0
        self.codes = _callback_codes()
        self.mux: _SessionMux | None = None
        if activity is not None:
            session = activity._session
            existing = getattr(session, "_speko_original_sdk_tools", None)
            if existing is not None and (
                not isinstance(existing, _SessionMux)
                or existing.session() is not session
            ):
                return
            self.mux = existing or _SessionMux(session)
            self.mux.ports.add(self)

    def record(self, call: llm.FunctionCall) -> None:
        if self.closed or call.call_id in self.calls:
            raise _unavailable()
        size = len(call.model_dump_json(exclude={"extra"}).encode("utf-8"))
        if self.bytes + size > _FRAME_BYTES:
            raise _unavailable()
        self.bytes += size
        self.calls[call.call_id] = _OriginalCall(
            call, asyncio.get_running_loop().create_future()
        )

    def is_cancelled(self, identity: str) -> bool:
        record = self.calls.get(identity)
        return bool(record is not None and record.cancelled)

    def response_guard(self, responses: list[dict[str, Any]]) -> Callable[[], bool]:
        # IDs select only this native session's already captured original calls.
        # They never identify or cancel an SDK task; exact objects do that.
        if not responses or any(item["id"] not in self.calls for item in responses):
            raise _unavailable()
        original = [(item, self.calls[item["id"]]) for item in responses]

        def before_write() -> bool:
            if self.closed:
                return False
            selected = [
                (item, record) for item, record in original if not record.cancelled
            ]
            responses[:] = [item for item, _ in selected]
            if not selected:
                return False
            task = asyncio.current_task()
            if task is None:
                raise _unavailable()
            for _, record in selected:
                record.publications.add(task)
            task.add_done_callback(lambda _: self._release_if_joined())
            return True

        return before_write

    def _capture(self, items: Any) -> None:
        activity, native = self.activity(), self.native()
        frame = inspect.currentframe()
        try:
            # _capture <- this exact instance mux <- genuine SDK callback.
            caller = frame.f_back.f_back if frame and frame.f_back else None
            # The SDK also reports retained/completed tool items through this
            # instance method. Those notifications issue no task ownership.
            if caller is None or caller.f_code not in self.codes:
                return
            for item in items:
                if not isinstance(item, llm.FunctionCall):
                    continue
                record = self.calls.get(item.call_id)
                if record is None:
                    continue
                exact = item is record.call
                own_activity = (
                    caller is not None and caller.f_locals.get("self") is activity
                )
                if not exact and not own_activity:
                    continue
                if (
                    not exact
                    or caller is None
                    or caller.f_code not in self.codes
                    or not own_activity
                    or activity is None
                    or native is None
                    or activity._rt_session is not native
                    or record.speech is not None
                ):
                    record.unknown = True
                    if native is not None:
                        native._fatal(_unavailable())
                    raise _unavailable()
                record.speech = caller.f_locals.get("speech_handle")
                from livekit.agents.voice.speech_handle import SpeechHandle

                if not isinstance(record.speech, SpeechHandle):
                    record.unknown = True
                    raise _unavailable()
                # Every supported SDK sets its exact task metadata immediately
                # after create_task, before the task's first event-loop step.
                # This callback is enqueued first by its genuine start callback.
                asyncio.get_running_loop().call_soon(self._registered, record)
        finally:
            del frame

    def _collect(self, record: _OriginalCall) -> None:
        from livekit.agents.voice.agent import _get_activity_task_info

        if record.speech is None:
            return
        for task in asyncio.all_tasks():
            if task is asyncio.current_task():
                continue
            info = _get_activity_task_info(task)
            if (
                info is not None
                and info.function_call is record.call
                and info.speech_handle is record.speech
                and task not in record.tasks
            ):
                record.tasks.add(task)
                task.add_done_callback(lambda _: self._release_if_joined())

    def _cancel_tasks(self, record: _OriginalCall) -> None:
        for task in record.tasks:
            if not task.done() and task not in record.requested:
                # Do not inject a second cancellation into SDK/user cleanup.
                cancelling = getattr(task, "cancelling", None)
                if not callable(cancelling) or not cancelling():
                    task.cancel()
                record.requested.add(task)

    def _registered(self, record: _OriginalCall) -> None:
        try:
            self._collect(record)
            record.registered = bool(record.tasks)
            if not record.registered:
                record.unknown = True
            if record.cancelled or self.closed:
                record.cancelled = True
                self._cancel_tasks(record)
        finally:
            if not record.registration.done():
                record.registration.set_result(None)

    async def _join(self, record: _OriginalCall) -> bool:
        loop = asyncio.get_running_loop()
        if not record.registration.done():
            await asyncio.wait(
                {record.registration}, timeout=max(0, record.deadline - loop.time())
            )
        if not record.registration.done() or not record.registered or record.unknown:
            record.unknown = True
            return False
        while True:
            self._collect(record)
            self._cancel_tasks(record)
            live = {task for task in record.tasks if not task.done()}
            if not live:
                record.joined = True
                self._release_if_joined()
                return True
            remaining = max(0, record.deadline - loop.time())
            if not remaining:
                record.unknown = True
                return False
            _, pending = await asyncio.wait(live, timeout=remaining)
            if pending:
                record.unknown = True
                return False

    async def cancel(self, identities: list[str]) -> bool:
        if not identities or any(identity not in self.calls for identity in identities):
            return False
        records = [self.calls[identity] for identity in identities]
        deadline = asyncio.get_running_loop().time() + 1
        # The irreversible negative is visible before any await/publication.
        for record in records:
            record.cancelled = True
            # An already entered write cannot be retracted by a local latch.
            # Close the physical session and retain UNKNOWN, rather than call
            # cancellation a clean provider-result withdrawal.
            if any(not task.done() for task in record.publications):
                record.unknown = True
            if record.deadline is None:
                record.deadline = deadline
        for record in records:
            self._collect(record)
            self._cancel_tasks(record)
        for record in records:
            if record.join_task is None:
                if self.mux is None:
                    record.unknown = True
                    continue
                record.join_task = asyncio.create_task(self._join(record))
        results = []
        for record in records:
            results.append(
                await asyncio.shield(record.join_task)
                if record.join_task is not None
                else False
            )
        return all(results)

    def request_close(self) -> None:
        self.closed = True
        for record in self.calls.values():
            record.cancelled = True
        for record in self.calls.values():
            self._collect(record)
            self._cancel_tasks(record)

    async def aclose(self) -> bool:
        self.request_close()
        joined = await self.cancel(list(self.calls)) if self.calls else True
        self._release_if_joined()
        return joined

    def _release_if_joined(self) -> None:
        if (
            self.closed
            and self.mux is not None
            and all(
                record.registered
                and record.registration.done()
                and all(task.done() for task in record.tasks | record.publications)
                for record in self.calls.values()
            )
        ):
            self.mux.remove(self)
            self.mux = None
