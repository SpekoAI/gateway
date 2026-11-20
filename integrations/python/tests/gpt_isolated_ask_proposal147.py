"""TEST-only composition. Root bounds/admission factory are NOT production issuers.

No existing production adapter is installed or replaced by this module. The
private AgentSession hook below is an executable proposal, not SDK public API.
Tools/results/cancellation need their separate exact-owner ports before mounting.
"""

from __future__ import annotations

import asyncio
import dataclasses
import inspect
import json
import time
from typing import Any

from livekit.agents import NOT_GIVEN, AgentSession, llm
from livekit.agents.utils import is_given

from speko_gateway._livekit_context import snapshot_context
from speko_gateway._livekit_gpt_live import _GPTLiveDuplexModel


@dataclasses.dataclass(frozen=True)
class OriginalAsk:
    instructions: str
    context_json: str
    captured_monotonic: float
    original_deadline: float
    tools: tuple[Any, ...]


@dataclasses.dataclass(eq=False)
class RetainedOwner:
    original: OriginalAsk
    speech: Any
    future: asyncio.Future | None = None
    client: Any = None
    adapter: Any = None
    inner: Any = None
    startup: asyncio.Task | None = None
    generations: list[Any] = dataclasses.field(default_factory=list)
    speech_members: list[Any] = dataclasses.field(default_factory=list)
    closed: bool = False
    calls: dict[str, Any] = dataclasses.field(default_factory=dict)
    close_task: asyncio.Task | None = None
    registrations: dict[str, asyncio.Future] = dataclasses.field(default_factory=dict)
    tasks: set[asyncio.Task] = dataclasses.field(default_factory=set)
    task_requests: set[asyncio.Task] = dataclasses.field(default_factory=set)
    joined: bool = False
    close_deadline: float | None = None


class TestRoot:
    """Fixture-only lifetime/capacity; no SQL grant or production root proof."""

    __test__ = False

    def __init__(self, *, deadline: float, maximum: int):
        self.original_deadline = deadline
        self.maximum = maximum
        self.closed = False
        self.owners: list[RetainedOwner] = []
        self.expiry: asyncio.TimerHandle | None = None
        self.close_task: asyncio.Task | None = None

    def require_open(self):
        if self.closed or time.monotonic() >= self.original_deadline:
            raise llm.RealtimeError("Original TEST root is closed or expired")


class IsolatedAskModel(llm.RealtimeModel):
    """Proposed same-model composition; every authority here is explicitly TEST."""

    def __init__(self, primary_model, backend, open_original, root: TestRoot):
        super().__init__(capabilities=primary_model.capabilities)
        self.primary_model, self.backend = (
            llm.DuplexRealtimeAdapter(primary_model.duplex_model),
            backend,
        )
        self.open_original, self.root = open_original, root
        self.proxy = None
        self.agent = None
        self.forwarding_owner = None
        self.original_generate = None
        self.primary_speeches = []

    def bind_actual_agent(self, agent: AgentSession):
        assert self.agent is None
        self.agent = agent
        original = agent.generate_reply
        assert original.__func__ is AgentSession.generate_reply
        self.original_generate = original

        def generate(**options):
            self.root.require_open()
            if len(self.root.owners) >= self.root.maximum:
                raise llm.RealtimeError("TEST retained-operation capacity exhausted")
            instructions = options.get("instructions", NOT_GIVEN)
            if not is_given(instructions):
                instructions = ""
            if not isinstance(instructions, str) or options.get("user_input"):
                raise llm.RealtimeError("TEST port covers string instructions only")
            if is_given(options.get("tools", NOT_GIVEN)):
                raise llm.RealtimeError("Exact original tool-owner port is unavailable")
            # Capture BEFORE the actual synchronous SDK call creates/schedules
            # its SpeechHandle/task. No await or reconstruction from public IDs.
            context = snapshot_context(agent.current_agent.chat_ctx)
            original_ask = OriginalAsk(
                instructions=instructions,
                context_json=json.dumps(context.to_dict(exclude_config_update=True)),
                captured_monotonic=time.monotonic(),
                original_deadline=self.root.original_deadline,
                tools=tuple(agent._activity.tools),
            )
            owner = RetainedOwner(original_ask, None)
            self.root.owners.append(owner)
            try:
                speech = original(**options)
            except BaseException:
                owner.closed = True
                raise
            owner.speech = speech
            owner.speech_members.append(speech)
            return speech

        agent.generate_reply = generate

        original_interrupt = agent.interrupt
        assert original_interrupt.__func__ is AgentSession.interrupt

        def interrupt(**options):
            if self.proxy is not None:
                self.proxy.request_close()
            local = original_interrupt(**options)

            async def joined():
                await local
                if self.proxy is not None:
                    await self.proxy.aclose()

            return asyncio.create_task(joined())

        agent.interrupt = interrupt

        def created(event):
            owner = self.forwarding_owner
            if owner is not None:
                owner.speech_members.append(event.speech_handle)
            elif not event.user_initiated:
                self.primary_speeches.append(event.speech_handle)

        agent.on("speech_created", created)

        from livekit.agents.voice.agent import _get_activity_task_info

        from speko_gateway._livekit_tool_cancellation import _callback_codes

        original_items = agent._tool_items_added
        assert original_items.__func__ is AgentSession._tool_items_added
        codes = _callback_codes()

        def items(values):
            frame = inspect.currentframe()
            try:
                caller = frame.f_back
                if (
                    caller.f_code in codes
                    and caller.f_locals.get("self") is agent._activity
                ):
                    speech = caller.f_locals.get("speech_handle")
                    for call in values:
                        if not isinstance(call, llm.FunctionCall):
                            continue
                        for owner in self.root.owners:
                            if owner.calls.get(call.call_id) is not call:
                                continue
                            if not any(
                                value is speech for value in owner.speech_members
                            ):
                                raise llm.RealtimeError(
                                    "Original tool SpeechHandle unavailable"
                                )

                            def registered(owner=owner, call=call, speech=speech):
                                found = []
                                for task in asyncio.all_tasks():
                                    info = _get_activity_task_info(task)
                                    if (
                                        info is not None
                                        and info.function_call is call
                                        and info.speech_handle is speech
                                    ):
                                        owner.tasks.add(task)
                                        found.append(task)
                                        if owner.closed:
                                            self.proxy._cancel_actual(owner)
                                owner.registrations[call.call_id].set_result(
                                    bool(found)
                                )

                            asyncio.get_running_loop().call_soon(registered)
            finally:
                del frame
            original_items(values)

        agent._tool_items_added = items

    def session(self, *, turn_detection_disabled=False):
        assert self.proxy is None
        self.proxy = IsolatedAskSession(self, self.primary_model.session())
        return self.proxy

    async def aclose(self):
        if self.proxy is not None:
            await self.proxy.aclose()
        await self.primary_model.aclose()


class IsolatedAskSession(llm.RealtimeSession):
    def __init__(self, model, primary):
        super().__init__(model)
        self.model, self.primary = model, primary
        self.closed = False
        self.primary_generations = []

        def primary_generation(event):
            self.primary_generations.append(event)
            self.emit("generation_created", event)

        primary.on("generation_created", primary_generation)
        primary.on("error", lambda event: self.emit("error", event))
        root = model.root
        root.expiry = asyncio.get_running_loop().call_at(
            asyncio.get_running_loop().time()
            + max(0, root.original_deadline - time.monotonic()),
            self.request_close,
        )

    @property
    def chat_ctx(self):
        owner = self._sdk_owner()
        return owner.inner.chat_ctx if owner is not None else self.primary.chat_ctx

    @property
    def tools(self):
        owner = self._sdk_owner()
        return owner.inner.tools if owner is not None else self.primary.tools

    def _sdk_owner(self):
        from livekit.agents.voice.agent_activity import AgentActivity

        reply_code = inspect.unwrap(AgentActivity._realtime_reply_task).__code__
        generation_code = inspect.unwrap(
            AgentActivity._realtime_generation_task_impl
        ).__code__
        frame = inspect.currentframe()
        try:
            for _ in range(64):
                if frame is None:
                    return None
                if frame.f_code in {reply_code, generation_code}:
                    activity = frame.f_locals.get("self")
                    speech = frame.f_locals.get("speech_handle")
                    if (
                        activity is not self.model.agent._activity
                        or activity.realtime_llm_session is not self
                    ):
                        raise llm.RealtimeError("Original Activity changed")
                    for owner in self.model.root.owners:
                        if frame.f_code is reply_code and owner.speech is speech:
                            return owner
                        event = frame.f_locals.get("generation_ev")
                        if (
                            frame.f_code is generation_code
                            and any(x is event for x in owner.generations)
                            and any(x is speech for x in owner.speech_members)
                        ):
                            return owner
                    # The genuine primary generation remains a separate caller
                    # operation, without access to any application Ask Future.
                    event = frame.f_locals.get("generation_ev")
                    if any(x is event for x in self.primary_generations):
                        return None
                    raise llm.RealtimeError(
                        "Original event/SpeechHandle membership unavailable"
                    )
                frame = frame.f_back
        finally:
            del frame
        return None

    def generate_reply(self, **options):
        self.model.root.require_open()
        owner = self._sdk_owner()
        if owner is None:
            raise llm.RealtimeError("Exact original SDK application ask is unavailable")
        if owner.future is not None or owner.closed:
            raise llm.RealtimeError("Original ask cannot be rebound")
        owner.future = asyncio.get_running_loop().create_future()
        owner.startup = asyncio.create_task(self._start_owner(owner))

        def cancelled(future):
            if future.cancelled():
                self.close_owner(owner)

        owner.future.add_done_callback(cancelled)
        return owner.future

    async def _start_owner(self, owner):
        try:
            self.model.root.require_open()
            # One fresh HTTP/native attempt for THIS genuine ask. The fixture
            # factory uses the SAME selected route/model/backend, never retries
            # an old attempt or resets RelayRealtimeClient._attempted.
            owner.client = self.model.open_original(owner.original)
            native_model = _GPTLiveDuplexModel(
                owner.client,
                model=self.model.primary_model.model,
                voice=self.model.primary_model.duplex_model._voice,
                backend=self.model.backend,
            )
            owner.adapter = llm.DuplexRealtimeAdapter(native_model)
            owner.inner = owner.adapter.session()

            def generation(event):
                owner.generations.append(event)
                if not owner.closed and not owner.future.done():
                    event.user_initiated = True
                    owner.future.set_result(event)
                # Later bursts remain owned by THIS retained operation. They
                # cannot consult or resolve another ask's Future.
                self.model.forwarding_owner = owner
                try:
                    self.emit("generation_created", event)
                finally:
                    self.model.forwarding_owner = None

            owner.inner.on("generation_created", generation)

            def original_call(call):
                if call.call_id in owner.calls:
                    raise llm.RealtimeError("Original backend call identity repeated")
                owner.calls[call.call_id] = call
                owner.registrations[call.call_id] = (
                    asyncio.get_running_loop().create_future()
                )

            owner.inner.duplex_session.on("function_call", original_call)
            context = llm.ChatContext.from_dict(json.loads(owner.original.context_json))
            instructions = self.primary.duplex_session._instructions
            if owner.original.instructions:
                instructions += "\n" + owner.original.instructions
            await owner.inner._update_session(
                instructions=instructions,
                chat_ctx=context,
                tools=list(owner.original.tools),
            )
            await owner.inner.duplex_session.start()
            self.model.root.require_open()
            if owner.closed:
                return
            # Startup/history can delegate before this command. Their backend
            # still has the immutable ask installed before physical open.
            owner.inner.duplex_session._generate_reply(
                instructions=owner.original.instructions
            )
        except BaseException as error:  # noqa: BLE001 -- TEST custody retains startup failure
            if owner.future is not None and not owner.future.done():
                if isinstance(error, asyncio.CancelledError):
                    owner.future.cancel()
                else:
                    owner.future.set_exception(error)
            if owner.client is not None and owner.client._voice is not None:
                owner.client._voice.abort()

    def request_close(self):
        if self.closed:
            return
        self.closed = self.model.root.closed = True
        for owner in self.model.root.owners:
            owner.closed = True
            if owner.future is not None and not owner.future.done():
                owner.future.set_exception(
                    llm.RealtimeError("Original TEST root closed")
                )
            if owner.client is not None and owner.client._voice is not None:
                owner.client._voice.abort()
        self.model.root.close_task = asyncio.create_task(self._join())

    def close_owner(self, owner):
        if owner.close_task is not None:
            return
        owner.closed = True
        owner.close_deadline = asyncio.get_running_loop().time() + 1
        if owner.future is not None and not owner.future.done():
            owner.future.set_exception(llm.RealtimeError("Original ask cancelled"))
        if owner.client is not None and owner.client._voice is not None:
            owner.client._voice.abort()
        self._cancel_actual(owner)
        owner.close_task = asyncio.create_task(self._join_owner(owner))

    def _cancel_actual(self, owner):
        from livekit.agents.voice.agent import _get_activity_task_info

        # The SDK executor creates an additional actual task after the start
        # callback. It inherits exact metadata; registration alone is no join.
        for task in asyncio.all_tasks():
            info = _get_activity_task_info(task)
            if (
                info is not None
                and any(call is info.function_call for call in owner.calls.values())
                and any(speech is info.speech_handle for speech in owner.speech_members)
            ):
                owner.tasks.add(task)
        for task in owner.tasks:
            if not task.done() and task not in owner.task_requests:
                cancelling = getattr(task, "cancelling", None)
                if not callable(cancelling) or not cancelling():
                    task.cancel()
                owner.task_requests.add(task)

    async def _join_owner(self, owner):
        if owner.startup is not None:
            if not owner.startup.done():
                owner.startup.cancel()
            await asyncio.gather(owner.startup, return_exceptions=True)
        if owner.inner is not None:
            await owner.inner.aclose()
        if owner.adapter is not None:
            await owner.adapter.aclose()
        if owner.client is not None:
            await owner.client.aclose()
        # Callback registration and actual tool tasks are distinct joins.
        deadline = owner.close_deadline
        registrations = set(owner.registrations.values())
        if registrations:
            await asyncio.wait(
                registrations,
                timeout=max(0, deadline - asyncio.get_running_loop().time()),
            )
        self._cancel_actual(owner)
        live = {task for task in owner.tasks if not task.done()}
        if live:
            await asyncio.wait(
                live, timeout=max(0, deadline - asyncio.get_running_loop().time())
            )
        owner.joined = all(
            future.done() and future.result() for future in registrations
        ) and all(task.done() for task in owner.tasks)

    async def _join(self):
        for owner in self.model.root.owners:
            self.close_owner(owner)
        await asyncio.gather(*(owner.close_task for owner in self.model.root.owners))
        await self.primary.aclose()

    async def aclose(self):
        self.request_close()
        if self.model.root.expiry is not None:
            self.model.root.expiry.cancel()
        if self.model.root.close_task is not None:
            await self.model.root.close_task

    async def _update_session(self, **options):
        await self.primary._update_session(**options)

    async def update_chat_ctx(self, context):
        owner = self._sdk_owner()
        if owner is None:
            await self.primary.update_chat_ctx(context)
            return
        if owner.closed:
            raise llm.RealtimeError("Original owner is closed")
        outputs = [
            item for item in context.items if isinstance(item, llm.FunctionCallOutput)
        ]
        if any(output.call_id not in owner.calls for output in outputs):
            raise llm.RealtimeError("Foreign result cannot enter original owner")
        await owner.inner.update_chat_ctx(context)

    async def update_instructions(self, value):
        await self.primary.update_instructions(value)

    async def update_tools(self, tools):
        await self.primary.update_tools(tools)

    def update_options(self, **options):
        self.primary.update_options(**options)

    def push_audio(self, frame):
        self.primary.push_audio(frame)

    def push_video(self, frame):
        self.primary.push_video(frame)

    def commit_audio(self):
        self.primary.commit_audio()

    def clear_audio(self):
        self.primary.clear_audio()

    def interrupt(self):
        # No fake exact-owner cancellation from this ID-less SDK method.
        # Explicit root close is the only installed TEST cleanup boundary.
        if self.closed:
            return
        owner = self._sdk_owner()
        if owner is None:
            raise llm.RealtimeError(
                "Exact original interruption owner port unavailable"
            )
        self.close_owner(owner)

    def truncate(self, **options):
        self.primary.truncate(**options)
