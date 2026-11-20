"""Original application-ask ownership, not provider completion or paid authority.

One new unfunded HTTP operation per genuine application invocation. The relay
must independently authenticate, admit, fund and cap every physical request.
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

from ._livekit_context import snapshot_context
from ._livekit_gpt_live import _GPTLiveDuplexModel
from ._livekit_tools import _relay_tools
from .client import GatewayError
from .relay_realtime import _FRAME_BYTES


@dataclasses.dataclass(frozen=True)
class OriginalAsk:
    instructions: str
    context_json: str
    captured_monotonic: float
    original_deadline: float
    new_client_once: Any
    base_instructions: str
    user_input: str | None
    tools: tuple[Any, ...]
    tool_schema: str


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
    timer: asyncio.TimerHandle | None = None
    context_json: str | None = None

    def context(self):
        return llm.ChatContext.from_dict(
            json.loads(self.context_json or self.original.context_json)
        )


class _Registry:
    def __init__(self, source_client):
        self.source_client = source_client
        self.maximum = (
            128  # Existing session identity/retention bound, not paid capacity.
        )
        self.closed = False
        self.owners = []
        self.close_task = None
        self.retained_bytes = 0

    def retain(self, original):
        size = len(
            (
                original.context_json
                + original.instructions
                + original.base_instructions
                + original.tool_schema
            ).encode("utf-8")
        )
        if (
            len(self.owners) >= self.maximum
            or self.retained_bytes + size > _FRAME_BYTES
        ):
            raise llm.RealtimeError("GPT-Live retained operation bound exceeded")
        self.retained_bytes += size
        owner = RetainedOwner(original, None)
        self.owners.append(owner)
        return owner

    def require_open(self):
        if self.closed or self.source_client._closed:
            raise llm.RealtimeError("Original GPT-Live session is closed")


class _OwnerScope:
    """Private instance port for actual original SDK calls and event membership."""

    def __init__(self, primary_model, backend, source_client, activity):
        self.primary_model, self.backend = primary_model, backend
        self.source_client = source_client
        self.activity = activity
        self.root = _Registry(source_client)
        self.proxy = None
        self.agent = None
        self.forwarding_owner = None
        self.original_generate = None
        self.primary_speeches = []

    def bind_actual_agent(self, agent: AgentSession):
        assert self.agent is None
        self.agent = agent
        original = agent.generate_reply
        original_interrupt = agent.interrupt
        original_items = agent._tool_items_added
        original_conversation = agent._conversation_item_added
        if (
            getattr(original, "__func__", None) is not AgentSession.generate_reply
            or getattr(original_interrupt, "__func__", None)
            is not AgentSession.interrupt
            or getattr(original_items, "__func__", None)
            is not AgentSession._tool_items_added
            or getattr(original_conversation, "__func__", None)
            is not AgentSession._conversation_item_added
        ):
            return
        self.original_generate = original

        def generate(**options):
            captured_at = time.monotonic()
            deadline, create_once = self.source_client._capture_original_request()
            self.root.require_open()
            if len(self.root.owners) >= self.root.maximum:
                raise llm.RealtimeError("GPT-Live retained operation bound exceeded")
            instructions = options.get("instructions", NOT_GIVEN)
            if not is_given(instructions):
                instructions = ""
            if not isinstance(instructions, str):
                raise llm.RealtimeError(
                    "GPT-Live requires original string instructions"
                )
            user_input = options.get("user_input", NOT_GIVEN)
            if isinstance(user_input, llm.ChatMessage):
                user_input = user_input.text_content
            if not is_given(user_input):
                user_input = None
            if user_input is not None and not isinstance(user_input, str):
                raise llm.RealtimeError("GPT-Live requires text input")
            tool_context = llm.ToolContext(self.activity.tools)
            selection = options.get("tools", NOT_GIVEN)
            if is_given(selection):
                available = {tool.id: tool for tool in tool_context.flatten()}
                available.update({tool.id: tool for tool in tool_context.toolsets})
                if not isinstance(selection, list) or any(
                    not isinstance(name, str) or name not in available
                    for name in selection
                ):
                    raise llm.RealtimeError(
                        "Original application tool selection is unavailable"
                    )
                tool_context = llm.ToolContext(
                    [available[name] for name in set(selection)]
                )
            original_tools = tool_context.flatten()
            # Capture BEFORE the actual synchronous SDK call creates/schedules
            # its SpeechHandle/task. No await or reconstruction from public IDs.
            context = snapshot_context(
                options.get("chat_ctx")
                if is_given(options.get("chat_ctx", NOT_GIVEN))
                else agent.current_agent.chat_ctx
            )
            original_ask = OriginalAsk(
                instructions=instructions,
                context_json=json.dumps(context.to_dict(exclude_config_update=True)),
                captured_monotonic=captured_at,
                original_deadline=deadline,
                new_client_once=create_once,
                base_instructions=agent.current_agent.instructions,
                user_input=user_input,
                tools=tuple(original_tools),
                tool_schema=json.dumps(_relay_tools(original_tools), sort_keys=True),
            )
            owner = self.root.retain(original_ask)
            try:
                speech = original(**options)
            except BaseException:
                owner.closed = True
                raise
            owner.speech = speech
            owner.speech_members.append(speech)

            def stopped(handle):
                if handle.interrupted or handle.exception() is not None:
                    self.proxy.close_owner(owner)

            speech.add_done_callback(stopped)
            owner.timer = asyncio.get_running_loop().call_later(
                max(0, deadline - time.monotonic()), self.proxy.close_owner, owner
            )
            return speech

        agent.generate_reply = generate
        self.generate_wrapper = generate

        def interrupt(**options):
            owners = tuple(self.root.owners)
            for owner in owners:
                self.proxy.close_owner(owner)
            local = original_interrupt(**options)

            async def joined():
                await local
                await asyncio.shield(
                    asyncio.gather(*(owner.close_task for owner in owners))
                )

            return asyncio.create_task(joined())

        agent.interrupt = interrupt
        self.interrupt_wrapper, self.original_interrupt = interrupt, original_interrupt

        def created(event):
            owner = self.forwarding_owner
            if owner is not None:
                owner.speech_members.append(event.speech_handle)
            elif not event.user_initiated:
                self.primary_speeches.append(event.speech_handle)

        agent.on("speech_created", created)
        self.created_callback = created

        from livekit.agents.voice.agent import _get_activity_task_info

        from speko_gateway._livekit_tool_cancellation import _callback_codes

        codes = _callback_codes()

        def items(values):
            frame = inspect.currentframe()
            try:
                caller = frame.f_back
                if (
                    caller.f_code in codes
                    and caller.f_locals.get("self") is self.activity
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
        self.items_wrapper, self.original_items = items, original_items

        def conversation(message):
            if (
                not self.root.closed
                and isinstance(message, llm.ChatMessage)
                and message.role == "assistant"
                and message.text_content
                and self.proxy._sdk_owner() is not None
            ):
                # This is the actual SDK's forwarded/heard segment, inside
                # its original generation/SpeechHandle frame. It is ordinary
                # conversation history, never the private ask instructions or
                # a claim that the physical provider response has ended.
                detached = message.model_copy(deep=True)
                self.proxy.primary.duplex_session._append_context_item(detached)
                self.proxy.primary._chat_ctx._upsert_item(
                    detached.model_copy(deep=True)
                )
            original_conversation(message)

        agent._conversation_item_added = conversation
        self.conversation_wrapper = conversation
        self.original_conversation = original_conversation

    def release_if_joined(self):
        if (
            not self.root.closed
            or self.original_generate is None
            or not all(owner.joined for owner in self.root.owners)
        ):
            return
        if self.agent.generate_reply is self.generate_wrapper:
            self.agent.generate_reply = self.original_generate
        if self.agent.interrupt is self.interrupt_wrapper:
            self.agent.interrupt = self.original_interrupt
        if self.agent._tool_items_added is self.items_wrapper:
            self.agent._tool_items_added = self.original_items
        if self.agent._conversation_item_added is self.conversation_wrapper:
            self.agent._conversation_item_added = self.original_conversation
        self.agent.off("speech_created", self.created_callback)
        self.original_generate = None

    def capture_direct(self, primary, options):
        captured_at = time.monotonic()
        deadline, create_once = self.source_client._capture_original_request()
        self.root.require_open()
        if len(self.root.owners) >= self.root.maximum:
            raise llm.RealtimeError("GPT-Live retained operation bound exceeded")
        instructions = options.get("instructions", NOT_GIVEN)
        if not is_given(instructions):
            instructions = ""
        if not isinstance(instructions, str):
            raise llm.RealtimeError("GPT-Live requires string instructions")
        context = snapshot_context(primary.chat_ctx)
        owner = RetainedOwner(
            OriginalAsk(
                instructions=instructions,
                context_json=json.dumps(context.to_dict(exclude_config_update=True)),
                captured_monotonic=captured_at,
                original_deadline=deadline,
                new_client_once=create_once,
                base_instructions=primary.duplex_session._instructions,
                user_input=None,
                tools=tuple(primary.tools.flatten()),
                tool_schema=json.dumps(
                    _relay_tools(primary.tools.flatten()), sort_keys=True
                ),
            ),
            None,
        )
        owner = self.root.retain(owner.original)
        owner.timer = asyncio.get_running_loop().call_later(
            max(0, deadline - time.monotonic()), self.proxy.close_owner, owner
        )
        return owner


class GptOwnerSession(llm.RealtimeSession):
    def __init__(self, model, primary, activity):
        super().__init__(model)
        self.model = _OwnerScope(
            model, model.duplex_model._backend, model.duplex_model._client, activity
        )
        self.model.proxy = self
        self.primary = primary
        if activity is not None:
            self.model.bind_actual_agent(activity._session)
        self.closed = False
        self.primary_generations = []

        def primary_generation(event):
            if len(self.primary_generations) >= self.model.root.maximum:
                self.request_close()
                self.emit(
                    "error",
                    llm.RealtimeModelError(
                        timestamp=time.time(),
                        label=self.realtime_model.label,
                        error=llm.RealtimeError(
                            "GPT-Live generation retention bound exceeded"
                        ),
                        recoverable=False,
                    ),
                )
                return
            self.primary_generations.append(event)
            self.emit("generation_created", event)

        primary.on("generation_created", primary_generation)

        def failed(event):
            self.request_close()
            self.emit("error", event)

        primary.on("error", failed)
        for event in (
            "input_speech_started",
            "input_speech_stopped",
            "input_audio_transcription_completed",
            "session_reconnected",
            "metrics_collected",
            "remote_item_added",
        ):
            primary.on(event, lambda value, event=event: self.emit(event, value))

    @property
    def duplex_session(self):
        return self.primary.duplex_session

    @property
    def chat_ctx(self):
        owner = self._sdk_owner()
        if owner is not None:
            return (
                snapshot_context(owner.inner.chat_ctx)
                if owner.inner is not None
                else owner.context()
            )
        return snapshot_context(self.primary.chat_ctx)

    @property
    def tools(self):
        owner = self._sdk_owner()
        return (
            owner.inner.tools
            if owner is not None and owner.inner is not None
            else self.primary.tools
        )

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
                        activity is not self.model.activity
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
        try:
            return self._generate_original(**options)
        except (llm.RealtimeError, GatewayError, RuntimeError, ValueError):
            future = asyncio.get_running_loop().create_future()
            future.set_exception(
                llm.RealtimeError(
                    "Original GPT-Live application request is unavailable"
                )
            )
            return future

    def _generate_original(self, **options):
        self.primary.duplex_session._update_options(
            tool_choice=options.get("tool_choice", NOT_GIVEN)
        )
        tools = options.get("tools", NOT_GIVEN)
        if is_given(tools) and tools != self.primary.tools.flatten():
            raise llm.RealtimeError("GPT-Live does not support per-response tools")
        self.model.root.require_open()
        owner = self._sdk_owner()
        if owner is None:
            if self.model.activity is not None:
                raise llm.RealtimeError(
                    "Exact original SDK application ask is unavailable"
                )
            owner = self.model.capture_direct(self.primary, options)
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
            # One fresh HTTP/native attempt for THIS genuine ask. The constructor-owned
            # factory uses the SAME selected route/model/backend, never retries
            # an old attempt or resets RelayRealtimeClient._attempted.
            if owner.closed:
                return
            if (
                json.dumps(_relay_tools(list(owner.original.tools)), sort_keys=True)
                != owner.original.tool_schema
            ):
                raise llm.RealtimeError("Original application tool schema changed")
            owner.client = owner.original.new_client_once()
            native_model = _GPTLiveDuplexModel(
                owner.client,
                model=self.model.primary_model.model,
                voice=self.model.primary_model.duplex_model._voice,
                backend=self.model.backend,
                _original_ask=owner,
            )
            owner.adapter = llm.DuplexRealtimeAdapter(native_model)
            owner.inner = owner.adapter.session()

            def generation(event):
                if len(owner.generations) >= self.model.root.maximum:
                    self.request_close()
                    self.emit(
                        "error",
                        llm.RealtimeModelError(
                            timestamp=time.time(),
                            label=self.realtime_model.label,
                            error=llm.RealtimeError(
                                "GPT-Live generation retention bound exceeded"
                            ),
                            recoverable=False,
                        ),
                    )
                    return
                owner.generations.append(event)
                if owner.closed:
                    return
                if not owner.future.done():
                    if owner.timer is not None:
                        owner.timer.cancel()
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

            def failed(event):
                if not owner.closed:
                    if owner.future is not None and not owner.future.done():
                        owner.future.set_exception(
                            llm.RealtimeError(
                                "Original GPT-Live request failed before generation"
                            )
                        )
                        self.close_owner(owner)
                    else:
                        self.request_close()
                        self.emit("error", event)

            owner.inner.on("error", failed)
            owner.inner.on(
                "metrics_collected", lambda event: self.emit("metrics_collected", event)
            )

            def original_call(call):
                if call.call_id in owner.calls:
                    raise llm.RealtimeError("Original backend call identity repeated")
                owner.calls[call.call_id] = call
                owner.registrations[call.call_id] = (
                    asyncio.get_running_loop().create_future()
                )

            owner.inner.duplex_session.on("function_call", original_call)
            context = owner.context()
            instructions = owner.original.base_instructions
            if owner.original.instructions:
                instructions += "\n" + owner.original.instructions
            await owner.inner._update_session(
                instructions=instructions,
                chat_ctx=context,
                tools=list(owner.original.tools),
            )
            await owner.inner.duplex_session.start()
            self.model.root.require_open()
            owner.client._remaining_setup()
            if owner.closed:
                return
            # Startup/history can delegate before this command. Their backend
            # still has the immutable ask installed before physical open.
            if not owner.inner.duplex_session._seen_delegations:
                owner.inner.duplex_session._generate_reply(
                    instructions=owner.original.instructions
                )
        except BaseException as error:  # noqa: BLE001 -- owned startup failure retains its cause
            if owner.future is not None and not owner.future.done():
                if isinstance(error, asyncio.CancelledError):
                    owner.future.cancel()
                else:
                    owner.future.set_exception(
                        llm.RealtimeError(
                            "Original GPT-Live request failed before generation"
                        )
                    )
            if owner.client is not None and owner.client._voice is not None:
                owner.client._voice.abort()
            self.close_owner(owner)

    def request_close(self):
        if self.closed:
            return
        self.closed = self.model.root.closed = True
        for owner in self.model.root.owners:
            owner.closed = True
            if owner.future is not None and not owner.future.done():
                owner.future.set_exception(
                    llm.RealtimeError("Original GPT-Live session closed")
                )
            if owner.client is not None and owner.client._voice is not None:
                owner.client._voice.abort()
        self.model.root.close_task = asyncio.create_task(self._join())

    def close_owner(self, owner):
        if owner.close_task is not None:
            return
        owner.closed = True
        if owner.timer is not None:
            owner.timer.cancel()
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
                and task not in owner.tasks
            ):
                owner.tasks.add(task)
                task.add_done_callback(
                    lambda _, owner=owner: self._actual_tasks_settled(owner)
                )
        for task in owner.tasks:
            if not task.done() and task not in owner.task_requests:
                cancelling = getattr(task, "cancelling", None)
                if not callable(cancelling) or not cancelling():
                    task.cancel()
                owner.task_requests.add(task)

    def _actual_tasks_settled(self, owner):
        if owner.closed and owner.close_task is not None and owner.close_task.done():
            owner.joined = all(
                future.done() and future.result()
                for future in owner.registrations.values()
            ) and all(task.done() for task in owner.tasks)
            self.model.release_if_joined()

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
        self.model.release_if_joined()

    async def aclose(self):
        self.request_close()
        if self.model.root.close_task is not None:
            await asyncio.shield(self.model.root.close_task)

    async def _update_session(self, **options):
        await self.primary._update_session(**options)

    async def start(self):
        await self.primary.duplex_session.start()

    async def update_chat_ctx(self, context):
        owner = self._sdk_owner()
        if owner is None:
            await self.primary.update_chat_ctx(context)
            return
        if owner.closed:
            raise llm.RealtimeError("Original owner is closed")
        if owner.inner is None:
            # Genuine SDK reply frame supplies its actual newly-created item;
            # its immutable content was captured at the public invocation.
            if owner.original.user_input is None:
                raise llm.RealtimeError(
                    "Unexpected original application context update"
                )
            known = {item.id: item for item in owner.context().items}
            new = [item for item in context.items if item.id not in known]
            if (
                len(new) != 1
                or not isinstance(new[0], llm.ChatMessage)
                or new[0].role != "user"
                or new[0].text_content != owner.original.user_input
            ):
                raise llm.RealtimeError("Original application input changed")
            owner.context_json = json.dumps(
                snapshot_context(context).to_dict(exclude_config_update=True)
            )
            return
        outputs = [
            item for item in context.items if isinstance(item, llm.FunctionCallOutput)
        ]
        if any(output.call_id not in owner.calls for output in outputs):
            raise llm.RealtimeError("Foreign result cannot enter original owner")
        await owner.inner.update_chat_ctx(context)

    async def update_instructions(self, value):
        await self.primary.update_instructions(value)

    async def update_tools(self, tools):
        owner = self._sdk_owner()
        if owner is not None:
            # The framework installs then restores per-response tools. Neither
            # operation can mutate the independent primary or an already
            # captured background tool round belonging to this original ask.
            if (
                owner.inner is None
                and json.dumps(
                    _relay_tools(llm.ToolContext(tools).flatten()), sort_keys=True
                )
                != owner.original.tool_schema
            ):
                raise llm.RealtimeError("Original application tool selection changed")
            return
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
        # Explicit root close is the whole-session cleanup boundary.
        if self.closed:
            return
        frame = inspect.currentframe()
        try:
            code = inspect.unwrap(AgentSession._teardown_activity).__code__
            cursor = frame
            for _ in range(64):
                if cursor is None:
                    break
                if (
                    cursor.f_code is code
                    and cursor.f_locals.get("self") is self.model.agent
                ):
                    self.request_close()
                    return
                cursor = cursor.f_back
        finally:
            del frame
        owner = self._sdk_owner()
        if owner is not None:
            self.close_owner(owner)
        elif self.model.activity is None:
            self.request_close()
        else:
            # Genuine primary caller barge-in keeps the native duplex provider's
            # own interruption policy. It cannot cancel/adopt an application ask.
            self.primary.interrupt()

    def truncate(self, **options):
        self.primary.truncate(**options)
