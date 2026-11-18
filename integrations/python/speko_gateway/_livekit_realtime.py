"""LiveKit native speech-to-speech; every socket uses one scoped relay attempt."""

from __future__ import annotations

import asyncio
import base64
import dataclasses
import inspect
import json
import time
import uuid
import weakref
from collections import deque
from typing import Any

from livekit import rtc
from livekit.agents import NOT_GIVEN, NotGivenOr, llm, utils
from livekit.agents.utils import is_given
from livekit.agents.utils.aio.channel import ChanFull

from ._livekit_context import snapshot_context
from ._livekit_tools import _relay_tools
from ._relay_transport import _error
from .client import GatewayError
from .relay_realtime import (
    _FRAME_BYTES,
    RealtimeProtocol,
    RelayRealtimeClient,
    RelayRealtimeSession,
)

_PROVIDERS = {
    "openai.realtime.v1": "openai",
    "xai.realtime.v1": "xai",
    "google.live.v1": "google",
}


class _FrameworkTurnBinding:
    """An instance-owned SDK mode fence, never worker/native authority."""

    def __init__(self, activity: Any) -> None:
        from livekit.agents.voice.agent_activity import AgentActivity

        self.activity = weakref.ref(activity)
        self.sessions: weakref.WeakSet[_RealtimeSession] = weakref.WeakSet()
        self.original = activity.update_options
        self.original_reply = activity._generate_reply
        self.original_configuration = {
            name: getattr(activity, name)
            for name in ("update_chat_ctx", "update_instructions", "update_tools")
        }
        if (
            not inspect.ismethod(self.original_reply)
            or self.original_reply.__func__ is not AgentActivity._generate_reply
            or any(
                not inspect.ismethod(method)
                or method.__func__ is not getattr(AgentActivity, name)
                for name, method in self.original_configuration.items()
            )
        ):
            raise GatewayError(
                "SDK explicit reply ownership is unavailable", retryable=False
            )

        def reply(*args: Any, **options: Any) -> Any:
            for session in list(self.sessions):
                if session._vad_input_seen and any(
                    is_given(options.get(name, NOT_GIVEN))
                    and options.get(name) is not None
                    for name in ("user_message", "instructions")
                ):
                    raise GatewayError(
                        "Typed reply requires provider input correlation",
                        retryable=False,
                    )
                if session._unattributed_application_reply:
                    raise GatewayError(
                        "Explicit reply correlation is unavailable after provider interruption",
                        retryable=False,
                    )
            return self.original_reply(*args, **options)

        self.observed_reply = reply
        activity._generate_reply = reply

        def bind_configuration(name: str, original: Any) -> Any:
            async def configuration(*args: Any, **options: Any) -> Any:
                if name == "update_chat_ctx":
                    sessions = list(self.sessions)
                    owner = self.activity()
                    if len(sessions) != 1 or owner is None:
                        raise GatewayError(
                            "SDK context ownership is unavailable", retryable=False
                        )
                    return await sessions[0]._update_framework_context(
                        owner, *args, **options
                    )
                for session in list(self.sessions):
                    session._check_context_boundary()
                return await original(*args, **options)

            return configuration

        self.observed_configuration = {
            name: bind_configuration(name, method)
            for name, method in self.original_configuration.items()
        }
        for name, method in self.observed_configuration.items():
            setattr(activity, name, method)

        def update(*args: Any, **options: Any) -> Any:
            change = options.get("turn_detection", NOT_GIVEN)
            for session in list(self.sessions):
                session._check_context_boundary()
                if not session._google:
                    continue
                expected = "manual" if session._manual_turns else "realtime_llm"
                if is_given(change) and change != expected:
                    failure = GatewayError(
                        "Gemini turn ownership is immutable", retryable=False
                    )
                    session._fatal(failure)
                    raise failure
            result = self.original(*args, **options)
            for session in list(self.sessions):
                session._check_framework_mode()
            return result

        self.observed = update
        activity.update_options = update

    def release(self, session: _RealtimeSession) -> None:
        self.sessions.discard(session)
        activity = self.activity()
        if not self.sessions and activity is not None:
            if activity._generate_reply is self.observed_reply:
                activity._generate_reply = self.original_reply
            if activity.update_options is self.observed:
                activity.update_options = self.original
            for name, method in self.observed_configuration.items():
                if getattr(activity, name) is method:
                    setattr(activity, name, self.original_configuration[name])
            if getattr(activity, "_speko_realtime_turn_binding", None) is self:
                del activity._speko_realtime_turn_binding


def _transcription(**fields: Any) -> llm.InputTranscriptionCompleted:
    # Added after the supported 1.4.5 framework floor; its absence changes
    # timestamp metadata only. Transcript/finality fields remain mandatory.
    if "turn_started_at" not in llm.InputTranscriptionCompleted.__dataclass_fields__:
        fields.pop("turn_started_at", None)
    return llm.InputTranscriptionCompleted(**fields)


class RealtimeModel(llm.RealtimeModel):
    """Execute an explicit OpenAI, xAI or Gemini route through the native sidecar.

    Input transcription never implicitly enables a separately priced ASR model.
    Gemini's native input/output transcripts are requested as part of its session.
    The caller owns the supplied client and closes it after this model.
    """

    def __init__(
        self,
        client: RelayRealtimeClient,
        *,
        protocol: RealtimeProtocol,
        model: str,
        voice: str,
        temperature: float | None = None,
        allow_interruptions: bool = True,
    ) -> None:
        if protocol not in _PROVIDERS:
            raise ValueError(
                "This LiveKit model requires a conversational realtime protocol"
            )
        if not model or model == "auto" or not isinstance(voice, str):
            raise ValueError(
                "Realtime execution requires a concrete compiled model and voice"
            )
        if temperature is not None and not 0 <= temperature <= 2:
            raise ValueError("Realtime temperature must be between zero and two")
        if protocol == "openai.realtime.v1" and temperature is not None:
            raise ValueError("OpenAI realtime does not support a sampling temperature")
        self._client, self._protocol, self._model = client, protocol, model
        self._voice, self._temperature = voice, temperature
        self._allow_interruptions = allow_interruptions
        self._sessions: weakref.WeakSet[_RealtimeSession] = weakref.WeakSet()
        self._closed = self._used = False
        google = protocol == "google.live.v1"
        declared = {
            "message_truncation": not google,
            "turn_detection": True,
            "user_transcription": google,
            "auto_tool_reply_generation": google,
            "audio_output": True,
            "manual_function_calls": not google,
            "can_disable_turn_detection": True,
            "mutable_chat_context": not google,
            "mutable_instructions": not google,
            "mutable_tools": not google,
            "per_response_tool_choice": not google,
            "supports_say": True,
        }
        known = {field.name for field in dataclasses.fields(llm.RealtimeCapabilities)}
        super().__init__(
            capabilities=llm.RealtimeCapabilities(
                **{k: v for k, v in declared.items() if k in known}
            )
        )

    @property
    def model(self) -> str:
        return self._model

    @property
    def provider(self) -> str:
        return _PROVIDERS[self._protocol]

    def session(self, *, turn_detection_disabled: bool = False) -> _RealtimeSession:
        if self._closed or self._used:
            raise GatewayError("Realtime model attempt is exhausted", retryable=False)
        owner = self._validate_sdk_turn_mode(turn_detection_disabled)
        self._used = True
        session = _RealtimeSession(self, turn_detection_disabled)
        self._sessions.add(session)
        if owner is not None:
            session._bind_framework(owner)
        return session

    def _validate_sdk_turn_mode(self, disabled: bool) -> Any:
        if self._protocol != "google.live.v1":
            return
        from livekit.agents.voice.agent_activity import AgentActivity

        # Refuse wrappers rather than guessing an unmatched framework owner is
        # a direct manual caller. Automatic restart/swap has no owned one-use
        # transport/turn boundary here. This is local SDK capability checking,
        # not a claim, credential, caller-presence or execution-grant proof.
        frame = inspect.currentframe()
        selected = None
        try:
            for _ in range(64):
                if frame is None:
                    break
                owner = frame.f_locals.get("self")
                if isinstance(owner, AgentActivity):
                    self._validate_framework_owner(owner, disabled)
                    if selected is not None and selected is not owner:
                        raise GatewayError(
                            "Gemini framework ownership is ambiguous", retryable=False
                        )
                    selected = owner
                elif (
                    isinstance(owner, llm.RealtimeSession)
                    or isinstance(owner, llm.RealtimeModel)
                    and owner is not self
                ):
                    raise GatewayError(
                        "Gemini realtime wrappers/fallback ownership is unsupported",
                        retryable=False,
                    )
                frame = frame.f_back
            if frame is not None:
                raise GatewayError(
                    "Gemini framework ownership exceeds its bound", retryable=False
                )
            return selected
        finally:
            del frame

    def _validate_framework_owner(self, owner: Any, disabled: bool) -> None:
        if owner.llm is not self:
            raise GatewayError(
                "Gemini realtime wrapper ownership is unsupported", retryable=False
            )
        mode = owner._turn_detection
        requested = owner._agent.turn_detection
        explicit = is_given(requested)
        if not explicit:
            requested = owner._session.turn_detection
            explicit = getattr(
                owner._session, "_turn_detection_explicit", is_given(requested)
            )
        if (
            requested in ("vad", "stt")
            or (disabled and mode != "manual")
            or mode not in (None, "manual", "realtime_llm")
        ):
            raise GatewayError(
                "Gemini asynchronous VAD requires correlated input capture",
                retryable=False,
            )
        if (mode == "manual" or explicit and requested == "manual") and not disabled:
            raise GatewayError(
                "Gemini manual input requires negotiated turn ownership",
                retryable=False,
            )
        if getattr(owner, "_interruption_detection_enabled", False):
            raise GatewayError(
                "Gemini local interruption ownership is unsupported", retryable=False
            )
        if disabled and owner._session.input.audio is not None:
            raise GatewayError(
                "Gemini manual AgentSession audio sources require an owned turn boundary",
                retryable=False,
            )

    async def aclose(self) -> None:
        self._closed = True
        await asyncio.gather(*(session.aclose() for session in self._sessions))


class _Message:
    def __init__(self, item_id: str) -> None:
        self.id = item_id
        self.audio = utils.aio.Chan[rtc.AudioFrame](maxsize=256)
        self.text = utils.aio.Chan[str](maxsize=256)
        self.transcript = ""
        self.interrupted = False

    def close(self, *, interrupted: bool = False) -> None:
        if interrupted and not self.interrupted:
            self.interrupted = True
            self.transcript = ""
            # These frames have not reached the framework's playback path.
            # A normal completion drains them; an interruption discards them.
            for stream in (self.audio, self.text):
                while not stream.empty():
                    stream.recv_nowait()
        self.audio.close()
        self.text.close()


class _Generation:
    def __init__(self, response_id: str, user_initiated: bool) -> None:
        self.id, self.interrupted = response_id, False
        self.messages: dict[str, _Message] = {}
        self.calls: set[str] = set()
        self.message_stream = utils.aio.Chan[llm.MessageGeneration](maxsize=16)
        self.function_stream = utils.aio.Chan[llm.FunctionCall](maxsize=128)
        self.event = llm.GenerationCreatedEvent(
            message_stream=self.message_stream,
            function_stream=self.function_stream,
            user_initiated=user_initiated,
            response_id=response_id,
        )

    def message(self, item_id: str) -> _Message:
        if item_id not in self.messages:
            if len(self.messages) >= 16:
                raise GatewayError("Realtime output exceeds its bound", retryable=False)
            message = _Message(item_id)
            self.messages[item_id] = message
            modalities = asyncio.get_running_loop().create_future()
            modalities.set_result(["audio", "text"])
            self.message_stream.send_nowait(
                llm.MessageGeneration(
                    message_id=item_id,
                    text_stream=message.text,
                    audio_stream=message.audio,
                    modalities=modalities,
                )
            )
        return self.messages[item_id]

    def close(self) -> None:
        for message in self.messages.values():
            message.close(interrupted=self.interrupted)
        self.message_stream.close()
        self.function_stream.close()


class _RealtimeSession(llm.RealtimeSession):
    def __init__(self, model: RealtimeModel, turn_detection_disabled: bool) -> None:
        super().__init__(model)
        self._model = model
        self._manual_turns = turn_detection_disabled
        self._google = model._protocol == "google.live.v1"
        self._instructions = ""
        self._chat_ctx = llm.ChatContext.empty()
        self._tools = llm.ToolContext.empty()
        self._known_items: set[str] = set()
        self._tool_choice: llm.ToolChoice | None = "auto"
        self._configured = asyncio.Event()
        self._ready = asyncio.get_running_loop().create_future()
        self._ready.add_done_callback(self._observe_future)
        self._commands = utils.aio.Chan[str](maxsize=256)
        self._queued_input_bytes = 0
        self._socket: RelayRealtimeSession | None = None
        self._generation: _Generation | None = None
        self._pending: deque[asyncio.Future[llm.GenerationCreatedEvent]] = deque()
        self._pending_timers: dict[asyncio.Future, asyncio.TimerHandle] = {}
        self._contextual_futures: weakref.WeakSet[asyncio.Future] = weakref.WeakSet()
        self._typed_context_queued = False
        # Negative capability only: provider-VAD input can have another unseen
        # response. No observed output/terminal establishes a drain barrier.
        self._vad_input_seen = False
        self._resampler: rtc.AudioResampler | None = None
        self._input_rate: int | None = None
        self._input_hz = 16000 if self._google else 24000
        self._audio_active = self._committed = False
        # A commit starts provider work before its first output exists locally.
        # This is only a restriction on admitting uncorrelated new work, never
        # a fabricated response identity or acknowledgement.
        self._google_response_pending = False
        self._google_response_interrupted = False
        self._google_interrupted_pending: set[asyncio.Future] = set()
        self._provider_interruption_feedback = False
        self._provider_retired_futures: weakref.WeakSet[asyncio.Future] = (
            weakref.WeakSet()
        )
        self._unattributed_application_reply = False
        self._activity_interrupt_target: _Generation | None = None
        self._uncommitted_source_samples = 0
        self._framework_binding: _FrameworkTurnBinding | None = None
        self._closed = False
        self._failure: GatewayError | None = None
        self._input_text = ""
        self._input_item = ""
        self._user_started: float | None = None
        self._running = False
        self._task = asyncio.create_task(self._run())

    def on(self, event: Any, callback: Any = None) -> Any:
        if callback is None:
            return lambda selected: self.on(event, selected)
        from livekit.agents.voice.agent_activity import AgentActivity

        owner = getattr(callback, "__self__", None)
        if isinstance(owner, AgentActivity):
            self._bind_framework(owner)
        elif (
            self._google
            and isinstance(owner, llm.RealtimeSession)
            and owner is not self
        ):
            failure = GatewayError(
                "Gemini realtime wrapper ownership is unsupported", retryable=False
            )
            self._fatal(failure)
            raise failure
        return super().on(event, callback)

    def _check_explicit_reply_owner(self) -> None:
        frame = inspect.currentframe()
        try:
            for _ in range(64):
                if frame is None:
                    return
                owner = frame.f_locals.get("self")
                if isinstance(owner, llm.RealtimeSession) and owner is not self:
                    raise GatewayError(
                        "Realtime wrapper explicit reply ownership is unavailable",
                        retryable=False,
                    )
                frame = frame.f_back
            if frame is not None:
                raise GatewayError(
                    "Realtime reply ownership exceeds its bound", retryable=False
                )
        finally:
            del frame

    def _bind_framework(self, owner: Any) -> None:
        try:
            if self._google:
                self._model._validate_framework_owner(owner, self._manual_turns)
            elif owner.llm is not self._model:
                raise GatewayError(
                    "SDK explicit reply ownership is unavailable", retryable=False
                )
        except GatewayError as failure:
            self._fatal(failure)
            raise
        binding = getattr(owner, "_speko_realtime_turn_binding", None)
        if binding is None:
            binding = _FrameworkTurnBinding(owner)
            owner._speko_realtime_turn_binding = binding
        if (
            self._framework_binding is not None
            and self._framework_binding is not binding
        ):
            self._framework_binding.release(self)
        binding.sessions.add(self)
        self._framework_binding = binding

    def _check_framework_mode(self) -> None:
        binding = self._framework_binding
        if binding is None:
            return
        owner = binding.activity()
        try:
            if (
                owner is None
                or owner.update_options is not binding.observed
                or owner._generate_reply is not binding.observed_reply
                or any(
                    getattr(owner, name) is not method
                    for name, method in binding.observed_configuration.items()
                )
            ):
                raise GatewayError(
                    "Gemini framework turn owner is unavailable", retryable=False
                )
            if self._google:
                self._model._validate_framework_owner(owner, self._manual_turns)
        except GatewayError as failure:
            self._fatal(failure)
            raise

    def _ensure_live(self) -> None:
        if self._closed or self._failure is not None:
            raise llm.RealtimeError("Realtime session is closed")
        self._check_framework_mode()
        for pending in list(self._pending):
            if pending.cancelled():
                self._pending_cancelled(pending)
                if self._closed or self._failure is not None:
                    raise llm.RealtimeError("Realtime session is closed")

    @staticmethod
    def _observe_future(future: asyncio.Future) -> None:
        if not future.cancelled():
            future.exception()

    @property
    def chat_ctx(self) -> llm.ChatContext:
        return snapshot_context(self._chat_ctx)

    @property
    def tools(self) -> llm.ToolContext:
        return self._tools

    async def start(self) -> None:
        """Wait for the native provider configuration, without opening a second attempt."""
        self._configured.set()
        await asyncio.shield(self._ready)

    async def _update_session(
        self, *, instructions=NOT_GIVEN, chat_ctx=NOT_GIVEN, tools=NOT_GIVEN
    ) -> None:
        # Atomic startup configuration is also available before the framework
        # introduced this helper. Do not swallow configuration errors.
        if is_given(instructions):
            await self.update_instructions(instructions)
        if is_given(chat_ctx):
            await self.update_chat_ctx(chat_ctx)
        if is_given(tools):
            await self.update_tools(tools)

    async def update_instructions(self, instructions: str) -> None:
        self._check_context_boundary()
        if not self._configured.is_set():
            self._instructions = instructions
            return
        if self._google:
            raise llm.RealtimeError("Gemini instructions are immutable after setup")
        await self._ready
        self._check_context_boundary()
        self._instructions = instructions
        self._enqueue(
            {"type": "session.update", "session": {"instructions": instructions}}
        )

    async def update_tools(self, tools: list[llm.Tool]) -> None:
        self._check_context_boundary()
        context = llm.ToolContext(tools)
        _relay_tools(context.flatten())
        if self._google and context.flatten():
            # Supported LiveKit versions execute the Agent's original tools,
            # not wrappers returned by this session. No public per-call cancel
            # hook lets us honor Gemini toolCallCancellation for that work.
            # Refuse before opening a socket rather than dispatch side effects
            # we cannot stop. Extended-thinking async tools need this too.
            self._fatal(
                GatewayError("Gemini tool cancellation is unavailable", retryable=False)
            )
            raise llm.RealtimeError("Gemini tool cancellation is unavailable")
        if self._configured.is_set():
            if self._google:
                raise llm.RealtimeError("Gemini tools are immutable after setup")
            await self._ready
            self._check_context_boundary()
            self._enqueue(
                {
                    "type": "session.update",
                    "session": {"tools": self._wire_tools(context)},
                }
            )
        self._tools = context
        # AgentSession stages instructions, history and tools in this order.
        # Nothing can leave before the whole initial configuration is captured.
        self._configured.set()

    async def update_chat_ctx(self, chat_ctx: llm.ChatContext) -> None:
        if not self._configured.is_set():
            self._chat_ctx = snapshot_context(chat_ctx)
            return
        self._check_explicit_reply_owner()
        await self._ready
        self._preflight_context(chat_ctx)
        chat_ctx = snapshot_context(chat_ctx)
        items = [
            item
            for item in chat_ctx.items
            if item.type in {"message", "function_call", "function_call_output"}
        ]
        if self._unattributed_application_reply and items != self._chat_ctx.items:
            raise GatewayError(
                "Explicit reply correlation is unavailable after provider interruption",
                retryable=False,
            )
        if self._google:
            additions = [item for item in items if item.id not in self._known_items]
            if additions and self._google_response_pending:
                # RealtimeError is best effort in AgentSession's typed path.
                # Refuse BEFORE append/history changes while older provider
                # work could still claim the next reply future.
                raise GatewayError(
                    "Wait for the Gemini response to finish before adding typed context",
                    retryable=False,
                )
            if (
                additions
                and self._manual_turns
                and (self._audio_active or self._uncommitted_source_samples)
            ):
                # AgentSession appends typed input before generate_reply and
                # treats RealtimeError from this update as best effort. Refuse
                # with a non-best-effort error BEFORE wire/history mutation;
                # leave original capture intact for an explicit commit/clear.
                raise GatewayError(
                    "Commit manual input before adding typed context", retryable=False
                )
            # Gemini appends text context. Tool execution stays refused until
            # the framework has an owned per-call cancellation interface.
            if any(
                not isinstance(item, llm.ChatMessage)
                or item.role != "user"
                or not item.text_content
                or not all(isinstance(part, str) for part in item.content)
                for item in additions
            ):
                raise llm.RealtimeError("Gemini context updates require user text")
            turns = [
                {"role": "user", "parts": [{"text": item.text_content}]}
                for item in additions
                if isinstance(item, llm.ChatMessage)
            ]
            if turns:
                self._enqueue(
                    {"clientContent": {"turns": turns, "turnComplete": False}}
                )
                # This explicit typed turn is new work. It must not consume
                # the "reply already requested by audio commit" shortcut.
                self._committed = False
            self._known_items.update(item.id for item in additions)
        else:
            wanted = {item.id for item in items}
            for removed in self._known_items - wanted:
                self._enqueue({"type": "conversation.item.delete", "item_id": removed})
            for item in items:
                if item.id not in self._known_items:
                    self._enqueue(
                        {
                            "type": "conversation.item.create",
                            "item": self._wire_item(item),
                        }
                    )
            self._known_items = wanted
        if any(
            isinstance(item, llm.ChatMessage)
            and item.role == "user"
            and item.id not in {old.id for old in self._chat_ctx.items}
            for item in items
        ):
            self._typed_context_queued = True
        self._chat_ctx = snapshot_context(chat_ctx)

    def _preflight_context(self, chat_ctx: llm.ChatContext) -> None:
        self._check_context_boundary()
        previous = {item.id: item for item in self._chat_ctx.items}
        changed_user = any(
            isinstance(item, llm.ChatMessage)
            and item.role == "user"
            and (
                item.id not in previous
                or not isinstance(previous[item.id], llm.ChatMessage)
                or previous[item.id].content != item.content
                or previous[item.id].role != item.role
            )
            for item in chat_ctx.items
        )
        if not changed_user:
            return
        if self._vad_input_seen:
            raise GatewayError(
                "Typed context requires provider input correlation", retryable=False
            )
        if self._google:
            if self._google_response_pending:
                raise GatewayError(
                    "Wait for the Gemini response to finish before adding typed context",
                    retryable=False,
                )
            if self._manual_turns and (
                self._audio_active or self._uncommitted_source_samples
            ):
                raise GatewayError(
                    "Commit manual input before adding typed context", retryable=False
                )
        elif self._pending or self._generation is not None:
            raise GatewayError(
                "Wait for the realtime response to finish before adding typed context",
                retryable=False,
            )

    async def _update_framework_context(
        self,
        owner: Any,
        chat_ctx: llm.ChatContext,
        *,
        exclude_invalid_function_calls: bool = True,
    ) -> None:
        from livekit.agents.voice.generation import remove_instructions

        staged = snapshot_context(chat_ctx)
        lock = getattr(owner, "_rt_chat_ctx_lock", None)
        if isinstance(lock, asyncio.Lock) and lock.locked():
            raise GatewayError(
                "SDK context update requires correlation while its lock is busy",
                retryable=False,
            )
        await self.start()

        async def apply() -> None:
            self._check_framework_mode()
            if owner._rt_session is not self:
                raise GatewayError(
                    "SDK context ownership is unavailable", retryable=False
                )
            context = snapshot_context(
                staged.copy(
                    tools=owner.tools if exclude_invalid_function_calls else NOT_GIVEN
                )
            )
            remove_instructions(context)
            self._preflight_context(context)
            await self.update_chat_ctx(context)
            # The original SDK assigns this BEFORE its native await. Publish
            # only after native acceptance, with the real SDK lock held.
            owner._agent._chat_ctx = snapshot_context(context)

        if lock is None:
            # 1.4.5/1.6.8 have no context lock. Readiness is already complete;
            # preflight, native enqueue and assignment do not yield here.
            await apply()
        elif isinstance(lock, asyncio.Lock):
            async with lock:
                await apply()
        else:
            raise GatewayError(
                "SDK context lock ownership is unavailable", retryable=False
            )

    def update_options(
        self, *, tool_choice: NotGivenOr[llm.ToolChoice | None] = NOT_GIVEN
    ) -> None:
        if not is_given(tool_choice):
            return
        self._check_context_boundary()
        if self._google and tool_choice not in (None, "auto"):
            raise llm.RealtimeError("Gemini per-response tool choice is unavailable")
        self._tool_choice = tool_choice

    def _check_context_boundary(self) -> None:
        self._ensure_live()
        if self._unattributed_application_reply:
            raise GatewayError(
                "Explicit reply correlation is unavailable after provider interruption",
                retryable=False,
            )

    def _enqueue(self, event: dict[str, Any]) -> None:
        self._ensure_live()
        packet = json.dumps(
            event, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        )
        size = len(packet.encode("utf-8"))
        if size > _FRAME_BYTES or self._queued_input_bytes + size > _FRAME_BYTES:
            self._fatal(
                GatewayError("Realtime input exceeds its byte bound", retryable=False)
            )
            return
        try:
            self._commands.send_nowait(packet)
            self._queued_input_bytes += size
        except ChanFull:
            self._fatal(
                GatewayError("Realtime input exceeds its bound", retryable=False)
            )

    def start_user_activity(self) -> None:
        self._ensure_live()
        self._check_manual_input_boundary()
        self._user_started = time.time()
        if self._google and self._manual_turns and not self._audio_active:
            self._enqueue({"realtimeInput": {"activityStart": {}}})
            self._audio_active = True
            self._activity_interrupt_target = self._generation

    def push_audio(self, frame: rtc.AudioFrame) -> None:
        self._ensure_live()
        self._check_manual_input_boundary()
        if frame.num_channels != 1:
            raise ValueError("Realtime input must be mono PCM16")
        if not self._manual_turns and frame.samples_per_channel:
            if self._generation is None and any(
                future in self._contextual_futures for future in self._pending
            ):
                failure = GatewayError(
                    "Input overlaps an uncorrelated typed response", retryable=False
                )
                self._fatal(failure)
                raise failure
            self._vad_input_seen = True
        self._configured.set()
        if self._input_rate is None:
            self._input_rate = frame.sample_rate
            if frame.sample_rate != self._input_hz:
                self._resampler = rtc.AudioResampler(
                    input_rate=frame.sample_rate,
                    output_rate=self._input_hz,
                    num_channels=1,
                )
        if frame.sample_rate != self._input_rate:
            raise ValueError("Realtime input rate cannot change during a session")
        # Original source ownership precedes conversion. A short raw frame can
        # remain entirely inside the native resampler without opening activity.
        self._uncommitted_source_samples += frame.samples_per_channel
        for converted in self._resampler.push(frame) if self._resampler else [frame]:
            self._send_audio(bytes(converted.data))

    def _send_audio(self, pcm: bytes) -> None:
        if not pcm:
            return
        if len(pcm) > (_FRAME_BYTES - 1024) * 3 // 4:
            self._fatal(
                GatewayError("Realtime audio frame exceeds its bound", retryable=False)
            )
            return
        encoded = base64.b64encode(pcm).decode("ascii")
        if self._google:
            if self._manual_turns and not self._audio_active:
                self.start_user_activity()
            self._enqueue(
                {
                    "realtimeInput": {
                        "audio": {
                            "data": encoded,
                            "mimeType": f"audio/pcm;rate={self._input_hz}",
                        }
                    }
                }
            )
        else:
            self._enqueue({"type": "input_audio_buffer.append", "audio": encoded})

    def push_video(self, frame: rtc.VideoFrame) -> None:
        raise llm.RealtimeError("This realtime model accepts audio only")

    def _check_manual_input_boundary(self) -> None:
        if self._unattributed_application_reply:
            failure = GatewayError(
                "Input response correlation is unavailable after provider interruption",
                retryable=False,
            )
            self._fatal(failure)
            raise failure
        if self._google and self._manual_turns and self._google_response_pending:
            # No input frontier or response identity can attribute a second
            # activity. Refuse BEFORE accepting source samples, conversion or
            # physical controls; do not keep an apparently usable poisoned
            # transport sending audio whose handles can never succeed.
            for pending in list(self._pending):
                self._retire_pending(pending)
                if not pending.done():
                    pending.set_exception(
                        llm.RealtimeError(
                            "Gemini manual input overlaps an unsettled response"
                        )
                    )
            failure = GatewayError(
                "Wait for the Gemini terminal response before sending more manual input",
                retryable=False,
            )
            self._fatal(failure)
            raise failure

    def commit_audio(self) -> None:
        self._ensure_live()
        if self._unattributed_application_reply:
            raise GatewayError(
                "Explicit reply correlation is unavailable after provider interruption",
                retryable=False,
            )
        if self._google and self._manual_turns and self._google_response_pending:
            raise GatewayError(
                "Wait for the Gemini terminal response before committing again",
                retryable=False,
            )
        if self._resampler:
            for frame in self._resampler.flush():
                self._send_audio(bytes(frame.data))
        if self._google:
            if self._manual_turns and self._audio_active:
                self._enqueue({"realtimeInput": {"activityEnd": {}}})
                self._audio_active = False
                self._activity_interrupt_target = None
                self._committed = True
                self._google_response_pending = True
                self._google_response_interrupted = False
        else:
            self._enqueue({"type": "input_audio_buffer.commit"})
        self._uncommitted_source_samples = 0
        self._reset_resampler()

    def _reset_resampler(self) -> None:
        self._resampler = None
        if self._input_rate is not None and self._input_rate != self._input_hz:
            self._resampler = rtc.AudioResampler(
                input_rate=self._input_rate, output_rate=self._input_hz, num_channels=1
            )

    def clear_audio(self) -> None:
        self._ensure_live()
        if not self._google:
            self._enqueue({"type": "input_audio_buffer.clear"})
        elif self._manual_turns and self._audio_active:
            self._fatal(
                GatewayError(
                    "Gemini cannot clear already forwarded manual input",
                    retryable=False,
                )
            )
            return
        # Still-local capture is discarded, never flushed into the next turn.
        self._uncommitted_source_samples = 0
        self._reset_resampler()

    def generate_reply(
        self,
        *,
        instructions: NotGivenOr[str] = NOT_GIVEN,
        tool_choice: NotGivenOr[llm.ToolChoice] = NOT_GIVEN,
        tools: NotGivenOr[list[llm.Tool]] = NOT_GIVEN,
    ) -> asyncio.Future[llm.GenerationCreatedEvent]:
        future = asyncio.get_running_loop().create_future()
        future.add_done_callback(self._observe_future)
        try:
            self._ensure_live()
            self._check_explicit_reply_owner()
        except (llm.RealtimeError, GatewayError) as failure:
            future.set_exception(failure)
            return future
        if self._unattributed_application_reply:
            future.set_exception(
                llm.RealtimeError(
                    "Explicit reply correlation is unavailable after provider interruption"
                )
            )
            return future
        if self._pending:
            future.set_exception(
                llm.RealtimeError("A realtime reply is already pending")
            )
            return future
        contextual = self._typed_context_queued or (
            is_given(instructions) and instructions is not None
        )
        if self._vad_input_seen and contextual:
            future.set_exception(
                llm.RealtimeError("Typed reply requires provider input correlation")
            )
            return future
        if (
            self._google
            and self._manual_turns
            and (self._audio_active or self._uncommitted_source_samples)
        ):
            future.set_exception(
                llm.RealtimeError("Commit manual input before requesting a reply")
            )
            return future
        committed_reply = self._committed and not is_given(instructions)
        if (
            self._google
            and self._google_response_pending
            and (
                self._google_response_interrupted
                or not committed_reply
                or self._generation is not None
            )
        ):
            future.set_exception(
                llm.RealtimeError("Wait for the Gemini response to finish")
            )
            return future
        self._configured.set()
        self._pending.append(future)
        if contextual:
            self._contextual_futures.add(future)
        self._typed_context_queued = False
        future.add_done_callback(self._pending_cancelled)
        self._pending_timers[future] = asyncio.get_running_loop().call_later(
            self._model._client._setup_timeout, self._reply_timeout, future
        )
        if self._google:
            if is_given(tools) or is_given(tool_choice) and tool_choice != "auto":
                self._reply_timeout(future)
                return future
            if committed_reply:
                self._committed = False
            else:
                content: dict[str, Any] = {"turnComplete": True}
                if is_given(instructions):
                    content["turns"] = [
                        {"role": "user", "parts": [{"text": instructions}]}
                    ]
                self._google_response_pending = True
                self._google_response_interrupted = False
                self._enqueue({"clientContent": content})
        else:
            response: dict[str, Any] = {}
            if is_given(instructions):
                response["instructions"] = instructions
            chosen = tool_choice if is_given(tool_choice) else self._tool_choice
            if chosen is not None:
                response["tool_choice"] = chosen
            if is_given(tools):
                response["tools"] = self._wire_tools(llm.ToolContext(tools))
            self._enqueue({"type": "response.create", "response": response})
        return future

    def say(self, text: str | Any) -> asyncio.Future[llm.GenerationCreatedEvent]:
        if not isinstance(text, str):
            raise llm.RealtimeError("Realtime say requires a complete bounded string")
        return self.generate_reply(
            instructions="Say exactly this text, without additions: "
            + json.dumps(text, ensure_ascii=False)
        )

    def _reply_timeout(self, future: asyncio.Future) -> None:
        if future not in self._pending:
            return
        if not future.done():
            future.set_exception(
                llm.RealtimeError("Realtime reply was not acknowledged")
            )
        self._retire_pending(future)
        self._fatal(
            GatewayError("Realtime reply was not acknowledged", retryable=False)
        )

    def _retire_pending(self, future: asyncio.Future) -> None:
        self._contextual_futures.discard(future)
        self._google_interrupted_pending.discard(future)
        if future in self._pending:
            self._pending.remove(future)
        timer = self._pending_timers.pop(future, None)
        if timer is not None:
            timer.cancel()

    def _pending_cancelled(self, future: asyncio.Future) -> None:
        if future.cancelled() and future in self._pending:
            provider_interrupted = future in self._google_interrupted_pending
            self._retire_pending(future)
            if provider_interrupted:
                return
            # No response identity/cancel acknowledgement exists yet. Retire
            # this exact future and stop the physical request, not just playback.
            self._fatal(
                GatewayError(
                    "Realtime reply cancelled before generation", retryable=False
                )
            )

    def _sdk_interruption_cleanup(self) -> bool:
        from livekit.agents.voice.agent_activity import AgentActivity

        frame = inspect.currentframe()
        try:
            # Only the immediate genuine SDK callsite may suppress its own
            # stale cleanup. A programmatic Activity.interrupt, even inside a
            # provider notification, remains an independent local stop.
            caller = frame.f_back.f_back if frame and frame.f_back else None
            if caller is None:
                return False
            owner = caller.f_locals.get("self")
            if not isinstance(owner, AgentActivity) or owner._rt_session is not self:
                return False
            code = caller.f_code
            if code is inspect.unwrap(AgentActivity.interrupt).__code__:
                parent = caller.f_back
                return bool(
                    self._provider_interruption_feedback
                    and parent is not None
                    and parent.f_code
                    is inspect.unwrap(AgentActivity._on_input_speech_started).__code__
                    and parent.f_locals.get("self") is owner
                )
            if code is inspect.unwrap(AgentActivity._realtime_reply_task).__code__:
                return (
                    caller.f_locals.get("generate_reply_fut")
                    in self._provider_retired_futures
                )
            generation_cleanup = (
                AgentActivity._realtime_generation_task,
                getattr(AgentActivity, "_realtime_generation_task_impl", None),
            )
            if any(
                method is not None and code is inspect.unwrap(method).__code__
                for method in generation_cleanup
            ):
                event = caller.f_locals.get("generation_ev")
                return bool(
                    event is not None
                    and (
                        self._generation is None
                        or event is not self._generation.event
                        or self._generation.interrupted
                    )
                )
            return False
        finally:
            del frame

    def interrupt(self) -> None:
        if self._sdk_interruption_cleanup():
            return
        if self._generation is not None and self._generation.interrupted:
            # Genuine SDK playback cleanup was handled above. A separate
            # caller stop still owns physical cancellation, even when the
            # provider already marked this active generation interrupted.
            self._fatal(
                GatewayError(
                    "Interrupted provider work locally stopped", retryable=False
                )
            )
            return
        if self._pending:
            for future in list(self._pending):
                self._retire_pending(future)
                if not future.done():
                    future.set_exception(
                        llm.RealtimeError(
                            "Realtime reply interrupted before generation"
                        )
                    )
            self._fatal(
                GatewayError(
                    "Realtime reply interrupted before generation", retryable=False
                )
            )
            return
        if self._unattributed_application_reply and self._generation is None:
            self._fatal(
                GatewayError("Unattributed reply locally interrupted", retryable=False)
            )
            return
        if self._google and self._google_response_pending and self._generation is None:
            # An unclaimed commit (including skip_reply=True) still owns a
            # physical response. There is no pre-output cancel identity; do not
            # pretend a local interrupt/clear drained it for a later task.
            self._fatal(
                GatewayError(
                    "Realtime reply interrupted before generation", retryable=False
                )
            )
            return
        if self._generation is not None and not self._generation.interrupted:
            self._generation.interrupted = True
            self._generation.close()
            if not self._google:
                self._enqueue(
                    {"type": "response.cancel", "response_id": self._generation.id}
                )
            elif self._manual_turns:
                if not self._model._allow_interruptions or (
                    self._audio_active
                    and self._activity_interrupt_target is not self._generation
                ):
                    # Input opened before this generation cannot cancel it.
                    # Close only when cancellation is actually unavailable.
                    self._fatal(
                        GatewayError(
                            "Realtime provider cancellation is unavailable",
                            retryable=False,
                        )
                    )
                    return
                if self._audio_active:
                    # External VAD queues a real activityStart targeting this
                    # generation, then calls interrupt. Preserve its audio/end
                    # on the same socket; do not pulse or terminate that input.
                    return
                # A local interrupt of an ACTIVE generation must reach Gemini.
                # Balance the cancellation pulse without claiming caller audio
                # or leaving a synthetic activity open. Provider feedback marks
                # the generation interrupted BEFORE entering this method, so it
                # never sends another pulse (including asynchronous cleanup).
                self._enqueue({"realtimeInput": {"activityStart": {}}})
                self._enqueue({"realtimeInput": {"activityEnd": {}}})
            else:
                # Automatic VAD has no dedicated cancel-only control here.
                # Sending a new client turn would create different model work.
                self._fatal(
                    GatewayError(
                        "Realtime provider cancellation is unavailable", retryable=False
                    )
                )

    def truncate(
        self,
        *,
        message_id: str,
        modalities: list[str],
        audio_end_ms: int,
        audio_transcript: NotGivenOr[str] = NOT_GIVEN,
    ) -> None:
        if self._google:
            return
        if "audio" in modalities:
            if type(audio_end_ms) is not int or audio_end_ms < 0:
                raise ValueError(
                    "Realtime truncation requires a nonnegative audio boundary"
                )
            self._enqueue(
                {
                    "type": "conversation.item.truncate",
                    "item_id": message_id,
                    "content_index": 0,
                    "audio_end_ms": audio_end_ms,
                }
            )
            heard = audio_transcript if is_given(audio_transcript) else ""
            if self._generation and message_id in self._generation.messages:
                self._generation.messages[message_id].transcript = heard
            item = self._chat_ctx.get_by_id(message_id)
            if isinstance(item, llm.ChatMessage):
                item.content = [heard]
                item.interrupted = True

    def _wire_tools(self, context: llm.ToolContext) -> list[dict[str, Any]]:
        declarations = _relay_tools(context.flatten())
        if self._google:
            if declarations:
                raise llm.RealtimeError("Gemini tool cancellation is unavailable")
            return []
        return [{"type": "function", **item} for item in declarations]

    @staticmethod
    def _wire_item(item: llm.ChatItem) -> dict[str, Any]:
        if isinstance(item, llm.FunctionCall):
            return {
                "type": "function_call",
                "id": item.id,
                "call_id": item.call_id,
                "name": item.name,
                "arguments": item.arguments,
            }
        if isinstance(item, llm.FunctionCallOutput):
            return {
                "type": "function_call_output",
                "id": item.id,
                "call_id": item.call_id,
                "output": item.output,
            }
        if (
            not isinstance(item, llm.ChatMessage)
            or item.role not in {"user", "assistant", "system"}
            or any(not isinstance(part, str) for part in item.content)
        ):
            raise llm.RealtimeError(
                "Realtime history requires text messages or function items"
            )
        kind = "output_text" if item.role == "assistant" else "input_text"
        return {
            "type": "message",
            "id": item.id,
            "role": item.role,
            "content": [{"type": kind, "text": item.text_content or ""}],
        }

    def _configuration(self) -> dict[str, Any]:
        model = self._model
        if self._google:
            generation: dict[str, Any] = {
                "responseModalities": ["AUDIO"],
                "speechConfig": {
                    "voiceConfig": {"prebuiltVoiceConfig": {"voiceName": model._voice}}
                },
            }
            if model._temperature is not None:
                generation["temperature"] = model._temperature
            setup: dict[str, Any] = {
                "model": model.model,
                "generationConfig": generation,
                "systemInstruction": {"parts": [{"text": self._instructions}]},
                "inputAudioTranscription": {},
                "outputAudioTranscription": {},
                "tools": self._wire_tools(self._tools),
                "realtimeInputConfig": {
                    "automaticActivityDetection": {"disabled": self._manual_turns},
                    "activityHandling": "START_OF_ACTIVITY_INTERRUPTS"
                    if model._allow_interruptions
                    else "NO_INTERRUPTION",
                },
            }
            if model.model == "gemini-3.8-live-extended-thinking":
                generation["thinkingConfig"] = {"thinkingLevel": "LOW"}
            if self._chat_ctx.items:
                setup["historyConfig"] = {"initialHistoryInClientContent": True}
            return {"setup": setup}
        turn_detection = (
            None
            if self._manual_turns
            else {
                "type": "server_vad",
                "interrupt_response": model._allow_interruptions,
            }
        )
        input_audio: dict[str, Any] = {"format": {"type": "audio/pcm", "rate": 24000}}
        output_audio = {"format": {"type": "audio/pcm", "rate": 24000}}
        session: dict[str, Any] = {
            "instructions": self._instructions,
            "tools": self._wire_tools(self._tools),
            "audio": {"input": input_audio, "output": output_audio},
        }
        if model._protocol == "openai.realtime.v1":
            session.update(type="realtime", output_modalities=["audio"])
            input_audio.update(turn_detection=turn_detection, transcription=None)
            if model._voice:
                output_audio["voice"] = model._voice
        else:
            session["turn_detection"] = turn_detection
            if model._voice:
                session["voice"] = model._voice
            if model._temperature is not None:
                session["temperature"] = model._temperature
        return {"type": "session.update", "session": session}

    def _is_ready(self, event: dict[str, Any], configuration: dict[str, Any]) -> bool:
        if self._google:
            return isinstance(event.get("setupComplete"), dict)
        if event.get("type") != "session.updated" or not isinstance(
            event.get("session"), dict
        ):
            return False

        # The router's adapter has its own initial update. Its acknowledgment
        # cannot release audio ahead of our compiled configuration. Compare the
        # echoed settings, including disabling separately billed OpenAI ASR.
        def contains(actual: Any, requested: Any) -> bool:
            if isinstance(requested, dict):
                return isinstance(actual, dict) and all(
                    key in actual and contains(actual[key], value)
                    for key, value in requested.items()
                )
            if isinstance(requested, list):
                return (
                    isinstance(actual, list)
                    and len(actual) == len(requested)
                    and all(
                        contains(effective, expected)
                        for effective, expected in zip(actual, requested, strict=True)
                    )
                )
            return actual == requested

        return contains(event["session"], configuration["session"])

    @staticmethod
    def _decode(frame: str | bytes) -> dict[str, Any]:
        try:
            event = json.loads(frame)
        except (ValueError, TypeError):
            raise GatewayError("Realtime event is invalid", retryable=False) from None
        if not isinstance(event, dict):
            raise GatewayError("Realtime event is invalid", retryable=False)
        if "error" in event:
            raise _error(event)
        return event

    async def _run(self) -> None:
        self._running = True
        writer = None
        try:
            await self._configured.wait()
            if self._closed:
                return
            if self._failure is not None:
                raise self._failure
            self._check_framework_mode()
            configuration = self._configuration()
            self._socket = await self._model._client.open(
                protocol=self._model._protocol, model=self._model.model
            )
            await self._socket.send_json(configuration)
            events = self._socket.events()

            async def configured() -> None:
                async for frame in events:
                    if self._is_ready(self._decode(frame), configuration):
                        return
                raise GatewayError(
                    "Realtime setup ended before readiness", retryable=False
                )

            await asyncio.wait_for(configured(), self._model._client._setup_timeout)
            for item in self._chat_ctx.items:
                if item.type not in {
                    "message",
                    "function_call",
                    "function_call_output",
                }:
                    continue
                self._known_items.add(item.id)
                if not self._google:
                    await self._socket.send_json(
                        {
                            "type": "conversation.item.create",
                            "item": self._wire_item(item),
                        }
                    )
            if self._google and self._chat_ctx.items:
                turns = []
                for item in self._chat_ctx.items:
                    if not isinstance(item, llm.ChatMessage) or item.role not in {
                        "user",
                        "assistant",
                    }:
                        raise llm.RealtimeError(
                            "Gemini initial history requires user or assistant text"
                        )
                    turns.append(
                        {
                            "role": "model" if item.role == "assistant" else "user",
                            "parts": [{"text": item.text_content or ""}],
                        }
                    )
                await self._socket.send_json(
                    {"clientContent": {"turns": turns, "turnComplete": True}}
                )
            self._ready.set_result(None)

            async def send() -> None:
                async for command in self._commands:
                    try:
                        self._ensure_live()
                        await self._socket.send(command)
                    finally:
                        self._queued_input_bytes = max(
                            0, self._queued_input_bytes - len(command.encode("utf-8"))
                        )

            writer = asyncio.create_task(send())

            async def receive() -> None:
                async for frame in events:
                    event = self._decode(frame)
                    if self._google:
                        await self._google_event(event)
                    else:
                        await self._realtime_event(event)
                if not self._closed:
                    raise GatewayError("Realtime socket ended", retryable=False)

            reader = asyncio.create_task(receive())
            try:
                done, _ = await asyncio.wait(
                    {reader, writer}, return_when=asyncio.FIRST_COMPLETED
                )
                for task in done:
                    task.result()
            finally:
                # Reader cancellation can race an already-completed receive on
                # Python 3.10. Stop physical I/O before joining either task.
                self._socket.abort()
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)
        except asyncio.CancelledError:
            pass
        except Exception as error:  # noqa: BLE001 -- never expose provider payloads to framework logs
            self._failure = (
                error
                if isinstance(error, GatewayError)
                else GatewayError("Realtime session failed", retryable=False)
            )
        finally:
            self._resampler = None
            self._commands.close()
            while not self._commands.empty():
                self._commands.recv_nowait()
            self._queued_input_bytes = 0
            if self._socket is not None:
                self._socket.abort()
            if writer is not None:
                writer.cancel()
                await asyncio.gather(writer, return_exceptions=True)
            if self._socket is not None:
                await self._socket.aclose()
            if self._generation:
                self._generation.close()
            failure = self._failure or GatewayError(
                "Realtime session is closed", retryable=False
            )
            if not self._ready.done():
                self._ready.set_exception(failure)
            for future in self._pending:
                if not future.done():
                    future.set_exception(
                        llm.RealtimeError("Realtime session ended before its reply")
                    )
            for timer in self._pending_timers.values():
                timer.cancel()
            self._pending.clear()
            self._pending_timers.clear()
            self._contextual_futures.clear()
            self._google_interrupted_pending.clear()
            self._provider_retired_futures.clear()
            if self._framework_binding is not None:
                self._framework_binding.release(self)
                self._framework_binding = None
            if self._failure is not None:
                self.emit(
                    "error",
                    llm.RealtimeModelError(
                        timestamp=time.time(),
                        label=self._model.label,
                        error=self._failure,
                        recoverable=False,
                    ),
                )

    def _fatal(self, failure: GatewayError) -> None:
        if self._failure is None:
            self._failure = failure
            self._resampler = None
            self._uncommitted_source_samples = 0
            self._configured.set()
            # Stop physical I/O before cancellation/join. Python 3.10 can lose
            # task cancellation when an awaited producer completes concurrently.
            self._commands.close()
            if self._socket is not None:
                self._socket.abort()
            if self._running:
                self._task.cancel()

    def _begin(self, response_id: str) -> _Generation:
        self._ensure_live()
        if self._generation is not None:
            if not self._generation.interrupted:
                raise GatewayError("Realtime generations overlap", retryable=False)
            self._end()
        pending = self._pending.popleft() if self._pending else None
        if pending is not None:
            self._contextual_futures.discard(pending)
        generation = self._generation = _Generation(response_id, pending is not None)
        if pending is not None:
            self._pending_timers.pop(pending).cancel()
            if not pending.done():
                pending.set_result(generation.event)
        self.emit("generation_created", generation.event)
        return generation

    def _end(self) -> None:
        if self._generation is not None:
            for message in self._generation.messages.values():
                self._known_items.add(message.id)
                self._chat_ctx.items.append(
                    llm.ChatMessage(
                        id=message.id,
                        role="assistant",
                        content=[message.transcript],
                        interrupted=self._generation.interrupted,
                    )
                )
            self._generation.close()
            self._generation = None

    async def _audio(self, message: _Message, encoded: str) -> None:
        try:
            pcm = base64.b64decode(encoded, validate=True)
        except (ValueError, TypeError):
            raise GatewayError("Realtime audio is invalid", retryable=False) from None
        if len(pcm) % 2:
            raise GatewayError("Realtime PCM alignment is invalid", retryable=False)
        # Bounded 20 ms output frames let LiveKit stream and pace playback.
        for offset in range(0, len(pcm), 960):
            if message.interrupted:
                return
            data = pcm[offset : offset + 960]
            try:
                await asyncio.wait_for(
                    message.audio.send(
                        rtc.AudioFrame(
                            data=data,
                            sample_rate=24000,
                            num_channels=1,
                            samples_per_channel=len(data) // 2,
                        )
                    ),
                    5,
                )
            except utils.aio.ChanClosed:
                if message.interrupted:
                    return
                raise GatewayError(
                    "Realtime output closed unexpectedly", retryable=False
                ) from None

    async def _text(self, message: _Message, text: str) -> None:
        if message.interrupted:
            return
        if len(message.transcript) + len(text) > 128 << 10:
            raise GatewayError("Realtime transcript exceeds its bound", retryable=False)
        message.transcript += text
        if text:
            try:
                await asyncio.wait_for(message.text.send(text), 5)
            except utils.aio.ChanClosed:
                if not message.interrupted:
                    raise GatewayError(
                        "Realtime output closed unexpectedly", retryable=False
                    ) from None

    async def _function(self, generation: _Generation, item: dict[str, Any]) -> None:
        if self._unattributed_application_reply:
            raise GatewayError(
                "Tool reply correlation is unavailable after provider interruption",
                retryable=False,
            )
        call_id = item.get("call_id", item.get("id"))
        if not isinstance(call_id, str) or not isinstance(item.get("name"), str):
            raise GatewayError("Realtime function call is invalid", retryable=False)
        if call_id in generation.calls:
            return
        arguments = item.get("arguments", "{}")
        if not isinstance(arguments, str) or len(arguments) > 128 << 10:
            raise GatewayError(
                "Realtime function arguments are invalid", retryable=False
            )
        try:
            parsed = json.loads(arguments)
        except ValueError:
            raise GatewayError(
                "Realtime function arguments are invalid", retryable=False
            ) from None
        if not isinstance(parsed, dict):
            raise GatewayError(
                "Realtime function arguments must be an object", retryable=False
            )
        call = llm.FunctionCall(
            id=item.get("item_id") or item.get("id") or "item_" + uuid.uuid4().hex,
            call_id=call_id,
            name=item["name"],
            arguments=arguments,
        )
        generation.calls.add(call_id)
        self._known_items.add(call.id)
        self._chat_ctx.items.append(call)
        generation.function_stream.send_nowait(call)

    async def _realtime_event(self, event: dict[str, Any]) -> None:
        self._ensure_live()
        kind = event.get("type")
        if kind == "response.created":
            if self._unattributed_application_reply:
                # An unseen server ID, including one followed by a cancelled
                # terminal, cannot identify the retired application request or
                # prove this is a new caller answer. Stop before creating any
                # SDK generation, playback channels or retained history.
                raise GatewayError(
                    "Provider response correlation is unavailable after interruption",
                    retryable=False,
                )
            if not self._manual_turns and not self._pending:
                self._vad_input_seen = True
            self._begin(event["response"]["id"])
            return
        if kind == "input_audio_buffer.speech_started":
            if not self._manual_turns:
                self._vad_input_seen = True
            self._user_started = time.time()
            if self._model._allow_interruptions and self._pending:
                # No response ID exists for this request yet. Retire its real
                # futures, never bind a later unkeyed response to a new ask.
                # Further input/output cannot establish a new caller turn.
                # Refuse it before acceptance; never guess the next server ID.
                self._unattributed_application_reply = True
                for pending in list(self._pending):
                    if not pending.done():
                        self._provider_retired_futures.add(pending)
                    self._retire_pending(pending)
                    if not pending.done():
                        pending.cancel()
            if self._model._allow_interruptions and self._generation:
                self._generation.interrupted = True
                self._generation.close()
            self._provider_interruption_feedback = True
            try:
                self.emit("input_speech_started", llm.InputSpeechStartedEvent())
            finally:
                self._provider_interruption_feedback = False
            return
        if kind == "input_audio_buffer.speech_stopped":
            if not self._manual_turns:
                self._vad_input_seen = True
            self.emit(
                "input_speech_stopped",
                llm.InputSpeechStoppedEvent(user_transcription_enabled=False),
            )
            return
        if kind == "conversation.item.input_audio_transcription.completed":
            if not self._manual_turns:
                self._vad_input_seen = True
                if self._generation is None and any(
                    future in self._contextual_futures for future in self._pending
                ):
                    raise GatewayError(
                        "Provider input overlaps an uncorrelated typed response",
                        retryable=False,
                    )
            self._known_items.add(event["item_id"])
            self._chat_ctx.items.append(
                llm.ChatMessage(
                    id=event["item_id"], role="user", content=[event["transcript"]]
                )
            )
            self.emit(
                "input_audio_transcription_completed",
                _transcription(
                    item_id=event["item_id"],
                    transcript=event["transcript"],
                    is_final=True,
                    turn_started_at=self._user_started,
                ),
            )
            return
        generation = self._generation
        if (
            generation is None
            or (
                event.get("response_id")
                or event.get("response", {}).get("id")
                or generation.id
            )
            != generation.id
        ):
            return
        if kind == "response.done":
            response = event.get("response", {})
            self._end()
            if response.get("status") not in (None, "completed", "cancelled"):
                raise GatewayError("Realtime generation failed", retryable=False)
            return
        if generation.interrupted:
            return
        item_id = event.get("item_id")
        if (
            kind == "response.output_item.added"
            and event.get("item", {}).get("type") == "message"
        ):
            generation.message(event["item"]["id"])
        elif kind in {"response.output_audio.delta", "response.audio.delta"}:
            await self._audio(generation.message(item_id), event["delta"])
        elif kind in {
            "response.output_audio_transcript.delta",
            "response.audio_transcript.delta",
            "response.output_text.delta",
            "response.text.delta",
        }:
            await self._text(generation.message(item_id), event["delta"])
        elif kind in {"response.output_audio.done", "response.audio.done"}:
            generation.message(item_id).audio.close()
        elif kind == "response.function_call_arguments.done":
            await self._function(generation, event)
        elif kind == "response.output_item.done":
            item = event.get("item", {})
            if item.get("type") == "function_call":
                await self._function(generation, item)
            elif item.get("type") == "message":
                generation.message(item["id"]).close()

    async def _google_event(self, event: dict[str, Any]) -> None:
        self._ensure_live()
        if "goAway" in event:
            raise GatewayError("Realtime provider session is ending", retryable=False)
        content = event.get("serverContent", {})
        if "toolCall" in event or "toolCallCancellation" in event:
            raise GatewayError("Gemini tool execution is unavailable", retryable=False)
        if content.get("interrupted"):
            in_flight = (
                self._google_response_pending
                or self._generation is not None
                or bool(self._pending)
            )
            if in_flight:
                self._google_response_interrupted = True
                self._provider_retired_futures.update(
                    pending for pending in self._pending if not pending.done()
                )
                # Keep these real futures ineligible for output. The SDK will
                # cancel them after its synchronous playback feedback; that
                # exact cancellation is provider-caused, not a new local stop.
                self._google_interrupted_pending.update(
                    pending for pending in self._pending if not pending.done()
                )
            if self._generation:
                self._generation.interrupted = True
                self._generation.close()
            # LiveKit owns frames that already left our channels. Its speech
            # event interrupts the active handle and clears that playback too.
            self._provider_interruption_feedback = True
            try:
                self.emit("input_speech_started", llm.InputSpeechStartedEvent())
                self.emit(
                    "input_speech_stopped",
                    llm.InputSpeechStoppedEvent(user_transcription_enabled=True),
                )
            finally:
                self._provider_interruption_feedback = False
        transcript = content.get("inputTranscription")
        if isinstance(transcript, dict) and transcript.get("text"):
            if not self._manual_turns:
                self._vad_input_seen = True
                if self._generation is None and any(
                    future in self._contextual_futures for future in self._pending
                ):
                    raise GatewayError(
                        "Provider input overlaps an uncorrelated typed response",
                        retryable=False,
                    )
            self._input_item = self._input_item or "google_user_" + uuid.uuid4().hex
            self._input_text += transcript["text"]
            if len(self._input_text) > 128 << 10:
                raise GatewayError(
                    "Realtime transcript exceeds its bound", retryable=False
                )
            self.emit(
                "input_audio_transcription_completed",
                _transcription(
                    item_id=self._input_item,
                    transcript=self._input_text,
                    is_final=False,
                ),
            )
        output = content.get("modelTurn")
        spoken = content.get("outputTranscription")
        if (
            (output or spoken)
            and self._generation is None
            and not self._google_response_interrupted
        ):
            if not self._manual_turns and not self._pending:
                self._vad_input_seen = True
            self._google_response_pending = True
            self._begin("google_response_" + uuid.uuid4().hex)
        generation = self._generation
        if generation is not None and not generation.interrupted:
            if spoken and spoken.get("text"):
                await self._text(
                    generation.message(generation.id + "_message"), spoken["text"]
                )
            for part in (output or {}).get("parts", []):
                inline = part.get("inlineData")
                if inline:
                    mime = inline.get("mimeType", "")
                    if mime not in {"audio/pcm;rate=24000", "audio/pcm"}:
                        raise GatewayError(
                            "Realtime output format is invalid", retryable=False
                        )
                    await self._audio(
                        generation.message(generation.id + "_message"), inline["data"]
                    )
        if content.get("turnComplete"):
            if self._input_item:
                # The provider has already consumed this audio. LiveKit echoes
                # the emitted item back in context updates; mark its exact ID
                # before callbacks so it cannot be resent as new typed input.
                self._known_items.add(self._input_item)
                self._chat_ctx.items.append(
                    llm.ChatMessage(
                        id=self._input_item, role="user", content=[self._input_text]
                    )
                )
                self.emit(
                    "input_audio_transcription_completed",
                    _transcription(
                        item_id=self._input_item,
                        transcript=self._input_text,
                        is_final=True,
                        turn_started_at=self._user_started,
                    ),
                )
                self._input_item, self._input_text = "", ""
            self._end()
            # generationComplete, interruption feedback, local playback and
            # a completed Future do not establish an idle provider. Extended
            # thinking explicitly requires its documented IDLE status.
            status = content.get("interactionStatus")
            if status == "IDLE" or (
                status is None
                and self._model.model != "gemini-3.8-live-extended-thinking"
            ):
                self._google_response_pending = False
                self._google_response_interrupted = False
                self._committed = False
                for pending in list(self._pending):
                    self._retire_pending(pending)
                    if not pending.done():
                        pending.set_exception(
                            llm.RealtimeError("Gemini turn ended without a reply")
                        )

    async def aclose(self) -> None:
        self._closed = True
        self._resampler = None
        self._uncommitted_source_samples = 0
        self._configured.set()
        self._commands.close()
        if self._socket is not None:
            self._socket.abort()
        if self._running:
            self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)
        if not self._ready.done():
            self._ready.set_exception(
                GatewayError("Realtime session is closed", retryable=False)
            )
        for timer in self._pending_timers.values():
            timer.cancel()
        for future in self._pending:
            if not future.done():
                future.set_exception(llm.RealtimeError("Realtime session is closed"))
        self._pending.clear()
        self._pending_timers.clear()
        self._contextual_futures.clear()
        self._google_interrupted_pending.clear()
        self._provider_retired_futures.clear()
        if self._framework_binding is not None:
            self._framework_binding.release(self)
            self._framework_binding = None
