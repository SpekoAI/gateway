"""Full duplex GPT-Live on one native relay attempt, with explicit client delegation.

The caller owns both scoped clients and the separately selected Speko backend.
Native transcript deltas stay fragments; paid STT supplies canonical user turns.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import inspect
import json
import re
import time
import weakref
from collections.abc import AsyncIterable
from typing import Any

from livekit import rtc
from livekit.agents import NOT_GIVEN, NotGivenOr, llm, utils
from livekit.agents.utils import is_given
from livekit.agents.utils.aio.channel import ChanFull

from ._livekit_context import snapshot_context
from ._livekit_tools import _relay_tools
from ._relay_transport import _error
from .client import GatewayError
from .relay import RelayLLMClient
from .relay_realtime import RelayRealtimeClient, RelayRealtimeSession

# Older supported frameworks can still import the existing speech integrations.
_Model = getattr(llm, "DuplexModel", object)
_Session = getattr(llm, "DuplexSession", object)
_TEXT_BYTES = 128 << 10
_ID = re.compile(r"[!-~]{1,200}")
_SCOPED_REPLY = "GPT-Live per-reply instructions require provider correlation"


class _GPTReplyBinding:
    """Own the existing SDK reply-task seam; no native/worker authority."""

    def __init__(self, activity: Any, session: _GPTLiveRealtimeSession) -> None:
        from livekit.agents.voice.agent_activity import AgentActivity

        original = getattr(activity, "_realtime_reply_task", None)
        original_context = activity.update_chat_ctx
        if (
            activity.llm is not session.realtime_model
            or getattr(activity, "_rt_session", None) is not session
            or not inspect.ismethod(original)
            or original.__func__ is not AgentActivity._realtime_reply_task
            or not {"speech_handle", "instructions", "user_input"}.issubset(
                inspect.signature(original).parameters
            )
            or not inspect.ismethod(original_context)
            or original_context.__func__ is not AgentActivity.update_chat_ctx
            or not isinstance(activity._rt_chat_ctx_lock, asyncio.Lock)
        ):
            raise llm.RealtimeError("GPT-Live SDK reply ownership is unavailable")
        self.activity = weakref.ref(activity)
        self.session = weakref.ref(session)
        self.original = original
        self.original_context = original_context

        async def context(
            chat_ctx: llm.ChatContext, *, exclude_invalid_function_calls: bool = True
        ) -> None:
            from livekit.agents.voice.generation import remove_instructions

            self.check()
            current = self.session()
            owner = self.activity()
            assert current is not None and owner is not None
            changed = current._changes_user_context(chat_ctx)
            if changed and (
                current.duplex_session._typed_context_unsafe
                or owner._rt_chat_ctx_lock.locked()
            ):
                # The actual SDK assigns Agent history before acquiring this
                # lock. Refuse before that assignment, not after a native error.
                raise GatewayError(
                    "GPT-Live typed context requires delegation correlation",
                    retryable=False,
                )
            staged = snapshot_context(chat_ctx)
            await current.start()
            async with owner._rt_chat_ctx_lock:
                self.check()
                filtered = snapshot_context(
                    staged.copy(
                        tools=owner.tools
                        if exclude_invalid_function_calls
                        else NOT_GIVEN
                    )
                )
                remove_instructions(filtered)
                await current.update_chat_ctx(filtered)
                owner._agent._chat_ctx = snapshot_context(filtered)

        self.observed_context = context
        activity.update_chat_ctx = context

        async def reply(*args: Any, **options: Any) -> None:
            current = self.session()
            try:
                self.check()
                assert current is not None
                if options.get("instructions") is not None:
                    raise llm.RealtimeError(_SCOPED_REPLY)
                if (
                    is_given(options.get("user_input", NOT_GIVEN))
                    and options.get("user_input") is not None
                    and current.duplex_session._typed_context_unsafe
                ):
                    raise llm.RealtimeError(
                        "GPT-Live typed reply requires delegation correlation"
                    )
            except (llm.RealtimeError, GatewayError) as failure:
                # Preserve the real SpeechHandle's failure semantics. This
                # task has not entered the SDK's typed append/history path.
                options["speech_handle"]._mark_done(error=failure)
                return
            assert current is not None
            await self.original(*args, **options)

        self.observed = reply
        activity._realtime_reply_task = reply

    def check(self) -> None:
        activity, session = self.activity(), self.session()
        if (
            activity is None
            or session is None
            or activity._realtime_reply_task is not self.observed
            or activity.update_chat_ctx is not self.observed_context
            or activity._rt_session is not session
            or session._reply_binding is not self
            or session.duplex_session._closing
            or session.duplex_session._failure is not None
        ):
            # RealtimeError is best-effort during SDK context updates.
            raise GatewayError(
                "GPT-Live SDK reply owner is unavailable", retryable=False
            )

    def release(self) -> None:
        activity = self.activity()
        if activity is not None and activity._realtime_reply_task is self.observed:
            activity._realtime_reply_task = self.original
        if activity is not None and activity.update_chat_ctx is self.observed_context:
            activity.update_chat_ctx = self.original_context


def _failure() -> GatewayError:
    return GatewayError("Relay GPT-Live session failed", retryable=False)


def _text(value: Any, maximum: int = _TEXT_BYTES) -> str:
    if not isinstance(value, str) or len(value.encode("utf-8")) > maximum:
        raise _failure()
    return value


def _parts(text: str):
    # A UTF-8 byte bound also bounds byte-level tokenizer tokens, without
    # assuming an English characters/token ratio or cutting a Unicode scalar.
    part, size = [], 0
    for character in text:
        width = len(character.encode("utf-8"))
        if size + width > 500:
            yield "".join(part)
            part, size = [], 0
        part.append(character)
        size += width
    if part:
        yield "".join(part)


def _result_digest(item) -> bytes:
    value = [_text(item.output), item.name, item.is_error, item.reply_required]
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False).encode("utf-8")
    ).digest()


class _GPTLiveDuplexModel(_Model):
    """Full-duplex voice plus a separately configured native Speko backend.

    Requires a framework with the public DuplexModel interface (LiveKit 1.8.10
    or a compatible release). Neither client can read vendor/environment keys.
    """

    def __init__(
        self,
        client: RelayRealtimeClient,
        *,
        model: str,
        voice: str,
        backend: Any,
    ) -> None:
        if _Model is object:
            raise ValueError("GPT-Live requires LiveKit's DuplexModel interface")
        # Import lazily: livekit.py exports this class alongside its LLM adapter.
        from .livekit import LLM

        if (
            not isinstance(backend, LLM)
            or not isinstance(backend._client, RelayLLMClient)
            or not backend._client._native_socket
            or backend._provider_name == "auto"
            or backend._model == "auto"
            or not isinstance(backend._provider_name, str)
            or not re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", backend._provider_name
            )
            or not isinstance(backend._model, str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}", backend._model)
            or not isinstance(backend._max_output_tokens, int)
            or isinstance(backend._max_output_tokens, bool)
            or not 0 < backend._max_output_tokens <= 0xFFFFFFFF
            or backend._client._platform_session_id != client._platform_session_id
        ):
            raise ValueError(
                "GPT-Live needs a concrete bounded session-scoped Speko backend"
            )
        if (
            not isinstance(model, str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", model)
            or model == "auto"
            or not isinstance(voice, str)
            or not voice
            or len(voice.encode("utf-8")) > 200
        ):
            raise ValueError("GPT-Live needs a concrete compiled model and voice")
        super().__init__(
            capabilities=llm.DuplexCapabilities(
                user_transcription=False,
                auto_tool_reply_generation=True,
                mutable_chat_context=False,
                mutable_instructions=False,
                mutable_tools=True,
            )
        )
        self._client, self._backend = client, backend
        self._model, self._voice = model, voice
        self._used = self._closed = False
        self._sessions: weakref.WeakSet[_GPTLiveSession] = weakref.WeakSet()

    @property
    def model(self) -> str:
        return self._model

    @property
    def provider(self) -> str:
        return "openai"

    def audio_gate(self):
        # The installed native provider implementation declares this same
        # silence floor and pause duration; segmentation stays in LiveKit.
        return llm.FixedGate(0.0006, min_silence_duration=0.8)

    def session(self):
        if self._closed or self._used:
            raise GatewayError("GPT-Live model attempt is exhausted", retryable=False)
        self._used = True
        session = _GPTLiveSession(self)
        self._sessions.add(session)
        return session

    async def aclose(self) -> None:
        self._closed = True
        await asyncio.gather(*(session.aclose() for session in self._sessions))


class _GPTLiveSession(_Session):
    def __init__(self, model: _GPTLiveDuplexModel) -> None:
        super().__init__(model)
        self._model = model
        self._instructions = ""
        self._history = llm.ChatContext.empty()
        self._tools = llm.ToolContext.empty()
        self._audio = utils.aio.Chan(maxsize=256)
        self._commands = utils.aio.Chan(maxsize=256)
        self._delegations = utils.aio.Chan(maxsize=16)
        self._seen_delegations: set[str] = set()
        self._tool_results: dict[str, asyncio.Future] = {}
        self._completed_results: dict[str, bytes] = {}
        self._fragments: list[dict[str, Any]] = []
        self._fragment_bytes = 0
        self._pending_reply_hint: str | None = None
        self._pending_reply_future: asyncio.Future | None = None
        self._pending_reply_timer: asyncio.TimerHandle | None = None
        # A negative capability fence, not an acknowledgement or source proof.
        # Retirement, preambles and claimed IDs do not drain unseen delegates.
        self._reply_hint_issued = False
        # Ordinary input can also leave an unseen old delegation. A first
        # explicit hint is not proof that it precedes all provider work.
        self._live_input_seen = False
        self._socket: RelayRealtimeSession | None = None
        self._resampler: rtc.AudioResampler | None = None
        self._input_format: tuple[int, int] | None = None
        self._started = False
        self._ready = asyncio.get_running_loop().create_future()
        self._remote_closed = asyncio.Event()
        self._failure: GatewayError | None = None
        self._running = False
        self._task = asyncio.create_task(self._run(), name="SpekoGPTLive")

    @property
    def audio_stream(self) -> AsyncIterable:
        return self._audio

    @property
    def tools(self):
        return self._tools.copy()

    @property
    def _typed_context_unsafe(self) -> bool:
        return self._reply_hint_issued or self._live_input_seen

    async def start(self) -> None:
        await asyncio.shield(self._ready)

    def _queue(self, command: dict[str, Any]) -> None:
        if self._failure is not None:
            raise self._failure
        if self._closing:
            raise _failure()
        try:
            self._commands.send_nowait(command)
        except (ChanFull, utils.aio.ChanClosed):
            self._fatal()
            raise _failure() from None

    def push_audio(self, frame: rtc.AudioFrame) -> None:
        if frame.samples_per_channel:
            self._live_input_seen = True
            self._retire_pending_reply()
        current = frame.sample_rate, frame.num_channels
        if frame.num_channels != 1:
            raise ValueError("GPT-Live input must be mono PCM")
        if current != self._input_format:
            if self._input_format is not None:
                raise ValueError("GPT-Live input format cannot change within a session")
            self._input_format = current
            if frame.sample_rate != 24000:
                self._resampler = rtc.AudioResampler(
                    input_rate=frame.sample_rate, output_rate=24000, num_channels=1
                )
        frames = self._resampler.push(frame) if self._resampler else [frame]
        for output in frames:
            self._queue(
                {
                    "type": "session.input_audio.append",
                    "audio": base64.b64encode(output.data).decode("ascii"),
                }
            )

    def _append(self, kind: str, text: str, delegation_id: str | None = None) -> None:
        for part in _parts(_text(text)):
            self._queue({"type": kind, "delegation_id": delegation_id, "content": part})

    async def _update_instructions(self, instructions: str) -> None:
        instructions = _text(instructions)
        if self._started and instructions != self._instructions:
            raise llm.RealtimeError("GPT-Live startup instructions are immutable")
        self._instructions = instructions

    async def _update_tools(self, tools: list[llm.Tool]) -> None:
        if len(tools) > 128:
            raise ValueError("GPT-Live tool count exceeds its bound")
        _relay_tools(tools)  # no provider-hosted tools or silent omissions
        self._tools = llm.ToolContext(tools)

    def _update_options(self, *, tool_choice: NotGivenOr = NOT_GIVEN) -> None:
        if is_given(tool_choice) and tool_choice not in (None, "auto"):
            raise llm.RealtimeError(
                "GPT-Live does not support a per-response tool choice"
            )

    async def _append_items(self, items: list[llm.ChatItem]) -> None:
        if self._typed_context_unsafe and any(
            isinstance(item, llm.ChatMessage) and item.role == "user" for item in items
        ):
            # Preflight the whole batch before history, voice append or tool
            # results. RealtimeError here is best-effort in the SDK.
            raise GatewayError(
                "GPT-Live typed context requires delegation correlation",
                retryable=False,
            )
        if len(self._history.items) + len(items) > 256:
            raise llm.RealtimeError("GPT-Live history exceeds its bound")
        for item in items:
            if isinstance(item, llm.FunctionCallOutput):
                digest = _result_digest(item)
                future = self._tool_results.get(item.call_id)
                if future is not None:
                    if not future.done():
                        future.set_result(item.model_copy(deep=True))
                    elif _result_digest(future.result()) != digest:
                        raise llm.RealtimeError(
                            "GPT-Live received conflicting tool results"
                        )
                    continue
                if self._completed_results.get(item.call_id) == digest:
                    continue
                raise llm.RealtimeError(
                    "GPT-Live received an unknown or conflicting tool result"
                )
            self._remember([item])
            if isinstance(item, llm.ChatMessage) and item.role == "user":
                self._retire_pending_reply()
            if self._started and isinstance(item, llm.ChatMessage):
                text = item.text_content
                if text:
                    self._live_input_seen = True
                    self._append(
                        "session.instructions.append"
                        if item.role in {"system", "developer"}
                        else "session.thinking.append",
                        f"{item.role}: {text}",
                    )

    def _remember(self, items) -> None:
        combined = self._history.items + items
        if (
            len(combined) > 256
            or sum(len(item.model_dump_json().encode("utf-8")) for item in combined)
            > _TEXT_BYTES
        ):
            raise llm.RealtimeError("GPT-Live history exceeds its bound")
        self._history.insert(snapshot_context(llm.ChatContext(list(items))).items)

    def _backend_context(self) -> llm.ChatContext:
        context = snapshot_context(self._history)
        if self._instructions:
            context.items.insert(
                0, llm.ChatMessage(role="system", content=[self._instructions])
            )
        return context

    def _generate_reply(
        self, *, instructions=NOT_GIVEN, tool_choice=NOT_GIVEN, tools=NOT_GIVEN
    ) -> None:
        if is_given(instructions) and instructions is not None:
            # The protocol carries no per-ask identity/drain barrier. Reject
            # scoped tasks BEFORE append instead of running an ambiguous or
            # silently downgraded backend task (including on idle PCM).
            raise llm.RealtimeError(_SCOPED_REPLY)
        pending = self._pending_reply_future
        if pending is not None and pending.cancelled():
            self._retire_pending_reply(pending)
        if self._pending_reply_hint is not None:
            raise llm.RealtimeError("GPT-Live has an undelivered reply hint")
        self._update_options(tool_choice=tool_choice)
        if is_given(tools) and tools != self._tools.flatten():
            raise llm.RealtimeError("GPT-Live does not support per-response tools")
        # A delegation may precede any transcript or use an older offset. Only
        # canonical context independent of that future offset can make this
        # unscoped ask usable. Never insert the delivery hint as fake context.
        if not self._model._backend._request_body(
            self._backend_context(), self._tools.flatten()
        )["input"]:
            raise llm.RealtimeError("GPT-Live reply requires canonical backend context")
        ask = "Reply to the caller now. Do not wait for the caller to speak first. After that, pause and listen."
        # This is only an unscoped voice-delivery hint. It contributes no task
        # instructions or privileges to any backend context/tool round.
        ask = _text(ask)
        self._retire_pending_reply()
        self._append("session.commentary.append", ask)
        self._reply_hint_issued = True
        self._pending_reply_hint = ask

    def _retire_pending_reply(self, future: asyncio.Future | None = None) -> None:
        if future is not None and self._pending_reply_future is not future:
            return
        pending = self._pending_reply_future
        if self._pending_reply_timer is not None and (
            pending is None or pending.done()
        ):
            self._pending_reply_timer.cancel()
        self._pending_reply_hint = None
        self._pending_reply_future = None
        self._pending_reply_timer = None

    def _startup(self) -> dict[str, Any]:
        history = []
        for item in self._history.items:
            if not isinstance(item, llm.ChatMessage) or not item.text_content:
                raise llm.RealtimeError(
                    "GPT-Live startup history requires text messages"
                )
            history.append(
                {
                    "type": "message",
                    "role": item.role,
                    "content": [
                        {
                            "type": "output_text"
                            if item.role == "assistant"
                            else "input_text",
                            "text": _text(item.text_content),
                        }
                    ],
                }
            )
        if len(json.dumps(history).encode("utf-8")) > _TEXT_BYTES:
            raise llm.RealtimeError("GPT-Live startup history exceeds its bound")
        return {
            "type": "session.start",
            "session": {
                "model": self._model.model,
                "instructions": self._instructions,
                "input": history,
                "audio": {
                    "format": {"type": "audio/pcm", "rate": 24000},
                    "output": {"voice": self._model._voice},
                },
                "delegation": {"type": "client"},
            },
        }

    async def _event(self, event: dict[str, Any]) -> None:
        kind = event.get("type")
        if not self._ready.done() and kind not in {"session.started", "error"}:
            raise _failure()
        if kind not in {"session.started", "session.closed", "error"}:
            self._live_input_seen = True
        if kind == "session.started":
            if self._ready.done():
                raise _failure()
            self._ready.set_result(None)
        elif kind == "session.output_audio.delta":
            pcm = base64.b64decode(_text(event.get("delta"), 2 << 20), validate=True)
            if not pcm or len(pcm) % 2:
                raise _failure()
            # Small frames let the framework place transcripts on streamed audio.
            for offset in range(0, len(pcm), 960):
                data = pcm[offset : offset + 960]
                await self._audio.send(
                    llm.DuplexAudioFrame(
                        frame=rtc.AudioFrame(data, 24000, 1, len(data) // 2)
                    )
                )
        elif kind in {
            "session.input_transcript.delta",
            "session.output_transcript.delta",
        }:
            text = _text(event.get("delta"))
            if kind == "session.input_transcript.delta" and text:
                # Input supersedes the unscoped delivery hint. Transcript
                # fragments never claim a task or fabricate a complete turn.
                self._retire_pending_reply()
            start, end = event.get("start_ms"), event.get("end_ms")
            if (
                any(
                    not isinstance(v, int) or isinstance(v, bool) or v < 0
                    for v in (start, end)
                )
                or end < start
            ):
                raise _failure()
            self._fragment_bytes += len(text.encode("utf-8"))
            if self._fragment_bytes > _TEXT_BYTES or len(self._fragments) >= 2048:
                raise _failure()
            self._fragments.append(
                {
                    "role": "user"
                    if kind == "session.input_transcript.delta"
                    else "assistant",
                    "text": text,
                    "start_ms": start,
                    "end_ms": end,
                }
            )
            if kind == "session.output_transcript.delta":
                self.emit(
                    "transcript_delta",
                    llm.DuplexOutputTranscriptDelta(
                        text=text, start_ms=start, end_ms=end
                    ),
                )
        elif kind == "session.delegation.created":
            delegation = event.get("delegation", {})
            identity, offset = delegation.get("id"), event.get("offset_ms")
            if (
                delegation.get("target") != "client"
                or not isinstance(identity, str)
                or not _ID.fullmatch(identity)
                or not isinstance(offset, int)
                or isinstance(offset, bool)
                or offset < 0
            ):
                raise _failure()
            if identity in self._seen_delegations:
                return
            if len(self._seen_delegations) >= 128:
                raise _failure()
            self._seen_delegations.add(identity)
            context = self._backend_context()
            pending = self._pending_reply_future
            if pending is not None and pending.cancelled():
                self._retire_pending_reply(pending)
            if self._pending_reply_hint is not None:
                self._retire_pending_reply()
            fragments = [f for f in self._fragments if f["end_ms"] <= offset]
            if fragments:
                context.add_message(
                    role="user",
                    content="Live speech fragments in delivery order (not complete turns):\n"
                    + json.dumps(fragments, ensure_ascii=False),
                )
            self._delegations.send_nowait((identity, context, self._tools.flatten()))
        elif kind == "session.closed":
            self._remote_closed.set()
            if not self._closing:
                raise _failure()
        elif kind == "error":
            raise _error(event)

    async def _backend_run(self, identity, context, tools) -> None:
        for _ in range(16):
            text, calls = "", []
            # Native Speko LLM disables framework retries at its actual transport.
            async with self._model._backend.chat(
                chat_ctx=context, tools=tools
            ) as stream:
                async for chunk in stream:
                    if chunk.delta is None:
                        continue
                    if chunk.delta.content:
                        text = _text(text + chunk.delta.content)
                    calls.extend(chunk.delta.tool_calls)
                    if len(calls) > 128:
                        raise _failure()
            if not calls:
                if not text:
                    raise _failure()
                self._append("session.commentary.append", text, identity)
                return
            functions = []
            for call in calls:
                if (
                    not _ID.fullmatch(call.call_id)
                    or call.call_id in self._tool_results
                    or call.call_id in self._completed_results
                    or len(self._completed_results) + len(self._tool_results) >= 2048
                ):
                    raise _failure()
                _text(call.arguments)
                function = llm.FunctionCall(
                    call_id=call.call_id, name=call.name, arguments=call.arguments
                )
                self._tool_results[call.call_id] = (
                    asyncio.get_running_loop().create_future()
                )
                functions.append(function)
            context.insert(functions)
            for function in functions:
                self.emit("function_call", function)
            results = await asyncio.gather(
                *(self._tool_results[f.call_id] for f in functions)
            )
            context.insert(results)
            self._remember(functions + results)
            for result in results:
                self._completed_results[result.call_id] = _result_digest(result)
                self._tool_results.pop(result.call_id)
            if not any(result.reply_required for result in results):
                self._append(
                    "session.thinking.append",
                    json.dumps(
                        [result.output for result in results], ensure_ascii=False
                    ),
                    identity,
                )
                return
        raise _failure()

    async def _run(self) -> None:
        self._running = True
        tasks: list[asyncio.Task] = []
        try:
            await self._configured.wait()
            if self._closing:
                return
            if self._failure is not None:
                raise self._failure
            startup = self._startup()
            self._started = True
            self._socket = await self._model._client.open(
                protocol="openai.live.v1", model=self._model.model
            )
            await self._socket.send_json(startup)

            async def receive():
                async for frame in self._socket.events():
                    if not isinstance(frame, str):
                        raise _failure()
                    event = json.loads(frame)
                    if not isinstance(event, dict):
                        raise _failure()
                    await self._event(event)
                if not self._closing:
                    raise _failure()

            async def send():
                await asyncio.wait_for(
                    asyncio.shield(self._ready), self._model._client._setup_timeout
                )
                async for command in self._commands:
                    await self._socket.send_json(command)

            async def delegate():
                await asyncio.shield(self._ready)
                async for identity, context, tools in self._delegations:
                    await asyncio.wait_for(
                        self._backend_run(identity, context, tools), 120
                    )

            tasks = [asyncio.create_task(fn()) for fn in (receive, send, delegate)]
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        except asyncio.CancelledError:
            pass
        except Exception:  # noqa: BLE001 -- vendor content never enters logs/errors
            self._failure = self._failure or _failure()
        finally:
            self._retire_pending_reply()
            # Python 3.10 wait_for can lose cancellation when its awaited
            # future completes concurrently. Fence physical I/O and wake idle
            # producers before joining, including that ready/error race.
            self._commands.close()
            self._delegations.close()
            if self._socket is not None:
                self._socket.abort()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._audio.close()
            for future in self._tool_results.values():
                if not future.done():
                    future.cancel()
            if not self._ready.done():
                self._ready.set_exception(self._failure or _failure())
                self._ready.exception()  # safe when framework never called start()
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
            if self._socket is not None:
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(self._socket.aclose(), 1)

    def _fatal(self) -> None:
        if self._failure is None:
            self._failure = _failure()
            self._configured.set()
            if self._running:
                self._task.cancel()

    async def aclose(self) -> None:
        if self._closing:
            await asyncio.gather(self._task, return_exceptions=True)
            return
        await super().aclose()
        if self._socket is not None and self._ready.done() and self._failure is None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(
                    self._socket.send_json({"type": "session.close"}), 1
                )
                await asyncio.wait_for(self._remote_closed.wait(), 1)
        if self._running:
            self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)


_Adapter = getattr(llm, "DuplexRealtimeAdapter", object)


class GPTLiveModel(_Adapter):
    """Native GPT-Live with explicit Speko backend and duplex audio segmentation.

    A tool-only first generation satisfies an explicit reply request, just as
    it does for the other realtime protocols. This lets AgentSession execute
    the tool before waiting for the voice answer; otherwise both wait forever.
    """

    def __init__(
        self, client: RelayRealtimeClient, *, model: str, voice: str, backend: Any
    ) -> None:
        if _Adapter is object:
            raise ValueError("GPT-Live requires LiveKit's DuplexModel interface")
        super().__init__(
            _GPTLiveDuplexModel(client, model=model, voice=voice, backend=backend)
        )

    def session(self, *, turn_detection_disabled: bool = False):
        return _GPTLiveRealtimeSession(
            self, super().session(turn_detection_disabled=turn_detection_disabled)
        )


class _GPTLiveRealtimeSession(llm.RealtimeSession):
    def __init__(self, model: GPTLiveModel, inner: llm.RealtimeSession):
        super().__init__(model)
        self._inner = inner
        self._reply_binding: _GPTReplyBinding | None = None
        self._reply: asyncio.Future | None = None
        self._reply_timeout = 15.0
        inner.on("generation_created", self._generation)
        inner.on("error", self._error)
        for event in (
            "input_speech_started",
            "input_speech_stopped",
            "input_audio_transcription_completed",
            "session_reconnected",
            "metrics_collected",
            "remote_item_added",
        ):
            inner.on(event, lambda value, event=event: self.emit(event, value))

    def on(self, event, callback=None):
        if callback is None:
            return lambda selected: self.on(event, selected)
        from livekit.agents.voice.agent_activity import AgentActivity

        owner = getattr(callback, "__self__", None)
        try:
            if isinstance(owner, AgentActivity):
                if (
                    self._reply_binding is None
                    or self._reply_binding.activity() is not owner
                ):
                    if self._reply_binding is not None:
                        self._reply_binding.release()
                    self._reply_binding = _GPTReplyBinding(owner, self)
            elif isinstance(owner, llm.RealtimeSession) and owner is not self:
                raise llm.RealtimeError(
                    "GPT-Live wrapper reply ownership is unavailable"
                )
        except (llm.RealtimeError, GatewayError):
            self.duplex_session._fatal()
            raise
        return super().on(event, callback)

    @property
    def duplex_session(self):
        return self._inner.duplex_session

    @property
    def chat_ctx(self):
        return snapshot_context(self._inner.chat_ctx)

    @property
    def tools(self):
        return self._inner.tools

    def _generation(self, generation):
        if self._reply is not None and not self._reply.done():
            # A spoken preamble can fulfil this delivery future before the
            # backend work arrives. Hint expiration is independent.
            generation.user_initiated = True
            self._reply.set_result(generation)
            self._reply = None
        self.emit("generation_created", generation)

    def _fail_reply(self):
        self.duplex_session._retire_pending_reply()
        if self._reply is not None and not self._reply.done():
            self._reply.set_exception(
                llm.RealtimeError("GPT-Live ended before its reply")
            )
        self._reply = None

    def _error(self, error):
        self._fail_reply()
        self.emit("error", error)

    def generate_reply(
        self, *, instructions=NOT_GIVEN, tool_choice=NOT_GIVEN, tools=NOT_GIVEN
    ):
        future = asyncio.get_running_loop().create_future()
        if self._reply is not None and not self._reply.done():
            future.set_exception(llm.RealtimeError("GPT-Live reply is already pending"))
            return future
        try:
            self.duplex_session._generate_reply(
                instructions=instructions, tool_choice=tool_choice, tools=tools
            )
        except (llm.RealtimeError, GatewayError) as error:
            future.set_exception(error)
            return future

        self._reply = future
        self.duplex_session._pending_reply_future = future

        def expire():
            self.duplex_session._retire_pending_reply(future)
            if self._reply is future:
                if not future.done():
                    future.set_exception(llm.RealtimeError("GPT-Live reply timed out"))
                self._reply = None

        timer = asyncio.get_running_loop().call_later(self._reply_timeout, expire)
        self.duplex_session._pending_reply_timer = timer

        def delivered(done):
            if done.cancelled():
                self.duplex_session._retire_pending_reply(done)
                if self._reply is done:
                    self._reply = None
            # A preamble keeps the delivery hint's original expiration alive.
            # Cancellation callbacks retire only their own timer.
            if (
                done.cancelled()
                or self.duplex_session._pending_reply_future is not done
            ):
                timer.cancel()

        future.add_done_callback(delivered)
        return future

    async def start(self):
        await self.duplex_session.start()

    async def _update_session(self, **options):
        if is_given(options.get("chat_ctx", NOT_GIVEN)):
            options["chat_ctx"] = snapshot_context(options["chat_ctx"])
        await self._inner._update_session(**options)

    async def update_instructions(self, instructions):
        await self._inner.update_instructions(instructions)

    async def update_chat_ctx(self, chat_ctx):
        if self._reply_binding is not None:
            try:
                self._reply_binding.check()
            except GatewayError:
                self.duplex_session._fatal()
                raise
        if self.duplex_session._typed_context_unsafe and self._changes_user_context(
            chat_ctx
        ):
            raise GatewayError(
                "GPT-Live typed context requires delegation correlation",
                retryable=False,
            )
        await self._inner.update_chat_ctx(snapshot_context(chat_ctx))

    def _changes_user_context(self, chat_ctx: llm.ChatContext) -> bool:
        previous = {
            item.id: item
            for item in self.chat_ctx.items
            if isinstance(item, llm.ChatMessage) and item.role == "user"
        }
        return any(
            isinstance(item, llm.ChatMessage)
            and item.role == "user"
            and previous.get(item.id) != item
            for item in chat_ctx.items
        )

    async def update_tools(self, tools):
        await self._inner.update_tools(tools)

    def update_options(self, *, tool_choice=NOT_GIVEN):
        self._inner.update_options(tool_choice=tool_choice)

    def push_audio(self, frame):
        self._inner.push_audio(frame)

    def start_user_activity(self):
        self.duplex_session._retire_pending_reply()
        self._inner.start_user_activity()

    def push_video(self, frame):
        self._inner.push_video(frame)

    def commit_audio(self):
        self._inner.commit_audio()

    def clear_audio(self):
        self._inner.clear_audio()

    def interrupt(self):
        self.duplex_session._retire_pending_reply()
        self._inner.interrupt()

    def truncate(self, **options):
        self._inner.truncate(**options)

    async def aclose(self):
        self._fail_reply()
        try:
            await self._inner.aclose()
        finally:
            if self._reply_binding is not None:
                self._reply_binding.release()
                self._reply_binding = None
