"""LiveKit native speech-to-speech; every socket uses one scoped relay attempt."""

from __future__ import annotations

import asyncio
import base64
import dataclasses
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

from ._livekit_tools import _relay_tools
from ._relay_transport import _error
from .client import GatewayError
from .relay_realtime import RealtimeProtocol, RelayRealtimeClient, RelayRealtimeSession

_PROVIDERS = {
    "openai.realtime.v1": "openai",
    "xai.realtime.v1": "xai",
    "google.live.v1": "google",
}


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
        self._used = True
        session = _RealtimeSession(self, turn_detection_disabled)
        self._sessions.add(session)
        return session

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
        self._commands = utils.aio.Chan[dict[str, Any]](maxsize=256)
        self._socket: RelayRealtimeSession | None = None
        self._generation: _Generation | None = None
        self._pending: deque[asyncio.Future[llm.GenerationCreatedEvent]] = deque()
        self._pending_timers: dict[asyncio.Future, asyncio.TimerHandle] = {}
        self._resampler: rtc.AudioResampler | None = None
        self._input_rate: int | None = None
        self._input_hz = 16000 if self._google else 24000
        self._audio_active = self._committed = False
        self._closed = False
        self._failure: GatewayError | None = None
        self._input_text = ""
        self._input_item = ""
        self._user_started: float | None = None
        self._running = False
        self._task = asyncio.create_task(self._run())

    @staticmethod
    def _observe_future(future: asyncio.Future) -> None:
        if not future.cancelled():
            future.exception()

    @property
    def chat_ctx(self) -> llm.ChatContext:
        return self._chat_ctx.copy()

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
        if not self._configured.is_set():
            self._instructions = instructions
            return
        if self._google:
            raise llm.RealtimeError("Gemini instructions are immutable after setup")
        await self._ready
        self._instructions = instructions
        self._enqueue(
            {"type": "session.update", "session": {"instructions": instructions}}
        )

    async def update_tools(self, tools: list[llm.Tool]) -> None:
        context = llm.ToolContext(tools)
        _relay_tools(context.flatten())
        if self._configured.is_set():
            if self._google:
                raise llm.RealtimeError("Gemini tools are immutable after setup")
            await self._ready
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
            self._chat_ctx = chat_ctx.copy()
            return
        await self._ready
        items = [
            item
            for item in chat_ctx.items
            if item.type in {"message", "function_call", "function_call_output"}
        ]
        if self._google:
            results = [
                item
                for item in items
                if isinstance(item, llm.FunctionCallOutput)
                and item.id not in self._known_items
            ]
            if results:
                self._enqueue(
                    {
                        "toolResponse": {
                            "functionResponses": [
                                {
                                    "id": item.call_id,
                                    "name": item.name,
                                    "response": {"result": item.output},
                                }
                                for item in results
                            ]
                        }
                    }
                )
                self._known_items.update(item.id for item in results)
            unexpected = [item for item in items if item.id not in self._known_items]
            if unexpected:
                raise llm.RealtimeError(
                    "Gemini context updates require native tool results"
                )
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
        self._chat_ctx = chat_ctx.copy()

    def update_options(
        self, *, tool_choice: NotGivenOr[llm.ToolChoice | None] = NOT_GIVEN
    ) -> None:
        if not is_given(tool_choice):
            return
        if self._google and tool_choice not in (None, "auto"):
            raise llm.RealtimeError("Gemini per-response tool choice is unavailable")
        self._tool_choice = tool_choice

    def _enqueue(self, event: dict[str, Any]) -> None:
        if self._closed or self._failure is not None:
            raise llm.RealtimeError("Realtime session is closed")
        try:
            self._commands.send_nowait(event)
        except ChanFull:
            self._fatal(
                GatewayError("Realtime input exceeds its bound", retryable=False)
            )

    def start_user_activity(self) -> None:
        self._user_started = time.time()
        if self._google and self._manual_turns and not self._audio_active:
            self._enqueue({"realtimeInput": {"activityStart": {}}})
            self._audio_active = True

    def push_audio(self, frame: rtc.AudioFrame) -> None:
        if frame.num_channels != 1:
            raise ValueError("Realtime input must be mono PCM16")
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
        for converted in self._resampler.push(frame) if self._resampler else [frame]:
            self._send_audio(bytes(converted.data))

    def _send_audio(self, pcm: bytes) -> None:
        if not pcm:
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

    def commit_audio(self) -> None:
        if self._resampler:
            for frame in self._resampler.flush():
                self._send_audio(bytes(frame.data))
        if self._google:
            if self._manual_turns and self._audio_active:
                self._enqueue({"realtimeInput": {"activityEnd": {}}})
                self._audio_active = False
                self._committed = True
        else:
            self._enqueue({"type": "input_audio_buffer.commit"})

    def clear_audio(self) -> None:
        if not self._google:
            self._enqueue({"type": "input_audio_buffer.clear"})

    def generate_reply(
        self,
        *,
        instructions: NotGivenOr[str] = NOT_GIVEN,
        tool_choice: NotGivenOr[llm.ToolChoice] = NOT_GIVEN,
        tools: NotGivenOr[list[llm.Tool]] = NOT_GIVEN,
    ) -> asyncio.Future[llm.GenerationCreatedEvent]:
        future = asyncio.get_running_loop().create_future()
        future.add_done_callback(self._observe_future)
        if self._closed or self._failure is not None:
            future.set_exception(llm.RealtimeError("Realtime session is closed"))
            return future
        if self._pending:
            future.set_exception(
                llm.RealtimeError("A realtime reply is already pending")
            )
            return future
        self._configured.set()
        self._pending.append(future)
        self._pending_timers[future] = asyncio.get_running_loop().call_later(
            self._model._client._setup_timeout, self._reply_timeout, future
        )
        if self._google:
            if is_given(tools) or is_given(tool_choice) and tool_choice != "auto":
                self._reply_timeout(future)
                return future
            if self._committed and not is_given(instructions):
                self._committed = False
            else:
                content: dict[str, Any] = {"turnComplete": True}
                if is_given(instructions):
                    content["turns"] = [
                        {"role": "user", "parts": [{"text": instructions}]}
                    ]
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
        if not future.done():
            future.set_exception(
                llm.RealtimeError("Realtime reply was not acknowledged")
            )
            self._fatal(
                GatewayError("Realtime reply was not acknowledged", retryable=False)
            )

    def interrupt(self) -> None:
        if self._generation is not None and not self._generation.interrupted:
            self._generation.interrupted = True
            self._generation.close()
            if not self._google:
                self._enqueue(
                    {"type": "response.cancel", "response_id": self._generation.id}
                )
        if self._google and self._manual_turns:
            self.start_user_activity()

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
            return (
                [
                    {
                        "functionDeclarations": [
                            {
                                "name": item["name"],
                                "description": item.get("description", ""),
                                "parametersJsonSchema": item.get(
                                    "parameters", {"type": "object"}
                                ),
                            }
                            for item in declarations
                        ]
                    }
                ]
                if declarations
                else []
            )
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
            self._socket = await self._model._client.open(
                protocol=self._model._protocol, model=self._model.model
            )
            configuration = self._configuration()
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
                    {"clientContent": {"turns": turns, "turnComplete": False}}
                )
            self._ready.set_result(None)

            async def send() -> None:
                async for command in self._commands:
                    await self._socket.send_json(command)

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
            if writer is not None:
                writer.cancel()
                await asyncio.gather(writer, return_exceptions=True)
            if self._socket is not None:
                await self._socket.aclose()
            self._commands.close()
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
            self._configured.set()
            if self._running:
                self._task.cancel()

    def _begin(self, response_id: str) -> _Generation:
        if self._generation is not None:
            if not self._generation.interrupted:
                raise GatewayError("Realtime generations overlap", retryable=False)
            self._end()
        pending = self._pending.popleft() if self._pending else None
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
        kind = event.get("type")
        if kind == "response.created":
            self._begin(event["response"]["id"])
            return
        if kind == "input_audio_buffer.speech_started":
            self._user_started = time.time()
            if self._model._allow_interruptions and self._generation:
                self._generation.interrupted = True
                self._generation.close()
            self.emit("input_speech_started", llm.InputSpeechStartedEvent())
            return
        if kind == "input_audio_buffer.speech_stopped":
            self.emit(
                "input_speech_stopped",
                llm.InputSpeechStoppedEvent(user_transcription_enabled=False),
            )
            return
        if kind == "conversation.item.input_audio_transcription.completed":
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
        if "goAway" in event:
            raise GatewayError("Realtime provider session is ending", retryable=False)
        content = event.get("serverContent", {})
        if content.get("interrupted") and self._generation:
            self._generation.interrupted = True
            self._generation.close()
        transcript = content.get("inputTranscription")
        if isinstance(transcript, dict) and transcript.get("text"):
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
        calls = event.get("toolCall", {}).get("functionCalls", [])
        if (output or spoken or calls) and self._generation is None:
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
            for call in calls:
                await self._function(
                    generation,
                    {
                        "id": call["id"],
                        "name": call["name"],
                        "arguments": json.dumps(call.get("args", {}), allow_nan=False),
                    },
                )
        if content.get("turnComplete"):
            if self._input_item:
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
        elif calls:
            # A complete native tool-call batch delegates control to LiveKit.
            # Waiting for turnComplete here would deadlock before toolResponse.
            self._end()

    async def aclose(self) -> None:
        self._closed = True
        self._configured.set()
        if self._running:
            self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)
        self._commands.close()
        if not self._ready.done():
            self._ready.set_exception(
                GatewayError("Realtime session is closed", retryable=False)
            )
        for timer in self._pending_timers.values():
            timer.cancel()
        for future in self._pending:
            if not future.done():
                future.set_exception(llm.RealtimeError("Realtime session is closed"))
