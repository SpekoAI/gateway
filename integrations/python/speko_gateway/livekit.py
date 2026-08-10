"""LiveKit Agents STT, TTS, and LLM implementations backed by Speko.

STT and TTS stream through the colocated local Speko Gateway socket. LLM
talks HTTPS to the hosted Speko relay (`relay.speko.dev`) — a different
trust boundary: the conversation content travels to the relay.
"""

from __future__ import annotations

import asyncio
import weakref
from typing import Any

import aiohttp
from livekit.agents import (
    DEFAULT_API_CONNECT_OPTIONS,
    NOT_GIVEN,
    APIConnectionError,
    APIConnectOptions,
    NotGivenOr,
    llm,
    stt,
    tts,
    utils,
)
from livekit.agents.llm.tool_context import get_raw_function_info
from livekit.agents.utils import is_given

from ._livekit_bridge import (
    LiveKitSpeechEvent,
    LiveKitSTTBridge,
    LiveKitSTTStream,
    LiveKitTTSBridge,
    LiveKitTTSStream,
)
from .client import GatewayClient, GatewayError
from .relay import RelayError, RelayLLMClient


class STT(stt.STT):
    """Stream speech through a colocated Speko Gateway."""

    def __init__(
        self,
        client: GatewayClient | None = None,
        *,
        language: str = "en",
        model: str = "nova-3",
        sample_rate: int = 16_000,
    ) -> None:
        super().__init__(
            capabilities=stt.STTCapabilities(
                streaming=True,
                interim_results=True,
                diarization=False,
                aligned_transcript=False,
                offline_recognize=True,
                keyterms=False,
                chat_context=False,
            )
        )
        self._client = client or GatewayClient.from_env()
        self._owns_client = client is None
        self._language = language
        self._model = model
        self._sample_rate = sample_rate
        self._streams: weakref.WeakSet[SpeechStream] = weakref.WeakSet()

    @property
    def model(self) -> str:
        return self._model

    @property
    def provider(self) -> str:
        return "speko"

    async def _recognize_impl(
        self,
        buffer: Any,
        *,
        language: NotGivenOr[str] = NOT_GIVEN,
        conn_options: APIConnectOptions,
    ) -> stt.SpeechEvent:
        stream = self.stream(language=language, conn_options=conn_options)
        try:
            frames = buffer if isinstance(buffer, list) else [buffer]
            for frame in frames:
                stream.push_frame(frame)
            stream.end_input()
            final: stt.SpeechEvent | None = None
            async for event in stream:
                if event.type == stt.SpeechEventType.FINAL_TRANSCRIPT:
                    final = event
            if final is None:
                raise APIConnectionError("Speko STT ended without a final transcript")
            return final
        finally:
            await stream.aclose()

    def stream(
        self,
        *,
        language: NotGivenOr[str] = NOT_GIVEN,
        conn_options: APIConnectOptions = DEFAULT_API_CONNECT_OPTIONS,
    ) -> SpeechStream:
        resolved_language = str(language) if is_given(language) else self._language
        bridge = LiveKitSTTBridge(
            self._client,
            language=resolved_language,
            model=self._model,
        )
        stream = SpeechStream(
            stt_instance=self,
            bridge=bridge,
            language=resolved_language,
            conn_options=conn_options,
            sample_rate=self._sample_rate,
        )
        self._streams.add(stream)
        return stream

    async def aclose(self) -> None:
        for stream in list(self._streams):
            await stream.aclose()
        self._streams.clear()
        if self._owns_client:
            await self._client.aclose()


class SpeechStream(stt.RecognizeStream):
    def __init__(
        self,
        *,
        stt_instance: STT,
        bridge: LiveKitSTTBridge,
        language: str,
        conn_options: APIConnectOptions,
        sample_rate: int,
    ) -> None:
        super().__init__(
            stt=stt_instance,
            conn_options=conn_options,
            sample_rate=sample_rate,
        )
        self._bridge = bridge
        self._language = language
        self._request_id = utils.shortuuid()
        self._gateway_stream: LiveKitSTTStream | None = None
        self._speaking = False

    async def _run(self) -> None:
        first_frame = None
        async for item in self._input_ch:
            if isinstance(item, self._FlushSentinel):
                continue
            first_frame = item
            break
        if first_frame is None:
            return

        try:
            self._gateway_stream = await self._bridge.start(first_frame)
            await self._gateway_stream.push_frame(first_frame)
            send_task = asyncio.create_task(
                self._send_audio(), name="speko.livekit.stt.send"
            )
            receive_task = asyncio.create_task(
                self._receive_events(), name="speko.livekit.stt.receive"
            )
            try:
                done, _ = await asyncio.wait(
                    (send_task, receive_task), return_when=asyncio.FIRST_COMPLETED
                )
                for task in done:
                    task.result()
                if receive_task not in done:
                    await receive_task
            finally:
                await utils.aio.cancel_and_wait(send_task, receive_task)
        except asyncio.CancelledError:
            raise
        except GatewayError as err:
            classification = "/".join(
                value for value in (err.source, err.code) if value
            )
            suffix = f" ({classification})" if classification else ""
            raise APIConnectionError(
                f"Speko Gateway STT failed{suffix}", retryable=err.retryable
            ) from err
        except OSError as err:
            raise APIConnectionError("Speko Gateway STT transport failed") from err
        finally:
            if self._gateway_stream is not None:
                try:
                    await asyncio.shield(self._gateway_stream.aclose())
                except (GatewayError, OSError):
                    pass

    async def _send_audio(self) -> None:
        assert self._gateway_stream is not None
        last_was_flush = False
        async for item in self._input_ch:
            if isinstance(item, self._FlushSentinel):
                if not last_was_flush:
                    await self._gateway_stream.flush()
                last_was_flush = True
            else:
                await self._gateway_stream.push_frame(item)
                last_was_flush = False
        if not last_was_flush:
            await self._gateway_stream.flush()
        await self._gateway_stream.finish()

    async def _receive_events(self) -> None:
        assert self._gateway_stream is not None
        async for event in self._gateway_stream.events():
            self._emit(event)
        if self._speaking:
            self._emit_end()

    def _emit(self, event: LiveKitSpeechEvent) -> None:
        if event.provider_request_id:
            self._request_id = event.provider_request_id
        if event.type == "speech.started":
            if not self._speaking:
                self._speaking = True
                self._event_ch.send_nowait(
                    stt.SpeechEvent(
                        type=stt.SpeechEventType.START_OF_SPEECH,
                        request_id=self._request_id,
                    )
                )
            return
        if event.type == "speech.ended":
            self._emit_end()
            return
        if event.type not in {"transcript.delta", "transcript.final"} or not event.text:
            return
        if not self._speaking:
            self._speaking = True
            self._event_ch.send_nowait(
                stt.SpeechEvent(
                    type=stt.SpeechEventType.START_OF_SPEECH,
                    request_id=self._request_id,
                )
            )
        self._event_ch.send_nowait(
            stt.SpeechEvent(
                type=(
                    stt.SpeechEventType.FINAL_TRANSCRIPT
                    if event.type == "transcript.final"
                    else stt.SpeechEventType.INTERIM_TRANSCRIPT
                ),
                request_id=self._request_id,
                alternatives=[
                    stt.SpeechData(
                        language=self._language,
                        text=event.text,
                        start_time=event.start_time or 0.0,
                        end_time=event.end_time or 0.0,
                    )
                ],
            )
        )

    def _emit_end(self) -> None:
        if not self._speaking:
            return
        self._speaking = False
        self._event_ch.send_nowait(
            stt.SpeechEvent(
                type=stt.SpeechEventType.END_OF_SPEECH,
                request_id=self._request_id,
            )
        )


class TTS(tts.TTS):
    """Synthesize speech through a colocated Speko Gateway."""

    def __init__(
        self,
        client: GatewayClient | None = None,
        *,
        voice: str = "",
        language: str = "en",
        model: str = "auto",
        provider: str = "auto",
        sample_rate: int = 24_000,
        max_input_characters: int = 100_000,
    ) -> None:
        super().__init__(
            capabilities=tts.TTSCapabilities(streaming=True),
            sample_rate=sample_rate,
            num_channels=1,
        )
        self._client = client or GatewayClient.from_env()
        self._owns_client = client is None
        self._voice = voice
        self._language = language
        self._model = model
        self._provider_name = provider
        self._max_input_characters = max_input_characters
        self._streams: weakref.WeakSet[SynthesisStream] = weakref.WeakSet()

    @property
    def model(self) -> str:
        return self._model

    @property
    def provider(self) -> str:
        return "speko"

    def synthesize(
        self,
        text: str,
        *,
        conn_options: APIConnectOptions = DEFAULT_API_CONNECT_OPTIONS,
    ) -> tts.ChunkedStream:
        return self._synthesize_with_stream(text, conn_options=conn_options)

    def stream(
        self,
        *,
        conn_options: APIConnectOptions = DEFAULT_API_CONNECT_OPTIONS,
    ) -> SynthesisStream:
        bridge = LiveKitTTSBridge(
            self._client,
            voice=self._voice,
            language=self._language,
            model=self._model,
            provider=self._provider_name,
            sample_rate=self.sample_rate,
            num_channels=self.num_channels,
            max_input_characters=self._max_input_characters,
        )
        stream = SynthesisStream(
            tts_instance=self,
            bridge=bridge,
            conn_options=conn_options,
        )
        self._streams.add(stream)
        return stream

    async def aclose(self) -> None:
        for stream in list(self._streams):
            await stream.aclose()
        self._streams.clear()
        if self._owns_client:
            await self._client.aclose()


class SynthesisStream(tts.SynthesizeStream):
    def __init__(
        self,
        *,
        tts_instance: TTS,
        bridge: LiveKitTTSBridge,
        conn_options: APIConnectOptions,
    ) -> None:
        super().__init__(tts=tts_instance, conn_options=conn_options)
        self._bridge = bridge
        self._gateway_stream: LiveKitTTSStream | None = None

    async def _run(self, output_emitter: tts.AudioEmitter) -> None:
        # The emitter must be initialized even when no text ever arrives:
        # the base class calls end_input() on it unconditionally after _run
        # returns, and an uninitialized emitter raises there. Only the
        # gateway dial waits for the first token.
        output_emitter.initialize(
            request_id=utils.shortuuid(),
            sample_rate=self._tts.sample_rate,
            num_channels=self._tts.num_channels,
            mime_type="audio/pcm",
            stream=True,
        )
        first_token: str | None = None
        async for item in self._input_ch:
            if isinstance(item, self._FlushSentinel):
                continue
            first_token = item
            break
        if first_token is None:
            return

        try:
            self._gateway_stream = await self._bridge.start()
            self._mark_started()
            await self._gateway_stream.append_text(first_token)
            send_task = asyncio.create_task(
                self._send_text(), name="speko.livekit.tts.send"
            )
            receive_task = asyncio.create_task(
                self._receive_audio(output_emitter), name="speko.livekit.tts.receive"
            )
            try:
                done, _ = await asyncio.wait(
                    (send_task, receive_task), return_when=asyncio.FIRST_COMPLETED
                )
                for task in done:
                    task.result()
                if receive_task not in done:
                    await receive_task
            finally:
                await utils.aio.cancel_and_wait(send_task, receive_task)
        except asyncio.CancelledError:
            raise
        except GatewayError as err:
            classification = "/".join(
                value for value in (err.source, err.code) if value
            )
            suffix = f" ({classification})" if classification else ""
            raise APIConnectionError(
                f"Speko Gateway TTS failed{suffix}", retryable=err.retryable
            ) from err
        except OSError as err:
            raise APIConnectionError("Speko Gateway TTS transport failed") from err
        finally:
            if self._gateway_stream is not None:
                try:
                    await asyncio.shield(self._gateway_stream.aclose())
                except (GatewayError, OSError):
                    pass

    async def _send_text(self) -> None:
        assert self._gateway_stream is not None
        pending = True
        async for item in self._input_ch:
            if isinstance(item, self._FlushSentinel):
                if pending:
                    await self._gateway_stream.commit()
                pending = False
            else:
                await self._gateway_stream.append_text(item)
                pending = True
        if pending:
            await self._gateway_stream.commit()
        await self._gateway_stream.finish()

    async def _receive_audio(self, output_emitter: tts.AudioEmitter) -> None:
        assert self._gateway_stream is not None
        # LiveKit synthesizes one segment per stream instance, so audio after
        # the segment's audio.done is ignored rather than reopening a segment
        # the emitter would count as a mismatch.
        segment_open = False
        segment_done = False
        async for event in self._gateway_stream.events():
            if segment_done:
                continue
            if event.type == "audio.started":
                if not segment_open:
                    segment_open = True
                    output_emitter.start_segment(
                        segment_id=event.provider_request_id or utils.shortuuid()
                    )
                continue
            if event.type == "audio.frame" and event.audio is not None:
                if not segment_open:
                    segment_open = True
                    output_emitter.start_segment(segment_id=utils.shortuuid())
                output_emitter.push(event.audio)
                continue
            if event.type == "audio.done" and segment_open:
                segment_open = False
                segment_done = True
                output_emitter.end_segment()


class LLM(llm.LLM):
    """Generate responses through the hosted Speko relay."""

    def __init__(
        self,
        client: RelayLLMClient | None = None,
        *,
        provider: str = "auto",
        model: str = "auto",
        objective: str = "balanced",
        max_output_tokens: int = 8_192,
        temperature: float | None = None,
    ) -> None:
        super().__init__()
        if (provider == "auto") != (model == "auto"):
            raise ValueError("provider and model must both be auto or both be explicit")
        self._client = client or RelayLLMClient.from_env()
        self._owns_client = client is None
        self._provider_name = provider
        self._model = model
        self._objective = objective
        self._max_output_tokens = max_output_tokens
        self._temperature = temperature

    @property
    def model(self) -> str:
        return self._model

    @property
    def provider(self) -> str:
        return "speko"

    def chat(
        self,
        *,
        chat_ctx: llm.ChatContext,
        tools: list[llm.Tool] | None = None,
        conn_options: APIConnectOptions = DEFAULT_API_CONNECT_OPTIONS,
        parallel_tool_calls: NotGivenOr[bool] = NOT_GIVEN,
        tool_choice: NotGivenOr[Any] = NOT_GIVEN,
        extra_kwargs: NotGivenOr[dict[str, Any]] = NOT_GIVEN,
    ) -> RelayLLMStream:
        return RelayLLMStream(
            llm_instance=self,
            client=self._client,
            chat_ctx=chat_ctx,
            tools=tools or [],
            conn_options=conn_options,
        )

    def _request_body(
        self, chat_ctx: llm.ChatContext, tools: list[llm.Tool]
    ) -> dict[str, Any]:
        if self._provider_name == "auto":
            routing: dict[str, Any] = {
                "mode": "auto",
                "objective": self._objective,
            }
        else:
            routing = {
                "mode": "explicit",
                "provider": self._provider_name,
                "model": self._model,
            }
        body: dict[str, Any] = {
            "routing": routing,
            "input": _relay_input(chat_ctx),
            "max_output_tokens": self._max_output_tokens,
        }
        if tools:
            body["tools"] = _relay_tools(tools)
        if self._temperature is not None:
            body["temperature"] = self._temperature
        return body

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()
        await super().aclose()


class RelayLLMStream(llm.LLMStream):
    def __init__(
        self,
        *,
        llm_instance: LLM,
        client: RelayLLMClient,
        chat_ctx: llm.ChatContext,
        tools: list[llm.Tool],
        conn_options: APIConnectOptions,
    ) -> None:
        super().__init__(
            llm_instance,
            chat_ctx=chat_ctx,
            tools=tools,
            conn_options=conn_options,
        )
        self._relay_llm = llm_instance
        self._client = client

    async def _run(self) -> None:
        request = self._relay_llm._request_body(self._chat_ctx, self._tools)
        if not request["input"]:
            raise APIConnectionError(
                "Speko relay LLM requires at least one text message in the"
                " chat context",
                retryable=False,
            )
        response_id = utils.shortuuid()
        events = self._client.stream_response(request)
        try:
            async for event, payload in events:
                if event == "response.created":
                    response_id = str(payload.get("response_id", response_id))
                    continue
                if event == "response.text.delta":
                    delta = str(payload.get("delta", ""))
                    if delta:
                        self._event_ch.send_nowait(
                            llm.ChatChunk(
                                id=response_id,
                                delta=llm.ChoiceDelta(role="assistant", content=delta),
                            )
                        )
                    continue
                if event == "response.item.completed":
                    item = payload.get("item")
                    if isinstance(item, dict) and item.get("type") == "function_call":
                        self._event_ch.send_nowait(
                            llm.ChatChunk(
                                id=response_id,
                                delta=llm.ChoiceDelta(
                                    role="assistant",
                                    tool_calls=[
                                        llm.FunctionToolCall(
                                            name=str(item.get("name", "")),
                                            arguments=str(
                                                item.get("arguments") or "{}"
                                            ),
                                            call_id=str(item.get("call_id", "")),
                                        )
                                    ],
                                ),
                            )
                        )
                    continue
                if event == "response.completed":
                    usage = payload.get("usage")
                    if isinstance(usage, dict):
                        self._event_ch.send_nowait(
                            llm.ChatChunk(
                                id=response_id, usage=_completion_usage(usage)
                            )
                        )
        except RelayError as err:
            suffix = f" ({err.code})" if err.code else ""
            raise APIConnectionError(
                f"Speko relay LLM failed{suffix}", retryable=err.retryable
            ) from err
        except (aiohttp.ClientError, OSError) as err:
            raise APIConnectionError("Speko relay LLM transport failed") from err
        finally:
            await events.aclose()


def _relay_input(chat_ctx: llm.ChatContext) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for item in chat_ctx.items:
        if item.type == "message":
            role = "system" if item.role == "developer" else item.role
            if role not in ("system", "user", "assistant"):
                continue
            text = item.text_content
            if not text:
                continue
            items.append(
                {
                    "type": "message",
                    "role": role,
                    "content": [{"type": "text", "text": text}],
                }
            )
        elif item.type == "function_call":
            items.append(
                {
                    "type": "function_call",
                    "call_id": item.call_id,
                    "name": item.name,
                    "arguments": item.arguments or "{}",
                }
            )
        elif item.type == "function_call_output":
            entry: dict[str, Any] = {
                "type": "function_result",
                "call_id": item.call_id,
            }
            if item.output:
                entry["result"] = item.output
            items.append(entry)
    return items


def _relay_tools(tools: list[llm.Tool]) -> list[dict[str, Any]]:
    declarations: list[dict[str, Any]] = []
    for tool in tools:
        if llm.is_function_tool(tool):
            schema = llm.utils.build_legacy_openai_schema(tool, internally_tagged=True)
            raw: dict[str, Any] = {
                "name": schema["name"],
                "description": schema.get("description") or "",
                "parameters": schema.get("parameters"),
            }
        elif llm.is_raw_function_tool(tool):
            info = get_raw_function_info(tool)
            raw = {
                "name": info.raw_schema.get("name", info.name),
                "description": info.raw_schema.get("description") or "",
                "parameters": info.raw_schema.get("parameters"),
            }
        else:
            raise ValueError(
                "Speko relay LLM supports function tools only; got "
                f"{type(tool).__name__}"
            )
        declaration: dict[str, Any] = {"name": raw["name"]}
        if raw["description"]:
            declaration["description"] = raw["description"]
        if raw["parameters"] is not None:
            declaration["parameters"] = raw["parameters"]
        declarations.append(declaration)
    return declarations


def _completion_usage(usage: dict[str, Any]) -> llm.CompletionUsage:
    input_tokens = int(usage.get("input_tokens", 0))
    cached = int(usage.get("cached_input_tokens", 0))
    output_tokens = int(usage.get("output_tokens", 0))
    reasoning = int(usage.get("reasoning_tokens", 0))
    prompt_tokens = input_tokens + cached
    completion_tokens = output_tokens + reasoning
    return llm.CompletionUsage(
        completion_tokens=completion_tokens,
        prompt_tokens=prompt_tokens,
        prompt_cached_tokens=cached,
        total_tokens=prompt_tokens + completion_tokens,
    )
