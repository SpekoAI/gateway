"""Native Pipecat STT, LLM, and TTS services backed by Speko."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from pipecat.frames.frames import (
    CancelFrame,
    EndFrame,
    ErrorFrame,
    Frame,
    InterimTranscriptionFrame,
    LLMContextFrame,
    LLMFullResponseEndFrame,
    LLMFullResponseStartFrame,
    StartFrame,
    TranscriptionFrame,
    TTSAudioRawFrame,
    TTSStoppedFrame,
    VADUserStoppedSpeakingFrame,
)
from pipecat.metrics.metrics import LLMTokenUsage
from pipecat.processors.aggregators.llm_context import LLMContext, is_given
from pipecat.processors.frame_processor import FrameDirection
from pipecat.services.llm_service import FunctionCallFromLLM, LLMService
from pipecat.services.settings import LLMSettings, STTSettings, TTSSettings
from pipecat.services.stt_service import STTService as PipecatSTTService
from pipecat.services.tts_service import TTSService as PipecatTTSService
from pipecat.transcriptions.language import Language
from pipecat.utils.time import time_now_iso8601
from pipecat.utils.tracing.service_decorators import traced_llm

from ._voice import CredentialSource, execution_from_env, stt_options_payload
from .client import (
    CanonicalEvent,
    GatewayClient,
    GatewayError,
    GatewaySession,
    SessionConfig,
)
from .relay import RelayError, RelayLLMClient

_INTEGRATION_VERSION = "0.1.0"


class SpekoSTTService(PipecatSTTService):
    """Stream Pipecat input audio through a Speko Gateway STT session."""

    def __init__(
        self,
        client: GatewayClient | None = None,
        *,
        language: str = "en",
        model: str = "auto",
        provider: str = "auto",
        credential_source: CredentialSource = "auto",
        sample_rate: int | None = None,
        num_channels: int = 1,
        diarization: bool | None = None,
        keywords: Sequence[str] | None = None,
        noise_reduction: bool | None = None,
        provider_options: Mapping[str, Mapping[str, Any]] | None = None,
        ready_timeout: float = 15.0,
        session_id: str = "",
        **kwargs: Any,
    ) -> None:
        super().__init__(
            sample_rate=sample_rate,
            settings=STTSettings(model=model, language=language),
            **kwargs,
        )
        if num_channels < 1:
            raise ValueError("num_channels must be positive")
        self._client = client or GatewayClient.from_env()
        self._owns_client = client is None
        self._language = language
        self._model = model
        self._provider = provider
        self._credential_source = credential_source
        self._num_channels = num_channels
        self._ready_timeout = ready_timeout
        self._platform_session_id = session_id
        self._stt_options = stt_options_payload(
            diarization=diarization,
            keywords=keywords,
            noise_reduction=noise_reduction,
            provider_options=provider_options,
        )
        self._session: GatewaySession | None = None
        self._receive_task: asyncio.Task[None] | None = None
        self._audio_since_commit = False

    async def start(self, frame: StartFrame) -> None:
        await super().start(frame)
        try:
            await self._connect()
        except (GatewayError, OSError) as error:
            await self.push_error(
                _gateway_failure("STT", error), exception=error, fatal=True
            )

    async def stop(self, frame: EndFrame) -> None:
        await self._finish(graceful=True)
        await super().stop(frame)

    async def cancel(self, frame: CancelFrame) -> None:
        await self._finish(graceful=False)
        await super().cancel(frame)

    async def cleanup(self) -> None:
        await self._finish(graceful=False)
        if self._owns_client:
            await self._client.aclose()
        await super().cleanup()

    async def process_frame(self, frame: Frame, direction: FrameDirection) -> None:
        # The Gateway's explicit audio.commit must precede Pipecat's VAD-stop
        # frame downstream, otherwise a turn aggregator can wait for a final
        # transcript that the provider has not yet been asked to flush.
        if isinstance(frame, VADUserStoppedSpeakingFrame):
            await self._commit_audio()
        await super().process_frame(frame, direction)

    async def run_stt(self, audio: bytes) -> AsyncGenerator[Frame | None, None]:
        if self._session is None:
            yield ErrorFrame(
                error="Speko Gateway STT session is not connected",
                fatal=True,
                processor=self,
            )
            return
        try:
            await self._session.send_audio(audio)
            self._audio_since_commit = True
            yield None
        except (GatewayError, OSError) as error:
            yield ErrorFrame(
                error=_gateway_failure("STT", error),
                fatal=True,
                processor=self,
                exception=error,
            )

    async def _connect(self) -> None:
        if self._session is not None:
            return
        await self._client.wait_until_ready(timeout=self._ready_timeout)
        request: dict[str, Any] = {
            "provider": self._provider,
            "language": self._language,
            "model": self._model,
        }
        if self._platform_session_id:
            request["client_session_id"] = self._platform_session_id
        if self._stt_options:
            request["stt"] = self._stt_options
        self._session = await self._client.open(
            SessionConfig(
                kind="stt",
                execution=execution_from_env(self._credential_source),
                request=request,
                media={
                    "encoding": "pcm_s16le",
                    "sample_rate_hz": self.sample_rate,
                    "channels": self._num_channels,
                },
                integration={
                    "name": "pipecat-python",
                    "version": _INTEGRATION_VERSION,
                    "transport": "pipecat",
                },
            )
        )
        self._receive_task = asyncio.create_task(
            self._receive_events(), name="speko.pipecat.stt.receive"
        )

    async def _commit_audio(self) -> None:
        if self._session is not None and self._audio_since_commit:
            await self._session.commit_audio()
            self._audio_since_commit = False

    async def _receive_events(self) -> None:
        assert self._session is not None
        try:
            async for event in self._session.events():
                if event.type == "error":
                    raise _stream_error(event)
                frame = _transcription_frame(
                    event,
                    user_id=self._user_id,
                    language=self._language,
                )
                if frame is not None:
                    if isinstance(frame, TranscriptionFrame):
                        await self.emit_stt_usage_metrics()
                    await self.push_frame(frame)
        except asyncio.CancelledError:
            raise
        except (GatewayError, OSError) as error:
            await self.push_error(
                _gateway_failure("STT", error), exception=error, fatal=True
            )

    async def _finish(self, *, graceful: bool) -> None:
        session = self._session
        task = self._receive_task
        if session is None:
            return
        try:
            if graceful:
                if self._audio_since_commit:
                    await session.commit_audio()
                    self._audio_since_commit = False
                await session.finish()
                if task is not None:
                    try:
                        await asyncio.wait_for(task, timeout=5.0)
                    except TimeoutError:
                        pass
            else:
                try:
                    await session.cancel()
                except (GatewayError, OSError):
                    pass
        finally:
            self._session = None
            self._receive_task = None
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            try:
                await session.aclose()
            except (GatewayError, OSError, RuntimeError):
                pass


@dataclass
class _TTSContextState:
    session: GatewaySession
    task: asyncio.Task[None] | None = None
    finishing: bool = False
    interrupted: bool = False


class SpekoTTSService(PipecatTTSService):
    """Stream Pipecat text through one Speko Gateway session per bot turn."""

    def __init__(
        self,
        client: GatewayClient | None = None,
        *,
        voice: str = "",
        language: str = "en",
        model: str = "auto",
        provider: str = "auto",
        credential_source: CredentialSource = "auto",
        sample_rate: int | None = None,
        num_channels: int = 1,
        max_input_characters: int = 100_000,
        ready_timeout: float = 15.0,
        session_id: str = "",
        **kwargs: Any,
    ) -> None:
        kwargs.setdefault("push_start_frame", True)
        kwargs.setdefault("push_stop_frames", False)
        kwargs.setdefault("stop_frame_timeout_s", 15.0)
        super().__init__(
            sample_rate=sample_rate,
            settings=TTSSettings(model=model, voice=voice, language=language),
            **kwargs,
        )
        if num_channels < 1:
            raise ValueError("num_channels must be positive")
        self._client = client or GatewayClient.from_env()
        self._owns_client = client is None
        self._voice = voice
        self._language = language
        self._model = model
        self._provider = provider
        self._credential_source = credential_source
        self._num_channels = num_channels
        self._max_input_characters = max_input_characters
        self._ready_timeout = ready_timeout
        self._platform_session_id = session_id
        self._contexts: dict[str, _TTSContextState] = {}
        self._ready = False

    @property
    def supports_processing_metrics(self) -> bool:
        # Audio arrives on background Gateway receiver tasks after run_tts
        # returns; TTFB and TTFA are the meaningful latency metrics.
        return False

    async def start(self, frame: StartFrame) -> None:
        await super().start(frame)
        await self._ensure_ready()

    async def stop(self, frame: EndFrame) -> None:
        await self._finish_all(interrupted=False)
        await super().stop(frame)

    async def cancel(self, frame: CancelFrame) -> None:
        await self._finish_all(interrupted=True)
        await super().cancel(frame)

    async def cleanup(self) -> None:
        await self._finish_all(interrupted=True)
        if self._owns_client:
            await self._client.aclose()
        await super().cleanup()

    async def run_tts(
        self, text: str, context_id: str
    ) -> AsyncGenerator[Frame | None, None]:
        state: _TTSContextState | None = None
        try:
            state = await self._context(context_id)
            await state.session.append_text(text)
            # Commit each Pipecat sentence so synthesis starts while the LLM is
            # still producing the remainder of the response.
            await state.session.commit_text()
            await self.start_tts_usage_metrics(text)
            yield None
        except (GatewayError, OSError) as error:
            if state is not None:
                await self._close_state(context_id, state, interrupted=True)
            yield ErrorFrame(
                error=_gateway_failure("TTS", error),
                fatal=True,
                processor=self,
                exception=error,
            )
            yield TTSStoppedFrame(context_id=context_id)
            await self.remove_audio_context(context_id)

    async def flush_audio(self, context_id: str | None = None) -> None:
        context_id = context_id or self.get_active_audio_context_id()
        if not context_id:
            return
        state = self._contexts.get(context_id)
        if state is None or state.finishing:
            return
        state.finishing = True
        await state.session.finish()

    async def on_audio_context_interrupted(self, context_id: str) -> None:
        state = self._contexts.get(context_id)
        if state is not None:
            await self._close_state(context_id, state, interrupted=True)

    async def _ensure_ready(self) -> None:
        if not self._ready:
            await self._client.wait_until_ready(timeout=self._ready_timeout)
            self._ready = True

    async def _context(self, context_id: str) -> _TTSContextState:
        state = self._contexts.get(context_id)
        if state is not None:
            return state
        await self._ensure_ready()
        request: dict[str, Any] = {
            "provider": self._provider,
            "language": self._language,
            "model": self._model,
            "max_input_characters": self._max_input_characters,
        }
        if self._platform_session_id:
            request["client_session_id"] = self._platform_session_id
        if self._voice:
            request["voice"] = self._voice
        session = await self._client.open(
            SessionConfig(
                kind="tts",
                execution=execution_from_env(self._credential_source),
                request=request,
                media={
                    "encoding": "pcm_s16le",
                    "sample_rate_hz": self.sample_rate,
                    "channels": self._num_channels,
                },
                integration={
                    "name": "pipecat-python",
                    "version": _INTEGRATION_VERSION,
                    "transport": "pipecat",
                },
            )
        )
        state = _TTSContextState(session=session)
        self._contexts[context_id] = state
        state.task = asyncio.create_task(
            self._receive_audio(context_id, state),
            name=f"speko.pipecat.tts.receive.{context_id}",
        )
        return state

    async def _receive_audio(self, context_id: str, state: _TTSContextState) -> None:
        try:
            async for event in state.session.events():
                if event.type == "error":
                    raise _stream_error(event)
                if event.type == "audio.frame" and event.audio:
                    await self.append_to_audio_context(
                        context_id,
                        TTSAudioRawFrame(
                            audio=event.audio,
                            sample_rate=self.sample_rate,
                            num_channels=self._num_channels,
                            context_id=context_id,
                        ),
                    )
        except asyncio.CancelledError:
            raise
        except (GatewayError, OSError) as error:
            await self.push_error(_gateway_failure("TTS", error), exception=error)
        finally:
            if not state.interrupted and self.audio_context_available(context_id):
                await self.append_to_audio_context(
                    context_id, TTSStoppedFrame(context_id=context_id)
                )
                await self.remove_audio_context(context_id)
            try:
                await state.session.aclose()
            except (GatewayError, OSError, RuntimeError):
                pass
            finally:
                if self._contexts.get(context_id) is state:
                    self._contexts.pop(context_id, None)

    async def _finish_all(self, *, interrupted: bool) -> None:
        states = list(self._contexts.items())
        if not interrupted:
            for context_id, state in states:
                if not state.finishing:
                    state.finishing = True
                    await state.session.finish()
            if states:
                await asyncio.gather(
                    *(state.task for _, state in states if state.task is not None),
                    return_exceptions=True,
                )
            return
        for context_id, state in states:
            await self._close_state(context_id, state, interrupted=True)

    async def _close_state(
        self, context_id: str, state: _TTSContextState, *, interrupted: bool
    ) -> None:
        state.interrupted = interrupted
        try:
            await state.session.cancel()
        except (GatewayError, OSError):
            pass
        try:
            await state.session.aclose()
        except (GatewayError, OSError, RuntimeError):
            pass
        if state.task is not None and not state.task.done():
            state.task.cancel()
            await asyncio.gather(state.task, return_exceptions=True)
        if self._contexts.get(context_id) is state:
            self._contexts.pop(context_id, None)


class SpekoLLMService(LLMService):
    """Stream a Pipecat universal LLM context through Speko Router.

    Router speaks a provider-neutral Responses-style protocol.  Pipecat's
    OpenAI-compatible universal context is normalized here instead of exposing
    provider SDK objects to the worker.
    """

    Settings = LLMSettings

    def __init__(
        self,
        client: RelayLLMClient | None = None,
        *,
        provider: str = "auto",
        model: str = "auto",
        objective: str = "balanced",
        max_output_tokens: int = 8_192,
        temperature: float | None = None,
        top_p: float | None = None,
        system_instruction: str | None = None,
        session_id: str = "",
        **kwargs: Any,
    ) -> None:
        if (provider == "auto") != (model == "auto"):
            raise ValueError("provider and model must both be auto or both be explicit")
        if max_output_tokens <= 0:
            raise ValueError("max_output_tokens must be positive")
        settings = LLMSettings(
            model=model,
            system_instruction=system_instruction,
            temperature=temperature,
            max_tokens=max_output_tokens,
            top_p=top_p,
            top_k=None,
            frequency_penalty=None,
            presence_penalty=None,
            seed=None,
            filter_incomplete_user_turns=False,
            user_turn_completion_config=None,
            extra={},
        )
        kwargs.setdefault("run_in_parallel", False)
        super().__init__(settings=settings, **kwargs)
        self._client = client or RelayLLMClient.from_env(session_id=session_id)
        self._owns_client = client is None
        self._provider = provider
        self._model = model
        self._objective = objective
        self._max_output_tokens = max_output_tokens
        self._temperature = temperature
        self._top_p = top_p

    def can_generate_metrics(self) -> bool:
        return True

    async def _update_settings(self, delta: LLMSettings) -> dict[str, Any]:
        if is_given(delta.model):
            model = delta.model
            if not isinstance(model, str) or not model:
                raise ValueError("model must be a non-empty string")
            if (self._provider == "auto") != (model == "auto"):
                raise ValueError(
                    "runtime model updates cannot change between auto and explicit routing"
                )
        changed = await super()._update_settings(delta)
        if "model" in changed:
            self._model = self._settings.model
        return changed

    async def cleanup(self) -> None:
        if self._owns_client:
            await self._client.aclose()
        await super().cleanup()

    async def run_inference(
        self,
        context: LLMContext,
        max_tokens: int | None = None,
        system_instruction: str | None = None,
    ) -> str | None:
        request = self._request(
            context,
            max_output_tokens=max_tokens,
            system_instruction=system_instruction,
        )
        text: list[str] = []
        events = self._client.stream_response(request)
        try:
            async for event, payload in events:
                if event == "response.text.delta":
                    text.append(str(payload.get("delta", "")))
        finally:
            await events.aclose()
        return "".join(text) or None

    @traced_llm
    async def _process_context(self, context: LLMContext) -> None:
        await self.start_ttfb_metrics()
        function_calls: list[FunctionCallFromLLM] = []
        first_output = True
        events = self._client.stream_response(self._request(context))
        try:
            async for event, payload in events:
                if event == "response.text.delta":
                    delta = str(payload.get("delta", ""))
                    if delta:
                        if first_output:
                            first_output = False
                            await self.stop_ttfb_metrics()
                        await self._push_llm_text(delta)
                    continue
                if event == "response.item.completed":
                    item = payload.get("item")
                    if not isinstance(item, dict) or item.get("type") != "function_call":
                        continue
                    if first_output:
                        first_output = False
                        await self.stop_ttfb_metrics()
                    arguments = item.get("arguments") or "{}"
                    try:
                        parsed_arguments = json.loads(arguments)
                    except (TypeError, json.JSONDecodeError):
                        await self.push_error("Speko Router returned invalid function arguments")
                        continue
                    if not isinstance(parsed_arguments, dict):
                        await self.push_error("Speko Router returned non-object function arguments")
                        continue
                    function_calls.append(
                        FunctionCallFromLLM(
                            context=context,
                            tool_call_id=str(item.get("call_id", "")),
                            function_name=str(item.get("name", "")),
                            arguments=parsed_arguments,
                        )
                    )
                    continue
                if event == "response.completed":
                    usage = payload.get("usage")
                    if isinstance(usage, dict):
                        await self.start_llm_usage_metrics(_llm_token_usage(usage))
        finally:
            await events.aclose()
            if first_output:
                await self.stop_ttfb_metrics()
        if function_calls:
            await self.run_function_calls(function_calls)

    async def process_frame(self, frame: Frame, direction: FrameDirection) -> None:
        await super().process_frame(frame, direction)
        if not isinstance(frame, LLMContextFrame):
            await self.push_frame(frame, direction)
            return
        await self.push_frame(LLMFullResponseStartFrame())
        await self.start_processing_metrics()
        try:
            await self._process_context(frame.context)
        except asyncio.CancelledError:
            raise
        except (RelayError, OSError) as error:
            await self.push_error(
                f"Speko Router LLM failed{_relay_error_suffix(error)}",
                exception=error,
            )
        finally:
            await self.stop_processing_metrics()
            await self.push_frame(LLMFullResponseEndFrame())

    def _request(
        self,
        context: LLMContext,
        *,
        max_output_tokens: int | None = None,
        system_instruction: str | None = None,
    ) -> dict[str, Any]:
        if is_given(context.tool_choice):
            raise ValueError("Speko Router does not support Pipecat tool_choice")
        messages = list(context.get_messages())
        instruction = system_instruction or self._settings.system_instruction
        if instruction:
            messages.insert(0, {"role": "system", "content": instruction})
        request: dict[str, Any] = {
            "routing": (
                {"mode": "auto", "objective": self._objective}
                if self._provider == "auto"
                else {
                    "mode": "explicit",
                    "provider": self._provider,
                    "model": self._settings.model,
                }
            ),
            "input": _relay_input(messages),
            "max_output_tokens": max_output_tokens or self._settings.max_tokens,
        }
        if is_given(context.tools):
            tools = self.get_llm_adapter().from_standard_tools(context.tools)
            request["tools"] = [_relay_tool(tool) for tool in tools]
        if self._settings.temperature is not None:
            request["temperature"] = self._settings.temperature
        if self._settings.top_p is not None:
            request["top_p"] = self._settings.top_p
        return request


# Short aliases mirror the naming style of the existing LiveKit integration.
STT = SpekoSTTService
TTS = SpekoTTSService
LLM = SpekoLLMService


def _transcription_frame(
    event: CanonicalEvent, *, user_id: str, language: str
) -> TranscriptionFrame | InterimTranscriptionFrame | None:
    text = str(event.data.get("text", ""))
    if not text:
        return None
    result = {
        "provider_request_id": str(event.data.get("provider_request_id", "")),
        "extensions": event.extensions,
    }
    detected_language = str(
        event.data.get("language")
        or event.data.get("detected_language")
        or event.extensions.get("language", "")
        or language
    )
    result["language"] = detected_language
    try:
        frame_language: Language | None = Language(detected_language)
    except ValueError:
        frame_language = None
    if event.type == "transcript.delta":
        return InterimTranscriptionFrame(
            text=text,
            user_id=user_id,
            timestamp=time_now_iso8601(),
            language=frame_language,
            result=result,
        )
    if event.type == "transcript.final":
        return TranscriptionFrame(
            text=text,
            user_id=user_id,
            timestamp=time_now_iso8601(),
            language=frame_language,
            result=result,
            finalized=bool(event.data.get("speech_final", False)),
        )
    return None


def _stream_error(event: CanonicalEvent) -> GatewayError:
    retryable = event.data.get("retryable", True)
    return GatewayError(
        "Gateway provider stream failed",
        code=str(event.data.get("code", "")),
        source=str(event.data.get("source", "")),
        retryable=retryable if isinstance(retryable, bool) else True,
    )


def _gateway_failure(kind: str, error: BaseException) -> str:
    if isinstance(error, GatewayError):
        classification = "/".join(
            value for value in (error.source, error.code) if value
        )
        suffix = f" ({classification})" if classification else ""
    else:
        suffix = ""
    return f"Speko Gateway {kind} failed{suffix}"


def _relay_input(messages: Sequence[Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for message in messages:
        if not isinstance(message, dict):
            continue
        role = str(message.get("role", ""))
        if role in ("system", "developer", "user", "assistant"):
            text = _message_text(message.get("content"))
            if text:
                items.append(
                    {
                        "type": "message",
                        "role": "system" if role == "developer" else role,
                        "content": [{"type": "text", "text": text}],
                    }
                )
            if role == "assistant":
                for call in message.get("tool_calls") or []:
                    if not isinstance(call, dict):
                        continue
                    function = call.get("function")
                    if not isinstance(function, dict):
                        continue
                    items.append(
                        {
                            "type": "function_call",
                            "call_id": str(call.get("id", "")),
                            "name": str(function.get("name", "")),
                            "arguments": str(function.get("arguments") or "{}"),
                        }
                    )
        elif role == "tool":
            items.append(
                {
                    "type": "function_result",
                    "call_id": str(message.get("tool_call_id", "")),
                    "result": _message_text(message.get("content")),
                }
            )
    return items


def _message_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if not isinstance(content, Sequence):
        return ""
    parts: list[str] = []
    for part in content:
        if isinstance(part, str):
            parts.append(part)
        elif isinstance(part, dict) and part.get("type") in ("text", "input_text"):
            parts.append(str(part.get("text", "")))
    return "".join(parts)


def _relay_tool(tool: Any) -> dict[str, Any]:
    if not isinstance(tool, dict) or tool.get("type") != "function":
        raise ValueError("Speko Router supports function tools only")
    function = tool.get("function")
    if not isinstance(function, dict) or not function.get("name"):
        raise ValueError("Speko Router received an invalid function tool")
    return {
        "name": str(function["name"]),
        "description": str(function.get("description", "")),
        "parameters": function.get("parameters") or {"type": "object", "properties": {}},
    }


def _llm_token_usage(usage: Mapping[str, Any]) -> LLMTokenUsage:
    cached = int(usage.get("cached_input_tokens", 0))
    reasoning = int(usage.get("reasoning_tokens", 0))
    prompt = int(usage.get("input_tokens", 0)) + cached
    completion = int(usage.get("output_tokens", 0)) + reasoning
    return LLMTokenUsage(
        prompt_tokens=prompt,
        completion_tokens=completion,
        total_tokens=prompt + completion,
        cache_read_input_tokens=cached,
        reasoning_tokens=reasoning,
    )


def _relay_error_suffix(error: BaseException) -> str:
    if isinstance(error, RelayError) and error.code:
        return f" ({error.code})"
    return ""


__all__ = [
    "LLM",
    "STT",
    "TTS",
    "SpekoLLMService",
    "SpekoSTTService",
    "SpekoTTSService",
]
