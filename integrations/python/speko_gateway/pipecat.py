"""Native Pipecat STT and TTS services for the local Speko Gateway."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from pipecat.frames.frames import (
    CancelFrame,
    EndFrame,
    ErrorFrame,
    Frame,
    InterimTranscriptionFrame,
    StartFrame,
    TranscriptionFrame,
    TTSAudioRawFrame,
    TTSStoppedFrame,
    VADUserStoppedSpeakingFrame,
)
from pipecat.processors.frame_processor import FrameDirection
from pipecat.services.stt_service import STTService as PipecatSTTService
from pipecat.services.tts_service import TTSService as PipecatTTSService
from pipecat.transcriptions.language import Language
from pipecat.utils.time import time_now_iso8601

from ._voice import CredentialSource, execution_from_env, stt_options_payload
from .client import (
    CanonicalEvent,
    GatewayClient,
    GatewayError,
    GatewaySession,
    SessionConfig,
)

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
        **kwargs: Any,
    ) -> None:
        super().__init__(sample_rate=sample_rate, **kwargs)
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
        await self._connect()

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
        **kwargs: Any,
    ) -> None:
        kwargs.setdefault("push_start_frame", True)
        kwargs.setdefault("push_stop_frames", False)
        kwargs.setdefault("stop_frame_timeout_s", 15.0)
        super().__init__(sample_rate=sample_rate, **kwargs)
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


# Short aliases mirror the naming style of the existing LiveKit integration.
STT = SpekoSTTService
TTS = SpekoTTSService


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
    try:
        frame_language: Language | None = Language(language)
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


__all__ = ["STT", "TTS", "SpekoSTTService", "SpekoTTSService"]
