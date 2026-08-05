"""LiveKit Agents streaming STT implementation backed by Speko Gateway."""

from __future__ import annotations

import asyncio
import weakref
from typing import Any

from livekit.agents import (
    DEFAULT_API_CONNECT_OPTIONS,
    NOT_GIVEN,
    APIConnectionError,
    APIConnectOptions,
    NotGivenOr,
    stt,
    utils,
)
from livekit.agents.utils import is_given

from ._livekit_bridge import LiveKitSpeechEvent, LiveKitSTTBridge, LiveKitSTTStream
from .client import GatewayClient, GatewayError


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
        await self._gateway_stream.aclose()

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
