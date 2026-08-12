"""Provider-neutral LiveKit audio mapping for Speko Gateway."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, Protocol

from .client import (
    CanonicalEvent,
    GatewayClient,
    GatewayError,
    GatewaySession,
    SessionConfig,
)

_INTEGRATION_VERSION = "0.1.0"


class AudioFrameLike(Protocol):
    data: Any
    sample_rate: int
    num_channels: int


@dataclass(frozen=True)
class LiveKitSpeechEvent:
    type: str
    text: str = ""
    start_time: float | None = None
    end_time: float | None = None
    provider_request_id: str = ""


@dataclass(frozen=True)
class LiveKitTTSEvent:
    type: str
    audio: bytes | None = None
    provider_request_id: str = ""


def execution_from_env() -> dict[str, str]:
    """Match the request to Gateway's configured managed or BYOK mode."""

    managed = bool(
        os.environ.get("SPEKO_API_KEY", "").strip()
        or os.environ.get("SPEKO_API_KEY_FILE", "").strip()
    )
    if managed:
        return {
            "provider_route": "auto",
            "credential_source": "managed",
            "relay_policy": "allowed",
        }
    return {
        "provider_route": "provider_direct",
        "credential_source": "byok",
        "relay_policy": "forbidden",
    }


class LiveKitSTTBridge:
    """Map LiveKit audio-frame semantics to the canonical STT stream."""

    def __init__(
        self, client: GatewayClient, *, language: str = "en", model: str = "nova-3"
    ) -> None:
        self._client = client
        self._language = language
        self._model = model

    async def start(self, frame: AudioFrameLike) -> LiveKitSTTStream:
        session = await self._client.open(
            SessionConfig(
                kind="stt",
                execution=execution_from_env(),
                request={"language": self._language, "model": self._model},
                media={
                    "encoding": "pcm_s16le",
                    "sample_rate_hz": frame.sample_rate,
                    "channels": frame.num_channels,
                },
                integration={
                    "name": "livekit-python",
                    "version": _INTEGRATION_VERSION,
                    "transport": "livekit-webrtc",
                },
            )
        )
        return LiveKitSTTStream(session)


class LiveKitTTSBridge:
    """Map LiveKit synthesis semantics to the canonical TTS stream."""

    def __init__(
        self,
        client: GatewayClient,
        *,
        voice: str = "",
        language: str = "en",
        model: str = "auto",
        provider: str = "auto",
        sample_rate: int = 24_000,
        num_channels: int = 1,
        max_input_characters: int = 100_000,
    ) -> None:
        self._client = client
        self._voice = voice
        self._language = language
        self._model = model
        self._provider = provider
        self._sample_rate = sample_rate
        self._num_channels = num_channels
        self._max_input_characters = max_input_characters

    async def start(self) -> LiveKitTTSStream:
        request: dict[str, Any] = {
            "provider": self._provider,
            "model": self._model,
            "language": self._language,
            "max_input_characters": self._max_input_characters,
        }
        if self._voice:
            request["voice"] = self._voice
        session = await self._client.open(
            SessionConfig(
                kind="tts",
                execution=execution_from_env(),
                request=request,
                media={
                    "encoding": "pcm_s16le",
                    "sample_rate_hz": self._sample_rate,
                    "channels": self._num_channels,
                },
                integration={
                    "name": "livekit-python",
                    "version": _INTEGRATION_VERSION,
                    "transport": "livekit-webrtc",
                },
            )
        )
        return LiveKitTTSStream(session)


class LiveKitTTSStream:
    def __init__(self, session: GatewaySession) -> None:
        self._session = session

    @property
    def session_id(self) -> str:
        """Gateway session identifier, exposed for profiler leg attachment."""

        return str(self._session.metadata.get("session_id", ""))

    @property
    def attempt_id(self) -> str:
        """Gateway attempt identifier, exposed for profiler leg attachment."""

        return str(self._session.metadata.get("attempt_id", ""))

    async def append_text(self, text: str) -> None:
        await self._session.append_text(text)

    async def commit(self) -> None:
        await self._session.commit_text()

    async def finish(self) -> None:
        await self._session.finish()

    async def aclose(self) -> None:
        await self._session.aclose()

    async def events(self) -> AsyncIterator[LiveKitTTSEvent]:
        async for event in self._session.events():
            if event.type == "error":
                raise _stream_error(event)
            if event.type == "audio.frame" and event.audio:
                yield LiveKitTTSEvent(type="audio.frame", audio=event.audio)
                continue
            if event.type in {"audio.started", "audio.done"}:
                yield LiveKitTTSEvent(
                    type=event.type,
                    provider_request_id=str(event.data.get("provider_request_id", "")),
                )


class LiveKitSTTStream:
    def __init__(self, session: GatewaySession) -> None:
        self._session = session

    @property
    def session_id(self) -> str:
        """Gateway session identifier, exposed for profiler leg attachment."""

        return str(self._session.metadata.get("session_id", ""))

    @property
    def attempt_id(self) -> str:
        """Gateway attempt identifier, exposed for profiler leg attachment."""

        return str(self._session.metadata.get("attempt_id", ""))

    async def push_frame(self, frame: AudioFrameLike) -> None:
        await self._session.send_audio(frame.data)

    async def flush(self) -> None:
        await self._session.commit_audio()

    async def finish(self) -> None:
        await self._session.finish()

    async def aclose(self) -> None:
        await self._session.aclose()

    async def events(self) -> AsyncIterator[LiveKitSpeechEvent]:
        async for event in self._session.events():
            if event.type == "error":
                raise _stream_error(event)
            mapped = _speech_event(event)
            if mapped is not None:
                yield mapped


def _stream_error(event: CanonicalEvent) -> GatewayError:
    retryable = event.data.get("retryable", True)
    return GatewayError(
        "Gateway provider stream failed",
        code=str(event.data.get("code", "")),
        source=str(event.data.get("source", "")),
        retryable=retryable if isinstance(retryable, bool) else True,
    )


def _speech_event(event: CanonicalEvent) -> LiveKitSpeechEvent | None:
    if event.type not in {
        "speech.started",
        "speech.ended",
        "transcript.delta",
        "transcript.final",
    }:
        return None
    data = event.data
    return LiveKitSpeechEvent(
        type=event.type,
        text=str(data.get("text", "")),
        start_time=_milliseconds(data.get("audio_start_ms")),
        end_time=_milliseconds(data.get("audio_end_ms")),
        provider_request_id=str(data.get("provider_request_id", "")),
    )


def _milliseconds(value: Any) -> float | None:
    return float(value) / 1_000 if isinstance(value, (int, float)) else None
