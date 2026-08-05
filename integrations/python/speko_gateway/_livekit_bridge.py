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


class LiveKitSTTStream:
    def __init__(self, session: GatewaySession) -> None:
        self._session = session

    async def push_frame(self, frame: AudioFrameLike) -> None:
        await self._session.send_audio(frame.data)

    async def flush(self) -> None:
        await self._session.commit_audio()

    async def aclose(self) -> None:
        await self._session.aclose()

    async def events(self) -> AsyncIterator[LiveKitSpeechEvent]:
        async for event in self._session.events():
            if event.type == "error":
                code = str(event.data.get("code", ""))
                source = str(event.data.get("source", ""))
                retryable = event.data.get("retryable", True)
                raise GatewayError(
                    "Gateway provider stream failed",
                    code=code,
                    source=source,
                    retryable=retryable if isinstance(retryable, bool) else True,
                )
            mapped = _speech_event(event)
            if mapped is not None:
                yield mapped


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
