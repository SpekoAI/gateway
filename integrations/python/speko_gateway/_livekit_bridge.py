"""Provider-neutral LiveKit audio mapping for Speko Gateway."""

from __future__ import annotations

import logging
import math
from collections import Counter
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, replace
from itertools import pairwise
from typing import Any, Protocol

from ._speech_client import SpeechClient, SpeechSession
from ._voice import CredentialSource, execution_from_env
from .client import (
    CanonicalEvent,
    GatewayError,
    SessionConfig,
)
from .relay_speech import RelaySpeechClient

_INTEGRATION_VERSION = "0.1.0"
_logger = logging.getLogger(__name__)


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
    # language is the provider-identified language of a final, "" when the
    # provider identified none (a session pinned to one language).
    language: str = ""
    speaker_id: str | None = None
    # A segmented native final carries its complete text on its last part.
    # Empty strings mark earlier parts; None preserves standalone event fakes.
    canonical_text: str | None = None
    speaker_segments: tuple[LiveKitSpeechEvent, ...] = ()


@dataclass(frozen=True)
class LiveKitTTSEvent:
    type: str
    audio: bytes | None = None
    provider_request_id: str = ""


class LiveKitSTTBridge:
    """Map LiveKit audio-frame semantics to the canonical STT stream."""

    def __init__(
        self,
        client: SpeechClient,
        *,
        language: str = "en",
        model: str = "auto",
        provider: str = "auto",
        credential_source: CredentialSource = "auto",
        stt_options: dict[str, Any] | None = None,
        auto_reconnect: bool = False,
        on_continuation: Callable[[CanonicalEvent], None] | None = None,
    ) -> None:
        if auto_reconnect and not isinstance(client, RelaySpeechClient):
            raise ValueError("auto_reconnect requires a RelaySpeechClient")
        self._client = client
        self._on_continuation = on_continuation
        self._language = language
        self._model = model
        self._provider = provider
        self._credential_source = credential_source
        self._stt_options = stt_options
        self._auto_reconnect = auto_reconnect

    async def start(self, frame: AudioFrameLike) -> LiveKitSTTStream:
        request: dict[str, Any] = {
            "provider": self._provider,
            "language": self._language,
            "model": self._model,
        }
        if self._stt_options:
            request["stt"] = self._stt_options
        # Only a RelaySpeechClient takes the flag; other clients keep their
        # own signature.
        reconnect = {"auto_reconnect": True} if self._auto_reconnect else {}
        session = await self._client.open(
            SessionConfig(
                kind="stt",
                execution=execution_from_env(self._credential_source),
                request=request,
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
            ),
            **reconnect,
        )
        return LiveKitSTTStream(
            session,
            local_speakers=not isinstance(self._client, RelaySpeechClient),
            on_continuation=self._on_continuation,
        )


class LiveKitTTSBridge:
    """Map LiveKit synthesis semantics to the canonical TTS stream."""

    def __init__(
        self,
        client: SpeechClient,
        *,
        voice: str = "",
        language: str = "en",
        model: str = "auto",
        provider: str = "auto",
        sample_rate: int = 24_000,
        num_channels: int = 1,
        max_input_characters: int = 10_000,
        credential_source: CredentialSource = "auto",
        tts_options: dict[str, Any] | None = None,
    ) -> None:
        self._client = client
        self._voice = voice
        self._language = language
        self._model = model
        self._provider = provider
        self._sample_rate = sample_rate
        self._num_channels = num_channels
        self._max_input_characters = max_input_characters
        self._credential_source = credential_source
        self._tts_options = tts_options

    async def start(self) -> LiveKitTTSStream:
        request: dict[str, Any] = {
            "provider": self._provider,
            "model": self._model,
            "language": self._language,
            "max_input_characters": self._max_input_characters,
        }
        if self._voice:
            request["voice"] = self._voice
        if self._tts_options:
            request["tts"] = self._tts_options
        session = await self._client.open(
            SessionConfig(
                kind="tts",
                execution=execution_from_env(self._credential_source),
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
    def __init__(self, session: SpeechSession) -> None:
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
    def __init__(
        self,
        session: SpeechSession,
        *,
        local_speakers: bool = False,
        on_continuation: Callable[[CanonicalEvent], None] | None = None,
    ) -> None:
        self._session = session
        self._local_speakers = local_speakers
        self._on_continuation = on_continuation
        # Each session.continued or restarted starts a new provider session,
        # whose speaker labels name other people than the same labels before.
        self._generation = 0
        self.continuation_counts: Counter[str] = Counter()

    @property
    def recovering(self) -> bool:
        """True while a dropped stream is being continued (auto_reconnect)."""
        return getattr(self._session, "recovering", False) is True

    @property
    def request_id(self) -> str:
        return str(self._session.metadata.get("request_id", ""))

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
            if event.type == "warning":
                # A non-fatal Gateway notice, such as an ignored provider option.
                # Log it and keep the stream open.
                _logger.warning(
                    "Speko Gateway STT warning: %s (ignored_option=%s, "
                    "provider_request_id=%s)",
                    event.data.get("message", ""),
                    event.data.get("ignored_option", ""),
                    event.data.get("provider_request_id", ""),
                )
                continue
            if event.type in _CONTINUATION_EVENTS:
                self._continuation_event(event)
                # The stream sees session.reconnecting (interim text is void)
                # and session.gap (the turn's transcript may miss words).
                yield LiveKitSpeechEvent(type=event.type)
                continue
            if event.type == "transcript.final" and event.data.get("commit_failed"):
                # A commit answered without its transcript: its audio was lost.
                # Fail the turn rather than finish it with words missing.
                self.continuation_counts["commit_failed"] += 1
                raise GatewayError(
                    "Speko Gateway STT commit failed",
                    code="commit_failed",
                    source="relay",
                    retryable=False,
                )
            for mapped in _speech_events(event, local_speakers=self._local_speakers):
                yield self._scoped(mapped)

    def _continuation_event(self, event: CanonicalEvent) -> None:
        self.continuation_counts[event.type] += 1
        if event.type in {"session.continued", "session.restarted"}:
            self._generation += 1
        log = _logger.warning if event.type == "session.gap" else _logger.info
        log("Speko Gateway STT %s: %s", event.type, event.data)
        if self._on_continuation is not None:
            self._on_continuation(event)

    def _scoped(self, event: LiveKitSpeechEvent) -> LiveKitSpeechEvent:
        if not self._generation:
            return event

        def scope(speaker: str | None) -> str | None:
            return None if speaker is None else f"{speaker}@{self._generation}"

        return replace(
            event,
            speaker_id=scope(event.speaker_id),
            speaker_segments=tuple(
                replace(part, speaker_id=scope(part.speaker_id))
                for part in event.speaker_segments
            ),
        )


_CONTINUATION_EVENTS = frozenset(
    {
        "session.reconnecting",
        "session.continued",
        "session.restarted",
        "session.gap",
    }
)


def _stream_error(event: CanonicalEvent) -> GatewayError:
    retryable = event.data.get("retryable", True)
    return GatewayError(
        "Gateway provider stream failed",
        code=str(event.data.get("code", "")),
        source=str(event.data.get("source", "")),
        retryable=retryable if isinstance(retryable, bool) else True,
    )


def _speech_events(
    event: CanonicalEvent, *, local_speakers: bool = False
) -> tuple[LiveKitSpeechEvent, ...]:
    if event.type not in {
        "speech.started",
        "speech.ended",
        "transcript.delta",
        "transcript.final",
    }:
        return ()
    data = event.data
    mapped = LiveKitSpeechEvent(
        type=event.type,
        text=str(data.get("text", "")),
        start_time=_milliseconds(data.get("audio_start_ms")),
        end_time=_milliseconds(data.get("audio_end_ms")),
        provider_request_id=str(data.get("provider_request_id", "")),
        language=str(data.get("language", "")),
        speaker_id=_speaker(data.get("speaker"), local_speakers=local_speakers),
        canonical_text=str(data.get("text", ""))
        if event.type == "transcript.final"
        else None,
    )
    segments = data.get("segments")
    if (
        event.type != "transcript.final"
        or not isinstance(segments, list)
        or not any(
            isinstance(segment, dict) and segment.get("speaker") not in (None, "")
            for segment in segments
        )
    ):
        return (mapped,)
    # Segment text is optional alignment evidence, not the canonical final.
    # Forward and backward alignment agree only on unambiguous ordered spans.
    if any(
        not isinstance(segment, dict) or not isinstance(segment.get("text"), str)
        for segment in segments
    ):
        raise GatewayError("Native speech attribution is invalid", retryable=False)
    attributed = []
    for segment in segments:
        start = _milliseconds(segment.get("start_ms"))
        end = _milliseconds(segment.get("end_ms"))
        if (
            start is None
            or end is None
            or not math.isfinite(start)
            or not math.isfinite(end)
            or not 0 <= start <= end
        ):
            raise GatewayError("Native speech attribution is invalid", retryable=False)
        attributed.append(
            replace(
                mapped,
                text=segment["text"],
                start_time=start,
                end_time=end,
                speaker_id=_speaker(
                    segment.get("speaker"), local_speakers=local_speakers
                )
                or mapped.speaker_id,
            )
        )
    attributed = [part for part in attributed if part.text]
    forward, backward = [], []
    cursor = 0
    for part in attributed:
        start = mapped.text.find(part.text, cursor) if part.text else -1
        if start < 0:
            return (replace(mapped, speaker_id=None),)
        forward.append(start)
        cursor = start + len(part.text)
    cursor = len(mapped.text)
    for part in reversed(attributed):
        start = mapped.text.rfind(part.text, 0, cursor) if part.text else -1
        if start < 0:
            return (replace(mapped, speaker_id=None),)
        backward.append(start)
        cursor = start
    backward.reverse()
    uncertain = [
        (start, latest + len(part.text))
        for part, start, latest in zip(attributed, forward, backward)
        if start != latest
    ]
    parts, cursor = [], 0

    def gap(start: int, end: int) -> None:
        boundaries = sorted({start, end} | {
            max(start, min(end, position))
            for span in uncertain for position in span
        })
        for left, right in pairwise(boundaries):
            if mapped.text[left:right].strip():
                parts.append(replace(
                    mapped,
                    text=mapped.text[left:right],
                    start_time=None,
                    end_time=None,
                    speaker_id=None if any(a < right and left < b for a, b in uncertain) else mapped.speaker_id,
                    canonical_text="",
                ))

    for part, start, latest in zip(attributed, forward, backward):
        if start != latest:
            continue
        gap(cursor, start)
        parts.append(replace(part, canonical_text=""))
        cursor = start + len(part.text)
    gap(cursor, len(mapped.text))
    if not parts:
        return (mapped,)
    parts[-1] = replace(parts[-1], canonical_text=mapped.text)
    return tuple(parts)


def _speaker(value: Any, *, local_speakers: bool = False) -> str | None:
    if value is None or value == "":
        return None
    # The local Gateway's existing Modulate adapter uses JSON integer IDs.
    # Native relay labels remain strings; bool is never a speaker identifier.
    if local_speakers and type(value) is int:
        return str(value)
    if not isinstance(value, str):
        raise GatewayError("Native speech attribution is invalid", retryable=False)
    return value


def _milliseconds(value: Any) -> float | None:
    return float(value) / 1_000 if isinstance(value, (int, float)) else None
