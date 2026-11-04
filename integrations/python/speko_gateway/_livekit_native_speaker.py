"""Keep speaker RMS positions local while SDK transcript positions stay global."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from livekit.agents import stt


@dataclass
class NativeSpeechData(stt.SpeechData):
    # Dataclass fields survive the SDK's public lossless replace operation.
    # Witnesses contain only scalar values, never streams, futures or owners.
    _native_input_times: tuple[float, float] | None = field(default=None, repr=False)
    speaker_segments: tuple[stt.SpeechData, ...] = ()
    canonical_text: str | None = None
    _native_final: tuple[str, int] | None = field(default=None, repr=False)
    _local_part_text: str | None = field(default=None, repr=False)
    _local_group: str | None = field(default=None, repr=False)
    _local_suppressed: bool = field(default=False, repr=False)
    _local_after_eos: bool = field(default=False, repr=False)
    provider_speaker_id: str | None = None
    _native_speaker_scope: str | None = field(default=None, repr=False)


def bind_native_speaker_timing(stream: Any) -> None:
    detector = stream._detector
    original, original_close = detector.on_stt_event, stream.aclose
    scope: str | None = None

    def on_event(event: stt.SpeechEvent) -> stt.SpeechEvent | None:
        nonlocal scope
        data = event.alternatives[0] if event.alternatives else None
        if isinstance(data, NativeSpeechData) and data._native_speaker_scope is not None and data._native_speaker_scope != scope:
            # Labels from a fresh physical request cannot compete with a
            # previous request's primary identity. Keep the accepted PCM
            # history and its timeline; only identity state starts over.
            detector._primary_speaker = None
            detector._speaker_data.clear()
            scope = data._native_speaker_scope
        if not isinstance(data, NativeSpeechData) or data._native_input_times is None:
            return original(event)
        start, end = data.start_time, data.end_time
        data.start_time, data.end_time = data._native_input_times
        try:
            # SDK speaker detection is synchronous. Only its own RMS lookup
            # sees the local positions; callers retain the source timeline.
            return original(event)
        finally:
            data.start_time, data.end_time = start, end

    async def close() -> None:
        try:
            await original_close()
        finally:
            if detector.on_stt_event is on_event:
                detector.on_stt_event = original
            if stream.aclose is close:
                stream.aclose = original_close

    detector.on_stt_event, stream.aclose = on_event, close
