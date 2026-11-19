"""Keep speaker RMS positions local while SDK transcript positions stay global."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from livekit.agents import stt

from .client import GatewayError


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
    _local_terminal: bool = field(default=False, repr=False)
    _local_filtered: bool = field(default=False, repr=False)
    provider_speaker_id: str | None = None
    timing_source: str | None = None
    _native_timing_unavailable: bool = field(default=False, repr=False)
    _native_speaker_scope: str | None = field(default=None, repr=False)


def bind_native_speaker_timing(stream: Any, *, unsupported: Any = lambda: None) -> None:
    detector = stream._detector
    original, original_close = detector.on_stt_event, stream.aclose
    scope: str | None = None

    def on_event(event: stt.SpeechEvent) -> stt.SpeechEvent | None:
        nonlocal scope
        data = event.alternatives[0] if event.alternatives else None
        if (
            event.type is stt.SpeechEventType.FINAL_TRANSCRIPT
            and isinstance(data, NativeSpeechData)
            and data._native_timing_unavailable
            and detector._detect_primary
        ):
            unsupported()
            raise GatewayError("Native speaker detection requires an attributable captured interval", retryable=False)
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
            result = original(event)
            if (
                event.type is stt.SpeechEventType.FINAL_TRANSCRIPT
                and data.timing_source == "captured_request"
                and data.speaker_id is not None
                and detector._detect_primary
                and data.is_primary_speaker is None
            ):
                unsupported()
                raise GatewayError("Captured speech cannot support primary-speaker detection", retryable=False)
            return result
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
