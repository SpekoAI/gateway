"""Keep speaker RMS positions local while SDK transcript positions stay global."""

from __future__ import annotations

from typing import Any

from livekit.agents import stt


class NativeSpeechData(stt.SpeechData):
    # A private witness, deliberately absent from the SDK dataclass's wire
    # fields. The wrapper hears accepted PCM without custom-node filtered gaps.
    _native_input_times: tuple[float, float] | None = None


def bind_native_speaker_timing(stream: Any) -> None:
    detector = stream._detector
    original, original_close = detector.on_stt_event, stream.aclose

    def on_event(event: stt.SpeechEvent) -> stt.SpeechEvent | None:
        data = event.alternatives[0] if event.alternatives else None
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
