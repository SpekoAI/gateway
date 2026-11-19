"""Actual detector replay and fresh interruption clocks after abandoned onset."""

from __future__ import annotations

import asyncio
import threading

import pytest
from livekit import rtc
from livekit.agents import vad
from test_relay_speech_livekit import owned_probability_vad

from speko_gateway.livekit import DrainingVAD


@pytest.mark.parametrize("engine", ["inference", "silero16", "silero8"])
@pytest.mark.parametrize("confirmed", [False, True])
async def test_replayed_frontier_abandoned_onset_cannot_donate_quiet_duration(
    engine, confirmed
):
    source = owned_probability_vad(
        engine,
        min_speech_duration=0.064,
        min_silence_duration=0.256,
        prefix_padding_duration=0,
        _any_voice=True,
    )
    started, release = asyncio.Event(), threading.Event()
    loop, original, first_stream = asyncio.get_running_loop(), source.stream, True

    def delayed_stream():
        nonlocal first_stream
        stream = original()
        if not first_stream:
            return stream
        first_stream = False
        if engine == "inference":
            predictor = stream._native_vad

            class Delayed:
                def predict(self, window):
                    loop.call_soon_threadsafe(started.set)
                    if not release.wait(3):
                        raise RuntimeError("owned inference release timed out")
                    return predictor.predict(window)

                def reset(self):
                    predictor.reset()

            stream._native_vad = Delayed()
        else:
            predictor = stream._model

            class Delayed:
                sample_rate = predictor.sample_rate
                window_size_samples = predictor.window_size_samples

                def __call__(self, window):
                    loop.call_soon_threadsafe(started.set)
                    if not release.wait(3):
                        raise RuntimeError("owned inference release timed out")
                    return predictor(window)

                def reset(self):
                    predictor.reset()

            stream._model = Delayed()
        return stream

    source.stream = delayed_stream
    stream = DrainingVAD(source, flush_mode="reset").stream()
    quiet = b"\0\0" * 512
    tentative = b"\xff\x0f" * (1024 if confirmed else 512)
    silence, fresh = b"\0\0" * 16384, b"\xff\x0f" * 1536

    def frame(data):
        return rtc.AudioFrame(data, 16000, 1, len(data) // 2)

    try:
        stream.push_frame(frame(quiet))
        await asyncio.wait_for(started.wait(), 3)
        # The actual reset must retain this unclassified, physically captured
        # window as its replay frontier. No private clock/frontier is injected.
        stream.flush()
        release.set()

        async def replayed():
            while stream._segment != 1 or stream._processed < len(quiet) // 2:
                await asyncio.sleep(0)

        await asyncio.wait_for(replayed(), 3)
        assert stream._capture_frontier == len(quiet) // 2
        stream.push_frame(frame(tentative + silence + fresh))
        stream.end_input()

        async def collect():
            return [event async for event in stream]

        events = await asyncio.wait_for(collect(), 3)
        inferred = [e for e in events if e.type is vad.VADEventType.INFERENCE_DONE]
        assert b"".join(bytes(f.data) for e in inferred for f in e.frames) == (
            quiet + tentative + silence + fresh
        )
        starts = [e for e in events if e.type is vad.VADEventType.START_OF_SPEECH]
        assert len(starts) == (2 if confirmed else 1)
        assert starts[-1].speech_duration == pytest.approx(0.064)
        assert starts[-1].timestamp == pytest.approx(
            (len(quiet + tentative + silence) // 2 + 1024) / 16000
        )
        assert inferred[-1].speaking
        assert inferred[-1].speech_duration == pytest.approx(0.096), (
            "abandoned onset donated intervening quiet to fresh interruption"
        )
    finally:
        release.set()
        await stream.aclose()
