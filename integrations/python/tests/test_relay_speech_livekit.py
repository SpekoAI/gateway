from __future__ import annotations

import asyncio
import copy
import dataclasses
import inspect
import json
import threading
import time
import uuid
from contextlib import asynccontextmanager
from functools import wraps
from types import SimpleNamespace

import aiohttp
import pytest
from aiohttp import web
from livekit import rtc
from livekit.agents import (
    DEFAULT_API_CONNECT_OPTIONS,
    Agent,
    AgentSession,
    APIConnectionError,
    APIConnectOptions,
    stt,
    vad,
)
from livekit.agents.voice import audio_recognition as recognition
from livekit.agents.voice import io

from speko_gateway import GatewayError, RelaySpeechClient
from speko_gateway.livekit import STT, TTS, DrainingVAD


@asynccontextmanager
async def peer(handler):
    app = web.Application()
    app.router.add_get("/v1/{kind}/stream", handler)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    try:
        await web.TCPSite(runner, "127.0.0.1", 0).start()
        yield f"http://127.0.0.1:{runner.addresses[0][1]}"
    finally:
        await runner.cleanup()


async def ready(ws):
    await ws.send_json(
        {
            "type": "session.ready",
            "request_id": "owned-livekit",
            "route": {
                "provider": "fixture",
                "model": "fixture-model",
                "region": "owned",
            },
        }
    )


def frame():
    return rtc.AudioFrame(
        data=b"\xff\x0f" * 320,
        sample_rate=16000,
        num_channels=1,
        samples_per_channel=320,
    )


class ControlledVAD(vad.VAD):
    """A separate owned boundary source, never a transcript-derived endpoint."""

    def __init__(self, *, automatic=False, inference_rate=16000):
        super().__init__(capabilities=vad.VADCapabilities(update_interval=0.02))
        self.current = None
        self.frames = asyncio.Queue()
        self.automatic = automatic
        self.inference_rate = inference_rate
        self.index = 0
        self.prefix = []

    def stream(self):
        self.current = ControlledVADStream(self)
        return self.current

    def processed(self, value, *, raw_silence=0):
        self.index += (
            value.samples_per_channel * self.inference_rate // value.sample_rate
        )
        self.prefix.append(value)
        self.emit(vad.VADEventType.INFERENCE_DONE, frames=[value], raw_silence=raw_silence)

    def emit(self, kind, *, frames=None, speech_duration=0.02, silence_duration=0, raw_silence=0):
        assert self.current is not None
        self.current._event_ch.send_nowait(
            vad.VADEvent(
                type=kind,
                samples_index=self.index,
                timestamp=self.index / self.inference_rate,
                speech_duration=speech_duration,
                silence_duration=silence_duration,
                raw_accumulated_silence=raw_silence,
                frames=list(self.prefix if frames is None else frames),
            )
        )


class ControlledVADStream(vad.VADStream):
    async def _main_task(self):
        speaking = False
        async for value in self._input_ch:
            if not isinstance(value, self._FlushSentinel):
                await self._vad.frames.put(value)
                if self._vad.automatic:
                    self._vad.processed(value)
                    if not speaking:
                        speaking = True
                        self._vad.emit(vad.VADEventType.START_OF_SPEECH)
        if speaking:
            self._vad.emit(vad.VADEventType.END_OF_SPEECH)


class OwnedAudioInput(io.AudioInput):
    def __init__(self, *, track_delivery=False):
        super().__init__(label="owned-native-input")
        self.frames = asyncio.Queue()
        self.delivery = asyncio.get_running_loop().create_future() if track_delivery else None
        self._delivery_scheduled = False

    async def __anext__(self):
        value = await self.frames.get()
        if value is None:
            raise StopAsyncIteration
        if self.delivery is not None and not self._delivery_scheduled:
            self._delivery_scheduled = True
            lower = time.time()
            asyncio.get_running_loop().call_soon(self._record_delivery, lower)
        return value

    def _record_delivery(self, lower):
        if not self.delivery.done():
            self.delivery.set_result((lower, time.time()))


def input_epoch(session):
    recorder = getattr(session, "_recorder_io", None)
    return getattr(recorder, "recording_started_at", None) or session._started_at


async def assert_source_times(session, audio, actual, expected):
    legacy = not hasattr(session._activity._audio_recognition, "_input_started_at")
    if legacy:
        lower, upper = await asyncio.wait_for(asyncio.shield(audio.delivery), 3)
        epoch = input_epoch(session)
        assert len(actual) == len(expected)
        for value, offset in zip(actual, expected):
            assert lower - epoch + offset - 1 / 16000 <= value <= upper - epoch + offset + 1 / 16000
    else:
        assert actual == pytest.approx(expected, abs=1 / 16000)


def wrap_native_stt(model, wrapper):
    if wrapper in ("multi_speaker", "nested"):
        model = stt.MultiSpeakerAdapter(stt=model)
    if wrapper in ("fallback", "nested"):
        model = stt.FallbackAdapter([model], max_retry_per_stt=3, retry_interval=0)
    return model


def assert_request_speakers(finals):
    identities = {}
    for data in finals:
        label = data.provider_speaker_id
        if label is None:
            assert data.speaker_id is None
        else:
            assert data.speaker_id and data.speaker_id != label
            assert identities.setdefault(label, data.speaker_id) == data.speaker_id
    assert len(set(identities.values())) == len(identities)


def owned_probability_vad(engine, **options):
    """Real buffering/resampling/threshold tasks, calibrated inference scores."""
    public_events = options.pop("_events", None)
    any_voice = options.pop("_any_voice", False)
    windows = options.pop("_windows", None)
    if engine == "inference":
        from livekit.agents import inference

        if not hasattr(inference, "VAD"):
            pytest.skip("SDK predates local inference VAD")
        detector = inference.VAD(**options)
    else:
        silero = pytest.importorskip("livekit.plugins.silero")
        detector = silero.VAD.load(sample_rate=8000 if engine == "silero8" else 16000, **options)

    def probability(window, *, scale):
        values = [abs(float(value)) for value in window]
        if windows is not None:
            windows.append(values)
        level = max(values) if any_voice else sum(values) / len(values)
        return 0.9 if level > scale else 0.0

    original = detector.stream

    def stream():
        result = original()
        if public_events is not None:
            emit = result._event_ch.send_nowait

            def observed(event):
                public_events.append(event.type)
                emit(event)

            result._event_ch.send_nowait = observed
        if engine == "inference":
            class Predictor:
                def predict(self, window):
                    return probability(window, scale=1000)

                def reset(self):
                    pass

            result._native_vad = Predictor()
        else:
            model = result._model

            class Predictor:
                sample_rate, window_size_samples = model.sample_rate, model.window_size_samples

                def __call__(self, window):
                    return probability(window, scale=1000 / 32767)

                def reset(self):
                    pass

            class Probability:
                def apply(self, *, exp, sample):
                    return sample

                def reset(self):
                    pass

            result._model, result._exp_filter = Predictor(), Probability()
        return result

    detector.stream = stream
    return detector


def owned_local_turn_detector(predictions):
    """The actual local v1-mini transport and stream, with calibrated scores."""
    from livekit.agents import inference

    if not hasattr(inference, "TurnDetector"):
        pytest.skip("SDK predates local audio turn detection")
    detector = inference.TurnDetector(version="v1-mini", unlikely_threshold=0.5)
    original = detector.stream

    def stream(**kwargs):
        result = original(**kwargs)

        class Predictor:
            def predict(self, window):
                predictions.append(window.copy())
                return 0.9

        result._transport._eot = Predictor()
        return result

    detector.stream = stream
    return detector


@pytest.mark.parametrize("engine", ["inference", "silero16", "silero8"])
async def test_draining_framework_vad_keeps_real_minimums_and_event_audio(engine):
    public, events, completed = [], [], asyncio.Queue()
    detector = DrainingVAD(owned_probability_vad(
        engine, min_speech_duration=0.02, min_silence_duration=0.02,
        prefix_padding_duration=0, _any_voice=True, _events=public,
    ))
    stream = detector.stream()

    async def collect():
        async for event in stream:
            events.append(event)
            completed.put_nowait(event)

    collector = asyncio.create_task(collect())
    async def push(raw):
        stream.push_frame(rtc.AudioFrame(data=raw, sample_rate=16000, num_channels=1, samples_per_channel=len(raw) // 2))
        remaining = len(raw)
        while remaining:
            event = await asyncio.wait_for(completed.get(), 3)
            if event.type is vad.VADEventType.INFERENCE_DONE:
                remaining -= sum(len(bytes(frame.data)) for frame in event.frames)
        await asyncio.sleep(0)

    try:
        tiny_voice, voice = b"\xff\x0f" * 80, b"\xff\x0f" * 1536
        tiny_quiet, quiet = b"\0\0" * 80, b"\0\0" * 1024
        await push(tiny_voice)
        assert vad.VADEventType.START_OF_SPEECH in public
        assert not any(event.type is vad.VADEventType.START_OF_SPEECH for event in events)
        await push(voice)
        assert any(event.type is vad.VADEventType.START_OF_SPEECH for event in events)
        await push(tiny_quiet)
        assert not any(event.type is vad.VADEventType.END_OF_SPEECH for event in events)
        await push(quiet)
        stream.end_input()
        await asyncio.wait_for(collector, 3)
        actual = b"".join(bytes(frame.data) for event in events if event.type is vad.VADEventType.INFERENCE_DONE for frame in event.frames)
        assert actual == tiny_voice + voice + tiny_quiet + quiet
        ended = [event for event in events if event.type is vad.VADEventType.END_OF_SPEECH]
        assert len(ended) == 1
        assert ended[0].silence_duration >= 0.02
        assert b"".join(bytes(frame.data) for frame in ended[0].frames) in actual
        assert events[-1].timestamp == pytest.approx(len(actual) / 2 / 16000)
        rate = 8000 if engine == "silero8" else 16000
        for event in events:
            assert event.samples_index == round(event.timestamp * rate)
    finally:
        await stream.aclose()
        await asyncio.gather(collector, return_exceptions=True)


@pytest.mark.parametrize("engine", ["inference", "silero16", "silero8"])
async def test_draining_framework_vad_streams_keep_independent_audio_and_cleanup(engine):
    detector = DrainingVAD(owned_probability_vad(engine))
    speaking, silent = detector.stream(), detector.stream()
    voice, quiet = b"\xff\x0f" * 1024, b"\0\0" * 320
    try:
        speaking.push_frame(rtc.AudioFrame(data=voice, sample_rate=16000, num_channels=1, samples_per_channel=1024))
        silent.push_frame(rtc.AudioFrame(data=quiet, sample_rate=16000, num_channels=1, samples_per_channel=320))
        speaking.end_input()
        silent.end_input()

        async def collect(stream):
            return [event async for event in stream]

        spoken, paused = await asyncio.wait_for(asyncio.gather(collect(speaking), collect(silent)), 3)
        for events, expected in ((spoken, voice), (paused, quiet)):
            assert b"".join(bytes(frame.data) for event in events if event.type is vad.VADEventType.INFERENCE_DONE for frame in event.frames) == expected
        assert any(event.type is vad.VADEventType.START_OF_SPEECH for event in spoken)
        assert not any(event.type is vad.VADEventType.START_OF_SPEECH for event in paused)
    finally:
        await asyncio.gather(speaking.aclose(), silent.aclose())
    for stream in (speaking, silent):
        assert stream._task.done() and stream._metrics_task.done()
        assert stream._inner._task.done() and stream._inner._metrics_task.done()


@pytest.mark.parametrize("finish", [False, True])
async def test_draining_framework_vad_end_input_keeps_audio_behind_inflight_padding(finish):
    detector = owned_probability_vad("inference", min_speech_duration=0.02)
    started, ended, release = asyncio.Event(), asyncio.Event(), threading.Event()
    loop, original = asyncio.get_running_loop(), detector.stream

    def paused_stream():
        result = original()
        predictor, end_input = result._native_vad, result.end_input

        class Paused:
            first = True

            def predict(self, window):
                if self.first:
                    self.first = False
                    loop.call_soon_threadsafe(started.set)
                    if not release.wait(3):
                        raise RuntimeError("owned inference release timed out")
                return predictor.predict(window)

            def reset(self):
                predictor.reset()

        def end():
            end_input()
            ended.set()

        result._native_vad, result.end_input = Paused(), end
        return result

    detector.stream = paused_stream
    stream = DrainingVAD(detector).stream()
    quiet, voice = b"\0\0" * 320, b"\xff\x0f" * 320
    try:
        stream.push_frame(rtc.AudioFrame(data=quiet, sample_rate=16000, num_channels=1, samples_per_channel=320))
        await asyncio.wait_for(started.wait(), 3)
        stream.push_frame(rtc.AudioFrame(data=voice, sample_rate=16000, num_channels=1, samples_per_channel=320))
        if finish:
            stream.end_input()
            await asyncio.wait_for(ended.wait(), 3)
        else:
            # The later real frontier's idle deadline expires while the first
            # padding window is still in inference. No further input follows.
            await asyncio.sleep(0.15)
        release.set()

        async def collect():
            events, processed = [], 0
            async for event in stream:
                events.append(event)
                if event.type is vad.VADEventType.INFERENCE_DONE:
                    processed += sum(len(bytes(frame.data)) for frame in event.frames)
                if not finish and processed == len(quiet + voice):
                    break
            return events

        events = await asyncio.wait_for(collect(), 3)
        assert b"".join(bytes(frame.data) for event in events if event.type is vad.VADEventType.INFERENCE_DONE for frame in event.frames) == quiet + voice
        if finish:
            assert sum(event.type is vad.VADEventType.START_OF_SPEECH for event in events) == 1
    finally:
        release.set()
        await stream.aclose()


@pytest.mark.parametrize("engine", ["inference", "silero16", "silero8"])
@pytest.mark.parametrize("queued", [False, True])
async def test_draining_framework_vad_flush_is_a_real_hard_boundary(engine, queued):
    detector = DrainingVAD(owned_probability_vad(engine, min_speech_duration=0.02, prefix_padding_duration=0, _any_voice=True))
    stream = detector.stream()
    first, second = b"\xff\x0f" * (1024 if queued else 160), b"\xff\x0f" * 320
    try:
        stream.push_frame(rtc.AudioFrame(data=first, sample_rate=16000, num_channels=1, samples_per_channel=len(first) // 2))
        if queued:
            async def processed():
                while getattr(stream, "_processed", 0) < len(first) // 2:
                    await asyncio.sleep(0.005)
            await asyncio.wait_for(processed(), 3)
        stream.flush()
        stream.push_frame(rtc.AudioFrame(data=second, sample_rate=16000, num_channels=1, samples_per_channel=320))
        stream.end_input()

        async def collect():
            return [event async for event in stream]

        events = await asyncio.wait_for(collect(), 3)
        actual = b"".join(bytes(frame.data) for event in events if event.type is vad.VADEventType.INFERENCE_DONE for frame in event.frames)
        expected = second if queued else first + second
        assert actual == expected, "flush reclassifies unprocessed capture with fresh counters"
        starts = [event for event in events if event.type is vad.VADEventType.START_OF_SPEECH]
        assert len(starts) == 1
        assert b"".join(bytes(frame.data) for frame in starts[0].frames) == expected
        assert starts[0].speech_duration == pytest.approx(len(expected) / 2 / 16000)
        assert starts[0].timestamp == pytest.approx(len(expected) / 2 / 16000)
    finally:
        await stream.aclose()


@pytest.mark.parametrize("engine", ["inference", "silero16", "silero8"])
async def test_draining_framework_vad_buffered_silence_has_no_packet_duration_delay(engine):
    stream = DrainingVAD(owned_probability_vad(engine, min_silence_duration=0.25)).stream()
    events = asyncio.Queue()

    async def collect():
        async for event in stream:
            events.put_nowait(event)

    async def boundary(kind):
        while True:
            event = await events.get()
            if event.type is kind:
                return event

    collector = asyncio.create_task(collect())
    try:
        stream.push_frame(rtc.AudioFrame(data=b"\xff\x0f" * 2048, sample_rate=16000, num_channels=1, samples_per_channel=2048))
        await asyncio.wait_for(boundary(vad.VADEventType.START_OF_SPEECH), 3)
        stream.push_frame(rtc.AudioFrame(data=b"\0\0" * 4000, sample_rate=16000, num_channels=1, samples_per_channel=4000))
        ended = await asyncio.wait_for(boundary(vad.VADEventType.END_OF_SPEECH), 0.22)
        assert ended.silence_duration >= 0.25
    finally:
        await stream.aclose()
        await asyncio.gather(collector, return_exceptions=True)


@pytest.mark.parametrize("mode", ["vad", "stt_flush", "stt_flush_resume"])
async def test_draining_framework_vad_preserves_actual_response_interruption(mode):
    if "capabilities" not in inspect.signature(io.AudioOutput).parameters:
        pytest.skip("SDK predates the current audio output contract")
    started, blocked, release = asyncio.Event(), asyncio.Event(), threading.Event()
    hold, streams = asyncio.Event(), []
    source = owned_probability_vad("inference", min_speech_duration=0.02, min_silence_duration=0.25, _any_voice=True)
    original, loop = source.stream, asyncio.get_running_loop()

    def owned_stream():
        inner, count = original(), 0
        predictor = inner._native_vad

        class Predictor:
            def predict(self, window):
                nonlocal count
                count += 1
                if mode.startswith("stt_flush") and count == 16:
                    loop.call_soon_threadsafe(blocked.set)
                    if not release.wait(3):
                        raise RuntimeError("owned inference release timed out")
                return predictor.predict(window)

            def reset(self):
                predictor.reset()

        inner._native_vad = Predictor()
        return inner

    source.stream = owned_stream
    detector = DrainingVAD(source)
    original_stream = detector.stream

    def framework_stream():
        stream = original_stream()
        streams.append(stream)
        return stream

    detector.stream = framework_stream

    class LocalSTT(stt.STT):
        def __init__(self):
            super().__init__(capabilities=stt.STTCapabilities(streaming=True, interim_results=False))
            self.started = asyncio.Queue()

        async def _recognize_impl(self, *args, **kwargs):
            raise NotImplementedError

        def stream(self, *, language=None, conn_options=DEFAULT_API_CONNECT_OPTIONS):
            return LocalStream(stt=self, conn_options=conn_options)

    class LocalStream(stt.RecognizeStream):
        async def _run(self):
            self._stt.started.put_nowait(self)
            async for _ in self._input_ch:
                pass

    class Output(io.AudioOutput):
        def __init__(self):
            super().__init__(label="owned-response", capabilities=io.AudioOutputCapabilities(pause=False))

        async def capture_frame(self, frame):
            await super().capture_frame(frame)
            started.set()

        def flush(self):
            super().flush()
            self.on_playback_finished(playback_position=0.02, interrupted=False)

        def clear_buffer(self):
            self.on_playback_finished(playback_position=0.02, interrupted=True)

    def frame(raw):
        return rtc.AudioFrame(data=raw, sample_rate=16000, num_channels=1, samples_per_channel=len(raw) // 2)

    async def response():
        yield frame(b"\0\0" * 320)
        await hold.wait()

    async def until(predicate):
        while not predicate():
            await asyncio.sleep(0.005)

    local, audio = LocalSTT(), OwnedAudioInput()
    session = AgentSession(stt=local, llm=None, tts=None, vad=detector, turn_detection="vad" if mode == "vad" else "stt", min_interruption_duration=0.5, resume_false_interruption=False)
    session.input.audio, session.output.audio = audio, Output()
    try:
        options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
        await session.start(agent=Agent(instructions="Owned response interruption"), **options)
        stt_stream = await asyncio.wait_for(local.started.get(), 3)
        speech = session.say("Owned ongoing response", audio=response(), allow_interruptions=True)
        await asyncio.wait_for(started.wait(), 3)
        await audio.frames.put(frame(b"\xff\x0f" * 7680))  # 480 ms of captured speech
        await asyncio.wait_for(until(lambda: streams and streams[0]._processed >= 7680), 3)
        assert not speech.interrupted
        if mode == "vad":
            await audio.frames.put(frame(b"\0\0" * 4096))
            await asyncio.wait_for(until(lambda: speech.interrupted), 2)
        else:
            quiet, voiced = (4096,1024) if mode == "stt_flush_resume" else (1024,80)
            await audio.frames.put(frame(b"\0\0" * quiet + b"\xff\x0f" * voiced))
            await asyncio.wait_for(blocked.wait(), 3)
            stt_stream._event_ch.send_nowait(stt.SpeechEvent(type=stt.SpeechEventType.END_OF_SPEECH))
            await asyncio.wait_for(until(lambda: streams[0]._flushes > 0), 3)
            release.set()
            await asyncio.wait_for(until(lambda: streams[0]._segment == streams[0]._flushes), 3)
            await asyncio.sleep(0.1)
            assert not speech.interrupted, "pre-EOS speech cannot interrupt the response after flush"
            assert session._activity._audio_recognition._speaking is (mode == "stt_flush_resume")
            assert session._activity._audio_recognition._vad_speech_started is (mode == "stt_flush_resume")
            await audio.frames.put(frame(b"\xff\x0f" * 9216))
            await asyncio.wait_for(until(lambda: speech.interrupted), 2)
    finally:
        release.set()
        hold.set()
        await audio.frames.put(None)
        await session.aclose()
        await local.aclose()


@pytest.mark.parametrize("engine", ["inference", "silero16", "silero8"])
@pytest.mark.parametrize("binding", ["session", "agent"])
@pytest.mark.parametrize("wrapper", ["direct", "nested"])
async def test_native_worker_framework_vad_and_local_turn_detector_drain_partial_silence(engine, binding, wrapper):
    source = owned_probability_vad("inference", min_silence_duration=0.032)
    framework_vad = DrainingVAD(owned_probability_vad(engine, min_silence_duration=0.25))
    predictions, turns, packets = [], asyncio.Queue(), []
    detector = owned_local_turn_detector(predictions)

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        index = len(packets)
        packets.append(bytearray())
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.BINARY:
                packets[index].extend(message.data)
            elif json.loads(message.data)["type"] == "session.close":
                await ws.send_json({"type": "transcript.final", "text": f"worker turn {index}"})
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    class OwnedAgent(Agent):
        async def on_user_turn_completed(self, turn_ctx, new_message):
            turns.put_nowait(new_message.text_content)

    async with peer(handle) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin)
        model = STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=source, diarization=wrapper == "nested")
        wrapped = wrap_native_stt(model, wrapper)
        session = AgentSession(
            stt=wrapped, llm=None, tts=None, vad=framework_vad,
            turn_handling={"turn_detection": detector, "endpointing": {"min_delay": 0.05, "max_delay": 0.5}, "interruption": {"mode": "vad", "enabled": True}},
        )
        audio = OwnedAudioInput()
        session.input.audio = audio
        agent = OwnedAgent(instructions="Owned worker detector integration")
        if binding == "session":
            model.bind_session(session)
        else:
            model.bind_agent(agent)
        try:
            await session.start(agent=agent, record=False)
            all_captured = b""
            for index in range(3):
                # The first pause reaches 250ms only in a partial inference
                # window. Later turns have ordinary silence margin, since
                # a mixed speech/silence window can legitimately classify
                # some captured quiet as speech after a previous padded tail.
                payload = b"\xff\x0f" * 1024 + b"\0\0" * (4000 if index == 0 else 5120)
                all_captured += payload
                await audio.frames.put(rtc.AudioFrame(data=payload, sample_rate=16000, num_channels=1, samples_per_channel=len(payload) // 2))
                try:
                    committed = await asyncio.wait_for(turns.get(), 3)
                except TimeoutError:
                    state = session._activity._audio_recognition
                    pytest.fail(f"turn {index} stalled: requests={list(map(len, packets))}, predictions={len(predictions)}, speaking={state._speaking}, transcript={state._audio_transcript!r}, streams={[(value._available_samples, value._pending_transcript, value._vad_speaking) for value in model._streams]}")
                assert committed == f"worker turn {index}"
                assert len(predictions) >= index + 1, "the local audio turn detector must participate in each turn"
                assert predictions[-1].tobytes() == payload, "VAD-only padding must not enter the independent turn detector"
            assert len(packets) == 3
            delivered = b"".join(map(bytes, packets))
            assert delivered == all_captured[:len(delivered)]
            assert not session._activity._audio_recognition._speaking
            assert not model._native_failed
        finally:
            await audio.frames.put(None)
            await session.aclose()
            await wrapped.aclose()
            await model.aclose()
            await client.aclose()


async def test_native_end_input_drains_new_real_audio_behind_inflight_padding():
    from livekit.agents import inference

    if not hasattr(inference, "VAD"):
        pytest.skip("SDK predates local inference VAD")
    detector = owned_probability_vad("inference", min_speech_duration=0.02, min_silence_duration=0.032)
    started, release = asyncio.Event(), threading.Event()
    loop, original = asyncio.get_running_loop(), detector.stream

    def paused_stream():
        result = original()
        predictor = result._native_vad

        class Paused:
            first = True

            def predict(self, window):
                if self.first:
                    self.first = False
                    loop.call_soon_threadsafe(started.set)
                    if not release.wait(3):
                        raise RuntimeError("owned inference release timed out")
                return predictor.predict(window)

            def reset(self):
                predictor.reset()

        result._native_vad = Paused()
        return result

    detector.stream = paused_stream
    requests = []

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        requests.append([])
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.BINARY:
                requests[0].append(message.data)
            elif message.type is aiohttp.WSMsgType.TEXT and json.loads(message.data)["type"] == "session.close":
                await ws.send_json({"type": "transcript.final", "text": "complete"})
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with peer(handle) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin)
        model = STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=detector)
        stream = model.stream(manual_turns=True)
        try:
            quiet, voice = b"\0\0" * 320, b"\xff\x0f" * 320
            stream.push_frame(rtc.AudioFrame(data=quiet, sample_rate=16000, num_channels=1, samples_per_channel=320))
            await asyncio.wait_for(started.wait(), 3)
            stream.push_frame(rtc.AudioFrame(data=voice, sample_rate=16000, num_channels=1, samples_per_channel=320))
            stream.end_input()

            async def ended():
                while not stream._input_finished:
                    await asyncio.sleep(0)

            await asyncio.wait_for(ended(), 3)
            release.set()

            async def collect():
                return [event async for event in stream]

            events = await asyncio.wait_for(collect(), 3)
            assert len(requests) == 1, "the final 8ms must complete the real 20ms speech minimum"
            assert b"".join(requests[0]) == quiet + voice
            assert [event.alternatives[0].text for event in events if event.type is stt.SpeechEventType.FINAL_TRANSCRIPT] == ["complete"]
            assert not model._native_failed
        finally:
            release.set()
            await stream.aclose()
            await model.aclose()
            await client.aclose()


async def test_native_continuous_40ms_packets_do_not_receive_idle_padding():
    windows, received = [], []
    detector = owned_probability_vad("inference", min_speech_duration=0.05, min_silence_duration=0.032, _windows=windows)

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.BINARY:
                received.append(message.data)
            elif message.type is aiohttp.WSMsgType.TEXT and json.loads(message.data)["type"] == "session.close":
                await ws.send_json({"type": "transcript.final", "text": "complete"})
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with peer(handle) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin)
        model = STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=detector)
        stream = model.stream(manual_turns=True)
        try:
            data = b"\x11\x11" * 640
            for _ in range(5):
                stream.push_frame(rtc.AudioFrame(data=data, sample_rate=16000, num_channels=1, samples_per_channel=640))
                await asyncio.sleep(0.04)
            assert windows and all(all(value > 0 for value in window) for window in windows), "continuous captured speech must not be interleaved with synthetic silence"
            stream.end_input()

            async def collect():
                return [event async for event in stream]

            await asyncio.wait_for(collect(), 3)
            assert b"".join(received) == data * 5
            assert not model._native_failed
        finally:
            await stream.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize(
    "gap,speech_duration,terminal_delay",
    [(0.35, 0.02, 0.35), (0.224, 0.064, 0.35), (1.0, 0.064, 1.0)],
)
async def test_actual_agent_session_commits_complete_native_turns_after_delayed_finals(
    gap, speech_duration, terminal_delay
):
    source = ControlledVAD(inference_rate=8000)
    late, first_audio, commit_received = (
        asyncio.Event(),
        asyncio.Event(),
        asyncio.Event(),
    )
    seen = []
    second_admitted = asyncio.Event()

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        index = len(seen)
        seen.append([])
        if index == 1:
            second_admitted.set()
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.BINARY:
                seen[index].append(message.data)
                if len(seen[index]) == 1:
                    await ws.send_json(
                        {
                            "type": "transcript.final",
                            "text": "first" if index == 0 else "second",
                        }
                    )
                first_audio.set()
                continue
            value = json.loads(message.data)
            if value["type"] == "input.commit" and index == 0:
                commit_received.set()
                await late.wait()
                await ws.send_json(
                    {"type": "transcript.final", "text": "late remainder"}
                )
            if value["type"] == "session.close":
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with peer(handle) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_fixture",
            session_id=str(uuid.uuid4()),
            base_url=origin,
        )
        model = STT(
            client,
            provider="fixture",
            model="fixture-model",
            credential_source="managed",
            turn_vad=source,
        )
        session = AgentSession(
            stt=model,
            llm=None,
            tts=None,
            vad=None,
            turn_detection="stt",
            min_endpointing_delay=0.2,
            max_endpointing_delay=0.2,
        )
        audio = OwnedAudioInput()
        session.input.audio = audio
        turns = asyncio.Queue()

        class OwnedAgent(Agent):
            async def on_user_turn_completed(self, turn_ctx, new_message):
                turns.put_nowait(new_message.text_content)

        try:
            start_options = (
                {"record": False}
                if "record" in inspect.signature(session.start).parameters
                else {}
            )
            agent = OwnedAgent(instructions="Owned local speech fixture")
            model.bind_agent(agent)
            await session.start(
                agent=agent,
                **start_options,
            )
            first = frame()
            second = rtc.AudioFrame(
                data=b"\x22\x11" * round(speech_duration * 16000),
                sample_rate=16000,
                num_channels=1,
                samples_per_channel=round(speech_duration * 16000),
            )
            silence = rtc.AudioFrame(
                data=b"\0\0" * round((gap - speech_duration) * 16000),
                sample_rate=16000,
                num_channels=1,
                samples_per_channel=round((gap - speech_duration) * 16000),
            )
            trailing = rtc.AudioFrame(
                data=b"\0\0" * 8800,
                sample_rate=16000,
                num_channels=1,
                samples_per_channel=8800,
            )
            await audio.frames.put(first)
            await audio.frames.put(trailing)
            await audio.frames.put(silence)
            await audio.frames.put(second)
            await asyncio.wait_for(source.frames.get(), 3)
            await asyncio.wait_for(source.frames.get(), 3)
            await asyncio.wait_for(source.frames.get(), 3)
            await asyncio.wait_for(source.frames.get(), 3)
            source.processed(first)
            source.emit(vad.VADEventType.START_OF_SPEECH)
            await asyncio.wait_for(first_audio.wait(), 3)
            source.processed(trailing)
            source.emit(vad.VADEventType.END_OF_SPEECH, silence_duration=0.55)
            source.processed(silence)
            source.processed(second)
            source.emit(
                vad.VADEventType.START_OF_SPEECH,
                frames=[second],
                speech_duration=speech_duration,
            )
            source.emit(vad.VADEventType.END_OF_SPEECH, frames=[second])
            await asyncio.wait_for(commit_received.wait(), 3)
            await asyncio.sleep(terminal_delay)
            assert turns.empty(), (
                "AgentSession committed an incomplete native transcript"
            )
            assert seen[0] == [bytes(first.data), bytes(trailing.data)]
            late.set()
            # The framework's actual commit releases admission. An elapsed
            # one-second gap must not be replayed as another second of delay.
            await asyncio.wait_for(second_admitted.wait(), 0.8)
            assert await asyncio.wait_for(turns.get(), 3) == "first late remainder"
            assert await asyncio.wait_for(turns.get(), 3) == "second"
            assert seen == [
                [bytes(first.data), bytes(trailing.data)],
                [bytes(second.data)],
            ]
        finally:
            late.set()
            await audio.frames.put(None)
            await session.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("boundary", ["start", "end"])
@pytest.mark.parametrize("engine", ["inference", "silero16", "silero8"])
async def test_native_padding_cannot_satisfy_real_speech_or_silence_minimums(boundary, engine):
    public, turns, requests, finished = [], asyncio.Queue(), [], asyncio.Queue()
    detector = owned_probability_vad(
        engine, min_speech_duration=0.02 if boundary == "start" else 0.05,
        min_silence_duration=0.032 if boundary == "start" else 0.02,
        _events=public, _any_voice=True,
    )

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        index = len(requests)
        requests.append([])
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.BINARY:
                requests[index].append(message.data)
            elif message.type is aiohttp.WSMsgType.TEXT and json.loads(message.data)["type"] == "session.close":
                await ws.send_json({"type": "transcript.final", "text": "first" if index == 0 else "second"})
                await ws.send_json({"type": "session.closed", "usage": {}})
                finished.put_nowait(index)
                await ws.close()
        return ws

    class OwnedAgent(Agent):
        async def on_user_turn_completed(self, turn_ctx, new_message):
            turns.put_nowait(new_message.text_content)

    def pcm(data):
        return rtc.AudioFrame(data=data, sample_rate=16000, num_channels=1, samples_per_channel=len(data) // 2)

    async with peer(handle) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin)
        model = STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=detector)
        session = AgentSession(stt=model, llm=None, tts=None, vad=None, turn_detection="stt", min_endpointing_delay=0.2, max_endpointing_delay=0.2)
        audio = OwnedAudioInput()
        session.input.audio = audio
        model.bind_session(session)
        try:
            options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
            await session.start(agent=OwnedAgent(instructions="Owned real duration thresholds"), **options)
            voice, quiet = b"\xff\x0f" * 512, b"\0\0" * 512
            await audio.frames.put(pcm(voice * 2 + (quiet if boundary == "start" else b"")))
            if boundary == "start":
                assert await asyncio.wait_for(finished.get(), 3) == 0
            await audio.frames.put(pcm((b"\xff\x0f" if boundary == "start" else b"\0\0") * 80))
            await asyncio.sleep(0.35)
            assert turns.empty()
            if boundary == "start":
                assert public.count(vad.VADEventType.START_OF_SPEECH) >= 2, "SDK virtual duration has reached its threshold"
                assert len(requests) == 1, "5ms of real speech must not gain a second admission"
            else:
                assert public.count(vad.VADEventType.END_OF_SPEECH) >= 1, "SDK virtual silence has reached its threshold"
                assert finished.empty(), "5ms of real silence must not close a request"
            await audio.frames.put(pcm(voice * 3 + quiet * 3 if boundary == "start" else quiet * 3))
            expected = "first second" if boundary == "start" else "first"
            assert await asyncio.wait_for(turns.get(), 3) == expected
            assert not model._native_failed
        finally:
            await audio.frames.put(None)
            await session.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("engine", ["inference", "silero16", "silero8"])
@pytest.mark.parametrize("wrapper", ["direct", "multi_speaker"])
async def test_native_repeated_padded_frontiers_keep_provider_pcm_and_source_times(engine, wrapper):
    detector = owned_probability_vad(engine, min_speech_duration=0.05, min_silence_duration=0.032)
    requests, turns, finals, source = [], asyncio.Queue(), asyncio.Queue(), []
    captured = 0

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        index = len(requests)
        requests.append([])
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.BINARY:
                requests[index].append(message.data)
            elif message.type is aiohttp.WSMsgType.TEXT and json.loads(message.data)["type"] == "session.close":
                await ws.send_json({"type": "transcript.final", "text": f"turn{index}", "speaker": "A", "segments": [{"text": f"turn{index}", "speaker": "A", "start_ms": 0, "end_ms": 20}]})
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    class OwnedAgent(Agent):
        async def stt_node(self, audio, settings):
            async def capture():
                nonlocal captured
                async for value in audio:
                    witness = value.userdata["_speko_native_input_position"]
                    source.append((captured, captured + value.samples_per_channel, float(witness.start)))
                    captured += value.samples_per_channel
                    yield value
            async for event in Agent.default.stt_node(self, capture(), settings):
                if event.type is stt.SpeechEventType.FINAL_TRANSCRIPT:
                    finals.put_nowait(event.alternatives[0])
                yield event

        async def on_user_turn_completed(self, turn_ctx, new_message):
            turns.put_nowait(new_message.text_content)

    def source_time(sample):
        for start, end, timestamp in source:
            if start <= sample < end:
                return timestamp + (sample - start) / 16000
        raise AssertionError("provider timestamp exceeded captured input")

    async with peer(handle) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin)
        model = STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=detector, diarization=wrapper == "multi_speaker")
        wrapped = wrap_native_stt(model, wrapper)
        session = AgentSession(stt=wrapped, llm=None, tts=None, vad=None, turn_detection="stt", min_endpointing_delay=0.05, max_endpointing_delay=0.05)
        audio = OwnedAudioInput()
        session.input.audio = audio
        model.bind_session(session)
        try:
            options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
            await session.start(agent=OwnedAgent(instructions="Owned repeated detector padding"), **options)
            # Neither captured waveform contains zero bytes. Any invented
            # silence reaching a physical request is directly observable.
            voice, quiet = b"\x11\x11" * 1024, b"\x01\x01" * (1024 + 320)
            payload = voice + quiet
            all_captured = b""
            assigned = 0
            for index in range(4):
                all_captured += payload
                await audio.frames.put(rtc.AudioFrame(data=payload, sample_rate=16000, num_channels=1, samples_per_channel=len(payload) // 2))
                assert await asyncio.wait_for(turns.get(), 3) == f"turn{index}"
                data = await asyncio.wait_for(finals.get(), 3)
                raw = b"".join(requests[index])
                assert raw and b"\0" not in raw
                assert raw == all_captured[assigned * 2:assigned * 2 + len(raw)]
                assert data._native_input_times == pytest.approx((assigned / 16000, (assigned + 320) / 16000))
                assert data.start_time == pytest.approx(source_time(assigned), abs=1 / 16000)
                assert data.end_time == pytest.approx(source_time(assigned + 320), abs=1 / 16000)
                assigned += len(raw) // 2
            assert len(requests) == 4
            assert not model._native_failed
        finally:
            await audio.frames.put(None)
            await session.aclose()
            await wrapped.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("wrapper", ["direct", "multi_speaker", "fallback", "nested"])
@pytest.mark.parametrize("buffered,replace_input", [
    pytest.param(False, False, id="fresh"),
    pytest.param(True, False, id="buffered"),
    pytest.param(False, True, id="replaced_input"),
    pytest.param(False, "delegate", id="delegated_input"),
])
@pytest.mark.parametrize("binding", ["session", "agent"])
async def test_native_initial_node_timestamps_follow_the_sdk_input_epoch(wrapper, buffered, replace_input, binding):
    source, waiting, resume = ControlledVAD(), asyncio.Event(), asyncio.Event()
    turns, positions = asyncio.Queue(), []

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.TEXT and json.loads(message.data)["type"] == "session.close":
                await ws.send_json({"type": "transcript.final", "text": "complete", "segments": [{"text": "complete", "start_ms": 0, "end_ms": 20}]})
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    class OwnedAgent(Agent):
        async def stt_node(self, audio, settings):
            if replace_input:
                await anext(audio)
            waiting.set()
            await resume.wait()
            async for event in Agent.default.stt_node(self, audio, settings):
                if event.type in (stt.SpeechEventType.FINAL_TRANSCRIPT, stt.SpeechEventType.END_OF_SPEECH):
                    positions.append(event.alternatives[0].end_time)
                yield event

        async def on_user_turn_completed(self, turn_ctx, new_message):
            turns.put_nowait(new_message.text_content)

    async with peer(handle) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin)
        model = STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=source, diarization=wrapper in ("multi_speaker", "nested"))
        recognition_model = wrap_native_stt(model, wrapper)
        session = AgentSession(stt=recognition_model, llm=None, tts=None, vad=None, turn_detection="stt", min_endpointing_delay=0.05, max_endpointing_delay=0.05)
        audio = OwnedAudioInput(track_delivery=True)
        session.input.audio = audio
        if replace_input:
            await audio.frames.put(frame())
        original_forward = session._forward_audio_task
        agent = OwnedAgent(instructions="Owned initial input epoch")
        if binding == "session":
            model.bind_session(session)
        else:
            model.bind_agent(agent)
        first_frame = frame()
        first_frame.userdata["producer"] = {"fixture": "original"}
        original_data = bytes(first_frame.data)
        try:
            options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
            await session.start(agent=agent, **options)
            await asyncio.wait_for(waiting.wait(), 3)
            if replace_input:
                await asyncio.sleep(0.35)
                if replace_input == "delegate":
                    session.input.audio = io.AudioInput(label="owned-delegated-input", source=session.input.audio)
                else:
                    audio = OwnedAudioInput(track_delivery=True)
                    session.input.audio = audio
            if buffered:
                await audio.frames.put(first_frame)
                # Establish delivery before delaying the node: its startup
                # cannot legitimately move this buffered frame's timestamp.
                await asyncio.wait_for(asyncio.shield(audio.delivery), 3)
            await asyncio.sleep(0.35)
            resume.set()
            if not buffered:
                await audio.frames.put(first_frame)
            observed = await asyncio.wait_for(source.frames.get(), 3)
            source.processed(observed)
            source.emit(vad.VADEventType.START_OF_SPEECH, frames=[observed])
            await audio.frames.put(rtc.AudioFrame(data=b"\0\0" * 320, sample_rate=16000, num_channels=1, samples_per_channel=320))
            observed = await asyncio.wait_for(source.frames.get(), 3)
            source.processed(observed)
            source.emit(vad.VADEventType.END_OF_SPEECH, frames=[observed], silence_duration=0.02)
            assert await asyncio.wait_for(turns.get(), 3) == "complete"
            assert len(positions) == 2
            legacy = not hasattr(session._activity._audio_recognition, "_input_started_at")
            end = 0.04 if replace_input == "delegate" or replace_input and not legacy else 0.02
            await assert_source_times(session, audio, positions, [end, end])
            assert first_frame.userdata == {"producer": {"fixture": "original"}}
            assert bytes(first_frame.data) == original_data
        finally:
            resume.set()
            await audio.frames.put(None)
            await session.aclose()
            if recognition_model is not model:
                await recognition_model.aclose()
            await model.aclose()
            await client.aclose()
            assert session._forward_audio_task == original_forward
            assert session.input.audio is None


@pytest.mark.parametrize("own_vad", [False, True])
@pytest.mark.parametrize("wrapper", ["direct", "nested"])
@pytest.mark.parametrize("first_terminal_delay", [0, 0.35])
async def test_native_continuation_is_transcribed_before_framework_turn_commit(own_vad, wrapper, first_terminal_delay):
    await assert_native_continuation(own_vad, wrapper, first_terminal_delay)


async def assert_native_continuation(own_vad, wrapper, first_terminal_delay, *, turn_detector=None, expected_turns=("first second",), wait_for_first_terminal=False, first_text="first"):
    source = ControlledVAD()
    session_vad = ControlledVAD() if own_vad else None
    admitted, terminal, turns, packets = asyncio.Queue(), asyncio.Queue(), asyncio.Queue(), []

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        index = len(packets)
        packets.append(bytearray())
        await ready(ws)
        admitted.put_nowait(index)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.BINARY:
                packets[index].extend(message.data)
            elif json.loads(message.data)["type"] == "session.close":
                await asyncio.sleep(first_terminal_delay if index == 0 else 0.35)
                await ws.send_json({"type": "transcript.final", "text": first_text if index == 0 else "second"})
                await ws.send_json({"type": "session.closed", "usage": {}})
                terminal.put_nowait(index)
                await ws.close()
        return ws

    class OwnedAgent(Agent):
        async def on_user_turn_completed(self, turn_ctx, new_message):
            turns.put_nowait(new_message.text_content)

    async with peer(handle) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin)
        model = STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=source, diarization=wrapper == "nested")
        recognition_model = wrap_native_stt(model, wrapper)
        session = AgentSession(stt=recognition_model, llm=None, tts=None, vad=session_vad, turn_detection=turn_detector if turn_detector is not None else "stt", min_endpointing_delay=0.05 if turn_detector is not None else 0.2, max_endpointing_delay=0.5 if turn_detector is not None else 0.2)
        audio = OwnedAudioInput()
        session.input.audio = audio

        async def boundary(samples, kind, silence=0):
            value = rtc.AudioFrame(data=(b"\0\0" if silence else b"\xff\x0f") * samples, sample_rate=16000, num_channels=1, samples_per_channel=samples)
            await audio.frames.put(value)
            for detector in (source, session_vad) if session_vad is not None else (source,):
                observed = await asyncio.wait_for(detector.frames.get(), 3)
                detector.processed(observed, raw_silence=silence if detector is session_vad else 0)
                detector.emit(kind, frames=[observed], silence_duration=silence)
            return bytes(value.data)

        model.bind_session(session)
        framework_recognition = None
        try:
            options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
            await session.start(agent=OwnedAgent(instructions="Owned continued user turn"), **options)
            framework_recognition = session._activity._audio_recognition
            first = await boundary(320, vad.VADEventType.START_OF_SPEECH)
            assert await asyncio.wait_for(admitted.get(), 3) == 0
            await asyncio.sleep(0.02)
            pause_seconds = 0.2 if turn_detector is not None else 0.02
            pause = await boundary(round(pause_seconds * 16000), vad.VADEventType.END_OF_SPEECH, pause_seconds)
            if wait_for_first_terminal:
                assert await asyncio.wait_for(terminal.get(), 3) == 0
            await asyncio.sleep(0.02)
            second = await boundary(320, vad.VADEventType.START_OF_SPEECH)
            if not wait_for_first_terminal:
                assert await asyncio.wait_for(terminal.get(), 3) == 0
            assert await asyncio.wait_for(admitted.get(), 0.1 if len(expected_turns) == 1 else 0.8) == 1, "resumed speech must transcribe before endpointing commits the first fragment"
            if len(expected_turns) == 1:
                assert turns.empty()
            else:
                assert await asyncio.wait_for(turns.get(), 3) == expected_turns[0]
            trailing = await boundary(8800, vad.VADEventType.END_OF_SPEECH, 0.55)
            assert await asyncio.wait_for(terminal.get(), 3) == 1
            assert await asyncio.wait_for(turns.get(), 3) == expected_turns[-1]
            assert turns.empty()
            assert list(map(bytes, packets)) == [first + pause, second + trailing]
        finally:
            await audio.frames.put(None)
            await session.aclose()
            if recognition_model is not model:
                await recognition_model.aclose()
            await model.aclose()
            await client.aclose()
            if framework_recognition is not None:
                assert framework_recognition._turn_detector is turn_detector


@pytest.mark.parametrize("probability", [0.1, 0.9])
@pytest.mark.parametrize("first_text", ["first", "first  part"])
async def test_native_turn_uses_actual_detector_decision_for_queued_intermediate_pause(probability, first_text):
    class OwnedTurnDetector:
        model, provider = "owned-turn-detector", "owned"

        async def supports_language(self, language):
            return True

        async def unlikely_threshold(self, language):
            return 0.5

        async def predict_end_of_turn(self, chat_ctx, *, timeout=None):
            await asyncio.sleep(0.01)
            return probability

    expected = (f"{first_text} second",) if probability < 0.5 else (first_text, "second")
    await assert_native_continuation(True, "nested", 0.35, turn_detector=OwnedTurnDetector(), expected_turns=expected, first_text=first_text)


@pytest.mark.skipif(not hasattr(recognition, "_StreamingTurnDetector"), reason="SDK predates streaming turn detection")
@pytest.mark.parametrize("probability", [0.1, 0.9])
@pytest.mark.parametrize("queued", [False, True])
async def test_native_continuation_with_streaming_turn_detection(probability, queued):
    expected = ("first second",) if queued or probability < 0.5 else ("first", "second")
    await assert_native_continuation(True, "nested", 0.35 if queued else 0, turn_detector=owned_streaming_turn_detector(probability), expected_turns=expected, wait_for_first_terminal=not queued)


def owned_streaming_turn_detector(probability):
    from livekit.agents.voice.turn import TurnDetectionEvent

    class OwnedStream:
        model, provider, is_fallback, prediction_timeout = "owned-stream", "owned", False, 1.0

        def __init__(self):
            self.audio = bytearray()
            self.closed = False

        async def supports_language(self, language):
            return True

        async def unlikely_threshold(self, language):
            return 0.5

        async def backchannel_threshold(self, language):
            return None

        def predict(self):
            future = asyncio.get_running_loop().create_future()
            future.set_result(TurnDetectionEvent(type="eot_prediction", end_of_turn_probability=probability, last_speaking_time=time.time()))
            return future

        def cancel_inference(self, *, timed_out=False):
            pass

        def push_audio(self, frame):
            self.audio.extend(frame.data.cast("B"))

        def flush(self, reason=None):
            pass

        def end_input(self):
            pass

        async def aclose(self):
            self.closed = True

    class OwnedDetector:
        model, provider = "owned-stream", "owned"

        def __init__(self):
            self.streams = []

        def stream(self, *, conn_options=None):
            stream = OwnedStream()
            self.streams.append(stream)
            return stream

    return OwnedDetector()


@pytest.mark.skipif(not hasattr(recognition.AudioRecognition, "_detach_turn_detector"), reason="SDK predates retained streaming turn detection")
async def test_native_handoffs_keep_one_live_turn_detector_stream_and_release_old_observers():
    source, session_vad = ControlledVAD(), ControlledVAD()
    entered, turns, seen = asyncio.Queue(), asyncio.Queue(), []
    detector = owned_streaming_turn_detector(0.9)

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        seen.append(request.headers["Idempotency-Key"])
        index = len(seen)
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.TEXT and json.loads(message.data)["type"] == "session.close":
                await ws.send_json({"type": "transcript.final", "text": f"turn {index}"})
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    class OwnedAgent(Agent):
        async def on_enter(self):
            entered.put_nowait(self)

        async def on_user_turn_completed(self, turn_ctx, new_message):
            turns.put_nowait(new_message.text_content)

    async with peer(handle) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin)
        model = STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=source)
        session = AgentSession(stt=model, llm=None, tts=None, vad=session_vad, turn_detection=detector, min_endpointing_delay=0.01, max_endpointing_delay=0.01)
        audio = OwnedAudioInput()
        session.input.audio = audio
        model.bind_session(session)
        recognitions, expected_audio = [], bytearray()
        try:
            options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
            for index in range(1, 4):
                agent = OwnedAgent(instructions="Owned streaming detector handoff")
                if index == 1:
                    await session.start(agent=agent, **options)
                else:
                    session.update_agent(agent)
                assert await asyncio.wait_for(entered.get(), 3) is agent
                recognitions.append(session._activity._audio_recognition)
                for samples, kind in [(320, vad.VADEventType.START_OF_SPEECH), (8800, vad.VADEventType.END_OF_SPEECH)]:
                    value = rtc.AudioFrame(data=(b"\xff\x0f" if samples == 320 else b"\0\0") * samples, sample_rate=16000, num_channels=1, samples_per_channel=samples)
                    expected_audio.extend(value.data.cast("B"))
                    await audio.frames.put(value)
                    for vad_model in (source, session_vad):
                        observed = await asyncio.wait_for(vad_model.frames.get(), 3)
                        vad_model.processed(observed, raw_silence=0.55 if samples == 8800 and vad_model is session_vad else 0)
                        vad_model.emit(kind, frames=[observed], silence_duration=0.55 if samples == 8800 else 0)
                assert await asyncio.wait_for(turns.get(), 3) == f"turn {index}"
                assert len(detector.streams) == 1
                assert not detector.streams[0].closed
                observed_stream = recognitions[-1]._turn_detector_stream
                assert observed_stream.original is detector.streams[0], "handoffs must not retain previous observers in a chain"
                for old in recognitions[:-1]:
                    assert old._turn_detector is detector
            assert len(seen) == len(set(seen)) == 3
            assert bytes(detector.streams[0].audio) == bytes(expected_audio)
        finally:
            await audio.frames.put(None)
            await session.aclose()
            await model.aclose()
            await client.aclose()
            assert detector.streams[0].closed
            assert all(item._turn_detector is detector for item in recognitions)


@pytest.mark.parametrize("input_rate", [16000, 48000])
async def test_native_turn_waits_for_vad_input_frontier_and_rearms_when_silence_drains(input_rate):
    source, terminal, turns, seen = ControlledVAD(), asyncio.Event(), asyncio.Queue(), []

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        seen.append(request.headers["Idempotency-Key"])
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.TEXT and json.loads(message.data)["type"] == "session.close":
                await asyncio.sleep(0.05)
                await ws.send_json({"type": "transcript.final", "text": "complete"})
                await ws.send_json({"type": "session.closed", "usage": {}})
                terminal.set()
                await ws.close()
        return ws

    class OwnedAgent(Agent):
        async def on_user_turn_completed(self, turn_ctx, new_message):
            turns.put_nowait(new_message.text_content)

    async with peer(handle) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin)
        model = STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=source)
        session = AgentSession(stt=model, llm=None, tts=None, vad=None, turn_detection="stt", min_endpointing_delay=0.1, max_endpointing_delay=0.1)
        audio = OwnedAudioInput()
        session.input.audio = audio
        model.bind_session(session)
        try:
            options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
            await session.start(agent=OwnedAgent(instructions="Owned VAD processing frontier"), **options)
            duration = 0.1 if input_rate != 16000 else 0.02
            for kind in (vad.VADEventType.START_OF_SPEECH, vad.VADEventType.END_OF_SPEECH):
                samples = round(input_rate * duration)
                await audio.frames.put(rtc.AudioFrame(data=b"\xff\x0f" * samples, sample_rate=input_rate, num_channels=1, samples_per_channel=samples))
                observed = await asyncio.wait_for(source.frames.get(), 3)
                source.processed(observed)
                source.emit(kind, frames=[observed], silence_duration=0.02 if kind is vad.VADEventType.END_OF_SPEECH else 0)
            samples = round(input_rate * duration)
            silence = rtc.AudioFrame(data=b"\0\0" * samples, sample_rate=input_rate, num_channels=1, samples_per_channel=samples)
            await audio.frames.put(silence)
            pending = await asyncio.wait_for(source.frames.get(), 3)
            await asyncio.wait_for(terminal.wait(), 3)
            await asyncio.sleep(0.25)
            assert turns.empty(), "an unprocessed input frame can still contain continued speech"
            source.processed(pending)
            assert await asyncio.wait_for(turns.get(), 1) == "complete", "draining silence must rearm a deferred turn without another transcript"
            assert len(seen) == 1
        finally:
            await audio.frames.put(None)
            await session.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.skipif(
    not hasattr(recognition, "_STTPipeline"),
    reason="This SDK release predates AgentSession's outer STT recovery pump",
)
@pytest.mark.parametrize("wrapper", ["direct", "multi_speaker", "fallback", "nested"])
async def test_actual_agent_stt_recovery_pump_cannot_readmit_a_failed_native_request(
    monkeypatch, wrapper,
):
    monkeypatch.setattr(recognition, "_STT_RECONNECT_INTERVAL", 0)
    seen = []
    source = ControlledVAD(automatic=True)

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        seen.append(request.headers["Idempotency-Key"])
        await ready(ws)
        await ws.send_json(
            {"type": "error", "error": {"code": "provider_failure", "retryable": True}}
        )
        await ws.close()
        return ws

    async with peer(handle) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_fixture",
            session_id=str(uuid.uuid4()),
            base_url=origin,
        )
        model = STT(
            client,
            provider="fixture",
            model="fixture-model",
            credential_source="managed",
            turn_vad=source,
            diarization=wrapper in ("multi_speaker", "nested"),
        )
        recognition_model = wrap_native_stt(model, wrapper)
        agent = Agent(instructions="Owned recovery fixture")
        model.bind_agent(agent)
        events = rtc.EventEmitter()
        agent._activity = SimpleNamespace(
            stt=recognition_model,
            vad=None,
            _audio_recognition=None,
            # Isolate the real SDK recovery pump; no successful framework turn
            # is authorized by this fixture. Actual sessions are covered below.
            on_end_of_turn=lambda _info: False,
            session=SimpleNamespace(
                on=events.on,
                off=events.off,
                current_agent=agent,
                conn_options=SimpleNamespace(
                    stt_conn_options=APIConnectOptions(max_retry=3, retry_interval=0)
                ),
                _recorder_io=None,
                _started_at=time.time(),
            ),
        )
        agent._activity.session._activity = agent._activity
        pipeline = recognition._STTPipeline(
            lambda audio, settings: agent.stt_node(audio, settings)
        )

        async def continue_input():
            for _ in range(100):
                if pipeline._pump_task.done():
                    return
                pipeline.audio_ch.send_nowait(frame())
                await asyncio.sleep(0.005)

        producer = asyncio.create_task(continue_input())
        try:
            with pytest.raises(GatewayError, match="control-plane recovery"):
                await asyncio.wait_for(pipeline._pump_task, 3)
            assert len(seen) == 1
            pipeline.audio_ch.send_nowait(frame())
            assert pipeline._pump_task.done()
            with pytest.raises(GatewayError, match="control-plane recovery"):
                model.stream()
            assert len(seen) == 1
        finally:
            await producer
            await pipeline.aclose()
            if recognition_model is not model:
                await recognition_model.aclose()
            await model.aclose()
            await client.aclose()


async def test_livekit_native_stt_orders_processed_audio_and_waits_for_all_turn_finals():
    source = ControlledVAD()
    late = asyncio.Event()
    committed = asyncio.Event()
    first_audio = asyncio.Event()
    seen = []

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        turn = len(seen)
        seen.append({"key": request.headers["Idempotency-Key"], "audio": []})
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.BINARY:
                seen[turn]["audio"].append(message.data)
                for suffix in ["a", "b"]:
                    await ws.send_json(
                        {"type": "transcript.final", "text": f"{turn + 1}{suffix}"}
                    )
                if turn == 0:
                    first_audio.set()
                continue
            value = json.loads(message.data)
            if value["type"] == "input.commit" and turn == 0:
                committed.set()
                await late.wait()
                await ws.send_json({"type": "transcript.final", "text": "late-1"})
            if value["type"] == "session.close":
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with peer(handle) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_fixture",
            session_id=str(uuid.uuid4()),
            base_url=origin,
        )
        model = STT(
            client,
            provider="fixture",
            model="fixture-model",
            credential_source="managed",
            turn_vad=source,
        )
        stream = model.stream(
            manual_turns=True,
            conn_options=APIConnectOptions(max_retry=3, retry_interval=0),
        )
        try:
            first, second = (
                frame(),
                rtc.AudioFrame(
                    data=b"\x22\x11" * 320,
                    sample_rate=16000,
                    num_channels=1,
                    samples_per_channel=320,
                ),
            )
            stream.push_frame(first)
            stream.push_frame(second)
            await asyncio.wait_for(source.frames.get(), 2)
            await asyncio.wait_for(source.frames.get(), 2)
            source.processed(first)
            source.emit(vad.VADEventType.START_OF_SPEECH)
            assert (
                await asyncio.wait_for(anext(stream), 2)
            ).type is stt.SpeechEventType.START_OF_SPEECH
            await asyncio.wait_for(first_audio.wait(), 2)
            source.emit(vad.VADEventType.END_OF_SPEECH)
            source.processed(second)
            await asyncio.wait_for(committed.wait(), 2)
            pending = asyncio.create_task(anext(stream))
            await asyncio.sleep(0.02)
            assert not pending.done(), (
                "a final segment cannot complete an unfinished native request"
            )
            assert seen[0]["audio"] == [bytes(first.data)]
            late.set()
            assert (await asyncio.wait_for(pending, 2)).alternatives[0].text == "1a"
            assert (await asyncio.wait_for(anext(stream), 2)).alternatives[
                0
            ].text == "1b"
            assert (await asyncio.wait_for(anext(stream), 2)).alternatives[
                0
            ].text == "late-1"
            assert (
                await asyncio.wait_for(anext(stream), 2)
            ).type is stt.SpeechEventType.END_OF_SPEECH
            source.emit(vad.VADEventType.START_OF_SPEECH, frames=[second])
            source.emit(vad.VADEventType.END_OF_SPEECH, frames=[second])
            assert (
                await asyncio.wait_for(anext(stream), 2)
            ).type is stt.SpeechEventType.START_OF_SPEECH
            for suffix in ["a", "b"]:
                assert (await asyncio.wait_for(anext(stream), 2)).alternatives[
                    0
                ].text == f"2{suffix}"
            assert (
                await asyncio.wait_for(anext(stream), 2)
            ).type is stt.SpeechEventType.END_OF_SPEECH
            assert seen[1]["audio"] == [bytes(second.data)]
            assert seen[0]["key"] != seen[1]["key"]
            stream.end_input()
            assert [event async for event in stream] == []
            assert len(seen) == 2
        finally:
            late.set()
            await stream.aclose()
            await model.aclose()
            await client.aclose()
        assert source.current._task.done()


@pytest.mark.parametrize(
    "failure", ["retryable-error", "setup-timeout", "lost-terminal"]
)
@pytest.mark.parametrize("method", ["stream", "synthesize"])
async def test_actual_livekit_tts_cannot_replay_native_speech_even_with_retry_enabled(
    failure, method
):
    seen = []
    inputs = []

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        seen.append(request.headers["Idempotency-Key"])
        if failure != "setup-timeout":
            await ready(ws)
        async for message in ws:
            value = json.loads(message.data)
            if value["type"] == "input.append":
                inputs.append(value["text"])
            if value["type"] == "input.commit":
                if failure == "retryable-error":
                    await ws.send_json(
                        {
                            "type": "error",
                            "error": {"code": "provider_failure", "retryable": True},
                        }
                    )
                await ws.close()
        return ws

    async with peer(handle) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_fixture",
            session_id=str(uuid.uuid4()),
            base_url=origin,
            setup_timeout=0.1,
        )
        model = TTS(
            client,
            provider="fixture",
            model="fixture-model",
            credential_source="managed",
        )
        options = APIConnectOptions(max_retry=3, retry_interval=0)
        stream = (
            model.stream(conn_options=options)
            if method == "stream"
            else model.synthesize("synthetic", conn_options=options)
        )
        if method == "stream":
            stream.push_text("synthetic")
            stream.end_input()
        try:
            with pytest.raises(APIConnectionError):
                async for _ in stream:
                    pass
            assert len(seen) == 1
            assert inputs == ([] if failure == "setup-timeout" else ["synthetic"])
            assert options.max_retry == 3
        finally:
            await stream.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("method", ["stream", "recognize"])
async def test_actual_livekit_stt_cannot_retry_an_ambiguous_native_error(method):
    seen = []

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        seen.append(request.headers["Idempotency-Key"])
        await ready(ws)
        await ws.send_json(
            {"type": "error", "error": {"code": "provider_failure", "retryable": True}}
        )
        await ws.close()
        return ws

    async with peer(handle) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_fixture",
            session_id=str(uuid.uuid4()),
            base_url=origin,
        )
        model = STT(
            client,
            provider="fixture",
            model="fixture-model",
            credential_source="managed",
            turn_vad=ControlledVAD(automatic=True),
        )
        stream = None
        try:
            with pytest.raises(APIConnectionError):
                if method == "recognize":
                    await model.recognize(
                        frame(),
                        conn_options=APIConnectOptions(max_retry=3, retry_interval=0),
                    )
                else:
                    stream = model.stream(
                        manual_turns=True,
                        conn_options=APIConnectOptions(max_retry=3, retry_interval=0),
                    )
                    stream.push_frame(frame())
                    stream.end_input()
                    async for _ in stream:
                        pass
            assert len(seen) == 1
        finally:
            if stream is not None:
                await stream.aclose()
            await model.aclose()
            await client.aclose()


async def test_native_livekit_stt_refuses_missing_speech_boundary_source_before_admission():
    client = RelaySpeechClient(
        api_key="sk_speko_agent_fixture",
        session_id=str(uuid.uuid4()),
        base_url="http://127.0.0.1:1",
    )
    try:
        with pytest.raises(ValueError, match="turn_vad"):
            STT(
                client,
                provider="fixture",
                model="fixture-model",
                credential_source="managed",
            )
    finally:
        await client.aclose()


async def test_native_recognition_chunks_a_completed_audio_buffer_and_keeps_every_final():
    payload = b"\xff\x0f" * ((1 << 20) + 320)
    observed = []

    async def handle(request):
        # aiohttp's uncompressed frame limit is exclusive; native Go peers
        # accept a message whose size equals their 2 MiB read limit.
        ws = web.WebSocketResponse(max_msg_size=(2 << 20) + 1)
        await ws.prepare(request)
        await ws.receive_json()
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.BINARY:
                observed.append(message.data)
                await ws.send_json(
                    {
                        "type": "transcript.final",
                        "text": "first" if len(observed) == 1 else "second",
                    }
                )
            elif (
                message.type is aiohttp.WSMsgType.TEXT
                and json.loads(message.data)["type"] == "session.close"
            ):
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        assert ws.exception() is None
        return ws

    async with peer(handle) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_fixture",
            session_id=str(uuid.uuid4()),
            base_url=origin,
        )
        model = STT(
            client,
            provider="fixture",
            model="fixture-model",
            credential_source="managed",
            turn_vad=ControlledVAD(),
        )
        try:
            result = await model.recognize(
                rtc.AudioFrame(
                    data=payload,
                    sample_rate=16000,
                    num_channels=1,
                    samples_per_channel=len(payload) // 2,
                )
            )
            assert result.alternatives[0].text == "first second"
            assert [len(raw) for raw in observed] == [2 << 20, 640]
            assert b"".join(observed) == payload
        finally:
            await model.aclose()
            await client.aclose()


async def test_native_stt_publishes_interims_while_audio_is_still_streaming():
    source = ControlledVAD()
    observed = []

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.BINARY:
                observed.append(message.data)
                await ws.send_json({"type": "transcript.delta", "text": "streaming"})
            elif json.loads(message.data)["type"] == "session.close":
                await ws.send_json({"type": "transcript.final", "text": "complete"})
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with peer(handle) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_fixture",
            session_id=str(uuid.uuid4()),
            base_url=origin,
        )
        model = STT(
            client,
            provider="fixture",
            model="fixture-model",
            credential_source="managed",
            turn_vad=source,
        )
        stream = model.stream(manual_turns=True)
        try:
            audio = frame()
            stream.push_frame(audio)
            await asyncio.wait_for(source.frames.get(), 2)
            source.processed(audio)
            source.emit(vad.VADEventType.START_OF_SPEECH)
            assert (
                await asyncio.wait_for(anext(stream), 2)
            ).type is stt.SpeechEventType.START_OF_SPEECH
            interim = await asyncio.wait_for(anext(stream), 2)
            assert interim.type is stt.SpeechEventType.INTERIM_TRANSCRIPT
            assert interim.alternatives[0].text == "streaming"
            assert observed == [bytes(audio.data)]
            source.emit(vad.VADEventType.END_OF_SPEECH)
            assert (await asyncio.wait_for(anext(stream), 2)).alternatives[
                0
            ].text == "complete"
            assert (
                await asyncio.wait_for(anext(stream), 2)
            ).type is stt.SpeechEventType.END_OF_SPEECH
            stream.end_input()
            assert [event async for event in stream] == []
        finally:
            await stream.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("failure", ["unprocessed-boundary", "stalled-detector-bound", "missing-activation-threshold", "invalid-probability", "invalid-activation-threshold"])
async def test_native_stt_refuses_unprocessed_or_unbounded_vad_input_without_admission(
    failure,
):
    seen = []

    async def handle(request):
        seen.append(request.path)
        return web.Response(status=500)

    async with peer(handle) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_fixture",
            session_id=str(uuid.uuid4()),
            base_url=origin,
        )
        source = ControlledVAD()
        model = STT(
            client,
            provider="fixture",
            model="fixture-model",
            credential_source="managed",
            turn_vad=source,
        )
        stream = model.stream(manual_turns=True)
        try:
            if failure == "unprocessed-boundary":
                audio = frame()
                stream.push_frame(audio)
                await asyncio.wait_for(source.frames.get(), 2)
                source.emit(vad.VADEventType.START_OF_SPEECH, frames=[audio])
            elif failure == "stalled-detector-bound":
                stream.push_frame(
                    rtc.AudioFrame(
                        data=b"\0\0" * (31 * 16000),
                        sample_rate=16000,
                        num_channels=1,
                        samples_per_channel=31 * 16000,
                    )
                )
            else:
                audio = frame()
                stream.push_frame(audio)
                await asyncio.wait_for(source.frames.get(), 2)
                if failure != "missing-activation-threshold":
                    source.activation_threshold = -1 if failure == "invalid-activation-threshold" else 0.5
                source.current._event_ch.send_nowait(vad.VADEvent(
                    type=vad.VADEventType.INFERENCE_DONE,
                    samples_index=320, timestamp=0.02, frames=[audio],
                    speech_duration=0, silence_duration=0,
                    probability=float("nan") if failure == "invalid-probability" else 0.9,
                ))
                source.emit(vad.VADEventType.START_OF_SPEECH, frames=[audio])
            with pytest.raises(APIConnectionError):
                async for _ in stream:
                    pass
            assert seen == []
            with pytest.raises(GatewayError, match="control-plane recovery"):
                model.stream()
        finally:
            await stream.aclose()
            await model.aclose()
            await client.aclose()


async def test_cancelling_native_stt_after_configure_fences_admission_before_ready():
    configured = asyncio.Event()
    seen = []

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        seen.append(request.headers["Idempotency-Key"])
        configured.set()
        async for _ in ws:
            pass
        return ws

    async with peer(handle) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_fixture",
            session_id=str(uuid.uuid4()),
            base_url=origin,
        )
        model = STT(
            client,
            turn_vad=ControlledVAD(automatic=True),
            provider="fixture",
            model="fixture-model",
            credential_source="managed",
        )
        stream = model.stream(manual_turns=True)
        try:
            stream.push_frame(frame())
            await asyncio.wait_for(configured.wait(), 2)
            await asyncio.wait_for(stream.aclose(), 2)
            with pytest.raises(GatewayError, match="control-plane recovery"):
                model.stream()
            assert len(seen) == 1
        finally:
            await stream.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("start_offset", [0, 7.25])
async def test_native_vad_prefix_never_replays_prior_audio_and_times_follow_the_stream(
    start_offset,
):
    source = ControlledVAD()
    seen = []

    def audio(raw):
        return rtc.AudioFrame(
            data=raw,
            sample_rate=16000,
            num_channels=1,
            samples_per_channel=len(raw) // 2,
        )

    first = audio(b"\x11\x22" * 1600)
    silence = audio(b"\0\0" * 640)
    second = audio(b"\x33\x44" * 1600)
    tail = audio(bytes(first.data)[-1920:])

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        index = len(seen)
        seen.append(bytearray())
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.BINARY:
                seen[index].extend(message.data)
            elif json.loads(message.data)["type"] == "session.close":
                await ws.send_json(
                    {
                        "type": "transcript.final",
                        "text": "first" if index == 0 else "second",
                        "audio_start_ms": 0,
                        "audio_end_ms": 100 if index == 0 else 140,
                    }
                )
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with peer(handle) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_fixture",
            session_id=str(uuid.uuid4()),
            base_url=origin,
        )
        model = STT(
            client,
            turn_vad=source,
            provider="fixture",
            model="fixture-model",
            credential_source="managed",
        )
        stream = model.stream(manual_turns=True)
        stream.start_time_offset = start_offset

        async def completed_turn():
            result = None
            async for event in stream:
                if event.type is stt.SpeechEventType.FINAL_TRANSCRIPT:
                    result = event.alternatives[0]
                if event.type is stt.SpeechEventType.END_OF_SPEECH:
                    return result

        try:
            stream.push_frame(first)
            await asyncio.wait_for(source.frames.get(), 2)
            source.processed(first)
            source.emit(vad.VADEventType.START_OF_SPEECH)
            source.emit(vad.VADEventType.END_OF_SPEECH)
            first_result = await asyncio.wait_for(completed_turn(), 2)
            for value in [silence, second]:
                stream.push_frame(value)
                await asyncio.wait_for(source.frames.get(), 2)
                source.processed(value)
            source.emit(
                vad.VADEventType.START_OF_SPEECH, frames=[tail, silence, second]
            )
            source.emit(vad.VADEventType.END_OF_SPEECH)
            second_result = await asyncio.wait_for(completed_turn(), 2)
            stream.end_input()
            assert [event async for event in stream] == []
            assert [bytes(value) for value in seen] == [
                bytes(first.data),
                bytes(silence.data) + bytes(second.data),
            ]
            # LiveKit adds its own sub-millisecond task-start offset.
            assert first_result.start_time == pytest.approx(start_offset, abs=0.001)
            assert first_result.end_time == pytest.approx(start_offset + 0.1, abs=0.001)
            assert second_result.start_time == pytest.approx(
                start_offset + 0.1, abs=0.001
            )
            assert second_result.end_time == pytest.approx(
                start_offset + 0.24, abs=0.001
            )
        finally:
            await stream.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("large", ["prefix", "inference"])
async def test_vad_audio_always_fits_native_message_bounds(large):
    source = ControlledVAD()
    observed = []

    def audio(samples):
        return rtc.AudioFrame(
            data=b"\x11\x22" * samples,
            sample_rate=48000,
            num_channels=1,
            samples_per_channel=samples,
        )

    prefix = audio(23 * 48000 if large == "prefix" else 960)
    inference = audio(23 * 48000 if large == "inference" else 960)

    async def handle(request):
        ws = web.WebSocketResponse(max_msg_size=(2 << 20) + 1)
        await ws.prepare(request)
        await ws.receive_json()
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.BINARY:
                observed.append(message.data)
            elif json.loads(message.data)["type"] == "session.close":
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with peer(handle) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_fixture",
            session_id=str(uuid.uuid4()),
            base_url=origin,
        )
        model = STT(
            client,
            turn_vad=source,
            sample_rate=48000,
            provider="fixture",
            model="fixture-model",
            credential_source="managed",
        )
        stream = model.stream(manual_turns=True)
        try:
            stream.push_frame(prefix)
            await asyncio.wait_for(source.frames.get(), 2)
            source.processed(prefix)
            source.emit(vad.VADEventType.START_OF_SPEECH)
            assert (
                await asyncio.wait_for(anext(stream), 2)
            ).type is stt.SpeechEventType.START_OF_SPEECH
            stream.push_frame(inference)
            await asyncio.wait_for(source.frames.get(), 2)
            source.processed(inference)
            source.emit(vad.VADEventType.END_OF_SPEECH)
            stream.end_input()
            assert [event.type async for event in stream] == [
                stt.SpeechEventType.END_OF_SPEECH
            ]
            assert all(len(value) <= 2 << 20 for value in observed)
            assert b"".join(observed) == bytes(prefix.data) + bytes(inference.data)
        finally:
            await stream.aclose()
            await model.aclose()
            await client.aclose()


async def test_native_eos_uses_speech_end_before_vad_trailing_silence():
    source = ControlledVAD()

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ready(ws)
        async for message in ws:
            if (
                message.type is aiohttp.WSMsgType.TEXT
                and json.loads(message.data)["type"] == "session.close"
            ):
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with peer(handle) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_fixture",
            session_id=str(uuid.uuid4()),
            base_url=origin,
        )
        model = STT(
            client,
            provider="fixture",
            model="fixture-model",
            credential_source="managed",
            turn_vad=source,
        )
        stream = model.stream(manual_turns=True)
        stream.start_time_offset = 4
        try:
            speech = frame()
            stream.push_frame(speech)
            await asyncio.wait_for(source.frames.get(), 2)
            source.processed(speech)
            source.emit(vad.VADEventType.START_OF_SPEECH)
            assert (
                await asyncio.wait_for(anext(stream), 2)
            ).type is stt.SpeechEventType.START_OF_SPEECH
            trailing = rtc.AudioFrame(
                data=b"\0\0" * 8800,
                sample_rate=16000,
                num_channels=1,
                samples_per_channel=8800,
            )
            stream.push_frame(trailing)
            await asyncio.wait_for(source.frames.get(), 2)
            source.processed(trailing)
            source.emit(vad.VADEventType.END_OF_SPEECH, silence_duration=0.55)
            stream.end_input()
            events = [event async for event in stream]
            assert [event.type for event in events] == [
                stt.SpeechEventType.END_OF_SPEECH
            ]
            assert events[0].alternatives[0].end_time == pytest.approx(4.02, abs=0.001)
        finally:
            await stream.aclose()
            await model.aclose()
            await client.aclose()


async def test_native_framework_stream_requires_a_binding_to_a_running_session():
    seen = []

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        seen.append(request.headers["Idempotency-Key"])
        await ready(ws)
        async for message in ws:
            if (
                message.type is aiohttp.WSMsgType.TEXT
                and json.loads(message.data)["type"] == "session.close"
            ):
                await ws.send_json({"type": "transcript.final", "text": "complete"})
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with peer(handle) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_fixture",
            session_id=str(uuid.uuid4()),
            base_url=origin,
        )
        model = STT(
            client,
            provider="fixture",
            model="fixture-model",
            credential_source="managed",
            turn_vad=ControlledVAD(automatic=True),
        )
        with pytest.raises(ValueError, match="bind_agent"):
            model.stream()
        assert seen == []
        agent = Agent(instructions="Owned uncommitted turn fixture")
        original = agent.on_user_turn_completed
        model.bind_agent(agent)
        try:
            with pytest.raises(ValueError, match="running AgentSession"):
                model.stream()
            assert seen == []
            assert agent.on_user_turn_completed == original
        finally:
            await model.aclose()
            assert agent.on_user_turn_completed == original
            await client.aclose()


@pytest.mark.parametrize("transition", ["skip_reply", "handoff"])
@pytest.mark.parametrize("binding", ["agent", "session"])
@pytest.mark.parametrize("wrapper", ["direct", "multi_speaker", "fallback", "nested"])
async def test_actual_native_agent_session_keeps_turn_authority_across_commits_and_handoffs(
    transition, binding, wrapper,
):
    source = ControlledVAD()
    seen = []

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        seen.append(request.headers["Idempotency-Key"])
        text = f"turn {len(seen)}"
        await ready(ws)
        async for message in ws:
            if (
                message.type is aiohttp.WSMsgType.TEXT
                and json.loads(message.data)["type"] == "session.close"
            ):
                await ws.send_json({"type": "transcript.final", "text": text})
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with peer(handle) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_fixture",
            session_id=str(uuid.uuid4()),
            base_url=origin,
        )
        model = STT(
            client,
            provider="fixture",
            model="fixture-model",
            credential_source="managed",
            turn_vad=source,
            diarization=wrapper in ("multi_speaker", "nested"),
        )
        recognition_model = wrap_native_stt(model, wrapper)
        session = AgentSession(
            stt=recognition_model,
            llm=None,
            tts=None,
            vad=None,
            turn_detection="manual" if transition == "skip_reply" else "stt",
            min_endpointing_delay=0.05,
            max_endpointing_delay=0.05,
        )
        audio = OwnedAudioInput()
        session.input.audio = audio
        transcripts, turns, items = asyncio.Queue(), asyncio.Queue(), asyncio.Queue()
        session.on(
            "user_input_transcribed",
            lambda event: transcripts.put_nowait(event.transcript)
            if event.is_final
            else None,
        )
        session.on(
            "conversation_item_added",
            lambda event: items.put_nowait(event.item.text_content)
            if getattr(event.item, "role", None) == "user"
            else None,
        )

        class OwnedAgent(Agent):
            async def on_enter(self):
                entered.set()

            async def on_user_turn_completed(self, turn_ctx, new_message):
                turns.put_nowait(new_message.text_content)

        async def utterance():
            for value in (
                frame(),
                rtc.AudioFrame(
                    data=b"\0\0" * 8800,
                    sample_rate=16000,
                    num_channels=1,
                    samples_per_channel=8800,
                ),
            ):
                await audio.frames.put(value)
                observed = await asyncio.wait_for(source.frames.get(), 3)
                source.processed(observed)
                source.emit(
                    vad.VADEventType.START_OF_SPEECH
                    if value.samples_per_channel == 320
                    else vad.VADEventType.END_OF_SPEECH,
                    frames=[observed],
                    silence_duration=0 if value.samples_per_channel == 320 else 0.55,
                )

        entered = asyncio.Event()
        agent = OwnedAgent(instructions="Owned native lifecycle fixture")
        original = agent.on_user_turn_completed
        original_node, original_update = agent.stt_node, session._update_activity
        if binding == "session":
            model.bind_session(session)
        else:
            model.bind_agent(agent)
        try:
            options = (
                {"record": False}
                if "record" in inspect.signature(session.start).parameters
                else {}
            )
            await session.start(agent=agent, **options)
            await utterance()
            assert await asyncio.wait_for(transcripts.get(), 3) == "turn 1"
            if transition == "skip_reply":
                # Queue the next utterance before the first framework commit.
                await utterance()
                await asyncio.wait_for(
                    session.commit_user_turn(skip_reply=True, stt_flush_duration=0), 3
                )
                assert await asyncio.wait_for(items.get(), 3) == "turn 1"
                assert turns.empty(), "skip_reply must preserve the SDK's hook behavior"
            else:
                assert await asyncio.wait_for(turns.get(), 3) == "turn 1"
                entered.clear()
                session.update_agent(OwnedAgent(instructions="Owned handoff target"))
                await asyncio.wait_for(entered.wait(), 3)
                await utterance()
            assert await asyncio.wait_for(transcripts.get(), 3) == "turn 2"
            if transition == "skip_reply":
                await asyncio.wait_for(
                    session.commit_user_turn(stt_flush_duration=0), 3
                )
            assert await asyncio.wait_for(turns.get(), 3) == "turn 2"
            if transition == "handoff":
                # Receiving the first transcript under the new Agent does not
                # prove its commit released a retained stream for more input.
                await utterance()
                assert await asyncio.wait_for(transcripts.get(), 3) == "turn 3"
                assert await asyncio.wait_for(turns.get(), 3) == "turn 3"
            expected = 3 if transition == "handoff" else 2
            assert len(seen) == len(set(seen)) == expected
            assert agent.on_user_turn_completed == original
            assert not model._native_failed
        finally:
            await audio.frames.put(None)
            await session.aclose()
            if recognition_model is not model:
                await recognition_model.aclose()
            await model.aclose()
            await client.aclose()
            assert agent.stt_node == original_node
            assert session._update_activity == original_update


@pytest.mark.skipif(
    not hasattr(recognition.AudioRecognition, "_detach_stt"),
    reason="SDK recreates STT instead of retaining its pipeline",
)
@pytest.mark.parametrize("binding", ["agent", "session"])
async def test_native_final_during_detached_handoff_binds_the_consuming_activity(binding):
    source = ControlledVAD()
    closing, terminal, detached, attach = (asyncio.Event() for _ in range(4))
    seen, turns = [], asyncio.Queue()

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        seen.append(request.headers["Idempotency-Key"])
        index = len(seen)
        await ready(ws)
        async for message in ws:
            if (
                message.type is aiohttp.WSMsgType.TEXT
                and json.loads(message.data)["type"] == "session.close"
            ):
                if index == 1:
                    closing.set()
                    await terminal.wait()
                await ws.send_json({"type": "transcript.final", "text": f"turn {index}"})
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with peer(handle) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin,
        )
        model = STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=source)
        session = AgentSession(stt=model, llm=None, tts=None, vad=None, turn_detection="stt", min_endpointing_delay=0.05, max_endpointing_delay=0.05)
        audio = OwnedAudioInput()
        session.input.audio = audio

        class OwnedAgent(Agent):
            async def on_user_turn_completed(self, turn_ctx, new_message):
                turns.put_nowait(new_message.text_content)

        async def utterance():
            for value in (frame(), rtc.AudioFrame(data=b"\0\0" * 8800, sample_rate=16000, num_channels=1, samples_per_channel=8800)):
                await audio.frames.put(value)
                observed = await asyncio.wait_for(source.frames.get(), 3)
                source.processed(observed)
                source.emit(vad.VADEventType.START_OF_SPEECH if value.samples_per_channel == 320 else vad.VADEventType.END_OF_SPEECH, frames=[observed], silence_duration=0 if value.samples_per_channel == 320 else 0.55)

        agent = OwnedAgent(instructions="Owned detached handoff fixture")
        if binding == "session":
            model.bind_session(session)
        else:
            model.bind_agent(agent)
        try:
            options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
            await session.start(agent=agent, **options)
            await utterance()
            await asyncio.wait_for(closing.wait(), 3)
            old = session._activity
            recognition_instance = old._audio_recognition
            original = recognition_instance._detach_stt

            async def pause_after_detach():
                pipeline = await original()
                detached.set()
                await attach.wait()
                return pipeline

            recognition_instance._detach_stt = pause_after_detach
            session.update_agent(OwnedAgent(instructions="Owned next activity"))
            await asyncio.wait_for(detached.wait(), 3)
            assert session._activity is old
            terminal.set()
            # Let the real retained pump queue the final while no SDK consumer
            # exists. The old activity is deliberately still visible here.
            async def wait_for_queued_final():
                while not any(getattr(stream, "_turn_commit", None) is not None for stream in model._streams):
                    await asyncio.sleep(0)

            await asyncio.wait_for(wait_for_queued_final(), 3)
            assert session._activity is old
            attach.set()
            assert await asyncio.wait_for(turns.get(), 3) == "turn 1"
            await utterance()
            assert await asyncio.wait_for(turns.get(), 3) == "turn 2"
            assert len(seen) == len(set(seen)) == 2
            assert not model._native_failed
        finally:
            terminal.set()
            attach.set()
            await audio.frames.put(None)
            await session.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("wrapper", ["direct", "nested"])
@pytest.mark.parametrize("change", ["handoff", pytest.param("update_options", marks=pytest.mark.skipif(not hasattr(Agent, "update_options"), reason="SDK predates live model option updates"))])
async def test_native_model_handoffs_keep_only_the_active_recognition_owner_bound(wrapper, change):
    sources = [ControlledVAD(), ControlledVAD()]
    seen, turns = [], asyncio.Queue()

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        seen.append(request.headers["Idempotency-Key"])
        index, interims_sent = len(seen), False
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.BINARY and not interims_sent:
                interims_sent = True
                if index == 2:
                    # Exercise actual pump -> recognition -> session events,
                    # rather than directly calling either model's callback.
                    for _ in range(600):
                        await ws.send_json({"type": "transcript.delta", "text": "owned interim"})
            elif (
                message.type is aiohttp.WSMsgType.TEXT
                and json.loads(message.data)["type"] == "session.close"
            ):
                await ws.send_json({"type": "transcript.final", "text": f"turn {index}"})
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with peer(handle) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin)
        models = [STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=source, diarization=wrapper == "nested") for source in sources]
        recognition_models = [wrap_native_stt(model, wrapper) for model in models]
        session = AgentSession(llm=None, tts=None, vad=None, turn_detection="stt", min_endpointing_delay=0.05, max_endpointing_delay=0.05)
        audio = OwnedAudioInput()
        session.input.audio = audio
        entered = asyncio.Queue()

        class OwnedAgent(Agent):
            async def on_enter(self):
                entered.put_nowait(None)

            async def on_user_turn_completed(self, turn_ctx, new_message):
                turns.put_nowait(new_message.text_content)

        for model in models:
            model.bind_session(session)
        try:
            options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
            await session.start(agent=OwnedAgent(instructions="Owned first model", stt=recognition_models[0]), **options)
            await asyncio.wait_for(entered.get(), 3)
            owners = [0, 1, 0] if change == "handoff" else [0, 1, 0, 1, 0]
            original_activity = session._activity
            for index, owner in enumerate(owners, start=1):
                if index > 1:
                    if change == "handoff":
                        session.update_agent(OwnedAgent(instructions="Owned model handoff", stt=recognition_models[owner]))
                        await asyncio.wait_for(entered.get(), 3)
                    else:
                        session.current_agent.update_options(stt=recognition_models[owner])
                        assert session._activity is original_activity
                source = sources[owner]
                for value in (frame(), rtc.AudioFrame(data=b"\0\0" * 8800, sample_rate=16000, num_channels=1, samples_per_channel=8800)):
                    await audio.frames.put(value)
                    observed = await asyncio.wait_for(source.frames.get(), 3)
                    source.processed(observed)
                    source.emit(vad.VADEventType.START_OF_SPEECH if value.samples_per_channel == 320 else vad.VADEventType.END_OF_SPEECH, frames=[observed], silence_duration=0 if value.samples_per_channel == 320 else 0.55)
                assert await asyncio.wait_for(turns.get(), 3) == f"turn {index}"
                assert models[1 - owner]._turn_binding is None
                callback, depth = session._activity.on_end_of_turn, 0
                while getattr(callback, "__name__", None) == "committed":
                    depth += 1
                    assert depth == 1, "live swaps must not retain callbacks owned by previous models"
                    callback = inspect.getclosurevars(callback).nonlocals["original"]
                assert depth == 1
            assert len(seen) == len(set(seen)) == len(owners)
            assert all(not model._native_failed for model in models)
        finally:
            await audio.frames.put(None)
            await session.aclose()
            for recognition_model, model in zip(recognition_models, models):
                if recognition_model is not model:
                    await recognition_model.aclose()
            for model in models:
                await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("wrapper", ["direct", "multi_speaker", "fallback", "nested"])
@pytest.mark.parametrize("attribution", ["whole_final", "segments", "louder", "inherited", "unattributed"])
@pytest.mark.parametrize("prefix_frames", [0, 35])
async def test_native_actual_agent_preserves_provider_speakers_through_supported_wrappers(monkeypatch, wrapper, attribution, prefix_frames):
    source, turns, finals = ControlledVAD(), asyncio.Queue(), []
    speaker_streams = []
    original_stream = stt.MultiSpeakerAdapter.stream

    def capture_stream(adapter, *args, **kwargs):
        stream = original_stream(adapter, *args, **kwargs)
        speaker_streams.append((stream, stream._detector.on_stt_event, stream.aclose))
        return stream

    monkeypatch.setattr(stt.MultiSpeakerAdapter, "stream", capture_stream)
    payload = {"type": "transcript.final", "text": "one two", "speaker": "A"}
    expected = [("one two", "A", 0.0, 0.0)]
    if attribution != "whole_final":
        payload["segments"] = [
            {"text": "one", "start_ms": 0, "end_ms": 300, "speaker": "A"},
            {"text": "two", "start_ms": 300, "end_ms": 600},
        ]
        if attribution in ("segments", "louder"):
            payload["segments"][1]["speaker"] = "B"
        if attribution == "unattributed":
            del payload["speaker"]
        second_speaker = "B" if attribution in ("segments", "louder") else None if attribution == "unattributed" else "A"
        expected = [("one", "A", 0.0, 0.3), ("two", second_speaker, 0.3, 0.6)]

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.TEXT and json.loads(message.data)["type"] == "session.close":
                await ws.send_json(payload)
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with peer(handle) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin)
        model = STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=source, diarization=True)
        recognition_model = wrap_native_stt(model, wrapper)
        session = AgentSession(llm=None, tts=None, vad=None, turn_detection="stt", min_endpointing_delay=0, max_endpointing_delay=0)
        audio = OwnedAudioInput(track_delivery=True)
        session.input.audio = audio

        class OwnedAgent(Agent):
            async def stt_node(self, audio, settings):
                for _ in range(prefix_frames):
                    await anext(audio)
                if prefix_frames:
                    async def dsp_frames(source):
                        async for value in source:
                            replacement = rtc.AudioFrame(data=bytearray(value.data), sample_rate=value.sample_rate, num_channels=value.num_channels, samples_per_channel=value.samples_per_channel)
                            replacement.userdata.update(copy.deepcopy(value.userdata))
                            yield replacement
                    audio = dsp_frames(audio)
                async for event in Agent.default.stt_node(self, audio, settings):
                    if event.type is stt.SpeechEventType.FINAL_TRANSCRIPT:
                        finals.append(event.alternatives[0])
                    yield event

            async def on_user_turn_completed(self, turn_ctx, new_message):
                turns.put_nowait(new_message.text_content)

        model.bind_session(session)
        try:
            options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
            await session.start(agent=OwnedAgent(instructions="Owned diarization fixture", stt=recognition_model), **options)
            for _ in range(prefix_frames):
                await audio.frames.put(frame())
            for samples, kind in [(9600, vad.VADEventType.START_OF_SPEECH), (8800, vad.VADEventType.END_OF_SPEECH)]:
                pcm = (b"\xff\x0f" if samples == 9600 else b"\0\0") * samples
                if attribution == "louder" and samples == 9600:
                    pcm = b"\xff\x0f" * 4800 + b"\xff\x3f" * 4800
                value = rtc.AudioFrame(data=pcm, sample_rate=16000, num_channels=1, samples_per_channel=samples)
                await audio.frames.put(value)
                observed = await asyncio.wait_for(source.frames.get(), 3)
                source.processed(observed)
                source.emit(kind, frames=[observed], silence_duration=0 if samples == 9600 else 0.55)
            assert await asyncio.wait_for(turns.get(), 3) == "one two"
            if attribution != "whole_final":
                expected = [(text, speaker, start + prefix_frames * 0.02, end + prefix_frames * 0.02) for text, speaker, start, end in expected]
            assert [(item.text, item.provider_speaker_id) for item in finals] == [(text, speaker) for text, speaker, _, _ in expected]
            assert_request_speakers(finals)
            if attribution == "whole_final":
                assert [(item.start_time, item.end_time) for item in finals] == [(0.0, 0.0)]
            else:
                await assert_source_times(session, audio, [item.start_time for item in finals], [start for _, _, start, _ in expected])
                await assert_source_times(session, audio, [item.end_time for item in finals], [end for _, _, _, end in expected])
            if wrapper in ("multi_speaker", "nested") and attribution != "whole_final":
                assert [item.is_primary_speaker for item in finals] == [True, None if attribution == "unattributed" else attribution in ("inherited", "louder")]
        finally:
            await audio.frames.put(None)
            await session.aclose()
            if recognition_model is not model:
                await recognition_model.aclose()
            await model.aclose()
            await client.aclose()
            for stream, on_event, close in speaker_streams:
                assert stream._detector.on_stt_event == on_event
                assert stream.aclose == close


async def test_overlapping_sdk_activity_updates_keep_the_queued_targets_input_binding():
    source, turns = ControlledVAD(), asyncio.Queue()

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.TEXT and json.loads(message.data)["type"] == "session.close":
                await ws.send_json({"type": "transcript.final", "text": "queued target"})
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    class OwnedAgent(Agent):
        async def on_user_turn_completed(self, turn_ctx, new_message):
            turns.put_nowait(new_message.text_content)

    async with peer(handle) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin)
        model = STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=source)
        session = AgentSession(stt=model, llm=None, tts=None, vad=None, turn_detection="stt", min_endpointing_delay=0, max_endpointing_delay=0)
        audio, tasks, locked = OwnedAudioInput(), [], False
        session.input.audio = audio
        model.bind_session(session)
        targets = [OwnedAgent(instructions="Owned queued target") for _ in range(2)]
        try:
            options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
            await session.start(agent=OwnedAgent(instructions="Owned initial activity"), **options)
            await session._activity_lock.acquire()
            locked = True
            # AgentTask uses this same private SDK entrypoint. Both wrappers
            # attach before either original update can acquire the SDK lock.
            for target in targets:
                tasks.append(asyncio.create_task(session._update_activity(target, wait_on_enter=False)))
            await asyncio.sleep(0)
            binding = model._native_input_binding
            assert all(target in binding._agents for target in targets)
            session._activity_lock.release()
            locked = False
            await asyncio.wait_for(asyncio.gather(*tasks), 10)
            assert targets[1] in binding._agents, "the waiting target must survive the first update's cleanup"
            for samples, kind in [(320, vad.VADEventType.START_OF_SPEECH), (8800, vad.VADEventType.END_OF_SPEECH)]:
                value = rtc.AudioFrame(data=(b"\xff\x0f" if samples == 320 else b"\0\0") * samples, sample_rate=16000, num_channels=1, samples_per_channel=samples)
                await audio.frames.put(value)
                observed = await asyncio.wait_for(source.frames.get(), 3)
                source.processed(observed)
                source.emit(kind, frames=[observed], silence_duration=0 if samples == 320 else 0.55)
            assert await asyncio.wait_for(turns.get(), 3) == "queued target"
            assert not model._native_failed
        finally:
            if locked:
                session._activity_lock.release()
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await audio.frames.put(None)
            await session.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("binding", ["agent", "session"])
async def test_detached_agent_can_run_in_second_session_without_disrupting_retained_native_pipeline(binding):
    entered, turns, seen = asyncio.Queue(), asyncio.Queue(), []

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        index = len(seen) + 1
        seen.append(request.headers["Idempotency-Key"])
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.TEXT and json.loads(message.data)["type"] == "session.close":
                await ws.send_json({"type": "transcript.final", "text": f"turn {index}"})
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    class OwnedAgent(Agent):
        async def on_enter(self):
            entered.put_nowait(self)

        async def on_user_turn_completed(self, turn_ctx, new_message):
            turns.put_nowait(new_message.text_content)

    async with peer(handle) as origin:
        clients = [RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin) for _ in range(2)]
        sources = [ControlledVAD(), ControlledVAD()]
        models = [STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=source) for client, source in zip(clients, sources)]
        sessions = [AgentSession(stt=model, llm=None, tts=None, vad=None, turn_detection="stt", min_endpointing_delay=0, max_endpointing_delay=0) for model in models]
        inputs = [OwnedAudioInput(), OwnedAudioInput()]
        for index, (session, model, audio) in enumerate(zip(sessions, models, inputs)):
            session.input.audio = audio
            if index == 0 or binding == "session":
                model.bind_session(session)

        async def utterance(index):
            for samples, kind in [(320, vad.VADEventType.START_OF_SPEECH), (8800, vad.VADEventType.END_OF_SPEECH)]:
                value = rtc.AudioFrame(data=(b"\xff\x0f" if samples == 320 else b"\0\0") * samples, sample_rate=16000, num_channels=1, samples_per_channel=samples)
                await inputs[index].frames.put(value)
                observed = await asyncio.wait_for(sources[index].frames.get(), 3)
                sources[index].processed(observed)
                sources[index].emit(kind, frames=[observed], silence_duration=0 if samples == 320 else 0.55)

        first, next_agent = OwnedAgent(instructions="Owned reusable Agent"), OwnedAgent(instructions="Owned handoff target")
        original_node = first.stt_node
        options = {"record": False} if "record" in inspect.signature(sessions[0].start).parameters else {}
        try:
            await sessions[0].start(agent=first, **options)
            assert await asyncio.wait_for(entered.get(), 3) is first
            await utterance(0)
            assert await asyncio.wait_for(turns.get(), 3) == "turn 1"
            old_pipeline = getattr(sessions[0]._activity._audio_recognition, "_stt_pipeline", None)
            sessions[0].update_agent(next_agent)
            assert await asyncio.wait_for(entered.get(), 3) is next_agent
            assert first._activity is None
            if binding == "agent":
                models[1].bind_agent(first)
            await sessions[1].start(agent=first, **options)
            assert await asyncio.wait_for(entered.get(), 3) is first
            if hasattr(recognition.AudioRecognition, "_detach_stt"):
                assert sessions[0]._activity._audio_recognition._stt_pipeline is old_pipeline
            await utterance(0)
            assert await asyncio.wait_for(turns.get(), 3) == "turn 2"
            await utterance(1)
            assert await asyncio.wait_for(turns.get(), 3) == "turn 3"
            assert len(seen) == len(set(seen)) == 3
        finally:
            for audio in inputs:
                await audio.frames.put(None)
            for session in reversed(sessions):
                await session.aclose()
            for model in models:
                await model.aclose()
            for client in clients:
                await client.aclose()
            assert first.stt_node == original_node


@pytest.mark.parametrize("binding", ["session", "agent"])
async def test_native_source_observer_releases_its_owned_hooks_after_local_handoff(binding):
    class LocalSTT(stt.STT):
        def __init__(self):
            super().__init__(capabilities=stt.STTCapabilities(streaming=True, interim_results=False))
            self.started, self.frames = asyncio.Queue(), asyncio.Queue()

        async def _recognize_impl(self, *args, **kwargs):
            raise NotImplementedError

        def stream(self, *, language=None, conn_options=DEFAULT_API_CONNECT_OPTIONS):
            return LocalStream(stt=self, conn_options=conn_options)

    class LocalStream(stt.RecognizeStream):
        async def _run(self):
            self._stt.started.put_nowait(self)
            async for value in self._input_ch:
                if isinstance(value, rtc.AudioFrame):
                    self._stt.frames.put_nowait(value)

    source = ControlledVAD()
    client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url="http://127.0.0.1:1")
    model, local = STT(client, turn_vad=source), LocalSTT()
    session = AgentSession(stt=model, llm=None, tts=None, vad=None)
    audio = OwnedAudioInput()
    session.input.audio = audio
    agent = Agent(instructions="Owned native input lifecycle")
    original_node, original_forward = agent.stt_node, session._forward_audio_task
    original_enable = session.input.set_audio_enabled
    if binding == "session":
        model.bind_session(session)
    else:
        model.bind_agent(agent)
    try:
        options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
        await session.start(agent=agent, **options)
        await audio.frames.put(frame())
        await asyncio.wait_for(source.frames.get(), 3)
        captured_source = session.input.audio
        assert captured_source is audio
        session.update_agent(Agent(instructions="Owned local input lifecycle", stt=local))
        await asyncio.wait_for(local.started.get(), 3)
        owned_binding = model._native_input_binding
        await model.aclose()
        assert session.input.audio is audio
        assert session._forward_audio_task == original_forward
        assert agent.stt_node == original_node

        value = frame()
        value.userdata["producer"] = "untouched"
        await audio.frames.put(value)
        observed = await asyncio.wait_for(local.frames.get(), 3)
        assert bytes(observed.data) == bytes(value.data)
        assert observed.userdata == {"producer": "untouched"}
        assert session.input.audio is audio
        assert session._forward_audio_task == original_forward
        assert not owned_binding.owners
        assert model._native_input_binding is None
        assert session.input.set_audio_enabled == original_enable
    finally:
        await audio.frames.put(None)
        await session.aclose()
        await model.aclose()
        await local.aclose()
        await client.aclose()


@pytest.mark.parametrize("idle_gap", [0, 0.15])
async def test_later_native_owner_rebinds_the_running_source_after_local_handoff(idle_gap):
    class LocalSTT(stt.STT):
        def __init__(self):
            super().__init__(capabilities=stt.STTCapabilities(streaming=True, interim_results=False))
            self.started = asyncio.Event()

        async def _recognize_impl(self, *args, **kwargs):
            raise NotImplementedError

        def stream(self, *, language=None, conn_options=DEFAULT_API_CONNECT_OPTIONS):
            return LocalStream(stt=self, conn_options=conn_options)

    class LocalStream(stt.RecognizeStream):
        async def _run(self):
            self._stt.started.set()
            async for _ in self._input_ch:
                pass

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.TEXT and json.loads(message.data)["type"] == "session.close":
                await ws.send_json({"type": "transcript.final", "text": "rebound", "segments": [{"text": "rebound", "start_ms": 0, "end_ms": 20}]})
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    entered, turns, finals = asyncio.Event(), asyncio.Queue(), []

    class NextAgent(Agent):
        async def stt_node(self, audio, settings):
            async for event in Agent.default.stt_node(self, audio, settings):
                if event.type is stt.SpeechEventType.FINAL_TRANSCRIPT:
                    finals.extend(event.alternatives)
                yield event

        async def on_enter(self):
            entered.set()

        async def on_user_turn_completed(self, turn_ctx, new_message):
            turns.put_nowait(new_message.text_content)

    async with peer(handle) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin)
        sources, local = [ControlledVAD(), ControlledVAD()], LocalSTT()
        models = [STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=source) for source in sources]
        session = AgentSession(stt=models[0], llm=None, tts=None, vad=None, turn_detection="stt", min_endpointing_delay=0, max_endpointing_delay=0)
        audio = OwnedAudioInput()
        session.input.audio = audio
        models[0].bind_session(session)
        try:
            options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
            await session.start(agent=Agent(instructions="Owned first native source"), **options)
            await audio.frames.put(frame())
            await asyncio.wait_for(sources[0].frames.get(), 3)
            session.update_agent(Agent(instructions="Owned temporary local source", stt=local))
            await asyncio.wait_for(local.started.wait(), 3)
            await models[0].aclose()
            assert session.input.audio is audio
            await asyncio.sleep(idle_gap)
            target = NextAgent(instructions="Owned rebound native source", stt=models[1])
            models[1].bind_agent(target)
            session.update_agent(target)
            await asyncio.wait_for(entered.wait(), 3)
            bounds = None
            for samples, kind in [(320, vad.VADEventType.START_OF_SPEECH), (8800, vad.VADEventType.END_OF_SPEECH)]:
                value = rtc.AudioFrame(data=(b"\xff\x0f" if samples == 320 else b"\0\0") * samples, sample_rate=16000, num_channels=1, samples_per_channel=samples)
                before = time.time()
                await audio.frames.put(value)
                observed = await asyncio.wait_for(sources[1].frames.get(), 3)
                if samples == 320:
                    bounds = before, time.time()
                sources[1].processed(observed)
                sources[1].emit(kind, frames=[observed], silence_duration=0 if samples == 320 else 0.55)
            assert await asyncio.wait_for(turns.get(), 3) == "rebound"
            assert not models[1]._native_failed
            if not hasattr(session._activity._audio_recognition, "_input_started_at"):
                epoch = input_epoch(session)
                # The fixture can deliver its first 20 ms faster than playout;
                # that captured media must still precede the resumed frame.
                assert bounds[0] - epoch - 1 / 16000 <= finals[0].start_time <= bounds[1] - epoch + 0.02 + 1 / 16000
            else:
                assert finals[0].start_time == pytest.approx(0, abs=1 / 16000)
        finally:
            await audio.frames.put(None)
            await session.aclose()
            for model in models:
                await model.aclose()
            await local.aclose()
            await client.aclose()


@pytest.mark.parametrize("flush_duration", [0.2, None])
@pytest.mark.parametrize("held_frame", [False, True])
async def test_disabled_audio_manual_commit_accepts_sdk_generated_flush_frames(flush_duration, held_frame):
    class SilenceVAD(ControlledVAD):
        def stream(self):
            self.current = SilenceStream(self)
            return self.current

    class SilenceStream(ControlledVADStream):
        async def _main_task(self):
            speaking = False
            async for value in self._input_ch:
                if isinstance(value, self._FlushSentinel):
                    continue
                self._vad.frames.put_nowait(value)
                self._vad.processed(value)
                if any(value.data.cast("B")) and not speaking:
                    speaking = True
                    self._vad.emit(vad.VADEventType.START_OF_SPEECH, frames=[value])
                elif not any(value.data.cast("B")) and speaking:
                    speaking = False
                    self._vad.emit(vad.VADEventType.END_OF_SPEECH, frames=[value], silence_duration=value.duration)

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.TEXT and json.loads(message.data)["type"] == "session.close":
                await ws.send_json({"type": "transcript.final", "text": "flushed", "segments": [{"text": "flushed", "start_ms": 0, "end_ms": 20}]})
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    items, finals = asyncio.Queue(), []

    class OwnedAgent(Agent):
        async def stt_node(self, audio, settings):
            async for event in Agent.default.stt_node(self, audio, settings):
                if event.type is stt.SpeechEventType.FINAL_TRANSCRIPT:
                    finals.extend(event.alternatives)
                yield event

    class HeldInput(io.AudioInput):
        def __init__(self, source):
            super().__init__(label="owned-held-source", source=source)
            self.held, self.release = asyncio.Event(), asyncio.Event()

        async def __anext__(self):
            value = await super().__anext__()
            if not self.held.is_set():
                self.held.set()
                await self.release.wait()
            return value

    async with peer(handle) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin)
        source = SilenceVAD()
        model = STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=source)
        session = AgentSession(stt=model, llm=None, tts=None, vad=None, turn_detection="manual", min_endpointing_delay=0, max_endpointing_delay=0)
        audio = OwnedAudioInput()
        session.input.audio = audio
        session.on("conversation_item_added", lambda event: items.put_nowait(event.item.text_content) if event.item.role == "user" else None)
        model.bind_session(session)
        held = None
        try:
            options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
            await session.start(agent=OwnedAgent(instructions="Owned disabled-input flush"), **options)
            await audio.frames.put(frame())
            await asyncio.wait_for(source.frames.get(), 3)
            if held_frame:
                held = HeldInput(session.input.audio)
                session.input.audio = held
                await audio.frames.put(frame())
                await asyncio.wait_for(held.held.wait(), 3)
            for index in range(3):
                if index:
                    # Resume at a real capture cadence, rather than queueing
                    # several periods of PCM instantaneously in this fixture.
                    await asyncio.sleep(0.03)
                    session.input.set_audio_enabled(True)
                    if index == 1 and held is not None:
                        held.release.set()
                    else:
                        await audio.frames.put(frame())
                    await asyncio.wait_for(source.frames.get(), 3)
                delivered_at = time.time()
                session.input.set_audio_enabled(False)
                flush = {} if flush_duration is None else {"stt_flush_duration": flush_duration}
                await asyncio.wait_for(session.commit_user_turn(transcript_timeout=1, skip_reply=True, **flush), 3)
                assert await asyncio.wait_for(items.get(), 3) == "flushed"
                assert not model._native_failed
                while not source.frames.empty():
                    source.frames.get_nowait()
                if not hasattr(session._activity._audio_recognition, "_input_started_at"):
                    # Injected silence is recognition input, never captured
                    # media two seconds ahead of the session/recording clock.
                    assert finals[-1].end_time <= delivered_at - input_epoch(session) + 0.04
                if index:
                    assert finals[-1].start_time >= finals[-2].end_time - 1 / 16000
            assert len(finals) == 3
        finally:
            if held is not None:
                held.release.set()
            await audio.frames.put(None)
            await session.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("delegated", [False, True])
async def test_observing_input_preserves_public_identity_and_same_input_assignment(delegated):
    source, turns, identity = ControlledVAD(), asyncio.Queue(), []

    class Input(OwnedAudioInput):
        __hash__ = None  # AudioInput's public contract does not require hashing.

        def __init__(self):
            super().__init__()
            self.attached = self.detached = 0
            self.iterations = 0

        def __aiter__(self):
            self.iterations += 1
            return super().__aiter__()

        def on_attached(self):
            self.attached += 1

        def on_detached(self):
            self.detached += 1
            self.frames.put_nowait(None)

        async def __anext__(self):
            value = await super().__anext__()
            identity.append(session.input.audio is public_input)
            return value

    class OwnedAgent(Agent):
        async def on_user_turn_completed(self, turn_ctx, new_message):
            turns.put_nowait(new_message.text_content)

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.TEXT and json.loads(message.data)["type"] == "session.close":
                await ws.send_json({"type": "transcript.final", "text": "same input"})
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with peer(handle) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin)
        model = STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=source)
        session = AgentSession(stt=model, llm=None, tts=None, vad=None, turn_detection="stt", min_endpointing_delay=0, max_endpointing_delay=0)
        audio = Input()
        public_input = io.AudioInput(label="owned-delegated-input", source=audio) if delegated else audio
        session.input.audio = public_input
        model.bind_session(session)
        try:
            options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
            await session.start(agent=OwnedAgent(instructions="Owned input identity"), **options)
            for samples, kind in [(320, vad.VADEventType.START_OF_SPEECH), (8800, vad.VADEventType.END_OF_SPEECH)]:
                await audio.frames.put(rtc.AudioFrame(data=b"\xff\x0f" * samples, sample_rate=16000, num_channels=1, samples_per_channel=samples))
                observed = await asyncio.wait_for(source.frames.get(), 3)
                source.processed(observed)
                source.emit(kind, frames=[observed], silence_duration=0 if samples == 320 else 0.55)
                counts = audio.attached, audio.detached
                assert audio.iterations == (0 if delegated else 1)
                assert session.input.audio is public_input
                session.input.audio = public_input
                await asyncio.sleep(0)
                assert (audio.attached, audio.detached) == counts
            assert await asyncio.wait_for(turns.get(), 3) == "same input"
            assert identity and all(identity)
        finally:
            await audio.frames.put(None)
            await session.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("change", ["reattach", "restart", "held_delegate"])
@pytest.mark.parametrize("binding", ["session", "agent"])
async def test_legacy_source_clock_tracks_capture_and_session_epochs(change, binding):
    if hasattr(recognition.AudioRecognition, "_input_started_at"):
        pytest.skip("modern SDK owns its input clock")
    observed = asyncio.Queue()

    class OwnedAgent(Agent):
        async def stt_node(self, audio, settings):
            async def capture():
                async for value in audio:
                    witness = value.userdata["_speko_native_input_position"]
                    observed.put_nowait((float(witness.start), bytes(value.data)))
                    yield value
            async for event in Agent.default.stt_node(self, capture(), settings):
                yield event

    class HeldInput(io.AudioInput):
        def __init__(self, source):
            super().__init__(label="owned-capture-hold", source=source)
            self.captured, self.release = asyncio.Event(), asyncio.Event()

        async def __anext__(self):
            value = await super().__anext__()
            self.captured.set()
            await self.release.wait()
            return value

    client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url="http://127.0.0.1:1")
    source = ControlledVAD()
    model = STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=source)
    session = AgentSession(stt=model, llm=None, tts=None, vad=None, turn_detection="stt", min_endpointing_delay=0, max_endpointing_delay=0)
    audio = OwnedAudioInput()
    session.input.audio = audio
    original_changed, original_next = session.input._audio_changed, audio.__anext__
    agent = OwnedAgent(instructions="Owned input clock lifecycle")
    if binding == "session":
        model.bind_session(session)
    else:
        model.bind_agent(agent)
    held = None
    try:
        options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
        await session.start(agent=agent, **options)
        samples = 16000 if change == "restart" else 320
        await audio.frames.put(rtc.AudioFrame(data=b"\0\0" * samples, sample_rate=16000, num_channels=1, samples_per_channel=samples))
        await asyncio.wait_for(observed.get(), 3)
        value = await asyncio.wait_for(source.frames.get(), 3)
        source.processed(value)
        if change == "reattach":
            session.input.audio = None
            await asyncio.sleep(0.2)
            session.input.audio = audio
        elif change == "restart":
            await session.aclose()
            while not observed.empty():
                observed.get_nowait()  # closing the old recognition injects flush PCM
            session.input.audio = audio
            await session.start(agent=OwnedAgent(instructions="Owned restarted input clock"), **options)
        else:
            held = HeldInput(audio)
            session.input.audio = held
            await audio.frames.put(frame())
            await asyncio.wait_for(held.captured.wait(), 3)
            captured_at = time.time() - input_epoch(session)
            session.input.set_audio_enabled(False)
            await asyncio.sleep(0.2)
            session.input.set_audio_enabled(True)
            held.release.set()
        lower = time.time() - input_epoch(session)
        if held is None:
            await audio.frames.put(frame())
        start, pcm = await asyncio.wait_for(observed.get(), 3)
        upper = time.time() - input_epoch(session)
        if held is not None:
            assert captured_at - 0.04 <= start <= captured_at + 0.04
            assert session.input.audio is held and held.source is audio
        else:
            assert lower - 1 / 16000 <= start <= upper + 1 / 16000
            assert session.input.audio is audio
        assert pcm == bytes(frame().data)
        assert not model._native_failed
    finally:
        if held is not None:
            held.release.set()
        await audio.frames.put(None)
        await session.aclose()
        await model.aclose()
        await client.aclose()
    assert session.input._audio_changed == original_changed
    assert audio.__anext__ == original_next


async def test_legacy_capture_uses_the_recorders_first_frame_epoch(tmp_path):
    if hasattr(recognition.AudioRecognition, "_input_started_at"):
        pytest.skip("modern SDK owns its input clock")
    from livekit.agents.voice.recorder_io import RecorderIO

    observed = asyncio.Queue()

    class OwnedAgent(Agent):
        async def stt_node(self, audio, settings):
            async def capture():
                async for value in audio:
                    observed.put_nowait(value.userdata["_speko_native_input_position"])
                    yield value
            async for event in Agent.default.stt_node(self, capture(), settings):
                yield event

    source = ControlledVAD()
    client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url="http://127.0.0.1:1")
    model = STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=source)
    session = AgentSession(stt=model, llm=None, tts=None, vad=None)
    audio = OwnedAudioInput()
    session.input.audio = audio
    model.bind_session(session)
    recorder = RecorderIO(agent_session=session)
    recorded = recorder.record_input(audio)
    recorder.record_output(None)
    original_next = audio.__anext__
    try:
        await session.start(agent=OwnedAgent(instructions="Owned recorder epoch"), record=False)
        await asyncio.sleep(0.1)
        await recorder.start(output_path=tmp_path / "owned-input.ogg")
        session._recorder_io = recorder
        session.input.audio = recorded
        session.update_agent(OwnedAgent(instructions="Owned recorded input"))
        await asyncio.wait_for(session._update_activity_atask, 3)
        while not observed.empty():
            observed.get_nowait()
        await audio.frames.put(frame())
        witness = await asyncio.wait_for(observed.get(), 3)
        assert witness.observed
        assert float(witness.start) == pytest.approx(0, abs=1 / 16000)
        assert recorder.recording_started_at > session._started_at + 0.09
        assert session.input.audio is recorded and recorded.source is audio
    finally:
        await audio.frames.put(None)
        await session.aclose()
        await recorder.aclose()
        await model.aclose()
        await client.aclose()
    assert audio.__anext__ == original_next


async def test_source_replaced_in_prewarm_stays_observed_before_recognition_exists():
    sources, entered, turns = [ControlledVAD(), ControlledVAD()], asyncio.Event(), asyncio.Queue()
    observations = []

    class ReplacementInput(OwnedAudioInput):
        async def __anext__(self):
            value = await super().__anext__()
            observations.append(getattr(session._activity, "_audio_recognition", None) is None)
            return value

    class ReplacingSTT(STT):
        def prewarm(self):
            session.input.audio = replacement
            replacement.frames.put_nowait(rtc.AudioFrame(data=b"\0\0" * 320, sample_rate=16000, num_channels=1, samples_per_channel=320))
            activity = session._activity
            original = activity._resume_scheduling_task

            async def resume_after_source_delivery():
                try:
                    await replacement.delivery
                    return await original()
                finally:
                    activity._resume_scheduling_task = original

            # Hold this owned activity at a real asynchronous startup boundary.
            activity._resume_scheduling_task = resume_after_source_delivery

    class NextAgent(Agent):
        async def on_enter(self):
            await replacement.delivery
            entered.set()

        async def on_user_turn_completed(self, turn_ctx, new_message):
            turns.put_nowait(new_message.text_content)

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.TEXT and json.loads(message.data)["type"] == "session.close":
                await ws.send_json({"type": "transcript.final", "text": "replaced early"})
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with peer(handle) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin)
        models = [STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=sources[0]), ReplacingSTT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=sources[1])]
        session = AgentSession(stt=models[0], llm=None, tts=None, vad=None, turn_detection="stt", min_endpointing_delay=0, max_endpointing_delay=0)
        audio, replacement = OwnedAudioInput(), ReplacementInput(track_delivery=True)
        session.input.audio = audio
        for model in models:
            model.bind_session(session)
        try:
            options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
            await session.start(agent=Agent(instructions="Owned original input"), **options)
            session.update_agent(NextAgent(instructions="Owned early replacement", stt=models[1]))
            await asyncio.wait_for(entered.wait(), 3)
            assert observations[0], "the replacement pump must start before recognition is constructed"
            for samples, kind in [(320, vad.VADEventType.START_OF_SPEECH), (8800, vad.VADEventType.END_OF_SPEECH)]:
                value = rtc.AudioFrame(data=(b"\xff\x0f" if samples == 320 else b"\0\0") * samples, sample_rate=16000, num_channels=1, samples_per_channel=samples)
                await replacement.frames.put(value)
                observed = await asyncio.wait_for(sources[1].frames.get(), 3)
                sources[1].processed(observed)
                sources[1].emit(kind, frames=[observed], silence_duration=0 if samples == 320 else 0.55)
            assert await asyncio.wait_for(turns.get(), 3) == "replaced early"
            assert not models[1]._native_failed
        finally:
            await audio.frames.put(None)
            await replacement.frames.put(None)
            await session.aclose()
            for model in models:
                await model.aclose()
            await client.aclose()


@pytest.mark.skipif(not hasattr(Agent, "update_options"), reason="SDK predates live model option updates")
async def test_retained_closed_native_input_node_cannot_reinstall_hooks_in_local_sessions():
    class LocalSTT(stt.STT):
        def __init__(self):
            super().__init__(capabilities=stt.STTCapabilities(streaming=True, interim_results=False))
            self.started, self.frames = asyncio.Queue(), asyncio.Queue()

        async def _recognize_impl(self, *args, **kwargs):
            raise NotImplementedError

        def stream(self, *, language=None, conn_options=DEFAULT_API_CONNECT_OPTIONS):
            return LocalStream(stt=self, conn_options=conn_options)

    class LocalStream(stt.RecognizeStream):
        async def _run(self):
            self._stt.started.put_nowait(self)
            async for value in self._input_ch:
                if isinstance(value, rtc.AudioFrame):
                    self._stt.frames.put_nowait(value)

    client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url="http://127.0.0.1:1")
    model, local = STT(client, turn_vad=ControlledVAD()), LocalSTT()
    sessions = [AgentSession(stt=stt_model, llm=None, tts=None, vad=None) for stt_model in (model, local)]
    inputs = [OwnedAudioInput(), OwnedAudioInput()]
    for session, audio in zip(sessions, inputs):
        session.input.audio = audio
    agent = Agent(instructions="Owned local replacement fixture")
    original_node, original_update = agent.stt_node, agent.update_options
    model.bind_session(sessions[0])
    options = {"record": False} if "record" in inspect.signature(sessions[0].start).parameters else {}
    try:
        await sessions[0].start(agent=agent, **options)
        agent.update_options(stt=local)
        await asyncio.wait_for(local.started.get(), 3)
        await model.aclose()
        assert agent.stt_node == original_node
        sessions[0].clear_user_turn()
        await asyncio.wait_for(local.started.get(), 3)
        assert agent.stt_node == original_node
        assert agent.update_options == original_update
        await inputs[0].frames.put(None)
        await sessions[0].aclose()
        await sessions[1].start(agent=agent, **options)
        await asyncio.wait_for(local.started.get(), 3)
        await inputs[1].frames.put(frame())
        assert bytes((await asyncio.wait_for(local.frames.get(), 3)).data) == bytes(frame().data)
        assert agent.stt_node == original_node
    finally:
        for audio in inputs:
            await audio.frames.put(None)
        for session in reversed(sessions):
            await session.aclose()
        await model.aclose()
        await local.aclose()
        await client.aclose()


@pytest.mark.parametrize("wrapper", ["direct", "nested"])
@pytest.mark.parametrize("shape", ["unspaced", "partial", "punctuation", "ambiguous"])
@pytest.mark.parametrize("copy_fields", [False, True])
@pytest.mark.parametrize("replay", [False, True])
async def test_valid_native_segments_keep_canonical_text_and_available_speakers(wrapper, shape, copy_fields, replay):
    if replay and not hasattr(recognition.AudioRecognition, "_process_stt_event"):
        pytest.skip("SDK predates adaptive transcript replay")
    text = {"unspaced": "你好", "partial": "hello world", "punctuation": "Hello, world!", "ambiguous": "yes yes done"}[shape]
    segments = [{"text": "你" if shape == "unspaced" else "hello" if shape == "partial" else "Hello", "start_ms": 0, "end_ms": 300, "speaker": "A"}]
    if shape != "partial":
        segments.append({"text": "好" if shape == "unspaced" else "world", "start_ms": 300, "end_ms": 600, "speaker": "B"})
    if shape == "ambiguous":
        segments = [{"text": "yes", "start_ms": 0, "end_ms": 300, "speaker": "B"}, {"text": "done", "start_ms": 300, "end_ms": 600, "speaker": "C"}]
    source, turns, finals, seen = ControlledVAD(), asyncio.Queue(), [], []

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        seen.append(request.headers["Idempotency-Key"])
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.TEXT and json.loads(message.data)["type"] == "session.close":
                await ws.send_json({"type": "transcript.final", "text": text, "segments": segments, **({"speaker": "A"} if shape == "ambiguous" else {})})
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    class OwnedAgent(Agent):
        async def stt_node(self, audio, settings):
            async for event in Agent.default.stt_node(self, audio, settings):
                if shape == "punctuation":
                    event = copy.deepcopy(event)
                if copy_fields:
                    event = dataclasses.replace(event, alternatives=[dataclasses.replace(data) for data in event.alternatives])
                    # SDK telemetry must still be able to encode dataclasses;
                    # the witness must not retain a stream, future or owner.
                    json.dumps(dataclasses.asdict(event), ensure_ascii=False)
                if event.type is stt.SpeechEventType.FINAL_TRANSCRIPT:
                    finals.extend(event.alternatives)
                yield event

        async def on_user_turn_completed(self, turn_ctx, new_message):
            turns.put_nowait(new_message.text_content)

    async with peer(handle) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin)
        model = STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=source, diarization=True)
        wrapped = wrap_native_stt(model, wrapper)
        session = AgentSession(stt=wrapped, llm=None, tts=None, vad=None, turn_detection="stt", min_endpointing_delay=0, max_endpointing_delay=0)
        audio = OwnedAudioInput()
        session.input.audio = audio
        model.bind_session(session)
        transcript_binding = None
        try:
            options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
            await session.start(agent=OwnedAgent(instructions="Owned canonical attribution"), **options)
            transcript_binding = model._native_transcript_binding
            current_recognition = session._activity._audio_recognition
            if replay:
                current_recognition._transcript_gate_active = True
            for samples, kind in [(9600, vad.VADEventType.START_OF_SPEECH), (8800, vad.VADEventType.END_OF_SPEECH)]:
                value = rtc.AudioFrame(data=(b"\xff\x0f" if samples == 9600 else b"\0\0") * samples, sample_rate=16000, num_channels=1, samples_per_channel=samples)
                await audio.frames.put(value)
                observed = await asyncio.wait_for(source.frames.get(), 3)
                source.processed(observed)
                source.emit(kind, frames=[observed], silence_duration=0 if samples == 9600 else 0.55)
            if replay:
                async def held_end():
                    while not any(event.type is stt.SpeechEventType.END_OF_SPEECH for event in current_recognition._transcript_buffer):
                        await asyncio.sleep(0)
                await asyncio.wait_for(held_end(), 3)
                assert not current_recognition._audio_transcript
                assert turns.empty()
                current_recognition._flush_held_transcripts()
            assert await asyncio.wait_for(turns.get(), 3) == text
            assert len(seen) == 1
            assert not model._native_failed
            assert all(not stream._final_batches for stream in model._streams)
            expected = {None, "C"} if shape == "ambiguous" else {"A", None} if shape == "partial" else {"A", "B"}
            assert {data.provider_speaker_id for data in finals} >= expected
            assert_request_speakers(finals)
            if shape == "ambiguous":
                assert all(data.speaker_id is None for data in finals if "yes" in data.text)
            assert "".join("".join(data.text.split()) for data in finals) == "".join(text.split())
        finally:
            await audio.frames.put(None)
            await session.aclose()
            if wrapped is not model:
                await wrapped.aclose()
            await model.aclose()
            await client.aclose()
            if transcript_binding is not None:
                bound_recognition, original, _ = transcript_binding
                assert getattr(bound_recognition, original.__name__) is original
                assert model._native_transcript_binding is None


@pytest.mark.parametrize("wrapper", ["multi_speaker", "nested"])
async def test_native_request_speakers_are_independent_and_reset_primary_detection(wrapper):
    source, turns, finals, requests = ControlledVAD(), asyncio.Queue(), [], []

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        requests.append(request.headers["Idempotency-Key"])
        text = f"speaker {len(requests)}"
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.TEXT and json.loads(message.data)["type"] == "session.close":
                await ws.send_json({"type": "transcript.final", "text": text, "segments": [{"text": text, "speaker": "A", "start_ms": 0, "end_ms": 600}]})
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    class OwnedAgent(Agent):
        async def stt_node(self, audio, settings):
            async for event in Agent.default.stt_node(self, audio, settings):
                if event.type is stt.SpeechEventType.FINAL_TRANSCRIPT:
                    finals.extend(event.alternatives)
                yield event

        async def on_user_turn_completed(self, turn_ctx, new_message):
            turns.put_nowait(new_message.text_content)

    async with peer(handle) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin)
        model = STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=source, diarization=True)
        wrapped = wrap_native_stt(model, wrapper)
        session = AgentSession(stt=wrapped, llm=None, tts=None, vad=None, turn_detection="stt", min_endpointing_delay=0, max_endpointing_delay=0)
        audio = OwnedAudioInput()
        session.input.audio = audio
        model.bind_session(session)
        try:
            options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
            await session.start(agent=OwnedAgent(instructions="Owned request-local speakers"), **options)
            for index, pcm in enumerate([b"\xff\x7f", b"\x20\x00"]):
                for samples, kind in [(9600, vad.VADEventType.START_OF_SPEECH), (8800, vad.VADEventType.END_OF_SPEECH)]:
                    await audio.frames.put(rtc.AudioFrame(data=(pcm if samples == 9600 else b"\0\0") * samples, sample_rate=16000, num_channels=1, samples_per_channel=samples))
                    observed = await asyncio.wait_for(source.frames.get(), 3)
                    source.processed(observed)
                    source.emit(kind, frames=[observed], silence_duration=0 if samples == 9600 else 0.55)
                assert await asyncio.wait_for(turns.get(), 3) == f"speaker {index + 1}"
            assert len(requests) == len(set(requests)) == len(finals) == 2
            assert finals[0].speaker_id != finals[1].speaker_id, "the same vendor label in two requests is not a persistent speaker identity"
            assert [data.provider_speaker_id for data in finals] == ["A", "A"]
            assert [data.is_primary_speaker for data in finals] == [True, True], "primary detection must restart for a new request's identity space"
        finally:
            await audio.frames.put(None)
            await session.aclose()
            await wrapped.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.skipif(not hasattr(Agent, "update_options"), reason="SDK predates live model option updates")
@pytest.mark.parametrize("native_first", [False, True])
async def test_native_and_gateway_model_swaps_restore_the_actual_sdk_hooks(native_first):
    from livekit.agents.voice.agent_activity import AgentActivity

    from speko_gateway.client import CanonicalEvent

    source, turns, requests = ControlledVAD(), asyncio.Queue(), []
    text = "你好"

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        requests.append(request.headers["Idempotency-Key"])
        await ready(ws)
        sent = False
        async for message in ws:
            if message.type is aiohttp.WSMsgType.BINARY and not sent:
                sent = True
                await ws.send_json({"type": "transcript.final", "text": text, "segments": [{"text": "你", "speaker": "A", "start_ms": 0, "end_ms": 10}, {"text": "好", "speaker": "B", "start_ms": 10, "end_ms": 20}]})
            if message.type is aiohttp.WSMsgType.TEXT and json.loads(message.data)["type"] == "session.close":
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    class OwnedAgent(Agent):
        async def on_user_turn_completed(self, turn_ctx, new_message):
            turns.put_nowait(new_message.text_content)

    class GatewayTransport:
        # Exercise Gateway STT's stream/lifecycle with an owned, real speech
        # transport. Actual Gateway Unix-socket attribution is covered above.
        async def open(self, config):
            session = await client.open(dataclasses.replace(config, execution={"credential_source": "managed", "provider_route": "speko_relay", "relay_policy": "required"}, request={**config.request, "provider": "fixture", "model": "fixture-model"}))
            return GatewaySession(session)

    class GatewaySession:
        def __init__(self, session):
            self.session = session

        def __getattr__(self, name):
            return getattr(self.session, name)

        async def events(self):
            async for event in self.session.events():
                yield event
                if event.type == "transcript.final":
                    yield CanonicalEvent(type="speech.ended", data={})

    async with peer(handle) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin)
        models = [STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=source), STT(GatewayTransport())]
        order = [0, 1, 0] if native_first else [1, 0, 1]
        agent = OwnedAgent(instructions="Owned mixed-model hook lifecycle", stt=models[order[0]])
        session = AgentSession(llm=None, tts=None, vad=None, turn_detection="stt", min_endpointing_delay=0, max_endpointing_delay=0)
        audio = OwnedAudioInput()
        session.input.audio = audio
        models[0].bind_session(session)
        activity, recognizer = None, None
        try:
            options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
            await session.start(agent=agent, **options)
            activity = session._activity
            recognizer = activity._audio_recognition
            for index, selected in enumerate(order):
                if index:
                    agent.update_options(stt=models[selected])
                if selected == 0:
                    for samples, kind in [(320, vad.VADEventType.START_OF_SPEECH), (8800, vad.VADEventType.END_OF_SPEECH)]:
                        await audio.frames.put(rtc.AudioFrame(data=b"\xff\x0f" * samples, sample_rate=16000, num_channels=1, samples_per_channel=samples))
                        observed = await asyncio.wait_for(source.frames.get(), 3)
                        source.processed(observed)
                        source.emit(kind, frames=[observed], silence_duration=0 if samples == 320 else 0.55)
                else:
                    await audio.frames.put(frame())
                assert await asyncio.wait_for(turns.get(), 3) == text
            assert len(requests) == len(set(requests)) == 3
        finally:
            await audio.frames.put(None)
            await session.aclose()
            for model in reversed(models):
                await model.aclose()
            await client.aclose()
            if activity is not None:
                assert activity.on_end_of_turn.__func__ is AgentActivity.on_end_of_turn
                method = "_process_stt_event" if hasattr(recognizer, "_process_stt_event") else "_on_stt_event"
                assert getattr(recognizer, method).__func__ is getattr(recognition.AudioRecognition, method)


@pytest.mark.parametrize("skip_reply", [False, True])
async def test_manual_commit_promoted_interim_is_replaced_by_terminal_canonical_text(skip_reply):
    source, interim, release = ControlledVAD(), asyncio.Event(), asyncio.Event()
    turns, items = asyncio.Queue(), asyncio.Queue()

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ready(ws)
        sent = False
        async for message in ws:
            if message.type is aiohttp.WSMsgType.BINARY and not sent:
                sent = True
                await ws.send_json({"type": "transcript.delta", "text": "draft"})
            elif message.type is aiohttp.WSMsgType.TEXT and json.loads(message.data)["type"] == "session.close":
                await release.wait()
                await ws.send_json({"type": "transcript.final", "text": "draft final"})
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    class OwnedAgent(Agent):
        async def on_user_turn_completed(self, turn_ctx, new_message):
            turns.put_nowait(new_message.text_content)

    async with peer(handle) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin)
        model = STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=source)
        session = AgentSession(stt=model, llm=None, tts=None, vad=None, turn_detection="manual", min_endpointing_delay=0, max_endpointing_delay=0)
        audio = OwnedAudioInput()
        session.input.audio = audio
        session.on("user_input_transcribed", lambda event: interim.set() if not event.is_final else None)
        session.on("conversation_item_added", lambda event: items.put_nowait(event.item.text_content) if event.item.role == "user" else None)
        model.bind_session(session)
        try:
            options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
            await session.start(agent=OwnedAgent(instructions="Owned manual transcript fallback"), **options)
            for samples, kind in [(320, vad.VADEventType.START_OF_SPEECH), (8800, vad.VADEventType.END_OF_SPEECH)]:
                value = rtc.AudioFrame(data=(b"\xff\x0f" if samples == 320 else b"\0\0") * samples, sample_rate=16000, num_channels=1, samples_per_channel=samples)
                await audio.frames.put(value)
                observed = await asyncio.wait_for(source.frames.get(), 3)
                source.processed(observed)
                source.emit(kind, frames=[observed], silence_duration=0 if samples == 320 else 0.55)
            await asyncio.wait_for(interim.wait(), 3)
            await asyncio.wait_for(session.commit_user_turn(transcript_timeout=0.01, stt_flush_duration=0, skip_reply=skip_reply), 3)
            assert items.empty(), "an interim cannot commit before physical terminal evidence"
            if not hasattr(session._activity._audio_recognition, "_input_started_at"):
                assert session._activity._audio_recognition._audio_transcript == "draft", "exercise the actual legacy interim promotion"
            release.set()
            if skip_reply:
                assert await asyncio.wait_for(items.get(), 3) == "draft final"
                assert turns.empty()
            else:
                # With no LLM, the legacy SDK calls the hook without adding
                # a chat item; the accepted canonical message is the contract.
                assert await asyncio.wait_for(turns.get(), 3) == "draft final"
            assert not model._native_failed
        finally:
            release.set()
            await audio.frames.put(None)
            await session.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("partial", [False, True])
async def test_native_recognize_preserves_canonical_text_and_attributed_spans(partial):
    text = "hello world" if partial else "你好"
    segments = [{"text": "hello" if partial else "你", "speaker": "A", "start_ms": 0, "end_ms": 10}]
    if not partial:
        segments.append({"text": "好", "speaker": "B", "start_ms": 10, "end_ms": 20})

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.TEXT and json.loads(message.data)["type"] == "session.close":
                await ws.send_json({"type": "transcript.final", "text": text, "segments": segments})
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with peer(handle) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin)
        model = STT(client, provider="fixture", model="fixture-model", credential_source="managed", diarization=True, turn_vad=ControlledVAD(automatic=True))
        try:
            event = await model.recognize(frame())
            data = event.alternatives[0]
            assert data.text == text
            assert data.canonical_text == text
            assert data._native_final is None
            attributed = [part for part in data.speaker_segments if part.speaker_id is not None]
            assert [(part.text, part.provider_speaker_id) for part in attributed] == [(part["text"], part["speaker"]) for part in segments]
            assert_request_speakers(data.speaker_segments)
            if partial:
                assert any(part.text.strip() == "world" and part.speaker_id is None for part in data.speaker_segments)
        finally:
            await model.aclose()
            await client.aclose()


async def test_native_recognize_keeps_every_provider_final_in_one_request():
    requests = []

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        requests.append(request.headers["Idempotency-Key"])
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.TEXT and json.loads(message.data)["type"] == "session.close":
                await ws.send_json({"type": "transcript.final", "text": "one", "speaker": "A"})
                await ws.send_json({"type": "transcript.final", "text": "two", "speaker": "A"})
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with peer(handle) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin)
        model = STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=ControlledVAD())
        silence = rtc.AudioFrame(data=b"\0\0" * 8800, sample_rate=16000, num_channels=1, samples_per_channel=8800)
        try:
            event = await model.recognize([frame(), silence, frame(), silence])
            assert event.alternatives[0].text == "one two"
            assert [part.text for part in event.alternatives[0].speaker_segments] == ["one", "two"]
            assert len(requests) == 1
        finally:
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("invalid", ["invalid_label", "invalid_time"])
async def test_native_diarization_cannot_drop_words_or_publish_invalid_attribution(invalid):
    seen = []

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        seen.append(request.headers["Idempotency-Key"])
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.TEXT and json.loads(message.data)["type"] == "session.close":
                await ws.send_json({
                    "type": "transcript.final", "text": "one two",
                    "segments": [{"text": "one two", "start_ms": -1 if invalid == "invalid_time" else 0, "end_ms": 20, "speaker": 7 if invalid == "invalid_label" else "A"}],
                })
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with peer(handle) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin)
        model = STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=ControlledVAD(), diarization=True)
        try:
            with pytest.raises(APIConnectionError):
                await model.recognize(frame(), conn_options=APIConnectOptions(max_retry=3, retry_interval=0))
            assert model._native_failed
            assert len(seen) == 1
        finally:
            await model.aclose()
            await client.aclose()


@pytest.mark.skipif(not hasattr(Agent, "update_options"), reason="SDK predates live model option updates")
@pytest.mark.parametrize("wrapper", ["direct", "multi_speaker", "fallback", "nested"])
@pytest.mark.parametrize("buffered,prefix_frames", [(True, 0), (False, 0), (False, 35)])
async def test_delayed_wrapped_native_node_preserves_sample_timeline_and_endpointing_after_live_model_swap(wrapper, buffered, prefix_frames):
    sources = [ControlledVAD(), ControlledVAD()]
    node_waiting, resume_node = asyncio.Event(), asyncio.Event()
    prefix_consumed = asyncio.Event()
    turns = asyncio.Queue()
    terminal_at, seen, speech_ends, packets = [], [], [], []
    end_anchors, turn_completed_at = [], []

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        index = len(seen) + 1
        seen.append(request.headers["Idempotency-Key"])
        pcm = bytearray()
        packets.append(pcm)
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.BINARY:
                pcm.extend(message.data)
            if message.type is aiohttp.WSMsgType.TEXT and json.loads(message.data)["type"] == "session.close":
                await ws.send_json({"type": "transcript.final", "text": f"turn {index}"})
                await ws.send_json({"type": "session.closed", "usage": {}})
                terminal_at.append(time.monotonic())
                await ws.close()
        return ws

    async with peer(handle) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin)
        models = [STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=source, diarization=wrapper in ("multi_speaker", "nested")) for source in sources]
        wrapped = [wrap_native_stt(model, wrapper) for model in models]
        session = AgentSession(llm=None, tts=None, vad=None, turn_detection="stt", min_endpointing_delay=0.3, max_endpointing_delay=0.3)
        audio = OwnedAudioInput()
        session.input.audio = audio

        class OwnedAgent(Agent):
            async def stt_node(self, audio, model_settings):
                if self.stt is wrapped[1]:
                    node_waiting.set()
                    await resume_node.wait()
                    for _ in range(prefix_frames):
                        await anext(audio)
                    prefix_consumed.set()
                async for event in Agent.default.stt_node(self, audio, model_settings):
                    if self.stt is wrapped[1] and event.type is stt.SpeechEventType.END_OF_SPEECH:
                        speech_ends.append(event.alternatives[0].end_time)
                    yield event

            async def on_user_turn_completed(self, turn_ctx, new_message):
                turn_completed_at.append((time.time(), time.monotonic()))
                turns.put_nowait(new_message.text_content)

        async def boundary(source, value, kind, silence):
            await audio.frames.put(value)
            observed = await asyncio.wait_for(source.frames.get(), 3)
            source.processed(observed)
            source.emit(kind, frames=[observed], silence_duration=silence)

        for model in models:
            model.bind_session(session)
        try:
            options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
            agent = OwnedAgent(instructions="Owned delayed STT fixture", stt=wrapped[0])
            await session.start(agent=agent, **options)
            await boundary(sources[0], frame(), vad.VADEventType.START_OF_SPEECH, 0)
            await boundary(sources[0], rtc.AudioFrame(data=b"\0\0" * 8800, sample_rate=16000, num_channels=1, samples_per_channel=8800), vad.VADEventType.END_OF_SPEECH, 0.55)
            assert await asyncio.wait_for(turns.get(), 3) == "turn 1"
            agent.update_options(stt=wrapped[1])
            await asyncio.wait_for(node_waiting.wait(), 3)
            current_recognition = session._activity._audio_recognition
            original_eou = current_recognition._run_eou_detection

            @wraps(original_eou)
            def measure_eou(*args, **kwargs):
                if current_recognition._audio_transcript == "turn 2" and current_recognition._last_speaking_time is not None:
                    end_anchors.append(current_recognition._last_speaking_time)
                return original_eou(*args, **kwargs)

            current_recognition._run_eou_detection = measure_eou
            if buffered:
                await audio.frames.put(frame())
            await asyncio.sleep(0.65)
            resume_node.set()
            if not buffered:
                if prefix_frames:
                    # Reuse one SDK frame too: each input occurrence still
                    # needs its own position before custom-node filtering.
                    ignored = rtc.AudioFrame(data=b"\x01\x00" * 320, sample_rate=16000, num_channels=1, samples_per_channel=320)
                    for _ in range(prefix_frames):
                        await audio.frames.put(ignored)
                        await asyncio.sleep(0.02)
                    await asyncio.wait_for(prefix_consumed.wait(), 3)
                    assert ignored.userdata == {}, "tracking must not mutate the producer's reused frame"
                async def stream_created():
                    while sources[1].current is None:
                        await asyncio.sleep(0.001)
                await asyncio.wait_for(stream_created(), 3)
                await audio.frames.put(frame())
            observed = await asyncio.wait_for(sources[1].frames.get(), 3)
            sources[1].processed(observed)
            sources[1].emit(vad.VADEventType.START_OF_SPEECH, frames=[observed])
            await boundary(sources[1], rtc.AudioFrame(data=b"\0\0" * 320, sample_rate=16000, num_channels=1, samples_per_channel=320), vad.VADEventType.END_OF_SPEECH, 0.02)
            assert await asyncio.wait_for(turns.get(), 3) == "turn 2"
            # The SDK can rebase through an epoch-sized float; one 16 kHz
            # sample is the relevant precision, rather than nanoseconds.
            assert speech_ends == pytest.approx([prefix_frames * 0.02 + 0.02], abs=1 / 16000), "speech retains its position before custom input consumption"
            elapsed = turn_completed_at[1][1] - terminal_at[1]
            if buffered:
                assert elapsed < 0.2, "old buffered speech has already outlived the endpointing delay"
            else:
                # Provider closure can arrive after part of the SDK delay has
                # elapsed. Measure the configured delay from its actual SDK
                # speech anchor, including source-clock drift under load.
                assert end_anchors
                assert turn_completed_at[1][0] - end_anchors[-1] >= 0.3 - 1 / 16000, "fresh input must retain the configured endpointing delay"
            assert len(seen) == len(set(seen)) == 2
            assert bytes(packets[1]) == bytes(frame().data) + b"\0\0" * 320, "filtered input stays out of the provider and kept PCM is unchanged"
        finally:
            resume_node.set()
            await audio.frames.put(None)
            await session.aclose()
            for recognition_model, model in zip(wrapped, models):
                if recognition_model is not model:
                    await recognition_model.aclose()
                await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("wrapper", ["multiple_models", "rewritten", "suppressed", "foreign_model"])
async def test_native_wrapper_refuses_unsupported_authority_before_admission(wrapper):
    calls = []

    async def handle(request):
        calls.append(request.path)
        return web.Response(status=500)

    async with peer(handle) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin)
        model = STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=ControlledVAD(), diarization=True)
        other = STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=ControlledVAD())
        if wrapper == "multiple_models":
            recognition_model = stt.FallbackAdapter([model, other], max_retry_per_stt=0, retry_interval=0)
            message = "control-plane recovery"
        elif wrapper == "foreign_model":
            recognition_model = other
            message = "active recognition model"
        else:
            recognition_model = stt.MultiSpeakerAdapter(stt=model, primary_format="speaker: {text}" if wrapper == "rewritten" else "{text}", suppress_background_speaker=wrapper == "suppressed")
            message = "transcript-preserving"
        session = AgentSession(stt=recognition_model, llm=None, tts=None, vad=None, turn_detection="stt")
        model.bind_session(session)
        try:
            options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
            await session.start(agent=Agent(instructions="Owned unsupported wrapper fixture"), **options)
            with pytest.raises(ValueError, match=message):
                model.stream()
            assert calls == []
            assert not model._native_failed
        finally:
            await session.aclose()
            await recognition_model.aclose()
            await model.aclose()
            await other.aclose()
            await client.aclose()


@pytest.mark.parametrize("status", [401, 403, 429, 503])
async def test_native_websocket_http_refusal_is_never_a_retryable_connection_failure(
    status,
):
    seen = []

    async def handle(request):
        seen.append(1)
        return web.json_response(
            {"error": {"message": "private content", "retryable": True}}, status=status
        )

    async with peer(handle) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_fixture",
            session_id=str(uuid.uuid4()),
            base_url=origin,
        )
        try:
            from speko_gateway.client import SessionConfig

            cfg = SessionConfig(
                kind="tts",
                execution={
                    "credential_source": "managed",
                    "provider_route": "speko_relay",
                    "relay_policy": "required",
                },
                request={"provider": "fixture", "model": "fixture-model"},
                media={"encoding": "pcm_s16le", "sample_rate_hz": 24000, "channels": 1},
            )
            with pytest.raises(GatewayError) as error:
                await client.open(cfg)
            assert not error.value.retryable
            assert error.value.code == f"http_{status}"
            assert "private" not in str(error.value)
        finally:
            await client.aclose()
    assert seen == [1]


@pytest.mark.parametrize("frontier", ["partial_silence", "pending_onset", "cleared_onset", "stalled_onset"])
@pytest.mark.parametrize("activation_threshold", [0.2, 0.8])
@pytest.mark.parametrize("engine", ["inference", "silero16", "silero8"])
async def test_native_actual_inference_vad_handles_incomplete_frontiers(frontier, activation_threshold, engine):
    # Keep the installed detector's buffering, threshold state, event ordering,
    # executor and stream lifecycle. Only its owned model probabilities are
    # deterministic, so this exercises the real 512-sample inference window.
    detector = owned_probability_vad(engine, min_speech_duration=0.05, min_silence_duration=0.032, activation_threshold=activation_threshold)
    terminal, turns, packets = asyncio.Event(), asyncio.Queue(), []

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        index = len(packets)
        packets.append([])
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.BINARY:
                packets[index].append(message.data)
            elif message.type is aiohttp.WSMsgType.TEXT and json.loads(message.data)["type"] == "session.close":
                await ws.send_json({"type": "transcript.final", "text": "first" if index == 0 else "second"})
                await ws.send_json({"type": "session.closed", "usage": {}})
                terminal.set()
                await ws.close()
        return ws

    class OwnedAgent(Agent):
        async def on_user_turn_completed(self, turn_ctx, new_message):
            turns.put_nowait(new_message.text_content)

    def audio_frame(pcm):
        return rtc.AudioFrame(data=pcm, sample_rate=16000, num_channels=1, samples_per_channel=len(pcm) // 2)

    async with peer(handle) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin)
        model = STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=detector)
        session = AgentSession(stt=model, llm=None, tts=None, vad=None, turn_detection="stt", min_endpointing_delay=0.2, max_endpointing_delay=0.2)
        audio = OwnedAudioInput()
        session.input.audio = audio
        model.bind_session(session)
        try:
            options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
            await session.start(agent=OwnedAgent(instructions="Owned real VAD frontiers"), **options)
            speech, silence = b"\xff\x0f" * 512, b"\0\0" * 512
            first = speech * 2 + silence
            await audio.frames.put(audio_frame(first))
            tail = b"\0\0" * 320 if frontier == "partial_silence" else speech
            await audio.frames.put(audio_frame(tail))
            await asyncio.wait_for(terminal.wait(), 3)
            if frontier == "partial_silence":
                assert await asyncio.wait_for(turns.get(), 1) == "first", "a partial silent inference window must not stall a completed turn"
                assert len(packets) == 1
            else:
                await asyncio.sleep(0.35)
                assert turns.empty(), "a possible onset before START_OF_SPEECH must keep the previous turn pending"
                if frontier == "stalled_onset":
                    async def failed():
                        while not model._native_failed:
                            await asyncio.sleep(0.01)
                    await asyncio.wait_for(failed(), 2)
                    assert turns.empty(), "an ambiguous paused onset cannot authorize a turn"
                    assert len(packets) == 1, "recovery never retries provider admission"
                elif frontier == "cleared_onset":
                    await audio.frames.put(audio_frame(silence))
                    assert await asyncio.wait_for(turns.get(), 2) == "first"
                    assert len(packets) == 1
                else:
                    await audio.frames.put(audio_frame(speech + silence))
                    assert await asyncio.wait_for(turns.get(), 2) == "first second"
                    assert len(packets) == 2
                    assert b"".join(packets[1]) == speech * 2 + silence
            assert b"".join(packets[0]) == first, "detector processing must not invent provider PCM"
            assert model._native_failed is (frontier == "stalled_onset")
        finally:
            await audio.frames.put(None)
            await session.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("engine", ["inference", "silero16", "silero8"])
@pytest.mark.parametrize("progressing", [False, True])
async def test_native_onset_recovery_distinguishes_paused_and_progressing_real_time_speech(engine, progressing):
    detector = owned_probability_vad(engine, min_speech_duration=1.5, min_silence_duration=0.032)
    turns, packets, terminal = asyncio.Queue(), [], asyncio.Event()

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        packet = []
        packets.append(packet)
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.BINARY:
                packet.append(message.data)
            elif message.type is aiohttp.WSMsgType.TEXT and json.loads(message.data)["type"] == "session.close":
                await ws.send_json({"type": "transcript.final", "text": "continuous captured speech"})
                await ws.send_json({"type": "session.closed", "usage": {}})
                terminal.set()
                await ws.close()
        return ws

    class OwnedAgent(Agent):
        async def on_user_turn_completed(self, turn_ctx, new_message):
            turns.put_nowait(new_message.text_content)

    def frame(raw):
        return rtc.AudioFrame(data=raw, sample_rate=16000, num_channels=1, samples_per_channel=len(raw) // 2)

    async def failed(model):
        while not model._native_failed:
            await asyncio.sleep(0.01)

    voice, quiet = b"\xff\x0f" * 512, b"\0\0" * 512
    async with peer(handle) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id=str(uuid.uuid4()), base_url=origin)
        model = STT(client, provider="fixture", model="fixture-model", credential_source="managed", turn_vad=detector)
        session = AgentSession(stt=model, llm=None, tts=None, vad=None, turn_detection="stt", min_endpointing_delay=0.05, max_endpointing_delay=0.05)
        audio = OwnedAudioInput()
        session.input.audio = audio
        model.bind_session(session)
        try:
            options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
            await session.start(agent=OwnedAgent(instructions="Owned progressing onset"), **options)
            count = 50 if progressing else 1
            for _ in range(count):
                await audio.frames.put(frame(voice))
                await asyncio.sleep(0.032)
                assert not model._native_failed
            if progressing:
                await audio.frames.put(frame(quiet * 3))
                await asyncio.wait_for(terminal.wait(), 3)
                assert await asyncio.wait_for(turns.get(), 3) == "continuous captured speech"
                assert not model._native_failed
                assert len(packets) == 1
                actual = b"".join(packets[0])
                assert actual.startswith(voice * count)
                assert (voice * count + quiet * 3).startswith(actual)
            else:
                await asyncio.wait_for(failed(model), 2)
                assert packets == [] and turns.empty()
        finally:
            await audio.frames.put(None)
            await session.aclose()
            await model.aclose()
            await client.aclose()
