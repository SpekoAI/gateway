from __future__ import annotations

import asyncio
import copy
import inspect
import json
import time
import uuid
from contextlib import asynccontextmanager
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
from speko_gateway.livekit import STT, TTS


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
    def __init__(self):
        super().__init__(label="owned-native-input")
        self.frames = asyncio.Queue()

    async def __anext__(self):
        value = await self.frames.get()
        if value is None:
            raise StopAsyncIteration
        return value


def wrap_native_stt(model, wrapper):
    if wrapper in ("multi_speaker", "nested"):
        model = stt.MultiSpeakerAdapter(stt=model)
    if wrapper in ("fallback", "nested"):
        model = stt.FallbackAdapter([model], max_retry_per_stt=3, retry_interval=0)
    return model


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


@pytest.mark.parametrize("own_vad", [False, True])
@pytest.mark.parametrize("wrapper", ["direct", "nested"])
@pytest.mark.parametrize("first_terminal_delay", [0, 0.35])
async def test_native_continuation_is_transcribed_before_framework_turn_commit(own_vad, wrapper, first_terminal_delay):
    await assert_native_continuation(own_vad, wrapper, first_terminal_delay)


async def assert_native_continuation(own_vad, wrapper, first_terminal_delay, *, turn_detector=None, expected_turns=("first second",), wait_for_first_terminal=False):
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
                await ws.send_json({"type": "transcript.final", "text": "first" if index == 0 else "second"})
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
async def test_native_turn_uses_actual_detector_decision_for_queued_intermediate_pause(probability):
    class OwnedTurnDetector:
        model, provider = "owned-turn-detector", "owned"

        async def supports_language(self, language):
            return True

        async def unlikely_threshold(self, language):
            return 0.5

        async def predict_end_of_turn(self, chat_ctx, *, timeout=None):
            await asyncio.sleep(0.01)
            return probability

    expected = ("first second",) if probability < 0.5 else ("first", "second")
    await assert_native_continuation(True, "nested", 0.35, turn_detector=OwnedTurnDetector(), expected_turns=expected)


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


@pytest.mark.parametrize("failure", ["unprocessed-boundary", "stalled-detector-bound"])
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
            else:
                stream.push_frame(
                    rtc.AudioFrame(
                        data=b"\0\0" * (31 * 16000),
                        sample_rate=16000,
                        num_channels=1,
                        samples_per_channel=31 * 16000,
                    )
                )
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
        audio = OwnedAudioInput()
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
            assert [(item.text, item.speaker_id) for item in finals] == [(text, speaker) for text, speaker, _, _ in expected]
            assert [item.start_time for item in finals] == pytest.approx([start for _, _, start, _ in expected])
            assert [item.end_time for item in finals] == pytest.approx([end for _, _, _, end in expected])
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


@pytest.mark.parametrize("invalid", ["missing_words", "invalid_label", "invalid_time"])
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
                    "segments": [{"text": "one" if invalid == "missing_words" else "one two", "start_ms": -1 if invalid == "invalid_time" else 0, "end_ms": 20, "speaker": 7 if invalid == "invalid_label" else "A"}],
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
            elapsed = time.monotonic() - terminal_at[1]
            if buffered:
                assert elapsed < 0.2, "old buffered speech has already outlived the endpointing delay"
            else:
                assert elapsed >= 0.2, "fresh input must retain the configured endpointing delay"
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
