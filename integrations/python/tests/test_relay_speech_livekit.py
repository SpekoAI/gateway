from __future__ import annotations

import asyncio
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

    def processed(self, value):
        self.index += (
            value.samples_per_channel * self.inference_rate // value.sample_rate
        )
        self.prefix.append(value)
        self.emit(vad.VADEventType.INFERENCE_DONE, frames=[value])

    def emit(self, kind, *, frames=None, speech_duration=0.02, silence_duration=0):
        assert self.current is not None
        self.current._event_ch.send_nowait(
            vad.VADEvent(
                type=kind,
                samples_index=self.index,
                timestamp=self.index / self.inference_rate,
                speech_duration=speech_duration,
                silence_duration=silence_duration,
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


@pytest.mark.skipif(
    not hasattr(recognition, "_STTPipeline"),
    reason="This SDK release predates AgentSession's outer STT recovery pump",
)
async def test_actual_agent_stt_recovery_pump_cannot_readmit_a_failed_native_request(
    monkeypatch,
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
        )
        agent = Agent(instructions="Owned recovery fixture")
        model.bind_agent(agent)
        agent._activity = SimpleNamespace(
            stt=model,
            vad=None,
            _audio_recognition=None,
            # Isolate the real SDK recovery pump; no successful framework turn
            # is authorized by this fixture. Actual sessions are covered below.
            on_end_of_turn=lambda _info: False,
            session=SimpleNamespace(
                current_agent=agent,
                conn_options=SimpleNamespace(
                    stt_conn_options=APIConnectOptions(max_retry=3, retry_interval=0)
                ),
                _recorder_io=None,
                _started_at=time.time(),
            ),
        )
        pipeline = recognition._STTPipeline(
            lambda audio, settings: Agent.default.stt_node(agent, audio, settings)
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
async def test_actual_native_agent_session_keeps_turn_authority_across_commits_and_handoffs(
    transition, binding,
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
        )
        session = AgentSession(
            stt=model,
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
            assert len(seen) == len(set(seen)) == 2
            assert agent.on_user_turn_completed == original
            assert not model._native_failed
        finally:
            await audio.frames.put(None)
            await session.aclose()
            await model.aclose()
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
