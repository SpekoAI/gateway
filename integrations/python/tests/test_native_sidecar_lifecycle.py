"""Actual SDK/sidecar lifecycle controls for selected and completed requests."""

from __future__ import annotations

import asyncio
import inspect
import json
import uuid

import aiohttp
import pytest
from aiohttp import web
from livekit import rtc
from livekit.agents import Agent, AgentSession, APIConnectionError, vad
from livekit.agents.voice import audio_recognition as recognition
from test_native_sidecar import (
    LOCAL_TOKEN,
    MODEL_KEY,
    collect_stream,
    native_stack,
    ready,
)
from test_native_sidecar import native_binary as sidecar_binary
from test_relay_speech_livekit import ControlledVAD, OwnedAudioInput, frame

from speko_gateway import RelaySpeechClient
from speko_gateway.livekit import STT, TTS

native_binary = sidecar_binary


async def eventually(check):
    async def wait():
        while not check():
            await asyncio.sleep(0)

    await asyncio.wait_for(wait(), 3)


@pytest.mark.parametrize("failure", ["error", "eof"])
async def test_selected_native_standby_failure_never_opens_a_replacement(
    native_binary, failure
):
    configured, release = asyncio.Event(), asyncio.Event()
    admissions = []

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        admissions.append(request.headers["Idempotency-Key"])
        if len(admissions) == 1:
            configured.set()
            await release.wait()
            if failure == "error":
                await ws.send_json(
                    {
                        "type": "error",
                        "error": {"code": "provider_failure", "retryable": True},
                    }
                )
            await ws.close()
            return ws
        await ready(ws)
        async for message in ws:
            control = json.loads(message.data)
            if control["type"] == "input.commit":
                await ws.send_json({"type": "utterance.started", "sequence": 1})
                await ws.send_bytes(b"\x01\x00" * 480)
                await ws.send_json({"type": "utterance.done", "sequence": 1})
            elif control["type"] == "session.close":
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with native_stack(native_binary, handle) as (socket, _):
        client = RelaySpeechClient(
            api_key=MODEL_KEY,
            session_id=str(uuid.uuid4()),
            socket_path=socket,
            local_auth_token=LOCAL_TOKEN,
        )
        model = TTS(
            client, provider="fixture", model="fixture-model", standby_sessions=1
        )
        model.prewarm()
        await asyncio.wait_for(configured.wait(), 3)
        stream = model.stream()
        stream.push_text("Original reply.")
        stream.end_input()
        consuming = asyncio.create_task(collect_stream(stream))
        try:
            # The actual reply has selected the still-opening standby before
            # the peer fails its original configure. No standby is injected.
            await eventually(lambda: not model._standbys)
            release.set()
            error = None
            try:
                await asyncio.wait_for(consuming, 3)
            except APIConnectionError as failure_error:
                error = failure_error
            assert len(admissions) == 1, "selected ambiguous standby was readmitted"
            assert error is not None and not error.retryable
        finally:
            release.set()
            if not consuming.done():
                consuming.cancel()
            await asyncio.gather(consuming, return_exceptions=True)
            await stream.aclose()
            await model.aclose()
            await client.aclose()


async def test_empty_native_requests_release_batches_while_stream_stays_open(
    native_binary,
):
    source, closed, turns = ControlledVAD(), [], asyncio.Queue()

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
                closed.append(request.headers["Idempotency-Key"])
                if len(closed) == 4:
                    await ws.send_json(
                        {"type": "transcript.final", "text": "healthy after silence"}
                    )
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    class OwnedAgent(Agent):
        async def on_user_turn_completed(self, turn_ctx, new_message):
            turns.put_nowait(new_message.text_content)

    async with native_stack(native_binary, handle) as (socket, _):
        client = RelaySpeechClient(
            api_key=MODEL_KEY,
            session_id=str(uuid.uuid4()),
            socket_path=socket,
            local_auth_token=LOCAL_TOKEN,
        )
        model = STT(client, provider="fixture", model="fixture-model", turn_vad=source)
        session = AgentSession(
            stt=model,
            llm=None,
            tts=None,
            vad=None,
            turn_detection="stt",
            min_endpointing_delay=0,
            max_endpointing_delay=0,
        )
        audio = OwnedAudioInput()
        session.input.audio = audio
        model.bind_session(session)
        counts = []
        try:
            options = (
                {"record": False}
                if "record" in inspect.signature(session.start).parameters
                else {}
            )
            await session.start(
                agent=OwnedAgent(instructions="Owned empty results"), **options
            )
            for number in range(4):
                for samples, kind in [
                    (320, vad.VADEventType.START_OF_SPEECH),
                    (8800, vad.VADEventType.END_OF_SPEECH),
                ]:
                    value = (
                        frame()
                        if samples == 320
                        else rtc.AudioFrame(
                            data=b"\0\0" * samples,
                            sample_rate=16000,
                            num_channels=1,
                            samples_per_channel=samples,
                        )
                    )
                    await audio.frames.put(value)
                    observed = await asyncio.wait_for(source.frames.get(), 3)
                    source.processed(observed)
                    source.emit(
                        kind,
                        frames=[observed],
                        silence_duration=0 if samples == 320 else 0.55,
                    )
                await eventually(
                    lambda number=number: (
                        len(closed) == number + 1
                        and all(
                            not stream._admission_started for stream in model._streams
                        )
                    )
                )
                if number < 3:
                    counts.append(
                        sum(len(stream._final_batches) for stream in model._streams)
                    )
            assert await asyncio.wait_for(turns.get(), 3) == "healthy after silence"
            assert counts == [0, 0, 0], "empty terminal requests retained final batches"
            assert len(set(closed)) == 4 and not model._native_failed
            assert all(not stream._final_batches for stream in model._streams)
        finally:
            await audio.frames.put(None)
            await session.aclose()
            await model.aclose()
            await client.aclose()


async def test_recorded_legacy_consecutive_frames_keep_exact_positions(
    native_binary, tmp_path
):
    if hasattr(recognition.AudioRecognition, "_input_started_at"):
        pytest.skip("modern SDK owns its input clock")
    from livekit.agents.voice.recorder_io import RecorderIO

    observed, source = asyncio.Queue(), ControlledVAD()

    class OwnedAgent(Agent):
        async def stt_node(self, audio, settings):
            async def capture():
                async for value in audio:
                    observed.put_nowait(value.userdata["_speko_native_input_position"])
                    yield value

            async for event in Agent.default.stt_node(self, capture(), settings):
                yield event

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

    async with native_stack(native_binary, handle) as (socket, _):
        client = RelaySpeechClient(
            api_key=MODEL_KEY,
            session_id=str(uuid.uuid4()),
            socket_path=socket,
            local_auth_token=LOCAL_TOKEN,
        )
        model = STT(client, provider="fixture", model="fixture-model", turn_vad=source)
        session = AgentSession(stt=model, llm=None, tts=None, vad=None)
        audio = OwnedAudioInput()
        session.input.audio = audio
        model.bind_session(session)
        recorder = RecorderIO(agent_session=session)
        recorded = recorder.record_input(audio)
        recorder.record_output(None)
        original_next = audio.__anext__
        try:
            await session.start(
                agent=OwnedAgent(instructions="Owned recorder epoch"), record=False
            )
            await asyncio.sleep(0.1)
            await recorder.start(output_path=tmp_path / "owned-input.ogg")
            session._recorder_io = recorder
            session.input.audio = recorded
            session.update_agent(OwnedAgent(instructions="Owned recorded frames"))
            await asyncio.wait_for(session._update_activity_atask, 3)
            while not observed.empty():
                observed.get_nowait()
            witnesses = []
            for _ in range(3):
                await audio.frames.put(frame())
                witnesses.append(await asyncio.wait_for(observed.get(), 3))
                if len(witnesses) > 1:
                    left, right = witnesses[-2:]
                    assert right.start == left.start + left.duration, (
                        "recorder epoch publication overlapped consecutive captured frames"
                    )
            assert all(w.observed for w in witnesses)
            assert float(witnesses[0].start) == pytest.approx(0, abs=1 / 16000)
            assert not model._native_failed
            assert recorder.recording_started_at > session._started_at + 0.09
            assert session.input.audio is recorded and recorded.source is audio
        finally:
            await audio.frames.put(None)
            await session.aclose()
            await recorder.aclose()
            await model.aclose()
            await client.aclose()
        assert audio.__anext__ == original_next
