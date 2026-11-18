"""Connected sources and real provider feedback must have honest ownership."""

from __future__ import annotations

import asyncio
import base64
import json

import pytest
from aiohttp import web
from livekit import rtc
from livekit.agents import AgentSession, llm
from test_native_realtime_cancellation import client_model, start_agent
from test_native_realtime_commit import MANUAL
from test_native_realtime_context import google_output
from test_native_sidecar import LOCAL_TOKEN, MODEL_KEY, native_stack
from test_native_sidecar import native_binary as native_binary  # noqa: PLC0414
from test_relay_speech_livekit import OwnedAudioInput

from speko_gateway import GatewayError, RelayRealtimeClient
from speko_gateway.livekit import RealtimeModel


@pytest.mark.skipif(not MANUAL, reason="SDK cannot negotiate explicit manual mode")
async def test_continuous_manual_source_refuses_before_first_turn_not_first_reply(
    native_binary,
):
    source = OwnedAudioInput()
    opened, commands = [], []
    first_input, answered, closed, stop = (asyncio.Event() for _ in range(4))
    outputs = []

    async def peer(request):
        opened.append(request.path)
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})

        async def answer():
            await asyncio.sleep(0.1)
            if not ws.closed:
                await google_output(ws, "FIRST_CONNECTED_REPLY")

        try:
            async for message in ws:
                command = json.loads(message.data)
                commands.append(command)
                control = command.get("realtimeInput", {})
                if "audio" in control:
                    first_input.set()
                if "activityEnd" in control:
                    outputs.append(asyncio.create_task(answer()))
        finally:
            closed.set()
            for task in outputs:
                task.cancel()
            await asyncio.gather(*outputs, return_exceptions=True)
        return ws

    async def microphone():
        while not stop.is_set():
            # The real SDK forwards these continuously; commit never pauses it.
            source.frames.put_nowait(rtc.AudioFrame(bytes(640), 16000, 1, 320))
            await asyncio.sleep(0.02)

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        actor = AgentSession(
            llm=model, turn_detection="manual", vad=None, stt=None, tts=None
        )
        actor.input.audio = source
        actor.on(
            "conversation_item_added",
            lambda event: (
                answered.set()
                if isinstance(event.item, llm.ChatMessage)
                and event.item.text_content == "FIRST_CONNECTED_REPLY"
                else None
            ),
        )
        producer = asyncio.create_task(microphone())
        try:
            try:
                await start_agent(actor)
            except (GatewayError, llm.RealtimeError) as error:
                assert "manual" in str(error).lower() and "source" in str(error).lower()
                assert not opened and not commands and not first_input.is_set()
                return
            await asyncio.wait_for(first_input.wait(), 5)
            actor.commit_user_turn(skip_reply=True)
            tasks = [
                asyncio.create_task(answered.wait()),
                asyncio.create_task(closed.wait()),
            ]
            try:
                await asyncio.wait(
                    tasks, timeout=5, return_when=asyncio.FIRST_COMPLETED
                )
                assert answered.is_set() and not closed.is_set(), (
                    "accepted continuous source killed its FIRST committed reply"
                )
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
            pytest.fail(
                "incompatible continuous manual source was not refused before its first turn"
            )
        finally:
            stop.set()
            producer.cancel()
            await asyncio.gather(producer, return_exceptions=True)
            source.frames.put_nowait(None)
            await actor.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("repeated_feedback", [False, True])
async def test_connected_provider_vad_second_answer_survives_postterminal_feedback(
    native_binary, repeated_feedback
):
    source, sockets, words = OwnedAudioInput(), asyncio.Queue(), asyncio.Queue()
    feedback, stop = asyncio.Event(), asyncio.Event()
    late_sent = False
    replies = 0
    frames = []

    async def peer(request):
        nonlocal replies
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        sockets.put_nowait(ws)
        async for message in ws:
            command = json.loads(message.data)
            audio = command.get("realtimeInput", {}).get("audio")
            if audio:
                frames.append(base64.b64decode(audio["data"]))
                if replies == 0:
                    replies += 1
                    await google_output(ws, "FIRST_ANSWER")
                elif late_sent and replies == 1:
                    replies += 1
                    await google_output(ws, "SECOND_ANSWER")
        return ws

    async def microphone():
        while not stop.is_set():
            source.frames.put_nowait(rtc.AudioFrame(b"\x03\x12" * 320, 16000, 1, 320))
            await asyncio.sleep(0.02)

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        actor = AgentSession(
            llm=model, turn_detection="realtime_llm", vad=None, stt=None, tts=None
        )
        actor.input.audio = source
        actor.on(
            "conversation_item_added",
            lambda event: (
                words.put_nowait(event.item.text_content)
                if isinstance(event.item, llm.ChatMessage)
                and event.item.role == "assistant"
                else None
            ),
        )
        producer = asyncio.create_task(microphone())
        try:
            await start_agent(actor)
            native = next(iter(model._sessions))
            native.on("input_speech_stopped", lambda event: feedback.set())
            ws = await asyncio.wait_for(sockets.get(), 5)
            assert await asyncio.wait_for(words.get(), 5) == "FIRST_ANSWER"
            if hasattr(actor, "wait_for_idle"):
                await asyncio.wait_for(actor.wait_for_idle(), 5)
            assert native._generation is None and not native._google_response_pending
            for _ in range(2 if repeated_feedback else 1):
                feedback.clear()
                await ws.send_json({"serverContent": {"interrupted": True}})
                await asyncio.wait_for(feedback.wait(), 5)
            late_sent = True
            assert await asyncio.wait_for(words.get(), 5) == "SECOND_ANSWER"
            assert native._failure is None and len(frames) >= 2
            assert all(frame == b"\x03\x12" * 320 for frame in frames)
        finally:
            stop.set()
            producer.cancel()
            await asyncio.gather(producer, return_exceptions=True)
            source.frames.put_nowait(None)
            await actor.aclose()
            await model.aclose()
            await client.aclose()


async def server_answer(ws, identity, text):
    await ws.send_json({"type": "response.created", "response": {"id": identity}})
    await ws.send_json(
        {
            "type": "response.output_audio_transcript.delta",
            "response_id": identity,
            "item_id": identity + "_msg",
            "delta": text,
        }
    )
    await ws.send_json(
        {
            "type": "response.output_audio.delta",
            "response_id": identity,
            "item_id": identity + "_msg",
            "delta": base64.b64encode(b"\x04\x10" * 2400).decode(),
        }
    )
    await ws.send_json(
        {"type": "response.done", "response": {"id": identity, "status": "completed"}}
    )


@pytest.mark.parametrize(
    "protocol,model_name,voice",
    [
        ("openai.realtime.v1", "gpt-realtime-2.1", "marin"),
        ("xai.realtime.v1", "grok-voice-3.0", "eve"),
    ],
)
@pytest.mark.parametrize("action", ["provider", "local-before", "local-during"])
async def test_server_vad_preoutput_feedback_retires_future_before_new_input_refusal(
    native_binary, monkeypatch, protocol, model_name, voice, action
):
    source = OwnedAudioInput()
    sockets, futures, words = (asyncio.Queue() for _ in range(3))
    requested, feedback, closed, stop = (asyncio.Event() for _ in range(4))
    commands, pcm = [], bytearray()

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        setup = await ws.receive_json()
        await ws.send_json({"type": "session.updated", "session": setup["session"]})
        sockets.put_nowait(ws)
        try:
            async for message in ws:
                command = json.loads(message.data)
                commands.append(command)
                if command.get("type") == "response.create":
                    requested.set()
                elif command.get("type") == "input_audio_buffer.append":
                    pcm.extend(base64.b64decode(command["audio"]))
        finally:
            closed.set()
        return ws

    async def microphone():
        while not stop.is_set():
            source.frames.put_nowait(rtc.AudioFrame(b"\x02\x10" * 480, 24000, 1, 480))
            await asyncio.sleep(0.02)

    async with native_stack(native_binary, peer) as (socket, _):
        client = RelayRealtimeClient(
            api_key=MODEL_KEY,
            session_id="ses_provider_feedback",
            socket_path=socket,
            local_auth_token=LOCAL_TOKEN,
        )
        model = RealtimeModel(client, protocol=protocol, model=model_name, voice=voice)
        actor = AgentSession(
            llm=model, turn_detection="realtime_llm", vad=None, stt=None, tts=None
        )
        actor.input.audio = source
        actor.on(
            "conversation_item_added",
            lambda event: (
                words.put_nowait(event.item.text_content)
                if isinstance(event.item, llm.ChatMessage)
                and event.item.role == "assistant"
                else None
            ),
        )
        producer = asyncio.create_task(microphone())
        try:
            await start_agent(actor)
            native = next(iter(model._sessions))
            generate = native.generate_reply

            def observed(**options):
                future = generate(**options)
                futures.put_nowait(future)
                return future

            monkeypatch.setattr(native, "generate_reply", observed)
            native.on("input_speech_started", lambda event: feedback.set())
            if action == "local-during":
                native.on("input_speech_started", lambda event: actor.interrupt())
            await native.start()
            ws = await asyncio.wait_for(sockets.get(), 5)
            original_speech = actor.generate_reply(
                instructions="ORIGINAL_PREOUTPUT_ASK"
            )
            original = await asyncio.wait_for(futures.get(), 5)
            timer = native._pending_timers[original]
            await asyncio.wait_for(requested.wait(), 5)
            assert original in native._pending and native._generation is None
            if action == "local-before":
                await asyncio.wait_for(actor.interrupt(), 5)
            else:
                await ws.send_json(
                    {
                        "type": "input_audio_buffer.speech_started",
                        "item_id": "caller_input",
                        "audio_start_ms": 0,
                    }
                )
                await asyncio.wait_for(feedback.wait(), 5)
            if action != "provider":
                await asyncio.wait_for(closed.wait(), 5)
                await asyncio.wait_for(asyncio.shield(native._task), 5)
                assert original.done() and (
                    original.cancelled() or original.exception() is not None
                )
                assert timer.cancelled() and not native._pending_timers
                return
            await asyncio.wait_for(original_speech.wait_for_playout(), 5)
            assert original_speech.interrupted and original.cancelled()
            assert (
                timer.cancelled() and not native._pending and not native._pending_timers
            )
            # No response ID can distinguish the interrupted application ask
            # from a new caller answer. The continuously connected source is
            # refused BEFORE its next frame is accepted; this is not advertised
            # as a healthy caller-continuation lifecycle.
            await asyncio.wait_for(closed.wait(), 5)
            await asyncio.wait_for(asyncio.shield(native._task), 5)
            assert native._failure is not None and "correlation" in str(native._failure)
            assert native._generation is None and words.empty()
            assert len([c for c in commands if c.get("type") == "response.create"]) == 1
            assert not any(
                c.get("type") == "input_audio_buffer.commit" for c in commands
            )
            assert pcm and len(pcm) % 960 == 0
        finally:
            stop.set()
            producer.cancel()
            await asyncio.gather(producer, return_exceptions=True)
            source.frames.put_nowait(None)
            await actor.aclose()
            await model.aclose()
            await client.aclose()
