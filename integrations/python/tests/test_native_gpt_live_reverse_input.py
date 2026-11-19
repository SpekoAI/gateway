"""An unsettled application future cannot be completed by subsequent input."""

from __future__ import annotations

import asyncio
import base64
import json

import pytest
from aiohttp import web
from livekit import rtc
from livekit.agents import AgentSession, llm
from test_native_gpt_live import models, response
from test_native_gpt_live_typed_fence import voice_output
from test_native_realtime_cancellation import start_agent
from test_native_sidecar import native_binary as native_binary  # noqa: PLC0414
from test_native_sidecar import native_stack
from test_relay_speech_livekit import OwnedAudioInput

pytestmark = pytest.mark.skipif(
    not hasattr(llm, "DuplexModel"), reason="SDK predates DuplexModel"
)


@pytest.mark.parametrize("claimed", [False, True])
@pytest.mark.parametrize("source_kind", ["pcm", "resampler", "transcript"])
async def test_input_fences_original_pending_typed_future_before_capture(
    native_binary, claimed, source_kind
):
    sockets, futures = asyncio.Queue(), asyncio.Queue()
    asked, backend_started, input_seen, closed = (asyncio.Event() for _ in range(4))
    release_backend = asyncio.Event()
    commands, requests = [], []

    async def peer(request):
        if request.path == "/v1/llm/responses":
            requests.append(await request.json())
            backend_started.set()
            await release_backend.wait()
            return response(
                [
                    ("response.text.delta", {"delta": "ORIGINAL_TYPED_ANSWER"}),
                    ("response.completed", {}),
                ]
            )
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"type": "session.started"})
        sockets.put_nowait(ws)
        try:
            async for frame in ws:
                command = json.loads(frame.data)
                commands.append(command)
                if command["type"] == "session.commentary.append":
                    asked.set()
                elif command["type"] == "session.close":
                    await ws.send_json({"type": "session.closed"})
                    await ws.close()
        finally:
            closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        actor = AgentSession(llm=model, turn_detection="realtime_llm", vad=None)
        source = OwnedAudioInput()
        try:
            agent = await start_agent(actor)
            native = agent.realtime_llm_session
            await native.start()
            ws = await asyncio.wait_for(sockets.get(), 5)
            generate = native.generate_reply

            def observe_reply(**options):
                future = generate(**options)
                futures.put_nowait(future)
                return future

            native.generate_reply = observe_reply
            speech = actor.generate_reply(user_input="ORIGINAL_TYPED_REQUEST")
            original = await asyncio.wait_for(futures.get(), 5)
            await asyncio.wait_for(asked.wait(), 5)
            assert not original.done()
            if claimed:
                await ws.send_json(
                    {
                        "type": "session.delegation.created",
                        "offset_ms": 0,
                        "delegation": {"id": "original_typed", "target": "client"},
                    }
                )
                await asyncio.wait_for(backend_started.wait(), 5)
                assert "ORIGINAL_TYPED_REQUEST" in json.dumps(requests[0]["input"])
                assert not original.done()
            if source_kind == "transcript":
                event = native.duplex_session._event

                async def observe_event(value):
                    try:
                        await event(value)
                    finally:
                        if value.get("type") == "session.input_transcript.delta":
                            input_seen.set()

                native.duplex_session._event = observe_event
                await ws.send_json(
                    {
                        "type": "session.input_transcript.delta",
                        "delta": "NEW_CALLER",
                        "start_ms": 0,
                        "end_ms": 100,
                    }
                )
            else:
                push_audio = native.push_audio

                def observe_frame(frame):
                    try:
                        push_audio(frame)
                    finally:
                        input_seen.set()

                native.push_audio = observe_frame
                actor.input.audio = source
                source.frames.put_nowait(
                    rtc.AudioFrame(
                        b"\x02\x10" * 480,
                        48000 if source_kind == "resampler" else 24000,
                        1,
                        480,
                    )
                )
            await asyncio.wait_for(input_seen.wait(), 5)
            assert original.done(), (
                "input discarded the hint but left its real application future claimable"
            )
            assert original.cancelled() or original.exception() is not None
            await asyncio.wait_for(speech.wait_for_playout(), 5)
            await asyncio.wait_for(closed.wait(), 5)
            assert native.duplex_session._failure is not None
            assert not any(c["type"] == "session.input_audio.append" for c in commands)
            assert len(requests) == int(claimed)
        finally:
            release_backend.set()
            source.frames.put_nowait(None)
            await actor.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()


async def test_completed_first_typed_reply_keeps_ordinary_caller_audio(native_binary):
    sockets = asyncio.Queue()
    audio_seen = asyncio.Event()
    pcm = []

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"type": "session.started"})
        sockets.put_nowait(ws)
        async for frame in ws:
            command = json.loads(frame.data)
            if command["type"] == "session.commentary.append":
                await voice_output(ws, "FIRST_TYPED_ANSWER")
            elif command["type"] == "session.input_audio.append":
                pcm.append(base64.b64decode(command["audio"]))
                audio_seen.set()
            elif command["type"] == "session.close":
                await ws.send_json({"type": "session.closed"})
                await ws.close()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        actor = AgentSession(llm=model, turn_detection="realtime_llm", vad=None)
        source = OwnedAudioInput()
        try:
            await start_agent(actor)
            native = actor.current_agent.realtime_llm_session
            await native.start()
            await asyncio.wait_for(sockets.get(), 5)
            speech = actor.generate_reply(user_input="FIRST_TYPED_REQUEST")
            await asyncio.wait_for(speech.wait_for_playout(), 5)
            assert speech.exception() is None
            actor.input.audio = source
            source.frames.put_nowait(rtc.AudioFrame(b"\x02\x10" * 480, 24000, 1, 480))
            await asyncio.wait_for(audio_seen.wait(), 5)
            assert pcm == [b"\x02\x10" * 480]
            assert native.duplex_session._failure is None
        finally:
            source.frames.put_nowait(None)
            await actor.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()
