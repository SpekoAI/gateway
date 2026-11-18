"""Stage SDK context under its real lock before native and Agent mutation."""

from __future__ import annotations

import asyncio
import base64
import json

import pytest
from aiohttp import web
from livekit import rtc
from livekit.agents import AgentSession, llm
from test_native_realtime_boundaries import assert_resampled
from test_native_realtime_cancellation import client_model, start_agent
from test_native_realtime_commit import MANUAL
from test_native_realtime_context import google_output
from test_native_realtime_fences import create_model, ready, speech_start
from test_native_realtime_provider_feedback import server_answer
from test_native_sidecar import native_binary as native_binary  # noqa: PLC0414
from test_native_sidecar import native_stack
from test_relay_speech_livekit import OwnedAudioInput

from speko_gateway import GatewayError

ROUTES = [
    ("google", "gemini-3.8-live", "Kore"),
    ("openai", "gpt-realtime-2.1", "marin"),
    ("xai", "grok-voice-3.0", "eve"),
]


@pytest.mark.parametrize("protocol,name,voice", ROUTES)
async def test_first_typed_known_responses_then_connected_ordinary_flow(
    native_binary, protocol, name, voice
):
    """No caller input exists before typed work; real terminals retire it."""
    source = OwnedAudioInput()
    words = asyncio.Queue()
    commands, pcm = [], bytearray()
    responses = 0

    async def peer(request):
        nonlocal responses
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ready(ws, protocol, await ws.receive_json())

        async def answer(text):
            nonlocal responses
            responses += 1
            if protocol == "google":
                await google_output(ws, text)
            else:
                await server_answer(ws, "actual_response_" + str(responses), text)

        async for msg in ws:
            command = json.loads(msg.data)
            commands.append(command)
            control = command.get("realtimeInput", {})
            audio = control.get("audio")
            if audio or command.get("type") == "input_audio_buffer.append":
                data = base64.b64decode(audio["data"] if audio else command["audio"])
                pcm.extend(data)
                await answer("ORDINARY_CALLER_ANSWER_" + str(len(pcm) // 960))
            if (
                command.get("clientContent", {}).get("turnComplete")
                or command.get("type") == "response.create"
            ):
                await answer("SAFE_TYPED_ANSWER_" + str(responses + 1))
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = create_model(socket, protocol, name, voice)
        actor = AgentSession(llm=model, turn_detection="realtime_llm", vad=None)
        actor.on(
            "conversation_item_added",
            lambda e: (
                words.put_nowait(e.item.text_content)
                if isinstance(e.item, llm.ChatMessage) and e.item.role == "assistant"
                else None
            ),
        )
        try:
            agent = await start_agent(actor)
            native = agent.realtime_llm_session
            for index in (1, 2):
                speech = actor.generate_reply(
                    user_input="SAFE_TYPED_USER_" + str(index)
                )
                await asyncio.wait_for(speech.wait_for_playout(), 5)
                assert speech.exception() is None
                assert await asyncio.wait_for(
                    words.get(), 5
                ) == "SAFE_TYPED_ANSWER_" + str(index)
                if hasattr(actor, "wait_for_idle"):
                    await asyncio.wait_for(actor.wait_for_idle(), 5)
            actor.input.audio = source
            rate = 16000 if protocol == "google" else 24000
            # Actual AgentSession source keeps forwarding, without a synthetic
            # turn frontier or pausing between frames for a provider answer.
            for _ in range(5):
                source.frames.put_nowait(
                    rtc.AudioFrame(b"\x02\x10" * 480, rate, 1, 480)
                )
            for index in range(1, 6):
                assert await asyncio.wait_for(
                    words.get(), 5
                ) == "ORDINARY_CALLER_ANSWER_" + str(index)
            assert pcm == b"\x02\x10" * 2400
            assert not native._pending and not native._pending_timers
            assert native._failure is None
            assert all(
                "SAFE_TYPED_USER_" + str(i) in json.dumps(commands) for i in (1, 2)
            )
            with pytest.raises(GatewayError, match="correlation"):
                actor.generate_reply(user_input="UNATTRIBUTABLE_AFTER_CALLER")
            assert "UNATTRIBUTABLE_AFTER_CALLER" not in str(agent.chat_ctx.items)
        finally:
            source.frames.put_nowait(None)
            await actor.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.skipif(not MANUAL, reason="SDK cannot negotiate manual turns")
@pytest.mark.parametrize("committed", [False, True])
@pytest.mark.parametrize("rate", [16000, 48000])
@pytest.mark.parametrize("busy", [False, True])
async def test_manual_context_preflight_preserves_original_capture_and_future(
    native_binary, committed, rate, busy, monkeypatch
):
    commands, pcm, requests = [], bytearray(), []
    sockets, futures, words = asyncio.Queue(), asyncio.Queue(), asyncio.Queue()
    ended = asyncio.Event()
    marker = "REFUSED_CONTEXT_CANNOT_REPLAY"

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        sockets.put_nowait(ws)
        async for msg in ws:
            command = json.loads(msg.data)
            commands.append(command)
            control = command.get("realtimeInput", {})
            if "audio" in control:
                pcm.extend(base64.b64decode(control["audio"]["data"]))
            if "activityEnd" in control:
                ended.set()
            if command.get("clientContent", {}).get("turnComplete"):
                requests.append(command)
                await google_output(ws, "SAFE_NEXT_TYPED_ANSWER")
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        actor = AgentSession(
            llm=model, turn_detection="manual", vad=None, stt=None, tts=None
        )
        actor.on(
            "conversation_item_added",
            lambda e: (
                words.put_nowait(e.item.text_content)
                if isinstance(e.item, llm.ChatMessage) and e.item.role == "assistant"
                else None
            ),
        )
        try:
            agent = await start_agent(actor)
            native = agent.realtime_llm_session
            await native.start()
            lock = getattr(agent._activity, "_rt_chat_ctx_lock", None)
            if busy and not isinstance(lock, asyncio.Lock):
                pytest.skip("SDK version has no realtime context lock")
            ws = await asyncio.wait_for(sockets.get(), 5)
            captured = b"\x21\x10" * 480
            native.push_audio(rtc.AudioFrame(captured, rate, 1, 480))
            original_generate = native.generate_reply

            def observed(**options):
                future = original_generate(**options)
                futures.put_nowait(future)
                return future

            monkeypatch.setattr(native, "generate_reply", observed)
            if committed:
                actor.commit_user_turn(skip_reply=True)
                await asyncio.wait_for(ended.wait(), 5)
                handle = actor.generate_reply()
                original = await asyncio.wait_for(futures.get(), 5)
                timer = native._pending_timers[original]
            before_count = native._uncommitted_source_samples
            before_native = str(native.chat_ctx.items)
            before_agent = str(agent.chat_ctx.items)
            staged = agent.chat_ctx.copy()
            staged.add_message(role="user", content=marker)
            if busy:
                async with lock:
                    with pytest.raises(GatewayError):
                        await agent.update_chat_ctx(staged)
            else:
                with pytest.raises(GatewayError):
                    await agent.update_chat_ctx(staged)
            assert str(agent.chat_ctx.items) == before_agent
            assert str(native.chat_ctx.items) == before_native
            assert native._uncommitted_source_samples == before_count
            if not committed:
                actor.commit_user_turn(skip_reply=True)
                await asyncio.wait_for(ended.wait(), 5)
                handle = actor.generate_reply()
                original = await asyncio.wait_for(futures.get(), 5)
                timer = native._pending_timers[original]
            assert native._pending[0] is original and not original.done()
            assert not timer.cancelled()
            await google_output(ws, "ORIGINAL_AUDIO_ANSWER")
            await asyncio.wait_for(handle.wait_for_playout(), 5)
            assert handle.exception() is None and original.result() is not None
            assert timer.cancelled() and not native._pending_timers
            assert await asyncio.wait_for(words.get(), 5) == "ORIGINAL_AUDIO_ANSWER"
            if rate == 48000:
                assert_resampled(pcm, captured)
            else:
                assert pcm == captured
            if hasattr(actor, "wait_for_idle"):
                await asyncio.wait_for(actor.wait_for_idle(), 5)
            await agent.update_chat_ctx(agent.chat_ctx.copy())
            next_handle = actor.generate_reply(user_input="SAFE_NEXT_TYPED_USER")
            await asyncio.wait_for(next_handle.wait_for_playout(), 5)
            assert next_handle.exception() is None
            assert await asyncio.wait_for(words.get(), 5) == "SAFE_NEXT_TYPED_ANSWER"
            assert len(requests) == 1
            assert "SAFE_NEXT_TYPED_USER" in json.dumps(commands)
            assert marker not in json.dumps(commands)
            assert all(
                marker not in str(ctx.items)
                for ctx in (agent.chat_ctx, actor.history, native.chat_ctx)
            )
            assert native._failure is None
        finally:
            await actor.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("protocol,name,voice", ROUTES)
@pytest.mark.parametrize("evidence", ["pcm", "transcript"])
async def test_provider_input_before_first_output_cannot_claim_later_typed_task(
    native_binary, protocol, name, voice, evidence
):
    sockets, words = asyncio.Queue(), asyncio.Queue()
    transcript = asyncio.Event()
    commands, pcm = [], bytearray()
    marker = "REFUSED_TYPED_CANNOT_CLAIM_OLD_AUDIO"

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ready(ws, protocol, await ws.receive_json())
        sockets.put_nowait(ws)
        async for msg in ws:
            command = json.loads(msg.data)
            commands.append(command)
            audio = command.get("realtimeInput", {}).get("audio")
            if audio:
                pcm.extend(base64.b64decode(audio["data"]))
            if command.get("type") == "input_audio_buffer.append":
                pcm.extend(base64.b64decode(command["audio"]))
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = create_model(socket, protocol, name, voice)
        actor = AgentSession(
            llm=model, turn_detection="realtime_llm", vad=None, stt=None, tts=None
        )
        actor.on(
            "conversation_item_added",
            lambda e: (
                words.put_nowait(e.item.text_content)
                if isinstance(e.item, llm.ChatMessage) and e.item.role == "assistant"
                else None
            ),
        )
        try:
            agent = await start_agent(actor)
            native = agent.realtime_llm_session
            native.on("input_audio_transcription_completed", lambda _: transcript.set())
            await native.start()
            ws = await asyncio.wait_for(sockets.get(), 5)
            hz = 16000 if protocol == "google" else 24000
            if evidence == "pcm":
                native.push_audio(rtc.AudioFrame(b"\x02\x10" * 480, hz, 1, 480))
            else:
                await ws.send_json(
                    {
                        "serverContent": {
                            "inputTranscription": {"text": "OLD_CALLER_INPUT"}
                        }
                    }
                    if protocol == "google"
                    else {
                        "type": "conversation.item.input_audio_transcription.completed",
                        "item_id": "old_caller",
                        "transcript": "OLD_CALLER_INPUT",
                    }
                )
                await asyncio.wait_for(transcript.wait(), 5)
            with pytest.raises(GatewayError, match="correlation"):
                actor.generate_reply(user_input=marker)
            assert not native._pending and not native._pending_timers
            assert all(
                marker not in str(c.items)
                for c in (agent.chat_ctx, actor.history, native.chat_ctx)
            )
            if protocol == "google":
                await google_output(ws, "OLD_AUDIO_ANSWER")
            else:
                await server_answer(ws, "old_audio", "OLD_AUDIO_ANSWER")
            assert await asyncio.wait_for(words.get(), 5) == "OLD_AUDIO_ANSWER"
            if hasattr(actor, "wait_for_idle"):
                await asyncio.wait_for(actor.wait_for_idle(), 5)
            # A terminal does not prove the absence of another unseen response.
            with pytest.raises(GatewayError, match="correlation"):
                actor.generate_reply(user_input=marker)
            native.push_audio(rtc.AudioFrame(b"\x02\x10" * 480, hz, 1, 480))
            if protocol == "google":
                await google_output(ws, "NEXT_REAL_CALLER_ANSWER")
            else:
                await server_answer(ws, "next_caller", "NEXT_REAL_CALLER_ANSWER")
            assert await asyncio.wait_for(words.get(), 5) == "NEXT_REAL_CALLER_ANSWER"
            # Real outbound stream-end is an ordering barrier, not completion
            # or an acknowledgement of a response identity.
            native.clear_audio()

            async def flushed():
                while len(pcm) < (1920 if evidence == "pcm" else 960):
                    await asyncio.sleep(0)

            await asyncio.wait_for(flushed(), 5)
            assert pcm == b"\x02\x10" * (960 if evidence == "pcm" else 480)
            assert marker not in json.dumps(commands)
            assert native._failure is None
        finally:
            await actor.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("protocol,name,voice", ROUTES)
async def test_input_overlapping_first_typed_pending_is_stopped_before_capture(
    native_binary, monkeypatch, protocol, name, voice
):
    sockets, futures = asyncio.Queue(), asyncio.Queue()
    requested, closed = asyncio.Event(), asyncio.Event()
    commands, errors = [], []

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ready(ws, protocol, await ws.receive_json())
        sockets.put_nowait(ws)
        try:
            async for msg in ws:
                command = json.loads(msg.data)
                commands.append(command)
                if (
                    command.get("clientContent", {}).get("turnComplete")
                    or command.get("type") == "response.create"
                ):
                    requested.set()
        finally:
            closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = create_model(socket, protocol, name, voice)
        actor = AgentSession(llm=model, turn_detection="realtime_llm", vad=None)
        actor.on("error", errors.append)
        try:
            agent = await start_agent(actor)
            native = agent.realtime_llm_session
            original = native.generate_reply

            def observed(**options):
                future = original(**options)
                futures.put_nowait(future)
                return future

            monkeypatch.setattr(native, "generate_reply", observed)
            handle = actor.generate_reply(user_input="SAFE_FIRST_TYPED_UNTIL_OVERLAP")
            future = await asyncio.wait_for(futures.get(), 5)
            timer = native._pending_timers[future]
            await asyncio.wait_for(requested.wait(), 5)
            with pytest.raises(GatewayError, match="overlap"):
                native.push_audio(rtc.AudioFrame(b"\x12\x10" * 480, 48000, 1, 480))
            await asyncio.wait_for(closed.wait(), 5)
            await asyncio.wait_for(asyncio.shield(native._task), 5)
            await asyncio.wait_for(handle.wait_for_playout(), 5)
            assert handle.exception() is not None or handle.interrupted
            assert errors and not errors[0].error.recoverable
            assert future.done() and (
                future.cancelled() or future.exception() is not None
            )
            assert timer.cancelled() and not native._pending_timers
            assert native._uncommitted_source_samples == 0 and native._resampler is None
            assert not any(
                "audio" in c.get("realtimeInput", {})
                or c.get("type") == "input_audio_buffer.append"
                for c in commands
            )
        finally:
            await actor.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("protocol,name,voice", ROUTES[1:])
async def test_busy_sdk_lock_never_assigns_context_before_feedback_check(
    native_binary, protocol, name, voice
):
    sockets = asyncio.Queue()
    requested, feedback = asyncio.Event(), asyncio.Event()
    commands = []

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ready(ws, protocol, await ws.receive_json())
        sockets.put_nowait(ws)
        async for msg in ws:
            command = json.loads(msg.data)
            commands.append(command)
            if command.get("type") == "response.create":
                requested.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = create_model(socket, protocol, name, voice)
        actor = AgentSession(llm=model, turn_detection="realtime_llm", vad=None)
        try:
            agent = await start_agent(actor)
            lock = getattr(agent._activity, "_rt_chat_ctx_lock", None)
            if not isinstance(lock, asyncio.Lock):
                pytest.skip("SDK version has no realtime context lock")
            native = agent.realtime_llm_session
            native.on("input_speech_started", lambda _: feedback.set())
            await native.start()
            ws = await asyncio.wait_for(sockets.get(), 5)
            handle = actor.generate_reply(instructions="ORIGINAL_PREOUTPUT_TASK")
            await asyncio.wait_for(requested.wait(), 5)
            staged = agent.chat_ctx.copy()
            staged.add_message(role="user", content="LOCKED_CONTEXT_MUST_NOT_LEAK")
            async with lock:
                update = asyncio.create_task(agent.update_chat_ctx(staged))
                scheduled = asyncio.Event()
                asyncio.get_running_loop().call_soon(scheduled.set)
                await scheduled.wait()
                assert "LOCKED_CONTEXT" not in str(agent.chat_ctx.items)
                await ws.send_json(speech_start(protocol))
                await asyncio.wait_for(feedback.wait(), 5)
            with pytest.raises(GatewayError, match="correlation"):
                await update
            await asyncio.wait_for(handle.wait_for_playout(), 5)
            assert all(
                "LOCKED_CONTEXT" not in str(c.items)
                for c in (agent.chat_ctx, actor.history, native.chat_ctx)
            )
            assert "LOCKED_CONTEXT" not in json.dumps(commands)
        finally:
            await actor.aclose()
            await model.aclose()
            await client.aclose()
