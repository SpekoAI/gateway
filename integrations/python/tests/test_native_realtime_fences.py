"""Unattributed provider output/configuration never becomes a new SDK turn."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json

import pytest
from aiohttp import web
from livekit import rtc
from livekit.agents import AgentSession, llm
from test_native_realtime_boundaries import either
from test_native_realtime_cancellation import start_agent
from test_native_realtime_provider_feedback import server_answer
from test_native_sidecar import LOCAL_TOKEN, MODEL_KEY, native_stack
from test_native_sidecar import native_binary as native_binary  # noqa: PLC0414
from test_relay_speech_livekit import OwnedAudioInput

from speko_gateway import GatewayError, RelayRealtimeClient
from speko_gateway.livekit import RealtimeModel

ROUTES = [
    ("openai", "gpt-realtime-2.1", "marin"),
    ("xai", "grok-voice-3.0", "eve"),
]


def create_model(socket, protocol, name, voice):
    client = RelayRealtimeClient(
        api_key=MODEL_KEY,
        session_id="ses_output_fence",
        socket_path=socket,
        local_auth_token=LOCAL_TOKEN,
    )
    model = RealtimeModel(
        client,
        protocol="google.live.v1"
        if protocol == "google"
        else protocol + ".realtime.v1",
        model=name,
        voice=voice,
    )
    return client, model


async def ready(ws, protocol, setup):
    await ws.send_json(
        {"setupComplete": {}}
        if protocol == "google"
        else {"type": "session.updated", "session": setup["session"]}
    )


def speech_start(protocol):
    return (
        {"serverContent": {"interrupted": True}}
        if protocol == "google"
        else {"type": "input_audio_buffer.speech_started"}
    )


@pytest.mark.parametrize("protocol,name,voice", ROUTES)
@pytest.mark.parametrize("order", ["old-first", "caller-first", "terminal-first"])
async def test_unattributed_created_output_cannot_reach_playback_or_history(
    native_binary, monkeypatch, protocol, name, voice, order
):
    sockets, futures = asyncio.Queue(), asyncio.Queue()
    requested, feedback, closed, leaked = (asyncio.Event() for _ in range(4))
    commands, generations, items = [], [], []

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ready(ws, protocol, await ws.receive_json())
        sockets.put_nowait(ws)
        try:
            async for msg in ws:
                command = json.loads(msg.data)
                commands.append(command)
                if command.get("type") == "response.create":
                    requested.set()
        finally:
            closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = create_model(socket, protocol, name, voice)
        actor = AgentSession(llm=model, turn_detection="realtime_llm", vad=None)
        try:
            await start_agent(actor)
            native = next(iter(model._sessions))
            generate = native.generate_reply

            def observed(**options):
                future = generate(**options)
                futures.put_nowait(future)
                return future

            monkeypatch.setattr(native, "generate_reply", observed)
            native.on("input_speech_started", lambda _: feedback.set())
            native.on(
                "generation_created", lambda ev: (generations.append(ev), leaked.set())
            )
            actor.on("conversation_item_added", lambda ev: items.append(ev.item))
            await native.start()
            ws = await asyncio.wait_for(sockets.get(), 5)
            handle = actor.generate_reply(instructions="ORIGINAL_APPLICATION_ASK")
            original = await asyncio.wait_for(futures.get(), 5)
            timer = native._pending_timers[original]
            await asyncio.wait_for(requested.wait(), 5)
            await ws.send_json(speech_start(protocol))
            await asyncio.wait_for(feedback.wait(), 5)
            await asyncio.wait_for(handle.wait_for_playout(), 5)
            assert original.cancelled() and timer.cancelled()
            assert not native._pending and not native._pending_timers
            assert native._failure is None
            with pytest.raises(GatewayError, match="correlation"):
                actor.generate_reply(user_input="REFUSED_NEW_TYPED_CONTEXT")
            assert "REFUSED_NEW_TYPED_CONTEXT" not in str(actor.history.items)
            assert "REFUSED_NEW_TYPED_CONTEXT" not in str(native.chat_ctx.items)
            if order == "terminal-first":
                await ws.send_json(
                    {
                        "type": "response.done",
                        "response": {"id": "unseen_old", "status": "cancelled"},
                    }
                )
            identity = "unseen_caller" if order == "caller-first" else "unseen_old"
            # These are actual server IDs, not local assertions about which
            # request they belong to. Even a prior cancelled terminal cannot
            # establish a drain barrier for other unseen response IDs.
            with contextlib.suppress(ConnectionError):
                for event in [
                    {"type": "response.created", "response": {"id": identity}},
                    {
                        "type": "response.output_audio_transcript.delta",
                        "response_id": identity,
                        "item_id": "unseen_message",
                        "delta": "UNATTRIBUTED_OUTPUT_MUST_NOT_PLAY",
                    },
                    {
                        "type": "response.output_audio.delta",
                        "response_id": identity,
                        "item_id": "unseen_message",
                        "delta": base64.b64encode(b"\x04\x10" * 2400).decode(),
                    },
                    {
                        "type": "response.done",
                        "response": {"id": identity, "status": "cancelled"},
                    },
                ]:
                    await ws.send_json(event)
            await either(closed, leaked)
            assert closed.is_set() and not leaked.is_set(), (
                "unattributed response opened SDK playback"
            )
            await asyncio.wait_for(asyncio.shield(native._task), 5)
            assert not generations and native._generation is None
            assert "UNATTRIBUTED_OUTPUT_MUST_NOT_PLAY" not in str(items)
            assert "UNATTRIBUTED_OUTPUT_MUST_NOT_PLAY" not in str(actor.history.items)
            assert "UNATTRIBUTED_OUTPUT_MUST_NOT_PLAY" not in str(native.chat_ctx.items)
            assert len([c for c in commands if c.get("type") == "response.create"]) == 1
        finally:
            await actor.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("protocol,name,voice", ROUTES)
async def test_known_response_vad_feedback_keeps_connected_caller_flow(
    native_binary, protocol, name, voice
):
    source, sockets, words = OwnedAudioInput(), asyncio.Queue(), asyncio.Queue()
    feedback, stop, closed = (asyncio.Event() for _ in range(3))
    frames = []

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ready(ws, protocol, await ws.receive_json())
        sockets.put_nowait(ws)
        try:
            async for msg in ws:
                command = json.loads(msg.data)
                if command.get("type") == "input_audio_buffer.append":
                    frames.append(base64.b64decode(command["audio"]))
        finally:
            closed.set()
        return ws

    async def microphone():
        while not stop.is_set():
            source.frames.put_nowait(rtc.AudioFrame(b"\x03\x12" * 480, 24000, 1, 480))
            await asyncio.sleep(0.02)

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = create_model(socket, protocol, name, voice)
        actor = AgentSession(llm=model, turn_detection="realtime_llm", vad=None)
        actor.input.audio = source
        actor.on(
            "conversation_item_added",
            lambda ev: (
                words.put_nowait(ev.item.text_content)
                if isinstance(ev.item, llm.ChatMessage)
                else None
            ),
        )
        producer = asyncio.create_task(microphone())
        try:
            await start_agent(actor)
            native = next(iter(model._sessions))
            native.on("input_speech_started", lambda _: feedback.set())
            await native.start()
            ws = await asyncio.wait_for(sockets.get(), 5)
            created = asyncio.Event()
            native.on("generation_created", lambda _: created.set())
            await ws.send_json(
                {"type": "response.created", "response": {"id": "known_original"}}
            )
            await asyncio.wait_for(created.wait(), 5)
            await ws.send_json(speech_start(protocol))
            await asyncio.wait_for(feedback.wait(), 5)
            assert (
                not native._unattributed_application_reply and native._failure is None
            )
            await ws.send_json(
                {
                    "type": "response.output_audio_transcript.delta",
                    "response_id": "known_original",
                    "item_id": "known_msg",
                    "delta": "LATE_KNOWN_MUST_NOT_RETAIN",
                }
            )
            await ws.send_json(
                {
                    "type": "response.output_audio.delta",
                    "response_id": "known_original",
                    "item_id": "known_msg",
                    "delta": base64.b64encode(b"\x05\x10" * 2400).decode(),
                }
            )
            await ws.send_json(
                {
                    "type": "response.done",
                    "response": {"id": "known_original", "status": "cancelled"},
                }
            )
            await server_answer(ws, "next_known", "REAL_NEXT_CALLER_ANSWER")

            async def next_answer():
                while await words.get() != "REAL_NEXT_CALLER_ANSWER":
                    pass

            await asyncio.wait_for(next_answer(), 5)
            assert native._failure is None and not closed.is_set()
            assert "LATE_KNOWN_MUST_NOT_RETAIN" not in str(native.chat_ctx.items)
            assert "LATE_KNOWN_MUST_NOT_RETAIN" not in str(actor.history.items)
            assert frames and all(frame == b"\x03\x12" * 480 for frame in frames)
        finally:
            stop.set()
            producer.cancel()
            await asyncio.gather(producer, return_exceptions=True)
            source.frames.put_nowait(None)
            await actor.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("protocol,name,voice", ROUTES)
@pytest.mark.parametrize("change", ["instructions", "tools", "tool-choice", "input"])
async def test_unattributed_context_refuses_before_local_and_physical_mutation(
    native_binary, protocol, name, voice, change
):
    sockets = asyncio.Queue()
    requested, feedback, barrier, closed = (asyncio.Event() for _ in range(4))
    commands = []

    @llm.function_tool
    async def replacement_tool(value: str) -> str:
        """Return a value."""
        return value

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ready(ws, protocol, await ws.receive_json())
        sockets.put_nowait(ws)
        try:
            async for msg in ws:
                command = json.loads(msg.data)
                commands.append(command)
                if command.get("type") == "response.create":
                    requested.set()
                if command.get("type") == "input_audio_buffer.clear":
                    barrier.set()
        finally:
            closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = create_model(socket, protocol, name, voice)
        native = model.session()
        native.on("input_speech_started", lambda _: feedback.set())
        try:
            await native._update_session(instructions="ORIGINAL_SYSTEM", tools=[])
            await native.start()
            ws = await asyncio.wait_for(sockets.get(), 5)
            original = native.generate_reply(instructions="original ask")
            await asyncio.wait_for(requested.wait(), 5)
            await ws.send_json(speech_start(protocol))
            await asyncio.wait_for(feedback.wait(), 5)
            assert original.cancelled()
            previous_choice = native._tool_choice
            with pytest.raises(GatewayError, match="correlation"):
                if change == "instructions":
                    await native.update_instructions("NEW_SYSTEM_MUST_NOT_INSTALL")
                elif change == "tools":
                    await native.update_tools([replacement_tool])
                elif change == "tool-choice":
                    native.update_options(tool_choice="required")
                else:
                    native.push_audio(rtc.AudioFrame(b"\x03\x12" * 480, 48000, 1, 480))
            assert native._instructions == "ORIGINAL_SYSTEM"
            assert (
                native.tools.flatten() == [] and native._tool_choice == previous_choice
            )
            if change == "input":
                await asyncio.wait_for(closed.wait(), 5)
                await asyncio.wait_for(asyncio.shield(native._task), 5)
                assert native._resampler is None
            else:
                # A restriction-only real provider clear is an ordered outbound
                # barrier, not another physical model request or a quiet timer.
                native.clear_audio()
                await asyncio.wait_for(barrier.wait(), 5)
                assert native._failure is None
            assert not any(
                c.get("type") in {"session.update", "input_audio_buffer.append"}
                for c in commands
            )
        finally:
            await native.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize(
    "protocol,name,voice", [*ROUTES, ("google", "gemini-3.8-live", "Kore")]
)
@pytest.mark.parametrize("attached", [False, True])
async def test_independent_stop_inside_active_feedback_physically_joins(
    native_binary, monkeypatch, protocol, name, voice, attached
):
    sockets = asyncio.Queue()
    requested, feedback, closed = (asyncio.Event() for _ in range(3))

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ready(ws, protocol, await ws.receive_json())
        sockets.put_nowait(ws)
        try:
            async for msg in ws:
                command = json.loads(msg.data)
                if (
                    "clientContent" in command
                    or command.get("type") == "response.create"
                ):
                    requested.set()
        finally:
            closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = create_model(socket, protocol, name, voice)
        actor = (
            AgentSession(llm=model, turn_detection="realtime_llm", vad=None)
            if attached
            else None
        )
        try:
            if actor:
                await start_agent(actor)
                native = next(iter(model._sessions))
            else:
                native = model.session()
                await native._update_session(tools=[])
            await native.start()
            ws = await asyncio.wait_for(sockets.get(), 5)

            def local_stop(_):
                (actor.interrupt if actor else native.interrupt)()
                feedback.set()

            native.on("input_speech_started", local_stop)
            if actor:
                futures = asyncio.Queue()
                generate = native.generate_reply

                def observed(**options):
                    future = generate(**options)
                    futures.put_nowait(future)
                    return future

                monkeypatch.setattr(native, "generate_reply", observed)
                actor.generate_reply(instructions="original ask")
                reply = await asyncio.wait_for(futures.get(), 5)
            else:
                reply = native.generate_reply(instructions="original ask")
            await asyncio.wait_for(requested.wait(), 5)
            await ws.send_json(
                {"serverContent": {"outputTranscription": {"text": "ACTIVE_ORIGINAL"}}}
                if protocol == "google"
                else {"type": "response.created", "response": {"id": "known_active"}}
            )
            await asyncio.wait_for(reply, 5)
            assert native._generation is not None
            await ws.send_json(speech_start(protocol))
            await asyncio.wait_for(feedback.wait(), 5)
            await asyncio.wait_for(closed.wait(), 1)
            await asyncio.wait_for(asyncio.shield(native._task), 5)
            assert native._failure is not None and not native._pending_timers
        finally:
            if actor:
                await actor.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("protocol,name,voice", ROUTES)
@pytest.mark.parametrize("change", ["instructions", "tools", "context", "tool-choice"])
async def test_sdk_configuration_refuses_before_agent_state_mutation(
    native_binary, protocol, name, voice, change
):
    sockets = asyncio.Queue()
    requested, feedback, barrier = (asyncio.Event() for _ in range(3))
    commands = []

    @llm.function_tool
    async def replacement_tool(value: str) -> str:
        """Return a value."""
        return value

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
            if command.get("type") == "input_audio_buffer.clear":
                barrier.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = create_model(socket, protocol, name, voice)
        actor = AgentSession(llm=model, turn_detection="realtime_llm", vad=None)
        try:
            agent = await start_agent(actor)
            native = next(iter(model._sessions))
            native.on("input_speech_started", lambda _: feedback.set())
            await native.start()
            ws = await asyncio.wait_for(sockets.get(), 5)
            handle = actor.generate_reply(instructions="original ask")
            await asyncio.wait_for(requested.wait(), 5)
            await ws.send_json(speech_start(protocol))
            await asyncio.wait_for(feedback.wait(), 5)
            await asyncio.wait_for(handle.wait_for_playout(), 5)
            original_choice = agent._activity._tool_choice
            with pytest.raises(GatewayError, match="correlation"):
                if change == "instructions":
                    await agent.update_instructions("REJECTED_SDK_INSTRUCTIONS")
                elif change == "tools":
                    await agent.update_tools([replacement_tool])
                elif change == "context":
                    context = agent.chat_ctx.copy()
                    context.add_message(role="user", content="REJECTED_SDK_CONTEXT")
                    await agent.update_chat_ctx(context)
                else:
                    agent._activity.update_options(tool_choice="required")
            assert agent.instructions == "Synthetic task" and not agent.tools
            assert agent._activity._tool_choice == original_choice
            assert (
                native._instructions == "Synthetic task" and not native.tools.flatten()
            )
            assert "REJECTED_SDK" not in str(agent.chat_ctx.items)
            assert "REJECTED_SDK" not in str(native.chat_ctx.items)
            native.clear_audio()
            await asyncio.wait_for(barrier.wait(), 5)
            assert not any(c.get("type") == "session.update" for c in commands)
            assert "REJECTED_SDK" not in json.dumps(commands)
            assert native._failure is None
        finally:
            await actor.aclose()
            await model.aclose()
            await client.aclose()
