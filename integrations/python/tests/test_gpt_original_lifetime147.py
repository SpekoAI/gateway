"""Targeted actual SDK original-context and remaining setup lifetime controls."""

from __future__ import annotations

import asyncio
import json

import pytest
from aiohttp import web
from livekit.agents import AgentSession, llm
from test_gemini_input_review147 import until
from test_gpt_isolated_ask_proposal147 import CapturedOutput, send_audio
from test_native_gpt_live import models, response
from test_native_sidecar import LOCAL_TOKEN, MODEL_KEY, native_stack
from test_native_sidecar import native_binary as native_binary  # noqa: PLC0414
from test_realtime_review147 import start

from speko_gateway import RelayRealtimeClient
from speko_gateway.livekit import GPTLiveModel

pytestmark = pytest.mark.skipif(
    not hasattr(llm, "DuplexModel"), reason="SDK lacks its public duplex interface"
)


async def test_original_ask_context_survives_actual_sdk_lock_and_caller_mutation(
    native_binary,
):
    requests = []

    async def peer(request):
        if request.path == "/v1/llm/responses":
            body = await request.json()
            requests.append(body)
            return response(
                [
                    ("response.text.delta", {"delta": "Original detached answer"}),
                    ("response.completed", {}),
                ]
            )
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        startup = await ws.receive_json()
        await ws.send_json(
            {"type": "session.started", "session": {"id": "actual-native"}}
        )
        if "Immutable original ask" in startup["session"]["instructions"]:
            await ws.send_json(
                {
                    "type": "session.delegation.created",
                    "offset_ms": 0,
                    "delegation": {"id": "actual-delegation", "target": "client"},
                }
            )
        async for message in ws:
            command = json.loads(message.data)
            if command.get("delegation_id") == "actual-delegation":
                await send_audio(ws, b"\x01\x10" * 2400)
            if command.get("type") == "session.close":
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        session = AgentSession(llm=model, stt=None, tts=None, vad=None)
        session.output.audio = CapturedOutput()
        context = llm.ChatContext.empty()
        context.add_message(role="user", content="Original retained value")
        lock = None
        try:
            await start(session, chat_ctx=context)
            lock = session._activity._rt_chat_ctx_lock
            await lock.acquire()
            handle = session.generate_reply(
                user_input="Original typed input", instructions="Immutable original ask"
            )
            actual = next(
                item
                for item in session.current_agent.chat_ctx.items
                if isinstance(item, llm.ChatMessage) and item.role == "user"
            )
            actual.content[0] = "Mutation after original invocation"
            lock.release()
            await asyncio.wait_for(handle.wait_for_playout(), 5)
            await until(lambda: len(requests) == 1)
            wire = json.dumps(requests[0]["input"])
            assert "Original retained value" in wire and "Original typed input" in wire
            assert (
                "Immutable original ask" in wire
                and "Mutation after original invocation" not in wire
            )
            assert handle.exception() is None
        finally:
            if lock is not None and lock.locked():
                lock.release()
            await session.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()


async def test_sdk_queue_wait_consumes_original_setup_without_late_admission(
    native_binary,
):
    startups, closed = [], []

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        startup = await ws.receive_json()
        startups.append(startup)
        await ws.send_json(
            {"type": "session.started", "session": {"id": "actual-native"}}
        )
        async for message in ws:
            command = json.loads(message.data)
            if command.get("type") == "session.commentary.append":
                await send_audio(ws, b"\x02\x20" * 2400)
            if command.get("type") == "session.close":
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        closed.append(startup)
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        unused_voice, backend_client, backend, unused_model = models(socket)
        await unused_model.aclose()
        await unused_voice.aclose()
        voice = RelayRealtimeClient(
            api_key=MODEL_KEY,
            session_id="ses_native_duplex",
            socket_path=socket,
            local_auth_token=LOCAL_TOKEN,
            setup_timeout=0.5,
        )
        model = GPTLiveModel(voice, model="gpt-live-1", voice="marin", backend=backend)
        session = AgentSession(llm=model, stt=None, tts=None, vad=None)
        session.output.audio = CapturedOutput()
        lock = None
        try:
            await start(session)
            await until(lambda: len(startups) == 1)
            proxy = next(iter(model._owned_sessions))
            lock = session._activity._rt_chat_ctx_lock
            await lock.acquire()
            expired = session.generate_reply(
                user_input="Original queued input", instructions="Expired original ask"
            )
            original = proxy.model.root.owners[0]
            await until(lambda: original.closed)
            assert original.client is None and len(startups) == 1
            lock.release()
            await asyncio.wait_for(expired.wait_for_playout(), 5)
            assert expired.exception() is not None
            later = session.generate_reply(instructions="Fresh genuine C")
            await asyncio.wait_for(later.wait_for_playout(), 5)
            assert later.exception() is None
            assert len(startups) == 2 and all(
                "Expired original ask" not in event["session"]["instructions"]
                for event in startups
            )
            assert "Fresh genuine C" in startups[-1]["session"]["instructions"]
            assert not closed
        finally:
            if lock is not None and lock.locked():
                lock.release()
            await session.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()


async def test_new_request_canonical_refusal_does_not_replay_or_close_primary_flow(
    native_binary,
):
    from livekit import rtc
    from test_gemini_input_review147 import Input

    primary_ready, caller_heard = asyncio.Event(), asyncio.Event()
    opens, backend_work = [], []

    async def peer(request):
        if request.path == "/v1/llm/responses":
            backend_work.append(request)
            return response([("response.completed", {})])
        opens.append(request.headers.copy())
        if primary_ready.is_set():
            return web.json_response(
                {
                    "error": {
                        "source": "relay",
                        "code": "insufficient_credit",
                        "hint": "Original fixture refusal",
                        "retryable": False,
                    }
                },
                status=402,
            )
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json(
            {"type": "session.started", "session": {"id": "actual-primary"}}
        )
        primary_ready.set()
        async for message in ws:
            command = json.loads(message.data)
            if command.get("type") == "session.input_audio.append":
                caller_heard.set()
            if command.get("type") == "session.close":
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        session = AgentSession(llm=model, stt=None, tts=None, vad=None)
        source = Input()
        session.input.audio, session.output.audio = source, CapturedOutput()
        try:
            await start(session)
            await asyncio.wait_for(primary_ready.wait(), 5)
            handle = session.generate_reply(instructions="New original unfunded ask")
            await asyncio.wait_for(handle.wait_for_playout(), 5)
            proxy = next(iter(model._owned_sessions))
            owner = proxy.model.root.owners[0]
            await until(
                lambda: owner.close_task is not None and owner.close_task.done()
            )
            assert handle.exception() is not None and not proxy.closed
            assert len(opens) == 2 and backend_work == []
            assert len({headers["Idempotency-Key"] for headers in opens}) == 2
            assert all(
                headers["Authorization"] == "Bearer " + MODEL_KEY for headers in opens
            )
            source.frames.put_nowait(rtc.AudioFrame(b"\x01\x30" * 2400, 24000, 1, 2400))
            await asyncio.wait_for(caller_heard.wait(), 5)
            assert len(opens) == 2, "a declined genuine request was retried"
        finally:
            await session.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()


async def test_noncooperative_original_sdk_tool_stays_retained_unknown_until_actual_join(
    native_binary,
):
    from livekit.agents import function_tool

    entered, cancelled, release = (asyncio.Event() for _ in range(3))
    effects = []

    @function_tool
    async def pending_tool() -> str:
        """Deliberately ignore cancellation to expose an unproved local join."""
        entered.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            cancelled.set()
            await release.wait()
        effects.append("effect-after-cancel")
        return "Unproved actual tool effect"

    async def peer(request):
        if request.path == "/v1/llm/responses":
            await request.json()
            return response(
                [
                    (
                        "response.item.completed",
                        {
                            "item": {
                                "type": "function_call",
                                "name": "pending_tool",
                                "call_id": "actual-original-call",
                                "arguments": "{}",
                            }
                        },
                    ),
                    ("response.completed", {}),
                ]
            )
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        startup = await ws.receive_json()
        await ws.send_json(
            {"type": "session.started", "session": {"id": "actual-native"}}
        )
        if "Noncooperative original ask" in startup["session"]["instructions"]:
            await ws.send_json(
                {
                    "type": "session.delegation.created",
                    "offset_ms": 0,
                    "delegation": {
                        "id": "actual-native-delegation",
                        "target": "client",
                    },
                }
            )
        async for message in ws:
            if json.loads(message.data).get("type") == "session.close":
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        session = AgentSession(llm=model, stt=None, tts=None, vad=None)
        session.output.audio = CapturedOutput()
        try:
            await start(session, tools=[pending_tool])
            session.generate_reply(instructions="Noncooperative original ask")
            await asyncio.wait_for(entered.wait(), 5)
            proxy = next(iter(model._owned_sessions))
            owner = proxy.model.root.owners[0]
            interrupted = session.interrupt(force=True)
            await asyncio.wait_for(cancelled.wait(), 5)
            await asyncio.wait_for(interrupted, 5)
            assert not owner.joined and any(not task.done() for task in owner.tasks)
            assert (
                owner in proxy.model.root.owners
                and proxy.model.original_generate is not None
            )
            assert effects == [], "unjoined local work was claimed complete"
            release.set()
            await until(lambda: all(task.done() for task in owner.tasks))
            assert effects == ["effect-after-cancel"]
            await until(lambda: owner.joined)
        finally:
            release.set()
            await session.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()


async def test_actually_played_owner_text_reaches_primary_without_private_task(
    native_binary,
):
    from livekit import rtc
    from test_gemini_input_review147 import Input

    heard, microphone_barrier, primary_backend = (asyncio.Event() for _ in range(3))
    primary_context = []
    private_task = "Private original application instruction"
    spoken = "Actually heard original segment"
    backend_payloads = []

    async def peer(request):
        if request.path == "/v1/llm/responses":
            body = await request.json()
            backend_payloads.append(body)
            if private_task not in json.dumps(body["input"]):
                primary_backend.set()
            return response(
                [
                    ("response.text.delta", {"delta": "Bounded original answer"}),
                    ("response.completed", {}),
                ]
            )
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        startup = await ws.receive_json()
        dedicated = private_task in startup["session"]["instructions"]
        await ws.send_json(
            {"type": "session.started", "session": {"id": "actual-native"}}
        )
        if dedicated:
            await ws.send_json(
                {
                    "type": "session.delegation.created",
                    "offset_ms": 0,
                    "delegation": {"id": "owned-actual-delegation", "target": "client"},
                }
            )
        async for message in ws:
            command = json.loads(message.data)
            if dedicated and command.get("delegation_id") == "owned-actual-delegation":
                await ws.send_json(
                    {
                        "type": "session.output_transcript.delta",
                        "delta": spoken,
                        "start_ms": 0,
                        "end_ms": 100,
                    }
                )
                await send_audio(ws, b"\x01\x20" * 2400)
            if not dedicated and command.get("type") == "session.thinking.append":
                primary_context.append(command["content"])
            if not dedicated and command.get("type") == "session.input_audio.append":
                microphone_barrier.set()
                if any(spoken in text for text in primary_context):
                    await ws.send_json(
                        {
                            "type": "session.delegation.created",
                            "offset_ms": 0,
                            "delegation": {
                                "id": "primary-caller-delegation",
                                "target": "client",
                            },
                        }
                    )
            if command.get("type") == "session.close":
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        session = AgentSession(llm=model, stt=None, tts=None, vad=None)
        source = Input()
        session.input.audio, session.output.audio = source, CapturedOutput()
        session.on(
            "conversation_item_added",
            lambda event: (
                heard.set()
                if getattr(event.item, "role", None) == "assistant"
                and event.item.text_content == spoken
                else None
            ),
        )
        try:
            await start(session)
            handle = session.generate_reply(instructions=private_task)
            await asyncio.wait_for(handle.wait_for_playout(), 5)
            await asyncio.wait_for(heard.wait(), 5)
            source.frames.put_nowait(rtc.AudioFrame(b"\x02\x30" * 2400, 24000, 1, 2400))
            await asyncio.wait_for(microphone_barrier.wait(), 5)
            assert any(spoken in text for text in primary_context), (
                "ordinary primary forgot an actually played owned segment"
            )
            assert all(private_task not in text for text in primary_context)
            await asyncio.wait_for(primary_backend.wait(), 5)
            payload = next(
                body
                for body in backend_payloads
                if private_task not in json.dumps(body["input"])
            )
            assert spoken in json.dumps(payload["input"])
            proxy = next(iter(model._owned_sessions))
            assert not proxy.model.root.owners[0].closed
        finally:
            await session.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()


async def test_public_interrupt_joins_original_asks_and_preserves_later_caller_flow(
    native_binary,
):
    from livekit import rtc
    from test_gemini_input_review147 import Input

    microphone = asyncio.Event()
    requests = []

    async def peer(request):
        if request.path == "/v1/llm/responses":
            body = await request.json()
            requests.append(body)
            return response(
                [
                    ("response.text.delta", {"delta": "Original independent answer"}),
                    ("response.completed", {}),
                ]
            )
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        startup = await ws.receive_json()
        instructions = startup["session"]["instructions"]
        dedicated = (
            "Original ask before interrupt" in instructions
            or "New genuine ask after interrupt" in instructions
        )
        await ws.send_json(
            {"type": "session.started", "session": {"id": "actual-native"}}
        )
        if dedicated:
            await ws.send_json(
                {
                    "type": "session.delegation.created",
                    "offset_ms": 0,
                    "delegation": {"id": "actual-delegation", "target": "client"},
                }
            )
        async for message in ws:
            command = json.loads(message.data)
            if dedicated and command.get("delegation_id") == "actual-delegation":
                await send_audio(ws, b"\x01\x20" * 2400)
            if not dedicated and command.get("type") == "session.input_audio.append":
                microphone.set()
            if command.get("type") == "session.close":
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        session = AgentSession(llm=model, stt=None, tts=None, vad=None)
        source = Input()
        session.input.audio, session.output.audio = source, CapturedOutput()
        try:
            await start(session)
            first = session.generate_reply(instructions="Original ask before interrupt")
            await asyncio.wait_for(first.wait_for_playout(), 5)
            proxy = next(iter(model._owned_sessions))
            owner = proxy.model.root.owners[0]
            interrupted = session.interrupt(force=True)
            try:
                assert owner.closed and not proxy.closed, (
                    "public interruption permanently retired the ordinary conversation"
                )
            finally:
                await asyncio.wait_for(interrupted, 5)
            assert owner.joined and not proxy.primary.duplex_session._task.done()
            source.frames.put_nowait(rtc.AudioFrame(b"\x02\x30" * 2400, 24000, 1, 2400))
            await asyncio.wait_for(microphone.wait(), 5)
            second = session.generate_reply(
                instructions="New genuine ask after interrupt"
            )
            await asyncio.wait_for(second.wait_for_playout(), 5)
            assert first.exception() is None and second.exception() is None
            assert len(requests) == 2 and len(proxy.model.root.owners) == 2
            assert "Original ask before interrupt" in json.dumps(requests[0]["input"])
            assert "New genuine ask after interrupt" in json.dumps(requests[1]["input"])
        finally:
            await session.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()
