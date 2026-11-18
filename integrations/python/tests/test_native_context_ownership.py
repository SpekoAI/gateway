"""Real SDK shallow copies and unkeyed delegates cannot rewrite retained work."""

from __future__ import annotations

import asyncio
import json

import pytest
from aiohttp import web
from livekit import rtc
from livekit.agents import AgentSession, llm
from test_native_gpt_live import models, response
from test_native_gpt_live_typed_fence import voice_output
from test_native_realtime_cancellation import start_agent
from test_native_realtime_context import delegate
from test_native_sidecar import native_binary as native_binary  # noqa: PLC0414
from test_native_sidecar import native_stack

from speko_gateway import GatewayError

pytestmark = pytest.mark.skipif(
    not hasattr(llm, "DuplexModel"), reason="SDK predates DuplexModel"
)


@pytest.mark.parametrize("view", ["agent", "native", "append", "backend"])
async def test_mutable_context_never_rewrites_owned_backend_history(
    native_binary, view
):
    sockets, answered = asyncio.Queue(), asyncio.Queue()
    asked = asyncio.Event()
    requests, retained = [], []

    async def peer(request):
        if request.path == "/v1/llm/responses":
            requests.append(await request.json())
            return response(
                [
                    ("response.text.delta", {"delta": "CANONICAL_ANSWER"}),
                    ("response.completed", {}),
                ]
            )
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"type": "session.started"})
        sockets.put_nowait(ws)
        async for message in ws:
            command = json.loads(message.data)
            if command["type"] == "session.commentary.append":
                if command.get("delegation_id") is None:
                    asked.set()
                else:
                    answered.put_nowait(command["delegation_id"])
                    await voice_output(ws, "CANONICAL_ANSWER")
            elif command["type"] == "session.close":
                await ws.send_json({"type": "session.closed"})
                await ws.close()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        actor = AgentSession(llm=model, turn_detection="manual", vad=None)
        original_chat = backend.chat

        def capture(**options):
            retained.append(options["chat_ctx"])
            return original_chat(**options)

        backend.chat = capture
        try:
            initial = llm.ChatContext.empty()
            initial.add_message(role="user", content="ORIGINAL_CANONICAL_USER")
            agent = await start_agent(actor, chat_ctx=initial)
            native = agent.realtime_llm_session
            await native.start()
            appended = llm.ChatMessage(role="assistant", content=["ORIGINAL_APPEND"])
            await native.duplex_session._append_items([appended])
            speech = actor.generate_reply()
            await asyncio.wait_for(asked.wait(), 5)
            ws = await asyncio.wait_for(sockets.get(), 5)
            if view in ("agent", "native"):
                copied = (agent.chat_ctx if view == "agent" else native.chat_ctx).copy()
                next(
                    i
                    for i in copied.items
                    if isinstance(i, llm.ChatMessage) and i.role == "user"
                ).content = ["EDITED_AFTER_HINT"]
                with pytest.raises(GatewayError, match="correlation"):
                    await (agent if view == "agent" else native).update_chat_ctx(copied)
            elif view == "append":
                appended.content = ["EDITED_AFTER_HINT"]
            await ws.send_json(delegate("old_first"))
            assert await asyncio.wait_for(answered.get(), 5) == "old_first"
            await asyncio.wait_for(speech.wait_for_playout(), 5)
            assert speech.exception() is None
            if view == "backend":
                next(
                    i
                    for i in retained[0].items
                    if isinstance(i, llm.ChatMessage) and i.role == "user"
                ).content = ["EDITED_AFTER_HINT"]
                await ws.send_json(delegate("old_second"))
                assert await asyncio.wait_for(answered.get(), 5) == "old_second"
            assert len(requests) == (2 if view == "backend" else 1)
            assert all(
                "ORIGINAL_CANONICAL_USER" in json.dumps(r["input"])
                and "EDITED_AFTER_HINT" not in json.dumps(r)
                for r in requests
            )
            assert all(r["max_output_tokens"] == 123 for r in requests)
            assert "EDITED_AFTER_HINT" not in str(native.chat_ctx.items)
            assert "EDITED_AFTER_HINT" not in str(native.duplex_session._history.items)
            assert native.duplex_session._failure is None
        finally:
            await actor.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()


@pytest.mark.parametrize("explicit_none", [False, True])
async def test_repeated_genuinely_unscoped_agent_reply_has_no_new_task(
    native_binary, explicit_none
):
    requests, commands = [], []
    asks = 0

    async def peer(request):
        nonlocal asks
        if request.path == "/v1/llm/responses":
            requests.append(await request.json())
            return response(
                [
                    ("response.text.delta", {"delta": "ANSWER"}),
                    ("response.completed", {}),
                ]
            )
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"type": "session.started"})
        async for message in ws:
            command = json.loads(message.data)
            commands.append(command)
            if command["type"] == "session.commentary.append":
                if command.get("delegation_id") is None:
                    asks += 1
                    await ws.send_json(delegate("unscoped_" + str(asks)))
                else:
                    await voice_output(ws, "ANSWER")
            elif command["type"] == "session.close":
                await ws.send_json({"type": "session.closed"})
                await ws.close()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        actor = AgentSession(llm=model, turn_detection="manual", vad=None)
        try:
            initial = llm.ChatContext.empty()
            initial.add_message(role="user", content="CANONICAL_CONTEXT")
            await start_agent(actor, chat_ctx=initial)
            for _ in range(3):
                speech = actor.generate_reply(
                    **(
                        {"user_input": None, "instructions": None}
                        if explicit_none
                        else {}
                    )
                )
                await asyncio.wait_for(speech.wait_for_playout(), 5)
                assert speech.exception() is None
                await asyncio.wait_for(actor.wait_for_idle(), 5)
            assert asks == 3 and len(requests) == 3
            assert all(
                "CANONICAL_CONTEXT" in json.dumps(r["input"])
                and r["max_output_tokens"] == 123
                for r in requests
            )
            assert not any(c.get("content") == "None" for c in commands)
            assert agent_pending(actor) is None
        finally:
            await actor.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()


def agent_pending(actor):
    return actor.current_agent.realtime_llm_session.duplex_session._pending_reply_hint


@pytest.mark.parametrize(
    "evidence", ["pcm", "resampler", "transcript", "delegation", "context"]
)
async def test_first_typed_after_ordinary_work_refuses_before_any_context(
    native_binary, evidence
):
    sockets, delegated = asyncio.Queue(), asyncio.Queue()
    commands, requests = [], []
    marker = "REJECTED_NEW_TYPED_WORK"

    async def peer(request):
        if request.path == "/v1/llm/responses":
            requests.append(await request.json())
            return response(
                [
                    ("response.text.delta", {"delta": "OLD_ANSWER"}),
                    ("response.completed", {}),
                ]
            )
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"type": "session.started"})
        sockets.put_nowait(ws)
        async for message in ws:
            command = json.loads(message.data)
            commands.append(command)
            if command["type"] == "session.commentary.append" and command.get(
                "delegation_id"
            ):
                delegated.put_nowait(command["delegation_id"])
                await voice_output(ws, "OLD_ANSWER")
            elif command["type"] == "session.close":
                await ws.send_json({"type": "session.closed"})
                await ws.close()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        actor = AgentSession(llm=model, turn_detection="manual", vad=None)
        try:
            agent = await start_agent(actor)
            native = agent.realtime_llm_session
            await native.start()
            ws = await asyncio.wait_for(sockets.get(), 5)
            if evidence in ("pcm", "resampler"):
                native.push_audio(
                    rtc.AudioFrame(
                        b"\x12\x10" * 480,
                        48000 if evidence == "resampler" else 24000,
                        1,
                        480,
                    )
                )
            elif evidence == "context":
                context = agent.chat_ctx.copy()
                context.add_message(role="user", content="OLD_CALLER_CONTEXT")
                await agent.update_chat_ctx(context)
            else:
                if evidence == "delegation":
                    await ws.send_json(delegate("old_before_ask"))
                    assert (
                        await asyncio.wait_for(delegated.get(), 5) == "old_before_ask"
                    )
                await ws.send_json(
                    {
                        "type": "session.input_transcript.delta",
                        "delta": "OLD_CALLER_WORDS",
                        "start_ms": 0,
                        "end_ms": 100,
                    }
                )

                async def received():
                    while not native.duplex_session._fragments:
                        await asyncio.sleep(0)

                await asyncio.wait_for(received(), 5)
            assert not native.duplex_session._reply_hint_issued
            rejected = actor.generate_reply(user_input=marker)
            await asyncio.wait_for(rejected.wait_for_playout(), 5)
            assert isinstance(rejected.exception(), llm.RealtimeError)
            for context in (
                agent.chat_ctx,
                actor.history,
                native.chat_ctx,
                native.duplex_session._history,
            ):
                assert marker not in str(context.items)
            await ws.send_json(delegate("late_old_ordinary"))
            assert await asyncio.wait_for(delegated.get(), 5) == "late_old_ordinary"
            await ws.send_json(
                {
                    "type": "session.input_transcript.delta",
                    "delta": "NEXT_REAL_CALLER",
                    "start_ms": 101,
                    "end_ms": 200,
                }
            )
            await ws.send_json({**delegate("next_ordinary"), "offset_ms": 200})
            assert await asyncio.wait_for(delegated.get(), 5) == "next_ordinary"
            assert all(
                marker not in json.dumps(r) and r["max_output_tokens"] == 123
                for r in requests
            )
            assert "NEXT_REAL_CALLER" in json.dumps(requests[-1]["input"])
            assert marker not in json.dumps(commands)
            assert not any(
                c["type"] == "session.commentary.append"
                and c.get("delegation_id") is None
                for c in commands
            )
            assert (
                agent_pending(actor) is None
                and native.duplex_session._pending_reply_timer is None
            )
            assert native.duplex_session._failure is None
        finally:
            await actor.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()
