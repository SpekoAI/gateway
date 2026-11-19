"""Actual queue admission must precede native and framework context publication."""

from __future__ import annotations

import asyncio
import json

import pytest
from aiohttp import web
from livekit import rtc
from livekit.agents import AgentSession
from test_native_realtime_cancellation import start_agent
from test_native_realtime_fences import ROUTES, create_model, ready
from test_native_sidecar import native_binary as native_binary  # noqa: PLC0414
from test_native_sidecar import native_stack

from speko_gateway import GatewayError


@pytest.mark.parametrize("protocol,name,voice", ROUTES)
@pytest.mark.parametrize("bound", ["bytes", "count"])
async def test_unsent_overflow_context_never_enters_native_or_agent_history(
    native_binary, protocol, name, voice, bound
):
    paused, release, closed = (asyncio.Event() for _ in range(3))
    commands = []

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ready(ws, protocol, await ws.receive_json())
        try:
            async for message in ws:
                commands.append(json.loads(message.data))
        finally:
            closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = create_model(socket, protocol, name, voice)
        actor = AgentSession(llm=model, turn_detection="realtime_llm", vad=None)
        try:
            agent = await start_agent(actor)
            native = agent.realtime_llm_session
            await native.start()
            original_send = native._socket.send

            async def paused_send(command):
                paused.set()
                await release.wait()
                await original_send(command)

            native._socket.send = paused_send
            frame = (
                rtc.AudioFrame(bytes(600000), 24000, 1, 300000)
                if bound == "bytes"
                else rtc.AudioFrame(bytes(960), 24000, 1, 480)
            )
            native.push_audio(frame)
            await asyncio.wait_for(paused.wait(), 5)
            for _ in range(1 if bound == "bytes" else 255):
                native.push_audio(frame)
            accepted = agent.chat_ctx.copy()
            accepted.add_message(role="assistant", content="ACCEPTED_BEFORE_OVERFLOW")
            await agent.update_chat_ctx(accepted)
            assert "ACCEPTED_BEFORE_OVERFLOW" in str(native.chat_ctx.items)
            assert "ACCEPTED_BEFORE_OVERFLOW" in str(agent.chat_ctx.items)
            assert native._failure is None
            old_native = str(native.chat_ctx.items)
            old_agent = str(agent.chat_ctx.items)
            old_known = set(native._known_items)
            context = agent.chat_ctx.copy()
            context.add_message(
                role="assistant",
                content="UNSENT_CONTEXT_" + ("x" * 600000 if bound == "bytes" else "x"),
            )
            with pytest.raises(GatewayError, match="exceeds its.*bound"):
                await agent.update_chat_ctx(context)
            assert native._failure is not None
            assert str(native.chat_ctx.items) == old_native
            assert str(agent.chat_ctx.items) == old_agent
            assert native._known_items == old_known
            release.set()
            await asyncio.wait_for(closed.wait(), 5)
            assert "UNSENT_CONTEXT_" not in str(commands)
        finally:
            release.set()
            await actor.aclose()
            await model.aclose()
            await client.aclose()
