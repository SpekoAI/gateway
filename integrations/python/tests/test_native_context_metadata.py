"""SDK metadata stays detached and never becomes provider JSON or authority."""

from __future__ import annotations

import asyncio
import json

import pytest
from aiohttp import web
from livekit.agents import AgentSession, llm
from test_native_gpt_live import models
from test_native_realtime_cancellation import start_agent
from test_native_realtime_fences import ROUTES, create_model, ready
from test_native_sidecar import native_binary as native_binary  # noqa: PLC0414
from test_native_sidecar import native_stack


def opaque_context():
    context = llm.ChatContext.empty()
    message = context.add_message(role="user", content="ORIGINAL_CONTEXT_TEXT")
    message.extra.update(opaque=b"\xff", nested={"values": ["ORIGINAL_METADATA"]})
    return context, message


def assert_detached(native, message):
    message.extra["nested"]["values"][0] = "CHANGED_CALLER_METADATA"
    message.content = ["CHANGED_CALLER_TEXT"]
    retained = next(
        item
        for item in native.chat_ctx.items
        if isinstance(item, llm.ChatMessage) and item.role == "user"
    )
    assert retained.text_content == "ORIGINAL_CONTEXT_TEXT"
    assert retained.extra["opaque"] == b"\xff"
    assert retained.extra["nested"] == {"values": ["ORIGINAL_METADATA"]}


@pytest.mark.parametrize("protocol,name,voice", ROUTES)
@pytest.mark.parametrize("stage", ["startup", "update"])
async def test_native_opaque_metadata_is_local_and_detached(
    native_binary, protocol, name, voice, stage
):
    items = asyncio.Queue()

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ready(ws, protocol, await ws.receive_json())
        async for frame in ws:
            command = json.loads(frame.data)
            if command["type"] == "conversation.item.create":
                items.put_nowait(command["item"])
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = create_model(socket, protocol, name, voice)
        actor = AgentSession(llm=model, turn_detection="realtime_llm", vad=None)
        try:
            context, message = opaque_context()
            agent = await start_agent(
                actor, chat_ctx=context if stage == "startup" else None
            )
            native = agent.realtime_llm_session
            await asyncio.wait_for(native.start(), 5)
            if stage == "update":
                await asyncio.wait_for(agent.update_chat_ctx(context), 5)
            wire = await asyncio.wait_for(items.get(), 5)
            assert wire["content"][0]["text"] == "ORIGINAL_CONTEXT_TEXT"
            assert "extra" not in wire and "opaque" not in json.dumps(wire)
            assert_detached(native, message)
        finally:
            await actor.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.skipif(not hasattr(llm, "DuplexModel"), reason="SDK predates DuplexModel")
async def test_gpt_live_retains_opaque_metadata_without_serializing_it(native_binary):
    startups = asyncio.Queue()

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        startups.put_nowait(await ws.receive_json())
        await ws.send_json({"type": "session.started"})
        async for frame in ws:
            if json.loads(frame.data)["type"] == "session.close":
                await ws.send_json({"type": "session.closed"})
                await ws.close()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        actor = AgentSession(llm=model, turn_detection="manual", vad=None)
        try:
            context, message = opaque_context()
            agent = await start_agent(actor, chat_ctx=context)
            native = agent.realtime_llm_session
            await asyncio.wait_for(native.start(), 5)
            wire = await asyncio.wait_for(startups.get(), 5)
            assert "ORIGINAL_CONTEXT_TEXT" in json.dumps(wire)
            assert "opaque" not in json.dumps(wire)
            assert_detached(native, message)
        finally:
            await actor.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()
