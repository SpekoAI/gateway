"""SDK metadata stays detached and never becomes provider JSON or authority."""

from __future__ import annotations

import asyncio
import json

import pytest
from aiohttp import web
from livekit.agents import AgentSession, llm
from test_native_gpt_live import models, response
from test_native_realtime_cancellation import start_agent
from test_native_realtime_fences import ROUTES, create_model, ready
from test_native_sidecar import native_binary as native_binary  # noqa: PLC0414
from test_native_sidecar import native_stack

from speko_gateway import GatewayError
from speko_gateway.relay_realtime import _FRAME_BYTES


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
    sockets, backend_requests = asyncio.Queue(), asyncio.Queue()

    async def peer(request):
        if request.path == "/v1/llm/responses":
            backend_requests.put_nowait(await request.json())
            return response(
                [
                    ("response.text.delta", {"delta": "ORIGINAL_BACKEND_ANSWER"}),
                    ("response.completed", {}),
                ]
            )
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        startups.put_nowait(await ws.receive_json())
        await ws.send_json({"type": "session.started"})
        sockets.put_nowait(ws)
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
            ws = await asyncio.wait_for(sockets.get(), 5)
            await ws.send_json(
                {
                    "type": "session.delegation.created",
                    "offset_ms": 0,
                    "delegation": {"id": "opaque_metadata", "target": "client"},
                }
            )
            backend_wire = await asyncio.wait_for(backend_requests.get(), 5)
            assert "ORIGINAL_CONTEXT_TEXT" in json.dumps(backend_wire["input"])
            assert "CHANGED_CALLER_TEXT" not in json.dumps(backend_wire)
            assert "opaque" not in json.dumps(backend_wire)
            assert backend_wire["max_output_tokens"] == 123
        finally:
            await actor.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()


async def test_opaque_metadata_does_not_remove_the_provider_text_bound(native_binary):
    items = []

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ready(ws, "openai", await ws.receive_json())
        async for frame in ws:
            command = json.loads(frame.data)
            if command["type"] == "conversation.item.create":
                items.append(command["item"])
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = create_model(socket, *ROUTES[0])
        actor = AgentSession(llm=model, turn_detection="realtime_llm", vad=None)
        try:
            agent = await start_agent(actor)
            native = agent.realtime_llm_session
            await asyncio.wait_for(native.start(), 5)
            original = native.chat_ctx
            original_agent = agent.chat_ctx.copy()
            context, message = opaque_context()
            message.content = ["x" * (_FRAME_BYTES + 1)]
            with pytest.raises(GatewayError, match="frame bound"):
                await agent.update_chat_ctx(context)
            assert native.chat_ctx.items == original.items
            assert agent.chat_ctx.items == original_agent.items
        finally:
            await actor.aclose()
            await model.aclose()
            await client.aclose()
    assert items == []
