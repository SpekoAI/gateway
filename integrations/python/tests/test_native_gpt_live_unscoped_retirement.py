"""An expired/cancelled delivery hint cannot donate its answer to another ask."""

from __future__ import annotations

import asyncio
import json

import pytest
from aiohttp import web
from livekit.agents import AgentSession, llm
from test_native_gpt_live import models, response
from test_native_gpt_live_typed_fence import voice_output
from test_native_realtime_cancellation import start_agent
from test_native_sidecar import native_binary as native_binary  # noqa: PLC0414
from test_native_sidecar import native_stack

pytestmark = pytest.mark.skipif(
    not hasattr(llm, "DuplexModel"), reason="SDK predates duplex"
)


@pytest.mark.parametrize("retirement", ["expiry", "cancellation"])
async def test_retired_unscoped_reply_cannot_be_replaced_before_old_work_drains(
    native_binary, retirement
):
    hints, sockets = asyncio.Queue(), asyncio.Queue()
    backend_done = asyncio.Event()
    commands, requests = [], []

    async def peer(request):
        if request.path == "/v1/llm/responses":
            requests.append(await request.json())
            return response(
                [
                    ("response.text.delta", {"delta": "OLD_DELAYED_ANSWER"}),
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
            if command["type"] == "session.commentary.append":
                if command.get("delegation_id") is None:
                    hints.put_nowait(command)
                    if (
                        hints.qsize() > 0
                        and sum(
                            c["type"] == "session.commentary.append"
                            and c.get("delegation_id") is None
                            for c in commands
                        )
                        == 2
                    ):
                        await ws.send_json(
                            {
                                "type": "session.delegation.created",
                                "offset_ms": 0,
                                "delegation": {"id": "old_delayed", "target": "client"},
                            }
                        )
                elif command["delegation_id"] == "old_delayed":
                    await voice_output(ws, "OLD_DELAYED_ANSWER")
                    backend_done.set()
            elif command["type"] == "session.close":
                await ws.send_json({"type": "session.closed"})
                await ws.close()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        actor = AgentSession(llm=model, turn_detection="manual", vad=None)
        try:
            context = llm.ChatContext.empty()
            context.add_message(role="user", content="ORIGINAL_USER")
            agent = await start_agent(actor, chat_ctx=context)
            native = agent.realtime_llm_session
            await native.start()
            await asyncio.wait_for(sockets.get(), 5)
            old = actor.generate_reply()
            await asyncio.wait_for(hints.get(), 5)
            original = native._reply
            assert original is not None and not original.done()
            if retirement == "cancellation":
                await actor.interrupt()
                assert original.cancelled()
            else:
                # The real original 15-second deadline is unchanged.
                await asyncio.wait_for(old.wait_for_playout(), 20)
                assert old.exception() is not None
                assert original.done() and original.exception() is not None
            replacement = actor.generate_reply()
            await asyncio.wait_for(replacement.wait_for_playout(), 5)
            assert replacement.exception() is not None, (
                "old retired answer fulfilled replacement unscoped reply"
            )
            assert native._reply is None
            assert (
                sum(
                    c["type"] == "session.commentary.append"
                    and c.get("delegation_id") is None
                    for c in commands
                )
                == 1
            )
            assert not requests and not backend_done.is_set()
            assert "OLD_DELAYED_ANSWER" not in str(actor.history.items)
        finally:
            await actor.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()
