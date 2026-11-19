"""A late delegation cannot execute or satisfy a newer typed application ask."""

from __future__ import annotations

import asyncio
import base64
import json

import pytest
from aiohttp import web
from livekit import rtc
from livekit.agents import AgentSession, llm
from test_native_gpt_live import models, response
from test_native_realtime_cancellation import start_agent
from test_native_sidecar import native_binary as native_binary  # noqa: PLC0414
from test_native_sidecar import native_stack

from speko_gateway import GatewayError

pytestmark = pytest.mark.skipif(
    not hasattr(llm, "DuplexModel"), reason="SDK predates duplex"
)


async def voice_output(ws, text):
    await ws.send_json(
        {
            "type": "session.output_transcript.delta",
            "delta": text,
            "start_ms": 100,
            "end_ms": 200,
        }
    )
    await ws.send_json(
        {
            "type": "session.output_audio.delta",
            "delta": base64.b64encode(b"\x01\x10" * 2400).decode(),
        }
    )


@pytest.mark.parametrize(
    "retirement", ["interrupted", "expired", "preamble", "pending", "claimed"]
)
async def test_typed_ask_refuses_before_context_while_old_delegation_can_arrive(
    native_binary, retirement
):
    sockets = asyncio.Queue()
    first_ask, first_backend_done, ordinary_done = (asyncio.Event() for _ in range(3))
    requests, commands, backend_after_refusal = [], [], []
    refusal_observed = False
    rejected = "NEW_TYPED_WORK_MUST_NOT_REACH_OLD_DELEGATION"

    async def peer(request):
        if request.path == "/v1/llm/responses":
            requests.append(await request.json())
            backend_after_refusal.append(refusal_observed)
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
        async for msg in ws:
            command = json.loads(msg.data)
            commands.append(command)
            if command["type"] == "session.commentary.append":
                identity = command.get("delegation_id")
                if identity is None:
                    first_ask.set()
                elif identity == "late_original":
                    first_backend_done.set()
                    await voice_output(ws, "OLD_CANONICAL_ANSWER")
                elif identity == "first_claimed":
                    await voice_output(ws, "CLAIMED_ORIGINAL_ANSWER")
                elif identity == "ordinary_caller":
                    ordinary_done.set()
                    await voice_output(ws, "ORDINARY_CALLER_ANSWER")
            if command["type"] == "session.close":
                await ws.send_json({"type": "session.closed"})
                await ws.close()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        actor = AgentSession(llm=model, turn_detection="manual", vad=None)
        try:
            initial = llm.ChatContext.empty()
            initial.add_message(role="user", content="ORIGINAL_CANONICAL_USER")
            await start_agent(actor, chat_ctx=initial)
            native = actor.current_agent.realtime_llm_session
            if retirement == "expired":
                native._reply_timeout = 0.05
            old = actor.generate_reply()
            await asyncio.wait_for(first_ask.wait(), 5)
            ws = await asyncio.wait_for(sockets.get(), 5)
            original_timer = native.duplex_session._pending_reply_timer
            assert original_timer is not None
            if retirement == "interrupted":
                await asyncio.wait_for(actor.interrupt(), 5)
                await asyncio.wait_for(old.wait_for_playout(), 5)
            elif retirement == "expired":
                await asyncio.wait_for(old.wait_for_playout(), 5)
                assert old.exception() is not None
            elif retirement == "preamble":
                await voice_output(ws, "ORIGINAL_PREAMBLE")
                await asyncio.wait_for(old.wait_for_playout(), 5)
                assert old.exception() is None and not original_timer.cancelled()
            elif retirement == "claimed":
                await ws.send_json(
                    {
                        "type": "session.delegation.created",
                        "offset_ms": 0,
                        "delegation": {"id": "first_claimed", "target": "client"},
                    }
                )
                await asyncio.wait_for(old.wait_for_playout(), 5)
                assert old.exception() is None
            attempted = actor.generate_reply(user_input=rejected)
            await asyncio.wait_for(attempted.wait_for_playout(), 5)
            assert attempted.exception() is not None, (
                "uncorrelated typed request did not refuse"
            )
            refusal_observed = True
            assert rejected not in str(actor.history.items)
            assert rejected not in str(actor.current_agent.chat_ctx.items)
            assert rejected not in str(native.chat_ctx.items)
            assert rejected not in str(native.duplex_session._history.items)
            replacement = actor.current_agent.chat_ctx.copy()
            replacement.add_message(
                role="user", content="DIRECT_CONTEXT_MUST_NOT_APPEND"
            )
            with pytest.raises(GatewayError, match="correlation"):
                await actor.current_agent.update_chat_ctx(replacement)
            with pytest.raises(GatewayError, match="correlation"):
                await native.update_chat_ctx(replacement)
            with pytest.raises(GatewayError, match="correlation"):
                await native.duplex_session._append_items(
                    [
                        llm.ChatMessage(
                            role="user", content=["DIRECT_DUPLEX_MUST_REFUSE"]
                        )
                    ]
                )
            edited = actor.current_agent.chat_ctx.copy()
            for index, item in enumerate(edited.items):
                if isinstance(item, llm.ChatMessage) and item.role == "user":
                    edited.items[index] = item.model_copy(
                        update={"content": ["EDITED_USER_MUST_REFUSE"]}
                    )
            with pytest.raises(GatewayError, match="correlation"):
                await native.update_chat_ctx(edited)
            with pytest.raises(GatewayError, match="correlation"):
                await actor.current_agent.update_chat_ctx(edited)
            assert "DIRECT_CONTEXT_MUST_NOT_APPEND" not in str(
                actor.current_agent.chat_ctx.items
            )
            assert "DIRECT_CONTEXT_MUST_NOT_APPEND" not in str(native.chat_ctx.items)
            assert "DIRECT_CONTEXT_MUST_NOT_APPEND" not in str(
                native.duplex_session._history.items
            )
            if retirement in {"pending", "preamble"}:
                assert native.duplex_session._pending_reply_timer is original_timer
                assert not original_timer.cancelled()
            await ws.send_json(
                {
                    "type": "session.delegation.created",
                    "offset_ms": 0,
                    "delegation": {"id": "late_original", "target": "client"},
                }
            )
            await asyncio.wait_for(first_backend_done.wait(), 5)
            # Backend delivery began before voice_output completed. Observe
            # the original real handle before calling the next input healthy.
            await asyncio.wait_for(old.wait_for_playout(), 5)
            # Genuine subsequent caller input remains canonical speech, rather
            # than a replacement application task associated by unseen ID.
            native.push_audio(rtc.AudioFrame(b"\x02\x10" * 480, 24000, 1, 480))
            await ws.send_json(
                {
                    "type": "session.input_transcript.delta",
                    "delta": "REAL_SUBSEQUENT_CALLER",
                    "start_ms": 201,
                    "end_ms": 300,
                }
            )
            await ws.send_json(
                {
                    "type": "session.delegation.created",
                    "offset_ms": 300,
                    "delegation": {"id": "ordinary_caller", "target": "client"},
                }
            )
            await asyncio.wait_for(ordinary_done.wait(), 5)
            start = 1 if retirement == "claimed" else 0
            assert len(requests) == 2 + start and all(backend_after_refusal[start:])
            assert all(body["max_output_tokens"] == 123 for body in requests)
            assert "REAL_SUBSEQUENT_CALLER" in json.dumps(requests[-1]["input"])
            assert rejected not in json.dumps(requests) and rejected not in json.dumps(
                commands
            )
            assert "DIRECT_CONTEXT_MUST_NOT_APPEND" not in json.dumps(requests)
            assert "DIRECT_CONTEXT_MUST_NOT_APPEND" not in json.dumps(commands)
            for marker in ("DIRECT_DUPLEX_MUST_REFUSE", "EDITED_USER_MUST_REFUSE"):
                assert marker not in json.dumps(requests) and marker not in json.dumps(
                    commands
                )
                assert marker not in str(actor.current_agent.chat_ctx.items)
                assert marker not in str(native.chat_ctx.items)
            assert (
                len(
                    [
                        c
                        for c in commands
                        if c["type"] == "session.commentary.append"
                        and c.get("delegation_id") is None
                    ]
                )
                == 1
            )
            assert (
                attempted.exception() is not None
                and native.duplex_session._failure is None
            )
        finally:
            await actor.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()


@pytest.mark.parametrize("busy_before_first", [False, True])
async def test_first_unscoped_typed_turn_without_prior_ask_remains_supported(
    native_binary,
    busy_before_first,
):
    requests, commands = [], []

    async def peer(request):
        if request.path == "/v1/llm/responses":
            requests.append(await request.json())
            return response(
                [
                    ("response.text.delta", {"delta": "FIRST_TYPED_ANSWER"}),
                    ("response.completed", {}),
                ]
            )
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"type": "session.started"})
        async for msg in ws:
            command = json.loads(msg.data)
            commands.append(command)
            if command["type"] == "session.commentary.append":
                if command.get("delegation_id") is None:
                    await ws.send_json(
                        {
                            "type": "session.delegation.created",
                            "offset_ms": 0,
                            "delegation": {"id": "first_typed", "target": "client"},
                        }
                    )
                else:
                    await voice_output(ws, "FIRST_TYPED_ANSWER")
            if command["type"] == "session.close":
                await ws.send_json({"type": "session.closed"})
                await ws.close()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        actor = AgentSession(llm=model, turn_detection="manual", vad=None)
        try:
            initial = llm.ChatContext.empty()
            initial.add_message(role="user", content="PRE_HINT_CANONICAL_CONTEXT")
            await start_agent(actor, chat_ctx=initial)
            if busy_before_first:
                competing = actor.current_agent.chat_ctx.copy()
                competing.add_message(role="user", content="BUSY_CONTEXT_MUST_REFUSE")
                async with actor.current_agent._activity._rt_chat_ctx_lock:
                    with pytest.raises(GatewayError, match="correlation"):
                        await asyncio.wait_for(
                            actor.current_agent.update_chat_ctx(competing), 5
                        )
                assert "BUSY_CONTEXT_MUST_REFUSE" not in str(
                    actor.current_agent.chat_ctx.items
                )
            first = actor.generate_reply(user_input="FIRST_CANONICAL_TYPED_REQUEST")
            await asyncio.wait_for(first.wait_for_playout(), 5)
            assert first.exception() is None and len(requests) == 1
            assert "FIRST_CANONICAL_TYPED_REQUEST" in json.dumps(requests[0]["input"])
            assert "PRE_HINT_CANONICAL_CONTEXT" in json.dumps(requests[0]["input"])
            assert requests[0]["max_output_tokens"] == 123
            assert "FIRST_CANONICAL_TYPED_REQUEST" in json.dumps(commands)
            assert "BUSY_CONTEXT_MUST_REFUSE" not in json.dumps(commands)
            assert "BUSY_CONTEXT_MUST_REFUSE" not in json.dumps(requests)
        finally:
            await actor.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()
