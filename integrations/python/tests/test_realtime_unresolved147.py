"""Enabled ownership gates and the preserved actual SDK cancellation regression."""

from __future__ import annotations

import asyncio
import base64
import json

import pytest
from aiohttp import web
from livekit.agents import AgentSession, llm
from test_native_gpt_live import models, response
from test_native_sidecar import native_binary as native_binary  # noqa: PLC0414
from test_native_sidecar import native_stack
from test_realtime_review147 import client_model, start


async def test_native_tool_cancellation_stops_an_actual_framework_task(native_binary):
    entered, release, finished, cancelled, barrier = (asyncio.Event() for _ in range(5))
    effects = []

    @llm.function_tool
    async def pending_tool(value: str) -> str:
        """Run a synthetic cancellable operation."""
        entered.set()
        try:
            await release.wait()
            effects.append(value)
            return value
        except asyncio.CancelledError:
            cancelled.set()
            raise
        finally:
            finished.set()

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        await ws.send_json(
            {
                "toolCall": {
                    "functionCalls": [
                        {
                            "id": "original-tool",
                            "name": "pending_tool",
                            "args": {"value": "effect"},
                        }
                    ]
                }
            }
        )
        await entered.wait()
        await ws.send_json({"toolCallCancellation": {"ids": ["original-tool"]}})
        await ws.send_json(
            {
                "serverContent": {
                    "inputTranscription": {"text": "Cancellation receive barrier"}
                }
            }
        )
        async for _ in ws:
            pass
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        session = AgentSession(
            llm=model, stt=None, tts=None, vad=None, turn_detection="manual"
        )
        try:
            await start(session, tools=[pending_tool])
            native = next(iter(model._sessions))
            native.on(
                "input_audio_transcription_completed",
                lambda event: (
                    barrier.set()
                    if event.transcript == "Cancellation receive barrier"
                    else None
                ),
            )
            await asyncio.wait_for(barrier.wait(), 5)
            await asyncio.sleep(0)
            release.set()
            await asyncio.wait_for(finished.wait(), 5)
            assert cancelled.is_set() and effects == [], (
                "native cancellation left an actual tool task able to perform its effect"
            )
        finally:
            release.set()
            await session.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.skipif(
    not hasattr(llm, "DuplexModel"), reason="SDK lacks its public duplex interface"
)
async def test_scoped_gpt_request_reaches_only_its_genuine_delegated_backend(
    native_binary,
):
    requests = asyncio.Queue()
    counted = []
    ask_seen, backend_rendered = asyncio.Event(), asyncio.Event()
    ask = "Original scoped task 147"

    async def peer(request):
        if request.path == "/v1/llm/responses":
            body = await request.json()
            counted.append(body)
            requests.put_nowait(body)
            return response(
                [
                    ("response.text.delta", {"delta": "Owned answer"}),
                    ("response.completed", {}),
                ]
            )
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        startup = await ws.receive_json()
        await ws.send_json(
            {"type": "session.started", "session": {"id": "owned-session"}}
        )
        requested = False

        async def delegate_original():
            nonlocal requested
            if requested:
                return
            requested = True
            await ws.send_json(
                {
                    "type": "session.delegation.created",
                    "offset_ms": 0,
                    "delegation": {"id": "original-delegation", "target": "client"},
                }
            )

        # A dedicated original ask installs its immutable scope at startup,
        # before any auto-delegation. The old single socket receives it later.
        if ask in startup["session"].get("instructions", ""):
            ask_seen.set()
            await delegate_original()
        async for message in ws:
            command = json.loads(message.data)
            if command.get("type") == "session.commentary.append":
                if command.get("delegation_id") == "original-delegation":
                    backend_rendered.set()
                elif ask in command.get("content", ""):
                    ask_seen.set()
                    await delegate_original()
            if command.get("type") == "session.close":
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        session = model.session()
        reply = None
        try:
            await session._update_session(
                instructions="Original startup",
                chat_ctx=llm.ChatContext.empty(),
                tools=[],
            )
            await session.duplex_session.start()
            reply = session.generate_reply(instructions=ask)
            request = await asyncio.wait_for(requests.get(), 5)
            assert ask in json.dumps(request["input"]), (
                "the genuine scoped request was absent from delegated backend input"
            )
            await asyncio.wait_for(ask_seen.wait(), 5)
            await asyncio.wait_for(backend_rendered.wait(), 5)
            assert len(counted) == 1, "one original ask replayed its backend admission"
        finally:
            if reply is not None:
                reply.cancel()
                await asyncio.gather(reply, return_exceptions=True)
            await session.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()


@pytest.mark.skipif(
    not hasattr(llm, "DuplexModel"), reason="SDK lacks its public duplex interface"
)
async def test_retired_gpt_ask_cannot_assign_delayed_voice_to_the_next_reply(
    native_binary,
):
    generations = asyncio.Queue()
    first_command, second_command = asyncio.Event(), asyncio.Event()
    first_socket = None
    request_count = 0

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json(
            {"type": "session.started", "session": {"id": "owned-session"}}
        )

        async def delayed_original_voice():
            await second_command.wait()
            if ws is first_socket:
                # This exact physical stream received the first genuine ask
                # before the second API invocation. No inferred provider ID.
                await ws.send_json(
                    {
                        "type": "session.output_audio.delta",
                        "delta": base64.b64encode(b"\x01\x10" * 2400).decode(),
                    }
                )

        task = asyncio.create_task(delayed_original_voice())
        try:
            async for message in ws:
                command = json.loads(message.data)
                if command.get(
                    "type"
                ) == "session.commentary.append" and not command.get("delegation_id"):
                    nonlocal first_socket, request_count
                    request_count += 1
                    if not first_command.is_set():
                        first_socket = ws
                        first_command.set()
                    else:
                        second_command.set()
                if command.get("type") == "session.close":
                    await ws.send_json({"type": "session.closed", "usage": {}})
                    await ws.close()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        session = model.session()
        first = second = None
        session.on("generation_created", generations.put_nowait)
        try:
            await session._update_session(chat_ctx=llm.ChatContext.empty(), tools=[])
            await session.duplex_session.start()
            first = session.generate_reply()
            await asyncio.wait_for(first_command.wait(), 5)
            second = session.generate_reply()
            await asyncio.gather(first, return_exceptions=True)
            await asyncio.wait_for(generations.get(), 5)
            assert not second.done(), (
                "delayed voice from a retired ask claimed the next reply handle"
            )
            assert request_count == 2, (
                "a genuine ask opened more than one native request"
            )
        finally:
            for future in [first, second]:
                if future is not None:
                    future.cancel()
                    await asyncio.gather(future, return_exceptions=True)
            await session.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()
