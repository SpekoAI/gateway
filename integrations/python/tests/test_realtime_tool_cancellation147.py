"""Actual SDK ownership, callback ordering and unjoined-task controls."""

from __future__ import annotations

import asyncio
import json

import pytest
from aiohttp import web
from livekit.agents import AgentSession, llm, utils
from test_native_sidecar import native_binary as native_binary  # noqa: PLC0414
from test_native_sidecar import native_stack
from test_realtime_review147 import client_model, start


async def until(predicate):
    async def wait():
        while not predicate():
            await asyncio.sleep(0)

    await asyncio.wait_for(wait(), 5)


async def tool_call(ws, *, value="effect"):
    await ws.send_json(
        {
            "toolCall": {
                "functionCalls": [
                    {
                        "id": "same-provider-id",
                        "name": "pending_tool",
                        "args": {"value": value},
                    }
                ]
            }
        }
    )


async def test_cancel_before_sdk_registration_prevents_first_task_effect(native_binary):
    ready, publish, held, release, closed = (asyncio.Event() for _ in range(5))
    effects = []

    @llm.function_tool
    async def pending_tool(value: str) -> str:
        """Record the first actual tool task side effect."""
        effects.append(value)
        return value

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        ready.set()
        await publish.wait()
        await tool_call(ws)
        await held.wait()
        await ws.send_json({"toolCallCancellation": {"ids": ["same-provider-id"]}})
        async for _ in ws:
            pass
        closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        session = AgentSession(
            llm=model, stt=None, tts=None, vad=None, turn_detection="manual"
        )
        try:
            await start(session, tools=[pending_tool])
            await asyncio.wait_for(ready.wait(), 5)
            native = next(iter(model._sessions))
            port = native._tool_port
            activity = port.activity()
            original = activity._realtime_generation_task

            async def delayed(*args, **kwargs):
                held.set()
                await release.wait()
                return await original(*args, **kwargs)

            activity._realtime_generation_task = delayed
            publish.set()
            await until(lambda: port.is_cancelled("same-provider-id"))
            record = port.calls["same-provider-id"]
            assert not record.registration.done() and effects == []
            release.set()
            await until(lambda: record.joined or bool(effects))
            assert effects == [], (
                "the original SDK tool task ran before its cancellation latch"
            )
            assert record.registered and record.tasks
            assert all(task.done() for task in record.tasks) and effects == [], (
                "the original SDK tool task ran before its cancellation latch"
            )
        finally:
            release.set()
            await session.aclose()
            await model.aclose()
            await client.aclose()
        await asyncio.wait_for(closed.wait(), 5)


async def test_same_provider_id_in_another_agent_session_keeps_its_tool(native_binary):
    entered = {value: asyncio.Event() for value in ["left", "right"]}
    release, cancel, barrier, result_received = (asyncio.Event() for _ in range(4))
    cancelled, effects = set(), []
    connected = 0

    @llm.function_tool
    async def pending_tool(value: str) -> str:
        """Wait independently for this original session's operation."""
        entered[value].set()
        try:
            await release.wait()
            effects.append(value)
            return value
        except asyncio.CancelledError:
            cancelled.add(value)
            raise

    async def peer(request):
        nonlocal connected
        value = ["left", "right"][connected]
        connected += 1
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        await tool_call(ws, value=value)
        await entered[value].wait()
        if value == "left":
            await cancel.wait()
            await ws.send_json({"toolCallCancellation": {"ids": ["same-provider-id"]}})
            await ws.send_json(
                {"serverContent": {"inputTranscription": {"text": "joined"}}}
            )
        async for message in ws:
            if value == "right" and "toolResponse" in json.loads(message.data):
                result_received.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        owned = []
        try:
            for value in ["left", "right"]:
                client, model = client_model(socket)
                session = AgentSession(
                    llm=model, stt=None, tts=None, vad=None, turn_detection="manual"
                )
                owned.append((client, model, session))
                await start(session, tools=[pending_tool])
                await asyncio.wait_for(entered[value].wait(), 5)
            left = next(iter(owned[0][1]._sessions))
            right = next(iter(owned[1][1]._sessions))
            left.on("input_audio_transcription_completed", lambda _: barrier.set())
            cancel.set()
            await asyncio.wait_for(barrier.wait(), 5)
            left_call = left._tool_port.calls["same-provider-id"]
            right_call = right._tool_port.calls["same-provider-id"]
            assert left_call.call is not right_call.call
            assert left_call.speech is not right_call.speech
            assert cancelled == {"left"} and not right_call.cancelled
            release.set()
            await until(lambda: effects == ["right"])
            await asyncio.wait_for(result_received.wait(), 5)
            assert right._failure is None
            assert left_call.joined and effects == ["right"], (
                "native cancellation crossed an exact SDK call/session boundary"
            )
        finally:
            release.set()
            for client, model, session in reversed(owned):
                await session.aclose()
                await model.aclose()
                await client.aclose()


async def test_copied_function_call_cannot_issue_task_ownership(
    native_binary, monkeypatch
):
    ready, publish, closed = (asyncio.Event() for _ in range(3))
    effects = []

    @llm.function_tool
    async def pending_tool(value: str) -> str:
        """Record a forbidden reconstructed call's effect."""
        effects.append(value)
        return value

    original = utils.aio.Chan.send_nowait

    def copied(channel, value):
        return original(
            channel,
            value.model_copy(deep=True)
            if isinstance(value, llm.FunctionCall)
            else value,
        )

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        ready.set()
        await publish.wait()
        await tool_call(ws)
        async for _ in ws:
            pass
        closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        session = AgentSession(
            llm=model, stt=None, tts=None, vad=None, turn_detection="manual"
        )
        try:
            await start(session, tools=[pending_tool])
            await asyncio.wait_for(ready.wait(), 5)
            native = next(iter(model._sessions))
            monkeypatch.setattr(utils.aio.Chan, "send_nowait", copied)
            publish.set()
            await until(lambda: closed.is_set() or bool(effects))
            record = native._tool_port.calls["same-provider-id"]
            assert record.unknown and not record.registered and effects == [], (
                "a copied FunctionCall acquired original SDK task ownership"
            )
        finally:
            await session.aclose()
            await model.aclose()
            await client.aclose()


async def test_noncooperative_actual_task_remains_retained_and_unjoined(native_binary):
    entered, cancel, closed, release, finished = (asyncio.Event() for _ in range(5))
    cancellations = []

    @llm.function_tool
    async def pending_tool(value: str) -> str:
        """Keep an actual task alive after refusing cooperative cancellation."""
        entered.set()
        try:
            while not release.is_set():
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    cancellations.append(value)
            return value
        finally:
            finished.set()

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        await tool_call(ws)
        await entered.wait()
        await cancel.wait()
        await ws.send_json({"toolCallCancellation": {"ids": ["same-provider-id"]}})
        async for _ in ws:
            pass
        closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        session = AgentSession(
            llm=model, stt=None, tts=None, vad=None, turn_detection="manual"
        )
        try:
            await start(session, tools=[pending_tool])
            await asyncio.wait_for(entered.wait(), 5)
            native = next(iter(model._sessions))
            port = native._tool_port
            cancel.set()
            await asyncio.wait_for(closed.wait(), 5)
            record = port.calls["same-provider-id"]
            await until(lambda: record.unknown)
            assert record.cancelled and not record.joined
            assert record.registered and any(not task.done() for task in record.tasks)
            assert (
                cancellations and port.mux is not None and native._failure is not None
            ), "a noncooperative tool was forgotten or reported joined"
            release.set()
            await asyncio.wait_for(finished.wait(), 5)
        finally:
            release.set()
            await session.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("batch", [False, True])
async def test_cancelled_result_waiting_for_physical_write_is_removed(
    native_binary, batch
):
    ready, publish, cancel, barrier, drained = (asyncio.Event() for _ in range(5))
    received = []

    @llm.function_tool
    async def pending_tool(value: str) -> str:
        """Complete before its result can acquire the physical writer."""
        return value

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        ready.set()
        await publish.wait()
        await tool_call(ws)
        if batch:
            await ws.send_json(
                {
                    "toolCall": {
                        "functionCalls": [
                            {
                                "id": "uncancelled-provider-id",
                                "name": "pending_tool",
                                "args": {"value": "retained"},
                            }
                        ]
                    }
                }
            )
        await cancel.wait()
        await ws.send_json({"toolCallCancellation": {"ids": ["same-provider-id"]}})
        await ws.send_json(
            {"serverContent": {"inputTranscription": {"text": "cancelled"}}}
        )
        async for message in ws:
            value = json.loads(message.data)
            received.append(value)
            if value.get("clientContent", {}).get("turnComplete"):
                drained.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        session = AgentSession(
            llm=model, stt=None, tts=None, vad=None, turn_detection="manual"
        )
        lock = reply = None
        try:
            await start(session, tools=[pending_tool])
            await asyncio.wait_for(ready.wait(), 5)
            native = next(iter(model._sessions))
            await native._ready
            native.on("input_audio_transcription_completed", lambda _: barrier.set())
            lock = native._socket._write_lock
            await lock.acquire()
            publish.set()
            await until(lambda: bool(lock._waiters))
            if batch:
                await until(
                    lambda: (
                        len(
                            [
                                item
                                for item in native._chat_ctx.items
                                if isinstance(item, llm.FunctionCallOutput)
                            ]
                        )
                        == 2
                    )
                )
            cancel.set()
            await asyncio.wait_for(barrier.wait(), 5)
            record = native._tool_port.calls["same-provider-id"]
            assert record.joined and not record.publications
            reply = native.generate_reply()
            lock.release()
            lock = None
            await asyncio.wait_for(drained.wait(), 5)
            results = [
                response
                for item in received
                for response in item.get("toolResponse", {}).get(
                    "functionResponses", []
                )
            ]
            assert [item["id"] for item in results] == (
                ["uncancelled-provider-id"] if batch else []
            ), (
                "a cancelled queued tool result reached the physical writer or an unrelated result was lost"
            )
        finally:
            if lock is not None and lock.locked():
                lock.release()
            if reply is not None:
                reply.cancel()
                await asyncio.gather(reply, return_exceptions=True)
            await session.aclose()
            await model.aclose()
            await client.aclose()


async def test_cancellation_during_entered_result_write_stays_unknown(
    native_binary, monkeypatch
):
    ready, publish, entered, cancel, release, closed = (
        asyncio.Event() for _ in range(6)
    )
    received = []

    @llm.function_tool
    async def pending_tool(value: str) -> str:
        """Complete before its entered result publication is held."""
        return value

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        ready.set()
        await publish.wait()
        await tool_call(ws)
        await cancel.wait()
        await ws.send_json({"toolCallCancellation": {"ids": ["same-provider-id"]}})
        async for message in ws:
            received.append(json.loads(message.data))
        closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        session = AgentSession(
            llm=model, stt=None, tts=None, vad=None, turn_detection="manual"
        )
        try:
            await start(session, tools=[pending_tool])
            await asyncio.wait_for(ready.wait(), 5)
            native = next(iter(model._sessions))
            await native._ready
            websocket = native._socket._websocket
            original = websocket.send_str

            async def held(data, *args, **kwargs):
                if "toolResponse" in json.loads(data):
                    entered.set()
                    await release.wait()
                return await original(data, *args, **kwargs)

            monkeypatch.setattr(websocket, "send_str", held)
            publish.set()
            await asyncio.wait_for(entered.wait(), 5)
            record = native._tool_port.calls["same-provider-id"]
            assert any(not task.done() for task in record.publications)
            cancel.set()
            await until(lambda: closed.is_set() or record.joined)
            assert record.cancelled and record.unknown and not record.joined, (
                "an entered tool result write was reported cleanly withdrawn"
            )
            assert native._failure is not None and record.publications
            assert not received
        finally:
            release.set()
            await session.aclose()
            await model.aclose()
            await client.aclose()


async def test_low_level_consumer_has_no_sdk_task_cancellation_authority(native_binary):
    publish, closed = (asyncio.Event() for _ in range(2))

    @llm.function_tool
    async def pending_tool(value: str) -> str:
        """Declare a tool without an owning AgentSession task."""
        return value

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        await tool_call(ws)
        await publish.wait()
        await ws.send_json({"toolCallCancellation": {"ids": ["same-provider-id"]}})
        async for _ in ws:
            pass
        closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        native = model.session()
        try:
            await native._update_session(tools=[pending_tool])
            await native.start()
            await until(lambda: "same-provider-id" in native._tool_port.calls)
            record = native._tool_port.calls["same-provider-id"]
            publish.set()
            await asyncio.wait_for(closed.wait(), 5)
            assert native._tool_port.mux is None
            assert record.unknown and not record.registered and not record.joined
            assert native._failure is not None and not record.tasks, (
                "a low-level function stream was mistaken for an owned SDK task"
            )
        finally:
            await native.aclose()
            await model.aclose()
            await client.aclose()


async def test_registration_after_unknown_close_never_reopens_the_tool(native_binary):
    ready, publish, held, release, closed, resumed = (asyncio.Event() for _ in range(6))
    effects = []

    @llm.function_tool
    async def pending_tool(value: str) -> str:
        """Expose any late callback that opens a retired tool."""
        effects.append(value)
        return value

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        ready.set()
        await publish.wait()
        await tool_call(ws)
        await held.wait()
        await ws.send_json({"toolCallCancellation": {"ids": ["same-provider-id"]}})
        async for _ in ws:
            pass
        closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        session = AgentSession(
            llm=model, stt=None, tts=None, vad=None, turn_detection="manual"
        )
        try:
            await start(session, tools=[pending_tool])
            await asyncio.wait_for(ready.wait(), 5)
            native = next(iter(model._sessions))
            port = native._tool_port
            activity = port.activity()
            original = activity._realtime_generation_task

            async def delayed(*args, **kwargs):
                held.set()
                while not release.is_set():
                    try:
                        await release.wait()
                    except asyncio.CancelledError:
                        pass
                try:
                    return await original(*args, **kwargs)
                finally:
                    resumed.set()

            activity._realtime_generation_task = delayed
            publish.set()
            await asyncio.wait_for(closed.wait(), 5)
            record = port.calls["same-provider-id"]
            assert record.cancelled and record.unknown and not record.joined
            assert not record.registration.done() and port.mux is not None
            release.set()
            await asyncio.wait_for(resumed.wait(), 5)
            assert record.unknown and not record.joined and effects == [], (
                "a late original SDK continuation reopened an unknown tool"
            )
        finally:
            release.set()
            await session.aclose()
            await model.aclose()
            await client.aclose()


async def test_actual_tool_is_cancelled_before_join_continuation_wait(native_binary):
    entered, release, cancelled, cancel, held, join_release = (
        asyncio.Event() for _ in range(6)
    )
    effects = []

    @llm.function_tool
    async def pending_tool(value: str) -> str:
        """Run only if cancellation has not already reached its actual task."""
        entered.set()
        try:
            await release.wait()
            effects.append(value)
            return value
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        await tool_call(ws)
        await entered.wait()
        await cancel.wait()
        await ws.send_json({"toolCallCancellation": {"ids": ["same-provider-id"]}})
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
            await asyncio.wait_for(entered.wait(), 5)
            port = next(iter(model._sessions))._tool_port
            original = port._join

            async def delayed(record):
                held.set()
                await join_release.wait()
                return await original(record)

            port._join = delayed
            cancel.set()
            await asyncio.wait_for(held.wait(), 5)
            release.set()
            await until(lambda: cancelled.is_set() or bool(effects))
            assert cancelled.is_set() and effects == [], (
                "actual SDK tool cancellation waited behind its join continuation"
            )
            join_release.set()
            await until(lambda: port.calls["same-provider-id"].joined)
        finally:
            release.set()
            join_release.set()
            await session.aclose()
            await model.aclose()
            await client.aclose()
