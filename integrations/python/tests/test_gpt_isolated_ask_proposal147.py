"""Actual SDK/owned-peer proof only; no production admission/root issuer exists."""

from __future__ import annotations

import asyncio
import base64
import json
import time

import pytest
from aiohttp import web
from gpt_isolated_ask_proposal147 import IsolatedAskModel, TestRoot
from livekit import rtc
from livekit.agents import llm
from test_gemini_input_review147 import Input, until
from test_native_gpt_live import models, response
from test_native_sidecar import LOCAL_TOKEN, MODEL_KEY, native_stack
from test_native_sidecar import native_binary as native_binary  # noqa: PLC0414
from test_realtime_review147 import Output, start

from speko_gateway import RelayRealtimeClient

pytestmark = pytest.mark.skipif(
    not hasattr(llm, "DuplexModel"), reason="SDK lacks its public duplex interface"
)


@pytest.fixture(params=["prototype", "production"])
def composition(request):
    return request.param


class CapturedOutput(Output):
    def __init__(self):
        super().__init__()
        self.bytes = []

    async def capture_frame(self, frame):
        self.bytes.append(bytes(frame.data))
        await super().capture_frame(frame)


async def send_audio(ws, pcm):
    await ws.send_json(
        {"type": "session.output_audio.delta", "delta": base64.b64encode(pcm).decode()}
    )
    # Actual native silence closes this SDK playback burst; it does NOT close
    # the physical operation or claim whole-answer/provider completion.
    await ws.send_json(
        {
            "type": "session.output_audio.delta",
            "delta": base64.b64encode(b"\0\0" * 24000).decode(),
        }
    )


async def test_isolated_original_asks_retain_streams_and_real_sdk_playback(
    native_binary,
    composition,
):
    sockets = {}
    closed = set()
    backend_inputs = []
    redundant_prompts = []
    base_input = asyncio.Event()
    release_a, release_b, release_late, release_base = (
        asyncio.Event() for _ in range(4)
    )
    a_pcm, b_pcm, late_pcm, base_pcm = (
        value * 2400 for value in [b"\x01\x11", b"\x02\x22", b"\x03\x33", b"\x04\x44"]
    )
    controller = None

    async def peer(request):
        if request.path == "/v1/llm/responses":
            payload = await request.json()
            backend_inputs.append(payload)
            assert payload["routing"] == {
                "mode": "explicit",
                "provider": "anthropic",
                "model": "claude-sonnet-4",
            }
            assert payload["max_output_tokens"] == 123
            return response(
                [
                    (
                        "response.text.delta",
                        {"delta": "Genuine bounded backend answer"},
                    ),
                    ("response.completed", {}),
                ]
            )
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        startup = await ws.receive_json()
        instructions = startup["session"]["instructions"]
        label = (
            "A"
            if "Only application A" in instructions
            else "B"
            if "Only application B" in instructions
            else "primary"
        )
        assert label not in sockets
        sockets[label] = ws
        await ws.send_json(
            {"type": "session.started", "session": {"id": "native-provider-session"}}
        )

        async def output():
            if label == "A":
                await release_a.wait()
                await send_audio(ws, a_pcm)
                await release_late.wait()
                await send_audio(ws, late_pcm)
            elif label == "B":
                await release_b.wait()
                await send_audio(ws, b_pcm)
            else:
                await release_base.wait()
                await base_input.wait()
                await send_audio(ws, base_pcm)

        task = asyncio.create_task(output())
        if label != "primary":
            # Genuine startup delegation PRECEDES the commentary command.
            # Immutable scoped context must already belong to this operation.
            await ws.send_json(
                {
                    "type": "session.delegation.created",
                    "offset_ms": 0,
                    "delegation": {"id": f"provider-{label}", "target": "client"},
                }
            )
        try:
            async for message in ws:
                command = json.loads(message.data)
                if (
                    label != "primary"
                    and command.get("type") == "session.commentary.append"
                    and command.get("delegation_id") is None
                ):
                    redundant_prompts.append(label)
                if (
                    label == "primary"
                    and command.get("type") == "session.input_audio.append"
                ):
                    base_input.set()
                if command.get("type") == "session.close":
                    await ws.send_json({"type": "session.closed", "usage": {}})
                    await ws.close()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            closed.add(label)
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        voice, backend_client, backend, primary = models(socket)
        # TEST provisioner supplies a real local original deadline and a
        # retained-operation ceiling. Neither is claimed as a canonical issuer.
        root = TestRoot(deadline=time.monotonic() + 10, maximum=2)
        clients = []

        def open_original(original):
            root.require_open()
            assert original.original_deadline == root.original_deadline
            client = RelayRealtimeClient(
                api_key=MODEL_KEY,
                session_id="ses_native_duplex",
                socket_path=socket,
                local_auth_token=LOCAL_TOKEN,
                setup_timeout=min(
                    voice._setup_timeout, original.original_deadline - time.monotonic()
                ),
            )
            clients.append(client)
            return client

        controller = (
            primary
            if composition == "production"
            else IsolatedAskModel(primary, backend, open_original, root)
        )
        from livekit.agents import AgentSession

        session = AgentSession(llm=controller, stt=None, tts=None, vad=None)
        if composition == "prototype":
            controller.bind_actual_agent(session)
        source, output = Input(), CapturedOutput()
        session.input.audio, session.output.audio = source, output
        proxy = None
        try:
            await start(session)
            proxy = (
                next(iter(controller._owned_sessions))
                if composition == "production"
                else controller.proxy
            )
            scope = proxy.model
            root = scope.root
            a = session.generate_reply(instructions="Only application A")
            b = session.generate_reply(instructions="Only application B")
            original_owners = tuple(root.owners)
            assert original_owners[0].speech is a and original_owners[1].speech is b
            assert all(
                owner.original.captured_monotonic <= time.monotonic()
                for owner in original_owners
            )
            release_a.set()
            await until(
                lambda: (
                    root.owners[0].future is not None and root.owners[0].future.done()
                )
            )
            await asyncio.wait_for(a.wait_for_playout(), 5)
            await until(lambda: "B" in sockets and root.owners[1].future is not None)
            await until(lambda: len(backend_inputs) == 2)
            if composition == "production":
                assert not redundant_prompts, (
                    "an already delegated original startup was asked a second time"
                )
            assert all(
                ("Only application A" in json.dumps(payload["input"]))
                != ("Only application B" in json.dumps(payload["input"]))
                for payload in backend_inputs
            ), "one genuine original backend lost or borrowed another ask's scope"
            if composition == "production":
                clients = [owner.client for owner in root.owners]
            assert not closed and all(client._attempted for client in clients)
            source.frames.put_nowait(rtc.AudioFrame(b"\x05\x55" * 2400, 24000, 1, 2400))
            release_late.set()
            release_base.set()
            await until(lambda: len(root.owners[0].generations) >= 2)
            await until(lambda: len(proxy.primary_generations) >= 1)
            assert not root.owners[1].future.done() and not b.done(), (
                "retained old/base voice claimed the newer genuine SDK ask"
            )
            assert len(root.owners[0].speech_members) >= 2, (
                "late original voice lost its actual SDK SpeechHandle membership"
            )
            release_b.set()
            await asyncio.wait_for(b.wait_for_playout(), 5)
            assert b.exception() is None and a.exception() is None
            for handle in root.owners[0].speech_members[1:] + scope.primary_speeches:
                await asyncio.wait_for(handle.wait_for_playout(), 5)
            assert all(
                any(pattern[:2] in frame for frame in output.bytes)
                for pattern in [a_pcm, b_pcm, late_pcm, base_pcm]
            ), "queued original native audio was silently truncated"
            assert not closed and len(root.owners) == len(clients) == 2
            if composition == "prototype":
                with pytest.raises(
                    llm.RealtimeError, match="retained-operation capacity"
                ):
                    session.generate_reply(instructions="No invented capacity")
            assert len(clients) == 2
        finally:
            for event in [release_a, release_b, release_late, release_base]:
                event.set()
            if proxy is not None:
                proxy.request_close()
                await proxy.aclose()
            await session.aclose()
            await controller.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()
        await until(lambda: closed == {"primary", "A", "B"})
        assert all(owner.startup.done() for owner in root.owners)
        assert all(owner.inner.duplex_session._task.done() for owner in root.owners)


@pytest.mark.parametrize("cancel", [False, True])
async def test_isolated_tool_results_and_global_interrupt_keep_exact_sdk_owner(
    native_binary, cancel, composition
):
    from livekit.agents import AgentSession, function_tool

    entered, release, cancelled = (asyncio.Event() for _ in range(3))
    effects, requests, closed = [], [], []
    actual_tool_tasks = []

    @function_tool
    async def owned_lookup(value: str) -> str:
        """Read one actual caller-owned test value."""
        actual_tool_tasks.append(asyncio.current_task())
        entered.set()
        try:
            if cancel:
                await release.wait()
            effects.append(value)
            return "tool-" + value
        except asyncio.CancelledError:
            cancelled.set()
            raise

    @function_tool
    async def unused_tool() -> str:
        """This registered tool is outside either original ask's selection."""
        effects.append("wrong-selection")
        return "Must not run"

    async def peer(request):
        if request.path == "/v1/llm/responses":
            body = await request.json()
            requests.append(body)
            assert [tool["name"] for tool in body["tools"]] == ["owned_lookup"]
            text = json.dumps(body["input"])
            if "Scoped tool C" in text:
                assert any(
                    item.get("type") == "function_result" for item in body["input"]
                ), "a later genuine ask lost captured original tool history"
                return response(
                    [
                        ("response.text.delta", {"delta": "Third genuine answer"}),
                        ("response.completed", {}),
                    ]
                )
            label = "A" if "Scoped tool A" in text else "B"
            outputs = [
                item for item in body["input"] if item.get("type") == "function_result"
            ]
            if outputs:
                assert len(outputs) == 1 and outputs[0]["result"] == "tool-" + label
                return response(
                    [
                        (
                            "response.text.delta",
                            {"delta": "Actual tool answer " + label},
                        ),
                        ("response.completed", {}),
                    ]
                )
            return response(
                [
                    (
                        "response.item.completed",
                        {
                            "item": {
                                "type": "function_call",
                                "name": "owned_lookup",
                                # Deliberately identical across PHYSICAL native owners.
                                "call_id": "same-backend-id",
                                "arguments": json.dumps({"value": label}),
                            }
                        },
                    ),
                    ("response.completed", {}),
                ]
            )
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        startup = await ws.receive_json()
        instructions = startup["session"]["instructions"]
        label = (
            "C"
            if "Scoped tool C" in instructions
            else "A"
            if "Scoped tool A" in instructions
            else "B"
            if "Scoped tool B" in instructions
            else "primary"
        )
        await ws.send_json(
            {"type": "session.started", "session": {"id": "actual-native"}}
        )
        if label != "primary":
            await ws.send_json(
                {
                    "type": "session.delegation.created",
                    "offset_ms": 0,
                    "delegation": {"id": "same-native-delegation", "target": "client"},
                }
            )
        async for message in ws:
            command = json.loads(message.data)
            if command.get("delegation_id") == "same-native-delegation":
                await send_audio(
                    ws, (b"\x01\x20" if label == "A" else b"\x02\x30") * 2400
                )
            if command.get("type") == "session.close":
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        closed.append(label)
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        voice, backend_client, backend, primary = models(socket)
        root = TestRoot(deadline=time.monotonic() + 10, maximum=2)

        def open_original(original):
            return RelayRealtimeClient(
                api_key=MODEL_KEY,
                session_id="ses_native_duplex",
                socket_path=socket,
                local_auth_token=LOCAL_TOKEN,
                setup_timeout=min(
                    voice._setup_timeout, original.original_deadline - time.monotonic()
                ),
            )

        model = (
            primary
            if composition == "production"
            else IsolatedAskModel(primary, backend, open_original, root)
        )
        session = AgentSession(llm=model, stt=None, tts=None, vad=None)
        if composition == "prototype":
            model.bind_actual_agent(session)
        session.output.audio = CapturedOutput()
        proxy = None
        try:
            await start(
                session,
                tools=[owned_lookup, unused_tool]
                if composition == "production"
                else [owned_lookup],
            )
            proxy = (
                next(iter(model._owned_sessions))
                if composition == "production"
                else model.proxy
            )
            root = proxy.model.root
            selected = (
                {"tools": ["owned_lookup"]} if composition == "production" else {}
            )
            a = session.generate_reply(instructions="Scoped tool A", **selected)
            b = (
                None
                if cancel
                else session.generate_reply(instructions="Scoped tool B", **selected)
            )
            await asyncio.wait_for(entered.wait(), 5)
            if cancel:
                interrupted = session.interrupt(force=True)
                assert all(owner.closed for owner in root.owners)
                assert root.closed == (composition == "prototype")
                await asyncio.wait_for(interrupted, 5)
                assert cancelled.is_set() and all(
                    task.done() for task in actual_tool_tasks
                ), "the actual SDK inner tool task outlived its claimed join"
                await asyncio.wait_for(cancelled.wait(), 5)
                release.set()
                assert effects == [] and all(owner.joined for owner in root.owners)
                assert all(task.done() for owner in root.owners for task in owner.tasks)
            else:
                await asyncio.wait_for(a.wait_for_playout(), 5)
                await asyncio.wait_for(b.wait_for_playout(), 5)
                await until(lambda: len(requests) == 4)
                assert sorted(effects) == ["A", "B"]
                assert all(len(owner.calls) == 1 for owner in root.owners)
                assert (
                    root.owners[0].calls["same-backend-id"]
                    is not root.owners[1].calls["same-backend-id"]
                )
                if composition == "production":
                    c = session.generate_reply(instructions="Scoped tool C", **selected)
                    await asyncio.wait_for(c.wait_for_playout(), 5)
                    await until(lambda: len(requests) == 5)
                    assert c.exception() is None and len(root.owners) == 3
                    assert len(proxy.primary.tools.flatten()) == 2, (
                        "per-ask tools mutated ordinary primary configuration"
                    )
        finally:
            release.set()
            if proxy is not None:
                proxy.request_close()
                await proxy.aclose()
            await session.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()
        assert all(owner.startup.done() for owner in root.owners)
        assert all(owner.inner.duplex_session._task.done() for owner in root.owners)


@pytest.mark.parametrize("phase", ["before", "during"])
async def test_original_sdk_cancellation_cannot_publish_after_delayed_open(
    native_binary, phase, composition
):
    from livekit.agents import AgentSession

    primary_ready, request_seen, release, peer_finished = (
        asyncio.Event() for _ in range(4)
    )
    child_started = []
    requests = []

    async def peer(request):
        requests.append(request)
        if len(requests) > 1:
            request_seen.set()
            await release.wait()
            if request.transport is None or request.transport.is_closing():
                peer_finished.set()
                return web.Response(status=409)
        ws = web.WebSocketResponse()
        try:
            await ws.prepare(request)
            startup = await ws.receive_json()
            if len(requests) > 1:
                child_started.append(startup)
            else:
                primary_ready.set()
            await ws.send_json(
                {"type": "session.started", "session": {"id": "owned-session"}}
            )
            async for message in ws:
                if json.loads(message.data).get("type") == "session.close":
                    await ws.send_json({"type": "session.closed", "usage": {}})
                    await ws.close()
        except (ConnectionError, RuntimeError):
            # Actual disconnected transport, never permission to retry the ask.
            pass
        finally:
            if len(requests) > 1:
                peer_finished.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        voice, backend_client, backend, primary = models(socket)
        root = TestRoot(deadline=time.monotonic() + 10, maximum=2)
        clients = []

        def open_original(original):
            root.require_open()
            client = RelayRealtimeClient(
                api_key=MODEL_KEY,
                session_id="ses_native_duplex",
                socket_path=socket,
                local_auth_token=LOCAL_TOKEN,
                setup_timeout=min(
                    voice._setup_timeout, original.original_deadline - time.monotonic()
                ),
            )
            clients.append(client)
            return client

        model = (
            primary
            if composition == "production"
            else IsolatedAskModel(primary, backend, open_original, root)
        )
        session = AgentSession(llm=model, stt=None, tts=None, vad=None)
        if composition == "prototype":
            model.bind_actual_agent(session)
        session.output.audio = CapturedOutput()
        proxy = None
        try:
            await start(session)
            proxy = (
                next(iter(model._owned_sessions))
                if composition == "production"
                else model.proxy
            )
            root = proxy.model.root
            await asyncio.wait_for(primary_ready.wait(), 5)
            speech = session.generate_reply(instructions="Original cancelled ask")
            if phase == "during":
                await asyncio.wait_for(request_seen.wait(), 5)
            interrupted = session.interrupt(force=True)
            assert root.owners[0].closed
            assert root.closed == (composition == "prototype")
            await asyncio.wait_for(interrupted, 5)
            release.set()
            if phase == "during":
                await asyncio.wait_for(peer_finished.wait(), 5)
            assert child_started == [], (
                "a cancelled original open published late startup"
            )
            if composition == "production":
                clients = [
                    owner.client for owner in root.owners if owner.client is not None
                ]
            assert len(clients) == (1 if phase == "during" else 0)
            await asyncio.wait_for(speech.wait_for_playout(), 5)
        finally:
            release.set()
            if proxy is not None:
                proxy.request_close()
                await proxy.aclose()
            await session.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()
        assert all(
            owner.startup is None or owner.startup.done() for owner in root.owners
        )
