"""GPT-Live has no per-ask identity: refuse scoped tasks before installation."""

from __future__ import annotations

import asyncio
import base64
import json

import pytest
from aiohttp import web
from livekit import rtc
from livekit.agents import AgentSession, llm
from test_native_gpt_live import models, response
from test_native_realtime_boundaries import either
from test_native_realtime_cancellation import start_agent
from test_native_sidecar import native_binary as native_binary  # noqa: PLC0414
from test_native_sidecar import native_stack
from test_relay_speech_livekit import OwnedAudioInput

pytestmark = pytest.mark.skipif(
    not hasattr(llm, "DuplexModel"), reason="SDK predates duplex"
)


def delegation(identity):
    return {
        "type": "session.delegation.created",
        "offset_ms": 100,
        "delegation": {"id": identity, "target": "client"},
    }


async def test_gpt_claimed_request_cannot_enable_a_later_scoped_ask(native_binary):
    requests, commands = [], []
    done = asyncio.Event()
    marker = "SECOND_TASK_MUST_NOT_BE_INSTALLED"

    async def peer(request):
        if request.path == "/v1/llm/responses":
            requests.append(await request.json())
            return response(
                [
                    ("response.text.delta", {"delta": "ordinary answer"}),
                    ("response.completed", {}),
                ]
            )
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"type": "session.started"})
        first = await ws.receive_json()
        commands.append(first)
        await ws.send_json(delegation("claimed_first"))
        answer = await ws.receive_json()
        commands.append(answer)
        assert answer["delegation_id"] == "claimed_first"
        await ws.send_json(
            {
                "type": "session.output_audio.delta",
                "delta": base64.b64encode(b"\x01\x10" * 2400).decode(),
            }
        )
        async for message in ws:
            command = json.loads(message.data)
            commands.append(command)
            if command["type"] == "session.input_audio.append":
                break
        # Equal offsets and distinct unseen IDs carry no new-ask attribution.
        for identity in ("late_old_after_claim", "new_ordinary"):
            await ws.send_json(delegation(identity))
            answer = await ws.receive_json()
            commands.append(answer)
            assert answer["delegation_id"] == identity
        done.set()
        async for message in ws:
            command = json.loads(message.data)
            commands.append(command)
            if command["type"] == "session.close":
                await ws.send_json({"type": "session.closed"})
                await ws.close()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        native = model.session()
        scoped = None
        try:
            await native._update_session(
                instructions="Permanent canonical prompt", tools=[]
            )
            await native.start()
            # An unscoped voice delivery hint adds no task/permissions to a backend request.
            await asyncio.wait_for(native.generate_reply(), 5)
            scoped = native.generate_reply(instructions=marker)
            native.push_audio(rtc.AudioFrame(bytes(960), 24000, 1, 480))
            await asyncio.wait_for(done.wait(), 5)
            assert (
                scoped.done()
                and isinstance(scoped.exception(), llm.RealtimeError)
                and marker not in json.dumps(commands)
            ), (
                "claimed prior request admitted an uncorrelated scoped ask on the physical voice connection"
            )
            assert len(requests) == 3 and all(
                marker not in json.dumps(body["input"]) for body in requests
            )
            assert all(body["max_output_tokens"] == 123 for body in requests)
            assert native.duplex_session._failure is None
        finally:
            if scoped is not None:
                if not scoped.done():
                    scoped.cancel()
                elif not scoped.cancelled():
                    scoped.exception()
            await native.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()


async def test_gpt_idle_microphone_after_rejected_scoped_ask_keeps_ordinary_flow(
    native_binary,
):
    ask_seen, failed_before_backend, done = (
        asyncio.Event(),
        asyncio.Event(),
        asyncio.Event(),
    )
    requests, commands, backend_before_failure, microphones = [], [], [], []
    marker = "INITIAL_PRIVATE_TASK_MUST_FAIL_BEFORE_BACKEND"

    async def peer(request):
        if request.path == "/v1/llm/responses":
            backend_before_failure.append(not failed_before_backend.is_set())
            requests.append(await request.json())
            return response(
                [
                    ("response.text.delta", {"delta": "ordinary answer"}),
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
            if (
                command["type"] == "session.commentary.append"
                and command.get("delegation_id") is None
            ):
                ask_seen.set()
            elif command["type"] == "session.input_audio.append":
                microphones.append(base64.b64decode(command["audio"]))
                await ws.send_json(delegation("ordinary_after_idle"))
            elif command["type"] == "session.commentary.append":
                done.set()
            elif command["type"] == "session.close":
                await ws.send_json({"type": "session.closed"})
                await ws.close()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        audio = OwnedAudioInput()
        actor = AgentSession(
            llm=model, turn_detection="realtime_llm", stt=None, tts=None, vad=None
        )
        actor.input.audio = audio
        finished = None
        try:
            await start_agent(actor)
            speech = actor.generate_reply(instructions=marker)

            async def observe_failure():
                try:
                    await speech.wait_for_playout()
                finally:
                    failed_before_backend.set()

            finished = asyncio.create_task(observe_failure())
            await either(ask_seen, failed_before_backend)
            # PCM is deliberately silence; no sample/time speech classification is made.
            audio.frames.put_nowait(rtc.AudioFrame(bytes(960), 24000, 1, 480))
            await asyncio.wait_for(done.wait(), 5)
            assert (
                failed_before_backend.is_set()
                and not ask_seen.is_set()
                and marker not in json.dumps(commands)
            ), (
                "idle microphone arrived after an admitted scoped task instead of a pre-install refusal"
            )
            assert backend_before_failure == [False] and len(requests) == 1
            assert marker not in json.dumps(requests[0]["input"])
            assert microphones == [bytes(960)]
            assert isinstance(speech.exception(), llm.RealtimeError)
        finally:
            audio.frames.put_nowait(None)
            if finished is not None:
                finished.cancel()
                await asyncio.gather(finished, return_exceptions=True)
            await actor.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()
