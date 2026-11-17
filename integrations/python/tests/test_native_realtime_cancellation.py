"""Actual SDK, sidecar and owned peer cancellation; no vendor or cell authority."""

from __future__ import annotations

import asyncio
import base64
import inspect

import pytest
from aiohttp import web
from livekit import rtc
from livekit.agents import Agent, AgentSession, llm
from livekit.agents.voice import io
from test_native_sidecar import LOCAL_TOKEN, MODEL_KEY, native_stack
from test_native_sidecar import native_binary as native_binary  # noqa: PLC0414

from speko_gateway import GatewayError, RelayRealtimeClient
from speko_gateway.livekit import RealtimeModel


def client_model(socket):
    client = RelayRealtimeClient(
        api_key=MODEL_KEY,
        session_id="ses_cancel_owned",
        socket_path=socket,
        local_auth_token=LOCAL_TOKEN,
    )
    return client, RealtimeModel(
        client, protocol="google.live.v1", model="gemini-3.8-live", voice="Kore"
    )


async def start_agent(session, tools=()):
    options = (
        {"record": False}
        if "record" in inspect.signature(session.start).parameters
        else {}
    )
    await session.start(
        agent=Agent(instructions="Synthetic task", tools=list(tools)), **options
    )


@pytest.mark.parametrize(
    "model_name", ["gemini-3.8-live", "gemini-3.8-live-extended-thinking"]
)
async def test_gemini_finishes_initial_history_before_live_audio(
    native_binary, model_name
):
    received, errors = [], []
    done = asyncio.Event()

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        setup = await ws.receive_json()
        assert setup["setup"]["historyConfig"] == {
            "initialHistoryInClientContent": True
        }
        assert setup["setup"]["tools"] == []
        await ws.send_json({"setupComplete": {}})
        received.append(await ws.receive_json())
        assert received[-1] == {
            "clientContent": {
                "turns": [
                    {"role": "user", "parts": [{"text": "Prior synthetic context"}]}
                ],
                "turnComplete": True,
            }
        }
        received.append(await ws.receive_json())
        assert "audio" in received[-1]["realtimeInput"]
        done.set()
        async for _ in ws:
            pass
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, _ = client_model(socket)
        model = RealtimeModel(
            client, protocol="google.live.v1", model=model_name, voice="Kore"
        )
        session = model.session()
        session.on("error", errors.append)
        try:
            history = llm.ChatContext.empty()
            history.add_message(role="user", content="Prior synthetic context")
            await session._update_session(chat_ctx=history, tools=[])
            session.push_audio(rtc.AudioFrame(b"\x01\x10" * 480, 16000, 1, 480))
            await asyncio.wait_for(done.wait(), 5)
            assert len(received) == 2 and errors == []
        finally:
            await session.aclose()
            await model.aclose()
            await client.aclose()


async def test_gemini_barge_in_clears_audio_already_buffered_in_agent_playback(
    native_binary,
):
    audible, cleared, release = (asyncio.Event() for _ in range(3))
    captured, errors = [], []

    class Output(io.AudioOutput):
        def __init__(self):
            options = {"label": "owned-interrupted-playback"}
            if "capabilities" in inspect.signature(io.AudioOutput).parameters:
                options["capabilities"] = io.AudioOutputCapabilities(pause=False)
            super().__init__(**options)

        async def capture_frame(self, frame):
            await super().capture_frame(frame)
            captured.append(bytes(frame.data))
            audible.set()
            await release.wait()

        def flush(self):
            super().flush()

        def clear_buffer(self):
            cleared.set()
            self.on_playback_finished(playback_position=0, interrupted=True)

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        await ws.receive_json()
        await ws.send_json(
            {
                "serverContent": {
                    "modelTurn": {
                        "parts": [
                            {
                                "inlineData": {
                                    "mimeType": "audio/pcm;rate=24000",
                                    "data": base64.b64encode(
                                        b"\x01\x10" * 48000
                                    ).decode(),
                                }
                            }
                        ]
                    }
                }
            }
        )
        await ws.send_json({"serverContent": {"turnComplete": True}})
        await audible.wait()
        # The native generation has already ended; playback still owns audio.
        await ws.send_json({"serverContent": {"interrupted": True}})
        async for _ in ws:
            pass
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        agent = AgentSession(
            llm=model, turn_detection="manual", vad=None, stt=None, tts=None
        )
        agent.output.audio = Output()
        agent.on("error", errors.append)
        try:
            await start_agent(agent)
            speech = agent.generate_reply(instructions="Speak the synthetic response")
            await asyncio.wait_for(audible.wait(), 5)
            await asyncio.wait_for(cleared.wait(), 5)
            assert captured and len(captured) < 100
            assert speech.interrupted
            assert errors == []
        finally:
            release.set()
            await agent.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize(
    "model_name", ["gemini-3.8-live", "gemini-3.8-live-extended-thinking"]
)
async def test_gemini_tools_refuse_before_socket_and_cannot_start_side_effects(
    native_binary, model_name
):
    opened, effects = [], []

    @llm.function_tool
    async def lookup(value: str) -> str:
        """Read a synthetic value."""
        effects.append(value)
        return value

    async def peer(request):
        opened.append(request.path)
        return web.Response(status=500)

    async with native_stack(native_binary, peer) as (socket, _):
        client, _ = client_model(socket)
        model = RealtimeModel(
            client, protocol="google.live.v1", model=model_name, voice="Kore"
        )
        session = model.session()
        try:
            with pytest.raises(
                llm.RealtimeError, match="tool cancellation is unavailable"
            ):
                await session._update_session(tools=[lookup])
            # A framework may treat update failures as best effort. The same
            # session stays fenced even if it subsequently tries to start.
            with pytest.raises(GatewayError, match="tool cancellation is unavailable"):
                await session.start()
            assert effects == [] and opened == []
        finally:
            await session.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("stop", ["abort", "close"])
async def test_native_socket_abort_closes_paused_reader_and_prevents_redial(
    native_binary,
    stop,
):
    closed = asyncio.Event()
    opened = []
    close_codes = []

    async def peer(request):
        opened.append(request.path)
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.send_json({"setupComplete": {}})
        async for _ in ws:
            pass
        close_codes.append(ws.close_code)
        closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, _ = client_model(socket)
        session = await client.open(protocol="google.live.v1", model="gemini-3.8-live")
        events = session.events()
        try:
            await anext(events)
            # Leave the async generator paused at its yield. Physical close is
            # independent of both that consumer and any SDK output playback.
            if stop == "abort":
                session.abort()
            await asyncio.wait_for(session.aclose(), 2)
            await asyncio.wait_for(closed.wait(), 5)
            if stop == "close":
                assert close_codes == [1000]
            with pytest.raises(GatewayError, match="exhausted"):
                await client.open(protocol="google.live.v1", model="gemini-3.8-live")
            with pytest.raises(GatewayError, match="closed"):
                await session.send_json({"clientContent": {"turnComplete": True}})
            assert len(opened) == 1
        finally:
            await events.aclose()
            await client.aclose()
