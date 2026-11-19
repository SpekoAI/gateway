"""Owned SDK/sidecar regressions for the original realtime review boundaries."""

from __future__ import annotations
import asyncio
import base64
import inspect
import json
import pytest
from aiohttp import web
from livekit import rtc
from livekit.agents import Agent, AgentSession, llm
from livekit.agents.voice import io
from test_native_sidecar import LOCAL_TOKEN, MODEL_KEY, native_stack
from test_native_sidecar import native_binary as native_binary  # noqa: PLC0414
from speko_gateway import RelayRealtimeClient
from speko_gateway.livekit import RealtimeModel


def client_model(socket, model_name="gemini-3.8-live"):
    client = RelayRealtimeClient(
        api_key=MODEL_KEY,
        session_id="ses_review147",
        socket_path=socket,
        local_auth_token=LOCAL_TOKEN,
    )
    return client, RealtimeModel(
        client, protocol="google.live.v1", model=model_name, voice="Kore"
    )


async def start(session, **kwargs):
    options = (
        {"record": False}
        if "record" in inspect.signature(session.start).parameters
        else {}
    )
    await session.start(
        agent=Agent(instructions="Owned review control", **kwargs), **options
    )


class Output(io.AudioOutput):
    def __init__(self, *, hold=False):
        options = {"label": "owned-review147"}
        if "capabilities" in inspect.signature(io.AudioOutput).parameters:
            options["capabilities"] = io.AudioOutputCapabilities(pause=False)
        super().__init__(**options)
        self.captured, self.cleared, self.release = (
            asyncio.Event(),
            asyncio.Event(),
            asyncio.Event(),
        )
        self.duration, self.hold = 0, hold
        if not hold:
            self.release.set()

    async def capture_frame(self, frame):
        await super().capture_frame(frame)
        self.duration += frame.duration
        self.captured.set()
        await self.release.wait()

    def flush(self):
        super().flush()
        self.on_playback_finished(playback_position=self.duration, interrupted=False)

    def clear_buffer(self):
        self.cleared.set()
        self.on_playback_finished(playback_position=0, interrupted=True)


async def answer(ws, text="Owned reply"):
    await ws.send_json(
        {
            "serverContent": {
                "modelTurn": {
                    "parts": [
                        {
                            "inlineData": {
                                "mimeType": "audio/pcm;rate=24000",
                                "data": base64.b64encode(b"\x01\x10" * 4800).decode(),
                            }
                        }
                    ]
                },
                "outputTranscription": {"text": text},
            }
        }
    )
    await ws.send_json({"serverContent": {"turnComplete": True}})


@pytest.mark.parametrize(
    "model_name", ["gemini-3.8-live", "gemini-3.8-live-extended-thinking"]
)
async def test_initial_history_is_completed_before_queued_audio(
    native_binary, model_name
):
    received, done = [], asyncio.Event()

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        setup = await ws.receive_json()
        assert setup["setup"]["historyConfig"]["initialHistoryInClientContent"]
        await ws.send_json({"setupComplete": {}})
        received.extend([await ws.receive_json(), await ws.receive_json()])
        done.set()
        async for _ in ws:
            pass
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket, model_name)
        session = model.session()
        try:
            history = llm.ChatContext.empty()
            history.add_message(role="user", content="Original history")
            await session._update_session(chat_ctx=history, tools=[])
            session.push_audio(rtc.AudioFrame(b"\x01\x10" * 480, 16000, 1, 480))
            await asyncio.wait_for(done.wait(), 5)
            assert received[0] == {
                "clientContent": {
                    "turns": [
                        {"role": "user", "parts": [{"text": "Original history"}]}
                    ],
                    "turnComplete": True,
                }
            }
            assert "audio" in received[1]["realtimeInput"]
        finally:
            await session.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize(
    "model_name", ["gemini-3.8-live", "gemini-3.8-live-extended-thinking"]
)
async def test_tool_setup_declares_the_actual_model_behavior(native_binary, model_name):
    configurations = asyncio.Queue()

    @llm.function_tool
    async def lookup(value: str) -> str:
        """Read a synthetic value."""
        return value

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        configurations.put_nowait(await ws.receive_json())
        await ws.send_json({"setupComplete": {}})
        async for _ in ws:
            pass
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket, model_name)
        session = model.session()
        try:
            await session._update_session(tools=[lookup])
            await session.start()
            configuration = await asyncio.wait_for(configurations.get(), 5)
            declaration = configuration["setup"]["tools"][0]["functionDeclarations"][0]
            assert declaration["name"] == "lookup"
            assert declaration.get("behavior", "BLOCKING") == (
                "NON_BLOCKING"
                if model_name.endswith("extended-thinking")
                else "BLOCKING"
            )
        finally:
            await session.aclose()
            await model.aclose()
            await client.aclose()


async def test_real_agent_keeps_each_typed_user_turn_in_provider_context(native_binary):
    requests = asyncio.Queue()

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        turn = []
        async for message in ws:
            command = json.loads(message.data)
            turn.append(command)
            if command.get("clientContent", {}).get("turnComplete"):
                requests.put_nowait(turn)
                turn = []
                await answer(ws)
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        session = AgentSession(
            llm=model, stt=None, tts=None, vad=None, turn_detection="manual"
        )
        session.output.audio = Output()
        try:
            await start(session)
            for text in [
                "First genuine typed request",
                "Second independent typed request",
            ]:
                speech = session.generate_reply(user_input=text)
                commands = await asyncio.wait_for(requests.get(), 5)
                assert [
                    part["text"]
                    for command in commands
                    for turn in command.get("clientContent", {}).get("turns", [])
                    for part in turn["parts"]
                ] == [text], "typed input was omitted or assigned to another reply"
                await asyncio.wait_for(speech.wait_for_playout(), 5)
        finally:
            await session.aclose()
            await model.aclose()
            await client.aclose()


async def test_provider_barge_in_clears_framework_playback_after_native_terminal(
    native_binary,
):
    output = Output(hold=True)

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        await ws.receive_json()
        await answer(ws)
        await output.captured.wait()
        await ws.send_json({"serverContent": {"interrupted": True}})
        async for _ in ws:
            pass
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        session = AgentSession(
            llm=model, stt=None, tts=None, vad=None, turn_detection="manual"
        )
        session.output.audio = output
        try:
            await start(session)
            speech = session.generate_reply()
            await asyncio.wait_for(output.captured.wait(), 5)
            await asyncio.wait_for(output.cleared.wait(), 5)
            assert speech.interrupted
        finally:
            output.release.set()
            await session.aclose()
            await model.aclose()
            await client.aclose()
