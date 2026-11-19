"""Owned SDK/sidecar regressions for the original realtime review boundaries."""

from __future__ import annotations

import asyncio
import base64
import inspect
import json
from array import array

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
            history.items[0].content[0] = "Late caller-owned mutation"
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


@pytest.mark.parametrize("operation", ["typed", "clear"])
async def test_unconverted_source_audio_is_owned_before_resampling(
    native_binary, operation
):
    received, closed, complete = [], asyncio.Event(), asyncio.Event()

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        async for message in ws:
            command = json.loads(message.data)
            received.append(command)
            if "activityEnd" in command.get("realtimeInput", {}):
                complete.set()
        closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        session = model.session(turn_detection_disabled=True)
        try:
            await session._update_session(tools=[])
            await session.start()
            first, second = (
                rtc.AudioFrame(b"\x01\x70" * 480, 48000, 1, 480),
                rtc.AudioFrame(b"\x02\x90" * 480, 48000, 1, 480),
            )
            session.push_audio(first)
            assert not received, (
                "this control requires a frame still held inside conversion"
            )
            if operation == "typed":
                from speko_gateway import GatewayError

                context = session.chat_ctx
                context.add_message(role="user", content="Cannot overtake captured PCM")
                with pytest.raises(GatewayError, match="active input"):
                    await session.update_chat_ctx(context)
                await asyncio.wait_for(closed.wait(), 5)
                assert not any("clientContent" in command for command in received)
            else:
                session.clear_audio()
                session.push_audio(second)
                session.commit_audio()
                await asyncio.wait_for(complete.wait(), 5)
                actual = b"".join(
                    base64.b64decode(command["realtimeInput"]["audio"]["data"])
                    for command in received
                    if "audio" in command.get("realtimeInput", {})
                )
                converter = rtc.AudioResampler(
                    input_rate=48000, output_rate=16000, num_channels=1
                )
                expected = b"".join(
                    bytes(frame.data)
                    for frame in [*converter.push(second), *converter.flush()]
                )
                # Independent native resamplers use dither, so their PCM can
                # differ by one LSB. Opposite-polarity source frames make old
                # input leakage far larger than that numerical noise.
                assert len(actual) == len(expected) and actual
                assert (
                    max(
                        abs(a - b)
                        for a, b in zip(
                            array("h", actual), array("h", expected), strict=True
                        )
                    )
                    <= 2
                ), "cleared original samples escaped into the later commit"
        finally:
            await session.aclose()
            await model.aclose()
            await client.aclose()


async def test_independent_stop_inside_provider_feedback_physically_joins(
    native_binary,
):
    ready, closed = asyncio.Event(), asyncio.Event()

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        await ready.wait()
        await ws.receive_json()
        await ws.send_json(
            {"serverContent": {"outputTranscription": {"text": "Active provider work"}}}
        )
        await ws.send_json({"serverContent": {"interrupted": True}})
        async for _ in ws:
            pass
        closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        session = model.session(turn_detection_disabled=True)
        try:
            await session._update_session(tools=[])
            await session.start()
            session.on("input_speech_started", lambda _: session.interrupt())
            ready.set()
            reply = session.generate_reply()
            await asyncio.wait_for(closed.wait(), 5)
            await asyncio.wait_for(asyncio.shield(session._task), 5)
            await asyncio.gather(reply, return_exceptions=True)
            assert session._generation.interrupted and session._failure is not None
        finally:
            ready.set()
            await session.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("stop", ["abort", "close"])
async def test_physical_close_joins_a_paused_native_reader(native_binary, stop):
    closed = asyncio.Event()

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.send_json({"setupComplete": {}})
        async for _ in ws:
            pass
        closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        stream = await client.open(protocol="google.live.v1", model="gemini-3.8-live")
        events = stream.events()
        try:
            assert json.loads(await anext(events)) == {"setupComplete": {}}
            if stop == "abort":
                stream.abort()
                await asyncio.wait_for(closed.wait(), 5)
            await asyncio.wait_for(stream.aclose(), 2)
            await asyncio.wait_for(closed.wait(), 5)
            from speko_gateway import GatewayError

            with pytest.raises(GatewayError, match="exhausted"):
                await client.open(protocol="google.live.v1", model="gemini-3.8-live")
        finally:
            await events.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.skipif(
    not hasattr(llm, "DuplexModel"), reason="SDK lacks its public duplex interface"
)
async def test_healthy_gpt_session_preserves_two_successive_application_replies(
    native_binary,
):
    from test_native_gpt_live import models
    from test_native_realtime_livekit import collect_generation

    requests = []
    voices = [b"\x01\x10" * 2400, b"\x02\x20" * 2400]

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json(
            {"type": "session.started", "session": {"id": "owned-repeat-session"}}
        )
        async for message in ws:
            command = json.loads(message.data)
            if command["type"] == "session.close":
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
                break
            requests.append(command)
            assert (
                command["type"] == "session.commentary.append"
                and command["delegation_id"] is None
            )
            payload = voices[len(requests) - 1]
            await ws.send_json(
                {
                    "type": "session.output_audio.delta",
                    "delta": base64.b64encode(payload + b"\0\0" * 24000).decode(),
                }
            )
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        session = model.session()
        try:
            await session._update_session(chat_ctx=llm.ChatContext.empty(), tools=[])
            await session.duplex_session.start()
            for expected in voices:
                generation = await asyncio.wait_for(session.generate_reply(), 5)
                messages, functions = await asyncio.wait_for(
                    collect_generation(generation), 5
                )
                assert generation.user_initiated and functions == []
                pcm = b"".join(message[1] for message in messages)
                assert pcm.startswith(expected) and not pcm[len(expected) :].strip(
                    b"\0"
                )
            assert len(requests) == 2
            # This is a healthy SDK segmentation control, not proof that silence
            # drained a different or delayed original provider operation.
        finally:
            await session.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()
