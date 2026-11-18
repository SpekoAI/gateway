"""Actual SDK, sidecar and owned peer cancellation; no vendor or cell authority."""

from __future__ import annotations

import asyncio
import base64
import inspect
import json
import struct

import pytest
from aiohttp import web
from livekit import rtc
from livekit.agents import Agent, AgentSession, llm
from livekit.agents.voice import io
from test_native_sidecar import LOCAL_TOKEN, MODEL_KEY, native_stack
from test_native_sidecar import native_binary as native_binary  # noqa: PLC0414

from speko_gateway import GatewayError, RelayRealtimeClient
from speko_gateway.livekit import RealtimeModel


def client_model(socket, *, allow_interruptions=True):
    client = RelayRealtimeClient(
        api_key=MODEL_KEY,
        session_id="ses_cancel_owned",
        socket_path=socket,
        local_auth_token=LOCAL_TOKEN,
    )
    return client, RealtimeModel(
        client,
        protocol="google.live.v1",
        model="gemini-3.8-live",
        voice="Kore",
        allow_interruptions=allow_interruptions,
    )


async def start_agent(session, tools=()):
    options = (
        {"record": False}
        if "record" in inspect.signature(session.start).parameters
        else {}
    )
    agent = Agent(instructions="Synthetic task", tools=list(tools))
    await session.start(agent=agent, **options)
    return agent


async def test_gemini_spoken_context_update_does_not_resend_audio_as_typed_input(
    native_binary,
):
    spoken, received_typed = asyncio.Event(), asyncio.Event()
    received, errors = [], []
    speech_text, typed_text = "Already consumed spoken input", "A genuine typed turn"

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        await ws.send_json(
            {"serverContent": {"inputTranscription": {"text": speech_text}}}
        )
        await ws.send_json({"serverContent": {"turnComplete": True}})
        async for message in ws:
            command = json.loads(message.data)
            received.append(command)
            if typed_text in str(command):
                received_typed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        session = AgentSession(
            llm=model, turn_detection="realtime_llm", vad=None, stt=None, tts=None
        )
        session.on("error", errors.append)
        session.on(
            "conversation_item_added",
            lambda event: (
                spoken.set()
                if isinstance(event.item, llm.ChatMessage)
                and event.item.role == "user"
                and event.item.text_content == speech_text
                else None
            ),
        )
        try:
            agent = await start_agent(session)
            await asyncio.wait_for(spoken.wait(), 5)
            await agent.update_chat_ctx(agent.chat_ctx)
            await agent.update_chat_ctx(agent.chat_ctx)
            context = agent.chat_ctx.copy()
            context.add_message(role="user", content=typed_text)
            await agent.update_chat_ctx(context)
            await asyncio.wait_for(received_typed.wait(), 5)
            assert received == [
                {
                    "clientContent": {
                        "turns": [{"role": "user", "parts": [{"text": typed_text}]}],
                        "turnComplete": False,
                    }
                }
            ]
            assert errors == []
        finally:
            await session.aclose()
            await model.aclose()
            await client.aclose()


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


@pytest.mark.parametrize("manual", [False, True])
@pytest.mark.parametrize("cause", ["provider", "explicit"])
async def test_gemini_barge_in_clears_audio_already_buffered_in_agent_playback(
    native_binary,
    manual,
    cause,
):
    if cause != "provider" and not manual:
        pytest.skip("this regression targets explicit manual-turn cancellation")
    if (
        manual
        and "can_disable_turn_detection"
        not in llm.RealtimeCapabilities.__dataclass_fields__
    ):
        pytest.skip("framework cannot negotiate manual realtime turns")
    audible, cleared, release = (asyncio.Event() for _ in range(3))
    barrier_received, second_turn_received = asyncio.Event(), asyncio.Event()
    terminal = asyncio.Event()
    before_barrier, second_turn = [], []
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
        setup = await ws.receive_json()
        assert (
            setup["setup"]["realtimeInputConfig"]["automaticActivityDetection"][
                "disabled"
            ]
            == manual
        )
        await ws.send_json({"setupComplete": {}})
        if manual:
            assert (await ws.receive_json()) == {"realtimeInput": {"activityStart": {}}}
        assert "audio" in (await ws.receive_json())["realtimeInput"]
        if manual:
            assert (await ws.receive_json()) == {"realtimeInput": {"activityEnd": {}}}
        else:
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
        if cause == "provider":
            await ws.send_json({"serverContent": {"turnComplete": True}})
        await audible.wait()
        if cause == "provider":
            # The native generation has already ended; playback still owns audio.
            await ws.send_json({"serverContent": {"interrupted": True}})
        async for message in ws:
            command = json.loads(message.data)
            if "clientContent" in command:
                barrier_received.set()
                break
            before_barrier.append(command)
            if cause != "provider" and "activityStart" in command.get(
                "realtimeInput", {}
            ):
                # The owned provider stops its ACTIVE generation on the control,
                # then reports interruption. Feedback must not send another one.
                await ws.send_json(
                    {
                        "serverContent": {
                            "interrupted": True,
                            "turnComplete": True,
                            "inputTranscription": {"text": "original spoken words"},
                        }
                    }
                )
        async for message in ws:
            command = json.loads(message.data)
            second_turn.append(command)
            if ("activityEnd" if manual else "audio") in command.get(
                "realtimeInput", {}
            ):
                second_turn_received.set()
                break
        async for _ in ws:
            pass
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        detector = None
        if cause == "external_vad":
            from test_relay_speech_livekit import ControlledVAD

            detector = ControlledVAD()
        agent = AgentSession(
            llm=model,
            turn_detection="vad"
            if cause == "external_vad"
            else "manual"
            if manual
            else "realtime_llm",
            vad=detector,
            stt=None,
            tts=None,
        )
        agent.output.audio = Output()
        agent.on("error", errors.append)
        try:
            current_agent = await start_agent(agent)
            native = next(iter(model._sessions))
            native.on(
                "input_audio_transcription_completed",
                lambda event: terminal.set() if event.is_final else None,
            )
            native.push_audio(rtc.AudioFrame(b"\x01\x10" * 480, 16000, 1, 480))
            native.commit_audio()
            # Manual activityEnd already starts this audio response. A second
            # instructed ask would be uncorrelated with its delayed output.
            speech = (
                agent.generate_reply()
                if manual
                else agent.generate_reply(instructions="Speak the synthetic response")
            )
            await asyncio.wait_for(audible.wait(), 5)
            if cause == "explicit":
                await asyncio.wait_for(agent.interrupt(), 5)
            await asyncio.wait_for(cleared.wait(), 5)
            if cause == "explicit":
                await asyncio.wait_for(terminal.wait(), 5)
            assert captured and len(captured) < 100
            assert speech.interrupted
            await agent.interrupt()
            context = current_agent.chat_ctx.copy()
            context.add_message(role="user", content="Typed synchronization barrier")
            await current_agent.update_chat_ctx(context)
            await asyncio.wait_for(barrier_received.wait(), 5)
            native.push_audio(rtc.AudioFrame(b"\x02\x10" * 480, 16000, 1, 480))
            native.commit_audio()
            await asyncio.wait_for(second_turn_received.wait(), 5)
            assert before_barrier == (
                [
                    {"realtimeInput": {"activityStart": {}}},
                    {"realtimeInput": {"activityEnd": {}}},
                ]
                if cause == "explicit"
                else []
            ), (
                "explicit cancellation must reach the provider; feedback must not reopen activity"
            )
            if manual:
                assert len(second_turn) == 3
                assert second_turn[0] == {"realtimeInput": {"activityStart": {}}}
                assert "audio" in second_turn[-2]["realtimeInput"]
                assert second_turn[-1] == {"realtimeInput": {"activityEnd": {}}}
            else:
                assert (
                    len(second_turn) == 1 and "audio" in second_turn[0]["realtimeInput"]
                )
            assert errors == []
        finally:
            release.set()
            await agent.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("reason", ["disabled", "input_active"])
async def test_gemini_manual_interrupt_closes_transport_when_control_is_unavailable(
    native_binary, reason
):
    if (
        "can_disable_turn_detection"
        not in llm.RealtimeCapabilities.__dataclass_fields__
    ):
        pytest.skip("framework cannot negotiate manual realtime turns")
    closed = asyncio.Event()
    received, errors = [], []

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        if reason == "input_active":
            received.append(await ws.receive_json())
        if reason != "input_active":
            await ws.receive_json()  # The explicit reply request.
        await ws.send_json(
            {"serverContent": {"outputTranscription": {"text": "Active reply"}}}
        )
        async for message in ws:
            received.append(json.loads(message.data))
        closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket, allow_interruptions=reason != "disabled")
        session = model.session(turn_detection_disabled=True)
        session.on("error", errors.append)
        try:
            await session._update_session(instructions="Synthetic task", tools=[])
            await session.start()
            generations = asyncio.Queue()
            session.on("generation_created", generations.put_nowait)
            if reason == "input_active":
                session.start_user_activity()
                await asyncio.wait_for(generations.get(), 5)
            else:
                await asyncio.wait_for(session.generate_reply(instructions="Reply"), 5)
            session.interrupt()
            await asyncio.wait_for(closed.wait(), 5)
            await asyncio.wait_for(asyncio.shield(session._task), 5)
            assert len(errors) == 1 and not errors[0].recoverable
            assert received == (
                [{"realtimeInput": {"activityStart": {}}}]
                if reason == "input_active"
                else []
            ), "unavailable cancellation must close I/O without faking a caller turn"
            assert session._generation.interrupted
        finally:
            await session.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("turn_mode", ["vad", "stt"])
async def test_gemini_async_turn_modes_refuse_before_native_input_can_split(
    native_binary, turn_mode
):
    from test_relay_speech_livekit import ControlledVAD

    opened = []

    async def peer(request):
        opened.append(request.path)
        return web.Response(status=500)

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        session = AgentSession(
            llm=model, turn_detection=turn_mode, vad=ControlledVAD(), stt=None, tts=None
        )
        try:
            with pytest.raises(GatewayError, match="correlated.*capture"):
                await start_agent(session)
            assert opened == [] and not client._attempted
        finally:
            await session.aclose()
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


async def test_gemini_resampled_onset_and_remainder_keep_one_complete_activity(
    native_binary,
):
    done = asyncio.Event()
    received, errors = [], []

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        assert await ws.receive_json() == {"realtimeInput": {"activityStart": {}}}
        async for message in ws:
            command = json.loads(message.data)["realtimeInput"]
            if "activityEnd" in command:
                done.set()
                break
            assert "audio" in command
            received.append(base64.b64decode(command["audio"]["data"]))
        async for _ in ws:
            pass
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        session = model.session(turn_detection_disabled=True)
        session.on("error", errors.append)
        try:
            await session._update_session(tools=[])
            await session.start()
            session.push_audio(rtc.AudioFrame(b"\x00\x10" * 14400, 48000, 1, 14400))
            session.start_user_activity()
            session.push_audio(rtc.AudioFrame(b"\x00\x50" * 14400, 48000, 1, 14400))
            session.commit_audio()
            await asyncio.wait_for(done.wait(), 5)
            samples = [
                value[0] for value in struct.iter_unpack("<h", b"".join(received))
            ]
            assert len(samples) == 9600, (
                "source onset/remainder samples were lost or repeated"
            )
            assert max(abs(value - 0x1000) for value in samples[500:4000]) <= 2
            assert max(abs(value - 0x5000) for value in samples[5300:9000]) <= 2
            assert errors == []
        finally:
            await session.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("kind", ["idle_then_speech", "long_manual_speech"])
@pytest.mark.parametrize("manual", [True, False])
async def test_gemini_raw_manual_and_connected_provider_vad_keep_idle_and_long_input(
    native_binary, kind, manual
):
    if (
        manual
        and "can_disable_turn_detection"
        not in llm.RealtimeCapabilities.__dataclass_fields__
    ):
        pytest.skip("SDK cannot negotiate explicit manual AgentSession turns")
    from test_relay_speech_livekit import OwnedAudioInput

    audio = OwnedAudioInput()
    received, errors, opened = bytearray(), [], []
    complete, closed, ended = asyncio.Event(), asyncio.Event(), asyncio.Event()
    silence = bytes(640)
    speech = b"\x04\x20" * 320
    frames = (
        ([silence] * 101 + [speech] * 10)
        if kind == "idle_then_speech"
        else [speech] * 131
    )
    expected = b"".join(frames)

    async def peer(request):
        opened.append(request.path)
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        setup = await ws.receive_json()
        assert (
            setup["setup"]["realtimeInputConfig"]["automaticActivityDetection"][
                "disabled"
            ]
            is manual
        )
        await ws.send_json({"setupComplete": {}})
        if manual:
            assert await ws.receive_json() == {"realtimeInput": {"activityStart": {}}}
        async for msg in ws:
            control = json.loads(msg.data)["realtimeInput"]
            if "activityEnd" in control:
                ended.set()
                break
            received.extend(base64.b64decode(control["audio"]["data"]))
            if len(received) == len(expected):
                complete.set()
                if not manual:
                    ended.set()
        async for _ in ws:
            pass
        closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        session = AgentSession(
            llm=model,
            turn_detection="manual" if manual else "realtime_llm",
            stt=None,
            tts=None,
            vad=None,
        )
        if not manual:
            session.input.audio = audio
        session.on("error", errors.append)
        try:
            await start_agent(session)
            native = next(iter(model._sessions))
            await native.start()
            for raw in frames:
                if manual:
                    native.push_audio(rtc.AudioFrame(raw, 16000, 1, 320))
                else:
                    audio.frames.put_nowait(rtc.AudioFrame(raw, 16000, 1, 320))
                await asyncio.sleep(0.02 if kind == "idle_then_speech" else 0)
                if closed.is_set():
                    break
            completed = asyncio.create_task(complete.wait())
            failed = asyncio.create_task(closed.wait())
            try:
                await asyncio.wait_for(
                    asyncio.wait(
                        [completed, failed], return_when=asyncio.FIRST_COMPLETED
                    ),
                    5,
                )
                assert complete.is_set() and not closed.is_set(), (
                    "healthy input closed before its explicit commit"
                )
            finally:
                completed.cancel()
                failed.cancel()
                await asyncio.gather(completed, failed, return_exceptions=True)
            native.commit_audio()
            await asyncio.wait_for(ended.wait(), 5)
            assert bytes(received) == expected and len(opened) == 1 and errors == []
            assert native._failure is None
        finally:
            audio.frames.put_nowait(None)
            await session.aclose()
            await model.aclose()
            await client.aclose()


async def test_gemini_async_vad_without_correlated_capture_refuses_before_open(
    native_binary,
):
    from test_relay_speech_livekit import ControlledVAD

    opened = []

    async def peer(request):
        opened.append(request.path)
        return web.Response(status=500)

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        session = AgentSession(
            llm=model, turn_detection="vad", vad=ControlledVAD(), stt=None, tts=None
        )
        try:
            with pytest.raises(GatewayError, match="correlated.*capture"):
                await start_agent(session)
            assert opened == [] and not client._attempted
        finally:
            await session.aclose()
            await model.aclose()
            await client.aclose()


async def test_gemini_uncommitted_manual_input_refuses_reply_without_splitting_turn(
    native_binary,
):
    received, opened = bytearray(), []
    onset_received, end_received = asyncio.Event(), asyncio.Event()
    onset, remainder = b"\x03\x12" * 320, b"\x06\x41" * 640
    controls = []

    async def peer(request):
        opened.append(request.path)
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        async for message in ws:
            command = json.loads(message.data)
            if "clientContent" in command:
                assert end_received.is_set(), (
                    "reply entered an uncommitted physical turn"
                )
                await ws.send_json(
                    {"serverContent": {"outputTranscription": {"text": "Next reply"}}}
                )
                await ws.send_json({"serverContent": {"turnComplete": True}})
                continue
            control = command["realtimeInput"]
            if "audio" in control:
                received.extend(base64.b64decode(control["audio"]["data"]))
                onset_received.set()
            else:
                controls.append(control)
                if "activityEnd" in control:
                    end_received.set()
                    await ws.send_json(
                        {
                            "serverContent": {
                                "outputTranscription": {"text": "Committed audio reply"}
                            }
                        }
                    )
                    await ws.send_json({"serverContent": {"turnComplete": True}})
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        native = model.session(turn_detection_disabled=True)
        try:
            await native._update_session(tools=[])
            await native.start()
            native.push_audio(rtc.AudioFrame(onset, 16000, 1, 320))
            await asyncio.wait_for(onset_received.wait(), 5)
            with pytest.raises(llm.RealtimeError, match="Commit manual input"):
                await native.generate_reply(instructions="Do not split the caller")
            native.start_user_activity()  # Idempotent, never rolls the physical activity.
            native.push_audio(rtc.AudioFrame(remainder, 16000, 1, 640))
            native.commit_audio()
            original_reply = native.generate_reply()
            await asyncio.wait_for(end_received.wait(), 5)
            from test_native_realtime_livekit import collect_generation

            generation = await asyncio.wait_for(original_reply, 5)
            messages, calls = await asyncio.wait_for(collect_generation(generation), 5)
            assert messages == [["Committed audio reply", b""]] and not calls
            generation = await asyncio.wait_for(
                native.generate_reply(
                    instructions="Reply after the committed response"
                ),
                5,
            )
            messages, calls = await asyncio.wait_for(collect_generation(generation), 5)
            assert messages == [["Next reply", b""]] and not calls
            assert bytes(received) == onset + remainder
            assert controls == [{"activityStart": {}}, {"activityEnd": {}}]
            assert len(opened) == 1 and native._failure is None
        finally:
            closing = asyncio.create_task(native.aclose())
            try:
                done, _ = await asyncio.wait({closing}, timeout=2)
                assert closing in done, (
                    "native close joined before stopping its physical reader"
                )
            finally:
                if native._socket is not None:
                    native._socket.abort()
                await asyncio.gather(closing, return_exceptions=True)
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize(
    "failure", ["queued_bytes", "oversize_frame", "forwarded_clear"]
)
async def test_gemini_input_loss_or_backpressure_closes_one_owned_transport(
    native_binary, monkeypatch, failure
):
    closed, read_audio, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    opened, errors = [], []

    async def peer(request):
        opened.append(request.path)
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        async for message in ws:
            if "audio" in json.loads(message.data).get("realtimeInput", {}):
                read_audio.set()
        closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        native = model.session(turn_detection_disabled=True)
        native.on("error", errors.append)
        try:
            await native._update_session(tools=[])
            await native.start()
            if failure == "queued_bytes":
                original_send = native._socket.send

                async def paused_send(command):
                    await release.wait()
                    await original_send(command)

                monkeypatch.setattr(native._socket, "send", paused_send)
                # Fewer than 256 frames, but more than the physical byte budget.
                for _ in range(40):
                    native.push_audio(rtc.AudioFrame(bytes(64000), 16000, 1, 32000))
                    if native._failure is not None:
                        break
            elif failure == "oversize_frame":
                native.push_audio(rtc.AudioFrame(bytes(2 << 20), 16000, 1, 1 << 20))
            else:
                native.push_audio(rtc.AudioFrame(bytes(640), 16000, 1, 320))
                await asyncio.wait_for(read_audio.wait(), 5)
                native.clear_audio()
            await asyncio.wait_for(closed.wait(), 5)
            await asyncio.wait_for(asyncio.shield(native._task), 5)
            assert native._failure is not None and not native._failure.retryable
            assert len(errors) == 1 and not errors[0].recoverable
            with pytest.raises(GatewayError, match="exhausted"):
                await client.open(protocol="google.live.v1", model="gemini-3.8-live")
            assert len(opened) == 1
        finally:
            release.set()
            await native.aclose()
            await model.aclose()
            await client.aclose()


async def test_gemini_sdk_close_joins_with_its_event_consumer_paused(
    native_binary, monkeypatch
):
    from speko_gateway.relay_realtime import RelayRealtimeSession

    paused, closed, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    opened = []
    original_events = RelayRealtimeSession.events

    async def paused_events(transport):
        events = original_events(transport)
        try:
            yield await anext(events)  # Actual provider setupComplete.
            paused.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                # A paused/lost cancellation must not own physical shutdown.
                pass
            await closed.wait()
        finally:
            await events.aclose()

    monkeypatch.setattr(RelayRealtimeSession, "events", paused_events)

    async def peer(request):
        opened.append(request.path)
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        async for _ in ws:
            pass
        closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        native = model.session(turn_detection_disabled=True)
        closing = None
        try:
            await native._update_session(tools=[])
            await native.start()
            await asyncio.wait_for(paused.wait(), 5)
            closing = asyncio.create_task(native.aclose())
            done, _ = await asyncio.wait({closing}, timeout=2)
            assert closed.is_set() and closing in done, (
                "paused consumer retained physical I/O during SDK close"
            )
            assert native._task.done() and len(opened) == 1
        finally:
            release.set()
            if native._socket is not None:
                native._socket.abort()
            if closing is not None:
                await asyncio.gather(closing, return_exceptions=True)
            await native.aclose()
            await model.aclose()
            await client.aclose()
