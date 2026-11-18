"""Actual SDK, sidecar and owned peer cancellation; no vendor or cell authority."""

from __future__ import annotations

import asyncio
import base64
import inspect
import json

import pytest
from aiohttp import web
from livekit import rtc
from livekit.agents import Agent, AgentSession, llm, vad
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
            llm=model, turn_detection="manual", vad=None, stt=None, tts=None
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
@pytest.mark.parametrize("cause", ["provider", "explicit", "external_vad"])
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
                    {"serverContent": {"interrupted": True, "turnComplete": True}}
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
            native.push_audio(rtc.AudioFrame(b"\x01\x10" * 480, 16000, 1, 480))
            native.commit_audio()
            speech = agent.generate_reply(instructions="Speak the synthetic response")
            await asyncio.wait_for(audible.wait(), 5)
            if cause == "explicit":
                await asyncio.wait_for(agent.interrupt(), 5)
            elif cause == "external_vad":
                # Exercise LiveKit's real external-VAD callback: it calls
                # start_user_activity THEN interrupt while speech is active.
                current_agent._activity.on_vad_inference_done(
                    vad.VADEvent(
                        type=vad.VADEventType.INFERENCE_DONE,
                        samples_index=16000,
                        timestamp=1,
                        speech_duration=1,
                        silence_duration=0,
                        speaking=True,
                    )
                )
                assert native._audio_active and native._generation.interrupted
                assert native._failure is None, "genuine caller barge-in aborted I/O"
            await asyncio.wait_for(cleared.wait(), 5)
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
                else [{"realtimeInput": {"activityStart": {}}}]
                if cause == "external_vad"
                else []
            ), (
                "explicit cancellation must reach the provider; feedback must not reopen activity"
            )
            if manual:
                expected = 2 if cause == "external_vad" else 3
                assert len(second_turn) == expected
                if cause != "external_vad":
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
            if reason == "input_active":
                session.start_user_activity()
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


@pytest.mark.parametrize(
    "continuous_pcm", [b"\x00\x00", b"\x03\x10"], ids=["silence", "speech"]
)
async def test_gemini_continuous_input_before_response_preserves_real_vad_barge_in(
    native_binary, continuous_pcm
):
    if (
        "can_disable_turn_detection"
        not in llm.RealtimeCapabilities.__dataclass_fields__
    ):
        pytest.skip("framework cannot negotiate manual realtime turns")
    from test_relay_speech_livekit import ControlledVAD, OwnedAudioInput

    detector, audio = ControlledVAD(), OwnedAudioInput()
    initial_seen, continuous_seen, audible, cleared = (
        asyncio.Event() for _ in range(4)
    )
    reply_seen = asyncio.Event()
    provider_cancelled, caller_seen, turn_ended, synchronized, next_seen = (
        asyncio.Event() for _ in range(5)
    )
    release_output = asyncio.Event()
    next_audio = asyncio.Event()
    opened, controls, following, errors = [], [], [], []
    caller_pcm, next_pcm = b"\x04\x10" * 480, b"\x05\x10" * 480

    class Output(io.AudioOutput):
        def __init__(self):
            options = {"label": "owned-continuous-playback"}
            if "capabilities" in inspect.signature(io.AudioOutput).parameters:
                options["capabilities"] = io.AudioOutputCapabilities(pause=False)
            super().__init__(**options)

        async def capture_frame(self, frame):
            await super().capture_frame(frame)
            audible.set()
            await release_output.wait()

        def flush(self):
            super().flush()

        def clear_buffer(self):
            cleared.set()
            self.on_playback_finished(playback_position=0, interrupted=True)

    async def peer(request):
        opened.append(request.path)
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        setup = await ws.receive_json()
        assert setup["setup"]["realtimeInputConfig"]["activityHandling"] == (
            "START_OF_ACTIVITY_INTERRUPTS"
        )
        await ws.send_json({"setupComplete": {}})
        assert await ws.receive_json() == {"realtimeInput": {"activityStart": {}}}
        assert "audio" in (await ws.receive_json())["realtimeInput"]
        initial_seen.set()
        assert await ws.receive_json() == {"realtimeInput": {"activityEnd": {}}}
        assert "clientContent" in await ws.receive_json()
        reply_seen.set()
        # Real continuous input reopens activity BEFORE this response exists.
        assert await ws.receive_json() == {"realtimeInput": {"activityStart": {}}}
        idle = await ws.receive_json()
        assert base64.b64decode(idle["realtimeInput"]["audio"]["data"]) == (
            continuous_pcm * 480
        )
        continuous_seen.set()
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
        # A start from before generation cannot cancel it. Require a NEW,
        # balanced real boundary before acknowledging provider cancellation.
        for expected in ("activityEnd", "activityStart"):
            command = await ws.receive_json()
            controls.append(command)
            assert command == {"realtimeInput": {expected: {}}}
        provider_cancelled.set()
        await ws.send_json(
            {"serverContent": {"interrupted": True, "turnComplete": True}}
        )
        command = await ws.receive_json()
        controls.append(command)
        assert base64.b64decode(command["realtimeInput"]["audio"]["data"]) == caller_pcm
        caller_seen.set()
        command = await ws.receive_json()
        controls.append(command)
        assert command == {"realtimeInput": {"activityEnd": {}}}
        turn_ended.set()
        assert "clientContent" in await ws.receive_json(), (
            "phantom provider-feedback activity"
        )
        synchronized.set()
        for index in range(3):
            following.append(await ws.receive_json())
            if index == 1:
                next_audio.set()
        assert following[0] == {"realtimeInput": {"activityStart": {}}}
        assert (
            base64.b64decode(following[1]["realtimeInput"]["audio"]["data"]) == next_pcm
        )
        assert following[2] == {"realtimeInput": {"activityEnd": {}}}
        next_seen.set()
        async for _ in ws:
            pass
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        session = AgentSession(
            llm=model, turn_detection="vad", vad=detector, stt=None, tts=None
        )
        session.input.audio, session.output.audio = audio, Output()
        session.on("error", errors.append)
        try:
            agent = await start_agent(session)
            native = next(iter(model._sessions))
            initial = rtc.AudioFrame(b"\x01\x10" * 480, 16000, 1, 480)
            audio.frames.put_nowait(initial)
            await asyncio.wait_for(detector.frames.get(), 5)
            await asyncio.wait_for(initial_seen.wait(), 5)
            native.commit_audio()
            speech = session.generate_reply(instructions="Active synthetic response")
            await asyncio.wait_for(reply_seen.wait(), 5)
            audio.frames.put_nowait(rtc.AudioFrame(continuous_pcm * 480, 16000, 1, 480))
            await asyncio.wait_for(detector.frames.get(), 5)
            await asyncio.wait_for(continuous_seen.wait(), 5)
            await asyncio.wait_for(audible.wait(), 5)
            assert native._audio_active
            assert native._activity_interrupt_target is not native._generation
            # The actual owned VAD stream feeds LiveKit's recognition callback,
            # not a manually reordered pair of plugin methods.
            detector.emit(vad.VADEventType.INFERENCE_DONE, speech_duration=1)
            await asyncio.wait_for(cleared.wait(), 5)
            assert native._failure is None, "continuous input barge-in aborted I/O"
            await asyncio.wait_for(provider_cancelled.wait(), 5)
            assert speech.interrupted and native._audio_active
            # Repeated VAD feedback must preserve the real activity as well.
            detector.emit(vad.VADEventType.INFERENCE_DONE, speech_duration=1)
            audio.frames.put_nowait(rtc.AudioFrame(caller_pcm, 16000, 1, 480))
            await asyncio.wait_for(detector.frames.get(), 5)
            await asyncio.wait_for(caller_seen.wait(), 5)
            native.commit_audio()
            await asyncio.wait_for(turn_ended.wait(), 5)
            context = agent.chat_ctx.copy()
            context.add_message(role="user", content="Continuous input barrier")
            await agent.update_chat_ctx(context)
            await asyncio.wait_for(synchronized.wait(), 5)
            audio.frames.put_nowait(rtc.AudioFrame(next_pcm, 16000, 1, 480))
            await asyncio.wait_for(detector.frames.get(), 5)
            await asyncio.wait_for(next_audio.wait(), 5)
            native.commit_audio()
            await asyncio.wait_for(next_seen.wait(), 5)
            assert len(opened) == 1 and len(controls) == 4 and errors == []
        finally:
            release_output.set()
            audio.frames.put_nowait(None)
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
