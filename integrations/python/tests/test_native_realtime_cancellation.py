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
@pytest.mark.parametrize("preexisting_activity", [False, True])
async def test_gemini_continuous_input_before_response_preserves_real_vad_barge_in(
    native_binary, continuous_pcm, preexisting_activity
):
    if (
        "can_disable_turn_detection"
        not in llm.RealtimeCapabilities.__dataclass_fields__
    ):
        pytest.skip("framework cannot negotiate manual realtime turns")
    from test_relay_speech_livekit import ControlledVAD, OwnedAudioInput

    detector, audio = ControlledVAD(), OwnedAudioInput()
    audible, cleared, release_output = (asyncio.Event() for _ in range(3))
    initial_seen, reply_seen, provider_cancelled, caller_seen, turn_ended = (
        asyncio.Event() for _ in range(5)
    )
    synchronized, next_seen = asyncio.Event(), asyncio.Event()
    opened, controls, turns, errors = [], [], [], []
    initial_pcm, continuous_pcm = b"\x01\x10" * 480, continuous_pcm * 480
    onset_pcm, remainder_pcm = b"\x04\x10" * 9600, b"\x06\x10" * 480
    next_pcm, cancellation_starts = b"\x05\x10" * 480, 0

    def frame(pcm):
        return rtc.AudioFrame(pcm, 16000, 1, len(pcm) // 2)

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
        nonlocal cancellation_starts
        opened.append(request.path)
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        setup = await ws.receive_json()
        assert (
            setup["setup"]["realtimeInputConfig"]["activityHandling"]
            == "START_OF_ACTIVITY_INTERRUPTS"
        )
        await ws.send_json({"setupComplete": {}})
        active, generating = None, False

        async def receive():
            nonlocal active, generating, cancellation_starts
            command = await ws.receive_json()
            controls.append(command)
            real = command.get("realtimeInput", {})
            if "activityStart" in real:
                assert active is None, "provider activity starts must stay balanced"
                active = bytearray()
                if generating:
                    generating = False
                    cancellation_starts += 1
                    await ws.send_json(
                        {"serverContent": {"interrupted": True, "turnComplete": True}}
                    )
                    provider_cancelled.set()
            elif "audio" in real:
                assert active is not None, "manual audio requires its own activity"
                active.extend(base64.b64decode(real["audio"]["data"]))
                if initial_pcm in active:
                    initial_seen.set()
                if remainder_pcm in active:
                    caller_seen.set()
            elif "activityEnd" in real:
                assert active is not None, "provider activity ends must stay balanced"
                assert active, "provider feedback produced an empty caller activity"
                turns.append(bytes(active))
                if remainder_pcm in active:
                    turn_ended.set()
                if next_pcm in active:
                    next_seen.set()
                active = None
            return command

        # The FIFO places already-forwarded continuous input before the reply,
        # making the provider's generation boundary deterministic.
        while "clientContent" not in await receive():
            pass
        reply_seen.set()
        generating = True
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
        while True:
            command = await receive()
            if "clientContent" in command:
                assert "Continuous input barrier" in str(command)
                assert active is None, "provider feedback reopened caller activity"
                synchronized.set()
                break
        while not next_seen.is_set():
            await receive()
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
            audio.frames.put_nowait(frame(initial_pcm))
            await asyncio.wait_for(detector.frames.get(), 5)
            native.commit_audio()
            await asyncio.wait_for(initial_seen.wait(), 5)
            if preexisting_activity:
                native.start_user_activity()
            audio.frames.put_nowait(frame(continuous_pcm))
            await asyncio.wait_for(detector.frames.get(), 5)
            speech = session.generate_reply(instructions="Active synthetic response")
            await asyncio.wait_for(reply_seen.wait(), 5)
            await asyncio.wait_for(audible.wait(), 5)
            assert native._activity_interrupt_target is not native._generation

            # SDK1.8.10 forwards the identifiable onset to realtime BEFORE
            # queueing it to VAD; only then release the actual async callback.
            onset = frame(onset_pcm)
            audio.frames.put_nowait(onset)
            assert (
                bytes((await asyncio.wait_for(detector.frames.get(), 5)).data)
                == onset_pcm
            )
            detector.emit(
                vad.VADEventType.INFERENCE_DONE, frames=[onset], speech_duration=0.6
            )
            await asyncio.wait_for(cleared.wait(), 5)
            assert native._failure is None, "continuous input barge-in aborted I/O"
            await asyncio.wait_for(provider_cancelled.wait(), 5)
            assert speech.interrupted and native._audio_active
            detector.emit(
                vad.VADEventType.INFERENCE_DONE, frames=[], speech_duration=0.6
            )
            audio.frames.put_nowait(frame(remainder_pcm))
            await asyncio.wait_for(detector.frames.get(), 5)
            await asyncio.wait_for(caller_seen.wait(), 5)
            native.commit_audio()
            await asyncio.wait_for(turn_ended.wait(), 5)
            context = agent.chat_ctx.copy()
            context.add_message(role="user", content="Continuous input barrier")
            await agent.update_chat_ctx(context)
            await asyncio.wait_for(synchronized.wait(), 5)
            audio.frames.put_nowait(frame(next_pcm))
            await asyncio.wait_for(detector.frames.get(), 5)
            native.commit_audio()
            await asyncio.wait_for(next_seen.wait(), 5)

            # Inspect physical provider turns and exact bytes, not method order.
            assert (
                b"".join(turns)
                == initial_pcm + continuous_pcm + onset_pcm + remainder_pcm + next_pcm
            )
            onset_turns = [i for i, value in enumerate(turns) if onset_pcm in value]
            remainder_turns = [
                i for i, value in enumerate(turns) if remainder_pcm in value
            ]
            assert onset_turns == remainder_turns and len(onset_turns) == 1, (
                "caller onset and remainder were split across provider turns"
            )
            assert turns[onset_turns[0]].endswith(onset_pcm + remainder_pcm)
            assert cancellation_starts == 1
            assert len(opened) == 1 and errors == [] and native._failure is None
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


@pytest.mark.parametrize(
    "stop",
    [
        "samples",
        "bytes",
        "pending_deadline",
        "queued_deadline",
        "resampler_deadline",
        "unrepresentable",
        "queued_clear",
        "close",
    ],
)
async def test_gemini_retained_input_is_bounded_and_physically_closed(
    native_binary, monkeypatch, stop
):
    from speko_gateway import _livekit_realtime as implementation

    queued = stop in ("queued_deadline", "queued_clear")
    if "deadline" in stop:
        monkeypatch.setattr(implementation, "_INPUT_HOLD_SECONDS", 0.1)
    if stop == "bytes":
        monkeypatch.setattr(implementation, "_INPUT_HOLD_BYTES", 8)
    setup_seen, peer_closed = asyncio.Event(), asyncio.Event()
    opened, delivered, errors = [], [], []

    async def peer(request):
        opened.append(request.path)
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        setup_seen.set()
        if not queued:
            await ws.send_json({"setupComplete": {}})
        async for message in ws:
            delivered.append(json.loads(message.data))
        peer_closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        session = model.session(turn_detection_disabled=True)
        session.on("error", errors.append)
        try:
            await session._update_session(
                instructions="Owned retained capture", tools=[]
            )
            await asyncio.wait_for(setup_seen.wait(), 5)
            if not queued:
                await session.start()
            rate = 48000 if stop in ("resampler_deadline", "unrepresentable") else 16000
            samples = 1 if rate == 48000 else 2 if stop == "bytes" else 320
            session.push_audio(rtc.AudioFrame(b"\x03\x10" * samples, rate, 1, samples))
            held = session._held_input
            assert held is not None
            original_deadline = held.until
            if stop in ("resampler_deadline", "unrepresentable"):
                assert not held.pcm, (
                    "fixture must exercise pre-output resampler capture"
                )
            if stop == "samples":
                session.push_audio(rtc.AudioFrame(b"\x04\x10" * 32000, 16000, 1, 32000))
            elif stop == "bytes":
                session.push_audio(rtc.AudioFrame(b"\x04\x10" * 8, 16000, 1, 8))
            elif stop in ("queued_deadline", "queued_clear", "unrepresentable"):
                session.commit_audio()
                if stop != "unrepresentable":
                    assert session._held_input is None and held in session._input_holds
                    assert held.until == original_deadline
                if stop == "queued_clear":
                    session.clear_audio()
            elif stop == "pending_deadline":
                session.push_audio(rtc.AudioFrame(b"\x04\x10" * 320, 16000, 1, 320))
                assert session._held_input is held and held.until == original_deadline
            elif stop == "close":
                await session.aclose()
            await asyncio.wait_for(peer_closed.wait(), 2)
            await asyncio.wait_for(asyncio.shield(session._task), 2)
            assert not session._input_holds and session._held_input is None
            assert not held.pcm and held.timer.cancelled()
            assert delivered == [] and len(opened) == 1
            if stop == "close":
                assert errors == []
            else:
                assert len(errors) == 1 and not errors[0].recoverable
                assert session._failure is not None and not session._failure.retryable
                with pytest.raises(llm.RealtimeError, match="closed"):
                    session.push_audio(rtc.AudioFrame(b"\x05\x10" * 320, 16000, 1, 320))
            with pytest.raises(GatewayError, match="exhausted"):
                await client.open(protocol="google.live.v1", model="gemini-3.8-live")
        finally:
            await session.aclose()
            await model.aclose()
            await client.aclose()


async def test_gemini_clear_unresolved_resampled_input_keeps_only_the_next_capture(
    native_binary,
):
    done = asyncio.Event()
    turns, errors = [], []

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        assert await ws.receive_json() == {"realtimeInput": {"activityStart": {}}}
        audio = await ws.receive_json()
        turns.append(base64.b64decode(audio["realtimeInput"]["audio"]["data"]))
        assert await ws.receive_json() == {"realtimeInput": {"activityEnd": {}}}
        done.set()
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
            session.push_audio(rtc.AudioFrame(b"\x04\x10" * 4800, 48000, 1, 4800))
            discarded = session._held_input
            session.clear_audio()
            assert not discarded.pcm and discarded.timer.cancelled()
            original = rtc.AudioFrame(b"\x05\x20" * 4800, 48000, 1, 4800)
            converter = rtc.AudioResampler(
                input_rate=48000, output_rate=16000, num_channels=1
            )
            expected = b"".join(
                bytes(frame.data)
                for frame in converter.push(original) + converter.flush()
            )
            session.push_audio(original)
            session.commit_audio()
            await asyncio.wait_for(done.wait(), 5)
            assert len(turns) == 1 and len(turns[0]) == len(expected) and expected
            actual_samples = [
                sample[0] for sample in struct.iter_unpack("<h", turns[0])
            ]
            expected_samples = [
                sample[0] for sample in struct.iter_unpack("<h", expected)
            ]
            # Keep exact length and tight PCM16 value integrity across native
            # converter instances. The onset/remainder regression at the
            # provider's native rate remains byte-exact.
            assert (
                max(
                    abs(a - b)
                    for a, b in zip(actual_samples, expected_samples, strict=True)
                )
                <= 2
            )
            assert errors == []
        finally:
            await session.aclose()
            await model.aclose()
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
