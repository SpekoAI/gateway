"""Committed audio must retire before uncorrelated typed work can be admitted."""

from __future__ import annotations

import asyncio
import base64
import json

import pytest
from aiohttp import web
from livekit import rtc
from livekit.agents import AgentSession, llm
from test_native_realtime_cancellation import client_model, start_agent
from test_native_realtime_context import google_output, history_text
from test_native_realtime_livekit import collect_generation
from test_native_sidecar import native_binary as native_binary  # noqa: PLC0414
from test_native_sidecar import native_stack
from test_relay_speech_livekit import OwnedAudioInput

from speko_gateway import GatewayError
from speko_gateway.livekit import RealtimeModel

MANUAL = "can_disable_turn_detection" in llm.RealtimeCapabilities.__dataclass_fields__


@pytest.mark.parametrize("attached", [False, True])
@pytest.mark.parametrize("typed_first", [False, True])
async def test_committed_audio_rejects_typed_context_before_delayed_first_output(
    native_binary, monkeypatch, attached, typed_first
):
    if attached and not MANUAL:
        pytest.skip("SDK cannot negotiate manual AgentSession turns")
    rejected, supported = "REJECTED_BEFORE_AUDIO_REPLY", "SUPPORTED_AFTER_AUDIO_REPLY"
    commands, pcm, opened = [], bytearray(), []
    ended, connected, assistants = asyncio.Event(), asyncio.Queue(), asyncio.Queue()
    leaked = asyncio.Event()

    async def peer(request):
        opened.append(request.path)
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        connected.put_nowait(ws)
        async for message in ws:
            command = json.loads(message.data)
            commands.append(command)
            control = command.get("realtimeInput", {})
            if "audio" in control:
                pcm.extend(base64.b64decode(control["audio"]["data"]))
            if "activityEnd" in control:
                ended.set()
            if command.get("clientContent", {}).get("turnComplete"):
                if rejected in json.dumps(commands):
                    # Both original reproductions: wrong work can win the
                    # future regardless of which unkeyed answer arrives first.
                    outputs = ["TYPED_REJECTED", "AUDIO_ORIGINAL"]
                    for text in outputs if typed_first else reversed(outputs):
                        await google_output(ws, text)
                else:
                    await google_output(ws, "TYPED_SUPPORTED")
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        actor = None
        audio = OwnedAudioInput()
        if attached:
            actor = AgentSession(
                llm=model, turn_detection="manual", vad=None, stt=None, tts=None
            )
            actor.input.audio = audio
            actor.on(
                "conversation_item_added",
                lambda event: (
                    assistants.put_nowait(event.item.text_content)
                    if isinstance(event.item, llm.ChatMessage)
                    and event.item.role == "assistant"
                    else None
                ),
            )
            actor.on(
                "conversation_item_added",
                lambda event: (
                    leaked.set()
                    if isinstance(event.item, llm.ChatMessage)
                    and rejected in (event.item.text_content or "")
                    else None
                ),
            )
            await start_agent(actor)
            native = next(iter(model._sessions))
        else:
            native = model.session(turn_detection_disabled=True)
            await native._update_session(tools=[])
        try:
            await native.start()
            ws = await asyncio.wait_for(connected.get(), 5)
            raw = b"\x01\x10" * 480
            frame = rtc.AudioFrame(raw, 16000, 1, 480)
            if actor is not None:
                accepted, push = asyncio.Event(), native.push_audio

                def captured(frame):
                    push(frame)
                    accepted.set()

                monkeypatch.setattr(native, "push_audio", captured)
                audio.frames.put_nowait(frame)
                await asyncio.wait_for(accepted.wait(), 5)
            else:
                native.push_audio(frame)
            if actor is not None:
                actor.commit_user_turn(skip_reply=True)
            else:
                native.commit_audio()
            await asyncio.wait_for(ended.wait(), 5)
            assert bytes(pcm) == raw
            assert native._generation is None and native._committed
            before = history_text(native.chat_ctx)
            if actor is not None:
                speech = actor.generate_reply(user_input=rejected)
                completed = asyncio.ensure_future(speech.wait_for_playout())
                changed = asyncio.create_task(leaked.wait())
                try:
                    done, _ = await asyncio.wait(
                        {completed, changed},
                        timeout=5,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    assert done, (
                        "typed request neither refused nor reported its side effect"
                    )
                    assert not leaked.is_set(), (
                        "rejected typed text entered actual SDK history"
                    )
                    await asyncio.wait_for(completed, 5)
                finally:
                    completed.cancel()
                    changed.cancel()
                    await asyncio.gather(completed, changed, return_exceptions=True)
                failure = speech.exception()
            else:
                context = native.chat_ctx
                context.add_message(role="user", content=rejected)
                try:
                    await native.update_chat_ctx(context)
                    await asyncio.wait_for(native.generate_reply(), 5)
                except (GatewayError, llm.RealtimeError) as error:
                    failure = error
                else:
                    failure = None
            assert isinstance(failure, GatewayError), (
                "typed work was admitted while the original committed audio could claim it"
            )
            assert history_text(native.chat_ctx) == before
            assert rejected not in json.dumps(commands)
            assert not any("clientContent" in command for command in commands)
            if actor is not None:
                assert rejected not in history_text(actor.history)
                assert rejected not in history_text(actor.current_agent.chat_ctx)
            # Attach only the original audio's future, never a newer task.
            original = native.generate_reply() if actor is None else None
            assert not any("clientContent" in command for command in commands)
            await google_output(ws, "AUDIO_ORIGINAL")
            if actor is not None:
                assert await asyncio.wait_for(assistants.get(), 5) == "AUDIO_ORIGINAL"
                await asyncio.wait_for(actor.wait_for_idle(), 5)
            else:
                generation = await asyncio.wait_for(original, 5)
                messages, calls = await asyncio.wait_for(
                    collect_generation(generation), 5
                )
                assert messages == [["AUDIO_ORIGINAL", b"\x01\x10" * 2400]]
                assert (
                    calls == [] and not native._pending and not native._pending_timers
                )
            assert native._generation is None
            if actor is not None:
                speech = actor.generate_reply(user_input=supported)
                await asyncio.wait_for(speech.wait_for_playout(), 5)
                assert speech.exception() is None
                assert await asyncio.wait_for(assistants.get(), 5) == "TYPED_SUPPORTED"
            else:
                context = native.chat_ctx
                context.add_message(role="user", content=supported)
                await native.update_chat_ctx(context)
                generation = await asyncio.wait_for(native.generate_reply(), 5)
                messages, calls = await asyncio.wait_for(
                    collect_generation(generation), 5
                )
                assert messages == [["TYPED_SUPPORTED", b"\x01\x10" * 2400]]
                assert calls == []
            assert rejected not in json.dumps(commands)
            assert rejected not in history_text(native.chat_ctx)
            assert supported in history_text(native.chat_ctx)
            assert len(opened) == 1 and native._failure is None
            assert bytes(pcm) == raw
            if actor is not None:
                assert rejected not in history_text(actor.history)
                assert rejected not in history_text(actor.current_agent.chat_ctx)
        finally:
            audio.frames.put_nowait(None)
            if actor is not None:
                await actor.aclose()
            await native.aclose()
            await model.aclose()
            await client.aclose()


async def refuse_context(native, marker):
    context = native.chat_ctx
    context.add_message(role="user", content=marker)
    before = history_text(native.chat_ctx)
    with pytest.raises(GatewayError, match="response to finish"):
        await native.update_chat_ctx(context)
    assert history_text(native.chat_ctx) == before


@pytest.mark.parametrize(
    "boundary", ["normal", "continued", "interrupted", "no-output"]
)
@pytest.mark.parametrize("extended", [False, True])
async def test_original_audio_future_survives_clear_and_only_terminal_provider_boundary(
    native_binary, boundary, extended
):
    commands, pcm = [], bytearray()
    ended, terminal = asyncio.Event(), asyncio.Event()
    connected, generations = asyncio.Queue(), asyncio.Queue()
    rejected, supported = "REJECTED_DURING_AUDIO", "SAFE_TYPED_AFTER_TERMINAL"

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        connected.put_nowait(ws)
        async for message in ws:
            command = json.loads(message.data)
            commands.append(command)
            control = command.get("realtimeInput", {})
            if "audio" in control:
                pcm.extend(base64.b64decode(control["audio"]["data"]))
            if "activityEnd" in control:
                ended.set()
            if command.get("clientContent", {}).get("turnComplete"):
                await google_output(ws, "NEW_TYPED_ANSWER")
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        if extended:
            model = RealtimeModel(
                client,
                protocol="google.live.v1",
                model="gemini-3.8-live-extended-thinking",
                voice="Kore",
            )
        native = model.session(turn_detection_disabled=True)
        native.on("generation_created", generations.put_nowait)
        native.on(
            "input_audio_transcription_completed",
            lambda event: terminal.set() if event.is_final else None,
        )
        try:
            await native._update_session(tools=[])
            await native.start()
            ws = await asyncio.wait_for(connected.get(), 5)
            raw = b"\x02\x14" * 480
            native.push_audio(rtc.AudioFrame(raw, 16000, 1, 480))
            native.commit_audio()
            original = native.generate_reply()
            timer = native._pending_timers[original]
            await asyncio.wait_for(ended.wait(), 5)
            assert bytes(pcm) == raw
            native.clear_audio()
            await refuse_context(native, rejected)
            assert not original.done() and native._pending_timers[original] is timer
            assert not timer.cancelled()
            with pytest.raises(llm.RealtimeError):
                await native.generate_reply(
                    instructions="NEW_TASK_MUST_NOT_CLAIM_AUDIO"
                )
            assert native._pending_timers[original] is timer
            if boundary != "no-output":
                await ws.send_json(
                    {
                        "serverContent": {
                            "outputTranscription": {"text": "ORIGINAL_AUDIO"}
                        }
                    }
                )
                generation = await asyncio.wait_for(original, 5)
                assert generation is await asyncio.wait_for(generations.get(), 5)
                assert generation.user_initiated
                message = await asyncio.wait_for(
                    generation.message_stream.__anext__(), 5
                )
                assert (
                    await asyncio.wait_for(message.text_stream.__anext__(), 5)
                    == "ORIGINAL_AUDIO"
                )
                assert timer.cancelled() and not native._pending_timers
                await refuse_context(native, rejected)
                await ws.send_json(
                    {
                        "serverContent": {
                            "generationComplete": True,
                            "outputTranscription": {"text": "_LAST"},
                        }
                    }
                )
                assert (
                    await asyncio.wait_for(message.text_stream.__anext__(), 5)
                    == "_LAST"
                )
                await refuse_context(native, rejected)
                if extended and boundary == "normal":
                    await ws.send_json({"serverContent": {"turnComplete": True}})
                    assert [part async for part in message.text_stream] == []
                    await refuse_context(native, rejected)
                if boundary == "continued":
                    await ws.send_json(
                        {
                            "serverContent": {
                                "turnComplete": True,
                                "interactionStatus": "IN_PROGRESS",
                            }
                        }
                    )
                    assert [part async for part in message.text_stream] == []
                    await refuse_context(native, rejected)
                    await ws.send_json(
                        {
                            "serverContent": {
                                "outputTranscription": {"text": "LATE_OLD_AUDIO"}
                            }
                        }
                    )
                    late = await asyncio.wait_for(generations.get(), 5)
                    assert not late.user_initiated
                    drain = asyncio.create_task(collect_generation(late))
                else:
                    drain = None
                if boundary == "interrupted":
                    await ws.send_json({"serverContent": {"interrupted": True}})
                    assert [part async for part in message.text_stream] == []
                    await refuse_context(native, rejected)
            else:
                drain = None
            await ws.send_json(
                {
                    "serverContent": {
                        "turnComplete": True,
                        "interactionStatus": "IDLE",
                        "inputTranscription": {"text": "Actual original audio words"},
                    }
                }
            )
            await asyncio.wait_for(terminal.wait(), 5)
            if boundary == "no-output":
                with pytest.raises(llm.RealtimeError, match="ended without a reply"):
                    await asyncio.wait_for(original, 5)
                assert timer.cancelled()
            elif drain is not None:
                messages, calls = await asyncio.wait_for(drain, 5)
                assert messages == [["LATE_OLD_AUDIO", b""]] and not calls
            else:
                assert [part async for part in message.text_stream] == []
            assert not native._pending and not native._pending_timers
            context = native.chat_ctx
            context.add_message(role="user", content=supported)
            await native.update_chat_ctx(context)
            generation = await asyncio.wait_for(native.generate_reply(), 5)
            messages, calls = await asyncio.wait_for(collect_generation(generation), 5)
            assert messages == [["NEW_TYPED_ANSWER", b"\x01\x10" * 2400]] and not calls
            assert rejected not in json.dumps(commands)
            assert rejected not in history_text(native.chat_ctx)
            assert "NEW_TASK_MUST_NOT_CLAIM_AUDIO" not in json.dumps(commands)
            assert bytes(pcm) == raw
            assert [
                c["realtimeInput"]
                for c in commands
                if "realtimeInput" in c and "audio" not in c["realtimeInput"]
            ] == [{"activityStart": {}}, {"activityEnd": {}}]
        finally:
            await native.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("begin", ["audio", "activity"])
async def test_unclaimed_overlap_surfaces_before_new_input_or_typed_context(
    native_binary, begin
):
    commands, pcm = [], bytearray()
    ended, closed = asyncio.Event(), asyncio.Event()

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        try:
            async for message in ws:
                command = json.loads(message.data)
                commands.append(command)
                control = command.get("realtimeInput", {})
                if "audio" in control:
                    pcm.extend(base64.b64decode(control["audio"]["data"]))
                if "activityEnd" in control:
                    ended.set()
        finally:
            closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        native = model.session(turn_detection_disabled=True)
        try:
            await native._update_session(tools=[])
            await native.start()
            first, second = b"\x01\x10" * 480, b"\x03\x40" * 480
            native.push_audio(rtc.AudioFrame(first, 16000, 1, 480))
            native.commit_audio()
            await asyncio.wait_for(ended.wait(), 5)
            with pytest.raises(GatewayError, match="terminal response"):
                if begin == "activity":
                    native.start_user_activity()
                else:
                    native.push_audio(rtc.AudioFrame(second, 16000, 1, 480))
            await asyncio.wait_for(closed.wait(), 5)
            await asyncio.wait_for(asyncio.shield(native._task), 5)
            assert bytes(pcm) == first
            assert (
                len(
                    [c for c in commands if "activityEnd" in c.get("realtimeInput", {})]
                )
                == 1
            )
            assert not native._pending and not native._pending_timers
            assert not any("clientContent" in c for c in commands)
            context = native.chat_ctx
            context.add_message(role="user", content="NO_TASK_FOR_UNCORRELATED_INPUT")
            with pytest.raises((GatewayError, llm.RealtimeError)):
                await native.update_chat_ctx(context)
            assert "NO_TASK_FOR_UNCORRELATED_INPUT" not in history_text(native.chat_ctx)
        finally:
            await native.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize(
    "action", ["unclaimed-interrupt", "cancel-future", "close", "clear-forwarded"]
)
async def test_committed_audio_stop_joins_physical_request_without_admitting_typed_work(
    native_binary, action
):
    commands, pcm = [], bytearray()
    ended, closed, new_input = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        try:
            async for message in ws:
                command = json.loads(message.data)
                commands.append(command)
                control = command.get("realtimeInput", {})
                if "audio" in control:
                    pcm.extend(base64.b64decode(control["audio"]["data"]))
                    if len(pcm) == 1920:
                        new_input.set()
                if "activityEnd" in control:
                    ended.set()
        finally:
            closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        native = model.session(turn_detection_disabled=True)
        try:
            await native._update_session(tools=[])
            await native.start()
            raw = b"\x04\x12" * 480
            native.push_audio(rtc.AudioFrame(raw, 16000, 1, 480))
            if action == "clear-forwarded":
                native.push_audio(rtc.AudioFrame(raw, 16000, 1, 480))
                await asyncio.wait_for(new_input.wait(), 5)
                native.clear_audio()
                original = None
            else:
                native.commit_audio()
                original = (
                    native.generate_reply()
                    if action in {"cancel-future", "close"}
                    else None
                )
                await asyncio.wait_for(ended.wait(), 5)
            if action == "unclaimed-interrupt":
                native.interrupt()
            elif action == "cancel-future":
                original.cancel()
            elif action == "close":
                await native.aclose()
            await asyncio.wait_for(closed.wait(), 5)
            await asyncio.wait_for(asyncio.shield(native._task), 5)
            assert not native._pending and not native._pending_timers
            if original is not None and not original.cancelled():
                with pytest.raises(llm.RealtimeError):
                    original.result()
            context = native.chat_ctx
            context.add_message(role="user", content="REJECTED_AFTER_STOP")
            with pytest.raises((GatewayError, llm.RealtimeError)):
                await native.update_chat_ctx(context)
            with pytest.raises(llm.RealtimeError):
                await native.generate_reply()
            assert not any("clientContent" in command for command in commands)
            assert "REJECTED_AFTER_STOP" not in history_text(native.chat_ctx)
            assert bytes(pcm) == raw * (2 if action == "clear-forwarded" else 1)
            assert native._task.done()
        finally:
            await native.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.skipif(not MANUAL, reason="SDK cannot negotiate manual AgentSession turns")
async def test_preoutput_provider_feedback_keeps_unclaimed_commit_fenced_without_abort(
    native_binary,
):
    connected, ended, closed = asyncio.Queue(), asyncio.Event(), asyncio.Event()
    feedback, terminal, assistants = asyncio.Event(), asyncio.Event(), asyncio.Queue()
    commands, pcm = [], bytearray()

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        connected.put_nowait(ws)
        try:
            async for message in ws:
                command = json.loads(message.data)
                commands.append(command)
                control = command.get("realtimeInput", {})
                if "audio" in control:
                    pcm.extend(base64.b64decode(control["audio"]["data"]))
                if "activityEnd" in control:
                    ended.set()
                if command.get("clientContent", {}).get("turnComplete"):
                    await google_output(ws, "AFTER_ACKNOWLEDGED_INTERRUPTION")
        finally:
            closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        actor = AgentSession(
            llm=model, turn_detection="manual", vad=None, stt=None, tts=None
        )
        actor.on(
            "conversation_item_added",
            lambda event: (
                assistants.put_nowait(event.item.text_content)
                if isinstance(event.item, llm.ChatMessage)
                and event.item.role == "assistant"
                else None
            ),
        )
        try:
            await start_agent(actor)
            native = next(iter(model._sessions))
            native.on("input_speech_stopped", lambda event: feedback.set())
            native.on(
                "input_audio_transcription_completed",
                lambda event: terminal.set() if event.is_final else None,
            )
            await native.start()
            ws = await asyncio.wait_for(connected.get(), 5)
            raw = b"\x03\x12" * 480
            native.push_audio(rtc.AudioFrame(raw, 16000, 1, 480))
            actor.commit_user_turn(skip_reply=True)
            await asyncio.wait_for(ended.wait(), 5)
            assert not native._pending and native._generation is None
            # Real provider feedback re-enters AgentActivity.interrupt even
            # though no local generation/playback handle exists yet.
            await ws.send_json({"serverContent": {"interrupted": True}})
            await asyncio.wait_for(feedback.wait(), 5)
            assert native._failure is None and not closed.is_set(), (
                "provider feedback unnecessarily aborted the unclaimed audio response"
            )
            await refuse_context(native, "REJECTED_BEFORE_FEEDBACK_TERMINAL")
            await ws.send_json(
                {
                    "serverContent": {
                        "turnComplete": True,
                        "inputTranscription": {"text": "original consumed words"},
                    }
                }
            )
            await asyncio.wait_for(terminal.wait(), 5)
            speech = actor.generate_reply(
                user_input="SUPPORTED_AFTER_FEEDBACK_TERMINAL"
            )
            await asyncio.wait_for(speech.wait_for_playout(), 5)
            assert speech.exception() is None
            assert (
                await asyncio.wait_for(assistants.get(), 5)
                == "AFTER_ACKNOWLEDGED_INTERRUPTION"
            )
            assert bytes(pcm) == raw and not closed.is_set()
            assert [
                c["realtimeInput"]
                for c in commands
                if "realtimeInput" in c and "audio" not in c["realtimeInput"]
            ] == [{"activityStart": {}}, {"activityEnd": {}}]
            assert "REJECTED_BEFORE_FEEDBACK_TERMINAL" not in json.dumps(commands)
            assert "REJECTED_BEFORE_FEEDBACK_TERMINAL" not in history_text(
                native.chat_ctx
            )
            assert "REJECTED_BEFORE_FEEDBACK_TERMINAL" not in history_text(
                actor.history
            )
            assert "REJECTED_BEFORE_FEEDBACK_TERMINAL" not in history_text(
                actor.current_agent.chat_ctx
            )
            assert native._failure is None
        finally:
            await actor.aclose()
            await native.aclose()
            await model.aclose()
            await client.aclose()
