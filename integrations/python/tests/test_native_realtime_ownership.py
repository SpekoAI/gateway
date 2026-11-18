"""Actual manual input and pending-future ownership, never inferred reply IDs."""

from __future__ import annotations

import asyncio
import base64
import json

import pytest
from aiohttp import web
from livekit import rtc
from livekit.agents import AgentSession, llm
from test_native_realtime_boundaries import assert_resampled
from test_native_realtime_cancellation import client_model, start_agent
from test_native_realtime_commit import MANUAL, refuse_context
from test_native_realtime_context import google_output, history_text
from test_native_realtime_livekit import collect_generation
from test_native_sidecar import native_binary as native_binary  # noqa: PLC0414
from test_native_sidecar import native_stack
from test_relay_speech_livekit import OwnedAudioInput

from speko_gateway import GatewayError


@pytest.mark.parametrize("new_first", [False, True])
@pytest.mark.parametrize("sample_rate", [16000, 48000])
async def test_pending_original_cannot_succeed_after_unkeyed_input_overlap(
    native_binary, monkeypatch, new_first, sample_rate
):
    sockets, ends = asyncio.Queue(), asyncio.Queue()
    commands, pcm, answer = [], bytearray(), None
    closed = asyncio.Event()

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        sockets.put_nowait(ws)
        try:
            async for message in ws:
                command = json.loads(message.data)
                commands.append(command)
                control = command.get("realtimeInput", {})
                if "audio" in control:
                    pcm.extend(base64.b64decode(control["audio"]["data"]))
                if "activityEnd" in control:
                    ends.put_nowait(None)
        finally:
            closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        native = model.session(turn_detection_disabled=True)
        try:
            await native._update_session(tools=[])
            await native.start()
            ws = await asyncio.wait_for(sockets.get(), 5)
            original_pcm, newer_pcm = b"\x01\x10" * 480, b"\x03\x40" * 480
            sent, send = bytearray(), native._send_audio

            def observed(data):
                sent.extend(data)
                send(data)

            monkeypatch.setattr(native, "_send_audio", observed)
            native.push_audio(rtc.AudioFrame(original_pcm, sample_rate, 1, 480))
            native.commit_audio()
            original = native.generate_reply()
            timer = native._pending_timers[original]
            await asyncio.wait_for(ends.get(), 5)
            assert original in native._pending and not original.done()
            refused = False
            try:
                native.push_audio(rtc.AudioFrame(newer_pcm, sample_rate, 1, 480))
                native.commit_audio()
            except GatewayError:
                refused = True
            if not refused:
                # Exact old-head hazard, with a genuine original pending future
                # and both physical answer orders, not just unsolicited output.
                await asyncio.wait_for(ends.get(), 5)
                await ws.send_json({"serverContent": {"interrupted": True}})
                outputs = (
                    ("NEW_AUDIO", "OLD_AUDIO")
                    if new_first
                    else ("OLD_AUDIO", "NEW_AUDIO")
                )
                for text in outputs:
                    await google_output(ws, text)
                event = await asyncio.wait_for(original, 5)
                answer, _ = await asyncio.wait_for(collect_generation(event), 5)
            assert refused, (
                f"uncorrelated input was accepted and original succeeded: {answer!r}"
            )
            await asyncio.wait_for(closed.wait(), 5)
            await asyncio.wait_for(asyncio.shield(native._task), 5)
            with pytest.raises(llm.RealtimeError):
                original.result()
            assert bytes(pcm) == bytes(sent)
            if sample_rate == 16000:
                assert bytes(pcm) == original_pcm
            else:
                assert_resampled(bytes(pcm), original_pcm)
            assert (
                len(
                    [c for c in commands if "activityEnd" in c.get("realtimeInput", {})]
                )
                == 1
            )
            assert (
                timer.cancelled() and not native._pending and not native._pending_timers
            )
            assert not native._uncommitted_source_samples
        finally:
            await native.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.skipif(not MANUAL, reason="SDK cannot negotiate manual AgentSession input")
@pytest.mark.parametrize("immediate_terminal", [False, True])
async def test_claimed_provider_feedback_retires_real_sdk_future_without_local_abort(
    native_binary, monkeypatch, immediate_terminal
):
    connected, futures, handles = asyncio.Queue(), asyncio.Queue(), asyncio.Queue()
    ended, feedback, terminal, closed, local_ask = (asyncio.Event() for _ in range(5))
    commands, pcm, generations = [], bytearray(), []

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
                    if "LOCAL_CANCEL_AFTER_PROVIDER" in json.dumps(command):
                        local_ask.set()
                    else:
                        await google_output(ws, "SAFE_NEW_TYPED_ANSWER")
        finally:
            closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        actor = AgentSession(
            llm=model, turn_detection="manual", vad=None, stt=None, tts=None
        )
        actor.on(
            "speech_created", lambda event: handles.put_nowait(event.speech_handle)
        )
        try:
            await start_agent(actor)
            native = next(iter(model._sessions))
            generate = native.generate_reply

            def observed(**options):
                future = generate(**options)
                futures.put_nowait(future)
                return future

            monkeypatch.setattr(native, "generate_reply", observed)
            native.on("input_speech_stopped", lambda event: feedback.set())
            native.on("generation_created", generations.append)
            native.on(
                "input_audio_transcription_completed",
                lambda event: terminal.set() if event.is_final else None,
            )
            await native.start()
            ws = await asyncio.wait_for(connected.get(), 5)
            raw = b"\x02\x11" * 480
            native.push_audio(rtc.AudioFrame(raw, 16000, 1, 480))
            actor.commit_user_turn()
            speech = await asyncio.wait_for(handles.get(), 5)
            original = await asyncio.wait_for(futures.get(), 5)
            assert original in native._pending and not original.done()
            timer = native._pending_timers[original]
            await asyncio.wait_for(ended.wait(), 5)
            event = {"interrupted": True}
            if immediate_terminal:
                event.update(
                    turnComplete=True,
                    inputTranscription={"text": "original consumed words"},
                )
            await ws.send_json({"serverContent": event})
            await asyncio.wait_for(feedback.wait(), 5)
            await asyncio.wait_for(speech.wait_for_playout(), 5)
            assert native._failure is None and not closed.is_set(), (
                "claimed provider feedback entered local fatal cancellation"
            )
            assert speech.interrupted and original.done()
            assert original.cancelled() or original.exception() is not None
            assert (
                timer.cancelled() and not native._pending and not native._pending_timers
            )
            if not immediate_terminal:
                await refuse_context(native, "REJECTED_DURING_PROVIDER_CANCEL")
                # Unheard fragments of the interrupted turn cannot become a
                # new generation or satisfy the retired original future.
                await ws.send_json(
                    {
                        "serverContent": {
                            "outputTranscription": {"text": "LATE_UNHEARD_OLD"}
                        }
                    }
                )
                await ws.send_json(
                    {
                        "serverContent": {
                            "turnComplete": True,
                            "inputTranscription": {"text": "original consumed words"},
                        }
                    }
                )
            await asyncio.wait_for(terminal.wait(), 5)
            assert not generations
            await asyncio.wait_for(actor.wait_for_idle(), 5)
            newer = actor.generate_reply(user_input="SAFE_TYPED_AFTER_PROVIDER_CANCEL")
            await asyncio.wait_for(newer.wait_for_playout(), 5)
            assert newer.exception() is None
            await asyncio.wait_for(futures.get(), 5)
            assert len(generations) == 1 and not closed.is_set()
            assert bytes(pcm) == raw
            assert "REJECTED_DURING_PROVIDER_CANCEL" not in json.dumps(commands)
            assert "LATE_UNHEARD_OLD" not in history_text(native.chat_ctx)
            assert "LATE_UNHEARD_OLD" not in history_text(actor.history)
            assert [
                c["realtimeInput"]
                for c in commands
                if "realtimeInput" in c and "audio" not in c["realtimeInput"]
            ] == [{"activityStart": {}}, {"activityEnd": {}}]
            # A later REAL SDK future has no provider-interrupted provenance.
            # Its local cancellation must still stop/join the physical work.
            local = actor.generate_reply(instructions="LOCAL_CANCEL_AFTER_PROVIDER")
            local_future = await asyncio.wait_for(futures.get(), 5)
            await asyncio.wait_for(local_ask.wait(), 5)
            local_future.cancel()
            await asyncio.wait_for(closed.wait(), 5)
            await asyncio.wait_for(asyncio.shield(native._task), 5)
            await asyncio.wait_for(local.wait_for_playout(), 5)
            assert local_future.cancelled() and not native._pending_timers
        finally:
            await actor.aclose()
            await native.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.skipif(not MANUAL, reason="SDK cannot negotiate manual AgentSession input")
@pytest.mark.parametrize("first_skip_reply", [False, True])
async def test_actual_agent_ordinary_audio_then_typed_turn_after_real_terminals(
    native_binary, monkeypatch, first_skip_reply
):
    connected, ends, futures, handles, words = (asyncio.Queue() for _ in range(5))
    commands, pcm = [], bytearray()

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
                ends.put_nowait(None)
            if command.get("clientContent", {}).get("turnComplete"):
                await google_output(ws, "SAFE_TYPED_THIRD")
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        actor = AgentSession(
            llm=model, turn_detection="manual", vad=None, stt=None, tts=None
        )
        actor.on(
            "speech_created", lambda event: handles.put_nowait(event.speech_handle)
        )
        actor.on(
            "conversation_item_added",
            lambda event: (
                words.put_nowait(event.item.text_content)
                if isinstance(event.item, llm.ChatMessage)
                and event.item.role == "assistant"
                else None
            ),
        )
        try:
            await start_agent(actor)
            native = next(iter(model._sessions))
            generate = native.generate_reply

            def observed(**options):
                future = generate(**options)
                futures.put_nowait(future)
                return future

            monkeypatch.setattr(native, "generate_reply", observed)
            await native.start()
            ws = await asyncio.wait_for(connected.get(), 5)
            first, second = b"\x01\x10" * 480, b"\x03\x40" * 480
            for index, raw in enumerate((first, second)):
                native.push_audio(rtc.AudioFrame(raw, 16000, 1, 480))
                skip = first_skip_reply and index == 0
                actor.commit_user_turn(skip_reply=skip)
                future = None if skip else await asyncio.wait_for(futures.get(), 5)
                await asyncio.wait_for(ends.get(), 5)
                timer = native._pending_timers.get(future)
                with pytest.raises(GatewayError, match="committing again"):
                    actor.commit_user_turn()
                assert futures.empty()
                if future is not None:
                    assert (
                        native._pending_timers[future] is timer
                        and not timer.cancelled()
                    )
                expected = "FIRST_AUDIO" if index == 0 else "SECOND_AUDIO"
                await google_output(ws, expected)
                speech = await asyncio.wait_for(handles.get(), 5)
                await asyncio.wait_for(speech.wait_for_playout(), 5)
                assert speech.exception() is None and not speech.interrupted
                assert await asyncio.wait_for(words.get(), 5) == expected
                if future is not None:
                    assert future.done() and future.exception() is None
                await asyncio.wait_for(actor.wait_for_idle(), 5)
                assert not native._pending and not native._pending_timers
            assert bytes(pcm) == first + second
            speech = actor.generate_reply(user_input="SAFE_TYPED_CONTEXT_THIRD")
            await asyncio.wait_for(speech.wait_for_playout(), 5)
            assert speech.exception() is None
            assert await asyncio.wait_for(words.get(), 5) == "SAFE_TYPED_THIRD"
            assert "SAFE_TYPED_CONTEXT_THIRD" in history_text(actor.history)
            assert native._failure is None
            assert (
                len(
                    [c for c in commands if "activityEnd" in c.get("realtimeInput", {})]
                )
                == 2
            )
        finally:
            await actor.aclose()
            await native.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.skipif(not MANUAL, reason="SDK cannot negotiate manual AgentSession input")
async def test_late_attached_manual_source_refuses_before_its_first_pcm(
    native_binary, monkeypatch
):
    peer_closed, actor_closed = asyncio.Event(), asyncio.Event()
    pcm = bytearray()
    commands = []

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
        finally:
            peer_closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        audio = OwnedAudioInput()
        actor = AgentSession(
            llm=model, turn_detection="manual", vad=None, stt=None, tts=None
        )
        actor.on("close", lambda event: actor_closed.set())
        try:
            await start_agent(actor)
            native = next(iter(model._sessions))
            await native.start()
            actor.input.audio = audio
            audio.frames.put_nowait(rtc.AudioFrame(b"\x01\x10" * 480, 16000, 1, 480))
            await asyncio.wait_for(peer_closed.wait(), 5)
            await asyncio.wait_for(actor_closed.wait(), 5)
            assert not pcm and not commands
            assert native._task.done() and not native._pending_timers
            assert (
                not native._uncommitted_source_samples and native._user_started is None
            )
        finally:
            audio.frames.put_nowait(None)
            await actor.aclose()
            await model.aclose()
            await client.aclose()


async def test_real_local_cancel_cannot_be_relabelled_by_later_provider_feedback(
    native_binary, monkeypatch
):
    sockets, ended, closed = asyncio.Queue(), asyncio.Event(), asyncio.Event()

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        sockets.put_nowait(ws)
        try:
            async for message in ws:
                if "activityEnd" in json.loads(message.data).get("realtimeInput", {}):
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
            ws = await asyncio.wait_for(sockets.get(), 5)
            native.push_audio(rtc.AudioFrame(b"\x01\x10" * 480, 16000, 1, 480))
            native.commit_audio()
            original = native.generate_reply()
            await asyncio.wait_for(ended.wait(), 5)
            decode = native._decode

            def local_cancel_before_feedback(frame):
                event = decode(frame)
                if event.get("serverContent", {}).get("interrupted"):
                    # Deterministic actual WS ingress: the public Future.cancel
                    # wins before feedback classification, while its existing
                    # scheduled done callback has not run yet.
                    original.cancel()
                return event

            monkeypatch.setattr(native, "_decode", local_cancel_before_feedback)
            await ws.send_json(
                {"serverContent": {"interrupted": True, "turnComplete": True}}
            )
            await asyncio.wait_for(closed.wait(), 5)
            await asyncio.wait_for(asyncio.shield(native._task), 5)
            assert original.cancelled() and native._failure is not None
            assert not native._pending and not native._pending_timers
        finally:
            await native.aclose()
            await model.aclose()
            await client.aclose()
