"""Actual automatic Gemini input ownership and local-clear boundaries."""

from __future__ import annotations

import asyncio
import base64
import dataclasses
import json

import pytest
from aiohttp import web
from livekit import rtc
from livekit.agents import AgentSession, llm
from livekit.agents.voice import io
from test_native_sidecar import native_binary as native_binary  # noqa: PLC0414
from test_native_sidecar import native_stack
from test_realtime_review147 import Output, answer, client_model, start


class Input(io.AudioInput):
    def __init__(self):
        super().__init__(label="owned-gemini-input147")
        self.frames = asyncio.Queue()

    async def __anext__(self):
        return await self.frames.get()


async def until(predicate):
    async def wait():
        while not predicate():
            await asyncio.sleep(0)

    await asyncio.wait_for(wait(), 5)


@pytest.mark.parametrize("clear_completed", [False, True])
async def test_automatic_completed_voice_allows_genuine_typed_followup(
    native_binary, clear_completed
):
    heard, spoken, typed, closed = (asyncio.Event() for _ in range(4))
    received = []

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        setup = await ws.receive_json()
        assert not setup["setup"]["realtimeInputConfig"]["automaticActivityDetection"][
            "disabled"
        ]
        await ws.send_json({"setupComplete": {}})
        async for message in ws:
            command = json.loads(message.data)
            received.append(command)
            if "audio" in command.get("realtimeInput", {}):
                heard.set()
                await ws.send_json(
                    {
                        "serverContent": {
                            "inputTranscription": {"text": "Original voice request"}
                        }
                    }
                )
                await answer(ws, "Completed original voice answer")
            if any(
                part.get("text") == "Independent typed followup"
                for turn in command.get("clientContent", {}).get("turns", [])
                for part in turn["parts"]
            ):
                typed.set()
            if command.get("clientContent", {}).get("turnComplete"):
                await answer(ws, "Typed answer")
        closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        source = Input()
        session = AgentSession(
            llm=model, stt=None, tts=None, vad=None, turn_detection="realtime_llm"
        )
        session.input.audio = source
        session.output.audio = Output()
        session.on(
            "conversation_item_added",
            lambda event: (
                spoken.set()
                if isinstance(event.item, llm.ChatMessage)
                and event.item.role == "assistant"
                and event.item.text_content == "Completed original voice answer"
                else None
            ),
        )
        try:
            await start(session)
            native = next(iter(model._sessions))
            assert not native._manual_turns
            source.frames.put_nowait(rtc.AudioFrame(b"\x01\x10" * 1600, 16000, 1, 1600))
            await asyncio.wait_for(heard.wait(), 5)
            await asyncio.wait_for(spoken.wait(), 5)
            if clear_completed:
                session.clear_user_turn()
            speech = session.generate_reply(user_input="Independent typed followup")
            await asyncio.wait_for(speech.wait_for_playout(), 5)
            assert typed.is_set() and native._failure is None and not closed.is_set(), (
                "completed automatic voice input blocked a genuine typed followup"
            )
            assert any(
                command.get("clientContent", {}).get("turnComplete")
                for command in received
            )
        finally:
            await session.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("automatic", [False, True])
async def test_clear_after_peer_received_audio_fences_original_input(
    native_binary, automatic
):
    heard, typed, closed = (asyncio.Event() for _ in range(3))

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        async for message in ws:
            command = json.loads(message.data)
            if "audio" in command.get("realtimeInput", {}):
                heard.set()
            if "clientContent" in command:
                typed.set()
                if command["clientContent"].get("turnComplete"):
                    await answer(ws, "Must not mix old audio")
        closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        if not automatic and "can_disable_turn_detection" not in {
            field.name for field in dataclasses.fields(llm.RealtimeCapabilities)
        }:
            # 1.4.5 normalizes AgentSession manual mode away. Exercise its
            # supported public native manual factory, without fabricating an
            # AgentSession mode or adding a capability skip.
            native = model.session(turn_detection_disabled=True)
            try:
                await native._update_session(tools=[])
                await native.start()
                native.push_audio(rtc.AudioFrame(b"\x01\x10" * 1600, 16000, 1, 1600))
                await asyncio.wait_for(heard.wait(), 5)
                native.clear_audio()
                await asyncio.wait_for(closed.wait(), 5)
                assert (
                    closed.is_set()
                    and not typed.is_set()
                    and native._failure is not None
                )
            finally:
                await native.aclose()
                await model.aclose()
                await client.aclose()
            return
        source = Input()
        session = AgentSession(
            llm=model,
            stt=None,
            tts=None,
            vad=None,
            turn_detection="realtime_llm" if automatic else "manual",
        )
        session.input.audio = source
        session.output.audio = Output()
        try:
            await start(session)
            native = next(iter(model._sessions))
            assert native._manual_turns != automatic
            source.frames.put_nowait(rtc.AudioFrame(b"\x01\x10" * 1600, 16000, 1, 1600))
            await asyncio.wait_for(heard.wait(), 5)
            session.clear_user_turn()
            speech = session.generate_reply(
                user_input="Must not mix with cleared input"
            )
            await until(lambda: closed.is_set() or typed.is_set())
            assert (
                closed.is_set() and not typed.is_set() and native._failure is not None
            ), "cleared transmitted automatic input contaminated a later typed turn"
            await asyncio.wait_for(speech.wait_for_playout(), 5)
        finally:
            await session.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize(
    "later",
    ["sent", "queued", "queued-before-observation", "resampler", "resampler-tail"],
)
async def test_automatic_completion_preserves_later_original_input(
    native_binary, later
):
    observed, complete, ended, later_received = (asyncio.Event() for _ in range(4))
    received = []
    first_received, observe_allowed = asyncio.Event(), asyncio.Event()
    rate = 48000 if later.startswith("resampler") else 16000

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        audio_count = 0

        async def finish():
            await complete.wait()
            await ws.send_json({"serverContent": {"turnComplete": True}})

        task = asyncio.create_task(finish())
        try:
            async for message in ws:
                command = json.loads(message.data)
                received.append(command)
                if "audio" in command.get("realtimeInput", {}):
                    audio_count += 1
                    if audio_count == 1:
                        first_received.set()
                        await observe_allowed.wait()
                        samples = (
                            len(
                                base64.b64decode(
                                    command["realtimeInput"]["audio"]["data"]
                                )
                            )
                            // 2
                        )
                        await ws.send_json(
                            {
                                "voiceActivity": {
                                    "type": "ACTIVITY_END",
                                    "audioOffset": f"{samples // 16000}.{samples % 16000 * 62500:09d}s",
                                }
                            }
                        )
                        await ws.send_json(
                            {
                                "serverContent": {
                                    "inputTranscription": {
                                        "text": "Original observed input"
                                    },
                                    "outputTranscription": {
                                        "text": "Original response"
                                    },
                                }
                            }
                        )
                    else:
                        later_received.set()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        ended.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        native = model.session()
        lock = None
        try:
            await native._update_session(tools=[])
            await native.start()
            native.on(
                "input_audio_transcription_completed",
                lambda event: observed.set() if not event.is_final else None,
            )
            native.push_audio(
                rtc.AudioFrame(
                    b"\x01\x10" * (4800 if rate == 48000 else 1600),
                    rate,
                    1,
                    4800 if rate == 48000 else 1600,
                )
            )
            await asyncio.wait_for(first_received.wait(), 5)
            if later == "queued-before-observation":
                lock = native._socket._write_lock
                await lock.acquire()
                native.push_audio(rtc.AudioFrame(b"\x02\x20" * 320, rate, 1, 320))
            observe_allowed.set()
            await asyncio.wait_for(observed.wait(), 5)
            if later == "queued":
                lock = native._socket._write_lock
                await lock.acquire()
            if later not in {"queued-before-observation", "resampler-tail"}:
                samples = 480 if later == "resampler" else 320
                native.push_audio(
                    rtc.AudioFrame(b"\x02\x20" * samples, rate, 1, samples)
                )
            if later == "sent":
                await asyncio.wait_for(later_received.wait(), 5)
            complete.set()
            await until(lambda: native._generation is None and native._input_item == "")
            context = native.chat_ctx
            context.add_message(
                role="user", content="Must not overtake later captured input"
            )
            from speko_gateway import GatewayError

            with pytest.raises(GatewayError, match="active input"):
                await native.update_chat_ctx(context)
            await asyncio.wait_for(ended.wait(), 5)
            assert not any("clientContent" in command for command in received), (
                "an older automatic turn completed newer captured input"
            )
        finally:
            if lock is not None and lock.locked():
                lock.release()
            complete.set()
            await native.aclose()
            await model.aclose()
            await client.aclose()


async def test_automatic_still_local_resampler_input_can_be_cleared(native_binary):
    typed, complete = (asyncio.Event() for _ in range(2))
    received = []

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        async for message in ws:
            command = json.loads(message.data)
            received.append(command)
            if "clientContent" in command:
                typed.set()
                if command["clientContent"].get("turnComplete"):
                    await answer(ws, "After local clear")
                    complete.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        native = model.session()
        reply = None
        try:
            await native._update_session(tools=[])
            await native.start()
            native.push_audio(rtc.AudioFrame(b"\x01\x70" * 480, 48000, 1, 480))
            assert not received and native._uncommitted_source_samples == 480
            native.clear_audio()
            context = native.chat_ctx
            context.add_message(role="user", content="After genuine local clear")
            await native.update_chat_ctx(context)
            reply = native.generate_reply()
            await asyncio.wait_for(complete.wait(), 5)
            await asyncio.wait_for(reply, 5)
            assert typed.is_set() and native._failure is None
            assert not any(
                "audio" in command.get("realtimeInput", {}) for command in received
            ), "still-local cleared samples escaped to the automatic provider"
        finally:
            if reply is not None:
                reply.cancel()
                await asyncio.gather(reply, return_exceptions=True)
            await native.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("native_end", [False, True])
@pytest.mark.parametrize("next_operation", ["typed", "clear"])
async def test_old_first_response_cannot_complete_newer_already_sent_audio(
    native_binary, next_operation, native_end
):
    first_received, later_received, release_old, old_completed = (
        asyncio.Event() for _ in range(4)
    )
    received = []

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})

        async def old_answer():
            await release_old.wait()
            if native_end:
                await ws.send_json(
                    {"voiceActivity": {"type": "ACTIVITY_END", "audioOffset": "0.1s"}}
                )
            await ws.send_json(
                {
                    "serverContent": {
                        "inputTranscription": {"text": "Only original input A"}
                    }
                }
            )
            await answer(ws, "Only original answer A")

        task = asyncio.create_task(old_answer())
        try:
            async for message in ws:
                command = json.loads(message.data)
                received.append(command)
                if "audio" in command.get("realtimeInput", {}):
                    if first_received.is_set():
                        later_received.set()
                    else:
                        first_received.set()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        native = model.session()
        try:
            await native._update_session(tools=[])
            await native.start()
            native.on(
                "input_audio_transcription_completed",
                lambda event: old_completed.set() if event.is_final else None,
            )
            native.push_audio(rtc.AudioFrame(b"\x01\x10" * 1600, 16000, 1, 1600))
            await asyncio.wait_for(first_received.wait(), 5)
            native.push_audio(rtc.AudioFrame(b"\x02\x20" * 1600, 16000, 1, 1600))
            # The real peer has BOTH audio inputs before it releases A's FIRST
            # server event. B is not merely queued and has no response or ACK.
            await asyncio.wait_for(later_received.wait(), 5)
            release_old.set()
            await asyncio.wait_for(old_completed.wait(), 5)
            if next_operation == "typed":
                context = native.chat_ctx
                context.add_message(role="user", content="Must not overtake input B")
                from speko_gateway import GatewayError

                with pytest.raises(GatewayError, match="active input"):
                    await native.update_chat_ctx(context)
            else:
                native.clear_audio()
                assert native._failure is not None, (
                    "an old first response falsely acknowledged newer transmitted audio"
                )
            assert not any("clientContent" in command for command in received)
        finally:
            release_old.set()
            await native.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("clear_completed", [False, True])
async def test_native_activity_end_prefix_allows_genuine_typed_followup(
    native_binary, clear_completed
):
    heard, spoken, typed, closed = (asyncio.Event() for _ in range(4))
    received = []

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        setup = await ws.receive_json()
        assert not setup["setup"]["realtimeInputConfig"]["automaticActivityDetection"][
            "disabled"
        ]
        await ws.send_json({"setupComplete": {}})
        async for message in ws:
            command = json.loads(message.data)
            received.append(command)
            if "audio" in command.get("realtimeInput", {}):
                heard.set()
                await ws.send_json(
                    {"voiceActivity": {"type": "ACTIVITY_END", "audioOffset": "0.1s"}}
                )
                await ws.send_json(
                    {
                        "serverContent": {
                            "inputTranscription": {"text": "Original voice request"}
                        }
                    }
                )
                await answer(ws, "Completed original voice answer")
            if any(
                part.get("text") == "Independent typed followup"
                for turn in command.get("clientContent", {}).get("turns", [])
                for part in turn["parts"]
            ):
                typed.set()
            if command.get("clientContent", {}).get("turnComplete"):
                await answer(ws, "Typed answer")
        closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        source = Input()
        session = AgentSession(
            llm=model, stt=None, tts=None, vad=None, turn_detection="realtime_llm"
        )
        session.input.audio = source
        session.output.audio = Output()
        session.on(
            "conversation_item_added",
            lambda event: (
                spoken.set()
                if isinstance(event.item, llm.ChatMessage)
                and event.item.role == "assistant"
                and event.item.text_content == "Completed original voice answer"
                else None
            ),
        )
        try:
            await start(session)
            native = next(iter(model._sessions))
            assert not native._manual_turns
            source.frames.put_nowait(rtc.AudioFrame(b"\x01\x10" * 1600, 16000, 1, 1600))
            await asyncio.wait_for(heard.wait(), 5)
            await asyncio.wait_for(spoken.wait(), 5)
            if clear_completed:
                session.clear_user_turn()
            speech = session.generate_reply(user_input="Independent typed followup")
            await asyncio.wait_for(speech.wait_for_playout(), 5)
            assert typed.is_set() and native._failure is None and not closed.is_set(), (
                "completed automatic voice input blocked a genuine typed followup"
            )
            assert any(
                command.get("clientContent", {}).get("turnComplete")
                for command in received
            )
        finally:
            await session.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize(
    "boundary",
    [
        "absent",
        "malformed",
        "negative",
        "too-far",
        "regressed",
        "replay",
        "unsent",
        "start-only",
        "partial",
        "fraction",
        "zero",
        "genuine",
    ],
)
async def test_native_offset_cannot_retire_unproved_source_after_clear(
    native_binary, boundary
):
    first, later, completed = (asyncio.Event() for _ in range(3))
    commands = asyncio.Queue()
    received = []

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})

        async def send():
            while True:
                event = await commands.get()
                if event is not None:
                    await ws.send_json(event)
                await ws.send_json(
                    {"serverContent": {"inputTranscription": {"text": "Old input"}}}
                )
                await answer(ws, "Original response")

        task = asyncio.create_task(send())
        try:
            async for message in ws:
                command = json.loads(message.data)
                received.append(command)
                if "audio" in command.get("realtimeInput", {}):
                    (later if first.is_set() else first).set()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        native = model.session()
        lock = None
        try:
            await native._update_session(tools=[])
            await native.start()
            native.on(
                "input_audio_transcription_completed",
                lambda event: completed.set() if event.is_final else None,
            )
            native.push_audio(rtc.AudioFrame(b"\x01\x10" * 1600, 16000, 1, 1600))
            await asyncio.wait_for(first.wait(), 5)
            commands.put_nowait(
                {"voiceActivity": {"type": "ACTIVITY_END", "audioOffset": "0.1s"}}
            )
            await asyncio.wait_for(completed.wait(), 5)
            native.clear_audio()
            assert native._failure is None
            completed.clear()
            if boundary == "unsent":
                lock = native._socket._write_lock
                await lock.acquire()
            native.push_audio(rtc.AudioFrame(b"\x02\x20" * 320, 16000, 1, 320))
            if boundary != "unsent":
                await asyncio.wait_for(later.wait(), 5)
            offsets = {
                "malformed": "0.12",
                "negative": "-0.12s",
                "too-far": "0.2s",
                "regressed": "0.05s",
                "replay": "0.1s",
                "unsent": "0.12s",
                "start-only": "0.12s",
                "partial": "0.11s",
                "fraction": "0.119999999s",
                "zero": "0s",
                "genuine": "0.12s",
            }
            commands.put_nowait(
                None
                if boundary == "absent"
                else {
                    "voiceActivity": {
                        "type": "ACTIVITY_START"
                        if boundary == "start-only"
                        else "ACTIVITY_END",
                        "audioOffset": offsets[boundary],
                    }
                }
            )
            await asyncio.wait_for(completed.wait(), 5)
            context = native.chat_ctx
            context.add_message(role="user", content="Cannot erase original later PCM")
            from speko_gateway import GatewayError

            if boundary == "genuine":
                await native.update_chat_ctx(context)
                await until(
                    lambda: any("clientContent" in command for command in received)
                )
                assert native._failure is None
            else:
                with pytest.raises(GatewayError, match="active input"):
                    await native.update_chat_ctx(context)
                assert not any("clientContent" in command for command in received)
        finally:
            if lock is not None and lock.locked():
                lock.release()
            await native.aclose()
            await model.aclose()
            await client.aclose()
