"""Actual SDK typed reply refusal must precede context and physical side effects."""

from __future__ import annotations

import asyncio
import base64
import json

import pytest
from aiohttp import web
from livekit import rtc
from livekit.agents import Agent, AgentSession, llm
from test_native_gpt_live import models, response
from test_native_realtime_boundaries import assert_resampled
from test_native_realtime_cancellation import client_model, start_agent
from test_native_sidecar import native_binary as native_binary  # noqa: PLC0414
from test_native_sidecar import native_stack
from test_relay_speech_livekit import OwnedAudioInput

DUPLEX = hasattr(llm, "DuplexModel")
MANUAL = "can_disable_turn_detection" in llm.RealtimeCapabilities.__dataclass_fields__


def history_text(context):
    return json.dumps([item.model_dump(mode="json") for item in context.items])


def backend_response():
    return response(
        [
            ("response.text.delta", {"delta": "ordinary answer"}),
            ("response.completed", {}),
        ]
    )


def delegate(identity):
    return {
        "type": "session.delegation.created",
        "offset_ms": 100,
        "delegation": {"id": identity, "target": "client"},
    }


async def google_output(ws, text):
    await ws.send_json(
        {
            "serverContent": {
                "outputTranscription": {"text": text},
                "modelTurn": {
                    "parts": [
                        {
                            "inlineData": {
                                "mimeType": "audio/pcm;rate=24000",
                                "data": base64.b64encode(b"\x01\x10" * 2400).decode(),
                            }
                        }
                    ]
                },
            }
        }
    )
    await ws.send_json({"serverContent": {"turnComplete": True}})


@pytest.mark.skipif(not DUPLEX, reason="SDK predates DuplexModel")
@pytest.mark.parametrize("attached", [False, True])
async def test_empty_gpt_unscoped_ask_refuses_before_append_and_ordinary_audio_survives(
    native_binary, attached
):
    commands, requests, opened = [], [], []
    ordinary_done = asyncio.Event()

    async def peer(request):
        if request.path == "/v1/llm/responses":
            requests.append(await request.json())
            return backend_response()
        opened.append(request.path)
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        setup = await ws.receive_json()
        assert (
            setup["session"]["instructions"] == "" and setup["session"]["input"] == []
        )
        await ws.send_json({"type": "session.started"})
        async for message in ws:
            command = json.loads(message.data)
            commands.append(command)
            if (
                command["type"] == "session.commentary.append"
                and command.get("delegation_id") is None
            ):
                # Exact original failure: a delegation arrives before any transcript.
                await ws.send_json(delegate("empty_before_transcript"))
            elif command["type"] == "session.input_audio.append":
                assert base64.b64decode(command["audio"]) == b"\x03\x12" * 480
                await ws.send_json(
                    {
                        "type": "session.input_transcript.delta",
                        "delta": "actual caller words",
                        "start_ms": 0,
                        "end_ms": 100,
                    }
                )
                await ws.send_json(delegate("ordinary_after_refusal"))
            elif command.get("delegation_id") == "ordinary_after_refusal":
                ordinary_done.set()
            elif command["type"] == "session.close":
                await ws.send_json({"type": "session.closed"})
                await ws.close()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        actor, audio = None, OwnedAudioInput()
        if attached:
            actor = AgentSession(
                llm=model, turn_detection="realtime_llm", stt=None, tts=None, vad=None
            )
            actor.input.audio = audio
            await actor.start(agent=Agent(instructions=""), record=False)
            native = actor.current_agent._activity._rt_session
        else:
            native = model.session()
            await native._update_session(instructions="", tools=[])
        try:
            await native.start()
            if actor is not None:
                speech = actor.generate_reply()
                await asyncio.wait_for(speech.wait_for_playout(), 5)
                failure = speech.exception()
            else:
                try:
                    await asyncio.wait_for(native.generate_reply(), 5)
                except llm.RealtimeError as error:
                    failure = error
                else:
                    failure = None
            assert isinstance(failure, llm.RealtimeError)
            assert native.duplex_session._failure is None and not any(
                c["type"] == "session.commentary.append" for c in commands
            ), (
                "empty canonical context sent an unusable ask or killed its owned session"
            )
            assert not native.duplex_session._history.items and not requests
            assert native.duplex_session._pending_reply_hint is None
            assert native.duplex_session._pending_reply_timer is None
            frame = rtc.AudioFrame(b"\x03\x12" * 480, 24000, 1, 480)
            if actor is not None:
                audio.frames.put_nowait(frame)
            else:
                native.push_audio(frame)
            await asyncio.wait_for(ordinary_done.wait(), 5)
            assert len(requests) == 1 and len(opened) == 1
            assert "actual caller words" in json.dumps(requests[0]["input"])
            assert "Reply to the caller now" not in json.dumps(requests[0]["input"])
            assert requests[0]["max_output_tokens"] == 123
            assert native.duplex_session._failure is None
        finally:
            audio.frames.put_nowait(None)
            if actor is not None:
                await actor.aclose()
            await native.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()


@pytest.mark.skipif(not MANUAL, reason="SDK cannot negotiate manual AgentSession turns")
@pytest.mark.parametrize("samples", [480, 960])
@pytest.mark.parametrize("message_input", [False, True])
async def test_typed_gemini_reply_preserves_original_uncommitted_capture_and_context(
    native_binary, monkeypatch, samples, message_input
):
    rejected, supported = "REJECTED_TYPED_INPUT", "SUPPORTED_AFTER_REAL_COMMIT"
    commands, pcm, converted, opened = [], bytearray(), bytearray(), []
    accepted, assistant = asyncio.Event(), asyncio.Queue()

    async def peer(request):
        opened.append(request.path)
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        setup = await ws.receive_json()
        assert setup["setup"]["realtimeInputConfig"]["automaticActivityDetection"][
            "disabled"
        ]
        await ws.send_json({"setupComplete": {}})
        async for message in ws:
            command = json.loads(message.data)
            commands.append(command)
            control = command.get("realtimeInput", {})
            if "audio" in control:
                pcm.extend(base64.b64decode(control["audio"]["data"]))
            elif "activityEnd" in control:
                await google_output(ws, "Committed original audio")
            elif command.get("clientContent", {}).get("turnComplete"):
                await google_output(ws, "Supported typed reply")
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        audio = OwnedAudioInput()
        actor = AgentSession(
            llm=model, turn_detection="manual", vad=None, stt=None, tts=None
        )
        actor.input.audio = audio
        actor.on(
            "conversation_item_added",
            lambda event: (
                assistant.put_nowait(event.item.text_content)
                if isinstance(event.item, llm.ChatMessage)
                and event.item.role == "assistant"
                else None
            ),
        )
        try:
            await start_agent(actor)
            native = next(iter(model._sessions))
            await native.start()
            push, send = native.push_audio, native._send_audio

            def captured(frame):
                push(frame)
                accepted.set()

            def sent(data):
                converted.extend(data)
                send(data)

            monkeypatch.setattr(native, "push_audio", captured)
            monkeypatch.setattr(native, "_send_audio", sent)
            raw = b"\x03\x12" * samples
            audio.frames.put_nowait(rtc.AudioFrame(raw, 48000, 1, samples))
            await asyncio.wait_for(accepted.wait(), 5)
            converter = native._resampler
            assert converter is not None and not native._audio_active
            user_input = (
                llm.ChatMessage(role="user", content=[rejected])
                if message_input
                else rejected
            )
            speech = actor.generate_reply(user_input=user_input)
            await asyncio.wait_for(speech.wait_for_playout(), 5)
            assert speech.exception() is not None
            assert native._uncommitted_source_samples == samples
            assert native._resampler is converter and not native._audio_active
            assert not pcm and not converted
            native.commit_audio()
            assert (
                await asyncio.wait_for(assistant.get(), 5) == "Committed original audio"
            )
            assert bytes(pcm) == bytes(converted)
            assert_resampled(bytes(pcm), raw)
            assert native._uncommitted_source_samples == 0
            await asyncio.wait_for(actor.wait_for_idle(), 5)
            histories = [native.chat_ctx, actor.history, actor.current_agent.chat_ctx]
            assert rejected not in json.dumps(commands), (
                "rejected typed turn reached the physical peer"
            )
            assert all(rejected not in history_text(ctx) for ctx in histories), (
                "rejected typed turn changed local history"
            )
            ordinary = actor.generate_reply(user_input=supported)
            await asyncio.wait_for(ordinary.wait_for_playout(), 5)
            assert ordinary.exception() is None
            assert await asyncio.wait_for(assistant.get(), 5) == "Supported typed reply"
            assert len(opened) == 1 and native._failure is None
            histories = [native.chat_ctx, actor.history, actor.current_agent.chat_ctx]
            assert rejected not in json.dumps(commands), (
                "rejected typed turn reached the physical peer"
            )
            assert all(rejected not in history_text(ctx) for ctx in histories), (
                "rejected typed turn changed local history"
            )
            assert supported in json.dumps(commands)
            assert [
                next(iter(c["realtimeInput"]))
                for c in commands
                if "realtimeInput" in c and "audio" not in c["realtimeInput"]
            ] == ["activityStart", "activityEnd"]
        finally:
            audio.frames.put_nowait(None)
            await actor.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.skipif(not DUPLEX, reason="SDK predates DuplexModel")
@pytest.mark.parametrize("message_input", [False, True])
async def test_gpt_scoped_typed_refusal_cannot_leak_to_later_ordinary_backend(
    native_binary, message_input
):
    rejected, scoped, supported = (
        "REJECTED_TYPED_INPUT",
        "UNSUPPORTED_SCOPED_TASK",
        "SUPPORTED_TYPED_INPUT",
    )
    commands, requests, opened = [], [], []
    assistant = asyncio.Queue()

    async def peer(request):
        if request.path == "/v1/llm/responses":
            requests.append(await request.json())
            return backend_response()
        opened.append(request.path)
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"type": "session.started"})
        async for message in ws:
            command = json.loads(message.data)
            commands.append(command)
            if command["type"] == "session.input_audio.append":
                await ws.send_json(
                    {
                        "type": "session.input_transcript.delta",
                        "delta": "ordinary caller words",
                        "start_ms": 0,
                        "end_ms": 100,
                    }
                )
                await ws.send_json(delegate("ordinary_after_scoped_refusal"))
            elif (
                command["type"] == "session.commentary.append"
                and command.get("delegation_id") is None
            ):
                await ws.send_json(delegate("ordinary_supported_typed"))
            elif command["type"] == "session.commentary.append":
                begin = 200 if len(requests) == 1 else 400
                await ws.send_json(
                    {
                        "type": "session.output_transcript.delta",
                        "delta": "ordinary answer",
                        "start_ms": begin,
                        "end_ms": begin + 100,
                    }
                )
                await ws.send_json(
                    {
                        "type": "session.output_audio.delta",
                        "delta": base64.b64encode(b"\x01\x10" * 2400).decode(),
                    }
                )
            elif command["type"] == "session.close":
                await ws.send_json({"type": "session.closed"})
                await ws.close()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        audio = OwnedAudioInput()
        actor = AgentSession(
            llm=model, turn_detection="realtime_llm", vad=None, stt=None, tts=None
        )
        actor.input.audio = audio
        actor.on(
            "conversation_item_added",
            lambda event: (
                assistant.put_nowait(event.item.text_content)
                if isinstance(event.item, llm.ChatMessage)
                and event.item.role == "assistant"
                else None
            ),
        )
        try:
            await start_agent(actor)
            activity = actor.current_agent._activity
            native = activity._rt_session
            user_input = (
                llm.ChatMessage(role="user", content=[rejected])
                if message_input
                else rejected
            )
            speech = actor.generate_reply(user_input=user_input, instructions=scoped)
            await asyncio.wait_for(speech.wait_for_playout(), 5)
            assert isinstance(speech.exception(), llm.RealtimeError)
            audio.frames.put_nowait(rtc.AudioFrame(b"\x01\x10" * 480, 24000, 1, 480))
            assert await asyncio.wait_for(assistant.get(), 5) == "ordinary answer"
            await asyncio.wait_for(actor.wait_for_idle(), 5)
            ordinary = actor.generate_reply(user_input=supported)
            await asyncio.wait_for(ordinary.wait_for_playout(), 5)
            assert ordinary.exception() is None
            assert await asyncio.wait_for(assistant.get(), 5) == "ordinary answer"
            assert len(opened) == 1 and len(requests) == 2
            assert all(body["max_output_tokens"] == 123 for body in requests)
            assert "ordinary caller words" in json.dumps(requests[0]["input"])
            assert supported in json.dumps(requests[1]["input"])
            histories = [
                native.duplex_session._history,
                native.chat_ctx,
                actor.history,
                actor.current_agent.chat_ctx,
            ]
            assert all(
                rejected not in history_text(ctx) and scoped not in history_text(ctx)
                for ctx in histories
            )
            assert rejected not in json.dumps(commands) and scoped not in json.dumps(
                commands
            ), "unsupported typed task reached the voice peer"
            assert rejected not in json.dumps(requests) and scoped not in json.dumps(
                requests
            ), "later ordinary backend executed rejected input"
            assert native.duplex_session._failure is None
        finally:
            audio.frames.put_nowait(None)
            await actor.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()
        assert native._reply_binding is None
        assert (
            getattr(activity._realtime_reply_task, "__func__", None)
            is type(activity)._realtime_reply_task
        )


@pytest.mark.skipif(
    not DUPLEX or not hasattr(llm, "RealtimeModelFallbackAdapter"),
    reason="SDK has no duplex/fallback",
)
async def test_gpt_unknown_wrapper_reply_owner_refuses_before_open(native_binary):
    from speko_gateway import GatewayError

    opened = []

    async def peer(request):
        opened.append(request.path)
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"type": "session.started"})
        async for _ in ws:
            pass
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        wrapper = llm.RealtimeModelFallbackAdapter([model])
        actor = AgentSession(
            llm=wrapper, turn_detection="realtime_llm", stt=None, tts=None, vad=None
        )
        try:
            with pytest.raises((llm.RealtimeError, GatewayError), match="ownership"):
                await start_agent(actor)
            assert not opened and not voice._attempted
        finally:
            await actor.aclose()
            await wrapper.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()


@pytest.mark.skipif(not DUPLEX, reason="SDK predates DuplexModel")
async def test_gpt_lost_existing_reply_task_binding_stops_before_context_or_reuse(
    native_binary,
):
    from speko_gateway import GatewayError

    closed, commands, requests = asyncio.Event(), [], []

    async def peer(request):
        if request.path == "/v1/llm/responses":
            requests.append(await request.json())
            return backend_response()
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"type": "session.started"})
        async for message in ws:
            commands.append(json.loads(message.data))
        closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        actor = AgentSession(
            llm=model, turn_detection="realtime_llm", stt=None, tts=None, vad=None
        )
        try:
            agent = await start_agent(actor)
            activity = agent._activity
            native = activity._rt_session
            await native.start()
            binding = native._reply_binding
            # Simulate the SDK replacing its existing owned task seam. Do not
            # create an alternative callback, task identity or provider control.
            activity._realtime_reply_task = binding.original
            speech = actor.generate_reply(
                user_input="REJECTED_AFTER_BINDING_LOSS",
                instructions="UNSUPPORTED_TASK",
            )
            await asyncio.wait_for(speech.wait_for_playout(), 5)
            await asyncio.wait_for(closed.wait(), 5)
            await asyncio.wait_for(asyncio.shield(native.duplex_session._task), 5)
            assert commands == [] and requests == []
            assert native.duplex_session._failure is not None
            assert "REJECTED_AFTER_BINDING_LOSS" not in history_text(native.chat_ctx)
            assert "REJECTED_AFTER_BINDING_LOSS" not in history_text(agent.chat_ctx)
            with pytest.raises((GatewayError, llm.RealtimeError)):
                await native.generate_reply()
        finally:
            await actor.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()
        assert native._reply_binding is None
        assert (
            getattr(activity._realtime_reply_task, "__func__", None)
            is type(activity)._realtime_reply_task
        )
