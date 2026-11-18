"""Owned SDK/sidecar peers: mode ownership, raw capture and cancellation."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
from array import array
from contextlib import asynccontextmanager

import pytest
from aiohttp import web
from livekit import rtc
from livekit.agents import AgentSession, llm
from test_native_realtime_cancellation import client_model, start_agent
from test_native_realtime_livekit import collect_generation
from test_native_sidecar import LOCAL_TOKEN, MODEL_KEY, native_stack
from test_native_sidecar import native_binary as native_binary  # noqa: PLC0414
from test_relay_speech_livekit import ControlledVAD

from speko_gateway import GatewayError, RelayRealtimeClient
from speko_gateway.livekit import RealtimeModel


async def either(*events):
    tasks = [asyncio.create_task(event.wait()) for event in events]
    try:
        done, _ = await asyncio.wait(
            tasks, timeout=5, return_when=asyncio.FIRST_COMPLETED
        )
        assert done, "owned peer produced neither expected physical outcome"
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@asynccontextmanager
async def manual_input(socket, attached, monkeypatch):
    if (
        attached
        and "can_disable_turn_detection"
        not in llm.RealtimeCapabilities.__dataclass_fields__
    ):
        pytest.skip("SDK cannot negotiate manual AgentSession input")
    client, model = client_model(socket)
    actor, audio, items = None, None, asyncio.Queue()
    if attached:
        actor = AgentSession(
            llm=model, turn_detection="manual", vad=None, stt=None, tts=None
        )
        actor.on(
            "conversation_item_added",
            lambda event: (
                items.put_nowait(event.item.text_content)
                if isinstance(event.item, llm.ChatMessage)
                and event.item.role == "assistant"
                else None
            ),
        )
        await start_agent(actor)
        native = next(iter(model._sessions))
    else:
        native = model.session(turn_detection_disabled=True)
        await native._update_session(tools=[])
    generations, accepted = asyncio.Queue(), asyncio.Queue()
    if not attached:
        native.on("generation_created", generations.put_nowait)
    original_push = native.push_audio
    original_send, converted = native._send_audio, bytearray()

    def send(data):
        converted.extend(data)
        original_send(data)

    monkeypatch.setattr(native, "_send_audio", send)

    def push(frame):
        original_push(frame)
        accepted.put_nowait(frame)

    monkeypatch.setattr(native, "push_audio", push)

    async def capture(raw):
        frame = rtc.AudioFrame(raw, 48000, 1, len(raw) // 2)
        if audio is None:
            native.push_audio(frame)
        else:
            audio.frames.put_nowait(frame)
        delivered = await asyncio.wait_for(accepted.get(), 3)
        assert delivered.sample_rate == 48000, (
            "SDK precondition: original capture must reach the adapter"
        )

    async def output(expected):
        if attached:
            assert await asyncio.wait_for(items.get(), 5) == expected
        else:
            generation = await asyncio.wait_for(generations.get(), 5)
            messages, calls = await asyncio.wait_for(collect_generation(generation), 5)
            assert calls == [] and messages[0][0] == expected

    async def reply(**options):
        if actor is not None:
            await asyncio.wait_for(
                actor.generate_reply(**options).wait_for_playout(), 5
            )
        else:
            await asyncio.wait_for(native.generate_reply(**options), 5)

    try:
        await native.start()
        yield native, capture, output, converted, reply
    finally:
        if audio is not None:
            audio.frames.put_nowait(None)
        if actor is not None:
            await actor.aclose()
        await native.aclose()
        await model.aclose()
        await client.aclose()


def assert_resampled(actual, raw):
    converter = rtc.AudioResampler(input_rate=48000, output_rate=16000, num_channels=1)
    frame = rtc.AudioFrame(raw, 48000, 1, len(raw) // 2)
    reference = b"".join(
        bytes(part.data) for part in [*converter.push(frame), *converter.flush()]
    )
    assert len(actual) == len(reference) == len(raw) // 3
    # Independent native SoX instances dither INT16 quantization. Wire bytes
    # must match the actual converter exactly; reference values differ <=2 LSB.
    assert all(
        abs(a - b) <= 2
        for a, b in zip(array("h", actual), array("h", reference), strict=True)
    )


@pytest.mark.skipif(
    not hasattr(llm, "RealtimeModelFallbackAdapter"),
    reason="SDK has no realtime fallback",
)
@pytest.mark.parametrize("mode", ["manual", "vad"])
async def test_gemini_framework_fallback_refuses_before_any_child_socket(
    native_binary, mode
):
    opened = []

    async def peer(request):
        opened.append(request.path)
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        async for _ in ws:
            pass
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        wrapper = llm.RealtimeModelFallbackAdapter([model])
        actor = AgentSession(
            llm=wrapper, turn_detection=mode, vad=ControlledVAD(), stt=None, tts=None
        )
        try:
            with pytest.raises(
                GatewayError, match="wrapper|fallback|ownership|correlated"
            ):
                await start_agent(actor)
                for native in list(model._sessions):
                    await native.start()
            assert opened == [] and not client._attempted
        finally:
            await actor.aclose()
            await wrapper.aclose()
            await client.aclose()


@pytest.mark.skipif(
    not hasattr(llm, "RealtimeModelFallbackAdapter"),
    reason="SDK has no realtime fallback",
)
async def test_gemini_later_fallback_child_cannot_open_without_direct_sdk_ownership(
    native_binary,
):
    opened = []

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        command = await ws.receive_json()
        opened.append("google" if "setup" in command else "openai")
        if "setup" in command:
            await ws.send_json({"setupComplete": {}})
        else:
            await ws.send_json(
                {"type": "session.updated", "session": command["session"]}
            )
        async for message in ws:
            command = json.loads(message.data)
            if command.get("type") == "session.update":
                await ws.send_json(
                    {"type": "session.updated", "session": command["session"]}
                )
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        google_client, google = client_model(socket)
        first_client = RelayRealtimeClient(
            api_key=MODEL_KEY,
            session_id="ses_owned_swap",
            socket_path=socket,
            local_auth_token=LOCAL_TOKEN,
        )
        first = RealtimeModel(
            first_client,
            protocol="openai.realtime.v1",
            model="gpt-realtime-2.1",
            voice="marin",
        )
        wrapper = llm.RealtimeModelFallbackAdapter(
            [first, google], regenerate_on_swap=False
        )
        actor = AgentSession(
            llm=wrapper, turn_detection="manual", stt=None, tts=None, vad=None
        )
        try:
            await start_agent(actor)
            await next(iter(first._sessions)).start()
            await wrapper.restart_session(switch_model=True)
            for native in list(google._sessions):
                with contextlib.suppress(GatewayError, llm.RealtimeError):
                    await native.start()
            assert opened == ["openai"] and not google_client._attempted, (
                "fallback swap opened an unowned Gemini transport"
            )
        finally:
            await actor.aclose()
            await wrapper.aclose()
            await google_client.aclose()
            await first_client.aclose()


async def test_gemini_live_turn_mode_change_stops_before_uncorrelated_pcm(
    native_binary,
):
    if (
        "can_disable_turn_detection"
        not in llm.RealtimeCapabilities.__dataclass_fields__
    ):
        pytest.skip("SDK cannot negotiate manual AgentSession turns")
    closed, received = asyncio.Event(), asyncio.Event()
    controls = []

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        setup = await ws.receive_json()
        assert setup["setup"]["realtimeInputConfig"]["automaticActivityDetection"][
            "disabled"
        ]
        await ws.send_json({"setupComplete": {}})
        async for message in ws:
            control = json.loads(message.data)
            controls.append(control)
            if "audio" in control.get("realtimeInput", {}):
                received.set()
        closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        actor = AgentSession(
            llm=model, turn_detection="manual", vad=ControlledVAD(), stt=None, tts=None
        )
        try:
            await start_agent(actor)
            native = next(iter(model._sessions))
            await native.start()
            with contextlib.suppress(GatewayError, llm.RealtimeError):
                actor.update_options(turn_detection="vad")
            with contextlib.suppress(GatewayError, llm.RealtimeError):
                native.push_audio(rtc.AudioFrame(b"\x05\x21" * 320, 16000, 1, 320))
            await either(closed, received)
            assert closed.is_set() and not received.is_set(), (
                "live mode change forwarded uncorrelated caller PCM"
            )
            await asyncio.wait_for(asyncio.shield(native._task), 5)
            assert native._failure is not None and controls == []
        finally:
            await actor.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("attached", [True, False])
@pytest.mark.parametrize("samples", [480, 960])
async def test_gemini_reply_cannot_overtake_raw_resampler_capture(
    native_binary, monkeypatch, attached, samples
):
    controls, pcm = [], bytearray()
    ended, opened = asyncio.Event(), []

    async def peer(request):
        opened.append(request.path)
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        async for message in ws:
            command = json.loads(message.data)
            controls.append(command)
            control = command.get("realtimeInput", {})
            if "audio" in control:
                pcm.extend(base64.b64decode(control["audio"]["data"]))
            text = None
            if "activityEnd" in control:
                ended.set()
                text = "committed input"
            elif "clientContent" in command:
                text = "separate reply"
            if text:
                await ws.send_json(
                    {"serverContent": {"outputTranscription": {"text": text}}}
                )
                await ws.send_json(
                    {
                        "serverContent": {
                            "modelTurn": {
                                "parts": [
                                    {
                                        "inlineData": {
                                            "mimeType": "audio/pcm;rate=24000",
                                            "data": base64.b64encode(
                                                b"\x01\x10" * 2400
                                            ).decode(),
                                        }
                                    }
                                ]
                            }
                        }
                    }
                )
                await ws.send_json({"serverContent": {"turnComplete": True}})
        return ws

    async with (
        native_stack(native_binary, peer) as (socket, _),
        manual_input(socket, attached, monkeypatch) as (
            native,
            capture,
            output,
            converted,
            reply,
        ),
    ):
        raw = b"\x03\x12" * samples
        await capture(raw)
        assert not native._audio_active, (
            "SDK precondition: short original capture must still be local"
        )
        rejected = False
        try:
            await asyncio.wait_for(
                native.generate_reply(instructions="do not overtake source"), 5
            )
        except llm.RealtimeError:
            rejected = True
        assert rejected and controls == [], (
            "reply physically overtook uncommitted original resampler input"
        )
        native.commit_audio()
        if not attached:
            await reply()
        await output("committed input")
        assert ended.is_set() and bytes(pcm) == bytes(converted)
        assert_resampled(bytes(pcm), raw)
        assert [
            next(iter(control["realtimeInput"]))
            for control in controls
            if "audio" not in control.get("realtimeInput", {})
        ] == ["activityStart", "activityEnd"]
        await reply(instructions="after real commit")
        await output("separate reply")
        assert len(opened) == 1 and native._failure is None


@pytest.mark.parametrize("attached", [True, False])
@pytest.mark.parametrize("samples", [480, 960])
async def test_gemini_clear_discards_original_local_capture_before_conversion(
    native_binary, monkeypatch, attached, samples
):
    controls, pcm, before_barrier = [], bytearray(), []
    barrier, ended = asyncio.Event(), asyncio.Event()

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        async for message in ws:
            command = json.loads(message.data)
            if "clientContent" in command:
                barrier.set()
                continue
            if not barrier.is_set():
                before_barrier.append(command)
            controls.append(command)
            control = command["realtimeInput"]
            if "audio" in control:
                pcm.extend(base64.b64decode(control["audio"]["data"]))
            if "activityEnd" in control:
                ended.set()
        return ws

    async with (
        native_stack(native_binary, peer) as (socket, _),
        manual_input(socket, attached, monkeypatch) as (
            native,
            capture,
            _,
            converted,
            _,
        ),
    ):
        await capture(b"\x00\x10" * samples)
        assert not native._audio_active, (
            "SDK precondition: original capture must remain in the resampler"
        )
        native.clear_audio()
        native.commit_audio()
        context = native.chat_ctx
        context.add_message(role="user", content="AFTER_EMPTY_CLEAR_BARRIER")
        await native.update_chat_ctx(context)
        await asyncio.wait_for(barrier.wait(), 5)
        assert before_barrier == [], (
            "cleared original samples leaked physically before the typed barrier"
        )
        raw = b"\x00\x50" * samples
        await capture(raw)
        native.commit_audio()
        await asyncio.wait_for(ended.wait(), 5)
        assert bytes(pcm) == bytes(converted)
        assert_resampled(bytes(pcm), raw)
        assert len(controls) >= 3 and native._failure is None


@pytest.mark.parametrize("before_output", [True, False])
async def test_gemini_agent_explicit_cancel_stops_owned_provider_request(
    native_binary, before_output
):
    manual = (
        before_output
        and "can_disable_turn_detection"
        in llm.RealtimeCapabilities.__dataclass_fields__
    )
    ask, output_seen, barrier, closed = (asyncio.Event() for _ in range(4))
    release, controls, errors = asyncio.Event(), [], []

    async def peer(request):
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
        control = await ws.receive_json()
        controls.append(control)
        ask.set()

        async def generate():
            if before_output:
                await release.wait()
            await ws.send_json(
                {"serverContent": {"outputTranscription": {"text": "ongoing reply"}}}
            )
            while not ws.closed:
                await ws.send_json(
                    {
                        "serverContent": {
                            "modelTurn": {
                                "parts": [
                                    {
                                        "inlineData": {
                                            "mimeType": "audio/pcm;rate=24000",
                                            "data": base64.b64encode(
                                                b"\x01\x10" * 480
                                            ).decode(),
                                        }
                                    }
                                ]
                            }
                        }
                    }
                )
                await asyncio.sleep(0.02)

        producer = asyncio.create_task(generate())
        try:
            async for message in ws:
                command = json.loads(message.data)
                controls.append(command)
                if "clientContent" in command:
                    barrier.set()
        finally:
            producer.cancel()
            await asyncio.gather(producer, return_exceptions=True)
            closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        # Select provider VAD explicitly on the old SDK; it cannot negotiate manual turns.
        actor = AgentSession(
            llm=model,
            turn_detection="manual" if manual else "realtime_llm",
            stt=None,
            tts=None,
            vad=None,
        )
        try:
            await start_agent(actor)
            native = next(iter(model._sessions))
            await native.start()
            native.on("generation_created", lambda _: output_seen.set())
            native.on("error", errors.append)
            speech = actor.generate_reply(instructions="first bounded reply")
            await asyncio.wait_for(ask.wait(), 5)
            if not before_output:
                await asyncio.wait_for(output_seen.wait(), 5)
            await asyncio.wait_for(actor.interrupt(), 5)
            context = native.chat_ctx
            context.add_message(role="user", content="AFTER_CANCEL_OUTBOUND_BARRIER")
            with contextlib.suppress(llm.RealtimeError, GatewayError):
                await native.update_chat_ctx(context)
            await either(closed, barrier)
            assert closed.is_set() and not barrier.is_set(), (
                "explicit cancellation left the provider request live behind an outbound barrier"
            )
            await asyncio.wait_for(asyncio.shield(native._task), 5)
            assert (
                not native._pending
                and not native._pending_timers
                and speech.interrupted
            )
            rejected = native.generate_reply(
                instructions="cannot reuse stopped attempt"
            )
            with pytest.raises(llm.RealtimeError, match="closed"):
                await rejected
        finally:
            release.set()
            await actor.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.skipif(
    "can_disable_turn_detection" in llm.RealtimeCapabilities.__dataclass_fields__,
    reason="SDK can negotiate disabled provider VAD",
)
async def test_gemini_unnegotiated_sdk_manual_mode_refuses_before_open(native_binary):
    opened = []

    async def peer(request):
        opened.append(request.path)
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        async for _ in ws:
            pass
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        actor = AgentSession(
            llm=model, turn_detection="manual", stt=None, tts=None, vad=None
        )
        try:
            with pytest.raises(GatewayError, match="negotiated turn ownership"):
                await start_agent(actor)
                for native in list(model._sessions):
                    await native.start()
            assert not opened and not client._attempted
        finally:
            await actor.aclose()
            await model.aclose()
            await client.aclose()
