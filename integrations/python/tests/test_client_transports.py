from __future__ import annotations

import asyncio
import dataclasses
import inspect
import tempfile
import uuid
from contextlib import asynccontextmanager
from importlib.metadata import version
from pathlib import Path
from types import SimpleNamespace

import aiohttp
import pytest
from aiohttp import web

from speko_gateway._livekit_bridge import LiveKitSTTBridge
from speko_gateway.client import GatewayClient, SessionConfig
from speko_gateway.relay import RelayError, RelayLLMClient


@asynccontextmanager
async def server(app: web.Application, *, unix: bool = False):
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    # Keep the Unix path short enough for macOS as well as Linux.
    with tempfile.TemporaryDirectory(prefix="gateway-test-", dir="/tmp") as tmp:
        try:
            if unix:
                address = str(Path(tmp) / "gateway.sock")
                site = web.UnixSite(runner, address)
            else:
                site = web.TCPSite(runner, "127.0.0.1", 0)
            await site.start()
            if not unix:
                address = f"http://127.0.0.1:{runner.addresses[0][1]}"
            yield address
        finally:
            await runner.cleanup()


def assert_marker(headers) -> None:
    assert headers.getall("User-Agent") == [f"speko-gateway/{version('speko-gateway')}"]


@asynccontextmanager
async def gateway_stt_peer(payloads, *, after_eos=False, final_gate=None):
    async def create(request):
        await request.json()
        return web.json_response({"stream_url": "/v1/sessions/fixture/stream"}, status=201)

    async def websocket(request):
        ws = web.WebSocketResponse(protocols=["speko.voice.v0.r3"])
        await ws.prepare(request)
        index = 0
        async for message in ws:
            if message.type is aiohttp.WSMsgType.BINARY:
                final, ended = ("transcript.final", payloads(index)), ("speech.ended", {})
                for kind, data in [("speech.started", {}), *( [ended, final] if after_eos else [final, ended])]:
                    if kind == "transcript.final" and final_gate is not None:
                        await final_gate.wait()
                    await ws.send_json({"type": kind, "data": data})
                index += 1
            elif message.type is aiohttp.WSMsgType.TEXT and message.json()["type"] == "session.close":
                await ws.close()
        return ws

    app = web.Application()
    app.router.add_post("/v1/sessions", create)
    app.router.add_get("/v1/sessions/fixture/stream", websocket)
    async with server(app, unix=True) as address:
        yield address


@pytest.mark.parametrize("shape", ["terminal", "middle", "whole"])
@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize("manual", [False, True])
@pytest.mark.parametrize("after_eos", [False, True])
async def test_gateway_background_suppression_keeps_filtered_canonical_turns(shape, nested, manual, after_eos):
    from livekit import rtc
    from livekit.agents import Agent, AgentSession, stt
    from livekit.agents.voice import io

    from speko_gateway.livekit import STT

    turns, items, frames, finals, envelopes = asyncio.Queue(), asyncio.Queue(), asyncio.Queue(), [], []
    first, release = asyncio.Event(), asyncio.Event()

    class Input(io.AudioInput):
        def __init__(self):
            super().__init__(label="owned-filtered-input")

        async def __anext__(self):
            value = await frames.get()
            if value is None:
                raise StopAsyncIteration
            return value

    class OwnedAgent(Agent):
        async def stt_node(self, audio, settings):
            async for event in Agent.default.stt_node(self, audio, settings):
                event = dataclasses.replace(event, alternatives=[dataclasses.replace(data) for data in event.alternatives])
                if event.type is stt.SpeechEventType.FINAL_TRANSCRIPT:
                    finals.extend(data.text for data in event.alternatives if data.text)
                    envelopes.append(dataclasses.asdict(event))
                yield event
                if event.type is stt.SpeechEventType.FINAL_TRANSCRIPT and event.alternatives[0].text and not first.is_set():
                    first.set()
                    if manual and shape != "whole":
                        await release.wait()

        async def on_user_turn_completed(self, turn_ctx, new_message):
            turns.put_nowait(new_message.text_content)

    def payload(index):
        offset = 1000 * index
        if shape == "whole":
            text = ["甲", "乙", "丙"][index]
            parts = [(text, 3 if index == 1 else 2, 0, 900)]
        else:
            text = "你好" if shape == "terminal" else "你好吗"
            parts = [("你", 2, 0, 300), ("好", 3, 300, 600)]
            if shape == "middle":
                parts.append(("吗", 2, 600, 900))
        return {"text": text, "segments": [{"text": word, "speaker": speaker, "start_ms": start + offset, "end_ms": end + offset} for word, speaker, start, end in parts]}

    async with gateway_stt_peer(payload, after_eos=after_eos) as address:
        client = GatewayClient(socket_path=address, local_auth_token="owned-filtered-canonical")
        model = STT(client, diarization=True)
        speaker = stt.MultiSpeakerAdapter(stt=model, suppress_background_speaker=True)
        wrapped = stt.FallbackAdapter([speaker], max_retry_per_stt=0) if nested else speaker
        session = AgentSession(stt=wrapped, llm=None, tts=None, vad=None, turn_detection="stt", min_endpointing_delay=0, max_endpointing_delay=0)
        session.input.audio = Input()
        session.on("conversation_item_added", lambda event: items.put_nowait(event.item.text_content) if getattr(event.item, "role", None) == "user" else None)
        try:
            options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
            await session.start(agent=OwnedAgent(instructions="Owned filtered canonical turns"), **options)
            for index in range(3 if shape == "whole" else 2):
                loud, quiet = b"\xff\x3f", b"\x20\x00"
                pcm = (quiet if index == 1 else loud) * 16000 if shape == "whole" else loud * 4800 + quiet * 4800 + loud * 6400
                await frames.put(rtc.AudioFrame(data=pcm, sample_rate=16000, num_channels=1, samples_per_channel=16000))
                if manual and shape != "whole" and index == 0:
                    await asyncio.wait_for(first.wait(), 3)
                    session.commit_user_turn(transcript_timeout=0, skip_reply=True)
                    await asyncio.sleep(0.05)
                    assert turns.empty() and items.empty()
                    release.set()
                if shape == "whole" and index == 1:
                    continue
                expected = ["甲", "乙", "丙"][index] if shape == "whole" else "你" if shape == "terminal" else "你吗"
                assert await asyncio.wait_for((items if manual and shape != "whole" and index == 0 else turns).get(), 3) == expected
            assert "好" not in finals and "乙" not in finals
            assert all("好" not in repr(event) and "乙" not in repr(event) for event in envelopes), "suppressed content must also be absent from all outward event metadata"
        finally:
            release.set()
            await frames.put(None)
            await session.aclose()
            await wrapped.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("mode", ["manual", "vad"])
@pytest.mark.parametrize("nested", [False, True])
async def test_gateway_late_final_obeys_the_active_framework_turn_policy(mode, nested):
    from livekit import rtc
    from livekit.agents import Agent, AgentSession, stt, vad
    from livekit.agents.voice import io
    from test_relay_speech_livekit import ControlledVAD

    from speko_gateway.livekit import STT

    frames, items, turns = asyncio.Queue(), asyncio.Queue(), asyncio.Queue()
    allowed, final_received = asyncio.Event(), asyncio.Event()

    class Input(io.AudioInput):
        def __init__(self):
            super().__init__(label="owned-late-policy-input")

        async def __anext__(self):
            value = await frames.get()
            if value is None:
                raise StopAsyncIteration
            return value

    class OwnedAgent(Agent):
        async def stt_node(self, audio, settings):
            async for event in Agent.default.stt_node(self, audio, settings):
                yield event
                if event.type is stt.SpeechEventType.FINAL_TRANSCRIPT:
                    final_received.set()

        async def on_user_turn_completed(self, turn_ctx, new_message):
            turns.put_nowait(new_message.text_content)

    detector = ControlledVAD() if mode == "vad" else None
    async with gateway_stt_peer(lambda _: {"text": "hello"}, after_eos=True, final_gate=allowed) as address:
        client = GatewayClient(socket_path=address, local_auth_token="owned-late-policy")
        model = STT(client)
        wrapped = stt.FallbackAdapter([model], max_retry_per_stt=0) if nested else model
        session = AgentSession(stt=wrapped, llm=None, tts=None, vad=detector, turn_detection=mode, min_endpointing_delay=0, max_endpointing_delay=0)
        session.input.audio = Input()
        session.on("conversation_item_added", lambda event: items.put_nowait(event.item.text_content) if getattr(event.item, "role", None) == "user" else None)
        try:
            options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
            await session.start(agent=OwnedAgent(instructions="Owned late final policy"), **options)
            await frames.put(rtc.AudioFrame(data=b"\xff\x0f" * 320, sample_rate=16000, num_channels=1, samples_per_channel=320))
            if detector is not None:
                value = await asyncio.wait_for(detector.frames.get(), 3)
                detector.processed(value)
                detector.emit(vad.VADEventType.START_OF_SPEECH)
                await asyncio.sleep(0.02)
                assert session._activity._audio_recognition._speaking
            allowed.set()
            await asyncio.wait_for(final_received.wait(), 3)
            await asyncio.sleep(0.15)
            assert items.empty() and turns.empty(), "late final must wait for a manual commit or VAD end"
            if mode == "manual":
                await asyncio.wait_for(session.commit_user_turn(transcript_timeout=0, stt_flush_duration=0, skip_reply=True), 3)
            else:
                detector.emit(vad.VADEventType.END_OF_SPEECH, silence_duration=0.02)
            assert await asyncio.wait_for((items if mode == "manual" else turns).get(), 3) == "hello"
        finally:
            allowed.set()
            await frames.put(None)
            await session.aclose()
            await wrapped.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize("clear", [False, True])
async def test_gateway_deferred_manual_commit_waits_for_vad_end_and_keeps_skip_reply(nested, clear):
    from livekit import rtc
    from livekit.agents import Agent, AgentSession, stt, vad
    from livekit.agents.voice import io
    from test_relay_speech_livekit import ControlledVAD

    from speko_gateway.livekit import STT

    frames, items, replies = asyncio.Queue(), asyncio.Queue(), []
    first, release, final = asyncio.Event(), asyncio.Event(), asyncio.Event()
    allowed = asyncio.Event()

    class Input(io.AudioInput):
        def __init__(self):
            super().__init__(label="owned-deferred-manual-input")

        async def __anext__(self):
            value = await frames.get()
            if value is None:
                raise StopAsyncIteration
            return value

    class OwnedAgent(Agent):
        async def stt_node(self, audio, settings):
            async for event in Agent.default.stt_node(self, audio, settings):
                yield event
                if event.type is stt.SpeechEventType.FINAL_TRANSCRIPT:
                    if not first.is_set():
                        first.set()
                        await release.wait()
                    else:
                        final.set()

        async def on_user_turn_completed(self, turn_ctx, new_message):
            replies.append(new_message.text_content)

    payload = {"text": "hello world", "segments": [{"text": "hello", "speaker": 2, "start_ms": 0, "end_ms": 10}, {"text": "world", "speaker": 3, "start_ms": 10, "end_ms": 20}]}
    async with gateway_stt_peer(lambda _: payload, after_eos=True, final_gate=allowed) as address:
        client = GatewayClient(socket_path=address, local_auth_token="owned-deferred-manual")
        model = STT(client, diarization=True)
        wrapped = stt.FallbackAdapter([stt.MultiSpeakerAdapter(stt=model)], max_retry_per_stt=0) if nested else model
        detector = ControlledVAD()
        session = AgentSession(stt=wrapped, llm=None, tts=None, vad=detector, turn_detection="manual", min_endpointing_delay=0, max_endpointing_delay=0)
        session.input.audio = Input()
        session.on("conversation_item_added", lambda event: items.put_nowait(event.item.text_content) if getattr(event.item, "role", None) == "user" else None)
        try:
            options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
            await session.start(agent=OwnedAgent(instructions="Owned deferred manual policy"), **options)
            await frames.put(rtc.AudioFrame(data=b"\xff\x0f" * 320, sample_rate=16000, num_channels=1, samples_per_channel=320))
            value = await asyncio.wait_for(detector.frames.get(), 3)
            detector.processed(value)
            detector.emit(vad.VADEventType.START_OF_SPEECH)
            allowed.set()
            await asyncio.wait_for(first.wait(), 3)
            session.commit_user_turn(transcript_timeout=0, stt_flush_duration=0, skip_reply=True)
            binding = session._activity._speko_local_transcript_binding

            async def deferred():
                while binding.deferred is None:
                    await asyncio.sleep(0)

            await asyncio.wait_for(deferred(), 3)
            detector.emit(vad.VADEventType.START_OF_SPEECH)
            await asyncio.sleep(0.02)
            assert session._activity._audio_recognition._speaking
            release.set()
            await asyncio.wait_for(final.wait(), 3)
            await asyncio.sleep(0.05)
            assert items.empty(), "the explicit decision must wait while speech is active"
            if clear:
                session.clear_user_turn()
                assert binding.deferred is None
                final.clear()
                await frames.put(rtc.AudioFrame(data=b"\xff\x0f" * 320, sample_rate=16000, num_channels=1, samples_per_channel=320))
                value = await asyncio.wait_for(detector.frames.get(), 3)
                detector.processed(value)
                detector.emit(vad.VADEventType.START_OF_SPEECH)
                await asyncio.wait_for(final.wait(), 3)
            detector.emit(vad.VADEventType.END_OF_SPEECH, silence_duration=0.02)
            if clear:
                await asyncio.sleep(0.1)
                assert items.empty(), "a cleared deferred decision cannot commit the later utterance"
                session.commit_user_turn(transcript_timeout=0, stt_flush_duration=0, skip_reply=True)
            assert await asyncio.wait_for(items.get(), 3) == "hello world"
            assert replies == []
            recognition = session._activity._audio_recognition
            await model.aclose()
            assert recognition._on_vad_event.__func__ is type(recognition)._on_vad_event
            clear_method = "_clear_user_turn" if hasattr(recognition, "_clear_user_turn") else "clear_user_turn"
            assert getattr(recognition, clear_method).__func__ is getattr(type(recognition), clear_method)
        finally:
            allowed.set()
            release.set()
            await frames.put(None)
            await session.aclose()
            await wrapped.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("nested", [False, True])
async def test_gateway_partial_final_does_not_gate_a_replacement_stt(nested):
    from livekit import rtc
    from livekit.agents import DEFAULT_API_CONNECT_OPTIONS, Agent, AgentSession, stt
    from livekit.agents.voice import io

    from speko_gateway.livekit import STT

    if not hasattr(Agent, "update_options"):
        pytest.skip("SDK predates live model option updates")
    frames, turns = asyncio.Queue(), asyncio.Queue()
    first, release, started = asyncio.Event(), asyncio.Event(), asyncio.Event()

    class Input(io.AudioInput):
        def __init__(self):
            super().__init__(label="owned-replacement-input")

        async def __anext__(self):
            value = await frames.get()
            if value is None:
                raise StopAsyncIteration
            return value

    class Replacement(stt.STT):
        def __init__(self):
            super().__init__(capabilities=stt.STTCapabilities(streaming=True, interim_results=False))

        async def _recognize_impl(self, *args, **kwargs):
            raise NotImplementedError

        def stream(self, *, language=None, conn_options=DEFAULT_API_CONNECT_OPTIONS):
            return ReplacementStream(stt=self, conn_options=conn_options)

    class ReplacementStream(stt.RecognizeStream):
        async def _run(self):
            started.set()
            sent = False
            async for value in self._input_ch:
                if isinstance(value, rtc.AudioFrame) and not sent:
                    sent = True
                    for kind in [stt.SpeechEventType.START_OF_SPEECH, stt.SpeechEventType.FINAL_TRANSCRIPT, stt.SpeechEventType.END_OF_SPEECH]:
                        self._event_ch.send_nowait(stt.SpeechEvent(type=kind, alternatives=[stt.SpeechData(language="en", text="replacement" if kind is stt.SpeechEventType.FINAL_TRANSCRIPT else "")]))

    class OwnedAgent(Agent):
        async def stt_node(self, audio, settings):
            async for event in Agent.default.stt_node(self, audio, settings):
                yield event
                if event.type is stt.SpeechEventType.FINAL_TRANSCRIPT and getattr(event.alternatives[0], "canonical_text", None) == "":
                    first.set()
                    await release.wait()

        async def on_user_turn_completed(self, turn_ctx, new_message):
            turns.put_nowait(new_message.text_content)

    payload = {"text": "你好", "segments": [{"text": "你", "speaker": 2, "start_ms": 0, "end_ms": 400}, {"text": "好", "speaker": 3, "start_ms": 400, "end_ms": 800}]}
    async with gateway_stt_peer(lambda _: payload) as address:
        client = GatewayClient(socket_path=address, local_auth_token="owned-replacement-canonical")
        model, replacement = STT(client, diarization=True), Replacement()
        wrapped = stt.FallbackAdapter([stt.MultiSpeakerAdapter(stt=model)], max_retry_per_stt=0) if nested else model
        agent = OwnedAgent(instructions="Owned local recognition replacement")
        session = AgentSession(stt=wrapped, llm=None, tts=None, vad=None, turn_detection="stt", min_endpointing_delay=0, max_endpointing_delay=0)
        session.input.audio = Input()
        try:
            options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
            await session.start(agent=agent, **options)
            await frames.put(rtc.AudioFrame(data=b"\xff\x0f" * 16000, sample_rate=16000, num_channels=1, samples_per_channel=16000))
            await asyncio.wait_for(first.wait(), 3)
            binding = session._activity._speko_local_transcript_binding
            async def pending():
                while not binding.pending:
                    await asyncio.sleep(0)
            await asyncio.wait_for(pending(), 3)
            original_turn = binding.turn[0]
            agent.update_options(stt=replacement)
            await asyncio.wait_for(started.wait(), 3)
            expected = f"{session._activity._audio_recognition._audio_transcript} replacement".lstrip()
            await frames.put(rtc.AudioFrame(data=b"\xff\x0f" * 320, sample_rate=16000, num_channels=1, samples_per_channel=320))
            assert await asyncio.wait_for(turns.get(), 3) == expected
            assert binding.processing is None and binding.turn is None
            assert session._activity.on_end_of_turn == original_turn
        finally:
            release.set()
            await frames.put(None)
            await session.aclose()
            await wrapped.aclose()
            await model.aclose()
            await replacement.aclose()
            await client.aclose()


async def test_router_native_sse_preserves_headers_body_and_streaming() -> None:
    seen = []
    first_consumed = asyncio.Event()

    async def responses(request):
        seen.append((request.headers.copy(), await request.json()))
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)
        await response.write(
            b'event: response.output_text.delta\ndata: {"delta":"synthetic"}\n\n'
        )
        await asyncio.wait_for(first_consumed.wait(), 3)
        await response.write(
            b'event: response.completed\ndata: {"status":"completed"}\n\n'
        )
        await response.write_eof()
        return response

    app = web.Application()
    app.router.add_post("/v1/llm/responses", responses)
    async with server(app) as address:
        client = RelayLLMClient(api_key="router-fixture-only", base_url=address)
        body = {"model": "synthetic", "input": [], "stream": False}
        try:
            stream = client.stream_response(body)
            assert await asyncio.wait_for(anext(stream), 3) == (
                "response.output_text.delta",
                {"delta": "synthetic"},
            )
            first_consumed.set()
            assert await asyncio.wait_for(anext(stream), 3) == (
                "response.completed",
                {"status": "completed"},
            )
            with pytest.raises(StopAsyncIteration):
                await asyncio.wait_for(anext(stream), 3)
        finally:
            first_consumed.set()
            await client.aclose()

    assert len(seen) == 1
    headers, sent = seen[0]
    assert_marker(headers)
    assert headers["Authorization"] == "Bearer router-fixture-only"
    assert headers["Accept"] == "text/event-stream"
    assert headers["Content-Type"] == "application/json"
    assert uuid.UUID(headers["Idempotency-Key"]).version == 4
    assert sent == {**body, "stream": True}
    assert body["stream"] is False


async def test_router_native_rejection_keeps_error_and_does_not_retry() -> None:
    seen = []

    async def reject(request):
        seen.append(request.headers.copy())
        await request.read()
        return web.json_response(
            {
                "error": {
                    "code": "unauthorized",
                    "retryable": False,
                    "request_id": "router-fixture-request",
                }
            },
            status=401,
        )

    app = web.Application()
    app.router.add_post("/v1/llm/responses", reject)
    async with server(app) as address:
        client = RelayLLMClient(api_key="router-fixture-only", base_url=address)
        try:
            with pytest.raises(RelayError) as caught:
                await asyncio.wait_for(anext(client.stream_response({"input": []})), 3)
            assert caught.value.code == "unauthorized"
            assert caught.value.retryable is False
            assert caught.value.request_id == "router-fixture-request"
        finally:
            await client.aclose()
    assert len(seen) == 1
    assert_marker(seen[0])


async def test_gateway_native_unix_http_and_websocket_preserve_protocol() -> None:
    seen = []
    commands = []
    stream_consumed = asyncio.Event()
    subprotocol = "speko.voice.v0.r3"
    turn_events = [{"type": "turn.started", "data": {"initiator": "agent"}}]
    config = SessionConfig(
        kind="tts",
        execution={"route": "provider_direct", "credential_source": "byok"},
        request={"voice": "synthetic"},
        media={"encoding": "pcm_s16le", "sample_rate_hz": 24000, "channels": 1},
        integration={"name": "fixture", "version": "0.1.0"},
    )

    async def record(request):
        body = await request.json() if request.can_read_body else None
        seen.append((request.path, request.headers.copy(), body))
        if request.path == "/v1/sessions":
            return web.json_response(
                {"stream_url": "/v1/sessions/fixture/stream"}, status=201
            )
        return web.json_response({})

    async def websocket(request):
        seen.append((request.path, request.headers.copy(), None))
        response = web.WebSocketResponse(protocols=[subprotocol])
        await response.prepare(request)
        commands.append(await response.receive_json(timeout=3))
        commands.append(await response.receive_json(timeout=3))
        await response.send_bytes(b"\x00\x01")
        # The native client must yield the frame before the server closes.
        await asyncio.wait_for(stream_consumed.wait(), 3)
        await response.send_json({"type": "audio.done", "data": {"ok": True}})
        commands.append(await response.receive_json(timeout=3))
        await response.close()
        return response

    app = web.Application()
    app.router.add_get("/readyz", record)
    app.router.add_post("/v1/turn-events", record)
    app.router.add_post("/v1/sessions", record)
    app.router.add_get("/v1/sessions/fixture/stream", websocket)
    async with server(app, unix=True) as address:
        client = GatewayClient(
            socket_path=address, local_auth_token="local-fixture-only"
        )
        session = None
        try:
            assert await asyncio.wait_for(client.ready(), 3) is True
            await asyncio.wait_for(client.post_turn_events(turn_events), 3)
            session = await asyncio.wait_for(
                client.open(config, idempotency_key="caller-fixture-idempotency"), 3
            )
            await session.append_text("synthetic")
            await session.commit_text()
            events = session.events()
            first = await asyncio.wait_for(anext(events), 3)
            assert first.type == "audio.frame" and first.audio == b"\x00\x01"
            stream_consumed.set()
            second = await asyncio.wait_for(anext(events), 3)
            assert second.type == "audio.done" and second.data == {"ok": True}
            await session.finish()
            with pytest.raises(StopAsyncIteration):
                await asyncio.wait_for(anext(events), 3)
        finally:
            stream_consumed.set()
            if session is not None:
                await session.aclose()
            await client.aclose()

    assert [path for path, _, _ in seen] == [
        "/readyz",
        "/v1/turn-events",
        "/v1/sessions",
        "/v1/sessions/fixture/stream",
    ]
    for _, headers, _ in seen:
        assert_marker(headers)
        assert headers["Authorization"] == "Bearer local-fixture-only"
        assert headers["Host"] == "speko-gateway"
    assert seen[1][2] == {"events": turn_events}
    assert seen[2][2] == config.as_json()
    assert seen[2][1]["Idempotency-Key"] == "caller-fixture-idempotency"
    assert seen[2][1]["Content-Type"] == "application/json"
    assert seen[3][1]["Sec-WebSocket-Protocol"] == subprotocol
    assert commands == [
        {"type": "text.append", "data": {"text": "synthetic"}},
        {"type": "text.commit", "data": None},
        {"type": "session.close", "data": None},
    ]


async def test_marker_is_not_a_global_aiohttp_default() -> None:
    seen = []

    async def capture(request):
        seen.append(request.headers.copy())
        return web.Response()

    app = web.Application()
    app.router.add_get("/unrelated", capture)
    async with (
        server(app) as address,
        aiohttp.ClientSession(headers={"User-Agent": "caller-fixture"}) as s,
        s.get(f"{address}/unrelated") as response,
    ):
        await response.read()
    assert len(seen) == 1
    assert seen[0].getall("User-Agent") == ["caller-fixture"]
    assert "Authorization" not in seen[0]


@pytest.mark.parametrize("kind,speaker", [
    ("transcript.delta", 2),
    ("transcript.final", 2),
    ("transcript.final", "A"),
])
async def test_local_gateway_preserves_modulate_speaker_labels(kind, speaker):
    async def create(request):
        await request.json()
        return web.json_response({"stream_url": "/v1/sessions/fixture/stream"}, status=201)

    async def websocket(request):
        ws = web.WebSocketResponse(protocols=["speko.voice.v0.r3"])
        await ws.prepare(request)
        await ws.send_json({"type": kind, "data": {"text": "synthetic", "speaker": speaker}})
        await ws.receive_json(timeout=3)
        await ws.close()
        return ws

    app = web.Application()
    app.router.add_post("/v1/sessions", create)
    app.router.add_get("/v1/sessions/fixture/stream", websocket)
    async with server(app, unix=True) as address:
        client = GatewayClient(socket_path=address, local_auth_token="owned-local-speaker")
        stream = None
        try:
            stream = await LiveKitSTTBridge(client).start(SimpleNamespace(sample_rate=16000, num_channels=1))
            await stream.finish()
            events = [event async for event in stream.events()]
            assert [(event.type, event.text, event.speaker_id) for event in events] == [(kind, "synthetic", str(speaker))]
        finally:
            if stream is not None:
                await stream.aclose()
            await client.aclose()


@pytest.mark.parametrize("partial", [False, True])
async def test_local_gateway_sdk_final_retains_canonical_text_and_segment_metadata(partial):
    from livekit import rtc

    from speko_gateway.livekit import STT

    text = "hello world" if partial else "你好"
    segments = [{"text": "hello" if partial else "你", "speaker": 2, "start_ms": 0, "end_ms": 10}]
    if not partial:
        segments.append({"text": "好", "speaker": 3, "start_ms": 10, "end_ms": 20})

    async def create(request):
        await request.json()
        return web.json_response({"stream_url": "/v1/sessions/fixture/stream"}, status=201)

    async def websocket(request):
        ws = web.WebSocketResponse(protocols=["speko.voice.v0.r3"])
        await ws.prepare(request)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.TEXT and message.json()["type"] == "session.close":
                await ws.send_json({"type": "transcript.final", "data": {"text": text, "segments": segments}})
                await ws.close()
        return ws

    app = web.Application()
    app.router.add_post("/v1/sessions", create)
    app.router.add_get("/v1/sessions/fixture/stream", websocket)
    async with server(app, unix=True) as address:
        client = GatewayClient(socket_path=address, local_auth_token="owned-local-attribution")
        model = STT(client)
        try:
            result = await asyncio.wait_for(model.recognize(rtc.AudioFrame(data=b"\0\0" * 320, sample_rate=16000, num_channels=1, samples_per_channel=320)), 3)
            data = result.alternatives[0]
            assert data.text == text
            attributed = [part for part in data.speaker_segments if part.speaker_id is not None]
            assert [(part.text, part.speaker_id) for part in attributed] == [(part["text"], str(part["speaker"])) for part in segments]
            if partial:
                assert any(part.text.strip() == "world" and part.speaker_id is None for part in data.speaker_segments)
        finally:
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("partial", [False, True])
@pytest.mark.parametrize("manual", [None, False, True])
@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize("formatted", [False, True])
async def test_local_gateway_session_keeps_standard_speakers_and_canonical_turn(partial, manual, nested, formatted):
    from livekit import rtc
    from livekit.agents import Agent, AgentSession, stt
    from livekit.agents.voice import io

    from speko_gateway.livekit import STT

    text = "hello world" if partial else "你好"
    segments = [{"text": "hello" if partial else "你", "speaker": 2, "start_ms": 0, "end_ms": 400}]
    if not partial:
        segments.append({"text": "好", "speaker": 3, "start_ms": 400, "end_ms": 800})
    first, release = asyncio.Event(), asyncio.Event()
    next_final = asyncio.Event()
    turns, items, entered, finals, transcription = asyncio.Queue(), asyncio.Queue(), asyncio.Queue(), [], []

    class Audio(io.AudioInput):
        def __init__(self):
            super().__init__(label="owned-local-sdk-input")
            self.frames = asyncio.Queue()

        async def __anext__(self):
            value = await self.frames.get()
            if value is None:
                raise StopAsyncIteration
            return value

    class OwnedAgent(Agent):
        async def on_enter(self):
            entered.put_nowait(self)

        async def stt_node(self, audio, settings):
            async for event in Agent.default.stt_node(self, audio, settings):
                event = dataclasses.replace(event, alternatives=[dataclasses.replace(data) for data in event.alternatives])
                if event.type is stt.SpeechEventType.FINAL_TRANSCRIPT:
                    finals.extend(event.alternatives)
                yield event
                if event.type is stt.SpeechEventType.FINAL_TRANSCRIPT and event.alternatives[0].canonical_text == "" and not first.is_set():
                    first.set()
                    if manual is not None:
                        await release.wait()

        async def on_user_turn_completed(self, turn_ctx, new_message):
            turns.put_nowait(new_message.text_content)

    class HandoffAgent(Agent):
        async def on_enter(self):
            entered.put_nowait(self)

        async def on_user_turn_completed(self, turn_ctx, new_message):
            turns.put_nowait(new_message.text_content)

    async def create(request):
        await request.json()
        return web.json_response({"stream_url": "/v1/sessions/fixture/stream"}, status=201)

    async def websocket(request):
        ws = web.WebSocketResponse(protocols=["speko.voice.v0.r3"])
        await ws.prepare(request)
        sent = False
        async for message in ws:
            if message.type is aiohttp.WSMsgType.BINARY and (not sent or next_final.is_set()):
                sent = True
                next_final.clear()
                for kind, data in [("speech.started", {}), ("transcript.final", {"text": text, "segments": segments}), ("speech.ended", {})]:
                    await ws.send_json({"type": kind, "data": data})
            if message.type is aiohttp.WSMsgType.TEXT and message.json()["type"] == "session.close":
                await ws.close()
        return ws

    app = web.Application()
    app.router.add_post("/v1/sessions", create)
    app.router.add_get("/v1/sessions/fixture/stream", websocket)
    async with server(app, unix=True) as address:
        client = GatewayClient(socket_path=address, local_auth_token="owned-local-sdk-attribution")
        model = STT(client, diarization=True)
        speaker_wrapper = stt.MultiSpeakerAdapter(stt=model, primary_format="[primary {speaker_id}] {text}" if formatted else "{text}", background_format="[background {speaker_id}] {text}" if formatted else "{text}")
        wrapped = speaker_wrapper
        if nested:
            wrapped = stt.FallbackAdapter([wrapped], max_retry_per_stt=0)
        session = AgentSession(stt=wrapped, llm=None, tts=None, vad=None, turn_detection="stt", min_endpointing_delay=0, max_endpointing_delay=0)
        session.on("user_input_transcribed", lambda event: transcription.append(event))
        session.on("conversation_item_added", lambda event: items.put_nowait(event.item.text_content) if getattr(event.item, "role", None) == "user" else None)
        audio = Audio()
        session.input.audio = audio
        bindings = []
        try:
            options = {"record": False} if "record" in inspect.signature(session.start).parameters else {}
            await session.start(agent=OwnedAgent(instructions="Owned local SDK attribution"), **options)
            await asyncio.wait_for(entered.get(), 3)
            binding = getattr(session._activity, "_speko_local_transcript_binding", None)
            if binding is not None:
                bindings.append(binding)
            await audio.frames.put(rtc.AudioFrame(data=b"\xff\x0f" * 16000, sample_rate=16000, num_channels=1, samples_per_channel=16000))
            if manual is not None:
                await asyncio.wait_for(first.wait(), 3)
                session.commit_user_turn(transcript_timeout=0, skip_reply=manual)
                await asyncio.sleep(0.05)
                assert turns.empty()
                assert items.empty()
                assert binding.deferred is not None
                release.set()
            event_texts = [f"[{'primary' if index == 0 else 'background'} {part['speaker']}] {part['text']}" if formatted else part["text"] for index, part in enumerate(segments)] + ([" world"] if partial else [])
            expected_turn = " ".join(event_texts) if formatted else text
            assert await asyncio.wait_for((items if manual is True else turns).get(), 3) == expected_turn
            assert [(data.text.strip(), data.speaker_id) for data in finals] == [(part.strip(), str(segments[index]["speaker"]) if index < len(segments) else None) for index, part in enumerate(event_texts)]
            assert {event.speaker_id for event in transcription if event.is_final} >= ({"2", None} if partial else {"2", "3"})
            assert finals[0].is_primary_speaker is True
            if not partial:
                assert finals[-1].is_primary_speaker is False
            assert not hasattr(session._activity._start_session, "__wrapped__")
            if manual is None and not partial and not formatted:
                # First move from the copying node to the SDK default, then
                # hand off again so SDKs that retain default pipelines do so.
                for index in range(2):
                    previous_pipeline = getattr(session._activity._audio_recognition, "_stt_pipeline", None)
                    target = HandoffAgent(instructions="Owned local SDK handoff")
                    session.update_agent(target)
                    assert await asyncio.wait_for(entered.get(), 3) is target
                    if index and previous_pipeline is not None:
                        assert session._activity._audio_recognition._stt_pipeline is previous_pipeline
                    next_final.set()
                    await audio.frames.put(rtc.AudioFrame(data=b"\xff\x0f" * 16000, sample_rate=16000, num_channels=1, samples_per_channel=16000))
                    assert await asyncio.wait_for(turns.get(), 3) == text
                    bindings.append(session._activity._speko_local_transcript_binding)
        finally:
            hooks = [binding.processing for binding in bindings if binding.processing is not None]
            release.set()
            await audio.frames.put(None)
            await session.aclose()
            await wrapped.aclose()
            await model.aclose()
            await client.aclose()
            for binding in bindings:
                assert binding.processing is None
                assert binding.turn is None
            for recognition, method, original, _ in hooks:
                assert getattr(recognition, method) is original
            assert not hasattr(speaker_wrapper, "_speko_local_prewarm_binding")
            assert not hasattr(wrapped, "_speko_local_prewarm_binding")


@pytest.mark.parametrize('status,code,retryable', [(403,'provider_not_entitled',False),(401,'authentication_failed',False),(429,'rate_limited',True),(503,'service_unavailable',True)])
async def test_gateway_admission_error_preserves_code_and_retryability(status, code, retryable):
    from speko_gateway.client import GatewayError
    async def reject(request):
        return web.json_response({'error': {'code': code}}, status=status)
    app = web.Application()
    app.router.add_post('/v1/sessions', reject)
    async with server(app, unix=True) as socket:
        client = GatewayClient(socket_path=socket, local_auth_token='fixture')
        try:
            with pytest.raises(GatewayError) as caught:
                await client.open(SessionConfig(kind='stt',execution={},request={}))
            assert caught.value.code == code
            assert caught.value.retryable is retryable
        finally:
            await client.aclose()


@pytest.mark.parametrize("retryable", [False, True])
async def test_gateway_provider_open_error_details(retryable):
    from speko_gateway.client import GatewayError

    async def reject(request):
        return web.json_response({"error": {
            "code": "session_open_failed", "source": "provider",
            "retryable": retryable,
            "provider": {"code": "provider_unavailable", "status": 503},
            "message": "private upstream message",
        }}, status=502)

    app = web.Application()
    app.router.add_post("/v1/sessions", reject)
    async with server(app, unix=True) as socket:
        client = GatewayClient(socket_path=socket, local_auth_token="fixture")
        try:
            with pytest.raises(GatewayError) as caught:
                await client.open(SessionConfig(kind="tts", execution={}, request={}))
            assert caught.value.code == "session_open_failed"
            assert caught.value.source == "provider"
            assert caught.value.retryable is retryable
            assert "provider provider_unavailable HTTP 503" in str(caught.value)
            assert "private upstream message" not in str(caught.value)
        finally:
            await client.aclose()
