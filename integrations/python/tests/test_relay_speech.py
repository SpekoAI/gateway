from __future__ import annotations

import asyncio
import json
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace

import aiohttp
import pytest
from aiohttp import web

from speko_gateway import GatewayError, RelaySpeechClient
from speko_gateway._livekit_bridge import LiveKitSTTBridge, LiveKitTTSBridge
from speko_gateway.client import SessionConfig


@asynccontextmanager
async def server(handler):
    app = web.Application()
    app.router.add_get("/v1/{kind}/stream", handler)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    try:
        await web.TCPSite(runner, "127.0.0.1", 0).start()
        yield f"http://127.0.0.1:{runner.addresses[0][1]}"
    finally:
        await runner.cleanup()


def config(kind="stt"):
    return SessionConfig(
        kind=kind,
        execution={
            "provider_route": "speko_relay",
            "credential_source": "managed",
            "relay_policy": "required",
        },
        request={"provider": "fixture", "model": "fixture-model", "language": "en"},
        media={"encoding": "pcm_s16le", "sample_rate_hz": 16000, "channels": 1},
    )


async def ready(ws):
    await ws.send_json(
        {
            "type": "session.ready",
            "request_id": "request-fixture",
            "route": {
                "provider": "fixture",
                "model": "fixture-model",
                "region": "fixture-region",
            },
        }
    )


@pytest.mark.parametrize("kind", ["stt", "tts"])
async def test_native_speech_drives_the_actual_framework_bridge(kind):
    seen, controls, audio = [], [], []
    pcm = b"\xff\x0f" * 16000

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        seen.append((request.headers.copy(), await ws.receive_json()))
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.BINARY:
                audio.append(message.data)
                continue
            value = json.loads(message.data)
            controls.append(value)
            if value["type"] == "input.commit":
                if kind == "stt":
                    await ws.send_json(
                        {
                            "type": "transcript.final",
                            "text": "synthetic",
                            "segments": [
                                {"start_ms": 5, "end_ms": 1005, "text": "synthetic"}
                            ],
                        }
                    )
                else:
                    await ws.send_json({"type": "utterance.started", "sequence": 1})
                    await ws.send_bytes(pcm)
                    await ws.send_json({"type": "utterance.done", "sequence": 1})
                await ws.send_json(
                    {"type": "usage.updated", "usage": {"duration_ms": 1000}}
                )
            if value["type"] == "session.close":
                await ws.send_json(
                    {"type": "session.closed", "usage": {"duration_ms": 1000}}
                )
                await ws.close()
        return ws

    session_id = str(uuid.uuid4())
    async with server(handle) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_fixture", session_id=session_id, base_url=origin
        )
        try:
            if kind == "stt":
                bridge = LiveKitSTTBridge(
                    client,
                    provider="fixture",
                    model="fixture-model",
                    credential_source="managed",
                )
                stream = await bridge.start(
                    SimpleNamespace(sample_rate=16000, num_channels=1)
                )
                await stream.push_frame(SimpleNamespace(data=pcm))
                await stream.flush()
            else:
                bridge = LiveKitTTSBridge(
                    client,
                    provider="fixture",
                    model="fixture-model",
                    credential_source="managed",
                )
                stream = await bridge.start()
                await stream.append_text("synthetic")
                await stream.commit()
            await stream.finish()
            events = [event async for event in stream.events()]
            if kind == "stt":
                assert audio == [pcm]
                assert [
                    (event.text, event.start_time, event.end_time) for event in events
                ] == [("synthetic", 0.005, 1.005)]
            else:
                assert [event.type for event in events] == [
                    "audio.started",
                    "audio.frame",
                    "audio.done",
                ]
                assert b"".join(event.audio or b"" for event in events) == pcm
                assert controls[0] == {"type": "input.append", "text": "synthetic"}
            assert stream.session_id == session_id
        finally:
            await client.aclose()
    headers, configured = seen[0]
    assert headers["Authorization"] == "Bearer sk_speko_agent_fixture"
    assert headers["Speko-Client-Session-ID"] == session_id
    assert uuid.UUID(headers["Idempotency-Key"]).version == 4
    assert configured["type"] == "session.configure"
    assert configured["routing"] == {
        "mode": "explicit",
        "provider": "fixture",
        "model": "fixture-model",
    }
    assert configured["language"] == "en"
    assert configured["audio"]["encoding"] == "pcm_s16le"
    if kind == "tts":
        assert configured["voice"] == ""
    assert controls[-1] == {"type": "session.close"}


async def test_tts_cancel_is_native_and_cannot_reset_the_session_text_limit():
    controls = []

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ready(ws)
        async for message in ws:
            value = json.loads(message.data)
            controls.append(value)
            if value["type"] == "session.close":
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with server(handle) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_fixture",
            session_id=str(uuid.uuid4()),
            base_url=origin,
        )
        try:
            cfg = config("tts")
            cfg.request["max_input_characters"] = 3
            stream = await client.open(cfg, idempotency_key="owned-cancel-fixture")
            await stream.append_text("ab")
            await stream.cancel()
            with pytest.raises(GatewayError, match="input limit"):
                await stream.append_text("cd")
            await stream.append_text("c")
            await stream.finish()
            await stream.finish()
            assert [event.type async for event in stream.events()] == [
                "usage.reported",
                "session.closed",
            ]
        finally:
            await client.aclose()
    assert controls == [
        {"type": "input.append", "text": "ab"},
        {"type": "input.cancel"},
        {"type": "input.append", "text": "c"},
        {"type": "session.close"},
    ]


async def test_native_speech_refuses_byok_or_auto_routes_before_network_io():
    seen = []

    async def handle(request):
        seen.append(request.path)
        return web.Response(status=500)

    async with server(handle) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_fixture",
            session_id=str(uuid.uuid4()),
            base_url=origin,
        )
        try:
            cfg = config()
            cfg.execution["credential_source"] = "byok"
            with pytest.raises(ValueError, match="managed"):
                await client.open(cfg)
            cfg = config()
            cfg.request["provider"] = "auto"
            with pytest.raises(ValueError, match="concrete"):
                await client.open(cfg)
            cfg = config()
            cfg.request["client_session_id"] = str(uuid.uuid4())
            with pytest.raises(ValueError, match="another platform session"):
                await client.open(cfg)
        finally:
            await client.aclose()
    assert seen == []


async def test_parallel_session_clients_keep_credentials_private_and_report_only_terminal_usage(
    monkeypatch,
):
    seen = []
    monkeypatch.setenv("SPEKO_API_KEY", "sk_speko_global_must_not_be_used")

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        seen.append(
            (
                request.headers["Authorization"],
                request.headers["Speko-Client-Session-ID"],
            )
        )
        await ws.receive_json()
        await ready(ws)
        await ws.send_json({"type": "usage.updated", "usage": {"duration_ms": 1000}})
        await ws.send_json({"type": "session.closed", "usage": {"duration_ms": 2000}})
        await ws.close()
        return ws

    sessions = [str(uuid.uuid4()), str(uuid.uuid4())]
    async with server(handle) as origin:

        async def run(index):
            client = RelaySpeechClient(
                api_key=f"sk_speko_agent_{index}",
                session_id=sessions[index],
                base_url=origin,
            )
            try:
                stream = await client.open(config())
                assert "sk_speko_" not in repr(stream.metadata)
                events = [event async for event in stream.events()]
                assert [event.type for event in events] == [
                    "usage.progress",
                    "usage.reported",
                    "session.closed",
                ]
                assert events[1].data == {"duration_ms": 2000}
            finally:
                await client.aclose()

        await asyncio.gather(run(0), run(1))
    assert sorted(seen) == [
        (f"Bearer sk_speko_agent_{i}", sessions[i]) for i in range(2)
    ]


@pytest.mark.parametrize(
    "phase", ["admission", "stream", "missing-terminal", "missing-usage", "malformed"]
)
async def test_native_refusals_and_lost_terminal_usage_do_not_retry_or_leak_content(
    phase,
):
    seen = []

    async def handle(request):
        seen.append(1)
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        if phase != "admission":
            await ready(ws)
        if phase in {"admission", "stream"}:
            await ws.send_json(
                {
                    "type": "error",
                    "error": {
                        "code": "unauthorized",
                        "retryable": False,
                        "message": "private transcript and sk_speko_agent_secret",
                    },
                }
            )
        elif phase == "malformed":
            await ws.send_str("private transcript malformed")
        elif phase == "missing-usage":
            await ws.send_json({"type": "session.closed"})
        await ws.close()
        return ws

    async with server(handle) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_fixture",
            session_id=str(uuid.uuid4()),
            base_url=origin,
        )
        try:
            with pytest.raises(GatewayError) as failure:
                stream = await client.open(config())
                async for _ in stream.events():
                    pass
            assert not failure.value.retryable
            assert "private" not in str(failure.value)
            assert "sk_speko" not in str(failure.value)
        finally:
            await client.aclose()
    assert seen == [1]


async def test_redirect_is_refused_before_a_second_request():
    seen = []

    async def handle(request):
        seen.append(request.path)
        raise web.HTTPFound("/v1/tts/stream")

    async with server(handle) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_fixture",
            session_id=str(uuid.uuid4()),
            base_url=origin,
        )
        try:
            with pytest.raises(GatewayError, match="redirects"):
                await client.open(config())
        finally:
            await client.aclose()
    assert seen == ["/v1/stt/stream"]


async def test_setup_timeout_closes_the_socket_without_sending_audio():
    closed = asyncio.Event()

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        async for _ in ws:
            pytest.fail("Audio cannot be sent before admission")
        closed.set()
        return ws

    async with server(handle) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_fixture",
            session_id=str(uuid.uuid4()),
            base_url=origin,
            setup_timeout=0.05,
        )
        try:
            with pytest.raises(GatewayError, match="connection failed"):
                await client.open(config())
            await asyncio.wait_for(closed.wait(), 2)
        finally:
            await client.aclose()


@pytest.mark.parametrize("kind", ["stt", "tts"])
async def test_actual_pipecat_services_use_native_speech_admission_and_audio(kind):
    pytest.importorskip("pipecat")
    from unittest.mock import AsyncMock

    from pipecat.frames.frames import TTSAudioRawFrame

    from speko_gateway.pipecat import SpekoSTTService, SpekoTTSService

    pcm = b"\xff\x0f" * 160
    received, controls = [], []

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        configured = await ws.receive_json()
        assert configured["routing"]["provider"] == "fixture"
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.BINARY:
                received.append(message.data)
                continue
            value = json.loads(message.data)
            controls.append(value)
            if value["type"] == "input.commit":
                if kind == "stt":
                    await ws.send_json(
                        {"type": "transcript.final", "text": "synthetic"}
                    )
                else:
                    await ws.send_json({"type": "utterance.started", "sequence": 1})
                    await ws.send_bytes(pcm)
                    await ws.send_json({"type": "utterance.done", "sequence": 1})
            if value["type"] == "session.close":
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with server(handle) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_fixture",
            session_id=str(uuid.uuid4()),
            base_url=origin,
        )
        if kind == "stt":
            service = SpekoSTTService(
                client,
                provider="fixture",
                model="fixture-model",
                credential_source="managed",
                sample_rate=16000,
            )
            service._sample_rate = 16000
            delivered = asyncio.Event()
            frames = []

            async def capture(frame):
                frames.append(frame)
                delivered.set()

            service.push_frame = AsyncMock(side_effect=capture)
            service.push_error = AsyncMock()
            try:
                await service._connect()
                assert [item async for item in service.run_stt(pcm)] == [None]
                await service._commit_audio()
                await asyncio.wait_for(delivered.wait(), 2)
                assert frames[0].text == "synthetic"
                assert received == [pcm]
                await service._finish(graceful=True)
                service.push_error.assert_not_awaited()
            finally:
                await service._finish(graceful=False)
                await client.aclose()
        else:
            service = SpekoTTSService(
                client,
                provider="fixture",
                model="fixture-model",
                credential_source="managed",
                sample_rate=24000,
            )
            service._sample_rate = 24000
            service.push_error = AsyncMock()
            context_id = "native-relay-turn"
            try:
                await service._ensure_ready()
                await service.create_audio_context(context_id)
                assert [
                    item async for item in service.run_tts("synthetic", context_id)
                ] == [None]
                state = service._contexts[context_id]
                frame = await asyncio.wait_for(
                    service._audio_contexts[context_id].get(), 2
                )
                assert isinstance(frame, TTSAudioRawFrame)
                assert frame.audio == pcm
                await service.flush_audio(context_id)
                await asyncio.wait_for(state.task, 2)
                service.push_error.assert_not_awaited()
            finally:
                await service._finish_all(interrupted=True)
                await client.aclose()
    assert controls[-1] == {"type": "session.close"}
