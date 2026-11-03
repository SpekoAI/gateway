from __future__ import annotations

import asyncio
import json
import uuid
from contextlib import asynccontextmanager

import aiohttp
import pytest
from aiohttp import web
from livekit import rtc
from livekit.agents import APIConnectionError, APIConnectOptions, stt, vad

from speko_gateway import GatewayError, RelaySpeechClient
from speko_gateway.livekit import STT, TTS


@asynccontextmanager
async def peer(handler):
    app = web.Application()
    app.router.add_get("/v1/{kind}/stream", handler)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    try:
        await web.TCPSite(runner, "127.0.0.1", 0).start()
        yield f"http://127.0.0.1:{runner.addresses[0][1]}"
    finally:
        await runner.cleanup()


async def ready(ws):
    await ws.send_json(
        {
            "type": "session.ready",
            "request_id": "owned-livekit",
            "route": {
                "provider": "fixture",
                "model": "fixture-model",
                "region": "owned",
            },
        }
    )


def frame():
    return rtc.AudioFrame(
        data=b"\xff\x0f" * 320,
        sample_rate=16000,
        num_channels=1,
        samples_per_channel=320,
    )


class ControlledVAD(vad.VAD):
    """A separate owned boundary source, never a transcript-derived endpoint."""

    def __init__(self):
        super().__init__(capabilities=vad.VADCapabilities(update_interval=0.02))
        self.current = None
        self.frames = asyncio.Queue()

    def stream(self):
        self.current = ControlledVADStream(self)
        return self.current

    def emit(self, kind):
        assert self.current is not None
        self.current._event_ch.send_nowait(
            vad.VADEvent(
                type=kind,
                samples_index=320,
                timestamp=0.02,
                speech_duration=0.02,
                silence_duration=0,
                frames=[],
            )
        )


class ControlledVADStream(vad.VADStream):
    async def _main_task(self):
        async for value in self._input_ch:
            if not isinstance(value, self._FlushSentinel):
                await self._vad.frames.put(value)


async def test_livekit_native_stt_gets_two_live_turns_from_vad_not_multiple_finals():
    source = ControlledVAD()
    transcripts = [asyncio.Event(), asyncio.Event()]
    late = asyncio.Event()
    closed = asyncio.Event()
    seen = []

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        seen.append(request.headers["Idempotency-Key"])
        await ready(ws)
        turns = commits = 0
        async for message in ws:
            if message.type is aiohttp.WSMsgType.BINARY:
                turn = turns
                turns += 1
                await transcripts[turn].wait()
                for suffix in ["a", "b"]:
                    await ws.send_json(
                        {"type": "transcript.final", "text": f"{turn + 1}{suffix}"}
                    )
                continue
            value = json.loads(message.data)
            if value["type"] == "input.commit":
                commits += 1
                if commits == 1:
                    await late.wait()
                    await ws.send_json({"type": "transcript.final", "text": "late-1"})
            if value["type"] == "session.close":
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        closed.set()
        return ws

    async with peer(handle) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_fixture",
            session_id=str(uuid.uuid4()),
            base_url=origin,
        )
        model = STT(
            client,
            provider="fixture",
            model="fixture-model",
            credential_source="managed",
            turn_vad=source,
        )
        stream = model.stream(
            conn_options=APIConnectOptions(max_retry=3, retry_interval=0)
        )
        try:
            for turn in range(2):
                stream.push_frame(frame())
                await asyncio.wait_for(source.frames.get(), 2)
                source.emit(vad.VADEventType.START_OF_SPEECH)
                assert (
                    await asyncio.wait_for(anext(stream), 2)
                ).type is stt.SpeechEventType.START_OF_SPEECH
                transcripts[turn].set()
                for suffix in ["a", "b"]:
                    event = await asyncio.wait_for(anext(stream), 2)
                    assert event.type is stt.SpeechEventType.FINAL_TRANSCRIPT
                    assert event.alternatives[0].text == f"{turn + 1}{suffix}"
                pending = asyncio.create_task(anext(stream))
                await asyncio.sleep(0)
                assert not pending.done()
                source.emit(vad.VADEventType.END_OF_SPEECH)
                assert (
                    await asyncio.wait_for(pending, 2)
                ).type is stt.SpeechEventType.END_OF_SPEECH
                assert not closed.is_set()
                if turn == 0:
                    late.set()
                    event = await asyncio.wait_for(anext(stream), 2)
                    assert event.type is stt.SpeechEventType.FINAL_TRANSCRIPT
                    assert event.alternatives[0].text == "late-1"
            stream.end_input()
            assert [event async for event in stream] == []
            assert len(seen) == 1
        finally:
            late.set()
            for event in transcripts:
                event.set()
            await stream.aclose()
            await model.aclose()
            await client.aclose()
        assert source.current._task.done()


@pytest.mark.parametrize(
    "failure", ["retryable-error", "setup-timeout", "lost-terminal"]
)
@pytest.mark.parametrize("method", ["stream", "synthesize"])
async def test_actual_livekit_tts_cannot_replay_native_speech_even_with_retry_enabled(
    failure, method
):
    seen = []
    inputs = []

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        seen.append(request.headers["Idempotency-Key"])
        if failure != "setup-timeout":
            await ready(ws)
        async for message in ws:
            value = json.loads(message.data)
            if value["type"] == "input.append":
                inputs.append(value["text"])
            if value["type"] == "input.commit":
                if failure == "retryable-error":
                    await ws.send_json(
                        {
                            "type": "error",
                            "error": {"code": "provider_failure", "retryable": True},
                        }
                    )
                await ws.close()
        return ws

    async with peer(handle) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_fixture",
            session_id=str(uuid.uuid4()),
            base_url=origin,
            setup_timeout=0.1,
        )
        model = TTS(
            client,
            provider="fixture",
            model="fixture-model",
            credential_source="managed",
        )
        options = APIConnectOptions(max_retry=3, retry_interval=0)
        stream = (
            model.stream(conn_options=options)
            if method == "stream"
            else model.synthesize("synthetic", conn_options=options)
        )
        if method == "stream":
            stream.push_text("synthetic")
            stream.end_input()
        try:
            with pytest.raises(APIConnectionError):
                async for _ in stream:
                    pass
            assert len(seen) == 1
            assert inputs == ([] if failure == "setup-timeout" else ["synthetic"])
            assert options.max_retry == 3
        finally:
            await stream.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("method", ["stream", "recognize"])
async def test_actual_livekit_stt_cannot_retry_an_ambiguous_native_error(method):
    seen = []

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        seen.append(request.headers["Idempotency-Key"])
        await ready(ws)
        await ws.send_json(
            {"type": "error", "error": {"code": "provider_failure", "retryable": True}}
        )
        await ws.close()
        return ws

    async with peer(handle) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_fixture",
            session_id=str(uuid.uuid4()),
            base_url=origin,
        )
        model = STT(
            client,
            provider="fixture",
            model="fixture-model",
            credential_source="managed",
            turn_vad=ControlledVAD(),
        )
        stream = None
        try:
            with pytest.raises(APIConnectionError):
                if method == "recognize":
                    await model.recognize(
                        frame(),
                        conn_options=APIConnectOptions(max_retry=3, retry_interval=0),
                    )
                else:
                    stream = model.stream(
                        conn_options=APIConnectOptions(max_retry=3, retry_interval=0)
                    )
                    stream.push_frame(frame())
                    stream.end_input()
                    async for _ in stream:
                        pass
            assert len(seen) == 1
        finally:
            if stream is not None:
                await stream.aclose()
            await model.aclose()
            await client.aclose()


async def test_native_livekit_stt_refuses_missing_speech_boundary_source_before_admission():
    client = RelaySpeechClient(
        api_key="sk_speko_agent_fixture",
        session_id=str(uuid.uuid4()),
        base_url="http://127.0.0.1:1",
    )
    try:
        with pytest.raises(ValueError, match="turn_vad"):
            STT(
                client,
                provider="fixture",
                model="fixture-model",
                credential_source="managed",
            )
    finally:
        await client.aclose()


@pytest.mark.parametrize("status", [401, 403, 429, 503])
async def test_native_websocket_http_refusal_is_never_a_retryable_connection_failure(
    status,
):
    seen = []

    async def handle(request):
        seen.append(1)
        return web.json_response(
            {"error": {"message": "private content", "retryable": True}}, status=status
        )

    async with peer(handle) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_fixture",
            session_id=str(uuid.uuid4()),
            base_url=origin,
        )
        try:
            from speko_gateway.client import SessionConfig

            cfg = SessionConfig(
                kind="tts",
                execution={
                    "credential_source": "managed",
                    "provider_route": "speko_relay",
                    "relay_policy": "required",
                },
                request={"provider": "fixture", "model": "fixture-model"},
                media={"encoding": "pcm_s16le", "sample_rate_hz": 24000, "channels": 1},
            )
            with pytest.raises(GatewayError) as error:
                await client.open(cfg)
            assert not error.value.retryable
            assert error.value.code == f"http_{status}"
            assert "private" not in str(error.value)
        finally:
            await client.aclose()
    assert seen == [1]
