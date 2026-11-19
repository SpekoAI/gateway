"""Actual SDK dispatch cancellation must retain native admission ambiguity."""

from __future__ import annotations

import asyncio
import json
import uuid

import pytest
from aiohttp import web

pytest.importorskip("pipecat")
from pipecat.frames.frames import AggregatedTextFrame, ErrorFrame
from pipecat.processors.frame_processor import FrameDirection
from pipecat.utils.text.base_text_aggregator import AggregationType
from test_relay_speech_livekit import peer, ready

from speko_gateway import RelaySpeechClient
from speko_gateway.pipecat import SpekoTTSService


def sentence(text):
    return AggregatedTextFrame(text=text, aggregated_by=AggregationType.SENTENCE)


async def test_native_admission_cancel_fences_next_actual_sdk_sentence():
    admissions, frames = [], []
    configured, closed, error_seen = (asyncio.Event() for _ in range(3))

    async def handler(request):
        index = len(admissions)
        admissions.append(request.headers["Idempotency-Key"])
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        configure = await ws.receive_json()
        assert configure["type"] == "session.configure"
        if index == 0:
            configured.set()
        else:
            await ready(ws)
        try:
            async for message in ws:
                control = json.loads(message.data)
                if control["type"] == "input.commit":
                    await ws.send_json({"type": "utterance.done", "sequence": 1})
                elif control["type"] in ("session.cancel", "session.close"):
                    await ws.send_json({"type": "session.closed", "usage": {}})
                    await ws.close()
        finally:
            if index == 0:
                closed.set()
        return ws

    async with peer(handler) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_owned",
            session_id=str(uuid.uuid4()),
            base_url=origin,
        )
        service = SpekoTTSService(
            client,
            provider="fixture",
            model="fixture-model",
            credential_source="managed",
            sample_rate=24000,
        )
        service._sample_rate = 24000

        async def output(frame, direction=FrameDirection.DOWNSTREAM):
            frames.append(frame)
            if isinstance(frame, ErrorFrame):
                error_seen.set()

        service.push_frame = output
        active = asyncio.create_task(service._push_tts_frames(sentence("Original.")))
        try:
            await asyncio.wait_for(configured.wait(), 3)
            active.cancel()
            with pytest.raises(asyncio.CancelledError):
                await active
            await asyncio.wait_for(closed.wait(), 3)
            await service._push_tts_frames(sentence("Next in the same turn."))
            await service._push_tts_frames(sentence("Still no recovery decision."))
            assert len(admissions) == 1, "cancelled native admission was reopened"
            assert service._native_failed
            await asyncio.wait_for(error_seen.wait(), 3)
            assert all(f.fatal for f in frames if isinstance(f, ErrorFrame))
        finally:
            if not active.done():
                active.cancel()
            await asyncio.gather(active, return_exceptions=True)
            await service.cleanup()
            await client.aclose()


async def test_ordinary_gateway_admission_cancel_keeps_existing_recovery():
    from test_client_transports import server

    from speko_gateway import GatewayClient

    admissions, commits = [], []
    creating, release, committed = (asyncio.Event() for _ in range(3))

    async def ready_gateway(request):
        return web.Response(status=200)

    async def create(request):
        await request.json()
        index = len(admissions)
        admissions.append(request.headers["Idempotency-Key"])
        if index == 0:
            creating.set()
            await release.wait()
        return web.json_response(
            {"stream_url": f"/v1/sessions/{index}/stream"}, status=201
        )

    async def stream(request):
        ws = web.WebSocketResponse(protocols=["speko.voice.v0.r3"])
        await ws.prepare(request)
        async for message in ws:
            control = message.json()
            if control["type"] == "text.append":
                commits.append(control["data"]["text"])
            elif control["type"] == "text.commit":
                await ws.send_json({"type": "audio.done"})
                committed.set()
            elif control["type"] in ("session.close", "response.cancel"):
                await ws.close()
        return ws

    app = web.Application()
    app.router.add_get("/readyz", ready_gateway)
    app.router.add_post("/v1/sessions", create)
    app.router.add_get("/v1/sessions/{index}/stream", stream)
    async with server(app, unix=True) as address:
        client = GatewayClient(
            socket_path=address, local_auth_token="owned-local-control"
        )
        service = SpekoTTSService(client, sample_rate=24000)
        service._sample_rate = 24000

        async def output(frame, direction=FrameDirection.DOWNSTREAM):
            pass

        service.push_frame = output
        active = asyncio.create_task(service._push_tts_frames(sentence("Original.")))
        try:
            await asyncio.wait_for(creating.wait(), 3)
            active.cancel()
            with pytest.raises(asyncio.CancelledError):
                await active
            await service._push_tts_frames(sentence("Next local sentence."))
            await asyncio.wait_for(committed.wait(), 3)
            assert len(set(admissions)) == len(admissions) == 2
            assert not service._native_failed
            assert commits == ["Next local sentence."]
        finally:
            release.set()
            if not active.done():
                active.cancel()
            await asyncio.gather(active, return_exceptions=True)
            await service.cleanup()
            await client.aclose()
