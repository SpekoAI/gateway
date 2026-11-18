"""Actual Pipecat dispatch and owned native WS; no production provider authority."""

from __future__ import annotations

import asyncio
import json
import uuid

import aiohttp
import pytest

pytest.importorskip("pipecat")
from aiohttp import web
from pipecat.frames.frames import (
    AggregatedTextFrame,
    ErrorFrame,
    TTSAudioRawFrame,
    TTSStoppedFrame,
)
from pipecat.processors.frame_processor import FrameDirection
from pipecat.utils.text.base_text_aggregator import AggregationType
from test_relay_speech_livekit import peer, ready

from speko_gateway import RelaySpeechClient
from speko_gateway.pipecat import SpekoTTSService


@pytest.mark.parametrize("failure", ["error", "eof", "consumer", "consumer_stop"])
async def test_native_pipecat_background_failure_cannot_readmit_next_sdk_sentence(
    failure,
):
    admissions, commits, frames = [], [], []
    error_seen, closed = asyncio.Event(), asyncio.Event()

    async def handle(request):
        index = len(admissions)
        admissions.append(request.headers["Idempotency-Key"])
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ready(ws)
        pending = ""
        async for message in ws:
            if message.type is not aiohttp.WSMsgType.TEXT:
                continue
            control = json.loads(message.data)
            if control["type"] == "input.append":
                pending += control["text"]
            elif control["type"] == "input.commit":
                commits.append(pending)
                pending = ""
                if index == 0:
                    if failure in ("error", "consumer_stop"):
                        await ws.send_json(
                            {
                                "type": "error",
                                "code": "owned_ambiguous",
                                "retryable": True,
                            }
                        )
                    if failure == "consumer":
                        await ws.send_bytes(b"\x01\x00" * 480)
                        continue
                    await ws.close()
                    closed.set()
                    return ws
                await ws.send_json({"type": "utterance.done", "sequence": 1})
            elif control["type"] in ("session.close", "session.cancel"):
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        closed.set()
        return ws

    async with peer(handle) as origin:
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
        original_append = service.append_to_audio_context
        stop_failed = False

        async def append(context_id, frame):
            nonlocal stop_failed
            if failure == "consumer" and isinstance(frame, TTSAudioRawFrame):
                raise RuntimeError("owned SDK consumer failed")
            if (
                failure == "consumer_stop"
                and not stop_failed
                and isinstance(frame, TTSStoppedFrame)
            ):
                stop_failed = True
                raise RuntimeError("owned SDK stop queue failed")
            await original_append(context_id, frame)

        service.append_to_audio_context = append
        try:
            # Use Pipecat's real sentence dispatcher/context lifecycle, without
            # ending the SDK turn or asking a control plane to recover it.
            await service._push_tts_frames(
                AggregatedTextFrame(
                    text="First physical sentence.",
                    aggregated_by=AggregationType.SENTENCE,
                )
            )
            await asyncio.wait_for(error_seen.wait(), 3)
            await asyncio.wait_for(closed.wait(), 3)

            async def drained():
                while service._contexts:
                    await asyncio.sleep(0)

            await asyncio.wait_for(drained(), 3)
            await service._push_tts_frames(
                AggregatedTextFrame(
                    text="Next sentence in the same turn.",
                    aggregated_by=AggregationType.SENTENCE,
                )
            )
            await service._push_tts_frames(
                AggregatedTextFrame(
                    text="Still no recovery decision.",
                    aggregated_by=AggregationType.SENTENCE,
                )
            )
            assert len(admissions) == 1, (
                f"ambiguous failure opened {len(admissions)} physical admissions: {admissions}"
            )
            assert commits == ["First physical sentence."]
            assert frames and all(
                frame.fatal for frame in frames if isinstance(frame, ErrorFrame)
            )
            assert service._native_failed
        finally:
            await service.cleanup()
            await client.aclose()


async def test_ordinary_gateway_pipecat_retains_its_existing_error_recovery():
    from test_client_transports import server

    from speko_gateway import GatewayClient

    admissions, commits, frames = [], [], []
    failed, second_committed = asyncio.Event(), asyncio.Event()

    async def ready_gateway(request):
        return web.Response(status=200)

    async def create(request):
        await request.json()
        index = len(admissions)
        admissions.append(request.headers["Idempotency-Key"])
        return web.json_response(
            {"stream_url": f"/v1/sessions/{index}/stream"}, status=201
        )

    async def stream(request):
        index = int(request.match_info["index"])
        ws = web.WebSocketResponse(protocols=["speko.voice.v0.r3"])
        await ws.prepare(request)
        pending = ""
        async for message in ws:
            control = message.json()
            if control["type"] == "text.append":
                pending += control["data"]["text"]
            elif control["type"] == "text.commit":
                commits.append(pending)
                pending = ""
                if index == 0:
                    await ws.send_json(
                        {"type": "error", "data": {"code": "owned_legacy_error"}}
                    )
                else:
                    await ws.send_bytes(b"\x01\x00" * 480)
                    await ws.send_json({"type": "audio.done"})
                    second_committed.set()
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
            frames.append(frame)
            if isinstance(frame, ErrorFrame):
                failed.set()

        service.push_frame = output
        try:
            await service._push_tts_frames(
                AggregatedTextFrame(
                    text="First local sentence.", aggregated_by=AggregationType.SENTENCE
                )
            )
            await asyncio.wait_for(failed.wait(), 3)

            async def drained():
                while service._contexts:
                    await asyncio.sleep(0)

            await asyncio.wait_for(drained(), 3)
            await service._push_tts_frames(
                AggregatedTextFrame(
                    text="Next local sentence.", aggregated_by=AggregationType.SENTENCE
                )
            )
            await asyncio.wait_for(second_committed.wait(), 3)
            assert len(set(admissions)) == len(admissions) == 2
            assert not service._native_failed
            assert commits == ["First local sentence.", "Next local sentence."]
            errors = [frame for frame in frames if isinstance(frame, ErrorFrame)]
            assert len(errors) == 1 and not errors[0].fatal
        finally:
            await service.cleanup()
            await client.aclose()
