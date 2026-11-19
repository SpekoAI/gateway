"""Actual Pipecat frame-queue cancellation at distinct native I/O stages."""

from __future__ import annotations

import asyncio
import json
import socket
import uuid

import pytest

pytest.importorskip("pipecat")
from aiohttp import web
from pipecat.frames.frames import (
    AggregatedTextFrame,
    ErrorFrame,
    InterruptionFrame,
    LLMFullResponseEndFrame,
    LLMFullResponseStartFrame,
    TTSAudioRawFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineWorker
from pipecat.processors.frame_processor import FrameProcessor
from pipecat.utils.text.base_text_aggregator import AggregationType
from pipecat.workers.runner import WorkerRunner
from test_relay_speech_livekit import peer, ready

from speko_gateway import RelaySpeechClient
from speko_gateway.pipecat import SpekoTTSService


def sentence(text):
    return AggregatedTextFrame(text=text, aggregated_by=AggregationType.SENTENCE)


async def eventually(check):
    async def wait():
        while not check():
            await asyncio.sleep(0)

    await asyncio.wait_for(wait(), 3)


@pytest.mark.parametrize("stage", ["waiting", "admission", "send"])
async def test_actual_frame_queue_distinguishes_waiting_from_ambiguous_native_io(stage):
    admissions, commits, errors, audio = [], [], [], []
    configured, first_commit, paused, release_read, closed, interrupted, progressed = (
        asyncio.Event() for _ in range(7)
    )

    async def handle(request):
        index = len(admissions)
        admissions.append(request.headers["Idempotency-Key"])
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        assert (await ws.receive_json())["type"] == "session.configure"
        if index == 0:
            configured.set()
        if index != 0 or stage != "admission":
            await ready(ws)
        pending = ""
        try:
            async for message in ws:
                control = json.loads(message.data)
                if control["type"] == "input.append":
                    pending += control["text"]
                elif control["type"] == "input.commit":
                    commits.append((index, len(pending)))
                    pending = ""
                    await ws.send_json({"type": "utterance.started", "sequence": 1})
                    await ws.send_bytes((index + 1).to_bytes(2, "little") * 480)
                    if index == 0:
                        first_commit.set()
                    if index != 0 or stage == "send":
                        await ws.send_json({"type": "utterance.done", "sequence": 1})
                    if index == 0 and stage == "send":
                        request.transport.pause_reading()
                        paused.set()
                        try:
                            await release_read.wait()
                        finally:
                            request.transport.resume_reading()
                elif control["type"] in ("input.cancel", "session.close"):
                    await ws.send_json({"type": "session.closed", "usage": {}})
                    await ws.close()
        finally:
            if index == 0:
                closed.set()
        return ws

    class Output(FrameProcessor):
        async def process_frame(self, frame, direction):
            await super().process_frame(frame, direction)
            if isinstance(frame, TTSAudioRawFrame):
                audio.append(frame.audio)
                if frame.audio.startswith(b"\x02\x00"):
                    progressed.set()
            elif isinstance(frame, InterruptionFrame):
                interrupted.set()
            await self.push_frame(frame, direction)

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
        worker = PipelineWorker(Pipeline([service, Output()]), enable_rtvi=False)
        runner = WorkerRunner(handle_sigint=False, handle_sigterm=False)

        @worker.event_handler("on_pipeline_error")
        async def on_error(_worker, frame):
            errors.append(frame)
            progressed.set()

        await runner.add_workers(worker)
        running = asyncio.create_task(runner.run())
        try:
            await worker.queue_frames(
                [LLMFullResponseStartFrame(), sentence("First physical sentence.")]
            )
            await asyncio.wait_for(configured.wait(), 3)
            state = None
            if stage != "admission":
                await asyncio.wait_for(first_commit.wait(), 3)
                await eventually(lambda: bool(service._contexts))
                state = next(iter(service._contexts.values()))
                if stage == "send":
                    await asyncio.wait_for(paused.wait(), 3)
                    transport = state.session._websocket._writer.transport
                    transport.get_extra_info("socket").setsockopt(
                        socket.SOL_SOCKET, socket.SO_SNDBUF, 4096
                    )
                    transport.set_write_buffer_limits(high=1024, low=512)
                    await worker.queue_frame(sentence("界" * 60000))
                    await eventually(
                        lambda: (
                            transport.get_write_buffer_size() > 0
                            and state.session._lock.locked()
                        )
                    )
                else:
                    await worker.queue_frame(sentence("Queued second sentence."))
                    await eventually(lambda: bool(state.utterance_done._waiters))
            await worker.queue_frame(InterruptionFrame())
            if stage == "send":
                # The physical sender is already blocked on actual transport
                # backpressure. Let cancellation release its real send lock,
                # then allow the peer to read so cleanup can physically join.
                await eventually(lambda: not state.session._lock.locked())
                release_read.set()
            await asyncio.wait_for(interrupted.wait(), 3)
            await asyncio.wait_for(closed.wait(), 3)
            await worker.queue_frames(
                [
                    LLMFullResponseStartFrame(),
                    sentence("Next independent turn."),
                    LLMFullResponseEndFrame(),
                ]
            )
            await asyncio.wait_for(progressed.wait(), 3)
            if stage == "waiting":
                assert not errors, "healthy waiting interruption poisoned the next turn"
                assert not service._native_failed
                assert len(admissions) == 2 and len(set(admissions)) == 2
                assert [index for index, _ in commits] == [0, 1]
                assert audio and audio[-1].startswith(b"\x02\x00")
            else:
                assert service._native_failed
                assert len(admissions) == 1, "ambiguous native I/O was readmitted"
                assert errors and all(
                    isinstance(f, ErrorFrame) and f.fatal for f in errors
                )
        finally:
            release_read.set()
            await worker.cancel()
            await asyncio.wait_for(running, 3)
            await client.aclose()
