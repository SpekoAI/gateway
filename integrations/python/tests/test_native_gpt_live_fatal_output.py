import asyncio
import base64
import json

import pytest
from livekit.agents import llm

from aiohttp import web
from livekit.agents import AgentSession
from livekit.agents.voice import io
from test_native_gpt_live import models
from test_native_realtime_cancellation import start_agent
from test_native_sidecar import native_binary as native_binary  # noqa: PLC0414
from test_native_sidecar import native_stack


@pytest.mark.skipif(not hasattr(llm, "DuplexModel"), reason="SDK predates duplex")
async def test_buffered_output_after_fatal_cannot_start_an_sdk_generation(
    native_binary,
):
    asked = asyncio.Event()
    closed = asyncio.Event()
    captured = []

    class Output(io.AudioOutput):
        def __init__(self):
            super().__init__(
                label="capture", capabilities=io.AudioOutputCapabilities(pause=False)
            )
            self.duration = 0

        async def capture_frame(self, frame):
            await super().capture_frame(frame)
            captured.append(bytes(frame.data))
            self.duration += frame.duration

        def flush(self):
            super().flush()
            self.on_playback_finished(
                playback_position=self.duration, interrupted=False
            )

        def clear_buffer(self):
            self.on_playback_finished(playback_position=0, interrupted=True)

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"type": "session.started"})
        try:
            async for frame in ws:
                cmd = json.loads(frame.data)
                if cmd["type"] == "session.commentary.append":
                    asked.set()
                    await ws.send_json(
                        {
                            "type": "session.output_audio.delta",
                            "delta": base64.b64encode(b"\x01\x10" * 2400).decode(),
                        }
                    )
                    await ws.send_json(
                        {
                            "type": "session.input_transcript.delta",
                            "delta": "NEW_CALLER",
                            "start_ms": 0,
                            "end_ms": 100,
                        }
                    )
        finally:
            closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        actor = AgentSession(llm=model, turn_detection="realtime_llm", vad=None)
        actor.output.audio = Output()
        try:
            await start_agent(actor)
            native = actor.current_agent.realtime_llm_session
            await native.start()
            generations = []
            inner_generations = []
            native._inner.on("generation_created", inner_generations.append)
            speech_events = []
            native.on(
                "generation_created",
                lambda value: generations.append(
                    (native.duplex_session._failure is not None, value)
                ),
            )
            actor.on("speech_created", speech_events.append)
            speech = actor.generate_reply(user_input="TYPED")
            await asyncio.wait_for(asked.wait(), 5)
            await asyncio.wait_for(closed.wait(), 5)
            await asyncio.wait_for(speech.wait_for_playout(), 5)
            assert speech.exception() is not None
            assert native.duplex_session._failure is not None
            assert not inner_generations
            assert not generations, (
                f"emitted {generations!r}; speech_events={speech_events!r}; captured={len(captured)}"
            )
        finally:
            await actor.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()
