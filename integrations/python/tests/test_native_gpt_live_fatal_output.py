import asyncio
import base64
import json

import pytest
from aiohttp import web
from livekit.agents import AgentSession, llm
from livekit.agents.voice import io
from test_native_gpt_live import models
from test_native_realtime_cancellation import start_agent
from test_native_sidecar import native_binary as native_binary  # noqa: PLC0414
from test_native_sidecar import native_stack


@pytest.mark.skipif(not hasattr(llm, "DuplexModel"), reason="SDK predates duplex")
@pytest.mark.parametrize("failure_at", ["ingress", "segmenter"])
async def test_buffered_output_after_fatal_cannot_start_an_sdk_generation(
    native_binary, monkeypatch, failure_at
):
    asked = asyncio.Event()
    closed = asyncio.Event()
    captured = []
    release_audio = asyncio.Event()
    if failure_at == "ingress":
        from speko_gateway._livekit_gpt_live import _GPTLiveSession

        original_audio = _GPTLiveSession._live_audio

        async def held_audio(session):
            # Retain real queued PCM until the independent ingress failure has
            # closed the physical stream. No synthetic generation is injected.
            await release_audio.wait()
            async for frame in original_audio(session):
                yield frame

        monkeypatch.setattr(_GPTLiveSession, "_live_audio", held_audio)

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
                    if failure_at == "ingress":
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
            if failure_at == "segmenter":
                original_update = native._inner._gate.update

                def fail_at_actual_segmenter(frame):
                    native.duplex_session._fatal()
                    return original_update(frame)

                monkeypatch.setattr(
                    native._inner._gate, "update", fail_at_actual_segmenter
                )
            speech = actor.generate_reply(user_input="TYPED")
            await asyncio.wait_for(asked.wait(), 5)
            await asyncio.wait_for(closed.wait(), 5)
            release_audio.set()
            await asyncio.wait_for(speech.wait_for_playout(), 5)
            segmenter = native._inner._segment_atask
            await asyncio.wait_for(asyncio.gather(segmenter, return_exceptions=True), 5)
            assert segmenter.done()
            if not segmenter.cancelled():
                assert segmenter.exception() is None
            assert speech.exception() is not None
            assert native.duplex_session._failure is not None
            if failure_at == "ingress":
                assert not inner_generations
            assert not captured
            assert len(speech_events) == 1
            assert all(
                getattr(item, "role", None) != "assistant"
                for item in actor.history.items
            )
            assert not generations, (
                f"emitted {generations!r}; speech_events={speech_events!r}; captured={len(captured)}"
            )
        finally:
            release_audio.set()
            await actor.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()
