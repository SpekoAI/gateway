from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, patch

from pipecat.frames.frames import (
    InterimTranscriptionFrame,
    TranscriptionFrame,
    TTSAudioRawFrame,
    TTSStoppedFrame,
    VADUserStoppedSpeakingFrame,
)
from pipecat.processors.frame_processor import FrameDirection
from pipecat.services.stt_service import STTService as PipecatSTTService

from speko_gateway.client import (
    CanonicalEvent,
    GatewayClient,
    GatewayError,
    SessionConfig,
)
from speko_gateway.pipecat import (
    SpekoSTTService,
    SpekoTTSService,
    _transcription_frame,
)


class FakeGatewaySession:
    def __init__(self, events: list[CanonicalEvent] | None = None) -> None:
        self._events = events or []
        self.sent_audio: list[bytes] = []
        self.audio_commits = 0
        self.appended_text: list[str] = []
        self.text_commits = 0
        self.finishes = 0
        self.cancels = 0
        self.closed = False
        self.finish_called = asyncio.Event()

    async def send_audio(self, audio: bytes) -> None:
        self.sent_audio.append(audio)

    async def commit_audio(self) -> None:
        self.audio_commits += 1

    async def append_text(self, text: str) -> None:
        self.appended_text.append(text)

    async def commit_text(self) -> None:
        self.text_commits += 1

    async def finish(self) -> None:
        self.finishes += 1
        self.finish_called.set()

    async def cancel(self) -> None:
        self.cancels += 1

    async def aclose(self) -> None:
        self.closed = True

    async def events(self) -> AsyncIterator[CanonicalEvent]:
        for event in self._events:
            yield event
        if any(event.type == "audio.frame" for event in self._events):
            await self.finish_called.wait()


class FakeGatewayClient:
    def __init__(self, *sessions: FakeGatewaySession) -> None:
        self._sessions = list(sessions)
        self.ready_timeouts: list[float] = []
        self.opened: list[SessionConfig] = []
        self.closed = False

    async def wait_until_ready(self, *, timeout: float) -> None:
        self.ready_timeouts.append(timeout)

    async def open(self, config: SessionConfig) -> FakeGatewaySession:
        self.opened.append(config)
        return self._sessions.pop(0)

    async def aclose(self) -> None:
        self.closed = True


async def _run_once(generator):
    return [frame async for frame in generator]


async def test_gateway_readiness_wait_tolerates_sidecar_startup_race() -> None:
    client = object.__new__(GatewayClient)
    ready = AsyncMock(side_effect=[OSError("socket not created"), False, True])
    client.ready = ready

    await client.wait_until_ready(timeout=0.1, interval=0.001)

    assert ready.await_count == 3


async def test_gateway_readiness_wait_is_bounded() -> None:
    client = object.__new__(GatewayClient)
    client.ready = AsyncMock(return_value=False)

    try:
        await client.wait_until_ready(timeout=0.005, interval=0.001)
    except GatewayError as error:
        assert str(error) == "Gateway did not become ready within 0.005 seconds"
    else:
        raise AssertionError("expected readiness timeout")


def test_transcription_events_map_to_native_pipecat_frames() -> None:
    interim = _transcription_frame(
        CanonicalEvent(
            type="transcript.delta",
            data={"text": "hel", "provider_request_id": "req-1"},
        ),
        user_id="caller",
        language="en",
    )
    final = _transcription_frame(
        CanonicalEvent(
            type="transcript.final",
            data={
                "text": "hello",
                "provider_request_id": "req-1",
                "speech_final": True,
            },
            extensions={"deepgram": {"words": []}},
        ),
        user_id="caller",
        language="en",
    )

    assert isinstance(interim, InterimTranscriptionFrame)
    assert interim.text == "hel"
    assert isinstance(final, TranscriptionFrame)
    assert final.text == "hello"
    assert final.finalized is True
    assert final.result == {
        "provider_request_id": "req-1",
        "extensions": {"deepgram": {"words": []}},
    }


async def test_stt_streams_audio_and_commits_before_vad_stop() -> None:
    session = FakeGatewaySession()
    client = FakeGatewayClient(session)
    service = SpekoSTTService(client, sample_rate=16_000)  # type: ignore[arg-type]
    service._sample_rate = 16_000
    await service._connect()

    assert await _run_once(service.run_stt(b"\x01\x00" * 160)) == [None]
    parent_process = AsyncMock()
    with patch.object(PipecatSTTService, "process_frame", parent_process):
        await service.process_frame(
            VADUserStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM
        )

    assert session.sent_audio == [b"\x01\x00" * 160]
    assert session.audio_commits == 1
    parent_process.assert_awaited_once()
    config = client.opened[0].as_json()
    assert config["media"] == {
        "encoding": "pcm_s16le",
        "sample_rate_hz": 16_000,
        "channels": 1,
    }
    assert config["integration"] == {
        "name": "pipecat-python",
        "version": "0.1.0",
        "transport": "pipecat",
    }

    await service._finish(graceful=False)


async def test_tts_streams_sentences_in_one_turn_and_closes_context() -> None:
    session = FakeGatewaySession(
        [
            CanonicalEvent(type="audio.started"),
            CanonicalEvent(type="audio.frame", audio=b"\x01\x00" * 240),
            CanonicalEvent(type="audio.done"),
            CanonicalEvent(type="audio.started"),
            CanonicalEvent(type="audio.frame", audio=b"\x02\x00" * 240),
            CanonicalEvent(type="audio.done"),
        ]
    )
    client = FakeGatewayClient(session)
    service = SpekoTTSService(client, sample_rate=24_000)  # type: ignore[arg-type]
    service._sample_rate = 24_000
    context_id = "turn-1"
    await service.create_audio_context(context_id)

    assert await _run_once(service.run_tts("Hello.", context_id)) == [None]
    assert await _run_once(service.run_tts("How are you?", context_id)) == [None]
    await service.flush_audio(context_id)

    state = service._contexts[context_id]
    assert state.task is not None
    await state.task

    queued = []
    queue = service._audio_contexts[context_id]
    while not queue.empty():
        queued.append(queue.get_nowait())

    audio = [frame for frame in queued if isinstance(frame, TTSAudioRawFrame)]
    assert [frame.audio for frame in audio] == [
        b"\x01\x00" * 240,
        b"\x02\x00" * 240,
    ]
    assert any(isinstance(frame, TTSStoppedFrame) for frame in queued)
    assert queued[-1] is None
    assert session.appended_text == ["Hello.", "How are you?"]
    assert session.text_commits == 2
    assert session.finishes == 1
    assert session.closed is True
    assert client.ready_timeouts == [15.0]


async def test_tts_interruption_cancels_only_the_active_turn() -> None:
    session = FakeGatewaySession()
    client = FakeGatewayClient(session)
    service = SpekoTTSService(client, sample_rate=24_000)  # type: ignore[arg-type]
    service._sample_rate = 24_000
    context_id = "turn-interrupted"
    await service.create_audio_context(context_id)
    await _run_once(service.run_tts("Please interrupt me.", context_id))

    await service.on_audio_context_interrupted(context_id)

    assert session.cancels == 1
    assert session.closed is True
    assert context_id not in service._contexts
