from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, patch

import pytest
from pipecat.adapters.schemas.function_schema import FunctionSchema
from pipecat.frames.frames import (
    ErrorFrame,
    InterimTranscriptionFrame,
    TranscriptionFrame,
    TTSAudioRawFrame,
    TTSStoppedFrame,
    VADUserStoppedSpeakingFrame,
)
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.frame_processor import FrameDirection
from pipecat.services.settings import LLMSettings, STTSettings, TTSSettings
from pipecat.services.stt_service import STTService as PipecatSTTService

from speko_gateway.client import (
    CanonicalEvent,
    GatewayClient,
    GatewayError,
    SessionConfig,
)
from speko_gateway.pipecat import (
    SpekoLLMService,
    SpekoSTTService,
    SpekoTTSService,
    _transcription_frame,
)


class FakeRelayClient:
    def __init__(self, events: list[tuple[str, dict]] | None = None) -> None:
        self.events = events or []
        self.requests: list[dict] = []
        self.closed = False

    async def stream_response(self, request: dict):
        self.requests.append(request)
        for event in self.events:
            yield event

    async def aclose(self) -> None:
        self.closed = True


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
    def __init__(self, *sessions: FakeGatewaySession | BaseException) -> None:
        self._sessions = list(sessions)
        self.ready_timeouts: list[float] = []
        self.opened: list[SessionConfig] = []
        self.closed = False

    async def wait_until_ready(self, *, timeout: float) -> None:
        self.ready_timeouts.append(timeout)

    async def open(self, config: SessionConfig) -> FakeGatewaySession:
        self.opened.append(config)
        result = self._sessions.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result

    async def aclose(self) -> None:
        self.closed = True


async def _run_once(generator):
    return [frame async for frame in generator]


async def test_gateway_readiness_wait_tolerates_sidecar_startup_race() -> None:
    # The subject is the RETRY, not the deadline: a missing socket and a
    # not-ready answer are both startup state, so the third probe is the one
    # that returns. The timeout is deliberately far larger than the work it
    # bounds, because a tight budget here measures the CI runner's scheduler
    # instead of the client. At 0.1s this asserted that three mocked awaits
    # and two 1ms sleeps all landed inside 100ms of wall clock, and it failed
    # on loaded runners while passing locally on the same commit. The deadline
    # is covered by the two tests below, which assert the error and the bound
    # without racing anything.
    client = object.__new__(GatewayClient)
    ready = AsyncMock(side_effect=[OSError("socket not created"), False, True])
    client.ready = ready

    await client.wait_until_ready(timeout=30, interval=0.001)

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


async def test_gateway_readiness_deadline_bounds_a_stalled_request() -> None:
    client = object.__new__(GatewayClient)

    async def stalled_ready() -> bool:
        await asyncio.Event().wait()
        return False

    client.ready = AsyncMock(side_effect=stalled_ready)

    try:
        await asyncio.wait_for(
            client.wait_until_ready(timeout=0.01, interval=0.001), timeout=0.25
        )
    except GatewayError as error:
        assert str(error) == "Gateway did not become ready within 0.01 seconds"
    except TimeoutError as error:
        raise AssertionError("stalled readiness request exceeded its deadline") from error
    else:
        raise AssertionError("expected stalled readiness request to time out")


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
        "language": "en",
    }


def test_transcription_uses_detected_language_when_gateway_supplies_it() -> None:
    frame = _transcription_frame(
        CanonicalEvent(
            type="transcript.final",
            data={"text": "Salom", "language": "uz"},
        ),
        user_id="caller",
        language="en",
    )
    assert isinstance(frame, TranscriptionFrame)
    assert frame.language is not None and frame.language.value == "uz"
    assert frame.result["language"] == "uz"


def test_llm_request_maps_universal_messages_and_full_tool_schema() -> None:
    async def handler(_params) -> None:
        return None

    tool = FunctionSchema(
        name="book_slot",
        description="Book a slot",
        properties={
            "day": {"type": "string", "enum": ["monday", "tuesday"]},
            "count": {"type": "integer", "minimum": 1},
        },
        required=["day"],
        handler=handler,
    )
    context = LLMContext(
        messages=[
            {"role": "developer", "content": "Be concise"},
            {"role": "user", "content": "Book Monday"},
            {
                "role": "assistant",
                "tool_calls": [
                    {
                        "id": "call-1",
                        "type": "function",
                        "function": {"name": "book_slot", "arguments": '{"day":"monday"}'},
                    }
                ],
            },
            {"role": "tool", "tool_call_id": "call-1", "content": "confirmed"},
        ],
        tools=[tool],
    )
    llm = SpekoLLMService(FakeRelayClient(), system_instruction="Base prompt")  # type: ignore[arg-type]

    request = llm._request(context)

    assert request["routing"] == {"mode": "auto", "objective": "balanced"}
    assert request["input"] == [
        {
            "type": "message",
            "role": "system",
            "content": [{"type": "text", "text": "Base prompt"}],
        },
        {
            "type": "message",
            "role": "system",
            "content": [{"type": "text", "text": "Be concise"}],
        },
        {
            "type": "message",
            "role": "user",
            "content": [{"type": "text", "text": "Book Monday"}],
        },
        {
            "type": "function_call",
            "call_id": "call-1",
            "name": "book_slot",
            "arguments": '{"day":"monday"}',
        },
        {"type": "function_result", "call_id": "call-1", "result": "confirmed"},
    ]
    assert request["tools"] == [
        {
            "name": "book_slot",
            "description": "Book a slot",
            "parameters": {
                "type": "object",
                "properties": {
                    "day": {"type": "string", "enum": ["monday", "tuesday"]},
                    "count": {"type": "integer", "minimum": 1},
                },
                "required": ["day"],
            },
        }
    ]


async def test_llm_run_inference_collects_streamed_text() -> None:
    relay = FakeRelayClient(
        [
            ("response.created", {"response_id": "response-1"}),
            ("response.text.delta", {"delta": "CONVER"}),
            ("response.text.delta", {"delta": "SATION"}),
            ("response.completed", {"usage": {"input_tokens": 2, "output_tokens": 1}}),
        ]
    )
    llm = SpekoLLMService(relay)  # type: ignore[arg-type]

    result = await llm.run_inference(
        LLMContext(messages=[{"role": "user", "content": "Classify"}]),
        max_tokens=4,
        system_instruction="Return one word",
    )

    assert result == "CONVERSATION"
    assert relay.requests[0]["max_output_tokens"] == 4
    assert relay.requests[0]["input"][0]["content"][0]["text"] == "Return one word"


async def test_llm_typed_settings_updates_change_router_request() -> None:
    llm = SpekoLLMService(  # type: ignore[arg-type]
        FakeRelayClient(),
        provider="openai",
        model="gpt-default",
        max_output_tokens=100,
    )
    await llm._update_settings(
        LLMSettings(
            model="gpt-node",
            temperature=0.2,
            top_p=0.8,
            max_tokens=321,
        )
    )

    request = llm._request(LLMContext(messages=[{"role": "user", "content": "Hi"}]))

    assert request["routing"] == {
        "mode": "explicit",
        "provider": "openai",
        "model": "gpt-node",
    }
    assert request["max_output_tokens"] == 321
    assert request["temperature"] == 0.2
    assert request["top_p"] == 0.8


async def test_llm_settings_cannot_change_auto_route_mode() -> None:
    auto = SpekoLLMService(FakeRelayClient())  # type: ignore[arg-type]
    explicit = SpekoLLMService(  # type: ignore[arg-type]
        FakeRelayClient(), provider="openai", model="gpt-default"
    )

    for service, model in ((auto, "gpt-node"), (explicit, "auto")):
        try:
            await service._update_settings(LLMSettings(model=model))
        except ValueError as error:
            assert "cannot change between auto and explicit routing" in str(error)
        else:
            raise AssertionError("expected route-mode update to be rejected")

    assert auto._settings.model == "auto"
    assert explicit._settings.model == "gpt-default"


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


def test_voice_services_initialize_complete_pipecat_settings() -> None:
    session = FakeGatewaySession()
    client = FakeGatewayClient(session)

    stt = SpekoSTTService(  # type: ignore[arg-type]
        client, model="nova-3", language="en"
    )
    tts = SpekoTTSService(  # type: ignore[arg-type]
        client, model="sonic-3", voice="amy", language="en"
    )

    assert stt._settings == STTSettings(model="nova-3", language="en")
    assert tts._settings == TTSSettings(model="sonic-3", voice="amy", language="en")


def test_voice_services_honor_caller_supplied_pipecat_settings() -> None:
    session = FakeGatewaySession()
    client = FakeGatewayClient(session)

    stt = SpekoSTTService(  # type: ignore[arg-type]
        client, settings=STTSettings(model="whisper-1", language="fr")
    )
    tts = SpekoTTSService(  # type: ignore[arg-type]
        client,
        settings=TTSSettings(model="sonic-3", voice="amy", language="fr"),
    )

    assert stt._model == "whisper-1"
    assert stt._language == "fr"
    assert tts._model == "sonic-3"
    assert tts._voice == "amy"
    assert tts._language == "fr"


async def test_stt_start_surfaces_gateway_admission_failure_as_fatal() -> None:
    client = FakeGatewayClient(FakeGatewaySession())
    service = SpekoSTTService(client)  # type: ignore[arg-type]
    failure = GatewayError(
        "Gateway rejected request (no_eligible_route, HTTP 422)",
        code="no_eligible_route",
        retryable=False,
    )
    service._connect = AsyncMock(side_effect=failure)  # type: ignore[method-assign]
    service.push_error = AsyncMock()  # type: ignore[method-assign]

    with patch.object(PipecatSTTService, "start", AsyncMock()):
        await service.start(object())  # type: ignore[arg-type]

    service.push_error.assert_awaited_once_with(
        "Speko Gateway STT failed (no_eligible_route)",
        exception=failure,
        fatal=True,
    )


async def test_stt_can_fallback_to_managed_auto_when_explicit_route_is_ineligible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPEKO_API_KEY", "test-managed-key")
    failure = GatewayError(
        "Gateway rejected request (no_eligible_route, HTTP 422)",
        code="no_eligible_route",
        retryable=False,
    )
    session = FakeGatewaySession()
    client = FakeGatewayClient(failure, session)
    service = SpekoSTTService(  # type: ignore[arg-type]
        client,
        provider="meta",
        model="muse-voice-transcribe-1.0",
        credential_source="auto",
        fallback_to_auto_on_no_eligible_route=True,
        sample_rate=16_000,
    )
    service._sample_rate = 16_000

    await service._connect()

    assert [config.request for config in client.opened] == [
        {
            "provider": "meta",
            "language": "en",
            "model": "muse-voice-transcribe-1.0",
        },
        {"provider": "auto", "language": "en", "model": "auto"},
    ]
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


async def test_tts_admission_failure_is_fatal() -> None:
    client = FakeGatewayClient(FakeGatewaySession())
    service = SpekoTTSService(client)  # type: ignore[arg-type]
    failure = GatewayError(
        "Gateway rejected request (no_eligible_route, HTTP 422)",
        code="no_eligible_route",
        retryable=False,
    )
    service._context = AsyncMock(side_effect=failure)  # type: ignore[method-assign]

    frames = await _run_once(service.run_tts("Hello", "turn-failed"))

    error = next(frame for frame in frames if isinstance(frame, ErrorFrame))
    assert error.fatal is True
    assert error.error == "Speko Gateway TTS failed (no_eligible_route)"


async def test_tts_can_fallback_to_managed_auto_without_vendor_voice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPEKO_API_KEY", "test-managed-key")
    failure = GatewayError(
        "Gateway rejected request (no_eligible_route, HTTP 422)",
        code="no_eligible_route",
        retryable=False,
    )
    first = FakeGatewaySession()
    second = FakeGatewaySession()
    client = FakeGatewayClient(failure, first, second)
    service = SpekoTTSService(  # type: ignore[arg-type]
        client,
        provider="openai",
        model="gpt-4o-mini-tts",
        voice="coral",
        credential_source="auto",
        fallback_to_auto_on_no_eligible_route=True,
        sample_rate=24_000,
    )
    service._sample_rate = 24_000

    first_state = await service._context("turn-1")
    second_state = await service._context("turn-2")

    assert [config.request for config in client.opened] == [
        {
            "provider": "openai",
            "language": "en",
            "model": "gpt-4o-mini-tts",
            "max_input_characters": 100_000,
            "voice": "coral",
        },
        {
            "provider": "auto",
            "language": "en",
            "model": "auto",
            "max_input_characters": 100_000,
        },
        {
            "provider": "auto",
            "language": "en",
            "model": "auto",
            "max_input_characters": 100_000,
        },
    ]
    await service._close_state("turn-1", first_state, interrupted=True)
    await service._close_state("turn-2", second_state, interrupted=True)


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


class SequentialTTSGatewaySession(FakeGatewaySession):
    """A Gateway stream whose next utterance requires the previous audio.done."""

    def __init__(self) -> None:
        super().__init__()
        self.pending: asyncio.Queue[CanonicalEvent | None] = asyncio.Queue()
        self.active = False
        self.overlapped = False

    async def append_text(self, text: str) -> None:
        if self.active:
            self.overlapped = True
            await self.pending.put(
                CanonicalEvent(
                    type="error",
                    data={"source": "runtime", "code": "internal"},
                )
            )
        await super().append_text(text)

    async def commit_text(self) -> None:
        self.active = True
        await super().commit_text()

    async def complete_utterance(self) -> None:
        self.active = False
        await self.pending.put(CanonicalEvent(type="audio.done"))

    async def finish(self) -> None:
        await super().finish()
        await self.pending.put(None)

    async def events(self) -> AsyncIterator[CanonicalEvent]:
        while (event := await self.pending.get()) is not None:
            yield event


async def test_tts_waits_for_audio_done_before_submitting_next_sentence() -> None:
    session = SequentialTTSGatewaySession()
    service = SpekoTTSService(FakeGatewayClient(session), sample_rate=24_000)
    service._sample_rate = 24_000
    service.push_error = AsyncMock()
    context_id = "turn-sequential"
    await service.create_audio_context(context_id)
    await _run_once(service.run_tts("Hi there!", context_id))
    state = service._contexts[context_id]
    await session.pending.put(CanonicalEvent(type="audio.frame", audio=b"\x01\x00"))
    # Audio must stream while synthesis is still in progress.
    first_audio = await asyncio.wait_for(service._audio_contexts[context_id].get(), 2)
    assert isinstance(first_audio, TTSAudioRawFrame)
    second = asyncio.create_task(
        _run_once(service.run_tts("Thanks for calling.", context_id))
    )
    try:
        await asyncio.sleep(0)
        assert not second.done()
        assert session.appended_text == ["Hi there!"]
        assert not session.overlapped
        await session.complete_utterance()
        assert await asyncio.wait_for(second, 2) == [None]
        assert session.appended_text == ["Hi there!", "Thanks for calling."]
        assert session.text_commits == 2
        await session.complete_utterance()
        await service.flush_audio(context_id)
        await asyncio.wait_for(state.task, 2)
        service.push_error.assert_not_awaited()
        assert session.finishes == 1
        assert session.closed
    finally:
        second.cancel()
        await asyncio.gather(second, return_exceptions=True)
        await service._finish_all(interrupted=True)


@pytest.mark.parametrize("terminal", ["interrupt", "error", "eof"])
async def test_tts_waiting_sentence_is_released_on_terminal_event(terminal: str) -> None:
    session = SequentialTTSGatewaySession()
    service = SpekoTTSService(FakeGatewayClient(session), sample_rate=24_000)
    service._sample_rate = 24_000
    service.push_error = AsyncMock()
    context_id = "turn-terminal"
    await service.create_audio_context(context_id)
    await _run_once(service.run_tts("First sentence.", context_id))
    state = service._contexts[context_id]
    second = asyncio.create_task(
        _run_once(service.run_tts("Queued sentence.", context_id))
    )
    try:
        await asyncio.sleep(0)
        assert not second.done()
        if terminal == "interrupt":
            await service.on_audio_context_interrupted(context_id)
        elif terminal == "error":
            await session.pending.put(
                CanonicalEvent(
                    type="error",
                    data={"source": "provider", "code": "provider_unavailable"},
                )
            )
        else:
            await session.pending.put(None)
        await asyncio.wait_for(second, 2)
        await asyncio.wait_for(
            asyncio.gather(state.task, return_exceptions=True), 2
        )
        assert state.task.done()
        assert session.appended_text == ["First sentence."]
        assert not session.overlapped
        assert service.push_error.await_count == (1 if terminal == "error" else 0)
    finally:
        second.cancel()
        await asyncio.gather(second, return_exceptions=True)
        await service._finish_all(interrupted=True)
