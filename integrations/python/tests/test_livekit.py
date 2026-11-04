from __future__ import annotations

import asyncio
import dataclasses
import time
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest
from livekit import rtc
from livekit.agents import APIConnectionError, APIConnectOptions, APIError
from livekit.agents import llm as agents_llm
from livekit.agents import stt as agents_stt

from speko_gateway import livekit as speko_livekit
from speko_gateway._livekit_bridge import (
    LiveKitSpeechEvent,
    LiveKitTTSEvent,
    execution_from_env,
)
from speko_gateway.client import CanonicalEvent, GatewayError, SessionConfig
from speko_gateway.livekit import LLM, STT, TTS
from speko_gateway.relay import RelayError


class FakeClient:
    def __init__(self) -> None:
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True


class FakeStream:
    def __init__(self) -> None:
        self.frames: list[bytes] = []
        self.flushes = 0
        self.finishes = 0
        self.closed = False
        self.finished = asyncio.Event()
        self.final_received_while_open = False

    async def push_frame(self, frame: rtc.AudioFrame) -> None:
        self.frames.append(bytes(frame.data))

    async def flush(self) -> None:
        self.flushes += 1

    async def finish(self) -> None:
        self.finishes += 1
        self.finished.set()

    async def aclose(self) -> None:
        self.closed = True

    async def events(self) -> AsyncIterator[LiveKitSpeechEvent]:
        yield LiveKitSpeechEvent(type="speech.started", provider_request_id="dg-test")
        yield LiveKitSpeechEvent(type="transcript.delta", text="hello")
        await self.finished.wait()
        self.final_received_while_open = not self.closed
        yield LiveKitSpeechEvent(type="transcript.final", text="hello world")
        yield LiveKitSpeechEvent(type="speech.ended")


class FakeBridge:
    def __init__(self, stream: FakeStream) -> None:
        self.stream = stream

    async def start(self, _frame: rtc.AudioFrame) -> FakeStream:
        return self.stream


class FailingStream(FakeStream):
    async def events(self) -> AsyncIterator[LiveKitSpeechEvent]:
        raise GatewayError(
            "Gateway provider stream failed",
            code="session_lifetime_exceeded",
            source="runtime",
            retryable=True,
        )
        yield  # pragma: no cover


def frame() -> rtc.AudioFrame:
    return rtc.AudioFrame(
        data=b"\x00\x00" * 320,
        sample_rate=16_000,
        num_channels=1,
        samples_per_channel=320,
    )


async def test_stream_maps_gateway_events_to_livekit() -> None:
    client = FakeClient()
    plugin = STT(client)  # type: ignore[arg-type]
    fake_stream = FakeStream()
    stream = plugin.stream()
    stream._bridge = FakeBridge(fake_stream)  # type: ignore[assignment]

    stream.push_frame(frame())
    stream.flush()
    stream.end_input()
    events = [event async for event in stream]

    assert [event.type for event in events] == [
        agents_stt.SpeechEventType.START_OF_SPEECH,
        agents_stt.SpeechEventType.INTERIM_TRANSCRIPT,
        agents_stt.SpeechEventType.FINAL_TRANSCRIPT,
        agents_stt.SpeechEventType.END_OF_SPEECH,
    ]
    assert events[2].alternatives[0].text == "hello world"
    assert events[2].request_id == "dg-test"
    assert fake_stream.frames == [bytes(frame().data)]
    assert fake_stream.flushes == 1
    assert fake_stream.finishes == 1
    assert fake_stream.final_received_while_open is True
    assert fake_stream.closed is True

    await plugin.aclose()
    assert client.closed is False


class SpeechEndedBeforeFinalStream(FakeStream):
    async def events(self) -> AsyncIterator[LiveKitSpeechEvent]:
        yield LiveKitSpeechEvent(type="speech.started", provider_request_id="dg-order-test")
        yield LiveKitSpeechEvent(type="transcript.delta", text="hello")
        await self.finished.wait()
        self.final_received_while_open = not self.closed
        # speech.ended arrives BEFORE transcript.final
        yield LiveKitSpeechEvent(type="speech.ended")
        yield LiveKitSpeechEvent(type="transcript.final", text="hello world")


async def test_stream_handles_speech_ended_before_final_transcript() -> None:
    client = FakeClient()
    plugin = STT(client)  # type: ignore[arg-type]
    fake_stream = SpeechEndedBeforeFinalStream()
    stream = plugin.stream()
    stream._bridge = FakeBridge(fake_stream)  # type: ignore[assignment]

    stream.push_frame(frame())
    stream.flush()
    stream.end_input()
    events = [event async for event in stream]

    assert [event.type for event in events] == [
        agents_stt.SpeechEventType.START_OF_SPEECH,
        agents_stt.SpeechEventType.INTERIM_TRANSCRIPT,
        agents_stt.SpeechEventType.END_OF_SPEECH,
        agents_stt.SpeechEventType.FINAL_TRANSCRIPT,
    ]
    assert events[3].alternatives[0].text == "hello world"
    assert events[3].request_id == "dg-order-test"
    assert stream._speaking is False

    await plugin.aclose()


class GoogleMultiTurnStream(FakeStream):
    async def events(self) -> AsyncIterator[LiveKitSpeechEvent]:
        # Turn 1: transcript.final followed by speech.ended
        yield LiveKitSpeechEvent(type="transcript.final", text="turn one", provider_request_id="google-1")
        yield LiveKitSpeechEvent(type="speech.ended")
        # Turn 2: transcript.final followed by speech.ended
        yield LiveKitSpeechEvent(type="transcript.final", text="turn two", provider_request_id="google-2")
        yield LiveKitSpeechEvent(type="speech.ended")


async def test_stream_handles_multi_turn_transcript_led_events() -> None:
    client = FakeClient()
    plugin = STT(client)  # type: ignore[arg-type]
    fake_stream = GoogleMultiTurnStream()
    stream = plugin.stream()
    stream._bridge = FakeBridge(fake_stream)  # type: ignore[assignment]

    stream.push_frame(frame())
    stream.flush()
    stream.end_input()
    events = [event async for event in stream]

    # Both turns must receive balanced START_OF_SPEECH, FINAL_TRANSCRIPT, and END_OF_SPEECH
    assert [event.type for event in events] == [
        agents_stt.SpeechEventType.START_OF_SPEECH,
        agents_stt.SpeechEventType.FINAL_TRANSCRIPT,
        agents_stt.SpeechEventType.END_OF_SPEECH,
        agents_stt.SpeechEventType.START_OF_SPEECH,
        agents_stt.SpeechEventType.FINAL_TRANSCRIPT,
        agents_stt.SpeechEventType.END_OF_SPEECH,
    ]
    assert events[1].alternatives[0].text == "turn one"
    assert events[4].alternatives[0].text == "turn two"
    assert stream._speaking is False

    await plugin.aclose()


async def test_end_input_flushes_and_drains_final_events_before_close() -> None:
    plugin = STT(FakeClient())  # type: ignore[arg-type]
    fake_stream = FakeStream()
    stream = plugin.stream()
    stream._bridge = FakeBridge(fake_stream)  # type: ignore[assignment]

    stream.push_frame(frame())
    stream.end_input()
    events = [event async for event in stream]

    assert agents_stt.SpeechEventType.FINAL_TRANSCRIPT in {
        event.type for event in events
    }
    assert fake_stream.flushes == 1
    assert fake_stream.finishes == 1
    assert fake_stream.final_received_while_open is True
    assert fake_stream.closed is True


def test_plugin_declares_streaming_stt() -> None:
    plugin = STT(FakeClient())  # type: ignore[arg-type]
    assert plugin.capabilities.streaming is True
    assert plugin.capabilities.interim_results is True
    assert plugin.provider == "speko"
    assert plugin.model == "auto"


async def test_stream_preserves_safe_gateway_error_classification() -> None:
    plugin = STT(FakeClient())  # type: ignore[arg-type]
    stream = plugin.stream(conn_options=APIConnectOptions(max_retry=0))
    stream._bridge = FakeBridge(FailingStream())  # type: ignore[assignment]
    stream.push_frame(frame())
    stream.end_input()

    try:
        _ = [event async for event in stream]
    except APIConnectionError as error:
        assert (
            error.message == "Speko Gateway STT failed (runtime/session_lifetime_exceeded)"
        )
        assert error.retryable is True
    else:
        raise AssertionError("expected the Gateway failure to reach LiveKit")

    await plugin.aclose()


class FakeTTSStream:
    def __init__(self) -> None:
        self.appended: list[str] = []
        self.commits = 0
        self.finishes = 0
        self.closed = False
        self.finished = asyncio.Event()
        self.done_sent_while_open = False

    async def append_text(self, text: str) -> None:
        self.appended.append(text)

    async def commit(self) -> None:
        self.commits += 1

    async def finish(self) -> None:
        self.finishes += 1
        self.finished.set()

    async def aclose(self) -> None:
        self.closed = True

    async def events(self) -> AsyncIterator[LiveKitTTSEvent]:
        yield LiveKitTTSEvent(type="audio.started", provider_request_id="dg-tts")
        yield LiveKitTTSEvent(type="audio.frame", audio=b"\x01\x00" * 2_400)
        await self.finished.wait()
        self.done_sent_while_open = not self.closed
        yield LiveKitTTSEvent(type="audio.frame", audio=b"\x02\x00" * 2_400)
        yield LiveKitTTSEvent(type="audio.done")


class FakeTTSBridge:
    def __init__(self, stream: FakeTTSStream) -> None:
        self.stream = stream

    async def start(self) -> FakeTTSStream:
        return self.stream


class FailingTTSStream(FakeTTSStream):
    async def events(self) -> AsyncIterator[LiveKitTTSEvent]:
        raise GatewayError(
            "Gateway provider stream failed",
            code="usage_reservation_exhausted",
            source="runtime",
            retryable=False,
        )
        yield  # pragma: no cover


async def test_tts_stream_maps_gateway_audio_to_livekit() -> None:
    client = FakeClient()
    plugin = TTS(client)  # type: ignore[arg-type]
    fake_stream = FakeTTSStream()
    stream = plugin.stream()
    stream._bridge = FakeTTSBridge(fake_stream)  # type: ignore[assignment]

    stream.push_text("hello ")
    stream.push_text("world")
    stream.flush()
    stream.end_input()
    audio = [event async for event in stream]

    assert audio, "expected synthesized audio frames"
    assert audio[-1].is_final is True
    assert {event.segment_id for event in audio} == {"dg-tts"}
    combined = b"".join(bytes(event.frame.data) for event in audio)
    assert combined == b"\x01\x00" * 2_400 + b"\x02\x00" * 2_400
    assert fake_stream.appended == ["hello ", "world"]
    assert fake_stream.commits == 1
    assert fake_stream.finishes == 1
    assert fake_stream.done_sent_while_open is True
    assert fake_stream.closed is True

    await plugin.aclose()
    assert client.closed is False


def test_plugin_declares_streaming_tts() -> None:
    plugin = TTS(FakeClient())  # type: ignore[arg-type]
    assert plugin.capabilities.streaming is True
    assert plugin.provider == "speko"
    assert plugin.model == "auto"
    assert plugin.sample_rate == 24_000
    assert plugin.num_channels == 1


async def test_tts_stream_preserves_safe_gateway_error_classification() -> None:
    plugin = TTS(FakeClient())  # type: ignore[arg-type]
    stream = plugin.stream(conn_options=APIConnectOptions(max_retry=0))
    stream._bridge = FakeTTSBridge(FailingTTSStream())  # type: ignore[assignment]
    stream.push_text("hello")
    stream.end_input()

    try:
        _ = [event async for event in stream]
    except APIConnectionError as error:
        assert (
            error.message
            == "Speko Gateway TTS failed (runtime/usage_reservation_exhausted)"
        )
        assert error.retryable is False
    else:
        raise AssertionError("expected the Gateway failure to reach LiveKit")

    await plugin.aclose()


class FakeRelayClient:
    def __init__(self, events: list[tuple[str, dict]]) -> None:
        self.events = events
        self.requests: list[dict] = []
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True

    async def stream_response(self, request: dict):
        self.requests.append(request)
        for event, payload in self.events:
            yield event, payload


class FailingRelayClient(FakeRelayClient):
    async def stream_response(self, request: dict):
        self.requests.append(request)
        raise RelayError(
            "Router rejected request (insufficient_credit, HTTP 402)",
            code="insufficient_credit",
            retryable=False,
        )
        yield  # pragma: no cover


def _relay_events() -> list[tuple[str, dict]]:
    return [
        ("response.created", {"response_id": "resp_req-1"}),
        (
            "response.item.added",
            {"output_index": 0, "item": {"type": "message", "role": "assistant"}},
        ),
        ("response.text.delta", {"output_index": 0, "delta": "Hello"}),
        ("response.text.delta", {"output_index": 0, "delta": " there"}),
        (
            "response.item.completed",
            {
                "output_index": 1,
                "item": {
                    "type": "function_call",
                    "call_id": "call-1",
                    "name": "lookup",
                    "arguments": '{"city": "Kyiv"}',
                },
            },
        ),
        (
            "response.completed",
            {
                "stop_reason": "tool_call",
                "usage": {
                    "input_tokens": 90,
                    "cached_input_tokens": 10,
                    "output_tokens": 7,
                    "reasoning_tokens": 3,
                },
            },
        ),
    ]


async def test_llm_stream_maps_relay_events_to_chunks() -> None:
    client = FakeRelayClient(_relay_events())
    plugin = LLM(client)  # type: ignore[arg-type]
    ctx = agents_llm.ChatContext.empty()
    ctx.add_message(role="user", content="hi")

    stream = plugin.chat(chat_ctx=ctx)
    chunks = [chunk async for chunk in stream]

    assert all(chunk.id == "resp_req-1" for chunk in chunks)
    text = "".join(
        chunk.delta.content for chunk in chunks if chunk.delta and chunk.delta.content
    )
    assert text == "Hello there"
    tool_calls = [
        call for chunk in chunks if chunk.delta for call in chunk.delta.tool_calls
    ]
    assert len(tool_calls) == 1
    assert tool_calls[0].name == "lookup"
    assert tool_calls[0].call_id == "call-1"
    assert tool_calls[0].arguments == '{"city": "Kyiv"}'
    usage = [chunk.usage for chunk in chunks if chunk.usage]
    assert len(usage) == 1
    assert usage[0].prompt_tokens == 100
    assert usage[0].prompt_cached_tokens == 10
    assert usage[0].completion_tokens == 10
    assert usage[0].total_tokens == 110

    await plugin.aclose()
    assert client.closed is False


async def test_llm_request_body_maps_history_and_tools() -> None:
    client = FakeRelayClient(_relay_events())
    plugin = LLM(client)  # type: ignore[arg-type]

    ctx = agents_llm.ChatContext.empty()
    ctx.add_message(role="developer", content="be brief")
    ctx.add_message(role="user", content="weather?")
    ctx.items.append(
        agents_llm.FunctionCall(
            call_id="call-1", name="lookup", arguments='{"city": "Kyiv"}'
        )
    )
    ctx.items.append(
        agents_llm.FunctionCallOutput(call_id="call-1", output="sunny", is_error=False)
    )

    @agents_llm.function_tool
    async def lookup(city: str) -> str:
        """Look up the weather."""
        return "sunny"

    body = plugin._request_body(ctx, [lookup])

    assert body["routing"] == {"mode": "auto", "objective": "balanced"}
    assert body["max_output_tokens"] > 0
    assert body["input"] == [
        {
            "type": "message",
            "role": "system",
            "content": [{"type": "text", "text": "be brief"}],
        },
        {
            "type": "message",
            "role": "user",
            "content": [{"type": "text", "text": "weather?"}],
        },
        {
            "type": "function_call",
            "call_id": "call-1",
            "name": "lookup",
            "arguments": '{"city": "Kyiv"}',
        },
        {"type": "function_result", "call_id": "call-1", "result": "sunny"},
    ]
    assert len(body["tools"]) == 1
    assert body["tools"][0]["name"] == "lookup"
    assert body["tools"][0]["description"] == "Look up the weather."
    assert "city" in body["tools"][0]["parameters"]["properties"]

    await plugin.aclose()


def test_llm_rejects_half_explicit_routing() -> None:
    try:
        LLM(FakeRelayClient([]), provider="openai")  # type: ignore[arg-type]
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for provider without model")


def _user_ctx() -> agents_llm.ChatContext:
    ctx = agents_llm.ChatContext.empty()
    ctx.add_message(role="user", content="hi")
    return ctx


async def test_llm_omits_reasoning_effort_by_default() -> None:
    plugin = LLM(FakeRelayClient([]))  # type: ignore[arg-type]

    assert "reasoning_effort" not in plugin._request_body(_user_ctx(), [])

    await plugin.aclose()


@pytest.mark.parametrize(
    "effort", ["none", "minimal", "low", "medium", "high", "xhigh"]
)
async def test_llm_sends_each_reasoning_effort(effort: str) -> None:
    client = FakeRelayClient(_relay_events())
    plugin = LLM(  # type: ignore[arg-type]
        client, provider="openai", model="gpt-5.6-luna", reasoning_effort=effort
    )

    stream = plugin.chat(chat_ctx=_user_ctx())
    async for _ in stream:
        pass

    assert client.requests[0]["reasoning_effort"] == effort
    assert "temperature" not in client.requests[0]

    await plugin.aclose()


@pytest.mark.parametrize("effort", ["", "None", "max", "HIGH", True, 1])
def test_llm_rejects_unknown_reasoning_effort(effort: Any) -> None:
    with pytest.raises(ValueError, match="reasoning_effort must be None or one of"):
        LLM(FakeRelayClient([]), reasoning_effort=effort)  # type: ignore[arg-type]


async def test_llm_stream_preserves_relay_error_classification() -> None:
    client = FailingRelayClient([])
    plugin = LLM(client)  # type: ignore[arg-type]
    ctx = agents_llm.ChatContext.empty()
    ctx.add_message(role="user", content="hi")

    stream = plugin.chat(chat_ctx=ctx, conn_options=APIConnectOptions(max_retry=0))
    try:
        _ = [chunk async for chunk in stream]
    except APIConnectionError as error:
        assert error.message == "Speko Router LLM failed (insufficient_credit)"
        assert error.retryable is False
    else:
        raise AssertionError("expected the Router failure to reach LiveKit")

    await plugin.aclose()


async def test_tts_stream_without_text_ends_cleanly() -> None:
    class NeverBridge:
        def __init__(self) -> None:
            self.starts = 0

        async def start(self) -> FakeTTSStream:
            self.starts += 1
            return FakeTTSStream()

    plugin = TTS(FakeClient())  # type: ignore[arg-type]
    bridge = NeverBridge()
    stream = plugin.stream()
    stream._bridge = bridge  # type: ignore[assignment]

    stream.end_input()
    audio = [event async for event in stream]

    assert audio == []
    assert bridge.starts == 0

    await plugin.aclose()


async def test_malformed_sse_payload_normalizes_to_relay_error() -> None:
    from speko_gateway.relay import _sse_events

    class StubContent:
        def __init__(self, lines: list[bytes]) -> None:
            self._lines = lines

        def __aiter__(self):
            return self._iterate()

        async def _iterate(self):
            for line in self._lines:
                yield line

    class StubResponse:
        def __init__(self, lines: list[bytes]) -> None:
            self.content = StubContent(lines)

    response = StubResponse(
        [
            b"event: response.created\n",
            b'data: {"response_id": "resp_req-1"}\n',
            b"\n",
            b"event: response.text.delta\n",
            b"data: {not json}\n",
            b"\n",
        ]
    )

    received: list[str] = []
    try:
        async for event, _payload in _sse_events(response):  # type: ignore[arg-type]
            received.append(event)
    except RelayError as error:
        assert error.retryable is True
        assert "malformed" in str(error)
    else:
        raise AssertionError("expected malformed SSE to raise RelayError")
    assert received == ["response.created"]


def test_routing_mode_follows_gateway_configuration(monkeypatch) -> None:
    monkeypatch.delenv("SPEKO_API_KEY", raising=False)
    monkeypatch.delenv("SPEKO_API_KEY_FILE", raising=False)
    assert execution_from_env() == {
        "provider_route": "provider_direct",
        "credential_source": "byok",
        "relay_policy": "forbidden",
    }

    monkeypatch.setenv("SPEKO_API_KEY", "configured")
    assert execution_from_env() == {
        "provider_route": "auto",
        "credential_source": "managed",
        "relay_policy": "allowed",
    }

    assert execution_from_env("byok") == {
        "provider_route": "provider_direct",
        "credential_source": "byok",
        "relay_policy": "forbidden",
    }
    monkeypatch.delenv("SPEKO_API_KEY", raising=False)
    assert execution_from_env("managed") == {
        "provider_route": "auto",
        "credential_source": "managed",
        "relay_policy": "allowed",
    }


async def test_voice_plugins_forward_provider_and_credential_source() -> None:
    stt_plugin = STT(
        FakeClient(),
        provider="assemblyai",
        credential_source="byok",  # type: ignore[arg-type]
    )
    stt_stream = stt_plugin.stream()
    stt_bridge = stt_stream._bridge
    assert stt_bridge._provider == "assemblyai"
    assert stt_bridge._model == "auto"
    assert stt_bridge._credential_source == "byok"

    tts_plugin = TTS(
        FakeClient(),
        provider="elevenlabs",
        credential_source="managed",  # type: ignore[arg-type]
    )
    tts_stream = tts_plugin.stream()
    tts_bridge = tts_stream._bridge
    assert tts_bridge._provider == "elevenlabs"
    assert tts_bridge._credential_source == "managed"
    await stt_stream.aclose()
    await tts_stream.aclose()


def test_stt_options_payload_builds_the_request_block() -> None:
    from speko_gateway._voice import stt_options_payload

    assert stt_options_payload() is None
    assert stt_options_payload(keywords=[]) is None
    # Explicit False IS sent: it turns a vendor default off, which is not the
    # same as saying nothing.
    assert stt_options_payload(diarization=False) == {"diarization": False}
    assert stt_options_payload(
        diarization=True,
        keywords=["Speko", "Casey"],
        noise_reduction=True,
        provider_options={"deepgram": {"numerals": True}},
    ) == {
        "diarization": True,
        "keywords": ["Speko", "Casey"],
        "noise_reduction": True,
        "provider_options": {"deepgram": {"numerals": True}},
    }


_HAS_KEYTERMS = "keyterms" in {
    field.name for field in dataclasses.fields(agents_stt.STTCapabilities)
}


def test_stt_declares_the_capabilities_it_asked_for() -> None:
    plain = STT(FakeClient())  # type: ignore[arg-type]
    assert plain.capabilities.diarization is False
    if _HAS_KEYTERMS:  # livekit-agents >= 1.6.5
        assert plain.capabilities.keyterms is False
    assert plain._stt_options is None

    asking = STT(  # type: ignore[arg-type]
        FakeClient(),
        diarization=True,
        keywords=["Speko"],
        provider_options={"deepgram": {"numerals": True}},
    )
    # Declared here, enforced at session create: a provider that cannot honor
    # the ask refuses the session (stt_option_unsupported) before any audio.
    assert asking.capabilities.diarization is True
    if _HAS_KEYTERMS:
        assert asking.capabilities.keyterms is True
    assert asking._stt_options == {
        "diarization": True,
        "keywords": ["Speko"],
        "provider_options": {"deepgram": {"numerals": True}},
    }


@dataclasses.dataclass
class _PreKeytermsSTTCapabilities:
    """STTCapabilities as livekit-agents shipped it before 1.6.5."""

    streaming: bool
    interim_results: bool
    diarization: bool = False
    aligned_transcript: bool = False
    offline_recognize: bool = True


def test_stt_constructs_on_livekit_agents_before_keyterms(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Runs against whatever livekit-agents is installed; CI also runs the whole
    # suite against the real 1.4.5 floor.
    monkeypatch.setattr(agents_stt, "STTCapabilities", _PreKeytermsSTTCapabilities)

    asking = STT(  # type: ignore[arg-type]
        FakeClient(),
        diarization=True,
        keywords=["Speko"],
    )

    assert asking.capabilities == _PreKeytermsSTTCapabilities(
        streaming=True,
        interim_results=True,
        diarization=True,
    )
    # The capability flag is gone, but the keywords still reach the Gateway.
    assert asking._stt_options == {"diarization": True, "keywords": ["Speko"]}


def test_stt_on_livekit_agents_before_manual_flush_keeps_flushing_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(agents_stt, "STTCapabilities", _PreKeytermsSTTCapabilities)

    plugin = STT(FakeClient(), provider="soniox")  # type: ignore[arg-type]

    assert plugin.capabilities == _PreKeytermsSTTCapabilities(
        streaming=True,
        interim_results=True,
    )


@dataclasses.dataclass
class _ManualFlushSTTCapabilities:
    """STTCapabilities as livekit-agents ships it from 1.8.9."""

    streaming: bool
    interim_results: bool
    diarization: bool = False
    aligned_transcript: bool = False
    offline_recognize: bool = True
    keyterms: bool = False
    chat_context: bool = False
    manual_flush: bool = False


@pytest.mark.parametrize(
    ("provider", "manual_flush"),
    [
        # audio.commit is Soniox's mid-stream finalize.
        ("soniox", True),
        (" Soniox ", True),
        # The route is unknown until the session plan names it.
        ("auto", False),
        # audio.commit is a no-op on Deepgram Flux and drops a sub-50 ms
        # AssemblyAI tail; it ends the input on Gladia and Gemini, and
        # transcribes a buffered batch on Hamsa.
        ("deepgram", False),
        ("assemblyai", False),
        ("gladia", False),
        ("gemini", False),
        ("hamsa", False),
    ],
)
def test_stt_asks_livekit_to_flush_at_turn_end_only_where_commit_finalizes(
    monkeypatch: pytest.MonkeyPatch, provider: str, manual_flush: bool
) -> None:
    monkeypatch.setattr(agents_stt, "STTCapabilities", _ManualFlushSTTCapabilities)

    plugin = STT(FakeClient(), provider=provider)  # type: ignore[arg-type]

    assert plugin.capabilities.manual_flush is manual_flush


def test_stt_declares_manual_flush_on_the_installed_livekit_agents() -> None:
    plugin = STT(FakeClient(), provider="soniox")  # type: ignore[arg-type]
    fields = {field.name for field in dataclasses.fields(agents_stt.STTCapabilities)}
    if "manual_flush" in fields:  # livekit-agents >= 1.8.9
        assert plugin.capabilities.manual_flush is True
    else:
        assert not hasattr(plugin.capabilities, "manual_flush")


def test_cache_write_tokens_are_included_in_completion_usage() -> None:
    from speko_gateway.livekit import _completion_usage

    usage = _completion_usage({
        "input_tokens": 300,
        "cached_input_tokens": 100,
        "cache_write_5m_tokens": 11,
        "cache_write_1h_tokens": 19,
        "output_tokens": 34,
    })
    assert usage.prompt_tokens == 430
    assert usage.prompt_cached_tokens == 100
    assert usage.total_tokens == 464


class GoogleEmptyFinalStream(FakeStream):
    async def events(self) -> AsyncIterator[LiveKitSpeechEvent]:
        yield LiveKitSpeechEvent(type="transcript.delta", text="partial", provider_request_id="google-silent")
        yield LiveKitSpeechEvent(type="transcript.final", text="")
        yield LiveKitSpeechEvent(type="speech.ended")
        yield LiveKitSpeechEvent(type="transcript.final", text="next turn", provider_request_id="google-next")
        yield LiveKitSpeechEvent(type="speech.ended")


async def test_empty_final_does_not_steal_the_next_turn_start() -> None:
    plugin = STT(FakeClient())  # type: ignore[arg-type]
    stream = plugin.stream()
    stream._bridge = FakeBridge(GoogleEmptyFinalStream())  # type: ignore[assignment]
    stream.push_frame(frame())
    stream.flush()
    stream.end_input()
    events = [event async for event in stream]
    assert [event.type for event in events] == [
        agents_stt.SpeechEventType.START_OF_SPEECH,
        agents_stt.SpeechEventType.INTERIM_TRANSCRIPT,
        agents_stt.SpeechEventType.END_OF_SPEECH,
        agents_stt.SpeechEventType.START_OF_SPEECH,
        agents_stt.SpeechEventType.FINAL_TRANSCRIPT,
        agents_stt.SpeechEventType.END_OF_SPEECH,
    ]
    assert events[4].alternatives[0].text == "next turn"
    await plugin.aclose()


class IdentifiedLanguageStream(FakeStream):
    async def events(self) -> AsyncIterator[LiveKitSpeechEvent]:
        yield LiveKitSpeechEvent(type="transcript.delta", text="ನಮಸ್ಕಾರ")
        await self.finished.wait()
        yield LiveKitSpeechEvent(type="transcript.final", text="ನಮಸ್ಕಾರ", language="kn")
        yield LiveKitSpeechEvent(type="transcript.final", text="hello")


async def test_final_reports_the_identified_language() -> None:
    plugin = STT(FakeClient(), language="multi")  # type: ignore[arg-type]
    stream = plugin.stream()
    stream._bridge = FakeBridge(IdentifiedLanguageStream())  # type: ignore[assignment]

    stream.push_frame(frame())
    stream.end_input()
    finals = [
        event
        async for event in stream
        if event.type == agents_stt.SpeechEventType.FINAL_TRANSCRIPT
    ]

    # An identified final carries its language; one without keeps the
    # session's configured language.
    assert [final.alternatives[0].language for final in finals] == ["kn", "multi"]
    await plugin.aclose()


async def test_tts_update_options_applies_to_the_next_stream() -> None:
    plugin = TTS(FakeClient(), language="en", voice="v-en")  # type: ignore[arg-type]
    plugin.update_options(language="kn", voice="")
    stream = plugin.stream()
    assert stream._bridge._language == "kn"
    assert stream._bridge._voice == ""
    await plugin.aclose()


class _RecordingOpenClient:
    def __init__(self) -> None:
        self.opened: list[Any] = []

    async def open(self, config: Any) -> object:
        self.opened.append(config)
        return object()


def test_tts_options_payload_builds_the_request_block() -> None:
    from speko_gateway._voice import tts_options_payload

    assert tts_options_payload() is None
    assert tts_options_payload(provider_options={}) is None
    assert tts_options_payload(
        provider_options={"cartesia": {"normalization": "en-IN"}}
    ) == {"provider_options": {"cartesia": {"normalization": "en-IN"}}}


async def test_tts_provider_options_reach_the_session_request() -> None:
    from speko_gateway._livekit_bridge import LiveKitTTSBridge

    plugin = TTS(  # type: ignore[arg-type]
        FakeClient(),
        provider="elevenlabs",
        model="eleven_flash_v2_5",
        provider_options={"elevenlabs": {"stability": 0.4, "style": 0.2}},
    )
    stream = plugin.stream()
    bridge = stream._bridge
    assert isinstance(bridge, LiveKitTTSBridge)
    recorder = _RecordingOpenClient()
    bridge._client = recorder  # type: ignore[assignment]
    await bridge.start()
    assert recorder.opened[0].request["tts"] == {
        "provider_options": {"elevenlabs": {"stability": 0.4, "style": 0.2}}
    }
    await stream.aclose()

    # A plugin with no settings sends no tts block at all, so requests stay
    # byte-identical for every existing caller.
    plain = TTS(FakeClient())  # type: ignore[arg-type]
    plain_stream = plain.stream()
    plain_recorder = _RecordingOpenClient()
    plain_stream._bridge._client = plain_recorder  # type: ignore[assignment]
    await plain_stream._bridge.start()  # type: ignore[attr-defined]
    assert "tts" not in plain_recorder.opened[0].request
    await plain_stream.aclose()
    await plugin.aclose()
    await plain.aclose()


async def test_tts_update_options_replaces_and_clears_provider_options() -> None:
    plugin = TTS(  # type: ignore[arg-type]
        FakeClient(), provider_options={"cartesia": {"normalization": "en-IN"}}
    )
    plugin.update_options(provider_options={"cartesia": {"normalization": "off"}})
    assert plugin.stream()._bridge._tts_options == {  # type: ignore[attr-defined]
        "provider_options": {"cartesia": {"normalization": "off"}}
    }
    plugin.update_options(provider_options=None)
    assert plugin.stream()._bridge._tts_options is None  # type: ignore[attr-defined]
    plugin.update_options(language="hi")
    assert plugin._tts_options is None
    await plugin.aclose()


class FakeGatewaySession:
    def __init__(
        self,
        number: int,
        request: dict[str, Any],
        *,
        answer_delay: float,
        quirk: str | None,
    ) -> None:
        self.metadata = {
            "session_id": f"session_{number}",
            "attempt_id": f"attempt_{number}",
        }
        self.request = request
        self.appended: list[str] = []
        self.first_text_at: float | None = None
        self.closed = False
        self._answer_delay = answer_delay
        self._quirk = quirk
        self._peer_closed = False
        self._finished = asyncio.Event()
        self._speaker: asyncio.Task[None] | None = None
        self._events: asyncio.Queue[CanonicalEvent | None] = asyncio.Queue()
        self._events.put_nowait(CanonicalEvent(type="session.ready"))
        if quirk == "fails-at-open":
            self.fail()
        elif quirk == "closes-at-open":
            self._events.put_nowait(CanonicalEvent(type="session.closed"))
            self._events.put_nowait(None)

    async def append_text(self, text: str) -> None:
        if self._quirk == "refuses-text" or self.closed or self._peer_closed:
            raise ConnectionResetError("Cannot write to closing transport")
        if self._speaker is None:
            self.first_text_at = time.perf_counter()
            self._speaker = asyncio.create_task(self._speak())
        self.appended.append(text)

    async def commit_text(self) -> None:
        pass

    async def finish(self) -> None:
        self._finished.set()

    async def aclose(self) -> None:
        self.closed = True
        if self._speaker is not None:
            self._speaker.cancel()
        self._events.put_nowait(None)

    def fail(self) -> None:
        self._events.put_nowait(
            CanonicalEvent(
                type="error",
                data={"code": "provider_closed", "source": "provider"},
            )
        )
        self._events.put_nowait(None)

    async def events(self) -> AsyncIterator[CanonicalEvent]:
        while (event := await self._events.get()) is not None:
            yield event
        self._peer_closed = True

    async def _speak(self) -> None:
        await asyncio.sleep(self._answer_delay)
        self._events.put_nowait(
            CanonicalEvent(type="audio.started", data={"provider_request_id": "req"})
        )
        self._events.put_nowait(
            CanonicalEvent(type="audio.frame", audio=b"\x01\x00" * 2_400)
        )
        await self._finished.wait()
        self._events.put_nowait(CanonicalEvent(type="audio.done"))
        self._events.put_nowait(None)


class FakeGateway:
    def __init__(self, *, open_delay: float = 0.01, answer_delay: float = 0.05) -> None:
        self.open_delay = open_delay
        self.answer_delay = answer_delay
        self.opens_allowed = asyncio.Event()
        self.opens_allowed.set()
        self.opens = 0
        self.cancelled_opens = 0
        self.failed_opens = 0
        self.failing_opens = 0
        self.quirks: list[str] = []
        self.sessions: list[FakeGatewaySession] = []

    async def open(self, config: SessionConfig, **_: Any) -> FakeGatewaySession:
        self.opens += 1
        try:
            await self.opens_allowed.wait()
            await asyncio.sleep(self.open_delay)
        except asyncio.CancelledError:
            self.cancelled_opens += 1
            raise
        if self.failing_opens:
            self.failing_opens -= 1
            self.failed_opens += 1
            raise GatewayError(
                "Gateway rejected request",
                code="provider_unavailable",
                source="provider",
            )
        session = FakeGatewaySession(
            len(self.sessions),
            config.request,
            answer_delay=self.answer_delay,
            quirk=self.quirks.pop(0) if self.quirks else None,
        )
        self.sessions.append(session)
        return session

    def spoken(self) -> list[FakeGatewaySession]:
        return [session for session in self.sessions if session.appended]

    def nothing_left_open(self) -> bool:
        settled = len(self.sessions) + self.cancelled_opens + self.failed_opens
        return settled == self.opens and all(s.closed for s in self.sessions)

    async def aclose(self) -> None:
        pass


async def until(condition: Callable[[], bool], timeout: float = 2.0) -> None:
    deadline = time.perf_counter() + timeout
    while not condition():
        assert time.perf_counter() < deadline, "condition not reached in time"
        await asyncio.sleep(0.005)


async def speak(plugin: TTS, text: str) -> tuple[float, list[Any]]:
    stream = plugin.stream(conn_options=APIConnectOptions(max_retry=0))
    pushed_at = time.perf_counter()
    stream.push_text(text)
    stream.end_input()

    async def audio() -> list[Any]:
        return [event async for event in stream]

    try:
        return pushed_at, await asyncio.wait_for(audio(), timeout=5)
    finally:
        await stream.aclose()


async def test_tts_second_reply_does_not_wait_for_a_session_to_open() -> None:
    gateway = FakeGateway(open_delay=0.5)
    plugin = TTS(gateway)  # type: ignore[arg-type]

    first_pushed, first_audio = await speak(plugin, "first")
    await until(lambda: len(gateway.sessions) == 2)
    gateway.opens_allowed.clear()
    _, second_audio = await speak(plugin, "second")

    assert first_audio and second_audio
    spoken = {session.appended[0]: session for session in gateway.spoken()}
    assert set(spoken) == {"first", "second"}
    assert spoken["first"].first_text_at - first_pushed >= 0.45, (
        "the first reply waited"
    )
    assert len(gateway.sessions) == 2, "the second reply spoke on an open session"

    await plugin.aclose()
    assert gateway.nothing_left_open()


async def test_tts_first_reply_after_prewarm_uses_the_standby() -> None:
    gateway = FakeGateway(open_delay=0.3)
    plugin = TTS(gateway)  # type: ignore[arg-type]
    plugin.prewarm()
    plugin.prewarm()
    await until(lambda: len(gateway.sessions) == 1)
    assert gateway.opens == 1, "a second prewarm() opens nothing more"
    gateway.opens_allowed.clear()

    _, audio = await speak(plugin, "hello")

    assert audio
    assert gateway.sessions[0].appended == ["hello"]

    await plugin.aclose()
    assert gateway.nothing_left_open()


async def test_tts_language_change_replaces_the_standby() -> None:
    gateway = FakeGateway()
    plugin = TTS(gateway, language="en")  # type: ignore[arg-type]
    plugin.prewarm()
    await until(lambda: len(gateway.sessions) == 1)
    english = gateway.sessions[0]

    plugin.update_options(language="en")
    assert gateway.opens == 1, "an unchanged language keeps the standby"
    plugin.update_options(language="hi")
    await until(lambda: english.closed and len(gateway.sessions) == 2)
    gateway.opens_allowed.clear()
    _, audio = await speak(plugin, "namaste")

    assert audio
    (spoken,) = gateway.spoken()
    assert spoken.request["language"] == "hi"
    assert english.appended == []

    await plugin.aclose()
    assert gateway.nothing_left_open()


@pytest.mark.parametrize(
    "death",
    [
        "ended-while-idle",
        "closed-at-open",
        "open-failed",
        "opening-then-failed",
        "opening-then-ended",
    ],
)
async def test_tts_dead_standby_falls_back_to_a_fresh_session(death: str) -> None:
    opening = death.startswith("opening-")
    gateway = FakeGateway(open_delay=0.2 if opening else 0.01)
    gateway.failing_opens = int(death in ("open-failed", "opening-then-failed"))
    gateway.quirks = {
        "closed-at-open": ["closes-at-open"],
        "opening-then-ended": ["fails-at-open"],
    }.get(death, [])
    plugin = TTS(gateway)  # type: ignore[arg-type]
    plugin.prewarm()
    if death == "ended-while-idle":
        await until(lambda: len(gateway.sessions) == 1)
        gateway.sessions[0].fail()
    elif death == "closed-at-open":
        await until(lambda: len(gateway.sessions) == 1)
    elif death == "open-failed":
        await until(lambda: gateway.failed_opens == 1)
    await asyncio.sleep(0 if opening else 0.05)

    _, audio = await speak(plugin, "hello")

    assert audio, "the reply speaks on a fresh session"
    (spoken,) = gateway.spoken()
    assert spoken.appended == ["hello"]
    if gateway.sessions[0] is not spoken:
        assert gateway.sessions[0].appended == []

    await plugin.aclose()
    assert gateway.nothing_left_open()


async def test_tts_moves_a_reply_to_a_fresh_session_at_most_once() -> None:
    gateway = FakeGateway(open_delay=0.2)
    gateway.failing_opens = 3
    plugin = TTS(gateway)  # type: ignore[arg-type]
    plugin.prewarm()

    with pytest.raises(APIConnectionError) as caught:
        await speak(plugin, "hello")

    assert (
        caught.value.message
        == "Speko Gateway TTS failed (provider/provider_unavailable)"
    )
    assert gateway.opens == 3, "the standby, its replacement, one fresh session"

    await plugin.aclose()
    assert gateway.nothing_left_open()


async def test_tts_does_not_resend_text_a_standby_refused() -> None:
    gateway = FakeGateway()
    gateway.quirks = ["refuses-text"]
    plugin = TTS(gateway)  # type: ignore[arg-type]
    plugin.prewarm()
    await until(lambda: len(gateway.sessions) == 1)

    with pytest.raises(APIConnectionError) as caught:
        await speak(plugin, "hello")

    assert caught.value.message == "Speko Gateway TTS transport failed"
    assert gateway.spoken() == []
    assert gateway.opens == 2, "the standby and its replacement, no resend"

    await plugin.aclose()
    assert gateway.nothing_left_open()


async def test_tts_without_standby_sessions_opens_each_session_at_its_text() -> None:
    gateway = FakeGateway(open_delay=0.1)
    plugin = TTS(gateway, standby_sessions=0)  # type: ignore[arg-type]
    plugin.prewarm()
    await asyncio.sleep(0.15)
    assert gateway.opens == 0

    first_pushed, _ = await speak(plugin, "first")
    second_pushed, _ = await speak(plugin, "second")

    first, second = gateway.spoken()
    assert first.first_text_at - first_pushed >= 0.09
    assert second.first_text_at - second_pushed >= 0.09
    assert gateway.opens == 2
    assert gateway.nothing_left_open()

    await plugin.aclose()


async def test_tts_sends_a_fresh_session_its_first_text_before_reading_it() -> None:
    gateway = FakeGateway()
    gateway.quirks = ["closes-at-open"]
    plugin = TTS(gateway, standby_sessions=0)  # type: ignore[arg-type]

    with pytest.raises(APIError):
        await speak(plugin, "hello")

    assert gateway.sessions[0].appended == ["hello"]

    await plugin.aclose()
    assert gateway.nothing_left_open()


async def test_tts_reply_takes_a_live_standby_past_one_that_ended() -> None:
    gateway = FakeGateway()
    plugin = TTS(gateway, standby_sessions=2)  # type: ignore[arg-type]
    plugin.prewarm()
    await until(lambda: len(gateway.sessions) == 2)
    gateway.sessions[0].fail()
    await asyncio.sleep(0.05)
    gateway.opens_allowed.clear()

    _, audio = await speak(plugin, "hello")

    assert audio
    assert gateway.spoken() == [gateway.sessions[1]]
    assert gateway.opens == 4, "replacements for the ended and the taken standby"

    await plugin.aclose()
    assert gateway.nothing_left_open()


async def test_tts_stream_keeps_the_settings_it_was_created_with() -> None:
    gateway = FakeGateway()
    plugin = TTS(gateway, language="en")  # type: ignore[arg-type]
    plugin.prewarm()
    await until(lambda: len(gateway.sessions) == 1)
    stream = plugin.stream(conn_options=APIConnectOptions(max_retry=0))
    plugin.update_options(language="hi")
    await until(lambda: len(gateway.sessions) == 2)

    stream.push_text("hello")
    stream.end_input()
    audio = [event async for event in stream]
    await stream.aclose()

    assert audio
    (spoken,) = gateway.spoken()
    assert spoken.request["language"] == "en"

    await plugin.aclose()
    assert gateway.nothing_left_open()


@pytest.mark.parametrize(
    "ending",
    [
        "close-while-opening",
        "close-while-open",
        "interrupted-while-the-standby-opens",
        "interrupted-reply",
        "no-text-reply",
    ],
)
async def test_tts_leaves_no_gateway_session_open(ending: str) -> None:
    gateway = FakeGateway()
    if ending in ("close-while-opening", "interrupted-while-the-standby-opens"):
        gateway.opens_allowed.clear()
    plugin = TTS(gateway)  # type: ignore[arg-type]
    plugin.prewarm()
    if gateway.opens_allowed.is_set():
        await until(lambda: len(gateway.sessions) == 1)

    if ending == "interrupted-while-the-standby-opens":
        stream = plugin.stream()
        stream.push_text("hello")
        await until(lambda: gateway.opens == 2)
        await stream.aclose()
        assert gateway.cancelled_opens == 1, "the reply cancelled the open it awaited"
    elif ending == "interrupted-reply":
        stream = plugin.stream()
        stream.push_text("hello")
        await until(lambda: gateway.sessions[0].appended == ["hello"])
        await stream.aclose()
        assert gateway.sessions[0].closed
    elif ending == "no-text-reply":
        stream = plugin.stream()
        stream.end_input()
        assert [event async for event in stream] == []
        assert gateway.opens == 1, "a reply without text takes no standby"
        assert not gateway.sessions[0].closed

    await plugin.aclose()

    assert gateway.nothing_left_open()


@pytest.mark.parametrize("path", ["standby", "opening-standby", "fresh"])
async def test_tts_ttfb_counts_from_the_text_the_provider_got(path: str) -> None:
    gateway = FakeGateway(open_delay=0.6, answer_delay=0.05)
    plugin = TTS(gateway, standby_sessions=int(path != "fresh"))  # type: ignore[arg-type]
    metrics: list[Any] = []
    plugin.on("metrics_collected", metrics.append)
    plugin.prewarm()
    if path == "standby":
        await until(lambda: len(gateway.sessions) == 1)

    await speak(plugin, "hello")

    assert len(metrics) == 1
    assert 0.04 < metrics[0].ttfb < gateway.open_delay, "ttfb leaves out the open"

    await plugin.aclose()


async def test_tts_closes_a_standby_no_reply_took(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(speko_livekit, "_STANDBY_MAX_AGE", 0.1)
    gateway = FakeGateway()
    plugin = TTS(gateway)  # type: ignore[arg-type]
    plugin.prewarm()
    await until(lambda: len(gateway.sessions) == 1)

    await until(lambda: gateway.sessions[0].closed)
    await asyncio.sleep(0.15)
    assert gateway.opens == 1, "an expired standby waits for the next reply"

    _, audio = await speak(plugin, "hello")
    assert audio
    assert gateway.sessions[0].appended == []

    await plugin.aclose()
    assert gateway.nothing_left_open()


def test_tts_rejects_a_negative_standby_count() -> None:
    with pytest.raises(ValueError):
        TTS(FakeClient(), standby_sessions=-1)  # type: ignore[arg-type]
