from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from livekit import rtc
from livekit.agents import APIConnectionError, APIConnectOptions
from livekit.agents import llm as agents_llm
from livekit.agents import stt as agents_stt

from speko_gateway._livekit_bridge import (
    LiveKitSpeechEvent,
    LiveKitTTSEvent,
    execution_from_env,
)
from speko_gateway.client import GatewayError
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
            str(error) == "Speko Gateway STT failed (runtime/session_lifetime_exceeded)"
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
            str(error)
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
            "relay rejected request (insufficient_credit, HTTP 402)",
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


async def test_llm_stream_preserves_relay_error_classification() -> None:
    client = FailingRelayClient([])
    plugin = LLM(client)  # type: ignore[arg-type]
    ctx = agents_llm.ChatContext.empty()
    ctx.add_message(role="user", content="hi")

    stream = plugin.chat(chat_ctx=ctx, conn_options=APIConnectOptions(max_retry=0))
    try:
        _ = [chunk async for chunk in stream]
    except APIConnectionError as error:
        assert str(error) == "Speko relay LLM failed (insufficient_credit)"
        assert error.retryable is False
    else:
        raise AssertionError("expected the relay failure to reach LiveKit")

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
