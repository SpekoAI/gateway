from __future__ import annotations

from collections.abc import AsyncIterator

from livekit import rtc
from livekit.agents import APIConnectionError, APIConnectOptions
from livekit.agents import stt as agents_stt

from speko_gateway._livekit_bridge import (
    LiveKitSpeechEvent,
    execution_from_env,
)
from speko_gateway.client import GatewayError
from speko_gateway.livekit import STT


class FakeClient:
    def __init__(self) -> None:
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True


class FakeStream:
    def __init__(self) -> None:
        self.frames: list[bytes] = []
        self.flushes = 0
        self.closed = False

    async def push_frame(self, frame: rtc.AudioFrame) -> None:
        self.frames.append(bytes(frame.data))

    async def flush(self) -> None:
        self.flushes += 1

    async def aclose(self) -> None:
        self.closed = True

    async def events(self) -> AsyncIterator[LiveKitSpeechEvent]:
        yield LiveKitSpeechEvent(type="speech.started", provider_request_id="dg-test")
        yield LiveKitSpeechEvent(type="transcript.delta", text="hello")
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
    assert fake_stream.closed is True

    await plugin.aclose()
    assert client.closed is False


def test_plugin_declares_streaming_stt() -> None:
    plugin = STT(FakeClient())  # type: ignore[arg-type]
    assert plugin.capabilities.streaming is True
    assert plugin.capabilities.interim_results is True
    assert plugin.provider == "speko"
    assert plugin.model == "nova-3"


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
