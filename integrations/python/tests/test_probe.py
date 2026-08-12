from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from livekit import (
    agents as _agents,  # noqa: F401  (venv sanity: probe never imports it)
)
from livekit.agents import llm as agents_llm
from test_livekit import (
    FailingRelayClient,
    FakeBridge,
    FakeClient,
    FakeRelayClient,
    FakeStream,
    FakeTTSBridge,
    FakeTTSStream,
    _relay_events,
    frame,
)

import speko_gateway.probe as probe_module
from speko_gateway.livekit import LLM, STT, TTS
from speko_gateway.probe import (
    ConversationProbe,
    active_probe,
    report_leg,
    report_marker,
)


class FakeGatewayClient:
    def __init__(self) -> None:
        self.batches: list[list[dict[str, Any]]] = []
        self.closed = False

    async def post_turn_events(self, events: list[dict[str, Any]]) -> None:
        self.batches.append(list(events))

    async def aclose(self) -> None:
        self.closed = True


class FailingGatewayClient(FakeGatewayClient):
    async def post_turn_events(self, events: list[dict[str, Any]]) -> None:
        raise OSError("socket gone")


class FakeEmitter:
    def __init__(self) -> None:
        self._handlers: dict[str, list[Any]] = {}

    def on(self, event: str, callback: Any) -> None:
        self._handlers.setdefault(event, []).append(callback)

    def off(self, event: str, callback: Any) -> None:
        handlers = self._handlers.get(event, [])
        if callback in handlers:
            handlers.remove(callback)

    def emit(self, event: str, payload: Any) -> None:
        for callback in list(self._handlers.get(event, [])):
            callback(payload)


class FakeAgentSession(FakeEmitter):
    def __init__(self) -> None:
        super().__init__()
        self.output = SimpleNamespace(audio=FakeEmitter())
        self.user_state = "listening"


class FakeSpeechHandle:
    def __init__(self) -> None:
        self.interrupted = False
        self._callbacks: list[Any] = []

    def add_done_callback(self, callback: Any) -> None:
        self._callbacks.append(callback)

    def finish(self, *, interrupted: bool = False) -> None:
        self.interrupted = interrupted
        for callback in list(self._callbacks):
            callback(self)


def user_state(session: FakeAgentSession, old: str, new: str) -> None:
    session.user_state = new
    session.emit("user_state_changed", SimpleNamespace(old_state=old, new_state=new))


def posted(client: FakeGatewayClient) -> list[dict[str, Any]]:
    return [event for batch in client.batches for event in batch]


def new_probe() -> tuple[ConversationProbe, FakeAgentSession, FakeGatewayClient]:
    session = FakeAgentSession()
    client = FakeGatewayClient()
    probe = ConversationProbe(session, client=client)  # type: ignore[arg-type]
    return probe, session, client


async def test_probe_records_user_turn_lifecycle() -> None:
    probe, session, client = new_probe()
    probe.start()
    assert active_probe() is probe

    user_state(session, "listening", "speaking")
    user_state(session, "speaking", "listening")
    session.emit(
        "user_input_transcribed",
        SimpleNamespace(transcript="the user's words", is_final=True),
    )
    handle = FakeSpeechHandle()
    session.emit(
        "speech_created",
        SimpleNamespace(
            speech_handle=handle, user_initiated=False, source="generate_reply"
        ),
    )
    session.output.audio.emit("playback_started", SimpleNamespace(created_at=1.0))
    session.output.audio.emit(
        "playback_finished",
        SimpleNamespace(playback_position=2.5, interrupted=False),
    )
    handle.finish()

    await probe.aclose()
    assert active_probe() is None

    events = posted(client)
    assert [event["type"] for event in events] == [
        "conversation.started",
        "turn.started",
        "user.speech.started",
        "user.speech.ended",
        "user.transcript.final",
        "playback.started",
        "playback.stopped",
        "turn.completed",
        "conversation.ended",
    ]
    conversation_id = events[0]["conversation_id"]
    assert conversation_id == probe.conversation_id
    assert conversation_id.startswith("conv_") and len(conversation_id) == 37
    assert all(event["conversation_id"] == conversation_id for event in events)
    assert [event["seq"] for event in events] == list(range(1, len(events) + 1))
    assert all(event["created_at_ms"] > 0 for event in events)
    monos = [event["data"]["mono_ms"] for event in events]
    assert all(mono >= 0 for mono in monos) and monos == sorted(monos)

    assert "turn_id" not in events[0] and "turn_id" not in events[-1]
    assert all(event["turn_id"] == "turn_000001" for event in events[1:-1])
    assert events[0]["data"]["integration"] == "livekit-python"
    assert set(events[0]["data"]) == {"mono_ms", "integration", "integration_version"}
    assert events[1]["data"]["initiator"] == "user"
    assert events[6]["data"]["interrupted"] is False
    assert events[6]["data"]["playback_position_ms"] == 2500
    assert events[-1]["data"] == {
        "mono_ms": events[-1]["data"]["mono_ms"],
        "reason": "shutdown",
        "turn_count": 1,
    }
    # Content-free: the transcript text must never appear anywhere.
    assert "the user's words" not in repr(events)
    # A caller-owned client is not closed by the probe.
    assert client.closed is False
    assert probe.dropped_events == 0


async def test_probe_marks_interruption_and_opens_next_turn() -> None:
    probe, session, client = new_probe()
    probe.start()

    handle = FakeSpeechHandle()
    session.emit(
        "speech_created",
        SimpleNamespace(speech_handle=handle, user_initiated=True, source="say"),
    )
    session.output.audio.emit("playback_started", SimpleNamespace(created_at=1.0))
    user_state(session, "listening", "speaking")  # barge-in over agent audio
    session.output.audio.emit(
        "playback_finished",
        SimpleNamespace(playback_position=1.0, interrupted=True),
    )
    handle.finish(interrupted=True)

    await probe.aclose()
    events = posted(client)
    assert [event["type"] for event in events] == [
        "conversation.started",
        "turn.started",
        "playback.started",
        "interrupt.detected",
        "user.speech.started",
        "playback.stopped",
        "interrupt.cancel_sent",
        "turn.completed",
        "turn.started",
        "user.speech.started",
        "conversation.ended",
    ]
    assert events[1]["data"]["initiator"] == "agent"
    assert events[1]["turn_id"] == "turn_000001"
    # The barge-in markers land inside the interrupted turn.
    for index in (3, 4, 5, 6, 7):
        assert events[index]["turn_id"] == "turn_000001"
    assert events[5]["data"]["interrupted"] is True
    assert events[5]["data"]["playback_position_ms"] == 1000
    # The still-speaking user opens the next turn immediately.
    assert events[8]["turn_id"] == "turn_000002"
    assert events[8]["data"]["initiator"] == "user"
    assert events[-1]["data"]["turn_count"] == 2


async def test_probe_maps_close_reason_and_stops_after_end() -> None:
    probe, session, client = new_probe()
    probe.start()
    session.emit(
        "close",
        SimpleNamespace(reason=SimpleNamespace(value="participant_disconnected")),
    )
    # Anything observed after conversation.ended is discarded.
    user_state(session, "listening", "speaking")
    await probe.aclose()

    events = posted(client)
    assert [event["type"] for event in events] == [
        "conversation.started",
        "conversation.ended",
    ]
    assert events[-1]["data"]["reason"] == "hangup"
    assert events[-1]["data"]["turn_count"] == 0


async def test_probe_records_tool_markers_without_tool_names() -> None:
    probe, session, client = new_probe()
    probe.start()
    user_state(session, "listening", "speaking")

    session.emit(
        "tool_execution_updated",
        SimpleNamespace(
            update=SimpleNamespace(
                type="tool_call_started",
                function_call=SimpleNamespace(call_id="call-1", name="secret_tool"),
            )
        ),
    )
    session.emit(
        "tool_execution_updated",
        SimpleNamespace(
            update=SimpleNamespace(
                type="tool_call_ended", call_id="call-1", status="error", message="boom"
            )
        ),
    )
    # Unknown call ids and non-terminal updates are ignored.
    session.emit(
        "tool_execution_updated",
        SimpleNamespace(
            update=SimpleNamespace(
                type="tool_call_ended", call_id="ghost", status="done"
            )
        ),
    )
    await probe.aclose()

    events = posted(client)
    tool_events = [event for event in events if event["type"].startswith("tool.")]
    assert [event["type"] for event in tool_events] == [
        "tool.started",
        "tool.completed",
    ]
    assert tool_events[0]["data"] == {
        "mono_ms": tool_events[0]["data"]["mono_ms"],
        "tool_index": 1,
    }
    assert tool_events[1]["data"]["ok"] is False
    assert "secret_tool" not in repr(events) and "boom" not in repr(events)


async def test_registry_hooks_attach_legs_and_markers_to_open_turn() -> None:
    probe, session, client = new_probe()
    # Before start, the registry hooks are strict no-ops.
    report_leg("stt", session_id="sess-x", attempt_id="att-x")
    report_marker("llm.requested")

    probe.start()
    # No open turn yet: still no-ops.
    report_leg("stt", session_id="sess-x", attempt_id="att-x")
    report_marker("llm.requested")

    user_state(session, "listening", "speaking")
    report_leg("stt", session_id="sess-1", attempt_id="att-1", provider="deepgram")
    report_leg("tts", session_id="sess-2", attempt_id="att-2", model="sonic-2")
    report_leg("llm", request_id="req-9")
    report_leg("stt", session_id="", attempt_id="")  # incomplete: dropped
    report_leg("smtp", request_id="req-9")  # unknown kind: dropped
    report_marker("llm.requested")
    report_marker(
        "llm.completed", ok=True, verdict="content-smuggle"
    )  # extra field dropped
    report_marker("session.opened")  # outside the hook vocabulary: dropped
    await probe.aclose()

    events = posted(client)
    legs = [event for event in events if event["type"] == "leg.attached"]
    assert len(legs) == 3
    assert legs[0]["data"] == {
        "mono_ms": legs[0]["data"]["mono_ms"],
        "kind": "stt",
        "session_id": "sess-1",
        "attempt_id": "att-1",
        "provider": "deepgram",
    }
    assert legs[1]["data"]["kind"] == "tts" and legs[1]["data"]["model"] == "sonic-2"
    assert legs[2]["data"] == {
        "mono_ms": legs[2]["data"]["mono_ms"],
        "kind": "llm",
        "request_id": "req-9",
    }
    markers = [event["type"] for event in events]
    assert markers.count("llm.requested") == 1
    completed = next(event for event in events if event["type"] == "llm.completed")
    assert set(completed["data"]) == {"mono_ms", "ok"}
    assert "session.opened" not in markers


async def test_probe_batches_at_64_and_drops_beyond_bounded_queue() -> None:
    probe, session, client = new_probe()
    probe.start()
    user_state(session, "listening", "speaking")
    # Three markers are queued already (started, turn.started, speech.started).
    for _ in range(1300):
        report_marker("llm.requested")
    # 1024-event bound: 1021 of the 1300 fit; conversation.ended is also shed.
    await probe.aclose()

    events = posted(client)
    assert len(events) == 1024
    assert all(len(batch) <= 64 for batch in client.batches)
    assert probe.dropped_events == 1300 + 4 - 1024
    # Sequence numbers are only consumed by queued events, so they stay dense.
    assert [event["seq"] for event in events] == list(range(1, 1025))


async def test_probe_never_raises_into_the_agent() -> None:
    session = FakeAgentSession()
    client = FailingGatewayClient()
    probe = ConversationProbe(session, client=client)  # type: ignore[arg-type]
    probe.start()
    probe.start()  # double start is harmless

    session.emit("user_state_changed", SimpleNamespace())  # missing attributes
    session.emit("speech_created", SimpleNamespace())  # missing handle
    session.emit("tool_execution_updated", SimpleNamespace(update=None))
    session.output.audio.emit("playback_finished", SimpleNamespace())
    session.emit("close", SimpleNamespace())  # reason missing entirely
    await probe.aclose()
    await probe.aclose()  # double close is harmless

    # Every queued marker was shed on the failing socket and counted.
    assert client.batches == []
    assert probe.dropped_events > 0


class LegFakeSTTStream(FakeStream):
    session_id = "sess-stt-7"
    attempt_id = "att-stt-7"


async def test_stt_stream_attaches_leg_to_active_probe() -> None:
    probe, session, client = new_probe()
    probe.start()
    user_state(session, "listening", "speaking")

    plugin = STT(FakeClient())  # type: ignore[arg-type]
    fake_stream = LegFakeSTTStream()
    stream = plugin.stream()
    stream._bridge = FakeBridge(fake_stream)  # type: ignore[assignment]
    stream.push_frame(frame())
    stream.end_input()
    _ = [event async for event in stream]
    await plugin.aclose()
    await probe.aclose()

    legs = [event for event in posted(client) if event["type"] == "leg.attached"]
    assert len(legs) == 1
    assert legs[0]["data"]["kind"] == "stt"
    assert legs[0]["data"]["session_id"] == "sess-stt-7"
    assert legs[0]["data"]["attempt_id"] == "att-stt-7"


class LegFakeTTSStream(FakeTTSStream):
    session_id = "sess-tts-3"
    attempt_id = "att-tts-3"


async def test_tts_stream_reports_request_first_audio_and_leg() -> None:
    probe, session, client = new_probe()
    probe.start()
    user_state(session, "listening", "speaking")

    plugin = TTS(FakeClient())  # type: ignore[arg-type]
    fake_stream = LegFakeTTSStream()
    stream = plugin.stream()
    stream._bridge = FakeTTSBridge(fake_stream)  # type: ignore[assignment]
    stream.push_text("hello")
    stream.flush()
    stream.end_input()
    _ = [event async for event in stream]
    await plugin.aclose()
    await probe.aclose()

    events = posted(client)
    types = [event["type"] for event in events]
    assert types.count("tts.requested") == 1
    assert types.count("tts.first_audio") == 1
    assert types.index("tts.requested") < types.index("tts.first_audio")
    legs = [event for event in events if event["type"] == "leg.attached"]
    assert legs[0]["data"]["kind"] == "tts"
    assert legs[0]["data"]["session_id"] == "sess-tts-3"


async def test_llm_stream_reports_request_first_token_and_completion() -> None:
    probe, session, client = new_probe()
    probe.start()
    user_state(session, "listening", "speaking")

    plugin = LLM(FakeRelayClient(_relay_events()))  # type: ignore[arg-type]
    context = agents_llm.ChatContext.empty()
    context.add_message(role="user", content="hi")
    _ = [chunk async for chunk in plugin.chat(chat_ctx=context)]
    await plugin.aclose()
    await probe.aclose()

    events = posted(client)
    types = [event["type"] for event in events]
    assert types.count("llm.requested") == 1
    assert types.count("llm.first_token") == 1
    assert types.index("llm.requested") < types.index("llm.first_token")
    completed = [event for event in events if event["type"] == "llm.completed"]
    assert len(completed) == 1
    assert completed[0]["data"]["ok"] is True


async def test_llm_stream_reports_failed_completion() -> None:
    probe, session, client = new_probe()
    probe.start()
    user_state(session, "listening", "speaking")

    from livekit.agents import APIConnectionError, APIConnectOptions

    plugin = LLM(FailingRelayClient([]))  # type: ignore[arg-type]
    context = agents_llm.ChatContext.empty()
    context.add_message(role="user", content="hi")
    stream = plugin.chat(chat_ctx=context, conn_options=APIConnectOptions(max_retry=0))
    try:
        _ = [chunk async for chunk in stream]
    except APIConnectionError:
        pass
    await plugin.aclose()
    await probe.aclose()

    events = posted(client)
    completed = [event for event in events if event["type"] == "llm.completed"]
    assert len(completed) == 1
    assert completed[0]["data"]["ok"] is False


async def test_relay_reports_llm_leg_from_response_header() -> None:
    from speko_gateway.relay import _report_llm_leg

    probe, session, client = new_probe()
    probe.start()
    user_state(session, "listening", "speaking")

    _report_llm_leg(SimpleNamespace(headers={"Speko-Request-ID": "req-relay-1"}))  # type: ignore[arg-type]
    _report_llm_leg(SimpleNamespace(headers={}))  # type: ignore[arg-type]
    await probe.aclose()

    legs = [event for event in posted(client) if event["type"] == "leg.attached"]
    assert len(legs) == 1
    assert legs[0]["data"] == {
        "mono_ms": legs[0]["data"]["mono_ms"],
        "kind": "llm",
        "request_id": "req-relay-1",
    }


def test_module_registry_defaults_to_none() -> None:
    assert probe_module.active_probe() is None
