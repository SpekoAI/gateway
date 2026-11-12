from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
from aiohttp import web
from livekit import (
    agents as _agents,  # noqa: F401  (venv sanity: probe never imports it)
)
from livekit.agents import llm as agents_llm
from test_client_transports import server as gateway_server
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
from test_native_speech import collect, server

import speko_gateway.probe as probe_module
from speko_gateway.client import GatewayClient
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
    report_leg("evaluation", request_id="req-eval-1", provider="typesafe")
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
    assert len(legs) == 4
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
    assert legs[3]["data"] == {
        "mono_ms": legs[3]["data"]["mono_ms"],
        "kind": "evaluation",
        "request_id": "req-eval-1",
        "provider": "typesafe",
    }
    markers = [event["type"] for event in events]
    assert markers.count("llm.requested") == 1
    completed = next(event for event in events if event["type"] == "llm.completed")
    assert set(completed["data"]) == {"mono_ms", "ok"}
    assert "session.opened" not in markers


@pytest.mark.parametrize("request_id", ["req-tts-1", "r" * 256])
async def test_native_speech_attaches_router_identity_through_active_probe(
    request_id,
) -> None:
    probe, session, client = new_probe()
    probe.start()
    user_state(session, "listening", "speaking")

    async def audio(request, _):
        await request.read()
        return web.Response(
            body=b"audio",
            headers={"Content-Type": "audio/pcm", "Speko-Request-ID": request_id},
        )

    try:
        async with server(audio) as (speech, calls):
            assert await collect(speech) == b"audio"
    finally:
        await probe.aclose()

    legs = [event for event in posted(client) if event["type"] == "leg.attached"]
    assert len(legs) == 1 and probe.dropped_events == 0
    assert legs[0]["turn_id"] == "turn_000001"
    assert legs[0]["data"] == {
        "mono_ms": legs[0]["data"]["mono_ms"],
        "kind": "tts",
        "request_id": request_id,
    }
    assert calls == ["/v1/tts/speech"]


@pytest.mark.parametrize(
    "request_ids", [["req/invalid"], ["r" * 257], ["req-one", "req-two"]]
)
async def test_native_speech_invalid_identity_never_poisons_probe_batch(
    request_ids,
) -> None:
    probe, session, client = new_probe()
    probe.start()
    user_state(session, "listening", "speaking")

    async def audio(request, _):
        await request.read()
        return web.Response(
            body=b"audio",
            headers=[("Content-Type", "audio/pcm")]
            + [("Speko-Request-ID", value) for value in request_ids],
        )

    try:
        async with server(audio) as (speech, _):
            assert await collect(speech) == b"audio"
    finally:
        await probe.aclose()
    assert not [event for event in posted(client) if event["type"] == "leg.attached"]
    assert probe.dropped_events == 0


@pytest.mark.parametrize("accept_router_tts", [False, True])
async def test_native_speech_mixed_gateway_versions_preserve_unrelated_markers(
    accept_router_tts,
) -> None:
    batches, accepted = [], []

    async def ingress(request):
        batch = (await request.json())["events"]
        batches.append(batch)
        # The old Go vocabulary rejects the whole batch for a TTS request ID.
        if not accept_router_tts and any(
            event["type"] == "leg.attached"
            and event["data"]["kind"] == "tts"
            and "request_id" in event["data"]
            for event in batch
        ):
            return web.json_response({"error": "old vocabulary"}, status=400)
        accepted.extend(batch)
        return web.json_response({"accepted": len(batch), "dropped": 0}, status=202)

    async def audio(request, _):
        await request.read()
        return web.Response(
            body=b"audio",
            headers={"Content-Type": "audio/pcm", "Speko-Request-ID": "req-tts-1"},
        )

    app = web.Application()
    app.router.add_post("/v1/turn-events", ingress)
    async with gateway_server(app, unix=True) as socket:
        client = GatewayClient(socket_path=socket, local_auth_token="owned-local")
        session = FakeAgentSession()
        probe = ConversationProbe(session, client=client)
        probe.start()
        user_state(session, "listening", "speaking")
        user_state(session, "speaking", "listening")
        report_leg("tts", session_id="sess-gateway", attempt_id="att-gateway")
        try:
            async with server(audio) as (speech, _):
                assert await collect(speech) == b"audio"
            report_marker("tts.first_audio")
            probe._complete_turn()
        finally:
            await probe.aclose()
            await client.aclose()

    assert len(batches) == 3  # Original markers, isolated Router TTS, trailing markers.
    assert [event["seq"] for batch in batches for event in batch] == list(
        range(1, sum(map(len, batches)) + 1)
    )
    assert len(batches[1]) == 1
    assert batches[1][0]["data"]["request_id"] == "req-tts-1"
    assert [event["type"] for event in accepted if event["type"] != "leg.attached"] == [
        "conversation.started",
        "turn.started",
        "user.speech.started",
        "user.speech.ended",
        "tts.first_audio",
        "turn.completed",
        "conversation.ended",
    ]
    assert any(event["data"].get("session_id") == "sess-gateway" for event in accepted)
    router_legs = [event for event in accepted if event["data"].get("request_id")]
    assert len(router_legs) == int(accept_router_tts)
    assert probe.dropped_events == int(not accept_router_tts)


@pytest.mark.parametrize("failed_group", [2, 3])
async def test_partial_telemetry_failure_preserves_prior_acknowledgements(failed_group):
    batches, accepted = [], []

    async def ingress(request):
        batch = (await request.json())["events"]
        batches.append(batch)
        if len(batches) == failed_group:
            request.transport.abort()
            return web.Response(status=503)
        accepted.extend(batch)
        return web.json_response({"accepted": len(batch), "dropped": 0}, status=202)

    app = web.Application()
    app.router.add_post("/v1/turn-events", ingress)
    async with gateway_server(app, unix=True) as socket:
        client = GatewayClient(socket_path=socket, local_auth_token="owned-local")
        session = FakeAgentSession()
        probe = ConversationProbe(session, client=client)
        probe.start()
        user_state(session, "listening", "speaking")
        user_state(session, "speaking", "listening")
        report_leg("tts", request_id="req-tts-1")
        report_marker("tts.first_audio")
        probe._complete_turn()
        try:
            await asyncio.wait_for(probe.aclose(), 1)
        finally:
            await client.aclose()
    assert len(batches) == failed_group  # No retry or send after socket failure.
    expected_accepted = 4 if failed_group == 2 else 5
    assert len(accepted) == expected_accepted
    assert [event["seq"] for event in accepted] == list(range(1, expected_accepted + 1))
    assert probe.dropped_events == 8 - expected_accepted


async def test_turn_ingress_acknowledgement_does_not_wait_for_ignored_response_body():
    release = asyncio.Event()

    async def ingress(request):
        await request.read()
        response = web.StreamResponse(status=202, headers={"Content-Length": "4096"})
        await response.prepare(request)
        await release.wait()
        return response

    app = web.Application()
    app.router.add_post("/v1/turn-events", ingress)
    async with gateway_server(app, unix=True) as socket:
        client = GatewayClient(socket_path=socket, local_auth_token="owned-local")
        try:
            dropped = await asyncio.wait_for(
                client.post_turn_events([{"type": "turn.completed", "data": {}}]), 1
            )
            assert dropped == 0
        finally:
            release.set()
            await client.aclose()


@pytest.mark.parametrize("start_with_turn", [False, True])
async def test_native_speech_late_headers_do_not_attach_to_a_later_turn(
    start_with_turn,
) -> None:
    probe, session, client = new_probe()
    probe.start()
    if start_with_turn:
        user_state(session, "listening", "speaking")

    async def audio(request, _):
        await request.read()
        if start_with_turn:
            # Finish the first turn, then open another before headers arrive.
            probe._complete_turn()
        user_state(session, "listening", "speaking")
        return web.Response(
            body=b"audio",
            headers={"Content-Type": "audio/pcm", "Speko-Request-ID": "req-late"},
        )

    try:
        async with server(audio) as (speech, _):
            assert await collect(speech) == b"audio"
    finally:
        await probe.aclose()
    assert not [event for event in posted(client) if event["type"] == "leg.attached"]


@pytest.mark.parametrize(
    "kind,identity",
    [
        ("tts", {"request_id": "req", "session_id": "sess", "attempt_id": "att"}),
        ("tts", {"request_id": "req", "session_id": "sess"}),
        ("tts", {"request_id": "req", "attempt_id": "att"}),
        ("tts", {"session_id": "sess"}),
        ("tts", {"attempt_id": "att"}),
        ("tts", {}),
        ("stt", {"request_id": "req"}),
        ("stt", {"request_id": "req", "session_id": "sess", "attempt_id": "att"}),
        ("llm", {"request_id": "req", "session_id": "sess"}),
        ("evaluation", {"request_id": "req", "attempt_id": "att"}),
    ],
)
async def test_probe_refuses_mixed_or_partial_leg_identity(kind, identity) -> None:
    probe, session, client = new_probe()
    probe.start()
    user_state(session, "listening", "speaking")
    report_leg(kind, **identity)
    await probe.aclose()
    assert not [event for event in posted(client) if event["type"] == "leg.attached"]


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


class BlockedThenFailingClient(FakeGatewayClient):
    """First post blocks until released, then every post fails."""

    def __init__(self) -> None:
        super().__init__()
        self.release = asyncio.Event()
        self.calls = 0

    async def post_turn_events(self, events: list[dict[str, Any]]) -> None:
        self.calls += 1
        if self.calls == 1:
            await self.release.wait()
        raise OSError("socket gone")


async def test_probe_rearms_flush_when_a_post_fails_with_markers_queued() -> None:
    # Markers appended while a post is in flight can consume the flush timer
    # through a no-op _begin_flush; when that post then fails, the leftovers
    # must be re-armed and drained rather than stranded until the next append.
    session = FakeAgentSession()
    client = BlockedThenFailingClient()
    probe = ConversationProbe(session, client=client)  # type: ignore[arg-type]
    probe.start()

    probe._begin_flush()  # first batch (conversation.started) goes in flight
    user_state(session, "listening", "speaking")  # two markers queue mid-flight
    probe._begin_flush()  # the timer firing mid-flight: consumed, no-op
    assert probe._flush_timer is None

    client.release.set()  # the in-flight post now fails
    for _ in range(3):
        await asyncio.sleep(0)

    # Without the re-arm the two queued markers would sit here forever with no
    # timer and no task. With it, a fresh flush drains and counts them.
    await asyncio.sleep(0.3)
    assert probe.dropped_events == 3
    assert len(probe._pending) == 0
    await probe.aclose()


async def test_probe_close_wins_over_deferred_rearm() -> None:
    # A failed post schedules a deferred re-arm; when aclose runs first, that
    # callback must not start another flush task racing the shutdown flush,
    # and every emitted marker must be accounted for exactly once.
    session = FakeAgentSession()
    client = BlockedThenFailingClient()
    probe = ConversationProbe(session, client=client)  # type: ignore[arg-type]
    probe.start()

    probe._begin_flush()  # conversation.started goes in flight
    user_state(session, "listening", "speaking")  # two markers queue mid-flight
    client.release.set()
    await probe.aclose()

    for _ in range(3):
        await asyncio.sleep(0)
    await asyncio.sleep(0.3)
    assert probe._flush_timer is None
    assert probe._flush_task is None or probe._flush_task.done()
    assert len(probe._pending) == 0
    # started + turn.started + speech.started + conversation.ended, all failed.
    assert probe.dropped_events == 4


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


async def test_relay_reports_evaluation_leg_from_response_header() -> None:
    from speko_gateway.relay import _report_evaluation_leg

    probe, session, client = new_probe()
    probe.start()
    user_state(session, "listening", "speaking")

    _report_evaluation_leg(
        SimpleNamespace(headers={"Speko-Request-ID": "req-evaluation-1"}),
        probe,
        probe.current_turn_id,
    )  # type: ignore[arg-type]
    await probe.aclose()

    legs = [event for event in posted(client) if event["type"] == "leg.attached"]
    assert len(legs) == 1
    assert legs[0]["data"] == {
        "mono_ms": legs[0]["data"]["mono_ms"],
        "kind": "evaluation",
        "request_id": "req-evaluation-1",
    }


def test_module_registry_defaults_to_none() -> None:
    assert probe_module.active_probe() is None
