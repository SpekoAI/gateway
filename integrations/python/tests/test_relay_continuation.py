"""Stream continuation against a fake relay that speaks the STT wire.

The fake relay "recognizes" audio deterministically: every 100 ms block of
16 kHz mono PCM is one word named after its sample value, and silence (0) is
no word. A word's time is its block's position on the session timeline (the
connection's own clock plus the continued session's audio offset). So the
transcript of a stream is known in advance, whatever the drops, and a stitched
transcript can be checked word for word and millisecond for millisecond.
"""

from __future__ import annotations

import asyncio
import json
import struct
import time
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import aiohttp
import pytest
from aiohttp import web

from speko_gateway import GatewayError, RelaySpeechClient
from speko_gateway._relay_continuation import (
    REPLAY_SPEED,
    ContinuingSpeechSession,
    Limits,
    align,
)
from speko_gateway.client import SessionConfig

RATE = 16_000
BLOCK = 3_200  # 100 ms of 16-bit mono PCM
BLOCK_MS = 100


def block(value: int) -> bytes:
    return struct.pack("<h", value) * (BLOCK // 2)


@dataclass
class Dial:
    key: str
    configure: str
    connection: int


@dataclass
class FakeRelay:
    """A relay whose drops and refusals are scripted per test."""

    confirm: bool = True
    covered: bool = True
    final_every: int = 3
    # drop(connection, event, info) -> True closes the socket right there.
    drop: Callable[[int, str, int], bool] = lambda *_: False
    # refuse(dial index) -> error object sent instead of session.ready.
    refuse: Callable[[int], dict[str, Any] | None] = lambda _: None
    # covered_ms stops moving at this value (a broken chain), when set.
    freeze_covered: int | None = None
    # False: finals carry no segment times (text-only seams).
    timed: bool = True
    # False: session.ready does not promise to echo commit ids.
    commit_ids: bool = True
    # Seconds a dial waits before session.ready, by dial index.
    ready_delay: Callable[[int], float] = lambda _: 0.0
    # Per connection: ("bytes", monotonic time, total) and ("commit", total).
    received: dict[int, list[tuple]] = field(default_factory=dict)
    dials: list[Dial] = field(default_factory=list)
    started: set[str] = field(default_factory=set)
    connections: int = 0

    async def handle(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        key = request.headers.get("Idempotency-Key", "")
        message = await ws.receive()
        dial_index = len(self.dials)
        self.dials.append(Dial(key, message.data, self.connections))
        configure = json.loads(message.data)
        if key in self.started:
            await ws.send_json(
                {"type": "error", "error": {"code": "request_already_started", "retryable": False}}
            )
            await ws.close()
            return ws
        refusal = self.refuse(dial_index)
        if refusal is not None:
            await ws.send_json({"type": "error", "error": refusal})
            await ws.close()
            return ws
        connection = self.connections
        self.connections += 1
        if self.drop(connection, "configure", 0):
            self.started.add(key)
            await ws.close()
            return ws
        continuation = configure.get("continuation")
        confirmed = self.confirm and isinstance(continuation, dict)
        offset = continuation.get("audio_offset_ms", 0) if confirmed else 0
        self.started.add(key)
        ready = {
            "type": "session.ready",
            "request_id": f"req_{connection}",
            "route": {"provider": "fixture", "model": "fixture-model", "region": "r1"},
        }
        if self.commit_ids:
            ready["commit_ids"] = True
        delay = self.ready_delay(dial_index)
        if delay:
            await asyncio.sleep(delay)
        if confirmed:
            ready["continuation"] = True
        await ws.send_json(ready)
        received, buffer, pending, finals = 0, b"", [], 0

        async def final(commit_id: str | None = None) -> None:
            nonlocal pending, finals
            words = [word for word, _ in pending]
            payload: dict[str, Any] = {"type": "transcript.final", "text": " ".join(words)}
            if pending and self.timed:
                payload["segments"] = [
                    {"text": word, "start_ms": start, "end_ms": start + BLOCK_MS}
                    for word, start in pending
                ]
            if commit_id is not None and self.commit_ids:
                payload["commit_id"] = commit_id
            if confirmed and self.covered:
                payload["covered_ms"] = offset + (received // BLOCK) * BLOCK_MS
                if self.freeze_covered is not None:
                    payload["covered_ms"] = self.freeze_covered
            pending = []
            await ws.send_json(payload)
            finals += 1

        record = self.received.setdefault(connection, [])
        async for message in ws:
            if message.type is aiohttp.WSMsgType.BINARY:
                buffer += message.data
                received += len(message.data)
                record.append(("bytes", time.monotonic(), received))
                if self.drop(connection, "bytes", received):
                    await ws.close()
                    return ws
                while len(buffer) >= BLOCK:
                    value = struct.unpack("<h", buffer[:2])[0]
                    start = offset + (received - len(buffer)) // BLOCK * BLOCK_MS
                    buffer = buffer[BLOCK:]
                    if value:
                        pending.append((f"w{value}", start))
                        await ws.send_json({"type": "transcript.delta", "text": f"w{value}"})
                    if len(pending) >= self.final_every:
                        await final()
                        if self.drop(connection, "final", finals):
                            await ws.close()
                            return ws
                continue
            control = json.loads(message.data)
            if control["type"] == "input.commit":
                record.append(("commit", received))
                if self.drop(connection, "commit", 0):
                    await ws.close()
                    return ws
                await final(control.get("commit_id"))
            elif control["type"] == "session.close":
                if pending:
                    await final()
                await ws.send_json({"type": "session.closed", "usage": {"duration_ms": received // 32}})
                await ws.close()
        return ws


@asynccontextmanager
async def relay(fake: FakeRelay):
    app = web.Application()
    app.router.add_get("/v1/{kind}/stream", fake.handle)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    try:
        await web.TCPSite(runner, "127.0.0.1", 0).start()
        yield f"http://127.0.0.1:{runner.addresses[0][1]}"
    finally:
        await runner.cleanup()


def config() -> SessionConfig:
    return SessionConfig(
        kind="stt",
        execution={
            "provider_route": "speko_relay",
            "credential_source": "managed",
            "relay_policy": "required",
        },
        request={"provider": "fixture", "model": "fixture-model", "language": "en"},
        media={"encoding": "pcm_s16le", "sample_rate_hz": RATE, "channels": 1},
    )


FAST = Limits(retry_delay=0.01, replay_speed=50)


async def run(
    fake: FakeRelay,
    values: list[int],
    *,
    chunk: int = BLOCK // 2,
    commits: tuple[int, ...] = (),
    limits: Limits = FAST,
    pace: float = 0.002,
) -> tuple[list[Any], list[str], ContinuingSpeechSession]:
    """Stream the blocks, commit after the given block indexes, finish, and
    return every event and every commit id."""
    audio = b"".join(block(value) for value in values)
    commit_ids: list[str] = []
    events: list[Any] = []
    async with relay(fake) as origin:
        client = RelaySpeechClient(
            api_key="sk_speko_agent_fixture",
            session_id="00000000-0000-4000-8000-000000000000",
            base_url=origin,
        )
        try:
            session = await client.open(config(), auto_reconnect=True)
            assert isinstance(session, ContinuingSpeechSession)
            session._limits = limits

            async def read() -> None:
                async for event in session.events():
                    events.append(event)

            reader = asyncio.create_task(read())
            try:
                for offset in range(0, len(audio), chunk):
                    await session.send_audio(audio[offset : offset + chunk])
                    end = offset + chunk
                    if end % BLOCK == 0 and end // BLOCK - 1 in commits:
                        commit_ids.append(await session.commit_audio())
                    await asyncio.sleep(pace)
                await session.finish()
            except GatewayError:
                # A closed session refuses input; the reader holds the cause.
                pass
            await asyncio.wait_for(reader, 10)
        finally:
            await client.aclose()
    return events, commit_ids, session


def finals(events: list[Any]) -> list[Any]:
    return [event for event in events if event.type == "transcript.final"]


def words(events: list[Any]) -> list[str]:
    return [word for event in finals(events) for word in event.data["text"].split()]


def expected(values: list[int]) -> list[str]:
    return [f"w{value}" for value in values if value]


def assert_timeline(events: list[Any], values: list[int]) -> None:
    """Every timed word sits at its own block's place on one timeline."""
    place = {f"w{value}": index * BLOCK_MS for index, value in enumerate(values) if value}
    for event in finals(events):
        for segment in event.data.get("segments", []):
            assert segment["start_ms"] == place[segment["text"]], segment


def assert_commits_once(events: list[Any], commit_ids: list[str]) -> None:
    answered = [event.data["commit_id"] for event in finals(events) if "commit_id" in event.data]
    assert sorted(answered) == sorted(commit_ids)


VALUES = list(range(1, 41))  # 4 s, 40 distinct words


async def test_no_drop_baseline():
    events, commits, _ = await run(FakeRelay(), VALUES, commits=(9, 19))
    assert words(events) == expected(VALUES)
    assert_timeline(events, VALUES)
    assert_commits_once(events, commits)
    assert not [event for event in events if event.type.startswith("session.") and event.type != "session.closed"]


@pytest.mark.parametrize(
    "name, drop",
    [
        ("mid word", lambda conn, event, info: conn == 0 and event == "bytes" and info == 11 * BLOCK + BLOCK // 2),
        ("right after a final", lambda conn, event, info: conn == 0 and event == "final" and info == 2),
        ("during replay", lambda conn, event, info: (conn == 0 and event == "bytes" and info == 20 * BLOCK) or (conn == 1 and event == "bytes" and info == BLOCK + BLOCK // 2)),
        ("during a pending commit", lambda conn, event, info: conn == 0 and event == "commit"),
        ("during dial", lambda conn, event, info: (conn == 0 and event == "bytes" and info == 15 * BLOCK) or (conn == 1 and event == "configure")),
    ],
)
async def test_drop_is_stitched_exactly(name, drop):
    fake = FakeRelay(drop=drop)
    events, commits, _ = await run(fake, VALUES, commits=(9, 29))
    assert words(events) == expected(VALUES), name
    assert_timeline(events, VALUES)
    assert_commits_once(events, commits)
    continued = [event for event in events if event.type == "session.continued"]
    assert continued and all(event.data["exact"] for event in continued)
    assert not [event for event in events if event.type == "session.gap"]
    # Every continuing session starts where the previous one was finalized.
    for dial in fake.dials[1:]:
        continuation = json.loads(dial.configure)["continuation"]
        assert continuation["audio_offset_ms"] % BLOCK_MS == 0
        assert continuation["of"].startswith("req_")


async def test_unknown_dial_retries_with_the_same_key_and_bytes():
    fake = FakeRelay(drop=lambda conn, event, info: (conn == 0 and event == "bytes" and info == 15 * BLOCK) or (conn == 1 and event == "configure"))
    await run(fake, VALUES)
    assert fake.dials[1].key == fake.dials[2].key
    assert fake.dials[1].configure == fake.dials[2].configure


async def test_request_already_started_takes_a_new_key():
    started = {}

    def drop(conn, event, info):
        return conn == 0 and event == "bytes" and info == 15 * BLOCK

    fake = FakeRelay(drop=drop)
    original = fake.handle

    async def handle(request):
        # The first continuation dial "succeeds" server side but its answer
        # is lost, so its retry with the same key finds the request started.
        if len(fake.dials) == 1 and not started:
            started["key"] = request.headers["Idempotency-Key"]
            fake.started.add(started["key"])
        return await original(request)

    fake.handle = handle
    events, _, _ = await run(fake, VALUES)
    assert words(events) == expected(VALUES)
    assert fake.dials[1].key == started["key"]
    assert fake.dials[2].key != started["key"]


async def test_request_in_progress_waits_and_keeps_the_key():
    fake = FakeRelay(
        drop=lambda conn, event, info: conn == 0 and event == "bytes" and info == 15 * BLOCK,
        refuse=lambda index: {"code": "request_in_progress", "retryable": True} if index in (1, 2, 3) else None,
    )
    events, _, _ = await run(fake, VALUES)
    assert words(events) == expected(VALUES)
    assert len({dial.key for dial in fake.dials[1:]}) == 1
    assert len(fake.dials) == 5  # in-progress waits spend no attempt


async def test_concurrency_exhausted_waits_inside_the_outage_budget():
    fake = FakeRelay(
        drop=lambda conn, event, info: conn == 0 and event == "bytes" and info == 15 * BLOCK,
        refuse=lambda index: {"code": "concurrency_exhausted", "retryable": True} if 1 <= index <= 4 else None,
    )
    events, _, _ = await run(fake, VALUES)
    assert words(events) == expected(VALUES)
    assert len(fake.dials) == 6

    fake = FakeRelay(
        drop=lambda conn, event, info: conn == 0 and event == "bytes" and info == 15 * BLOCK,
        refuse=lambda index: {"code": "concurrency_exhausted", "retryable": True} if index >= 1 else None,
    )
    with pytest.raises(GatewayError) as failure:
        await run(fake, VALUES, limits=Limits(retry_delay=0.05, outage_seconds=0.3, replay_speed=50))
    assert failure.value.code == "outage_timeout"
    assert "concurrency_exhausted" in str(failure.value)


@pytest.mark.parametrize("code", ["invalid_request", "authentication_failed", "insufficient_credit", "budget_exhausted"])
async def test_never_reconnects_on_a_final_refusal(code):
    fake = FakeRelay(
        drop=lambda conn, event, info: conn == 0 and event == "bytes" and info == 15 * BLOCK,
        refuse=lambda index: {"code": code, "retryable": True} if index >= 1 else None,
    )
    with pytest.raises(GatewayError) as failure:
        await run(fake, VALUES)
    assert failure.value.code == code
    assert len(fake.dials) == 2


async def test_mid_stream_errors_reconnect_only_when_retryable():
    class ErrorRelay(FakeRelay):
        pass

    for code, retryable, reconnects in [
        ("provider_unavailable", True, True),
        ("provider_unavailable", False, False),
        ("budget_exhausted", True, False),
    ]:
        fake = ErrorRelay()
        original = fake.handle

        async def handle(request, fake=fake, original=original, code=code, retryable=retryable):
            if fake.connections == 0:
                ws = web.WebSocketResponse()
                await ws.prepare(request)
                await ws.receive()
                fake.dials.append(Dial(request.headers["Idempotency-Key"], "", 0))
                fake.connections += 1
                await ws.send_json({"type": "session.ready", "request_id": "req_0", "commit_ids": True, "continuation": True, "route": {"provider": "fixture", "model": "fixture-model", "region": "r1"}})
                await ws.receive()
                await ws.send_json({"type": "error", "error": {"code": code, "retryable": retryable}})
                await ws.close()
                return ws
            return await original(request)

        fake.handle = handle
        if reconnects:
            events, _, _ = await run(fake, VALUES)
            assert words(events) == expected(VALUES)
        else:
            with pytest.raises(GatewayError):
                await run(fake, VALUES)
            assert len(fake.dials) == 1


async def test_three_attempts_per_outage():
    fake = FakeRelay(drop=lambda conn, event, info: (conn == 0 and event == "bytes" and info == 15 * BLOCK) or (conn >= 1 and event == "configure"))
    with pytest.raises(GatewayError):
        await run(fake, VALUES)
    assert len(fake.dials) == 1 + 3


async def test_attempts_reset_after_a_final_on_new_audio():
    # Each continuing session works long enough to finalize new audio, then
    # drops: five outages in a row, each recovered on its first attempt.
    def drop(conn, event, info):
        return 0 <= conn < 5 and event == "final" and info == 2

    fake = FakeRelay(drop=drop)
    events, _, _ = await run(fake, VALUES)
    assert words(events) == expected(VALUES)
    assert len([event for event in events if event.type == "session.continued"]) == 5


async def test_no_reconnect_while_there_is_no_audio_to_send():
    fake = FakeRelay(drop=lambda conn, event, info: conn == 0 and event == "final" and info == 1)
    async with relay(fake) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id="00000000-0000-4000-8000-000000000000", base_url=origin)
        try:
            session = await client.open(config(), auto_reconnect=True)
            session._limits = FAST
            events = []

            async def read():
                async for event in session.events():
                    events.append(event)

            reader = asyncio.create_task(read())
            for value in (1, 2, 3):
                await session.send_audio(block(value))
            await asyncio.sleep(0.3)
            # The drop came right after a final that covered every byte sent.
            assert [event.type for event in events][-1] == "session.reconnecting"
            assert len(fake.dials) == 1
            await session.send_audio(block(4))
            await asyncio.sleep(0.3)
            assert len(fake.dials) == 2
            await session.finish()
            await asyncio.wait_for(reader, 5)
            assert words(events) == ["w1", "w2", "w3", "w4"]
        finally:
            await client.aclose()


async def test_overlap_seam_without_covered_ms():
    fake = FakeRelay(covered=False, drop=lambda conn, event, info: conn == 0 and event == "bytes" and info == 23 * BLOCK)
    events, commits, _ = await run(fake, VALUES, chunk=BLOCK, commits=(9, 29))
    assert words(events) == expected(VALUES)
    assert_timeline(events, VALUES)
    assert_commits_once(events, commits)
    continued = [event for event in events if event.type == "session.continued"]
    assert continued and continued[0].data["exact"] is False
    assert not [event for event in events if event.type == "session.gap"]


async def test_unclear_overlap_reports_a_gap_instead_of_guessing():
    # Every word sounds the same, so no place in the old tail fits best.
    values = [7] * 40
    fake = FakeRelay(covered=False, timed=False, drop=lambda conn, event, info: conn == 0 and event == "bytes" and info == 23 * BLOCK)
    events, _, _ = await run(fake, values, chunk=BLOCK)
    assert [event for event in events if event.type == "session.gap"]


async def test_commit_whose_audio_fell_out_of_the_log_fails_once():
    # A 1 s log, no final before the drop, and the drop takes the commit with
    # it: the start of the commit's audio has fallen out of the log.
    fake = FakeRelay(final_every=100, drop=lambda conn, event, info: conn == 0 and event == "commit")
    events, commits, _ = await run(fake, VALUES, commits=(14,), limits=Limits(retry_delay=0.01, log_cap_ms=1_000, replay_speed=50))
    failed = [event for event in finals(events) if "commit_failed" in event.data]
    assert [event.data["commit_id"] for event in failed] == commits
    assert failed[0].data["commit_failed"] == "audio_fell_out_of_log"
    assert_commits_once(events, commits)
    gaps = [event for event in events if event.type == "session.gap"]
    assert gaps and gaps[0].data["from_ms"] == 0 and gaps[0].data["to_ms"] > 0


async def test_unconfirmed_continuation_restarts_without_pretending():
    fake = FakeRelay(confirm=False, drop=lambda conn, event, info: conn == 0 and event == "bytes" and info == 15 * BLOCK)
    events, _, _ = await run(fake, VALUES)
    assert not [event for event in events if event.type == "session.continued"]
    restarted = [event for event in events if event.type == "session.restarted"]
    assert restarted
    # The server never confirmed it, so the client asked for continuation no
    # further: the reconnect is a plain new session.
    assert "continuation" not in json.loads(fake.dials[1].configure)


async def test_old_clients_send_the_same_configure():
    fake = FakeRelay()
    async with relay(fake) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id="00000000-0000-4000-8000-000000000000", base_url=origin)
        try:
            session = await client.open(config())
            assert not isinstance(session, ContinuingSpeechSession)
            await session.finish()
            async for _ in session.events():
                pass
        finally:
            await client.aclose()
    assert "continuation" not in json.loads(fake.dials[0].configure)


async def test_integrations_pass_the_flag_through():
    from speko_gateway._livekit_bridge import LiveKitSTTBridge

    fake = FakeRelay()
    async with relay(fake) as origin:
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id="00000000-0000-4000-8000-000000000000", base_url=origin)
        try:
            bridge = LiveKitSTTBridge(client, provider="fixture", model="fixture-model", credential_source="managed", auto_reconnect=True)
            stream = await bridge.start(type("Frame", (), {"sample_rate": RATE, "num_channels": 1})())
            assert isinstance(stream._session, ContinuingSpeechSession)
            await stream.aclose()

            pipecat = pytest.importorskip("speko_gateway.pipecat")
            service = pipecat.SpekoSTTService(client, provider="fixture", model="fixture-model", credential_source="managed", sample_rate=RATE, auto_reconnect=True)
            service._sample_rate = RATE
            session = await service._open_session(provider="fixture", model="fixture-model")
            assert isinstance(session, ContinuingSpeechSession)
            await session.aclose()
        finally:
            await client.aclose()
    assert [json.loads(dial.configure)["continuation"] for dial in fake.dials] == [{}, {}]
    with pytest.raises(ValueError):
        LiveKitSTTBridge(object(), auto_reconnect=True)


async def test_auto_reconnect_needs_pcm_stt():
    client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id="00000000-0000-4000-8000-000000000000", base_url="http://127.0.0.1:1")
    opus = config()
    opus.media["encoding"] = "opus"
    try:
        with pytest.raises(ValueError):
            await client.open(opus, auto_reconnect=True)
    finally:
        await client.aclose()


def test_align_is_unique_or_nothing():
    tail = ["a", "b", "c", "d", "e"]
    assert align(tail, ["d", "e", "f"]) == 3
    assert align(tail, ["b", "c"]) == 1
    assert align(tail, ["x", "y"]) is None
    assert align(tail, ["e", "f"]) is None  # one word is too little
    assert align(["a", "b", "a", "b"], ["a", "b"]) is None  # two places fit


async def test_silence_overflow_is_dropped_without_a_gap():
    # Three words, then 3 s of silence against a 1 s log, a drop at the end
    # of the silence, then three more words. No delta came after the last
    # final, so the dropped audio is silence: no gap, and covered_ms keeps it
    # exact.
    values = [1, 2, 3] + [0] * 30 + [4, 5, 6]
    limits = Limits(retry_delay=0.01, log_cap_ms=1_000, replay_speed=50)
    for covered in (True, False):
        fake = FakeRelay(covered=covered, drop=lambda conn, event, info: conn == 0 and event == "bytes" and info == 33 * BLOCK)
        events, _, _ = await run(fake, values, chunk=BLOCK, limits=limits)
        assert words(events) == expected(values)
        assert not [event for event in events if event.type == "session.gap"]
        continued = [event for event in events if event.type == "session.continued"]
        assert continued and continued[0].data["exact"] is covered


async def test_overflow_after_speech_is_a_gap():
    # Words heard (deltas) but not finalized, against a 1 s log: the dropped
    # audio held speech, so its loss is reported.
    values = list(range(1, 31))
    fake = FakeRelay(final_every=100, drop=lambda conn, event, info: conn == 0 and event == "bytes" and info == 25 * BLOCK)
    events, _, _ = await run(fake, values, chunk=BLOCK, limits=Limits(retry_delay=0.01, log_cap_ms=1_000, replay_speed=50))
    gaps = [event for event in events if event.type == "session.gap"]
    assert gaps and gaps[0].data["from_ms"] == 0 and gaps[0].data["to_ms"] > 0


async def test_frozen_covered_ms_falls_back_to_overlap():
    # covered_ms stops at 0 while finals keep coming; a drop 35 s in must not
    # find the log pinned at 0 (overflowed, a gap) but take the overlap seam.
    values = list(range(1, 401))
    fake = FakeRelay(freeze_covered=0, drop=lambda conn, event, info: conn == 0 and event == "bytes" and info == 350 * BLOCK)
    events, _, _ = await run(fake, values, chunk=BLOCK, pace=0.001)
    assert words(events) == expected(values)
    assert not [event for event in events if event.type == "session.gap"]
    continued = [event for event in events if event.type == "session.continued"]
    assert continued and continued[0].data["exact"] is False
    assert_timeline(events, values)


async def test_backlog_is_paced_and_commits_stay_in_place():
    # The drop takes the commit after block 9; blocks 10-19 queue during the
    # outage. The new session gets all 20 blocks at no more than 2x
    # realtime, with the commit right after block 9.
    values = list(range(1, 21))
    fake = FakeRelay(final_every=100, drop=lambda conn, event, info: conn == 0 and event == "commit")
    events, commits, _ = await run(fake, values, chunk=BLOCK, commits=(9,), limits=Limits(retry_delay=0.01))
    assert words(events) == expected(values)
    assert_commits_once(events, commits)
    record = fake.received[1]
    assert ("commit", 10 * BLOCK) in record
    times = [(moment, total) for kind, moment, *rest in record if kind == "bytes" for total in rest]
    first = times[0][0]
    rate = RATE * 2 * REPLAY_SPEED
    for moment, total in times:
        assert total <= rate * (moment - first) + 2 * BLOCK, (moment - first, total)
    assert times[-1][0] - first >= (20 * BLOCK - 2 * BLOCK) / rate


# Unit checks on one session over a stub socket -------------------------------


class StubSocket:
    """Records what the session writes; replays scripted incoming frames."""

    def __init__(self, incoming: list[dict[str, Any]] | None = None, fail: bool = False) -> None:
        self.incoming = list(incoming or [])
        self.fail = fail
        self.sent: list[Any] = []
        self.closed = False

    async def send_bytes(self, data: bytes) -> None:
        if self.fail or self.closed:
            raise ConnectionResetError("closed")
        self.sent.append(data)

    async def send_json(self, value: Any) -> None:
        if self.fail or self.closed:
            raise ConnectionResetError("closed")
        self.sent.append(value)

    async def close(self) -> None:
        self.closed = True

    def __aiter__(self):
        return self._messages()

    async def _messages(self):
        for value in self.incoming:
            yield aiohttp.WSMessage(aiohttp.WSMsgType.TEXT, json.dumps(value), None)
        yield aiohttp.WSMessage(aiohttp.WSMsgType.CLOSE, None, None)


READY = {"type": "session.ready", "request_id": "req_0", "route": {"provider": "fixture"}, "continuation": True, "commit_ids": True}


def stub_session(socket: StubSocket, dial=None, sample_rate: int = RATE, **limits: Any) -> ContinuingSpeechSession:
    async def no_dial(configure, key, cancelled):
        raise AssertionError("no dial expected")

    return ContinuingSpeechSession(
        dial=dial or no_dial,
        configure={"type": "session.configure"},
        websocket=socket,
        ready=READY,
        metadata={"request_id": "req_0", "route": {"provider": "fixture"}},
        sample_rate=sample_rate,
        channels=1,
        limits=Limits(retry_delay=0.01, replay_speed=50, **limits),
    )


def ms(value: int) -> int:
    return value * RATE * 2 // 1000


async def test_failed_sends_are_not_delivered_and_never_dropped():
    session = stub_session(StubSocket(fail=True), log_cap_ms=1_000)
    for _ in range(30):
        await session.send_audio(block(1))
    # Nothing reached a provider: nothing is delivered, nothing is silence.
    assert session._delivered == 0
    assert session._log_start() == 0
    session._on_final({"text": "", "covered_ms": 2_000})
    assert session._trim == 0


async def test_queued_audio_does_not_make_covered_ms_stale():
    # 2 s delivered, then 10 s queued while the socket is down. A final
    # covering 1 s is fresh against what was delivered (not what was queued),
    # so it stays the exact replay point.
    socket = StubSocket()
    session = stub_session(socket, log_cap_ms=1_000)
    for _ in range(20):
        await session.send_audio(block(1))
    session._heard = True  # speech: the cap may not treat it as silence
    session._live, session._recovering = False, True
    for _ in range(100):
        await session.send_audio(block(2))
    assert session._delivered == ms(2_000)
    session._on_final({"text": "w1", "covered_ms": 1_000})
    assert session._trim == ms(1_000) and session._exact
    # The cap never cuts past what was delivered.
    assert session._log_start() <= session._delivered


def seam_session(old: list[tuple[str, int | None]], start_ms: int) -> ContinuingSpeechSession:
    from speko_gateway._relay_continuation import _Final

    session = stub_session(StubSocket())
    for text, end_ms in old:
        # Timed finals get one 100 ms segment per word, ending at end_ms.
        words = text.split()
        segments = ()
        if end_ms is not None:
            segments = tuple(
                ((word,), end_ms - 100 * (len(words) - index), end_ms - 100 * (len(words) - index - 1))
                for index, word in enumerate(words)
            )
        session._history.append(_Final(words, end_ms, ms(start_ms + 5_000), segments))
    return session


async def resume(session: ContinuingSpeechSession, start_ms: int, ready: dict[str, Any] | None = None) -> tuple[list[Any], StubSocket]:
    socket = StubSocket()
    events = await session._resume(socket, ready or READY, asked=True, start=ms(start_ms))
    return events, socket


def seg(text: str, start: int, end: int) -> dict[str, Any]:
    return {"text": text, "start_ms": start, "end_ms": end}


async def test_seam_drops_repeats_by_time_and_keeps_new_repeats_of_old_words():
    session = seam_session([("we said yes", 1_000)], 500)
    from speko_gateway._relay_continuation import _Audio

    session._log = [_Audio(ms(500), block(1) * 30)]
    session._appended = session._log[0].end
    await resume(session, 500)
    # "yes yes please" after a pause: it all starts after the last old word
    # ended, so none of it is a repeat, although "yes" matches the old text.
    final = {"text": "yes yes please", "segments": [seg("yes", 3_000, 3_100), seg("yes", 3_200, 3_300), seg("please", 3_400, 3_500)]}
    events = session._on_final(final)
    assert [event.type for event in events] == ["transcript.final"]
    assert events[0].data["text"] == "yes yes please"
    await session.aclose()


@pytest.mark.parametrize("old, new, new_end", [("about 1.5", "15", 1_000), ("about twenty one", "21", 990)])
async def test_a_segment_inside_delivered_audio_is_a_repeat(old, new, new_end):
    from speko_gateway._relay_continuation import _Audio

    session = seam_session([(old, 1_000)], 500)
    session._log = [_Audio(ms(500), block(1) * 30)]
    session._appended = session._log[0].end
    await resume(session, 500)
    final = {"text": f"{new} apples", "segments": [seg(new, 800, new_end), seg("apples", 1_100, 1_200)]}
    events = session._on_final(final)
    # Live finals carry segment times only: a segment that lies inside the
    # delivered audio is a repeat, however its words are spelled.
    assert [event.type for event in events] == ["transcript.final"]
    assert events[0].data["text"] == "apples"
    await session.aclose()


@pytest.mark.parametrize("old, new", [("about 1.5", "15 apples"), ("about twenty one", "21 apples")])
async def test_untimed_seam_does_not_guess_on_numbers(old, new):
    from speko_gateway._relay_continuation import _Audio

    session = seam_session([(old, None)], 500)
    session._log = [_Audio(ms(500), block(1) * 30)]
    session._appended = session._log[0].end
    await resume(session, 500)
    events = session._on_final({"text": new})
    # Neither a false match ("1.5" is not "15") nor a silent duplicate.
    assert [event.type for event in events] == ["session.gap", "transcript.final"]
    assert events[1].data["text"] == new
    await session.aclose()


async def test_seam_only_looks_at_old_words_inside_the_replay():
    from speko_gateway._relay_continuation import _Audio

    session = seam_session([("yes yes", 400)], 500)
    session._log = [_Audio(ms(500), block(1) * 30)]
    session._appended = session._log[0].end
    await resume(session, 500)
    assert session._seam is None
    events = session._on_final({"text": "yes yes please"})
    assert events[0].data["text"] == "yes yes please"
    await session.aclose()


async def test_cancel_during_dial_adopts_no_socket():
    adopted = StubSocket()
    dialing = asyncio.Event()
    release = asyncio.Event()

    async def dial(configure, key, cancelled):
        dialing.set()
        await release.wait()
        return adopted, {**READY, "request_id": "req_1"}

    session = stub_session(StubSocket(), dial=dial)
    await session.send_audio(block(1))
    collected = []

    async def read():
        async for event in session.events():
            collected.append(event)

    reader = asyncio.create_task(read())
    await asyncio.wait_for(dialing.wait(), 2)
    await session.cancel()
    release.set()
    await asyncio.wait_for(reader, 2)
    assert adopted.closed and adopted.sent == []
    assert [event.type for event in collected] == ["session.reconnecting"]
    with pytest.raises(GatewayError):
        await session.send_audio(block(2))


async def test_commit_ids_reach_the_app_once():
    session = stub_session(StubSocket())
    await session.send_audio(block(1))
    commit_id = await session.commit_audio()
    first = session._on_final({"text": "w1", "commit_id": commit_id})
    again = session._on_final({"text": "", "commit_id": commit_id})
    assert first[-1].data["commit_id"] == commit_id
    assert "commit_id" not in again[-1].data


async def test_session_closed_resolves_pending_commits():
    socket = StubSocket([{"type": "session.closed", "usage": {"duration_ms": 100}}])
    session = stub_session(socket)
    await session.send_audio(block(1))
    commit_id = await session.commit_audio()
    events = [event async for event in session.events()]
    failed = [event for event in finals(events) if event.data.get("commit_failed")]
    assert [(event.data["commit_id"], event.data["commit_failed"]) for event in failed] == [(commit_id, "session_closed")]
    assert events[-1].type == "session.closed"


async def test_any_dial_failure_resolves_pending_commits():
    async def dial(configure, key, cancelled):
        raise RuntimeError("unexpected")

    session = stub_session(StubSocket(), dial=dial)
    await session.send_audio(block(1))
    commit_id = await session.commit_audio()
    events = []
    with pytest.raises(RuntimeError):
        async for event in session.events():
            events.append(event)
    assert [event.data["commit_id"] for event in finals(events)] == [commit_id]
    assert finals(events)[0].data["commit_failed"] == "stream_failed"


async def test_overflow_fails_only_commits_whose_own_audio_fell_out():
    from speko_gateway._relay_continuation import _Audio, _Commit

    session = stub_session(StubSocket())
    early = _Commit(ms(1_000), "speko-c1", 0)
    late = _Commit(ms(3_000), "speko-c2", ms(2_500))
    session._commit_seq = 2
    session._pending = {"speko-c1": early, "speko-c2": late}
    session._log = [early, _Audio(ms(2_000), block(1) * 20), late]
    session._appended = ms(4_000)
    events, _ = await resume(session, 2_000)
    failed = [event.data["commit_id"] for event in finals(events)]
    assert failed == ["speko-c1"]
    assert "speko-c2" in session._pending
    gap = next(event for event in events if event.type == "session.gap")
    assert gap.data == {"from_ms": 0, "to_ms": 2_000}
    # The gap comes before the failed commit it explains.
    assert events.index(gap) < events.index(finals(events)[0])
    await session.aclose()


async def test_offset_is_the_replay_start_after_overflow():
    values = list(range(1, 31))
    fake = FakeRelay(final_every=100, drop=lambda conn, event, info: conn == 0 and event == "bytes" and info == 25 * BLOCK)
    events, _, _ = await run(fake, values, chunk=BLOCK, limits=Limits(retry_delay=0.01, log_cap_ms=1_000, replay_speed=50))
    gap = next(event for event in events if event.type == "session.gap")
    offset = json.loads(fake.dials[1].configure)["continuation"]["audio_offset_ms"]
    assert offset == gap.data["to_ms"] > 0
    # Words after the gap sit at their own place on the one timeline.
    assert_timeline(events, values)
    assert words(events)[-1] == "w30"


async def test_repeated_drops_share_one_outage_budget():
    # Every new session drops after one replayed block, before any final on
    # new audio: that is one outage, so three attempts in all.
    fake = FakeRelay(drop=lambda conn, event, info: (conn == 0 and event == "bytes" and info == 15 * BLOCK) or (conn >= 1 and event == "bytes" and info >= BLOCK))
    with pytest.raises(GatewayError):
        await run(fake, VALUES)
    assert len(fake.dials) == 1 + 3


async def test_a_dial_is_bounded_by_the_remaining_budget():
    dials = []

    async def dial(configure, key, cancelled):
        dials.append(key)
        await asyncio.sleep(5)
        raise AssertionError("not reached")

    session = stub_session(StubSocket(), dial=dial, outage_seconds=0.3)
    await session.send_audio(block(1))
    started = time.monotonic()
    with pytest.raises(GatewayError) as failure:
        async for _ in session.events():
            pass
    assert time.monotonic() - started < 1
    # The budget ran out on a hung dial: an outage timeout, as when it runs
    # out between dials.
    assert failure.value.code == "outage_timeout"
    # The timed-out dial may have started a session: its retry reuses the key.
    assert len(set(dials)) == 1


async def test_unanswered_commits_and_undelivered_audio_are_bounded():
    session = stub_session(StubSocket(), max_pending_commits=2)
    await session.commit_audio()
    await session.commit_audio()
    with pytest.raises(GatewayError):
        await session.commit_audio()

    session = stub_session(StubSocket(), log_cap_ms=500, outage_seconds=0.5)
    session._live, session._recovering = False, True
    with pytest.raises(GatewayError):
        for _ in range(20):
            await session.send_audio(block(1))
    assert len(session._log) <= 11


async def test_replay_starts_before_continued_and_goes_in_small_frames():
    from speko_gateway._relay_continuation import _Audio

    session = stub_session(StubSocket())
    session._log = [_Audio(0, block(1) * 50)]
    session._appended = session._log[0].end
    events, socket = await resume(session, 0)
    # The first frame is out by the time session.continued is reported.
    assert events[-1].type == "session.continued"
    assert len(socket.sent) == 1 and len(socket.sent[0]) == BLOCK
    await asyncio.sleep(0.3)
    assert all(len(frame) <= BLOCK for frame in socket.sent if isinstance(frame, bytes))
    assert sum(len(frame) for frame in socket.sent if isinstance(frame, bytes)) == 50 * BLOCK
    await session.aclose()


async def test_without_commit_ids_flush_keeps_its_place():
    values = list(range(1, 21))
    fake = FakeRelay(commit_ids=False, final_every=100, drop=lambda conn, event, info: conn == 0 and event == "commit")
    events, commits, _ = await run(fake, values, chunk=BLOCK, commits=(9,), limits=Limits(retry_delay=0.01))
    assert commits == [None]
    assert words(events) == expected(values)
    assert ("commit", 10 * BLOCK) in fake.received[1]


async def test_ids_the_new_session_will_not_echo_are_not_sent():
    session = stub_session(StubSocket())
    session._live = False
    await session.send_audio(block(1))
    commit_id = await session.commit_audio()
    events, socket = await resume(session, 0, {**READY, "request_id": "req_1", "commit_ids": False})
    assert [(event.data["commit_id"], event.data["commit_failed"]) for event in finals(events)] == [(commit_id, "commit_ids_not_echoed")]
    await asyncio.sleep(0.2)
    assert {"type": "input.commit"} in socket.sent
    assert not [frame for frame in socket.sent if isinstance(frame, dict) and "commit_id" in frame]
    await session.aclose()



class HangingSocket(StubSocket):
    """A socket whose writes never complete; reads end once it is closed."""

    def __init__(self) -> None:
        super().__init__()
        self.gone = asyncio.Event()

    async def send_bytes(self, data: bytes) -> None:
        await asyncio.Event().wait()

    async def send_json(self, value: Any) -> None:
        await asyncio.Event().wait()

    async def close(self) -> None:
        self.closed = True
        self.gone.set()

    def __aiter__(self):
        return self

    async def __anext__(self):
        await self.gone.wait()
        raise StopAsyncIteration


async def test_a_hung_replay_write_is_a_drop():
    hung = HangingSocket()

    async def dial(configure, key, cancelled):
        return hung, {**READY, "request_id": "req_1"}

    session = stub_session(StubSocket(), dial=dial, outage_seconds=0.5, write_timeout=0.1)
    await session.send_audio(block(1))
    events = []

    async def read():
        async for event in session.events():
            events.append(event)

    started = time.monotonic()
    reader = asyncio.create_task(read())
    await asyncio.sleep(0.1)
    # The first replay write hangs under the lock; commit and finish (what
    # LiveKit and Pipecat call before their wait loops) still return.
    commit_id = await asyncio.wait_for(session.commit_audio(), 2)
    await asyncio.wait_for(session.finish(), 2)
    with pytest.raises(GatewayError):
        await asyncio.wait_for(reader, 2)
    assert time.monotonic() - started < 2
    assert "session.continued" not in [event.type for event in events]
    assert [event.data["commit_id"] for event in finals(events)] == [commit_id]


async def test_seam_uses_one_tolerance_around_the_old_end():
    from speko_gateway._relay_continuation import _Audio

    for text, segments, kept in [
        # A new word just after the old end is new, whatever its length.
        ("fine", [seg("fine", 1_002, 1_028)], "fine"),
        # The old last word again, timed across the old end: a repeat.
        ("yes fine", [seg("yes", 985, 1_035), seg("fine", 1_100, 1_200)], "fine"),
    ]:
        session = seam_session([("we said yes", 1_000)], 500)
        session._log = [_Audio(ms(500), block(1) * 30)]
        session._appended = session._log[0].end
        await resume(session, 500)
        events = session._on_final({"text": text, "segments": segments})
        assert [event.type for event in events] == ["transcript.final"], text
        assert events[0].data["text"] == kept
        assert [segment["text"] for segment in events[0].data["segments"]] == [kept]
        await session.aclose()


async def test_seam_trims_a_straddling_segment_with_the_text():
    from speko_gateway._relay_continuation import _Audio, _drop_segment_words

    session = seam_session([("a b c", 1_000)], 500)
    session._log = [_Audio(ms(500), block(1) * 30)]
    session._appended = session._log[0].end
    await resume(session, 500)
    # "b c d" from 810: it overlaps old "b" and "c", and repeats them exactly.
    events = session._on_final({"text": "b c d", "segments": [{**seg("b c d", 810, 1_200), "speaker": "S1"}]})
    assert events[-1].data["text"] == "d"
    assert events[-1].data["segments"] == [{"text": "d", "start_ms": 1_000, "end_ms": 1_200, "speaker": "S1"}]
    await session.aclose()
    # The text path keeps the rest of a partly repeated segment too.
    data = {"text": "b c d e", "segments": [{"text": "b c d"}, {"text": "e"}]}
    _drop_segment_words(data, 2)
    assert data["segments"] == [{"text": "d"}, {"text": "e"}]


async def test_the_input_bound_counts_the_new_chunk_and_every_flush():
    session = stub_session(StubSocket(), log_cap_ms=500, outage_seconds=0.5)
    session._live, session._recovering = False, True
    with pytest.raises(GatewayError):
        await session.send_audio(block(1) * 20)  # 2 s in one call, limit 1 s
    assert session._log == []

    session = stub_session(StubSocket(), max_pending_commits=3)
    session._commit_ids = False
    for _ in range(3):
        assert await session.commit_audio() is None
    with pytest.raises(GatewayError):
        await session.commit_audio()


async def test_the_cap_holds_while_the_reader_sits_on_continued():
    async def dial(configure, key, cancelled):
        return StubSocket(), {**READY, "request_id": "req_1"}

    session = stub_session(StubSocket(), dial=dial, log_cap_ms=1_000)
    session._heard = True
    for _ in range(10):
        await session.send_audio(block(1))
    session._live = False
    recovery = session._recover("socket_closed")
    seen = [(await recovery.__anext__()).type]
    while seen[-1] != "session.continued":
        seen.append((await recovery.__anext__()).type)
    # The reader has not come back for the next event; input keeps coming.
    for _ in range(100):
        await session.send_audio(block(2))
        await asyncio.sleep(0.001)
    await asyncio.sleep(0.2)
    audio = sum(len(entry.data) for entry in session._log if hasattr(entry, "data"))
    assert audio <= ms(1_000) + 2 * BLOCK
    await recovery.aclose()
    await session.aclose()


async def test_an_untimed_late_final_keeps_the_outage_budget():
    session = stub_session(StubSocket())
    for _ in range(100):
        await session.send_audio(block(1))
    session._replay_end, session._attempts, session._budget_left = ms(1_000), 2, 5.0
    commit_id = await session.commit_audio()
    session._on_final({"text": "late words"})
    session._on_final({"text": "", "commit_id": commit_id})
    # A segment ending within the rounding of the replay end proves nothing.
    session._on_final({"text": "w1", "segments": [seg("w1", 900, 1_001)]})
    assert session._attempts == 2 and session._budget_left == 5.0
    session._on_final({"text": "w1", "segments": [seg("w1", 950, 1_002)]})
    assert session._attempts == 0 and session._budget_left == session._limits.outage_seconds


async def test_no_commit_is_taken_once_pending_commits_are_answered():
    socket = StubSocket([{"type": "session.closed", "usage": {"duration_ms": 100}}])
    session = stub_session(socket)
    await session.send_audio(block(1))
    commit_id = await session.commit_audio()
    stream = session.events()
    failed = await stream.__anext__()
    assert failed.data["commit_id"] == commit_id
    # Before the stream's end runs, a new commit could never be answered.
    with pytest.raises(GatewayError):
        await session.commit_audio()
    await stream.aclose()


async def test_no_continued_when_the_first_replay_write_fails():
    from speko_gateway._relay_continuation import _Audio

    session = stub_session(StubSocket())
    session._log = [_Audio(0, block(1) * 5)]
    session._appended = session._log[0].end
    socket = StubSocket(fail=True)
    events = await session._resume(socket, {**READY, "request_id": "req_1"}, asked=True, start=0)
    assert "session.continued" not in [event.type for event in events]
    assert socket.closed
    await session.aclose()


async def test_a_cancelled_dial_sends_no_configure():
    from speko_gateway._relay_continuation import DialCancelled

    frames = []

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        async for message in ws:
            frames.append(message.data)
        return ws

    app = web.Application()
    app.router.add_get("/v1/{kind}/stream", handle)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    try:
        await web.TCPSite(runner, "127.0.0.1", 0).start()
        client = RelaySpeechClient(api_key="sk_speko_agent_fixture", session_id="00000000-0000-4000-8000-000000000000", base_url=f"http://127.0.0.1:{runner.addresses[0][1]}")
        try:
            with pytest.raises(DialCancelled):
                await client._dial("stt", "{}", "key", "fixture", "fixture-model", lambda: True)
        finally:
            await client.aclose()
        await asyncio.sleep(0.1)
    finally:
        await runner.cleanup()
    assert frames == []


async def test_a_covered_seam_is_exact_despite_rounding():
    # Real Deepgram: covered_ms 28754 (floored) while the last word's segment
    # ends at 28755 (rounded). The replay starts at the covered boundary, so
    # nothing old lies inside it: exact, no seam. The first new word starts
    # where the old one ended, not 1 ms before it.

    session = stub_session(StubSocket())
    for _ in range(30):
        await session.send_audio(block(1))
    session._on_final({"text": "seven thirty", "covered_ms": 1_000, "segments": [seg("seven thirty", 400, 1_001)]})
    assert session._trim == ms(1_000)
    events, _ = await resume(session, 1_000)
    assert session._seam is None
    continued = next(event for event in events if event.type == "session.continued")
    assert continued.data["exact"] is True
    final = session._on_final({"text": "my account", "covered_ms": 1_800, "segments": [seg("my account", 1_000, 1_700)]})
    assert final[-1].data["segments"][0]["start_ms"] == 1_001
    assert final[-1].data["text"] == "my account"
    await session.aclose()


def test_backlog_pacing_is_twice_realtime():
    assert REPLAY_SPEED == 2.0
    assert Limits().replay_frame_ms == 100



async def test_catching_up_outlasts_the_outage_budget():
    # The new session is reached inside a 0.3 s budget, then needs about 2 s
    # to catch up at 2x: the budget limits reaching a session, not catching up.
    fake = FakeRelay(
        drop=lambda conn, event, info: conn == 0 and event == "bytes" and info == 15 * BLOCK,
        ready_delay=lambda index: 0.2 if index == 1 else 0.0,
    )
    events, commits, _ = await run(fake, VALUES, commits=(9, 29), limits=Limits(retry_delay=0.01, outage_seconds=0.3, replay_speed=2))
    assert words(events) == expected(VALUES)
    assert_commits_once(events, commits)
    assert [event.type for event in events].count("session.reconnecting") == 1


async def test_a_hung_write_on_the_old_socket_does_not_block_the_new_one():
    good = StubSocket()

    async def dial(configure, key, cancelled):
        return good, {**READY, "request_id": "req_1"}

    session = stub_session(HangingSocket(), dial=dial, write_timeout=0.2)
    events = []

    async def read():
        async for event in session.events():
            events.append(event)

    reader = asyncio.create_task(read())
    # The live write hangs; past write_timeout it is a drop, and the new
    # session is adopted and gets the audio.
    await asyncio.wait_for(session.send_audio(block(1)), 1)
    for _ in range(50):
        if "session.continued" in [event.type for event in events]:
            break
        await asyncio.sleep(0.02)
    assert "session.continued" in [event.type for event in events]
    await asyncio.sleep(0.1)
    assert block(1) in good.sent
    await session.aclose()
    reader.cancel()
    await asyncio.gather(reader, return_exceptions=True)


class CloseHangs(StubSocket):
    async def send_json(self, value: Any) -> None:
        if value.get("type") == "session.close":
            await asyncio.Event().wait()
        await super().send_json(value)


async def test_session_close_after_the_replay_is_bounded():
    from speko_gateway._relay_continuation import _Audio

    session = stub_session(StubSocket(), write_timeout=0.1)
    session._log = [_Audio(0, block(1) * 3)]
    session._appended = session._log[0].end
    socket = CloseHangs()
    await session._resume(socket, {**READY, "request_id": "req_1"}, asked=True, start=0)
    session._finishing = True
    drainer = session._drainer
    await asyncio.wait_for(drainer, 1)
    assert socket.closed
    await session.aclose()


def timed_final(*segments: tuple[str, int, int]):
    from speko_gateway._relay_continuation import _Final

    words = [word for text, _, _ in segments for word in text.split()]
    return _Final(words, segments[-1][2], ms(10_000), tuple((tuple(text.split()), start, end) for text, start, end in segments))


async def test_a_word_said_again_after_the_old_end_is_new():
    from speko_gateway._relay_continuation import _Audio

    session = stub_session(StubSocket())
    session._history.append(timed_final(("we", 250, 500), ("said", 500, 750), ("yes", 750, 1_000)))
    session._log = [_Audio(ms(500), block(1) * 30)]
    session._appended = session._log[0].end
    await resume(session, 500)
    # "said yes" replayed with timing jitter: repeats. Then "yes" again at
    # 1002 ms: the same word, but 252 ms after the old one started: new.
    events = session._on_final({"text": "said yes yes", "segments": [seg("said", 540, 760), seg("yes", 790, 990), seg("yes", 1_002, 1_120)]})
    assert [event.type for event in events] == ["transcript.final"]
    assert events[0].data["text"] == "yes"
    assert events[0].data["segments"] == [seg("yes", 1_002, 1_120)]
    await session.aclose()


async def test_a_covered_seam_is_exact_at_44_1_khz():
    rate = 44_100
    session = stub_session(StubSocket(), sample_rate=rate)
    for _ in range(60):
        await session.send_audio(block(1))
    # covered_ms 1001 is 44144.1 samples: the offset rounds the replay start
    # down to 1000 ms, while the last word ends at 1002 (rounded).
    session._on_final({"text": "seven", "covered_ms": 1_001, "segments": [seg("seven", 500, 1_002)]})
    events = await session._resume(StubSocket(), {**READY, "request_id": "req_1"}, asked=True, start=session._trim)
    assert session._trim * 1000 // (rate * 2) == 1_000
    assert session._seam is None
    continued = next(event for event in events if event.type == "session.continued")
    assert continued.data["exact"] is True
    await session.aclose()


async def test_asyncio_timeouts_are_caught_on_python_3_10():
    from speko_gateway import _relay_continuation as module

    assert asyncio.TimeoutError in module._TIMEOUTS

    class Times(StubSocket):
        async def send_bytes(self, data: bytes) -> None:
            raise asyncio.TimeoutError

    socket = Times()
    session = stub_session(socket)
    await session.send_audio(block(1))
    assert session._live is False and socket.closed


async def test_a_restarted_session_is_put_on_the_stream_timeline():
    values = list(range(1, 121))
    fake = FakeRelay(confirm=False, drop=lambda conn, event, info: conn == 0 and event == "bytes" and info == 100 * BLOCK)
    events, _, _ = await run(fake, values, chunk=BLOCK)
    restarted = next(event for event in events if event.type == "session.restarted")
    assert restarted.data["from_ms"] > 0
    # Every word sits at its own block's place, after the restart too.
    assert words(events) == expected(values)
    assert_timeline(events, values)


async def test_the_commit_limit_names_every_marker():
    session = stub_session(StubSocket(), max_pending_commits=1)
    await session.commit_audio()
    with pytest.raises(GatewayError) as failure:
        await session.commit_audio()
    assert "commit markers" in str(failure.value)



def old_final(*segments: tuple[str, int, int]):
    from speko_gateway._relay_continuation import _Final

    words = [word for text, _, _ in segments for word in text.split()]
    return _Final(words, segments[-1][2], ms(30_000), tuple((tuple(text.split()), start, end) for text, start, end in segments))


async def stitched(old: list[Any], start_ms: int, finals: list[dict[str, Any]], ready: dict[str, Any] | None = None) -> list[Any]:
    from speko_gateway._relay_continuation import _Audio

    session = stub_session(StubSocket())
    session._history.extend(old)
    session._log = [_Audio(ms(start_ms), block(1) * 100)]
    session._trim = ms(start_ms)
    session._appended = session._log[0].end
    await resume(session, start_ms, ready)
    events = []
    for final in finals:
        events.extend(session._on_event("transcript.final", {"type": "transcript.final", **final}))
    await session.aclose()
    return events


async def test_real_restarted_session_drops_a_segment_inside_delivered_audio():
    # Live Deepgram, restarted-old-server: the plain new session started at
    # 9500 ms and re-transcribed the rest of an already delivered segment.
    old = [old_final(("six seven eight nine and the amount is fifteen dollars and fifty cents", 9_240, 13_875))]
    events = await stitched(
        old,
        9_500,
        [
            {"text": "seven eight nine and the amount is fifteen dollars and fifty cents", "segments": [seg("seven eight nine and the amount is fifteen dollars and fifty cents", 0, 4_365)]},
            {"text": "the weather in almaty today is cold about minus five degrees", "segments": [seg("the weather in almaty today is cold about minus five degrees", 4_365, 8_370)]},
        ],
        {**READY, "request_id": "req_22", "continuation": False},
    )
    # The second final starts 10 ms before the old end and does not repeat
    # the old words it overlaps: kept whole, with an honest 10 ms gap.
    assert [event.type for event in events] == ["session.gap", "transcript.final"]
    assert events[0].data == {"from_ms": 13_865, "to_ms": 13_875}
    assert events[1].data["text"] == "the weather in almaty today is cold about minus five degrees"
    assert events[1].data["segments"][0]["start_ms"] == 13_865


async def test_no_covered_seam_keeps_a_straddler_that_does_not_repeat_every_old_word_it_overlaps():
    # Fake relay without covered_ms, cut at 17.3 s: one new segment straddles
    # the old end (16000). It overlaps all 12 old words but repeats only the
    # last 10: kept whole, with a gap up to the old end.
    old = [
        old_final(("w98 w176 w252 w22", 10_000, 12_000)),
        old_final(("w13 w17 w56 w176", 12_000, 14_000)),
        old_final(("w254 w233 w33 w38", 14_000, 16_000)),
    ]
    text = "w252 w22 w13 w17 w56 w176 w254 w233 w33 w38 w252 w117 w168 w103"
    events = await stitched(old, 11_000, [{"text": text, "segments": [seg(text, 11_002, 18_000)]}])
    assert [event.type for event in events] == ["session.gap", "transcript.final"]
    assert events[0].data == {"from_ms": 11_002, "to_ms": 16_000}
    assert events[1].data["text"] == text


async def test_a_straddling_segment_that_diverges_gets_a_gap_for_its_span_only():
    old = [old_final(("a", 700, 800), ("b", 800, 900), ("c", 900, 1_000))]
    events = await stitched(old, 500, [{"text": "a x y", "segments": [seg("a x y", 750, 1_300)]}])
    # The delivered tail "a b c" is no prefix of "a x y": kept whole, with a
    # gap up to the old end.
    assert [event.type for event in events] == ["session.gap", "transcript.final"]
    assert events[0].data == {"from_ms": 750, "to_ms": 1_000}
    assert events[1].data["text"] == "a x y"


def word(text: str, start: int, end: int) -> dict[str, Any]:
    return {"text": text, "start_ms": start, "end_ms": end}


async def test_words_match_one_to_one_by_text_and_start():
    from speko_gateway._relay_continuation import _Final

    old = [_Final(["no"], 700, ms(30_000), ((("no",), 500, 700),), (("no", 500, 700),))]
    # Two new "no"s 200 ms apart: the one old "no" repeats only the first.
    events = await stitched(old, 400, [{"text": "no no", "segments": [seg("no no", 510, 900)], "words": [word("no", 510, 690), word("no", 710, 900)]}])
    assert [event.type for event in events] == ["transcript.final"]
    assert events[0].data["text"] == "no"
    assert events[0].data["words"] == [word("no", 710, 900)]
    assert events[0].data["segments"][0]["start_ms"] == 710
    # A common prefix that then diverges over delivered audio: a gap there.
    old = [_Final(["we", "said", "yes"], 1_000, ms(30_000), ((("we", "said", "yes"), 300, 1_000),), (("we", 300, 500), ("said", 500, 750), ("yes", 750, 1_000)))]
    events = await stitched(old, 200, [{"text": "we said no", "words": [word("we", 310, 500), word("said", 510, 740), word("no", 760, 990)], "segments": [seg("we said no", 310, 990)]}])
    assert [event.type for event in events] == ["session.gap", "transcript.final"]
    assert events[0].data == {"from_ms": 760, "to_ms": 1_000}
    assert events[1].data["text"] == "no"


async def test_the_budget_stops_at_ready_not_after_the_first_write():
    from speko_gateway._relay_continuation import _Audio

    class Slow(StubSocket):
        async def send_bytes(self, data: bytes) -> None:
            await asyncio.sleep(0.3)
            await super().send_bytes(data)

    session = stub_session(StubSocket())
    session._log = [_Audio(0, block(1) * 3)]
    session._appended = session._log[0].end
    session._deadline = time.monotonic() + 1.0
    await session._resume(Slow(), {**READY, "request_id": "req_1"}, asked=True, start=0)
    assert session._deadline is None
    assert session._budget_left > 0.9
    await session.aclose()


async def test_covered_ms_moving_within_one_ms_of_samples_keeps_the_seam_exact():
    rate = 44_100
    session = stub_session(StubSocket(), sample_rate=rate)
    for _ in range(60):
        await session.send_audio(block(1))
    session._on_final({"text": "six", "covered_ms": 1_000, "segments": [seg("six", 500, 1_000)]})
    # 1001 ms rounds to the same whole ms of samples as 1000.
    session._on_final({"text": "seven", "covered_ms": 1_001, "segments": [seg("seven", 1_000, 1_002)]})
    events = await session._resume(StubSocket(), {**READY, "request_id": "req_1"}, asked=True, start=session._trim)
    assert session._seam is None
    continued = next(event for event in events if event.type == "session.continued")
    assert continued.data["exact"] is True
    await session.aclose()


async def test_without_covered_ms_the_replay_starts_before_the_last_final_ended():
    # Live Deepgram without covered_ms: the final "[9230-13865] six ... fifty
    # cents" arrived with 20 s delivered. The replay at the 23.7 s cut must
    # start a second before 13865, not 5 s before arrival (15 s), or "the
    # weather in almaty today is cold about" (13865-16400) is lost silently.
    session = stub_session(StubSocket())
    session._on_event("transcript.delta", {"type": "transcript.delta", "text": "six"})
    for _ in range(200):
        await session.send_audio(block(1))
    text = "six seven eight nine and the amount is fifteen dollars and fifty cents"
    session._on_final({"text": text, "segments": [seg(text, 9_230, 13_865)]})
    assert session._trim == ms(12_865)
    session._on_event("transcript.delta", {"type": "transcript.delta", "text": "the weather"})
    for _ in range(37):
        await session.send_audio(block(1))
    start = max(session._trim, session._log_start())
    assert start == ms(12_865)
    events, _ = await resume(session, 12_865, {**READY, "request_id": "req_1", "continuation": False})
    assert "session.gap" not in [event.type for event in events]
    new = "fifty cents the weather in almaty today is cold about minus five degrees please call me back"
    events = session._on_event("transcript.final", {"type": "transcript.final", "text": new, "segments": [seg(new, 0, 8_410)]})
    # "fifty cents" overlaps the whole old segment, which it does not repeat
    # in full: kept, with a gap; nothing after 13865 is lost.
    assert [event.type for event in events] == ["session.gap", "transcript.final"]
    assert events[0].data == {"from_ms": 12_865, "to_ms": 13_865}
    assert events[1].data["text"] == new
    await session.aclose()


async def test_without_covered_ms_a_log_that_fell_short_reports_the_missing_span():
    # The same stream with a 5 s log: by the cut the log starts at 18.7 s, so
    # 13865-18700 (after the last final's words) is reported, not dropped.
    session = stub_session(StubSocket(), log_cap_ms=5_000)
    session._on_event("transcript.delta", {"type": "transcript.delta", "text": "six"})
    for _ in range(200):
        await session.send_audio(block(1))
    text = "six seven eight nine and the amount is fifteen dollars and fifty cents"
    session._on_final({"text": text, "segments": [seg(text, 9_230, 13_865)]})
    session._on_event("transcript.delta", {"type": "transcript.delta", "text": "the weather"})
    for _ in range(37):
        await session.send_audio(block(1))
    start = max(session._trim, session._log_start())
    assert start == ms(18_700)
    events, _ = await resume(session, 18_700, {**READY, "request_id": "req_1", "continuation": False})
    gaps = [event.data for event in events if event.type == "session.gap"]
    assert gaps == [{"from_ms": 13_865, "to_ms": 18_700}]
    await session.aclose()


async def test_one_old_word_repeats_one_new_word_only():
    # Old "no" (800-1000) and new "no no" (800-1200): one "no" is kept, in one
    # final or across two.
    old = [old_final(("no", 800, 1_000))]
    events = await stitched(old, 500, [{"text": "no no", "segments": [seg("no no", 800, 1_200)]}])
    assert [event.type for event in events] == ["transcript.final"]
    assert events[0].data["text"] == "no"
    events = await stitched(old, 500, [{"text": "no", "segments": [seg("no", 800, 1_000)]}, {"text": "no", "segments": [seg("no", 1_000, 1_200)]}])
    assert [event.type for event in events] == ["transcript.final"]
    assert events[0].data["text"] == "no"
    # With words[] the old word cannot match twice across finals either.
    old = [old_final(("no", 800, 1_000))]
    old[0].timed_words = (("no", 800, 1_000),)
    events = await stitched(
        old,
        500,
        [
            {"text": "no", "segments": [seg("no", 800, 1_000)], "words": [word("no", 800, 1_000)]},
            {"text": "no", "segments": [seg("no", 1_000, 1_200)], "words": [word("no", 1_000, 1_200)]},
        ],
    )
    assert [event.type for event in events] == ["transcript.final"]
    assert events[0].data["text"] == "no"
    assert events[0].data["words"] == [word("no", 1_000, 1_200)]


async def test_an_inserted_word_keeps_the_whole_segment_with_a_gap():
    # Old "no I said no" (400-1450), new "no uh I said no wait" (410-1750):
    # the old words all lie in the new span and are no exact prefix of it, so
    # the segment is kept whole and the seam reported: never a silent repeat.
    old = [old_final(("no I said no", 400, 1_450))]
    text = "no uh I said no wait"
    events = await stitched(old, 300, [{"text": text, "segments": [seg(text, 410, 1_750)]}])
    assert [event.type for event in events] == ["session.gap", "transcript.final"]
    assert events[0].data == {"from_ms": 410, "to_ms": 1_450}
    assert events[1].data["text"] == text


async def test_a_word_past_the_old_end_survives_a_straddling_repeat():
    # Old "send it" (500-1000), new "send it to" (510-1100): "to" is new.
    old = [old_final(("send it", 500, 1_000))]
    events = await stitched(old, 400, [{"text": "send it to", "segments": [seg("send it to", 510, 1_100)]}])
    assert [event.type for event in events] == ["transcript.final"]
    assert events[0].data["text"] == "to"


async def test_a_straddling_segment_loses_exactly_the_repeated_tail():
    # Old "call me back" (12500-13000), new "call me back tomorrow"
    # (12500-14200): "tomorrow" only, starting where the old words ended.
    old = [old_final(("call me back", 12_500, 13_000))]
    text = "call me back tomorrow"
    events = await stitched(old, 12_000, [{"text": text, "segments": [seg(text, 12_500, 14_200)]}])
    assert [event.type for event in events] == ["transcript.final"]
    assert events[0].data["text"] == "tomorrow"
    assert events[0].data["segments"] == [seg("tomorrow", 13_000, 14_200)]


async def test_a_new_command_after_a_repeated_word_is_kept_with_a_gap():
    # Old "say go left" (200-800), new "say go now" (210-1200): the tail is no
    # prefix of it, so all of it is kept and the seam reported.
    old = [old_final(("say go left", 200, 800))]
    text = "say go now"
    events = await stitched(old, 100, [{"text": text, "segments": [seg(text, 210, 1_200)]}])
    assert [event.type for event in events] == ["session.gap", "transcript.final"]
    assert events[0].data == {"from_ms": 210, "to_ms": 800}
    assert events[1].data["text"] == "say go now"


async def test_a_second_reconnect_uses_the_latest_delivered_end():
    # "turn left" ended at 1000; the first reconnect delivered "um" ending at
    # 830. A second reconnect still knows the words up to 1000 were
    # delivered, so a replayed "left" is a repeat.
    old = [old_final(("turn left", 600, 1_000)), old_final(("um", 700, 830))]
    events = await stitched(old, 500, [{"text": "left", "segments": [seg("left", 790, 1_000)]}, {"text": "go", "segments": [seg("go", 1_000, 1_200)]}])
    assert [event.data["text"] for event in events] == ["go"]


async def test_an_untimed_final_after_a_timed_repeat_is_kept_with_a_gap():
    # The timed "go now" repeats the old one and goes. An untimed "go now"
    # after it cannot be told from it by text: kept, and the seam reported.
    old = [old_final(("go now", 500, 1_000))]
    events = await stitched(old, 400, [{"text": "go now", "segments": [seg("go now", 510, 1_000)]}, {"text": "go now"}])
    assert [event.type for event in events] == ["session.gap", "transcript.final"]
    assert events[1].data["text"] == "go now"


async def test_a_straddler_that_changes_a_word_is_kept_with_a_gap():
    # "send it" (500-1000), then "send this now" (510-1100): not a repeat.
    old = [old_final(("send it", 500, 1_000))]
    events = await stitched(old, 400, [{"text": "send this now", "segments": [seg("send this now", 510, 1_100)]}])
    assert [event.type for event in events] == ["session.gap", "transcript.final"]
    assert events[0].data == {"from_ms": 510, "to_ms": 1_000}
    assert events[1].data["text"] == "send this now"


async def test_only_old_words_overlapping_the_segment_count():
    # Old "go" (500-840) and "go" (850-1000), new "go go" (850-1200): only the
    # second old "go" overlaps it, so one "go" repeats and one is new.
    old = [old_final(("go", 500, 840), ("go", 850, 1_000))]
    events = await stitched(old, 400, [{"text": "go go", "segments": [seg("go go", 850, 1_200)]}])
    assert [event.type for event in events] == ["transcript.final"]
    assert events[0].data["text"] == "go"


async def test_a_straddler_over_a_different_word_is_kept_with_a_gap():
    # "turn left" (650-1000), then "a left now" (890-1380): not a repeat of
    # the overlapped "turn left", so kept whole with a gap.
    old = [old_final(("turn left", 650, 1_000))]
    events = await stitched(old, 500, [{"text": "a left now", "segments": [seg("a left now", 890, 1_380)]}])
    assert [event.type for event in events] == ["session.gap", "transcript.final"]
    assert events[0].data == {"from_ms": 890, "to_ms": 1_000}
    assert events[1].data["text"] == "a left now"


async def test_a_word_after_the_old_end_is_new_on_the_words_path():
    # Old "no" (890-1000); a new "no" at 1005-1195 lies after the old end.
    old = [old_final(("no", 890, 1_000))]
    old[0].timed_words = (("no", 890, 1_000),)
    events = await stitched(old, 500, [{"text": "no", "segments": [seg("no", 1_005, 1_195)], "words": [word("no", 1_005, 1_195)]}])
    assert [event.type for event in events] == ["transcript.final"]
    assert events[0].data["text"] == "no"
