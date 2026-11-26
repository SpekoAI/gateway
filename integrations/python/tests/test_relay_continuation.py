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
from speko_gateway._relay_continuation import ContinuingSpeechSession, Limits, align
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
            "commit_ids": True,
        }
        if confirmed:
            ready["continuation"] = True
        await ws.send_json(ready)
        received, buffer, pending, finals = 0, b"", [], 0

        async def final(commit_id: str | None = None) -> None:
            nonlocal pending, finals
            words = [word for word, _ in pending]
            payload: dict[str, Any] = {"type": "transcript.final", "text": " ".join(words)}
            if pending:
                payload["segments"] = [
                    {"text": word, "start_ms": start, "end_ms": start + BLOCK_MS}
                    for word, start in pending
                ]
            if commit_id is not None:
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
    assert failure.value.code == "concurrency_exhausted"


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
    fake = FakeRelay(covered=False, drop=lambda conn, event, info: conn == 0 and event == "bytes" and info == 23 * BLOCK)
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
    # outage. The new session gets all 20 blocks at no more than 1.25x
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
    rate = RATE * 2 * 1.25
    for moment, total in times:
        assert total <= rate * (moment - first) + 2 * BLOCK, (moment - first, total)
    assert times[-1][0] - first >= (20 * BLOCK - 2 * BLOCK) / rate
