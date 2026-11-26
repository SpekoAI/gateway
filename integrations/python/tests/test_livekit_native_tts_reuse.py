"""A LiveKit TTS on the native relay keeps one admitted stream for its replies."""

from __future__ import annotations

import asyncio
import json
import uuid
from contextlib import asynccontextmanager

import pytest
from aiohttp import web
from livekit.agents import APIConnectOptions

from speko_gateway import RelaySpeechClient
from speko_gateway import _livekit_native_tts as native_tts
from speko_gateway.livekit import TTS

PCM = {name: bytes([value, 0]) * 480 for name, value in (("one", 1), ("two", 2))}
STALE = b"\x09\x00" * 480


class Relay:
    """An owned relay peer: counts admissions and records what each stream was sent."""

    def __init__(
        self, *, hold_first: bool = False, silent_start: bool = False, kill_idle_after: float = 0
    ) -> None:
        self.kill_idle_after = kill_idle_after
        self.killed = 0
        self.connections = 0
        self.controls: list[list[str]] = []
        self.closed: list[bool] = []
        self.commits = 0
        self.hold_first = hold_first
        self.silent_start = silent_start
        self.sockets: list[web.WebSocketResponse] = []
        self.first_audio = asyncio.Event()

    async def handle(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        self.connections += 1
        controls: list[str] = []
        self.controls.append(controls)
        self.closed.append(False)
        index = len(self.closed) - 1
        self.sockets.append(ws)
        await ws.send_json(
            {
                "type": "session.ready",
                "request_id": f"request-{self.connections}",
                "route": {"provider": "fixture", "model": "fixture-model"},
            }
        )
        sequence, late = 0, False
        killer: asyncio.Task[None] | None = None

        async def kill() -> None:
            # The relay's progress timer: no provider event for a while ends the stream.
            await asyncio.sleep(self.kill_idle_after)
            self.killed += 1
            await ws.send_json(
                {"type": "error", "error": {"code": "request_timeout", "retryable": True}}
            )
            await ws.close()

        async for message in ws:
            if killer is not None:
                killer.cancel()
            control = json.loads(message.data)
            controls.append(control["type"])
            if control["type"] == "input.commit":
                self.commits += 1
                if self.hold_first and self.commits == 1:
                    if not self.silent_start:
                        await ws.send_json({"type": "utterance.started", "sequence": 1})
                        await ws.send_bytes(PCM["one"])
                    self.first_audio.set()
                    continue
                if late:
                    # Audio of the cancelled utterance still on its way.
                    late = False
                    await ws.send_bytes(STALE)
                    await ws.send_json({"type": "utterance.done", "sequence": 1})
                sequence += 1
                await ws.send_json({"type": "utterance.started", "sequence": sequence})
                await ws.send_bytes(PCM["two" if self.commits > 1 else "one"])
                await ws.send_json({"type": "utterance.done", "sequence": sequence})
                if self.kill_idle_after:
                    killer = asyncio.create_task(kill())
            elif control["type"] == "input.cancel" and self.hold_first:
                # The relay does not acknowledge a cancel.
                late = True
            elif control["type"] == "session.close":
                self.closed[index] = True
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws


@asynccontextmanager
async def model_on(relay: Relay, **options):
    app = web.Application()
    app.router.add_get("/v1/{kind}/stream", relay.handle)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 0).start()
    client = RelaySpeechClient(
        api_key="sk_speko_agent_fixture",
        session_id=str(uuid.uuid4()),
        base_url=f"http://127.0.0.1:{runner.addresses[0][1]}",
    )
    model = TTS(
        client,
        provider="fixture",
        model="fixture-model",
        credential_source="managed",
        **options,
    )
    try:
        yield model
    finally:
        await model.aclose()
        await client.aclose()
        await runner.cleanup()


async def speak(model: TTS, text: str = "hello there") -> bytes:
    stream = model.stream(conn_options=APIConnectOptions(max_retry=0))
    stream.push_text(text)
    stream.end_input()
    try:

        async def audio() -> bytes:
            return b"".join([bytes(e.frame.data) async for e in stream])

        return await asyncio.wait_for(audio(), 5)
    finally:
        await stream.aclose()


async def until(condition, timeout: float = 3.0) -> None:
    async with asyncio.timeout(timeout):
        while not condition():
            await asyncio.sleep(0.005)


async def test_two_replies_share_one_admitted_stream() -> None:
    relay = Relay()
    async with model_on(relay) as model:
        assert PCM["one"] in await speak(model)
        assert PCM["two"] in await speak(model)
        assert relay.connections == 1
        assert relay.commits == 2
        assert "session.close" not in relay.controls[0]
        assert not relay.closed[0]


async def test_call_end_closes_the_stream_cleanly() -> None:
    relay = Relay()
    async with model_on(relay) as model:
        await speak(model)
        await model.aclose()
        assert relay.closed == [True]
        assert relay.controls[0][-1] == "session.close"


@pytest.mark.parametrize("how", ["idle-close", "relay-drops-it"])
async def test_a_stream_idle_too_long_is_reopened_for_the_next_reply(
    monkeypatch: pytest.MonkeyPatch, how: str
) -> None:
    monkeypatch.setattr(native_tts, "_IDLE_CLOSE", 0.05 if how == "idle-close" else 999)
    relay = Relay()
    async with model_on(relay) as model:
        await speak(model)
        if how == "idle-close":
            await until(lambda: relay.closed == [True])
        else:
            # The relay ends an idle stream, as at a lease it could not renew.
            await relay.sockets[0].send_json(
                {"type": "error", "error": {"code": "lease_expired", "retryable": True}}
            )
            await relay.sockets[0].close()
            await asyncio.sleep(0.05)
        assert PCM["two"] in await speak(model)
        assert relay.connections == 2
        # The second reply is a new utterance on a new stream, never a replay.
        assert relay.commits == 2


async def test_the_idle_close_comes_before_the_relay_would_end_the_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The relay ends a stream that is idle after a commit; the client must close it first.
    monkeypatch.setattr(native_tts, "_IDLE_CLOSE", 0.05)
    relay = Relay(kill_idle_after=0.3)
    async with model_on(relay) as model:
        await speak(model)
        await until(lambda: relay.closed == [True])
        await asyncio.sleep(0.4)
        assert relay.killed == 0
        assert PCM["two"] in await speak(model)
        assert relay.connections == 2


def test_the_idle_close_is_well_under_the_relays_inactivity_timeout() -> None:
    assert native_tts._IDLE_CLOSE <= 15.0


async def test_barge_in_cancels_only_the_utterance_and_keeps_the_stream() -> None:
    relay = Relay(hold_first=True)
    async with model_on(relay) as model:
        first = model.stream(conn_options=APIConnectOptions(max_retry=0))
        first.push_text("hello there")
        first.end_input()
        heard = asyncio.create_task(anext(first))
        await asyncio.wait_for(relay.first_audio.wait(), 3)
        await asyncio.wait_for(heard, 3)
        await first.aclose()  # the caller talks over the reply
        await until(lambda: "input.cancel" in relay.controls[0])
        # Late audio of the cancelled utterance never reaches the next reply.
        audio = await speak(model)
        assert STALE not in audio and PCM["one"] not in audio
        assert PCM["two"] in audio
        assert relay.connections == 1
        assert "session.close" not in relay.controls[0]


async def test_barge_in_before_any_audio_does_not_reuse_an_unclear_stream() -> None:
    relay = Relay(hold_first=True, silent_start=True)
    async with model_on(relay) as model:
        first = model.stream(conn_options=APIConnectOptions(max_retry=0))
        first.push_text("hello there")
        first.end_input()
        await asyncio.wait_for(relay.first_audio.wait(), 3)
        await first.aclose()
        assert PCM["two"] in await speak(model)
        assert relay.connections == 2


async def test_the_stream_rotates_before_exceeding_its_character_limit() -> None:
    relay = Relay()
    async with model_on(relay, max_input_characters=15) as model:
        await speak(model, "hello there")
        await speak(model, "hello there")
        await until(lambda: relay.closed[0])
        assert relay.connections == 2
