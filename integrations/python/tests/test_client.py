from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from speko_gateway.client import GatewayClient, GatewayError, GatewaySession


async def test_gateway_readiness_wait_tolerates_sidecar_startup_race() -> None:
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


async def test_gateway_session_send_audio_guards_closed_state() -> None:
    class FakeWS:
        def __init__(self) -> None:
            self.sent_bytes: list[bytes] = []
            self.sent_json: list[dict] = []
            self.closed = False

        async def send_bytes(self, data: bytes) -> None:
            self.sent_bytes.append(data)

        async def send_json(self, data: dict) -> None:
            self.sent_json.append(data)

        async def close(self) -> None:
            self.closed = True

    ws = FakeWS()
    session = GatewaySession(ws, {})  # type: ignore[arg-type]
    await session.send_audio(b"\x01\x02")
    assert ws.sent_bytes == [b"\x01\x02"]

    await session.finish()
    with pytest.raises(GatewayError, match="Gateway session is finishing"):
        await session.send_audio(b"\x03\x04")

    await session.aclose()
    with pytest.raises(GatewayError, match="Gateway session is closed"):
        await session.send_audio(b"\x05\x06")


async def test_audio_writes_and_finish_are_serialized() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    sent: list[bytes | dict] = []

    class BlockingWS:
        async def send_bytes(self, data: bytes) -> None:
            if data == b"first":
                entered.set()
                await release.wait()
            sent.append(data)

        async def send_json(self, data: dict) -> None:
            sent.append(data)

    session = GatewaySession(BlockingWS(), {})  # type: ignore[arg-type]
    first = asyncio.create_task(session.send_audio(b"first"))
    await entered.wait()
    second = asyncio.create_task(session.send_audio(b"second"))
    finish = asyncio.create_task(session.finish())
    await asyncio.sleep(0)
    assert sent == []
    release.set()
    await asyncio.wait_for(asyncio.gather(first, second, finish), timeout=1)
    assert sent[:2] == [b"first", b"second"]
    assert sent[2]["type"] == "session.close"
    with pytest.raises(GatewayError, match="finishing"):
        await session.send_audio(b"late")
