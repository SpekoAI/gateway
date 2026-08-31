from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

from speko_gateway.client import GatewayClient, GatewayError


async def test_gateway_readiness_wait_tolerates_sidecar_startup_race() -> None:
    client = object.__new__(GatewayClient)
    ready = AsyncMock(side_effect=[OSError("socket not created"), False, True])
    client.ready = ready

    await client.wait_until_ready(timeout=0.1, interval=0.001)

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
