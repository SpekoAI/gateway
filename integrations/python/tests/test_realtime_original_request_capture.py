"""Constructor-captured unfunded operations; no provider or admission authority."""

from __future__ import annotations

import asyncio
import time

import pytest

from speko_gateway import GatewayError, RelayRealtimeClient


async def test_original_native_request_capture_is_one_shot_and_uses_constructor_custody():
    client = RelayRealtimeClient(
        api_key="sk_speko_original_fixture",
        session_id="ses_original_request",
        base_url="http://127.0.0.1:1",
        setup_timeout=1,
    )
    child = None
    try:
        client._session.headers["Authorization"] = "Bearer sk_speko_changed_fixture"
        before = time.monotonic()
        deadline, create_once = client._capture_original_request()
        captured = time.monotonic()
        child = create_once()
        assert before + 1 <= deadline <= captured + 1
        assert (
            child._session.headers["Authorization"]
            == "Bearer sk_speko_original_fixture"
        )
        assert child._original_setup_deadline == deadline
        assert 0 < child._remaining_setup() <= client._setup_timeout
        assert not client._attempted and not child._attempted
        with pytest.raises(GatewayError, match="Original realtime request expired"):
            create_once()
    finally:
        if child is not None:
            await child.aclose()
        await client.aclose()


async def test_expired_or_closed_original_capture_cannot_create_late_client():
    client = RelayRealtimeClient(
        api_key="sk_speko_original_fixture",
        session_id="ses_original_request",
        base_url="http://127.0.0.1:1",
        setup_timeout=0.01,
    )
    try:
        deadline, expired = client._capture_original_request()
        await asyncio.sleep(0.02)
        assert time.monotonic() >= deadline
        with pytest.raises(GatewayError, match="Original realtime request expired"):
            expired()
        _, closed = client._capture_original_request()
        await client.aclose()
        with pytest.raises(GatewayError, match="Original realtime request expired"):
            closed()
        assert not client._attempted
    finally:
        await client.aclose()


@pytest.mark.parametrize("timeout", [float("nan"), float("inf"), 0])
async def test_original_native_request_requires_a_finite_setup_horizon(timeout):
    with pytest.raises(ValueError, match="finite and positive"):
        RelayRealtimeClient(
            api_key="sk_speko_original_fixture",
            session_id="ses_original_request",
            base_url="http://127.0.0.1:1",
            setup_timeout=timeout,
        )
