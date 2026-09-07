from __future__ import annotations

import asyncio
import tempfile
import uuid
from contextlib import asynccontextmanager
from importlib.metadata import version
from pathlib import Path

import aiohttp
import pytest
from aiohttp import web

from speko_gateway.client import GatewayClient, SessionConfig
from speko_gateway.relay import RelayError, RelayLLMClient


@asynccontextmanager
async def server(app: web.Application, *, unix: bool = False):
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    # Keep the Unix path short enough for macOS as well as Linux.
    with tempfile.TemporaryDirectory(prefix="gateway-test-", dir="/tmp") as tmp:
        try:
            if unix:
                address = str(Path(tmp) / "gateway.sock")
                site = web.UnixSite(runner, address)
            else:
                site = web.TCPSite(runner, "127.0.0.1", 0)
            await site.start()
            if not unix:
                address = f"http://127.0.0.1:{runner.addresses[0][1]}"
            yield address
        finally:
            await runner.cleanup()


def assert_marker(headers) -> None:
    assert headers.getall("User-Agent") == [f"speko-gateway/{version('speko-gateway')}"]


async def test_router_native_sse_preserves_headers_body_and_streaming() -> None:
    seen = []
    first_consumed = asyncio.Event()

    async def responses(request):
        seen.append((request.headers.copy(), await request.json()))
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)
        await response.write(
            b'event: response.output_text.delta\ndata: {"delta":"synthetic"}\n\n'
        )
        await asyncio.wait_for(first_consumed.wait(), 3)
        await response.write(
            b'event: response.completed\ndata: {"status":"completed"}\n\n'
        )
        await response.write_eof()
        return response

    app = web.Application()
    app.router.add_post("/v1/llm/responses", responses)
    async with server(app) as address:
        client = RelayLLMClient(api_key="router-fixture-only", base_url=address)
        body = {"model": "synthetic", "input": [], "stream": False}
        try:
            stream = client.stream_response(body)
            assert await asyncio.wait_for(anext(stream), 3) == (
                "response.output_text.delta",
                {"delta": "synthetic"},
            )
            first_consumed.set()
            assert await asyncio.wait_for(anext(stream), 3) == (
                "response.completed",
                {"status": "completed"},
            )
            with pytest.raises(StopAsyncIteration):
                await asyncio.wait_for(anext(stream), 3)
        finally:
            first_consumed.set()
            await client.aclose()

    assert len(seen) == 1
    headers, sent = seen[0]
    assert_marker(headers)
    assert headers["Authorization"] == "Bearer router-fixture-only"
    assert headers["Accept"] == "text/event-stream"
    assert headers["Content-Type"] == "application/json"
    assert uuid.UUID(headers["Idempotency-Key"]).version == 4
    assert sent == {**body, "stream": True}
    assert body["stream"] is False


async def test_router_native_rejection_keeps_error_and_does_not_retry() -> None:
    seen = []

    async def reject(request):
        seen.append(request.headers.copy())
        await request.read()
        return web.json_response(
            {
                "error": {
                    "code": "unauthorized",
                    "retryable": False,
                    "request_id": "router-fixture-request",
                }
            },
            status=401,
        )

    app = web.Application()
    app.router.add_post("/v1/llm/responses", reject)
    async with server(app) as address:
        client = RelayLLMClient(api_key="router-fixture-only", base_url=address)
        try:
            with pytest.raises(RelayError) as caught:
                await asyncio.wait_for(anext(client.stream_response({"input": []})), 3)
            assert caught.value.code == "unauthorized"
            assert caught.value.retryable is False
            assert caught.value.request_id == "router-fixture-request"
        finally:
            await client.aclose()
    assert len(seen) == 1
    assert_marker(seen[0])


async def test_gateway_native_unix_http_and_websocket_preserve_protocol() -> None:
    seen = []
    commands = []
    stream_consumed = asyncio.Event()
    subprotocol = "speko.voice.v0.r3"
    turn_events = [{"type": "turn.started", "data": {"initiator": "agent"}}]
    config = SessionConfig(
        kind="tts",
        execution={"route": "provider_direct", "credential_source": "byok"},
        request={"voice": "synthetic"},
        media={"encoding": "pcm_s16le", "sample_rate_hz": 24000, "channels": 1},
        integration={"name": "fixture", "version": "0.1.0"},
    )

    async def record(request):
        body = await request.json() if request.can_read_body else None
        seen.append((request.path, request.headers.copy(), body))
        if request.path == "/v1/sessions":
            return web.json_response(
                {"stream_url": "/v1/sessions/fixture/stream"}, status=201
            )
        return web.json_response({})

    async def websocket(request):
        seen.append((request.path, request.headers.copy(), None))
        response = web.WebSocketResponse(protocols=[subprotocol])
        await response.prepare(request)
        commands.append(await response.receive_json(timeout=3))
        commands.append(await response.receive_json(timeout=3))
        await response.send_bytes(b"\x00\x01")
        # The native client must yield the frame before the server closes.
        await asyncio.wait_for(stream_consumed.wait(), 3)
        await response.send_json({"type": "audio.done", "data": {"ok": True}})
        commands.append(await response.receive_json(timeout=3))
        await response.close()
        return response

    app = web.Application()
    app.router.add_get("/readyz", record)
    app.router.add_post("/v1/turn-events", record)
    app.router.add_post("/v1/sessions", record)
    app.router.add_get("/v1/sessions/fixture/stream", websocket)
    async with server(app, unix=True) as address:
        client = GatewayClient(
            socket_path=address, local_auth_token="local-fixture-only"
        )
        session = None
        try:
            assert await asyncio.wait_for(client.ready(), 3) is True
            await asyncio.wait_for(client.post_turn_events(turn_events), 3)
            session = await asyncio.wait_for(
                client.open(config, idempotency_key="caller-fixture-idempotency"), 3
            )
            await session.append_text("synthetic")
            await session.commit_text()
            events = session.events()
            first = await asyncio.wait_for(anext(events), 3)
            assert first.type == "audio.frame" and first.audio == b"\x00\x01"
            stream_consumed.set()
            second = await asyncio.wait_for(anext(events), 3)
            assert second.type == "audio.done" and second.data == {"ok": True}
            await session.finish()
            with pytest.raises(StopAsyncIteration):
                await asyncio.wait_for(anext(events), 3)
        finally:
            stream_consumed.set()
            if session is not None:
                await session.aclose()
            await client.aclose()

    assert [path for path, _, _ in seen] == [
        "/readyz",
        "/v1/turn-events",
        "/v1/sessions",
        "/v1/sessions/fixture/stream",
    ]
    for _, headers, _ in seen:
        assert_marker(headers)
        assert headers["Authorization"] == "Bearer local-fixture-only"
        assert headers["Host"] == "speko-gateway"
    assert seen[1][2] == {"events": turn_events}
    assert seen[2][2] == config.as_json()
    assert seen[2][1]["Idempotency-Key"] == "caller-fixture-idempotency"
    assert seen[2][1]["Content-Type"] == "application/json"
    assert seen[3][1]["Sec-WebSocket-Protocol"] == subprotocol
    assert commands == [
        {"type": "text.append", "data": {"text": "synthetic"}},
        {"type": "text.commit", "data": None},
        {"type": "session.close", "data": None},
    ]


async def test_marker_is_not_a_global_aiohttp_default() -> None:
    seen = []

    async def capture(request):
        seen.append(request.headers.copy())
        return web.Response()

    app = web.Application()
    app.router.add_get("/unrelated", capture)
    async with (
        server(app) as address,
        aiohttp.ClientSession(headers={"User-Agent": "caller-fixture"}) as s,
        s.get(f"{address}/unrelated") as response,
    ):
        await response.read()
    assert len(seen) == 1
    assert seen[0].getall("User-Agent") == ["caller-fixture"]
    assert "Authorization" not in seen[0]
