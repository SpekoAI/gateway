from __future__ import annotations

import asyncio
import json
import time
from contextlib import asynccontextmanager

import pytest
from aiohttp import web

from speko_gateway import RelayTTSClient
from speko_gateway.relay import RelayError

TEXT = "  exact café 漢🙂\0\n"
REQUEST = {
    "input": TEXT,
    "routing": {"mode": "explicit", "provider": "cartesia", "model": "sonic-3"},
    "audio": {"encoding": "pcm_s16le", "sample_rate_hz": 24000, "channels": 1},
}


@asynccontextmanager
async def server(handler, **options):
    calls = []
    release = asyncio.Event()

    async def handle(request):
        calls.append(request.path)
        return await handler(request, release)

    app = web.Application()
    app.router.add_route("*", "/{path:.*}", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    origin = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
    client = RelayTTSClient(
        api_key="owned-private-key",
        base_url=origin,
        session_id="owned-session",
        **options,
    )
    try:
        yield client, calls
    finally:
        release.set()
        await client.aclose()
        await runner.cleanup()


async def collect(client, request=None, **options):
    return b"".join(
        [
            chunk
            async for chunk in client.synthesize(
                REQUEST if request is None else request,
                **{"idempotency_key": "original-work", **options},
            )
        ]
    )


async def test_exact_unicode_bound_request_audio_and_probe_metadata_are_transported_once(
    monkeypatch,
):
    data = json.loads(json.dumps(REQUEST))
    data["options"] = {"number": 9007199254740993}
    legs = []
    monkeypatch.setattr(
        "speko_gateway.speech.report_leg",
        lambda *args, **kwargs: legs.append((args, kwargs)),
    )

    async def handle(request, _):
        assert request.method == "POST" and request.path == "/v1/tts/speech"
        assert request.headers["Authorization"] == "Bearer owned-private-key"
        assert request.headers["Idempotency-Key"] == "original-work"
        assert request.headers["Speko-Client-Session-ID"] == "owned-session"
        assert request.headers["Accept-Encoding"] == "identity"
        data["input"], data["options"]["number"] = "mutated", 0
        body = await request.read()
        assert b"9007199254740993" in body
        assert json.loads(body) == {
            **REQUEST,
            "options": {"number": 9007199254740993},
            "speko_work_nonce": "a" * 64,
            "speko_input_blinder": "b" * 64,
        }
        return web.Response(
            body=b"\x00\x01\x00\xff",
            headers={"Content-Type": "audio/pcm", "Speko-Request-ID": "req:owned"},
        )

    async with server(handle) as (client, calls):
        assert (
            await collect(client, data, work_nonce="a" * 64, input_blinder="b" * 64)
            == b"\x00\x01\x00\xff"
        )
    assert calls == ["/v1/tts/speech"]
    assert legs == [(("tts",), {"request_id": "req:owned"})]


async def test_unbound_speech_sends_no_private_work_markers():
    async def handle(request, _):
        assert await request.json() == REQUEST
        return web.Response(body=b"audio", content_type="application/octet-stream")

    async with server(handle) as (client, calls):
        assert await collect(client) == b"audio"
    assert len(calls) == 1


@pytest.mark.parametrize(
    "change",
    [
        "nonce_only",
        "blinder_only",
        "uppercase",
        "blank_key",
        "header_injection",
        "mixed",
        "missing_input",
        "surrogate",
        "nan",
        "bytes",
        "oversized",
        "nonobject",
    ],
)
async def test_invalid_speech_requests_fail_before_http_without_private_exception_details(
    change,
):
    request = dict(REQUEST)
    kwargs = {}
    if change == "nonce_only":
        kwargs["work_nonce"] = "a" * 64
    elif change == "blinder_only":
        kwargs["input_blinder"] = "b" * 64
    elif change == "uppercase":
        kwargs.update(work_nonce="A" * 64, input_blinder="b" * 64)
    elif change == "blank_key":
        kwargs["idempotency_key"] = ""
    elif change == "header_injection":
        kwargs["idempotency_key"] = "private\r\nvalue"
    elif change == "mixed":
        request["speko_model_request"] = "private"
    elif change == "missing_input":
        del request["input"]
    elif change == "surrogate":
        request["input"] = "private\ud800"
    elif change == "nan":
        request["options"] = {"value": float("nan")}
    elif change == "bytes":
        request["input"] = b"private"
    elif change == "oversized":
        request["input"] = "x" * (1024 * 1024)
    else:
        request = []

    async def unexpected(*_):
        pytest.fail("invalid speech crossed HTTP")

    async with server(unexpected) as (client, calls):
        with pytest.raises(ValueError) as failed:
            await collect(client, request, **kwargs)
        assert str(failed.value) == "invalid Router speech request"
        assert failed.value.__context__ is None
    assert calls == []


@pytest.mark.parametrize("status", [301, 302, 307, 400, 401, 403, 409, 429, 503])
async def test_http_errors_never_redirect_retry_or_echo_response_content(status):
    async def handle(_, __):
        return web.Response(
            status=status,
            text='{"error":{"code":"private-content","message":"private-content"}}',
            headers={"Location": "/sink"},
        )

    async with server(handle) as (client, calls):
        with pytest.raises(RelayError) as failed:
            await collect(client)
        assert "private-content" not in str(failed.value)
        assert "private-content" not in failed.value.code
        assert failed.value.__context__ is None
    assert calls == ["/v1/tts/speech"]


@pytest.mark.parametrize(
    "headers",
    [
        {"Content-Type": "application/json"},
        {"Content-Type": "text/plain"},
        {"Content-Type": "audio/pcm", "Content-Encoding": "gzip"},
        [("Content-Type", "audio/pcm"), ("Content-Type", "audio/pcm")],
    ],
)
async def test_invalid_audio_envelopes_are_refused_before_any_bytes(headers):
    async def handle(_, __):
        return web.Response(body=b"private", headers=headers)

    async with server(handle) as (client, calls):
        with pytest.raises(RelayError) as failed:
            await collect(client)
        assert failed.value.code == "invalid_response"
    assert len(calls) == 1


async def test_empty_audio_is_not_reported_as_a_completed_utterance():
    async def handle(_, __):
        return web.Response(body=b"", content_type="audio/pcm")

    async with server(handle) as (client, _):
        with pytest.raises(RelayError, match="no speech audio"):
            await collect(client)


@pytest.mark.parametrize("chunked", [False, True])
async def test_audio_bounds_apply_to_declared_and_streamed_bytes(chunked):
    async def handle(request, _):
        if not chunked:
            return web.Response(body=b"12345", content_type="audio/pcm")
        response = web.StreamResponse(headers={"Content-Type": "audio/pcm"})
        await response.prepare(request)
        await response.write(b"12345")
        await response.write_eof()
        return response

    async with server(handle, max_audio_bytes=4) as (client, calls):
        with pytest.raises(RelayError):
            await collect(client)
    assert len(calls) == 1


@pytest.mark.parametrize("mode", ["headers", "body", "trickle"])
async def test_network_waits_are_bounded_including_total_trickle_time(mode):
    async def handle(request, release):
        if mode == "headers":
            await release.wait()
            return web.Response(body=b"audio", content_type="audio/pcm")
        response = web.StreamResponse(headers={"Content-Type": "audio/pcm"})
        await response.prepare(request)
        try:
            if mode == "body":
                await release.wait()
            else:
                for _ in range(20):
                    await asyncio.sleep(0.02)
                    await response.write(b"x")
        except ConnectionError:
            pass
        return response

    async with server(handle, io_timeout=0.05) as (client, calls):
        started = time.monotonic()
        with pytest.raises(RelayError) as failed:
            await collect(client)
        assert failed.value.code == "timeout"
        assert failed.value.__context__ is None
        assert time.monotonic() - started < 0.5
    assert len(calls) == 1


async def test_consumer_pause_does_not_leave_an_io_timer_active_across_yield():
    releases = []

    async def handle(request, release):
        releases.append(release)
        response = web.StreamResponse(headers={"Content-Type": "audio/pcm"})
        await response.prepare(request)
        await response.write(b"a")
        await release.wait()
        await response.write(b"b")
        await response.write_eof()
        return response

    async with server(handle, io_timeout=0.05) as (client, _):
        iterator = client.synthesize(REQUEST, idempotency_key="original-work")
        assert await anext(iterator) == b"a"
        await asyncio.sleep(0.08)
        # Consumer work may outlive the network wait budget without a pending
        # SDK timer cancelling it or poisoning a later ready audio read.
        releases[0].set()
        assert await anext(iterator) == b"b"
        with pytest.raises(StopAsyncIteration):
            await anext(iterator)
        assert not client._responses


async def test_cancellation_during_headers_closes_its_owned_request_without_retry():
    entered = asyncio.Event()

    async def handle(_, release):
        entered.set()
        await release.wait()
        return web.Response(body=b"audio", content_type="audio/pcm")

    async with server(handle) as (client, calls):
        pending = asyncio.create_task(collect(client))
        await entered.wait()
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert not client._responses
    assert calls == ["/v1/tts/speech"]


async def test_closing_while_consumer_is_paused_prevents_buffered_audio_delivery():
    async def handle(request, release):
        response = web.StreamResponse(headers={"Content-Type": "audio/pcm"})
        await response.prepare(request)
        await response.write(b"a")
        await release.wait()
        return response

    async with server(handle) as (client, _):
        iterator = client.synthesize(REQUEST, idempotency_key="original-work")
        assert await anext(iterator) == b"a"
        await client.aclose()
        with pytest.raises(RelayError):
            await anext(iterator)
        assert not client._responses


@pytest.mark.parametrize(
    "options",
    [
        {"io_timeout": 0},
        {"io_timeout": True},
        {"io_timeout": float("inf")},
        {"max_audio_bytes": 0},
        {"max_audio_bytes": True},
        {"session_id": "private\nvalue"},
        {"base_url": "https://example.invalid:bad"},
        {"base_url": "https://user:private@example.invalid"},
        {"base_url": "https://example.invalid/?private"},
    ],
)
async def test_invalid_configuration_has_no_transport_or_private_error_details(options):
    with pytest.raises(ValueError) as failed:
        RelayTTSClient(api_key="owned-private-key", **options)
    assert str(failed.value) == "invalid Router speech configuration"
    assert failed.value.__context__ is None
