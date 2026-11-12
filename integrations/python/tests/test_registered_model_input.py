from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import pytest
from aiohttp import web

from speko_gateway.relay import RelayError, RelayLLMClient


@asynccontextmanager
async def server(handler):
    app = web.Application()
    app.router.add_post("/v1/llm/responses", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    client = RelayLLMClient(
        api_key="owned-work-key",
        base_url=f"http://127.0.0.1:{port}",
        session_id="owned-session",
    )
    try:
        yield client
    finally:
        await client.aclose()
        await runner.cleanup()


def registered(stream=False, rendering="agent-graph/openai-responses/v1"):
    # Whitespace, original escapes, multibyte text and a large integer inside
    # raw tool arguments must survive as the exact cell-owned request string.
    body = (
        ' { "stream" : '
        + ("true" if stream else "false")
        + ', "input" : "owned café 👋 \\u0061", "arguments":"{\\"n\\":9007199254740993}" }\n'
    )
    return {
        "rendering": rendering,
        "request_body": body,
        "work_nonce": "a" * 64,
        "input_blinder": "b" * 64,
        "idempotency_key": "original-work:1",
    }


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize(
    "rendering",
    ["agent-graph/openai-responses/v1", "agent-graph/anthropic-messages/v1"],
)
async def test_actual_http_retains_exact_protected_body_key_and_session(
    stream, rendering
):
    requests = []
    response = {"id": "resp_owned", "output": [{"type": "message"}]}

    async def handler(request):
        requests.append((await request.json(), dict(request.headers)))
        if stream:
            return web.Response(
                text='event: response.text.delta\ndata: {"delta":"owned"}\n\nevent: response.completed\ndata: {"response_id":"resp_owned"}\n\n',
                content_type="text/event-stream",
            )
        return web.json_response(response)

    args = registered(stream, rendering)
    async with server(handler) as client:
        # A deliberate caller retry preserves original transport identity;
        # the client itself makes exactly one attempt per invocation.
        for _ in range(2):
            if stream:
                result = [
                    item async for item in client.stream_registered_response(**args)
                ]
                assert [item[0] for item in result] == [
                    "response.text.delta",
                    "response.completed",
                ]
            else:
                assert await client.complete_registered_response(**args) == response
    assert len(requests) == 2
    for body, headers in requests:
        assert body == {
            "speko_work_nonce": args["work_nonce"],
            "speko_input_blinder": args["input_blinder"],
            "speko_model_rendering": rendering,
            "speko_model_request": args["request_body"],
        }
        assert body["speko_model_request"].encode("utf-8") == args[
            "request_body"
        ].encode("utf-8")
        assert headers["Idempotency-Key"] == "original-work:1"
        assert headers["Speko-Client-Session-ID"] == "owned-session"
        assert headers["Authorization"] == "Bearer owned-work-key"
        assert headers["Content-Type"] == "application/json"


@pytest.mark.parametrize(
    "body",
    [
        "{}",
        '{"stream":true}',
        '{"stream":1}',
        "[]",
        "null",
        '{"stream":false,"stream":false}',
        '{"stream":false,"x":{"n":1,"n":2}}',
        '{"stream":false,"x":NaN}',
        '{"stream":false',
        '{"stream":false,"x":"\ud800"}',
        '{"stream":false,"x":"' + "😀" * 66000 + '"}',
    ],
)
async def test_invalid_registered_json_or_mode_refuses_before_a_socket(body):
    client = RelayLLMClient(
        api_key="owned", base_url="http://127.0.0.1:1", session_id="owned"
    )
    args = registered()
    args["request_body"] = body
    try:
        with pytest.raises(ValueError, match="registered model input is invalid"):
            await client.complete_registered_response(**args)
    finally:
        await client.aclose()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("rendering", ""),
        ("rendering", "x" * 129),
        ("rendering", "bad rendering"),
        ("work_nonce", "a" * 63),
        ("input_blinder", "B" * 64),
        ("idempotency_key", ""),
        ("idempotency_key", "x" * 257),
        ("idempotency_key", "界" * 85 + "xx"),
        ("idempotency_key", " original"),
        ("idempotency_key", "bad\r\nheader"),
    ],
)
async def test_invalid_metadata_refuses_before_a_socket(field, value):
    client = RelayLLMClient(
        api_key="owned", base_url="http://127.0.0.1:1", session_id="owned"
    )
    args = registered()
    args[field] = value
    try:
        with pytest.raises(ValueError):
            await client.complete_registered_response(**args)
    finally:
        await client.aclose()


async def test_registered_transport_requires_session_and_matching_stream_mode():
    client = RelayLLMClient(api_key="owned", base_url="http://127.0.0.1:1")
    try:
        with pytest.raises(ValueError, match="session_id"):
            await client.complete_registered_response(**registered())
    finally:
        await client.aclose()
    client = RelayLLMClient(
        api_key="owned", base_url="http://127.0.0.1:1", session_id="owned"
    )
    try:
        with pytest.raises(ValueError, match="registered model input"):
            _ = [
                item
                async for item in client.stream_registered_response(**registered(False))
            ]
    finally:
        await client.aclose()


@pytest.mark.parametrize("key", ["original/work #1", "x" * 256, "界" * 85 + "x"])
async def test_opaque_retry_key_preserves_its_original_value_and_byte_bound(key):
    seen = []

    async def handler(request):
        await request.read()
        seen.append(request.headers["Idempotency-Key"])
        return web.json_response({"id": "resp_owned"})

    args = registered()
    args["idempotency_key"] = key
    async with server(handler) as client:
        await client.complete_registered_response(**args)
    assert seen == [key]


@pytest.mark.parametrize("stream", [False, True])
async def test_redirect_is_never_followed_or_retried(stream):
    attempts = []

    async def handler(request):
        attempts.append(await request.read())
        return web.Response(status=307, headers={"Location": "/v1/llm/responses"})

    async with server(handler) as client:
        with pytest.raises(RelayError, match="HTTP 307"):
            if stream:
                _ = [
                    item
                    async for item in client.stream_registered_response(
                        **registered(True)
                    )
                ]
            else:
                await client.complete_registered_response(**registered())
    assert len(attempts) == 1


@pytest.mark.parametrize("stream", [False, True])
async def test_http_error_preserves_code_and_never_retries(stream):
    attempts = []

    async def handler(request):
        attempts.append(await request.read())
        return web.json_response(
            {"error": {"code": "authority_unavailable", "retryable": True}}, status=503
        )

    async with server(handler) as client:
        with pytest.raises(RelayError) as error:
            if stream:
                _ = [
                    item
                    async for item in client.stream_registered_response(
                        **registered(True)
                    )
                ]
            else:
                await client.complete_registered_response(**registered())
    assert error.value.code == "authority_unavailable"
    assert error.value.retryable is True
    assert len(attempts) == 1


@pytest.mark.parametrize("terminal", ["missing", "error", "malformed"])
async def test_stream_failure_never_becomes_completion(terminal):
    async def handler(request):
        await request.read()
        body = 'event: response.text.delta\ndata: {"delta":"partial"}\n\n'
        if terminal == "error":
            body += 'event: error\ndata: {"error":{"code":"provider_error","retryable":false}}\n\n'
        if terminal == "malformed":
            body += "event: response.completed\ndata: not-json\n\n"
        return web.Response(text=body, content_type="text/event-stream")

    seen = []
    async with server(handler) as client:
        with pytest.raises(RelayError):
            async for event in client.stream_registered_response(**registered(True)):
                seen.append(event[0])
    assert seen == ["response.text.delta"]


@pytest.mark.parametrize(
    "body", ["{}", "[]", "not-json", '{"error":{"code":"provider_error"}}']
)
async def test_malformed_or_error_sync_response_cannot_be_a_result(body):
    async def handler(request):
        await request.read()
        return web.Response(text=body, content_type="application/json")

    async with server(handler) as client:
        with pytest.raises(RelayError):
            await client.complete_registered_response(**registered())


@pytest.mark.parametrize("stream", [False, True])
async def test_cancellation_closes_the_actual_request_without_retry(stream):
    entered, released = asyncio.Event(), asyncio.Event()
    transport = []

    async def handler(request):
        await request.read()
        transport.append(request.transport)
        entered.set()
        await released.wait()
        return web.Response(text="")

    async with server(handler) as client:

        async def call():
            if stream:
                return [
                    item
                    async for item in client.stream_registered_response(
                        **registered(True)
                    )
                ]
            return await client.complete_registered_response(**registered())

        task = asyncio.create_task(call())
        try:
            await asyncio.wait_for(entered.wait(), 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

            async def disconnected():
                while not transport[0].is_closing():
                    await asyncio.sleep(0)

            await asyncio.wait_for(disconnected(), 2)
            assert len(transport) == 1
        finally:
            released.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("bound", [False, True])
@pytest.mark.parametrize("suffix", ["", "\n", "\r\n"])
async def test_valid_terminal_json_without_frame_terminator_is_not_completion(
    bound, suffix
):
    async def handler(request):
        await request.read()
        return web.Response(
            text='event: response.completed\ndata: {"response_id":"resp_owned"}'
            + suffix,
            content_type="text/event-stream",
        )

    seen = []
    async with server(handler) as client:
        events = (
            client.stream_registered_response(**registered(True))
            if bound
            else client.stream_response({"input": []})
        )
        with pytest.raises(RelayError):
            async for event in events:
                seen.append(event[0])
    assert seen == [], "EOF cannot dispatch an incomplete completion frame"


@pytest.mark.parametrize("bound", [False, True])
@pytest.mark.parametrize("newline", ["\n", "\r\n"])
async def test_a_fully_terminated_terminal_frame_remains_valid(bound, newline):
    async def handler(request):
        await request.read()
        return web.Response(
            text=newline.join(
                [
                    "event: response.completed",
                    'data: {"response_id":"resp_owned"}',
                    "",
                    "",
                ]
            ),
            content_type="text/event-stream",
        )

    async with server(handler) as client:
        events = (
            client.stream_registered_response(**registered(True))
            if bound
            else client.stream_response({"input": []})
        )
        assert [item[0] async for item in events] == ["response.completed"]
