from __future__ import annotations

import asyncio
import json
import traceback
from contextlib import asynccontextmanager

import pytest
from aiohttp import web

from speko_gateway.relay import RelayError, RelayLLMClient


def arguments(stream=False):
    return {
        "rendering": "agent-graph/openai-responses/v1",
        "request_body": json.dumps({"stream": stream}),
        "work_nonce": "a" * 64,
        "input_blinder": "b" * 64,
        "idempotency_key": "owned-work",
    }


def response_text():
    return json.dumps(
        {
            "id": "resp_owned",
            "route": {
                "provider": "openai",
                "model": "owned",
                "region": "us1",
                "attempt_id": "att_owned",
            },
            "output": [
                {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "text", "text": "owned"}],
                }
            ],
            "stop_reason": "stop",
            "usage": {"input_tokens": 1, "output_tokens": 1},
        },
        separators=(",", ":"),
    )


def frame(event, data):
    return (
        "event: "
        + event
        + "\ndata: "
        + json.dumps(data, separators=(",", ":"))
        + "\n\n"
    )


def stream_text():
    return (
        frame("response.created", {"response_id": "resp_owned"})
        + frame(
            "response.item.added",
            {
                "output_index": 0,
                "item": {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "text", "text": ""}],
                },
            },
        )
        + frame("response.text.delta", {"output_index": 0, "delta": "owned"})
        + frame(
            "response.item.completed",
            {
                "output_index": 0,
                "item": {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "text", "text": "owned"}],
                },
            },
        )
        + frame(
            "response.completed",
            {"stop_reason": "stop", "usage": {"input_tokens": 1, "output_tokens": 1}},
        )
    )


@asynccontextmanager
async def peer(handler):
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


@pytest.mark.parametrize(
    "wire",
    [
        response_text().replace(
            '"stop_reason":"stop"',
            '"stop_reason":"max_output_tokens","stop_reason":"stop"',
        ),
        response_text().replace(
            '"stop_reason":"stop"', '"stop_reason":"stop","stop_reason":"stop"'
        ),
        response_text().replace(
            '"text":"owned"', '"text":"owned-private-wire-marker","text":"owned"'
        ),
        response_text().replace('"output_tokens":1', '"output_tokens":NaN'),
        response_text().replace('"output_tokens":1', '"output_tokens":1e999'),
        response_text().replace('"text":"owned"', '"text":"\\ud800"'),
        "[]",
        "null",
        "true",
        b'{"bad":"\xff"}',
        " " * (1024 * 1024) + response_text(),
    ],
    ids=[
        "conflicting-stop",
        "duplicate-stop",
        "nested-duplicate",
        "nan",
        "overflow",
        "surrogate",
        "array",
        "null",
        "boolean",
        "invalid-utf8",
        "body-bound",
    ],
)
async def test_original_http_json_cannot_hide_conflicting_or_invalid_evidence(wire):
    requests = []

    async def handler(request):
        requests.append(await request.read())
        return web.Response(
            body=wire.encode("utf-8") if isinstance(wire, str) else wire,
            content_type="application/json",
        )

    async with peer(handler) as client:
        with pytest.raises(RelayError) as failure:
            await client.complete_registered_response(**arguments())
        assert failure.value.__cause__ is None and failure.value.__context__ is None
        assert "owned-private-wire-marker" not in "".join(
            traceback.format_exception(failure.value)
        )
    assert len(requests) == 1


def malformed_streams():
    wire = stream_text()
    delta = frame("response.text.delta", {"output_index": 0, "delta": "owned"})
    for value in ["[]", "null", "true", "1", '"owned-private-wire-marker"']:
        yield (
            "nonobject-" + value[:4],
            wire.replace(delta, "event: response.text.delta\ndata: " + value + "\n\n"),
        )
    yield (
        "duplicate-stop",
        wire.replace(
            '"stop_reason":"stop"',
            '"stop_reason":"max_output_tokens","stop_reason":"stop"',
        ),
    )
    yield (
        "duplicate-delta",
        wire.replace(
            '"delta":"owned"', '"delta":"owned-private-wire-marker","delta":"owned"'
        ),
    )
    yield (
        "duplicate-event",
        wire.replace(
            "event: response.completed", "event: error\nevent: response.completed"
        ),
    )
    yield "missing-data", wire.replace(delta, "event: response.text.delta\n\n")
    yield "unnamed-data", wire.replace(delta, 'data: {"delta":"owned"}\n\n')
    yield (
        "trailing-error",
        wire + frame("error", {"error": {"code": "owned-private-wire-marker"}}),
    )
    yield (
        "trailing-terminal",
        wire + frame("response.completed", {"stop_reason": "stop", "usage": {}}),
    )
    yield "trailing-delta", wire + delta
    yield (
        "bare-cr-hidden-error",
        wire
        + ': heartbeat\revent: error\rdata: {"error":{"code":"owned-private-wire-marker"}}\r\r\n',
    )
    yield (
        "bare-cr-hidden-terminal",
        wire
        + ": heartbeat\r"
        + frame("response.completed", {"stop_reason": "stop"}).replace("\n", "\r")
        + "\n",
    )
    yield "bare-cr-only", wire.replace("\n", "\r")
    yield "extra-cr-delimiter", wire + "\r\r\n"
    yield "bare-cr-final-comment", wire + ": heartbeat\r"
    yield (
        "unterminated-tail",
        wire + 'event: error\ndata: {"error":{"code":"owned-private-wire-marker"}}',
    )
    yield "stream-bound", (": " + "a" * 2048 + "\n") * 600 + wire


_MALFORMED_STREAMS = list(malformed_streams())


@pytest.mark.parametrize(
    ("name", "wire"),
    _MALFORMED_STREAMS,
    ids=[name for name, _ in _MALFORMED_STREAMS],
)
async def test_registered_stream_never_hides_malformed_or_post_terminal_frames(
    name, wire
):
    seen, terminals = [], []

    async def handler(request):
        seen.append(await request.read())
        return web.Response(body=wire.encode("utf-8"), content_type="text/event-stream")

    async with peer(handler) as client:
        with pytest.raises(RelayError) as failure:
            async for event, _ in client.stream_registered_response(**arguments(True)):
                if event == "response.completed":
                    terminals.append(event)
        assert failure.value.__cause__ is None and failure.value.__context__ is None
        assert "owned-private-wire-marker" not in "".join(
            traceback.format_exception(failure.value)
        )
    assert len(seen) == 1 and not terminals, name


@pytest.mark.parametrize("stream", [False, True])
async def test_raw_tool_argument_strings_keep_intentional_duplicate_keys_and_original_tokens(
    stream,
):
    raw = ' {"n":9007199254740993,"n":1.25,"label":"漢\\u0000🙂"} '
    item = {
        "type": "function_call",
        "call_id": "call_1",
        "name": "lookup",
        "arguments": raw,
    }

    async def handler(request):
        await request.read()
        if stream:
            wire = (
                frame("response.created", {"response_id": "resp_owned"})
                + frame(
                    "response.item.added",
                    {
                        "output_index": 0,
                        "item": {
                            "type": "function_call",
                            "call_id": "call_1",
                            "name": "lookup",
                        },
                    },
                )
                + frame(
                    "response.function_call_arguments.delta",
                    {"output_index": 0, "delta": raw},
                )
                + frame("response.item.completed", {"output_index": 0, "item": item})
                + frame("response.completed", {"stop_reason": "tool_call", "usage": {}})
            )
            return web.Response(text=wire, content_type="text/event-stream")
        body = json.loads(response_text())
        body["output"] = [item]
        body["stop_reason"] = "tool_call"
        return web.json_response(body)

    async with peer(handler) as client:
        if stream:
            frames = [
                record
                async for record in client.stream_registered_response(**arguments(True))
            ]
            assert frames[-2][1]["item"]["arguments"] == raw
        else:
            assert (await client.complete_registered_response(**arguments()))["output"][
                0
            ]["arguments"] == raw


@pytest.mark.parametrize("crlf", [False, True])
async def test_multiline_data_comments_and_line_endings_remain_valid(crlf):
    async def handler(request):
        await request.read()
        wire = 'event: response.created\ndata: {\ndata: "response_id":"resp_owned"\ndata: }\n\n'
        wire += (
            frame("response.completed", {"stop_reason": "stop", "usage": {}})
            + ": heartbeat\n\n"
        )
        return web.Response(
            text=wire.replace("\n", "\r\n") if crlf else wire,
            content_type="text/event-stream",
        )

    async with peer(handler) as client:
        assert [
            name
            async for name, _ in client.stream_registered_response(**arguments(True))
        ] == ["response.created", "response.completed"]


async def test_success_terminal_waits_for_clean_eof_and_pending_read_is_cancellable():
    entered, release = asyncio.Event(), asyncio.Event()

    async def handler(request):
        await request.read()
        response = web.StreamResponse(headers={"content-type": "text/event-stream"})
        await response.prepare(request)
        await response.write(stream_text().encode("utf-8"))
        entered.set()
        await release.wait()
        return response

    async with peer(handler) as client:
        terminals = []

        async def read():
            async for event, _ in client.stream_registered_response(**arguments(True)):
                if event == "response.completed":
                    terminals.append(event)

        task = asyncio.create_task(read())
        try:
            await asyncio.wait_for(entered.wait(), 2)
            done, _ = await asyncio.wait([task], timeout=0.1)
            assert not done and not terminals, (
                "success escaped before the terminal's trailing evidence was read"
            )
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            release.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
