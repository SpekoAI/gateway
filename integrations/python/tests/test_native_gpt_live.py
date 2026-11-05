"""Actual native sidecar, GPT-Live duplex interface and separately paid backend."""

from __future__ import annotations

import asyncio
import base64
import json

import pytest
from aiohttp import web
from livekit import rtc
from livekit.agents import llm
from test_native_realtime_livekit import collect_generation, owned_lookup
from test_native_sidecar import LOCAL_TOKEN, MODEL_KEY, native_stack
from test_native_sidecar import native_binary as native_binary  # noqa: PLC0414

from speko_gateway import RelayLLMClient, RelayRealtimeClient
from speko_gateway.livekit import LLM, GPTLiveModel

pytestmark = pytest.mark.skipif(
    not hasattr(llm, "DuplexModel"), reason="framework predates DuplexModel"
)


def response(events):
    return web.Response(
        text="".join(
            "event: " + kind + "\ndata: " + json.dumps(value) + "\n\n"
            for kind, value in events
        ),
        content_type="text/event-stream",
    )


def models(socket):
    options = {
        "api_key": MODEL_KEY,
        "session_id": "ses_native_duplex",
        "socket_path": socket,
        "local_auth_token": LOCAL_TOKEN,
    }
    voice = RelayRealtimeClient(**options)
    backend_client = RelayLLMClient(**options)
    backend = LLM(
        backend_client,
        provider="anthropic",
        model="claude-sonnet-4",
        max_output_tokens=123,
        temperature=0,
    )
    model = GPTLiveModel(voice, model="gpt-live-1", voice="marin", backend=backend)
    return voice, backend_client, backend, model


async def test_gpt_live_gates_audio_and_funds_client_backend_tools_without_native_turn_inference(
    native_binary,
):
    configured, permit_ready, received_audio, release_second, recalled = (
        asyncio.Event() for _ in range(5)
    )
    requests, opened, errors, transcripts = [], [], [], []
    generations = asyncio.Queue()
    first, second = b"\x01\x10" * 2400, b"\x02\x10" * 2400

    async def handle(request):
        if request.path == "/v1/llm/responses":
            body = await request.json()
            requests.append((body, request.headers.copy()))
            assert body["routing"] == {
                "mode": "explicit",
                "provider": "anthropic",
                "model": "claude-sonnet-4",
            }
            assert body["max_output_tokens"] == 123 and body["temperature"] == 0
            if len(requests) == 1:
                assert body["tools"][0]["name"] == "owned_lookup"
                assert "synthetic caller fragment" in json.dumps(body["input"])
                assert "future-fragment" not in json.dumps(body["input"])
                return response(
                    [
                        (
                            "response.item.completed",
                            {
                                "item": {
                                    "type": "function_call",
                                    "name": "owned_lookup",
                                    "call_id": "call_owned_live",
                                    "arguments": '{"value":"one"}',
                                }
                            },
                        ),
                        ("response.completed", {}),
                    ]
                )
            assert len(requests) in (2, 3)
            assert any(
                item.get("type") == "function_result"
                and item["result"] == "owned-result"
                for item in body["input"]
            )
            if len(requests) == 3:
                return response(
                    [
                        (
                            "response.text.delta",
                            {"delta": "Remembered verified result."},
                        ),
                        ("response.completed", {}),
                    ]
                )
            return response(
                [
                    ("response.text.delta", {"delta": "First. "}),
                    ("response.text.delta", {"delta": "Second."}),
                    ("response.completed", {}),
                ]
            )
        assert request.path_qs == "/v1/openai/gpt-live"
        opened.append(request.headers.copy())
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        start = await ws.receive_json()
        assert start["type"] == "session.start"
        assert start["session"]["delegation"] == {"type": "client"}
        assert start["session"]["model"] == "gpt-live-1"
        assert start["session"]["audio"]["output"] == {"voice": "marin"}
        assert start["session"]["instructions"] == "owned-private-prompt"
        assert "responses" not in start["session"]
        pending = asyncio.create_task(ws.receive())
        configured.set()
        try:
            await permit_ready.wait()
            assert not pending.done(), "PCM left before session.started"
        finally:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        await ws.send_json({"type": "session.started", "session": {"id": "live_owned"}})
        audio = await ws.receive_json()
        assert audio["type"] == "session.input_audio.append"
        assert len(base64.b64decode(audio["audio"])) > 0
        received_audio.set()
        await ws.send_json(
            {
                "type": "session.input_transcript.delta",
                "delta": "synthetic caller fragment",
                "start_ms": 0,
                "end_ms": 100,
            }
        )
        delegation = {
            "type": "session.delegation.created",
            "offset_ms": 100,
            "delegation": {"id": "del_owned", "target": "client"},
        }
        await ws.send_json(delegation)
        await ws.send_json(
            delegation
        )  # a redelivery is not another paid backend request
        await ws.send_json(
            {
                "type": "session.input_transcript.delta",
                "delta": "future-fragment",
                "start_ms": 101,
                "end_ms": 200,
            }
        )
        result = await ws.receive_json()
        assert result == {
            "type": "session.commentary.append",
            "delegation_id": "del_owned",
            "content": "First. Second.",
        }
        for pcm, text, begin in [(first, "First. ", 200), (second, "Second.", 300)]:
            await ws.send_json(
                {
                    "type": "session.output_transcript.delta",
                    "delta": text,
                    "start_ms": begin,
                    "end_ms": begin + 100,
                }
            )
            await ws.send_json(
                {
                    "type": "session.output_audio.delta",
                    "delta": base64.b64encode(pcm).decode(),
                }
            )
            if pcm is first:
                await release_second.wait()
        await ws.send_json(
            {
                "type": "session.delegation.created",
                "offset_ms": 400,
                "delegation": {"id": "del_recall", "target": "client"},
            }
        )
        recall = await ws.receive_json()
        assert recall == {
            "type": "session.commentary.append",
            "delegation_id": "del_recall",
            "content": "Remembered verified result.",
        }
        recalled.set()
        async for message in ws:
            command = json.loads(message.data)
            assert command["type"] == "session.close"
            await ws.send_json({"type": "session.closed", "usage": {"seconds": 1}})
            await ws.close()
        return ws

    async with native_stack(native_binary, handle) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        session = model.session()
        session.on("generation_created", generations.put_nowait)
        session.on("error", errors.append)
        session.on("input_audio_transcription_completed", transcripts.append)
        try:
            await session._update_session(
                instructions="owned-private-prompt",
                chat_ctx=llm.ChatContext.empty(),
                tools=[owned_lookup],
            )
            session.push_audio(rtc.AudioFrame(b"\x01\x01" * 4800, 48000, 1, 4800))
            await asyncio.wait_for(configured.wait(), 5)
            await asyncio.sleep(0.03)
            permit_ready.set()
            await asyncio.wait_for(session.duplex_session.start(), 5)
            await asyncio.wait_for(received_audio.wait(), 5)
            tool_generation = await asyncio.wait_for(generations.get(), 5)
            messages, calls = await asyncio.wait_for(
                collect_generation(tool_generation), 5
            )
            assert messages == [] and len(calls) == 1
            context = session.chat_ctx
            context.items.append(
                llm.FunctionCallOutput(
                    call_id=calls[0].call_id,
                    name=calls[0].name,
                    output=await owned_lookup("one"),
                    is_error=False,
                )
            )
            await session.update_chat_ctx(context)
            generation = await asyncio.wait_for(generations.get(), 5)
            message = await asyncio.wait_for(anext(generation.message_stream), 5)
            first_frame = await asyncio.wait_for(anext(message.audio_stream), 5)
            assert bytes(first_frame.data) == first[:960]
            assert not release_second.is_set()
            release_second.set()
            pcm = bytes(first_frame.data) + b"".join(
                [bytes(frame.data) async for frame in message.audio_stream]
            )
            text = "".join([part async for part in message.text_stream])
            assert pcm == first + second
            assert text == "First. Second."
            assert [call async for call in generation.function_stream] == []
            assert transcripts == [], (
                "native deltas were promoted into final user turns"
            )
            assert errors == []
            await asyncio.wait_for(recalled.wait(), 5)
            assert len(requests) == 3 and len(opened) == 1
            assert len({headers["Idempotency-Key"] for _, headers in requests}) == 3
            for headers in [opened[0], *(headers for _, headers in requests)]:
                assert headers["Authorization"] == "Bearer " + MODEL_KEY
                assert headers["Speko-Client-Session-Id"] == "ses_native_duplex"
                assert LOCAL_TOKEN not in json.dumps(dict(headers))
        finally:
            permit_ready.set()
            release_second.set()
            await session.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()


@pytest.mark.parametrize(
    "failure",
    [
        "before-ready",
        "malformed",
        "binary",
        "provider",
        "backend-retryable",
        "backend-truncated",
        "missing-ready",
    ],
)
async def test_gpt_live_refuses_protocol_and_backend_failures_without_paid_replay(
    native_binary, failure, caplog
):
    opened, paid, failures = [], [], asyncio.Queue()
    private = "owned-private-error-must-not-escape"

    async def handle(request):
        if request.path == "/v1/llm/responses":
            paid.append(await request.json())
            if failure == "backend-retryable":
                return response(
                    [
                        (
                            "error",
                            {
                                "error": {
                                    "code": "unavailable",
                                    "message": private,
                                    "retryable": True,
                                }
                            },
                        )
                    ]
                )
            return response([("response.text.delta", {"delta": private})])
        opened.append(1)
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        if failure == "before-ready":
            await ws.send_json(
                {
                    "type": "session.output_audio.delta",
                    "delta": base64.b64encode(b"\0\0").decode(),
                }
            )
        elif failure != "missing-ready":
            await ws.send_json(
                {"type": "session.started", "session": {"id": "live_error"}}
            )
            if failure == "malformed":
                await ws.send_str(private)
            elif failure == "binary":
                await ws.send_bytes(private.encode())
            elif failure == "provider":
                await ws.send_json(
                    {
                        "type": "error",
                        "error": {"code": "unavailable", "message": private},
                    }
                )
            else:
                await ws.send_json(
                    {
                        "type": "session.delegation.created",
                        "offset_ms": 0,
                        "delegation": {"target": "client", "id": "del_error"},
                    }
                )
        async for _ in ws:
            pass
        return ws

    async with native_stack(native_binary, handle) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        voice._setup_timeout = 0.05
        session = model.session()
        session.on("error", failures.put_nowait)
        try:
            await session._update_session(instructions="owned-private-prompt", tools=[])
            error = await asyncio.wait_for(failures.get(), 5)
            await asyncio.wait_for(asyncio.shield(session.duplex_session._task), 5)
            assert session.duplex_session._audio.qsize() == 0
            assert not error.recoverable
            assert not error.error.retryable
            assert private not in str(error) + caplog.text
            assert MODEL_KEY not in str(error) + caplog.text
            assert len(opened) == 1
            assert len(paid) == (1 if failure.startswith("backend-") else 0)
            with pytest.raises(Exception, match="attempt is exhausted"):
                model.session()
        finally:
            await session.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()


async def test_real_agent_session_runs_gpt_live_backend_tool_and_streams_both_audio_chunks(
    native_binary,
):
    from livekit.agents import Agent, AgentSession
    from livekit.agents.voice import io

    audible, release, said = asyncio.Event(), asyncio.Event(), asyncio.Event()
    captured, spoken, tools_called, requests, errors = [], [], [], [], []
    first, second = b"\x01\x10" * 2400, b"\x02\x10" * 2400

    @llm.function_tool
    async def owned_agent_lookup(value: str) -> str:
        """Look up one synthetic value."""
        tools_called.append(value)
        return "verified-owned-result"

    class Output(io.AudioOutput):
        def __init__(self):
            super().__init__(
                label="owned-live-duplex",
                capabilities=io.AudioOutputCapabilities(pause=False),
            )
            self.duration = 0

        async def capture_frame(self, frame):
            await super().capture_frame(frame)
            captured.append(bytes(frame.data))
            self.duration += frame.duration
            audible.set()

        def flush(self):
            super().flush()
            self.on_playback_finished(
                playback_position=self.duration, interrupted=False
            )

        def clear_buffer(self):
            self.on_playback_finished(playback_position=0, interrupted=True)

    async def handle(request):
        if request.path == "/v1/llm/responses":
            body = await request.json()
            requests.append(body)
            if len(requests) == 1:
                return response(
                    [
                        (
                            "response.item.completed",
                            {
                                "item": {
                                    "type": "function_call",
                                    "call_id": "call_real_agent",
                                    "name": "owned_agent_lookup",
                                    "arguments": '{"value":"one"}',
                                }
                            },
                        ),
                        ("response.completed", {}),
                    ]
                )
            assert len(requests) == 2
            assert any(
                item.get("result") == "verified-owned-result" for item in body["input"]
            )
            return response(
                [
                    ("response.text.delta", {"delta": "First. Second."}),
                    ("response.completed", {}),
                ]
            )
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json(
            {"type": "session.started", "session": {"id": "live_real_agent"}}
        )
        ask = await ws.receive_json()
        assert (
            ask["type"] == "session.commentary.append" and ask["delegation_id"] is None
        )
        await ws.send_json(
            {
                "type": "session.input_transcript.delta",
                "delta": "synthetic caller fragment",
                "start_ms": 0,
                "end_ms": 100,
            }
        )
        await ws.send_json(
            {
                "type": "session.delegation.created",
                "offset_ms": 100,
                "delegation": {"id": "del_real_agent", "target": "client"},
            }
        )
        answer = await ws.receive_json()
        assert answer["type"] == "session.commentary.append"
        assert answer["delegation_id"] == "del_real_agent"
        assert answer["content"] == "First. Second."
        for pcm, text, begin in [(first, "First. ", 100), (second, "Second.", 200)]:
            await ws.send_json(
                {
                    "type": "session.output_transcript.delta",
                    "delta": text,
                    "start_ms": begin,
                    "end_ms": begin + 100,
                }
            )
            await ws.send_json(
                {
                    "type": "session.output_audio.delta",
                    "delta": base64.b64encode(pcm).decode(),
                }
            )
            if pcm is first:
                await release.wait()
        async for message in ws:
            command = json.loads(message.data)
            assert command["type"] == "session.close"
            await ws.send_json({"type": "session.closed", "usage": {"seconds": 1}})
            await ws.close()
        return ws

    async with native_stack(native_binary, handle) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        session = AgentSession(
            llm=model, stt=None, tts=None, vad=None, turn_detection="manual"
        )
        session.output.audio = Output()
        session.on("error", errors.append)

        def completed(event):
            if (
                isinstance(event.item, llm.ChatMessage)
                and event.item.role == "assistant"
            ):
                spoken.append(event.item.text_content)
                said.set()

        session.on("conversation_item_added", completed)
        try:
            await session.start(
                agent=Agent(
                    instructions="owned-private-prompt", tools=[owned_agent_lookup]
                ),
                record=False,
            )
            speech = session.generate_reply(instructions="Say the verified tool result")
            try:
                await asyncio.wait_for(audible.wait(), 5)
            except TimeoutError:
                pytest.fail(
                    f"duplex stalled: backend_requests={len(requests)}, tools_called={tools_called}, errors={len(errors)}"
                )
            assert not release.is_set()
            release.set()
            await asyncio.wait_for(speech.wait_for_playout(), 5)
            await asyncio.wait_for(said.wait(), 5)
            assert b"".join(captured).startswith(first + second)
            assert not b"".join(captured)[len(first + second) :].strip(b"\0")
            assert spoken == ["First. Second."]
            assert tools_called == ["one"]
            assert len(requests) == 2
            assert errors == []
        finally:
            release.set()
            await session.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()


async def test_gpt_live_input_overflow_refuses_before_any_provider_or_backend_activation(
    native_binary,
):
    requests, failed = [], asyncio.Queue()

    async def handle(request):
        requests.append(request.path)
        return web.Response(status=500)

    async with native_stack(native_binary, handle) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        session = model.session()
        session.on("error", failed.put_nowait)
        try:
            frame = rtc.AudioFrame(b"\x01\x01" * 480, 24000, 1, 480)
            with pytest.raises(Exception, match="session failed"):
                for _ in range(257):
                    session.push_audio(frame)
            error = await asyncio.wait_for(failed.get(), 5)
            assert not error.recoverable
            assert requests == []
        finally:
            await session.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()


@pytest.mark.parametrize(
    "invalid",
    ["automatic-backend", "unbounded-backend", "another-session", "hosted-backend"],
)
async def test_gpt_live_requires_explicit_bounded_backend_in_the_same_native_session(
    native_binary, invalid
):
    requests = []

    async def handle(request):
        requests.append(request.path)
        return web.Response(status=500)

    async with native_stack(native_binary, handle) as (socket, _):
        options = {
            "api_key": MODEL_KEY,
            "session_id": "ses_native_duplex",
            "socket_path": socket,
            "local_auth_token": LOCAL_TOKEN,
        }
        voice = RelayRealtimeClient(**options)
        if invalid == "another-session":
            options["session_id"] = "ses_other"
        elif invalid == "hosted-backend":
            options = {
                "api_key": MODEL_KEY,
                "session_id": "ses_native_duplex",
                "base_url": "https://must-not-contact.invalid",
            }
        backend_client = RelayLLMClient(**options)
        backend = LLM(
            backend_client,
            provider="auto" if invalid == "automatic-backend" else "anthropic",
            model="auto" if invalid == "automatic-backend" else "claude-sonnet-4",
            max_output_tokens=0 if invalid == "unbounded-backend" else 123,
        )
        try:
            with pytest.raises(ValueError, match="session-scoped Speko backend"):
                GPTLiveModel(voice, model="gpt-live-1", voice="marin", backend=backend)
            assert requests == []
        finally:
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()


@pytest.mark.parametrize("mode", ["quiet-tool", "unicode-result"])
async def test_gpt_live_preserves_quiet_tool_policy_and_native_append_bounds(
    native_binary, mode
):
    requests, received = [], asyncio.Event()
    expected = "你好🌍 " * 200
    generations = asyncio.Queue()

    async def handle(request):
        if request.path == "/v1/llm/responses":
            requests.append(await request.json())
            assert len(requests) == 1
            events = (
                [
                    (
                        "response.item.completed",
                        {
                            "item": {
                                "type": "function_call",
                                "call_id": "call_quiet",
                                "name": "owned_lookup",
                                "arguments": '{"value":"one"}',
                            }
                        },
                    )
                ]
                if mode == "quiet-tool"
                else [("response.text.delta", {"delta": expected})]
            )
            return response(events + [("response.completed", {})])
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json(
            {"type": "session.started", "session": {"id": "live_bounds"}}
        )
        await ws.send_json(
            {
                "type": "session.delegation.created",
                "offset_ms": 0,
                "delegation": {"id": "del_bounds", "target": "client"},
            }
        )
        text = ""
        while True:
            command = await ws.receive_json()
            assert command["delegation_id"] == "del_bounds"
            assert len(command["content"].encode("utf-8")) <= 500
            text += command["content"]
            if mode == "quiet-tool":
                assert command["type"] == "session.thinking.append"
                assert json.loads(text) == ["owned-result"]
                break
            assert command["type"] == "session.commentary.append"
            if len(text) == len(expected):
                assert text == expected
                break
        received.set()
        async for message in ws:
            assert json.loads(message.data)["type"] == "session.close"
            await ws.send_json({"type": "session.closed", "usage": {"seconds": 0}})
            await ws.close()
        return ws

    async with native_stack(native_binary, handle) as (socket, _):
        voice, backend_client, backend, model = models(socket)
        session = model.session()
        session.on("generation_created", generations.put_nowait)
        try:
            await session._update_session(
                instructions="owned-private-prompt", tools=[owned_lookup]
            )
            if mode == "quiet-tool":
                generation = await asyncio.wait_for(generations.get(), 5)
                messages, calls = await asyncio.wait_for(
                    collect_generation(generation), 5
                )
                assert messages == [] and len(calls) == 1
                result = llm.FunctionCallOutput(
                    call_id=calls[0].call_id,
                    name=calls[0].name,
                    output="owned-result",
                    is_error=False,
                    reply_required=False,
                )
                context = session.chat_ctx
                context.items.append(result)
                await session.update_chat_ctx(context)
            await asyncio.wait_for(received.wait(), 5)
            assert len(requests) == 1
        finally:
            await session.aclose()
            await model.aclose()
            await backend.aclose()
            await backend_client.aclose()
            await voice.aclose()
