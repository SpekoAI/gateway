"""Native Go sidecar plus the real LiveKit realtime interface, without vendor plugins."""

from __future__ import annotations

import asyncio
import base64
import copy
import json

import aiohttp
import pytest
from aiohttp import web
from livekit import rtc
from livekit.agents import llm
from test_native_sidecar import LOCAL_TOKEN, MODEL_KEY, native_stack
from test_native_sidecar import (
    native_binary as native_binary,  # noqa: PLC0414 -- shared pytest fixture
)

from speko_gateway import RelayRealtimeClient
from speko_gateway.livekit import RealtimeModel


@llm.function_tool
async def owned_lookup(value: str) -> str:
    """Look up a synthetic value."""
    return "owned-result"


async def collect_text(stream):
    return "".join([part async for part in stream])


async def collect_audio(stream):
    return b"".join([bytes(frame.data) async for frame in stream])


async def collect_generation(generation):
    messages = []
    async for message in generation.message_stream:
        messages.append(
            await asyncio.gather(
                collect_text(message.text_stream), collect_audio(message.audio_stream)
            )
        )
    return messages, [call async for call in generation.function_stream]


@pytest.mark.parametrize(
    "protocol,model,voice",
    [
        ("openai.realtime.v1", "gpt-realtime-2.1", "marin"),
        ("xai.realtime.v1", "grok-voice-3.0", "eve"),
        ("google.live.v1", "gemini-3.8-live", "Kore"),
    ],
)
async def test_native_realtime_waits_for_own_settings_then_executes_tool_and_audio(
    native_binary, protocol, model, voice
):
    google = protocol == "google.live.v1"
    configuration_seen, allow_ready = asyncio.Event(), asyncio.Event()
    generations = asyncio.Queue()
    calls, sent_audio, failures, inputs = [], bytearray(), [], []
    first = b"\x01\x01" * 2400
    second = b"\x02\x02" * 2400

    async def receive_until(ws, check):
        async for message in ws:
            assert message.type is aiohttp.WSMsgType.TEXT
            event = json.loads(message.data)
            inputs.append(event)
            if google:
                audio = event.get("realtimeInput", {}).get("audio")
                if audio:
                    assert audio["mimeType"] == "audio/pcm;rate=16000"
                    sent_audio.extend(base64.b64decode(audio["data"]))
            elif event.get("type") == "input_audio_buffer.append":
                sent_audio.extend(base64.b64decode(event["audio"]))
            if check(event):
                return event
        raise AssertionError("native socket ended before expected control")

    async def handle(request):
        calls.append((request.path, request.headers.copy()))
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        configuration = await ws.receive_json()
        if google:
            setup = configuration["setup"]
            assert setup["model"] == model
            assert setup["realtimeInputConfig"]["automaticActivityDetection"][
                "disabled"
            ]
            assert setup["systemInstruction"] == {
                "parts": [{"text": "owned-private-prompt"}]
            }
            assert (
                setup["tools"][0]["functionDeclarations"][0]["name"] == "owned_lookup"
            )
        else:
            wanted = configuration["session"]
            assert wanted["instructions"] == "owned-private-prompt"
            assert wanted["tools"][0]["name"] == "owned_lookup"
            if protocol == "openai.realtime.v1":
                assert wanted["audio"]["input"]["transcription"] is None
                assert wanted["audio"]["input"]["turn_detection"] is None
            # Native adapters send an earlier default update. Its acknowledgment
            # must not open our audio gate, even though the socket is already 101.
            initial = copy.deepcopy(wanted)
            initial["instructions"] = ""
            if protocol == "openai.realtime.v1":
                initial["audio"]["input"]["transcription"] = {"model": "whisper-1"}
            await ws.send_json({"type": "session.updated", "session": initial})
        unexpected = asyncio.create_task(ws.receive())
        configuration_seen.set()
        try:
            await allow_ready.wait()
            assert not unexpected.done(), (
                "audio or model work left before our own ready acknowledgment"
            )
            if google:
                await ws.send_json({"setupComplete": {}})
            else:
                effective = copy.deepcopy(wanted)
                # Providers return their effective tool settings, including
                # default metadata beyond the caller's requested schema.
                for tool in effective["tools"]:
                    tool["strict"] = False
                await ws.send_json({"type": "session.updated", "session": effective})
        finally:
            unexpected.cancel()
            await asyncio.gather(unexpected, return_exceptions=True)
        if google:
            await receive_until(
                ws, lambda e: "activityEnd" in e.get("realtimeInput", {})
            )
            await ws.send_json(
                {"serverContent": {"inputTranscription": {"text": "synthetic caller"}}}
            )
            await ws.send_json(
                {
                    "toolCall": {
                        "functionCalls": [
                            {
                                "id": "call-owned",
                                "name": "owned_lookup",
                                "args": {"value": "one"},
                            }
                        ]
                    }
                }
            )
            result = await receive_until(ws, lambda e: "toolResponse" in e)
            assert result["toolResponse"]["functionResponses"] == [
                {
                    "id": "call-owned",
                    "name": "owned_lookup",
                    "response": {"result": "owned-result"},
                }
            ]
            for pcm, text in [(first, "First. "), (second, "Second.")]:
                await ws.send_json(
                    {
                        "serverContent": {
                            "modelTurn": {
                                "parts": [
                                    {
                                        "inlineData": {
                                            "mimeType": "audio/pcm;rate=24000",
                                            "data": base64.b64encode(pcm).decode(),
                                        }
                                    }
                                ]
                            },
                            "outputTranscription": {"text": text},
                        }
                    }
                )
            await ws.send_json({"serverContent": {"turnComplete": True}})
        else:
            await receive_until(ws, lambda e: e.get("type") == "response.create")
            await ws.send_json(
                {"type": "response.created", "response": {"id": "response-tool"}}
            )
            await ws.send_json(
                {
                    "type": "response.function_call_arguments.done",
                    "response_id": "response-tool",
                    "item_id": "item-tool",
                    "call_id": "call-owned",
                    "name": "owned_lookup",
                    "arguments": '{"value":"one"}',
                }
            )
            await ws.send_json(
                {
                    "type": "response.output_item.done",
                    "response_id": "response-tool",
                    "item": {
                        "type": "function_call",
                        "id": "item-tool",
                        "call_id": "call-owned",
                        "name": "owned_lookup",
                        "arguments": '{"value":"one"}',
                    },
                }
            )
            await ws.send_json(
                {
                    "type": "response.done",
                    "response": {"id": "response-tool", "status": "completed"},
                }
            )
            result = await receive_until(
                ws, lambda e: e.get("type") == "conversation.item.create"
            )
            assert result["item"]["type"] == "function_call_output"
            assert result["item"]["output"] == "owned-result"
            await receive_until(ws, lambda e: e.get("type") == "response.create")
            await ws.send_json(
                {"type": "response.created", "response": {"id": "response-audio"}}
            )
            await ws.send_json(
                {
                    "type": "response.output_item.added",
                    "response_id": "response-audio",
                    "item": {"type": "message", "id": "message-audio"},
                }
            )
            for pcm, text in [(first, "First. "), (second, "Second.")]:
                await ws.send_json(
                    {
                        "type": "response.output_audio.delta",
                        "response_id": "response-audio",
                        "item_id": "message-audio",
                        "delta": base64.b64encode(pcm).decode(),
                    }
                )
                await ws.send_json(
                    {
                        "type": "response.output_audio_transcript.delta",
                        "response_id": "response-audio",
                        "item_id": "message-audio",
                        "delta": text,
                    }
                )
            await ws.send_json(
                {
                    "type": "response.output_audio.done",
                    "response_id": "response-audio",
                    "item_id": "message-audio",
                }
            )
            await ws.send_json(
                {
                    "type": "response.output_item.done",
                    "response_id": "response-audio",
                    "item": {"type": "message", "id": "message-audio"},
                }
            )
            await ws.send_json(
                {
                    "type": "response.done",
                    "response": {"id": "response-audio", "status": "completed"},
                }
            )
        async for _ in ws:
            pass
        return ws

    async with native_stack(native_binary, handle) as (socket, _):
        client = RelayRealtimeClient(
            api_key=MODEL_KEY,
            session_id="ses_realtime_worker",
            socket_path=socket,
            local_auth_token=LOCAL_TOKEN,
        )
        model_client = RealtimeModel(
            client, protocol=protocol, model=model, voice=voice
        )
        session = model_client.session(turn_detection_disabled=True)
        session.on("generation_created", generations.put_nowait)
        session.on("error", failures.append)
        try:
            await session._update_session(
                instructions="owned-private-prompt",
                chat_ctx=llm.ChatContext.empty(),
                tools=[owned_lookup],
            )
            await asyncio.wait_for(configuration_seen.wait(), 5)
            session.push_audio(
                rtc.AudioFrame(
                    data=b"\x10\x10" * 4800,
                    sample_rate=48000,
                    num_channels=1,
                    samples_per_channel=4800,
                )
            )
            session.commit_audio()
            reply = session.generate_reply()
            await asyncio.sleep(0.05)
            allow_ready.set()
            await asyncio.wait_for(session.start(), 5)
            generation = await asyncio.wait_for(reply, 5)
            assert generation is await asyncio.wait_for(generations.get(), 5)
            messages, functions = await asyncio.wait_for(
                collect_generation(generation), 5
            )
            assert messages == []
            assert len(functions) == 1  # arguments.done plus item.done is one call
            function = functions[0]
            assert (
                function.call_id,
                function.name,
                json.loads(function.arguments),
            ) == ("call-owned", "owned_lookup", {"value": "one"})
            context = session.chat_ctx
            context.items.append(
                llm.FunctionCallOutput(
                    call_id=function.call_id,
                    name=function.name,
                    output=await owned_lookup("one"),
                    is_error=False,
                )
            )
            await session.update_chat_ctx(context)
            if not google:
                second_reply = session.generate_reply()
                generation = await asyncio.wait_for(second_reply, 5)
                assert generation is await asyncio.wait_for(generations.get(), 5)
            else:
                generation = await asyncio.wait_for(generations.get(), 5)
            messages, functions = await asyncio.wait_for(
                collect_generation(generation), 5
            )
            assert functions == []
            assert messages == [["First. Second.", first + second]]
            assert failures == []
        finally:
            allow_ready.set()
            await session.aclose()
            await model_client.aclose()
            await client.aclose()
    assert len(calls) == 1
    assert calls[0][1]["Authorization"] == "Bearer " + MODEL_KEY
    assert calls[0][1]["Speko-Client-Session-Id"] == "ses_realtime_worker"
    assert LOCAL_TOKEN not in json.dumps(dict(calls[0][1]))
    expected_samples = 1600 if google else 2400
    assert abs(len(sent_audio) // 2 - expected_samples) <= 2


@pytest.mark.parametrize("method", ["reply", "say"])
@pytest.mark.parametrize(
    "protocol,model,voice",
    [
        ("openai.realtime.v1", "gpt-realtime-2.1", "marin"),
        ("xai.realtime.v1", "grok-voice-3.0", "eve"),
        ("google.live.v1", "gemini-3.8-live", "Kore"),
    ],
)
async def test_real_agent_session_plays_both_native_realtime_chunks_in_one_turn(
    native_binary, method, protocol, model, voice
):
    import inspect

    from livekit.agents import Agent, AgentSession
    from livekit.agents.voice import io

    if (
        method == "say"
        and "supports_say" not in llm.RealtimeCapabilities.__dataclass_fields__
    ):
        pytest.skip("framework predates native realtime AgentSession.say support")

    audible, release = asyncio.Event(), asyncio.Event()
    captured, reported, opened, errors = [], [], [], []
    first, second = b"\x01\x10" * 4800, b"\x02\x10" * 4800
    google = protocol == "google.live.v1"

    class Output(io.AudioOutput):
        def __init__(self):
            options = {"label": "owned-native-realtime"}
            if "capabilities" in inspect.signature(io.AudioOutput).parameters:
                options["capabilities"] = io.AudioOutputCapabilities(pause=False)
            super().__init__(**options)
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
        opened.append(request.headers.copy())
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        configuration = await ws.receive_json()
        if google:
            await ws.send_json({"setupComplete": {}})
        else:
            await ws.send_json(
                {"type": "session.updated", "session": configuration["session"]}
            )
        command = await ws.receive_json()
        assert (
            ("clientContent" in command)
            if google
            else command["type"] == "response.create"
        )
        if not google:
            await ws.send_json(
                {"type": "response.created", "response": {"id": "response-audible"}}
            )
            await ws.send_json(
                {
                    "type": "response.output_item.added",
                    "response_id": "response-audible",
                    "item": {"type": "message", "id": "message-audible"},
                }
            )
        for pcm, text in [(first, "First. "), (second, "Second.")]:
            if google:
                await ws.send_json(
                    {
                        "serverContent": {
                            "modelTurn": {
                                "parts": [
                                    {
                                        "inlineData": {
                                            "mimeType": "audio/pcm;rate=24000",
                                            "data": base64.b64encode(pcm).decode(),
                                        }
                                    }
                                ]
                            },
                            "outputTranscription": {"text": text},
                        }
                    }
                )
            else:
                await ws.send_json(
                    {
                        "type": "response.output_audio.delta",
                        "response_id": "response-audible",
                        "item_id": "message-audible",
                        "delta": base64.b64encode(pcm).decode(),
                    }
                )
                await ws.send_json(
                    {
                        "type": "response.output_audio_transcript.delta",
                        "response_id": "response-audible",
                        "item_id": "message-audible",
                        "delta": text,
                    }
                )
            if pcm is first:
                await release.wait()
        if google:
            await ws.send_json({"serverContent": {"turnComplete": True}})
        else:
            await ws.send_json(
                {
                    "type": "response.output_item.done",
                    "response_id": "response-audible",
                    "item": {"type": "message", "id": "message-audible"},
                }
            )
            await ws.send_json(
                {
                    "type": "response.done",
                    "response": {"id": "response-audible", "status": "completed"},
                }
            )
        async for _ in ws:
            pass
        return ws

    async with native_stack(native_binary, handle) as (socket, _):
        client = RelayRealtimeClient(
            api_key=MODEL_KEY,
            session_id="ses_realtime_audible",
            socket_path=socket,
            local_auth_token=LOCAL_TOKEN,
        )
        realtime = RealtimeModel(client, protocol=protocol, model=model, voice=voice)
        session = AgentSession(
            llm=realtime, stt=None, tts=None, vad=None, turn_detection="manual"
        )
        session.output.audio = Output()
        session.on("error", errors.append)
        session.on(
            "conversation_item_added",
            lambda event: (
                reported.append(event.item.text_content)
                if isinstance(event.item, llm.ChatMessage)
                and event.item.role == "assistant"
                else None
            ),
        )
        try:
            options = (
                {"record": False}
                if "record" in inspect.signature(session.start).parameters
                else {}
            )
            await session.start(
                agent=Agent(instructions="owned-private-prompt"), **options
            )
            speech = (
                session.say("First. Second.")
                if method == "say"
                else session.generate_reply(
                    instructions="Say the synthetic test response"
                )
            )
            await asyncio.wait_for(audible.wait(), 5)
            assert not release.is_set()
            release.set()
            await asyncio.wait_for(speech.wait_for_playout(), 5)
            expected = first + second
            marker = b"\0\0" * 240
            assert b"".join(captured) in (expected, expected + marker)
            assert reported == ["First. Second."]
            assert errors == []
            assert len(opened) == 1
        finally:
            release.set()
            await session.aclose()
            await realtime.aclose()
            await client.aclose()


@pytest.mark.parametrize(
    "protocol,model,voice",
    [
        ("openai.realtime.v1", "gpt-realtime-2.1", "marin"),
        ("xai.realtime.v1", "grok-voice-3.0", "eve"),
    ],
)
async def test_native_realtime_interrupt_discards_unplayed_backlog_without_closing_session(
    native_binary, protocol, model, voice
):
    observed, errors = [], []
    # Fill the bounded reader queue while nothing has reached playback.
    backlog = base64.b64encode(b"\x01\x10" * (480 * 300)).decode()
    next_pcm = b"\x02\x10" * 480

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        configuration = await ws.receive_json()
        await ws.send_json(
            {"type": "session.updated", "session": configuration["session"]}
        )
        assert (await ws.receive_json())["type"] == "response.create"
        await ws.send_json(
            {"type": "response.created", "response": {"id": "response-old"}}
        )
        await ws.send_json(
            {
                "type": "response.output_item.added",
                "response_id": "response-old",
                "item": {"type": "message", "id": "message-old"},
            }
        )
        await ws.send_json(
            {
                "type": "response.output_audio_transcript.delta",
                "response_id": "response-old",
                "item_id": "message-old",
                "delta": "Unheard.",
            }
        )
        await ws.send_json(
            {
                "type": "response.output_audio.delta",
                "response_id": "response-old",
                "item_id": "message-old",
                "delta": backlog,
            }
        )
        for kind in [
            "response.cancel",
            "conversation.item.truncate",
            "response.create",
        ]:
            command = await ws.receive_json()
            observed.append(command)
            assert command["type"] == kind
        assert observed[1]["audio_end_ms"] == 0
        await ws.send_json(
            {"type": "response.created", "response": {"id": "response-new"}}
        )
        # The old terminal is late. It cannot close or contaminate the next reply.
        await ws.send_json(
            {
                "type": "response.output_audio.delta",
                "response_id": "response-old",
                "item_id": "message-old",
                "delta": base64.b64encode(b"\x03\x10" * 480).decode(),
            }
        )
        await ws.send_json(
            {
                "type": "response.done",
                "response": {"id": "response-old", "status": "cancelled"},
            }
        )
        await ws.send_json(
            {
                "type": "response.output_audio.delta",
                "response_id": "response-new",
                "item_id": "message-new",
                "delta": base64.b64encode(next_pcm).decode(),
            }
        )
        await ws.send_json(
            {
                "type": "response.output_audio_transcript.delta",
                "response_id": "response-new",
                "item_id": "message-new",
                "delta": "Next.",
            }
        )
        await ws.send_json(
            {
                "type": "response.done",
                "response": {"id": "response-new", "status": "completed"},
            }
        )
        async for _ in ws:
            pass
        return ws

    async with native_stack(native_binary, handle) as (socket, _):
        client = RelayRealtimeClient(
            api_key=MODEL_KEY,
            session_id="ses_realtime_interrupt",
            socket_path=socket,
            local_auth_token=LOCAL_TOKEN,
        )
        model_client = RealtimeModel(
            client, protocol=protocol, model=model, voice=voice
        )
        session = model_client.session()
        session.on("error", errors.append)
        try:
            await session._update_session(
                instructions="owned-private-prompt",
                chat_ctx=llm.ChatContext.empty(),
                tools=[],
            )
            await asyncio.wait_for(session.start(), 3)
            old = await asyncio.wait_for(session.generate_reply(), 3)
            message = await asyncio.wait_for(anext(old.message_stream), 3)

            async def backpressured():
                while session._generation.message("message-old").audio.qsize() < 256:
                    await asyncio.sleep(0.001)

            await asyncio.wait_for(backpressured(), 3)
            session.interrupt()
            session.truncate(
                message_id="message-old",
                modalities=["audio"],
                audio_end_ms=0,
                audio_transcript="",
            )
            remaining = [frame async for frame in message.audio_stream]
            assert len(remaining) == 0, "unplayed backlog survived interruption"
            generation = await asyncio.wait_for(session.generate_reply(), 3)
            messages, calls = await asyncio.wait_for(collect_generation(generation), 3)
            assert messages == [["Next.", next_pcm]]
            assert calls == [] and errors == []
            assert not session._task.done()
            prior = session.chat_ctx.get_by_id("message-old")
            assert isinstance(prior, llm.ChatMessage) and not prior.text_content
        finally:
            await session.aclose()
            await model_client.aclose()
            await client.aclose()


@pytest.mark.parametrize(
    "protocol,model,voice",
    [
        ("openai.realtime.v1", "gpt-realtime-2.1", "marin"),
        ("xai.realtime.v1", "grok-voice-3.0", "eve"),
        ("google.live.v1", "gemini-3.8-live", "Kore"),
    ],
)
async def test_native_realtime_resamples_two_committed_turns_without_reusing_eof_state(
    native_binary, protocol, model, voice
):
    samples, errors = [], []
    google = protocol == "google.live.v1"
    pcm = b"\x02\x10" * 480

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        configuration = await ws.receive_json()
        await ws.send_json(
            {"setupComplete": {}}
            if google
            else {"type": "session.updated", "session": configuration["session"]}
        )
        for turn in range(2):
            size = 0
            async for message in ws:
                event = json.loads(message.data)
                if google:
                    audio = event.get("realtimeInput", {}).get("audio")
                    if audio:
                        size += len(base64.b64decode(audio["data"])) // 2
                    if "activityEnd" in event.get("realtimeInput", {}):
                        break
                else:
                    if event.get("type") == "input_audio_buffer.append":
                        size += len(base64.b64decode(event["audio"])) // 2
                    if event.get("type") == "response.create":
                        break
            samples.append(size)
            if google:
                await ws.send_json(
                    {
                        "serverContent": {
                            "modelTurn": {
                                "parts": [
                                    {
                                        "inlineData": {
                                            "mimeType": "audio/pcm;rate=24000",
                                            "data": base64.b64encode(pcm).decode(),
                                        }
                                    }
                                ]
                            },
                            "outputTranscription": {"text": "Turn."},
                        }
                    }
                )
                await ws.send_json({"serverContent": {"turnComplete": True}})
            else:
                response_id, item_id = f"response-{turn}", f"message-{turn}"
                await ws.send_json(
                    {"type": "response.created", "response": {"id": response_id}}
                )
                await ws.send_json(
                    {
                        "type": "response.output_audio.delta",
                        "response_id": response_id,
                        "item_id": item_id,
                        "delta": base64.b64encode(pcm).decode(),
                    }
                )
                await ws.send_json(
                    {
                        "type": "response.output_audio_transcript.delta",
                        "response_id": response_id,
                        "item_id": item_id,
                        "delta": "Turn.",
                    }
                )
                await ws.send_json(
                    {
                        "type": "response.done",
                        "response": {"id": response_id, "status": "completed"},
                    }
                )
        async for _ in ws:
            pass
        return ws

    async with native_stack(native_binary, handle) as (socket, _):
        client = RelayRealtimeClient(
            api_key=MODEL_KEY,
            session_id="ses_realtime_two_turns",
            socket_path=socket,
            local_auth_token=LOCAL_TOKEN,
        )
        model_client = RealtimeModel(
            client, protocol=protocol, model=model, voice=voice
        )
        session = model_client.session(turn_detection_disabled=True)
        session.on("error", errors.append)
        try:
            await session._update_session(
                instructions="owned-private-prompt",
                chat_ctx=llm.ChatContext.empty(),
                tools=[],
            )
            await asyncio.wait_for(session.start(), 3)
            for _ in range(2):
                session.push_audio(
                    rtc.AudioFrame(
                        data=b"\x10\x10" * 4800,
                        sample_rate=48000,
                        num_channels=1,
                        samples_per_channel=4800,
                    )
                )
                session.commit_audio()
                generation = await asyncio.wait_for(session.generate_reply(), 3)
                messages, calls = await asyncio.wait_for(
                    collect_generation(generation), 3
                )
                assert messages == [["Turn.", pcm]] and calls == []
            assert errors == []
        finally:
            await session.aclose()
            await model_client.aclose()
            await client.aclose()
    expected = 1600 if google else 2400
    assert len(samples) == 2 and all(abs(size - expected) <= 2 for size in samples)
