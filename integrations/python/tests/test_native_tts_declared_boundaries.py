"""Actual owned sidecar controls for declared and unavailable word boundaries."""

from __future__ import annotations

import asyncio
import inspect
import json

import pytest
import test_native_sidecar as native_cases
from aiohttp import web
from livekit import rtc
from livekit.agents import Agent, AgentSession, APIConnectionError
from livekit.agents import llm as agents_llm
from livekit.agents.voice import io

native_binary = native_cases.native_binary


@pytest.mark.parametrize(
    "first,rest",
    [
        ("Give 0.", "05 mg of medicine."),
        ("Visit example.", "com for help."),
    ],
)
async def test_period_continuations_after_original_lookahead_keep_exact_value(
    native_binary, first, rest
):
    # Reuse the original actual-sidecar value/PCM/finality assertions. The
    # producer's pause exceeds the unchanged old 300-ms lookahead.
    await native_cases.test_native_tts_keeps_split_values_in_one_utterance(
        native_binary, first, rest, 0.4, True
    )


async def test_oversized_undelimited_word_refuses_before_text_commit(native_binary):
    opened, commits, appended = [], [], []

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        opened.append(request.headers["Idempotency-Key"])
        await native_cases.ready(ws)
        async for message in ws:
            control = json.loads(message.data)
            if control["type"] == "input.append":
                appended.append(control["text"])
            elif control["type"] == "input.commit":
                commits.append(1)
                await ws.send_json(
                    {"type": "utterance.started", "sequence": len(commits)}
                )
                await ws.send_bytes(b"\x01\x00" * 480)
                await ws.send_json({"type": "utterance.done", "sequence": len(commits)})
            elif control["type"] == "session.close":
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with native_cases.native_tts(native_binary, handle) as model:
        stream = model.stream()
        try:
            stream.push_text("x" * 241)
            stream.end_input()
            with pytest.raises(APIConnectionError) as error:
                await asyncio.wait_for(native_cases.collect_stream(stream), 3)
            assert not error.value.retryable
            assert len(opened) == 1 and not appended and not commits
        finally:
            await stream.aclose()


@pytest.mark.parametrize(
    "first,rest",
    [
        ("The meeting starts at 9 a.m. Ple", "ase join us."),
        ("Travel via St. Louis", "ville tomorrow."),
        ("Use 2 fl. oz. of water", "fall spray."),
    ],
)
async def test_default_agent_text_transform_completes_exact_words_and_pcm(
    native_binary, first, rest
):
    commits, captured, reported, opened = [], [], [], []
    payload = b"\x01\x00" * 4800

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        opened.append(request.headers["Idempotency-Key"])
        await native_cases.ready(ws)
        pending = ""
        async for message in ws:
            control = json.loads(message.data)
            if control["type"] == "input.append":
                pending += control["text"]
            elif control["type"] == "input.commit":
                commits.append(pending)
                pending = ""
                await ws.send_json(
                    {"type": "utterance.started", "sequence": len(commits)}
                )
                await ws.send_bytes(payload)
                await ws.send_json({"type": "utterance.done", "sequence": len(commits)})
            elif control["type"] == "session.close":
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    class Output(io.AudioOutput):
        def __init__(self):
            options = {"label": "owned-default-text-transform"}
            if "capabilities" in inspect.signature(io.AudioOutput).parameters:
                options["capabilities"] = io.AudioOutputCapabilities(pause=False)
            super().__init__(**options)
            self.duration = 0

        async def capture_frame(self, frame: rtc.AudioFrame):
            await super().capture_frame(frame)
            captured.append(bytes(frame.data))
            self.duration += frame.duration

        def flush(self):
            super().flush()
            self.on_playback_finished(
                playback_position=self.duration, interrupted=False
            )

        def clear_buffer(self):
            self.on_playback_finished(playback_position=0, interrupted=True)

    async def text():
        yield first
        await asyncio.sleep(0.4)
        yield rest

    async with native_cases.native_tts(native_binary, handle) as model:
        # Keep the real SDK default Markdown/emoji transform, including its
        # genuine delayed whitespace/EOF behavior. No transform is replaced.
        session = AgentSession(stt=None, llm=None, tts=model, vad=None)
        session.output.audio = Output()
        session.on(
            "conversation_item_added",
            lambda event: (
                reported.append(event.item.text_content)
                if isinstance(event.item, agents_llm.ChatMessage)
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
                agent=Agent(instructions="Owned default text transform"), **options
            )
            speech = session.say(text(), allow_interruptions=False)
            await asyncio.wait_for(speech.wait_for_playout(), 3)
            assert commits == [first + rest]
            marker = b"\0\0" * (model.sample_rate // 100)
            assert b"".join(captured) in (payload, payload + marker)
            assert reported == [first + rest]
            assert len(opened) == 1
        finally:
            await session.aclose()
