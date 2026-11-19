"""Actual automatic Gemini input ownership and local-clear boundaries."""

from __future__ import annotations

import asyncio
import json

import pytest
from aiohttp import web
from livekit import rtc
from livekit.agents import AgentSession, llm
from livekit.agents.voice import io
from test_native_sidecar import native_binary as native_binary  # noqa: PLC0414
from test_native_sidecar import native_stack
from test_realtime_review147 import Output, answer, client_model, start


class Input(io.AudioInput):
    def __init__(self):
        super().__init__(label="owned-gemini-input147")
        self.frames = asyncio.Queue()

    async def __anext__(self):
        return await self.frames.get()


async def until(predicate):
    async def wait():
        while not predicate():
            await asyncio.sleep(0)

    await asyncio.wait_for(wait(), 5)


@pytest.mark.parametrize("clear_completed", [False, True])
async def test_automatic_completed_voice_allows_genuine_typed_followup(
    native_binary, clear_completed
):
    heard, spoken, typed, closed = (asyncio.Event() for _ in range(4))
    received = []

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        setup = await ws.receive_json()
        assert not setup["setup"]["realtimeInputConfig"]["automaticActivityDetection"][
            "disabled"
        ]
        await ws.send_json({"setupComplete": {}})
        async for message in ws:
            command = json.loads(message.data)
            received.append(command)
            if "audio" in command.get("realtimeInput", {}):
                heard.set()
                await ws.send_json(
                    {
                        "serverContent": {
                            "inputTranscription": {"text": "Original voice request"}
                        }
                    }
                )
                await answer(ws, "Completed original voice answer")
            if any(
                part.get("text") == "Independent typed followup"
                for turn in command.get("clientContent", {}).get("turns", [])
                for part in turn["parts"]
            ):
                typed.set()
            if command.get("clientContent", {}).get("turnComplete"):
                await answer(ws, "Typed answer")
        closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        source = Input()
        session = AgentSession(
            llm=model, stt=None, tts=None, vad=None, turn_detection="realtime_llm"
        )
        session.input.audio = source
        session.output.audio = Output()
        session.on(
            "conversation_item_added",
            lambda event: (
                spoken.set()
                if isinstance(event.item, llm.ChatMessage)
                and event.item.role == "assistant"
                and event.item.text_content == "Completed original voice answer"
                else None
            ),
        )
        try:
            await start(session)
            native = next(iter(model._sessions))
            assert not native._manual_turns
            source.frames.put_nowait(rtc.AudioFrame(b"\x01\x10" * 1600, 16000, 1, 1600))
            await asyncio.wait_for(heard.wait(), 5)
            await asyncio.wait_for(spoken.wait(), 5)
            if clear_completed:
                session.clear_user_turn()
            speech = session.generate_reply(user_input="Independent typed followup")
            await asyncio.wait_for(speech.wait_for_playout(), 5)
            assert typed.is_set() and native._failure is None and not closed.is_set(), (
                "completed automatic voice input blocked a genuine typed followup"
            )
            assert any(
                command.get("clientContent", {}).get("turnComplete")
                for command in received
            )
        finally:
            await session.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("automatic", [False, True])
async def test_clear_after_peer_received_audio_fences_original_input(
    native_binary, automatic
):
    heard, typed, closed = (asyncio.Event() for _ in range(3))

    async def peer(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ws.send_json({"setupComplete": {}})
        async for message in ws:
            command = json.loads(message.data)
            if "audio" in command.get("realtimeInput", {}):
                heard.set()
            if "clientContent" in command:
                typed.set()
                if command["clientContent"].get("turnComplete"):
                    await answer(ws, "Must not mix old audio")
        closed.set()
        return ws

    async with native_stack(native_binary, peer) as (socket, _):
        client, model = client_model(socket)
        source = Input()
        session = AgentSession(
            llm=model,
            stt=None,
            tts=None,
            vad=None,
            turn_detection="realtime_llm" if automatic else "manual",
        )
        session.input.audio = source
        session.output.audio = Output()
        try:
            await start(session)
            native = next(iter(model._sessions))
            assert native._manual_turns != automatic
            source.frames.put_nowait(rtc.AudioFrame(b"\x01\x10" * 1600, 16000, 1, 1600))
            await asyncio.wait_for(heard.wait(), 5)
            session.clear_user_turn()
            speech = session.generate_reply(
                user_input="Must not mix with cleared input"
            )
            await until(lambda: closed.is_set() or typed.is_set())
            assert (
                closed.is_set() and not typed.is_set() and native._failure is not None
            ), "cleared transmitted automatic input contaminated a later typed turn"
            await asyncio.wait_for(speech.wait_for_playout(), 5)
        finally:
            await session.aclose()
            await model.aclose()
            await client.aclose()
