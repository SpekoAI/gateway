"""Desired word ownership when a direct producer pauses without a boundary."""

from __future__ import annotations

import asyncio
import json

import pytest
from aiohttp import web
from test_native_sidecar import collect_stream, native_tts, ready
from test_native_sidecar import native_binary as sidecar_binary

native_binary = sidecar_binary


@pytest.mark.parametrize(
    "first,rest,word",
    [
        ("The meeting starts at 9 a.m. Ple", "ase join us.", "Please"),
        ("Travel via St. Louis", "ville tomorrow.", "Louisville"),
        ("Use 2 fl. oz. of water", "fall spray.", "waterfall"),
    ],
)
async def test_idle_pause_cannot_split_an_unfinished_original_word(
    native_binary, first, rest, word
):
    commits = []

    async def handle(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        await ready(ws)
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
                await ws.send_bytes(b"\x01\x00" * 480)
                await ws.send_json({"type": "utterance.done", "sequence": len(commits)})
            elif control["type"] == "session.close":
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with native_tts(native_binary, handle) as model:
        stream = model.stream()
        consuming = asyncio.create_task(collect_stream(stream))
        try:
            stream.push_text(first)
            await asyncio.sleep(0.4)
            stream.push_text(rest)
            stream.end_input()
            assert await asyncio.wait_for(consuming, 3)
            assert "".join(commits) == first + rest
            assert any(word in piece for piece in commits), (
                "idle commit split an unfinished word across provider utterances"
            )
        finally:
            if not consuming.done():
                consuming.cancel()
            await asyncio.gather(consuming, return_exceptions=True)
            await stream.aclose()
